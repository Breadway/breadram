//! The llama.cpp server adapter.
//!
//! This adapter observes documented idle-timer-exempt endpoints and, when
//! automation is enabled, exposes the router's documented unload operation
//! to conformance callers. Daemon policy still cannot unload a loaded model
//! without complete idle evidence, which this adapter does not yet provide.
//! Correlated per-model status confirmation is implemented and verified
//! against a real server (see docs/LLAMACPP.md): unload, reload, inference
//! after relief, and separately measured host and device effects.
//!
//! What it can honestly report:
//!
//! - **Inventory**, from the router's `GET /models` (each model's id, path,
//!   and status) or a single model's path from `GET /props`. Properties are
//!   only used for single-model inventory after `/models` reports absence.
//! - **Activity** remains unknown for resident models: loaded does not mean
//!   processing. A valid router inventory containing only unloaded or sleeping
//!   models, or a single-model properties response reporting sleep, is idle.
//!
//! Every URL this adapter can build is listed in the module constants. The write
//! path is the router's documented `POST /models/unload`, driven only when
//! automation enables the UnloadModel capability and confirmed by the exact
//! model id plus its status in the same `/models` entry. Single-model servers
//! expose no documented manual unload and are never written to. A logical
//! unload status is not a measurement of memory reclaimed.
//!
//! The endpoints that are exempt from the server's idle timer (`/health`,
//! `/props`, `/models`) are exactly the ones this adapter probes, so
//! observation never wakes a sleeping model and never resets the sleep clock.
//! That is a property of the server documented upstream, and it is what makes
//! observation here genuinely free.

use std::sync::{Mutex, MutexGuard};

use serde::Deserialize;

use crate::config::{ActionKind, ProviderKind};
use crate::inventory::Workload;
use crate::policy::activity::ActivityEvidence;

use super::capability::{AdapterCapabilities, AdapterCapability, CAPABILITY_PROBE_VERSION};
use super::probe::{classify, ProbeRequest, Response};
use super::{
    AdapterOperation, Confirmation, ManagedResource, OperationRequest, ProbeOutcome,
    WorkloadAdapter,
};

/// The readiness endpoint, public and always exempt from the idle timer.
pub const HEALTH_PATH: &str = "/health";
/// The router's model list, exempt from the idle timer.
pub const MODELS_PATH: &str = "/models";
/// The server's global properties, exempt from the idle timer.
pub const PROPS_PATH: &str = "/props";
/// The router's documented unload endpoint, driven only when automation is on.
pub const UNLOAD_PATH: &str = "/models/unload";
/// Not used by discovery: this path is not documented as idle-timer exempt.
pub const V1_MODELS_PATH: &str = "/v1/models";
/// The model the server itself reports, used for diagnostics.
pub const MODEL_PATH_NOTE: &str = "model_path";

/// What a model is doing right now, as the server reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelStatus {
    /// The router has loaded the model and it is serving.
    Loaded,
    /// The router is loading it now.
    Loading,
    /// The server is downloading the model files.
    Downloading,
    /// The router holds the model but nothing is loaded.
    Unloaded,
    /// The server put the model to sleep itself.
    Sleeping,
    /// The last load attempt failed.
    Failed,
    /// Missing or unrecognized state, never evidence of inactivity.
    Unknown,
}

impl ModelStatus {
    /// The wire name the router reports.
    pub fn as_str(&self) -> &'static str {
        match self {
            ModelStatus::Loaded => "loaded",
            ModelStatus::Loading => "loading",
            ModelStatus::Downloading => "downloading",
            ModelStatus::Unloaded => "unloaded",
            ModelStatus::Sleeping => "sleeping",
            ModelStatus::Failed => "failed",
            ModelStatus::Unknown => "unknown",
        }
    }
}

/// One model the server reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportedModel {
    /// The model id the router uses, or the single model's id.
    pub id: String,
    /// What the server says it is doing.
    pub status: ModelStatus,
    /// The model file size, when the server reported one.
    ///
    /// A memory-mapped model shares pages with the page cache, so this is an
    /// estimate of model bytes and never a measurement of host RAM usage.
    pub size_bytes: Option<u64>,
}

/// The adapter's view of one llama.cpp server.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Observation {
    /// What the server answered to the model list, when the surface exists.
    models: Vec<ReportedModel>,
    /// Whether the last read of the model list succeeded.
    fresh: bool,
    /// Whether the last readiness read succeeded.
    health_ok: bool,
    /// Whether the server itself reports sleeping.
    sleeping: Option<bool>,
    /// Whether this is a router (many models) rather than one model.
    router: bool,
    /// The model file the server named, when it did.
    model_path: Option<String>,
    last_error: Option<String>,
    observed_at_ms: Option<u64>,
    /// When the server was first seen holding nothing, in monotonic ms.
    idle_since_ms: Option<u64>,
}

impl Observation {
    fn empty() -> Self {
        Self {
            models: Vec::new(),
            fresh: false,
            health_ok: false,
            sleeping: None,
            router: false,
            model_path: None,
            last_error: None,
            observed_at_ms: None,
            idle_since_ms: None,
        }
    }
}

/// An adapter for one configured llama.cpp server.
///
/// The adapter performs no I/O: it asks for probes with
/// [`WorkloadAdapter::discover`] and is handed the answers back through
/// [`WorkloadAdapter::observe`]. Until an answer has been recorded the
/// workload reports unknown activity, which policy never treats as idle.
pub struct LlamaCppAdapter {
    name: String,
    endpoint: String,
    scope: String,
    capabilities: AdapterCapabilities,
    state: Mutex<Observation>,
}

impl std::fmt::Debug for LlamaCppAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlamaCppAdapter")
            .field("name", &self.name)
            .field("endpoint", &self.endpoint)
            .field("scope", &self.scope)
            .field("capabilities", &self.capabilities)
            .field("state", &self.state)
            .finish()
    }
}

impl LlamaCppAdapter {
    /// An adapter for one server, owning the workload with this scope key.
    pub fn new(
        name: impl Into<String>,
        endpoint: impl Into<String>,
        scope: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            endpoint: endpoint.into(),
            scope: scope.into(),
            capabilities: AdapterCapabilities::observation_only(),
            state: Mutex::new(Observation::empty()),
        }
    }

    /// State the capability set, for example from the registry.
    pub fn with_capabilities(mut self, capabilities: AdapterCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// The capability set a probe of these endpoints establishes.
    ///
    /// Observation is the implementation ceiling, not the upstream API ceiling.
    pub fn capabilities_from_probes(models: &Response, props: &Response) -> AdapterCapabilities {
        let list = classify(models).is_usable() && parse_models(&models.body).is_ok();
        let info = matches!(models.status, 404 | 405 | 501)
            && classify(props).is_usable()
            && parse_props(&props.body).is_ok_and(|(sleeping, path)| {
                sleeping.is_some() && path.is_some_and(|path| !path.is_empty())
            });
        AdapterCapabilities::from_probe(
            CAPABILITY_PROBE_VERSION,
            [
                (AdapterCapability::Discovery, CapabilityStateProbe::ok()),
                (
                    AdapterCapability::Inventory,
                    if list || info {
                        CapabilityStateProbe::ok()
                    } else {
                        CapabilityStateProbe::unavailable(
                            "neither the model list nor the model info answered",
                        )
                    },
                ),
                (
                    AdapterCapability::Activity,
                    if list || info {
                        CapabilityStateProbe::ok()
                    } else {
                        CapabilityStateProbe::unavailable(
                            "the server reports no state to read activity from",
                        )
                    },
                ),
            ],
        )
    }

    fn state(&self) -> MutexGuard<'_, Observation> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Whether the server answered with a model list last tick.
    pub fn is_fresh(&self) -> bool {
        self.state().fresh
    }

    /// Why the last reading could not be used, when it could not.
    pub fn last_error(&self) -> Option<String> {
        self.state().last_error.clone()
    }

    fn owns_scope(&self, workload: &Workload) -> bool {
        workload.id.scope == self.scope
    }
}

/// A tiny alias so the probe readings below read as states, not functions.
type CapabilityStateProbe = crate::capability::CapabilityState;

impl CapabilityStateProbe {
    fn ok() -> Self {
        crate::capability::CapabilityState::Available
    }

    fn unavailable(reason: &str) -> Self {
        crate::capability::CapabilityState::Unavailable {
            reason: reason.to_string(),
        }
    }
}

impl WorkloadAdapter for LlamaCppAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> ProviderKind {
        ProviderKind::LlamaCpp
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn capabilities(&self) -> &AdapterCapabilities {
        &self.capabilities
    }

    fn owns(&self, workload: &Workload) -> bool {
        self.owns_scope(workload)
    }

    fn discover(&self) -> Vec<ProbeRequest> {
        if !self.capabilities.observes(AdapterCapability::Discovery) {
            return Vec::new();
        }
        // Exactly the endpoints that are exempt from the server's idle timer,
        // so observation neither wakes a sleeping model nor resets its clock.
        vec![
            ProbeRequest::get(format!("{}{HEALTH_PATH}", self.endpoint)),
            ProbeRequest::get(format!("{}{MODELS_PATH}", self.endpoint)),
            ProbeRequest::get(format!("{}{PROPS_PATH}", self.endpoint)),
        ]
    }

    fn observe(&self, outcomes: &[ProbeOutcome], at_ms: u64) -> Vec<String> {
        if !self.capabilities.observes(AdapterCapability::Discovery) {
            return Vec::new();
        }
        let mut notes = Vec::new();
        let mut health: Option<Response> = None;
        let mut models: Option<Response> = None;
        let mut props: Option<Response> = None;
        let mut v1: Option<Response> = None;
        let mut errors: Vec<String> = Vec::new();

        for outcome in outcomes {
            let Some(path) = outcome.request.url.strip_prefix(self.endpoint.as_str()) else {
                continue;
            };
            match &outcome.result {
                Ok(response) => match path {
                    HEALTH_PATH => health = Some(response.clone()),
                    MODELS_PATH => models = Some(response.clone()),
                    PROPS_PATH => props = Some(response.clone()),
                    V1_MODELS_PATH => v1 = Some(response.clone()),
                    other => notes.push(format!(
                        "provider {} answered an unexpected path {other}",
                        self.name
                    )),
                },
                Err(error) => {
                    let reason = error.to_string();
                    // A refused or missing endpoint is reported, not guessed.
                    notes.push(format!(
                        "provider {} could not be reached at {path}: {reason}",
                        self.name
                    ));
                    errors.push(reason);
                }
            }
        }

        let mut state = self.state();
        let previous_idle = state.idle_since_ms;
        *state = Observation::empty();
        state.observed_at_ms = Some(at_ms);
        state.last_error = None;
        state.fresh = false;

        if let Some(response) = health {
            state.health_ok = classify(&response).is_usable();
        } else {
            state.health_ok = false;
        }

        let mut read_anything = false;
        let mut single_surface = false;
        if let Some(response) = models {
            let capability = classify(&response);
            if capability.is_usable() {
                match parse_models(&response.body) {
                    Ok(list) => {
                        state.models = list;
                        state.router = true;
                        read_anything = true;
                        // A failed load is a failure the server reported, not a
                        // stale success, so it is carried as an error reason.
                        if state.models.iter().any(|m| m.status == ModelStatus::Failed) {
                            state.last_error =
                                Some("the router reported a model that failed to load".to_string());
                        }
                    }
                    Err(reason) => {
                        state.last_error = Some(reason);
                    }
                }
            } else if matches!(response.status, 404 | 405 | 501) {
                // An absent router surface allows a properties-only fallback.
                single_surface = true;
            } else {
                state.last_error = Some(format!(
                    "the model list answered {} and could not be read",
                    response.status
                ));
            }
        } else if let Some(reason) = errors.first() {
            state.last_error = Some(reason.clone());
        }

        // Never use /v1/models: it can wake or reset the native idle timer.
        let _ = v1;

        if let Some(response) = props {
            if classify(&response).is_usable() {
                match parse_props(&response.body) {
                    Ok((sleeping, model_path)) => {
                        state.sleeping = sleeping;
                        state.model_path = model_path;
                        if single_surface && state.last_error.is_none() {
                            if let (Some(sleeping), Some(path)) =
                                (state.sleeping, state.model_path.clone())
                            {
                                if !path.is_empty() {
                                    state.models = vec![ReportedModel {
                                        id: path,
                                        status: if sleeping {
                                            ModelStatus::Sleeping
                                        } else {
                                            ModelStatus::Loaded
                                        },
                                        size_bytes: None,
                                    }];
                                    read_anything = true;
                                }
                            }
                        }
                    }
                    Err(reason) => {
                        if state.last_error.is_none() {
                            state.last_error = Some(reason);
                        }
                    }
                }
            }
        }

        state.fresh = read_anything;
        // A failed load, an unknown status, or a failed read all hold
        // attention: none of them may be read as retirement, and a reading that
        // cannot prove the opposite is never idle.
        let holds_nothing = !state.fresh
            || state.holding_anything()
            || state.last_error.is_some()
            || state
                .models
                .iter()
                .any(|model| model.status == ModelStatus::Failed);
        if holds_nothing {
            state.idle_since_ms = None;
        } else {
            state.idle_since_ms = Some(previous_idle.unwrap_or(at_ms).min(at_ms));
        }
        if let Some(reason) = &state.last_error {
            notes.push(format!(
                "provider {} observation failed: {reason}",
                self.name
            ));
        }
        notes
    }

    fn inventory(&self) -> Vec<ManagedResource> {
        if !self.capabilities.observes(AdapterCapability::Inventory) {
            return Vec::new();
        }
        let state = self.state();
        if !state.fresh {
            return Vec::new();
        }
        state
            .models
            .iter()
            .map(|model| ManagedResource {
                name: model.id.clone(),
                host_ram_bytes: None,
                device_ram_bytes: None,
                idle: matches!(model.status, ModelStatus::Unloaded | ModelStatus::Sleeping),
            })
            .collect()
    }

    fn inventory_is_current(&self) -> bool {
        self.capabilities.observes(AdapterCapability::Inventory) && self.state().fresh
    }

    fn activity(&self, workload: &Workload) -> ActivityEvidence {
        if !self.owns_scope(workload) || !self.capabilities.observes(AdapterCapability::Activity) {
            return ActivityEvidence::Unknown;
        }
        let state = self.state();
        if !state.fresh {
            return ActivityEvidence::Unknown;
        }
        if state.models.iter().any(|model| {
            matches!(
                model.status,
                ModelStatus::Loading | ModelStatus::Downloading
            )
        }) {
            return ActivityEvidence::Active;
        }
        if state.holding_anything() || state.reports_failure() {
            return ActivityEvidence::Unknown;
        }
        match state.idle_since_ms {
            Some(since_ms) => ActivityEvidence::Idle { since_ms },
            None => ActivityEvidence::Unknown,
        }
    }

    fn operation(&self, action: ActionKind, workload: &Workload) -> Option<AdapterOperation> {
        if action != ActionKind::UnloadIdleModel || !self.owns_scope(workload) {
            return None;
        }
        // Only a configured or probed capability and a successful reading may
        // act: neither alone is enough, so a failed probe cannot trigger a write.
        if !self.capabilities.supports(AdapterCapability::UnloadModel) {
            return None;
        }
        let state = self.state();
        if !state.fresh {
            return None;
        }
        // Router unload is per model and confirmed by that model's own status in
        // the same listing. Only a router observation may act: single-model
        // servers expose no documented manual unload, so they are never
        // touched here even when their inventory reads Loaded.
        if !state.router {
            return None;
        }
        let target = state
            .models
            .iter()
            .find(|model| model.status == ModelStatus::Loaded)?;
        let body = format!(
            "{{\"model\":{}}}",
            serde_json::to_string(&target.id).unwrap_or_else(|_| "\"\"".to_string())
        );
        Some(AdapterOperation {
            kind: action,
            request: OperationRequest::post(format!("{}{UNLOAD_PATH}", self.endpoint), body),
            // A logical unload: no measured host or device delta is claimed, and
            // the unloaded status is not a measurement of reclaimed memory.
            expected_host_ram_delta: 0,
            expected_device_ram_delta: 0,
            reversible: true,
            note: format!(
                "unloading {} releases its loaded weights; the next request that uses it reloads it at a cold-start cost, and the unloaded status is not a measurement of memory reclaimed",
                target.id
            ),
            confirmation: Confirmation::model_status(
                ProbeRequest::get(format!("{}{MODELS_PATH}", self.endpoint)),
                target.id.clone(),
                ModelStatus::Unloaded.as_str(),
            ),
        })
    }
}

impl Observation {
    /// Whether the server is currently holding or doing anything at all.
    ///
    /// Loading counts, a failed load counts as holding attention, and a model
    /// the server put to sleep already released its memory, so only
    /// unloaded power the idle reading.
    fn holding_anything(&self) -> bool {
        self.models.iter().any(|model| {
            matches!(
                model.status,
                ModelStatus::Loaded
                    | ModelStatus::Loading
                    | ModelStatus::Downloading
                    | ModelStatus::Unknown
            )
        })
    }

    /// Whether the server reported a failure it has not cleared.
    ///
    /// This is the state a failed load, an unknown status, or an unreadable
    /// answer leaves behind. It is attention rather than retirement, and it is
    /// reported as a reason rather than folded into "unknown".
    fn reports_failure(&self) -> bool {
        self.last_error.is_some()
            || self
                .models
                .iter()
                .any(|model| model.status == ModelStatus::Failed)
    }
}

/// The router's `GET /models` answer.
#[derive(Deserialize)]
struct ModelsBody {
    data: Vec<ModelsEntry>,
}

#[derive(Deserialize)]
struct ModelsEntry {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    status: Option<StatusBody>,
}

#[derive(Deserialize)]
struct StatusBody {
    #[serde(default)]
    value: Option<String>,
}

fn parse_models(body: &str) -> Result<Vec<ReportedModel>, String> {
    let parsed: ModelsBody = serde_json::from_str(body).map_err(|err| format!("{err}"))?;
    let mut models = Vec::new();
    for entry in parsed.data {
        let id = entry
            .id
            .filter(|id| !id.is_empty())
            .ok_or_else(|| "model entry has no nonempty id".to_string())?;
        let status = entry
            .status
            .and_then(|status| status.value)
            .as_deref()
            .map_or(ModelStatus::Unknown, ModelStatus::from_wire);
        models.push(ReportedModel {
            id,
            status,
            size_bytes: entry.size,
        });
    }
    Ok(models)
}

impl ModelStatus {
    /// Map a wire status onto the set above.
    ///
    /// Unrecognized values remain unknown rather than becoming idle or failed.
    fn from_wire(value: &str) -> Self {
        match value {
            "loaded" => ModelStatus::Loaded,
            "loading" => ModelStatus::Loading,
            "downloading" => ModelStatus::Downloading,
            "unloaded" => ModelStatus::Unloaded,
            "sleeping" => ModelStatus::Sleeping,
            "failed" => ModelStatus::Failed,
            _ => ModelStatus::Unknown,
        }
    }
}

/// The `GET /props` answer, read only for the two fields that matter.
#[derive(Deserialize)]
struct PropsBody {
    #[serde(default)]
    is_sleeping: Option<bool>,
    #[serde(default)]
    model_path: Option<String>,
}

fn parse_props(body: &str) -> Result<(Option<bool>, Option<String>), String> {
    let parsed: PropsBody = serde_json::from_str(body).map_err(|err| format!("{err}"))?;
    Ok((parsed.is_sleeping, parsed.model_path))
}

/// The routes this adapter can build, in one place so the set is auditable.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory::{ControlLevel, MemoryAccounting, WorkloadId};

    fn workload(scope: &str) -> Workload {
        Workload {
            id: WorkloadId {
                scope: scope.to_string(),
                generation: 1,
            },
            name: scope.to_string(),
            pids: vec![100],
            accounting: MemoryAccounting::Cgroup {
                current_bytes: Some(1024),
            },
            control: ControlLevel::Eligible,
            limit_bytes: None,
            ancestor_bytes: None,
        }
    }

    const SCOPE: &str = "unit:llama.service";
    const ENDPOINT: &str = "http://127.0.0.1:8080";

    fn adapter() -> LlamaCppAdapter {
        LlamaCppAdapter::new("llama", ENDPOINT, SCOPE).with_capabilities(
            AdapterCapabilities::configured(&[
                AdapterCapability::Discovery,
                AdapterCapability::Inventory,
                AdapterCapability::Activity,
            ]),
        )
    }

    fn ok(body: &str) -> Response {
        Response::new(200, body)
    }

    /// Feed one answer to the matching probe, by URL path.
    fn observe_all(adapter: &LlamaCppAdapter, at_ms: u64, answers: Vec<(&str, Response)>) {
        let outcomes: Vec<ProbeOutcome> = answers
            .into_iter()
            .map(|(path, response)| ProbeOutcome {
                request: ProbeRequest::get(format!("{ENDPOINT}{path}")),
                result: Ok(response),
            })
            .collect();
        adapter.observe(&outcomes, at_ms);
    }

    #[test]
    fn a_router_unload_is_confirmed_by_the_exact_model_id_and_status() {
        let adapter = LlamaCppAdapter::new("llama", ENDPOINT, SCOPE).with_capabilities(
            AdapterCapabilities::configured(&[
                AdapterCapability::Discovery,
                AdapterCapability::Inventory,
                AdapterCapability::Activity,
                AdapterCapability::UnloadModel,
            ]),
        );
        observe_all(
            &adapter,
            1_000,
            vec![
                (HEALTH_PATH, ok(r#"{"status":"ok"}"#)),
                (
                    MODELS_PATH,
                    ok(r#"{"data":[{"id":"llama","status":{"value":"loaded"}}]}"#),
                ),
            ],
        );
        let operation = adapter
            .operation(ActionKind::UnloadIdleModel, &workload(SCOPE))
            .expect("a loaded router model can be unloaded");
        assert_eq!(operation.request.url, format!("{ENDPOINT}{UNLOAD_PATH}"));
        assert!(operation
            .request
            .body
            .as_deref()
            .unwrap_or_default()
            .contains("llama"));
        // The confirmation reads the exact model's own status, so a sibling with
        // a shared substring cannot cross-confirm it.
        assert!(operation
            .confirmation
            .confirms(r#"{"data":[{"id":"llama","status":{"value":"unloaded"}}]}"#));
        assert!(!operation.confirmation.confirms(
            r#"{"data":[{"id":"llama-other","status":{"value":"unloaded"}},{"id":"llama","status":{"value":"loaded"}}]}"#
        ));
    }

    #[test]
    fn a_single_model_server_offers_no_unload() {
        let adapter = LlamaCppAdapter::new("llama", ENDPOINT, SCOPE).with_capabilities(
            AdapterCapabilities::configured(&[
                AdapterCapability::Discovery,
                AdapterCapability::Inventory,
                AdapterCapability::Activity,
                AdapterCapability::UnloadModel,
            ]),
        );
        // A properties-only answer, with the router list absent, is a single
        // model server with no documented manual unload.
        observe_all(
            &adapter,
            1_000,
            vec![
                (HEALTH_PATH, ok(r#"{"status":"ok"}"#)),
                (MODELS_PATH, Response::new(404, "")),
                (PROPS_PATH, ok(r#"{"model_path":"/models/a.gguf"}"#)),
            ],
        );
        assert!(adapter
            .operation(ActionKind::UnloadIdleModel, &workload(SCOPE))
            .is_none());
    }

    #[test]
    fn an_awake_single_model_server_is_never_sent_an_unload() {
        let adapter = LlamaCppAdapter::new("llama", ENDPOINT, SCOPE).with_capabilities(
            AdapterCapabilities::configured(&[
                AdapterCapability::Discovery,
                AdapterCapability::Inventory,
                AdapterCapability::Activity,
                AdapterCapability::UnloadModel,
            ]),
        );
        // An awake single-model server builds inventory (a Loaded entry with
        // the router flag unset), but the router surface it would confirm
        // against does not exist, so no unload may be offered.
        observe_all(
            &adapter,
            1_000,
            vec![
                (HEALTH_PATH, ok(r#"{"status":"ok"}"#)),
                (MODELS_PATH, Response::new(404, "")),
                (
                    PROPS_PATH,
                    ok(r#"{"is_sleeping":false,"model_path":"/models/a.gguf"}"#),
                ),
            ],
        );
        assert_eq!(adapter.inventory().len(), 1);
        assert!(adapter
            .operation(ActionKind::UnloadIdleModel, &workload(SCOPE))
            .is_none());
    }

    #[test]
    fn it_probes_only_idle_timer_exempt_endpoints() {
        let adapter = adapter();
        let urls: Vec<String> = adapter
            .discover()
            .into_iter()
            .map(|request| request.url)
            .collect();
        assert_eq!(
            urls,
            vec![
                format!("{ENDPOINT}{HEALTH_PATH}"),
                format!("{ENDPOINT}{MODELS_PATH}"),
                format!("{ENDPOINT}{PROPS_PATH}"),
            ]
        );
    }

    #[test]
    fn automation_never_turns_loaded_residency_into_confirmed_idleness() {
        let adapter = adapter().with_capabilities(AdapterCapabilities::configured(&[
            AdapterCapability::Discovery,
            AdapterCapability::Inventory,
            AdapterCapability::Activity,
            AdapterCapability::UnloadModel,
        ]));
        for at in [1_000, 61_000, 121_000] {
            observe_all(
                &adapter,
                at,
                vec![
                    (HEALTH_PATH, ok(r#"{"status":"ok"}"#)),
                    (
                        MODELS_PATH,
                        ok(r#"{"data":[{"id":"m1","status":{"value":"loaded"}}]}"#),
                    ),
                ],
            );
            let evidence = adapter.activity(&workload(SCOPE));
            assert_eq!(evidence, ActivityEvidence::Unknown);
            assert!(!evidence.is_confirmed_idle(at, 30_000));
        }
        // The protocol operation remains implemented, but no idle evidence is
        // fabricated merely to make that low-level API reachable in policy.
        assert!(adapter
            .operation(ActionKind::UnloadIdleModel, &workload(SCOPE))
            .is_some());
    }

    #[test]
    fn a_router_reports_its_models_and_their_status() {
        let adapter = adapter();
        observe_all(
            &adapter,
            1_000,
            vec![
                (HEALTH_PATH, ok(r#"{"status":"ok"}"#)),
                (
                    MODELS_PATH,
                    ok(r#"{"data":[{"id":"m1","size":4096,"status":{"value":"loaded"}}]}"#),
                ),
                (
                    PROPS_PATH,
                    ok(r#"{"is_sleeping":false,"model_path":"/models/m1.gguf"}"#),
                ),
                (V1_MODELS_PATH, ok(r#"{"data":[]}"#)),
            ],
        );
        assert!(adapter.is_fresh());
        let inventory = adapter.inventory();
        assert_eq!(inventory.len(), 1);
        assert_eq!(inventory[0].name, "m1");
        assert!(!inventory[0].idle);
        // Loaded is not busy, but it is not idle either: a listed model can be
        // serving, and nothing here proves it is not.
        assert_eq!(
            adapter.activity(&workload(SCOPE)),
            ActivityEvidence::Unknown
        );
        // And no operation exists, in any mode.
        assert!(adapter
            .operation(ActionKind::UnloadIdleModel, &workload(SCOPE))
            .is_none());
        assert!(adapter
            .operation(ActionKind::SleepModel, &workload(SCOPE))
            .is_none());
    }

    #[test]
    fn a_sleeping_model_is_idle_and_holding_nothing() {
        let adapter = adapter();
        observe_all(
            &adapter,
            1_000,
            vec![
                (HEALTH_PATH, ok(r#"{"status":"ok"}"#)),
                (
                    MODELS_PATH,
                    ok(r#"{"data":[{"id":"m1","status":{"value":"unloaded"}}]}"#),
                ),
                (PROPS_PATH, ok(r#"{"is_sleeping":true}"#)),
                (V1_MODELS_PATH, ok(r#"{"data":[]}"#)),
            ],
        );
        assert_eq!(
            adapter.activity(&workload(SCOPE)),
            ActivityEvidence::Idle { since_ms: 1_000 }
        );
        let inventory = adapter.inventory();
        assert_eq!(inventory.len(), 1);
        assert!(inventory[0].idle);
    }

    #[test]
    fn a_failed_load_is_a_reported_error_not_an_idle_model() {
        let adapter = adapter();
        observe_all(
            &adapter,
            1_000,
            vec![
                (HEALTH_PATH, ok(r#"{"status":"ok"}"#)),
                (
                    MODELS_PATH,
                    ok(r#"{"data":[{"id":"m1","status":{"value":"failed","exit_code":1}}]}"#),
                ),
                (PROPS_PATH, ok(r#"{"is_sleeping":false}"#)),
                (V1_MODELS_PATH, ok(r#"{"data":[]}"#)),
            ],
        );
        assert!(adapter.last_error().is_some());
        // A failed load is attention, not idleness: something went wrong and
        // the operator needs to know the workload is not quietly retired.
        assert_eq!(
            adapter.activity(&workload(SCOPE)),
            ActivityEvidence::Unknown
        );
    }

    #[test]
    fn a_router_listing_an_unknown_status_is_not_an_idle_model() {
        let adapter = adapter();
        observe_all(
            &adapter,
            1_000,
            vec![
                (HEALTH_PATH, ok(r#"{"status":"ok"}"#)),
                (
                    MODELS_PATH,
                    ok(r#"{"data":[{"id":"m1","status":{"value":"restarting"}}]}"#),
                ),
                (PROPS_PATH, ok(r#"{"is_sleeping":false}"#)),
                (V1_MODELS_PATH, ok(r#"{"data":[]}"#)),
            ],
        );
        // Unknown states are failures, which hold attention, so the workload
        // is never treated as a candidate for relief it did not earn.
        assert_eq!(
            adapter.activity(&workload(SCOPE)),
            ActivityEvidence::Unknown
        );
        let inventory = adapter.inventory();
        assert_eq!(inventory.len(), 1);
        assert!(!inventory[0].idle);
    }

    #[test]
    fn a_single_model_server_is_read_from_properties() {
        let adapter = adapter();
        observe_all(
            &adapter,
            1_000,
            vec![
                (HEALTH_PATH, ok(r#"{"status":"ok"}"#)),
                (MODELS_PATH, Response::new(404, "{}")),
                (
                    PROPS_PATH,
                    ok(r#"{"is_sleeping":false,"model_path":"/models/m1.gguf"}"#),
                ),
                (
                    V1_MODELS_PATH,
                    ok(r#"{"data":[{"id":"/models/m1.gguf","meta":{"size":4096}}]}"#),
                ),
            ],
        );
        assert!(adapter.is_fresh());
        let inventory = adapter.inventory();
        assert_eq!(inventory.len(), 1);
        assert_eq!(inventory[0].name, "/models/m1.gguf");
        assert_eq!(inventory[0].host_ram_bytes, None);
        assert_eq!(
            adapter.activity(&workload(SCOPE)),
            ActivityEvidence::Unknown
        );
    }

    #[test]
    fn an_unreadable_server_reports_unknown_rather_than_idle() {
        let adapter = adapter();
        observe_all(
            &adapter,
            1_000,
            vec![
                (HEALTH_PATH, ok(r#"{"status":"ok"}"#)),
                (MODELS_PATH, ok("not json")),
                (PROPS_PATH, ok("not json")),
                (V1_MODELS_PATH, ok("not json")),
            ],
        );
        assert!(adapter.last_error().is_some());
        assert_eq!(
            adapter.activity(&workload(SCOPE)),
            ActivityEvidence::Unknown
        );
        assert!(adapter.inventory().is_empty());
        assert!(!adapter.inventory_is_current());
    }

    #[test]
    fn a_read_failure_clears_a_previous_success() {
        let adapter = adapter();
        observe_all(
            &adapter,
            1_000,
            vec![
                (HEALTH_PATH, ok(r#"{"status":"ok"}"#)),
                (
                    MODELS_PATH,
                    ok(r#"{"data":[{"id":"m1","status":{"value":"loaded"}}]}"#),
                ),
                (PROPS_PATH, ok(r#"{"is_sleeping":false}"#)),
                (V1_MODELS_PATH, ok(r#"{"data":[]}"#)),
            ],
        );
        assert!(adapter.is_fresh());
        let outcomes = vec![ProbeOutcome {
            request: ProbeRequest::get(format!("{ENDPOINT}{MODELS_PATH}")),
            result: Err(crate::adapter::probe::ProbeError::Timeout { timeout_ms: 1_000 }),
        }];
        adapter.observe(&outcomes, 2_000);
        assert!(adapter.last_error().is_some());
        assert_eq!(
            adapter.activity(&workload(SCOPE)),
            ActivityEvidence::Unknown
        );
        assert!(adapter.inventory().is_empty());
    }

    #[test]
    fn it_owns_only_its_scope() {
        let adapter = adapter();
        assert!(adapter.owns(&workload(SCOPE)));
        assert!(!adapter.owns(&workload("unit:other.service")));
        assert_eq!(
            adapter.activity(&workload("unit:other.service")),
            ActivityEvidence::Unknown
        );
    }

    #[test]
    fn capabilities_stop_at_observation() {
        let capabilities = LlamaCppAdapter::capabilities_from_probes(
            &Response::new(404, "{}"),
            &Response::new(404, "{}"),
        );
        assert!(capabilities.supports(AdapterCapability::Discovery));
        // A 404 on both surfaces means neither answered, so nothing is
        // observed: an absent endpoint is not a reading of zero.
        assert!(!capabilities.observes(AdapterCapability::Inventory));
        assert!(!capabilities.supports(AdapterCapability::Inventory));
        assert!(!capabilities.supports(AdapterCapability::UnloadModel));
        assert!(!capabilities.supports(AdapterCapability::SleepModel));
        assert!(!capabilities.supports(AdapterCapability::WakeModel));
        assert!(!capabilities.supports(AdapterCapability::ModelAdministration));
    }

    #[test]
    fn an_unknown_status_is_not_failed_or_idle() {
        assert_eq!(
            ModelStatus::from_wire("unloading_for_dinner"),
            ModelStatus::Unknown
        );
        assert_eq!(ModelStatus::from_wire("sleeping"), ModelStatus::Sleeping);
        assert_eq!(ModelStatus::Loaded.as_str(), "loaded");
    }

    #[test]
    fn absent_unauthorized_and_props_only_cannot_refresh_router_inventory() {
        for status in [401, 403, 404, 429, 501] {
            let adapter = adapter();
            observe_all(
                &adapter,
                100,
                vec![(
                    MODELS_PATH,
                    ok(r#"{"data":[{"id":"m1","status":{"value":"unloaded"}}]}"#),
                )],
            );
            assert!(adapter.inventory_is_current());
            observe_all(
                &adapter,
                200,
                vec![
                    (MODELS_PATH, Response::new(status, "{}")),
                    (PROPS_PATH, ok(r#"{"is_sleeping":true}"#)),
                ],
            );
            assert!(!adapter.inventory_is_current());
            assert_eq!(
                adapter.activity(&workload(SCOPE)),
                ActivityEvidence::Unknown
            );
            observe_all(
                &adapter,
                300,
                vec![(PROPS_PATH, ok(r#"{"is_sleeping":true,"model_path":"m1"}"#))],
            );
            assert!(!adapter.inventory_is_current());
        }
    }

    #[test]
    fn missing_status_and_missing_list_are_not_empty_idle_inventory() {
        let adapter = adapter();
        observe_all(
            &adapter,
            100,
            vec![(MODELS_PATH, ok(r#"{"data":[{"id":"m1"}]}"#))],
        );
        assert_eq!(
            adapter.activity(&workload(SCOPE)),
            ActivityEvidence::Unknown
        );
        assert!(!adapter.inventory()[0].idle);
        observe_all(&adapter, 200, vec![(MODELS_PATH, ok("{}"))]);
        assert!(!adapter.inventory_is_current());
    }

    #[test]
    fn router_can_change_to_sleeping_single_model_without_sticky_mode() {
        let adapter = adapter();
        observe_all(
            &adapter,
            100,
            vec![(
                MODELS_PATH,
                ok(r#"{"data":[{"id":"router-model","status":{"value":"loaded"}}]}"#),
            )],
        );
        observe_all(
            &adapter,
            200,
            vec![
                (MODELS_PATH, Response::new(404, "{}")),
                (
                    PROPS_PATH,
                    ok(r#"{"is_sleeping":true,"model_path":"single.gguf"}"#),
                ),
            ],
        );
        assert!(adapter.inventory_is_current());
        assert_eq!(adapter.inventory()[0].name, "single.gguf");
        assert!(adapter.inventory()[0].idle);
        assert_eq!(
            adapter.activity(&workload(SCOPE)),
            ActivityEvidence::Idle { since_ms: 200 }
        );
    }

    #[test]
    fn malformed_success_cannot_grant_inventory_capability() {
        let capabilities = LlamaCppAdapter::capabilities_from_probes(
            &ok("{}"),
            &ok(r#"{"is_sleeping":true,"model_path":"m1"}"#),
        );
        assert!(!capabilities.supports(AdapterCapability::Inventory));
    }

    #[test]
    fn unknown_refresh_breaks_continuous_idle_evidence() {
        let adapter = adapter();
        let idle = r#"{"data":[{"id":"m1","status":{"value":"unloaded"}}]}"#;
        observe_all(&adapter, 100, vec![(MODELS_PATH, ok(idle))]);
        observe_all(&adapter, 200, vec![]);
        observe_all(&adapter, 300, vec![(MODELS_PATH, ok(idle))]);
        assert_eq!(
            adapter.activity(&workload(SCOPE)),
            ActivityEvidence::Idle { since_ms: 300 }
        );
    }
}
