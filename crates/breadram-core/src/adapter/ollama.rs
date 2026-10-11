//! The Ollama adapter.
//!
//! Ollama keeps a model in memory for `keep_alive` after the last request, so a
//! quiet host can be holding several gigabytes of model that nothing is using.
//! This adapter reads that state and offers one bounded, verified operation: ask
//! the service to release a loaded model, then confirm it is gone.
//!
//! What it does, precisely:
//!
//! - **Inventory** comes from `GET /api/ps`, the documented list of loaded
//!   models. Each entry carries the total size and the part held in device
//!   memory, and the two are reported separately. The remainder is reported as
//!   model bytes held on the host, never as host RAM usage: a model's file size
//!   is not the memory it costs, and mmap-backed pages are shared with the page
//!   cache.
//! - **Unload** is `POST /api/generate` with `{"model": ..., "keep_alive": 0}`,
//!   the documented release flow, confirmed by a second `GET /api/ps` that must
//!   no longer list the model.
//! - **Observation** is `GET /api/version`, recorded for diagnostics.
//!
//! What it deliberately does not do:
//!
//! - It never pulls a model, deletes a model file, or calls any other endpoint.
//!   The only URLs it can produce are the three above, which is a property of the
//!   code rather than a promise.
//! - It never treats a loaded model's presence, or its expiry, as proof that
//!   nothing is being inferred. A loaded model is [`ActivityEvidence::Active`]
//!   however long its expiry is in the future or past, because a listed model can
//!   still be serving a long request.
//! - It never unloads a model whose keep-alive policy it cannot vouch for. A
//!   model is released only when the service reported an expiry that reads as a
//!   bounded keep-alive and the operator did not pin it. No reported expiry, an
//!   expiry that does not parse, and a residency far beyond any ordinary
//!   keep-alive are all treated as a policy choice to keep the model loaded, and
//!   are named in [`OllamaAdapter::release_refusals`] rather than dropped.
//!
//! The keep-alive rule is the one place a wall clock is read, because expiry is
//! expressed in wall-clock time by the service. Everything that decides whether
//! inference is happening uses monotonic milliseconds from the caller, and the
//! only field of the keep-alive decision that a test cannot control is
//! [`OllamaAdapter::with_reference_time`], which exists so it can.
//!
//! Idleness is therefore proven from the other direction: an empty loaded set,
//! observed continuously, is Ollama's own guarantee that no model memory is held.
//! The idle clock starts at the first observation that saw nothing loaded, and a
//! request reported through [`OllamaAdapter::note_request`] resets it. A gateway
//! that fronts the service should call that hook, and the serving coordination
//! in `crate::serving` makes the same guarantee from request leases.
//!
//! Supported versions are not gated on a version number. A build is supported
//! when `GET /api/ps` answers with a parseable body, and one that does not is
//! reported unsupported with the reason, which is the observable fact that
//! actually matters. The reported version is kept for diagnostics.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

use serde::Deserialize;

use crate::capability::CapabilityState;
use crate::config::{ActionKind, ProviderKind};
use crate::inventory::Workload;
use crate::policy::activity::ActivityEvidence;
use crate::serving::{LeaseId, Leases, RequestMeta, TrackingCompleteness};

use super::capability::{AdapterCapabilities, AdapterCapability, CAPABILITY_PROBE_VERSION};
use super::probe::{classify, ProbeRequest, Response};
use super::{AdapterOperation, Confirmation, ManagedResource, OperationRequest, WorkloadAdapter};

/// The path that lists loaded models.
pub const PS_PATH: &str = "/api/ps";
/// The path that reports the service version.
pub const VERSION_PATH: &str = "/api/version";
/// The path that performs the documented unload.
pub const GENERATE_PATH: &str = "/api/generate";

/// The longest reported residency that still reads as a bounded keep-alive.
///
/// A model the service says it will unload within this window was loaded with
/// an ordinary keep-alive, so releasing it early is within the adapter's
/// remit. Beyond it, the residency is a deliberate choice - a keep-alive of days,
/// or the indefinite one a service reports as a date decades away - so the model
/// is treated as pinned. The conservative direction is to leave memory in use
/// rather than to break a policy the operator asked for.
pub const DEFAULT_MAX_BOUNDED_KEEP_ALIVE_MS: u64 = 24 * 60 * 60 * 1_000;

/// Why an Ollama reading could not be used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OllamaError {
    /// The service answered but the endpoint is not exposed.
    #[error("the Ollama service does not expose {path}: {reason}")]
    Unsupported {
        /// The path that was asked for.
        path: &'static str,
        /// Why, in the service's words or the probe's.
        reason: String,
    },
    /// The service answered with something that is not the documented shape.
    #[error("{path} did not answer with the documented shape: {reason}")]
    Malformed {
        /// The path that was asked for.
        path: &'static str,
        /// What was wrong.
        reason: String,
    },
}

/// One model the service reports as loaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedModel {
    /// The model name, as the service reports it.
    pub name: String,
    /// Total size in bytes, host and device together.
    pub size_bytes: u64,
    /// The part held in device memory, in bytes.
    pub size_vram_bytes: u64,
    /// The service's own expiry, when it reports one.
    ///
    /// Recorded for diagnostics only. It is never used as evidence that nothing
    /// is being inferred.
    pub expires_at: Option<String>,
    /// Whether the operator pinned this model.
    pub pinned: bool,
}

impl LoadedModel {
    /// The model bytes held on the host, which is the part not in device memory.
    ///
    /// An estimate of model bytes, not a measurement of host RAM usage: a
    /// memory-mapped model shares its pages with the page cache.
    pub fn host_model_bytes(&self) -> u64 {
        self.size_bytes.saturating_sub(self.size_vram_bytes)
    }

    /// Why this model must not be released automatically, or `None` when it may
    /// be.
    ///
    /// Every answer other than "the service reported a bounded keep-alive and the
    /// operator did not pin it" is a refusal, and each names its own cause so it
    /// can be explained rather than silently skipped.
    pub fn release_refusal(&self, now_unix_ms: u64, horizon_ms: u64) -> Option<&'static str> {
        if self.pinned {
            return Some("the operator pinned this model");
        }
        let Some(expires_at) = self.expires_at.as_deref() else {
            // Ollama does not report the keep-alive a model was loaded with, so a
            // model it does not say it will unload is treated as kept in place.
            return Some("the service did not report when this model will be unloaded");
        };
        let Some(expires_at_ms) = parse_rfc3339_ms(expires_at) else {
            return Some("the reported expiry could not be read as a timestamp");
        };
        if expires_at_ms <= now_unix_ms {
            // Already due to go: releasing it now changes nothing.
            return None;
        }
        if expires_at_ms - now_unix_ms > horizon_ms {
            return Some("the reported expiry is far beyond an ordinary keep-alive");
        }
        None
    }

    /// Whether this model may be released automatically.
    pub fn is_unloadable(&self, now_unix_ms: u64, horizon_ms: u64) -> bool {
        self.release_refusal(now_unix_ms, horizon_ms).is_none()
    }
}

/// The adapter's view of one Ollama service.
#[derive(Debug)]
struct Observation {
    version: Option<String>,
    loaded: BTreeMap<String, LoadedModel>,
    /// Whether the last `/api/ps` read succeeded. A failed read proves nothing,
    /// so nothing is released on the strength of the previous one.
    fresh: bool,
    /// When the loaded set was first observed empty, in monotonic milliseconds.
    idle_since_ms: Option<u64>,
    last_error: Option<String>,
    observed_at_ms: Option<u64>,
    /// Requests reported by a gateway in front of the service.
    requests: Leases,
}

/// An adapter for one configured Ollama service.
#[derive(Debug)]
pub struct OllamaAdapter {
    name: String,
    endpoint: String,
    scope: String,
    capabilities: AdapterCapabilities,
    pinned: Vec<String>,
    tracking: TrackingCompleteness,
    /// The wall-clock instant keep-alive policy is read against, in unix
    /// milliseconds. Never used for activity evidence, which is monotonic.
    reference_unix_ms: u64,
    /// How long a reported residency may last and still read as a bounded
    /// keep-alive.
    max_bounded_keep_alive_ms: u64,
    state: Mutex<Observation>,
}

impl OllamaAdapter {
    /// An adapter for one service, owning the workload with this scope key.
    ///
    /// The capability set starts as observation-only, which is the conservative
    /// default: call [`OllamaAdapter::capabilities_from_probes`] after probing and
    /// configure the result, or state the capabilities explicitly.
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
            pinned: Vec::new(),
            tracking: TrackingCompleteness::Unknown,
            reference_unix_ms: now_unix_ms(),
            max_bounded_keep_alive_ms: DEFAULT_MAX_BOUNDED_KEEP_ALIVE_MS,
            state: Mutex::new(Observation {
                version: None,
                loaded: BTreeMap::new(),
                fresh: false,
                idle_since_ms: None,
                last_error: None,
                observed_at_ms: None,
                requests: Leases::new(0),
            }),
        }
    }

    /// Declare how completely requests are tracked, and from when.
    ///
    /// [`TrackingCompleteness::Complete`] means the caller sees every request
    /// that reaches the service, and reports each one through
    /// [`OllamaAdapter::begin_request`] and [`OllamaAdapter::end_request`].
    /// Idleness then comes from those reports, which is the serving coordination
    /// contract, and a loaded model no longer blocks relief: a model can be
    /// loaded and unused at the same time, which is the whole reason Ollama
    /// keeps one resident.
    ///
    /// Without it, the adapter can only prove idleness from an empty model list,
    /// and a loaded model is never treated as idle however long its expiry is.
    /// The start time is required rather than assumed, so a freshly started
    /// adapter is never reported as idle since before it existed.
    pub fn with_tracking(mut self, tracking: TrackingCompleteness, since_ms: u64) -> Self {
        self.tracking = tracking;
        self.state().requests = Leases::new(since_ms);
        self
    }

    /// How completely requests are tracked.
    pub fn tracking(&self) -> TrackingCompleteness {
        self.tracking
    }

    /// State the capability set, for example from
    /// [`OllamaAdapter::capabilities_from_probes`].
    pub fn with_capabilities(mut self, capabilities: AdapterCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Models the operator pinned, which are never released.
    ///
    /// A deployment that loads a model with a long keep-alive should list it
    /// here. It is not inferred: the service does not report the keep-alive.
    pub fn with_pinned_models(mut self, pinned: impl IntoIterator<Item = String>) -> Self {
        self.pinned = pinned.into_iter().collect();
        self
    }

    /// Read keep-alive policy against this instant instead of the current one.
    ///
    /// For tests and for a caller that already has a wall-clock reading. It
    /// affects nothing but the expiry comparison: activity is monotonic and is
    /// never decided by this value.
    pub fn with_reference_time(mut self, unix_ms: u64) -> Self {
        self.reference_unix_ms = unix_ms;
        self
    }

    /// Change how long a reported residency may last and still be treated as an
    /// ordinary keep-alive rather than a pin.
    ///
    /// Raise it to allow releasing a model the operator loaded with a long
    /// keep-alive; lower it to protect more models.
    pub fn with_max_bounded_keep_alive(mut self, keep_alive_ms: u64) -> Self {
        self.max_bounded_keep_alive_ms = keep_alive_ms;
        self
    }

    /// The capability set a probe of `/api/ps` establishes.
    ///
    /// An answering endpoint means the model list and its release flow are
    /// available. A refusal means they are not, and the reason is carried into
    /// the capability state rather than dropped.
    pub fn capabilities_from_probes(ps: &Response) -> AdapterCapabilities {
        let state = classify(ps);
        AdapterCapabilities::from_probe(
            CAPABILITY_PROBE_VERSION,
            [
                (AdapterCapability::Discovery, CapabilityState::Available),
                (AdapterCapability::Inventory, state.clone()),
                (AdapterCapability::Activity, state.clone()),
                (AdapterCapability::UnloadModel, state.clone()),
                (AdapterCapability::ModelAdministration, state),
            ],
        )
    }

    /// A local lock, recovering from a poisoned one rather than panicking.
    fn state(&self) -> MutexGuard<'_, Observation> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The service version last reported, when one was seen.
    pub fn version(&self) -> Option<String> {
        self.state().version.clone()
    }

    /// Why the last reading could not be used, when it could not.
    pub fn last_error(&self) -> Option<String> {
        self.state().last_error.clone()
    }

    /// The models the last successful reading reported as loaded.
    pub fn loaded(&self) -> Vec<LoadedModel> {
        self.state().loaded.values().cloned().collect()
    }

    /// The models that may be released automatically, largest first.
    pub fn unload_targets(&self) -> Vec<LoadedModel> {
        let mut targets: Vec<LoadedModel> = self
            .loaded()
            .into_iter()
            .filter(|model| {
                model.is_unloadable(self.reference_unix_ms, self.max_bounded_keep_alive_ms)
            })
            .collect();
        targets.sort_by_key(|model| std::cmp::Reverse(model.size_bytes));
        targets
    }

    /// Every loaded model that will not be released automatically, with the
    /// reason, so a refusal is explained rather than invisible.
    pub fn release_refusals(&self) -> Vec<(LoadedModel, &'static str)> {
        self.loaded()
            .into_iter()
            .filter_map(|model| {
                model
                    .release_refusal(self.reference_unix_ms, self.max_bounded_keep_alive_ms)
                    .map(|reason| (model, reason))
            })
            .collect()
    }

    /// Record that a request reached the backend, and return its lease.
    ///
    /// A service that has just been asked to do something is not idle, whatever
    /// the model list says, so this also resets the empty-set idle clock. A
    /// gateway in front of the service should report every request this way.
    pub fn begin_request(&self, now_ms: u64) -> LeaseId {
        let mut state = self.state();
        state.idle_since_ms = None;
        state
            .requests
            .admit(RequestMeta::new(self.name.clone()), now_ms)
            .lease()
    }

    /// Record that a request finished.
    pub fn end_request(&self, id: LeaseId, now_ms: u64) {
        self.state().requests.finish(id, now_ms);
    }

    /// Record that a request was cancelled or its client went away.
    pub fn cancel_request(&self, id: LeaseId, now_ms: u64) {
        self.state().requests.cancel(id, now_ms);
    }

    /// Read `GET /api/version`.
    pub fn observe_version(
        &self,
        response: &Response,
        now_ms: u64,
    ) -> Result<Option<String>, OllamaError> {
        let mut state = self.state();
        state.observed_at_ms = Some(now_ms);
        let capability = classify(response);
        if !capability.is_usable() {
            let reason = capability
                .reason()
                .unwrap_or("the version endpoint did not answer")
                .to_string();
            state.last_error = Some(reason.clone());
            return Err(OllamaError::Unsupported {
                path: VERSION_PATH,
                reason,
            });
        }
        let version = parse_version(&response.body).map_err(|reason| {
            state.last_error = Some(reason.clone());
            OllamaError::Malformed {
                path: VERSION_PATH,
                reason,
            }
        })?;
        state.version = Some(version.clone());
        Ok(Some(version))
    }

    /// Read `GET /api/ps` and update the loaded set.
    ///
    /// A read that fails proves nothing, so it clears the loaded view and the
    /// idle clock rather than leaving the previous reading in place to be acted
    /// on. The previous models stay available for diagnostics only through
    /// [`OllamaAdapter::loaded`] before the next failed read.
    pub fn observe_loaded(
        &self,
        response: &Response,
        now_ms: u64,
    ) -> Result<Vec<LoadedModel>, OllamaError> {
        let mut state = self.state();
        state.observed_at_ms = Some(now_ms);
        let capability = classify(response);
        if !capability.is_usable() {
            let reason = capability
                .reason()
                .unwrap_or("the model list did not answer")
                .to_string();
            state.fresh = false;
            state.loaded.clear();
            state.idle_since_ms = None;
            state.last_error = Some(reason.clone());
            return Err(OllamaError::Unsupported {
                path: PS_PATH,
                reason,
            });
        }
        let models = match parse_ps(&response.body) {
            Ok(models) => models,
            Err(reason) => {
                state.fresh = false;
                state.loaded.clear();
                state.idle_since_ms = None;
                state.last_error = Some(reason.clone());
                return Err(OllamaError::Malformed {
                    path: PS_PATH,
                    reason,
                });
            }
        };

        state.loaded = models
            .into_iter()
            .map(|mut model| {
                model.pinned = model.pinned || self.pinned.contains(&model.name);
                (model.name.clone(), model)
            })
            .collect();
        state.fresh = true;
        state.last_error = None;
        if state.loaded.is_empty() {
            // The first observation of an empty set starts the idle clock.
            if state.idle_since_ms.is_none() {
                state.idle_since_ms = Some(now_ms);
            }
        } else {
            state.idle_since_ms = None;
        }
        Ok(state.loaded.values().cloned().collect())
    }

    /// Whether the last `/api/ps` read succeeded.
    pub fn is_fresh(&self) -> bool {
        self.state().fresh
    }

    fn owns_scope(&self, workload: &Workload) -> bool {
        workload.id.scope == self.scope
    }
}

impl WorkloadAdapter for OllamaAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> ProviderKind {
        ProviderKind::Ollama
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
        vec![
            ProbeRequest::get(format!("{}{VERSION_PATH}", self.endpoint)),
            ProbeRequest::get(format!("{}{PS_PATH}", self.endpoint)),
        ]
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
            .loaded
            .values()
            .map(|model| ManagedResource {
                name: model.name.clone(),
                host_ram_bytes: Some(model.host_model_bytes()),
                device_ram_bytes: (model.size_vram_bytes > 0).then_some(model.size_vram_bytes),
                // A loaded model is never reported as idle: being loaded is not
                // evidence that nothing is being inferred.
                idle: false,
            })
            .collect()
    }

    fn activity(&self, workload: &Workload) -> ActivityEvidence {
        if !self.owns_scope(workload) || !self.capabilities.observes(AdapterCapability::Activity) {
            return ActivityEvidence::Unknown;
        }
        let state = self.state();
        if self.tracking == TrackingCompleteness::Complete {
            // Every request is visible, so the leases are the evidence and a
            // loaded model is irrelevant: resident but unused is exactly the
            // state worth relieving.
            return state.requests.evidence();
        }
        if !state.fresh {
            // Never probed, or the last read failed. Unknown is never idle.
            return ActivityEvidence::Unknown;
        }
        if !state.loaded.is_empty() {
            return ActivityEvidence::Active;
        }
        match state.idle_since_ms {
            Some(since_ms) => ActivityEvidence::Idle { since_ms },
            // Empty, but a request has been reported since it became empty.
            None => ActivityEvidence::Unknown,
        }
    }

    fn operation(&self, action: ActionKind, workload: &Workload) -> Option<AdapterOperation> {
        if action != ActionKind::UnloadIdleModel || !self.owns_scope(workload) {
            return None;
        }
        // A configured or probed capability, and a successful reading to act on:
        // neither alone is enough, so a failed probe cannot trigger a write.
        if !self.capabilities.supports(AdapterCapability::UnloadModel) {
            return None;
        }
        if !self.state().fresh {
            return None;
        }
        // One model per action, the largest unloadable one, so each release is
        // separately confirmed and a pinned model is never touched.
        let target = self.unload_targets().into_iter().next()?;
        let host_delta = -(target.host_model_bytes() as i64);
        let device_delta = -(target.size_vram_bytes as i64);
        Some(AdapterOperation {
            kind: action,
            request: OperationRequest::post(
                format!("{}{GENERATE_PATH}", self.endpoint),
                format!(
                    "{{\"model\":{},\"keep_alive\":0}}",
                    json_string(&target.name)
                ),
            ),
            expected_host_ram_delta: host_delta,
            expected_device_ram_delta: device_delta,
            // Reloading is possible but costs a cold start, which the
            // explanation carries rather than hiding.
            reversible: true,
            note: format!(
                "releasing {} frees about {} of host memory now; the next request that uses it pays the model's cold-start load time",
                target.name,
                human_bytes(target.host_model_bytes())
            ),
            confirmation: Confirmation::absent(
                ProbeRequest::get(format!("{}{PS_PATH}", self.endpoint)),
                target.name,
            ),
        })
    }
}

/// The version reported by `GET /api/version`.
fn parse_version(body: &str) -> Result<String, String> {
    #[derive(Deserialize)]
    struct VersionBody {
        version: String,
    }
    serde_json::from_str::<VersionBody>(body)
        .map(|parsed| parsed.version)
        .map_err(|err| format!("{err}"))
}

/// The models reported by `GET /api/ps`.
fn parse_ps(body: &str) -> Result<Vec<LoadedModel>, String> {
    #[derive(Deserialize)]
    struct PsBody {
        #[serde(default)]
        models: Vec<PsModel>,
    }

    #[derive(Deserialize)]
    struct PsModel {
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        model: Option<String>,
        #[serde(default)]
        size: Option<u64>,
        #[serde(default)]
        size_vram: Option<u64>,
        #[serde(default)]
        expires_at: Option<String>,
    }

    let parsed: PsBody = serde_json::from_str(body).map_err(|err| format!("{err}"))?;
    let mut models = Vec::new();
    for raw in parsed.models {
        // Newer builds report `name`, older ones `model`. A model with neither
        // cannot be released by name, so it is skipped rather than guessed at.
        let Some(name) = raw.name.or(raw.model) else {
            continue;
        };
        let expires_at = raw.expires_at.filter(|value| !value.is_empty());
        models.push(LoadedModel {
            name,
            size_bytes: raw.size.unwrap_or(0),
            size_vram_bytes: raw.size_vram.unwrap_or(0),
            expires_at,
            pinned: false,
        });
    }
    Ok(models)
}

/// A JSON string literal, so a model name cannot break out of the body.
fn json_string(value: &str) -> String {
    match serde_json::to_string(value) {
        Ok(encoded) => encoded,
        Err(_) => "\"\"".to_string(),
    }
}

/// A byte count a person can read, for an explanation.
fn human_bytes(bytes: u64) -> String {
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    let bytes_f = bytes as f64;
    if bytes_f >= GIB {
        format!("{:.1} GiB", bytes_f / GIB)
    } else if bytes_f >= MIB {
        format!("{:.0} MiB", bytes_f / MIB)
    } else {
        format!("{bytes} bytes")
    }
}

/// The current wall-clock instant in unix milliseconds.
///
/// Used only to read keep-alive policy, which the service expresses in
/// wall-clock time. A clock that cannot be read is treated as the unix epoch,
/// which makes every reported expiry look like a pin: the conservative answer.
fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since_epoch| since_epoch.as_millis() as u64)
        .unwrap_or(0)
}

/// Parse an RFC 3339 timestamp, as Ollama reports an expiry, into unix
/// milliseconds.
///
/// Deliberately strict, and deliberately returning `None` rather than a guess:
/// this value decides whether a model may be released, so a timestamp this
/// function does not understand must not be read as an ordinary keep-alive.
/// Accepted: `YYYY-MM-DDTHH:MM:SS` with optional fractional seconds and either
/// `Z` or a `+HH:MM` / `-HH:MM` offset. Instants before 1970 are rejected.
fn parse_rfc3339_ms(value: &str) -> Option<u64> {
    let bytes = value.as_bytes();
    if bytes.len() < 20 || bytes[4] != b'-' || bytes[7] != b'-' || bytes[10] != b'T' {
        return None;
    }
    let year: i64 = value.get(0..4)?.parse().ok()?;
    let month: u32 = value.get(5..7)?.parse().ok()?;
    let day: u32 = value.get(8..10)?.parse().ok()?;
    let hour: u64 = value.get(11..13)?.parse().ok()?;
    let minute: u64 = value.get(14..16)?.parse().ok()?;
    let second: u64 = value.get(17..19)?.parse().ok()?;
    if bytes[13] != b':' || bytes[16] != b':' {
        return None;
    }
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    // A leap second is clamped rather than rejected: the instant is real and the
    // second it belongs to is unambiguous.
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }

    let mut index = 19;
    let mut millis: u64 = 0;
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        let start = index;
        let mut digits = 0u32;
        let mut fraction: u64 = 0;
        while let Some(digit) = bytes.get(index).filter(|byte| byte.is_ascii_digit()) {
            if digits < 3 {
                fraction = fraction * 10 + u64::from(digit - b'0');
            }
            digits += 1;
            index += 1;
        }
        if index == start {
            return None;
        }
        millis = fraction * 10u64.pow(3u32.saturating_sub(digits.min(3)));
    }

    let offset_seconds: i64 = match bytes.get(index) {
        Some(b'Z') if index + 1 == bytes.len() => 0,
        Some(sign @ (b'+' | b'-')) => {
            let rest = value.get(index + 1..)?;
            let (hours, minutes) = rest.split_once(':')?;
            if hours.len() != 2 || minutes.len() != 2 {
                return None;
            }
            let hours: i64 = hours.parse().ok()?;
            let minutes: i64 = minutes.parse().ok()?;
            if hours > 23 || minutes > 59 {
                return None;
            }
            let magnitude = hours * 3_600 + minutes * 60;
            if *sign == b'+' {
                magnitude
            } else {
                -magnitude
            }
        }
        _ => return None,
    };

    let days = days_from_civil(year, month, day);
    let seconds =
        days * 86_400 + hour as i64 * 3_600 + minute as i64 * 60 + second as i64 - offset_seconds;
    if seconds < 0 {
        return None;
    }
    Some(seconds as u64 * 1_000 + millis)
}

/// Days since 1970-01-01 for a proleptic Gregorian date.
///
/// The standard civil-from-days inverse, with the era arithmetic done in a way
/// that does not depend on integer division rounding toward zero.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_index = month as i64;
    let day_of_year =
        (153 * (if month_index > 2 {
            month_index - 3
        } else {
            month_index + 9
        }) + 2)
            / 5
            + day as i64
            - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Whether a year is a leap year in the proleptic Gregorian calendar.
fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// How many days a month has.
fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        _ => 28,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_791_676_800_000; // 2026-10-11T00:00:00Z

    #[test]
    fn a_reported_expiry_is_read_as_the_instant_it_names() {
        assert_eq!(parse_rfc3339_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_rfc3339_ms("2026-10-11T00:00:00Z"), Some(NOW));
        assert_eq!(
            parse_rfc3339_ms("2026-10-11T15:30:00Z"),
            Some(NOW + 15 * 3_600_000 + 30 * 60_000)
        );
        // Ollama reports sub-second precision.
        assert_eq!(parse_rfc3339_ms("2026-10-11T00:00:00.5Z"), Some(NOW + 500));
        assert_eq!(
            parse_rfc3339_ms("2026-10-11T00:00:00.123Z"),
            Some(NOW + 123)
        );
        assert_eq!(
            parse_rfc3339_ms("2026-10-11T00:00:00.1234567Z"),
            Some(NOW + 123)
        );
        // The sample in Ollama's own documentation is offset from UTC.
        assert_eq!(
            parse_rfc3339_ms("2026-10-11T07:00:00+07:00"),
            Some(NOW),
            "the same instant expressed in a local offset"
        );
        assert_eq!(parse_rfc3339_ms("2026-10-10T23:00:00-01:00"), Some(NOW));
        assert_eq!(
            parse_rfc3339_ms("2024-02-29T00:00:00Z"),
            Some(1_709_164_800_000),
            "a leap day"
        );
    }

    #[test]
    fn a_timestamp_that_is_not_understood_is_refused_rather_than_guessed() {
        // Each of these decides a release, so an unreadable value must not be
        // read as an ordinary keep-alive.
        for value in [
            "",
            "soon",
            "0",
            "never",
            "2026-13-01T00:00:00Z", // month 13
            "2026-02-30T00:00:00Z", // February 30th
            "2023-02-29T00:00:00Z", // not a leap year
            "2026-10-11 00:00:00Z", // space instead of T
            "2026-10-11T00:00",     // no seconds
            "2026-10-11T00:00:00",  // no offset
            "2026-10-11T00:00:00+0700",
            "2026-10-11T25:00:00Z",
            "2026-10-11T00:60:00Z",
            "2026-10-11T00:00:00+99:00",
            "1900-01-01T00:00:00Z", // before the unix epoch
        ] {
            assert_eq!(parse_rfc3339_ms(value), None, "{value}");
        }
    }

    #[test]
    fn keep_alive_policy_decides_a_release_and_names_its_reason() {
        let model = |expires_at: Option<&str>, pinned: bool| LoadedModel {
            name: "llama3:8b".to_string(),
            size_bytes: 4 * 1024 * 1024 * 1024,
            size_vram_bytes: 0,
            expires_at: expires_at.map(str::to_string),
            pinned,
        };
        let horizon = DEFAULT_MAX_BOUNDED_KEEP_ALIVE_MS;

        // An ordinary keep-alive: the service says it will go in half an hour.
        let soon = model(Some("2026-10-11T00:30:00Z"), false);
        assert!(soon.is_unloadable(NOW, horizon));

        // Already expired: releasing it now changes nothing.
        let past = model(Some("2026-10-10T00:00:00Z"), false);
        assert!(past.is_unloadable(NOW, horizon));

        // A residency of days, an expiry of decades, no expiry at all, an
        // unreadable expiry, and an operator pin are all refusals.
        let long = model(Some("2026-10-20T00:00:00Z"), false);
        assert_eq!(
            long.release_refusal(NOW, horizon),
            Some("the reported expiry is far beyond an ordinary keep-alive")
        );
        let indefinite = model(Some("2099-01-01T00:00:00Z"), false);
        assert_eq!(
            indefinite.release_refusal(NOW, horizon),
            Some("the reported expiry is far beyond an ordinary keep-alive")
        );
        let unknown = model(None, false);
        assert_eq!(
            unknown.release_refusal(NOW, horizon),
            Some("the service did not report when this model will be unloaded")
        );
        let unreadable = model(Some("whenever"), false);
        assert_eq!(
            unreadable.release_refusal(NOW, horizon),
            Some("the reported expiry could not be read as a timestamp")
        );
        let pinned = model(Some("2026-10-11T00:30:00Z"), true);
        assert_eq!(
            pinned.release_refusal(NOW, horizon),
            Some("the operator pinned this model")
        );

        // The horizon is a dial, not a constant of nature.
        assert!(long.is_unloadable(NOW, 30 * 24 * 60 * 60 * 1_000));
    }

    #[test]
    fn an_explanation_uses_a_readable_size() {
        assert_eq!(human_bytes(0), "0 bytes");
        assert_eq!(human_bytes(1_024), "1024 bytes");
        assert_eq!(human_bytes(4 * 1024 * 1024 * 1024), "4.0 GiB");
        assert_eq!(human_bytes(512 * 1024 * 1024), "512 MiB");
    }

    #[test]
    fn the_clock_is_read_as_a_unix_instant() {
        // Sanity: the helper is not the epoch, and it is monotone enough to
        // order two readings.
        let first = now_unix_ms();
        assert!(first > 1_700_000_000_000, "{first}");
        assert!(now_unix_ms() >= first);
    }
}
