//! Optional, capability-based workload adapters.
//!
//! breadram manages memory generically, using procfs, cgroups, and systemd, and
//! needs no adapter at all. An adapter is an optional extra: it lets a specific
//! runtime (a model server, a job runner, another scheduler) describe its
//! workloads, say whether they are busy, and offer a bounded set of release,
//! drain, sleep, and wake operations.
//!
//! The isolation runs in one direction. The policy crate depends only on
//! [`crate::policy::activity::ActivitySource`]; the registry implements it, so
//! registration, capability discovery, and every adapter can be absent without
//! the policy core changing or even being aware.
//!
//! Three properties hold across the whole module:
//!
//! - **Discovery is bounded and read-only.** [`ProbeRequest`] cannot express a
//!   write. Only [`OperationRequest`], which an adapter returns from
//!   [`WorkloadAdapter::operation`], can, and a probe is refused unless its URL
//!   sits under an explicitly configured endpoint prefix.
//! - **Capabilities are never assumed.** An unprobed capability is unsupported,
//!   a degraded one is reported but not driven, and a probe whose version this
//!   build does not understand is discarded in favour of observation.
//! - **Nothing is inferred from a model API.** An OpenAI-compatible inference
//!   endpoint answering `404` or `405` for a model administration surface is
//!   recorded as unsupported rather than guessed at.

pub mod breadsearch;
pub mod capability;
pub mod executor;
pub mod hook;
pub mod http;
pub mod llamacpp;
pub mod mock;
pub mod ollama;
pub mod probe;

pub use breadsearch::{
    is_breadsearch_url, socket_path_for, BreadsearchAdapter, SocketError, SocketRunner, SocketSpec,
    UnixSocketRunner,
};
pub use capability::{AdapterCapabilities, AdapterCapability, CAPABILITY_PROBE_VERSION};
pub use executor::{AdapterExecutor, DEFAULT_OPERATION_TIMEOUT_MS, MAX_ERROR_EXCERPT_CHARS};
pub use hook::{
    is_hook_url, parse_release, parse_status, parse_wake, HookAdapter, HookError, HookRelease,
    HookRunner, HookStatus, HookVerb, HookWake, ProcessHookRunner,
};
pub use http::HttpTransport;
pub use llamacpp::{LlamaCppAdapter, ModelStatus, ReportedModel};
pub use mock::MockAdapter;
pub use ollama::{LoadedModel, OllamaAdapter, OllamaError, GENERATE_PATH, PS_PATH, VERSION_PATH};
pub use probe::{
    classify, is_encrypted_or_loopback, Auth, CircuitConfig, Method, NoSleep, OperationTransport,
    ProbeClient, ProbeError, ProbeRequest, RecordingSleeper, Redacted, Response, ScriptedTransport,
    Sleeper, Transport, TransportError,
};
pub use refresh_adapters as refresh;

use crate::config::{ActionKind, Config, HookSpec, ProviderKind};
use crate::inventory::{Workload, WorkloadId};
use crate::policy::activity::{ActivityEvidence, ActivitySource};
use crate::telemetry::fs::Clock;

/// A method an operation may use.
///
/// Writes live here and nowhere else, so the read-only guarantee on probes is
/// structural rather than a convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationMethod {
    /// Read, for a confirmation.
    Get,
    /// Create or submit.
    Post,
    /// Replace.
    Put,
    /// Remove.
    Delete,
}

impl OperationMethod {
    /// The wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            OperationMethod::Get => "GET",
            OperationMethod::Post => "POST",
            OperationMethod::Put => "PUT",
            OperationMethod::Delete => "DELETE",
        }
    }
}

/// A request an adapter wants to send to change something.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationRequest {
    /// The method, which may be a write.
    pub method: OperationMethod,
    /// The absolute URL.
    pub url: String,
    /// How to authenticate.
    pub auth: Auth,
    /// An optional JSON body.
    pub body: Option<String>,
}

impl OperationRequest {
    /// A write with a JSON body.
    pub fn post(url: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            method: OperationMethod::Post,
            url: url.into(),
            auth: Auth::None,
            body: Some(body.into()),
        }
    }

    /// The same request with bearer authentication.
    pub fn with_bearer(mut self, secret: impl Into<String>) -> Self {
        self.auth = Auth::Bearer(Redacted::new(secret));
        self
    }
}

/// A structured confirmation: a specific model id must carry a specific status
/// in the listing the probe returns.
///
/// This is preferred over the substring checks, which cannot tell two models
/// apart when one id or status contains the other as a substring.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelStatusCheck {
    /// The exact model id to find.
    pub id: String,
    /// The status that id must have.
    pub status: String,
}

/// A read-only check that an operation took effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Confirmation {
    /// A read-only probe, never a write.
    pub request: ProbeRequest,
    /// A substring that must appear for the operation to count as effective.
    pub expect_body_contains: Option<String>,
    /// A substring that must **not** appear for the operation to count as
    /// effective.
    ///
    /// Some effects are the absence of something: an unloaded model is
    /// confirmed by the model no longer being listed, not by a phrase appearing.
    /// A confirmation that checks neither is refused as unverifiable rather than
    /// treated as successful.
    pub expect_body_absent: Option<String>,
    /// A structured model id plus status check, preferred over the substring
    /// checks when present.
    pub expect_model_status: Option<ModelStatusCheck>,
}

impl Confirmation {
    /// A confirmation that requires a substring to appear.
    pub fn contains(request: ProbeRequest, expected: impl Into<String>) -> Self {
        Self {
            request,
            expect_body_contains: Some(expected.into()),
            expect_body_absent: None,
            expect_model_status: None,
        }
    }

    /// A confirmation that requires a substring to be gone.
    pub fn absent(request: ProbeRequest, unexpected: impl Into<String>) -> Self {
        Self {
            request,
            expect_body_contains: None,
            expect_body_absent: Some(unexpected.into()),
            expect_model_status: None,
        }
    }

    /// A confirmation that a specific model id carries a specific status.
    ///
    /// This is the only form that cannot be fooled by a substring: it matches
    /// the exact id and reads that entry's own status.
    pub fn model_status(
        request: ProbeRequest,
        id: impl Into<String>,
        status: impl Into<String>,
    ) -> Self {
        Self {
            request,
            expect_body_contains: None,
            expect_body_absent: None,
            expect_model_status: Some(ModelStatusCheck {
                id: id.into(),
                status: status.into(),
            }),
        }
    }

    /// Whether a response body confirms the effect.
    ///
    /// An unverifiable confirmation, one that checks nothing, is never a
    /// success. A structured check is used in preference to the substrings.
    pub fn confirms(&self, body: &str) -> bool {
        if let Some(check) = &self.expect_model_status {
            return model_status_matches(body, check);
        }
        match (&self.expect_body_contains, &self.expect_body_absent) {
            (None, None) => false,
            (contains, absent) => {
                contains
                    .as_ref()
                    .is_none_or(|expected| body.contains(expected.as_str()))
                    && absent
                        .as_ref()
                        .is_none_or(|unexpected| !body.contains(unexpected.as_str()))
            }
        }
    }
}

/// Whether the listing names `check.id` and gives it exactly `check.status`.
///
/// The listing may be a bare array or wrapped in a `data` array. Two models that
/// share a substring cannot cross-confirm, because the id must match exactly and
/// the status is read from that same entry.
fn model_status_matches(body: &str, check: &ModelStatusCheck) -> bool {
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(body) else {
        return false;
    };
    let entries = match parsed.as_array() {
        Some(entries) => entries,
        None => match parsed.get("data").and_then(|value| value.as_array()) {
            Some(entries) => entries,
            None => return false,
        },
    };
    entries.iter().any(|entry| {
        entry.get("id").and_then(|value| value.as_str()) == Some(check.id.as_str())
            && entry_status(entry) == Some(check.status.as_str())
    })
}

/// The status of a listing entry, whether it is a bare string or an object with
/// a `value` field, which is how the llama.cpp router nests it.
fn entry_status(entry: &serde_json::Value) -> Option<&str> {
    match entry.get("status")? {
        serde_json::Value::String(status) => Some(status.as_str()),
        other => other.get("value").and_then(|value| value.as_str()),
    }
}

/// A resource an adapter manages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedResource {
    /// The resource name, for example a model name.
    pub name: String,
    /// The host memory it currently holds, when known.
    pub host_ram_bytes: Option<u64>,
    /// The device memory it currently holds, when known.
    pub device_ram_bytes: Option<u64>,
    /// Whether the adapter reports it as idle.
    pub idle: bool,
}

/// One operation an adapter can perform, with its expected effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterOperation {
    /// The action this operation implements.
    pub kind: ActionKind,
    /// The request that performs it.
    pub request: OperationRequest,
    /// The expected change in host memory, in bytes. Negative is relief.
    pub expected_host_ram_delta: i64,
    /// The expected change in device memory, in bytes.
    pub expected_device_ram_delta: i64,
    /// Whether the operation can be undone by the adapter.
    pub reversible: bool,
    /// What a person should be told about performing it.
    ///
    /// A release is never free: unloading a model means the next request that
    /// uses it pays a cold start, and a provider sleep means memory moves to the
    /// host rather than disappearing. Stating that here keeps the consequence in
    /// the operation instead of leaving an explanation to invent it.
    pub note: String,
    /// How to check that it worked.
    pub confirmation: Confirmation,
}

/// The answer to one probe an adapter asked for.
///
/// An adapter cannot perform I/O, so it asks for the probes it needs and the
/// caller returns the answers here. That keeps discovery bounded, keeps every
/// credential in one place, and means an adapter with no observation of its own
/// is simply an adapter that ignores these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeOutcome {
    /// The request that was issued.
    pub request: ProbeRequest,
    /// What came back, or why nothing did.
    pub result: Result<Response, ProbeError>,
}

/// The contract an optional runtime integration implements.
///
/// Only the identity methods are required. Everything else has a conservative
/// default, so an adapter that can do nothing but say "this workload is mine and
/// it is busy" is a valid adapter.
pub trait WorkloadAdapter: Send + Sync {
    /// A unique name, used in diagnostics.
    fn name(&self) -> &str;
    /// Which provider interface this speaks.
    fn kind(&self) -> ProviderKind;
    /// The explicitly configured endpoint, never a discovered one.
    fn endpoint(&self) -> &str;
    /// What this adapter can do, discovered or configured, and versioned.
    fn capabilities(&self) -> &AdapterCapabilities;
    /// Whether this adapter is responsible for a workload.
    fn owns(&self, workload: &Workload) -> bool;

    /// Bounded, read-only probes used to discover capabilities.
    fn discover(&self) -> Vec<ProbeRequest> {
        Vec::new()
    }

    /// Record the answers to [`WorkloadAdapter::discover`], so this adapter's
    /// view of its endpoint is current.
    ///
    /// The caller probes and hands the results back here before anything is
    /// planned or executed. The default records nothing, which is right for an
    /// adapter that needs no observation, and an adapter that needs one must say
    /// so: an adapter that returns probes without recording their answers is
    /// treated as never having observed anything, which is the safe direction.
    ///
    /// Returned notes are reported to the operator, so a read that failed is
    /// visible rather than merely absent.
    fn observe(&self, outcomes: &[ProbeOutcome], at_ms: u64) -> Vec<String> {
        let _ = (outcomes, at_ms);
        Vec::new()
    }

    /// Read-only inventory of the resources this adapter manages.
    fn inventory(&self) -> Vec<ManagedResource> {
        Vec::new()
    }

    /// Whether [`WorkloadAdapter::inventory`] reflects a current reading.
    ///
    /// This is what separates "the provider holds nothing" from "the provider
    /// could not be read": an empty inventory from a current reading is a
    /// measurement of zero, and the same empty inventory from a failed read is
    /// no measurement at all. The default is `false`, so an adapter that does
    /// not say is treated as unmeasured rather than as holding nothing.
    fn inventory_is_current(&self) -> bool {
        false
    }

    /// Whether a workload is busy, idle, or unknown.
    fn activity(&self, _workload: &Workload) -> ActivityEvidence {
        ActivityEvidence::Unknown
    }

    /// The operation for an action, if this adapter supports one.
    fn operation(&self, _action: ActionKind, _workload: &Workload) -> Option<AdapterOperation> {
        None
    }

    /// The validated hook spec, when this adapter runs a hook program.
    ///
    /// Only [`HookAdapter`] returns one. The default keeps every HTTP adapter
    /// compiling unchanged and lets the executor find the hook owner without
    /// downcasting.
    fn hook_spec(&self) -> Option<HookSpec> {
        None
    }

    /// Record a hook program answer outside the probe path.
    ///
    /// The default ignores it, which is right for every adapter that never
    /// runs a hook.
    fn note_hook_result(&self, _result: Result<String, HookError>, _at_ms: u64) -> Vec<String> {
        Vec::new()
    }

    /// Run this adapter's hook program once and record what came back.
    /// The default does nothing, so adapters without a hook cost nothing.
    /// The socket this adapter reads and writes, when it speaks a daemon
    /// socket rather than HTTP or a hook program. Only [`BreadsearchAdapter`]
    /// returns one; the default keeps every other adapter compiling unchanged
    /// and lets the executor find the socket owner without downcasting.
    fn socket_spec(&self) -> Option<SocketSpec> {
        None
    }

    /// Record a socket answer outside the refresh path.
    ///
    /// The default ignores it, which is right for every adapter that never
    /// touches a socket.
    fn note_socket_result(
        &self,
        _result: Result<String, crate::adapter::breadsearch::SocketError>,
        _at_ms: u64,
    ) -> Vec<String> {
        Vec::new()
    }

    /// Read this adapter's daemon socket once and record what came back.
    ///
    /// The default does nothing, so adapters without a socket cost nothing on
    /// a refresh pass.
    fn refresh_socket(
        &self,
        _runner: &dyn crate::adapter::breadsearch::SocketRunner,
        _at_ms: u64,
    ) -> Vec<String> {
        Vec::new()
    }
    fn refresh_hook(&self, _runner: &dyn HookRunner, _at_ms: u64) -> Vec<String> {
        Vec::new()
    }
}

/// The registered adapters.
///
/// An empty registry is the normal case and is fully functional: generic Linux
/// workload management needs no adapter.
#[derive(Default)]
pub struct AdapterRegistry {
    adapters: Vec<Box<dyn WorkloadAdapter>>,
}

impl AdapterRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register the adapters a configuration describes.
    ///
    /// Only a configured endpoint is ever attached: nothing is discovered, no
    /// port is guessed, and an endpoint that names no workload scope is not
    /// attached at all, because activity evidence has to belong to a workload the
    /// inventory also sees. Every endpoint that contributed nothing returns a
    /// note saying why, so a configuration mistake is visible rather than silent.
    ///
    /// Control capabilities are granted only when the operator set
    /// `allow_automation` on that endpoint. Otherwise the adapter is observation
    /// only, which is the conservative default: it can report what it sees and
    /// cannot be asked to write.
    pub fn from_config(config: &Config) -> (Self, Vec<String>) {
        let mut registry = Self::new();
        let mut notes = Vec::new();
        for provider in &config.providers {
            let Some(scope) = provider.scope.as_deref() else {
                notes.push(format!(
                    "provider {} is configured but names no workload scope, so it contributes nothing",
                    provider.name
                ));
                continue;
            };
            // Control capabilities are granted per kind below, only when the
            // operator set `allow_automation`. Otherwise the adapter is
            // observation only, which is the conservative default.
            if !provider.allow_automation {
                notes.push(format!(
                    "provider {} is observation only: automation may not target it",
                    provider.name
                ));
            }
            match &provider.kind {
                ProviderKind::Ollama => {
                    let mut observation = vec![
                        AdapterCapability::Discovery,
                        AdapterCapability::Inventory,
                        AdapterCapability::Activity,
                    ];
                    if provider.allow_automation {
                        observation.push(AdapterCapability::UnloadModel);
                        observation.push(AdapterCapability::ModelAdministration);
                    }
                    let capabilities = AdapterCapabilities::configured(&observation);
                    registry.register(Box::new(
                        OllamaAdapter::new(
                            provider.name.clone(),
                            provider.url.clone(),
                            scope.to_string(),
                        )
                        .with_capabilities(capabilities)
                        .with_shared_device_memory(provider.igpu),
                    ));
                    if provider.igpu {
                        notes.push(format!(
                            "provider {} runs on an integrated GPU: a model's reported device memory is attributed to host RAM, because on an iGPU it is a carve-out of system memory, not dedicated VRAM",
                            provider.name
                        ));
                    }
                }
                ProviderKind::Hook => {
                    match HookSpec::from_endpoint(provider) {
                        Some(spec) => {
                            // Hooks are driven by an explicit program, not by
                            // probing, so discovery is meaningless for them. A
                            // hook that cannot be attached is reported rather
                            // than silently dropped.
                            let mut hook_observation = vec![
                                AdapterCapability::Inventory,
                                AdapterCapability::Activity,
                            ];
                            if provider.allow_automation {
                                hook_observation.push(AdapterCapability::UnloadModel);
                                hook_observation.push(AdapterCapability::WakeModel);
                            }
                            let capabilities =
                                AdapterCapabilities::configured(&hook_observation);
                            registry.register(Box::new(
                                HookAdapter::new(
                                    provider.name.clone(),
                                    provider.url.clone(),
                                    scope.to_string(),
                                    spec,
                                )
                                .with_capabilities(capabilities),
                            ));
                        }
                        None => notes.push(format!(
                            "provider {} is a hook with an invalid command, so it contributes nothing",
                            provider.name
                        )),
                    }
                }
                ProviderKind::LlamaCpp => {
                    let mut observation = vec![
                        AdapterCapability::Discovery,
                        AdapterCapability::Inventory,
                        AdapterCapability::Activity,
                    ];
                    if provider.allow_automation {
                        observation.push(AdapterCapability::UnloadModel);
                    }
                    let capabilities = AdapterCapabilities::configured(&observation);
                    registry.register(Box::new(
                        LlamaCppAdapter::new(
                            provider.name.clone(),
                            provider.url.clone(),
                            scope.to_string(),
                        )
                        .with_capabilities(capabilities),
                    ));
                    if provider.allow_automation {
                        notes.push(format!(
                            "provider {} may unload a router model when automation allows it; the unload is confirmed by the exact model id and its status, and no host or device memory change is measured",
                            provider.name
                        ));
                    } else {
                        notes.push(format!(
                            "provider {} is observation only: automation is off, so no unload is offered; single-model manual sleep is not documented; generic controls apply",
                            provider.name
                        ));
                    }
                }
                ProviderKind::Breadsearch => {
                    match socket_path_for_endpoint(&provider.url) {
                        Some(socket_path) => {
                            let mut observation = vec![
                                AdapterCapability::Inventory,
                                AdapterCapability::Activity,
                            ];
                            if provider.allow_automation {
                                observation.push(AdapterCapability::UnloadModel);
                            }
                            let capabilities =
                                AdapterCapabilities::configured(&observation);
                            registry.register(Box::new(
                                BreadsearchAdapter::new(
                                    provider.name.clone(),
                                    socket_path,
                                    scope.to_string(),
                                )
                                .with_capabilities(capabilities),
                            ));
                            if provider.allow_automation {
                                notes.push(format!(
                                    "provider {} may unload the breadmill model when automation allows it; the unload is confirmed by a fresh status read, and the next search reloads the model on demand",
                                    provider.name
                                ));
                            } else {
                                notes.push(format!(
                                    "provider {} is observation only: automation is off, so no unload is offered",
                                    provider.name
                                ));
                            }
                        }
                        None => notes.push(format!(
                            "provider {} is a breadsearch endpoint without a breadsearch:<socket path> url, so it contributes nothing",
                            provider.name
                        )),
                    }
                }
                other => notes.push(format!(
                    "provider {} speaks {other:?}, which this build has no adapter for yet",
                    provider.name
                )),
            }
        }
        (registry, notes)
    }

    /// Register an adapter. Registration is always optional.
    pub fn register(&mut self, adapter: Box<dyn WorkloadAdapter>) {
        self.adapters.push(adapter);
    }

    /// Whether no adapters are registered.
    pub fn is_empty(&self) -> bool {
        self.adapters.is_empty()
    }

    /// How many adapters are registered.
    pub fn len(&self) -> usize {
        self.adapters.len()
    }

    /// The registered adapter names.
    pub fn names(&self) -> Vec<&str> {
        self.adapters.iter().map(|adapter| adapter.name()).collect()
    }

    /// The explicitly configured endpoints, sorted and de-duplicated.
    ///
    /// This is the allow list a transport is restricted to. Only endpoints that
    /// were configured are ever in it: nothing is discovered or guessed, so a
    /// request can only go where an operator said a provider is.
    pub fn endpoints(&self) -> Vec<String> {
        let mut endpoints: Vec<String> = self
            .adapters
            .iter()
            .map(|adapter| adapter.endpoint().to_string())
            .collect();
        endpoints.sort();
        endpoints.dedup();
        endpoints
    }

    /// Every registered adapter, in registration order.
    pub fn adapters(&self) -> impl Iterator<Item = &dyn WorkloadAdapter> {
        self.adapters.iter().map(AsRef::as_ref)
    }

    /// Every adapter that claims a workload.
    pub fn owners<'a>(&'a self, workload: &Workload) -> Vec<&'a dyn WorkloadAdapter> {
        self.adapters
            .iter()
            .filter(|adapter| adapter.owns(workload))
            .map(AsRef::as_ref)
            .collect()
    }

    /// The read-only inventory of every adapter, in registration order.
    pub fn inventory(&self) -> Vec<ManagedResource> {
        self.adapters
            .iter()
            .flat_map(|adapter| adapter.inventory())
            .collect()
    }

    /// Every adapter's discovery probes, de-duplicated by URL and sorted, so a
    /// shared endpoint is asked once and the order is deterministic.
    pub fn discovery_requests(&self) -> Vec<ProbeRequest> {
        let mut requests: Vec<ProbeRequest> = self
            .adapters
            .iter()
            .flat_map(|adapter| adapter.discover())
            .collect();
        requests.sort_by(|left, right| left.url.cmp(&right.url));
        requests.dedup_by(|left, right| left.url == right.url);
        requests
    }

    /// Offer every probe answer to every adapter.
    ///
    /// Each adapter matches the outcomes on its own endpoint, so a shared
    /// endpoint is asked once and every adapter that needs the answer gets it.
    /// Notes come back in registration order.
    pub fn observe(&self, outcomes: &[ProbeOutcome], at_ms: u64) -> Vec<String> {
        self.adapters
            .iter()
            .flat_map(|adapter| adapter.observe(outcomes, at_ms))
            .collect()
    }

    /// The best activity answer for a workload.
    ///
    /// The first adapter that actually knows something wins. An adapter that
    /// reports nothing does not suppress a later adapter that does, and if none
    /// knows, the answer is unknown rather than a guess.
    pub fn activity(&self, workload: &Workload) -> ActivityEvidence {
        for adapter in self.owners(workload) {
            let evidence = adapter.activity(workload);
            if evidence != ActivityEvidence::Unknown {
                return evidence;
            }
        }
        ActivityEvidence::Unknown
    }

    /// The first owner that can perform an action.
    pub fn operation(&self, action: ActionKind, workload: &Workload) -> Option<AdapterOperation> {
        self.owners(workload)
            .into_iter()
            .find_map(|adapter| adapter.operation(action, workload))
    }

    /// Run every hook program once and record what came back.
    ///
    /// Notes come back in registration order. Adapters without a hook do
    /// nothing here.
    pub fn refresh_hooks(&self, runner: &dyn HookRunner, at_ms: u64) -> Vec<String> {
        self.adapters
            .iter()
            .flat_map(|adapter| adapter.refresh_hook(runner, at_ms))
            .collect()
    }

    /// Read every socket daemon once and record what came back.
    ///
    /// Notes come back in registration order. Adapters without a socket do
    /// nothing here.
    pub fn refresh_sockets(
        &self,
        runner: &dyn crate::adapter::breadsearch::SocketRunner,
        at_ms: u64,
    ) -> Vec<String> {
        self.adapters
            .iter()
            .flat_map(|adapter| adapter.refresh_socket(runner, at_ms))
            .collect()
    }

    /// An activity source the policy core can consume.
    ///
    /// This is the whole bridge. Policy sees [`ActivitySource`] and never learns
    /// that adapters exist.
    pub fn activity_source<'a>(&'a self, workloads: &'a [Workload]) -> RegistryActivity<'a> {
        RegistryActivity {
            registry: self,
            workloads,
        }
    }
}

/// The socket path a breadsearch endpoint URL names.
///
/// Endpoint URLs look like `breadsearch:/run/user/1000/breadmill.sock`. The
/// path must be absolute: a relative socket path is a configuration mistake,
/// not something to resolve against a daemon working directory.
fn socket_path_for_endpoint(url: &str) -> Option<String> {
    let path = url.strip_prefix("breadsearch:")?;
    if !path.starts_with('/') || path.contains('\0') {
        return None;
    }
    Some(path.to_string())
}

/// Read every configured endpoint once and record what came back.
///
/// The probes come from the adapters themselves, so only an explicitly
/// configured endpoint is ever visited, and a shared endpoint is asked once. A
/// read that failed is recorded as a failed reading rather than leaving the
/// previous success in place, and the returned notes name what could not be
/// read.
pub fn refresh_adapters(
    registry: &AdapterRegistry,
    transport: &dyn Transport,
    sleeper: &dyn Sleeper,
    clock: &dyn Clock,
    circuit: CircuitConfig,
) -> Vec<String> {
    let requests = registry.discovery_requests();
    if requests.is_empty() {
        return Vec::new();
    }
    let client = ProbeClient::new(transport, sleeper, clock, circuit, registry.endpoints());
    let outcomes: Vec<ProbeOutcome> = requests
        .iter()
        .map(|request| ProbeOutcome {
            request: request.clone(),
            result: client.probe(request),
        })
        .collect::<Vec<ProbeOutcome>>();
    registry.observe(&outcomes, clock.monotonic_ms())
}

/// Run every hook program once and record what came back.
///
/// Only adapters that run a hook do anything here; every other adapter is
/// skipped by its own default. A read that failed is recorded as a failed
/// reading rather than leaving the previous success in place, and the
/// returned notes name the hook that could not be read.
pub fn refresh_hooks(
    registry: &AdapterRegistry,
    runner: &dyn HookRunner,
    at_ms: u64,
) -> Vec<String> {
    registry.refresh_hooks(runner, at_ms)
}

/// Read every socket daemon once and record what came back.
///
/// Only adapters that speak a daemon socket do anything here; every other
/// adapter is skipped by its own default. A read that failed is recorded as a
/// failed reading rather than leaving the previous success in place, and the
/// returned notes name the daemon that could not be read.
pub fn refresh_sockets(
    registry: &AdapterRegistry,
    runner: &dyn crate::adapter::breadsearch::SocketRunner,
    at_ms: u64,
) -> Vec<String> {
    registry.refresh_sockets(runner, at_ms)
}

/// The bridge from the adapter registry to the policy core.
pub struct RegistryActivity<'a> {
    registry: &'a AdapterRegistry,
    workloads: &'a [Workload],
}

impl ActivitySource for RegistryActivity<'_> {
    fn evidence(&self, workload: &WorkloadId) -> ActivityEvidence {
        match self
            .workloads
            .iter()
            .find(|candidate| &candidate.id == workload)
        {
            Some(workload) => self.registry.activity(workload),
            None => ActivityEvidence::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory::{ControlLevel, MemoryAccounting};

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

    #[test]
    fn an_empty_registry_knows_nothing() {
        let registry = AdapterRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert_eq!(
            registry.activity(&workload("unit:app.service")),
            ActivityEvidence::Unknown
        );
        assert!(registry.inventory().is_empty());
        assert!(registry.discovery_requests().is_empty());
        assert!(registry
            .operation(ActionKind::UnloadIdleModel, &workload("unit:app.service"))
            .is_none());
    }

    #[test]
    fn a_registry_with_no_owner_still_reports_unknown() {
        let mut registry = AdapterRegistry::new();
        registry.register(Box::new(
            MockAdapter::new("mock", "http://127.0.0.1:1").owning("unit:other"),
        ));
        assert_eq!(
            registry.activity(&workload("unit:app.service")),
            ActivityEvidence::Unknown
        );
    }

    #[test]
    fn a_configured_provider_is_attached_only_with_a_scope() {
        let config = Config {
            providers: vec![
                crate::config::ProviderEndpoint {
                    name: "ollama".to_string(),
                    kind: ProviderKind::Ollama,
                    url: "http://127.0.0.1:11434".to_string(),
                    scope: Some("unit:ollama.service".to_string()),
                    credential_ref: None,
                    allow_automation: true,
                    igpu: false,
                    command: Vec::new(),
                    hook_timeout_ms: None,
                    hook_max_output_bytes: None,
                },
                crate::config::ProviderEndpoint {
                    name: "vllm".to_string(),
                    kind: ProviderKind::Vllm,
                    url: "https://gpu.example.net:8000".to_string(),
                    scope: Some("unit:vllm.service".to_string()),
                    credential_ref: None,
                    allow_automation: true,
                    igpu: false,
                    command: Vec::new(),
                    hook_timeout_ms: None,
                    hook_max_output_bytes: None,
                },
                crate::config::ProviderEndpoint {
                    name: "stray".to_string(),
                    kind: ProviderKind::Ollama,
                    url: "http://127.0.0.1:11500".to_string(),
                    scope: None,
                    credential_ref: None,
                    allow_automation: true,
                    igpu: false,
                    command: Vec::new(),
                    hook_timeout_ms: None,
                    hook_max_output_bytes: None,
                },
            ],
            ..Config::default()
        };
        let (registry, notes) = AdapterRegistry::from_config(&config);
        assert_eq!(registry.names(), vec!["ollama"]);
        assert_eq!(notes.len(), 2, "{notes:?}");
        assert!(notes.iter().any(|note| note.contains("no workload scope")));
        assert!(notes.iter().any(|note| note.contains("no adapter for yet")));

        // With a scope, the adapter owns that workload and reports its endpoint.
        let owners = registry.owners(&workload("unit:ollama.service"));
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].kind(), ProviderKind::Ollama);
        assert_eq!(owners[0].endpoint(), "http://127.0.0.1:11434");
        assert!(
            owners[0]
                .capabilities()
                .supports(AdapterCapability::UnloadModel),
            "allow_automation was set, so a release may be offered"
        );
    }

    #[test]
    fn an_endpoint_without_allow_automation_is_observation_only() {
        let config = Config {
            providers: vec![crate::config::ProviderEndpoint {
                name: "ollama".to_string(),
                kind: ProviderKind::Ollama,
                url: "http://127.0.0.1:11434".to_string(),
                scope: Some("unit:ollama.service".to_string()),
                credential_ref: None,
                allow_automation: false,
                igpu: false,
                command: Vec::new(),
                hook_timeout_ms: None,
                hook_max_output_bytes: None,
            }],
            ..Config::default()
        };
        let (registry, notes) = AdapterRegistry::from_config(&config);
        let owners = registry.owners(&workload("unit:ollama.service"));
        assert_eq!(owners.len(), 1);
        assert!(owners[0]
            .capabilities()
            .supports(AdapterCapability::Activity));
        assert!(
            !owners[0]
                .capabilities()
                .supports(AdapterCapability::UnloadModel),
            "observation is allowed, control is not"
        );
        assert_eq!(
            owners[0].operation(
                ActionKind::UnloadIdleModel,
                &workload("unit:ollama.service")
            ),
            None
        );
        assert!(notes.iter().any(|note| note.contains("observation only")));
    }

    #[test]
    fn discovery_requests_are_sorted_and_deduplicated() {
        let mut registry = AdapterRegistry::new();
        registry.register(Box::new(
            MockAdapter::new("b", "http://127.0.0.1:2")
                .owning("unit:b")
                .with_capabilities(AdapterCapabilities::configured(&[
                    AdapterCapability::Discovery,
                ])),
        ));
        registry.register(Box::new(
            MockAdapter::new("a", "http://127.0.0.1:2")
                .owning("unit:a")
                .with_capabilities(AdapterCapabilities::configured(&[
                    AdapterCapability::Discovery,
                ])),
        ));
        let requests = registry.discovery_requests();
        assert_eq!(requests.len(), 1, "a shared endpoint is asked once");
        assert_eq!(requests[0].url, "http://127.0.0.1:2/breadram/capabilities");
    }

    #[test]
    fn a_structured_model_check_cannot_be_fooled_by_a_substring() {
        // Two ids where one contains the other, and statuses where one is a
        // substring of the other. A substring confirmation can cross-confirm; the
        // structured check reads the exact id's own status and must not.
        let body = r#"[{"id":"llama","status":"loading"},{"id":"llama-2","status":"loaded"}]"#;
        let request = || ProbeRequest::get("http://127.0.0.1:1/models");
        assert!(
            Confirmation::contains(request(), "loaded").confirms(body),
            "a bare substring matches llama-2's status even when the question is about llama"
        );
        assert!(Confirmation::model_status(request(), "llama-2", "loaded").confirms(body));
        assert!(
            !Confirmation::model_status(request(), "llama", "loaded").confirms(body),
            "llama is loading, so it must not be confirmed as loaded by its sibling"
        );
        assert!(!Confirmation::model_status(request(), "llama-3", "loaded").confirms(body));
        assert!(Confirmation::model_status(request(), "llama", "loading").confirms(body));
    }
}
