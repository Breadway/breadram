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

pub mod capability;
pub mod mock;
pub mod probe;

pub use capability::{AdapterCapabilities, AdapterCapability, CAPABILITY_PROBE_VERSION};
pub use mock::MockAdapter;
pub use probe::{
    classify, is_encrypted_or_loopback, Auth, CircuitConfig, Method, NoSleep, ProbeClient,
    ProbeError, ProbeRequest, RecordingSleeper, Redacted, Response, ScriptedTransport, Sleeper,
    Transport, TransportError,
};

use crate::config::{ActionKind, ProviderKind};
use crate::inventory::{Workload, WorkloadId};
use crate::policy::activity::{ActivityEvidence, ActivitySource};

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

/// A read-only check that an operation took effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Confirmation {
    /// A read-only probe, never a write.
    pub request: ProbeRequest,
    /// A substring that must appear in the confirming response for the
    /// operation to count as effective.
    pub expect_body_contains: String,
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
    /// How to check that it worked.
    pub confirmation: Confirmation,
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

    /// Read-only inventory of the resources this adapter manages.
    fn inventory(&self) -> Vec<ManagedResource> {
        Vec::new()
    }

    /// Whether a workload is busy, idle, or unknown.
    fn activity(&self, _workload: &Workload) -> ActivityEvidence {
        ActivityEvidence::Unknown
    }

    /// The operation for an action, if this adapter supports one.
    fn operation(&self, _action: ActionKind, _workload: &Workload) -> Option<AdapterOperation> {
        None
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
}
