//! A mock adapter.
//!
//! This is both a test double and the worked example of the extension contract
//! in `docs/ADAPTER-CONTRACT.md`. A runtime integrates by implementing
//! [`WorkloadAdapter`] and registering it; nothing in the policy core changes.

use crate::config::{ActionKind, ProviderKind};
use crate::inventory::Workload;
use crate::policy::activity::ActivityEvidence;

use super::capability::{AdapterCapabilities, AdapterCapability};
use super::probe::ProbeRequest;
use super::{AdapterOperation, Confirmation, ManagedResource, OperationRequest, WorkloadAdapter};

/// A configurable adapter used by tests and as the extension example.
#[derive(Debug, Clone)]
pub struct MockAdapter {
    name: String,
    endpoint: String,
    owns_prefix: Option<String>,
    capabilities: AdapterCapabilities,
    activity: ActivityEvidence,
    resources: Vec<ManagedResource>,
    host_ram_bytes: u64,
    device_ram_bytes: u64,
}

impl MockAdapter {
    /// A mock that owns nothing and knows nothing.
    pub fn new(name: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            endpoint: endpoint.into(),
            owns_prefix: None,
            capabilities: AdapterCapabilities::observation_only(),
            activity: ActivityEvidence::Unknown,
            resources: Vec::new(),
            host_ram_bytes: 0,
            device_ram_bytes: 0,
        }
    }

    /// Claim the workloads whose scope starts with this prefix.
    pub fn owning(mut self, scope_prefix: impl Into<String>) -> Self {
        self.owns_prefix = Some(scope_prefix.into());
        self
    }

    /// Set the capability set.
    pub fn with_capabilities(mut self, capabilities: AdapterCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Set the activity answer.
    pub fn with_activity(mut self, activity: ActivityEvidence) -> Self {
        self.activity = activity;
        self
    }

    /// Add a managed resource.
    pub fn with_resource(mut self, resource: ManagedResource) -> Self {
        self.resources.push(resource);
        self
    }

    /// Set the memory this mock reports for a release operation, and the device
    /// memory a sleep operation would move into host memory.
    pub fn with_memory(mut self, host_ram_bytes: u64, device_ram_bytes: u64) -> Self {
        self.host_ram_bytes = host_ram_bytes;
        self.device_ram_bytes = device_ram_bytes;
        self
    }
}

impl WorkloadAdapter for MockAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> ProviderKind {
        ProviderKind::Hook
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn capabilities(&self) -> &AdapterCapabilities {
        &self.capabilities
    }

    fn owns(&self, workload: &Workload) -> bool {
        self.owns_prefix
            .as_ref()
            .is_some_and(|prefix| workload.id.scope.starts_with(prefix.as_str()))
    }

    fn discover(&self) -> Vec<ProbeRequest> {
        if !self.capabilities.observes(AdapterCapability::Discovery) {
            return Vec::new();
        }
        vec![ProbeRequest::get(format!(
            "{}/breadram/capabilities",
            self.endpoint
        ))]
    }

    fn inventory(&self) -> Vec<ManagedResource> {
        if !self.capabilities.observes(AdapterCapability::Inventory) {
            return Vec::new();
        }
        self.resources.clone()
    }

    fn activity(&self, _workload: &Workload) -> ActivityEvidence {
        if !self.capabilities.observes(AdapterCapability::Activity) {
            return ActivityEvidence::Unknown;
        }
        self.activity
    }

    fn operation(&self, action: ActionKind, workload: &Workload) -> Option<AdapterOperation> {
        let url = format!("{}/breadram/operations", self.endpoint);
        match action {
            ActionKind::UnloadIdleModel
                if self.capabilities.supports(AdapterCapability::UnloadModel) =>
            {
                Some(AdapterOperation {
                    kind: action,
                    request: OperationRequest::post(
                        &url,
                        format!(
                            "{{\"operation\":\"unload\",\"target\":\"{}\"}}",
                            workload.id.scope
                        ),
                    ),
                    // Releasing a loaded model returns host memory.
                    expected_host_ram_delta: -(self.host_ram_bytes as i64),
                    expected_device_ram_delta: 0,
                    reversible: true,
                    note: "the mock frees nothing of its own; its effect is simulated".to_string(),
                    confirmation: Confirmation::contains(
                        ProbeRequest::get(format!("{url}/status")),
                        "unloaded",
                    ),
                })
            }
            ActionKind::SleepModel if self.capabilities.supports(AdapterCapability::SleepModel) => {
                Some(AdapterOperation {
                    kind: action,
                    request: OperationRequest::post(
                        &url,
                        format!(
                            "{{\"operation\":\"sleep\",\"target\":\"{}\"}}",
                            workload.id.scope
                        ),
                    ),
                    // Offload frees device memory by moving it into host memory.
                    expected_host_ram_delta: self.device_ram_bytes as i64,
                    expected_device_ram_delta: -(self.device_ram_bytes as i64),
                    reversible: true,
                    note: "the mock moves nothing; a real offload trades device memory for host memory".to_string(),
                    confirmation: Confirmation::contains(
                        ProbeRequest::get(format!("{url}/status")),
                        "sleeping",
                    ),
                })
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::OperationMethod;
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

    fn idle_adapter() -> MockAdapter {
        MockAdapter::new("mock", "http://127.0.0.1:9000")
            .owning("unit:mock.service")
            .with_capabilities(AdapterCapabilities::configured(&[
                AdapterCapability::Discovery,
                AdapterCapability::Inventory,
                AdapterCapability::Activity,
                AdapterCapability::UnloadModel,
                AdapterCapability::SleepModel,
            ]))
            .with_activity(ActivityEvidence::Idle { since_ms: 0 })
            .with_memory(8 * 1024 * 1024 * 1024, 16 * 1024 * 1024 * 1024)
            .with_resource(ManagedResource {
                name: "llama".to_string(),
                host_ram_bytes: Some(1024),
                device_ram_bytes: None,
                idle: true,
            })
    }

    #[test]
    fn an_observation_only_adapter_discovers_and_reports_nothing() {
        let adapter = MockAdapter::new("mock", "http://127.0.0.1:9000").owning("unit:mock.service");
        assert!(adapter.discover().is_empty());
        assert!(adapter.inventory().is_empty());
        assert_eq!(
            adapter.activity(&workload("unit:mock.service")),
            ActivityEvidence::Unknown
        );
        assert!(adapter
            .operation(ActionKind::UnloadIdleModel, &workload("unit:mock.service"))
            .is_none());
    }

    #[test]
    fn an_unload_frees_host_memory_and_is_confirmable() {
        let adapter = idle_adapter();
        let operation = adapter
            .operation(ActionKind::UnloadIdleModel, &workload("unit:mock.service"))
            .expect("unload supported");
        assert!(operation.expected_host_ram_delta < 0);
        assert!(operation.reversible);
        assert_eq!(operation.request.method, OperationMethod::Post);
        assert_eq!(
            operation.confirmation.expect_body_contains.as_deref(),
            Some("unloaded")
        );
        assert!(operation.confirmation.confirms(r#"{"status":"unloaded"}"#));
        assert!(!operation.confirmation.confirms(r#"{"status":"loaded"}"#));
    }

    #[test]
    fn a_sleep_offload_moves_memory_into_host_ram() {
        let adapter = idle_adapter();
        let operation = adapter
            .operation(ActionKind::SleepModel, &workload("unit:mock.service"))
            .expect("sleep supported");
        assert!(
            operation.expected_host_ram_delta > 0,
            "offload raises host RAM"
        );
        assert!(
            operation.expected_device_ram_delta < 0,
            "and frees device memory"
        );
    }

    #[test]
    fn ownership_is_by_scope_prefix() {
        let adapter = idle_adapter();
        assert!(adapter.owns(&workload("unit:mock.service")));
        assert!(!adapter.owns(&workload("unit:other.service")));
    }

    #[test]
    fn an_unsupported_action_has_no_operation() {
        let adapter = idle_adapter();
        assert!(adapter
            .operation(ActionKind::CgroupSoftLimit, &workload("unit:mock.service"))
            .is_none());
    }
}
