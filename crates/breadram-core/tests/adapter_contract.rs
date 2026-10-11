//! The adapter contract, exercised end to end.
//!
//! These tests hold the module's promises: generic management works with no
//! adapters at all, a mock adapter changes policy behaviour without any change
//! to policy, probes are read-only and endpoint-restricted, and a model API that
//! does not expose administration is never assumed to.

use breadram_core::adapter::{
    classify, AdapterCapabilities, AdapterCapability, AdapterRegistry, CircuitConfig, MockAdapter,
    NoSleep, ProbeClient, ProbeError, ProbeRequest, RecordingSleeper, Response, ScriptedTransport,
    TransportError, CAPABILITY_PROBE_VERSION,
};
use breadram_core::capability::CapabilityState;
use breadram_core::config::{ActionKind, Automation, Budget, Config};
use breadram_core::inventory::{
    collect_inventory, ControlLevel, MemoryAccounting, Workload, WorkloadId, CAP_PROC_INVENTORY,
};
use breadram_core::policy::{
    plan, ActionOutcome, ActionRecord, PlanChannel, PlannerInput, PolicyHistory, PressureState,
};
use breadram_core::telemetry::fs::{RealFs, SystemClock};
use breadram_core::telemetry::{
    collect_host, MemInfo, MemorySnapshot, PsiLine, PsiReading, VmStat,
};

const GIB: u64 = 1024 * 1024 * 1024;
const TOTAL: u64 = 64 * GIB;
const NOW: u64 = 1_000_000;

fn snapshot() -> MemorySnapshot {
    MemorySnapshot {
        observed_at_ms: NOW,
        meminfo: MemInfo {
            total: TOTAL,
            free: GIB,
            available: Some(4 * GIB),
            buffers: 0,
            cached: 0,
            swap_total: 0,
            swap_free: 0,
            anon: None,
            shmem: None,
        },
        psi: Some(PsiReading {
            some: PsiLine {
                avg10: 30.0,
                avg60: 30.0,
                avg300: 30.0,
                total_us: 0,
            },
            full: None,
        }),
        vmstat: VmStat::default(),
        capabilities: Default::default(),
    }
}

fn model_workload(scope: &str, bytes: u64) -> Workload {
    Workload {
        id: WorkloadId {
            scope: scope.to_string(),
            generation: 1,
        },
        name: scope.to_string(),
        pids: vec![100],
        accounting: MemoryAccounting::Cgroup {
            current_bytes: Some(bytes),
        },
        control: ControlLevel::Eligible,
    }
}

fn config(actions: &[ActionKind]) -> Config {
    Config {
        automation: Automation {
            allowed_actions: actions.to_vec(),
        },
        ..Config::default()
    }
}

fn plan_with(
    workloads: &[Workload],
    config: &Config,
    activity: &dyn breadram_core::policy::ActivitySource,
) -> breadram_core::policy::Plan {
    let snapshot = snapshot();
    plan(
        PlannerInput {
            state: PressureState::Critical,
            snapshot: &snapshot,
            workloads,
            config,
            history: &PolicyHistory::new(16, 60_000, 10),
            activity,
        },
        PlanChannel::Automatic,
    )
}

#[test]
fn generic_management_works_with_no_adapters_registered() {
    // The host itself is readable through the generic path.
    assert!(collect_host(&RealFs, &SystemClock::new()).is_ok());
    assert!(collect_inventory(&RealFs)
        .capabilities
        .is_usable(CAP_PROC_INVENTORY));

    // And an empty registry still lets generic, non adapter control be planned.
    let registry = AdapterRegistry::new();
    assert!(registry.is_empty());
    let workloads = vec![model_workload("exe:build", 9 * GIB)];
    let config = Config {
        automation: Automation {
            allowed_actions: vec![ActionKind::CgroupSoftLimit],
        },
        budgets: vec![Budget {
            scope: "exe:build".to_string(),
            max_bytes: GIB,
        }],
        ..Config::default()
    };
    let plan = plan_with(&workloads, &config, &registry.activity_source(&workloads));
    assert_eq!(plan.actions.len(), 1);
    assert_eq!(plan.actions[0].kind, ActionKind::CgroupSoftLimit);
}

#[test]
fn with_no_adapter_an_idle_release_is_never_planned() {
    // Nothing knows anything, so nothing may be treated as idle.
    let registry = AdapterRegistry::new();
    let workloads = vec![model_workload("unit:ollama.service", 12 * GIB)];
    let plan = plan_with(
        &workloads,
        &config(&[ActionKind::UnloadIdleModel]),
        &registry.activity_source(&workloads),
    );
    assert!(plan.is_empty());
    assert!(plan
        .explanation
        .iter()
        .any(|line| line.contains("none is proven")));
}

#[test]
fn a_registered_adapter_changes_policy_behaviour_without_changing_policy() {
    let workloads = vec![model_workload("unit:mock.service", 12 * GIB)];

    // The same policy call, with the registry as the activity source.
    let mut registry = AdapterRegistry::new();
    registry.register(Box::new(
        MockAdapter::new("mock", "http://127.0.0.1:9000")
            .owning("unit:mock.service")
            .with_capabilities(AdapterCapabilities::configured(&[
                AdapterCapability::Activity,
                AdapterCapability::UnloadModel,
            ]))
            .with_activity(breadram_core::policy::ActivityEvidence::Idle { since_ms: 0 })
            .with_memory(12 * GIB, 0),
    ));

    let plan = plan_with(
        &workloads,
        &config(&[ActionKind::UnloadIdleModel]),
        &registry.activity_source(&workloads),
    );
    assert_eq!(plan.actions.len(), 1);
    assert_eq!(plan.actions[0].kind, ActionKind::UnloadIdleModel);
    assert_ne!(
        registry.operation(ActionKind::UnloadIdleModel, &workloads[0]),
        None
    );
}

#[test]
fn an_unknown_answering_adapter_does_not_suppress_one_that_knows() {
    // The first adapter owns the workload but knows nothing; the second knows.
    let mut registry = AdapterRegistry::new();
    registry.register(Box::new(
        MockAdapter::new("silent", "http://127.0.0.1:1")
            .owning("unit:mock.service")
            .with_capabilities(AdapterCapabilities::configured(&[
                AdapterCapability::Activity,
            ])),
    ));
    registry.register(Box::new(
        MockAdapter::new("chatty", "http://127.0.0.1:2")
            .owning("unit:mock.service")
            .with_capabilities(AdapterCapabilities::configured(&[
                AdapterCapability::Activity,
            ]))
            .with_activity(breadram_core::policy::ActivityEvidence::Idle { since_ms: 0 }),
    ));
    let workload = model_workload("unit:mock.service", GIB);
    assert_eq!(
        registry.activity(&workload),
        breadram_core::policy::ActivityEvidence::Idle { since_ms: 0 }
    );
}

#[test]
fn a_model_api_without_administration_is_not_assumed_to_have_it() {
    // An OpenAI-compatible inference surface answering 404 for an
    // administration probe implies no administration capability.
    let capabilities = AdapterCapabilities::from_probe(
        CAPABILITY_PROBE_VERSION,
        [(
            AdapterCapability::ModelAdministration,
            classify(&Response::new(404, "not found")),
        )],
    );
    assert!(!capabilities.supports(AdapterCapability::ModelAdministration));

    // An unprobed capability is unsupported for the same reason.
    let unprobed = AdapterCapabilities::from_probe(CAPABILITY_PROBE_VERSION, []);
    assert!(!unprobed.supports(AdapterCapability::ModelAdministration));
    assert!(unprobed.is_observation_only());
}

#[test]
fn probes_are_read_only_and_confined_to_configured_endpoints() {
    let endpoint = "http://127.0.0.1:9000";
    let transport = ScriptedTransport::new().push(Ok(Response::new(200, "{}")));
    let sleeper = RecordingSleeper::new();
    let clock = SystemClock::new();
    let probe = ProbeClient::new(
        &transport,
        &sleeper,
        &clock,
        CircuitConfig::default(),
        vec![endpoint.to_string()],
    );

    let mut registry = AdapterRegistry::new();
    registry.register(Box::new(
        MockAdapter::new("mock", endpoint)
            .owning("unit:mock.service")
            .with_capabilities(AdapterCapabilities::configured(&[
                AdapterCapability::Discovery,
            ])),
    ));

    let requests = registry.discovery_requests();
    assert_eq!(requests.len(), 1);
    for request in &requests {
        // Read-only, and inside the configured endpoint.
        assert!(probe.allows(&request.url));
        probe.probe(request).expect("probe allowed");
    }
    assert!(transport
        .calls()
        .iter()
        .all(|call| call.method == breadram_core::adapter::Method::Get));

    // A request aimed anywhere else is refused before it leaves.
    assert!(matches!(
        probe.probe(&ProbeRequest::get(
            "http://10.0.0.5:9000/breadram/capabilities"
        )),
        Err(ProbeError::NotAllowed { .. })
    ));
}

#[test]
fn a_credential_never_reaches_a_rendered_request() {
    let request =
        ProbeRequest::get("https://api.example.net/v1/models").with_bearer("hunter2-secret");
    let rendered = format!("{request:?}");
    assert!(rendered.contains("[redacted]"));
    assert!(!rendered.contains("hunter2-secret"));

    let transport = ScriptedTransport::new().push(Err(TransportError::Failure(
        "connection refused".to_string(),
    )));
    let sleeper = NoSleep;
    let clock = SystemClock::new();
    let probe = ProbeClient::new(
        &transport,
        &sleeper,
        &clock,
        CircuitConfig::default(),
        vec!["https://api.example.net".to_string()],
    );
    let error = probe.probe(&request).expect_err("refused");
    assert!(!error.to_string().contains("hunter2-secret"));
    assert!(!format!("{error:?}").contains("hunter2-secret"));
}

#[test]
fn a_retried_probe_reports_its_attempts_and_leaves_a_history() {
    let transport = ScriptedTransport::new()
        .push(Err(TransportError::Failure("refused".to_string())))
        .push(Ok(Response::new(200, "{}")));
    let sleeper = RecordingSleeper::new();
    let clock = SystemClock::new();
    let config = CircuitConfig {
        timeout_ms: 500,
        backoff_ms: 100,
        ..CircuitConfig::default()
    };
    let probe = ProbeClient::new(
        &transport,
        &sleeper,
        &clock,
        config,
        vec!["http://127.0.0.1:9000".to_string()],
    );
    let response = probe
        .probe(&ProbeRequest::get(
            "http://127.0.0.1:9000/breadram/capabilities",
        ))
        .expect("second attempt succeeds");
    assert_eq!(response.status, 200);
    assert_eq!(transport.call_count(), 2);
    assert_eq!(transport.timeouts(), vec![500, 500]);
    assert_eq!(sleeper.waits(), vec![100]);
}

#[test]
fn a_history_record_can_mark_an_adapter_action_ineffective() {
    // The policy's backoff works the same whether the action came from a
    // generic control or an adapter operation.
    let workloads = vec![model_workload("exe:build", 9 * GIB)];
    let config = Config {
        automation: Automation {
            allowed_actions: vec![ActionKind::CgroupSoftLimit],
        },
        budgets: vec![Budget {
            scope: "exe:build".to_string(),
            max_bytes: GIB,
        }],
        ..Config::default()
    };
    let mut history = PolicyHistory::new(16, 60_000, 10);
    history.record(ActionRecord {
        target: workloads[0].id.clone(),
        kind: ActionKind::CgroupSoftLimit,
        at_ms: NOW - 600_000,
        outcome: ActionOutcome::Ineffective,
    });
    let snapshot = snapshot();
    let registry = AdapterRegistry::new();
    let plan = plan(
        PlannerInput {
            state: PressureState::Critical,
            snapshot: &snapshot,
            workloads: &workloads,
            config: &config,
            history: &history,
            activity: &registry.activity_source(&workloads),
        },
        PlanChannel::Automatic,
    );
    assert!(plan.is_empty());
    assert!(plan
        .explanation
        .iter()
        .any(|line| line.contains("already proved ineffective")));
}

#[test]
fn a_capability_response_is_classified_conservatively() {
    assert_eq!(
        classify(&Response::new(200, "ok")),
        CapabilityState::Available
    );
    assert!(classify(&Response::new(403, "forbidden"))
        .reason()
        .is_some());
    assert!(!classify(&Response::new(405, "no")).is_usable());
    assert!(!classify(&Response::new(500, "boom")).is_usable());
}
