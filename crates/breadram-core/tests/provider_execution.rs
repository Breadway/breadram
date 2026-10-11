//! Provider actions over a real socket, against an in-process fake provider.
//!
//! Nothing here needs a model server or a GPU. A fake service speaks the
//! documented Ollama API on a loopback port, and the real path runs against it:
//! the real probe client, the real HTTP transport, the real adapter, the real
//! planner, the real coordinator, and the real chained executor. The assertions
//! are about what reached the wire and what the journal recorded, not about a
//! mock of the logic under test.
//!
//! Four properties are pinned:
//!
//! 1. An idle model that the provider holds is planned, released, confirmed, and
//!    journalled with a **measured** effect, and the entry names the cold start
//!    the release costs.
//! 2. A backend with a live request is never written to at all.
//! 3. A provider that cannot be reached, and one whose effect cannot be
//!    confirmed, are journalled honestly rather than reported as success.
//! 4. The chain sends each action to the executor that claims it: a host control
//!    is never sent to a provider, and a provider action is never sent to a host
//!    control.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use breadram_core::adapter::{
    refresh_adapters, AdapterCapabilities, AdapterCapability, CircuitConfig, HttpTransport,
    NoSleep, OllamaAdapter,
};
use breadram_core::capability::CapabilityReport;
use breadram_core::config::{ActionKind, Automation, Config};
use breadram_core::coordinator::{
    ActionState, AppliedChange, Cancellation, ChainedExecutor, Confirmation, Coordinator, Executor,
    ExecutorError, MemoryObservation, OwnerToken, Precondition,
};
use breadram_core::execution::{execute_plan, ExecutionPass};
use breadram_core::inventory::{ControlLevel, MemoryAccounting, Workload, WorkloadId};
use breadram_core::policy::{
    plan, Plan, PlanChannel, PlannerInput, PolicyHistory, PressureState, StateMachine,
};
use breadram_core::serving::TrackingCompleteness;
use breadram_core::telemetry::fs::Clock;
use breadram_core::telemetry::{MemInfo, MemorySnapshot, PsiLine, PsiReading, VmStat};
use breadram_core::ManagementMode;

const GIB: u64 = 1024 * 1024 * 1024;
const TOTAL: u64 = 64 * GIB;
const SCOPE: &str = "unit:ollama.service";
/// 2026-10-11T00:00:00Z, so the model's reported expiry below is read against a
/// fixed instant rather than the clock the test happens to run on.
const FIXED_NOW_MS: u64 = 1_791_676_800_000;
const MODEL: &str = "llama3:8b";
/// The model's bytes: 4 GiB total, 3 GiB of it in device memory.
const MODEL_BYTES: u64 = 4 * GIB;
const MODEL_VRAM_BYTES: u64 = 3 * GIB;
const MODEL_HOST_BYTES: u64 = MODEL_BYTES - MODEL_VRAM_BYTES;
/// The journal capacity these tests use.
const JOURNAL_CAPACITY: usize = 64;

/// A clock the test controls, so activity and timestamps are exact.
struct TestClock(u64);

impl Clock for TestClock {
    fn monotonic_ms(&self) -> u64 {
        self.0
    }
}

/// The provider's state, shared with the serving thread.
#[derive(Default)]
struct FakeState {
    /// Whether the model is currently resident.
    loaded: Mutex<bool>,
    /// Whether an accepted unload nevertheless leaves the model resident, which
    /// is what a provider that ignores the request looks like.
    stubborn: Mutex<bool>,
    /// Every request line the fake saw, in order.
    requests: Mutex<Vec<String>>,
    /// Every request body the fake read, in order.
    bodies: Mutex<Vec<String>>,
}

/// A fake Ollama service on a loopback port.
struct FakeProvider {
    endpoint: String,
    state: Arc<FakeState>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl FakeProvider {
    fn start(loaded: bool) -> Self {
        Self::start_with(loaded, false)
    }

    /// A provider that accepts an unload but keeps the model resident.
    fn stubborn() -> Self {
        Self::start_with(true, true)
    }

    fn start_with(loaded: bool, stubborn: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback port");
        let endpoint = format!("http://{}", listener.local_addr().expect("a bound address"));
        listener
            .set_nonblocking(true)
            .expect("non-blocking accepts");
        let state = Arc::new(FakeState {
            loaded: Mutex::new(loaded),
            stubborn: Mutex::new(stubborn),
            ..FakeState::default()
        });
        let stop = Arc::new(AtomicBool::new(false));
        let serving = Arc::clone(&state);
        let stopping = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            while !stopping.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let _ = handle_connection(stream, &serving);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            endpoint,
            state,
            stop,
            handle: Some(handle),
        }
    }

    fn requests(&self) -> Vec<String> {
        self.state.requests.lock().expect("not poisoned").clone()
    }

    fn bodies(&self) -> Vec<String> {
        self.state.bodies.lock().expect("not poisoned").clone()
    }
}

impl Drop for FakeProvider {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Answer one request, reading the body so the write is real rather than assumed.
fn handle_connection(mut stream: TcpStream, state: &FakeState) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    // Read the head, then exactly the body the head declares.
    let head_end = loop {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Ok(());
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(position) = find(&buffer, b"\r\n\r\n") {
            break position + 4;
        }
    };
    let head = String::from_utf8_lossy(&buffer[..head_end]).to_string();
    let length = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())?
        })
        .unwrap_or(0);
    while buffer.len() < head_end + length {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    let body = String::from_utf8_lossy(&buffer[head_end..]).to_string();
    let request_line = head.lines().next().unwrap_or_default().to_string();
    state
        .requests
        .lock()
        .expect("not poisoned")
        .push(request_line.clone());
    if !body.is_empty() {
        state
            .bodies
            .lock()
            .expect("not poisoned")
            .push(body.clone());
    }

    let path = request_line.split_whitespace().nth(1).unwrap_or("/");
    let response = match (request_line.split_whitespace().next(), path) {
        (Some("GET"), "/api/version") => json(200, r#"{"version":"0.12.6"}"#),
        (Some("GET"), "/api/ps") => {
            let loaded = *state.loaded.lock().expect("not poisoned");
            json(200, &ps_body(loaded))
        }
        (Some("POST"), "/api/generate") => {
            // The documented unload is a generate with a zero keep-alive.
            let unloading = body.contains("\"keep_alive\":0");
            let stubborn = *state.stubborn.lock().expect("not poisoned");
            if unloading && !stubborn {
                *state.loaded.lock().expect("not poisoned") = false;
            }
            json(
                200,
                if unloading {
                    r#"{"done":true}"#
                } else {
                    r#"{"done":false}"#
                },
            )
        }
        _ => json(404, r#"{"error":"not found"}"#),
    };
    stream.write_all(&response)?;
    stream.flush()
}

fn ps_body(loaded: bool) -> String {
    if loaded {
        format!(
            r#"{{"models":[{{"name":"{MODEL}","model":"{MODEL}","size":{MODEL_BYTES},"size_vram":{MODEL_VRAM_BYTES},"expires_at":"2026-10-11T16:00:00Z"}}]}}"#
        )
    } else {
        r#"{"models":[]}"#.to_string()
    }
}

fn json(status: u16, body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn workload() -> Workload {
    Workload {
        id: WorkloadId {
            scope: SCOPE.to_string(),
            generation: 7,
        },
        name: "ollama.service".to_string(),
        pids: vec![4242],
        accounting: MemoryAccounting::Cgroup {
            current_bytes: Some(4 * GIB),
        },
        control: ControlLevel::Eligible,
        limit_bytes: None,
    }
}

/// A provider whose requests are all visible, which is what makes a resident
/// model releasable.
fn registry(endpoint: &str) -> breadram_core::adapter::AdapterRegistry {
    let mut registry = breadram_core::adapter::AdapterRegistry::new();
    registry.register(Box::new(
        OllamaAdapter::new("ollama", endpoint, SCOPE)
            .with_tracking(TrackingCompleteness::Complete, 0)
            .with_reference_time(FIXED_NOW_MS)
            .with_capabilities(AdapterCapabilities::configured(&[
                AdapterCapability::Discovery,
                AdapterCapability::Inventory,
                AdapterCapability::Activity,
                AdapterCapability::UnloadModel,
            ])),
    ));
    registry
}

fn config() -> Config {
    Config {
        automation: Automation {
            allowed_actions: vec![ActionKind::UnloadIdleModel],
        },
        ..Config::default()
    }
}

/// Drive the real state machine through a sustained episode, so the plan is made
/// against a state the machine actually reached.
fn pressured(config: &Config) -> PressureState {
    let mut machine = StateMachine::new(config);
    let mut state = PressureState::Healthy;
    for (at_ms, avg10, available_fraction) in [
        (1_000, 1.0, 0.5),
        (4_000, 30.0, 0.1),
        (16_000, 30.0, 0.1),
        (60_000, 30.0, 0.1),
    ] {
        state = machine.observe(&snapshot(at_ms, avg10, available_fraction));
    }
    state
}

fn snapshot(now_ms: u64, avg10: f64, available_fraction: f64) -> MemorySnapshot {
    MemorySnapshot {
        observed_at_ms: now_ms,
        meminfo: MemInfo {
            total: TOTAL,
            free: 0,
            available: Some((TOTAL as f64 * available_fraction) as u64),
            buffers: 0,
            cached: 0,
            swap_total: 0,
            swap_free: 0,
            anon: None,
            shmem: None,
        },
        psi: Some(PsiReading {
            some: PsiLine {
                avg10,
                avg60: avg10,
                avg300: avg10,
                total_us: 0,
            },
            full: None,
        }),
        vmstat: VmStat::default(),
        capabilities: CapabilityReport::new(),
    }
}

/// A host control that fails loudly if it is ever asked to do anything.
///
/// It claims the cgroup actions so the chain has two members, and it is never
/// expected to act on a provider action.
#[derive(Default)]
struct RefusingControl {
    preconditioned: Mutex<Vec<ActionKind>>,
}

impl Executor for RefusingControl {
    fn supports(&self, kind: ActionKind) -> bool {
        matches!(
            kind,
            ActionKind::CgroupSoftLimit | ActionKind::ReclaimProtection
        )
    }

    fn precondition(&self, request: &breadram_core::coordinator::ActionRequest) -> Precondition {
        self.preconditioned
            .lock()
            .expect("not poisoned")
            .push(request.kind);
        Precondition::Ready
    }

    fn apply(
        &self,
        request: &breadram_core::coordinator::ActionRequest,
    ) -> Result<AppliedChange, ExecutorError> {
        Err(ExecutorError::Failed(format!(
            "this control should never have been asked to apply {:?}",
            request.kind
        )))
    }

    fn confirm(&self, _request: &breadram_core::coordinator::ActionRequest) -> Confirmation {
        Confirmation::Refused {
            reason: "this control should never have been asked to confirm".to_string(),
        }
    }

    fn observe(&self, _target: &WorkloadId) -> MemoryObservation {
        MemoryObservation::default()
    }

    fn read_value(&self, key: &str) -> Result<Option<String>, ExecutorError> {
        Err(ExecutorError::Unsupported(format!("{key} is not mine")))
    }

    fn write_value(&self, key: &str, _value: &str) -> Result<(), ExecutorError> {
        Err(ExecutorError::Unsupported(format!("{key} is not mine")))
    }
}

/// The whole path, over a real socket, up to the journal.
struct Harness {
    provider: FakeProvider,
    registry: breadram_core::adapter::AdapterRegistry,
    workloads: Vec<Workload>,
    config: Config,
    plan: Plan,
    transport: HttpTransport,
}

impl Harness {
    /// Refresh the adapter from the provider, then plan, exactly as the daemon
    /// does: read the provider first, then reason about it.
    fn start(provider: FakeProvider, clock: &TestClock) -> Self {
        let registry = registry(&provider.endpoint);
        let workloads = vec![workload()];
        let config = config();
        let transport = HttpTransport::new();
        // The daemon's refresh step, unchanged.
        let notes = refresh_adapters(
            &registry,
            &transport,
            &NoSleep,
            clock,
            CircuitConfig::default(),
        );
        assert!(
            notes.is_empty(),
            "the fake provider is reachable: {notes:?}"
        );

        // The real state machine has to hold an episode before it will call the
        // host pressured: a single bad reading is a spike, not pressure.
        let state = pressured(&config);
        assert_eq!(state, PressureState::Critical, "the episode is critical");
        let observation = snapshot(clock.monotonic_ms(), 30.0, 0.1);
        let history = PolicyHistory::new(64, 60_000, 10);
        let plan = plan(
            PlannerInput {
                state,
                snapshot: &observation,
                workloads: &workloads,
                config: &config,
                history: &history,
                activity: &registry.activity_source(&workloads),
            },
            PlanChannel::Automatic,
        );
        Self {
            provider,
            registry,
            workloads,
            config,
            plan,
            transport,
        }
    }

    /// Run one plan through the real chain and the real coordinator.
    fn execute(
        &self,
        coordinator: &mut Coordinator,
        clock: &TestClock,
        mode: ManagementMode,
    ) -> ExecutionPass {
        let adapter = breadram_core::adapter::AdapterExecutor::new(
            &self.registry,
            &self.workloads,
            &self.transport,
            &self.transport,
            &NoSleep,
            clock,
        );
        let control = RefusingControl::default();
        let executor = ChainedExecutor::new(vec![&control, &adapter]);
        execute_plan(
            coordinator,
            &self.plan,
            &self.config,
            mode,
            &self.workloads,
            &executor,
            clock.monotonic_ms(),
            &Cancellation::new(),
        )
    }
}

fn coordinator() -> Coordinator {
    Coordinator::new(OwnerToken("test-daemon".to_string()), JOURNAL_CAPACITY)
}

#[test]
fn an_idle_model_is_released_and_the_effect_is_measured() {
    let clock = TestClock(60_000);
    let provider = FakeProvider::start(true);
    let harness = Harness::start(provider, &clock);

    // The provider is fresh, idle, and holds a releasable model, so exactly one
    // action is proposed.
    assert_eq!(
        harness.plan.actions.len(),
        1,
        "{:?}",
        harness.plan.explanation
    );
    assert_eq!(harness.plan.actions[0].kind, ActionKind::UnloadIdleModel);
    assert_eq!(harness.plan.actions[0].target.scope, SCOPE);

    let mut coordinator = coordinator();
    let pass = harness.execute(&mut coordinator, &clock, ManagementMode::Automatic);
    assert_eq!(pass.results.len(), 1, "{}", pass.summary());
    assert_eq!(pass.results[0].state, ActionState::Confirmed);
    assert!(
        pass.results[0].detail.contains("applied and confirmed"),
        "{}",
        pass.results[0].detail
    );

    // The write reached the wire as the documented unload, for this model only.
    let requests = harness.provider.requests();
    assert!(
        requests.contains(&"POST /api/generate HTTP/1.1".to_string()),
        "{requests:?}"
    );
    assert_eq!(
        harness.provider.bodies(),
        vec![format!(r#"{{"model":"{MODEL}","keep_alive":0}}"#)]
    );

    // The journal has the measured effect, both sides, and the cost of the
    // release, which the operator has to be able to see.
    let history: Vec<_> = coordinator.journal().history().collect();
    assert_eq!(history.len(), 1, "{history:?}");
    let entry = history[0];
    assert_eq!(entry.scope, SCOPE);
    assert_eq!(entry.kind, Some(ActionKind::UnloadIdleModel));
    assert_eq!(entry.state, ActionState::Confirmed);
    let effect = entry.effect.clone().expect("a measured effect");
    // The requested figure is the plan's estimate, sized from the workload's
    // memory; the observed figures are the provider's own readings, before and
    // after, which is what makes the effect a measurement rather than a claim.
    assert_eq!(
        effect.observed_host_ram_delta,
        Some(-(MODEL_HOST_BYTES as i64))
    );
    assert_eq!(
        effect.observed_device_ram_delta,
        Some(-(MODEL_VRAM_BYTES as i64))
    );
    assert_eq!(effect.requested_host_ram_delta, -(4 * GIB as i64));
    assert!(
        entry.reason.contains("cold-start"),
        "the release cost is named: {}",
        entry.reason
    );
    assert!(
        entry.reason.contains("the provider expects"),
        "the plan's estimate and the provider's expectation are both stated: {}",
        entry.reason
    );

    // A second pass is a no-op: the provider reports nothing resident, so there
    // is nothing to release and nothing is written twice.
    let again = harness.execute(&mut coordinator, &clock, ManagementMode::Automatic);
    assert_eq!(again.results.len(), 1, "{again:?}");
    assert_eq!(
        again.results[0].state,
        ActionState::SkippedDuplicate,
        "the same action is not written twice: {again:?}"
    );
    assert_eq!(
        harness
            .provider
            .requests()
            .iter()
            .filter(|line| line.starts_with("POST"))
            .count(),
        1,
        "exactly one write reached the provider: {:?}",
        harness.provider.requests()
    );
}

#[test]
fn a_backend_with_a_request_in_flight_is_never_written_to() {
    let clock = TestClock(60_000);
    let provider = FakeProvider::start(true);
    // The gateway reports a request that is still running, which is the only
    // thing that can prove a backend is busy rather than merely resident.
    let registry = live_registry(&provider.endpoint, clock.monotonic_ms());
    let workloads = vec![workload()];
    let config = config();
    let transport = HttpTransport::new();
    assert!(refresh_adapters(
        &registry,
        &transport,
        &NoSleep,
        &clock,
        CircuitConfig::default()
    )
    .is_empty());

    // Nothing is planned, whatever the pressure is.
    let observation = snapshot(clock.monotonic_ms(), 30.0, 0.1);
    let history = PolicyHistory::new(64, 60_000, 10);
    let plan = plan(
        PlannerInput {
            state: PressureState::Critical,
            snapshot: &observation,
            workloads: &workloads,
            config: &config,
            history: &history,
            activity: &registry.activity_source(&workloads),
        },
        PlanChannel::Automatic,
    );
    assert!(
        plan.actions.is_empty(),
        "a busy backend is not planned against: {:?}",
        plan.explanation
    );
    assert!(
        plan.explanation
            .iter()
            .any(|line| line.contains("idle") || line.contains("activity")),
        "the reason is explained: {:?}",
        plan.explanation
    );

    // And if the action were requested anyway, the executor refuses it before
    // anything reaches the wire, because a request is in flight.
    let adapter = breadram_core::adapter::AdapterExecutor::new(
        &registry, &workloads, &transport, &transport, &NoSleep, &clock,
    );
    use breadram_core::coordinator::ActionRequest;
    let request = ActionRequest::new(
        workload().id,
        ActionKind::UnloadIdleModel,
        "testing the busy path",
        clock.monotonic_ms(),
    );
    match adapter.precondition(&request) {
        Precondition::Denied { reason } => {
            assert!(reason.contains("reports activity"), "{reason}")
        }
        other => panic!("expected a denial, got {other:?}"),
    }
    assert!(
        provider
            .requests()
            .iter()
            .all(|line| line.starts_with("GET")),
        "nothing was written: {:?}",
        provider.requests()
    );
}

#[test]
fn a_provider_that_cannot_be_reached_is_reported_and_never_releases() {
    let clock = TestClock(60_000);
    // A port nothing is listening on: the probe fails and the adapter says so.
    let registry = registry("http://127.0.0.1:9");
    let transport = HttpTransport::new();
    let notes = refresh_adapters(
        &registry,
        &transport,
        &NoSleep,
        &clock,
        CircuitConfig::default(),
    );
    assert_eq!(notes.len(), 2, "both reads are reported: {notes:?}");
    assert!(
        notes
            .iter()
            .all(|note| note.contains("could not be reached")),
        "{notes:?}"
    );

    // Nothing was read, so nothing may be released: the executor refuses before
    // anything reaches the wire, and it says which read is missing.
    let workloads = vec![workload()];
    let adapter = breadram_core::adapter::AdapterExecutor::new(
        &registry, &workloads, &transport, &transport, &NoSleep, &clock,
    );
    use breadram_core::coordinator::ActionRequest;
    let request = ActionRequest::new(
        workload().id,
        ActionKind::UnloadIdleModel,
        "testing the unreachable path",
        clock.monotonic_ms(),
    );
    match adapter.precondition(&request) {
        Precondition::Unsupported { reason } => {
            assert!(reason.contains("nothing to release"), "{reason}")
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
    match adapter.apply(&request) {
        Err(ExecutorError::Unsupported(_)) => {}
        other => panic!("nothing may be written: {other:?}"),
    }
}

#[test]
fn a_release_the_provider_ignores_is_journalled_as_unconfirmed() {
    let clock = TestClock(60_000);
    // The provider answers the unload and keeps the model resident, so the
    // confirmation read still lists it.
    let harness = Harness::start(FakeProvider::stubborn(), &clock);
    assert_eq!(harness.plan.actions.len(), 1);

    let mut coordinator = coordinator();
    let pass = harness.execute(&mut coordinator, &clock, ManagementMode::Automatic);
    assert_eq!(pass.results.len(), 1, "{}", pass.summary());
    assert_eq!(pass.results[0].state, ActionState::Unconfirmed);
    assert!(
        pass.results[0].detail.contains("still reports"),
        "the effect is not claimed: {}",
        pass.results[0].detail
    );

    // The write did happen, and the journal says exactly what was and was not
    // achieved: the effect is not there, and the entry does not claim it is.
    assert!(harness
        .provider
        .requests()
        .contains(&"POST /api/generate HTTP/1.1".to_string()));
    let history: Vec<_> = coordinator.journal().history().collect();
    assert_eq!(history.len(), 1, "{history:?}");
    let entry = history[0];
    assert_eq!(entry.state, ActionState::Unconfirmed);
    assert!(
        entry.reason.contains("cold-start"),
        "the cost is still recorded: {}",
        entry.reason
    );
    assert_eq!(
        entry
            .effect
            .clone()
            .expect("a measured effect")
            .observed_host_ram_delta,
        Some(0),
        "the model is still resident, and the measurement says so"
    );
}

#[test]
fn monitor_mode_explains_the_release_and_writes_nothing() {
    let clock = TestClock(60_000);
    let provider = FakeProvider::start(true);
    let harness = Harness::start(provider, &clock);
    assert_eq!(harness.plan.actions.len(), 1);

    let mut coordinator = coordinator();
    let pass = harness.execute(&mut coordinator, &clock, ManagementMode::Monitor);
    assert!(pass.results.is_empty(), "{pass:?}");
    assert!(
        pass.notes.iter().any(|note| note.contains("monitor")),
        "the reason is explained: {:?}",
        pass.notes
    );
    assert!(
        harness
            .provider
            .requests()
            .iter()
            .all(|line| line.starts_with("GET")),
        "a monitor mode pass must not write: {:?}",
        harness.provider.requests()
    );
}

/// A provider whose gateway reports a request that is still running.
fn live_registry(endpoint: &str, now_ms: u64) -> breadram_core::adapter::AdapterRegistry {
    let mut registry = breadram_core::adapter::AdapterRegistry::new();
    let adapter = OllamaAdapter::new("ollama", endpoint, SCOPE)
        .with_tracking(TrackingCompleteness::Complete, 0)
        .with_reference_time(FIXED_NOW_MS)
        .with_capabilities(AdapterCapabilities::configured(&[
            AdapterCapability::Discovery,
            AdapterCapability::Inventory,
            AdapterCapability::Activity,
            AdapterCapability::UnloadModel,
        ]));
    let _lease = adapter.begin_request(now_ms - 1_000);
    registry.register(Box::new(adapter));
    registry
}
