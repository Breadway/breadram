//! Cross-repo integration for the breadsearch adapter.
//!
//! A scripted breadmill speaks the real socket protocol over a real Unix
//! socket, and the real `UnixSocketRunner` carries every round trip, so what
//! is exercised is the wire contract, not a mock of it: busy refusal,
//! release with confirmation, demand reload, failed reload, and breadsearch
//! completely uninstalled. The fake models the daemon's documented behavior
//! (verified against a real breadmill during development); it is not a second
//! implementation to keep in sync, only a script of answers.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use breadram_core::adapter::{
    AdapterExecutor, AdapterRegistry, NoSleep, ScriptedTransport, SocketRunner, UnixSocketRunner,
};
use breadram_core::config::{ActionKind, Config, ProviderEndpoint, ProviderKind};
use breadram_core::coordinator::{
    ActionRequest, ActionState, Cancellation, Confirmation, Coordinator, Executor, OwnerToken,
};
use breadram_core::inventory::{ControlLevel, MemoryAccounting, Workload, WorkloadId};
use breadram_core::telemetry::fs::Clock;
use breadram_core::ManagementMode;

const SCOPE: &str = "unit:breadmill.service";

/// A clock the test controls.
struct TestClock(u64);

impl Clock for TestClock {
    fn monotonic_ms(&self) -> u64 {
        self.0
    }
}

fn workload() -> Workload {
    Workload {
        id: WorkloadId {
            scope: SCOPE.to_string(),
            generation: 1,
        },
        name: "breadmill.service".to_string(),
        pids: vec![100],
        accounting: MemoryAccounting::Cgroup {
            current_bytes: Some(900_000_000),
        },
        control: ControlLevel::Eligible,
        limit_bytes: None,
        ancestor_bytes: None,
    }
}

fn request() -> ActionRequest {
    ActionRequest::new(
        workload().id.clone(),
        ActionKind::UnloadIdleModel,
        "testing",
        60_000,
    )
}

/// One scripted breadmill: answers in connection order, recording requests.
struct FakeMill {
    socket_path: String,
    seen: Arc<Mutex<Vec<String>>>,
    dir: std::path::PathBuf,
}

impl FakeMill {
    fn start(answers: Vec<&str>) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "breadram-mill-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let socket_path = dir.join("breadmill.sock").to_string_lossy().into_owned();
        let answers: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(
            answers.into_iter().map(str::to_string).collect(),
        ));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let listener = UnixListener::bind(&socket_path).unwrap();
        let count = answers.lock().unwrap().len();
        let worker_answers = Arc::clone(&answers);
        let worker_seen = Arc::clone(&seen);
        std::thread::spawn(move || {
            for _ in 0..count {
                let Ok((stream, _)) = listener.accept() else {
                    break;
                };
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                if reader.read_line(&mut line).is_err() {
                    break;
                }
                worker_seen.lock().unwrap().push(line.trim().to_string());
                let answer = worker_answers
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or_else(|| r#"{"type":"error","message":"unscripted"}"#.to_string());
                let _ = stream.try_clone().unwrap().write_all(answer.as_bytes());
            }
        });
        Self {
            socket_path,
            seen,
            dir,
        }
    }

    fn requests(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }
}

impl Drop for FakeMill {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn registry_for(socket_path: &str) -> AdapterRegistry {
    let config = Config {
        providers: vec![ProviderEndpoint {
            name: "breadmill".to_string(),
            kind: ProviderKind::Breadsearch,
            url: format!("breadsearch:{socket_path}"),
            scope: Some(SCOPE.to_string()),
            credential_ref: None,
            allow_automation: true,
            igpu: false,
            command: Vec::new(),
            hook_timeout_ms: None,
            hook_max_output_bytes: None,
        }],
        mode: ManagementMode::Automatic,
        ..Config::default()
    };
    let (registry, notes) = AdapterRegistry::from_config(&config);
    assert!(
        notes
            .iter()
            .all(|note| !note.contains("contributes nothing")),
        "{notes:?}"
    );
    registry
}

fn socket_query(socket_path: &str, body: &str) -> String {
    let mut stream = UnixStream::connect(socket_path).unwrap();
    stream.write_all(body.as_bytes()).unwrap();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    line
}

fn executor<'a>(
    registry: &'a AdapterRegistry,
    workloads: &'a [Workload],
    probes: &'a ScriptedTransport,
    operations: &'a ScriptedTransport,
    clock: &'a TestClock,
    sockets: &'a UnixSocketRunner,
) -> AdapterExecutor<'a> {
    AdapterExecutor::new(registry, workloads, probes, operations, &NoSleep, clock)
        .with_socket_runner(sockets)
}

fn run(
    executor: &AdapterExecutor,
    workloads: &[Workload],
) -> Vec<breadram_core::coordinator::ActionResult> {
    let mut coordinator = Coordinator::new(OwnerToken("test".to_string()), 16);
    coordinator.execute(
        &[request()],
        workloads,
        executor,
        60_000,
        &Cancellation::new(),
    )
}

const IDLE: &str = r#"{"type":"status_info","indexed":41,"pending":0,"model_ready":true,"in_flight":0,"loading":false,"model_bytes":547310275,"version":"0.4.0"}
"#;
const UNLOADED: &str = r#"{"type":"status_info","indexed":41,"pending":0,"model_ready":false,"in_flight":0,"loading":false,"model_bytes":547310275,"version":"0.4.0"}
"#;
const OK: &str = r#"{"type":"ok"}
"#;

#[test]
fn release_confirms_through_a_fresh_status() {
    let mill = FakeMill::start(vec![IDLE, OK, UNLOADED]);
    let registry = registry_for(&mill.socket_path);
    let sockets = UnixSocketRunner;
    assert!(registry.refresh_sockets(&sockets, 0).is_empty());
    let workloads = [workload()];
    let probes = ScriptedTransport::new();
    let operations = ScriptedTransport::new();
    let clock = TestClock(60_000);
    let executor = executor(
        &registry,
        &workloads,
        &probes,
        &operations,
        &clock,
        &sockets,
    );
    let results = run(&executor, &workloads);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].state, ActionState::Confirmed, "{results:?}");
    let seen = mill.requests();
    assert_eq!(seen.len(), 3, "{seen:?}");
    assert!(seen[1].contains("unload_model"), "{seen:?}");
    assert!(seen[2].contains("status"), "{seen:?}");
}

#[test]
fn a_busy_daemon_refuses_and_nothing_is_written() {
    let mill = FakeMill::start(vec![
        IDLE,
        r#"{"type":"error","message":"busy: 2 queries in flight"}
"#,
    ]);
    let registry = registry_for(&mill.socket_path);
    let sockets = UnixSocketRunner;
    registry.refresh_sockets(&sockets, 0);
    let workloads = [workload()];
    let probes = ScriptedTransport::new();
    let operations = ScriptedTransport::new();
    let clock = TestClock(60_000);
    let executor = executor(
        &registry,
        &workloads,
        &probes,
        &operations,
        &clock,
        &sockets,
    );
    let results = run(&executor, &workloads);
    assert_eq!(results[0].state, ActionState::Denied, "{results:?}");
    assert!(results[0].detail.contains("busy"), "{results:?}");
    assert!(
        matches!(executor.confirm(&request()), Confirmation::NotYet { .. }),
        "no fresh unload to confirm"
    );
}

#[test]
fn demand_reload_is_observed_from_absent_to_ready() {
    let mill = FakeMill::start(vec![
        UNLOADED,
        r#"{"type":"error","message":"model loading — retry shortly"}
"#,
        r#"{"type":"status_info","indexed":41,"pending":0,"model_ready":false,"in_flight":0,"loading":true,"model_bytes":547310275,"version":"0.4.0"}
"#,
        r#"{"type":"status_info","indexed":41,"pending":0,"model_ready":true,"in_flight":0,"loading":false,"model_bytes":547310275,"version":"0.4.0"}
"#,
    ]);
    let registry = registry_for(&mill.socket_path);
    let sockets = UnixSocketRunner;
    registry.refresh_sockets(&sockets, 0);
    let workloads = [workload()];
    // Absent: no unload is offered for a model that is already gone.
    assert!(registry
        .operation(ActionKind::UnloadIdleModel, &workloads[0])
        .is_none());
    // The search that finds the model gone starts the reload and is told to
    // retry; the load then runs and the model comes back.
    let answer = socket_query(
        &mill.socket_path,
        "{\"type\":\"query\",\"query\":\"hello\",\"limit\":2}\n",
    );
    assert!(answer.contains("model loading"), "{answer}");
    registry.refresh_sockets(&sockets, 61_000);
    assert!(registry
        .operation(ActionKind::UnloadIdleModel, &workloads[0])
        .is_none());
    registry.refresh_sockets(&sockets, 62_000);
    assert!(registry
        .operation(ActionKind::UnloadIdleModel, &workloads[0])
        .is_some());
}

#[test]
fn a_failed_reload_leaves_the_model_absent_and_unoffered() {
    let mill = FakeMill::start(vec![
        UNLOADED,
        OK,
        r#"{"type":"status_info","indexed":41,"pending":0,"model_ready":false,"in_flight":0,"loading":false,"model_bytes":null,"version":"0.4.0"}
"#,
    ]);
    let registry = registry_for(&mill.socket_path);
    let sockets = UnixSocketRunner;
    registry.refresh_sockets(&sockets, 0);
    let workloads = [workload()];
    assert!(registry
        .operation(ActionKind::UnloadIdleModel, &workloads[0])
        .is_none());
    // The load verb is accepted but the model never comes back: a later
    // refresh still finds it absent, with no size to report and no unload
    // to offer for it.
    let reply = breadram_core::adapter::UnixSocketRunner
        .request(&mill.socket_path, r#"{"type":"load_model"}"#)
        .expect("the load verb is accepted");
    assert!(reply.contains(r#""type":"ok""#), "{reply}");
    registry.refresh_sockets(&sockets, 61_000);
    assert!(registry
        .operation(ActionKind::UnloadIdleModel, &workloads[0])
        .is_none());
    let inventory = registry.inventory();
    assert_eq!(inventory.len(), 1);
    assert_eq!(inventory[0].host_ram_bytes, None);
}

#[test]
fn breadsearch_uninstalled_is_ordinary_and_unblocks_nothing() {
    let missing = std::env::temp_dir().join(format!(
        "breadram-no-mill-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let missing = missing
        .join("breadmill.sock")
        .to_string_lossy()
        .into_owned();
    let registry = registry_for(&missing);
    let sockets = UnixSocketRunner;
    let notes = registry.refresh_sockets(&sockets, 0);
    assert!(
        notes
            .iter()
            .any(|note| note.contains("may not be installed")),
        "{notes:?}"
    );
    let workloads = [workload()];
    let probes = ScriptedTransport::new();
    let operations = ScriptedTransport::new();
    let clock = TestClock(60_000);
    let executor = executor(
        &registry,
        &workloads,
        &probes,
        &operations,
        &clock,
        &sockets,
    );
    // The workload is simply unknown: nothing is planned off it, and the
    // executor declines rather than failing the pass.
    let results = run(&executor, &workloads);
    assert_eq!(results[0].state, ActionState::Unsupported, "{results:?}");
}
