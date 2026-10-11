//! Hook conformance: the real runner against real programs.
//!
//! Unit tests pin the protocol parsing with scripted answers. These tests pin
//! the execution safeguards with real processes: timeouts kill, oversized
//! output is refused, unsafe programs never run, the environment is hygienic,
//! and the shipped example speaks the protocol end to end. Every temporary
//! program lives in an owned directory that is removed when its test ends, so
//! the suite leaves nothing behind.

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use breadram_core::adapter::{
    AdapterExecutor, AdapterRegistry, HookRunner, HookVerb, NoSleep, ProcessHookRunner,
    ScriptedTransport,
};
use breadram_core::config::{Config, HookSpec, ProviderEndpoint, ProviderKind};
use breadram_core::coordinator::{ActionRequest, Executor, Precondition};
use breadram_core::inventory::{ControlLevel, MemoryAccounting, Workload, WorkloadId};
use breadram_core::telemetry::fs::Clock;
use breadram_core::ManagementMode;

static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

/// An owned temporary directory, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let unique = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "breadram-hook-{}-{unique}-{label}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create the test directory");
        Self(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct TestClock(u64);

impl Clock for TestClock {
    fn monotonic_ms(&self) -> u64 {
        self.0
    }
}

/// Write an executable shell program into `dir`.
fn write_hook(dir: &TempDir, name: &str, body: &str) -> PathBuf {
    let path = dir.path().join(name);
    let mut file = std::fs::File::create(&path).expect("create the hook program");
    file.write_all(body.as_bytes())
        .expect("write the hook program");
    drop(file);
    let mut permissions = std::fs::metadata(&path)
        .expect("stat the hook program")
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&path, permissions).expect("chmod the hook program");
    path
}

fn spec_for(program: &Path) -> HookSpec {
    HookSpec::with_command(vec![program.display().to_string()])
}

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
fn the_shipped_example_speaks_the_protocol_end_to_end() {
    let dir = TempDir::new("example");
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/hook-example.py");
    let program = dir.path().join("hook-example.py");
    std::fs::copy(&source, &program).expect("copy the example");
    let mut permissions = std::fs::metadata(&program)
        .expect("stat the example")
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&program, permissions).expect("chmod the example");

    let runner = ProcessHookRunner;
    let spec = spec_for(&program);
    let status: serde_json::Value =
        serde_json::from_str(&runner.run(&spec, HookVerb::Status).expect("status answers"))
            .expect("status is JSON");
    assert_eq!(status["busy"], false);
    let release: serde_json::Value = serde_json::from_str(
        &runner
            .run(&spec, HookVerb::Release)
            .expect("release answers"),
    )
    .expect("release is JSON");
    assert_eq!(release["released"], true);
    let wake: serde_json::Value =
        serde_json::from_str(&runner.run(&spec, HookVerb::Wake).expect("wake answers"))
            .expect("wake is JSON");
    assert_eq!(wake["awake"], true);
}

#[test]
fn a_busy_example_declines_its_release() {
    let dir = TempDir::new("busy");
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/hook-example.py");
    let program = dir.path().join("hook-example.py");
    std::fs::copy(&source, &program).expect("copy the example");
    let mut permissions = std::fs::metadata(&program)
        .expect("stat the example")
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&program, permissions).expect("chmod the example");
    std::fs::write(dir.path().join(".hook-example.lock"), "training\n").expect("hold the lock");

    let runner = ProcessHookRunner;
    let spec = spec_for(&program);
    let status: serde_json::Value =
        serde_json::from_str(&runner.run(&spec, HookVerb::Status).expect("status answers"))
            .expect("status is JSON");
    assert_eq!(status["busy"], true);
    let release: serde_json::Value = serde_json::from_str(
        &runner
            .run(&spec, HookVerb::Release)
            .expect("release answers"),
    )
    .expect("release is JSON");
    assert_eq!(release["released"], false);
}

#[test]
fn a_hook_that_hangs_is_killed_at_its_deadline() {
    let dir = TempDir::new("timeout");
    let program = write_hook(&dir, "hang.sh", "#!/bin/sh\nsleep 30\n");
    let mut spec = spec_for(&program);
    spec.timeout_ms = 200;
    let started = std::time::Instant::now();
    let error = ProcessHookRunner
        .run(&spec, HookVerb::Status)
        .expect_err("the hang outlives its deadline");
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    assert_eq!(
        error.to_string(),
        "the hook did not answer within 200 ms and was killed"
    );
}

#[test]
fn output_beyond_the_cap_is_refused() {
    let dir = TempDir::new("output");
    let program = write_hook(&dir, "loud.sh", "#!/bin/sh\nhead -c 100000 /dev/zero\n");
    let mut spec = spec_for(&program);
    spec.max_output_bytes = 4096;
    let error = ProcessHookRunner
        .run(&spec, HookVerb::Status)
        .expect_err("100 KiB exceeds a 4 KiB cap");
    assert_eq!(error.to_string(), "the hook printed more than 4096 bytes");
}

#[test]
fn a_hook_carries_no_ambient_secrets() {
    let dir = TempDir::new("env");
    let program = write_hook(&dir, "env.sh", "#!/bin/sh\nenv\n");
    let spec = spec_for(&program);
    std::env::set_var("BREADRAM_CONFORMANCE_SECRET", "s3cret-marker");
    let body = ProcessHookRunner
        .run(&spec, HookVerb::Status)
        .expect("env answers");
    std::env::remove_var("BREADRAM_CONFORMANCE_SECRET");
    assert!(body.contains("BREADRAM_HOOK=1"), "{body}");
    assert!(!body.contains("s3cret-marker"), "{body}");
}

#[test]
fn a_nonzero_hook_reports_its_stderr() {
    let dir = TempDir::new("failing");
    let program = write_hook(
        &dir,
        "fail.sh",
        "#!/bin/sh\necho 'checkpoint in progress' >&2\nexit 3\n",
    );
    let spec = spec_for(&program);
    let error = ProcessHookRunner
        .run(&spec, HookVerb::Status)
        .expect_err("exit 3 fails");
    assert_eq!(
        error.to_string(),
        "the hook exited with status 3: checkpoint in progress"
    );
}

#[test]
fn an_unreadable_answer_is_reported_not_guessed() {
    let dir = TempDir::new("garbage");
    let program = write_hook(&dir, "garbage.sh", "#!/bin/sh\necho 'not json'\n");
    let spec = spec_for(&program);
    let body = ProcessHookRunner
        .run(&spec, HookVerb::Status)
        .expect("the program answers");
    assert!(breadram_core::adapter::parse_status(&body).is_err());
}

#[test]
fn a_world_writable_program_never_runs() {
    let dir = TempDir::new("perms");
    let program = write_hook(&dir, "open.sh", "#!/bin/sh\necho '{\"busy\": false}'\n");
    let mut permissions = std::fs::metadata(&program)
        .expect("stat the hook")
        .permissions();
    permissions.set_mode(0o777);
    std::fs::set_permissions(&program, permissions).expect("chmod the hook");
    let spec = spec_for(&program);
    let error = ProcessHookRunner
        .run(&spec, HookVerb::Status)
        .expect_err("world-writable is refused");
    assert!(
        error.to_string().contains("writable by group or others"),
        "{error}"
    );
}

#[test]
fn a_missing_program_is_refused() {
    let dir = TempDir::new("missing");
    let spec = spec_for(&dir.path().join("no-such-hook"));
    let error = ProcessHookRunner
        .run(&spec, HookVerb::Status)
        .expect_err("nothing to run");
    assert!(error.to_string().contains("cannot stat"), "{error}");
}

#[test]
fn a_configured_hook_flows_from_config_to_release() {
    let dir = TempDir::new("registry");
    let program = write_hook(
        &dir,
        "steady.sh",
        "#!/bin/sh\nif [ \"$1\" = status ]; then echo '{\"busy\": false, \"host_ram_bytes\": 1048576}'; elif [ \"$1\" = release ]; then echo '{\"released\": true}'; else echo '{\"awake\": true}'; fi\n",
    );
    let scope = "unit:steady.service";
    let config = Config {
        providers: vec![ProviderEndpoint {
            name: "steady".to_string(),
            kind: ProviderKind::Hook,
            url: "hook:steady".to_string(),
            scope: Some(scope.to_string()),
            credential_ref: None,
            allow_automation: true,
            command: vec![program.display().to_string()],
            hook_timeout_ms: None,
            hook_max_output_bytes: None,
        }],
        mode: ManagementMode::Automatic,
        ..Config::default()
    };
    let (registry, notes) = AdapterRegistry::from_config(&config);
    assert_eq!(registry.names(), vec!["steady"]);
    assert!(
        notes
            .iter()
            .all(|note| !note.contains("contributes nothing")),
        "{notes:?}"
    );

    let runner = ProcessHookRunner;
    assert!(registry.refresh_hooks(&runner, 0).is_empty());
    let workloads = [workload(scope)];
    let probes = ScriptedTransport::new();
    let operations = ScriptedTransport::new();
    let clock = TestClock(60_000);
    let executor = AdapterExecutor::new(
        &registry,
        &workloads,
        &probes,
        &operations,
        &NoSleep,
        &clock,
    )
    .with_hook_runner(&runner);
    let request = ActionRequest::new(
        workloads[0].id.clone(),
        breadram_core::config::ActionKind::UnloadIdleModel,
        "testing",
        60_000,
    );
    assert_eq!(executor.precondition(&request), Precondition::Ready);
    let change = executor.apply(&request).expect("the hook releases");
    assert!(change.note.expect("a note").contains("through its hook"));
    match executor.confirm(&request) {
        breadram_core::coordinator::Confirmation::Confirmed => {}
        other => panic!("expected a confirmation, got {other:?}"),
    }
    assert_eq!(operations.operation_count(), 0, "no HTTP request is sent");
}
