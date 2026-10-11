//! The command line contract, tested against a fake daemon.
//!
//! These tests do not repeat the policy or the protocol. They pin what a user
//! and a script depend on: which exit code means what, that a question is asked
//! over the socket rather than guessed locally, that an unknown figure stays
//! unknown instead of becoming zero, and that the JSON form is the daemon's own
//! payload rather than a second shape that could drift from it.
//!
//! The daemon here is deliberately a stub. Nothing in this file imports the real
//! daemon, so the CLI is tested through the interface it actually uses.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};

use breadram_core::config::ActionKind;
use breadram_core::ipc::{
    ActionJournalRecord, ActionsPayload, AppliedPayload, ClientMessage, DiagnosticsPayload,
    ErrorCode, JournalSummary, MemoryBreakdown, OwnedValueRecord, PlanPayload,
    PlannedActionPayload, PressureSummary, RequestBody, ResponseBody, RulesPayload, ServerMessage,
    StatusPayload, WorkloadSummary, WorkloadsPayload, PROTOCOL_MAJOR, PROTOCOL_MINOR,
};
use breadram_core::telemetry::{
    CgroupSwap, OomManagement, SwapArea, SwapKind, SwapLayout, SwapLimit, WorkloadSwap, ZramDevice,
    ZswapState,
};
use breadram_core::{CapabilityReport, ManagementMode};

/// The exit codes the CLI promises.
const OK: i32 = 0;
const USAGE: i32 = 2;
const UNAVAILABLE: i32 = 3;
const REFUSED: i32 = 4;

static NEXT_SOCKET: AtomicUsize = AtomicUsize::new(0);

/// A socket path in a directory that removes itself when the test ends.
///
/// The tests drive the real CLI, so a real socket file is needed. Leaving the
/// directory behind on every run filled the temporary directory with one entry
/// per test per run, and a stale socket from an earlier run is not merely untidy:
/// it can be mistaken for a live daemon.
struct TestSocket {
    dir: PathBuf,
    path: PathBuf,
}

impl TestSocket {
    fn new(name: &str) -> Self {
        let unique = NEXT_SOCKET.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "breadram-cli-{}-{unique}-{name}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create the test directory");
        Self {
            path: dir.join("breadram.sock"),
            dir,
        }
    }
}

impl std::ops::Deref for TestSocket {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestSocket {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn temp_socket(name: &str) -> TestSocket {
    TestSocket::new(name)
}

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_breadram"))
        .args(args)
        .output()
        .expect("run the CLI")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

fn exit_code(output: &Output) -> i32 {
    output
        .status
        .code()
        .expect("the CLI must exit, not be killed")
}

/// Start a stub daemon that answers every request with a fixed payload.
fn start_daemon(socket: &Path) {
    let listener = UnixListener::bind(socket).expect("bind the test socket");
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { return };
            let _ = serve(stream);
        }
    });
}

fn serve(stream: UnixStream) -> std::io::Result<()> {
    let reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;
    for line in reader.lines() {
        let line = line?;
        let Ok(message) = breadram_core::ipc::decode::<ClientMessage>(&line) else {
            return Ok(());
        };
        let body = match message.body {
            RequestBody::Hello(_) => ResponseBody::Hello {
                major: PROTOCOL_MAJOR,
                minor: PROTOCOL_MINOR,
                server: "test-1.0".to_string(),
            },
            RequestBody::Status => ResponseBody::Status(Box::new(status())),
            RequestBody::Capabilities => ResponseBody::Capabilities(capabilities()),
            RequestBody::Workloads => ResponseBody::Workloads(Box::new(workloads())),
            RequestBody::Explain => ResponseBody::Plan(Box::new(plan())),
            RequestBody::Rules => ResponseBody::Rules(Box::new(rules())),
            RequestBody::Actions => ResponseBody::Actions(Box::new(actions())),
            RequestBody::Diagnostics => ResponseBody::Diagnostics(Box::new(diagnostics())),
            RequestBody::SetMode { mode, persist } => {
                ResponseBody::Applied(Box::new(AppliedPayload {
                    summary: format!("management mode set to {mode} (persist {persist})"),
                    persisted: persist,
                    rules: rules(),
                }))
            }
            RequestBody::SetProtection { add, remove, .. } => {
                let mut next = rules();
                // A scope that is not protected cannot be unprotected, which is
                // how the real daemon refuses a change that would do nothing.
                let known = remove
                    .iter()
                    .all(|scope| next.protected.iter().any(|current| current == scope));
                if !known {
                    ResponseBody::Error(breadram_core::ipc::ApiError::new(
                        ErrorCode::BadRequest,
                        "nothing to change: every scope was already in that state",
                    ))
                } else {
                    next.protected.extend(add);
                    next.protected.retain(|scope| !remove.contains(scope));
                    ResponseBody::Applied(Box::new(AppliedPayload {
                        summary: format!("protection updated: {} scope(s)", next.protected.len()),
                        persisted: false,
                        rules: next,
                    }))
                }
            }
            RequestBody::Subscribe => ResponseBody::Subscribed { interval_ms: 500 },
        };
        let response = ServerMessage {
            id: message.id,
            body,
        };
        let text = breadram_core::ipc::encode(&response).expect("encode the response");
        writeln!(writer, "{text}")?;
        writer.flush()?;
    }
    Ok(())
}

fn status() -> StatusPayload {
    StatusPayload {
        mode: ManagementMode::Monitor,
        version: "test-1.0".to_string(),
        observed_at_ms: 4_200,
        available_bytes: Some(6 * 1024 * 1024 * 1024),
        total_bytes: Some(32 * 1024 * 1024 * 1024),
        workloads: 2,
        capabilities: capabilities(),
    }
}

fn capabilities() -> CapabilityReport {
    let mut report = CapabilityReport::new();
    report.set("psi", breadram_core::CapabilityState::Available);
    report.set(
        "cgroup_v2",
        breadram_core::CapabilityState::Unavailable {
            reason: "the memory controller is not available on the root cgroup".to_string(),
        },
    );
    report
}

fn workload(scope: &str, name: &str, cgroup: Option<u64>, rss: Option<u64>) -> WorkloadSummary {
    WorkloadSummary {
        scope: scope.to_string(),
        generation: 7,
        name: name.to_string(),
        processes: 3,
        cgroup_bytes: cgroup,
        rss_bytes: rss,
        estimated: cgroup.is_none(),
        control: if cgroup.is_some() {
            "eligible".to_string()
        } else {
            "observe_only".to_string()
        },
    }
}

fn workloads() -> WorkloadsPayload {
    WorkloadsPayload {
        observed_at_ms: 4_200,
        workloads: vec![
            workload("unit:ollama.service", "ollama", Some(9_000_000_000), None),
            workload("exe:python3", "python3", None, Some(1_500_000_000)),
        ],
        capabilities: capabilities(),
    }
}

fn plan() -> PlanPayload {
    PlanPayload {
        state: "pressured".to_string(),
        channel: "dry_run".to_string(),
        would_execute: false,
        actions: vec![PlannedActionPayload {
            target: "unit:ollama.service".to_string(),
            generation: 7,
            kind: ActionKind::CgroupSoftLimit,
            reason: "the unit holds more than its budget".to_string(),
            expected_host_ram_delta: -2 * 1024 * 1024 * 1024,
            requires_confirmed_idle: false,
        }],
        explanation: vec!["unit:ollama.service: soft limit would be lowered".to_string()],
        observed_at_ms: 4_200,
    }
}

fn rules() -> RulesPayload {
    RulesPayload {
        mode: ManagementMode::Automatic,
        config_path: Some("/etc/breadram/config.toml".to_string()),
        allowed_actions: vec![ActionKind::CgroupSoftLimit],
        protected: vec!["unit:sshd.service".to_string()],
        budgets: Vec::new(),
        rules: Vec::new(),
    }
}

fn actions() -> ActionsPayload {
    ActionsPayload {
        plan: plan(),
        history: vec![ActionJournalRecord {
            id: 99,
            scope: "unit:ollama.service".to_string(),
            kind: Some(ActionKind::CgroupSoftLimit),
            state: "confirmed".to_string(),
            at_ms: 4_000,
            reason: "the unit held more than its budget".to_string(),
            requested_host_ram_delta: Some(-2 * 1024 * 1024 * 1024),
            observed_host_ram_delta: Some(-1_536 * 1024 * 1024),
            requested_device_ram_delta: Some(0),
            observed_device_ram_delta: None,
            owned_values: vec![OwnedValueRecord {
                key: "cgroup:unit:ollama.service/memory.high".to_string(),
                original: "max".to_string(),
                last_applied: "8589934592".to_string(),
            }],
        }],
        history_capacity: 5_000,
        history_note: None,
    }
}

fn diagnostics() -> DiagnosticsPayload {
    DiagnosticsPayload {
        trends: Vec::new(),
        mode: ManagementMode::Monitor,
        version: "test-1.0".to_string(),
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: PROTOCOL_MINOR,
        socket: "/run/user/1000/breadram.sock".to_string(),
        uptime_ms: 1_200_000,
        observed_at_ms: 4_200,
        config_path: None,
        memory: MemoryBreakdown {
            ram_total_bytes: Some(32 * 1024 * 1024 * 1024),
            ram_available_bytes: None,
            swap_total_bytes: Some(0),
            swap_used_bytes: None,
            shared_bytes: None,
            vram_bytes: None,
            swap_pages_in: None,
            swap_pages_out: None,
            notes: vec![
                "MemAvailable is missing on this kernel, so available RAM is unknown".to_string(),
                "device memory is not measured: no adapter or provider reports VRAM yet"
                    .to_string(),
            ],
        },
        pressure: PressureSummary {
            state: "pressured".to_string(),
            some_avg10: Some(12.5),
            full_avg10: None,
            available_fraction: Some(0.18),
        },
        jobs_enabled: false,
        workloads: 2,
        control_targets: 1,
        capabilities: capabilities(),
        journal: JournalSummary {
            entries: 0,
            capacity: 5_000,
            executing: false,
        },
        swap: SwapLayout {
            areas: vec![SwapArea {
                path: "/dev/zram0".to_string(),
                kind: SwapKind::Partition,
                size_bytes: 4 * 1024 * 1024 * 1024,
                used_bytes: 1024 * 1024 * 1024,
                priority: Some(100),
            }],
            total_bytes: 4 * 1024 * 1024 * 1024,
            used_bytes: 1024 * 1024 * 1024,
            zswap: Some(ZswapState {
                enabled: false,
                max_pool_percent: Some(20),
                compressor: Some("lz4".to_string()),
                zpool: None,
            }),
            zram: vec![ZramDevice {
                name: "zram0".to_string(),
                disksize_bytes: Some(4 * 1024 * 1024 * 1024),
                mem_used_bytes: Some(512 * 1024 * 1024),
                algorithm: Some("lz4".to_string()),
            }],
            oom: OomManagement {
                systemd_oomd_installed: true,
                kernel_oom_kills: Some(2),
            },
            swap_in_pages: Some(12),
            swap_out_pages: None,
            swap_table_readable: true,
        },
        workload_swap: vec![WorkloadSwap {
            scope: "unit:app.service".to_string(),
            cgroup: "/user.slice/app.service".to_string(),
            swap: CgroupSwap {
                current_bytes: Some(1024),
                limit: SwapLimit::Unlimited,
                high_limit: SwapLimit::Unknown,
                oom_kills: None,
            },
        }],
        notes: Vec::new(),
    }
}

#[test]
fn a_missing_daemon_is_an_unavailable_exit_not_a_crash() {
    let socket = temp_socket("missing");
    let socket_arg = socket.display().to_string();
    let output = run(&["--socket", &socket_arg, "status"]);
    assert_eq!(exit_code(&output), UNAVAILABLE);
    assert!(
        stderr(&output).contains("no breadram daemon is listening"),
        "stderr should name the problem: {}",
        stderr(&output)
    );
    assert!(
        stderr(&output).contains(&socket_arg),
        "stderr should name the socket it tried: {}",
        stderr(&output)
    );
}

#[test]
fn an_invalid_mode_is_a_usage_error_even_without_a_daemon() {
    let socket = temp_socket("mode");
    let socket_arg = socket.display().to_string();
    let output = run(&["--socket", &socket_arg, "mode", "aggressive"]);
    assert_eq!(exit_code(&output), USAGE);
    assert!(
        stderr(&output).contains("unknown management mode"),
        "stderr should explain the accepted values: {}",
        stderr(&output)
    );
}

#[test]
fn help_lists_the_core_commands() {
    let output = run(&["--help"]);
    assert_eq!(exit_code(&output), OK);
    let text = stdout(&output);
    for command in [
        "status",
        "workloads",
        "capabilities",
        "explain",
        "rules",
        "actions",
        "diagnostics",
        "mode",
        "protect",
    ] {
        assert!(text.contains(command), "{command} is missing from --help");
    }
    assert!(
        text.contains("Exit codes"),
        "the exit codes must be documented in the help: {text}"
    );
}

#[test]
fn status_is_read_from_the_daemon_in_both_forms() {
    let socket = temp_socket("status");
    start_daemon(&socket);
    let socket_arg = socket.display().to_string();

    let human = run(&["--socket", &socket_arg, "status"]);
    assert_eq!(exit_code(&human), OK);
    let text = stdout(&human);
    assert!(text.contains("mode:       monitor"), "{text}");
    assert!(text.contains("6.0 GiB"), "{text}");
    assert!(
        text.contains("unavailable: the memory controller"),
        "{text}"
    );

    let json = run(&["--socket", &socket_arg, "--json", "status"]);
    assert_eq!(exit_code(&json), OK);
    let value: serde_json::Value = serde_json::from_str(&stdout(&json)).expect("valid JSON");
    assert_eq!(value["mode"], "monitor");
    assert_eq!(value["observed_at_ms"], 4_200);
    assert_eq!(value["total_bytes"], 32u64 * 1024 * 1024 * 1024);
}

#[test]
fn workloads_distinguish_accounting_from_an_estimate() {
    let socket = temp_socket("workloads");
    start_daemon(&socket);
    let socket_arg = socket.display().to_string();

    let output = run(&["--socket", &socket_arg, "workloads"]);
    assert_eq!(exit_code(&output), OK);
    let text = stdout(&output);
    assert!(text.contains("unit:ollama.service"), "{text}");
    assert!(
        text.contains("(cgroup)"),
        "exact accounting must say so: {text}"
    );
    assert!(
        text.contains("RSS, over-counts shared pages"),
        "an estimate must never look exact: {text}"
    );
}

#[test]
fn explain_shows_the_state_the_channel_and_the_reason() {
    let socket = temp_socket("explain");
    start_daemon(&socket);
    let socket_arg = socket.display().to_string();

    let output = run(&["--socket", &socket_arg, "explain"]);
    assert_eq!(exit_code(&output), OK);
    let text = stdout(&output);
    assert!(text.contains("pressure:   pressured"), "{text}");
    assert!(text.contains("channel:    dry_run"), "{text}");
    assert!(text.contains("would run:  no"), "{text}");
    assert!(
        text.contains("the unit holds more than its budget"),
        "{text}"
    );
    assert!(text.contains("release 2.0 GiB"), "{text}");
    assert!(text.contains("explanation:"), "{text}");
}

#[test]
fn actions_report_reason_target_result_and_observed_effect_separately() {
    let socket = temp_socket("actions");
    start_daemon(&socket);
    let socket_arg = socket.display().to_string();

    let output = run(&["--socket", &socket_arg, "actions"]);
    assert_eq!(exit_code(&output), OK);
    let text = stdout(&output);
    assert!(text.contains("history:    1 of 5000 entries"), "{text}");
    assert!(
        text.contains("cgroup_soft_limit on unit:ollama.service"),
        "{text}"
    );
    assert!(text.contains("confirmed"), "{text}");
    assert!(
        text.contains("requested release 2.0 GiB, observed release 1.5 GiB"),
        "the requested effect and the observed effect must be separate: {text}"
    );
    assert!(
        text.contains("device memory: requested no change, observed not measured"),
        "{text}"
    );
    assert!(
        text.contains("cgroup:unit:ollama.service/memory.high: 8589934592 (was max)"),
        "the owned value and its original must both be shown: {text}"
    );
}

#[test]
fn diagnostics_keep_unknown_figures_unknown() {
    let socket = temp_socket("diagnostics");
    start_daemon(&socket);
    let socket_arg = socket.display().to_string();

    let output = run(&["--socket", &socket_arg, "diagnostics"]);
    assert_eq!(exit_code(&output), OK);
    let text = stdout(&output);
    assert!(
        text.contains("system RAM: total 32.0 GiB, available unknown"),
        "{text}"
    );
    assert!(
        text.contains("swap:       total 0 B, used unknown"),
        "{text}"
    );
    assert!(text.contains("VRAM:       unknown"), "{text}");
    assert!(text.contains("full avg10 unknown"), "{text}");
    assert!(
        text.contains("no adapter or provider reports VRAM yet"),
        "an unknown figure must carry its reason: {text}"
    );
    assert!(text.contains("executing: false"), "{text}");

    // The swap layout is reported, including the part that makes reclaim cost
    // CPU instead of I/O, and the OOM manager breadram coexists with.
    assert!(
        text.contains("swap areas: 1 (total 4.0 GiB, used 1.0 GiB)"),
        "{text}"
    );
    assert!(
        text.contains("compression: compressed swap in use"),
        "{text}"
    );
    assert!(
        text.contains("/dev/zram0 (partition, 4.0 GiB, used 1.0 GiB, priority 100)"),
        "{text}"
    );
    assert!(
        text.contains("zram0 (compressed device, size 4.0 GiB, in use 512.0 MiB)"),
        "{text}"
    );
    assert!(
        text.contains("zswap (disabled), pool up to 20% of RAM, compressor lz4"),
        "{text}"
    );
    assert!(
        text.contains("a systemd OOM manager is installed and 2 OOM kill(s) since boot"),
        "{text}"
    );
    assert!(
        text.contains("unit:app.service (in use 1.0 KiB, may swap without a limit"),
        "the effective limit and the workload it applies to are both shown: {text}"
    );
}

#[test]
fn rules_show_the_mode_the_allowed_actions_and_the_protection() {
    let socket = temp_socket("rules");
    start_daemon(&socket);
    let socket_arg = socket.display().to_string();

    let output = run(&["--socket", &socket_arg, "rules"]);
    assert_eq!(exit_code(&output), OK);
    let text = stdout(&output);
    assert!(text.contains("mode:       automatic"), "{text}");
    assert!(
        text.contains("config:     /etc/breadram/config.toml"),
        "{text}"
    );
    assert!(text.contains("allowed:    cgroup_soft_limit"), "{text}");
    assert!(text.contains("protected:  unit:sshd.service"), "{text}");
}

#[test]
fn a_mode_change_reports_whether_it_was_written_to_disk() {
    let socket = temp_socket("set-mode");
    start_daemon(&socket);
    let socket_arg = socket.display().to_string();

    let human = run(&["--socket", &socket_arg, "mode", "automatic"]);
    assert_eq!(exit_code(&human), OK);
    let text = stdout(&human);
    assert!(text.contains("management mode set to automatic"), "{text}");
    assert!(
        text.contains("this change lasts until the daemon restarts"),
        "an unpersisted change must not look permanent: {text}"
    );

    let json = run(&[
        "--socket",
        &socket_arg,
        "--json",
        "mode",
        "automatic",
        "--persist",
    ]);
    assert_eq!(exit_code(&json), OK);
    let value: serde_json::Value = serde_json::from_str(&stdout(&json)).expect("valid JSON");
    assert_eq!(value["persisted"], true);
    assert_eq!(value["rules"]["mode"], "automatic");
}

#[test]
fn a_change_that_would_do_nothing_is_refused_and_not_reported_as_success() {
    let socket = temp_socket("refused");
    start_daemon(&socket);
    let socket_arg = socket.display().to_string();

    let output = run(&[
        "--socket",
        &socket_arg,
        "unprotect",
        "unit:not-protected.service",
    ]);
    assert_eq!(exit_code(&output), REFUSED);
    assert!(
        stderr(&output).contains("nothing to change"),
        "the daemon's reason must reach the user: {}",
        stderr(&output)
    );
    assert!(
        stdout(&output).is_empty(),
        "a refused change must print no result: {}",
        stdout(&output)
    );
}
