#![cfg(unix)]
//! End to end tests for the daemon protocol.
//!
//! These spawn the real `breadramd` binary on a temporary socket and talk to it
//! with the real client, so start, stop, restart, subscribe, and version
//! rejection are verified through the same interface a user drives.

use std::path::{Path, PathBuf};
use std::time::Duration;

mod support;
use support::{wait_until_ready, Daemon};

use breadram_core::ipc::{
    decode, encode, ApiError, Client, ClientError, ClientMessage, ErrorCode, Hello, RequestBody,
    ResponseBody, ServerMessage, PROTOCOL_MAJOR,
};
use breadram_core::ManagementMode;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

fn temp_socket(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("breadram-it-{tag}-{}.sock", std::process::id()))
}

fn spawn_daemon(socket: &Path) -> Daemon {
    Daemon::start(socket, None)
}

async fn connect(socket: &Path) -> Client {
    let mut client = Client::connect(socket).await.expect("connect");
    client.handshake("it").await.expect("handshake");
    client
}

fn stop(child: Daemon) {
    child.stop();
}

#[tokio::test]
async fn connecting_to_a_missing_daemon_is_actionable() {
    let socket = temp_socket("missing");
    let _ = std::fs::remove_file(&socket);

    let err = Client::connect(&socket).await.expect_err("must fail");
    assert!(matches!(err, ClientError::Unavailable { .. }));
    assert!(
        err.to_string().contains("not running"),
        "message should explain the daemon is absent: {err}"
    );
}

#[tokio::test]
async fn starts_serves_status_and_capabilities() {
    let socket = temp_socket("status");
    let child = spawn_daemon(&socket);
    wait_until_ready(&socket).await;

    let mut client = connect(&socket).await;
    let status = client.status().await.expect("status");
    assert_eq!(status.mode, ManagementMode::Monitor);
    assert!(!status.version.is_empty());
    assert!(status.total_bytes.unwrap_or(0) > 0, "host has memory");
    assert!(status.workloads > 0, "host has processes");

    let capabilities = client.capabilities().await.expect("capabilities");
    assert!(capabilities.is_usable("meminfo"));

    stop(child);
    let _ = std::fs::remove_file(&socket);
}

#[tokio::test]
async fn subscribe_streams_status_events() {
    let socket = temp_socket("subscribe");
    let child = spawn_daemon(&socket);
    wait_until_ready(&socket).await;

    let mut client = connect(&socket).await;
    let interval_ms = client.subscribe().await.expect("subscribe");
    assert!(interval_ms >= 250, "interval should be bounded");

    let event = tokio::time::timeout(Duration::from_secs(8), client.next_event())
        .await
        .expect("an event within the timeout")
        .expect("event");
    assert_eq!(event.kind, "status");

    stop(child);
    let _ = std::fs::remove_file(&socket);
}

#[tokio::test]
async fn restart_replaces_a_stale_socket() {
    let socket = temp_socket("restart");

    let child = spawn_daemon(&socket);
    wait_until_ready(&socket).await;
    connect(&socket).await.status().await.expect("first status");
    // SIGKILL leaves the socket file behind, which the next daemon must replace.
    stop(child);

    let child = spawn_daemon(&socket);
    wait_until_ready(&socket).await;
    connect(&socket)
        .await
        .status()
        .await
        .expect("second status");
    stop(child);
    let _ = std::fs::remove_file(&socket);
}

#[tokio::test]
async fn an_incompatible_version_is_rejected_and_closed() {
    let socket = temp_socket("version");
    let child = spawn_daemon(&socket);
    wait_until_ready(&socket).await;

    let stream = UnixStream::connect(&socket).await.expect("raw connect");
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);

    let message = ClientMessage {
        id: 1,
        body: RequestBody::Hello(Hello {
            major: PROTOCOL_MAJOR + 1,
            minor: 0,
            client: "raw".to_string(),
        }),
    };
    let line = encode(&message).expect("encode");
    write.write_all(line.as_bytes()).await.expect("write");
    write.write_all(b"\n").await.expect("write");
    write.flush().await.expect("flush");

    let mut response = String::new();
    reader.read_line(&mut response).await.expect("read");
    let message: ServerMessage = decode(&response).expect("decode");
    match message.body {
        ResponseBody::Error(ApiError { code, .. }) => {
            assert_eq!(code, ErrorCode::IncompatibleVersion);
        }
        other => panic!("expected a version error, got {other:?}"),
    }

    // The daemon must close the connection after rejecting the version.
    let mut extra = String::new();
    let read = reader.read_line(&mut extra).await.expect("read eof");
    assert_eq!(read, 0, "connection should be closed");

    stop(child);
    let _ = std::fs::remove_file(&socket);
}

#[tokio::test]
async fn jobs_submit_cancel_and_list_round_trip_through_the_daemon() {
    let dir = std::env::temp_dir().join(format!("breadram-jobs-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("test dir");
    let config = dir.join("config.toml");
    std::fs::write(&config, "[jobs]\nenabled = true\n").expect("config");
    let socket = dir.join("breadram.sock");

    let child = Daemon::start(&socket, Some(&config));
    wait_until_ready(&socket).await;
    let mut client = connect(&socket).await;

    let queue = client.jobs().await.expect("jobs");
    assert!(queue.enabled);
    assert!(queue.pending.is_empty());
    assert!(queue.admitted.is_empty());

    let queue = client
        .submit_job(
            "nightly-build".to_string(),
            0,
            vec!["/bin/true".to_string()],
            None,
            Vec::new(),
        )
        .await
        .expect("submit");
    assert_eq!(queue.pending.len(), 1);
    assert_eq!(queue.pending[0].id, "nightly-build");
    assert!(
        queue.pending[0].reservation_bytes > 0,
        "a zero estimate means the configured default"
    );

    let err = client
        .submit_job(
            "nightly-build".to_string(),
            0,
            vec!["/bin/true".to_string()],
            None,
            Vec::new(),
        )
        .await
        .expect_err("a duplicate must be refused");
    assert!(
        matches!(err, ClientError::Api(_)),
        "the refusal must reach the client: {err}"
    );

    // A submit without a program never reaches the queue.
    let err = client
        .submit_job("no-program".to_string(), 0, Vec::new(), None, Vec::new())
        .await
        .expect_err("a program is required");
    assert!(
        matches!(err, ClientError::Api(_)),
        "the refusal must reach the client: {err}"
    );

    let queue = client
        .cancel_job("nightly-build".to_string())
        .await
        .expect("cancel");
    assert!(queue.pending.is_empty());

    let err = client
        .cancel_job("nightly-build".to_string())
        .await
        .expect_err("a missing job must be refused");
    assert!(matches!(err, ClientError::Api(_)));

    stop(child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn jobs_submit_is_unavailable_when_admission_is_disabled() {
    let socket = temp_socket("jobs-disabled");
    let child = spawn_daemon(&socket);
    wait_until_ready(&socket).await;
    let mut client = connect(&socket).await;

    let queue = client.jobs().await.expect("jobs");
    assert!(!queue.enabled);

    let err = client
        .submit_job(
            "nightly-build".to_string(),
            0,
            vec!["/bin/true".to_string()],
            None,
            Vec::new(),
        )
        .await
        .expect_err("a disabled queue must refuse submits");
    assert!(
        matches!(err, ClientError::Api(_)),
        "the refusal must reach the client: {err}"
    );

    stop(child);
    let _ = std::fs::remove_file(&socket);
}

/// Poll the jobs report until `ready` holds or the deadline passes.
async fn wait_for_jobs(
    client: &mut Client,
    deadline: std::time::Instant,
    ready: impl Fn(&breadram_core::ipc::JobsPayload) -> bool,
) -> breadram_core::ipc::JobsPayload {
    loop {
        let queue = client.jobs().await.expect("jobs");
        if ready(&queue) {
            return queue;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the queue never reached the wanted state: {queue:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Launching without a systemd user session, or under real memory pressure, is
/// an honest refusal, not a silent pass: the supervisor has nothing to run the
/// job in, or the host cannot take more work right now.
fn launch_skipped(err: &ClientError) -> bool {
    matches!(err, ClientError::Api(api) if api.message.contains("systemd") || api.message.contains("pressure"))
}

#[tokio::test]
async fn jobs_launch_runs_to_exit_and_reports_the_status() {
    let dir = std::env::temp_dir().join(format!("breadram-launch-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("test dir");
    let config = dir.join("config.toml");
    std::fs::write(&config, "[jobs]\nenabled = true\n").expect("config");
    let socket = dir.join("breadram.sock");

    let child = Daemon::start(&socket, Some(&config));
    wait_until_ready(&socket).await;
    let mut client = connect(&socket).await;

    let id = format!("it-true-{}", std::process::id());
    client
        .submit_job(
            id.clone(),
            64 * 1024 * 1024,
            vec!["/bin/true".to_string()],
            None,
            Vec::new(),
        )
        .await
        .expect("submit");
    match client.launch_job(id.clone()).await {
        Err(err) if launch_skipped(&err) => {
            eprintln!("skipped: supervised launch refused by the environment: {err}");
            stop(child);
            let _ = std::fs::remove_dir_all(&dir);
            return;
        }
        launched => {
            launched.expect("launch");
        }
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let queue = wait_for_jobs(&mut client, deadline, |queue| !queue.finished.is_empty()).await;
    assert_eq!(queue.finished.len(), 1);
    assert_eq!(queue.finished[0].id, id);
    assert_eq!(queue.finished[0].exit_code, Some(0));
    assert!(queue.pending.is_empty());
    assert!(queue.admitted.is_empty());
    assert!(queue.running.is_empty());
    assert_eq!(
        queue.reserved_bytes, 0,
        "the reservation is released on exit"
    );

    stop(child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn jobs_cancel_stops_a_running_job() {
    let dir = std::env::temp_dir().join(format!("breadram-cancel-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("test dir");
    let config = dir.join("config.toml");
    std::fs::write(&config, "[jobs]\nenabled = true\n").expect("config");
    let socket = dir.join("breadram.sock");

    let child = Daemon::start(&socket, Some(&config));
    wait_until_ready(&socket).await;
    let mut client = connect(&socket).await;

    let id = format!("it-sleep-{}", std::process::id());
    client
        .submit_job(
            id.clone(),
            64 * 1024 * 1024,
            vec!["/bin/sleep".to_string(), "60".to_string()],
            None,
            Vec::new(),
        )
        .await
        .expect("submit");
    match client.launch_job(id.clone()).await {
        Err(err) if launch_skipped(&err) => {
            eprintln!("skipped: supervised launch refused by the environment: {err}");
            stop(child);
            let _ = std::fs::remove_dir_all(&dir);
            return;
        }
        launched => {
            launched.expect("launch");
        }
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    wait_for_jobs(&mut client, deadline, |queue| !queue.running.is_empty()).await;

    // Cancellation is asynchronous: the verb reports the queue as the
    // supervisor stops the scope, and completion lands shortly after.
    client.cancel_job(id.clone()).await.expect("cancel");

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let queue = wait_for_jobs(&mut client, deadline, |queue| !queue.finished.is_empty()).await;
    assert_eq!(queue.finished[0].id, id);
    assert!(
        queue.finished[0].note.contains("cancelled"),
        "the record must say it was cancelled: {:?}",
        queue.finished[0]
    );
    assert_eq!(
        queue.reserved_bytes, 0,
        "the reservation is released on cancel"
    );

    stop(child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn jobs_auto_launch_starts_the_head_without_an_explicit_verb() {
    let dir = std::env::temp_dir().join(format!("breadram-auto-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("test dir");
    // Automation allows nothing, so automatic mode cannot touch the host: the
    // only write the loop may perform is the opted-in job launch itself.
    let config = dir.join("config.toml");
    std::fs::write(
        &config,
        "[jobs]\nenabled = true\nauto_launch = true\n[automation]\nallowed_actions = []\n",
    )
    .expect("config");
    let socket = dir.join("breadram.sock");

    let child = Daemon::start(&socket, Some(&config));
    wait_until_ready(&socket).await;
    let mut client = connect(&socket).await;
    client
        .set_mode(ManagementMode::Automatic, false)
        .await
        .expect("automatic mode");

    let id = format!("it-auto-{}", std::process::id());
    client
        .submit_job(
            id.clone(),
            64 * 1024 * 1024,
            vec!["/bin/true".to_string()],
            None,
            Vec::new(),
        )
        .await
        .expect("submit");
    if !can_supervise() {
        eprintln!("skipped: no systemd user session for supervised launch");
        stop(child);
        let _ = std::fs::remove_dir_all(&dir);
        return;
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let launched = wait_for_jobs_result(&mut client, deadline).await;
    assert_eq!(launched.finished.len(), 1);
    assert_eq!(launched.finished[0].id, id);
    assert_eq!(launched.finished[0].exit_code, Some(0));

    stop(child);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Whether this host can supervise a launch: the same gate the daemon
/// enforces, probed from the test side so a session without systemd reports a
/// skip instead of a failure.
fn can_supervise() -> bool {
    if std::env::var_os("XDG_RUNTIME_DIR")
        .map(|dir| dir.is_empty())
        .unwrap_or(true)
    {
        return false;
    }
    std::process::Command::new("systemd-run")
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

/// Poll until a finished job appears.
async fn wait_for_jobs_result(
    client: &mut Client,
    deadline: std::time::Instant,
) -> breadram_core::ipc::JobsPayload {
    loop {
        let queue = client.jobs().await.expect("jobs");
        if !queue.finished.is_empty() {
            return queue;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the loop never launched the head: {queue:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn jobs_launch_runs_two_jobs_at_once_within_budget() {
    let dir = std::env::temp_dir().join(format!("breadram-two-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("test dir");
    let config = dir.join("config.toml");
    std::fs::write(&config, "[jobs]\nenabled = true\n").expect("config");
    let socket = dir.join("breadram.sock");

    let child = Daemon::start(&socket, Some(&config));
    wait_until_ready(&socket).await;
    let mut client = connect(&socket).await;

    // FIFO order: the first submitted launches first.
    let first = format!("it-one-{}", std::process::id());
    let second = format!("it-two-{}", std::process::id());
    let third = format!("it-three-{}", std::process::id());
    for (id, program) in [
        (first.clone(), "/bin/sleep"),
        (second.clone(), "/bin/sleep"),
        (third.clone(), "/bin/true"),
    ] {
        let argv = if program == "/bin/sleep" {
            vec![program.to_string(), "60".to_string()]
        } else {
            vec![program.to_string()]
        };
        client
            .submit_job(id, 64 * 1024 * 1024, argv, None, Vec::new())
            .await
            .expect("submit");
    }
    // Launching past the head is refused: the queue launches in order.
    let err = client
        .launch_job(second.clone())
        .await
        .expect_err("out-of-order launch must be refused");
    assert!(
        matches!(err, ClientError::Api(_)),
        "the refusal must reach the client: {err}"
    );
    match client.launch_job(first.clone()).await {
        Err(err) if launch_skipped(&err) => {
            eprintln!("skipped: supervised launch refused by the environment: {err}");
            stop(child);
            let _ = std::fs::remove_dir_all(&dir);
            return;
        }
        launched => {
            launched.expect("launch one");
        }
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    wait_for_jobs(&mut client, deadline, |queue| !queue.running.is_empty()).await;

    // The first job is running, so the second is now the head and launches
    // too: both run at once inside the same budget.
    client.launch_job(second.clone()).await.expect("launch two");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let queue = wait_for_jobs(&mut client, deadline, |queue| queue.running.len() == 2).await;
    assert_eq!(queue.running.len(), 2);
    assert_eq!(
        queue.reserved_bytes,
        128 * 1024 * 1024,
        "both reservations are held at once"
    );

    // Cancelling covers both paths: running jobs stop through their
    // supervisors, and the still-pending third leaves the queue.
    client.cancel_job(first.clone()).await.expect("cancel one");
    client.cancel_job(second.clone()).await.expect("cancel two");
    client
        .cancel_job(third.clone())
        .await
        .expect("cancel three");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let queue = wait_for_jobs(&mut client, deadline, |queue| queue.finished.len() == 2).await;
    assert_eq!(queue.reserved_bytes, 0, "every reservation is released");
    assert!(queue.pending.is_empty() && queue.admitted.is_empty());

    stop(child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn jobs_launch_uses_the_submitted_directory_and_environment() {
    let dir = std::env::temp_dir().join(format!("breadram-cwdenv-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("test dir");
    let workdir = dir.join("work");
    std::fs::create_dir_all(&workdir).expect("work dir");
    let config = dir.join("config.toml");
    std::fs::write(&config, "[jobs]\nenabled = true\n").expect("config");
    let socket = dir.join("breadram.sock");

    let child = Daemon::start(&socket, Some(&config));
    wait_until_ready(&socket).await;
    let mut client = connect(&socket).await;

    // A relative program argument resolves in the submitted directory: the
    // marker must appear there, proving the job ran with that cwd.
    let touch = format!("it-touch-{}", std::process::id());
    client
        .submit_job(
            touch.clone(),
            64 * 1024 * 1024,
            vec!["/usr/bin/touch".to_string(), "cwd-marker".to_string()],
            Some(workdir.display().to_string()),
            Vec::new(),
        )
        .await
        .expect("submit");
    match client.launch_job(touch.clone()).await {
        Err(err) if launch_skipped(&err) => {
            eprintln!("skipped: supervised launch refused by the environment: {err}");
            stop(child);
            let _ = std::fs::remove_dir_all(&dir);
            return;
        }
        launched => {
            launched.expect("launch");
        }
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    wait_for_jobs(&mut client, deadline, |queue| {
        queue.finished.iter().any(|done| done.id == touch)
    })
    .await;
    assert!(
        workdir.join("cwd-marker").exists(),
        "the job must run in the submitted directory"
    );

    // The environment reaches the job: the shell exits with the submitted code.
    let shell = format!("it-shell-{}", std::process::id());
    client
        .submit_job(
            shell.clone(),
            64 * 1024 * 1024,
            vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                "exit $WANT_CODE".to_string(),
            ],
            None,
            vec!["WANT_CODE=7".to_string()],
        )
        .await
        .expect("submit");
    client.launch_job(shell.clone()).await.expect("launch");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let queue = wait_for_jobs(&mut client, deadline, |queue| {
        queue.finished.iter().any(|done| done.id == shell)
    })
    .await;
    let done = queue
        .finished
        .iter()
        .find(|done| done.id == shell)
        .expect("finished");
    assert_eq!(
        done.exit_code,
        Some(7),
        "the submitted environment must reach the job"
    );

    stop(child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn daemon_shutdown_stops_supervised_jobs() {
    let dir = std::env::temp_dir().join(format!("breadram-shutdown-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("test dir");
    let config = dir.join("config.toml");
    std::fs::write(&config, "[jobs]\nenabled = true\n").expect("config");
    let socket = dir.join("breadram.sock");

    let child = Daemon::start(&socket, Some(&config));
    wait_until_ready(&socket).await;
    let mut client = connect(&socket).await;

    let id = format!("it-shutdown-{}", std::process::id());
    client
        .submit_job(
            id.clone(),
            64 * 1024 * 1024,
            vec!["/bin/sleep".to_string(), "60".to_string()],
            None,
            Vec::new(),
        )
        .await
        .expect("submit");
    match client.launch_job(id.clone()).await {
        Err(err) if launch_skipped(&err) => {
            eprintln!("skipped: supervised launch refused by the environment: {err}");
            stop(child);
            let _ = std::fs::remove_dir_all(&dir);
            return;
        }
        launched => {
            launched.expect("launch");
        }
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    wait_for_jobs(&mut client, deadline, |queue| !queue.running.is_empty()).await;
    drop(client);

    // SIGTERM is the clean path: the daemon stops its jobs before it goes.
    let status = std::process::Command::new("kill")
        .args(["-TERM", &child.pid().to_string()])
        .status()
        .expect("signal the daemon");
    assert!(status.success());
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        if tokio::net::UnixStream::connect(&socket).await.is_err() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the daemon never exited after SIGTERM"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let scope_gone = {
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            let gone = std::process::Command::new("systemctl")
                .args(["--user", "list-units", &format!("breadram-job-{id}.scope")])
                .output()
                .map(|output| {
                    !String::from_utf8_lossy(&output.stdout)
                        .contains(&format!("breadram-job-{id}.scope"))
                })
                .unwrap_or(true);
            if gone {
                break true;
            }
            if std::time::Instant::now() >= deadline {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };
    assert!(scope_gone, "shutdown must stop the supervised scope");
    stop(child);
    let _ = std::fs::remove_dir_all(&dir);
}
