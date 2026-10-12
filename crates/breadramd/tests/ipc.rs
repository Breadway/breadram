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
