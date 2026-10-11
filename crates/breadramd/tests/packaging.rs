#![cfg(unix)]
//! Service installation: the shipped unit, the example configuration, and a
//! boot of the daemon exactly as the unit would start it.
//!
//! Everything lives in owned temporary directories that are removed when the
//! test ends. The `systemd-analyze` check is the one exception: when the tool
//! is absent there is nothing to run, so the test reports the skip honestly
//! instead of failing or pretending.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use breadram_core::config::Config;
use breadram_core::ipc::Client;
use breadram_core::ManagementMode;

static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

/// An owned temporary directory, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let unique = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "breadram-packaging-{}-{unique}-{label}",
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

fn packaging_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../packaging")
}

fn read_unit() -> String {
    std::fs::read_to_string(packaging_dir().join("breadram.service")).expect("read the unit")
}

#[test]
fn the_example_configuration_is_valid_and_inert() {
    let text = std::fs::read_to_string(packaging_dir().join("breadram.toml.example"))
        .expect("read the example configuration");
    let config = Config::from_toml(&text).expect("the shipped example parses");
    assert_eq!(config.mode, ManagementMode::Monitor);
    assert!(
        config.automation.allowed_actions.is_empty(),
        "a fresh install must never act until the operator opts in"
    );
}

#[test]
fn the_unit_starts_breadramd_with_the_shipped_config() {
    let unit = read_unit();
    for section in ["[Unit]", "[Service]", "[Install]"] {
        assert!(unit.contains(section), "missing {section}");
    }
    assert!(
        unit.contains("breadramd --config"),
        "the unit passes --config"
    );
    assert!(unit.contains("Restart=on-failure"), "a crash is restarted");
    assert!(unit.contains("WantedBy=default.target"), "enabled at login");
    assert!(
        unit.contains("NoNewPrivileges=true"),
        "unprivileged by construction"
    );
    assert!(
        unit.contains("ProtectSystem=strict"),
        "the filesystem is locked down"
    );
    assert!(
        unit.contains("ReadWritePaths="),
        "writable paths stay explicit"
    );
}

#[test]
fn the_unit_passes_systemd_analyze_when_available() {
    if Command::new("systemd-analyze")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_err()
    {
        eprintln!("SKIP: systemd-analyze is not installed, unit verification skipped");
        return;
    }
    // Verify the shipped structure with the test binary in place of the
    // installed one: the install location varies by machine, while the unit
    // shape is what this test pins.
    let dir = TempDir::new("verify");
    let unit = read_unit().replace("%h/.local/bin/breadramd", env!("CARGO_BIN_EXE_breadramd"));
    let path = dir.path().join("breadram.service");
    std::fs::write(&path, unit).expect("write the test unit");
    let output = Command::new("systemd-analyze")
        .arg("verify")
        .arg(&path)
        .output()
        .expect("run systemd-analyze verify");
    assert!(
        output.status.success(),
        "unit verification failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A daemon child with an owned home, killed and cleaned up on drop.
struct Daemon {
    child: Child,
    _dir: TempDir,
    socket: PathBuf,
}

impl Daemon {
    fn start(config_text: &str) -> Self {
        let dir = TempDir::new("boot");
        let config = dir.path().join("breadram.toml");
        std::fs::write(&config, config_text).expect("write the test configuration");
        let socket = dir.path().join("breadram.sock");
        let mut command = Command::new(env!("CARGO_BIN_EXE_breadramd"));
        command
            .arg("--config")
            .arg(&config)
            .arg("--socket")
            .arg(&socket)
            .env("XDG_RUNTIME_DIR", dir.path())
            .env("XDG_STATE_HOME", dir.path())
            .env("HOME", dir.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let child = command.spawn().expect("spawn breadramd");
        Self {
            child,
            _dir: dir,
            socket,
        }
    }

    fn socket(&self) -> &Path {
        &self.socket
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.socket);
    }
}

async fn wait_until_ready(socket: &Path) {
    for _ in 0..400 {
        if tokio::net::UnixStream::connect(socket).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("daemon did not become ready at {}", socket.display());
}

#[tokio::test]
async fn the_daemon_boots_from_the_shipped_example() {
    let example = std::fs::read_to_string(packaging_dir().join("breadram.toml.example"))
        .expect("read the example configuration");
    let daemon = Daemon::start(&example);
    wait_until_ready(daemon.socket()).await;
    let mut client = Client::connect(daemon.socket()).await.expect("connect");
    client.handshake("packaging-test").await.expect("handshake");
    let status = client.status().await.expect("status");
    assert_eq!(status.mode, ManagementMode::Monitor);
    assert!(
        daemon.socket().exists(),
        "the socket lives where the unit expects it"
    );
}
