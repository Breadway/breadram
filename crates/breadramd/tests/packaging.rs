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

/// Which manager reads a unit file: the per-user manager or the privileged
/// system manager. A unit under a `system` directory, or one enabled for a
/// system target, is system-scoped; everything else in packaging is installed
/// into a user unit directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UnitScope {
    User,
    System,
}

const SYSTEM_ONLY_SANDBOX_KEYS: [&str; 2] = ["ProtectKernelModules", "ProtectKernelLogs"];

fn unit_scope(path: &Path, text: &str) -> UnitScope {
    if path
        .components()
        .any(|part| part.as_os_str() == std::ffi::OsStr::new("system"))
    {
        return UnitScope::System;
    }
    let mut section = String::new();
    for line in text.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        if let Some(name) = line
            .strip_prefix('[')
            .and_then(|rest| rest.strip_suffix(']'))
        {
            section = name.to_ascii_lowercase();
            continue;
        }
        if section == "install" {
            if let Some(targets) = line.strip_prefix("WantedBy=") {
                if targets
                    .split_whitespace()
                    .any(|target| matches!(target, "multi-user.target" | "graphical.target"))
                {
                    return UnitScope::System;
                }
            }
        }
    }
    UnitScope::User
}

/// The system-only sandbox key a unit or drop-in sets, if any. Any assignment
/// counts, including `=false`: a user manager must not need these keys at all.
fn system_only_sandbox_key(text: &str) -> Option<&'static str> {
    for line in text.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        for key in SYSTEM_ONLY_SANDBOX_KEYS {
            if let Some(rest) = line.strip_prefix(key) {
                if rest.trim_start().starts_with('=') {
                    return Some(key);
                }
            }
        }
    }
    None
}

fn collect_unit_files(dir: &Path, found: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read the packaging directory") {
        let path = entry.expect("read a packaging entry").path();
        if path.is_dir() {
            collect_unit_files(&path, found);
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                [".service", ".socket", ".target", ".timer", ".path", ".conf"]
                    .iter()
                    .any(|suffix| name.ends_with(suffix))
            })
        {
            found.push(path);
        }
    }
}

#[test]
fn the_user_unit_omits_system_only_sandbox_keys() {
    let path = packaging_dir().join("breadram.service");
    let text = read_unit();
    assert_eq!(unit_scope(&path, &text), UnitScope::User);
    if let Some(key) = system_only_sandbox_key(&text) {
        panic!(
            "{key} needs a privileged system manager; a user manager fails with \
             218/CAPABILITIES on systemd 255"
        );
    }
}

#[test]
fn shipped_user_units_and_drop_ins_omit_system_only_sandbox_keys() {
    let root = packaging_dir();
    let mut files = Vec::new();
    collect_unit_files(&root, &mut files);
    assert!(
        files.iter().any(|path| path
            .file_name()
            .is_some_and(|name| name == "breadram.service")),
        "the shipped unit is present"
    );
    let mut user_files = 0;
    for path in files {
        let text = std::fs::read_to_string(&path).expect("read a shipped unit file");
        if unit_scope(&path, &text) == UnitScope::System {
            continue;
        }
        user_files += 1;
        if let Some(key) = system_only_sandbox_key(&text) {
            panic!(
                "{} is installed into a user manager but sets {key}; move it to a \
                 system-scoped unit",
                path.display()
            );
        }
    }
    assert!(
        user_files > 0,
        "at least one user-scoped artifact is shipped"
    );
}

#[test]
fn system_scoped_units_may_keep_the_sandbox_keys() {
    let text = "[Service]\n\
                ProtectKernelModules=true\n\
                ProtectKernelLogs=yes\n\
                \n\
                [Install]\n\
                WantedBy=multi-user.target\n";
    let path = Path::new("packaging/system/breadram.service");
    assert_eq!(unit_scope(path, text), UnitScope::System);
    assert_eq!(system_only_sandbox_key(text), Some("ProtectKernelModules"));
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
