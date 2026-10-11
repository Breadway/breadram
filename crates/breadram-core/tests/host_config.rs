#![cfg(unix)]
//! Exercises configuration loading and credential permissions against the real
//! filesystem, which is where the permission check actually matters.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use breadram_core::config::{check_credential_permissions, Config};

/// A directory that removes itself when the test ends.
///
/// A test that leaves a temporary directory behind on every run is a slow leak
/// that eventually has to be cleaned by hand, so the directory owns its own
/// removal.
struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "breadram-config-test-{}-{label}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        Self(dir)
    }

    fn path(&self) -> PathBuf {
        self.0.clone()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn loads_a_configuration_file() {
    let dir = TempDir::new("loads");
    let path = dir.path().join("breadram.toml");
    fs::write(
        &path,
        "mode = \"automatic\"\n[protection]\nprotected = [\"unit:sshd.service\"]\n",
    )
    .expect("write config");

    let text = fs::read_to_string(&path).expect("read config");
    let config = Config::from_toml(&text).expect("valid config");
    assert_eq!(config.protection.protected.len(), 1);
}

#[test]
fn a_credential_file_must_be_owner_only() {
    let dir = TempDir::new("credentials");
    let path = dir.path().join("credentials.toml");
    fs::write(&path, "ollama = \"token\"\n").expect("write credentials");

    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("chmod 600");
    let mode = fs::metadata(&path).expect("stat").permissions().mode() & 0o777;
    assert!(check_credential_permissions(mode).is_ok());

    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("chmod 644");
    let mode = fs::metadata(&path).expect("stat").permissions().mode() & 0o777;
    assert!(
        check_credential_permissions(mode).is_err(),
        "group or world readable credentials must be rejected"
    );
}
