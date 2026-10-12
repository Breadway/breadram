//! Shared monitor-only daemon ownership for disposable socket tests.
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tokio::net::UnixStream;

pub struct Daemon {
    child: Child,
    socket: PathBuf,
}

impl Daemon {
    pub fn start(socket: &Path, config: Option<&Path>) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_breadramd"));
        command
            .arg("--socket")
            .arg(socket)
            .arg("--mode")
            .arg("monitor")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(config) = config {
            command.arg("--config").arg(config);
        }
        Self {
            child: command.spawn().expect("spawn test daemon"),
            socket: socket.to_path_buf(),
        }
    }

    /// Kill and reap while retaining the stale socket for restart tests.
    pub fn stop(mut self) {
        self.kill_and_wait();
        self.socket = PathBuf::new();
    }

    /// The daemon process id, for signalling it in shutdown tests.
    /// Only the ipc suite uses it; the shared module compiles per target.
    #[allow(dead_code)]
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    fn kill_and_wait(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.kill_and_wait();
        if !self.socket.as_os_str().is_empty() {
            let _ = std::fs::remove_file(&self.socket);
        }
    }
}

pub async fn wait_until_ready(socket: &Path) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if UnixStream::connect(socket).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("daemon readiness deadline at {}", socket.display()));
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn assertion_unwind_reaps_the_child() {
        let socket =
            std::env::temp_dir().join(format!("breadram-unwind-{}.sock", std::process::id()));
        let mut pid = None;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let daemon = Daemon::start(&socket, None);
            pid = Some(daemon.child.id());
            panic!("injected assertion failure");
        }));
        assert!(result.is_err());
        assert!(!Path::new(&format!("/proc/{}", pid.unwrap())).exists());
        assert!(!socket.exists());
        // Explicit termination also reaps; stop preserves a stale socket only
        // when one was actually created, for the restart scenario.
        let daemon = Daemon::start(&socket, None);
        let pid = daemon.child.id();
        daemon.stop();
        assert!(!Path::new(&format!("/proc/{pid}")).exists());
    }
}
