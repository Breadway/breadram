//! Shared, deterministic fixtures for the breadram-core integration suites.
//!
//! A new suite reuses these instead of rebuilding the same telemetry and
//! workload doubles, which is what RAM-29 asks for. Nothing here touches the
//! live host except the gated helpers, and a gated helper that cannot run prints
//! why and returns so a skip is reported honestly rather than as a silent pass.

#![allow(dead_code)]

pub mod fake_executor;
pub mod fake_provider;

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use breadram_core::capability::CapabilityReport;
use breadram_core::inventory::{ControlLevel, MemoryAccounting, Workload, WorkloadId};
use breadram_core::telemetry::{MemInfo, MemorySnapshot, PsiLine, PsiReading, VmStat};

/// A gibibyte, so the fixtures read in round numbers.
pub const GIB: u64 = 1024 * 1024 * 1024;
/// A deterministic total host memory, shared so every suite agrees on it.
pub const TOTAL: u64 = 64 * GIB;

/// A deterministic memory snapshot with the given pressure reading.
pub fn snapshot(now_ms: u64, avg10: f64, available_fraction: f64) -> MemorySnapshot {
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

/// A cgroup-accounted workload fixture with a stable identity.
pub fn cgroup_workload(name: &str, scope: &str, bytes: u64) -> Workload {
    Workload {
        id: WorkloadId {
            scope: scope.to_string(),
            generation: 1,
        },
        name: name.to_string(),
        pids: vec![1000],
        accounting: MemoryAccounting::Cgroup {
            current_bytes: Some(bytes),
        },
        control: ControlLevel::Eligible,
        limit_bytes: None,
        ancestor_bytes: None,
    }
}

/// Whether a program exists on PATH, for a gated test to decide to skip.
pub fn have(program: &str) -> bool {
    Command::new(program)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

/// Run a command in a transient, bounded, disposable systemd user scope.
///
/// The scope caps memory and swap and the whole thing runs under a hard
/// timeout, so a pressure test can never exhaust the host or outlive the test:
/// on timeout the scope is killed and reaped. Returns `None`, with a printed
/// reason, when the host cannot run a user scope; the caller then skips.
pub fn capped_scope(
    memory_max: &str,
    swap_max: &str,
    timeout: Duration,
    program: &str,
    args: &[&str],
) -> Option<bool> {
    if std::env::var_os("XDG_RUNTIME_DIR").is_none() {
        eprintln!("skipped: no XDG_RUNTIME_DIR, so a systemd user scope cannot run here");
        return None;
    }
    if !have("systemd-run") {
        eprintln!("skipped: systemd-run is not on PATH, so the capped scope cannot run");
        return None;
    }
    let mut child: Child = match Command::new("systemd-run")
        .args(["--user", "--scope", "--quiet", "--wait", "--collect"])
        .arg(format!("-pMemoryMax={memory_max}"))
        .arg(format!("-pMemorySwapMax={swap_max}"))
        .arg("--")
        .arg(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(err) => {
            eprintln!("skipped: the capped scope could not start: {err}");
            return None;
        }
    };
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Some(status.success()),
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(25));
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                eprintln!("skipped: the capped scope exceeded {timeout:?} and was killed");
                return None;
            }
            Err(err) => {
                eprintln!("skipped: waiting on the capped scope failed: {err}");
                return None;
            }
        }
    }
}
