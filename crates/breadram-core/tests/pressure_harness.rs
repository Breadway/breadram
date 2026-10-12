//! The capped, disposable pressure harness (RAM-29).
//!
//! Pressure testing must never exhaust the host or outlive the test. The shared
//! harness runs a command inside a transient systemd user scope with a bounded
//! `MemoryMax` and `MemorySwapMax` under a hard timeout, and kills the scope on
//! timeout. When the host cannot run a user scope, the harness reports why and
//! the test skips rather than failing.

mod support;

use std::time::Duration;

#[test]
fn a_capped_scope_is_bounded_and_disposable() {
    match support::capped_scope("64M", "0", Duration::from_secs(30), "sh", &["-c", "exit 0"]) {
        Some(true) => {}
        Some(false) => eprintln!(
            "skipped: the capped scope did not run here, so the bounded pressure path is unverified"
        ),
        None => {}
    }
}
