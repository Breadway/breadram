#![cfg(target_os = "linux")]
//! Exercises telemetry collection against the live host.
//!
//! These read the real `procfs` and cgroup files, not fixtures, so they prove the
//! parsers match the kernel on this machine. A missing source degrades a
//! capability rather than failing, which is the behaviour the daemon relies on.

use breadram_core::telemetry::{collect_host, RealFs, SystemClock, CAP_MEMINFO};

#[test]
fn collects_from_the_live_host() {
    let snapshot = collect_host(&RealFs, &SystemClock::new()).expect("host snapshot");

    assert!(
        snapshot.meminfo.total > 0,
        "total memory should be non zero"
    );
    assert!(
        snapshot.meminfo.free <= snapshot.meminfo.total,
        "free memory cannot exceed total"
    );
    assert!(
        snapshot.capabilities.is_usable(CAP_MEMINFO),
        "/proc/meminfo must be usable on the test host"
    );

    // PSI and swap activity are the interesting signals, but a test host may
    // legitimately lack either, so assert only on values that are present.
    if let Some(psi) = snapshot.psi {
        assert!(psi.some.avg300 >= 0.0);
        if let Some(full) = psi.full {
            assert!(full.avg300 >= 0.0);
        }
    }
    if let Some(swap) = snapshot.meminfo.swap_used() {
        assert!(swap <= snapshot.meminfo.swap_total);
    }
}
