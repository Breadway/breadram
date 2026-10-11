#![cfg(target_os = "linux")]
//! Exercises workload discovery against the live host.
//!
//! This proves discovery works from procfs alone, headlessly, with none of
//! breadsearch, breadd, Ollama, llama.cpp, or vLLM present.

use std::collections::HashSet;

use breadram_core::inventory::{collect_inventory, CAP_PROC_INVENTORY};
use breadram_core::telemetry::RealFs;

#[test]
fn discovers_workloads_on_the_live_host() {
    let inventory = collect_inventory(&RealFs);

    assert!(
        inventory.capabilities.is_usable(CAP_PROC_INVENTORY),
        "procfs inventory must be usable on the test host"
    );
    assert!(
        !inventory.workloads.is_empty(),
        "a running host always has processes"
    );

    for workload in &inventory.workloads {
        assert!(!workload.pids.is_empty(), "a workload has at least one pid");
        assert!(!workload.id.scope.is_empty(), "a workload has a scope key");
    }

    // Every process belongs to exactly one workload, so grouping never double
    // counts a process across workloads.
    let mut seen = HashSet::new();
    for workload in &inventory.workloads {
        for pid in &workload.pids {
            assert!(seen.insert(*pid), "pid {pid} was grouped twice");
        }
    }
}
