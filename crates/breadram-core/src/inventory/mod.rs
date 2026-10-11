//! Workload inventory: which processes exist and how they group.
//!
//! The inventory reads procfs and groups processes into workloads without
//! needing a named application adapter. It stays usable on a headless host with
//! no bread apps and no model runtime installed.

pub mod parse;
pub mod workload;

pub use parse::{
    parse_cgroup_path, parse_cmdline, parse_proc_stat, parse_proc_status, systemd_unit,
    ProcParseError, ProcStat, ProcStatus,
};
pub use workload::{
    collect_inventory, ControlLevel, Inventory, MemoryAccounting, Workload, WorkloadId,
    CAP_PROC_INVENTORY, CGROUP_V2_ROOT, PROC_ROOT,
};
