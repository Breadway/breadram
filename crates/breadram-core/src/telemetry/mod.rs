//! Memory pressure telemetry.
//!
//! Everything here is pure parsing plus a thin collection layer over an
//! injectable file source and clock. That keeps it fully testable from fixtures
//! and means the daemon never has to shell out or block on a live host to be
//! exercised.
//!
//! Two rules run through the whole module:
//!
//! 1. Missing data is never zero. Fields the kernel may omit are `Option`, and a
//!    probe that cannot read a source records a degraded capability with a
//!    reason.
//! 2. Observation times are monotonic. Snapshot timestamps come from a
//!    [`Clock`], so a wall clock change cannot corrupt a time series.

pub mod cgroup;
pub mod fs;
pub mod meminfo;
pub mod psi;
pub mod snapshot;
pub mod swap;
pub mod vmstat;

pub use cgroup::{collect_cgroup, CgroupMemory, CgroupSnapshot};
pub use fs::{Clock, FileSource, RealFs, SystemClock};
pub use meminfo::{parse_meminfo, MemInfo, MemInfoError};
pub use psi::{parse_psi, PsiError, PsiLine, PsiReading};
pub use snapshot::{
    collect_host, is_gap, MemorySnapshot, TelemetryError, CAP_CGROUP_V2, CAP_MEMINFO, CAP_PSI,
    CAP_SWAP_ACTIVITY, CGROUP_V2_ROOT, MAX_OBSERVATION_GAP_MS, PROC_MEMINFO, PROC_PRESSURE_MEMORY,
    PROC_VMSTAT,
};
pub use swap::{
    collect_cgroup_swap, collect_swap, parse_swaps, CgroupSwap, OomManagement, SwapArea, SwapKind,
    SwapLayout, SwapLimit, WorkloadSwap, ZramDevice, ZswapState,
};
pub use vmstat::{counter_delta, parse_vmstat, VmStat};
