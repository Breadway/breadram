//! The common host memory snapshot.
//!
//! One snapshot type feeds policy, the CLI, and integrations, so every consumer
//! sees the same rounded view of the host. Capabilities record what could
//! actually be read, and a source that could not be read produces a degraded
//! capability instead of a silent default.

use std::path::Path;

use super::fs::{Clock, FileSource};
use super::meminfo::{parse_meminfo, MemInfo};
use super::psi::{parse_psi, PsiReading};
use super::vmstat::{parse_vmstat, VmStat};
use crate::{CapabilityReport, CapabilityState};

/// `/proc/meminfo`.
pub const PROC_MEMINFO: &str = "/proc/meminfo";
/// `/proc/pressure/memory`.
pub const PROC_PRESSURE_MEMORY: &str = "/proc/pressure/memory";
/// `/proc/vmstat`.
pub const PROC_VMSTAT: &str = "/proc/vmstat";
/// The cgroup v2 unified hierarchy mount point.
pub const CGROUP_V2_ROOT: &str = "/sys/fs/cgroup";

/// Capability name for memory pressure stall information.
pub const CAP_PSI: &str = "psi";
/// Capability name for the cgroup v2 unified hierarchy.
pub const CAP_CGROUP_V2: &str = "cgroup_v2";
/// Capability name for `/proc/meminfo`.
pub const CAP_MEMINFO: &str = "meminfo";
/// Capability name for swap activity counters.
pub const CAP_SWAP_ACTIVITY: &str = "swap_activity";

/// Two observations further apart than this are treated as a gap, which can
/// mean the daemon was suspended, starved, or restarted.
pub const MAX_OBSERVATION_GAP_MS: u64 = 5_000;

/// A single host memory observation.
#[derive(Debug, Clone, PartialEq)]
pub struct MemorySnapshot {
    /// Monotonic observation time in milliseconds.
    pub observed_at_ms: u64,
    /// Host memory totals.
    pub meminfo: MemInfo,
    /// Pressure stall information, `None` when PSI is unavailable.
    pub psi: Option<PsiReading>,
    /// Swap activity counters.
    pub vmstat: VmStat,
    /// Which sources could actually be read on this host.
    pub capabilities: CapabilityReport,
}

/// Why a snapshot could not be produced.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TelemetryError {
    /// An essential source could not be read.
    #[error("cannot read {path}: {reason}")]
    Unreadable {
        /// The path that could not be read.
        path: String,
        /// The underlying reason.
        reason: String,
    },
    /// An essential source was read but could not be parsed.
    #[error("cannot parse {path}: {reason}")]
    Parse {
        /// The path that could not be parsed.
        path: String,
        /// The parse failure.
        reason: String,
    },
}

/// Collect one host memory snapshot.
///
/// `/proc/meminfo` is essential, since without it there is nothing to reason
/// about. Everything else degrades: a missing PSI or vmstat source records an
/// unavailable capability and the snapshot still succeeds.
pub fn collect_host(
    fs: &dyn FileSource,
    clock: &dyn Clock,
) -> Result<MemorySnapshot, TelemetryError> {
    let mut capabilities = CapabilityReport::new();

    let meminfo = match fs.read_to_string(Path::new(PROC_MEMINFO)) {
        Ok(text) => {
            let parsed = parse_meminfo(&text).map_err(|err| TelemetryError::Parse {
                path: PROC_MEMINFO.to_string(),
                reason: err.to_string(),
            })?;
            capabilities.set(CAP_MEMINFO, CapabilityState::Available);
            parsed
        }
        Err(err) => {
            return Err(TelemetryError::Unreadable {
                path: PROC_MEMINFO.to_string(),
                reason: err.to_string(),
            })
        }
    };

    let psi = match fs.read_to_string(Path::new(PROC_PRESSURE_MEMORY)) {
        Ok(text) => match parse_psi(&text) {
            Ok(reading) => {
                capabilities.set(CAP_PSI, CapabilityState::Available);
                Some(reading)
            }
            Err(err) => {
                capabilities.set(
                    CAP_PSI,
                    CapabilityState::Unavailable {
                        reason: err.to_string(),
                    },
                );
                None
            }
        },
        Err(err) => {
            capabilities.set(
                CAP_PSI,
                CapabilityState::Unavailable {
                    reason: format!("cannot read {PROC_PRESSURE_MEMORY}: {err}"),
                },
            );
            None
        }
    };

    let vmstat = match fs.read_to_string(Path::new(PROC_VMSTAT)) {
        Ok(text) => {
            let parsed = parse_vmstat(&text);
            if parsed.pswpin.is_some() || parsed.pswpout.is_some() {
                capabilities.set(CAP_SWAP_ACTIVITY, CapabilityState::Available);
            } else {
                capabilities.set(
                    CAP_SWAP_ACTIVITY,
                    CapabilityState::Unavailable {
                        reason: "pswpin and pswpout missing from vmstat".to_string(),
                    },
                );
            }
            parsed
        }
        Err(err) => {
            capabilities.set(
                CAP_SWAP_ACTIVITY,
                CapabilityState::Unavailable {
                    reason: format!("cannot read {PROC_VMSTAT}: {err}"),
                },
            );
            VmStat::default()
        }
    };

    if fs.is_file(&Path::new(CGROUP_V2_ROOT).join("cgroup.controllers")) {
        capabilities.set(CAP_CGROUP_V2, CapabilityState::Available);
    } else {
        capabilities.set(
            CAP_CGROUP_V2,
            CapabilityState::Unavailable {
                reason: "cgroup v2 unified hierarchy not detected".to_string(),
            },
        );
    }

    Ok(MemorySnapshot {
        observed_at_ms: clock.monotonic_ms(),
        meminfo,
        psi,
        vmstat,
        capabilities,
    })
}

/// Whether two observation times are far enough apart to be a gap.
pub fn is_gap(previous_ms: u64, current_ms: u64) -> bool {
    current_ms.saturating_sub(previous_ms) > MAX_OBSERVATION_GAP_MS
}

#[cfg(test)]
mod tests {
    use super::super::fs::test_doubles::{FixedClock, MemoryFs};
    use super::*;

    const MEMINFO: &str = "MemTotal: 1000 kB\nMemFree: 100 kB\nMemAvailable: 400 kB\n";
    const PSI: &str =
        "some avg10=0.00 avg60=0.00 avg300=0.00 total=1\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=1\n";
    const VMSTAT: &str = "pswpin 10\npswpout 20\n";

    fn full_host() -> MemoryFs {
        MemoryFs::new()
            .with(PROC_MEMINFO, MEMINFO)
            .with(PROC_PRESSURE_MEMORY, PSI)
            .with(PROC_VMSTAT, VMSTAT)
            .with(
                &format!("{CGROUP_V2_ROOT}/cgroup.controllers"),
                "cpuset cpu io memory\n",
            )
    }

    #[test]
    fn a_full_host_reports_every_capability() {
        let snapshot = collect_host(&full_host(), &FixedClock(1234)).unwrap();
        assert_eq!(snapshot.observed_at_ms, 1234);
        assert!(snapshot.psi.is_some());
        assert_eq!(snapshot.vmstat.pswpin, Some(10));
        for cap in [CAP_MEMINFO, CAP_PSI, CAP_SWAP_ACTIVITY, CAP_CGROUP_V2] {
            assert!(snapshot.capabilities.is_usable(cap), "{cap} unusable");
        }
    }

    #[test]
    fn missing_psi_degrades_but_still_snapshots() {
        let fs = full_host().without(PROC_PRESSURE_MEMORY);
        let snapshot = collect_host(&fs, &FixedClock(0)).unwrap();
        assert_eq!(snapshot.psi, None);
        assert!(!snapshot.capabilities.is_usable(CAP_PSI));
        assert!(snapshot
            .capabilities
            .get(CAP_PSI)
            .unwrap()
            .reason()
            .is_some());
    }

    #[test]
    fn missing_vmstat_degrades_swap_activity() {
        let fs = full_host().without(PROC_VMSTAT);
        let snapshot = collect_host(&fs, &FixedClock(0)).unwrap();
        assert_eq!(snapshot.vmstat, VmStat::default());
        assert!(!snapshot.capabilities.is_usable(CAP_SWAP_ACTIVITY));
    }

    #[test]
    fn missing_cgroup_v2_is_reported() {
        let fs = MemoryFs::new()
            .with(PROC_MEMINFO, MEMINFO)
            .with(PROC_PRESSURE_MEMORY, PSI)
            .with(PROC_VMSTAT, VMSTAT);
        let snapshot = collect_host(&fs, &FixedClock(0)).unwrap();
        assert!(!snapshot.capabilities.is_usable(CAP_CGROUP_V2));
    }

    #[test]
    fn unreadable_meminfo_is_a_hard_error() {
        let fs = MemoryFs::new();
        assert!(matches!(
            collect_host(&fs, &FixedClock(0)),
            Err(TelemetryError::Unreadable { .. })
        ));
    }

    #[test]
    fn malformed_meminfo_is_a_parse_error() {
        let fs = MemoryFs::new().with(PROC_MEMINFO, "garbage\n");
        assert!(matches!(
            collect_host(&fs, &FixedClock(0)),
            Err(TelemetryError::Parse { .. })
        ));
    }

    #[test]
    fn a_long_pause_reads_as_a_gap() {
        // A normal interval is not a gap.
        assert!(!is_gap(1000, 2000));
        // A suspension or a starved daemon shows up as a large jump.
        assert!(is_gap(1000, 1000 + MAX_OBSERVATION_GAP_MS + 1));
    }

    #[test]
    fn a_backwards_reading_is_not_a_gap() {
        assert!(!is_gap(5000, 4000));
    }
}
