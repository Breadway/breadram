//! Workload identity and grouping.
//!
//! A workload is one or more processes that belong together: a systemd service
//! and its workers, a desktop app scope and its children, or a loose group
//! sharing an executable identity. Grouping by a stable scope key plus a
//! generation derived from process start times means a reused pid or cgroup path
//! cannot be mistaken for the workload that used to be there.
//!
//! Discovery works from procfs alone. It needs none of breadsearch, breadd,
//! Ollama, llama.cpp, or vLLM, and every one of them may be absent.

use std::collections::BTreeMap;
use std::path::Path;

use super::parse::{
    parse_cgroup_path, parse_cmdline, parse_proc_stat, parse_proc_status, systemd_unit,
};
use crate::telemetry::fs::FileSource;
use crate::{CapabilityReport, CapabilityState};

/// The procfs root.
pub const PROC_ROOT: &str = "/proc";
/// The cgroup v2 mount point, where per-cgroup `memory.current` lives.
pub const CGROUP_V2_ROOT: &str = "/sys/fs/cgroup";

/// Capability name for process inventory.
pub const CAP_PROC_INVENTORY: &str = "proc_inventory";

/// What a workload's memory figure means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryAccounting {
    /// From cgroup accounting, which charges each page to exactly one group.
    Cgroup {
        /// `memory.current` for the group, when readable.
        current_bytes: Option<u64>,
    },
    /// Summed resident set size across member processes.
    ///
    /// This over counts, because a shared page is counted in every process that
    /// maps it, so it is always an estimate and never an exact usage.
    RssEstimate {
        /// Summed RSS in bytes.
        rss_bytes: u64,
        /// How many processes were summed.
        processes: usize,
    },
}

impl MemoryAccounting {
    /// The best available byte figure, when there is one.
    pub fn bytes(&self) -> Option<u64> {
        match self {
            MemoryAccounting::Cgroup { current_bytes } => *current_bytes,
            MemoryAccounting::RssEstimate { rss_bytes, .. } => Some(*rss_bytes),
        }
    }

    /// Whether the figure is an estimate rather than exact accounting.
    pub fn is_estimate(&self) -> bool {
        matches!(self, MemoryAccounting::RssEstimate { .. })
    }
}

/// What the daemon may do with a workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlLevel {
    /// Visible, but the daemon will not act on it automatically.
    ObserveOnly,
    /// May receive configured controls, once policy authorizes them.
    Eligible,
}

/// A stable identity for a workload.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct WorkloadId {
    /// The grouping key, for example `unit:ollama.service` or `exe:python3`.
    pub scope: String,
    /// A generation token taken from process start times, so a reused pid or
    /// cgroup path produces a different id.
    pub generation: u64,
}

/// One grouped workload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workload {
    /// Stable identity.
    pub id: WorkloadId,
    /// Human readable name.
    pub name: String,
    /// Member process ids, sorted.
    pub pids: Vec<u32>,
    /// How the memory figure was obtained.
    pub accounting: MemoryAccounting,
    /// What may be done with it.
    pub control: ControlLevel,
}

/// The result of one inventory pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inventory {
    /// Discovered workloads, sorted by id.
    pub workloads: Vec<Workload>,
    /// What could be read on this host.
    pub capabilities: CapabilityReport,
}

struct Group {
    scope: String,
    name: String,
    cgroup: Option<String>,
    identity_known: bool,
    pids: Vec<u32>,
    generation: u64,
    rss_bytes: u64,
    processes: usize,
}

/// Discover the workloads visible in procfs.
///
/// Processes that exit between the directory listing and the read are skipped,
/// which is the normal race rather than an error. A process whose reported pid
/// does not match the directory name is also skipped, because that means the pid
/// was reused mid read.
pub fn collect_inventory(fs: &dyn FileSource) -> Inventory {
    let mut capabilities = CapabilityReport::new();

    let entries = match fs.list_dir(Path::new(PROC_ROOT)) {
        Ok(entries) => {
            capabilities.set(CAP_PROC_INVENTORY, CapabilityState::Available);
            entries
        }
        Err(err) => {
            capabilities.set(
                CAP_PROC_INVENTORY,
                CapabilityState::Unavailable {
                    reason: format!("cannot list {PROC_ROOT}: {err}"),
                },
            );
            return Inventory {
                workloads: Vec::new(),
                capabilities,
            };
        }
    };

    let mut groups: BTreeMap<String, Group> = BTreeMap::new();

    for name in entries {
        let Ok(pid) = name.parse::<u32>() else {
            continue;
        };
        let dir = Path::new(PROC_ROOT).join(&name);
        let stat = fs
            .read_to_string(&dir.join("stat"))
            .ok()
            .and_then(|text| parse_proc_stat(&text).ok());
        let Some(stat) = stat else {
            continue;
        };
        if stat.pid != pid {
            continue;
        }

        let cgroup = fs
            .read_to_string(&dir.join("cgroup"))
            .ok()
            .and_then(|text| parse_cgroup_path(&text));
        let unit = cgroup.as_deref().and_then(systemd_unit);
        let exe = fs
            .read_to_string(&dir.join("cmdline"))
            .ok()
            .and_then(|text| parse_cmdline(&text));
        let status = fs
            .read_to_string(&dir.join("status"))
            .ok()
            .map_or_else(Default::default, |text| parse_proc_status(&text));

        let group_cgroup = cgroup.clone();
        let (scope, name, identity_known) = if let Some(unit) = unit {
            (format!("unit:{unit}"), unit, true)
        } else if let Some(path) = cgroup.as_deref() {
            let name = path.rsplit('/').next().unwrap_or(path).to_string();
            (format!("cgroup:{path}"), name, true)
        } else if let Some(exe) = exe {
            (format!("exe:{exe}"), exe, true)
        } else {
            (format!("comm:{}", stat.comm), stat.comm.clone(), false)
        };

        let group = groups.entry(scope.clone()).or_insert_with(|| Group {
            scope,
            name,
            cgroup: group_cgroup,
            identity_known,
            pids: Vec::new(),
            generation: u64::MAX,
            rss_bytes: 0,
            processes: 0,
        });
        group.identity_known |= identity_known;
        group.pids.push(stat.pid);
        group.generation = group.generation.min(stat.starttime);
        group.rss_bytes = group
            .rss_bytes
            .saturating_add(status.rss_bytes.unwrap_or(0));
        group.processes += 1;
    }

    let mut workloads: Vec<Workload> = groups
        .into_values()
        .map(|group| {
            let accounting = match group
                .cgroup
                .as_deref()
                .and_then(|path| read_cgroup_current(fs, path))
            {
                Some(current) => MemoryAccounting::Cgroup {
                    current_bytes: Some(current),
                },
                None => MemoryAccounting::RssEstimate {
                    rss_bytes: group.rss_bytes,
                    processes: group.processes,
                },
            };
            let control = if group.identity_known {
                ControlLevel::Eligible
            } else {
                ControlLevel::ObserveOnly
            };
            let mut pids = group.pids;
            pids.sort_unstable();
            pids.dedup();
            Workload {
                id: WorkloadId {
                    scope: group.scope,
                    generation: group.generation,
                },
                name: group.name,
                pids,
                accounting,
                control,
            }
        })
        .collect();

    workloads.sort_by(|a, b| a.id.cmp(&b.id));

    Inventory {
        workloads,
        capabilities,
    }
}

fn read_cgroup_current(fs: &dyn FileSource, cgroup_path: &str) -> Option<u64> {
    let relative = cgroup_path.trim_start_matches('/');
    let path = Path::new(CGROUP_V2_ROOT)
        .join(relative)
        .join("memory.current");
    fs.read_to_string(&path).ok()?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::super::super::telemetry::fs::test_doubles::MemoryFs;
    use super::*;

    fn stat_line(pid: u32, comm: &str, ppid: u32, starttime: u64) -> String {
        let mut after = vec!["0".to_string(); 20];
        after[0] = "S".to_string();
        after[1] = ppid.to_string();
        after[19] = starttime.to_string();
        format!("{pid} ({comm}) {}", after.join(" "))
    }

    fn unit_cgroup(unit: &str) -> String {
        format!("0::/user.slice/user-1000.slice/{unit}\n")
    }

    #[test]
    fn groups_a_service_and_its_workers_into_one_workload() {
        let fs = MemoryFs::new()
            .with("/proc/100/stat", &stat_line(100, "server", 1, 5000))
            .with("/proc/100/cgroup", &unit_cgroup("app.service"))
            .with("/proc/100/cmdline", "/usr/bin/server\0--serve\0")
            .with("/proc/100/status", "VmRSS:\t 1000 kB\n")
            .with("/proc/101/stat", &stat_line(101, "worker", 100, 5001))
            .with("/proc/101/cgroup", &unit_cgroup("app.service"))
            .with("/proc/101/cmdline", "/usr/bin/server\0--worker\0")
            .with("/proc/101/status", "VmRSS:\t 2000 kB\n");

        let inventory = collect_inventory(&fs);
        assert_eq!(inventory.workloads.len(), 1);
        let workload = &inventory.workloads[0];
        assert_eq!(workload.id.scope, "unit:app.service");
        assert_eq!(workload.pids, vec![100, 101]);
        assert_eq!(
            workload.id.generation, 5000,
            "generation is the earliest start"
        );
        assert_eq!(workload.control, ControlLevel::Eligible);
    }

    #[test]
    fn cgroup_accounting_is_preferred_over_rss() {
        let fs = MemoryFs::new()
            .with("/proc/100/stat", &stat_line(100, "server", 1, 5000))
            .with("/proc/100/cgroup", &unit_cgroup("app.service"))
            .with("/proc/100/status", "VmRSS:\t 1000 kB\n")
            .with(
                "/sys/fs/cgroup/user.slice/user-1000.slice/app.service/memory.current",
                "536870912\n",
            );

        let inventory = collect_inventory(&fs);
        let accounting = inventory.workloads[0].accounting;
        assert_eq!(accounting.bytes(), Some(536_870_912));
        assert!(!accounting.is_estimate());
    }

    #[test]
    fn rss_is_labelled_as_an_estimate() {
        let fs = MemoryFs::new()
            .with("/proc/100/stat", &stat_line(100, "build", 1, 5000))
            .with("/proc/100/cmdline", "/usr/bin/build\0")
            .with("/proc/100/status", "VmRSS:\t 1000 kB\n")
            .with("/proc/101/stat", &stat_line(101, "build", 100, 5001))
            .with("/proc/101/cmdline", "/usr/bin/build\0")
            .with("/proc/101/status", "VmRSS:\t 2000 kB\n");

        let inventory = collect_inventory(&fs);
        let workload = &inventory.workloads[0];
        assert_eq!(workload.id.scope, "exe:build");
        assert!(workload.accounting.is_estimate());
        assert_eq!(workload.accounting.bytes(), Some(3000 * 1024));
    }

    #[test]
    fn a_reused_cgroup_path_produces_a_new_generation() {
        let first = MemoryFs::new()
            .with("/proc/100/stat", &stat_line(100, "server", 1, 5000))
            .with("/proc/100/cgroup", &unit_cgroup("app.service"));
        let second = MemoryFs::new()
            .with("/proc/100/stat", &stat_line(100, "server", 1, 9000))
            .with("/proc/100/cgroup", &unit_cgroup("app.service"));

        let a = collect_inventory(&first);
        let b = collect_inventory(&second);
        assert_eq!(a.workloads[0].id.scope, b.workloads[0].id.scope);
        assert_ne!(
            a.workloads[0].id.generation, b.workloads[0].id.generation,
            "a different start time must change the generation"
        );
        assert_ne!(a.workloads[0].id, b.workloads[0].id);
    }

    #[test]
    fn an_unidentified_workload_is_observe_only() {
        // No cgroup and an empty cmdline, as a kernel thread has.
        let fs = MemoryFs::new().with("/proc/9/stat", &stat_line(9, "kthreadd", 0, 10));
        let inventory = collect_inventory(&fs);
        assert_eq!(inventory.workloads.len(), 1);
        assert_eq!(inventory.workloads[0].control, ControlLevel::ObserveOnly);
    }

    #[test]
    fn non_numeric_proc_entries_are_ignored() {
        let fs = MemoryFs::new()
            .with("/proc/meminfo", "MemTotal: 1 kB\n")
            .with("/proc/self/stat", "ignored")
            .with("/proc/100/stat", &stat_line(100, "app", 1, 1));
        let inventory = collect_inventory(&fs);
        assert_eq!(inventory.workloads.len(), 1);
    }

    #[test]
    fn a_process_that_exited_mid_read_is_skipped() {
        // Directory entry exists but stat is unreadable.
        let fs = MemoryFs::new().with("/proc/100/cgroup", &unit_cgroup("app.service"));
        let inventory = collect_inventory(&fs);
        assert!(inventory.workloads.is_empty());
    }

    #[test]
    fn a_pid_reuse_race_is_detected() {
        // /proc/100/stat reports a different pid, so the entry is stale.
        let fs = MemoryFs::new().with("/proc/100/stat", &stat_line(999, "other", 1, 1));
        let inventory = collect_inventory(&fs);
        assert!(inventory.workloads.is_empty());
    }

    #[test]
    fn a_proc_listing_failure_degrades_the_capability() {
        let fs = MemoryFs::new();
        let inventory = collect_inventory(&fs);
        assert!(inventory.workloads.is_empty());
        assert!(!inventory.capabilities.is_usable(CAP_PROC_INVENTORY));
    }
}
