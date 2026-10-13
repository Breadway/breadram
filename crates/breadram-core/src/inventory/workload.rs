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
    container_root, parse_cgroup_path, parse_cmdline, parse_proc_stat, parse_proc_status,
    systemd_unit,
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
    /// The cgroup's effective memory limit (`memory.max`), when one is set.
    ///
    /// This is the group's own cap, distinct from host `MemAvailable`. It is
    /// `None` when the group has no cap (`max`) or the file is unreadable.
    pub limit_bytes: Option<u64>,
    /// The tightest `memory.max` on the parent chain, when one is set.
    ///
    /// This is never the workload's own cap: a soft limit writes that cap, so
    /// clamping a budget against it would clamp the budget against itself. It
    /// is `None` when every ancestor is unlimited (`max`) or unreadable.
    pub ancestor_bytes: Option<u64>,
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
        // Account a container once at its host-visible boundary, not once for
        // the parent and again for each nested worker's inclusive memory.current.
        let container = cgroup.as_deref().and_then(container_root);
        let cgroup = container.clone().or(cgroup);
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
        let (scope, name, identity_known) = if let Some(path) = container {
            (
                format!("cgroup:{path}"),
                path.rsplit('/').next().unwrap_or(&path).to_string(),
                true,
            )
        } else if let Some(unit) = unit {
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
            // The group's own cap, read from the same cgroup it is charged to,
            // so a container's effective limit is reported rather than inferred
            // from the host.
            let limit_bytes = group
                .cgroup
                .as_deref()
                .and_then(|path| read_cgroup_limit(fs, path));
            // The tightest parent cap, read from the chain above the group, so
            // a budget never plans a limit a parent would already forbid.
            let ancestor_bytes = group
                .cgroup
                .as_deref()
                .and_then(|path| ancestor_limit(fs, path));
            Workload {
                id: WorkloadId {
                    scope: group.scope,
                    generation: group.generation,
                },
                name: group.name,
                pids,
                accounting,
                control,
                limit_bytes,
                ancestor_bytes,
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

/// The group's effective memory cap, `memory.max`, when one is set.
///
/// `max` means no cap and yields `None`, so "no limit" and "unreadable" never
/// look the same as a byte figure. This is the cgroup's own ceiling, which is
/// what a container is actually bounded by, not the host's `MemAvailable`.
fn read_cgroup_limit(fs: &dyn FileSource, cgroup_path: &str) -> Option<u64> {
    let relative = cgroup_path.trim_start_matches('/');
    let path = Path::new(CGROUP_V2_ROOT).join(relative).join("memory.max");
    fs.read_to_string(&path).ok()?.trim().parse().ok()
}

/// The tightest `memory.max` on the parent chain, when one is set.
///
/// The workload's own `memory.max` is the cap a soft limit writes, so it is
/// not an ancestor: the walk starts at the next directory up. Every level up
/// to (but not including) the cgroup root is read, and the smallest set limit
/// wins, because a child is bounded by its tightest parent. The cgroup root
/// has no parent that breadram may treat as a limit. `max` means unlimited at
/// that level and is skipped, as is an unreadable file: a level that cannot be
/// read constrains nothing, and the walk continues above it.
pub fn ancestor_limit(fs: &dyn FileSource, cgroup_path: &str) -> Option<u64> {
    let mut current = Path::new(cgroup_path.trim_matches('/')).to_path_buf();
    let mut tightest: Option<u64> = None;
    while let Some(parent) = current.parent() {
        let Some(text) = parent.to_str().filter(|text| !text.is_empty()) else {
            break;
        };
        let path = Path::new(CGROUP_V2_ROOT).join(text).join("memory.max");
        if let Some(raw) = fs
            .read_to_string(&path)
            .ok()
            .map(|text| text.trim().to_string())
        {
            // `max` is unlimited, not zero: it constrains nothing.
            if raw != "max" {
                if let Ok(bytes) = raw.parse::<u64>() {
                    tightest = Some(tightest.map_or(bytes, |best: u64| best.min(bytes)));
                }
            }
        }
        current = parent.to_path_buf();
    }
    tightest
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
    fn the_ancestor_limit_is_the_tightest_parent_cap() {
        let fs = MemoryFs::new()
            .with(
                "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice/app.service/memory.max",
                "8589934592\n",
            )
            .with(
                "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice/memory.max",
                "4294967296\n",
            )
            .with(
                "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/memory.max",
                "max\n",
            );
        assert_eq!(
            ancestor_limit(
                &fs,
                "/user.slice/user-1000.slice/user@1000.service/app.slice/app.service"
            ),
            Some(4_294_967_296),
            "the tightest parent wins, the unit's own cap is not an ancestor, max is unlimited"
        );
        assert_eq!(
            ancestor_limit(&fs, "/user.slice/user-1000.slice/user@1000.service"),
            None,
            "higher levels are unlimited or unreadable here"
        );
        assert_eq!(
            ancestor_limit(&fs, "/"),
            None,
            "the cgroup root has no ancestor"
        );
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
    fn nested_container_workers_share_one_host_charge_and_generation() {
        for runtime in ["docker", "libpod"] {
            let root = format!("/user.slice/{runtime}-{}.scope", "a".repeat(64));
            let fs = MemoryFs::new()
                .with("/proc/100/stat", &stat_line(100, "server", 1, 5000))
                .with("/proc/100/cgroup", &format!("0::{root}\n"))
                .with("/proc/101/stat", &stat_line(101, "worker", 100, 5001))
                .with("/proc/101/cgroup", &format!("0::{root}/workers\n"))
                .with(&format!("/sys/fs/cgroup{root}/memory.current"), "4096")
                .with(&format!("/sys/fs/cgroup{root}/memory.max"), "1073741824")
                .with(
                    &format!("/sys/fs/cgroup{root}/workers/memory.current"),
                    "2048",
                );
            let inventory = collect_inventory(&fs);
            assert_eq!(inventory.workloads.len(), 1);
            assert_eq!(inventory.workloads[0].pids, vec![100, 101]);
            assert_eq!(inventory.workloads[0].accounting.bytes(), Some(4096));
            assert_eq!(inventory.workloads[0].id.scope, format!("cgroup:{root}"));
            // The group's own cap is read from the boundary, distinct from the
            // host, and the nested worker's own (absent) cap does not shadow it.
            assert_eq!(inventory.workloads[0].limit_bytes, Some(1_073_741_824));
        }
    }

    #[test]
    fn an_unlimited_group_reports_no_limit_rather_than_zero() {
        let fs = MemoryFs::new()
            .with("/proc/100/stat", &stat_line(100, "server", 1, 5000))
            .with("/proc/100/cgroup", &unit_cgroup("app.service"))
            .with(
                "/sys/fs/cgroup/user.slice/user-1000.slice/app.service/memory.current",
                "4096",
            )
            .with(
                "/sys/fs/cgroup/user.slice/user-1000.slice/app.service/memory.max",
                "max\n",
            );
        let inventory = collect_inventory(&fs);
        assert_eq!(inventory.workloads[0].limit_bytes, None);
    }

    #[test]
    fn an_unreadable_cgroup_falls_back_to_estimated_rss_and_reports_no_limit() {
        // Limited permissions: the cgroup accounting files cannot be read. The
        // public inventory path must still produce a workload, with an honest
        // estimate and no fabricated limit, rather than failing or reporting zero.
        let fs = MemoryFs::new()
            .with("/proc/100/stat", &stat_line(100, "server", 1, 5000))
            .with("/proc/100/cgroup", &unit_cgroup("app.service"))
            .with("/proc/100/status", "VmRSS:\t 2048 kB\n");
        let inventory = collect_inventory(&fs);
        let workload = &inventory.workloads[0];
        assert!(
            workload.accounting.is_estimate(),
            "unreadable accounting is an estimate, not a measured zero"
        );
        assert_eq!(workload.accounting.bytes(), Some(2 * 1024 * 1024));
        assert_eq!(workload.limit_bytes, None);
    }

    #[test]
    fn a_replaced_container_gets_a_new_generation() {
        let root = format!("/user.slice/docker-{}.scope", "b".repeat(64));
        let before = MemoryFs::new()
            .with("/proc/100/stat", &stat_line(100, "server", 1, 5000))
            .with("/proc/100/cgroup", &format!("0::{root}\n"))
            .with(&format!("/sys/fs/cgroup{root}/memory.current"), "4096");
        let after = MemoryFs::new()
            .with("/proc/200/stat", &stat_line(200, "server", 1, 9000))
            .with("/proc/200/cgroup", &format!("0::{root}\n"))
            .with(&format!("/sys/fs/cgroup{root}/memory.current"), "4096");
        let first = collect_inventory(&before);
        let second = collect_inventory(&after);
        assert_eq!(first.workloads[0].id.scope, second.workloads[0].id.scope);
        assert_ne!(
            first.workloads[0].id.generation, second.workloads[0].id.generation,
            "a reused container path is a new generation, not the workload that used to be there"
        );
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
