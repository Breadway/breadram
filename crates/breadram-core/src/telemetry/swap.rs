//! Swap, compression, and existing OOM management, as this host actually has it.
//!
//! breadram reports this layout; it never changes it. Swappiness, swap files,
//! `fstab`, and kernel parameters belong to the administrator, and a second
//! competing OOM killer is exactly what this module exists to avoid becoming.
//! The report is therefore descriptive everywhere: what is configured, what it
//! costs to use, and which constraint applies when there is none.
//!
//! Three rules run through the module:
//!
//! 1. **Missing is not the same as absent.** A file that cannot be read is
//!    reported as unreadable, while a file that says there is no swap is reported
//!    as no swap. Those are different findings and the notes say so.
//! 2. **The layout is never assumed.** A machine may have disk swap, compressed
//!    swap, both, or neither, and the report is built from what is on the host.
//! 3. **Nothing is modified.** Every function here reads, and there is no write
//!    path in this module at all.

use std::path::Path;

use serde::{Deserialize, Serialize};

use super::fs::FileSource;
use super::vmstat::parse_vmstat;

/// The swap table.
pub const PROC_SWAPS: &str = "/proc/swaps";
/// The kernel's compressed swap cache parameters.
pub const ZSWAP_PARAMETERS: &str = "/sys/module/zswap/parameters";
/// The block device tree, where zram devices appear.
pub const SYS_BLOCK: &str = "/sys/block";
/// The unit file locations a systemd OOM manager would be installed at.
pub const OOMD_UNITS: [&str; 3] = [
    "/usr/lib/systemd/system/systemd-oomd.service",
    "/lib/systemd/system/systemd-oomd.service",
    "/etc/systemd/system/systemd-oomd.service",
];

/// What a swap area is backed by, which decides what reclaim costs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwapKind {
    /// A block device.
    Partition,
    /// A regular file.
    File,
    /// A kind this build does not recognise. Reported rather than guessed at.
    Unknown,
}

/// One swap area, as the kernel's swap table describes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwapArea {
    /// The device or file path.
    pub path: String,
    /// What it is backed by.
    pub kind: SwapKind,
    /// Its size in bytes.
    pub size_bytes: u64,
    /// How much of it is in use, in bytes.
    pub used_bytes: u64,
    /// Its configured priority, when the kernel reports one.
    pub priority: Option<i32>,
}

impl SwapArea {
    /// Whether this area is a compressed device rather than disk.
    pub fn compressed(&self) -> bool {
        self.path.contains("zram") || self.path.contains("zswap")
    }
}

/// The kernel's compressed swap cache, when the host uses one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZswapState {
    /// Whether the cache is switched on.
    pub enabled: bool,
    /// The share of RAM the compressed pool may use, when reported.
    pub max_pool_percent: Option<u64>,
    /// The compression algorithm, when reported.
    pub compressor: Option<String>,
    /// The allocator backing the pool, when reported.
    pub zpool: Option<String>,
}

/// One compressed RAM block device.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZramDevice {
    /// The device name, for example `zram0`.
    pub name: String,
    /// Its configured size in bytes.
    pub disksize_bytes: Option<u64>,
    /// The memory it is actually using, in bytes.
    pub mem_used_bytes: Option<u64>,
    /// The compression algorithm, when reported.
    pub algorithm: Option<String>,
}

/// The OOM management this host already has.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OomManagement {
    /// Whether a systemd OOM manager unit is installed.
    pub systemd_oomd_installed: bool,
    /// How many OOM kills the kernel has recorded since boot.
    ///
    /// `None` means the counter could not be read, which is not the same as a
    /// count of zero.
    pub kernel_oom_kills: Option<u64>,
}

/// What limits a workload's swap, as its cgroup reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "limit", rename_all = "snake_case")]
pub enum SwapLimit {
    /// The cgroup may swap without a limit, which is the kernel's `max`.
    Unlimited,
    /// A byte cap.
    Bytes {
        /// The cap in bytes.
        bytes: u64,
    },
    /// The limit could not be read, so nothing is claimed about it.
    Unknown,
}

impl SwapLimit {
    /// The cap in bytes, when there is one.
    pub fn bytes(self) -> Option<u64> {
        match self {
            SwapLimit::Bytes { bytes } => Some(bytes),
            SwapLimit::Unlimited | SwapLimit::Unknown => None,
        }
    }

    /// Whether swap is disabled for this workload.
    pub fn disabled(self) -> bool {
        matches!(self, SwapLimit::Bytes { bytes } if bytes == 0)
    }
}

/// The swap state of one cgroup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CgroupSwap {
    /// `memory.swap.current` in bytes.
    pub current_bytes: Option<u64>,
    /// `memory.swap.max`, as read.
    pub limit: SwapLimit,
    /// `memory.swap.high`, the throttle threshold, as read.
    pub high_limit: SwapLimit,
    /// How many OOM kills this cgroup's own `memory.events` recorded.
    pub oom_kills: Option<u64>,
}

impl CgroupSwap {
    /// The constraint in words, without naming a scope.
    ///
    /// A phrase is always produced, including when nothing could be read, so a
    /// limit that is absent is explained rather than left to be misread as zero.
    pub fn limit_phrase(&self) -> String {
        match self.limit {
            SwapLimit::Bytes { bytes: 0 } => "may not swap at all".to_string(),
            SwapLimit::Bytes { bytes } => format!("may swap up to {bytes} bytes"),
            SwapLimit::Unlimited => {
                "may swap without a limit, which is the kernel default".to_string()
            }
            SwapLimit::Unknown => {
                "the swap limit could not be read, so whether swap is capped here is not known"
                    .to_string()
            }
        }
    }

    /// Why this workload's swap is constrained the way it is, naming the scope.
    pub fn explain(&self, scope: &str) -> String {
        match self.limit {
            // The unknown case is a statement about the scope rather than a
            // property of it, so it reads better as its own clause.
            SwapLimit::Unknown => format!("{scope}: {}", self.limit_phrase()),
            _ => format!("{scope} {}", self.limit_phrase()),
        }
    }
}

/// One controllable workload and the swap limit that applies to it.
///
/// This is what makes a limit reportable rather than theoretical: the figure
/// comes from the cgroup the workload is actually charged to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadSwap {
    /// The workload's stable scope key.
    pub scope: String,
    /// The cgroup the figure was read from.
    pub cgroup: String,
    /// What was read there.
    pub swap: CgroupSwap,
}

/// Everything breadram knows about swap and OOM management on this host.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwapLayout {
    /// The configured swap areas.
    pub areas: Vec<SwapArea>,
    /// Total configured swap in bytes.
    pub total_bytes: u64,
    /// Swap in use in bytes.
    pub used_bytes: u64,
    /// The compressed swap cache, when the kernel exposes one.
    pub zswap: Option<ZswapState>,
    /// Compressed RAM block devices.
    pub zram: Vec<ZramDevice>,
    /// The OOM management that is already installed.
    pub oom: OomManagement,
    /// Pages swapped in since boot, from `/proc/vmstat`.
    pub swap_in_pages: Option<u64>,
    /// Pages swapped out since boot, from `/proc/vmstat`.
    pub swap_out_pages: Option<u64>,
    /// Whether `/proc/swaps` could be read at all.
    pub swap_table_readable: bool,
}

impl SwapLayout {
    /// Whether any configured swap compresses.
    ///
    /// This is what decides what reclaim costs: CPU instead of I/O, or I/O.
    pub fn compressed(&self) -> bool {
        self.zswap.as_ref().is_some_and(|zswap| zswap.enabled)
            || self.areas.iter().any(SwapArea::compressed)
            || !self.zram.is_empty()
    }

    /// Whether the host has no swap at all, and it is known rather than assumed.
    pub fn has_no_swap(&self) -> bool {
        self.swap_table_readable && self.areas.is_empty() && self.zram.is_empty()
    }

    /// The constraints this layout puts on relief, in words.
    ///
    /// Each line names a fact and its consequence, so an operator reading the
    /// report knows why breadram behaves the way it does here, and so a missing
    /// swap and an unreadable one never read the same.
    pub fn notes(&self) -> Vec<String> {
        let mut notes = Vec::new();
        if !self.swap_table_readable {
            notes.push(
                "the swap table could not be read, so whether this host has swap is unknown"
                    .to_string(),
            );
        } else if self.has_no_swap() {
            notes.push(
                "no swap is configured, so an anonymous page cannot be moved out of RAM: relief comes from reclaiming caches and, at the limit, from the kernel OOM killer"
                    .to_string(),
            );
        } else if self.compressed() {
            notes.push(
                "swap is compressed on this host, so reclaiming an anonymous page costs CPU rather than disk I/O"
                    .to_string(),
            );
        } else {
            notes.push(
                "swap is configured without compression, so reclaiming an anonymous page costs disk I/O"
                    .to_string(),
            );
        }

        if !self.zram.is_empty() {
            notes.push(format!(
                "{} compressed RAM device(s) are configured; breadram reports their size and usage and never resizes one",
                self.zram.len()
            ));
        }
        if let Some(zswap) = &self.zswap {
            if zswap.enabled {
                match zswap.max_pool_percent {
                    Some(percent) => notes.push(format!(
                        "the compressed swap cache is enabled and may hold up to {percent}% of RAM; breadram never changes that share"
                    )),
                    None => notes.push(
                        "the compressed swap cache is enabled; its pool share could not be read and is left alone"
                            .to_string(),
                    ),
                }
            }
        }

        if self.oom.systemd_oomd_installed {
            notes.push(
                "a systemd OOM manager is installed and remains the OOM manager on this host: breadram reports OOM activity and installs no second killer"
                    .to_string(),
            );
        } else {
            notes.push(
                "no separate OOM manager was detected, so the kernel OOM killer is the only one and breadram installs no second killer"
                    .to_string(),
            );
        }
        match self.oom.kernel_oom_kills {
            Some(0) => notes.push("no OOM kill has been recorded since boot".to_string()),
            Some(count) => notes.push(format!(
                "the kernel has recorded {count} OOM kill(s) since boot, which breadram reports rather than duplicates"
            )),
            None => notes.push(
                "the kernel OOM kill counter could not be read, so OOM activity since boot is unknown"
                    .to_string(),
            ),
        }

        notes.push(
            "breadram never changes global swappiness, swap files, fstab entries, or kernel parameters; the swap layout above is the administrator's"
                .to_string(),
        );
        notes
    }
}

/// Parse the kernel's swap table.
///
/// The file is whitespace aligned with a header line. A malformed row is skipped
/// rather than failing the read: the table is the kernel's and one odd line
/// should not hide the rest.
pub fn parse_swaps(contents: &str) -> Vec<SwapArea> {
    let mut areas = Vec::new();
    for line in contents.lines().skip(1) {
        let mut parts = line.split_whitespace();
        let (Some(path), Some(kind), Some(size), Some(used)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        // The header of an empty table is still a header, so a row that names a
        // size in kilobytes is the only kind worth keeping.
        let (Ok(size), Ok(used)) = (size.parse::<u64>(), used.parse::<u64>()) else {
            continue;
        };
        areas.push(SwapArea {
            path: path.to_string(),
            kind: match kind {
                "partition" => SwapKind::Partition,
                "file" => SwapKind::File,
                _ => SwapKind::Unknown,
            },
            // The table reports kibibytes.
            size_bytes: size.saturating_mul(1024),
            used_bytes: used.saturating_mul(1024),
            priority: parts.next().and_then(|raw| raw.parse::<i32>().ok()),
        });
    }
    areas
}

/// Read the whole swap and OOM picture from the host.
pub fn collect_swap(fs: &dyn FileSource) -> SwapLayout {
    let (areas, swap_table_readable) = match fs.read_to_string(Path::new(PROC_SWAPS)) {
        Ok(contents) => (parse_swaps(&contents), true),
        Err(_) => (Vec::new(), false),
    };
    let total_bytes = areas.iter().map(|area| area.size_bytes).sum();
    let used_bytes = areas.iter().map(|area| area.used_bytes).sum();

    let zram = collect_zram(fs);
    let zswap = collect_zswap(fs);
    let vmstat = fs
        .read_to_string(Path::new(super::snapshot::PROC_VMSTAT))
        .ok()
        .map(|contents| parse_vmstat(&contents));
    let oom = OomManagement {
        systemd_oomd_installed: OOMD_UNITS.iter().any(|unit| fs.is_file(Path::new(unit))),
        kernel_oom_kills: vmstat.as_ref().and_then(|stat| stat.oom_kill),
    };

    SwapLayout {
        areas,
        total_bytes,
        used_bytes,
        zswap,
        zram,
        oom,
        swap_in_pages: vmstat.as_ref().and_then(|stat| stat.pswpin),
        swap_out_pages: vmstat.as_ref().and_then(|stat| stat.pswpout),
        swap_table_readable,
    }
}

/// Read the compressed swap cache parameters, when the module is loaded.
fn collect_zswap(fs: &dyn FileSource) -> Option<ZswapState> {
    let parameters = Path::new(ZSWAP_PARAMETERS);
    if !fs.is_file(parameters.join("enabled").as_path()) {
        return None;
    }
    Some(ZswapState {
        // The kernel writes `Y` or `N` here.
        enabled: read(fs, parameters, "enabled")
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("y")),
        max_pool_percent: read(fs, parameters, "max_pool_percent")
            .and_then(|value| value.trim().parse::<u64>().ok()),
        compressor: read(fs, parameters, "compressor").map(|value| value.trim().to_string()),
        zpool: read(fs, parameters, "zpool").map(|value| value.trim().to_string()),
    })
}

/// Read every compressed RAM block device.
fn collect_zram(fs: &dyn FileSource) -> Vec<ZramDevice> {
    let Ok(names) = fs.list_dir(Path::new(SYS_BLOCK)) else {
        return Vec::new();
    };
    let mut devices = Vec::new();
    for name in names {
        if !name.starts_with("zram") {
            continue;
        }
        let dir = Path::new(SYS_BLOCK).join(&name);
        devices.push(ZramDevice {
            name,
            disksize_bytes: read(fs, &dir, "disksize")
                .and_then(|value| value.trim().parse::<u64>().ok()),
            mem_used_bytes: read(fs, &dir, "mem_used_total")
                .and_then(|value| value.trim().parse::<u64>().ok()),
            algorithm: read(fs, &dir, "comp_algorithm")
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
        });
    }
    devices.sort_by(|left, right| left.name.cmp(&right.name));
    devices
}

/// Read a cgroup's swap state.
///
/// This is what makes a workload's swap limit reportable: `memory.swap.max` is
/// `max` for unlimited, a byte count for a cap, and absent altogether on a kernel
/// or a cgroup where the swap controller is not enabled. The three are reported
/// separately, because "unlimited" and "unknown" lead to different advice.
pub fn collect_cgroup_swap(fs: &dyn FileSource, cgroup_path: &str) -> CgroupSwap {
    let dir = Path::new(super::snapshot::CGROUP_V2_ROOT).join(cgroup_path.trim_start_matches('/'));
    let parse_limit = |name: &str| match read(fs, &dir, name) {
        Some(text) => match text.trim() {
            "max" => SwapLimit::Unlimited,
            raw => match raw.parse::<u64>() {
                Ok(bytes) => SwapLimit::Bytes { bytes },
                Err(_) => SwapLimit::Unknown,
            },
        },
        None => SwapLimit::Unknown,
    };
    let oom_kills = read(fs, &dir, "memory.events").and_then(|text| {
        text.lines().find_map(|line| {
            let (key, value) = line.split_once(' ')?;
            (key == "oom_kill")
                .then(|| value.trim().parse::<u64>().ok())
                .flatten()
        })
    });

    CgroupSwap {
        current_bytes: read(fs, &dir, "memory.swap.current")
            .and_then(|value| value.trim().parse::<u64>().ok()),
        limit: parse_limit("memory.swap.max"),
        high_limit: parse_limit("memory.swap.high"),
        oom_kills,
    }
}

fn read(fs: &dyn FileSource, dir: &Path, name: &str) -> Option<String> {
    fs.read_to_string(&dir.join(name)).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_disk_swap_table_is_parsed_in_bytes() {
        let table = "Filename\t\t\t\tType\t\tSize\t\tUsed\t\tPriority\n\
                     /swapfile                               file            8388604         0               -2\n\
                     /dev/nvme0n1p3                          partition       16777212        1048576         100\n";
        let areas = parse_swaps(table);
        assert_eq!(areas.len(), 2);
        assert_eq!(areas[0].path, "/swapfile");
        assert_eq!(areas[0].kind, SwapKind::File);
        assert_eq!(areas[0].size_bytes, 8_388_604 * 1024);
        assert_eq!(areas[0].used_bytes, 0);
        assert_eq!(areas[0].priority, Some(-2));
        assert_eq!(areas[1].kind, SwapKind::Partition);
        assert_eq!(areas[1].priority, Some(100));
        assert!(!areas[0].compressed());
    }

    #[test]
    fn a_compressed_device_is_recognised_from_the_table() {
        let table = "Filename\t\t\t\tType\t\tSize\t\tUsed\t\tPriority\n\
                     /dev/zram0                              partition       4194300         2097152         100\n";
        let areas = parse_swaps(table);
        assert_eq!(areas.len(), 1);
        assert!(areas[0].compressed(), "a zram device compresses");
    }

    #[test]
    fn an_empty_table_is_no_swap_rather_than_an_error() {
        let table = "Filename\t\t\t\tType\t\tSize\t\tUsed\t\tPriority\n";
        assert!(parse_swaps(table).is_empty());
    }

    #[test]
    fn malformed_rows_are_skipped_rather_than_failing_the_read() {
        let table = "Filename\t\t\t\tType\t\tSize\t\tUsed\t\tPriority\n\
                     /good            file      100  50  -1\n\
                     /broken          file\n\
                     /nonsense        file      notanumber  3  -1\n";
        let areas = parse_swaps(table);
        assert_eq!(areas.len(), 1);
        assert_eq!(areas[0].path, "/good");
        assert_eq!(areas[0].priority, Some(-1));
    }

    #[test]
    fn a_host_with_no_swap_says_so() {
        let fs = crate::telemetry::fs::test_doubles::MemoryFs::new()
            .with(
                PROC_SWAPS,
                "Filename\t\t\t\tType\t\tSize\t\tUsed\t\tPriority\n",
            )
            .with("/proc/vmstat", "pswpin 0\npswpout 0\noom_kill 0\n");
        let layout = collect_swap(&fs);
        assert!(layout.swap_table_readable);
        assert!(layout.has_no_swap());
        assert_eq!(layout.total_bytes, 0);
        assert!(!layout.compressed());
        assert_eq!(layout.oom.kernel_oom_kills, Some(0));
        let notes = layout.notes();
        assert!(
            notes
                .iter()
                .any(|note| note.contains("no swap is configured")),
            "{notes:?}"
        );
        assert!(
            notes
                .iter()
                .any(|note| note.contains("no OOM kill has been recorded")),
            "{notes:?}"
        );
        assert!(
            notes
                .iter()
                .any(|note| note.contains("never changes global swappiness")),
            "the promise is explicit: {notes:?}"
        );
    }

    #[test]
    fn an_unreadable_swap_table_is_unknown_rather_than_empty() {
        let fs = crate::telemetry::fs::test_doubles::MemoryFs::new();
        let layout = collect_swap(&fs);
        assert!(!layout.swap_table_readable);
        assert!(
            !layout.has_no_swap(),
            "nothing was read, so nothing is known"
        );
        assert_eq!(layout.oom.kernel_oom_kills, None);
        let notes = layout.notes();
        assert!(
            notes
                .iter()
                .any(|note| note
                    .contains("could not be read, so whether this host has swap is unknown")),
            "{notes:?}"
        );
        assert!(
            notes
                .iter()
                .any(|note| note.contains("OOM kill counter could not be read")),
            "{notes:?}"
        );
    }

    #[test]
    fn compressed_swap_is_reported_with_its_bounds() {
        let fs = crate::telemetry::fs::test_doubles::MemoryFs::new()
            .with(
                PROC_SWAPS,
                "Filename\t\t\t\tType\t\tSize\t\tUsed\t\tPriority\n\
                 /dev/zram0    partition   4194300   2097152   100\n",
            )
            .with("/proc/vmstat", "pswpin 12\npswpout 34\noom_kill 2\n")
            .with(&format!("{ZSWAP_PARAMETERS}/enabled"), "Y\n")
            .with(&format!("{ZSWAP_PARAMETERS}/max_pool_percent"), "20\n")
            .with(&format!("{ZSWAP_PARAMETERS}/compressor"), "lz4\n")
            .with(&format!("{SYS_BLOCK}/zram0/disksize"), "4294967296\n")
            .with(&format!("{SYS_BLOCK}/zram0/mem_used_total"), "1073741824\n")
            .with(&format!("{SYS_BLOCK}/zram0/comp_algorithm"), "lz4\n")
            .with("/usr/lib/systemd/system/systemd-oomd.service", "[Unit]\n");
        let layout = collect_swap(&fs);
        assert!(layout.compressed());
        assert_eq!(layout.total_bytes, 4_194_300 * 1024);
        assert_eq!(layout.used_bytes, 2_097_152 * 1024);
        assert_eq!(layout.swap_in_pages, Some(12));
        assert_eq!(layout.swap_out_pages, Some(34));
        assert_eq!(layout.zram.len(), 1);
        assert_eq!(layout.zram[0].name, "zram0");
        assert_eq!(layout.zram[0].disksize_bytes, Some(4_294_967_296));
        assert_eq!(layout.zram[0].mem_used_bytes, Some(1_073_741_824));
        let zswap = layout.zswap.clone().expect("zswap is present");
        assert!(zswap.enabled);
        assert_eq!(zswap.max_pool_percent, Some(20));
        assert_eq!(zswap.compressor.as_deref(), Some("lz4"));
        assert!(layout.oom.systemd_oomd_installed);

        let notes = layout.notes();
        assert!(
            notes
                .iter()
                .any(|note| note.contains("costs CPU rather than disk I/O")),
            "{notes:?}"
        );
        assert!(
            notes.iter().any(|note| note.contains("20% of RAM")),
            "{notes:?}"
        );
        assert!(
            notes
                .iter()
                .any(|note| note.contains("remains the OOM manager")),
            "{notes:?}"
        );
        assert!(
            notes
                .iter()
                .any(|note| note.contains("recorded 2 OOM kill(s) since boot")),
            "{notes:?}"
        );
    }

    #[test]
    fn no_zswap_parameters_means_no_zswap_section() {
        let fs = crate::telemetry::fs::test_doubles::MemoryFs::new();
        assert_eq!(collect_zswap(&fs), None);
        assert!(collect_zram(&fs).is_empty());
    }

    #[test]
    fn a_workload_swap_limit_is_read_from_its_cgroup() {
        let root = crate::telemetry::snapshot::CGROUP_V2_ROOT;
        let group = "/user.slice/user-1000.slice/app.service";
        let fs = crate::telemetry::fs::test_doubles::MemoryFs::new()
            .with(&format!("{root}{group}/memory.swap.current"), "1048576\n")
            .with(&format!("{root}{group}/memory.swap.max"), "max\n")
            .with(&format!("{root}{group}/memory.swap.high"), "0\n")
            .with(
                &format!("{root}{group}/memory.events"),
                "low 0\nhigh 0\nmax 0\noom 1\noom_kill 1\n",
            );
        let swap = collect_cgroup_swap(&fs, group);
        assert_eq!(swap.current_bytes, Some(1_048_576));
        assert_eq!(swap.limit, SwapLimit::Unlimited);
        assert_eq!(swap.high_limit, SwapLimit::Bytes { bytes: 0 });
        assert_eq!(swap.oom_kills, Some(1));
        assert!(swap.explain("unit:app.service").contains("without a limit"));
    }

    #[test]
    fn a_disabled_swap_limit_and_an_unreadable_one_read_differently() {
        let root = crate::telemetry::snapshot::CGROUP_V2_ROOT;
        let fs = crate::telemetry::fs::test_doubles::MemoryFs::new()
            .with(&format!("{root}/app/memory.swap.max"), "0\n");
        let disabled = collect_cgroup_swap(&fs, "/app");
        assert!(disabled.limit.disabled());
        assert_eq!(disabled.limit.bytes(), Some(0));
        assert!(disabled
            .explain("unit:app.service")
            .contains("may not swap at all"));

        // Nothing readable: reported as unknown, and never as a cap of zero.
        let missing = collect_cgroup_swap(&fs, "/nothing-here");
        assert_eq!(missing.limit, SwapLimit::Unknown);
        assert_eq!(missing.limit.bytes(), None);
        assert!(!missing.limit.disabled());
        assert!(missing.oom_kills.is_none());
        assert!(missing
            .explain("unit:app.service")
            .contains("could not be read"));
    }
}
