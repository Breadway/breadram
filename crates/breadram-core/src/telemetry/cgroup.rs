//! Parsers for cgroup v2 memory files, and a collector for one cgroup.
//!
//! A cgroup's memory is reported from its own `memory.*` files. Consumers must
//! not sum a parent and its children, because a child's pages are already
//! charged to the parent; reading each group directly and letting the caller
//! pick the level it wants avoids that double count.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use super::fs::FileSource;
use super::psi::{parse_psi, PsiReading};

/// Memory readings for one cgroup v2 directory.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CgroupMemory {
    /// `memory.current` in bytes.
    pub current: Option<u64>,
    /// `memory.stat` values. Most are bytes; some, such as `oom_kill`, are counts.
    pub stat: HashMap<String, u64>,
    /// `memory.events` counters such as `oom`, `oom_kill`, `high`, and `max`.
    pub events: HashMap<String, u64>,
    /// `memory.pressure`, when PSI is enabled for cgroups.
    pub pressure: Option<PsiReading>,
}

/// A cgroup directory and its memory readings.
#[derive(Debug, Clone, PartialEq)]
pub struct CgroupSnapshot {
    /// The cgroup directory that was read.
    pub path: PathBuf,
    /// The readings. Unreadable files are recorded as absent.
    pub memory: CgroupMemory,
}

/// Read the memory files of a cgroup v2 directory.
///
/// Unreadable files are recorded as absent rather than failing the whole read,
/// because cgroups appear and disappear constantly and a missing file usually
/// just means the group exited.
pub fn collect_cgroup(fs: &dyn FileSource, dir: &Path) -> CgroupSnapshot {
    let current = read(fs, dir, "memory.current").and_then(|text| parse_single_u64(&text));
    let stat = read(fs, dir, "memory.stat").map_or_else(HashMap::new, |text| parse_keyed(&text));
    let events =
        read(fs, dir, "memory.events").map_or_else(HashMap::new, |text| parse_keyed(&text));
    let pressure = read(fs, dir, "memory.pressure").and_then(|text| parse_psi(&text).ok());

    CgroupSnapshot {
        path: dir.to_path_buf(),
        memory: CgroupMemory {
            current,
            stat,
            events,
            pressure,
        },
    }
}

fn read(fs: &dyn FileSource, dir: &Path, name: &str) -> Option<String> {
    fs.read_to_string(&dir.join(name)).ok()
}

/// Parse a file holding a single unsigned integer, such as `memory.current`.
pub(crate) fn parse_single_u64(contents: &str) -> Option<u64> {
    contents.trim().parse().ok()
}

/// Parse `key value` lines where the value is an unsigned integer.
pub(crate) fn parse_keyed(contents: &str) -> HashMap<String, u64> {
    let mut fields = HashMap::new();
    for raw in contents.lines() {
        let mut parts = raw.split_whitespace();
        let (Some(key), Some(value)) = (parts.next(), parts.next()) else {
            continue;
        };
        if let Ok(number) = value.parse::<u64>() {
            fields.insert(key.to_string(), number);
        }
    }
    fields
}

#[cfg(test)]
mod tests {
    use super::super::fs::test_doubles::MemoryFs;
    use super::*;
    use std::path::Path;

    const DIR: &str = "/sys/fs/cgroup/user.slice/user-1000.slice";

    #[test]
    fn parses_all_four_files() {
        let fs = MemoryFs::new()
            .with(&format!("{DIR}/memory.current"), "22235926528\n")
            .with(
                &format!("{DIR}/memory.stat"),
                "anon 12345\nfile 678\nshmem 90\n",
            )
            .with(
                &format!("{DIR}/memory.events"),
                "low 0\nhigh 3\nmax 1\noom 0\noom_kill 0\n",
            )
            .with(
                &format!("{DIR}/memory.pressure"),
                "some avg10=0.00 avg60=0.02 avg300=1.17 total=33645600\nfull avg10=0.00 avg60=0.02 avg300=1.16 total=33142024\n",
            );

        let snapshot = collect_cgroup(&fs, Path::new(DIR));
        assert_eq!(snapshot.memory.current, Some(22_235_926_528));
        assert_eq!(snapshot.memory.stat.get("anon"), Some(&12345));
        assert_eq!(snapshot.memory.events.get("oom_kill"), Some(&0));
        assert!(snapshot.memory.pressure.is_some());
    }

    #[test]
    fn partial_permissions_leave_the_rest_absent() {
        let fs = MemoryFs::new().with(&format!("{DIR}/memory.current"), "4096\n");
        let snapshot = collect_cgroup(&fs, Path::new(DIR));
        assert_eq!(snapshot.memory.current, Some(4096));
        assert!(snapshot.memory.stat.is_empty());
        assert!(snapshot.memory.events.is_empty());
        assert_eq!(snapshot.memory.pressure, None);
    }

    #[test]
    fn a_disappearing_group_reads_as_empty() {
        let fs = MemoryFs::new();
        let snapshot = collect_cgroup(&fs, Path::new(DIR));
        assert_eq!(snapshot.memory, CgroupMemory::default());
    }

    #[test]
    fn malformed_current_is_absent_not_zero() {
        let fs = MemoryFs::new().with(&format!("{DIR}/memory.current"), "not-a-number\n");
        let snapshot = collect_cgroup(&fs, Path::new(DIR));
        assert_eq!(snapshot.memory.current, None);
    }

    #[test]
    fn malformed_stat_lines_are_skipped() {
        let fs = MemoryFs::new().with(
            &format!("{DIR}/memory.stat"),
            "anon 100\nbroken\nfile notanumber\nshmem 5\n",
        );
        let snapshot = collect_cgroup(&fs, Path::new(DIR));
        assert_eq!(snapshot.memory.stat.get("anon"), Some(&100));
        assert_eq!(snapshot.memory.stat.get("shmem"), Some(&5));
        assert_eq!(snapshot.memory.stat.len(), 2);
    }
}
