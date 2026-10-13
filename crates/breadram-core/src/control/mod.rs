//! Authorized, reversible memory controls.
//!
//! breadram changes memory settings through the manager that owns the cgroup,
//! never by rewriting a systemd-owned hierarchy itself. Control is offered only
//! where it is actually authorized, available, and still aimed at the same
//! workload, and where it is not, the reason is reported rather than a silent
//! no-op.
//!
//! What is offered, precisely:
//!
//! - A **bounded soft limit** (`MemoryHigh`). This throttles and reclaims; it
//!   reserves nothing. It is not a hard cap and it is never described as one.
//! - **Reclaim protection**, bounded and best effort (`MemoryLow`). It biases
//!   reclaim away from a workload; it does not guarantee the memory stays.
//!
//! Both are reversible: the original value is read before the write, recorded by
//! the action coordinator, and restored only while breadram still owns the
//! setting.

pub mod executor;
pub mod manager;

pub use executor::{ControlExecutor, PROTECTION_FRACTION};
pub use manager::{
    capability_from, parse_bytes, setting_key, CommandOutput, CommandRunner, ControlError,
    ManagerCommand, ProcessRunner, UnitName, UnitProperty, MAX_REQUESTED_BYTES,
    MIN_REQUESTED_BYTES,
};

use std::collections::BTreeMap;
use std::path::Path;

use crate::capability::CapabilityState;
use crate::inventory::parse::{parse_cgroup_path, parse_proc_stat, systemd_unit};
use crate::inventory::PROC_ROOT;
use crate::telemetry::fs::FileSource;

/// A unit breadram is allowed to control, and the identity to verify at write
/// time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlTarget {
    /// The workload scope key, for example `unit:app.service`.
    pub scope: String,
    /// The validated unit name.
    pub unit: UnitName,
    /// The cgroup path observed when the target was discovered.
    pub control_group: String,
    /// The generation observed when the target was discovered.
    pub generation: u64,
    /// Whether the unit belongs to the user manager.
    pub user_manager: bool,
}

/// The units breadram may control, keyed by workload scope.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ControlTargets {
    targets: BTreeMap<String, ControlTarget>,
}

impl ControlTargets {
    /// An empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add or replace a target.
    pub fn insert(&mut self, target: ControlTarget) {
        self.targets.insert(target.scope.clone(), target);
    }

    /// The target for a scope.
    pub fn get(&self, scope: &str) -> Option<&ControlTarget> {
        self.targets.get(scope)
    }

    /// How many targets there are.
    pub fn len(&self) -> usize {
        self.targets.len()
    }

    /// Whether there are none.
    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    /// The targets.
    pub fn iter(&self) -> impl Iterator<Item = &ControlTarget> {
        self.targets.values()
    }
}

/// Whether a cgroup path belongs to the user manager.
///
/// Only the user manager is ever addressed, so a control cannot be aimed at a
/// system unit. A path that is not canonical is refused as well: the kernel
/// reports canonical paths, so a `..` segment means something is wrong rather
/// than merely unusual, and it must not be able to walk out of the user slice.
pub fn is_user_unit(control_group: &str) -> bool {
    if !control_group.starts_with("/user.slice/") {
        return false;
    }
    !control_group
        .split('/')
        .any(|segment| segment == "." || segment == "..")
}

/// Discover the user units visible in procfs.
///
/// The generation is the earliest member start time, matching the inventory, so
/// an action planned against a workload can be checked against the live unit.
pub fn discover_targets(fs: &dyn FileSource) -> ControlTargets {
    let mut targets = ControlTargets::new();
    let Ok(entries) = fs.list_dir(Path::new(PROC_ROOT)) else {
        return targets;
    };

    // unit name -> (earliest generation, cgroup path)
    let mut found: BTreeMap<String, (u64, String)> = BTreeMap::new();
    for entry in entries {
        let Ok(pid) = entry.parse::<u32>() else {
            continue;
        };
        let dir = Path::new(PROC_ROOT).join(&entry);
        let Some(control_group) = fs
            .read_to_string(&dir.join("cgroup"))
            .ok()
            .and_then(|text| parse_cgroup_path(&text))
        else {
            continue;
        };
        if !is_user_unit(&control_group) {
            continue;
        }
        let Some(unit) = systemd_unit(&control_group) else {
            continue;
        };
        let generation = fs
            .read_to_string(&dir.join("stat"))
            .ok()
            .and_then(|text| parse_proc_stat(&text).ok())
            .filter(|stat| stat.pid == pid)
            .map_or(0, |stat| stat.starttime);

        let slot = found
            .entry(unit)
            .or_insert_with(|| (generation, control_group.clone()));
        slot.0 = slot.0.min(generation);
    }

    for (name, (generation, control_group)) in found {
        let Ok(unit) = UnitName::parse(&name) else {
            continue;
        };
        targets.insert(ControlTarget {
            scope: format!("unit:{name}"),
            unit,
            control_group,
            generation,
            user_manager: true,
        });
    }
    targets
}

/// What breadram can actually control for one target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlAvailability {
    /// Whether the cgroup v2 unified hierarchy is mounted.
    pub cgroup_v2: CapabilityState,
    /// Whether the memory controller is enabled for the target's subtree.
    pub memory_controller: CapabilityState,
    /// Whether the target belongs to the user manager.
    pub user_manager: CapabilityState,
    /// The effective limit inherited from an ancestor, when there is one.
    pub effective_max_bytes: Option<u64>,
}

impl ControlAvailability {
    /// Whether every prerequisite is available.
    pub fn can_control(&self) -> bool {
        self.cgroup_v2.is_usable()
            && self.memory_controller.is_usable()
            && self.user_manager.is_usable()
    }

    /// The first reason control is unavailable, for reporting.
    pub fn reason(&self) -> Option<String> {
        for state in [&self.cgroup_v2, &self.memory_controller, &self.user_manager] {
            if let Some(reason) = state.reason() {
                return Some(reason.to_string());
            }
        }
        None
    }
}

/// Probe what can be controlled for a target.
pub fn probe_availability(fs: &dyn FileSource, target: &ControlTarget) -> ControlAvailability {
    let root = Path::new(crate::telemetry::snapshot::CGROUP_V2_ROOT);
    let cgroup_v2 = match fs.read_to_string(&root.join("cgroup.controllers")) {
        Ok(text) if text.split_whitespace().any(|name| name == "memory") => {
            CapabilityState::Available
        }
        Ok(_) => CapabilityState::Unavailable {
            reason: "the memory controller is not available on the root cgroup".to_string(),
        },
        Err(error) => CapabilityState::Unavailable {
            reason: format!("cannot read the cgroup v2 hierarchy: {error}"),
        },
    };

    let relative = target.control_group.trim_start_matches('/');
    let unit_path = root.join(relative);
    let memory_max = fs.read_to_string(&unit_path.join("memory.max")).ok();
    let memory_controller = match &memory_max {
        Some(_) => CapabilityState::Available,
        None => CapabilityState::Unavailable {
            reason: format!(
                "the memory controller is not enabled for {}",
                target.control_group
            ),
        },
    };
    // The unit's own cap is what a soft limit writes, so it is not the
    // ancestor: the effective limit is the tightest parent cap, and the plan
    // already carries the same number from the inventory.
    let effective_max_bytes = crate::inventory::ancestor_limit(fs, &target.control_group);

    let user_manager = if target.user_manager {
        CapabilityState::Available
    } else {
        CapabilityState::Unavailable {
            reason: "breadram controls user units only, this unit belongs to the system manager"
                .to_string(),
        }
    };

    ControlAvailability {
        cgroup_v2,
        memory_controller,
        user_manager,
        effective_max_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::fs::test_doubles::MemoryFs;

    fn stat_line(pid: u32, comm: &str, ppid: u32, starttime: u64) -> String {
        let mut after = vec!["0".to_string(); 20];
        after[0] = "S".to_string();
        after[1] = ppid.to_string();
        after[19] = starttime.to_string();
        format!("{pid} ({comm}) {}", after.join(" "))
    }

    const APP_CGROUP: &str = "/user.slice/user-1000.slice/user@1000.service/app.slice/app.service";
    const SYSTEM_CGROUP: &str = "/system.slice/sshd.service";

    #[test]
    fn discovery_finds_user_units_only() {
        let fs = MemoryFs::new()
            .with("/proc/100/stat", &stat_line(100, "app", 1, 500))
            .with("/proc/100/cgroup", &format!("0::{APP_CGROUP}\n"))
            .with("/proc/200/stat", &stat_line(200, "sshd", 1, 100))
            .with("/proc/200/cgroup", &format!("0::{SYSTEM_CGROUP}\n"));

        let targets = discover_targets(&fs);
        assert_eq!(targets.len(), 1);
        let target = targets.get("unit:app.service").expect("the user unit");
        assert_eq!(target.unit.as_str(), "app.service");
        assert_eq!(target.control_group, APP_CGROUP);
        assert_eq!(target.generation, 500);
        assert!(target.user_manager);
        assert!(targets.get("unit:sshd.service").is_none());
    }

    #[test]
    fn discovery_takes_the_earliest_generation() {
        let fs = MemoryFs::new()
            .with("/proc/100/stat", &stat_line(100, "app", 1, 900))
            .with("/proc/100/cgroup", &format!("0::{APP_CGROUP}\n"))
            .with("/proc/101/stat", &stat_line(101, "app", 100, 500))
            .with("/proc/101/cgroup", &format!("0::{APP_CGROUP}\n"));

        let targets = discover_targets(&fs);
        assert_eq!(
            targets.get("unit:app.service").expect("target").generation,
            500
        );
    }

    #[test]
    fn a_non_canonical_cgroup_path_is_not_a_user_unit() {
        // The prefix must not be usable to walk out of the user slice.
        assert!(!is_user_unit("/user.slice/../../system.slice/x.service"));
        assert!(!is_user_unit("/user.slice/./x/../x.service"));
        assert!(!is_user_unit("/system.slice/docker-abc.scope"));
        assert!(is_user_unit(APP_CGROUP));
    }

    #[test]
    fn availability_explains_a_missing_memory_controller() {
        let target = ControlTarget {
            scope: "unit:app.service".to_string(),
            unit: UnitName::parse("app.service").expect("valid"),
            control_group: APP_CGROUP.to_string(),
            generation: 1,
            user_manager: true,
        };
        let fs = MemoryFs::new().with("/sys/fs/cgroup/cgroup.controllers", "cpu io\n");
        let availability = probe_availability(&fs, &target);
        assert!(!availability.can_control());
        assert!(availability
            .reason()
            .is_some_and(|reason| reason.contains("memory controller")));
    }

    #[test]
    fn availability_reads_the_effective_ancestor_limit() {
        let target = ControlTarget {
            scope: "unit:app.service".to_string(),
            unit: UnitName::parse("app.service").expect("valid"),
            control_group: APP_CGROUP.to_string(),
            generation: 1,
            user_manager: true,
        };
        let parent = "/user.slice/user-1000.slice/user@1000.service/app.slice";
        let grandparent = "/user.slice/user-1000.slice/user@1000.service";
        let fs = MemoryFs::new()
            .with("/sys/fs/cgroup/cgroup.controllers", "cpu memory\n")
            .with(
                &format!("/sys/fs/cgroup{APP_CGROUP}/memory.max"),
                "8589934592\n",
            )
            .with(
                &format!("/sys/fs/cgroup{parent}/memory.max"),
                "4294967296\n",
            )
            .with(&format!("/sys/fs/cgroup{grandparent}/memory.max"), "max\n");
        let availability = probe_availability(&fs, &target);
        assert!(availability.can_control());
        assert_eq!(
            availability.effective_max_bytes,
            Some(4_294_967_296),
            "the tightest parent wins, and the unit's own cap is not an ancestor"
        );
        assert_eq!(availability.reason(), None);
    }

    #[test]
    fn availability_with_unlimited_parents_has_no_ancestor_limit() {
        let target = ControlTarget {
            scope: "unit:app.service".to_string(),
            unit: UnitName::parse("app.service").expect("valid"),
            control_group: APP_CGROUP.to_string(),
            generation: 1,
            user_manager: true,
        };
        let fs = MemoryFs::new()
            .with("/sys/fs/cgroup/cgroup.controllers", "cpu memory\n")
            .with(
                &format!("/sys/fs/cgroup{APP_CGROUP}/memory.max"),
                "8589934592\n",
            );
        let availability = probe_availability(&fs, &target);
        assert!(availability.can_control());
        assert_eq!(
            availability.effective_max_bytes, None,
            "an unreadable parent constrains nothing"
        );
    }

    #[test]
    fn availability_explains_a_system_unit() {
        let target = ControlTarget {
            scope: "unit:sshd.service".to_string(),
            unit: UnitName::parse("sshd.service").expect("valid"),
            control_group: SYSTEM_CGROUP.to_string(),
            generation: 1,
            user_manager: false,
        };
        let fs = MemoryFs::new()
            .with("/sys/fs/cgroup/cgroup.controllers", "cpu memory\n")
            .with(
                &format!("/sys/fs/cgroup{SYSTEM_CGROUP}/memory.max"),
                "max\n",
            );
        let availability = probe_availability(&fs, &target);
        assert!(!availability.can_control());
        assert!(availability
            .reason()
            .is_some_and(|reason| reason.contains("user units only")));
    }
}
