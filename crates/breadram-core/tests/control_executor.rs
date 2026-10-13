//! Integration tests for the authorized memory controls.
//!
//! Every test drives the real control path: the real availability probe, the
//! real typed command rendering, and the real action coordinator. Only the
//! filesystem view and the process runner are substituted, so the assertions are
//! about the argument vectors that would have reached systemd and the values that
//! would have been written, not about a mock of the logic under test.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::Mutex;

use breadram_core::config::ActionKind;
use breadram_core::control::manager::{CommandOutput, CommandRunner, ControlError, UnitName};
use breadram_core::control::{
    discover_targets, probe_availability, ControlAvailability, ControlExecutor, ControlTargets,
    ManagerCommand, PROTECTION_FRACTION,
};
use breadram_core::coordinator::{
    ActionRequest, ActionState, Cancellation, Confirmation, Coordinator, Executor,
    MemoryObservation, OwnerToken, Precondition, RestoreOutcome,
};
use breadram_core::inventory::{ControlLevel, MemoryAccounting, Workload, WorkloadId};
use breadram_core::telemetry::fs::FileSource;

/// An in-memory filesystem, local to this test crate so the whole control path
/// can be exercised against a disposable cgroup tree.
#[derive(Debug, Default, Clone)]
struct MemoryFs {
    files: HashMap<String, String>,
}

impl MemoryFs {
    fn new() -> Self {
        Self::default()
    }

    fn with(mut self, path: &str, contents: &str) -> Self {
        self.files.insert(path.to_string(), contents.to_string());
        self
    }
}

impl FileSource for MemoryFs {
    fn read_to_string(&self, path: &Path) -> io::Result<String> {
        self.files
            .get(&path.display().to_string())
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "not in fixture"))
    }

    fn is_file(&self, path: &Path) -> bool {
        self.files.contains_key(&path.display().to_string())
    }

    fn list_dir(&self, path: &Path) -> io::Result<Vec<String>> {
        let prefix = format!("{}/", path.display());
        let mut names: Vec<String> = Vec::new();
        for key in self.files.keys() {
            if let Some(rest) = key.strip_prefix(&prefix) {
                let name = rest.split('/').next().unwrap_or(rest).to_string();
                if !names.contains(&name) {
                    names.push(name);
                }
            }
        }
        if names.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "no such directory in fixture",
            ));
        }
        names.sort();
        Ok(names)
    }
}

const GIB: u64 = 1024 * 1024 * 1024;
const APP_CGROUP: &str = "/user.slice/user-1000.slice/user@1000.service/app.slice/app.service";
const MEMORY_MAX: u64 = 8 * GIB;

fn stat_line(pid: u32, comm: &str, ppid: u32, starttime: u64) -> String {
    let mut after = vec!["0".to_string(); 20];
    after[0] = "S".to_string();
    after[1] = ppid.to_string();
    after[19] = starttime.to_string();
    format!("{pid} ({comm}) {}", after.join(" "))
}

/// A cgroup tree with the memory controller enabled for the app unit.
///
/// The parent slice carries the same 8 GiB cap as the unit, so the ancestor
/// clamp has a real parent to read: the unit's own file is never the ancestor.
fn host_fs() -> MemoryFs {
    MemoryFs::new()
        .with("/sys/fs/cgroup/cgroup.controllers", "cpu memory pids\n")
        .with(
            "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice/memory.max",
            &format!("{MEMORY_MAX}\n"),
        )
        .with(
            &format!("/sys/fs/cgroup{APP_CGROUP}/memory.max"),
            &format!("{MEMORY_MAX}\n"),
        )
        .with(
            &format!("/sys/fs/cgroup{APP_CGROUP}/memory.current"),
            &format!("{}\n", 6 * GIB),
        )
        .with("/proc/100/stat", &stat_line(100, "app", 1, 500))
        .with("/proc/100/cgroup", &format!("0::{APP_CGROUP}\n"))
}

/// A runner that records the commands it was asked to run and answers from a
/// script, so a test can assert exactly what would have reached systemd.
#[derive(Default)]
struct FakeRunner {
    commands: Mutex<Vec<ManagerCommand>>,
    properties: Mutex<HashMap<String, String>>,
    refusals: Mutex<Vec<String>>,
}

impl FakeRunner {
    fn with_property(self, property: &str, value: &str) -> Self {
        self.properties
            .lock()
            .map(|mut map| map.insert(property.to_string(), value.to_string()))
            .ok();
        self
    }

    fn commands(&self) -> Vec<ManagerCommand> {
        self.commands.lock().map(|c| c.clone()).unwrap_or_default()
    }

    /// The assignment from the most recent write, ignoring reads.
    fn last_set(&self) -> Option<String> {
        self.commands()
            .into_iter()
            .rev()
            .find_map(|command| match command {
                ManagerCommand::SetMemoryHigh { .. } | ManagerCommand::SetMemoryLow { .. } => {
                    command.argv().last().cloned()
                }
                _ => None,
            })
    }

    fn set_property(&self, property: &str, value: &str) {
        if let Ok(mut map) = self.properties.lock() {
            map.insert(property.to_string(), value.to_string());
        }
    }

    fn refuse(&self, substring: &str) {
        if let Ok(mut refusals) = self.refusals.lock() {
            refusals.push(substring.to_string());
        }
    }

    fn property(&self, name: &str) -> String {
        self.properties
            .lock()
            .map(|map| map.get(name).cloned().unwrap_or_default())
            .unwrap_or_default()
    }
}

impl CommandRunner for FakeRunner {
    fn run(&self, command: &ManagerCommand) -> Result<CommandOutput, ControlError> {
        if let Ok(mut commands) = self.commands.lock() {
            commands.push(command.clone());
        }

        let argv = command.argv();
        let last = argv.last().cloned().unwrap_or_default();

        if let Ok(refusals) = self.refusals.lock() {
            if refusals.iter().any(|needle| last.contains(needle)) {
                return Ok(CommandOutput::failed("Access denied"));
            }
        }

        match command {
            ManagerCommand::Show { property, .. } => {
                let value = self.property(property.as_str());
                if value.is_empty() {
                    // An unknown unit: the manager reports nothing and fails.
                    return Ok(CommandOutput::failed("Unit not found"));
                }
                Ok(CommandOutput::ok(value))
            }
            ManagerCommand::SetMemoryHigh { .. }
            | ManagerCommand::SetMemoryLow { .. }
            | ManagerCommand::ClearMemoryHigh { .. }
            | ManagerCommand::ClearMemoryLow { .. } => {
                if let Some((property, value)) = last.split_once('=') {
                    self.set_property(property, value);
                }
                Ok(CommandOutput::ok(""))
            }
        }
    }
}

fn app_targets(fs: &dyn FileSource) -> ControlTargets {
    discover_targets(fs)
}

fn app_request(limit_bytes: u64) -> ActionRequest {
    ActionRequest::new(
        WorkloadId {
            scope: "unit:app.service".to_string(),
            generation: 500,
        },
        ActionKind::CgroupSoftLimit,
        "the workload is over its budget",
        1_000,
    )
    .with_limit_bytes(limit_bytes)
    .with_expected_host_ram_delta(-(2 * GIB as i64))
}

fn workload() -> Workload {
    Workload {
        id: WorkloadId {
            scope: "unit:app.service".to_string(),
            generation: 500,
        },
        name: "app".to_string(),
        pids: vec![100],
        accounting: MemoryAccounting::Cgroup {
            current_bytes: Some(6 * GIB),
        },
        control: ControlLevel::Eligible,
        limit_bytes: None,
        ancestor_bytes: None,
    }
}

fn run(
    coordinator: &mut Coordinator,
    executor: &ControlExecutor<'_>,
    requests: &[ActionRequest],
    generation: u64,
) -> Vec<breadram_core::coordinator::ActionResult> {
    let workload = Workload {
        id: WorkloadId {
            scope: "unit:app.service".to_string(),
            generation,
        },
        ..workload()
    };
    coordinator.execute(requests, &[workload], executor, 2_000, &Cancellation::new())
}

#[test]
fn a_soft_limit_is_applied_through_the_user_manager() {
    let fs = host_fs();
    let runner = FakeRunner::default()
        .with_property("ControlGroup", APP_CGROUP)
        .with_property("ActiveState", "active")
        .with_property("MemoryHigh", "infinity");
    let targets = app_targets(&fs);
    let executor = ControlExecutor::new(&fs, &runner, &targets);
    let mut coordinator = Coordinator::new(OwnerToken("instance-1".to_string()), 16);

    let results = run(&mut coordinator, &executor, &[app_request(4 * GIB)], 500);
    assert_eq!(results[0].state, ActionState::Confirmed, "{results:?}");

    // The write went out as a typed set-property against the user manager.
    let set = runner
        .commands()
        .into_iter()
        .find(|command| matches!(command, ManagerCommand::SetMemoryHigh { .. }))
        .expect("a set-property command");
    let argv = set.argv();
    assert_eq!(argv[0], "systemctl");
    assert_eq!(argv[1], "--user");
    assert_eq!(argv[2], "--no-pager");
    assert_eq!(argv[3], "set-property");
    assert_eq!(argv[4], "app.service");
    assert_eq!(argv[5], format!("MemoryHigh={}", 4 * GIB));

    // And it was journalled with its original value, so it can be undone.
    let entry = coordinator
        .journal()
        .entries()
        .back()
        .expect("a journal entry");
    assert_eq!(entry.values.len(), 1);
    assert_eq!(entry.values[0].key, "systemd:app.service:MemoryHigh");
    assert_eq!(entry.values[0].original, "infinity");
    assert_eq!(entry.values[0].last_applied, (4 * GIB).to_string());
}

#[test]
fn the_measured_effect_comes_from_the_cgroup() {
    let fs = host_fs();
    let runner = FakeRunner::default()
        .with_property("ControlGroup", APP_CGROUP)
        .with_property("ActiveState", "active")
        .with_property("MemoryHigh", "infinity");
    let targets = app_targets(&fs);
    let executor = ControlExecutor::new(&fs, &runner, &targets);
    let mut coordinator = Coordinator::new(OwnerToken("instance-1".to_string()), 16);

    let results = run(&mut coordinator, &executor, &[app_request(4 * GIB)], 500);
    let effect = results[0].effect.as_ref().expect("an effect");
    assert_eq!(effect.requested_host_ram_delta, -(2 * GIB as i64));
    // memory.current is read before and after, so a throttled workload that has
    // not released anything yet reports no observed relief rather than a guess.
    assert_eq!(effect.observed_host_ram_delta, Some(0));
}

#[test]
fn an_unavailable_controller_means_observe_and_explain() {
    // No memory controller on the root, so nothing can be controlled.
    let fs = MemoryFs::new()
        .with("/sys/fs/cgroup/cgroup.controllers", "cpu pids\n")
        .with("/proc/100/stat", &stat_line(100, "app", 1, 500))
        .with("/proc/100/cgroup", &format!("0::{APP_CGROUP}\n"));
    let runner = FakeRunner::default()
        .with_property("ControlGroup", APP_CGROUP)
        .with_property("ActiveState", "active");
    let targets = app_targets(&fs);
    let executor = ControlExecutor::new(&fs, &runner, &targets);
    let mut coordinator = Coordinator::new(OwnerToken("instance-1".to_string()), 16);

    let results = run(&mut coordinator, &executor, &[app_request(4 * GIB)], 500);
    assert_eq!(results[0].state, ActionState::Unsupported);
    assert!(
        results[0].detail.contains("memory controller"),
        "the reason must be reported: {}",
        results[0].detail
    );
    assert!(
        runner.commands().is_empty(),
        "nothing may be sent when control is unavailable"
    );
}

#[test]
fn a_system_unit_is_refused() {
    // A system unit is discovered but never controllable: only the user manager
    // is ever addressed.
    let system_cgroup = "/system.slice/sshd.service";
    let fs = MemoryFs::new()
        .with("/sys/fs/cgroup/cgroup.controllers", "cpu memory\n")
        .with(
            &format!("/sys/fs/cgroup{system_cgroup}/memory.max"),
            "max\n",
        )
        .with("/proc/200/stat", &stat_line(200, "sshd", 1, 100))
        .with("/proc/200/cgroup", &format!("0::{system_cgroup}\n"));
    let runner = FakeRunner::default();
    let targets = app_targets(&fs);
    assert!(targets.is_empty(), "a system unit is not a target");

    // Even if a caller names it, it is not controllable.
    let targets = ControlTargets::new();
    let executor = ControlExecutor::new(&fs, &runner, &targets);
    let request = ActionRequest::new(
        WorkloadId {
            scope: "unit:sshd.service".to_string(),
            generation: 100,
        },
        ActionKind::CgroupSoftLimit,
        "test",
        1_000,
    )
    .with_limit_bytes(GIB);
    assert!(matches!(
        executor.precondition(&request),
        Precondition::Unsupported { .. }
    ));
}

#[test]
fn a_denied_control_is_reported_as_denied() {
    let fs = host_fs();
    let runner = FakeRunner::default()
        .with_property("ControlGroup", APP_CGROUP)
        .with_property("ActiveState", "active")
        .with_property("MemoryHigh", "infinity");
    runner.refuse("MemoryHigh");
    let targets = app_targets(&fs);
    let executor = ControlExecutor::new(&fs, &runner, &targets);
    let mut coordinator = Coordinator::new(OwnerToken("instance-1".to_string()), 16);

    let results = run(&mut coordinator, &executor, &[app_request(4 * GIB)], 500);
    assert_eq!(results[0].state, ActionState::Denied);
    assert!(results[0].detail.contains("Access denied"));
}

#[test]
fn a_vanished_target_is_reported_as_superseded() {
    let fs = host_fs();
    // The manager no longer knows the unit at all.
    let runner = FakeRunner::default();
    let targets = app_targets(&fs);
    let executor = ControlExecutor::new(&fs, &runner, &targets);
    let mut coordinator = Coordinator::new(OwnerToken("instance-1".to_string()), 16);

    let results = run(&mut coordinator, &executor, &[app_request(4 * GIB)], 500);
    assert_eq!(results[0].state, ActionState::Superseded);
}

#[test]
fn a_target_that_moved_to_another_cgroup_is_refused() {
    let fs = host_fs();
    let runner = FakeRunner::default().with_property(
        "ControlGroup",
        "/user.slice/user-1000.slice/user@1000.service/other.slice/app.service",
    );
    runner.set_property("ActiveState", "active");
    let targets = app_targets(&fs);
    let executor = ControlExecutor::new(&fs, &runner, &targets);
    let mut coordinator = Coordinator::new(OwnerToken("instance-1".to_string()), 16);

    let results = run(&mut coordinator, &executor, &[app_request(4 * GIB)], 500);
    assert_eq!(results[0].state, ActionState::Superseded);
    assert!(results[0].detail.contains("now runs in"));
    assert!(
        !runner
            .commands()
            .iter()
            .any(|command| matches!(command, ManagerCommand::SetMemoryHigh { .. })),
        "no write may go out for a moved target"
    );
}

#[test]
fn a_restarted_unit_is_detected_as_a_new_generation() {
    let fs = host_fs();
    let runner = FakeRunner::default()
        .with_property("ControlGroup", APP_CGROUP)
        .with_property("ActiveState", "active")
        .with_property("MemoryHigh", "infinity");
    let targets = app_targets(&fs);
    let executor = ControlExecutor::new(&fs, &runner, &targets);
    let mut coordinator = Coordinator::new(OwnerToken("instance-1".to_string()), 16);

    // Planned against generation 500, but generation 900 is live now.
    let results = run(&mut coordinator, &executor, &[app_request(4 * GIB)], 900);
    assert_eq!(results[0].state, ActionState::Superseded);
}

#[test]
fn a_requested_limit_above_the_ancestor_limit_is_clamped_and_reported() {
    let fs = host_fs();
    let runner = FakeRunner::default()
        .with_property("ControlGroup", APP_CGROUP)
        .with_property("ActiveState", "active")
        .with_property("MemoryHigh", "infinity");
    let targets = app_targets(&fs);
    let executor = ControlExecutor::new(&fs, &runner, &targets);
    let mut coordinator = Coordinator::new(OwnerToken("instance-1".to_string()), 16);

    // Ask for 32 GiB inside an 8 GiB ancestor limit.
    let results = run(&mut coordinator, &executor, &[app_request(32 * GIB)], 500);
    assert_eq!(results[0].state, ActionState::Confirmed);
    assert!(
        results[0].detail.contains("clamped"),
        "the clamp must be reported: {}",
        results[0].detail
    );
    assert_eq!(
        runner.last_set().as_deref(),
        Some(format!("MemoryHigh={MEMORY_MAX}").as_str())
    );
}

#[test]
fn a_sub_megabyte_limit_is_refused() {
    let fs = host_fs();
    let runner = FakeRunner::default()
        .with_property("ControlGroup", APP_CGROUP)
        .with_property("ActiveState", "active");
    let targets = app_targets(&fs);
    let executor = ControlExecutor::new(&fs, &runner, &targets);
    let mut coordinator = Coordinator::new(OwnerToken("instance-1".to_string()), 16);

    let results = run(&mut coordinator, &executor, &[app_request(1024)], 500);
    assert_eq!(results[0].state, ActionState::Denied);
    assert!(results[0].detail.contains("minimum"));
}

#[test]
fn reclaim_protection_is_bounded_and_described_as_best_effort() {
    let fs = host_fs();
    let runner = FakeRunner::default()
        .with_property("ControlGroup", APP_CGROUP)
        .with_property("ActiveState", "active")
        .with_property("MemoryLow", "0");
    let targets = app_targets(&fs);
    let executor = ControlExecutor::new(&fs, &runner, &targets);
    let mut coordinator = Coordinator::new(OwnerToken("instance-1".to_string()), 16);

    // Ask for the whole ancestor limit as protection.
    let request = ActionRequest::new(
        WorkloadId {
            scope: "unit:app.service".to_string(),
            generation: 500,
        },
        ActionKind::ReclaimProtection,
        "shield the interactive workload",
        1_000,
    )
    .with_limit_bytes(MEMORY_MAX);
    let results = run(&mut coordinator, &executor, &[request], 500);

    assert_eq!(results[0].state, ActionState::Confirmed);
    assert!(
        results[0].detail.contains("best effort"),
        "protection must be described as best effort: {}",
        results[0].detail
    );
    let expected = ((MEMORY_MAX as f64) * PROTECTION_FRACTION) as u64;
    assert_eq!(
        runner.last_set().as_deref(),
        Some(format!("MemoryLow={expected}").as_str())
    );
}

#[test]
fn a_rollback_restores_the_original_through_the_manager() {
    let fs = host_fs();
    let runner = FakeRunner::default()
        .with_property("ControlGroup", APP_CGROUP)
        .with_property("ActiveState", "active")
        .with_property("MemoryHigh", "infinity");
    let targets = app_targets(&fs);
    let executor = ControlExecutor::new(&fs, &runner, &targets);
    let mut coordinator = Coordinator::new(OwnerToken("instance-1".to_string()), 16);

    let results = run(&mut coordinator, &executor, &[app_request(4 * GIB)], 500);
    assert_eq!(results[0].state, ActionState::Confirmed);
    assert_eq!(runner.property("MemoryHigh"), (4 * GIB).to_string());

    // A restart, then recovery: the original neutral value goes back.
    let mut restarted = Coordinator::resume(
        OwnerToken("instance-2".to_string()),
        coordinator.journal().clone(),
    );
    let restored = restarted.recover(&executor, 9_000);
    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0].outcome, RestoreOutcome::Restored);
    assert_eq!(runner.property("MemoryHigh"), "infinity");
    assert!(runner
        .commands()
        .iter()
        .any(|command| matches!(command, ManagerCommand::ClearMemoryHigh { .. })));
}

#[test]
fn a_rollback_leaves_a_newer_external_value_alone() {
    let fs = host_fs();
    let runner = FakeRunner::default()
        .with_property("ControlGroup", APP_CGROUP)
        .with_property("ActiveState", "active")
        .with_property("MemoryHigh", "infinity");
    let targets = app_targets(&fs);
    let executor = ControlExecutor::new(&fs, &runner, &targets);
    let mut coordinator = Coordinator::new(OwnerToken("instance-1".to_string()), 16);

    run(&mut coordinator, &executor, &[app_request(4 * GIB)], 500);

    // Someone else changes the setting after breadram.
    runner.set_property("MemoryHigh", &(8 * GIB).to_string());

    let restored = coordinator.recover(&executor, 9_000);
    assert!(matches!(
        restored[0].outcome,
        RestoreOutcome::SkippedForeignChange { .. }
    ));
    assert_eq!(
        runner.property("MemoryHigh"),
        (8 * GIB).to_string(),
        "the newer configuration must be left alone"
    );
}

#[test]
fn availability_is_reported_with_a_reason_for_every_prerequisite() {
    let fs = host_fs();
    let targets = app_targets(&fs);
    let target = targets.get("unit:app.service").expect("the app target");
    let availability = probe_availability(&fs, target);
    assert_eq!(
        availability,
        ControlAvailability {
            cgroup_v2: breadram_core::capability::CapabilityState::Available,
            memory_controller: breadram_core::capability::CapabilityState::Available,
            user_manager: breadram_core::capability::CapabilityState::Available,
            effective_max_bytes: Some(MEMORY_MAX),
        }
    );
    assert!(availability.can_control());
}

#[test]
fn the_executor_reports_memory_from_the_cgroup() {
    let fs = host_fs();
    let runner = FakeRunner::default();
    let targets = app_targets(&fs);
    let executor = ControlExecutor::new(&fs, &runner, &targets);
    let observation = executor.observe(&WorkloadId {
        scope: "unit:app.service".to_string(),
        generation: 500,
    });
    assert_eq!(observation.host_ram_bytes, Some(6 * GIB));
    assert_eq!(observation.device_ram_bytes, None);
    assert_eq!(
        observation,
        MemoryObservation {
            host_ram_bytes: Some(6 * GIB),
            device_ram_bytes: None,
        }
    );
}

#[test]
fn a_written_value_is_confirmed_from_the_manager() {
    let fs = host_fs();
    let runner = FakeRunner::default()
        .with_property("ControlGroup", APP_CGROUP)
        .with_property("ActiveState", "active")
        .with_property("MemoryHigh", "infinity");
    let targets = app_targets(&fs);
    let executor = ControlExecutor::new(&fs, &runner, &targets);
    let request = app_request(4 * GIB);

    assert!(matches!(
        executor.precondition(&request),
        Precondition::Ready
    ));
    let change = executor.apply(&request).expect("applied");
    assert_eq!(change.values.len(), 1);
    assert_eq!(change.note, None);
    assert_eq!(executor.confirm(&request), Confirmation::Confirmed);

    // If the setting is changed underneath, confirmation reports it honestly.
    runner.set_property("MemoryHigh", "1073741824");
    assert!(matches!(
        executor.confirm(&request),
        Confirmation::NotYet { .. }
    ));
}

#[test]
fn an_unknown_unit_reads_as_gone_rather_than_failing() {
    let fs = host_fs();
    let runner = FakeRunner::default();
    let targets = app_targets(&fs);
    let executor = ControlExecutor::new(&fs, &runner, &targets);
    assert_eq!(
        executor.read_value("systemd:app.service:MemoryHigh"),
        Ok(None)
    );
}

#[test]
fn a_unit_name_cannot_smuggle_an_option() {
    // The type is the guard: a name that could be read as a flag never becomes a
    // command in the first place.
    assert!(UnitName::parse("--help").is_err());
    assert!(UnitName::parse("-H.service").is_err());
}
