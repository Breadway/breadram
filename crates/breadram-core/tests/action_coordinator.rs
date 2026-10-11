//! Failure injection for the action coordinator.
//!
//! A fake executor scripts every failure mode the coordinator must report
//! honestly: a partial write, a lost connection, a timeout, a refusal, a busy
//! target, a vanished target, and a target that already has a confirmed effect.
//! Recovery is exercised across a simulated restart, with a conflicting external
//! change in the middle.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use breadram_core::config::ActionKind;
use breadram_core::coordinator::{
    ActionRequest, ActionState, AppliedChange, Cancellation, Confirmation, Coordinator, Executor,
    ExecutorError, MemoryObservation, OwnedValue, OwnerToken, Precondition, RestoreOutcome,
};
use breadram_core::inventory::{ControlLevel, MemoryAccounting, Workload, WorkloadId};

const KEY: &str = "cgroup:unit:app.service/memory.high";
const GIB: u64 = 1024 * 1024 * 1024;

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn workload(scope: &str, generation: u64, name: &str) -> Workload {
    Workload {
        id: WorkloadId {
            scope: scope.to_string(),
            generation,
        },
        name: name.to_string(),
        pids: vec![100],
        accounting: MemoryAccounting::Cgroup {
            current_bytes: Some(4 * GIB),
        },
        control: ControlLevel::Eligible,
    }
}

fn request(scope: &str, generation: u64) -> ActionRequest {
    ActionRequest::new(
        WorkloadId {
            scope: scope.to_string(),
            generation,
        },
        ActionKind::CgroupSoftLimit,
        "the workload is over its budget",
        1_000,
    )
    .with_expected_host_ram_delta(-(2 * GIB as i64))
}

#[derive(Default)]
struct FakeExecutor {
    supported: Mutex<Vec<ActionKind>>,
    precondition: Mutex<Option<Precondition>>,
    applies: Mutex<VecDeque<Result<AppliedChange, ExecutorError>>>,
    confirmations: Mutex<VecDeque<Confirmation>>,
    observations: Mutex<HashMap<String, MemoryObservation>>,
    after_observation: Mutex<Option<MemoryObservation>>,
    values: Mutex<HashMap<String, String>>,
    writes: Mutex<Vec<(String, String)>>,
    apply_calls: Mutex<usize>,
}

impl FakeExecutor {
    fn with(kinds: &[ActionKind], initial_host_ram: u64, after_host_ram: u64) -> Self {
        let executor = Self {
            supported: Mutex::new(kinds.to_vec()),
            ..Self::default()
        };
        lock(&executor.observations).insert(
            "unit:app.service".to_string(),
            MemoryObservation {
                host_ram_bytes: Some(initial_host_ram),
                device_ram_bytes: Some(8 * GIB),
            },
        );
        *lock(&executor.after_observation) = Some(MemoryObservation {
            host_ram_bytes: Some(after_host_ram),
            device_ram_bytes: Some(6 * GIB),
        });
        executor
    }

    fn script_apply(self, outcome: Result<AppliedChange, ExecutorError>) -> Self {
        lock(&self.applies).push_back(outcome);
        self
    }

    fn script_confirmation(self, confirmation: Confirmation) -> Self {
        lock(&self.confirmations).push_back(confirmation);
        self
    }

    fn set_precondition(&self, precondition: Precondition) {
        *lock(&self.precondition) = Some(precondition);
    }

    fn revoke_support(&self, kind: ActionKind) {
        lock(&self.supported).retain(|candidate| *candidate != kind);
    }

    fn set_value(&self, key: &str, value: &str) {
        lock(&self.values).insert(key.to_string(), value.to_string());
    }

    fn value(&self, key: &str) -> Option<String> {
        lock(&self.values).get(key).cloned()
    }

    fn apply_count(&self) -> usize {
        *lock(&self.apply_calls)
    }

    fn write_count(&self) -> usize {
        lock(&self.writes).len()
    }
}

fn owned(key: &str, original: &str, last_applied: &str) -> OwnedValue {
    OwnedValue {
        key: key.to_string(),
        original: original.to_string(),
        last_applied: last_applied.to_string(),
        owner: OwnerToken("instance-1".to_string()),
    }
}

impl Executor for FakeExecutor {
    fn supports(&self, kind: ActionKind) -> bool {
        lock(&self.supported).contains(&kind)
    }

    fn precondition(&self, _request: &ActionRequest) -> Precondition {
        lock(&self.precondition)
            .clone()
            .unwrap_or(Precondition::Ready)
    }

    fn apply(&self, _request: &ActionRequest) -> Result<AppliedChange, ExecutorError> {
        *lock(&self.apply_calls) += 1;
        let outcome = lock(&self.applies)
            .pop_front()
            .unwrap_or_else(|| Ok(AppliedChange::default()));
        if outcome.is_ok() {
            if let Some(after) = *lock(&self.after_observation) {
                lock(&self.observations).insert("unit:app.service".to_string(), after);
            }
            if let Ok(change) = &outcome {
                for value in &change.values {
                    self.set_value(&value.key, &value.last_applied);
                }
            }
        }
        outcome
    }

    fn confirm(&self, _request: &ActionRequest) -> Confirmation {
        lock(&self.confirmations)
            .pop_front()
            .unwrap_or(Confirmation::Confirmed)
    }

    fn observe(&self, target: &WorkloadId) -> MemoryObservation {
        lock(&self.observations)
            .get(&target.scope)
            .copied()
            .unwrap_or_default()
    }

    fn read_value(&self, key: &str) -> Result<Option<String>, ExecutorError> {
        Ok(self.value(key))
    }

    fn write_value(&self, key: &str, value: &str) -> Result<(), ExecutorError> {
        lock(&self.writes).push((key.to_string(), value.to_string()));
        self.set_value(key, value);
        Ok(())
    }
}

fn coordinator() -> Coordinator {
    Coordinator::new(OwnerToken("instance-1".to_string()), 64)
}

fn run(
    coordinator: &mut Coordinator,
    executor: &FakeExecutor,
    requests: &[ActionRequest],
) -> Vec<breadram_core::coordinator::ActionResult> {
    let workloads = vec![workload("unit:app.service", 1, "app")];
    coordinator.execute(requests, &workloads, executor, 2_000, &Cancellation::new())
}

#[test]
fn a_confirmed_action_records_the_requested_and_observed_effect() {
    let executor = FakeExecutor::with(&[ActionKind::CgroupSoftLimit], 4 * GIB, 2 * GIB)
        .script_apply(Ok(AppliedChange {
            values: vec![owned(KEY, "4294967296", "2147483648")],
        }));
    let mut coordinator = coordinator();
    let results = run(
        &mut coordinator,
        &executor,
        &[request("unit:app.service", 1)],
    );

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].state, ActionState::Confirmed);
    let effect = results[0].effect.as_ref().expect("an effect");
    assert_eq!(effect.requested_host_ram_delta, -(2 * GIB as i64));
    assert_eq!(effect.observed_host_ram_delta, Some(-(2 * GIB as i64)));
    assert_eq!(effect.observed_device_ram_delta, Some(-(2 * GIB as i64)));
    assert_eq!(coordinator.journal().len(), 1);
}

#[test]
fn a_duplicate_request_is_idempotent() {
    let executor = FakeExecutor::with(&[ActionKind::CgroupSoftLimit], 4 * GIB, 2 * GIB);
    let mut coordinator = coordinator();
    let requests = [
        request("unit:app.service", 1),
        request("unit:app.service", 1),
    ];
    let results = run(&mut coordinator, &executor, &requests);

    assert_eq!(results[0].state, ActionState::Confirmed);
    assert_eq!(results[1].state, ActionState::SkippedDuplicate);
    assert_eq!(executor.apply_count(), 1, "the effect happens once");
}

#[test]
fn a_failure_does_not_block_a_retry() {
    let executor = FakeExecutor::with(&[ActionKind::CgroupSoftLimit], 4 * GIB, 2 * GIB)
        .script_apply(Err(ExecutorError::Failed("transient".to_string())));
    let mut coordinator = coordinator();
    let requests = [
        request("unit:app.service", 1),
        request("unit:app.service", 1),
    ];
    let results = run(&mut coordinator, &executor, &requests);

    assert_eq!(results[0].state, ActionState::Failed);
    assert_eq!(
        results[1].state,
        ActionState::Confirmed,
        "a retry must be allowed after a failure"
    );
    assert_eq!(executor.apply_count(), 2);
}

#[test]
fn a_busy_target_is_reported_and_stays_retryable() {
    let executor = FakeExecutor::with(&[ActionKind::CgroupSoftLimit], 4 * GIB, 2 * GIB);
    executor.set_precondition(Precondition::Busy {
        reason: "the workload is restarting".to_string(),
    });
    let mut coordinator = coordinator();
    let requests = [request("unit:app.service", 1)];
    let first = run(&mut coordinator, &executor, &requests);
    assert_eq!(first[0].state, ActionState::Busy);
    assert_eq!(first[0].detail, "the workload is restarting");
    assert_eq!(executor.apply_count(), 0, "nothing was applied");

    executor.set_precondition(Precondition::Ready);
    let second = run(&mut coordinator, &executor, &requests);
    assert_eq!(second[0].state, ActionState::Confirmed);
}

#[test]
fn unsupported_denied_and_timed_out_are_distinct() {
    // No executor supports it.
    let executor = FakeExecutor::with(&[], 4 * GIB, 4 * GIB);
    let mut coordinator = coordinator();
    let results = run(
        &mut coordinator,
        &executor,
        &[request("unit:app.service", 1)],
    );
    assert_eq!(results[0].state, ActionState::Unsupported);
    assert_eq!(executor.apply_count(), 0);

    // Supported, but the precondition says no.
    let executor = FakeExecutor::with(&[ActionKind::CgroupSoftLimit], 4 * GIB, 4 * GIB);
    executor.set_precondition(Precondition::Denied {
        reason: "not permitted in this mode".to_string(),
    });
    let results = run(
        &mut coordinator,
        &executor,
        &[request("unit:app.service", 1)],
    );
    assert_eq!(results[0].state, ActionState::Denied);

    // The executor times out mid change.
    let executor = FakeExecutor::with(&[ActionKind::CgroupSoftLimit], 4 * GIB, 4 * GIB)
        .script_apply(Err(ExecutorError::Timeout("no response".to_string())));
    let results = run(
        &mut coordinator,
        &executor,
        &[request("unit:app.service", 1)],
    );
    assert_eq!(results[0].state, ActionState::TimedOut);
}

#[test]
fn an_expired_deadline_stops_before_anything_runs() {
    let executor = FakeExecutor::with(&[ActionKind::CgroupSoftLimit], 4 * GIB, 4 * GIB);
    let mut coordinator = coordinator();
    let expired = request("unit:app.service", 1).with_deadline(1_000);
    let workloads = vec![workload("unit:app.service", 1, "app")];
    let results = coordinator.execute(
        &[expired],
        &workloads,
        &executor,
        2_000,
        &Cancellation::new(),
    );
    assert_eq!(results[0].state, ActionState::TimedOut);
    assert_eq!(executor.apply_count(), 0);
}

#[test]
fn a_cancelled_batch_stops_before_applying() {
    let executor = FakeExecutor::with(&[ActionKind::CgroupSoftLimit], 4 * GIB, 4 * GIB);
    let mut coordinator = coordinator();
    let cancel = Cancellation::new();
    cancel.cancel();
    let workloads = vec![workload("unit:app.service", 1, "app")];
    let results = coordinator.execute(
        &[request("unit:app.service", 1)],
        &workloads,
        &executor,
        2_000,
        &cancel,
    );
    assert_eq!(results[0].state, ActionState::Cancelled);
    assert_eq!(executor.apply_count(), 0);
}

#[test]
fn a_replaced_target_is_superseded() {
    let executor = FakeExecutor::with(&[ActionKind::CgroupSoftLimit], 4 * GIB, 4 * GIB);
    let mut coordinator = coordinator();
    // The plan was made against generation 1, but generation 2 is live now.
    let workloads = vec![workload("unit:app.service", 2, "app")];
    let results = coordinator.execute(
        &[request("unit:app.service", 1)],
        &workloads,
        &executor,
        2_000,
        &Cancellation::new(),
    );
    assert_eq!(results[0].state, ActionState::Superseded);
    assert_eq!(executor.apply_count(), 0);

    // Capability revoked between planning and execution.
    let executor = FakeExecutor::with(&[ActionKind::CgroupSoftLimit], 4 * GIB, 4 * GIB);
    executor.revoke_support(ActionKind::CgroupSoftLimit);
    let results = run(
        &mut coordinator,
        &executor,
        &[request("unit:app.service", 1)],
    );
    assert_eq!(results[0].state, ActionState::Unsupported);
    assert_eq!(executor.apply_count(), 0);
}

#[test]
fn a_partial_write_is_reported_and_journalled_for_restore() {
    let executor = FakeExecutor::with(&[ActionKind::CgroupSoftLimit], 4 * GIB, 3 * GIB)
        .script_apply(Err(ExecutorError::Partial {
            reason: "the second setting was refused".to_string(),
            values: vec![owned(KEY, "4294967296", "2147483648")],
        }));
    executor.set_value(KEY, "2147483648");
    let mut coordinator = coordinator();
    let results = run(
        &mut coordinator,
        &executor,
        &[request("unit:app.service", 1)],
    );

    assert_eq!(results[0].state, ActionState::Failed);
    assert!(results[0].detail.contains("partially"));
    let entry = coordinator
        .journal()
        .entries()
        .front()
        .expect("a journal entry");
    assert_eq!(entry.values.len(), 1, "the partial write was recorded");
    assert_eq!(entry.values[0].owner, OwnerToken("instance-1".to_string()));

    // And recovery can put it back.
    let restored = coordinator.recover(&executor, 3_000);
    assert_eq!(restored[0].outcome, RestoreOutcome::Restored);
    assert_eq!(executor.value(KEY).as_deref(), Some("4294967296"));
}

#[test]
fn a_disconnect_is_reported_as_a_failure_with_the_reason() {
    let executor = FakeExecutor::with(&[ActionKind::CgroupSoftLimit], 4 * GIB, 4 * GIB)
        .script_apply(Err(ExecutorError::Disconnected(
            "the socket closed".to_string(),
        )));
    let mut coordinator = coordinator();
    let results = run(
        &mut coordinator,
        &executor,
        &[request("unit:app.service", 1)],
    );
    assert_eq!(results[0].state, ActionState::Failed);
    assert!(results[0]
        .detail
        .contains("connection to the target was lost"));
    assert!(results[0].detail.contains("the socket closed"));
}

#[test]
fn an_unconfirmed_effect_is_not_reported_as_confirmed() {
    let executor = FakeExecutor::with(&[ActionKind::CgroupSoftLimit], 4 * GIB, 3 * GIB)
        .script_confirmation(Confirmation::NotYet {
            reason: "reclaim has not started yet".to_string(),
        });
    let mut coordinator = coordinator();
    let results = run(
        &mut coordinator,
        &executor,
        &[request("unit:app.service", 1)],
    );
    assert_eq!(results[0].state, ActionState::Unconfirmed);
    assert!(!results[0].is_confirmed());
}

#[test]
fn recovery_after_a_restart_restores_values_breadram_still_owns() {
    let executor = FakeExecutor::with(&[ActionKind::CgroupSoftLimit], 4 * GIB, 2 * GIB)
        .script_apply(Ok(AppliedChange {
            values: vec![owned(KEY, "4294967296", "2147483648")],
        }));
    let mut first_run = coordinator();
    run(&mut first_run, &executor, &[request("unit:app.service", 1)]);
    assert_eq!(executor.value(KEY).as_deref(), Some("2147483648"));

    // A restart: the journal survives, the instance token does not.
    let mut restarted = Coordinator::resume(
        OwnerToken("instance-2".to_string()),
        first_run.journal().clone(),
    );
    let restored = restarted.recover(&executor, 9_000);
    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0].outcome, RestoreOutcome::Restored);
    assert_eq!(executor.value(KEY).as_deref(), Some("4294967296"));

    // A second recovery has nothing left to do.
    let again = restarted.recover(&executor, 10_000);
    assert_eq!(again[0].outcome, RestoreOutcome::SkippedNoChange);
}

#[test]
fn recovery_does_not_overwrite_a_newer_external_change() {
    let executor = FakeExecutor::with(&[ActionKind::CgroupSoftLimit], 4 * GIB, 2 * GIB)
        .script_apply(Ok(AppliedChange {
            values: vec![owned(KEY, "4294967296", "2147483648")],
        }));
    let mut coordinator = coordinator();
    run(
        &mut coordinator,
        &executor,
        &[request("unit:app.service", 1)],
    );

    // Another manager writes its own value after breadram.
    executor.set_value(KEY, "1073741824");

    let restored = coordinator.recover(&executor, 9_000);
    assert_eq!(
        restored[0].outcome,
        RestoreOutcome::SkippedForeignChange {
            current: "1073741824".to_string()
        }
    );
    assert_eq!(
        executor.value(KEY).as_deref(),
        Some("1073741824"),
        "the newer configuration must be left alone"
    );
    assert_eq!(executor.write_count(), 0, "nothing was written");
}

#[test]
fn recovery_skips_a_target_that_is_gone() {
    let executor = FakeExecutor::with(&[ActionKind::CgroupSoftLimit], 4 * GIB, 2 * GIB)
        .script_apply(Ok(AppliedChange {
            values: vec![owned(KEY, "4294967296", "2147483648")],
        }));
    let mut coordinator = coordinator();
    run(
        &mut coordinator,
        &executor,
        &[request("unit:app.service", 1)],
    );
    // The target directory disappears.
    lock(&executor.values).remove(KEY);

    let restored = coordinator.recover(&executor, 9_000);
    assert_eq!(restored[0].outcome, RestoreOutcome::SkippedTargetGone);
}

#[test]
fn the_journal_never_holds_a_command_line_a_prompt_or_a_credential() {
    // A workload whose human name is a full command line with a secret and a
    // prompt: none of it may reach the journal.
    let noisy = "/usr/bin/server --api-key=hunter2 --prompt \"summarise the payroll file\"";
    let executor = FakeExecutor::with(&[ActionKind::CgroupSoftLimit], 4 * GIB, 3 * GIB);
    let mut coordinator = coordinator();
    let workloads = vec![workload("unit:app.service", 1, noisy)];
    coordinator.execute(
        &[request("unit:app.service", 1)],
        &workloads,
        &executor,
        2_000,
        &Cancellation::new(),
    );

    let rendered = format!("{:?}", coordinator.journal());
    assert!(
        rendered.contains("unit:app.service"),
        "the scope key is kept"
    );
    assert!(!rendered.contains("hunter2"), "no credential");
    assert!(!rendered.contains("--api-key"), "no command line");
    assert!(!rendered.contains("payroll file"), "no prompt");
    assert!(!rendered.contains("/usr/bin/server"), "no executable path");
    assert!(
        !rendered.contains("PATH=") && !rendered.contains("HOME="),
        "no environment variables"
    );
}

#[test]
fn the_journal_stays_bounded_under_a_long_run() {
    let executor = FakeExecutor::with(&[ActionKind::CgroupSoftLimit], 4 * GIB, 2 * GIB);
    let mut coordinator = Coordinator::new(OwnerToken("instance-1".to_string()), 4);
    let workloads = vec![workload("unit:app.service", 1, "app")];
    for tick in 0..20u64 {
        // A fresh generation each time, so every request is a distinct action.
        let workloads = vec![workload("unit:app.service", tick + 1, "app")];
        let request = ActionRequest::new(
            WorkloadId {
                scope: "unit:app.service".to_string(),
                generation: tick + 1,
            },
            ActionKind::CgroupSoftLimit,
            "over budget",
            1_000 + tick,
        );
        coordinator.execute(
            &[request],
            &workloads,
            &executor,
            2_000 + tick,
            &Cancellation::new(),
        );
    }
    let _ = workloads;
    assert_eq!(coordinator.journal().len(), 4);
    assert_eq!(coordinator.journal().capacity(), 4);
}
