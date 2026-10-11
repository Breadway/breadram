//! Turning plans into actions, safely.
//!
//! The planner proposes; the coordinator disposes. It takes a batch of proposed
//! actions and, one at a time and in order, revalidates each against the live
//! inventory, applies it through an [`Executor`], confirms the effect, measures
//! what actually changed, and journals the whole thing.
//!
//! Six properties are load bearing:
//!
//! 1. **Idempotent.** An action's id is derived from its target and kind, so a
//!    repeated request carries the same id. If that id already has a confirmed
//!    effect, the duplicate is skipped. A failure does not block a retry.
//! 2. **Revalidated immediately before running.** The target generation is
//!    re-checked against the current inventory and the executor is asked whether
//!    it still supports the action, after planning and right before execution.
//! 3. **Honest.** Unsupported, busy, denied, timed out, failed, unconfirmed, and
//!    confirmed are distinct states and none of them is quietly rounded off.
//! 4. **Measured.** The requested effect and the observed effect are recorded
//!    separately, and host memory and device memory are recorded separately.
//! 5. **Bounded and reversible.** The journal is fixed size and keeps the
//!    original and last applied value of every daemon-owned setting, so a restore
//!    can be safe.
//! 6. **Quiet.** Journal entries hold a stable scope key, never a command line,
//!    an environment variable, a prompt, or a credential.
//!
//! Execution is serialized: [`Coordinator::execute`] runs requests one at a time
//! in the calling thread, so there is no interleaving and no need for internal
//! locking. One coordinator is owned by one task.

pub mod executor;
pub mod journal;
pub mod persist;

pub use executor::{
    AppliedChange, ChainedExecutor, Confirmation, Executor, ExecutorError, MemoryObservation,
    Precondition,
};
pub use journal::{
    ActionJournal, JournalEntry, OwnedValue, OwnerToken, RestoreOutcome, RestoreResult,
};
pub use persist::{PersistedJournal, PersistedOwnedValue, JOURNAL_STATE_VERSION};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::config::ActionKind;
use crate::inventory::{Workload, WorkloadId};

/// A stable, idempotency id for an action.
///
/// Derived from the target scope, its generation, and the action kind with a
/// fixed hash, so the same logical action always produces the same id, in this
/// process and after a restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ActionId(u64);

impl ActionId {
    /// The id for an action against a stable scope key and generation.
    pub fn from_scope(scope: &str, generation: u64, kind: ActionKind) -> Self {
        let key = format!("{scope}\u{1f}{generation}\u{1f}{kind:?}");
        Self(fnv1a(key.as_bytes()))
    }

    /// The id for an action against a workload.
    pub fn for_target(target: &WorkloadId, kind: ActionKind) -> Self {
        Self::from_scope(&target.scope, target.generation, kind)
    }

    /// The raw hash, for diagnostics.
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Rebuild an id from the value a previous run recorded.
    ///
    /// Used when a journal is recovered from disk, so a restore can be journalled
    /// against the action that originally wrote the value. It is the same hash a
    /// lookup would produce: a recovered id is never invented.
    pub fn from_raw(raw: u64) -> Self {
        Self(raw)
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// A bounded unit of work derived from one planned action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionRequest {
    /// The target workload, including the generation it was planned against.
    pub target: WorkloadId,
    /// What to do.
    pub kind: ActionKind,
    /// Why, in words, for the journal.
    pub reason: String,
    /// The expected host memory change in bytes. Negative is relief.
    pub expected_host_ram_delta: i64,
    /// The expected device memory change in bytes.
    pub expected_device_ram_delta: i64,
    /// When the plan was made, in monotonic milliseconds.
    pub requested_at_ms: u64,
    /// The latest time the action may start. Zero means no deadline.
    pub deadline_ms: u64,
    /// The value the action intends to reach, when the action has one.
    ///
    /// For a bounded soft limit this is the budget in bytes to limit the
    /// workload to. An executor that needs a value refuses the action when this
    /// is absent rather than guessing one.
    pub limit_bytes: Option<u64>,
}

impl ActionRequest {
    /// A request with no deadline.
    pub fn new(
        target: WorkloadId,
        kind: ActionKind,
        reason: impl Into<String>,
        at_ms: u64,
    ) -> Self {
        Self {
            target,
            kind,
            reason: reason.into(),
            expected_host_ram_delta: 0,
            expected_device_ram_delta: 0,
            requested_at_ms: at_ms,
            deadline_ms: 0,
            limit_bytes: None,
        }
    }

    /// Set the value the action intends to reach.
    pub fn with_limit_bytes(mut self, limit_bytes: u64) -> Self {
        self.limit_bytes = Some(limit_bytes);
        self
    }

    /// Set the deadline.
    pub fn with_deadline(mut self, deadline_ms: u64) -> Self {
        self.deadline_ms = deadline_ms;
        self
    }

    /// Set the expected host memory change.
    pub fn with_expected_host_ram_delta(mut self, delta: i64) -> Self {
        self.expected_host_ram_delta = delta;
        self
    }

    /// Set the expected device memory change.
    pub fn with_expected_device_ram_delta(mut self, delta: i64) -> Self {
        self.expected_device_ram_delta = delta;
        self
    }

    /// The idempotency id.
    pub fn id(&self) -> ActionId {
        ActionId::for_target(&self.target, self.kind)
    }

    /// Whether the deadline has passed at `now_ms`. A zero deadline never
    /// expires.
    pub fn is_expired(&self, now_ms: u64) -> bool {
        self.deadline_ms != 0 && now_ms >= self.deadline_ms
    }
}

/// How an action ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionState {
    /// Applied and confirmed.
    Confirmed,
    /// Applied but the effect could not be confirmed.
    Unconfirmed,
    /// A duplicate of an action that was already confirmed.
    SkippedDuplicate,
    /// No executor can apply this action.
    Unsupported,
    /// The target changed or vanished since the plan was made.
    Superseded,
    /// The target or executor was busy. Retry later.
    Busy,
    /// Policy, configuration, or the system refused.
    Denied,
    /// The deadline passed before the action could start, or the executor timed
    /// out.
    TimedOut,
    /// The caller cancelled before the action started.
    Cancelled,
    /// The change failed, including a partial application.
    Failed,
    /// A daemon-owned value was restored.
    RolledBack,
}

impl ActionState {
    /// Whether a repeated request for this id should be skipped.
    ///
    /// Only a confirmed effect is truly already applied. A failure, a timeout, a
    /// refusal, or a busy target must remain retryable.
    pub fn is_terminal(self) -> bool {
        !matches!(self, ActionState::Busy)
    }

    /// A short, stable name for reports.
    pub fn as_str(self) -> &'static str {
        match self {
            ActionState::Confirmed => "confirmed",
            ActionState::Unconfirmed => "unconfirmed",
            ActionState::SkippedDuplicate => "skipped_duplicate",
            ActionState::Unsupported => "unsupported",
            ActionState::Superseded => "superseded",
            ActionState::Busy => "busy",
            ActionState::Denied => "denied",
            ActionState::TimedOut => "timed_out",
            ActionState::Cancelled => "cancelled",
            ActionState::Failed => "failed",
            ActionState::RolledBack => "rolled_back",
        }
    }
}

/// The requested and measured effect of an action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionEffect {
    /// The host memory change the plan expected.
    pub requested_host_ram_delta: i64,
    /// The host memory change that was measured, when it could be measured.
    pub observed_host_ram_delta: Option<i64>,
    /// The device memory change the plan expected.
    pub requested_device_ram_delta: i64,
    /// The device memory change that was measured, when it could be measured.
    pub observed_device_ram_delta: Option<i64>,
}

/// What happened to one action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionResult {
    /// The idempotency id.
    pub id: ActionId,
    /// The target scope key.
    pub scope: String,
    /// The action.
    pub kind: ActionKind,
    /// How it ended.
    pub state: ActionState,
    /// A human readable detail, which never contains a credential.
    pub detail: String,
    /// The requested and observed effect, when the action ran.
    pub effect: Option<ActionEffect>,
}

impl ActionResult {
    /// Whether the effect was confirmed.
    pub fn is_confirmed(&self) -> bool {
        self.state == ActionState::Confirmed
    }
}

/// A cooperative cancellation flag.
#[derive(Debug, Default, Clone)]
pub struct Cancellation {
    flag: Arc<AtomicBool>,
}

impl Cancellation {
    /// A fresh, uncancelled flag.
    pub fn new() -> Self {
        Self::default()
    }

    /// Ask the coordinator to stop after the action in flight.
    pub fn cancel(&self) {
        self.flag.store(true, Ordering::SeqCst);
    }

    /// Whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    /// Clear the flag.
    pub fn reset(&self) {
        self.flag.store(false, Ordering::SeqCst);
    }
}

/// Applies planned actions one at a time, journaling each one.
#[derive(Debug, Clone)]
pub struct Coordinator {
    instance: OwnerToken,
    journal: ActionJournal,
}

impl Coordinator {
    /// A new coordinator with an empty, bounded journal.
    pub fn new(instance: OwnerToken, journal_capacity: usize) -> Self {
        Self {
            instance,
            journal: ActionJournal::new(journal_capacity),
        }
    }

    /// Resume from a journal recovered from disk, as after a restart.
    pub fn resume(instance: OwnerToken, journal: ActionJournal) -> Self {
        Self { instance, journal }
    }

    /// The journal.
    pub fn journal(&self) -> &ActionJournal {
        &self.journal
    }

    /// This instance's owner token.
    pub fn instance(&self) -> &OwnerToken {
        &self.instance
    }

    /// Execute a batch of requests, in order, one at a time.
    pub fn execute(
        &mut self,
        requests: &[ActionRequest],
        workloads: &[Workload],
        executor: &dyn Executor,
        now_ms: u64,
        cancel: &Cancellation,
    ) -> Vec<ActionResult> {
        let mut results = Vec::with_capacity(requests.len());
        for request in requests {
            results.push(self.execute_one(request, workloads, executor, now_ms, cancel));
        }
        results
    }

    fn execute_one(
        &mut self,
        request: &ActionRequest,
        workloads: &[Workload],
        executor: &dyn Executor,
        now_ms: u64,
        cancel: &Cancellation,
    ) -> ActionResult {
        let id = request.id();
        let scope = request.target.scope.clone();
        // Why the action ran, and what it turned out to involve. An executor that
        // has something to say about the consequence, such as the cold start a
        // model release costs, has it recorded with the entry rather than only in
        // the pass that reported it.
        let mut journal_reason = request.reason.clone();

        macro_rules! finish {
            ($state:expr, $detail:expr, $effect:expr, $values:expr) => {{
                let result = ActionResult {
                    id,
                    scope: scope.clone(),
                    kind: request.kind,
                    state: $state,
                    detail: $detail,
                    effect: $effect,
                };
                self.journal.record(JournalEntry {
                    id,
                    scope: scope.clone(),
                    kind: Some(request.kind),
                    state: result.state,
                    at_ms: now_ms,
                    reason: journal_reason.clone(),
                    effect: result.effect.clone(),
                    values: $values,
                });
                return result;
            }};
        }

        if self.journal.has_confirmed(id) {
            finish!(
                ActionState::SkippedDuplicate,
                "this action already has a confirmed effect".to_string(),
                None,
                Vec::new()
            );
        }
        if cancel.is_cancelled() {
            finish!(
                ActionState::Cancelled,
                "cancelled before the action started".to_string(),
                None,
                Vec::new()
            );
        }
        if request.is_expired(now_ms) {
            finish!(
                ActionState::TimedOut,
                "the deadline passed before the action could start".to_string(),
                None,
                Vec::new()
            );
        }
        if let Some(reason) = revalidate(&request.target, workloads) {
            finish!(ActionState::Superseded, reason, None, Vec::new());
        }
        if !executor.supports(request.kind) {
            finish!(
                ActionState::Unsupported,
                "no executor supports this action on this host".to_string(),
                None,
                Vec::new()
            );
        }
        match executor.precondition(request) {
            Precondition::Ready => {}
            Precondition::Unsupported { reason } => {
                finish!(ActionState::Unsupported, reason, None, Vec::new());
            }
            Precondition::Busy { reason } => {
                finish!(ActionState::Busy, reason, None, Vec::new());
            }
            Precondition::Denied { reason } => {
                finish!(ActionState::Denied, reason, None, Vec::new());
            }
            Precondition::TargetGone { reason } => {
                finish!(ActionState::Superseded, reason, None, Vec::new());
            }
        }

        let before = executor.observe(&request.target);
        let effect_of = |before: MemoryObservation, after: MemoryObservation| ActionEffect {
            requested_host_ram_delta: request.expected_host_ram_delta,
            observed_host_ram_delta: delta(before.host_ram_bytes, after.host_ram_bytes),
            requested_device_ram_delta: request.expected_device_ram_delta,
            observed_device_ram_delta: delta(before.device_ram_bytes, after.device_ram_bytes),
        };

        match executor.apply(request) {
            Ok(change) => {
                let confirmation = executor.confirm(request);
                let after = executor.observe(&request.target);
                let note = change.note.clone();
                if let Some(note) = &note {
                    journal_reason = format!("{} ({note})", request.reason);
                }
                let values = self.own(change.values);
                match confirmation {
                    Confirmation::Confirmed => finish!(
                        ActionState::Confirmed,
                        with_note("applied and confirmed", &note),
                        Some(effect_of(before, after)),
                        values
                    ),
                    Confirmation::NotYet { reason } => finish!(
                        ActionState::Unconfirmed,
                        with_note(
                            &format!("applied but the effect is not visible yet: {reason}"),
                            &note
                        ),
                        Some(effect_of(before, after)),
                        values
                    ),
                    Confirmation::Refused { reason } => finish!(
                        ActionState::Failed,
                        with_note(
                            &format!("the target refused to confirm the effect: {reason}"),
                            &note
                        ),
                        Some(effect_of(before, after)),
                        values
                    ),
                }
            }
            Err(ExecutorError::Partial { reason, values }) => {
                let values = self.own(values);
                finish!(
                    ActionState::Failed,
                    format!("applied only partially: {reason}"),
                    None,
                    values
                );
            }
            Err(ExecutorError::Timeout(reason)) => {
                finish!(ActionState::TimedOut, reason, None, Vec::new());
            }
            Err(ExecutorError::Unsupported(reason)) => {
                finish!(ActionState::Unsupported, reason, None, Vec::new());
            }
            Err(ExecutorError::Denied(reason)) => {
                finish!(ActionState::Denied, reason, None, Vec::new());
            }
            Err(ExecutorError::Disconnected(reason)) => {
                finish!(
                    ActionState::Failed,
                    format!("the connection to the target was lost: {reason}"),
                    None,
                    Vec::new()
                );
            }
            Err(ExecutorError::Failed(reason)) => {
                finish!(ActionState::Failed, reason, None, Vec::new());
            }
        }
    }

    fn own(&self, values: Vec<OwnedValue>) -> Vec<OwnedValue> {
        values
            .into_iter()
            .map(|value| OwnedValue {
                owner: self.instance.clone(),
                ..value
            })
            .collect()
    }

    /// Restore daemon-owned values that breadram still owns.
    ///
    /// This is what runs after a restart, on disable, on rule removal, and when a
    /// target exits. It walks the journal newest first and, for each key it has
    /// not already handled, restores the original value only while the value in
    /// place is still the one breadram last wrote. Anything else means another
    /// manager changed it, and a newer configuration is never overwritten.
    pub fn recover(&mut self, executor: &dyn Executor, now_ms: u64) -> Vec<RestoreResult> {
        let mut results = Vec::new();
        for (id, value) in self.journal.owned_values() {
            let outcome = match executor.read_value(&value.key) {
                Ok(None) => RestoreOutcome::SkippedTargetGone,
                Ok(Some(current)) if current == value.original => RestoreOutcome::SkippedNoChange,
                Ok(Some(current)) if current == value.last_applied => {
                    match executor.write_value(&value.key, &value.original) {
                        Ok(()) => RestoreOutcome::Restored,
                        Err(error) => RestoreOutcome::Failed {
                            reason: error.to_string(),
                        },
                    }
                }
                Ok(Some(current)) => RestoreOutcome::SkippedForeignChange { current },
                Err(error) => RestoreOutcome::Failed {
                    reason: error.to_string(),
                },
            };
            if matches!(outcome, RestoreOutcome::Restored) {
                self.journal.record(JournalEntry {
                    id,
                    scope: value.key.clone(),
                    // A restore is not the action it undid.
                    kind: None,
                    state: ActionState::RolledBack,
                    at_ms: now_ms,
                    reason: format!("restored {} to its original value", value.key),
                    effect: None,
                    values: Vec::new(),
                });
            }
            results.push(RestoreResult {
                id,
                key: value.key,
                outcome,
            });
        }
        results
    }
}

/// Append an executor note to a detail string, when there is one.
fn with_note(base: &str, note: &Option<String>) -> String {
    match note {
        Some(note) => format!("{base} ({note})"),
        None => base.to_string(),
    }
}

/// Revalidate the target generation against the current inventory.
///
/// Returns a reason when the action must not run.
fn revalidate(target: &WorkloadId, workloads: &[Workload]) -> Option<String> {
    match workloads
        .iter()
        .find(|workload| workload.id.scope == target.scope)
    {
        None => Some(format!("the target {} is no longer present", target.scope)),
        Some(workload) if workload.id.generation != target.generation => Some(format!(
            "the target {} was replaced (generation {} became {})",
            target.scope, target.generation, workload.id.generation
        )),
        Some(_) => None,
    }
}

/// The measured change between two readings, when both are known.
fn delta(before: Option<u64>, after: Option<u64>) -> Option<i64> {
    let before = before?;
    let after = after?;
    let change = i64::try_from(after).ok()? - i64::try_from(before).ok()?;
    Some(change)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_id_is_stable_and_target_specific() {
        let a = ActionId::from_scope("unit:app.service", 7, ActionKind::CgroupSoftLimit);
        let b = ActionId::from_scope("unit:app.service", 7, ActionKind::CgroupSoftLimit);
        assert_eq!(a, b);
        assert_ne!(
            a,
            ActionId::from_scope("unit:app.service", 8, ActionKind::CgroupSoftLimit),
            "a new generation is a different action"
        );
        assert_ne!(
            a,
            ActionId::from_scope("unit:app.service", 7, ActionKind::SleepModel),
            "a different action is a different id"
        );
        assert_ne!(
            a,
            ActionId::from_scope("unit:other.service", 7, ActionKind::CgroupSoftLimit)
        );
    }

    #[test]
    fn an_id_is_a_fixed_hash_of_the_stable_key() {
        // The literal is FNV-1a over "unit:app.service\x1f1\x1fSleepModel".
        // Pinning it means the id cannot drift with a hasher or format change,
        // which is what lets a duplicate request be recognised after a restart.
        assert_eq!(
            ActionId::from_scope("unit:app.service", 1, ActionKind::SleepModel).as_u64(),
            0x6b3c_65b6_2fcd_a25c
        );
    }

    #[test]
    fn only_busy_is_retryable() {
        assert!(!ActionState::Busy.is_terminal());
        for state in [
            ActionState::Confirmed,
            ActionState::Unconfirmed,
            ActionState::SkippedDuplicate,
            ActionState::Unsupported,
            ActionState::Superseded,
            ActionState::Denied,
            ActionState::TimedOut,
            ActionState::Cancelled,
            ActionState::Failed,
            ActionState::RolledBack,
        ] {
            assert!(state.is_terminal(), "{state:?} should be terminal");
        }
    }

    #[test]
    fn a_zero_deadline_never_expires() {
        let request = ActionRequest::new(
            WorkloadId {
                scope: "unit:app.service".to_string(),
                generation: 1,
            },
            ActionKind::CgroupSoftLimit,
            "test",
            0,
        );
        assert!(!request.is_expired(u64::MAX));
        assert!(request.with_deadline(100).is_expired(100));
    }

    fn workload(scope: &str, generation: u64) -> Workload {
        use crate::inventory::{ControlLevel, MemoryAccounting};
        Workload {
            id: WorkloadId {
                scope: scope.to_string(),
                generation,
            },
            name: scope.to_string(),
            pids: vec![1],
            accounting: MemoryAccounting::Cgroup {
                current_bytes: Some(1024),
            },
            control: ControlLevel::Eligible,
        }
    }

    #[test]
    fn revalidation_catches_a_missing_and_a_replaced_target() {
        let workloads = vec![workload("unit:app.service", 2)];
        let current = WorkloadId {
            scope: "unit:app.service".to_string(),
            generation: 2,
        };
        assert_eq!(revalidate(&current, &workloads), None);

        let replaced = WorkloadId {
            scope: "unit:app.service".to_string(),
            generation: 1,
        };
        assert!(revalidate(&replaced, &workloads).is_some());

        let missing = WorkloadId {
            scope: "unit:gone.service".to_string(),
            generation: 1,
        };
        assert!(revalidate(&missing, &workloads).is_some());
    }

    #[test]
    fn a_delta_needs_both_readings() {
        assert_eq!(delta(Some(100), Some(60)), Some(-40));
        assert_eq!(delta(Some(100), None), None);
        assert_eq!(delta(None, Some(1)), None);
    }

    #[test]
    fn cancellation_is_observable() {
        let cancel = Cancellation::new();
        assert!(!cancel.is_cancelled());
        cancel.cancel();
        assert!(cancel.is_cancelled());
        cancel.reset();
        assert!(!cancel.is_cancelled());
    }
}
