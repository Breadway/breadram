//! The plan-execute step: the one place a plan becomes a write.
//!
//! The planner proposes and the coordinator disposes. This module joins them,
//! and it is deliberately the only code that decides whether a proposed action
//! may actually be written, so the answer to "why did nothing happen" is in one
//! place:
//!
//! 1. **Monitor mode never writes.** Not because a caller happens to pass no
//!    requests, but because the mode is checked here. The plan is still built and
//!    explained, so a monitor-mode daemon can describe exactly what automatic
//!    mode would do.
//! 2. **The configuration is re-checked.** The planner already declines an action
//!    that is not in `allowed_actions`, and this checks again, because a plan can
//!    outlive the configuration it was made under.
//! 3. **Nothing is dropped silently.** An action that is refused before it runs,
//!    or by the executor, is recorded: the coordinator journals the executor's
//!    refusal, and this module returns a note for its own refusals.
//!
//! The step is a plain function over a coordinator and an [`Executor`], so the
//! whole path can be tested against a fake executor, and the daemon calls it with
//! the real control executor from the control layer.

use crate::config::{ActionKind, Config};
use crate::coordinator::{
    ActionRequest, ActionResult, ActionState, Cancellation, Coordinator, Executor,
};
use crate::inventory::{MemoryAccounting, Workload};
use crate::policy::{budget_for, BudgetInputs, Plan};
use crate::ManagementMode;

/// What one pass over a plan did, or why it wrote nothing.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExecutionPass {
    /// The requests the plan implied and the configuration allowed.
    pub requests: Vec<ActionRequest>,
    /// What happened to each of them, in order.
    pub results: Vec<ActionResult>,
    /// Why nothing ran, or what was refused before running. Never empty when the
    /// pass wrote nothing.
    pub notes: Vec<String>,
    /// How many limits this pass put back after a stall rise. A restore is a
    /// write to the host and to the journal even when no action ran.
    pub reversed: usize,
}

impl ExecutionPass {
    /// A pass that wrote nothing, with the reason.
    pub fn declined(reason: impl Into<String>) -> Self {
        Self {
            requests: Vec::new(),
            results: Vec::new(),
            notes: vec![reason.into()],
            reversed: 0,
        }
    }

    /// How many actions were applied and confirmed.
    pub fn confirmed(&self) -> usize {
        self.results
            .iter()
            .filter(|result| result.state == ActionState::Confirmed)
            .count()
    }

    /// How many were journalled in a state other than confirmed.
    pub fn not_confirmed(&self) -> usize {
        self.results.len().saturating_sub(self.confirmed())
    }

    /// Whether anything was written. A refusal is not a write, but it is still
    /// recorded.
    pub fn wrote(&self) -> bool {
        !self.results.is_empty() || self.reversed > 0
    }

    /// One line describing the pass, for a report or a log.
    pub fn summary(&self) -> String {
        if self.results.is_empty() && self.reversed > 0 {
            return format!(
                "{} soft limit(s) put back after a stall rise",
                self.reversed
            );
        }
        if self.results.is_empty() {
            return self
                .notes
                .first()
                .cloned()
                .unwrap_or_else(|| "nothing to do".to_string());
        }
        format!(
            "{} action(s) attempted, {} confirmed, {} not confirmed",
            self.results.len(),
            self.confirmed(),
            self.not_confirmed()
        )
    }
}

/// The requests a plan implies, keeping only what the configuration allows.
///
/// Returns the requests in the order the plan proposed them, plus a note for
/// every action that was refused here. An action with no value to apply is still
/// requested: the executor refuses it with its own reason, and the refusal is
/// journalled, which is more useful than a silent skip.
pub fn requests_for_plan(
    plan: &Plan,
    config: &Config,
    workloads: &[Workload],
    now_ms: u64,
    host_bytes: u64,
) -> (Vec<ActionRequest>, Vec<String>) {
    let mut requests = Vec::new();
    let mut notes = Vec::new();
    for action in &plan.actions {
        if !config.automation.allowed_actions.contains(&action.kind) {
            notes.push(format!(
                "{} on {} was not requested: automation does not allow it",
                action_kind_name(action.kind),
                action.target.scope
            ));
            continue;
        }
        let workload = workloads
            .iter()
            .find(|candidate| candidate.id.scope == action.target.scope);
        let mut request = ActionRequest::new(
            action.target.clone(),
            action.kind,
            action.reason.clone(),
            now_ms,
        )
        .with_expected_host_ram_delta(action.expected_host_ram_delta);
        if let Some(value) = value_to_apply(
            action.kind,
            config,
            workload,
            host_bytes,
            action.ancestor_bytes,
        ) {
            request = request.with_limit_bytes(value);
        }
        requests.push(request);
    }
    (requests, notes)
}

/// The value an action intends to reach, when it has one.
///
/// A bounded soft limit uses the effective budget: the configured number,
/// shrunk by the host size and the desktop reserve, and by the tightest parent
/// cap the plan carries. A limit invented by the daemon would be policy nobody
/// asked for, so a soft limit with no configured budget, or a budget that
/// leaves no room, is absent and the executor refuses the action by name
/// rather than guessing. Reclaim protection with no configured budget falls
/// back to the current usage, which is the only bounded number available.
fn value_to_apply(
    kind: ActionKind,
    config: &Config,
    workload: Option<&Workload>,
    host_bytes: u64,
    ancestor_bytes: Option<u64>,
) -> Option<u64> {
    let workload = workload?;
    match kind {
        ActionKind::CgroupSoftLimit => {
            let configured = budget_for(config, workload)?;
            crate::policy::effective_budget(BudgetInputs {
                configured_bytes: configured,
                host_bytes,
                ancestor_bytes,
            })
            .ok()
            .map(|budget| budget.bytes)
        }
        ActionKind::ReclaimProtection => {
            budget_for(config, workload).or(match workload.accounting {
                MemoryAccounting::Cgroup { current_bytes } => current_bytes,
                MemoryAccounting::RssEstimate { rss_bytes, .. } => Some(rss_bytes),
            })
        }
        _ => None,
    }
}

/// Run a plan through the coordinator, gated on the management mode.
///
/// In any mode other than automatic this writes nothing and says so. The caller
/// is expected to have journalled the plan itself through the explanation path;
/// this only reports the refusal.
#[allow(clippy::too_many_arguments)]
pub fn execute_plan(
    coordinator: &mut Coordinator,
    plan: &Plan,
    config: &Config,
    mode: ManagementMode,
    workloads: &[Workload],
    executor: &dyn Executor,
    now_ms: u64,
    cancel: &Cancellation,
    host_bytes: u64,
) -> ExecutionPass {
    if mode != ManagementMode::Automatic {
        return ExecutionPass::declined(format!(
            "{mode} mode never writes: the plan is explained, not executed"
        ));
    }
    let (requests, notes) = requests_for_plan(plan, config, workloads, now_ms, host_bytes);
    if requests.is_empty() {
        let mut pass = ExecutionPass::declined(if plan.actions.is_empty() {
            "the plan proposed no action".to_string()
        } else {
            format!(
                "none of the {} proposed action(s) was permitted",
                plan.actions.len()
            )
        });
        // A refusal this module made is still visible.
        pass.notes.extend(notes);
        return pass;
    }
    let results = coordinator.execute(&requests, workloads, executor, now_ms, cancel);
    ExecutionPass {
        requests,
        results,
        notes,
        reversed: 0,
    }
}

/// The configuration name of an action, for a note a person reads.
fn action_kind_name(kind: ActionKind) -> &'static str {
    match kind {
        ActionKind::CgroupSoftLimit => "a bounded soft limit",
        ActionKind::ReclaimProtection => "reclaim protection",
        ActionKind::UnloadIdleModel => "an idle model release",
        ActionKind::SleepModel => "a backend sleep",
        ActionKind::ReduceJobConcurrency => "a job concurrency reduction",
        ActionKind::QueueBackgroundJob => "a background job delay",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ActionKind, Automation, Budget, Config};
    use crate::coordinator::{
        AppliedChange, ExecutorError, MemoryObservation, OwnerToken, Precondition,
    };
    use crate::inventory::{ControlLevel, WorkloadId};
    use crate::policy::PlannedAction;

    #[test]
    fn a_pass_that_only_put_a_limit_back_still_counts_as_a_write() {
        let mut pass = ExecutionPass::declined("nothing planned");
        assert!(!pass.wrote());
        pass.reversed = 1;
        assert!(pass.wrote());
        assert_eq!(
            pass.summary(),
            "1 soft limit(s) put back after a stall rise"
        );
    }

    const GIB: u64 = 1024 * 1024 * 1024;
    /// A fixed instant, so an assertion about a recorded timestamp is exact.
    const NOW_MS: u64 = 12_345;

    struct FakeExecutor {
        support: bool,
        precondition: Precondition,
        apply: Result<AppliedChange, ExecutorError>,
        before: u64,
        after: u64,
        applies: std::sync::atomic::AtomicUsize,
    }

    impl FakeExecutor {
        fn confirming(before: u64, after: u64) -> Self {
            Self {
                support: true,
                precondition: Precondition::Ready,
                apply: Ok(AppliedChange::default()),
                before,
                after,
                applies: std::sync::atomic::AtomicUsize::new(0),
            }
        }
    }

    impl Executor for FakeExecutor {
        fn supports(&self, _kind: ActionKind) -> bool {
            self.support
        }

        fn precondition(&self, _request: &ActionRequest) -> Precondition {
            self.precondition.clone()
        }

        fn apply(&self, _request: &ActionRequest) -> Result<AppliedChange, ExecutorError> {
            self.applies
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.apply.clone()
        }

        fn read_value(&self, _key: &str) -> Result<Option<String>, ExecutorError> {
            Ok(None)
        }

        fn write_value(&self, _key: &str, _value: &str) -> Result<(), ExecutorError> {
            Err(ExecutorError::Unsupported(
                "the fake stores nothing".to_string(),
            ))
        }

        fn confirm(&self, _request: &ActionRequest) -> crate::coordinator::Confirmation {
            crate::coordinator::Confirmation::Confirmed
        }

        fn observe(&self, _target: &WorkloadId) -> MemoryObservation {
            MemoryObservation {
                host_ram_bytes: Some(
                    if self.applies.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                        self.before
                    } else {
                        self.after
                    },
                ),
                device_ram_bytes: None,
            }
        }
    }

    fn workload(scope: &str, generation: u64, bytes: u64) -> Workload {
        Workload {
            id: WorkloadId {
                scope: scope.to_string(),
                generation,
            },
            name: scope.to_string(),
            pids: vec![1],
            accounting: MemoryAccounting::Cgroup {
                current_bytes: Some(bytes),
            },
            control: ControlLevel::Eligible,
            limit_bytes: None,
            ancestor_bytes: None,
        }
    }

    fn plan(actions: Vec<PlannedAction>) -> Plan {
        Plan {
            state: crate::policy::PressureState::Pressured,
            channel: crate::policy::PlanChannel::Automatic,
            would_execute: true,
            actions,
            explanation: Vec::new(),
        }
    }

    fn soft_limit(scope: &str, generation: u64) -> PlannedAction {
        PlannedAction {
            target: WorkloadId {
                scope: scope.to_string(),
                generation,
            },
            kind: ActionKind::CgroupSoftLimit,
            reason: "the workload is over its budget".to_string(),
            expected_host_ram_delta: -(2 * GIB as i64),
            requires_confirmed_idle: false,
            ancestor_bytes: None,
        }
    }

    fn config(allowed: Vec<ActionKind>, budget: Option<u64>) -> Config {
        Config {
            automation: Automation {
                allowed_actions: allowed,
            },
            budgets: budget
                .map(|max_bytes| {
                    vec![Budget {
                        scope: "unit:build.service".to_string(),
                        max_bytes,
                    }]
                })
                .unwrap_or_default(),
            ..Config::default()
        }
    }

    fn coordinator() -> Coordinator {
        Coordinator::new(OwnerToken("instance-1".to_string()), 16)
    }

    #[test]
    fn automatic_mode_applies_and_records_the_measured_effect() {
        let workloads = vec![workload("unit:build.service", 5, 6 * GIB)];
        let settings = config(vec![ActionKind::CgroupSoftLimit], Some(2 * GIB));
        let executor = FakeExecutor::confirming(6 * GIB, 4 * GIB);
        let mut coordinator = coordinator();

        let pass = execute_plan(
            &mut coordinator,
            &plan(vec![soft_limit("unit:build.service", 5)]),
            &settings,
            ManagementMode::Automatic,
            &workloads,
            &executor,
            NOW_MS,
            &Cancellation::new(),
            64 * GIB,
        );

        assert_eq!(pass.confirmed(), 1, "{}", pass.summary());
        assert_eq!(
            pass.requests[0].limit_bytes,
            Some(2 * GIB),
            "the configured budget is the value applied"
        );
        let effect = pass.results[0].effect.as_ref().expect("a measured effect");
        assert_eq!(effect.requested_host_ram_delta, -(2 * GIB as i64));
        assert_eq!(effect.observed_host_ram_delta, Some(-(2 * GIB as i64)));

        // And the journal holds the reason, the target, the time, the result, and
        // the measured effect.
        let entries = coordinator.journal().entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].scope, "unit:build.service");
        assert_eq!(entries[0].at_ms, NOW_MS);
        assert_eq!(entries[0].reason, "the workload is over its budget");
        assert_eq!(entries[0].state, ActionState::Confirmed);
        assert!(entries[0].effect.is_some());
    }

    #[test]
    fn monitor_mode_writes_nothing_whatever_the_plan_says() {
        let workloads = vec![workload("unit:build.service", 5, 6 * GIB)];
        let settings = config(vec![ActionKind::CgroupSoftLimit], Some(2 * GIB));
        let executor = FakeExecutor::confirming(6 * GIB, 4 * GIB);
        let mut coordinator = coordinator();

        let pass = execute_plan(
            &mut coordinator,
            &plan(vec![soft_limit("unit:build.service", 5)]),
            &settings,
            ManagementMode::Monitor,
            &workloads,
            &executor,
            NOW_MS,
            &Cancellation::new(),
            64 * GIB,
        );

        assert!(!pass.wrote());
        assert_eq!(
            executor.applies.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "nothing may be written"
        );
        assert!(
            coordinator.journal().is_empty(),
            "and nothing is journalled as an action"
        );
        assert!(pass.notes[0].contains("never writes"), "{:?}", pass.notes);
    }

    #[test]
    fn an_action_the_configuration_forbids_is_refused_with_a_reason() {
        let workloads = vec![workload("unit:build.service", 5, 6 * GIB)];
        // The plan was made under a configuration that allowed it; the current
        // configuration does not, which is what a reload looks like.
        let settings = config(Vec::new(), Some(2 * GIB));
        let executor = FakeExecutor::confirming(6 * GIB, 4 * GIB);
        let mut coordinator = coordinator();

        let pass = execute_plan(
            &mut coordinator,
            &plan(vec![soft_limit("unit:build.service", 5)]),
            &settings,
            ManagementMode::Automatic,
            &workloads,
            &executor,
            NOW_MS,
            &Cancellation::new(),
            64 * GIB,
        );

        assert!(!pass.wrote());
        assert_eq!(
            executor.applies.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert!(
            pass.notes
                .iter()
                .any(|note| note.contains("automation does not allow it")),
            "{:?}",
            pass.notes
        );
    }

    #[test]
    fn an_executor_refusal_is_journalled_rather_than_dropped() {
        let workloads = vec![workload("unit:build.service", 5, 6 * GIB)];
        let settings = config(vec![ActionKind::CgroupSoftLimit], None);
        let mut executor = FakeExecutor::confirming(6 * GIB, 6 * GIB);
        executor.apply = Err(ExecutorError::Denied(
            "the action did not name the value to apply".to_string(),
        ));
        let mut coordinator = coordinator();

        let pass = execute_plan(
            &mut coordinator,
            &plan(vec![soft_limit("unit:build.service", 5)]),
            &settings,
            ManagementMode::Automatic,
            &workloads,
            &executor,
            NOW_MS,
            &Cancellation::new(),
            64 * GIB,
        );

        assert_eq!(pass.results[0].state, ActionState::Denied);
        assert!(pass.results[0].detail.contains("did not name the value"));
        assert_eq!(
            coordinator.journal().len(),
            1,
            "a refusal is recorded, not silently dropped"
        );
        assert_eq!(
            coordinator.journal().last_state(pass.results[0].id),
            Some(ActionState::Denied)
        );
    }

    #[test]
    fn a_target_superseded_between_planning_and_writing_is_not_touched() {
        // The plan was made against generation 4; the inventory now holds 5.
        let workloads = vec![workload("unit:build.service", 5, 6 * GIB)];
        let settings = config(vec![ActionKind::CgroupSoftLimit], Some(2 * GIB));
        let executor = FakeExecutor::confirming(6 * GIB, 4 * GIB);
        let mut coordinator = coordinator();

        let pass = execute_plan(
            &mut coordinator,
            &plan(vec![soft_limit("unit:build.service", 4)]),
            &settings,
            ManagementMode::Automatic,
            &workloads,
            &executor,
            NOW_MS,
            &Cancellation::new(),
            64 * GIB,
        );

        assert_eq!(pass.results[0].state, ActionState::Superseded);
        assert!(
            pass.results[0].detail.contains("replaced"),
            "{}",
            pass.results[0].detail
        );
        assert_eq!(
            executor.applies.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[test]
    fn a_failure_part_way_through_is_recorded_and_does_not_stop_the_pass() {
        let workloads = vec![
            workload("unit:build.service", 5, 6 * GIB),
            workload("unit:render.service", 3, 8 * GIB),
        ];
        let settings = Config {
            automation: Automation {
                allowed_actions: vec![ActionKind::CgroupSoftLimit],
            },
            budgets: vec![
                Budget {
                    scope: "unit:build.service".to_string(),
                    max_bytes: 2 * GIB,
                },
                Budget {
                    scope: "unit:render.service".to_string(),
                    max_bytes: GIB,
                },
            ],
            ..Config::default()
        };
        let mut executor = FakeExecutor::confirming(6 * GIB, 4 * GIB);
        let mut coordinator = coordinator();
        let actions = vec![
            PlannedAction {
                expected_host_ram_delta: -(2 * GIB as i64),
                ..soft_limit("unit:build.service", 5)
            },
            PlannedAction {
                expected_host_ram_delta: -(GIB as i64),
                ..soft_limit("unit:render.service", 3)
            },
        ];

        // A fake that fails the first request and succeeds the second.
        struct OnceFailing {
            inner: FakeExecutor,
            failed: std::sync::atomic::AtomicBool,
        }
        impl Executor for OnceFailing {
            fn supports(&self, kind: ActionKind) -> bool {
                self.inner.supports(kind)
            }
            fn precondition(&self, request: &ActionRequest) -> Precondition {
                self.inner.precondition(request)
            }
            fn apply(&self, request: &ActionRequest) -> Result<AppliedChange, ExecutorError> {
                if !self.failed.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    return Err(ExecutorError::Partial {
                        reason: "the manager rejected half the write".to_string(),
                        values: Vec::new(),
                    });
                }
                self.inner.apply(request)
            }
            fn confirm(&self, _request: &ActionRequest) -> crate::coordinator::Confirmation {
                self.inner.confirm(_request)
            }
            fn observe(&self, target: &WorkloadId) -> MemoryObservation {
                self.inner.observe(target)
            }
            fn read_value(&self, key: &str) -> Result<Option<String>, ExecutorError> {
                self.inner.read_value(key)
            }
            fn write_value(&self, key: &str, value: &str) -> Result<(), ExecutorError> {
                self.inner.write_value(key, value)
            }
        }
        let executor = OnceFailing {
            inner: std::mem::replace(&mut executor, FakeExecutor::confirming(0, 0)),
            failed: std::sync::atomic::AtomicBool::new(false),
        };

        let (requests, _) =
            requests_for_plan(&plan(actions), &settings, &workloads, NOW_MS, 64 * GIB);
        let results = coordinator.execute(
            &requests,
            &workloads,
            &executor,
            NOW_MS,
            &Cancellation::new(),
        );

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].state, ActionState::Failed);
        assert!(results[0].detail.contains("only partially"));
        assert_eq!(
            results[1].state,
            ActionState::Confirmed,
            "one failure must not stop the pass"
        );
        assert_eq!(coordinator.journal().len(), 2);
    }

    #[test]
    fn a_soft_limit_with_no_budget_names_no_value() {
        let workloads = vec![workload("unit:build.service", 5, 6 * GIB)];
        let settings = config(vec![ActionKind::CgroupSoftLimit], None);
        let (requests, notes) = requests_for_plan(
            &plan(vec![soft_limit("unit:build.service", 5)]),
            &settings,
            &workloads,
            NOW_MS,
            64 * GIB,
        );
        assert!(notes.is_empty());
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].limit_bytes, None,
            "a soft limit with no configured budget is not invented from current usage"
        );
    }

    #[test]
    fn a_soft_limit_is_the_effective_budget_not_the_configured_wish() {
        let workloads = vec![workload("unit:build.service", 5, 6 * GIB)];
        // The wish is the whole host. The applied number leaves the desktop reserve.
        let settings = config(vec![ActionKind::CgroupSoftLimit], Some(8 * GIB));
        let (requests, _) = requests_for_plan(
            &plan(vec![soft_limit("unit:build.service", 5)]),
            &settings,
            &workloads,
            NOW_MS,
            8 * GIB,
        );
        assert_eq!(
            requests[0].limit_bytes,
            Some(8 * GIB - crate::policy::DESKTOP_RESERVE_BYTES)
        );
    }

    #[test]
    fn a_soft_limit_is_clamped_to_the_tighter_parent() {
        let workloads = vec![workload("unit:build.service", 5, 6 * GIB)];
        let settings = config(vec![ActionKind::CgroupSoftLimit], Some(4 * GIB));
        let action = PlannedAction {
            ancestor_bytes: Some(2 * GIB),
            ..soft_limit("unit:build.service", 5)
        };
        let (requests, _) =
            requests_for_plan(&plan(vec![action]), &settings, &workloads, NOW_MS, 64 * GIB);
        assert_eq!(
            requests[0].limit_bytes,
            Some(2 * GIB),
            "the parent cap wins over the configured wish"
        );
    }

    #[test]
    fn a_pass_with_nothing_to_do_says_so() {
        let settings = config(vec![ActionKind::CgroupSoftLimit], Some(2 * GIB));
        let executor = FakeExecutor::confirming(0, 0);
        let mut coordinator = coordinator();
        let pass = execute_plan(
            &mut coordinator,
            &plan(Vec::new()),
            &settings,
            ManagementMode::Automatic,
            &[],
            &executor,
            NOW_MS,
            &Cancellation::new(),
            64 * GIB,
        );
        assert!(!pass.wrote());
        assert_eq!(pass.summary(), "the plan proposed no action");
    }

    #[test]
    fn a_confirmed_action_is_not_applied_twice() {
        let workloads = vec![workload("unit:build.service", 5, 6 * GIB)];
        let settings = config(vec![ActionKind::CgroupSoftLimit], Some(2 * GIB));
        let executor = FakeExecutor::confirming(6 * GIB, 4 * GIB);
        let mut coordinator = coordinator();
        let proposed = plan(vec![soft_limit("unit:build.service", 5)]);

        let first = execute_plan(
            &mut coordinator,
            &proposed,
            &settings,
            ManagementMode::Automatic,
            &workloads,
            &executor,
            NOW_MS,
            &Cancellation::new(),
            64 * GIB,
        );
        let second = execute_plan(
            &mut coordinator,
            &proposed,
            &settings,
            ManagementMode::Automatic,
            &workloads,
            &executor,
            NOW_MS + 1,
            &Cancellation::new(),
            64 * GIB,
        );

        assert_eq!(first.confirmed(), 1);
        assert_eq!(second.results[0].state, ActionState::SkippedDuplicate);
        assert_eq!(
            executor.applies.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the same logical action ran once"
        );
    }
}
