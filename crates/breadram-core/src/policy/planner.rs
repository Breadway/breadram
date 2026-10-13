//! The action planner.
//!
//! Given a pressure state, a snapshot, the workload inventory, validated
//! configuration, a bounded action history, and activity evidence, the planner
//! returns a [`Plan`]: at most a few proposed actions plus a line by line
//! explanation of every decision, including the ones it declined.
//!
//! The planner is a pure function. It holds no system handles and mutates
//! nothing, so monitor mode and dry runs run this exact code and simply discard
//! the actions while keeping the explanation. That is why the channel only
//! affects [`Plan::would_execute`] and never the reasoning.
//!
//! Four gates are applied to every proposed action before it can be planned:
//!
//! 1. It must be in the configured allowed set.
//! 2. It must not be expected to raise host memory while the host is pressured.
//! 3. If it needs confirmed idleness, the idle evidence must be present and have
//!    held long enough.
//! 4. It must not be cooling down, rate limited, or already known to be
//!    ineffective against this target.

use crate::config::{
    priority_with_source, resolve_priority, select_rule, ActionKind, Config, Priority,
};
use crate::inventory::{ControlLevel, Workload, WorkloadId};
use crate::policy::activity::{ActivityEvidence, ActivitySource};
use crate::policy::history::PolicyHistory;
use crate::policy::state::PressureState;
use crate::telemetry::MemorySnapshot;

/// How long a workload must have been idle before its memory may be released.
///
/// This is what stops a momentary gap in activity, or a missing reading, from
/// being treated as idleness. Missing evidence is never idle at any duration.
pub const MIN_IDLE_MS: u64 = 30_000;

/// How many actions a single plan may contain while pressured.
pub const MAX_ACTIONS_PRESSURED: usize = 1;
/// How many actions a single plan may contain while critical.
pub const MAX_ACTIONS_CRITICAL: usize = 2;

/// Which channel asked for the plan.
///
/// This never changes the plan or its explanation. It only records whether the
/// caller is permitted to carry the actions out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanChannel {
    /// Observation only: explain, never execute.
    Monitor,
    /// Explain and show what automatic mode would do: never execute.
    DryRun,
    /// Explain and, if the daemon is in automatic mode, execute.
    Automatic,
}

/// One proposed action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedAction {
    /// The workload the action targets.
    pub target: WorkloadId,
    /// What would be done.
    pub kind: ActionKind,
    /// Why, in words, for the explanation.
    pub reason: String,
    /// The expected change in host memory in bytes. Negative means relief.
    pub expected_host_ram_delta: i64,
    /// Whether the action needs proven idleness before it may run.
    pub requires_confirmed_idle: bool,
}

/// The result of one planning pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// The pressure state the plan was made in.
    pub state: PressureState,
    /// The channel that asked for it.
    pub channel: PlanChannel,
    /// Whether the caller is permitted to execute the actions.
    pub would_execute: bool,
    /// Proposed actions, most relief first, within the escalation bound.
    pub actions: Vec<PlannedAction>,
    /// The reasoning, including every declined action and why.
    pub explanation: Vec<String>,
}

impl Plan {
    /// Whether the plan proposes nothing.
    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }
}

/// The inputs to a planning pass.
#[derive(Clone, Copy)]
pub struct PlannerInput<'a> {
    /// The current pressure state.
    pub state: PressureState,
    /// The observation the plan is made against.
    pub snapshot: &'a MemorySnapshot,
    /// Discovered workloads.
    pub workloads: &'a [Workload],
    /// Validated configuration.
    pub config: &'a Config,
    /// Bounded action history.
    pub history: &'a PolicyHistory,
    /// Activity evidence for the candidate workloads.
    pub activity: &'a dyn ActivitySource,
}

/// Build a plan.
pub fn plan(input: PlannerInput<'_>, channel: PlanChannel) -> Plan {
    let PlannerInput {
        state,
        snapshot,
        workloads,
        config,
        history,
        activity,
    } = input;
    let now = snapshot.observed_at_ms;
    let mut explanation = Vec::new();

    if !state.is_under_pressure() {
        explanation.push(format!("the host is {state}, no actions are planned"));
        return finish(state, channel, Vec::new(), explanation);
    }

    if history.rate_limited(now) {
        explanation.push(format!(
            "rate limited: {} actions in the recent window, holding",
            history.actions_in_window(now)
        ));
        return finish(state, channel, Vec::new(), explanation);
    }

    let max_actions = match state {
        PressureState::Critical => MAX_ACTIONS_CRITICAL,
        _ => MAX_ACTIONS_PRESSURED,
    };

    let mut actions: Vec<PlannedAction> = Vec::new();
    for workload in ordered_candidates(workloads, config, activity) {
        if actions.len() >= max_actions {
            explanation.push(format!(
                "escalation bound of {max_actions} actions reached for {state}"
            ));
            break;
        }
        let scope = workload.id.scope.clone();

        // A live activity hint protects the workload even without a rule: work
        // that is actually running must not be reclaimed, and the reason is
        // explained rather than silently dropped.
        let (effective, source) = explain_priority(
            config,
            workload,
            activity,
            serves_protected(config, workload, workloads),
        );
        if effective == Priority::Protected {
            explanation.push(format!(
                "{scope}: {source}, so it is protected from action; activity is not idle"
            ));
            continue;
        }

        if history.is_cooling_down(&workload.id, now, config.timing.cooldown_ms) {
            explanation.push(format!("{scope}: in cooldown, skipped"));
            continue;
        }

        let marker = explanation.len();
        for action in generate(workload, config) {
            if action.expected_host_ram_delta > 0 {
                explanation.push(format!(
                    "{scope}: {:?} is expected to raise host memory during pressure, refused",
                    action.kind
                ));
                continue;
            }
            if action.requires_confirmed_idle
                && !activity
                    .evidence(&workload.id)
                    .is_confirmed_idle(now, MIN_IDLE_MS)
            {
                explanation.push(format!(
                    "{scope}: {:?} needs confirmed idleness and none is proven, skipped",
                    action.kind
                ));
                continue;
            }
            if history.ineffective_count(&workload.id, action.kind) > 0 {
                explanation.push(format!(
                    "{scope}: {:?} already proved ineffective here, escalating past it",
                    action.kind
                ));
                continue;
            }
            explanation.push(format!(
                "{scope}: planned {:?} to {}",
                action.kind, action.reason
            ));
            actions.push(action);
            break;
        }

        if explanation.len() == marker {
            explanation.push(format!("{scope}: no eligible action for its current state"));
        }
    }

    if actions.is_empty() {
        explanation.push("nothing eligible was planned, remaining in observation".to_string());
    }

    finish(state, channel, actions, explanation)
}

fn finish(
    state: PressureState,
    channel: PlanChannel,
    actions: Vec<PlannedAction>,
    explanation: Vec<String>,
) -> Plan {
    Plan {
        state,
        channel,
        would_execute: channel == PlanChannel::Automatic,
        actions,
        explanation,
    }
}

/// The eligible candidates, least important and largest first.
///
/// Ordering is deterministic: background before normal, then the largest
/// current usage, then the scope key, so identical inputs always produce an
/// identical plan.
fn ordered_candidates<'a>(
    workloads: &'a [Workload],
    config: &Config,
    activity: &dyn ActivitySource,
) -> Vec<&'a Workload> {
    let mut candidates: Vec<&Workload> = workloads
        .iter()
        .filter(|workload| workload.control == ControlLevel::Eligible)
        .filter(|workload| !is_action_protected(config, workload))
        .collect();
    candidates.sort_by(|left, right| {
        let (left_priority, _) = explain_priority(
            config,
            left,
            activity,
            serves_protected(config, left, workloads),
        );
        let (right_priority, _) = explain_priority(
            config,
            right,
            activity,
            serves_protected(config, right, workloads),
        );
        priority_rank(left_priority)
            .cmp(&priority_rank(right_priority))
            .then_with(|| usage(right).cmp(&usage(left)))
            .then_with(|| left.id.scope.cmp(&right.id.scope))
    });
    candidates
}

/// The activity hint for a workload, or `None` when nothing proves it active.
///
/// Only real `Active` evidence is a hint. Unknown and idle evidence is not, so a
/// missing reading can never demote or protect a workload.
fn active_hint(activity: &dyn ActivitySource, workload: &Workload) -> Option<&'static str> {
    match activity.evidence(&workload.id) {
        ActivityEvidence::Active => Some("active workload evidence"),
        ActivityEvidence::Idle { .. } | ActivityEvidence::Unknown => None,
    }
}

/// Explain a workload's effective priority and why it applies.
///
/// Explicit rules win, then a dependency of a protected workload, then a live
/// activity hint, then the standalone normal default. A missing hint is never
/// read as idle, and a live hint always protects the workload from action.
pub fn explain_priority(
    config: &Config,
    workload: &Workload,
    activity: &dyn ActivitySource,
    serves_protected: bool,
) -> (Priority, String) {
    priority_with_source(
        &config.rules,
        workload,
        active_hint(activity, workload),
        serves_protected,
    )
}

/// Whether a workload serves a protected workload, inferred from cgroup nesting.
///
/// A workload whose cgroup path is an ancestor of a protected workload's cgroup
/// path is serving it, so it is protected too rather than being reclaimed out
/// from under the thing it serves. This is read from the cgroup tree the
/// inventory already collected, not guessed: it holds only for `cgroup:` scopes,
/// where nesting is real.
pub fn serves_protected(config: &Config, workload: &Workload, workloads: &[Workload]) -> bool {
    let Some(base) = workload.id.scope.strip_prefix("cgroup:") else {
        return false;
    };
    let prefix = format!("{base}/");
    workloads.iter().any(|other| {
        is_action_protected(config, other)
            && other
                .id
                .scope
                .strip_prefix("cgroup:")
                .is_some_and(|path| path.starts_with(&prefix))
    })
}

/// Whether a workload is shielded from automatic action entirely.
pub fn is_action_protected(config: &Config, workload: &Workload) -> bool {
    config
        .protection
        .protected
        .iter()
        .any(|scope| scope == &workload.id.scope)
        || resolve_priority(&config.rules, workload) == Priority::Protected
}

/// The bounded memory budget for a workload, if one is configured.
///
/// An explicit budget for the scope wins over a rule's budget.
pub fn budget_for(config: &Config, workload: &Workload) -> Option<u64> {
    if let Some(budget) = config
        .budgets
        .iter()
        .find(|budget| budget.scope == workload.id.scope)
    {
        return Some(budget.max_bytes);
    }
    select_rule(&config.rules, workload).and_then(|rule| rule.budget_bytes)
}

/// The candidate actions for one workload, most relief first.
///
/// Every action here is a configured [`ActionKind`]. That enum carries no kill,
/// cache drop, or global virtual memory setting, so the planner cannot propose
/// a destructive operation even by mistake. `ReclaimProtection` is deliberately
/// not generated here: it is applied by the coordinator alongside a limit rather
/// than as a standalone relief action.
fn generate(workload: &Workload, config: &Config) -> Vec<PlannedAction> {
    let allowed = &config.automation.allowed_actions;
    let bytes = usage(workload);
    let mut actions = Vec::new();

    if allowed.contains(&ActionKind::UnloadIdleModel) && bytes > 0 {
        actions.push(PlannedAction {
            target: workload.id.clone(),
            kind: ActionKind::UnloadIdleModel,
            reason: "the workload has been idle long enough to release its model memory"
                .to_string(),
            expected_host_ram_delta: relief(bytes),
            requires_confirmed_idle: true,
        });
    }

    if allowed.contains(&ActionKind::CgroupSoftLimit) {
        if let Some(budget) = budget_for(config, workload) {
            if bytes > budget {
                let over = bytes - budget;
                actions.push(PlannedAction {
                    target: workload.id.clone(),
                    kind: ActionKind::CgroupSoftLimit,
                    reason: format!(
                        "it is {over} bytes over its {budget} byte budget, a bounded limit reclaims that"
                    ),
                    expected_host_ram_delta: relief(over),
                    requires_confirmed_idle: false,
                });
            }
        }
    }

    if allowed.contains(&ActionKind::ReduceJobConcurrency) && bytes > 0 {
        // Halving the workers halves the live memory the job holds for work:
        // the executor writes half the current count, so the plan promises
        // half the usage rather than an unrelated fraction. The estimate the
        // job was admitted under stays the reservation; this is the relief a
        // reduce is expected to free from it.
        actions.push(PlannedAction {
            target: workload.id.clone(),
            kind: ActionKind::ReduceJobConcurrency,
            reason: "it can continue at lower concurrency with less live memory".to_string(),
            expected_host_ram_delta: relief(bytes / 2),
            requires_confirmed_idle: false,
        });
    }

    if allowed.contains(&ActionKind::QueueBackgroundJob) && config.jobs.enabled {
        actions.push(PlannedAction {
            target: workload.id.clone(),
            kind: ActionKind::QueueBackgroundJob,
            reason: "further background admission can wait".to_string(),
            expected_host_ram_delta: 0,
            requires_confirmed_idle: false,
        });
    }

    if allowed.contains(&ActionKind::SleepModel) && bytes > 0 {
        actions.push(PlannedAction {
            target: workload.id.clone(),
            kind: ActionKind::SleepModel,
            reason: "provider offload frees device memory by moving it into host RAM".to_string(),
            expected_host_ram_delta: cost(bytes),
            requires_confirmed_idle: true,
        });
    }

    actions
}

fn usage(workload: &Workload) -> u64 {
    workload.accounting.bytes().unwrap_or(0)
}

fn priority_rank(priority: Priority) -> u8 {
    match priority {
        Priority::Background => 0,
        Priority::Normal => 1,
        Priority::Protected => 2,
    }
}

/// A negative byte delta that saturates instead of overflowing.
fn relief(bytes: u64) -> i64 {
    -i64::try_from(bytes).unwrap_or(i64::MAX)
}

/// A positive byte delta that saturates instead of overflowing.
fn cost(bytes: u64) -> i64 {
    i64::try_from(bytes).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::capability::CapabilityReport;
    use crate::config::{Rule, RuleMatch};
    use crate::inventory::MemoryAccounting;
    use crate::policy::activity::ActivityEvidence;
    use crate::policy::history::{ActionOutcome, ActionRecord};
    use crate::telemetry::{MemInfo, PsiLine, PsiReading, VmStat};

    const GIB: u64 = 1024 * 1024 * 1024;
    const TOTAL: u64 = 100 * GIB;
    const NOW: u64 = 1_000_000;

    #[derive(Default)]
    struct FixedActivity(HashMap<String, ActivityEvidence>);

    impl FixedActivity {
        fn idle(scope: &str, since_ms: u64) -> Self {
            let mut map = HashMap::new();
            map.insert(scope.to_string(), ActivityEvidence::Idle { since_ms });
            Self(map)
        }

        fn active(scope: &str) -> Self {
            let mut map = HashMap::new();
            map.insert(scope.to_string(), ActivityEvidence::Active);
            Self(map)
        }
    }

    impl ActivitySource for FixedActivity {
        fn evidence(&self, workload: &WorkloadId) -> ActivityEvidence {
            self.0.get(&workload.scope).copied().unwrap_or_default()
        }
    }

    fn snapshot() -> MemorySnapshot {
        MemorySnapshot {
            observed_at_ms: NOW,
            meminfo: MemInfo {
                total: TOTAL,
                free: GIB,
                available: Some(8 * GIB),
                buffers: 0,
                cached: 0,
                swap_total: 0,
                swap_free: 0,
                anon: None,
                shmem: None,
            },
            psi: Some(PsiReading {
                some: PsiLine {
                    avg10: 30.0,
                    avg60: 30.0,
                    avg300: 30.0,
                    total_us: 0,
                },
                full: None,
            }),
            vmstat: VmStat::default(),
            capabilities: CapabilityReport::new(),
        }
    }

    fn workload(name: &str, scope: &str, bytes: u64) -> Workload {
        Workload {
            id: WorkloadId {
                scope: scope.to_string(),
                generation: 1,
            },
            name: name.to_string(),
            pids: vec![100],
            accounting: MemoryAccounting::Cgroup {
                current_bytes: Some(bytes),
            },
            control: ControlLevel::Eligible,
            limit_bytes: None,
        }
    }

    fn config(actions: &[ActionKind]) -> Config {
        Config {
            automation: crate::config::Automation {
                allowed_actions: actions.to_vec(),
            },
            ..Config::default()
        }
    }

    fn empty_history() -> PolicyHistory {
        PolicyHistory::new(64, 60_000, 10)
    }

    fn run(
        state: PressureState,
        workloads: &[Workload],
        config: &Config,
        history: &PolicyHistory,
        activity: &dyn ActivitySource,
    ) -> Plan {
        let snapshot = snapshot();
        plan(
            PlannerInput {
                state,
                snapshot: &snapshot,
                workloads,
                config,
                history,
                activity,
            },
            PlanChannel::Automatic,
        )
    }

    #[test]
    fn a_healthy_host_plans_nothing() {
        let plans = run(
            PressureState::Healthy,
            &[workload("build", "exe:build", 8 * GIB)],
            &config(&[ActionKind::CgroupSoftLimit]),
            &empty_history(),
            &FixedActivity::default(),
        );
        assert!(plans.is_empty());
        assert_eq!(
            plans.explanation,
            vec!["the host is healthy, no actions are planned".to_string()]
        );
    }

    #[test]
    fn a_recovering_host_holds_off() {
        let plans = run(
            PressureState::Recovering,
            &[workload("build", "exe:build", 8 * GIB)],
            &config(&[ActionKind::CgroupSoftLimit]),
            &empty_history(),
            &FixedActivity::default(),
        );
        assert!(plans.is_empty());
    }

    #[test]
    fn the_least_important_workload_is_chosen_first() {
        let config = Config {
            automation: crate::config::Automation {
                allowed_actions: vec![ActionKind::CgroupSoftLimit],
            },
            budgets: vec![
                crate::config::Budget {
                    scope: "exe:big".to_string(),
                    max_bytes: GIB,
                },
                crate::config::Budget {
                    scope: "exe:small".to_string(),
                    max_bytes: GIB,
                },
            ],
            rules: vec![crate::config::Rule {
                name: "protect-the-editor".to_string(),
                match_on: RuleMatch::Scope {
                    value: "exe:editor".to_string(),
                },
                priority: Priority::Protected,
                budget_bytes: None,
            }],
            ..Config::default()
        };
        let workloads = vec![
            workload("editor", "exe:editor", 20 * GIB),
            workload("small", "exe:small", 2 * GIB),
            workload("big", "exe:big", 9 * GIB),
        ];
        let plans = run(
            PressureState::Critical,
            &workloads,
            &config,
            &empty_history(),
            &FixedActivity::default(),
        );
        // Two actions in critical, and the protected editor is never targeted.
        assert_eq!(plans.actions.len(), 2);
        let scopes: Vec<&str> = plans
            .actions
            .iter()
            .map(|action| action.target.scope.as_str())
            .collect();
        assert_eq!(scopes, vec!["exe:big", "exe:small"]);
        assert!(
            !scopes.contains(&"exe:editor"),
            "a protected workload is never targeted"
        );
    }

    #[test]
    fn a_protected_workload_is_never_targeted() {
        let config = Config {
            protection: crate::config::Protection {
                protected: vec!["exe:build".to_string()],
            },
            automation: crate::config::Automation {
                allowed_actions: vec![ActionKind::CgroupSoftLimit],
            },
            budgets: vec![crate::config::Budget {
                scope: "exe:build".to_string(),
                max_bytes: GIB,
            }],
            ..Config::default()
        };
        let plans = run(
            PressureState::Critical,
            &[workload("build", "exe:build", 40 * GIB)],
            &config,
            &empty_history(),
            &FixedActivity::default(),
        );
        assert!(plans.is_empty());
    }

    #[test]
    fn releasing_idle_memory_needs_proven_idleness() {
        let config = config(&[ActionKind::UnloadIdleModel]);
        let workloads = vec![workload("model", "unit:ollama.service", 12 * GIB)];

        // No evidence at all.
        let unknown = run(
            PressureState::Critical,
            &workloads,
            &config,
            &empty_history(),
            &FixedActivity::default(),
        );
        assert!(unknown.is_empty());
        assert!(unknown
            .explanation
            .iter()
            .any(|line| line.contains("none is proven")));

        // Idle, but not for long enough.
        let fresh = run(
            PressureState::Critical,
            &workloads,
            &config,
            &empty_history(),
            &FixedActivity::idle("unit:ollama.service", NOW - 1_000),
        );
        assert!(fresh.is_empty());

        // Idle for long enough, so it is planned.
        let settled = run(
            PressureState::Critical,
            &workloads,
            &config,
            &empty_history(),
            &FixedActivity::idle("unit:ollama.service", NOW - MIN_IDLE_MS - 1),
        );
        assert_eq!(settled.actions.len(), 1);
        assert_eq!(settled.actions[0].kind, ActionKind::UnloadIdleModel);
        assert!(settled.actions[0].expected_host_ram_delta < 0);
    }

    #[test]
    fn an_active_workload_is_never_released() {
        let mut activity = HashMap::new();
        activity.insert("unit:ollama.service".to_string(), ActivityEvidence::Active);
        let plans = run(
            PressureState::Critical,
            &[workload("model", "unit:ollama.service", 12 * GIB)],
            &config(&[ActionKind::UnloadIdleModel]),
            &empty_history(),
            &FixedActivity(activity),
        );
        assert!(plans.is_empty());
    }

    #[test]
    fn provider_offload_is_refused_while_the_host_is_pressured() {
        // The only permitted action moves model memory into host RAM, so the
        // planner must refuse it and say why.
        let plans = run(
            PressureState::Critical,
            &[workload("model", "unit:vllm.service", 20 * GIB)],
            &config(&[ActionKind::SleepModel]),
            &empty_history(),
            &FixedActivity::idle("unit:vllm.service", 0),
        );
        assert!(plans.is_empty());
        assert!(plans
            .explanation
            .iter()
            .any(|line| line.contains("raise host memory during pressure")));
    }

    #[test]
    fn cooldown_skips_a_recently_acted_target() {
        let workloads = vec![workload("build", "exe:build", 9 * GIB)];
        let config = Config {
            automation: crate::config::Automation {
                allowed_actions: vec![ActionKind::CgroupSoftLimit],
            },
            budgets: vec![crate::config::Budget {
                scope: "exe:build".to_string(),
                max_bytes: GIB,
            }],
            ..Config::default()
        };
        let mut history = empty_history();
        history.record(ActionRecord {
            target: workloads[0].id.clone(),
            kind: ActionKind::CgroupSoftLimit,
            at_ms: NOW - 1_000,
            outcome: ActionOutcome::Succeeded,
        });
        let plans = run(
            PressureState::Critical,
            &workloads,
            &config,
            &history,
            &FixedActivity::default(),
        );
        assert!(plans.is_empty());
        assert!(plans
            .explanation
            .iter()
            .any(|line| line.contains("in cooldown")));
    }

    #[test]
    fn the_rate_limit_stops_all_new_actions() {
        let workloads = vec![workload("build", "exe:build", 9 * GIB)];
        let config = Config {
            automation: crate::config::Automation {
                allowed_actions: vec![ActionKind::CgroupSoftLimit],
            },
            budgets: vec![crate::config::Budget {
                scope: "exe:build".to_string(),
                max_bytes: GIB,
            }],
            ..Config::default()
        };
        let mut history = PolicyHistory::new(64, 60_000, 1);
        history.record(ActionRecord {
            target: WorkloadId {
                scope: "exe:other".to_string(),
                generation: 1,
            },
            kind: ActionKind::CgroupSoftLimit,
            at_ms: NOW - 500,
            outcome: ActionOutcome::Succeeded,
        });
        let plans = run(
            PressureState::Critical,
            &workloads,
            &config,
            &history,
            &FixedActivity::default(),
        );
        assert!(plans.is_empty());
        assert!(plans
            .explanation
            .iter()
            .any(|line| line.contains("rate limited")));
    }

    #[test]
    fn escalation_is_bounded_by_state() {
        let config = Config {
            automation: crate::config::Automation {
                allowed_actions: vec![ActionKind::CgroupSoftLimit],
            },
            budgets: ["a", "b", "c"]
                .iter()
                .map(|name| crate::config::Budget {
                    scope: format!("exe:{name}"),
                    max_bytes: GIB,
                })
                .collect(),
            ..Config::default()
        };
        let workloads: Vec<Workload> = ["a", "b", "c"]
            .iter()
            .map(|name| workload(name, &format!("exe:{name}"), 9 * GIB))
            .collect();

        let pressured = run(
            PressureState::Pressured,
            &workloads,
            &config,
            &empty_history(),
            &FixedActivity::default(),
        );
        assert_eq!(pressured.actions.len(), MAX_ACTIONS_PRESSURED);

        let critical = run(
            PressureState::Critical,
            &workloads,
            &config,
            &empty_history(),
            &FixedActivity::default(),
        );
        assert_eq!(critical.actions.len(), MAX_ACTIONS_CRITICAL);
        assert!(critical
            .explanation
            .iter()
            .any(|line| line.contains("escalation bound")));
    }

    #[test]
    fn an_ineffective_action_is_not_repeated() {
        let workloads = vec![workload("build", "exe:build", 9 * GIB)];
        let config = Config {
            automation: crate::config::Automation {
                allowed_actions: vec![
                    ActionKind::CgroupSoftLimit,
                    ActionKind::ReduceJobConcurrency,
                ],
            },
            budgets: vec![crate::config::Budget {
                scope: "exe:build".to_string(),
                max_bytes: GIB,
            }],
            ..Config::default()
        };
        let mut history = empty_history();
        history.record(ActionRecord {
            target: workloads[0].id.clone(),
            kind: ActionKind::CgroupSoftLimit,
            at_ms: NOW - 600_000,
            outcome: ActionOutcome::Ineffective,
        });
        let plans = run(
            PressureState::Critical,
            &workloads,
            &config,
            &history,
            &FixedActivity::default(),
        );
        assert_eq!(plans.actions.len(), 1);
        assert_eq!(plans.actions[0].kind, ActionKind::ReduceJobConcurrency);
        assert!(plans
            .explanation
            .iter()
            .any(|line| line.contains("already proved ineffective")));
    }

    #[test]
    fn monitor_and_dry_run_explain_identically_and_execute_nothing() {
        let workloads = vec![workload("build", "exe:build", 9 * GIB)];
        let config = Config {
            automation: crate::config::Automation {
                allowed_actions: vec![ActionKind::CgroupSoftLimit],
            },
            budgets: vec![crate::config::Budget {
                scope: "exe:build".to_string(),
                max_bytes: GIB,
            }],
            ..Config::default()
        };
        let snapshot = snapshot();
        let history = empty_history();
        let activity = FixedActivity::default();
        let build = |channel| {
            plan(
                PlannerInput {
                    state: PressureState::Critical,
                    snapshot: &snapshot,
                    workloads: &workloads,
                    config: &config,
                    history: &history,
                    activity: &activity,
                },
                channel,
            )
        };

        let monitor = build(PlanChannel::Monitor);
        let dry_run = build(PlanChannel::DryRun);
        let automatic = build(PlanChannel::Automatic);

        assert_eq!(monitor.explanation, dry_run.explanation);
        assert_eq!(monitor.explanation, automatic.explanation);
        assert_eq!(monitor.actions, automatic.actions);
        assert!(!monitor.would_execute);
        assert!(!dry_run.would_execute);
        assert!(automatic.would_execute);
    }

    #[test]
    fn planning_does_not_mutate_its_inputs() {
        let workloads = vec![workload("build", "exe:build", 9 * GIB)];
        let config = Config {
            automation: crate::config::Automation {
                allowed_actions: vec![ActionKind::CgroupSoftLimit],
            },
            budgets: vec![crate::config::Budget {
                scope: "exe:build".to_string(),
                max_bytes: GIB,
            }],
            ..Config::default()
        };
        let snapshot = snapshot();
        let mut history = empty_history();
        history.record(ActionRecord {
            target: workloads[0].id.clone(),
            kind: ActionKind::CgroupSoftLimit,
            at_ms: NOW - 600_000,
            outcome: ActionOutcome::Succeeded,
        });

        let config_before = config.clone();
        let snapshot_before = snapshot.clone();
        let history_before = history.clone();
        let workloads_before = workloads.clone();

        let _ = run(
            PressureState::Critical,
            &workloads,
            &config,
            &history,
            &FixedActivity::default(),
        );

        assert_eq!(config, config_before);
        assert_eq!(snapshot, snapshot_before);
        assert_eq!(history, history_before);
        assert_eq!(workloads, workloads_before);
    }

    #[test]
    fn the_action_vocabulary_has_no_destructive_operation() {
        // An exhaustive match over the enum: adding a kill, a global cache drop,
        // or a global virtual memory setting variant to ActionKind fails to
        // compile here, which is the guard that routine policy cannot reach one.
        let producible = [
            ActionKind::CgroupSoftLimit,
            ActionKind::ReclaimProtection,
            ActionKind::UnloadIdleModel,
            ActionKind::SleepModel,
            ActionKind::ReduceJobConcurrency,
            ActionKind::QueueBackgroundJob,
        ];
        for kind in producible {
            match kind {
                ActionKind::CgroupSoftLimit
                | ActionKind::ReclaimProtection
                | ActionKind::UnloadIdleModel
                | ActionKind::SleepModel
                | ActionKind::ReduceJobConcurrency
                | ActionKind::QueueBackgroundJob => {}
            }
        }
    }

    #[test]
    fn no_configured_action_means_no_plan() {
        let plans = run(
            PressureState::Critical,
            &[workload("build", "exe:build", 9 * GIB)],
            &config(&[]),
            &empty_history(),
            &FixedActivity::default(),
        );
        assert!(plans.is_empty());
        assert!(plans
            .explanation
            .iter()
            .any(|line| line.contains("remaining in observation")));
    }

    #[test]
    fn a_live_activity_hint_protects_a_workload_with_no_rule() {
        let plans = run(
            PressureState::Critical,
            &[workload("build", "exe:build", 9 * GIB)],
            &config(&[ActionKind::CgroupSoftLimit]),
            &empty_history(),
            &FixedActivity::active("exe:build"),
        );
        assert!(
            plans.is_empty(),
            "a workload with live activity must not be reclaimed: {:?}",
            plans.explanation
        );
    }

    #[test]
    fn conflicting_rules_pick_the_most_specific_and_outrank_a_hint() {
        // Two rules match the same workload. The more specific wins, and either
        // beats a live activity hint, which is the precedence the card asks for.
        let w = workload("build", "exe:build", 8 * GIB);
        let mut config = config(&[]);
        config.rules = vec![
            Rule {
                name: "broad".into(),
                match_on: RuleMatch::Executable {
                    value: "build".into(),
                },
                priority: Priority::Protected,
                budget_bytes: None,
            },
            Rule {
                name: "exact".into(),
                match_on: RuleMatch::Scope {
                    value: "exe:build".into(),
                },
                priority: Priority::Background,
                budget_bytes: None,
            },
        ];
        let (priority, source) =
            explain_priority(&config, &w, &FixedActivity::active("exe:build"), false);
        assert_eq!(
            priority,
            Priority::Background,
            "the more specific rule wins"
        );
        assert!(source.starts_with("explicit rule"));
    }

    #[test]
    fn a_workload_serving_a_protected_cgroup_is_protected() {
        // A parent cgroup whose nested workload is protected is serving it, so
        // it is protected too rather than being reclaimed out from under it. This
        // is read from the cgroup tree, not guessed.
        let parent = workload("parent", "cgroup:/app", 8 * GIB);
        let child = workload("child", "cgroup:/app/protected", GIB);
        let mut config = config(&[ActionKind::CgroupSoftLimit]);
        config.protection.protected = vec!["cgroup:/app/protected".to_string()];
        let workloads = vec![parent.clone(), child.clone()];

        assert!(serves_protected(&config, &parent, &workloads));
        assert!(!serves_protected(&config, &child, &workloads));

        let (priority, source) = explain_priority(
            &config,
            &parent,
            &FixedActivity::default(),
            serves_protected(&config, &parent, &workloads),
        );
        assert_eq!(priority, Priority::Protected);
        assert!(
            source.contains("dependency"),
            "the source names it: {source}"
        );

        // And the plan never reclaims the parent.
        let plans = run(
            PressureState::Critical,
            &workloads,
            &config,
            &empty_history(),
            &FixedActivity::default(),
        );
        assert!(plans
            .actions
            .iter()
            .all(|action| action.target != parent.id));
    }
}
