//! Replay the pressure policy over a scripted episode.
//!
//! One timeline drives the real state machine and the real planner through a
//! spike, a sustained episode that escalates to critical, a conflict between a
//! protected workload and a background one, an action that proves ineffective,
//! and a recovery back to health. Nothing is stubbed except the inputs.

use breadram_core::config::{
    ActionKind, Automation, Budget, Config, Priority, Protection, Rule, RuleMatch, Timing,
};
use breadram_core::inventory::{Workload, WorkloadId};
use breadram_core::policy::{
    plan, ActionOutcome, ActionRecord, ActivityEvidence, ActivitySource, Plan, PlanChannel,
    PlannerInput, PolicyHistory, PressureState, StateMachine,
};
mod support;

use support::{cgroup_workload, snapshot, GIB};

struct NoActivity;

impl ActivitySource for NoActivity {
    fn evidence(&self, _workload: &WorkloadId) -> ActivityEvidence {
        ActivityEvidence::Unknown
    }
}

/// The scenario: a protected editor, a background build, and a normal workload.
fn scenario() -> (Vec<Workload>, Config) {
    let workloads = vec![
        cgroup_workload("editor", "exe:editor", 20 * GIB),
        cgroup_workload("build", "exe:build", 9 * GIB),
        cgroup_workload("other", "exe:other", 4 * GIB),
    ];
    let config = Config {
        timing: Timing {
            settle_ms: 3_000,
            cooldown_ms: 1_000,
            min_dwell_ms: 10_000,
        },
        protection: Protection {
            protected: vec!["exe:editor".to_string()],
        },
        budgets: vec![
            Budget {
                scope: "exe:build".to_string(),
                max_bytes: GIB,
            },
            Budget {
                scope: "exe:other".to_string(),
                max_bytes: GIB,
            },
        ],
        rules: vec![Rule {
            name: "builders-are-background".to_string(),
            match_on: RuleMatch::Scope {
                value: "exe:build".to_string(),
            },
            priority: Priority::Background,
            budget_bytes: None,
        }],
        automation: Automation {
            allowed_actions: vec![ActionKind::CgroupSoftLimit],
        },
        ..Config::default()
    };
    (workloads, config)
}

struct Harness {
    machine: StateMachine,
    history: PolicyHistory,
    workloads: Vec<Workload>,
    config: Config,
    tick: u64,
}

impl Harness {
    fn new() -> Self {
        let (workloads, config) = scenario();
        Self {
            machine: StateMachine::new(&config),
            history: PolicyHistory::new(64, 60_000, 10),
            workloads,
            config,
            tick: 0,
        }
    }

    /// Advance the clock to `now_ms`, feed one observation, and plan against
    /// the state that results.
    fn step(&mut self, now_ms: u64, avg10: f64, available_fraction: f64) -> Plan {
        self.tick = now_ms;
        let observation = snapshot(now_ms, avg10, available_fraction);
        let state = self.machine.observe(&observation);
        let activity = NoActivity;
        plan(
            PlannerInput {
                state,
                snapshot: &observation,
                workloads: &self.workloads,
                config: &self.config,
                history: &self.history,
                activity: &activity,
            },
            PlanChannel::Automatic,
        )
    }

    fn state(&self) -> PressureState {
        self.machine.state()
    }

    fn record(&mut self, scope: &str, outcome: ActionOutcome) {
        self.history.record(ActionRecord {
            target: WorkloadId {
                scope: scope.to_string(),
                generation: 1,
            },
            kind: ActionKind::CgroupSoftLimit,
            at_ms: self.tick,
            outcome,
        });
    }
}

fn targeted(plan: &Plan) -> Vec<&str> {
    plan.actions
        .iter()
        .map(|action| action.target.scope.as_str())
        .collect()
}

#[test]
fn the_episode_replays_through_spike_pressure_ineffectiveness_and_recovery() {
    let mut harness = Harness::new();

    // Calm.
    let calm = harness.step(1_000, 1.0, 0.5);
    assert_eq!(harness.state(), PressureState::Healthy);
    assert!(calm.is_empty());

    // A single critical reading is a spike, not pressure: it does not hold long
    // enough to settle, so the state does not move and nothing is planned.
    let spike = harness.step(2_000, 40.0, 0.5);
    assert_eq!(harness.state(), PressureState::Healthy);
    assert!(spike.is_empty());

    harness.step(2_500, 1.0, 0.5);

    // Sustained stall. It has not settled yet.
    harness.step(4_000, 8.0, 0.4);
    assert_eq!(harness.state(), PressureState::Healthy);

    // Settled and past the minimum dwell: pressured, and the least important
    // workload is chosen. The protected editor is the largest and is still left
    // alone, and the background build is chosen ahead of the normal workload.
    let pressured = harness.step(12_000, 8.0, 0.4);
    assert_eq!(harness.state(), PressureState::Pressured);
    assert_eq!(targeted(&pressured), vec!["exe:build"]);
    assert!(pressured.actions[0].expected_host_ram_delta < 0);
    harness.record("exe:build", ActionOutcome::Ineffective);

    // The action did not help. The planner must not repeat it against the same
    // target; it escalates to the next candidate instead.
    let escalate = harness.step(14_000, 8.0, 0.4);
    assert_eq!(harness.state(), PressureState::Pressured);
    assert_eq!(targeted(&escalate), vec!["exe:other"]);
    assert!(escalate
        .explanation
        .iter()
        .any(|line| line.contains("already proved ineffective")));
    harness.record("exe:other", ActionOutcome::Succeeded);

    // Conditions worsen.
    harness.step(16_000, 30.0, 0.1);
    assert_eq!(harness.state(), PressureState::Pressured);

    let critical = harness.step(28_000, 30.0, 0.1);
    assert_eq!(harness.state(), PressureState::Critical);
    // The build is still skipped as ineffective, so the normal workload is used.
    assert_eq!(targeted(&critical), vec!["exe:other"]);
    assert!(
        !targeted(&critical).contains(&"exe:editor"),
        "the protected editor is never targeted"
    );

    // Recovery holds before declaring health.
    harness.step(29_000, 0.0, 0.5);
    assert_eq!(harness.state(), PressureState::Critical);
    let recovering = harness.step(40_000, 0.0, 0.5);
    assert_eq!(harness.state(), PressureState::Recovering);
    assert!(recovering.is_empty());
    harness.step(41_000, 0.0, 0.5);
    assert_eq!(harness.state(), PressureState::Recovering);

    let healthy = harness.step(52_000, 0.0, 0.5);
    assert_eq!(harness.state(), PressureState::Healthy);
    assert!(healthy.is_empty());
}

#[test]
fn an_identical_episode_produces_an_identical_plan() {
    let replay = || {
        let mut harness = Harness::new();
        harness.step(1_000, 1.0, 0.5);
        harness.step(4_000, 8.0, 0.4);
        let first = harness.step(12_000, 8.0, 0.4);
        harness.record("exe:build", ActionOutcome::Ineffective);
        let second = harness.step(14_000, 8.0, 0.4);
        (first, second)
    };

    assert_eq!(replay(), replay());
}
