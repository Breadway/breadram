//! Pressure policy: the state machine and the action planner.
//!
//! This module is pure logic. It reads a telemetry snapshot, the workload
//! inventory, the configuration, and a bounded action history, and produces a
//! plan plus an explanation. It performs no side effects and holds no handles to
//! the system, so monitor mode, dry runs, and replay all exercise exactly the
//! same code path that automatic mode would.
//!
//! Two rules are structural here rather than conventional:
//!
//! 1. The planner can only ever propose a [`crate::config::ActionKind`]. That
//!    enum has no kill, cache drop, or global virtual memory setting variant, so
//!    routine policy cannot reach for one.
//! 2. Any proposed action that would increase host memory is discarded while the
//!    host is under pressure, which includes provider offload actions that move
//!    device memory into CPU RAM.

pub mod activity;
pub mod history;
pub mod planner;
pub mod state;

pub use activity::{ActivityEvidence, ActivitySource, DebouncedActivity, UnknownActivity};
pub use history::{ActionOutcome, ActionRecord, PolicyHistory};
pub use planner::{
    budget_for, explain_priority, is_action_protected, plan, Plan, PlanChannel, PlannedAction,
    PlannerInput, MAX_ACTIONS_CRITICAL, MAX_ACTIONS_PRESSURED, MIN_IDLE_MS,
};
pub use state::{PressureState, StateMachine, RELEASE_FRACTION};
