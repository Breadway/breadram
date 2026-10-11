//! Bounded action history used for cooldowns, rate limits, and backoff.
//!
//! The history is a fixed size ring buffer, so a long running daemon cannot grow
//! it without bound. It answers three questions the planner needs: has this
//! target been acted on recently, are we acting too often overall, and has a
//! particular action already proved ineffective for a target.

use std::collections::VecDeque;

use crate::config::ActionKind;
use crate::inventory::WorkloadId;

/// What happened after an action was planned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionOutcome {
    /// Planned but not yet executed.
    Planned,
    /// Executed and confirmed.
    Succeeded,
    /// Execution failed or was refused.
    Failed,
    /// Executed and confirmed, but the pressure did not improve.
    Ineffective,
}

/// One entry in the history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionRecord {
    /// The workload the action targeted.
    pub target: WorkloadId,
    /// The action taken.
    pub kind: ActionKind,
    /// When it happened, in monotonic milliseconds.
    pub at_ms: u64,
    /// The outcome.
    pub outcome: ActionOutcome,
}

/// A bounded record of past actions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyHistory {
    records: VecDeque<ActionRecord>,
    max_records: usize,
    rate_window_ms: u64,
    max_actions_per_window: usize,
}

impl PolicyHistory {
    /// Create a history bounded to `max_records` entries, rate limiting to
    /// `max_actions_per_window` actions within `rate_window_ms`.
    pub fn new(max_records: usize, rate_window_ms: u64, max_actions_per_window: usize) -> Self {
        Self {
            records: VecDeque::with_capacity(max_records.min(64)),
            max_records: max_records.max(1),
            rate_window_ms,
            max_actions_per_window,
        }
    }

    /// Add a record, dropping the oldest if the buffer is full.
    pub fn record(&mut self, record: ActionRecord) {
        if self.records.len() == self.max_records {
            self.records.pop_front();
        }
        self.records.push_back(record);
    }

    /// All records, oldest first.
    pub fn records(&self) -> &VecDeque<ActionRecord> {
        &self.records
    }

    /// How many entries are retained.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether nothing has been recorded.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// The most recent time a target was acted on, if any.
    pub fn last_action_at(&self, target: &WorkloadId) -> Option<u64> {
        self.records
            .iter()
            .filter(|record| &record.target == target)
            .map(|record| record.at_ms)
            .max()
    }

    /// Whether a target is still inside its cooldown.
    pub fn is_cooling_down(&self, target: &WorkloadId, now_ms: u64, cooldown_ms: u64) -> bool {
        match self.last_action_at(target) {
            Some(at) => now_ms.saturating_sub(at) < cooldown_ms,
            None => false,
        }
    }

    /// How many actions were taken within the rate window ending at `now_ms`.
    pub fn actions_in_window(&self, now_ms: u64) -> usize {
        self.records
            .iter()
            .filter(|record| now_ms.saturating_sub(record.at_ms) < self.rate_window_ms)
            .count()
    }

    /// Whether the overall rate limit is currently reached.
    pub fn rate_limited(&self, now_ms: u64) -> bool {
        self.actions_in_window(now_ms) >= self.max_actions_per_window
    }

    /// How many times an action of this kind already proved ineffective for a
    /// target, which is the signal to escalate rather than repeat.
    pub fn ineffective_count(&self, target: &WorkloadId, kind: ActionKind) -> usize {
        self.records
            .iter()
            .filter(|record| {
                &record.target == target
                    && record.kind == kind
                    && record.outcome == ActionOutcome::Ineffective
            })
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(scope: &str) -> WorkloadId {
        WorkloadId {
            scope: scope.to_string(),
            generation: 1,
        }
    }

    fn record(scope: &str, kind: ActionKind, at_ms: u64, outcome: ActionOutcome) -> ActionRecord {
        ActionRecord {
            target: target(scope),
            kind,
            at_ms,
            outcome,
        }
    }

    #[test]
    fn the_buffer_stays_bounded() {
        let mut history = PolicyHistory::new(3, 60_000, 100);
        for i in 0..10 {
            history.record(record(
                "a",
                ActionKind::CgroupSoftLimit,
                i,
                ActionOutcome::Succeeded,
            ));
        }
        assert_eq!(history.len(), 3);
    }

    #[test]
    fn cooldown_tracks_the_most_recent_action() {
        let mut history = PolicyHistory::new(8, 60_000, 100);
        history.record(record(
            "a",
            ActionKind::CgroupSoftLimit,
            1_000,
            ActionOutcome::Succeeded,
        ));
        assert!(history.is_cooling_down(&target("a"), 1_500, 1_000));
        assert!(!history.is_cooling_down(&target("a"), 2_500, 1_000));
        assert!(!history.is_cooling_down(&target("b"), 1_500, 1_000));
    }

    #[test]
    fn the_rate_limit_counts_only_the_window() {
        let mut history = PolicyHistory::new(16, 1_000, 2);
        history.record(record(
            "a",
            ActionKind::CgroupSoftLimit,
            0,
            ActionOutcome::Succeeded,
        ));
        history.record(record(
            "b",
            ActionKind::CgroupSoftLimit,
            500,
            ActionOutcome::Succeeded,
        ));
        assert!(history.rate_limited(900));
        // Once the first two fall outside the window the limit clears.
        assert!(!history.rate_limited(1_600));
    }

    #[test]
    fn ineffective_actions_are_counted_per_target_and_kind() {
        let mut history = PolicyHistory::new(8, 60_000, 100);
        history.record(record(
            "a",
            ActionKind::SleepModel,
            10,
            ActionOutcome::Ineffective,
        ));
        history.record(record(
            "a",
            ActionKind::SleepModel,
            20,
            ActionOutcome::Succeeded,
        ));
        assert_eq!(
            history.ineffective_count(&target("a"), ActionKind::SleepModel),
            1
        );
        assert_eq!(
            history.ineffective_count(&target("a"), ActionKind::UnloadIdleModel),
            0
        );
        assert_eq!(
            history.ineffective_count(&target("b"), ActionKind::SleepModel),
            0
        );
    }
}
