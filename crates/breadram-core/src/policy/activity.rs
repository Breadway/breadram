//! Activity evidence for workloads.
//!
//! The planner will not release a workload's memory unless it can prove the
//! workload is idle. Missing evidence is [`ActivityEvidence::Unknown`], which is
//! never treated as idle, and an idle reading only counts once it has held for a
//! minimum time, so a momentary gap in activity is not enough either.

use crate::inventory::WorkloadId;

/// What is known about whether a workload is currently doing work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ActivityEvidence {
    /// The workload is serving or doing work. Do not release its memory.
    Active,
    /// The workload has been idle since `since_ms` on the monotonic clock.
    Idle {
        /// When the idle period began, in monotonic milliseconds.
        since_ms: u64,
    },
    /// Nothing is known. This is the default and is never treated as idle.
    #[default]
    Unknown,
}

impl ActivityEvidence {
    /// Whether this is idle evidence that has held long enough to trust.
    pub fn is_confirmed_idle(&self, now_ms: u64, min_idle_ms: u64) -> bool {
        match self {
            // A checked subtraction, so a `since_ms` in the future (clock skew, or
            // a recycled identity) can never be read as a long idle period.
            ActivityEvidence::Idle { since_ms } => now_ms
                .checked_sub(*since_ms)
                .is_some_and(|idle_ms| idle_ms >= min_idle_ms),
            ActivityEvidence::Active | ActivityEvidence::Unknown => false,
        }
    }
}

/// A source of activity evidence.
///
/// Inference coordination and provider adapters implement this in later cards.
pub trait ActivitySource {
    /// The evidence for a workload.
    fn evidence(&self, workload: &WorkloadId) -> ActivityEvidence;
}

/// A source that knows nothing, which is the correct default.
#[derive(Debug, Default, Clone, Copy)]
pub struct UnknownActivity;

impl ActivitySource for UnknownActivity {
    fn evidence(&self, _workload: &WorkloadId) -> ActivityEvidence {
        ActivityEvidence::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_is_never_idle() {
        assert!(!ActivityEvidence::Unknown.is_confirmed_idle(1_000_000, 0));
    }

    #[test]
    fn active_is_never_idle() {
        assert!(!ActivityEvidence::Active.is_confirmed_idle(1_000_000, 0));
    }

    #[test]
    fn idle_counts_once_it_has_held_long_enough() {
        let evidence = ActivityEvidence::Idle { since_ms: 1_000 };
        assert!(!evidence.is_confirmed_idle(1_500, 1_000));
        assert!(evidence.is_confirmed_idle(2_000, 1_000));
    }

    #[test]
    fn a_future_since_time_is_not_idle() {
        // Clock skew or a reused id must not read as a long idle period.
        let evidence = ActivityEvidence::Idle { since_ms: 500_000 };
        assert!(!evidence.is_confirmed_idle(1_000, 0));
    }

    #[test]
    fn the_unknown_source_reports_nothing() {
        let id = WorkloadId {
            scope: "unit:app.service".to_string(),
            generation: 1,
        };
        assert_eq!(UnknownActivity.evidence(&id), ActivityEvidence::Unknown);
    }
}
