//! The pressure state machine.
//!
//! Four states, entered and left with hysteresis, a settle window, and a
//! minimum dwell time, so a single noisy reading cannot flap the policy:
//!
//! - [`PressureState::Healthy`]: nothing to do.
//! - [`PressureState::Pressured`]: sustained stalls, begin relieving.
//! - [`PressureState::Critical`]: severe stalls or very low available memory.
//! - [`PressureState::Recovering`]: pressure has eased; hold off on new actions
//!   and let the system settle before declaring health.
//!
//! Escalation and release use different thresholds. A state is entered at the
//! configured threshold and left only once the signal falls well below it, which
//! is what stops a signal sitting on the boundary from oscillating.
//!
//! Pressure requires PSI. The available memory fraction can only take the host
//! to [`PressureState::Critical`], because a low `MemAvailable` without a stall
//! signal is not evidence that anything is actually suffering.

use std::fmt;

use crate::config::{Config, PressureThresholds, Timing};
use crate::telemetry::MemorySnapshot;

/// Fraction of an entry threshold at which the corresponding state is left.
///
/// Below 1.0 this gives hysteresis. It is a constant rather than configuration
/// on purpose: it is a stability property of the machine, not a policy knob, and
/// changing it needs the replay tests to be re-run.
pub const RELEASE_FRACTION: f64 = 0.5;

/// The host's pressure state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PressureState {
    /// No sustained stalls and memory is available.
    Healthy,
    /// Stalls have eased but recently been present; hold before acting again.
    Recovering,
    /// Sustained stalls; relieve pressure.
    Pressured,
    /// Severe stalls or very low available memory; relieve deliberately.
    Critical,
}

impl PressureState {
    /// Whether the host is currently under pressure, so actions may be planned.
    pub fn is_under_pressure(self) -> bool {
        matches!(self, PressureState::Pressured | PressureState::Critical)
    }
}

impl fmt::Display for PressureState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            PressureState::Healthy => "healthy",
            PressureState::Recovering => "recovering",
            PressureState::Pressured => "pressured",
            PressureState::Critical => "critical",
        };
        f.write_str(name)
    }
}

/// The state machine.
#[derive(Debug, Clone)]
pub struct StateMachine {
    enter_pressured: f64,
    exit_pressured: f64,
    enter_critical: f64,
    exit_critical: f64,
    critical_available_fraction: f64,
    settle_ms: u64,
    min_dwell_ms: u64,
    state: PressureState,
    since_ms: u64,
    candidate: Option<(PressureState, u64)>,
}

impl StateMachine {
    /// Build a machine from validated configuration, starting healthy.
    pub fn new(config: &Config) -> Self {
        Self::with_thresholds(&config.pressure, &config.timing)
    }

    /// Build a machine from explicit thresholds and timings.
    pub fn with_thresholds(thresholds: &PressureThresholds, timing: &Timing) -> Self {
        Self {
            enter_pressured: thresholds.pressured_some_avg10,
            exit_pressured: thresholds.pressured_some_avg10 * RELEASE_FRACTION,
            enter_critical: thresholds.critical_some_avg10,
            exit_critical: thresholds.critical_some_avg10 * RELEASE_FRACTION,
            critical_available_fraction: thresholds.critical_available_fraction,
            settle_ms: timing.settle_ms,
            min_dwell_ms: timing.min_dwell_ms,
            state: PressureState::Healthy,
            since_ms: 0,
            candidate: None,
        }
    }

    /// The current state.
    pub fn state(&self) -> PressureState {
        self.state
    }

    /// When the current state was entered, in monotonic milliseconds.
    pub fn since_ms(&self) -> u64 {
        self.since_ms
    }

    /// Force the machine into a state, for replay and for resuming across a gap.
    pub fn reset(&mut self, state: PressureState, at_ms: u64) {
        self.state = state;
        self.since_ms = at_ms;
        self.candidate = None;
    }

    /// Feed one observation and return the state after it.
    ///
    /// A candidate state must hold for the settle window and the current state
    /// must have lasted the minimum dwell time before the machine moves.
    pub fn observe(&mut self, snapshot: &MemorySnapshot) -> PressureState {
        let now = snapshot.observed_at_ms;
        let raw = self.severity(snapshot);

        if raw == self.state {
            self.candidate = None;
            return self.state;
        }

        let candidate_since = match self.candidate {
            Some((candidate, since)) if candidate == raw => since,
            _ => {
                self.candidate = Some((raw, now));
                now
            }
        };

        let settled = now.saturating_sub(candidate_since) >= self.settle_ms;
        let dwelled = now.saturating_sub(self.since_ms) >= self.min_dwell_ms;
        if settled && dwelled {
            self.state = raw;
            self.since_ms = now;
            self.candidate = None;
        }

        self.state
    }

    /// The state the signals alone imply, using hysteresis around the current
    /// state, before settle and dwell are applied.
    fn severity(&self, snapshot: &MemorySnapshot) -> PressureState {
        let avg10 = snapshot.psi.map(|reading| reading.some.avg10);
        let available_fraction = match (snapshot.meminfo.available, snapshot.meminfo.total) {
            (Some(available), total) if total > 0 => Some(available as f64 / total as f64),
            _ => None,
        };

        let critical_enter = avg10.is_some_and(|avg| avg >= self.enter_critical)
            || available_fraction
                .is_some_and(|fraction| fraction <= self.critical_available_fraction);
        let critical_stay = avg10.is_some_and(|avg| avg >= self.exit_critical)
            || available_fraction
                .is_some_and(|fraction| fraction <= self.critical_available_fraction);
        let pressured_enter = avg10.is_some_and(|avg| avg >= self.enter_pressured);
        let pressured_stay = avg10.is_some_and(|avg| avg >= self.exit_pressured);

        match self.state {
            PressureState::Healthy => {
                if critical_enter {
                    PressureState::Critical
                } else if pressured_enter {
                    PressureState::Pressured
                } else {
                    PressureState::Healthy
                }
            }
            PressureState::Pressured => {
                if critical_enter {
                    PressureState::Critical
                } else if pressured_stay {
                    PressureState::Pressured
                } else {
                    PressureState::Recovering
                }
            }
            PressureState::Critical => {
                if critical_stay {
                    PressureState::Critical
                } else if pressured_stay {
                    PressureState::Pressured
                } else {
                    PressureState::Recovering
                }
            }
            PressureState::Recovering => {
                if critical_enter {
                    PressureState::Critical
                } else if pressured_enter {
                    PressureState::Pressured
                } else {
                    PressureState::Healthy
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::CapabilityReport;
    use crate::telemetry::{MemInfo, PsiLine, PsiReading, VmStat};

    const TOTAL: u64 = 100 * 1024 * 1024 * 1024;

    fn snapshot(now_ms: u64, avg10: Option<f64>, available_fraction: f64) -> MemorySnapshot {
        MemorySnapshot {
            observed_at_ms: now_ms,
            meminfo: MemInfo {
                total: TOTAL,
                free: 0,
                available: Some((TOTAL as f64 * available_fraction) as u64),
                buffers: 0,
                cached: 0,
                swap_total: 0,
                swap_free: 0,
                anon: None,
                shmem: None,
            },
            psi: avg10.map(|avg| PsiReading {
                some: PsiLine {
                    avg10: avg,
                    avg60: avg,
                    avg300: avg,
                    total_us: 0,
                },
                full: None,
            }),
            vmstat: VmStat::default(),
            capabilities: CapabilityReport::new(),
        }
    }

    fn machine() -> StateMachine {
        // Defaults: pressured at 5, critical at 25, critical available 5 percent.
        // Defaults: settle 3000 ms, min dwell 10000 ms.
        StateMachine::with_thresholds(&PressureThresholds::default(), &Timing::default())
    }

    #[test]
    fn a_calm_host_stays_healthy() {
        let mut machine = machine();
        for tick in 0..5 {
            let state = machine.observe(&snapshot(tick * 2_000, Some(1.0), 0.5));
            assert_eq!(state, PressureState::Healthy);
        }
    }

    #[test]
    fn a_single_spike_does_not_move_the_state() {
        let mut machine = machine();
        // One critical reading, then back to calm well inside the dwell window.
        assert_eq!(
            machine.observe(&snapshot(1_000, Some(40.0), 0.5)),
            PressureState::Healthy
        );
        assert_eq!(
            machine.observe(&snapshot(2_500, Some(0.0), 0.5)),
            PressureState::Healthy
        );
        assert_eq!(
            machine.observe(&snapshot(9_000, Some(0.0), 0.5)),
            PressureState::Healthy
        );
    }

    #[test]
    fn sustained_pressure_escalates_and_then_recovers() {
        let mut machine = machine();

        // Sustained stall: pressured after settle passes and the minimum dwell
        // on Healthy has elapsed.
        assert_eq!(
            machine.observe(&snapshot(1_000, Some(8.0), 0.5)),
            PressureState::Healthy
        );
        assert_eq!(
            machine.observe(&snapshot(5_000, Some(8.0), 0.5)),
            PressureState::Healthy
        );
        assert_eq!(
            machine.observe(&snapshot(11_000, Some(8.0), 0.5)),
            PressureState::Pressured
        );

        // It gets worse: critical once critical has settled and dwell elapsed.
        assert_eq!(
            machine.observe(&snapshot(12_000, Some(30.0), 0.5)),
            PressureState::Pressured
        );
        assert_eq!(
            machine.observe(&snapshot(22_000, Some(30.0), 0.5)),
            PressureState::Critical
        );

        // Pressure eases: recovering, not straight to healthy.
        assert_eq!(
            machine.observe(&snapshot(23_000, Some(0.0), 0.5)),
            PressureState::Critical
        );
        assert_eq!(
            machine.observe(&snapshot(34_000, Some(0.0), 0.5)),
            PressureState::Recovering
        );

        // Only after the dwell does health return.
        assert_eq!(
            machine.observe(&snapshot(35_000, Some(0.0), 0.5)),
            PressureState::Recovering
        );
        assert_eq!(
            machine.observe(&snapshot(46_000, Some(0.0), 0.5)),
            PressureState::Healthy
        );
    }

    #[test]
    fn hysteresis_holds_pressure_until_the_signal_falls_well_below_entry() {
        let mut machine = machine();
        machine.reset(PressureState::Pressured, 0);

        // 4.0 is below the 5.0 entry threshold but above the 2.5 release
        // threshold, so the host stays pressured.
        assert_eq!(
            machine.observe(&snapshot(1_000, Some(4.0), 0.5)),
            PressureState::Pressured
        );
        assert_eq!(
            machine.observe(&snapshot(11_000, Some(4.0), 0.5)),
            PressureState::Pressured
        );

        // 1.0 is below the release threshold, so it may leave.
        assert_eq!(
            machine.observe(&snapshot(12_000, Some(1.0), 0.5)),
            PressureState::Pressured
        );
        assert_eq!(
            machine.observe(&snapshot(23_000, Some(1.0), 0.5)),
            PressureState::Recovering
        );
    }

    #[test]
    fn low_available_memory_alone_reaches_critical() {
        let mut machine = machine();
        assert_eq!(
            machine.observe(&snapshot(1_000, Some(0.0), 0.02)),
            PressureState::Healthy
        );
        // Critical needs the settle window, so one reading is not enough.
        assert_eq!(
            machine.observe(&snapshot(2_000, Some(0.0), 0.02)),
            PressureState::Healthy
        );
        assert_eq!(
            machine.observe(&snapshot(11_000, Some(0.0), 0.02)),
            PressureState::Critical
        );
    }

    #[test]
    fn without_psi_a_host_cannot_be_reported_as_pressured() {
        let mut machine = machine();
        // No PSI and plenty of memory: healthy, no matter the duration.
        for tick in 0..10 {
            assert_eq!(
                machine.observe(&snapshot(tick * 5_000, None, 0.5)),
                PressureState::Healthy
            );
        }
    }

    #[test]
    fn missing_available_memory_cannot_be_read_as_pressure() {
        let mut machine = machine();
        let mut snapshot = snapshot(1_000, Some(1.0), 0.5);
        snapshot.meminfo.available = None;
        assert_eq!(machine.observe(&snapshot), PressureState::Healthy);
    }
}
