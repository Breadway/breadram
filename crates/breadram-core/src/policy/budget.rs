//! Effective memory budgets.
//!
//! A configured budget is a wish. The number that may actually be applied is
//! the smaller of that wish, the ancestor cgroup limit, and whatever is left
//! of host RAM after a reserve for the desktop and for consumers breadram does
//! not manage. A limit that makes stalls worse is reversed rather than kept.

use std::collections::HashMap;

use crate::control::MIN_REQUESTED_BYTES;

/// Bytes left aside for the desktop and unmanaged consumers (1 GiB).
///
/// This is a floor, not a measurement of the desktop. A host smaller than the
/// reserve plus the minimum useful limit cannot take a soft limit at all.
pub const DESKTOP_RESERVE_BYTES: u64 = 1024 * 1024 * 1024;

/// How much extra stall, in microseconds accumulated within one monitor tick,
/// reverses a soft limit.
///
/// The signal is the scope's own cgroup `memory.pressure` `total` counter: a
/// cumulative figure, so any thrashing shows up in the same tick rather than
/// after a ten-second average has moved. Half a CPU-second of extra stall in
/// one tick (the default tick is three seconds) is thrashing, not noise: a
/// limit that coincides with that is put back rather than kept.
pub const STALL_SCOPE_DELTA_RISE_US: u64 = 500_000;

/// Why an effective budget could not be produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetRefusal {
    /// The configured budget is below the smallest useful soft limit.
    BelowMinimum,
    /// After the desktop reserve and the ancestor limit, nothing usable remains.
    NoRoom,
}

/// The inputs to one effective-budget calculation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetInputs {
    /// The operator's configured budget for this workload, in bytes.
    pub configured_bytes: u64,
    /// Host RAM in bytes. Zero means unknown, which refuses a limit.
    pub host_bytes: u64,
    /// The ancestor `memory.max`, when one is set. `None` means unlimited.
    pub ancestor_bytes: Option<u64>,
}

/// A budget that may be applied, and why it is not the configured number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveBudget {
    /// The bytes to request as `MemoryHigh`.
    pub bytes: u64,
    /// Human-readable clamps, empty when the configured number was usable.
    pub notes: Vec<String>,
}

/// The smaller of the configured budget, the ancestor limit, and host RAM
/// minus the desktop reserve.
///
/// Refuses rather than inventing a number when the host size is unknown or
/// the remainder is below [`MIN_REQUESTED_BYTES`]. A soft limit is never a
/// hard cap: callers still apply it as `MemoryHigh`.
pub fn effective_budget(inputs: BudgetInputs) -> Result<EffectiveBudget, BudgetRefusal> {
    if inputs.configured_bytes < MIN_REQUESTED_BYTES {
        return Err(BudgetRefusal::BelowMinimum);
    }
    if inputs.host_bytes == 0 || inputs.host_bytes <= DESKTOP_RESERVE_BYTES {
        return Err(BudgetRefusal::NoRoom);
    }
    let host_room = inputs.host_bytes - DESKTOP_RESERVE_BYTES;
    let mut bytes = inputs.configured_bytes;
    let mut notes = Vec::new();
    if let Some(ancestor) = inputs.ancestor_bytes {
        if ancestor < MIN_REQUESTED_BYTES {
            return Err(BudgetRefusal::NoRoom);
        }
        if bytes > ancestor {
            notes.push(format!(
                "clamped from {bytes} to the ancestor limit {ancestor}"
            ));
            bytes = ancestor;
        }
    }
    if bytes > host_room {
        notes.push(format!(
            "clamped from {bytes} to {host_room}, leaving {DESKTOP_RESERVE_BYTES} bytes for the desktop and unmanaged consumers"
        ));
        bytes = host_room;
    }
    if bytes < MIN_REQUESTED_BYTES {
        return Err(BudgetRefusal::NoRoom);
    }
    Ok(EffectiveBudget { bytes, notes })
}

/// What to do with a soft limit after watching stalls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StallVerdict {
    /// Stalls did not rise. Keep the limit.
    Keep,
    /// Stalls rose. Put the previous value back.
    Reverse,
}

/// Compare stall accumulation across the tick after a limit.
///
/// `before_us` and `after_us` are the stall microseconds accumulated during
/// the tick before the limit and the tick after it (deltas of the scope's
/// pressure `total` counter, not the counters themselves). A rise of
/// [`STALL_SCOPE_DELTA_RISE_US`] or more reverses. A drop or a flat reading
/// keeps the limit. This does not claim the limit caused the rise: it only
/// refuses to keep an intervention whose stall accumulation got worse.
pub fn stall_delta_verdict(before_us: u64, after_us: u64) -> StallVerdict {
    if after_us.saturating_sub(before_us) >= STALL_SCOPE_DELTA_RISE_US {
        StallVerdict::Reverse
    } else {
        StallVerdict::Keep
    }
}

/// One watched scope's stall accumulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct ScopeStall {
    /// The pressure `total` counter at the last tick.
    total_us: u64,
    /// Stall microseconds accumulated during the last tick.
    delta_us: u64,
}

/// Per-scope stall accumulation across monitor ticks.
///
/// Host PSI `avg10` cannot move by a point inside one short tick, so a
/// reversal rule written against it never fires. This watches each limited
/// scope's own cgroup `memory.pressure` `total` counter instead, which
/// accumulates immediately when that scope thrashes. The caller feeds one
/// reading per watched scope per tick; scopes absent from a reading are
/// dropped, so the watch stays bounded by the pending set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StallWatch {
    scopes: HashMap<String, ScopeStall>,
}

impl StallWatch {
    /// An empty watch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether nothing is watched.
    pub fn is_empty(&self) -> bool {
        self.scopes.is_empty()
    }

    /// Judge one tick's pressure totals against the previous tick.
    ///
    /// `current` holds one `(scope, total_us)` per scope limited on the
    /// previous pass. Returns the scopes whose accumulation rose by at least
    /// [`STALL_SCOPE_DELTA_RISE_US`]. A scope seen for the first time only
    /// sets its baseline and is never returned. Stored readings are replaced
    /// by this tick's, and scopes not in `current` are forgotten.
    pub fn judge(&mut self, current: &[(String, u64)]) -> Vec<String> {
        let mut risen = Vec::new();
        let mut next = HashMap::with_capacity(current.len());
        for (scope, total_us) in current {
            let delta_us = total_us.saturating_sub(
                self.scopes
                    .get(scope)
                    .map(|known| known.total_us)
                    .unwrap_or(*total_us),
            );
            if let Some(known) = self.scopes.get(scope) {
                if stall_delta_verdict(known.delta_us, delta_us) == StallVerdict::Reverse {
                    risen.push(scope.clone());
                }
            }
            next.insert(
                scope.clone(),
                ScopeStall {
                    total_us: *total_us,
                    delta_us,
                },
            );
        }
        self.scopes = next;
        risen
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inputs(configured: u64, host: u64, ancestor: Option<u64>) -> BudgetInputs {
        BudgetInputs {
            configured_bytes: configured,
            host_bytes: host,
            ancestor_bytes: ancestor,
        }
    }

    #[test]
    fn a_budget_inside_the_room_is_unchanged() {
        let host = 16 * DESKTOP_RESERVE_BYTES;
        let got = effective_budget(inputs(2 * DESKTOP_RESERVE_BYTES, host, None)).unwrap();
        assert_eq!(got.bytes, 2 * DESKTOP_RESERVE_BYTES);
        assert!(got.notes.is_empty());
    }

    #[test]
    fn the_desktop_reserve_shrinks_a_budget_that_would_fill_the_host() {
        let host = 4 * DESKTOP_RESERVE_BYTES;
        let got = effective_budget(inputs(host, host, None)).unwrap();
        assert_eq!(got.bytes, host - DESKTOP_RESERVE_BYTES);
        assert!(got.notes.iter().any(|note| note.contains("desktop")));
    }

    #[test]
    fn the_ancestor_limit_wins_when_it_is_tighter() {
        let host = 16 * DESKTOP_RESERVE_BYTES;
        let ancestor = 2 * DESKTOP_RESERVE_BYTES;
        let got =
            effective_budget(inputs(8 * DESKTOP_RESERVE_BYTES, host, Some(ancestor))).unwrap();
        assert_eq!(got.bytes, ancestor);
        assert!(got.notes.iter().any(|note| note.contains("ancestor")));
    }

    #[test]
    fn unknown_host_memory_refuses_a_limit() {
        assert_eq!(
            effective_budget(inputs(DESKTOP_RESERVE_BYTES, 0, None)),
            Err(BudgetRefusal::NoRoom)
        );
    }

    #[test]
    fn a_host_smaller_than_the_reserve_refuses_a_limit() {
        assert_eq!(
            effective_budget(inputs(MIN_REQUESTED_BYTES, DESKTOP_RESERVE_BYTES, None)),
            Err(BudgetRefusal::NoRoom)
        );
    }

    #[test]
    fn a_rising_stall_reverses_and_a_flat_one_keeps() {
        assert_eq!(
            stall_delta_verdict(100_000, 100_000 + STALL_SCOPE_DELTA_RISE_US),
            StallVerdict::Reverse
        );
        assert_eq!(
            stall_delta_verdict(100_000, 100_000 + STALL_SCOPE_DELTA_RISE_US - 1),
            StallVerdict::Keep
        );
        assert_eq!(stall_delta_verdict(100_000, 100_000), StallVerdict::Keep);
        assert_eq!(stall_delta_verdict(900_000, 100_000), StallVerdict::Keep);
    }

    #[test]
    fn the_watch_needs_two_ticks_and_forgets_dropped_scopes() {
        let mut watch = StallWatch::new();
        assert!(watch.is_empty());
        // The first tick only sets the baseline.
        assert!(watch
            .judge(&[("unit:a.service".to_string(), 1_000_000)])
            .is_empty());
        // A flat second tick keeps the limit.
        assert!(watch
            .judge(&[("unit:a.service".to_string(), 1_100_000)])
            .is_empty());
        // A tick that accumulates half a CPU-second more reverses.
        let risen = watch.judge(&[(
            "unit:a.service".to_string(),
            1_100_000 + 100_000 + STALL_SCOPE_DELTA_RISE_US,
        )]);
        assert_eq!(risen, vec!["unit:a.service".to_string()]);
        // A scope that stops being reported is forgotten.
        assert!(watch.judge(&[]).is_empty());
        assert!(watch.is_empty());
    }
}
