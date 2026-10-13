//! Effective memory budgets.
//!
//! A configured budget is a wish. The number that may actually be applied is
//! the smaller of that wish, the ancestor cgroup limit, and whatever is left
//! of host RAM after a reserve for the desktop and for consumers breadram does
//! not manage. A limit that makes stalls worse is reversed rather than kept.

use crate::control::MIN_REQUESTED_BYTES;

/// Bytes left aside for the desktop and unmanaged consumers (1 GiB).
///
/// This is a floor, not a measurement of the desktop. A host smaller than the
/// reserve plus the minimum useful limit cannot take a soft limit at all.
pub const DESKTOP_RESERVE_BYTES: u64 = 1024 * 1024 * 1024;

/// How much a stall reading (in percent, times 100 so it stays an integer)
/// must rise before an intervention is treated as thrashing.
///
/// PSI `avg10` is a percentage. One point is the smallest rise worth acting
/// on: noise below that does not reverse a limit that is otherwise helping.
pub const STALL_RISE_POINTS: u32 = 1;

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

/// Compare stall percent before and after a limit.
///
/// `before` and `after` are PSI-style percentages in the range 0 to 100.
/// A rise of [`STALL_RISE_POINTS`] or more reverses. A drop or a flat reading
/// keeps the limit. This does not claim the limit caused the rise: it only
/// refuses to keep an intervention whose stall reading got worse.
pub fn stall_verdict(before_percent: u32, after_percent: u32) -> StallVerdict {
    if after_percent.saturating_sub(before_percent) >= STALL_RISE_POINTS {
        StallVerdict::Reverse
    } else {
        StallVerdict::Keep
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
        assert_eq!(stall_verdict(5, 6), StallVerdict::Reverse);
        assert_eq!(stall_verdict(5, 5), StallVerdict::Keep);
        assert_eq!(stall_verdict(20, 4), StallVerdict::Keep);
        assert_eq!(stall_verdict(100, 100), StallVerdict::Keep);
    }
}
