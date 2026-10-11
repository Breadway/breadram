//! Swap activity from `/proc/vmstat`, plus a counter delta helper.
//!
//! `pswpin` and `pswpout` are cumulative counters since boot. Their absolute
//! value is meaningless; what matters is the rate between two observations. A
//! counter can reset (a reboot, or a replaced cgroup or process), and a reset
//! must read as unknown activity, not as a negative rate.

use std::collections::HashMap;

/// Swap activity counters, absent when the field was not present.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VmStat {
    /// Pages swapped in since boot.
    pub pswpin: Option<u64>,
    /// Pages swapped out since boot.
    pub pswpout: Option<u64>,
}

/// Parse the swap related fields of `/proc/vmstat`.
///
/// This is lenient on purpose: `/proc/vmstat` carries hundreds of counters and
/// we only want two, so unknown and malformed lines are skipped rather than
/// failing the whole read.
pub fn parse_vmstat(contents: &str) -> VmStat {
    let fields = parse_keyed(contents);
    VmStat {
        pswpin: fields.get("pswpin").copied(),
        pswpout: fields.get("pswpout").copied(),
    }
}

/// The increase between two raw counter readings.
///
/// Returns `None` when the counter went backwards, which means the counter was
/// reset or the observed object was replaced. Callers must treat that as
/// unknown activity instead of a negative rate.
pub fn counter_delta(previous: u64, current: u64) -> Option<u64> {
    current.checked_sub(previous)
}

fn parse_keyed(contents: &str) -> HashMap<String, u64> {
    let mut fields = HashMap::new();
    for raw in contents.lines() {
        let mut parts = raw.split_whitespace();
        let (Some(key), Some(value)) = (parts.next(), parts.next()) else {
            continue;
        };
        if let Ok(number) = value.parse::<u64>() {
            fields.insert(key.to_string(), number);
        }
    }
    fields
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_swap_counters() {
        let contents = "nr_free_pages 12345\npswpin 102445\npswpout 465559\n";
        let stat = parse_vmstat(contents);
        assert_eq!(stat.pswpin, Some(102_445));
        assert_eq!(stat.pswpout, Some(465_559));
    }

    #[test]
    fn missing_counters_are_none() {
        let stat = parse_vmstat("nr_free_pages 10\n");
        assert_eq!(stat.pswpin, None);
        assert_eq!(stat.pswpout, None);
    }

    #[test]
    fn malformed_lines_are_skipped() {
        let contents = "pswpin\npswpout notanumber\npswpin 7\n";
        let stat = parse_vmstat(contents);
        assert_eq!(stat.pswpin, Some(7));
        assert_eq!(stat.pswpout, None);
    }

    #[test]
    fn delta_of_a_normal_increase() {
        assert_eq!(counter_delta(100, 140), Some(40));
    }

    #[test]
    fn delta_of_a_reset_is_unknown() {
        assert_eq!(counter_delta(500, 3), None);
    }

    #[test]
    fn delta_of_no_change_is_zero() {
        assert_eq!(counter_delta(42, 42), Some(0));
    }
}
