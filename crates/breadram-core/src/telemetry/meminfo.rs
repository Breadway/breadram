//! Parser for `/proc/meminfo`.
//!
//! Values are converted to bytes. Fields that older kernels may omit are
//! `Option`, so a missing field is never confused with a zero value.

use std::collections::HashMap;

/// The subset of `/proc/meminfo` breadram uses, in bytes.
///
/// `SwapCached` is intentionally not folded into `cached`: it is swap backed, so
/// it is not free file cache and must not be counted as reclaimable memory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemInfo {
    /// `MemTotal`.
    pub total: u64,
    /// `MemFree`.
    pub free: u64,
    /// `MemAvailable`, absent on kernels before 3.14.
    pub available: Option<u64>,
    /// `Buffers`.
    pub buffers: u64,
    /// `Cached` (page cache, excluding swap cache).
    pub cached: u64,
    /// `SwapTotal`.
    pub swap_total: u64,
    /// `SwapFree`.
    pub swap_free: u64,
    /// `AnonPages`, absent if unreadable.
    pub anon: Option<u64>,
    /// `Shmem`, which is memory backed files and shared buffers.
    pub shmem: Option<u64>,
}

impl MemInfo {
    /// Used swap in bytes. `None` when no swap is configured, so callers can
    /// tell "no swap" apart from "swap full".
    pub fn swap_used(&self) -> Option<u64> {
        if self.swap_total == 0 {
            None
        } else {
            Some(self.swap_total.saturating_sub(self.swap_free))
        }
    }
}

/// A `/proc/meminfo` parse failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MemInfoError {
    /// A line was not of the form `Key: value [unit]`.
    #[error("malformed meminfo line: {0:?}")]
    MalformedLine(String),
    /// A value was not a number.
    #[error("bad number {value:?} on meminfo line: {line:?}")]
    BadNumber {
        /// The full offending line.
        line: String,
        /// The value that failed to parse.
        value: String,
    },
    /// A unit other than `kB` or `B` was seen.
    #[error("unknown unit {unit:?} on meminfo line: {line:?}")]
    UnknownUnit {
        /// The full offending line.
        line: String,
        /// The unrecognized unit.
        unit: String,
    },
    /// A required field was missing.
    #[error("required field {0} missing from meminfo")]
    MissingField(&'static str),
}

/// Parse `/proc/meminfo` contents.
pub fn parse_meminfo(contents: &str) -> Result<MemInfo, MemInfoError> {
    let fields = parse_fields(contents)?;
    Ok(MemInfo {
        total: required(&fields, "MemTotal")?,
        free: required(&fields, "MemFree")?,
        available: fields.get("MemAvailable").copied(),
        buffers: fields.get("Buffers").copied().unwrap_or(0),
        cached: fields.get("Cached").copied().unwrap_or(0),
        swap_total: fields.get("SwapTotal").copied().unwrap_or(0),
        swap_free: fields.get("SwapFree").copied().unwrap_or(0),
        anon: fields.get("AnonPages").copied(),
        shmem: fields.get("Shmem").copied(),
    })
}

fn required(fields: &HashMap<String, u64>, key: &'static str) -> Result<u64, MemInfoError> {
    fields
        .get(key)
        .copied()
        .ok_or(MemInfoError::MissingField(key))
}

fn parse_fields(contents: &str) -> Result<HashMap<String, u64>, MemInfoError> {
    let mut fields = HashMap::new();
    for raw in contents.lines() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let (key, rest) = line
            .split_once(':')
            .ok_or_else(|| MemInfoError::MalformedLine(line.to_string()))?;
        let mut parts = rest.split_whitespace();
        let value = parts
            .next()
            .ok_or_else(|| MemInfoError::MalformedLine(line.to_string()))?;
        let number: u64 = value.parse().map_err(|_| MemInfoError::BadNumber {
            line: line.to_string(),
            value: value.to_string(),
        })?;
        let bytes = match parts.next() {
            None => number,
            Some("kB") => number.saturating_mul(1024),
            Some("B") => number,
            Some(other) => {
                return Err(MemInfoError::UnknownUnit {
                    line: line.to_string(),
                    unit: other.to_string(),
                })
            }
        };
        fields.insert(key.trim().to_string(), bytes);
    }
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "\
MemTotal:       32121664 kB
MemFree:         3982248 kB
MemAvailable:   14627684 kB
Buffers:              12 kB
Cached:          9952808 kB
SwapCached:       921660 kB
AnonPages:       7803076 kB
Shmem:            512000 kB
SwapTotal:       8388604 kB
SwapFree:        4194304 kB
";

    #[test]
    fn parses_a_normal_file_and_converts_kb_to_bytes() {
        let info = parse_meminfo(SAMPLE).unwrap();
        assert_eq!(info.total, 32_121_664 * 1024);
        assert_eq!(info.free, 3_982_248 * 1024);
        assert_eq!(info.available, Some(14_627_684 * 1024));
        assert_eq!(info.buffers, 12 * 1024);
        assert_eq!(info.cached, 9_952_808 * 1024);
        assert_eq!(info.anon, Some(7_803_076 * 1024));
        assert_eq!(info.shmem, Some(512_000 * 1024));
    }

    #[test]
    fn missing_available_is_none_not_zero() {
        let contents = "MemTotal: 100 kB\nMemFree: 50 kB\n";
        let info = parse_meminfo(contents).unwrap();
        assert_eq!(info.available, None);
        assert_eq!(info.buffers, 0);
    }

    #[test]
    fn zero_is_not_confused_with_missing() {
        let contents = "MemTotal: 100 kB\nMemFree: 0 kB\nMemAvailable: 0 kB\n";
        let info = parse_meminfo(contents).unwrap();
        assert_eq!(info.free, 0);
        assert_eq!(info.available, Some(0));
    }

    #[test]
    fn swap_used_distinguishes_no_swap_from_full_swap() {
        let no_swap = "MemTotal: 1 kB\nMemFree: 1 kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n";
        assert_eq!(parse_meminfo(no_swap).unwrap().swap_used(), None);

        let full = "MemTotal: 1 kB\nMemFree: 1 kB\nSwapTotal: 10 kB\nSwapFree: 0 kB\n";
        assert_eq!(parse_meminfo(full).unwrap().swap_used(), Some(10 * 1024));
    }

    #[test]
    fn bytes_without_a_unit_are_taken_as_bytes() {
        let contents = "MemTotal: 4096\nMemFree: 2048\n";
        let info = parse_meminfo(contents).unwrap();
        assert_eq!(info.total, 4096);
    }

    #[test]
    fn extra_whitespace_is_tolerated() {
        let contents = "  MemTotal:    100   kB  \n\nMemFree: 50 kB\n";
        let info = parse_meminfo(contents).unwrap();
        assert_eq!(info.total, 100 * 1024);
    }

    #[test]
    fn a_line_without_a_colon_is_malformed() {
        let contents = "MemTotal 100 kB\n";
        assert!(matches!(
            parse_meminfo(contents),
            Err(MemInfoError::MalformedLine(_))
        ));
    }

    #[test]
    fn a_non_numeric_value_is_reported() {
        let contents = "MemTotal: lots kB\n";
        assert!(matches!(
            parse_meminfo(contents),
            Err(MemInfoError::BadNumber { .. })
        ));
    }

    #[test]
    fn an_unknown_unit_is_reported() {
        let contents = "MemTotal: 100 MB\n";
        assert!(matches!(
            parse_meminfo(contents),
            Err(MemInfoError::UnknownUnit { .. })
        ));
    }

    #[test]
    fn a_missing_required_field_is_reported() {
        let contents = "MemFree: 50 kB\n";
        assert!(matches!(
            parse_meminfo(contents),
            Err(MemInfoError::MissingField("MemTotal"))
        ));
    }

    #[test]
    fn a_huge_value_does_not_overflow() {
        let contents = "MemTotal: 18446744073709551615 kB\nMemFree: 1 kB\n";
        let info = parse_meminfo(contents).unwrap();
        assert_eq!(info.total, u64::MAX);
    }
}
