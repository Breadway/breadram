//! Parser for Linux pressure stall information (PSI).
//!
//! See <https://www.kernel.org/doc/html/latest/accounting/psi.html>. PSI reports
//! how long tasks were stalled waiting for a resource. `some` means at least one
//! task was stalled, `full` means every task was stalled. `full` is absent on
//! kernels before 5.13 for some resources, so it is `Option`.

/// One `some` or `full` PSI line.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PsiLine {
    /// Percentage of the last 10 seconds spent stalled.
    pub avg10: f64,
    /// Percentage of the last 60 seconds spent stalled.
    pub avg60: f64,
    /// Percentage of the last 300 seconds spent stalled.
    pub avg300: f64,
    /// Cumulative stall time in microseconds since boot.
    pub total_us: u64,
}

/// A full PSI reading for one resource.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PsiReading {
    /// The `some` line, which every PSI capable kernel reports.
    pub some: PsiLine,
    /// The `full` line, absent on some kernels.
    pub full: Option<PsiLine>,
}

/// A PSI parse failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PsiError {
    /// A key/value pair was not of the form `key=value`.
    #[error("malformed PSI field {0:?}")]
    MalformedField(String),
    /// A numeric field failed to parse.
    #[error("bad PSI value {value:?} for {key}")]
    BadValue {
        /// The field name.
        key: String,
        /// The offending value.
        value: String,
    },
    /// The required `some` line was not present.
    #[error("PSI reading has no \"some\" line")]
    MissingSome,
    /// A `some` or `full` line was missing one of its fields.
    #[error("incomplete PSI \"{0}\" line")]
    IncompleteLine(String),
}

/// Parse the contents of a PSI file such as `/proc/pressure/memory`.
pub fn parse_psi(contents: &str) -> Result<PsiReading, PsiError> {
    let mut some = None;
    let mut full = None;

    for raw in contents.lines() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.split_whitespace();
        let kind = parts.next().unwrap_or_default();

        // Only `some` and `full` are PSI lines. Anything else (a comment, a
        // future addition) is skipped rather than treated as malformed.
        if kind != "some" && kind != "full" {
            continue;
        }

        let mut avg10 = None;
        let mut avg60 = None;
        let mut avg300 = None;
        let mut total_us = None;

        for field in parts {
            let (key, value) = field
                .split_once('=')
                .ok_or_else(|| PsiError::MalformedField(field.to_string()))?;
            match key {
                "avg10" => avg10 = Some(parse_f64(key, value)?),
                "avg60" => avg60 = Some(parse_f64(key, value)?),
                "avg300" => avg300 = Some(parse_f64(key, value)?),
                "total" => total_us = Some(parse_u64(key, value)?),
                // Unknown keys are ignored so a newer kernel does not break us.
                _ => {}
            }
        }

        let parsed = PsiLine {
            avg10: avg10.ok_or_else(|| PsiError::IncompleteLine(kind.to_string()))?,
            avg60: avg60.ok_or_else(|| PsiError::IncompleteLine(kind.to_string()))?,
            avg300: avg300.ok_or_else(|| PsiError::IncompleteLine(kind.to_string()))?,
            total_us: total_us.ok_or_else(|| PsiError::IncompleteLine(kind.to_string()))?,
        };

        match kind {
            "some" => some = Some(parsed),
            "full" => full = Some(parsed),
            _ => {}
        }
    }

    Ok(PsiReading {
        some: some.ok_or(PsiError::MissingSome)?,
        full,
    })
}

fn parse_f64(key: &str, value: &str) -> Result<f64, PsiError> {
    value.parse().map_err(|_| PsiError::BadValue {
        key: key.to_string(),
        value: value.to_string(),
    })
}

fn parse_u64(key: &str, value: &str) -> Result<u64, PsiError> {
    value.parse().map_err(|_| PsiError::BadValue {
        key: key.to_string(),
        value: value.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "some avg10=0.00 avg60=0.02 avg300=1.17 total=33645600\nfull avg10=0.00 avg60=0.02 avg300=1.16 total=33142024\n";

    #[test]
    fn parses_some_and_full() {
        let reading = parse_psi(SAMPLE).unwrap();
        assert_eq!(reading.some.avg10, 0.0);
        assert_eq!(reading.some.avg60, 0.02);
        assert_eq!(reading.some.avg300, 1.17);
        assert_eq!(reading.some.total_us, 33_645_600);
        let full = reading.full.unwrap();
        assert_eq!(full.total_us, 33_142_024);
    }

    #[test]
    fn full_line_may_be_absent() {
        let reading = parse_psi("some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n").unwrap();
        assert_eq!(reading.some.total_us, 0);
        assert_eq!(reading.full, None);
    }

    #[test]
    fn a_reading_without_some_is_an_error() {
        assert_eq!(
            parse_psi("full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"),
            Err(PsiError::MissingSome)
        );
    }

    #[test]
    fn unknown_keys_are_ignored() {
        let reading = parse_psi("some avg10=1.0 avg60=2.0 avg300=3.0 total=4 future=9\n").unwrap();
        assert_eq!(reading.some.avg10, 1.0);
    }

    #[test]
    fn a_field_without_equals_is_malformed() {
        assert!(matches!(
            parse_psi("some avg10=0.00 avg60 avg300=0.00 total=0\n"),
            Err(PsiError::MalformedField(_))
        ));
    }

    #[test]
    fn a_bad_number_is_reported() {
        assert!(matches!(
            parse_psi("some avg10=abc avg60=0.00 avg300=0.00 total=0\n"),
            Err(PsiError::BadValue { .. })
        ));
    }
    #[test]
    fn a_line_that_is_not_psi_is_skipped() {
        assert_eq!(parse_psi("hello world\n"), Err(PsiError::MissingSome));
    }

    #[test]
    fn an_incomplete_line_is_reported() {
        assert_eq!(
            parse_psi("some avg10=1.0 avg60=2.0\n"),
            Err(PsiError::IncompleteLine("some".to_string()))
        );
    }
}
