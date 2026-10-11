//! Bounded, numeric, in-memory trend diagnostics. Growth is not a leak verdict.
use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

/// A workload's memory figure: the only kind `growth` judges.
pub const KIND_WORKLOAD: u8 = 0;
/// Host available memory, sampled once per pass.
pub const KIND_HOST: u8 = 1;
/// The pressure state as a level, sampled once per pass.
pub const KIND_PRESSURE: u8 = 2;
/// A measured action effect, recorded from a journal entry.
pub const KIND_RECOVERY: u8 = 3;

#[derive(Debug, Clone, PartialEq, serde::Deserialize, Serialize)]
pub struct TrendSample {
    pub at_ms: u64,
    /// Opaque numeric target key, never a command line or credential-bearing URL.
    pub target_key: u64,
    pub generation: u64,
    pub bytes: Option<u64>,
    /// False for RSS estimates or other potentially shared accounting.
    pub measured: bool,
    /// What this sample describes: a workload, the host, a pressure level, or a
    /// measured action effect. Numeric, so the schema still carries no text.
    pub kind: u8,
    /// A signed measured host memory change in bytes for a recovery sample.
    /// Negative is relief. `None` for every other kind.
    pub delta: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Growth {
    Unknown,
    NotSustained,
    Sustained {
        increase_bytes: u64,
        interval_ms: u64,
        measured: bool,
    },
}

/// A reachable, operator-facing growth judgement for one workload.
///
/// It carries the net measured relief recorded for the same target inside the
/// window, so a rising figure is never read without the relief that also
/// happened. Growth alone is not a leak verdict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct GrowthReport {
    /// The opaque target key, matching the exported samples.
    pub target_key: u64,
    /// The generation the judgement was made against.
    pub generation: u64,
    /// The sustained-growth judgement.
    pub growth: Growth,
    /// Net measured relief recorded for this target inside the window.
    pub recorded_relief_bytes: i64,
}

pub struct TrendHistory {
    capacity: usize,
    retention_ms: u64,
    samples: VecDeque<TrendSample>,
}

impl TrendHistory {
    /// Apply new limits immediately, including while collection is disabled.
    pub fn configure(&mut self, capacity: usize, retention_ms: u64, now_ms: u64) {
        self.capacity = capacity;
        self.retention_ms = retention_ms;
        self.samples
            .retain(|sample| sample.at_ms <= now_ms && now_ms - sample.at_ms <= retention_ms);
        while self.samples.len() > capacity {
            self.samples.pop_front();
        }
    }

    pub fn retained(&self, now_ms: u64) -> Vec<TrendSample> {
        self.samples
            .iter()
            .filter(|sample| sample.at_ms <= now_ms && now_ms - sample.at_ms <= self.retention_ms)
            .cloned()
            .collect()
    }

    pub fn new(capacity: usize, retention_ms: u64) -> Self {
        Self {
            capacity,
            retention_ms,
            samples: VecDeque::new(),
        }
    }

    pub fn record(&mut self, sample: TrendSample) {
        // A restart or clock reversal starts a fresh epoch; never join timelines.
        if self
            .samples
            .back()
            .is_some_and(|last| sample.at_ms < last.at_ms)
        {
            self.samples.clear();
        }
        let now = sample.at_ms;
        self.samples
            .retain(|old| now.saturating_sub(old.at_ms) <= self.retention_ms);
        if self.capacity == 0 {
            return;
        }
        while self.samples.len() >= self.capacity {
            self.samples.pop_front();
        }
        self.samples.push_back(sample);
    }

    pub fn growth(&self, target_key: u64, generation: u64, now_ms: u64) -> Growth {
        let relevant: Vec<_> = self
            .samples
            .iter()
            .filter(|sample| {
                sample.kind == KIND_WORKLOAD
                    && sample.target_key == target_key
                    && sample.at_ms <= now_ms
                    && now_ms - sample.at_ms <= self.retention_ms
            })
            .collect();
        // Reuse, reset, duplicate timestamps and gaps invalidate a sustained claim.
        if relevant.len() < 3
            || relevant
                .iter()
                .any(|sample| sample.generation != generation || sample.bytes.is_none())
        {
            return Growth::Unknown;
        }
        if relevant
            .windows(2)
            .any(|pair| pair[1].at_ms <= pair[0].at_ms)
        {
            return Growth::Unknown;
        }
        if relevant
            .windows(2)
            .any(|pair| pair[1].bytes < pair[0].bytes)
        {
            return Growth::Unknown;
        }
        if relevant
            .windows(2)
            .any(|pair| pair[1].bytes == pair[0].bytes)
        {
            return Growth::NotSustained;
        }
        let first = relevant[0];
        let last = relevant[relevant.len() - 1];
        Growth::Sustained {
            increase_bytes: last.bytes.unwrap() - first.bytes.unwrap(),
            interval_ms: last.at_ms - first.at_ms,
            measured: relevant.iter().all(|sample| sample.measured),
        }
    }

    /// Export only numeric fields; the caller controls where the JSON is sent.
    pub fn export_json(&self, now_ms: u64) -> Result<String, serde_json::Error> {
        let retained: Vec<_> = self
            .samples
            .iter()
            .filter(|sample| sample.at_ms <= now_ms && now_ms - sample.at_ms <= self.retention_ms)
            .collect();
        serde_json::to_string(&retained)
    }

    /// One growth judgement per retained workload, with its recorded relief.
    ///
    /// This is the operator-facing view: it makes the sustained-growth
    /// judgement reachable outside the unit tests, and pairs each figure with
    /// the measured relief for the same target so growth is told apart from
    /// relief rather than read as a leak on its own.
    pub fn report(&self, now_ms: u64) -> Vec<GrowthReport> {
        let mut seen: Vec<(u64, u64)> = Vec::new();
        let mut reports = Vec::new();
        for sample in &self.samples {
            if sample.kind != KIND_WORKLOAD
                || sample.at_ms > now_ms
                || now_ms - sample.at_ms > self.retention_ms
                || seen.contains(&(sample.target_key, sample.generation))
            {
                continue;
            }
            seen.push((sample.target_key, sample.generation));
            reports.push(GrowthReport {
                target_key: sample.target_key,
                generation: sample.generation,
                growth: self.growth(sample.target_key, sample.generation, now_ms),
                recorded_relief_bytes: self.relief_for(sample.target_key, now_ms),
            });
        }
        reports
    }

    /// Net measured relief recorded for a target inside the retained window.
    pub fn relief_for(&self, target_key: u64, now_ms: u64) -> i64 {
        self.samples
            .iter()
            .filter(|sample| {
                sample.kind == KIND_RECOVERY
                    && sample.target_key == target_key
                    && sample.at_ms <= now_ms
                    && now_ms - sample.at_ms <= self.retention_ms
            })
            .filter_map(|sample| sample.delta)
            .fold(0i64, i64::saturating_add)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample(at_ms: u64, bytes: Option<u64>) -> TrendSample {
        TrendSample {
            at_ms,
            target_key: 7,
            generation: 1,
            bytes,
            measured: false,
            kind: KIND_WORKLOAD,
            delta: None,
        }
    }

    fn recovery(at_ms: u64, target_key: u64, delta: i64) -> TrendSample {
        TrendSample {
            at_ms,
            target_key,
            generation: 1,
            bytes: None,
            measured: true,
            kind: KIND_RECOVERY,
            delta: Some(delta),
        }
    }

    #[test]
    fn report_pairs_growth_with_recorded_relief_and_ignores_non_workload_samples() {
        let mut history = TrendHistory::new(20, 1000);
        history.record(sample(1, Some(10)));
        history.record(sample(2, Some(20)));
        history.record(sample(3, Some(30)));
        history.record(recovery(4, 7, 4096));
        history.record(TrendSample {
            at_ms: 4,
            target_key: 900,
            generation: 0,
            bytes: Some(1),
            measured: true,
            kind: KIND_HOST,
            delta: None,
        });
        let report = history.report(4);
        assert_eq!(
            report.len(),
            1,
            "host and recovery samples are not growth targets: {report:?}"
        );
        assert_eq!(report[0].target_key, 7);
        assert_eq!(
            report[0].growth,
            Growth::Sustained {
                increase_bytes: 20,
                interval_ms: 2,
                measured: false
            }
        );
        assert_eq!(report[0].recorded_relief_bytes, 4096);
    }
    #[test]
    fn growth_is_qualified_and_missing_reset_reuse_are_unknown() {
        let mut history = TrendHistory::new(10, 100);
        for (time, bytes) in [(1, 10), (2, 20), (3, 30)] {
            history.record(sample(time, Some(bytes)));
        }
        assert_eq!(
            history.growth(7, 1, 3),
            Growth::Sustained {
                increase_bytes: 20,
                interval_ms: 2,
                measured: false
            }
        );
        assert_eq!(history.growth(7, 2, 3), Growth::Unknown);
        history.record(sample(4, Some(1)));
        assert_eq!(history.growth(7, 1, 4), Growth::Unknown);
        history.record(sample(5, None));
        assert_eq!(history.growth(7, 1, 5), Growth::Unknown);
    }
    #[test]
    fn reconfiguration_expires_and_shrinks_without_a_new_sample() {
        let mut history = TrendHistory::new(10, 100);
        for time in 1..=5 {
            history.record(sample(time, Some(time)));
        }
        history.configure(2, 100, 5);
        assert_eq!(history.retained(5).len(), 2);
        history.configure(2, 1, 10);
        assert!(history.retained(10).is_empty());
    }

    #[test]
    fn retention_capacity_clock_restart_and_numeric_export() {
        let mut history = TrendHistory::new(3, 5);
        for time in 1..=4 {
            history.record(sample(time, Some(time)));
        }
        assert_eq!(history.samples.len(), 3);
        assert_eq!(history.export_json(20).unwrap(), "[]");
        history.record(sample(20, Some(20)));
        assert_eq!(history.samples.len(), 1);
        history.record(sample(1, Some(1)));
        assert_eq!(history.samples.len(), 1);
        assert_eq!(history.growth(7, 1, 1), Growth::Unknown);
        let json = history.export_json(1).unwrap();
        let fields = serde_json::from_str::<serde_json::Value>(&json).unwrap();
        assert_eq!(fields[0].as_object().unwrap().len(), 7);
        assert_eq!(TrendHistory::new(3, 5).export_json(1).unwrap(), "[]");
        let mut disabled = TrendHistory::new(0, 5);
        disabled.record(sample(1, Some(1)));
        assert_eq!(disabled.export_json(1).unwrap(), "[]");
    }
}
