//! Opted-in background admission. This module never launches a process.
//!
//! Strict FIFO prevents a stream of small jobs from starving an older large
//! job. An estimate larger than available capacity blocks rather than weakening
//! the safety limit with age. Reservations remain until explicit completion.

use std::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};

/// The most admission records a persisted file will carry, so it stays bounded.
pub const MAX_PERSISTED_ADMISSIONS: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingJob {
    pub id: String,
    /// Caller supplied upper bound, including uncertainty, in bytes.
    pub reservation_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitError {
    NotBackground,
    Invalid,
    Duplicate,
    Full,
}

/// In-memory metadata only: no environment, argv, cwd or credentials are stored.
pub struct AdmissionQueue {
    capacity: usize,
    pending: VecDeque<PendingJob>,
    admitted: BTreeMap<String, u64>,
}

impl AdmissionQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            pending: VecDeque::new(),
            admitted: BTreeMap::new(),
        }
    }

    pub fn submit(&mut self, job: PendingJob, background_opt_in: bool) -> Result<(), SubmitError> {
        if !background_opt_in {
            return Err(SubmitError::NotBackground);
        }
        if job.id.is_empty()
            || job.id.len() > 128
            || job.reservation_bytes == 0
            || !job
                .id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
        {
            return Err(SubmitError::Invalid);
        }
        if self.admitted.contains_key(&job.id) || self.pending.iter().any(|old| old.id == job.id) {
            return Err(SubmitError::Duplicate);
        }
        if self.pending.len().saturating_add(self.admitted.len()) >= self.capacity {
            return Err(SubmitError::Full);
        }
        self.pending.push_back(job);
        Ok(())
    }

    pub fn pending(&self) -> impl Iterator<Item = &PendingJob> {
        self.pending.iter()
    }

    /// The admissions currently held, for inspection and for persistence.
    pub fn admitted(&self) -> impl Iterator<Item = (&str, u64)> {
        self.admitted
            .iter()
            .map(|(id, bytes)| (id.as_str(), *bytes))
    }

    /// Cancellation never kills an already admitted job.
    pub fn cancel(&mut self, id: &str) -> bool {
        let Some(index) = self.pending.iter().position(|job| job.id == id) else {
            return false;
        };
        self.pending.remove(index);
        true
    }

    pub fn reserved_bytes(&self) -> u64 {
        self.admitted
            .values()
            .fold(0, |sum, bytes| sum.saturating_add(*bytes))
    }

    /// `capacity_bytes` is a conservative configured total admission budget,
    /// not MemAvailable (which already reflects some admitted allocations).
    /// Unknown or unhealthy pressure refuses all new admissions.
    /// One mutable owner makes check-and-reserve atomic across simultaneous jobs.
    pub fn admit(
        &mut self,
        capacity_bytes: Option<u64>,
        pressure_healthy: bool,
    ) -> Option<PendingJob> {
        if !pressure_healthy {
            return None;
        }
        let capacity = capacity_bytes?;
        let job = self.pending.front()?;
        if job.reservation_bytes > capacity.saturating_sub(self.reserved_bytes()) {
            return None;
        }
        let job = self.pending.pop_front()?;
        self.admitted.insert(job.id.clone(), job.reservation_bytes);
        Some(job)
    }

    /// Keep an inaccurate estimate reserved at its higher observed value. This
    /// stops subsequent admissions; it does not kill an underestimated process.
    pub fn observe_usage(&mut self, id: &str, actual_bytes: u64) -> bool {
        let Some(reserved) = self.admitted.get_mut(id) else {
            return false;
        };
        *reserved = (*reserved).max(actual_bytes);
        true
    }

    /// Called only after the owner observes process exit or failed launch.
    pub fn complete(&mut self, id: &str) -> bool {
        self.admitted.remove(id).is_some()
    }
}

/// One admitted reservation, recorded so a restart can refuse to reuse it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedAdmission {
    /// The admission id.
    pub id: String,
    /// The reserved upper bound in bytes.
    pub reservation_bytes: u64,
}

/// A bounded, durable record of what a previous run had admitted.
///
/// A restart cannot know whether a recorded process is still the one this
/// daemon started, so this never re-admits on load. It reports the reservations
/// so the daemon can refuse to reuse them, which is the conservative direction
/// the card asks for: recover owned reservations, or refuse to reuse them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedAdmissions {
    /// The recorded admissions, bounded by [`MAX_PERSISTED_ADMISSIONS`].
    pub records: Vec<PersistedAdmission>,
}

impl PersistedAdmissions {
    /// Capture the current admissions, bounded to `max` and to the module cap.
    pub fn from_queue(queue: &AdmissionQueue, max: usize) -> Self {
        let mut records: Vec<PersistedAdmission> = queue
            .admitted()
            .map(|(id, reservation_bytes)| PersistedAdmission {
                id: id.to_string(),
                reservation_bytes,
            })
            .collect();
        records.truncate(max.min(MAX_PERSISTED_ADMISSIONS));
        Self { records }
    }

    /// Encode for a bounded state file. An encoding that cannot fail still has a
    /// safe fallback, so a write never produces an unreadable file.
    pub fn encode(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{\"records\":[]}".to_string())
    }

    /// Decode a state file, or `None` if it cannot be read.
    pub fn decode(text: &str) -> Option<Self> {
        serde_json::from_str(text).ok()
    }

    /// The ids a restart refuses to reuse rather than trusting.
    pub fn refused(&self) -> Vec<&str> {
        self.records
            .iter()
            .map(|record| record.id.as_str())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn job(id: &str, bytes: u64) -> PendingJob {
        PendingJob {
            id: id.into(),
            reservation_bytes: bytes,
        }
    }

    #[test]
    fn pressure_unknown_cancel_and_simultaneous_reservations() {
        let mut queue = AdmissionQueue::new(3);
        assert_eq!(
            queue.submit(job("foreground", 10), false),
            Err(SubmitError::NotBackground)
        );
        queue.submit(job("one", 60), true).unwrap();
        queue.submit(job("two", 60), true).unwrap();
        assert!(queue.admit(None, true).is_none());
        assert!(queue.admit(Some(100), false).is_none());
        assert_eq!(queue.admit(Some(100), true).unwrap().id, "one");
        assert!(queue.admit(Some(100), true).is_none());
        assert!(!queue.cancel("one"));
        assert!(queue.cancel("two"));
        assert!(queue.complete("one"));
        assert_eq!(queue.reserved_bytes(), 0);
    }

    #[test]
    fn older_large_job_cannot_be_overtaken_and_underestimate_stops_admission() {
        let mut queue = AdmissionQueue::new(4);
        queue.submit(job("large", 90), true).unwrap();
        queue.submit(job("small", 10), true).unwrap();
        assert!(queue.admit(Some(80), true).is_none());
        assert_eq!(queue.admit(Some(100), true).unwrap().id, "large");
        assert!(queue.observe_usage("large", 110));
        assert!(queue.admit(Some(100), true).is_none());
        assert!(queue.complete("large"));
        assert_eq!(queue.admit(Some(100), true).unwrap().id, "small");
    }

    #[test]
    fn total_metadata_bound_duplicates_and_arithmetic_overflow() {
        let mut queue = AdmissionQueue::new(2);
        queue.submit(job("one", u64::MAX), true).unwrap();
        assert_eq!(
            queue.submit(job("one", 1), true),
            Err(SubmitError::Duplicate)
        );
        queue.submit(job("two", 1), true).unwrap();
        assert_eq!(queue.submit(job("three", 1), true), Err(SubmitError::Full));
        queue.admit(Some(u64::MAX), true).unwrap();
        assert!(queue.admit(Some(u64::MAX), true).is_none());
        assert_eq!(queue.pending().count(), 1);
    }

    #[test]
    fn a_persisted_record_is_bounded_and_refuses_to_reuse() {
        let mut queue = AdmissionQueue::new(8);
        queue.submit(job("a", 10), true).unwrap();
        queue.submit(job("b", 20), true).unwrap();
        assert_eq!(queue.admit(Some(100), true).unwrap().id, "a");
        assert_eq!(
            queue.admitted().collect::<Vec<_>>(),
            vec![("a", 10)],
            "admissions are inspectable"
        );

        // The record is bounded, round-trips, and never claims recovery: a
        // restart refuses to reuse a reservation it cannot prove is still its own.
        let persisted = PersistedAdmissions::from_queue(&queue, 0);
        assert!(persisted.records.is_empty(), "a zero bound keeps none");
        let persisted = PersistedAdmissions::from_queue(&queue, MAX_PERSISTED_ADMISSIONS);
        assert_eq!(persisted.records.len(), 1);
        let decoded = PersistedAdmissions::decode(&persisted.encode()).unwrap();
        assert_eq!(decoded, persisted);
        assert_eq!(decoded.refused(), vec!["a"]);
        assert_eq!(PersistedAdmissions::decode("not json"), None);
    }
}
