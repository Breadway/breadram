//! A bounded action journal and the values breadram owns.
//!
//! The journal is the daemon's memory of what it did and why. It is fixed size,
//! so a long running daemon cannot grow it without bound, and it is deliberately
//! narrow: an entry holds a stable scope key, the action, the state, the reason,
//! the measured effect, and the daemon-owned values that were changed.
//!
//! It never holds a command line, an environment variable, a prompt, or a
//! credential. That is a property of the type rather than a filtering step: there
//! is nowhere for such a value to be stored.
//!
//! Ownership is recorded so a restore can be safe. For every daemon-owned value
//! the journal keeps the original and the last value breadram wrote. A restore
//! rewrites the original only while the current value still equals what breadram
//! last wrote. If anything else changed it in the meantime, the restore backs off
//! rather than overwriting a newer configuration.

use std::collections::VecDeque;

use crate::config::ActionKind;

use super::{ActionEffect, ActionId, ActionState};

/// Which breadram instance wrote a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerToken(pub String);

/// A daemon-owned setting that breadram changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedValue {
    /// A stable setting key, for example `cgroup:unit:app.service/memory.high`.
    pub key: String,
    /// The value before breadram touched it.
    pub original: String,
    /// The value breadram last wrote.
    pub last_applied: String,
    /// The breadram instance that wrote it.
    pub owner: OwnerToken,
}

/// One journal entry: what ran, why, and what it changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalEntry {
    /// The idempotency id of the action.
    pub id: ActionId,
    /// The stable scope key of the target. Never a command line.
    pub scope: String,
    /// The action, or `None` for an entry that is not an action, such as a
    /// restore. A rollback must not claim to be the action it undid.
    pub kind: Option<ActionKind>,
    /// How it ended.
    pub state: ActionState,
    /// When it ended, in monotonic milliseconds.
    pub at_ms: u64,
    /// Why it ran, in words.
    pub reason: String,
    /// The requested and observed effect, when one was measured.
    pub effect: Option<ActionEffect>,
    /// The daemon-owned values it changed.
    pub values: Vec<OwnedValue>,
}

/// A bounded, in-memory journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionJournal {
    capacity: usize,
    entries: VecDeque<JournalEntry>,
}

impl ActionJournal {
    /// A journal holding at most `capacity` entries.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: VecDeque::new(),
        }
    }

    /// Append an entry, evicting the oldest when full.
    pub fn record(&mut self, entry: JournalEntry) {
        while self.entries.len() >= self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
    }

    /// The entries, oldest first.
    pub fn entries(&self) -> &VecDeque<JournalEntry> {
        &self.entries
    }

    /// How many entries are retained.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the journal is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The configured capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The most recent state recorded for an id.
    pub fn last_state(&self, id: ActionId) -> Option<ActionState> {
        self.entries
            .iter()
            .rev()
            .find(|entry| entry.id == id)
            .map(|entry| entry.state)
    }

    /// Whether this action already has a confirmed effect, which is what makes a
    /// repeated request idempotent.
    pub fn has_confirmed(&self, id: ActionId) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.id == id && entry.state == ActionState::Confirmed)
    }

    /// Every owned value, newest first, de-duplicated by key.
    ///
    /// The newest write of a key is the one that matters, so an older entry for
    /// the same key is skipped.
    pub fn owned_values(&self) -> Vec<(ActionId, OwnedValue)> {
        let mut seen: Vec<&str> = Vec::new();
        let mut values = Vec::new();
        for entry in self.entries.iter().rev() {
            for value in &entry.values {
                if seen.contains(&value.key.as_str()) {
                    continue;
                }
                seen.push(&value.key);
                values.push((entry.id, value.clone()));
            }
        }
        values
    }
}

/// What happened when one owned value was restored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoreOutcome {
    /// The original value was written back.
    Restored,
    /// The value was already the original, so there was nothing to do.
    SkippedNoChange,
    /// Something else changed the value after breadram, so it was left alone.
    SkippedForeignChange {
        /// The value currently in place.
        current: String,
    },
    /// The target no longer exists.
    SkippedTargetGone,
    /// The restore failed.
    Failed {
        /// Why.
        reason: String,
    },
}

/// The result of restoring one owned value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreResult {
    /// The action that originally wrote the value.
    pub id: ActionId,
    /// The setting key.
    pub key: String,
    /// What happened.
    pub outcome: RestoreOutcome,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ActionKind;
    use crate::coordinator::ActionId;

    fn value(key: &str, original: &str, last_applied: &str) -> OwnedValue {
        OwnedValue {
            key: key.to_string(),
            original: original.to_string(),
            last_applied: last_applied.to_string(),
            owner: OwnerToken("instance-1".to_string()),
        }
    }

    fn entry(id: ActionId, key: Option<&str>, state: ActionState) -> JournalEntry {
        JournalEntry {
            id,
            scope: "unit:app.service".to_string(),
            kind: Some(ActionKind::CgroupSoftLimit),
            state,
            at_ms: 1,
            reason: "test".to_string(),
            effect: None,
            values: key
                .map(|key| vec![value(key, "100", "50")])
                .unwrap_or_default(),
        }
    }

    #[test]
    fn the_journal_is_bounded() {
        let mut journal = ActionJournal::new(2);
        let id = ActionId::from_scope("a", 1, ActionKind::CgroupSoftLimit);
        for _ in 0..5 {
            journal.record(entry(id, None, ActionState::Confirmed));
        }
        assert_eq!(journal.len(), 2);
        assert_eq!(journal.capacity(), 2);
    }

    #[test]
    fn a_confirmed_action_is_recognised_as_already_applied() {
        let mut journal = ActionJournal::new(4);
        let id = ActionId::from_scope("a", 1, ActionKind::CgroupSoftLimit);
        assert!(!journal.has_confirmed(id));
        journal.record(entry(id, None, ActionState::Failed));
        assert!(
            !journal.has_confirmed(id),
            "a failure must not block a retry"
        );
        journal.record(entry(id, None, ActionState::Confirmed));
        assert!(journal.has_confirmed(id));
    }

    #[test]
    fn owned_values_keep_only_the_newest_write_of_a_key() {
        let mut journal = ActionJournal::new(8);
        let first = ActionId::from_scope("a", 1, ActionKind::CgroupSoftLimit);
        let second = ActionId::from_scope("a", 1, ActionKind::CgroupSoftLimit);
        journal.record(entry(
            first,
            Some("cgroup:x/memory.high"),
            ActionState::Failed,
        ));
        journal.record(entry(
            second,
            Some("cgroup:x/memory.high"),
            ActionState::Confirmed,
        ));
        let values = journal.owned_values();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].0, second, "the newest write wins");
    }

    #[test]
    fn last_state_reports_the_most_recent_entry_for_an_id() {
        let mut journal = ActionJournal::new(8);
        let id = ActionId::from_scope("a", 1, ActionKind::CgroupSoftLimit);
        journal.record(entry(id, None, ActionState::Busy));
        journal.record(entry(id, None, ActionState::Confirmed));
        assert_eq!(journal.last_state(id), Some(ActionState::Confirmed));
    }
}
