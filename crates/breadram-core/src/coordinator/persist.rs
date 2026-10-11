//! What the daemon must remember across a restart.
//!
//! The in-memory journal is a bounded report of what happened. What a restart
//! actually needs is narrower: the values breadram still owns, with the value
//! that was there before it touched them, so a restart can put them back without
//! overwriting a newer external configuration.
//!
//! The state file is versioned, and a file written by a build this one does not
//! understand is refused rather than reinterpreted. It holds setting keys and
//! values only: never a command line, an environment variable, a prompt, or a
//! credential, because there is nowhere in these types to put one.

use serde::{Deserialize, Serialize};

use super::journal::{ActionJournal, OwnedValue, OwnerToken};
use super::ActionId;

/// The journal state format this build writes and understands.
pub const JOURNAL_STATE_VERSION: u32 = 1;

/// One owned value, as it is stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedOwnedValue {
    /// The id of the action that wrote it, so a restore can be journalled
    /// against the action it undoes.
    pub id: u64,
    /// The stable setting key.
    pub key: String,
    /// The value before breadram wrote it.
    pub original: String,
    /// The value breadram last wrote.
    pub last_applied: String,
    /// The instance that wrote it.
    pub owner: String,
}

/// The stored journal state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedJournal {
    /// The state format version.
    pub version: u32,
    /// The values breadram owns, newest write per key.
    pub values: Vec<PersistedOwnedValue>,
}

impl PersistedJournal {
    /// Everything the journal currently owns.
    pub fn from_journal(journal: &ActionJournal) -> Self {
        Self {
            version: JOURNAL_STATE_VERSION,
            values: journal
                .owned_values()
                .into_iter()
                .map(|(id, value)| PersistedOwnedValue {
                    id: id.as_u64(),
                    key: value.key,
                    original: value.original,
                    last_applied: value.last_applied,
                    owner: value.owner.0,
                })
                .collect(),
        }
    }

    /// Whether there is nothing to restore.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// A journal that owns exactly these values.
    pub fn to_journal(&self, capacity: usize) -> ActionJournal {
        ActionJournal::from_owned_values(
            capacity,
            self.values.iter().map(|value| {
                (
                    ActionId::from_raw(value.id),
                    OwnedValue {
                        key: value.key.clone(),
                        original: value.original.clone(),
                        last_applied: value.last_applied.clone(),
                        owner: OwnerToken(value.owner.clone()),
                    },
                )
            }),
        )
    }

    /// Serialize to one line of JSON.
    pub fn encode(&self) -> Result<String, String> {
        serde_json::to_string(self).map_err(|err| err.to_string())
    }

    /// Parse a stored journal, refusing a format this build does not know.
    pub fn decode(text: &str) -> Result<Self, String> {
        let parsed: Self = serde_json::from_str(text).map_err(|err| err.to_string())?;
        if parsed.version != JOURNAL_STATE_VERSION {
            return Err(format!(
                "the journal state file is version {}, and this build writes and reads version {JOURNAL_STATE_VERSION}",
                parsed.version
            ));
        }
        Ok(parsed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ActionKind;

    fn journal_with_a_value() -> ActionJournal {
        let id = ActionId::from_scope("unit:app.service", 1, ActionKind::CgroupSoftLimit);
        ActionJournal::from_owned_values(
            8,
            [(
                id,
                OwnedValue {
                    key: "cgroup:unit:app.service/memory.high".to_string(),
                    original: "max".to_string(),
                    last_applied: "2147483648".to_string(),
                    owner: OwnerToken("instance-1".to_string()),
                },
            )],
        )
    }

    #[test]
    fn the_owned_values_survive_a_round_trip() {
        let journal = journal_with_a_value();
        let stored = PersistedJournal::from_journal(&journal);
        assert_eq!(stored.version, JOURNAL_STATE_VERSION);
        assert!(!stored.is_empty());

        let line = stored.encode().expect("encodes");
        let parsed = PersistedJournal::decode(&line).expect("decodes");
        assert_eq!(parsed, stored);

        let recovered = parsed.to_journal(8);
        let values = recovered.owned_values();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].1.original, "max");
        assert_eq!(values[0].1.last_applied, "2147483648");
        assert_eq!(values[0].1.owner.0, "instance-1");
        assert_eq!(
            values[0].0,
            ActionId::from_scope("unit:app.service", 1, ActionKind::CgroupSoftLimit),
            "the recovered id is the id the value was written under"
        );
    }

    #[test]
    fn a_state_file_from_another_build_is_refused_rather_than_reinterpreted() {
        let err = PersistedJournal::decode(
            r#"{"version":99,"values":[{"id":1,"key":"k","original":"a","last_applied":"b","owner":"o"}]}"#,
        )
        .expect_err("a newer format must not be guessed at");
        assert!(err.contains("version 99"), "{err}");
    }

    #[test]
    fn a_malformed_state_file_is_an_error_not_an_empty_journal() {
        assert!(PersistedJournal::decode("not json").is_err());
        assert!(PersistedJournal::decode(r#"{"version":1}"#).is_err());
    }

    #[test]
    fn an_empty_journal_stores_nothing() {
        let stored = PersistedJournal::from_journal(&ActionJournal::new(4));
        assert!(stored.is_empty());
        assert!(stored.to_journal(4).is_empty());
    }
}
