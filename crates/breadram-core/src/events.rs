//! Bounded command correlation independent of the optional bread transport.
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Done,
    Failed,
    TimedOut,
}

/// Pending correlation IDs contain no prompts, environment, or credentials.
pub struct PendingCommands {
    capacity: usize,
    pending: BTreeMap<String, u64>,
}

impl PendingCommands {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            pending: BTreeMap::new(),
        }
    }
    pub fn begin(&mut self, id: &str, deadline_ms: u64) -> bool {
        if id.is_empty()
            || id.len() > 128
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            || self.pending.contains_key(id)
            || self.pending.len() >= self.capacity
        {
            return false;
        }
        self.pending.insert(id.to_string(), deadline_ms);
        true
    }
    pub fn confirm(&mut self, id: &str, success: bool, now_ms: u64) -> Option<Outcome> {
        let deadline = self.pending.remove(id)?;
        Some(if now_ms >= deadline {
            Outcome::TimedOut
        } else if success {
            Outcome::Done
        } else {
            Outcome::Failed
        })
    }
    pub fn expire(&mut self, now_ms: u64) -> Vec<String> {
        let expired: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, deadline)| now_ms >= **deadline)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &expired {
            self.pending.remove(id);
        }
        expired
    }
}

pub fn valid_command_namespace(name: &str) -> bool {
    matches!(
        name,
        "bread.command.ram.status" | "bread.command.ram.mode" | "bread.command.ram.action"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn no_subscriber_duplicate_late_and_foreign_confirmations_are_not_success() {
        let mut pending = PendingCommands::new(1);
        assert!(pending.begin("request_1", 100));
        assert!(!pending.begin("request_1", 100));
        assert!(!pending.begin("request_2", 100));
        assert_eq!(pending.confirm("foreign", true, 10), None);
        assert_eq!(
            pending.confirm("request_1", true, 100),
            Some(Outcome::TimedOut)
        );
        assert!(pending.begin("request_2", 200));
        assert_eq!(pending.expire(200), vec!["request_2"]);
    }
    #[test]
    fn namespace_and_id_are_restricted() {
        assert!(valid_command_namespace("bread.command.ram.mode"));
        assert!(!valid_command_namespace("bread.power.mode"));
        assert!(!PendingCommands::new(1).begin("../secret", 100));
    }
}
