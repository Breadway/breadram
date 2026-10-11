//! Capability reporting.
//!
//! The daemon reports what it can actually do on this specific host instead of
//! assuming a fixed feature set. A capability that cannot be probed, is not
//! permitted, or is only partially supported is reported as degraded or
//! unavailable with a reason. Missing data is never treated as success.

use serde::{Deserialize, Serialize};

/// The state of a single capability on the current host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum CapabilityState {
    /// Fully supported and probed.
    Available,
    /// Usable, but with a documented limitation.
    Degraded {
        /// Why the capability is only partially available.
        reason: String,
    },
    /// Not usable here. The reason must explain what is missing.
    Unavailable {
        /// Why the capability cannot be used.
        reason: String,
    },
}

impl CapabilityState {
    /// True when the capability works in some form (available or degraded).
    pub fn is_usable(&self) -> bool {
        matches!(
            self,
            CapabilityState::Available | CapabilityState::Degraded { .. }
        )
    }

    /// The explanatory reason when the capability is not fully available.
    pub fn reason(&self) -> Option<&str> {
        match self {
            CapabilityState::Available => None,
            CapabilityState::Degraded { reason } | CapabilityState::Unavailable { reason } => {
                Some(reason)
            }
        }
    }
}

/// A named capability and its state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capability {
    /// Stable capability name, for example `psi` or `cgroup_v2`.
    pub name: String,
    /// How the capability stands on this host.
    pub state: CapabilityState,
}

/// A collection of capability readings, keyed by name.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityReport {
    /// The reported capabilities, in insertion order.
    pub capabilities: Vec<Capability>,
}

impl CapabilityReport {
    /// An empty report.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record (or replace) the state of a named capability.
    pub fn set(&mut self, name: impl Into<String>, state: CapabilityState) {
        let name = name.into();
        match self.capabilities.iter_mut().find(|c| c.name == name) {
            Some(existing) => existing.state = state,
            None => self.capabilities.push(Capability { name, state }),
        }
    }

    /// The state of a named capability, if it was reported.
    pub fn get(&self, name: &str) -> Option<&CapabilityState> {
        self.capabilities
            .iter()
            .find(|c| c.name == name)
            .map(|c| &c.state)
    }

    /// True when the named capability is usable in some form.
    pub fn is_usable(&self, name: &str) -> bool {
        self.get(name).is_some_and(CapabilityState::is_usable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_then_get_round_trips() {
        let mut report = CapabilityReport::new();
        report.set("psi", CapabilityState::Available);
        assert_eq!(report.get("psi"), Some(&CapabilityState::Available));
        assert!(report.is_usable("psi"));
    }

    #[test]
    fn set_replaces_in_place() {
        let mut report = CapabilityReport::new();
        report.set("psi", CapabilityState::Available);
        report.set(
            "psi",
            CapabilityState::Unavailable {
                reason: "missing".into(),
            },
        );
        assert_eq!(report.capabilities.len(), 1);
        assert!(!report.is_usable("psi"));
    }

    #[test]
    fn unknown_capability_is_not_usable() {
        // Absence of evidence is not evidence of a working capability.
        let report = CapabilityReport::new();
        assert!(!report.is_usable("does_not_exist"));
        assert_eq!(report.get("does_not_exist"), None);
    }

    #[test]
    fn degraded_is_usable_and_carries_a_reason() {
        let state = CapabilityState::Degraded {
            reason: "cgroup v1".into(),
        };
        assert!(state.is_usable());
        assert_eq!(state.reason(), Some("cgroup v1"));
    }

    #[test]
    fn available_has_no_reason() {
        assert_eq!(CapabilityState::Available.reason(), None);
    }

    #[test]
    fn survives_a_json_round_trip() {
        let mut report = CapabilityReport::new();
        report.set("psi", CapabilityState::Available);
        report.set(
            "cgroup_v2",
            CapabilityState::Degraded {
                reason: "no delegation".into(),
            },
        );
        report.set(
            "gpu",
            CapabilityState::Unavailable {
                reason: "no accelerator".into(),
            },
        );
        let json = serde_json::to_string(&report).expect("serialize");
        let back: CapabilityReport = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(report, back);
    }
}
