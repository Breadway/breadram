//! How much the daemon is allowed to do.

use std::fmt;
use std::str::FromStr;

/// The daemon's management mode.
///
/// The mode is the top level safety switch. Anything the operator can do
/// automatically has an explicit monitor-only equivalent that only explains
/// what it would do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ManagementMode {
    /// Observe and explain only. Never change system state.
    #[default]
    Monitor,
    /// Apply configured, reversible actions when the policy calls for them.
    Automatic,
}

impl ManagementMode {
    /// The stable string form used on the wire and in configuration files.
    pub fn as_str(self) -> &'static str {
        match self {
            ManagementMode::Monitor => "monitor",
            ManagementMode::Automatic => "automatic",
        }
    }
}

impl fmt::Display for ManagementMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Returned when a string is not a recognized management mode.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown management mode {0:?}, expected \"monitor\" or \"automatic\"")]
pub struct ModeParseError(pub String);

impl FromStr for ManagementMode {
    type Err = ModeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "monitor" => Ok(ManagementMode::Monitor),
            "automatic" => Ok(ManagementMode::Automatic),
            other => Err(ModeParseError(other.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_monitor() {
        // A daemon that starts without explicit configuration must not act.
        assert_eq!(ManagementMode::default(), ManagementMode::Monitor);
    }

    #[test]
    fn round_trips_through_strings() {
        for mode in [ManagementMode::Monitor, ManagementMode::Automatic] {
            assert_eq!(mode.as_str().parse::<ManagementMode>().unwrap(), mode);
            assert_eq!(mode.to_string(), mode.as_str());
        }
    }

    #[test]
    fn rejects_unknown_modes() {
        let err = "aggressive".parse::<ManagementMode>().unwrap_err();
        assert_eq!(err, ModeParseError("aggressive".to_string()));
    }
}
