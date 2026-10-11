//! A small set of documented profiles.
//!
//! A profile resolves to a complete, explicit configuration rather than a layer
//! of hidden defaults, so the effective settings are always inspectable.

use std::fmt;
use std::str::FromStr;

use super::{ActionKind, Config, ConfigError};
use crate::ManagementMode;

/// A named starting point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// Observe and explain only. The default for a fresh install.
    Monitor,
    /// A desktop machine: relieve pressure without disturbing interactive work.
    Desktop,
    /// A headless server: prefer throttling background jobs over touching
    /// interactive work.
    HeadlessServer,
}

impl Profile {
    /// Every profile, in documentation order.
    pub const ALL: [Profile; 3] = [Profile::Monitor, Profile::Desktop, Profile::HeadlessServer];

    /// The stable string form used in configuration and on the command line.
    pub fn as_str(self) -> &'static str {
        match self {
            Profile::Monitor => "monitor",
            Profile::Desktop => "desktop",
            Profile::HeadlessServer => "headless-server",
        }
    }

    /// A one line description for help output.
    pub fn description(self) -> &'static str {
        match self {
            Profile::Monitor => "observe and explain only, never act",
            Profile::Desktop => "automatic relief that protects interactive work",
            Profile::HeadlessServer => "automatic relief that prefers throttling background jobs",
        }
    }

    /// Resolve to a complete configuration.
    pub fn resolve(self) -> Config {
        let mut config = Config::default();
        match self {
            Profile::Monitor => {
                config.mode = ManagementMode::Monitor;
                config.automation.allowed_actions.clear();
            }
            Profile::Desktop => {
                config.mode = ManagementMode::Automatic;
                config.automation.allowed_actions = vec![
                    ActionKind::ReclaimProtection,
                    ActionKind::CgroupSoftLimit,
                    ActionKind::UnloadIdleModel,
                ];
                config.protection.protected = vec![
                    "exe:Hyprland".to_string(),
                    "unit:systemd-logind.service".to_string(),
                ];
            }
            Profile::HeadlessServer => {
                config.mode = ManagementMode::Automatic;
                config.automation.allowed_actions = vec![
                    ActionKind::CgroupSoftLimit,
                    ActionKind::ReclaimProtection,
                    ActionKind::QueueBackgroundJob,
                ];
            }
        }
        config
    }
}

impl fmt::Display for Profile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Profile {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "monitor" => Ok(Profile::Monitor),
            "desktop" => Ok(Profile::Desktop),
            "headless-server" | "headless" => Ok(Profile::HeadlessServer),
            other => Err(ConfigError::UnknownProfile(other.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::validate;

    #[test]
    fn every_profile_resolves_to_a_valid_configuration() {
        for profile in Profile::ALL {
            let config = profile.resolve();
            validate(&config).unwrap_or_else(|err| panic!("profile {profile} is invalid: {err}"));
            assert_eq!(config.version, super::super::CURRENT_CONFIG_VERSION);
        }
    }

    #[test]
    fn monitor_allows_no_automatic_actions() {
        let config = Profile::Monitor.resolve();
        assert_eq!(config.mode, ManagementMode::Monitor);
        assert!(config.automation.allowed_actions.is_empty());
    }

    #[test]
    fn desktop_protects_the_compositor() {
        let config = Profile::Desktop.resolve();
        assert!(config
            .protection
            .protected
            .iter()
            .any(|scope| scope.contains("Hyprland")));
    }

    #[test]
    fn profiles_round_trip_through_strings() {
        for profile in Profile::ALL {
            assert_eq!(profile.as_str().parse::<Profile>().unwrap(), profile);
            assert_eq!(profile.to_string(), profile.as_str());
        }
        assert_eq!(
            "headless".parse::<Profile>().unwrap(),
            Profile::HeadlessServer
        );
    }

    #[test]
    fn an_unknown_profile_is_rejected() {
        assert!(matches!(
            "turbo".parse::<Profile>(),
            Err(ConfigError::UnknownProfile(_))
        ));
    }
}
