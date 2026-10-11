//! The systemd manager boundary.
//!
//! breadram does not rewrite a systemd-owned cgroup hierarchy. Every memory
//! control goes through the owning manager, as a typed command, so there is no
//! path from policy to an arbitrary command.
//!
//! Three limits are structural rather than conventional:
//!
//! - [`ManagerCommand`] enumerates every operation that exists. There is no
//!   variant that runs a program, and no variant that takes a free-form string
//!   to pass to a shell.
//! - [`UnitName`] is validated on construction, rejecting anything that could be
//!   read as an option or escape the unit namespace.
//! - Only the user manager is addressed, so a control can never be aimed at a
//!   system unit.

use crate::capability::CapabilityState;

/// A validation or execution failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ControlError {
    /// A unit name was not usable.
    #[error("invalid unit name {name:?}: {reason}")]
    InvalidUnit {
        /// The rejected name.
        name: String,
        /// Why it was rejected.
        reason: String,
    },
    /// A value was out of range.
    #[error("invalid {field} value {value}: {reason}")]
    InvalidValue {
        /// The field.
        field: String,
        /// The value.
        value: u64,
        /// Why it was rejected.
        reason: String,
    },
    /// The execution mechanism itself failed, for example the binary is missing.
    #[error("cannot run the manager: {0}")]
    Runner(String),
    /// The manager ran and reported a failure.
    #[error("the manager refused the operation: {0}")]
    Refused(String),
    /// A setting key did not name something this manager can read or write.
    #[error("unsupported setting key {0:?}")]
    UnsupportedKey(String),
}

/// The largest soft limit or protection this build will request (1 TiB).
///
/// A bound on the requested value, so a bad configuration cannot be turned into
/// an absurd kernel value.
pub const MAX_REQUESTED_BYTES: u64 = 1024 * 1024 * 1024 * 1024;

/// The smallest useful soft limit (1 MiB).
pub const MIN_REQUESTED_BYTES: u64 = 1024 * 1024;

/// A validated systemd unit name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UnitName(String);

impl UnitName {
    /// Validate a unit name.
    ///
    /// Accepts the character set systemd documents for unit names, requires a
    /// known suffix, and refuses a leading `-` (which a tool could read as an
    /// option) or a path separator (which would leave the unit namespace).
    pub fn parse(name: &str) -> Result<Self, ControlError> {
        let reject = |reason: &str| ControlError::InvalidUnit {
            name: name.to_string(),
            reason: reason.to_string(),
        };

        if name.is_empty() {
            return Err(reject("it is empty"));
        }
        if name.len() > 256 {
            return Err(reject("it is longer than 256 bytes"));
        }
        if name.starts_with('-') || name.starts_with('.') {
            return Err(reject("it must not start with '-' or '.'"));
        }
        if name.contains('/') || name.contains('\\') && !name.contains("\\x") {
            return Err(reject("it must not contain a path separator"));
        }
        let allowed = name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, ':' | '_' | '.' | '@' | '-' | '\\'));
        if !allowed {
            return Err(reject(
                "it contains a character that is not valid in a unit name",
            ));
        }
        if !(name.ends_with(".service") || name.ends_with(".scope") || name.ends_with(".slice")) {
            return Err(reject("it must end in .service, .scope, or .slice"));
        }
        // A template unit left uninstantiated cannot be addressed.
        if name.contains('@') && name.split('@').nth(1) == Some(".service") {
            return Err(reject("the template instance is empty"));
        }
        Ok(Self(name.to_string()))
    }

    /// The unit name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A unit property breadram reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnitProperty {
    /// The unit's cgroup path, used to verify the target.
    ControlGroup,
    /// The unit's active state.
    ActiveState,
    /// The soft limit currently in effect.
    MemoryHigh,
    /// The reclaim protection currently in effect.
    MemoryLow,
}

impl UnitProperty {
    /// The systemd property name.
    pub fn as_str(self) -> &'static str {
        match self {
            UnitProperty::ControlGroup => "ControlGroup",
            UnitProperty::ActiveState => "ActiveState",
            UnitProperty::MemoryHigh => "MemoryHigh",
            UnitProperty::MemoryLow => "MemoryLow",
        }
    }
}

/// A typed operation on the user manager.
///
/// This is the complete vocabulary. Adding an operation is a code change with a
/// test, not a configuration option.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagerCommand {
    /// Set a bounded soft limit.
    SetMemoryHigh {
        /// The unit.
        unit: UnitName,
        /// The limit in bytes.
        bytes: u64,
    },
    /// Set a best effort reclaim protection.
    SetMemoryLow {
        /// The unit.
        unit: UnitName,
        /// The protection in bytes.
        bytes: u64,
    },
    /// Remove the soft limit.
    ClearMemoryHigh {
        /// The unit.
        unit: UnitName,
    },
    /// Remove the reclaim protection.
    ClearMemoryLow {
        /// The unit.
        unit: UnitName,
    },
    /// Read one property.
    Show {
        /// The unit.
        unit: UnitName,
        /// The property.
        property: UnitProperty,
    },
}

impl ManagerCommand {
    /// The argument vector, always aimed at the user manager.
    pub fn argv(&self) -> Vec<String> {
        let mut argv = vec![
            "systemctl".to_string(),
            "--user".to_string(),
            "--no-pager".to_string(),
        ];
        match self {
            ManagerCommand::SetMemoryHigh { unit, bytes } => {
                argv.push("set-property".to_string());
                argv.push(unit.as_str().to_string());
                argv.push(format!("MemoryHigh={bytes}"));
            }
            ManagerCommand::SetMemoryLow { unit, bytes } => {
                argv.push("set-property".to_string());
                argv.push(unit.as_str().to_string());
                argv.push(format!("MemoryLow={bytes}"));
            }
            ManagerCommand::ClearMemoryHigh { unit } => {
                argv.push("set-property".to_string());
                argv.push(unit.as_str().to_string());
                argv.push("MemoryHigh=infinity".to_string());
            }
            ManagerCommand::ClearMemoryLow { unit } => {
                argv.push("set-property".to_string());
                argv.push(unit.as_str().to_string());
                argv.push("MemoryLow=0".to_string());
            }
            ManagerCommand::Show { unit, property } => {
                argv.push("show".to_string());
                argv.push(unit.as_str().to_string());
                argv.push("--property".to_string());
                argv.push(property.as_str().to_string());
                argv.push("--value".to_string());
            }
        }
        argv
    }

    /// The journal key for the setting this command affects.
    pub fn setting_key(&self) -> Option<String> {
        let (unit, property) = match self {
            ManagerCommand::SetMemoryHigh { unit, .. }
            | ManagerCommand::ClearMemoryHigh { unit } => (unit, UnitProperty::MemoryHigh),
            ManagerCommand::SetMemoryLow { unit, .. } | ManagerCommand::ClearMemoryLow { unit } => {
                (unit, UnitProperty::MemoryLow)
            }
            ManagerCommand::Show { .. } => return None,
        };
        Some(setting_key(unit, property))
    }
}

/// The stable journal key for a unit property.
pub fn setting_key(unit: &UnitName, property: UnitProperty) -> String {
    format!("systemd:{}:{}", unit.as_str(), property.as_str())
}

/// The result of running a command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    /// The exit status.
    pub status: i32,
    /// Standard output.
    pub stdout: String,
    /// Standard error.
    pub stderr: String,
}

impl CommandOutput {
    /// A successful output with the given stdout.
    pub fn ok(stdout: impl Into<String>) -> Self {
        Self {
            status: 0,
            stdout: stdout.into(),
            stderr: String::new(),
        }
    }

    /// A failed output with the given stderr.
    pub fn failed(stderr: impl Into<String>) -> Self {
        Self {
            status: 1,
            stdout: String::new(),
            stderr: stderr.into(),
        }
    }

    /// Whether the command succeeded.
    pub fn is_ok(&self) -> bool {
        self.status == 0
    }

    /// The trimmed standard output.
    pub fn value(&self) -> &str {
        self.stdout.trim()
    }
}

/// Runs a validated command.
///
/// This is the only seam through which breadram reaches systemd, so tests drive
/// the whole control path without touching the host.
pub trait CommandRunner: Send + Sync {
    /// Run one command.
    fn run(&self, command: &ManagerCommand) -> Result<CommandOutput, ControlError>;
}

/// The real runner.
///
/// It spawns `systemctl` with an argument vector that can only have come from
/// [`ManagerCommand::argv`], so no caller can pass a program or a shell string.
#[derive(Debug, Default, Clone, Copy)]
pub struct ProcessRunner;

impl CommandRunner for ProcessRunner {
    fn run(&self, command: &ManagerCommand) -> Result<CommandOutput, ControlError> {
        let argv = command.argv();
        let Some((program, rest)) = argv.split_first() else {
            return Err(ControlError::Runner("empty argument vector".to_string()));
        };
        let output = std::process::Command::new(program)
            .args(rest)
            .output()
            .map_err(|error| ControlError::Runner(error.to_string()))?;
        Ok(CommandOutput {
            status: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        })
    }
}

/// Parse a systemd byte setting such as `4294967296` or `infinity`.
pub fn parse_bytes(value: &str) -> Option<u64> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if value == "infinity" || value == "[not set]" {
        return None;
    }
    value.parse::<u64>().ok()
}

/// A capability reading for one control, so an unavailable control explains
/// itself instead of silently doing nothing.
pub fn capability_from(output: &CommandOutput) -> CapabilityState {
    if output.is_ok() {
        CapabilityState::Available
    } else {
        CapabilityState::Unavailable {
            reason: output.stderr.trim().to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_valid_unit_name_is_accepted() {
        for name in [
            "ollama.service",
            "app-worker@1.service",
            "user.slice",
            "app-123.scope",
        ] {
            assert!(UnitName::parse(name).is_ok(), "{name} should be valid");
        }
    }

    #[test]
    fn a_name_that_could_be_an_option_is_rejected() {
        assert!(matches!(
            UnitName::parse("--version"),
            Err(ControlError::InvalidUnit { .. })
        ));
        assert!(matches!(
            UnitName::parse("-H.service"),
            Err(ControlError::InvalidUnit { .. })
        ));
    }

    #[test]
    fn a_path_separator_is_rejected() {
        assert!(UnitName::parse("../../etc/passwd").is_err());
        assert!(UnitName::parse("foo/bar.service").is_err());
    }

    #[test]
    fn whitespace_and_shell_metacharacters_are_rejected() {
        for name in [
            "a b.service",
            "a;b.service",
            "a$(id).service",
            "a`id`.service",
        ] {
            assert!(UnitName::parse(name).is_err(), "{name} must be rejected");
        }
    }

    #[test]
    fn a_missing_or_unknown_suffix_is_rejected() {
        assert!(UnitName::parse("ollama").is_err());
        assert!(UnitName::parse("ollama.timer").is_err());
    }

    #[test]
    fn an_uninstantiated_template_is_rejected() {
        assert!(UnitName::parse("app@.service").is_err());
        assert!(UnitName::parse("app@1.service").is_ok());
    }

    #[test]
    fn every_command_addresses_the_user_manager() {
        let unit = UnitName::parse("app.service").expect("valid");
        let commands = [
            ManagerCommand::SetMemoryHigh {
                unit: unit.clone(),
                bytes: 1024,
            },
            ManagerCommand::SetMemoryLow {
                unit: unit.clone(),
                bytes: 1024,
            },
            ManagerCommand::ClearMemoryHigh { unit: unit.clone() },
            ManagerCommand::ClearMemoryLow { unit: unit.clone() },
            ManagerCommand::Show {
                unit: unit.clone(),
                property: UnitProperty::ControlGroup,
            },
        ];
        for command in commands {
            let argv = command.argv();
            assert_eq!(argv[0], "systemctl");
            assert_eq!(argv[1], "--user");
        }
    }

    #[test]
    fn the_vocabulary_contains_no_free_form_execution() {
        // Exhaustive over the enum: there is no variant that runs a program or
        // takes a command string.
        let unit = UnitName::parse("app.service").expect("valid");
        let command = ManagerCommand::SetMemoryHigh { unit, bytes: 1024 };
        match command {
            ManagerCommand::SetMemoryHigh { .. }
            | ManagerCommand::SetMemoryLow { .. }
            | ManagerCommand::ClearMemoryHigh { .. }
            | ManagerCommand::ClearMemoryLow { .. }
            | ManagerCommand::Show { .. } => {}
        }
    }

    #[test]
    fn a_soft_limit_renders_as_a_set_property() {
        let command = ManagerCommand::SetMemoryHigh {
            unit: UnitName::parse("app.service").expect("valid"),
            bytes: 2_147_483_648,
        };
        let argv = command.argv();
        assert_eq!(argv[3], "set-property");
        assert_eq!(argv[4], "app.service");
        assert_eq!(argv[5], "MemoryHigh=2147483648");
        assert_eq!(
            command.setting_key().as_deref(),
            Some("systemd:app.service:MemoryHigh")
        );
    }

    #[test]
    fn clearing_writes_the_neutral_value() {
        let unit = UnitName::parse("app.service").expect("valid");
        assert!(ManagerCommand::ClearMemoryHigh { unit: unit.clone() }
            .argv()
            .contains(&"MemoryHigh=infinity".to_string()));
        assert!(ManagerCommand::ClearMemoryLow { unit }
            .argv()
            .contains(&"MemoryLow=0".to_string()));
    }

    #[test]
    fn a_show_has_no_setting_key() {
        let command = ManagerCommand::Show {
            unit: UnitName::parse("app.service").expect("valid"),
            property: UnitProperty::ActiveState,
        };
        assert_eq!(command.setting_key(), None);
        assert!(command.argv().contains(&"ActiveState".to_string()));
    }

    #[test]
    fn infinity_reads_as_no_limit() {
        assert_eq!(parse_bytes("infinity"), None);
        assert_eq!(parse_bytes("[not set]"), None);
        assert_eq!(parse_bytes(""), None);
        assert_eq!(parse_bytes(" 4294967296\n"), Some(4_294_967_296));
    }

    #[test]
    fn a_refusal_is_reported_with_its_reason() {
        let state = capability_from(&CommandOutput::failed("Access denied"));
        assert!(!state.is_usable());
        assert_eq!(state.reason(), Some("Access denied"));
        assert!(capability_from(&CommandOutput::ok("4294967296")).is_usable());
    }
}
