//! Endpoint credentials, kept out of the main configuration.
//!
//! Credentials live in their own file with restricted permissions, referenced by
//! name from `[[providers]]`. The type never reveals a value through `Debug` or
//! any other formatting, so a secret cannot leak into a log by accident.

use std::collections::BTreeMap;
use std::fmt;

use super::ConfigError;

/// The most permissive mode a credential file may have: owner read and write.
pub const CREDENTIAL_FILE_MODE: u32 = 0o600;

/// The placeholder used wherever a secret would otherwise be printed.
pub const REDACTED: &str = "<redacted>";

/// Reject a credential file that is group or world accessible.
pub fn check_credential_permissions(mode: u32) -> Result<(), ConfigError> {
    if mode & 0o077 == 0 {
        Ok(())
    } else {
        Err(ConfigError::CredentialPermissions {
            mode,
            max: CREDENTIAL_FILE_MODE,
        })
    }
}

/// Named secrets whose formatting never reveals a value.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct Credentials {
    entries: BTreeMap<String, String>,
}

impl Credentials {
    /// An empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add or replace a secret.
    pub fn insert(&mut self, name: impl Into<String>, secret: impl Into<String>) {
        self.entries.insert(name.into(), secret.into());
    }

    /// Look up a secret.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.entries.get(name).map(String::as_str)
    }

    /// The configured names, which are safe to show.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(String::as_str)
    }

    /// A copy with every value replaced, safe to log or export.
    pub fn redacted(&self) -> BTreeMap<String, &'static str> {
        self.entries
            .keys()
            .map(|name| (name.clone(), REDACTED))
            .collect()
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.entries.keys().map(|name| (name, REDACTED)))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_never_prints_a_secret() {
        let mut credentials = Credentials::new();
        credentials.insert("ollama", "super-secret-token");
        let rendered = format!("{credentials:?}");
        assert!(!rendered.contains("super-secret-token"));
        assert!(rendered.contains("ollama"));
    }

    #[test]
    fn redacted_keeps_names_and_hides_values() {
        let mut credentials = Credentials::new();
        credentials.insert("vllm", "abc123");
        let redacted = credentials.redacted();
        assert_eq!(redacted.get("vllm"), Some(&REDACTED));
        assert!(!format!("{redacted:?}").contains("abc123"));
    }

    #[test]
    fn owner_only_mode_is_accepted() {
        assert_eq!(check_credential_permissions(0o600), Ok(()));
        assert_eq!(check_credential_permissions(0o400), Ok(()));
    }

    #[test]
    fn group_or_world_readable_is_rejected() {
        assert!(matches!(
            check_credential_permissions(0o640),
            Err(ConfigError::CredentialPermissions { .. })
        ));
        assert!(matches!(
            check_credential_permissions(0o604),
            Err(ConfigError::CredentialPermissions { .. })
        ));
    }
}
