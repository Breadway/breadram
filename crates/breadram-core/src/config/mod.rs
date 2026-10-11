//! Validated configuration.
//!
//! Configuration is a versioned TOML document. It is parsed, migrated, and
//! validated before it is ever applied, so an invalid file is rejected as a
//! whole rather than half applied. [`ConfigStore::reload`] keeps the previous
//! valid configuration when the new text is bad, and exposes the error.

pub mod credentials;
pub mod profile;
pub mod rule;

pub use credentials::{check_credential_permissions, Credentials, CREDENTIAL_FILE_MODE, REDACTED};
pub use profile::Profile;
pub use rule::{resolve_priority, select_rule, Priority, Rule, RuleMatch};

use serde::{Deserialize, Serialize};

use crate::ManagementMode;

/// The configuration schema version this build writes and understands.
pub const CURRENT_CONFIG_VERSION: u32 = 1;

/// The smallest budget that is not obviously impossible (1 MiB).
pub const MIN_BUDGET_BYTES: u64 = 1024 * 1024;
/// Upper bound on retained history records.
pub const MAX_HISTORY_RECORDS: u32 = 1_000_000;
/// Upper bound on retained history age.
pub const MAX_HISTORY_DAYS: u32 = 365;
/// Upper bound on retained history size (1 GiB).
pub const MAX_HISTORY_BYTES: u64 = 1024 * 1024 * 1024;
/// Upper bound on queued jobs.
pub const MAX_QUEUED_JOBS: u32 = 10_000;

/// The interface settings for one provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    /// A local Ollama service.
    Ollama,
    /// A llama.cpp server.
    LlamaCpp,
    /// A vLLM server.
    Vllm,
    /// The breadsearch model lifecycle.
    Breadsearch,
    /// A generic local hook adapter.
    Hook,
}

/// An action the daemon may take automatically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionKind {
    /// A bounded cgroup soft limit.
    CgroupSoftLimit,
    /// Bounded reclaim protection.
    ReclaimProtection,
    /// Unload an idle model through a provider adapter.
    UnloadIdleModel,
    /// Sleep a vLLM backend.
    SleepModel,
    /// Reduce a cooperative job's concurrency.
    ReduceJobConcurrency,
    /// Delay a background job until resources permit.
    QueueBackgroundJob,
}

/// Pressure thresholds that move the policy between states.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PressureThresholds {
    /// `some avg10` at or above which the host is pressured.
    pub pressured_some_avg10: f64,
    /// `some avg10` at or above which the host is critical.
    pub critical_some_avg10: f64,
    /// Available memory fraction at or below which the host is critical.
    pub critical_available_fraction: f64,
}

impl Default for PressureThresholds {
    fn default() -> Self {
        Self {
            pressured_some_avg10: 5.0,
            critical_some_avg10: 25.0,
            critical_available_fraction: 0.05,
        }
    }
}

/// Dwell and cooldown timings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Timing {
    /// How long a pressure reading must hold before the state changes.
    pub settle_ms: u64,
    /// How long to wait before acting on the same target again.
    pub cooldown_ms: u64,
    /// Minimum time in a state before it may change again.
    pub min_dwell_ms: u64,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            settle_ms: 3_000,
            cooldown_ms: 30_000,
            min_dwell_ms: 10_000,
        }
    }
}

/// Workloads that must not be touched automatically.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Protection {
    /// Workload scope keys that are protected.
    pub protected: Vec<String>,
}

/// A bounded memory budget for one workload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Budget {
    /// The workload scope key.
    pub scope: String,
    /// The budget in bytes.
    pub max_bytes: u64,
}

/// A configured provider endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderEndpoint {
    /// A unique name.
    pub name: String,
    /// Which provider this is.
    pub kind: ProviderKind,
    /// The endpoint URL.
    pub url: String,
    /// The name of a credential in the separate credential file.
    #[serde(default)]
    pub credential_ref: Option<String>,
    /// Whether automatic actions may target this endpoint.
    #[serde(default)]
    pub allow_automation: bool,
}

/// Which actions automation may take.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Automation {
    /// The allowed actions. Empty means automation does nothing.
    pub allowed_actions: Vec<ActionKind>,
}

/// Background job admission settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct JobPolicy {
    /// Whether job admission is enabled.
    pub enabled: bool,
    /// The maximum number of queued jobs.
    pub max_queued: u32,
    /// The default per job memory estimate in bytes.
    pub default_estimate_bytes: u64,
}

impl Default for JobPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            max_queued: 32,
            default_estimate_bytes: 2 * 1024 * 1024 * 1024,
        }
    }
}

/// Bounded history retention.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct History {
    /// Maximum retained records.
    pub max_records: u32,
    /// Maximum retained age in days.
    pub max_age_days: u32,
    /// Maximum retained size in bytes.
    pub max_bytes: u64,
}

impl Default for History {
    fn default() -> Self {
        Self {
            max_records: 10_000,
            max_age_days: 14,
            max_bytes: 16 * 1024 * 1024,
        }
    }
}

/// A complete, validated configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// The schema version.
    pub version: u32,
    /// The management mode.
    pub mode: ManagementMode,
    /// Pressure thresholds.
    pub pressure: PressureThresholds,
    /// Dwell and cooldown timings.
    pub timing: Timing,
    /// Protected workloads.
    pub protection: Protection,
    /// Bounded per workload budgets.
    pub budgets: Vec<Budget>,
    /// Provider endpoints.
    pub providers: Vec<ProviderEndpoint>,
    /// Which actions automation may take.
    pub automation: Automation,
    /// Background job settings.
    pub jobs: JobPolicy,
    /// History retention.
    pub history: History,
    /// Per workload rules.
    pub rules: Vec<Rule>,
    /// The legacy version 0 `automatic` switch, migrated into `mode` and never
    /// written back out.
    #[serde(default, rename = "automatic", skip_serializing)]
    pub legacy_automatic: Option<bool>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            version: CURRENT_CONFIG_VERSION,
            mode: ManagementMode::default(),
            pressure: PressureThresholds::default(),
            timing: Timing::default(),
            protection: Protection::default(),
            budgets: Vec::new(),
            providers: Vec::new(),
            automation: Automation::default(),
            jobs: JobPolicy::default(),
            history: History::default(),
            rules: Vec::new(),
            legacy_automatic: None,
        }
    }
}

impl Config {
    /// Parse, migrate, and validate configuration text.
    pub fn from_toml(text: &str) -> Result<Config, ConfigError> {
        let mut config: Config =
            toml::from_str(text).map_err(|err| ConfigError::Parse(err.to_string()))?;
        config.normalize()?;
        validate(&config)?;
        Ok(config)
    }

    fn normalize(&mut self) -> Result<(), ConfigError> {
        if self.version > CURRENT_CONFIG_VERSION {
            return Err(ConfigError::FutureVersion {
                found: self.version,
                supported: CURRENT_CONFIG_VERSION,
            });
        }
        if self.version == 0 || self.legacy_automatic.is_some() {
            if let Some(automatic) = self.legacy_automatic.take() {
                self.mode = if automatic {
                    ManagementMode::Automatic
                } else {
                    ManagementMode::Monitor
                };
            }
            self.version = CURRENT_CONFIG_VERSION;
        }
        Ok(())
    }
}

/// A configuration failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    /// The document could not be parsed.
    #[error("cannot parse configuration: {0}")]
    Parse(String),
    /// The document declares a schema version this build does not know.
    #[error("configuration version {found} is newer than the supported version {supported}")]
    FutureVersion {
        /// The version found in the document.
        found: u32,
        /// The version this build supports.
        supported: u32,
    },
    /// An unknown profile name.
    #[error("unknown profile {0:?}")]
    UnknownProfile(String),
    /// A threshold was out of range or inconsistent.
    #[error("invalid threshold {field}: {reason}")]
    InvalidThreshold {
        /// The field name.
        field: String,
        /// Why it is invalid.
        reason: String,
    },
    /// A timing value was out of range or inconsistent.
    #[error("invalid timing {field}: {reason}")]
    InvalidTiming {
        /// The field name.
        field: String,
        /// Why it is invalid.
        reason: String,
    },
    /// A budget was impossible.
    #[error("impossible budget for {scope}: {reason}")]
    InvalidBudget {
        /// The scope key.
        scope: String,
        /// Why it is impossible.
        reason: String,
    },
    /// Two budgets target the same scope.
    #[error("duplicate budget for scope {0}")]
    DuplicateBudget(String),
    /// An endpoint was missing, malformed, or unsafe for automation.
    #[error("unsafe endpoint {name}: {reason}")]
    UnsafeEndpoint {
        /// The provider name.
        name: String,
        /// Why it is unsafe.
        reason: String,
    },
    /// Two providers share a name.
    #[error("duplicate provider {0}")]
    DuplicateProvider(String),
    /// Two rules share a name.
    #[error("duplicate rule {0}")]
    DuplicateRule(String),
    /// Two rules match the same workloads with different priorities.
    #[error("conflicting rule {name}: {reason}")]
    ConflictingRule {
        /// The later rule's name.
        name: String,
        /// Why they conflict.
        reason: String,
    },
    /// A history setting was out of range.
    #[error("invalid history setting {field}: {reason}")]
    InvalidHistory {
        /// The field name.
        field: String,
        /// Why it is invalid.
        reason: String,
    },
    /// A job setting was out of range.
    #[error("invalid job setting {field}: {reason}")]
    InvalidJob {
        /// The field name.
        field: String,
        /// Why it is invalid.
        reason: String,
    },
    /// The credential file is too permissive.
    #[error("credential file mode {mode:o} is too permissive, expected at most {max:o}")]
    CredentialPermissions {
        /// The mode found.
        mode: u32,
        /// The maximum allowed mode.
        max: u32,
    },
}

/// Validate a normalized configuration.
pub fn validate(config: &Config) -> Result<(), ConfigError> {
    validate_pressure(&config.pressure)?;
    validate_timing(&config.timing)?;
    validate_budgets(&config.budgets)?;
    validate_providers(&config.providers)?;
    validate_rules(&config.rules)?;
    validate_history(&config.history)?;
    validate_jobs(&config.jobs)?;
    Ok(())
}

fn validate_pressure(pressure: &PressureThresholds) -> Result<(), ConfigError> {
    let some = pressure.pressured_some_avg10;
    let critical = pressure.critical_some_avg10;
    let fraction = pressure.critical_available_fraction;

    if !(some > 0.0 && some < critical) {
        return Err(ConfigError::InvalidThreshold {
            field: "pressured_some_avg10".to_string(),
            reason: format!(
                "must be greater than 0 and less than critical_some_avg10 ({critical})"
            ),
        });
    }
    if !(critical > 0.0 && critical <= 100.0) {
        return Err(ConfigError::InvalidThreshold {
            field: "critical_some_avg10".to_string(),
            reason: "must be greater than 0 and at most 100".to_string(),
        });
    }
    if !(fraction > 0.0 && fraction < 1.0) {
        return Err(ConfigError::InvalidThreshold {
            field: "critical_available_fraction".to_string(),
            reason: "must be greater than 0 and less than 1".to_string(),
        });
    }
    Ok(())
}

fn validate_timing(timing: &Timing) -> Result<(), ConfigError> {
    if timing.cooldown_ms == 0 {
        return Err(ConfigError::InvalidTiming {
            field: "cooldown_ms".to_string(),
            reason: "must be greater than 0".to_string(),
        });
    }
    if timing.min_dwell_ms == 0 {
        return Err(ConfigError::InvalidTiming {
            field: "min_dwell_ms".to_string(),
            reason: "must be greater than 0".to_string(),
        });
    }
    if timing.settle_ms > timing.min_dwell_ms {
        return Err(ConfigError::InvalidTiming {
            field: "settle_ms".to_string(),
            reason: "must not exceed min_dwell_ms".to_string(),
        });
    }
    Ok(())
}

fn validate_budgets(budgets: &[Budget]) -> Result<(), ConfigError> {
    let mut seen: Vec<&str> = Vec::new();
    for budget in budgets {
        if budget.max_bytes < MIN_BUDGET_BYTES {
            return Err(ConfigError::InvalidBudget {
                scope: budget.scope.clone(),
                reason: format!("must be at least {MIN_BUDGET_BYTES} bytes"),
            });
        }
        if seen.contains(&budget.scope.as_str()) {
            return Err(ConfigError::DuplicateBudget(budget.scope.clone()));
        }
        seen.push(&budget.scope);
    }
    Ok(())
}

fn validate_providers(providers: &[ProviderEndpoint]) -> Result<(), ConfigError> {
    let mut seen: Vec<&str> = Vec::new();
    for provider in providers {
        if seen.contains(&provider.name.as_str()) {
            return Err(ConfigError::DuplicateProvider(provider.name.clone()));
        }
        seen.push(&provider.name);
        validate_endpoint(provider)?;
    }
    Ok(())
}

fn validate_endpoint(provider: &ProviderEndpoint) -> Result<(), ConfigError> {
    let unsafe_endpoint = |reason: &str| ConfigError::UnsafeEndpoint {
        name: provider.name.clone(),
        reason: reason.to_string(),
    };

    if let Some(rest) = provider.url.strip_prefix("unix:") {
        if rest.is_empty() {
            return Err(unsafe_endpoint("unix endpoint has no path"));
        }
        return Ok(());
    }

    let (scheme, rest) = provider
        .url
        .split_once("://")
        .ok_or_else(|| unsafe_endpoint("endpoint needs a scheme such as http://"))?;
    if scheme != "http" && scheme != "https" {
        return Err(unsafe_endpoint("scheme must be http, https, or unix"));
    }
    let authority = rest.split(['/', '?']).next().unwrap_or_default();
    if authority.is_empty() {
        return Err(unsafe_endpoint("endpoint has no host"));
    }
    if authority.contains('@') {
        return Err(unsafe_endpoint(
            "endpoint must not embed credentials, use credential_ref instead",
        ));
    }

    let host = authority
        .rsplit_once(':')
        .map_or(authority, |(host, _port)| host);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let loopback = host == "localhost" || host.starts_with("127.") || host == "::1";

    if provider.allow_automation && !loopback {
        return Err(unsafe_endpoint(
            "automation is limited to loopback or unix endpoints",
        ));
    }
    if scheme == "http" && !loopback {
        return Err(unsafe_endpoint(
            "plain http is limited to loopback, use https for a remote endpoint",
        ));
    }
    Ok(())
}

fn validate_rules(rules: &[Rule]) -> Result<(), ConfigError> {
    let mut names: Vec<&str> = Vec::new();
    for rule in rules {
        if names.contains(&rule.name.as_str()) {
            return Err(ConfigError::DuplicateRule(rule.name.clone()));
        }
        names.push(&rule.name);
    }

    for (index, first) in rules.iter().enumerate() {
        for second in &rules[index + 1..] {
            if first.match_on == second.match_on && first.priority != second.priority {
                return Err(ConfigError::ConflictingRule {
                    name: second.name.clone(),
                    reason: format!(
                        "matches the same workloads as {:?} with a different priority",
                        first.name
                    ),
                });
            }
        }
    }
    Ok(())
}

fn validate_history(history: &History) -> Result<(), ConfigError> {
    if history.max_records == 0 || history.max_records > MAX_HISTORY_RECORDS {
        return Err(ConfigError::InvalidHistory {
            field: "max_records".to_string(),
            reason: format!("must be between 1 and {MAX_HISTORY_RECORDS}"),
        });
    }
    if history.max_age_days == 0 || history.max_age_days > MAX_HISTORY_DAYS {
        return Err(ConfigError::InvalidHistory {
            field: "max_age_days".to_string(),
            reason: format!("must be between 1 and {MAX_HISTORY_DAYS}"),
        });
    }
    if history.max_bytes == 0 || history.max_bytes > MAX_HISTORY_BYTES {
        return Err(ConfigError::InvalidHistory {
            field: "max_bytes".to_string(),
            reason: format!("must be between 1 and {MAX_HISTORY_BYTES}"),
        });
    }
    Ok(())
}

fn validate_jobs(jobs: &JobPolicy) -> Result<(), ConfigError> {
    if jobs.max_queued > MAX_QUEUED_JOBS {
        return Err(ConfigError::InvalidJob {
            field: "max_queued".to_string(),
            reason: format!("must be at most {MAX_QUEUED_JOBS}"),
        });
    }
    if jobs.enabled && jobs.default_estimate_bytes == 0 {
        return Err(ConfigError::InvalidJob {
            field: "default_estimate_bytes".to_string(),
            reason: "must be greater than 0 when job admission is enabled".to_string(),
        });
    }
    Ok(())
}

/// Holds the current valid configuration and reloads it atomically.
#[derive(Debug, Clone)]
pub struct ConfigStore {
    current: Config,
}

impl ConfigStore {
    /// Start from an already valid configuration.
    pub fn new(initial: Config) -> Self {
        Self { current: initial }
    }

    /// The current valid configuration.
    pub fn current(&self) -> &Config {
        &self.current
    }

    /// Replace the configuration from text.
    ///
    /// On any parse or validation failure the previous configuration is kept and
    /// the error is returned, so a bad reload never leaves a half applied state.
    pub fn reload(&mut self, toml_text: &str) -> Result<(), ConfigError> {
        let next = Config::from_toml(toml_text)?;
        self.current = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FULL: &str = r#"
version = 1
mode = "automatic"

[pressure]
pressured_some_avg10 = 4.0
critical_some_avg10 = 20.0
critical_available_fraction = 0.08

[timing]
settle_ms = 2000
cooldown_ms = 20000
min_dwell_ms = 9000

[protection]
protected = ["unit:sshd.service"]

[[budgets]]
scope = "unit:ollama.service"
max_bytes = 8589934592

[[providers]]
name = "ollama"
kind = "ollama"
url = "http://127.0.0.1:11434"
credential_ref = "ollama"
allow_automation = true

[automation]
allowed_actions = ["cgroup_soft_limit", "reclaim_protection", "unload_idle_model"]

[jobs]
enabled = true
max_queued = 16
default_estimate_bytes = 2147483648

[history]
max_records = 5000
max_age_days = 7
max_bytes = 8388608

[[rules]]
name = "protect-compositor"
match = { kind = "executable", value = "Hyprland" }
priority = "protected"

[[rules]]
name = "background-builders"
match = { kind = "executable", value = "cargo" }
priority = "background"
budget_bytes = 2147483648
"#;

    #[test]
    fn parses_a_full_configuration() {
        let config = Config::from_toml(FULL).unwrap();
        assert_eq!(config.version, CURRENT_CONFIG_VERSION);
        assert_eq!(config.mode, ManagementMode::Automatic);
        assert_eq!(config.budgets.len(), 1);
        assert_eq!(config.providers.len(), 1);
        assert_eq!(config.rules.len(), 2);
        assert_eq!(config.automation.allowed_actions.len(), 3);
    }

    #[test]
    fn an_empty_document_is_the_safe_default() {
        let config = Config::from_toml("").unwrap();
        assert_eq!(config.mode, ManagementMode::Monitor);
        assert!(config.automation.allowed_actions.is_empty());
    }

    #[test]
    fn a_legacy_automatic_switch_migrates_into_mode() {
        let config = Config::from_toml("automatic = true\n").unwrap();
        assert_eq!(config.mode, ManagementMode::Automatic);
        assert_eq!(config.version, CURRENT_CONFIG_VERSION);
        assert_eq!(config.legacy_automatic, None);
    }

    #[test]
    fn a_future_version_is_rejected() {
        assert!(matches!(
            Config::from_toml("version = 99\n"),
            Err(ConfigError::FutureVersion { found: 99, .. })
        ));
    }

    #[test]
    fn an_impossible_budget_is_rejected() {
        let text = "[[budgets]]\nscope = \"unit:x.service\"\nmax_bytes = 10\n";
        assert!(matches!(
            Config::from_toml(text),
            Err(ConfigError::InvalidBudget { .. })
        ));
    }

    #[test]
    fn duplicate_budgets_are_rejected() {
        let text = "[[budgets]]\nscope = \"unit:x.service\"\nmax_bytes = 1048576\n\n\
                    [[budgets]]\nscope = \"unit:x.service\"\nmax_bytes = 2097152\n";
        assert!(matches!(
            Config::from_toml(text),
            Err(ConfigError::DuplicateBudget(_))
        ));
    }

    #[test]
    fn a_remote_automation_endpoint_is_rejected() {
        let text = "[[providers]]\nname = \"v\"\nkind = \"vllm\"\n\
                    url = \"https://gpu.example.net:8000\"\nallow_automation = true\n";
        assert!(matches!(
            Config::from_toml(text),
            Err(ConfigError::UnsafeEndpoint { .. })
        ));
    }

    #[test]
    fn a_remote_observation_endpoint_over_https_is_allowed() {
        let text = "[[providers]]\nname = \"v\"\nkind = \"vllm\"\n\
                    url = \"https://gpu.example.net:8000\"\n";
        assert!(Config::from_toml(text).is_ok());
    }

    #[test]
    fn a_plain_http_remote_endpoint_is_rejected() {
        let text = "[[providers]]\nname = \"v\"\nkind = \"vllm\"\n\
                    url = \"http://gpu.example.net:8000\"\n";
        assert!(matches!(
            Config::from_toml(text),
            Err(ConfigError::UnsafeEndpoint { .. })
        ));
    }

    #[test]
    fn an_endpoint_with_embedded_credentials_is_rejected() {
        let text = "[[providers]]\nname = \"v\"\nkind = \"vllm\"\n\
                    url = \"http://user:pass@127.0.0.1:8000\"\n";
        assert!(matches!(
            Config::from_toml(text),
            Err(ConfigError::UnsafeEndpoint { .. })
        ));
    }

    #[test]
    fn a_unix_endpoint_is_accepted() {
        let text = "[[providers]]\nname = \"h\"\nkind = \"hook\"\n\
                    url = \"unix:/run/breadram/hook.sock\"\nallow_automation = true\n";
        assert!(Config::from_toml(text).is_ok());
    }

    #[test]
    fn duplicate_providers_are_rejected() {
        let text =
            "[[providers]]\nname = \"a\"\nkind = \"ollama\"\nurl = \"http://127.0.0.1:1\"\n\n\
                    [[providers]]\nname = \"a\"\nkind = \"ollama\"\nurl = \"http://127.0.0.1:2\"\n";
        assert!(matches!(
            Config::from_toml(text),
            Err(ConfigError::DuplicateProvider(_))
        ));
    }

    #[test]
    fn unsupported_actions_are_rejected_at_parse_time() {
        let text = "[automation]\nallowed_actions = [\"reboot_machine\"]\n";
        assert!(matches!(
            Config::from_toml(text),
            Err(ConfigError::Parse(_))
        ));
    }

    #[test]
    fn duplicate_rules_are_rejected() {
        let text = "[[rules]]\nname = \"r\"\nmatch = { kind = \"executable\", value = \"a\" }\npriority = \"normal\"\n\n\
                    [[rules]]\nname = \"r\"\nmatch = { kind = \"executable\", value = \"b\" }\npriority = \"normal\"\n";
        assert!(matches!(
            Config::from_toml(text),
            Err(ConfigError::DuplicateRule(_))
        ));
    }

    #[test]
    fn conflicting_rules_are_rejected() {
        let text = "[[rules]]\nname = \"a\"\nmatch = { kind = \"executable\", value = \"x\" }\npriority = \"protected\"\n\n\
                    [[rules]]\nname = \"b\"\nmatch = { kind = \"executable\", value = \"x\" }\npriority = \"background\"\n";
        assert!(matches!(
            Config::from_toml(text),
            Err(ConfigError::ConflictingRule { .. })
        ));
    }

    #[test]
    fn inconsistent_thresholds_are_rejected() {
        let text = "[pressure]\npressured_some_avg10 = 30.0\ncritical_some_avg10 = 10.0\n";
        assert!(matches!(
            Config::from_toml(text),
            Err(ConfigError::InvalidThreshold { .. })
        ));
    }

    #[test]
    fn settle_longer_than_the_minimum_dwell_is_rejected() {
        let text = "[timing]\nsettle_ms = 20000\nmin_dwell_ms = 5000\n";
        assert!(matches!(
            Config::from_toml(text),
            Err(ConfigError::InvalidTiming { .. })
        ));
    }

    #[test]
    fn an_absent_provider_does_not_invalidate_the_rest() {
        // A config with no providers at all is perfectly valid.
        let config =
            Config::from_toml("[protection]\nprotected = [\"unit:sshd.service\"]\n").unwrap();
        assert!(config.providers.is_empty());
        assert_eq!(config.protection.protected.len(), 1);
    }

    #[test]
    fn reload_keeps_the_previous_configuration_on_failure() {
        let mut store = ConfigStore::new(Profile::Desktop.resolve());
        let before = store.current().clone();

        let err = store.reload("[[budgets]]\nscope = \"x\"\nmax_bytes = 1\n");
        assert!(err.is_err());
        assert_eq!(store.current(), &before, "previous config must survive");

        assert!(store.reload("mode = \"automatic\"\n").is_ok());
        assert_eq!(store.current().mode, ManagementMode::Automatic);
    }
}
