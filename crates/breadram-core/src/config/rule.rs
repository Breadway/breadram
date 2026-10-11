//! Per-workload rules and the precedence used to resolve them.
//!
//! Rules match on stable workload identity, never on a pid. Precedence is by
//! specificity, and a tie is broken by the earlier rule in the file, so a
//! narrow rule can override a broad one and the order in the file is
//! predictable.

use serde::{Deserialize, Serialize};

use crate::inventory::Workload;

/// How a rule identifies the workloads it applies to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RuleMatch {
    /// An exact workload scope, for example `unit:ollama.service`.
    Scope {
        /// The scope key to compare against.
        value: String,
    },
    /// A systemd unit name, with or without the `.service` suffix as configured.
    Unit {
        /// The unit name.
        value: String,
    },
    /// An executable identity, for example `python3`.
    Executable {
        /// The executable file name.
        value: String,
    },
    /// A cgroup path prefix.
    CgroupPrefix {
        /// The path prefix.
        value: String,
    },
}

impl RuleMatch {
    /// How specific this match is. Higher wins.
    pub fn specificity(&self) -> u8 {
        match self {
            RuleMatch::Scope { .. } => 4,
            RuleMatch::Unit { .. } => 3,
            RuleMatch::Executable { .. } => 2,
            RuleMatch::CgroupPrefix { .. } => 1,
        }
    }

    /// The configured value.
    pub fn value(&self) -> &str {
        match self {
            RuleMatch::Scope { value }
            | RuleMatch::Unit { value }
            | RuleMatch::Executable { value }
            | RuleMatch::CgroupPrefix { value } => value,
        }
    }

    /// Whether this match applies to a workload.
    pub fn matches(&self, workload: &Workload) -> bool {
        match self {
            RuleMatch::Scope { value } => workload.id.scope == *value,
            RuleMatch::Unit { value } => {
                workload.id.scope == format!("unit:{value}") || workload.name == *value
            }
            RuleMatch::Executable { value } => {
                workload.id.scope == format!("exe:{value}") || workload.name == *value
            }
            RuleMatch::CgroupPrefix { value } => {
                workload.id.scope.starts_with(&format!("cgroup:{value}"))
            }
        }
    }
}

/// A workload's scheduling priority.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Priority {
    /// First to yield under pressure.
    Background,
    /// The default.
    #[default]
    Normal,
    /// Last to yield, and shielded from automatic action.
    Protected,
}

impl std::fmt::Display for Priority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Priority::Background => "background",
            Priority::Normal => "normal",
            Priority::Protected => "protected",
        })
    }
}

/// A named rule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Rule {
    /// A unique name, used in diagnostics.
    pub name: String,
    /// What the rule matches.
    #[serde(rename = "match")]
    pub match_on: RuleMatch,
    /// The priority it assigns.
    pub priority: Priority,
    /// An optional bounded memory budget in bytes.
    #[serde(default)]
    pub budget_bytes: Option<u64>,
}

/// The rule that applies to a workload, or `None` if no rule matches.
///
/// The most specific matching rule wins; ties go to the earlier rule in the
/// list.
pub fn select_rule<'a>(rules: &'a [Rule], workload: &Workload) -> Option<&'a Rule> {
    let mut best: Option<&Rule> = None;
    for rule in rules {
        if !rule.match_on.matches(workload) {
            continue;
        }
        match best {
            None => best = Some(rule),
            Some(current) if rule.match_on.specificity() > current.match_on.specificity() => {
                best = Some(rule);
            }
            _ => {}
        }
    }
    best
}

/// The priority for a workload, defaulting to [`Priority::Normal`].
pub fn resolve_priority(rules: &[Rule], workload: &Workload) -> Priority {
    select_rule(rules, workload).map_or(Priority::Normal, |rule| rule.priority)
}

/// Explain priority without treating a missing focus hint as idle evidence.
pub fn priority_with_source(
    rules: &[Rule],
    workload: &Workload,
    active_hint: Option<&str>,
    serves_protected: bool,
) -> (Priority, String) {
    if let Some(rule) = select_rule(rules, workload) {
        return (rule.priority, format!("explicit rule {}", rule.name));
    }
    if serves_protected {
        return (
            Priority::Protected,
            "dependency of a protected workload".into(),
        );
    }
    if let Some(hint) = active_hint {
        return (Priority::Protected, format!("active workload hint: {hint}"));
    }
    (
        Priority::Normal,
        "standalone default; missing activity is not idle".into(),
    )
}

/// Promotion is immediate for safety; demotion waits for stable absence.
/// Clock reversal resets the wait rather than manufacturing elapsed time.
#[derive(Default)]
pub struct HintDebouncer {
    active: bool,
    absent_since_ms: Option<u64>,
}
impl HintDebouncer {
    pub fn update(&mut self, observed_active: bool, now_ms: u64, dwell_ms: u64) -> bool {
        if observed_active {
            self.active = true;
            self.absent_since_ms = None;
        } else if self.active {
            let since = self.absent_since_ms.get_or_insert(now_ms);
            if now_ms < *since {
                *since = now_ms;
            }
            if now_ms.saturating_sub(*since) >= dwell_ms {
                self.active = false;
                self.absent_since_ms = None;
            }
        }
        self.active
    }

    /// Whether the hint is currently held active.
    pub fn is_active(&self) -> bool {
        self.active
    }
}

/// Per-scope debounced activity hints for the whole host.
///
/// Promotion is immediate, so work that starts is never reclaimed; demotion
/// waits for stable absence, so one missing reading cannot demote a workload and
/// then re-promote it, which would oscillate reclaim and reload.
#[derive(Default)]
pub struct HintState {
    per_scope: std::collections::BTreeMap<String, HintDebouncer>,
}

impl HintState {
    /// Fold one observation into the hint for a scope and report whether it is
    /// active now.
    pub fn update(
        &mut self,
        scope: &str,
        observed_active: bool,
        now_ms: u64,
        dwell_ms: u64,
    ) -> bool {
        self.per_scope.entry(scope.to_string()).or_default().update(
            observed_active,
            now_ms,
            dwell_ms,
        )
    }

    /// Whether the scope is currently held active.
    pub fn is_active(&self, scope: &str) -> bool {
        self.per_scope
            .get(scope)
            .is_some_and(HintDebouncer::is_active)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory::{ControlLevel, MemoryAccounting, WorkloadId};

    fn workload(name: &str, scope: &str) -> Workload {
        Workload {
            id: WorkloadId {
                scope: scope.to_string(),
                generation: 1,
            },
            name: name.to_string(),
            pids: vec![1],
            accounting: MemoryAccounting::Cgroup {
                current_bytes: Some(0),
            },
            control: ControlLevel::Eligible,
            limit_bytes: None,
        }
    }

    fn rule(name: &str, match_on: RuleMatch, priority: Priority) -> Rule {
        Rule {
            name: name.to_string(),
            match_on,
            priority,
            budget_bytes: None,
        }
    }

    #[test]
    fn explicit_rule_overrides_activity_and_dependencies() {
        let w = workload("app", "unit:app.service");
        let rules = vec![rule(
            "user",
            RuleMatch::Scope {
                value: w.id.scope.clone(),
            },
            Priority::Background,
        )];
        assert_eq!(
            priority_with_source(&rules, &w, Some("audio"), true).0,
            Priority::Background
        );
        assert_eq!(
            priority_with_source(&[], &w, None, true).0,
            Priority::Protected
        );
        assert_eq!(
            priority_with_source(&[], &w, None, false).0,
            Priority::Normal
        );
    }
    #[test]
    fn rapid_focus_changes_cannot_demote_and_backwards_clock_restarts_wait() {
        let mut hints = HintDebouncer::default();
        assert!(hints.update(true, 100, 50));
        assert!(hints.update(false, 110, 50));
        assert!(hints.update(true, 120, 50));
        assert!(hints.update(false, 130, 50));
        assert!(hints.update(false, 10, 50));
        assert!(hints.update(false, 59, 50));
        assert!(!hints.update(false, 60, 50));
    }

    #[test]
    fn a_more_specific_rule_wins() {
        let rules = vec![
            rule(
                "broad",
                RuleMatch::CgroupPrefix {
                    value: "/user.slice".into(),
                },
                Priority::Background,
            ),
            rule(
                "narrow",
                RuleMatch::Unit {
                    value: "app.service".into(),
                },
                Priority::Protected,
            ),
        ];
        let w = workload("app.service", "unit:app.service");
        assert_eq!(select_rule(&rules, &w).unwrap().name, "narrow");
    }

    #[test]
    fn an_earlier_rule_wins_a_tie() {
        let rules = vec![
            rule(
                "first",
                RuleMatch::Executable {
                    value: "python3".into(),
                },
                Priority::Protected,
            ),
            rule(
                "second",
                RuleMatch::Executable {
                    value: "python3".into(),
                },
                Priority::Background,
            ),
        ];
        let w = workload("python3", "exe:python3");
        assert_eq!(select_rule(&rules, &w).unwrap().name, "first");
    }

    #[test]
    fn no_match_defaults_to_normal() {
        let rules = vec![rule(
            "other",
            RuleMatch::Unit {
                value: "other.service".into(),
            },
            Priority::Protected,
        )];
        let w = workload("app.service", "unit:app.service");
        assert_eq!(select_rule(&rules, &w), None);
        assert_eq!(resolve_priority(&rules, &w), Priority::Normal);
    }

    #[test]
    fn scope_matches_exactly() {
        let rules = vec![rule(
            "exact",
            RuleMatch::Scope {
                value: "unit:app.service".into(),
            },
            Priority::Protected,
        )];
        assert!(select_rule(&rules, &workload("app.service", "unit:app.service")).is_some());
        assert!(select_rule(&rules, &workload("app.service", "unit:other.service")).is_none());
    }

    #[test]
    fn a_hint_promotes_at_once_and_demotes_only_after_stable_absence() {
        let mut hints = HintState::default();
        assert!(hints.update("exe:build", true, 100, 50));
        assert!(hints.is_active("exe:build"));
        // One quiet reading does not demote, so a missing sample cannot oscillate.
        assert!(hints.update("exe:build", false, 110, 50));
        assert!(hints.is_active("exe:build"));
        // Stable absence demotes.
        assert!(!hints.update("exe:build", false, 160, 50));
        assert!(!hints.is_active("exe:build"));
    }
}
