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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Priority {
    /// First to yield under pressure.
    Background,
    /// The default.
    Normal,
    /// Last to yield, and shielded from automatic action.
    Protected,
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
}
