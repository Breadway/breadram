//! The control executor.
//!
//! This is where a planned action becomes a real memory setting. It applies
//! everything through the user manager, verifies the target immediately before
//! every write, reads the previous value first so the change is reversible, and
//! reports what it actually set.
//!
//! Two honesty rules are enforced here:
//!
//! - A soft limit is a throttling hint, not a reservation, and the value written
//!   is the value the journal records.
//! - Reclaim protection is bounded by [`PROTECTION_FRACTION`] of the effective
//!   ancestor limit. Asking for more is clamped, and the clamp is reported, so
//!   protection is never quietly presented as a guarantee.

use std::path::Path;

use crate::config::ActionKind;
use crate::coordinator::executor::{
    AppliedChange, Confirmation, Executor, ExecutorError, MemoryObservation, Precondition,
};
use crate::coordinator::journal::OwnedValue;
use crate::coordinator::{ActionRequest, OwnerToken};
use crate::inventory::WorkloadId;
use crate::telemetry::fs::FileSource;
use crate::telemetry::snapshot::CGROUP_V2_ROOT;

use super::manager::{
    parse_bytes, setting_key, CommandOutput, CommandRunner, ControlError, ManagerCommand, UnitName,
    UnitProperty, MAX_REQUESTED_BYTES, MIN_REQUESTED_BYTES,
};
use super::{probe_availability, ControlAvailability, ControlTarget, ControlTargets};

/// The largest share of the effective ancestor limit reclaim protection may
/// claim.
///
/// Protecting one workload can only push reclaim onto another, so the claim is
/// bounded rather than open ended. It is a bound on best effort, not a promise.
pub const PROTECTION_FRACTION: f64 = 0.25;

/// Applies memory controls through the user manager.
///
/// The filesystem view carries `Send + Sync` because an [`Executor`] must be
/// shareable: the coordinator may be driven from the daemon's async runtime.
pub struct ControlExecutor<'a> {
    fs: &'a (dyn FileSource + Send + Sync),
    runner: &'a dyn CommandRunner,
    targets: &'a ControlTargets,
}

impl<'a> ControlExecutor<'a> {
    /// Build an executor over the existing filesystem view, command runner, and
    /// discovered targets.
    pub fn new(
        fs: &'a (dyn FileSource + Send + Sync),
        runner: &'a dyn CommandRunner,
        targets: &'a ControlTargets,
    ) -> Self {
        Self {
            fs,
            runner,
            targets,
        }
    }
}

impl ControlExecutor<'_> {
    /// Resolve a scope to a controllable target, or explain why not.
    fn resolve(&self, scope: &str) -> Result<(&ControlTarget, ControlAvailability), String> {
        let Some(target) = self.targets.get(scope) else {
            return Err(format!(
                "{scope} is not a running user unit that breadram may control"
            ));
        };
        let availability = probe_availability(self.fs, target);
        if !availability.can_control() {
            return Err(availability
                .reason()
                .unwrap_or_else(|| "memory control is not available for this unit".to_string()));
        }
        Ok((target, availability))
    }

    fn show(&self, target: &ControlTarget, property: UnitProperty) -> Result<String, String> {
        let command = ManagerCommand::Show {
            unit: target.unit.clone(),
            property,
        };
        let output = self
            .runner
            .run(&command)
            .map_err(|error| error.to_string())?;
        if !output.is_ok() {
            let reason = output.stderr.trim();
            return Err(if reason.is_empty() {
                "the manager does not know this unit".to_string()
            } else {
                reason.to_string()
            });
        }
        Ok(output.value().to_string())
    }

    /// Verify the unit is still the one the action was planned against.
    fn verify_identity(&self, target: &ControlTarget) -> Result<(), String> {
        let current = self.show(target, UnitProperty::ControlGroup)?;
        if current != target.control_group {
            return Err(format!(
                "{} now runs in {current} instead of {}",
                target.unit.as_str(),
                target.control_group
            ));
        }
        let state = self.show(target, UnitProperty::ActiveState)?;
        if state != "active" {
            return Err(format!("{} is {state}", target.unit.as_str()));
        }
        Ok(())
    }

    fn issue(&self, command: ManagerCommand) -> Result<CommandOutput, ExecutorError> {
        let output = self.runner.run(&command).map_err(|error| match error {
            ControlError::Runner(reason) => ExecutorError::Disconnected(reason),
            other => ExecutorError::Failed(other.to_string()),
        })?;
        if output.is_ok() {
            return Ok(output);
        }
        let reason = output.stderr.trim().to_string();
        let reason = if reason.is_empty() {
            "the manager refused the operation".to_string()
        } else {
            reason
        };
        if reason.to_lowercase().contains("denied") || reason.contains("Permission") {
            Err(ExecutorError::Denied(reason))
        } else {
            Err(ExecutorError::Failed(reason))
        }
    }

    /// The setting and the value to write, with any clamp explained.
    fn desired(
        &self,
        request: &ActionRequest,
        target: &ControlTarget,
        availability: &ControlAvailability,
    ) -> Result<(UnitProperty, u64, Option<String>), ExecutorError> {
        let property = match request.kind {
            ActionKind::CgroupSoftLimit => UnitProperty::MemoryHigh,
            ActionKind::ReclaimProtection => UnitProperty::MemoryLow,
            other => {
                return Err(ExecutorError::Unsupported(format!(
                    "{other:?} is not a memory control"
                )))
            }
        };
        let Some(requested) = request.limit_bytes else {
            return Err(ExecutorError::Denied(
                "the action did not name the value to apply".to_string(),
            ));
        };
        if requested < MIN_REQUESTED_BYTES {
            return Err(ExecutorError::Denied(format!(
                "{requested} bytes is below the {MIN_REQUESTED_BYTES} byte minimum"
            )));
        }
        if requested > MAX_REQUESTED_BYTES {
            return Err(ExecutorError::Denied(format!(
                "{requested} bytes is above the {MAX_REQUESTED_BYTES} byte maximum"
            )));
        }

        let ancestor = availability.effective_max_bytes;
        let (value, note) = match property {
            UnitProperty::MemoryHigh => match ancestor {
                Some(parent) if requested > parent => (
                    parent,
                    Some(format!(
                        "clamped from {requested} to the effective ancestor limit {parent}"
                    )),
                ),
                _ => (requested, None),
            },
            UnitProperty::MemoryLow => {
                let cap = ancestor.map_or(MAX_REQUESTED_BYTES, |parent| {
                    ((parent as f64) * PROTECTION_FRACTION) as u64
                });
                let cap = cap.clamp(MIN_REQUESTED_BYTES, MAX_REQUESTED_BYTES);
                if requested > cap {
                    (
                        cap,
                        Some(format!(
                            "clamped from {requested} to {cap}, a bounded share of the \
                             effective ancestor limit; protection is best effort"
                        )),
                    )
                } else {
                    (requested, None)
                }
            }
            _ => unreachable!("only the two memory properties are produced above"),
        };
        let _ = target;
        Ok((property, value, note))
    }
}

impl Executor for ControlExecutor<'_> {
    fn supports(&self, kind: ActionKind) -> bool {
        matches!(
            kind,
            ActionKind::CgroupSoftLimit | ActionKind::ReclaimProtection
        )
    }

    fn precondition(&self, request: &ActionRequest) -> Precondition {
        if !self.supports(request.kind) {
            return Precondition::Unsupported {
                reason: format!("{:?} is not a memory control", request.kind),
            };
        }
        match self.resolve(&request.target.scope) {
            Err(reason) => Precondition::Unsupported { reason },
            Ok((target, _)) => match self.verify_identity(target) {
                Ok(()) => Precondition::Ready,
                Err(reason) => Precondition::TargetGone { reason },
            },
        }
    }

    fn apply(&self, request: &ActionRequest) -> Result<AppliedChange, ExecutorError> {
        let (target, availability) = self
            .resolve(&request.target.scope)
            .map_err(ExecutorError::Denied)?;
        // Verified again here, not only in the precondition, so every write
        // checks the target it is about to change.
        self.verify_identity(target)
            .map_err(ExecutorError::Failed)?;

        let (property, value, note) = self.desired(request, target, &availability)?;
        let original = self.show(target, property).map_err(ExecutorError::Failed)?;

        let command = match property {
            UnitProperty::MemoryHigh => ManagerCommand::SetMemoryHigh {
                unit: target.unit.clone(),
                bytes: value,
            },
            UnitProperty::MemoryLow => ManagerCommand::SetMemoryLow {
                unit: target.unit.clone(),
                bytes: value,
            },
            _ => unreachable!("desired only produces the two memory properties"),
        };
        self.issue(command)?;

        Ok(AppliedChange {
            values: vec![OwnedValue {
                key: setting_key(&target.unit, property),
                original,
                last_applied: value.to_string(),
                // Stamped by the coordinator, which owns the instance identity.
                owner: OwnerToken(String::new()),
            }],
            note,
        })
    }

    fn confirm(&self, request: &ActionRequest) -> Confirmation {
        let Ok((target, availability)) = self.resolve(&request.target.scope) else {
            return Confirmation::NotYet {
                reason: "the target is no longer controllable".to_string(),
            };
        };
        let Ok((property, value, _)) = self.desired(request, target, &availability) else {
            return Confirmation::NotYet {
                reason: "the action no longer names a value to apply".to_string(),
            };
        };
        match self.show(target, property) {
            Ok(current) if current == value.to_string() => Confirmation::Confirmed,
            Ok(current) => Confirmation::NotYet {
                reason: format!("{} reads {current}, not {value}", property.as_str()),
            },
            Err(reason) => Confirmation::Refused { reason },
        }
    }

    fn observe(&self, target: &WorkloadId) -> MemoryObservation {
        let Some(control) = self.targets.get(&target.scope) else {
            return MemoryObservation::default();
        };
        let relative = control.control_group.trim_start_matches('/');
        let path = Path::new(CGROUP_V2_ROOT)
            .join(relative)
            .join("memory.current");
        let host_ram_bytes = self
            .fs
            .read_to_string(&path)
            .ok()
            .and_then(|text| text.trim().parse::<u64>().ok());
        MemoryObservation {
            host_ram_bytes,
            device_ram_bytes: None,
        }
    }

    fn read_value(&self, key: &str) -> Result<Option<String>, ExecutorError> {
        let (unit, property) = parse_setting_key(key).map_err(ExecutorError::Failed)?;
        let command = ManagerCommand::Show {
            unit: unit.clone(),
            property,
        };
        match self.runner.run(&command) {
            Ok(output) if output.is_ok() => Ok(Some(output.value().to_string())),
            // An unknown unit means the target is gone, not that the read failed.
            Ok(_) => Ok(None),
            Err(error) => Err(ExecutorError::Disconnected(error.to_string())),
        }
    }

    fn write_value(&self, key: &str, value: &str) -> Result<(), ExecutorError> {
        let (unit, property) = parse_setting_key(key).map_err(ExecutorError::Failed)?;
        let value = value.trim().to_string();
        let command = match (property, parse_bytes(&value)) {
            (UnitProperty::MemoryHigh, Some(bytes)) => {
                ManagerCommand::SetMemoryHigh { unit, bytes }
            }
            (UnitProperty::MemoryLow, Some(0)) => ManagerCommand::ClearMemoryLow { unit },
            (UnitProperty::MemoryLow, Some(bytes)) => ManagerCommand::SetMemoryLow { unit, bytes },
            (UnitProperty::MemoryHigh, None) => ManagerCommand::ClearMemoryHigh { unit },
            _ => {
                return Err(ExecutorError::Failed(format!(
                    "cannot restore {key} to {value:?}"
                )))
            }
        };
        self.issue(command).map(|_| ())
    }
}

/// Parse a journal setting key back into a unit and property.
fn parse_setting_key(key: &str) -> Result<(UnitName, UnitProperty), String> {
    let rest = key
        .strip_prefix("systemd:")
        .ok_or_else(|| format!("{key} is not a systemd setting key"))?;
    let (unit, property) = rest
        .rsplit_once(':')
        .ok_or_else(|| format!("{key} is not a systemd setting key"))?;
    let unit = UnitName::parse(unit).map_err(|error| error.to_string())?;
    let property = match property {
        "MemoryHigh" => UnitProperty::MemoryHigh,
        "MemoryLow" => UnitProperty::MemoryLow,
        "ControlGroup" => UnitProperty::ControlGroup,
        "ActiveState" => UnitProperty::ActiveState,
        other => return Err(format!("{other} is not a readable setting")),
    };
    Ok((unit, property))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::manager::CommandRunner;

    struct UnusedRunner;

    impl CommandRunner for UnusedRunner {
        fn run(&self, _command: &ManagerCommand) -> Result<CommandOutput, ControlError> {
            Err(ControlError::Runner("not used".to_string()))
        }
    }

    #[test]
    fn a_setting_key_round_trips() {
        let unit = UnitName::parse("app.service").expect("valid");
        for property in [UnitProperty::MemoryHigh, UnitProperty::MemoryLow] {
            let key = setting_key(&unit, property);
            let (parsed, parsed_property) = parse_setting_key(&key).expect("round trip");
            assert_eq!(parsed, unit);
            assert_eq!(parsed_property, property);
        }
    }

    #[test]
    fn a_foreign_key_is_rejected() {
        assert!(parse_setting_key("cgroup:app/memory.high").is_err());
        assert!(parse_setting_key("systemd:app.service:Bogus").is_err());
        assert!(parse_setting_key("systemd:not-a-unit:MemoryHigh").is_err());
    }

    #[test]
    fn only_memory_controls_are_supported() {
        let fs = crate::telemetry::fs::test_doubles::MemoryFs::new();
        let runner = UnusedRunner;
        let targets = ControlTargets::new();
        let executor = ControlExecutor::new(&fs, &runner, &targets);
        assert!(executor.supports(ActionKind::CgroupSoftLimit));
        assert!(executor.supports(ActionKind::ReclaimProtection));
        assert!(!executor.supports(ActionKind::SleepModel));
        assert!(!executor.supports(ActionKind::UnloadIdleModel));
    }

    #[test]
    fn an_unknown_scope_is_unsupported_and_explains_itself() {
        let fs = crate::telemetry::fs::test_doubles::MemoryFs::new();
        let runner = UnusedRunner;
        let targets = ControlTargets::new();
        let executor = ControlExecutor::new(&fs, &runner, &targets);
        let request = ActionRequest::new(
            WorkloadId {
                scope: "unit:app.service".to_string(),
                generation: 1,
            },
            ActionKind::CgroupSoftLimit,
            "over budget",
            1_000,
        )
        .with_limit_bytes(1024 * 1024);
        match executor.precondition(&request) {
            Precondition::Unsupported { reason } => {
                assert!(reason.contains("user unit"), "reason was {reason}");
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn a_missing_value_is_refused_rather_than_guessed() {
        let fs = crate::telemetry::fs::test_doubles::MemoryFs::new();
        let runner = UnusedRunner;
        let targets = ControlTargets::new();
        let executor = ControlExecutor::new(&fs, &runner, &targets);
        let request = ActionRequest::new(
            WorkloadId {
                scope: "unit:app.service".to_string(),
                generation: 1,
            },
            ActionKind::CgroupSoftLimit,
            "over budget",
            1_000,
        );
        // Nothing to apply, and no target to apply it to: the refusal is honest.
        assert!(executor.apply(&request).is_err());
    }
}
