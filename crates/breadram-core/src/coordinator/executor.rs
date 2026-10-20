//! The executor boundary.
//!
//! An executor is the only thing that touches the host. The coordinator decides
//! whether an action may run and records what happened; the executor applies the
//! change, confirms it, and reports the memory the target actually holds.
//!
//! Every failure an executor can report has its own variant, so the coordinator
//! can pass the honest answer on instead of collapsing it into "failed": a
//! partial write is not the same as a refusal, and a lost connection is not the
//! same as a timeout.

use crate::config::ActionKind;
use crate::inventory::WorkloadId;

use super::journal::OwnedValue;
use super::ActionRequest;

/// A precondition check, made immediately before execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Precondition {
    /// The change may proceed.
    Ready,
    /// The executor cannot do this, or can no longer do it.
    Unsupported {
        /// Why.
        reason: String,
    },
    /// The target or the executor is busy. Retry later.
    Busy {
        /// Why.
        reason: String,
    },
    /// Policy, configuration, or the system refused.
    Denied {
        /// Why.
        reason: String,
    },
    /// The target no longer exists in the form the action was planned against.
    TargetGone {
        /// Why.
        reason: String,
    },
}

/// Why applying a change failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExecutorError {
    /// Some of the change was written before the failure.
    #[error("the change was applied only partially: {reason}")]
    Partial {
        /// What went wrong.
        reason: String,
        /// The values that were written before the failure, so they can be
        /// journaled and restored.
        values: Vec<OwnedValue>,
    },
    /// The connection to the target was lost.
    #[error("the connection to the target was lost: {0}")]
    Disconnected(String),
    /// The change did not finish inside the deadline.
    #[error("the change timed out: {0}")]
    Timeout(String),
    /// The change was refused.
    #[error("the change was refused: {0}")]
    Denied(String),
    /// The change is not supported by this executor.
    #[error("the change is not supported: {0}")]
    Unsupported(String),
    /// The change failed for another reason.
    #[error("the change failed: {0}")]
    Failed(String),
}

/// What an executor managed to change.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AppliedChange {
    /// Daemon-owned values that were written, each carrying its original.
    pub values: Vec<OwnedValue>,
    /// Anything the caller should know about what happened, such as a value
    /// that was clamped to stay within a bound. Reported, never hidden.
    pub note: Option<String>,
}

/// The result of a confirmation check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Confirmation {
    /// The effect is present.
    Confirmed,
    /// The effect is not observable yet.
    NotYet {
        /// Why not.
        reason: String,
    },
    /// The target refused to confirm.
    Refused {
        /// Why.
        reason: String,
    },
}

/// The memory a target currently holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MemoryObservation {
    /// Host memory in bytes, when it could be read.
    pub host_ram_bytes: Option<u64>,
    /// Device memory in bytes, when it could be read.
    pub device_ram_bytes: Option<u64>,
}

/// Applies and confirms one kind of change.
pub trait Executor: Send + Sync {
    /// Whether this executor can apply the action at all.
    fn supports(&self, kind: ActionKind) -> bool;

    /// A cheap check made immediately before execution.
    fn precondition(&self, request: &ActionRequest) -> Precondition;

    /// Apply the change. A partial application is reported as such.
    fn apply(&self, request: &ActionRequest) -> Result<AppliedChange, ExecutorError>;

    /// Read-only confirmation that the effect is present.
    fn confirm(&self, request: &ActionRequest) -> Confirmation;

    /// Whether fresh, read-only evidence proves a previously confirmed
    /// transient effect no longer exists. Unknown stays false. This is not
    /// permission to apply: all ordinary execution guards must still pass.
    fn confirmed_effect_absent(&self, _request: &ActionRequest) -> bool {
        false
    }

    /// The memory the target holds, read before and after a change.
    fn observe(&self, target: &WorkloadId) -> MemoryObservation;

    /// Read a daemon-owned value. `None` means the target is gone.
    fn read_value(&self, key: &str) -> Result<Option<String>, ExecutorError>;

    /// Write a daemon-owned value.
    fn write_value(&self, key: &str, value: &str) -> Result<(), ExecutorError>;

    /// Undo an action that is not expressible as a value write, for example
    /// waking a backend that was slept.
    fn undo(&self, request: &ActionRequest) -> Result<(), ExecutorError> {
        let _ = request;
        Err(ExecutorError::Unsupported(
            "this executor cannot undo that action".to_string(),
        ))
    }
}

/// Several executors behind one, so a plan of mixed actions runs in one pass.
///
/// Host controls and provider actions do not overlap: each executor claims the
/// actions it can carry out and declines the rest, so an action reaches exactly
/// one of them. The first executor that claims an action is the only one asked
/// to apply, confirm, or undo it, so two implementations can never both write.
///
/// The order is the caller's: a provider action is never sent to a host control,
/// and a host control is never sent to a provider.
pub struct ChainedExecutor<'a> {
    executors: Vec<&'a dyn Executor>,
}

impl<'a> ChainedExecutor<'a> {
    /// Chain the given executors, in priority order.
    pub fn new(executors: Vec<&'a dyn Executor>) -> Self {
        Self { executors }
    }

    /// The executor that claims an action, if any.
    fn for_kind(&self, kind: ActionKind) -> Option<&'a dyn Executor> {
        self.executors
            .iter()
            .copied()
            .find(|executor| executor.supports(kind))
    }
}

impl Executor for ChainedExecutor<'_> {
    fn supports(&self, kind: ActionKind) -> bool {
        self.for_kind(kind).is_some()
    }

    fn confirmed_effect_absent(&self, request: &ActionRequest) -> bool {
        self.for_kind(request.kind)
            .is_some_and(|executor| executor.confirmed_effect_absent(request))
    }

    fn precondition(&self, request: &ActionRequest) -> Precondition {
        match self.for_kind(request.kind) {
            Some(executor) => executor.precondition(request),
            None => Precondition::Unsupported {
                reason: format!("no executor on this host supports {:?}", request.kind),
            },
        }
    }

    fn apply(&self, request: &ActionRequest) -> Result<AppliedChange, ExecutorError> {
        match self.for_kind(request.kind) {
            Some(executor) => executor.apply(request),
            None => Err(ExecutorError::Unsupported(format!(
                "no executor on this host supports {:?}",
                request.kind
            ))),
        }
    }

    fn confirm(&self, request: &ActionRequest) -> Confirmation {
        match self.for_kind(request.kind) {
            Some(executor) => executor.confirm(request),
            None => Confirmation::NotYet {
                reason: format!("no executor on this host supports {:?}", request.kind),
            },
        }
    }

    fn observe(&self, target: &WorkloadId) -> MemoryObservation {
        // The first executor that actually measured something wins, so a
        // provider that can report device memory is not suppressed by a host
        // control that cannot.
        let mut observation = MemoryObservation::default();
        for executor in &self.executors {
            let reading = executor.observe(target);
            if observation.host_ram_bytes.is_none() {
                observation.host_ram_bytes = reading.host_ram_bytes;
            }
            if observation.device_ram_bytes.is_none() {
                observation.device_ram_bytes = reading.device_ram_bytes;
            }
        }
        observation
    }

    fn read_value(&self, key: &str) -> Result<Option<String>, ExecutorError> {
        // The first executor that owns the key answers; the first complaint is
        // kept if none does, so the reason is the most specific one available.
        let mut first_error = None;
        for executor in &self.executors {
            match executor.read_value(key) {
                Ok(value) => return Ok(value),
                Err(error) => first_error.get_or_insert(error),
            };
        }
        Err(first_error
            .unwrap_or_else(|| ExecutorError::Unsupported("no executor is configured".to_string())))
    }

    fn write_value(&self, key: &str, value: &str) -> Result<(), ExecutorError> {
        let mut first_error = None;
        for executor in &self.executors {
            match executor.write_value(key, value) {
                Ok(()) => return Ok(()),
                Err(error) => first_error.get_or_insert(error),
            };
        }
        Err(first_error
            .unwrap_or_else(|| ExecutorError::Unsupported("no executor is configured".to_string())))
    }

    fn undo(&self, request: &ActionRequest) -> Result<(), ExecutorError> {
        match self.for_kind(request.kind) {
            Some(executor) => executor.undo(request),
            None => Err(ExecutorError::Unsupported(format!(
                "no executor on this host supports {:?}",
                request.kind
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ActionKind;
    use crate::coordinator::journal::{OwnedValue, OwnerToken};
    use crate::inventory::WorkloadId;
    use std::sync::Mutex;

    /// An executor that claims one action and records every call it receives, so
    /// a test can prove which executor was asked.
    struct Claiming {
        kind: ActionKind,
        observed: MemoryObservation,
        seen: Mutex<Vec<String>>,
        owned_keys: Vec<String>,
    }

    impl Claiming {
        fn new(kind: ActionKind, observed: MemoryObservation) -> Self {
            Self {
                kind,
                observed,
                seen: Mutex::new(Vec::new()),
                owned_keys: Vec::new(),
            }
        }

        fn owning(self, keys: &[&str]) -> Self {
            Self {
                owned_keys: keys.iter().map(|key| key.to_string()).collect(),
                ..self
            }
        }

        fn seen(&self) -> Vec<String> {
            self.seen.lock().expect("not poisoned").clone()
        }

        fn note(&self, call: &str) {
            self.seen
                .lock()
                .expect("not poisoned")
                .push(call.to_string());
        }
    }

    impl Executor for Claiming {
        fn supports(&self, kind: ActionKind) -> bool {
            kind == self.kind
        }

        fn precondition(&self, _request: &ActionRequest) -> Precondition {
            self.note("precondition");
            Precondition::Ready
        }

        fn apply(&self, _request: &ActionRequest) -> Result<AppliedChange, ExecutorError> {
            self.note("apply");
            Ok(AppliedChange {
                values: vec![OwnedValue {
                    owner: OwnerToken("test-executor".to_string()),
                    key: "claimed".to_string(),
                    original: "before".to_string(),
                    last_applied: "after".to_string(),
                }],
                note: None,
            })
        }

        fn confirm(&self, _request: &ActionRequest) -> Confirmation {
            self.note("confirm");
            Confirmation::Confirmed
        }

        fn observe(&self, _target: &WorkloadId) -> MemoryObservation {
            self.note("observe");
            self.observed
        }

        fn read_value(&self, key: &str) -> Result<Option<String>, ExecutorError> {
            self.note("read_value");
            if self.owned_keys.iter().any(|owned| owned == key) {
                return Ok(Some("value".to_string()));
            }
            Err(ExecutorError::Unsupported(format!("{key} is not mine")))
        }

        fn write_value(&self, key: &str, _value: &str) -> Result<(), ExecutorError> {
            self.note("write_value");
            if self.owned_keys.iter().any(|owned| owned == key) {
                return Ok(());
            }
            Err(ExecutorError::Unsupported(format!("{key} is not mine")))
        }

        fn undo(&self, _request: &ActionRequest) -> Result<(), ExecutorError> {
            self.note("undo");
            Ok(())
        }
    }

    fn request(kind: ActionKind) -> ActionRequest {
        ActionRequest::new(
            WorkloadId {
                scope: "unit:app.service".to_string(),
                generation: 1,
            },
            kind,
            "testing the chain",
            0,
        )
    }

    fn target() -> WorkloadId {
        WorkloadId {
            scope: "unit:app.service".to_string(),
            generation: 1,
        }
    }

    #[test]
    fn an_action_reaches_exactly_one_executor() {
        let control = Claiming::new(ActionKind::CgroupSoftLimit, MemoryObservation::default());
        let provider = Claiming::new(
            ActionKind::UnloadIdleModel,
            MemoryObservation {
                host_ram_bytes: Some(1024),
                device_ram_bytes: None,
            },
        );
        let chain = ChainedExecutor::new(vec![&control, &provider]);

        assert!(chain.supports(ActionKind::CgroupSoftLimit));
        assert!(chain.supports(ActionKind::UnloadIdleModel));
        assert!(
            !chain.supports(ActionKind::ReduceJobConcurrency),
            "an action nothing implements is not claimed by the chain"
        );

        assert_eq!(
            chain.precondition(&request(ActionKind::CgroupSoftLimit)),
            Precondition::Ready
        );
        assert_eq!(control.seen(), vec!["precondition"]);
        assert!(
            provider.seen().is_empty(),
            "a host control is never sent to a provider"
        );

        chain
            .apply(&request(ActionKind::UnloadIdleModel))
            .expect("the provider applies it");
        assert_eq!(provider.seen(), vec!["apply"]);
        assert_eq!(
            control.seen(),
            vec!["precondition"],
            "a provider action is never sent to a host control"
        );
    }

    #[test]
    fn an_action_nothing_claims_is_refused_by_every_route() {
        let control = Claiming::new(ActionKind::CgroupSoftLimit, MemoryObservation::default());
        let chain = ChainedExecutor::new(vec![&control]);
        let request = request(ActionKind::QueueBackgroundJob);

        assert!(!chain.supports(ActionKind::QueueBackgroundJob));
        assert!(matches!(
            chain.precondition(&request),
            Precondition::Unsupported { .. }
        ));
        assert!(matches!(
            chain.apply(&request),
            Err(ExecutorError::Unsupported(_))
        ));
        assert!(matches!(
            chain.confirm(&request),
            Confirmation::NotYet { .. }
        ));
        assert!(matches!(
            chain.undo(&request),
            Err(ExecutorError::Unsupported(_))
        ));
        assert!(control.seen().is_empty(), "nothing reached the control");
    }

    #[test]
    fn the_first_measurement_that_exists_wins() {
        // A host control cannot see device memory and a provider cannot read a
        // cgroup, so a chain that only took the first reading would lose one of
        // them.
        let control = Claiming::new(
            ActionKind::CgroupSoftLimit,
            MemoryObservation {
                host_ram_bytes: Some(4096),
                device_ram_bytes: None,
            },
        );
        let provider = Claiming::new(
            ActionKind::UnloadIdleModel,
            MemoryObservation {
                host_ram_bytes: Some(1024),
                device_ram_bytes: Some(2048),
            },
        );
        let chain = ChainedExecutor::new(vec![&control, &provider]);
        assert_eq!(
            chain.observe(&target()),
            MemoryObservation {
                host_ram_bytes: Some(4096),
                device_ram_bytes: Some(2048),
            }
        );
    }

    #[test]
    fn a_value_is_read_and_written_by_the_executor_that_owns_it() {
        let control = Claiming::new(ActionKind::CgroupSoftLimit, MemoryObservation::default())
            .owning(&["cgroup.limit"]);
        let provider = Claiming::new(ActionKind::UnloadIdleModel, MemoryObservation::default())
            .owning(&["provider.keep_alive"]);
        let chain = ChainedExecutor::new(vec![&control, &provider]);

        assert_eq!(
            chain.read_value("provider.keep_alive").expect("readable"),
            Some("value".to_string())
        );
        assert!(chain.write_value("cgroup.limit", "1").is_ok());

        // Neither owner knows the key, so the first executor's complaint is kept
        // rather than the last one's.
        match chain.read_value("nobody.owns.this") {
            Err(ExecutorError::Unsupported(reason)) => {
                assert!(
                    reason.contains("cgroup.limit") || reason.contains("not mine"),
                    "{reason}"
                )
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }
}
