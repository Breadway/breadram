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
