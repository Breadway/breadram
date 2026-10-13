//! Cooperative control over supervised jobs.
//!
//! A `ReduceJobConcurrency` plan becomes a smaller number in a directive file
//! the job already reads, never a freeze and never a claim about memory the
//! job holds. The executor halves toward one, refuses inside an open pause,
//! and confirms only while a reduce instruction is actually on disk. A pause
//! that expired is not this action's to clear: the daemon's resume pass and
//! `finish_job` own the rest of the lifecycle, so directives are not journal
//! values and `read_value`/`write_value` say so.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::concurrency::{
    read_directive, request_pause, request_reduce, unix_ms, PauseOutcome, ReduceOutcome,
    WorkerDirective, MAX_PAUSE,
};
use crate::config::ActionKind;
use crate::coordinator::{
    ActionRequest, AppliedChange, Confirmation, Executor, ExecutorError, MemoryObservation,
    Precondition,
};
use crate::inventory::WorkloadId;

/// The scope unit prefix for supervised jobs, so a workload scope maps back
/// to the job id the daemon admitted. Ids are restricted to unit-safe bytes
/// at submit time, so the mapping is reversible without sanitizing.
pub const JOB_SCOPE_PREFIX: &str = "breadram-job-";

/// The workload scope for a supervised job id.
pub fn scope_for_job(job_id: &str) -> String {
    format!("unit:{JOB_SCOPE_PREFIX}{job_id}.scope")
}

/// The job id for a workload scope, or `None` when the scope is not a
/// supervised job.
pub fn job_id_for_scope(scope: &str) -> Option<String> {
    let unit = scope.strip_prefix("unit:")?;
    let id = unit
        .strip_prefix(JOB_SCOPE_PREFIX)?
        .strip_suffix(".scope")?;
    if id.is_empty() {
        return None;
    }
    Some(id.to_string())
}

/// The running jobs one pass may reduce, keyed by workload scope.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JobTargets {
    targets: BTreeMap<String, String>,
}

impl JobTargets {
    /// An empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// The targets for the given running job ids.
    pub fn for_running(ids: impl IntoIterator<Item = String>) -> Self {
        let mut targets = Self::new();
        for id in ids {
            targets.insert(scope_for_job(&id), id);
        }
        targets
    }

    /// Add or replace a target.
    pub fn insert(&mut self, scope: String, job_id: String) {
        self.targets.insert(scope, job_id);
    }

    /// The job id for a scope.
    pub fn get(&self, scope: &str) -> Option<&str> {
        self.targets.get(scope).map(String::as_str)
    }

    /// How many targets there are.
    pub fn len(&self) -> usize {
        self.targets.len()
    }

    /// Every targeted scope.
    pub fn scopes(&self) -> impl Iterator<Item = &String> {
        self.targets.keys()
    }

    /// Whether there are none.
    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }
}

/// Writes reduce instructions to the directive files of running jobs.
pub struct JobExecutor {
    dir: PathBuf,
    targets: JobTargets,
}

impl JobExecutor {
    /// Build an executor over the directive directory and the jobs running
    /// on this pass.
    pub fn new(dir: PathBuf, targets: JobTargets) -> Self {
        Self { dir, targets }
    }
}

impl Executor for JobExecutor {
    fn supports(&self, kind: ActionKind) -> bool {
        matches!(kind, ActionKind::ReduceJobConcurrency)
    }

    fn precondition(&self, request: &ActionRequest) -> Precondition {
        if !self.supports(request.kind) {
            return Precondition::Unsupported {
                reason: format!("{:?} is not a job concurrency control", request.kind),
            };
        }
        let Some(job_id) = self.targets.get(&request.target.scope) else {
            return Precondition::Unsupported {
                reason: format!("{} is not a running supervised job", request.target.scope),
            };
        };
        match read_directive(&self.dir, job_id) {
            Some(WorkerDirective::Pause { resume_unix_ms, .. }) if unix_ms() < resume_unix_ms => {
                Precondition::Busy {
                    reason: format!("a pause until {resume_unix_ms} still holds"),
                }
            }
            _ => Precondition::Ready,
        }
    }

    fn apply(&self, request: &ActionRequest) -> Result<AppliedChange, ExecutorError> {
        let Some(job_id) = self.targets.get(&request.target.scope) else {
            return Err(ExecutorError::Denied(format!(
                "{} is not a running supervised job",
                request.target.scope
            )));
        };
        let now = unix_ms();
        // The reduce ladder bottoms out at one worker. A job already told to
        // run one, and still over budget, is paused instead: bounded,
        // self-resuming, and reported as the escalation it is.
        if matches!(
            read_directive(&self.dir, job_id),
            Some(WorkerDirective::Reduce { workers: 1 })
        ) {
            match request_pause(
                &self.dir,
                job_id,
                1,
                now.saturating_add(MAX_PAUSE.as_millis() as u64),
                now,
            )
            .map_err(|error| {
                ExecutorError::Failed(format!("the directive could not be written: {error}"))
            })? {
                PauseOutcome::Paused { resume_unix_ms, .. } => {
                    return Ok(AppliedChange {
                        values: Vec::new(),
                        note: Some(format!(
                            "{job_id} is already at one worker, pausing until {resume_unix_ms} instead"
                        )),
                    });
                }
                PauseOutcome::RefusedBusy { resume_unix_ms } => {
                    return Err(ExecutorError::Denied(format!(
                        "a pause until {resume_unix_ms} still holds for {job_id}: retry after the deadline"
                    )));
                }
            }
        }
        match request_reduce(&self.dir, job_id, now).map_err(|error| {
            ExecutorError::Failed(format!("the directive could not be written: {error}"))
        })? {
            ReduceOutcome::Reduced { workers } => Ok(AppliedChange {
                values: Vec::new(),
                note: Some(format!(
                    "asked {job_id} to drop to {workers} worker(s) at its next safe point"
                )),
            }),
            ReduceOutcome::RefusedBusy { resume_unix_ms } => Err(ExecutorError::Denied(format!(
                "a pause until {resume_unix_ms} still holds for {job_id}: retry after the deadline"
            ))),
        }
    }

    fn confirm(&self, request: &ActionRequest) -> Confirmation {
        let Ok(job_id) = self
            .targets
            .get(&request.target.scope)
            .ok_or(())
            .map(str::to_string)
        else {
            return Confirmation::Refused {
                reason: format!("{} is not a running supervised job", request.target.scope),
            };
        };
        // A reduce on disk, or an open pause, is the throttling this action
        // effects. A pause the executor itself just wrote at the floor reads
        // the same as one it did not, and either way the job is stopped.
        match read_directive(&self.dir, &job_id) {
            Some(WorkerDirective::Reduce { .. }) => Confirmation::Confirmed,
            Some(WorkerDirective::Pause { resume_unix_ms, .. }) if unix_ms() < resume_unix_ms => {
                Confirmation::Confirmed
            }
            _ => Confirmation::NotYet {
                reason: format!("no reduce instruction is on disk for {job_id}"),
            },
        }
    }

    fn observe(&self, _target: &WorkloadId) -> MemoryObservation {
        // Job memory is the scope's cgroup figure, which the inventory already
        // reports. The directive file carries no measurement, so nothing here
        // is invented for the effect journal.
        MemoryObservation::default()
    }

    fn read_value(&self, key: &str) -> Result<Option<String>, ExecutorError> {
        let _ = key;
        Err(ExecutorError::Unsupported(
            "job directives are not journal values: resume and finish own their lifecycle"
                .to_string(),
        ))
    }

    fn write_value(&self, key: &str, _value: &str) -> Result<(), ExecutorError> {
        let _ = key;
        Err(ExecutorError::Unsupported(
            "job directives are not journal values: resume and finish own their lifecycle"
                .to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::concurrency::write_directive;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn scratch() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "breadram-jobexec-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn request(scope: &str) -> ActionRequest {
        ActionRequest::new(
            WorkloadId {
                scope: scope.to_string(),
                generation: 7,
            },
            ActionKind::ReduceJobConcurrency,
            "the job can continue at lower concurrency",
            1_000,
        )
    }

    #[test]
    fn scopes_map_to_job_ids_and_back() {
        assert_eq!(scope_for_job("nightly"), "unit:breadram-job-nightly.scope");
        assert_eq!(
            job_id_for_scope("unit:breadram-job-nightly.scope"),
            Some("nightly".to_string())
        );
        assert_eq!(job_id_for_scope("unit:app.service"), None);
        assert_eq!(job_id_for_scope("exe:python3"), None);
        assert_eq!(job_id_for_scope("unit:breadram-job-.scope"), None);
    }

    #[test]
    fn an_unknown_scope_is_not_a_job() {
        let dir = scratch();
        let executor = JobExecutor::new(dir.clone(), JobTargets::new());
        assert!(!executor.supports(ActionKind::CgroupSoftLimit));
        assert!(executor.supports(ActionKind::ReduceJobConcurrency));
        match executor.precondition(&request("unit:app.service")) {
            Precondition::Unsupported { reason } => {
                assert!(reason.contains("not a running supervised job"), "{reason}");
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
        assert!(executor.apply(&request("unit:app.service")).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_reduce_halves_and_confirms_while_it_is_on_disk() {
        let dir = scratch();
        let scope = scope_for_job("nightly");
        let executor = JobExecutor::new(
            dir.clone(),
            JobTargets::for_running(["nightly".to_string()]),
        );
        write_directive(&dir, "nightly", WorkerDirective::Run { workers: 4 }).unwrap();
        assert_eq!(executor.precondition(&request(&scope)), Precondition::Ready);
        let change = executor.apply(&request(&scope)).expect("a reduce applies");
        assert_eq!(
            change.note.as_deref(),
            Some("asked nightly to drop to 2 worker(s) at its next safe point")
        );
        assert!(change.values.is_empty());
        assert_eq!(executor.confirm(&request(&scope)), Confirmation::Confirmed);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn at_the_floor_a_reduce_becomes_a_bounded_pause() {
        let dir = scratch();
        let scope = scope_for_job("nightly");
        let executor = JobExecutor::new(
            dir.clone(),
            JobTargets::for_running(["nightly".to_string()]),
        );
        write_directive(&dir, "nightly", WorkerDirective::Reduce { workers: 1 }).unwrap();
        let before = unix_ms();
        let change = executor
            .apply(&request(&scope))
            .expect("the floor escalates");
        let note = change.note.expect("an escalation is reported");
        assert!(
            note.contains("already at one worker, pausing until"),
            "{note}"
        );
        match read_directive(&dir, "nightly") {
            Some(WorkerDirective::Pause {
                workers,
                resume_unix_ms,
            }) => {
                assert_eq!(workers, 1);
                assert!(
                    resume_unix_ms <= before + 30_000,
                    "a pause never exceeds the cap"
                );
            }
            other => panic!("expected a bounded pause, got {other:?}"),
        }
        assert_eq!(executor.confirm(&request(&scope)), Confirmation::Confirmed);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_pause_blocks_a_reduce_and_a_run_is_not_a_confirmation() {
        let dir = scratch();
        let scope = scope_for_job("nightly");
        let executor = JobExecutor::new(
            dir.clone(),
            JobTargets::for_running(["nightly".to_string()]),
        );
        match executor.precondition(&request("unit:app.service")) {
            Precondition::Unsupported { .. } => {}
            other => panic!("expected Unsupported, got {other:?}"),
        }
        write_directive(
            &dir,
            "nightly",
            WorkerDirective::Pause {
                workers: 1,
                resume_unix_ms: unix_ms() + 60_000,
            },
        )
        .unwrap();
        match executor.precondition(&request(&scope)) {
            Precondition::Busy { reason } => assert!(reason.contains("still holds"), "{reason}"),
            other => panic!("expected Busy, got {other:?}"),
        }
        assert!(executor.apply(&request(&scope)).is_err());
        write_directive(&dir, "nightly", WorkerDirective::Run { workers: 2 }).unwrap();
        assert_eq!(executor.precondition(&request(&scope)), Precondition::Ready);
        match executor.confirm(&request(&scope)) {
            Confirmation::NotYet { .. } => {}
            other => panic!("a run file is not a confirmation, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
