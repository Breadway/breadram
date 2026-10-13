//! Cooperative concurrency for an opted-in job.
//!
//! breadram cannot change how many workers an arbitrary program started with.
//! A job that wants to yield under pressure reads a small file breadram writes
//! and drops its own extra workers, or pauses, at a safe point. Nothing here
//! freezes a process, and nothing here claims that pausing releases memory the
//! process already holds.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// How long a pause may last before the job must resume on its own.
///
/// A daemon that dies mid-pause must not leave the job sitting forever. The
/// deadline is written into the file, so the job can honor it without the
/// daemon still being alive.
pub const MAX_PAUSE: Duration = Duration::from_secs(30);

/// What an opted-in job should do with its own worker count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerDirective {
    /// Keep `workers` running.
    Run { workers: u32 },
    /// Drop to `workers` at the next safe point. Not a freeze.
    Reduce { workers: u32 },
    /// Stop taking new work until `resume_unix_ms`, then run `workers` again.
    ///
    /// The deadline is absolute so a dead daemon cannot extend it. Holding
    /// already-allocated memory is the job's own choice: this is not a request
    /// to release it.
    Pause { workers: u32, resume_unix_ms: u64 },
}

impl WorkerDirective {
    /// The worker count the directive names for when the job is running.
    pub fn workers(self) -> u32 {
        match self {
            WorkerDirective::Run { workers }
            | WorkerDirective::Reduce { workers }
            | WorkerDirective::Pause { workers, .. } => workers,
        }
    }

    fn encode(self) -> String {
        match self {
            WorkerDirective::Run { workers } => format!("run {workers}\n"),
            WorkerDirective::Reduce { workers } => format!("reduce {workers}\n"),
            WorkerDirective::Pause {
                workers,
                resume_unix_ms,
            } => format!("pause {workers} {resume_unix_ms}\n"),
        }
    }

    fn parse(text: &str) -> Option<Self> {
        let mut parts = text.split_whitespace();
        let verb = parts.next()?;
        let workers = parts.next()?.parse::<u32>().ok()?;
        if workers == 0 {
            return None;
        }
        match verb {
            "run" if parts.next().is_none() => Some(WorkerDirective::Run { workers }),
            "reduce" if parts.next().is_none() => Some(WorkerDirective::Reduce { workers }),
            "pause" => {
                let resume_unix_ms = parts.next()?.parse::<u64>().ok()?;
                if parts.next().is_some() {
                    return None;
                }
                Some(WorkerDirective::Pause {
                    workers,
                    resume_unix_ms,
                })
            }
            _ => None,
        }
    }
}

/// A pause the job should still be honoring at `now_unix_ms`.
///
/// Past the deadline the pause is over even if the file still says pause:
/// the daemon may have died and must not be required to clear it.
pub fn pause_active(directive: WorkerDirective, now_unix_ms: u64) -> bool {
    match directive {
        WorkerDirective::Pause { resume_unix_ms, .. } => now_unix_ms < resume_unix_ms,
        _ => false,
    }
}

/// Where one job reads its directive from.
///
/// The path is derived from the job id, which is already restricted to ASCII
/// letters, digits, hyphen, and underscore, so it cannot escape the directory.
pub fn directive_path(dir: &Path, job_id: &str) -> PathBuf {
    dir.join(format!("{job_id}.workers"))
}

/// Write a directive so a job can read it. Replaces the previous one.
pub fn write_directive(dir: &Path, job_id: &str, directive: WorkerDirective) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let path = directive_path(dir, job_id);
    let tmp = dir.join(format!(
        ".{job_id}.{}.tmp",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    fs::write(&tmp, directive.encode())?;
    fs::rename(&tmp, &path)
}

/// Read the directive, or `None` when the file is missing or not understood.
///
/// A missing file means the job keeps its own count. It is not an error and
/// it is not a request to stop or to stay paused.
pub fn read_directive(dir: &Path, job_id: &str) -> Option<WorkerDirective> {
    let text = fs::read_to_string(directive_path(dir, job_id)).ok()?;
    WorkerDirective::parse(&text)
}

/// Remove a directive. A finished job must not leave one behind for a reused id.
pub fn clear_directive(dir: &Path, job_id: &str) -> io::Result<()> {
    match fs::remove_file(directive_path(dir, job_id)) {
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

/// What a reduce request did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReduceOutcome {
    /// A smaller worker count was written.
    Reduced { workers: u32 },
    /// The job is inside an open pause deadline, so a reduce is meaningless:
    /// its workers are already joined. Retry after the deadline.
    RefusedBusy { resume_unix_ms: u64 },
}

/// Ask an opted-in job to halve its workers, down to a floor of one.
///
/// The halving is read from the current directive, so a repeated reduce keeps
/// shrinking toward one rather than re-asking for the same count. A missing or
/// garbled file becomes `reduce 1`: the minimum useful instruction, not a
/// guess about the job's own count. A reduce inside an open pause is refused
/// rather than queued behind it.
pub fn request_reduce(dir: &Path, job_id: &str, now_unix_ms: u64) -> io::Result<ReduceOutcome> {
    if let Some(WorkerDirective::Pause { resume_unix_ms, .. }) = read_directive(dir, job_id) {
        if now_unix_ms < resume_unix_ms {
            return Ok(ReduceOutcome::RefusedBusy { resume_unix_ms });
        }
    }
    // Halve toward a floor of one: 4 becomes 2, 2 becomes 1, and 1 stays 1
    // rather than dropping to an unusable zero. A repeated reduce keeps
    // shrinking until it reaches the floor.
    let workers = match read_directive(dir, job_id) {
        Some(directive) => (directive.workers() / 2).max(1),
        None => 1,
    };
    write_directive(dir, job_id, WorkerDirective::Reduce { workers })?;
    Ok(ReduceOutcome::Reduced { workers })
}

/// What a pause request did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PauseOutcome {
    /// The pause was written, clamped to the cap when asked for more.
    Paused {
        workers: u32,
        resume_unix_ms: u64,
        clamped: bool,
    },
    /// A pause whose deadline has not passed is still on disk. It is refused
    /// rather than replaced: the first deadline stands.
    RefusedBusy { resume_unix_ms: u64 },
}

/// Ask an opted-in job to stop taking new work until `resume_unix_ms`.
///
/// A second pause inside an open deadline is refused. A deadline past
/// `now_unix_ms + MAX_PAUSE` is clamped to the cap and reported: a pause the
/// job cannot honor alone must never be written.
pub fn request_pause(
    dir: &Path,
    job_id: &str,
    workers: u32,
    resume_unix_ms: u64,
    now_unix_ms: u64,
) -> io::Result<PauseOutcome> {
    if let Some(WorkerDirective::Pause { resume_unix_ms, .. }) = read_directive(dir, job_id) {
        if now_unix_ms < resume_unix_ms {
            return Ok(PauseOutcome::RefusedBusy { resume_unix_ms });
        }
    }
    let cap = now_unix_ms.saturating_add(MAX_PAUSE.as_millis() as u64);
    let clamped = resume_unix_ms > cap;
    let resume_unix_ms = resume_unix_ms.min(cap);
    let workers = workers.max(1);
    write_directive(
        dir,
        job_id,
        WorkerDirective::Pause {
            workers,
            resume_unix_ms,
        },
    )?;
    Ok(PauseOutcome::Paused {
        workers,
        resume_unix_ms,
        clamped,
    })
}

/// Write `run` over an expired pause, returning what was resumed.
///
/// Returns `None` when there is nothing to resume: no file, a live directive,
/// or an open pause. The resume count comes from the pause itself, which
/// names the workers to run again. This is what ends a pause after recovery:
/// the job would resume on its own by the deadline, and this clears the file
/// so the next instruction starts clean.
pub fn resume_if_expired(
    dir: &Path,
    job_id: &str,
    now_unix_ms: u64,
) -> io::Result<Option<WorkerDirective>> {
    let Some(WorkerDirective::Pause {
        workers,
        resume_unix_ms,
    }) = read_directive(dir, job_id)
    else {
        return Ok(None);
    };
    if now_unix_ms < resume_unix_ms {
        return Ok(None);
    }
    let resumed = WorkerDirective::Run { workers };
    write_directive(dir, job_id, resumed)?;
    Ok(Some(resumed))
}

/// Unix time in milliseconds, for pause deadlines.
pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "breadram-conc-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn a_job_reads_the_directive_it_was_given() {
        let dir = scratch();
        assert!(read_directive(&dir, "job-1").is_none());
        write_directive(&dir, "job-1", WorkerDirective::Reduce { workers: 1 }).unwrap();
        assert_eq!(
            read_directive(&dir, "job-1"),
            Some(WorkerDirective::Reduce { workers: 1 })
        );
        write_directive(&dir, "job-1", WorkerDirective::Run { workers: 4 }).unwrap();
        assert_eq!(
            read_directive(&dir, "job-1"),
            Some(WorkerDirective::Run { workers: 4 })
        );
        clear_directive(&dir, "job-1").unwrap();
        assert!(read_directive(&dir, "job-1").is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_garbled_file_is_not_a_directive() {
        let dir = scratch();
        fs::write(directive_path(&dir, "job-2"), "freeze 0\n").unwrap();
        assert!(read_directive(&dir, "job-2").is_none());
        fs::write(directive_path(&dir, "job-2"), "pause 1\n").unwrap();
        assert!(read_directive(&dir, "job-2").is_none());
        fs::write(directive_path(&dir, "job-2"), "run 0\n").unwrap();
        assert!(read_directive(&dir, "job-2").is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_pause_ends_at_its_deadline_without_the_daemon() {
        let dir = scratch();
        let now = 1_000_000_u64;
        write_directive(
            &dir,
            "job-3",
            WorkerDirective::Pause {
                workers: 1,
                resume_unix_ms: now + 1_000,
            },
        )
        .unwrap();
        let directive = read_directive(&dir, "job-3").unwrap();
        assert!(pause_active(directive, now));
        assert!(pause_active(directive, now + 999));
        assert!(!pause_active(directive, now + 1_000));
        // The file can still say pause. The deadline, not a live daemon, ends it.
        assert!(matches!(
            read_directive(&dir, "job-3"),
            Some(WorkerDirective::Pause { .. })
        ));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_pause_longer_than_the_cap_is_the_callers_mistake_to_avoid() {
        assert_eq!(MAX_PAUSE, Duration::from_secs(30));
    }

    #[test]
    fn a_reduce_halves_toward_one_and_refuses_inside_a_pause() {
        let dir = scratch();
        // No file yet: the minimum useful instruction.
        assert_eq!(
            request_reduce(&dir, "job-4", 1_000).unwrap(),
            ReduceOutcome::Reduced { workers: 1 }
        );
        write_directive(&dir, "job-4", WorkerDirective::Run { workers: 4 }).unwrap();
        assert_eq!(
            request_reduce(&dir, "job-4", 1_000).unwrap(),
            ReduceOutcome::Reduced { workers: 2 }
        );
        assert_eq!(
            request_reduce(&dir, "job-4", 1_000).unwrap(),
            ReduceOutcome::Reduced { workers: 1 }
        );
        assert_eq!(
            request_reduce(&dir, "job-4", 1_000).unwrap(),
            ReduceOutcome::Reduced { workers: 1 },
            "one is the floor, never zero"
        );
        // An open pause refuses the reduce.
        write_directive(
            &dir,
            "job-4",
            WorkerDirective::Pause {
                workers: 1,
                resume_unix_ms: 2_000,
            },
        )
        .unwrap();
        assert_eq!(
            request_reduce(&dir, "job-4", 1_000).unwrap(),
            ReduceOutcome::RefusedBusy {
                resume_unix_ms: 2_000
            }
        );
        // Past the deadline the pause no longer blocks.
        assert_eq!(
            request_reduce(&dir, "job-4", 2_000).unwrap(),
            ReduceOutcome::Reduced { workers: 1 }
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_pause_is_clamped_and_a_second_pause_is_refused() {
        let dir = scratch();
        let out = request_pause(&dir, "job-5", 2, 1_000 + 60_000, 1_000).unwrap();
        assert_eq!(
            out,
            PauseOutcome::Paused {
                workers: 2,
                resume_unix_ms: 1_000 + 30_000,
                clamped: true,
            }
        );
        assert_eq!(
            request_pause(&dir, "job-5", 2, 1_000 + 60_000, 1_000).unwrap(),
            PauseOutcome::RefusedBusy {
                resume_unix_ms: 1_000 + 30_000,
            },
            "the first deadline stands"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn resume_clears_only_an_expired_pause() {
        let dir = scratch();
        assert_eq!(resume_if_expired(&dir, "job-6", 1_000).unwrap(), None);
        write_directive(&dir, "job-6", WorkerDirective::Run { workers: 3 }).unwrap();
        assert_eq!(resume_if_expired(&dir, "job-6", 1_000).unwrap(), None);
        write_directive(
            &dir,
            "job-6",
            WorkerDirective::Pause {
                workers: 2,
                resume_unix_ms: 2_000,
            },
        )
        .unwrap();
        assert_eq!(resume_if_expired(&dir, "job-6", 1_000).unwrap(), None);
        assert_eq!(
            resume_if_expired(&dir, "job-6", 2_000).unwrap(),
            Some(WorkerDirective::Run { workers: 2 })
        );
        assert_eq!(
            read_directive(&dir, "job-6"),
            Some(WorkerDirective::Run { workers: 2 })
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
