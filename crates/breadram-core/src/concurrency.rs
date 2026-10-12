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
        let mut parts = text.trim().split_whitespace();
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
}
