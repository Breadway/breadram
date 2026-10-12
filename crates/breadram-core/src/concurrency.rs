//! Cooperative concurrency for an opted-in job.
//!
//! breadram cannot change how many workers an arbitrary program started with.
//! A job that wants to yield under pressure reads a small file breadram writes
//! and drops its own extra workers at a safe point. Nothing here freezes a
//! process, and nothing here claims that pausing releases memory the process
//! already holds.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// What an opted-in job should do with its own worker count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerDirective {
    /// Keep the worker count the job was started with.
    Run { workers: u32 },
    /// Drop to `workers` at the next safe point. Not a freeze.
    Reduce { workers: u32 },
}

impl WorkerDirective {
    /// The worker count the directive names.
    pub fn workers(self) -> u32 {
        match self {
            WorkerDirective::Run { workers } | WorkerDirective::Reduce { workers } => workers,
        }
    }

    fn verb(self) -> &'static str {
        match self {
            WorkerDirective::Run { .. } => "run",
            WorkerDirective::Reduce { .. } => "reduce",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        let (verb, count) = text.trim().split_once(' ')?;
        let workers = count.parse::<u32>().ok()?;
        if workers == 0 {
            return None;
        }
        match verb {
            "run" => Some(WorkerDirective::Run { workers }),
            "reduce" => Some(WorkerDirective::Reduce { workers }),
            _ => None,
        }
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
    let body = format!("{} {}\n", directive.verb(), directive.workers());
    let tmp = dir.join(format!(
        ".{job_id}.{}.tmp",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    fs::write(&tmp, body)?;
    fs::rename(&tmp, &path)
}

/// Read the directive, or `None` when the file is missing or not understood.
///
/// A missing file means the job keeps its own count. It is not an error and
/// it is not a request to stop.
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
        let _ = fs::remove_dir_all(&dir);
    }
}
