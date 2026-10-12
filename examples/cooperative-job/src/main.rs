//! One real opted-in workload for RAM-25.
//!
//! Reads `$BREADRAM_WORKERS_FILE` (a breadram directive file) and:
//! - `run N` / `reduce N`: keeps N workers, each hashing a private 8 MiB buffer
//! - `pause N <deadline-ms>`: stops taking new work until that unix deadline,
//!   then runs N again even if the file still says pause
//! - missing or garbled file: one worker. Never a stop, never a stuck pause.
//!
//! A pause joins the workers, which frees their buffers. That is the job
//! dropping work at a safe point, not breadram freezing the process, and it
//! does not release any allocation the job chooses to keep. This example keeps
//! nothing across the pause, so RSS falls by about 8 MiB per stopped worker.
//!
//! Prints one JSON line on stdout every half second:
//! `{"units":N,"delta":D,"workers":W,"paused":bool,"rss_kib":K}`.
//! Exits 0 after `$BREADRAM_SECONDS` (default 20).

use std::env;
use std::path::PathBuf;
use std::process::exit;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use breadram_core::concurrency::{pause_active, read_directive, unix_ms, WorkerDirective};

const BUF: usize = 8 * 1024 * 1024;

struct Worker {
    stop: Arc<AtomicBool>,
    handle: thread::JoinHandle<()>,
}

fn main() {
    let path = match env::var("BREADRAM_WORKERS_FILE") {
        Ok(value) if !value.is_empty() => PathBuf::from(value),
        _ => {
            eprintln!("BREADRAM_WORKERS_FILE is required");
            exit(2);
        }
    };
    let seconds: u64 = env::var("BREADRAM_SECONDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    let units = Arc::new(AtomicU64::new(0));
    let mut workers: Vec<Worker> = Vec::new();
    let started = Instant::now();
    let mut last_report = Instant::now() - Duration::from_secs(1);
    let mut last_units = 0_u64;

    while started.elapsed() < Duration::from_secs(seconds) {
        let (want, paused) = desire(&path);
        if paused {
            join_all(&mut workers);
        } else {
            while (workers.len() as u32) > want {
                if let Some(worker) = workers.pop() {
                    stop_one(worker);
                }
            }
            while (workers.len() as u32) < want {
                workers.push(spawn(Arc::clone(&units)));
            }
        }

        if last_report.elapsed() >= Duration::from_millis(500) {
            let now = units.load(Ordering::Relaxed);
            let rss = rss_kib().unwrap_or(0);
            println!(
                "{{\"units\":{now},\"delta\":{},\"workers\":{},\"paused\":{paused},\"rss_kib\":{rss}}}",
                now.saturating_sub(last_units),
                if paused { 0 } else { workers.len() }
            );
            let _ = std::io::Write::flush(&mut std::io::stdout());
            last_units = now;
            last_report = Instant::now();
        }
        thread::sleep(Duration::from_millis(50));
    }
    join_all(&mut workers);
}

fn desire(path: &std::path::Path) -> (u32, bool) {
    let Some(directive) = read_file(path) else {
        return (1, false);
    };
    if pause_active(directive, unix_ms()) {
        return (directive.workers(), true);
    }
    (directive.workers(), false)
}

fn read_file(path: &std::path::Path) -> Option<WorkerDirective> {
    let parent = path.parent()?;
    let name = path.file_name()?.to_str()?;
    let id = name.strip_suffix(".workers")?;
    read_directive(parent, id)
}

fn spawn(units: Arc<AtomicU64>) -> Worker {
    let stop = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        let mut buf = vec![0_u8; BUF];
        let mut n = 0_u64;
        while !flag.load(Ordering::Relaxed) {
            for (i, byte) in buf.iter_mut().enumerate() {
                *byte = (*byte).wrapping_add((i as u8).wrapping_add(n as u8));
            }
            let mut acc = 0_u64;
            for byte in &buf {
                acc = acc.wrapping_mul(131).wrapping_add(*byte as u64);
            }
            std::hint::black_box(acc);
            n += 1;
            if n.is_multiple_of(4) {
                units.fetch_add(4, Ordering::Relaxed);
                thread::sleep(Duration::from_millis(2));
            }
        }
    });
    Worker { stop, handle }
}

fn stop_one(worker: Worker) {
    worker.stop.store(true, Ordering::Relaxed);
    let _ = worker.handle.join();
}

fn join_all(workers: &mut Vec<Worker>) {
    while let Some(worker) = workers.pop() {
        stop_one(worker);
    }
}

fn rss_kib() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb = rest.split_whitespace().next()?;
            return kb.parse().ok();
        }
    }
    None
}
