//! Read-only file access and a monotonic clock, both injectable.
//!
//! Telemetry reads a handful of `procfs` and cgroup files. Routing that through
//! a trait means the collection layer can be tested against fixtures instead of
//! the live host, and it keeps the read surface small and obviously read-only.

use std::io;
use std::path::Path;
use std::time::Instant;

/// A read-only view of the filesystem, narrow enough to fake in tests.
pub trait FileSource {
    /// Read a whole file to a string, without following anything unusual.
    ///
    /// A missing file or a permission error is an error, never an empty string.
    fn read_to_string(&self, path: &Path) -> io::Result<String>;

    /// Whether `path` exists and is a readable regular file.
    fn is_file(&self, path: &Path) -> bool;

    /// List the entry names directly under a directory, in any order.
    ///
    /// Only the immediate children are returned, and this never recurses. An
    /// unreadable directory is an error.
    fn list_dir(&self, path: &Path) -> io::Result<Vec<String>>;
}

/// Reads from the real filesystem.
#[derive(Debug, Default, Clone, Copy)]
pub struct RealFs;

impl FileSource for RealFs {
    fn read_to_string(&self, path: &Path) -> io::Result<String> {
        std::fs::read_to_string(path)
    }

    fn is_file(&self, path: &Path) -> bool {
        path.is_file()
    }

    fn list_dir(&self, path: &Path) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            if let Some(name) = entry.file_name().to_str() {
                names.push(name.to_string());
            }
        }
        names.sort();
        Ok(names)
    }
}

/// A monotonic clock.
///
/// Observation times must never go backwards, so wall clock adjustments cannot
/// corrupt a time series. Note that `CLOCK_MONOTONIC` does not advance while the
/// machine is suspended, so a suspension appears as a short gap; consumers that
/// care detect it with [`crate::telemetry::snapshot::is_gap`].
pub trait Clock {
    /// Milliseconds since a fixed, monotonic origin.
    fn monotonic_ms(&self) -> u64;
}

/// The process wide monotonic clock.
#[derive(Debug, Clone, Copy)]
pub struct SystemClock {
    start: Instant,
}

impl SystemClock {
    /// Start a clock whose origin is now.
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn monotonic_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
}

#[cfg(test)]
pub(crate) mod test_doubles {
    use super::{Clock, FileSource};
    use std::collections::HashMap;
    use std::io;
    use std::path::{Path, PathBuf};

    /// An in-memory filesystem for parser and collection tests.
    #[derive(Debug, Default, Clone)]
    pub(crate) struct MemoryFs {
        files: HashMap<PathBuf, String>,
    }

    impl MemoryFs {
        pub(crate) fn new() -> Self {
            Self::default()
        }

        pub(crate) fn with(mut self, path: &str, contents: &str) -> Self {
            self.files.insert(PathBuf::from(path), contents.to_string());
            self
        }

        pub(crate) fn without(mut self, path: &str) -> Self {
            self.files.remove(Path::new(path));
            self
        }
    }

    impl FileSource for MemoryFs {
        fn read_to_string(&self, path: &Path) -> io::Result<String> {
            self.files
                .get(path)
                .cloned()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "not in fixture"))
        }

        fn is_file(&self, path: &Path) -> bool {
            self.files.contains_key(path)
        }

        fn list_dir(&self, path: &Path) -> io::Result<Vec<String>> {
            let prefix = format!("{}/", path.display());
            let mut names: Vec<String> = Vec::new();
            for key in self.files.keys() {
                let key = key.display().to_string();
                if let Some(rest) = key.strip_prefix(&prefix) {
                    let name = rest.split('/').next().unwrap_or(rest).to_string();
                    if !names.contains(&name) {
                        names.push(name);
                    }
                }
            }
            if names.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "no such directory in fixture",
                ));
            }
            names.sort();
            Ok(names)
        }
    }

    /// A clock that returns a fixed reading, or a scripted sequence.
    #[derive(Debug, Clone)]
    pub(crate) struct FixedClock(pub(crate) u64);

    impl Clock for FixedClock {
        fn monotonic_ms(&self) -> u64 {
            self.0
        }
    }
}
