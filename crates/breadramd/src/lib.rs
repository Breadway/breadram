//! breadramd, the breadram memory management daemon.
//!
//! The library exposes [`daemon::run`] so the daemon can be started by the
//! binary and by integration tests without duplicating the event loop.

pub mod bread;
pub mod daemon;
