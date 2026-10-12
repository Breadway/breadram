//! breadram core: the shared contract between breadramd and its clients.
//!
//! This crate holds the types that the daemon, the CLI, and every optional
//! integration agree on. It deliberately depends on nothing from the bread
//! ecosystem: no `breadd`, no `breadsearch`, no model runtime, no GPU library,
//! and no desktop toolkit. Everything here must build and run on a headless
//! Linux host with nothing else installed, because core memory management is
//! not allowed to wait on any optional component.
//!
//! The crate is intentionally small. It defines the contract; the policy,
//! telemetry, and control implementations live in later crates and later
//! milestones (see `docs/ISSUE-MAP.md`).

#![forbid(unsafe_code)]

pub mod adapter;
pub mod capability;
pub mod config;
pub mod control;
pub mod coordinator;
pub mod events;
pub mod execution;
pub mod inventory;
pub mod ipc;
pub mod jobs;
pub mod launch;
pub mod mode;
pub mod policy;
pub mod serving;
pub mod telemetry;
pub mod trends;

pub use capability::{Capability, CapabilityReport, CapabilityState};
pub use ipc::{check_protocol, ProtocolError, PROTOCOL_MAJOR, PROTOCOL_MINOR};
pub use mode::{ManagementMode, ModeParseError};

/// Product name shared by the daemon, the CLI, and the documentation.
pub const PRODUCT_NAME: &str = "breadram";

/// Version of this crate, taken from Cargo metadata at build time.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
