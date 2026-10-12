//! The optional bread integration.
//!
//! breadram publishes bounded pressure and action events and honors a small set
//! of commands, but only when breadd is actually there. Every call here degrades
//! to doing nothing when the bus is absent, slow, or speaks an unsupported
//! version, so standalone memory management never depends on bread.
//!
//! Events and commands use schema version 1. A command is
//! `bread.command.ram.<verb>` carrying `version` and `request_id`, and it
//! completes with `bread.ram.<verb>.done` or `.failed` carrying the same request
//! id. Publishing is best effort: a reply is the only thing that counts as
//! success, and a publish with nobody listening is not one.

use std::sync::{Arc, Mutex};

use bread_utils::bread_client::{BreadClient, BreadEvent, Subscription};
use breadram_core::events::{valid_command_namespace, PendingCommands};
use breadram_core::telemetry::Clock;
use breadram_core::ManagementMode;
use serde_json::{json, Value};

use crate::daemon::SharedState;

/// The app id breadram is registered under.
const APP_ID: &str = "ram";
/// The bread API this build speaks. A bus reporting something else is ignored.
const SUPPORTED_API: &str = "1.7.0";
/// How many commands may await confirmation at once.
const PENDING_CAPACITY: usize = 32;
/// How long a command waits before it is reported as timed out, and how often
/// the bus version is rechecked.
const COMMAND_TIMEOUT_MS: u64 = 5_000;
/// Commands this build honors.
const VERBS: [&str; 3] = ["status", "mode", "action"];

/// What one honored command changed, so the caller can apply it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandEffect {
    /// Switch the management mode.
    Mode(ManagementMode),
}

/// The bus client, the commands waiting on a confirmation, and the commands the
/// subscription thread has received.
pub struct Bridge {
    client: BreadClient,
    /// Kept alive for the bridge: dropping it stops the subscription thread.
    _subscription: Subscription,
    pending: PendingCommands,
    inbox: Arc<Mutex<Vec<BreadEvent>>>,
    /// When the bus was last asked which API it speaks.
    checked_at_ms: u64,
    /// Whether the bus answered with the supported version last time.
    supported: bool,
}

impl Bridge {
    /// A bridge that has not yet asked the bus anything.
    ///
    /// Not a `Default`: building one starts a reconnecting subscription thread,
    /// which is a side effect rather than a plain value. That thread reconnects
    /// on its own, so a bus that appears later is picked up without breadram
    /// restarting.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let client = BreadClient::connect(APP_ID);
        let inbox = Arc::new(Mutex::new(Vec::new()));
        let queued = Arc::clone(&inbox);
        let subscription = client.subscribe("bread.command.ram.**", move |event| {
            if let Ok(mut inbox) = queued.lock() {
                // Bounded like everything else: a flood of commands is dropped
                // rather than allowed to grow without limit.
                if inbox.len() < PENDING_CAPACITY {
                    inbox.push(event);
                }
            }
        });
        Self {
            client,
            _subscription: subscription,
            pending: PendingCommands::new(PENDING_CAPACITY),
            inbox,
            checked_at_ms: 0,
            supported: false,
        }
    }

    /// Whether the bus is present and speaks the supported version.
    ///
    /// Rechecked at most every few seconds, so a bus that appears or disappears
    /// is noticed without a socket round trip on every pass. A bus that cannot
    /// be reached, or that reports another version, is not usable, and that
    /// changes nothing about how breadram manages memory.
    fn usable(&mut self, now_ms: u64) -> bool {
        if now_ms.saturating_sub(self.checked_at_ms) < COMMAND_TIMEOUT_MS {
            return self.supported;
        }
        self.checked_at_ms = now_ms;
        self.supported = self.client.api_version().as_deref() == Some(SUPPORTED_API);
        self.supported
    }
}

/// Publish the current pressure and honor any commands that arrived.
///
/// Nothing here blocks the monitor for long: the client connects per call with
/// its own short timeouts, and a missing bus returns at once. The returned
/// effects are mode changes the caller applies through the same path a
/// configuration edit uses.
pub(crate) fn tick(state: &Arc<SharedState>, bridge: &mut Bridge) -> Vec<CommandEffect> {
    if !bridge.usable(state.clock.monotonic_ms()) {
        return Vec::new();
    }
    publish_pressure(state, bridge);
    take_commands(state, bridge)
}

/// Publish one bounded pressure reading. The units are named in the fields.
fn publish_pressure(state: &Arc<SharedState>, bridge: &Bridge) {
    // The snapshot the daemon already computed this pass, so the event describes
    // the same reading a status command would return.
    let Ok(published) = state.published.try_read() else {
        return;
    };
    bridge.client.emit(
        "bread.ram.pressure",
        json!({
            "version": 1,
            "state": published.pressure.to_string(),
            "mode": published.status.mode.as_str(),
            "available_bytes": published.status.available_bytes,
            "observed_at_ms": published.status.observed_at_ms,
        }),
    );
}

/// Drain the commands that arrived and answer each one.
fn take_commands(state: &Arc<SharedState>, bridge: &mut Bridge) -> Vec<CommandEffect> {
    let mut effects = Vec::new();
    let now_ms = state.clock.monotonic_ms();
    for id in bridge.pending.expire(now_ms) {
        // A command that produced nothing in time is a failure, said so rather
        // than left hanging.
        complete(
            bridge,
            "action",
            &id,
            false,
            "timed out before it was confirmed",
        );
    }
    let events: Vec<BreadEvent> = bridge
        .inbox
        .lock()
        .map(|mut inbox| inbox.drain(..).collect())
        .unwrap_or_default();
    for event in &events {
        if let Some(effect) = handle_command(bridge, event) {
            effects.push(effect);
        }
    }
    effects
}

/// Honor one command, or reject it with a reason.
fn handle_command(bridge: &mut Bridge, event: &BreadEvent) -> Option<CommandEffect> {
    let verb = event.event.rsplit('.').next().unwrap_or("");
    if !valid_command_namespace(&event.event) || !VERBS.contains(&verb) {
        return None;
    }
    let request_id = event
        .data
        .get("request_id")
        .and_then(Value::as_str)
        .unwrap_or("");
    if event.data.get("version").and_then(Value::as_u64) != Some(1) {
        complete(
            bridge,
            verb,
            request_id,
            false,
            "unsupported command version",
        );
        return None;
    }
    let deadline = bridge.checked_at_ms.saturating_add(COMMAND_TIMEOUT_MS);
    if !bridge.pending.begin(request_id, deadline) {
        // A duplicate, empty, oversized, or over-capacity id is refused rather
        // than run twice.
        complete(bridge, verb, request_id, false, "rejected request id");
        return None;
    }
    match verb {
        "status" => {
            complete(bridge, verb, request_id, true, "reported");
            None
        }
        "mode" => match requested_mode(&event.data) {
            Some(mode) => {
                complete(bridge, verb, request_id, true, "mode applied");
                Some(CommandEffect::Mode(mode))
            }
            None => {
                complete(bridge, verb, request_id, false, "unknown management mode");
                None
            }
        },
        "action" => {
            // An action command names a supported kind and nothing else: there is
            // no field that runs a program. Accepting it means it was well formed.
            let ok = supported_kind(&event.data);
            let detail = if ok {
                "action accepted"
            } else {
                "unsupported action"
            };
            complete(bridge, verb, request_id, ok, detail);
            None
        }
        _ => None,
    }
}

/// The mode a command asks for, when it names one breadram knows.
fn requested_mode(data: &Value) -> Option<ManagementMode> {
    match data.get("mode").and_then(Value::as_str) {
        Some("monitor") => Some(ManagementMode::Monitor),
        Some("automatic") => Some(ManagementMode::Automatic),
        _ => None,
    }
}

/// Whether a command names an action kind this build honors.
///
/// Restricted to the two kinds the planner can actually carry out. Anything
/// else, including a field that would name a program, is refused.
fn supported_kind(data: &Value) -> bool {
    matches!(
        data.get("kind").and_then(Value::as_str),
        Some("unload_idle_model" | "cgroup_soft_limit")
    )
}

/// Publish the completion for one command. Best effort, like every emit.
fn complete(bridge: &mut Bridge, verb: &str, request_id: &str, success: bool, detail: &str) {
    let outcome = if success { "done" } else { "failed" };
    bridge.client.emit(
        &format!("bread.ram.{verb}.{outcome}"),
        json!({
            "version": 1,
            "request_id": request_id,
            "detail": detail,
        }),
    );
    // Recorded too, so a late duplicate confirmation cannot count again. Judged
    // just before the deadline: confirm treats an id whose deadline has already
    // arrived as a timeout, so a command answered in the same pass must not be
    // reported as timed out.
    let now = bridge.checked_at_ms.saturating_sub(1);
    let _ = bridge.pending.confirm(request_id, success, now);
}
