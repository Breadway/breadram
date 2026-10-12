//! The optional bread integration.
//!
//! breadram publishes bounded pressure and honors a small set of commands, but
//! only when breadd is actually there and speaks a compatible API. Every call
//! here degrades to doing nothing when the bus is absent, slow, or speaks an
//! incompatible version, so standalone memory management never depends on bread.
//!
//! Events and commands use schema version 1. A command is
//! `bread.command.ram.<verb>` carrying `version` and `request_id`. Completion is
//! `bread.ram.<verb>.done` or `.failed` carrying the same request id, and that
//! emit is not itself a confirmation: a command finishes only when a subscriber
//! echoes the request id back, or when the deadline passes. Publishing is best
//! effort. A publish with nobody listening is a timeout, not a success.

use std::sync::{Arc, Mutex};

use bread_utils::bread_client::{BreadClient, BreadEvent, Subscription};
use breadram_core::events::{valid_command_namespace, PendingCommands};
use breadram_core::telemetry::Clock;
use breadram_core::ManagementMode;
use serde_json::{json, Value};

use crate::daemon::SharedState;

/// The app id breadram is registered under.
const APP_ID: &str = "ram";
/// The oldest bread API this build speaks. Newer 1.x releases stay compatible.
const MIN_API: (u64, u64, u64) = (1, 7, 0);
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

/// A command this build has accepted and is waiting to hear back about.
struct OpenCommand {
    verb: String,
    request_id: String,
}

/// The bus client, the commands waiting on a confirmation, and the commands the
/// subscription thread has received.
pub struct Bridge {
    client: BreadClient,
    /// Kept alive for the bridge: dropping it stops the subscription thread.
    _subscription: Subscription,
    pending: PendingCommands,
    /// Commands accepted this session that still need an external confirmation.
    open: Vec<OpenCommand>,
    inbox: Arc<Mutex<Vec<BreadEvent>>>,
    /// When the bus was last asked which API it speaks.
    checked_at_ms: u64,
    /// Whether the bus answered with a compatible version last time.
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
        Self::from_client(BreadClient::connect(APP_ID))
    }

    fn from_client(client: BreadClient) -> Self {
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
            open: Vec::new(),
            inbox,
            checked_at_ms: 0,
            supported: false,
        }
    }
}

/// Whether `reported` is a bread API this build can talk to.
///
/// `1.7.0` is the floor: that is the release that made unsourced command events
/// legal. Later 1.x releases (the running daemon reports `1.7.1`, source is
/// `1.8.0`) keep that contract, so an exact pin would ignore a current bus.
/// A missing version, a 0.x bus, or a 2.x bus is not usable.
pub fn compatible_api(reported: Option<&str>) -> bool {
    let Some(reported) = reported else {
        return false;
    };
    let mut parts = reported.split('.');
    let Some(major) = parts.next().and_then(|part| part.parse::<u64>().ok()) else {
        return false;
    };
    let minor = parts
        .next()
        .and_then(|part| part.parse::<u64>().ok())
        .unwrap_or(0);
    let patch = parts
        .next()
        .and_then(|part| part.parse::<u64>().ok())
        .unwrap_or(0);
    // Trailing build metadata is ignored; a fourth numeric component is not a
    // version this build claims to understand.
    if parts.next().is_some() {
        return false;
    }
    (major, minor, patch) >= MIN_API && major == MIN_API.0
}

/// Publish the current pressure and honor any commands that arrived.
///
/// Nothing here blocks the monitor for long: the client connects per call with
/// its own short timeouts, and a missing bus returns at once. The returned
/// effects are mode changes the caller applies in memory only.
pub(crate) fn tick(state: &Arc<SharedState>, bridge: &mut Bridge) -> Vec<CommandEffect> {
    if !usable(bridge, state.clock.monotonic_ms()) {
        return Vec::new();
    }
    publish_pressure(state, bridge);
    take_commands(state, bridge)
}

/// Whether the bus is present and speaks a compatible version.
///
/// Rechecked at most every few seconds, so a bus that appears, disappears, or
/// restarts onto a new version is noticed without a socket round trip on every
/// pass. A bus that cannot be reached is not usable, and that changes nothing
/// about how breadram manages memory.
fn usable(bridge: &mut Bridge, now_ms: u64) -> bool {
    if now_ms.saturating_sub(bridge.checked_at_ms) < COMMAND_TIMEOUT_MS && bridge.checked_at_ms != 0
    {
        return bridge.supported;
    }
    bridge.checked_at_ms = now_ms;
    bridge.supported = compatible_api(bridge.client.api_version().as_deref());
    bridge.supported
}

/// Publish one bounded pressure reading. The units are named in the fields.
fn publish_pressure(state: &Arc<SharedState>, bridge: &Bridge) {
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
    settle(bridge, now_ms);
    let events: Vec<BreadEvent> = bridge
        .inbox
        .lock()
        .map(|mut inbox| inbox.drain(..).collect())
        .unwrap_or_default();
    for event in &events {
        if let Some(effect) = handle_command(bridge, event, now_ms) {
            effects.push(effect);
        }
    }
    effects
}

/// Close commands whose deadline passed, and record confirmations a subscriber
/// echoed back. Our own completion emit is not in this inbox, so it cannot
/// confirm itself.
fn settle(bridge: &mut Bridge, now_ms: u64) {
    for id in bridge.pending.expire(now_ms) {
        let verb = take_open(bridge, &id).unwrap_or_else(|| "action".to_string());
        emit_completion(
            bridge,
            &verb,
            &id,
            false,
            "timed out before it was confirmed",
        );
    }
}

/// Honor one command, or reject it with a reason.
///
/// A well-formed command is applied and then left pending. Success is a later
/// confirmation that names the same request id, not the completion emit itself.
fn handle_command(bridge: &mut Bridge, event: &BreadEvent, now_ms: u64) -> Option<CommandEffect> {
    if let Some(request_id) = confirmation_id(event) {
        let success = event.event.ends_with(".done");
        let _ = bridge.pending.confirm(&request_id, success, now_ms);
        take_open(bridge, &request_id);
        return None;
    }
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
        emit_completion(
            bridge,
            verb,
            request_id,
            false,
            "unsupported command version",
        );
        return None;
    }
    let deadline = now_ms.saturating_add(COMMAND_TIMEOUT_MS);
    if !bridge.pending.begin(request_id, deadline) {
        // A duplicate, empty, oversized, or over-capacity id is refused rather
        // than run twice.
        emit_completion(bridge, verb, request_id, false, "rejected request id");
        return None;
    }
    match verb {
        "status" => {
            emit_completion(bridge, verb, request_id, true, &status_detail(bridge));
            remember(bridge, verb, request_id);
            None
        }
        "mode" => match requested_mode(&event.data) {
            Some(mode) => {
                emit_completion(bridge, verb, request_id, true, "mode applied");
                remember(bridge, verb, request_id);
                Some(CommandEffect::Mode(mode))
            }
            None => {
                // Refused before it was pending work: drop the reservation so
                // the id can be reused, and do not wait for a confirmation of
                // a change that did not happen.
                let _ = bridge.pending.confirm(request_id, false, now_ms);
                emit_completion(bridge, verb, request_id, false, "unknown management mode");
                None
            }
        },
        "action" => {
            // An action command names a supported kind and nothing else: there
            // is no field that runs a program. Accepting it means it was well
            // formed. Carrying it out still belongs to the planner.
            let ok = supported_kind(&event.data);
            if ok {
                emit_completion(bridge, verb, request_id, true, "action accepted");
                remember(bridge, verb, request_id);
            } else {
                let _ = bridge.pending.confirm(request_id, false, now_ms);
                emit_completion(bridge, verb, request_id, false, "unsupported action");
            }
            None
        }
        _ => None,
    }
}

/// The request id of a completion a subscriber echoed, when this event is one.
fn confirmation_id(event: &BreadEvent) -> Option<String> {
    let rest = event.event.strip_prefix("bread.ram.")?;
    let (verb, outcome) = rest.rsplit_once('.')?;
    if !VERBS.contains(&verb) || (outcome != "done" && outcome != "failed") {
        return None;
    }
    event
        .data
        .get("request_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
}

fn remember(bridge: &mut Bridge, verb: &str, request_id: &str) {
    bridge.open.push(OpenCommand {
        verb: verb.to_string(),
        request_id: request_id.to_string(),
    });
}

fn take_open(bridge: &mut Bridge, request_id: &str) -> Option<String> {
    let index = bridge
        .open
        .iter()
        .position(|command| command.request_id == request_id)?;
    Some(bridge.open.swap_remove(index).verb)
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

/// The pressure a status command reports, so the completion carries the reading.
fn status_detail(_bridge: &Bridge) -> String {
    // The caller publishes pressure before honoring commands, so a subscriber
    // already has the reading. The completion names that it was reported.
    "reported".to_string()
}

/// Publish the completion for one command. Best effort, like every emit.
///
/// This does not confirm the command. `PendingCommands::confirm` only runs
/// when a subscriber echoes the request id, or the deadline expires it.
fn emit_completion(bridge: &Bridge, verb: &str, request_id: &str, success: bool, detail: &str) {
    let outcome = if success { "done" } else { "failed" };
    bridge.client.emit(
        &format!("bread.ram.{verb}.{outcome}"),
        json!({
            "version": 1,
            "request_id": request_id,
            "detail": detail,
        }),
    );
}

/// Drive one command through the bridge without a daemon, for tests.
#[cfg(test)]
fn drive(bridge: &mut Bridge, event: BreadEvent, now_ms: u64) -> Vec<CommandEffect> {
    bridge.checked_at_ms = now_ms;
    bridge.supported = true;
    settle(bridge, now_ms);
    let mut effects = Vec::new();
    if let Some(effect) = handle_command(bridge, &event, now_ms) {
        effects.push(effect);
    }
    effects
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hides the real bread config and runtime dir for one test.
    ///
    /// `resolve_socket_path` prefers `~/.config/bread/breadd.toml`, which on
    /// this host names the live socket. A test of the absent-bus path has to
    /// move `HOME` and `XDG_RUNTIME_DIR` or it dials the running daemon.
    struct AbsentBus {
        previous_home: Option<String>,
        previous_runtime: Option<String>,
    }

    impl AbsentBus {
        fn enter() -> Self {
            let previous_home = std::env::var("HOME").ok();
            let previous_runtime = std::env::var("XDG_RUNTIME_DIR").ok();
            let dir = std::env::temp_dir().join(format!(
                "breadram-no-bus-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0)
            ));
            std::fs::create_dir_all(&dir).unwrap();
            // SAFETY: restored on drop. These tests do not spawn threads that
            // read HOME before this returns, and the client reads it lazily.
            unsafe {
                std::env::set_var("HOME", &dir);
                std::env::set_var("XDG_RUNTIME_DIR", &dir);
            }
            Self {
                previous_home,
                previous_runtime,
            }
        }
    }

    impl Drop for AbsentBus {
        fn drop(&mut self) {
            unsafe {
                restore("HOME", &self.previous_home);
                restore("XDG_RUNTIME_DIR", &self.previous_runtime);
            }
        }
    }

    unsafe fn restore(key: &str, previous: &Option<String>) {
        match previous {
            Some(value) => unsafe { std::env::set_var(key, value) },
            None => unsafe { std::env::remove_var(key) },
        }
    }

    fn command(name: &str, data: Value) -> BreadEvent {
        BreadEvent {
            event: name.to_string(),
            timestamp: 0,
            data,
        }
    }

    fn harness() -> Bridge {
        // connect() does not open a socket. subscribe() starts a thread that
        // backs off while breadd is absent, which is the degradation path.
        Bridge::from_client(BreadClient::connect("ram"))
    }

    #[test]
    fn api_floor_accepts_current_buses_and_rejects_others() {
        assert!(compatible_api(Some("1.7.0")));
        assert!(compatible_api(Some("1.7.1")));
        assert!(compatible_api(Some("1.8.0")));
        assert!(!compatible_api(None));
        assert!(!compatible_api(Some("1.6.9")));
        assert!(!compatible_api(Some("2.0.0")));
        assert!(!compatible_api(Some("not-a-version")));
    }

    #[test]
    fn absent_bus_does_not_become_usable() {
        // This host runs breadd. Point the client at a directory with no
        // socket so the test is actually about an absent bus.
        let _guard = AbsentBus::enter();
        let mut bridge = harness();
        // A missing bus is not a compatible bus, and checking it does not
        // panic or block the caller.
        assert!(!usable(&mut bridge, 1));
        assert_eq!(bridge.checked_at_ms, 1);
        // Inside the recheck window the cached answer stands, so a dead bus
        // is not dialed on every pass.
        assert!(!usable(&mut bridge, 2));
        assert_eq!(bridge.checked_at_ms, 1);
        // After the window it is asked again, and still absent.
        assert!(!usable(&mut bridge, 1 + COMMAND_TIMEOUT_MS));
        assert_eq!(bridge.checked_at_ms, 1 + COMMAND_TIMEOUT_MS);
    }

    #[test]
    fn duplicate_command_is_refused_and_completion_is_not_success() {
        let mut bridge = harness();
        let mode = command(
            "bread.command.ram.mode",
            json!({"version": 1, "request_id": "mode-1", "mode": "automatic"}),
        );
        let effects = drive(&mut bridge, mode.clone(), 1_000);
        assert_eq!(
            effects,
            vec![CommandEffect::Mode(ManagementMode::Automatic)]
        );
        // Emitting .done did not confirm it: the id is still pending, so the
        // same command is refused instead of applied twice.
        let again = drive(&mut bridge, mode, 1_100);
        assert!(again.is_empty());
        assert_eq!(
            bridge
                .open
                .iter()
                .filter(|open| open.request_id == "mode-1")
                .count(),
            1
        );
    }

    #[test]
    fn no_subscriber_times_out_instead_of_succeeding() {
        let mut bridge = harness();
        let mode = command(
            "bread.command.ram.mode",
            json!({"version": 1, "request_id": "mode-2", "mode": "monitor"}),
        );
        assert_eq!(
            drive(&mut bridge, mode, 1_000),
            vec![CommandEffect::Mode(ManagementMode::Monitor)]
        );
        // Nobody echoed the request id back. Past the deadline the command is
        // a timeout, and it is no longer open.
        settle(&mut bridge, 1_000 + COMMAND_TIMEOUT_MS);
        assert!(bridge.open.is_empty());
        assert_eq!(bridge.pending.confirm("mode-2", true, 1_000), None);
    }

    #[test]
    fn subscriber_confirmation_completes_and_a_late_one_does_not() {
        let mut bridge = harness();
        let mode = command(
            "bread.command.ram.mode",
            json!({"version": 1, "request_id": "mode-3", "mode": "automatic"}),
        );
        drive(&mut bridge, mode, 1_000);
        let echo = command(
            "bread.ram.mode.done",
            json!({"version": 1, "request_id": "mode-3"}),
        );
        assert!(drive(&mut bridge, echo, 1_500).is_empty());
        assert!(bridge.open.is_empty());
        // A second echo finds nothing pending.
        assert_eq!(bridge.pending.confirm("mode-3", true, 1_600), None);
    }

    #[test]
    fn reconnect_window_rechecks_a_bus_that_went_away() {
        let _guard = AbsentBus::enter();
        let mut bridge = harness();
        bridge.checked_at_ms = 1_000;
        bridge.supported = true;
        // Still inside the window: the last answer stands, so a blip does not
        // drop memory management onto a health call every pass.
        assert!(usable(&mut bridge, 1_000 + COMMAND_TIMEOUT_MS - 1));
        // The window elapsed and no bus is listening, so support drops.
        assert!(!usable(&mut bridge, 1_000 + COMMAND_TIMEOUT_MS));
        assert!(!bridge.supported);
    }
}
