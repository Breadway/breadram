//! The breadsearch model-lifecycle adapter.
//!
//! breadmill holds one embedding model behind a Unix socket: it indexes files
//! with it and answers queries with it. The disposable idle resource is the
//! loaded ONNX session, not the index, the configuration, or the model files,
//! and all of those stay where they are across a release. When the daemon is
//! idle and automated, breadram asks it to drop the session; the next search
//! starts a background reload itself and says the model is loading, so the
//! search that triggers the reload retries and the one after finds a warm
//! model.
//!
//! What it can honestly report:
//!
//! - **Inventory**, from the socket `Status`: the model file size the daemon
//!   reports. A memory-mapped model shares pages with the page cache, so this
//!   is an estimate of model bytes and never a measurement of host RAM use.
//! - **Activity** from the same `Status`: in-flight queries, files awaiting
//!   indexing, and a running background load are work. Anything else fresh is
//!   idle; anything stale is unknown, which policy never treats as idle.
//!
//! Every socket path this adapter can touch is derived from the one configured
//! endpoint, and every verb it can send is listed in the module constants. A
//! daemon old enough to lack the unload verbs answers them with a parse
//! error, which is reported rather than guessed at: observation keeps working
//! against old daemons, and no unload is ever offered to one. A missing socket
//! is not an error to retry loudly: breadsearch may simply not be installed,
//! and breadram works without it.

use std::io::{BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use serde::Deserialize;

use crate::config::{ActionKind, ProviderKind};
use crate::inventory::Workload;
use crate::policy::activity::ActivityEvidence;

use super::capability::{AdapterCapabilities, AdapterCapability};
use super::{
    AdapterOperation, Confirmation, ManagedResource, OperationMethod, OperationRequest,
    ProbeRequest, WorkloadAdapter,
};

/// The socket verb that reports what the daemon holds.
pub const STATUS_VERB: &str = "status";
/// The socket verb that drops the loaded session.
pub const UNLOAD_VERB: &str = "unload";
/// The socket verb that starts a background load.
pub const LOAD_VERB: &str = "load";

/// How long one socket round trip may take, in milliseconds.
///
/// Status and unload answers are immediate; a load verb only starts the
/// background load and answers at once, so nothing here waits out a model
/// load.
pub const SOCKET_TIMEOUT_MS: u64 = 5_000;
/// How much of a socket answer is read before it is refused, in bytes.
///
/// Status documents are a few hundred bytes. Anything past the cap is not a
/// status.
pub const MAX_SOCKET_RESPONSE_BYTES: usize = 64 * 1024;

/// Why a socket round trip could not be used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SocketError {
    /// Nothing listens at the socket path, or it is not a socket at all.
    /// Breadsearch may simply not be installed, which is ordinary.
    #[error("no breadmill answers at {0}")]
    Unreachable(String),
    /// The daemon did not answer in time.
    #[error("breadmill did not answer within {0} ms")]
    Timeout(u64),
    /// The answer was larger than the cap.
    #[error("breadmill answered with more than {0} bytes")]
    TooLarge(usize),
    /// The bytes on the wire failed.
    #[error("the breadmill conversation failed: {0}")]
    Io(String),
    /// The answer was not the documented shape.
    #[error("the breadmill answer could not be read: {0}")]
    Malformed(String),
}

/// One JSON round trip over a daemon socket.
///
/// The runner owns every execution safeguard: timeouts on connect, read, and
/// write, and a response cap. The adapter owns the protocol on top: what the
/// answer means.
pub trait SocketRunner: Send + Sync {
    /// Send `body` (one JSON line) to `socket_path` and return the one-line
    /// answer.
    fn request(&self, socket_path: &str, body: &str) -> Result<String, SocketError>;
}

/// A real Unix socket round trip with timeouts and a response cap.
#[derive(Debug, Clone, Copy, Default)]
pub struct UnixSocketRunner;

impl SocketRunner for UnixSocketRunner {
    fn request(&self, socket_path: &str, body: &str) -> Result<String, SocketError> {
        let timeout = Duration::from_millis(SOCKET_TIMEOUT_MS);
        let stream = UnixStream::connect(socket_path)
            .map_err(|_| SocketError::Unreachable(socket_path.to_string()))?;
        stream
            .set_read_timeout(Some(timeout))
            .and_then(|()| stream.set_write_timeout(Some(timeout)))
            .map_err(|error| SocketError::Io(error.to_string()))?;
        let mut line = body.to_string();
        if !line.ends_with('\n') {
            line.push('\n');
        }
        stream
            .try_clone()
            .and_then(|mut writer| writer.write_all(line.as_bytes()))
            .map_err(|error| SocketError::Io(error.to_string()))?;
        let mut reader = BufReader::new(stream);
        let mut answer = Vec::new();
        loop {
            let mut chunk = [0_u8; 4096];
            match reader.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => {
                    answer.extend_from_slice(&chunk[..n]);
                    if answer.len() > MAX_SOCKET_RESPONSE_BYTES {
                        return Err(SocketError::TooLarge(MAX_SOCKET_RESPONSE_BYTES));
                    }
                    if answer.contains(&b'\n') {
                        break;
                    }
                }
                Err(error) => {
                    if error.kind() == std::io::ErrorKind::TimedOut
                        || error.kind() == std::io::ErrorKind::WouldBlock
                    {
                        return Err(SocketError::Timeout(SOCKET_TIMEOUT_MS));
                    }
                    return Err(SocketError::Io(error.to_string()));
                }
            }
        }
        let line = answer
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|at| &answer[..at])
            .unwrap_or(&answer);
        String::from_utf8(line.to_vec()).map_err(|error| SocketError::Malformed(error.to_string()))
    }
}

/// The daemon status, as breadmill reports it.
///
/// Every field but the first three is new with the unload verbs, so each is
/// optional: an older daemon still parses, and its silence on the new fields
/// is exactly how it is told apart.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct BreadmillStatus {
    /// Chunks indexed.
    #[serde(default)]
    pub indexed: usize,
    /// Files awaiting indexing.
    #[serde(default)]
    pub pending: usize,
    /// A model session is loaded.
    #[serde(default)]
    pub model_ready: bool,
    /// Queries currently inside an embedding call.
    #[serde(default)]
    pub in_flight: usize,
    /// A background model load is running.
    #[serde(default)]
    pub loading: bool,
    /// The model file size in bytes from the last load, when one happened.
    #[serde(default)]
    pub model_bytes: Option<u64>,
    /// The daemon version that answered. Absent on older daemons.
    #[serde(default)]
    pub version: Option<String>,
    /// Total queries the daemon has served since it started.
    ///
    /// A query holds `in_flight` for milliseconds, so a poll every few
    /// seconds almost never sees one. This counter does not depend on timing:
    /// any change between two readings proves queries ran in between. Absent
    /// on daemons that predate it, which gives no signal either way.
    #[serde(default)]
    pub queries_served: Option<u64>,
}

/// Whether this reading, set against the previous one, shows work.
///
/// Queries in flight, files awaiting indexing, and a running background load
/// are all work: a load compiles the backend and would resurrect an unloaded
/// model the moment it finishes. A moved query counter is work that finished
/// between two polls. A counter that went backwards means the daemon restarted
/// in between, which is also activity, never idleness.
fn shows_work(previous: Option<&BreadmillStatus>, status: &BreadmillStatus) -> bool {
    if status.in_flight > 0 || status.pending > 0 || status.loading {
        return true;
    }
    match (
        previous.and_then(|previous| previous.queries_served),
        status.queries_served,
    ) {
        (Some(before), Some(now)) => before != now,
        _ => false,
    }
}

/// Parse a status answer: a `status_info` response or, for tolerance, a bare
/// status object.
pub fn parse_status(body: &str) -> Result<BreadmillStatus, String> {
    #[derive(Deserialize)]
    struct Tagged {
        #[serde(rename = "type")]
        kind: String,
        #[serde(flatten)]
        status: BreadmillStatus,
    }
    if let Ok(tagged) = serde_json::from_str::<Tagged>(body) {
        if tagged.kind == "status_info" {
            return Ok(tagged.status);
        }
    }
    // A tagged answer of any other kind (an `ok`, an `error`, hits) is a
    // misrouted response, not a status. Only an untagged object is parsed
    // leniently, for older and future shapes.
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(body) {
        if value
            .as_object()
            .is_some_and(|object| object.contains_key("type"))
        {
            return Err("not a breadmill status: tagged with another response kind".to_string());
        }
    }
    serde_json::from_str::<BreadmillStatus>(body)
        .map_err(|error| format!("not a breadmill status: {error}"))
}

/// Whether a breadmill old enough to lack a version field can unload.
///
/// Only a daemon that reports a version speaks the unload verbs. Absence of
/// the field is the old daemon, not a new one being modest.
pub fn supports_unload(status: &BreadmillStatus) -> bool {
    status.version.is_some()
}

/// The adapter's view of one breadmill daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Observation {
    /// The last status that parsed.
    status: Option<BreadmillStatus>,
    /// Whether the last refresh read the daemon successfully.
    fresh: bool,
    /// Why the last refresh could not be used, when it could not.
    last_error: Option<String>,
    /// When the daemon was first seen holding nothing, in monotonic ms.
    idle_since_ms: Option<u64>,
    /// When the last refresh ran, in monotonic ms.
    observed_at_ms: Option<u64>,
}

impl Observation {
    fn empty() -> Self {
        Self {
            status: None,
            fresh: false,
            last_error: None,
            idle_since_ms: None,
            observed_at_ms: None,
        }
    }
}

/// An adapter for one configured breadmill daemon.
///
/// The adapter performs no I/O: it asks for socket reads through the runner
/// the caller supplies to [`BreadsearchAdapter::refresh`], and records the
/// answers. Until a status has parsed, the workload reports unknown activity,
/// which policy never treats as idle.
pub struct BreadsearchAdapter {
    name: String,
    socket_path: String,
    endpoint_url: String,
    scope: String,
    capabilities: AdapterCapabilities,
    state: Mutex<Observation>,
}

impl std::fmt::Debug for BreadsearchAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BreadsearchAdapter")
            .field("name", &self.name)
            .field("socket_path", &self.socket_path)
            .field("scope", &self.scope)
            .field("capabilities", &self.capabilities)
            .field("state", &self.state)
            .finish()
    }
}

impl BreadsearchAdapter {
    /// An adapter for one daemon socket, owning the workload with this scope.
    pub fn new(
        name: impl Into<String>,
        socket_path: impl Into<String>,
        scope: impl Into<String>,
    ) -> Self {
        let socket_path = socket_path.into();
        let endpoint_url = format!("breadsearch:{socket_path}");
        Self {
            name: name.into(),
            socket_path,
            endpoint_url,
            scope: scope.into(),
            capabilities: AdapterCapabilities::observation_only(),
            state: Mutex::new(Observation::empty()),
        }
    }

    /// State the capability set, for example from the registry.
    pub fn with_capabilities(mut self, capabilities: AdapterCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    fn state(&self) -> MutexGuard<'_, Observation> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Why the last reading could not be used, when it could not.
    pub fn last_error(&self) -> Option<String> {
        self.state().last_error.clone()
    }

    fn owns_scope(&self, workload: &Workload) -> bool {
        workload.id.scope == self.scope
    }

    /// Read the daemon once and record what came back.
    ///
    /// A failed read is recorded as a failed reading rather than leaving the
    /// previous success in place, and the returned notes name the endpoint.
    /// Missing breadsearch is ordinary (it may not be installed) and reads
    /// that way rather than as an error to chase.
    pub fn refresh(&self, runner: &dyn SocketRunner, at_ms: u64) -> Vec<String> {
        let mut observation = self.state();
        observation.observed_at_ms = Some(at_ms);
        match runner.request(&self.socket_path, r#"{"type":"status"}"#) {
            Ok(body) => match parse_status(&body) {
                Ok(status) => {
                    if shows_work(observation.status.as_ref(), &status) {
                        observation.idle_since_ms = None;
                    } else if observation.idle_since_ms.is_none() {
                        observation.idle_since_ms = Some(at_ms);
                    }
                    observation.status = Some(status);
                    observation.fresh = true;
                    observation.last_error = None;
                    Vec::new()
                }
                Err(reason) => {
                    observation.status = None;
                    observation.fresh = false;
                    observation.idle_since_ms = None;
                    observation.last_error = Some(reason.clone());
                    vec![format!(
                        "provider {} answered, but {reason}; treating its model as unknown",
                        self.name
                    )]
                }
            },
            Err(SocketError::Unreachable(_)) => {
                observation.status = None;
                observation.fresh = false;
                observation.idle_since_ms = None;
                observation.last_error = None;
                vec![format!(
                    "provider {} has no daemon at {}: breadsearch may not be installed",
                    self.name, self.socket_path
                )]
            }
            Err(error) => {
                observation.status = None;
                observation.fresh = false;
                observation.idle_since_ms = None;
                observation.last_error = Some(error.to_string());
                vec![format!("provider {} could not be read: {error}", self.name)]
            }
        }
    }

    /// Record a socket answer outside the refresh path.
    ///
    /// The executor calls this with every confirmation read, so the next
    /// activity reading is the one just taken rather than the last tick's.
    pub fn note_socket_result(
        &self,
        result: Result<String, SocketError>,
        at_ms: u64,
    ) -> Vec<String> {
        match result {
            Ok(body) => match parse_status(&body) {
                Ok(status) => {
                    let mut observation = self.state();
                    if shows_work(observation.status.as_ref(), &status) {
                        observation.idle_since_ms = None;
                    } else if observation.idle_since_ms.is_none() {
                        observation.idle_since_ms = Some(at_ms);
                    }
                    observation.status = Some(status);
                    observation.fresh = true;
                    observation.last_error = None;
                    observation.observed_at_ms = Some(at_ms);
                    Vec::new()
                }
                Err(reason) => {
                    let mut observation = self.state();
                    observation.fresh = false;
                    observation.idle_since_ms = None;
                    observation.last_error = Some(reason.clone());
                    vec![format!(
                        "provider {} answered, but {reason}; treating its model as unknown",
                        self.name
                    )]
                }
            },
            Err(error) => {
                let mut observation = self.state();
                observation.fresh = false;
                observation.idle_since_ms = None;
                observation.last_error = Some(error.to_string());
                vec![format!("provider {} could not be read: {error}", self.name)]
            }
        }
    }
}

/// The socket path a `breadsearch:` endpoint URL names.
///
/// Endpoint URLs look like `breadsearch:/run/user/1000/breadmill.sock`, and
/// operation URLs append a verb: `breadsearch:/run/user/1000/breadmill.sock:unload`.
/// Anything else is not a breadsearch URL and never touches a socket.
pub fn socket_path_for(url: &str) -> Option<(&str, &str)> {
    let rest = url.strip_prefix("breadsearch:")?;
    let (path, verb) = rest.rsplit_once(':')?;
    if !path.starts_with('/') || verb.is_empty() {
        return None;
    }
    if !matches!(verb, STATUS_VERB | UNLOAD_VERB | LOAD_VERB) {
        return None;
    }
    Some((path, verb))
}

/// Whether a URL names a breadsearch socket operation.
pub fn is_breadsearch_url(url: &str) -> bool {
    socket_path_for(url).is_some()
}

/// What the executor needs to reach a socket adapter's daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocketSpec {
    /// The socket path, never a URL or a command.
    pub socket_path: String,
}

/// The operation URL for a verb on an endpoint.
pub fn operation_url(endpoint: &str, verb: &str) -> String {
    format!("{endpoint}:{verb}")
}

impl WorkloadAdapter for BreadsearchAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> ProviderKind {
        ProviderKind::Breadsearch
    }

    fn endpoint(&self) -> &str {
        &self.endpoint_url
    }

    fn capabilities(&self) -> &AdapterCapabilities {
        &self.capabilities
    }

    fn owns(&self, workload: &Workload) -> bool {
        self.owns_scope(workload)
    }

    fn inventory(&self) -> Vec<ManagedResource> {
        let observation = self.state();
        let Some(status) = observation.status.as_ref().filter(|_| observation.fresh) else {
            return Vec::new();
        };
        let idle = status.in_flight == 0 && status.pending == 0 && !status.loading;
        vec![ManagedResource {
            name: format!(
                "breadmill model ({})",
                status.version.as_deref().unwrap_or("unknown")
            ),
            host_ram_bytes: status.model_bytes,
            device_ram_bytes: None,
            idle,
        }]
    }

    fn inventory_is_current(&self) -> bool {
        self.state().fresh
    }

    fn activity(&self, workload: &Workload) -> ActivityEvidence {
        if !self.owns_scope(workload) {
            return ActivityEvidence::Unknown;
        }
        let observation = self.state();
        let Some(status) = observation.status.as_ref().filter(|_| observation.fresh) else {
            return ActivityEvidence::Unknown;
        };
        if status.in_flight > 0 || status.pending > 0 || status.loading {
            return ActivityEvidence::Active;
        }
        match observation.idle_since_ms {
            Some(since_ms) => ActivityEvidence::Idle { since_ms },
            None => ActivityEvidence::Unknown,
        }
    }

    fn socket_spec(&self) -> Option<SocketSpec> {
        Some(SocketSpec {
            socket_path: self.socket_path.clone(),
        })
    }

    fn note_socket_result(&self, result: Result<String, SocketError>, at_ms: u64) -> Vec<String> {
        self.note_socket_result(result, at_ms)
    }

    fn refresh_socket(&self, runner: &dyn SocketRunner, at_ms: u64) -> Vec<String> {
        self.refresh(runner, at_ms)
    }

    fn operation(&self, action: ActionKind, workload: &Workload) -> Option<AdapterOperation> {
        if action != ActionKind::UnloadIdleModel || !self.owns_scope(workload) {
            return None;
        }
        if !self.capabilities.supports(AdapterCapability::UnloadModel) {
            return None;
        }
        // An older daemon has no unload verb; offering one would only fail
        // loudly at apply time. Observation keeps working against it.
        let observation = self.state();
        let status = observation.status.as_ref().filter(|_| observation.fresh)?;
        if !supports_unload(status) {
            return None;
        }
        if status.in_flight > 0 || status.pending > 0 || !status.model_ready {
            return None;
        }
        let endpoint = format!("breadsearch:{}", self.socket_path);
        let relief = status.model_bytes.map(|bytes| bytes as i64).unwrap_or(0);
        Some(AdapterOperation {
            kind: ActionKind::UnloadIdleModel,
            request: OperationRequest {
                method: OperationMethod::Delete,
                url: operation_url(&endpoint, UNLOAD_VERB),
                auth: super::probe::Auth::None,
                body: None,
            },
            expected_host_ram_delta: -relief,
            expected_device_ram_delta: 0,
            reversible: true,
            note: format!(
                "unloads the breadmill embedding model (about {} bytes of mapped model, cold start reloads it on the next search)",
                status.model_bytes.map(|bytes| bytes.to_string()).as_deref().unwrap_or("unknown")
            ),
            confirmation: Confirmation {
                request: ProbeRequest::get(operation_url(&endpoint, STATUS_VERB)),
                expect_body_contains: Some("\"model_ready\":false".to_string()),
                expect_body_absent: None,
                expect_model_status: None,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    /// One scripted answer per socket call, in order, every call recorded.
    #[derive(Default)]
    struct ScriptedSockets {
        answers: std::sync::Mutex<VecDeque<Result<String, SocketError>>>,
        calls: std::sync::Mutex<Vec<(String, String)>>,
    }

    impl ScriptedSockets {
        fn answer(body: &str) -> Self {
            Self {
                answers: std::sync::Mutex::new(vec![Ok(body.to_string())].into()),
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn failing(error: SocketError) -> Self {
            Self {
                answers: std::sync::Mutex::new(vec![Err(error)].into()),
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl SocketRunner for ScriptedSockets {
        fn request(&self, socket_path: &str, body: &str) -> Result<String, SocketError> {
            self.calls
                .lock()
                .expect("not poisoned")
                .push((socket_path.to_string(), body.to_string()));
            self.answers
                .lock()
                .expect("not poisoned")
                .pop_front()
                .unwrap_or_else(|| panic!("no socket answer scripted for {body}"))
        }
    }

    const SOCKET: &str = "/run/user/1000/breadmill.sock";
    const SCOPE: &str = "unit:breadmill.service";

    const IDLE_STATUS: &str = r#"{"type":"status_info","indexed":41,"pending":0,"model_ready":true,"in_flight":0,"loading":false,"model_bytes":547310275,"version":"0.4.0"}"#;
    const BUSY_STATUS: &str = r#"{"type":"status_info","indexed":41,"pending":0,"model_ready":true,"in_flight":2,"loading":false,"model_bytes":547310275,"version":"0.4.0"}"#;
    const INDEXING_STATUS: &str = r#"{"type":"status_info","indexed":40,"pending":3,"model_ready":true,"in_flight":0,"loading":false,"model_bytes":547310275,"version":"0.4.0"}"#;
    const OLD_STATUS: &str =
        r#"{"type":"status_info","indexed":41,"pending":0,"model_ready":true}"#;
    const UNLOADED_STATUS: &str = r#"{"type":"status_info","indexed":41,"pending":0,"model_ready":false,"in_flight":0,"loading":false,"model_bytes":547310275,"version":"0.4.0"}"#;

    /// A quiet status carrying a query counter, for the polling-gap tests.
    fn served(count: u64) -> String {
        format!(
            r#"{{"type":"status_info","indexed":41,"pending":0,"model_ready":true,"in_flight":0,"loading":false,"model_bytes":547310275,"version":"0.4.0","queries_served":{count}}}"#
        )
    }

    fn adapter() -> BreadsearchAdapter {
        BreadsearchAdapter::new("breadmill", SOCKET, SCOPE).with_capabilities(
            AdapterCapabilities::configured(&[
                AdapterCapability::Inventory,
                AdapterCapability::Activity,
                AdapterCapability::UnloadModel,
            ]),
        )
    }

    fn workload() -> Workload {
        Workload {
            id: crate::inventory::WorkloadId {
                scope: SCOPE.to_string(),
                generation: 1,
            },
            name: "breadmill.service".to_string(),
            pids: vec![100],
            accounting: crate::inventory::MemoryAccounting::Cgroup {
                current_bytes: Some(1024),
            },
            control: crate::inventory::ControlLevel::Eligible,
            limit_bytes: None,
            ancestor_bytes: None,
        }
    }

    #[test]
    fn status_parses_new_old_and_garbage() {
        let status = parse_status(IDLE_STATUS).unwrap();
        assert_eq!(status.in_flight, 0);
        assert_eq!(status.model_bytes, Some(547_310_275));
        assert_eq!(status.version.as_deref(), Some("0.4.0"));
        assert!(supports_unload(&status));
        let old = parse_status(OLD_STATUS).unwrap();
        assert_eq!(old.in_flight, 0);
        assert_eq!(old.model_bytes, None);
        assert_eq!(old.version, None);
        assert!(!supports_unload(&old));
        assert!(parse_status("not json").is_err());
        assert!(parse_status(r#"{"type":"ok"}"#).is_err());
    }

    #[test]
    fn socket_urls_name_a_path_and_a_verb() {
        assert_eq!(
            socket_path_for("breadsearch:/run/user/1000/breadmill.sock:unload"),
            Some(("/run/user/1000/breadmill.sock", "unload"))
        );
        assert!(socket_path_for("breadsearch:/run/user/1000/breadmill.sock").is_none());
        assert!(socket_path_for("breadsearch:relative.sock:unload").is_none());
        assert!(socket_path_for("breadsearch:/run/user/1000/breadmill.sock:query").is_none());
        assert!(socket_path_for("http://localhost:11434").is_none());
        assert!(is_breadsearch_url("breadsearch:/x.sock:status"));
        assert!(!is_breadsearch_url("hook:trainer/status"));
    }

    #[test]
    fn queries_finishing_between_polls_keep_the_daemon_active() {
        // Every poll sees in_flight 0, as a millisecond query always leaves
        // it, yet the counter moves each time: the daemon is in use.
        let adapter = adapter();
        // The first reading has nothing to compare against and starts the
        // idle clock. The two readings after it each show the counter moved.
        for (at_ms, count) in [(60_000, 10), (63_000, 11), (66_000, 13)] {
            let sockets = ScriptedSockets::answer(&served(count));
            assert!(adapter.refresh(&sockets, at_ms).is_empty());
        }
        assert_eq!(
            adapter.activity(&workload()),
            ActivityEvidence::Unknown,
            "a moving counter must never read as confirmed idle"
        );
        // Unknown evidence is what keeps the policy from planning an unload.
        assert!(!adapter
            .activity(&workload())
            .is_confirmed_idle(120_000, 30_000));
    }

    #[test]
    fn a_flat_counter_lets_idleness_accrue_from_the_first_flat_reading() {
        let adapter = adapter();
        let first = ScriptedSockets::answer(&served(10));
        adapter.refresh(&first, 60_000);
        // The first reading has nothing to compare against, so it counts as
        // quiet and the idle clock starts there.
        assert_eq!(
            adapter.activity(&workload()),
            ActivityEvidence::Idle { since_ms: 60_000 }
        );
        let flat = ScriptedSockets::answer(&served(10));
        adapter.refresh(&flat, 63_000);
        assert_eq!(
            adapter.activity(&workload()),
            ActivityEvidence::Idle { since_ms: 60_000 }
        );
        let moved = ScriptedSockets::answer(&served(11));
        adapter.refresh(&moved, 66_000);
        assert_eq!(adapter.activity(&workload()), ActivityEvidence::Unknown);
        let flat_again = ScriptedSockets::answer(&served(11));
        adapter.refresh(&flat_again, 69_000);
        assert_eq!(
            adapter.activity(&workload()),
            ActivityEvidence::Idle { since_ms: 69_000 },
            "idleness restarts from the first flat reading after activity"
        );
    }

    #[test]
    fn a_confirmation_read_compares_against_the_previous_reading() {
        // The executor feeds every confirmation read through
        // note_socket_result. It must compare against the stored reading, so a
        // counter that moved is not swallowed by landing between two polls.
        let adapter = adapter();
        adapter.refresh(&ScriptedSockets::answer(&served(10)), 60_000);
        let notes = adapter.note_socket_result(Ok(served(11)), 62_000);
        assert!(notes.is_empty());
        assert_eq!(adapter.activity(&workload()), ActivityEvidence::Unknown);
        // The next poll is flat against the confirmation read, so idleness
        // restarts there.
        adapter.refresh(&ScriptedSockets::answer(&served(11)), 63_000);
        assert_eq!(
            adapter.activity(&workload()),
            ActivityEvidence::Idle { since_ms: 63_000 }
        );
    }

    #[test]
    fn a_counter_that_goes_backwards_is_activity_not_idleness() {
        // A restarted daemon begins again at zero.
        let adapter = adapter();
        adapter.refresh(&ScriptedSockets::answer(&served(500)), 60_000);
        adapter.refresh(&ScriptedSockets::answer(&served(0)), 63_000);
        assert_eq!(adapter.activity(&workload()), ActivityEvidence::Unknown);
    }

    #[test]
    fn a_daemon_without_the_counter_behaves_as_before() {
        // No counter on either reading gives no signal: only in-flight work,
        // pending files and loads count, exactly as they did.
        let adapter = adapter();
        adapter.refresh(&ScriptedSockets::answer(IDLE_STATUS), 60_000);
        adapter.refresh(&ScriptedSockets::answer(IDLE_STATUS), 63_000);
        assert_eq!(
            adapter.activity(&workload()),
            ActivityEvidence::Idle { since_ms: 60_000 }
        );
    }

    #[test]
    fn refresh_records_idle_busy_and_absence() {
        let adapter = adapter();
        let sockets = ScriptedSockets::answer(IDLE_STATUS);
        assert!(adapter.refresh(&sockets, 60_000).is_empty());
        assert!(adapter.inventory_is_current());
        assert_eq!(
            adapter.activity(&workload()),
            ActivityEvidence::Idle { since_ms: 60_000 }
        );
        let inventory = adapter.inventory();
        assert_eq!(inventory.len(), 1);
        assert_eq!(inventory[0].host_ram_bytes, Some(547_310_275));
        assert!(inventory[0].idle);

        let busy = ScriptedSockets::answer(BUSY_STATUS);
        assert!(adapter.refresh(&busy, 61_000).is_empty());
        assert_eq!(adapter.activity(&workload()), ActivityEvidence::Active);

        let indexing = ScriptedSockets::answer(INDEXING_STATUS);
        assert!(adapter.refresh(&indexing, 62_000).is_empty());
        assert_eq!(adapter.activity(&workload()), ActivityEvidence::Active);

        let gone = ScriptedSockets::failing(SocketError::Unreachable(SOCKET.to_string()));
        let notes = adapter.refresh(&gone, 63_000);
        assert!(
            notes
                .iter()
                .any(|note| note.contains("may not be installed")),
            "{notes:?}"
        );
        assert!(!adapter.inventory_is_current());
        assert_eq!(adapter.activity(&workload()), ActivityEvidence::Unknown);
        assert!(adapter.inventory().is_empty());
    }

    #[test]
    fn unload_is_offered_only_when_idle_ready_and_new() {
        let adapter = adapter();
        let sockets = ScriptedSockets::answer(IDLE_STATUS);
        adapter.refresh(&sockets, 60_000);
        let operation = adapter
            .operation(ActionKind::UnloadIdleModel, &workload())
            .expect("an idle loaded model unloads");
        assert_eq!(
            operation.request.url,
            "breadsearch:/run/user/1000/breadmill.sock:unload"
        );
        assert_eq!(operation.expected_host_ram_delta, -(547_310_275_i64));
        assert!(operation.reversible);

        for status in [BUSY_STATUS, INDEXING_STATUS, UNLOADED_STATUS, OLD_STATUS] {
            let fresh = BreadsearchAdapter::new("breadmill", SOCKET, SCOPE).with_capabilities(
                AdapterCapabilities::configured(&[
                    AdapterCapability::Inventory,
                    AdapterCapability::Activity,
                    AdapterCapability::UnloadModel,
                ]),
            );
            let sockets = ScriptedSockets::answer(status);
            fresh.refresh(&sockets, 60_000);
            assert!(
                fresh
                    .operation(ActionKind::UnloadIdleModel, &workload())
                    .is_none(),
                "no unload while busy, indexing, unloaded, or old: {status}"
            );
        }

        let observed = BreadsearchAdapter::new("breadmill", SOCKET, SCOPE)
            .with_capabilities(AdapterCapabilities::observation_only());
        let sockets = ScriptedSockets::answer(IDLE_STATUS);
        observed.refresh(&sockets, 60_000);
        assert!(
            observed
                .operation(ActionKind::UnloadIdleModel, &workload())
                .is_none(),
            "observation only never writes"
        );
    }
}
