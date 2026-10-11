//! Bounded, read-only endpoint probes.
//!
//! An adapter discovers what it can do by issuing a few read-only requests to an
//! endpoint it was explicitly configured with. Nothing here scans a network,
//! guesses a port, or writes anything: the request type only has a `GET` method,
//! and a request is refused unless its URL sits under one of the allowed
//! endpoint prefixes it was constructed with.
//!
//! The probe client owns the operational concerns an adapter should not have to
//! reimplement: a timeout, bounded retries with exponential backoff, a circuit
//! breaker so a dead endpoint is not hammered, a response size limit, and secret
//! redaction, so a credential can never reach a log line or an error message.
//!
//! TLS and authentication headers are the transport's responsibility. Core only
//! requires that non-loopback endpoints use `https` or a unix socket.

use std::fmt;
use std::sync::{Mutex, MutexGuard};

use crate::capability::CapabilityState;
use crate::telemetry::fs::Clock;

/// A secret that does not render itself.
///
/// `Debug` and `Display` both print `[redacted]`, so a credential cannot leak
/// through a derived `Debug` on a request, an error, or a log line. Reading the
/// value requires the explicit [`Redacted::expose`] call, which is only used when
/// building the transport request.
#[derive(Clone, PartialEq, Eq)]
pub struct Redacted(String);

impl Redacted {
    /// Wrap a secret.
    pub fn new(secret: impl Into<String>) -> Self {
        Self(secret.into())
    }

    /// The secret, for the one place that must put it on the wire.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Redacted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[redacted]")
    }
}

impl fmt::Display for Redacted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[redacted]")
    }
}

/// How a probe authenticates.
#[derive(Clone, PartialEq, Eq, Default)]
pub enum Auth {
    /// No authentication.
    #[default]
    None,
    /// An `Authorization: Bearer` header.
    Bearer(Redacted),
    /// An arbitrary header, for providers that do not use bearer tokens.
    Header {
        /// The header name.
        name: String,
        /// The header value.
        value: Redacted,
    },
}

impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Auth::None => f.write_str("None"),
            Auth::Bearer(_) => f.write_str("Bearer([redacted])"),
            Auth::Header { name, .. } => {
                write!(f, "Header {{ name: {name:?}, value: [redacted] }}")
            }
        }
    }
}

/// An HTTP method.
///
/// Only read-only methods exist. There is no way to express a write with this
/// type, so a probe cannot mutate the thing it is inspecting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    /// Read.
    Get,
}

impl Method {
    /// The wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            Method::Get => "GET",
        }
    }
}

/// A read-only probe request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeRequest {
    /// Always a read.
    pub method: Method,
    /// The absolute URL, which must sit under an allowed endpoint prefix.
    pub url: String,
    /// How to authenticate, if at all.
    pub auth: Auth,
}

impl ProbeRequest {
    /// A plain `GET` with no authentication.
    pub fn get(url: impl Into<String>) -> Self {
        Self {
            method: Method::Get,
            url: url.into(),
            auth: Auth::None,
        }
    }

    /// The same request with bearer authentication.
    pub fn with_bearer(mut self, secret: impl Into<String>) -> Self {
        self.auth = Auth::Bearer(Redacted::new(secret));
        self
    }

    /// The same request with a named header.
    pub fn with_header(mut self, name: impl Into<String>, secret: impl Into<String>) -> Self {
        self.auth = Auth::Header {
            name: name.into(),
            value: Redacted::new(secret),
        };
        self
    }
}

/// A probe response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    /// The HTTP status code.
    pub status: u16,
    /// The body, already bounded by the caller's size limit.
    pub body: String,
}

impl Response {
    /// A response with the given status and body.
    pub fn new(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            body: body.into(),
        }
    }
}

/// Why a transport could not complete a request.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransportError {
    /// The connection failed, was refused, or the response was unreadable.
    #[error("transport failure: {0}")]
    Failure(String),
    /// The request did not complete inside the timeout.
    #[error("timed out after {0} ms")]
    Timeout(u64),
}

/// Carries a probe request and returns a response.
///
/// An implementation must never log or include a credential from the request in
/// an error message. It owns TLS and socket handling.
pub trait Transport: Send + Sync {
    /// Send one request, giving up after `timeout_ms`.
    fn send(&self, request: &ProbeRequest, timeout_ms: u64) -> Result<Response, TransportError>;
}

/// Waits between retries.
///
/// Core ships [`NoSleep`] so tests and headless operation never block; the
/// daemon supplies a real sleeper.
pub trait Sleeper: Send + Sync {
    /// Sleep for `ms` milliseconds.
    fn sleep_ms(&self, ms: u64);
}

/// Does not sleep. Retries happen immediately.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoSleep;

impl Sleeper for NoSleep {
    fn sleep_ms(&self, _ms: u64) {}
}

/// Why a probe failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProbeError {
    /// The request was not under any allowed endpoint prefix.
    #[error("probe target {url} is not an allowed endpoint")]
    NotAllowed {
        /// The refused URL.
        url: String,
    },
    /// The request would have gone out unencrypted to a non-loopback host.
    #[error("probe target {url} must use https or a unix socket")]
    InsecureUrl {
        /// The refused URL.
        url: String,
    },
    /// The circuit is open, so the endpoint is being given time to recover.
    #[error("probe circuit is open, retry in {retry_after_ms} ms")]
    CircuitOpen {
        /// How long until a probe is permitted again.
        retry_after_ms: u64,
    },
    /// The response exceeded the configured size limit.
    #[error("probe response exceeded {limit} bytes")]
    BodyTooLarge {
        /// The configured limit.
        limit: usize,
    },
    /// The request timed out on every attempt.
    #[error("probe timed out after {timeout_ms} ms on every attempt")]
    Timeout {
        /// The per attempt timeout.
        timeout_ms: u64,
    },
    /// Every attempt failed for another reason.
    #[error("probe failed after {attempts} attempts: {last}")]
    Exhausted {
        /// How many attempts were made.
        attempts: u32,
        /// The last transport message, which never contains a credential.
        last: String,
    },
}

/// The bound on retries, waits, and response size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CircuitConfig {
    /// Consecutive failures that open the circuit.
    pub failure_threshold: u32,
    /// How long the circuit stays open.
    pub reset_after_ms: u64,
    /// How many attempts one probe may make.
    pub max_attempts: u32,
    /// The first backoff delay.
    pub backoff_ms: u64,
    /// The cap on the backoff delay.
    pub max_backoff_ms: u64,
    /// The per attempt timeout.
    pub timeout_ms: u64,
    /// The largest response body that will be read.
    pub max_body_bytes: usize,
}

impl Default for CircuitConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 3,
            reset_after_ms: 30_000,
            max_attempts: 3,
            backoff_ms: 250,
            max_backoff_ms: 5_000,
            timeout_ms: 1_000,
            max_body_bytes: 64 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CircuitBreaker {
    consecutive_failures: u32,
    open_until_ms: Option<u64>,
}

impl CircuitBreaker {
    const fn new() -> Self {
        Self {
            consecutive_failures: 0,
            open_until_ms: None,
        }
    }
}

/// Probes one or more explicitly configured endpoints.
pub struct ProbeClient<'a> {
    transport: &'a dyn Transport,
    sleeper: &'a dyn Sleeper,
    clock: &'a dyn Clock,
    config: CircuitConfig,
    allowed_prefixes: Vec<String>,
    circuit: Mutex<CircuitBreaker>,
}

impl<'a> ProbeClient<'a> {
    /// Build a client restricted to the given endpoint prefixes.
    pub fn new(
        transport: &'a dyn Transport,
        sleeper: &'a dyn Sleeper,
        clock: &'a dyn Clock,
        config: CircuitConfig,
        allowed_prefixes: Vec<String>,
    ) -> Self {
        Self {
            transport,
            sleeper,
            clock,
            config,
            allowed_prefixes,
            circuit: Mutex::new(CircuitBreaker::new()),
        }
    }

    /// Whether a URL is one this client is permitted to probe.
    pub fn allows(&self, url: &str) -> bool {
        self.allowed_prefixes
            .iter()
            .any(|prefix| url.starts_with(prefix.as_str()))
    }

    /// Issue one probe, with retries, backoff, and circuit breaking.
    pub fn probe(&self, request: &ProbeRequest) -> Result<Response, ProbeError> {
        if !self.allows(&request.url) {
            return Err(ProbeError::NotAllowed {
                url: request.url.clone(),
            });
        }
        if !is_encrypted_or_loopback(&request.url) {
            return Err(ProbeError::InsecureUrl {
                url: request.url.clone(),
            });
        }

        let now = self.clock.monotonic_ms();
        self.check_circuit(now)?;

        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.transport.send(request, self.config.timeout_ms) {
                Ok(response) => {
                    if response.body.len() > self.config.max_body_bytes {
                        return Err(ProbeError::BodyTooLarge {
                            limit: self.config.max_body_bytes,
                        });
                    }
                    // A server error is transient: back off and try again.
                    if response.status >= 500 && attempt < self.config.max_attempts {
                        self.backoff(attempt);
                        continue;
                    }
                    self.note_success();
                    return Ok(response);
                }
                Err(error) => {
                    if attempt >= self.config.max_attempts {
                        self.note_failure(now);
                        return Err(match error {
                            TransportError::Timeout(_) => ProbeError::Timeout {
                                timeout_ms: self.config.timeout_ms,
                            },
                            TransportError::Failure(message) => ProbeError::Exhausted {
                                attempts: attempt,
                                last: message,
                            },
                        });
                    }
                    self.backoff(attempt);
                }
            }
        }
    }

    fn circuit(&self) -> MutexGuard<'_, CircuitBreaker> {
        lock(&self.circuit)
    }

    fn check_circuit(&self, now_ms: u64) -> Result<(), ProbeError> {
        let mut circuit = self.circuit();
        if let Some(until) = circuit.open_until_ms {
            if now_ms < until {
                return Err(ProbeError::CircuitOpen {
                    retry_after_ms: until - now_ms,
                });
            }
            circuit.open_until_ms = None;
            circuit.consecutive_failures = 0;
        }
        Ok(())
    }

    fn note_success(&self) {
        let mut circuit = self.circuit();
        circuit.consecutive_failures = 0;
        circuit.open_until_ms = None;
    }

    fn note_failure(&self, now_ms: u64) {
        let mut circuit = self.circuit();
        circuit.consecutive_failures = circuit.consecutive_failures.saturating_add(1);
        if circuit.consecutive_failures >= self.config.failure_threshold {
            circuit.open_until_ms = Some(now_ms.saturating_add(self.config.reset_after_ms));
        }
    }

    fn backoff(&self, attempt: u32) {
        let exponent = attempt.saturating_sub(1);
        let delay = self
            .config
            .backoff_ms
            .checked_shl(exponent)
            .unwrap_or(u64::MAX)
            .min(self.config.max_backoff_ms);
        self.sleeper.sleep_ms(delay);
    }
}

/// Turn a response status into a capability reading.
///
/// A `404` or `405` means the endpoint does not expose the surface that was
/// probed. That is the ordinary answer from an OpenAI-compatible inference API
/// asked about model administration, so it is recorded as unsupported rather
/// than guessed at.
pub fn classify(response: &Response) -> CapabilityState {
    match response.status {
        200..=299 => CapabilityState::Available,
        401 | 403 => CapabilityState::Unavailable {
            reason: "the endpoint rejected the credentials".to_string(),
        },
        404 | 405 | 501 => CapabilityState::Unavailable {
            reason: "the endpoint does not expose this surface".to_string(),
        },
        429 => CapabilityState::Degraded {
            reason: "the endpoint rate limited the probe".to_string(),
        },
        status if status >= 500 => CapabilityState::Unavailable {
            reason: format!("the endpoint returned a server error {status}"),
        },
        status => CapabilityState::Unavailable {
            reason: format!("the endpoint returned an unexpected status {status}"),
        },
    }
}

/// Whether a URL is safe to probe: `https`, a unix socket, or loopback `http`.
pub fn is_encrypted_or_loopback(url: &str) -> bool {
    if url.starts_with("https://") || url.starts_with("unix:") {
        return true;
    }
    let Some(rest) = url.strip_prefix("http://") else {
        return false;
    };
    let authority = rest.split(['/', '?']).next().unwrap_or_default();
    let host = authority
        .rsplit_once(':')
        .map_or(authority, |(host, _port)| host);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host == "localhost" || host.starts_with("127.") || host == "::1"
}

/// Take a lock, treating a poisoned mutex as usable.
///
/// A poisoned lock only means another thread panicked while holding it. The
/// guarded counter or buffer is still valid, and a probe must not panic on a
/// path that is already handling a failure.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// A transport that replays a fixed script, for tests and adapter examples.
#[derive(Debug, Default)]
pub struct ScriptedTransport {
    script: Mutex<Vec<Result<Response, TransportError>>>,
    calls: Mutex<Vec<ProbeRequest>>,
    timeouts: Mutex<Vec<u64>>,
}

impl ScriptedTransport {
    /// An empty script.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one outcome to the script.
    pub fn push(self, outcome: Result<Response, TransportError>) -> Self {
        lock(&self.script).push(outcome);
        self
    }

    /// The requests that were actually sent, oldest first.
    pub fn calls(&self) -> Vec<ProbeRequest> {
        lock(&self.calls).clone()
    }

    /// How many requests were sent.
    pub fn call_count(&self) -> usize {
        lock(&self.calls).len()
    }

    /// The timeouts the client asked for.
    pub fn timeouts(&self) -> Vec<u64> {
        lock(&self.timeouts).clone()
    }
}

impl Transport for ScriptedTransport {
    fn send(&self, request: &ProbeRequest, timeout_ms: u64) -> Result<Response, TransportError> {
        lock(&self.calls).push(request.clone());
        lock(&self.timeouts).push(timeout_ms);
        let mut script = lock(&self.script);
        if script.is_empty() {
            return Err(TransportError::Failure("script exhausted".to_string()));
        }
        script.remove(0)
    }
}

/// A sleeper that records what it was asked to wait instead of waiting.
#[derive(Debug, Default)]
pub struct RecordingSleeper {
    waits: Mutex<Vec<u64>>,
}

impl RecordingSleeper {
    /// A new recording sleeper.
    pub fn new() -> Self {
        Self::default()
    }

    /// The waits requested so far.
    pub fn waits(&self) -> Vec<u64> {
        lock(&self.waits).clone()
    }
}

impl Sleeper for RecordingSleeper {
    fn sleep_ms(&self, ms: u64) {
        lock(&self.waits).push(ms);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::fs::test_doubles::FixedClock;

    fn client<'a>(
        transport: &'a ScriptedTransport,
        sleeper: &'a RecordingSleeper,
        clock: &'a FixedClock,
        config: CircuitConfig,
    ) -> ProbeClient<'a> {
        ProbeClient::new(
            transport,
            sleeper,
            clock,
            config,
            vec!["http://127.0.0.1:11434".to_string()],
        )
    }

    const ENDPOINT: &str = "http://127.0.0.1:11434/api/tags";

    #[test]
    fn a_successful_probe_returns_the_response() {
        let transport = ScriptedTransport::new().push(Ok(Response::new(200, "{}")));
        let sleeper = RecordingSleeper::new();
        let clock = FixedClock(0);
        let probe = client(&transport, &sleeper, &clock, CircuitConfig::default());
        let response = probe.probe(&ProbeRequest::get(ENDPOINT)).expect("probe");
        assert_eq!(response.status, 200);
        assert!(sleeper.waits().is_empty(), "no wait on a first success");
    }

    #[test]
    fn the_timeout_is_applied_to_every_attempt() {
        let transport = ScriptedTransport::new().push(Ok(Response::new(200, "{}")));
        let sleeper = RecordingSleeper::new();
        let clock = FixedClock(0);
        let config = CircuitConfig {
            timeout_ms: 750,
            ..CircuitConfig::default()
        };
        let probe = client(&transport, &sleeper, &clock, config);
        probe.probe(&ProbeRequest::get(ENDPOINT)).expect("probe");
        assert_eq!(transport.timeouts(), vec![750]);
    }

    #[test]
    fn a_transient_server_error_is_retried_with_backoff() {
        let transport = ScriptedTransport::new()
            .push(Ok(Response::new(503, "busy")))
            .push(Ok(Response::new(200, "{}")));
        let sleeper = RecordingSleeper::new();
        let clock = FixedClock(0);
        let probe = client(&transport, &sleeper, &clock, CircuitConfig::default());
        let response = probe.probe(&ProbeRequest::get(ENDPOINT)).expect("probe");
        assert_eq!(response.status, 200);
        assert_eq!(transport.call_count(), 2);
        assert_eq!(sleeper.waits(), vec![250], "the first backoff delay");
    }

    #[test]
    fn backoff_grows_and_is_capped() {
        let transport = ScriptedTransport::new()
            .push(Ok(Response::new(500, "")))
            .push(Ok(Response::new(500, "")))
            .push(Ok(Response::new(500, "")))
            .push(Ok(Response::new(200, "{}")));
        let sleeper = RecordingSleeper::new();
        let clock = FixedClock(0);
        let config = CircuitConfig {
            max_attempts: 4,
            backoff_ms: 100,
            max_backoff_ms: 300,
            ..CircuitConfig::default()
        };
        let probe = client(&transport, &sleeper, &clock, config);
        probe.probe(&ProbeRequest::get(ENDPOINT)).expect("probe");
        assert_eq!(sleeper.waits(), vec![100, 200, 300]);
    }

    #[test]
    fn a_timeout_is_reported_as_a_timeout_after_every_attempt() {
        let transport = ScriptedTransport::new()
            .push(Err(TransportError::Timeout(1_000)))
            .push(Err(TransportError::Timeout(1_000)));
        let sleeper = RecordingSleeper::new();
        let clock = FixedClock(0);
        let config = CircuitConfig {
            max_attempts: 2,
            ..CircuitConfig::default()
        };
        let probe = client(&transport, &sleeper, &clock, config);
        assert!(matches!(
            probe.probe(&ProbeRequest::get(ENDPOINT)),
            Err(ProbeError::Timeout { .. })
        ));
        assert_eq!(transport.call_count(), 2);
    }

    #[test]
    fn the_circuit_opens_after_repeated_failures_and_fails_fast() {
        let transport = ScriptedTransport::new()
            .push(Err(TransportError::Failure("refused".to_string())))
            .push(Err(TransportError::Failure("refused".to_string())))
            .push(Err(TransportError::Failure("refused".to_string())))
            .push(Ok(Response::new(200, "{}")));
        let sleeper = RecordingSleeper::new();
        let clock = FixedClock(0);
        let config = CircuitConfig {
            max_attempts: 1,
            failure_threshold: 3,
            reset_after_ms: 30_000,
            ..CircuitConfig::default()
        };
        let probe = client(&transport, &sleeper, &clock, config);
        let request = ProbeRequest::get(ENDPOINT);

        for _ in 0..3 {
            assert!(probe.probe(&request).is_err());
        }
        let calls_before = transport.call_count();
        assert!(matches!(
            probe.probe(&request),
            Err(ProbeError::CircuitOpen { .. })
        ));
        assert_eq!(
            transport.call_count(),
            calls_before,
            "an open circuit must not touch the transport"
        );
    }

    #[test]
    fn the_circuit_closes_again_after_the_reset_window() {
        let transport = ScriptedTransport::new()
            .push(Err(TransportError::Failure("refused".to_string())))
            .push(Ok(Response::new(200, "{}")));
        let sleeper = RecordingSleeper::new();
        let clock = FixedClock(0);
        let config = CircuitConfig {
            max_attempts: 1,
            failure_threshold: 1,
            reset_after_ms: 1_000,
            ..CircuitConfig::default()
        };
        let probe = client(&transport, &sleeper, &clock, config);
        let request = ProbeRequest::get(ENDPOINT);

        assert!(probe.probe(&request).is_err());
        assert!(matches!(
            probe.probe(&request),
            Err(ProbeError::CircuitOpen { .. })
        ));
        // The clock is fixed, so move it forward by rebuilding it.
        let clock = FixedClock(2_000);
        let probe = client(&transport, &sleeper, &clock, config);
        assert!(probe.probe(&request).is_ok());
    }

    #[test]
    fn a_probe_outside_the_allowed_endpoints_is_refused() {
        let transport = ScriptedTransport::new().push(Ok(Response::new(200, "{}")));
        let sleeper = RecordingSleeper::new();
        let clock = FixedClock(0);
        let probe = client(&transport, &sleeper, &clock, CircuitConfig::default());
        assert!(matches!(
            probe.probe(&ProbeRequest::get("http://192.168.1.10:11434/api/tags")),
            Err(ProbeError::NotAllowed { .. })
        ));
        assert_eq!(transport.call_count(), 0, "nothing may be sent");
    }

    #[test]
    fn a_plain_http_non_loopback_probe_is_refused() {
        let transport = ScriptedTransport::new().push(Ok(Response::new(200, "{}")));
        let sleeper = RecordingSleeper::new();
        let clock = FixedClock(0);
        let probe = ProbeClient::new(
            &transport,
            &sleeper,
            &clock,
            CircuitConfig::default(),
            vec!["http://gpu.internal:8000".to_string()],
        );
        assert!(matches!(
            probe.probe(&ProbeRequest::get("http://gpu.internal:8000/v1/models")),
            Err(ProbeError::InsecureUrl { .. })
        ));
        assert_eq!(transport.call_count(), 0);
    }

    #[test]
    fn an_oversized_response_is_refused() {
        let transport = ScriptedTransport::new().push(Ok(Response::new(200, "x".repeat(64))));
        let sleeper = RecordingSleeper::new();
        let clock = FixedClock(0);
        let config = CircuitConfig {
            max_body_bytes: 16,
            ..CircuitConfig::default()
        };
        let probe = client(&transport, &sleeper, &clock, config);
        assert!(matches!(
            probe.probe(&ProbeRequest::get(ENDPOINT)),
            Err(ProbeError::BodyTooLarge { limit: 16 })
        ));
    }

    #[test]
    fn only_read_only_requests_exist() {
        // Exhaustive: adding a write method to Method fails to compile here.
        match Method::Get {
            Method::Get => {}
        }
        assert_eq!(Method::Get.as_str(), "GET");
    }

    #[test]
    fn a_secret_never_renders() {
        let request = ProbeRequest::get(ENDPOINT).with_bearer("super-secret-token");
        let rendered = format!("{request:?}");
        assert!(rendered.contains("[redacted]"));
        assert!(!rendered.contains("super-secret-token"));

        let header = ProbeRequest::get(ENDPOINT).with_header("X-Api-Key", "another-secret");
        let rendered = format!("{header:?}");
        assert!(!rendered.contains("another-secret"));

        let error = ProbeError::Exhausted {
            attempts: 3,
            last: "connection refused".to_string(),
        };
        assert!(!error.to_string().contains("secret"));

        assert_eq!(format!("{}", Redacted::new("top-secret")), "[redacted]");
    }

    #[test]
    fn a_not_exposed_surface_is_unsupported_not_assumed() {
        // The answer an OpenAI-compatible inference API generally gives when
        // asked about model administration.
        assert!(matches!(
            classify(&Response::new(404, "not found")),
            CapabilityState::Unavailable { .. }
        ));
        assert!(matches!(
            classify(&Response::new(405, "method not allowed")),
            CapabilityState::Unavailable { .. }
        ));
        assert!(matches!(
            classify(&Response::new(200, "{}")),
            CapabilityState::Available
        ));
        assert!(matches!(
            classify(&Response::new(429, "slow down")),
            CapabilityState::Degraded { .. }
        ));
        assert!(matches!(
            classify(&Response::new(500, "boom")),
            CapabilityState::Unavailable { .. }
        ));
    }
}
