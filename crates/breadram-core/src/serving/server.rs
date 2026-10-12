//! The optional local inference gateway: a socket server over the routing core.
//!
//! This is the piece of RAM-45 that faces the network. It admits every request
//! through [`Gateway::route`] before dispatching, so a stream holds its lease
//! for its whole duration, a client disconnect releases it, an idempotent retry
//! joins or replays instead of double counting, and a backend whose idleness
//! cannot be proven is refused rather than served through a path that could be
//! slept from under it.
//!
//! What it preserves end to end:
//!
//! - Native and OpenAI-compatible request and response semantics: the head is
//!   parsed only for routing metadata, the body passes through byte for byte,
//!   and the backend's response head and body are relayed verbatim, including
//!   streams, which relay until the backend closes or the client goes away.
//! - Cancellation: a client that disconnects mid-stream releases its lease, so
//!   the backend becomes idle again instead of holding a dead stream.
//! - Demand-driven wake on the request path: an arrival at a sleeping backend
//!   begins the wake in the lifecycle and answers 503 with a retry hint.
//! - No prompt or completion logging: only heads are read. A route outcome
//!   carries a client label, a request kind, whether the response is streamed,
//!   and an idempotency key, and there is nowhere to store content.
//!
//! Bounds, so one client cannot take the server down: one request per
//! connection (`Connection: close`), a 64 KiB head cap, a 64 MiB body cap for
//! non-streamed traffic, and timeouts on head reads and backend dials. Streams
//! relay until either side closes, which is what makes them streams.
//!
//! Still to come for the full card: performing the provider wake the
//! `WakeThenRetry` outcome begins (today the server records it in the lifecycle
//! and serves the retry hint; driving the provider sleep and wake commands
//! through `SleepDecision`, `DrainOutcome`, and `WakeHealth` belongs to the
//! adapter work), sharing one in-flight response between joined requests rather
//! than proxying each (today every `Forward` outcome is proxied; the core still
//! counts it once), and the operator configuration that points a client at the
//! gateway. Backends are addressed by scope with a fixed generation; tracking a
//! generation rollover across a backend restart is future work.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::inventory::WorkloadId;
use crate::policy::activity::{ActivityEvidence, ActivitySource};

use super::gateway::{
    classify, disposition, Disposition, Gateway, RouteOutcome, ACCEPT_HEADER, CLIENT_HEADER,
    IDEMPOTENCY_HEADER,
};
use super::http::{
    parse_head, parse_response_head, ParseError, RequestHead, ResponseHead, MAX_HEADER_BYTES,
};
use super::lease::{Admission, ClientId, LeaseId, RequestKind};
use super::lifecycle::{Backend, ServingPolicy, TrackingCompleteness, WakeHealth, WakeRequest};

/// The largest head accepted, in bytes.
pub const MAX_HEAD_BYTES: usize = MAX_HEADER_BYTES;
/// The largest non-streamed body proxied in either direction, in bytes.
pub const MAX_BODY_BYTES: u64 = 64 * 1024 * 1024;
/// How long one head read may take.
pub const HEAD_TIMEOUT: Duration = Duration::from_secs(30);
/// How long dialing a backend may take.
pub const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the backend's response head may take.
pub const RESPONSE_HEAD_TIMEOUT: Duration = Duration::from_secs(60);

/// One configured path to a backend.
#[derive(Debug, Clone)]
pub struct Route {
    /// The request-target prefix this route serves, for example `/v1`.
    /// The longest matching prefix wins.
    pub path_prefix: String,
    /// The workload scope the backend serves, for example `unit:llama.service`.
    pub workload_scope: String,
    /// Where the backend listens, for example `127.0.0.1:8080`.
    pub backend_addr: String,
    /// How completely requests are tracked. The gateway itself is complete
    /// tracking; anything else degrades honestly through the core.
    pub tracking: TrackingCompleteness,
    /// How to wake this backend on the request path, when configured. Without
    /// it the server still serves the retry hint and enforces the wake
    /// timeout, but no provider wake is performed.
    pub wake: Option<WakeSpec>,
}

/// How the server wakes one backend on the request path.
///
/// Router-shaped on purpose: the load call names a model id, and health is the
/// same structured id-plus-status listing the adapter confirms against. Other
/// shapes have no wake configured, and their arrivals keep the retry hint
/// without a wake anyone could verify.
#[derive(Debug, Clone)]
pub struct WakeSpec {
    /// The load path, for example `/models/load`.
    pub load_path: String,
    /// The model id to load.
    pub model_id: String,
    /// The listing path that reports the model id with its status, for
    /// example `/models`.
    pub health_path: String,
}

/// The gateway's static configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// The routes to backends, matched by longest request-target prefix.
    pub routes: Vec<Route>,
}

/// The gateway socket server: routing core plus loopback proxying.
pub struct GatewayServer {
    inner: Arc<Inner>,
}

struct Inner {
    gateway: Mutex<Gateway>,
    routes: Vec<Route>,
    addrs: HashMap<String, String>,
    started: Instant,
}

impl GatewayServer {
    /// A server over awake backends built from the configuration.
    pub fn new(config: ServerConfig, now_ms: u64) -> Self {
        let backends = config
            .routes
            .iter()
            .map(|route| {
                Backend::new(
                    workload(&route.workload_scope),
                    route.tracking,
                    ServingPolicy::default(),
                    now_ms,
                )
            })
            .collect();
        Self::with_backends(config, backends)
    }

    /// A server over caller-supplied backends, so a test (or a restart
    /// reconcile) can start from lifecycle states the configuration alone
    /// cannot express, such as a confirmed sleep.
    pub fn with_backends(config: ServerConfig, backends: Vec<Backend>) -> Self {
        let addrs = config
            .routes
            .iter()
            .map(|route| (route.workload_scope.clone(), route.backend_addr.clone()))
            .collect();
        Self {
            inner: Arc::new(Inner {
                gateway: Mutex::new(Gateway::new(backends)),
                routes: config.routes,
                addrs,
                started: Instant::now(),
            }),
        }
    }

    /// Milliseconds since the server started, for lease timestamps.
    pub fn now_ms(&self) -> u64 {
        self.inner.started.elapsed().as_millis() as u64
    }

    /// What the gateway knows about a workload: active while a lease is in
    /// flight, idle once the last one ends, unknown when untracked.
    pub async fn evidence(&self, workload_scope: &str) -> ActivityEvidence {
        self.inner
            .gateway
            .lock()
            .await
            .evidence(&workload(workload_scope))
    }

    /// Serve connections until the listener closes. One task per connection.
    pub async fn serve(&self, listener: TcpListener) {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let state = Arc::clone(&self.inner);
            tokio::spawn(async move {
                serve_conn(state, stream).await;
            });
        }
    }
}

/// The workload identity the server uses: by scope, with a fixed generation.
fn workload(scope: &str) -> WorkloadId {
    WorkloadId {
        scope: scope.to_string(),
        generation: 1,
    }
}

/// The longest configured prefix matching the target, if any.
fn match_route<'a>(routes: &'a [Route], target: &str) -> Option<&'a Route> {
    routes
        .iter()
        .filter(|route| target.starts_with(route.path_prefix.as_str()))
        .max_by_key(|route| route.path_prefix.len())
}

async fn serve_conn(state: Arc<Inner>, mut client: TcpStream) {
    let (head, mut buffered) = match read_head(&mut client, HEAD_TIMEOUT).await {
        Ok(found) => found,
        Err(status) => {
            let _ = write_error(&mut client, status, "the request head was not usable").await;
            return;
        }
    };
    if head.content_length.is_some_and(|len| len > MAX_BODY_BYTES) {
        let _ = write_error(
            &mut client,
            413,
            "the request body exceeds the gateway limit",
        )
        .await;
        return;
    }
    let Some(route) = match_route(&state.routes, &head.target) else {
        let _ = write_error(&mut client, 404, "no gateway route serves this path").await;
        return;
    };
    let scope = route.workload_scope.clone();
    let addr = state
        .addrs
        .get(&scope)
        .cloned()
        .unwrap_or_else(|| route.backend_addr.clone());
    let wake_spec = route.wake.clone();
    let meta = classify(
        head.header(CLIENT_HEADER),
        head.header(IDEMPOTENCY_HEADER),
        head.header(ACCEPT_HEADER),
    );
    let client_label = meta.client.clone();
    let now = state.started.elapsed().as_millis() as u64;
    let outcome = state
        .gateway
        .lock()
        .await
        .route(&workload(&scope), meta.clone(), now);
    match disposition(&meta, &outcome) {
        Disposition::Proxy { .. } => {
            let RouteOutcome::Forward { admission, .. } = outcome else {
                let _ =
                    write_error(&mut client, 502, "the routing decision was not forwardable").await;
                return;
            };
            let body = buffered.split_off(head.body_offset);
            let head_bytes = buffered;
            proxy(
                &state,
                &scope,
                &client_label,
                &addr,
                &head,
                head_bytes,
                body,
                client,
                admission,
            )
            .await;
        }
        Disposition::Retry { retry_after_ms } => {
            let reason = match &outcome {
                RouteOutcome::WakeThenRetry { wake, .. } => {
                    // The lifecycle began the wake; performing it is this
                    // task's job, with the lifecycle's own timeout as the
                    // bound. The client is answered at once either way.
                    let driver = Arc::clone(&state);
                    let scope = scope.clone();
                    let addr = addr.clone();
                    let wake = wake.clone();
                    let spec = wake_spec.clone();
                    tokio::spawn(async move {
                        drive_wake(driver, scope, addr, wake, spec).await;
                    });
                    "the backend is waking: the wake was begun on the request path; \
                     retry after the hint"
                        .to_string()
                }
                RouteOutcome::Retry { reason, .. } => reason.clone(),
                _ => "the backend is not serving yet".to_string(),
            };
            let _ = write_retry(&mut client, retry_after_ms, &reason).await;
        }
        Disposition::Refuse => {
            let reason = match &outcome {
                RouteOutcome::Refused { reason, .. } => reason.clone(),
                _ => "the backend cannot serve".to_string(),
            };
            let _ = write_error(&mut client, 502, &reason).await;
        }
    }
}

/// Read one head, with a bound and a timeout. Reads from a client or a backend.
async fn read_head(
    stream: &mut TcpStream,
    timeout: Duration,
) -> Result<(RequestHead, Vec<u8>), u16> {
    let mut buffered = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let read = tokio::time::timeout(timeout, stream.read(&mut chunk))
            .await
            .map_err(|_| 408u16)?
            .map_err(|_| 400u16)?;
        if read == 0 {
            return Err(400);
        }
        buffered.extend_from_slice(&chunk[..read]);
        if buffered.len() > MAX_HEAD_BYTES + 4096 {
            return Err(413);
        }
        match parse_head(&buffered) {
            Ok(head) => return Ok((head, buffered)),
            Err(ParseError::Incomplete) => continue,
            Err(ParseError::TooLarge) => return Err(413),
            Err(ParseError::Malformed) => return Err(400),
        }
    }
}

/// How often a waking backend is polled for health.
pub const WAKE_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// The largest health listing read, in bytes.
const MAX_HEALTH_BYTES: u64 = 1024 * 1024;

/// Perform one request-path wake: run the provider load, then verify health
/// until the lifecycle's own timeout.
///
/// Without a wake spec there is nothing to perform, so the task simply holds
/// the timeout: the lifecycle still bounds the waking state either way. A
/// result that arrives after the backend already moved on is ignored by the
/// lifecycle rather than applied, so a slow poll cannot corrupt the state.
async fn drive_wake(
    state: Arc<Inner>,
    scope: String,
    addr: String,
    wake: WakeRequest,
    spec: Option<WakeSpec>,
) {
    let timeout = Duration::from_millis(wake.timeout_ms);
    let health = match tokio::time::timeout(timeout, perform_wake(&addr, &spec)).await {
        Ok(health) => health,
        Err(_) => {
            let now = state.started.elapsed().as_millis() as u64;
            state
                .gateway
                .lock()
                .await
                .backend_mut(&workload(&scope))
                .map(|backend| backend.wake_timed_out(now));
            return;
        }
    };
    let now = state.started.elapsed().as_millis() as u64;
    state
        .gateway
        .lock()
        .await
        .backend_mut(&workload(&scope))
        .map(|backend| backend.wake_completed(health, now));
}

/// Run the provider wake and verify it, or explain why it did not happen.
async fn perform_wake(addr: &str, spec: &Option<WakeSpec>) -> WakeHealth {
    let Some(spec) = spec else {
        // No provider wake is configured: hold the timeout so the lifecycle
        // still bounds the waking state, then report through the timeout path.
        std::future::pending::<()>().await;
        unreachable!("the timeout fires first");
    };
    if let Err(reason) = post_load(addr, spec).await {
        return WakeHealth::Unhealthy { reason };
    }
    poll_loaded(addr, spec).await
}

/// POST the load call: one attempt, reported honestly either way.
async fn post_load(addr: &str, spec: &WakeSpec) -> Result<(), String> {
    let body = format!(
        "{{\"model\":{}}}",
        serde_json::to_string(&spec.model_id)
            .map_err(|err| format!("the model id could not be encoded: {err}"))?
    );
    let head = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        spec.load_path,
        addr,
        body.len(),
        body
    );
    let mut backend = tokio::time::timeout(DIAL_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| format!("the backend at {addr} could not be reached"))?
        .map_err(|_| format!("the backend at {addr} could not be reached"))?;
    backend
        .write_all(head.as_bytes())
        .await
        .map_err(|_| format!("the backend at {addr} closed the connection"))?;
    backend
        .flush()
        .await
        .map_err(|_| format!("the backend at {addr} closed the connection"))?;
    let (response, _) = read_response_head(&mut backend)
        .await
        .map_err(|_| format!("the backend at {addr} answered nothing usable"))?;
    if (200..300).contains(&response.status) {
        Ok(())
    } else {
        Err(format!("the load call answered {}", response.status))
    }
}

/// Poll the listing until the model id carries the loaded status.
///
/// Transport failures keep polling until the caller's deadline: a backend that
/// is still starting answers nothing for a while. A listing that parses but
/// names another outcome is also waited out. Only a listing that cannot be a
/// router listing at all is an immediate failure.
async fn poll_loaded(addr: &str, spec: &WakeSpec) -> WakeHealth {
    loop {
        tokio::time::sleep(WAKE_POLL_INTERVAL).await;
        let mut backend = match tokio::time::timeout(DIAL_TIMEOUT, TcpStream::connect(addr)).await {
            Ok(Ok(stream)) => stream,
            _ => continue,
        };
        let head = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            spec.health_path, addr
        );
        if backend.write_all(head.as_bytes()).await.is_err() {
            continue;
        }
        let Ok((response, buffered)) = read_response_head(&mut backend).await else {
            continue;
        };
        if !(200..300).contains(&response.status) {
            continue;
        }
        let body = match read_body_exact(&mut backend, &response, buffered).await {
            Ok(body) => body,
            Err(_) => continue,
        };
        match model_status(&body, &spec.model_id) {
            ModelStatus::Loaded => return WakeHealth::Healthy,
            ModelStatus::Unparseable => {
                return WakeHealth::Unhealthy {
                    reason: "the health listing is not a router listing".to_string(),
                };
            }
            ModelStatus::Other => continue,
        }
    }
}

/// What the listing says about one model id.
enum ModelStatus {
    Loaded,
    Other,
    Unparseable,
}

/// Read the structured id-plus-status listing: the exact model id carrying
/// the loaded status, never a substring match across entries.
fn model_status(body: &[u8], model_id: &str) -> ModelStatus {
    let Ok(listing) = serde_json::from_slice::<serde_json::Value>(body) else {
        return ModelStatus::Unparseable;
    };
    let Some(entries) = listing.get("data").and_then(|data| data.as_array()) else {
        return ModelStatus::Unparseable;
    };
    for entry in entries {
        if entry.get("id").and_then(|id| id.as_str()) != Some(model_id) {
            continue;
        }
        let status = entry
            .get("status")
            .and_then(|status| status.get("value"))
            .and_then(|value| value.as_str());
        match status {
            Some("loaded") => return ModelStatus::Loaded,
            Some(_) => return ModelStatus::Other,
            None => return ModelStatus::Unparseable,
        }
    }
    ModelStatus::Other
}

/// Read one response body by its framing, bounded for health listings.
async fn read_body_exact(
    backend: &mut TcpStream,
    head: &ResponseHead,
    mut buffered: Vec<u8>,
) -> Result<Vec<u8>, ()> {
    let mut body = buffered.split_off(head.body_offset);
    let rest = if head.chunked {
        return Err(());
    } else if let Some(length) = head.content_length {
        length.saturating_sub(body.len() as u64)
    } else {
        // Health listings always declare a length; anything else is not one.
        return Err(());
    };
    if body.len() as u64 + rest > MAX_HEALTH_BYTES {
        return Err(());
    }
    let mut chunk = [0u8; 8192];
    let mut remaining = rest;
    while remaining > 0 {
        let want = remaining.min(chunk.len() as u64) as usize;
        let read = backend.read(&mut chunk[..want]).await.map_err(|_| ())?;
        if read == 0 {
            return Err(());
        }
        body.extend_from_slice(&chunk[..read]);
        remaining -= read as u64;
    }
    Ok(body)
}

/// Read one response head from a backend, with a timeout.
async fn read_response_head(backend: &mut TcpStream) -> Result<(ResponseHead, Vec<u8>), ()> {
    let mut buffered = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let read = tokio::time::timeout(RESPONSE_HEAD_TIMEOUT, backend.read(&mut chunk))
            .await
            .map_err(|_| ())?
            .map_err(|_| ())?;
        if read == 0 {
            return Err(());
        }
        buffered.extend_from_slice(&chunk[..read]);
        if buffered.len() > MAX_HEAD_BYTES + 4096 {
            return Err(());
        }
        match parse_response_head(&buffered) {
            Ok(head) => return Ok((head, buffered)),
            Err(ParseError::Incomplete) => continue,
            Err(_) => return Err(()),
        }
    }
}

/// Which side of a relay failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Client,
    Backend,
}

/// How a relay ended without a complete response.
#[derive(Debug, PartialEq, Eq)]
enum RelayError {
    /// An I/O failure on one side.
    Failed(Side),
    /// A non-streamed body exceeded the cap.
    TooLarge,
}

/// How a body is framed on the wire.
#[derive(Debug, Clone, Copy)]
enum Framing {
    /// Exactly this many bytes follow.
    Length(u64),
    /// Chunk framing; the terminator ends the body.
    Chunked,
    /// The body ends when the stream closes.
    UntilClose,
}

/// Relay one body from `src` to `dst`, returning the bytes moved.
///
/// Chunk framing is forwarded raw, extensions and trailers included, so the
/// semantics survive even though the gateway never interprets the content.
/// `cap` bounds non-streamed traffic; `None` relays until the stream closes.
/// `timeout` bounds each idle read; `None` waits as long as a stream needs.
async fn relay(
    src: &mut TcpStream,
    dst: &mut TcpStream,
    src_side: Side,
    framing: Framing,
    already: Vec<u8>,
    cap: Option<u64>,
    timeout: Option<Duration>,
) -> Result<u64, RelayError> {
    let mut total = already.len() as u64;
    if let Some(cap) = cap {
        if total > cap {
            return Err(RelayError::TooLarge);
        }
    }
    dst.write_all(&already)
        .await
        .map_err(|_| RelayError::Failed(dst_side(src_side)))?;
    let mut chunk = [0u8; 8192];
    match framing {
        Framing::Length(length) => {
            let mut remaining = length.saturating_sub(already.len() as u64);
            while remaining > 0 {
                let want = remaining.min(chunk.len() as u64) as usize;
                let read = read_some(src, &mut chunk[..want], timeout)
                    .await
                    .map_err(|_| RelayError::Failed(src_side))?;
                if read == 0 {
                    return Err(RelayError::Failed(src_side));
                }
                dst.write_all(&chunk[..read])
                    .await
                    .map_err(|_| RelayError::Failed(dst_side(src_side)))?;
                remaining -= read as u64;
            }
        }
        Framing::Chunked => {
            // The already-buffered bytes may hold a whole framing; feed them
            // through the chunk parser before reading more.
            let mut pending: Vec<u8> = Vec::new();
            let mut first = true;
            loop {
                if first {
                    first = false;
                } else {
                    let read = read_some(src, &mut chunk, timeout)
                        .await
                        .map_err(|_| RelayError::Failed(src_side))?;
                    if read == 0 {
                        return Err(RelayError::Failed(src_side));
                    }
                    pending.extend_from_slice(&chunk[..read]);
                }
                match take_chunk(&mut pending, &mut total, cap).map_err(|err| match err {
                    TakeError::Malformed => RelayError::Failed(src_side),
                    TakeError::TooLarge => RelayError::TooLarge,
                })? {
                    ChunkTake::NeedMore => continue,
                    ChunkTake::Forwarded(bytes) => {
                        dst.write_all(&bytes)
                            .await
                            .map_err(|_| RelayError::Failed(dst_side(src_side)))?;
                    }
                    ChunkTake::Done(bytes) => {
                        dst.write_all(&bytes)
                            .await
                            .map_err(|_| RelayError::Failed(dst_side(src_side)))?;
                        break;
                    }
                }
            }
        }
        Framing::UntilClose => loop {
            let read = read_some(src, &mut chunk, timeout)
                .await
                .map_err(|_| RelayError::Failed(src_side))?;
            if read == 0 {
                break;
            }
            total += read as u64;
            if let Some(cap) = cap {
                if total > cap {
                    return Err(RelayError::TooLarge);
                }
            }
            dst.write_all(&chunk[..read])
                .await
                .map_err(|_| RelayError::Failed(dst_side(src_side)))?;
        },
    }
    dst.flush()
        .await
        .map_err(|_| RelayError::Failed(dst_side(src_side)))?;
    Ok(total)
}

/// The other side of a relay.
fn dst_side(src_side: Side) -> Side {
    match src_side {
        Side::Client => Side::Backend,
        Side::Backend => Side::Client,
    }
}

async fn read_some(
    stream: &mut TcpStream,
    buf: &mut [u8],
    timeout: Option<Duration>,
) -> std::io::Result<usize> {
    match timeout {
        Some(limit) => tokio::time::timeout(limit, stream.read(buf))
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "idle read timed out")
            })?,
        None => stream.read(buf).await,
    }
}

/// Resolve when the client goes away: EOF, an error, or pipelined bytes the
/// one-request-per-connection server does not serve.
async fn wait_client_close(client: &mut TcpStream) {
    let mut probe = [0u8; 1];
    loop {
        match client.readable().await {
            Ok(()) => match client.try_read(&mut probe) {
                // EOF, or bytes beyond the single request: gone either way.
                Ok(_) => return,
                Err(ref err) if err.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(_) => return,
            },
            Err(_) => return,
        }
    }
}

/// What the chunk parser did with its input.
enum ChunkTake {
    /// The framing is incomplete; read more before forwarding anything.
    NeedMore,
    /// Forward these raw bytes; the terminator was not among them.
    Forwarded(Vec<u8>),
    /// Forward these raw bytes; they hold the terminating zero chunk and its
    /// trailers, so the body is over afterward.
    Done(Vec<u8>),
}

/// Why the chunk parser stopped.
enum TakeError {
    /// The framing was not valid chunk syntax.
    Malformed,
    /// A non-streamed body exceeded the cap.
    TooLarge,
}

/// Consume one step of chunk framing from `pending`, accounting `total`.
fn take_chunk(
    pending: &mut Vec<u8>,
    total: &mut u64,
    cap: Option<u64>,
) -> Result<ChunkTake, TakeError> {
    // A chunk-size line ends at the first CRLF.
    let Some(line_end) = find_crlf(pending) else {
        return Ok(ChunkTake::NeedMore);
    };
    let line = String::from_utf8_lossy(&pending[..line_end]).to_string();
    let size_text = line.split(';').next().unwrap_or("").trim();
    let size = u64::from_str_radix(size_text, 16).map_err(|_| TakeError::Malformed)?;
    if size == 0 {
        // The zero chunk is followed by trailers and a blank line. Forward
        // everything through the blank line, then the body is over.
        let rest = &pending[line_end + 2..];
        let Some(trailers_end) = find_double_crlf(rest) else {
            return Ok(ChunkTake::NeedMore);
        };
        let end = line_end + 2 + trailers_end + 4;
        let bytes: Vec<u8> = pending.drain(..end).collect();
        *total += bytes.len() as u64;
        if let Some(cap) = cap {
            if *total > cap {
                return Err(TakeError::TooLarge);
            }
        }
        return Ok(ChunkTake::Done(bytes));
    }
    // A data chunk is size bytes plus its trailing CRLF.
    let data_end = line_end + 2 + size as usize + 2;
    if pending.len() < data_end {
        return Ok(ChunkTake::NeedMore);
    }
    if pending[line_end + 2 + size as usize..data_end] != *b"\r\n" {
        return Err(TakeError::Malformed);
    }
    let bytes: Vec<u8> = pending.drain(..data_end).collect();
    *total += bytes.len() as u64;
    if let Some(cap) = cap {
        if *total > cap {
            return Err(TakeError::TooLarge);
        }
    }
    Ok(ChunkTake::Forwarded(bytes))
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|window| window == b"\r\n")
}

fn find_double_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|window| window == b"\r\n\r\n")
}

/// Proxy one admitted request to its backend and relay the response.
#[allow(clippy::too_many_arguments)]
async fn proxy(
    state: &Arc<Inner>,
    scope: &str,
    client_label: &ClientId,
    addr: &str,
    head: &RequestHead,
    head_bytes: Vec<u8>,
    buffered_body: Vec<u8>,
    mut client: TcpStream,
    admission: Admission,
) {
    let now = state.started.elapsed().as_millis() as u64;
    let lease = admission.lease();
    // Leases admitted through this server start running immediately; only a
    // queued lease needs promoting to running. A lease that vanished between
    // routing and dispatch is genuinely stale.
    {
        let mut gateway = state.gateway.lock().await;
        let Some(backend) = gateway.backend_mut(&workload(scope)) else {
            let _ = write_error(&mut client, 502, "the backend went away").await;
            return;
        };
        let queued = backend
            .leases()
            .get(lease)
            .map(|held| held.kind == RequestKind::Queued)
            .unwrap_or(false);
        if queued && !backend.dispatch(lease, now) {
            let _ = write_error(&mut client, 502, "the admission was no longer current").await;
            return;
        }
        if backend.leases().get(lease).is_none() {
            let _ = write_error(&mut client, 502, "the admission was no longer current").await;
            return;
        }
    }
    let mut backend = match tokio::time::timeout(DIAL_TIMEOUT, TcpStream::connect(addr)).await {
        Ok(Ok(stream)) => stream,
        _ => {
            cancel(state, scope, lease, now + 1).await;
            let _ = write_error(&mut client, 502, "the backend could not be reached").await;
            return;
        }
    };
    // The head goes verbatim; only the routing headers were read, and the body
    // that follows is never inspected.
    if backend.write_all(&head_bytes).await.is_err() {
        cancel(state, scope, lease, now + 1).await;
        let _ = write_error(&mut client, 502, "the backend closed the connection").await;
        return;
    }
    let request_framing = if head.chunked {
        Framing::Chunked
    } else if let Some(length) = head.content_length {
        Framing::Length(length)
    } else {
        Framing::Length(0)
    };
    match relay(
        &mut client,
        &mut backend,
        Side::Client,
        request_framing,
        buffered_body,
        Some(MAX_BODY_BYTES),
        Some(HEAD_TIMEOUT),
    )
    .await
    {
        Ok(_) => {}
        // The request never reached the backend whole. A client that went away
        // releases every lease it holds so a retry actually runs; a backend
        // failure ends this lease and tells the waiting client plainly.
        Err(RelayError::Failed(Side::Client)) => {
            cancel_client(state, client_label, now + 1).await;
            return;
        }
        Err(RelayError::TooLarge) => {
            cancel(state, scope, lease, now + 1).await;
            let _ = write_error(
                &mut client,
                413,
                "the request body exceeds the gateway limit",
            )
            .await;
            return;
        }
        Err(_) => {
            cancel(state, scope, lease, now + 1).await;
            let _ = write_error(&mut client, 502, "the backend closed the connection").await;
            return;
        }
    }
    // Read the backend's head and relay it verbatim. A client that goes away
    // while the backend thinks releases every lease it holds right away,
    // rather than waiting out the response timeout.
    let (response_head, response_buffered) = tokio::select! {
        head = read_response_head(&mut backend) => match head {
            Ok(found) => found,
            Err(_) => {
                cancel(state, scope, lease, now + 1).await;
                let _ = write_error(&mut client, 502, "the backend answered nothing usable").await;
                return;
            }
        },
        _ = wait_client_close(&mut client) => {
            cancel_client(state, client_label, now + 1).await;
            return;
        }
    };
    if client
        .write_all(&response_buffered[..response_head.body_offset])
        .await
        .is_err()
    {
        cancel_client(state, client_label, now + 1).await;
        return;
    }
    // A stream relays until either side closes; anything else is capped.
    let streaming = head.wants_stream();
    let cap = (!streaming).then_some(MAX_BODY_BYTES);
    let timeout = if streaming {
        None
    } else {
        Some(RESPONSE_HEAD_TIMEOUT)
    };
    let response_body = response_buffered[response_head.body_offset..].to_vec();
    let response_framing = if response_head.chunked {
        Framing::Chunked
    } else if let Some(length) = response_head.content_length {
        Framing::Length(length)
    } else {
        Framing::UntilClose
    };
    // Backend failures after dispatch end the lease without remembering it, so
    // a retry actually runs; only a fully relayed response finishes it. A
    // client that went away releases every lease it holds, per the core's
    // disconnect rule.
    let now = state.started.elapsed().as_millis() as u64;
    match relay(
        &mut backend,
        &mut client,
        Side::Backend,
        response_framing,
        response_body,
        cap,
        timeout,
    )
    .await
    {
        Ok(_) => {
            finish(state, scope, lease, now).await;
        }
        Err(RelayError::Failed(Side::Client)) => {
            cancel_client(state, client_label, now).await;
        }
        Err(_) => {
            cancel(state, scope, lease, now).await;
        }
    }
}

async fn cancel(state: &Arc<Inner>, scope: &str, lease: LeaseId, now: u64) {
    if let Some(backend) = state.gateway.lock().await.backend_mut(&workload(scope)) {
        backend.cancel(lease, now);
    }
}

async fn cancel_client(state: &Arc<Inner>, client: &ClientId, now: u64) {
    let scopes: Vec<String> = state
        .routes
        .iter()
        .map(|route| route.workload_scope.clone())
        .collect();
    let mut gateway = state.gateway.lock().await;
    for scope in scopes {
        if let Some(backend) = gateway.backend_mut(&workload(&scope)) {
            backend.cancel_client(client, now);
        }
    }
}

async fn finish(state: &Arc<Inner>, scope: &str, lease: LeaseId, now: u64) {
    if let Some(backend) = state.gateway.lock().await.backend_mut(&workload(scope)) {
        backend.finish(lease, now);
    }
}

fn retry_after_secs(retry_after_ms: u64) -> u64 {
    retry_after_ms.div_ceil(1000).max(1)
}

async fn write_error(client: &mut TcpStream, status: u16, reason: &str) -> std::io::Result<()> {
    let body = format!("{reason}\n");
    let response = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        reason_phrase(status),
        body.len()
    );
    client.write_all(response.as_bytes()).await?;
    client.flush().await
}

async fn write_retry(
    client: &mut TcpStream,
    retry_after_ms: u64,
    reason: &str,
) -> std::io::Result<()> {
    let body = format!("{reason}\n");
    let response = format!(
        "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nRetry-After: {}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
        retry_after_secs(retry_after_ms)
    );
    client.write_all(response.as_bytes()).await?;
    client.flush().await
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        400 => "Bad Request",
        404 => "Not Found",
        408 => "Request Timeout",
        413 => "Content Too Large",
        502 => "Bad Gateway",
        _ => "Error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serving::lifecycle::{DrainOutcome, SleepDecision};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::AsyncWriteExt;

    const SCOPE: &str = "unit:llama.service";

    fn route_to(addr: &str, tracking: TrackingCompleteness) -> Route {
        Route {
            path_prefix: "/v1".to_string(),
            workload_scope: SCOPE.to_string(),
            backend_addr: addr.to_string(),
            tracking,
            wake: None,
        }
    }

    /// A fake backend: reads one head plus its content-length body, records
    /// what it saw, and answers with the fixed bytes.
    async fn start_backend(
        answer: Vec<u8>,
    ) -> (String, Arc<AtomicUsize>, Arc<Mutex<Vec<(String, Vec<u8>)>>>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port");
        let addr = listener.local_addr().expect("an address").to_string();
        let hits = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let thread_hits = Arc::clone(&hits);
        let thread_seen = Arc::clone(&seen);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let hits = Arc::clone(&thread_hits);
                let seen = Arc::clone(&thread_seen);
                let answer = answer.clone();
                tokio::spawn(async move {
                    hits.fetch_add(1, Ordering::Relaxed);
                    let mut buffered = Vec::new();
                    let mut chunk = [0u8; 4096];
                    let head = loop {
                        let Ok(read) = stream.read(&mut chunk).await else {
                            return;
                        };
                        if read == 0 {
                            return;
                        }
                        buffered.extend_from_slice(&chunk[..read]);
                        match parse_head(&buffered) {
                            Ok(head) => break head,
                            Err(ParseError::Incomplete) => continue,
                            Err(_) => return,
                        }
                    };
                    let length = head.content_length.unwrap_or(0).min(1024 * 1024) as usize;
                    while buffered.len() < head.body_offset + length {
                        let Ok(read) = stream.read(&mut chunk).await else {
                            return;
                        };
                        if read == 0 {
                            break;
                        }
                        buffered.extend_from_slice(&chunk[..read]);
                    }
                    seen.lock().await.push((
                        String::from_utf8_lossy(&buffered[..head.body_offset]).to_string(),
                        buffered[head.body_offset..].to_vec(),
                    ));
                    let _ = stream.write_all(&answer).await;
                });
            }
        });
        (addr, hits, seen)
    }

    /// A fake backend that reads the head and then never answers.
    async fn start_hanging_backend() -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port");
        let addr = listener.local_addr().expect("an address").to_string();
        let hits = Arc::new(AtomicUsize::new(0));
        let thread_hits = Arc::clone(&hits);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                thread_hits.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    let mut buffered = Vec::new();
                    let mut chunk = [0u8; 4096];
                    loop {
                        let Ok(read) = stream.read(&mut chunk).await else {
                            return;
                        };
                        if read == 0 {
                            return;
                        }
                        buffered.extend_from_slice(&chunk[..read]);
                        if parse_head(&buffered).is_ok() {
                            break;
                        }
                    }
                    // Hold the connection open without answering, so a client
                    // disconnect is the only thing that ends the lease.
                    tokio::time::sleep(Duration::from_secs(60)).await;
                });
            }
        });
        (addr, hits)
    }

    async fn start_gateway(config: ServerConfig) -> (String, Arc<GatewayServer>) {
        let server = Arc::new(GatewayServer::new(config, 0));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port");
        let addr = listener.local_addr().expect("an address").to_string();
        let serving = Arc::clone(&server);
        tokio::spawn(async move {
            serving.serve(listener).await;
        });
        (addr, server)
    }

    /// One request over a fresh connection, read until the server closes.
    async fn round_trip(addr: &str, request: &[u8]) -> Vec<u8> {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream.write_all(request).await.expect("write");
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.expect("read");
        out
    }

    fn status_line(response: &[u8]) -> String {
        let text = String::from_utf8_lossy(response).to_string();
        text.lines().next().unwrap_or_default().to_string()
    }

    fn header(response: &[u8], name: &str) -> Option<String> {
        let text = String::from_utf8_lossy(response).to_string();
        let head = text.split("\r\n\r\n").next().unwrap_or_default();
        head.lines().skip(1).find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.trim()
                .eq_ignore_ascii_case(name)
                .then(|| value.trim().to_string())
        })
    }

    #[tokio::test]
    async fn a_proxied_request_returns_the_backend_bytes() {
        let answer = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 17\r\nConnection: close\r\n\r\n{\"ok\":true}\n\n"
            .to_vec();
        let (backend, hits, seen) = start_backend(answer.clone()).await;
        let config = ServerConfig {
            routes: vec![route_to(&backend, TrackingCompleteness::Complete)],
        };
        let (gateway, _server) = start_gateway(config).await;

        let request = b"POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nContent-Length: 7\r\nIdempotency-Key: k1\r\nConnection: close\r\n\r\n{\"a\":1}";
        let via_gateway = round_trip(&gateway, request).await;
        let direct = round_trip(&backend, request).await;
        assert_eq!(
            via_gateway, direct,
            "pointing at the gateway produces the same bytes as pointing at the backend"
        );
        assert_eq!(hits.load(Ordering::Relaxed), 2);
        let seen = seen.lock().await;
        assert_eq!(seen.len(), 2);
        assert!(seen[0].0.contains("POST /v1/chat/completions HTTP/1.1"));
        assert!(seen[0].0.contains("Idempotency-Key: k1"));
        assert_eq!(seen[0].1, b"{\"a\":1}");
    }

    #[tokio::test]
    async fn a_stream_relays_until_the_backend_closes() {
        let answer = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\ndata: one\n\ndata: two\n\n"
            .to_vec();
        let (backend, _, _) = start_backend(answer.clone()).await;
        let config = ServerConfig {
            routes: vec![route_to(&backend, TrackingCompleteness::Complete)],
        };
        let (gateway, _) = start_gateway(config).await;

        let request = b"GET /v1/models HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\nConnection: close\r\n\r\n";
        let via_gateway = round_trip(&gateway, request).await;
        assert_eq!(
            via_gateway, answer,
            "a stream relays verbatim until the backend closes"
        );
    }

    #[tokio::test]
    async fn a_chunked_response_is_relayed_verbatim() {
        let answer = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nhello\r\n0\r\nX-Trailer: yes\r\n\r\n"
            .to_vec();
        let (backend, _, _) = start_backend(answer.clone()).await;
        let config = ServerConfig {
            routes: vec![route_to(&backend, TrackingCompleteness::Complete)],
        };
        let (gateway, _) = start_gateway(config).await;

        let request = b"POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\nConnection: close\r\n\r\nhi";
        let via_gateway = round_trip(&gateway, request).await;
        assert_eq!(via_gateway, answer, "chunk framing survives the proxy");
    }

    #[tokio::test]
    async fn a_disconnect_before_the_response_releases_the_lease() {
        let (backend, hits) = start_hanging_backend().await;
        let config = ServerConfig {
            routes: vec![route_to(&backend, TrackingCompleteness::Complete)],
        };
        let (gateway, server) = start_gateway(config).await;

        let mut stream = TcpStream::connect(&gateway).await.expect("connect");
        stream
            .write_all(b"GET /v1/models HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .expect("write");
        // Wait until the lease is in flight, so the disconnect ends something.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if matches!(server.evidence(SCOPE).await, ActivityEvidence::Active) {
                break;
            }
            assert!(Instant::now() < deadline, "the lease never went in flight");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drop(stream);
        // The disconnect releases the lease promptly, not at the 60s backend
        // timeout: the backend becomes idle again.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if !matches!(server.evidence(SCOPE).await, ActivityEvidence::Active) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the lease was still held 10s after the disconnect"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(hits.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn a_sleeping_backend_answers_retry_after_without_a_dispatch() {
        let (backend, hits, _) = start_backend(
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
        )
        .await;
        let mut asleep = Backend::new(
            workload(SCOPE),
            TrackingCompleteness::Complete,
            ServingPolicy::default(),
            0,
        );
        assert!(matches!(
            asleep.begin_drain(1_000_000),
            SleepDecision::Allowed { .. }
        ));
        assert!(matches!(
            asleep.drain_outcome(1_000_000),
            DrainOutcome::Ready { .. }
        ));
        asleep.sleep_confirmed(1_000_000).expect("sleep");
        let config = ServerConfig {
            routes: vec![route_to(&backend, TrackingCompleteness::Complete)],
        };
        let server = Arc::new(GatewayServer::with_backends(config, vec![asleep]));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port");
        let addr = listener.local_addr().expect("an address").to_string();
        let serving = Arc::clone(&server);
        tokio::spawn(async move {
            serving.serve(listener).await;
        });

        let response = round_trip(
            &addr,
            b"GET /v1/models HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(
            status_line(&response).contains("503"),
            "a sleeping backend refuses with a retry hint: {}",
            status_line(&response)
        );
        let retry_after: u64 = header(&response, "retry-after")
            .expect("a retry hint")
            .parse()
            .expect("seconds");
        assert!(retry_after > 0, "the client is told when to come back");
        assert_eq!(
            hits.load(Ordering::Relaxed),
            0,
            "nothing was dispatched to the sleeping backend"
        );
    }

    #[tokio::test]
    async fn an_untracked_backend_is_refused() {
        let (backend, hits, _) = start_backend(
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
        )
        .await;
        let config = ServerConfig {
            routes: vec![route_to(&backend, TrackingCompleteness::Unknown)],
        };
        let (gateway, _) = start_gateway(config).await;

        let response = round_trip(
            &gateway,
            b"GET /v1/models HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(
            status_line(&response).contains("502"),
            "an untracked backend is refused: {}",
            status_line(&response)
        );
        assert_eq!(hits.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn an_unknown_path_is_a_404_and_a_bad_head_is_a_400() {
        let (backend, hits, _) = start_backend(
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
        )
        .await;
        let config = ServerConfig {
            routes: vec![route_to(&backend, TrackingCompleteness::Complete)],
        };
        let (gateway, _) = start_gateway(config).await;

        let response = round_trip(
            &gateway,
            b"GET /elsewhere HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(
            status_line(&response).contains("404"),
            "{}",
            status_line(&response)
        );

        let response = round_trip(&gateway, b"GET\r\n\r\n").await;
        assert!(
            status_line(&response).contains("400"),
            "{}",
            status_line(&response)
        );

        let big = format!(
            "POST /v1/x HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            MAX_BODY_BYTES + 1
        );
        let response = round_trip(&gateway, big.as_bytes()).await;
        assert!(
            status_line(&response).contains("413"),
            "{}",
            status_line(&response)
        );
        assert_eq!(hits.load(Ordering::Relaxed), 0, "refusals never dispatch");
    }

    #[test]
    fn routes_match_the_longest_prefix() {
        let routes = vec![
            Route {
                path_prefix: "/v1".to_string(),
                workload_scope: "a".to_string(),
                backend_addr: "127.0.0.1:1".to_string(),
                tracking: TrackingCompleteness::Complete,
                wake: None,
            },
            Route {
                path_prefix: "/v1/special".to_string(),
                workload_scope: "b".to_string(),
                backend_addr: "127.0.0.1:2".to_string(),
                tracking: TrackingCompleteness::Complete,
                wake: None,
            },
        ];
        assert_eq!(
            match_route(&routes, "/v1/models").unwrap().workload_scope,
            "a"
        );
        assert_eq!(
            match_route(&routes, "/v1/special/models")
                .unwrap()
                .workload_scope,
            "b"
        );
        assert!(match_route(&routes, "/other").is_none());
    }

    #[test]
    fn retry_after_rounds_up_to_whole_seconds() {
        assert_eq!(retry_after_secs(1), 1);
        assert_eq!(retry_after_secs(1000), 1);
        assert_eq!(retry_after_secs(1001), 2);
    }
}

#[cfg(test)]
mod wake_tests {
    use super::*;
    use crate::serving::lifecycle::{DrainOutcome, SleepDecision};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::AsyncWriteExt;

    const SCOPE: &str = "unit:llama.service";

    /// A router-shaped fake: unloaded until asked to load, then loaded
    /// unless constructed to never flip.
    struct RouterState {
        loaded: Mutex<bool>,
        loads: AtomicUsize,
        flip: bool,
    }

    fn flip_state() -> Arc<RouterState> {
        Arc::new(RouterState {
            loaded: Mutex::new(false),
            loads: AtomicUsize::new(0),
            flip: true,
        })
    }

    fn stuck_state() -> Arc<RouterState> {
        Arc::new(RouterState {
            loaded: Mutex::new(false),
            loads: AtomicUsize::new(0),
            flip: false,
        })
    }

    async fn start_router(state: Arc<RouterState>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port");
        let addr = listener.local_addr().expect("an address").to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    let mut buffered = Vec::new();
                    let mut chunk = [0u8; 4096];
                    let head = loop {
                        let Ok(read) = stream.read(&mut chunk).await else {
                            return;
                        };
                        if read == 0 {
                            return;
                        }
                        buffered.extend_from_slice(&chunk[..read]);
                        match parse_head(&buffered) {
                            Ok(head) => break head,
                            Err(ParseError::Incomplete) => continue,
                            Err(_) => return,
                        }
                    };
                    // Read the declared body, if any.
                    let length = head.content_length.unwrap_or(0).min(1024 * 1024) as usize;
                    while buffered.len() < head.body_offset + length {
                        let Ok(read) = stream.read(&mut chunk).await else {
                            return;
                        };
                        if read == 0 {
                            break;
                        }
                        buffered.extend_from_slice(&chunk[..read]);
                    }
                    let body = String::from_utf8_lossy(&buffered[head.body_offset..]).to_string();
                    let answer = if head.method == "POST" && head.target == "/models/load" {
                        state.loads.fetch_add(1, Ordering::Relaxed);
                        *state.loaded.lock().await = body.contains("\"qwen3-8b\"") && state.flip;
                        "{\"success\":true}".to_string()
                    } else if head.method == "GET" && head.target == "/models" {
                        let loaded = *state.loaded.lock().await;
                        format!(
                            "{{\"data\":[{{\"id\":\"qwen3-8b\",\"status\":{{\"value\":\"{}\"}}}}]}}",
                            if loaded { "loaded" } else { "unloaded" }
                        )
                    } else {
                        "{\"ok\":true}".to_string()
                    };
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        answer.len(),
                        answer
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        addr
    }

    fn asleep_with_policy(policy: ServingPolicy) -> Backend {
        let workload = WorkloadId {
            scope: SCOPE.to_string(),
            generation: 1,
        };
        let mut backend = Backend::new(workload, TrackingCompleteness::Complete, policy, 0);
        assert!(matches!(
            backend.begin_drain(1_000_000),
            SleepDecision::Allowed { .. }
        ));
        assert!(matches!(
            backend.drain_outcome(1_000_000),
            DrainOutcome::Ready { .. }
        ));
        backend.sleep_confirmed(1_000_000).expect("sleep");
        backend
    }

    fn short_policy(wake_timeout_ms: u64, max_wake_attempts: u32) -> ServingPolicy {
        ServingPolicy {
            wake_timeout_ms,
            max_wake_attempts,
            ..ServingPolicy::default()
        }
    }

    async fn start_gateway_with(
        addr: &str,
        backends: Vec<Backend>,
        wake: Option<WakeSpec>,
    ) -> (String, Arc<GatewayServer>) {
        let config = ServerConfig {
            routes: vec![Route {
                path_prefix: "/v1".to_string(),
                workload_scope: SCOPE.to_string(),
                backend_addr: addr.to_string(),
                tracking: TrackingCompleteness::Complete,
                wake,
            }],
        };
        let server = Arc::new(GatewayServer::with_backends(config, backends));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port");
        let gateway = listener.local_addr().expect("an address").to_string();
        let serving = Arc::clone(&server);
        tokio::spawn(async move {
            serving.serve(listener).await;
        });
        (gateway, server)
    }

    fn wake_spec() -> WakeSpec {
        WakeSpec {
            load_path: "/models/load".to_string(),
            model_id: "qwen3-8b".to_string(),
            health_path: "/models".to_string(),
        }
    }

    async fn get(gateway: &str, target: &str) -> Vec<u8> {
        let mut stream = TcpStream::connect(gateway).await.expect("connect");
        let request = format!("GET {target} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await.expect("write");
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.expect("read");
        out
    }

    fn status_of(response: &[u8]) -> String {
        String::from_utf8_lossy(response)
            .lines()
            .next()
            .unwrap_or_default()
            .to_string()
    }

    #[tokio::test]
    async fn a_sleeping_backend_wakes_and_serves_on_retry() {
        let state = flip_state();
        let addr = start_router(Arc::clone(&state)).await;
        let backend = asleep_with_policy(ServingPolicy::default());
        let (gateway, server) = start_gateway_with(&addr, vec![backend], Some(wake_spec())).await;

        // The arrival begins the wake and answers the retry hint.
        let first = get(&gateway, "/v1/models").await;
        assert!(status_of(&first).contains("503"), "{}", status_of(&first));

        // The wake runs the provider load and verifies health: the next
        // arrival is served instead of refused.
        let deadline = Instant::now() + Duration::from_secs(20);
        let second = loop {
            let response = get(&gateway, "/v1/models").await;
            if status_of(&response).contains("200") {
                break response;
            }
            assert!(
                Instant::now() < deadline,
                "the backend never woke: {}",
                String::from_utf8_lossy(&response)
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        assert!(state.loads.load(Ordering::Relaxed) >= 1);
        assert!(
            String::from_utf8_lossy(&second).contains("\"ok\":true"),
            "the retry is proxied to the woken backend"
        );
        assert!(
            !matches!(server.evidence(SCOPE).await, ActivityEvidence::Active),
            "the completed wake leaves no lease behind"
        );
    }

    #[tokio::test]
    async fn a_wake_that_never_completes_returns_to_sleep() {
        // The load is performed but health never arrives: the wake must time
        // out rather than sit in waking forever.
        let state = stuck_state();
        let addr = start_router(Arc::clone(&state)).await;
        let backend = asleep_with_policy(short_policy(1000, 5));
        let (gateway, _) = start_gateway_with(&addr, vec![backend], Some(wake_spec())).await;

        let first = get(&gateway, "/v1/models").await;
        assert!(status_of(&first).contains("503"));

        // Past the wake timeout, a new arrival begins a new wake instead of
        // meeting a backend stuck waking: the reason names a begun wake.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let second = get(&gateway, "/v1/models").await;
        assert!(status_of(&second).contains("503"));
        assert!(
            String::from_utf8_lossy(&second).contains("begun on the request path"),
            "a fresh wake begins after the timeout: {}",
            String::from_utf8_lossy(&second)
        );
        assert!(
            state.loads.load(Ordering::Relaxed) >= 1,
            "the provider load was performed"
        );
    }

    #[tokio::test]
    async fn a_wake_without_a_spec_still_times_out() {
        // No wake is configured, so nothing is performed: the timeout alone
        // must return the backend to sleep rather than leaving it waking.
        let state = flip_state();
        let addr = start_router(Arc::clone(&state)).await;
        let backend = asleep_with_policy(short_policy(1000, 5));
        let (gateway, _) = start_gateway_with(&addr, vec![backend], None).await;

        let first = get(&gateway, "/v1/models").await;
        assert!(status_of(&first).contains("503"));

        tokio::time::sleep(Duration::from_millis(1500)).await;
        let second = get(&gateway, "/v1/models").await;
        assert!(status_of(&second).contains("503"));
        assert!(
            String::from_utf8_lossy(&second).contains("begun on the request path"),
            "a fresh wake begins after the timeout: {}",
            String::from_utf8_lossy(&second)
        );
        assert_eq!(
            state.loads.load(Ordering::Relaxed),
            0,
            "no provider call without a configured wake"
        );
    }

    #[tokio::test]
    async fn exhausted_wake_attempts_fail_open_with_a_reason() {
        let state = stuck_state();
        let addr = start_router(Arc::clone(&state)).await;
        let backend = asleep_with_policy(short_policy(500, 1));
        let (gateway, _) = start_gateway_with(&addr, vec![backend], None).await;

        assert!(status_of(&get(&gateway, "/v1/models").await).contains("503"));
        // One attempt is allowed: after its timeout the backend is failed,
        // and arrivals are refused rather than retried forever.
        tokio::time::sleep(Duration::from_millis(1200)).await;
        let refused = get(&gateway, "/v1/models").await;
        assert!(
            status_of(&refused).contains("502"),
            "an unwakeable backend is refused: {}",
            status_of(&refused)
        );
    }
}
