//! The daemon event loop, monitoring task, and connection handling.
//!
//! Two concerns are deliberately separated. A monitoring task collects a status
//! snapshot on its own interval and never blocks on a client. An accept loop
//! serves connections, and each connection is handled in its own task, so a
//! slow or stalled client can only affect itself. Optional integrations do not
//! exist yet, so an absent integration cannot stall anything.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use breadram_core::config::Config;
use breadram_core::inventory::collect_inventory;
use breadram_core::ipc::{
    self, decode, encode, ApiError, ClientMessage, ErrorCode, EventPayload, RequestBody,
    ResponseBody, ServerMessage, StatusPayload, MAX_MESSAGE_BYTES, PROTOCOL_MAJOR, PROTOCOL_MINOR,
};
use breadram_core::telemetry::{collect_host, Clock, RealFs, SystemClock};
use breadram_core::{ManagementMode, VERSION};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedWriteHalf;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, RwLock};

/// How many events a slow client may fall behind before it starts dropping.
const EVENT_CHANNEL_CAPACITY: usize = 16;

/// Why the daemon could not start or stopped unexpectedly.
#[derive(Debug, thiserror::Error)]
pub enum DaemonError {
    /// Another daemon is already using the socket.
    #[error("another breadramd is already listening on {0}")]
    AlreadyRunning(PathBuf),
    /// The socket could not be bound.
    #[error("cannot bind {path}: {source}")]
    Bind {
        /// The socket path.
        path: PathBuf,
        /// The underlying error.
        #[source]
        source: std::io::Error,
    },
    /// A generic I/O failure.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

struct SharedState {
    status: RwLock<StatusPayload>,
    events: broadcast::Sender<EventPayload>,
    interval_ms: u64,
    socket_uid: u32,
}

/// What a handler should do next.
enum Action {
    Continue,
    Close,
    Subscribed(broadcast::Receiver<EventPayload>),
}

/// Run the daemon until it receives SIGINT or SIGTERM.
///
/// The socket is bound owner only, a stale socket left by a crash is replaced,
/// and the socket file is removed on a clean shutdown.
pub async fn run(config: Config, socket: PathBuf) -> Result<(), DaemonError> {
    if socket.exists() {
        match UnixStream::connect(&socket).await {
            // A live daemon means we must not steal the socket.
            Ok(_) => return Err(DaemonError::AlreadyRunning(socket)),
            // Otherwise the file is stale, so replace it.
            Err(_) => {
                let _ = std::fs::remove_file(&socket);
            }
        }
    }
    if let Some(parent) = socket.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let listener = UnixListener::bind(&socket).map_err(|source| DaemonError::Bind {
        path: socket.clone(),
        source,
    })?;
    set_owner_only(&socket)?;
    let socket_uid = owner_uid(&socket)?;

    let interval_ms = config.timing.settle_ms.max(250);
    let interval = Duration::from_millis(interval_ms);
    let mode = config.mode;
    let clock = SystemClock::new();

    let (events, _receiver) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
    let state = Arc::new(SharedState {
        status: RwLock::new(collect_status(&clock, mode)),
        events,
        interval_ms,
        socket_uid,
    });

    spawn_monitor(Arc::clone(&state), interval, mode, clock);

    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                if let Ok((stream, _addr)) = accepted {
                    let state = Arc::clone(&state);
                    tokio::spawn(async move {
                        let _ = serve_connection(stream, state).await;
                    });
                }
            }
            _ = sigint.recv() => break,
            _ = sigterm.recv() => break,
        }
    }

    drop(listener);
    let _ = std::fs::remove_file(&socket);
    Ok(())
}

fn spawn_monitor(
    state: Arc<SharedState>,
    interval: Duration,
    mode: ManagementMode,
    clock: SystemClock,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            // The read is blocking, so keep it off the async runtime.
            let status =
                match tokio::task::spawn_blocking(move || collect_status(&clock, mode)).await {
                    Ok(status) => status,
                    Err(_) => continue,
                };
            *state.status.write().await = status.clone();
            let _ = state.events.send(EventPayload {
                kind: "status".to_string(),
                observed_at_ms: status.observed_at_ms,
                available_bytes: status.available_bytes,
            });
        }
    });
}

fn collect_status(clock: &SystemClock, mode: ManagementMode) -> StatusPayload {
    let fs = RealFs;
    let inventory = collect_inventory(&fs);
    match collect_host(&fs, clock) {
        Ok(snapshot) => StatusPayload {
            mode,
            version: VERSION.to_string(),
            observed_at_ms: snapshot.observed_at_ms,
            available_bytes: snapshot.meminfo.available,
            total_bytes: Some(snapshot.meminfo.total),
            workloads: inventory.workloads.len(),
            capabilities: snapshot.capabilities,
        },
        Err(_) => StatusPayload {
            mode,
            version: VERSION.to_string(),
            observed_at_ms: clock.monotonic_ms(),
            available_bytes: None,
            total_bytes: None,
            workloads: inventory.workloads.len(),
            capabilities: inventory.capabilities,
        },
    }
}

async fn serve_connection(stream: UnixStream, state: Arc<SharedState>) -> std::io::Result<()> {
    // Only the user who owns the socket may talk to it. The socket is already
    // owner only, so this is defence in depth against a replaced path.
    if let Ok(cred) = stream.peer_cred() {
        if cred.uid() != state.socket_uid {
            return Ok(());
        }
    }

    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    let mut line = String::new();
    let mut greeted = false;
    let mut subscription: Option<broadcast::Receiver<EventPayload>> = None;

    loop {
        line.clear();
        let mut next_subscription = None;

        let action = if let Some(receiver) = subscription.as_mut() {
            tokio::select! {
                read = reader.read_line(&mut line) => match read {
                    Ok(0) | Err(_) => Action::Close,
                    Ok(_) => handle_request(&line, &state, &mut greeted, &mut write).await?,
                },
                event = receiver.recv() => match event {
                    Ok(event) => {
                        send_message(&mut write, 0, ResponseBody::Event(event)).await?;
                        Action::Continue
                    }
                    // A slow client drops events rather than stalling the daemon.
                    Err(broadcast::error::RecvError::Lagged(_)) => Action::Continue,
                    Err(broadcast::error::RecvError::Closed) => Action::Close,
                },
            }
        } else {
            match reader.read_line(&mut line).await {
                Ok(0) | Err(_) => Action::Close,
                Ok(_) => handle_request(&line, &state, &mut greeted, &mut write).await?,
            }
        };

        match action {
            Action::Continue => {}
            Action::Close => break,
            Action::Subscribed(receiver) => next_subscription = Some(receiver),
        }
        if let Some(receiver) = next_subscription {
            subscription = Some(receiver);
        }
    }

    Ok(())
}

async fn handle_request(
    line: &str,
    state: &SharedState,
    greeted: &mut bool,
    write: &mut OwnedWriteHalf,
) -> std::io::Result<Action> {
    if line.trim().is_empty() {
        return Ok(Action::Continue);
    }
    if line.len() > MAX_MESSAGE_BYTES {
        send_message(
            write,
            0,
            ResponseBody::Error(ApiError::new(
                ErrorCode::BadRequest,
                format!("message exceeds {MAX_MESSAGE_BYTES} bytes"),
            )),
        )
        .await?;
        return Ok(Action::Close);
    }

    let message: ClientMessage = match decode(line) {
        Ok(message) => message,
        Err(err) => {
            send_message(
                write,
                0,
                ResponseBody::Error(ApiError::new(ErrorCode::BadRequest, err.to_string())),
            )
            .await?;
            return Ok(Action::Continue);
        }
    };

    if !*greeted {
        return match message.body {
            RequestBody::Hello(hello) => match ipc::check_protocol(hello.major, hello.minor) {
                Ok(()) => {
                    *greeted = true;
                    send_message(
                        write,
                        message.id,
                        ResponseBody::Hello {
                            major: PROTOCOL_MAJOR,
                            minor: PROTOCOL_MINOR,
                            server: VERSION.to_string(),
                        },
                    )
                    .await?;
                    Ok(Action::Continue)
                }
                Err(err) => {
                    send_message(
                        write,
                        message.id,
                        ResponseBody::Error(ApiError::new(
                            ErrorCode::IncompatibleVersion,
                            err.to_string(),
                        )),
                    )
                    .await?;
                    Ok(Action::Close)
                }
            },
            _ => {
                send_message(
                    write,
                    message.id,
                    ResponseBody::Error(ApiError::new(
                        ErrorCode::BadRequest,
                        "the first message must be a hello handshake",
                    )),
                )
                .await?;
                Ok(Action::Close)
            }
        };
    }

    match message.body {
        RequestBody::Hello(_) => {
            send_message(
                write,
                message.id,
                ResponseBody::Error(ApiError::new(ErrorCode::BadRequest, "already greeted")),
            )
            .await?;
            Ok(Action::Continue)
        }
        RequestBody::Status => {
            let status = state.status.read().await.clone();
            send_message(write, message.id, ResponseBody::Status(Box::new(status))).await?;
            Ok(Action::Continue)
        }
        RequestBody::Capabilities => {
            let capabilities = state.status.read().await.capabilities.clone();
            send_message(write, message.id, ResponseBody::Capabilities(capabilities)).await?;
            Ok(Action::Continue)
        }
        RequestBody::Subscribe => {
            let receiver = state.events.subscribe();
            send_message(
                write,
                message.id,
                ResponseBody::Subscribed {
                    interval_ms: state.interval_ms,
                },
            )
            .await?;
            Ok(Action::Subscribed(receiver))
        }
    }
}

async fn send_message(
    write: &mut OwnedWriteHalf,
    id: u64,
    body: ResponseBody,
) -> std::io::Result<()> {
    let line = match encode(&ServerMessage { id, body }) {
        Ok(line) => line,
        // Our own message should always encode; if it cannot, drop it rather
        // than tear down the connection.
        Err(_) => return Ok(()),
    };
    write.write_all(line.as_bytes()).await?;
    write.write_all(b"\n").await?;
    write.flush().await?;
    Ok(())
}

fn set_owner_only(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

fn owner_uid(path: &Path) -> std::io::Result<u32> {
    use std::os::unix::fs::MetadataExt;
    Ok(std::fs::metadata(path)?.uid())
}
