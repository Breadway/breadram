//! The daemon event loop, monitoring task, and connection handling.
//!
//! Two concerns are deliberately separated. A monitoring task collects a status
//! snapshot on its own interval and never blocks on a client. An accept loop
//! serves connections, and each connection is handled in its own task, so a
//! slow or stalled client can only affect itself. Optional integrations do not
//! exist yet, so an absent integration cannot stall anything.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use breadram_core::config::{validate, Config};
use breadram_core::control::discover_targets;
use breadram_core::coordinator::ActionJournal;
use breadram_core::inventory::{collect_inventory, MemoryAccounting, Workload};
use breadram_core::ipc::{
    self, control_level_name, decode, encode, pressure_state_name, ActionJournalRecord,
    ActionsPayload, ApiError, AppliedPayload, ClientMessage, DiagnosticsPayload, ErrorCode,
    EventPayload, JournalSummary, MemoryBreakdown, PlanPayload, PressureSummary, RequestBody,
    ResponseBody, RulesPayload, ServerMessage, StatusPayload, WorkloadSummary, WorkloadsPayload,
    MAX_MESSAGE_BYTES, PROTOCOL_MAJOR, PROTOCOL_MINOR,
};
use breadram_core::policy::{
    plan as plan_actions, Plan, PlanChannel, PlannerInput, PolicyHistory, PressureState,
    StateMachine, UnknownActivity,
};
use breadram_core::telemetry::snapshot::MemorySnapshot;
use breadram_core::telemetry::{collect_host, Clock, RealFs, SystemClock};
use breadram_core::{CapabilityReport, ManagementMode, VERSION};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedWriteHalf;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, RwLock};

/// How many events a slow client may fall behind before it starts dropping.
const EVENT_CHANNEL_CAPACITY: usize = 16;

/// The rate window and bound for the policy history.
///
/// The planner's rate limit reads these, so they describe the policy, not the
/// journal size.
const HISTORY_RATE_WINDOW_MS: u64 = 60_000;
/// The number of actions the planner allows inside the rate window.
const HISTORY_MAX_ACTIONS_PER_WINDOW: usize = 10;

/// The daemon observes and explains. It does not execute yet.
///
/// Said in one place so a report never implies that automatic action is running
/// when it is not.
const EXECUTION_NOTE: &str =
    "breadramd observes and explains only, so it has recorded no actions yet";

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
    published: RwLock<Published>,
    config: RwLock<Config>,
    config_version: AtomicU64,
    config_path: Option<PathBuf>,
    socket: PathBuf,
    journal: RwLock<ActionJournal>,
    events: broadcast::Sender<EventPayload>,
    interval_ms: u64,
    socket_uid: u32,
    clock: SystemClock,
}

/// The last complete observation.
///
/// One pass over procfs serves every client, so a status request, an explanation,
/// and a diagnostic bundle all describe the same instant. Nothing a client asks
/// for can trigger a collection of its own or block the monitor.
#[derive(Clone)]
struct Published {
    status: StatusPayload,
    snapshot: Option<MemorySnapshot>,
    workloads: Vec<Workload>,
    /// What the inventory could read, which is its own report: a snapshot
    /// capability says nothing about whether procfs could be walked.
    inventory_capabilities: CapabilityReport,
    control_targets: usize,
    pressure: PressureState,
    plan: Option<Plan>,
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
    run_with_config_path(config, socket, None).await
}

/// Run the daemon, remembering where its configuration came from.
///
/// The path is used for one thing only: writing a change back when a client
/// explicitly asks for it with `persist`. Without it, a change applies to the
/// running daemon and a restart reverts to the file.
pub async fn run_with_config_path(
    config: Config,
    socket: PathBuf,
    config_path: Option<PathBuf>,
) -> Result<(), DaemonError> {
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
    let clock = SystemClock::new();

    let mode = config.mode;

    // One observation before the socket is served, so the first client never
    // sees an empty daemon.
    let initial = {
        let config = config.clone();
        tokio::task::spawn_blocking(move || {
            let mut runtime = PolicyRuntime::new(&config, 0);
            observe(&clock, &config, &mut runtime.machine, &mut runtime.history)
        })
        .await
        .unwrap_or_else(|_| unavailable(mode, clock.monotonic_ms()))
    };

    let journal_capacity = config.history.max_records as usize;
    let (events, _receiver) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
    let state = Arc::new(SharedState {
        published: RwLock::new(initial),
        config: RwLock::new(config),
        config_version: AtomicU64::new(0),
        config_path,
        socket: socket.clone(),
        journal: RwLock::new(ActionJournal::new(journal_capacity)),
        events,
        interval_ms,
        socket_uid,
        clock,
    });

    spawn_monitor(Arc::clone(&state), interval);

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

/// The policy state that has to survive between monitor ticks.
///
/// It moves into the blocking worker and back on every tick, because hysteresis
/// depends on the machine keeping its history and the planner needs the same
/// record of past actions.
struct PolicyRuntime {
    machine: StateMachine,
    history: PolicyHistory,
    config_version: u64,
}

impl PolicyRuntime {
    fn new(config: &Config, config_version: u64) -> Self {
        Self {
            machine: StateMachine::new(config),
            history: policy_history(config),
            config_version,
        }
    }
}

fn spawn_monitor(state: Arc<SharedState>, interval: Duration) {
    tokio::spawn(async move {
        let clock = state.clock;
        let initial_config = state.config.read().await.clone();
        let mut runtime = Some(PolicyRuntime::new(
            &initial_config,
            state.config_version.load(Ordering::Relaxed),
        ));
        drop(initial_config);
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let current = state.config_version.load(Ordering::Relaxed);
            let config = state.config.read().await.clone();
            if state.config_version.load(Ordering::Relaxed) != current {
                // A configuration change landed while the configuration was
                // being read, so this pass may pair the wrong version with the
                // wrong settings. Drop it and pick the change up next tick.
                continue;
            }
            let Some(current_runtime) = runtime.take() else {
                break;
            };
            // The reads are blocking, so keep them off the async runtime. The
            // policy state moves into the task and back, so hysteresis keeps its
            // history across ticks.
            let rebuild_config = config.clone();
            let collected = tokio::task::spawn_blocking(move || {
                let mut current_runtime = current_runtime;
                if current_runtime.config_version != current {
                    // A configuration change restarts the machine, so a new
                    // threshold takes effect at once instead of after a dwell.
                    current_runtime = PolicyRuntime::new(&config, current);
                }
                let published = observe(
                    &clock,
                    &config,
                    &mut current_runtime.machine,
                    &mut current_runtime.history,
                );
                (published, current_runtime)
            })
            .await;
            let published = match collected {
                Ok((published, next_runtime)) => {
                    runtime = Some(next_runtime);
                    published
                }
                Err(err) => {
                    // A panicked worker must not stop the monitor. Rebuild the
                    // policy state and keep watching, saying plainly that it
                    // happened rather than silently resetting.
                    tracing::warn!(error = %err, "a monitor pass failed, rebuilding the policy state");
                    runtime = Some(PolicyRuntime::new(&rebuild_config, current));
                    continue;
                }
            };
            if state.config_version.load(Ordering::Relaxed) != current {
                // A change landed while this pass was reading the host. It
                // published an observation built from the newer settings, so
                // this one is dropped rather than overwriting it with the mode
                // the host had a moment ago.
                continue;
            }
            let status = published.status.clone();
            *state.published.write().await = published;
            let _ = state.events.send(EventPayload {
                kind: "status".to_string(),
                observed_at_ms: status.observed_at_ms,
                available_bytes: status.available_bytes,
            });
        }
    });
}

/// A history bounded by the configured retention.
fn policy_history(config: &Config) -> PolicyHistory {
    PolicyHistory::new(
        config.history.max_records as usize,
        HISTORY_RATE_WINDOW_MS,
        HISTORY_MAX_ACTIONS_PER_WINDOW,
    )
}

/// Collect one complete observation and run the policy over it.
fn observe(
    clock: &SystemClock,
    config: &Config,
    machine: &mut StateMachine,
    history: &mut PolicyHistory,
) -> Published {
    let fs = RealFs;
    let inventory = collect_inventory(&fs);
    let control_targets = discover_targets(&fs).len();
    let snapshot = collect_host(&fs, clock).ok();
    let pressure = match &snapshot {
        Some(snapshot) => machine.observe(snapshot),
        // Without meminfo there is nothing to reason about, and a guess would be
        // worse than an honest healthy reading.
        None => PressureState::Healthy,
    };
    let plan = snapshot.as_ref().map(|snapshot| {
        plan_actions(
            PlannerInput {
                state: pressure,
                snapshot,
                workloads: &inventory.workloads,
                config,
                history,
                // No adapter is registered yet, so no workload has proven
                // activity. Unknown is never idle.
                activity: &UnknownActivity,
            },
            // The plan describes what automatic mode would do. Nothing executes,
            // so it is never labelled as a plan that would run.
            PlanChannel::DryRun,
        )
    });
    let status = match &snapshot {
        Some(snapshot) => StatusPayload {
            mode: config.mode,
            version: VERSION.to_string(),
            observed_at_ms: snapshot.observed_at_ms,
            available_bytes: snapshot.meminfo.available,
            total_bytes: Some(snapshot.meminfo.total),
            workloads: inventory.workloads.len(),
            capabilities: snapshot.capabilities.clone(),
        },
        None => StatusPayload {
            mode: config.mode,
            version: VERSION.to_string(),
            observed_at_ms: clock.monotonic_ms(),
            available_bytes: None,
            total_bytes: None,
            workloads: inventory.workloads.len(),
            capabilities: inventory.capabilities.clone(),
        },
    };
    Published {
        status,
        snapshot,
        workloads: inventory.workloads,
        inventory_capabilities: inventory.capabilities,
        control_targets,
        pressure,
        plan,
    }
}

/// What the daemon knows before it can read anything.
fn unavailable(mode: ManagementMode, observed_at_ms: u64) -> Published {
    Published {
        status: StatusPayload {
            mode,
            version: VERSION.to_string(),
            observed_at_ms,
            available_bytes: None,
            total_bytes: None,
            workloads: 0,
            capabilities: breadram_core::CapabilityReport::new(),
        },
        snapshot: None,
        workloads: Vec::new(),
        inventory_capabilities: breadram_core::CapabilityReport::new(),
        control_targets: 0,
        pressure: PressureState::Healthy,
        plan: None,
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
            let status = state.published.read().await.status.clone();
            send_message(write, message.id, ResponseBody::Status(Box::new(status))).await?;
            Ok(Action::Continue)
        }
        RequestBody::Capabilities => {
            let capabilities = state.published.read().await.status.capabilities.clone();
            send_message(write, message.id, ResponseBody::Capabilities(capabilities)).await?;
            Ok(Action::Continue)
        }
        RequestBody::Workloads => {
            let payload = workloads_payload(&*state.published.read().await);
            send_message(
                write,
                message.id,
                ResponseBody::Workloads(Box::new(payload)),
            )
            .await?;
            Ok(Action::Continue)
        }
        RequestBody::Explain => {
            let result = plan_payload(&*state.published.read().await)
                .map(|plan| ResponseBody::Plan(Box::new(plan)));
            send_result(write, message.id, result).await
        }
        RequestBody::Rules => {
            let config = state.config.read().await.clone();
            let payload = rules_payload(&config, state.config_path.as_deref());
            send_message(write, message.id, ResponseBody::Rules(Box::new(payload))).await?;
            Ok(Action::Continue)
        }
        RequestBody::Actions => {
            let result = {
                let published = state.published.read().await;
                let journal = state.journal.read().await;
                actions_payload(&published, &journal)
                    .map(|payload| ResponseBody::Actions(Box::new(payload)))
            };
            send_result(write, message.id, result).await
        }
        RequestBody::Diagnostics => {
            let config = state.config.read().await.clone();
            let result = {
                let published = state.published.read().await;
                let journal = state.journal.read().await;
                Ok(ResponseBody::Diagnostics(Box::new(diagnostics_payload(
                    state, &published, &config, &journal,
                ))))
            };
            send_result(write, message.id, result).await
        }
        RequestBody::SetMode { mode, persist } => {
            let mut next = state.config.read().await.clone();
            next.mode = mode;
            let result = apply_config_change(
                state,
                next,
                persist,
                format!("management mode set to {mode}"),
            )
            .await;
            send_result(write, message.id, result).await
        }
        RequestBody::SetProtection {
            add,
            remove,
            persist,
        } => {
            let result = set_protection(state, &add, &remove, persist).await;
            send_result(write, message.id, result).await
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

async fn send_result(
    write: &mut OwnedWriteHalf,
    id: u64,
    result: Result<ResponseBody, ApiError>,
) -> std::io::Result<Action> {
    let body = result.unwrap_or_else(ResponseBody::Error);
    send_message(write, id, body).await?;
    Ok(Action::Continue)
}

fn workloads_payload(published: &Published) -> WorkloadsPayload {
    WorkloadsPayload {
        observed_at_ms: published.status.observed_at_ms,
        workloads: published.workloads.iter().map(workload_summary).collect(),
        capabilities: published.inventory_capabilities.clone(),
    }
}

fn workload_summary(workload: &Workload) -> WorkloadSummary {
    let (cgroup_bytes, rss_bytes) = match &workload.accounting {
        MemoryAccounting::Cgroup { current_bytes } => (*current_bytes, None),
        MemoryAccounting::RssEstimate { rss_bytes, .. } => (None, Some(*rss_bytes)),
    };
    WorkloadSummary {
        scope: workload.id.scope.clone(),
        generation: workload.id.generation,
        name: workload.name.clone(),
        processes: workload.pids.len(),
        cgroup_bytes,
        rss_bytes,
        estimated: workload.accounting.is_estimate(),
        control: control_level_name(workload.control).to_string(),
    }
}

fn rules_payload(config: &Config, config_path: Option<&Path>) -> RulesPayload {
    RulesPayload {
        mode: config.mode,
        config_path: config_path.map(|path| path.display().to_string()),
        allowed_actions: config.automation.allowed_actions.clone(),
        protected: config.protection.protected.clone(),
        budgets: config.budgets.clone(),
        rules: config.rules.clone(),
    }
}

fn plan_payload(published: &Published) -> Result<PlanPayload, ApiError> {
    match &published.plan {
        Some(plan) => Ok(PlanPayload::from_plan(
            plan,
            published.status.observed_at_ms,
        )),
        None => Err(ApiError::new(
            ErrorCode::Unavailable,
            "the daemon has no host memory observation yet, so it has no plan to explain",
        )),
    }
}

fn actions_payload(
    published: &Published,
    journal: &ActionJournal,
) -> Result<ActionsPayload, ApiError> {
    let plan = plan_payload(published)?;
    let history: Vec<ActionJournalRecord> = journal
        .entries()
        .iter()
        .rev()
        .map(ActionJournalRecord::from_entry)
        .collect();
    Ok(ActionsPayload {
        plan,
        history,
        history_capacity: journal.capacity(),
        history_note: Some(EXECUTION_NOTE.to_string()),
    })
}

fn diagnostics_payload(
    state: &SharedState,
    published: &Published,
    config: &Config,
    journal: &ActionJournal,
) -> DiagnosticsPayload {
    let mut notes = vec![EXECUTION_NOTE.to_string()];
    if config.automation.allowed_actions.is_empty() {
        notes.push(
            "automation allows no actions, so automatic mode would never act on this host"
                .to_string(),
        );
    }
    DiagnosticsPayload {
        mode: config.mode,
        version: VERSION.to_string(),
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: PROTOCOL_MINOR,
        socket: state.socket.display().to_string(),
        uptime_ms: state.clock.monotonic_ms(),
        observed_at_ms: published.status.observed_at_ms,
        config_path: state
            .config_path
            .as_ref()
            .map(|path| path.display().to_string()),
        memory: memory_breakdown(published),
        pressure: pressure_summary(published),
        jobs_enabled: config.jobs.enabled,
        workloads: published.workloads.len(),
        control_targets: published.control_targets,
        capabilities: merged_capabilities(published),
        journal: JournalSummary {
            entries: journal.len(),
            capacity: journal.capacity(),
            executing: false,
        },
        notes,
    }
}

/// Everything that could be read, from both the telemetry and the inventory.
///
/// Capability names are distinct between the two reports, so one view of the
/// host answers "what can breadram actually do here" without the reader having
/// to know which side a name came from.
fn merged_capabilities(published: &Published) -> CapabilityReport {
    let mut merged = published.inventory_capabilities.clone();
    for capability in &published.status.capabilities.capabilities {
        merged.set(capability.name.clone(), capability.state.clone());
    }
    merged
}

fn memory_breakdown(published: &Published) -> MemoryBreakdown {
    let mut notes = Vec::new();
    let Some(snapshot) = &published.snapshot else {
        notes.push("host memory could not be read, so every figure is unknown".to_string());
        return MemoryBreakdown {
            ram_total_bytes: None,
            ram_available_bytes: None,
            swap_total_bytes: None,
            swap_used_bytes: None,
            shared_bytes: None,
            vram_bytes: None,
            swap_pages_in: None,
            swap_pages_out: None,
            notes,
        };
    };
    let meminfo = &snapshot.meminfo;
    if meminfo.available.is_none() {
        notes.push(
            "MemAvailable is missing on this kernel, so available RAM is unknown".to_string(),
        );
    }
    if meminfo.shmem.is_none() {
        notes.push("Shmem is missing from /proc/meminfo, so shared memory is unknown".to_string());
    }
    if meminfo.swap_total == 0 {
        notes.push("no swap is configured on this host".to_string());
    }
    if snapshot.vmstat.pswpin.is_none() && snapshot.vmstat.pswpout.is_none() {
        notes.push("swap activity counters are unavailable, so swap rate is unknown".to_string());
    }
    notes
        .push("device memory is not measured: no adapter or provider reports VRAM yet".to_string());
    MemoryBreakdown {
        ram_total_bytes: Some(meminfo.total),
        ram_available_bytes: meminfo.available,
        swap_total_bytes: Some(meminfo.swap_total),
        swap_used_bytes: meminfo.swap_used(),
        shared_bytes: meminfo.shmem,
        vram_bytes: None,
        swap_pages_in: snapshot.vmstat.pswpin,
        swap_pages_out: snapshot.vmstat.pswpout,
        notes,
    }
}

fn pressure_summary(published: &Published) -> PressureSummary {
    let psi = published
        .snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.psi);
    let available_fraction = published.snapshot.as_ref().map(|snapshot| {
        let available = snapshot.meminfo.available.unwrap_or(snapshot.meminfo.free);
        available as f64 / snapshot.meminfo.total.max(1) as f64
    });
    PressureSummary {
        state: pressure_state_name(published.pressure).to_string(),
        some_avg10: psi.map(|reading| reading.some.avg10),
        full_avg10: psi.and_then(|reading| reading.full).map(|line| line.avg10),
        available_fraction,
    }
}

async fn apply_config_change(
    state: &SharedState,
    next: Config,
    persist: bool,
    summary: String,
) -> Result<ResponseBody, ApiError> {
    if let Err(err) = validate(&next) {
        return Err(ApiError::new(
            ErrorCode::BadRequest,
            format!("the change would leave an invalid configuration: {err}"),
        ));
    }
    // The file is written before the change is applied, so a failed write
    // changes nothing at all.
    let persisted = if persist {
        persist_config(state, &next).await?
    } else {
        false
    };
    *state.config.write().await = next.clone();
    state.config_version.fetch_add(1, Ordering::Relaxed);
    let published = {
        let config = next.clone();
        let clock = state.clock;
        tokio::task::spawn_blocking(move || {
            let mut machine = StateMachine::new(&config);
            let mut history = policy_history(&config);
            observe(&clock, &config, &mut machine, &mut history)
        })
        .await
        .map_err(|err| ApiError::new(ErrorCode::Internal, err.to_string()))?
    };
    *state.published.write().await = published;
    Ok(ResponseBody::Applied(Box::new(AppliedPayload {
        summary,
        persisted,
        rules: rules_payload(&next, state.config_path.as_deref()),
    })))
}

async fn persist_config(state: &SharedState, config: &Config) -> Result<bool, ApiError> {
    let Some(path) = state.config_path.clone() else {
        return Err(ApiError::new(
            ErrorCode::BadRequest,
            "no configuration file was loaded, so there is nothing to write: start breadramd with --config",
        ));
    };
    let text = toml::to_string(config)
        .map_err(|err| ApiError::new(ErrorCode::Internal, err.to_string()))?;
    tokio::task::spawn_blocking(move || std::fs::write(&path, text))
        .await
        .map_err(|err| ApiError::new(ErrorCode::Internal, err.to_string()))?
        .map_err(|err| ApiError::new(ErrorCode::Internal, err.to_string()))?;
    Ok(true)
}

async fn set_protection(
    state: &SharedState,
    add: &[String],
    remove: &[String],
    persist: bool,
) -> Result<ResponseBody, ApiError> {
    let mut next = state.config.read().await.clone();
    let mut changed = Vec::new();
    for scope in add {
        let scope = scope.trim();
        if scope.is_empty() {
            return Err(ApiError::new(
                ErrorCode::BadRequest,
                "a protected scope must not be empty",
            ));
        }
        if !next.protection.protected.iter().any(|known| known == scope) {
            next.protection.protected.push(scope.to_string());
            changed.push(format!("protected {scope}"));
        }
    }
    for scope in remove {
        let scope = scope.trim();
        if let Some(index) = next
            .protection
            .protected
            .iter()
            .position(|known| known == scope)
        {
            next.protection.protected.remove(index);
            changed.push(format!("unprotected {scope}"));
        }
    }
    if changed.is_empty() {
        return Err(ApiError::new(
            ErrorCode::BadRequest,
            "nothing to change: every scope was already in that state",
        ));
    }
    apply_config_change(state, next, persist, changed.join(", ")).await
}

fn set_owner_only(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

fn owner_uid(path: &Path) -> std::io::Result<u32> {
    use std::os::unix::fs::MetadataExt;
    Ok(std::fs::metadata(path)?.uid())
}
