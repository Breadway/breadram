//! The daemon event loop, monitoring task, and connection handling.
//!
//! Two concerns are deliberately separated. A monitoring task collects a status
//! snapshot on its own interval and never blocks on a client. An accept loop
//! serves connections, and each connection is handled in its own task, so a
//! slow or stalled client can only affect itself. Optional integrations do not
//! exist yet, so an absent integration cannot stall anything.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use breadram_core::adapter::{
    AdapterExecutor, AdapterRegistry, CircuitConfig, HttpTransport, ProcessHookRunner, Sleeper,
};
use breadram_core::config::{validate, ActionKind, Config, GatewayTracking, HintState, Priority};
use breadram_core::control::{discover_targets, ControlExecutor, ProcessRunner};
use breadram_core::coordinator::{
    ActionJournal, Cancellation, ChainedExecutor, Coordinator, Executor, OwnerToken,
    PersistedJournal, RestoreOutcome,
};
use breadram_core::execution::{execute_plan, ExecutionPass};
use breadram_core::inventory::{collect_inventory, MemoryAccounting, Workload};
use breadram_core::ipc::{
    self, control_level_name, decode, encode, pressure_state_name, ActionJournalRecord,
    ActionsPayload, AdmittedJobView, ApiError, AppliedPayload, ClientMessage, DiagnosticsPayload,
    ErrorCode, EventPayload, FinishedJobView, JobsPayload, JournalSummary, MemoryBreakdown,
    PendingJobView, PlanPayload, PressureSummary, RequestBody, ResponseBody, RulesPayload,
    RunningJobView, ServerMessage, StatusPayload, WorkloadSummary, WorkloadsPayload,
    MAX_MESSAGE_BYTES, PROTOCOL_MAJOR, PROTOCOL_MINOR,
};
use breadram_core::jobs::{
    AdmissionQueue, PendingJob, PersistedAdmissions, MAX_PERSISTED_ADMISSIONS,
};
use breadram_core::policy::{
    explain_priority, plan as plan_actions, serves_protected, ActivityEvidence, ActivitySource,
    DebouncedActivity, Plan, PlanChannel, PlannerInput, PolicyHistory, PressureState, StateMachine,
    MIN_IDLE_MS,
};
use breadram_core::serving::{
    GatewayServer, Route as GatewayRoute, ServerConfig as GatewayConfig, TrackingCompleteness,
    WakeSpec as GatewayWakeSpec,
};
use breadram_core::telemetry::snapshot::MemorySnapshot;
use breadram_core::telemetry::{
    collect_cgroup_swap, collect_gtt, collect_host, collect_swap, Clock, GttMemory, RealFs,
    SwapLayout, SystemClock, WorkloadSwap, CAP_GPU_GTT,
};
use breadram_core::trends::{
    TrendHistory, TrendSample, KIND_HOST, KIND_PRESSURE, KIND_RECOVERY, KIND_WORKLOAD,
};
use breadram_core::{CapabilityReport, ManagementMode, VERSION};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedWriteHalf;
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio::sync::{broadcast, Mutex, RwLock};

/// How many events a slow client may fall behind before it starts dropping.
const EVENT_CHANNEL_CAPACITY: usize = 16;

/// The rate window and bound for the policy history.
///
/// The planner's rate limit reads these, so they describe the policy, not the
/// journal size.
const HISTORY_RATE_WINDOW_MS: u64 = 60_000;
/// The number of actions the planner allows inside the rate window.
const HISTORY_MAX_ACTIONS_PER_WINDOW: usize = 10;

/// Said in one place, so a report never implies that automatic action is running
/// when it is not.
const OBSERVATION_ONLY_NOTE: &str =
    "breadramd is in monitor mode, which never writes: no action is executed and the journal records none";

/// The default router load path a gateway wake calls.
const DEFAULT_WAKE_LOAD_PATH: &str = "/models/load";
/// The default router listing path a gateway wake verifies against.
const DEFAULT_WAKE_HEALTH_PATH: &str = "/models";

/// The scope unit name prefix for supervised jobs.
const JOB_SCOPE_PREFIX: &str = "breadram-job-";

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
    /// The gateway listen address could not be bound.
    #[error("cannot serve the gateway on {addr}: {source}")]
    GatewayBind {
        /// The listen address.
        addr: String,
        /// The underlying error.
        #[source]
        source: std::io::Error,
    },
    /// A generic I/O failure.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub(crate) struct SharedState {
    trends: RwLock<TrendHistory>,
    trend_keys: std::collections::hash_map::RandomState,
    pub(crate) published: RwLock<Published>,
    config: RwLock<Config>,
    config_version: AtomicU64,
    config_path: Option<PathBuf>,
    socket: PathBuf,
    journal: RwLock<ActionJournal>,
    /// What the monitor has to say about executing: what it restored after a
    /// restart, what it attached, and what it refused. Written by the monitor,
    /// read by a diagnostic request.
    execution_notes: RwLock<Vec<String>>,
    /// Where the owned values are remembered between runs, when there is
    /// somewhere to put them.
    state_path: Option<PathBuf>,
    /// A cooperative stop flag, so a shutdown does not start another write.
    cancel: Cancellation,
    events: broadcast::Sender<EventPayload>,
    interval_ms: u64,
    socket_uid: u32,
    pub(crate) clock: SystemClock,
    /// Explicitly opted-in background jobs waiting for or holding admission
    /// reservations. The daemon owns the queue and supervises what launches
    /// from it, explicitly or through the opted-in automatic loop.
    jobs: Mutex<AdmissionQueue>,
    /// Admission ids a previous run recorded. This run refuses to reuse them:
    /// it cannot prove a recorded process is still its own.
    jobs_refused: RwLock<Vec<String>>,
    /// Launch specifications by job id. Memory only: never persisted,
    /// journalled, or reported, so arguments, directories, and environment
    /// never leave the running daemon.
    job_specs: Mutex<HashMap<String, JobSpec>>,
    /// Supervised children by job id, for cancellation.
    job_running: Mutex<HashMap<String, JobCancel>>,
    /// Recently finished jobs, newest last, bounded.
    job_finished: Mutex<VecDeque<FinishedJob>>,
    /// Scopes the gateway currently holds a lease for. Written by the gateway
    /// task, read by the monitor, so a request in flight is active evidence
    /// the planner can see and will not unload from under.
    gateway_active: RwLock<Vec<String>>,
    /// Scopes the monitor confirmed an idle unload for. Written by the monitor,
    /// read by the gateway task, which marks the backend asleep so the next
    /// request wakes it instead of being forwarded into a miss.
    gateway_unloaded: Mutex<Vec<String>>,
}

/// What to execute for one admitted job.
///
/// Arguments, the working directory, and the environment travel only as far as
/// the spawn call: they are never persisted, journalled, or reported.
#[derive(Debug, Clone)]
struct JobSpec {
    /// The program and its arguments, executed directly with no shell.
    argv: Vec<String>,
    /// The working directory. The daemon's own when absent.
    cwd: Option<String>,
    /// Extra environment entries as KEY=VALUE strings, applied over the
    /// inherited environment.
    env: Vec<String>,
}

/// How to stop one supervised job.
struct JobCancel {
    /// Cancelling the supervisor stops the scope and the waiter.
    cancel: tokio::sync::oneshot::Sender<()>,
}

/// One finished job, remembered in a bounded list.
#[derive(Debug, Clone)]
struct FinishedJob {
    /// The job id.
    id: String,
    /// The exit code, absent when a signal ended the job.
    exit_code: Option<i32>,
    /// What happened, in words.
    note: String,
    /// The peak memory observed while it ran, when it could be read.
    peak_bytes: Option<u64>,
}

/// How many finished jobs the report remembers.
const MAX_FINISHED_JOBS: usize = 32;
/// The most arguments one submit may carry.
const MAX_JOB_ARGC: usize = 128;
/// The most bytes one argument may carry.
const MAX_JOB_ARG_BYTES: usize = 4096;
/// How long a cancelled scope has to empty after SIGTERM before SIGKILL.
const JOB_STOP_GRACE: Duration = Duration::from_secs(5);
/// How often a running job's memory peak is sampled.
const JOB_PEAK_INTERVAL: Duration = Duration::from_millis(100);
/// The cgroup hierarchy root for scope discovery.
const CGROUP_ROOT: &str = "/sys/fs/cgroup";

/// The last complete observation.
///
/// One pass over procfs serves every client, so a status request, an explanation,
/// and a diagnostic bundle all describe the same instant. Nothing a client asks
/// for can trigger a collection of its own or block the monitor.
#[derive(Clone)]
pub(crate) struct Published {
    pub(crate) status: StatusPayload,
    snapshot: Option<MemorySnapshot>,
    workloads: Vec<Workload>,
    /// What the inventory could read, which is its own report: a snapshot
    /// capability says nothing about whether procfs could be walked.
    inventory_capabilities: CapabilityReport,
    control_targets: usize,
    pub(crate) pressure: PressureState,
    plan: Option<Plan>,
    /// What this pass learned from the configured providers: an endpoint that
    /// could not be read, or an answer that could not be understood. Reported
    /// rather than swallowed, because an adapter that cannot see its provider
    /// cannot offer a release either.
    probe_notes: Vec<String>,
    /// The swap, compression, and OOM management on this host, as read.
    swap: SwapLayout,
    /// The effective swap limit of each workload breadram can control.
    workload_swap: Vec<WorkloadSwap>,
    /// The effective priority of each workload and why it applies.
    priorities: Vec<WorkloadPriority>,
    /// Integrated GPU shared memory, read once per pass.
    gtt: GttMemory,
}

/// The effective priority of a workload and the reason it applies, for the wire.
#[derive(Debug, Clone)]
struct WorkloadPriority {
    scope: String,
    priority: Priority,
    source: String,
}

/// What a handler should do next.
enum Action {
    Continue,
    Close,
    Subscribed(broadcast::Receiver<EventPayload>),
    /// Start supervision for an admitted job, then remember the queue.
    LaunchJob {
        /// The job id.
        id: String,
        /// What to execute. Memory only, never persisted.
        spec: JobSpec,
        /// The admitted reservation in bytes.
        reservation: u64,
        /// The scope unit the job runs in.
        scope_unit: String,
        /// The supervisor's cancellation receiver.
        cancel_rx: tokio::sync::oneshot::Receiver<()>,
    },
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
            observe(
                &clock,
                &config,
                &mut runtime.machine,
                &mut runtime.history,
                &runtime.registry,
                &mut runtime.hints,
                Observation {
                    probes: &ProviderProbes::default(),
                    gateway_active: &[],
                },
            )
        })
        .await
        .unwrap_or_else(|_| unavailable(mode, clock.monotonic_ms()))
    };

    let journal_capacity = config.history.max_records as usize;
    let job_capacity = config.jobs.max_queued as usize;
    let (events, _receiver) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
    let state = Arc::new(SharedState {
        trends: RwLock::new(TrendHistory::new(0, 0)),
        trend_keys: std::collections::hash_map::RandomState::new(),
        published: RwLock::new(initial),
        config: RwLock::new(config),
        config_version: AtomicU64::new(0),
        config_path,
        socket: socket.clone(),
        journal: RwLock::new(ActionJournal::new(journal_capacity)),
        jobs: Mutex::new(AdmissionQueue::new(job_capacity)),
        jobs_refused: RwLock::new(Vec::new()),
        job_specs: Mutex::new(HashMap::new()),
        job_running: Mutex::new(HashMap::new()),
        job_finished: Mutex::new(VecDeque::new()),
        gateway_active: RwLock::new(Vec::new()),
        gateway_unloaded: Mutex::new(Vec::new()),
        execution_notes: RwLock::new(Vec::new()),
        state_path: default_state_path(),
        cancel: Cancellation::new(),
        events,
        interval_ms,
        socket_uid,
        clock,
    });

    spawn_monitor(Arc::clone(&state), interval);

    // A previous run's admission record is refused, never re-admitted: this
    // run cannot prove a recorded process is still its own.
    {
        let (refused, notes) = load_refused_admissions(state.state_path.as_deref());
        *state.jobs_refused.write().await = refused;
        state.execution_notes.write().await.extend(notes);
    }

    // The optional inference gateway serves the configured routes on its own
    // loopback listener for as long as the daemon runs. It shares nothing with
    // the monitor pass except the configuration it started from.
    spawn_gateway(Arc::clone(&state)).await?;

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

    // Owned lifecycle: the daemon stops the jobs it supervises before it
    // goes away, so nothing it admitted keeps running unmanaged. Scopes from
    // a violently killed run are not touched here: their ids stay refused and
    // a launch collides with a lingering scope by name, so they cannot be
    // mistaken for this run's work.
    stop_all_jobs(&state).await;

    // Ask a write in flight to stop before the last one starts. Nothing is
    // interrupted part way through: the flag is only read between actions.
    state.cancel.cancel();

    drop(listener);
    let _ = std::fs::remove_file(&socket);
    Ok(())
}

/// Stop every supervised job: SIGTERM to each scope, a short grace, then
/// SIGKILL to stragglers. Best effort and bounded, so shutdown stays prompt.
async fn stop_all_jobs(state: &SharedState) {
    let ids: Vec<String> = state.job_running.lock().await.keys().cloned().collect();
    if ids.is_empty() {
        return;
    }
    for id in &ids {
        kill_scope_unit(&job_scope_unit(id), "TERM");
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if ids
            .iter()
            .all(|id| find_scope_dir(Path::new(CGROUP_ROOT), &job_scope_unit(id)).is_none())
        {
            break;
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for id in &ids {
        let unit = job_scope_unit(id);
        if find_scope_dir(Path::new(CGROUP_ROOT), &unit).is_some() {
            kill_scope_unit(&unit, "KILL");
        }
    }
}

/// The bounded path to the configured providers.
///
/// It is deliberately small and copyable: the probe client owns the retries, the
/// backoff, the timeout, and the response cap, so the daemon only has to say how
/// to reach the wire and how to wait.
#[derive(Clone, Copy, Default)]
struct ProviderProbes {
    transport: HttpTransport,
    sleeper: ThreadSleeper,
    circuit: CircuitConfig,
    hooks: ProcessHookRunner,
}

/// A sleeper that really waits.
///
/// Only the daemon's blocking monitor worker uses this. Core ships a no-op
/// sleeper, so tests and library users never block on a retry.
#[derive(Debug, Default, Clone, Copy)]
struct ThreadSleeper;

impl Sleeper for ThreadSleeper {
    fn sleep_ms(&self, ms: u64) {
        std::thread::sleep(Duration::from_millis(ms));
    }
}

/// Read every configured provider once, and record what came back.
///
/// The probes come from the adapters, which only ever ask about endpoints an
/// operator configured, so nothing is discovered and no port is guessed. A read
/// that failed is recorded as a failed reading rather than leaving the previous
/// success in place, and the returned notes name the endpoint that could not be
/// read. Hook programs run under the same rule, through the hook runner.
fn refresh_adapters(
    registry: &AdapterRegistry,
    probes: &ProviderProbes,
    clock: &SystemClock,
) -> Vec<String> {
    let mut notes = breadram_core::adapter::refresh_adapters(
        registry,
        &probes.transport,
        &probes.sleeper,
        clock,
        probes.circuit,
    );
    notes.extend(breadram_core::adapter::refresh_hooks(
        registry,
        &probes.hooks,
        clock.monotonic_ms(),
    ));
    notes
}

/// The policy state that has to survive between monitor ticks.
///
/// It moves into the blocking worker and back on every tick, because hysteresis
/// depends on the machine keeping its history, the planner needs the same record
/// of past actions, and an adapter's view of a provider only means something if
/// it is the same adapter from one tick to the next.
struct PolicyRuntime {
    machine: StateMachine,
    history: PolicyHistory,
    registry: AdapterRegistry,
    /// Per-scope debounced activity hints. Promotion is immediate and demotion
    /// waits for stable absence, so a focus change cannot oscillate reclaim.
    hints: HintState,
    /// What the configured providers contributed, or why they contributed
    /// nothing.
    adapter_notes: Vec<String>,
    config_version: u64,
}

impl PolicyRuntime {
    fn new(config: &Config, config_version: u64) -> Self {
        let (registry, adapter_notes) = AdapterRegistry::from_config(config);
        Self {
            machine: StateMachine::new(config),
            history: policy_history(config),
            registry,
            hints: HintState::default(),
            adapter_notes,
            config_version,
        }
    }
}

/// Serve the optional inference gateway for as long as the daemon runs.
///
/// Disabled by default: without `[gateway] enabled` this does nothing. When
/// enabled, the configuration was already validated, so the listen address and
/// every backend address are loopback socket addresses.
async fn spawn_gateway(state: Arc<SharedState>) -> Result<(), DaemonError> {
    let (enabled, listen, routes, now_ms) = {
        let config = state.config.read().await;
        (
            config.gateway.enabled,
            config.gateway.listen.clone(),
            config.gateway.routes.clone(),
            state.clock.monotonic_ms(),
        )
    };
    if !enabled {
        return Ok(());
    }
    // Validation requires a listen address when enabled; a programmatically
    // built configuration without one fails loudly rather than binding a
    // default nobody asked for.
    let listen = listen.ok_or_else(|| DaemonError::GatewayBind {
        addr: "(unset)".to_string(),
        source: std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "the gateway is enabled without a listen address",
        ),
    })?;
    let listener = TcpListener::bind(&listen)
        .await
        .map_err(|source| DaemonError::GatewayBind {
            addr: listen.clone(),
            source,
        })?;
    let routes = routes
        .iter()
        .map(|route| GatewayRoute {
            path_prefix: route.path_prefix.clone(),
            workload_scope: route.workload_scope.clone(),
            backend_addr: route.backend_addr.clone(),
            tracking: match route.tracking {
                GatewayTracking::Complete => TrackingCompleteness::Complete,
                GatewayTracking::Bypassable => TrackingCompleteness::Bypassable {
                    provider_proves_idle: false,
                },
                GatewayTracking::Unknown => TrackingCompleteness::Unknown,
            },
            wake: route.wake_model.as_ref().map(|model| GatewayWakeSpec {
                load_path: route
                    .wake_load_path
                    .clone()
                    .unwrap_or_else(|| DEFAULT_WAKE_LOAD_PATH.to_string()),
                model_id: model.clone(),
                health_path: route
                    .wake_health_path
                    .clone()
                    .unwrap_or_else(|| DEFAULT_WAKE_HEALTH_PATH.to_string()),
            }),
        })
        .collect();
    let server = Arc::new(GatewayServer::new(GatewayConfig { routes }, now_ms));
    // Before the listener opens, so a backend that is already asleep is woken
    // by the first request instead of being served into a miss. Reconcile
    // drops leases, which is why it happens here and never again.
    server.reconcile_startup().await;
    // Publish the scopes that hold a lease, so the planner sees a request in
    // flight as active and will not unload the backend from under it.
    let publishing = Arc::clone(&server);
    let shared = Arc::clone(&state);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(250));
        loop {
            ticker.tick().await;
            let active = publishing.active_scopes().await;
            *shared.gateway_active.write().await = active;
            // Apply the unloads the monitor confirmed, so the next request
            // wakes the backend instead of missing.
            let unloaded: Vec<String> = shared.gateway_unloaded.lock().await.drain(..).collect();
            for scope in unloaded {
                publishing.note_unloaded(&scope).await;
            }
        }
    });
    tokio::spawn(async move {
        server.serve(listener).await;
    });
    Ok(())
}

fn spawn_monitor(state: Arc<SharedState>, interval: Duration) {
    tokio::spawn(async move {
        // The optional bread bridge. It publishes pressure and honors commands
        // only while breadd is present and speaks the supported version, and it
        // does nothing at all otherwise.
        let mut bridge = crate::bread::Bridge::new();
        let clock = state.clock;
        let initial_config = state.config.read().await.clone();
        let mut runtime = Some(PolicyRuntime::new(
            &initial_config,
            state.config_version.load(Ordering::Relaxed),
        ));
        let journal_capacity = initial_config.history.max_records as usize;
        // What happened when this daemon started, kept for the life of the
        // process: a restore is a one time event, but it is one a report should
        // still be able to explain hours later.
        let (persisted, mut startup_notes) = load_persisted(state.state_path.as_deref());
        let mut coordinator = Some(Coordinator::resume(
            owner_token(),
            persisted.to_journal(journal_capacity),
        ));
        // The stall reading from the previous pass, so a soft limit can be
        // reversed when the next reading is worse. None until the first pass.
        let mut previous_stall_percent: Option<u32> = None;
        let mut previous_mode = initial_config.mode;

        // A restart restores the values breadram still owns, and only those: a
        // value something else changed is left alone, and a target that is gone
        // is skipped. In monitor mode nothing is written at all, so leftover
        // values are reported instead of restored, because a monitor mode daemon
        // must not change the host.
        if !persisted.is_empty() {
            if initial_config.mode == ManagementMode::Automatic {
                let taken = coordinator.take().expect("a coordinator to resume from");
                let instance = owner_token();
                let recovered = tokio::task::spawn_blocking(move || {
                    let mut coordinator = Coordinator::resume(instance, taken.journal().clone());
                    let fs = RealFs;
                    let targets = discover_targets(&fs);
                    let runner = ProcessRunner;
                    let executor = ControlExecutor::new(&fs, &runner, &targets);
                    let results = coordinator.recover(&executor, clock.monotonic_ms());
                    (coordinator, results)
                })
                .await;
                match recovered {
                    Ok((next, results)) => {
                        startup_notes.push(describe_recovery(&results));
                        state
                            .execution_notes
                            .write()
                            .await
                            .clone_from(&startup_notes);
                        publish_and_persist(&state, &next, true).await;
                        coordinator = Some(next);
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "the startup restore failed");
                        startup_notes.push(
                            "a restore from a previous run could not be attempted".to_string(),
                        );
                        coordinator = Some(Coordinator::resume(
                            owner_token(),
                            persisted.to_journal(journal_capacity),
                        ));
                    }
                }
            } else {
                startup_notes.push(format!(
                    "{} value(s) written by a previous run are still in place and were not restored, because monitor mode never writes; switch to automatic mode once to restore them",
                    persisted.values.len()
                ));
            }
        }

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
            let Some(current_coordinator) = coordinator.take() else {
                break;
            };
            // The reads and the writes are blocking, so keep them off the async
            // runtime. The policy state and the coordinator move into the task
            // and back, so hysteresis keeps its history and an action is only
            // applied once.
            let rebuild_config = config.clone();
            let cancel = state.cancel.clone();
            // A snapshot of the gateway's leases, so a request in flight counts
            // as active for this pass.
            let gateway_active = state.gateway_active.read().await.clone();
            let collected = tokio::task::spawn_blocking(move || {
                let mut current_runtime = current_runtime;
                if current_runtime.config_version != current {
                    // A configuration change restarts the machine, so a new
                    // threshold takes effect at once instead of after a dwell.
                    current_runtime = PolicyRuntime::new(&config, current);
                }
                let probes = ProviderProbes::default();
                let published = observe(
                    &clock,
                    &config,
                    &mut current_runtime.machine,
                    &mut current_runtime.history,
                    &current_runtime.registry,
                    &mut current_runtime.hints,
                    Observation {
                        probes: &probes,
                        gateway_active: &gateway_active,
                    },
                );
                let (next, pass) = run_execution(
                    current_coordinator,
                    ExecutionStep {
                        config: &config,
                        plan: published.plan.as_ref(),
                        workloads: &published.workloads,
                        now_ms: published.status.observed_at_ms,
                        host_bytes: published
                            .snapshot
                            .as_ref()
                            .map(|snapshot| snapshot.meminfo.total)
                            .unwrap_or(0),
                        cancel: &cancel,
                        registry: &current_runtime.registry,
                        probes: &probes,
                        clock: &clock,
                    },
                );
                (published, current_runtime, next, pass)
            })
            .await;
            let (published, next_runtime, next_coordinator, pass) = match collected {
                Ok(collected) => collected,
                Err(err) => {
                    // A panicked worker must not stop the monitor. Rebuild the
                    // policy state and keep watching, saying plainly that it
                    // happened rather than silently resetting.
                    tracing::warn!(error = %err, "a monitor pass failed, rebuilding the policy state");
                    runtime = Some(PolicyRuntime::new(&rebuild_config, current));
                    coordinator = Some(Coordinator::resume(
                        owner_token(),
                        ActionJournal::new(journal_capacity),
                    ));
                    continue;
                }
            };
            runtime = Some(next_runtime);
            if state.config_version.load(Ordering::Relaxed) != current {
                // A change landed while this pass was reading the host. It
                // published an observation built from the newer settings, so
                // this one is dropped rather than overwriting it with the mode
                // the host had a moment ago. The coordinator keeps the values it
                // wrote, which are still breadram's to restore.
                coordinator = Some(next_coordinator);
                continue;
            }

            let wrote = pass.as_ref().is_some_and(ExecutionPass::wrote);
            let mut notes = startup_notes.clone();
            notes.extend(published.probe_notes.iter().cloned());
            if let Some(runtime) = runtime.as_ref() {
                notes.extend(runtime.adapter_notes.iter().cloned());
            }
            match &pass {
                Some(pass) if wrote => notes.push(pass.summary()),
                Some(pass) => notes.extend(pass.notes.iter().cloned()),
                None => {}
            }
            *state.execution_notes.write().await = notes;
            // A confirmed idle unload has to reach the gateway, or the next
            // request is forwarded into a backend that is no longer serving.
            if let Some(pass) = &pass {
                let unloaded: Vec<String> = pass
                    .results
                    .iter()
                    .filter(|result| {
                        result.kind == ActionKind::UnloadIdleModel && result.is_confirmed()
                    })
                    .map(|result| result.scope.clone())
                    .collect();
                if !unloaded.is_empty() {
                    state.gateway_unloaded.lock().await.extend(unloaded);
                }
            }
            publish_and_persist(&state, &next_coordinator, wrote).await;
            let stall_now = published.snapshot.as_ref().and_then(|snapshot| {
                snapshot
                    .psi
                    .map(|reading| reading.some.avg10.round() as u32)
            });
            // A stall that rose after a soft limit is put back, and so is every
            // owned value when automatic mode turns off. Both go through the
            // journal, which leaves a value another manager changed. A target
            // that was replaced is already refused before the write, in
            // revalidate.
            let mode_now = state.config.read().await.mode;
            let stall_rose =
                breadram_core::execution::stall_should_reverse(previous_stall_percent, stall_now);
            let left_automatic =
                previous_mode == ManagementMode::Automatic && mode_now != ManagementMode::Automatic;
            previous_stall_percent = stall_now;
            previous_mode = mode_now;
            let held = next_coordinator;
            if stall_rose || left_automatic {
                let reason = if stall_rose {
                    "a stall reading rose after a soft limit"
                } else {
                    "automatic mode turned off"
                };
                let restored = tokio::task::spawn_blocking(move || {
                    let mut coordinator = held;
                    let fs = RealFs;
                    let targets = discover_targets(&fs);
                    let runner = ProcessRunner;
                    let executor = ControlExecutor::new(&fs, &runner, &targets);
                    let results = coordinator.recover(&executor, clock.monotonic_ms());
                    (coordinator, results)
                })
                .await;
                match restored {
                    Ok((next, _results)) => {
                        tracing::info!(reason, "restored owned values");
                        publish_and_persist(&state, &next, true).await;
                        coordinator = Some(next);
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, reason, "the restore failed");
                        coordinator = Some(Coordinator::resume(
                            owner_token(),
                            ActionJournal::new(journal_capacity),
                        ));
                    }
                }
            } else {
                coordinator = Some(held);
            }

            let status = published.status.clone();
            {
                let config = state.config.read().await;
                let mut trends = state.trends.write().await;
                collect_trends(
                    &mut trends,
                    &published,
                    pass.as_ref(),
                    &config,
                    &state.trend_keys,
                );
            }
            *state.published.write().await = published;
            {
                let config = state.config.read().await;
                if config.mode == ManagementMode::Automatic
                    && config.jobs.enabled
                    && config.jobs.auto_launch
                {
                    drop(config);
                    auto_launch(&state).await;
                }
            }
            for effect in crate::bread::tick(&state, &mut bridge) {
                let crate::bread::CommandEffect::Mode(mode) = effect;
                // In memory only: a bus command must not rewrite the
                // operator's configuration file. The next pass plans under
                // the new mode, and a restart returns to the file.
                let mut config = state.config.write().await;
                if config.mode != mode {
                    config.mode = mode;
                    state.config_version.fetch_add(1, Ordering::Relaxed);
                }
            }
            let _ = state.events.send(EventPayload {
                kind: "status".to_string(),
                observed_at_ms: status.observed_at_ms,
                available_bytes: status.available_bytes,
            });
        }
    });
}

/// Publish what the coordinator holds, and remember it for a restart.
///
/// The journal is only copied out when something was written, so a quiet host
/// does not copy it on every tick.
async fn publish_and_persist(state: &SharedState, coordinator: &Coordinator, wrote: bool) {
    if wrote {
        *state.journal.write().await = coordinator.journal().clone();
    }
    let journal = coordinator.journal();
    if journal.is_empty() && !wrote {
        return;
    }
    let stored = PersistedJournal::from_journal(journal);
    let path = state.state_path.clone();
    let text = match stored.encode() {
        Ok(text) => text,
        Err(err) => {
            tracing::warn!(error = %err, "the owned values could not be encoded");
            return;
        }
    };
    let Some(path) = path else {
        return;
    };
    let shown = path.display().to_string();
    let written = tokio::task::spawn_blocking(move || write_state_file(&path, &text)).await;
    if let Ok(Err(err)) = written {
        tracing::warn!(error = %err, path = %shown, "the owned values could not be written");
    }
}

/// Describe what a startup restore did, in one line.
fn describe_recovery(results: &[breadram_core::coordinator::RestoreResult]) -> String {
    if results.is_empty() {
        return "no value from a previous run needed restoring".to_string();
    }
    let count = |want: fn(&RestoreOutcome) -> bool| {
        results
            .iter()
            .filter(|result| want(&result.outcome))
            .count()
    };
    let restored = count(|outcome| *outcome == RestoreOutcome::Restored);
    let gone = count(|outcome| *outcome == RestoreOutcome::SkippedTargetGone);
    let unchanged = count(|outcome| *outcome == RestoreOutcome::SkippedNoChange);
    let foreign = count(|outcome| matches!(outcome, RestoreOutcome::SkippedForeignChange { .. }));
    let failed = count(|outcome| matches!(outcome, RestoreOutcome::Failed { .. }));

    let mut line = format!("restored {restored} value(s) written by a previous run");
    if gone > 0 {
        line.push_str(&format!(", skipped {gone} whose target no longer exists"));
    }
    if unchanged > 0 {
        line.push_str(&format!(
            ", left {unchanged} already at their original value"
        ));
    }
    if foreign > 0 {
        line.push_str(&format!(
            ", left {foreign} alone because something else had changed them"
        ));
    }
    if failed > 0 {
        line.push_str(&format!(", and failed to restore {failed}"));
    }
    line
}

/// Run the execute step for one pass, with the real control executor.
///
/// Only the automatic mode writes, and only the actions the configuration
/// allows. Host controls go through systemd, which is the manager that owns the
/// unit's cgroup, so breadram never rewrites a hierarchy itself.
fn run_execution(
    mut coordinator: Coordinator,
    step: ExecutionStep<'_>,
) -> (Coordinator, Option<ExecutionPass>) {
    let ExecutionStep {
        config,
        plan,
        workloads,
        now_ms,
        host_bytes,
        cancel,
        registry,
        probes,
        clock,
    } = step;
    let Some(plan) = plan else {
        return (coordinator, None);
    };
    let fs = RealFs;
    let targets = discover_targets(&fs);
    let runner = ProcessRunner;
    let control = ControlExecutor::new(&fs, &runner, &targets);
    // Host controls and provider actions are disjoint, so each executor claims
    // the actions it can carry out and the chain keeps them in one pass. The
    // transport is the same value on both sides: a probe can only read, and only
    // an operation can write.
    let provider = AdapterExecutor::new(
        registry,
        workloads,
        &probes.transport,
        &probes.transport,
        &probes.sleeper,
        clock,
    )
    .with_hook_runner(&probes.hooks);
    let executor = ChainedExecutor::new(vec![&control, &provider]);
    let pass = execute_with(
        &mut coordinator,
        config,
        plan,
        workloads,
        &executor,
        now_ms,
        cancel,
        host_bytes,
    );
    (coordinator, Some(pass))
}

/// Everything one execution pass needs beyond the coordinator itself.
struct ExecutionStep<'a> {
    config: &'a Config,
    plan: Option<&'a Plan>,
    workloads: &'a [Workload],
    now_ms: u64,
    /// Host RAM from the snapshot. Zero means unknown, which refuses a soft limit.
    host_bytes: u64,
    cancel: &'a Cancellation,
    /// The configured providers, so a provider action can be carried out.
    registry: &'a AdapterRegistry,
    probes: &'a ProviderProbes,
    clock: &'a SystemClock,
}

/// The execute step itself, over any executor.
///
/// Split out so the daemon's own gating can be tested without a host control:
/// a monitor mode pass must write nothing whatever the plan says.
#[allow(clippy::too_many_arguments)]
fn execute_with(
    coordinator: &mut Coordinator,
    config: &Config,
    plan: &Plan,
    workloads: &[Workload],
    executor: &dyn Executor,
    now_ms: u64,
    cancel: &Cancellation,
    host_bytes: u64,
) -> ExecutionPass {
    execute_plan(
        coordinator,
        plan,
        config,
        config.mode,
        workloads,
        executor,
        now_ms,
        cancel,
        host_bytes,
    )
}

/// A label for the values this instance writes.
fn owner_token() -> OwnerToken {
    OwnerToken(format!("breadramd-{}", std::process::id()))
}

/// Where the owned values live between runs.
///
/// The socket lives in the runtime directory, which is cleared on a reboot, so
/// it is the wrong place for something that has to survive one. If there is
/// nowhere to write, the daemon still works: it simply cannot restore anything
/// after a restart, and nothing is claimed to the contrary.
fn default_state_path() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os("XDG_STATE_HOME").filter(|dir| !dir.is_empty()) {
        return Some(PathBuf::from(dir).join("breadram/journal.json"));
    }
    let home = std::env::var_os("HOME").filter(|dir| !dir.is_empty())?;
    Some(PathBuf::from(home).join(".local/state/breadram/journal.json"))
}

/// Read the values a previous run owned.
///
/// A missing file is the ordinary first run. A file this build cannot read is
/// reported and ignored, never guessed at: restoring a value from a file written
/// by another build is exactly the case where guessing would write the wrong
/// thing.
fn load_persisted(path: Option<&Path>) -> (PersistedJournal, Vec<String>) {
    let empty = PersistedJournal {
        version: breadram_core::coordinator::JOURNAL_STATE_VERSION,
        values: Vec::new(),
    };
    let Some(path) = path else {
        return (
            empty,
            vec!["no state directory is available, so the values breadram owns cannot be remembered across a restart".to_string()],
        );
    };
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return (empty, Vec::new()),
        Err(err) => {
            return (
                empty,
                vec![format!(
                    "the state file at {} could not be read, so nothing was restored: {err}",
                    path.display()
                )],
            )
        }
    };
    match PersistedJournal::decode(&text) {
        Ok(persisted) => (persisted, Vec::new()),
        Err(err) => (
            empty,
            vec![format!(
                "the state file at {} was not understood, so nothing was restored: {err}",
                path.display()
            )],
        ),
    }
}

/// Write the owned values, creating the directory when needed.
fn write_state_file(path: &Path, text: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Written beside the target and renamed, so a crash cannot leave a half
    // written state file that a restart would then read.
    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, text)?;
    std::fs::rename(&temporary, path)
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
/// One observation's inputs that are not the policy state itself.
struct Observation<'a> {
    probes: &'a ProviderProbes,
    /// Scopes the gateway holds a lease for, so a request in flight is active.
    gateway_active: &'a [String],
}

fn observe(
    clock: &SystemClock,
    config: &Config,
    machine: &mut StateMachine,
    history: &mut PolicyHistory,
    registry: &AdapterRegistry,
    hints: &mut HintState,
    observation: Observation<'_>,
) -> Published {
    let fs = RealFs;
    // The providers are read first, so the policy reasons about what a backend is
    // doing now rather than about the last answer that happened to succeed.
    let probe_notes = refresh_adapters(registry, observation.probes, clock);
    let inventory = collect_inventory(&fs);
    // Fold this pass's activity readings into the debounced hints, then build the
    // one activity view the plan and the status both reason about. Promotion is
    // immediate; demotion waits for MIN_IDLE_MS of stable absence.
    let raw_activity = registry.activity_source(&inventory.workloads);
    let now_ms = clock.monotonic_ms();
    for workload in &inventory.workloads {
        // A gateway lease is active evidence on its own: a request in flight
        // must not be unloaded, even when the provider has not answered yet.
        let active = observation
            .gateway_active
            .iter()
            .any(|scope| scope == &workload.id.scope)
            || matches!(
                raw_activity.evidence(&workload.id),
                ActivityEvidence::Active
            );
        hints.update(&workload.id.scope, active, now_ms, MIN_IDLE_MS);
    }
    let activity = DebouncedActivity::new(&raw_activity, hints);
    let priorities: Vec<WorkloadPriority> = inventory
        .workloads
        .iter()
        .map(|workload| {
            let (priority, source) = explain_priority(
                config,
                workload,
                &activity,
                serves_protected(config, workload, &inventory.workloads),
            );
            WorkloadPriority {
                scope: workload.id.scope.clone(),
                priority,
                source,
            }
        })
        .collect();
    // The swap layout is read once per pass and reported, never changed: the
    // swappiness, swap files, fstab, and kernel parameters are the
    // administrator's, and breadram installs no second OOM manager.
    let swap = collect_swap(&fs);
    // Integrated GPU shared memory is read once per pass, like the swap layout:
    // it is reported and never changed.
    let gtt = collect_gtt(&fs);
    let targets = discover_targets(&fs);
    let control_targets = targets.len();
    // A swap limit is only meaningful against the cgroup a workload is actually
    // charged to, so it is read from the targets breadram can control.
    let workload_swap: Vec<WorkloadSwap> = targets
        .iter()
        .map(|target| WorkloadSwap {
            scope: target.scope.clone(),
            cgroup: target.control_group.clone(),
            swap: collect_cgroup_swap(&fs, &target.control_group),
        })
        .collect();
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
                // A configured provider is the only thing that can prove a
                // model backend is idle. With none attached, nothing has proven
                // activity, and unknown is never idle. The view is debounced so a
                // brief gap cannot demote and re-promote a workload.
                activity: &activity,
            },
            // The channel says whether the caller may execute. In automatic mode
            // it is the plan that will be carried out; in any other mode the
            // same reasoning is kept and the actions are dropped.
            if config.mode == ManagementMode::Automatic {
                PlanChannel::Automatic
            } else {
                PlanChannel::DryRun
            },
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
        probe_notes,
        swap,
        workload_swap,
        priorities,
        gtt,
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
        probe_notes: Vec::new(),
        swap: SwapLayout::default(),
        workload_swap: Vec::new(),
        priorities: Vec::new(),
        gtt: GttMemory::default(),
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
            Action::LaunchJob {
                id,
                spec,
                reservation,
                scope_unit,
                cancel_rx,
            } => {
                // Supervision starts first. The record is re-read under the
                // lock afterward, so a completion that already ran is never
                // overwritten by this launch's record.
                tokio::spawn(supervise_job(
                    Arc::clone(&state),
                    id,
                    spec,
                    reservation,
                    scope_unit,
                    cancel_rx,
                ));
                let queue = state.jobs.lock().await;
                let record =
                    PersistedAdmissions::from_queue(&queue, MAX_PERSISTED_ADMISSIONS).encode();
                let empty = queue.pending().count() == 0 && queue.admitted().count() == 0;
                drop(queue);
                persist_jobs(&state, record, empty).await;
            }
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
                actions_payload(&published, &journal, state.config.read().await.mode)
                    .map(|payload| ResponseBody::Actions(Box::new(payload)))
            };
            send_result(write, message.id, result).await
        }
        RequestBody::Diagnostics => {
            let config = state.config.read().await.clone();
            let result = {
                let published = state.published.read().await;
                let journal = state.journal.read().await;
                let execution_notes = state.execution_notes.read().await;
                let (trends, growth) = {
                    let history = state.trends.read().await;
                    let now = published.status.observed_at_ms;
                    (history.retained(now), history.report(now))
                };
                Ok(ResponseBody::Diagnostics(Box::new(diagnostics_payload(
                    state,
                    &published,
                    &config,
                    &journal,
                    &execution_notes,
                    trends,
                    growth,
                ))))
            };
            send_result(write, message.id, result).await
        }
        RequestBody::Jobs => {
            let queue = state.jobs.lock().await;
            let payload = jobs_report(state, &queue).await;
            send_message(write, message.id, ResponseBody::Jobs(Box::new(payload))).await?;
            Ok(Action::Continue)
        }
        RequestBody::SubmitJob {
            job_id,
            estimate_bytes,
            argv,
            cwd,
            env,
        } => {
            let config = state.config.read().await.clone();
            if !config.jobs.enabled {
                return send_result(
                    write,
                    message.id,
                    Err(ApiError::new(
                        ErrorCode::Unavailable,
                        "background job admission is disabled: set [jobs] enabled to use it",
                    )),
                )
                .await;
            }
            let estimate = if estimate_bytes == 0 {
                config.jobs.default_estimate_bytes
            } else {
                estimate_bytes
            };
            let result = match check_submit_spec(&argv, &cwd, &env) {
                Err(reason) => Err(ApiError::new(
                    ErrorCode::BadRequest,
                    format!("the job was not queued: {reason}"),
                )),
                Ok(()) => {
                    let mut queue = state.jobs.lock().await;
                    if state.jobs_refused.read().await.contains(&job_id) {
                        Err(ApiError::new(
                            ErrorCode::BadRequest,
                            format!(
                                "the id {job_id} was admitted by a previous run: choose a new id"
                            ),
                        ))
                    } else {
                        match queue.submit(
                            PendingJob {
                                id: job_id.clone(),
                                reservation_bytes: estimate,
                            },
                            true,
                        ) {
                            Ok(()) => {
                                state.job_specs.lock().await.insert(
                                    job_id.clone(),
                                    JobSpec {
                                        argv: argv.clone(),
                                        cwd: cwd.clone(),
                                        env: env.clone(),
                                    },
                                );
                                let payload = jobs_report(state, &queue).await;
                                let record = PersistedAdmissions::from_queue(
                                    &queue,
                                    MAX_PERSISTED_ADMISSIONS,
                                )
                                .encode();
                                let empty =
                                    queue.pending().count() == 0 && queue.admitted().count() == 0;
                                drop(queue);
                                persist_jobs(state, record, empty).await;
                                Ok(ResponseBody::Jobs(Box::new(payload)))
                            }
                            Err(err) => Err(ApiError::new(
                                ErrorCode::BadRequest,
                                format!("the job was not queued: {err:?}"),
                            )),
                        }
                    }
                }
            };
            send_result(write, message.id, result).await
        }
        RequestBody::LaunchJob { job_id } => match launch_job(state, &job_id).await {
            Ok(launch) => {
                send_message(
                    write,
                    message.id,
                    ResponseBody::Jobs(Box::new(launch.payload)),
                )
                .await?;
                Ok(Action::LaunchJob {
                    id: launch.id,
                    spec: launch.spec,
                    reservation: launch.reservation,
                    scope_unit: launch.scope_unit,
                    cancel_rx: launch.cancel_rx,
                })
            }
            Err(err) => send_result(write, message.id, Err(err)).await,
        },
        RequestBody::CancelJob { job_id } => {
            // A running job stops through its supervisor; a pending job leaves
            // the queue. The report that follows describes the queue after the
            // change in both cases.
            // Bind the removal first: a guard held by an if-let scrutinee
            // lives through the body, which would deadlock the report below
            // when it locks the same map.
            let handle = state.job_running.lock().await.remove(&job_id);
            if let Some(handle) = handle {
                let _ = handle.cancel.send(());
                let queue = state.jobs.lock().await;
                let payload = jobs_report(state, &queue).await;
                send_message(write, message.id, ResponseBody::Jobs(Box::new(payload))).await?;
                return Ok(Action::Continue);
            }
            let mut queue = state.jobs.lock().await;
            let result = if queue.cancel(&job_id) {
                state.job_specs.lock().await.remove(&job_id);
                let payload = jobs_report(state, &queue).await;
                let record =
                    PersistedAdmissions::from_queue(&queue, MAX_PERSISTED_ADMISSIONS).encode();
                let empty = queue.pending().count() == 0 && queue.admitted().count() == 0;
                drop(queue);
                persist_jobs(state, record, empty).await;
                Ok(ResponseBody::Jobs(Box::new(payload)))
            } else {
                Err(ApiError::new(
                    ErrorCode::BadRequest,
                    format!("no queued job named {job_id}: only pending jobs can be cancelled"),
                ))
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
        workloads: published
            .workloads
            .iter()
            .map(|workload| workload_summary(workload, &published.priorities))
            .collect(),
        capabilities: published.inventory_capabilities.clone(),
    }
}

/// Try one automatic launch per monitor pass: with auto_launch set, the head
/// of the queue starts as soon as pressure and budget permit. Only the head
/// is attempted, so a younger job never overtakes an older one, and an
/// over-budget head simply waits for completions. Routine refusals stay at
/// debug level; only supervision startup failures are warned about by the
/// paths they already travel.
async fn auto_launch(state: &Arc<SharedState>) {
    let head = {
        let queue = state.jobs.lock().await;
        let mut pending = queue.pending();
        let head = pending.next().map(|job| job.id.clone());
        drop(pending);
        head
    };
    let Some(id) = head else {
        return;
    };
    match launch_job(state, &id).await {
        Ok(launch) => {
            let Launch {
                id,
                spec,
                reservation,
                scope_unit,
                cancel_rx,
                ..
            } = launch;
            tokio::spawn(supervise_job(
                Arc::clone(state),
                id,
                spec,
                reservation,
                scope_unit,
                cancel_rx,
            ));
            let queue = state.jobs.lock().await;
            let record = PersistedAdmissions::from_queue(&queue, MAX_PERSISTED_ADMISSIONS).encode();
            let empty = queue.pending().count() == 0 && queue.admitted().count() == 0;
            drop(queue);
            persist_jobs(state, record, empty).await;
        }
        Err(err) => {
            tracing::debug!(error = %err, "automatic launch held the queue head");
        }
    }
}

/// Present the background job queue for the wire.
///
/// Reads no lock the caller holds: every arm reads configuration and
/// observation first, then locks the queue, then reports.
async fn jobs_report(state: &SharedState, queue: &AdmissionQueue) -> JobsPayload {
    let config = state.config.read().await.clone();
    let published = state.published.read().await.clone();
    let refused = state.jobs_refused.read().await.clone();
    let running = state
        .job_running
        .lock()
        .await
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    let finished = state.job_finished.lock().await.clone();
    jobs_payload(&config, queue, &published, &refused, &running, &finished)
}

/// What a launch admitted: everything the connection loop needs to start
/// supervision and remember it.
struct Launch {
    /// The job id.
    id: String,
    /// What to execute. Memory only, never persisted.
    spec: JobSpec,
    /// The admitted reservation in bytes.
    reservation: u64,
    /// The scope unit the job runs in.
    scope_unit: String,
    /// The supervisor's cancellation receiver.
    cancel_rx: tokio::sync::oneshot::Receiver<()>,
    /// The queue report after admission.
    payload: JobsPayload,
}

/// Admit the oldest pending job when it is the requested one, and prepare its
/// supervision.
///
/// Admission stays strict FIFO: only the head of the queue can launch, so a
/// younger job never overtakes an older one. Launches arrive explicitly or
/// from the opted-in automatic loop; either way the daemon supervises exactly
/// what an operator asked it to run. The caller spawns supervision first and
/// persists second, so a completion that already ran is never overwritten by
/// a stale record.
async fn launch_job(state: &SharedState, job_id: &str) -> Result<Launch, ApiError> {
    let config = state.config.read().await.clone();
    if !config.jobs.enabled {
        return Err(ApiError::new(
            ErrorCode::Unavailable,
            "background job admission is disabled: set [jobs] enabled to use it",
        ));
    }
    if !have_job_supervision() {
        return Err(ApiError::new(
            ErrorCode::Unavailable,
            "launch needs a systemd user session: systemd-run is unavailable or XDG_RUNTIME_DIR is unset",
        ));
    }
    let spec = state
        .job_specs
        .lock()
        .await
        .get(job_id)
        .cloned()
        .ok_or_else(|| {
            ApiError::new(
                ErrorCode::BadRequest,
                format!("no queued job named {job_id}: submit it before launching"),
            )
        })?;
    if let Err(reason) = check_launch_paths(&spec.argv, &spec.cwd) {
        return Err(ApiError::new(
            ErrorCode::BadRequest,
            format!("the job cannot launch: {reason}"),
        ));
    }
    let scope_unit = job_scope_unit(job_id);
    if find_scope_dir(Path::new(CGROUP_ROOT), &scope_unit).is_some() {
        return Err(ApiError::new(
            ErrorCode::BadRequest,
            format!("a scope for {job_id} already exists: it outlived a previous run"),
        ));
    }
    let published = state.published.read().await.clone();
    let healthy = published.snapshot.is_some() && published.pressure == PressureState::Healthy;
    if !healthy {
        return Err(ApiError::new(
            ErrorCode::Unavailable,
            format!(
                "pressure is {}: launch is refused until the host is healthy",
                pressure_state_name(published.pressure)
            ),
        ));
    }
    let mut queue = state.jobs.lock().await;
    let head_is_mine = queue
        .pending()
        .next()
        .map(|job| job.id == job_id)
        .unwrap_or(false);
    if !head_is_mine {
        let ahead = queue
            .pending()
            .next()
            .map(|job| job.id.clone())
            .unwrap_or_default();
        return Err(ApiError::new(
            ErrorCode::BadRequest,
            if ahead.is_empty() {
                format!("no queued job named {job_id}: submit it before launching")
            } else {
                format!("{ahead} is ahead of {job_id}: the queue launches in order")
            },
        ));
    }
    let admitted = queue
        .admit(Some(config.jobs.total_budget_bytes), true)
        .filter(|job| job.id == job_id)
        .map(|job| job.reservation_bytes);
    let Some(reservation) = admitted else {
        let remaining = config
            .jobs
            .total_budget_bytes
            .saturating_sub(queue.reserved_bytes());
        return Err(ApiError::new(
            ErrorCode::Unavailable,
            format!(
                "the reservation does not fit the remaining budget ({remaining} bytes left): \
                 cancel it or raise [jobs] total_budget_bytes"
            ),
        ));
    };
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
    state
        .job_running
        .lock()
        .await
        .insert(job_id.to_string(), JobCancel { cancel: cancel_tx });
    let payload = jobs_report(state, &queue).await;
    drop(queue);
    Ok(Launch {
        id: job_id.to_string(),
        spec,
        reservation,
        scope_unit,
        cancel_rx,
        payload,
    })
}

/// How a supervised job ended.
enum Supervised {
    /// The waiter observed an exit status.
    Exited(std::process::ExitStatus),
    /// The waiter itself failed before any status arrived.
    Failed(String),
    /// The operator cancelled it.
    Cancelled,
}

/// Run one admitted job to completion inside a transient user scope.
///
/// The scope bounds memory at the reservation and cleans up when it empties;
/// the program runs directly with no shell. Standard streams are discarded:
/// output is data the queue must never hold.
async fn supervise_job(
    state: Arc<SharedState>,
    id: String,
    spec: JobSpec,
    reservation: u64,
    scope_unit: String,
    mut cancel: tokio::sync::oneshot::Receiver<()>,
) {
    let mut command = tokio::process::Command::new("systemd-run");
    // All systemd-run options precede the `--` separator: anything after it
    // is the job's own command line, passed verbatim with no shell.
    command.args([
        "--user",
        "--scope",
        "--quiet",
        "--collect",
        &format!("--unit={scope_unit}"),
        "-p",
        &format!("MemoryMax={reservation}"),
        "-p",
        &format!("MemorySwapMax={reservation}"),
    ]);
    if let Some(dir) = &spec.cwd {
        command.arg(format!("--working-directory={dir}"));
    }
    command.arg("--").args(&spec.argv);
    for entry in &spec.env {
        if let Some((key, value)) = entry.split_once('=') {
            command.env(key, value);
        }
    }
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => {
            finish_job(
                &state,
                &id,
                None,
                format!("the job failed to start: {err}"),
                None,
            )
            .await;
            return;
        }
    };
    let mut peak = 0u64;
    let outcome = loop {
        tokio::select! {
            status = child.wait() => break match status {
                Ok(exit) => Supervised::Exited(exit),
                Err(err) => Supervised::Failed(err.to_string()),
            },
            _ = &mut cancel => break Supervised::Cancelled,
            _ = tokio::time::sleep(JOB_PEAK_INTERVAL) => {
                if let Some(sample) = read_scope_peak(Path::new(CGROUP_ROOT), &scope_unit) {
                    peak = peak.max(sample);
                }
            }
        }
    };
    match outcome {
        Supervised::Cancelled => {
            // Stop the scope gracefully, then forcefully: the waiter alone
            // cannot stop the scope's members.
            kill_scope_unit(&scope_unit, "TERM");
            let deadline = Instant::now() + JOB_STOP_GRACE;
            while find_scope_dir(Path::new(CGROUP_ROOT), &scope_unit).is_some()
                && Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            if find_scope_dir(Path::new(CGROUP_ROOT), &scope_unit).is_some() {
                kill_scope_unit(&scope_unit, "KILL");
            }
            let _ = child.kill().await;
            let _ = child.wait().await;
            finish_job(
                &state,
                &id,
                None,
                "cancelled by the operator".to_string(),
                Some(peak),
            )
            .await;
        }
        Supervised::Exited(status) => {
            if let Some(sample) = read_scope_peak(Path::new(CGROUP_ROOT), &scope_unit) {
                peak = peak.max(sample);
            }
            let (code, note) = exit_note(&status);
            finish_job(&state, &id, code, note, Some(peak)).await;
        }
        Supervised::Failed(err) => {
            finish_job(
                &state,
                &id,
                None,
                format!("the supervisor failed before any status arrived: {err}"),
                Some(peak),
            )
            .await;
        }
    }
}

/// Describe an exit status without inventing one.
fn exit_note(status: &std::process::ExitStatus) -> (Option<i32>, String) {
    use std::os::unix::process::ExitStatusExt;
    match status.code() {
        Some(code) => (
            Some(code),
            if code == 0 {
                "exited cleanly".to_string()
            } else {
                format!("exited with code {code}")
            },
        ),
        None => (
            None,
            format!(
                "ended by signal {}",
                status
                    .signal()
                    .map(|signal| signal.to_string())
                    .unwrap_or_else(|| "?".to_string())
            ),
        ),
    }
}

/// Remember a finished job in the bounded list, newest last.
fn push_finished(finished: &mut VecDeque<FinishedJob>, item: FinishedJob) {
    finished.push_back(item);
    while finished.len() > MAX_FINISHED_JOBS {
        finished.pop_front();
    }
}

/// Release a job's reservation, remember how it ended, and persist the queue.
///
/// Runs once per job, from the supervisor that owned it.
async fn finish_job(
    state: &Arc<SharedState>,
    id: &str,
    exit_code: Option<i32>,
    note: String,
    peak_bytes: Option<u64>,
) {
    let mut queue = state.jobs.lock().await;
    if let Some(peak) = peak_bytes {
        queue.observe_usage(id, peak);
    }
    queue.complete(id);
    state.job_specs.lock().await.remove(id);
    state.job_running.lock().await.remove(id);
    {
        let mut finished = state.job_finished.lock().await;
        push_finished(
            &mut finished,
            FinishedJob {
                id: id.to_string(),
                exit_code,
                note,
                peak_bytes,
            },
        );
    }
    let record = PersistedAdmissions::from_queue(&queue, MAX_PERSISTED_ADMISSIONS).encode();
    let empty = queue.pending().count() == 0 && queue.admitted().count() == 0;
    drop(queue);
    persist_jobs(state, record, empty).await;
}

/// Remember the queue beside the journal state file, when there is somewhere
/// to put it. A restart refuses the record rather than re-admitting it.
///
/// An empty queue removes the record instead of writing one, so submitting and
/// then cancelling every job leaves no file behind: a missing file stays the
/// ordinary first run.
async fn persist_jobs(state: &SharedState, record: String, empty: bool) {
    let Some(path) = jobs_state_path(state.state_path.as_deref()) else {
        return;
    };
    let written = tokio::task::spawn_blocking(move || {
        if empty {
            let _ = std::fs::remove_file(&path);
            return Ok(());
        }
        write_state_file(&path, &record)
    })
    .await;
    if let Ok(Err(err)) = written {
        tracing::warn!(error = %err, "the job queue could not be written");
    }
}

/// Check a submit's execution fields before anything is queued.
///
/// Arguments, the directory, and the environment are validated here so a job
/// that could never launch is refused at submit time rather than at launch.
fn check_submit_spec(argv: &[String], cwd: &Option<String>, env: &[String]) -> Result<(), String> {
    if argv.is_empty() {
        return Err(
            "submit needs a program: pass arguments after --, for example -- /usr/bin/make -j4"
                .to_string(),
        );
    }
    if argv.len() > MAX_JOB_ARGC {
        return Err(format!("at most {MAX_JOB_ARGC} arguments are accepted"));
    }
    for arg in argv {
        if arg.len() > MAX_JOB_ARG_BYTES {
            return Err(format!("an argument exceeds {MAX_JOB_ARG_BYTES} bytes"));
        }
        if arg.contains('\0') {
            return Err("arguments must not contain NUL bytes".to_string());
        }
    }
    if !argv[0].starts_with('/') {
        return Err(format!(
            "the program must be an absolute path, not {}: no PATH lookup is performed",
            argv[0]
        ));
    }
    if let Some(dir) = cwd {
        if dir.is_empty() {
            return Err("the working directory must not be empty".to_string());
        }
        if dir.contains('\0') {
            return Err("the working directory must not contain NUL bytes".to_string());
        }
    }
    for entry in env {
        let (key, _) = entry
            .split_once('=')
            .ok_or_else(|| format!("environment entry {entry:?} must be KEY=VALUE"))?;
        if key.is_empty() || key.contains('\0') {
            return Err(format!("environment entry {entry:?} has no usable key"));
        }
    }
    Ok(())
}

/// Check the launch-time facts a submit cannot prove: the program exists and
/// is executable, and the working directory exists and is a directory.
fn check_launch_paths(argv: &[String], cwd: &Option<String>) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    let program = &argv[0];
    let metadata = std::fs::metadata(program)
        .map_err(|_| format!("the program {program} could not be read"))?;
    if !metadata.is_file() {
        return Err(format!("the program {program} is not a file"));
    }
    if metadata.permissions().mode() & 0o111 == 0 {
        return Err(format!("the program {program} is not executable"));
    }
    if let Some(dir) = cwd {
        let metadata =
            std::fs::metadata(dir).map_err(|_| format!("the directory {dir} could not be read"))?;
        if !metadata.is_dir() {
            return Err(format!("the working directory {dir} is not a directory"));
        }
    }
    Ok(())
}

/// Whether a systemd user session can supervise a job here.
fn have_job_supervision() -> bool {
    if std::env::var_os("XDG_RUNTIME_DIR")
        .map(|dir| dir.is_empty())
        .unwrap_or(true)
    {
        return false;
    }
    std::process::Command::new("systemd-run")
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

/// The scope unit name for a job id. Ids are restricted to unit-safe bytes at
/// submit time, so no further sanitizing is needed.
fn job_scope_unit(id: &str) -> String {
    format!("{JOB_SCOPE_PREFIX}{id}.scope")
}

/// Find a scope unit's cgroup directory under a hierarchy root.
fn find_scope_dir(root: &Path, unit: &str) -> Option<PathBuf> {
    fn walk(dir: &Path, unit: &str, depth: usize, found: &mut Option<PathBuf>) {
        if found.is_some() || depth == 0 {
            return;
        }
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.file_name().and_then(|name| name.to_str()) == Some(unit) {
                *found = Some(path);
                return;
            }
            if path.is_dir() {
                walk(&path, unit, depth - 1, found);
            }
        }
    }
    let mut found = None;
    walk(root, unit, 8, &mut found);
    found
}

/// Read a scope's peak memory in bytes, when the scope still exists.
fn read_scope_peak(root: &Path, unit: &str) -> Option<u64> {
    let dir = find_scope_dir(root, unit)?;
    let text = std::fs::read_to_string(dir.join("memory.peak")).ok()?;
    text.trim().parse().ok()
}

/// Signal every process in a scope unit, best effort.
fn kill_scope_unit(unit: &str, signal: &str) {
    let _ = std::process::Command::new("systemctl")
        .args(["--user", "kill", unit, &format!("--signal={signal}")])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
}

/// The job queue record beside the journal state file, when there is somewhere
/// to put it.
fn jobs_state_path(state_path: Option<&Path>) -> Option<PathBuf> {
    let path = state_path?;
    Some(path.parent()?.join("jobs.json"))
}

/// Read the admission record a previous run left behind.
///
/// A missing file is the ordinary first run. Anything recorded is reported as
/// refused rather than re-admitted: this run cannot prove a recorded process
/// is still its own.
fn load_refused_admissions(path: Option<&Path>) -> (Vec<String>, Vec<String>) {
    let Some(path) = jobs_state_path(path) else {
        return (
            Vec::new(),
            vec!["no state directory is available, so the job queue cannot be remembered across a restart".to_string()],
        );
    };
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return (Vec::new(), Vec::new()),
        Err(err) => {
            return (
                Vec::new(),
                vec![format!(
                    "the job queue file at {} could not be read, so no previous admissions were refused: {err}",
                    path.display()
                )],
            )
        }
    };
    match PersistedAdmissions::decode(&text) {
        Some(record) => (
            record.refused().iter().map(|id| id.to_string()).collect(),
            Vec::new(),
        ),
        None => (
            Vec::new(),
            vec![format!(
                "the job queue file at {} was not understood, so no previous admissions were refused",
                path.display()
            )],
        ),
    }
}
/// Present one queue for the wire, with its admission readiness.
///
/// Readiness is pressure plus observation: an observation must exist and
/// pressure must be healthy, or admission is held. Unknown or unhealthy
/// pressure refuses all new admissions, so the report says so instead of
/// leaving an operator to guess why nothing is admitted.
fn jobs_payload(
    config: &Config,
    queue: &AdmissionQueue,
    published: &Published,
    refused: &[String],
    running: &[String],
    finished: &VecDeque<FinishedJob>,
) -> JobsPayload {
    let mut notes = Vec::new();
    if !config.jobs.enabled {
        notes.push(
            "background job admission is disabled: the queue accepts nothing \
             until [jobs] enabled is set"
                .to_string(),
        );
    }
    if !refused.is_empty() {
        notes.push(format!(
            "a previous run admitted {} job(s); this run refuses to reuse those ids: {}",
            refused.len(),
            refused.join(", ")
        ));
    }
    let (admission_ready, readiness_note) = match &published.snapshot {
        None => (
            false,
            Some("no observation yet: admission is held".to_string()),
        ),
        Some(_) if published.pressure != PressureState::Healthy => (
            false,
            Some(format!(
                "pressure is {}: admission is held",
                pressure_state_name(published.pressure)
            )),
        ),
        Some(_) => (true, None),
    };
    if queue.pending().count() > 0 || queue.admitted().count() > 0 {
        notes.push(
            "reservations are metadata only: launch specifications live in \
             daemon memory, and a restart does not reconstruct running work"
                .to_string(),
        );
    }
    JobsPayload {
        enabled: config.jobs.enabled,
        total_budget_bytes: config.jobs.total_budget_bytes,
        reserved_bytes: queue.reserved_bytes(),
        pending: queue
            .pending()
            .map(|job| PendingJobView {
                id: job.id.clone(),
                reservation_bytes: job.reservation_bytes,
            })
            .collect(),
        admitted: queue
            .admitted()
            .map(|(id, reservation_bytes)| AdmittedJobView {
                id: id.to_string(),
                reservation_bytes,
            })
            .collect(),
        running: queue
            .admitted()
            .filter(|(id, _)| running.iter().any(|live| live == id))
            .map(|(id, reservation_bytes)| RunningJobView {
                id: id.to_string(),
                reservation_bytes,
            })
            .collect(),
        finished: finished
            .iter()
            .map(|done| FinishedJobView {
                id: done.id.clone(),
                exit_code: done.exit_code,
                note: done.note.clone(),
                peak_bytes: done.peak_bytes,
            })
            .collect(),
        admission_ready,
        readiness_note,
        notes,
    }
}

fn workload_summary(workload: &Workload, priorities: &[WorkloadPriority]) -> WorkloadSummary {
    let (cgroup_bytes, rss_bytes) = match &workload.accounting {
        MemoryAccounting::Cgroup { current_bytes } => (*current_bytes, None),
        MemoryAccounting::RssEstimate { rss_bytes, .. } => (None, Some(*rss_bytes)),
    };
    let (priority, priority_source) = priorities
        .iter()
        .find(|entry| entry.scope == workload.id.scope)
        .map_or((Priority::Normal, String::new()), |entry| {
            (entry.priority, entry.source.clone())
        });
    WorkloadSummary {
        scope: workload.id.scope.clone(),
        generation: workload.id.generation,
        name: workload.name.clone(),
        processes: workload.pids.len(),
        cgroup_bytes,
        rss_bytes,
        estimated: workload.accounting.is_estimate(),
        control: control_level_name(workload.control).to_string(),
        priority,
        priority_source,
        limit_bytes: workload.limit_bytes,
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
    mode: ManagementMode,
) -> Result<ActionsPayload, ApiError> {
    let plan = plan_payload(published)?;
    // Actions and restores only: a value recovered from a previous run is
    // breadram's to restore, but it is not something this run did.
    let history: Vec<ActionJournalRecord> = journal
        .history()
        .rev()
        .map(ActionJournalRecord::from_entry)
        .collect();
    // In automatic mode an empty history means nothing needed doing, not that
    // nothing runs, so no note is added: a note here would be a false reason.
    let history_note =
        (mode != ManagementMode::Automatic).then(|| OBSERVATION_ONLY_NOTE.to_string());
    Ok(ActionsPayload {
        plan,
        history,
        history_capacity: journal.capacity(),
        history_note,
    })
}

/// Collection reuses the inventory snapshot and never performs another procfs read.
fn collect_trends(
    history: &mut TrendHistory,
    published: &Published,
    pass: Option<&ExecutionPass>,
    config: &Config,
    keys: &std::collections::hash_map::RandomState,
) {
    use std::hash::BuildHasher;
    let now = published.status.observed_at_ms;
    // Reserve a conservative wire budget per record to keep diagnostics bounded.
    // The normal diagnostics response also needs room for capabilities and notes.
    let capacity = (config.history.max_records as usize)
        .min((config.history.max_bytes / 256) as usize)
        .min(MAX_MESSAGE_BYTES / 1024);
    history.configure(
        capacity,
        u64::from(config.history.max_age_days) * 86_400_000,
        now,
    );
    // Host available memory and the pressure level, once per pass, so the series
    // the card asks for exist alongside workload memory.
    history.record(TrendSample {
        at_ms: now,
        target_key: keys.hash_one("host"),
        generation: 0,
        bytes: published.status.available_bytes,
        measured: true,
        kind: KIND_HOST,
        delta: None,
    });
    history.record(TrendSample {
        at_ms: now,
        target_key: keys.hash_one("pressure"),
        generation: 0,
        bytes: Some(pressure_level(published.pressure)),
        measured: true,
        kind: KIND_PRESSURE,
        delta: None,
    });
    for workload in &published.workloads {
        history.record(TrendSample {
            at_ms: now,
            target_key: keys.hash_one(&workload.id.scope),
            generation: workload.id.generation,
            bytes: workload.accounting.bytes(),
            measured: !workload.accounting.is_estimate(),
            kind: KIND_WORKLOAD,
            delta: None,
        });
    }
    // A measured action effect becomes a recovery sample on the same opaque key
    // as its target, so the following workload samples can be read as relief
    // rather than mistaken for growth.
    if let Some(pass) = pass {
        for result in &pass.results {
            let Some(delta) = result
                .effect
                .as_ref()
                .and_then(|effect| effect.observed_host_ram_delta)
            else {
                continue;
            };
            history.record(TrendSample {
                at_ms: now,
                target_key: keys.hash_one(&result.scope),
                generation: 0,
                bytes: None,
                measured: true,
                kind: KIND_RECOVERY,
                delta: Some(delta),
            });
        }
    }
}

/// The pressure state as an ordered level, for a numeric-only series.
fn pressure_level(state: PressureState) -> u64 {
    match state {
        PressureState::Healthy => 0,
        PressureState::Recovering => 1,
        PressureState::Pressured => 2,
        PressureState::Critical => 3,
    }
}

fn diagnostics_payload(
    state: &SharedState,
    published: &Published,
    config: &Config,
    journal: &ActionJournal,
    execution_notes: &[String],
    trends: Vec<TrendSample>,
    growth: Vec<breadram_core::trends::GrowthReport>,
) -> DiagnosticsPayload {
    let mut notes = Vec::new();
    match config.mode {
        ManagementMode::Automatic => notes.push(
            "breadramd is in automatic mode: a plan is carried out on each pass, within the allowed actions"
                .to_string(),
        ),
        other => notes.push(format!("{other} mode never writes: {OBSERVATION_ONLY_NOTE}")),
    }
    notes.extend(execution_notes.iter().cloned());
    if config.automation.allowed_actions.is_empty() {
        notes.push(
            "automation allows no actions, so automatic mode would never act on this host"
                .to_string(),
        );
    }
    // The swap and OOM constraints are part of the same explanation: what relief
    // costs here, and which manager already owns OOM.
    notes.extend(published.swap.notes());
    for workload in &published.workload_swap {
        notes.push(workload.swap.explain(&workload.scope));
    }
    DiagnosticsPayload {
        trends,
        growth,
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
            entries: journal.history_len(),
            capacity: journal.capacity(),
            executing: config.mode == ManagementMode::Automatic,
        },
        swap: published.swap.clone(),
        workload_swap: published.workload_swap.clone(),
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
    // Integrated GPU shared memory is its own capability, so a missing iGPU is a
    // reported gap rather than a silent zero.
    let gtt_state = if published.gtt.used_bytes().is_some() {
        breadram_core::capability::CapabilityState::Available
    } else if published.gtt.present() {
        breadram_core::capability::CapabilityState::Degraded {
            reason: "an integrated GPU reports a GTT total but no used count, so its footprint is unknown"
                .to_string(),
        }
    } else if published.gtt.drm_readable {
        breadram_core::capability::CapabilityState::Unavailable {
            reason: "no integrated GPU shared memory counter was found".to_string(),
        }
    } else {
        breadram_core::capability::CapabilityState::Unavailable {
            reason: "the drm class could not be read".to_string(),
        }
    };
    merged.set(CAP_GPU_GTT.to_string(), gtt_state);
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
            gtt_used_bytes: None,
            gtt_total_bytes: None,
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
        .push("dedicated VRAM is not measured: no discrete-GPU adapter reports it yet".to_string());
    notes.extend(published.gtt.notes());
    MemoryBreakdown {
        ram_total_bytes: Some(meminfo.total),
        ram_available_bytes: meminfo.available,
        swap_total_bytes: Some(meminfo.swap_total),
        swap_used_bytes: meminfo.swap_used(),
        shared_bytes: meminfo.shmem,
        vram_bytes: None,
        gtt_used_bytes: published.gtt.used_bytes(),
        gtt_total_bytes: published.gtt.total_bytes(),
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
            let runtime = PolicyRuntime::new(&config, 0);
            let mut hints = HintState::default();
            observe(
                &clock,
                &config,
                &mut machine,
                &mut history,
                &runtime.registry,
                &mut hints,
                Observation {
                    probes: &ProviderProbes::default(),
                    gateway_active: &[],
                },
            )
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

#[cfg(test)]
mod tests {
    use super::*;
    use breadram_core::config::{ActionKind, Automation, Budget};

    /// A directory that removes itself when the test ends.
    ///
    /// A test that writes a daemon state file and leaves the directory behind
    /// leaks one entry into the temporary directory per run.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "breadram-daemon-test-{}-{label}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            std::fs::create_dir_all(&dir).expect("create the test directory");
            Self(dir)
        }

        fn path(&self) -> PathBuf {
            self.0.clone()
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    use breadram_core::coordinator::{
        ActionRequest, AppliedChange, Confirmation, ExecutorError, MemoryObservation, OwnedValue,
        Precondition,
    };
    use breadram_core::inventory::{ControlLevel, WorkloadId};
    use breadram_core::policy::{Plan, PlanChannel, PlannedAction, PressureState};
    use std::sync::atomic::{AtomicUsize, Ordering};

    const GIB: u64 = 1024 * 1024 * 1024;
    const SCOPE: &str = "unit:build.service";

    /// An executor that records what it was asked to do and writes nothing.
    struct FakeExecutor {
        applies: AtomicUsize,
        memory: u64,
    }

    impl FakeExecutor {
        fn new() -> Self {
            Self {
                applies: AtomicUsize::new(0),
                memory: 6 * GIB,
            }
        }

        fn applies(&self) -> usize {
            self.applies.load(Ordering::SeqCst)
        }
    }

    impl Executor for FakeExecutor {
        fn supports(&self, _kind: ActionKind) -> bool {
            true
        }

        fn precondition(&self, _request: &ActionRequest) -> Precondition {
            Precondition::Ready
        }

        fn apply(&self, _request: &ActionRequest) -> Result<AppliedChange, ExecutorError> {
            self.applies.fetch_add(1, Ordering::SeqCst);
            Ok(AppliedChange {
                values: vec![OwnedValue {
                    key: "cgroup:unit:build.service/memory.high".to_string(),
                    original: "max".to_string(),
                    last_applied: (2 * GIB).to_string(),
                    owner: OwnerToken("test".to_string()),
                }],
                note: None,
            })
        }

        fn confirm(&self, _request: &ActionRequest) -> Confirmation {
            Confirmation::Confirmed
        }

        fn observe(&self, _target: &WorkloadId) -> MemoryObservation {
            MemoryObservation {
                host_ram_bytes: Some(self.memory),
                device_ram_bytes: None,
            }
        }

        fn read_value(&self, _key: &str) -> Result<Option<String>, ExecutorError> {
            Ok(Some("max".to_string()))
        }

        fn write_value(&self, _key: &str, _value: &str) -> Result<(), ExecutorError> {
            Err(ExecutorError::Unsupported(
                "the fake writes nothing".to_string(),
            ))
        }
    }

    fn config(allowed: Vec<ActionKind>) -> Config {
        Config {
            mode: ManagementMode::Automatic,
            automation: Automation {
                allowed_actions: allowed,
            },
            budgets: vec![Budget {
                scope: SCOPE.to_string(),
                max_bytes: 2 * GIB,
            }],
            ..Config::default()
        }
    }

    fn workload() -> Workload {
        Workload {
            id: WorkloadId {
                scope: SCOPE.to_string(),
                generation: 7,
            },
            name: SCOPE.to_string(),
            pids: vec![1],
            accounting: MemoryAccounting::Cgroup {
                current_bytes: Some(6 * GIB),
            },
            control: ControlLevel::Eligible,
            limit_bytes: None,
        }
    }

    #[test]
    fn trend_collection_is_bounded_numeric_and_observation_based() {
        let mut published = unavailable(ManagementMode::Monitor, 10);
        published.workloads = vec![workload()];
        published.status.available_bytes = Some(3 * GIB);
        published.pressure = PressureState::Pressured;
        let keys = std::collections::hash_map::RandomState::new();
        let mut history = TrendHistory::new(0, 0);
        let mut config = Config::default();
        config.history.max_records = 64;
        for at in [10, 20, 30] {
            published.status.observed_at_ms = at;
            collect_trends(&mut history, &published, None, &config, &keys);
        }
        let retained = history.retained(30);
        // Each pass records a host sample, a pressure sample, and a workload
        // sample, so the series the card asks for exist alongside workload bytes.
        assert_eq!(retained.len(), 9);
        assert_eq!(retained.iter().filter(|s| s.kind == KIND_HOST).count(), 3);
        assert!(retained
            .iter()
            .filter(|s| s.kind == KIND_PRESSURE)
            .all(|s| s.bytes == Some(pressure_level(PressureState::Pressured))));
        let workload_samples: Vec<_> = retained
            .iter()
            .filter(|s| s.kind == KIND_WORKLOAD)
            .collect();
        assert_eq!(workload_samples.len(), 3);
        assert_eq!(workload_samples[2].bytes, Some(6 * GIB));
        assert!(workload_samples[2].measured);
        // The growth judgement is reachable, and paired with recorded relief.
        let report = history.report(30);
        assert_eq!(report.len(), 1);
        assert_eq!(report[0].recorded_relief_bytes, 0);
        let export = history.export_json(30).unwrap();
        assert!(!export.contains(SCOPE));
        // The wire budget still bounds retention: a zero byte budget keeps nothing.
        config.history.max_bytes = 0;
        collect_trends(&mut history, &published, None, &config, &keys);
        assert!(history.retained(30).is_empty());
    }

    #[test]
    fn trends_expire_and_a_reused_target_does_not_join_an_old_generation() {
        let keys = std::collections::hash_map::RandomState::new();
        let mut config = Config::default();
        config.history.max_records = 64;
        // Zero age means anything not sampled exactly now has expired, which is
        // how a restart-that-never-joined-a-timeline shows up.
        config.history.max_age_days = 0;
        let mut history = TrendHistory::new(0, 0);
        let mut published = unavailable(ManagementMode::Monitor, 10);
        published.workloads = vec![workload()];
        published.status.observed_at_ms = 10;
        collect_trends(&mut history, &published, None, &config, &keys);
        assert!(history
            .retained(10)
            .iter()
            .any(|sample| sample.kind == KIND_WORKLOAD && sample.generation == 7));

        // A reused target appears with a new generation, and the old series has
        // expired rather than being joined.
        published.workloads[0].id.generation = 99;
        published.status.observed_at_ms = 20;
        collect_trends(&mut history, &published, None, &config, &keys);
        let workloads: Vec<_> = history
            .retained(20)
            .into_iter()
            .filter(|sample| sample.kind == KIND_WORKLOAD)
            .collect();
        assert!(!workloads.is_empty());
        assert!(workloads.iter().all(|sample| sample.generation == 99));
    }

    fn plan() -> Plan {
        Plan {
            state: PressureState::Pressured,
            channel: PlanChannel::Automatic,
            would_execute: true,
            actions: vec![PlannedAction {
                target: workload().id,
                kind: ActionKind::CgroupSoftLimit,
                reason: "the workload is over its budget".to_string(),
                expected_host_ram_delta: -(2 * GIB as i64),
                requires_confirmed_idle: false,
            }],
            explanation: Vec::new(),
        }
    }

    fn coordinator() -> Coordinator {
        Coordinator::new(OwnerToken("test".to_string()), 32)
    }

    #[test]
    fn an_automatic_pass_writes_and_journals_the_measured_effect() {
        let executor = FakeExecutor::new();
        let mut coordinator = coordinator();
        let settings = config(vec![ActionKind::CgroupSoftLimit]);

        let pass = execute_with(
            &mut coordinator,
            &settings,
            &plan(),
            &[workload()],
            &executor,
            1_000,
            &Cancellation::new(),
            64 * 1024 * 1024 * 1024,
        );

        assert_eq!(executor.applies(), 1);
        assert_eq!(pass.confirmed(), 1, "{}", pass.summary());
        let entry = &coordinator.journal().entries()[0];
        assert_eq!(entry.scope, SCOPE);
        assert_eq!(entry.at_ms, 1_000);
        assert_eq!(entry.reason, "the workload is over its budget");
        assert_eq!(
            entry.state,
            breadram_core::coordinator::ActionState::Confirmed
        );
        assert_eq!(
            entry
                .effect
                .as_ref()
                .and_then(|effect| effect.observed_host_ram_delta),
            Some(0),
            "the effect is measured from the executor's own readings"
        );
        assert_eq!(
            entry.values[0].key, "cgroup:unit:build.service/memory.high",
            "and the value breadram now owns is recorded, so it can be restored"
        );
    }

    #[test]
    fn a_monitor_mode_pass_writes_nothing_whatever_the_plan_says() {
        let executor = FakeExecutor::new();
        let mut coordinator = coordinator();
        let settings = Config {
            mode: ManagementMode::Monitor,
            ..config(vec![ActionKind::CgroupSoftLimit])
        };

        let pass = execute_with(
            &mut coordinator,
            &settings,
            &plan(),
            &[workload()],
            &executor,
            1_000,
            &Cancellation::new(),
            64 * 1024 * 1024 * 1024,
        );

        assert_eq!(executor.applies(), 0, "monitor mode must not write");
        assert!(coordinator.journal().is_empty());
        assert!(!pass.wrote());
        assert!(pass.notes[0].contains("never writes"), "{:?}", pass.notes);
    }

    #[test]
    fn a_pass_refuses_what_the_configuration_does_not_allow() {
        let executor = FakeExecutor::new();
        let mut coordinator = coordinator();
        let settings = config(Vec::new());

        let pass = execute_with(
            &mut coordinator,
            &settings,
            &plan(),
            &[workload()],
            &executor,
            1_000,
            &Cancellation::new(),
            64 * 1024 * 1024 * 1024,
        );

        assert_eq!(executor.applies(), 0);
        assert!(coordinator.journal().is_empty());
        assert!(
            pass.notes
                .iter()
                .any(|note| note.contains("automation does not allow it")),
            "{:?}",
            pass.notes
        );
    }

    #[test]
    fn a_second_pass_does_not_apply_the_same_action_twice() {
        let executor = FakeExecutor::new();
        let mut coordinator = coordinator();
        let settings = config(vec![ActionKind::CgroupSoftLimit]);

        for _ in 0..3 {
            execute_with(
                &mut coordinator,
                &settings,
                &plan(),
                &[workload()],
                &executor,
                1_000,
                &Cancellation::new(),
                64 * 1024 * 1024 * 1024,
            );
        }

        assert_eq!(executor.applies(), 1, "a confirmed action is not repeated");
        assert_eq!(coordinator.journal().len(), 3);
    }

    #[test]
    fn a_configured_provider_is_read_by_the_daemon_and_its_failure_is_reported() {
        use breadram_core::config::{ProviderEndpoint, ProviderKind};

        // A provider on a port nothing is listening on, so the daemon's own read
        // path is exercised rather than a fake.
        let settings = Config {
            providers: vec![ProviderEndpoint {
                name: "ollama".to_string(),
                kind: ProviderKind::Ollama,
                url: "http://127.0.0.1:9".to_string(),
                scope: Some(SCOPE.to_string()),
                credential_ref: None,
                allow_automation: true,
                igpu: false,
                command: Vec::new(),
                hook_timeout_ms: None,
                hook_max_output_bytes: None,
            }],
            ..Config::default()
        };
        let clock = SystemClock::new();
        let (registry, attached) = AdapterRegistry::from_config(&settings);
        assert_eq!(registry.names(), vec!["ollama"]);
        assert!(attached.is_empty(), "{attached:?}");

        let probes = ProviderProbes::default();
        let notes = refresh_adapters(&registry, &probes, &clock);
        assert_eq!(notes.len(), 2, "both reads are reported: {notes:?}");
        assert!(
            notes
                .iter()
                .all(|note| note.contains("could not be reached")),
            "{notes:?}"
        );

        // With no provider configured, nothing is asked and nothing is claimed.
        let (empty, _) = AdapterRegistry::from_config(&Config::default());
        assert!(empty.is_empty());
        assert!(refresh_adapters(&empty, &probes, &clock).is_empty());
    }

    #[test]
    fn the_daemon_reports_the_swap_layout_of_this_host_and_never_changes_it() {
        // The collection path the daemon uses, on this host rather than a
        // fixture: it must not panic, it must always explain itself, and the
        // promise that the layout is left alone must be in the report.
        let swap = collect_swap(&RealFs);
        let notes = swap.notes();
        assert!(!notes.is_empty());
        assert!(
            notes
                .iter()
                .any(|note| note.contains("never changes global swappiness")),
            "{notes:?}"
        );
    }

    #[test]
    fn integrated_gpu_memory_is_reported_separately_and_never_folded_into_host_ram() {
        // The GTT figure is copied into the breakdown, never added to or
        // subtracted from host available memory: on an iGPU it is a carve-out of
        // the same host RAM, so folding it in would count it twice.
        let mut published = unavailable(ManagementMode::Monitor, 1);
        published.snapshot = collect_host(&RealFs, &SystemClock::new()).ok();
        published.gtt = collect_gtt(&RealFs);
        let breakdown = memory_breakdown(&published);

        assert_eq!(breakdown.gtt_used_bytes, published.gtt.used_bytes());
        assert_eq!(breakdown.gtt_total_bytes, published.gtt.total_bytes());
        if let Some(snapshot) = &published.snapshot {
            assert_eq!(
                breakdown.ram_available_bytes, snapshot.meminfo.available,
                "host available memory is untouched by the GTT figure"
            );
        }
        assert!(breakdown
            .notes
            .iter()
            .any(|note| note.contains("GTT") || note.contains("integrated GPU")));
    }

    #[test]
    fn the_owned_values_survive_a_state_file_and_a_missing_one_is_not_an_error() {
        let directory = TempDir::new("state-file");
        let path = directory.path().join("journal.json");

        // A first run has no state file, which is not a problem to report.
        let (persisted, notes) = load_persisted(Some(&path));
        assert!(persisted.is_empty());
        assert!(notes.is_empty(), "{notes:?}");

        // After a write, the values come back with their originals.
        let executor = FakeExecutor::new();
        let mut coordinator = coordinator();
        let settings = config(vec![ActionKind::CgroupSoftLimit]);
        execute_with(
            &mut coordinator,
            &settings,
            &plan(),
            &[workload()],
            &executor,
            1_000,
            &Cancellation::new(),
            64 * 1024 * 1024 * 1024,
        );
        let stored = PersistedJournal::from_journal(coordinator.journal());
        write_state_file(&path, &stored.encode().expect("encodes")).expect("writes");
        let (reloaded, notes) = load_persisted(Some(&path));
        assert!(notes.is_empty(), "{notes:?}");
        assert_eq!(reloaded, stored);
        let resumed = reloaded.to_journal(32);
        assert_eq!(resumed.owned_values().len(), 1);
        assert_eq!(resumed.owned_values()[0].1.original, "max");

        // A file this build cannot read is reported and ignored.
        std::fs::write(&path, "{ not json").expect("writes");
        let (unreadable, notes) = load_persisted(Some(&path));
        assert!(unreadable.is_empty());
        assert_eq!(notes.len(), 1);
        assert!(notes[0].contains("not understood"), "{notes:?}");
        // The directory removes itself when this test ends.
    }

    #[test]
    fn a_daemon_with_no_state_directory_says_so() {
        let (persisted, notes) = load_persisted(None);
        assert!(persisted.is_empty());
        assert!(notes[0].contains("no state directory"), "{notes:?}");
    }

    #[test]
    fn the_job_queue_record_survives_a_state_file_and_refusals_are_reported() {
        let directory = TempDir::new("jobs-state-file");
        let journal = directory.path().join("journal.json");

        // The record lives beside the journal state file.
        assert_eq!(
            jobs_state_path(Some(&journal)),
            Some(directory.path().join("jobs.json"))
        );
        assert_eq!(jobs_state_path(None), None);

        // A first run has no record, which is not a problem to report.
        let (refused, notes) = load_refused_admissions(Some(&journal));
        assert!(refused.is_empty());
        assert!(notes.is_empty(), "{notes:?}");

        // After a write, the admissions come back as refusals, never re-admitted.
        let mut queue = AdmissionQueue::new(8);
        queue
            .submit(
                PendingJob {
                    id: "a".into(),
                    reservation_bytes: 10,
                },
                true,
            )
            .unwrap();
        queue
            .submit(
                PendingJob {
                    id: "b".into(),
                    reservation_bytes: 20,
                },
                true,
            )
            .unwrap();
        assert_eq!(queue.admit(Some(100), true).unwrap().id, "a");
        let record = PersistedAdmissions::from_queue(&queue, MAX_PERSISTED_ADMISSIONS);
        let path = jobs_state_path(Some(&journal)).expect("a path");
        write_state_file(&path, &record.encode()).expect("writes");
        let (refused, notes) = load_refused_admissions(Some(&journal));
        assert!(notes.is_empty(), "{notes:?}");
        assert_eq!(refused, vec!["a".to_string()]);

        // A file this build cannot read is reported and nothing is refused.
        std::fs::write(&path, "{ not json").expect("writes");
        let (refused, notes) = load_refused_admissions(Some(&journal));
        assert!(refused.is_empty());
        assert_eq!(notes.len(), 1);

        // Without a state directory the daemon says so.
        let (refused, notes) = load_refused_admissions(None);
        assert!(refused.is_empty());
        assert!(notes[0].contains("no state directory"), "{notes:?}");
        // The directory removes itself when this test ends.
    }

    #[test]
    fn the_jobs_report_states_admission_readiness_honestly() {
        use breadram_core::config::JobPolicy;

        let config = Config {
            jobs: JobPolicy {
                enabled: true,
                ..JobPolicy::default()
            },
            ..Config::default()
        };
        let queue = AdmissionQueue::new(8);

        // No observation yet: admission is held, and the report says why.
        let published = unavailable(ManagementMode::Automatic, 0);
        let payload = jobs_payload(&config, &queue, &published, &[], &[], &VecDeque::new());
        assert!(!payload.admission_ready);
        assert!(
            payload
                .readiness_note
                .as_deref()
                .unwrap_or_default()
                .contains("no observation"),
            "{:?}",
            payload.readiness_note
        );

        // A healthy observation: ready, with nothing to explain.
        let mut observed = unavailable(ManagementMode::Automatic, 0);
        observed.snapshot = Some(MemorySnapshot {
            observed_at_ms: 0,
            meminfo: breadram_core::telemetry::MemInfo {
                total: 8 * GIB,
                free: 0,
                available: Some(4 * GIB),
                buffers: 0,
                cached: 0,
                swap_total: 0,
                swap_free: 0,
                anon: None,
                shmem: None,
            },
            psi: None,
            vmstat: breadram_core::telemetry::VmStat::default(),
            capabilities: CapabilityReport::new(),
        });
        observed.pressure = PressureState::Healthy;
        let payload = jobs_payload(&config, &queue, &observed, &[], &[], &VecDeque::new());
        assert!(payload.admission_ready);
        assert!(payload.readiness_note.is_none());

        // Critical pressure: held, with the state named.
        observed.pressure = PressureState::Critical;
        let payload = jobs_payload(&config, &queue, &observed, &[], &[], &VecDeque::new());
        assert!(!payload.admission_ready);
        assert!(
            payload
                .readiness_note
                .as_deref()
                .unwrap_or_default()
                .contains("critical"),
            "{:?}",
            payload.readiness_note
        );

        // Refused ids from a previous run are named in the notes.
        let payload = jobs_payload(
            &config,
            &queue,
            &observed,
            &["old-job".to_string()],
            &[],
            &VecDeque::new(),
        );
        assert!(
            payload.notes.iter().any(|note| note.contains("old-job")),
            "{:?}",
            payload.notes
        );
    }

    #[test]
    fn a_submit_without_a_runnable_program_is_refused_before_queueing() {
        assert!(check_submit_spec(&[], &None, &[]).is_err());
        assert!(check_submit_spec(&["relative/bin".to_string()], &None, &[]).is_err());
        assert!(check_submit_spec(&["/bin/true".to_string()], &Some(String::new()), &[]).is_err());
        assert!(
            check_submit_spec(&["/bin/true".to_string()], &None, &["NOEQUALS".to_string()])
                .is_err()
        );
        assert!(check_submit_spec(
            &["/bin/true".to_string()],
            &None,
            &["GOOD=1".to_string(), "EMPTY=".to_string()],
        )
        .is_ok());
        let many = vec!["/bin/true".to_string(); MAX_JOB_ARGC + 1];
        assert!(check_submit_spec(&many, &None, &[]).is_err());

        assert!(check_launch_paths(&["/bin/true".to_string()], &None).is_ok());
        assert!(check_launch_paths(&["/does/not/exist".to_string()], &None).is_err());
        assert!(check_launch_paths(&["/tmp".to_string()], &None).is_err());
        assert!(check_launch_paths(
            &["/bin/true".to_string()],
            &Some("/does/not/exist".to_string())
        )
        .is_err());
    }

    #[test]
    fn scope_peak_reads_honestly_from_a_scope_directory() {
        let root = TempDir::new("cgroup-root");
        let nested = root.path().join("user.slice").join("user-1000.slice");
        let owned = nested.join("breadram-job-old.scope");
        std::fs::create_dir_all(&owned).expect("scope dir");
        std::fs::create_dir_all(nested.join("other.scope")).expect("other scope");
        std::fs::write(root.path().join("not-a-dir"), b"x").expect("file");

        assert_eq!(
            find_scope_dir(&root.path(), "breadram-job-old.scope"),
            Some(owned.clone())
        );
        assert_eq!(
            find_scope_dir(&root.path(), "breadram-job-gone.scope"),
            None
        );
        assert_eq!(
            read_scope_peak(&root.path(), "breadram-job-old.scope"),
            None,
            "no peak file means no reading, never zero"
        );
        std::fs::write(owned.join("memory.peak"), b"1048576\n").expect("peak");
        assert_eq!(
            read_scope_peak(&root.path(), "breadram-job-old.scope"),
            Some(1048576)
        );
    }

    #[test]
    fn the_finished_list_stays_bounded_with_the_newest_last() {
        let mut finished = VecDeque::new();
        for index in 0..MAX_FINISHED_JOBS + 5 {
            push_finished(
                &mut finished,
                FinishedJob {
                    id: format!("job-{index}"),
                    exit_code: Some(0),
                    note: "exited cleanly".to_string(),
                    peak_bytes: None,
                },
            );
        }
        assert_eq!(finished.len(), MAX_FINISHED_JOBS);
        assert_eq!(finished.front().expect("first").id, "job-5");
        assert_eq!(
            finished.back().expect("last").id,
            format!("job-{}", MAX_FINISHED_JOBS + 4)
        );
    }
}
