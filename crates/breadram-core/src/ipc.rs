//! Versioned local IPC contract.
//!
//! The daemon and every client (the CLI, the desktop UI, and optional
//! integrations) speak one newline delimited JSON protocol over an owner only
//! Unix socket. That protocol is versioned so a mismatch fails loudly with an
//! actionable error instead of silently misbehaving.
//!
//! Compatibility rule: the major version must match exactly, and a peer may
//! speak a minor version lower than or equal to this build's. A peer that
//! speaks a newer minor version is rejected, because this build cannot know
//! about fields or messages it does not ship.
//!
//! Every request carries an id and every response echoes it, so a client can
//! match replies to requests and detect a desynchronized stream.

use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::config::{ActionKind, Budget, Priority, Rule};
use crate::coordinator::{ActionEffect, ActionState, JournalEntry};
use crate::policy::{Plan, PlanChannel, PlannedAction, PressureState};
use crate::telemetry::{SwapLayout, WorkloadSwap};
use crate::{CapabilityReport, ManagementMode};

/// Major protocol version. Breaking changes bump this.
pub const PROTOCOL_MAJOR: u16 = 1;

/// Minor protocol version. Backward compatible additions bump this.
///
/// 2 adds the swap and OOM diagnostic section and the per workload swap limits
/// to the diagnostics report. 3 adds the bounded trend samples. 4 adds the
/// growth report paired with recorded relief. 5 adds the per workload priority
/// and its source. 6 adds the per workload effective memory limit. 7 adds
/// integrated GPU shared memory. 8 adds the background job queue requests and
/// the jobs report. Nothing was removed or renumbered.
pub const PROTOCOL_MINOR: u16 = 8;

/// The largest single message, in bytes. Oversized messages are rejected.
pub const MAX_MESSAGE_BYTES: usize = 1024 * 1024;

/// A rejected protocol version negotiation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProtocolError {
    /// The peer speaks a different major version.
    #[error(
        "incompatible IPC major version: peer speaks {peer_major}, this build speaks {local_major}"
    )]
    MajorMismatch {
        /// Major version reported by the peer.
        peer_major: u16,
        /// Major version this build speaks.
        local_major: u16,
    },
    /// The peer speaks a newer minor version than this build understands.
    #[error("peer IPC minor version {peer_minor} is newer than this build's {local_minor}")]
    PeerTooNew {
        /// Minor version reported by the peer.
        peer_minor: u16,
        /// Minor version this build speaks.
        local_minor: u16,
    },
}

/// Check whether a peer speaking `peer_major`/`peer_minor` may talk to this
/// build.
pub fn check_protocol(peer_major: u16, peer_minor: u16) -> Result<(), ProtocolError> {
    if peer_major != PROTOCOL_MAJOR {
        return Err(ProtocolError::MajorMismatch {
            peer_major,
            local_major: PROTOCOL_MAJOR,
        });
    }
    if peer_minor > PROTOCOL_MINOR {
        return Err(ProtocolError::PeerTooNew {
            peer_minor,
            local_minor: PROTOCOL_MINOR,
        });
    }
    Ok(())
}

/// A framing failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IpcError {
    /// The message exceeds [`MAX_MESSAGE_BYTES`].
    #[error("message is {len} bytes, over the {MAX_MESSAGE_BYTES} byte limit")]
    TooLarge {
        /// The observed size in bytes.
        len: usize,
    },
    /// The message could not be serialized.
    #[error("cannot encode message: {0}")]
    Encode(String),
    /// The message could not be parsed.
    #[error("cannot decode message: {0}")]
    Decode(String),
}

/// The opening message from a client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    /// The client's major protocol version.
    pub major: u16,
    /// The client's minor protocol version.
    pub minor: u16,
    /// A short client name for diagnostics.
    pub client: String,
}

/// What a client is asking for.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestBody {
    /// The mandatory first message.
    Hello(Hello),
    /// The current status snapshot.
    Status,
    /// The current capability report.
    Capabilities,
    /// Start streaming status events.
    Subscribe,
    /// The discovered workloads.
    Workloads,
    /// Explain what the policy would do right now, without doing it.
    Explain,
    /// The settings in effect: mode, permitted actions, protection, rules.
    Rules,
    /// The current plan and the bounded action history.
    Actions,
    /// The full diagnostic bundle.
    Diagnostics,
    /// The background job queue: pending and admitted reservations.
    Jobs,
    /// Submit an explicitly opted-in background job to the queue.
    SubmitJob {
        /// The job id: 1 to 128 ASCII alphanumeric, hyphen, or underscore bytes.
        /// Named job_id because the message envelope already carries id.
        job_id: String,
        /// The conservative upper-bound memory estimate in bytes. Zero means
        /// the configured default estimate.
        #[serde(default)]
        estimate_bytes: u64,
        /// The program and its arguments, executed directly with no shell.
        /// The first entry must be an absolute path.
        #[serde(default)]
        argv: Vec<String>,
        /// The working directory. The daemon's own when absent.
        #[serde(default)]
        cwd: Option<String>,
        /// Extra environment entries as KEY=VALUE strings, applied over the
        /// inherited environment. Never stored beyond the running job.
        #[serde(default)]
        env: Vec<String>,
    },
    /// Admit a queued job and launch it under supervision.
    LaunchJob {
        /// The queued job id to admit and launch.
        job_id: String,
    },
    /// Cancel a queued job. Never stops an already admitted job.
    CancelJob {
        /// The job id to remove from the pending queue.
        job_id: String,
    },
    /// Change the management mode.
    SetMode {
        /// The mode to switch to.
        mode: ManagementMode,
        /// Write the change back to the configuration file.
        #[serde(default)]
        persist: bool,
    },
    /// Add or remove protected workloads.
    SetProtection {
        /// Scope keys to protect.
        #[serde(default)]
        add: Vec<String>,
        /// Scope keys to stop protecting.
        #[serde(default)]
        remove: Vec<String>,
        /// Write the change back to the configuration file.
        #[serde(default)]
        persist: bool,
    },
}

/// A client message with its request id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientMessage {
    /// The request id, echoed by the matching response.
    pub id: u64,
    /// The request.
    #[serde(flatten)]
    pub body: RequestBody,
}

/// A status snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusPayload {
    /// The current management mode.
    pub mode: ManagementMode,
    /// The daemon version.
    pub version: String,
    /// Monotonic observation time in milliseconds.
    pub observed_at_ms: u64,
    /// Available memory in bytes, when known.
    pub available_bytes: Option<u64>,
    /// Total memory in bytes, when known.
    pub total_bytes: Option<u64>,
    /// How many workloads are currently known.
    pub workloads: usize,
    /// What the daemon can do on this host.
    pub capabilities: CapabilityReport,
}

/// A streamed event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventPayload {
    /// The event kind, currently always `status`.
    pub kind: String,
    /// Monotonic observation time in milliseconds.
    pub observed_at_ms: u64,
    /// Available memory in bytes, when known.
    pub available_bytes: Option<u64>,
}

/// A machine readable error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// The peer's protocol version is not supported.
    IncompatibleVersion,
    /// The request type is not understood.
    UnknownRequest,
    /// The request was understood but malformed.
    BadRequest,
    /// The daemon cannot serve the request right now.
    Unavailable,
    /// An internal failure.
    Internal,
}

/// An actionable error returned to a client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiError {
    /// A stable code.
    pub code: ErrorCode,
    /// A human readable explanation.
    pub message: String,
}

impl ApiError {
    /// Build an error.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for ApiError {}

/// What the daemon replies with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseBody {
    /// The handshake reply.
    Hello {
        /// The daemon's major protocol version.
        major: u16,
        /// The daemon's minor protocol version.
        minor: u16,
        /// The daemon version.
        server: String,
    },
    /// A status snapshot.
    Status(Box<StatusPayload>),
    /// A capability report.
    Capabilities(CapabilityReport),
    /// The subscription was accepted.
    Subscribed {
        /// The interval between status events, in milliseconds.
        interval_ms: u64,
    },
    /// A streamed event.
    Event(EventPayload),
    /// The discovered workloads.
    Workloads(Box<WorkloadsPayload>),
    /// An explanation of the current plan.
    Plan(Box<PlanPayload>),
    /// The settings in effect.
    Rules(Box<RulesPayload>),
    /// The current plan and the bounded action history.
    Actions(Box<ActionsPayload>),
    /// The diagnostic bundle.
    Diagnostics(Box<DiagnosticsPayload>),
    /// The background job queue.
    Jobs(Box<JobsPayload>),
    /// The result of a configuration change.
    Applied(Box<AppliedPayload>),
    /// An error.
    Error(ApiError),
}

/// One workload, as the daemon sees it.
///
/// The memory figures are reported separately by source. A cgroup figure is
/// exact accounting; a summed RSS is an over-estimate because a shared page is
/// counted in every process that maps it. `estimated` says which one this is, so
/// a caller never mistakes an estimate for a measurement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadSummary {
    /// The stable grouping key, for example `unit:ollama.service`.
    pub scope: String,
    /// The generation taken from process start times.
    pub generation: u64,
    /// A human readable name.
    pub name: String,
    /// How many processes are grouped here.
    pub processes: usize,
    /// Exact cgroup memory, when the kernel accounts it.
    pub cgroup_bytes: Option<u64>,
    /// Summed resident set size, when that is all there is.
    pub rss_bytes: Option<u64>,
    /// Whether the figure is an estimate rather than exact accounting.
    pub estimated: bool,
    /// `eligible` when policy may control it, otherwise `observe_only`.
    pub control: String,
    /// The effective priority, from rules, hints, or a dependency.
    #[serde(default)]
    pub priority: Priority,
    /// Why that priority applies, in words, so it is not a bare number.
    #[serde(default)]
    pub priority_source: String,
    /// The group's effective memory cap (`memory.max`), when one is set. This is
    /// distinct from host available memory.
    #[serde(default)]
    pub limit_bytes: Option<u64>,
}

/// The workload inventory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadsPayload {
    /// Monotonic observation time in milliseconds.
    pub observed_at_ms: u64,
    /// The workloads, sorted by scope.
    pub workloads: Vec<WorkloadSummary>,
    /// What could be read on this host.
    pub capabilities: CapabilityReport,
}

/// One proposed action, with the reason it was proposed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedActionPayload {
    /// The scope key of the target.
    pub target: String,
    /// The generation the plan was made against.
    pub generation: u64,
    /// What would be done.
    pub kind: ActionKind,
    /// Why, for the explanation.
    pub reason: String,
    /// The expected host memory change in bytes. Negative is relief.
    pub expected_host_ram_delta: i64,
    /// Whether proven idleness is required before it may run.
    pub requires_confirmed_idle: bool,
}

/// A plan, annotated for the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanPayload {
    /// The pressure state the plan was made in.
    pub state: String,
    /// Which channel asked for the plan.
    pub channel: String,
    /// Whether the caller is permitted to execute it.
    pub would_execute: bool,
    /// The proposed actions, most relief first.
    pub actions: Vec<PlannedActionPayload>,
    /// The reasoning, including every declined action and why.
    pub explanation: Vec<String>,
    /// Monotonic time the plan was made, in milliseconds.
    pub observed_at_ms: u64,
}

/// The settings in effect.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RulesPayload {
    /// The management mode.
    pub mode: ManagementMode,
    /// The configuration file the daemon loaded, when it loaded one.
    pub config_path: Option<String>,
    /// Which actions automation may take. Empty means automation does nothing.
    pub allowed_actions: Vec<ActionKind>,
    /// Scope keys that must not be touched automatically.
    pub protected: Vec<String>,
    /// Bounded per workload budgets.
    pub budgets: Vec<Budget>,
    /// Per workload rules, in evaluation order.
    pub rules: Vec<Rule>,
}

/// A daemon-owned setting that an action changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnedValueRecord {
    /// A stable setting key.
    pub key: String,
    /// The value before breadram touched it.
    pub original: String,
    /// The value breadram last wrote.
    pub last_applied: String,
}

/// One recorded action: what ran, why, and what it changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionJournalRecord {
    /// The idempotency id.
    pub id: u64,
    /// The stable scope key of the target. Never a command line.
    pub scope: String,
    /// The action, or `None` for an entry that is not an action, such as a
    /// restore. A rollback never claims to be the action it undid.
    pub kind: Option<ActionKind>,
    /// How it ended.
    pub state: String,
    /// When it ended, in monotonic milliseconds.
    pub at_ms: u64,
    /// Why it ran, in words.
    pub reason: String,
    /// The host memory change the plan expected.
    pub requested_host_ram_delta: Option<i64>,
    /// The host memory change that was measured, when it could be.
    pub observed_host_ram_delta: Option<i64>,
    /// The device memory change the plan expected.
    pub requested_device_ram_delta: Option<i64>,
    /// The device memory change that was measured, when it could be.
    pub observed_device_ram_delta: Option<i64>,
    /// The daemon-owned values it changed.
    pub owned_values: Vec<OwnedValueRecord>,
}

/// The current plan and the bounded history of actions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionsPayload {
    /// The plan the daemon would follow right now.
    pub plan: PlanPayload,
    /// Recorded actions, newest first.
    pub history: Vec<ActionJournalRecord>,
    /// How many entries the journal retains at most.
    pub history_capacity: usize,
    /// Why the history is empty, when the daemon is not executing anything.
    pub history_note: Option<String>,
}

/// One pending background job, as the daemon sees it.
///
/// Metadata only: no arguments, environment, working directory, prompts, or
/// credentials are stored or reported.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingJobView {
    /// The job id.
    pub id: String,
    /// The conservative upper-bound memory estimate in bytes.
    pub reservation_bytes: u64,
}

/// One admitted reservation the daemon holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmittedJobView {
    /// The job id.
    pub id: String,
    /// The reserved upper bound in bytes, raised by observed usage.
    pub reservation_bytes: u64,
}

/// One running job under supervision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunningJobView {
    /// The job id.
    pub id: String,
    /// The reserved upper bound in bytes.
    pub reservation_bytes: u64,
}

/// One finished job, remembered in a bounded list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinishedJobView {
    /// The job id.
    pub id: String,
    /// The exit code, absent when a signal ended the job.
    pub exit_code: Option<i32>,
    /// What happened, in words: exited, cancelled by the operator, or failed.
    pub note: String,
    /// The peak memory observed while it ran, when it could be read.
    pub peak_bytes: Option<u64>,
}

/// The background job queue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobsPayload {
    /// Whether job admission is enabled in the daemon configuration.
    pub enabled: bool,
    /// The conservative total admission budget in bytes.
    pub total_budget_bytes: u64,
    /// Bytes currently reserved by admitted jobs.
    pub reserved_bytes: u64,
    /// Jobs waiting for admission, oldest first.
    pub pending: Vec<PendingJobView>,
    /// Reservations the daemon holds, by id.
    pub admitted: Vec<AdmittedJobView>,
    /// Jobs running under supervision, by id.
    #[serde(default)]
    pub running: Vec<RunningJobView>,
    /// Recently finished jobs, newest last, bounded by the daemon.
    #[serde(default)]
    pub finished: Vec<FinishedJobView>,
    /// Whether the daemon would admit right now: an observation exists and
    /// pressure is healthy. Unknown or unhealthy pressure refuses all new
    /// admissions. Defaults to false so an older report never reads as ready.
    #[serde(default)]
    pub admission_ready: bool,
    /// Why admission is held, when it is. Absent when ready.
    #[serde(default)]
    pub readiness_note: Option<String>,
    /// Anything a human should know when reading this report.
    #[serde(default)]
    pub notes: Vec<String>,
}

/// The host memory figures, each from its own source.
///
/// A figure that could not be read is `None` with a note, so an unknown value is
/// never displayed as zero. VRAM is separate from system RAM: it is reported only
/// when a provider or adapter can actually measure it, and it is never inferred
/// from host memory.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryBreakdown {
    /// Total system RAM in bytes.
    pub ram_total_bytes: Option<u64>,
    /// Available system RAM in bytes, when the kernel reports it.
    pub ram_available_bytes: Option<u64>,
    /// Total swap in bytes. Zero means no swap is configured.
    pub swap_total_bytes: Option<u64>,
    /// Used swap in bytes, absent when no swap is configured.
    pub swap_used_bytes: Option<u64>,
    /// Shared memory (`Shmem`) in bytes.
    pub shared_bytes: Option<u64>,
    /// Device memory in bytes, when something can measure it.
    pub vram_bytes: Option<u64>,
    /// Integrated GPU shared memory in use, distinct from host RAM and from
    /// dedicated VRAM. Defaulted so an older peer still decodes.
    #[serde(default)]
    pub gtt_used_bytes: Option<u64>,
    /// Integrated GPU shared memory total, when the driver reports one.
    #[serde(default)]
    pub gtt_total_bytes: Option<u64>,
    /// Pages swapped in since boot, when the kernel reports them.
    pub swap_pages_in: Option<u64>,
    /// Pages swapped out since boot, when the kernel reports them.
    pub swap_pages_out: Option<u64>,
    /// Why a figure is missing, one note per missing figure.
    pub notes: Vec<String>,
}

/// The pressure reading the policy is acting on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PressureSummary {
    /// The pressure state.
    pub state: String,
    /// The 10 second `some` average, when PSI is available.
    pub some_avg10: Option<f64>,
    /// The 10 second `full` average, when the kernel reports it.
    pub full_avg10: Option<f64>,
    /// Available memory as a fraction of total.
    pub available_fraction: Option<f64>,
}

/// What the action journal holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalSummary {
    /// How many entries are retained.
    pub entries: usize,
    /// The configured capacity.
    pub capacity: usize,
    /// Whether a plan-execute loop is running in the daemon.
    pub executing: bool,
}

/// Everything a bug report needs, in one response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiagnosticsPayload {
    /// The management mode.
    pub mode: ManagementMode,
    /// The daemon version.
    pub version: String,
    /// The protocol major version.
    pub protocol_major: u16,
    /// The protocol minor version.
    pub protocol_minor: u16,
    /// The socket the daemon is listening on.
    pub socket: String,
    /// How long the daemon has been running, in milliseconds.
    pub uptime_ms: u64,
    /// Monotonic observation time in milliseconds.
    pub observed_at_ms: u64,
    /// The configuration file the daemon loaded, when it loaded one.
    pub config_path: Option<String>,
    /// Host memory, split by source.
    pub memory: MemoryBreakdown,
    /// The pressure reading.
    pub pressure: PressureSummary,
    /// Whether background job admission is enabled.
    pub jobs_enabled: bool,
    /// How many workloads were discovered.
    pub workloads: usize,
    /// How many systemd user units could be controlled.
    pub control_targets: usize,
    /// What could be read and controlled on this host.
    pub capabilities: CapabilityReport,
    /// What the action journal holds.
    pub journal: JournalSummary,
    /// The swap, compression, and OOM management this host already has.
    ///
    /// Descriptive only: breadram reports this layout and never changes it.
    /// Defaulted on read, so a client of either minor version can decode it.
    #[serde(default)]
    pub swap: SwapLayout,
    /// The effective swap limit of each workload breadram can control.
    #[serde(default)]
    pub workload_swap: Vec<WorkloadSwap>,
    /// Bounded numeric memory samples. Keys are daemon-local, not scope names.
    #[serde(default)]
    pub trends: Vec<crate::trends::TrendSample>,
    /// One sustained-growth judgement per retained workload, paired with the
    /// measured relief recorded for the same target.
    #[serde(default)]
    pub growth: Vec<crate::trends::GrowthReport>,
    /// Anything a human should know when reading this report.
    pub notes: Vec<String>,
}

/// The result of a configuration change.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppliedPayload {
    /// A one line description of what changed.
    pub summary: String,
    /// Whether the change was written back to the configuration file.
    pub persisted: bool,
    /// The settings in effect after the change.
    pub rules: RulesPayload,
}

/// The wire name of a pressure state.
pub fn pressure_state_name(state: PressureState) -> &'static str {
    match state {
        PressureState::Healthy => "healthy",
        PressureState::Recovering => "recovering",
        PressureState::Pressured => "pressured",
        PressureState::Critical => "critical",
    }
}

/// The wire name of a plan channel.
pub fn plan_channel_name(channel: PlanChannel) -> &'static str {
    match channel {
        PlanChannel::Monitor => "monitor",
        PlanChannel::DryRun => "dry_run",
        PlanChannel::Automatic => "automatic",
    }
}

/// The wire name of an action state.
pub fn action_state_name(state: ActionState) -> &'static str {
    match state {
        ActionState::Confirmed => "confirmed",
        ActionState::Unconfirmed => "unconfirmed",
        ActionState::SkippedDuplicate => "skipped_duplicate",
        ActionState::Unsupported => "unsupported",
        ActionState::Superseded => "superseded",
        ActionState::Busy => "busy",
        ActionState::Denied => "denied",
        ActionState::TimedOut => "timed_out",
        ActionState::Cancelled => "cancelled",
        ActionState::Failed => "failed",
        ActionState::RolledBack => "rolled_back",
    }
}

/// The wire name of a control level.
pub fn control_level_name(level: crate::inventory::ControlLevel) -> &'static str {
    match level {
        crate::inventory::ControlLevel::Eligible => "eligible",
        crate::inventory::ControlLevel::ObserveOnly => "observe_only",
    }
}

impl PlannedActionPayload {
    /// Present one planned action for the wire.
    pub fn from_planned(action: &PlannedAction) -> Self {
        Self {
            target: action.target.scope.clone(),
            generation: action.target.generation,
            kind: action.kind,
            reason: action.reason.clone(),
            expected_host_ram_delta: action.expected_host_ram_delta,
            requires_confirmed_idle: action.requires_confirmed_idle,
        }
    }
}

impl PlanPayload {
    /// Present a plan for the wire.
    pub fn from_plan(plan: &Plan, observed_at_ms: u64) -> Self {
        Self {
            state: pressure_state_name(plan.state).to_string(),
            channel: plan_channel_name(plan.channel).to_string(),
            would_execute: plan.would_execute,
            actions: plan
                .actions
                .iter()
                .map(PlannedActionPayload::from_planned)
                .collect(),
            explanation: plan.explanation.clone(),
            observed_at_ms,
        }
    }
}

impl ActionJournalRecord {
    /// Present one journal entry for the wire.
    pub fn from_entry(entry: &JournalEntry) -> Self {
        let effect: Option<&ActionEffect> = entry.effect.as_ref();
        Self {
            id: entry.id.as_u64(),
            scope: entry.scope.clone(),
            kind: entry.kind,
            state: action_state_name(entry.state).to_string(),
            at_ms: entry.at_ms,
            reason: entry.reason.clone(),
            requested_host_ram_delta: effect.map(|e| e.requested_host_ram_delta),
            observed_host_ram_delta: effect.and_then(|e| e.observed_host_ram_delta),
            requested_device_ram_delta: effect.map(|e| e.requested_device_ram_delta),
            observed_device_ram_delta: effect.and_then(|e| e.observed_device_ram_delta),
            owned_values: entry
                .values
                .iter()
                .map(|value| OwnedValueRecord {
                    key: value.key.clone(),
                    original: value.original.clone(),
                    last_applied: value.last_applied.clone(),
                })
                .collect(),
        }
    }
}

/// A server message with the id of the request it answers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServerMessage {
    /// The id of the request this answers, or 0 for an unsolicited event.
    pub id: u64,
    /// The response.
    #[serde(flatten)]
    pub body: ResponseBody,
}

/// Encode a message to a single line, without the trailing newline.
pub fn encode<T: Serialize>(message: &T) -> Result<String, IpcError> {
    let text = serde_json::to_string(message).map_err(|err| IpcError::Encode(err.to_string()))?;
    if text.len() > MAX_MESSAGE_BYTES {
        return Err(IpcError::TooLarge { len: text.len() });
    }
    Ok(text)
}

/// Decode a single line into a message.
pub fn decode<T: for<'de> Deserialize<'de>>(line: &str) -> Result<T, IpcError> {
    let trimmed = line.trim_end_matches(['\n', '\r']);
    if trimmed.len() > MAX_MESSAGE_BYTES {
        return Err(IpcError::TooLarge { len: trimmed.len() });
    }
    serde_json::from_str(trimmed).map_err(|err| IpcError::Decode(err.to_string()))
}

/// The default socket path: under `XDG_RUNTIME_DIR` when set, which is owner
/// only per user, otherwise a socket in the temp directory.
pub fn default_socket_path() -> PathBuf {
    match std::env::var_os("XDG_RUNTIME_DIR") {
        Some(dir) if !dir.is_empty() => PathBuf::from(dir).join("breadram.sock"),
        _ => std::env::temp_dir().join("breadram.sock"),
    }
}

/// An async client for the daemon protocol.
pub mod client {
    use super::{
        check_protocol, decode, encode, ActionsPayload, ApiError, AppliedPayload, ClientMessage,
        DiagnosticsPayload, EventPayload, Hello, IpcError, JobsPayload, PlanPayload, RequestBody,
        ResponseBody, RulesPayload, ServerMessage, StatusPayload, WorkloadsPayload,
        MAX_MESSAGE_BYTES, PROTOCOL_MAJOR, PROTOCOL_MINOR,
    };
    use crate::{CapabilityReport, ManagementMode};
    use std::path::Path;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
    use tokio::net::UnixStream;

    /// Why a client call failed.
    #[derive(Debug, thiserror::Error)]
    pub enum ClientError {
        /// The daemon is not reachable.
        #[error("breadramd is not running or the socket at {path} is unavailable: {source}")]
        Unavailable {
            /// The socket path that was tried.
            path: String,
            /// The underlying connect error.
            #[source]
            source: std::io::Error,
        },
        /// The daemon returned an error.
        #[error("daemon error: {0}")]
        Api(ApiError),
        /// The daemon closed the connection.
        #[error("the daemon closed the connection")]
        Closed,
        /// The daemon speaks an incompatible protocol version.
        #[error("incompatible protocol version: {0}")]
        Version(#[from] super::ProtocolError),
        /// A framing error.
        #[error(transparent)]
        Frame(#[from] IpcError),
        /// An I/O error.
        #[error("io error: {0}")]
        Io(#[from] std::io::Error),
        /// The daemon sent a response this client did not expect.
        #[error("unexpected response from the daemon: {0}")]
        Unexpected(String),
    }

    /// A connected client.
    #[derive(Debug)]
    pub struct Client {
        reader: BufReader<OwnedReadHalf>,
        writer: OwnedWriteHalf,
        next_id: u64,
    }

    impl Client {
        /// Connect to a daemon socket.
        pub async fn connect(path: &Path) -> Result<Self, ClientError> {
            let stream =
                UnixStream::connect(path)
                    .await
                    .map_err(|source| ClientError::Unavailable {
                        path: path.display().to_string(),
                        source,
                    })?;
            let (read, write) = stream.into_split();
            Ok(Self {
                reader: BufReader::new(read),
                writer: write,
                next_id: 1,
            })
        }

        /// Perform the version handshake. Returns the daemon version.
        pub async fn handshake(&mut self, client_name: &str) -> Result<String, ClientError> {
            let body = RequestBody::Hello(Hello {
                major: PROTOCOL_MAJOR,
                minor: PROTOCOL_MINOR,
                client: client_name.to_string(),
            });
            match self.call(body).await? {
                ResponseBody::Hello { server, major, .. } => {
                    check_protocol(major, PROTOCOL_MINOR).map_err(ClientError::Version)?;
                    Ok(server)
                }
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        /// Request the current status.
        pub async fn status(&mut self) -> Result<StatusPayload, ClientError> {
            match self.call(RequestBody::Status).await? {
                ResponseBody::Status(status) => Ok(*status),
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        /// Request the capability report.
        pub async fn capabilities(&mut self) -> Result<CapabilityReport, ClientError> {
            match self.call(RequestBody::Capabilities).await? {
                ResponseBody::Capabilities(report) => Ok(report),
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        /// Request the discovered workloads.
        pub async fn workloads(&mut self) -> Result<WorkloadsPayload, ClientError> {
            match self.call(RequestBody::Workloads).await? {
                ResponseBody::Workloads(payload) => Ok(*payload),
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        /// Ask for an explanation of the current plan.
        pub async fn explain(&mut self) -> Result<PlanPayload, ClientError> {
            match self.call(RequestBody::Explain).await? {
                ResponseBody::Plan(plan) => Ok(*plan),
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        /// Request the settings in effect.
        pub async fn rules(&mut self) -> Result<RulesPayload, ClientError> {
            match self.call(RequestBody::Rules).await? {
                ResponseBody::Rules(rules) => Ok(*rules),
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        /// Request the plan and the bounded action history.
        pub async fn actions(&mut self) -> Result<ActionsPayload, ClientError> {
            match self.call(RequestBody::Actions).await? {
                ResponseBody::Actions(actions) => Ok(*actions),
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        /// Request the diagnostic bundle.
        pub async fn diagnostics(&mut self) -> Result<DiagnosticsPayload, ClientError> {
            match self.call(RequestBody::Diagnostics).await? {
                ResponseBody::Diagnostics(diagnostics) => Ok(*diagnostics),
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        /// Request the background job queue.
        pub async fn jobs(&mut self) -> Result<JobsPayload, ClientError> {
            match self.call(RequestBody::Jobs).await? {
                ResponseBody::Jobs(jobs) => Ok(*jobs),
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        /// Submit an explicitly opted-in background job. Zero estimates use the
        /// configured default. Returns the queue after the change.
        pub async fn submit_job(
            &mut self,
            job_id: String,
            estimate_bytes: u64,
            argv: Vec<String>,
            cwd: Option<String>,
            env: Vec<String>,
        ) -> Result<JobsPayload, ClientError> {
            match self
                .call(RequestBody::SubmitJob {
                    job_id,
                    estimate_bytes,
                    argv,
                    cwd,
                    env,
                })
                .await?
            {
                ResponseBody::Jobs(jobs) => Ok(*jobs),
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        /// Admit a queued job and launch it under supervision.
        pub async fn launch_job(&mut self, job_id: String) -> Result<JobsPayload, ClientError> {
            match self.call(RequestBody::LaunchJob { job_id }).await? {
                ResponseBody::Jobs(jobs) => Ok(*jobs),
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        /// Cancel a queued job. Returns the queue after the change.
        pub async fn cancel_job(&mut self, job_id: String) -> Result<JobsPayload, ClientError> {
            match self.call(RequestBody::CancelJob { job_id }).await? {
                ResponseBody::Jobs(jobs) => Ok(*jobs),
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        /// Change the management mode, optionally writing it to the file.
        pub async fn set_mode(
            &mut self,
            mode: ManagementMode,
            persist: bool,
        ) -> Result<AppliedPayload, ClientError> {
            match self.call(RequestBody::SetMode { mode, persist }).await? {
                ResponseBody::Applied(applied) => Ok(*applied),
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        /// Protect or unprotect workloads, optionally writing it to the file.
        pub async fn set_protection(
            &mut self,
            add: Vec<String>,
            remove: Vec<String>,
            persist: bool,
        ) -> Result<AppliedPayload, ClientError> {
            match self
                .call(RequestBody::SetProtection {
                    add,
                    remove,
                    persist,
                })
                .await?
            {
                ResponseBody::Applied(applied) => Ok(*applied),
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        /// Subscribe to status events.
        pub async fn subscribe(&mut self) -> Result<u64, ClientError> {
            match self.call(RequestBody::Subscribe).await? {
                ResponseBody::Subscribed { interval_ms } => Ok(interval_ms),
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        /// Read the next event from a subscription.
        pub async fn next_event(&mut self) -> Result<EventPayload, ClientError> {
            match self.recv().await?.body {
                ResponseBody::Event(event) => Ok(event),
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        async fn call(&mut self, body: RequestBody) -> Result<ResponseBody, ClientError> {
            let id = self.next_id;
            self.next_id += 1;
            self.send(&ClientMessage { id, body }).await?;
            let response = self.recv().await?;
            if response.id != id {
                return Err(ClientError::Unexpected(format!(
                    "response id {} does not match request id {id}",
                    response.id
                )));
            }
            Ok(response.body)
        }

        async fn send(&mut self, message: &ClientMessage) -> Result<(), ClientError> {
            let line = encode(message)?;
            self.writer.write_all(line.as_bytes()).await?;
            self.writer.write_all(b"\n").await?;
            self.writer.flush().await?;
            Ok(())
        }

        async fn recv(&mut self) -> Result<ServerMessage, ClientError> {
            let mut line = String::new();
            let read = self.reader.read_line(&mut line).await?;
            if read == 0 {
                return Err(ClientError::Closed);
            }
            if line.len() > MAX_MESSAGE_BYTES {
                return Err(ClientError::Frame(IpcError::TooLarge { len: line.len() }));
            }
            Ok(decode(&line)?)
        }
    }
}

pub use client::{Client, ClientError};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_version_is_accepted() {
        assert_eq!(check_protocol(PROTOCOL_MAJOR, PROTOCOL_MINOR), Ok(()));
    }

    #[test]
    fn older_minor_is_accepted() {
        assert_eq!(check_protocol(PROTOCOL_MAJOR, 0), Ok(()));
    }

    #[test]
    fn different_major_is_rejected() {
        let err = check_protocol(PROTOCOL_MAJOR + 1, PROTOCOL_MINOR).unwrap_err();
        assert!(matches!(err, ProtocolError::MajorMismatch { .. }));
    }

    #[test]
    fn newer_minor_is_rejected() {
        let err = check_protocol(PROTOCOL_MAJOR, PROTOCOL_MINOR + 1).unwrap_err();
        assert!(matches!(err, ProtocolError::PeerTooNew { .. }));
    }

    #[test]
    fn a_wrong_major_beats_a_wrong_minor() {
        let err = check_protocol(PROTOCOL_MAJOR + 1, PROTOCOL_MINOR + 5).unwrap_err();
        assert!(matches!(err, ProtocolError::MajorMismatch { .. }));
    }

    #[test]
    fn messages_round_trip_as_json_lines() {
        let message = ClientMessage {
            id: 7,
            body: RequestBody::Hello(Hello {
                major: PROTOCOL_MAJOR,
                minor: PROTOCOL_MINOR,
                client: "cli".to_string(),
            }),
        };
        let line = encode(&message).unwrap();
        assert!(!line.contains('\n'));
        let decoded: ClientMessage = decode(&line).unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn a_response_round_trips() {
        let message = ServerMessage {
            id: 3,
            body: ResponseBody::Error(ApiError::new(ErrorCode::UnknownRequest, "nope")),
        };
        let line = encode(&message).unwrap();
        let decoded: ServerMessage = decode(&line).unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn a_jobs_report_round_trips() {
        let message = ServerMessage {
            id: 9,
            body: ResponseBody::Jobs(Box::new(JobsPayload {
                enabled: true,
                total_budget_bytes: 8 * 1024 * 1024 * 1024,
                reserved_bytes: 0,
                pending: vec![PendingJobView {
                    id: "nightly-build".to_string(),
                    reservation_bytes: 1024,
                }],
                admitted: Vec::new(),
                running: Vec::new(),
                finished: Vec::new(),
                admission_ready: true,
                readiness_note: None,
                notes: vec!["admission runs only in automatic mode".to_string()],
            })),
        };
        let line = encode(&message).unwrap();
        let decoded: ServerMessage = decode(&line).unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn a_diagnostics_payload_from_the_previous_minor_version_still_decodes() {
        // The swap section was added in minor version 2, the trend samples in 3, the
        // growth report in 4, the per workload priority and source in 5, the per
        // workload effective memory limit in 6, integrated GPU shared memory in
        // 7, and the background job queue in 8. A client that speaks an older minor must still be able to read a report
        // from this build, and a report written before each addition must still
        // decode here, which is why those fields are defaulted rather than required.
        assert_eq!(PROTOCOL_MINOR, 8);
        let older = r#"{
            "mode": "monitor",
            "version": "0.1.0",
            "protocol_major": 1,
            "protocol_minor": 1,
            "socket": "/run/user/1000/breadram.sock",
            "uptime_ms": 10,
            "observed_at_ms": 20,
            "config_path": null,
            "memory": {
                "ram_total_bytes": null, "ram_available_bytes": null,
                "swap_total_bytes": null, "swap_used_bytes": null,
                "shared_bytes": null, "vram_bytes": null,
                "swap_pages_in": null, "swap_pages_out": null, "notes": []
            },
            "pressure": {
                "state": "healthy", "some_avg10": null,
                "full_avg10": null, "available_fraction": null
            },
            "jobs_enabled": false,
            "workloads": 0,
            "control_targets": 0,
            "capabilities": { "capabilities": [] },
            "journal": { "entries": 0, "capacity": 10, "executing": false },
            "notes": []
        }"#;
        let decoded: DiagnosticsPayload = serde_json::from_str(older).expect("the older shape");
        assert!(!decoded.swap.swap_table_readable);
        assert!(decoded.workload_swap.is_empty());
        assert!(decoded.swap.areas.is_empty());
    }

    #[test]
    fn an_oversized_message_is_rejected() {
        let big = "x".repeat(MAX_MESSAGE_BYTES + 1);
        assert!(matches!(
            decode::<ClientMessage>(&big),
            Err(IpcError::TooLarge { .. })
        ));
    }

    #[test]
    fn malformed_json_is_a_decode_error() {
        assert!(matches!(
            decode::<ClientMessage>("not json"),
            Err(IpcError::Decode(_))
        ));
    }

    #[test]
    fn every_command_round_trips_as_a_request() {
        let bodies = vec![
            RequestBody::Status,
            RequestBody::Capabilities,
            RequestBody::Subscribe,
            RequestBody::Workloads,
            RequestBody::Explain,
            RequestBody::Rules,
            RequestBody::Actions,
            RequestBody::Diagnostics,
            RequestBody::Jobs,
            RequestBody::SubmitJob {
                job_id: "nightly-build".to_string(),
                estimate_bytes: 0,
                argv: vec!["/usr/bin/make".to_string()],
                cwd: None,
                env: Vec::new(),
            },
            RequestBody::LaunchJob {
                job_id: "nightly-build".to_string(),
            },
            RequestBody::CancelJob {
                job_id: "nightly-build".to_string(),
            },
            RequestBody::SetMode {
                mode: ManagementMode::Automatic,
                persist: true,
            },
            RequestBody::SetProtection {
                add: vec!["unit:sshd.service".to_string()],
                remove: Vec::new(),
                persist: false,
            },
        ];
        for body in bodies {
            let message = ClientMessage { id: 1, body };
            let line = encode(&message).unwrap();
            let decoded: ClientMessage = decode(&line).unwrap();
            assert_eq!(decoded, message);
        }
    }

    #[test]
    fn a_write_request_without_persist_defaults_to_false() {
        let decoded: ClientMessage = decode(r#"{"id":1,"type":"set_mode","mode":"automatic"}"#)
            .expect("a request without the optional field");
        assert_eq!(
            decoded.body,
            RequestBody::SetMode {
                mode: ManagementMode::Automatic,
                persist: false,
            }
        );
    }

    #[test]
    fn action_states_have_stable_wire_names() {
        use crate::coordinator::ActionState;
        let names = [
            (ActionState::Confirmed, "confirmed"),
            (ActionState::Unconfirmed, "unconfirmed"),
            (ActionState::SkippedDuplicate, "skipped_duplicate"),
            (ActionState::Unsupported, "unsupported"),
            (ActionState::Superseded, "superseded"),
            (ActionState::Busy, "busy"),
            (ActionState::Denied, "denied"),
            (ActionState::TimedOut, "timed_out"),
            (ActionState::Cancelled, "cancelled"),
            (ActionState::Failed, "failed"),
            (ActionState::RolledBack, "rolled_back"),
        ];
        for (state, name) in names {
            assert_eq!(action_state_name(state), name);
        }
    }

    #[test]
    fn an_action_kind_serializes_as_its_configuration_name() {
        let json = serde_json::to_string(&ActionKind::CgroupSoftLimit).unwrap();
        assert_eq!(json, "\"cgroup_soft_limit\"");
    }
}
