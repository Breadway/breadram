//! Cooperative hooks for runtimes without a built-in adapter.
//!
//! Some workloads know how to shed memory but speak no protocol breadram
//! understands: a Python training job, a renderer, a cache-heavy service. A
//! hook is the cooperative bridge for those: an explicitly configured program
//! that reports whether its workload is busy and carries out release and wake
//! operations on request.
//!
//! Three properties keep this safe:
//!
//! - **The command comes from the configuration file and nowhere else.** A
//!   hook program and its arguments are stated in `breadram.toml`, validated
//!   before use, and run with no shell. Nothing an inference endpoint answers
//!   and nothing discovered process metadata contains can change what runs.
//! - **Hook output is data, never instructions.** A hook prints JSON
//!   documents; breadram parses them for a busy flag and byte counts. There
//!   is no field that names a program, a path, or a follow-up command.
//! - **Every invocation is bounded.** Ownership of the program is checked, the
//!   environment is cleared, output beyond the cap is refused, and a hook that
//!   outlives its deadline is killed.
//!
//! The wire protocol is documented in `docs/HOOKS.md`. The worked,
//! non-Go example is `examples/hook-example.py`.

use std::io::Read;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::config::{ActionKind, HookSpec, ProviderKind};
use crate::inventory::Workload;
use crate::policy::activity::ActivityEvidence;

use super::capability::{AdapterCapabilities, AdapterCapability};
use super::probe::{Auth, ProbeRequest};
use super::{
    AdapterOperation, Confirmation, ManagedResource, OperationMethod, OperationRequest,
    WorkloadAdapter,
};

/// How much of a hook's stderr is kept when it fails, in characters.
pub const MAX_HOOK_EXCERPT_CHARS: usize = 200;
/// How much stderr is read before the rest is discarded, in bytes.
const MAX_STDERR_BYTES: usize = 8 * 1024;
/// How long a hook note may be before it is truncated, in characters.
const MAX_HOOK_NOTE_CHARS: usize = 300;
/// How long a finished hook's pipes may take to close, in milliseconds.
///
/// A hook's grandchildren inherit its pipes, so a backgrounded descendant can
/// hold a pipe open after the hook itself exits. Rather than wait on it
/// forever, the reader is given this grace period and then detached: the
/// reading is reported as failed, and the detached thread ends whenever the
/// pipe finally closes.
const READER_GRACE_MS: u64 = 2_000;

/// How often a running hook is polled, in milliseconds.
const HOOK_POLL_MS: u64 = 5;

/// A verb breadram may ask a hook to perform.
///
/// The verb travels as the final argument: `<program> <args...> <verb>`.
/// There is no other way to steer a hook, so hook output cannot smuggle in a
/// new command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookVerb {
    /// Report whether the workload is busy and what it holds.
    Status,
    /// Drop disposable memory, confirmed by a later status.
    Release,
    /// Bring back what a release dropped.
    Wake,
}

impl HookVerb {
    /// The wire name, passed as the final argument.
    pub fn as_str(self) -> &'static str {
        match self {
            HookVerb::Status => "status",
            HookVerb::Release => "release",
            HookVerb::Wake => "wake",
        }
    }
}

/// Why a hook invocation could not be used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HookError {
    /// The program failed its ownership or shape checks.
    #[error("refusing to run the hook: {0}")]
    Unsafe(String),
    /// The program could not be started or its pipes could not be read.
    #[error("the hook could not be run: {0}")]
    Io(String),
    /// The program outlived its deadline and was killed.
    #[error("the hook did not answer within {0} ms and was killed")]
    Timeout(u64),
    /// The program printed more than the configured cap.
    #[error("the hook printed more than {0} bytes")]
    TooLarge(usize),
    /// The program exited nonzero.
    #[error("the hook exited with status {status}: {excerpt}")]
    NonZero {
        /// The exit status code.
        status: i32,
        /// The start of its stderr.
        excerpt: String,
    },
    /// The program answered with something that is not the documented shape.
    #[error("the hook answer could not be read: {0}")]
    Malformed(String),
}

/// Whether a URL names a hook operation rather than an HTTP one.
///
/// Hook URLs look like `hook:<name>/<verb>` and sit under the configured
/// `hook:<name>` endpoint, so the registry's prefix check covers them without
/// any new allow list.
pub fn is_hook_url(url: &str) -> bool {
    url.starts_with("hook:")
}

/// Runs one hook invocation to completion.
///
/// The runner owns every execution safeguard: ownership checks, a cleared
/// environment, an output cap, and a deadline. The adapter owns the protocol
/// on top: what the output means.
pub trait HookRunner: Send + Sync {
    /// Run `spec` with `verb` as its final argument and return its stdout.
    fn run(&self, spec: &HookSpec, verb: HookVerb) -> Result<String, HookError>;
}

/// What a hook reported about its workload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookStatus {
    /// Whether the workload is doing work right now.
    pub busy: bool,
    /// The host memory the workload currently holds, when reported.
    pub host_ram_bytes: Option<u64>,
    /// The device memory the workload currently holds, when reported.
    pub device_ram_bytes: Option<u64>,
    /// What a person should be told, when the hook said anything.
    pub note: Option<String>,
}

/// What a hook reported about a release.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookRelease {
    /// Whether disposable memory was dropped.
    pub released: bool,
    /// What a person should be told, when the hook said anything.
    pub note: Option<String>,
}

/// What a hook reported about a wake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookWake {
    /// Whether the workload is back.
    pub awake: bool,
    /// What a person should be told, when the hook said anything.
    pub note: Option<String>,
}

/// Read a hook status document.
///
/// The document must be exactly one JSON object with a `busy` flag. Anything
/// else is refused rather than guessed at.
pub fn parse_status(body: &str) -> Result<HookStatus, String> {
    #[derive(serde::Deserialize)]
    struct StatusDoc {
        busy: bool,
        #[serde(default)]
        host_ram_bytes: Option<u64>,
        #[serde(default)]
        device_ram_bytes: Option<u64>,
        #[serde(default)]
        note: Option<String>,
    }
    let doc: StatusDoc =
        serde_json::from_str(body).map_err(|err| format!("expected {{\"busy\": ...}}: {err}"))?;
    Ok(HookStatus {
        busy: doc.busy,
        host_ram_bytes: doc.host_ram_bytes,
        device_ram_bytes: doc.device_ram_bytes,
        note: doc.note.map(|note| truncate_note(&note)),
    })
}

/// Read a hook release document.
pub fn parse_release(body: &str) -> Result<HookRelease, String> {
    #[derive(serde::Deserialize)]
    struct ReleaseDoc {
        released: bool,
        #[serde(default)]
        note: Option<String>,
    }
    let doc: ReleaseDoc = serde_json::from_str(body)
        .map_err(|err| format!("expected {{\"released\": ...}}: {err}"))?;
    Ok(HookRelease {
        released: doc.released,
        note: doc.note.map(|note| truncate_note(&note)),
    })
}

/// Read a hook wake document.
pub fn parse_wake(body: &str) -> Result<HookWake, String> {
    #[derive(serde::Deserialize)]
    struct WakeDoc {
        awake: bool,
        #[serde(default)]
        note: Option<String>,
    }
    let doc: WakeDoc =
        serde_json::from_str(body).map_err(|err| format!("expected {{\"awake\": ...}}: {err}"))?;
    Ok(HookWake {
        awake: doc.awake,
        note: doc.note.map(|note| truncate_note(&note)),
    })
}

/// Shorten a hook note to a journal-safe length, on a character boundary.
fn truncate_note(note: &str) -> String {
    if note.chars().count() <= MAX_HOOK_NOTE_CHARS {
        return note.to_string();
    }
    let prefix: String = note.chars().take(MAX_HOOK_NOTE_CHARS).collect();
    format!("{prefix}... (truncated)")
}

/// A short, bounded piece of hook output for an error message.
fn excerpt(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return "no reason given".to_string();
    }
    if trimmed.chars().count() <= MAX_HOOK_EXCERPT_CHARS {
        return trimmed.to_string();
    }
    let prefix: String = trimmed.chars().take(MAX_HOOK_EXCERPT_CHARS).collect();
    format!("{prefix}... (truncated)")
}

/// Check that a hook program is safe to execute.
///
/// The program must be a regular file, executable, without setuid or setgid
/// bits, and writable by nobody but its owner. On Linux it must additionally
/// be owned by the caller or by root, so a hook cannot be swapped for another
/// user's program. These checks run on every invocation rather than once at
/// startup, so a program replaced after configuration is still refused.
fn check_executable(path: &str) -> Result<(), String> {
    let meta =
        std::fs::metadata(path).map_err(|err| format!("cannot stat hook program {path}: {err}"))?;
    if !meta.is_file() {
        return Err(format!("hook program {path} is not a regular file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let mode = meta.permissions().mode();
        if mode & 0o111 == 0 {
            return Err(format!("hook program {path} is not executable"));
        }
        if mode & 0o6000 != 0 {
            return Err(format!(
                "hook program {path} carries setuid or setgid bits, refusing"
            ));
        }
        if mode & 0o022 != 0 {
            return Err(format!(
                "hook program {path} is writable by group or others, refusing"
            ));
        }
        #[cfg(target_os = "linux")]
        {
            let uid = meta.uid();
            let euid = effective_uid().ok_or_else(|| {
                format!("cannot determine the caller, so hook program {path} is refused")
            })?;
            if uid != euid && uid != 0 {
                return Err(format!(
                    "hook program {path} is owned by neither the caller nor root, refusing"
                ));
            }
        }
    }
    Ok(())
}

/// The effective user id of this process, read from procfs.
///
/// This avoids a libc dependency for a single number breadram already knows
/// how to read: breadram is a procfs reader by trade.
#[cfg(all(unix, target_os = "linux"))]
fn effective_uid() -> Option<u32> {
    let text = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in text.lines() {
        let Some(rest) = line.strip_prefix("Uid:") else {
            continue;
        };
        let real = rest.split_whitespace().next()?;
        return real.parse().ok();
    }
    None
}

/// The real hook runner: spawns the configured program with safeguards.
///
/// Each invocation gets a cleared environment with only `PATH`, `LANG`, and
/// a `BREADRAM_HOOK` marker, a closed stdin, capped stdout and stderr pipes,
/// and a deadline. Standard output beyond the cap is refused; standard error
/// is kept only for failure excerpts.
#[derive(Debug, Default, Clone, Copy)]
pub struct ProcessHookRunner;

impl HookRunner for ProcessHookRunner {
    fn run(&self, spec: &HookSpec, verb: HookVerb) -> Result<String, HookError> {
        check_executable(&spec.command[0]).map_err(HookError::Unsafe)?;
        // A hook saved by an editor at this exact instant holds the program
        // open for writing, which the kernel reports as ETXTBSY on exec. That
        // is transient by nature, so the spawn is retried a few times before
        // the tick reports the hook as unrunnable.
        let mut child = None;
        let mut spawn_error = String::new();
        for _ in 0..4 {
            match spawn_hook(spec, verb) {
                Ok(ready) => {
                    child = Some(ready);
                    break;
                }
                Err(err) if err.kind() == std::io::ErrorKind::ExecutableFileBusy => {
                    spawn_error = err.to_string();
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(err) => {
                    return Err(HookError::Io(format!("cannot start hook: {err}")));
                }
            }
        }
        let mut child =
            child.ok_or_else(|| HookError::Io(format!("cannot start hook: {spawn_error}")))?;

        let mut stdout = child.stdout.take();
        let mut stderr = child.stderr.take();
        let cap = spec.max_output_bytes.saturating_add(1);
        let (stdout_cell, stdout_handle) = spawn_pipe_reader(stdout.take(), cap);
        let (stderr_cell, stderr_handle) = spawn_pipe_reader(stderr.take(), MAX_STDERR_BYTES + 1);

        let deadline = Duration::from_millis(spec.timeout_ms.max(1));
        let started = Instant::now();
        let status = loop {
            match child.try_wait() {
                Err(err) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    detach_pipe_reader(stdout_handle);
                    detach_pipe_reader(stderr_handle);
                    return Err(HookError::Io(format!("cannot wait on the hook: {err}")));
                }
                Ok(Some(status)) => break status,
                Ok(None) => {}
            }
            if started.elapsed() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                detach_pipe_reader(stdout_handle);
                detach_pipe_reader(stderr_handle);
                return Err(HookError::Timeout(spec.timeout_ms));
            }
            std::thread::sleep(Duration::from_millis(HOOK_POLL_MS));
        };

        let stdout_result = collect_pipe_reader(stdout_cell, stdout_handle);
        let stderr_result = collect_pipe_reader(stderr_cell, stderr_handle);
        let (Some(stdout_result), Some(stderr_result)) = (stdout_result, stderr_result) else {
            return Err(HookError::Io(
                "the hook output did not arrive after the hook exited".to_string(),
            ));
        };
        match (status.code(), stdout_result, stderr_result) {
            (_, Ok(out), _) if out.len() > spec.max_output_bytes => {
                Err(HookError::TooLarge(spec.max_output_bytes))
            }
            (Some(0), Ok(out), _) => String::from_utf8(out)
                .map_err(|_| HookError::Malformed("the hook answer is not UTF-8".to_string())),
            (Some(0), Err(err), _) => Err(HookError::Io(err)),
            (Some(code), _, stderr) => Err(HookError::NonZero {
                status: code,
                excerpt: excerpt(&String::from_utf8_lossy(&stderr.unwrap_or_default())),
            }),
            (None, _, stderr) => Err(HookError::NonZero {
                status: -1,
                excerpt: excerpt(&String::from_utf8_lossy(&stderr.unwrap_or_default())),
            }),
        }
    }
}

/// Build and spawn one hook invocation under the execution safeguards.
///
/// The environment is cleared except for a minimal `PATH`, `LANG`, and a
/// `BREADRAM_HOOK` marker; standard input is closed; both outputs are piped
/// for bounded reading.
fn spawn_hook(spec: &HookSpec, verb: HookVerb) -> std::io::Result<std::process::Child> {
    let mut command = std::process::Command::new(&spec.command[0]);
    command.args(&spec.command[1..]);
    command.arg(verb.as_str());
    command.env_clear();
    command.env("PATH", "/usr/bin:/bin");
    command.env("LANG", "C.UTF-8");
    command.env("BREADRAM_HOOK", "1");
    command.stdin(std::process::Stdio::null());
    command.stdout(std::process::Stdio::piped());
    command.stderr(std::process::Stdio::piped());
    command.current_dir("/");
    command.spawn()
}

/// A pipe reading in flight: the cell the reader fills, and the thread.
type PipeCell = std::sync::Arc<std::sync::Mutex<Option<Result<Vec<u8>, String>>>>;
type PipeRead = (PipeCell, std::thread::JoinHandle<()>);

/// Read a pipe on its own thread, storing the outcome in a shared cell.
fn spawn_pipe_reader(pipe: Option<impl Read + Send + 'static>, cap: usize) -> PipeRead {
    let cell = std::sync::Arc::new(std::sync::Mutex::new(None));
    let writer = std::sync::Arc::clone(&cell);
    let handle = std::thread::spawn(move || {
        let outcome = read_bounded(pipe, cap);
        *writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(outcome);
    });
    (cell, handle)
}

/// Stop waiting on a reader without abandoning its thread.
///
/// The detached thread still joins its handle later and ends whenever the
/// pipe finally closes, so no thread is leaked: it is only no longer on this
/// caller's critical path.
fn detach_pipe_reader(handle: std::thread::JoinHandle<()>) {
    std::thread::spawn(move || {
        let _ = handle.join();
    });
}

/// Collect a pipe reading, giving it a grace period after the hook exited.
///
/// Returns `None` when the pipe stayed open past the grace period, which a
/// backgrounded descendant can cause by inheriting the pipe. The reader is
/// detached in that case rather than waited on forever.
fn collect_pipe_reader(
    cell: PipeCell,
    handle: std::thread::JoinHandle<()>,
) -> Option<Result<Vec<u8>, String>> {
    let started = Instant::now();
    let grace = Duration::from_millis(READER_GRACE_MS);
    loop {
        if let Some(outcome) = cell
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            detach_pipe_reader(handle);
            return Some(outcome);
        }
        if started.elapsed() >= grace {
            detach_pipe_reader(handle);
            return None;
        }
        std::thread::sleep(Duration::from_millis(HOOK_POLL_MS));
    }
}

/// Read a pipe to the end, keeping at most `cap` bytes plus one.
///
/// The extra byte is what tells a capped read from a complete one: the caller
/// refuses anything longer than its configured limit.
fn read_bounded(pipe: Option<impl Read>, cap: usize) -> Result<Vec<u8>, String> {
    let Some(mut pipe) = pipe else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    let mut chunk = [0u8; 8 * 1024];
    loop {
        let read = pipe
            .read(&mut chunk)
            .map_err(|err| format!("cannot read the hook output: {err}"))?;
        if read == 0 {
            return Ok(out);
        }
        out.extend_from_slice(&chunk[..read]);
        if out.len() > cap {
            return Ok(out);
        }
    }
}

/// The adapter's cached view of one hook program.
#[derive(Debug, Clone, PartialEq, Eq)]
struct HookObservation {
    fresh: bool,
    busy: Option<bool>,
    host_ram_bytes: Option<u64>,
    device_ram_bytes: Option<u64>,
    note: Option<String>,
    idle_since_ms: Option<u64>,
    last_error: Option<String>,
    observed_at_ms: Option<u64>,
}

impl HookObservation {
    fn empty() -> Self {
        Self {
            fresh: false,
            busy: None,
            host_ram_bytes: None,
            device_ram_bytes: None,
            note: None,
            idle_since_ms: None,
            last_error: None,
            observed_at_ms: None,
        }
    }
}

/// An adapter for one configured hook program.
///
/// The adapter performs no I/O itself: the caller runs the program through a
/// [`HookRunner`] and hands the answer back through [`HookAdapter::refresh`],
/// mirroring the probe pattern HTTP adapters use. Until a status answer has
/// been recorded the workload reports unknown activity, which policy never
/// treats as idle.
pub struct HookAdapter {
    name: String,
    endpoint: String,
    scope: String,
    spec: HookSpec,
    capabilities: AdapterCapabilities,
    state: Mutex<HookObservation>,
}

impl std::fmt::Debug for HookAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HookAdapter")
            .field("name", &self.name)
            .field("endpoint", &self.endpoint)
            .field("scope", &self.scope)
            .field("capabilities", &self.capabilities)
            .field("state", &self.state)
            .finish()
    }
}

impl HookAdapter {
    /// An adapter for one hook program, owning the workload with this scope.
    ///
    /// The capability set starts as observation-only: use
    /// [`HookAdapter::with_capabilities`] to state what the deployment allows.
    pub fn new(
        name: impl Into<String>,
        endpoint: impl Into<String>,
        scope: impl Into<String>,
        spec: HookSpec,
    ) -> Self {
        Self {
            name: name.into(),
            endpoint: endpoint.into(),
            scope: scope.into(),
            spec,
            capabilities: AdapterCapabilities::observation_only(),
            state: Mutex::new(HookObservation::empty()),
        }
    }

    /// State the capability set, for example from the registry.
    pub fn with_capabilities(mut self, capabilities: AdapterCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// The validated hook spec this adapter runs.
    pub fn hook_spec(&self) -> HookSpec {
        self.spec.clone()
    }

    /// The release operation URL for this hook.
    pub fn release_url(&self) -> String {
        format!("{}/release", self.endpoint)
    }

    /// The status marker URL for this hook.
    pub fn status_url(&self) -> String {
        format!("{}/status", self.endpoint)
    }

    /// The wake operation URL for this hook.
    pub fn wake_url(&self) -> String {
        format!("{}/wake", self.endpoint)
    }

    fn state(&self) -> MutexGuard<'_, HookObservation> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Run the status verb and record what came back.
    ///
    /// This is the whole read step. A failed read proves nothing, so it clears
    /// the cached view and the idle clock rather than leaving the previous
    /// reading in place to be acted on.
    pub fn refresh(&self, runner: &dyn HookRunner, at_ms: u64) -> Vec<String> {
        self.note_hook_result(runner.run(&self.spec, HookVerb::Status), at_ms)
    }

    /// Record a status answer, from [`HookAdapter::refresh`] or a confirmation.
    pub fn note_hook_result(&self, result: Result<String, HookError>, at_ms: u64) -> Vec<String> {
        let mut state = self.state();
        state.observed_at_ms = Some(at_ms);
        match result {
            Ok(body) => match parse_status(&body) {
                Ok(status) => {
                    state.fresh = true;
                    state.busy = Some(status.busy);
                    state.host_ram_bytes = status.host_ram_bytes;
                    state.device_ram_bytes = status.device_ram_bytes;
                    state.note = status.note;
                    state.last_error = None;
                    if status.busy {
                        state.idle_since_ms = None;
                    } else if state.idle_since_ms.is_none() {
                        state.idle_since_ms = Some(at_ms);
                    }
                    Vec::new()
                }
                Err(reason) => {
                    state.fresh = false;
                    state.busy = None;
                    state.host_ram_bytes = None;
                    state.device_ram_bytes = None;
                    state.note = None;
                    state.idle_since_ms = None;
                    state.last_error = Some(reason.clone());
                    vec![format!("hook {} answered unreadably: {reason}", self.name)]
                }
            },
            Err(error) => {
                let reason = error.to_string();
                state.fresh = false;
                state.busy = None;
                state.host_ram_bytes = None;
                state.device_ram_bytes = None;
                state.note = None;
                state.idle_since_ms = None;
                state.last_error = Some(reason.clone());
                vec![format!("hook {} could not be read: {reason}", self.name)]
            }
        }
    }

    /// Why the last reading could not be used, when it could not.
    pub fn last_error(&self) -> Option<String> {
        self.state().last_error.clone()
    }

    /// Whether the last status read succeeded.
    pub fn is_fresh(&self) -> bool {
        self.state().fresh
    }

    fn owns_scope(&self, workload: &Workload) -> bool {
        workload.id.scope == self.scope
    }

    /// A negative byte delta that saturates instead of overflowing.
    fn relief(bytes: u64) -> i64 {
        -i64::try_from(bytes).unwrap_or(i64::MAX)
    }
}

impl WorkloadAdapter for HookAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> ProviderKind {
        ProviderKind::Hook
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn capabilities(&self) -> &AdapterCapabilities {
        &self.capabilities
    }

    fn owns(&self, workload: &Workload) -> bool {
        self.owns_scope(workload)
    }

    fn inventory(&self) -> Vec<ManagedResource> {
        if !self.capabilities.observes(AdapterCapability::Inventory) {
            return Vec::new();
        }
        let state = self.state();
        if !state.fresh {
            return Vec::new();
        }
        vec![ManagedResource {
            name: self.name.clone(),
            host_ram_bytes: state.host_ram_bytes,
            device_ram_bytes: state.device_ram_bytes,
            idle: state.busy.is_some_and(|busy| !busy),
        }]
    }

    fn inventory_is_current(&self) -> bool {
        self.capabilities.observes(AdapterCapability::Inventory) && self.state().fresh
    }

    fn activity(&self, workload: &Workload) -> ActivityEvidence {
        if !self.owns_scope(workload) || !self.capabilities.observes(AdapterCapability::Activity) {
            return ActivityEvidence::Unknown;
        }
        let state = self.state();
        if !state.fresh {
            return ActivityEvidence::Unknown;
        }
        match (state.busy, state.idle_since_ms) {
            (Some(true), _) => ActivityEvidence::Active,
            (Some(false), Some(since_ms)) => ActivityEvidence::Idle { since_ms },
            _ => ActivityEvidence::Unknown,
        }
    }

    fn operation(&self, action: ActionKind, workload: &Workload) -> Option<AdapterOperation> {
        if action != ActionKind::UnloadIdleModel || !self.owns_scope(workload) {
            return None;
        }
        if !self.capabilities.supports(AdapterCapability::UnloadModel) {
            return None;
        }
        if !self.state().fresh {
            return None;
        }
        let state = self.state();
        let host_delta = Self::relief(state.host_ram_bytes.unwrap_or(0));
        let device_delta = Self::relief(state.device_ram_bytes.unwrap_or(0));
        let measured = state.host_ram_bytes.is_some() || state.device_ram_bytes.is_some();
        let consequence = state
            .note
            .clone()
            .unwrap_or_else(|| "the next use pays the hook's reload cost".to_string());
        let note = if measured {
            format!(
                "releasing {} through its hook; {consequence}",
                workload.id.scope
            )
        } else {
            format!(
                "releasing {} through its hook with an unmeasured effect; {consequence}",
                workload.id.scope
            )
        };
        drop(state);
        Some(AdapterOperation {
            kind: action,
            request: OperationRequest {
                method: OperationMethod::Post,
                url: self.release_url(),
                auth: Auth::None,
                body: None,
            },
            expected_host_ram_delta: host_delta,
            expected_device_ram_delta: device_delta,
            reversible: true,
            note,
            confirmation: Confirmation {
                request: ProbeRequest::get(self.status_url()),
                expect_body_contains: None,
                expect_body_absent: None,
            },
        })
    }

    fn hook_spec(&self) -> Option<HookSpec> {
        Some(self.spec.clone())
    }

    fn note_hook_result(&self, result: Result<String, HookError>, at_ms: u64) -> Vec<String> {
        HookAdapter::note_hook_result(self, result, at_ms)
    }

    fn refresh_hook(&self, runner: &dyn HookRunner, at_ms: u64) -> Vec<String> {
        self.refresh(runner, at_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DEFAULT_HOOK_OUTPUT_BYTES;
    use crate::inventory::{ControlLevel, MemoryAccounting, WorkloadId};
    use std::collections::VecDeque;

    fn workload(scope: &str) -> Workload {
        Workload {
            id: WorkloadId {
                scope: scope.to_string(),
                generation: 1,
            },
            name: scope.to_string(),
            pids: vec![100],
            accounting: MemoryAccounting::Cgroup {
                current_bytes: Some(1024),
            },
            control: ControlLevel::Eligible,
            limit_bytes: None,
        }
    }

    fn spec() -> HookSpec {
        HookSpec::with_command(vec!["/usr/local/bin/hook".to_string()])
    }

    fn capable_adapter() -> HookAdapter {
        HookAdapter::new("trainer", "hook:trainer", "unit:trainer.service", spec())
            .with_capabilities(AdapterCapabilities::configured(&[
                AdapterCapability::Inventory,
                AdapterCapability::Activity,
                AdapterCapability::UnloadModel,
                AdapterCapability::WakeModel,
            ]))
    }

    /// A scripted runner: expectations are checked in order, answers replayed.
    struct ScriptedRunner {
        expected: Mutex<VecDeque<(HookVerb, Result<String, HookError>)>>,
    }

    impl ScriptedRunner {
        fn new() -> Self {
            Self {
                expected: Mutex::new(VecDeque::new()),
            }
        }

        fn push(self, verb: HookVerb, answer: Result<String, HookError>) -> Self {
            self.expected
                .lock()
                .expect("not poisoned")
                .push_back((verb, answer));
            self
        }
    }

    impl HookRunner for ScriptedRunner {
        fn run(&self, _spec: &HookSpec, verb: HookVerb) -> Result<String, HookError> {
            self.expected
                .lock()
                .expect("not poisoned")
                .pop_front()
                .map_or_else(
                    || panic!("unexpected hook invocation: {verb:?}"),
                    |(expected, answer)| {
                        assert_eq!(expected, verb);
                        answer
                    },
                )
        }
    }

    #[test]
    fn a_status_document_reports_busy_and_memory() {
        let status =
            parse_status(r#"{"busy": true, "host_ram_bytes": 1073741824, "note": "training"}"#)
                .expect("parses");
        assert!(status.busy);
        assert_eq!(status.host_ram_bytes, Some(1_073_741_824));
        assert_eq!(status.device_ram_bytes, None);
        assert_eq!(status.note.as_deref(), Some("training"));
    }

    #[test]
    fn a_status_document_must_name_busy() {
        for body in ["{}", r#"{"host_ram_bytes": 1}"#, "[]", "null", "busy", ""] {
            assert!(parse_status(body).is_err(), "refused: {body}");
        }
        assert!(parse_status("{\"busy\": false} trailing").is_err());
    }

    #[test]
    fn a_release_document_reports_its_outcome() {
        let release =
            parse_release(r#"{"released": true, "note": "dropped cache"}"#).expect("parses");
        assert!(release.released);
        let wake = parse_wake(r#"{"awake": false}"#).expect("parses");
        assert!(!wake.awake);
        assert!(parse_release(r#"{"released": "yes"}"#).is_err());
    }

    #[test]
    fn an_unread_hook_is_unknown_and_owns_nothing() {
        let adapter = capable_adapter();
        assert_eq!(
            adapter.activity(&workload("unit:trainer.service")),
            ActivityEvidence::Unknown
        );
        assert!(adapter.inventory().is_empty());
        assert!(!adapter.inventory_is_current());
        assert!(!adapter.owns(&workload("unit:other.service")));
        assert!(adapter.owns(&workload("unit:trainer.service")));
        assert!(adapter
            .operation(
                ActionKind::UnloadIdleModel,
                &workload("unit:trainer.service")
            )
            .is_none());
    }

    #[test]
    fn a_busy_hook_is_active_and_an_idle_hook_is_idle() {
        let adapter = capable_adapter();
        let runner = ScriptedRunner::new()
            .push(HookVerb::Status, Ok(r#"{"busy": true}"#.to_string()))
            .push(
                HookVerb::Status,
                Ok(r#"{"busy": false, "host_ram_bytes": 512}"#.to_string()),
            );
        assert!(adapter.refresh(&runner, 1_000).is_empty());
        assert_eq!(
            adapter.activity(&workload("unit:trainer.service")),
            ActivityEvidence::Active
        );
        assert!(adapter.refresh(&runner, 2_000).is_empty());
        assert_eq!(
            adapter.activity(&workload("unit:trainer.service")),
            ActivityEvidence::Idle { since_ms: 2_000 }
        );
        let inventory = adapter.inventory();
        assert_eq!(inventory.len(), 1);
        assert_eq!(inventory[0].host_ram_bytes, Some(512));
        assert!(inventory[0].idle);
    }

    #[test]
    fn a_failed_read_clears_the_previous_success() {
        let adapter = capable_adapter();
        let runner = ScriptedRunner::new();
        runner
            .expected
            .lock()
            .expect("not poisoned")
            .push_back((HookVerb::Status, Ok(r#"{"busy": false}"#.to_string())));
        assert!(adapter.refresh(&runner, 1_000).is_empty());
        assert!(adapter.is_fresh());
        runner
            .expected
            .lock()
            .expect("not poisoned")
            .push_back((HookVerb::Status, Err(HookError::Timeout(5_000))));
        let notes = adapter.refresh(&runner, 2_000);
        assert_eq!(notes.len(), 1);
        assert!(!adapter.is_fresh());
        assert_eq!(
            adapter.activity(&workload("unit:trainer.service")),
            ActivityEvidence::Unknown
        );
        assert!(adapter.inventory().is_empty());
        assert!(adapter.last_error().is_some());
    }

    #[test]
    fn a_release_operation_carries_the_hook_effect_and_a_status_confirmation() {
        let adapter = capable_adapter();
        let runner = ScriptedRunner::new();
        runner.expected.lock().expect("not poisoned").push_back((
            HookVerb::Status,
            Ok(r#"{"busy": false, "host_ram_bytes": 1073741824, "note": "cache"}"#.to_string()),
        ));
        assert!(adapter.refresh(&runner, 1_000).is_empty());
        let operation = adapter
            .operation(
                ActionKind::UnloadIdleModel,
                &workload("unit:trainer.service"),
            )
            .expect("release supported");
        assert_eq!(operation.request.url, "hook:trainer/release");
        assert_eq!(operation.request.method, OperationMethod::Post);
        assert!(operation.expected_host_ram_delta < 0);
        assert!(operation.reversible);
        assert!(operation.note.contains("cache"), "{}", operation.note);
        assert_eq!(operation.confirmation.request.url, "hook:trainer/status");
    }

    #[test]
    fn an_observation_only_hook_offers_no_operation() {
        let adapter = HookAdapter::new("trainer", "hook:trainer", "unit:trainer.service", spec())
            .with_capabilities(AdapterCapabilities::configured(&[
                AdapterCapability::Inventory,
                AdapterCapability::Activity,
            ]));
        let runner = ScriptedRunner::new();
        runner
            .expected
            .lock()
            .expect("not poisoned")
            .push_back((HookVerb::Status, Ok(r#"{"busy": false}"#.to_string())));
        assert!(adapter.refresh(&runner, 1_000).is_empty());
        assert!(adapter
            .operation(
                ActionKind::UnloadIdleModel,
                &workload("unit:trainer.service")
            )
            .is_none());
        assert!(adapter
            .operation(ActionKind::SleepModel, &workload("unit:trainer.service"))
            .is_none());
    }

    #[test]
    fn hook_urls_are_recognised_by_scheme() {
        assert!(is_hook_url("hook:trainer/release"));
        assert!(is_hook_url("hook:trainer/status"));
        assert!(!is_hook_url("http://127.0.0.1:11434/api/ps"));
        assert!(!is_hook_url("unix:/run/breadram/hook.sock"));
    }

    #[test]
    fn an_unsafe_program_is_refused_before_anything_runs() {
        assert!(check_executable("/does/not/exist").is_err());
        assert!(check_executable("relative/path").is_err());
    }

    #[test]
    fn the_default_bounds_match_the_config_defaults() {
        let spec = HookSpec::with_command(vec!["/bin/hook".to_string()]);
        assert_eq!(spec.timeout_ms, crate::config::DEFAULT_HOOK_TIMEOUT_MS);
        assert_eq!(spec.max_output_bytes, DEFAULT_HOOK_OUTPUT_BYTES);
    }
}
