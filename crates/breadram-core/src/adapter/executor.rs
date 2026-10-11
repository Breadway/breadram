//! Carrying out an action through a provider adapter.
//!
//! The control executor governs cgroups that the kernel owns. This one governs a
//! runtime that owns its own memory and speaks for itself: a model server, a
//! scheduler. It implements the same [`Executor`] boundary, so the coordinator
//! treats both identically: it revalidates the target, applies, confirms, and
//! journals whatever this reports.
//!
//! Six properties hold here, and each of them is the difference between a
//! release that can be trusted and one that cannot:
//!
//! 1. **No write without a configured, capable owner.** The workload must belong
//!    to an adapter that was explicitly configured, the adapter must hold the
//!    capability for this action, and the operation must come from that adapter.
//!    Nothing is inferred from a model API.
//! 2. **No write without proven idleness.** A release is refused while the
//!    provider reports activity, while it knows nothing, and while its idle
//!    reading has not held for the policy minimum.
//! 3. **No write outside a configured endpoint.** An operation whose URL is not
//!    under a configured provider endpoint is refused before anything is sent.
//! 4. **Confirmed by a read, never assumed.** The effect is checked with a
//!    read-only probe, and the answer is fed back into the adapter, so the
//!    measured before and after come from real readings rather than a guess.
//! 5. **Nothing is silently rounded off.** A provider that answers with an error
//!    status, a transport that times out, and an unconfirmed effect are three
//!    different results.
//! 6. **Nothing is undone blindly.** A provider operation is undone by the
//!    provider's own surface or not at all, so [`Executor::undo`] is unsupported
//!    here rather than a guess, and it says so.

use crate::config::ActionKind;
use crate::coordinator::{
    AppliedChange, Confirmation, Executor, ExecutorError, MemoryObservation, Precondition,
};
use crate::inventory::{Workload, WorkloadId};
use crate::policy::activity::ActivityEvidence;
use crate::policy::MIN_IDLE_MS;
use crate::telemetry::fs::Clock;

use super::capability::AdapterCapability;
use super::hook::{is_hook_url, parse_release, parse_status, parse_wake, HookRunner, HookVerb};
use super::probe::{
    CircuitConfig, OperationTransport, ProbeClient, ProbeError, ProbeRequest, Response, Sleeper,
    Transport, TransportError,
};
use super::{AdapterOperation, AdapterRegistry, ProbeOutcome};
use crate::config::HookSpec;

/// How long a provider operation is given before it is abandoned.
pub const DEFAULT_OPERATION_TIMEOUT_MS: u64 = 5_000;

/// How much of a provider's error body is kept in a reason.
///
/// A provider error is what makes a failure diagnosable, but it is unbounded and
/// may echo something the operator sent, so only a short prefix is ever carried
/// into a journal entry.
pub const MAX_ERROR_EXCERPT_CHARS: usize = 200;

/// Why an action cannot be carried out through a provider.
///
/// Each variant maps to exactly one [`Precondition`], so a refusal is reported as
/// the kind of refusal it is instead of collapsing into "failed".
#[derive(Debug, Clone, PartialEq, Eq)]
enum Refusal {
    /// No configured provider can do this.
    Unsupported(String),
    /// A provider, or the configuration, refused.
    Denied(String),
    /// The target is no longer the workload the action was planned against.
    Gone(String),
}

impl Refusal {
    fn reason(&self) -> &str {
        match self {
            Refusal::Unsupported(reason) | Refusal::Denied(reason) | Refusal::Gone(reason) => {
                reason
            }
        }
    }
}

/// Executes provider actions through the adapters a configuration registered.
///
/// The workload list is the same one the plan was made against, so a target that
/// has been replaced is caught here as well as in the coordinator.
pub struct AdapterExecutor<'a> {
    registry: &'a AdapterRegistry,
    workloads: &'a [Workload],
    transport: &'a dyn Transport,
    operations: &'a dyn OperationTransport,
    sleeper: &'a dyn Sleeper,
    // An executor has to be shareable across threads, so the clock is the one
    // handle that carries the extra bound here.
    clock: &'a (dyn Clock + Sync),
    circuit: CircuitConfig,
    operation_timeout_ms: u64,
    /// Runs hook programs. `None` outside the daemon, which refuses hook
    /// operations rather than running them with no runner.
    hooks: Option<&'a dyn HookRunner>,
}

impl<'a> AdapterExecutor<'a> {
    /// An executor over the registered adapters.
    ///
    /// The read path and the write path are separate transports, so the thing
    /// that confirms an effect cannot be the same value that could write one.
    pub fn new(
        registry: &'a AdapterRegistry,
        workloads: &'a [Workload],
        transport: &'a dyn Transport,
        operations: &'a dyn OperationTransport,
        sleeper: &'a dyn Sleeper,
        clock: &'a (dyn Clock + Sync),
    ) -> Self {
        Self {
            registry,
            workloads,
            transport,
            operations,
            sleeper,
            clock,
            circuit: CircuitConfig::default(),
            operation_timeout_ms: DEFAULT_OPERATION_TIMEOUT_MS,
            hooks: None,
        }
    }

    /// Change the retry, timeout, and size bounds used to confirm an effect.
    pub fn with_circuit(mut self, circuit: CircuitConfig) -> Self {
        self.circuit = circuit;
        self
    }

    /// Change how long an operation is given.
    pub fn with_operation_timeout(mut self, operation_timeout_ms: u64) -> Self {
        self.operation_timeout_ms = operation_timeout_ms;
        self
    }

    /// Run hook operations through this runner.
    ///
    /// The daemon supplies the real process runner. Without one, hook
    /// operations are refused rather than run by some default.
    pub fn with_hook_runner(mut self, runner: &'a dyn HookRunner) -> Self {
        self.hooks = Some(runner);
        self
    }

    /// Whether this executor can carry an action out at all.
    ///
    /// Only the provider-backed actions are claimed. Everything else is left to
    /// the executor that owns it, so the two never compete for an action.
    pub fn handles(kind: ActionKind) -> bool {
        capability_for(kind).is_some()
    }

    /// The workload an id currently refers to.
    fn workload(&self, target: &WorkloadId) -> Option<&'a Workload> {
        self.workloads
            .iter()
            .find(|workload| workload.id.scope == target.scope)
    }

    /// Whether a URL is under a configured provider endpoint.
    fn allows(&self, url: &str) -> bool {
        self.registry
            .endpoints()
            .iter()
            .any(|endpoint| url.starts_with(endpoint.as_str()))
    }

    /// Refuse a URL the transport must not be asked to visit.
    ///
    /// This is checked before anything is sent, so a misconfigured operation is
    /// reported as a refusal rather than as a transport failure after the fact.
    /// Hook URLs never reach the HTTP transport: they name a configured hook
    /// program, so only the endpoint prefix check applies to them.
    fn check_url(&self, url: &str) -> Result<(), Refusal> {
        if !self.allows(url) {
            return Err(Refusal::Denied(format!(
                "{url} is not under a configured provider endpoint, so breadram will not send a request there"
            )));
        }
        if is_hook_url(url) {
            return Ok(());
        }
        if !super::is_encrypted_or_loopback(url) {
            return Err(Refusal::Denied(format!(
                "{url} is not a loopback endpoint, and this build ships no TLS stack"
            )));
        }
        Ok(())
    }

    /// The operation to carry out, or why none can be.
    fn resolve(
        &self,
        request: &crate::coordinator::ActionRequest,
    ) -> Result<AdapterOperation, Refusal> {
        let Some(capability) = capability_for(request.kind) else {
            return Err(Refusal::Unsupported(format!(
                "{} is not an action a provider adapter offers",
                action_name(request.kind)
            )));
        };
        let Some(workload) = self.workload(&request.target) else {
            return Err(Refusal::Gone(format!(
                "the target {} is no longer present",
                request.target.scope
            )));
        };
        if workload.id.generation != request.target.generation {
            return Err(Refusal::Gone(format!(
                "the target {} was replaced (generation {} became {})",
                request.target.scope, request.target.generation, workload.id.generation
            )));
        }
        let owners = self.registry.owners(workload);
        if owners.is_empty() {
            return Err(Refusal::Unsupported(format!(
                "no configured provider owns {}, so nothing can release its memory",
                workload.id.scope
            )));
        }

        let now_ms = self.clock.monotonic_ms();
        let mut reasons = Vec::new();
        for adapter in owners {
            if !adapter.capabilities().supports(capability) {
                reasons.push(format!(
                    "provider {} does not offer {}",
                    adapter.name(),
                    capability.as_str()
                ));
                continue;
            }
            let Some(operation) = adapter.operation(request.kind, workload) else {
                reasons.push(format!(
                    "provider {} has nothing to release on {} right now",
                    adapter.name(),
                    workload.id.scope
                ));
                continue;
            };
            // The provider's own view is the last gate before a write. A busy
            // provider, an unknown one, and one that went idle a moment ago are
            // all left alone.
            let evidence = adapter.activity(workload);
            if !evidence.is_confirmed_idle(now_ms, MIN_IDLE_MS) {
                return Err(Refusal::Denied(activity_refusal(evidence)));
            }
            self.check_url(&operation.request.url)?;
            self.check_url(&operation.confirmation.request.url)?;
            if is_hook_url(&operation.request.url) && self.hooks.is_none() {
                return Err(Refusal::Denied(
                    "the operation needs a hook runner, which this caller did not provide"
                        .to_string(),
                ));
            }
            return Ok(operation);
        }
        Err(Refusal::Unsupported(reasons.join("; ")))
    }

    /// Feed a probe answer back into the adapter that asked for it.
    ///
    /// This is what makes the effect measurable: the coordinator reads the target
    /// after confirming, so the reading has to be current before it does.
    fn record(&self, request: &ProbeRequest, result: Result<Response, ProbeError>) {
        let outcome = ProbeOutcome {
            request: request.clone(),
            result,
        };
        let _ = self.registry.observe(&[outcome], self.clock.monotonic_ms());
    }

    /// The hook owner and spec for a workload, when one exists.
    fn hook_owner(&self, workload: &Workload) -> Option<(String, HookSpec)> {
        self.registry
            .owners(workload)
            .into_iter()
            .find_map(|adapter| {
                adapter
                    .hook_spec()
                    .map(|spec| (adapter.name().to_string(), spec))
            })
    }

    /// Carry out a hook release operation.
    ///
    /// The hook program runs with its configured bounds, and its answer
    /// decides the outcome: a hook that declines reports a denial, a hook
    /// that cannot be run reports a failure, and only `{"released": true}`
    /// counts as applied. Confirmation still happens separately, through a
    /// fresh status read.
    fn apply_hook(
        &self,
        request: &crate::coordinator::ActionRequest,
        operation: &AdapterOperation,
    ) -> Result<AppliedChange, ExecutorError> {
        let Some(runner) = self.hooks else {
            return Err(ExecutorError::Denied(
                "the operation needs a hook runner, which this caller did not provide".to_string(),
            ));
        };
        let Some(workload) = self.workload(&request.target) else {
            return Err(ExecutorError::Failed(format!(
                "the target {} is no longer present",
                request.target.scope
            )));
        };
        let Some((_, spec)) = self.hook_owner(workload) else {
            return Err(ExecutorError::Unsupported(format!(
                "no configured hook owns {}, so nothing can release its memory",
                workload.id.scope
            )));
        };
        match runner.run(&spec, HookVerb::Release) {
            Ok(body) => match parse_release(&body) {
                Ok(release) if release.released => Ok(AppliedChange {
                    values: Vec::new(),
                    note: Some(operation_note(request, operation)),
                }),
                Ok(release) => {
                    Err(ExecutorError::Denied(release.note.unwrap_or_else(|| {
                        "the hook declined the release".to_string()
                    })))
                }
                Err(reason) => Err(ExecutorError::Failed(format!(
                    "the hook answer could not be read: {reason}"
                ))),
            },
            Err(error) => Err(hook_error(error)),
        }
    }

    /// Confirm a hook release with a fresh status read.
    ///
    /// The answer is recorded into the owning adapter before it is judged, so
    /// the next reading of the target is the one the hook just gave. Only a
    /// status that parses and reports not busy confirms the effect.
    fn confirm_hook(
        &self,
        request: &crate::coordinator::ActionRequest,
        _operation: &AdapterOperation,
    ) -> Confirmation {
        let now_ms = self.clock.monotonic_ms();
        let Some(runner) = self.hooks else {
            return Confirmation::NotYet {
                reason: "the operation needs a hook runner, which this caller did not provide"
                    .to_string(),
            };
        };
        let Some(workload) = self.workload(&request.target) else {
            return Confirmation::NotYet {
                reason: format!("the target {} is no longer present", request.target.scope),
            };
        };
        let owners = self.registry.owners(workload);
        let Some(adapter) = owners.iter().find(|adapter| adapter.hook_spec().is_some()) else {
            return Confirmation::NotYet {
                reason: format!(
                    "no configured hook owns {}, so there is nothing to read back",
                    workload.id.scope
                ),
            };
        };
        let (Some(spec), name) = (adapter.hook_spec(), adapter.name().to_string()) else {
            return Confirmation::NotYet {
                reason: "the hook owner has no command to read back".to_string(),
            };
        };
        let result = runner.run(&spec, HookVerb::Status);
        let _ = adapter.note_hook_result(result.clone(), now_ms);
        match result {
            Ok(body) => match parse_status(&body) {
                Ok(status) if !status.busy => Confirmation::Confirmed,
                Ok(_) => Confirmation::NotYet {
                    reason: format!(
                        "hook {name} still reports busy, so the effect is not visible yet"
                    ),
                },
                Err(reason) => Confirmation::NotYet {
                    reason: format!("the effect could not be read back: {reason}"),
                },
            },
            Err(error) => Confirmation::NotYet {
                reason: format!("the effect could not be read back: {error}"),
            },
        }
    }
}

impl Executor for AdapterExecutor<'_> {
    fn supports(&self, kind: ActionKind) -> bool {
        Self::handles(kind) && !self.registry.is_empty()
    }

    fn precondition(&self, request: &crate::coordinator::ActionRequest) -> Precondition {
        match self.resolve(request) {
            Ok(_) => Precondition::Ready,
            Err(Refusal::Unsupported(reason)) => Precondition::Unsupported { reason },
            Err(Refusal::Denied(reason)) => Precondition::Denied { reason },
            Err(Refusal::Gone(reason)) => Precondition::TargetGone { reason },
        }
    }

    fn apply(
        &self,
        request: &crate::coordinator::ActionRequest,
    ) -> Result<AppliedChange, ExecutorError> {
        let operation = self.resolve(request).map_err(|refusal| match refusal {
            Refusal::Unsupported(reason) => ExecutorError::Unsupported(reason),
            Refusal::Denied(reason) => ExecutorError::Denied(reason),
            Refusal::Gone(reason) => ExecutorError::Failed(reason),
        })?;
        if is_hook_url(&operation.request.url) {
            return self.apply_hook(request, &operation);
        }
        match self
            .operations
            .send_operation(&operation.request, self.operation_timeout_ms)
        {
            Ok(response) if (200..300).contains(&response.status) => Ok(AppliedChange {
                // A provider setting is not a daemon-owned value: there is no
                // original to remember, and the provider itself owns restoring
                // it, so nothing is claimed as reversible here.
                values: Vec::new(),
                note: Some(operation_note(request, &operation)),
            }),
            Ok(response) => Err(ExecutorError::Denied(format!(
                "provider answered {} to the {} request: {}",
                response.status,
                operation.request.method.as_str(),
                excerpt(&response.body)
            ))),
            Err(TransportError::Timeout(ms)) => Err(ExecutorError::Timeout(format!(
                "the provider did not answer within {ms} ms"
            ))),
            Err(TransportError::Failure(message)) => Err(ExecutorError::Disconnected(message)),
        }
    }

    fn confirm(&self, request: &crate::coordinator::ActionRequest) -> Confirmation {
        let operation = match self.resolve(request) {
            Ok(operation) => operation,
            Err(refusal) => {
                return Confirmation::NotYet {
                    reason: format!(
                        "the operation could not be reconstructed to confirm it: {}",
                        refusal.reason()
                    ),
                }
            }
        };
        let probe = ProbeClient::new(
            self.transport,
            self.sleeper,
            self.clock,
            self.circuit,
            self.registry.endpoints(),
        );
        if is_hook_url(&operation.confirmation.request.url) {
            return self.confirm_hook(request, &operation);
        }
        let result = probe.probe(&operation.confirmation.request);
        // The answer is recorded before it is judged, so the next reading of the
        // target is the one the provider just gave, whatever it said.
        self.record(&operation.confirmation.request, result.clone());
        match result {
            Ok(response) if operation.confirmation.confirms(&response.body) => {
                Confirmation::Confirmed
            }
            Ok(response) => Confirmation::NotYet {
                reason: unconfirmed(&operation.confirmation, &response.body),
            },
            Err(error) => Confirmation::NotYet {
                reason: format!("the effect could not be read back: {error}"),
            },
        }
    }

    fn observe(&self, target: &WorkloadId) -> MemoryObservation {
        let Some(workload) = self.workload(target) else {
            return MemoryObservation::default();
        };
        let mut host_ram_bytes: Option<u64> = None;
        let mut device_ram_bytes: Option<u64> = None;
        for adapter in self.registry.owners(workload) {
            let resources = adapter.inventory();
            for resource in &resources {
                add(&mut host_ram_bytes, resource.host_ram_bytes);
                add(&mut device_ram_bytes, resource.device_ram_bytes);
            }
            if resources.is_empty() && adapter.inventory_is_current() {
                // The provider was read and holds nothing, which is a measured
                // zero rather than a missing reading. Reporting it as unknown
                // would hide the release that just happened.
                host_ram_bytes = Some(host_ram_bytes.unwrap_or(0));
                device_ram_bytes = Some(device_ram_bytes.unwrap_or(0));
            }
        }
        MemoryObservation {
            host_ram_bytes,
            device_ram_bytes,
        }
    }

    fn read_value(&self, key: &str) -> Result<Option<String>, ExecutorError> {
        Err(ExecutorError::Unsupported(format!(
            "a provider setting is not a daemon-owned value, so {key} is not readable through this executor"
        )))
    }

    fn write_value(&self, key: &str, _value: &str) -> Result<(), ExecutorError> {
        Err(ExecutorError::Unsupported(format!(
            "a provider setting is not a daemon-owned value, so {key} is not writable through this executor"
        )))
    }

    fn undo(&self, request: &crate::coordinator::ActionRequest) -> Result<(), ExecutorError> {
        let Some(workload) = self.workload(&request.target) else {
            return Err(ExecutorError::Failed(format!(
                "the target {} is no longer present",
                request.target.scope
            )));
        };
        let owners = self.registry.owners(workload);
        let mut hook_seen = false;
        for adapter in &owners {
            let Some(spec) = adapter.hook_spec() else {
                continue;
            };
            hook_seen = true;
            if !adapter
                .capabilities()
                .supports(AdapterCapability::WakeModel)
            {
                continue;
            }
            let Some(runner) = self.hooks else {
                return Err(ExecutorError::Denied(
                    "waking the hook needs a hook runner, which this caller did not provide"
                        .to_string(),
                ));
            };
            match runner.run(&spec, HookVerb::Wake) {
                Ok(body) => match parse_wake(&body) {
                    Ok(wake) if wake.awake => return Ok(()),
                    Ok(wake) => {
                        return Err(ExecutorError::Denied(wake.note.unwrap_or_else(|| {
                            format!("hook {} declined the wake", adapter.name())
                        })));
                    }
                    Err(reason) => {
                        return Err(ExecutorError::Failed(format!(
                            "the hook answer could not be read: {reason}"
                        )));
                    }
                },
                Err(error) => return Err(hook_error(error)),
            }
        }
        if hook_seen {
            return Err(ExecutorError::Unsupported(format!(
                "hook {} does not offer wake, so there is nothing to undo through",
                owners
                    .iter()
                    .find_map(|adapter| { adapter.hook_spec().map(|_| adapter.name().to_string()) })
                    .unwrap_or_else(|| workload.id.scope.clone())
            )));
        }
        Err(ExecutorError::Unsupported(
            "this executor cannot undo that action".to_string(),
        ))
    }
}

/// The capability an action needs from an adapter, if any.
fn capability_for(kind: ActionKind) -> Option<AdapterCapability> {
    match kind {
        ActionKind::UnloadIdleModel => Some(AdapterCapability::UnloadModel),
        ActionKind::SleepModel => Some(AdapterCapability::SleepModel),
        // Cgroup limits belong to the control executor, and the cooperative
        // actions have no implementation at all yet, so neither is claimed here.
        ActionKind::CgroupSoftLimit
        | ActionKind::ReclaimProtection
        | ActionKind::ReduceJobConcurrency
        | ActionKind::QueueBackgroundJob => None,
    }
}

/// The stable name of an action, for a reason that has to be readable.
fn action_name(kind: ActionKind) -> &'static str {
    match kind {
        ActionKind::CgroupSoftLimit => "cgroup_soft_limit",
        ActionKind::ReclaimProtection => "reclaim_protection",
        ActionKind::UnloadIdleModel => "unload_idle_model",
        ActionKind::SleepModel => "sleep_model",
        ActionKind::ReduceJobConcurrency => "reduce_job_concurrency",
        ActionKind::QueueBackgroundJob => "queue_background_job",
    }
}

/// Why a provider's own activity reading blocks a release.
fn activity_refusal(evidence: ActivityEvidence) -> String {
    match evidence {
        ActivityEvidence::Active => {
            "the provider reports activity, so nothing is released".to_string()
        }
        ActivityEvidence::Idle { .. } => format!(
            "the provider has been idle for less than the {} ms policy minimum",
            MIN_IDLE_MS
        ),
        ActivityEvidence::Unknown => {
            "nothing has proven this backend idle, and unknown is never idle".to_string()
        }
    }
}

/// What a person should be told about an accepted operation.
///
/// The adapter's own note comes first, because it carries the consequence -
/// a model release costs a cold start, and that is the part an operator needs.
/// When the provider's own expectation differs from the plan's estimate, both
/// figures are stated: the plan sized its relief from the workload's memory,
/// while the provider knows what it actually holds, and a reader comparing the
/// requested and observed effect should not have to guess which is which.
fn operation_note(
    request: &crate::coordinator::ActionRequest,
    operation: &AdapterOperation,
) -> String {
    let mut note = operation.note.clone();
    if operation.expected_host_ram_delta != request.expected_host_ram_delta
        || operation.expected_device_ram_delta != request.expected_device_ram_delta
    {
        note.push_str(&format!(
            "; the provider expects {} host bytes and {} device bytes to change, where the plan estimated {} host and {} device",
            operation.expected_host_ram_delta,
            operation.expected_device_ram_delta,
            request.expected_host_ram_delta,
            request.expected_device_ram_delta
        ));
    }
    note
}

/// Why a response body does not confirm the effect.
fn unconfirmed(confirmation: &super::Confirmation, body: &str) -> String {
    match (
        &confirmation.expect_body_contains,
        &confirmation.expect_body_absent,
    ) {
        (None, None) => "the confirmation checks nothing, which is not a confirmation".to_string(),
        (Some(expected), _) if !body.contains(expected.as_str()) => {
            format!("the provider does not report {expected}, so the effect is not visible yet")
        }
        (_, Some(unexpected)) if body.contains(unexpected.as_str()) => {
            format!("the provider still reports {unexpected}, so the effect is not visible yet")
        }
        _ => "the provider response did not confirm the effect".to_string(),
    }
}

/// Map a hook failure onto the executor error it most resembles.
///
/// A hook that cannot be reached is disconnected; a hook that answers badly
/// failed; a hook that refuses, whether by policy or by deadline, is denied
/// or timed out. The mapping keeps the coordinator's retry and journal
/// semantics identical for hooks and HTTP providers.
fn hook_error(error: super::hook::HookError) -> ExecutorError {
    use super::hook::HookError;
    match error {
        HookError::Timeout(ms) => {
            ExecutorError::Timeout(format!("the hook did not answer within {ms} ms"))
        }
        HookError::Unsafe(reason) => ExecutorError::Denied(reason),
        HookError::NonZero { status, excerpt } => {
            ExecutorError::Denied(format!("hook exited with status {status}: {excerpt}"))
        }
        HookError::Io(reason) => ExecutorError::Disconnected(reason),
        HookError::TooLarge(cap) => {
            ExecutorError::Failed(format!("the hook printed more than {cap} bytes"))
        }
        HookError::Malformed(reason) => ExecutorError::Failed(reason),
    }
}

/// A short, bounded piece of a provider's error body.
fn excerpt(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return "no reason given".to_string();
    }
    if trimmed.chars().count() <= MAX_ERROR_EXCERPT_CHARS {
        return trimmed.to_string();
    }
    let prefix: String = trimmed.chars().take(MAX_ERROR_EXCERPT_CHARS).collect();
    format!("{prefix}... (truncated)")
}

/// Add a reading to a running total, keeping "not measured" distinct from zero.
fn add(total: &mut Option<u64>, value: Option<u64>) {
    if let Some(value) = value {
        *total = Some(total.unwrap_or(0).saturating_add(value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{
        AdapterCapabilities, Confirmation as AdapterConfirmation, NoSleep, OllamaAdapter,
        ScriptedTransport,
    };
    use crate::adapter::{HookError, HookRunner, HookSpec, HookVerb};
    use crate::config::{Config, ProviderEndpoint, ProviderKind};
    use crate::coordinator::ActionRequest;
    use crate::inventory::{ControlLevel, MemoryAccounting};
    use crate::serving::TrackingCompleteness;
    use crate::telemetry::fs::test_doubles::FixedClock;
    use crate::ManagementMode;

    const ENDPOINT: &str = "http://127.0.0.1:11434";
    const SCOPE: &str = "unit:ollama.service";

    fn workload() -> Workload {
        Workload {
            id: WorkloadId {
                scope: SCOPE.to_string(),
                generation: 1,
            },
            name: "ollama.service".to_string(),
            pids: vec![100],
            accounting: MemoryAccounting::Cgroup {
                current_bytes: Some(1024),
            },
            control: ControlLevel::Eligible,
            limit_bytes: None,
        }
    }

    /// A fixed instant the recorded expiry below is read against.
    const FIXED_NOW_MS: u64 = 1_791_676_800_000;

    /// One loaded model, held with an ordinary bounded keep-alive so it is
    /// releasable.
    fn loaded(model: &str) -> Response {
        Response::new(
            200,
            format!(
                r#"{{"models":[{{"name":"{model}","size":4294967296,"size_vram":3221225472,"expires_at":"2026-10-11T16:00:00Z"}}]}}"#
            ),
        )
    }

    /// The capability set a configured provider with automation gets.
    fn writable_capabilities() -> AdapterCapabilities {
        AdapterCapabilities::configured(&[
            AdapterCapability::Discovery,
            AdapterCapability::Inventory,
            AdapterCapability::Activity,
            AdapterCapability::UnloadModel,
        ])
    }

    /// An adapter that declares every request visible, which is what makes a
    /// loaded model releasable, pinned to a fixed keep-alive instant.
    fn tracking_adapter() -> OllamaAdapter {
        OllamaAdapter::new("ollama", ENDPOINT, SCOPE)
            .with_tracking(TrackingCompleteness::Complete, 0)
            .with_reference_time(FIXED_NOW_MS)
            .with_capabilities(writable_capabilities())
    }

    fn registry(allow_automation: bool) -> AdapterRegistry {
        let config = Config {
            providers: vec![ProviderEndpoint {
                name: "ollama".to_string(),
                kind: ProviderKind::Ollama,
                url: ENDPOINT.to_string(),
                scope: Some(SCOPE.to_string()),
                credential_ref: None,
                allow_automation,
                command: Vec::new(),
                hook_timeout_ms: None,
                hook_max_output_bytes: None,
            }],
            mode: ManagementMode::Automatic,
            ..Config::default()
        };
        AdapterRegistry::from_config(&config).0
    }

    /// A registry whose adapter declares that every request is visible, which is
    /// what makes a loaded model releasable.
    fn tracking_registry() -> AdapterRegistry {
        let mut registry = AdapterRegistry::new();
        registry.register(Box::new(tracking_adapter()));
        registry
    }

    /// Feed a `/api/ps` answer into a registry's adapter.
    fn observe_loaded(registry: &AdapterRegistry, response: Response, at_ms: u64) {
        registry.observe(
            &[ProbeOutcome {
                request: ProbeRequest::get(format!("{ENDPOINT}/api/ps")),
                result: Ok(response),
            }],
            at_ms,
        );
    }

    fn request(kind: ActionKind) -> ActionRequest {
        ActionRequest::new(workload().id.clone(), kind, "testing", 60_000)
    }

    fn executor<'a>(
        registry: &'a AdapterRegistry,
        workloads: &'a [Workload],
        probes: &'a ScriptedTransport,
        operations: &'a ScriptedTransport,
        clock: &'a FixedClock,
    ) -> AdapterExecutor<'a> {
        AdapterExecutor::new(registry, workloads, probes, operations, &NoSleep, clock)
    }

    #[test]
    fn a_url_outside_a_configured_endpoint_is_refused() {
        let registry = registry(true);
        let probes = ScriptedTransport::new();
        let operations = ScriptedTransport::new();
        let adapter = executor(&registry, &[], &probes, &operations, &FixedClock(0));
        let refusal = adapter
            .check_url("http://127.0.0.1:9999/api/generate")
            .expect_err("not configured");
        assert!(matches!(refusal, Refusal::Denied(_)));
        assert!(refusal.reason().contains("not under a configured"));
        assert!(adapter
            .check_url(&format!("{ENDPOINT}/api/generate"))
            .is_ok());
    }

    #[test]
    fn a_plaintext_remote_endpoint_is_refused_before_anything_is_sent() {
        let mut registry = AdapterRegistry::new();
        registry.register(Box::new(OllamaAdapter::new(
            "remote",
            "http://10.0.0.5:8000",
            SCOPE,
        )));
        let probes = ScriptedTransport::new();
        let operations = ScriptedTransport::new();
        let adapter = executor(&registry, &[], &probes, &operations, &FixedClock(0));
        let refusal = adapter
            .check_url("http://10.0.0.5:8000/api/generate")
            .expect_err("a remote host in the clear");
        assert!(
            refusal.reason().contains("loopback"),
            "{}",
            refusal.reason()
        );
        // A URL on another host is refused as unconfigured, whatever its scheme.
        let elsewhere = adapter
            .check_url("https://gpu.example.net:8000/api/generate")
            .expect_err("not a configured endpoint");
        assert!(
            elsewhere.reason().contains("not under a configured"),
            "{}",
            elsewhere.reason()
        );
    }

    #[test]
    fn an_empty_registry_supports_nothing() {
        let registry = AdapterRegistry::new();
        let probes = ScriptedTransport::new();
        let operations = ScriptedTransport::new();
        let adapter = executor(&registry, &[], &probes, &operations, &FixedClock(0));
        assert!(!adapter.supports(ActionKind::UnloadIdleModel));
    }

    #[test]
    fn an_observation_only_provider_is_never_asked_to_write() {
        let registry = registry(false);
        let workloads = [workload()];
        let probes = ScriptedTransport::new();
        let operations = ScriptedTransport::new();
        let adapter = executor(&registry, &workloads, &probes, &operations, &FixedClock(0));
        assert!(adapter.supports(ActionKind::UnloadIdleModel));
        let reason = match adapter.precondition(&request(ActionKind::UnloadIdleModel)) {
            Precondition::Unsupported { reason } => reason,
            other => panic!("expected a refusal, got {other:?}"),
        };
        assert!(reason.contains("does not offer unload_model"), "{reason}");
        assert_eq!(operations.operation_count(), 0);
    }

    #[test]
    fn a_release_is_refused_until_the_provider_has_been_observed() {
        let registry = tracking_registry();
        let workloads = [workload()];
        let probes = ScriptedTransport::new();
        let operations = ScriptedTransport::new();
        let clock = FixedClock(60_000);
        let adapter = executor(&registry, &workloads, &probes, &operations, &clock);

        // Never observed: nothing is proven, and unknown is never idle.
        let reason = match adapter.precondition(&request(ActionKind::UnloadIdleModel)) {
            Precondition::Unsupported { reason } => reason,
            other => panic!("expected a refusal, got {other:?}"),
        };
        assert!(reason.contains("nothing to release"), "{reason}");
        assert_eq!(operations.operation_count(), 0);

        // Once observed, an idle provider with a releasable model is ready.
        observe_loaded(&registry, loaded("llama3:8b"), clock.monotonic_ms());
        assert_eq!(
            adapter.precondition(&request(ActionKind::UnloadIdleModel)),
            Precondition::Ready
        );
    }

    #[test]
    fn a_release_is_refused_while_a_request_is_in_flight() {
        let clock = FixedClock(60_000);
        let mut registry = AdapterRegistry::new();
        let adapter = tracking_adapter();
        // A request that started a second ago is still running, which is what the
        // tracker reports as activity.
        let _lease = adapter.begin_request(59_000);
        registry.register(Box::new(adapter));
        observe_loaded(&registry, loaded("llama3:8b"), clock.monotonic_ms());

        let workloads = [workload()];
        let probes = ScriptedTransport::new();
        let operations = ScriptedTransport::new();
        let executor = executor(&registry, &workloads, &probes, &operations, &clock);
        match executor.precondition(&request(ActionKind::UnloadIdleModel)) {
            Precondition::Denied { reason } => {
                assert!(reason.contains("reports activity"), "{reason}")
            }
            other => panic!("expected a denial, got {other:?}"),
        }
        assert_eq!(
            operations.operation_count(),
            0,
            "nothing is written while the provider is serving"
        );
    }

    #[test]
    fn an_unloadable_model_is_ready_and_its_expected_change_is_negative() {
        let clock = FixedClock(60_000);
        let registry = tracking_registry();
        observe_loaded(&registry, loaded("llama3:8b"), clock.monotonic_ms());
        let workloads = [workload()];
        let probes = ScriptedTransport::new();
        let operations = ScriptedTransport::new();
        let executor = executor(&registry, &workloads, &probes, &operations, &clock);
        assert_eq!(
            executor.precondition(&request(ActionKind::UnloadIdleModel)),
            Precondition::Ready
        );
        let operation = executor
            .resolve(&request(ActionKind::UnloadIdleModel))
            .expect("resolves");
        assert_eq!(operation.kind, ActionKind::UnloadIdleModel);
        assert_eq!(operation.request.method.as_str(), "POST");
        assert!(operation.request.url.ends_with("/api/generate"));
        assert_eq!(operation.expected_host_ram_delta, -1_073_741_824);
        assert_eq!(operation.expected_device_ram_delta, -3_221_225_472);
        assert!(operation.note.contains("cold-start"), "{}", operation.note);
    }

    #[test]
    fn a_provider_that_cannot_be_reached_is_reported_not_guessed() {
        let clock = FixedClock(60_000);
        let registry = tracking_registry();
        observe_loaded(&registry, loaded("llama3:8b"), clock.monotonic_ms());
        let workloads = [workload()];
        let probes = ScriptedTransport::new();
        let operations =
            ScriptedTransport::new().push_operation(Err(TransportError::Timeout(5_000)));
        let executor = executor(&registry, &workloads, &probes, &operations, &clock);
        match executor.apply(&request(ActionKind::UnloadIdleModel)) {
            Err(ExecutorError::Timeout(reason)) => assert!(reason.contains("5000 ms"), "{reason}"),
            other => panic!("expected a timeout, got {other:?}"),
        }
        assert_eq!(operations.operation_count(), 1);
    }

    #[test]
    fn a_refused_operation_is_reported_with_the_provider_status() {
        let clock = FixedClock(60_000);
        let registry = tracking_registry();
        observe_loaded(&registry, loaded("llama3:8b"), clock.monotonic_ms());
        let workloads = [workload()];
        let probes = ScriptedTransport::new();
        let operations = ScriptedTransport::new()
            .push_operation(Ok(Response::new(400, r#"{"error":"model is pinned"}"#)));
        let executor = executor(&registry, &workloads, &probes, &operations, &clock);
        match executor.apply(&request(ActionKind::UnloadIdleModel)) {
            Err(ExecutorError::Denied(reason)) => {
                assert!(reason.contains("400"), "{reason}");
                assert!(reason.contains("model is pinned"), "{reason}");
            }
            other => panic!("expected a denial, got {other:?}"),
        }
    }

    #[test]
    fn the_target_generation_is_revalidated_before_anything_is_sent() {
        let clock = FixedClock(60_000);
        let registry = tracking_registry();
        observe_loaded(&registry, loaded("llama3:8b"), clock.monotonic_ms());
        let mut replaced = workload();
        replaced.id.generation = 2;
        let workloads = [replaced];
        let probes = ScriptedTransport::new();
        let operations = ScriptedTransport::new();
        let executor = executor(&registry, &workloads, &probes, &operations, &clock);
        match executor.precondition(&request(ActionKind::UnloadIdleModel)) {
            Precondition::TargetGone { reason } => {
                assert!(reason.contains("generation 1 became 2"), "{reason}")
            }
            other => panic!("expected a gone target, got {other:?}"),
        }
        assert_eq!(operations.operation_count(), 0);
    }

    #[test]
    fn a_confirmed_release_measures_the_change_from_the_provider_reading() {
        let clock = FixedClock(60_000);
        let registry = tracking_registry();
        observe_loaded(&registry, loaded("llama3:8b"), clock.monotonic_ms());
        let workloads = [workload()];
        // The provider still lists the model at the first confirmation and drops
        // it at the second, so an unconfirmed release is distinguishable.
        let probes = ScriptedTransport::new()
            .push(Ok(loaded("llama3:8b")))
            .push(Ok(Response::new(200, r#"{"models":[]}"#)));
        let operations = ScriptedTransport::new().push_operation(Ok(Response::new(200, "{}")));
        let executor = executor(&registry, &workloads, &probes, &operations, &clock);
        let request = request(ActionKind::UnloadIdleModel);

        let before = executor.observe(&request.target);
        assert_eq!(before.host_ram_bytes, Some(1_073_741_824));
        assert_eq!(before.device_ram_bytes, Some(3_221_225_472));

        let change = executor.apply(&request).expect("the provider accepts it");
        assert!(change.note.expect("a note").contains("cold-start"));
        match executor.confirm(&request) {
            Confirmation::NotYet { reason } => {
                assert!(reason.contains("still reports llama3:8b"), "{reason}")
            }
            other => panic!("the model is still listed, so this is not confirmed: {other:?}"),
        }
        // The reading was taken from the provider, not invented, so the change is
        // still the pre-release figure.
        assert_eq!(
            executor.observe(&request.target).host_ram_bytes,
            Some(1_073_741_824)
        );

        // Now the second confirmation sees it gone, and the measurement follows.
        match executor.confirm(&request) {
            Confirmation::Confirmed => {}
            other => panic!("expected a confirmation, got {other:?}"),
        }
        let after = executor.observe(&request.target);
        assert_eq!(
            after.host_ram_bytes,
            Some(0),
            "a fresh reading of an empty set is a measured zero"
        );
        assert_eq!(after.device_ram_bytes, Some(0));
    }

    #[test]
    fn a_setting_is_not_a_daemon_owned_value() {
        let registry = tracking_registry();
        let workloads = [workload()];
        let probes = ScriptedTransport::new();
        let operations = ScriptedTransport::new();
        let executor = executor(&registry, &workloads, &probes, &operations, &FixedClock(0));
        assert!(matches!(
            executor.read_value("provider.ollama.keep_alive"),
            Err(ExecutorError::Unsupported(_))
        ));
        assert!(matches!(
            executor.write_value("provider.ollama.keep_alive", "0"),
            Err(ExecutorError::Unsupported(_))
        ));
        assert!(matches!(
            executor.undo(&request(ActionKind::UnloadIdleModel)),
            Err(ExecutorError::Unsupported(_))
        ));
    }

    #[test]
    fn a_probe_result_is_recorded_against_the_endpoint_it_came_from() {
        let registry = tracking_registry();
        let response = loaded("llama3:8b");
        registry.observe(
            &[
                ProbeOutcome {
                    request: ProbeRequest::get("http://127.0.0.1:9999/api/ps"),
                    result: Ok(response.clone()),
                },
                ProbeOutcome {
                    request: ProbeRequest::get(format!("{ENDPOINT}/api/ps")),
                    result: Ok(response),
                },
            ],
            1_000,
        );
        let adapter = registry.adapters().next().expect("one adapter");
        assert_eq!(adapter.inventory().len(), 1);
        assert_eq!(adapter.inventory()[0].name, "llama3:8b");
    }

    #[test]
    fn only_provider_actions_are_claimed() {
        assert!(AdapterExecutor::handles(ActionKind::UnloadIdleModel));
        assert!(AdapterExecutor::handles(ActionKind::SleepModel));
        assert!(!AdapterExecutor::handles(ActionKind::CgroupSoftLimit));
        assert!(!AdapterExecutor::handles(ActionKind::ReclaimProtection));
        assert!(!AdapterExecutor::handles(ActionKind::ReduceJobConcurrency));
        assert!(!AdapterExecutor::handles(ActionKind::QueueBackgroundJob));
    }

    #[test]
    fn an_error_body_excerpt_is_bounded() {
        assert_eq!(excerpt("  "), "no reason given");
        assert_eq!(excerpt("nope"), "nope");
        let long = "x".repeat(MAX_ERROR_EXCERPT_CHARS + 50);
        let excerpt = excerpt(&long);
        assert!(excerpt.ends_with("... (truncated)"), "{excerpt}");
        assert!(
            excerpt.chars().count() < MAX_ERROR_EXCERPT_CHARS + 30,
            "a reason stays short: {} chars",
            excerpt.chars().count()
        );
    }

    #[test]
    fn a_reading_of_zero_is_not_the_same_as_no_reading() {
        let mut total = None;
        add(&mut total, None);
        assert_eq!(total, None);
        add(&mut total, Some(0));
        assert_eq!(total, Some(0));
        add(&mut total, Some(5));
        assert_eq!(total, Some(5));
    }

    #[test]
    fn an_unconfirmed_reason_names_what_is_missing() {
        let absent = AdapterConfirmation::absent(ProbeRequest::get("u"), "llama3");
        assert!(unconfirmed(&absent, "llama3").contains("still reports llama3"));
        let contains = AdapterConfirmation::contains(ProbeRequest::get("u"), "ok");
        assert!(unconfirmed(&contains, "{}").contains("does not report ok"));
    }

    const HOOK_ENDPOINT: &str = "hook:trainer";
    const HOOK_SCOPE: &str = "unit:trainer.service";

    fn hook_workload() -> Workload {
        Workload {
            id: WorkloadId {
                scope: HOOK_SCOPE.to_string(),
                generation: 1,
            },
            name: "trainer.service".to_string(),
            pids: vec![100],
            accounting: MemoryAccounting::Cgroup {
                current_bytes: Some(1024),
            },
            control: ControlLevel::Eligible,
            limit_bytes: None,
        }
    }

    fn hook_registry(allow_automation: bool) -> AdapterRegistry {
        let config = Config {
            providers: vec![ProviderEndpoint {
                name: "trainer".to_string(),
                kind: ProviderKind::Hook,
                url: HOOK_ENDPOINT.to_string(),
                scope: Some(HOOK_SCOPE.to_string()),
                credential_ref: None,
                allow_automation,
                command: vec!["/usr/local/bin/breadram-hook-trainer".to_string()],
                hook_timeout_ms: None,
                hook_max_output_bytes: None,
            }],
            mode: ManagementMode::Automatic,
            ..Config::default()
        };
        let (registry, notes) = AdapterRegistry::from_config(&config);
        assert!(
            notes
                .iter()
                .all(|note| !note.contains("contributes nothing")),
            "{notes:?}"
        );
        assert_eq!(registry.names(), vec!["trainer"]);
        registry
    }

    /// A canned hook runner: one answer per verb, every call recorded.
    #[derive(Default)]
    struct FakeHooks {
        status: std::sync::Mutex<Option<Result<String, HookError>>>,
        release: std::sync::Mutex<Option<Result<String, HookError>>>,
        wake: std::sync::Mutex<Option<Result<String, HookError>>>,
        calls: std::sync::Mutex<Vec<HookVerb>>,
    }

    impl FakeHooks {
        fn with_status(body: &str) -> Self {
            Self {
                status: std::sync::Mutex::new(Some(Ok(body.to_string()))),
                ..Self::default()
            }
        }
    }

    impl HookRunner for FakeHooks {
        fn run(&self, _spec: &HookSpec, verb: HookVerb) -> Result<String, HookError> {
            self.calls.lock().expect("not poisoned").push(verb);
            let slot = match verb {
                HookVerb::Status => &self.status,
                HookVerb::Release => &self.release,
                HookVerb::Wake => &self.wake,
            };
            slot.lock()
                .expect("not poisoned")
                .clone()
                .unwrap_or_else(|| panic!("no {verb:?} answer scripted"))
        }
    }

    fn hook_request() -> ActionRequest {
        ActionRequest::new(
            hook_workload().id.clone(),
            ActionKind::UnloadIdleModel,
            "testing",
            60_000,
        )
    }

    fn hook_executor<'a>(
        registry: &'a AdapterRegistry,
        workloads: &'a [Workload],
        probes: &'a ScriptedTransport,
        operations: &'a ScriptedTransport,
        clock: &'a FixedClock,
        hooks: Option<&'a FakeHooks>,
    ) -> AdapterExecutor<'a> {
        let base = AdapterExecutor::new(registry, workloads, probes, operations, &NoSleep, clock);
        match hooks {
            Some(runner) => base.with_hook_runner(runner),
            None => base,
        }
    }

    #[test]
    fn a_hook_release_is_refused_without_a_runner() {
        let registry = hook_registry(true);
        let seeder = FakeHooks::with_status(r#"{"busy": false}"#);
        registry.refresh_hooks(&seeder, 0);
        let workloads = [hook_workload()];
        let probes = ScriptedTransport::new();
        let operations = ScriptedTransport::new();
        let executor = hook_executor(
            &registry,
            &workloads,
            &probes,
            &operations,
            &FixedClock(60_000),
            None,
        );
        match executor.precondition(&hook_request()) {
            Precondition::Denied { reason } => assert!(reason.contains("hook runner"), "{reason}"),
            other => panic!("expected a denial, got {other:?}"),
        }
        assert_eq!(operations.operation_count(), 0);
    }

    #[test]
    fn an_idle_hook_release_is_applied_and_confirmed() {
        let registry = hook_registry(true);
        let hooks = FakeHooks {
            status: std::sync::Mutex::new(Some(Ok(
                r#"{"busy": false, "host_ram_bytes": 1073741824}"#.to_string(),
            ))),
            release: std::sync::Mutex::new(Some(Ok(r#"{"released": true}"#.to_string()))),
            ..FakeHooks::default()
        };
        registry.refresh_hooks(&hooks, 0);
        let workloads = [hook_workload()];
        let probes = ScriptedTransport::new();
        let operations = ScriptedTransport::new();
        let executor = hook_executor(
            &registry,
            &workloads,
            &probes,
            &operations,
            &FixedClock(60_000),
            Some(&hooks),
        );
        assert_eq!(executor.precondition(&hook_request()), Precondition::Ready);
        let change = executor
            .apply(&hook_request())
            .expect("the hook accepts it");
        let note = change.note.expect("a note");
        assert!(note.contains("through its hook"), "{note}");
        assert_eq!(operations.operation_count(), 0, "no HTTP request is sent");
        match executor.confirm(&hook_request()) {
            Confirmation::Confirmed => {}
            other => panic!("expected a confirmation, got {other:?}"),
        }
        assert_eq!(
            *hooks.calls.lock().expect("not poisoned"),
            vec![HookVerb::Status, HookVerb::Release, HookVerb::Status]
        );
    }

    #[test]
    fn a_busy_hook_is_never_released() {
        let registry = hook_registry(true);
        let hooks = FakeHooks::with_status(r#"{"busy": true}"#);
        registry.refresh_hooks(&hooks, 0);
        let workloads = [hook_workload()];
        let probes = ScriptedTransport::new();
        let operations = ScriptedTransport::new();
        let executor = hook_executor(
            &registry,
            &workloads,
            &probes,
            &operations,
            &FixedClock(60_000),
            Some(&hooks),
        );
        match executor.precondition(&hook_request()) {
            Precondition::Denied { reason } => {
                assert!(reason.contains("reports activity"), "{reason}")
            }
            other => panic!("expected a denial, got {other:?}"),
        }
    }

    #[test]
    fn a_hook_that_declines_reports_a_denial() {
        let registry = hook_registry(true);
        let hooks = FakeHooks {
            status: std::sync::Mutex::new(Some(Ok(r#"{"busy": false}"#.to_string()))),
            release: std::sync::Mutex::new(Some(Ok(
                r#"{"released": false, "note": "checkpoint in progress"}"#.to_string(),
            ))),
            ..FakeHooks::default()
        };
        registry.refresh_hooks(&hooks, 0);
        let workloads = [hook_workload()];
        let probes = ScriptedTransport::new();
        let operations = ScriptedTransport::new();
        let executor = hook_executor(
            &registry,
            &workloads,
            &probes,
            &operations,
            &FixedClock(60_000),
            Some(&hooks),
        );
        match executor.apply(&hook_request()) {
            Err(ExecutorError::Denied(reason)) => {
                assert!(reason.contains("checkpoint in progress"), "{reason}")
            }
            other => panic!("expected a denial, got {other:?}"),
        }
    }

    #[test]
    fn a_still_busy_hook_leaves_the_release_unconfirmed() {
        let registry = hook_registry(true);
        let hooks = FakeHooks {
            status: std::sync::Mutex::new(Some(Ok(r#"{"busy": true}"#.to_string()))),
            release: std::sync::Mutex::new(Some(Ok(r#"{"released": true}"#.to_string()))),
            ..FakeHooks::default()
        };
        // Seed idle so the release resolves, then report busy again.
        *hooks.status.lock().expect("not poisoned") = Some(Ok(r#"{"busy": false}"#.to_string()));
        registry.refresh_hooks(&hooks, 0);
        *hooks.status.lock().expect("not poisoned") = Some(Ok(r#"{"busy": true}"#.to_string()));
        let workloads = [hook_workload()];
        let probes = ScriptedTransport::new();
        let operations = ScriptedTransport::new();
        let executor = hook_executor(
            &registry,
            &workloads,
            &probes,
            &operations,
            &FixedClock(60_000),
            Some(&hooks),
        );
        executor
            .apply(&hook_request())
            .expect("the hook accepts it");
        match executor.confirm(&hook_request()) {
            Confirmation::NotYet { reason } => {
                assert!(reason.contains("still reports busy"), "{reason}")
            }
            other => panic!("expected NotYet, got {other:?}"),
        }
    }

    #[test]
    fn a_hook_wake_runs_through_undo() {
        let registry = hook_registry(true);
        let hooks = FakeHooks {
            status: std::sync::Mutex::new(Some(Ok(r#"{"busy": false}"#.to_string()))),
            wake: std::sync::Mutex::new(Some(Ok(r#"{"awake": true}"#.to_string()))),
            ..FakeHooks::default()
        };
        registry.refresh_hooks(&hooks, 0);
        let workloads = [hook_workload()];
        let probes = ScriptedTransport::new();
        let operations = ScriptedTransport::new();
        let executor = hook_executor(
            &registry,
            &workloads,
            &probes,
            &operations,
            &FixedClock(60_000),
            Some(&hooks),
        );
        executor.undo(&hook_request()).expect("the hook wakes");
        assert_eq!(
            *hooks.calls.lock().expect("not poisoned"),
            vec![HookVerb::Status, HookVerb::Wake]
        );
    }

    #[test]
    fn undo_without_a_wake_capability_is_unsupported() {
        let registry = hook_registry(false);
        let hooks = FakeHooks::with_status(r#"{"busy": false}"#);
        registry.refresh_hooks(&hooks, 0);
        let workloads = [hook_workload()];
        let probes = ScriptedTransport::new();
        let operations = ScriptedTransport::new();
        let executor = hook_executor(
            &registry,
            &workloads,
            &probes,
            &operations,
            &FixedClock(60_000),
            Some(&hooks),
        );
        match executor.undo(&hook_request()) {
            Err(ExecutorError::Unsupported(reason)) => {
                assert!(reason.contains("does not offer wake"), "{reason}")
            }
            other => panic!("expected unsupported, got {other:?}"),
        }
    }

    #[test]
    fn a_hook_url_outside_a_configured_endpoint_is_refused() {
        let registry = hook_registry(true);
        let probes = ScriptedTransport::new();
        let operations = ScriptedTransport::new();
        let executor = hook_executor(&registry, &[], &probes, &operations, &FixedClock(0), None);
        let refusal = executor
            .check_url("hook:other/release")
            .expect_err("not configured");
        assert!(
            refusal.reason().contains("not under a configured"),
            "{}",
            refusal.reason()
        );
        assert!(executor.check_url("hook:trainer/release").is_ok());
    }
}
