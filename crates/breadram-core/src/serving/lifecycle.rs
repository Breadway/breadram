//! The drain, sleep, and wake lifecycle of one model backend.
//!
//! A backend is put to sleep only when the evidence says nothing is in flight,
//! and its wake must be verified healthy before any new request reaches it.
//! Between those two points sits the race this module exists to close: a request
//! that arrives exactly while the idle check is running, or exactly while the
//! provider is going to sleep.
//!
//! Three properties are structural:
//!
//! 1. **Sleeping is a transition, not a side effect.** The state machine never
//!    sleeps by itself. It reports what is allowed and the caller performs the
//!    provider call, so nothing asynchronous is hidden here and the whole
//!    lifecycle can be tested against a clock the test controls.
//! 2. **Nothing is admitted without a verified wake.** Only
//!    [`WakeHealth::Healthy`] returns a backend to [`ServingState::Awake`], so a
//!    backend that is asleep, waking, or of unknown health refuses requests with
//!    a reason and a retry hint instead of forwarding them.
//! 3. **Unproven idleness is not a green light.** When a client can reach the
//!    provider without breadram seeing the request, and the provider cannot
//!    prove it has no work in flight, automatic unload is reported as
//!    unsupported unless the operator explicitly allows it.

use crate::inventory::WorkloadId;
use crate::policy::MIN_IDLE_MS;

use super::lease::{Admission, ClientId, Lease, LeaseId, Leases, RequestMeta};

/// How long a client is told to wait before retrying a refused request.
pub const DEFAULT_RETRY_AFTER_MS: u64 = 1_000;

/// Whether every request path is observed by breadram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackingCompleteness {
    /// Every request that can reach the backend is admitted through breadram,
    /// so leases prove idleness.
    Complete,
    /// A client can reach the backend directly, but the provider can prove it
    /// has no work in flight, so idleness is still provable.
    Bypassable {
        /// Whether the provider itself can prove idleness.
        provider_proves_idle: bool,
    },
    /// Nothing proves idleness. Automatic unload must not be attempted.
    Unknown,
}

impl TrackingCompleteness {
    /// The stable name used in diagnostics.
    pub fn as_str(self) -> &'static str {
        match self {
            TrackingCompleteness::Complete => "complete",
            TrackingCompleteness::Bypassable {
                provider_proves_idle: true,
            } => "bypassable_provider_proves_idle",
            TrackingCompleteness::Bypassable {
                provider_proves_idle: false,
            } => "bypassable",
            TrackingCompleteness::Unknown => "unknown",
        }
    }

    /// Why unproven idleness is a problem, when it is one.
    fn unsupported_reason(self) -> Option<&'static str> {
        match self {
            TrackingCompleteness::Complete => None,
            TrackingCompleteness::Bypassable {
                provider_proves_idle: true,
            } => None,
            TrackingCompleteness::Bypassable {
                provider_proves_idle: false,
            } => Some(
                "clients can reach this backend without breadram seeing the request, and the provider cannot prove it is idle",
            ),
            TrackingCompleteness::Unknown => Some(
                "it is not known whether every request reaches this backend through breadram",
            ),
        }
    }
}

/// The operator's policy for one backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServingPolicy {
    /// How long a backend must be idle before it may be put to sleep.
    pub sleep_after_idle_ms: u64,
    /// How long a drain may take before it is abandoned.
    pub drain_timeout_ms: u64,
    /// How long a wake may take before it is considered failed.
    pub wake_timeout_ms: u64,
    /// How many wake attempts are made before the backend is marked failed.
    pub max_wake_attempts: u32,
    /// How long a refused client is told to wait before retrying.
    pub retry_after_ms: u64,
    /// Whether an arrival during a drain cancels the drain instead of being
    /// refused.
    ///
    /// Off by default. Refusing with a retry hint keeps the sleep from being
    /// abandoned by a single stray request, and the client is told when to come
    /// back rather than being left to guess.
    pub abort_drain_on_arrival: bool,
    /// Whether a queued request blocks sleep.
    ///
    /// On by default: a queued request will run, so sleeping now would mean
    /// waking again immediately.
    pub queued_blocks_sleep: bool,
    /// Whether automatic unload may proceed even when requests can bypass
    /// tracking.
    ///
    /// Off by default, which is what makes automatic unload unsupported for a
    /// backend whose idleness cannot be proven.
    pub allow_untracked_unload: bool,
}

impl Default for ServingPolicy {
    fn default() -> Self {
        Self {
            sleep_after_idle_ms: 60_000,
            drain_timeout_ms: 30_000,
            wake_timeout_ms: 60_000,
            max_wake_attempts: 3,
            retry_after_ms: DEFAULT_RETRY_AFTER_MS,
            abort_drain_on_arrival: false,
            queued_blocks_sleep: true,
            allow_untracked_unload: false,
        }
    }
}

impl ServingPolicy {
    /// The policy with values that cannot make the lifecycle unsafe.
    ///
    /// A zero idle minimum would let a backend sleep between two requests of the
    /// same conversation, and a zero attempt budget would make waking fail
    /// before it was tried, so both are clamped rather than accepted.
    fn bounded(mut self) -> Self {
        self.sleep_after_idle_ms = self.sleep_after_idle_ms.max(MIN_IDLE_MS);
        self.max_wake_attempts = self.max_wake_attempts.max(1);
        self.drain_timeout_ms = self.drain_timeout_ms.max(1);
        self.wake_timeout_ms = self.wake_timeout_ms.max(1);
        if self.retry_after_ms == 0 {
            self.retry_after_ms = DEFAULT_RETRY_AFTER_MS;
        }
        self
    }
}

/// The serving state of one backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServingState {
    /// Serving requests.
    Awake,
    /// Asked to stop, waiting for in-flight work to end before sleeping.
    Draining {
        /// When the drain began, in monotonic milliseconds.
        since_ms: u64,
        /// When the drain is abandoned, in monotonic milliseconds.
        deadline_ms: u64,
    },
    /// Asleep, with the provider having confirmed it.
    Sleeping {
        /// When it went to sleep, in monotonic milliseconds.
        since_ms: u64,
        /// How many wake attempts have already failed in this sleep cycle.
        wake_attempts: u32,
    },
    /// A wake is in progress. Nothing is admitted until it is verified healthy.
    Waking {
        /// Which attempt this is, counting from one.
        attempt: u32,
        /// When it began, in monotonic milliseconds.
        started_ms: u64,
    },
    /// Not known to be serving and not known to be asleep, which is what a
    /// restart against an unresponsive provider looks like. A health check is
    /// required before serving.
    Unverified {
        /// When the state became unknown, in monotonic milliseconds.
        since_ms: u64,
        /// How many verification attempts have failed.
        attempts: u32,
    },
    /// Unload is not supported for this backend.
    Unsupported {
        /// Why, always naming what could not be proven.
        reason: String,
    },
    /// Waking failed repeatedly. An explicit reset or a fresh reconcile is
    /// required, and clients get an actionable error until then.
    Failed {
        /// Why it failed, in the words of the last failure.
        reason: String,
        /// When it failed, in monotonic milliseconds.
        since_ms: u64,
    },
}

impl ServingState {
    /// The stable name used on the wire and in diagnostics.
    pub fn name(&self) -> &'static str {
        match self {
            ServingState::Awake => "awake",
            ServingState::Draining { .. } => "draining",
            ServingState::Sleeping { .. } => "sleeping",
            ServingState::Waking { .. } => "waking",
            ServingState::Unverified { .. } => "unverified",
            ServingState::Unsupported { .. } => "unsupported",
            ServingState::Failed { .. } => "failed",
        }
    }

    /// Whether requests are being served.
    pub fn is_serving(&self) -> bool {
        matches!(self, ServingState::Awake)
    }

    /// Whether the backend is asleep.
    pub fn is_asleep(&self) -> bool {
        matches!(self, ServingState::Sleeping { .. })
    }
}

/// Why a request was not admitted.
///
/// Every variant is actionable: each carries either a retry hint or the reason
/// the caller can hand to a client.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdmitError {
    /// The backend is draining to sleep and the request was not dispatched.
    #[error("the backend is draining to sleep, retry in {retry_after_ms} ms")]
    Draining {
        /// How long to wait before retrying.
        retry_after_ms: u64,
    },
    /// The backend is asleep, so it must be woken first.
    #[error("the backend is asleep, wake it and retry in {retry_after_ms} ms")]
    Asleep {
        /// How long to wait before retrying.
        retry_after_ms: u64,
    },
    /// A wake is already in progress.
    #[error("the backend is waking (attempt {attempt}), retry in {retry_after_ms} ms")]
    Waking {
        /// Which attempt is in progress.
        attempt: u32,
        /// How long to wait before retrying.
        retry_after_ms: u64,
    },
    /// Health has not been verified since the state became unknown.
    #[error("the backend health is unverified, retry in {retry_after_ms} ms")]
    Unverified {
        /// How many verification attempts have failed.
        attempts: u32,
        /// How long to wait before retrying.
        retry_after_ms: u64,
    },
    /// The backend cannot serve and waking is not possible.
    #[error("the backend cannot serve: {reason}")]
    Unavailable {
        /// Why, in words a client can act on.
        reason: String,
    },
}

/// The result of asking what should happen to a backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SleepDecision {
    /// The backend may be drained now.
    Allowed {
        /// How long it has been idle, in milliseconds.
        idle_for_ms: u64,
    },
    /// Not idle for long enough yet.
    Waiting {
        /// When it becomes eligible, in monotonic milliseconds.
        until_ms: u64,
    },
    /// Work is in flight.
    Busy {
        /// How many distinct requests are in flight.
        in_flight: usize,
        /// How many are dispatched and running.
        dispatched: usize,
        /// How many are queued.
        queued: usize,
        /// How many are streams.
        streaming: usize,
    },
    /// Untracked requests may reach the backend and the provider cannot prove
    /// idleness, so automatic unload is unsupported.
    Unsupported {
        /// Why, in words an operator can act on.
        reason: String,
    },
    /// The backend is not serving, so there is nothing to put to sleep.
    NotServing {
        /// The current state name.
        state: &'static str,
    },
}

/// What a drain should do next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainOutcome {
    /// No work is in flight, so the provider sleep may be performed. Call
    /// [`Backend::sleep_confirmed`] once it is.
    Ready {
        /// How long the backend has been idle, in milliseconds.
        idle_for_ms: u64,
    },
    /// Work is still in flight, so the drain continues.
    Waiting {
        /// How many requests are still in flight.
        in_flight: usize,
        /// When the drain is abandoned, in monotonic milliseconds.
        deadline_ms: u64,
    },
    /// The drain ran out of time and was abandoned. The backend is awake again.
    TimedOut {
        /// How many requests were still in flight at the deadline.
        in_flight: usize,
    },
    /// Nothing is draining.
    NotDraining,
}

/// How a wake ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WakeHealth {
    /// The backend answered and is serving.
    Healthy,
    /// The backend answered but is not healthy, or could not be reached.
    Unhealthy {
        /// Why, in words the operator can act on.
        reason: String,
    },
}

/// Why a transition was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransitionError {
    /// The backend is not in a state that allows this transition.
    #[error("the backend is {state}, which does not allow {transition}")]
    WrongState {
        /// The current state name.
        state: &'static str,
        /// The transition that was refused.
        transition: &'static str,
    },
    /// Requests are still in flight.
    #[error("{in_flight} request(s) are still in flight")]
    Busy {
        /// How many requests are in flight.
        in_flight: usize,
    },
    /// Waking has already failed too many times.
    #[error("waking failed {attempts} times: {reason}")]
    AttemptsExhausted {
        /// How many attempts were made.
        attempts: u32,
        /// The last failure.
        reason: String,
    },
}

/// A wake the caller must perform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WakeRequest {
    /// The workload to wake.
    pub workload: WorkloadId,
    /// Which attempt this is, counting from one.
    pub attempt: u32,
    /// How long the wake may take before it is considered failed.
    pub timeout_ms: u64,
}

/// What a provider reports about a backend, as observed after a restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderState {
    /// The provider says the backend is asleep.
    Sleeping,
    /// The provider says the backend is serving.
    Serving,
    /// The provider did not answer, so nothing is known.
    Unknown,
}

/// One model backend, with its leases and its serving state.
#[derive(Debug)]
pub struct Backend {
    workload: WorkloadId,
    tracking: TrackingCompleteness,
    policy: ServingPolicy,
    state: ServingState,
    leases: Leases,
}

impl Backend {
    /// A backend that starts awake, which is the safe assumption: an awake
    /// backend only ever wastes memory, a sleeping one that is used wastes a
    /// request.
    pub fn new(
        workload: WorkloadId,
        tracking: TrackingCompleteness,
        policy: ServingPolicy,
        now_ms: u64,
    ) -> Self {
        let policy = policy.bounded();
        let state = match tracking.unsupported_reason() {
            Some(reason) if !policy.allow_untracked_unload => ServingState::Unsupported {
                reason: reason.to_string(),
            },
            _ => ServingState::Awake,
        };
        Self {
            workload,
            tracking,
            policy,
            state,
            leases: Leases::new(now_ms),
        }
    }

    /// The workload this backend serves.
    pub fn workload(&self) -> &WorkloadId {
        &self.workload
    }

    /// The current state.
    pub fn state(&self) -> &ServingState {
        &self.state
    }

    /// The policy in effect, after bounding.
    pub fn policy(&self) -> &ServingPolicy {
        &self.policy
    }

    /// How completely requests are tracked.
    pub fn tracking(&self) -> TrackingCompleteness {
        self.tracking
    }

    /// The leases.
    pub fn leases(&self) -> &Leases {
        &self.leases
    }

    /// The activity evidence for this backend.
    pub fn evidence(&self) -> crate::policy::activity::ActivityEvidence {
        self.leases.evidence()
    }

    /// Why automatic unload is unsupported, when it is.
    ///
    /// Reportable on its own, so a diagnostic bundle can explain the refusal
    /// before any action is attempted.
    pub fn unsupported_reason(&self) -> Option<&'static str> {
        match &self.state {
            ServingState::Unsupported { .. } => self.tracking.unsupported_reason(),
            _ if self.tracking.unsupported_reason().is_some() => {
                // Allowed by explicit operator policy, so it is not a refusal,
                // but the caller can still see why it would normally be one.
                None
            }
            _ => None,
        }
    }

    /// Admit a request, or refuse it with a reason and a retry hint.
    pub fn admit(&mut self, meta: RequestMeta, now_ms: u64) -> Result<Admission, AdmitError> {
        match self.state {
            ServingState::Awake => Ok(self.leases.admit(meta, now_ms)),
            ServingState::Draining { .. } => {
                if self.policy.abort_drain_on_arrival {
                    // The operator chose to favour the client over the sleep.
                    self.state = ServingState::Awake;
                    Ok(self.leases.admit(meta, now_ms))
                } else {
                    Err(AdmitError::Draining {
                        retry_after_ms: self.policy.retry_after_ms,
                    })
                }
            }
            ServingState::Sleeping { .. } => Err(AdmitError::Asleep {
                retry_after_ms: self.policy.retry_after_ms,
            }),
            ServingState::Waking { attempt, .. } => Err(AdmitError::Waking {
                attempt,
                retry_after_ms: self.policy.retry_after_ms,
            }),
            ServingState::Unverified { attempts, .. } => Err(AdmitError::Unverified {
                attempts,
                retry_after_ms: self.policy.retry_after_ms,
            }),
            ServingState::Unsupported { ref reason } => Err(AdmitError::Unavailable {
                reason: reason.clone(),
            }),
            ServingState::Failed { ref reason, .. } => Err(AdmitError::Unavailable {
                reason: reason.clone(),
            }),
        }
    }

    /// Release a lease because its request finished.
    pub fn finish(&mut self, id: LeaseId, now_ms: u64) -> Option<Lease> {
        self.leases.finish(id, now_ms)
    }

    /// Promote a queued request to a running one.
    pub fn dispatch(&mut self, id: LeaseId, now_ms: u64) -> bool {
        self.leases.dispatch(id, now_ms)
    }

    /// Release a lease because its request was cancelled.
    pub fn cancel(&mut self, id: LeaseId, now_ms: u64) -> Option<Lease> {
        self.leases.cancel(id, now_ms)
    }

    /// Release every lease of one client, as a disconnect does.
    pub fn cancel_client(&mut self, client: &ClientId, now_ms: u64) -> Vec<Lease> {
        self.leases.cancel_client(client, now_ms)
    }

    /// What should happen to this backend, without changing anything.
    pub fn sleep_decision(&self, now_ms: u64) -> SleepDecision {
        if !matches!(self.state, ServingState::Awake) {
            return SleepDecision::NotServing {
                state: self.state.name(),
            };
        }
        if let Some(reason) = self.tracking.unsupported_reason() {
            if !self.policy.allow_untracked_unload {
                return SleepDecision::Unsupported {
                    reason: reason.to_string(),
                };
            }
        }
        let blocking = if self.policy.queued_blocks_sleep {
            self.leases.in_flight()
        } else {
            self.leases.dispatched()
        };
        if blocking > 0 {
            return SleepDecision::Busy {
                in_flight: self.leases.in_flight(),
                dispatched: self.leases.dispatched(),
                queued: self.leases.queued(),
                streaming: self.leases.streaming(),
            };
        }
        let idle_since = self.leases.last_activity_ms();
        let idle_for_ms = now_ms.saturating_sub(idle_since);
        if idle_for_ms < self.policy.sleep_after_idle_ms {
            return SleepDecision::Waiting {
                until_ms: idle_since.saturating_add(self.policy.sleep_after_idle_ms),
            };
        }
        SleepDecision::Allowed { idle_for_ms }
    }

    /// Begin a drain when the decision allows it.
    ///
    /// Returns the decision either way, so the caller reports the reason it did
    /// not happen rather than silently doing nothing.
    pub fn begin_drain(&mut self, now_ms: u64) -> SleepDecision {
        let decision = self.sleep_decision(now_ms);
        if let SleepDecision::Allowed { .. } = decision {
            self.state = ServingState::Draining {
                since_ms: now_ms,
                deadline_ms: now_ms.saturating_add(self.policy.drain_timeout_ms),
            };
        }
        decision
    }

    /// What a drain in progress should do next.
    pub fn drain_outcome(&mut self, now_ms: u64) -> DrainOutcome {
        let ServingState::Draining { deadline_ms, .. } = self.state else {
            return DrainOutcome::NotDraining;
        };
        if self.leases.in_flight() == 0 {
            return DrainOutcome::Ready {
                idle_for_ms: now_ms.saturating_sub(self.leases.last_activity_ms()),
            };
        }
        if now_ms >= deadline_ms {
            // A drain that cannot finish must not hold the backend in limbo: it
            // is awake and serving until the next attempt, and the caller is
            // told how much work was still in flight.
            let in_flight = self.leases.in_flight();
            self.state = ServingState::Awake;
            return DrainOutcome::TimedOut { in_flight };
        }
        DrainOutcome::Waiting {
            in_flight: self.leases.in_flight(),
            deadline_ms,
        }
    }

    /// Record that the provider confirmed the sleep.
    pub fn sleep_confirmed(&mut self, now_ms: u64) -> Result<ServingState, TransitionError> {
        if let ServingState::Draining { .. } = self.state {
            if self.leases.in_flight() > 0 {
                return Err(TransitionError::Busy {
                    in_flight: self.leases.in_flight(),
                });
            }
            self.state = ServingState::Sleeping {
                since_ms: now_ms,
                wake_attempts: 0,
            };
            return Ok(self.state.clone());
        }
        Err(TransitionError::WrongState {
            state: self.state.name(),
            transition: "confirm a sleep",
        })
    }

    /// Abandon a drain, leaving the backend awake and serving.
    pub fn abort_drain(&mut self) -> ServingState {
        if matches!(self.state, ServingState::Draining { .. }) {
            self.state = ServingState::Awake;
        }
        self.state.clone()
    }

    /// Start a wake. The caller performs the provider call and reports back.
    ///
    /// A wake is allowed from a confirmed sleep and from an unverified state,
    /// because both mean the backend is not serving.
    pub fn wake_request(&mut self, now_ms: u64) -> Result<WakeRequest, TransitionError> {
        let attempts = match &self.state {
            ServingState::Sleeping { wake_attempts, .. } => *wake_attempts,
            ServingState::Unverified { attempts, .. } => *attempts,
            other => {
                return Err(TransitionError::WrongState {
                    state: other.name(),
                    transition: "wake",
                })
            }
        };
        let attempt = attempts.saturating_add(1);
        if attempt > self.policy.max_wake_attempts {
            let reason = format!(
                "waking stopped after {} attempts",
                self.policy.max_wake_attempts
            );
            self.state = ServingState::Failed {
                reason: reason.clone(),
                since_ms: now_ms,
            };
            return Err(TransitionError::AttemptsExhausted {
                attempts: self.policy.max_wake_attempts,
                reason,
            });
        }
        self.state = ServingState::Waking {
            attempt,
            started_ms: now_ms,
        };
        Ok(WakeRequest {
            workload: self.workload.clone(),
            attempt,
            timeout_ms: self.policy.wake_timeout_ms,
        })
    }

    /// Report the result of a wake.
    ///
    /// Only a healthy backend returns to serving. An unhealthy one goes back to
    /// being asleep or unverified with the attempt counted, so a bounded number
    /// of tries is made and the failure is then visible rather than retried
    /// forever.
    pub fn wake_completed(&mut self, health: WakeHealth, now_ms: u64) -> ServingState {
        let ServingState::Waking { attempt, .. } = self.state else {
            // A result that arrives out of order is ignored: the state already
            // says what is happening.
            return self.state.clone();
        };
        match health {
            WakeHealth::Healthy => {
                self.state = ServingState::Awake;
            }
            WakeHealth::Unhealthy { reason } => {
                self.state = self.after_failed_attempt(attempt, reason, now_ms);
            }
        }
        self.state.clone()
    }

    /// Report that a wake timed out before it answered.
    pub fn wake_timed_out(&mut self, now_ms: u64) -> ServingState {
        let ServingState::Waking { attempt, .. } = self.state else {
            return self.state.clone();
        };
        let reason = format!(
            "the wake did not complete within {} ms",
            self.policy.wake_timeout_ms
        );
        self.state = self.after_failed_attempt(attempt, reason, now_ms);
        self.state.clone()
    }

    /// Record a failed verification of an unverified backend.
    pub fn verify_completed(
        &mut self,
        health: WakeHealth,
        now_ms: u64,
    ) -> Result<ServingState, TransitionError> {
        match self.state.clone() {
            ServingState::Unverified { attempts, .. } => match health {
                WakeHealth::Healthy => {
                    self.state = ServingState::Awake;
                    Ok(self.state.clone())
                }
                WakeHealth::Unhealthy { reason } => {
                    let next = attempts.saturating_add(1);
                    if next >= self.policy.max_wake_attempts {
                        self.state = ServingState::Failed {
                            reason,
                            since_ms: now_ms,
                        };
                    } else {
                        self.state = ServingState::Unverified {
                            since_ms: now_ms,
                            attempts: next,
                        };
                    }
                    Ok(self.state.clone())
                }
            },
            ServingState::Waking { .. } => Ok(self.wake_completed(health, now_ms)),
            other => Err(TransitionError::WrongState {
                state: other.name(),
                transition: "verify health",
            }),
        }
    }

    fn after_failed_attempt(&self, attempt: u32, reason: String, now_ms: u64) -> ServingState {
        if attempt >= self.policy.max_wake_attempts {
            return ServingState::Failed {
                reason,
                since_ms: now_ms,
            };
        }
        // Back to a state a later request can wake from, with the attempt
        // counted, so the retry budget is spent rather than reset.
        ServingState::Unverified {
            since_ms: now_ms,
            attempts: attempt,
        }
    }

    /// Reconcile the state after a restart.
    ///
    /// A restart loses the in-flight requests: their clients see a closed
    /// connection and retry with the same idempotency key. Nothing is assumed
    /// about the provider beyond what it reports, and an unknown answer means a
    /// health check before serving rather than a guess.
    pub fn reconcile(&mut self, observed: ProviderState, now_ms: u64) -> ServingState {
        self.leases.reset(now_ms);
        self.state = match observed {
            ProviderState::Sleeping => ServingState::Sleeping {
                since_ms: now_ms,
                wake_attempts: 0,
            },
            ProviderState::Serving => ServingState::Awake,
            ProviderState::Unknown => ServingState::Unverified {
                since_ms: now_ms,
                attempts: 0,
            },
        };
        self.state.clone()
    }

    /// Clear a failure, leaving the backend unverified so a health check or a
    /// wake is required before it serves again.
    pub fn reset(&mut self, now_ms: u64) -> ServingState {
        self.state = ServingState::Unverified {
            since_ms: now_ms,
            attempts: 0,
        };
        self.state.clone()
    }
}
