//! Demand-driven serving coordination.
//!
//! Putting a model backend to sleep frees the most memory of anything breadram
//! does, and it is also the only action that can break a client: sleep a backend
//! that is still serving, or dispatch to one that has not finished waking, and
//! the request fails. This module is the part that makes the action safe.
//!
//! It has two halves, and neither performs I/O:
//!
//! - [`lease`] tracks in-flight requests. Idleness is proven from the leases, a
//!   retry does not count twice, and a cancelled stream releases its lease.
//! - [`lifecycle`] is the drain, sleep, wake, and verification state machine for
//!   one backend. It decides what is allowed and reports why, and the caller
//!   performs the provider calls and reports the results back.
//!
//! Because the module owns no clock and no socket, the whole lifecycle is
//! exercised in tests against a clock the test advances by hand, including the
//! races that are hardest to reproduce against a live provider: a request
//! arriving during a drain, a stream outliving an idle check, and a wake that
//! fails repeatedly.
//!
//! The activity evidence this produces is what the policy's planner consumes, so
//! a workload with a live lease is never released even when its process is quiet
//! and its cgroup looks idle.
//!
//! Nothing here stores a request or a response. A lease holds a client label, a
//! request kind, whether the response is streamed, an idempotency key, and
//! monotonic timestamps, so a gateway built on top cannot log a prompt or a
//! completion through these types even by accident.

pub mod gateway;
pub mod lease;
pub mod lifecycle;

pub use gateway::{Gateway, RouteOutcome};
pub use lease::{
    Admission, ClientId, Lease, LeaseId, Leases, RequestKind, RequestMeta, WorkloadLeases,
    RETRY_MEMORY,
};
pub use lifecycle::{
    AdmitError, Backend, DrainOutcome, ProviderState, ServingPolicy, ServingState, SleepDecision,
    TrackingCompleteness, TransitionError, WakeHealth, WakeRequest, DEFAULT_RETRY_AFTER_MS,
};
