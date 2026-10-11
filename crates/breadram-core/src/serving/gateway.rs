//! The request-path gateway core: admission, demand-driven wake, and routing.
//!
//! This is the part of the gateway a socket server drives. It performs no I/O:
//! given a request's metadata and the set of backends, it decides whether to
//! forward the request (holding a lease for its whole duration), whether to
//! trigger a wake and tell the client to retry, or whether to refuse. The caller
//! owns the socket, the provider commands, and the clock.
//!
//! Nothing here stores a body. A route outcome carries a client label, a request
//! kind, whether the response is streamed, and an idempotency key, so a gateway
//! built on it cannot log a prompt or a completion even by accident.

use crate::inventory::WorkloadId;

use super::lease::{Admission, RequestMeta};
use super::lifecycle::{AdmitError, Backend, ServingState, WakeRequest};

/// The routing decision for one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteOutcome {
    /// Forward to the backend, holding this admission for the request's life.
    ///
    /// A `Joined` or `Replayed` admission is not a second dispatch: the caller
    /// serves the client but does not count the request again.
    Forward {
        /// The backend that will serve it.
        backend: WorkloadId,
        /// The admission, which says whether this started or joined or replayed.
        admission: Admission,
    },
    /// The backend was asleep, so a wake was begun on the request path. The
    /// request was **not** dispatched, and the client should retry after the
    /// hint. The caller performs the provider wake from `wake` and reports the
    /// result back to the backend.
    WakeThenRetry {
        /// The backend that is waking.
        backend: WorkloadId,
        /// How long the client should wait before retrying.
        retry_after_ms: u64,
        /// The wake the caller must perform and report.
        wake: WakeRequest,
    },
    /// The backend is not serving and no wake was begun (it is already waking or
    /// draining, or its health is unverified). Nothing was dispatched.
    Retry {
        /// The backend.
        backend: WorkloadId,
        /// Why, in words a client can act on.
        reason: String,
        /// How long the client should wait before retrying.
        retry_after_ms: u64,
    },
    /// The backend cannot serve and retrying will not help until it is reset.
    Refused {
        /// The backend.
        backend: WorkloadId,
        /// Why, in words a client can act on.
        reason: String,
    },
}

/// The routing core over a set of backends.
#[derive(Debug)]
pub struct Gateway {
    backends: Vec<Backend>,
}

impl Gateway {
    /// A gateway over these backends. One backend per workload.
    pub fn new(backends: Vec<Backend>) -> Self {
        Self { backends }
    }

    /// The backend serving a workload, when there is one.
    pub fn backend(&self, workload: &WorkloadId) -> Option<&Backend> {
        self.backends
            .iter()
            .find(|backend| backend.workload() == workload)
    }

    /// The backend serving a workload, for the caller to record results on.
    pub fn backend_mut(&mut self, workload: &WorkloadId) -> Option<&mut Backend> {
        self.backends
            .iter_mut()
            .find(|backend| backend.workload() == workload)
    }

    /// Route one request to the backend that serves its workload.
    ///
    /// A request that arrives at a sleeping backend triggers the wake here, on
    /// the request path, rather than waiting for the daemon's own timer. A
    /// request that arrives at a draining, waking, or unverified backend is
    /// refused with a retry hint and never dispatched to a backend that is not
    /// proven healthy.
    pub fn route(&mut self, workload: &WorkloadId, meta: RequestMeta, now_ms: u64) -> RouteOutcome {
        let Some(backend) = self.backend_mut(workload) else {
            return RouteOutcome::Refused {
                backend: workload.clone(),
                reason: "no backend serves this workload".to_string(),
            };
        };
        // Demand-driven wake: an arrival at a confirmed sleep begins the wake.
        if matches!(backend.state(), ServingState::Sleeping { .. }) {
            let retry_after_ms = backend.policy().retry_after_ms;
            return match backend.wake_request(now_ms) {
                Ok(wake) => RouteOutcome::WakeThenRetry {
                    backend: workload.clone(),
                    retry_after_ms,
                    wake,
                },
                // Attempts exhausted, or a state that cannot wake: actionable.
                Err(err) => RouteOutcome::Refused {
                    backend: workload.clone(),
                    reason: err.to_string(),
                },
            };
        }
        match backend.admit(meta, now_ms) {
            Ok(admission) => RouteOutcome::Forward {
                backend: workload.clone(),
                admission,
            },
            Err(AdmitError::Unavailable { reason }) => RouteOutcome::Refused {
                backend: workload.clone(),
                reason,
            },
            Err(err) => RouteOutcome::Retry {
                retry_after_ms: retry_hint(&err),
                reason: err.to_string(),
                backend: workload.clone(),
            },
        }
    }
}

/// The retry hint an admission error carries.
fn retry_hint(err: &AdmitError) -> u64 {
    match err {
        AdmitError::Draining { retry_after_ms }
        | AdmitError::Asleep { retry_after_ms }
        | AdmitError::Waking { retry_after_ms, .. }
        | AdmitError::Unverified { retry_after_ms, .. } => *retry_after_ms,
        AdmitError::Unavailable { .. } => 0,
    }
}

/// The header the gateway reads for a client label.
pub const CLIENT_HEADER: &str = "x-breadram-client";
/// The header the gateway reads for an idempotency key.
pub const IDEMPOTENCY_HEADER: &str = "idempotency-key";
/// The header that marks a streamed response.
pub const ACCEPT_HEADER: &str = "accept";

/// Classify an inbound request into the metadata the core needs.
///
/// Only headers are read. The request body is never inspected, so a prompt or a
/// completion cannot be captured here, and the classified metadata has nowhere
/// to put one.
pub fn classify(
    client_header: Option<&str>,
    idempotency_header: Option<&str>,
    accept_header: Option<&str>,
) -> RequestMeta {
    let client = client_header
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("anonymous");
    let mut meta = RequestMeta::new(client);
    if let Some(key) = idempotency_header
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        meta = meta.with_key(key);
    }
    if accept_header.is_some_and(|value| value.to_ascii_lowercase().contains("text/event-stream")) {
        meta = meta.streaming();
    }
    meta
}

/// What the socket server should do with a routing outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// Proxy the request to the backend, streaming the response when the client
    /// asked for a stream.
    Proxy {
        /// Whether the response is a stream.
        streaming: bool,
    },
    /// Refuse with a retry hint, which the server reports as 503 and a
    /// `Retry-After` in seconds.
    Retry {
        /// How long the client should wait, in milliseconds.
        retry_after_ms: u64,
    },
    /// Refuse without a retry hint.
    Refuse,
}

/// The disposition for a routing outcome.
pub fn disposition(meta: &RequestMeta, outcome: &RouteOutcome) -> Disposition {
    match outcome {
        RouteOutcome::Forward { .. } => Disposition::Proxy {
            streaming: meta.streaming,
        },
        RouteOutcome::WakeThenRetry { retry_after_ms, .. }
        | RouteOutcome::Retry { retry_after_ms, .. } => Disposition::Retry {
            retry_after_ms: *retry_after_ms,
        },
        RouteOutcome::Refused { .. } => Disposition::Refuse,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serving::lease::ClientId;
    use crate::serving::lifecycle::{
        DrainOutcome, ServingPolicy, SleepDecision, TrackingCompleteness,
    };

    const SCOPE: &str = "unit:llama.service";

    fn workload() -> WorkloadId {
        WorkloadId {
            scope: SCOPE.to_string(),
            generation: 1,
        }
    }

    fn backend(tracking: TrackingCompleteness) -> Backend {
        Backend::new(workload(), tracking, ServingPolicy::default(), 0)
    }

    /// Drive a backend to a confirmed sleep, so the wake path can be exercised.
    fn asleep(tracking: TrackingCompleteness) -> Backend {
        let mut backend = backend(tracking);
        // A large `now` makes the idle minimum trivially satisfied, so the drain
        // is allowed with no in-flight work.
        assert!(matches!(
            backend.begin_drain(1_000_000),
            SleepDecision::Allowed { .. }
        ));
        assert!(matches!(
            backend.drain_outcome(1_000_000),
            DrainOutcome::Ready { .. }
        ));
        backend.sleep_confirmed(1_000_000).unwrap();
        backend
    }

    #[test]
    fn an_awake_backend_forwards_and_an_idempotent_retry_joins_without_double_counting() {
        let mut gateway = Gateway::new(vec![backend(TrackingCompleteness::Complete)]);
        let meta = RequestMeta::new("c1").with_key("k1");
        let first = gateway.route(&workload(), meta.clone(), 1);
        assert!(matches!(
            first,
            RouteOutcome::Forward {
                admission: Admission::Started(_),
                ..
            }
        ));
        let again = gateway.route(&workload(), meta, 2);
        assert!(
            matches!(
                again,
                RouteOutcome::Forward {
                    admission: Admission::Joined(_),
                    ..
                }
            ),
            "the same key while running joins rather than counting twice"
        );
        let backend = gateway.backend(&workload()).unwrap();
        assert_eq!(backend.leases().in_flight(), 1);
    }

    #[test]
    fn a_stream_holds_its_lease_until_it_finishes_and_a_disconnect_releases_it() {
        let mut gateway = Gateway::new(vec![backend(TrackingCompleteness::Complete)]);
        let meta = RequestMeta::new("c1").streaming();
        let RouteOutcome::Forward { admission, .. } = gateway.route(&workload(), meta, 1) else {
            panic!("a stream is forwarded");
        };
        let lease = admission.lease();
        let backend = gateway.backend_mut(&workload()).unwrap();
        backend.dispatch(lease, 2);
        assert_eq!(backend.leases().streaming(), 1);

        // A disconnect releases the lease, so the backend can become idle again.
        let released = backend.cancel_client(&ClientId("c1".to_string()), 3);
        assert_eq!(released.len(), 1);
        assert!(backend.leases().is_empty());
    }

    #[test]
    fn an_arrival_at_a_sleeping_backend_wakes_it_and_refuses_with_a_retry_hint() {
        let mut gateway = Gateway::new(vec![asleep(TrackingCompleteness::Complete)]);
        let outcome = gateway.route(&workload(), RequestMeta::new("c1"), 1_000_001);
        match outcome {
            RouteOutcome::WakeThenRetry {
                retry_after_ms,
                wake,
                ..
            } => {
                assert!(retry_after_ms > 0, "the client is told when to come back");
                assert_eq!(wake.workload, workload());
                assert_eq!(wake.attempt, 1);
            }
            other => panic!("expected a wake on the request path, got {other:?}"),
        }
        // The wake is now in progress, so a second arrival is refused with a
        // hint and no second wake is begun.
        let second = gateway.route(&workload(), RequestMeta::new("c2"), 1_000_002);
        assert!(matches!(second, RouteOutcome::Retry { retry_after_ms, .. } if retry_after_ms > 0));
    }

    #[test]
    fn a_backend_whose_idleness_cannot_be_proven_refuses_to_sleep_or_serve() {
        // Unknown tracking means the backend is Unsupported, so routing refuses
        // rather than serving through a path that could be slept from under it.
        let mut gateway = Gateway::new(vec![backend(TrackingCompleteness::Unknown)]);
        let outcome = gateway.route(&workload(), RequestMeta::new("c1"), 1);
        assert!(
            matches!(outcome, RouteOutcome::Refused { .. }),
            "an untracked backend is refused unless the operator allowed it: {outcome:?}"
        );
    }

    #[test]
    fn a_replayed_request_after_finish_is_recognized_and_not_dispatched_again() {
        let mut gateway = Gateway::new(vec![backend(TrackingCompleteness::Complete)]);
        let meta = RequestMeta::new("c1").with_key("k1");
        let RouteOutcome::Forward { admission, .. } = gateway.route(&workload(), meta.clone(), 1)
        else {
            panic!("forwarded");
        };
        let lease = admission.lease();
        gateway.backend_mut(&workload()).unwrap().finish(lease, 2);
        let replay = gateway.route(&workload(), meta, 3);
        assert!(
            matches!(
                replay,
                RouteOutcome::Forward {
                    admission: Admission::Replayed(_),
                    ..
                }
            ),
            "a replayed key is recognized, not counted as a new request"
        );
    }

    #[test]
    fn an_inbound_request_is_classified_from_headers_only() {
        // A client label, an idempotency key, and a stream marker, all from
        // headers. No body is read, so a prompt cannot be captured here.
        let meta = classify(Some("c1"), Some("k1"), Some("text/event-stream"));
        assert_eq!(meta.client, ClientId("c1".to_string()));
        assert_eq!(meta.key.as_deref(), Some("k1"));
        assert!(meta.streaming);

        // Missing or blank headers leave the safe defaults.
        let meta = classify(None, Some("   "), Some("application/json"));
        assert_eq!(meta.client, ClientId("anonymous".to_string()));
        assert_eq!(meta.key, None);
        assert!(!meta.streaming);
    }

    #[test]
    fn an_outcome_maps_to_a_disposition() {
        let streaming = classify(None, None, Some("text/event-stream"));
        let mut gateway = Gateway::new(vec![backend(TrackingCompleteness::Complete)]);
        let outcome = gateway.route(&workload(), streaming.clone(), 1);
        assert_eq!(
            disposition(&streaming, &outcome),
            Disposition::Proxy { streaming: true }
        );

        let plain = classify(None, None, None);
        let mut sleeping = Gateway::new(vec![asleep(TrackingCompleteness::Complete)]);
        let outcome = sleeping.route(&workload(), plain.clone(), 1_000_001);
        assert!(matches!(
            disposition(&plain, &outcome),
            Disposition::Retry { retry_after_ms } if retry_after_ms > 0
        ));

        let mut untracked = Gateway::new(vec![backend(TrackingCompleteness::Unknown)]);
        let outcome = untracked.route(&workload(), plain.clone(), 1);
        assert_eq!(disposition(&plain, &outcome), Disposition::Refuse);
    }
}
