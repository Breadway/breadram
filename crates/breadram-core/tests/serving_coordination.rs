//! Serving coordination under the races a live provider makes hard to reproduce.
//!
//! Every test drives the lifecycle against a clock the test advances by hand, so
//! a long stream, a drain that times out, and a wake that fails three times take
//! no wall clock time and are exactly reproducible.

use breadram_core::inventory::WorkloadId;
use breadram_core::policy::activity::{ActivityEvidence, ActivitySource};
use breadram_core::policy::MIN_IDLE_MS;
use breadram_core::serving::{
    Admission, AdmitError, Backend, ClientId, DrainOutcome, ProviderState, RequestKind,
    RequestMeta, ServingPolicy, ServingState, SleepDecision, TrackingCompleteness, TransitionError,
    WakeHealth, WorkloadLeases,
};

fn workload() -> WorkloadId {
    WorkloadId {
        scope: "unit:ollama.service".to_string(),
        generation: 7,
    }
}

fn policy() -> ServingPolicy {
    ServingPolicy {
        // The policy minimum, because anything shorter is clamped to it.
        sleep_after_idle_ms: MIN_IDLE_MS,
        drain_timeout_ms: 2_000,
        wake_timeout_ms: 3_000,
        max_wake_attempts: 3,
        ..ServingPolicy::default()
    }
}

fn backend_with(policy: ServingPolicy) -> Backend {
    Backend::new(workload(), TrackingCompleteness::Complete, policy, 0)
}

fn unhealthy(reason: &str) -> WakeHealth {
    WakeHealth::Unhealthy {
        reason: reason.to_string(),
    }
}

/// Walk a backend from awake to confirmed asleep.
fn sleep_backend(backend: &mut Backend, now: &mut u64) {
    *now += MIN_IDLE_MS + 1_000;
    let decision = backend.begin_drain(*now);
    assert!(
        matches!(decision, SleepDecision::Allowed { .. }),
        "the backend should be drainable: {decision:?}"
    );
    assert!(matches!(
        backend.drain_outcome(*now),
        DrainOutcome::Ready { .. }
    ));
    backend
        .sleep_confirmed(*now)
        .expect("the sleep is confirmed");
    assert!(backend.state().is_asleep());
}

#[test]
fn work_in_flight_is_rejected_as_a_sleep_reason() {
    let mut backend = backend_with(policy());
    let admission = backend
        .admit(RequestMeta::new("cli"), 0)
        .expect("the backend is awake");
    assert!(matches!(admission, Admission::Started(_)));

    let decision = backend.sleep_decision(60_000);
    match decision {
        SleepDecision::Busy {
            in_flight,
            dispatched,
            queued,
            streaming,
        } => {
            assert_eq!(in_flight, 1);
            assert_eq!(dispatched, 1);
            assert_eq!(queued, 0);
            assert_eq!(streaming, 0);
        }
        other => panic!("a busy backend must be reported busy, got {other:?}"),
    }
    // A refused drain must not move the backend into a drain.
    assert_eq!(*backend.state(), ServingState::Awake);
}

#[test]
fn a_backend_sleeps_only_after_the_idle_minimum() {
    let mut backend = backend_with(policy());
    let mut now = 0;
    backend
        .admit(RequestMeta::new("cli"), now)
        .expect("admitted");
    backend.finish(breadram_core::serving::LeaseId(1), now);

    assert!(
        matches!(
            backend.sleep_decision(now + 1_000),
            SleepDecision::Waiting { .. }
        ),
        "one second of quiet is not enough"
    );
    sleep_backend(&mut backend, &mut now);
    assert_eq!(backend.evidence(), ActivityEvidence::Idle { since_ms: 0 });
}

#[test]
fn a_request_arriving_during_a_drain_is_refused_with_a_retry_hint() {
    let mut backend = backend_with(policy());
    let now = MIN_IDLE_MS + 1;
    let decision = backend.begin_drain(now);
    assert!(matches!(decision, SleepDecision::Allowed { .. }));
    assert!(matches!(backend.state(), ServingState::Draining { .. }));

    let err = backend
        .admit(RequestMeta::new("cli"), now)
        .expect_err("a draining backend must refuse");
    assert_eq!(
        err,
        AdmitError::Draining {
            retry_after_ms: policy().retry_after_ms
        }
    );

    // The sleep still completes, which is the point of refusing.
    assert!(matches!(
        backend.drain_outcome(now),
        DrainOutcome::Ready { .. }
    ));
    backend.sleep_confirmed(now).expect("confirmed");
    assert!(backend.state().is_asleep());
}

#[test]
fn an_operator_can_favour_the_client_over_the_sleep() {
    let mut backend = backend_with(ServingPolicy {
        abort_drain_on_arrival: true,
        ..policy()
    });
    let now = MIN_IDLE_MS + 1;
    assert!(matches!(
        backend.begin_drain(now),
        SleepDecision::Allowed { .. }
    ));

    let admission = backend
        .admit(RequestMeta::new("cli").streaming(), now)
        .expect("the arrival cancels the drain instead of being refused");
    assert!(matches!(admission, Admission::Started(_)));
    assert_eq!(
        *backend.state(),
        ServingState::Awake,
        "the drain was abandoned, so the backend is serving"
    );
    assert_eq!(backend.leases().streaming(), 1);
}

#[test]
fn a_long_stream_holds_the_backend_awake_for_its_whole_duration() {
    let mut backend = backend_with(policy());
    let mut now = 0;
    let admission = backend
        .admit(RequestMeta::new("cli").streaming(), now)
        .expect("admitted");
    let id = admission.lease();

    // Well past the idle minimum, and past two drains' worth of time.
    for _ in 0..5 {
        now += 30_000;
        assert!(
            matches!(backend.sleep_decision(now), SleepDecision::Busy { .. }),
            "a stream in flight must hold the backend open"
        );
        assert_eq!(backend.evidence(), ActivityEvidence::Active);
    }

    backend.finish(id, now);
    now += policy().sleep_after_idle_ms;
    assert!(matches!(
        backend.sleep_decision(now),
        SleepDecision::Allowed { .. }
    ));
}

#[test]
fn a_client_that_disappears_mid_stream_releases_its_lease() {
    let mut backend = backend_with(policy());
    let mut now = 0;
    backend
        .admit(RequestMeta::new("cli").streaming(), now)
        .expect("admitted");
    backend
        .admit(RequestMeta::new("cli"), now)
        .expect("a second request from the same client");
    backend
        .admit(RequestMeta::new("other"), now)
        .expect("a request from another client");

    let client = ClientId("cli".to_string());
    let released = backend.cancel_client(&client, now);
    assert_eq!(released.len(), 2, "both of the client's requests are gone");
    assert_eq!(backend.leases().in_flight(), 1, "the other client stays");

    backend.finish(breadram_core::serving::LeaseId(3), now);
    now += policy().sleep_after_idle_ms;
    assert!(
        matches!(backend.sleep_decision(now), SleepDecision::Allowed { .. }),
        "a cancelled stream must not leave the backend permanently busy"
    );
}

#[test]
fn a_queued_request_blocks_sleep_unless_policy_says_otherwise() {
    let mut backend = backend_with(policy());
    backend
        .admit(RequestMeta::new("cli").with_kind(RequestKind::Queued), 0)
        .expect("admitted");
    assert!(
        matches!(
            backend.sleep_decision(60_000),
            SleepDecision::Busy { queued: 1, .. }
        ),
        "a queued request will run, so it holds the backend open"
    );

    let mut permissive = backend_with(ServingPolicy {
        queued_blocks_sleep: false,
        ..policy()
    });
    let mut now = 0;
    permissive
        .admit(RequestMeta::new("cli").with_kind(RequestKind::Queued), now)
        .expect("admitted");
    now += 60_000;
    assert!(
        matches!(permissive.begin_drain(now), SleepDecision::Allowed { .. }),
        "with queued work ignored, the drain may start"
    );

    // The queued request is then dispatched while the drain is running, which is
    // the race the drain deadline exists for.
    assert!(permissive.dispatch(breadram_core::serving::LeaseId(1), now));
    assert!(matches!(
        permissive.drain_outcome(now),
        DrainOutcome::Waiting { in_flight: 1, .. }
    ));
    assert_eq!(
        permissive.drain_outcome(now + 2_000),
        DrainOutcome::TimedOut { in_flight: 1 },
        "a drain that cannot finish is abandoned rather than held open"
    );
    assert_eq!(*permissive.state(), ServingState::Awake);
}

#[test]
fn a_retry_is_not_a_second_request() {
    let mut backend = backend_with(policy());
    let first = backend
        .admit(RequestMeta::new("cli").with_key("req-1"), 0)
        .expect("admitted");
    let Admission::Started(id) = first else {
        panic!("the first attempt is a new request, got {first:?}");
    };

    let joined = backend
        .admit(RequestMeta::new("cli").with_key("req-1"), 1_000)
        .expect("the backend is awake");
    assert_eq!(
        joined,
        Admission::Joined(id),
        "an in-flight retry joins the original"
    );
    assert_eq!(backend.leases().in_flight(), 1, "and is counted once");

    backend.finish(id, 2_000);
    let replayed = backend
        .admit(RequestMeta::new("cli").with_key("req-1"), 3_000)
        .expect("the backend is awake");
    assert_eq!(
        replayed,
        Admission::Replayed(id),
        "a retry after the work finished is not dispatched again"
    );
    assert_eq!(backend.leases().in_flight(), 0);

    // A cancelled request is not remembered as finished, so a retry runs.
    let admission = backend
        .admit(RequestMeta::new("cli").with_key("req-2"), 4_000)
        .expect("admitted");
    backend.cancel(admission.lease(), 4_100);
    let retry = backend
        .admit(RequestMeta::new("cli").with_key("req-2"), 4_200)
        .expect("admitted");
    assert!(
        matches!(retry, Admission::Started(_)),
        "cancellation does not answer a retry from nothing"
    );
}

#[test]
fn a_sleeping_backend_demands_a_wake_and_admits_nothing_until_it_is_healthy() {
    let mut backend = backend_with(policy());
    let mut now = 0;
    sleep_backend(&mut backend, &mut now);

    let refused = backend
        .admit(RequestMeta::new("cli"), now)
        .expect_err("a sleeping backend cannot serve");
    assert_eq!(
        refused,
        AdmitError::Asleep {
            retry_after_ms: policy().retry_after_ms
        }
    );

    let wake = backend.wake_request(now).expect("a wake can start");
    assert_eq!(wake.attempt, 1);
    assert_eq!(wake.workload, workload());
    assert_eq!(wake.timeout_ms, policy().wake_timeout_ms);

    let refused = backend
        .admit(RequestMeta::new("cli").streaming(), now)
        .expect_err("nothing reaches a backend that has not finished waking");
    assert!(
        matches!(refused, AdmitError::Waking { attempt: 1, .. }),
        "got {refused:?}"
    );

    let state = backend.wake_completed(WakeHealth::Healthy, now);
    assert_eq!(state, ServingState::Awake);
    backend
        .admit(RequestMeta::new("cli"), now)
        .expect("requests flow once the wake is verified");
}

#[test]
fn awaken_that_never_became_healthy_keeps_the_backend_unavailable() {
    let mut backend = backend_with(policy());
    let mut now = 0;
    sleep_backend(&mut backend, &mut now);

    let wake = backend.wake_request(now).expect("a wake can start");
    assert_eq!(wake.attempt, 1);
    let state = backend.wake_completed(unhealthy("the model directory is missing"), now);
    assert_eq!(
        state,
        ServingState::Unverified {
            since_ms: now,
            attempts: 1
        },
        "a failed attempt is remembered, not reset"
    );
    assert!(backend.leases().is_empty());
}

#[test]
fn a_wake_that_keeps_failing_becomes_an_actionable_failure() {
    let mut backend = backend_with(policy());
    let mut now = 0;
    sleep_backend(&mut backend, &mut now);

    for attempt in 1..=3 {
        let wake = backend
            .wake_request(now)
            .unwrap_or_else(|err| panic!("attempt {attempt} should be allowed: {err}"));
        assert_eq!(wake.attempt, attempt);
        backend.wake_completed(unhealthy("the provider is unreachable"), now);
    }

    assert_eq!(
        backend.state().name(),
        "failed",
        "the attempt budget is bounded"
    );
    let refused = backend
        .admit(RequestMeta::new("cli"), now)
        .expect_err("a failed backend cannot serve");
    let message = refused.to_string();
    assert!(
        message.contains("the provider is unreachable"),
        "the client error must carry the reason: {message}"
    );

    let err = backend
        .wake_request(now)
        .expect_err("waking must not be retried forever");
    assert!(
        matches!(
            err,
            TransitionError::WrongState {
                state: "failed",
                ..
            }
        ),
        "got {err:?}"
    );

    // An operator can clear it, and then health must be proven before serving.
    assert!(matches!(
        backend.reset(now),
        ServingState::Unverified { attempts: 0, .. }
    ));
    let refused = backend
        .admit(RequestMeta::new("cli"), now)
        .expect_err("an unverified backend cannot serve");
    assert!(matches!(refused, AdmitError::Unverified { .. }));
    assert_eq!(
        backend
            .verify_completed(WakeHealth::Healthy, now)
            .expect("verified"),
        ServingState::Awake
    );
}

#[test]
fn a_wake_that_times_out_spends_an_attempt() {
    let mut backend = backend_with(policy());
    let mut now = 0;
    sleep_backend(&mut backend, &mut now);

    backend.wake_request(now).expect("a wake can start");
    let state = backend.wake_timed_out(now);
    assert_eq!(
        state,
        ServingState::Unverified {
            since_ms: now,
            attempts: 1
        }
    );
    let wake = backend.wake_request(now).expect("a second attempt");
    assert_eq!(wake.attempt, 2);
    assert_eq!(
        backend.wake_completed(WakeHealth::Healthy, now),
        ServingState::Awake
    );
}

#[test]
fn a_confirmed_sleep_cannot_be_recorded_while_work_is_in_flight() {
    let mut backend = backend_with(ServingPolicy {
        queued_blocks_sleep: false,
        ..policy()
    });
    let mut now = 0;
    backend
        .admit(RequestMeta::new("cli").with_kind(RequestKind::Queued), now)
        .expect("admitted");
    now += 60_000;
    assert!(matches!(
        backend.begin_drain(now),
        SleepDecision::Allowed { .. }
    ));
    backend.dispatch(breadram_core::serving::LeaseId(1), now);

    let err = backend
        .sleep_confirmed(now)
        .expect_err("a backend with work in flight must not be declared asleep");
    assert_eq!(err, TransitionError::Busy { in_flight: 1 });
    assert!(matches!(backend.state(), ServingState::Draining { .. }));

    // And a confirmation that arrives out of order is refused rather than
    // silently accepted.
    backend.abort_drain();
    assert!(matches!(
        backend.sleep_confirmed(now),
        Err(TransitionError::WrongState { state: "awake", .. })
    ));
}

#[test]
fn unprovable_idleness_makes_automatic_unload_unsupported() {
    for tracking in [
        TrackingCompleteness::Unknown,
        TrackingCompleteness::Bypassable {
            provider_proves_idle: false,
        },
    ] {
        let mut backend = Backend::new(workload(), tracking, policy(), 0);
        assert_eq!(
            backend.state().name(),
            "unsupported",
            "{tracking:?} cannot prove idleness"
        );
        assert!(
            backend
                .unsupported_reason()
                .is_some_and(|reason| reason.contains("prove") || reason.contains("known")),
            "the refusal must explain itself"
        );
        let refused = backend
            .admit(RequestMeta::new("cli"), 0)
            .expect_err("an unsupported backend does not serve through breadram");
        assert!(
            matches!(refused, AdmitError::Unavailable { .. }),
            "got {refused:?}"
        );

        // Explicit operator policy is the documented way to override it.
        let mut allowed = Backend::new(
            workload(),
            tracking,
            ServingPolicy {
                allow_untracked_unload: true,
                ..policy()
            },
            0,
        );
        assert_eq!(*allowed.state(), ServingState::Awake);
        allowed.admit(RequestMeta::new("cli"), 0).expect("admitted");
    }
}

#[test]
fn a_provider_that_proves_idleness_is_drainable_even_when_requests_bypass_tracking() {
    let mut backend = Backend::new(
        workload(),
        TrackingCompleteness::Bypassable {
            provider_proves_idle: true,
        },
        policy(),
        0,
    );
    assert_eq!(*backend.state(), ServingState::Awake);
    assert!(backend.unsupported_reason().is_none());
    let mut now = 0;
    sleep_backend(&mut backend, &mut now);
}

#[test]
fn a_restart_reconciles_sleeping_state_without_guessing() {
    // The provider says it is asleep: believe it, and require a wake.
    let mut backend = backend_with(policy());
    assert_eq!(
        backend.reconcile(ProviderState::Sleeping, 100),
        ServingState::Sleeping {
            since_ms: 100,
            wake_attempts: 0
        }
    );

    // The provider says it is serving: serve.
    assert_eq!(
        backend.reconcile(ProviderState::Serving, 200),
        ServingState::Awake
    );

    // The provider does not answer: verify before serving, do not guess.
    assert_eq!(
        backend.reconcile(ProviderState::Unknown, 300),
        ServingState::Unverified {
            since_ms: 300,
            attempts: 0
        }
    );
    assert!(matches!(
        backend.admit(RequestMeta::new("cli"), 300),
        Err(AdmitError::Unverified { .. })
    ));
    assert_eq!(
        backend
            .verify_completed(WakeHealth::Healthy, 300)
            .expect("verified"),
        ServingState::Awake
    );

    // A restart also loses the in-flight requests, and the leases say so rather
    // than reporting a backend that looks busy forever.
    let mut busy = backend_with(policy());
    busy.admit(RequestMeta::new("cli").streaming(), 0)
        .expect("admitted");
    assert_eq!(busy.evidence(), ActivityEvidence::Active);
    busy.reconcile(ProviderState::Serving, 400);
    assert_eq!(busy.leases().in_flight(), 0);
    assert_eq!(busy.evidence(), ActivityEvidence::Idle { since_ms: 400 });
}

#[test]
fn the_policy_cannot_be_configured_into_an_unsafe_lifecycle() {
    let bounded = backend_with(ServingPolicy {
        sleep_after_idle_ms: 0,
        max_wake_attempts: 0,
        drain_timeout_ms: 0,
        wake_timeout_ms: 0,
        retry_after_ms: 0,
        ..ServingPolicy::default()
    });
    assert_eq!(bounded.policy().sleep_after_idle_ms, MIN_IDLE_MS);
    assert_eq!(bounded.policy().max_wake_attempts, 1);
    assert!(bounded.policy().drain_timeout_ms > 0);
    assert!(bounded.policy().wake_timeout_ms > 0);
    assert!(bounded.policy().retry_after_ms > 0);
}

#[test]
fn leases_are_the_evidence_the_planner_sees() {
    let mut tracked = WorkloadLeases::new(workload(), 0);
    assert_eq!(
        tracked.leases().evidence(),
        ActivityEvidence::Idle { since_ms: 0 }
    );

    tracked
        .leases_mut()
        .admit(RequestMeta::new("cli").streaming(), 1_000);
    assert_eq!(tracked.leases().evidence(), ActivityEvidence::Active);
    assert_eq!(tracked.evidence(&workload()), ActivityEvidence::Active);

    // A workload this tracker does not serve is unknown, never idle.
    let other = WorkloadId {
        scope: "unit:llamacpp.service".to_string(),
        generation: 1,
    };
    assert_eq!(tracked.evidence(&other), ActivityEvidence::Unknown);

    // Only an idle period longer than the policy minimum counts, so a momentary
    // gap between two requests of the same conversation is not enough.
    tracked
        .leases_mut()
        .finish(breadram_core::serving::LeaseId(1), 2_000);
    let evidence = tracked.evidence(&workload());
    assert!(!evidence.is_confirmed_idle(2_500, MIN_IDLE_MS));
    assert_eq!(evidence, ActivityEvidence::Idle { since_ms: 2_000 });
}
