# Serving coordination

Sleeping a model backend frees more memory than anything else breadram does, and
it is the only action that can break a client. This document describes the part
that makes it safe: request leases, the drain/sleep/wake lifecycle, and the rules
that decide when automatic unload is allowed at all.

Everything here is in `breadram-core`, in `serving`. The module performs no I/O:
it decides what is allowed and reports why, and the caller (a provider adapter, or
a local gateway) performs the provider call and reports the result back. Because
the module owns no clock and no socket, every race below is exercised in tests
against a clock the test advances by hand.

## Request leases

A lease records that a request is in flight. It records nothing about the
request: a client label, a request kind, whether the response is streamed, an
optional idempotency key, and monotonic timestamps. There is nowhere to put a
prompt, a completion, a response body, or a credential, so "never log prompts or
completions" is a property of the types rather than a convention to audit.

| Behaviour | Why it matters |
|---|---|
| A retry with the same idempotency key while the request is in flight **joins** it | A client that retries a long request must not keep a backend awake by accident |
| A retry after the request finished is **replayed**, not dispatched | The work is already done; dispatching it again would double the load |
| A cancelled or disconnected request **releases** its lease and is not remembered as finished | A client that disappears mid-stream must not leave the backend permanently busy, and a retry must actually run |
| Queued requests hold a lease | A queued request will run, so sleeping now would mean waking immediately |
| A stream holds its lease for its whole duration | Idleness is never inferred from a quiet socket |

The tracker is bounded: at most `RETRY_MEMORY` finished keys are remembered, so a
busy gateway does not grow without bound. An evicted key is treated as a new
request, which is the safe direction.

Idleness is reported as `ActivityEvidence`: `Active` while anything is in flight,
and `Idle { since_ms }` from the moment the last lease ended. The policy minimum
still applies on top, so a momentary gap between two requests of the same
conversation is not enough. This is the same evidence type the planner already
consumes, so a workload with a live lease is never released even when its
processes are quiet and its cgroup looks idle.

## The lifecycle

One backend at a time moves through:

- `awake`: serving. Requests are admitted.
- `draining`: asked to stop, waiting for in-flight work to end. Requests are
  refused with a retry hint, unless the operator chose
  `abort_drain_on_arrival`, in which case the arrival cancels the drain and the
  backend goes back to serving.
- `sleeping`: the provider confirmed the sleep. Requests are refused with a wake
  demand.
- `waking`: a wake is in progress. Nothing is admitted, whatever the health.
- `unverified`: not known to be serving and not known to be asleep. This is what a
  restart against an unresponsive provider looks like. A health check is required
  before serving, and no request is forwarded on a guess.
- `unsupported`: unload is not provable for this backend. See below.
- `failed`: waking failed too many times. Clients get an actionable error until
  an operator resets it or a fresh reconcile clears it.

Three rules are structural rather than procedural:

**Sleeping is a transition, not a side effect.** The state machine never sleeps by
itself. `begin_drain` returns the decision, `drain_outcome` says when the provider
call may be made, and `sleep_confirmed` records the result. A drain that cannot
finish inside `drain_timeout_ms` is abandoned and the backend stays awake and
serving, because holding a backend in limbo is worse than not sleeping it.

**Nothing is admitted without a verified wake.** Only `WakeHealth::Healthy`
returns a backend to `awake`. An unhealthy wake or a timeout spends an attempt and
returns the backend to a state a later demand can wake from, with the attempt
counted, so the retry budget is spent rather than reset. After
`max_wake_attempts` the backend is `failed`, and the error a client sees names the
reason and the attempt count.

**A zero-length idle minimum or attempt budget is clamped, not accepted.** A zero
idle minimum would let a backend sleep between two requests of the same
conversation, and a zero attempt budget would make waking fail before it was
tried.

## When automatic unload is unsupported

Provable idleness is the precondition for sleeping. Two answers make it
unprovable:

- `TrackingCompleteness::Unknown`: it is not known whether every request reaches
  the backend through breadram.
- `TrackingCompleteness::Bypassable { provider_proves_idle: false }`: clients can
  reach the backend directly, and the provider cannot prove it has no work in
  flight.

For both, the backend starts `unsupported` and automatic unload is refused with a
reason, and the refusal is reportable on its own through `unsupported_reason()` so
a diagnostic bundle can explain it before anything is attempted. An operator can
override it explicitly with `ServingPolicy::allow_untracked_unload`. A
`Bypassable { provider_proves_idle: true }` backend is drainable without an
override, because the provider proves what breadram cannot observe.

## Restarts

A restart loses the in-flight requests: their clients see a closed connection and
retry, and a retry carries the same idempotency key. `reconcile` therefore clears
the leases rather than reporting a backend that looks busy forever, and sets the
state from what the provider reports:

| Provider reports | Resulting state |
|---|---|
| Sleeping | `sleeping`, so a request demands a wake |
| Serving | `awake` |
| Nothing | `unverified`, so health is checked before serving |

## Not implemented yet

The lifecycle and the leases are complete and tested. The optional local
inference gateway that would feed them from real traffic is a separate piece of
work: it must preserve native and OpenAI-compatible request and response
semantics, including streaming, and it must admit every request through a lease
before dispatch. Until it exists, the only `ActivitySource` an adapter provides
is the provider's own answer, and a backend whose idleness cannot be proven is
reported unsupported rather than guessed at.
