# breadram adapter contract

An adapter is an optional integration with a specific runtime. breadram manages
memory generically through procfs, cgroups, and systemd, and needs no adapter at
all. Registering one only adds knowledge and, at most, bounded control
operations.

The isolation runs one way: the policy core depends on a single trait,
`ActivitySource`, and the adapter registry implements it. Nothing in policy
refers to an adapter, so every adapter can be absent, disabled, or broken
without affecting generic management.

## The trait

Implement `breadram_core::adapter::WorkloadAdapter`:

```rust
pub trait WorkloadAdapter: Send + Sync {
    fn name(&self) -> &str;
    fn kind(&self) -> ProviderKind;
    fn endpoint(&self) -> &str;
    fn capabilities(&self) -> &AdapterCapabilities;
    fn owns(&self, workload: &Workload) -> bool;

    // The following all have conservative defaults.
    fn discover(&self) -> Vec<ProbeRequest> { Vec::new() }
    fn inventory(&self) -> Vec<ManagedResource> { Vec::new() }
    fn activity(&self, _workload: &Workload) -> ActivityEvidence { ActivityEvidence::Unknown }
    fn operation(&self, _action: ActionKind, _workload: &Workload) -> Option<AdapterOperation> { None }
}
```

Only the identity methods are required, so an adapter that can do nothing but
claim a workload and report whether it is busy is a valid adapter.
`MockAdapter` in `breadram-core/src/adapter/mock.rs` is the worked example.

Registration:

```rust
let mut registry = AdapterRegistry::new();
registry.register(Box::new(MyAdapter::new(endpoint)));
let source = registry.activity_source(&workloads);
let plan = plan(PlannerInput { state, snapshot, workloads, config, history, activity: &source }, channel);
```

## Rules the contract enforces

**Read-only discovery.** `ProbeRequest` cannot express a write: its `Method`
enum has only `Get`. Writes are reachable only through `OperationRequest`, which
an adapter returns from `operation`.

**Explicit endpoints only.** A `ProbeClient` is constructed with the set of
endpoint prefixes it may talk to. A request outside that set fails with
`ProbeError::NotAllowed` before anything is sent, so there is no discovery scan
and no port guessing. A non-loopback `http://` target fails with
`ProbeError::InsecureUrl`; use `https`, a unix socket, or loopback.

**Capabilities are never assumed.** `AdapterCapabilities::observation_only()` is
the default. `supports()` is true only for `CapabilityState::Available`, so a
`Degraded` reading is reported but never used to trigger an operation. A reading
from a `probe_version` other than `CAPABILITY_PROBE_VERSION` is discarded and the
adapter falls back to observation.

**No inference from a model API.** Classify a probe with
`adapter::classify`: `404`, `405`, and `501` mean the endpoint does not expose
the surface, which is the ordinary answer from an OpenAI-compatible inference API
asked about model administration. It is recorded as unsupported, never guessed.

**Idleness must be proven.** `activity` returns `Unknown` by default, and policy
never treats `Unknown` as idle. An `Idle { since_ms }` reading only counts once it
has held for the policy minimum, and a `since_ms` in the future cannot be read as
a long idle period.

**Secrets never render.** Wrap a credential in `Redacted`. `Debug` and `Display`
print `[redacted]`, so it cannot leak through a request's derived `Debug`, an
error message, or a log line. The value is only readable through `expose()`,
which the transport calls when putting it on the wire.

**Operational bounds are the client's job.** `ProbeClient` applies a per attempt
timeout, bounded retries with exponential backoff, a response size limit, and a
circuit breaker that opens after repeated failures and fails fast, so a dead
endpoint is not hammered. TLS and socket handling belong to the `Transport`.

**An operation states its effect, its consequence, and its confirmation.**
`AdapterOperation` carries `expected_host_ram_delta` and
`expected_device_ram_delta`, whether it is reversible, a `note`, and a read-only
`Confirmation`. Policy uses the delta: an operation expected to raise host memory
is refused while the host is pressured, which is exactly why a provider offload
(sleep) is not chosen under host pressure even when the provider supports it. The
`note` is what a person is told: a release that costs a cold start, or an offload
that moves memory rather than freeing it, is never reported as free.

**A confirmation must check something.** `Confirmation::contains` requires a
substring to appear and `Confirmation::absent` requires one to be gone, so an
effect that is the *disappearance* of something is expressible. A confirmation
that checks neither is never a success, because an unverifiable check cannot
establish that the operation took effect.

## The Ollama adapter

`breadram-core/src/adapter/ollama.rs` is the reference implementation for a
model server, and the shape another runtime should copy. It talks to one
configured endpoint and can produce exactly three URLs:

| Purpose | Request |
| --- | --- |
| Inventory | `GET /api/ps` |
| Diagnostics | `GET /api/version` |
| Release | `POST /api/generate` with `{"model": ..., "keep_alive": 0}` |

It never pulls a model, deletes a model file, or calls an inference endpoint.
That is a property of the code rather than a promise: those three paths are the
only ones it can build.

**Version support is behavioural.** A build is supported when `/api/ps` answers
with a parseable body; one that does not is reported unsupported with the reason.
The reported version is kept for diagnostics, since a version number is not what
decides whether the inventory works.

**Memory is reported in two parts.** Each loaded model reports its total size and
the part held in device memory. The difference is reported as model bytes on the
host, never as host RAM usage: a memory-mapped model shares its pages with the
page cache, so its file size is not the memory it costs.

**Being loaded is not being idle.** A listed model is always `Active`, whatever
its reported expiry says, because a model that is loaded can still be serving a
long request. Idleness comes from the serving coordination contract: either an
empty model list observed continuously, or complete request tracking through
`begin_request`/`end_request`/`cancel_request`, in which case a resident but
unused model is idle and that is exactly the state worth relieving.

**Keep-alive policy is preserved.** A model is released only when it is not
operator-pinned *and* the reported expiry reads as an ordinary keep-alive. No
reported expiry, an expiry that does not parse, and a residency beyond
`DEFAULT_MAX_BOUNDED_KEEP_ALIVE_MS` are all treated as a policy choice to keep
the model loaded. `release_refusals()` names the reason for each one so a model
left in memory is explained rather than skipped in silence, and
`with_max_bounded_keep_alive` moves the horizon for a deployment that really does
want to release a long-keep-alive model.

This is the one place an adapter reads a wall clock, because a service expresses
an expiry in wall-clock time. Everything that decides whether inference is
happening uses monotonic milliseconds supplied by the caller, and
`with_reference_time` exists so a test can fix the instant the policy is read
against.

## Adding a runtime

1. Implement `WorkloadAdapter`, returning `Unknown` from `activity` until you can
   actually tell.
2. Build `AdapterCapabilities` by probing with `classify`, or state them with
   `AdapterCapabilities::configured` if probing is not appropriate.
3. Register the adapter. If you need a new capability or action, extend
   `AdapterCapability` and bump `CAPABILITY_PROBE_VERSION`; policy does not change.
4. Provide a `Transport` for the real endpoint. `ScriptedTransport` covers tests
   and `RecordingSleeper` lets you assert the backoff without waiting.
5. State what a release costs in the operation's `note`, and make the evidence
   for it provable to a reader: `adapter::ollama` and `adapter::mock` are the two
   worked examples, and every rule above has a test that pins it.
