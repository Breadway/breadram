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

**An operation states its effect and its confirmation.** `AdapterOperation`
carries `expected_host_ram_delta` and `expected_device_ram_delta`, whether it is
reversible, and a read-only `Confirmation`. Policy uses the delta: an operation
expected to raise host memory is refused while the host is pressured, which is
exactly why a provider offload (sleep) is not chosen under host pressure even
when the provider supports it.

## Adding a runtime

1. Implement `WorkloadAdapter`, returning `Unknown` from `activity` until you can
   actually tell.
2. Build `AdapterCapabilities` by probing with `classify`, or state them with
   `AdapterCapabilities::configured` if probing is not appropriate.
3. Register the adapter. If you need a new capability or action, extend
   `AdapterCapability` and bump `CAPABILITY_PROBE_VERSION`; policy does not change.
4. Provide a `Transport` for the real endpoint. `ScriptedTransport` covers tests
   and `RecordingSleeper` lets you assert the backoff without waiting.
