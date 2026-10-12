# Isolated CPU and gated hardware testing (RAM-29)

Run from the repository root:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace
cargo test --workspace
cargo test -p breadramd --test ipc --test command_surface
cargo test -p breadram-core --test policy_replay --test action_coordinator --test provider_execution
cargo test -p breadram-core --test pressure_harness --test hardware_gate -- --nocapture
```

Expected result: all selected CPU tests pass, with no optional service required.
Policy replay covers pressure and recovery; coordinator failure injection covers
busy, unknown support, timeout, rollback and replaced targets; provider execution
uses a fake loopback service, never an operator's model endpoint.

## Shared fixtures

`crates/breadram-core/tests/support/mod.rs` holds the fixtures the core suites
share, so a new suite does not rebuild them:

- `snapshot(now_ms, avg10, available_fraction)`, the deterministic memory
  snapshot, used by `policy_replay` and `provider_execution`.
- `cgroup_workload(name, scope, bytes)`, the cgroup-accounted workload fixture.
- `have(program)`, a PATH probe for a gated test.
- `capped_scope(...)`, the capped disposable pressure harness below.
- `fake_provider`, the in-process fake Ollama loopback service used by
  `provider_execution`, with its model constants and fixed reference instant.
- `fake_executor`, the scripted coordinator executor with failure injection
  used by `action_coordinator`, with the lock and owned-value helpers.

## Capped pressure harness

Pressure tests must never exhaust the host or outlive the test.
`support::capped_scope` runs a command inside a transient systemd user scope with
a bounded `MemoryMax` and `MemorySwapMax` under a hard timeout, and kills the
scope on timeout. `pressure_harness.rs` exercises it. When the host cannot run a
user scope (no `XDG_RUNTIME_DIR`, no `systemd-run`, or the scope does not start),
the harness prints why and the test skips. Never pressurize the host, access
Hestia, or unload active models as a substitute for isolation.

## Gated hardware

`hardware_gate.rs` probes for `llama-server` and `vllm` and reports, in its
output (`-- --nocapture`), whether each real-hardware suite can run or is
skipped. A missing runtime is a reported skip, never a silent pass. The CPU-only
suite never needs a model runtime.

## Daemon suites

Both daemon suites share `tests/support/mod.rs` in `breadramd`: a monitor-only
child owner that kills and reaps on panic, closes inherited pipes, removes its
owned socket, and uses a ten-second readiness deadline. Restart tests can
explicitly stop while preserving the stale socket. The unwind regression verifies
child disappearance from `/proc` and socket removal. No workload pressure or
desktop action runs.

## Remaining on this card

None. The telemetry and workload fixtures, the in-process fake provider server,
and the action fixtures all live in shared `support` now. No hardware suite was
run here: `hardware_gate` reports the llama-server and vllm availability as
skips on hosts without them.