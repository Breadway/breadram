# Isolated CPU test support (RAM-29, partial)

Run from the repository root:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace
cargo test --workspace
cargo test -p breadramd --test ipc --test command_surface
cargo test -p breadram-core --test policy_replay --test action_coordinator --test provider_execution
```

Expected result: all selected CPU tests pass, with no optional service required.
Policy replay covers pressure/recovery; coordinator failure injection covers busy,
unknown support, timeout, rollback and replaced targets; provider execution uses
a fake loopback service, never an operator's model endpoint.

Both daemon suites now share `tests/support/mod.rs`: a monitor-only child owner
that kills/reaps on panic, closes inherited pipes, removes its owned socket, and
uses a ten-second readiness deadline. Restart tests can explicitly stop while
preserving the stale socket. The unwind regression verifies child disappearance
from `/proc` and socket removal. No workload pressure or desktop action runs.

Remaining RAM-29 acceptance: reusable telemetry/action fixtures and fake-provider
server support shared across core suites, a capped disposable pressure harness,
and real-hardware gating that reports skips. No hardware suite was run here.
Never pressurize the host, access Hestia, or unload active models as a substitute
for isolation. This change is not a certification of those unfinished suites.
