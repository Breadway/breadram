# breadram

Automatic memory management for BOS and the bread ecosystem.

breadram watches Linux memory pressure and, when asked to, applies bounded and
reversible relief so a machine keeps doing useful work instead of thrashing or
letting the out of memory killer pick a victim at random.

## What the first release does

The first release is the standalone core: a daemon and a CLI that monitor host
and cgroup memory pressure, identify large workloads, decide when the machine
is under pressure, and apply authorized, reversible controls. It runs headless
with nothing else installed. Automatic per workload relief, model server
adapters, job admission, and desktop integration follow in later milestones
(see `docs/ISSUE-MAP.md`).

By default it runs in `monitor` mode: it observes, explains, and records, and
does not change system state. `automatic` mode is opt in.

## Current state

The M1 foundation is implemented: telemetry, workload inventory, validated
configuration, and the daemon with its versioned local IPC. The daemon monitors
memory and workloads and serves a status snapshot over an owner only Unix
socket; the CLI reads it. Pressure policy, controls, model adapters, job
admission, and desktop integration are still to come, so the daemon currently
observes and reports rather than acting.

## Compatibility limits

- Supported and tested baseline is Linux with cgroup v2 and systemd. cgroup v1
  and missing PSI are degraded, explicitly reported states, not silent
  failures.
- No root daemon is required. Controls that need privilege go through a small
  typed helper or the owning manager, and only when the operator authorizes it.
- breadsearch, breadd, BOS, a compositor, model runtimes, and GPU libraries are
  all optional. Core management never waits on any of them.
- breadram coexists with existing OOM management and does not install a second
  out of memory killer.

## Build

```
cargo build --workspace
```

Run the daemon and client:

```
cargo run -p breadramd -- --mode monitor          # or --profile desktop
cargo run -p breadram-cli -- status
cargo run -p breadram-cli -- --json capabilities
cargo run -p breadram-cli -- watch
```

The daemon listens on `$XDG_RUNTIME_DIR/breadram.sock` unless `--socket` is
given, and the socket is owner only. `breadram` exits 0 on success, 1 when the
daemon is unavailable or returns an error, and 2 for a usage error.

## Layout

```
crates/breadram-core   shared contract types, no system or bread dependencies
crates/breadramd       the daemon binary
crates/breadram-cli    the breadram client binary
docs/                  project brief, architecture, issue map, handoff
```

## Documentation

- `docs/PROJECT.md`: the product contract.
- `docs/ARCHITECTURE.md`: the decisions the implementation honours.
- `docs/ISSUE-MAP.md`: the full card map and workflow.
- `docs/AGENT-HANDOFF.md`: starting point for the next agent.
- `docs/CLI.md`: the command line surface, its output contract, and exit codes.
- `docs/ADAPTER-CONTRACT.md`: the optional workload adapter interface.
- `docs/SERVING-COORDINATION.md`: request leases and the drain, sleep, and wake lifecycle.
- `CONTRIBUTING.md`: branch model and checks.

## License

MIT. See `LICENSE`.
