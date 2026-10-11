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

This repository is at RAM-1, the project contract and workspace bootstrap. The
daemon and CLI build and run, but they are bootstrap binaries: they parse
arguments, print the contract, and exit. Telemetry, workload identity, the
daemon lifecycle and IPC, and configuration land in RAM-2 through RAM-5.

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
cargo run -p breadramd -- --mode monitor
cargo run -p breadram-cli -- status
```

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
- `CONTRIBUTING.md`: branch model and checks.

## License

MIT. See `LICENSE`.
