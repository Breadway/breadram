# breadram architecture

This document records the decisions the implementation must honour. It is the
contract from RAM-1; later cards fill in the behaviour behind it.

## Crate layout

The workspace keeps a reusable core separate from the two binaries so that the
daemon, the CLI, and any future client share one definition of the contract.

| Crate | Role |
|-------|------|
| `crates/breadram-core` | Shared contract types. No system, bread, model, or GPU dependencies. |
| `crates/breadramd` | The daemon binary. Owns telemetry, policy, and control. |
| `crates/breadram-cli` | The `breadram` client binary. Talks to the daemon over local IPC. |

`breadram-core` is deliberately close to dependency free. If a type is only
needed by the daemon's implementation, it belongs in `breadramd`, not in the
core. This keeps the contract stable and keeps the core buildable and testable
anywhere.

## Decisions

### AD-1: Core is standalone

Core operation must not depend on breadsearch, breadd, BOS, a compositor, a
model runtime, or GPU libraries. The dependency direction is one way: optional
integrations depend on breadram, not the reverse. A machine with none of them
still gets full monitoring and standalone policy.

Consequence: `breadram-core` has no bread-ecosystem dependencies at all. A
future bread integration is an optional crate or an optional feature, never a
core dependency.

### AD-2: Monitor is the default

A fresh install observes and explains; it does not act. Automatic mode is opt
in. This makes the failure mode of a misconfigured daemon "did nothing" rather
than "did something surprising".

### AD-3: Report capability, do not assume it

The daemon probes what the host actually supports and reports it. Missing PSI,
cgroup v1, denied metadata, and unavailable controls are explicit degraded or
unavailable states with reasons. Absence of data is never treated as evidence
of a working feature, and is never treated as evidence that a workload is idle.

### AD-4: No second OOM killer

breadram coexists with existing OOM management. Routine policy does not kill
arbitrary workloads, drop global caches, or rewrite global VM settings.
Anything that can terminate a process is an explicit, documented, opt in
policy with tested consequences.

### AD-5: Reversible, ownership aware changes

Every applied change records the original value and its owner. Restores happen
only for values breadram still owns. This prevents breadram from clobbering a
newer value written by another manager, and makes crash and restart recovery
safe.

### AD-6: Typed, least privilege control

Controls that need privilege go through a small typed surface or through the
owning manager (systemd). There is no general root service and no arbitrary
command execution path. Helper surfaces accept typed operations against
verified targets only.

### AD-7: One versioned IPC contract

The daemon and every client share one local socket protocol with request ids,
actionable errors, and a major/minor version. The major version must match; a
peer may speak a lesser minor. This is defined in `breadram_core::ipc` so the
rule cannot drift between implementations.

### AD-8: Adapters are optional and isolated

Workload adapters register explicitly and can fail or be absent without
stalling monitoring or disabling core management. An adapter declares its
capabilities and version; unknown capabilities fall back to observation. There
is no network scanning: endpoints are explicit or come from verified local
service metadata.

### AD-9: Later milestone crates

When the daemon grows, its internals are split by concern (telemetry, inventory,
policy, control, adapters) inside `breadramd` rather than by new public crates.
A concern only becomes its own crate if another binary genuinely needs to link
it.

### AD-10: No Go in this project

breadram is Rust. The user is learning Go and does not want handed Go
solutions, so no Go source is written anywhere in this repository.

## Verification

Every change must pass, from a clean checkout:

```
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace
cargo test --workspace
```

Cards that touch system behaviour must additionally prove the standalone path
with no optional integration installed. Tests that need hardware breadram
cannot assume (GPU, specific providers) must be explicitly gated and must
report skips honestly instead of passing silently.
