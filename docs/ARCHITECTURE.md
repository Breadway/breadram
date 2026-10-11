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

The swap and OOM layout is therefore reported rather than managed, in
`breadram_core::telemetry::swap` and in `diagnostics`: which swap areas exist and
what each is backed by, whether swap compresses, the effective swap limit of each
workload breadram can control, whether a systemd OOM manager is installed, and
how many OOM kills the kernel has recorded. Nothing in that module writes. A
missing reading is reported as unknown and never as zero, so an unreadable swap
table does not read as a host with no swap. The rules and the degraded cases are
in `docs/SWAP-AND-OOM.md`.

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

### AD-11: Execution is one step, gated by the mode

The planner proposes and the coordinator disposes. The daemon joins them in one
place, `breadram_core::execution`, and that is the only code that decides whether
a proposed action may be written: the management mode is checked there, and the
configured allowed actions are checked again because a plan can outlive the
configuration it was made under. In any mode other than `automatic` the plan is
still built and explained and nothing is written, so a monitor mode daemon
describes exactly what automatic mode would do.

What breadram changed is remembered across runs in a versioned state file under
the state directory, and a restart restores a value only while it still equals
what breadram last wrote. A value something else changed is left alone, a target
that no longer exists is skipped, and a file this build does not understand is
reported and ignored rather than reinterpreted. Monitor mode never writes, so it
never restores either: it reports leftover values instead of clearing them.

### AD-12: Provider actions run through the same executor boundary

A provider action is a write like any other, so it goes through
`coordinator::Executor` rather than around it. `adapter::AdapterExecutor`
implements that boundary for the provider-backed actions: it revalidates the
capability and the workload generation, refuses a release that needs proven
idleness when idleness is unknown or too recent, checks every URL against the
configured endpoints before anything is sent, applies the operation through a
bounded loopback transport, confirms the effect with a read-only probe, and
reports the memory the provider holds before and after so the journalled effect
is measured rather than asserted. `ChainedExecutor` keeps host controls and
provider actions in one pass, dispatching by what each executor claims.

Three consequences are structural. First, a provider is read before it is
reasoned about: the daemon refreshes every configured endpoint each tick, so a
policy decision is never based on a stale reading, and a failed read is recorded
as a failure rather than left as an old success. Second, an executor that does
not know how to measure returns nothing rather than zero, so "not measured" and
"measured as zero" stay different in the journal. Third, nothing is undone
blindly: a provider action is undone by the provider's own surface or not at
all, and the executor reports that rather than guessing.

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
