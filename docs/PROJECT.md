# breadram project brief

breadram manages Linux memory pressure for arbitrary workloads. It watches host
and cgroup memory behaviour, decides when a machine is under pressure, and
applies bounded, reversible actions to relieve that pressure while preserving
the workloads the user cares about.

The problem it solves is the ordinary one: a desktop or small server runs a
model server, a browser, a build, or several of them at once, memory fills,
swap churns, and the machine stalls or the out of memory killer picks a victim
nobody wanted. breadram aims to make that outcome rare by acting earlier and
more precisely than a global kernel knob can.

## What breadram is not

- Not a second out of memory killer. It coexists with `systemd-oomd` and any
  configured OOM policy instead of installing a competing one.
- Not a global tuning tool. It does not rewrite swappiness, swapfiles, fstab,
  or kernel parameters behind the operator's back.
- Not tied to the bread ecosystem. breadsearch, breadd, BOS, a compositor,
  model runtimes, and GPU libraries are all optional.

## Hard product constraint

Core operation must work with none of the optional integrations present. A
headless Linux machine with no bread apps, no breadd, and no model runtime
installed must still get full monitoring, standalone policy, and authorized
controls. Any card whose plan assumes an optional component is present is
wrong until it proves the standalone path.

## Modes

| Mode | Behaviour |
|------|-----------|
| `monitor` | Observe, explain, and record. Never changes system state. This is the default. |
| `automatic` | Applies configured, reversible actions when policy calls for them. |

Monitor mode is the default so that a fresh install is inert until the operator
opts in. Every automatic action has a monitor-only explanation that describes
what it would have done and why.

## Capability reporting

The daemon reports what it can actually do on the current host. A capability
that is missing, denied, or only partly usable is reported as unavailable or
degraded with a reason, and is never presented as working. See
`breadram_core::capability`.

## Supported baseline

- Linux with cgroup v2 and systemd as the supported, tested baseline.
- cgroup v1 and missing PSI are degraded, explicitly reported states, not
  silent failures.
- Kernel PSI (`/proc/pressure/memory`) is the preferred pressure signal when
  present; polling is the fallback.
- No root daemon is required. Controls that need privilege are narrowly scoped
  and typed, never a general root service.

## Privilege boundary

breadram runs as the user. It observes only what the user can observe. Where a
control requires more privilege, it is applied through a small, typed helper or
through the owning manager (systemd), and only when the operator authorizes it.
breadram never executes configuration received from an inference endpoint or
from discovered process metadata.

## IPC versioning

The daemon, the CLI, and the desktop UI share one versioned local socket
protocol. The major version must match exactly; a peer may speak a lesser minor
version. Mismatches fail with an actionable error. See `breadram_core::ipc`.

## Adapter lifecycle

Workload adapters (Ollama, llama.cpp, vLLM, breadsearch, and generic hooks) are
optional and isolated from the policy core. An adapter may be absent, disabled,
or fail at any time without stalling monitoring or disabling core management.
Registration is explicit; there is no network scanning to discover adapters.

## Configuration locations

Configuration is TOML under the user's config directory (the exact schema lands
in RAM-5):

- `~/.config/breadram/breadram.toml` for settings, profiles, and rules.
- Endpoint credentials are stored separately with restricted permissions and
  are redacted everywhere they might be printed.

## Action ownership

Every change breadram makes records the original value, the value it applied,
and who owns the target. breadram restores only values it still owns, and never
overwrites a newer value written by another manager. The action journal is
bounded (see RAM-7).

## Milestone order

See `docs/ISSUE-MAP.md` for the full card list. The order is deliberate:
foundation and standalone operation (M1), then policy and control (M2), then
workload compatibility (M3), then priorities and jobs (M4), then desktop
integration (M5), then verification and delivery (M6). RAM-1 is the start
point; every other card builds on it.
