# breadram agent handoff

Read this before starting any breadram card. It says where things are, what the
rules are, and how to verify work.

## Read order

1. `docs/PROJECT.md` for the product contract (modes, capability reporting,
   privilege boundary, configuration locations, action ownership).
2. `docs/ARCHITECTURE.md` for the decisions the implementation must honour.
3. `docs/ISSUE-MAP.md` for the card list, dependencies, and workflow.
4. `CONTRIBUTING.md` for the branch model and checks.

## Where the work lives

- Tracker: breadboard project `RAM` at https://breadboard.breadway.dev.
- Repository: `Breadway/breadram`, cloned at `~/Projects/breadram`.
- Start point: RAM-1. Do not start a card whose blockers are unverified unless
  a documented interface lets it stand alone.

## Rules

- Branches: `feature/<short-name>` or `fix/<short-name>` off `main`. One PR per
  card, referenced by its `RAM-<n>` key. Never commit to `main`.
- Move a card to `doing` when work starts, `review` when it is implemented and
  verified, and `done` only after verification.
- No AI attribution in commits or PR bodies (no `Co-Authored-By`, no
  `Generated with`).
- No em dashes in anything authored. Use a comma, colon, or full stop.
- No Go source. breadram is Rust.
- Do not invent scope. Implement the card, and nothing else.

## Verification

From a clean checkout, before claiming a card is done:

```
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace
cargo test --workspace
```

Any card that touches system behaviour must also prove the standalone path with
no optional integration present. Tests that need hardware breadram cannot
assume must be gated and must report skips honestly.

## Current state

The daemon and the CLI are real now. Telemetry, workload inventory, validated
configuration, the versioned IPC surface, pressure policy, host controls, the
action coordinator, serving coordination, the Ollama adapter, and the
daemon's plan-execute step have landed. In `monitor` mode the daemon observes,
explains, and records nothing. In `automatic` mode it runs the plan on each pass:
it revalidates each action against the live inventory, applies it through systemd
(never by rewriting a hierarchy itself), confirms the effect, measures what
changed, journals it, and remembers the values it owns in a versioned state file
so a restart can restore them.

A provider action can now be carried out as well as planned. `adapter::HttpTransport`
is the real bounded loopback transport, `adapter::AdapterExecutor` implements the
coordinator's `Executor` boundary for the provider-backed actions, and
`ChainedExecutor` runs host controls and provider actions in one pass. The daemon
reads every configured endpoint once per tick before policy runs, so a decision
is never made on a stale provider reading. This was verified over a real socket
against an in-process fake provider: an idle release is planned, performed,
confirmed, and journalled with a measured effect, a busy backend is never written
to, an unreachable provider is reported, and an unconfirmable release is
journalled as unconfirmed rather than as success.

What is not there yet, named rather than implied:

- The release path is planned-never-executed until request tracking is declared.
  With `TrackingCompleteness::Unknown`, a resident model reports `Active`, so
  nothing is planned against it. That is the conservative direction and it is
  correct; the gateway that reports real requests is RAM-45, and a deployment
  that declares complete tracking is what makes a resident but unused model
  releasable. Nothing infers it.
- The Ollama adapter is the only one that can act on a single-model server. The
  llama.cpp adapter (RAM-13) observes servers and implements the documented
  router unload API, but daemon policy cannot automatically unload a loaded
  model because residency does not prove idleness. The low-level operation is
  confirmed by the exact model id and its status in the same listing when it
  is exercised directly; verified on build 11146 with Qwen3 8B with inference
  after relief and separately measured host vs device effects
  (`docs/LLAMACPP.md` records the evidence and the missing idle gate).
  Single-model immediate manual sleep is not documented. The
  generic hook adapter (RAM-15) also exists and runs under the safeguards in
  `docs/HOOKS.md`. vLLM (RAM-14),
  breadsearch (RAM-18), and the workload mapping for containers and services
  (RAM-16) are open. The transport speaks loopback `http` only: `https` is
  refused with a reason rather than sent in the clear.
- Background jobs (RAM-17), priorities and budgets beyond the per scope budget
  (RAM-21), swap and OOM diagnostics (RAM-23), trends (RAM-28), and the shared
  test fixtures (RAM-29) are still open.
- The shared test fixtures RAM-29 asks for (deterministic replay inputs and an
  isolated integration harness) do not exist yet, so each suite still builds its
  own. Tests do clean up after themselves now: every temporary directory and
  socket is removed when the test that made it ends, and the whole suite leaves
  nothing behind in `/tmp`.
- Swap, compression, and OOM management are reported rather than managed: the
  layout, the effective per workload swap limit, and the kernel's OOM kill count
  are in `diagnostics`, with the constraints explained in notes. Nothing global
  is ever changed, and no second OOM killer is installed.

Read `docs/ADAPTER-CONTRACT.md` before touching an adapter, `docs/CLI.md` before
touching the command surface, `docs/SERVING-COORDINATION.md` before touching
leases or the sleep and wake lifecycle, and `docs/SWAP-AND-OOM.md` before
touching swap, compression, or OOM diagnostics.
