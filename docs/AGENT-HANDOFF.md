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

What is not there yet, named rather than implied:

- No provider action is executed. A configured provider is attached and its
  activity evidence reaches policy, but nothing can call it: there is no HTTP
  transport in `breadram_core::adapter` yet, only `ScriptedTransport` for tests.
  That plus the workload mapping is RAM-13, RAM-14, RAM-16, and RAM-45.
- Background jobs (RAM-17), priorities and budgets beyond the per scope budget
  (RAM-21), swap and OOM diagnostics (RAM-23), trends (RAM-28), and the shared
  test fixtures (RAM-29) are still open.
- Tests that spawn the daemon leave their temporary directories behind in `/tmp`.
  Cleaning that up belongs to RAM-29.

Read `docs/ADAPTER-CONTRACT.md` before touching an adapter, `docs/CLI.md` before
touching the command surface, and `docs/SERVING-COORDINATION.md` before touching
leases or the sleep and wake lifecycle.
