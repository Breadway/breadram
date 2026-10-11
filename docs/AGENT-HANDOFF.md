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

RAM-1 (this card) defines the contract and the workspace skeleton. The daemon
and CLI are bootstrap binaries: they parse arguments, print the contract, and
exit. Telemetry (RAM-2), workload identity (RAM-3), lifecycle and IPC (RAM-4),
and configuration (RAM-5) are next.
