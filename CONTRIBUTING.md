# Contributing

breadram is part of the bread ecosystem and follows the same branch and review
workflow as every other ecosystem product.

## Branches

There is one long lived branch, `main`. Short lived work branches off `main`
and merges back by pull request:

```
feature/<short-name>
fix/<short-name>
```

One branch and one PR per card. Reference the card's `RAM-<n>` key in the PR
body. Never commit directly to `main`.

## Checks

Every change must pass, from a clean checkout:

```
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace
cargo test --workspace
```

Fix the cause of a failure. Do not weaken an assertion or silence a lint to get
green.

## Conventions

- Rust only. No Go source, anywhere.
- Match the surrounding code's style and structure.
- No AI attribution in commits or PR bodies.
- No em dashes in commits, PRs, docs, or comments. Use a comma, colon, or full
  stop.
- Keep a change scoped to its card. No drive by refactors or reformatting.
- Tests that cannot run without hardware or optional integrations must be
  gated and must report skips, never a silent pass.

## Layout

```
crates/breadram-core   shared contract types, no system or bread dependencies
crates/breadramd       the daemon binary
crates/breadram-cli    the breadram client binary
docs/                  project brief, architecture, issue map, handoff
```
