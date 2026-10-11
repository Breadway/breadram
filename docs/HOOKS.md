# breadram hook protocol

A hook is a cooperative bridge for a runtime breadram has no built-in adapter
for: a Python training job, a renderer, a cache-heavy service. The operator
configures a program, breadram runs it with a verb as its final argument, and
the program answers with one JSON document on standard output.

## Configuration

```toml
[[providers]]
name = "trainer"
kind = "hook"
url = "hook:trainer"
scope = "unit:trainer.service"
command = ["/usr/local/bin/breadram-hook-trainer"]
hook_timeout_ms = 5000
allow_automation = true
```

The fields:

- `url` is the hook identity, always `hook:<name>`. It is what ties status
  reads, release operations, and wake operations to this provider, and it is
  the prefix every hook operation URL must sit under.
- `command` is the program and its arguments, run with no shell. The first
  entry is an absolute path. Nothing else in the configuration, and nothing
  any endpoint answers, can change what runs.
- `hook_timeout_ms` (default 5000, between 100 and 30000) is the deadline for
  one invocation. A hook that outlives it is killed.
- `hook_max_output_bytes` (default 65536, between 4096 and 1048576) caps how
  much output is accepted. Anything longer is refused.
- `allow_automation` decides whether the hook is only observed or may also be
  asked to release and wake. Off is the default.

A hook provider without `allow_automation` still contributes inventory and
activity evidence, so its workload gets visibility and the generic cgroup
controls, with the clear limitation that breadram will never ask it to
release anything.

## Verbs

breadram appends exactly one argument to the configured command:

| Invocation | Meaning |
| ---------- | ------- |
| `<command...> status` | Report whether the workload is busy and what it holds. |
| `<command...> release` | Drop disposable memory. Must be safe to call when idle. |
| `<command...> wake` | Bring back what a release dropped. |

The answers:

```json
{"busy": false, "host_ram_bytes": 1073741824, "note": "epoch checkpoint done"}
{"released": true, "note": "dropped the preprocessed cache"}
{"awake": true}
```

- `status` requires `busy`. The byte counts are optional but recommended:
  without them the release effect is journalled as unmeasured rather than
  wrong. `note` is a human sentence, truncated to 300 characters in reports.
- `release` requires `released`. `false` with a note means the hook declined
  (a checkpoint is in progress, a render is running); the denial is
  journalled with the hook's own words. A release is only attempted when the
  hook has proven idleness, and it is confirmed by a later `status` that must
  report not busy.
- `wake` requires `awake` and runs when breadram undoes a release.

Anything else is refused: a nonzero exit, an answer over the cap, invalid
UTF-8, trailing garbage after the document, or a missing required field. A
failed read proves nothing, so the previous reading is discarded rather than
acted on.

## Execution safeguards

Every invocation, including the read-only `status`, runs under the same
rules:

- The program must be a regular file, executable, without setuid or setgid
  bits, and writable by nobody but its owner. On Linux it must be owned by
  the caller or by root. The check runs on every invocation, so a program
  swapped after configuration is still refused.
- The environment is cleared except `PATH=/usr/bin:/bin`, `LANG=C.UTF-8`,
  and `BREADRAM_HOOK=1`. No credential is passed; hooks that need one must
  read their own files.
- Standard input is closed. Standard output beyond the cap is refused, and
  only a short prefix of standard error is ever kept for failure messages.
- Cancellation is cooperative between actions: the coordinator never starts
  a new action after cancellation is requested, and a hook already in flight
  runs to its deadline.

## Writing a hook

The minimal example is `examples/hook-example.py`: a Python program (any
language works, as long as it is not Go in this repository) implementing all
three verbs over a small state file. Copy it, point `command` at the copy,
and keep the failure modes honest: report `busy: true` while work is in
flight, decline a release with `released: false` and a reason rather than
pretending, and never report idle from an unfocused window or a quiet
cgroup. Being loaded is not being idle.

To add a natively integrated runtime instead, implement
`breadram_core::adapter::WorkloadAdapter` following `docs/ADAPTER-CONTRACT.md`;
the Ollama adapter is the reference. Hooks are the fast path that needs no
Rust change.
