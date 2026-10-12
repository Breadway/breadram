# breadram command line

`breadram` is a client of the running daemon. It holds no policy of its own: it
asks over the daemon's versioned local socket and prints what came back. The
socket defaults to `$XDG_RUNTIME_DIR/breadram.sock`, and `--socket PATH`
overrides it.

Nothing in this tool needs BOS, breadd, breadsearch, or a model runtime. A
headless host with `breadramd` running is enough, and every command below works
outside the bread ecosystem.

## Commands

| Command | What it answers |
|---|---|
| `breadram status` | Mode, daemon version, observation time, available and total memory, workload count, and the capability report |
| `breadram watch` | Follows status events until interrupted |
| `breadram workloads` | Every workload breadram can see, with its process count, control level, and memory figure |
| `breadram capabilities` | What could be read and controlled on this host, with a reason for anything degraded or unavailable |
| `breadram explain` | What the policy would do right now, with the reasoning for every action and every declined action |
| `breadram rules` | The settings in effect: mode, configuration file, permitted automatic actions, protection, budgets, rules |
| `breadram actions` | The plan the daemon would follow, plus every recorded action with its result and its measured effect |
| `breadram diagnostics` | A full bundle for a bug report, including memory split by source |
| `breadram jobs` | The background job queue: pending and admitted reservations, with the budget |
| `breadram jobs submit --id <id> [--estimate-bytes <n>] [--cwd <dir>] [--env <k=v>]... -- <program> [args]` | Queues an explicitly opted-in background job; runs on launch or the opted-in loop |
| `breadram jobs launch --id <id>` | Admits the oldest pending job and launches it under supervision |
| `breadram jobs cancel --id <id>` | Cancels a queued job, or stops a running one |
| `breadram mode <monitor\|automatic>` | Changes the management mode |
| `breadram protect <scope>...` | Adds workloads that must not be touched automatically |
| `breadram unprotect <scope>...` | Removes protected workloads |

Every command takes `--json`, which prints the daemon's own response payload
rather than a human summary. The JSON field names are the protocol's field
names, so a script parses exactly what the daemon documents and the two cannot
drift apart.

## Reading the output

Figures are reported by source and are never guessed:

- **System RAM**, **swap**, and **shared memory** come from `/proc/meminfo`. A
  field the kernel does not report prints as `unknown`, with the reason in the
  notes; it is never shown as zero.
- **Swap rate** comes from the swap counters in `/proc/vmstat`, and is
  unavailable when the kernel does not report them.
- **VRAM** is device memory and is reported only when a provider or adapter can
  measure it. It is never inferred from host memory. With no adapter installed
  it prints as `unknown` and the note says why.
- **Workload memory** is either exact cgroup accounting, printed with
  `(cgroup)`, or a summed RSS, printed with `(RSS, over-counts shared pages)`.
  An estimate is always labelled, because a shared page is counted in every
  process that maps it.
- **Pressure** is the state the policy is acting on, with the PSI `some` and
  `full` ten second averages when the kernel reports them.
- **Action history** shows the requested effect and the observed effect
  separately, for host memory and device memory separately, and names the
  daemon-owned setting, the value breadram wrote, and the value that was there
  before. A result is one of the action states the coordinator defines
  (`confirmed`, `unconfirmed`, `skipped_duplicate`, `unsupported`, `superseded`,
  `busy`, `denied`, `timed_out`, `cancelled`, `failed`, `rolled_back`), and a
  rollback is never recorded as the action it undid.

## Exit codes

The exit codes are part of the contract.

| Code | Meaning |
|---|---|
| 0 | The command succeeded |
| 1 | Something unexpected went wrong |
| 2 | The command line was invalid, for example an unknown mode |
| 3 | No daemon is listening on the socket |
| 4 | The daemon understood the request and refused it; the message says why |
| 5 | The daemon speaks an incompatible protocol version |

A command that fails prints its reason on stderr and nothing on stdout, so a
script can rely on stdout being the result alone. A closed pipe, as when output
is piped into `head`, is not an error.

## Absent daemon

With no daemon running:

```
$ breadram status
error: no breadram daemon is listening on /run/user/1000/breadram.sock
hint: start breadramd, or pass --socket with the path it is using
$ echo $?
3
```

## Changing settings

`mode`, `protect`, and `unprotect` change the running daemon. By default the
change is in memory only and lasts until the daemon restarts, and the output
says so. With `--persist` the change is also written to the configuration file
the daemon was started with. Validation happens first and the file is written
before the change is applied, so a rejected or unwritable change leaves the
daemon exactly as it was. With no configuration file loaded there is nothing to
write, and a `--persist` request is refused with code 4 rather than being
silently treated as an in-memory change.

A configuration change restarts the pressure state machine, so a new threshold
takes effect immediately instead of after a dwell window. This is deliberate:
the operator asked for it now.

## Completion and help

`breadram --help` lists every command and names the exit codes. `breadram
--version` prints the client version, and `breadram status` prints the daemon
version and the protocol version it speaks, so a mismatch is visible rather than
implied.

## Not implemented yet

`breadram` does not yet have a job-launch interface (`breadram run` or similar).
Background job admission is a separate piece of work, and when it lands it will
appear here with the same conventions: a successful launch reported on stdout,
a refusal on stderr, and exit code 4 for a job that policy declines to admit.

The daemon currently observes and explains only. It records no actions, so
`breadram actions` prints an empty history and says why. The command, the
payloads, and the effect reporting are complete and tested; they will report
real actions once the plan-execute loop runs inside the daemon.
