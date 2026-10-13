# Background admission and supervised launch (RAM-24)
`AdmissionQueue` accepts explicitly opted-in background job IDs and conservative
upper-bound byte estimates. It exposes pending metadata and cancellation, bounds
pending plus admitted metadata, and atomically reserves against a configured total
admission budget. Unknown capacity or unhealthy pressure stops new admission.
Admitted allocations are not subtracted twice from MemAvailable: callers supply a
total budget rather than treating currently free memory as that budget.

FIFO is the fairness rule. Younger small jobs cannot indefinitely overtake older
large ones: only the head of the queue launches. A job that never fits must be
cancelled or given a larger operator budget; aging never overrides resource
safety. Observed underestimation raises the reservation and blocks further
launches. Exit or failed launch releases it.

`PersistedAdmissions` is the durable bounded record. It captures the current
admissions (bounded by `MAX_PERSISTED_ADMISSIONS`), round-trips through JSON, and
never claims recovery. A restart cannot prove a recorded process is still the one
it started, so the record reports the ids a restart refuses to reuse rather than
trusting them, which is the conservative direction the card asks for.

`[jobs] total_budget_bytes` is the conservative total admission budget the
daemon admits against. It defaults to 8 GiB and must be greater than 0 when
`[jobs] enabled` is true. It is a configured total, not MemAvailable, so
admitted reservations are not subtracted twice from free memory.

The daemon owns one `AdmissionQueue` sized by `[jobs] max_queued`. `breadram
jobs` inspects it, `breadram jobs submit` queues an explicitly opted-in job
(a zero estimate means the configured default), and `breadram jobs cancel`
removes a pending job or stops a running one. A disabled queue reports itself
and refuses submits. Submission, launch, and cancellation travel over the
versioned IPC (protocol minor 8), so the CLI reads exactly what the daemon
holds.

Every queue mutation is remembered beside the journal state file as
`jobs.json`. A restart never re-admits the record: the ids a previous run held
are reported as refused in the jobs report, because this run cannot prove a
recorded process is still its own. An empty queue removes the record instead of
writing one, so a missing file stays the ordinary first run.

The report states admission readiness honestly: ready only with an observation
in hand and healthy pressure. Unknown or unhealthy pressure holds admission,
with the reason named, which is the same rule `admit` enforces.

## Supervised launch

`breadram jobs launch` admits the head of the queue and runs it at once.
Launches arrive explicitly or from the opted-in automatic loop; either way the
daemon supervises exactly what an operator asked it to run, and only the
oldest pending job launches, so a younger job never overtakes an older one.

A launched job runs in a transient user scope named `breadram-job-<id>`, which
bounds memory at the reservation and cleans up when it empties. The program
runs directly with no shell: the first submit argument must be an absolute
path, and each argument is passed verbatim. Standard streams are discarded, so
job output never enters the queue, the journal, or a report. The environment is
inherited with the submitted KEY=VALUE entries applied over it; arguments,
directories, and environment live in daemon memory only and are never
persisted, journalled, or reported. Cancelling a running job stops its scope,
first gently then forcefully, and the finish is recorded with the real exit
status and the peak memory observed while it ran.

A restart drops pending submissions, whose launch specifications are memory
only. Admitted records come back as refusals, never resumed: a new run cannot
prove a recorded process is still its own, and the ids stay refused until other
queue activity rewrites the record. Scopes outliving a violently killed daemon
are left alone: their ids are refused and a launch collides with a lingering
scope by name, so they cannot be mistaken for new work, and they vanish with
their programs. A clean shutdown stops every supervised job first, so nothing
the daemon admitted keeps running unmanaged.

Launch needs a systemd user session. Without `systemd-run` or
`XDG_RUNTIME_DIR` it is refused plainly rather than run half supervised.

## Cooperative concurrency (RAM-25)

A supervised job is started with `BREADRAM_WORKERS_FILE` pointing at
`<state dir>/concurrency/<id>.workers`. The file holds one line:

- `run <n>`: keep n workers
- `reduce <n>`: drop to n workers at the next safe point
- `pause <n> <resume-unix-ms>`: take no new work until that absolute time,
  then run n workers again

breadram writes `run 1` at launch and deletes the file when the job finishes,
so a reused id cannot inherit a stale instruction. Under pressure the daemon
reduces a running job through the `ReduceJobConcurrency` action, which halves
the file's count toward a floor of one; the plan promises half the usage as
relief, matching what the executor writes. A job already at `reduce 1` that
is still over budget is paused instead, for the cap duration and reported as
the escalation it is: bounded, self-resuming, and never a freeze. A reduce
inside an open pause is refused as busy, and a second pause inside an open
deadline is refused the same way: the first deadline stands. A pause the
daemon writes is at most 30 seconds (`MAX_PAUSE`); anything longer is clamped
and reported. The deadline
is in the file, so the job resumes when it passes even if the daemon has
exited and the file still says `pause`. Every pass also resumes an expired
pause by writing `run`, so the file stops saying pause once the deadline is
past. A missing or garbled file means keep one worker going: it is not a
stop and it is not a pause. Estimates stay one chain: the caller estimate, or
the configured default when none is given, becomes the admission reservation,
which becomes the scope's `MemoryMax`; a reduce is expected to free half the
job's usage from it.

This is the only concurrency control. An arbitrary program that never reads
the file is not claimed to support a changed worker count, and breadram does
not freeze it. Pausing does not release memory the process already holds
unless the job itself frees it at the safe point.

The example is `examples/cooperative-job`. Each worker hashes a private 8 MiB
buffer and counts rounds. On `pause` it joins those workers, which frees the
buffers, and after the deadline it starts them again. `scripts/measure-cooperative.sh`
records units per half second and RSS across run, pause, and resume, refuses a
second pause inside an open deadline, and starts a fresh process against an
already-expired pause with no writer left to clear it.

Measured on this host (debug build, two workers, 8 s, then a 2.5 s pause):
average delta 13.1 units per half second while running two workers, 0.0 while
paused at steady state; RSS about 17742 KiB running and 2916 KiB paused, and
16196 KiB with 2860 KiB paused on a second run. The restart process reported
`paused: false` for every sample. The first paused sample still carries up to
half a second of rounds finished before the workers joined (16.0 on both
runs); it is reported separately and left out of the paused average, because
it mixes run work with pause work rather than measuring either. A job that
exits during a pause is a normal exit; the daemon removes the file in
`finish_job`, same as any other finish, which the daemon suite covers with a
paused directive on disk at finish time.

## Automatic admission

With `[jobs] auto_launch` set, the monitor loop launches the queue head by
itself in automatic mode: at most one launch per pass, through the same gates
as an explicit launch (enabled, supervision available, valid paths, no stale
scope, healthy pressure, fitting budget). Routine holds stay at debug level.
Off by default: launching stays explicit unless an operator opts in, and the
flag is inert without `[jobs] enabled` or outside automatic mode, where the
daemon never writes.

Submission, supervised launch, cancellation, automatic admission when opted
in, durability as a refuse-to-reuse record, pressure-aware readiness, and real
exit reporting all run through the daemon and the CLI. Wider verification
under sustained pressure and competing providers is deployment work beyond any
single branch.
