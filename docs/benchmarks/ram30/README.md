# RAM-30 preliminary measurements

These are development measurements, not release acceptance. Recorded on
Hermes on 2026-10-14 against breadram commit `91e49c2`, debug binaries.
Results are stored here rather than under `/tmp`, so rebooting preserves them.

## Reproduce

```sh
cargo build --locked -p breadramd -p breadram-cli -p cooperative-job
sh scripts/bench/bench-generic.sh
sh scripts/bench/bench-jobs.sh
sh scripts/bench/bench-daemon.sh
sh scripts/bench/test-lib.sh
sh scripts/bench/bench-foreground.sh
sh scripts/bench/bench-llamacpp.sh
sh scripts/bench/bench-standalone.sh
```

The generic and daemon workers allocate 96 MiB. Each scope is bounded by
160 MiB MemoryMax and 96 MiB MemorySwapMax. These hard caps belong to the
test harness, not breadram's generic control policy. The jobs example holds
two private 8 MiB buffers; it is a synthetic cooperative workload, not a
benchmark of arbitrary build tools. No installed provider is stopped or used.

## Results

| Generic case | Rounds / 6 s | Round p95 | Peak memory | Scope stall delta |
| --- | ---: | ---: | ---: | ---: |
| MemoryHigh 144 MiB | 1725 | 4.483 ms | 106,598,400 B | 0 us |
| MemoryHigh 40 MiB | 16 | 490.339 ms | 43,782,144 B | 994,031 us |

The tight run freed approximately 60 MiB of peak resident charge but lost
approximately 99% of throughput. That is a tradeoff, not an efficacy pass.
Round latency measures the memory-touch worker itself; it is **not** an
independent interactive foreground latency measurement.

Major faults are read from `memory.stat`, not `memory.events`. Host swap I/O
deltas come from `/proc/vmstat` and include unrelated host activity: they are
not attributable exclusively to a benchmark scope. Missing late scope
  samples now invalidate the generic run rather than passing as zeros. Scope
  cleanup explicitly stops the named disposable unit on exit or interruption.

The cooperative example averaged 5.1 units per half-second while running,
0.0 while steadily paused. RSS fell from 17,658 KiB to 2,832 KiB. The first
paused sample carries transition work (8 units) and is excluded separately.
The expired-deadline restart produced four running samples and no paused
samples. Busy refusal is explicitly unmeasured (`null`); the script does not
fake a production refusal by rewriting a file itself.

The real daemon had 14,736 KiB final RSS and used 85 CPU jiffies while the
unlimited worker ran for 18 seconds, with no interventions recorded. A second
run with a 16 MiB configured budget recorded one confirmed intervention and
**no rollback**. The reversal gate exited 1. CPU jiffies are raw counters,
not percent utilization; normalize by CLK_TCK and measured wall time before
setting overhead thresholds.

## After RAM-76

RAM-76 merged as `6585f6c8780fafb803bc20c00fd6469c59d87cff`, independently
approved on head `2ec367a` (PR 44 comment 4917), with successful merge CI.
The post-merge debug-build run is preserved in `post-ram76/daemon.jsonl`.
It records one confirmed limit followed by one rollback; the gate exits 0.
The no-action case used 0.90 CPU seconds over 18.083 wall seconds, approximately
4.98% of one core, with 14,904 KiB final RSS. These are short debug-build
measurements, not evidence of an acceptable release overhead threshold.

The harness now normalizes CPU jiffies by `SC_CLK_TCK` and measured wall time,
reports confirmed actions separately from journal events, and fails on CLI
errors, malformed JSON, missing generic samples, and failed workers. Provider
testing is still outstanding. RAM-78 tracks the separate sample-timestamp
jitter follow-up discovered during review.

## Open gates

### Extended single-mill investigation (2026-10-20)

The original serving observation reused the journal from its first idle
unload. Coordinator idempotency (`has_confirmed` for the same workload/action
ID) suppresses further attempts even after demand reload (RAM-84). Extending
that same-journal window alone cannot demonstrate protection: the counter-aware
build stayed ready for 128 seconds, but the legacy control also stayed ready
for 135 seconds without another unload. Both results are preserved under
`long-window/` and are inconclusive for activity safety.

The harness now starts a new private daemon with fresh state before the serving
phase, leaving the provider intact. This removes duplicate suppression from
the experiment without weakening the product guard. It requires 40 successful
queries over at least 120 seconds and counts query errors as failures.
Breadsearch PR 12 has now merged as `cce8e22`; integrated RAM-82 verification
passed using a binary built from that commit: zero busy-mill unloads, one
quiet-mill unload, exit 0, and no leftover units. The independent review of
breadram head `f92dfeb` is PR 50 comment 5257; its merge `75bccaf` has green
CI. The main-built provider result is in `integrated-ram82/contention.jsonl`.
RAM-82 is verified done, independently of the still-open RAM-30 matrix.

The fresh-journal single-mill control reproduces the failure: legacy breadmill,
140 seconds, one unwanted unload, 39 successful queries and one failed query,
exit 1. The counter-capable build completes 40 successful queries over 131
seconds, zero failures and zero unloads, exit 0. See
`long-window/{legacy,counter}-fresh-journal.jsonl`. This removes the duplicate
suppression confound without asserting that RAM-84 has been fixed.

### Reload lifecycle candidate (RAM-84)

With `BREADRAM_BENCH_RELOAD_CYCLE=1`, the contention runner instead waits for
an idle unload, triggers demand reload, stops sending queries, and requires a
second confirmed unload after at least 30 seconds of idle. It keeps the same
provider process, daemon and journal throughout. The RAM-84 candidate
`37289b0` produced two confirmed unloads, demand reload in 1.05 seconds and
33.02 seconds before the second idle unload (`reload-cycle/candidate.jsonl`).
This is pre-review candidate evidence, not an integrated fix claim. The
default contention workload and its protection assertions are unchanged.

The runner now records binary SHA256 values and whether the query counter is
actually present, so an old or replaced provider binary cannot be mistaken
for the fixed build. The first extended run predates those provenance fields.

### Repeated paired CPU inference recovery

Three private-router cycles in `paired-recovery/cpu-{1,2,3}.jsonl` compare
warm inference, unload and reload inference on the same CPU model/config.
Warm latency was 0.123, 0.125 and 0.198 seconds; reload was 0.890, 0.858 and
0.879 seconds, with within-run penalties of 0.768, 0.733 and 0.681 seconds.
Post-reload warm latency was 0.126, 0.129 and 0.115 seconds. This records the
recovery cost explicitly rather than presenting unload as free relief.
These are three host-specific API cycles, not daemon-driven model selection
or a statistically established production latency threshold.

### Whole-device context

`device_memory.py` reads DRM GTT and VRAM used/total counters separately from
host cgroup/RSS samples. Missing, invalid or negative counters remain null;
two unit tests exercise this. The CPU-only llama router run in
`release/llamacpp-device-context.jsonl` includes these counters at each phase.
They describe all workloads on the device, including the live desktop, so no
provider-specific device savings are inferred from them. GTT is shared host
RAM on this iGPU and must not be added to host memory as dedicated VRAM.

### Standalone observation smoke test

`bench-standalone.sh` uses Bubblewrap with private PID/network namespaces and
an isolated home, `/run`, `/etc`, and `/sys`. Only the two release application
binaries are copied into `/app`; `/usr` is read-only for the dynamic loader and
shell. No host ecosystem processes, settings, sockets or systemd manager are
reachable. The daemon starts with a minimal monitor-mode config and the CLI
successfully reads status, workload inventory and capabilities. Raw output is
three consecutive JSON payloads in `release/standalone.json`.

This verifies observation without breadd, breadsearch or a model provider
running. Meminfo, PSI and swap activity remain available; cgroup-v2 control is
honestly unavailable because `/sys` is empty. It does not prove management
efficacy in a clean standalone systemd installation; that gate stays open.

A second attempt used a bounded 512 MiB rootless Podman container from
`docker.io/library/archlinux:base`, no network and no host runtime mounts.
Systemd and its user manager started after container-only runtime/dbus setup,
but a 64 MiB bounded user scope could not move its process into the delegated
cgroup (`Permission denied`, `Input/output error`). Image/config identity and
diagnostics are saved in `standalone-systemd/`. The container was stopped and
removed. This is an environment limitation, not a management pass. No
privileged mode or host security changes were used.

A later service-unit attempt in the same image avoided moving an existing
`podman exec` process into a scope. The user manager launched `bench-worker`
itself with `MemoryMax=128M`, and a private daemon applied
`cgroup_soft_limit`, confirmed it, then rolled `MemoryHigh` back
(`standalone-systemd/actions.json`). The container stayed unprivileged, with
no network and a 512 MiB cap (`standalone-systemd/service-config.txt`), and
was stopped and removed. This is one bounded management cycle without host
bread services. It is not a full provider matrix, a release threshold, or a
substitute for a delegated VM when scope-move delegation is required.

### Private llama.cpp API recovery

`bench-llamacpp.sh` starts its own loopback router in a disposable systemd
service, capped at 1,536 MiB RAM and 256 MiB swap with a 120-second lifetime.
It uses a local 149,080,928-byte LFM2.5-230M Q4_0 model, CPU-only, two threads,
1,024-token context and one slot. Set `BREADRAM_BENCH_MODEL` to override the
local GGUF. It never sends requests to the installed router.

`release/llamacpp-recovery.jsonl` records initial inference at 0.592 seconds,
warm inference at 0.082 seconds, confirmed API unload at 0.104 seconds, and
demand-reload inference at 0.504 seconds. Each completion produced 10 tokens.
The warm request reused cached prompt tokens; cold/reload did not. Cgroup
charge fell from 314,253,312 to 161,456,128 bytes, a 152,797,184-byte reduction.
This includes charged file cache, not just anonymous resident memory. Summed
process RSS fell by 415,625,216 bytes, but can double-count shared mappings
and is not a physical-memory savings claim. Device usage is `null`: CPU-only
configuration does not turn an unmeasured device counter into zero.

This is real model/API recovery, **not** a daemon-driven management or actively
serving protection test. Those follow below. Several-provider contention,
load-dependent recovery thresholds and the vLLM contract remain gates.

### Daemon-driven breadsearch lifecycle

`bench-breadsearch-daemon.sh` runs a disposable breadmill (empty index roots,
isolated runtime/state/config/cache, bounded 2 GiB service) under a private
breadramd in automatic mode whose only permitted action is `unload_idle_model`.
Set `BREADMILL_BIN` to a verbs build; the installed binary predates the unload
verbs and the preflight refuses it fast. The verbs build used here was compiled
from the breadsearch checkout at `bc4b2d9` with `--features breadmill/full`
(the ROAD-15 workaround; CPU stays the runtime backend).

`release/breadsearch-daemon.jsonl` records a daemon-confirmed unload of the
quiet model, RSS falling from 872,140 KiB to 49,348 KiB (822,792 KiB freed),
demand reload completing in 3 s with hits returned, and a 45 s window of
continuous queries during which no second unload was journalled and the model
stayed ready. The idle unload and its efficacy are attributed to the daemon
journal. The 45 s window is **not** evidence of serving protection: see the
contention results below, where a mill queried every 3 s was unloaded once
within a 90 s window (RAM-82). The single-mill window was probably too short to
show it; that has not been established.

### Provider contention (failing gate)

`bench-contention.sh` runs two isolated breadmills under one daemon: mill-a is
queried every 3 s for 90 s, mill-b stays quiet, with bounded hog pressure. The
check counts confirmed unloads per scope from the daemon journal rather than
end-state readiness, because a query reloads an unloaded model on demand and
hides an unload. In 3 of 3 runs (`release/contention-{1,2,3}.jsonl`) the quiet
mill was unloaded by policy as intended, **and the busy mill was also unloaded
once**, so the gate fails. The cause is that the adapter derives idleness only
from polled `in_flight`/`pending`/`loading`, which a millisecond query almost
never overlaps, and breadmill exposes no last-query signal (RAM-82). This is
recorded as a failure, not a pass. An earlier run that reported
`contention_resolved: true` used the old end-state check and is superseded.

### Release-build repetitions

Three consecutive release-build daemon runs (`release/daemon-{1,2,3}.jsonl`)
all recorded one confirmed intervention and one rollback. No-action CPU was
2.098%, 2.101%, and 2.211% of one core; final RSS was 7,008, 7,084, and 6,912
KiB. The intervention runs used 2.206%, 2.480%, and 2.473% of one core. Each
subject ran for 18 seconds. These tests use a two-second tick and artificially
low pressure thresholds, not the installed default configuration.

The first repetition overlapped the explicitly labelled concurrent foreground
probe. After the repetitions completed, `release/foreground-isolated.jsonl`
recorded 400 samples per condition: baseline p95 0.301 ms, pressured p95
0.282 ms, no 20 ms deadline misses in either. This small background workload
did not measurably hurt this probe; it does not demonstrate that management
improves desktop latency.

Provisional regression ceilings for this exact synthetic setup: no-action
release CPU below 3% of one core and final RSS below 10 MiB, zero interventions
in the no-action case, and an observed rollback in each harmful-limit run.
These ceilings leave approximately 35% CPU and 45% RSS headroom above the
largest observed no-action values. The table below is the host-specific
regression record. It does not need another baseline run before review.

`bench-foreground.sh` measures a separate synthetic foreground workload:
scheduled-arrival-to-completion time for a 64 KiB SHA256 job at 20 ms intervals.
It keeps overdue arrivals rather than silently dropping them, and reports
p50/p95/p99, maximum latency and deadline misses. It compares an unloaded
period with a manually throttled background memory worker, not daemon-managed
policy. Do not interpret this as actual desktop frame time or an efficacy pass.
`release/foreground-concurrent.jsonl` was collected while release daemon
repetitions ran concurrently, so it is explicitly not a clean baseline.

## Host-specific regression ceilings

These ceilings are acceptance checks for this Hermes setup and these harnesses.
They are not universal production SLOs and do not claim that managed mode
improves desktop latency. A later run fails the gate when it exceeds a ceiling
or misses a required outcome. Headroom is relative to the largest observed
value in the cited runs.

| Gate | Evidence | Ceiling |
| --- | --- | --- |
| No-action release daemon | `release/daemon-{1,2,3}.jsonl` label `b` | CPU at most 3% of one core, final RSS at most 10 MiB, zero confirmed actions. Observed 2.098-2.211% and 6912-7084 KiB. |
| Harmful soft limit | same files, label `e` | Exactly one confirmed `cgroup_soft_limit` and one rollback. All three runs did this. |
| Generic tight limit | `generic.jsonl` | Record the throughput and stall tradeoff. Do not treat a 99% throughput loss or 994 ms of scope stall as an efficacy pass. |
| Serving protection | `long-window/counter-fresh-journal.jsonl` | At least 40 successful queries, zero failures, zero unloads, model still ready, over at least 120 seconds. |
| Provider contention | `integrated-ram82/contention-strict.jsonl` | Zero busy-mill unloads, at least one quiet-mill unload, 30 successful queries, zero failed queries. |
| CPU recovery penalty | `paired-recovery/cpu-{1,2,3}.jsonl` | Record warm and reload latency. Observed reload penalty 0.681-0.768 seconds on this 149,080,928-byte CPU model. A penalty above 2 seconds on this same setup fails the regression check. This is not a daemon-selected model and not a desktop latency SLO. |
| Foreground probe | `release/foreground-isolated.jsonl` | Zero 20 ms deadline misses. p95 stayed below 0.31 ms in both conditions. This probe does not show a managed-mode improvement. |
| Standalone management | `standalone-systemd/actions.json` | One confirmed soft limit followed by rollback, with no host bread services mounted. |

Action frequency in the no-action runs is zero. In the harmful-limit runs it is
one confirmed action and one rollback per 18-second window, about 3.3 confirmed
actions per minute while the over-budget worker remains. That frequency is
expected for this synthetic budget, not a steady-state production rate.

Swap I/O is measured in `generic.jsonl`: the loose run has zero `pswpin` and
`pswpout` deltas, and the tight run has 573 and 540. Those host counters
include unrelated activity, so they document pressure rather than exclusive
scope attribution. Scope stall, major faults, and throughput are the
scope-local tradeoff.

### Explicit limitations

- Automatic llama.cpp unload is unavailable. Loaded residency is not idle
  evidence (RAM-79). API recovery measurements above are direct router calls,
  not daemon policy efficacy.
- vLLM is deferred by the operator. No vLLM result is simulated or counted.
- Device counters are whole-device context. GTT on this iGPU is shared host
  RAM and is not dedicated VRAM savings.
- The foreground probe is a synthetic SHA256 job. It is not desktop frame time.
- These ceilings are host-specific regression checks. They do not establish a
  statistical production latency threshold or claim that managed mode makes
  interactive work faster.

With those limits stated, the measured gates above are complete enough for
independent review. A repeated noise study would tighten the latency claim,
but its absence does not erase the recorded outcomes.
