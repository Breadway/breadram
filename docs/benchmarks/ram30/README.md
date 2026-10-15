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
largest observed no-action values. They are host-specific regression checks,
not universal release acceptance. Independent foreground and provider gates
need repeated paired unmanaged/managed baselines before thresholds are set.

`bench-foreground.sh` measures a separate synthetic foreground workload:
scheduled-arrival-to-completion time for a 64 KiB SHA256 job at 20 ms intervals.
It keeps overdue arrivals rather than silently dropping them, and reports
p50/p95/p99, maximum latency and deadline misses. It compares an unloaded
period with a manually throttled background memory worker, not daemon-managed
policy. Do not interpret this as actual desktop frame time or an efficacy pass.
`release/foreground-concurrent.jsonl` was collected while release daemon
repetitions ran concurrently, so it is explicitly not a clean baseline.

- Establish repeated release-build overhead and latency baselines.
- Provider recovery, actively serving and competing model servers, independent
  foreground latency, separate device-memory sampling, repeated-run noise
  analysis, and a genuinely standalone installation remain unmeasured here.
- RAM-14/RAM-19 remain prerequisites for the complete supported-provider
  matrix. Unsupported vLLM is a limitation, never a simulated pass.
- Release thresholds have not been established from these preliminary runs.

RAM-30 must remain incomplete until those gates are verified. The successful
shell runs are not evidence that breadram's managed mode improves latency.
