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
samples currently fall back to zero, which is a harness limitation to fix
before release gating.

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

## Open gates

- RAM-76: fixed absolute stall-delta threshold depends on monitor interval;
  only the immediately following tick is eligible for reversal.
- Provider recovery, actively serving and competing model servers, independent
  foreground latency, separate device-memory sampling, repeated-run noise
  analysis, and a genuinely standalone installation remain unmeasured here.
- RAM-14/RAM-19 remain prerequisites for the complete supported-provider
  matrix. Unsupported vLLM is a limitation, never a simulated pass.
- Release thresholds have not been established from these preliminary runs.

RAM-30 must remain incomplete until those gates are verified. The successful
shell runs are not evidence that breadram's managed mode improves latency.
