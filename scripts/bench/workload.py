"""A measurable memory workload for benchmarks.

Touches every page of a fixed buffer in a loop and prints one JSON line per
round with its wall time in milliseconds, plus a final summary line. Used
only inside disposable systemd scopes with a hard MemoryMax, so a runaway
cannot hurt the host.

The per-round timings are the foreground latency signal: a throttled run
shows the same rounds taking longer, and percentiles come from the lines,
not from a second measurement pass.
"""

import json
import sys
import time


def main() -> None:
    mb = int(sys.argv[1])
    secs = float(sys.argv[2])
    buf = bytearray(mb * 1024 * 1024)
    step = 4096
    end = time.monotonic() + secs
    rounds = 0
    latencies = []
    while time.monotonic() < end:
        start = time.monotonic()
        total = 0
        for i in range(0, len(buf), step):
            buf[i] = (buf[i] + 1) & 255
            total += buf[i]
        # Keep the read live so a run cannot delete the loop.
        if total < 0:
            break
        elapsed_ms = (time.monotonic() - start) * 1000.0
        latencies.append(elapsed_ms)
        rounds += 1
        print(json.dumps({"round": rounds, "ms": round(elapsed_ms, 3)}), flush=True)
    latencies.sort()
    print(
        json.dumps(
            {
                "summary": True,
                "rounds": rounds,
                "p50_ms": round(latencies[len(latencies) // 2], 3) if latencies else 0,
                "p95_ms": round(
                    latencies[min(len(latencies) - 1, int(len(latencies) * 0.95))], 3
                )
                if latencies
                else 0,
            }
        ),
        flush=True,
    )


main()
