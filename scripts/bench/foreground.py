"""Independent foreground scheduling/compute probe, no provider dependencies.

Measures scheduled-arrival-to-completion latency for a small SHA256 job.
Missed intervals remain in the queue: the probe does not hide stalls by
starting its next interval only after the previous job completes. Keep this
outside the memory-limited background scope. This is a synthetic interactive
proxy, not a claim about actual desktop frame times.
"""

import argparse
import hashlib
import json
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=float, default=18)
    parser.add_argument("--interval-ms", type=float, default=20)
    args = parser.parse_args()
    if not 0 < args.seconds <= 120 or not 1 <= args.interval_ms <= 1000:
        parser.error("seconds must be in (0,120], interval-ms in [1,1000]")
    interval = args.interval_ms / 1000
    count = max(1, int(args.seconds / interval))
    payload = bytes(64 * 1024)
    start = time.monotonic()
    samples = []
    for index in range(count):
        due = start + index * interval
        time.sleep(max(0, due - time.monotonic()))
        hashlib.sha256(payload).digest()
        samples.append((time.monotonic() - due) * 1000)
    ordered = sorted(samples)
    print(json.dumps({
        "scenario": "foreground",
        "samples": count,
        "interval_ms": args.interval_ms,
        "wall_s": time.monotonic() - start,
        "p50_ms": ordered[len(ordered) // 2],
        "p95_ms": ordered[min(len(ordered) - 1, int(len(ordered) * .95))],
        "p99_ms": ordered[min(len(ordered) - 1, int(len(ordered) * .99))],
        "max_ms": max(samples),
        "deadline_misses": sum(value > args.interval_ms for value in samples),
    }))


if __name__ == "__main__":
    main()
