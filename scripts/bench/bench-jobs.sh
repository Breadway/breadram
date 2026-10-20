#!/bin/sh
# Scenario C: cooperative job throughput running versus paused.
#
# Drives examples/cooperative-job for 8 s with a 2.5 s pause in the middle,
# exactly like measure-cooperative.sh, but always reports JSON instead of
# gating: the gate belongs to the RAM-25 card, this records the numbers for
# the benchmark table. Busy refusal belongs to production control tests;
# this script only measures pause throughput and deadline recovery.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
repo=$root/../..
bin=$repo/target/debug/cooperative-job
if [ ! -x "$bin" ]; then
    echo "skipped: build the example first (cargo build -p cooperative-job)" >&2
    exit 0
fi
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
file=$dir/job.workers
now_ms() { date +%s%3N; }

printf 'run 2\n' > "$file"
BREADRAM_WORKERS_FILE=$file BREADRAM_SECONDS=8 "$bin" > "$dir/out" &
pid=$!
sleep 2
deadline=$(( $(now_ms) + 2500 ))
printf 'pause 2 %s\n' "$deadline" > "$file"
sleep 0.2
wait "$pid" || true

# Restart case: expired pause, no writer. Must report paused:false always.
old=$(( $(now_ms) - 1000 ))
printf 'pause 2 %s\n' "$old" > "$file"
BREADRAM_WORKERS_FILE=$file BREADRAM_SECONDS=2 "$bin" > "$dir/restart" &
wait $!

python3 - "$dir/out" "$dir/restart" <<'PY'
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
restart = [json.loads(l) for l in open(sys.argv[2]) if l.strip()]
run = [r for r in rows if not r["paused"] and r["workers"] == 2]
paused_all = [r for r in rows if r["paused"]]
paused = paused_all[1:]
transition = paused_all[:1]
def avg(xs, key):
    xs = [x[key] for x in xs]
    return sum(xs) / len(xs) if xs else 0
result = {
    "scenario": "jobs",
    "samples": len(rows),
    "run_samples": len(run),
    "paused_samples": len(paused),
    "avg_delta_run": round(avg(run, "delta"), 1),
    "avg_delta_paused": round(avg(paused, "delta"), 1),
    "transition_delta": round(avg(transition, "delta"), 1),
    "rss_run_kib": round(avg(run, "rss_kib")),
    "rss_paused_kib": round(avg(paused, "rss_kib")),
    "second_pause_refused": None,
    "restart_paused_samples": sum(1 for r in restart if r["paused"]),
    "restart_total_samples": len(restart),
}
print(json.dumps(result))
PY
