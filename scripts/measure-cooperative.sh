#!/bin/sh
# Measure examples/cooperative-job across run, pause, resume, and a deadline
# that outlives the writer (the restart case: nobody clears the file).
# A second pause while the first is still inside its deadline is refused.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
bin=${1:-$root/target/debug/cooperative-job}
if [ ! -x "$bin" ]; then
    echo "build the example first: cargo build -p cooperative-job" >&2
    exit 1
fi
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
file=$dir/job.workers
now_ms() { date +%s%3N; }

# Refuse to replace a pause that has not reached its deadline.
write_pause() {
    # A separate name: this function shares the caller's shell, so writing
    # `deadline` here would clobber the timestamp the caller just recorded.
    want=$1
    if [ -f "$file" ]; then
        set -- $(head -n 1 "$file")
        if [ "${1:-}" = "pause" ] && [ "${3:-0}" -gt "$(now_ms)" ]; then
            echo "busy refusal: pause until $3 still holds" >&2
            return 1
        fi
    fi
    printf 'pause 2 %s\n' "$want" > "$file"
}

printf 'run 2\n' > "$file"
BREADRAM_WORKERS_FILE=$file BREADRAM_SECONDS=8 "$bin" > "$dir/out" &
pid=$!
sleep 2
write_pause "$(( $(now_ms) + 2500 ))"
# The first deadline must still be the one on disk after a refused second pause.
deadline=$(awk '{print $3}' "$file")
if write_pause "$(( $(now_ms) + 60000 ))"; then
    echo "a second pause was accepted inside the deadline" >&2
    kill "$pid" 2>/dev/null || true
    exit 1
fi
got=$(awk '{print $3}' "$file")
if [ "$got" != "$deadline" ]; then
    echo "pause file changed: $got != $deadline" >&2
    kill "$pid" 2>/dev/null || true
    exit 1
fi
wait "$pid" || true

# Restart case: the file still says pause, and no writer is around to clear
# it. A new process must resume on its own once the deadline passes.
old=$(( $(now_ms) - 1000 ))
printf 'pause 2 %s\n' "$old" > "$file"
BREADRAM_WORKERS_FILE=$file BREADRAM_SECONDS=2 "$bin" > "$dir/restart" &
wait $!
if grep -q '"paused":true' "$dir/restart"; then
    echo "an expired pause was still honored" >&2
    cat "$dir/restart" >&2
    exit 1
fi
if ! grep -q '"paused":false' "$dir/restart"; then
    echo "restart produced no running sample" >&2
    exit 1
fi

python3 - "$dir/out" "$dir/restart" <<'PY'
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
restart = [json.loads(l) for l in open(sys.argv[2]) if l.strip()]
run = [r for r in rows if not r["paused"] and r["workers"] == 2]
# The first samples are still the initial run; later unpaused samples are the resume.
paused = [r for r in rows if r["paused"]]
# The first paused sample straddles the transition: up to half a second of
# work finished before the workers joined counts into it. Steady state is
# what the pause sustains, so that sample is reported but not averaged.
transition, paused = paused[:1], paused[1:]
def avg(xs, key):
    xs = [x[key] for x in xs]
    return sum(xs) / len(xs) if xs else 0
print(
    f"samples={len(rows)} run2={len(run)} paused={len(paused)} "
    f"transition_delta={avg(transition,'delta'):.1f} "
    f"avg_delta_run={avg(run,'delta'):.1f} avg_delta_pause={avg(paused,'delta'):.1f} "
    f"rss_run_kib={avg(run,'rss_kib'):.0f} rss_pause_kib={avg(paused,'rss_kib'):.0f} "
    f"restart_paused={sum(1 for r in restart if r['paused'])}",
    file=sys.stderr,
)
if not run or not paused:
    sys.exit("missing run or pause samples")
if avg(paused, "delta") > max(1.0, avg(run, "delta") * 0.25):
    sys.exit("pause did not cut throughput")
if avg(run, "rss_kib") < avg(paused, "rss_kib"):
    sys.exit("pause did not drop rss")
PY
echo "ok" >&2
