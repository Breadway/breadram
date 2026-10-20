#!/bin/sh
# Scenarios B and E: a real breadramd supervising disposable scopes.
#
# B (overhead): the daemon runs in automatic mode with no providers while a
# worker runs unmanaged. Reports daemon RSS and CPU consumed across the run
# plus how many actions it journalled (expected: none, nothing to do).
#
# E (ineffective intervention): the same daemon with a tight budget on the
# worker's scope. Reports whether the journal contains a rollback rather
# than assuming the stall watch reverses it. Missing reversal fails the gate.
#
# The daemon gets a private state dir, socket, and config, so the live
# system (if any) is untouched. Everything is killed on exit.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
. "$root/lib.sh"
require_session
if [ ! -x "$root/../../target/debug/breadramd" ] || [ ! -x "$root/../../target/debug/breadram" ]; then
    echo "skipped: build breadramd and breadram first (cargo build -p breadramd -p breadram-cli)" >&2
    exit 0
fi
daemon=${BREADRAM_BENCH_DAEMON:-$root/../../target/debug/breadramd}
cli=${BREADRAM_BENCH_CLI:-$root/../../target/debug/breadram}

start_daemon() {
    # $1 = state dir (created by caller), $2 = budget bytes or empty
    dir=$1
    budget=$2
    socket=$dir/breadram.sock
    config=$dir/breadram.toml
    # Hair-trigger pressure thresholds: the benchmark host is otherwise
    # healthy, and manufacturing real host pressure would endanger it. Any
    # stall the worker itself causes counts as pressure, which is exactly
    # the loop under test (limit bites, stalls rise, limit reverses).
    {
        echo 'mode = "automatic"'
        echo '[automation]'
        echo 'allowed_actions = ["cgroup_soft_limit"]'
        echo '[pressure]'
        echo 'pressured_some_avg10 = 0.01'
        echo 'critical_some_avg10 = 50.0'
        echo 'critical_available_fraction = 0.01'
        echo '[timing]'
        echo 'settle_ms = 2000'
        echo 'min_dwell_ms = 4000'
        echo 'cooldown_ms = 8000'
        if [ -n "$budget" ]; then
            echo '[[budgets]]'
            echo "scope = \"unit:$unit\""
            echo "max_bytes = $budget"
        fi
    } >"$config"
    XDG_STATE_HOME=$dir "$daemon" --config "$config" --socket "$socket" >"$dir/daemon.log" 2>&1 &
    echo $! >"$dir/daemon.pid"
    # Wait for the socket, bounded.
    for _ in $(seq 1 50); do
        [ -S "$socket" ] && break
        sleep 0.1
    done
    if [ ! -S "$socket" ]; then
        echo "daemon failed to start, log:" >&2
        tail -5 "$dir/daemon.log" >&2
        return 1
    fi
}

stop_daemon() {
    # $1 = state dir
    if [ -f "$1/daemon.pid" ]; then
        kill "$(cat "$1/daemon.pid")" 2>/dev/null || true
        wait "$(cat "$1/daemon.pid")" 2>/dev/null || true
    fi
}

daemon_rss_cpu() {
    # $1 = pid file. Prints rss_kb and total jiffies, one per line.
    pid=$(cat "$1/daemon.pid" 2>/dev/null || echo 0)
    echo "rss_kb=$(rss_kib "$pid")"
    echo "jiffies=$(cpu_jiffies "$pid")"
}

run_scenario() {
    label=$1
    budget=$2
    unit="breadram-bench-${label}-$$.scope"
    dir=$(mktemp -d)
    hog_unit="breadram-bench-hog-${label}-$$.scope"
    cleanup() {
        for stopping in "$unit" "$hog_unit"; do
            systemctl --user stop "$stopping" >/dev/null 2>&1 || true
        done
        stop_daemon "$dir"
        rm -f "${out:-}" "${out:-}.err"
        rm -rf "$dir"
    }
    trap cleanup EXIT
    trap 'exit 130' INT TERM
    export unit
    start_daemon "$dir" "$budget" || return 1
    socket=$dir/breadram.sock
    eval "$(daemon_rss_cpu "$dir" | sed 's/^/d0_/')"
    out=$(mktemp)
    # The E run adds a sacrificial hog under its own tight harness: it faults
    # constantly, manufacturing steady bounded host pressure so the healthy
    # host plans anything at all. The subject itself starts unlimited with a
    # near-zero stall baseline; the daemon's own tight write is what makes
    # its accumulation spike, and the reversal fires on that rise.
    hog=""; [ -n "$budget" ] && hog="yes"
    if [ -n "$hog" ]; then
        timeout 30 systemd-run --user --scope --quiet --collect \
            --unit="$hog_unit" \
            -p MemoryMax=160M \
            -p MemorySwapMax=96M \
            -p MemoryHigh=40M \
            -- python3 "$root/workload.py" 96 24 >/dev/null 2>&1 &
        hogpid=$!
        sleep 3
    fi
    started_ns=$(date +%s%N)
    timeout 26 systemd-run --user --scope --quiet --collect \
        --unit="$unit" \
        -p MemoryMax=160M \
        -p MemorySwapMax=96M \
        -- python3 "$root/workload.py" 96 18 >"$out" 2>"$out.err" &
    pid=$!
    if [ -n "$hog" ]; then
        set +e
        wait "$pid"
        status=$?
        systemctl --user stop "$hog_unit" >/dev/null 2>&1
        kill "$hogpid" 2>/dev/null
        wait "$hogpid" 2>/dev/null
        set -e
        hogpid=""
    else
        set +e
        wait "$pid"
        status=$?
        set -e
    fi
    eval "$(daemon_rss_cpu "$dir" | sed 's/^/d1_/')"
    ended_ns=$(date +%s%N)
    eval "$(summarize_rounds "$out")"
    journal=$("$cli" --socket "$socket" actions --json)
    rm -f "$out" "$out.err"
    cleanup
    trap - EXIT INT TERM
    python3 - "$label" "$budget" "$status" "$rounds" "$p50_ms" "$p95_ms" \
        "$d0_rss_kb" "$d1_rss_kb" "$d0_jiffies" "$d1_jiffies" "$journal" \
        "$started_ns" "$ended_ns" <<'PY'
import json, os, sys
(label, budget, status, rounds, p50, p95, r0, r1, j0, j1, journal, start, end) = sys.argv[1:]
journal = json.loads(journal)
wall_s = (int(end) - int(start)) / 1e9
cpu_s = (int(j1) - int(j0)) / os.sysconf("SC_CLK_TCK")
history = (journal.get("history") if isinstance(journal, dict) else []) or []
states = {}
for entry in history:
    states[entry.get("state", "?")] = states.get(entry.get("state", "?"), 0) + 1
result = {
    "scenario": "daemon",
    "label": label,
    "budget_bytes": int(budget or 0),
    "exit": int(status),
    "rounds": int(rounds),
    "p50_ms": float(p50),
    "p95_ms": float(p95),
    "daemon_rss_start_kb": int(r0),
    "daemon_rss_end_kb": int(r1),
    "daemon_jiffies": int(j1 or 0) - int(j0 or 0),
    "measured_wall_s": round(wall_s, 6),
    "daemon_cpu_s": round(cpu_s, 6),
    "daemon_cpu_percent_one_core": round(cpu_s / wall_s * 100, 3),
    "journal_events_per_minute": round(len(history) / wall_s * 60, 3),
    "confirmed_actions_per_minute": round(states.get("confirmed", 0) / wall_s * 60, 3),
    "journal_states": states,
    "journal_len": len(history),
    "reversal_observed": "rolled_back" in states,
}
print(json.dumps(result))
PY
}

# B first: nothing to manage, the daemon should journal nothing.
# Unit must exist before the daemon reads the config budget, so export a
# stable name per scenario; the budget branch below reuses it.
unit="breadram-bench-b-$$.scope"
export unit
b=$(run_scenario b "")
echo "$b"
# E: 16 MiB budget on a 96 MiB worker under a mild harness limit. The
# daemon's own write causes the stall rise it then reverses.
unit="breadram-bench-e-$$.scope"
export unit
e=$(run_scenario e 16777216)
echo "$e"
python3 - "$b" "$e" <<'PY'
import json, sys
b, e = (json.loads(x) for x in sys.argv[1:3])
print(
    f"idle daemon rss={b['daemon_rss_end_kb']}KiB jiffies={b['daemon_jiffies']} journal={b['journal_states']} "
    f"tight rounds={e['rounds']} p50={e['p95_ms']}ms journal={e['journal_states']}",
    file=sys.stderr,
)
if b["journal_len"] != 0:
    sys.exit("the idle daemon journalled actions with nothing to manage")
if b["exit"] or e["exit"]:
    sys.exit("a worker failed or timed out")
if e["rounds"] < 1:
    sys.exit("the budgeted worker produced no rounds")
if "rolled_back" not in e["journal_states"]:
    sys.exit("no reversal recorded for the stalling limit")
print("ok", file=sys.stderr)
PY
