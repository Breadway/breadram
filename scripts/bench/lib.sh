#!/bin/sh
# Shared helpers for the RAM-30 benchmark suite.
#
# Every scenario runs inside disposable systemd user scopes with a hard
# MemoryMax and MemorySwapMax plus a `timeout` kill, so a stalled workload
# cannot outlive its script or exhaust the host. Sampling helpers read
# cgroup files that may vanish mid-read; every read tolerates that.

# Sample a scope's cgroup counters. Prints shell assignments.
# Usage: sample_scope <control-group-path>
sample_scope() {
    cg=$1
    valid=1
    current=
    peak=
    pressure=
    full=
    pgfault=
    pgmajfault=
    if [ -n "$cg" ] && [ -r "/sys/fs/cgroup${cg}/memory.current" ]; then
        current=$(cat "/sys/fs/cgroup${cg}/memory.current" 2>/dev/null || true)
        peak=$(cat "/sys/fs/cgroup${cg}/memory.peak" 2>/dev/null || true)
        if [ -r "/sys/fs/cgroup${cg}/memory.pressure" ]; then
            pressure=$(awk '/^some/ { for (i = 2; i <= NF; i++) if ($i ~ /^total=/) { split($i, a, "="); print a[2] } }' "/sys/fs/cgroup${cg}/memory.pressure" 2>/dev/null || echo 0)
            full=$(awk '/^full/ { for (i = 2; i <= NF; i++) if ($i ~ /^total=/) { split($i, a, "="); print a[2] } }' "/sys/fs/cgroup${cg}/memory.pressure" 2>/dev/null || echo 0)
        fi
        if [ -r "/sys/fs/cgroup${cg}/memory.stat" ]; then
            pgfault=$(awk '/^pgfault / { print $2 }' "/sys/fs/cgroup${cg}/memory.stat" 2>/dev/null || echo 0)
            pgmajfault=$(awk '/^pgmajfault / { print $2 }' "/sys/fs/cgroup${cg}/memory.stat" 2>/dev/null || echo 0)
        fi
    fi
    for metric in current peak pressure full pgfault pgmajfault; do
        # Missing or malformed counters are not measurements of zero.
        eval 'value=${'"$metric"':-}'
        case "$value" in ''|*[!0-9]*) valid=0 ;; esac
    done
    [ -n "$cg" ] && [ -r "/sys/fs/cgroup${cg}/memory.stat" ] || valid=0
    echo "valid=$valid"
    echo "current=${current:-0}"
    echo "peak=${peak:-0}"
    echo "pressure=${pressure:-0}"
    echo "full=${full:-0}"
    echo "pgfault=${pgfault:-0}"
    echo "pgmajfault=${pgmajfault:-0}"
}

# Host memory pressure totals (some and full), one assignment per line.
host_pressure() {
    f=/sys/fs/cgroup/memory.pressure
    if [ -r "$f" ]; then
        awk '/^some/ { for (i = 2; i <= NF; i++) if ($i ~ /^total=/) { split($i, a, "="); print "host_some=" a[2] } } /^full/ { for (i = 2; i <= NF; i++) if ($i ~ /^total=/) { split($i, a, "="); print "host_full=" a[2] } }' "$f" 2>/dev/null
    else
        echo "host_some=0"
        echo "host_full=0"
    fi
}

# Swap activity counters from /proc/vmstat, one assignment per line.
swap_io() {
    echo "pgpgin=$(awk '/^pgpgin/ { print $2 }' /proc/vmstat)"
    echo "pgpgout=$(awk '/^pgpgout/ { print $2 }' /proc/vmstat)"
    echo "pswpin=$(awk '/^pswpin/ { print $2 }' /proc/vmstat)"
    echo "pswpout=$(awk '/^pswpout/ { print $2 }' /proc/vmstat)"
}

# RSS in KiB of a pid, or 0.
rss_kib() {
    awk '/VmRSS:/ { print $2 }' "/proc/$1/status" 2>/dev/null || echo 0
}

# Total CPU jiffies (utime+stime) of a pid, or 0.
cpu_jiffies() {
    awk '{ print $14 + $15 }' "/proc/$1/stat" 2>/dev/null || echo 0
}

# Summarize workload.py JSON lines from a file. Prints assignments:
# rounds, p50_ms, p95_ms (from summary line when present).
summarize_rounds() {
    python3 - "$1" <<'PY'
import json, sys
rounds, p50, p95 = 0, 0.0, 0.0
try:
    for line in open(sys.argv[1]):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if row.get("summary"):
            rounds = row.get("rounds", rounds)
            p50 = row.get("p50_ms", p50)
            p95 = row.get("p95_ms", p95)
except FileNotFoundError:
    pass
print(f"rounds={rounds} p50_ms={p50} p95_ms={p95}")
PY
}

# Skip the whole suite gracefully without a user session.
require_session() {
    if [ -z "${XDG_RUNTIME_DIR:-}" ] || ! command -v systemd-run >/dev/null || ! command -v timeout >/dev/null; then
        echo "skipped: no systemd user session" >&2
        exit 0
    fi
}
