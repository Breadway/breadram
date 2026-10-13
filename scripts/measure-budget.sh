#!/bin/sh
# Disposable budget measurement for RAM-22.
#
# Two transient user scopes. Each has a hard MemoryMax and MemorySwapMax so
# the host cannot be exhausted, and `timeout` kills the scope if MemoryHigh
# stalls it. The worker touches a fixed 48 MiB buffer for one second. A loose
# MemoryHigh lets it keep that buffer. A tight MemoryHigh pushes the excess
# into the scope's own swap and slows the loop.
#
# MemoryMax here is the harness. breadram's own control stays MemoryHigh,
# which throttles and does not kill. The stall sample is the scope's own
# cgroup pressure `total` counter: it accumulates inside one run, where the
# ten-second average cannot move. A tight limit that accumulates clearly more
# stall than the loose one is a Reverse verdict: that intervention would be
# put back, not kept.
set -eu
if [ -z "${XDG_RUNTIME_DIR:-}" ] || ! command -v systemd-run >/dev/null || ! command -v timeout >/dev/null; then
    echo "skipped: no systemd user session" >&2
    exit 0
fi
root=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
worker=$root/budget-worker.py

run_one() {
    label=$1
    high=$2
    unit="breadram-budget-${label}-$$.scope"
    out=$(mktemp)
    # timeout kills systemd-run, which is the scope owner, so a stalled
    # worker cannot outlive this script.
    timeout 8 systemd-run --user --scope --quiet --collect \
        --unit="$unit" \
        -p MemoryMax=80M \
        -p MemorySwapMax=48M \
        -p MemoryHigh="$high" \
        -- python3 "$worker" 48 1 >"$out" 2>"$out.err" &
    pid=$!
    sleep 0.4
    cg=$(systemctl --user show -p ControlGroup --value "$unit" 2>/dev/null || true)
    current=0
    pressure=0
    if [ -n "$cg" ] && [ -r "/sys/fs/cgroup${cg}/memory.current" ]; then
        current=$(cat "/sys/fs/cgroup${cg}/memory.current")
        if [ -r "/sys/fs/cgroup${cg}/memory.pressure" ]; then
            pressure=$(awk '/^some/ { for (i = 2; i <= NF; i++) if ($i ~ /^total=/) { split($i, a, "="); print a[2] } }' "/sys/fs/cgroup${cg}/memory.pressure")
        fi
    fi
    set +e
    wait "$pid"
    status=$?
    set -e
    rounds=$(cat "$out" 2>/dev/null || echo 0)
    rm -f "$out" "$out.err"
    # 124 is timeout's code. A killed scope still reports the sample.
    echo "$label high=$high rounds=${rounds:-0} current=$current pressure=$pressure status=$status"
}

loose=$(run_one loose 72M)
tight=$(run_one tight 20M)
echo "$loose"
echo "$tight"
python3 - "$loose" "$tight" <<'PY'
import sys
def field(line, key):
    for part in line.split():
        if part.startswith(key + "="):
            return part.split("=", 1)[1]
    return "0"
loose, tight = sys.argv[1], sys.argv[2]
lr, tr = int(field(loose, "rounds") or 0), int(field(tight, "rounds") or 0)
lc, tc = int(field(loose, "current") or 0), int(field(tight, "current") or 0)
lp, tp = int(field(loose, "pressure") or 0), int(field(tight, "pressure") or 0)
print(
    f"loose_rounds={lr} tight_rounds={tr} loose_current={lc} tight_current={tc} loose_stall_us={lp} tight_stall_us={tp}",
    file=sys.stderr,
)
if lr < 1:
    sys.exit("the loose scope produced no rounds")
# A tight limit that times out produced 0 rounds: that is a stall, and it
# is the throughput cost. It must not have kept more memory than the loose run.
if tr > lr:
    sys.exit("the tight limit did not cost throughput")
if tc and lc and tc > lc:
    sys.exit("the tight limit did not hold memory down")
# The totals are cumulative stall microseconds sampled 0.4s into each run.
# Half a CPU-second more under the tight limit is a Reverse: the daemon would
# put that limit back on the same signal.
if tp >= lp + 500_000 or tr == 0:
    print("stall verdict: Reverse", file=sys.stderr)
else:
    print("stall verdict: Keep", file=sys.stderr)
PY
echo ok
