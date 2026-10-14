#!/bin/sh
# Scenario A: generic memory worker, unmanaged versus MemoryHigh-managed.
#
# Two disposable scopes, each capped at 160 MiB plus 96 MiB swap. The worker
# touches 96 MiB for 3 seconds. Reports throughput, per-round latency,
# memory held and peaked, scope stall accumulation, host stall deltas, and
# swap activity deltas. MemoryHigh here is the harness bound; breadram's own
# control stays MemoryHigh too, so the numbers transfer to daemon-managed
# runs.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
. "$root/lib.sh"
require_session

run_one() {
    label=$1
    high=$2
    unit="breadram-bench-${label}-$$.scope"
    out=$(mktemp)
    eval "$(host_pressure | sed 's/^/before_/')"
    eval "$(swap_io | sed 's/^/sw_before_/')"
    # The worker runs 6 s; the late sample at 5 s catches the scope alive
    # (--collect unloads it the moment the worker exits, so anything read
    # after wait would be zeros from a vanished cgroup).
    timeout 14 systemd-run --user --scope --quiet --collect \
        --unit="$unit" \
        -p MemoryMax=160M \
        -p MemorySwapMax=96M \
        -p MemoryHigh="$high" \
        -- python3 "$root/workload.py" 96 6 >"$out" 2>"$out.err" &
    pid=$!
    sleep 0.5
    cg=$(systemctl --user show -p ControlGroup --value "$unit" 2>/dev/null || true)
    eval "$(sample_scope "$cg" | sed 's/^/mid_/')"
    sleep 4.5
    cg=$(systemctl --user show -p ControlGroup --value "$unit" 2>/dev/null || true)
    eval "$(sample_scope "$cg" | sed 's/^/late_/')"
    set +e
    wait "$pid"
    status=$?
    set -e
    eval "$(host_pressure | sed 's/^/after_/')"
    eval "$(swap_io | sed 's/^/sw_after_/')"
    eval "$(summarize_rounds "$out")"
    rm -f "$out" "$out.err"
    # 124 is timeout's code. A killed scope still reports the sample.
    python3 - "$label" "$high" "$status" "$rounds" "$p50_ms" "$p95_ms" \
        "$mid_current" "$late_peak" "$mid_pressure" "$late_pressure" "$late_pgfault" "$late_pgmajfault" \
        "$before_host_some" "$after_host_some" "$before_host_full" "$after_host_full" \
        "$sw_before_pgpgin" "$sw_after_pgpgin" "$sw_before_pgpgout" "$sw_after_pgpgout" \
        "$sw_before_pswpin" "$sw_after_pswpin" "$sw_before_pswpout" "$sw_after_pswpout" <<'PY'
import json, sys
(label, high, status, rounds, p50, p95, mid_current, late_peak, mid_p, late_p,
 late_fault, late_maj, b_some, a_some, b_full, a_full,
 b_in, a_in, b_out, a_out, b_sin, a_sin, b_sout, a_sout) = sys.argv[1:]
result = {
    "scenario": "generic",
    "label": label,
    "memory_high": high,
    "exit": int(status),
    "rounds": int(rounds),
    "p50_ms": float(p50),
    "p95_ms": float(p95),
    "mid_current_bytes": int(mid_current),
    "peak_bytes": int(late_peak),
    "scope_stall_us": max(0, int(late_p or 0) - int(mid_p or 0)),
    "scope_pgfault": int(late_fault),
    "scope_pgmajfault": int(late_maj),
    "host_some_delta_us": int(a_some or 0) - int(b_some or 0),
    "host_full_delta_us": int(a_full or 0) - int(b_full or 0),
    "pgpgin_delta": int(a_in or 0) - int(b_in or 0),
    "pgpgout_delta": int(a_out or 0) - int(b_out or 0),
    "pswpin_delta": int(a_sin or 0) - int(b_sin or 0),
    "pswpout_delta": int(a_sout or 0) - int(b_sout or 0),
}
print(json.dumps(result))
PY
}

loose=$(run_one loose 144M)
tight=$(run_one tight 40M)
echo "$loose"
echo "$tight"
python3 - "$loose" "$tight" <<'PY'
import json, sys
loose, tight = (json.loads(x) for x in sys.argv[1:3])
print(
    f"loose rounds={loose['rounds']} p50={loose['p50_ms']}ms peak={loose['peak_bytes']} "
    f"tight rounds={tight['rounds']} p50={tight['p50_ms']}ms peak={tight['peak_bytes']} "
    f"tight_stall_us={tight['scope_stall_us']} tight_majflt={tight['scope_pgmajfault']}",
    file=sys.stderr,
)
if loose["rounds"] < 1:
    sys.exit("the loose scope produced no rounds")
if tight["rounds"] > loose["rounds"]:
    sys.exit("the tight limit did not cost throughput")
if tight["peak_bytes"] and loose["peak_bytes"] and tight["peak_bytes"] > loose["peak_bytes"]:
    sys.exit("the tight limit did not hold memory down")
print("ok", file=sys.stderr)
PY
