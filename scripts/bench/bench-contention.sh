#!/bin/sh
# Provider contention: two disposable breadmills under one daemon and pressure.
#
# mill-a is queried continuously and must stay resident; mill-b is left quiet
# and must be unloaded by policy. Both run isolated (empty roots, own runtime,
# state, config, cache symlinks) in bounded services. The private daemon may
# only unload_idle_model. A sacrificial hog supplies bounded host pressure.
# Never touches installed sockets, state, or indexes.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
repo=$root/../..
daemon=${BREADRAM_BENCH_DAEMON:-$repo/target/release/breadramd}
cli=${BREADRAM_BENCH_CLI:-$repo/target/release/breadram}
mill=${BREADMILL_BIN:-$HOME/.cargo/bin/breadmill}
. "$root/lib.sh"
require_session
[ -x "$daemon" ] && [ -x "$cli" ] && [ -x "$mill" ] || {
    echo "skipped: build release breadramd/breadram and provide breadmill" >&2
    exit 0
}

tag="$$"
mill_a_unit="breadram-bench-ma-$tag.service"
mill_b_unit="breadram-bench-mb-$tag.service"
hog_unit="breadram-bench-hog-ct-$tag.scope"
base=$(mktemp -d)
rst=$base/ramstate
mkdir -p "$rst"

cleanup() {
    # One unit per call: a single call naming a unit that already exited fails
    # as a whole and stops none of the others, which leaked running mills.
    for unit in "$mill_a_unit" "$mill_b_unit" "$hog_unit"; do
        systemctl --user stop "$unit" >/dev/null 2>&1 || true
    done
    if [ -f "$rst/daemon.pid" ]; then
        kill "$(cat "$rst/daemon.pid")" 2>/dev/null || true
    fi
    rm -rf "$base"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

start_mill() {
    # $1 = a|b. Echoes the runtime dir.
    name=$1
    mbase=$base/mill-$name
    mkdir -p "$mbase/rt" "$mbase/st" "$mbase/cfg/breadsearch" \
        "$mbase/cache/breadsearch/models"
    ln -s "$HOME/.cache/breadsearch/models/model.onnx" "$mbase/cache/breadsearch/models/model.onnx"
    ln -s "$HOME/.cache/breadsearch/models/tokenizer.json" "$mbase/cache/breadsearch/models/tokenizer.json"
    printf '[index]\nroots = []\n' >"$mbase/cfg/breadsearch/config.toml"
    eval "unit=\$mill_${name}_unit"
    # Environment must travel via --setenv: a prefix only changes systemd-run's
    # own environment, and the service would silently use the live home dirs.
    systemd-run --user --quiet --collect \
        --unit="$unit" -p MemoryMax=2G -p MemorySwapMax=512M -p RuntimeMaxSec=900 \
        --setenv=XDG_RUNTIME_DIR="$mbase/rt" --setenv=XDG_STATE_HOME="$mbase/st" \
        --setenv=XDG_CONFIG_HOME="$mbase/cfg" --setenv=XDG_CACHE_HOME="$mbase/cache" \
        -- "$mill" serve >"$mbase/mill.log" 2>&1 &
    echo "$mbase/rt"
}

rpc() { python3 "$root/breadmill-rpc.py" "$@"; }

rt_a=$(start_mill a)
rt_b=$(start_mill b)
sock_a=$rt_a/breadmill.sock
sock_b=$rt_b/breadmill.sock
ready_a=0
ready_b=0
for _ in $(seq 1 120); do
    if [ "$ready_a" != 1 ] && rpc "$sock_a" status 2>/dev/null | grep -q '"model_ready": *true'; then
        ready_a=1
    fi
    if [ "$ready_b" != 1 ] && rpc "$sock_b" status 2>/dev/null | grep -q '"model_ready": *true'; then
        ready_b=1
    fi
    [ "$ready_a" = 1 ] && [ "$ready_b" = 1 ] && break
    sleep 2
done
[ "$ready_a" = 1 ] && [ "$ready_b" = 1 ] || {
    echo "mills never both ready; diagnostics:" >&2
    for unit in "$mill_a_unit" "$mill_b_unit"; do
        echo "$unit: $(systemctl --user is-active "$unit" 2>&1)" >&2
    done
    for sock in "$sock_a" "$sock_b"; do
        ls -la "$sock" >&2 2>&1 || true
        rpc "$sock" status 2>&1 | tail -n 2 >&2 || true
    done
    journalctl --user -u "$mill_a_unit" -u "$mill_b_unit" --no-pager -n 12 >&2 2>&1 || true
    exit 1
}
if [ -S "${XDG_RUNTIME_DIR:-/run/user/1000}/breadmill.sock" ]; then
    echo "isolation breach: a mill bound the live socket" >&2
    exit 1
fi
for sock in "$sock_a" "$sock_b"; do
    rpc "$sock" status 2>/dev/null | grep -q '"version":' || {
        echo "breadmill lacks unload verbs; set BREADMILL_BIN to a verbs build" >&2
        exit 1
    }
done

ramsock=$rst/breadram.sock
{
    echo 'mode = "automatic"'
    echo '[automation]'
    echo 'allowed_actions = ["unload_idle_model"]'
    echo '[pressure]'
    echo 'pressured_some_avg10 = 0.01'
    echo 'critical_some_avg10 = 50.0'
    echo 'critical_available_fraction = 0.01'
    echo '[timing]'
    echo 'settle_ms = 2000'
    echo 'min_dwell_ms = 4000'
    echo 'cooldown_ms = 8000'
    echo '[[providers]]'
    echo 'name = "mill-a"'
    echo 'kind = "breadsearch"'
    echo "url = \"breadsearch:$sock_a\""
    echo "scope = \"unit:$mill_a_unit\""
    echo 'allow_automation = true'
    echo '[[providers]]'
    echo 'name = "mill-b"'
    echo 'kind = "breadsearch"'
    echo "url = \"breadsearch:$sock_b\""
    echo "scope = \"unit:$mill_b_unit\""
    echo 'allow_automation = true'
} >"$rst/breadram.toml"
XDG_STATE_HOME=$rst "$daemon" --config "$rst/breadram.toml" --socket "$ramsock" >"$rst/daemon.log" 2>&1 &
echo $! >"$rst/daemon.pid"
for _ in $(seq 1 50); do [ -S "$ramsock" ] && break; sleep 0.1; done
[ -S "$ramsock" ] || { tail -5 "$rst/daemon.log" >&2; exit 1; }

timeout 300 systemd-run --user --scope --quiet --collect --unit="$hog_unit" \
    -p MemoryMax=160M -p MemorySwapMax=96M -p MemoryHigh=40M \
    -- python3 "$root/workload.py" 96 280 >/dev/null 2>&1 &
hogjob=$!

# Keep mill-a serving from the start, before any 30 s idle can accrue.
# mill-b stays quiet the whole window.
for _ in $(seq 1 30); do
    rpc "$sock_a" query "hello world" >/dev/null 2>&1 || true
    sleep 3
done
kill "$hogjob" 2>/dev/null || true
wait "$hogjob" 2>/dev/null || true

ready_a_end=$(rpc "$sock_a" status 2>/dev/null | grep -c '"model_ready": *true' || true)
ready_b_end=$(rpc "$sock_b" status 2>/dev/null | grep -c '"model_ready": *true' || true)
journal=$("$cli" --socket "$ramsock" actions --json)
python3 - "$ready_a_end" "$ready_b_end" "$journal" <<'PY'
import json, sys
ra, rb, journal = int(sys.argv[1]), int(sys.argv[2]), json.loads(sys.argv[3])
by_scope = {}
for e in journal.get("history", []):
    if e.get("kind") == "unload_idle_model" and e.get("state") == "confirmed":
        by_scope[e.get("scope", "?")] = by_scope.get(e.get("scope", "?"), 0) + 1
busy_unloads = sum(n for scope, n in by_scope.items() if "-ma-" in scope)
quiet_unloads = sum(n for scope, n in by_scope.items() if "-mb-" in scope)
# End-state readiness is not protection: a query reloads an unloaded model on
# demand, so a busy mill that was unloaded mid-window still ends up ready.
# Protection means the busy mill was never unloaded at all.
result = {
    "scenario": "provider_contention",
    "daemon_policy_driven": True,
    "busy_mill_still_ready": ra >= 1,
    "quiet_mill_still_ready": rb >= 1,
    "busy_mill_unloads": busy_unloads,
    "quiet_mill_unloads": quiet_unloads,
    "confirmed_unloads_by_scope": by_scope,
    "busy_mill_protected": busy_unloads == 0,
    "contention_resolved": busy_unloads == 0 and quiet_unloads >= 1 and rb < 1,
}
print(json.dumps(result))
if busy_unloads:
    sys.exit(f"serving protection failed: busy mill was unloaded {busy_unloads} time(s) under continuous queries")
if not (quiet_unloads >= 1 and rb < 1):
    sys.exit("quiet mill was not unloaded by policy")
print("ok", file=sys.stderr)
PY
