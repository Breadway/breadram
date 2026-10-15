#!/bin/sh
# Daemon-driven breadsearch lifecycle: idle unload by policy, then protection.
#
# A disposable breadmill (isolated runtime, state, config, and cache symlink)
# serves an empty index in a bounded user service. A private breadramd in
# automatic mode may only unload_idle_model, with hair-trigger pressure from a
# sacrificial hog so the healthy host plans anything at all.
#
# Phase A: after 30 s of quiet (MIN_IDLE_MS) the daemon must unload the idle
# model itself. Reports RSS before/after and reload latency on demand.
# Phase B: continuous queries for 45 s must keep the reloaded model resident:
# protection, not just efficacy. A second unload in that window fails the run.
#
# Never touches the installed breadmill socket, state, or index.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
repo=$root/../..
daemon=$repo/target/release/breadramd
cli=$repo/target/release/breadram
# The installed breadmill predates the unload verbs: its status carries no
# version, so the adapter honestly offers no unload. Override with a binary
# built from the unload-verbs branch (for example the breadsearch checkout
# built with --features breadmill/full per ROAD-15). The preflight below
# fails fast instead of waiting out the window against an old daemon.
mill=${BREADMILL_BIN:-$HOME/.cargo/bin/breadmill}
. "$root/lib.sh"
require_session
[ -x "$daemon" ] && [ -x "$cli" ] && [ -x "$mill" ] || {
    echo "skipped: build release breadramd/breadram and provide breadmill" >&2
    exit 0
}

tag="$$"
bm_unit="breadram-bench-mill-$tag.service"
hog_unit="breadram-bench-hog-bs-$tag.scope"
base=$(mktemp -d)
rt=$base/rt; bst=$base/bmstate; bcfg=$base/bmcfg; bcache=$base/bmcache; rst=$base/ramstate
mkdir -p "$rt" "$bst" "$bcfg" "$bcache/breadsearch/models" "$rst"
ln -s "$HOME/.cache/breadsearch/models/model.onnx" "$bcache/breadsearch/models/model.onnx"
ln -s "$HOME/.cache/breadsearch/models/tokenizer.json" "$bcache/breadsearch/models/tokenizer.json"
sock=$rt/breadmill.sock
ramsock=$rst/breadram.sock
ramcfg=$rst/breadram.toml

# Explicit empty index roots: breadmill defaults would otherwise crawl the
# live home directory (Documents, Projects) from this host.
mkdir -p "$bcfg/breadsearch"
printf '[index]\nroots = []\n' >"$bcfg/breadsearch/config.toml"

cleanup() {
    for unit in "$bm_unit" "$hog_unit"; do
        systemctl --user stop "$unit" >/dev/null 2>&1 || true
    done
    if [ -f "$rst/daemon.pid" ]; then
        kill "$(cat "$rst/daemon.pid")" 2>/dev/null || true
    fi
    rm -rf "$base"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

rpc() { python3 "$root/breadmill-rpc.py" "$sock" "$@"; }
bm_rss_kib() {
    total=0
    for pid in $(systemctl --user show -p MainPID --value "$bm_unit" 2>/dev/null); do
        [ "$pid" -gt 0 ] 2>/dev/null || continue
        rss=$(rss_kib "$pid")
        total=$((total + rss))
    done
    echo "$total"
}

# The breadmill service: bounded, isolated, CPU backend by default config.
systemd-run --user --quiet --collect --unit="$bm_unit" \
    -p MemoryMax=2G -p MemorySwapMax=512M -p RuntimeMaxSec=600 \
    --setenv=XDG_RUNTIME_DIR="$rt" --setenv=XDG_STATE_HOME="$bst" \
    --setenv=XDG_CONFIG_HOME="$bcfg" --setenv=XDG_CACHE_HOME="$bcache" \
    -- "$mill" serve >"$base/mill.log" 2>&1 &
milljob=$!
ready=0
for _ in $(seq 1 90); do
    if [ -S "$sock" ] && rpc status 2>/dev/null | grep -q '"model_ready": *true'; then
        ready=1
        break
    fi
    sleep 2
done
[ "$ready" = 1 ] || { echo "breadmill never became ready:" >&2; tail -5 "$base/mill.log" >&2; exit 1; }
if ! rpc status 2>/dev/null | grep -q '"version":'; then
    echo "breadmill lacks unload verbs (no version in status); set BREADMILL_BIN to a verbs build" >&2
    exit 1
fi
rss_before_kib=$(bm_rss_kib)

# Private daemon: unload only, hair-trigger pressure, fast ticks.
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
    echo 'name = "bench-mill"'
    echo 'kind = "breadsearch"'
    echo "url = \"breadsearch:$sock\""
    echo "scope = \"unit:$bm_unit\""
    echo 'allow_automation = true'
} >"$ramcfg"
XDG_STATE_HOME=$rst "$daemon" --config "$ramcfg" --socket "$ramsock" >"$rst/daemon.log" 2>&1 &
echo $! >"$rst/daemon.pid"
for _ in $(seq 1 50); do [ -S "$ramsock" ] && break; sleep 0.1; done
[ -S "$ramsock" ] || { tail -5 "$rst/daemon.log" >&2; exit 1; }

# Bounded pressure so the healthy host plans at all.
timeout 300 systemd-run --user --scope --quiet --collect --unit="$hog_unit" \
    -p MemoryMax=160M -p MemorySwapMax=96M -p MemoryHigh=40M \
    -- python3 "$root/workload.py" 96 280 >/dev/null 2>&1 &
hogjob=$!

# Phase A: the daemon itself must unload the quiet model (30 s idle + ticks).
unloaded=0
for _ in $(seq 1 75); do
    if rpc status 2>/dev/null | grep -q '"model_ready": *false'; then
        unloaded=1
        break
    fi
    sleep 2
done
journal_a=$("$cli" --socket "$ramsock" actions --json)
echo "$journal_a" | grep -q '"kind": *"unload_idle_model"[^}]*"state": *"confirmed"' \
    || echo "$journal_a" | grep -q 'unload_idle_model' || true
daemon_unloaded=$(echo "$journal_a" | python3 -c "
import json,sys
d = json.load(sys.stdin)
print(sum(1 for e in d.get('history', []) if e.get('kind') == 'unload_idle_model' and e.get('state') == 'confirmed'))
")
[ "$unloaded" = 1 ] || { echo "daemon never unloaded the idle model" >&2; echo "$journal_a" | tail -3 >&2; exit 1; }
[ "$daemon_unloaded" -ge 1 ] || { echo "unload happened without a daemon-confirmed action" >&2; exit 1; }
rss_after_kib=$(bm_rss_kib)

# Demand reload through a query; the search retries while loading.
reload_start=$(date +%s)
reloaded=0
rpc query "hello world" >/dev/null 2>&1 || true
for _ in $(seq 1 60); do
    if rpc status 2>/dev/null | grep -q '"model_ready": *true'; then
        if rpc query "hello world" 2>/dev/null | grep -q '"hits"'; then
            reloaded=1
            break
        fi
    fi
    sleep 2
done
reload_s=$(( $(date +%s) - reload_start ))
[ "$reloaded" = 1 ] || { echo "demand reload never completed" >&2; exit 1; }

# Phase B: continuous queries must protect the resident model for 45 s.
unloads_before=$daemon_unloaded
for _ in $(seq 1 15); do
    rpc query "hello world" >/dev/null 2>&1 || true
    sleep 3
done
journal_b=$("$cli" --socket "$ramsock" actions --json)
unloads_after=$(echo "$journal_b" | python3 -c "
import json,sys
d = json.load(sys.stdin)
print(sum(1 for e in d.get('history', []) if e.get('kind') == 'unload_idle_model' and e.get('state') == 'confirmed'))
")
still_ready=$(rpc status 2>/dev/null | grep -c '"model_ready": *true' || true)
kill "$hogjob" 2>/dev/null || true
wait "$hogjob" 2>/dev/null || true
python3 - "$rss_before_kib" "$rss_after_kib" "$reload_s" "$unloads_before" "$unloads_after" "$still_ready" <<'PY'
import json, sys
before, after, reload_s, ub, ua, ready = (int(x) for x in sys.argv[1:])
result = {
    "scenario": "breadsearch_daemon_driven",
    "daemon_policy_driven": True,
    "idle_unload_by_daemon": True,
    "rss_before_kib": before,
    "rss_after_kib": after,
    "rss_freed_kib": before - after,
    "reload_s": reload_s,
    "serving_window_s": 45,
    "model_still_ready": ready >= 1,
    "unloads_before_window": ub,
    "unloads_after_window": ua,
    "protection_held": ua == ub and ready >= 1,
}
print(json.dumps(result))
if ua != ub or ready < 1:
    sys.exit("serving protection failed: model unloaded under continuous queries")
print("ok", file=sys.stderr)
PY
