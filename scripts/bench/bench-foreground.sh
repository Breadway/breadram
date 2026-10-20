#!/bin/sh
# Independent foreground probe versus bounded generic memory pressure.
# This calibrates a proxy workload; it does not claim daemon efficacy.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
. "$root/lib.sh"
require_session
unit="breadram-bench-foreground-$$.scope"
dir=$(mktemp -d)
cleanup() {
    systemctl --user stop "$unit" >/dev/null 2>&1 || true
    rm -rf "$dir"
}
trap cleanup EXIT
trap 'exit 130' INT TERM
python3 "$root/foreground.py" --seconds 8 > "$dir/baseline"
timeout 20 systemd-run --user --scope --quiet --collect --unit="$unit" \
    -p MemoryMax=160M -p MemorySwapMax=96M -p MemoryHigh=40M \
    -- python3 "$root/workload.py" 96 12 > "$dir/background" 2> "$dir/error" &
worker=$!
sleep 1
if ! systemctl --user is-active --quiet "$unit"; then
    cat "$dir/error" >&2
    exit 1
fi
python3 "$root/foreground.py" --seconds 8 > "$dir/pressured"
wait "$worker"
python3 - "$dir" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
for label in ["baseline", "pressured"]:
    row = json.loads((root / label).read_text())
    row["label"] = label
    print(json.dumps(row))
PY
