#!/bin/sh
# Bounded CPU-only provider recovery probe. Never uses the installed server.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
. "$root/lib.sh"
require_session
command -v llama-server >/dev/null
model=${BREADRAM_BENCH_MODEL:-$HOME/.cache/llm/models/unsloth/LFM2.5-230M-GGUF/LFM2.5-230M-Q4_0.gguf}
[ -f "$model" ] || { echo 'missing local model; set BREADRAM_BENCH_MODEL' >&2; exit 1; }
unit="breadram-bench-llama-$$.service"
dir=$(mktemp -d)
cleanup() {
    systemctl --user stop "$unit" >/dev/null 2>&1 || true
    rm -rf "$dir"
}
trap cleanup EXIT
trap 'exit 130' INT TERM
# Pick a free loopback port, then fail if another process wins the bind race.
port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
cat > "$dir/models.ini" <<EOF
version = 1
[*]
device = none
n-gpu-layers = 0
ctx-size = 1024
parallel = 1
threads = 2
[bench-small]
model = $model
EOF
systemd-run --user --quiet --collect --unit="$unit" \
    -p MemoryMax=1536M -p MemorySwapMax=256M -p RuntimeMaxSec=120 \
    -- "$(command -v llama-server)" --host 127.0.0.1 --port "$port" \
    --models-preset "$dir/models.ini" --models-max 1
cg=$(systemctl --user show -p ControlGroup --value "$unit")
python3 "$root/llamacpp-recovery.py" "http://127.0.0.1:$port" \
    "/sys/fs/cgroup$cg" "$model"
