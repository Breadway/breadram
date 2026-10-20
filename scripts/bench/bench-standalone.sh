#!/bin/sh
# Minimal filesystem, private PID/network namespaces, no ecosystem runtime.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
daemon=$root/target/release/breadramd
cli=$root/target/release/breadram
[ -x "$daemon" ] && [ -x "$cli" ]
command -v bwrap >/dev/null
timeout 25 bwrap --unshare-all --die-with-parent \
    --ro-bind /usr /usr --symlink usr/lib /lib --symlink usr/lib /lib64 \
    --proc /proc --dev /dev --tmpfs /tmp --dir /run --dir /home \
    --dir /sys --dir /etc --dir /app \
    --ro-bind "$daemon" /app/breadramd --ro-bind "$cli" /app/breadram \
    --clearenv --setenv PATH /usr/bin --setenv HOME /home \
    --setenv XDG_RUNTIME_DIR /run --setenv XDG_STATE_HOME /tmp/state \
    /usr/bin/sh -eu -c '
        # Explicit configuration removes discovery of installed settings.
        printf "mode = \"monitor\"\n" > /tmp/config.toml
        /app/breadramd --config /tmp/config.toml --socket /run/ram.sock > /tmp/log 2>&1 &
        daemon_pid=$!
        trap '\''kill "$daemon_pid" 2>/dev/null || true'\'' EXIT
        for attempt in $(seq 1 50); do
            [ -S /run/ram.sock ] && break
            sleep .1
        done
        [ -S /run/ram.sock ] || { cat /tmp/log >&2; exit 1; }
        # Only these copied app binaries exist under /app. Host sockets,
        # configuration, home and systemd services are absent from the sandbox.
        [ ! -S /run/bread.sock ]
        [ ! -S /run/breadsearch.sock ]
        /app/breadram --socket /run/ram.sock status --json
        /app/breadram --socket /run/ram.sock workloads --json
        /app/breadram --socket /run/ram.sock capabilities --json
    '
