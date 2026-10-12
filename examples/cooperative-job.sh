#!/bin/sh
# Opted-in job for RAM-25. Reads $BREADRAM_WORKERS_FILE and keeps at most one
# extra worker while the file says "run" with a count above 1. "reduce 1", a
# missing file, or an unreadable file stops the extra worker. The job itself
# keeps running: a vanished daemon must not leave it paused.
set -u
file=${BREADRAM_WORKERS_FILE:-}
extra=""
trap 'if [ -n "$extra" ]; then kill "$extra" 2>/dev/null; fi' EXIT INT TERM
while true; do
    line=""
    if [ -n "$file" ] && [ -r "$file" ]; then
        line=$(head -n 1 "$file" 2>/dev/null || true)
    fi
    want=1
    case $line in
        "run "[2-9]*) want=2 ;;
    esac
    if [ "$want" -eq 1 ] && [ -n "$extra" ]; then
        kill "$extra" 2>/dev/null
        wait "$extra" 2>/dev/null
        extra=""
    fi
    if [ "$want" -eq 2 ] && [ -z "$extra" ]; then
        sleep 3600 &
        extra=$!
    fi
    # In the background so this script has no other child while it waits:
    # a foreground sleep would be counted as a worker.
    sleep 0.2 &
    wait $!
done
