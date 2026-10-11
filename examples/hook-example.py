#!/usr/bin/env python3
"""Minimal breadram hook example (RAM-15).

A cooperative hook is a program breadram runs with one verb argument:

    hook-example.py status   -> {"busy": ..., ...}
    hook-example.py release  -> {"released": ..., ...}
    hook-example.py wake     -> {"awake": ..., ...}

This example pretends to manage a preprocessed-data cache for a training job.
It keeps its state in a small JSON file next to itself:

- `busy` is true while the job holds its lock file (training in flight).
- `release` drops the cache directory listing when idle, and declines while
  busy so breadram journals the denial in the hook's own words.
- `wake` rebuilds the listing.

Copy this file, point a `hook` provider `command` at the copy, and keep the
failure modes honest: never report idle while work is in flight, and decline
a release rather than pretend one happened.

State lives beside the script so the example works unmodified from a checkout.
A real hook should keep its state under the workload's own runtime directory.
"""

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(HERE, ".hook-example-state.json")
LOCK_FILE = os.path.join(HERE, ".hook-example.lock")
CACHE_FILE = os.path.join(HERE, ".hook-example-cache.json")


def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return {"cache_bytes": 512 * 1024 * 1024}


def save_state(state):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(state, handle)
    os.replace(tmp, STATE_FILE)


def emit(document):
    json.dump(document, sys.stdout)
    sys.stdout.write("\n")


def do_status():
    state = load_state()
    busy = os.path.exists(LOCK_FILE)
    cache_bytes = state.get("cache_bytes", 0)
    emit(
        {
            "busy": busy,
            "host_ram_bytes": cache_bytes if not busy else 0,
            "note": "training in flight" if busy else "cache is droppable",
        }
    )


def do_release():
    if os.path.exists(LOCK_FILE):
        emit({"released": False, "note": "training in flight, release declined"})
        return
    state = load_state()
    freed = state.get("cache_bytes", 0)
    state["cache_bytes"] = 0
    save_state(state)
    try:
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
    except OSError:
        pass
    emit({"released": True, "note": f"dropped {freed} bytes of preprocessed cache"})


def do_wake():
    state = load_state()
    state["cache_bytes"] = 512 * 1024 * 1024
    save_state(state)
    with open(CACHE_FILE, "w", encoding="utf-8") as handle:
        handle.write("rebuilt\n")
    emit({"awake": True, "note": "cache listing rebuilt"})


def main(argv):
    if len(argv) != 2 or argv[1] not in ("status", "release", "wake"):
        sys.stderr.write("usage: hook-example.py [status|release|wake]\n")
        return 2
    {"status": do_status, "release": do_release, "wake": do_wake}[argv[1]]()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
