"""Verify two daemon-driven unloads with one demand reload in between.

Called instead of the contention query loop when explicitly selected. It never
unloads a model itself. The same daemon journal and provider process survive
the entire cycle, so duplicate suppression cannot be hidden by a restart.
"""

import json
import socket
import subprocess
import sys
import time


def rpc(path, payload):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
        stream.settimeout(30)
        stream.connect(path)
        stream.sendall((json.dumps(payload) + "\n").encode())
        with stream.makefile("rb") as file:
            return json.loads(file.readline(65536))


def main():
    sock, cli, daemon_sock, scope = sys.argv[1:]

    def confirmed():
        result = subprocess.run(
            [cli, "--socket", daemon_sock, "actions", "--json"],
            check=True,
            capture_output=True,
            text=True,
        )
        history = json.loads(result.stdout)["history"]
        return sum(
            row.get("scope") == scope
            and row.get("kind") == "unload_idle_model"
            and row.get("state") == "confirmed"
            for row in history
        )

    def wait_unload(count):
        deadline = time.monotonic() + 95
        while time.monotonic() < deadline:
            status = rpc(sock, {"type": "status"})
            if not status.get("model_ready") and confirmed() >= count:
                return
            time.sleep(1)
        raise RuntimeError(f"daemon did not confirm unload {count}")

    wait_unload(1)
    start = time.monotonic()
    deadline = start + 45
    while time.monotonic() < deadline:
        response = rpc(sock, {"type": "query", "query": "hello world", "limit": 3})
        if response.get("type") == "hits":
            break
        time.sleep(1)
    else:
        raise RuntimeError("demand reload did not return hits")
    reload_s = time.monotonic() - start
    if confirmed() != 1:
        raise RuntimeError("unexpected extra unload before idle grace")
    idle_start = time.monotonic()
    wait_unload(2)
    idle_s = time.monotonic() - idle_start
    if idle_s < 30:
        raise RuntimeError("second unload occurred before 30-second idle grace")
    print(
        json.dumps(
            {
                "scenario": "provider_reload_idle_cycle",
                "confirmed_unloads": confirmed(),
                "same_daemon": True,
                "demand_reload_s": reload_s,
                "second_idle_s": idle_s,
                "passed": True,
            }
        )
    )


if __name__ == "__main__":
    main()
