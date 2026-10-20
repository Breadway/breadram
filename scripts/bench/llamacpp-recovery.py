"""Measure real router inference, unload and demand reload on a private server.

Only called by bench-llamacpp.sh after it starts a bounded CPU-only service.
Reports cgroup charge (including file cache) and summed process RSS separately.
Device counters are whole-device context only, not provider-attributable savings.
No daemon policy is driven.
"""

import json
import pathlib
import sys
import time
import urllib.error
import urllib.request

from device_memory import snapshot as device_snapshot


def main():
    base, cg, model_path = sys.argv[1:]
    cg = pathlib.Path(cg)

    def request(path, body=None):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(
            base + path, data=data, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.load(response)

    def memory():
        # Router workers can use descendant cgroups. Missing samples fail.
        pids = set()
        for file in cg.rglob("cgroup.procs"):
            pids.update(int(pid) for pid in file.read_text().split())
        rss = 0
        for pid in pids:
            try:
                text = pathlib.Path(f"/proc/{pid}/status").read_text()
            except FileNotFoundError:
                continue
            for line in text.splitlines():
                if line.startswith("VmRSS:"):
                    rss += int(line.split()[1]) * 1024
        if not pids:
            raise RuntimeError("provider cgroup disappeared")
        return {
            "cgroup_current_bytes": int((cg / "memory.current").read_text()),
            "summed_process_rss_bytes": rss,
            "device_bytes": None,
            "whole_device_context": device_snapshot(),
        }

    deadline = time.monotonic() + 20
    while True:
        try:
            request("/health")
            break
        except (OSError, urllib.error.URLError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.2)

    def inference():
        start = time.monotonic()
        result = request(
            "/v1/chat/completions",
            {
                "model": "bench-small",
                "messages": [{"role": "user", "content": "Say hello."}],
                "max_tokens": 16,
                "temperature": 0,
            },
        )
        if not result.get("choices"):
            raise RuntimeError("inference returned no choices")
        return {
            "latency_s": time.monotonic() - start,
            "usage": result.get("usage"),
            "memory": memory(),
        }

    cold = inference()
    warm = inference()
    before = memory()
    start = time.monotonic()
    request("/models/unload", {"model": "bench-small"})
    # Logical confirmation and physical effects are separate observations.
    deadline = time.monotonic() + 10
    while True:
        listing = request("/models")
        entries = listing.get("data", listing.get("models", []))
        matched = [item for item in entries if item.get("id") == "bench-small"]
        status = matched[0].get("status") if matched else None
        if isinstance(status, dict):
            status = status.get("value")
        if status == "unloaded":
            break
        if time.monotonic() >= deadline:
            raise RuntimeError(f"unload not confirmed: {listing}")
        time.sleep(0.1)
    unload_s = time.monotonic() - start
    time.sleep(0.3)
    after = memory()
    reloaded = inference()
    warm_after_reload = inference()
    print(
        json.dumps(
            {
                "scenario": "llamacpp_api_recovery",
                "cpu_only": True,
                "daemon_policy_driven": False,
                "model_path": model_path,
                "model_file_bytes": pathlib.Path(model_path).stat().st_size,
                "cold": cold,
                "warm": warm,
                "reload": reloaded,
                "warm_after_reload": warm_after_reload,
                "reload_penalty_s": reloaded["latency_s"] - warm["latency_s"],
                "unload_s": unload_s,
                "unload_confirmed": True,
                "before_unload": before,
                "after_unload": after,
                "rss_reclaimed_bytes": before["summed_process_rss_bytes"]
                - after["summed_process_rss_bytes"],
                "cgroup_reclaimed_bytes": before["cgroup_current_bytes"]
                - after["cgroup_current_bytes"],
            }
        )
    )


if __name__ == "__main__":
    main()
