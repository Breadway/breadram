"""Read independently reported DRM memory counters without touching devices.

These are whole-device counters and include unrelated desktop activity. GTT
on an integrated GPU is shared system RAM, not additional dedicated VRAM.
Absent or unreadable counters remain null, never fabricated zeroes.
"""

import json
import pathlib
import time


def snapshot(root="/sys/class/drm"):
    devices = []
    for card in sorted(pathlib.Path(root).glob("card[0-9]*")):
        if not card.name[4:].isdigit() or not (card / "device").exists():
            continue
        device = card / "device"
        values = {}
        for kind in ("gtt", "vram"):
            for metric in ("used", "total"):
                path = device / f"mem_info_{kind}_{metric}"
                try:
                    value = int(path.read_text().strip())
                    if value < 0:
                        raise ValueError("negative counter")
                except (OSError, ValueError):
                    value = None
                values[f"{kind}_{metric}_bytes"] = value
        devices.append(
            {
                "card": card.name,
                "device_path": str(device.resolve()),
                "attribution": "whole_device_not_provider",
                **values,
            }
        )
    return {"monotonic_ns": time.monotonic_ns(), "devices": devices}


if __name__ == "__main__":
    print(json.dumps(snapshot()))
