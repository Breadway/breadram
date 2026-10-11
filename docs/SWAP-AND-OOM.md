# Swap, compression, and OOM management

breadram reports the swap and OOM layout of the machine it runs on. It never
changes it. Swappiness, swap files, `fstab` entries, compressed device sizes, and
kernel parameters belong to the administrator, and existing OOM management,
including the kernel's own killer and `systemd-oomd`, stays in charge. There is no
write path in `breadram_core::telemetry::swap` at all: every function reads.

This document covers what is inspected, what is reported, and what a degraded
host looks like.

## What is read

| Source | What it answers |
| --- | --- |
| `/proc/swaps` | Which swap areas exist, whether each is a partition or a file, its size, its usage, and its priority |
| `/proc/vmstat` | Pages swapped in and out since boot, and the OOM kills the kernel has performed |
| `/sys/module/zswap/parameters/*` | Whether the compressed swap cache is enabled, its pool share, compressor, and zpool |
| `/sys/block/zram*/*` | Compressed RAM devices: configured size, memory actually used, algorithm |
| `/usr/lib/systemd/system/systemd-oomd.service` (and the `/lib` and `/etc` locations) | Whether a systemd OOM manager is installed |
| a cgroup's `memory.swap.current`, `memory.swap.max`, `memory.swap.high`, `memory.events` | The effective swap limit of a workload breadram can control, and how often that cgroup hit it |

The cgroup figures are read for the workloads breadram can actually control, so a
limit is reported against the cgroup the workload is charged to rather than
against an assumption about which group it belongs in.

## What is reported

`breadram diagnostics` prints a swap section:

```
swap areas: 1 (total 4.0 GiB, used 1.0 GiB)
swap rate:  pages in 12, out unknown
compression: compressed swap in use
OOM:        a systemd OOM manager is installed and 2 OOM kill(s) since boot
  /dev/zram0 (partition, 4.0 GiB, used 1.0 GiB, priority 100)
  zram0 (compressed device, size 4.0 GiB, in use 512.0 MiB)
  zswap (enabled), pool up to 20% of RAM, compressor lz4
workload swap:
  unit:app.service (in use 1.0 MiB, unit:app.service may swap without a limit, which is the kernel default)
```

The same section is in the JSON form of `diagnostics`, as `swap` and
`workload_swap` on the payload.

Every report carries notes that state a fact and its consequence. Examples, one
from each posture:

- No swap: "no swap is configured, so an anonymous page cannot be moved out of
  RAM: relief comes from reclaiming caches and, at the limit, from the kernel OOM
  killer".
- Disk swap: "swap is configured without compression, so reclaiming an anonymous
  page costs disk I/O".
- Compressed swap: "swap is compressed on this host, so reclaiming an anonymous
  page costs CPU rather than disk I/O".
- Always: "breadram never changes global swappiness, swap files, fstab entries, or
  kernel parameters; the swap layout above is the administrator's".

## OOM: coexistence, not competition

breadram installs no OOM killer and does not tune one. It reports:

- whether a systemd OOM manager is installed, in which case that manager remains
  the OOM manager on the host;
- how many OOM kills the kernel has recorded since boot, from `/proc/vmstat`.

A count of zero, a count that could not be read, and a host with no OOM manager
are three different reports. A missing counter is never printed as zero kills.

Host memory pressure and device memory exhaustion are separate findings:
`available_bytes` and the pressure reading describe host RAM, while adapter
inventories are the only source of device memory figures, and an adapter that
reports nothing leaves the figure unknown rather than zero.

## Administrator setups breadram supports

| Setup | What breadram does |
| --- | --- |
| No swap at all | Reports it, and explains that relief can only come from reclaim, not from moving pages out |
| A disk swap file or partition | Reports each area with its size, usage, and priority, and leaves it alone |
| A compressed RAM device (`zram`) | Reports the device, its configured size, the memory it is really using, and treats reclaim as CPU-bound |
| The compressed swap cache (`zswap`) | Reports whether it is enabled and what share of RAM it may hold, and never changes that share |
| Compressed and disk swap together | Reports every area and calls the layout compressed, because at least one path compresses |
| `systemd-oomd` installed | Reports that it remains the OOM manager, and reports kernel OOM kills rather than acting on them |
| Per workload swap cap | Reports the cap, or that swap is disabled for that workload, or that the limit is unlimited, as its cgroup says |

## Degraded operation

| Condition | Report |
| --- | --- |
| `/proc/swaps` unreadable | "the swap table could not be read, so whether this host has swap is unknown". Nothing is inferred from the absence |
| `/proc/vmstat` unreadable | The swap rate and the OOM kill count are unknown, not zero |
| No zswap parameters | No compressed cache section at all, rather than a section claiming it is off |
| `memory.swap.max` absent or unreadable | The limit is `unknown`, explained as such. It is never shown as a cap of zero, which would read as "swap is disabled" |
| `memory.swap.max` of `max` | Reported as unlimited, which is the kernel default |
| `memory.swap.max` of `0` | Reported as "may not swap at all" |

A missing reading and a reading that says there is nothing are kept apart
everywhere, which is the same rule the rest of the telemetry follows.

## Protocol

The swap section was added in IPC minor version 2. It is defaulted on read, so a
client of the previous minor can still decode a report from this build, and a
report written before the addition still decodes here. Nothing was removed or
renumbered.
