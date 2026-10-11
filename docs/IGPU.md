# Integrated GPU shared memory (RAM-67, first piece)

On an integrated GPU the "device" memory is not dedicated RAM: the driver maps
pages into the graphics translation table (GTT) out of the same physical memory
the kernel already accounts. breadram therefore reports it separately and never
folds it into host available memory, and never confuses it with dedicated VRAM
on a discrete card.

The counters are read, read-only and bounded, from the drm class:

- `/sys/class/drm/card<N>/device/mem_info_gtt_used`
- `/sys/class/drm/card<N>/device/mem_info_gtt_total`

Cards are scanned from `card0` to a bound, and a card with neither counter is not
an integrated GPU. The figures are summed across cards, reported as
`gtt_used_bytes` and `gtt_total_bytes` in the memory breakdown of `breadram
diagnostics` (protocol minor 7, both defaulted), shown in the human output, and
exposed as the `gpu_gtt` capability.

Degradation is explicit: an unreadable drm class reports the capability
unavailable and the figure unknown, a card with a total but no used count reports
the capability degraded, and a host with no iGPU reports unavailable with a
reason. None of these is a fabricated zero. A model on an iGPU is holding host
RAM, which is what makes this matter for a relief decision.

## Verified live

On this host (AMD integrated GPU, card1, vendor 0x1002, amdgpu), `breadramd
--mode monitor` plus `breadram diagnostics` reported GTT used 7.5 GiB of 30.6
GiB, the `gpu_gtt` capability available, and the note above. The counter is
driver dependent: i915 and xe also expose `mem_info_gtt_*`; a host with neither
reports unknown, not zero.

## Inference attribution

A provider that runs on an integrated GPU is marked in the configuration:

```toml
[[providers]]
name = "ollama"
kind = "ollama"
url = "http://127.0.0.1:11434"
scope = "unit:ollama.service"
igpu = true
```

With `igpu = true`, the adapter attributes a loaded model's reported device
memory to host RAM, because on an iGPU it is a carve-out of system memory rather
than dedicated VRAM, and it reports no separate device figure. Reporting it as
device memory would hide where it really lives and could let a relief decision
treat the host as holding less than it does. Off by default, which keeps the two
figures separate for a discrete GPU or a CPU-only deployment. The registry
records a note saying which way the endpoint was attributed.

## Counted once

The GTT figure is reported in its own field and is never added to or subtracted
from host available memory: on an iGPU it is a carve-out of the same host RAM, so
folding it in would count it twice. A daemon test asserts that the breakdown
copies the GTT figure and leaves `ram_available_bytes` exactly as the kernel
reported it, and the notes say the two are separate.

## Still to come on this card

- Exercise the reading on a non-amdgpu driver host (i915 or xe). The counters are
  driver dependent; this host is amdgpu and verified live. i915 and xe also
  expose `mem_info_gtt_*`, but that is reference material, not a reading taken
  here.