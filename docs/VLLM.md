# vLLM: partial observation and restoration contract (RAM-14)

This adapter reads only `GET /is_sleeping` on an explicitly configured endpoint.
404/501, denied access, worker failure, malformed JSON, and missing responses
clear the previous reading. It never enables development mode, changes launch
arguments, or sends a sleep/wake request. Sleep state is not proof of idle
requests or valid restored weights. Activity and memory remain unknown.

The upstream developer documentation read on 2026-10-11 (page dated October 9)
requires operator-managed `VLLM_SERVER_DEV_MODE=1` and `--enable-sleep-mode` for
online administration. Those endpoints must not be exposed to users. No server
version or CUDA/ROCm deployment has been tested here.

Level 1 offloads weights into host RAM and discards KV cache. It is not host-RAM
relief. Level 2 discards weights but may retain CPU buffers, selected parameter
backups, CUDA graph backups, and residual device allocations. Neither level
implies zero host or device memory. Independent before/after readings are needed.

After level 2, ordered restoration is: allocate weights, collectively reload
weights, allocate KV cache, then verify inference. The `RestoreState` contract
rejects incomplete, out-of-order, and failed-worker evidence. No request may be
served from incomplete restoration. This contract is not wired into a real
transport or gateway and its verification input is not fabricated by the adapter.

RAM-14 remains doing. Still required: request-lease drain and dispatch integration,
bounded multi-step operations and confirmations, disabled-sleep and live-stream
integration, worker failure handling on a real deployment, successful inference
after restore, exact tested version/backend, and independent memory measurement.
The lease-gated control path is blocked on RAM-11 request tracking (doing) and
the RAM-45 gateway that feeds it, so no sleep is driven until those land.
