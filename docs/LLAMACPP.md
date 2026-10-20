# llama.cpp adapter (RAM-13)

This build observes configured servers. Its low-level adapter implements the
documented `POST /models/unload` API with exact model-id/status confirmation,
but **daemon-driven unload of a loaded router model is currently unavailable**.
Loaded residency reports unknown activity, and neither the adapter nor the
daemon supplies a complete idle signal for this provider. Policy and executor
both require confirmed idleness; enabling automation never bypasses them.
Loading is left to the router's own demand-driven behaviour or to an explicit
`POST /models/load`, both verified below against a real server.

## Observation surface

Discovery sends only `GET /health`, `GET /models`, and `GET /props`. These are on
the upstream README's idle-timer exemption list: they do not reset the native
sleep timer or reload a sleeping model on builds implementing that contract.
`/v1/models` and `/slots` are deliberately not probed because neither is listed
as exempt. Older builds have not been verified to implement that guarantee.

A parseable `/models` response supplies router IDs and statuses. If that surface
returns 404, 405, or 501, a properties response with a nonempty `model_path` and
an explicit `is_sleeping` supplies single-model inventory. Each refresh replaces
the previous observation. Failed, unauthorized, malformed, missing, or props-only
answers cannot make old router inventory current. Mode is re-evaluated per pass.

Loaded residency is not measured activity: it reports unknown. Loading and
downloading report active. Missing or unrecognized statuses report unknown, not
unloaded or failed. A complete current list containing only unloaded or sleeping
models reports idle; an explicit single-model sleep state also reports idle.
Failed load and failed observation never prove idle.

## Administration

The router documents `POST /models/load` and `POST /models/unload`. When
automation is enabled, the low-level adapter can construct the documented
unload operation for one loaded router model. This is not sufficient for daemon
execution: loaded models remain activity-unknown and fail the idle gate.
If an operation is exercised directly in adapter conformance tests, its effect
is confirmed by the structured predicate: the exact
model id must carry the `unloaded` status in the same `/models` entry, so a
sibling model with a shared substring cannot cross-confirm it. The old
body-substring confirmation is not used for this. Loading is left to the router's
own demand-driven behaviour or to an explicit load call. An unloaded status is
a logical state, not a measurement of physical memory reclaimed, and request
tracking remains the condition for treating a resident model as idle.

Single-model deployments document automatic `--sleep-idle-seconds`, not an
immediate manual sleep API. This adapter observes native sleep without changing
launch arguments. Native wake-on-request is left untouched. No `/tools` endpoint
is used.

Host RAM and VRAM remain independently unknown in adapter inventory. Model file
size is not RSS or cgroup usage, especially with mmap and shared page cache:
the verified deployment below held 5.2GB of weights at 60MB of host RSS with
the remainder in iGPU shared memory. The adapter reports what the server API
gives it, which for router models is neither figure, so unknown stays unknown
rather than becoming a fabricated zero.

## Configuration

```toml
[[providers]]
name = "llama"
kind = "llama_cpp"
url = "http://127.0.0.1:8080"
scope = "unit:llama.service"
```

Generic permitted Linux controls remain available without a provider. Registry
diagnostics state whether router administration is available: unloaded only
at the protocol level when automation enables the UnloadModel capability.
Runtime diagnostics explicitly state the missing activity gate. The automatic
loaded-model relief path must remain unavailable until trustworthy request
tracking is implemented; gateway in-flight leases alone only prove activity,
not complete idleness between requests. Generic controls remain available.

## Verified against a real server

Host `llama-server` at `/usr/bin/llama-server`, version 0.5.0-dev (build
11146, commit 7fe450e193), run in router mode on loopback with a single model:
Qwen3 8B Q4_K_M (5.2GB weights, sourced from the local Ollama blob store).
Hermes has an integrated AMD GPU, and this build offloads there, so the relief
below is almost entirely shared device memory rather than host RSS.

- Probes: `GET /health` answers `{"status":"ok"}`; `GET /models` lists the
  router id with a nested `status.value` field; `GET /props` reports the router
  role. All three are the adapter's exact probe set.
- Inference: `POST /v1/chat/completions` completes against the model (a
  reasoning model: substantive output arrives on the reasoning channel). Status
  becomes `loaded`.
- Unload: `POST /models/unload` with `{"model":"qwen3-8b"}` answers
  `{"success":true}`; the same `/models` entry flips to `unloaded`; the model
  subprocess is reaped. This is the exact request shape and confirmation
  predicate the adapter drives.
- Memory, measured independently of the adapter: host RSS of the model process
  60MB before unload against 9.8GB of mapped address space; integrated GPU
  shared memory 9.2GB in use with the model loaded against 0.5GB without it,
  about 8.7GB reclaimed per unload cycle. A second load settled near 30.3GB
  with full default context allocation (4 slots at 23k context in fp16 cache
  account for roughly 13GB of that before fragmentation), so context sizing,
  not weight size, dominates device cost on this build.
- Inference after relief: a completion issued at `unloaded` state reloads the
  model and succeeds, with status back to `loaded`. Native wake-on-request is
  preserved end to end.
- Load: `POST /models/load` with `{"model":"qwen3-8b"}` answers
  `{"success":true}` with status moving through `loading` to `loaded`.

The upstream README is reference material; the list above is experimental
verification on the stated build and model.
