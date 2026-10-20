# Breadsearch model lifecycle (RAM-18)

## Audit: what breadmill actually owns

breadmill (breadsearch `main`, audited at the `model-unload-verbs` branch)
holds one embedding model, nomic-embed-text-v1.5 as ONNX, loaded once at
startup on a background thread into `SharedState.embedder:
Mutex<Option<OrtEmbedder>>` with a `model_ready` flag. Index data lives in
SQLite plus saved vectors; configuration and model files live on disk; none
of those move on release.

- **Ownership.** The session is the only disposable allocation of note. Before
  the unload verbs, nothing at runtime could drop it: no IPC verb, no signal,
  no config watch. The indexer's `None => break` arm was defensive code for a
  state that could not happen.
- **In-flight work.** The socket server spawns a thread per connection. There
  was no in-flight counter: `pending` counts files awaiting indexing, not
  queries. Indexing takes the same embedder mutex per chunk, so an unload
  races safely with it (the next chunk sees no model and stops, keeping every
  chunk already stored). Queries held no lock across the whole request in a way
  that forbids unload, but yanking the model under a running search turns it
  into an error, so unload is refused while queries run.
- **Backend allocations.** The ONNX Runtime session plus the configured
  execution provider (CPU/NPU/ROCm/CUDA/OpenVINO). GPU providers compile per
  shape with a JIT cache and can take minutes on first load; the compiled
  blobs persist per backend, so a reload is faster than a first load. The
  model file (547,310,275 bytes on this host) is memory-mapped, so its size
  is an estimate of model bytes, never a measurement of host RAM.
- **Reload.** Restarting the daemon reloads at startup. There was no hot
  reload. The verbs below add one: an explicit background load, plus a
  query-triggered demand reload.

## The contract (breadmill `feature/model-unload-verbs`)

Socket verbs, newline JSON like the rest of the protocol:

- `unload_model` → `ok` drops the session and clears `model_ready`, or
  `error: busy: …` while queries run or a load runs. Configuration, model
  files, and indexed chunks are untouched.
- `load_model` → `ok` starts a background load when needed (idempotent while
  one runs); readiness is polled through `Status`, never waited out.
- `query` with no model starts a background reload and answers
  `model loading — retry shortly`. The search that triggers the reload
  retries; the next finds a warm model. Missing model files still answer
  `model not ready — run breadmill --fetch-model`.
- `status` gains `in_flight`, `loading`, `model_bytes`, and `version`. Every
  new field is optional, so older daemons still parse; absence of `version`
  is exactly how an older daemon is told apart.

An older daemon answers an unknown verb with a parse error, which changes
nothing. breadram offers no unload to one but keeps observing it.

## The adapter (this repo)

`BreadsearchAdapter` follows the hook structure with a `SocketRunner` trait
doing the I/O: `status` refreshes observation every pass, `unload` carries
`UnloadIdleModel`, and a fresh `status` with the model gone confirms it. The
executor reaches sockets through `breadsearch:<socket path>:<verb>` URLs that
stay under the configured endpoint, and refuses socket work with no runner,
like hooks. Operation relief is the reported model size, stated as an
estimate; device effect is unmeasured and reported as none; the cold-start
cost stays in the operation note.

Activity is `Active` while queries run, files await indexing, or a load
runs; fresh and quiet is `Idle` with the idle-since instant the policy
debounces; stale is `Unknown`, which never unloads.

A confirmed unload stays idempotent while the model remains absent or its
state is unknown. Once fresh status positively reports a resident model again,
the provider executor can prove the old unload effect absent, allowing another
unload only through the normal target, capability, idle-grace and policy
cooldown gates. This narrowly scoped rearming applies to breadmill; it does not
reset generic cgroup duplicate guards or infer reload from missing data.

## Measured on this host

Real breadmill (CPU backend, empty index roots, isolated state and socket):

- `status` before: `model_ready:true`, 881,316 KiB RSS.
- `unload_model` → `ok`; `status` after: `model_ready:false`.
- RSS with the model gone: 486,424 KiB.
- `query` while gone → `model loading — retry shortly`, `loading:true`.
- About 20 s later: `model_ready:true`; a query embeds and searches.
- Explicit `load_model` → `ok`, then a query returns hits.

The release freed roughly 400 MB of resident memory on this host with an
empty index; a loaded production index holds more. Reload on CPU took under
20 s; GPU backends pay their compile on first load per the backend notes in
breadmill's embed module.

## Tests

- `adapter::breadsearch` unit tests: status parsing (new, old, garbage,
  mistagged), URL shaping, refresh (idle, busy, indexing, absent), and unload
  offered only when idle, ready, new, and automated.
- `adapter::executor` socket tests: apply plus confirm, busy denial,
  no-runner refusal, and unreachable-as-disconnect.
- `tests/breadsearch_adapter.rs`: a scripted breadmill over a real socket
  with the real runner covers busy refusal, release with confirmation,
  demand reload from absent to ready, failed reload staying absent and
  unoffered, and breadsearch uninstalled as ordinary and non-blocking.
