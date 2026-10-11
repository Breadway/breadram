# breadram issue map

This is the full implementation map. It is tracked on breadboard project `RAM`
(https://breadboard.breadway.dev). RAM-1 is the start point; every other card
builds on the project contract it defines.

Workflow for every card:

- Move the card to `doing` when implementation actually starts.
- Work on a `feature/*` or `fix/*` branch off `main` and open one linked PR.
- Move the card to `review` once the implementation is complete and verified,
  not before.
- Move it to `done` only after it is verified, which is the reviewer's call.

Do not start a card whose blocking cards are unverified unless a documented
interface lets it proceed independently. Keeping the dependency order honest is
the point of the map.

## M1: foundation and standalone operation

- RAM-1: Define the project contract and bootstrap the Rust workspace (this card)
- RAM-2: Collect host and cgroup memory pressure telemetry (blocked by RAM-1)
- RAM-3: Identify workloads across processes, cgroups, and services (blocked by RAM-1, RAM-2)
- RAM-4: Implement breadramd lifecycle and versioned local IPC (blocked by RAM-1, RAM-5)
- RAM-5: Add validated configuration, profiles, and per-workload rules (blocked by RAM-1)

## M2: pressure policy and control

- RAM-6: Implement the pressure state machine and action planner (blocked by RAM-2, RAM-3, RAM-5)
- RAM-7: Execute actions with confirmations, history, and recovery (blocked by RAM-4, RAM-6)
- RAM-8: Build an optional, capability-based workload adapter interface (blocked by RAM-3, RAM-4, RAM-5)
- RAM-9: Implement authorized systemd and cgroup memory controls (blocked by RAM-3, RAM-4, RAM-5, RAM-7)
- RAM-10: Add the standalone CLI for status, rules, actions, and diagnostics (blocked by RAM-4, RAM-5, RAM-6, RAM-7, RAM-8)

## M3: workload compatibility

- RAM-11: Track inference activity and coordinate demand-driven wake (blocked by RAM-7, RAM-8)
- RAM-12: Add an Ollama inventory and idle-model unload adapter (blocked by RAM-7, RAM-8, RAM-11)
- RAM-13: Add a llama.cpp server and router adapter (blocked by RAM-7, RAM-8, RAM-11)
- RAM-14: Add a vLLM adapter with memory-aware sleep and wake (blocked by RAM-7, RAM-8, RAM-11)
- RAM-15: Add opt-in hooks for other runtimes and large applications (blocked by RAM-7, RAM-8)
- RAM-16: Support containerized and system-service workloads (blocked by RAM-3, RAM-8, RAM-9)
- RAM-17: Integrate optional bread events and command handling (blocked by RAM-4, RAM-7, RAM-8)
- RAM-18: Add an optional breadsearch model-release adapter (blocked by RAM-7, RAM-8, RAM-11, RAM-17)
- RAM-19: Verify the provider compatibility matrix and adapter contract (blocked by RAM-12, RAM-13, RAM-14, RAM-15, RAM-16, RAM-18)

## M4: application priorities and jobs

- RAM-20: Launch standalone and breadbox workloads into managed scopes (blocked by RAM-9, RAM-10, RAM-16)
- RAM-21: Apply protected, normal, and background workload priorities (blocked by RAM-3, RAM-5, RAM-9)
- RAM-22: Implement bounded automatic per-workload memory budgets (blocked by RAM-6, RAM-7, RAM-9, RAM-20, RAM-21)
- RAM-23: Diagnose swap, compression, and existing OOM management (blocked by RAM-2, RAM-9)
- RAM-24: Add pressure-aware admission for background jobs (blocked by RAM-6, RAM-9, RAM-10)
- RAM-25: Coordinate concurrency reduction and resume for opted-in jobs (blocked by RAM-7, RAM-8, RAM-24)

## M5: desktop integration

- RAM-26: Add breadbar pressure status and intervention notifications (blocked by RAM-10, RAM-17)
- RAM-27: Add the bos-settings memory management panel (blocked by RAM-5, RAM-10, RAM-22, RAM-23, RAM-24)
- RAM-28: Add bounded memory trends and workload growth diagnostics (blocked by RAM-2, RAM-3, RAM-7)

## M6: verification and delivery

- RAM-29: Build deterministic replay and isolated integration test tools (blocked by RAM-1)
- RAM-30: Measure efficacy, overhead, and inference recovery under load (blocked by RAM-12, RAM-13, RAM-14, RAM-19, RAM-22, RAM-25, RAM-28, RAM-29)
- RAM-31: Install breadram services with permissions and recovery (blocked by RAM-4, RAM-7, RAM-9, RAM-10)
- RAM-32: Add CI, bakery packaging, and ecosystem registration (blocked by RAM-1, RAM-29, RAM-31)
- RAM-33: Integrate into BOS and publish the operator and agent handoff (blocked by RAM-19, RAM-20, RAM-21, RAM-22, RAM-23, RAM-25, RAM-26, RAM-27, RAM-28, RAM-30, RAM-31, RAM-32)
