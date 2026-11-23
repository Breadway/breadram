# breadram: bread event integration

breadram is a standalone memory manager: it works exactly the same with or
without `breadd` running. When breadd *is* present and reports a compatible
1.x API at or above `1.7.0`, breadram publishes bounded pressure readings and
honors a small set of commands. See the parent `bread` repo's
`Documentation.md`, specifically its "Namespaces" and "Integrating a
bread\* app" sections, for the general convention this follows.

App id: **`ram`**. Transport: `bread-utils`'s `bread_client` module
(feature `bread-client`). The command subscription runs on its own
reconnecting background thread. The bus is rechecked at most every five
seconds, so a daemon that appears or restarts is picked up without a
breadram restart.

Events and commands use schema version 1. A command is
`bread.command.ram.<verb>` carrying `version` and `request_id`. Completion is
`bread.ram.<verb>.done` or `.failed` carrying the same request id.

## Events published (`bread.ram.*`)

| Event | Data | When |
|-------|------|------|
| `bread.ram.pressure` | `{ "version": 1, "state": "healthy" \| "recovering" \| "pressured" \| "critical", "mode": "monitor" \| "automatic", "available_bytes": <u64>, "observed_at_ms": <u64> }` | Each monitor tick while the bus is present and compatible. Publishing is best effort: a missing or slow bus means no event, not a failure. |
| `bread.ram.status.done` | `{ "version": 1, "request_id": "<id>", "detail": "reported" }` | A well-formed `bread.command.ram.status` was accepted. The current reading is the `bread.ram.pressure` event, not this confirmation. |
| `bread.ram.status.failed` | `{ "version": 1, "request_id": "<id>", "detail": "<message>" }` | A `status` command was malformed or unsupported. |
| `bread.ram.mode.done` | `{ "version": 1, "request_id": "<id>", "detail": "mode applied" }` | A `mode` command named `monitor` or `automatic` and the in-memory mode was changed. |
| `bread.ram.mode.failed` | `{ "version": 1, "request_id": "<id>", "detail": "<message>" }` | A `mode` command was malformed or named an unknown mode. |
| `bread.ram.action.done` | `{ "version": 1, "request_id": "<id>", "detail": "action accepted" }` | An `action` command named `unload_idle_model` or `cgroup_soft_limit` and was accepted. Acceptance does not mean the planner carried it out. |
| `bread.ram.action.failed` | `{ "version": 1, "request_id": "<id>", "detail": "<message>" }` | An `action` command was malformed or named an unsupported kind. |

A `.done` emit is not a confirmation. The command stays pending until a
subscriber echoes the same request id on `bread.ram.<verb>.done` or
`.failed`, or five seconds pass. No subscriber is a timeout, not a success.
A publish acknowledgement or a late reply is not success.

## Commands honored (`bread.command.ram.*`)

| Verb | Data | Effect |
|------|------|--------|
| `status` | `{ "version": 1, "request_id": "<id>" }` | Reports that the current pressure reading was published and emits `bread.ram.status.done` / `.failed`. |
| `mode` | `{ "version": 1, "request_id": "<id>", "mode": "monitor" \| "automatic" }` | Changes the running management mode in memory only. It does not rewrite the configuration file; a restart returns to the mode in that file. Emits `bread.ram.mode.done` / `.failed`. |
| `action` | `{ "version": 1, "request_id": "<id>", "kind": "unload_idle_model" \| "cgroup_soft_limit" }` | Validates and accepts the kind. Carrying it out still belongs to the planner, which only acts in automatic mode. Emits `bread.ram.action.done` / `.failed`. |

Every payload MUST contain `version: 1` and `request_id` (1-128 ASCII
alphanumeric, underscore, or hyphen bytes). A duplicate, empty, or oversized
id, an unsupported version, an unknown mode or kind, or a command that
produces nothing within five seconds is a failure.

## Not implemented

- No command verb other than `status`, `mode`, and `action`. In particular,
  no verb runs a program or names a target by executable path.
- Action commands are validated and answered here, but execution goes
  through the planner and only happens in automatic mode. The event bus does
  not carry out memory actions directly.
- No payload carries capabilities, permissions, prompt text, environment
  variables, or credentials.
- No event reports individual model unloads or cgroup changes. The bus
  reports pressure, mode, and command outcomes only.

## Fail-safe behavior

- If breadd is absent, slow, or speaks a 0.x or 2.x API, every call degrades
  to doing nothing. Standalone IPC remains the interface and memory
  management never depends on bread.
- The bus is rechecked at most every five seconds. A bus that disappears does
  not stop monitoring; a bus that appears later is picked up without a
  breadram restart.
- The command subscription reconnects with its own backoff loop. A dropped
  subscription does not affect memory management.
- A publish that nobody receives is a timeout, not an acknowledgement. The
  same request id is never run twice, and a late confirmation does not
  complete a command that already expired.
