# Optional bread integration (RAM-17)

breadram publishes bounded events and honors a small set of commands, but only
while breadd is running and reports a 1.x API at or above `1.7.0` (the running
daemon reports `1.7.1`, current source is `1.8.0`). The client is `bread-utils`
`v0.8.0` with the `bread-client` feature. Every call degrades to doing nothing
when the bus is absent, slow, or speaks an older or 2.x version, so
standalone IPC stays the interface and memory management never depends on bread.

The bus is rechecked at most every five seconds, and the command subscription
reconnects on its own, so a bus that appears or restarts later is picked up
without restarting breadram.

## Events

Schema version 1. `bread.ram.pressure` carries `version`, `state` (healthy,
recovering, pressured, or critical), `mode`, `available_bytes`, and
`observed_at_ms`. Publishing is best effort: nobody listening is not a failure.

## Commands

`bread.command.ram.status`, `.mode`, and `.action`. Each payload must contain
`version: 1` and `request_id` (1-128 ASCII alphanumeric, underscore, or hyphen
bytes). Mode adds `mode` of `monitor` or `automatic`. Action adds `kind` of
`unload_idle_model` or `cgroup_soft_limit`, and nothing that names a program.

Completion is `bread.ram.<verb>.done` or `.failed` with the same version and
request id. That emit is not a confirmation. The command stays pending until a
subscriber echoes the same request id on `bread.ram.<verb>.done` or `.failed`,
or five seconds pass. A duplicate, empty, or oversized id, an unsupported
version, an unknown mode or kind, or a command that produces nothing within
five seconds is a failure. Nobody listening is a timeout, not a success. A
publish acknowledgement or a late reply is not success.

A mode command changes the running mode in memory only. It does not rewrite the
configuration file, and a restart returns to the mode in that file.

## Still outside this build

`ram` is not in bread's `KNOWN_APPS` on main. Until
https://git.breadway.dev/Breadway/bread/pulls/35 merges, the bus rejects
`bread.ram.*` and `bread.command.ram.*` as an unknown app. That registration
is a one-line change in the bread repo and is not merged here. Action commands are
validated and answered here; carrying one out still goes through the planner,
which only acts in automatic mode. No payload carries capabilities yet.
