# Optional bread integration: pending transport (RAM-17)

No bread event is emitted or command honored by this build yet. Standalone IPC
remains the actual interface. The core has bounded correlation and timeout
tracking with restricted request IDs and exact command namespace validation.

Planned schema version 1 commands: `bread.command.ram.status`, `.mode`, `.action`.
Each payload must contain `version: 1` and `request_id` (1-128 ASCII alphanumeric,
underscore or hyphen bytes). Mode adds a validated management mode; action adds
an authorized stable target and supported action kind. Completion is a matching
`bread.ram.<verb>.done` or `.failed`, with the same version and request ID. A
publish acknowledgement, no listener, timeout, duplicate ID, or late reply is
not successful execution. No arbitrary command/program field is allowed.

Remaining acceptance: register `ram` in bread's current KNOWN_APPS, identify a
released bread-utils revision containing the current client API, wire nonblocking
daemon publication/subscription and reconnect, specify bounded pressure/capability
and action payloads with units, test unsupported API versions and actual bus
disconnects, and coordinate the cross-repo PR. Documentation.md lines 1600-1613
is the reference read for this work, not historical memory. No registry change
or released dependency support is claimed here.
