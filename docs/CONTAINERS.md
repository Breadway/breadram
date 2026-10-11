# Host-visible container grouping (RAM-16, partial)

Inventory recognizes canonical Docker and libpod scope IDs and Docker cgroupfs
IDs, using host `/proc` paths only. Nested worker groups collapse to the recognized
container boundary. Inclusive `memory.current` is read once there, avoiding a
second charge for a worker subgroup. No container PID is translated into a host
PID, no runtime command runs, and no Docker socket or root group is required.
Unreadable accounting falls back to explicitly estimated RSS, not measured zero.

The group's effective memory limit (`memory.max`) is read from the same
host-visible path and reported as `limit_bytes`, so a container's own ceiling is
real data distinct from host `MemAvailable`. `max` (no cap) and an unreadable
file both report no limit rather than a fabricated zero.

This does not grant control permission. System-service writes still require an
authorized control implementation, which currently supports user systemd units
only. Endpoint locality remains configuration validation's responsibility;
remote automation is refused.

## Verification

Unit fixtures cover nested Docker and libpod accounting with host process IDs,
the container boundary's `memory.max` (nested workers do not shadow it), an
uncapped group reporting no limit rather than zero, an unreadable cgroup falling
back to an estimated RSS with no fabricated limit (limited permissions through
the public inventory path), and a reused container path producing a new
generation rather than the workload that used to be there (container
replacement).

`crates/breadram-core/tests/container_integration.rs` is gated end to end: set
`BREADRAM_CONTAINER_TESTS=1` to run it. It starts one short-lived memory-limited
rootless container and asserts the inventory groups it once at its boundary and
reports the container's `memory.max`, and it starts a transient systemd user
service and asserts the inventory groups it once at `unit:<name>.service`. Each
case prints a reason and skips when its flag, runtime, or session bus is absent,
so the CPU-only suite needs no container runtime or user session.

Remaining acceptance: arbitrary nested service boundary accounting and
explicitly authorized system-service controls (which the control implementation
owns). RAM-16 is a host-visible grouping and reporting card, not a Docker/Podman
compatibility certification.