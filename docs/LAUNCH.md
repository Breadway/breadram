# Launching a workload into a managed scope (RAM-20)

`breadram launch --name <name> -- <absolute-program> [args...]` replaces the
current process with the program inside the user scope
`breadram-launch-<name>.scope`.

The scope is created with `systemd-run --user --scope --collect`. Forked
children stay in that scope because cgroup membership is inherited. The
command does not open the breadram socket, so a missing daemon does not block
the launch.

When `XDG_RUNTIME_DIR` is unset or `systemd-run` is not on `PATH`, the program
runs directly in the caller's cgroup and the fallback is printed on stderr.
That is a visible degradation, not a failure.

Moving an already-running process into a scope is not offered. A cgroup move
does not take the memory the process has already been charged for, so correct
grouping starts at launch. Existing user units that breadram already discovered
stay manageable through the control path; this wrapper does not migrate them.

The program must be an absolute path. Arguments after `--` are passed as-is,
with no shell. The scope name is 1 to 64 ASCII letters, digits, `-`, or `_`.

## Not in this change

breadbox does not yet hand a launch to this wrapper. A desktop app started
from breadbox still lands in breadbox's own scope until that handoff exists.
The wrapper here is the standalone path that handoff can call.
