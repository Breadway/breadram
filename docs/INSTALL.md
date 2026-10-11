# Installing breadram (RAM-31)

breadram ships as a systemd user service. It runs as the operator, needs no
root daemon, and treats breadsearch, breadd, BOS, a compositor, model runtimes,
and GPU libraries as optional: none of them are install requirements.

## Paths

| What | Where |
| ---- | ----- |
| Binaries (`breadramd`, `breadram`) | `~/.local/bin` by default |
| Unit file | `~/.config/systemd/user/breadram.service` |
| Configuration | `~/.config/breadram/breadram.toml` (mode 600) |
| Socket | `$XDG_RUNTIME_DIR/breadram.sock` (owner only) |
| Owned values across restarts | `$XDG_STATE_HOME/breadram/journal.json`, else `~/.local/state/breadram/journal.json` |

## Install

Install the binaries first, then run the installer:

```
cargo install --path crates/breadramd --root ~/.local
cargo install --path crates/breadram-cli --root ~/.local
./packaging/install.sh install
```

The installer copies the unit, installs the example configuration only when
none exists, reloads the user manager, and enables and starts the service. A
fresh install runs in `monitor` mode: it observes, explains, and never changes
system state. Rerunning the installer is safe and never overwrites the
configuration. With binaries elsewhere, point at them explicitly. The installer rewrites the
installed unit's `ExecStart` to that same path:

```
BIN=/usr/bin/breadramd ./packaging/install.sh install
```

## Enable, disable, upgrade, uninstall

```
systemctl --user stop breadram        # pause management, keep the unit
systemctl --user start breadram       # resume
systemctl --user disable --now breadram  # stop and do not start at login
./packaging/install.sh uninstall      # remove the unit, keep config and state
```

Upgrade by installing new binaries and restarting the service. The daemon
replaces a stale socket left by a crash instead of stealing a live one, and a
configuration that fails validation keeps the previous valid settings, so a
bad upgrade changes nothing halfway. Downgrades are safe in one direction:
a state file written by a newer build is reported and ignored, never
reinterpreted. To roll back fully, reinstall the older binaries and restart.

Uninstall preserves user files: the configuration and the state directory
stay, so a reinstall resumes where the operator left off. Remove them by hand
when that is the intent.

## Session exit, crash, and restart

- Session exit stops the user manager and the daemon with it. Owned cgroup
  values are restored on the next start in `automatic` mode; in `monitor`
  mode leftovers are reported, never silently cleared.
- A crash leaves the socket file behind. The next start probes it: a live
  daemon refuses a second instance, a stale file is replaced.
- After a restart, breadram restores only values it still owns. A value
  another manager changed meanwhile is left alone, and a target that no
  longer exists is skipped. The journal reports what the restore did.

## Hardening

The unit sets `NoNewPrivileges`, `ProtectSystem=strict`,
`ProtectControlGroups=yes`, and related options (see
`packaging/breadram.service`). They were chosen so the three things breadram
needs keep working: reading `/proc` and cgroupfs, talking to the user
manager for controls, and writing its socket and state. If a hardened option
ever blocks a platform, the failure surfaces as a degraded capability with a
reason, not a silent no-op.

## Headless and standalone

On a headless machine use the `headless-server` profile or the same install
steps over SSH; no compositor, bread app, or model runtime is required. The
`desktop` profile enables automatic relief that protects interactive work;
opt into it explicitly with `--profile desktop` or by setting
`mode = "automatic"` once monitoring looks right.

## Troubleshooting

- `breadram status` says the daemon is unavailable: check
  `systemctl --user status breadram`, then that `$XDG_RUNTIME_DIR/breadram.sock`
  exists, or pass `--socket` with the path the daemon logs at startup.
- `error: ... version ...`: the CLI and daemon speak a versioned protocol.
  A major mismatch fails with an actionable error; upgrade both binaries.
- The service fails right after editing the config: the file failed
  validation and the previous settings were kept. `journalctl --user -u
  breadram` shows the reason; fix the file and restart.
- `systemd-analyze verify ~/.config/systemd/user/breadram.service` checks
  the unit without starting anything.
