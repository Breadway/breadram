#!/bin/sh
# breadram user-service installer (RAM-31).
#
# Usage: install.sh [install|uninstall|status]
#
# install:   copies the unit file, installs the example configuration only
#            when none exists, reloads the user manager, and enables and
#            starts the service. Safe to rerun: it never overwrites an
#            existing configuration.
# uninstall: stops and disables the service and removes the unit file. It
#            leaves the configuration and the state directory alone, so user
#            files and a future reinstall survive.
# status:    prints whether the unit and the configuration are installed.
#
# The daemon binary itself is not built here: install it first (for example
# `cargo install --path crates/breadramd --root ~/.local`, or your bakery
# package), so that ~/.local/bin/breadramd exists. Override BIN to point at
# another location: BIN=/usr/bin/breadramd ./install.sh install.
set -eu

BIN="${BIN:-$HOME/.local/bin/breadramd}"
UNIT_SRC="$(dirname "$0")/breadram.service"
CONFIG_SRC="$(dirname "$0")/breadram.toml.example"
CONFIG_HOME="${XDG_CONFIG_HOME:-$HOME/.config}"
STATE_HOME="${XDG_STATE_HOME:-$HOME/.local/state}"
UNIT_DIR="$CONFIG_HOME/systemd/user"
CONFIG_DIR="$CONFIG_HOME/breadram"
STATE_DIR="$STATE_HOME/breadram"
CONFIG="$CONFIG_DIR/breadram.toml"

fail() {
    echo "install.sh: $1" >&2
    exit 1
}

need_systemctl() {
    command -v systemctl >/dev/null 2>&1 || fail "systemctl not found, is systemd running?"
}

do_install() {
    need_systemctl
    [ -x "$BIN" ] || fail "$BIN is not executable, install the breadramd binary first"
    # ReadWritePaths cannot create a missing mountpoint. The state directory
    # must exist before systemd applies the hardened namespace.
    mkdir -p "$UNIT_DIR" "$CONFIG_DIR" "$STATE_DIR"
    # The shipped unit uses the default binary path. An explicit BIN must be
    # reflected in the installed unit, otherwise installation checks one path
    # and systemd later executes another.
    if [ "$BIN" = "$HOME/.local/bin/breadramd" ]; then
        cp "$UNIT_SRC" "$UNIT_DIR/breadram.service"
    else
        sed "s|%h/.local/bin/breadramd|$BIN|" "$UNIT_SRC" > "$UNIT_DIR/breadram.service"
    fi
    if [ -e "$CONFIG" ]; then
        echo "keeping existing $CONFIG"
    else
        cp "$CONFIG_SRC" "$CONFIG"
        chmod 600 "$CONFIG"
        echo "installed example configuration to $CONFIG (monitor mode)"
    fi
    systemctl --user daemon-reload
    systemctl --user enable --now breadram.service
    systemctl --user --no-pager status breadram.service || true
}

do_uninstall() {
    need_systemctl
    systemctl --user disable --now breadram.service 2>/dev/null || true
    rm -f "$UNIT_DIR/breadram.service"
    systemctl --user daemon-reload
    echo "removed the unit; kept $CONFIG and state files"
}

do_status() {
    if [ -e "$UNIT_DIR/breadram.service" ]; then
        echo "unit: installed"
    else
        echo "unit: missing"
    fi
    if [ -e "$CONFIG" ]; then
        echo "config: $CONFIG"
    else
        echo "config: missing"
    fi
    if command -v systemctl >/dev/null 2>&1; then
        systemctl --user is-active breadram.service 2>/dev/null || echo "service: not running"
    fi
}

case "${1:-install}" in
install) do_install ;;
uninstall) do_uninstall ;;
status) do_status ;;
*) fail "usage: install.sh [install|uninstall|status]" ;;
esac
