#!/usr/bin/env bash
# scripts/hil-firewall.sh — temporary firewall helper for HIL testing.
#
# Opens inbound firewall access to every port that `hil-host --print-ports`
# emits (each line carries its own `/udp` or `/tcp` protocol), plus audio's
# TCP 7380, then reverts on exit.  Targets firewalld on Fedora.
#
# Usage:
#   hil-firewall.sh open   — open the ports, leave them open, print them, exit 0
#   hil-firewall.sh close  — remove exactly the rules `open` would add, exit 0
#   hil-firewall.sh run    — open → run `make hil-test` → revert on any exit
#
# Port discovery:
#   The port set is derived entirely from
#   `cargo run -p hil-host --release -- --print-ports` (run from firmware/),
#   which emits one `<name>_port=<value>/<proto>` line per port.  The script
#   opens every such line, so a new hil-host port is picked up automatically —
#   with its protocol — and needs no edit here.  `cargo run` rebuilds hil-host
#   on demand, so the ports queried always match the binary that will run.
#   Audio's TCP 7380 is the one script-held constant (out of scope to change).
#
# Privilege:
#   firewall-cmd calls are issued via `sudo`.  hil-host itself (cargo run) is
#   invoked as the current (non-root) user.
#
# Idempotence:
#   Before adding a port, the script checks whether it is already open.  Ports
#   already open before this script ran are left open on revert — only ports
#   this script added are removed.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIRMWARE_DIR="$SCRIPT_DIR/../firmware"

# Audio receiver port — the one script-held constant (out of scope to change).
AUDIO_TCP_PORT=7380

# The resolved port set: one `port|proto` entry per port, populated by
# resolve_ports.  Declared empty at global scope so the EXIT-trap cleanup can
# iterate it safely even if resolve_ports never ran (set -u would otherwise
# abort on an unset array).  This single list is the sole source of truth every
# open/close/revert site loops over — no per-port hand-threading.
PORT_SPECS=()

# opened["<port>/<proto>"]=1 iff this run added that firewall rule (so revert
# closes exactly what it opened, and pre-open ports are left as-is).
declare -A opened=()

# Set to 1 after cleanup runs so the EXIT trap after an INT doesn't double-revert.
_cleanup_done=0

# ---------------------------------------------------------------------------
# resolve_ports: populate PORT_SPECS from `hil-host --print-ports` plus the
#   audio constant.  Each print-ports line is `<name>_port=<value>/<proto>`.
# ---------------------------------------------------------------------------
resolve_ports() {
    local print_ports_out

    # Always use `cargo run` so hil-host is rebuilt on demand.  This ensures
    # the ports queried here match the binary that will actually run — stale
    # prebuilt binaries cannot silently produce mismatched port values.
    local runner=(cargo run -p hil-host --release --)

    # Capture stdout+stderr together; check exit code explicitly.
    # Under set -e, `var="$(cmd)"` does NOT abort on a non-zero exit from cmd
    # (bash exception: command-substitution assignments don't trigger set -e).
    # Using `if !` makes the exit-code check unconditional and surfaces cargo
    # build/runtime errors via the captured output rather than silencing them.
    if ! print_ports_out="$(cd "$FIRMWARE_DIR" && "${runner[@]}" --print-ports 2>&1)"; then
        echo "ERROR: hil-host --print-ports failed:" >&2
        printf '%s\n' "$print_ports_out" >&2
        exit 1
    fi

    # Parse every `<name>_port=<value>/<proto>` line into PORT_SPECS.  The regex
    # both selects real port lines (ignoring cargo's build chatter on stderr) and
    # validates the shape: an alphanumeric label, a numeric value, and a proto of
    # exactly udp or tcp — so the value passed to sudo firewall-cmd is trusted
    # independent of hil-host's output discipline.
    local line label value proto
    while IFS= read -r line; do
        if [[ "$line" =~ ^([a-z0-9_]+)_port=([0-9]+)/(udp|tcp)$ ]]; then
            label="${BASH_REMATCH[1]}"
            value="${BASH_REMATCH[2]}"
            proto="${BASH_REMATCH[3]}"
            if (( value < 1 || value > 65535 )); then
                echo "ERROR: hil-host --print-ports returned out-of-range ${label} port: '${value}'" >&2
                exit 1
            fi
            PORT_SPECS+=("${value}|${proto}")
        fi
    done <<< "$print_ports_out"

    if (( ${#PORT_SPECS[@]} == 0 )); then
        echo "ERROR: hil-host --print-ports produced no recognizable <name>_port=<value>/<proto> lines:" >&2
        printf '%s\n' "$print_ports_out" >&2
        exit 1
    fi

    # Audio's TCP port is the one constant the discovery does not cover.
    PORT_SPECS+=("${AUDIO_TCP_PORT}|tcp")
}

# ---------------------------------------------------------------------------
# Preflight: check that firewall-cmd and sudo are available.
# ---------------------------------------------------------------------------
preflight() {
    if ! command -v firewall-cmd &>/dev/null; then
        echo "ERROR: firewall-cmd not found — is firewalld installed?" >&2
        exit 1
    fi
    if ! sudo -n firewall-cmd --state &>/dev/null; then
        # Try a non-silent sudo to let the user authenticate, then verify.
        echo "Checking firewalld state (sudo may prompt for your password)..."
        if ! sudo firewall-cmd --state &>/dev/null; then
            echo "ERROR: firewalld is not running or sudo access to firewall-cmd failed." >&2
            exit 1
        fi
    fi
}

# ---------------------------------------------------------------------------
# open_port <port> <proto>
#   Opens the port if not already open, recording opened[port/proto]=1 only when
#   this run actually added the rule (so revert closes exactly what it opened).
# ---------------------------------------------------------------------------
open_port() {
    local port="$1" proto="$2"
    if sudo firewall-cmd --query-port="${port}/${proto}" &>/dev/null; then
        echo "  ${port}/${proto}: already open (will not close on revert)"
    else
        sudo firewall-cmd --add-port="${port}/${proto}"
        echo "  ${port}/${proto}: opened"
        opened["${port}/${proto}"]=1
    fi
}

# ---------------------------------------------------------------------------
# close_port <port> <proto>
#   Removes the port only if this run opened it (opened[port/proto]==1).
# ---------------------------------------------------------------------------
close_port() {
    local port="$1" proto="$2"
    if [[ "${opened["${port}/${proto}"]:-0}" == "1" ]]; then
        # Call --remove-port directly; firewall-cmd --remove-port is idempotent
        # (exits 0 even if the port was already absent), so no pre-query needed.
        if sudo firewall-cmd --remove-port="${port}/${proto}" &>/dev/null; then
            echo "  ${port}/${proto}: closed"
        else
            echo "  ${port}/${proto}: remove-port failed (firewalld error?)" >&2
        fi
    else
        echo "  ${port}/${proto}: was pre-open; left as-is"
    fi
}

# ---------------------------------------------------------------------------
# for_each_port <fn>: call `fn <port> <proto>` for every resolved spec.  The
#   length guard keeps `set -u` from aborting on an empty PORT_SPECS (cleanup
#   may run before resolve_ports populated it).
# ---------------------------------------------------------------------------
for_each_port() {
    local fn="$1" spec port proto
    (( ${#PORT_SPECS[@]} )) || return 0
    for spec in "${PORT_SPECS[@]}"; do
        IFS='|' read -r port proto <<< "$spec"
        "$fn" "$port" "$proto"
    done
}

# ---------------------------------------------------------------------------
# do_open: open every resolved port.
# ---------------------------------------------------------------------------
do_open() {
    preflight
    resolve_ports
    echo "Opening firewall ports:"
    for_each_port open_port
    echo "Firewall ports open."
}

# ---------------------------------------------------------------------------
# do_close: remove exactly the ports do_open would add.
#   For `close` subcommand: resolve ports fresh, mark all as opened, remove.
# ---------------------------------------------------------------------------
do_close() {
    preflight
    resolve_ports
    local spec port proto
    for spec in "${PORT_SPECS[@]}"; do
        IFS='|' read -r port proto <<< "$spec"
        opened["${port}/${proto}"]=1
    done
    echo "Closing firewall ports:"
    for_each_port close_port
    echo "Firewall ports closed."
}

# ---------------------------------------------------------------------------
# cleanup: idempotent revert used by trap and post-run.
# ---------------------------------------------------------------------------
cleanup() {
    if [[ "$_cleanup_done" == "1" ]]; then
        return
    fi
    _cleanup_done=1
    echo ""
    echo "Reverting firewall:"
    for_each_port close_port
    echo "Firewall reverted."
}

# ---------------------------------------------------------------------------
# Subcommand dispatch
# ---------------------------------------------------------------------------
SUBCMD="${1:-}"

case "$SUBCMD" in
    open)
        do_open
        echo ""
        echo "NOTE: ports are open until you run: $(basename "$0") close"
        echo "      (runtime rules clear on the next firewalld reload as a backstop)"
        ;;

    close)
        do_close
        ;;

    run)
        # Install the cleanup trap BEFORE do_open so that a mid-open failure
        # (e.g., the second or third firewall-cmd --add-port fails under set -e)
        # still reverts any ports that were already opened.  cleanup() is safe to
        # call with an empty or partial `opened` map: close_port only removes a
        # port whose opened[port/proto] flag is 1, which is set only after a
        # successful --add-port, and for_each_port no-ops when PORT_SPECS is
        # still empty (resolve_ports has not run yet).
        trap 'cleanup' EXIT
        trap 'cleanup; trap - INT; kill -INT $$' INT
        trap 'cleanup; trap - TERM; kill -TERM $$' TERM

        do_open

        # Run the HIL test in the foreground (same process group).
        # On Ctrl-C, SIGINT reaches the entire process group, so make/cargo/
        # hil-host receive it too and tear down before control returns here.
        # Capture exit status; cleanup runs via trap after make returns.
        hil_exit=0
        make -C "$FIRMWARE_DIR" hil-test || hil_exit=$?

        # Revert explicitly now (before EXIT trap fires) so we can log in order.
        cleanup

        exit $hil_exit
        ;;

    *)
        echo "Usage: $(basename "$0") open | close | run" >&2
        echo "" >&2
        echo "  open   Open the HIL firewall ports and exit (manual bracket)." >&2
        echo "  close  Remove the HIL firewall ports and exit (manual bracket)." >&2
        echo "  run    Open ports, run 'make hil-test', revert on any exit." >&2
        exit 1
        ;;
esac
