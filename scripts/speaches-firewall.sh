#!/usr/bin/env bash
# scripts/speaches-firewall.sh — temporary firewall helper for the speaches container.
#
# Opens inbound firewall access to the speaches HTTP API port so another host
# can reach this box's STT/TTS server, then reverts on exit.  Targets firewalld
# on Fedora.
#
# The container itself is already published on all interfaces: host/speaches-up.sh
# runs `podman run -p "$PORT:8000"`, and podman's default publish address is
# 0.0.0.0.  The firewall is therefore the only thing standing between the
# container and an external client — which is all this script touches.  It does
# not restart, reconfigure, or inspect the container.
#
# Usage:
#   speaches-firewall.sh open   — open the port, leave it open, print it, exit 0
#   speaches-firewall.sh close  — remove exactly the rule `open` would add, exit 0
#   speaches-firewall.sh run    — open, hold until interrupted, revert on exit
#
# Port discovery:
#   The port is read from the `PORT=` assignment in host/speaches-up.sh, so the
#   hole this script opens cannot drift from the port the container publishes.
#   Editing PORT in that one place is enough; there is no second constant here.
#
# Privilege:
#   firewall-cmd calls are issued via `sudo`.
#
# Scope:
#   Runtime rules only — no `--permanent`, no `--reload`.  The hole does not
#   survive a firewalld reload or a reboot, which is the intended blast radius
#   for a dev-cycle helper: forgetting to run `close` costs you a reload, not a
#   permanently exposed port.
#
# Idempotence:
#   Before adding the port, the script checks whether it is already open.  A
#   port already open before this script ran is left open on revert — only a
#   port this script added is removed.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPEACHES_UP="$SCRIPT_DIR/../host/speaches-up.sh"

PROTO=tcp

# The resolved port, populated by resolve_port.  Empty at global scope so the
# EXIT-trap cleanup can test it safely even if resolve_port never ran (set -u
# would otherwise abort on an unset variable).
PORT=""

# 1 iff this run added the firewall rule (so revert closes exactly what it
# opened, and a pre-open port is left as-is).
opened=0

# Set to 1 after cleanup runs so the EXIT trap after an INT doesn't double-revert.
_cleanup_done=0

# ---------------------------------------------------------------------------
# resolve_port: read PORT from host/speaches-up.sh.
#   Parsing the assignment rather than sourcing the file avoids executing a
#   script whose whole purpose is to tear down and restart a container.
# ---------------------------------------------------------------------------
resolve_port() {
    if [[ ! -f "$SPEACHES_UP" ]]; then
        echo "ERROR: cannot find speaches-up.sh at: $SPEACHES_UP" >&2
        exit 1
    fi

    # Match a bare `PORT=<digits>` assignment at column 0.  The regex both
    # selects the line and validates the shape, so the value handed to
    # sudo firewall-cmd is trusted independent of that script's formatting.
    local line
    while IFS= read -r line; do
        if [[ "$line" =~ ^PORT=([0-9]+)[[:space:]]*$ ]]; then
            PORT="${BASH_REMATCH[1]}"
            break
        fi
    done < "$SPEACHES_UP"

    if [[ -z "$PORT" ]]; then
        echo "ERROR: no 'PORT=<number>' assignment found in $SPEACHES_UP" >&2
        exit 1
    fi
    if (( PORT < 1 || PORT > 65535 )); then
        echo "ERROR: out-of-range PORT in $SPEACHES_UP: '$PORT'" >&2
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# Preflight: check that firewall-cmd and sudo access are available.
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
# open_port: open the port if not already open, recording opened=1 only when
#   this run actually added the rule.
# ---------------------------------------------------------------------------
open_port() {
    if sudo firewall-cmd --query-port="${PORT}/${PROTO}" &>/dev/null; then
        echo "  ${PORT}/${PROTO}: already open (will not close on revert)"
    else
        sudo firewall-cmd --add-port="${PORT}/${PROTO}"
        echo "  ${PORT}/${PROTO}: opened"
        opened=1
    fi
}

# ---------------------------------------------------------------------------
# close_port: remove the port only if this run opened it.
# ---------------------------------------------------------------------------
close_port() {
    [[ -n "$PORT" ]] || return 0
    if (( opened == 1 )); then
        # firewall-cmd --remove-port is idempotent (exits 0 even if the port is
        # already absent), so no pre-query is needed.
        if sudo firewall-cmd --remove-port="${PORT}/${PROTO}" &>/dev/null; then
            echo "  ${PORT}/${PROTO}: closed"
        else
            echo "  ${PORT}/${PROTO}: remove-port failed (firewalld error?)" >&2
        fi
    else
        echo "  ${PORT}/${PROTO}: was pre-open; left as-is"
    fi
}

# ---------------------------------------------------------------------------
# report_reach: print the LAN address a client on another host should target.
#   Best-effort: the route lookup is informational, so a failure downgrades to
#   a hint rather than aborting a successful open.
# ---------------------------------------------------------------------------
report_reach() {
    local ip
    ip="$(ip -4 -o route get 1.1.1.1 2>/dev/null | sed -n 's/.* src \([0-9.]*\).*/\1/p')"
    if [[ -n "$ip" ]]; then
        echo "speaches reachable at http://${ip}:${PORT}"
        echo "  point the remote host's parrot.toml [stt].url and [tts].url there"
    else
        echo "speaches reachable on port ${PORT} (could not determine this host's LAN IP)"
    fi
}

# ---------------------------------------------------------------------------
# cleanup: idempotent revert used by trap and post-run.
# ---------------------------------------------------------------------------
cleanup() {
    if (( _cleanup_done == 1 )); then
        return
    fi
    _cleanup_done=1
    echo ""
    echo "Reverting firewall:"
    close_port
    echo "Firewall reverted."
}

# ---------------------------------------------------------------------------
# Subcommand dispatch
# ---------------------------------------------------------------------------
SUBCMD="${1:-}"

case "$SUBCMD" in
    open)
        preflight
        resolve_port
        echo "Opening firewall port:"
        open_port
        echo ""
        report_reach
        echo ""
        echo "NOTE: the port is open until you run: $(basename "$0") close"
        echo "      (runtime rules clear on the next firewalld reload as a backstop)"
        ;;

    close)
        preflight
        resolve_port
        # `close` reverts the rule regardless of who added it: the user asked
        # for the port shut, so unlike the trap path there is no pre-open state
        # to preserve.
        opened=1
        echo "Closing firewall port:"
        close_port
        echo "Firewall port closed."
        ;;

    run)
        # Install the cleanup trap BEFORE opening so a mid-open failure still
        # reverts.  cleanup() is safe with PORT empty or opened=0.
        trap 'cleanup' EXIT
        trap 'cleanup; trap - INT; kill -INT $$' INT
        trap 'cleanup; trap - TERM; kill -TERM $$' TERM

        preflight
        resolve_port
        echo "Opening firewall port:"
        open_port
        echo ""
        report_reach
        echo ""
        echo "Holding the port open. Press Ctrl-C to close it and exit."

        # Sleep in a loop rather than `wait`: there is no child to wait on, and
        # a bare `sleep infinity` would need the same trap plumbing anyway.
        while true; do
            sleep 3600 &
            wait $! || true
        done
        ;;

    *)
        echo "Usage: $(basename "$0") open | close | run" >&2
        echo "" >&2
        echo "  open   Open the speaches firewall port and exit (manual bracket)." >&2
        echo "  close  Remove the speaches firewall port and exit (manual bracket)." >&2
        echo "  run    Open the port, hold until Ctrl-C, revert on exit." >&2
        exit 1
        ;;
esac
