#!/usr/bin/env bash
# scripts/test-hil-firewall.sh — self-check for hil-firewall.sh
#
# Stubs both `cargo` (so --print-ports returns canned output) and
# `firewall-cmd` (so it records calls without touching a real firewall),
# then asserts the correct add/remove decisions are made.
#
# Run as a plain shell script; exits 0 on pass, non-zero on failure.
# No external test framework required.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIREWALL_SCRIPT="$SCRIPT_DIR/hil-firewall.sh"

# ---------------------------------------------------------------------------
# Test harness
# ---------------------------------------------------------------------------
PASS=0
FAIL=0

pass() { echo "PASS: $1"; ((PASS++)) || true; }
fail() { echo "FAIL: $1"; ((FAIL++)) || true; }

# grep_fixed: search for a literal string without treating it as a grep option.
grep_fixed() {
    local needle="$1"
    grep -F -- "$needle"
}

assert_contains() {
    local label="$1" needle="$2" haystack="$3"
    if printf '%s' "$haystack" | grep_fixed "$needle" >/dev/null 2>&1; then
        pass "$label"
    else
        fail "$label — expected to find: $needle"
        echo "  Output was:"
        printf '%s\n' "$haystack" | sed 's/^/    /'
    fi
}

assert_not_contains() {
    local label="$1" needle="$2" haystack="$3"
    if ! printf '%s' "$haystack" | grep_fixed "$needle" >/dev/null 2>&1; then
        pass "$label"
    else
        fail "$label — expected NOT to find: $needle"
        echo "  Output was:"
        printf '%s\n' "$haystack" | sed 's/^/    /'
    fi
}

# ---------------------------------------------------------------------------
# Setup: temp dir for fake commands
# ---------------------------------------------------------------------------
TMPDIR_TEST="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_TEST"' EXIT

export FAKE_CALLS_FILE="$TMPDIR_TEST/firewall-cmd-calls"
export FAKE_OPEN_PORTS_FILE="$TMPDIR_TEST/open-ports"  # simulates firewalld runtime state
export PRINT_PORTS_OUTPUT_FILE="$TMPDIR_TEST/print-ports-output"

# Write the fake print-ports output (default ports).
printf 'udp_port=17380/udp\ntcp_port=17381/tcp\ninbound_frames_port=17382/tcp\nbackpressure_port=17383/tcp\npoll_readiness_port=17384/tcp\nrtd_port=17385/tcp\ntls_psk_port=17386/tcp\ntls_psk_bad_port=17387/tcp\n' > "$PRINT_PORTS_OUTPUT_FILE"

# Fake cargo: when called with `-- --print-ports`, emit canned output.
# FAKE_PRINT_PORTS_EXIT_CODE controls exit code (default 0).
# PRINT_PORTS_OUTPUT_FILE controls stdout content.
# Any other invocation (make hil-test → cargo run) exits 0 silently (success).
cat > "$TMPDIR_TEST/cargo" <<'EOF'
#!/usr/bin/env bash
# Fake cargo stub for hil-firewall.sh tests.
for arg in "$@"; do
    if [[ "$arg" == "--print-ports" ]]; then
        cat "$PRINT_PORTS_OUTPUT_FILE"
        exit "${FAKE_PRINT_PORTS_EXIT_CODE:-0}"
    fi
done
# Simulate a successful hil-test run.
exit 0
EOF
chmod +x "$TMPDIR_TEST/cargo"

# Also create a fake hil-host binary at the path resolve_ports checks first
# (firmware/target/release/hil-host), so the prebuilt-binary fast path is
# exercised and doesn't pick up any real binary that might exist there.
# We save and restore any real binary that was there.
REAL_HIL_HOST="$SCRIPT_DIR/../firmware/target/release/hil-host"
SAVED_HIL_HOST=""
if [[ -f "$REAL_HIL_HOST" ]]; then
    SAVED_HIL_HOST="$TMPDIR_TEST/hil-host.real"
    cp "$REAL_HIL_HOST" "$SAVED_HIL_HOST"
fi
mkdir -p "$(dirname "$REAL_HIL_HOST")"
cat > "$REAL_HIL_HOST" <<'EOF'
#!/usr/bin/env bash
# Fake hil-host binary for hil-firewall.sh tests.
for arg in "$@"; do
    if [[ "$arg" == "--print-ports" ]]; then
        cat "$PRINT_PORTS_OUTPUT_FILE"
        exit "${FAKE_PRINT_PORTS_EXIT_CODE:-0}"
    fi
done
exit 0
EOF
chmod +x "$REAL_HIL_HOST"

# Update the EXIT trap to restore the real binary (or remove the fake),
# then clean up the temp dir.  Restore must happen BEFORE rm -rf TMPDIR
# because the saved copy lives inside TMPDIR.
trap 'if [[ -n "$SAVED_HIL_HOST" ]]; then
          cp "$SAVED_HIL_HOST" "$REAL_HIL_HOST"
      else
          rm -f "$REAL_HIL_HOST"
      fi;
      rm -rf "$TMPDIR_TEST"' EXIT

# Fake make: invoke fake cargo (the test's PATH has fake cargo first).
cat > "$TMPDIR_TEST/make" <<'EOF'
#!/usr/bin/env bash
# Fake make stub: simulate `make hil-test` success or failure.
exit "${FAKE_MAKE_EXIT_CODE:-0}"
EOF
chmod +x "$TMPDIR_TEST/make"

# Fake sudo: strip 'sudo' and run the rest via the current PATH (so fake firewall-cmd is found).
cat > "$TMPDIR_TEST/sudo" <<'EOF'
#!/usr/bin/env bash
# Passthrough sudo: just run the rest of the arguments.
exec "$@"
EOF
chmod +x "$TMPDIR_TEST/sudo"

# Fake firewall-cmd: records calls, maintains a fake set of open ports.
# firewall-cmd uses = syntax for values: --add-port=17380/udp, --query-port=17380/udp, etc.
cat > "$TMPDIR_TEST/firewall-cmd" <<'FWEOF'
#!/usr/bin/env bash
# Fake firewall-cmd stub.
# Writes each invocation line to $FAKE_CALLS_FILE.
# Maintains $FAKE_OPEN_PORTS_FILE as a set of "port/proto" lines (one per line).

echo "$*" >> "$FAKE_CALLS_FILE"

# Parse the first argument for dispatch; extract value from = forms.
CMD="${1:-}"
VAL="${CMD#*=}"   # value after = (same as CMD if no =)
KEY="${CMD%%=*}"  # key before =

case "$KEY" in
    --state)
        exit 0
        ;;
    --query-port)
        spec="$VAL"
        if grep -qxF -- "$spec" "$FAKE_OPEN_PORTS_FILE" 2>/dev/null; then
            exit 0   # port is open
        else
            exit 1   # port is not open
        fi
        ;;
    --add-port)
        spec="$VAL"
        echo "$spec" >> "$FAKE_OPEN_PORTS_FILE"
        exit 0
        ;;
    --remove-port)
        spec="$VAL"
        if [[ -f "$FAKE_OPEN_PORTS_FILE" ]]; then
            grep -vxF -- "$spec" "$FAKE_OPEN_PORTS_FILE" > "$FAKE_OPEN_PORTS_FILE.tmp" 2>/dev/null || true
            mv "$FAKE_OPEN_PORTS_FILE.tmp" "$FAKE_OPEN_PORTS_FILE"
        fi
        exit 0
        ;;
    *)
        exit 0
        ;;
esac
FWEOF
chmod +x "$TMPDIR_TEST/firewall-cmd"

# Put fakes first on PATH.
export PATH="$TMPDIR_TEST:$PATH"

reset_state() {
    : > "$FAKE_CALLS_FILE"
    : > "$FAKE_OPEN_PORTS_FILE"
}

# ---------------------------------------------------------------------------
# Test 1: `open` with default ports opens 17380/udp, 17381/tcp, 7380/tcp
# ---------------------------------------------------------------------------
reset_state
printf 'udp_port=17380/udp\ntcp_port=17381/tcp\ninbound_frames_port=17382/tcp\nbackpressure_port=17383/tcp\npoll_readiness_port=17384/tcp\nrtd_port=17385/tcp\ntls_psk_port=17386/tcp\ntls_psk_bad_port=17387/tcp\n' > "$PRINT_PORTS_OUTPUT_FILE"

out1="$("$FIREWALL_SCRIPT" open 2>&1)"
calls1="$(cat "$FAKE_CALLS_FILE")"

assert_contains "open: adds 17380/udp"        "--add-port=17380/udp" "$calls1"
assert_contains "open: adds 17381/tcp"        "--add-port=17381/tcp" "$calls1"
assert_contains "open: adds 17384/tcp"        "--add-port=17384/tcp" "$calls1"
# rtd_port (17385) is the regression this data-driven rewrite fixes: it must be
# opened purely because --print-ports lists it, with no script-side entry.
assert_contains "open: adds 17385/tcp (rtd)"  "--add-port=17385/tcp" "$calls1"
assert_contains "open: adds 17386/tcp (tls-psk)"     "--add-port=17386/tcp" "$calls1"
assert_contains "open: adds 17387/tcp (tls-psk-bad)" "--add-port=17387/tcp" "$calls1"
assert_contains "open: adds 7380/tcp"         "--add-port=7380/tcp"  "$calls1"
assert_contains "open: console mentions 17380/udp" "17380/udp" "$out1"
assert_contains "open: console mentions 17381/tcp" "17381/tcp" "$out1"
assert_contains "open: console mentions 7380/tcp"  "7380/tcp"  "$out1"

# ---------------------------------------------------------------------------
# Test 2: `open` with overridden ports opens the overridden ports
# (AC8 override case — helper opens whatever --print-ports reports, not hardcoded values)
# ---------------------------------------------------------------------------
reset_state
printf 'udp_port=19000/udp\ntcp_port=19001/tcp\ninbound_frames_port=17382/tcp\nbackpressure_port=17383/tcp\npoll_readiness_port=17384/tcp\nrtd_port=17385/tcp\ntls_psk_port=17386/tcp\ntls_psk_bad_port=17387/tcp\n' > "$PRINT_PORTS_OUTPUT_FILE"

"$FIREWALL_SCRIPT" open >/dev/null 2>&1
calls2="$(cat "$FAKE_CALLS_FILE")"

assert_contains     "open(override): adds 19000/udp"          "--add-port=19000/udp" "$calls2"
assert_contains     "open(override): adds 19001/tcp"          "--add-port=19001/tcp" "$calls2"
assert_contains     "open(override): adds 7380/tcp"           "--add-port=7380/tcp"  "$calls2"
assert_not_contains "open(override): does NOT add 17380/udp"  "--add-port=17380/udp" "$calls2"
assert_not_contains "open(override): does NOT add 17381/tcp"  "--add-port=17381/tcp" "$calls2"

# ---------------------------------------------------------------------------
# Test 3: `close` removes the ports it would open (default ports case)
# ---------------------------------------------------------------------------
reset_state
printf 'udp_port=17380/udp\ntcp_port=17381/tcp\ninbound_frames_port=17382/tcp\nbackpressure_port=17383/tcp\npoll_readiness_port=17384/tcp\nrtd_port=17385/tcp\ntls_psk_port=17386/tcp\ntls_psk_bad_port=17387/tcp\n' > "$PRINT_PORTS_OUTPUT_FILE"

# Pre-populate the fake open-ports set so close finds something to remove.
printf '17380/udp\n17381/tcp\n7380/tcp\n' >> "$FAKE_OPEN_PORTS_FILE"

"$FIREWALL_SCRIPT" close >/dev/null 2>&1
calls3="$(cat "$FAKE_CALLS_FILE")"

assert_contains "close: removes 17380/udp" "--remove-port=17380/udp" "$calls3"
assert_contains "close: removes 17381/tcp" "--remove-port=17381/tcp" "$calls3"
assert_contains "close: removes 17384/tcp" "--remove-port=17384/tcp" "$calls3"
assert_contains "close: removes 7380/tcp"  "--remove-port=7380/tcp"  "$calls3"

# ---------------------------------------------------------------------------
# Test 4: `run` opens ports, runs make hil-test (fake success), reverts
# ---------------------------------------------------------------------------
reset_state
printf 'udp_port=17380/udp\ntcp_port=17381/tcp\ninbound_frames_port=17382/tcp\nbackpressure_port=17383/tcp\npoll_readiness_port=17384/tcp\nrtd_port=17385/tcp\ntls_psk_port=17386/tcp\ntls_psk_bad_port=17387/tcp\n' > "$PRINT_PORTS_OUTPUT_FILE"
export FAKE_MAKE_EXIT_CODE=0

"$FIREWALL_SCRIPT" run >/dev/null 2>&1 && exit4=0 || exit4=$?
calls4="$(cat "$FAKE_CALLS_FILE")"

assert_contains "run: opens 17380/udp"   "--add-port=17380/udp"    "$calls4"
assert_contains "run: opens 17381/tcp"   "--add-port=17381/tcp"    "$calls4"
assert_contains "run: opens 17384/tcp"   "--add-port=17384/tcp"    "$calls4"
assert_contains "run: opens 7380/tcp"    "--add-port=7380/tcp"     "$calls4"
assert_contains "run: removes 17380/udp" "--remove-port=17380/udp" "$calls4"
assert_contains "run: removes 17381/tcp" "--remove-port=17381/tcp" "$calls4"
assert_contains "run: removes 17384/tcp" "--remove-port=17384/tcp" "$calls4"
assert_contains "run: removes 7380/tcp"  "--remove-port=7380/tcp"  "$calls4"

if [[ "$exit4" == "0" ]]; then
    pass "run: propagates success exit code (0)"
else
    fail "run: expected exit 0, got $exit4"
fi

# ---------------------------------------------------------------------------
# Test 5: `run` propagates non-zero exit code from make hil-test and still reverts
# ---------------------------------------------------------------------------
reset_state
printf 'udp_port=17380/udp\ntcp_port=17381/tcp\ninbound_frames_port=17382/tcp\nbackpressure_port=17383/tcp\npoll_readiness_port=17384/tcp\nrtd_port=17385/tcp\ntls_psk_port=17386/tcp\ntls_psk_bad_port=17387/tcp\n' > "$PRINT_PORTS_OUTPUT_FILE"
export FAKE_MAKE_EXIT_CODE=1

"$FIREWALL_SCRIPT" run >/dev/null 2>&1 && exit5=0 || exit5=$?
calls5="$(cat "$FAKE_CALLS_FILE")"

assert_contains "run(fail): still removes 17380/udp after failure" \
    "--remove-port=17380/udp" "$calls5"
assert_contains "run(fail): still removes 17381/tcp after failure" \
    "--remove-port=17381/tcp" "$calls5"
assert_contains "run(fail): still removes 17384/tcp after failure" \
    "--remove-port=17384/tcp" "$calls5"
assert_contains "run(fail): still removes 7380/tcp after failure" \
    "--remove-port=7380/tcp" "$calls5"

if [[ "$exit5" == "1" ]]; then
    pass "run(fail): propagates failure exit code (1)"
else
    fail "run(fail): expected exit 1, got $exit5"
fi
unset FAKE_MAKE_EXIT_CODE

# ---------------------------------------------------------------------------
# Test 6: `run` with a pre-open port does NOT remove that port on revert (AC9)
# ---------------------------------------------------------------------------
reset_state
printf 'udp_port=17380/udp\ntcp_port=17381/tcp\ninbound_frames_port=17382/tcp\nbackpressure_port=17383/tcp\npoll_readiness_port=17384/tcp\nrtd_port=17385/tcp\ntls_psk_port=17386/tcp\ntls_psk_bad_port=17387/tcp\n' > "$PRINT_PORTS_OUTPUT_FILE"
# 7380/tcp is pre-open before run starts.
echo "7380/tcp" >> "$FAKE_OPEN_PORTS_FILE"
export FAKE_MAKE_EXIT_CODE=0

"$FIREWALL_SCRIPT" run > /dev/null 2>&1
calls6="$(cat "$FAKE_CALLS_FILE")"

# 7380 was pre-open so the helper should NOT have added it, and should NOT remove it.
assert_not_contains "run(pre-open 7380): does NOT remove pre-open 7380/tcp" \
    "--remove-port=7380/tcp" "$calls6"
# The dynamic ports should still be reverted.
assert_contains "run(pre-open 7380): still removes 17380/udp" \
    "--remove-port=17380/udp" "$calls6"
assert_contains "run(pre-open 7380): still removes 17381/tcp" \
    "--remove-port=17381/tcp" "$calls6"
unset FAKE_MAKE_EXIT_CODE

# ---------------------------------------------------------------------------
# Test 7: `run` reverts when SIGINT is sent (AC10 INT trap path)
# ---------------------------------------------------------------------------
reset_state
printf 'udp_port=17380/udp\ntcp_port=17381/tcp\ninbound_frames_port=17382/tcp\nbackpressure_port=17383/tcp\npoll_readiness_port=17384/tcp\nrtd_port=17385/tcp\ntls_psk_port=17386/tcp\ntls_psk_bad_port=17387/tcp\n' > "$PRINT_PORTS_OUTPUT_FILE"

# Make the fake make sleep so we have time to send SIGINT before it exits.
cat > "$TMPDIR_TEST/make" <<'EOF'
#!/usr/bin/env bash
sleep 5
exit "${FAKE_MAKE_EXIT_CODE:-0}"
EOF

"$FIREWALL_SCRIPT" run > /dev/null 2>&1 &
script_pid=$!

# Give the script time to open ports and start the fake make.
sleep 0.5

# Send SIGINT to the script process (simulating Ctrl-C).
kill -INT "$script_pid" 2>/dev/null || true

# Wait for the script to finish (trap fires, cleanup runs, process exits).
wait "$script_pid" 2>/dev/null || true

calls7="$(cat "$FAKE_CALLS_FILE")"

# Ports should have been opened before the INT.
assert_contains "run(SIGINT): had opened 17380/udp before INT"  "--add-port=17380/udp"    "$calls7"
# Cleanup trap should have removed them on INT.
assert_contains "run(SIGINT): removes 17380/udp on INT cleanup" "--remove-port=17380/udp" "$calls7"
assert_contains "run(SIGINT): removes 17381/tcp on INT cleanup" "--remove-port=17381/tcp" "$calls7"
assert_contains "run(SIGINT): removes 17384/tcp on INT cleanup" "--remove-port=17384/tcp" "$calls7"
assert_contains "run(SIGINT): removes 7380/tcp on INT cleanup"  "--remove-port=7380/tcp"  "$calls7"

# Restore the simple make stub for subsequent tests.
cat > "$TMPDIR_TEST/make" <<'EOF'
#!/usr/bin/env bash
exit "${FAKE_MAKE_EXIT_CODE:-0}"
EOF

# ---------------------------------------------------------------------------
# Test 8: `open` exits non-zero with an error when --print-ports fails (test-6)
# ---------------------------------------------------------------------------
reset_state
# Make the fake hil-host (and cargo fallback) return non-zero for --print-ports.
export FAKE_PRINT_PORTS_EXIT_CODE=1
printf 'build error: could not compile\n' > "$PRINT_PORTS_OUTPUT_FILE"

"$FIREWALL_SCRIPT" open >/dev/null 2>&1 && exit8=0 || exit8=$?
calls8="$(cat "$FAKE_CALLS_FILE")"
unset FAKE_PRINT_PORTS_EXIT_CODE
printf 'udp_port=17380/udp\ntcp_port=17381/tcp\ninbound_frames_port=17382/tcp\nbackpressure_port=17383/tcp\npoll_readiness_port=17384/tcp\nrtd_port=17385/tcp\ntls_psk_port=17386/tcp\ntls_psk_bad_port=17387/tcp\n' > "$PRINT_PORTS_OUTPUT_FILE"

if [[ "$exit8" != "0" ]]; then
    pass "open(print-ports-fail): exits non-zero when --print-ports fails"
else
    fail "open(print-ports-fail): expected non-zero exit when --print-ports fails, got 0"
fi
assert_not_contains "open(print-ports-fail): does NOT open any port when --print-ports fails" \
    "--add-port" "$calls8"

# ---------------------------------------------------------------------------
# Test 9: `open` exits non-zero when --print-ports produces garbage output (test-6b)
# ---------------------------------------------------------------------------
reset_state
# Make hil-host exit 0 but produce no parseable output.
export FAKE_PRINT_PORTS_EXIT_CODE=0
printf 'garbage output no port lines here\n' > "$PRINT_PORTS_OUTPUT_FILE"

out9="$("$FIREWALL_SCRIPT" open 2>&1)" && exit9=0 || exit9=$?
calls9="$(cat "$FAKE_CALLS_FILE")"
unset FAKE_PRINT_PORTS_EXIT_CODE
printf 'udp_port=17380/udp\ntcp_port=17381/tcp\ninbound_frames_port=17382/tcp\nbackpressure_port=17383/tcp\npoll_readiness_port=17384/tcp\nrtd_port=17385/tcp\ntls_psk_port=17386/tcp\ntls_psk_bad_port=17387/tcp\n' > "$PRINT_PORTS_OUTPUT_FILE"

if [[ "$exit9" != "0" ]]; then
    pass "open(garbage-output): exits non-zero when --print-ports output is unparseable"
else
    fail "open(garbage-output): expected non-zero exit, got 0"
fi
assert_not_contains "open(garbage-output): does NOT open any port" "--add-port" "$calls9"
assert_contains "open(garbage-output): error message mentions missing output" \
    "no recognizable" "$out9"

# ---------------------------------------------------------------------------
# Test 10: preflight fails when firewalld is not running (test-7)
# ---------------------------------------------------------------------------
reset_state
printf 'udp_port=17380/udp\ntcp_port=17381/tcp\ninbound_frames_port=17382/tcp\nbackpressure_port=17383/tcp\npoll_readiness_port=17384/tcp\nrtd_port=17385/tcp\ntls_psk_port=17386/tcp\ntls_psk_bad_port=17387/tcp\n' > "$PRINT_PORTS_OUTPUT_FILE"

# Replace the fake firewall-cmd with one that returns non-zero for --state.
cat > "$TMPDIR_TEST/firewall-cmd" <<'FWEOF'
#!/usr/bin/env bash
echo "$*" >> "$FAKE_CALLS_FILE"
CMD="${1:-}"
KEY="${CMD%%=*}"
case "$KEY" in
    --state) exit 1 ;;  # simulate firewalld not running
    *) exit 0 ;;
esac
FWEOF
chmod +x "$TMPDIR_TEST/firewall-cmd"

"$FIREWALL_SCRIPT" open >/dev/null 2>&1 && exit10=0 || exit10=$?
calls10="$(cat "$FAKE_CALLS_FILE")"

if [[ "$exit10" != "0" ]]; then
    pass "preflight(no-firewalld): exits non-zero when firewalld is not running"
else
    fail "preflight(no-firewalld): expected non-zero exit, got 0"
fi
assert_not_contains "preflight(no-firewalld): does NOT open any port" "--add-port" "$calls10"

# Restore the normal fake firewall-cmd.
cat > "$TMPDIR_TEST/firewall-cmd" <<'FWEOF'
#!/usr/bin/env bash
echo "$*" >> "$FAKE_CALLS_FILE"
CMD="${1:-}"
VAL="${CMD#*=}"
KEY="${CMD%%=*}"
case "$KEY" in
    --state) exit 0 ;;
    --query-port)
        spec="$VAL"
        if grep -qxF -- "$spec" "$FAKE_OPEN_PORTS_FILE" 2>/dev/null; then
            exit 0
        else
            exit 1
        fi
        ;;
    --add-port)
        spec="$VAL"
        echo "$spec" >> "$FAKE_OPEN_PORTS_FILE"
        exit 0
        ;;
    --remove-port)
        spec="$VAL"
        if [[ -f "$FAKE_OPEN_PORTS_FILE" ]]; then
            grep -vxF -- "$spec" "$FAKE_OPEN_PORTS_FILE" > "$FAKE_OPEN_PORTS_FILE.tmp" 2>/dev/null || true
            mv "$FAKE_OPEN_PORTS_FILE.tmp" "$FAKE_OPEN_PORTS_FILE"
        fi
        exit 0
        ;;
    *) exit 0 ;;
esac
FWEOF
chmod +x "$TMPDIR_TEST/firewall-cmd"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "Results: $PASS passed, $FAIL failed."

if [[ "$FAIL" -gt 0 ]]; then
    exit 1
fi
exit 0
