#!/usr/bin/env bash
#
# deploy-reachy-pod.test.sh — host-only regression tests for the payload's
# deploy plumbing.
#
# Everything this script decides before it reaches a device is text processing,
# and two of the decisions are expensive to get wrong:
#
#   * the robot refusal. A robot and a pod share one payload store and one
#     application slot, and the robot's payload is five applications under one
#     launcher with the pod among them. An activation of a pod-only payload on a
#     robot replaces all of it with a binary that cannot move the machine — a
#     robot that still talks, still answers, and never moves again, with nothing
#     narrating why. The refusal is the only thing standing between that and a
#     command typed out of habit in the wrong repository.
#   * the exit status. The self-test modes hand back the registry's own verdict,
#     and a hardware reading reported as a clean run is a reading nobody
#     re-takes.
#
# The device is a stubbed `ssh` that runs the remote command it is handed under
# bash, with every absolute device path rewritten into a fixture root. So the
# guard under test is the shipped one, evaluated as a shell condition against a
# fixture filesystem, rather than a mocked answer: a guard that stopped matching
# what a robot leaves on a unit fails here.

set -euo pipefail

HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
TOOL="${TOOL:-$HERE/deploy-reachy-pod.sh}" # overridable to run the suite against a modified tool

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# shellcheck source=test-lib.sh
. "$HERE/test-lib.sh"

STUBS="$WORK/stubs"
mkdir -p "$STUBS"

# Stubbed ssh: records argv, then runs the remote command under bash with the
# device's absolute paths rewritten into the fixture root. The pty form (--bench)
# and the plain form differ only in which argument is the command, so both are
# found by taking the last one.
#
# STUB_SSH_RC scripts a connection that never lands, which is a different
# condition from a device that answered.
cat >"$STUBS/ssh" <<'SSH_EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$STUB_SSH_ARGV"
if [ -n "${STUB_SSH_RC:-}" ]; then
	echo "ssh: connect to host: no route to host" >&2
	exit "$STUB_SSH_RC"
fi
# The reading mode hands ssh a command as separate words rather than one
# string, and there is nothing on this side to run: answer it and stop.
case "$*" in
*journalctl*) exit 0 ;;
esac
remote=${*: -1}
remote=${remote//\/run\/brenn-app/$STUB_ROOT/run/brenn-app}
remote=${remote//\/usr\/sbin\/brenn-app-activate/$STUB_STUBS/brenn-app-activate}
printf '%s\n' "$remote" >>"$STUB_SSH_REMOTE"
PATH="$STUB_STUBS:$PATH" bash -c "$remote"
SSH_EOF

# rsync: records argv and nothing else. What it would have copied is asserted
# from that.
cat >"$STUBS/rsync" <<'RSYNC_EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$STUB_RSYNC_ARGV"
exit 0
RSYNC_EOF

# The activation tool the device carries. Records the release it was handed.
cat >"$STUBS/brenn-app-activate" <<'ACTIVATE_EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$STUB_ACTIVATED"
exit "${STUB_ACTIVATE_RC:-0}"
ACTIVATE_EOF

# systemctl: the application is running when the fixture says so. Only
# is-active is asked of it.
cat >"$STUBS/systemctl" <<'SYSTEMCTL_EOF'
#!/usr/bin/env bash
case "$*" in
*is-active*) [ -e "$STUB_ROOT/app-running" ] && exit 0 || exit 3 ;;
esac
exit 1
SYSTEMCTL_EOF

# setpriv: the self-test run, dropped to the application's account. Records the
# argv and hands back the registry's scripted verdict.
cat >"$STUBS/setpriv" <<'SETPRIV_EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$STUB_SELFTEST"
exit "${STUB_SELFTEST_RC:-0}"
SETPRIV_EOF

chmod +x "$STUBS/ssh" "$STUBS/rsync" "$STUBS/brenn-app-activate" \
	"$STUBS/systemctl" "$STUBS/setpriv"

treenum=0
TREE=""
ROOT=""
SSH_ARGV=""
REMOTE=""
RSYNC_ARGV=""
ACTIVATED=""
SELFTEST=""

# A fresh fixture: the tool and its prelude where their own relative lookups
# land inside the tree, a device with an empty payload store, and no built
# payload yet.
new_tree() {
	treenum=$((treenum + 1))
	TREE="$WORK/tree-$treenum"
	ROOT="$TREE/device"
	mkdir -p "$TREE/firmware/tools" "$ROOT/run/brenn-app/releases"
	cp -- "$TOOL" "$HERE/lib.sh" "$TREE/firmware/tools/"
	chmod +x "$TREE/firmware/tools/$(basename -- "$TOOL")"
	SSH_ARGV="$TREE/ssh.argv"
	REMOTE="$TREE/ssh.remote"
	RSYNC_ARGV="$TREE/rsync.argv"
	ACTIVATED="$TREE/activated"
	SELFTEST="$TREE/selftest"
	: >"$SSH_ARGV"
	: >"$REMOTE"
	: >"$RSYNC_ARGV"
	: >"$ACTIVATED"
	: >"$SELFTEST"
	export STUB_SSH_ARGV="$SSH_ARGV" STUB_SSH_REMOTE="$REMOTE"
	export STUB_RSYNC_ARGV="$RSYNC_ARGV" STUB_ACTIVATED="$ACTIVATED"
	export STUB_SELFTEST="$SELFTEST" STUB_ROOT="$ROOT" STUB_STUBS="$STUBS"
	unset STUB_SSH_RC STUB_ACTIVATE_RC STUB_SELFTEST_RC
}

# The payload tree the build leaves behind.
build_payload() {
	local payload="$TREE/firmware/target/reachy-pod/payload"
	mkdir -p "$payload"
	install -m 0755 /dev/null "$payload/run"
	install -m 0755 /dev/null "$payload/reachy-pod"
}

# A unit that is a robot: brenn-reachy's payload in the same store, with the
# stamp its push writes at the root of it.
make_it_a_robot() {
	mkdir -p "$ROOT/run/brenn-app/releases/motion"
	printf 'commit=deadbeef\npushed_from=deadbeef\n' \
		>"$ROOT/run/brenn-app/releases/motion/provenance.txt"
}

run_tool() {
	set +e
	OUT=$(PATH="$STUBS:$PATH" "$TREE/firmware/tools/$(basename -- "$TOOL")" "$@" 2>&1)
	EC=$?
	set -e
}

# ── mode dispatch ─────────────────────────────────────────────────────────────

new_tree
run_tool
expect_die "dispatch-no-host" "usage:"

new_tree
run_tool reachy-dev
expect_die "dispatch-host-with-no-mode" "usage:"

new_tree
run_tool reachy-dev --restart
expect_die "dispatch-unknown-mode" "usage:"

new_tree
run_tool reachy-dev --activate
expect_die "activate-with-no-payload-refused" "no payload tree"
check "activate-with-no-payload-touches-nothing" \
	"$(yes_no [ ! -s "$SSH_ARGV" ])" "ssh was called: $(cat -- "$SSH_ARGV")"

# ── a plain pod: the deploy this script is for ────────────────────────────────

new_tree
build_payload
run_tool reachy-dev --activate
expect_exit "activate-succeeds" 0
check "activate-pushes-the-payload" \
	"$(yes_no grep -q 'releases/dev-' -- "$RSYNC_ARGV")" \
	"rsync argv was: $(cat -- "$RSYNC_ARGV")"
check "activate-hands-the-release-to-the-activation-tool" \
	"$(yes_no grep -q 'releases/dev-' -- "$ACTIVATED")" \
	"activate argv was: $(cat -- "$ACTIVATED")"

# The activation tool's own verdict is this script's: a contract check that
# rejected the payload is not a deploy that worked.
new_tree
build_payload
STUB_ACTIVATE_RC=1 run_tool reachy-dev --activate
check "a-rejected-release-is-a-failed-deploy" "$(yes_no [ "$EC" != 0 ])" \
	"exit ${EC}: $OUT"
check "a-rejected-release-names-the-unit-and-the-release" \
	"$(yes_no grep -q 'activating .* on reachy-dev failed' <<<"$OUT")" \
	"failure was: $OUT"

# ── the robot refusal ─────────────────────────────────────────────────────────

new_tree
build_payload
make_it_a_robot
run_tool reachy-dev --activate
expect_die "activating-on-a-robot-is-refused" "carries the robot's motion payload"
check "the-refusal-names-the-stamp-it-read" \
	"$(yes_no grep -q 'releases/motion/provenance.txt' <<<"$OUT")" \
	"refusal was: $OUT"
check "the-refusal-names-the-repository-that-owns-the-payload" \
	"$(yes_no grep -q 'brenn-reachy' <<<"$OUT")" "refusal was: $OUT"
# The remedy is read at the one moment it matters and nowhere else, so the whole
# line is pinned: the target name belongs to brenn-reachy's Makefile, and a
# remedy that misspells it stacks a second failure on the one being explained.
check "the-refusal-names-the-command-that-deploys-a-robot" \
	"$(yes_no grep -qF 'make -C ../brenn-reachy motion-deploy' <<<"$OUT")" \
	"refusal was: $OUT"
# Before a byte of it lands: the guard rides the connection that would have made
# the push's directory, so a robot is turned away with nothing pushed at it.
check "a-refused-robot-is-pushed-nothing" \
	"$(yes_no [ ! -s "$RSYNC_ARGV" ])" "rsync argv was: $(cat -- "$RSYNC_ARGV")"
check "a-refused-robot-activates-nothing" \
	"$(yes_no [ ! -s "$ACTIVATED" ])" "activate argv was: $(cat -- "$ACTIVATED")"

# The payload landing between the push and the activation — from another
# terminal, or from brenn-reachy's own deploy. Only the question asked in the
# same invocation as the act is binding, and this is the case that says so.
new_tree
build_payload
# The robot appears only after the pushing connection has been answered.
cat >"$STUBS/rsync" <<RSYNC_LATE_EOF
#!/usr/bin/env bash
printf '%s\n' "\$*" >>"\$STUB_RSYNC_ARGV"
mkdir -p -- "\$STUB_ROOT/run/brenn-app/releases/motion"
printf 'commit=deadbeef\n' >"\$STUB_ROOT/run/brenn-app/releases/motion/provenance.txt"
exit 0
RSYNC_LATE_EOF
chmod +x "$STUBS/rsync"
run_tool reachy-dev --activate
expect_die "a-robot-arriving-mid-deploy-is-still-refused" "carries the robot's motion payload"
check "a-robot-arriving-mid-deploy-activates-nothing" \
	"$(yes_no [ ! -s "$ACTIVATED" ])" "activate argv was: $(cat -- "$ACTIVATED")"
# Back to the recording stub for everything after this.
cat >"$STUBS/rsync" <<'RSYNC_EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$STUB_RSYNC_ARGV"
exit 0
RSYNC_EOF
chmod +x "$STUBS/rsync"

# An interrupted robot push leaves the directory and no stamp. The guard reads
# the stamp for exactly this: a pod deploy that refused on the directory would
# be unrefusable short of hand-deleting a tmpfs tree over ssh, on the very fault
# where the operator most needs a deploy to work.
new_tree
build_payload
mkdir -p "$ROOT/run/brenn-app/releases/motion"
run_tool reachy-dev --activate
expect_exit "an-empty-robot-release-is-not-a-payload" 0
check "an-empty-robot-release-still-activates-the-pod" \
	"$(yes_no grep -q 'releases/dev-' -- "$ACTIVATED")" \
	"activate argv was: $(cat -- "$ACTIVATED")"

# 4 is the guard's answer, but on the activating connection the guard is
# replaced by brenn-app-activate, whose status vocabulary is brenn-os's. A
# rejection that happens to be 4 must read as the activation failure it is, not
# as a robot the unit is not.
new_tree
build_payload
STUB_ACTIVATE_RC=4 run_tool reachy-dev --activate
check "an-activation-status-of-4-is-a-failed-deploy" "$(yes_no [ "$EC" != 0 ])" \
	"exit ${EC}: $OUT"
silent_about "an-activation-status-of-4-is-not-read-as-a-robot" \
	"carries the robot's motion payload"
check "an-activation-status-of-4-names-the-activation-failure" \
	"$(yes_no grep -q 'activating .* on reachy-dev failed' <<<"$OUT")" \
	"failure was: $OUT"

# The self-test modes are not the destructive one: they activate nothing and
# replace nothing, and a bench reading off a robot whose stack is stopped is
# worth having. Refusing them would remove a diagnostic to prevent nothing.
new_tree
build_payload
make_it_a_robot
run_tool reachy-dev --selftest
expect_exit "a-self-test-on-a-robot-is-not-refused" 0
check "a-self-test-on-a-robot-runs-the-registry" \
	"$(yes_no grep -q 'reachy-pod selftest' -- "$SELFTEST")" \
	"setpriv argv was: $(cat -- "$SELFTEST")"

# ── the self-test modes ───────────────────────────────────────────────────────

new_tree
build_payload
run_tool reachy-dev --selftest
expect_exit "selftest-succeeds" 0
check "selftest-drops-to-the-application-account" \
	"$(yes_no grep -q -e '--init-groups' -- "$SELFTEST")" \
	"setpriv argv was: $(cat -- "$SELFTEST")"
check "selftest-activates-nothing" \
	"$(yes_no [ ! -s "$ACTIVATED" ])" "activate argv was: $(cat -- "$ACTIVATED")"

# The registry's verdict is this script's. A hardware reading reported as a
# clean run is a reading nobody re-takes.
new_tree
build_payload
STUB_SELFTEST_RC=2 run_tool reachy-dev --selftest
expect_exit "selftest-passes-the-registry-verdict-through" 2

# A running application holds the board, so a self-test beside it would report
# device-busy for every case and read as a hardware fault.
new_tree
build_payload
touch "$ROOT/app-running"
run_tool reachy-dev --selftest
expect_die "selftest-refuses-while-the-application-runs" "holds the sound card"
check "the-busy-refusal-says-how-to-stop-it" \
	"$(yes_no grep -q 'systemctl stop' <<<"$OUT")" "refusal was: $OUT"

new_tree
build_payload
run_tool reachy-dev --bench
expect_exit "bench-succeeds" 0
check "bench-asks-for-a-terminal" \
	"$(yes_no grep -q -e '-t ' -- "$SSH_ARGV")" "ssh argv was: $(cat -- "$SSH_ARGV")"
check "bench-runs-the-manual-registry" \
	"$(yes_no grep -q -e '--manual' -- "$SELFTEST")" \
	"setpriv argv was: $(cat -- "$SELFTEST")"

# ── the mode that only reads ──────────────────────────────────────────────────

new_tree
run_tool reachy-dev --logs
expect_exit "logs-needs-no-payload" 0
check "logs-pushes-nothing" \
	"$(yes_no [ ! -s "$RSYNC_ARGV" ])" "rsync argv was: $(cat -- "$RSYNC_ARGV")"

# ── the device that never answered ────────────────────────────────────────────

new_tree
build_payload
STUB_SSH_RC=255 run_tool reachy-dev --selftest
check "an-unreachable-host-is-not-a-hardware-reading" "$(yes_no [ "$EC" != 0 ])" \
	"exit ${EC}: $OUT"
# The pushing connection is the first one a self-test makes, so this is where an
# unreachable unit is met — and the answer names it rather than handing back
# ssh's bare status.
check "an-unreachable-selftest-names-the-host" \
	"$(yes_no grep -q 'root@reachy-dev failed' <<<"$OUT")" "failure was: $OUT"

# The deploy path had nothing to say about this at all: it exited with ssh's own
# status and ssh's own line, and 255 from a transport that never landed reads to
# a caller exactly like an answer from the device.
new_tree
build_payload
STUB_SSH_RC=255 run_tool reachy-dev --activate
check "an-unreachable-host-fails-the-deploy" "$(yes_no [ "$EC" != 0 ])" \
	"exit ${EC}: $OUT"
check "the-unreachable-deploy-names-the-host" \
	"$(yes_no grep -q 'root@reachy-dev failed' <<<"$OUT")" "failure was: $OUT"
check "the-unreachable-deploy-says-nothing-was-pushed" \
	"$(yes_no grep -q 'Nothing was pushed' <<<"$OUT")" "failure was: $OUT"
check "an-unreachable-deploy-pushes-nothing" \
	"$(yes_no [ ! -s "$RSYNC_ARGV" ])" "rsync argv was: $(cat -- "$RSYNC_ARGV")"

test_summary deploy-reachy-pod.test.sh
