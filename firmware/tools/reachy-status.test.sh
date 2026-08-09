#!/usr/bin/env bash
#
# reachy-status.test.sh — host-only regression tests for reachy-status.sh.
#
# The status command's whole value is that it is trusted: an operator runs it
# instead of torquing servos to find out what is missing, so a check that
# silently stops being asked, or an answer read as OK when the device said
# otherwise, is worse than not having the command. Both are text processing —
# the probe is composed here, the judgement is made here — and neither needs a
# device.
#
# The device is a stubbed `ssh` that runs the probe against a fixture root:
# every path the probe tests is rooted at a directory this suite builds, and
# `systemctl` and `findmnt` are stubs beside it answering from files in that
# tree. So the probe under test is the shipped one, run as a shell script, and
# what it reads is a fixture filesystem rather than a mocked answer.

set -euo pipefail

HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
TOOL="${TOOL:-$HERE/reachy-status.sh}" # overridable to run the suite against a modified tool

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# shellcheck source=test-lib.sh
. "$HERE/test-lib.sh"

STUBS="$WORK/stubs"
mkdir -p "$STUBS"

# Stubbed ssh: runs the probe it is handed under bash, with the fixture root
# prefixed onto every absolute path the probe names. That prefix is the whole
# trick — it lets the shipped probe test real files without running as root on a
# real device — and it is applied to the probe text, not to its answers.
#
# STUB_SSH_RC scripts a connection that never lands, which is a different
# condition to report than a device that answered.
#
# STUB_SSH_SAYS scripts the third case: a connection that lands, exits zero, and
# says something other than what the probe would have said — a login banner, a
# device with no bash, a probe whose composition broke. Set to the empty string
# for a device that answered nothing at all.
cat >"$STUBS/ssh" <<'SSH_EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$STUB_SSH_ARGV"
if [ -n "${STUB_SSH_RC:-}" ]; then
	echo "ssh: connect to host: no route to host" >&2
	exit "$STUB_SSH_RC"
fi
if [ -n "${STUB_SSH_SAYS+set}" ]; then
	cat >"$STUB_SSH_PROBE"
	printf '%s' "$STUB_SSH_SAYS"
	exit 0
fi
probe=$(cat)
# Absolute device paths become fixture paths. The two prefixes the probe uses
# are the payload store and the account's home.
probe=${probe//\/run\/brenn-app/$STUB_ROOT/run/brenn-app}
probe=${probe//\/run\/reachy-motiond/$STUB_ROOT/run/reachy-motiond}
probe=${probe//\/var\/lib\/brenn-app/$STUB_ROOT/var/lib/brenn-app}
probe=${probe//\/dev\/tty/$STUB_ROOT/dev/tty}
printf '%s' "$probe" >"$STUB_SSH_PROBE"
PATH="$STUB_STUBS:$PATH" bash -s <<<"$probe"
SSH_EOF

# systemctl: a unit is active when the fixture says so. Only is-active is asked
# of it by the probe.
cat >"$STUBS/systemctl" <<'SYSTEMCTL_EOF'
#!/usr/bin/env bash
for arg in "$@"; do
	case "$arg" in
	*.service) [ -e "$STUB_ROOT/active/$arg" ] && exit 0 || exit 3 ;;
	esac
done
exit 1
SYSTEMCTL_EOF

# findmnt: the store is a mount point when the fixture says so. The real one
# answers about a mount table; a directory that exists is not a mount, which is
# exactly the distinction the check is for.
cat >"$STUBS/findmnt" <<'FINDMNT_EOF'
#!/usr/bin/env bash
[ -e "$STUB_ROOT/mounted" ] || exit 1
echo /run/brenn-app
FINDMNT_EOF

chmod +x "$STUBS/ssh" "$STUBS/systemctl" "$STUBS/findmnt"

treenum=0
TREE=""
ROOT=""
SSH_ARGV=""
PROBE=""

# A device with nothing on it: no mount, no payload, no configurations, no unit.
new_tree() {
	treenum=$((treenum + 1))
	TREE="$WORK/tree-$treenum"
	ROOT="$TREE/device"
	mkdir -p "$TREE/firmware/tools"
	cp -- "$TOOL" "$HERE/lib.sh" "$TREE/firmware/tools/"
	chmod +x "$TREE/firmware/tools/$(basename -- "$TOOL")"
	mkdir -p "$ROOT/run/brenn-app/conf" "$ROOT/run/brenn-app/releases/motiond" \
		"$ROOT/var/lib/brenn-app" "$ROOT/dev" "$ROOT/active"
	STATE="$ROOT/run/reachy-motiond/state"
	SSH_ARGV="$TREE/ssh.argv"
	PROBE="$TREE/ssh.probe"
	: >"$SSH_ARGV"
	: >"$PROBE"
	export STUB_SSH_ARGV="$SSH_ARGV" STUB_SSH_PROBE="$PROBE"
	export STUB_ROOT="$ROOT" STUB_STUBS="$STUBS"
	unset STUB_SSH_RC
	unset STUB_SSH_SAYS
}

# A device with everything on it.
provision_all() {
	touch "$ROOT/mounted"
	mkdir -p "$ROOT/run/brenn-app/current"
	install -m 0755 /dev/null "$ROOT/run/brenn-app/current/run"
	touch "$ROOT/active/brenn-app.service" "$ROOT/active/reachy-motiond.service"
	touch "$ROOT/run/brenn-app/conf/audio.conf"
	# The serial node the fixture bench file names is a fixture path: the probe
	# reads that value at run time out of the file, so it is one of the few
	# things the stub's path rewriting cannot reach.
	printf '[bus]\ndevice = "%s"\nbaud = 1000000\n' "$ROOT/dev/ttyAMA3" \
		>"$ROOT/var/lib/brenn-app/reachy-bench.toml"
	touch "$ROOT/var/lib/brenn-app/reachy-motiond.toml"
	touch "$ROOT/var/lib/brenn-app/motiond-token"
	install -m 0755 /dev/null "$ROOT/run/brenn-app/releases/motiond/reachy-motiond"
	touch "$ROOT/dev/ttyAMA3"
	# A running daemon writes this into its RuntimeDirectory, so a device that
	# is actually ready has one. The unit does not, which is exactly why the
	# `absent` case is a MISSING line rather than a silent pass.
	daemon_says state=resting watch=ok
}

# What the motion daemon has written about itself, as its own key=value lines.
daemon_says() {
	mkdir -p -- "$(dirname -- "$STATE")"
	printf '%s\n' "$@" >"$STATE"
	printf 'updated_unix=%s\n' "$(date +%s)" >>"$STATE"
}

run_tool() {
	set +e
	OUT=$(PATH="$STUBS:$PATH" "$TREE/firmware/tools/$(basename -- "$TOOL")" "$@" 2>&1)
	EC=$?
	set -e
}

# ── dispatch ──────────────────────────────────────────────────────────────────

new_tree
run_tool
check "no-host-refused" "$(yes_no [ "$EC" != 0 ])" "exit ${EC}: $OUT"
says "no-host-says-how-to-call-it" 'usage:'

# ── a device with nothing on it ───────────────────────────────────────────────

new_tree
run_tool reachy-dev
check "empty-device-exits-nonzero" "$(yes_no [ "$EC" = 1 ])" "exit ${EC}: $OUT"
says "empty-device-reports-the-missing-mount" 'MISSING.*/run/brenn-app is mounted'
says "empty-device-reports-the-missing-payload" 'MISSING.*audio payload'
says "empty-device-reports-the-dead-app-service" 'MISSING.*brenn-app.service is running'
says "empty-device-reports-the-missing-audio-conf" 'MISSING.*link configuration'
says "empty-device-reports-the-missing-bench-config" "MISSING.*machine's configuration"
says "empty-device-reports-the-missing-daemon-config" "MISSING.*motion daemon's configuration"
says "empty-device-reports-the-missing-token" "MISSING.*bus token"
says "empty-device-reports-the-missing-binary" 'MISSING.*motion daemon is deployed'
says "empty-device-reports-the-dead-motiond-service" 'MISSING.*reachy-motiond.service is running'
says "empty-device-reports-the-missing-port" 'MISSING.*servo bus node'
# Ten missing things, one fix. The whole point of naming it on every report is
# that nobody has to map a missing file back to the target that writes it.
says "empty-device-names-the-one-fix" 'make reachy-up'
# The record gates nothing any more, so its absence is stated and not counted.
says "empty-device-reports-the-record-without-judging-it" 'self-test: no record'
silent_about "empty-device-does-not-call-the-record-missing" 'MISSING.*self-test'

# ── a device with everything on it ────────────────────────────────────────────

new_tree
provision_all
run_tool reachy-dev
check "ready-device-exits-zero" "$(yes_no [ "$EC" = 0 ])" "exit ${EC}: $OUT"
says "ready-device-says-so" 'ready'
silent_about "ready-device-reports-nothing-missing" 'MISSING'
silent_about "ready-device-does-not-recommend-the-fix" 'make reachy-up'
# Every check is still asked. A check that quietly stopped being made would
# read exactly like a check that passed.
for label in 'is mounted' 'audio payload' 'brenn-app.service is running' \
	'link configuration' "machine's configuration" "motion daemon's configuration" \
	'bus token' 'motion daemon is deployed' 'reachy-motiond.service is running' \
	'servo bus node'; do
	says "ready-device-still-asks-about-${label// /-}" "OK .*${label}"
done

# One connection, not one per check: the answers should describe one moment,
# and a check per connection is a check per second of latency.
check "status-asks-once" \
	"$(yes_no [ "$(wc -l <"$SSH_ARGV")" = 1 ])" \
	"ssh argv was: $(cat -- "$SSH_ARGV")"
# Read-only, and nothing that moves. The probe is the only thing that runs on
# the device, so this is where "touches no servo" is enforceable.
check "status-runs-nothing-that-moves" \
	"$(no_yes grep -qE 'reachy-motiond |reachy-bench |systemctl (start|restart|stop)' -- "$PROBE")" \
	"the probe was: $(cat -- "$PROBE")"

# ── what the motion daemon says it is doing ───────────────────────────────────
#
# The check this command exists for as much as any file: a parked daemon does
# not exit, so its unit is active while it commands nothing at all. Every row
# below is a robot that would otherwise have been called `ready`.

new_tree
provision_all
run_tool reachy-dev
says "a-resting-daemon-is-ready" 'OK .*motion daemon is running and ready'

new_tree
provision_all
daemon_says state=active watch=ok
run_tool reachy-dev
check "an-active-daemon-is-ready-too" "$(yes_no [ "$EC" = 0 ])" "exit ${EC}: $OUT"
says "an-active-daemon-says-which-state" 'OK .*ready \(active\)'

# The line that stops a dead robot answering ready.
new_tree
provision_all
daemon_says state=parked watch=ok \
	'fault_stage=the motion loop' 'fault_detail=servo 4: timed out'
run_tool reachy-dev
check "a-parked-daemon-is-not-ready" "$(yes_no [ "$EC" = 1 ])" "exit ${EC}: $OUT"
says "a-parked-daemon-is-reported-as-faulted" 'MISSING.*FAULTED and is parked'
says "a-parked-daemon-names-the-stage" 'the motion loop'
says "a-parked-daemon-names-the-fault" 'servo 4: timed out'
says "a-parked-daemon-names-the-action" 'make reachy-motiond-logs'
silent_about "a-parked-daemon-is-not-called-ready" '^reachy-status.*: ready'
# Pushing files does not clear a fault, and saying so would send an operator
# round a loop that cannot end.
says "a-parked-daemon-says-the-push-does-not-fix-it" 'make reachy-up does not fix that'
silent_about "a-parked-daemon-is-not-blamed-on-a-missing-file" 'everything above is pushed by'

# Limp, safe, and unable to raise its head: the machine is at the minimum risk
# condition and recovers by itself, but nothing will wake until reads come back.
new_tree
provision_all
daemon_says state=resting watch=failing
run_tool reachy-dev
check "a-failing-watch-is-not-ready" "$(yes_no [ "$EC" = 1 ])" "exit ${EC}: $OUT"
says "a-failing-watch-says-the-head-will-not-raise" 'MISSING.*cannot read the machine'
says "a-failing-watch-says-it-recovers-by-itself" 'recovers by itself'

new_tree
provision_all
daemon_says state=starting watch=ok
run_tool reachy-dev
check "a-starting-daemon-is-not-yet-ready" "$(yes_no [ "$EC" = 1 ])" "exit ${EC}: $OUT"
says "a-starting-daemon-says-to-re-run" 'MISSING.*still coming up'

# The boot this feature was built for: a daemon that came up over a dead servo
# bus retries its startup look forever, so it holds `starting` until somebody
# fixes the cabling. Telling that operator to re-run in a moment is the one
# action that can never resolve it.
new_tree
provision_all
daemon_says state=starting watch=failing
run_tool reachy-dev
check "a-starting-daemon-over-a-dead-bus-is-not-ready" "$(yes_no [ "$EC" = 1 ])" \
	"exit ${EC}: $OUT"
says "a-starting-daemon-over-a-dead-bus-names-the-bus" 'MISSING.*cannot read the machine'
silent_about "a-starting-daemon-over-a-dead-bus-is-not-told-to-wait" 'still coming up'

# Mid-shutdown is not ready either, and it resolves on its own.
new_tree
provision_all
daemon_says state=stopping watch=ok
run_tool reachy-dev
check "a-stopping-daemon-is-not-ready" "$(yes_no [ "$EC" = 1 ])" "exit ${EC}: $OUT"
says "a-stopping-daemon-says-so" 'MISSING.*shutting down'
silent_about "a-stopping-daemon-is-not-called-ready" '^reachy-status.*: ready'

# A phase this script predates. Host and daemon are pushed separately, so the
# skew is a state this command will meet — and the answer that must never come
# out of it is `ready`.
new_tree
provision_all
daemon_says state=wedged watch=ok
run_tool reachy-dev
check "an-unknown-state-is-not-ready" "$(yes_no [ "$EC" = 1 ])" "exit ${EC}: $OUT"
says "an-unknown-state-says-it-does-not-know" 'MISSING.*does not know'
says "an-unknown-state-quotes-what-it-was-told" 'wedged'
silent_about "an-unknown-state-is-not-called-ready" '^reachy-status.*: ready'

# The unit is up and there is no file: either a daemon that has only just
# started, or one deployed before this file existed. Both are "come back", and
# neither is `ready`.
new_tree
provision_all
rm -f -- "$STATE"
run_tool reachy-dev
check "no-state-file-under-a-running-unit-is-not-ready" "$(yes_no [ "$EC" = 1 ])" \
	"exit ${EC}: $OUT"
says "no-state-file-says-what-to-do" 'MISSING.*written no state'

# With the unit down, its own MISSING line is the answer and the state is
# reported without being judged: RuntimeDirectory takes the file away on stop,
# so anything still there is a race with that.
new_tree
provision_all
rm -f -- "$ROOT/active/reachy-motiond.service"
run_tool reachy-dev
says "a-dead-unit-is-what-is-reported" 'MISSING.*reachy-motiond.service is running'
says "a-dead-unit-leaves-the-state-informational" '^  [-][-].*motion daemon state'
silent_about "a-dead-unit-does-not-double-report-the-daemon" 'MISSING.*motion daemon'

# ── the servo node the machine's own configuration names ──────────────────────

# A unit wired to another node: the bench file is the authority, and the report
# names the node it actually looked for rather than one it assumed.
new_tree
provision_all
printf '[envelope]\ndevice = "/dev/nonsense"\n\n[bus]\ndevice = "%s"  # this unit\n' \
	"$ROOT/dev/ttyAMA1" >"$ROOT/var/lib/brenn-app/reachy-bench.toml"
run_tool reachy-dev
check "another-node-is-missing-when-it-is-not-there" "$(yes_no [ "$EC" = 1 ])" \
	"exit ${EC}: $OUT"
says "the-report-names-the-node-the-bench-file-names" 'MISSING.*servo bus node.*/dev/ttyAMA1'
silent_about "a-device-key-outside-the-bus-table-is-not-read" 'nonsense'

new_tree
provision_all
printf '[bus]\ndevice = "%s"\n' "$ROOT/dev/ttyAMA1" \
	>"$ROOT/var/lib/brenn-app/reachy-bench.toml"
touch "$ROOT/dev/ttyAMA1"
run_tool reachy-dev
check "another-node-that-is-there-is-ok" "$(yes_no [ "$EC" = 0 ])" "exit ${EC}: $OUT"
says "the-ok-line-names-that-node" 'OK .*servo bus node.*/dev/ttyAMA1'

# With no bench file at all there is nothing to ask, so the default node stands
# — and the bench file's own MISSING line is the one that matters.
new_tree
provision_all
rm -f -- "$ROOT/var/lib/brenn-app/reachy-bench.toml"
touch "$ROOT/dev/ttyAMA3"
run_tool reachy-dev
says "with-no-bench-file-the-default-node-is-checked" 'OK .*servo bus node.*/dev/ttyAMA3'
says "with-no-bench-file-the-bench-file-is-what-is-missing" "MISSING.*machine's configuration"

# ── the self-test record, reported and never judged ───────────────────────────

new_tree
provision_all
{
	printf '[[cases]]\ncase = "presence"\noutcome = "Pass"\ndetail = "nine"\n'
	printf '[[cases]]\ncase = "datum"\noutcome = "Fail"\ndetail = "offsets"\n'
} >"$ROOT/var/lib/brenn-app/selftest-state.toml"
run_tool reachy-dev
check "a-failing-record-does-not-make-the-robot-unready" "$(yes_no [ "$EC" = 0 ])" \
	"exit ${EC}: $OUT"
says "the-record-is-counted-and-reported" 'self-test: 1 of 2'

# ── the device that never answered ────────────────────────────────────────────

new_tree
STUB_SSH_RC=255 run_tool reachy-dev
unset STUB_SSH_RC
check "unreachable-host-refuses" "$(yes_no [ "$EC" != 0 ])" "exit ${EC}: $OUT"
# "nothing is known" rather than "nothing is missing": a report that read an
# unreachable device as a ready one is the failure this command cannot have.
says "unreachable-host-says-nothing-is-known" 'nothing about this robot is known'
silent_about "unreachable-host-claims-nothing-is-ready" 'ready'

# ── the device that answered, and said nothing this command understands ───────
#
# The other half of "I know nothing", and the dangerous half: the connection
# landed and exited zero, so nothing above the parser refuses. With no check
# line to count, "nothing missing" and "nothing known" are the same reading —
# and one of them prints `ready` for a robot nobody looked at.

new_tree
STUB_SSH_SAYS='' run_tool reachy-dev
check "silent-probe-refuses" "$(yes_no [ "$EC" != 0 ])" "exit ${EC}: $OUT"
says "silent-probe-says-it-understood-nothing" 'answered nothing this command understands'
silent_about "silent-probe-is-not-ready" 'ready'

# The counter and not the emptiness is what decides: a probe that answered only
# the two informational keys produced output, and still asked nothing that can
# be OK or MISSING.
new_tree
STUB_SSH_SAYS=$'port_path=/dev/ttyAMA3\nselftest=no record on the device\n' run_tool reachy-dev
check "informational-only-probe-refuses" "$(yes_no [ "$EC" != 0 ])" "exit ${EC}: $OUT"
says "informational-only-probe-says-it-understood-nothing" 'answered nothing this command'
silent_about "informational-only-probe-is-not-ready" 'ready'

# And a banner is the reverse: it counts as an answer, so the report is a
# MISSING line rather than the guard — nonzero either way, ready neither way.
new_tree
STUB_SSH_SAYS=$'Welcome to brenn-os. Unauthorised access is prohibited.\n' run_tool reachy-dev
check "banner-only-probe-refuses" "$(yes_no [ "$EC" != 0 ])" "exit ${EC}: $OUT"
says "banner-only-probe-reports-it-as-missing" 'MISSING'
silent_about "banner-only-probe-is-not-ready" 'ready'

test_summary reachy-status.test.sh
