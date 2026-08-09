#!/usr/bin/env bash
#
# reachy-up.test.sh — host-only regression tests for the `reachy-up` target.
#
# `reachy-up` has no body beyond an ordering: payload, then the three
# configurations, then the daemon's binary and unit, then the status check. The
# order is the whole deliverable — six commands across two repositories became
# one, and the one is only correct if the service starts after everything it
# reads is already on the device. Reordered, or a line lost in a merge, and the
# target still exits zero on a healthy robot and fails confusingly on the one it
# exists for: `reachy-motiond-deploy` before `reachy-motiond-config` leaves
# systemd's ConditionPathExists unmet, and a dropped `reachy-bench-config` puts
# the daemon on a machine whose own configuration never arrived.
#
# `make -n` is what makes this testable without a device: recipe lines are
# printed rather than run, while lines naming $(MAKE) still recurse — so the
# whole expansion is printed, one line per tool invocation, and nothing runs.

set -euo pipefail

HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
FIRMWARE="$(cd -- "$HERE/.." && pwd)"

# shellcheck source=test-lib.sh
. "$HERE/test-lib.sh"

# A sub-make inherits this suite's own invocation otherwise, and the flags a
# `make check-host` run carries are not the flags being tested.
unset MAKEFLAGS MFLAGS

# Each recipe line that talks to a device, as the step it performs. Build steps
# and the REACHY_HOST guard are not steps of this sequence: they are
# prerequisites of the steps, and where they run is settled by the dependency
# graph rather than by this recipe.
plan() {
	while IFS= read -r line; do
		case "$line" in
		*deploy-reachy-pod.sh*--activate*) echo payload ;;
		*deploy-reachy-pod.sh*--selftest*) echo selftest ;;
		*deploy-reachy-pod.sh*--bench*) echo bench-selftest ;;
		*provision-reachy-pod.sh*) echo audio-conf ;;
		*deploy-reachy-motiond.sh*--bench-config*) echo bench-config ;;
		*deploy-reachy-motiond.sh*--config*) echo motiond-config ;;
		*deploy-reachy-motiond.sh*--token*) echo motiond-token ;;
		*deploy-reachy-motiond.sh*--deploy*) echo motiond-deploy ;;
		*reachy-status.sh*) echo status ;;
		esac
	done
}

run_tool() {
	set +e
	OUT=$(make -C "$FIRMWARE" -n "$@" REACHY_HOST=stub-host 2>&1)
	EC=$?
	set -e
}

# ── the sequence ──────────────────────────────────────────────────────────────

run_tool reachy-up
check "reachy-up-plans-without-a-device" "$(yes_no [ "$EC" = 0 ])" "exit ${EC}: $OUT"

STEPS=$(plan <<<"$OUT")
WANT=$(
	cat <<'STEPS'
payload
audio-conf
bench-config
motiond-config
motiond-token
motiond-deploy
status
STEPS
)
check "reachy-up-is-the-seven-steps-in-order" "$(yes_no [ "$STEPS" = "$WANT" ])" \
	"the plan was:"$'\n'"$STEPS"

# Named on their own account, because each is a line somebody could drop while
# the sequence still looked plausible.
says "reachy-up-pushes-the-machine-own-configuration" 'deploy-reachy-motiond\.sh .*--bench-config'
says "reachy-up-pushes-the-daemon-configuration" 'deploy-reachy-motiond\.sh .*--config'
says "reachy-up-pushes-the-bus-token" 'deploy-reachy-motiond\.sh .*--token'
says "reachy-up-ends-by-asking-whether-the-robot-is-ready" 'reachy-status\.sh'

# The service starts last of the daemon's steps: its unit's ConditionPathExists
# lines name the configuration and the token, so a restart before they land is a
# unit that stays dead and a deploy that refuses.
at() { grep -n -E "$1" <<<"$OUT" | head -1 | cut -d: -f1; }
DEPLOY_AT=$(at 'deploy-reachy-motiond\.sh .*--deploy')
CONFIG_AT=$(at 'deploy-reachy-motiond\.sh .*--config')
TOKEN_AT=$(at 'deploy-reachy-motiond\.sh .*--token')
WHERE="config at ${CONFIG_AT:-nowhere}, token at ${TOKEN_AT:-nowhere}, deploy at ${DEPLOY_AT:-nowhere}"
check "reachy-up-starts-the-service-after-its-configuration" \
	"$(yes_no [ "${DEPLOY_AT:-0}" -gt "${CONFIG_AT:-0}" ])" "$WHERE"
check "reachy-up-starts-the-service-after-its-token" \
	"$(yes_no [ "${DEPLOY_AT:-0}" -gt "${TOKEN_AT:-0}" ])" "$WHERE"

# Nothing gates on a self-test record, so bringing the robot up does not run
# one — a self-test is a supervised bench act, not a step in a recovery that
# has to be safe to run unattended.
silent_about "reachy-up-runs-no-self-test" '[-][-](selftest|bench)( |$)'

test_summary reachy-up.test.sh
