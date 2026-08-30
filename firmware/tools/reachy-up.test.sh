#!/usr/bin/env bash
#
# reachy-up.test.sh — host-only regression tests for the `reachy-up` target.
#
# `reachy-up` has no body beyond an ordering: the payload, then the
# configuration it reads, then the status check. The order is the whole
# deliverable, and it is only correct if the pod is on the device before it is
# given the address it dials and asked whether it is ready. Reordered, or a line
# lost in a merge, and the target still exits zero on a healthy unit and fails
# confusingly on the one it exists for: provisioning before the payload lands
# writes a configuration nothing reads yet, and a dropped `reachy-provision`
# leaves a pod waiting forever for an address.
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
status
STEPS
)
check "reachy-up-is-the-three-steps-in-order" "$(yes_no [ "$STEPS" = "$WANT" ])" \
	"the plan was:"$'\n'"$STEPS"

# Named on their own account, because each is a line somebody could drop while
# the sequence still looked plausible.
says "reachy-up-pushes-the-payload" 'deploy-reachy-pod\.sh .*--activate'
says "reachy-up-provisions-the-pod" 'provision-reachy-pod\.sh'
says "reachy-up-ends-by-asking-whether-the-pod-is-ready" 'reachy-status\.sh'

# Nothing in this sequence touches the motion stack: that half of a robot is
# deployed from brenn-reachy, and a line here pretending to bring it back would
# be a recovery that silently left the head dead.
silent_about "reachy-up-brings-up-no-motion-stack" 'motiond|motion'

# Nothing gates on a self-test record, so bringing the robot up does not run
# one — a self-test is a supervised bench act, not a step in a recovery that
# has to be safe to run unattended.
silent_about "reachy-up-runs-no-self-test" '[-][-](selftest|bench)( |$)'

test_summary reachy-up.test.sh
