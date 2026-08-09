#!/usr/bin/env bash
#
# deploy-reachy-motiond.test.sh — host-only regression tests for the motion
# daemon's deploy plumbing and the overlay mount its builds depend on.
#
# Everything these scripts decide before they reach a device or a container is
# text processing, and two of the decisions are expensive to get wrong:
#
#   * the exit status. The daemon's whole fault signal to an operator is its
#     verdict — 0 released, 6 faulted and released at the minimum risk
#     condition, 7 detached and released — and this script's contract is to pass
#     it through. A fault reported as a clean stop is a machine nobody goes to
#     look at.
#   * the overlay mount. While the workspace manifest redirects the motion
#     crates at a sibling clone, that mount is what makes *every* container
#     build in this repository resolve. A marker that stops matching drops it
#     silently, and the refusal written to say "clone brenn-reachy beside this
#     repository" is replaced by a cargo resolution failure deep inside an
#     emulated arm64 container.
#   * the unit. It is composed here and written to the device's tmpfs, so its
#     text is the only place the restart policy, the conditions that keep a
#     half-provisioned device quiet, and the stop budget that lets the head fold
#     are written down. A device is what would otherwise read them first.
#
# None of it needs a device, a container or a network, so all of it is asserted here,
# in the shape provision-reachy-pod.test.sh established: the function on its
# own, then the whole script out of a scratch tree with `ssh` and `rsync`
# stubbed on PATH.

set -euo pipefail

HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
TOOL="${TOOL:-$HERE/deploy-reachy-motiond.sh}" # overridable to run the suite against a modified tool

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# shellcheck source=test-lib.sh
. "$HERE/test-lib.sh"

# ── Layer 1: the overlay mount, on its own ────────────────────────────────────

# The real prelude, so what is asserted is the shipped function and the shipped
# constants. Each case overrides the two roots it reads inside a subshell, which
# is how a fixture manifest and a fixture clone get in front of it.
# shellcheck source=lib.sh
. "$HERE/lib.sh"

# The marker is a literal copy of a fragment of the workspace manifest, and a
# copy that has stopped matching is precisely the silent failure this half
# exists to catch. Only meaningful while the table it serves is there.
if grep -q '^\[patch\.' -- "$HERE/../Cargo.toml"; then
	check "marker-matches-the-workspace-manifest" \
		"$(yes_no grep -qF -- "$motion_patch_marker" "$HERE/../Cargo.toml")" \
		"the manifest carries a [patch] table that motion_patch_marker does not match"
fi

WITH_MARKER='[patch."https://github.com/rnortman/brenn-reachy"]
reachy-bench = { path = "../../brenn-reachy/crates/reachy-bench" }
'
NO_MARKER='[workspace]
members = ["devices/reachy-motiond"]
'

ELSEWHERE="$WORK/elsewhere/brenn-reachy"
mkdir -p "$ELSEWHERE/crates/reachy-bench"

overlaynum=0
OVERLAY_OUT=""
OVERLAY_EC=0
SIBLING=""

# run_overlay <manifest-body> [named-clone] — motion_overlay_volume against a
# fixture tree. The default lookup is `${repo_root}/../brenn-reachy`, so the
# fixture repo gets a sibling directory of its own; a case that wants the clone
# missing removes it, and one that wants it elsewhere names it.
run_overlay() {
	overlaynum=$((overlaynum + 1))
	local root="$WORK/overlay-$overlaynum/repo" named=${2:-}
	SIBLING="$WORK/overlay-$overlaynum/brenn-reachy"
	mkdir -p "$root/firmware" "$SIBLING/crates/reachy-bench"
	printf '%s' "$1" >"$root/firmware/Cargo.toml"
	[ -z "${3:-}" ] || rm -rf -- "$SIBLING"
	set +e
	OVERLAY_OUT=$(
		firmware_root="$root/firmware"
		repo_root="$root"
		if [ -n "$named" ]; then
			REACHY_MOTION_REPO=$named
		else
			unset REACHY_MOTION_REPO
		fi
		motion_overlay_volume 2>&1
	)
	OVERLAY_EC=$?
	set -e
}

# The state the day the pin lands: no table, so nothing is mounted and nothing
# is demanded of the workstation's layout.
run_overlay "$NO_MARKER"
check "overlay-absent-table-is-not-a-refusal" "$(yes_no [ "$OVERLAY_EC" = 0 ])" \
	"exit ${OVERLAY_EC}: ${OVERLAY_OUT}"
check "overlay-absent-table-mounts-nothing" "$(yes_no [ -z "$OVERLAY_OUT" ])" \
	"emitted '${OVERLAY_OUT}'"

# Today's state, with the clone where the default lookup expects it.
run_overlay "$WITH_MARKER"
check "overlay-sibling-clone-is-mounted-read-only" \
	"$(yes_no [ "$OVERLAY_OUT" = "${SIBLING}:/brenn-reachy:ro" ])" \
	"got '${OVERLAY_OUT}' (exit ${OVERLAY_EC})"

# The refusal has to name the escape hatch: the alternative an operator gets
# without it is a resolution failure inside an emulated container.
run_overlay "$WITH_MARKER" "" missing
check "overlay-missing-clone-refuses" "$(yes_no [ "$OVERLAY_EC" != 0 ])" \
	"exit ${OVERLAY_EC}, output '${OVERLAY_OUT}'"
check "overlay-missing-clone-names-the-override" \
	"$(yes_no grep -q REACHY_MOTION_REPO <<<"$OVERLAY_OUT")" \
	"refusal was: ${OVERLAY_OUT}"

# A clone somewhere else entirely, named for this invocation.
run_overlay "$WITH_MARKER" "$ELSEWHERE"
check "overlay-named-clone-is-mounted" \
	"$(yes_no [ "$OVERLAY_OUT" = "${ELSEWHERE}:/brenn-reachy:ro" ])" \
	"got '${OVERLAY_OUT}' (exit ${OVERLAY_EC})"

# A named path that is not a clone is refused rather than mounted empty, which
# would land as a cargo failure with nothing local to point at.
run_overlay "$WITH_MARKER" "$WORK/not-a-clone"
check "overlay-named-path-that-is-not-a-clone-refuses" \
	"$(yes_no [ "$OVERLAY_EC" != 0 ])" \
	"exit ${OVERLAY_EC}, output '${OVERLAY_OUT}'"

# ── Layer 2: the deploy script against a stubbed device ───────────────────────

STUBS="$WORK/stubs"
mkdir -p "$STUBS"

# Stubbed ssh: records every invocation's argv, captures the stdin of the
# install call, and answers the run — the one carrying -t — with a scripted
# code. Only that one, because the mkdir ahead of it failing would end the
# script through `set -e` and prove nothing about the passthrough.
cat >"$STUBS/ssh" <<'SSH_EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$STUB_SSH_ARGV"
pty=
last=
for arg in "$@"; do
	[ "$arg" = "-t" ] && pty=1
	last=$arg
done
if [ -n "$pty" ]; then
	printf '%s' "$last" >"$STUB_SSH_RUN"
	exit "${STUB_SSH_RC:-0}"
fi
case "$*" in
*install*) cat >"$STUB_SSH_STDIN" ;;
esac
# One remote command scripted to fail, named by a substring of its argv. The
# deploy path asks several questions over separate connections and what it does
# with a `no` differs per question, so a single scripted code cannot express it.
if [ -n "${STUB_SSH_FAIL:-}" ] && [[ $* == *"$STUB_SSH_FAIL"* ]]; then
	exit "${STUB_SSH_FAIL_RC:-1}"
fi
exit 0
SSH_EOF

# Stubbed rsync: records argv and nothing else. What it would have copied is
# asserted from that.
cat >"$STUBS/rsync" <<'RSYNC_EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$STUB_RSYNC_ARGV"
exit 0
RSYNC_EOF
chmod +x "$STUBS/ssh" "$STUBS/rsync"

treenum=0
TREE=""
SSH_ARGV=""
SSH_RUN=""
SSH_STDIN=""
RSYNC_ARGV=""
BINARY=""

# A fresh fixture tree: the tool and its prelude where their own relative
# lookups land inside the tree, and the stub sinks emptied.
new_tree() {
	treenum=$((treenum + 1))
	TREE="$WORK/tree-$treenum"
	mkdir -p "$TREE/firmware/tools" "$TREE/firmware/target/reachy-arm64/release"
	cp -- "$TOOL" "$HERE/lib.sh" "$TREE/firmware/tools/"
	chmod +x "$TREE/firmware/tools/$(basename -- "$TOOL")"
	BINARY="$TREE/firmware/target/reachy-arm64/release/reachy-motiond"
	SSH_ARGV="$TREE/ssh.argv"
	SSH_RUN="$TREE/ssh.run"
	SSH_STDIN="$TREE/ssh.stdin"
	RSYNC_ARGV="$TREE/rsync.argv"
	: >"$SSH_ARGV"
	: >"$RSYNC_ARGV"
	: >"$SSH_RUN"
	: >"$SSH_STDIN"
	export STUB_SSH_ARGV="$SSH_ARGV" STUB_SSH_RUN="$SSH_RUN"
	export STUB_SSH_STDIN="$SSH_STDIN" STUB_RSYNC_ARGV="$RSYNC_ARGV"
	unset STUB_SSH_RC STUB_SSH_FAIL STUB_SSH_FAIL_RC
	MOTION_REPO=""
}

# The binary the --run path insists on having built.
build_binary() {
	printf '#!/bin/false\n' >"$BINARY"
	chmod +x "$BINARY"
}

# Where the tool should look for the brenn-reachy clone, empty for "wherever it
# looks by default". Handed to the run rather than exported by a case, so the
# overlay layer above — which sets the same variable inside a subshell — cannot
# be read as the source of this one.
MOTION_REPO=""
run_tool() {
	set +e
	OUT=$(PATH="$STUBS:$PATH" REACHY_MOTION_REPO="$MOTION_REPO" \
		"$TREE/firmware/tools/$(basename -- "$TOOL")" "$@" 2>&1)
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
run_tool reachy-dev --config
expect_die "dispatch-config-with-no-file" "usage:"

new_tree
run_tool reachy-dev --config "$TREE/absent.toml"
expect_die "config-missing-file-refused" "no file at"
check "config-missing-file-touches-nothing" \
	"$(yes_no [ ! -s "$SSH_ARGV" ])" "ssh was called: $(cat -- "$SSH_ARGV")"

# ── the two provisioning modes ────────────────────────────────────────────────

new_tree
printf 'pod = "reachy00"\n' >"$TREE/local.toml"
run_tool reachy-dev --config "$TREE/local.toml"
expect_exit "config-push-succeeds" 0
check "config-push-sends-the-file-over-stdin" \
	"$(yes_no cmp -s -- "$TREE/local.toml" "$SSH_STDIN")" \
	"stdin was: $(cat -- "$SSH_STDIN")"
check "config-push-lands-in-the-account-home" \
	"$(yes_no grep -q 'install -m 0600 -o app -g app /dev/stdin /var/lib/brenn-app/reachy-motiond.toml' -- "$SSH_ARGV")" \
	"argv was: $(cat -- "$SSH_ARGV")"
check "config-push-creates-the-home-first" \
	"$(yes_no grep -q 'install -d -m 0700 -o app -g app -- /var/lib/brenn-app' -- "$SSH_ARGV")" \
	"argv was: $(cat -- "$SSH_ARGV")"

# The credential's whole reason for taking the stdin path: it reaches neither
# machine's process table, so neither the local file's name nor its contents are
# anywhere in a command line.
new_tree
printf 'not-a-real-token-0000\n' >"$TREE/tok"
run_tool reachy-dev --token "$TREE/tok"
expect_exit "token-push-succeeds" 0
check "token-push-sends-the-file-over-stdin" \
	"$(yes_no cmp -s -- "$TREE/tok" "$SSH_STDIN")" \
	"stdin was: $(cat -- "$SSH_STDIN")"
check "token-push-lands-mode-0600-owned-by-the-account" \
	"$(yes_no grep -q 'install -m 0600 -o app -g app /dev/stdin /var/lib/brenn-app/motiond-token' -- "$SSH_ARGV")" \
	"argv was: $(cat -- "$SSH_ARGV")"
check "token-contents-never-reach-a-command-line" \
	"$(no_yes grep -q not-a-real-token -- "$SSH_ARGV")" \
	"argv was: $(cat -- "$SSH_ARGV")"

# ── the run ───────────────────────────────────────────────────────────────────

new_tree
run_tool reachy-dev --run
expect_die "run-without-a-build-refused" "no device binary at"
check "run-without-a-build-touches-nothing" \
	"$(yes_no [ ! -s "$SSH_ARGV" ])" "ssh was called: $(cat -- "$SSH_ARGV")"

new_tree
build_binary
run_tool reachy-dev --run
expect_exit "run-clean-release-is-zero" 0
check "run-makes-the-release-directory" \
	"$(yes_no grep -q 'mkdir -p -- /run/brenn-app/releases/motiond' -- "$SSH_ARGV")" \
	"argv was: $(cat -- "$SSH_ARGV")"
check "run-pushes-the-binary-it-checked" \
	"$(yes_no grep -qF -- "${BINARY} root@reachy-dev:/run/brenn-app/releases/motiond/reachy-motiond" "$RSYNC_ARGV")" \
	"rsync argv was: $(cat -- "$RSYNC_ARGV")"
check "run-allocates-a-terminal-so-a-signal-reaches-the-daemon" \
	"$(yes_no grep -q '^-t ' -- "$SSH_ARGV")" "argv was: $(cat -- "$SSH_ARGV")"
check "run-drops-privilege-with-supplementary-groups" \
	"$(yes_no grep -q 'exec setpriv --reuid app --regid app --init-groups /run/brenn-app/releases/motiond/reachy-motiond' -- "$SSH_RUN")" \
	"remote command was: $(cat -- "$SSH_RUN")"
check "run-defaults-to-the-configuration-it-pushed" \
	"$(yes_no grep -q '/var/lib/brenn-app/reachy-motiond.toml' -- "$SSH_RUN")" \
	"remote command was: $(cat -- "$SSH_RUN")"
# The terminal merges the daemon's two streams, so the JSONL is split off on the
# device or it is not split off at all.
check "run-captures-the-daemon-jsonl-on-the-device" \
	"$(yes_no grep -q '>>/var/lib/brenn-app/motiond-capture.jsonl' -- "$SSH_RUN")" \
	"remote command was: $(cat -- "$SSH_RUN")"
check "run-says-where-the-capture-is" \
	"$(yes_no grep -q motiond-capture.jsonl <<<"$OUT")" "output was: $OUT"

# Arguments are the operator's, passed through as they were written — and one
# carrying a space must arrive as one argument, not two.
new_tree
build_binary
run_tool reachy-dev --run --config "/var/lib/brenn-app/other name.toml"
expect_exit "run-with-arguments-succeeds" 0
check "run-passes-arguments-through-quoted" \
	"$(yes_no grep -qF -- 'other\ name.toml' "$SSH_RUN")" \
	"remote command was: $(cat -- "$SSH_RUN")"
check "run-with-arguments-does-not-add-the-default" \
	"$(no_yes grep -q 'brenn-app/reachy-motiond.toml' -- "$SSH_RUN")" \
	"remote command was: $(cat -- "$SSH_RUN")"

# The passthrough. The daemon's verdict is the only thing that tells an operator
# — or a Make lane — that a machine faulted rather than stopped cleanly, and this
# script is the whole of the path it travels.
for rc in 6 7 1; do
	new_tree
	build_binary
	STUB_SSH_RC=$rc run_tool reachy-dev --run
	expect_exit "run-passes-through-exit-${rc}" "$rc"
	unset STUB_SSH_RC
done

# 255 is ssh's own, not the daemon's: it says the run never happened, which is a
# different thing to report than a machine that faulted.
new_tree
build_binary
STUB_SSH_RC=255 run_tool reachy-dev --run
unset STUB_SSH_RC
expect_die "run-ssh-failure-is-not-the-daemons-verdict" "did not run"

# A supervised run beside the service is two processes for one serial port. The
# flock refuses it either way; what this is for is saying which process holds it
# and how to stop that one.
new_tree
build_binary
run_tool reachy-dev --run
check "run-refuses-while-the-service-is-up" \
	"$(yes_no grep -q 'systemctl is-active --quiet reachy-motiond.service && exit 3' -- "$SSH_RUN")" \
	"remote command was: $(cat -- "$SSH_RUN")"

new_tree
build_binary
STUB_SSH_RC=3 run_tool reachy-dev --run
unset STUB_SSH_RC
expect_die "run-beside-the-service-names-the-unit" "reachy-motiond.service is running"

# ── the deploy ────────────────────────────────────────────────────────────────

new_tree
run_tool reachy-dev --deploy
expect_die "deploy-without-a-build-refused" "no device binary at"
check "deploy-without-a-build-touches-nothing" \
	"$(yes_no [ ! -s "$SSH_ARGV" ])" "ssh was called: $(cat -- "$SSH_ARGV")"

new_tree
build_binary
run_tool reachy-dev --deploy
expect_exit "deploy-returns" 0
check "deploy-pushes-the-binary-it-checked" \
	"$(yes_no grep -qF -- "${BINARY} root@reachy-dev:/run/brenn-app/releases/motiond/reachy-motiond" "$RSYNC_ARGV")" \
	"rsync argv was: $(cat -- "$RSYNC_ARGV")"
# The whole point of the target: it returns, and the daemon keeps running. A
# terminal anywhere in this path is the thing being removed.
check "deploy-holds-no-terminal" \
	"$(no_yes grep -q '^-t ' -- "$SSH_ARGV")" "argv was: $(cat -- "$SSH_ARGV")"
check "deploy-installs-the-unit-in-tmpfs" \
	"$(yes_no grep -q 'install -m 0644 /dev/stdin /run/systemd/system/reachy-motiond.service' -- "$SSH_ARGV")" \
	"argv was: $(cat -- "$SSH_ARGV")"
# A unit written but never reloaded is a file systemd has not read, and a reload
# without a restart leaves the old binary running. One invocation, so neither
# half can be the one that got interrupted.
check "deploy-reloads-and-restarts-in-one-invocation" \
	"$(yes_no grep -q 'systemctl daemon-reload && systemctl restart reachy-motiond.service' -- "$SSH_ARGV")" \
	"argv was: $(cat -- "$SSH_ARGV")"
check "deploy-asks-systemd-whether-it-came-up" \
	"$(yes_no grep -q 'systemctl is-active --quiet reachy-motiond.service' -- "$SSH_ARGV")" \
	"argv was: $(cat -- "$SSH_ARGV")"

# The unit itself, as installed. Read off the stdin the install call was given,
# which is the same file the device gets.
UNIT="$SSH_STDIN"
unit_says() {
	check "unit-$1" "$(yes_no grep -qF -- "$2" "$UNIT")" \
		"the unit was: $(cat -- "$UNIT")"
}
unit_says "runs-the-pushed-binary-with-the-pushed-config" \
	"ExecStart=/run/brenn-app/releases/motiond/reachy-motiond /var/lib/brenn-app/reachy-motiond.toml"
unit_says "runs-as-the-account-that-holds-the-port" "User=app"
unit_says "leaves-the-device-nodes-visible" "PrivateDevices=no"
# The three files a reboot clears. Without these a half-provisioned device
# crash-loops against a missing file instead of waiting for `make reachy-up`.
unit_says "waits-for-the-binary" \
	"ConditionPathExists=/run/brenn-app/releases/motiond/reachy-motiond"
unit_says "waits-for-the-configuration" \
	"ConditionPathExists=/var/lib/brenn-app/reachy-motiond.toml"
unit_says "waits-for-the-token" "ConditionPathExists=/var/lib/brenn-app/motiond-token"
# The fault doctrine in unit form: a crash restarts (commissioning and the
# startup fold re-run, which is safe), a fault and a futile bridge do not.
unit_says "restarts-a-crash" "Restart=on-failure"
unit_says "never-restarts-a-fault-or-a-futile-bridge" "RestartPreventExitStatus=6 7"
# The orderly stop is a stow move, a dwell, a verify sweep and the release.
unit_says "gives-the-stop-time-to-fold-the-head" "TimeoutStopSec=30"
# Where the daemon writes its state for reachy-status to read. systemd owns the
# directory's lifetime, which is what stops a stopped service leaving a stale
# `parked` behind for the next run to be judged by.
unit_says "gives-the-daemon-somewhere-to-say-what-it-is-doing" \
	"RuntimeDirectory=reachy-motiond"
# The file itself is a contract across two languages joined by nothing but the
# literal: the daemon writes this path, `reachy-status` reads it, and a
# `state::DEFAULT_PATH` unit test pins the same string on the Rust side. The
# unit's RuntimeDirectory is the directory half of it, checked above.
check "the-state-file-is-where-the-daemon-writes-it" \
	"$(yes_no [ "$motiond_state" = /run/reachy-motiond/state ])" \
	"lib.sh resolves it to: ${motiond_state}"

# A restart whose ConditionPathExists lines are unmet succeeds and leaves the
# unit inactive. Reported as "installed but not running" rather than as a
# successful deploy — this is the half-provisioned device the whole bring-up
# story exists for.
new_tree
build_binary
STUB_SSH_FAIL="is-active" run_tool reachy-dev --deploy
unset STUB_SSH_FAIL
expect_die "deploy-that-does-not-come-up-refuses" "not running"
check "deploy-that-does-not-come-up-names-the-fix" \
	"$(yes_no grep -q 'make reachy-up' <<<"$OUT")" "output was: $OUT"
check "deploy-that-does-not-come-up-shows-the-status" \
	"$(yes_no grep -q 'systemctl --no-pager --lines=20 status reachy-motiond.service' -- "$SSH_ARGV")" \
	"argv was: $(cat -- "$SSH_ARGV")"

# ── the machine's own configuration ───────────────────────────────────────────

new_tree
printf '[bus]\ndevice = "/dev/ttyAMA3"\n' >"$TREE/bench.toml"
run_tool reachy-dev --bench-config "$TREE/bench.toml"
expect_exit "bench-config-push-succeeds" 0
check "bench-config-push-sends-the-file-over-stdin" \
	"$(yes_no cmp -s -- "$TREE/bench.toml" "$SSH_STDIN")" \
	"stdin was: $(cat -- "$SSH_STDIN")"
check "bench-config-push-lands-where-the-daemon-reads-it" \
	"$(yes_no grep -q 'install -m 0600 -o app -g app /dev/stdin /var/lib/brenn-app/reachy-bench.toml' -- "$SSH_ARGV")" \
	"argv was: $(cat -- "$SSH_ARGV")"

# With no file named, the reviewed copy in the clone that authors it. That is
# what makes one command in this repository bring the whole robot back.
new_tree
CLONE="$TREE/brenn-reachy"
mkdir -p "$CLONE/crates/reachy-bench" "$CLONE/.local"
printf '[bus]\ndevice = "/dev/ttyAMA3"\n' >"$CLONE/.local/reachy-bench.toml"
MOTION_REPO="$CLONE"
run_tool reachy-dev --bench-config
expect_exit "bench-config-default-takes-the-clones-copy" 0
check "bench-config-default-sends-the-clones-file" \
	"$(yes_no cmp -s -- "$CLONE/.local/reachy-bench.toml" "$SSH_STDIN")" \
	"stdin was: $(cat -- "$SSH_STDIN")"

# A clone with no reviewed copy in it: refused naming both the file it wanted
# and the way to name another, rather than pushing nothing and reporting success.
rm -f -- "$CLONE/.local/reachy-bench.toml"
new_tree
MOTION_REPO="$CLONE"
run_tool reachy-dev --bench-config
expect_die "bench-config-default-missing-file-refuses" "no machine configuration at"
check "bench-config-default-missing-file-names-the-override" \
	"$(yes_no grep -q BENCH_CONFIG <<<"$OUT")" "output was: $OUT"

new_tree
MOTION_REPO="$WORK/no-clone-here"
run_tool reachy-dev --bench-config
MOTION_REPO=""
expect_die "bench-config-without-a-clone-refuses" "no brenn-reachy clone"
check "bench-config-without-a-clone-touches-nothing" \
	"$(yes_no [ ! -s "$SSH_ARGV" ])" "ssh was called: $(cat -- "$SSH_ARGV")"

# ── the journal ───────────────────────────────────────────────────────────────

new_tree
run_tool reachy-dev --logs
expect_exit "logs-succeeds" 0
check "logs-follows-the-units-journal" \
	"$(yes_no grep -q 'journalctl -u reachy-motiond.service -f' -- "$SSH_ARGV")" \
	"argv was: $(cat -- "$SSH_ARGV")"
check "logs-pushes-nothing" \
	"$(yes_no [ ! -s "$RSYNC_ARGV" ])" "rsync was called: $(cat -- "$RSYNC_ARGV")"

test_summary deploy-reachy-motiond.test.sh
