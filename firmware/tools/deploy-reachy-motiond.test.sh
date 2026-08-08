#!/usr/bin/env bash
#
# deploy-reachy-motiond.test.sh — host-only regression tests for the motion
# daemon's deploy plumbing and the overlay mount its builds depend on.
#
# Everything these scripts decide before they reach a device or a container is
# text processing, and two of the decisions are expensive to get wrong:
#
#   * the exit status. The daemon's whole fault signal to an operator is its
#     verdict — 0 released, 6 faulted with torque held, 7 parked with torque
#     held — and this script's contract is to pass it through. A machine that
#     faulted holding torque reported as a clean release is a head left up with
#     nobody told.
#   * the overlay mount. While the workspace manifest redirects the motion
#     crates at a sibling clone, that mount is what makes *every* container
#     build in this repository resolve. A marker that stops matching drops it
#     silently, and the refusal written to say "clone brenn-reachy beside this
#     repository" is replaced by a cargo resolution failure deep inside an
#     emulated arm64 container.
#
# Neither needs a device, a container or a network, so both are asserted here,
# in the shape provision-reachy-pod.test.sh established: the function on its
# own, then the whole script out of a scratch tree with `ssh` and `rsync`
# stubbed on PATH.

set -euo pipefail

HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
TOOL="${TOOL:-$HERE/deploy-reachy-motiond.sh}" # overridable to run the suite against a modified tool

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

failures=0
casenum=0

fail() {
	echo "FAIL [$1]: $2"
	failures=$((failures + 1))
}

pass() { echo "ok   [$1]"; }

# check <name> <0-or-1> <what-went-wrong> — assert a condition the caller ran.
check() {
	casenum=$((casenum + 1))
	if [ "$2" = 0 ]; then pass "$1"; else fail "$1" "$3"; fi
}

# The two spellings of a condition, so an assertion reads as what it asserts.
yes_no() { if "$@"; then echo 0; else echo 1; fi; }
no_yes() { if "$@"; then echo 1; else echo 0; fi; }

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
	unset STUB_SSH_RC
}

# The binary the --run path insists on having built.
build_binary() {
	printf '#!/bin/false\n' >"$BINARY"
	chmod +x "$BINARY"
}

OUT=""
EC=0
run_tool() {
	set +e
	OUT=$(PATH="$STUBS:$PATH" "$TREE/firmware/tools/$(basename -- "$TOOL")" "$@" 2>&1)
	EC=$?
	set -e
}

# expect_die <name> <substring> — the tool refuses and says why.
expect_die() {
	casenum=$((casenum + 1))
	local name=$1 want=$2
	if [ "$EC" = 0 ]; then
		fail "$name" "expected a non-zero exit; output: $OUT"
		return
	fi
	if [[ $OUT != *"$want"* ]]; then
		fail "$name" "output missing '${want}'; output: $OUT"
		return
	fi
	pass "$name"
}

# expect_exit <name> <code>
expect_exit() {
	casenum=$((casenum + 1))
	local name=$1 want=$2
	if [ "$EC" != "$want" ]; then
		fail "$name" "exit ${EC}, wanted ${want}; output: $OUT"
		return
	fi
	pass "$name"
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
# — or a Make lane — that a machine is holding torque, and this script is the
# whole of the path it travels.
for rc in 6 7 1; do
	new_tree
	build_binary
	STUB_SSH_RC=$rc run_tool reachy-dev --run
	expect_exit "run-passes-through-exit-${rc}" "$rc"
	unset STUB_SSH_RC
done

# 255 is ssh's own, not the daemon's: it says the run never happened, which is a
# different thing to report than a machine that faulted holding torque.
new_tree
build_binary
STUB_SSH_RC=255 run_tool reachy-dev --run
unset STUB_SSH_RC
expect_die "run-ssh-failure-is-not-the-daemons-verdict" "did not run"

echo "----"
if [ "$failures" -ne 0 ]; then
	echo "deploy-reachy-motiond.test.sh: FAIL — ${failures} case(s) failed"
	exit 1
fi
echo "deploy-reachy-motiond.test.sh: OK — ${casenum} cases passed"
