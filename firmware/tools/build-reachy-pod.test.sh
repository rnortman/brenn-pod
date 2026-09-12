#!/usr/bin/env bash
#
# build-reachy-pod.test.sh — host-only regression tests for the payload build's
# build record.
#
# The compile itself is an emulated arm64 container and is not run here; the
# container is a stub that drops an ELF header where cargo would have left one.
# What is under test is the record the script writes beside that binary —
# `commit=`, `dirty=`, `sha256=` — because that record is a cross-repository
# interface: brenn-reachy's payload build runs this script, reads the record, and
# refuses to ship a pod whose record disagrees with the checkout it compiled or
# with the file it is about to stage. A record that quietly stopped matching the
# tree would turn that refusal into a build that always fails, or worse, into a
# payload whose two halves of this repository are different revisions with
# nothing saying so.
#
# The sharp case is `dirty`. Three readers have to agree on what a dirty tree is
# — the stamp compiled into the binary, this record, and brenn-reachy's check —
# and all three mean tracked edits only. A consumer that overlays this checkout
# into its own build litters generated files through it, so counting untracked
# files would call every overlaid build dirty and refuse it.

set -euo pipefail

HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
TOOL="${TOOL:-$HERE/build-reachy-pod.sh}" # overridable to run the suite against a modified tool

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# shellcheck source=test-lib.sh
. "$HERE/test-lib.sh"

STUBS="$WORK/stubs"
mkdir -p "$STUBS"

# Stubbed podman. `image exists` says the builder image is already cached, so no
# image build is attempted; `run` is the compile, and what it leaves behind is an
# ELF header for the architecture STUB_MACHINE names at the path the real cargo
# would have written.
cat >"$STUBS/podman" <<'PODMAN_EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$STUB_PODMAN_ARGV"
case "$1" in
image)
	exit 0
	;;
run)
	mkdir -p -- "$(dirname -- "$STUB_BINARY")"
	# ELF magic, then e_machine at byte 18 little-endian: 183 is AArch64.
	machine=${STUB_MACHINE:-183}
	printf '\177ELF\002\001\001\000\000\000\000\000\000\000\000\000\002\000' \
		>"$STUB_BINARY"
	printf "$(printf '\\%03o\\%03o' $((machine % 256)) $((machine / 256)))" \
		>>"$STUB_BINARY"
	# Something for the digest to be a digest of, distinct per run.
	printf 'compiled %s\n' "${STUB_BINARY_BODY:-once}" >>"$STUB_BINARY"
	# An edit landing in the checkout while the compile runs, which is the window
	# in which the binary's own stamp is read.
	if [ -n "${STUB_EDIT_DURING_RUN:-}" ]; then
		printf '# edited mid-build\n' >>"$STUB_EDIT_DURING_RUN"
	fi
	exit 0
	;;
esac
exit 1
PODMAN_EOF
chmod +x "$STUBS/podman"

# A PATH that is this machine's, minus git — the one condition a stub cannot
# express, because absence is what is being tested. Everything else on PATH is
# linked through rather than enumerated, so a script that later reaches for
# another ordinary command still runs here instead of failing as if git were the
# problem.
NOGIT="$WORK/nogit"
mkdir -p "$NOGIT"
IFS=: read -r -a path_dirs <<<"$PATH"
for dir in "${path_dirs[@]}"; do
	[ -d "$dir" ] || continue
	for cmd in "$dir"/*; do
		[ -x "$cmd" ] || continue
		name=$(basename -- "$cmd")
		[ "$name" = git ] && continue
		[ -e "$NOGIT/$name" ] || ln -s -- "$cmd" "$NOGIT/$name"
	done
done
ln -sf -- "$STUBS/podman" "$NOGIT/podman"

treenum=0
TREE=""
BINFMT=""
PODMAN_ARGV=""
PAYLOAD=""
RECORD=""

# A fresh fixture: the tool and its prelude where their own relative lookups land
# inside the tree, the Containerfile the image tag is named for, a usable binfmt
# registration, and a git checkout with one commit — which is the tree the record
# is about.
new_tree() {
	treenum=$((treenum + 1))
	TREE="$WORK/tree-$treenum"
	BINFMT="$TREE/binfmt"
	mkdir -p "$TREE/firmware/tools" "$TREE/firmware/containers/reachy-builder" \
		"$BINFMT"
	cp -- "$TOOL" "$HERE/lib.sh" "$TREE/firmware/tools/"
	chmod +x "$TREE/firmware/tools/$(basename -- "$TOOL")"
	printf 'FROM scratch\n' \
		>"$TREE/firmware/containers/reachy-builder/Containerfile"
	printf 'enabled\nflags: OCF\n' >"$BINFMT/qemu-aarch64"

	PAYLOAD="$TREE/firmware/target/reachy-pod/payload"
	RECORD="$PAYLOAD/reachy-pod.build"
	# Outside the tree: the stub appends to it, and a witness file inside the
	# checkout would make the checkout dirty by being written.
	PODMAN_ARGV="$WORK/podman-argv-$treenum"
	: >"$PODMAN_ARGV"

	git -C "$TREE" init -q -b main
	git -C "$TREE" -c user.email=t@t -c user.name=t add -A
	git -C "$TREE" -c user.email=t@t -c user.name=t commit -qm "the fixture tree"

	export STUB_PODMAN_ARGV="$PODMAN_ARGV"
	export STUB_BINARY="$TREE/firmware/target/reachy-arm64/release/reachy-pod"
	unset STUB_MACHINE STUB_BINARY_BODY
}

# The revision the fixture's record must name.
head_of_tree() { git -C "$TREE" rev-parse HEAD; }

# One `key=value` out of the record the run wrote.
field() { sed -n "s/^$1=//p" -- "$RECORD" | head -n 1; }

run_tool() {
	set +e
	OUT=$(PATH="${STUB_PATH:-$STUBS:$PATH}" REACHY_BINFMT_DIR="$BINFMT" \
		"$TREE/firmware/tools/$(basename -- "$TOOL")" "$@" 2>&1)
	EC=$?
	set -e
}

# ── the record a clean checkout produces ──────────────────────────────────────

new_tree
run_tool
expect_ok "a-clean-build-succeeds"
check "the-record-lands-beside-the-binary" "$(yes_no [ -f "$RECORD" ])" \
	"no record at ${RECORD}; output: $OUT"
commit_read=$(field commit)
check "the-record-names-the-checkouts-revision" \
	"$(yes_no [ "$(field commit)" = "$(head_of_tree)" ])" \
	"record says $(field commit), tree is at $(head_of_tree)"
# Forty, not twelve: the startup line abbreviates, the record does not, and the
# consumer's check compares the strings exactly.
check "the-revision-is-the-whole-forty" \
	"$(yes_no [ "${#commit_read}" = 40 ])" "record says ${commit_read}"
check "a-clean-checkout-is-not-dirty" "$(yes_no [ "$(field dirty)" = false ])" \
	"record says dirty=$(field dirty)"
# The digest is of the payload's copy, which is the file a consumer stages and
# digests in turn — not of the compiler's output and not of the tarball.
check "the-digest-is-the-payload-binarys" \
	"$(yes_no [ "$(field sha256)" = "$(sha256sum -- "$PAYLOAD/reachy-pod" | cut -d' ' -f1)" ])" \
	"record says $(field sha256)"
check "the-digest-is-not-the-tarballs" \
	"$(no_yes [ "$(field sha256)" = "$(sha256sum -- "$TREE/firmware/target/reachy-pod/payload.tar.gz" | cut -d' ' -f1)" ])" \
	"record says $(field sha256)"
says "the-report-names-what-it-built-at" "built at      $(head_of_tree)"

# The digest tracks the binary rather than being written once: a second build of
# a different binary records a different digest.
new_tree
STUB_BINARY_BODY=twice run_tool
expect_ok "a-second-build-succeeds"
check "the-digest-follows-the-binary" \
	"$(yes_no [ "$(field sha256)" = "$(sha256sum -- "$PAYLOAD/reachy-pod" | cut -d' ' -f1)" ])" \
	"record says $(field sha256)"

# ── what dirty means ──────────────────────────────────────────────────────────

# The overlay case, and the reason the flag is spelled --untracked-files=no in
# all three places that read it: a consumer that resolves this repository's
# crates from the working tree writes generated build files through it. Those are
# not modified sources, and a build that refused them would refuse every overlaid
# build there is.
new_tree
mkdir -p "$TREE/host/crates/speech-pipeline"
printf 'generated\n' >"$TREE/host/crates/speech-pipeline/BUILD.bazel"
printf 'generated\n' >"$TREE/scratch-note"
run_tool
expect_ok "a-build-over-untracked-files-succeeds"
check "untracked-files-do-not-make-the-tree-dirty" \
	"$(yes_no [ "$(field dirty)" = false ])" "record says dirty=$(field dirty)"

new_tree
printf 'FROM scratch\n# edited\n' \
	>"$TREE/firmware/containers/reachy-builder/Containerfile"
run_tool
expect_ok "a-build-over-a-tracked-edit-succeeds"
check "a-tracked-edit-makes-the-tree-dirty" \
	"$(yes_no [ "$(field dirty)" = true ])" "record says dirty=$(field dirty)"
says "the-report-says-a-dirty-build-was-dirty" "built at .*\(dirty\)"

# ── what cannot be recorded is refused ────────────────────────────────────────

# Not a checkout at all. The record is the only thing that can attribute the
# binary to a revision, so a build that cannot write one does not pretend to.
# Both refusals are properties of the host and the checkout, so they are owed
# before the compile: an emulated arm64 build of the whole workspace is minutes
# to be told at the end that its payload is unusable.
new_tree
rm -rf -- "$TREE/.git"
run_tool
expect_die "a-tree-with-no-revision-is-refused" "answers no revision"
check "an-unattributable-build-writes-no-record" \
	"$(yes_no [ ! -f "$RECORD" ])" "record was: $(cat -- "$RECORD" 2>/dev/null)"
check "an-unattributable-build-compiles-nothing" \
	"$(yes_no [ ! -s "$PODMAN_ARGV" ])" "podman was asked: $(cat -- "$PODMAN_ARGV")"

# A tree that moved under the build. The binary stamps what the compile saw, so a
# record written from the pre-build read would say `dirty=false` about a binary
# whose startup line says `+dirty` — and brenn-reachy's check, comparing the
# record against the checkout it reads afterwards, would pass that pair.
new_tree
STUB_EDIT_DURING_RUN="$TREE/firmware/containers/reachy-builder/Containerfile" \
	run_tool
expect_die "a-tree-that-moved-during-the-build-is-refused" "moved while the build ran"
says "the-refusal-names-the-tree-it-started-from" "stood at .*dirty=false"
says "the-refusal-names-what-it-found-after" "now stands at .*dirty=true"
check "a-build-over-a-moving-tree-writes-no-record" \
	"$(yes_no [ ! -f "$RECORD" ])" "record was: $(cat -- "$RECORD" 2>/dev/null)"

new_tree
STUB_PATH="$NOGIT" run_tool
expect_die "a-build-without-git-is-refused" "git is not installed"
says "the-refusal-says-what-reads-the-record" "brenn-reachy"
check "a-build-without-git-compiles-nothing" \
	"$(yes_no [ ! -s "$PODMAN_ARGV" ])" "podman was asked: $(cat -- "$PODMAN_ARGV")"

test_summary build-reachy-pod.test.sh
