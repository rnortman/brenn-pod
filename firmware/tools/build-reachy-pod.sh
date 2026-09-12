#!/usr/bin/env bash
#
# Build the Reachy pod's payload: the aarch64 binary and the tree the device
# runs it from.
#
#   tools/build-reachy-pod.sh
#
# The compile happens inside the pinned Debian trixie arm64 container defined by
# containers/reachy-builder/Containerfile, so the binary is linked against the
# same dated archive the device image is bootstrapped from and the crate needs no
# cross-linker configuration. On a workstation that is not arm64 the container's
# instructions execute through the host's binfmt_misc registration, which is
# preflighted here: a missing registration is a refusal that says how to fix it,
# not a build that dies inside a dependency. All of that plumbing is lib.sh's,
# shared with every other binary this workspace puts on a Reachy.
#
# Three artifacts land under target/reachy-pod/:
#
#   payload/      the payload tree — `run` and the binary at its root
#   payload.tar.gz  the same tree as the application server will serve it
#   payload/reachy-pod.build  what this tree was when the binary was built
#
# The deploy path rsyncs the tree; the tarball is what a payload host publishes.
# They are built together so a hand-deployed payload and a served one cannot be
# different things.
#
# The build record beside the binary — `commit=`, `dirty=`, `sha256=` — is read
# by brenn-reachy, whose payload build runs this script and refuses to ship a pod
# whose record disagrees with the checkout it compiled or with the file it is
# about to stage. It is also the only place the full revision is written down:
# the binary itself prints the first twelve on its startup line.
#
# Knobs, environment only:
#
#   REACHY_PODMAN   the podman to run (default podman)
#   REACHY_BINFMT_DIR   where to look for the binfmt registration, for testing
#                       the preflight on a host that has one

set -euo pipefail

# shellcheck source=lib.sh
. "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

out_dir="${firmware_root}/target/reachy-pod"
payload_dir="${out_dir}/payload"
tarball="${out_dir}/payload.tar.gz"

binary_name=reachy-pod
binary="${arm64_target_dir}/release/${binary_name}"
record="${payload_dir}/${binary_name}.build"

# What the checkout is, as `record_preflight` reads it before the compile and
# `write_record` writes it after.
build_commit=
build_dirty=

assemble() {
	rm -rf -- "$payload_dir"
	mkdir -p -- "$payload_dir"
	cp -- "$binary" "${payload_dir}/${binary_name}"
	chmod 0755 -- "${payload_dir}/${binary_name}"

	# The whole contract with the operating system: an executable `run` at the
	# root of the tree. It execs rather than forks so the binary is the process
	# the service manager supervises and signals reach it directly.
	cat >"${payload_dir}/run" <<-'EOF'
		#!/bin/sh
		# The application payload's entry point. The working directory is the
		# payload root, and the pipeline reads its configuration from
		# /run/brenn-app/conf/audio.conf.
		exec ./reachy-pod run
	EOF
	chmod 0755 -- "${payload_dir}/run"

	# The archive's contents are the payload root: `run` at the top of the
	# archive, not inside a directory in it.
	tar -czf "$tarball" -C "$payload_dir" .
}

# What this checkout is, read before a byte is compiled.
#
# Beside `container_preflight` because it asks the same kind of question: a host
# without git, or a directory that is not a checkout, cannot produce a recorded
# payload, and that is worth an explained refusal in milliseconds rather than
# after an emulated arm64 build of the whole workspace. That is all the early
# read buys: what the record says is read again after the compile, because
# build-id's `build.rs` reads git from inside the container while it runs, and an
# edit made in that window lands in the binary's own stamp.
#
# Dirty is tracked edits only, the definition the compiled-in stamp uses
# (build-id's build.rs), so the record and the binary cannot disagree about what
# they are. Untracked files are excluded for a second reason too: a consumer
# overlaying this tree into its own build litters generated files through it, and
# that is not a modified source.
record_preflight() {
	command -v git >/dev/null 2>&1 ||
		die "git is not installed, so what this binary was built from cannot be recorded." \
			"The payload's build record is what brenn-reachy checks the pod against; a" \
			"payload without one is refused there. Install git."
	build_commit=$(git -C "$repo_root" rev-parse HEAD 2>/dev/null) && [ -n "$build_commit" ] ||
		die "${repo_root} answers no revision, so what this binary was built from cannot be recorded." \
			"The payload's build record names the commit the pod was compiled at, and only a" \
			"git checkout can say what that is."
	if [ -n "$(git -C "$repo_root" status --porcelain --untracked-files=no)" ]; then
		build_dirty=true
	else
		build_dirty=false
	fi
}

# What this checkout was when the compile ran, beside the binary it produced.
#
# The revision and the dirty flag are read again here, of the same tree the
# container compiled through its mount, and a tree that moved under the build is
# refused: the binary stamps what `build.rs` saw during the compile, so a record
# written from the earlier read could say `dirty=false` about a binary that says
# `+dirty` on its startup line — and brenn-reachy's check, which compares the
# record against the checkout, would pass the pair. The digest is of the
# payload's copy rather than the compiler's output, because the payload's copy is
# the file a consumer stages and digests in turn.
#
# Plain `key=value`, the form `audio.conf` and `provenance.txt` already use.
write_record() {
	local commit dirty
	commit=$(git -C "$repo_root" rev-parse HEAD 2>/dev/null) || commit=
	if [ -n "$(git -C "$repo_root" status --porcelain --untracked-files=no)" ]; then
		dirty=true
	else
		dirty=false
	fi
	[ "$commit" = "$build_commit" ] && [ "$dirty" = "$build_dirty" ] ||
		die "${repo_root} moved while the build ran: it stood at ${build_commit} (dirty=${build_dirty})" \
			"and now stands at ${commit:-no revision} (dirty=${dirty}). The binary carries what the" \
			"compile saw, so no record of this build can be trusted. Leave the tree alone and build again."
	{
		printf 'commit=%s\n' "$build_commit"
		printf 'dirty=%s\n' "$build_dirty"
		printf 'sha256=%s\n' \
			"$(sha256sum -- "${payload_dir}/${binary_name}" | cut -d' ' -f1)"
	} >"$record"
}

report() {
	local size
	size=$(du -h -- "${payload_dir}/${binary_name}" | cut -f1)
	echo "${prog}: payload tree  ${payload_dir}  (binary ${size})"
	echo "${prog}: payload tar   ${tarball}"
	echo "${prog}: sha256        $(sha256sum -- "$tarball" | cut -d' ' -f1)"

	local at
	at=$(record_field commit)
	if [ "$(record_field dirty)" = true ]; then
		at="${at} (dirty)"
	fi
	echo "${prog}: built at      ${at}"
}

# One `key=value` out of the record just written, for the report line.
record_field() {
	sed -n "s/^$1=//p" -- "$record" | head -n 1
}

container_preflight
record_preflight
tag=$(builder_image_tag)
ensure_builder_image "$tag"
container_build "$tag" "$binary_name"
verify_aarch64 "$binary"
assemble
write_record
report
