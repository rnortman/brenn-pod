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
# Two artifacts land under target/reachy-pod/:
#
#   payload/      the payload tree — `run` and the binary at its root
#   payload.tar.gz  the same tree as the application server will serve it
#
# The deploy path rsyncs the tree; the tarball is what a payload host publishes.
# They are built together so a hand-deployed payload and a served one cannot be
# different things.
#
# Knobs, environment only:
#
#   REACHY_PODMAN   the podman to run (default podman)
#   REACHY_BINFMT_DIR   where to look for the binfmt registration, for testing
#                       the preflight on a host that has one
#   REACHY_MOTION_REPO  the motion clone the workspace's overlay names (default
#                       ../brenn-reachy beside this repo). This build compiles
#                       none of it, but a [patch] table resolves workspace-wide,
#                       so the container has to be able to see it.

set -euo pipefail

# shellcheck source=lib.sh
. "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

out_dir="${firmware_root}/target/reachy-pod"
payload_dir="${out_dir}/payload"
tarball="${out_dir}/payload.tar.gz"

binary_name=reachy-pod
binary="${arm64_target_dir}/release/${binary_name}"

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

report() {
	local size
	size=$(du -h -- "${payload_dir}/${binary_name}" | cut -f1)
	echo "${prog}: payload tree  ${payload_dir}  (binary ${size})"
	echo "${prog}: payload tar   ${tarball}"
	echo "${prog}: sha256        $(sha256sum -- "$tarball" | cut -d' ' -f1)"
}

container_preflight
tag=$(builder_image_tag)
ensure_builder_image "$tag"
container_build "$tag" "$binary_name"
verify_aarch64 "$binary"
assemble
report
