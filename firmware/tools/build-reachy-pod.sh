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
# not a build that dies inside a dependency.
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

set -euo pipefail

prog=$(basename -- "$0")
firmware_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
repo_root=$(cd -- "${firmware_root}/.." && pwd)

podman=${REACHY_PODMAN:-podman}

# Where the repo appears inside the container. A fixed path rather than the
# host's, so what the build records of its own paths is the same on every
# machine that runs this.
container_repo=/src

out_dir="${firmware_root}/target/reachy-pod"
payload_dir="${out_dir}/payload"
tarball="${out_dir}/payload.tar.gz"

# The container's build products stay out of the host toolchain's target
# directory: same paths, different architecture, and cargo would rebuild the
# world on every alternation between the two.
target_dir="${firmware_root}/target/reachy-arm64"
cargo_home="${firmware_root}/target/reachy-cargo-home"

binary_name=reachy-pod
binary="${target_dir}/release/${binary_name}"

# ELF e_machine for AArch64. The build is emulated, so a misconfigured platform
# flag produces an x86_64 binary that runs perfectly on the workstation and not
# at all on the device.
elf_machine_aarch64=183

die() {
	echo "${prog}: $1" >&2
	shift
	local line
	for line in "$@"; do
		echo "    ${line}" >&2
	done
	exit 1
}

# Whether an aarch64 binfmt_misc registration usable from inside a container is
# present. The F flag is what makes it usable: it opens the interpreter at
# registration time, so the interpreter does not have to exist in the
# container's own filesystem.
binfmt_ready() {
	local dir reg
	dir=${REACHY_BINFMT_DIR:-/proc/sys/fs/binfmt_misc}
	for reg in "${dir}"/qemu-aarch64*; do
		[ -f "$reg" ] || continue
		grep -qx enabled "$reg" 2>/dev/null || continue
		grep -q '^flags:.*F' "$reg" 2>/dev/null || continue
		return 0
	done
	return 1
}

preflight() {
	command -v -- "$podman" >/dev/null 2>&1 ||
		die "the payload is built in a container and ${podman} is not installed." \
			"Install podman, or point REACHY_PODMAN at the one to use."

	case "$(uname -m)" in
		aarch64 | arm64) return 0 ;;
	esac

	binfmt_ready || die \
		"no usable aarch64 binfmt_misc registration, so an arm64 container cannot run on this $(uname -m) host." \
		"Install qemu-user-static, then check that the aarch64 registration is enabled" \
		"and carries the F flag:" \
		"    cat /proc/sys/fs/binfmt_misc/qemu-aarch64" \
		"Fedora registers it that way by default; on Debian and Ubuntu the" \
		"qemu-user-static package does."
}

# The image is named for the content of its definition, so editing the file or
# bumping a pin in it invalidates the cached image rather than silently reusing
# one built from an older definition.
image_tag() {
	local digest
	digest=$(sha256sum -- "${firmware_root}/containers/reachy-builder/Containerfile" | cut -c1-12) ||
		die "cannot read containers/reachy-builder/Containerfile, so the builder image cannot be named."
	# localhost/ explicitly: an unqualified name is a short name, and podman
	# resolves those against the host's unqualified-search-registries. The
	# builder image exists only here, and `brenn-pod/reachy-builder` is a name
	# we do not own on any public registry — so a miss must be a local error,
	# never a pull of whoever registered that name.
	echo "localhost/brenn-pod/reachy-builder:${digest}"
}

ensure_image() {
	local tag=$1
	if "$podman" image exists "$tag"; then
		return 0
	fi
	echo "${prog}: building the builder image ${tag} — first use of this definition" >&2
	"$podman" build \
		--platform linux/arm64 \
		--tag "$tag" \
		--file "${firmware_root}/containers/reachy-builder/Containerfile" \
		-- "${firmware_root}/containers/reachy-builder"
}

compile() {
	local tag=$1
	mkdir -p -- "$target_dir" "$cargo_home"

	# Rootless podman, uid 0 inside its user namespace — host-side this grants
	# nothing beyond the invoking user's own privileges, and the build products
	# land owned by the invoking user. SELinux labelling is disabled for this
	# container rather than relabelling the developer's checkout, which is what
	# mounting it with :z would do.
	"$podman" run --rm \
		--platform linux/arm64 \
		--pull=never \
		--security-opt label=disable \
		--volume "${repo_root}:${container_repo}" \
		--workdir "${container_repo}/firmware" \
		--env "CARGO_TARGET_DIR=${container_repo}/firmware/target/reachy-arm64" \
		--env "CARGO_HOME=${container_repo}/firmware/target/reachy-cargo-home" \
		"$tag" cargo build --release -p "$binary_name"
}

# e_machine — bytes 18 and 19 of the ELF header, little-endian. Read with od so
# the check costs no tooling a workstation might not carry.
elf_machine() {
	od -An -tu1 -j18 -N2 -- "$1" | awk '{print $1 + $2 * 256}'
}

assemble() {
	[ -f "$binary" ] || die "the build left no binary at ${binary}"

	local machine
	machine=$(elf_machine "$binary")
	[ "$machine" = "$elf_machine_aarch64" ] || die \
		"the build produced an ELF for machine ${machine}, not AArch64 (${elf_machine_aarch64})." \
		"The container ran on the wrong architecture; the device cannot execute this."

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
		# ${BRENN_DATA_DIR}/audio.conf.
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

preflight
tag=$(image_tag)
ensure_image "$tag"
compile "$tag"
assemble
report
