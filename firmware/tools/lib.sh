# shellcheck shell=bash
#
# Shared prelude for the firmware/tools scripts. Sourced, never executed — no
# shebang, not executable:
#
#     # shellcheck source=lib.sh
#     . "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"
#
# The dirname dance in that line stays in every caller by necessity — it is what
# finds this file — but everything after it resolves here, so the device-side
# constants below have one home rather than one per tool.

# Everything here is read by the scripts that source this file, so "appears
# unused" is the expected shape of every definition in it.
# shellcheck disable=SC2034

# The name a script reports itself as in its own messages. Sourcing does not
# change $0, so this is the outer script's path.
prog=$(basename -- "$0")

firmware_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
repo_root=$(cd -- "${firmware_root}/.." && pwd)

# The account the application runs as on the device: the only account that must
# be able to read the pod's key, and the account a self-test has to observe the
# hardware as or it observes nothing worth knowing.
app_user=app

# Where brenn-os mounts the payload store. A tmpfs, so nothing under it costs the
# device's flash a write and a reboot clears it; /run itself is noexec and this
# submount deliberately is not, so a payload has to live here to be executable.
# The unpacked releases and the pod's configuration are both under it — the
# latter path is also compiled into the pod (devices/reachy-pod/src/config.rs,
# CONF_DIR).
store_mount=/run/brenn-app

# The account's home on the device: writable, mode 0700, and on the volatile
# /var, so it is RAM and costs the eMMC no write. Configuration a binary reads
# and state it writes live here rather than beside the binary — a release
# directory is root-owned and belongs to whatever deploys into it. Must match the
# path the bench configuration is provisioned to; the daemon reads it in place.
app_home=/var/lib/brenn-app

# Fail with a headline and any number of indented detail lines.
die() {
	echo "${prog}: $1" >&2
	shift
	local line
	for line in "$@"; do
		echo "    ${line}" >&2
	done
	exit 1
}

# ── The arm64 builder container ─────────────────────────────────────────────
#
# Every Linux binary this workspace puts on a Reachy is compiled in the pinned
# container defined by containers/reachy-builder/Containerfile, against the same
# dated archive the device image is bootstrapped from. The plumbing below is
# shared by every such build so the pin, the preflight and the architecture
# check have one definition rather than one per binary.

podman=${REACHY_PODMAN:-podman}

# A fixed path rather than the host's, so what a build records of its own paths
# is the same on every machine that runs it.
container_repo=/src

# The container's build products stay out of the host toolchain's target
# directory: same paths, different architecture, and cargo would rebuild the
# world on every alternation between the two. One pair for every binary built
# this way — same workspace, same profile, same toolchain — so a second binary
# reuses the first one's dependency builds instead of doubling the RAM-cheap,
# time-expensive part of an emulated compile.
arm64_target_dir="${firmware_root}/target/reachy-arm64"
arm64_cargo_home="${firmware_root}/target/reachy-cargo-home"

# The build is emulated, so a misconfigured platform flag produces an x86_64
# binary that runs perfectly on the workstation and not at all on the device.
elf_machine_aarch64=183

# Whether an aarch64 binfmt_misc registration usable from inside a container is
# present. The F flag is what makes it usable: it opens the interpreter at
# registration time, so the interpreter does not have to exist in the
# container's own filesystem.
#
# REACHY_BINFMT_DIR names where to look, for testing the preflight on a host
# that has one.
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

# Refuse before starting rather than dying deep inside a dependency: no podman,
# or no way to execute arm64 instructions on this workstation.
container_preflight() {
	command -v -- "$podman" >/dev/null 2>&1 ||
		die "the device binary is built in a container and ${podman} is not installed." \
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
builder_image_tag() {
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

ensure_builder_image() {
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

# What an active motion overlay looks like in the workspace manifest, and where
# the clone it names has to appear inside the container. The patch paths are
# relative to firmware/Cargo.toml, so `../../brenn-reachy` under
# ${container_repo}/firmware resolves one level above the repo mount.
#
# These stand whether or not a table does: an absent table mounts nothing, so
# the overlay costs a grep on the cycles between the cross-repo changes it
# serves.
motion_patch_marker='path = "../../brenn-reachy/'
container_motion_repo=/brenn-reachy

# The volume specification the motion overlay needs, or nothing when the
# manifest carries no overlay to serve.
#
# Every build in this workspace needs it while the table stands, not only the
# daemon's: a `[patch]` table is applied during workspace-wide resolution, so a
# container that cannot see the clone fails to resolve before it compiles
# anything at all.
#
# Read-only: these builds compile the clone's sources and write nothing back
# into it. REACHY_MOTION_REPO names the clone when it is not beside this repo.
motion_overlay_volume() {
	grep -qF -- "$motion_patch_marker" "${firmware_root}/Cargo.toml" || return 0

	local root=${REACHY_MOTION_REPO:-${repo_root}/../brenn-reachy}
	[ -d "${root}/crates/reachy-bench" ] || die \
		"the workspace manifest redirects the motion crates at a clone beside this repo, and there is none at ${root}." \
		"Clone brenn-reachy beside this repository, or name the clone for this invocation:" \
		"    REACHY_MOTION_REPO=<path> make <target>"
	root=$(cd -- "$root" && pwd)
	echo "${root}:${container_motion_repo}:ro"
}

# Build one workspace package in the container.
container_build() {
	local tag=$1 package=$2
	# Expanded as ${mounts[@]+…} below: an empty array under `set -u` is an
	# unbound variable on bash before 4.4, and empty is what this becomes the
	# day the overlay's table goes away.
	local mounts=() overlay
	overlay=$(motion_overlay_volume)
	if [ -n "$overlay" ]; then
		echo "${prog}: overlaying the motion clone at ${overlay%%:*}" >&2
		mounts+=(--volume "$overlay")
	fi
	mkdir -p -- "$arm64_target_dir" "$arm64_cargo_home"

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
		${mounts[@]+"${mounts[@]}"} \
		--workdir "${container_repo}/firmware" \
		--env "CARGO_TARGET_DIR=${container_repo}/firmware/target/reachy-arm64" \
		--env "CARGO_HOME=${container_repo}/firmware/target/reachy-cargo-home" \
		"$tag" cargo build --release -p "$package"
}

# Refuse a binary the device cannot execute, and make the one it can executable.
verify_aarch64() {
	local binary=$1
	[ -f "$binary" ] || die "the build left no binary at ${binary}"

	# e_machine — bytes 18 and 19 of the ELF header, little-endian. Read with od
	# so the check costs no tooling a workstation might not carry.
	local machine
	machine=$(od -An -tu1 -j18 -N2 -- "$binary" | awk '{print $1 + $2 * 256}')
	[ "$machine" = "$elf_machine_aarch64" ] || die \
		"the build produced an ELF for machine ${machine}, not AArch64 (${elf_machine_aarch64})." \
		"The container ran on the wrong architecture; the device cannot execute this."

	chmod 0755 -- "$binary"
}
