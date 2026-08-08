#!/usr/bin/env bash
#
# Build the head-presence motion daemon for the device: an aarch64 executable.
#
#   tools/build-reachy-motiond.sh
#
# The same pinned arm64 container every other Reachy binary here is built in —
# see lib.sh for the preflight, the image pin and the architecture check. The
# artifact lands at target/reachy-arm64/release/reachy-motiond.
#
# There is no payload tree and no tarball. The daemon is not part of the
# brenn-app payload: it is pushed into the executable store and run in the
# foreground by an operator over ssh, so the only artifact is the binary.
#
# Knobs, environment only:
#
#   REACHY_PODMAN       the podman to run (default podman)
#   REACHY_BINFMT_DIR   where to look for the binfmt registration, for testing
#                       the preflight on a host that has one
#   REACHY_MOTION_REPO  the motion clone the workspace's overlay names (default
#                       ../brenn-reachy beside this repo)

set -euo pipefail

# shellcheck source=lib.sh
. "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

binary_name=reachy-motiond
binary="${arm64_target_dir}/release/${binary_name}"

report() {
	local size
	size=$(du -h -- "$binary" | cut -f1)
	echo "${prog}: device binary  ${binary}  (${size})"
	echo "${prog}: sha256         $(sha256sum -- "$binary" | cut -d' ' -f1)"
}

container_preflight
tag=$(builder_image_tag)
ensure_builder_image "$tag"
container_build "$tag" "$binary_name"
verify_aarch64 "$binary"
report
