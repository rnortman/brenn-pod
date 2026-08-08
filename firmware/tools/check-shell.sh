#!/usr/bin/env bash
#
# The lint gate for firmware/tools.
#
# This directory is the device-facing shell — the builder container, the payload
# push, the provisioning tools and the motion daemon's deploy path. The scripts
# carry `# shellcheck source=lib.sh` directives that only `-x` reads.
#
# The linter is optional and its absence prints a visible SKIP, the same
# arrangement scripts/check.sh makes for the repo-root lane: a workstation
# without it is not blocked, and CI runners carry it.

set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

if ! command -v shellcheck >/dev/null 2>&1; then
	echo "tools/check-shell.sh: shellcheck not installed — SKIPPED for firmware/tools/*.sh"
	exit 0
fi

echo "tools/check-shell.sh: running shellcheck -x on firmware/tools/*.sh"
# -P SCRIPTDIR so a sourced prelude is found beside the script that sources it,
# whatever directory the lane runs from.
shellcheck -x -P SCRIPTDIR -- *.sh
