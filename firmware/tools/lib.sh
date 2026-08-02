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
