#!/usr/bin/env bash
#
# Put the head-presence motion daemon on a device, and run it there.
#
#   tools/deploy-reachy-motiond.sh <host> --config <file>
#   tools/deploy-reachy-motiond.sh <host> --token <file>
#   tools/deploy-reachy-motiond.sh <host> --run [args...]
#
#   --config  push the daemon's configuration into the account's home. Separate
#             from --run because it changes rarely and a session is many runs.
#   --token   push the bearer token the bus attachment presents, mode 0600 and
#             owned by the account that reads it — the daemon refuses a token
#             file any other account can read.
#   --run     push the binary and run it in the foreground, as the account the
#             payload runs as. Everything after --run is passed to the daemon
#             verbatim; with nothing, it is given the configuration --config
#             pushed. The narration comes back over the terminal; the daemon's
#             JSONL is appended to a capture file on the device, which the run
#             names as it starts.
#
# The daemon is operator-run and supervised: it arms the machine, holds torque,
# and takes an operator's ^C as the one signal that stows, verifies and releases.
# So --run keeps a terminal, and nothing here installs a service.
#
# Three device paths, for three different reasons:
#
#   /run/brenn-app/releases  the binary. A tmpfs, so a push costs the device's
#       flash nothing and a reboot clears it. Binaries have to live here: /run
#       itself is mounted noexec and this submount deliberately is not.
#
#   /var/lib/brenn-app  the account's home — this daemon's configuration and its
#       token. Also RAM. They live here rather than beside the binary because
#       this is the account's own directory, mode 0700 and readable by the
#       process, where a release directory is root-owned and belongs to whatever
#       deploys into it.
#
#   the machine's own configuration, provisioned separately into that same
#       home. The daemon reads the bench file in place rather than a copy: the
#       crank datum, the envelope and the move durations describe one unit, and
#       two files describing one unit is two files to disagree.
#
# SSH lands as root and only root, which is why the run drops privilege: root
# opens any device node whatever udev said, so a serial port opened as root says
# nothing about the account that holds it in normal operation.
#
# Nothing here refuses while the audio payload is running. The pod carries no
# motion and never opens the servo bus — that separation is the point of the
# daemon existing — and a second speaker on the bus is answered where it has to
# be, by the port itself: the daemon refuses to start and names what holds it.

set -euo pipefail

# shellcheck source=lib.sh
. "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

binary="${arm64_target_dir}/release/reachy-motiond"

# One directory, reused, holding exactly one file. Nothing on this path
# activates a release and nothing prunes this directory: --delete would need a
# directory-to-directory sync, and a single file overwritten in place needs no
# pruning. A second artifact here would have to bring one with it.
release="${store_mount}/releases/motiond"

# Where the two files this script provisions land, and what the daemon is given
# when a run names no arguments of its own.
config_file="${app_home}/reachy-motiond.toml"
token_file="${app_home}/motiond-token"

# Where the daemon's structured output lands, on the device.
#
# The daemon writes narration to stderr and JSONL to stdout so a run can be both
# watched and parsed. A pty defeats that on its own: --run needs one so a ^C at
# the bench reaches the daemon, and a pty attaches the remote stdout and stderr
# to the same terminal, with CRLF translation on both. So the split is made on
# the device instead — stdout to this file, narration over the pty — and the
# capture is fetched afterwards. Appended, because a bring-up session is many
# runs and every record carries its own stamp.
capture_file="${app_home}/motiond-capture.jsonl"

usage() {
	die "usage: ${prog} <host> --config <file>|--token <file>|--run [args...]"
}

host=${1:-}
mode=${2:-}
[ -n "$host" ] || usage
shift 2 || usage

ssh_root() {
	ssh -o BatchMode=yes "root@${host}" "$@"
}

# Install one local file into the account's home on the device, over stdin.
#
# Over stdin rather than as an argument or a temporary file: for the token that
# is the whole point — the credential never reaches either machine's process
# table or shell history — and the configuration takes the same path so there is
# one way to do this rather than two. Mode 0600 and owned by the account,
# because it is the account that reads them.
install_for_app() {
	local src=$1 dest=$2
	[ -n "$src" ] || usage
	[ -f "$src" ] || die "no file at ${src}"
	echo "${prog}: pushing ${src} to ${host}:${dest}" >&2
	ssh_root "install -d -m 0700 -o ${app_user} -g ${app_user} -- ${app_home} &&
		install -m 0600 -o ${app_user} -g ${app_user} /dev/stdin ${dest}" <"$src"
}

case "$mode" in
	--config)
		install_for_app "${1:-}" "$config_file"
		;;

	--token)
		install_for_app "${1:-}" "$token_file"
		;;

	--run)
		[ -x "$binary" ] || die \
			"no device binary at ${binary}" \
			"Build one first: make reachy-motiond"

		echo "${prog}: pushing ${binary} to ${host}:${release}/" >&2
		ssh_root mkdir -p -- "$release"
		rsync -a -e "ssh -o BatchMode=yes" \
			"$binary" "root@${host}:${release}/reachy-motiond"

		# --init-groups is what puts the run in the dialout group, which is
		# what grants the serial node. Without it the drop would leave a run
		# with no supplementary groups at all and the port open would fail for
		# a reason that has nothing to do with the hardware.
		#
		# The working directory is the account's home, because that is where
		# the configuration and the machine's own bench file are.
		remote="cd ${app_home} || exit 1"
		# The capture is opened by this shell, still root, and inherited across
		# the drop — so the account never needs to be able to create it. umask
		# first, because what a fresh one is created with is this shell's.
		remote="${remote}; umask 0077"
		remote="${remote}; exec setpriv --reuid ${app_user} --regid ${app_user}"
		remote="${remote} --init-groups ${release}/reachy-motiond"
		if [ "$#" -eq 0 ]; then
			set -- "$config_file"
		fi
		for arg in "$@"; do
			remote="${remote} $(printf '%q' "$arg")"
		done
		remote="${remote} >>${capture_file}"

		echo "${prog}: capture appending to ${host}:${capture_file}" >&2
		echo "    fetch it with: ssh root@${host} cat ${capture_file}" >&2

		rc=0
		# A pty: the daemon narrates every move as it goes, and a ^C at the
		# bench is what reaches it. Without one the operator's only way to end
		# a run is to kill it, which leaves the servos holding.
		ssh -t -o BatchMode=yes "root@${host}" "$remote" || rc=$?
		if [ "$rc" = 255 ]; then
			die "ssh to root@${host} failed; the daemon did not run." \
				"Its own error is above."
		fi
		# The daemon's own verdict is this script's: 0 released, 6 faulted with
		# torque held, 7 parked with torque held.
		exit "$rc"
		;;

	*) usage ;;
esac
