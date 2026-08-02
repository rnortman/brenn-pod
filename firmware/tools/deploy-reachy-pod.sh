#!/usr/bin/env bash
#
# Put a built payload on the device, and optionally do something with it.
#
#   tools/deploy-reachy-pod.sh <host> --activate
#   tools/deploy-reachy-pod.sh <host> --selftest
#   tools/deploy-reachy-pod.sh <host> --bench
#   tools/deploy-reachy-pod.sh <host> --logs
#
# The first three push the payload tree built by build-reachy-pod.sh into the
# device's payload store and differ only in what happens next:
#
#   --activate  hand the release to brenn-app-activate, which applies the
#               contract check, switches the current symlink and restarts the
#               application. This is the development deploy.
#   --selftest  run the unattended bring-up registry out of the pushed release
#               without activating it, as the account the application runs as.
#   --bench     the same, for the registry's manual cases — the ones that need
#               someone standing at the array to speak into it.
#   --logs      follow what the deployed payload is saying. Pushes nothing; it
#               is here so the unit name has one owner rather than two.
#
# The store is a tmpfs, so a push costs the device's flash nothing and a reboot
# clears it. Binaries have to live there for another reason too: /run itself is
# mounted noexec and the /run/brenn-app submount deliberately is not, so a tree
# rsynced anywhere else under /run cannot be executed.
#
# SSH lands as root and only root, which is why the self-test mode drops
# privilege before running anything: root opens any device node whatever udev
# said, so a permission assertion taken as root passes vacuously and says
# nothing about the account the payload actually runs as.

set -euo pipefail

prog=$(basename -- "$0")
firmware_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

payload_dir="${firmware_root}/target/reachy-pod/payload"

# Where brenn-os keeps unpacked payloads, and the tool that makes one current.
store=/run/brenn-app/releases
activate=/usr/sbin/brenn-app-activate
service=brenn-app.service

# The account the application runs as. The self-test observes the hardware as
# this account or it observes nothing worth knowing.
app_user=app

die() {
	echo "${prog}: $1" >&2
	shift
	local line
	for line in "$@"; do
		echo "    ${line}" >&2
	done
	exit 1
}

usage() {
	die "usage: ${prog} <host> --activate|--selftest|--bench|--logs"
}

host=${1:-}
mode=${2:-}
[ -n "$host" ] || usage
case "$mode" in
	--activate | --selftest | --bench | --logs) ;;
	*) usage ;;
esac

ssh_root() {
	ssh -o BatchMode=yes "root@${host}" "$@"
}

# Nothing to build and nothing to push: this mode only reads.
if [ "$mode" = --logs ]; then
	ssh_root journalctl -u "$service" -f
	exit
fi

[ -x "${payload_dir}/run" ] || die \
	"no payload tree at ${payload_dir}" \
	"Build one first: make reachy-pod"

# Where this push lands. brenn-app-activate is the only thing that prunes the
# store, and it removes every release but the one it just made current — so an
# activation can afford a name that sorts by time and still leave one tree
# behind. The self-test modes activate nothing, so nothing prunes them: they
# reuse one directory, which rsync --delete makes idempotent. The store is the
# device's RAM and a bench session is many runs.
case "$mode" in
	--activate) release="dev-$(date -u +%Y%m%dT%H%M%SZ)" ;;
	*) release="dev-selftest" ;;
esac
dir="${store}/${release}"

echo "${prog}: pushing ${payload_dir}/ to ${host}:${dir}/" >&2
ssh_root mkdir -p -- "$dir"
rsync -a --delete -e "ssh -o BatchMode=yes" \
	"${payload_dir}/" "root@${host}:${dir}/"

case "$mode" in
	--activate)
		ssh_root "$activate" "$dir"
		;;
	--selftest | --bench)
		# A self-test wants the board to itself. A running application holds
		# the capture stream, the playback stream and the USB control
		# interface, so a self-test alongside it would report device-busy for
		# every case and read as a hardware fault. Refused rather than
		# silently stopped: what is running on a device is the operator's to
		# decide.
		#
		# The refusal and the run are one remote invocation. Asked separately,
		# the service can start in between — from another terminal, or from an
		# activation — and the run lands beside it anyway; and a check whose
		# only signal is a nonzero exit reads ssh's own failures (unreachable
		# host, host-key refusal under BatchMode) as the service being down.
		# Exit 3 is this script's answer for "it is running", and nothing else
		# produces it.
		#
		# --init-groups is what puts the run in the audio group, which is what
		# grants both the raw USB node and /dev/snd. Without it the drop would
		# leave a run with no supplementary groups at all and every device
		# assertion would fail for a reason that has nothing to do with the
		# hardware. Built once so the two registries cannot drift apart on it.
		remote="systemctl is-active --quiet ${service} && exit 3"
		remote="${remote}; exec setpriv --reuid ${app_user} --regid ${app_user}"
		remote="${remote} --init-groups ${dir}/reachy-pod selftest"
		rc=0
		if [ "$mode" = --bench ]; then
			# The manual registry prints prompts and expects the operator to act
			# on them, so this one wants a terminal: ssh -t keeps the remote
			# output unbuffered at the pty and a ^C at the bench reaches the run.
			ssh -t -o BatchMode=yes "root@${host}" "${remote} --manual" || rc=$?
		else
			ssh_root "$remote" || rc=$?
		fi
		case "$rc" in
			3)
				die "${service} is running on ${host}, and it holds the sound card and the USB control interface." \
					"Stop it, run the self-test, and start it again when you are done:" \
					"    ssh root@${host} systemctl stop ${service}"
				;;
			255)
				die "ssh to root@${host} failed; the registry did not run." \
					"Its own error is above. A self-test that never reached the board is not a hardware reading."
				;;
		esac
		# The registry's own verdict is this script's.
		exit "$rc"
		;;
esac
