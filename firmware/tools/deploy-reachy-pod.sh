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
#               application. This is the development deploy. It refuses a unit
#               carrying the robot's payload, whose pod is deployed from
#               brenn-reachy with the rest of that stack.
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

# shellcheck source=lib.sh
. "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

payload_dir="${firmware_root}/target/reachy-pod/payload"

# Where brenn-os keeps unpacked payloads, and the tool that makes one current.
store="${store_mount}/releases"
activate=/usr/sbin/brenn-app-activate
service=brenn-app.service

# How a unit that is a robot says so: brenn-reachy pushes the robot's payload
# into this directory of the same store and writes that stamp at its root with
# the push. Two names owned by another repository, spelled here because this is
# where they are read; they move together, and a rename there turns this refusal
# off silently — which is why the refusal names them in its own text.
#
# The stamp rather than the directory: rsync --delete leaves the directory
# behind when a push is interrupted, and an empty tree is not a payload.
robot_release="${store}/motion"
robot_stamp="${robot_release}/provenance.txt"

# What the device answers with when it carries that payload. Its own code, like
# the running-service refusal's 3, so a remote shell's own failures are not read
# as this answer.
#
# Not exclusively this script's, on one connection: the activation replaces the
# guard with brenn-app-activate, whose status vocabulary belongs to brenn-os and
# is documented nowhere here, so a 4 from that connection can be either answer.
rc_robot=4

# Asked on the device, ahead of the thing it guards. A robot's payload is five
# applications under one launcher — the pod among them — and an activation here
# would make a pod-only payload current in their place: a robot that still talks
# and can no longer move, with nothing narrating why. The pod on a robot is
# deployed by brenn-reachy's tooling with the rest of the stack.
#
# Only the activation is guarded. The self-test modes activate nothing and
# replace nothing, and a bench self-test on a robot whose stack is stopped is a
# reading somebody wants; what it must not do is run beside a live stack holding
# the board, and the running-service refusal below is what says so.
robot_guard="[ -e ${robot_stamp} ] && exit ${rc_robot}"

# The refusal itself, once, because it is reached from two places.
refuse_robot() {
	die "${host} carries the robot's motion payload (${robot_stamp}), and this pushes a pod-only one." \
		"Activating it here would replace the whole stack — the launcher, the motion processes and the pod with it —" \
		"with a payload that cannot move the machine. Deploy the robot's pod from brenn-reachy:" \
		"    make -C ../brenn-reachy motion-deploy"
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

# The guard rides the connection that makes the push's directory, so a unit that
# is a robot is turned away before a byte of a pod-only payload reaches it.
echo "${prog}: pushing ${payload_dir}/ to ${host}:${dir}/" >&2
guard=true
[ "$mode" != --activate ] || guard=$robot_guard
rc=0
ssh_root "${guard}; mkdir -p -- ${dir}" || rc=$?
case "$rc" in
	0) ;;
	"$rc_robot") refuse_robot ;;
	# Anything else is the connection or the device, not this script's answer:
	# an unreachable host, a host key BatchMode would not accept, a store that
	# would not take the directory. Named here because 4 is the only status this
	# guard assigns a meaning to, and a bare status reads as a device verdict.
	*) die "ssh to root@${host} failed, or ${dir} could not be made on it (exit ${rc})." \
		"Its own error is above. Nothing was pushed and nothing was activated." ;;
esac
rsync -a --delete -e "ssh -o BatchMode=yes" \
	"${payload_dir}/" "root@${host}:${dir}/"

case "$mode" in
	--activate)
		# Asked again, in the same invocation as the activation. The push above
		# is a second connection, so a robot's payload can land between the two
		# — from another terminal, or from brenn-reachy's own deploy — and only
		# the question asked with the act is binding.
		rc=0
		ssh_root "${robot_guard}; exec ${activate} ${dir}" || rc=$?
		case "$rc" in
			0) ;;
			# The guard's code, or the activation tool's own: exec puts that
			# tool where the guard stood, so the number alone does not say
			# which. A robot's payload arriving in between turns the deploy
			# away, which is the direction to be wrong in.
			"$rc_robot")
				if ssh_root "[ -e ${robot_stamp} ]"; then
					refuse_robot
				fi
				die "activating ${dir} on ${host} failed (exit ${rc})." \
					"Asked again, the unit did not show the robot's stamp, so this is the activation tool's own status and not a refusal." \
					"This script changed nothing after the push; ask the unit what it is running:" \
					"    firmware/tools/reachy-status.sh ${host}"
				;;
			# Either the activation tool refused the release or the connection
			# never landed, and the two are not distinguishable from a status
			# alone — which is why this says both rather than exiting silently
			# on ssh's own number. What the unit is running is not claimed: an
			# activation that failed partway through is one of the shapes this
			# covers.
			*) die "activating ${dir} on ${host} failed (exit ${rc})." \
				"Its own error is above: either brenn-app-activate refused the release," \
				"or the connection to the unit did not land." \
				"This script changed nothing after the push; ask the unit what it is running:" \
				"    firmware/tools/reachy-status.sh ${host}" ;;
		esac
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
