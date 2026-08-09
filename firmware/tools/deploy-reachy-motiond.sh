#!/usr/bin/env bash
#
# Put the head-presence motion daemon on a device, and leave it running there.
#
#   tools/deploy-reachy-motiond.sh <host> --config <file>
#   tools/deploy-reachy-motiond.sh <host> --token <file>
#   tools/deploy-reachy-motiond.sh <host> --bench-config [file]
#   tools/deploy-reachy-motiond.sh <host> --deploy
#   tools/deploy-reachy-motiond.sh <host> --run [args...]
#   tools/deploy-reachy-motiond.sh <host> --logs
#
#   --config  push the daemon's configuration into the account's home. Separate
#             from --deploy because it changes rarely and a session is many
#             deploys.
#   --token   push the bearer token the bus attachment presents, mode 0600 and
#             owned by the account that reads it — the daemon refuses a token
#             file any other account can read.
#   --bench-config
#             push the machine's own configuration — the crank datum, the
#             envelope, the move durations — into the same home. With no
#             argument it takes the sibling brenn-reachy clone's gitignored
#             .local/reachy-bench.toml, which is where the unit's reviewed copy
#             lives.
#   --deploy  push the binary, install the unit, restart it, report, and
#             return. The daemon runs unattended under systemd; nothing here
#             holds a terminal.
#   --run     push the binary and run it in the foreground instead, as the
#             account the payload runs as — the supervised bench form, refused
#             while the service is up. Everything after --run is passed to the
#             daemon verbatim; with nothing, it is given the configuration
#             --config pushed. The narration comes back over the terminal; the
#             daemon's JSONL is appended to a capture file on the device, which
#             the run names as it starts.
#   --logs    follow the service's journal. Pushes nothing.
#
# The daemon rests limp: torque is off whenever no motion script asks for the
# head, and every ending — a signal, a lapsed script, a fault — leaves it off
# again. That is what makes the service form safe: nobody has to be watching a
# terminal for the machine to end up at the minimum risk condition.
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
#   the machine's own configuration, in that same home. The daemon reads the
#       bench file in place rather than a copy: the crank datum, the envelope
#       and the move durations describe one unit, and two files describing one
#       unit is two files to disagree. This repository does not author that
#       file — brenn-reachy does — but --bench-config pushes it, so one command
#       here brings the whole robot up.
#
# The unit lands in /run/systemd/system, which is tmpfs: a reboot clears it
# along with the binary and the two configurations, and one `make reachy-up`
# puts all of it back. Nothing this pushes reaches the device's flash. Baking
# the unit into the OS image is a release-hardening act, not a dev convenience.
#
# SSH lands as root and only root, which is why the run drops privilege: root
# opens any device node whatever udev said, so a serial port opened as root says
# nothing about the account that holds it in normal operation. The service form
# gets the same treatment from systemd's User=.
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

# Where the files this script provisions land, and what the daemon is given when
# a run names no arguments of its own. The bench file's name is brenn-reachy's
# choice, not this script's: the daemon's configuration points at it and
# deploy-bench.sh writes it there.
config_file="${app_home}/reachy-motiond.toml"
token_file="${app_home}/motiond-token"
bench_config_file="${app_home}/reachy-bench.toml"

# The unit, and where it is written. /run/systemd/system is systemd's own
# runtime drop-in directory: it outranks /etc, it is RAM, and a daemon-reload is
# what makes a file there real.
service=reachy-motiond.service
unit_dir=/run/systemd/system
unit_path="${unit_dir}/${service}"

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
	die "usage: ${prog} <host> --config <file>|--token <file>|--bench-config [file]|--deploy|--run [args...]|--logs"
}

# The unit, composed here and installed over stdin.
#
# The three ConditionPathExists lines are what keeps a half-provisioned device
# quiet: after a reboot the binary and both configurations are gone with the
# tmpfs that held them, and a unit that ignored that would crash-loop against a
# missing file instead of waiting for `make reachy-up`.
#
# The restart policy is the fault doctrine in unit form. A crash restarts, and
# the restart re-runs commissioning and startup normalisation, which is safe:
# the daemon measures the machine and folds it. Exit 6 (faulted, and the machine
# is already at the minimum risk condition) and exit 7 (the bridge is futile —
# an auth or configuration problem) never restart, because retrying either one
# is noise an operator has to read past. A parked fault does not exit at all, so
# it cannot be restart-laundered.
#
# TimeoutStopSec clears the worst orderly stop — a stow move, the dwell, the
# verify sweep and the release — with margin. SIGTERM is the daemon's own
# shutdown path and needs no special casing here.
#
# The hardening mirrors brenn-app.service, with the one exception this daemon
# exists for: PrivateDevices stays off, because the servo bus is a device node
# and the `app` account's supplementary groups are what grant it.
unit_text() {
	cat <<-UNIT
		[Unit]
		Description=Reachy motion daemon (head presence)
		ConditionPathExists=${release}/reachy-motiond
		ConditionPathExists=${config_file}
		ConditionPathExists=${token_file}
		Wants=network-online.target
		After=network-online.target

		[Service]
		ExecStart=${release}/reachy-motiond ${config_file}
		WorkingDirectory=${app_home}
		User=${app_user}
		Group=${app_user}

		Restart=on-failure
		RestartSec=5s
		RestartPreventExitStatus=6 7
		TimeoutStopSec=30

		ProtectSystem=strict
		ProtectHome=yes
		ReadWritePaths=${app_home}
		NoNewPrivileges=yes
		PrivateDevices=no
		ProtectKernelTunables=yes
		ProtectKernelModules=yes
		ProtectControlGroups=yes
		RestrictSUIDSGID=yes
		LockPersonality=yes
	UNIT
}

# Push the binary into the executable store. Both modes that need one do this
# the same way, so a deploy and a supervised run cannot end up running different
# builds for different reasons.
push_binary() {
	[ -x "$binary" ] || die \
		"no device binary at ${binary}" \
		"Build one first: make reachy-motiond"

	echo "${prog}: pushing ${binary} to ${host}:${release}/" >&2
	ssh_root mkdir -p -- "$release"
	rsync -a -e "ssh -o BatchMode=yes" \
		"$binary" "root@${host}:${release}/reachy-motiond"
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

	--bench-config)
		bench_config=${1:-}
		if [ -z "$bench_config" ]; then
			# The unit's reviewed copy lives in the clone that authors it,
			# gitignored there because it holds one machine's crank datum. The
			# same clone the container builds mount, found the same way.
			motion_repo=$(motion_repo_root) || die \
				"no brenn-reachy clone to take the machine's configuration from." \
				"That file is brenn-reachy's — its .local/reachy-bench.toml is the reviewed" \
				"copy for this unit. Clone it beside this repository, name the clone with" \
				"REACHY_MOTION_REPO, or name the file for this invocation:" \
				"    make reachy-bench-config BENCH_CONFIG=<path>"
			bench_config="${motion_repo}/.local/reachy-bench.toml"
			[ -f "$bench_config" ] || die \
				"no machine configuration at ${bench_config}" \
				"That is the unit's reviewed bench file, written in brenn-reachy." \
				"Write it there, or name another for this invocation:" \
				"    make reachy-bench-config BENCH_CONFIG=<path>"
		fi
		install_for_app "$bench_config" "$bench_config_file"
		;;

	--deploy)
		push_binary

		# The unit goes over stdin like every other file this script installs,
		# and the reload and the restart ride the same invocation: a unit
		# written but not reloaded is a file systemd has never read, and the
		# two asked separately can be interrupted between.
		echo "${prog}: installing ${host}:${unit_path}" >&2
		remote="install -d -m 0755 -- ${unit_dir}"
		remote="${remote} && install -m 0644 /dev/stdin ${unit_path}"
		remote="${remote} && systemctl daemon-reload"
		remote="${remote} && systemctl restart ${service}"
		unit_text | ssh_root "$remote" || die \
			"could not install and restart ${service} on ${host}." \
			"ssh's own error is above."

		# What systemd makes of it, rather than what this script asked for. A
		# restart whose ConditionPathExists lines are unmet succeeds and leaves
		# the unit inactive — which is the half-provisioned device, and it is
		# exactly the failure that used to be discovered one refusal at a time.
		if ! ssh_root "systemctl is-active --quiet ${service}"; then
			ssh_root "systemctl --no-pager --lines=20 status ${service}" || true
			die "${service} is installed on ${host} but not running." \
				"Its status is above. Everything it needs is pushed by:" \
				"    make reachy-up"
		fi
		echo "${prog}: ${service} is running on ${host}" >&2
		echo "    follow it with: make reachy-motiond-logs" >&2
		;;

	--logs)
		ssh_root journalctl -u "$service" -f
		;;

	--run)
		push_binary

		# --init-groups is what puts the run in the dialout group, which is
		# what grants the serial node. Without it the drop would leave a run
		# with no supplementary groups at all and the port open would fail for
		# a reason that has nothing to do with the hardware.
		#
		# The working directory is the account's home, because that is where
		# the configuration and the machine's own bench file are.
		#
		# A supervised run wants the servo bus to itself, and the service is
		# the other thing on this device that opens it. Refused rather than
		# silently stopped: what is running on a device is the operator's to
		# decide. The refusal and the run are one remote invocation, because
		# asked separately the service can start in between; and a check whose
		# only signal is a nonzero exit reads ssh's own failures as the service
		# being down. Exit 3 is this script's answer for "it is running", and
		# nothing else produces it — the daemon's own codes are 0, 6 and 7. The
		# port's flock is the enforcement beneath this; this is the good
		# message.
		remote="systemctl is-active --quiet ${service} && exit 3"
		remote="${remote}; cd ${app_home} || exit 1"
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
		# bench reaches it — SIGINT and SIGTERM are the same signal to this
		# daemon, and both stow the head and take torque off. Without a pty the
		# only way to end a supervised run is to kill it from elsewhere, which
		# works but tells the operator watching nothing.
		ssh -t -o BatchMode=yes "root@${host}" "$remote" || rc=$?
		case "$rc" in
			3)
				die "${service} is running on ${host}, and a supervised run will not share the servo bus with it." \
					"Stop it, run in the foreground, and deploy again when you are done:" \
					"    ssh root@${host} systemctl stop ${service}"
				;;
			255)
				die "ssh to root@${host} failed; the daemon did not run." \
					"Its own error is above."
				;;
		esac
		# The daemon's own verdict is this script's: 0 released, 6 faulted and
		# released at the minimum risk condition, 7 detached and released.
		exit "$rc"
		;;

	*) usage ;;
esac
