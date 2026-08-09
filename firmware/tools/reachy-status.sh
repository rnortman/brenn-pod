#!/usr/bin/env bash
#
# Say whether a Reachy is ready, without touching a servo.
#
#   tools/reachy-status.sh <host>
#
# Every refusal this stack makes is well-built and none of them is reachable
# before you run the thing: the daemon's missing-bench-file refusal, the pod's
# patient wait for a configuration that is not there, the flock on the servo
# port. They surface one at a time, each after the previous one is fixed, and
# the cheapest of them costs a torqued servo to discover. This command asks all
# of those questions at once, reads only, and moves nothing.
#
# What it checks is what a reboot clears: the payload store is tmpfs, the
# account's home is tmpfs, and the unit that runs the motion daemon is written
# into tmpfs too. So every MISSING line here has the same fix — `make reachy-up`
# pushes all of it — and the line says so rather than making anyone map a
# missing file to the target that writes it.
#
# One ssh invocation, because a check per connection is a check per second of
# latency and because the answers should describe one moment. The device side
# emits `key=value` lines and every judgement about them is made here, where the
# fixing commands and this repository's names for things are.
#
# One question here is not about a file: what the motion daemon says it is
# doing. A parked daemon — one whose machine faulted — deliberately does not
# exit, because a fault is never auto-cleared and a process that exited would let
# systemd's restart policy re-torque a machine nobody has looked at. The cost is
# that `systemctl is-active` calls it running. So the daemon writes its state to
# a file and this reads it, which is what stops a dead robot answering `ready`.
# Its fix is not `make reachy-up`, so it is counted and reported apart.
#
# The self-test record is reported and never judged: nothing in either
# workspace gates on it any more. It is a diagnostic, and its absence is not a
# reason this robot is not ready.

set -euo pipefail

# shellcheck source=lib.sh
. "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

host=${1:-}
[ -n "$host" ] || die "usage: ${prog} <host>"

# The device-side names. The paths are lib.sh's; the units and the record are
# named by the two things that install them.
app_service=brenn-app.service
motiond_service=reachy-motiond.service
motiond_binary="${store_mount}/releases/motiond/reachy-motiond"
payload_run="${store_mount}/current/run"
audio_conf="${store_mount}/conf/audio.conf"
bench_config="${app_home}/reachy-bench.toml"
motiond_config="${app_home}/reachy-motiond.toml"
motiond_token="${app_home}/motiond-token"
selftest_record="${app_home}/selftest-state.toml"

# The serial node when the machine's own configuration is not there to name one.
# The bench file's [bus] device is the authority — a unit wired to another node
# says so there — and the probe below reads it when the file has arrived.
default_port=/dev/ttyAMA3

# The remote probe. Written to say what it found and decide nothing: a device
# that answers "the unit is inactive" is a fact, and whether that is a problem
# is this side's call.
#
# `command -v systemctl` is not checked: this runs on brenn-os, systemd is pid
# 1 there, and a missing systemctl would be a broken image rather than a
# condition to report politely.
probe() {
	cat <<PROBE
say() { printf '%s=%s\n' "\$1" "\$2"; }

# present <key> <test-flag> <path>
present() { if [ "\$2" "\$3" ]; then say "\$1" ok; else say "\$1" missing; fi; }

# active <key> <unit>
active() { if systemctl is-active --quiet "\$2"; then say "\$1" ok; else say "\$1" missing; fi; }

if findmnt -rno TARGET -- ${store_mount} >/dev/null 2>&1; then
	say mount ok
else
	say mount missing
fi

present payload -x ${payload_run}
active app_service ${app_service}
present audio_conf -f ${audio_conf}
present bench_config -f ${bench_config}
present motiond_config -f ${motiond_config}
present motiond_token -f ${motiond_token}
present motiond_binary -x ${motiond_binary}
active motiond_service ${motiond_service}

# What the motion daemon says about itself. A parked daemon does not exit — a
# fault is never auto-cleared — so the unit above is active for a machine that
# is commanding nothing at all. Reported as one packed value and judged on this
# side, like everything else.
if [ -f ${motiond_state} ]; then
	field() { sed -n "s/^\$1=//p" ${motiond_state} | head -n 1; }
	say motiond_state "\$(field state)|\$(field watch)|\$(field fault_stage)|\$(field fault_detail)"
else
	say motiond_state absent
fi

# The node the machine's own configuration names, when there is one to ask.
# Reported as its own key so this side can name the node it looked for rather
# than a node it assumed.
port=\$(awk '
	/^[[:space:]]*\[/ { bus = (\$0 ~ /^[[:space:]]*\[bus\]/); next }
	!bus { next }
	{
		line = \$0
		hash = index(line, "#")
		if (hash > 0) line = substr(line, 1, hash - 1)
		eq = index(line, "=")
		if (eq == 0) next
		key = substr(line, 1, eq - 1)
		val = substr(line, eq + 1)
		gsub(/[[:space:]]/, "", key)
		gsub(/^[[:space:]]*"?|"?[[:space:]]*\$/, "", val)
		if (key == "device") { print val; exit }
	}
' ${bench_config} 2>/dev/null)
[ -n "\$port" ] || port=${default_port}
say port_path "\$port"
present port -e "\$port"

if [ -f ${selftest_record} ]; then
	cases=\$(grep -c 'outcome = ' ${selftest_record} 2>/dev/null || true)
	passed=\$(grep -c 'outcome = "Pass"' ${selftest_record} 2>/dev/null || true)
	say selftest "\${passed:-0} of \${cases:-0} case(s) recorded as passing"
else
	say selftest "no record on the device"
fi
PROBE
}

answers=$(probe | ssh -o BatchMode=yes "root@${host}" bash -s) || die \
	"cannot reach root@${host}, so nothing about this robot is known." \
	"ssh's own error is above." \
	"    ssh root@${host} true"

# What each key means, in the order an operator meets these things: the store
# the payload lives in, the payload, the two configurations under it, the motion
# daemon's four files and its unit, and the bus.
label() {
	case "$1" in
		mount) echo "${store_mount} is mounted (brenn-os's payload store)" ;;
		payload) echo "the audio payload is unpacked (${payload_run})" ;;
		app_service) echo "${app_service} is running" ;;
		audio_conf) echo "the pod's link configuration is present (${audio_conf})" ;;
		bench_config) echo "the machine's configuration is present (${bench_config})" ;;
		motiond_config) echo "the motion daemon's configuration is present (${motiond_config})" ;;
		motiond_token) echo "the motion daemon's bus token is present (${motiond_token})" ;;
		motiond_binary) echo "the motion daemon is deployed (${motiond_binary})" ;;
		motiond_service) echo "${motiond_service} is running" ;;
		port) echo "the servo bus node is present (${port_path})" ;;
		*) echo "$1" ;;
	esac
}

port_path=$default_port
missing=0
unready=0
answered=0
selftest=
daemon_state=
service=missing

echo "${prog}: ${host}"
while IFS='=' read -r key value; do
	[ -n "$key" ] || continue
	case "$key" in
		port_path)
			port_path=$value
			continue
			;;
		selftest)
			selftest=$value
			continue
			;;
		motiond_state)
			# Counted as an answer, unlike the two informational keys: this
			# one is judged below, and can be the only reason a robot is
			# not ready.
			daemon_state=$value
			answered=$((answered + 1))
			continue
			;;
		motiond_service) service=$value ;;
	esac
	answered=$((answered + 1))
	case "$value" in
		ok) printf '  OK       %s\n' "$(label "$key")" ;;
		*)
			printf '  MISSING  %s\n' "$(label "$key")"
			missing=$((missing + 1))
			;;
	esac
done <<<"$answers"

# What the daemon says it is doing, which is the one answer here that is not a
# file being in place. It is judged apart from the rest because its fix is
# different: a parked daemon is a machine that faulted and is waiting for a
# person, and pushing files at it changes nothing.
#
# A parked or degraded daemon counts against readiness but not against the
# `make reachy-up` tally, so the closing advice stays true.
judge_daemon() {
	local packed=$1 state watch stage detail
	IFS='|' read -r state watch stage detail <<<"$packed"

	# An inactive unit is already a MISSING line of its own, and a state file
	# that outlived its service says nothing trustworthy: RuntimeDirectory
	# takes it away on stop, so anything left is a race with one.
	if [ "$service" != ok ]; then
		printf '  --       motion daemon state: %s (its unit is not running)\n' \
			"${state:-nothing recorded}"
		return
	fi

	# Sweeps that are not answering are the same news whatever phase the daemon
	# is in, and the phase where saying so matters most is the one that is not
	# resting: a boot over a dead servo bus holds `starting` until the cabling
	# is fixed, and "re-run this in a moment" is the one action that can never
	# resolve it. Parked is the exception — a fault is the larger news and names
	# an action of its own.
	if [ "$state" != parked ] && [ "$watch" = failing ]; then
		printf '  MISSING  the motion daemon cannot read the machine (%s), so no wake will raise the head. It is limp and safe, and recovers by itself when the servo bus answers again — check the bus cabling\n' \
			"$state"
		unready=$((unready + 1))
		return
	fi

	case "$state" in
		resting | active)
			printf '  OK       the motion daemon is running and ready (%s)\n' "$state"
			;;
		parked)
			printf '  MISSING  the motion daemon has FAULTED and is parked at %s: %s\n' \
				"${stage:-an unrecorded stage}" "${detail:-no detail recorded}"
			printf '           torque is off and it commands nothing until an operator acts: make reachy-motiond-logs, then restart the unit\n'
			unready=$((unready + 1))
			;;
		starting)
			printf '  MISSING  the motion daemon is still coming up (commissioning the machine) — re-run this in a moment\n'
			unready=$((unready + 1))
			;;
		stopping)
			printf '  MISSING  the motion daemon is shutting down — re-run this once it has\n'
			unready=$((unready + 1))
			;;
		absent | '')
			printf '  MISSING  the motion daemon has written no state, though its unit is running — it has only just started, or the deployed binary predates the state file. Re-run; redeploy if it persists\n'
			unready=$((unready + 1))
			;;
		*)
			printf '  MISSING  the motion daemon reported a state this command does not know: %s\n' \
				"$state"
			unready=$((unready + 1))
			;;
	esac
}

[ -z "$daemon_state" ] || judge_daemon "$daemon_state"

# Informational, always, and never counted: doctrine is that no record gates
# anything that moves.
[ -z "$selftest" ] || printf '  --       self-test: %s\n' "$selftest"

# A probe that answered nothing recognisable is not a healthy robot, and
# reporting it as "nothing missing" would be the worst answer available.
[ "$answered" -gt 0 ] || die \
	"root@${host} answered nothing this command understands." \
	"The probe's own output was: ${answers}"

if [ "$missing" -gt 0 ]; then
	echo "${prog}: ${missing} missing — everything above is pushed by: make reachy-up" >&2
fi
# Said separately because it is the one thing here that pushing files does not
# fix. A robot whose daemon has faulted is not ready, and saying "ready" for it
# is the answer this command cannot give.
if [ "$unready" -gt 0 ]; then
	echo "${prog}: the motion daemon is not ready — see the line above; make reachy-up does not fix that" >&2
fi
if [ "$missing" -gt 0 ] || [ "$unready" -gt 0 ]; then
	exit 1
fi
echo "${prog}: ready"
