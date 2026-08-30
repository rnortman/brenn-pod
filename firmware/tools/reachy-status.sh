#!/usr/bin/env bash
#
# Say whether a unit's pod is ready, without starting anything.
#
#   tools/reachy-status.sh <host>
#
# Every refusal this stack makes is well-built and none of them is reachable
# before you run the thing: the pod's patient wait for a configuration that is
# not there, the store that a reboot emptied. They surface one at a time, each
# after the previous one is fixed. This command asks all of those questions at
# once, reads only, and starts nothing.
#
# The audio half only. A robot's motion half — its payload, the machine's own
# configuration, the servo bus — is deployed and answered for by brenn-reachy;
# nothing here reads it, so a `ready` from this command says the pod is up and
# says nothing about whether the head can move.
#
# What it checks is what a reboot clears: the payload store is tmpfs and the
# account's home is tmpfs. So every MISSING line here has the same fix — `make
# reachy-up` pushes all of it — and the line says so rather than making anyone
# map a missing file to the target that writes it.
#
# One ssh invocation, because a check per connection is a check per second of
# latency and because the answers should describe one moment. The device side
# emits `key=value` lines and every judgement about them is made here, where the
# fixing commands and this repository's names for things are.
#
# The self-test record is reported and never judged: nothing in either
# workspace gates on it any more. It is a diagnostic, and its absence is not a
# reason this pod is not ready.

set -euo pipefail

# shellcheck source=lib.sh
. "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

host=${1:-}
[ -n "$host" ] || die "usage: ${prog} <host>"

# The device-side names. The paths are lib.sh's; the unit and the record are
# named by the two things that install them.
app_service=brenn-app.service
payload_run="${store_mount}/current/run"
audio_conf="${store_mount}/conf/audio.conf"
selftest_record="${app_home}/selftest-state.toml"

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
# the payload lives in, the payload, its unit, and the configuration it reads.
label() {
	case "$1" in
		mount) echo "${store_mount} is mounted (brenn-os's payload store)" ;;
		payload) echo "the audio payload is unpacked (${payload_run})" ;;
		app_service) echo "${app_service} is running" ;;
		audio_conf) echo "the pod's link configuration is present (${audio_conf})" ;;
		*) echo "$1" ;;
	esac
}

missing=0
answered=0
selftest=

echo "${prog}: ${host}"
while IFS='=' read -r key value; do
	[ -n "$key" ] || continue
	case "$key" in
		selftest)
			selftest=$value
			continue
			;;
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
	exit 1
fi
echo "${prog}: ready"
