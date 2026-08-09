#!/usr/bin/env bash
#
# Give one Reachy pod the configuration it needs to reach the audio host.
#
#   tools/provision-reachy-pod.sh <host> [speech-config]
#
# Everything is derived; the operator types nothing but the unit. The pod id is
# the device's own hostname, which is also its TLS-PSK identity. The address it
# dials and the key table it is filed in come from the speech daemon's config,
# so the two sides cannot be told different things — which holds only if the
# file named here is the file that daemon is actually started with. That is why
# the source is an argument and not a constant: SPEECH_CONFIG in
# firmware/Makefile names it, defaulting to the rung example a fresh checkout
# can run, and a workstation running a config from somewhere else sets it in
# .local/reachy.conf. A relative path is taken from the repository root.
#
# The key itself is generated once and reused on every later run: this command is
# idempotent, and a re-run after a reboot is the whole re-provisioning story. To
# rotate deliberately, delete the pod's line from the key table and re-run.
#
# The written file lands in the same tmpfs the payload store is in, so normal
# operation costs the device's flash nothing and a reboot clears configuration
# and payload together. The pod parks re-reading its file every five seconds, so
# the order of provisioning, deploying and rebooting never matters.
#
# The key never appears in an argument vector, a shell history, or either
# machine's process table: it is composed by shell builtins and delivered on
# ssh's standard input.

set -euo pipefail

# shellcheck source=lib.sh
. "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

# The speech daemon's configuration is the source of truth for both the address
# the pod dials and the key table it is filed in. Resolved below, once the
# arguments are read.
default_speech_config=host/config/parrot.toml

# Optional local tuning fragment, appended verbatim: CHANNEL, VAD_THRESHOLD,
# VAD_HANGOVER_MS. Gitignored — it is one workstation's opinion about one unit.
extra_conf="${firmware_root}/.local/audio.conf.extra"

# Where the pod reads its configuration. The same path is compiled into the pod
# (devices/reachy-pod/src/config.rs, CONF_DIR); a change in one without the other
# leaves the pod parked on a file nobody writes.
conf_dir="${store_mount}/conf"
conf_file="${conf_dir}/audio.conf"

host=${1:-}
[ -n "$host" ] || die "usage: ${prog} <host> [speech-config]"

# An absolute path stands; a relative one is from the repository root, so the
# Makefile variable reads the way a path in this repository is written.
host_config=${2:-$default_speech_config}
case "$host_config" in
	/*) ;;
	*) host_config="${repo_root}/${host_config}" ;;
esac

ssh_root() {
	ssh -o BatchMode=yes "root@${host}" "$@"
}

# One top-level key out of a TOML file, or empty when it is not set there.
#
# Top level only: everything this reads (listen_addr, pod_psk_file, a pod's key)
# lives above the first table header, and stopping there is what keeps a `url`
# under [stt] from answering for a `url` nobody asked about.
#
# Keys and values may be bare, double-quoted or single-quoted, and a `#` inside a
# quoted value is part of the value: this script quotes every id it writes,
# podctl writes whichever the id allows, and a hand-edited table may hold any of
# them. Each shape this misreads is silent and expensive — a path truncated at a
# `#` is a plausible wrong file, and a key read as absent is a second entry
# appended for a pod that already has one, which the daemon refuses the whole
# table over. A value whose quoting does not close is returned as it stands, so
# the caller's own check on it fails rather than a guess passing.
#
# Escapes inside a basic string are not decoded: nothing this reads holds one,
# and a value that needs one is a value this cannot answer for.
toml_top_level_value() {
	local file=$1 want=$2
	awk -v want="$want" '
		# The line up to a comment marker outside quotes, or the whole line.
		function strip_comment(line,   i, c, q, n) {
			q = ""
			n = length(line)
			for (i = 1; i <= n; i++) {
				c = substr(line, i, 1)
				if (q != "") { if (c == q) q = "" }
				else if (c == "\"" || c == "'\''") q = c
				else if (c == "#") return substr(line, 1, i - 1)
			}
			return line
		}
		# Where the key ends and the value begins: the first = outside quotes.
		function eq_index(line,   i, c, q, n) {
			q = ""
			n = length(line)
			for (i = 1; i <= n; i++) {
				c = substr(line, i, 1)
				if (q != "") { if (c == q) q = "" }
				else if (c == "\"" || c == "'\''") q = c
				else if (c == "=") return i
			}
			return 0
		}
		function trim(s) { gsub(/^[[:space:]]+|[[:space:]]+$/, "", s); return s }
		# The contents of a quoted token, or the token itself. Both ends must
		# agree: a half-quoted token is malformed, not a value with a stray mark.
		function unquote(s,   c, n) {
			n = length(s)
			c = substr(s, 1, 1)
			if (n >= 2 && (c == "\"" || c == "'\''") && substr(s, n, 1) == c)
				return substr(s, 2, n - 2)
			return s
		}
		/^[[:space:]]*\[/ { exit }
		{
			line = strip_comment($0)
			eq = eq_index(line)
			if (eq == 0) next
			key = unquote(trim(substr(line, 1, eq - 1)))
			val = unquote(trim(substr(line, eq + 1)))
			if (key == want) { print val; exit }
		}
	' "$file"
}

# ── What the host daemon says, checked before the device is touched ───────────

[ -f "$host_config" ] || die \
	"no speech daemon config at ${host_config}" \
	"That file is where the pod's address and the key table come from, and it has to be" \
	"the one speech-surface is started with. Name it in firmware/.local/reachy.conf:" \
	"    SPEECH_CONFIG=<path>" \
	"or write the default: docs/runbooks/reachy-end-to-end.md, step 1."

listen_addr=$(toml_top_level_value "$host_config" listen_addr)
psk_file=$(toml_top_level_value "$host_config" pod_psk_file)

[ -n "$listen_addr" ] || die "no listen_addr in ${host_config}" \
	"The pod dials that address; there is nothing to write without it."
[ -n "$psk_file" ] || die "no pod_psk_file in ${host_config}" \
	"That is the key table both sides are filed in."

# The pod is a different machine, so an address only this workstation can reach
# is a link that never comes up. The daemon refuses to bind 0.0.0.0 for the same
# reason; catch the rest here rather than after the file is on the device.
case "$listen_addr" in
	*:*[0-9]) ;;
	*) die "listen_addr ${listen_addr} in ${host_config} names no port" \
		"The pod dials an address and a port, e.g. 192.168.1.20:7380." ;;
esac
case "${listen_addr%:*}" in
	127.* | localhost | ::1 | "[::1]" | 0.0.0.0 | "" | "[::]")
		die "listen_addr ${listen_addr} in ${host_config} is not an address the pod can dial" \
			"Name the workstation's LAN address: ip -4 addr show scope global"
		;;
esac
case "$psk_file" in
	/*) ;;
	*) die "pod_psk_file ${psk_file} in ${host_config} is not an absolute path" \
		"The daemon resolves it relative to wherever it was started; name it literally." ;;
esac

# ── Who the pod is: its own hostname, which is its PSK identity ───────────────

# Read out of /proc rather than through a `hostname` binary: this is the value
# gethostname reports, which is the identity the pod actually presents, and it
# needs no tool to be installed in the image.
pod_id=$(ssh_root cat /proc/sys/kernel/hostname) || die \
	"cannot ask root@${host} for its hostname; nothing was provisioned." \
	"ssh's own error is above."

# First line, ends trimmed — and only the ends. Interior whitespace is left in
# place so the charset check below refuses it: the pod presents the name as the
# kernel holds it, so a name silently de-spaced here is filed under an identity
# no handshake ever offers.
pod_id=${pod_id%%$'\n'*}
pod_id=${pod_id#"${pod_id%%[![:space:]]*}"}
pod_id=${pod_id%"${pod_id##*[![:space:]]}"}
[ -n "$pod_id" ] || die "root@${host} reports no hostname" \
	"The hostname is the pod's TLS-PSK identity; a unit without one cannot connect."

# The id becomes a quoted TOML key in the key table and an identity in the
# handshake. Quoting handles the dot an FQDN-configured unit brings; a quote, a
# backslash, an equals sign or a space in the name would corrupt the line instead,
# and the keys in that table exist nowhere else. Refused here, naming the value in
# quotes so whitespace is visible, rather than written and discovered at the
# daemon's next start.
case "$pod_id" in
	*[!A-Za-z0-9._-]*)
		die "root@${host} reports the hostname \"${pod_id}\", which cannot be a key-table entry" \
			"A pod id may hold letters, digits, dot, hyphen and underscore." \
			"Give the unit a plainer hostname and re-run."
		;;
esac

# ── The key: reused if this pod is already filed, generated once if not ───────

if [ -e "$psk_file" ]; then
	[ -f "$psk_file" ] || die "${psk_file} is not a regular file"
	# The daemon's own rule, not a stricter one: it refuses a table with any
	# group or other bit set, matching ssh's posture on private keys, and says
	# nothing about the owner's. A read-only 0400 table loads there and must
	# load here — telling its operator to loosen a secrets file would be advice
	# in the wrong direction.
	mode=$(stat -c '%a' -- "$psk_file")
	if ((0"$mode" & 077)); then
		die "${psk_file} is mode ${mode}, and the daemon rejects a key table any other account can read." \
			"    chmod go-rwx ${psk_file}"
	fi
	psk=$(toml_top_level_value "$psk_file" "$pod_id")
else
	psk=
fi

if [ -n "$psk" ]; then
	# The same check a freshly generated key gets, and for the same reason: a
	# hand-truncated or oddly-quoted entry pushed as it reads leaves the daemon
	# working and the pod parked on a config error every five seconds, with
	# nothing on this workstation pointing at the line that caused it.
	[[ $psk =~ ^[0-9a-f]{64}$ ]] || die \
		"the entry for ${pod_id} in ${psk_file} is not a 64-character hex key" \
		"Every entry is one line: \"<pod id>\" = \"<64 hex characters>\"." \
		"Fix that line — or delete it, which files a fresh key — and re-run."
	echo "${prog}: ${pod_id} is already in ${psk_file}; reusing its key" >&2
else
	# The reader above is not a TOML parser. A table that mentions this pod while
	# yielding no key for it holds the entry in a shape the reader does not
	# understand, and appending a second one would define the key twice — which
	# the daemon refuses the whole table over, taking every other pod in it off
	# the air. "It is in there but I could not read it" fails loudly instead.
	if [ -f "$psk_file" ] && grep -qE "^[[:space:]]*[\"']?${pod_id//./\\.}[\"']?[[:space:]]*=" -- "$psk_file"; then
		die "${psk_file} names ${pod_id} but no key for it could be read" \
			"Every entry is one line: \"<pod id>\" = \"<64 hex characters>\"." \
			"Fix that line — or delete it, which files a fresh key — and re-run."
	fi

	command -v openssl >/dev/null 2>&1 || die \
		"openssl is not on PATH, and it is what generates the key"
	psk=$(openssl rand -hex 32)
	# A short or non-hex key fails the pod's own parser five seconds at a time
	# with nothing local to point at, so it is refused here instead.
	[[ $psk =~ ^[0-9a-f]{64}$ ]] || die "openssl produced no 64-character hex key"

	# Written through a temp file and renamed: the keys in this table exist
	# nowhere else, and a torn write takes the whole fleet off the air at the
	# daemon's next start.
	tmp="${psk_file}.tmp.$$"
	mkdir -p -- "$(dirname -- "$psk_file")"
	(
		umask 077
		: >"$tmp"
		if [ -f "$psk_file" ]; then
			cat -- "$psk_file" >>"$tmp"
			# A table whose last line has no newline would otherwise swallow the
			# entry appended after it. Command substitution eats a trailing
			# newline, so a non-empty result means the file ends mid-line.
			if [ -n "$(tail -c1 -- "$psk_file")" ]; then
				printf '\n' >>"$tmp"
			fi
		fi
		# The key is quoted, always. A pod id is a hostname, and an
		# FQDN-configured unit's dotted name written bare is a nested table
		# to TOML — which the daemon reads as a non-string entry and refuses
		# the whole file over, taking every other pod in the table off the
		# air with it.
		printf '"%s" = "%s"\n' "$pod_id" "$psk" >>"$tmp"
	)
	chmod 600 -- "$tmp"
	mv -f -- "$tmp" "$psk_file"
	echo "${prog}: filed a new key for ${pod_id} in ${psk_file}" >&2
fi

# ── The push: composed by builtins, delivered on ssh's stdin ──────────────────

# The store is a mount brenn-os provides. Creating the directory under an absent
# mount would put the file in /run, where the mount then hides it — and the pod
# would park on a path that exists, empty, forever.
ssh_root findmnt -rno TARGET -- "$store_mount" >/dev/null || die \
	"${store_mount} is not a mount point on ${host}, so there is nowhere to put the configuration." \
	"That mount is brenn-os's; check the image this unit is running." \
	"(The check itself: ssh root@${host} findmnt ${store_mount})"

remote="set -e; umask 022; mkdir -p -- ${conf_dir}; umask 077;"
remote="${remote} cat >${conf_file}.new;"
remote="${remote} chown ${app_user} ${conf_file}.new; chmod 600 ${conf_file}.new;"
remote="${remote} mv -f ${conf_file}.new ${conf_file}"

{
	printf 'ADDR=%s\nPSK=%s\n' "$listen_addr" "$psk"
	if [ -f "$extra_conf" ]; then
		cat -- "$extra_conf"
	fi
} | ssh_root "$remote" || die \
	"the configuration did not reach root@${host}:${conf_file}" \
	"ssh's own error is above; nothing partial is left behind (the file is renamed into place)."

echo "${prog}: ${pod_id} provisioned — ${conf_file} points at ${listen_addr}"
if [ -f "$extra_conf" ]; then
	echo "${prog}: appended $(basename -- "$extra_conf") verbatim"
fi
