#!/usr/bin/env bash
#
# provision-reachy-pod.test.sh — host-only regression tests for
# provision-reachy-pod.sh.
#
# Everything the provisioning tool does before it touches a device is text
# processing: a hand-rolled TOML reader, the loud validation gates, and the
# reuse-or-generate branch that decides the fate of the only copy of every pod's
# key. A wrong answer there is expensive and quiet — a key filed under an identity
# no handshake offers, a second entry for a pod that already has one (which the
# daemon refuses the whole table over, taking the fleet off the air), a path
# truncated into a plausible wrong file — and the bench run that would notice is
# not something anyone repeats per commit. So the device is stubbed and the logic
# is asserted here, the way check-realign-args.test.sh asserts its gate's awk
# parser.
#
# Two layers:
#   * the TOML reader on its own, extracted from the script and fed by hand;
#   * the whole script, run out of a scratch tree. It finds its host config and
#     its shared prelude relative to itself, so a copy inside a fixture tree
#     reads fixture files, with `ssh` and `openssl` stubbed on PATH.

set -euo pipefail

HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
TOOL="${TOOL:-$HERE/provision-reachy-pod.sh}" # overridable to run the suite against a modified tool

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# shellcheck source=test-lib.sh
. "$HERE/test-lib.sh"

# ── Layer 1: the TOML reader, on its own ──────────────────────────────────────

# The function under test, lifted out of the tool so the reader can be asserted
# without a fixture tree. A shape it misreads is the most expensive failure in
# the script, and it is pure text in, text out.
eval "$(sed -n '/^toml_top_level_value() {$/,/^}$/p' "$TOOL")"
declare -F toml_top_level_value >/dev/null || {
	echo "provision-reachy-pod.test.sh: FAIL — no toml_top_level_value found in $TOOL" >&2
	exit 1
}

# reader_case <name> <want> <key> <file body>
reader_case() {
	casenum=$((casenum + 1))
	local name=$1 want=$2 key=$3 body=$4
	local file="$WORK/reader-$casenum.toml" got
	printf '%s' "$body" >"$file"
	got=$(toml_top_level_value "$file" "$key")
	if [ "$got" != "$want" ]; then
		fail "$name" "read '${got}', wanted '${want}'"
		return
	fi
	pass "$name"
}

reader_case "reader-bare-key" "aa" reachy00 'reachy00 = "aa"
'
reader_case "reader-double-quoted-key" "bb" reachy00 '"reachy00" = "bb"
'
# Regression: a single-quoted (literal) key is legal TOML the daemon accepts. Read
# as absent, it becomes a second entry appended for a pod that already has one.
reader_case "reader-single-quoted-key" "cc" reachy00 "'reachy00' = \"cc\"
"
reader_case "reader-single-quoted-value" "dd" reachy00 "reachy00 = 'dd'
"
reader_case "reader-dotted-quoted-key" "ee" reachy01.lan '"reachy01.lan" = "ee"
'
# Regression: a `#` inside a quoted value is part of the value. Truncated, it is a
# shorter path that still passes an absolute-path check and names the wrong file.
reader_case "reader-hash-inside-value" "/tmp/x#1.toml" pod_psk_file 'pod_psk_file = "/tmp/x#1.toml"
'
reader_case "reader-trailing-comment-stripped" "192.168.1.20:7380" listen_addr 'listen_addr = "192.168.1.20:7380"  # the LAN address
'
reader_case "reader-whole-line-comment-ignored" "" reachy00 '# reachy00 = "aa"
'
reader_case "reader-below-table-header-ignored" "" reachy00 '[stt]
reachy00 = "aa"
'
reader_case "reader-absent-key-empty" "" reachy00 'reachy01 = "aa"
'
reader_case "reader-line-without-equals-skipped" "aa" reachy00 'not a setting
reachy00 = "aa"
'
# A half-quoted value is malformed, not a value with a stray mark: handing it back
# as it stands is what makes the caller's own check on it fail.
reader_case "reader-unterminated-quote-verbatim" '"/tmp/x' pod_psk_file 'pod_psk_file = "/tmp/x
'
reader_case "reader-first-equals-outside-quotes-splits" "a=b" reachy00 'reachy00 = "a=b"
'

# ── Layer 2: the whole script against a stubbed device ────────────────────────

STUBS="$WORK/stubs"
mkdir -p "$STUBS"

# Stubbed ssh: answers the two read-only queries the tool makes and captures the
# push. Which call is which is decided by the command it carries.
cat >"$STUBS/ssh" <<'SSH_EOF'
#!/usr/bin/env bash
for arg in "$@"; do
	case "$arg" in
	*/proc/sys/kernel/hostname*)
		printf '%s' "${STUB_HOSTNAME}"
		exit 0
		;;
	findmnt)
		[ -z "${STUB_NO_MOUNT:-}" ] || exit 1
		printf '/run/brenn-app\n'
		exit 0
		;;
	esac
done
printf '%s\n' "$*" >"$STUB_REMOTE_CMD"
cat >"$STUB_PUSH"
[ -z "${STUB_PUSH_FAIL:-}" ] || exit 255
SSH_EOF

# Stubbed openssl: a fixed key, so what got filed and pushed is assertable.
cat >"$STUBS/openssl" <<'SSL_EOF'
#!/usr/bin/env bash
[ "${1:-}" = rand ] || exit 1
printf '%s\n' "${STUB_KEY}"
SSL_EOF
chmod +x "$STUBS/ssh" "$STUBS/openssl"

# The fixture keys are built rather than written out: 64 hex characters spelled
# literally beside the word "key" is what a secret scanner is for.
hex64() {
	local out="" i
	for ((i = 0; i < 64; i++)); do out="${out}${1}"; done
	printf '%s' "$out"
}
KEY_NEW=$(hex64 a)
KEY_OLD=$(hex64 b)
KEY_UNUSED=$(hex64 c)

treenum=0
TREE=""
PSK_FILE=""
PUSHED=""
REMOTE_CMD=""

# host_config <line>... — the daemon config the tool reads its two facts from.
host_config() {
	local line
	: >"$TREE/host/config/parrot.toml"
	for line in "$@"; do printf '%s\n' "$line" >>"$TREE/host/config/parrot.toml"; done
	printf '\n[stt]\nurl = "http://127.0.0.1:8000"\n' >>"$TREE/host/config/parrot.toml"
}

# key_table <mode> <body> — a pre-existing key table at the configured path.
key_table() {
	printf '%s' "$2" >"$PSK_FILE"
	chmod "$1" -- "$PSK_FILE"
}

# A fresh fixture tree: the tool and its prelude copied where their own relative
# lookups land on fixture files, and a host config naming a fixture key table.
new_tree() {
	treenum=$((treenum + 1))
	TREE="$WORK/tree-$treenum"
	mkdir -p "$TREE/firmware/tools" "$TREE/host/config" "$TREE/keys"
	cp -- "$TOOL" "$HERE/lib.sh" "$TREE/firmware/tools/"
	chmod +x "$TREE/firmware/tools/$(basename -- "$TOOL")"
	PSK_FILE="$TREE/keys/psk.toml"
	PUSHED="$TREE/pushed.conf"
	REMOTE_CMD="$TREE/remote.cmd"
	host_config "listen_addr = \"192.168.1.20:7380\"" "pod_psk_file = \"${PSK_FILE}\""
	export STUB_HOSTNAME=reachy00 STUB_KEY="$KEY_NEW"
	export STUB_PUSH="$PUSHED" STUB_REMOTE_CMD="$REMOTE_CMD"
	unset STUB_NO_MOUNT STUB_PUSH_FAIL
}

# The subject with the whole argument vector spelled by the caller: what a case
# about argument handling needs is the order the words arrive in.
run_argv() {
	set +e
	OUT=$(PATH="$STUBS:$PATH" "$TREE/firmware/tools/$(basename -- "$TOOL")" "$@" 2>&1)
	EC=$?
	set -e
}

# The ordinary shape every other case wants: the unit first, the rest after it.
run_tool() {
	run_argv reachy-dev "$@"
}

new_tree
rm -f -- "$TREE/host/config/parrot.toml"
run_tool
expect_die "gate-no-speech-config" "no speech daemon config at"
check "gate-no-speech-config-device-untouched" "$(yes_no [ ! -e "$PUSHED" ])" "something was pushed"
# The file this reads has to be the one speech-surface is started with, and on
# this maintainer's workstation that is not the rung example. The refusal names
# the variable rather than leaving the reader to find the default in a Makefile.
check "gate-no-speech-config-names-the-variable" \
	"$(yes_no grep -q SPEECH_CONFIG <<<"$OUT")" "output was: $OUT"

# ── the speech config is an argument, not a constant ──────────────────────────

# An absolute path stands as written: what the live config's path looks like is
# the operator's business, and it is usually in another checkout entirely.
new_tree
mkdir -p "$TREE/elsewhere"
{
	printf 'listen_addr = "192.168.1.99:7380"\n'
	printf 'pod_psk_file = "%s"\n' "$PSK_FILE"
} >"$TREE/elsewhere/live.toml"
run_tool "$TREE/elsewhere/live.toml"
expect_ok "speech-config-absolute-path-is-read"
check "speech-config-absolute-path-is-what-was-pushed" \
	"$(yes_no grep -q 'ADDR=192.168.1.99:7380' -- "$PUSHED")" \
	"pushed: $(cat -- "$PUSHED")"

# A relative one is from the repository root, so the Makefile variable reads the
# way a path in this repository is written.
new_tree
mkdir -p "$TREE/elsewhere"
{
	printf 'listen_addr = "192.168.1.98:7380"\n'
	printf 'pod_psk_file = "%s"\n' "$PSK_FILE"
} >"$TREE/elsewhere/live.toml"
run_tool elsewhere/live.toml
expect_ok "speech-config-relative-path-is-from-the-repo-root"
check "speech-config-relative-path-is-what-was-pushed" \
	"$(yes_no grep -q 'ADDR=192.168.1.98:7380' -- "$PUSHED")" \
	"pushed: $(cat -- "$PUSHED")"

# With no argument, the default the Makefile ships — the rung example a fresh
# checkout can run.
new_tree
run_tool
expect_ok "speech-config-default-is-the-rung-example"
check "speech-config-default-is-what-was-pushed" \
	"$(yes_no grep -q 'ADDR=192.168.1.20:7380' -- "$PUSHED")" \
	"pushed: $(cat -- "$PUSHED")"

new_tree
host_config "pod_psk_file = \"${PSK_FILE}\""
run_tool
expect_die "gate-no-listen-addr" "no listen_addr in"

new_tree
host_config "listen_addr = \"192.168.1.20:7380\""
run_tool
expect_die "gate-no-psk-file" "no pod_psk_file in"

new_tree
host_config "listen_addr = \"192.168.1.20\"" "pod_psk_file = \"${PSK_FILE}\""
run_tool
expect_die "gate-portless-listen-addr" "names no port"

for bad in "127.0.0.1:7380" "localhost:7380" "[::1]:7380" "0.0.0.0:7380" "[::]:7380"; do
	new_tree
	host_config "listen_addr = \"${bad}\"" "pod_psk_file = \"${PSK_FILE}\""
	run_tool
	expect_die "gate-unreachable-listen-addr-${bad}" "is not an address the pod can dial"
done

# ── the two arrangements: a workstation daemon, and one on the unit ───────────

# Under the flag, a relative pod_psk_file is the key table beside the config that
# names it. The assembly-directory arrangement depends on this: that directory is
# copied into the payload the daemon runs from, so the file beside the config is
# the file the daemon opens, and a provision run that resolved it somewhere else
# would file the key in a table nothing reads and report success.
new_tree
host_config "listen_addr = \"192.168.1.20:7380\"" "pod_psk_file = \"pod-psk.toml\""
run_tool --on-unit
expect_ok "relative-psk-file-resolved-beside-the-config" "filed a new key for reachy00"
check "relative-psk-file-table-is-beside-the-config" \
	"$(yes_no grep -qxF "\"reachy00\" = \"${KEY_NEW}\"" -- "$TREE/host/config/pod-psk.toml")" \
	"table holds: $(cat -- "$TREE/host/config/pod-psk.toml" 2>&1)"
check "relative-psk-file-nothing-at-the-repo-root" \
	"$(yes_no [ ! -e "$TREE/pod-psk.toml" ])" \
	"a table was written at the repository root"

# The same spelling, with the config named from somewhere else entirely: the
# directory that decides is the config's, not the caller's and not the repo's.
new_tree
mkdir -p "$TREE/assembly"
{
	printf 'listen_addr = "192.168.1.97:7380"\n'
	printf 'pod_psk_file = "pod-psk.toml"\n'
} >"$TREE/assembly/speech.toml"
run_tool "$TREE/assembly/speech.toml" --on-unit
expect_ok "relative-psk-file-follows-an-absolute-config" "filed a new key"
check "relative-psk-file-filed-in-the-assembly-directory" \
	"$(yes_no [ -f "$TREE/assembly/pod-psk.toml" ])" \
	"assembly directory holds: $(ls -A -- "$TREE/assembly")"

# Both relaxations at once, which is the only arrangement the flag exists for:
# an assembly directory whose config dials loopback because the daemon is on the
# unit, and whose key table sits beside it because that directory is copied into
# the payload the daemon runs from. The isolated cases above each leave the other
# gate untripped, so a gate that fires only when both conditions hold — or a
# variable one branch clobbers for the next — would pass them and be found at the
# bench with a robot in front of somebody.
new_tree
mkdir -p "$TREE/assembly"
{
	printf 'listen_addr = "127.0.0.1:7380"\n'
	printf 'pod_psk_file = "pod-psk.toml"\n'
} >"$TREE/assembly/speech.toml"
run_tool "$TREE/assembly/speech.toml" --on-unit
expect_ok "on-robot-arrangement-provisions" "filed a new key"
check "on-robot-arrangement-files-the-table-beside-the-config" \
	"$(yes_no grep -qxF "\"reachy00\" = \"${KEY_NEW}\"" -- "$TREE/assembly/pod-psk.toml")" \
	"table holds: $(cat -- "$TREE/assembly/pod-psk.toml" 2>&1)"
check "on-robot-arrangement-pushes-the-loopback-address" \
	"$(yes_no grep -qxF 'ADDR=127.0.0.1:7380' -- "$PUSHED")" \
	"pushed: $(cat -- "$PUSHED")"
check "on-robot-arrangement-pushes-the-key-just-filed" \
	"$(yes_no grep -qF "$KEY_NEW" -- "$PUSHED")" \
	"pushed: $(cat -- "$PUSHED")"

# Without the flag the daemon's working directory is somebody else's business —
# the standalone runbook starts it from host/ with --config config/parrot.toml,
# one level away from the directory this would have resolved against — so the
# spelling is refused rather than filed where nothing reads it.
#
# The config is named on the command line rather than left to the default, so
# the re-run line the refusal hands back is checked against a path only the
# config that was read could have produced: this branch prints the same
# `SPEECH_CONFIG=` line the loopback one does, and a regression to the hardcoded
# default is invisible against a default-path fixture.
new_tree
mkdir -p "$TREE/assembly"
{
	printf 'listen_addr = "192.168.1.20:7380"\n'
	printf 'pod_psk_file = "pod-psk.toml"\n'
} >"$TREE/assembly/speech.toml"
run_tool "$TREE/assembly/speech.toml"
expect_die "gate-relative-psk-file-without-opt-in" "is not an absolute path"
check "gate-relative-psk-file-names-the-opt-in" \
	"$(yes_no grep -q 'ON_UNIT=1' <<<"$OUT")" "output was: $OUT"
check "gate-relative-psk-file-re-run-names-the-config-it-read" \
	"$(yes_no grep -qF "SPEECH_CONFIG=\"${TREE}/assembly/speech.toml\"" <<<"$OUT")" \
	"output was: $OUT"
check "gate-relative-psk-file-wrote-no-table" \
	"$(yes_no [ ! -e "$TREE/assembly/pod-psk.toml" ])" \
	"a table was written beside the config"
check "gate-relative-psk-file-device-untouched" \
	"$(yes_no [ ! -e "$PUSHED" ])" "something was pushed"

# An absolute pod_psk_file still stands as written — every other fixture in this
# suite spells one, and this is the case that says so on purpose.
new_tree
run_tool
expect_ok "absolute-psk-file-stands" "filed a new key"
check "absolute-psk-file-is-where-it-was-named" \
	"$(yes_no [ -f "$PSK_FILE" ])" "no table at ${PSK_FILE}"

# Loopback is refused for a workstation daemon — the pod is a different machine
# — and the refusal names the way to say it is not one.
for local_addr in "127.0.0.1:7380" "localhost:7380" "[::1]:7380"; do
	new_tree
	host_config "listen_addr = \"${local_addr}\"" "pod_psk_file = \"${PSK_FILE}\""
	run_tool
	expect_die "gate-loopback-without-opt-in-${local_addr}" "is not an address the pod can dial"
	check "gate-loopback-names-the-opt-in-${local_addr}" \
		"$(yes_no grep -q 'ON_UNIT=1' <<<"$OUT")" "output was: $OUT"

	new_tree
	host_config "listen_addr = \"${local_addr}\"" "pod_psk_file = \"${PSK_FILE}\""
	run_tool --on-unit
	expect_ok "on-unit-accepts-loopback-${local_addr}" "provisioned"
	check "on-unit-pushes-loopback-${local_addr}" \
		"$(yes_no grep -qxF "ADDR=${local_addr}" -- "$PUSHED")" \
		"pushed: $(cat -- "$PUSHED")"
done

# The command a refusal hands back has to name the config that was read, not the
# default: an operator re-running the printed line against a different file
# provisions from something they never looked at.
new_tree
mkdir -p "$TREE/assembly"
{
	printf 'listen_addr = "127.0.0.1:7380"\n'
	printf 'pod_psk_file = "%s"\n' "$PSK_FILE"
} >"$TREE/assembly/speech.toml"
run_tool "$TREE/assembly/speech.toml"
expect_die "gate-loopback-with-a-named-config" "is not an address the pod can dial"
check "gate-loopback-re-run-names-the-config-it-read" \
	"$(yes_no grep -qF "SPEECH_CONFIG=\"${TREE}/assembly/speech.toml\"" <<<"$OUT")" \
	"output was: $OUT"

# The flag lifts one refusal and not the others: an address naming no host is
# nothing to dial from anywhere, loopback included.
for wild in "0.0.0.0:7380" "[::]:7380"; do
	new_tree
	host_config "listen_addr = \"${wild}\"" "pod_psk_file = \"${PSK_FILE}\""
	run_tool --on-unit
	expect_die "gate-wildcard-refused-even-on-unit-${wild}" "is not an address the pod can dial"
	check "gate-wildcard-nothing-pushed-${wild}" "$(yes_no [ ! -e "$PUSHED" ])" "something was pushed"
done

# The flag may lead, because that is how an operator types it. What it must not
# do is be read as the speech config.
new_tree
host_config "listen_addr = \"127.0.0.1:7380\"" "pod_psk_file = \"${PSK_FILE}\""
run_argv --on-unit reachy-dev
expect_ok "on-unit-may-lead" "provisioned"

# A misspelt flag taken as a hostname would provision nothing and say it had.
new_tree
run_tool --on-unti
expect_die "unknown-option-refused" "unknown option --on-unti"
check "unknown-option-device-untouched" "$(yes_no [ ! -e "$PUSHED" ])" "something was pushed"

new_tree
run_tool "$TREE/host/config/parrot.toml" extra
expect_die "too-many-arguments-refused" "too many arguments"

new_tree
key_table 640 ''
run_tool
expect_die "gate-group-readable-table" "any other account can read"

new_tree
key_table 400 "\"reachy00\" = \"${KEY_OLD}\"
"
run_tool
expect_ok "gate-read-only-table-accepted" "reusing its key"

new_tree
STUB_HOSTNAME="reachy/00"
run_tool
expect_die "gate-hostname-charset" "cannot be a key-table entry"

# Regression: interior whitespace used to be stripped before the charset check,
# filing the key under a name no handshake ever presents.
new_tree
STUB_HOSTNAME="bad name"
run_tool
expect_die "gate-hostname-interior-space" 'reports the hostname "bad name"'
check "gate-hostname-interior-space-nothing-filed" "$(yes_no [ ! -e "$PSK_FILE" ])" "a key was filed"

new_tree
STUB_HOSTNAME=""
run_tool
expect_die "gate-hostname-empty" "reports no hostname"

new_tree
export STUB_NO_MOUNT=1
run_tool
expect_die "gate-store-not-mounted" "is not a mount point"

new_tree
export STUB_PUSH_FAIL=1
run_tool
expect_die "gate-push-failure" "did not reach root@"

new_tree
STUB_HOSTNAME=$'  reachy00 \r\nsomething else\n'
run_tool
expect_ok "hostname-first-line-ends-trimmed" "reachy00 provisioned"
check "hostname-filed-trimmed" \
	"$(yes_no grep -qxF "\"reachy00\" = \"${KEY_NEW}\"" -- "$PSK_FILE")" \
	"table holds: $(cat -A -- "$PSK_FILE")"

new_tree
run_tool
expect_ok "fresh-filed-a-new-key" "filed a new key for reachy00"
check "fresh-entry-is-quoted-and-alone" \
	"$(yes_no [ "$(cat -- "$PSK_FILE")" = "\"reachy00\" = \"${KEY_NEW}\"" ])" \
	"table holds: $(cat -- "$PSK_FILE")"
check "fresh-table-is-owner-only" \
	"$(yes_no [ "$(stat -c '%a' -- "$PSK_FILE")" = 600 ])" \
	"mode $(stat -c '%a' -- "$PSK_FILE")"
check "fresh-push-carries-addr-and-key" \
	"$(yes_no [ "$(cat -- "$PUSHED")" = "ADDR=192.168.1.20:7380
PSK=${KEY_NEW}" ])" \
	"pushed: $(cat -- "$PUSHED")"
check "fresh-push-renames-into-place" \
	"$(yes_no grep -q 'mv -f /run/brenn-app/conf/audio.conf.new /run/brenn-app/conf/audio.conf' -- "$REMOTE_CMD")" \
	"remote command: $(cat -- "$REMOTE_CMD")"

# The composed file speaks only keys the pod reads (devices/reachy-pod/src/config.rs):
# an unknown key is a load error there, so a stray line is a pod that never starts.
new_tree
mkdir -p "$TREE/firmware/.local"
printf 'CHANNEL=1\nVAD_THRESHOLD=0.6\n' >"$TREE/firmware/.local/audio.conf.extra"
run_tool
expect_ok "extra-appended" "appended audio.conf.extra verbatim"
unknown=$(cut -d= -f1 <"$PUSHED" | grep -vxE 'ADDR|PSK|CHANNEL|VAD_THRESHOLD|VAD_HANGOVER_MS' || true)
check "extra-keys-are-all-read-by-the-pod" "$(yes_no [ -z "$unknown" ])" "unknown keys: ${unknown}"
check "extra-appended-verbatim" \
	"$(yes_no grep -qxF 'VAD_THRESHOLD=0.6' -- "$PUSHED")" \
	"pushed: $(cat -- "$PUSHED")"

new_tree
run_tool
expect_ok "idempotent-first-run" "filed a new key"
cp -- "$PUSHED" "$TREE/pushed.first"
cp -- "$PSK_FILE" "$TREE/table.first"
STUB_KEY="$KEY_UNUSED"
run_tool
expect_ok "idempotent-second-run-reuses" "reusing its key"
check "idempotent-table-unchanged" \
	"$(yes_no cmp -s -- "$TREE/table.first" "$PSK_FILE")" \
	"table changed: $(cat -- "$PSK_FILE")"
check "idempotent-push-byte-identical" \
	"$(yes_no cmp -s -- "$TREE/pushed.first" "$PUSHED")" \
	"pushed: $(cat -- "$PUSHED")"

# A second pod is filed beside the first, not over it.
new_tree
key_table 600 "\"reachy00\" = \"${KEY_OLD}\"
"
STUB_HOSTNAME=reachy01.lan
run_tool
expect_ok "second-pod-filed-beside-the-first" "filed a new key for reachy01.lan"
check "second-pod-keeps-the-first-entry" \
	"$(yes_no grep -qxF "\"reachy00\" = \"${KEY_OLD}\"" -- "$PSK_FILE")" \
	"table holds: $(cat -- "$PSK_FILE")"
check "second-pod-entry-quoted" \
	"$(yes_no grep -qxF "\"reachy01.lan\" = \"${KEY_NEW}\"" -- "$PSK_FILE")" \
	"table holds: $(cat -- "$PSK_FILE")"

# A table whose last line has no newline: the appended entry must still land on
# its own line, or two good entries become one unparseable one.
new_tree
key_table 600 "\"reachy00\" = \"${KEY_OLD}\""
STUB_HOSTNAME=reachy01
run_tool
expect_ok "no-trailing-newline-append" "filed a new key for reachy01"
check "no-trailing-newline-entry-on-its-own-line" \
	"$(yes_no grep -qxF "\"reachy01\" = \"${KEY_NEW}\"" -- "$PSK_FILE")" \
	"table holds: $(cat -- "$PSK_FILE")"

new_tree
key_table 600 "reachy00 = \"${KEY_OLD}\"
"
run_tool
expect_ok "bare-entry-reused" "reusing its key"
check "bare-entry-not-duplicated" \
	"$(yes_no [ "$(grep -c reachy00 -- "$PSK_FILE")" = 1 ])" \
	"table holds: $(cat -- "$PSK_FILE")"
check "bare-entry-key-pushed" \
	"$(yes_no grep -qxF "PSK=${KEY_OLD}" -- "$PUSHED")" \
	"pushed: $(cat -- "$PUSHED")"

# Regression: a single-quoted key read as absent appended a second entry, which
# `PskTable::parse` refuses — every pod in the table off the air, not just this one.
new_tree
key_table 600 "'reachy00' = \"${KEY_OLD}\"
"
run_tool
expect_ok "single-quoted-entry-reused" "reusing its key"
check "single-quoted-entry-not-duplicated" \
	"$(yes_no [ "$(grep -c reachy00 -- "$PSK_FILE")" = 1 ])" \
	"table holds: $(cat -- "$PSK_FILE")"

# Regression: a reused entry was pushed unchecked. The host side keeps working, so
# only the pod notices — every five seconds, with nothing local to point at.
new_tree
key_table 600 '"reachy00" = "bbbb"
'
run_tool
expect_die "short-entry-refused" "is not a 64-character hex key"
check "short-entry-nothing-pushed" "$(yes_no [ ! -e "$PUSHED" ])" "something was pushed"

new_tree
key_table 600 "\"reachy00\" = \"${KEY_OLD^^}\"
"
run_tool
expect_die "uppercase-entry-refused" "is not a 64-character hex key"

# An entry the reader cannot see but the table plainly holds (here: misfiled under
# a table header). Appending would define the key twice; refusing says so instead.
new_tree
key_table 600 "[keys]
\"reachy00\" = \"${KEY_OLD}\"
"
run_tool
expect_die "unreadable-entry-refused-not-appended" "no key for it could be read"
check "unreadable-entry-table-untouched" \
	"$(yes_no [ "$(grep -c reachy00 -- "$PSK_FILE")" = 1 ])" \
	"table holds: $(cat -- "$PSK_FILE")"

# Regression: a `#` inside the quoted pod_psk_file value truncated the path, so the
# tool filed a key into a brand-new table nothing reads and reported success.
new_tree
PSK_FILE="$TREE/keys/psk#1.toml"
host_config "listen_addr = \"192.168.1.20:7380\"" "pod_psk_file = \"${PSK_FILE}\""
key_table 600 "\"reachy00\" = \"${KEY_OLD}\"
"
run_tool
expect_ok "hash-in-table-path-reads-the-real-table" "reusing its key"
check "hash-in-table-path-no-truncated-sibling" \
	"$(yes_no [ ! -e "$TREE/keys/psk" ])" \
	"a table was created at the truncated path"

# ── Layer 3: the Makefile's half of the opt-in ────────────────────────────────
#
# The flag above is reached through `make reachy-provision ON_UNIT=1`, and the
# guard deciding which spellings mean "yes" lives in the Makefile, not in this
# tool. `ON_UNIT=0` is what a person types to mean "not this time": taken as the
# opt-in it lifts the loopback and relative-key refusals they were relying on,
# and the provision run reports success either way. `make -n` runs none of it —
# the emitted command line is the whole contract here.
FIRMWARE="$(cd -- "$HERE/.." && pwd)"

# make_provision <var=value>... — what the recipe would run, and make's own
# status. No target is built and no device is touched.
make_provision() {
	set +e
	OUT=$(make -n -C "$FIRMWARE" reachy-provision "$@" 2>&1)
	EC=$?
	set -e
}

make_provision ON_UNIT=1
expect_ok "make-on-unit-1-is-the-opt-in"
check "make-on-unit-1-passes-the-flag" \
	"$(yes_no grep -q -- '--on-unit' <<<"$OUT")" "output was: $OUT"

make_provision
expect_ok "make-unset-provisions-the-standalone-arrangement"
check "make-unset-passes-no-flag" \
	"$(no_yes grep -q -- '--on-unit' <<<"$OUT")" "output was: $OUT"

# Anything but 1 or empty is refused rather than guessed at, and the refusal is
# make's, before the recipe exists to run.
for spelling in 0 yes true 2; do
	make_provision "ON_UNIT=${spelling}"
	expect_die "make-on-unit-${spelling}-refused" "the only opt-in spelling is ON_UNIT=1"
	check "make-on-unit-${spelling}-runs-nothing" \
		"$(no_yes grep -q provision-reachy-pod.sh <<<"$OUT")" "output was: $OUT"
done

# The config path reaches the tool as one argument: the refusals print this
# command for pasting, and a path with a space in it split into two positionals
# is "too many arguments" from a line the operator copied verbatim.
make_provision ON_UNIT=1 'SPEECH_CONFIG=/tmp/an assembly/speech.toml'
expect_ok "make-spaced-config-path-is-accepted"
check "make-quotes-the-config-path" \
	"$(yes_no grep -qF '"/tmp/an assembly/speech.toml"' <<<"$OUT")" "output was: $OUT"

test_summary provision-reachy-pod.test.sh
