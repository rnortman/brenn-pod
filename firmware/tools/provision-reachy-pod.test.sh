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

run_tool() {
	set +e
	OUT=$(PATH="$STUBS:$PATH" "$TREE/firmware/tools/$(basename -- "$TOOL")" reachy-dev "$@" 2>&1)
	EC=$?
	set -e
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

new_tree
host_config "listen_addr = \"192.168.1.20:7380\"" "pod_psk_file = \"keys/psk.toml\""
run_tool
expect_die "gate-relative-psk-file" "is not an absolute path"

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

test_summary provision-reachy-pod.test.sh
