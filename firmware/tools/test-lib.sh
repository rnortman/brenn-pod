# shellcheck shell=bash
#
# Shared harness for the firmware/tools *.test.sh suites. Sourced, never
# executed — no shebang, not executable:
#
#     # shellcheck source=test-lib.sh
#     . "$(dirname -- "${BASH_SOURCE[0]}")/test-lib.sh"
#
# All of it is text processing over the two conventions the suites already
# share: a case has a name, and the tool's last run leaves its output in $OUT
# and its status in $EC. Living here rather than once per suite means a better
# failure message — or a new spelling of an assertion — reaches every suite at
# once instead of costing one synchronized edit per copy.
#
# What stays with each suite is what differs: its fixture tree, its stubs, and
# the `run_tool` that knows which tool it is calling and with what environment.

# Everything here is read by the suites that source this file, so "appears
# unused" is the expected shape of every definition in it.
# shellcheck disable=SC2034

failures=0
casenum=0

# The tool's last run, filled in by each suite's own `run_tool`.
OUT=""
EC=0

fail() {
	echo "FAIL [$1]: $2"
	failures=$((failures + 1))
}

pass() { echo "ok   [$1]"; }

# check <name> <0-or-1> <what-went-wrong> — assert a condition the caller ran.
check() {
	casenum=$((casenum + 1))
	if [ "$2" = 0 ]; then pass "$1"; else fail "$1" "$3"; fi
}

# The two spellings of a condition, so an assertion reads as what it asserts.
yes_no() { if "$@"; then echo 0; else echo 1; fi; }
no_yes() { if "$@"; then echo 1; else echo 0; fi; }

# expect_die <name> <substring> — the tool refused and said why.
expect_die() {
	casenum=$((casenum + 1))
	local name=$1 want=$2
	if [ "$EC" = 0 ]; then
		fail "$name" "expected a non-zero exit; output: $OUT"
		return
	fi
	if [[ $OUT != *"$want"* ]]; then
		fail "$name" "output missing '${want}'; output: $OUT"
		return
	fi
	pass "$name"
}

# expect_ok <name> [substring] — the tool succeeded, and said this if asked.
expect_ok() {
	casenum=$((casenum + 1))
	local name=$1 want=${2:-}
	if [ "$EC" != 0 ]; then
		fail "$name" "expected success, got exit ${EC}; output: $OUT"
		return
	fi
	if [ -n "$want" ] && [[ $OUT != *"$want"* ]]; then
		fail "$name" "output missing '${want}'; output: $OUT"
		return
	fi
	pass "$name"
}

# expect_exit <name> <code> — the exact status, for the tools whose status is
# the contract.
expect_exit() {
	casenum=$((casenum + 1))
	local name=$1 want=$2
	if [ "$EC" != "$want" ]; then
		fail "$name" "exit ${EC}, wanted ${want}; output: $OUT"
		return
	fi
	pass "$name"
}

# says <name> <regex> — the output carries a line.
says() {
	check "$1" "$(yes_no grep -qE "$2" <<<"$OUT")" "output was:"$'\n'"$OUT"
}

# silent_about <name> <regex> — and does not.
silent_about() {
	check "$1" "$(no_yes grep -qE "$2" <<<"$OUT")" "output was:"$'\n'"$OUT"
}

# test_summary <suite> — the epilogue every suite ends on. Exits nonzero if any
# case failed, so `make check` fails with the suite that failed named.
test_summary() {
	echo "----"
	if [ "$failures" -ne 0 ]; then
		echo "$1: FAIL — ${failures} case(s) failed"
		exit 1
	fi
	echo "$1: OK — ${casenum} cases passed"
}
