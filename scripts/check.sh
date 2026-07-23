#!/usr/bin/env bash
# The scripts/ lane gate — the single definition of what this directory's check
# runs. Invoked by the pre-commit router's scripts lane and by the root `check`
# target, so the local hook and public CI enforce the same thing rather than two
# copies that drift.
#
# The linter is optional: when absent this prints a visible SKIP and passes, so
# dev machines without it are not blocked. Ubuntu runners preinstall it, so CI
# always runs it.
#
# (That sentence deliberately avoids opening a comment with the linter's name —
# it would be parsed as a malformed directive and error out.)
set -euo pipefail

cd "$(dirname "$0")/.."

run_shellcheck() {
    if command -v shellcheck >/dev/null 2>&1; then
        echo "scripts/check.sh: running shellcheck scripts/*.sh"
        shellcheck scripts/*.sh
    else
        echo "scripts/check.sh: shellcheck not installed — SKIPPED for scripts/*.sh"
    fi
}

# --shellcheck-only runs the lint step alone. The self-test uses it to pin both
# sides of the optional-shellcheck branch — a silently inverted guard would
# otherwise leave scripts/*.sh unlinted behind a passing green run.
if [ "${1:-}" = "--shellcheck-only" ]; then
    run_shellcheck
    exit 0
fi

echo "scripts/check.sh: running scripts/test-hil-firewall.sh"
scripts/test-hil-firewall.sh

echo "scripts/check.sh: running scripts/test-check-edition.sh"
scripts/test-check-edition.sh

echo "scripts/check.sh: running scripts/check-edition.sh"
scripts/check-edition.sh

run_shellcheck
