#!/usr/bin/env bash
# scripts/test-check-toolchain-pin.sh — self-check for check-toolchain-pin.sh
#
# Builds throwaway trees whose toolchain declarations are broken one at a time,
# and asserts the guard rejects each and accepts the good tree. Without this a
# grep pattern that stopped matching would leave the guard always-green — the
# same silent drift it exists to catch, and the reason the version is written in
# three files rather than one.
#
# Run as a plain shell script; exits 0 on pass, non-zero on failure.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GUARD="$SCRIPT_DIR/check-toolchain-pin.sh"

PASS=0
FAIL=0

pass() { echo "PASS: $1"; ((PASS++)) || true; }
fail() { echo "FAIL: $1"; ((FAIL++)) || true; }

TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

# A minimal tree the guard can read: the two workspace toolchain files, the
# workflow with its install step, and the device crate's own file, which the
# guard must leave alone.
new_tree() {
    local tree
    tree="$(mktemp -d "$TMP_ROOT/tree.XXXXXX")"
    mkdir -p "$tree/scripts" "$tree/.github/workflows" \
        "$tree/firmware/devices/respeaker-pod" "$tree/host"
    cp "$GUARD" "$tree/scripts/check-toolchain-pin.sh"

    printf '# firmware\n[toolchain]\nchannel = "1.97.1"\n' \
        >"$tree/firmware/rust-toolchain.toml"
    printf '# host\n[toolchain]\nchannel = "1.97.1"\n' \
        >"$tree/host/rust-toolchain.toml"
    printf '[toolchain]\nchannel = "esp"\n' \
        >"$tree/firmware/devices/respeaker-pod/rust-toolchain.toml"
    cat >"$tree/.github/workflows/ci.yml" <<'EOF'
jobs:
  check:
    steps:
      # Prose above the step names 1.97.1 too, and is not a declaration.
      - name: Install Rust
        run: rustup toolchain install 1.97.1 --component rustfmt --component clippy
EOF
    echo "$tree"
}

run_guard() {
    (cd "$1" && ./scripts/check-toolchain-pin.sh >"$1/out.txt" 2>&1)
}

expect_ok() {
    local tree=$1 label=$2
    if run_guard "$tree"; then
        pass "$label"
    else
        fail "$label — guard rejected it: $(cat "$tree/out.txt")"
    fi
}

expect_reject() {
    local tree=$1 label=$2 want=$3
    if run_guard "$tree"; then
        fail "$label — guard accepted it"
    elif grep -q -- "$want" "$tree/out.txt"; then
        pass "$label"
    else
        fail "$label — rejected, but not for the stated reason: $(cat "$tree/out.txt")"
    fi
}

tree="$(new_tree)"
expect_ok "$tree" "the shipped shape passes"
if grep -q '1.97.1 agrees' "$tree/out.txt"; then
    pass "and says which release the three agree on"
else
    fail "and says which release the three agree on"
fi

# The one-sided bump: the two workspaces move and CI is left behind, which is the
# failure this guard exists for.
tree="$(new_tree)"
sed -i 's/1\.97\.1/1.98.0/' "$tree/firmware/rust-toolchain.toml" "$tree/host/rust-toolchain.toml"
expect_reject "$tree" "workspaces bumped without ci.yml is rejected" "disagree"

tree="$(new_tree)"
sed -i 's/1\.97\.1/1.98.0/' "$tree/.github/workflows/ci.yml"
expect_reject "$tree" "ci.yml bumped without the workspaces is rejected" "disagree"

tree="$(new_tree)"
sed -i 's/1\.97\.1/1.98.0/' "$tree/host/rust-toolchain.toml"
expect_reject "$tree" "one workspace bumped alone is rejected" "disagree"

# The alias is what the pin replaced: a channel that is not a version is a gate
# whose compiler is the runner image's choice.
tree="$(new_tree)"
sed -i 's/^channel = "1\.97\.1"$/channel = "stable"/' "$tree/host/rust-toolchain.toml"
expect_reject "$tree" "a floating channel is rejected" "not an alias"

# The components are why the install step names a version at all.
tree="$(new_tree)"
sed -i 's/ --component clippy//' "$tree/.github/workflows/ci.yml"
expect_reject "$tree" "an install step that drops clippy is rejected" "--component clippy"

tree="$(new_tree)"
sed -i 's/ --component rustfmt//' "$tree/.github/workflows/ci.yml"
expect_reject "$tree" "an install step that drops rustfmt is rejected" "--component rustfmt"

# The parse itself: a file the guard cannot read, or a step that moved, has to be
# a refusal rather than a comparison of nothing.
tree="$(new_tree)"
rm -f "$tree/host/rust-toolchain.toml"
expect_reject "$tree" "a missing workspace toolchain file is rejected" "not readable"

tree="$(new_tree)"
sed -i 's/rustup toolchain install/rustup default/' "$tree/.github/workflows/ci.yml"
expect_reject "$tree" "an install step that moved is rejected" "found 0"

tree="$(new_tree)"
printf 'channel = "1.97.1"\n' >>"$tree/firmware/rust-toolchain.toml"
expect_reject "$tree" "two channel lines in one file are rejected" "found 2"

# The device crate runs a different compiler by design and is not this guard's
# business: its `channel = "esp"` sits in every tree above and none of them
# rejected it for that.
tree="$(new_tree)"
expect_ok "$tree" "the esp device crate's own channel is left alone"

echo
echo "check-toolchain-pin self-check: ${PASS} passed, ${FAIL} failed"
[ "$FAIL" -eq 0 ]
