#!/usr/bin/env bash
# scripts/test-check-edition.sh — self-check for check-edition.sh
#
# Builds throwaway repos (mktemp + git init) whose edition declarations are
# deliberately broken one at a time, and asserts the guard rejects each one and
# accepts the good tree. Without this a grep pattern that stops matching would
# degrade the guard to always-pass with no signal — the same silent drift the
# guard exists to catch.
#
# Run as a plain shell script; exits 0 on pass, non-zero on failure.
# No external test framework required.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GUARD="$SCRIPT_DIR/check-edition.sh"

PASS=0
FAIL=0

pass() { echo "PASS: $1"; ((PASS++)) || true; }
fail() { echo "FAIL: $1"; ((FAIL++)) || true; }

TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

# Build a minimal repo the guard can check: two workspace roots, the format hook,
# and one member manifest per workspace, all tracked in a fresh git index.
new_tree() {
    local tree
    tree="$(mktemp -d "$TMP_ROOT/tree.XXXXXX")"
    mkdir -p "$tree/scripts" "$tree/.claude/hooks" \
        "$tree/firmware/crates/demo" "$tree/host/crates/demo-host"
    cp "$GUARD" "$tree/scripts/check-edition.sh"

    cat >"$tree/firmware/Cargo.toml" <<'EOF'
[workspace]
members = ["crates/*"]

[workspace.package]
edition = "2024"
license = "MIT"
EOF
    cat >"$tree/host/Cargo.toml" <<'EOF'
[workspace]
members = ["crates/*"]

[workspace.package]
edition = "2024"
license = "MIT"
EOF
    cat >"$tree/firmware/crates/demo/Cargo.toml" <<'EOF'
[package]
name = "demo"
edition.workspace = true
EOF
    cat >"$tree/host/crates/demo-host/Cargo.toml" <<'EOF'
[package]
name = "demo-host"
edition.workspace = true
EOF
    cat >"$tree/.claude/hooks/format.sh" <<'EOF'
#!/bin/bash
# The edition is hardcoded; the workspace roots are the source of truth.
case "$file" in
    *.rs) rustfmt --edition 2024 "$file" >/dev/null 2>&1 ;;
esac
EOF

    git -C "$tree" init -q >/dev/null 2>&1
    git -C "$tree" add -A >/dev/null 2>&1
    printf '%s' "$tree"
}

GUARD_OUT=""
GUARD_EXIT=0
run_guard() {
    GUARD_OUT="$(bash "$1/scripts/check-edition.sh" 2>&1)" && GUARD_EXIT=0 || GUARD_EXIT=$?
}

report() {
    local label="$1" want_ok="$2" needle="$3"
    if [ "$want_ok" = "ok" ] && [ "$GUARD_EXIT" -ne 0 ]; then
        fail "$label — expected exit 0, got $GUARD_EXIT"
        printf '%s\n' "$GUARD_OUT" | sed 's/^/    /'
        return
    fi
    if [ "$want_ok" = "reject" ] && [ "$GUARD_EXIT" -eq 0 ]; then
        fail "$label — expected a non-zero exit, got 0"
        printf '%s\n' "$GUARD_OUT" | sed 's/^/    /'
        return
    fi
    if [ -n "$needle" ] && ! printf '%s' "$GUARD_OUT" | grep -qF -- "$needle"; then
        fail "$label — expected output to mention: $needle"
        printf '%s\n' "$GUARD_OUT" | sed 's/^/    /'
        return
    fi
    pass "$label"
}

# ---------------------------------------------------------------------------
# 1: the good tree passes and says what it agreed on
# ---------------------------------------------------------------------------
tree="$(new_tree)"
run_guard "$tree"
report "good tree: exits 0" ok "edition 2024 agrees"

# ---------------------------------------------------------------------------
# 2: a workspace root out of step with the others is rejected
# ---------------------------------------------------------------------------
tree="$(new_tree)"
sed -i 's/^edition = "2024"$/edition = "2021"/' "$tree/host/Cargo.toml"
run_guard "$tree"
report "root edition flipped: rejected" reject "editions disagree"

# ---------------------------------------------------------------------------
# 3: the hook's rustfmt flag out of step is rejected
# ---------------------------------------------------------------------------
tree="$(new_tree)"
sed -i 's/--edition 2024/--edition 2021/' "$tree/.claude/hooks/format.sh"
run_guard "$tree"
report "hook edition flipped: rejected" reject "editions disagree"

# ---------------------------------------------------------------------------
# 4: a member manifest re-introducing a literal key is rejected
# ---------------------------------------------------------------------------
tree="$(new_tree)"
sed -i 's/^edition\.workspace = true$/edition = "2024"/' "$tree/firmware/crates/demo/Cargo.toml"
git -C "$tree" add -A >/dev/null 2>&1
run_guard "$tree"
report "member literal key: rejected" reject "literal edition key"

# ---------------------------------------------------------------------------
# 5: a member manifest with no edition at all is rejected (Cargo would use 2015)
# ---------------------------------------------------------------------------
tree="$(new_tree)"
sed -i '/^edition\.workspace = true$/d' "$tree/host/crates/demo-host/Cargo.toml"
git -C "$tree" add -A >/dev/null 2>&1
run_guard "$tree"
report "member edition omitted: rejected" reject "missing 'edition.workspace = true'"

# ---------------------------------------------------------------------------
# 6: prose in the hook that spells out the flag is not a second declaration
# ---------------------------------------------------------------------------
tree="$(new_tree)"
sed -i '2i # Formatting runs rustfmt --edition 2024 on the file.' "$tree/.claude/hooks/format.sh"
run_guard "$tree"
report "hook comment naming the flag: still passes" ok "edition 2024 agrees"

# ---------------------------------------------------------------------------
# 7: a vendored third-party manifest is not held to the inheritance convention
# ---------------------------------------------------------------------------
tree="$(new_tree)"
mkdir -p "$tree/firmware/vendor/thirdparty"
cat >"$tree/firmware/vendor/thirdparty/Cargo.toml" <<'EOF'
[package]
name = "thirdparty"
edition = "2018"
EOF
git -C "$tree" add -A >/dev/null 2>&1
run_guard "$tree"
report "vendored manifest: ignored" ok "edition 2024 agrees"

# ---------------------------------------------------------------------------
# 8: an unusable git index fails loudly instead of skipping half the guard
# ---------------------------------------------------------------------------
tree="$(new_tree)"
rm -rf "$tree/.git"
run_guard "$tree"
report "no git index: rejected" reject ""

# ---------------------------------------------------------------------------
# 9: an unreadable workspace root is named, not silently skipped
# ---------------------------------------------------------------------------
tree="$(new_tree)"
rm -f "$tree/firmware/Cargo.toml"
run_guard "$tree"
report "missing workspace root: rejected" reject "firmware/Cargo.toml: not readable"

echo ""
echo "Results: $PASS passed, $FAIL failed."

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
exit 0
