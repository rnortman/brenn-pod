#!/usr/bin/env bash
# .githooks/self-test.sh — self-check for the pre-commit router.
#
# Two tiers:
#   1. Routing-plan tests — feed NUL-delimited staged-path lists to
#      `pre-commit --plan` on stdin and assert the printed plan tokens. Because
#      --plan shares the exact NUL read loop with the production git-diff path,
#      these tests exercise the real input parser and the real classifier.
#   2. Execute-mode tests — drive the whole hook (collection -> route -> lane
#      dispatch) with PATH-stubbed git/make/rustup, asserting that the right
#      runner fires, that a failing check blocks the commit, and that a failing
#      collection fails closed rather than skipping.
#   3. Real-rules tests — run the real brenn-scrub against the repo's real
#      .gitleaks.toml in a throwaway repo. Tiers 1-2 stub the binary, so
#      nothing there ever parses the rule config; this tier pins what the rules
#      actually match. Skipped with a visible notice when the tooling is absent.
#
# Also runs shellcheck on the hook and agent-hook scripts when the binary is
# present, and prints a visible skip notice when it is absent (no silent skips).
#
# Run as a plain shell script; exits 0 on pass, non-zero on failure. No real git
# repo or cargo toolchain required.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PRECOMMIT="$SCRIPT_DIR/pre-commit"
PREPUSH="$SCRIPT_DIR/pre-push"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ---------------------------------------------------------------------------
# Test harness
# ---------------------------------------------------------------------------
PASS=0
FAIL=0

pass() { echo "PASS: $1"; ((PASS++)) || true; }
fail() { echo "FAIL: $1"; ((FAIL++)) || true; }

# run_plan: emit the routing plan for the given paths (each arg is one staged
# path). Paths are NUL-delimited so spaces/newlines survive. With no args,
# printf still emits a single NUL — the empty-staged-set case.
run_plan() {
    printf '%s\0' "$@" | "$PRECOMMIT" --plan
}

# assert_plan: the plan for the given paths equals the expected token block.
# Usage: assert_plan LABEL EXPECTED PATH...
assert_plan() {
    local label="$1" expected="$2"
    shift 2
    local got
    got="$(run_plan "$@")"
    if [[ "$got" == "$expected" ]]; then
        pass "$label"
    else
        fail "$label — expected [$expected], got [$got]"
    fi
}

# assert_hardfail: `--plan` for the given path exits non-zero and names the
# routing table in its message.
assert_hardfail() {
    local label="$1" badpath="$2"
    local out rc
    out="$(printf '%s\0' "$badpath" | "$PRECOMMIT" --plan 2>&1)" && rc=0 || rc=$?
    if [[ "$rc" -ne 0 ]] && printf '%s' "$out" | grep -qF -- "add it to the routing table"; then
        pass "$label"
    else
        fail "$label — expected nonzero exit + routing-table message, got rc=$rc out=[$out]"
    fi
}

# ---------------------------------------------------------------------------
# Routing plan tests
# ---------------------------------------------------------------------------
assert_plan "empty staged set -> skip"                "skip"
assert_plan "docs-only -> skip"                       "skip"      "docs/adr/foo.md"
assert_plan ".claude-only -> skip"                    "skip"      ".claude/settings.json"
assert_plan ".github-only -> skip"                    "skip"      ".github/workflows/ci.yml"
assert_plan "root markdown -> skip"                   "skip"      "CLAUDE.md"
assert_plan "root dotfile -> skip"                    "skip"      ".gitignore"
assert_plan "root scrub config -> skip"               "skip"      ".gitleaks.toml"
assert_plan "root Makefile -> skip"                   "skip"      "Makefile"
assert_plan "root LICENSE -> skip"                    "skip"      "LICENSE"
assert_plan "root NOTICE -> skip"                     "skip"      "NOTICE"
assert_plan "firmware-only -> firmware"               "firmware"  "firmware/crates/audio-pipeline/src/lib.rs"
assert_plan "device-crate -> firmware-device"         "firmware-device" "firmware/devices/respeaker-pod/src/main.rs"
assert_plan "host-only -> host"                        "host"     "host/crates/pod-ingest/src/main.rs"
assert_plan "firmware+host -> firmware + host-covered" "$(printf 'firmware\nhost-covered')" "firmware/x.rs" "host/y.rs"
assert_plan "device+host -> firmware-device + host-covered" \
    "$(printf 'firmware-device\nhost-covered')" "firmware/devices/respeaker-pod/src/main.rs" "host/y.rs"
assert_plan "deletion-only firmware path -> firmware"  "firmware" "firmware/gone.rs"
assert_plan "scripts-only -> scripts"                 "scripts"   "scripts/thing.sh"
assert_plan ".githooks-only -> githooks"              "githooks"  ".githooks/pre-commit"
assert_plan "path with spaces -> firmware"            "firmware"  "firmware/a b c.rs"
assert_plan "path with newline -> firmware"           "firmware"  "$(printf 'firmware/a\nb.rs')"
assert_plan "all lanes (host deduped -> host-covered, docs/.claude benign)" \
    "$(printf 'firmware\nhost-covered\ngithooks\nscripts')" \
    "firmware/a.rs" "host/b.rs" ".githooks/pre-commit" "scripts/c.sh" "docs/e.md" ".claude/settings.json"

assert_hardfail "unknown top-level dir -> hard fail"  "weird-subproject/thing.rs"
assert_hardfail "unknown root file -> hard fail"      "Cargo.toml"

# ---------------------------------------------------------------------------
# Static wiring tests. These relate two files that must agree but have no
# runtime coupling, so nothing else can catch them drifting apart.
# ---------------------------------------------------------------------------

# Every run_script binding in the router must name a real executable. Rename a
# runner or drop its +x bit and every other test here stays green; the breakage
# surfaces later as "internal error" on the next commit touching that lane.
mapfile -t RUNNER_PATHS < <(
    grep -oE 'run_script "[^"]+"' "$PRECOMMIT" | sed 's/^run_script "//; s/"$//' | sort -u
)
if [ "${#RUNNER_PATHS[@]}" -eq 0 ]; then
    fail "lane runners: no run_script bindings found in $PRECOMMIT — extraction is broken"
else
    for rp in "${RUNNER_PATHS[@]}"; do
        if [ -x "$REPO_ROOT/$rp" ]; then
            pass "lane runner is present and executable: $rp"
        else
            fail "lane runner missing or not executable: $rp"
        fi
    done
fi

# The root `check` target claims to be the union of every lane CI runs, but it
# hardcodes its steps while the router derives lanes from staged paths. Add a
# lane to the router and CI silently stops covering it — the exact drift this
# repo's gate exists to prevent, one level up. Both sides are derived here.
#
# A token with no step and no exemption is a hard fail: the map below must be
# extended deliberately, which is the point.
gate_step_for_token() {
    case "$1" in
        # host/ is reached transitively: firmware's check-host runs it via
        # check-host-arch, so one step covers both workspaces.
        firmware|host)     echo '-C firmware check-host' ;;
        githooks)          echo '.githooks/self-test.sh' ;;
        scripts)           echo 'scripts/check.sh' ;;
        # Not lanes: a coverage announcement and the no-lane-touched token.
        host-covered|skip) echo '' ;;
        *)                 return 1 ;;
    esac
}

# Deliberate gaps in the public gate, each named with the slug that closes it.
gate_exempt_token() {
    case "$1" in
        firmware-device) echo 'TODO(ci-esp-clippy) — device clippy needs the esp toolchain' ;;
        *)               return 1 ;;
    esac
}

mapfile -t PLAN_TOKENS < <(
    sed -n '/^    local emitted=0$/,/^}$/p' "$PRECOMMIT" \
        | grep -oE '\becho [a-z-]+' | awk '{print $2}' | sort -u
)
CHECK_RECIPE="$(sed -n '/^check:$/,/^$/p' "$REPO_ROOT/Makefile")"

if [ "${#PLAN_TOKENS[@]}" -eq 0 ] || [ -z "$CHECK_RECIPE" ]; then
    fail "gate union: could not derive plan tokens or the root check recipe — extraction is broken"
else
    for tok in "${PLAN_TOKENS[@]}"; do
        if exemption="$(gate_exempt_token "$tok")"; then
            pass "gate union: '$tok' deliberately exempt — $exemption"
        elif ! step="$(gate_step_for_token "$tok")"; then
            fail "gate union: plan token '$tok' maps to no step in the root check target — add the step to Makefile's check, or declare an exemption naming its TODO slug"
        elif [ -z "$step" ]; then
            pass "gate union: '$tok' needs no step of its own"
        elif printf '%s\n' "$CHECK_RECIPE" | grep -qF -- "$step"; then
            pass "gate union: '$tok' -> '$step' present in root check"
        else
            fail "gate union: '$tok' expects '$step' in the root check recipe, which is missing it — public CI would not run this lane"
        fi
    done
fi

# ---------------------------------------------------------------------------
# Execute-mode tests: drive the whole hook with PATH-stubbed git/make/rustup.
# These reach the parts --plan never touches: collect_staged, the lane-dispatch
# loop, run_make, the esp-toolchain policy/degraded branch, and the failure
# vocabulary. The stubs make dispatch outcomes deterministic without a git repo
# or a cargo toolchain.
# ---------------------------------------------------------------------------
STUBDIR="$(mktemp -d)"
# A second stub dir, deliberately missing brenn-scrub, for the absent-binary
# case. The real binary lives in ~/.cargo/bin, so a PATH of this dir plus the
# system bins (grep/mktemp/rm, which the hook itself needs) genuinely has no
# brenn-scrub on it.
NOSCRUBDIR="$(mktemp -d)"
# Throwaway git repo for the real-rules tier; created there, cleaned up here.
RULEDIR=""
# Two PATHs for exercising scripts/check.sh's optional-shellcheck branch: one
# with no shellcheck at all, one where it exists and fails. Each holds a bash so
# the script's `env bash` shebang still resolves under the replaced PATH.
NOSCDIR="$(mktemp -d)"
SCFAILDIR="$(mktemp -d)"
trap 'rm -rf "$STUBDIR" "$NOSCRUBDIR" "$NOSCDIR" "$SCFAILDIR" ${RULEDIR:+"$RULEDIR"}' EXIT
ln -s "$(command -v bash)" "$NOSCDIR/bash"
ln -s "$(command -v bash)" "$SCFAILDIR/bash"
cat > "$SCFAILDIR/shellcheck" <<'STUB'
#!/usr/bin/env bash
echo "STUB-SHELLCHECK failing on $*"
exit 1
STUB
chmod +x "$SCFAILDIR/shellcheck"

cat > "$STUBDIR/git" <<'STUB'
#!/usr/bin/env bash
case "$1" in
    rev-parse) exit 0 ;;                       # HEAD exists (no unborn branch)
    diff)
        [ "${STUB_GIT_DIFF_FAIL:-0}" = 1 ] && exit 128
        # shellcheck disable=SC2086
        [ -n "${STUB_STAGED:-}" ] && printf '%s\0' $STUB_STAGED
        exit 0 ;;
    status) exit 0 ;;                          # porcelain: clean worktree
    *) exit 0 ;;
esac
STUB

cat > "$STUBDIR/make" <<'STUB'
#!/usr/bin/env bash
echo "STUB-MAKE $*"
exit "${STUB_MAKE_RC:-0}"
STUB

cat > "$STUBDIR/rustup" <<'STUB'
#!/usr/bin/env bash
# `esp` is present unless STUB_RUSTUP_NO_ESP=1, which drives the esp-absent
# policy/degraded branches. `stable` is always present.
[ "${STUB_RUSTUP_NO_ESP:-0}" = 1 ] || echo esp
echo stable
exit 0
STUB

cat > "$STUBDIR/brenn-scrub" <<'STUB'
#!/usr/bin/env bash
echo "STUB-SCRUB $*"
exit "${STUB_SCRUB_RC:-0}"
STUB

chmod +x "$STUBDIR/git" "$STUBDIR/make" "$STUBDIR/rustup" "$STUBDIR/brenn-scrub"
cp "$STUBDIR/git" "$STUBDIR/make" "$STUBDIR/rustup" "$NOSCRUBDIR/"

# run_hook: run the hook under the stubs with the given VAR=VAL env; captures the
# combined output in HOOK_OUT and the exit code in HOOK_RC.
run_hook() {
    HOOK_OUT="$(env "$@" PATH="$STUBDIR:$PATH" "$PRECOMMIT" 2>&1)" && HOOK_RC=0 || HOOK_RC=$?
}

# exec_rc_ok: HOOK_RC matches the wanted exit-code class (zero|nonzero).
exec_rc_ok() {
    case "$1" in
        zero)    [ "$HOOK_RC" -eq 0 ] ;;
        nonzero) [ "$HOOK_RC" -ne 0 ] ;;
    esac
}

# exec_case: assert exit-code class and that an output substring is present.
# Usage: exec_case LABEL zero|nonzero NEEDLE VAR=VAL...
exec_case() {
    local label="$1" want_rc="$2" needle="$3"
    shift 3
    run_hook "$@"
    if exec_rc_ok "$want_rc" && printf '%s' "$HOOK_OUT" | grep -qF -- "$needle"; then
        pass "$label"
    else
        fail "$label — rc=$HOOK_RC out=[$HOOK_OUT]"
    fi
}

# exec_line_case: assert exit-code class and that an exact whole output LINE is
# present, so 'check' cannot substring-alias 'check-host' (full vs degraded lane).
# Usage: exec_line_case LABEL zero|nonzero LINE VAR=VAL...
exec_line_case() {
    local label="$1" want_rc="$2" line="$3"
    shift 3
    run_hook "$@"
    if exec_rc_ok "$want_rc" && printf '%s\n' "$HOOK_OUT" | grep -qxF -- "$line"; then
        pass "$label"
    else
        fail "$label — rc=$HOOK_RC out=[$HOOK_OUT]"
    fi
}

# exec_case_absent: assert exit-code class, a required substring, and that a
# forbidden substring is absent.
# Usage: exec_case_absent LABEL zero|nonzero NEEDLE FORBIDDEN VAR=VAL...
exec_case_absent() {
    local label="$1" want_rc="$2" needle="$3" forbidden="$4"
    shift 4
    run_hook "$@"
    if exec_rc_ok "$want_rc" \
        && printf '%s' "$HOOK_OUT" | grep -qF -- "$needle" \
        && ! printf '%s' "$HOOK_OUT" | grep -qF -- "$forbidden"; then
        pass "$label"
    else
        fail "$label — rc=$HOOK_RC out=[$HOOK_OUT]"
    fi
}

# esp present (default stub): firmware lanes run the full `make check`. The
# whole-line match rules out the degraded `check-host` line.
exec_line_case "exec: firmware (non-device) + esp -> full make check" \
    zero "STUB-MAKE -C firmware check" STUB_STAGED="firmware/x.rs"
exec_case "exec: failing check blocks the commit" \
    nonzero "make -C firmware check' failed" STUB_STAGED="firmware/x.rs" STUB_MAKE_RC=1
exec_case "exec: docs-only skips with the honest notice" \
    zero "no checkable subproject touched" STUB_STAGED="docs/x.md"
exec_case "exec: collection failure fails closed (no silent skip)" \
    nonzero "git diff --cached failed while collecting" STUB_STAGED="firmware/x.rs" STUB_GIT_DIFF_FAIL=1

# ESP-toolchain policy branch (the design's "who may commit from which machine"
# decision). STUB_RUSTUP_NO_ESP=1 makes the stub report no esp toolchain.
exec_case_absent "exec: device-crate + no esp -> policy refusal (not a hook bug)" \
    nonzero "cannot lint firmware/devices/respeaker-pod" "internal error" \
    STUB_STAGED="firmware/devices/respeaker-pod/src/main.rs" STUB_RUSTUP_NO_ESP=1
exec_case "exec: firmware (non-device) + no esp -> device clippy skipped, not blocked" \
    zero "device-crate clippy SKIPPED" \
    STUB_STAGED="firmware/x.rs" STUB_RUSTUP_NO_ESP=1
exec_line_case "exec: no esp -> degraded lane runs make check-host" \
    zero "STUB-MAKE -C firmware check-host" \
    STUB_STAGED="firmware/x.rs" STUB_RUSTUP_NO_ESP=1
exec_line_case "exec: device-crate + esp -> full make check (not check-host)" \
    zero "STUB-MAKE -C firmware check" \
    STUB_STAGED="firmware/devices/respeaker-pod/src/main.rs"

# ---------------------------------------------------------------------------
# Scrub gate. It runs ahead of lane dispatch and is lane-independent, so the
# cases below pin: that it fires at all, that it fires even when no lane does,
# that a finding blocks the commit before any lane runs, and that an absent
# binary refuses rather than silently skipping the gate.
# ---------------------------------------------------------------------------
exec_line_case "scrub: runs on a firmware commit" \
    zero "STUB-SCRUB staged" STUB_STAGED="firmware/x.rs"
exec_line_case "scrub: runs on a docs-only commit (no lane dispatches)" \
    zero "STUB-SCRUB staged" STUB_STAGED="docs/x.md"

# The lever here is the stub's exit code, not any string: this pins that the
# router propagates a non-zero scrub exit, and that it does so *before* any lane
# runs — hence the assertion that no STUB-MAKE line was emitted. What strings
# actually produce that non-zero exit is the real-rules tier's job.
exec_case_absent "scrub: non-zero scrub exit blocks before lane dispatch" \
    nonzero "brenn-scrub staged' failed" "STUB-MAKE" \
    STUB_STAGED="firmware/x.rs" STUB_SCRUB_RC=1

# Absent binary: policy refusal, not a hook bug, and no lane runs.
run_hook_noscrub() {
    HOOK_OUT="$(env "$@" PATH="$NOSCRUBDIR:/usr/bin:/bin" "$PRECOMMIT" 2>&1)" \
        && HOOK_RC=0 || HOOK_RC=$?
}
run_hook_noscrub STUB_STAGED="firmware/x.rs"
if [ "$HOOK_RC" -ne 0 ] \
    && printf '%s' "$HOOK_OUT" | grep -qF -- "brenn-scrub not on PATH" \
    && ! printf '%s' "$HOOK_OUT" | grep -qF -- "internal error" \
    && ! printf '%s' "$HOOK_OUT" | grep -qF -- "STUB-MAKE"; then
    pass "scrub: absent binary -> policy refusal, no lane runs"
else
    fail "scrub: absent binary -> policy refusal — rc=$HOOK_RC out=[$HOOK_OUT]"
fi

# ---------------------------------------------------------------------------
# Push gate. No lane runs it, so without this case a broken pre-push commits
# clean and surfaces only at push time. The gate is enforcing: the hook execs
# `brenn-scrub range` with no --warn-only. The whole-line match pins that the
# flag does not creep back, and the second invocation pins the enforce contract
# itself — a non-zero scrub exit must propagate and block the push (warn-only
# would have swallowed it to rc 0).
# ---------------------------------------------------------------------------
ZERO_SHA="0000000000000000000000000000000000000000"
PUSH_REFLINE="$(printf 'refs/heads/main %s refs/heads/main %s\n' "$ZERO_SHA" "$ZERO_SHA")"
# Clean scrub: hook runs `range` (no --warn-only) and the push proceeds.
PUSH_OUT="$(printf '%s\n' "$PUSH_REFLINE" \
    | env PATH="$STUBDIR:$PATH" "$PREPUSH" 2>&1)" && PUSH_RC=0 || PUSH_RC=$?
# Dirty scrub: the non-zero exit must propagate and block the push.
printf '%s\n' "$PUSH_REFLINE" \
    | env STUB_SCRUB_RC=1 PATH="$STUBDIR:$PATH" "$PREPUSH" >/dev/null 2>&1 \
    && PUSH_BLOCK_RC=0 || PUSH_BLOCK_RC=$?
if [ "$PUSH_RC" -eq 0 ] \
    && printf '%s\n' "$PUSH_OUT" | grep -qxF -- "STUB-SCRUB range" \
    && [ "$PUSH_BLOCK_RC" -ne 0 ]; then
    pass "pre-push: runs scrub range, blocking (no --warn-only)"
else
    fail "pre-push: runs scrub range, blocking — rc=$PUSH_RC block_rc=$PUSH_BLOCK_RC out=[$PUSH_OUT]"
fi

# ---------------------------------------------------------------------------
# Agent write-time hook. Its exit code is load-bearing: the PreToolUse contract
# treats 2 as blocking and every other non-zero as a non-blocking error, so an
# absent binary must exit 1 — the write goes through, and the commit gate (which
# refuses outright, above) is the hard stop. Nothing else pins that number.
# ---------------------------------------------------------------------------
SC_HOOK="$REPO_ROOT/.claude/hooks/scrub-check.sh"
SC_OUT="$(printf '%s' '{"tool_name":"Write","tool_input":{"file_path":"x.md","content":"hi"}}' \
    | env PATH="$NOSCRUBDIR:/usr/bin:/bin" "$SC_HOOK" 2>&1)" && SC_RC=0 || SC_RC=$?
if [ "$SC_RC" -eq 1 ] && printf '%s' "$SC_OUT" | grep -qF -- "not on PATH"; then
    pass "scrub-check: absent binary -> exit 1 (non-blocking), not 0 or 2"
else
    fail "scrub-check: absent binary -> exit 1 — rc=$SC_RC out=[$SC_OUT]"
fi

# ---------------------------------------------------------------------------
# scripts/check.sh's optional-shellcheck branch, both directions. The dangerous
# one is silent: invert the guard or mistype the binary name and CI prints a
# SKIP line nobody reads while scripts/*.sh goes unlinted behind a green run.
# --shellcheck-only isolates the branch from the hil-firewall test, which is
# slow and irrelevant here.
# ---------------------------------------------------------------------------
SCRIPTS_CHECK="$REPO_ROOT/scripts/check.sh"

SC_SKIP_OUT="$(env PATH="$NOSCDIR" "$SCRIPTS_CHECK" --shellcheck-only 2>&1)" \
    && SC_SKIP_RC=0 || SC_SKIP_RC=$?
if [ "$SC_SKIP_RC" -eq 0 ] \
    && printf '%s' "$SC_SKIP_OUT" | grep -qF -- "SKIPPED for scripts/*.sh"; then
    pass "scripts/check.sh: shellcheck absent -> visible SKIP, rc 0"
else
    fail "scripts/check.sh: shellcheck absent -> visible SKIP — rc=$SC_SKIP_RC out=[$SC_SKIP_OUT]"
fi

SC_FAIL_OUT="$(env PATH="$SCFAILDIR" "$SCRIPTS_CHECK" --shellcheck-only 2>&1)" \
    && SC_FAIL_RC=0 || SC_FAIL_RC=$?
if [ "$SC_FAIL_RC" -ne 0 ] \
    && printf '%s' "$SC_FAIL_OUT" | grep -qF -- "STUB-SHELLCHECK"; then
    pass "scripts/check.sh: shellcheck present and failing -> non-zero exit"
else
    fail "scripts/check.sh: shellcheck failure must red — rc=$SC_FAIL_RC out=[$SC_FAIL_OUT]"
fi

# ---------------------------------------------------------------------------
# Real-rules tier: everything above stubs brenn-scrub, so .gitleaks.toml is
# never parsed by anything under test. Here the real binary scans real fixtures
# in a throwaway repo carrying this repo's real config. The two negative cases
# matter most — they pin the allowlist boundaries, the direction in which a
# widened path glob or a dropped regex silently stops catching anything.
# ---------------------------------------------------------------------------
if command -v brenn-scrub >/dev/null 2>&1 && command -v gitleaks >/dev/null 2>&1; then
    RULEDIR="$(mktemp -d)"
    mkdir -p "$RULEDIR/nohooks"
    cp "$REPO_ROOT/.gitleaks.toml" "$RULEDIR/.gitleaks.toml"
    (
        cd "$RULEDIR"
        git init -q .
        git config core.hooksPath "$RULEDIR/nohooks"
        git config user.email selftest@example.invalid
        git config user.name "self-test"
        git add .gitleaks.toml
        git commit -qm "rules"
    ) >/dev/null

    # scrub_flags: stage one fixture in the throwaway repo and return the real
    # scrub's exit code (non-zero = flagged). Leaves the repo clean either way.
    scrub_flags() {
        local name="$1" content="$2" rc=0
        printf '%s\n' "$content" > "$RULEDIR/$name"
        ( cd "$RULEDIR" && git add "$name" && brenn-scrub staged ) >/dev/null 2>&1 || rc=$?
        ( cd "$RULEDIR" && git rm -q -f --cached "$name" >/dev/null 2>&1 || true )
        rm -f "$RULEDIR/$name"
        return "$rc"
    }

    # rule_case: assert whether a fixture is flagged. WANT is flagged|clean.
    rule_case() {
        local label="$1" want="$2" name="$3" content="$4" rc=0 ok=0
        scrub_flags "$name" "$content" || rc=$?
        case "$want" in
            flagged) if [ "$rc" -ne 0 ]; then ok=1; fi ;;
            clean)   if [ "$rc" -eq 0 ]; then ok=1; fi ;;
        esac
        if [ "$ok" -eq 1 ]; then
            pass "$label"
        else
            fail "$label — want $want, scrub rc=$rc"
        fi
    }

    # The section symbol is assembled from its UTF-8 bytes rather than written
    # literally: a literal one in this file is itself a finding, so the commit
    # adding these cases would be blocked by the very rule they test.
    SECT="$(printf '\xc2\xa7')"
    rule_case "rules: section symbol in source is flagged" \
        flagged "a.rs" "// see the transport design ${SECT}3"
    rule_case "rules: RFC citation in source is allowlisted" \
        clean "b.rs" "// RFC 2119 ${SECT}5 defines MUST"
    rule_case "rules: section symbol in markdown is allowlisted" \
        clean "c.md" "## ${SECT}3 Scope"
else
    echo "self-test: brenn-scrub/gitleaks not installed — SKIPPED for real-rules tier"
fi

# ---------------------------------------------------------------------------
# Shellcheck on the hook scripts (when present)
# ---------------------------------------------------------------------------
if command -v shellcheck >/dev/null 2>&1; then
    if shellcheck "$PRECOMMIT" "$PREPUSH" "$SCRIPT_DIR/self-test.sh" \
        "$REPO_ROOT/.claude/hooks/scrub-check.sh" "$REPO_ROOT/.claude/hooks/format.sh"; then
        pass "shellcheck: hook scripts clean"
    else
        fail "shellcheck: hook scripts reported issues"
    fi
else
    echo "self-test: shellcheck not installed — SKIPPED for hook scripts"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "Results: $PASS passed, $FAIL failed."

if [[ "$FAIL" -gt 0 ]]; then
    exit 1
fi
exit 0
