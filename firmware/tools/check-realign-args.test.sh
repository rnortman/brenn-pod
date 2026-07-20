#!/usr/bin/env bash
#
# check-realign-args.test.sh — host-only regression tests for check-realign-args.sh.
#
# The gate's awk parser (frame/realign detection, composed a1-offset tracking, the two-class
# allowlist policy, and the detector self-test) is exactly the kind of state-machine logic
# where off-by-one / stale-state / matching bugs hide. Its runtime self-test only fires
# against a real xtensa ELF (needs the ESP toolchain + a built image), so it guards future
# compiled output — not the awk logic itself. This harness feeds hand-written objdump-style
# disassembly fixtures to the gate via a stub llvm-objdump and asserts exit code + key
# output, so the parser has regression coverage on every `make check` with no toolchain.
#
# Two allowlist classes are exercised:
#   REGULAR (ALLOWLIST)     — always-expected; absent-when-not-realigned => SELFTEST-MISS.
#   INTERMITTENT (INTERMITTENT) — oscillates with codegen; absent => silent; present-but-frame-
#                                 unparsed => loud.
# The live ALLOWLIST may legitimately be EMPTY (both current fingerprints are intermittent), so
# the pass/absent cases run against the live gate to prove the real config parses, while the
# class-semantics cases run against synthetic gate variants with controlled arrays so regular
# semantics stay covered regardless of the live contents.
#
# Each case builds a disassembly text, points the gate at it (LLVM_OBJDUMP stub cats the
# "ELF" path, which is the fixture file), and checks the verdict.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GATE="${GATE:-$HERE/check-realign-args.sh}"   # overridable so the suite can be run against a modified gate

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Stub llvm-objdump: ignore the flags, cat the last argument (the "ELF" == the fixture).
STUB="$WORK/objdump-stub.sh"
cat > "$STUB" <<'STUB_EOF'
#!/usr/bin/env bash
cat "${@: -1}"
STUB_EOF
chmod +x "$STUB"

failures=0
casenum=0

# emit_insn <addr> <mnemonic-and-operands>  — one objdump instruction line (one byte column).
emit_insn() { printf '%s: 00 %s\n' "$1" "$2"; }

# A realigned, clean allowlisted-style canary function: realigns the frame, then only touches
# a1 below the frame size and reads through a non-a1 base — no stack-arg access.
emit_canary_clean() {
  # $1 = symbol name, $2 = frame size
  printf '42010000 <%s>:\n' "$1"
  emit_insn 42010000 "entry a1, $2"
  emit_insn 42010003 "movi.n a8, 63"
  emit_insn 42010006 "and a8, a1, a8"
  emit_insn 42010009 "sub a8, a9, a8"
  emit_insn 4201000c "add.n a1, a1, a8"
  emit_insn 4201000f "s32i.n a0, a1, 4"
  emit_insn 42010012 "l32i a2, a5, 0"
}

# The gate's ALLOWLIST + INTERMITTENT arrays are the single source of truth for the canary set.
# Extract and eval both literals so the fixtures always match whatever is checked in — a deliberate
# allowlist change must never break these tests (they test the awk parser, not the list contents).
eval "$(sed -n '/^ALLOWLIST=(/,/^)$/p' "$GATE")"
eval "$(sed -n '/^INTERMITTENT=(/,/^)$/p' "$GATE")"
UNION=( "${ALLOWLIST[@]}" "${INTERMITTENT[@]}" )
if [[ "${#UNION[@]}" -eq 0 ]]; then
  echo "check-realign-args.test.sh: FAIL — could not extract ALLOWLIST/INTERMITTENT from $GATE" >&2
  exit 1
fi

# Fixture symbol for a list entry: entries containing '::' are full stems already; bare names get a
# synthetic module path (the gate matches an entry as a substring of the demangled stem, so both
# forms satisfy the recognition check).
sym_for() {
  if [[ "$1" == *"::"* ]]; then printf '%s' "$1"; else printf 'respeaker_pod::testgen::%s' "$1"; fi
}

# The designated victim canary that live-gate violation cases mutate: the first union entry. Being a
# real list member, its bad body exercises the listed-function message path ("reads/writes a stack
# argument"), not the not-in-allowlist path.
VICTIM_SYM="$(sym_for "${UNION[0]}")"

# Every union canary, realigned and clean.
emit_preamble() {
  local e
  for e in "${UNION[@]}"; do emit_canary_clean "$(sym_for "$e")" 96; done
}

# Every union canary except the victim; the caller emits its own (possibly bad) victim body.
emit_preamble_no_victim() {
  local e
  for e in "${UNION[@]:1}"; do emit_canary_clean "$(sym_for "$e")" 96; done
}

# make_gate_variant <out> <allowlist-newline-list> <intermittent-newline-list>
# Copy the gate but replace both array bodies, so class-specific semantics can be tested even when
# the live ALLOWLIST is empty. Empty list => empty array.
make_gate_variant() {
  local out="$1" al="$2" im="$3"
  awk -v al="$al" -v im="$im" '
    function block(s,  n,arr,i,o){ n=split(s,arr,"\n"); o=""; for(i=1;i<=n;i++) if(arr[i]!="") o=o"  \""arr[i]"\"\n"; return o }
    /^ALLOWLIST=\(/    { print; printf "%s", block(al); skip=1; next }
    /^INTERMITTENT=\(/ { print; printf "%s", block(im); skip=1; next }
    skip && /^\)$/     { print; skip=0; next }
    skip              { next }
    { print }
  ' "$GATE" > "$out"
  chmod +x "$out"
}

# run_case <name> <expected-exit> <expected-substring> <disassembly> [gate]
run_case() {
  casenum=$((casenum + 1))
  local name="$1" want_exit="$2" want_sub="$3" dis="$4" gate="${5:-$GATE}"
  local fixture="$WORK/case-$casenum.dis"
  printf '%s' "$dis" > "$fixture"
  local out ec
  set +e
  out="$(LLVM_OBJDUMP="$STUB" "$gate" "$fixture" 2>&1)"
  ec=$?
  set -e
  if [[ "$ec" != "$want_exit" ]]; then
    echo "FAIL [$name]: expected exit $want_exit, got $ec"
    echo "  output: $out"
    failures=$((failures + 1))
    return
  fi
  if [[ -n "$want_sub" && "$out" != *"$want_sub"* ]]; then
    echo "FAIL [$name]: output missing expected substring: $want_sub"
    echo "  output: $out"
    failures=$((failures + 1))
    return
  fi
  echo "ok   [$name]"
}

# --- Live-gate cases (prove the real, checked-in arrays parse and behave) ---

# 1. All union canaries realigned + clean → gate passes.
run_case "clean-pass" 0 "OK" "$(emit_preamble)"

# 2. Stock-build miscompile: a listed canary reads a1-relative at/past its frame → stack-arg violation.
run_case "stock-violation" 1 "reads/writes a stack argument" "$(
  emit_preamble_no_victim
  printf '42010000 <%s>:\n' "$VICTIM_SYM"
  emit_insn 42010000 "entry a1, 96"
  emit_insn 42010003 "add.n a1, a1, a8"
  emit_insn 42010006 "l32i a9, a1, 120"
)"

# 3. correctness-1 regression: a generic wrapper whose name only CONTAINS a list entry inside its
#    <...> type parameters, non-realigned, reading its legitimate incoming stack args (a1+frame),
#    must NOT be treated as the listed function and must NOT trip.
run_case "wrapper-not-listed" 0 "OK" "$(
  emit_preamble
  printf '42011ab8 <%s>:\n' "std::thread::lifecycle::spawn_unchecked::<${VICTIM_SYM}::{closure#1}, u32>"
  emit_insn 42011ab8 "entry a1, 144"
  emit_insn 42011abb "s32i.n a6, a1, 8"
  emit_insn 42011abe "l32i a9, a1, 144"
)"

# 4. A never-reviewed realigned Rust function in NEITHER list touching a stack arg is a hard VIOLATION.
run_case "unlisted-realigned" 1 "not in allowlist" "$(
  emit_preamble
  printf '42020000 <%s>:\n' "respeaker_pod::foo::unaudited_realigner"
  emit_insn 42020000 "entry a1, 96"
  emit_insn 42020003 "add.n a1, a1, a8"
  emit_insn 42020006 "l32i a9, a1, 120"
)"

# 5. correctness-2 regression: a register that held an a1+const offset and is then reloaded
#    (entry-SP idiom) must not carry the stale offset into a later access and false-trip.
run_case "stale-offset-invalidated" 0 "OK" "$(
  emit_preamble_no_victim
  printf '42010000 <%s>:\n' "$VICTIM_SYM"
  emit_insn 42010000 "entry a1, 96"
  emit_insn 42010003 "add.n a1, a1, a8"
  emit_insn 42010006 "addi a10, a1, 76"
  emit_insn 42010009 "l32i.n a10, a1, 12"
  emit_insn 4201000c "l32i a9, a10, 80"
)"

# 6. correctness-2 regression (non-a1 base): a register first loaded with an a1+const offset, then
#    reused via `addi aR, aX, imm` off a NON-a1 base, must drop the stale a1 offset so a later
#    access through aR is not mis-scored as a stack-arg read.
run_case "stale-offset-non-a1-base" 0 "OK" "$(
  emit_preamble_no_victim
  printf '42010000 <%s>:\n' "$VICTIM_SYM"
  emit_insn 42010000 "entry a1, 96"
  emit_insn 42010003 "add.n a1, a1, a8"
  emit_insn 42010006 "addi a10, a1, 76"
  emit_insn 42010009 "addi a10, a7, 4"
  emit_insn 4201000c "l32i a9, a10, 80"
)"

# --- Intermittent-class cases (against a variant with one intermittent entry, no regular entry) ---

IGATE="$WORK/gate-intermittent.sh"
make_gate_variant "$IGATE" "" "respeaker_pod::flip::channel_holder"
ICANARY="respeaker_pod::flip::channel_holder"

# 7. intermittent-absent: the only intermittent entry is entirely absent from the image → the gate
#    passes with NO SELFTEST-MISS (the defining property: a fingerprint flip does not thrash the gate).
run_case "intermittent-absent-no-miss" 0 "OK" "$(
  printf '42030000 <%s>:\n' "some::other::unrelated_leaf"
  emit_insn 42030000 "entry a1, 48"
  emit_insn 42030003 "s32i.n a0, a1, 4"
)" "$IGATE"

# 8. intermittent-present-clean: the intermittent entry present, realigned, clean → OK.
run_case "intermittent-present-clean" 0 "OK" "$(emit_canary_clean "$ICANARY" 96)" "$IGATE"

# 9. intermittent-present-stackarg: the intermittent entry present, realigned, reads a1 at/past its
#    frame → still a stack-arg VIOLATION (present entries are always scanned).
run_case "intermittent-present-stackarg" 1 "reads/writes a stack argument" "$(
  printf '42010000 <%s>:\n' "$ICANARY"
  emit_insn 42010000 "entry a1, 96"
  emit_insn 42010003 "add.n a1, a1, a8"
  emit_insn 42010006 "l32i a9, a1, 120"
)" "$IGATE"

# 10. intermittent-present-frame-drift: a present intermittent entry whose `entry` frame does not
#     parse (objdump format drift) must fail loudly — the scan was skipped, so no vacuous pass.
run_case "intermittent-present-frame-drift" 1 "did not parse" "$(
  printf '42010000 <%s>:\n' "$ICANARY"
  emit_insn 42010000 "entry a1, 0x60"
  emit_insn 42010003 "add.n a1, a1, a8"
  emit_insn 42010006 "s32i.n a0, a1, 4"
)" "$IGATE"

# --- Regular-class cases (against a variant with one regular entry, so regular semantics stay
#     covered even when the live ALLOWLIST is empty) ---

RGATE="$WORK/gate-regular.sh"
make_gate_variant "$RGATE" "respeaker_pod::always::regular_canary" ""
RCANARY="respeaker_pod::always::regular_canary"

# 11. regular-present-clean: the regular entry present, realigned, clean → OK (self-test satisfied).
run_case "regular-present-clean" 0 "OK" "$(emit_canary_clean "$RCANARY" 96)" "$RGATE"

# 12. regular-selftest-miss: a regular canary that stopped realigning must fail loudly — unlike an
#     intermittent entry, a regular entry is always expected.
run_case "regular-selftest-miss" 1 "SELFTEST-MISS" "$(
  printf '42010000 <%s>:\n' "$RCANARY"
  emit_insn 42010000 "entry a1, 96"
  emit_insn 42010003 "s32i.n a0, a1, 4"
)" "$RGATE"

# 13. errhandling-1 regression: a realigned regular canary whose entry frame does not parse to a
#     positive value (objdump format drift, e.g. hex) must fail loudly, not silently skip.
run_case "regular-frame-parse-drift" 1 "did not parse" "$(
  printf '42010000 <%s>:\n' "$RCANARY"
  emit_insn 42010000 "entry a1, 0x60"
  emit_insn 42010003 "add.n a1, a1, a8"
  emit_insn 42010006 "s32i.n a0, a1, 4"
)" "$RGATE"

echo "----"
if [[ "$failures" -ne 0 ]]; then
  echo "check-realign-args.test.sh: FAIL — $failures case(s) failed"
  exit 1
fi
echo "check-realign-args.test.sh: OK — $casenum cases passed"
