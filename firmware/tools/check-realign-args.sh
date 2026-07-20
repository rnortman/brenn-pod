#!/usr/bin/env bash
#
# check-realign-args.sh — build-time guard against the xtensa frame-realign / stack-arg
# miscompile. See TODO(xtensa-realign-stack-args).
#
# The esp Xtensa LLVM backend (stock esp channel) miscompiles a function that BOTH realigns
# its stack frame (holds an align-64 stack temporary, e.g. a `std::sync::mpsc` channel) AND
# takes stack-passed arguments (>6 incoming argument words): it reads those incoming
# arguments relative to the *realigned* SP instead of the entry SP, so it reads — and writes
# through — stale stack words rather than the caller-supplied references. The shipping device
# toolchain IS this stock, known-affected channel; this gate is the primary guard that no
# realigned Rust function in the image takes stack-passed arguments. The source-level fix it
# verifies is keeping every such function's incoming argument words <= 6 (all in registers) —
# e.g. the RtdSegmentIo bundling in net_tests.rs. It runs before every HIL flash
# (`make check-realign`, a prerequisite of `make flash`), so a regression that reintroduces a
# realigned stack-arg function fails the build instead of miscompiling silently.
#
# What it does, over the release firmware ELF:
#   1. Disassembles with the esp-clang llvm-objdump (the GNU objdump cannot decode this image).
#   2. Flags every Rust function whose prologue realigns the stack (an `add(.n) a1, a1, aX`
#      that rewrites SP after `entry`, or the alternate `movsp a1` encoding), within the first
#      instructions after `entry`.
#   3. Every flagged function must be in the checked-in ALLOWLIST below (functions audited to
#      take NO stack-passed arguments). A non-allowlisted realigned function fails the gate.
#   4. For each *realigned* function, scans every a1-relative load/store (plain offsets AND
#      the composed `movi/addmi` + `add`, then access-through-that-register form) and fails if
#      any effective offset reaches at or past the entry frame size — i.e. into the incoming
#      stack-argument region. No realigned Rust function in the image takes stack-passed
#      arguments, so on the stock build these reads never occur and the scan stays clean; the
#      scan's job is to keep it that way. A regression that reintroduced a realigned stack-arg
#      function (e.g. unbundling RtdSegmentIo so `rtd_run_one_segment` spills arguments) would
#      read those args a1-relative and trip here. The scan is gated
#      on `realigned` because a NON-realigned function reading `a1+off` past its frame is just
#      reading its own legitimate incoming stack args (windowed ABI), not a miscompile.
#   5. Detector self-test. Realigned functions carry one of two allowlist classes:
#      - REGULAR (ALLOWLIST): an always-expected realigned function. Each must still be *found and
#        detected realigned* as the exact canary (trailing-segment identity, not a substring inside
#        a closure or monomorphized sibling), with a parsed frame size. A regular entry that stops
#        realigning (or an encoding change that blinds the prologue detector, or an objdump format
#        drift that breaks the frame parse) is loud, so the gate can never pass vacuously.
#      - INTERMITTENT (INTERMITTENT): a reviewed function whose realignment oscillates build-to-build
#        with the upstream codegen instability. It is allowlisted (no VIOLATION) whenever present as
#        a realigned function, and raises NO SELFTEST-MISS when absent — so a fingerprint flip does
#        not thrash the gate. It is NOT self-test-exempt when PRESENT: a present intermittent function
#        whose `entry` frame fails to parse is still loud (its stack-arg scan was skipped), which
#        keeps the objdump-format-drift tripwire alive in whichever shape the current image presents.
#      A realigned Rust function in NEITHER list is always a hard VIOLATION.
#
# Usage: check-realign-args.sh [ELF]   (default: target/xtensa-esp32s3-espidf/release/respeaker-pod)
#        LLVM_OBJDUMP=/path/to/llvm-objdump  overrides objdump discovery.
#
# Exit: 0 = clean; 1 = a realign/stack-arg violation or a failed self-test; 2 = tooling error
# (ELF or objdump not found) — fail-closed, never silently skip.

set -euo pipefail

ELF="${1:-target/xtensa-esp32s3-espidf/release/respeaker-pod}"

# Rust functions known to realign their frame (align-64 stack temporaries) and audited to take
# NO stack-passed arguments. Two arrays. BOTH suppress the "not in allowlist" VIOLATION for a
# realigned function whose demangled STEM — the path before any generic `<...>` argument list —
# contains the entry, so an entry appearing only inside a generic wrapper's type parameters (e.g.
# `spawn_unchecked::<…sync_channel…>`) does NOT masquerade as a listed function, and one stem entry
# covers every monomorphization sharing that stem. BOTH still have their a1-relative access scanned,
# so a listed function that DOES read a stack argument fails the gate. They differ only in the
# absent-case self-test:
#   ALLOWLIST     — always-expected. Absent-when-not-realigned  => SELFTEST-MISS (fail closed).
#   INTERMITTENT  — oscillates with the upstream miscompile's codegen instability. Absent => silent;
#                   a present intermittent entry whose `entry` frame fails to parse is still loud.
# Adding to EITHER array is a deliberate act: it asserts you have confirmed the function's incoming
# argument words are ≤ 6 (all in registers). Entries are demangled STEMs spelled with `::`.
#
# ALLOWLIST is currently EMPTY: no realigned Rust function is present in *every* build. Both observed
# fingerprints are intermittent (below), so the runtime detector self-test has no always-present
# regular canary; the objdump-format-drift tripwire instead rides the present-intermittent frame
# parse. A future always-realigning function would be added here.
#
# INTERMITTENT — the align-64 `std::sync::mpmc` channel temporary (`entry a1, <frame>`, then the
# align-to-64 `movi 63`/`movi 64`/`and`/`sub`/`add a1` idiom) migrates between two codegen shapes
# build-to-build with no source change: the known upstream xtensa-realign miscompile's codegen
# instability (TODO(xtensa-realign-stack-args)). The realign fingerprint has flip-flopped THREE
# times, oscillating between exactly two states:
#
#   State A — the channel temporary OUTLINES into standalone `sync_channel::<T>` monomorphizations,
#   which realign (seen at 634f8f0-era builds and again at 3f6cb64). Four monos, each `entry` frame
#   416, each argument-count reviewed in dispositions-shipgate-user-2-a1.md:
#     - std::sync::mpmc::sync_channel::<respeaker_pod::speaker::PlaybackOutcome>
#     - std::sync::mpmc::sync_channel::<respeaker_pod::speaker::PlaybackRequest>
#     - std::sync::mpmc::sync_channel::<respeaker_pod::streamer::StreamerMsg>
#     - std::sync::mpmc::sync_channel::<()>
#   Each: 2 incoming words, both registers — a2 (sret for the `(SyncSender, Receiver)` tuple),
#   a3 (`bound`). Zero stack-passed arguments. Covered by the single stem entry `std::sync::mpmc::sync_channel`.
#
#   State B — the channel temporary INLINES back into its four callers, which then realign themselves
#   (seen at f35a786..804f38a). Argument-count reviewed in dispositions-shipgate-user-2-a2.md:
#     - respeaker_pod::main                               — entry frame 2528; zero incoming arguments.
#     - respeaker_pod::wifi::spawn_wifi_supervisor_thread — entry frame 480;  zero incoming arguments.
#     - respeaker_pod::speaker::run_speaker_output        — entry frame 608;  zero declared parameters;
#       its (Status, Payload) result returns via the sret pointer in a2, a register.
#     - respeaker_pod::net_tests::rtd_run_one_segment     — entry frame 3168; five incoming words, all
#       in registers: a2 (sret for the `Result<…, String>` return), a3 (`peer_ip: [u8; 4]`),
#       a4 (`rtd_port: u16`), a5 (`scenario: char`), a6 (`&mut RtdSegmentIo`). The RtdSegmentIo bundle
#       keeps the incoming argument words ≤ 6; do not unbundle it.
#
# Flip-flop history: 634f8f0 (state A) ↔ f35a786 (state B) ↔ 3f6cb64 (state A). All eight functions
# were disassembled on the release ELF and audited to take NO incoming stack argument; for every one
# the a1-relative access scan reaches no offset at or past the entry frame. The flashable image is
# stripped of DWARF at link, so the ELF-level disassembly the gate itself parses is the authoritative
# evidence. Audited 2026-07-11.
ALLOWLIST=(
)
INTERMITTENT=(
  "std::sync::mpmc::sync_channel"
  "respeaker_pod::main"
  "respeaker_pod::wifi::spawn_wifi_supervisor_thread"
  "respeaker_pod::speaker::run_speaker_output"
  "respeaker_pod::net_tests::rtd_run_one_segment"
)

if [[ ! -f "$ELF" ]]; then
  echo "check-realign-args: ERROR: firmware ELF not found: $ELF" >&2
  echo "  build it first (make build-firmware) or pass the path as \$1." >&2
  exit 2
fi

OBJDUMP="${LLVM_OBJDUMP:-}"
if [[ -z "$OBJDUMP" ]]; then
  # Newest esp-clang install wins.
  OBJDUMP="$(ls -1 "$HOME"/.espressif/tools/esp-clang/*/esp-clang/bin/llvm-objdump 2>/dev/null \
    | sort -V | tail -n1 || true)"
fi
if [[ -z "$OBJDUMP" || ! -x "$OBJDUMP" ]]; then
  echo "check-realign-args: ERROR: esp-clang llvm-objdump not found." >&2
  echo "  Set LLVM_OBJDUMP=/path/to/esp-clang/.../llvm-objdump (the GNU xtensa objdump cannot" >&2
  echo "  decode this image and must NOT be substituted)." >&2
  exit 2
fi

DIS="$(mktemp)"
trap 'rm -f "$DIS"' EXIT
"$OBJDUMP" --triple=xtensa --mcpu=esp32s3 -d -C "$ELF" > "$DIS"

# The parse + policy is one awk pass. It prints "VIOLATION: …" / "SELFTEST-MISS: …" lines and
# exits nonzero on any, so the awk exit status is the gate verdict.
awk -v allow="${ALLOWLIST[*]}" -v intermittent="${INTERMITTENT[*]}" '
BEGIN {
  n_allow = split(allow, a, " ");
  for (i = 1; i <= n_allow; i++) { allowlist[a[i]] = 1; seen_realigned[a[i]] = 0; }
  n_int = split(intermittent, im, " ");
  for (i = 1; i <= n_int; i++) { intermittent_set[im[i]] = 1; }
  PROLOGUE_WINDOW = 12;   # instructions after `entry` in which the realign idiom must appear
  violations = 0;
}

# True if `k` is the trailing `::`-delimited segment of `s` (an exact identity match, not a
# substring inside a longer path). Used by the self-test so a closure or monomorphized sibling
# whose stem merely *contains* an allowlist entry cannot satisfy the canary check vacuously.
function ends_with_segment(s, k,   ls, lk, before) {
  ls = length(s); lk = length(k);
  if (lk == 0 || lk > ls) return 0;
  if (substr(s, ls - lk + 1) != k) return 0;
  before = ls - lk;
  if (before == 0) return 1;
  return (substr(s, before, 1) == ":");
}

# Function header:  "420180b4 <demangled::name>:"
/^[0-9a-fA-F]+ <.*>:$/ {
  # Close out the previous function before starting a new one.
  finish_function();
  fname = $0;
  sub(/^[0-9a-fA-F]+ </, "", fname);
  sub(/>:$/, "", fname);
  is_rust = (index(fname, "::") > 0);   # demangled Rust paths contain "::"; C symbols do not
  frame = 0; insn = 0; realigned = 0; matched_allow = ""; matched_intermittent = ""; canary_match = "";
  delete a1off;                          # register -> known constant offset from a1
  delete movival;                        # register -> last movi immediate
  # Match the allowlist against the STEM (path before any generic `<...>`), then strip a
  # trailing `::` left by the truncation. `matched_allow` (substring of the stem) gates the
  # stack-arg scan; `canary_match` (entry as the trailing segment of the stem) is the stricter
  # identity the self-test requires, so a closure/monomorphization cannot satisfy it.
  stem = fname;
  p = index(stem, "<");
  if (p > 0) stem = substr(stem, 1, p - 1);
  sub(/::$/, "", stem);
  for (k in allowlist) {
    if (index(stem, k) > 0) matched_allow = k;
    if (ends_with_segment(stem, k)) canary_match = k;
  }
  # Intermittent entries gate the stack-arg scan and suppress the "not in allowlist" VIOLATION on
  # the same STEM-substring basis, but carry no canary self-test (absence is allowed).
  for (k in intermittent_set) {
    if (index(stem, k) > 0) matched_intermittent = k;
  }
  next;
}

# Instruction line:  "420180b4: <hex bytes>\t<mnemonic>\t<operands>"
/^[[:space:]]*[0-9a-fA-F]+:/ {
  if (fname == "") next;
  # Operands/mnemonic live after the byte columns. Take the tail after the last tab; if there
  # are no tabs (spacing-only), fall back to stripping the address+bytes heuristically.
  line = $0;
  # Normalize: drop the leading "  addr:" and the hex byte pairs that follow it.
  sub(/^[[:space:]]*[0-9a-fA-F]+:[[:space:]]*/, "", line);
  sub(/^([0-9a-fA-F][0-9a-fA-F][[:space:]]+)+/, "", line);   # strip byte columns
  gsub(/\t/, " ", line);
  gsub(/,/, " ", line);
  gsub(/[[:space:]]+/, " ", line);
  sub(/^ /, "", line); sub(/ $/, "", line);
  nf = split(line, t, " ");
  if (nf == 0) next;
  mn = t[1];
  insn++;

  if (mn == "entry") {
    # entry a1, <framesize>
    frame = t[3] + 0;
    next;
  }

  # --- realign detection (prologue window only) ---
  if (!realigned && insn <= PROLOGUE_WINDOW) {
    if ((mn == "add" || mn == "add.n") && t[2] == "a1" && (t[3] == "a1" || t[4] == "a1")) {
      realigned = 1;
    } else if (mn == "movsp" && t[2] == "a1") {
      realigned = 1;
    }
  }

  # --- track composed a1-relative base registers (whole function) ---
  if (mn == "movi" || mn == "movi.n") {
    # movi aReg, imm
    movival[t[2]] = t[3] + 0;
    delete a1off[t[2]];    # reg redefined as a constant, not an a1 offset
  } else if ((mn == "add" || mn == "add.n") && t[2] != "a1") {
    # aReg = op2 + op3 ; if one operand is a1 and the other is a known movi constant, aReg = a1 + const
    if (t[3] == "a1" && (t[4] in movival)) { a1off[t[2]] = movival[t[4]]; delete movival[t[2]]; }
    else if (t[4] == "a1" && (t[3] in movival)) { a1off[t[2]] = movival[t[3]]; delete movival[t[2]]; }
    else { delete a1off[t[2]]; delete movival[t[2]]; }
  } else if (mn == "addmi" && t[3] == "a1") {
    # addmi aReg, a1, imm
    a1off[t[2]] = t[4] + 0; delete movival[t[2]];
  } else if (mn == "addi" && t[3] == "a1") {
    a1off[t[2]] = t[4] + 0; delete movival[t[2]];
  } else if (mn == "addmi" || mn == "addi") {
    # addi/addmi off a non-a1 base: the destination no longer holds an a1 offset, so drop any
    # stale tracking for it (mirrors the non-a1 `add`/`add.n` branch). Without this, a register
    # reused as `addi aR, aX, imm` (aX != a1) would keep a prior a1off[aR] and mis-score a later
    # access through aR as a stack-arg read.
    delete a1off[t[2]]; delete movival[t[2]];
  }

  # --- a1-relative load/store scan (only meaningful once we know the frame size) ---
  # Only a *realigned* function can exhibit the miscompile: it reads its incoming stack
  # arguments relative to the realigned SP. A non-realigned function reading `a1+off` with
  # `off >= frame` is simply reading its own legitimate incoming stack-argument words (the
  # Xtensa windowed ABI places argument words 7+ at `a1+frame+k`) — not a fault. So the scan
  # fires only when `realigned` is set, for allowlisted and non-allowlisted Rust alike.
  if (mn ~ /^(l32i|l16ui|l16si|l8ui|s32i|s16i|s8i|l32i\.n|s32i\.n)$/) {
    # form: <mn> aData, aBase, <offset>
    base = t[3]; off = (nf >= 4 ? t[4] + 0 : 0);
    eff = -1;
    if (base == "a1") eff = off;
    else if (base in a1off) eff = a1off[base] + off;
    if (eff >= 0 && frame > 0 && eff >= frame && realigned) {
      if (matched_allow != "" || matched_intermittent != "") {
        printf("VIOLATION: allowlisted function %s reads/writes a stack argument: %s reaches a1+%d (entry frame %d)\n", fname, mn, eff, frame);
        violations++;
      } else if (is_rust) {
        printf("VIOLATION: realigned Rust function %s accesses a1+%d past its %d-byte frame (stack arg?)\n", fname, eff, frame);
        violations++;
      }
    }
  }

  # --- invalidate composed-offset tracking for registers this instruction overwrites ---
  # Runs after the scan so a load/ALU op that reads a tracked base is still scored against the
  # pre-write value of that base. Skips the constructive forms above (they just set tracking) and
  # instructions that do not write a tracked data register (stores, control flow, barriers).
  # Without this, a register reused after holding an `a1+const` value (e.g. a reloaded
  # entry-SP via `l32i aReg, a1, k`) would carry a stale offset into a later access and be
  # mis-scored — a codegen-dependent false violation on an otherwise clean build.
  if (mn == "movi" || mn == "movi.n" || mn == "addmi" || mn == "addi") {
    # constructive: tracking already (re)set above.
  } else if ((mn == "add" || mn == "add.n") && t[2] != "a1") {
    # constructive add: tracking already resolved above.
  } else if (mn ~ /^(call8|callx8|call4|callx4|call12|callx12|call0|callx0)$/) {
    # windowed calls return values in the low callee window (a10+); drop any pre-call offset.
    delete a1off["a10"]; delete movival["a10"];
    delete a1off["a11"]; delete movival["a11"];
  } else if (mn ~ /^(s32i|s16i|s8i|s32i\.n|s16i\.n|s8i\.n|s32ri|s32c1i)$/) {
    # store: t[2] is a source register, not a destination — leave tracking intact.
  } else if (mn ~ /^b/ || mn ~ /^(j|jx|ret|ret\.n|retw|retw\.n|nop|nop\.n|memw|extw|isync|rsync|esync|dsync|loop|loopnez|loopgtz|entry)$/) {
    # control flow / loops / barriers / entry: no tracked data-register write.
  } else if (nf >= 2 && t[2] ~ /^a[0-9]+$/) {
    delete a1off[t[2]]; delete movival[t[2]];
  }
  next;
}

function finish_function() {
  if (fname == "") return;
  if (is_rust && realigned) {
    if (canary_match != "") {
      # A realigned canary whose `entry` frame did not parse to a positive value means the
      # stack-arg scan (guarded by `frame > 0`) silently did nothing for the very function this
      # gate exists to check — objdump format drift or a column shift. Fail loudly rather than
      # mark the canary seen: a passing self-test must mean the scan actually ran.
      if (frame > 0) {
        seen_realigned[canary_match] = 1;
      } else {
        printf("SELFTEST-MISS: allowlisted function %s realigned but its `entry` frame did not parse (frame=%d) — the stack-arg scan was skipped; objdump output format may have drifted\n", fname, frame);
        violations++;
      }
    } else if (matched_intermittent != "") {
      # Present intermittent entry, realigned. No canary bookkeeping — its absence is allowed, so it
      # is not tracked in seen_realigned and never raises an END-block SELFTEST-MISS. But the stack-arg
      # scan (guarded by `frame > 0`) must actually have run: a present entry whose `entry` frame did
      # not parse was silently skipped, which — with no always-present regular canary — would let an
      # objdump format drift pass vacuously. Fail loudly in that case.
      if (frame <= 0) {
        printf("SELFTEST-MISS: intermittent function %s realigned but its `entry` frame did not parse (frame=%d) — the stack-arg scan was skipped; objdump output format may have drifted\n", fname, frame);
        violations++;
      }
    } else if (matched_allow == "") {
      printf("VIOLATION: realigned Rust function not in allowlist: %s (entry frame %d) — review its incoming argument count before allowlisting\n", fname, frame);
      violations++;
    }
    # A realigned symbol that matched an allowlist/intermittent substring but is NOT the canary (a
    # closure or monomorphized sibling) is intentionally neither counted as the canary nor reported
    # here; its own stack-arg accesses were already scanned above.
  }
  fname = "";
}

END {
  finish_function();
  # Detector self-test: every allowlist entry must have been found AND detected realigned.
  for (i = 1; i <= n_allow; i++) {
    if (seen_realigned[a[i]] == 0) {
      printf("SELFTEST-MISS: allowlisted function %s was not found as a realigned Rust function — the detector may be blind (encoding change?) or the function stopped realigning; remove its allowlist entry or fix the detector\n", a[i]);
      violations++;
    }
  }
  if (violations > 0) {
    printf("check-realign-args: FAIL — %d issue(s) above\n", violations);
    exit 1;
  }
  print "check-realign-args: OK — no realigned Rust function takes stack arguments; allowlist verified";
  exit 0;
}
' "$DIS"
