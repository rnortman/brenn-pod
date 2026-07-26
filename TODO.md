# TODOs

## `example-placeholder` (DO NOT TRIAGE — this is a fake entry)

This is a placeholder entry. Leave it here so the file is never empty. It is not a real TODO. You would reference it in code with `// TODO(example-placeholder)` comments. This is the basic TODO system design: An entry here with a slug used to join to code comments. Add real TODOs below this one in this format.

## `ci-device-clippy-first-run` — BLOCKED as of 2026-07-25 (needs a real GitHub-hosted runner)

The `device-clippy` job in `.github/workflows/ci.yml` has never executed. Everything below
its `actions/checkout` step is unexercised by any in-tree test and unverifiable off a
runner: the pinned espup release-asset URL and its sha256, `espup install --targets esp32s3
--toolchain-version 1.96.0.0` accepting that version string, `~/export-esp.sh` landing where
the toolchain cache expects it, `python3-venv` being enough for ESP-IDF's tools installer,
`build-std` finding `rust-src` in espup's toolchain, the assumption that clippy never needs
`ldproxy`, and the `!~/.espressif/dist` cache exclusion not provoking a re-download on a warm
restore. Two of the cheap failure modes were already retired against the maintainer's local
toolchain (`espup --version` prints `espup 0.17.1`; `rustc +esp --version` reports
`1.96.0.0`); the rest are runner-only. Until it runs, the repo carries an advisory job whose
green/red signal is unknown, and the cold wall time and disk footprint — the numbers that
decide whether the job is worth keeping — do not exist.

Blocked purely on pushing the branch, which is the maintainer's action. The job is
self-measuring: its `Footprint` step prints `du`/`df` on every run, including failed ones.

Done = a real run with all three jobs green, and the cold wall time, the warm wall time, the
`Footprint` step's `du`/`df` output, and the compressed cache sizes from the repo's Actions
caches page recorded in the ADR. If the cold run exceeds the 60-minute timeout, or warm runs
routinely exceed ~10 min, the numbers go to the maintainer with a keep/drop/restructure
recommendation rather than a silently raised timeout.

See `TODO(ci-device-clippy-first-run)` at the `device-clippy` job header in
`.github/workflows/ci.yml`.

## `ci-pinned-tool-extract` — BLOCKED as of 2026-07-25 (needs a factoring decision, and touching the two protected jobs)

`.github/workflows/ci.yml` now carries three near-copies of the pinned-binary install
skeleton — shellcheck (`check` job), gitleaks (`scrub` job), espup (`device-clippy` job):
`cd "$RUNNER_TEMP"` → curl a release asset → `sha256sum -c` → place/chmod → `--version`
self-check → PATH. The variation between them is real (raw binary vs tar.xz vs tar.gz,
PATH vs absolute invocation) but the part carrying the security discipline is identical, so
hardening it — `curl --retry`, a stricter version check, moving to published checksum files
— is three synchronized edits, and the copies have already drifted cosmetically. Three is
the conventional extraction threshold.

Deferred, not dismissed. Two reasons. First, the factoring is a decision, not a mechanical
move: a local composite action (`.github/actions/pinned-tool`) and a `scripts/` helper taking
url/sha/asset-type trade differently (the composite keeps it inside the workflow vocabulary
and can be reused by other workflows; the script is testable from a developer's shell and by
`scripts/check.sh`), and the asset-type variation has to be absorbed somewhere. Second, the
extraction necessarily rewrites the install steps of the two jobs whose behavior the current
CI work was explicitly constrained not to touch, and it would land before `device-clippy`
has had its first real green run — so the change would be made against an install path not
yet proven to work.

Done = the three call sites share one pinned-download implementation, each still naming its
own version + sha256, with the shellcheck and gitleaks jobs verified green afterward. Do it
no later than the arrival of a fourth pinned tool.

See `TODO(ci-pinned-tool-extract)` at the `Install espup` step in
`.github/workflows/ci.yml`.

## `podctl-dfu-serial` — BLOCKED as of 2026-07-18 (hardware observation: DFU-mode USB serial-number exposure unverified)

In `podctl`'s device-selection policy (`select()`, the `--serial`→DFU→AC4 branch — see
`docs/adr/2026/06/07-podctl-cli/design.md` §4 "Device targeting & selection policy" branch 2),
classifying a `--serial` match that lands on a DFU-mode pod as AC4 ("boot app firmware") rather
than AC7 ("not found") assumes the ESP32-S3 ROM/DFU bootloader USB descriptor exposes the same
USB serial number string as app mode. This is unverified: app-mode and DFU-mode are different
firmware (app vs ROM bootloader) with different USB descriptors; the bootloader may report
`serial_number: None` or a different value. If it reports `None`, the `--serial`→DFU→AC4 branch
is dead and the case silently falls to AC7. Per CLAUDE.md bring-up doctrine, confirm DFU-mode
serial-number exposure with a HIL observation (enumerate a pod forced into DFU, assert whether
`UsbPortInfo::serial_number` is `Some`) before pinning AC4 there. Until verified, the `--serial`
path is best-effort (matching requirements' "best-effort" framing for `--serial`) and AC7 on a
DFU pod is acceptable. The `--port`→DFU→AC4 path is guaranteed (port_name always matches) and is
unaffected. Place the `TODO(podctl-dfu-serial)` comment at the `--serial` branch of `select()`
when implemented.

## `espidf-lts-pin` — BLOCKED as of 2026-07-17 (external: awaiting an ESP-IDF LTS line that esp-idf-svc/hal support)

ESP-IDF is pinned to v5.5.4, which is what esp-idf-svc 0.52 / esp-idf-hal 0.46 are tested
against at bring-up. v5.3.x LTS was abandoned because the ecosystem's current crates are
incompatible with it. Revisit: once an ESP-IDF LTS release and the esp-idf-svc/hal ecosystem's
tested/compatible version actually align, pin to that LTS line for OTA-longevity and long-term
support. Referenced by `TODO(espidf-lts-pin)` at `ESP_IDF_VERSION` in
`firmware/devices/respeaker-pod/.cargo/config.toml`.

## `config-backend-parse-dont-validate` — BLOCKED as of 2026-07-18 (needs the `embedded` backend to land)

Blocked on the `embedded` backend / next config table landing: the trigger is a third copy of
the pattern, and there are still only 2 builders with 1 backend variant each.

`build_transcriber` and `build_synthesizer` in `host/crates/speech-surface/src/server.rs`
each extract their `http`-backend fields from `Option<String>` config values with
`.expect("... present when backend=http")`, re-asserting a presence invariant that a distant
`Config::validate()` enforces. A required field added to `validate()` but missed in a builder
compiles clean and panics at runtime. Fix: move to parse-don't-validate — have each backend
variant carry a struct with non-optional fields (e.g.
`TtsBackend::Http(HttpTtsEndpoint { url, model, voice, .. })`) produced by validation, so the
builders destructure instead of `expect`ing.

Shape of the refactor, recorded so the eventual implementer does not rediscover it: the
backend enums must lose their `Copy` derive (they would carry owned `String` fields), and the
flat TOML layout prevents moving the fields into the variant in place — the change implies a
two-type split, a raw deserialized config type and a validated one, with validation as the
conversion between them. Budget for that, not for an in-place field move.
See `TODO(config-backend-parse-dont-validate)` comment at `build_synthesizer` in
`host/crates/speech-surface/src/server.rs`.

## `xtensa-realign-stack-args` — BLOCKED as of 2026-07-18 (upstream: awaiting the Xtensa LLVM fix release)

The esp Xtensa LLVM backend (stock, pre-patch) miscompiles a function that BOTH realigns its
stack frame (holds an align-64 stack temporary, e.g. a `std::sync::mpsc` channel) AND takes
stack-passed arguments (>6 incoming argument words): it reads those incoming arguments relative
to the *realigned* SP instead of the entry SP, reading — and writing through — stale stack words
instead of the caller-supplied references (root cause + instruction-level proof:
`docs/adr/2026/07/07-audio-streamer-realtime-drain/design-delta-4.md` §1).

Current posture (E1b — `docs/adr/2026/07/07-audio-streamer-realtime-drain/design-e1b-toolchain.md`, `docs/adr/2026/07/07-audio-streamer-realtime-drain/holistic-reset-plan.md` §4 E1b): the
`esp-patched` pin was retired at E1b and the device crate builds on the stock `esp` channel
(`rust-toolchain.toml`), which still carries the miscompile. Delta-4's H1 register-only
signature is now implemented — `rtd_run_one_segment`'s four caller-owned `&mut` args are
bundled into `RtdSegmentIo` so it takes zero stack-passed arguments (delta-5 §4's withdrawal
and its conditional-revival clause are superseded: the revival happened, for E1b
decontamination rather than the anticipated multi-machine trigger). The guard is now (a) the
`RtdSegmentIo` constraint comment keeping every realigned Rust function's incoming argument
words ≤ 6 (all in registers), and (b) the build-time audit
`firmware/tools/check-realign-args.sh`, run before every HIL flash (`make check-realign`, a
prerequisite of `make flash`), which fails the build if any realigned Rust function reads a
stack argument.

Done = upstream fix released, the `esp` channel advanced past it, the audit retired, and the
`RtdSegmentIo` constraint comment relaxed to plain API-shape rationale. Upstream issue link:
TBD (file at the next opportunity; the minimized repro + the ported 21.1.3 patch already exist
in `~/src/llvm-xtensa-repro`).

See `TODO(xtensa-realign-stack-args)` in `firmware/tools/check-realign-args.sh` and the
matching `RtdSegmentIo` constraint comment in
`firmware/devices/respeaker-pod/src/net_tests.rs`.

## `heap-floor-post-flash-boot-path-offset`

The RTD heap-floor rebake (`docs/adr/2026/07/19-rtd-heap-floor-rebake/`) baked
`HEAP_MIN_EVER_FLOOR` (53_248) from five `reset reason = POWERON` cold-boot samples
(`mh_post` 76_008–78_564). The design §5.5 acceptance run, a `reset reason = unknown`
post-flash-reset boot at *better* signal than any bake sample, measured
`mh_post=67_916` — 8.1 KB below the lowest POWERON sample, consuming most of the
25% headroom (realized margin ~21.6%, not 25%) on a single non-bake-population
sample (`run-record.md` "Acceptance run" section).

`HEAP_MIN_EVER_FLOOR` also gates the `DeviceHealthCheck` self-test
(`evaluate_health` via `run_device_health_check`,
`firmware/devices/respeaker-pod/src/health.rs`), which runs on every suite run
regardless of boot path, including post-flash resets — but the floor was baked
exclusively on POWERON samples. If post-flash-reset boots systematically retain
less internal RAM than POWERON boots, the margin on the health-check path is
narrower than the bake record implies, and the first legitimate post-flash run to
dip further would fail as a surprise rather than the informed one-visible-rebake
tradeoff the design intended.

Deferred: distinguishing "systematic boot-path offset" from "five-run bake
under-sampling ordinary variance" needs more samples, and either explanation still
leaves the constant defensible today (53_248 is well below both the POWERON and the
single post-flash observation) — not a code-review action item, a data-gathering
one for a future measurement session.

Done = at least five post-flash-reset `mh_post` samples recorded (matching the
POWERON bake's sample count). If the post-flash population clusters measurably below
the POWERON population, re-bake `HEAP_MIN_EVER_FLOOR` against `min()` of both
populations combined, not against POWERON alone. Otherwise document why the single
low sample was an outlier.

The re-bake threshold is deliberately left unstated: the pre-re-bake arithmetic in
this entry (53_248, ~71 KB, the design's 25% headroom target) is obsolete and must be
re-derived at the measurement session against the floor as it stands — 24_576 against
an observed worst case of 30_512, i.e. ~5_936 B, ~19.5%, already under the original
bake rule's 25%. Note what that leaves: an 8.1 KB systematic boot-path offset of the
kind this entry hypothesizes would exceed the entire present margin.

Note (2026-07-23 ship-gate re-bake): `HEAP_MIN_EVER_FLOOR` was lowered
53_248 → 24_576 under the ship-gate directive, after this cycle's TLS-PSK +
heap-instrumentation additions drove the observed `min_heap_after` to 30_512.
The 53_248 figures above predate that re-bake; the boot-path-offset question itself
is unchanged.

Note (2026-07-25, `docs/adr/2026/07/25-pod--hil-report-observability/`): the labelling
half of this entry now exists — every reported heap sample carries its boot path. The
`StreamRealtimeDuplex` detail line carries `rr=<code>` beside `mh_post`, the
`DeviceHealth` typed report carries a `reset_reason` field beside `min_heap`, and every
`DeviceHealthCheck` fail detail ends in `rr=<code>`
(`device_protocol::reset_reason_label` decodes the code; a post-flash reset reports 0,
labelled `unknown`). So the five post-flash-reset samples this entry needs now
accumulate for free from ordinary `make hil-test` runs — no dedicated measurement
session to schedule, just transcripts to read. What remains is the reading and the
re-derivation above.

See `TODO(heap-floor-post-flash-boot-path-offset)` at `HEAP_MIN_EVER_FLOOR` in
`firmware/crates/device-protocol/src/lib.rs`.

## `tls-link-bench-measure` — BLOCKED as of 2026-07-25 (needs a bench session with the pod)

The TLS-PSK audio link (`docs/adr/2026/07/22-pod--tls-and-auth/`) landed with the
mbedTLS record buffers at their IDF defaults — 16 KB in + 4 KB out, roughly 20.5 KB
of internal RAM for the one long-lived session, since plain `malloc` stays internal
under `CONFIG_SPIRAM_USE_CAPS_ALLOC` — and the streamer thread's stack raised
20480 → 28672 for the ECDHE handshake. Both numbers are engineering estimates that
no bench run has confirmed: against the observed `mh_post` population (~76–78 KB
POWERON, 67.9 KB post-flash outlier) and `HEAP_MIN_EVER_FLOOR = 53_248`, 20.5 KB
plausibly fits but consumes most of the remaining margin.

The measurement is the design's §7 plan and needs hardware: a full `make hil-test`
suite run with the TLS link live, plus the two-invocation post-feed procedure from
`docs/adr/2026/07/19-heap-gate-measure/` (a second, separate
`RESPEAKER_HIL_ONLY=DeviceHealthCheck make hil-test` on the same boot), reading both
the post-feed `min_heap` trough and the streamer stack HWM the health report carries.
Expect a `HEAP_MIN_EVER_FLOOR` re-bake either way, combined with the sampling
`heap-floor-post-flash-boot-path-offset` already wants and following that entry's
"re-bake against `min()` of both populations" rule.

If the trough does not clear the floor with ~25% headroom, the recorded fallback
levers in preference order are `CONFIG_MBEDTLS_DYNAMIC_BUFFER=y` (allocate/free the
SSL buffers by connection state) then `CONFIG_MBEDTLS_SSL_VARIABLE_BUFFER_LENGTH=y`.
Both are global and must be re-validated against `run_tls_reachability`, which
speaks cert-based TLS to a public endpoint. Shrinking
`CONFIG_MBEDTLS_SSL_IN_CONTENT_LEN` is off the table for the same reason — public
endpoints send 16 KB records.

Deferred because it is a data-gathering session on the bench pod, not a code change:
the numbers cannot be produced from the host side, and an unexpected reading gets
human review before anything is re-baked to match it.

Done = post-feed `min_heap` and streamer stack HWM recorded with the TLS link live,
`HEAP_MIN_EVER_FLOOR` re-baked or explicitly confirmed against them, and the stack
size either confirmed or tuned to the observed watermark.

Note (2026-07-23 ship-gate re-bake): the predicted `HEAP_MIN_EVER_FLOOR` re-bake
happened — the floor was lowered 53_248 → 24_576 under the ship-gate directive
after the observed `min_heap_after` came in at 30_512 with the TLS link live. The
53_248 figure and the 20.5 KB-margin / ~76–78 KB `mh_post` headroom analysis above
predate it and need re-derivation against the new floor at the bench session; the
buffer/stack measurement this entry calls for is otherwise unchanged.

Note (2026-07-25 triage, `docs/adr/2026/07/25-pod--todo-burndown/`): validation confirmed
the instrumentation this entry needs already exists — the health report carries
`streamer_hwm` (`health.rs:123-133`), so no new code is required before the bench run. The
heap half is also partly discharged: the 30_512 `min_heap_after` reading was taken with the
TLS link live. What remains unmeasured is the **stack** half — no `streamer_hwm` reading
appears anywhere in the record, so the 28672 stack size has never been checked against an
actual watermark and may be several KB of waste. Blocked purely on bench time; collect the
`heap-floor-post-flash-boot-path-offset` samples in the same session.

See `TODO(tls-link-bench-measure)` at the streamer thread's `.stack_size` in
`firmware/devices/respeaker-pod/src/streamer.rs` and in the TLS-PSK block of
`firmware/devices/respeaker-pod/sdkconfig.defaults`.

