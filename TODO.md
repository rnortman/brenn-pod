# TODOs

## `example-placeholder` (DO NOT TRIAGE — this is a fake entry)

This is a placeholder entry. Leave it here so the file is never empty. It is not a real TODO. You would reference it in code with `// TODO(example-placeholder)` comments. This is the basic TODO system design: An entry here with a slug used to join to code comments. Add real TODOs below this one in this format.

## `brenn-brain-listen` — BLOCKED as of 2026-08-03 (needs a hold-open capability in the listener)

The pub/sub response vocabulary carries a `<listen/>` marker: "hold the microphone open after this
reply, the person is expected to keep talking without saying the wake word again". `BrennBrain`
recognizes it, strips it out of the speech, and reports `LinkListenUnsupported` — the behavior
behind it does not exist. Capture is wake-word-gated, and the only playback-time capture trigger
today is the barge latch (`OwwStream`'s barge path, `host/crates/speech-pipeline/src/listener/
runtime.rs`). Without the marker a conversational exchange costs a wake word per turn, which is
exactly the interaction an LLM on the other end is best at.

The marker is in the wire vocabulary now precisely so this lands without a schema change: the peer
already emits it, the codec already parses it, and the help document already tells the peer it is
accepted-but-inert. So this is listener + pipeline work only — a way to arm capture for a bounded
window after a reply finishes playing, without the wake gate, and a way for the brain to request it
that survives the pipeline's inline dispatch (the marker is known when the segment is spoken, but
the window opens when playback of that segment *ends*).

Deferred rather than dismissed: it is a listener state-machine change with its own failure modes
(what closes the window, what happens when the user says nothing, what happens when the held-open
capture picks up the pod's own playback tail), not a line in the brain.

Done = a `<listen/>`-marked reply holds capture open for a bounded window after its playback ends,
speech in that window dispatches as an ordinary utterance with no wake word, the window closes on
timeout or on the next dispatch, and `LinkListenUnsupported` is retired along with its stat and
console name.

See `TODO(brenn-brain-listen)` at the `Tag::Listen` arm of `BrennBrain::deliver` in
`host/crates/speech-pipeline/src/brenn_brain.rs`.

## `bridge-upgrade-rejection-terminal` — BLOCKED as of 2026-08-02 (needs a change in the brenn repo first)

A bearer token the brenn server does not accept — stale, rotated, mistyped, or pointed at the
wrong `[[remote]]` slug — is answered with a `401` on the websocket upgrade, before any socket
exists. `brenn-bridge` treats that as an ordinary connect failure and re-dials on the backoff
schedule forever, at `reconnect.max_backoff_ms` (default 30 s). Every one of those dials makes
the server emit a fail2ban-fed `AuthFailure` security event, so a pod with a stale credential
spends the rest of its uptime hammering the operator's own auth perimeter until it is banned
by it. This is the front-door twin of `bridge-violation-close-code`, which covers only the
post-attach refusal.

The futile-attachment budget does not catch it: that budget counts attachments on which this
bridge *sent* something, and a rejected upgrade never attaches at all.

The bridge cannot tell the case apart today. `NativeConnector::connect` collapses the
rejection into a stringly `TransportError` (which does carry tungstenite's status line), and
`AttachDriver::connect` then drops the string, answering `ConnInput::ConnectFailed` — the same
answer a refused TCP connect, a bad hostname or a dead server produces. Guessing from a run of
indistinguishable failures would kill a pod for an ordinary server restart.

What landed instead is visibility only: a failed dial now reaches the embedder as
`BridgeEvent::ConnectFailed`, which `bridge-probe` prints, so the failure is diagnosable even
though it is not classifiable.

Done = `brenn-attach-client` surfaces the handshake's HTTP status on a failed connect (a
status, not the response body), `brenn-bridge` ends the process on a `401`/`403` with an exit
code of its own, and the pod's dependency pin moves to the rev carrying it.

The brenn half is filed in the brenn repo's `TODO.md` under this same slug, at
`NativeConnector::connect`. The slug is the cross-repo join key — move both entries together.

See `TODO(bridge-upgrade-rejection-terminal)` at `Core::report_dial` in
`firmware/crates/brenn-bridge/src/bridge.rs`.

## `bridge-violation-close-code` — BLOCKED as of 2026-08-02 (needs a change in the brenn repo first)

When the brenn backend judges an attacher's frame a protocol violation it drops the socket
with no close code and no reason, so the bridge cannot tell a refusal from a network blip.
That matters because the two demand opposite responses: a blip wants a reconnect, and a
refusal wants the process to die — a bridge that reconnects and re-sends the refused
statement earns the same close forever, and each round trips a fail2ban-grade security event
against the operator's own pod.

`brenn-bridge` currently infers the refusal from its shape: `ReconnectConfig::max_futile_attachments`
consecutive attachments that sent something and were answered nothing end the process with
`exit::HARD_FAILURE`. That is a heuristic. A long enough run of drops landing between a
statement and its answer reads as a refusal, and a violation on an attachment that had
already been answered something else does not.

The exact signal is already half-built on both sides: `ConnConfig::terminal_close_code` and
`ConnEvent::PeerClosedTerminal` exist in `brenn-attach-client` for precisely this, and the
browser surface route already uses the mechanism for its stale-build code. What is missing is
the server end.

Done = the brenn remote route closes a violated attachment with a dedicated close code (a
code only, never the violation detail — that text is a security record, not something to hand
the offender), `brenn-bridge` declares it in `Config::conn_config`, the futile budget
becomes a backstop for the shapes the code cannot cover rather than the primary detector, and
a test pins whether a *pre-`Welcome`* handshake violation counts toward the futile budget at
all — it turns on whether the handshake `Hello` sets `spoke`, which nobody has traced. That
last item lives here rather than in brenn because the futile budget is pod code (the
`ConnEvent::Detached` arm of `Core::on_conn_event`, `firmware/crates/brenn-bridge/src/bridge.rs`).

The brenn half is filed in the brenn repo's `TODO.md` under this same slug, at the attach
route's violation teardown. The slug is the cross-repo join key — move both entries together.

See `TODO(bridge-violation-close-code)` at `terminal_close_code` in
`firmware/crates/brenn-bridge/src/config.rs`.

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


## `reachy-beam-mapping` — BLOCKED as of 2026-08-02 (needs a bench session with the reachy pod)

The XVF3800 hands the reachy pod two processed outputs on one stereo capture stream.
They are not two beams: both render the chip's one auto-selected look direction, and
the four telemetry indices are the beamformer's internal beams (two fixed, one
free-running, one auto-select that mirrors whichever fixed beam it has settled on).
So there is no channel↔beam pairing to bake. What is unknown is what each channel
actually carries on this firmware: on the reSpeaker Flex the two are the Conference
beam (AGC-pinned) and the ASR beam (~15 dB more headroom), r=0.91 between them, but
the first bench run of the reachy read the two channels as identical at the printed
precision, which would mean one source routed to both.

The bench case — `beam_energy_speech`, `firmware/devices/reachy-pod/src/beam.rs` —
takes the readings that settle it: the ch0↔ch1 power correlation, whether the two
sample streams are byte-identical, and the chip's own `AUDIO_MGR_OP_L`/`OP_R`
routing registers. It asserts what the pipeline depends on (every channel's level
follows the telemetry the gate is driven by) and fails on one contradiction
(sample-identical channels while the registers say the outputs differ), but it
concludes nothing about the channel identity itself.

Deferred because it is an observation, not a code change: the reading has to come off
the board with someone speaking into it, and per the bring-up doctrine it gets human
review before anything is baked in.

Done = a bench run of `make reachy-bench` recorded in the ADR directory, then baked
into the case as expectations: the ch0/ch1 relationship (one source duplicated versus
the Conference/ASR pair), the reviewed `OP_L`/`OP_R` values as identity assertions,
and the `CHANNEL=` default the reviewed characterization implies — inert if both
channels carry one source, otherwise a choice (the ASR flavor's extra headroom and
ASR tuning is the expected pick for a host-side wake-word/STT consumer).

See `TODO(reachy-beam-mapping)` at `beam_energy_speech` in
`firmware/devices/reachy-pod/src/beam.rs`.

## `barge-in-flake` — not blocked as of 2026-08-13 (un-reproduced; the next failing gate run is the sample)

`barge_in_flushes_playback_and_chains_the_interrupted_turn`
(`host/crates/speech-surface/tests/barge_integration.rs`) fails when the machine it runs
on is busy or has recently been busy, and passes when the machine is quiet. It has been
rejecting commits in both workspaces since 2026-08-02, always green on an immediate
re-run of the same tree with nothing disabled and no `--no-verify`.

**What has reproduced it, and what no longer does.** Every failure on the record is a
failure of a workspace-parallel test run: the first two `make -C firmware check-host`
runs that competed with a cold firmware build (the third passed, against zero failures in
~18 warm or solo runs at the same HEAD), and later four consecutive failures of `cargo
test -p speech-surface --test barge_integration` immediately after a full `make -C
firmware check` on a docs-only tree, with `-- --nocapture` passing once and five plain
runs a minute later green.

A deliberate localization run on 2026-08-13 reproduced nothing: 26 runs on that same
workstation, all green, across three shapes — the recipe as it was written here, a
corrected variant that generated real load (target directories wiped, 3.5 G rebuilt), and
six cycles of the gate's own shape, including one from a cold 239-crate target directory
(brenn-ops
`docs/adr/2026/08/12-multi--head-presence-safety-gate-convergence-contd/barge-localization/results.md`).
The run also found two defects in the recipe: on a clean tree the warm-up step is a
16-second cache hit that generates no load — warming from clean means deleting the target
directories first — and the standalone `cargo test -p speech-surface` invocation is not
the shape any failure has ever occurred in. Any future attempt runs in gate shape, `make
-C host check`.

So the flake is not available on demand, and the ordinary gate is the only instrument
known to produce a failing sample. **Next action: when the gate next rejects on this
test, save that run's full output before any re-run** and file it in the ADR of whatever
work tripped it. The failure carries its own evidence — every assertion interpolates
`daemon.diagnostics()` (the daemon's JSONL, stdout and stderr, printed in the panic) and
the pod tally covers the wire side.

**What is ruled out.** Any diff of ours: the two reproductions above were on trees whose
`host/` workspace was byte-identical to a green base, one of them docs-only. And solo
looping on an idle machine — about 29 cumulative clean isolated runs have produced
nothing, so the several-hundred-iteration idle loop this entry used to ask for is the
wrong instrument.

**The two open readings.** (a) A daemon race: a judge attributed a ~1-in-12 reproduction
to the `playback_no_pod` path when the client disconnects before synthesis lands, i.e.
the `None` arm of `emit_outcome` at
`host/crates/speech-surface/src/playback_router.rs:408` (the arm itself at `:420-426`).
(b) A harness-side timing race: the `-- --nocapture` contrast puts libtest's own output capture in the timing,
which (a) does not explain. Neither is established. Whatever slips, it slips by a lot —
every wait in the test rests on an observed wire or JSONL event with a 20 s deadline,
not on a sleep.

**Stakes.** Under (b) the cost is commits rejected at random and everyone trained to
re-run a red gate rather than read it — observed a fourth time in 2026-08, where a full
`make check` failed here and was re-run to green. That reflex is how a genuine
`speech-surface` regression gets waved through, because "green on a re-run" looks
identical. Under (a) barge-in occasionally misbehaves on live hardware, where nothing
re-runs it.

**The gate keeps this test.** Quarantining it behind a lane carrying this slug was
considered and rejected twice: while a reproduction looked cheap, because under reading
(a) this test failing is the only signal the project has of a barge-in defect that would
misbehave on live hardware, where nothing re-runs anything; and again once the
reproduction vanished, because the gate is then also the only collector of failing
samples, and quarantining would retire it. The accelerant available on request, and the
one instrument never yet tried in the shape that fails, is a several-hundred-cycle
overnight `make -C host check` loop.

Done = the slip localized to the daemon or to the harness, fixed, with a pinning test
for whichever it was; the test running in the ordinary gate; and a post-fix soak in gate
shape — a multi-cycle `make -C host check` loop — green, with the test riding the
ordinary gate without a rejection.

History: the five dated fact/inference notes this entry accreted between 2026-08-02 and
2026-08-12 are archived in the ADR implementation logs and judge verdicts they came from
— brenn-ops `docs/adr/2026/08/03-multi--brenn-brain-bridge-integration/` (the
`playback_no_pod` attribution), `2026/08/07-multi--wake-driven-head-presence/` (three
gate rejections in one feature, two of them mute and still unattributed to anything, so
not evidence for this flake either way), and
`2026/08/12-multi--head-presence-safety-gate-convergence-contd/` (both reproductions
above). The account in this entry is the current one.

See `TODO(barge-in-flake)` at
`barge_in_flushes_playback_and_chains_the_interrupted_turn` in
`host/crates/speech-surface/tests/barge_integration.rs`.


## `stow-budget-source-unpinned` — BLOCKED as of 2026-08-13 (needs a servo fixture the daemon's tests can reach)

`reachy-motiond` opens the deadline a defeated stow escalates on from the *machine's* file:
`SessionActive::stow_deadline` (`firmware/devices/reachy-motiond/src/motion.rs`) adds
`Engaged::stow_budget()`, which is the bench-resolved stow durations plus the settle
timeout (brenn-reachy `crates/reachy-bench/src/commands.rs`). The daemon's own
`stow_duration_s` override never reaches it — deliberately: a policy file may command a
longer controlled stow, but it may not lengthen how long a stow a person has defeated
keeps driving the head. That is the double-window defect this cycle removed, and the
runbook's `stow_duration_s` row now states the property to operators.

Nothing in this repo holds that line. The one test in the area,
`a_defeated_stow_escalates_on_the_clock_it_started_with`, runs against the `Fake`, whose
`stow_deadline` is a stub, so it pins that *one* window is opened and is blind to which
file sized it. A refactor to `self.stow.longest()` — the daemon already owns those
durations for the moves it commands — passes the whole gate and makes the runbook
sentence false.

Deferred rather than dismissed: pinning the seam needs a real `Engaged`, and this repo
has no `BusPort` fake at all. The two ways to get one are a decision, not a line — export
brenn-reachy's test-only scripted-servo fixture (`crates/reachy-bench/src/testutil.rs`,
`mod testutil;` today) as public test support, which changes a sibling crate's public
surface; or write a second scripted servo here, which is a second answer to what a servo
does with a write, the duplication that fixture exists to prevent.

Done = a test in this repo drives a session whose bench stow duration and daemon
`stow_duration_s` override differ, and asserts the deadline the escalation runs on comes
from the bench value — failing if the deadline is ever routed through `Clocks`.

See `TODO(stow-budget-source-unpinned)` at `SessionActive::stow_deadline` in
`firmware/devices/reachy-motiond/src/motion.rs`.


## `script-timebase` — BLOCKED as of 2026-08-09 (needs the Clockwork port's shared clock)

A motion script's step offsets are measured from the moment the daemon *received* the
script (`firmware/crates/motion-proto/src/schedule.rs`). Speech and motion therefore
start together only as closely as bridge delivery allows: the scripter emits the script
as the audio starts streaming, and whatever the delivery takes is added to every offset
in the timeline. That is well inside the ±500 ms this feature accepts, so the head's
raise reads as an acknowledgement and the scheduled stow lands over the tail of the
audio as intended.

It does not survive tighter coupling. Emote and gaze steps computed against the audio
timeline — a beat on a word, a tilt at a phrase — want the two timelines to be the same
timeline, and offsets-from-receipt cannot express that: the daemon has no way to know
what instant the host meant.

The schema already reserves the field. A `base` carrying an absolute start instant makes
every offset absolute too, and what it needs underneath is a timebase both ends share:
either an NTP-disciplined wall clock on both machines, or a clock-mapping beacon pairing
the pod's audio sample clock to the device's monotonic clock. After the Clockwork port
one scheduler owns the audio and motion timelines together and emits scripts against
that clock, which is the natural moment to pick — the two-binary split survives as long
as the timebase is shared.

Deferred rather than dismissed: it is a clock-distribution decision, not a field. Adding
`base` before there is a clock to interpret it against would put an absolute time on the
wire that each end reads differently, which is worse than the honest offsets.

Done = scripts carry absolute step times on a timebase both ends agree on, the daemon
executes against it, and offsets-from-receipt survive only as the fallback when no base
is present.

See `TODO(script-timebase)` at the wire schema in
`firmware/crates/motion-proto/src/script.rs`.
