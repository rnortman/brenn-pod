# TODOs

## `example-placeholder` (DO NOT TRIAGE — this is a fake entry)

This is a placeholder entry. Leave it here so the file is never empty. It is not a real TODO. You would reference it in code with `// TODO(example-placeholder)` comments. This is the basic TODO system design: An entry here with a slug used to join to code comments. Add real TODOs below this one in this format.

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

## `audio-auth`

Production TLS/auth selection for the audio TCP stream: mbedTLS-PSK exposure via
`EspTls` vs mutual TLS. Explicitly deferred from the MVP audio-transport milestone.
The feasibility question (whether `esp-idf-svc`'s `EspTls` exposes PSK configuration
at the Rust API level) should be verified before this milestone opens. Revisit-ifs
that could force an earlier decision: off-LAN streaming, browser-based live
monitoring. See `docs/adr/2026/06/09-audio-transport/decision.md` §Production auth.
Reference: `TODO(audio-auth)` comment at the streamer TCP connect site in
`firmware/devices/respeaker-pod/src/streamer.rs`.

## `espidf-lts-pin` — BLOCKED as of 2026-07-17 (external: awaiting an ESP-IDF LTS line that esp-idf-svc/hal support)

ESP-IDF is pinned to v5.5.4, which is what esp-idf-svc 0.52 / esp-idf-hal 0.46 are tested
against at bring-up. v5.3.x LTS was abandoned because the ecosystem's current crates are
incompatible with it. Revisit: once an ESP-IDF LTS release and the esp-idf-svc/hal ecosystem's
tested/compatible version actually align, pin to that LTS line for OTA-longevity and long-term
support. Referenced by `TODO(espidf-lts-pin)` at `ESP_IDF_VERSION` in
`firmware/devices/respeaker-pod/.cargo/config.toml`.

## `playback-auth`

ACCEPTED RISK (deliberate decision, recorded for the speaker-RX-audio increment —
`docs/adr/2026/06/21-speaker-rx-audio`).  `PlaybackSink` now drives the speaker amplifier
via `I2sStreamSink`, and the server→device audio channel is plaintext TCP with no peer
authentication.  Since the persistent-connection change the socket is *always open and
always reading*, independent of VAD, so any on-LAN host that can reach the audio port can
inject arbitrary audio (spoofed voice commands, covert audio) out of the speaker at any
moment, with no segment gating to bound the window.  This is accepted for now: the audio
server is treated as trusted and the LAN as the security perimeter.  The fix — mutual
TLS/PSK on the audio connection — is tracked as the separate follow-on `audio-auth` and is
NOT a blocker for the speaker-RX-audio work.  Re-evaluate this acceptance when `audio-auth`
lands.  See `TODO(playback-auth)` comment at the playback sink abstraction in
`firmware/devices/respeaker-pod/src/speaker.rs`.

## `pod-auth-threat-model`

Disposition (recorded 2026-07-18): LAN-trust accepted for now; revisit with `audio-auth` / device auth on `Hello`. Slug stays open pending that work.

The audio protocol carries no device authentication: `Hello.pod_id` is a
free-form string any LAN TCP peer supplies, and it keys the process-wide,
cross-connection `ResumeLedger` and drives record-store attribution
(`Segment.pod`/`room` → the training corpus and future Brenn utterance surface).
So any host that can reach the listener can impersonate a pod — seeding or
consuming a victim's resume entries (laundering or breaking the sample-count
cross-check) or streaming audio attributed to a room it is not in. This trust
model (LAN-trusted, real device auth deferred) predates this diff — it is
inherited from `audio-receiver` — but the host architecture *widens* what
`pod_id` controls, so the assumption needs an explicit decision recorded in the
design/ADR. Options: document LAN-trust as accepted; bind ledger entries to the
originating peer IP as cheap defense-in-depth (a mismatch downgrades to "fresh
segment", the safe direction); or device auth (pre-shared key / TLS on `Hello`)
before the corpus feeds model training. Deferred from the deep-review respond
round (security-1). See `TODO(pod-auth-threat-model)` at the `pod_id` acceptance
site in `feed_await_hello`, `host/crates/pod-ingest/src/session.rs`.

Residuals from `ingest-flood-hardening` (per-pod store quotas landed 2026-07-18):
per-pod byte quotas now bound a single spoofed identity's store footprint, but
because `pod_id` is unauthenticated, three flood residuals belong to this threat
model. (a) A flooder spoofing many *distinct* pod ids gets a full quota per
identity (bounded only by the global cap) and can also consume up to
`max_connections` connection slots (`host/crates/speech-surface/src/server.rs`,
the `Semaphore::new(config.max_connections)` site). (b) Ingest has no real-time
pacing — frames are consumed as fast as they arrive. (c) The shared drop-oldest
segment queue still admits cross-pod eviction *pressure* in principle, though
audio feeds now drop lossily per-connection rather than backing up globally. All
three are consequences of unauthenticated pod identity and close only with real
device auth on `Hello`.

## `pod-identity-trust`

Playback routing (`PlaybackRegistry` + router) is keyed on a self-asserted,
unauthenticated pod identity: any LAN peer can send a `Hello` with an arbitrary
`pod_id`, and the supersede registry hands routing to the latest claimant, so
`SpeakCmd` audio for a pod id is written to whichever socket last claimed it. As of
the HTTP STT/TTS + EchoBrain increment the router carries synthesized,
household-derived audio through exactly this path — the trigger this TODO named.

Disposition (recorded, not resolved): accepted for now. The deployment is a single
household on a trusted LAN behind the operator's own firewall — the same trust
envelope under which raw microphone audio already flows *inbound* unauthenticated,
which is the strictly more sensitive direction. No per-pod token is added now; a
takeover is loud because the supersede event is already JSONL-visible in the log.

The mitigation path when the trust envelope changes (off-LAN streaming, untrusted
peers) is the deferred `audio-auth` (TLS/PSK) candidate plus `pod-auth-threat-model`
(device auth on `Hello`), which together would authenticate the peer and eliminate
the impersonation surface. The slug stays open pending that work. Marked at
`SpeakCmd.target` in `host/crates/speech-pipeline/src/types.rs`.

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

## `wifi-assoc-inflight-flag-generation-race`

`WIFI_ASSOC_IN_FLIGHT` (`firmware/devices/respeaker-pod/src/wifi.rs`) suppresses a
self-inflicted `StaDisconnected` ring while the supervisor is blocked inside its own
`associate_from_active_config()` call, closing the ~17.4s backoff-bypass bug
(`design-delta-1.md`, Hole B). The suppression window is timing-based
(`store(true)`/`store(false)` bracket the *call*), not state-based: the failing
attempt's `StaDisconnected` event is delivered asynchronously on the event-loop task,
and `BlockingWifi::connect()`/`stop()` wait on driver state, not on that callback
having actually run. If the callback runs after `store(false)`, it rings anyway,
bypassing the backoff wait just computed for the next attempt — reintroducing the
fixed bug as a low-probability heisenbug — or, in the reversed edge, wrongly suppresses
a genuine external disconnect landing in the same window (bounded degradation:
recovery waits for the next ~30s tick instead of being prompt). Found in deep review
(`notes-deep-tracer-r1.md`,
`correctness-inflight-flag-clear-vs-async-disconnect`,
`docs/adr/2026/07/19-wifi-temporary-config/`).

Deferred: a proper fix needs state-based suppression, e.g. an attempt-generation
counter stamped by the supervisor at attempt start and checked by the callback, so a
ring is only suppressed for the exact attempt that produced it — a firmware-behavior
design decision (algorithm choice, plus a fresh empirical HIL validation cycle like the
one that found Hole A/B), not a mechanical fix. Reproducing it to confirm a fix
requires a low-probability hardware race, so the residual is a design decision the
project can pick up deliberately rather than a code review action item.

Done = suppression scoped to the exact attempt that produced a given disconnect event,
confirmed by extended real-hardware runs of `BootAssociationRetry` showing no
recurrence of sub-20s attempt-start spacing.

See `TODO(wifi-assoc-inflight-flag-generation-race)` at `WIFI_ASSOC_IN_FLIGHT` in
`firmware/devices/respeaker-pod/src/wifi.rs`.

## `hil-first-attempt-after-boot-ac9`

The first `make hil-test` invocation immediately after a physical power-cycle
reliably fails at the serial `Identify` handshake with `ERROR [AC9]: device present
but not responding with protocol frames` — boot console output accumulates in a
buffer that confuses the fixture on the very first attempt. A second/third
invocation against the *same* boot (no further power cycle) succeeds normally.
Observed and characterized during the RTD heap-floor rebake session
(`docs/adr/2026/07/19-rtd-heap-floor-rebake/run-record.md`): six of six power-cycles
in that session hit it on the first attempt (two on run 1), costing six aborted
`hil-test` invocations. It was investigated in-session and could plausibly be
mistaken for a recurrence of the `dd254e8e`-class serial-corruption bug, which is
the expensive failure mode this TODO exists to prevent — a future operator wasting
time re-diagnosing a known, benign fixture artifact as a regression, or the reverse.

Likely fix: drain and discard any pending serial input before sending the first
`Identify` command, so accumulated boot-console bytes don't desync the frame parser.

Done = the first `make hil-test` invocation after a physical power-cycle succeeds
(no AC9 error) across several power-cycles.

See `TODO(hil-first-attempt-after-boot-ac9)` at the `Identify` command send in
`firmware/crates/hil-host/src/main.rs`.

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
POWERON bake's sample count). If any sample lands below ~71 KB (i.e. realized
headroom against `HEAP_MIN_EVER_FLOOR` drops under the design's 25% target), or
the post-flash population otherwise clusters measurably below the POWERON
population, re-bake `HEAP_MIN_EVER_FLOOR` against `min()` of both populations
combined, not against POWERON alone. Otherwise document why the single low
sample was an outlier.

See `TODO(heap-floor-post-flash-boot-path-offset)` at `HEAP_MIN_EVER_FLOOR` in
`firmware/crates/device-protocol/src/lib.rs`.

## `post-feed-heap-durable-guard`

`heap-gate-measure` (`docs/adr/2026/07/19-heap-gate-measure/`) discharged the pre-deploy
heap gate with a one-time manual measurement: a full-suite `make hil-test` run followed by
a second, separate `RESPEAKER_HIL_ONLY=DeviceHealthCheck make hil-test` invocation on the
same boot. That two-invocation procedure is not part of the permanent `REGISTERED_TESTS`
registry — `DeviceHealthCheck` runs once, at position 4, *before* `FullDuplexRxIntegrity`
(position 24), so no routine suite run ever samples heap after the saturated-playback feed.
A future change that regresses inbound-path allocation (ring geometry, lwIP window, PSRAM
fallback to internal RAM) would drop the post-feed trough toward the floor with the routine
suite still green.

`heap-gate-measure`'s design explicitly scoped this out (design.md §5: "No new automated
tests... this is a one-time gate discharge and not a new durable test"), so closing this gap
means overriding that design decision, not a code-review action item. Candidate fix: register
`DeviceHealthCheck` a second time at a registry position after `FullDuplexRxIntegrity` (or a
dedicated test name dispatching the same handler) so every suite run re-asserts the post-feed
trough.

Deferred: needs a human/design decision on whether a durable regression guard is worth the
extra registered-test slot and per-run time cost, not just an obvious fix.

See `TODO(post-feed-heap-durable-guard)` at `REGISTERED_TESTS` in
`firmware/crates/device-protocol/src/lib.rs`.

