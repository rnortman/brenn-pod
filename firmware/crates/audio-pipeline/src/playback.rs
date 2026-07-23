//! TX-side playback sink and PCM expansion logic (speaker-rx-audio design §6).
//!
//! Pure, host-testable logic relocated out of the device crate (`respeaker-pod`)
//! so it runs under `cargo test --workspace` rather than being only compile-checked
//! by `make check` (the device crate cross-compiles to Xtensa and is not a workspace
//! `default-member`). The items here are the S16_LE-mono → 32-bit-stereo expansion
//! (`expand_sample_to_frame`), the PCM validation guard (`is_valid_s16le_pcm`), the
//! log rate limiter (`LogCountdown`), and the inbound playback sink (`I2sStreamSink`)
//! that expands inbound PCM straight into the single allocate-once SPSC byte ring
//! (`InboundPcmRing`) the capture/DAC thread drains (design §2 — the ring replaces the
//! prior bounded channel-of-`Vec`s).
//!
//! `std`-gated: the sink and the ring use `std::sync::{Arc, Mutex}` and a type-erased owned byte
//! store (`Box<dyn DerefMut<Target = [u8]> + Send>`, a `Box<[u8]>` for host tests and a PSRAM buffer
//! on device). The
//! `core`-only items (`expand_sample_to_frame`, `is_valid_s16le_pcm`, `LogCountdown`,
//! `I2S_TX_FRAME_BYTES`) are co-located here because their only consumers (the device
//! crate, the host sink, the tests) all build with `std`. (If a `no_std` consumer ever
//! needs the `core`-only items, split them into a non-gated submodule then.)
//!
//! The one device-coupled seam — reading the process-lifetime `INBOUND_PCM_PRODUCER` global
//! `static` for the ring's producer half — stays in `main.rs`. The relocated `I2sStreamSink`
//! keeps the producer-injecting `with_producer` as its sole constructor; `main.rs` wraps it
//! with a thin free function that resolves the static (design §6.2, §2.10).

/// Bytes per 32-bit stereo I2S frame (2 slots × 4 bytes).
pub const I2S_TX_FRAME_BYTES: usize = 8;

/// Bytes per raw S16_LE-mono wire sample — the inbound ring's fundamental storage unit.
///
/// The counterpart to [`I2S_TX_FRAME_BYTES`] on the raw side: the ring stores audio at
/// 2 B/sample and the consumer expands each sample to an 8 B I2S frame at DMA-write time
/// ([`expand_run_into`]). Named so the load-bearing raw↔expanded ratio
/// (`I2S_TX_FRAME_BYTES / WIRE_BYTES_PER_SAMPLE = 4×`) is one grep-able constant across the
/// alignment invariant, the expansion, and the drain call site, not a scattered literal `2`.
pub const WIRE_BYTES_PER_SAMPLE: usize = 2;

/// Cadence (in calls/frames) for the periodic playback-sink status log.
///
/// The inbound stream is paced at one frame per 20 ms, so 50 frames is ~1 s
/// between `frames=/samples=/full_stalls=` lines — frequent enough to confirm liveness
/// without flooding the rolling log. Used by the shared [`LogCountdown`] in both
/// `CountingSink::accept` (device crate) and `I2sStreamSink::accept`.
pub const PLAYBACK_LOG_CADENCE_FRAMES: u32 = 50;

/// Expand one mono `i16` PCM sample into a single 32-bit stereo I2S TX frame.
///
/// The tone/PCM content goes in the **left** slot, MSB-aligned in the top 16 bits of
/// the 32-bit slot; the **right** slot is silent. The frame is laid out little-endian
/// as `[0, 0, lo, hi,  0, 0, 0, 0]` — the sample's low byte at `[2]` and high byte at
/// `[3]` — matching the RX left-slot extraction (design §2.3 step 2).
///
/// This is the single source of truth for the S16_LE-mono → 32-bit-stereo expansion,
/// shared by `SineSource::fill_frames` (device crate) and `I2sStreamSink::accept` so
/// the two cannot diverge on the layout.
pub fn expand_sample_to_frame(sample: i16) -> [u8; I2S_TX_FRAME_BYTES] {
    let [lo, hi] = sample.to_le_bytes();
    // Left slot, MSB-aligned in the top 16 bits of the 32-bit slot; right slot silent.
    [0, 0, lo, hi, 0, 0, 0, 0]
}

/// Expand a contiguous run of raw S16_LE-mono wire bytes into 32-bit-stereo I2S TX frames —
/// the consumer's DMA-write-time expansion (design §3.1, "expand at DMA-write time").
///
/// The inbound PCM ring stores raw wire bytes ([`WIRE_BYTES_PER_SAMPLE`] = 2 B/sample); the
/// capture/DAC thread applies this expansion as it copies each drained run out to I2S TX. Each
/// input sample becomes one [`I2S_TX_FRAME_BYTES`]-byte frame via the shared
/// [`expand_sample_to_frame`] layout, so the raw-in-ring storage and the on-wire I2S framing share
/// a single source of truth for the layout and cannot diverge.
///
/// `raw.len()` must be even — a whole number of samples; the ring only ever hands the consumer
/// sample-aligned runs (design §3.1 alignment invariant). `out.len()` must be at least
/// `raw.len() / WIRE_BYTES_PER_SAMPLE × I2S_TX_FRAME_BYTES`; the first `raw.len() /
/// WIRE_BYTES_PER_SAMPLE` frames of `out` are written and any trailing bytes are left untouched.
/// Both preconditions are checked with release-active `assert!` (not `debug_assert!`): a violation
/// on-device would silently drop a byte and byte-shift the S16 framing into full-scale amplifier
/// noise — the exact audible-damage class this pipeline exists to prevent — so an intentional crash
/// is cheaper than corrupt playback (the check is O(1) per ~640 B run, negligible against the
/// per-sample expansion loop). Factored here (host-testable) rather than inline in the device drain
/// loop so the expansion is pinned by the same `cargo test` corpus as [`expand_sample_to_frame`].
pub fn expand_run_into(raw: &[u8], out: &mut [u8]) {
    assert!(
        raw.len().is_multiple_of(WIRE_BYTES_PER_SAMPLE),
        "expand_run_into: raw run ({} B) must be a whole number of S16 samples",
        raw.len()
    );
    assert!(
        out.len() >= raw.len() / WIRE_BYTES_PER_SAMPLE * I2S_TX_FRAME_BYTES,
        "expand_run_into: out ({} B) too small for {} expanded frames",
        out.len(),
        raw.len() / WIRE_BYTES_PER_SAMPLE
    );
    for (sample_bytes, frame) in raw
        .chunks_exact(WIRE_BYTES_PER_SAMPLE)
        .zip(out.chunks_exact_mut(I2S_TX_FRAME_BYTES))
    {
        let sample = i16::from_le_bytes([sample_bytes[0], sample_bytes[1]]);
        frame.copy_from_slice(&expand_sample_to_frame(sample));
    }
}

/// In-place counterpart to [`expand_run_into`]: expand `sample_count` raw S16_LE-mono samples
/// occupying `buf[..sample_count × WIRE_BYTES_PER_SAMPLE]` into the same buffer's
/// `buf[..sample_count × I2S_TX_FRAME_BYTES]` as 32-bit-stereo I2S TX frames, sharing the
/// [`expand_sample_to_frame`] layout.
///
/// Lets the capture drain use a **single** staging buffer — `copy_run_into` deposits a raw run,
/// this expands it in place, and the expanded bytes are written to I2S TX with a NON_BLOCK staging
/// cursor — so no separate raw/expanded buffer pair is needed. The expansion runs high-index → low
/// so each sample is read before the wider frame overwrites its bytes: sample `i` sits at
/// `[i·2, i·2+2)` and frame `i` at `[i·8, i·8+8)`; for any already-written frame `j > i`,
/// `j·8 ≥ (i+1)·8 > i·2+2`, so no not-yet-read sample is clobbered.
///
/// `buf.len()` must be at least `sample_count × I2S_TX_FRAME_BYTES` — checked with a release-active
/// `assert!` for the same reason as [`expand_run_into`]: a short buffer would byte-shift the S16
/// framing into full-scale amplifier noise.
pub fn expand_run_in_place(buf: &mut [u8], sample_count: usize) {
    assert!(
        buf.len() >= sample_count * I2S_TX_FRAME_BYTES,
        "expand_run_in_place: buf ({} B) too small for {} expanded frames",
        buf.len(),
        sample_count
    );
    for i in (0..sample_count).rev() {
        let lo = buf[i * WIRE_BYTES_PER_SAMPLE];
        let hi = buf[i * WIRE_BYTES_PER_SAMPLE + 1];
        let frame = expand_sample_to_frame(i16::from_le_bytes([lo, hi]));
        let base = i * I2S_TX_FRAME_BYTES;
        buf[base..base + I2S_TX_FRAME_BYTES].copy_from_slice(&frame);
    }
}

/// Validate an inbound S16_LE PCM payload: must be a non-zero, even number of bytes.
///
/// Single source of truth for the sink validation guard, shared by `CountingSink`
/// (device crate) and `I2sStreamSink` so the reference validator and the real speaker
/// sink cannot silently diverge on the rule — a malformed frame one sink rejects must
/// not reach TX in the other. Returns `true` if the payload is acceptable; on
/// rejection the caller emits the rate-limited discard warn and returns without
/// forwarding to TX.
pub fn is_valid_s16le_pcm(pcm: &[u8]) -> bool {
    !pcm.is_empty() && pcm.len().is_multiple_of(2)
}

/// Burst-event predicate (§2.3): does a per-window channel-full event delta warrant a
/// timestamped `warn!`?
///
/// Under backpressure (design §2c) the per-window delta counts `full_stalls` (channel-full
/// events the caller back-pressured on), not dropped chunks; the predicate is unchanged.
/// The threshold is `> 0` — **any** event in the rate-limited window fires the burst warn —
/// because a single channel-full stall means the DAC fell behind for ~20 ms, worth a
/// time-correlatable line (design §2.3, resolved decision 3: "log everything, threshold
/// tight"). This is deliberately not a noise-suppressing `> N`: the cadence of the
/// surrounding rate-limited `info!` block already bounds the warn rate to once per window
/// regardless of how many events occurred in it, so the per-event-logging prohibition (the
/// aggregate `info!` carries the running total; do not log per event) is still honored.
///
/// Factored out as a pure predicate so the burst condition is host-unit-testable against the
/// integer delta rather than against captured log output (§5).
pub fn is_drop_burst(window_event_delta: u32) -> bool {
    window_event_delta > 0
}

/// Pre-roll gate decision (design §2.2, §2.6, §4): given the current buffered depth and how long
/// the consumer has been filling, should the gate **clear** and real playback begin?
///
/// Two ways the gate clears (design §2.2):
/// - **Fill reached** — `buffered_chunks >= target`: the target-depth cushion is buffered, so play.
/// - **Fallback timeout** — the target was not reached within `max_wait_ms` of the *first chunk*
///   arriving: clear anyway and play whatever depth exists, so a short/sparse stream that never
///   reaches the target (e.g. a 2-chunk snippet against a 4-chunk target) still makes forward
///   progress and plays in full instead of waiting forever (design §3 "Short stream …").
///
/// `first_chunk_elapsed_ms` is `None` until the first chunk arrives — until then the fallback clock
/// has not started, so an idle connection (server connected but silent) waits in pre-roll with the
/// DAC muted, which is correct: there is nothing to play (design §3 "Pre-roll fill never reached …").
///
/// Factored out as a pure predicate so the gate arithmetic is host-unit-testable against integers
/// rather than against the I2S-owning capture thread, mirroring `is_tx_wedged` /
/// `should_warn_underrun_proxy` / [`is_drop_burst`] (design §4).
pub fn preroll_gate_ready(
    buffered_chunks: usize,
    target: usize,
    first_chunk_elapsed_ms: Option<u64>,
    max_wait_ms: u64,
) -> bool {
    if buffered_chunks >= target {
        return true;
    }
    matches!(first_chunk_elapsed_ms, Some(elapsed) if elapsed >= max_wait_ms)
}

/// Bytes of **raw S16_LE-mono wire PCM** the inbound playback ring buffers between the streamer
/// (producer) and the capture/DAC thread (consumer) — the single, allocate-once SPSC byte ring that
/// replaces the `INBOUND_PCM` channel-of-`Vec`s (design §3.1).
///
/// **Byte/ms reference frame (raw-mono storage, design §3.1).** The ring stores the inbound wire
/// bytes verbatim — S16_LE mono at 16 kHz, 2 bytes/sample — and the expansion to 32-bit-stereo I2S
/// frames ([`I2S_TX_FRAME_BYTES`] = 8 B/sample) is applied by the consumer at DMA-write time
/// ([`expand_run_into`]), so each stored byte is one raw wire byte rather than an expanded I2S frame.
/// So 1 s = 16 000 samples × 2 = 32 000 bytes; **1 ms = 32 bytes**; **20 ms (one wire frame) =
/// 640 bytes**.
///
/// **Chosen capacity: 65 536 bytes = 2 048 ms ≈ 102 wire frames** (design-delta-14 §4). This
/// *restores* the original design §3.1 value that design-delta-1 D1 shrank to 16 384 B / 512 ms —
/// the shrink was purely an internal-heap decision (the heap gate falsified the larger ring on the
/// internal RAM budget), and design-delta-13 moved the ring's backing storage to PSRAM, removing
/// that constraint. With the jitter budget widened (240 ms device preroll + up to 1 000 ms host
/// burst-lead, design-delta-14 §§2–3), the ring must hold the escalated preroll cap plus a full
/// host lead plus one frame: host lead 32 000 B + escalated preroll cap 30 720 B + one max frame
/// 1 280 B = 64 000 ≤ 65 536. Raw-mono storage is the load-bearing lever: the same allocation is
/// 4× deeper than expanded storage and moves per-frame expansion off the streamer thread.
///
/// Heap gate discharged 2026-07-20 (commit `1b866729`): same-boot pre/post-
/// `FullDuplexRxIntegrity` `DeviceHealthCheck` readings clear `device_protocol`'s
/// `HEAP_FREE_FLOOR` and `HEAP_MIN_EVER_FLOOR` (post-feed `min_heap=77112`). Full report
/// lines, margins, and the cross-check against the `rtd-heap-floor-rebake` population live
/// in `docs/adr/2026/07/19-heap-gate-measure/implementation-log.md`, the record of
/// authority — not duplicated here to avoid re-creating the drift this item cleaned up.
///
/// **Single named tunable.** One knob; with the [`Mutex`](std::sync::Mutex)-guarded plain-`u32`
/// indices the wrap math runs entirely under the lock with ordinary `%`/`wrapping_sub`, so there is
/// no `2^32 % cap == 0` requirement (the plain-integer wrap-math is documented on
/// `InboundRingProducer`) — only a whole-*sample* ([`WIRE_BYTES_PER_SAMPLE`]) multiple, so a wrap
/// split never lands mid-sample (design §3.1 alignment invariant, asserted below). 65 536 is even;
/// it need not be a whole number of the 640 B write unit.
pub const INBOUND_PCM_RING_BYTES: usize = 65_536;

/// Default playback burst lead in **milliseconds**: how far ahead of real time the host pacer
/// may run when bursting buffered PCM before dropping to real-time cadence. The single shared
/// source of truth for `speech_pipeline::PacerConfig::default().lead_ms`, the surface config
/// guard's default, and the HIL host pacer — a retune here moves all three atomically instead
/// of leaving hand-mirrored copies to drift.
///
/// Ring relationship: at the raw-mono wire rate (`SAMPLE_RATE_HZ / 1_000 × WIRE_BYTES_PER_SAMPLE`
/// = 16 000/1 000 × 2 = 32 B/ms), a full lead of buffered PCM is `lead_ms × 32` bytes and must
/// fit within [`INBOUND_PCM_RING_BYTES`] alongside the escalated preroll cap and one max frame.
/// The `const _` below fails the build if a retune ever violates that combined ring bound.
/// User-configured leads (≠ this default) are bounded at runtime by `PlaybackConfig::validate`.
pub const PLAYBACK_BURST_LEAD_MS: u64 = 1_000;

/// Compile-time guard: a full default burst lead of buffered raw PCM must fit in the inbound ring
/// *alongside* the escalated preroll cap and one max frame — the same combined steady-state bound
/// the ring is sized for ([`INBOUND_PCM_RING_BYTES`] doc). `lead_bytes + preroll_cap + max_frame ≤
/// ring`, i.e. `PLAYBACK_BURST_LEAD_MS × (samples/ms) × bytes/sample + 30 720 + 1 280 ≤ 65 536`.
/// A `lead ≤ ring` check alone would pass retunes (e.g. 2 000 ms) that overflow the ring's
/// documented invariant, so the guard binds the full sum.
const _: () = assert!(
    PLAYBACK_BURST_LEAD_MS as usize
        * (crate::ring::SAMPLE_RATE_HZ as usize / 1_000)
        * WIRE_BYTES_PER_SAMPLE
        + PLAYBACK_PREROLL_MAX_TARGET_BYTES
        + crate::wire::MAX_AUDIO_PAYLOAD
        <= INBOUND_PCM_RING_BYTES,
    "default playback burst lead + escalated preroll cap + one max frame must fit in the inbound PCM ring"
);

/// Pre-roll / minimum-fill target in **bytes** of buffered *raw* inbound PCM before the capture-thread
/// drain loop begins playing a fresh stream (the pcm-ring ADR's preroll cushion, re-based to raw
/// units in design §3.1) — 240 ms at the raw-mono rate (`12 × 640 = 7 680 bytes = 240 ms`).
///
/// With the ring as the hold buffer, preroll is a fill-level check (`consumer.available() >= target`,
/// both raw bytes) rather than a chunk count. Design-delta-14 §2 raises this from the original 80 ms
/// (`4 × 640 = 2 560 B`): warm-12/13 (build 804f38a) underran Scenario B at weak RSSI where the
/// host-observed inbound read-gap tail reached 86–127 ms and the user-stated delivery-gap tail on
/// this link is ≈ 100–400 ms — 80 ms was far too small a cold-start cushion. 240 ms ≈ 2× the worst
/// *measured* per-gap value and is the user-directed default; the deep tail (~400 ms) is covered by
/// the escalation ladder plus the host burst-lead (design-delta-14 §3), not by this base target.
pub const PLAYBACK_PREROLL_TARGET_BYTES: usize = 12 * 640;

/// Escalating-preroll ceiling in **bytes** of buffered raw inbound PCM (design §3.3, re-derived by
/// design-delta-14 §2 at the restored 65 536 ring): the largest fill target the adaptive re-arm
/// ([`next_preroll_target`]) will ever demand.
///
/// After a mid-stream underrun the drain loop re-arms preroll and *doubles* the target on each
/// successive underrun within one ring generation. Escalation keeps its existing shape — exactly two
/// doublings from the 240 ms base: 7 680 → 15 360 → 30 720 and caps here. 30 720 B = 960 ms raw
/// S16 mono — the "without hoarding half the ring" bound at the restored ring (`30 720 < 65 536/2`).
///
/// Reachable-regime note: the escalated target is only *filled* (vs. cleared by the
/// `PLAYBACK_PREROLL_MAX_WAIT_MS` = 500 ms fallback) when recovery delivery outpaces real-time. With
/// the host holding up to a 1 000 ms burst-lead (design-delta-14 §3), post-underrun recovery is
/// typically a burst at network rate, so the escalated targets are actually reachable in their
/// intended regime rather than fallback-capped at ~250 ms of lead as before. Under sustained
/// sub-real-time delivery the fallback still preempts the target — that regime is the OQ2 signal to
/// reopen the depth question (which design-delta-14 does), not something a larger target absorbs.
///
/// The cap is load-bearing for the §3.1 reachability invariant: the ring must hold the *maximum*
/// preroll target plus one max frame, and `30 720 + 1 280 ≤ 65 536` (design-delta-14 §2). The
/// reset-to-base on a generation change (reconnect) lives in the capture-thread drain loop.
pub const PLAYBACK_PREROLL_MAX_TARGET_BYTES: usize = 30_720;

/// Adaptive-preroll escalation step (design §3.3): given the current preroll fill target, the target to
/// demand after the *next* underrun within the same ring generation — the current target doubled,
/// clamped to [`PLAYBACK_PREROLL_MAX_TARGET_BYTES`].
///
/// From the 7 680 B base this converges 7 680 → 15 360 → 30 720 (cap) in two doublings and is
/// idempotent at the cap. `saturating_mul` guards the doubling against overflow (unreachable at these magnitudes,
/// but keeps the helper total). Reset back to the [`PLAYBACK_PREROLL_TARGET_BYTES`] base happens on a
/// ring generation change (reconnect), not here — this helper only escalates.
///
/// Factored out as a pure function so the doubling/cap arithmetic is host-unit-testable against
/// integers rather than against the I2S-owning capture thread, mirroring [`preroll_gate_ready`] /
/// [`is_drop_burst`] (design §3.3, §5).
pub fn next_preroll_target(current: usize) -> usize {
    current
        .saturating_mul(2)
        .min(PLAYBACK_PREROLL_MAX_TARGET_BYTES)
}

/// One drain write-unit / wire frame in **raw** bytes: 320 samples × 2 B = 640 (20 ms at 16 kHz)
/// (design §3.1). The consumer drains the ring in runs capped at
/// this size; each raw run is then expanded to `640 / 2 × I2S_TX_FRAME_BYTES = 2 560` I2S-frame bytes
/// at DMA-write time ([`expand_run_into`]), so each `write_all` keeps the calibrated ~20 ms meaning the
/// playback observability depends on (design §2.5).
pub const INBOUND_PCM_WRITE_UNIT_BYTES: usize = 640;

/// Consumer-stall watchdog threshold (design §6.2(b)): the number of *consecutive* ring-full
/// backpressure stalls (`accept` returning [`Accepted::Full`]) during which the consumer's `tail` has
/// not advanced before the sink warns that the consumer (capture/DAC thread) appears wedged rather
/// than merely behind.
///
/// A ring has no `Disconnected` state — a vanished consumer manifests as a *full ring with a frozen
/// `tail`* rather than as data loss (design §6.2). The channel design valued the "DAC fell behind
/// (healthy backpressure)" vs. "capture thread died (wedged)" distinction; this threshold restores it
/// as a `tail`-stall watchdog. Because `accept` runs on the streamer thread at the inbound frame
/// cadence (~20 ms/frame under streaming load), `PLAYBACK_LOG_CADENCE_FRAMES` stalls is ≈1 s of a full
/// ring with no consumer progress — long enough that a transient DAC hiccup (which advances `tail`
/// within a frame or two) never trips it, but a truly wedged consumer is surfaced within ~a second.
/// The warn is one-shot (edge-triggered): it fires once on crossing the threshold and re-arms only
/// after `tail` advances again (the watchdog is rate-limited by the edge, not re-emitted every stall).
const PLAYBACK_CONSUMER_STALL_WARN_STALLS: u32 = PLAYBACK_LOG_CADENCE_FRAMES;

/// Reachability invariant (re-based to raw units in design §3.1): the ring must hold at least the
/// preroll fill target plus one maximum frame, so the preroll gate is reachable and a single max frame
/// never wedges the stream on permanent `Full`. With raw-mono storage the largest frame `accept` can
/// see is `MAX_AUDIO_PAYLOAD` = 1280 **raw** bytes (the decoder rejects anything larger before `accept`
/// via `wire.rs` `DecodeError::OversizePcm`), since the ring stores raw wire bytes. The target checked
/// here is the **maximum** escalating preroll target `PLAYBACK_PREROLL_MAX_TARGET_BYTES` (design §3.1
/// requires the invariant to bind on the max once the §3.3 adaptive re-arm lands; design-delta-14 §2
/// sets the cap to 30 720 so `30 720 + 1 280 ≤ 65 536` holds at the restored ring). The base
/// `PLAYBACK_PREROLL_TARGET_BYTES` is trivially covered as it is ≤ the max.
const _: () = assert!(
    INBOUND_PCM_RING_BYTES >= PLAYBACK_PREROLL_MAX_TARGET_BYTES + crate::wire::MAX_AUDIO_PAYLOAD,
    "ring must hold the max escalating preroll target plus one max frame (design §3.1 reachability invariant, §3.3 max)"
);

/// Alignment invariant (design §3.1): `accept` writes and the consumer drains in whole *samples*
/// ([`WIRE_BYTES_PER_SAMPLE`]), and the ring cap must be a multiple of that so a wrap split never
/// lands mid-sample. 65 536 is even; host-test caps are likewise even. There is no whole-I2S-frame
/// (8 B) requirement, because the expansion to I2S frames happens in the consumer (`expand_run_into`),
/// not in the ring.
const _: () = assert!(
    INBOUND_PCM_RING_BYTES.is_multiple_of(WIRE_BYTES_PER_SAMPLE),
    "ring cap must be a whole number of S16 samples (design §3.1 alignment invariant)"
);

/// Fixed capacity of the ring's end-of-audio mark FIFO (design §3.4, edge case D).
///
/// Each mark is a wrapping `u32` byte position (like `head`/`tail`) recording where an
/// `EndOfAudio`/`FlushPlayback` boundary rides the buffered audio, so the capture thread's mute
/// decision fires when the *banked tail finishes playing* (`tail` reaches the mark), not when the
/// control frame arrives. Four is enough for the normal stream-end cadence (one boundary per
/// utterance, drained within a frame or two); overflow — more than four un-consumed boundaries
/// banked at once (reachable in pattern mode when sub-second tone+silence cycles burst-deliver
/// across a network stall) — drops the oldest mark with a warn (design §4 edge case D): the
/// consequence is one silence gap played unmuted (amp idle hiss), never lost or reordered audio.
const RING_EOA_MARK_CAP: usize = 4;

/// One pending end-of-audio boundary in [`RingState`]'s mark FIFO (design §3.4).
///
/// `pos` is the wrapping `u32` byte position (the `head` in effect when the producer received an
/// `EndOfAudio`/`FlushPlayback`) the boundary rides — the consumer's `tail` reaching it is the mute
/// cue. `generation` is the ring generation the mark was pushed in, so `apply_reset` can discard
/// boundaries left by a *superseded* connection while keeping one a flush pushed in the new
/// generation at the same position (design §3.4 / §3.5 / edge case E — position alone is ambiguous
/// there, since a dropped connection's final mark sits at exactly `head_at_reset`, the same place a
/// flush's `reset()` + `mark_end_of_audio()` lands its live mark).
#[derive(Clone, Copy)]
struct EndOfAudioMark {
    pos: u32,
    generation: u32,
}

/// Mutable inner state of [`InboundPcmRing`], guarded as a unit by the ring's one
/// [`Mutex`](std::sync::Mutex) (design §2.1, §2.6).
///
/// `head`/`tail` are plain wrapping `u32` byte counters — `head` = total bytes ever written by the
/// producer (mod `2^32`), `tail` = total bytes ever consumed. Physical ring positions are `head %
/// cap` / `tail % cap`; using wrapping *counters* (not wrapped positions) makes full-vs-empty
/// unambiguous without a spare slot via `wrapping_sub` deltas (`used = head.wrapping_sub(tail)`).
/// Because every read and advance of these counters happens under the same lock, the wrap math is
/// serialized within one critical section and never spans a thread boundary mid-update — so there is
/// no `2^32 % cap == 0` requirement (design §2.6). This mirrors `CaptureRing`'s plain-integer
/// `write_head` advanced under the `CAPTURE_RING` `Mutex`, except it is a wrapping `u32` with a paired
/// `tail` (a *consumed* ring) rather than a monotonic `u64` with no tail (a *history* ring).
///
/// `generation`/`head_at_reset` carry the reconnection-boundary handshake (design §2.8): on `reset()`
/// the producer bumps `generation` and records the current `head` into `head_at_reset`, both under
/// this lock; the consumer observes the change on its next pass and jumps `tail = head_at_reset` to
/// drop the dead connection's stale tail race-free. The `Mutex` supplies the ordering the lock-free
/// arm would have needed `Release`/`Acquire` for — no atomics.
///
/// `buf` is the allocate-once backing storage, kept **inside** the same `Mutex` as the indices so the
/// consumer copies its readable run out under the lock (into the caller's `tx_buf`) with no
/// `UnsafeCell`/raw-pointer buffer split and no cross-thread aliasing to reason about (design §2.5).
struct RingState {
    /// Total bytes ever written by the producer, mod `2^32` (sole writer: the producer).
    head: u32,
    /// Total bytes ever consumed, mod `2^32` (sole writer: the consumer).
    tail: u32,
    /// Reconnection-boundary counter, bumped by the producer under the lock on each `reset()`.
    generation: u32,
    /// The `head` value captured at the most recent `reset()`; the consumer jumps `tail` here on
    /// observing a `generation` change, dropping the dead connection's stale tail (design §2.8).
    head_at_reset: u32,
    /// Allocate-once backing buffer (length = `INBOUND_PCM_RING_BYTES`), guarded with the indices.
    ///
    /// Type-erased owned storage so the shared crate stays ignorant of *where* the bytes live: host
    /// tests and any non-PSRAM user pass a `Box<[u8]>` (via [`InboundPcmRing::new`]); the device
    /// passes a PSRAM-backed buffer (via [`InboundPcmRing::with_storage`], design-delta-14 §4). Both
    /// deref to `[u8]`, so the producer fill and consumer copy index it unchanged.
    buf: Box<dyn std::ops::DerefMut<Target = [u8]> + Send>,
    /// End-of-audio mark FIFO (design §3.4): the pending boundaries in arrival order (oldest at the
    /// front), each an [`EndOfAudioMark`] recording the `head` position and `generation` in effect
    /// when the producer received an `EndOfAudio`/`FlushPlayback`. The front mark is the next one the
    /// consumer's `tail` will reach — [`copy_run_into`](InboundRingConsumer::copy_run_into) caps a run
    /// to end exactly on its `pos` and reports `reached_end_of_audio`, and
    /// [`advance`](InboundRingConsumer::advance) /
    /// [`take_mark_at_tail`](InboundRingConsumer::take_mark_at_tail) pop it once `tail` arrives. A
    /// fixed-capacity `heapless::Deque` (not a growable collection) stores its elements inline, so the
    /// whole ring state stays allocation-free after boot, matching the allocate-once `buf`.
    /// `apply_reset` discards marks left by a superseded generation.
    marks: heapless::Deque<EndOfAudioMark, RING_EOA_MARK_CAP>,
    /// One-shot latch throttling the mark-FIFO-overflow `warn!` (security-1): set when an overflow
    /// warn is emitted, cleared the next time a mark is popped (FIFO drains below cap). Without it,
    /// every post-overflow `EndOfAudio`/`FlushPlayback` frame — the cheapest 3-byte frame in the
    /// protocol, unauthenticated post-Hello — emits a multi-line warn, so a malicious/compromised
    /// or on-path-injecting peer (or a buggy host in pattern mode across a network stall, design §4
    /// edge case D) could flood the device log, burn CPU on formatting, and contend the ring lock
    /// with the DAC drain thread. The latch bounds it to one warn per overflow episode.
    mark_overflow_warned: bool,
}

impl RingState {
    /// Push an end-of-audio mark at the producer's current `head`, tagged with the current
    /// `generation`.
    ///
    /// Returns `true` on a normal push, or `false` when the FIFO was already full and the oldest
    /// mark had to be dropped to make room (design §4 edge case D — the caller emits the warn). On
    /// overflow the front (oldest) mark is dropped and the new mark is appended at the back, so
    /// ordering is preserved and only the single oldest boundary is lost.
    fn push_mark(&mut self) -> bool {
        let mark = EndOfAudioMark {
            pos: self.head,
            generation: self.generation,
        };
        if self.marks.push_back(mark).is_ok() {
            true
        } else {
            self.marks.pop_front();
            self.marks
                .push_back(mark)
                .unwrap_or_else(|_| unreachable!("just freed a slot"));
            false
        }
    }

    /// The oldest pending mark's byte position (the front `pos`), or `None` when the FIFO is empty —
    /// the next boundary `tail` will reach.
    fn peek_mark_pos(&self) -> Option<u32> {
        self.marks.front().map(|m| m.pos)
    }

    /// Pop the oldest mark iff its `pos` equals `pos`, returning whether one was popped. Used by both
    /// the `tail`-reached pop in [`advance`](InboundRingConsumer::advance) and the empty-ring
    /// [`take_mark_at_tail`](InboundRingConsumer::take_mark_at_tail).
    fn pop_mark_if_at(&mut self, pos: u32) -> bool {
        if self.marks.front().is_some_and(|m| m.pos == pos) {
            self.marks.pop_front();
            // The FIFO drained below cap — re-arm the overflow warn latch (security-1) so the next
            // genuine overflow episode is reported.
            self.mark_overflow_warned = false;
            true
        } else {
            false
        }
    }

    /// Discard marks left by a superseded generation — the dead connection's boundaries after a
    /// reconnection reset (design §3.4, edge case E). Called from
    /// [`apply_reset`](InboundRingConsumer::apply_reset), where `self.generation` is already the new
    /// (post-`reset()`) value: a mark from an earlier generation belongs to the dropped tail and is
    /// discarded, while a mark pushed in the current generation — the flush `reset()` +
    /// `mark_end_of_audio()` boundary (§3.5), which lands at the same `head_at_reset` position a
    /// dropped connection's final mark would — is live and kept. Tagging by generation is what makes
    /// those two position-identical cases distinguishable.
    fn discard_dead_generation_marks(&mut self) {
        let live_gen = self.generation;
        for _ in 0..self.marks.len() {
            let Some(m) = self.marks.pop_front() else {
                break;
            };
            if m.generation == live_gen {
                // Survivors rotate to the back in order; after len() iterations the deque holds
                // exactly the live-generation marks, order preserved. push_back cannot fail — we
                // popped before each push, so occupancy never exceeds the starting count.
                let _ = self.marks.push_back(m);
            }
        }
    }
}

/// One SPSC byte ring shared by the streamer (producer) and capture/DAC thread (consumer),
/// realized with **one [`Mutex`](std::sync::Mutex) guarding the indices *and* the allocate-once
/// backing buffer together** — the directive's chosen "Mutex ring" shape, reusing `CaptureRing`'s
/// allocate-once + plain-integer-index-under-lock idiom (design §2.1, §2.6).
///
/// There are no atomics, no Acquire/Release pairing, and no cross-thread wrap-ordering concern: the
/// lock supplies all the ordering. The two ends are split into an [`InboundRingProducer`] (sole
/// `head` writer) and an [`InboundRingConsumer`] (sole `tail` writer), each holding an `Arc` of this
/// ring (see [`InboundPcmRing::split`]).
pub struct InboundPcmRing {
    cap: usize,
    state: std::sync::Mutex<RingState>,
}

impl InboundPcmRing {
    /// Allocate the ring once with `cap` bytes of zeroed backing storage and the indices at the
    /// boot/fresh-stream state (`head == tail == 0`, `generation == 0`). `cap` is normally
    /// [`INBOUND_PCM_RING_BYTES`]; a smaller `cap` is used by host tests for cheap wrap arithmetic.
    ///
    /// # Panics
    /// Delegates to [`with_storage`](Self::with_storage), so it panics on the same
    /// conditions: `cap == 0` (a zero-length ring can never hold a frame) or `cap` not a
    /// whole number of S16 samples. The `vec![0u8; cap]` storage vacuously satisfies the
    /// debug-only zeroed-storage diagnostic.
    pub fn new(cap: usize) -> Self {
        // Allocate-once: the `vec![0u8; cap].into_boxed_slice()` posture `CaptureRing` uses
        // (`main.rs` `vec![0i16; RING_CAPACITY_SAMPLES].into_boxed_slice()`).
        Self::with_storage(Box::new(vec![0u8; cap].into_boxed_slice()))
    }

    /// Allocate the ring around caller-owned, type-erased backing `storage` — the seam the device
    /// uses to place the ring's bytes in PSRAM (design-delta-14 §4) while the shared crate keeps no
    /// PSRAM knowledge. `storage` derefs to a byte slice whose length is the ring capacity;
    /// `new(cap)` is the plain-`Box<[u8]>` case that host tests and non-PSRAM users take.
    ///
    /// Reads are bounded to `[tail, head)`: the producer publishes `head` only after the bytes are
    /// written, and resets move `tail` only to a previously-written position, so no unwritten
    /// storage byte is ever observed. Zeroed storage is therefore **not** required for correctness;
    /// callers may pass recycled (non-zero) storage in release builds. A `debug_assert!` still
    /// checks for all-zero storage, purely for deterministic dev-time diagnostics — any byte that
    /// comes out of the ring in a debug/test build is then provably a byte a producer wrote.
    ///
    /// # Panics
    /// Panics if the storage length is 0 (a zero-length ring can never hold a frame) or is not a
    /// whole number of S16 samples ([`WIRE_BYTES_PER_SAMPLE`] — the alignment invariant a wrap split
    /// relies on). In debug builds it additionally panics on non-zeroed storage (dev-diagnostic
    /// only, per the note above); release builds accept any storage.
    pub fn with_storage(storage: Box<dyn std::ops::DerefMut<Target = [u8]> + Send>) -> Self {
        let cap = storage.len();
        assert!(cap > 0, "inbound PCM ring capacity must be > 0");
        assert!(
            cap.is_multiple_of(WIRE_BYTES_PER_SAMPLE),
            "inbound PCM ring storage must be a whole number of S16 samples (design §3.1 alignment invariant)"
        );
        debug_assert!(
            storage.iter().all(|&b| b == 0),
            "inbound PCM ring storage expected zeroed at construction (dev-diagnostic only; \
             reads are bounded to [tail, head), so zeroing is not required for correctness)"
        );
        InboundPcmRing {
            cap,
            state: std::sync::Mutex::new(RingState {
                head: 0,
                tail: 0,
                generation: 0,
                head_at_reset: 0,
                buf: storage,
                marks: heapless::Deque::new(),
                mark_overflow_warned: false,
            }),
        }
    }

    /// Ring capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Split the ring into its producer and consumer handles, each owning an `Arc` of the ring.
    ///
    /// Construct-once at boot: the producer goes to the streamer-thread sink, the consumer to the
    /// capture thread. The split enforces the SPSC roles at the type level — only the producer can
    /// `write`/`reset` (advance `head`/`generation`), only the consumer can `copy_run_into`/`advance`
    /// (advance `tail`) — even though the underlying `Mutex` would be sound regardless of caller.
    pub fn split(self) -> (InboundRingProducer, InboundRingConsumer) {
        let ring = std::sync::Arc::new(self);
        (
            InboundRingProducer { ring: ring.clone() },
            InboundRingConsumer { ring },
        )
    }
}

/// The write end of an [`InboundPcmRing`] — the **sole role that advances `head`**
/// (and `generation`/`head_at_reset`).
///
/// Constructed via [`InboundPcmRing::split`]. Held by `I2sStreamSink` in place of the
/// channel `SyncSender` the sink owned before the ring rewire.
///
/// `Clone` (design OQ §6.3 option (a)): a clone is just another `Arc` to the same ring, and
/// **every write is serialized by the ring `Mutex`**, so a second producer handle is simply a
/// second serialized writer — never a concurrent unsynchronized `head` mutation. This lets the
/// HIL handlers (`run_capture_periodic_line`, `run_playback_drain_rate`) inject into the *same*
/// production ring as the live streamer (keeping the production capture thread fed) instead of
/// each standing up an isolated test-only ring. The "single producer" of the SPSC discipline is
/// a *logical* role enforced by the `Mutex`-serialized `head` advance, not a uniqueness
/// constraint on the handle: cloning the handle does not violate it because the lock orders the
/// writes regardless of how many handles exist (design §2.6/§2.7).
#[derive(Clone)]
pub struct InboundRingProducer {
    ring: std::sync::Arc<InboundPcmRing>,
}

impl InboundRingProducer {
    /// Free space in bytes (room the producer may write before hitting `Full`). A brief
    /// lock-read-unlock; coherent because only the consumer's `tail` advance grows free space and
    /// only this producer's `head` advance shrinks it (design §2.3).
    pub fn free_total(&self) -> usize {
        let st = self
            .ring
            .state
            .lock()
            .expect("inbound PCM ring mutex poisoned");
        self.ring.cap - st.head.wrapping_sub(st.tail) as usize
    }

    /// Total bytes the consumer has consumed so far (the `tail` counter, mod `2^32`). A brief
    /// lock-read-unlock. The producer cannot observe the consumer's liveness through `free_total`
    /// alone (a full ring and a stalled-but-not-dead consumer look identical there); this exposes
    /// the *advancing* quantity so the sink's consumer-stall watchdog can tell "the DAC is draining,
    /// just slowly" (`tail` still climbing) from "the consumer has wedged" (`tail` frozen while the
    /// ring stays full) — the §6.2(b) liveness signal that replaces the channel's `Disconnected`
    /// distinction (design §6.2).
    pub fn consumed(&self) -> u32 {
        self.ring
            .state
            .lock()
            .expect("inbound PCM ring mutex poisoned")
            .tail
    }

    /// Try to write `need` expanded bytes into the ring, filling them via `fill` (design §2.3, §2.6).
    ///
    /// Locks the ring, computes `free = cap - head.wrapping_sub(tail)`, and:
    /// - if `free < need` → returns `false` having written **nothing** and left `head` unchanged
    ///   (the caller's frame stays buffered upstream; this is the `Accepted::Full` backpressure
    ///   cause). The room check and the copy share one critical section, so they cannot disagree.
    /// - otherwise → invokes `fill(dst)` to produce the expanded bytes, where `dst` is the contiguous
    ///   region the caller writes into. Because the free region may wrap the `cap` boundary, the
    ///   write is split into at most two runs: `fill` is called once per run with the run's slice, the
    ///   caller writing the corresponding sub-range of its source. `head` is advanced by `need` under
    ///   the same lock, so a consumer that later locks and reads `head` always sees fully-written
    ///   bytes. Returns `true`.
    ///
    /// `fill(offset, dst)` receives the byte `offset` into the logical frame at which `dst` begins
    /// (0 for the head run, `cap - (head % cap)` for the wrapped remainder) so the caller can copy the
    /// matching slice of its source. This mirrors `accept`'s `chunks_exact`/`expand_sample_to_frame`
    /// loop writing straight into the ring's free region instead of a freshly-allocated `Vec`.
    pub fn write(&self, need: usize, mut fill: impl FnMut(usize, &mut [u8])) -> bool {
        debug_assert!(
            need <= self.ring.cap,
            "frame ({need} B) exceeds ring capacity ({} B) — decoder must reject oversize PCM \
             before accept (design §3)",
            self.ring.cap
        );
        let mut st = self
            .ring
            .state
            .lock()
            .expect("inbound PCM ring mutex poisoned");
        let cap = self.ring.cap;
        let used = st.head.wrapping_sub(st.tail) as usize;
        let free = cap - used;
        if free < need {
            return false;
        }
        // Split the copy at the wrap boundary: head run into `[start..min(start+need, cap)]`,
        // remainder (if any) into `[0..rest]`. Both copies + the `head` advance are in this one
        // critical section, so the consumer never observes a partially-written frame (design §2.7).
        let start = (st.head as usize) % cap;
        let first = need.min(cap - start);
        fill(0, &mut st.buf[start..start + first]);
        if first < need {
            let rest = need - first;
            fill(first, &mut st.buf[0..rest]);
        }
        st.head = st.head.wrapping_add(need as u32);
        true
    }

    /// Mark a reconnection boundary (design §2.8): under the lock, bump `generation` and record the
    /// current `head` into `head_at_reset`. The consumer observes the change on its next drain pass
    /// and jumps `tail = head_at_reset`, dropping the dead connection's un-played tail race-free and
    /// re-arming preroll. Because the producer is the sole `head` writer, a reset it performs is
    /// ordered after all prior writes and before all subsequent writes by single-writer construction
    /// — the FIFO ordering `StreamReset` provided, without a separate ordered marker.
    pub fn reset(&self) {
        let mut st = self
            .ring
            .state
            .lock()
            .expect("inbound PCM ring mutex poisoned");
        st.generation = st.generation.wrapping_add(1);
        st.head_at_reset = st.head;
    }

    /// Push an end-of-audio boundary onto the ring at the current `head` (design §3.4): the mute
    /// decision must fire when the *banked tail finishes playing*, not when the control frame
    /// arrives, so the boundary rides the buffered audio as a mark at the head-of-write position.
    /// The consumer reaches it as `tail` climbs to `head` and arms the mute then
    /// ([`copy_run_into`](InboundRingConsumer::copy_run_into) /
    /// [`advance`](InboundRingConsumer::advance) /
    /// [`take_mark_at_tail`](InboundRingConsumer::take_mark_at_tail)).
    ///
    /// Called by `I2sStreamSink::end_of_audio`, and — after a `reset()` — on
    /// `flush_playback` (§3.5, the mark lands at the emptied ring's head so an immediate empty-ring
    /// poll observes it via `take_mark_at_tail`). On mark-FIFO overflow the oldest boundary is
    /// dropped with a warn (design §4 edge case D): audio is never lost or reordered, only one mute
    /// boundary is skipped (a silence gap played unmuted — amp idle hiss).
    pub fn mark_end_of_audio(&self) {
        // Set the latch and decide whether to warn under the lock, then release it before the
        // (rare, throttled) `warn!` so log formatting never holds the ring lock the DAC drain
        // thread shares (security-1). The warn fires at most once per overflow episode — the latch
        // re-arms in `pop_mark_if_at` when the FIFO drains below cap.
        let should_warn = {
            let mut st = self
                .ring
                .state
                .lock()
                .expect("inbound PCM ring mutex poisoned");
            // Warn only on a *fresh* overflow: the FIFO overflowed (push_mark returned false) and
            // the latch is not already set. Setting the latch here suppresses the flood until a
            // pop re-arms it in `pop_mark_if_at`.
            let overflowed = !st.push_mark();
            if overflowed && !st.mark_overflow_warned {
                st.mark_overflow_warned = true;
                true
            } else {
                false
            }
        };
        if should_warn {
            log::warn!(
                "inbound PCM ring: end-of-audio mark FIFO full ({RING_EOA_MARK_CAP} pending) — \
                 dropped oldest boundary; one mute boundary skipped (design §4 edge case D). \
                 Audio is not lost; a silence gap will play unmuted (amp idle hiss). \
                 Further overflows are suppressed until the FIFO drains (security-1)."
            );
        }
    }
}

/// One contiguous run the consumer copied out of the ring, plus the ring `generation` observed when
/// it was taken — returned by [`InboundRingConsumer::copy_run_into`] so the caller can apply a
/// pending reconnection reset before draining (design §2.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainRun {
    /// Bytes copied into the caller's `tx_buf` (`0` when the ring is empty / fully held in preroll).
    pub n: usize,
    /// The ring `generation` at copy time; the caller compares this against the last generation it
    /// acted on to detect a reconnection boundary it must apply (jump `tail`, re-arm preroll).
    pub generation: u32,
    /// `true` when this run was capped to end **exactly** on a pending end-of-audio mark (design
    /// §3.4): the banked tail up to a host `EndOfAudio`/`FlushPlayback` boundary has now been copied
    /// out, so the capture thread's drain loop arms the delayed mute. The mark itself is popped by
    /// the matching [`advance`](InboundRingConsumer::advance) once `tail` reaches it, so each
    /// boundary reports exactly once. A run that merely happens to be short (budget/wrap/`tx_buf`
    /// caps) without hitting a mark reports `false`. Marks that sit on an *empty* ring
    /// (`head == tail`) are never reported here — a zero-length run carries no boundary — and are
    /// instead observed via [`take_mark_at_tail`](InboundRingConsumer::take_mark_at_tail).
    pub reached_end_of_audio: bool,
}

/// The capture-thread read end of an [`InboundPcmRing`] — the **sole writer of `tail`**.
///
/// Constructed via [`InboundPcmRing::split`]. Held by the capture thread in place of the
/// channel `Receiver` it owned before the ring replaced the channel.
pub struct InboundRingConsumer {
    ring: std::sync::Arc<InboundPcmRing>,
}

impl InboundRingConsumer {
    /// Bytes the consumer may currently read (`head.wrapping_sub(tail)`). A brief lock-read-unlock;
    /// used by the preroll fill-level check (`available() >= PLAYBACK_PREROLL_TARGET_BYTES`, §2.9)
    /// and the empty-ring underrun-proxy poll. Coherent for the same reason as
    /// [`InboundRingProducer::free_total`].
    pub fn available(&self) -> usize {
        let st = self
            .ring
            .state
            .lock()
            .expect("inbound PCM ring mutex poisoned");
        st.head.wrapping_sub(st.tail) as usize
    }

    /// The current reconnection-boundary counter (`generation`). The caller compares this against the
    /// last value it acted on; on a change it applies the pending reset (jump `tail` to `head_at_reset`
    /// via [`apply_reset`](Self::apply_reset), re-arm preroll). A brief lock-read-unlock.
    pub fn generation(&self) -> u32 {
        self.ring
            .state
            .lock()
            .expect("inbound PCM ring mutex poisoned")
            .generation
    }

    /// Copy the next contiguous readable run into the caller's reused `tx_buf` **under the lock**,
    /// returning the bytes copied and the `generation` observed (design §2.5, §2.6).
    ///
    /// The run length is `min(max_run, available, cap - (tail % cap), tx_buf.len())` — capped at the
    /// per-pass budget the caller passes as `max_run` (`INBOUND_DRAIN_BYTES_PER_PASS`), the bytes
    /// available, the distance to the wrap boundary (so the run is contiguous), and the caller's
    /// staging-buffer length (normally one [`INBOUND_PCM_WRITE_UNIT_BYTES`] write-unit). The bytes are
    /// `copy_from_slice`d from the ring buffer into `tx_buf[..n]` while the lock is held, then the
    /// lock is released; the caller issues `write_all(&tx_buf[..n])` with the lock **already released**
    /// (never across the blocking DMA write), and re-locks only to [`advance`](Self::advance) `tail`
    /// for the bytes actually written. So the ring buffer is never touched outside the lock and there
    /// is no cross-thread aliasing — zero `unsafe`, no `UnsafeCell`/raw-pointer split.
    ///
    /// A wrapped readable region is simply two `copy_run_into` → `write_all` → `advance` iterations:
    /// this call returns only the run up to the wrap boundary, and the next call returns the wrapped
    /// remainder. Returns `n == 0` when the ring is empty (or fully held during preroll, when the
    /// caller withholds `advance`).
    pub fn copy_run_into(&self, max_run: usize, tx_buf: &mut [u8]) -> DrainRun {
        let st = self
            .ring
            .state
            .lock()
            .expect("inbound PCM ring mutex poisoned");
        let cap = self.ring.cap;
        let available = st.head.wrapping_sub(st.tail) as usize;
        let start = (st.tail as usize) % cap;
        let contiguous = cap - start;
        let base_n = available.min(max_run).min(contiguous).min(tx_buf.len());
        // Cap the run at the next pending end-of-audio mark so the boundary lands on a run edge
        // (design §3.4): if the oldest mark sits *ahead* of `tail` (`dist > 0`) and within this
        // run's natural reach (`dist <= base_n`), shorten the run to end exactly on it and report
        // `reached_end_of_audio`. A mark at the current `tail` (`dist == 0`) is **not** capped here:
        // a zero-length run would look identical to "ring empty", and — critically — a mark at
        // `tail` on a *non-empty* ring (an `EndOfAudio`/`FlushPlayback` immediately followed by fresh
        // audio, correctness-2) must be observed *before* draining past it, which a run spanning
        // beyond it cannot do. Both `dist == 0` cases are instead observed by the drain loop's
        // per-pass `take_mark_at_tail` (design §3.4), which runs before this call. A mark beyond the
        // run (`dist > base_n`) is left for a later pass. The mark is not popped here — `advance`
        // pops it once `tail` reaches it, so each boundary reports exactly once.
        let (n, reached_end_of_audio) = match st.peek_mark_pos() {
            Some(mark) => {
                let dist = mark.wrapping_sub(st.tail) as usize;
                if dist > 0 && dist <= base_n {
                    (dist, true)
                } else {
                    (base_n, false)
                }
            }
            None => (base_n, false),
        };
        tx_buf[..n].copy_from_slice(&st.buf[start..start + n]);
        DrainRun {
            n,
            generation: st.generation,
            reached_end_of_audio,
        }
    }

    /// Pop a pending end-of-audio mark equal to the current `tail`, returning whether one was
    /// popped (design §3.4). This is the observation path for boundaries that ride an **empty**
    /// ring (`head == tail`), which [`copy_run_into`](Self::copy_run_into) cannot report (its
    /// zero-length run is indistinguishable from "ring empty") yet two designed flows produce:
    /// a flush (`reset()` then `mark_end_of_audio()` on the emptied ring, §3.5) and a
    /// drop-as-end-of-audio after a stall has already drained the ring (§3.4). The capture-thread
    /// drain loop consults this in its empty-break arms and treats `true` identically to a
    /// [`DrainRun::reached_end_of_audio`] (the capture thread's drain loop). A brief
    /// lock-read-pop-unlock.
    pub fn take_mark_at_tail(&self) -> bool {
        let mut st = self
            .ring
            .state
            .lock()
            .expect("inbound PCM ring mutex poisoned");
        let tail = st.tail;
        st.pop_mark_if_at(tail)
    }

    /// Publish consumption of `n` bytes by advancing `tail` (`tail.wrapping_add(n)`) under the lock,
    /// freeing the space the producer observes on its next `write` (the re-arm, design §2.4). Called
    /// after a successful `write_all` of the bytes a prior `copy_run_into` returned; a failed
    /// `write_all` simply does **not** `advance`, leaving the bytes buffered for retry next pass.
    ///
    /// Returns `true` when advancing popped an end-of-audio mark that landed **exactly** on the new
    /// `tail`. In the normal path this is the mark a prior `copy_run_into` capped the run on (design
    /// §3.4). It also covers a race (correctness-1): the producer may push a mark at `head` between
    /// `copy_run_into` (which then saw no mark, reporting `reached_end_of_audio == false` and a run
    /// draining to `head`) and this `advance`; the new `tail` lands on that mark and the pop reports
    /// it here. The caller must treat a `true` return identically to `DrainRun::reached_end_of_audio`
    /// — otherwise the boundary is silently consumed and the mute never arms.
    pub fn advance(&self, n: usize) -> bool {
        let mut st = self
            .ring
            .state
            .lock()
            .expect("inbound PCM ring mutex poisoned");
        // The consumer must only ever advance `tail` over bytes a prior `copy_run_into` reported as
        // available (errhandling-4). An `n` larger than `head - tail` would push `tail` past `head`,
        // silently freeing space the consumer never read and letting the producer overwrite live
        // bytes. The single-caller drain loop is correct by construction; this guards a future caller
        // that double-advances or passes an oversized `n` from corrupting ring state undetectably.
        debug_assert!(
            n as u32 <= st.head.wrapping_sub(st.tail),
            "advance({n}) exceeds available bytes ({}) — tail must not pass head",
            st.head.wrapping_sub(st.tail),
        );
        st.tail = st.tail.wrapping_add(n as u32);
        // Pop an end-of-audio mark the tail has now reached (design §3.4): `copy_run_into` caps a
        // run to end exactly on the next mark and reports `reached_end_of_audio`, so after advancing
        // over such a run `tail == mark` and the boundary is consumed here — exactly once per mark.
        // Report whether a mark was popped so the caller can arm the mute even when the run was
        // *not* capped (the mark was pushed during the write; correctness-1).
        let tail = st.tail;
        st.pop_mark_if_at(tail)
    }

    /// Apply a pending reconnection reset (design §2.8): jump `tail = head_at_reset`, dropping the
    /// dead connection's un-played tail race-free (the producer recorded `head_at_reset` under this
    /// same lock), and return the `generation` now in effect so the caller records it as acted-on.
    /// The caller invokes this when [`copy_run_into`](Self::copy_run_into)'s observed `generation`
    /// differs from the last it acted on, then re-arms preroll and resets its underrun-edge state.
    pub fn apply_reset(&self) -> u32 {
        let mut st = self
            .ring
            .state
            .lock()
            .expect("inbound PCM ring mutex poisoned");
        // Discard the dead connection's un-reached end-of-audio marks before jumping `tail` (design
        // §3.4, edge case E): a mark from a superseded generation belongs to the dropped tail, while
        // a mark pushed in the current generation — the flush `reset()` + `mark_end_of_audio()`
        // boundary (§3.5), which lands at exactly `head_at_reset` — survives. `generation` was
        // already bumped by the producer's `reset()`, so this compares against the new value.
        st.discard_dead_generation_marks();
        st.tail = st.head_at_reset;
        st.generation
    }
}

/// Countdown-based log rate limiter.
///
/// Construct with a cadence; [`tick`](LogCountdown::tick) fires (`true`) on the very
/// first call, then once every `cadence + 1` calls thereafter, and the caller emits
/// its log only when `tick` returns `true`. Shared by `CountingSink::accept` and
/// `I2sStreamSink::accept` so the two sinks cannot diverge on the cadence.
///
/// The period is `cadence + 1`: after a fire, `remaining` is reloaded to `cadence`
/// and decremented to 0 over the next `cadence` calls, so the following fire lands
/// `cadence + 1` calls after the previous one.
pub struct LogCountdown {
    cadence: u32,
    remaining: u32,
}

impl LogCountdown {
    /// New limiter. The first [`tick`](LogCountdown::tick) fires immediately
    /// (`remaining` starts at 0), matching the prior sites' behavior (their
    /// `log_countdown` started at 0, logging on the first frame); thereafter it
    /// fires once every `cadence + 1` calls.
    pub fn new(cadence: u32) -> Self {
        LogCountdown {
            cadence,
            remaining: 0,
        }
    }

    /// Advance one step; return `true` when the caller should log this call.
    ///
    /// Fires on the first call and then every `cadence + 1` calls thereafter (the
    /// reload-to-`cadence`-then-count-down-to-0 cycle spans `cadence + 1` calls).
    pub fn tick(&mut self) -> bool {
        if self.remaining == 0 {
            self.remaining = self.cadence;
            true
        } else {
            self.remaining -= 1;
            false
        }
    }
}

/// Acceptance signal returned by [`PlaybackSink::accept`] (design §2c step 1).
///
/// The two states distinguish *steady-state overload* (the channel is full and the
/// caller must apply backpressure by holding the frame and re-reading the socket more
/// slowly) from *normal forward progress*. A `Full` return is the lever the streamer
/// uses to stop reading inbound TCP faster than the DAC drains: on `Full`, `consume_frames`
/// leaves the frame buffered and stops the decode loop without consuming it, so TCP flow
/// control pushes back on the sender instead of the sink dropping the chunk internally.
///
/// `Enqueued` is also returned by the should-never-happen `Disconnected`/no-sender arms
/// (which discard-and-count as before): back-pressuring on a dead channel would wedge the
/// inbound stream forever with no consumer to drain it, so those paths report forward
/// progress rather than refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Accepted {
    /// The frame was enqueued (or discarded-and-counted on a should-never-happen dead
    /// channel) — the caller may advance past it.
    Enqueued,
    /// The channel was full; the frame was **not** enqueued and was **not** consumed.
    /// The caller must hold the frame and apply backpressure (stop reading the socket
    /// until a slot frees) rather than dropping it.
    Full,
}

/// Inbound-playback trait: the integration point `consume_frames` calls for every
/// decoded inbound `Audio` frame.
///
/// EXPOSURE NOTE (persistent-audio-connection): the device drains this inbound channel
/// on every idle tick, independent of VAD — the socket is *always open and always
/// reading*, with no segment gating to bound what a peer can play out of the
/// speaker. The transport must authenticate the peer; without that, any
/// reachable host could inject arbitrary audio.
pub trait PlaybackSink {
    /// Accept one decoded inbound `Audio` frame's PCM. Returns [`Accepted::Full`] when the
    /// downstream channel is full and the frame could not be enqueued (the caller must
    /// apply backpressure and retry), or [`Accepted::Enqueued`] otherwise.
    fn accept(&mut self, pcm: &[u8]) -> Accepted;

    /// Handle an explicit end-of-audio marker (`InboundFrame::EndOfAudio`): the
    /// host has finished a stream. The banked tail plays out, then the DAC mutes. Default
    /// no-op so non-playback sinks (counting/test) need not react; `I2sStreamSink` overrides
    /// it to push an end-of-audio mark onto the ring.
    fn end_of_audio(&mut self) {}

    /// Handle a flush/stop control frame (`InboundFrame::Flush`): discard
    /// everything banked and go silent immediately (barge-in mechanism). Default no-op;
    /// `I2sStreamSink` overrides it to reset the ring and mark end-of-audio.
    fn flush_playback(&mut self) {}
}

/// Real speaker-output playback sink (Seam C, design §2.3).
///
/// Replaces `CountingSink` as the persistent-streamer-socket `inbound_sink`:
/// instead of discarding validated PCM, it copies each S16_LE-mono frame's **raw wire
/// bytes directly into the inbound PCM ring's free space** — the single, allocate-once SPSC
/// byte ring it holds as its [`InboundRingProducer`] write end (design §2.1, §3.1). The
/// capture thread (the sole I2S agent, the ring's consumer half) drains the ring, expands
/// each sample to a 32-bit-stereo I2S TX frame at DMA-write time ([`expand_run_into`], sharing
/// `expand_sample_to_frame`'s layout), and writes to I2S TX. There is **no per-frame heap
/// allocation** and no per-frame expansion on the streamer thread: the raw copy lands in the
/// pre-allocated ring instead of a freshly-`malloc`'d `Vec`, and the expansion runs on the
/// consumer (design §3.1).
///
/// `accept` never blocks the streamer thread: when the ring lacks sufficient free space for
/// the frame's raw bytes (total free `< need` — the producer's `write` splits the copy
/// across the wrap boundary, so a non-contiguous free region is not itself a `Full` cause) it
/// returns [`Accepted::Full`] without writing or discarding the
/// frame, so the caller can apply real backpressure (hold the frame, stop reading the
/// socket — design §2.4). Blocking the thread on I2S DMA drain would stall the outbound mic
/// capture it also carries (§2.1). A `Full` return is counted as a `full_stalls`
/// backpressure event; expected steady-state `full_stalls` is 0 absent overload. The
/// no-producer should-never-happen arm discards-and-counts (as `dead_channel_discards`) and
/// returns `Enqueued` — back-pressuring with no ring wired would wedge the stream forever.
///
/// The process-lifetime producer comes from the production ring split at boot in `main.rs`
/// (the `INBOUND_PCM_PRODUCER` global `static`, replacing `INBOUND_PCM_TX`); this struct is
/// constructed via [`with_producer`](I2sStreamSink::with_producer) (the device crate resolves
/// the static in a thin wrapper). The `CountingSink` reference sink is
/// retained in the device crate for the `TlsInboundFrames` HIL self-test.
pub struct I2sStreamSink {
    /// The ring producer (write end), held in place of the channel `SyncSender` the
    /// sink owned before the ring replaced the channel. `accept` writes expanded PCM into
    /// the ring through it; the reconnect-reset path calls [`reset`](InboundRingProducer::reset)
    /// on it (design §2.8). `None` if no ring was wired in `main()` when the sink was built —
    /// `accept` then drops every frame (counted) rather than panicking. There is no
    /// `Disconnected` state (the ring is a shared `Arc`, not a channel that can hang up — design
    /// §6.2): a vanished consumer manifests as sustained `full_stalls`/permanent backpressure,
    /// not data loss.
    producer: Option<InboundRingProducer>,
    frames: u32,
    samples: u64,
    /// Backpressure events **only**: times `accept` returned [`Accepted::Full`] because the
    /// ring lacked room (capture thread behind). Under real backpressure (design §2.4) a `Full`
    /// is **not** a dropped frame — the caller holds and retries the frame — so this counts
    /// *stalls*, not data loss. Steady-state target is 0. The should-never-happen no-producer
    /// discards are tracked separately in `dead_channel_discards` so an operator reading the
    /// periodic log can tell "DAC fell behind" (healthy backpressure, `full_stalls` climbing)
    /// from "ring unwired" (true audio loss, `dead_channel_discards` climbing) — the two have
    /// opposite remedies and must not be conflated (errhandling-2 / quality-1).
    full_stalls: u32,
    /// Value of `full_stalls` at the previous rate-limited `info!` window boundary, so the
    /// per-window stall delta (`full_stalls - full_stalls_at_last_log`) is recoverable for the
    /// backpressure-burst event detector (§2.3). A sub-second flood of ring-full stalls
    /// folds into the running `full_stalls` total of the aggregate `info!` line and is
    /// invisible as an *event*; tracking the per-window delta turns it into a timestamped
    /// `warn!` the user can correlate to an audible artifact (a backed-up DAC). Updated to the
    /// current `full_stalls` on each periodic emit.
    full_stalls_at_last_log: u32,
    /// True data loss: frames discarded because no producer was wired (firmware-ordering bug).
    /// A should-never-happen invariant violation — distinct from `full_stalls` (held, not lost).
    /// Surfaced in the periodic `info!` line so a sustained unwired ring is visible as climbing
    /// `dead_channel_discards` rather than masquerading as backpressure (errhandling-2 /
    /// quality-1). Steady-state target is 0; any non-zero value means audio was actually lost.
    /// Unlike the channel design there is no `Disconnected` path — the ring cannot hang up — so
    /// the no-producer arm is the only contributor.
    dead_channel_discards: u32,
    /// Consumer-stall watchdog state (design §6.2(b)) — the ring's replacement for the channel's
    /// `Disconnected` "capture thread died" signal. A ring cannot report a dead consumer; a wedged
    /// consumer instead shows up as a *full ring whose `tail` never advances*. These two fields let
    /// `accept`'s `Full` arm tell that apart from healthy backpressure (DAC merely behind, `tail`
    /// still climbing): `last_consumed_tail` is the consumer's `tail` (bytes consumed) at the most
    /// recent observation, and `stalls_since_tail_advanced` counts consecutive ring-full `accept`
    /// stalls during which `tail` has not moved. Crossing
    /// [`PLAYBACK_CONSUMER_STALL_WARN_STALLS`] emits a one-shot `warn!`; both reset when `tail`
    /// advances (the consumer made progress) — so the warn is edge-triggered, not per-stall.
    last_consumed_tail: u32,
    stalls_since_tail_advanced: u32,
    /// Edge latch for the consumer-stall `warn!`: set true when the threshold warn has fired, so it
    /// is emitted **once** per stall episode and re-armed only after `tail` advances again (design
    /// §6.2(b)). Prevents a wedged consumer from flooding the log with one warn per held frame.
    consumer_stall_warned: bool,
    log_countdown: LogCountdown,
}

impl I2sStreamSink {
    /// Construct with an explicit ring producer (or `None`). This is the sole constructor:
    /// `main.rs` resolves the `INBOUND_PCM_PRODUCER` global static and calls this; unit
    /// tests inject a real ring's producer half (design §5).
    ///
    /// `None` means no ring was wired — a firmware-ordering bug (the streamer is spawned after
    /// `main()` splits the ring), surfaced via the `dead_channel_discards` counter rather than a
    /// panic on the streamer thread.
    pub fn with_producer(producer: Option<InboundRingProducer>) -> Self {
        I2sStreamSink {
            producer,
            frames: 0,
            samples: 0,
            full_stalls: 0,
            full_stalls_at_last_log: 0,
            dead_channel_discards: 0,
            last_consumed_tail: 0,
            stalls_since_tail_advanced: 0,
            consumer_stall_warned: false,
            log_countdown: LogCountdown::new(PLAYBACK_LOG_CADENCE_FRAMES),
        }
    }
}

impl PlaybackSink for I2sStreamSink {
    fn accept(&mut self, pcm: &[u8]) -> Accepted {
        // Validate: must be a non-zero even number of bytes (S16_LE samples).
        // Rejected frames never reach TX (§2.3 step 1). A malformed frame is the caller's
        // to discard (it is not retryable), so report forward progress (`Enqueued`) — the
        // caller advances past it rather than holding it under backpressure.
        if !is_valid_s16le_pcm(pcm) {
            log::warn!(
                "streamer: inbound Audio frame has invalid PCM length {} — discarding",
                pcm.len()
            );
            return Accepted::Enqueued;
        }

        // Raw-mono storage (design §3.1): the ring holds the wire bytes verbatim; expansion to the
        // 32-bit-stereo I2S frame layout happens at DMA-write time in the consumer
        // (`expand_run_into`). `sample_count` is still tracked for the periodic `samples=` counter,
        // but the ring stores `need = pcm.len()` **raw** bytes, not `× I2S_TX_FRAME_BYTES` expanded.
        let sample_count = pcm.len() / WIRE_BYTES_PER_SAMPLE;
        let need = pcm.len();

        // Write the raw PCM directly into the ring's free space — no per-frame allocation and no
        // expansion on the streamer thread (design §3.1). `producer.write` locks the ring, checks
        // room *before* any copy, and on no room returns `false` having written nothing (the
        // `Accepted::Full` backpressure cause — the frame stays buffered upstream, design §2.4). On
        // room it invokes the `fill` closure once per contiguous run (the free region may wrap the
        // cap boundary), into which we memcpy the matching slice of `pcm`.
        //
        // `offset` is the byte position into the logical frame at which `dst` begins, so the closure
        // copies `pcm[offset..offset + dst.len()]`.
        //
        // Writes and drains stay whole-*sample* (WIRE_BYTES_PER_SAMPLE) aligned: `need = pcm.len()`
        // is even (`is_valid_s16le_pcm` rejects odd lengths) and `cap` is a multiple of
        // WIRE_BYTES_PER_SAMPLE (the `INBOUND_PCM_RING_BYTES` alignment assert; host-test caps are
        // likewise even), so `start = head % cap` is sample-aligned and the wrap-split lengths
        // (`min(need, cap - start)` and the remainder) are too. Only sample alignment is required,
        // not whole-I2S-frame (8 B) alignment, because the ring stores raw wire bytes and the
        // consumer expands to I2S frames at DMA-write time (design §3.1).
        let accepted = match self.producer.as_ref() {
            Some(producer) => {
                let wrote = producer.write(need, |offset, dst| {
                    dst.copy_from_slice(&pcm[offset..offset + dst.len()]);
                });
                if wrote {
                    // The write found room, so the consumer is keeping up well enough — clear the
                    // consumer-stall watchdog (design §6.2(b)): a successful write means the ring is
                    // not wedged-full, so the "DAC behind vs. consumer dead" signal is in the healthy
                    // state. (The watchdog also clears whenever `tail` advances on a `Full` stall
                    // below; clearing here covers the case where a stall episode ends with room
                    // reappearing.)
                    //
                    // We do NOT refresh `last_consumed_tail` here (efficiency-1): doing so cost a
                    // second ring lock on every healthy enqueue (the hot path this change exists to
                    // slim). `last_consumed_tail` is the watchdog *baseline*, only ever read in
                    // `note_consumer_stall` on the `Full` path — which re-reads `producer.consumed()`
                    // itself. A baseline sampled lazily at the first stall of an episode is equally
                    // correct: the watchdog fires only after PLAYBACK_CONSUMER_STALL_WARN_STALLS
                    // *consecutive* stalls with a frozen `tail`, so at worst the first stall poll of
                    // an episode establishes the baseline instead of counting — within tolerance.
                    self.stalls_since_tail_advanced = 0;
                    self.consumer_stall_warned = false;
                    Accepted::Enqueued
                } else {
                    // Ring-full backpressure. Run the consumer-stall watchdog (design §6.2(b))
                    // before the early return: read the consumer's `tail` and decide whether this is
                    // healthy backpressure (the DAC is draining, `tail` advancing) or a wedged
                    // consumer (a full ring whose `tail` is frozen) — the ring's replacement for the
                    // channel's `Disconnected` signal.
                    self.note_consumer_stall(producer.consumed());
                    // Backpressure event — not a drop. Count the stall and return `Full`;
                    // the caller will hold and retry the frame. Do NOT advance the
                    // frame/sample counters: the frame was not written and will be re-decoded
                    // and re-accepted on the next drain tick (so counting now would
                    // double-count on the retry).
                    //
                    // The socket-path read-throttle / TCP-window-close interaction this stall
                    // feeds is covered on real hardware by the `TlsInboundBackpressure` HIL
                    // self-test (an unpaced flood through the production socket → ring path,
                    // asserting `full_stalls > 0`, an exact frame count, and a clean EOF).
                    self.full_stalls = self.full_stalls.wrapping_add(1);
                    // Tick the periodic log *before* the early return (quality-2): under
                    // sustained backpressure every `accept` returns `Full` here, so a log gated
                    // only on the enqueued path below would go silent during exactly the overload
                    // the observability machinery exists to surface. Ticking here keeps the
                    // periodic `info!` + backpressure-burst `warn!` firing while the ring is full.
                    self.tick_periodic_log();
                    return Accepted::Full;
                }
            }
            // No producer wired (should-never-happen ordering bug). The construction-time
            // warn lives in `build_inbound_stream_sink` (the device wrapper in `main.rs`),
            // not in `with_producer` — this level is silent; the running count surfaces in the
            // periodic log below. This is true data loss (no ring to retry into), so it counts
            // as a dead-channel discard, not a backpressure stall. (A ring has no `Disconnected`
            // state — design §6.2 — so this no-producer arm is the only data-loss path.)
            None => {
                self.dead_channel_discards = self.dead_channel_discards.wrapping_add(1);
                Accepted::Enqueued
            }
        };

        // The frame was processed (written, or discarded on a should-never no-producer ring) —
        // advance the counters. The `Full` path returned above without reaching here.
        self.frames = self.frames.wrapping_add(1);
        self.samples = self.samples.wrapping_add(sample_count as u64);

        self.tick_periodic_log();

        accepted
    }

    /// Explicit end-of-audio (design §3.4): the host finished a stream. Push an end-of-audio
    /// mark onto the ring at the current `head` so the mute decision fires when the *banked
    /// tail finishes playing* (as `tail` climbs to the mark), not when this frame arrives — the
    /// whole point of the ring-riding marker (design §3.4 "the marker rides the ring"). A
    /// stalled pipeline still never mutes; only this explicit boundary (or a dropped connection,
    /// which the streamer routes here too, §3.4) arms the mute.
    ///
    /// No-producer arm: a should-never-happen firmware-ordering bug (streamer up before the ring
    /// split). There is no ring to mark, so this is a silent no-op — no `dead_channel_discards`
    /// bump, because no audio frame was lost (a control marker is not audio data); the unwired
    /// ring is already surfaced by `accept`'s `dead_channel_discards` counter.
    fn end_of_audio(&mut self) {
        if let Some(producer) = self.producer.as_ref() {
            producer.mark_end_of_audio();
        }
    }

    /// Flush/stop (design §3.5): discard everything banked and go silent immediately (barge-in
    /// mechanism). `reset()` bumps the ring generation — the consumer's `apply_reset` jumps
    /// `tail = head_at_reset` race-free, dropping the un-played tail and re-arming preroll — then
    /// `mark_end_of_audio()` lands a mark at the emptied ring's head (`head == tail`) so the drain
    /// loop's next empty-ring poll observes it via `take_mark_at_tail` and arms the mute (design
    /// §3.4/§3.5). Order matters: reset first (so the mark rides the *new* generation and survives
    /// `apply_reset`'s dead-generation discard), mark second.
    ///
    /// No-producer arm: silent no-op, same rationale as [`end_of_audio`](Self::end_of_audio).
    fn flush_playback(&mut self) {
        if let Some(producer) = self.producer.as_ref() {
            producer.reset();
            producer.mark_end_of_audio();
        }
    }
}

impl I2sStreamSink {
    /// Mark a (re)connection boundary on the ring (design §2.8), replacing the prior
    /// channel `StreamReset` enqueue.
    ///
    /// The reset is now an **infallible** producer operation: [`InboundRingProducer::reset`]
    /// locks the ring and bumps `generation` + records `head_at_reset` under that lock. Because
    /// the producer is the sole `head` writer, the reset is ordered after all prior writes and
    /// before all subsequent writes by single-writer construction — the FIFO "boundary, then
    /// fresh audio" ordering the old `StreamReset` channel message provided, without a separate
    /// ordered marker and **without a `Full` failure mode**: there is always room for a
    /// generation bump (it writes no audio bytes). The prior channel-based `Full`/`pending_reset`
    /// retry dance therefore disappears (design §2.8).
    ///
    /// Infallible: a generation bump always has room (it writes no audio bytes), so there is no
    /// result to return. The two arms differ only in their side effect:
    /// - **producer wired** → the reset is applied.
    /// - **no producer wired** (should-never-happen ordering bug) → dead-channel discard (counted
    ///   in `dead_channel_discards`, exactly as `accept`'s no-producer arm); there is no ring to
    ///   reset.
    pub fn send_stream_reset(&mut self) {
        match self.producer.as_ref() {
            Some(producer) => producer.reset(),
            // No producer wired (should-never-happen ordering bug) — discard-and-count like the
            // `accept` no-producer arm.
            None => {
                self.dead_channel_discards = self.dead_channel_discards.wrapping_add(1);
            }
        }
    }

    /// Consumer-stall watchdog (design §6.2(b)) — run on every ring-full `accept` stall.
    ///
    /// A ring has no `Disconnected` state, so a vanished/wedged consumer (capture/DAC thread) does
    /// not surface as data loss the way the channel's `dead_channel_discards` did; it surfaces as a
    /// *full ring whose `tail` never advances*. `tail` is the consumer's bytes-consumed counter
    /// (`producer.consumed()`), passed in. This method preserves the channel design's deliberate
    /// "DAC fell behind (healthy backpressure) vs. capture thread died (wedged)" distinction:
    /// - If `tail` advanced since the last observation, the consumer made progress (the DAC is
    ///   draining, just slower than the inbound rate) — reset the stall counter and re-arm the warn
    ///   edge. This is healthy backpressure, no warn.
    /// - If `tail` did **not** advance, increment the consecutive-stall counter; on first crossing
    ///   [`PLAYBACK_CONSUMER_STALL_WARN_STALLS`] (≈1 s of a full ring with no consumer progress),
    ///   emit a one-shot `warn!`. The latch (`consumer_stall_warned`) keeps it to one line per stall
    ///   episode — re-armed only when `tail` advances again — so a truly wedged consumer is surfaced
    ///   without flooding the log.
    fn note_consumer_stall(&mut self, tail: u32) {
        if tail != self.last_consumed_tail {
            // The consumer advanced `tail` since the last stall observation — it is alive and
            // draining (healthy backpressure, not a wedge). Reset the watchdog edge.
            self.last_consumed_tail = tail;
            self.stalls_since_tail_advanced = 0;
            self.consumer_stall_warned = false;
            return;
        }
        // `tail` is frozen while the ring is full — the consumer may be wedged.
        self.stalls_since_tail_advanced = self.stalls_since_tail_advanced.saturating_add(1);
        if self.stalls_since_tail_advanced >= PLAYBACK_CONSUMER_STALL_WARN_STALLS
            && !self.consumer_stall_warned
        {
            self.consumer_stall_warned = true;
            log::warn!(
                "streamer: inbound PCM ring full for {} consecutive stalls with no consumer \
                 progress (tail frozen at {} bytes) — capture/DAC thread appears wedged, not merely \
                 behind; inbound stream is backpressured and will not advance until it drains",
                self.stalls_since_tail_advanced,
                self.last_consumed_tail
            );
        }
    }

    /// Rate-limited logging: once per `PLAYBACK_LOG_CADENCE_FRAMES` (~1 s at 20 fps), emit the
    /// running playback counters plus the per-window backpressure-stall delta
    /// (`stalls_this_window=`) on a single `info!` line (§2.3).
    ///
    /// Called on **every** `accept` outcome — the enqueued/dead-channel path at the end of
    /// `accept` *and* the `Full` early-return path (quality-2). Gating this only on the enqueued
    /// path would silence the log during sustained backpressure (every `accept` returns `Full`
    /// then), which is exactly when the operator needs the signal.
    fn tick_periodic_log(&mut self) {
        if self.log_countdown.tick() {
            // The aggregate `full_stalls` counter is the running backpressure total; the
            // *per-window* delta (`full_stalls - full_stalls_at_last_log`) is the per-burst
            // signal. Previously the delta was emitted as its own per-occurrence
            // `BACKPRESSURE BURST` `warn!`, which flooded the log under sustained backpressure
            // (one line per window-with-stalls, drowning real signal). It is now folded into
            // this single periodic `info!` line as the `stalls_this_window=` field — the
            // information stays visible as a number (per-window delta alongside the cumulative
            // `full_stalls`) without a separate per-burst line. The field names *backpressure*
            // stalls specifically — true data loss is the separate `dead_channel_discards`
            // counter, never folded in here (quality-1).
            let stalls_this_window = self.full_stalls.wrapping_sub(self.full_stalls_at_last_log);
            log::info!(
                "streamer: inbound playback frames={} samples={} full_stalls={} stalls_this_window={} dead_channel_discards={}",
                self.frames,
                self.samples,
                self.full_stalls,
                stalls_this_window,
                self.dead_channel_discards
            );
            self.full_stalls_at_last_log = self.full_stalls;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        expand_run_in_place, expand_run_into, expand_sample_to_frame, is_drop_burst,
        next_preroll_target, preroll_gate_ready, Accepted, I2sStreamSink, InboundPcmRing,
        InboundRingConsumer, LogCountdown, PlaybackSink, I2S_TX_FRAME_BYTES,
        PLAYBACK_CONSUMER_STALL_WARN_STALLS, PLAYBACK_LOG_CADENCE_FRAMES,
        PLAYBACK_PREROLL_MAX_TARGET_BYTES, WIRE_BYTES_PER_SAMPLE,
    };

    /// Build a sink wired to a fresh ring of `cap` bytes, returning the sink and the ring's
    /// consumer half so a test can inspect what the producer wrote / drain it (the analogue of
    /// the prior channel `(tx, rx)` pair). The consumer is the sole way to advance `tail`, so a
    /// test that never drains reproduces a *full* ring deterministically (the analogue of a
    /// non-draining receiver).
    fn sink_with_ring(cap: usize) -> (I2sStreamSink, InboundRingConsumer) {
        let (producer, consumer) = InboundPcmRing::new(cap).split();
        (I2sStreamSink::with_producer(Some(producer)), consumer)
    }

    /// Drain all currently-available bytes out of the ring consumer in read order (the test
    /// analogue of pulling every chunk off the channel). Mirrors the `ring_drain` helper but
    /// reads exactly `available()` so callers can assert on the producer's written bytes.
    fn drain_all(consumer: &InboundRingConsumer) -> Vec<u8> {
        let mut out = Vec::new();
        let mut tx_buf = vec![0u8; super::INBOUND_PCM_WRITE_UNIT_BYTES];
        loop {
            let run = consumer.copy_run_into(usize::MAX, &mut tx_buf);
            if run.n == 0 {
                break;
            }
            out.extend_from_slice(&tx_buf[..run.n]);
            consumer.advance(run.n);
        }
        out
    }

    /// `expand_sample_to_frame` produces the exact RX-mirrored byte layout for a fixed
    /// input vector: content MSB-aligned in the left slot (`[0,0,lo,hi]`), right slot
    /// silent (`[0,0,0,0]`). Pins the shared S16_LE-mono → 32-bit-stereo expansion so the
    /// sine source and the inbound sink cannot diverge (design §2.3 step 2, §5).
    #[test]
    fn expand_sample_to_frame_exact_layout() {
        // (input i16, expected 8-byte frame) — covers zero, positive, negative, and the
        // signed extremes, each little-endian in the left slot with the right slot silent.
        let cases: [(i16, [u8; I2S_TX_FRAME_BYTES]); 5] = [
            (0x0000, [0, 0, 0x00, 0x00, 0, 0, 0, 0]),
            (0x1234, [0, 0, 0x34, 0x12, 0, 0, 0, 0]),
            (-1, [0, 0, 0xFF, 0xFF, 0, 0, 0, 0]),
            (i16::MAX, [0, 0, 0xFF, 0x7F, 0, 0, 0, 0]),
            (i16::MIN, [0, 0, 0x00, 0x80, 0, 0, 0, 0]),
        ];
        for (sample, expected) in cases {
            assert_eq!(
                expand_sample_to_frame(sample),
                expected,
                "sample {sample:#06x} did not expand to the expected left-slot/right-silent frame"
            );
        }
    }

    // ── expand_run_into — the consumer's DMA-write-time expansion (design §3.1, §5) ──────
    //
    // The ring stores raw wire bytes; the capture/DAC consumer applies this expansion as it drains.
    // These pin that the run-expansion produces exactly the per-sample `expand_sample_to_frame`
    // layout, including when a logical stream is delivered as two runs across a ring wrap (the
    // drain loop expands each run independently, so the concatenation must equal the whole-stream
    // expansion).

    /// A raw run expands to the exact concatenation of per-sample `expand_sample_to_frame` frames.
    #[test]
    fn expand_run_into_matches_per_sample_layout() {
        // Four samples spanning zero, positive, negative, and the signed extremes.
        let samples: [i16; 4] = [0x1234, -1, i16::MIN, i16::MAX];
        let mut raw = Vec::new();
        for s in samples {
            raw.extend_from_slice(&s.to_le_bytes());
        }
        let mut out = vec![0u8; samples.len() * I2S_TX_FRAME_BYTES];
        expand_run_into(&raw, &mut out);
        for (i, &s) in samples.iter().enumerate() {
            let base = i * I2S_TX_FRAME_BYTES;
            assert_eq!(
                &out[base..base + I2S_TX_FRAME_BYTES],
                &expand_sample_to_frame(s),
                "frame {i} must match the shared expansion layout for sample {s:#06x}"
            );
        }
    }

    /// A logical stream drained as two runs across a ring wrap expands, run-by-run, to the same
    /// bytes as expanding the whole stream at once — the byte-exact wrap-spanning property the drain
    /// loop relies on (it expands each `copy_run_into` run separately).
    #[test]
    fn expand_run_into_wrap_spanning_byte_exact() {
        // 256-byte ring; advance the read cursor near the seam, then buffer a run that wraps.
        let cap = 256;
        let (producer, consumer) = InboundPcmRing::new(cap).split();
        // Fill+drain 240 bytes so head == tail == 240 (only 16 bytes to the seam).
        assert!(ring_write(&producer, &pattern(0, 240)));
        let _ = ring_drain(&consumer, 240);
        // A 40-byte raw run (20 samples) now occupies [240..256) (16 B) + [0..24) (24 B) — a wrap.
        let raw_stream = pattern(1000, 40);
        assert!(ring_write(&producer, &raw_stream));

        // Reference: expand the whole 40-byte stream in one shot.
        let mut whole = vec![0u8; raw_stream.len() / 2 * I2S_TX_FRAME_BYTES];
        expand_run_into(&raw_stream, &mut whole);

        // Drain run-by-run (each run is contiguous, capped at the wrap boundary) and expand each,
        // concatenating the results — the drain loop's exact behavior.
        let mut run_buf = vec![0u8; INBOUND_PCM_WRITE_UNIT_BYTES];
        let mut assembled = Vec::new();
        loop {
            let run = consumer.copy_run_into(INBOUND_PCM_WRITE_UNIT_BYTES, &mut run_buf);
            if run.n == 0 {
                break;
            }
            let mut exp = vec![0u8; run.n / 2 * I2S_TX_FRAME_BYTES];
            expand_run_into(&run_buf[..run.n], &mut exp);
            assembled.extend_from_slice(&exp);
            consumer.advance(run.n);
        }
        assert_eq!(
            assembled, whole,
            "per-run expansion across a wrap must equal the whole-stream expansion byte-for-byte"
        );
    }

    /// In-place expansion produces byte-for-byte the same result as the out-of-place
    /// [`expand_run_into`] — the single-staging-buffer drain relies on this equivalence.
    #[test]
    fn expand_run_in_place_matches_expand_run_into() {
        // Samples spanning zero, positive, negative, and the signed extremes, repeated so the run
        // is long enough that a low-index sample is read only after several higher frames overwrote
        // the buffer above it (exercises the high→low ordering).
        let samples: Vec<i16> = (0..64)
            .map(|i| [0i16, 0x1234, -1, i16::MIN, i16::MAX][i % 5])
            .collect();
        let mut raw = Vec::new();
        for &s in &samples {
            raw.extend_from_slice(&s.to_le_bytes());
        }

        let mut reference = vec![0u8; samples.len() * I2S_TX_FRAME_BYTES];
        expand_run_into(&raw, &mut reference);

        // In place: a buffer sized to the expanded length whose first raw.len() bytes hold the run.
        let mut buf = vec![0u8; samples.len() * I2S_TX_FRAME_BYTES];
        buf[..raw.len()].copy_from_slice(&raw);
        expand_run_in_place(&mut buf, samples.len());

        assert_eq!(
            buf, reference,
            "in-place expansion must equal out-of-place expansion byte-for-byte"
        );
    }

    // ── I2sStreamSink accept-routing unit tests (design §3.1, §2.4, §5) ──────────────
    //
    // Host-unit coverage of the real speaker sink's `accept`, which stores **raw** wire bytes in the
    // SPSC ring (the consumer expands on its DMA-write path, design §3.1): valid PCM is copied
    // verbatim into the ring's free space (returns `Enqueued`) and the bytes the consumer reads back
    // are byte-identical to the input; odd/zero-length PCM is rejected (no write) and not counted; a
    // *full* ring returns `Accepted::Full` without dropping or counting the frame (the caller owns
    // the retry — real backpressure, design §2.4). Each wires the sink to a fresh ring and
    // inspects/drains via the ring's consumer half; a test that never drains reproduces a full ring
    // deterministically (the analogue of a non-draining receiver). The ring has no `Disconnected`
    // state, so the only data-loss path is the no-producer arm (its own test below). The DMA-write
    // expansion itself is covered by the `expand_run_into_*` tests further down.

    /// Valid PCM is stored **raw** in the ring (`pcm.len()` bytes, verbatim) and the consumer reads
    /// it back byte-exact; frame/sample counters advance; nothing is dropped. Expansion to the I2S
    /// frame layout happens at DMA-write time in the consumer (`expand_run_into`), not in `accept`
    /// (design §3.1).
    #[test]
    fn i2s_stream_sink_accept_stores_raw_chunk() {
        let (mut sink, consumer) = sink_with_ring(1024);

        // 4 i16 samples = 8 raw bytes in; stored verbatim as 8 bytes (no ×4 expansion at accept).
        let pcm: [u8; 8] = [0x34, 0x12, 0xFF, 0xFF, 0x00, 0x80, 0xFF, 0x7F];
        assert_eq!(
            sink.accept(&pcm),
            Accepted::Enqueued,
            "a successful write must report Enqueued"
        );

        assert_eq!(sink.frames, 1, "valid PCM must increment frames");
        assert_eq!(sink.samples, 4, "valid PCM must count i16 samples");
        assert_eq!(
            sink.full_stalls, 0,
            "a write into a non-full ring must not stall"
        );

        let stored = drain_all(&consumer);
        assert_eq!(
            stored.len(),
            pcm.len(),
            "the ring stores raw wire bytes verbatim — no expansion at accept time"
        );
        assert_eq!(
            &stored[..],
            &pcm[..],
            "raw bytes must round-trip through the ring byte-exact (no expansion, no reorder)"
        );
        assert_eq!(
            consumer.available(),
            0,
            "exactly one frame's raw bytes per accept — nothing left after draining"
        );
    }

    /// Wrap-boundary `accept` correctness (design §3.1, §2.5): when the ring's free region straddles
    /// the `cap` seam, `accept`'s `fill` closure is called once per run with the matching `offset`, so
    /// the raw frame is reassembled byte-exact across the wrap. This is the seam the per-frame `Vec`
    /// never had to handle and the ring most needs at the sink level (the ring-only
    /// `ring_single_straddling_write` test covers the raw ring; this proves `accept`'s `offset`→source
    /// mapping lands correctly through the real sink path).
    #[test]
    fn i2s_stream_sink_accept_writes_across_wrap() {
        // 64-byte ring (32 samples capacity, raw). Advance the cursor so the next write must wrap.
        let (mut sink, consumer) = sink_with_ring(64);
        // First write: 30 samples (60 raw B) then drain → head == tail == 60, free region wraps.
        let pad: Vec<u8> = (0..60u8).collect();
        assert_eq!(sink.accept(&pad), Accepted::Enqueued);
        assert_eq!(drain_all(&consumer).len(), 60);

        // Second write: 4 samples (8 raw B). With head at byte 60 of a 64-byte ring, the run is
        // [60..64) (4 B) + [0..4) (4 B) — a wrap. The fill closure must place bytes 0..4 in the head
        // run and 4..8 in the wrapped run.
        let pcm: [u8; 8] = [0x34, 0x12, 0xFF, 0xFF, 0x00, 0x80, 0xFF, 0x7F];
        assert_eq!(
            sink.accept(&pcm),
            Accepted::Enqueued,
            "straddling frame must fit and write"
        );

        let got = drain_all(&consumer);
        assert_eq!(
            got.len(),
            pcm.len(),
            "all raw bytes present across the seam"
        );
        assert_eq!(
            &got[..],
            &pcm[..],
            "the straddling raw write must reassemble byte-exact across the wrap seam"
        );
    }

    /// Odd- and zero-length PCM is rejected at the sink: no ring write, no counter
    /// movement (mirrors `CountingSink`'s validation guard, §2.3 step 1).
    #[test]
    fn i2s_stream_sink_rejects_invalid_pcm_lengths() {
        let (mut sink, consumer) = sink_with_ring(1024);

        // Rejected frames are not retryable, so the sink reports forward progress
        // (`Enqueued`) — the caller advances past them rather than holding under backpressure.
        assert_eq!(sink.accept(&[0u8; 3]), Accepted::Enqueued); // odd length (>1)
        assert_eq!(sink.accept(&[0u8; 1]), Accepted::Enqueued); // odd length 1 — lowest odd non-zero
        assert_eq!(sink.accept(&[]), Accepted::Enqueued); // empty (the is_empty() branch)

        assert_eq!(sink.frames, 0, "invalid PCM must not increment frames");
        assert_eq!(sink.samples, 0, "invalid PCM must not count samples");
        assert_eq!(sink.full_stalls, 0, "rejected frames are not ring stalls");
        assert_eq!(
            consumer.available(),
            0,
            "rejected PCM must not write any bytes into the ring"
        );

        // Boundary control: the smallest *valid* length (2 bytes = 1 sample) is accepted and stored
        // raw (2 bytes, not one 8-byte expanded frame), pinning the guard boundary at 2.
        assert_eq!(sink.accept(&[0x00, 0x00]), Accepted::Enqueued);
        assert_eq!(sink.frames, 1, "2-byte PCM is the smallest valid frame");
        assert_eq!(
            consumer.available(),
            2,
            "valid 2-byte PCM must write exactly its 2 raw bytes into the ring"
        );
    }

    /// A sink built with no producer wired (`with_producer(None)` — the should-never-happen
    /// firmware-ordering case) discards + counts every frame and reports `Enqueued`
    /// (forward progress) rather than panicking or back-pressuring an unwired ring. A ring has
    /// no `Disconnected` state (design §6.2), so this no-producer arm is the *only* data-loss
    /// path — the prior `i2s_stream_sink_drops_on_disconnected_channel` test is removed with the
    /// condition it exercised.
    #[test]
    fn i2s_stream_sink_drops_with_no_producer() {
        let mut sink = I2sStreamSink::with_producer(None);
        assert_eq!(
            sink.accept(&[0x00, 0x00]),
            Accepted::Enqueued,
            "no-producer path must report forward progress, not Full"
        );
        assert_eq!(
            sink.dead_channel_discards, 1,
            "no-producer path must discard + count as a dead-channel discard"
        );
        assert_eq!(
            sink.full_stalls, 0,
            "a no-producer discard is NOT a backpressure stall"
        );
        // `frames` counts frames *presented to the sink*, not frames written to the ring
        // (test-1): the no-producer arm discarded this frame (counted in `dead_channel_discards`
        // above), yet `frames` still advances because the periodic log's `frames` is the
        // "frames the sink saw" denominator against which `dead_channel_discards` is read. An
        // operator seeing `frames` climb with `dead_channel_discards` climbing in lockstep — and
        // audio silent — diagnoses the unwired ring correctly; the two counters are read together.
        assert_eq!(
            sink.frames, 1,
            "frames counts frames presented to the sink (incl. no-producer discards, which \
             dead_channel_discards counts separately) — NOT frames written to the ring"
        );

        // A second frame discards/counts too — the path is repeatable and never wedges.
        assert_eq!(sink.accept(&[0x00, 0x00]), Accepted::Enqueued);
        assert_eq!(
            sink.dead_channel_discards, 2,
            "second no-producer discard counts too"
        );
        assert_eq!(sink.full_stalls, 0, "still no backpressure stall");
    }

    /// A full ring (consumer never drains) makes `accept` return [`Accepted::Full`]
    /// without blocking — and **without discarding the frame or counting it as processed**
    /// (the caller owns the retry under real backpressure, design §2.4). The `full_stalls`
    /// counter advances (a backpressure event), but `frames`/`samples` do **not** (the frame
    /// will be re-decoded and re-accepted next tick, so counting it now would double-count on
    /// the retry). This is the critical-requirement test (replaces the channel-full test).
    #[test]
    fn i2s_stream_sink_accept_returns_full_on_full_ring() {
        // Ring sized to exactly N 1-sample frames of *raw* storage: N × 2 bytes (design §3.1). Pick
        // N = 8 so the fill loop mirrors the prior channel-capacity-8 structure.
        const N: usize = 8;
        const FRAME_RAW_BYTES: usize = WIRE_BYTES_PER_SAMPLE; // one S16 sample, stored raw
        let (mut sink, consumer) = sink_with_ring(N * FRAME_RAW_BYTES);

        // 2-byte (1-sample) frames keep the test cheap; fill the ring exactly, then overflow.
        let pcm: [u8; 2] = [0x00, 0x00];
        for _ in 0..N {
            assert_eq!(
                sink.accept(&pcm),
                Accepted::Enqueued,
                "ring must accept up to capacity"
            );
        }
        assert_eq!(sink.full_stalls, 0, "no stall before the ring is full");
        assert_eq!(sink.frames as usize, N, "every written frame is counted");

        // One more must return Full (consumer never drains) — without blocking the call.
        assert_eq!(
            sink.accept(&pcm),
            Accepted::Full,
            "overflow frame must signal Full, not silently drop"
        );
        assert_eq!(
            sink.full_stalls, 1,
            "the ring-full event must count as a backpressure stall"
        );
        // The held frame is NOT counted as processed — it will be retried.
        assert_eq!(
            sink.frames as usize, N,
            "a Full frame must NOT advance frames (the caller holds and retries it)"
        );
        assert_eq!(
            sink.samples as usize, N,
            "a Full frame must NOT advance samples (no double-count on retry)"
        );

        // Draining one frame's worth frees space: the held frame is accepted on the next call,
        // advancing the counters exactly once (proving no frame was lost to fullness — the
        // re-arm rides the consumer's advance, design §2.4).
        let mut tx_buf = vec![0u8; super::INBOUND_PCM_WRITE_UNIT_BYTES];
        let run = consumer.copy_run_into(FRAME_RAW_BYTES, &mut tx_buf);
        assert_eq!(
            run.n, FRAME_RAW_BYTES,
            "one frame's raw bytes must be drainable"
        );
        consumer.advance(run.n);
        assert_eq!(
            sink.accept(&pcm),
            Accepted::Enqueued,
            "once space frees, the retried frame writes"
        );
        assert_eq!(
            sink.frames as usize,
            N + 1,
            "the retried frame is counted exactly once, on successful write"
        );
    }

    // ── send_stream_reset (ring reset) unit tests (design §2.8, §4) ─────────────────
    //
    // `send_stream_reset` now calls `InboundRingProducer::reset` instead of enqueuing a
    // channel `StreamReset` message. The reset is **infallible** — it bumps the ring's
    // `generation` under the lock, writing no audio bytes, so it cannot fail and returns unit; the
    // prior channel `Full`/`pending_reset` retry path and the `Disconnected` arm are gone (a ring
    // cannot hang up — design §6.2). The no-producer arm remains (dead-channel discard). The
    // load-bearing FIFO ordering — "boundary, then fresh audio", which the consumer observes via
    // the generation bump — is preserved by single-writer construction (design §2.8) and pinned
    // by `stream_reset_is_ordered_before_a_following_chunk` below.

    /// `send_stream_reset` bumps the ring `generation` exactly once and does not touch the
    /// audio-frame or backpressure counters (a reset is a boundary marker, not audio).
    #[test]
    fn send_stream_reset_bumps_generation_once() {
        let (producer, consumer) = InboundPcmRing::new(1024).split();
        let mut sink = I2sStreamSink::with_producer(Some(producer));
        let gen0 = consumer.generation();

        sink.send_stream_reset();
        let gen1 = consumer.generation();
        assert_ne!(
            gen1, gen0,
            "send_stream_reset must bump the ring generation exactly once"
        );

        // A reset moves no audio-frame counters and is neither a stall nor a discard.
        assert_eq!(sink.frames, 0, "a reset is not an audio frame");
        assert_eq!(sink.samples, 0, "a reset carries no samples");
        assert_eq!(sink.full_stalls, 0, "a reset is not a stall");
        assert_eq!(
            sink.dead_channel_discards, 0,
            "a successful reset is not a dead-channel discard"
        );
    }

    /// A reset against a **full** ring still succeeds (it writes no audio bytes, so fullness
    /// cannot block it) — the prior channel `Full` reset path is gone (design §2.8). The ring's
    /// buffered audio is untouched: the generation bump records `head_at_reset = head` but does
    /// not drop any bytes until the consumer applies the reset.
    #[test]
    fn send_stream_reset_succeeds_on_full_ring() {
        const N: usize = 8;
        const FRAME_RAW_BYTES: usize = WIRE_BYTES_PER_SAMPLE; // one S16 sample, stored raw (design §3.1)
        let (producer, consumer) = InboundPcmRing::new(N * FRAME_RAW_BYTES).split();
        let mut sink = I2sStreamSink::with_producer(Some(producer));

        // Fill the ring exactly (consumer never drains).
        let pcm: [u8; 2] = [0x00, 0x00];
        for _ in 0..N {
            assert_eq!(sink.accept(&pcm), Accepted::Enqueued);
        }
        assert_eq!(consumer.available(), N * FRAME_RAW_BYTES, "ring is full");
        let gen0 = consumer.generation();

        // A reset against a full ring must still succeed (it buffers no audio).
        sink.send_stream_reset();
        assert_ne!(
            consumer.generation(),
            gen0,
            "the reset bumped generation even when full"
        );
        assert_eq!(
            sink.full_stalls, 0,
            "a reset is never a full_stalls backpressure event"
        );
        assert_eq!(
            sink.dead_channel_discards, 0,
            "a reset on a full ring is not a dead-channel discard"
        );
    }

    /// A sink built with no producer wired (`with_producer(None)`) discards + counts the reset and
    /// reports `Enqueued` (forward progress) — the same no-producer data-loss contract `accept`
    /// uses. (There is no `Disconnected` arm: a ring cannot hang up, design §6.2, so the prior
    /// `send_stream_reset_discards_on_disconnected_channel` test is removed with its condition.)
    #[test]
    fn send_stream_reset_discards_with_no_producer() {
        let mut sink = I2sStreamSink::with_producer(None);
        sink.send_stream_reset();
        assert_eq!(
            sink.dead_channel_discards, 1,
            "no-producer reset must count as a dead-channel discard"
        );
        assert_eq!(
            sink.full_stalls, 0,
            "a no-producer reset is NOT a backpressure stall"
        );
    }

    /// Reset ordering (design §2.8, test-2): a reset at a (re)connection boundary is ordered
    /// **before** the new stream's first frame. The consumer, on observing the generation bump,
    /// drops the dead connection's un-read tail and the first bytes it plays after the reset are
    /// the fresh stream's. This is the load-bearing FIFO "boundary, then fresh audio" guarantee
    /// the whole reset mechanism rests on — now enforced by single-writer + generation rather than
    /// channel order. A regression that lost the ordering would replay stale audio after a
    /// reconnect; this test pins it as an explicit asset.
    #[test]
    fn stream_reset_is_ordered_before_a_following_chunk() {
        let (producer, consumer) = InboundPcmRing::new(1024).split();
        let mut sink = I2sStreamSink::with_producer(Some(producer));

        // Dead connection writes a frame A that is never drained (the stale tail).
        let a: [u8; 2] = [0x11, 0x11];
        assert_eq!(sink.accept(&a), Accepted::Enqueued);
        // Establishment order: reset the boundary, then write the new stream's first frame B.
        sink.send_stream_reset();
        let b: [u8; 2] = [0x22, 0x22];
        assert_eq!(sink.accept(&b), Accepted::Enqueued);

        // The consumer observes the boundary and applies it (drops A's stale tail), then the first
        // bytes it plays are B's expansion — not A's. This is the reset-then-fresh-audio ordering.
        let gen0 = 0u32;
        assert_ne!(
            consumer.generation(),
            gen0,
            "the boundary bumped generation"
        );
        // `apply_reset` must return the generation now in effect — the value a real consumer
        // records as `acted_generation` (test-4). If it returned the pre-bump generation, the
        // consumer would treat the very next drain as *another* fresh reset and loop; assert the
        // returned generation matches the current one so that off-by-one is caught here.
        assert_eq!(
            consumer.apply_reset(),
            consumer.generation(),
            "apply_reset returns the generation now in effect (the value to record as acted-on)"
        );

        let played = drain_all(&consumer);
        assert_eq!(
            played.len(),
            b.len(),
            "after the reset only the post-boundary frame B remains (A's tail was dropped)"
        );
        assert_eq!(
            &played[..],
            &b[..],
            "the first raw bytes after the reset are the fresh stream's frame B, not the stale A"
        );
    }

    // ── I2sStreamSink end_of_audio / flush_playback mapping (design §3.4/§3.5) ───────
    //
    // The `PlaybackSink` trait methods (default no-op) are overridden by `I2sStreamSink`
    // to drive the ring mark primitives: `end_of_audio` pushes a mark at the banked tail
    // so the mute fires when the tail finishes playing; `flush_playback` resets (discards the
    // banked tail, generation bump) then marks the emptied head so an empty-ring poll arms the
    // mute immediately. These tests pin the sink→producer wiring; the ring mechanics themselves
    // are pinned by the `ring_end_of_audio_*` / `ring_flush_shape_*` tests in the ring module.

    /// `end_of_audio` maps to `mark_end_of_audio`: the mark rides the banked audio (no generation
    /// bump — end-of-audio plays the tail out, unlike flush) and the drain run ending on it reports
    /// the boundary.
    #[test]
    fn i2s_sink_end_of_audio_marks_the_banked_tail() {
        let (mut sink, consumer) = sink_with_ring(1024);
        let gen0 = consumer.generation();

        // Bank 100 B of audio, then signal end-of-audio.
        let pcm = vec![0x33u8; 100];
        assert_eq!(sink.accept(&pcm), Accepted::Enqueued);
        sink.end_of_audio();

        assert_eq!(
            consumer.generation(),
            gen0,
            "end_of_audio does NOT reset the ring — the banked tail plays out (unlike flush)"
        );

        // The banked tail drains; the run ending on the mark reports the boundary exactly once.
        let mut tx = vec![0u8; super::INBOUND_PCM_WRITE_UNIT_BYTES];
        let run = consumer.copy_run_into(usize::MAX, &mut tx);
        assert_eq!(
            run.n, 100,
            "the banked tail drains up to the end-of-audio mark"
        );
        assert!(
            run.reached_end_of_audio,
            "end_of_audio pushed a mark at the banked tail's head"
        );
        consumer.advance(run.n);
        let run2 = consumer.copy_run_into(usize::MAX, &mut tx);
        assert_eq!(run2.n, 0, "ring drained after the tail");
        assert!(
            !run2.reached_end_of_audio,
            "the boundary fires exactly once"
        );
    }

    /// `flush_playback` maps to `reset()` then `mark_end_of_audio()`: the banked tail is discarded
    /// (generation bump → consumer `apply_reset` jumps `tail`), and the mark lands on the emptied
    /// head so `take_mark_at_tail` arms the mute immediately (design §3.5 barge-in).
    #[test]
    fn i2s_sink_flush_discards_tail_and_marks_emptied_head() {
        let (mut sink, consumer) = sink_with_ring(1024);
        let gen0 = consumer.generation();

        // Bank audio the flush must discard.
        let pcm = vec![0x44u8; 200];
        assert_eq!(sink.accept(&pcm), Accepted::Enqueued);
        sink.flush_playback();

        assert_ne!(
            consumer.generation(),
            gen0,
            "flush resets the ring generation (discards the banked tail)"
        );

        // The consumer applies the reset: `tail` jumps to the emptied head, the banked audio is gone.
        let _ = consumer.apply_reset();
        let mut tx = vec![0u8; super::INBOUND_PCM_WRITE_UNIT_BYTES];
        let run = consumer.copy_run_into(usize::MAX, &mut tx);
        assert_eq!(
            run.n, 0,
            "flush discarded the banked audio — the ring is empty"
        );
        assert!(
            !run.reached_end_of_audio,
            "the empty-ring flush mark is not surfaced through a run"
        );

        // The flush mark survived the reset (pushed in the new generation) and sits at the empty
        // head — observed via take_mark_at_tail, exactly once, so the mute arms immediately.
        assert!(
            consumer.take_mark_at_tail(),
            "flush arms the mute via an empty-ring mark at the emptied head"
        );
        assert!(
            !consumer.take_mark_at_tail(),
            "the flush boundary is consumed exactly once"
        );
    }

    /// With no producer wired, both control-frame overrides are silent no-ops: a control marker is
    /// not audio data, so a missing ring is NOT counted as a `dead_channel_discards` audio loss
    /// (the unwired ring is already surfaced by `accept`'s discard counter).
    #[test]
    fn i2s_sink_control_frames_no_op_without_producer() {
        let mut sink = I2sStreamSink::with_producer(None);
        sink.end_of_audio();
        sink.flush_playback();
        assert_eq!(
            sink.dead_channel_discards, 0,
            "control frames are not audio frames — a missing ring is not an audio-loss discard"
        );
        assert_eq!(
            sink.full_stalls, 0,
            "control frames are not backpressure stalls"
        );
    }

    /// `is_drop_burst` (§2.3) fires on **any** drop in a window (`> 0`) and not on zero —
    /// the "log everything, threshold tight" resolution. A single dropped chunk is a real
    /// lost ~20 ms of audio and must surface as an event; zero drops must stay quiet so a
    /// clean window emits no burst warn.
    #[test]
    fn is_drop_burst_fires_on_any_nonzero_delta() {
        assert!(
            !is_drop_burst(0),
            "a clean window (0 drops) must not be a burst"
        );
        assert!(is_drop_burst(1), "a single dropped chunk is a burst event");
        assert!(is_drop_burst(2), "multiple drops in a window are a burst");
        assert!(is_drop_burst(u32::MAX), "a large delta is still a burst");
    }

    /// `preroll_gate_ready` (design §2.2, §2.6, §4): the gate-decision matrix.
    ///
    /// Below target before the fallback timeout → not ready (keep filling); at/above target →
    /// ready (cushion buffered); below target after the timeout → ready (fallback, so a short
    /// stream still plays); no chunk yet (`first_chunk_elapsed_ms = None`) → not ready (the
    /// fallback clock has not started — an idle connection waits in pre-roll). Uses the real
    /// `PLAYBACK_PREROLL_TARGET_BYTES` as `target` (the predicate is unit-agnostic — `buffered`
    /// and `target` are now ring fill *bytes*, not chunk counts, §2.9) and a fixed 500 ms fallback
    /// (the recommended `PLAYBACK_PREROLL_MAX_WAIT_MS`, which lives in `speaker.rs`).
    #[test]
    fn preroll_gate_ready_decision_matrix() {
        let target = PLAYBACK_PREROLL_TARGET_BYTES;
        let max_wait_ms = 500;

        // Below target, before the timeout (and with the clock running) → keep filling.
        assert!(
            !preroll_gate_ready(target - 1, target, Some(0), max_wait_ms),
            "below target with the fallback clock just started must not clear"
        );
        assert!(
            !preroll_gate_ready(target - 1, target, Some(max_wait_ms - 1), max_wait_ms),
            "below target one ms before the fallback deadline must not clear"
        );

        // At/above target → ready regardless of elapsed (even with no chunk-clock).
        assert!(
            preroll_gate_ready(target, target, Some(0), max_wait_ms),
            "exactly at target must clear (fill reached)"
        );
        assert!(
            preroll_gate_ready(target + 1, target, None, max_wait_ms),
            "above target must clear on fill even before the fallback clock starts"
        );

        // Below target, at/after the timeout → fallback clears so a short stream still plays.
        assert!(
            preroll_gate_ready(target - 1, target, Some(max_wait_ms), max_wait_ms),
            "below target at the fallback deadline must clear (fallback)"
        );
        assert!(
            preroll_gate_ready(1, target, Some(max_wait_ms + 100), max_wait_ms),
            "a short snippet past the fallback deadline must clear and play what is buffered"
        );

        // No chunk has arrived yet → the fallback clock has not started → never clear.
        assert!(
            !preroll_gate_ready(0, target, None, max_wait_ms),
            "no chunk yet (clock not started) must wait in pre-roll, not clear"
        );

        // Degenerate `target = 0` (not used in production — the const is 7_680 bytes — but the
        // signature permits it). The fill-reached arm returns `true` when `buffered >= target`, so `0 >= 0`
        // clears immediately *even with no chunk* (test-1). Production never passes 0; pinning the
        // behavior documents that the gate's "wait for the first chunk" property comes from
        // `target >= 1`, not from the predicate itself.
        assert!(
            preroll_gate_ready(0, 0, None, max_wait_ms),
            "target=0 clears immediately on the fill-reached arm (0 >= 0), even with no chunk"
        );

        // Zero-buffered fallback (test-3): the clock started, the deadline passed, but no chunk is
        // currently counted. Degenerate in production (the chunk that started the clock is normally
        // still held), but the predicate must still clear on the fallback arm so a sparse stream
        // always makes forward progress rather than wedging.
        assert!(
            preroll_gate_ready(0, target, Some(max_wait_ms), max_wait_ms),
            "zero-buffered past the fallback deadline must still clear (forward progress)"
        );
    }

    /// Driving `accept` against a non-draining (full) ring produces a non-zero per-window
    /// `full_stalls` delta — the condition `is_drop_burst` recognizes, now signalling
    /// *backpressure* rather than drops (§2.3, §5, design §2.4). Each overflow `accept`
    /// returns `Full` (the caller would hold and retry), so under this test's no-retry
    /// driving the held frames are never counted as processed: `frames`/`samples` advance
    /// only for the capacity-many writes, and every overflow call increments `full_stalls`.
    #[test]
    fn full_ring_window_produces_full_stall_signal() {
        // Ring sized to exactly `RING_FRAMES` 1-sample raw frames (2 B each, design §3.1) so the fill
        // loop mirrors the prior capacity-8 structure; overflow calls past that return `Full`.
        const RING_FRAMES: usize = 8;
        let (mut sink, _consumer) = sink_with_ring(RING_FRAMES * 2);

        // Fill the ring (each `Enqueued`); every call after that returns `Full` (consumer never
        // drains). Drive enough calls to outlast the ring capacity so stalls accumulate.
        let pcm: [u8; 2] = [0x00, 0x00];
        let total_calls = (PLAYBACK_LOG_CADENCE_FRAMES + 1) as usize;
        assert!(
            total_calls > RING_FRAMES,
            "must drive past ring capacity so stalls occur"
        );
        for i in 0..total_calls {
            let r = sink.accept(&pcm);
            if i < RING_FRAMES {
                assert_eq!(r, Accepted::Enqueued, "fill writes up to capacity");
            } else {
                assert_eq!(
                    r,
                    Accepted::Full,
                    "overflow calls signal Full (backpressure)"
                );
            }
        }

        // Only the capacity-many written frames are counted as processed; the held (Full)
        // frames are not (the caller owns the retry). This is the backpressure contract:
        // no frame is dropped, so processed-frame counters reflect only forward progress.
        assert_eq!(
            sink.frames as usize, RING_FRAMES,
            "only written frames advance frames (Full frames are held, not processed)"
        );
        assert_eq!(
            sink.samples as usize, RING_FRAMES,
            "only written frames advance samples"
        );
        let expected_stalls = total_calls - RING_FRAMES;
        assert_eq!(
            sink.full_stalls as usize, expected_stalls,
            "every overflow call counts as a backpressure stall"
        );

        // The per-window stall delta the burst detector computes is non-zero, so the burst
        // predicate fires — the timestamped backpressure `warn!` would be emitted on the
        // periodic tick. We assert the *predicate* directly (the unit under test is the
        // stall-delta signal; the tick cadence is covered by the LogCountdown tests and the
        // dedicated `full_stalls_tick_the_periodic_log_under_sustained_backpressure` test).
        let window_stall_delta = sink.full_stalls.wrapping_sub(sink.full_stalls_at_last_log);
        assert!(
            is_drop_burst(window_stall_delta),
            "a window with ring-full stalls must be recognized as a burst"
        );

        // Simulate the periodic-emit bookkeeping: snapshot the running total, then assert a
        // following window with no new stalls reports a zero delta (no spurious burst).
        sink.full_stalls_at_last_log = sink.full_stalls;
        let next_window_delta = sink.full_stalls.wrapping_sub(sink.full_stalls_at_last_log);
        assert!(
            !is_drop_burst(next_window_delta),
            "a window with no new stalls must not fire a burst"
        );
    }

    // ── consumer-stall watchdog (design §6.2(b)) ───────────────────────────────────
    //
    // A ring has no `Disconnected` state, so a wedged consumer (capture/DAC thread) shows up as a
    // *full ring whose `tail` never advances*, not as data loss. The watchdog in `accept`'s `Full`
    // arm (`note_consumer_stall`) restores the channel design's "DAC behind (healthy) vs. consumer
    // dead (wedged)" distinction: it warns once, edge-triggered, after
    // `PLAYBACK_CONSUMER_STALL_WARN_STALLS` consecutive stalls with no `tail` progress, and re-arms
    // only when `tail` advances. The `warn!` text is not assertable in a unit test, so these tests
    // assert on the observable watchdog state (`stalls_since_tail_advanced`, `consumer_stall_warned`).

    /// A wedged consumer (full ring, `tail` frozen) trips the watchdog exactly once at the
    /// threshold and then stays latched — the warn is one-shot per stall episode, not per held frame.
    #[test]
    fn consumer_stall_watchdog_warns_once_when_tail_frozen() {
        const RING_FRAMES: usize = 8;
        // Consumer is never advanced, so `tail` stays at 0 — the wedged-consumer case. 2 B/frame raw.
        let (mut sink, _consumer) = sink_with_ring(RING_FRAMES * 2);
        let pcm: [u8; 2] = [0x00, 0x00];
        for _ in 0..RING_FRAMES {
            assert_eq!(sink.accept(&pcm), Accepted::Enqueued, "fill to capacity");
        }
        assert_eq!(
            sink.stalls_since_tail_advanced, 0,
            "no stalls counted before the ring is full"
        );
        assert!(!sink.consumer_stall_warned, "not warned before any stall");

        // Stall up to one short of the threshold: counting, but not yet warned.
        for _ in 0..(PLAYBACK_CONSUMER_STALL_WARN_STALLS - 1) {
            assert_eq!(sink.accept(&pcm), Accepted::Full, "ring stays full");
        }
        assert_eq!(
            sink.stalls_since_tail_advanced,
            PLAYBACK_CONSUMER_STALL_WARN_STALLS - 1,
            "consecutive frozen-tail stalls accumulate"
        );
        assert!(
            !sink.consumer_stall_warned,
            "watchdog must not warn before the threshold"
        );

        // The threshold stall trips the one-shot warn latch.
        assert_eq!(sink.accept(&pcm), Accepted::Full);
        assert_eq!(
            sink.stalls_since_tail_advanced, PLAYBACK_CONSUMER_STALL_WARN_STALLS,
            "the threshold stall is counted"
        );
        assert!(
            sink.consumer_stall_warned,
            "watchdog must warn on crossing the threshold"
        );

        // Further stalls keep the latch set (one-shot) — no re-warn per held frame.
        sink.accept(&pcm);
        sink.accept(&pcm);
        assert!(
            sink.consumer_stall_warned,
            "warn stays latched under continued stall (one-shot per episode)"
        );
        assert!(
            sink.stalls_since_tail_advanced > PLAYBACK_CONSUMER_STALL_WARN_STALLS,
            "the stall counter keeps climbing while wedged"
        );
    }

    /// A latched watchdog must re-arm when room reappears and a write *succeeds* — the rearm at
    /// the successful-write site in `accept` (test-2), distinct from the `tail`-advance rearm in
    /// `note_consumer_stall`. After a stall episode latches the warn, the consumer drains, and the
    /// next `accept` finds room and enqueues: the success arm must clear `consumer_stall_warned`
    /// and `stalls_since_tail_advanced` so a *subsequent* fresh stall episode warns again rather
    /// than staying silently latched. A regression that dropped the success-arm clear would leave
    /// the warn permanently latched after one episode even on a healthy consumer.
    #[test]
    fn consumer_stall_watchdog_rearms_after_successful_write() {
        const RING_FRAMES: usize = 8;
        let (mut sink, consumer) = sink_with_ring(RING_FRAMES * 2); // 2 B/frame raw (design §3.1)
        let pcm: [u8; 2] = [0x00, 0x00];
        // Fill the ring, then stall past the threshold so the watchdog latches.
        for _ in 0..RING_FRAMES {
            assert_eq!(sink.accept(&pcm), Accepted::Enqueued, "fill to capacity");
        }
        for _ in 0..PLAYBACK_CONSUMER_STALL_WARN_STALLS {
            assert_eq!(
                sink.accept(&pcm),
                Accepted::Full,
                "ring stays full while wedged"
            );
        }
        assert!(
            sink.consumer_stall_warned,
            "precondition: the watchdog latched after the threshold stall episode"
        );
        assert!(
            sink.stalls_since_tail_advanced >= PLAYBACK_CONSUMER_STALL_WARN_STALLS,
            "precondition: the stall counter climbed to the threshold"
        );

        // The consumer drains one frame, freeing room. The held frame's next `accept` now finds
        // room and *succeeds* (rather than hitting Full again) — the success-arm rearm path.
        let mut tx_buf = vec![0u8; super::INBOUND_PCM_WRITE_UNIT_BYTES];
        let run = consumer.copy_run_into(2, &mut tx_buf); // one raw frame
        consumer.advance(run.n);
        assert_eq!(
            sink.accept(&pcm),
            Accepted::Enqueued,
            "freed space accepts — the successful-write rearm site runs"
        );

        assert!(
            !sink.consumer_stall_warned,
            "a successful write after a latched stall must re-arm the watchdog warn"
        );
        assert_eq!(
            sink.stalls_since_tail_advanced, 0,
            "a successful write must reset the consecutive-stall counter"
        );
    }

    /// Healthy backpressure — the consumer keeps draining (`tail` advances) — must never trip the
    /// watchdog: each advancing-`tail` stall resets the counter and re-arms the edge.
    #[test]
    fn consumer_stall_watchdog_resets_when_tail_advances() {
        const RING_FRAMES: usize = 8;
        let (mut sink, consumer) = sink_with_ring(RING_FRAMES * 2); // 2 B/frame raw (design §3.1)
        let pcm: [u8; 2] = [0x00, 0x00];
        for _ in 0..RING_FRAMES {
            assert_eq!(sink.accept(&pcm), Accepted::Enqueued, "fill to capacity");
        }

        let mut tx_buf = vec![0u8; super::INBOUND_PCM_WRITE_UNIT_BYTES];
        // Drive far more stalls than the threshold, but drain one frame between each so `tail`
        // advances every time — the DAC-merely-behind case. The watchdog compares each stall's
        // `tail` against the previously-observed one, so a stall whose `tail` advanced resets the
        // counter; the counter therefore oscillates between at most 1 (a fresh stall whose `tail`
        // matches the last observation) and 0 (the next stall, after the intervening drain advanced
        // `tail`) — it never climbs toward the threshold, so the watchdog never latches.
        for _ in 0..(PLAYBACK_CONSUMER_STALL_WARN_STALLS * 3) {
            // Ring is full → this stalls and runs the watchdog.
            assert_eq!(sink.accept(&pcm), Accepted::Full, "ring is full");
            assert!(
                !sink.consumer_stall_warned,
                "advancing tail (healthy backpressure) must never trip the consumer-stall warn"
            );
            assert!(
                sink.stalls_since_tail_advanced <= 1,
                "an advancing tail keeps the stall counter from climbing toward the threshold \
                 (it is reset on every stall that follows a drain)"
            );
            // Consumer drains one frame and the held frame is re-accepted, advancing `tail`.
            let run = consumer.copy_run_into(2, &mut tx_buf); // one raw frame
            consumer.advance(run.n);
            assert_eq!(sink.accept(&pcm), Accepted::Enqueued, "freed space accepts");
        }
        assert!(
            !sink.consumer_stall_warned,
            "the watchdog never warns while the consumer is making progress"
        );
        assert!(
            sink.stalls_since_tail_advanced < PLAYBACK_CONSUMER_STALL_WARN_STALLS,
            "the stall counter must stay well below the threshold under healthy backpressure"
        );
    }

    /// The periodic log must keep ticking under *sustained* backpressure (quality-2): every
    /// `accept` returning `Full` still advances `log_countdown`, so the periodic `info!` /
    /// burst `warn!` fire during exactly the overload they exist to surface — they are not
    /// gated behind the enqueued path. With the bug, a ring saturated for the whole window
    /// would emit no log at all (`full_stalls_at_last_log` would never update).
    #[test]
    fn full_stalls_tick_the_periodic_log_under_sustained_backpressure() {
        // Fill the ring first so every subsequent `accept` returns `Full`.
        const RING_FRAMES: usize = 8;
        let (mut sink, _consumer) = sink_with_ring(RING_FRAMES * 2); // 2 B/frame raw (design §3.1)
        let pcm: [u8; 2] = [0x00, 0x00];
        for _ in 0..RING_FRAMES {
            assert_eq!(sink.accept(&pcm), Accepted::Enqueued);
        }

        // The very first `accept` ticked the countdown once on construction's first call above,
        // updating `full_stalls_at_last_log` to the then-current `full_stalls` (0). Now drive a
        // full cadence-worth of *Full* returns: the countdown must fire again from the `Full`
        // path, snapshotting the now-nonzero `full_stalls` into `full_stalls_at_last_log`.
        let mut last_log_snapshot = sink.full_stalls_at_last_log;
        let mut fired = false;
        for _ in 0..(PLAYBACK_LOG_CADENCE_FRAMES + 2) {
            assert_eq!(sink.accept(&pcm), Accepted::Full, "ring stays saturated");
            if sink.full_stalls_at_last_log != last_log_snapshot {
                fired = true;
                last_log_snapshot = sink.full_stalls_at_last_log;
            }
        }
        assert!(
            fired,
            "the periodic log must fire from the Full path under sustained backpressure"
        );
        assert!(
            last_log_snapshot > 0,
            "the snapshotted full_stalls at the firing tick must reflect the accumulated stalls"
        );
    }

    /// `LogCountdown::tick` fires on the very first call, then once every `cadence + 1`
    /// calls thereafter — the verified behavior of the shipped `tick` (which reloads
    /// `remaining = cadence` on a fire and decrements to 0 over the next `cadence`
    /// calls, so the next fire lands `cadence + 1` calls later). This is faithful to
    /// the pre-refactor hand-rolled `if x == 0 { …; = cadence } else { -= 1 }` sites
    /// (design §6.5, resolution A — the prior tests asserted the wrong period and never
    /// ran because the device crate's tests are only compile-checked).
    #[test]
    fn log_countdown_fires_first_then_every_cadence() {
        let mut lc = LogCountdown::new(3);
        // Fires immediately on the first tick (remaining starts at 0).
        assert!(lc.tick(), "first tick must fire");
        // Then suppressed for `cadence` calls (period is cadence + 1)...
        assert!(!lc.tick(), "tick 2 suppressed");
        assert!(!lc.tick(), "tick 3 suppressed");
        assert!(!lc.tick(), "tick 4 suppressed");
        // ...and fires again on the `cadence + 1`th call after the last fire.
        assert!(lc.tick(), "tick 5 fires (cadence + 1 elapsed)");
        assert!(!lc.tick(), "tick 6 suppressed");
        assert!(!lc.tick(), "tick 7 suppressed");
        assert!(!lc.tick(), "tick 8 suppressed");
        assert!(lc.tick(), "tick 9 fires");
    }

    /// A cadence of 1 fires every other call: it fires on call 1, then reloads
    /// `remaining = 1`, which takes one decrement to reach 0, so the next fire is on
    /// call 3 (period cadence + 1 = 2). The verified shipped behavior (design §6.5,
    /// resolution A — the prior test wrongly asserted a fire on *every* call).
    #[test]
    fn log_countdown_cadence_one_fires_every_other_call() {
        let mut lc = LogCountdown::new(1);
        // Fires: 1, 3, 5, ...  Suppressed: 2, 4, ...
        let expected = [true, false, true, false, true, false];
        for (call, &want) in expected.iter().enumerate() {
            assert_eq!(
                lc.tick(),
                want,
                "cadence=1 must fire every other call; call {} expected {want}",
                call + 1
            );
        }
    }

    /// Degenerate cadence=0: `remaining` reloads to 0 on every fire, so every call fires
    /// (period 1). Unused in production (`PLAYBACK_LOG_CADENCE_FRAMES = 50`) but a
    /// guaranteed property of the implementation; pinning it catches a silent change from
    /// an "obvious cleanup" (e.g. an `assert!(cadence > 0)` or a reload-to-`cadence - 1`
    /// off-by-one fix) (test-7).
    #[test]
    fn log_countdown_cadence_zero_fires_every_call() {
        let mut lc = LogCountdown::new(0);
        for call in 1..=4 {
            assert!(lc.tick(), "cadence=0 must fire on every call (call {call})");
        }
    }

    // ── InboundPcmRing — the SPSC byte ring (design §2, test plan §4) ─────────────────────
    //
    // These exercise the ring data structure in isolation: write/read byte-exactness, wrap-around
    // split correctness, ring-full backpressure + re-arm, and the reset/generation boundary. They do
    // NOT exercise `I2sStreamSink`/the capture thread (that wiring is a later increment).

    use super::{
        DrainRun, InboundRingProducer, INBOUND_PCM_RING_BYTES, INBOUND_PCM_WRITE_UNIT_BYTES,
        PLAYBACK_PREROLL_TARGET_BYTES, RING_EOA_MARK_CAP,
    };

    /// Write `bytes` into the ring through the producer, returning whether it fit. The `fill`
    /// closure copies the matching sub-slice of `bytes` at the given logical `offset`, so a
    /// wrap-split write reassembles correctly (mirrors `accept` expanding straight into the ring).
    fn ring_write(producer: &InboundRingProducer, bytes: &[u8]) -> bool {
        producer.write(bytes.len(), |offset, dst| {
            dst.copy_from_slice(&bytes[offset..offset + dst.len()]);
        })
    }

    /// Drain up to `max` bytes out of the ring via repeated `copy_run_into` → `advance`, collecting
    /// the bytes in read order — the consumer's normal drain path minus the real `write_all`.
    /// Returns the collected bytes and the last `generation` observed.
    fn ring_drain(consumer: &InboundRingConsumer, max: usize) -> (Vec<u8>, u32) {
        let mut out = Vec::new();
        let mut tx_buf = vec![0u8; INBOUND_PCM_WRITE_UNIT_BYTES];
        let mut last_gen = 0;
        while out.len() < max {
            let DrainRun { n, generation, .. } =
                consumer.copy_run_into(max - out.len(), &mut tx_buf);
            last_gen = generation;
            if n == 0 {
                break;
            }
            out.extend_from_slice(&tx_buf[..n]);
            consumer.advance(n);
        }
        (out, last_gen)
    }

    /// Distinct, position-dependent byte pattern so a dropped/duplicated/misordered byte is caught.
    fn pattern(start: usize, len: usize) -> Vec<u8> {
        (0..len).map(|i| (start + i) as u8).collect()
    }

    /// Round-trip byte-exactness: bytes written through the producer come back through the consumer
    /// identical and in order (test plan §4 test 1 — the expansion-correctness assertion, here at the
    /// ring level: the byte sequence is preserved exactly).
    #[test]
    fn ring_round_trip_byte_exact() {
        let (producer, consumer) = InboundPcmRing::new(1024).split();
        let frame = pattern(0, 320); // arbitrary < cap byte run
        assert!(
            ring_write(&producer, &frame),
            "frame must fit an empty ring"
        );
        let (got, _) = ring_drain(&consumer, frame.len());
        assert_eq!(got, frame, "round-trip must preserve every byte in order");
    }

    /// Cloned producers share one ring (design OQ §6.3 option (a)): a clone of the producer writes
    /// into the **same** backing ring as the original — both writes are `Mutex`-serialized and the
    /// consumer reads them all in order. This is the property that lets the HIL handlers inject into
    /// the production ring alongside the live streamer instead of standing up an isolated test ring.
    #[test]
    fn ring_cloned_producer_shares_ring() {
        let (producer, consumer) = InboundPcmRing::new(1024).split();
        let producer2 = producer.clone();
        let first = pattern(0, 96);
        let second = pattern(96, 96);
        assert!(ring_write(&producer, &first), "first write must fit");
        assert!(
            ring_write(&producer2, &second),
            "the clone writes into the same ring's free space"
        );
        let (got, _) = ring_drain(&consumer, first.len() + second.len());
        let mut expected = first.clone();
        expected.extend_from_slice(&second);
        assert_eq!(
            got, expected,
            "both producer handles feed one ring; consumer reads both runs in write order"
        );
        // The clone observes the consumer's progress through the same shared state.
        assert_eq!(
            producer2.consumed(),
            (first.len() + second.len()) as u32,
            "the clone sees the shared `tail` advance"
        );
    }

    /// Empty/full distinction via the wrapping counters: a fresh ring is empty (`available == 0`,
    /// `free_total == cap`); filling it exactly to `cap` reports `available == cap` / `free_total ==
    /// 0` (full), with no spare-slot ambiguity (design §2.6).
    #[test]
    fn ring_empty_full_distinction() {
        let cap = 256;
        let (producer, consumer) = InboundPcmRing::new(cap).split();
        assert_eq!(consumer.available(), 0, "fresh ring is empty");
        assert_eq!(producer.free_total(), cap, "fresh ring is all free");

        assert!(
            ring_write(&producer, &pattern(0, cap)),
            "exactly cap must fit"
        );
        assert_eq!(
            consumer.available(),
            cap,
            "ring filled to cap reads as full"
        );
        assert_eq!(
            producer.free_total(),
            0,
            "no free space at cap (no spare slot)"
        );
    }

    /// Wrap-around correctness (test plan §4 test 2 — the structural test the ring most needs).
    /// Advance `head`/`tail` past `cap` several times with frames that straddle the wrap boundary;
    /// assert byte-exactness across every seam (producer split-copy and consumer split-read both land
    /// correctly).
    #[test]
    fn ring_wrap_byte_exact() {
        let cap = 300;
        let (producer, consumer) = InboundPcmRing::new(cap).split();
        // 200-byte frames against a 300-byte ring force a wrap on every other write; loop enough to
        // cross the boundary many times. A continuous counter as the payload catches any seam error.
        let frame_len = 200;
        let mut next = 0usize;
        for _ in 0..20 {
            let frame = pattern(next, frame_len);
            assert!(
                ring_write(&producer, &frame),
                "frame must fit (drained each round)"
            );
            let (got, _) = ring_drain(&consumer, frame_len);
            assert_eq!(
                got, frame,
                "wrap-split round-trip must be byte-exact at offset {next}"
            );
            next += frame_len;
        }
    }

    /// A single write whose run straddles the `cap` boundary is reassembled byte-exact (the producer
    /// split-copy + consumer split-read seam, isolated).
    #[test]
    fn ring_single_straddling_write() {
        let cap = 256;
        let (producer, consumer) = InboundPcmRing::new(cap).split();
        // Push tail+head to near the end of the buffer, then write a frame that wraps the seam.
        let pad = pattern(0, 200);
        assert!(ring_write(&producer, &pad));
        let (drained, _) = ring_drain(&consumer, 200);
        assert_eq!(drained, pad);
        // tail == head == 200; a 100-byte frame now occupies [200..256) + [0..44) — a wrap.
        let frame = pattern(1000, 100);
        assert!(ring_write(&producer, &frame), "straddling frame must fit");
        let (got, _) = ring_drain(&consumer, 100);
        assert_eq!(
            got, frame,
            "straddling write must reassemble byte-exact across the seam"
        );
    }

    /// Ring-full → write returns `false`, nothing written, `free_total` unchanged, bytes not consumed
    /// (test plan §4 test 3 — the critical backpressure requirement at the ring level). The caller's
    /// frame stays buffered upstream (the `Accepted::Full` cause).
    #[test]
    fn ring_full_rejects_without_writing() {
        let cap = 256;
        let (producer, consumer) = InboundPcmRing::new(cap).split();
        // Fill to within < one frame of cap.
        let filled = pattern(0, cap - 100);
        assert!(ring_write(&producer, &filled));
        let free_before = producer.free_total();
        assert_eq!(free_before, 100);

        // A 150-byte frame does not fit the 100 free bytes.
        let too_big = pattern(9000, 150);
        assert!(
            !ring_write(&producer, &too_big),
            "over-capacity frame must be rejected"
        );
        assert_eq!(
            producer.free_total(),
            free_before,
            "rejected write must not change free space"
        );
        assert_eq!(
            consumer.available(),
            cap - 100,
            "rejected write must not enqueue bytes"
        );

        // And what IS in the ring is exactly `filled` — the rejected bytes never landed.
        let (got, _) = ring_drain(&consumer, cap - 100);
        assert_eq!(
            got, filled,
            "only the pre-fill bytes are present; the rejected frame is absent"
        );
    }

    /// Ring-full re-arm (test plan §4 test 4): after a rejected write, the consumer advancing `tail`
    /// frees space the producer's next `write` observes, so the retry succeeds.
    #[test]
    fn ring_full_rearm_after_advance() {
        let cap = 256;
        let (producer, consumer) = InboundPcmRing::new(cap).split();
        assert!(
            ring_write(&producer, &pattern(0, cap)),
            "fill exactly to cap"
        );
        assert!(
            !ring_write(&producer, &pattern(0, 64)),
            "full ring rejects further writes"
        );

        // Consumer drains 64 bytes (one copy_run_into + advance), freeing exactly that much.
        let mut tx_buf = vec![0u8; INBOUND_PCM_WRITE_UNIT_BYTES];
        let run = consumer.copy_run_into(64, &mut tx_buf);
        assert_eq!(run.n, 64);
        consumer.advance(run.n);

        // The producer now sees room and the retry succeeds.
        assert_eq!(
            producer.free_total(),
            64,
            "advance freed exactly the drained bytes"
        );
        assert!(
            ring_write(&producer, &pattern(0, 64)),
            "retry succeeds once space is freed"
        );
    }

    /// `reset()` + generation boundary (test plan §4 test 6, ring level): the consumer observes the
    /// bumped `generation`, applies the reset (jumps `tail` to `head_at_reset`, dropping the stale
    /// tail), and then plays the post-reset bytes — not the dropped ones.
    #[test]
    fn ring_reset_drops_stale_tail() {
        let cap = 1024;
        let (producer, consumer) = InboundPcmRing::new(cap).split();
        let gen0 = consumer.generation();

        // Frame A is written but never drained — it is the dead connection's stale tail.
        let a = pattern(0, 200);
        assert!(ring_write(&producer, &a));
        // Reconnection boundary: producer resets, then writes frame B for the fresh stream.
        producer.reset();
        let b = pattern(5000, 200);
        assert!(ring_write(&producer, &b));

        // Consumer observes the generation change and applies the reset before draining.
        assert_ne!(consumer.generation(), gen0, "reset must bump generation");
        let g = consumer.apply_reset();
        assert_eq!(
            g,
            consumer.generation(),
            "apply_reset returns the generation now in effect"
        );

        // After the reset jump, the consumer plays B (the fresh stream), never A's stale tail.
        let (got, _) = ring_drain(&consumer, 200);
        assert_eq!(
            got, b,
            "after reset the consumer plays the post-reset frame, not the stale tail"
        );
        assert_eq!(
            consumer.available(),
            0,
            "no leftover bytes — A's tail was dropped"
        );
    }

    /// Back-to-back resets: the consumer jumps to the *latest* stream start, discarding any
    /// intermediate dead-connection tail (design §3 "Back-to-back resets").
    #[test]
    fn ring_back_to_back_resets_jump_to_latest() {
        let cap = 1024;
        let (producer, consumer) = InboundPcmRing::new(cap).split();
        assert!(ring_write(&producer, &pattern(0, 100))); // stream 1 (dead)
        producer.reset();
        assert!(ring_write(&producer, &pattern(200, 100))); // stream 2 (also dead)
        producer.reset();
        let c = pattern(900, 100); // stream 3 (live)
        assert!(ring_write(&producer, &c));

        consumer.apply_reset();
        let (got, _) = ring_drain(&consumer, 100);
        assert_eq!(got, c, "consumer jumps to the latest reset's stream start");
        assert_eq!(
            consumer.available(),
            0,
            "both dead-connection tails dropped"
        );
    }

    /// `copy_run_into` caps the run at the wrap boundary so each returned slice is contiguous: a
    /// readable region spanning the seam comes back as two runs, not one (design §2.5).
    #[test]
    fn ring_copy_run_caps_at_wrap_boundary() {
        let cap = 256;
        let (producer, consumer) = InboundPcmRing::new(cap).split();
        // Advance to put the read cursor at offset 200, then buffer 100 bytes → readable region
        // [200..256) + [0..44), straddling the seam.
        assert!(ring_write(&producer, &pattern(0, 200)));
        let (_pad, _) = ring_drain(&consumer, 200);
        let frame = pattern(0, 100);
        assert!(ring_write(&producer, &frame));

        let mut tx_buf = vec![0u8; INBOUND_PCM_WRITE_UNIT_BYTES];
        // First run is capped at the wrap boundary: only the 56 bytes in [200..256).
        let run1 = consumer.copy_run_into(100, &mut tx_buf);
        assert_eq!(
            run1.n, 56,
            "first run stops at the wrap boundary (256 - 200)"
        );
        consumer.advance(run1.n);
        // Second run returns the wrapped remainder.
        let run2 = consumer.copy_run_into(100, &mut tx_buf);
        assert_eq!(run2.n, 44, "second run returns the post-wrap remainder");
    }

    /// `copy_run_into` honors the per-pass byte budget (`max_run`) and the `tx_buf` length cap, so the
    /// consumer never returns more than one write-unit / one pass-budget per call (design §2.5).
    #[test]
    fn ring_copy_run_honors_caps() {
        let cap = 8192;
        let (producer, consumer) = InboundPcmRing::new(cap).split();
        assert!(ring_write(&producer, &pattern(0, 5000)));

        let mut tx_buf = vec![0u8; INBOUND_PCM_WRITE_UNIT_BYTES];
        // tx_buf length caps the run to one write-unit even though `max_run` is larger.
        let run = consumer.copy_run_into(5000, &mut tx_buf);
        assert_eq!(
            run.n, INBOUND_PCM_WRITE_UNIT_BYTES,
            "run capped at the tx_buf write-unit length"
        );

        // `max_run` caps below the write-unit and below availability.
        consumer.advance(run.n);
        let run2 = consumer.copy_run_into(100, &mut tx_buf);
        assert_eq!(run2.n, 100, "run capped at the per-pass max_run budget");
    }

    // ── end-of-audio marks (design §3.4/§3.5, test plan §5) ───────────────────────────────
    //
    // The mute decision must fire when the *banked tail finishes playing*, so an
    // `EndOfAudio`/`FlushPlayback` boundary rides the ring as a mark at the head-of-write position.
    // `copy_run_into` caps a run to end exactly on the next mark and reports `reached_end_of_audio`;
    // `advance` pops the mark once `tail` reaches it (so each boundary reports once); marks that sit
    // on an empty ring (`head == tail`) are observed via `take_mark_at_tail` instead; `apply_reset`
    // discards marks from a superseded generation while keeping a flush's live mark. These tests
    // exercise the ring mechanism only — the `I2sStreamSink` mapping and the
    // capture-thread drain-loop consumption of these signals are separate.

    /// A mark riding banked audio caps the drain run to end exactly on it, reports the boundary, and
    /// is consumed exactly once (`advance` pops it as `tail` reaches it) — the post-mark audio then
    /// drains with no further boundary.
    #[test]
    fn ring_end_of_audio_mark_caps_run_and_reports_once() {
        let (producer, consumer) = InboundPcmRing::new(1024).split();
        // Bank 100 B, mark end-of-audio at head (100), then bank 40 B more (head = 140).
        assert!(ring_write(&producer, &pattern(0, 100)));
        producer.mark_end_of_audio();
        assert!(ring_write(&producer, &pattern(100, 40)));

        let mut tx = vec![0u8; INBOUND_PCM_WRITE_UNIT_BYTES];
        // First run is capped to end exactly on the mark (100 B) and reports the boundary.
        let run = consumer.copy_run_into(usize::MAX, &mut tx);
        assert_eq!(run.n, 100, "run capped to end on the end-of-audio mark");
        assert!(
            run.reached_end_of_audio,
            "the run ending on the mark reports the boundary"
        );
        consumer.advance(run.n);

        // The mark is consumed exactly once: the post-mark bytes drain with no further boundary.
        let run2 = consumer.copy_run_into(usize::MAX, &mut tx);
        assert_eq!(run2.n, 40, "the remaining post-mark audio drains");
        assert!(
            !run2.reached_end_of_audio,
            "each boundary fires exactly once — advance popped the mark"
        );
        consumer.advance(run2.n);

        let run3 = consumer.copy_run_into(usize::MAX, &mut tx);
        assert_eq!(run3.n, 0, "ring drained");
        assert!(!run3.reached_end_of_audio);
    }

    /// A mark beyond the current run's byte budget is not reported until a later pass reaches it: a
    /// short run (capped by `max_run`, the wrap boundary, or `tx_buf`) that does not hit the mark
    /// reports `false`; only the run that ends on the mark reports `true`.
    #[test]
    fn ring_end_of_audio_mark_beyond_run_budget_waits() {
        let (producer, consumer) = InboundPcmRing::new(1024).split();
        assert!(ring_write(&producer, &pattern(0, 200)));
        producer.mark_end_of_audio(); // mark at 200

        let mut tx = vec![0u8; INBOUND_PCM_WRITE_UNIT_BYTES];
        // Budget the run below the mark distance: the boundary is not yet reached.
        let run = consumer.copy_run_into(50, &mut tx);
        assert_eq!(run.n, 50, "run honors the smaller per-pass budget");
        assert!(
            !run.reached_end_of_audio,
            "a mark beyond the run's reach is not reported yet"
        );
        consumer.advance(run.n);

        // Now within reach: the remaining 150 B to the mark caps the run and reports the boundary.
        let run2 = consumer.copy_run_into(usize::MAX, &mut tx);
        assert_eq!(
            run2.n, 150,
            "run capped exactly on the mark once within reach"
        );
        assert!(
            run2.reached_end_of_audio,
            "the boundary reports on reaching it"
        );
        consumer.advance(run2.n);
    }

    /// A mark on a completely empty ring (`head == tail`) — e.g. a Hello immediately followed by
    /// `EndOfAudio` with no audio — is never surfaced through `copy_run_into` (its zero-length run is
    /// indistinguishable from "ring empty"); it is observed via `take_mark_at_tail`, exactly once.
    #[test]
    fn ring_end_of_audio_mark_on_empty_ring_via_take_mark_at_tail() {
        let (producer, consumer) = InboundPcmRing::new(1024).split();
        producer.mark_end_of_audio(); // mark at head == tail == 0

        let mut tx = vec![0u8; INBOUND_PCM_WRITE_UNIT_BYTES];
        let run = consumer.copy_run_into(usize::MAX, &mut tx);
        assert_eq!(run.n, 0, "empty ring yields a zero-length run");
        assert!(
            !run.reached_end_of_audio,
            "an empty-ring mark is never reported through a run"
        );

        assert!(
            consumer.take_mark_at_tail(),
            "the empty-ring boundary is observed at the tail"
        );
        assert!(
            !consumer.take_mark_at_tail(),
            "the boundary is consumed exactly once"
        );
    }

    /// Multiple banked boundaries are reported in FIFO (arrival) order, each capping its own
    /// inter-boundary segment.
    #[test]
    fn ring_multiple_end_of_audio_marks_report_in_fifo_order() {
        let (producer, consumer) = InboundPcmRing::new(1024).split();
        // Three tone+EOA boundaries back to back: 30 B + mark, 30 B + mark, 30 B + mark.
        for seg in 0..3usize {
            assert!(ring_write(&producer, &pattern(seg * 30, 30)));
            producer.mark_end_of_audio(); // marks land at 30, 60, 90
        }

        let mut tx = vec![0u8; INBOUND_PCM_WRITE_UNIT_BYTES];
        for seg in 0..3usize {
            let run = consumer.copy_run_into(usize::MAX, &mut tx);
            assert_eq!(
                run.n, 30,
                "each run spans one inter-boundary segment (seg {seg})"
            );
            assert!(
                run.reached_end_of_audio,
                "each segment ends on its own boundary (seg {seg})"
            );
            consumer.advance(run.n);
        }
        let run = consumer.copy_run_into(usize::MAX, &mut tx);
        assert_eq!(run.n, 0, "ring drained after all three boundaries");
        assert!(!run.reached_end_of_audio);
    }

    /// Mark-FIFO overflow drops the **oldest** boundary with a warn (design §4 edge case D): pushing
    /// one more than `RING_EOA_MARK_CAP` boundaries loses the first, so the run to the first surviving
    /// mark plays the pre-dropped-mark bytes unmuted; audio is never lost or reordered.
    #[test]
    fn ring_mark_fifo_overflow_drops_oldest() {
        let (producer, consumer) = InboundPcmRing::new(1024).split();
        // Bank RING_EOA_MARK_CAP + 1 boundaries, each after 10 B of audio, without draining:
        // marks would be at 10, 20, ..., but the FIFO holds only RING_EOA_MARK_CAP, so the oldest
        // (10) is dropped. Surviving marks: 20, 30, ..., up to the last.
        for i in 0..(RING_EOA_MARK_CAP + 1) {
            assert!(ring_write(&producer, &pattern(i * 10, 10)));
            producer.mark_end_of_audio();
        }

        let mut tx = vec![0u8; INBOUND_PCM_WRITE_UNIT_BYTES];
        // First surviving boundary is at position 20 — the run to it (0..20) plays the bytes before
        // the dropped mark unmuted, exactly the edge-case-D degradation.
        let run = consumer.copy_run_into(usize::MAX, &mut tx);
        assert_eq!(
            run.n, 20,
            "run to the oldest SURVIVING mark (20); the dropped mark at 10 does not cap"
        );
        assert!(run.reached_end_of_audio);
        consumer.advance(run.n);

        // The remaining RING_EOA_MARK_CAP - 1 boundaries survive in order, each a 10 B segment.
        for i in 2..(RING_EOA_MARK_CAP + 1) {
            let run = consumer.copy_run_into(usize::MAX, &mut tx);
            assert_eq!(run.n, 10, "surviving inter-boundary segment {i}");
            assert!(run.reached_end_of_audio, "surviving boundary {i} reports");
            consumer.advance(run.n);
        }
        let run = consumer.copy_run_into(usize::MAX, &mut tx);
        assert_eq!(run.n, 0, "all surviving boundaries consumed");
    }

    /// The mark-FIFO overflow warn latch (security-1) re-arms once the FIFO drains below cap, so a
    /// later genuine overflow episode is reported again instead of being suppressed forever. The warn
    /// text is not assertable in this unit suite, so this asserts on the observable latch state
    /// (`mark_overflow_warned`), the same state-observation pattern the consumer-stall watchdog tests
    /// use.
    #[test]
    fn ring_mark_overflow_warn_latch_rearms_after_pop() {
        let (producer, consumer) = InboundPcmRing::new(1024).split();
        // Overflow the FIFO: RING_EOA_MARK_CAP + 1 boundaries at 10, 20, ... drop the oldest (10) and
        // arm the latch.
        for i in 0..(RING_EOA_MARK_CAP + 1) {
            assert!(ring_write(&producer, &pattern(i * 10, 10)));
            producer.mark_end_of_audio();
        }
        assert!(
            consumer.ring.state.lock().unwrap().mark_overflow_warned,
            "overflow arms the warn latch"
        );

        // Drain past the oldest surviving mark (20): the tail-reached pop in `advance` re-arms the
        // latch via `pop_mark_if_at`.
        let mut tx = vec![0u8; INBOUND_PCM_WRITE_UNIT_BYTES];
        let run = consumer.copy_run_into(usize::MAX, &mut tx);
        assert_eq!(run.n, 20, "run to the oldest surviving mark");
        consumer.advance(run.n);
        assert!(
            !consumer.ring.state.lock().unwrap().mark_overflow_warned,
            "popping a mark drains below cap and re-arms the latch"
        );

        // A second genuine overflow re-arms the latch again: the FIFO now holds RING_EOA_MARK_CAP - 1
        // marks; two more boundaries refill it and overflow, dropping the oldest survivor.
        let head = (RING_EOA_MARK_CAP + 1) * 10;
        assert!(ring_write(&producer, &pattern(head, 10)));
        producer.mark_end_of_audio();
        assert!(ring_write(&producer, &pattern(head + 10, 10)));
        producer.mark_end_of_audio();
        assert!(
            consumer.ring.state.lock().unwrap().mark_overflow_warned,
            "a fresh overflow after the re-arm sets the latch again"
        );
    }

    /// `apply_reset` discards a dead connection's un-reached boundary while the fresh stream's audio
    /// plays with no spurious boundary — the mark was tagged with the superseded generation.
    #[test]
    fn ring_apply_reset_discards_dead_generation_marks() {
        let (producer, consumer) = InboundPcmRing::new(1024).split();
        // Dead connection banks audio + an end-of-audio boundary, never drained.
        assert!(ring_write(&producer, &pattern(0, 100)));
        producer.mark_end_of_audio(); // dead mark at head 100, generation 0
                                      // Reconnect: reset bumps generation and records head_at_reset = head (100).
        producer.reset();
        // Fresh stream banks new audio (head → 150) with no boundary of its own.
        assert!(ring_write(&producer, &pattern(200, 50)));
        // Consumer applies the reset: the dead-generation mark is discarded, tail jumps to 100.
        let _ = consumer.apply_reset();

        let mut tx = vec![0u8; INBOUND_PCM_WRITE_UNIT_BYTES];
        let run = consumer.copy_run_into(usize::MAX, &mut tx);
        assert_eq!(run.n, 50, "only the fresh post-reset audio remains");
        assert!(
            !run.reached_end_of_audio,
            "the dead connection's boundary was discarded on reset"
        );
        consumer.advance(run.n);
        let run2 = consumer.copy_run_into(usize::MAX, &mut tx);
        assert_eq!(run2.n, 0, "ring drained, no leftover boundary");
    }

    /// The flush shape (design §3.5) — `reset()` then `mark_end_of_audio()` on the emptied ring —
    /// leaves an empty ring whose live mark (pushed in the NEW generation, at the same
    /// `head_at_reset` position a dead mark would occupy) survives `apply_reset` and is immediately
    /// reported by `take_mark_at_tail`. This is the position-identical case generation-tagging exists
    /// to disambiguate: a stale mark at the same spot is discarded, the flush's is kept.
    #[test]
    fn ring_flush_shape_live_mark_survives_reset_and_reports_at_tail() {
        let (producer, consumer) = InboundPcmRing::new(1024).split();
        // Prior audio + a stale boundary from the connection being flushed (generation 0).
        assert!(ring_write(&producer, &pattern(0, 80)));
        producer.mark_end_of_audio(); // stale mark at 80, gen 0

        // Flush: reset (gen → 1, head_at_reset = 80) then mark on the emptied ring at head 80, gen 1.
        producer.reset();
        producer.mark_end_of_audio();

        // Consumer applies the reset: the stale gen-0 mark at 80 is discarded, the live gen-1 mark at
        // 80 survives, and tail jumps to 80 == head (empty ring).
        let _ = consumer.apply_reset();
        assert_eq!(consumer.available(), 0, "flush emptied the ring");

        // copy_run_into cannot surface an empty-ring mark; take_mark_at_tail reports the live one.
        let mut tx = vec![0u8; INBOUND_PCM_WRITE_UNIT_BYTES];
        let run = consumer.copy_run_into(usize::MAX, &mut tx);
        assert_eq!(run.n, 0);
        assert!(!run.reached_end_of_audio);
        assert!(
            consumer.take_mark_at_tail(),
            "the flush's live boundary is observed at the emptied ring's tail"
        );
        assert!(
            !consumer.take_mark_at_tail(),
            "exactly one live boundary — the stale one was discarded, not double-counted"
        );
    }

    /// Regression (correctness-1): a mark pushed **after** `copy_run_into` (which then saw no mark
    /// and drained to `head`) but consumed by the matching `advance` is reported via `advance`'s
    /// return, not silently swallowed. The drain loop treats a `true` return like a capped-run
    /// boundary, so the mute still arms even though `reached_end_of_audio` was `false`.
    #[test]
    fn ring_advance_reports_mark_that_raced_the_copy() {
        let (producer, consumer) = InboundPcmRing::new(1024).split();
        assert!(ring_write(&producer, &pattern(0, 100)));

        // copy_run_into runs before the producer pushes the mark: no mark yet, run drains to head.
        let mut tx = vec![0u8; INBOUND_PCM_WRITE_UNIT_BYTES];
        let run = consumer.copy_run_into(usize::MAX, &mut tx);
        assert_eq!(run.n, 100, "run drains the banked audio to head");
        assert!(
            !run.reached_end_of_audio,
            "no mark existed when the run was taken"
        );

        // The producer pushes the end-of-audio mark at head (100) during the (simulated) write, i.e.
        // before advance. advance lands tail on it and must report the pop.
        producer.mark_end_of_audio();
        assert!(
            consumer.advance(run.n),
            "advance reports the boundary that landed at the new tail (correctness-1)"
        );
        // Consumed exactly once — a following empty poll finds nothing.
        assert!(
            !consumer.take_mark_at_tail(),
            "the raced boundary is not double-reported"
        );
    }

    /// Regression (correctness-2): a mark sitting at `tail` while the ring is **non-empty** (an
    /// `EndOfAudio`/`FlushPlayback` immediately followed by fresh audio) is observable via
    /// `take_mark_at_tail` — the path the drain loop now consults every pass — rather than being
    /// skipped by `copy_run_into` and stranded behind `tail` to head-of-line-block later boundaries.
    #[test]
    fn ring_mark_at_tail_on_nonempty_ring_observed_at_tail() {
        let (producer, consumer) = InboundPcmRing::new(1024).split();
        // Drain the ring to empty so the next mark lands at head == tail.
        assert!(ring_write(&producer, &pattern(0, 40)));
        let mut tx = vec![0u8; INBOUND_PCM_WRITE_UNIT_BYTES];
        let run = consumer.copy_run_into(usize::MAX, &mut tx);
        consumer.advance(run.n);
        assert_eq!(consumer.available(), 0, "ring drained to empty");

        // Boundary at the drained tail, then fresh audio arrives before the boundary is observed:
        // the mark is now at `tail` on a NON-empty ring.
        producer.mark_end_of_audio();
        assert!(ring_write(&producer, &pattern(40, 60)));
        assert!(consumer.available() > 0, "ring is non-empty");

        // The drain loop consults take_mark_at_tail before draining, so the boundary is caught
        // before the fresh audio is drained past it.
        assert!(
            consumer.take_mark_at_tail(),
            "the mark at tail on a non-empty ring is observed (correctness-2)"
        );
        assert!(
            !consumer.take_mark_at_tail(),
            "consumed exactly once; the following boundary (if any) is not blocked"
        );
        // The fresh audio then drains normally with no spurious boundary.
        let run2 = consumer.copy_run_into(usize::MAX, &mut tx);
        assert_eq!(run2.n, 60, "fresh post-boundary audio drains");
        assert!(!run2.reached_end_of_audio);
    }

    /// The production constants satisfy the design's invariants (mirrors the `const _: ()` asserts;
    /// pins the chosen capacity so a silent retune is visible in test output too).
    #[test]
    fn ring_production_constants() {
        assert_eq!(
            INBOUND_PCM_RING_BYTES, 65_536,
            "2 048 ms at 32 B/ms raw-mono (design §3.1, restored by design-delta-14 §4)"
        );
        assert_eq!(
            PLAYBACK_PREROLL_TARGET_BYTES, 7_680,
            "240 ms raw preroll target (design-delta-14 §2)"
        );
        assert_eq!(
            INBOUND_PCM_WRITE_UNIT_BYTES, 640,
            "one 20 ms raw wire frame (design §3.1)"
        );
        // 65_536 is a whole number of samples (even) — the only ring-cap alignment requirement
        // (design §3.1); it need NOT be a whole number of write units (65_536 % 640 == 256). The
        // reachability invariant (ring ≥ preroll target + one max raw frame) and the alignment
        // invariant (cap % 2 == 0) are enforced at compile time by the `const _: ()` asserts above
        // the constants, so they are not re-asserted here.
    }

    /// A zero-capacity ring panics at construction (the documented invariant).
    #[test]
    #[should_panic(expected = "must be > 0")]
    fn ring_zero_capacity_panics() {
        let _ = InboundPcmRing::new(0);
    }

    /// `with_storage` (design-delta-14 §4): a caller-owned zeroed `Box<[u8]>` is accepted, its
    /// length becomes the ring capacity, and the ring drains bytes written through it — i.e. the
    /// type-erased storage seam is functionally equivalent to `new(cap)`.
    #[test]
    fn with_storage_boxed_slice_round_trips() {
        let cap = 1024usize;
        let storage: Box<dyn std::ops::DerefMut<Target = [u8]> + Send> =
            Box::new(vec![0u8; cap].into_boxed_slice());
        let ring = InboundPcmRing::with_storage(storage);
        assert_eq!(
            ring.capacity(),
            cap,
            "capacity is taken from the storage length"
        );
        let (producer, consumer) = ring.split();
        let frame = [7u8; 640];
        assert!(ring_write(&producer, &frame), "a frame fits the fresh ring");
        let (drained, _gen) = ring_drain(&consumer, frame.len());
        assert_eq!(
            drained, frame,
            "bytes round-trip through PSRAM-shaped storage"
        );
    }

    /// `with_storage` rejects zero-length storage — the same invariant `new(0)` panics on.
    #[test]
    #[should_panic(expected = "must be > 0")]
    fn with_storage_zero_length_panics() {
        let storage: Box<dyn std::ops::DerefMut<Target = [u8]> + Send> =
            Box::new(Vec::<u8>::new().into_boxed_slice());
        let _ = InboundPcmRing::with_storage(storage);
    }

    /// `with_storage` rejects storage that is not a whole number of S16 samples (odd length would
    /// let a wrap split land mid-sample — the §3.1 alignment invariant).
    #[test]
    #[should_panic(expected = "whole number of S16 samples")]
    fn with_storage_odd_length_panics() {
        let storage: Box<dyn std::ops::DerefMut<Target = [u8]> + Send> =
            Box::new(vec![0u8; 1023].into_boxed_slice());
        let _ = InboundPcmRing::with_storage(storage);
    }

    /// `with_storage` fires its `debug_assert!` on non-zeroed storage — a dev-time diagnostic, not
    /// a correctness requirement (reads are bounded to `[tail, head)`). Cargo's default test
    /// profile compiles with `debug_assertions` on, so the assert fires here; release builds accept
    /// non-zeroed storage by design. If the crate ever tests with debug assertions off, gate this
    /// with `#[cfg(debug_assertions)]`.
    #[test]
    #[should_panic(expected = "expected zeroed")]
    fn with_storage_nonzero_storage_debug_asserts() {
        let mut backing = vec![0u8; 1024];
        backing[512] = 1;
        let storage: Box<dyn std::ops::DerefMut<Target = [u8]> + Send> =
            Box::new(backing.into_boxed_slice());
        let _ = InboundPcmRing::with_storage(storage);
    }

    /// `next_preroll_target` (design §3.3, cap per design-delta-1 D3): doubles the current target
    /// each successive underrun and clamps to `PLAYBACK_PREROLL_MAX_TARGET_BYTES`, converging from
    /// the 2 560 B base in two doublings and staying idempotent at the cap.
    #[test]
    fn next_preroll_target_doubles_and_caps() {
        // The escalation sequence from the 240 ms base: 7 680 → 15 360 → 30 720 (cap)
        // (design-delta-14 §2).
        assert_eq!(
            next_preroll_target(7_680),
            15_360,
            "first underrun doubles the 240 ms base to 480 ms"
        );
        assert_eq!(
            next_preroll_target(15_360),
            PLAYBACK_PREROLL_MAX_TARGET_BYTES,
            "second doubling (30 720) reaches the 960 ms cap"
        );
        // Idempotent at the cap — no unbounded growth once the ceiling is reached.
        assert_eq!(
            next_preroll_target(PLAYBACK_PREROLL_MAX_TARGET_BYTES),
            PLAYBACK_PREROLL_MAX_TARGET_BYTES,
            "at the cap the target stays put"
        );
        // A hypothetical target already above the cap is still clamped down.
        assert_eq!(
            next_preroll_target(PLAYBACK_PREROLL_MAX_TARGET_BYTES + 1),
            PLAYBACK_PREROLL_MAX_TARGET_BYTES,
            "above the cap clamps back to the ceiling"
        );
        // `saturating_mul` keeps the helper total against an overflowing input.
        assert_eq!(
            next_preroll_target(usize::MAX),
            PLAYBACK_PREROLL_MAX_TARGET_BYTES,
            "overflowing double saturates then clamps to the cap"
        );
    }

    /// Reset-to-base contract (design §3.3): a ring generation change (reconnect) drops the
    /// escalating target back to `PLAYBACK_PREROLL_TARGET_BYTES`. The capture-thread branch that
    /// performs the assignment isn't host-reachable, so pin the invariant the branch relies on —
    /// the base must sit strictly below the first escalation step (and thus the cap), so a reset
    /// genuinely *reduces* the demanded lead. If the base were ever retuned up to the cap, reset
    /// would silently become a no-op; this fails loudly instead.
    #[test]
    fn preroll_target_reset_is_base() {
        // base < first escalation step ≤ cap (the step is `next_preroll_target`-clamped),
        // so this transitively pins base < cap as well.
        assert!(
            PLAYBACK_PREROLL_TARGET_BYTES < next_preroll_target(PLAYBACK_PREROLL_TARGET_BYTES),
            "reset-to-base must land below the first escalation step, or reconnect reset is a no-op"
        );
    }

    /// Pin the escalating-preroll cap so a silent retune of the §3.3 ceiling is visible in test
    /// output (mirrors the `const _: ()` reachability assert; design-delta-14 §2 set it to 30 720).
    #[test]
    fn preroll_max_target_constant() {
        assert_eq!(
            PLAYBACK_PREROLL_MAX_TARGET_BYTES, 30_720,
            "960 ms raw-mono escalating-preroll cap (design §3.3, design-delta-14 §2)"
        );
    }

    /// Concurrent producer/consumer stress (design §4 test 9 — the deterministic correctness check
    /// the `Mutex` arm makes possible). A producer thread and a consumer thread share one ring (via
    /// a cloned `Arc`) and hammer it through thousands of write/read cycles that cross the wrap
    /// boundary many times, with a few interleaved `reset()`s. The test asserts byte-exactness: no
    /// byte the consumer reads is dropped, duplicated, or torn across a wrap or a reset boundary.
    ///
    /// Because the ring's `Mutex` serializes both ends, this exercises the *same* synchronization
    /// that runs on-device (unlike a weakly-ordered lock-free arm, whose discipline x86 cannot
    /// validate) — so a lock-discipline error (a torn split-copy, a `tail` jump racing a `write`)
    /// would surface here, not only in HIL. That is why the design carries no
    /// `TODO(inbound-ring-ordering-hil)`.
    ///
    /// Verification is **self-describing per byte**, so it needs no second cross-thread model that
    /// would itself race the ring. The producer stamps every byte with its absolute logical stream
    /// position mod 256 (`pattern(logical, len)`, `logical` climbing monotonically and *never*
    /// resetting), writing strictly in `logical` order. The ring therefore guarantees: the bytes the
    /// consumer reads are a contiguous, in-order, gap-free run of that stream — i.e. each byte is
    /// exactly its predecessor + 1 (mod 256) — **except** at a `reset()` boundary, where the read
    /// cursor legitimately jumps forward over the dropped tail (the next byte is *some* later
    /// position, still ≥ the last, never a repeat or a backward step). The consumer asserts this
    /// consecutive-stamp invariant on every run. At a generation change the boundary byte is *not*
    /// re-synced blindly: the producer records, per reset epoch, the exact `logical` stamp at which
    /// it called `reset()` (which is where `apply_reset` lands the read cursor), and the consumer
    /// asserts the first post-reset byte equals that recorded stamp — so the forward jump's landing
    /// position is checked under the concurrent reset race, not merely accepted.
    /// A dropped/duplicated/torn/reordered byte breaks the +1 chain and fails the assert.
    #[test]
    fn ring_concurrent_stress() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        // A small ring so the ~`ITERS` writes wrap the `cap` boundary many times over.
        let cap = 300;
        let (producer, consumer) = InboundPcmRing::new(cap).split();

        let drained = Arc::new(AtomicUsize::new(0)); // total bytes the consumer verified+advanced
        let producer_done = Arc::new(AtomicBool::new(false));

        const ITERS: usize = 4_000;
        // Mandatory bounded deadline: a tiny `cap` shuttling `ITERS` frames is almost always either
        // full or empty, so one thread is always spinning on the other. If the scheduler starves the
        // peer (e.g. both worker threads pinned to one contended core), this would otherwise peg a
        // core indefinitely with no diagnostic. Each spinning thread checks this deadline and panics
        // with a state dump so a hang surfaces as a fast, loud failure instead of a silent CPU peg.
        // 30 s is orders of magnitude above the unloaded completion time (~tens of ms) yet bounds the
        // worst case. Sleep-backoffs (below) keep the threads from burning the core while they wait.
        const STRESS_DEADLINE: Duration = Duration::from_secs(30);
        // A short sleep (rather than a bare `yield_now`) hands the core *voluntarily* to the peer that
        // must run next, so the producer↔consumer hand-off does not livelock-starve under contention.
        const BACKOFF: Duration = Duration::from_micros(100);
        let start = Instant::now();

        // Per reset epoch (generation), the exact stamp `apply_reset` will land the read cursor on:
        // the producer's `logical` byte count at the moment it called `reset()`. Shared so the
        // consumer can assert the first post-reset byte against the producer-recorded position.
        let reset_stamps: Arc<std::sync::Mutex<std::collections::HashMap<u32, u8>>> =
            Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        // The generation in effect before any producer reset; `reset()` bumps it by exactly 1 per
        // call, so the producer can compute each epoch's key without reading shared ring state.
        let base_gen = consumer.generation();

        let producer_done_w = Arc::clone(&producer_done);
        let reset_stamps_w = Arc::clone(&reset_stamps);
        let producer_thread = std::thread::spawn(move || {
            // `logical` is the absolute stream position; it climbs forever (even across resets), so
            // every byte ever written carries a stamp that is consecutive with its neighbours.
            let mut logical: usize = 0;
            let mut resets_done: u32 = 0;
            for i in 0..ITERS {
                // Vary the frame length (incl. lengths that straddle `cap`) so wrap splits land at
                // every offset; never exceed `cap` (the decoder guarantees `need <= cap`, design §3).
                let len = 1 + (i * 37) % cap;
                let frame = pattern(logical, len);
                // Retry until the frame fits (the consumer drains concurrently and frees space) —
                // the real producer holds the frame under backpressure, never drops it.
                while !ring_write(&producer, &frame) {
                    assert!(
                        start.elapsed() < STRESS_DEADLINE,
                        "producer stalled on a Full ring for >{STRESS_DEADLINE:?} at iter {i} \
                         (livelock/starvation — the consumer is not draining): \
                         available={}, frame_len={len}",
                        producer.free_total(),
                    );
                    std::thread::sleep(BACKOFF);
                }
                logical = logical.wrapping_add(len);

                // Occasionally reconnect: drop the un-read tail under the ring lock. The consumer's
                // `apply_reset` jumps `tail` to `head_at_reset` race-free (design §2.8) — the read
                // cursor leaps forward to the head at reset time, i.e. to `logical`. Record that
                // landing stamp keyed by the generation this reset produces *before* bumping the
                // generation, so any consumer that observes the new generation is guaranteed to find
                // the entry (insert happens-before `reset()`'s lock release happens-before the
                // consumer's generation observation).
                if i % 800 == 799 {
                    let gen = base_gen.wrapping_add(resets_done + 1);
                    reset_stamps_w
                        .lock()
                        .expect("reset_stamps mutex poisoned")
                        .insert(gen, logical as u8);
                    producer.reset();
                    resets_done += 1;
                }
            }
            producer_done_w.store(true, Ordering::Release);
        });

        let drained_w = Arc::clone(&drained);
        let producer_done_r = Arc::clone(&producer_done);
        let reset_stamps_r = Arc::clone(&reset_stamps);
        let consumer_thread = std::thread::spawn(move || {
            let mut tx_buf = vec![0u8; INBOUND_PCM_WRITE_UNIT_BYTES];
            let mut last_gen = consumer.generation();
            // The stamp the next consecutive byte must carry. The consumer starts at `tail == 0`
            // and the producer's first byte deterministically carries stamp 0, so this seeds to 0
            // and every drained byte is asserted from the very first. A reset overwrites it with the
            // producer-recorded landing stamp for the observed generation.
            let mut next_stamp: u8 = 0;
            let mut total: usize = 0;
            loop {
                let run = consumer.copy_run_into(INBOUND_PCM_WRITE_UNIT_BYTES, &mut tx_buf);
                if run.generation != last_gen {
                    // Producer reset: drop the stale tail, then resume verification. `apply_reset`
                    // jumps the read cursor forward to `head_at_reset` and returns the generation now
                    // in effect. That generation keys the producer-recorded landing stamp, so the
                    // first post-reset byte is asserted to equal the exact position the cursor jumped
                    // to — the forward jump is checked under the concurrent reset race, not accepted
                    // blindly. If a later reset intervened before the next drain, the run's generation
                    // will differ again and we loop back, overwriting this expectation before it is
                    // used, so `next_stamp` never straddles epochs.
                    let gen = consumer.apply_reset();
                    last_gen = gen;
                    next_stamp = *reset_stamps_r
                        .lock()
                        .expect("reset_stamps mutex poisoned")
                        .get(&gen)
                        .expect("no recorded reset stamp for observed generation");
                    continue;
                }
                if run.n == 0 {
                    // Ring momentarily empty. Done only once the producer has finished *and* the ring
                    // is drained; otherwise back off and retry.
                    if producer_done_r.load(Ordering::Acquire) && consumer.available() == 0 {
                        break;
                    }
                    assert!(
                        start.elapsed() < STRESS_DEADLINE,
                        "consumer stalled on an empty ring for >{STRESS_DEADLINE:?} \
                         (livelock/starvation — the producer is not writing): \
                         drained={total}, producer_done={}, generation={}",
                        producer_done_r.load(Ordering::Acquire),
                        consumer.generation(),
                    );
                    std::thread::sleep(BACKOFF);
                    continue;
                }
                // Each byte of the run must be exactly the previous + 1 (mod 256): the ring delivered
                // a contiguous, in-order, gap-free slice of the producer's stamped stream. A torn /
                // dropped / duplicated / reordered byte breaks this chain.
                for (k, &b) in tx_buf[..run.n].iter().enumerate() {
                    assert_eq!(
                        b,
                        next_stamp,
                        "stamp discontinuity at drained byte {} — a wrap/reset split must \
                         preserve byte order exactly (dropped/torn/duplicated byte)",
                        total + k
                    );
                    next_stamp = b.wrapping_add(1);
                }
                consumer.advance(run.n);
                total += run.n;
                drained_w.store(total, Ordering::Release);
            }
        });

        producer_thread.join().expect("producer thread panicked");
        consumer_thread.join().expect("consumer thread panicked");

        // The consumer actually exercised the ring across many wraps, not just spun on an empty one.
        assert!(
            drained.load(Ordering::Acquire) > cap * 8,
            "consumer should have drained well past several wraps"
        );
    }
}
