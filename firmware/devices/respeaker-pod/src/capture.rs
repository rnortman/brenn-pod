//! Audio capture: the RX capture ring, the I2S/capture constants, the
//! capture/playback fusion thread, and the `I2sWaveformSanity` HIL self-test.
//!
//! Moved verbatim from `main.rs` (see the ADR at
//! `docs/adr/2026/07/01-respeaker-pod-main-split`). Speaker-side items the capture
//! thread calls (`run_playback_sequence`, `speaker_stream_init`,
//! `write_silence_frames`, the playback-seam consts, and the TX/DMA consts) still
//! live in the crate root and are reached via `crate::`.

// Host view: these items exist for the tests and for the device-gated call sites.
#![cfg_attr(not(target_os = "espidf"), allow(dead_code))]

use audio_pipeline::playback::{PLAYBACK_PREROLL_TARGET_BYTES, next_preroll_target};

#[cfg(target_os = "espidf")]
use audio_pipeline::playback::{
    I2S_TX_FRAME_BYTES, INBOUND_PCM_WRITE_UNIT_BYTES, InboundRingConsumer, WIRE_BYTES_PER_SAMPLE,
    expand_run_in_place, preroll_gate_ready,
};
#[cfg(target_os = "espidf")]
use audio_pipeline::ring::{RING_CAPACITY_SAMPLES, RingIndex};
#[cfg(target_os = "espidf")]
use device_protocol::{Payload, Status, log_tokens};
#[cfg(target_os = "espidf")]
use esp_idf_svc::hal::delay::FreeRtos;
#[cfg(target_os = "espidf")]
use esp_idf_svc::hal::gpio::AnyIOPin;
#[cfg(target_os = "espidf")]
use esp_idf_svc::hal::i2s::config::{
    Config, DataBitWidth, Role, SlotMode, StdClkConfig, StdConfig, StdGpioConfig, StdSlotConfig,
};
#[cfg(target_os = "espidf")]
use esp_idf_svc::hal::i2s::{I2sBiDir, I2sDriver};
#[cfg(target_os = "espidf")]
use std::sync::Mutex;
#[cfg(target_os = "espidf")]
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(target_os = "espidf")]
use crate::aic3104::{aic3104_dac_mute_best_effort, aic3104_dac_unmute};
#[cfg(target_os = "espidf")]
use crate::i2c::I2C_BUS;
#[cfg(target_os = "espidf")]
use crate::{
    CAPTURE_I2S_BUF_BYTES, I2S_DMA_DESC_NUM, I2S_DMA_FRAME_NUM, INBOUND_PCM_CONSUMER,
    PLAYBACK_DAC_UNMUTE_SETTLE_FRAMES, PLAYBACK_PREROLL_MAX_WAIT_MS, PlaybackPhase,
    PlaybackRequest, STREAM_EOA_MUTE_DELAY_MS, TX_WEDGE_WARN_US, is_tx_wedged,
    run_playback_sequence, rx_deficit_frames, should_rearm_preroll, speaker_stream_init,
    write_silence_frames,
};
#[cfg(target_os = "espidf")]
use device_protocol::{TestData, test_report_fail, test_report_fail_fmt, test_report_ok};

// ── Audio capture ring (process-lifetime, boot-initialized) ──────────────────

/// State held inside `CAPTURE_RING` under a single mutex.
///
/// Callers:
/// - Capture thread: locks, writes samples, advances `write_head`, updates anchor.
///   Hold time ≤ one chunk write (≤320 samples × 2 B = 640 B memcpy ≈ negligible).
/// - Test handler / streamer: locks, reads samples and/or head/anchor, unlocks.
///
/// All fields are in the same lock so readers get a consistent snapshot.
#[cfg(target_os = "espidf")]
pub(crate) struct CaptureRing {
    /// PSRAM-allocated sample storage, capacity = `RING_CAPACITY_SAMPLES`.
    /// Indexed by the capture thread, streamer, and self-tests through `Deref`;
    /// never a DMA target (the I2S DMA lands in `dma_buf` and is CPU-copied in).
    pub(crate) samples: crate::psram::PsramBuf<i16>,
    /// Absolute sample index of the next slot to be written (monotonically increasing).
    pub(crate) write_head: u64,
    /// Sample index at the moment `anchor_ts_us` was recorded.
    pub(crate) anchor_sample: u64,
    /// `esp_timer_get_time()` µs at the moment `anchor_sample` was captured.
    pub(crate) anchor_ts_us: u64,
}

/// Process-lifetime audio capture ring. Initialized at boot before the capture
/// thread is spawned; `None` means capture has not yet been initialized (firmware
/// bug if a consumer sees `None` after boot).
#[cfg(target_os = "espidf")]
pub(crate) static CAPTURE_RING: Mutex<Option<CaptureRing>> = Mutex::new(None);

/// Set true while a HIL test handler owns [`CAPTURE_RING`] (via
/// [`CaptureQuiesceGuard`]). While set, the capture thread discards mic chunks —
/// sample writes, head advance, and anchor refresh all skipped under the ring
/// lock — and the telemetry thread feeds silence to the VAD, so the borrowing
/// test is the ring's sole writer with no production writer racing it. Always
/// false outside a HIL run, so production streaming is unaffected.
#[cfg(target_os = "espidf")]
pub(crate) static CAPTURE_QUIESCED: AtomicBool = AtomicBool::new(false);

/// RAII guard that sets [`CAPTURE_QUIESCED`] on construction and clears it on
/// drop, so every early-return path in a test handler restores capture.
///
/// Only HIL test handlers construct it, and HIL dispatch is single-threaded, so
/// there is no nesting or contention. A panic reboots the device, so a leaked
/// flag cannot outlive a boot.
#[cfg(target_os = "espidf")]
pub(crate) struct CaptureQuiesceGuard;

#[cfg(target_os = "espidf")]
impl CaptureQuiesceGuard {
    pub(crate) fn new() -> Self {
        CAPTURE_QUIESCED.store(true, Ordering::Release);
        CaptureQuiesceGuard
    }
}

#[cfg(target_os = "espidf")]
impl Drop for CaptureQuiesceGuard {
    fn drop(&mut self) {
        CAPTURE_QUIESCED.store(false, Ordering::Release);
    }
}

// ── I2S / capture constants ────────────────────────────────────────────────────

/// I2S sample rate: 16 kHz (stock l16k2ch firmware).
pub(crate) const I2S_SAMPLE_RATE_HZ: u32 = 16_000;

/// The device's fixed inbound playback format, validated against an inbound `Hello`.
///
/// The I2S clock is slaved to the XVF3800 at [`I2S_SAMPLE_RATE_HZ`] and the inbound
/// `accept` expansion path is hardwired S16_LE-mono (`pcm.len() / 2` divisor), so the
/// device cannot retune per stream: an inbound `Hello` declaring any other rate, depth,
/// channel count, or codec is unrecoverable garble. `consume_frames` passes this as the
/// `expected` format to `check_inbound_format` and drops the connection on a mismatch.
pub(crate) use audio_pipeline::wire::DEVICE_PLAYBACK_FORMAT;

/// Compile-time lock: the shared format's sample rate must equal the device's own I2S
/// clock. A drift in either fails the device build instead of a live HIL round-trip.
const _: () = assert!(DEVICE_PLAYBACK_FORMAT.sample_rate_hz == I2S_SAMPLE_RATE_HZ);

/// Dead-line guard: minimum absolute peak (`max(|min|, |max|)`) below which the
/// window is treated as a dead / all-zero line.
///
/// This is NOT a loudness floor — a healthy mic in a quiet room produces a quiet
/// but correlated signal that must PASS. Confirmed quiet-room audio reaches
/// `max_abs` as low as 38; this floor sits well below that and above a truly dead
/// line (≈0 ± a few EMI LSB). The real broken-vs-working discriminator is
/// `AUTOCORR_FLOOR`, not this guard.
#[cfg(target_os = "espidf")]
const ZERO_ABS_THRESHOLD: i16 = 16;

/// Frozen-line guard: minimum spread (`max − min`) below which the window is
/// treated as a stuck / constant / 1-bit-toggle line.
///
/// Autocorrelation cannot catch a frozen line on its own — a constant value has
/// `ac1 ≈ 1.0` and would sail through the autocorr gate — so this small spread floor
/// is the dedicated anti-frozen guard, decoupled from `ZERO_ABS_THRESHOLD`. A frozen
/// / 1-bit line has spread ≈ 0–2, while confirmed quiet-room audio has spread ≥ 76;
/// 32 separates the two with margin on both sides.
#[cfg(target_os = "espidf")]
const STUCK_SPREAD_FLOOR: i32 = 32;

/// Saturation fraction threshold: if more than this fraction of frames are at or beyond
/// ±I2S_SATURATION_ABS the signal is clipped/saturated.
#[cfg(target_os = "espidf")]
const SATURATION_FRAC_MAX: f32 = 0.95;

/// Near-full-scale threshold for saturation counting (margin below i16::MAX=32767).
/// Chosen to catch sustained clipping while allowing occasional transients.
#[cfg(target_os = "espidf")]
const I2S_SATURATION_ABS: i32 = 32700;

/// Poll sleep between NON_BLOCK I2S read attempts (milliseconds).
/// At 16 kHz stereo 32-bit, one DMA buffer of 240 frames fills in ~15 ms (8 B/frame).
/// Polling every 5 ms catches each buffer before it overflows.
#[cfg(target_os = "espidf")]
const I2S_POLL_SLEEP_MS: u32 = 5;

/// FreeRTOS priority for the pinned capture thread.
/// Above the default task priority (5) so the real-time audio path preempts general
/// work on its core, but deliberately below lwIP (18) and WiFi (23) — those stay on
/// core 0, and core isolation (this thread runs alone on core 1), not a priority war,
/// is the mechanism that keeps mic RX and DAC TX on cadence.
#[cfg(target_os = "espidf")]
const CAPTURE_THREAD_PRIORITY: u8 = 10;

/// How many 1 s summary windows to skip between `log::info!` emissions when idle
/// (no chunks written, no write failures, no RX deficit). During active playback
/// the three summary lines fire every window (~1 s) as before. This keeps the
/// console quiet while preserving full-rate telemetry when audio is flowing. The
/// HIL `CapturePeriodicLine` test drives playback, so chunks > 0 and the 1 s
/// cadence is preserved without touching the test.
#[cfg(target_os = "espidf")]
const SUMMARY_EMIT_INTERVAL: u64 = 20;

/// Maximum live (steady-state) inbound chunks drained per outer capture pass before
/// yielding to the outer loop's mic `driver.read` and the periodic emit gate.
///
/// Without this cap, a saturating inbound feed parks the consumer in back-to-back
/// ~30 ms `write_all`s and it never returns to the outer loop's RX poll or the
/// ≥1000 ms periodic-emit gate — RX capture starves while playback drains. Capping
/// the per-pass live drain forces the outer-loop yield: after at most this many live
/// chunks the inner loop `break`s, the mic is read, the emit gate is checked, and the
/// remaining channel contents drain on the next pass.
///
/// Value 2: the smallest cap that still clears a backlog faster than 1-per-poll while
/// keeping `cap × ~30 ms` (~60 ms worst-case write time) well under the 1 s emit gate.
/// Steady-state drain is ~1 live chunk per 5 ms poll (the 20 ms inbound cadence), so
/// the cap is never reached in steady state; under a transient multi-chunk backlog it
/// intentionally interleaves a mic read sooner. This caps only steady-state live
/// drain — NOT the held-chunk flush or the pre-roll fill, which are bounded
/// reconnect-window phases that must complete in one pass for correctness.
#[cfg(target_os = "espidf")]
const INBOUND_DRAIN_CHUNKS_PER_PASS: u64 = 2;

/// Maximum **raw** bytes drained from the inbound-PCM ring per outer capture pass — the
/// byte-equivalent of [`INBOUND_DRAIN_CHUNKS_PER_PASS`] for the raw-mono byte ring the consumer
/// reads. `2 × 640 = 1280` raw bytes (40 ms) = the same
/// 2-write-unit cap, expanding to `2 × 2560 = 5120` I2S-frame bytes / ~40 ms of `write_all` work,
/// so the outer-loop yield fires on the same cadence it did under the chunk cap. The consumer
/// drains in `≤ INBOUND_PCM_WRITE_UNIT_BYTES` (640 B raw) runs and `break`s the inner loop once
/// this budget is reached; a wrap split counts bytes across both halves, so a run that wraps still
/// consumes from the same budget and never forces an extra pass.
#[cfg(target_os = "espidf")]
const INBOUND_DRAIN_BYTES_PER_PASS: usize =
    INBOUND_DRAIN_CHUNKS_PER_PASS as usize * INBOUND_PCM_WRITE_UNIT_BYTES;

/// I2S slave startup warmup discard duration (microseconds).
///
/// After `rx_enable()`, the XVF3800 is not yet driving valid I2S on the wire, and the
/// ESP32 I2S slave DMA reads uncorrelated near-full-scale garbage for a period after
/// startup — on a cold boot this can extend well past 125 ms. The capture thread
/// drains I2S DMA (calls `read()`) but does not write samples into the capture ring
/// until this wall-clock window has elapsed. The window is deliberately generous —
/// assuming the chip always drives valid I2S within 1500 ms — because a couple of
/// seconds of post-boot latency before the ring is populated is negligible for a
/// voice node. Settle-detection (watching for correlated samples to appear) was
/// considered and declined for simplicity.
///
/// Keep in sync with `I2S_WAVEFORM_SANITY_RING_FILL_TIMEOUT_US` in the HIL test, which polls
/// for a full settled window after the ring starts filling.
#[cfg(target_os = "espidf")]
const CAPTURE_WARMUP_US: u64 = 1_500_000; // 1500 ms

/// Number of mono samples snapshotted for the I2sWaveformSanity test.
/// 4000 mono samples ≈ 250 ms at 16 kHz — same window as the old stereo-frame count.
#[cfg(target_os = "espidf")]
const I2S_WAVEFORM_SANITY_SAMPLES: usize = 4_000;

/// Timeout for polling the capture ring until it holds a full `I2S_WAVEFORM_SANITY_SAMPLES`
/// settled window (microseconds).
///
/// **Coupling to `CAPTURE_WARMUP_US`:** In the worst case the HIL test begins its ring-fill
/// poll at the very start of the capture warmup (e.g. on a cold boot before the ring has
/// accumulated any samples).  The warmup discards DMA output for `CAPTURE_WARMUP_US` (1500 ms)
/// without writing to the ring, so the ring-fill poll must outlast the warmup *plus* the time
/// to accumulate `I2S_WAVEFORM_SANITY_SAMPLES` at 16 kHz (4000 samples ≈ 250 ms), plus margin.
///
/// Formula: `CAPTURE_WARMUP_US + ~1000 ms margin = 2500 ms`.
///
/// **Must be updated if `CAPTURE_WARMUP_US` changes.**
///
/// If the ring never fills within this window the test fails with `reason=ring-not-filled`
/// rather than grading a partial window.
///
/// Budget check: host allocates 10 s (`RESPONSE_TIMEOUT`) for this test.
/// Worst-case device time = ring-fill poll (≤2500 ms) + stats computation (≤1 ms) ≪ 10 s.
#[cfg(target_os = "espidf")]
const I2S_WAVEFORM_SANITY_RING_FILL_TIMEOUT_US: u64 = 2_500_000; // 2500 ms = CAPTURE_WARMUP_US (1500 ms) + 1000 ms margin

/// Compile-time guard: the ring-fill poll timeout must exceed the warmup duration plus the
/// time required to accumulate `I2S_WAVEFORM_SANITY_SAMPLES` at 16 kHz.
///
/// Invariant: `TIMEOUT > WARMUP + SAMPLES * 1_000_000 / SAMPLE_RATE_HZ`
///   = 1_500_000 + 4_000 * 1_000_000 / 16_000 = 1_500_000 + 250_000 = 1_750_000 µs
///
/// If `CAPTURE_WARMUP_US` is raised without a matching raise of
/// `I2S_WAVEFORM_SANITY_RING_FILL_TIMEOUT_US`, the poll expires before the ring fills
/// and the test false-fails with `reason=ring-not-filled`.  This assert makes that
/// regression a build failure instead of a runtime surprise.
#[cfg(target_os = "espidf")]
const _: () = assert!(
    I2S_WAVEFORM_SANITY_RING_FILL_TIMEOUT_US
        > CAPTURE_WARMUP_US + (I2S_WAVEFORM_SANITY_SAMPLES as u64) * 1_000_000 / 16_000,
    "I2S_WAVEFORM_SANITY_RING_FILL_TIMEOUT_US must exceed CAPTURE_WARMUP_US \
     + ring-fill time; update it if CAPTURE_WARMUP_US changes"
);

/// Normalized lag-1 autocorrelation floor — the PRIMARY health gate for the
/// I2sWaveformSanity test.
///
/// This is the real broken-vs-working discriminator: full-scale random noise (the
/// two-master clock-contention failure mode) has ac1 ≈ 0, while real acoustic audio
/// is strongly correlated (confirmed across 60 quiet-room windows, ac1 0.41–0.97
/// every window). The floor 0.2 (200 milli) sits with margin below the observed
/// acoustic minimum and well above RNG noise. Keep in sync with
/// `I2S_HOST_AUTOCORR_FLOOR` in `hil-host/src/main.rs`.
#[cfg(target_os = "espidf")]
const AUTOCORR_FLOOR: f32 = 0.2;

/// Push as much of the staged expanded-PCM residue (`staged[*cursor..]`) as the TX DMA will
/// accept in **one NON_BLOCK write** (design §3.6). Advances `*cursor` by the bytes the DMA
/// accepted; a full DMA accepts zero and the residue is retried on the next pass — the write never
/// blocks, so it cannot stall the mic RX read that follows in the capture loop. The DMA ring is the
/// playback lead; steady state pins throughput to 1.0× because the DMA only frees ~one write-unit
/// per poll. `write_us` timing is folded into the periodic-summary accumulators (near-zero under
/// NON_BLOCK). A genuine driver fault (any error other than a full-DMA `ESP_ERR_TIMEOUT`, which is
/// backpressure) increments `*write_failures`.
///
/// I2S-wedge detection (design §3.6 edge case I): with NON_BLOCK writes a slow write cannot
/// happen, so the stall signal is the DMA moving **zero** bytes for a sustained span while
/// committed audio is pending — whether that surfaces as `Ok(0)`, a backpressure timeout, or a
/// driver error. All three feed one clock: each accepting write clears `*wedge_since` and re-arms
/// the once-until-recovery `*wedge_warned` latch; a zero-move write starts/continues the
/// `*wedge_since` clock and, once [`is_tx_wedged`] trips, emits one edge-triggered warn (latched, so
/// a persistent stall of either signature cannot flood the RT capture thread). Backpressure still
/// propagates to TCP via the ring regardless — this only surfaces a stuck DMA/codec or persistent
/// write fault.
/// Per-~1 s-window TX write telemetry. Its lifecycle is the summary window, not the run:
/// read in the periodic summary and zeroed (via `Default`) in the window-reset block alongside
/// the other window counters. Mutated only inside [`TxStager::push`].
#[derive(Default)]
#[cfg(target_os = "espidf")]
struct TxWriteStats {
    us_sum: u64,
    us_max: u64,
    attempts: u64,
    failures: u64,
}

/// Per-run staging buffer + NON_BLOCK-TX cursor + I2S-wedge clock/latch (design §3.6).
///
/// `buf[..len]` holds the expanded I2S frames of the last raw ring run; `cursor` is how many
/// of those bytes the TX DMA has already accepted. `buf` is filled by `copy_run_into` /
/// `expand_run_in_place` (borrowing only that field) before [`stage`](Self::stage) commits the
/// run length. `wedge_since` clocks a continuous zero-accept span (None = TX accepting, or
/// nothing to push); `wedge_warned` latches the edge-triggered warn once until a write is
/// accepted.
struct TxStager {
    buf: Vec<u8>,
    len: usize,
    cursor: usize,
    wedge_since: Option<std::time::Instant>,
    wedge_warned: bool,
}

impl TxStager {
    fn new(capacity: usize) -> Self {
        TxStager {
            buf: vec![0u8; capacity],
            len: 0,
            cursor: 0,
            wedge_since: None,
            wedge_warned: false,
        }
    }

    /// Is committed audio still pending in the staging buffer?
    fn has_residue(&self) -> bool {
        self.cursor < self.len
    }

    /// Commit `len` freshly-expanded bytes (already deposited in `buf`) as the run to drain,
    /// resetting the cursor to the buffer start.
    fn stage(&mut self, len: usize) {
        self.len = len;
        self.cursor = 0;
    }

    /// Reset the staging + wedge state (tone-test teardown): discard any staged residue and
    /// clear the wedge clock so the discarded bytes cannot trip a false wedge warn. `buf`
    /// contents are dead once `len = 0`.
    fn reset(&mut self) {
        self.len = 0;
        self.cursor = 0;
        self.wedge_since = None;
        self.wedge_warned = false;
    }

    /// Push as much of the staged residue (`buf[..len][cursor..]`) as the TX DMA will accept in
    /// **one NON_BLOCK write** (design §3.6). Advances `cursor` by the bytes the DMA accepted; a
    /// full DMA accepts zero and the residue is retried on the next pass — the write never
    /// blocks, so it cannot stall the mic RX read that follows in the capture loop. The DMA ring
    /// is the playback lead; steady state pins throughput to 1.0× because the DMA only frees
    /// ~one write-unit per poll. `write_us` timing is folded into `stats` (near-zero under
    /// NON_BLOCK). A genuine driver fault (any error other than a full-DMA `ESP_ERR_TIMEOUT`,
    /// which is backpressure) increments `stats.failures`.
    ///
    /// I2S-wedge detection (design §3.6 edge case I): with NON_BLOCK writes a slow write cannot
    /// happen, so the stall signal is the DMA moving **zero** bytes for a sustained span while
    /// committed audio is pending — whether that surfaces as `Ok(0)`, a backpressure timeout, or
    /// a driver error. All three feed one clock: each accepting write clears `wedge_since` and
    /// re-arms the once-until-recovery `wedge_warned` latch; a zero-move write starts/continues
    /// the `wedge_since` clock and, once [`is_tx_wedged`] trips, emits one edge-triggered warn
    /// (latched, so a persistent stall of either signature cannot flood the RT capture thread).
    /// Backpressure still propagates to TCP via the ring regardless — this only surfaces a stuck
    /// DMA/codec or persistent write fault.
    #[cfg(target_os = "espidf")]
    fn push(
        &mut self,
        driver: &mut I2sDriver<'static, I2sBiDir>,
        speaker_ready: bool,
        stats: &mut TxWriteStats,
    ) {
        if self.cursor >= self.len {
            return;
        }
        let write_start = std::time::Instant::now();
        let result = driver.write(
            &self.buf[..self.len][self.cursor..],
            esp_idf_svc::hal::delay::NON_BLOCK,
        );
        let write_us = write_start.elapsed().as_micros() as u64;
        // One `write()` call = one timed attempt: increment the mean denominator in lockstep with
        // its numerator so `write_us(mean)` is a true per-write average, not per-staged-run.
        stats.attempts = stats.attempts.wrapping_add(1);
        stats.us_sum = stats.us_sum.wrapping_add(write_us);
        if write_us > stats.us_max {
            stats.us_max = write_us;
        }
        match result {
            Ok(accepted) if accepted > 0 => {
                // TX is draining — advance, clear the wedge clock, re-arm the edge-triggered warn.
                self.cursor += accepted;
                self.wedge_since = None;
                self.wedge_warned = false;
                return;
            }
            Ok(_) => {
                // Full DMA accepted nothing: fall through to the shared zero-move wedge check.
            }
            Err(e) => {
                use esp_idf_svc::sys::ESP_ERR_TIMEOUT;
                // A NON_BLOCK write reporting a full DMA as ESP_ERR_TIMEOUT is backpressure, not a
                // fault — treat it exactly like a zero-accept. Any other error is a genuine driver
                // fault (e.g. TX not RUNNING after a tone-test teardown): count it. Either way no
                // bytes moved, so both fall through to the shared wedge check below.
                if e.code() != ESP_ERR_TIMEOUT {
                    stats.failures = stats.failures.wrapping_add(1);
                }
            }
        }
        // Zero bytes moved (full DMA, backpressure timeout, or a driver fault) while committed
        // audio is still pending: clock the continuous zero-move span and, once it crosses the
        // wedge threshold, emit ONE edge-triggered warn — latched until a write is accepted, so a
        // persistent stall of either signature cannot flood the RT capture thread.
        // Always true here (early paths return before this point with the cursor drained),
        // but read through `has_residue` so the residue predicate lives in exactly one place.
        let has_data = self.has_residue();
        let since = self.wedge_since.get_or_insert_with(std::time::Instant::now);
        let zero_us = since.elapsed().as_micros() as u64;
        if is_tx_wedged(zero_us, has_data, speaker_ready) && !self.wedge_warned {
            log::warn!(
                "capture: I2S TX wedge — no bytes accepted for >{} ms with committed audio pending and speaker ready (stuck DMA/codec or persistent write fault; ring backpressure still propagates to TCP). Suppressed until a write is accepted.",
                TX_WEDGE_WARN_US / 1000,
            );
            self.wedge_warned = true;
        }
    }
}

/// Speaker pre-roll / minimum-fill arming state (design §3.3). Groups the arm invariants that
/// otherwise live as loose locals mutated as a unit at divergent sites, so a new arm site cannot
/// forget to clear the fallback clock or pick the wrong target rule.
struct PrerollGate {
    /// While set, the consumer does not advance `tail` — the ring itself is the hold buffer.
    pending: bool,
    /// Fallback clock: starts when the first bytes arrive in the ring; None until first bytes,
    /// and reset on each re-arm so each window times from its own first bytes.
    first_chunk_at: Option<std::time::Instant>,
    /// Adaptive pre-roll target: base at boot / generation change, escalated on underrun.
    target: usize,
    /// Underrun-edge detection: true once a non-empty first-poll is seen, cleared at the
    /// non-empty→empty edge that `should_rearm_preroll` gates the warn/re-arm on. This is the
    /// live suppression (the vestigial `underrun_proxy_warned` latch is gone).
    saw_nonempty_since_empty: bool,
}

impl PrerollGate {
    /// Boot state: armed, no clock, base target, no non-empty poll seen yet.
    fn new() -> Self {
        PrerollGate {
            pending: true,
            first_chunk_at: None,
            target: PLAYBACK_PREROLL_TARGET_BYTES,
            saw_nonempty_since_empty: false,
        }
    }

    /// Generation-change re-base (reconnect): the previous generation's escalation does not
    /// carry across a fresh stream, so reset the target to base and clear the fallback clock.
    fn arm_base(&mut self) {
        self.pending = true;
        self.first_chunk_at = None;
        self.saw_nonempty_since_empty = false;
        self.target = PLAYBACK_PREROLL_TARGET_BYTES;
    }

    /// Mid-stream underrun escalate (design §3.3): rebuild lead on the next bytes and escalate
    /// the target — one underrun with this ring means the transient regime is severe, so a
    /// base-target rebuild would immediately re-underrun. Clears the fallback clock and the
    /// non-empty-edge flag (the first statement of the re-arm block); the target escalates via
    /// `next_preroll_target` and encodes underrun severity within one stream. Returns the new
    /// escalated target so the caller can log the transition without recomputing it.
    fn rearm_escalated(&mut self) -> usize {
        self.pending = true;
        self.first_chunk_at = None;
        self.saw_nonempty_since_empty = false;
        self.target = next_preroll_target(self.target);
        self.target
    }

    /// End-of-audio boundary transition (design §3.4): arm the delayed mute and full-re-base the
    /// gate so the next stream starts fresh. Routes the whole six-field transition through one
    /// method; the `break` vs fall-through control flow stays at the two call sites.
    fn on_end_of_audio_boundary(&mut self, mute_armed: &mut bool) {
        *mute_armed = true;
        self.pending = true;
        self.first_chunk_at = None;
        self.saw_nonempty_since_empty = false;
        self.target = PLAYBACK_PREROLL_TARGET_BYTES;
    }
}

// ── Capture thread ─────────────────────────────────────────────────────────────

/// Spawn the audio capture thread (runs for process lifetime).
///
/// Owns I2S0 and its GPIO pins. The main loop polls RX (mic capture) and services
/// TX (inbound playback), interleaving them on a single I2S bidir driver.
///
/// # Stack size
/// 8 KB. The 2560-byte RX/TX DMA buffers are heap-allocated (not stack) to avoid
/// overflow when both buffers plus the codec/amp call chain are live simultaneously.
///
/// # Capture ring write protocol
/// 1. Read stereo DMA bytes into `dma_buf` (no lock held during I2S read).
/// 2. Extract left-slot top-16 bits (communication beam): 32-bit stereo frames,
///    8 B each — sample at bytes [base+2, base+3] of each frame.
/// 3. Lock `CAPTURE_RING`, write samples, advance `write_head`, refresh anchor.
/// 4. Sleep `I2S_POLL_SLEEP_MS` and repeat.
///
/// Ring sample storage (~64 KB) lives in PSRAM (`psram::PsramBuf`); enlarging it
/// for multi-second pre-roll or stereo is bounded by PSRAM capacity, not internal SRAM.
#[cfg(target_os = "espidf")]
pub(crate) fn spawn_capture_thread(
    i2s: esp_idf_svc::hal::i2s::I2S0<'static>,
    bclk: esp_idf_svc::hal::gpio::Gpio8<'static>,
    din: esp_idf_svc::hal::gpio::Gpio43<'static>,
    dout: esp_idf_svc::hal::gpio::Gpio44<'static>,
    ws: esp_idf_svc::hal::gpio::Gpio7<'static>,
    playback_rx: std::sync::mpsc::Receiver<PlaybackRequest>,
) {
    use esp_idf_svc::hal::cpu::Core;
    use esp_idf_svc::hal::task::thread::ThreadSpawnConfiguration;

    // Pin the capture thread to core 1 at an elevated priority so the real-time
    // audio path is isolated from the WiFi/lwIP tasks on core 0. The pthread config
    // is process-wide, so it is captured, overridden for this spawn, then restored —
    // subsequently spawned threads (streamer, telemetry, LED) do not inherit it.
    let prev_cfg = ThreadSpawnConfiguration::get();
    ThreadSpawnConfiguration {
        priority: CAPTURE_THREAD_PRIORITY,
        pin_to_core: Some(Core::Core1),
        inherit: false,
        ..Default::default()
    }
    .set()
    .expect("failed to set capture-thread spawn config");

    let handle = std::thread::Builder::new()
        .name("capture".into())
        .stack_size(8192)
        .spawn(move || {
            // ── Init I2S driver ──────────────────────────────────────────────
            // ESP32 is I2S slave: the XVF3800 drives BCLK/WS. Role::Target consumes
            // its clocks; Role::Controller would cause two-master contention (noise).
            //
            // auto_clear(true): on TX-DMA underrun, emit zeros instead of replaying
            // the last buffer. The amp is always-on hardware, so any non-silent
            // underrun is audible — this keeps the line silent when no PCM is fed.
            //
            // DMA geometry applies to both RX and TX of the bidir driver.
            let channel_cfg = Config::new()
                .role(Role::Target)
                .auto_clear(true)
                .dma_buffer_count(I2S_DMA_DESC_NUM)
                .frames_per_buffer(I2S_DMA_FRAME_NUM);
            let clk_cfg = StdClkConfig::from_sample_rate_hz(I2S_SAMPLE_RATE_HZ);
            // 32-bit slots: XVF3800 frames 16-bit audio MSB-aligned in 32-bit slots.
            // Left = communication beam (auto-select, NS on). Right = silence.
            let slot_cfg =
                StdSlotConfig::philips_slot_default(DataBitWidth::Bits32, SlotMode::Stereo);
            let std_cfg = StdConfig::new(
                channel_cfg,
                clk_cfg,
                slot_cfg,
                StdGpioConfig::new(false, false, false),
            );
            let mclk: Option<AnyIOPin> = None;
            // Full-duplex (BiDir): RX and TX share the XVF3800's BCLK/WS clock.
            // Only RX is enabled here; TX is enabled on demand by the playback path.
            let mut driver =
                I2sDriver::<I2sBiDir>::new_std_bidir(i2s, &std_cfg, bclk, din, dout, mclk, ws)
                    .expect("capture: I2sDriver init failed — hardware fault");
            driver
                .rx_enable()
                .expect("capture: rx_enable failed — hardware fault");
            log::info!("capture: I2S0 initialized (bidir), capture thread running");

            // ── Heap-allocated I2S buffers (not stack — 8 KB stack can't hold both) ─
            // `dma_buf`: RX reads (warmup + capture loop). `tx_buf`: the tone-test /
            // resume-unmute silence path only (`run_playback_sequence`, `write_silence_frames`).
            // The streaming drain's single staging buffer lives in `stager` (declared below).
            let mut dma_buf: Vec<u8> = vec![0u8; CAPTURE_I2S_BUF_BYTES];
            let mut tx_buf: Vec<u8> = vec![0u8; CAPTURE_I2S_BUF_BYTES];
            // The streaming drain's single staging buffer lives inside `stager` (`stager.buf`):
            // `copy_run_into` deposits a raw ring run into its low bytes, `expand_run_in_place`
            // expands it to 32-bit-stereo I2S frames in place (design §3.1 "expand at DMA-write
            // time"), and the NON_BLOCK TX write drains it through `stager.cursor` across passes
            // (design §3.6). A single buffer suffices because the raw run is consumed into the
            // expanded form in one step and no silence write aliases it; the drain owns it.
            let mut stager = TxStager::new(CAPTURE_I2S_BUF_BYTES);

            // ── Startup warmup discard ───────────────────────────────────────
            // After rx_enable(), the XVF3800 outputs garbage for ~125+ ms. Drain
            // the DMA for CAPTURE_WARMUP_US without writing to the ring.
            {
                let t0_us = unsafe { esp_idf_svc::sys::esp_timer_get_time() } as u64;
                loop {
                    let now_us = unsafe { esp_idf_svc::sys::esp_timer_get_time() } as u64;
                    if now_us.saturating_sub(t0_us) >= CAPTURE_WARMUP_US {
                        break;
                    }
                    let _ = driver.read(&mut dma_buf, esp_idf_svc::hal::delay::NON_BLOCK);
                    FreeRtos::delay_ms(I2S_POLL_SLEEP_MS);
                }
                log::info!(
                    "capture: warmup discard complete ({} ms), ring will now populate",
                    CAPTURE_WARMUP_US / 1000
                );
            }

            // ── Boot-time codec/DAC bring-up ─────────────────────────────────
            // Establish the persistent "codec up, DAC unmuted, TX enabled" state once.
            // On failure, `speaker_ready` stays false: capture continues (RX is
            // independent of TX) and inbound PCM is drained and dropped.
            let speaker_ready = match speaker_stream_init(&mut driver) {
                Ok(()) => {
                    log::info!(
                        "capture: speaker stream init OK (codec up, DAC unmuted, TX enabled; amp is always-on hardware)"
                    );
                    true
                }
                Err(e) => {
                    log::warn!(
                        "capture: speaker stream init FAILED ({e:?}) — speaker disabled, capture continues"
                    );
                    false
                }
            };
            // ── Capture loop state ────────────────────────────────────────────
            let mut playback_disconnected_warned = false;

            // Take the inbound-PCM ring consumer that main() installed before spawn.
            // This thread is the sole consumer (tail-writer). None = boot ordering bug:
            // main() must install the consumer before spawn_capture_thread(). Panic
            // (= reboot under panic=abort) rather than run forever with inbound audio
            // silently dead.
            let inbound_consumer: InboundRingConsumer = INBOUND_PCM_CONSUMER
                .lock()
                .expect("INBOUND_PCM_CONSUMER mutex poisoned at capture-thread start")
                .take()
                .expect(
                    "inbound PCM ring consumer not installed before capture-thread spawn (boot ordering bug)",
                );
            // Tracks the ring generation we last applied a reset for. Initialized to the
            // ring's boot generation so the first pass isn't misread as a reset boundary.
            let mut acted_generation: u32 = inbound_consumer.generation();

            // ── Streaming DAC mute/unmute gate ──────────────────────────────
            // `dac_active`: true = DAC unmuted and streaming, false = muted/idle.
            // Starts false so the first stream chunk issues an unmute (redundant after
            // boot but keeps a single source of truth). Distinct from `needs_rebringup`
            // which tracks the tone-test teardown's full TX-disabled state.
            // `last_pcm_write`: clocks the post-boundary mute delay; None = disarmed.
            // `mute_armed` (design §3.4): the mute decision is DECOUPLED from data
            // starvation — the DAC soft-mute fires only STREAM_EOA_MUTE_DELAY_MS after a
            // drain run reaches an explicit end-of-audio boundary (or the connection is
            // dropped, which the streamer surfaces as an end-of-audio mark). A stalled
            // pipeline leaves `mute_armed` false and never mutes: auto_clear holds the
            // line at zeros while the ring waits for data. Set at a reached boundary,
            // cleared when new PCM is written before the delay elapses.
            let mut dac_active = false;
            let mut last_pcm_write: Option<std::time::Instant> = None;
            let mut mute_armed = false;

            // ── Once-until-recovery fault-log guards ─────────────────────────
            // Persistent faults (stuck I2C, unresponsive codec) would otherwise log per
            // chunk (~1600 lines/s). Each flag logs the transition into failure once,
            // then resets on the first success so transient faults log both edges.
            let mut dac_unmute_failed_warned = false;
            let mut rebringup_failed_warned = false;

            // ── Staging + NON_BLOCK-TX + I2S-wedge state (design §3.6) ──────
            // `stager.buf[..stager.len]` holds the expanded I2S frames of the last raw ring run;
            // `stager.cursor` is how many of those bytes the TX DMA has already accepted. When
            // `!stager.has_residue()` the staging buffer is drained and the drain loop pulls the
            // next raw run. Partial DMA acceptance leaves residue, pushed at the top of the next
            // drain-loop iteration before any new run — committed audio reaches the DAC
            // independent of the preroll/mark gates. `tail` is advanced when a run is copied
            // *into* staging, so at most one write-unit (~20 ms) of audio is committed-but-unplayed
            // here (design §3.5 flush latency bound). The wedge clock/latch live in the same
            // object (design §3.6 edge case I); under NON_BLOCK writes the failure signal is the
            // DMA accepting zero bytes for a sustained span (> TX_WEDGE_WARN_US) while committed
            // audio is pending. (`stager` is declared above with the DMA buffers.)

            // ── Pre-roll / minimum-fill gate ─────────────────────────────────
            // Armed at boot, on each reconnection, and re-armed on a mid-stream underrun
            // (design §3.3). While `pending`, the consumer does not advance `tail` — the ring
            // itself is the hold buffer. Clears when available() reaches `gate.target` or the
            // PLAYBACK_PREROLL_MAX_WAIT_MS fallback elapses.
            //
            // Adaptive target (design §3.3, delta-1 D3): starts at PLAYBACK_PREROLL_TARGET_BYTES
            // and doubles (capped) on each successive underrun within one ring generation; reset
            // to base on a generation change (reconnect) or a genuine stream end (idle-mute).
            // Escalation rebuilds more lead when recovery delivery runs faster than real-time (the
            // ring refills before the PLAYBACK_PREROLL_MAX_WAIT_MS fallback fires) — burst-gap
            // recovery. With the host holding up to a 1 s burst-lead, that recovery is typically a
            // burst at network rate, so the escalated targets (base 240 ms, cap 960 ms) are
            // reachable in their intended regime. Under *sustained* sub-real-time delivery the
            // 500 ms fallback bounds the rebuilt lead (the 960 ms cap exceeds the 500 ms wait), so
            // escalation cannot outrun that regime; sustained sub-real-time delivery at these
            // depths is an unexpected reading for human review, not a knob to nudge.
            //
            // `gate.saw_nonempty_since_empty` detects mid-stream ring starvation
            // (non-empty→empty while dac_active): it requires a prior non-empty first-poll to
            // distinguish real starvation from the normal end-of-stream window (under the design
            // §3.4 mute policy dac_active stays true through starvation — it drops only when an
            // end-of-audio boundary arms and fires the mute). It is the live edge suppression.
            let mut gate = PrerollGate::new();

            // ── Playback-TX observability counters (per ~1 s window) ────────
            // Emitted as periodic `capture: playback tx/obs/phase` log lines.
            // All reset after each emit so each line is self-contained.
            // `tx_chunks_written` counts write-units committed to staging (the `chunks=` token the
            // host drain-rate eval scores as `chunks × write_unit`). The staging arm only commits a
            // new run once the previous run's residue has fully drained (the `residue_pending`
            // break gates it), so a stalled TX stops incrementing this and the drain rate reads
            // low — the count over-reports drained bytes by at most the single in-flight run whose
            // residue straddles the window boundary, within the ±DMA-lead jitter the eval's
            // keep-up floor already absorbs.
            let mut tx_chunks_written: u64 = 0;
            // First-poll outcome per outer iteration: was data ready when we came to drain?
            let mut tx_empty_polls: u64 = 0;
            let mut tx_nonempty_polls: u64 = 0;
            // Empty first-polls during pre-roll are expected (waiting for fill), not
            // starvation. Routing them here keeps `tx_empty_polls` clean for steady-state.
            //
            // Semantic drift note (design §3.3 re-arm, step 2): a *mid-stream* underrun now
            // also sets `preroll_pending`, so during the ensuing refill every empty first
            // poll routes to `tx_preroll_waits` and every non-empty first poll increments
            // `tx_nonempty_polls` while the gate drains zero chunks. `tx_empty_polls` there-
            // fore counts underrun *edges*, not starvation duration, once a re-arm is live,
            // and refill windows read as "saturated" (high nonempty, ~zero empty) to the
            // HIL `PlaybackDrainRate` classifier even though the consumer is deliberately
            // holding. A controlled saturated HIL feed never underruns so this does not bite
            // the test today. `preroll_waits`/`preroll_rearms` (emitted below on the obs line)
            // are the discriminators: the host classifier correlates that line per window and
            // excludes any window where either is nonzero.
            let mut tx_preroll_waits: u64 = 0;
            // Per-window count of mid-stream underrun re-arms (design §3.3). Distinct from
            // `preroll_waits` (which conflates boot/reconnect/underrun preroll): non-zero
            // here is the evidence design-delta-1 D4 keys the OQ2 depth-reopen decision on.
            let mut tx_preroll_rearms: u64 = 0;
            // NON_BLOCK `write()` telemetry (µs sum/max, attempt/failure counts). `attempts` is
            // the mean denominator — one increment per `write()` call, matched to `us_sum`'s
            // numerator so the emitted `write_us(mean)` averages over writes, not over staged runs
            // (of which each takes several passes to drain under NON_BLOCK TX). Mutated only inside
            // `TxStager::push`; lifecycle is the ~1 s window (zeroed in the window-reset block).
            let mut tx_write_stats = TxWriteStats::default();
            // Max chunks drained in one outer iteration (backlog depth signal).
            let mut tx_max_backlog: u64 = 0;
            // DAC mute/unmute transition counts. Under the design §3.4 mute policy each
            // pair marks one explicit end-of-audio boundary (or drop) followed by a
            // resume; starvation never touches the amp. High counts per window mean
            // many short end-of-audio/restart cycles (e.g. the host tone/silence
            // pattern).
            let mut tx_resume_unmutes: u64 = 0;
            let mut tx_eoa_mutes: u64 = 0;
            // RX frames delivered this window (uncapped count from DMA, not the min(320)
            // ring-write cap). RX and TX share the same I2S clock, so this is a software
            // measurement of the wire's sample rate — a floor (TX backlog can depress it
            // but never inflate it).
            let mut rx_frames_delivered: u64 = 0;
            // Per-window RX-deficit suppression + warn latch (design §3.6 "RX-loss counter",
            // edge case K). A tone-test sequence services `run_playback_sequence` inline and
            // deliberately does NOT drain the mic RX DMA for its duration, so any window that
            // ran one has a depressed `rx_frames_delivered` that is not real loss — its deficit
            // is suppressed. `rx_deficit_warned` is a once-until-recovery latch: warn on the
            // first nonzero-deficit window, re-arm when a window returns to zero deficit.
            let mut tone_test_this_window = false;
            let mut rx_deficit_warned = false;
            // Wall-clock gate for ~1 s summary cadence. Uses Instant (not iteration count)
            // because the loop rate is irregular (continues on empty RX reads).
            let mut tx_summary_window_start = std::time::Instant::now();
            // Window counter for log-emission gating. The three `log::info!` summary
            // lines are emitted every window when audio is active (chunks > 0, write
            // failures, or RX deficit) but only every SUMMARY_EMIT_INTERVAL windows
            // when idle — keeps the console quiet without losing the 1 s measurement
            // cadence or suppressing anomaly reports. HIL tests drive playback, so
            // chunks > 0 and the 1 s cadence is preserved for the cadence assertion.
            let mut summary_window_count: u64 = 0;

            // After a tone test, the codec is left TX-disabled + DAC-muted.
            // This flag triggers re-init before the next streaming write.
            let mut needs_rebringup = false;

            // Cache this thread's FreeRTOS priority and core affinity for the periodic
            // summary line — confirms the core-1 pin and elevated priority took effect.
            let actual_prio: u32 =
                unsafe { esp_idf_svc::sys::uxTaskPriorityGet(core::ptr::null_mut()) };
            let actual_core: u32 = esp_idf_svc::hal::cpu::core() as u32;

            loop {
                // ── Service a pending playback request ────────────────────────
                // Non-blocking poll. During playback, RX DMA is not drained (capture
                // samples dropped for the sequence duration — acceptable, no concurrent
                // consumer). The driver stays enabled so warmup is not re-incurred.
                match playback_rx.try_recv() {
                    Ok(request) => {
                        // A tone-test sequence does not drain the mic RX DMA for its duration
                        // (comment above), so this window's rx_frames are not a real-rate sample —
                        // suppress its RX-deficit reading (design §3.6 edge case K).
                        tone_test_this_window = true;
                        let outcome =
                            run_playback_sequence(&mut driver, request.params, &mut tx_buf);
                        // Teardown left codec TX-disabled + DAC-muted. Flag re-init so
                        // the next streaming write restores the active state.
                        if speaker_ready {
                            needs_rebringup = true;
                            // Reset streaming gate to match the muted state so the first
                            // post-test chunk re-runs the resume-unmute from scratch.
                            dac_active = false;
                            last_pcm_write = None;
                            // Discard any staged residue committed before the test. The tone test
                            // disabled TX, so this run's tail can no longer be pushed (the DMA is
                            // stopped, and every push would error), while the re-bring-up that
                            // re-enables TX lives past the residue-pending break in the drain loop
                            // below — a pending residue would strand it and wedge playback for the
                            // process lifetime. Dropping it (≤20 ms of the already-interrupted prior
                            // stream) keeps re-bring-up reachable, and clears the wedge clock so the
                            // discarded residue cannot trip a false wedge warn.
                            stager.reset();
                        }
                        let _ = request.reply.send(outcome);
                    }
                    Err(std::sync::mpsc::TryRecvError::Empty) => {}
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        if !playback_disconnected_warned {
                            log::warn!("capture: playback request channel disconnected");
                            playback_disconnected_warned = true;
                        }
                    }
                }

                // ── Service inbound streaming PCM (design §3.6) ─────────────
                // Drain the inbound ring into the TX DMA with NON_BLOCK writes and a staging
                // cursor: each pass pushes only what the DMA will accept and never parks the
                // thread, so a slow TX drain can no longer starve the mic RX read below (the
                // ~48% mic-sample loss this step eliminates). The ~120 ms TX DMA ring is the
                // playback lead; steady-state throughput self-pins to 1.0× because the DMA frees
                // only ~one write-unit per poll. Capped at INBOUND_DRAIN_BYTES_PER_PASS raw bytes
                // per outer iteration so a post-preroll burst still yields to the mic read.
                let mut backlog_this_iter: u64 = 0;
                let mut first_poll = true;
                let mut drained_bytes_this_iter: usize = 0;

                // Step 0: reconnection-boundary reset. On a generation change, drop the old
                // connection's stale tail and re-arm pre-roll.
                let gen_now = inbound_consumer.generation();
                if gen_now != acted_generation {
                    acted_generation = inbound_consumer.apply_reset();
                    gate.arm_base();
                }

                loop {
                    // Push any staged residue FIRST (design §3.6): committed audio the DMA
                    // did not accept on a prior pass. This is gate-independent — it must reach
                    // the DAC even while pre-roll holds new ring data. `tail` was already
                    // advanced when this run was staged, so the residue is not re-derived from
                    // the ring; the buffer alone owns it.
                    if stager.has_residue() {
                        stager.push(&mut driver, speaker_ready, &mut tx_write_stats);
                    }
                    let residue_pending = stager.has_residue();

                    let available = inbound_consumer.available();

                    // End-of-audio boundary sitting exactly at `tail` (design §3.4).
                    // Consulted at the TOP of EVERY pass — the single observation path for a
                    // mark at `tail`, covering: a mark on an empty/just-emptied ring (EOA on a
                    // drained ring, flush, drop-as-EOA), a mark the producer pushed at `tail`
                    // while the ring is NON-empty (an EOA/flush immediately followed by fresh
                    // audio — which `copy_run_into` would otherwise skip and strand behind the
                    // tail), and the tail landing exactly on a mark. This is
                    // NOT starvation: arm the delayed mute and re-arm pre-roll for the next
                    // stream, then break (a following stream re-prerolls fresh). Any staged
                    // residue keeps draining from the next pass's top-of-loop push. A mark the
                    // tail *lands on* via `advance` is reported by `advance` in the stage arm
                    // below instead.
                    if inbound_consumer.take_mark_at_tail() {
                        gate.on_end_of_audio_boundary(&mut mute_armed);
                        break;
                    }

                    // First-poll classification: was ring data ready when we came to drain?
                    // (Independent of staged residue, which is already-committed audio.)
                    if first_poll {
                        if available > 0 {
                            tx_nonempty_polls = tx_nonempty_polls.wrapping_add(1);
                            first_poll = false;
                            gate.saw_nonempty_since_empty = true;
                        } else if residue_pending {
                            // Ring empty but committed audio is still draining from staging —
                            // the DAC is being fed, so this is NOT starvation and NOT a
                            // pre-roll fill wait. Do not count it or arm the underrun proxy;
                            // the residue drains from the next pass's top-of-loop push.
                            break;
                        } else {
                            // During pre-roll, empties are expected fill waits, not
                            // starvation — route to preroll_waits to keep empty_polls clean.
                            if gate.pending {
                                tx_preroll_waits = tx_preroll_waits.wrapping_add(1);
                            } else {
                                tx_empty_polls = tx_empty_polls.wrapping_add(1);
                            }

                            // Underrun-proxy edge: warn on non-empty→empty transition while
                            // DAC is active and not in pre-roll (excludes the normal
                            // end-of-stream idle window). `should_rearm_preroll` is the live
                            // edge suppression: it gates on `saw_nonempty_since_empty`, which
                            // `rearm_escalated` clears here, so the warn fires exactly once per
                            // non-empty→empty streak (no separate warn latch needed).
                            if should_rearm_preroll(
                                gate.saw_nonempty_since_empty,
                                dac_active,
                                gate.pending,
                            ) {
                                // Name the escalation the re-arm drives (base / 5 120 / capped)
                                // — the target and re-arm frequency are the field evidence
                                // design-delta-1 D4 keys the OQ2 depth decision on, so it must
                                // be legible in the log. Read the pre-escalation target before
                                // re-arm advances it, and log the exact target the gate adopts.
                                let before = gate.target;
                                // Re-arm pre-roll (design §3.3): rebuild lead on the next bytes
                                // instead of playing every subsequent gap at zero lead.
                                // Escalate the target — one underrun with this ring means the
                                // transient regime is severe, so 80 ms of rebuilt lead would
                                // immediately re-underrun. The escalation resets to base when
                                // the stream genuinely ends (a reached end-of-audio boundary,
                                // design §3.4, below), so it encodes underrun severity within
                                // one stream, not utterance count across a long-lived connection.
                                let escalated = gate.rearm_escalated();
                                log::warn!(
                                    "capture: underrun proxy: inbound playback ring emptied mid-stream, DAC active (TX DMA likely starved; not a measured underrun). Re-arming pre-roll, escalating fill target {}->{} bytes. Edges suppressed until a non-empty poll recovers.",
                                    before,
                                    escalated,
                                );
                                tx_preroll_rearms = tx_preroll_rearms.wrapping_add(1);
                            }
                            break;
                        }
                    }

                    // Step 1: pre-roll gate. Hold PCM in the ring (don't advance tail) until
                    // fill target or fallback timeout is reached.
                    if gate.pending {
                        if available > 0 && gate.first_chunk_at.is_none() {
                            gate.first_chunk_at = Some(std::time::Instant::now());
                        }
                        let elapsed_ms =
                            gate.first_chunk_at.map(|t| t.elapsed().as_millis() as u64);
                        if preroll_gate_ready(
                            available,
                            gate.target,
                            elapsed_ms,
                            PLAYBACK_PREROLL_MAX_WAIT_MS,
                        ) {
                            gate.pending = false;
                        } else {
                            break; // still filling
                        }
                    }

                    // Cannot stage a new run while committed residue is still in flight —
                    // the single staging buffer holds one run at a time. Yield to the mic
                    // read; the residue drains from the next pass's top-of-loop push.
                    if residue_pending {
                        break;
                    }

                    // Step 2: pull ≤ one write-unit of raw PCM from the ring into `stager.buf`,
                    // expand it in place, and push what the DMA accepts (NON_BLOCK).
                    let remaining = INBOUND_DRAIN_BYTES_PER_PASS
                        .saturating_sub(drained_bytes_this_iter)
                        .min(INBOUND_PCM_WRITE_UNIT_BYTES);
                    if remaining == 0 {
                        break; // per-pass byte cap reached
                    }
                    let run = inbound_consumer.copy_run_into(remaining, &mut stager.buf);
                    if run.n == 0 {
                        // Ring empty. A mark that lands at head==tail between the top-of-loop
                        // `take_mark_at_tail` and here (producer/consumer race, or a flush that
                        // emptied the ring) is caught by the next pass's top-of-loop check
                        // (≤ one 5 ms tick later, well inside the mute delay).
                        break;
                    }
                    // Belt-and-suspenders: if a reset landed between step 0 and here, these
                    // bytes are from the dead connection. Skip the write; next pass's
                    // apply_reset discards them (safe: tail is not advanced, and apply_reset
                    // jumps tail past these stale bytes).
                    if run.generation != acted_generation {
                        log::debug!(
                            "capture: inbound ring reset detected mid-drain (run gen {} != acted gen {}) — deferring {} stale bytes to next-pass apply_reset",
                            run.generation,
                            acted_generation,
                            run.n,
                        );
                        break;
                    }

                    backlog_this_iter = backlog_this_iter.wrapping_add(1);

                    // Gate: speaker readiness, tone-test re-bring-up, DAC resume-unmute. A
                    // rebring-up / resume fault breaks WITHOUT advancing `tail`, so the raw run
                    // stays in the ring for retry (the bytes copied into `staged` are simply
                    // overwritten next attempt). !speaker_ready drops the chunk: it advances
                    // `tail` below but stages nothing.
                    let mut staged_this_run = false;
                    if speaker_ready {
                        // Re-init codec after a tone test left it TX-disabled + DAC-muted.
                        if needs_rebringup {
                            match speaker_stream_init(&mut driver) {
                                Ok(()) => {
                                    log::info!(
                                        "capture: re-bring-up after tone test OK (TX enabled, DAC unmuted; amp is always-on hardware)"
                                    );
                                    needs_rebringup = false;
                                    rebringup_failed_warned = false;
                                }
                                Err(e) => {
                                    if !rebringup_failed_warned {
                                        log::warn!(
                                            "capture: re-bring-up after tone test FAILED ({e:?}) — dropping chunk, will retry (further failures suppressed until recovery)"
                                        );
                                        rebringup_failed_warned = true;
                                    }
                                    // Break, don't retry per chunk: speaker_stream_init pays
                                    // 100 ms DAC settle; retrying per chunk would overflow RX DMA.
                                    break;
                                }
                            }
                        }
                        // ── DAC resume-unmute on first chunk after idle ─────
                        // Sequence: (1) pre-roll silence so the soft-step clocks against zeros,
                        // (2) unmute DAC under scoped I2C lock, (3) stream real PCM. The DAC
                        // soft-mute keeps the analog stage powered, so only the ~20 ms soft-step
                        // margin is needed (not the 100 ms power-up settle). The silence pre-roll
                        // is a bounded blocking write_all through `tx_buf` — a non-steady-state
                        // phase (design §3.6), separate from the NON_BLOCK streaming path.
                        if !dac_active {
                            // Step 1: pre-roll silence into TX DMA.
                            if let Err(outcome) = write_silence_frames(
                                &mut driver,
                                &mut tx_buf,
                                PLAYBACK_DAC_UNMUTE_SETTLE_FRAMES,
                                PlaybackPhase::DacSilenceMargin,
                            ) {
                                log::warn!(
                                    "capture: resume pre-roll silence failed ({outcome:?}) — deferring unmute, will retry"
                                );
                                break;
                            }
                            // Step 2: unmute under scoped I2C lock. On failure, log once and
                            // stop this drain pass (don't thrash the bus per chunk).
                            let unmuted = match I2C_BUS
                                .lock()
                                .unwrap_or_else(|_| {
                                    panic!("I2C_BUS mutex poisoned in stream resume-unmute")
                                })
                                .as_mut()
                            {
                                Some(d) => match aic3104_dac_unmute(d) {
                                    Ok(()) => true,
                                    Err(e) => {
                                        if !dac_unmute_failed_warned {
                                            log::warn!(
                                                "capture: stream resume DAC unmute FAILED ({e:?}) — dropping chunk, will retry (further failures suppressed until recovery)"
                                            );
                                            dac_unmute_failed_warned = true;
                                        }
                                        false
                                    }
                                },
                                None => {
                                    if !dac_unmute_failed_warned {
                                        log::warn!(
                                            "capture: stream resume DAC unmute skipped — I2C_BUS unavailable (will retry)"
                                        );
                                        dac_unmute_failed_warned = true;
                                    }
                                    false
                                }
                            };
                            if !unmuted {
                                break;
                            }
                            dac_unmute_failed_warned = false;
                            dac_active = true;
                            tx_resume_unmutes = tx_resume_unmutes.wrapping_add(1);
                        }
                        // Step 3: expand raw S16 mono → 32-bit stereo I2S frames in place
                        // (design §3.1 "expand at DMA-write time") within `stager.buf`, then
                        // push what the DMA accepts. `run.n` raw bytes (≤ one 640 B write unit)
                        // expand to `run.n / WIRE_BYTES_PER_SAMPLE × I2S_TX_FRAME_BYTES` ≤ 2560 B,
                        // fitting `stager.buf` (CAPTURE_I2S_BUF_BYTES = 2560).
                        let sample_count = run.n / WIRE_BYTES_PER_SAMPLE;
                        let expanded_len = sample_count * I2S_TX_FRAME_BYTES;
                        expand_run_in_place(&mut stager.buf, sample_count);
                        stager.stage(expanded_len);
                        tx_chunks_written = tx_chunks_written.wrapping_add(1);
                        // Clock the post-boundary mute delay from when this run is committed to
                        // staging (design §3.4): the mute fires STREAM_EOA_MUTE_DELAY_MS after
                        // the last committed run, and 200 ms comfortably exceeds the ≤120 ms DMA
                        // + ≤20 ms staged residue playout, so the banked tail plays before mute.
                        last_pcm_write = Some(std::time::Instant::now());
                        stager.push(&mut driver, speaker_ready, &mut tx_write_stats);
                        staged_this_run = true;
                    }

                    // Advance `tail` (raw bytes now committed to staging) and handle the
                    // end-of-audio boundary. `advance` returns whether it popped a mark the new
                    // tail landed on — including one the producer pushed during the copy, which
                    // `copy_run_into` could not have reported. Runs for the !speaker_ready drop
                    // too, so a boundary is never stranded.
                    let popped_mark = inbound_consumer.advance(run.n);
                    drained_bytes_this_iter += run.n;
                    if run.reached_end_of_audio || popped_mark {
                        // This run drained the banked tail up to an explicit end-of-audio
                        // boundary (design §3.4). Arm the mute and re-arm pre-roll + reset the
                        // underrun-edge state and adaptive target so the next stream starts fresh.
                        gate.on_end_of_audio_boundary(&mut mute_armed);
                    } else {
                        // New PCM committed that is NOT a boundary: cancel any pending EOA mute
                        // (design §3.4 / edge case B — host ended one stream and immediately
                        // began another, so no spurious mute/unmute pair fires).
                        mute_armed = false;
                    }

                    // If the DMA did not accept the whole staged run, hold the residue and
                    // yield to the mic read; it drains from the next pass's top-of-loop push.
                    if staged_this_run && stager.has_residue() {
                        break;
                    }
                }
                if backlog_this_iter > tx_max_backlog {
                    tx_max_backlog = backlog_this_iter;
                }

                // ── Streaming DAC end-of-audio mute (design §3.4) ─────────────
                // The mute decision is DECOUPLED from data starvation. The DAC
                // soft-mutes ONLY when an explicit end-of-audio boundary (or a dropped
                // connection, surfaced by the streamer as an EOA mark) has been reached
                // — `mute_armed` — and then only STREAM_EOA_MUTE_DELAY_MS after the last
                // write, so the ≤120 ms TX DMA tail plays out first. auto_clear holds
                // the line at zeros through the delay, so the soft-step is click-safe.
                // A stalled pipeline (ring empty, no mark) never sets `mute_armed`, so
                // it never mutes — the 1 Hz starvation mute limit cycle (and its I2C
                // churn) is removed structurally, not tuned away. Best-effort: on I2C
                // failure the DAC stays unmuted into a silent line (quiet, with a log).
                if dac_active
                    && mute_armed
                    && let Some(t) = last_pcm_write
                    && t.elapsed().as_millis() as u64 >= STREAM_EOA_MUTE_DELAY_MS
                {
                    match I2C_BUS
                        .lock()
                        .unwrap_or_else(|_| {
                            panic!("I2C_BUS mutex poisoned in stream end-of-audio mute")
                        })
                        .as_mut()
                    {
                        Some(d) => aic3104_dac_mute_best_effort(d),
                        None => log::warn!(
                            "capture: stream end-of-audio mute skipped — I2C_BUS unavailable"
                        ),
                    }
                    dac_active = false;
                    last_pcm_write = None;
                    mute_armed = false;
                    // The adaptive pre-roll target was already reset to base at the
                    // boundary that armed this mute (the DrainCtl::Next / empty-tail
                    // arms above), so no target reset is needed here.
                    tx_eoa_mutes = tx_eoa_mutes.wrapping_add(1);
                }

                // ── Periodic ~1 s summary ───────────────────────────────────
                if tx_summary_window_start.elapsed().as_millis() as u64 >= 1000 {
                    // Real elapsed window (not nominal 1 s — write_all stalls stretch it).
                    let rx_window_us = tx_summary_window_start.elapsed().as_micros() as u64;
                    // Per-window mic RX-deficit (design §3.6 "RX-loss counter"): the shortfall of
                    // RX frames read vs the I2S clock's frame count over this window, dead-banded
                    // for jitter. Suppressed on any window that ran a tone-test (mic RX not drained
                    // during the sequence). This is the telemetry number that makes the ~48 % mic
                    // loss observable rather than reader arithmetic. A suppressed window is marked
                    // rx_win_ok=0 on line 2 so it is excluded rather than scored as a clean pass.
                    let rx_deficit = if tone_test_this_window {
                        0
                    } else {
                        rx_deficit_frames(rx_window_us, rx_frames_delivered)
                    };
                    // Mean per-write time: numerator and denominator are both counted once per
                    // `write()` call inside `TxStager::push`, so this is a true per-write average
                    // (residue retries and zero-accept polls each count as one write, matching how
                    // their duration is summed into `tx_write_stats.us_sum`).
                    let write_us_mean = tx_write_stats
                        .us_sum
                        .checked_div(tx_write_stats.attempts)
                        .unwrap_or(0);
                    // Emit the three summary lines every window when audio is active
                    // (chunks written, write failures, or RX deficit), but only every
                    // SUMMARY_EMIT_INTERVAL windows (~20 s) when idle. This keeps the
                    // console quiet during silence while preserving the 1 s cadence
                    // during playback (and during the HIL CapturePeriodicLine test,
                    // which drives playback and relies on that cadence).
                    let should_emit_summary = tx_chunks_written > 0
                        || tx_write_stats.failures > 0
                        || rx_deficit > 0
                        || summary_window_count.is_multiple_of(SUMMARY_EMIT_INTERVAL);
                    // Split into two log lines: line 1 carries HIL-parsed cross-check tokens
                    // and MUST fit in the 200-char heapless::String budget. Line 2 carries
                    // human-observability counters under a distinct prefix. Do NOT add tokens
                    // to line 1 — they can truncate the rx_window_us divisor.
                    if should_emit_summary {
                        log::info!(
                            "{}rx_frames={} {}{} {}{} write_us(mean/max)={}/{} max_backlog={} {}{} {}{}",
                            log_tokens::CAPTURE_TX_LINE,
                            rx_frames_delivered,
                            log_tokens::RX_WINDOW_US,
                            rx_window_us,
                            log_tokens::CHUNKS,
                            tx_chunks_written,
                            write_us_mean,
                            tx_write_stats.us_max,
                            tx_max_backlog,
                            log_tokens::NONEMPTY_POLLS,
                            tx_nonempty_polls,
                            log_tokens::POLL_EMPTY,
                            tx_empty_polls,
                        );
                        log::info!(
                            "{}writefail={} {}{} {}{} resume_unmutes={} eoa_mutes={} {}{} {}{} {}{} {}{}",
                            log_tokens::CAPTURE_OBS_LINE,
                            tx_write_stats.failures,
                            log_tokens::PREROLL_WAITS,
                            tx_preroll_waits,
                            log_tokens::PREROLL_REARMS,
                            tx_preroll_rearms,
                            tx_resume_unmutes,
                            tx_eoa_mutes,
                            log_tokens::RX_WIN_OK,
                            if tone_test_this_window { 0 } else { 1 },
                            log_tokens::RX_DEFICIT,
                            rx_deficit,
                            log_tokens::PRIO,
                            actual_prio,
                            log_tokens::CORE,
                            actual_core,
                        );
                    }
                    // Edge-triggered warn on any real RX deficit (design §3.6). rx_deficit is
                    // already tone-test-suppressed and dead-banded, so a nonzero value here is
                    // genuine mic loss — a starved read cadence or RX DMA overflow.
                    // Always checked regardless of emit gating — anomalies must not be delayed.
                    if rx_deficit > 0 {
                        if !rx_deficit_warned {
                            let rx_expected =
                                rx_window_us.saturating_mul(I2S_SAMPLE_RATE_HZ as u64) / 1_000_000;
                            log::warn!(
                                "capture: RX mic deficit {} frames this window (delivered {} of ~{} expected) — mic read starved or RX DMA overflow (further deficits suppressed until recovery)",
                                rx_deficit,
                                rx_frames_delivered,
                                rx_expected,
                            );
                            rx_deficit_warned = true;
                        }
                    } else {
                        rx_deficit_warned = false;
                    }
                    tx_chunks_written = 0;
                    tx_write_stats = TxWriteStats::default();
                    tx_empty_polls = 0;
                    tx_preroll_waits = 0;
                    tx_preroll_rearms = 0;
                    tx_nonempty_polls = 0;
                    tx_max_backlog = 0;
                    tx_resume_unmutes = 0;
                    tx_eoa_mutes = 0;
                    rx_frames_delivered = 0;
                    // Per-window flag: cleared each window so only a window that actually ran a
                    // tone-test suppresses its deficit. (rx_deficit_warned is a cross-window
                    // once-until-recovery latch and is NOT reset here.)
                    tone_test_this_window = false;
                    summary_window_count = summary_window_count.wrapping_add(1);
                    tx_summary_window_start = std::time::Instant::now();
                }

                // ── RX mic read ─────────────────────────────────────────────
                // NON_BLOCK: returns immediately. ESP_ERR_TIMEOUT = DMA empty (normal).
                let bytes_read =
                    match driver.read(&mut dma_buf, esp_idf_svc::hal::delay::NON_BLOCK) {
                        Ok(n) => n,
                        Err(e) => {
                            use esp_idf_svc::sys::ESP_ERR_TIMEOUT;
                            if e.code() != ESP_ERR_TIMEOUT {
                                log::warn!("capture: i2s read error: {:?}", e);
                            }
                            FreeRtos::delay_ms(I2S_POLL_SLEEP_MS);
                            continue;
                        }
                    };

                if bytes_read == 0 {
                    FreeRtos::delay_ms(I2S_POLL_SLEEP_MS);
                    continue;
                }
                if bytes_read % 8 != 0 {
                    log::warn!(
                        "capture: sub-frame read ({} bytes, not a multiple of 8) — DMA anomaly?",
                        bytes_read
                    );
                }

                // 32-bit stereo: 8 B/frame. Left-slot top-16 at [base+2, base+3].
                let frames = bytes_read / 8;
                // Accumulate uncapped frame count before the .min(320) cap. This feeds the
                // per-window `rx_deficit_frames` telemetry (design §3.6): a shortfall against the
                // I2S clock's frame count is loss from ANY cause. A ground-truth per-cause DMA
                // overflow count would need the HAL's `on_recv_q_ovf` callback, but esp-idf-hal
                // 0.46.2 keeps `rx_handle` private and folds that callback into the same dispatcher
                // as `on_recv`, so it cannot be wired without patching the HAL.
                rx_frames_delivered = rx_frames_delivered.wrapping_add(frames as u64);
                let frames = frames.min(320); // cap to dma_buf capacity

                let now_us = unsafe { esp_idf_svc::sys::esp_timer_get_time() } as u64;

                // Write mono samples into the capture ring, unless a HIL test has
                // quiesced capture and owns the ring. The flag is checked under the
                // lock so that once the test has set it and taken the lock for its
                // pre-fill, no later mic chunk can land; the whole commit body —
                // writes, head advance, and anchor refresh — is skipped so the thread
                // does not fight the test producer's anchor stamps.
                {
                    let mut guard = CAPTURE_RING.lock().unwrap_or_else(|_| {
                        panic!("CAPTURE_RING mutex poisoned in capture thread")
                    });
                    if !CAPTURE_QUIESCED.load(Ordering::Acquire) {
                        let ring = guard.as_mut().unwrap_or_else(|| {
                            panic!("CAPTURE_RING is None in capture thread — boot init bug")
                        });
                        let ridx = RingIndex::new(RING_CAPACITY_SAMPLES);

                        for i in 0..frames {
                            let base = i * 8;
                            let sample = i16::from_le_bytes([dma_buf[base + 2], dma_buf[base + 3]]);
                            let slot = ridx.slot(ring.write_head);
                            ring.samples[slot] = sample;
                            ring.write_head += 1;
                        }

                        ring.anchor_sample = ring.write_head.saturating_sub(1);
                        ring.anchor_ts_us = now_us;
                    }
                }

                FreeRtos::delay_ms(I2S_POLL_SLEEP_MS);
            }
        })
        .expect("capture: thread spawn failed — heap exhausted?");
    drop(handle);

    // Restore the prior pthread config so later spawns are unaffected by the pin.
    match prev_cfg {
        Some(cfg) => cfg.set().expect("failed to restore prior spawn config"),
        None => ThreadSpawnConfiguration::default()
            .set()
            .expect("failed to restore default spawn config"),
    }
}

/// Normalized lag-1 autocorrelation from pre-accumulated sums.
///
/// Returns r1 in [-1, 1] (0.0 when `sq_sum == 0`). The i64→f32 cast loses ≲0.01
/// relative — safe given the `AUTOCORR_FLOOR` margin (0.2 vs expected ~0.68).
fn autocorr_lag1_from_sums(lag1_sum: i64, sq_sum: i64) -> f32 {
    if sq_sum == 0 {
        0.0
    } else {
        (lag1_sum as f32) / (sq_sum as f32)
    }
}

/// I2S waveform sanity test.
///
/// Waits for the capture ring to fill, then checks that the audio is correlated
/// (lag-1 autocorrelation > `AUTOCORR_FLOOR`), not dead, not frozen, and not
/// saturated. No minimum-loudness floor — a quiet room passes.
#[cfg(target_os = "espidf")]
pub(crate) fn run_i2s_waveform_sanity() -> (Status, Payload) {
    // ── Wait for ring to fill ──────────────────────────────────────────────────
    let poll_t0_us = unsafe { esp_idf_svc::sys::esp_timer_get_time() } as u64;
    loop {
        let held_now = {
            let guard = CAPTURE_RING
                .lock()
                .unwrap_or_else(|_| panic!("CAPTURE_RING mutex poisoned (ring-fill poll)"));
            match guard.as_ref() {
                None => {
                    return test_report_fail(
                        "FAIL src=ring capture ring not initialized — firmware bug",
                    );
                }
                Some(r) => RingIndex::new(RING_CAPACITY_SAMPLES).held(r.write_head) as usize,
            }
        };
        if held_now >= I2S_WAVEFORM_SANITY_SAMPLES {
            break;
        }
        let now_us = unsafe { esp_idf_svc::sys::esp_timer_get_time() } as u64;
        if now_us.saturating_sub(poll_t0_us) >= I2S_WAVEFORM_SANITY_RING_FILL_TIMEOUT_US {
            return test_report_fail_fmt(format_args!(
                "FAIL src=ring reason=ring-not-filled samples={} needed={}",
                held_now, I2S_WAVEFORM_SANITY_SAMPLES,
            ));
        }
        FreeRtos::delay_ms(10);
    }

    // ── Compute waveform stats under the lock ──────────────────────────────────
    let mut s_min: i32 = i32::MAX;
    let mut s_max: i32 = i32::MIN;
    let mut sq_sum: i64 = 0;
    let mut sat: u32 = 0;
    // Lag-1 autocorrelation (i64 avoids overflow). sq_sum doubles as the denominator;
    // the off-by-one vs the lag1 index set is negligible at n=4000.
    let mut lag1_sum: i64 = 0;
    let mut prev_sv: i64 = 0;
    let samples_captured;

    {
        let guard = CAPTURE_RING
            .lock()
            .unwrap_or_else(|_| panic!("CAPTURE_RING mutex poisoned"));
        let ring = match guard.as_ref() {
            Some(r) => r,
            None => {
                return test_report_fail(
                    "FAIL src=ring capture ring not initialized — firmware bug",
                );
            }
        };
        let ridx = RingIndex::new(RING_CAPACITY_SAMPLES);
        let held = ridx.held(ring.write_head) as usize;
        // write_head is monotone, so held can't shrink below what we polled — consistency assert.
        let n_samples = held.min(I2S_WAVEFORM_SANITY_SAMPLES);
        if n_samples == 0 {
            return test_report_fail("FAIL src=ring samples=0 capture not yet started");
        }
        samples_captured = n_samples;
        let start = ring.write_head - n_samples as u64;
        for i in 0..n_samples {
            let slot = ridx.slot(start + i as u64);
            let sv = ring.samples[slot] as i32;
            if sv < s_min {
                s_min = sv;
            }
            if sv > s_max {
                s_max = sv;
            }
            sq_sum += (sv as i64) * (sv as i64);
            if sv >= I2S_SATURATION_ABS || sv <= -I2S_SATURATION_ABS {
                sat += 1;
            }
            if i > 0 {
                let sv64 = sv as i64;
                lag1_sum += sv64 * prev_sv;
            }
            prev_sv = sv as i64;
        }
    }

    let n = samples_captured as f32;
    let rms = ((sq_sum as f32) / n).sqrt();
    let sat_pct = ((sat as f32 / n) * 100.0) as u32;
    let max_abs = s_max.abs().max(s_min.abs());
    let spread = s_max - s_min;
    let sat_frac = sat as f32 / n;

    let r1 = autocorr_lag1_from_sums(lag1_sum, sq_sum);
    // Milli-units integer. Round to match the host's strict-greater-than gate.
    let ac1_milli = (r1 * 1000.0 + 0.5) as i32;

    // No minimum-loudness (rms) floor — a quiet room must pass. rms reported for observability.
    let live = (max_abs > ZERO_ABS_THRESHOLD as i32)
        && (spread > STUCK_SPREAD_FLOOR)
        && (sat_frac < SATURATION_FRAC_MAX)
        && (r1 > AUTOCORR_FLOOR);

    if live {
        test_report_ok(TestData::I2sWaveform {
            min: s_min,
            max: s_max,
            rms: rms as i32,
            sat_pct,
            samples: samples_captured as u32,
            ac1: ac1_milli,
        })
    } else {
        let reason = if max_abs <= ZERO_ABS_THRESHOLD as i32 {
            "all-zero"
        } else if spread <= STUCK_SPREAD_FLOOR {
            "stuck-constant"
        } else if sat_frac >= SATURATION_FRAC_MAX {
            "saturated"
        } else {
            "low-autocorr"
        };
        test_report_fail_fmt(format_args!(
            "FAIL src=ring reason={} ch min={} max={} rms={} sat={}% samples={} ac1={}",
            reason, s_min, s_max, rms as i32, sat_pct, samples_captured, ac1_milli,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PLAYBACK_PREROLL_TARGET_BYTES, PrerollGate, TxStager, autocorr_lag1_from_sums,
        next_preroll_target,
    };

    // ── PrerollGate ────────────────────────────────────────────────────────────

    #[test]
    fn preroll_gate_new_is_armed_at_base() {
        let g = PrerollGate::new();
        assert!(g.pending, "new gate is armed");
        assert!(g.first_chunk_at.is_none(), "new gate has no fallback clock");
        assert_eq!(g.target, PLAYBACK_PREROLL_TARGET_BYTES);
        assert!(!g.saw_nonempty_since_empty);
    }

    #[test]
    fn preroll_gate_arm_base_rebases_and_clears_clock() {
        let mut g = PrerollGate::new();
        // Simulate a live, escalated, mid-stream state.
        g.pending = false;
        g.first_chunk_at = Some(std::time::Instant::now());
        g.target = next_preroll_target(PLAYBACK_PREROLL_TARGET_BYTES);
        g.saw_nonempty_since_empty = true;

        g.arm_base();
        assert!(g.pending);
        assert!(
            g.first_chunk_at.is_none(),
            "arm_base clears the fallback clock"
        );
        assert_eq!(
            g.target, PLAYBACK_PREROLL_TARGET_BYTES,
            "arm_base re-bases to base"
        );
        assert!(!g.saw_nonempty_since_empty);
    }

    #[test]
    fn preroll_gate_rearm_escalated_escalates_and_clears_clock() {
        let mut g = PrerollGate::new();
        g.pending = false;
        g.first_chunk_at = Some(std::time::Instant::now());
        g.saw_nonempty_since_empty = true;
        let expected = next_preroll_target(g.target);

        g.rearm_escalated();
        assert!(g.pending);
        assert!(
            g.first_chunk_at.is_none(),
            "rearm_escalated clears the fallback clock"
        );
        assert_eq!(
            g.target, expected,
            "rearm_escalated escalates via next_preroll_target"
        );
        assert!(!g.saw_nonempty_since_empty);
    }

    #[test]
    fn preroll_gate_on_end_of_audio_boundary_sets_mute_and_rebases() {
        let mut g = PrerollGate::new();
        g.pending = false;
        g.first_chunk_at = Some(std::time::Instant::now());
        g.target = next_preroll_target(PLAYBACK_PREROLL_TARGET_BYTES);
        g.saw_nonempty_since_empty = true;
        let mut mute_armed = false;

        g.on_end_of_audio_boundary(&mut mute_armed);
        assert!(mute_armed, "boundary arms the mute");
        assert!(g.pending);
        assert!(g.first_chunk_at.is_none());
        assert_eq!(
            g.target, PLAYBACK_PREROLL_TARGET_BYTES,
            "boundary re-bases to base"
        );
        assert!(!g.saw_nonempty_since_empty);
    }

    // ── TxStager ───────────────────────────────────────────────────────────────

    #[test]
    fn tx_stager_new_has_no_residue() {
        let s = TxStager::new(2560);
        assert!(!s.has_residue(), "a fresh stager holds nothing");
    }

    #[test]
    fn tx_stager_stage_then_residue_until_drained() {
        let mut s = TxStager::new(2560);
        s.stage(640);
        assert!(s.has_residue(), "a staged run with cursor at 0 has residue");
        // Simulate the DMA accepting the whole run.
        s.cursor = 640;
        assert!(!s.has_residue(), "fully-drained run has no residue");
        // A partial acceptance leaves residue.
        s.stage(640);
        s.cursor = 100;
        assert!(s.has_residue(), "partial acceptance leaves residue");
    }

    #[test]
    fn tx_stager_reset_discards_residue_and_wedge_state() {
        let mut s = TxStager::new(2560);
        s.stage(640);
        s.cursor = 100;
        s.wedge_since = Some(std::time::Instant::now());
        s.wedge_warned = true;

        s.reset();
        assert!(!s.has_residue(), "reset discards staged residue");
        assert!(s.wedge_since.is_none(), "reset clears the wedge clock");
        assert!(!s.wedge_warned, "reset clears the wedge warn latch");
    }

    // ── autocorr_lag1_from_sums ────────────────────────────────────────────────

    #[test]
    fn autocorr_all_zero() {
        let r1 = autocorr_lag1_from_sums(0, 0);
        assert_eq!(r1, 0.0, "all-zero: r1 must be 0.0");
    }

    #[test]
    fn autocorr_constant_signal() {
        let n = 10_i64;
        let v: i64 = 1000;
        let sq_sum = n * v * v;
        let lag1_sum = (n - 1) * v * v;
        let r1 = autocorr_lag1_from_sums(lag1_sum, sq_sum);
        // r1 ≈ (n-1)/n = 0.9 (denominator has one extra x[0]² term)
        assert!(
            r1 > 0.85,
            "constant signal: r1 must be close to 1.0, got {r1}"
        );
    }

    #[test]
    fn autocorr_alternating_signal() {
        let n = 10_i64;
        let v: i64 = 1000;
        let sq_sum = n * v * v;
        let lag1_sum = -(n - 1) * v * v;
        let r1 = autocorr_lag1_from_sums(lag1_sum, sq_sum);
        assert!(
            r1 < -0.85,
            "alternating signal: r1 must be close to -1.0, got {r1}"
        );
    }

    /// Near-zero lag1_sum relative to sq_sum → r1 ≈ 0 (uncorrelated noise).
    #[test]
    fn autocorr_random_noise_near_zero() {
        let lag1_sum: i64 = 10;
        let sq_sum: i64 = 10_000;
        let r1 = autocorr_lag1_from_sums(lag1_sum, sq_sum);
        assert!(
            r1.abs() < 0.3,
            "RNG-like inputs (tiny lag1_sum vs sq_sum) must yield r1 near zero, got {r1}"
        );
        assert!(r1 > -1.0 && r1 < 1.0, "r1 must be in [-1, 1], got {r1}");
    }

    // ── detail length budget ───────────────────────────────────────────────────
    //
    // `test_report_fail_fmt` truncates rather than panicking, so this pins the
    // worst-case FAIL diagnostic under a conservative budget well inside
    // `device_protocol::TEST_RESULT_MSG_CAP` — truncating it would drop the trailing
    // numeric context an operator reads first.

    #[test]
    fn fail_detail_length_budget() {
        let worst_case = "FAIL src=ring reason=stuck-constant ch min=-32768 max=32767 \
             rms=32767 sat=100% samples=4000 ac1=-1000";
        assert!(
            worst_case.len() <= 127,
            "worst-case FAIL detail ({} bytes) exceeds 127-byte budget (conservative, well under TestResultMsg cap): {:?}",
            worst_case.len(),
            worst_case
        );
    }
}
