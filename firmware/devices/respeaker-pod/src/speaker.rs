//! Speaker / playback path for the respeaker-pod firmware.
//!
//! Owns the firmware-local sine source, the playback-request seam the HIL
//! `SpeakerOutput` test drives, the inbound-PCM streaming ring producer/consumer
//! statics, the `run_playback_sequence` / `speaker_stream_init` codec + I2S
//! bring-up the capture thread calls inline, and the `SpeakerOutput` /
//! `CapturePeriodicLine` / `PlaybackDrainRate` HIL self-test handlers.

// Host view: these items exist for the tests and for the device-gated call sites.
#![cfg_attr(not(target_os = "espidf"), allow(dead_code))]

use audio_pipeline::playback::{
    expand_sample_to_frame, InboundRingConsumer, InboundRingProducer, I2S_TX_FRAME_BYTES,
};
#[cfg(target_os = "espidf")]
use audio_pipeline::playback::{Accepted, I2sStreamSink, PlaybackSink};
#[cfg(target_os = "espidf")]
use device_protocol::{Payload, Status};
#[cfg(target_os = "espidf")]
use esp_idf_svc::hal::{
    delay::FreeRtos,
    i2s::{I2sBiDir, I2sDriver},
};
use std::sync::Mutex;

#[cfg(target_os = "espidf")]
use crate::aic3104::{
    aic3104_dac_mute_best_effort, aic3104_dac_unmute, aic3104_init, Aic3104InitError,
};
use crate::capture::I2S_SAMPLE_RATE_HZ;
#[cfg(target_os = "espidf")]
use crate::i2c::{ms_to_ticks, I2C_BUS};
#[cfg(target_os = "espidf")]
use device_protocol::{test_report_fail, test_report_fail_fmt, test_report_ok, TestData};

// ── Firmware-local sine source (speaker bring-up) ────────────────────────────

// A small phase-continuous sine generator that runs ON the ESP32-S3, used by the
// `SpeakerOutput` HIL self-test to synthesize a test tone. It deliberately does NOT
// import the host `audio_receiver::playback::SineSource`: that lives in a host-only
// crate and pulling it in would couple this bring-up to `audio-receiver` for no
// benefit. The firmware binary is a std build, so `f32::sin` is available on-device.
//
// The generator produces `i16` mono samples (`next_sample`) and packs them into the
// 32-bit stereo I2S TX byte layout the hardware uses: each 8-byte frame carries the
// mono content in the LEFT slot, occupying the top 16 bits of the 32-bit slot as a
// little-endian i16 (`[lo, hi]` at bytes [base+2, base+3]), with the low 16 bits and
// the entire right slot written as zero. This mirrors how the RX path extracts the
// left slot. `fill_silence` emits all-zero stereo frames for the pre-roll / post-roll
// windows.

/// I2S TX `write_all` per-call timeout.
///
/// Deliberately a **named TX constant**, not the borrowed `I2C_CTRL_TIMEOUT_TICKS`
/// (an I2C *control* bound unrelated to a DMA-buffer drain). `write_all` loops on the
/// returned byte count, calling `write` repeatedly; this bounds a single `write`'s wait
/// for one DMA buffer to drain. One esp-idf-hal DMA buffer is 240 frames ≈ 15 ms at
/// 16 kHz, so 100 ms is generous headroom while still bounding a hung TX channel.
///
/// Whether `write`/`write_all` on a `Role::Target` TX channel blocks to the I2S clock
/// or returns immediately is a hardware behavior this timeout does not depend on
/// either way: if it returns faster than real time, the tone-emit loop is hand-paced
/// instead (see `run_playback_sequence`).
#[cfg(target_os = "espidf")]
pub(crate) const I2S_TX_WRITE_TIMEOUT_TICKS: u32 = ms_to_ticks(100);

/// Pre-roll / post-roll silence window before the DAC unmute and after the tone.
/// Sized in **frames** (not wall-clock) so the silence is defined by bytes actually
/// written: `write_all` cannot truncate it. 1600 frames at 16 kHz = 100 ms —
/// comfortably long enough to demonstrate the I2S line carries silence before the DAC
/// unmutes (so its soft-step rides against silence under the always-on amp) and after
/// the tone, before the DAC re-mutes and TX stops.
#[cfg(target_os = "espidf")]
const PLAYBACK_SILENCE_FRAMES: usize = 1600;

/// Number of stereo frames generated per TX `write_all` chunk during the tone.
/// 320 frames × 8 B = 2560 B, matching the capture-side DMA chunk size. The tone loop
/// emits `ceil(duration_frames / chunk)` chunks; the last chunk is partial.
#[cfg(target_os = "espidf")]
const PLAYBACK_TX_CHUNK_FRAMES: usize = 320;

/// Byte size of the capture thread's reusable I2S buffers (RX DMA reads and TX
/// `write_all` chunks): 320 stereo frames × 8 B/frame = 2560 B.
///
/// These buffers are **allocated once at capture-thread startup on the heap** and
/// reused across every loop iteration — never re-allocated on the stack per call.
/// The capture thread runs on an 8 KB stack and, during a tone test, nests the RX
/// chunk buffer, the playback TX buffer, and the codec/amp call chain at once; a
/// fresh 2560-byte stack array per call overflowed that stack
/// (`A stack overflow in task pthread`). Heap-once-reused keeps the hot loop
/// allocation-free while bounding the thread's stack to its call frames.
#[cfg(target_os = "espidf")]
pub(crate) const CAPTURE_I2S_BUF_BYTES: usize = PLAYBACK_TX_CHUNK_FRAMES * I2S_TX_FRAME_BYTES;

/// Number of DMA descriptors in the shared bidir I2S ring (`dma_desc_num`).
///
/// The `I2sBiDir` driver on I2S0 applies one descriptor geometry to **both** the RX
/// and TX channels. Total ring capacity = `I2S_DMA_DESC_NUM × I2S_DMA_FRAME_NUM`
/// frames; at 6 × 320 = 1,920 frames ≈ 120 ms at 16 kHz on each channel. Named
/// explicitly (rather than left at the esp-idf-hal implicit default) so descriptor
/// geometry is an editable, bench-tunable knob.
#[cfg(target_os = "espidf")]
pub(crate) const I2S_DMA_DESC_NUM: u32 = 6;

/// Frames per DMA descriptor in the shared bidir I2S ring (`dma_frame_num`).
///
/// Set to `PLAYBACK_TX_CHUNK_FRAMES` (320) so each `write_all` of one 320-frame chunk
/// spans exactly **one** descriptor rather than straddling a descriptor boundary — the
/// esp-idf-hal default of 240 would make a 320-frame `write_all` span two 240-frame
/// descriptors, which was suspected to cause a per-chunk double-wait on TX drain.
#[cfg(target_os = "espidf")]
pub(crate) const I2S_DMA_FRAME_NUM: u32 = PLAYBACK_TX_CHUNK_FRAMES as u32;

/// Silence margin written into the TX DMA **before** each DAC mute/unmute I2C write. The
/// playback sequence runs on the single capture thread, so it cannot push I2S frames
/// *and* issue the I2C volume write at the same instant — the two are sequential. This
/// margin buffers zeros in the TX DMA so it has silence to clock out across the brief
/// I2C round-trip; the DAC soft-step is itself clocked by the XVF3800's continuous
/// BCLK, so it steps against silence even while the thread is busy on I2C. 320 frames
/// at 16 kHz = 20 ms — comfortably longer than one DAC-volume I2C round-trip at
/// 100 kHz. (Caveat: if the TX DMA ever underruns to *garbage* rather than repeating
/// the last silent buffer, the codec would soft-step against non-silence; this margin
/// exists to keep the DMA fed, and that assumption should be re-verified on hardware
/// if a pop is ever heard on unmute.)
#[cfg(target_os = "espidf")]
const PLAYBACK_DAC_MUTE_SILENCE_MARGIN_FRAMES: usize = 320;

/// Settle window of silence written **after** the DAC unmute so the soft-step completes
/// before tone samples start. Per the TI datasheet the soft-step runs at one
/// step/sample over ≤128 0.5 dB steps — under ~10 ms at 16 kHz — so 320 frames (20 ms)
/// covers it with margin. If the default soft-step rate ever proves audible on unmute,
/// lengthening this window (and/or the amp settle) is the de-pop knob to retune — not
/// register `0x2A`, which is HP-driver-only and cannot move the speaker-path click.
#[cfg(target_os = "espidf")]
pub(crate) const PLAYBACK_DAC_UNMUTE_SETTLE_FRAMES: usize = 320;

/// Continuous sine-wave sample generator with phase accumulation.
///
/// Maintains phase across calls so successive samples/frames are phase-continuous
/// (no click at frame boundaries). Mirrors the proven host `SineSource` shape
/// (`freq_hz` / `amplitude` / `sample_rate`, phase-continuous fill).
struct SineSource {
    /// Current phase in [0.0, 2π).
    phase: f32,
    /// Phase increment per sample = 2π × freq_hz / sample_rate_hz.
    phase_increment: f32,
    /// Amplitude (fraction of full-scale, already clamped to [0,1]) × i16::MAX.
    amplitude_scaled: f32,
}

impl SineSource {
    /// Create a new sine source starting at phase 0.
    ///
    /// `amplitude` is clamped to [0.0, 1.0] before scaling to i16 range.
    fn new(freq_hz: f32, amplitude: f32, sample_rate_hz: u32) -> Self {
        let amplitude_clamped = amplitude.clamp(0.0, 1.0);
        Self {
            phase: 0.0,
            phase_increment: 2.0 * std::f32::consts::PI * freq_hz / sample_rate_hz as f32,
            amplitude_scaled: amplitude_clamped * i16::MAX as f32,
        }
    }

    /// Produce the next phase-continuous mono `i16` sample and advance phase.
    fn next_sample(&mut self) -> i16 {
        let raw = self.phase.sin() * self.amplitude_scaled;
        // The amplitude was already clamped to 1.0 × i16::MAX; this clamp is a
        // safety net for floating-point rounding at the extremes.
        let sample = raw.clamp(i16::MIN as f32, i16::MAX as f32) as i16;
        self.phase += self.phase_increment;
        // Wrap phase to [0, 2π) to prevent precision loss over time.
        if self.phase >= 2.0 * std::f32::consts::PI {
            self.phase -= 2.0 * std::f32::consts::PI;
        }
        sample
    }

    /// Fill `out` with phase-continuous 32-bit stereo I2S frames carrying the tone in
    /// the left slot (MSB-aligned in the top 16 bits), right slot silent.
    ///
    /// `out.len()` must be a multiple of [`I2S_TX_FRAME_BYTES`]; any trailing partial
    /// frame is left untouched. Each frame is laid out little-endian as
    /// `[0, 0, lo, hi,  0, 0, 0, 0]` — the mono sample's low byte at `[base+2]` and
    /// high byte at `[base+3]`, matching the RX left-slot extraction.
    fn fill_frames(&mut self, out: &mut [u8]) {
        for frame in out.chunks_exact_mut(I2S_TX_FRAME_BYTES) {
            let sample = self.next_sample();
            frame.copy_from_slice(&expand_sample_to_frame(sample));
        }
    }
}

/// Fill `out` with all-zero 32-bit stereo frames (silence) for the pre-roll /
/// post-roll windows that keep the amp from emitting white noise on enable/teardown.
/// `out.len()` must be a multiple of [`I2S_TX_FRAME_BYTES`]; any trailing partial
/// frame is left untouched (the caller sizes the buffer in frames).
fn fill_silence(out: &mut [u8]) {
    for frame in out.chunks_exact_mut(I2S_TX_FRAME_BYTES) {
        frame.fill(0);
    }
}

// ── Playback request seam (speaker bring-up) ─────────────────────────────────

// I2S0 (the bidir driver) is owned for process lifetime by the capture thread and
// cannot be borrowed cross-thread cheaply (`split()` borrows `&mut self`). So the
// capture thread is the single I2S agent for both directions: the HIL test handler
// (a different thread) asks it to play a tone via this request/response seam, and the
// capture loop services the request inline between RX reads.
//
// Idiom: a capacity-1 `sync_channel<PlaybackRequest>`. Each request carries the tone
// parameters plus its own oneshot reply channel (a capacity-1 `SyncSender<PlaybackOutcome>`),
// so the requester blocks on the reply while the capture thread runs the playback
// sequence and reports the structured outcome. Capacity 1 is sufficient: only one HIL
// test runs at a time, and a second concurrent request would block the requester rather
// than racing the single I2S agent.

/// Tone parameters for an on-device playback request.
///
/// The capture thread synthesizes a tone with these parameters and pushes it out the
/// I2S TX channel. `amplitude` is a 0.0..=1.0 fraction of full scale (clamped by
/// `SineSource::new`).
#[derive(Clone, Copy, Debug)]
#[cfg(target_os = "espidf")]
pub(crate) struct PlaybackParams {
    freq_hz: f32,
    amplitude: f32,
    duration_ms: u32,
}

/// A playback request: tone parameters plus the oneshot reply channel the capture
/// thread uses to report the structured `PlaybackOutcome` back to the requester.
#[cfg(target_os = "espidf")]
pub(crate) struct PlaybackRequest {
    pub(crate) params: PlaybackParams,
    pub(crate) reply: std::sync::mpsc::SyncSender<PlaybackOutcome>,
}

/// Structured result of running the on-device playback sequence.
///
/// The HIL handler maps this to a `key=value` `TestResult` message: `Ok` → `PASS …
/// codec=ok`, each failure variant → the corresponding `FAIL … reason=<codec-init|
/// amp-enable|codec|i2s-write> …` (the DAC-unmute fault uses `reason=codec
/// reg=0x2b|0x2c`). Carries enough detail for the handler to localize the fault
/// without re-running.
#[derive(Clone, Debug)]
#[cfg(target_os = "espidf")]
pub(crate) enum PlaybackOutcome {
    /// The full sequence ran clean: codec init + read-back passed, the DAC unmuted and
    /// re-muted (each read-back verified), and the whole tone + silence windows wrote
    /// without an I2S error. (There is no amp toggle: the amp is always-on hardware and
    /// the cmd-0 GPO write is read-only — see `run_amp_always_on_gpo_inert`.) Carries
    /// the played tone parameters for the PASS message.
    Ok {
        freq_hz: f32,
        amplitude: f32,
        duration_ms: u32,
    },
    /// The I2C bus singleton was not initialized — a firmware boot bug, not a hardware
    /// fault. The amp was never enabled.
    BusUnavailable,
    /// `aic3104_init` failed (write error, read-back I2C error, or read-back mismatch).
    /// The amp was never enabled. Carries the structured codec error for `reg=…`.
    CodecInitFailed(Aic3104InitError),
    /// The DAC unmute (`0x2B`/`0x2C = 0x00`) failed — I2C write error, read-back I2C
    /// error, or read-back mismatch — *after* the amp was enabled. A silently
    /// stuck-muted DAC (amp on, no audio) is caught here programmatically instead of by
    /// the ear: it is reported as a distinct `reason=codec` FAIL, never a silent
    /// `codec=ok`. The sequence ran teardown (DAC-mute → post-roll → amp-disable)
    /// before returning, so the device is quiescent. Carries the structured codec
    /// error for `reg=…`.
    DacUnmuteFailed(Aic3104InitError),
    /// An I2S TX `write_all` failed mid-sequence (pre-roll, tone, or post-roll). The
    /// sequence jumped to amp-disable before returning, so the device is quiescent.
    /// Carries the raw `esp_err_t` code and which phase failed.
    I2sWriteFailed { phase: PlaybackPhase, code: i32 },
}

/// Which I2S phase failed, for the `I2sWriteFailed` diagnostic.
#[derive(Clone, Copy, Debug)]
#[cfg(target_os = "espidf")]
pub(crate) enum PlaybackPhase {
    /// `tx_enable()` itself failed — the TX channel never reached RUNNING, so no write
    /// was attempted. Distinct from a write failure so the FAIL message is not
    /// misattributed to a pre-roll write fault (a driver/bidir-init bug vs. a DMA fault).
    TxEnable,
    PreRollSilence,
    /// The silence margin fed into the TX DMA *before* the DAC unmute I2C write (and
    /// the analogous pre-mute margin before teardown), so the DMA has zeros to clock
    /// out across the I2C round-trip. Distinct from `PreRollSilence` so a TX fault here
    /// is not misread as a pre-roll failure (different device state: the amp is ON).
    DacSilenceMargin,
    /// The silence settle window written *after* the DAC unmute, so the soft-step
    /// completes before tone samples start. Distinct phase for the same diagnostic
    /// reason as `DacSilenceMargin`.
    DacUnmuteSettle,
    Tone,
    PostRollSilence,
}

/// Capacity for the playback request channel (capacity-1: one HIL test at a time).
#[cfg(target_os = "espidf")]
pub(crate) const PLAYBACK_CHAN_CAPACITY: usize = 1;

/// Process-lifetime sender half of the playback request channel.
///
/// Populated in `main()` before the capture thread is spawned. The HIL test handler
/// clones nothing — it borrows the sender under the lock, builds a oneshot reply
/// channel, sends the `PlaybackRequest`, and blocks on the reply. `None` until the
/// channel is wired in `main()`; a request that fires before then has no I2S agent to
/// serve it (the capture thread takes the receiver at startup).
#[cfg(target_os = "espidf")]
pub(crate) static PLAYBACK_REQUEST_TX: Mutex<Option<std::sync::mpsc::SyncSender<PlaybackRequest>>> =
    Mutex::new(None);

/// Stream idle-mute debounce margin.
///
/// Playout delay between an explicit end-of-audio boundary (or a dropped
/// connection) being reached and the capture-thread DAC soft-mute (design §3.4).
///
/// The mute decision is decoupled from data starvation — a stalled pipeline never
/// mutes. Instead the capture thread arms the mute only when a drain
/// run reaches an end-of-audio mark, then waits this long with no further writes so
/// the banked tail already committed to the TX DMA plays out before the analog
/// stage soft-steps to zero. 200 ms comfortably exceeds the ~90–120 ms I2S DMA ring
/// depth (so a snippet's tail is fully clocked out before the mute — no clipped
/// tail) and carries headroom for a shared-`I2C_BUS` stall against the 20 Hz
/// telemetry poller. If new PCM is written before the delay elapses (host ended one
/// stream and immediately began another) the arming clears and no mute/unmute pair
/// fires. Correctness does not depend on the exact value; it is a
/// playout/quiescence-timing knob that can be retuned.
#[cfg(target_os = "espidf")]
pub(crate) const STREAM_EOA_MUTE_DELAY_MS: u64 = 200;

/// Pre-roll / minimum-fill **fallback timeout** for a fresh inbound stream (milliseconds).
///
/// On a (re)connection boundary the capture-thread drain loop arms pre-roll and buffers up to
/// `PLAYBACK_PREROLL_TARGET_BYTES` (7_680 raw bytes = 240 ms; defined in `audio_pipeline::playback`)
/// before it begins playing, so the consumer cold-starts with a jitter cushion already in hand
/// instead of draining from depth 0. This constant bounds how long it waits: if the target depth
/// is not reached within `PLAYBACK_PREROLL_MAX_WAIT_MS` **of the first chunk arriving**, the gate
/// clears and the consumer plays whatever is buffered. That guarantees forward progress for a
/// short/sparse stream that never reaches the target — a single snippet shorter than the target
/// depth starts ~500 ms late (worst case) but plays in full, no underrun, no lost audio.
///
/// Design-delta-14 §2 raises this from 250 ms alongside the 80 ms → 240 ms preroll bump. Filling a
/// 240 ms target at exactly-real-time delivery takes 240 ms, a hair under the old 250 ms fallback —
/// jittery real-time delivery would trip the fallback and start playback below target, silently
/// defeating the prefill knob. 500 ms ≈ 2× the base-target real-time fill time keeps the fallback
/// from preempting a healthy fill while still hard-bounding onset under sustained sub-real-time
/// delivery. It is a latency/robustness knob that can be retuned at the bench once more reconnect
/// captures accumulate. Lives in `speaker.rs` because it is consumed only by the capture-thread
/// gate call, not by the host-unit-testable pure predicate in `audio_pipeline::playback`.
#[cfg(target_os = "espidf")]
pub(crate) const PLAYBACK_PREROLL_MAX_WAIT_MS: u64 = 500;

/// I2S-wedge threshold: continuous zero-accept span above which the TX is treated as wedged
/// (microseconds).
///
/// Under NON_BLOCK streaming writes (design §3.6) a slow write no longer exists — a write
/// either accepts bytes immediately or accepts zero because the DMA ring is full. The
/// steady-state DMA frees ~one write-unit (~20 ms of audio) per poll, so a healthy TX never
/// spends more than a few tens of ms accepting nothing while committed audio is pending.
/// 200 ms of *continuous* zero-acceptance with data waiting and the speaker up is therefore a
/// stuck DMA/codec (an I2S wedge), not backpressure: it matches the STREAM_EOA_MUTE_DELAY_MS
/// scale and sits well clear of the normal per-poll gap. The wedge only gates a diagnostic
/// warn — ring backpressure propagates to TCP regardless.
///
/// Consumed by [`is_tx_wedged`] (and its host-unit tests) and by the NON_BLOCK-write path in
/// `spawn_capture_thread`, which clocks the zero-accept span and gates a once-until-recovery
/// `log::warn!` on the predicate.
pub(crate) const TX_WEDGE_WARN_US: u64 = 200_000;

/// Is the I2S TX wedged? (design §3.6 edge case I)
///
/// Pure predicate: `true` iff the DMA has accepted zero bytes for longer than
/// [`TX_WEDGE_WARN_US`] (`zero_accept_us`) while committed audio is still pending
/// (`has_data`) and the speaker is up (`speaker_ready`). The duration alone is meaningless
/// without pending data (an idle TX legitimately accepts nothing) and a downed speaker never
/// stages audio to wedge on, hence the three-input AND.
/// Extracted as a pure function so the decision is host-unit-tested without the I2S driver or
/// a clock; the caller owns the zero-accept clock and the once-until-recovery warn latch.
pub(crate) fn is_tx_wedged(zero_accept_us: u64, has_data: bool, speaker_ready: bool) -> bool {
    has_data && speaker_ready && zero_accept_us > TX_WEDGE_WARN_US
}

/// RX-deficit jitter dead-band, per-mille of the window's expected frame count.
///
/// The mic RX and playback TX are slaved to one I2S clock, so a window delivers close to the
/// clock's frame count — but the per-window delivered count is quantized to the ~5 ms poll
/// cadence: the summary is emitted before that pass's mic read, so frames straddling a window
/// boundary land in the next window. The boundary error is up to one poll period (~80 frames at
/// 16 kHz), which is not real loss. The band is set well above that — 2 % ≈ 320 frames ≈ 4 poll
/// periods — so healthy-hardware jitter never reports (design §3.6 edge case K); the ~48 %
/// starvation this step eliminates (~7 700 frames) still sits ~24× above it.
const RX_DEFICIT_DEADBAND_PERMILLE: u64 = 20; // 2 %

/// Per-window mic RX-deficit, in frames (design §3.6 "RX-loss counter").
///
/// Pure computation of the telemetry number the requirement demands — loss observable as a
/// number, not reader arithmetic. RX and TX share one I2S clock, so a window of `window_us`
/// should deliver `window_us × I2S_SAMPLE_RATE_HZ / 1_000_000` mic frames; the shortfall
/// against the `rx_frames_delivered` the loop actually read is the deficit. Loss from ANY
/// cause — a starved read cadence, RX DMA overflow — surfaces here, which is why it is a
/// computed deficit rather than a HAL overflow callback.
///
/// A jitter dead-band ([`RX_DEFICIT_DEADBAND_PERMILLE`]) zeroes a deficit below 2 % of the
/// expected count. Returns 0 when the read meets or exceeds expected (the read can never
/// inflate above the clock). Extracted as a pure function so the deadband decision is
/// host-unit-tested without the capture thread or the I2S clock; the caller owns the
/// once-until-recovery warn latch and the tone-test-window suppression.
pub(crate) fn rx_deficit_frames(window_us: u64, rx_frames_delivered: u64) -> u64 {
    let expected = window_us.saturating_mul(I2S_SAMPLE_RATE_HZ as u64) / 1_000_000;
    let deficit = expected.saturating_sub(rx_frames_delivered);
    if deficit.saturating_mul(1000) < expected.saturating_mul(RX_DEFICIT_DEADBAND_PERMILLE) {
        0
    } else {
        deficit
    }
}

/// Should the drain loop re-arm pre-roll on this empty first poll? (design §3.3)
///
/// Pure predicate over the non-empty→empty *transition* — the channel delivered a chunk on a
/// recent poll (`saw_nonempty`) and now reads empty while the DAC is still streaming
/// (`dac_active`) — gated on `!preroll_pending`: re-arming while already rebuilding lead
/// would be redundant, and excluding it keeps the escalating-target doubling
/// ([`next_preroll_target`](audio_pipeline::playback::next_preroll_target)) firing exactly
/// once per underrun edge, not once per empty poll. Excluding the bare "empty while
/// `dac_active`" case is the load-bearing false-positive suppression: that bare condition
/// holds for the whole 200 ms end-of-stream window and would false-fire on every utterance
/// boundary. The re-arm (and the co-located underrun-proxy warn it guards) is rate-limited to
/// once per non-empty→empty streak by the caller clearing `saw_nonempty_since_empty` on the
/// edge.
///
/// Extracted as a pure function — mirroring [`is_tx_wedged`] — so the re-arm edge decision is
/// host-unit-tested without the capture thread, the ring, or a clock (design §3.3, §5). The
/// caller still owns the side effects: re-arming the pre-roll gate and escalating its target.
pub(crate) fn should_rearm_preroll(
    saw_nonempty: bool,
    dac_active: bool,
    preroll_pending: bool,
) -> bool {
    saw_nonempty && dac_active && !preroll_pending
}

/// Process-lifetime producer half of the inbound-PCM streaming ring.
///
/// Populated in `main()` before the capture thread is spawned, mirroring
/// `PLAYBACK_REQUEST_TX`. Every `I2sStreamSink` resolves this static at build time
/// (`build_inbound_stream_sink`) and **clones** the producer half into the sink — the
/// producer is `Clone` and all writes are `Mutex`-serialized, so the live streamer sink
/// and the HIL handlers all feed the one production ring. `None` until wired in `main()`.
pub(crate) static INBOUND_PCM_PRODUCER: Mutex<Option<InboundRingProducer>> = Mutex::new(None);

/// Process-lifetime consumer half of the inbound-PCM streaming ring.
///
/// Populated in `main()` at the same `InboundPcmRing::split()` as the producer. Stashed
/// here (rather than threaded through `spawn_capture_thread`) so the producer-side boot
/// wiring is self-contained; the capture-thread drain loop `take`s it from this static.
/// `None` until wired in `main()`; the capture thread panics (= reboot under
/// panic=abort) if it finds `None` at spawn, since that means a boot-ordering bug.
pub(crate) static INBOUND_PCM_CONSUMER: Mutex<Option<InboundRingConsumer>> = Mutex::new(None);

// ── Speaker-output tone defaults ─────────────────────────────────────────────
//
// 440 Hz / 50% amplitude / 1500 ms, matching the host `PlaybackConfig` defaults
// (`audio-receiver/src/playback.rs`). Named constants so volume/duration are
// trivially tunable at the bench.

/// Default tone frequency for the `SpeakerOutput` HIL test (Hz).
#[cfg(target_os = "espidf")]
const SPEAKER_TONE_FREQ_HZ: f32 = 440.0;
/// Default tone amplitude for the `SpeakerOutput` HIL test (0.0..=1.0 of full scale).
#[cfg(target_os = "espidf")]
const SPEAKER_TONE_AMPLITUDE: f32 = 0.5;
/// Default tone duration for the `SpeakerOutput` HIL test (ms).
#[cfg(target_os = "espidf")]
const SPEAKER_TONE_DURATION_MS: u32 = 1500;

/// HIL handler for `TestName::SpeakerOutput`.
///
/// Posts a `PlaybackRequest` (the default tone) onto the playback seam
/// (`PLAYBACK_REQUEST_TX`), so the capture thread — the single I2S agent — runs the
/// playback sequence inline, then blocks on the oneshot reply and maps the structured
/// `PlaybackOutcome` to a `(Status, Payload::TestReport)` pair:
///
/// - PASS: `TestData::SpeakerOutput { freq, amp, dur_ms, codec_ok: true }`
/// - FAIL: detail `FAIL src=speaker reason=<codec-init|amp-enable|codec|i2s-write|bus|seam> …`
///   (the DAC-unmute fault uses `reason=codec reg=0x2b|0x2c`)
///
/// `codec_ok` is the *programmatic* signal the host's `eval_speaker_pass` checks; the
/// *acoustic* result (audible 440 Hz, correct pitch) is confirmed by the human running
/// the test — there is no loopback, so neither this handler nor the host predicate
/// asserts sound.
#[cfg(target_os = "espidf")]
pub(crate) fn run_speaker_output() -> (Status, Payload) {
    // Build a capacity-1 oneshot reply channel and post the request onto the seam.
    // The capture thread takes the request via `try_recv` each loop iteration, runs
    // `run_playback_sequence` inline, and sends the `PlaybackOutcome` back here.
    let (reply_tx, reply_rx) = std::sync::mpsc::sync_channel::<PlaybackOutcome>(1);
    let request = PlaybackRequest {
        params: PlaybackParams {
            freq_hz: SPEAKER_TONE_FREQ_HZ,
            amplitude: SPEAKER_TONE_AMPLITUDE,
            duration_ms: SPEAKER_TONE_DURATION_MS,
        },
        reply: reply_tx,
    };

    {
        let guard = PLAYBACK_REQUEST_TX
            .lock()
            .unwrap_or_else(|_| panic!("PLAYBACK_REQUEST_TX mutex poisoned"));
        match guard.as_ref() {
            // `send` blocks only if a prior request is still queued (capacity 1); the
            // capture thread drains it within one poll interval, so this is bounded.
            Some(tx) => {
                if tx.send(request).is_err() {
                    // Receiver dropped — the capture thread is gone. A firmware bug,
                    // not a hardware fault: there is no I2S agent to serve the request.
                    return test_report_fail("FAIL src=speaker reason=seam capture-thread-gone");
                }
            }
            // `None` until `main()` wires the channel before spawning the capture thread.
            None => {
                return test_report_fail("FAIL src=speaker reason=seam channel-uninitialized");
            }
        }
    }

    // Block on the oneshot reply. The full sequence (codec init + pre/post-roll + tone +
    // settles) is a few seconds — comfortably under the host's 10 s `RESPONSE_TIMEOUT`,
    // so a plain blocking `recv` is correct: the host bounds the wall-clock.
    let outcome = match reply_rx.recv() {
        Ok(o) => o,
        // The reply sender was dropped without sending — the capture thread panicked or
        // exited mid-sequence. Report it rather than blocking forever.
        Err(_) => {
            return test_report_fail("FAIL src=speaker reason=seam reply-dropped");
        }
    };

    match outcome {
        PlaybackOutcome::Ok {
            freq_hz,
            amplitude,
            duration_ms,
        } => test_report_ok(TestData::SpeakerOutput {
            freq: freq_hz as u32,
            amp: (amplitude * 100.0) as u32,
            dur_ms: duration_ms,
            codec_ok: true,
        }),
        PlaybackOutcome::BusUnavailable => {
            test_report_fail("FAIL src=speaker reason=bus i2c-singleton-uninitialized")
        }
        PlaybackOutcome::CodecInitFailed(e) => match e {
            Aic3104InitError::Write { reg, code } => test_report_fail_fmt(format_args!(
                "FAIL src=speaker reason=codec-init reg={:#04x} i2c-write-err code={}",
                reg, code
            )),
            Aic3104InitError::Readback { reg, code } => test_report_fail_fmt(format_args!(
                "FAIL src=speaker reason=codec-init reg={:#04x} i2c-readback-err code={}",
                reg, code
            )),
            Aic3104InitError::Mismatch {
                reg,
                want,
                got,
                rw_mask,
            } => test_report_fail_fmt(format_args!(
                "FAIL src=speaker reason=codec-init reg={:#04x} want={:#04x} got={:#04x} mask={:#04x}",
                reg, want, got, rw_mask
            )),
        },
        // DAC-unmute fault: a distinct codec FAIL — never a silent `codec=ok` PASS —
        // naming the DAC-volume register so the operator sees `reg=0x2b|0x2c`.
        PlaybackOutcome::DacUnmuteFailed(e) => match e {
            Aic3104InitError::Write { reg, code } => test_report_fail_fmt(format_args!(
                "FAIL src=speaker reason=codec reg={:#04x} dac-unmute-write-err code={}",
                reg, code
            )),
            Aic3104InitError::Readback { reg, code } => test_report_fail_fmt(format_args!(
                "FAIL src=speaker reason=codec reg={:#04x} dac-unmute-readback-err code={}",
                reg, code
            )),
            Aic3104InitError::Mismatch {
                reg,
                want,
                got,
                rw_mask,
            } => test_report_fail_fmt(format_args!(
                "FAIL src=speaker reason=codec reg={:#04x} want={:#04x} got={:#04x} mask={:#04x}",
                reg, want, got, rw_mask
            )),
        },
        PlaybackOutcome::I2sWriteFailed { phase, code } => {
            let phase_str = match phase {
                PlaybackPhase::TxEnable => "tx-enable",
                PlaybackPhase::PreRollSilence => "pre-roll",
                PlaybackPhase::DacSilenceMargin => "dac-silence-margin",
                PlaybackPhase::DacUnmuteSettle => "dac-unmute-settle",
                PlaybackPhase::Tone => "tone",
                PlaybackPhase::PostRollSilence => "post-roll",
            };
            test_report_fail_fmt(format_args!(
                "FAIL src=speaker reason=i2s-write phase={} code={}",
                phase_str, code
            ))
        }
    }
}

/// Runs the on-device speaker playback sequence inline on the capture thread (the single
/// I2S agent) and returns the structured outcome for the requester.
///
/// Strict ordering enforces silence-at-every-DAC-transition: every DAC level change
/// soft-steps against a silent line, so the always-on amp reproduces silence across it.
/// (The amp itself is always-on hardware — the GPO cmd-0 write is read-only, see
/// `AmpAlwaysOnGpoInert` — so it is never an off/on lever; teardown is click-safe via the
/// DAC soft-mute, not via cutting the amp.)
///
/// 1. `aic3104_init` — codec init, DAC muted. On error → FAIL.
/// 2. `tx_enable`, then pre-roll silence so the line demonstrably carries silence.
/// 3. Unmute the DAC (silence margin → unmute, read-back-verified → settle silence) so it
///    soft-steps up against the silent line. On fault → FAIL (`reason=codec`); teardown
///    still runs.
/// 4. Emit the tone for `duration_ms` (skipped if the unmute failed).
/// 5. Mute the DAC first (silence margin → mute, best-effort) so it soft-steps *down*
///    against the silent line before teardown — no click. Then post-roll silence.
/// 6. `tx_disable` — quiescent teardown (DAC muted + TX stopped; RX stays enabled).
///
/// From step 3 on, failures are recorded and fall through to the unconditional teardown
/// (mute → post-roll → `tx_disable`) rather than returning early, so a fault anywhere
/// still leaves the device quiescent **and** click-free.
///
/// I2C transactions (steps 1/3/5) each lock `I2C_BUS` for that phase only — never across
/// the multi-second I2S writes (steps 2/4/5), which touch only the thread-local bidir
/// driver. `BusUnavailable` is returned if the bus singleton is missing (a boot bug).
///
/// # TX pacing
/// Whether `write_all` on a `Role::Target` TX channel blocks to the 16 kHz I2S clock or
/// returns immediately is not yet verified on the bench. This does **not** hand-pace: it
/// relies on `write_all` blocking until the DMA drains. If the bench ever shows it
/// returning faster than real time, add a `FreeRtos::delay_ms` per chunk sized to the
/// chunk's playout time — confirm which mechanism applies before trusting tone pitch.
#[cfg(target_os = "espidf")]
pub(crate) fn run_playback_sequence(
    driver: &mut I2sDriver<'static, I2sBiDir>,
    params: PlaybackParams,
    // Reusable TX chunk buffer, allocated once at capture-thread startup and passed in
    // (never stack-allocated per call — see `CAPTURE_I2S_BUF_BYTES`). Must be at least
    // `PLAYBACK_TX_CHUNK_FRAMES * I2S_TX_FRAME_BYTES` bytes; the caller sizes it exactly.
    tx_buf: &mut [u8],
) -> PlaybackOutcome {
    // No defensive amp-disable precedes codec init: the amp is always-on hardware (the
    // GPO cmd-0 write is read-only, see `AmpAlwaysOnGpoInert`), so there is no amp-off
    // lever. The soft reset's white-noise hazard is handled by the DAC starting muted.
    {
        let mut guard = I2C_BUS
            .lock()
            .unwrap_or_else(|_| panic!("I2C_BUS mutex poisoned in playback"));
        let d = match guard.as_mut() {
            Some(d) => d,
            None => return PlaybackOutcome::BusUnavailable,
        };
        if let Err(e) = aic3104_init(d) {
            return PlaybackOutcome::CodecInitFailed(e);
        }
    }

    // `tx_buf` (passed in by the caller) is the single chunk buffer reused for every TX
    // write (silence + tone) — allocated once at capture-thread startup on the heap, not
    // a fresh stack array per call (see `CAPTURE_I2S_BUF_BYTES`).
    debug_assert!(
        tx_buf.len() >= PLAYBACK_TX_CHUNK_FRAMES * I2S_TX_FRAME_BYTES,
        "run_playback_sequence: tx_buf too small for a full TX chunk"
    );

    // tx_enable must precede any write (write_all requires the RUNNING state). Boot-time
    // `speaker_stream_init` already leaves TX RUNNING for the process lifetime, and a
    // prior tone test's re-bring-up flow re-enables it before the next write — so
    // whenever this runs, TX is usually already enabled and `tx_enable()` returns
    // `ESP_ERR_INVALID_STATE` (259). That's the desired state, not a fault; any other
    // error is a real driver/bidir-init fault, surfaced as the distinct TxEnable phase.
    if let Err(e) = driver.tx_enable() {
        if e.code() == esp_idf_svc::sys::ESP_ERR_INVALID_STATE {
            log::debug!(
                "playback: tx_enable reports already-enabled (ESP_ERR_INVALID_STATE) — TX already RUNNING, proceeding"
            );
        } else {
            log::warn!("playback: tx_enable failed: {:?}", e);
            return PlaybackOutcome::I2sWriteFailed {
                phase: PlaybackPhase::TxEnable,
                code: e.code(),
            };
        }
    }
    if let Err(outcome) = write_silence_frames(
        driver,
        &mut *tx_buf,
        PLAYBACK_SILENCE_FRAMES,
        PlaybackPhase::PreRollSilence,
    ) {
        // Pre-roll failed before the DAC was ever unmuted: stop TX and return.
        let _ = driver.tx_disable();
        return outcome;
    }

    // From here, failures are recorded and fall through to the unconditional teardown
    // below (mute → post-roll → tx_disable) instead of returning early, so the device
    // always ends quiescent and click-free.

    // Unmute the DAC so it soft-steps up to 0 dB against the silent line (SLAS510G
    // §10.3.4.4) — the only level change the always-on speaker sees, always a soft-step,
    // never a hard jump. The capture thread can't push I2S frames and issue the I2C write
    // at the same instant, so a silence margin is fed into the TX DMA first (zeros to
    // clock out across the I2C round-trip), then the unmute, then a settle window so the
    // soft-step completes before the tone starts. The unmute is read-back-verified: a
    // stuck-muted DAC (silent tone) is caught here, not by ear.
    //
    // `dac_unmute_result` accumulates the first error; the labeled `'unmute` block lets
    // `break 'unmute` jump straight to the unconditional teardown with that error recorded
    // instead of returning early.
    let mut dac_unmute_result: Result<(), PlaybackOutcome> = Ok(());
    'unmute: {
        // Pre-write silence margin so the TX DMA has zeros buffered across the I2C gap.
        if let Err(outcome) = write_silence_frames(
            driver,
            &mut *tx_buf,
            PLAYBACK_DAC_MUTE_SILENCE_MARGIN_FRAMES,
            PlaybackPhase::DacSilenceMargin,
        ) {
            dac_unmute_result = Err(outcome);
            break 'unmute;
        }
        {
            let mut guard = I2C_BUS
                .lock()
                .unwrap_or_else(|_| panic!("I2C_BUS mutex poisoned in playback"));
            match guard.as_mut() {
                Some(d) => {
                    if let Err(e) = aic3104_dac_unmute(d) {
                        // Silent-tone bug (amp on, DAC stuck muted) — surface a distinct codec
                        // FAIL; teardown (5a → post-roll → 6) still runs to quiesce the device.
                        dac_unmute_result = Err(PlaybackOutcome::DacUnmuteFailed(e));
                        break 'unmute;
                    }
                }
                None => {
                    dac_unmute_result = Err(PlaybackOutcome::BusUnavailable);
                    break 'unmute;
                }
            }
        }
        // Settle: continue silence so the DAC soft-step completes before tone samples start.
        if let Err(outcome) = write_silence_frames(
            driver,
            &mut *tx_buf,
            PLAYBACK_DAC_UNMUTE_SETTLE_FRAMES,
            PlaybackPhase::DacUnmuteSettle,
        ) {
            dac_unmute_result = Err(outcome);
        }
    }

    // Play the tone. Skipped if the unmute failed — nothing to play through a
    // stuck-muted DAC; fall through to the unconditional teardown below.
    let mut tone_result: Result<(), PlaybackOutcome> = Ok(());
    if dac_unmute_result.is_ok() {
        let mut source = SineSource::new(params.freq_hz, params.amplitude, I2S_SAMPLE_RATE_HZ);
        let total_frames = duration_ms_to_frames(params.duration_ms);
        let mut remaining = total_frames;
        while remaining > 0 {
            let chunk_frames = remaining.min(PLAYBACK_TX_CHUNK_FRAMES);
            let chunk_bytes = chunk_frames * I2S_TX_FRAME_BYTES;
            source.fill_frames(&mut tx_buf[..chunk_bytes]);
            if let Err(e) = driver.write_all(&tx_buf[..chunk_bytes], I2S_TX_WRITE_TIMEOUT_TICKS) {
                log::warn!("playback: tone write_all failed: {:?}", e);
                tone_result = Err(PlaybackOutcome::I2sWriteFailed {
                    phase: PlaybackPhase::Tone,
                    code: e.code(),
                });
                break;
            }
            remaining -= chunk_frames;
        }
    }

    // Unconditional teardown: re-mute the DAC *before* TX stops so it soft-steps *down*
    // (SLAS510G §10.3.4.4) against the silent line rather than snapping to zero under the
    // always-on amp — the click-safe teardown lever (the amp itself is never cut). Feeds a
    // silence margin into the TX DMA first, same reasoning as the unmute above. Best-effort:
    // a failed mute doesn't abort teardown (TX still stops below and `auto_clear` holds the
    // line at silence); worst case is a cosmetic click. The margin write is likewise
    // best-effort — its failure must not skip the `tx_disable` below.
    let _ = write_silence_frames(
        driver,
        &mut *tx_buf,
        PLAYBACK_DAC_MUTE_SILENCE_MARGIN_FRAMES,
        PlaybackPhase::DacSilenceMargin,
    );
    {
        if let Some(d) = I2C_BUS
            .lock()
            .unwrap_or_else(|_| panic!("I2C_BUS mutex poisoned in playback"))
            .as_mut()
        {
            aic3104_dac_mute_best_effort(d);
        } else {
            // Structurally impossible today (the static never reverts to None after
            // codec init above saw Some), but logged so a future mutable-bus refactor
            // that trips this leaves evidence instead of a silent skip.
            log::warn!("playback step 5a: I2C_BUS unavailable, skipping DAC mute");
        }
    }

    // Post-roll silence before TX stops is load-bearing even on the tone-failure path —
    // the line must not go abruptly quiet into the always-on amp. If both the tone and
    // this fail, the tone error (which happened first) is the one surfaced below; TX
    // stops regardless, so the device still ends quiescent.
    let post_result = write_silence_frames(
        driver,
        &mut *tx_buf,
        PLAYBACK_SILENCE_FRAMES,
        PlaybackPhase::PostRollSilence,
    );

    // Stop TX unconditionally, including on a mid-tone/post-roll failure, so the device
    // never ends with a stopped (non-silent) line. RX stays enabled.
    let _ = driver.tx_disable();

    // Surface the first failure in sequence order (unmute → tone → post-roll); the
    // teardown above has already quiesced the device in every case.
    if let Err(outcome) = dac_unmute_result {
        return outcome;
    }
    if let Err(outcome) = tone_result {
        return outcome;
    }
    if let Err(outcome) = post_result {
        return outcome;
    }

    PlaybackOutcome::Ok {
        freq_hz: params.freq_hz,
        amplitude: params.amplitude,
        duration_ms: params.duration_ms,
    }
}

/// Writes `frames` of silence to the TX channel in `tx_buf`-sized chunks. `tx_buf` must
/// be a whole number of frames. On a `write_all` error returns
/// `Err(I2sWriteFailed{phase, …})` so the caller can localize and tear down.
#[cfg(target_os = "espidf")]
pub(crate) fn write_silence_frames(
    driver: &mut I2sDriver<'static, I2sBiDir>,
    tx_buf: &mut [u8],
    frames: usize,
    phase: PlaybackPhase,
) -> Result<(), PlaybackOutcome> {
    let chunk_frames = tx_buf.len() / I2S_TX_FRAME_BYTES;
    fill_silence(tx_buf); // buffer stays all-zero across chunks; fill once.
    let mut remaining = frames;
    while remaining > 0 {
        let n = remaining.min(chunk_frames);
        let bytes = n * I2S_TX_FRAME_BYTES;
        if let Err(e) = driver.write_all(&tx_buf[..bytes], I2S_TX_WRITE_TIMEOUT_TICKS) {
            log::warn!(
                "playback: silence write_all failed (phase={:?}): {:?}",
                phase,
                e
            );
            return Err(PlaybackOutcome::I2sWriteFailed {
                phase,
                code: e.code(),
            });
        }
        remaining -= n;
    }
    Ok(())
}

/// Frames in a tone of `duration_ms` at the fixed 16 kHz sample rate.
fn duration_ms_to_frames(duration_ms: u32) -> usize {
    (duration_ms as usize * I2S_SAMPLE_RATE_HZ as usize) / 1000
}

/// Structured failure from `speaker_stream_init`, naming the phase that faulted so the
/// boot log localizes a bring-up failure. Logged, not propagated: a failed init leaves
/// the speaker-ready flag false and the capture thread continues to capture (RX is
/// independent of TX).
///
/// `allow(dead_code)`: the per-variant payload is consumed only through the derived
/// `Debug` in `log::warn!`, which dead-code analysis doesn't see as a use — the fields
/// are real diagnostic payload, not unused state.
#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
#[cfg(target_os = "espidf")]
pub(crate) enum SpeakerStreamInitError {
    /// The shared `I2C_BUS` singleton was `None` (a boot-ordering bug — the bus is
    /// initialized before the capture thread spawns).
    BusUnavailable,
    /// `aic3104_init` failed (codec write / read-back fault). Carries the structured
    /// codec error for the boot log.
    CodecInit(Aic3104InitError),
    /// `driver.tx_enable()` failed (could not start the TX channel). Carries the raw
    /// `esp_err_t` code.
    TxEnable { code: i32 },
    /// `aic3104_dac_unmute` failed (write / read-back fault). Carries the structured codec
    /// error for the boot log.
    DacUnmute(Aic3104InitError),
}

/// Boot-time persistent codec/DAC bring-up for the streaming speaker path. Runs **once**
/// at capture-thread startup (after the warmup discard, before the capture loop) and
/// establishes the process-lifetime "codec/DAC up + settled, TX enabled, line at silence"
/// state the streaming DAC-mute gate and service-loop writes require. (The amp is
/// always-on hardware — the GPO cmd-0 write is read-only, see `AmpAlwaysOnGpoInert` — so
/// there is no amp-off lever.)
///
/// 1. `aic3104_init` — codec init, DAC muted. Already pays the 100 ms DAC power-up settle
///    internally (`AIC3104_DAC_POWERUP_SETTLE_MS`, a CPU sleep with TX not yet enabled) —
///    the load-bearing de-pop window, paid once at boot instead of per snippet. On error
///    → `CodecInit` (TX never enabled).
/// 2. `driver.tx_enable()` — start TX (`write_all` requires RUNNING). The channel is
///    `auto_clear=true`, so once RUNNING it emits zeros on underrun rather than replaying
///    a stale buffer. On error → `TxEnable`.
/// 3. `aic3104_dac_unmute` — soft-step the DAC up to 0 dB against the `auto_clear` silent
///    line (SLAS510G §10.3.4.4), inaudible because the line is silent, not because of any
///    amp state. On error → `DacUnmute`.
///
/// Idempotent: `aic3104_init` re-applies the table from a soft reset; `tx_enable` and
/// `aic3104_dac_unmute` are no-ops if already in that state. This lets the capture thread
/// re-run it after a one-shot tone test (whose teardown leaves TX disabled + DAC muted)
/// before the next streaming write.
///
/// I2C transactions (steps 1, 3) each lock `I2C_BUS` for that phase only, never across
/// `tx_enable` (step 2 touches only the thread-local driver) — mirroring
/// `run_playback_sequence`'s scoped guards.
#[cfg(target_os = "espidf")]
pub(crate) fn speaker_stream_init(
    driver: &mut I2sDriver<'static, I2sBiDir>,
) -> Result<(), SpeakerStreamInitError> {
    // Step 1: init the codec (pays the internal 100 ms DAC power-up settle).
    {
        let mut guard = I2C_BUS
            .lock()
            .unwrap_or_else(|_| panic!("I2C_BUS mutex poisoned in speaker_stream_init"));
        let d = match guard.as_mut() {
            Some(d) => d,
            None => return Err(SpeakerStreamInitError::BusUnavailable),
        };
        if let Err(e) = aic3104_init(d) {
            return Err(SpeakerStreamInitError::CodecInit(e));
        }
    }

    // Step 2: enable TX (no I2C lock held).
    if let Err(e) = driver.tx_enable() {
        return Err(SpeakerStreamInitError::TxEnable { code: e.code() });
    }

    // Step 3: unmute the DAC (silent because TX is RUNNING with auto_clear=true).
    {
        let mut guard = I2C_BUS
            .lock()
            .unwrap_or_else(|_| panic!("I2C_BUS mutex poisoned in speaker_stream_init"));
        let d = match guard.as_mut() {
            Some(d) => d,
            None => return Err(SpeakerStreamInitError::BusUnavailable),
        };
        if let Err(e) = aic3104_dac_unmute(d) {
            return Err(SpeakerStreamInitError::DacUnmute(e));
        }
    }

    Ok(())
}

// ── Playback sink abstraction ─────────────────────────────────────────────────
//
// `PlaybackSink` trait, `is_valid_s16le_pcm`, `LogCountdown`, and `I2sStreamSink` live
// in `audio_pipeline::playback`. The HIL-only `CountingSink` lives in `inbound.rs`.
//
// The inbound audio and control frames this sink consumes must arrive over an
// authenticated transport; without that, playback injection and the
// `FlushPlayback`/`EndOfAudio` mute paths are open to any reachable host.

/// Build an `I2sStreamSink` wired to the `INBOUND_PCM_PRODUCER` ring half.
///
/// Clones (not takes) the producer so the live streamer and HIL handlers can share
/// the same production ring. Multiple producer handles serialize through the ring
/// mutex. If the producer is `None` (firmware-ordering bug), the sink drops all chunks.
#[cfg(target_os = "espidf")]
pub(crate) fn build_inbound_stream_sink() -> I2sStreamSink {
    let producer = {
        let guard = INBOUND_PCM_PRODUCER
            .lock()
            .unwrap_or_else(|_| panic!("INBOUND_PCM_PRODUCER mutex poisoned"));
        guard.clone()
    };
    if producer.is_none() {
        log::warn!(
            "streamer: I2sStreamSink built without a ring producer (unwired) — inbound audio will be dropped"
        );
    }
    I2sStreamSink::with_producer(producer)
}

// ── CapturePeriodicLine HIL self-test ────────────────────────────────────

// The capture thread emits a periodic summary log line once per ~1 s.
// Feed duration must span at least two emit cadences so the host collects ≥2 lines.
#[cfg(target_os = "espidf")]
const CAPTURE_PERIODIC_LINE_FEED_MS: u64 = 2_500;
// Match the ~20 ms inbound streaming cadence so the capture thread drains at its
// normal rate rather than in a burst that trips channel-full drops.
const CAPTURE_PERIODIC_LINE_CHUNK_MS: u64 = 20;
// One 20 ms chunk of 16 kHz / 16-bit / mono PCM = 320 samples = 640 bytes.
const CAPTURE_PERIODIC_LINE_CHUNK_BYTES: usize =
    (I2S_SAMPLE_RATE_HZ as usize / 1000) * (CAPTURE_PERIODIC_LINE_CHUNK_MS as usize) * 2;

/// Capture-thread periodic-summary-line self-test.
///
/// Feeds inbound audio through the production playback path for
/// `CAPTURE_PERIODIC_LINE_FEED_MS` so the capture thread emits its periodic
/// `capture: playback tx …` summary log at least twice. The host eval asserts
/// those lines appeared at cadence. Returns [`TestData::CapturePeriodicLine`].
#[cfg(target_os = "espidf")]
pub(crate) fn run_capture_periodic_line() -> (Status, Payload) {
    use std::time::{Duration, Instant};

    // Non-silent PCM chunk. Exact waveform doesn't matter (test asserts summary-line
    // cadence, not audio fidelity), but non-zero keeps it valid for the sink.
    let chunk = vec![0x11u8; CAPTURE_PERIODIC_LINE_CHUNK_BYTES];

    // Production-wired sink — clones the ring producer the capture thread drains.
    let mut sink = build_inbound_stream_sink();

    let mut chunks_fed: u32 = 0;
    let start = Instant::now();
    while start.elapsed().as_millis() as u64 <= CAPTURE_PERIODIC_LINE_FEED_MS {
        sink.accept(&chunk);
        chunks_fed = chunks_fed.wrapping_add(1);
        std::thread::sleep(Duration::from_millis(CAPTURE_PERIODIC_LINE_CHUNK_MS));
    }

    log::info!(
        "CapturePeriodicLine: fed {} chunks over ~{} ms (production capture thread emits the \
         periodic summary line)",
        chunks_fed,
        CAPTURE_PERIODIC_LINE_FEED_MS,
    );
    test_report_ok(TestData::CapturePeriodicLine { chunks_fed })
}

// ── PlaybackDrainRate HIL self-test ──────────────────────────────────────

// Drives a steady, at-least-real-time inbound playback feed so the capture
// thread drains under saturation and emits its periodic summary lines. The host
// eval reads per-chunk drain timing and asserts healthy 16 kHz bounds.

// Feed duration spans several ~1 s emit windows for a robust sample.
#[cfg(target_os = "espidf")]
const PLAYBACK_DRAIN_RATE_FEED_MS: u64 = 5_000;
// Yield one full FreeRTOS tick (10 ms) when the ring is full, via
// `FreeRtos::delay_ms` (vTaskDelay). Must be a real scheduler yield — sub-tick
// requests busy-wait via usleep, starving core 0's idle task and triggering the
// Task WDT after 5 s (its ISR backtrace corrupts the COBS stream). A full-tick
// delay lets idle run and resets the WDT.
#[cfg(target_os = "espidf")]
const PLAYBACK_DRAIN_RATE_FULL_YIELD_MS: u32 = 10;

// Full-duplex mic-RX-integrity feed spans the same several ~1 s emit windows as the
// drain-rate feed: the capture thread must service RX at its 16 kHz cadence throughout
// the TX-drain-bound load for its per-window `rx_deficit` telemetry to be conclusive.
#[cfg(target_os = "espidf")]
const FULL_DUPLEX_RX_FEED_MS: u64 = 5_000;

/// Drive a steady, at-least-real-time inbound playback feed through the production path
/// for `feed_ms`, keeping the inbound PCM ring saturated so the capture thread is held
/// TX-drain-bound (and, for the full-duplex test, must service mic RX concurrently under
/// that load). Feeds non-silent PCM chunks in a tight loop, yielding one FreeRTOS tick
/// only on ring-full backpressure. Returns `(chunks_fed, feed_full)` — the host reads the
/// actual drain / RX figures from the capture thread's periodic log lines.
///
/// Shared by `run_playback_drain_rate` (asserts the raw-drain rate) and
/// `run_full_duplex_rx_integrity` (asserts the mic-RX deficit is zero): both need the same
/// saturating feed and differ only in which periodic-line telemetry the host scores.
#[cfg(target_os = "espidf")]
fn feed_saturating_playback(feed_ms: u64) -> (u32, u32) {
    use std::time::Instant;

    // Non-silent PCM chunk (same shape as CapturePeriodicLine).
    let chunk = vec![0x11u8; CAPTURE_PERIODIC_LINE_CHUNK_BYTES];

    // Production-wired sink — chunks flow through the real inbound PCM ring.
    let mut sink = build_inbound_stream_sink();

    let mut chunks_fed: u32 = 0;
    let mut feed_full: u32 = 0;
    let start = Instant::now();
    while start.elapsed().as_millis() as u64 <= feed_ms {
        match sink.accept(&chunk) {
            // Enqueued (or silently discarded if the channel is dead — the sink
            // returns Enqueued either way). A dead channel means no periodic lines,
            // so the host eval fails independently. No sleep — keep the ring full.
            Accepted::Enqueued => {
                chunks_fed = chunks_fed.wrapping_add(1);
            }
            // Ring full — yield one FreeRTOS tick to let the capture thread drain.
            Accepted::Full => {
                feed_full = feed_full.wrapping_add(1);
                FreeRtos::delay_ms(PLAYBACK_DRAIN_RATE_FULL_YIELD_MS);
            }
        }
    }
    (chunks_fed, feed_full)
}

/// Playback drain-rate self-test.
///
/// Runs the shared saturating feed for `PLAYBACK_DRAIN_RATE_FEED_MS`, keeping the ring
/// saturated so the capture thread is always drain-bound. Reports feed-side backpressure
/// counts; the host reads the actual drain figures from the capture thread's periodic log
/// lines.
///
/// Returns [`TestData::PlaybackDrainRate`].
#[cfg(target_os = "espidf")]
pub(crate) fn run_playback_drain_rate() -> (Status, Payload) {
    let (chunks_fed, feed_full) = feed_saturating_playback(PLAYBACK_DRAIN_RATE_FEED_MS);

    log::info!(
        "PlaybackDrainRate: fed {} chunks ({} feed_full backpressure events) over ~{} ms \
         (production capture thread emits the periodic drain summary lines)",
        chunks_fed,
        feed_full,
        PLAYBACK_DRAIN_RATE_FEED_MS,
    );
    // tx_wf: whole-frame device→host TX drops so the host can tell a missing periodic-window
    // count apart from device-side log-frame loss (TX ring full drops a whole frame silently).
    let tx_wf = crate::console::TX_WRITE_FAILURES.load(std::sync::atomic::Ordering::Relaxed);
    test_report_ok(TestData::PlaybackDrainRate {
        chunks_fed,
        feed_full,
        feed_ms: PLAYBACK_DRAIN_RATE_FEED_MS as u32,
        tx_wf,
    })
}

/// Full-duplex mic-RX-integrity self-test.
///
/// Runs the shared saturating feed for `FULL_DUPLEX_RX_FEED_MS` so the capture thread is
/// held TX-drain-bound and must service mic RX concurrently under that load — the exact
/// condition the pre-fix blocking-TX pass starved RX under (~48 % of mic samples dropped).
/// Reports feed-side backpressure counts; the host reads the per-window `rx_deficit=`
/// telemetry from the capture thread's periodic `capture: playback obs …` lines and asserts
/// it is zero across the saturated windows.
///
/// Returns [`TestData::FullDuplexRxIntegrity`].
#[cfg(target_os = "espidf")]
pub(crate) fn run_full_duplex_rx_integrity() -> (Status, Payload) {
    let (chunks_fed, feed_full) = feed_saturating_playback(FULL_DUPLEX_RX_FEED_MS);

    log::info!(
        "FullDuplexRxIntegrity: fed {} chunks ({} feed_full backpressure events) over ~{} ms \
         (production capture thread emits the periodic rx_deficit telemetry under TX load)",
        chunks_fed,
        feed_full,
        FULL_DUPLEX_RX_FEED_MS,
    );
    test_report_ok(TestData::FullDuplexRxIntegrity {
        chunks_fed,
        feed_full,
        feed_ms: FULL_DUPLEX_RX_FEED_MS as u32,
    })
}

// ── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::{duration_ms_to_frames, fill_silence, SineSource, I2S_TX_FRAME_BYTES};

    // ── I2S-wedge threshold ───────────────────────────────────────────────

    use super::{
        is_tx_wedged, rx_deficit_frames, should_rearm_preroll, CAPTURE_PERIODIC_LINE_CHUNK_BYTES,
        TX_WEDGE_WARN_US,
    };

    /// Below/at threshold is not a wedge (strict `>`), even with data pending and speaker up.
    #[test]
    fn tx_wedge_below_and_at_threshold_is_not_wedged() {
        assert!(
            !is_tx_wedged(20_000, true, true),
            "a brief zero-accept span is normal per-poll DMA-full, not a wedge"
        );
        assert!(!is_tx_wedged(TX_WEDGE_WARN_US - 1, true, true));
        assert!(
            !is_tx_wedged(TX_WEDGE_WARN_US, true, true),
            "at threshold: not a wedge (strict >)"
        );
        assert!(!is_tx_wedged(0, true, true));
    }

    /// Fires only past the threshold AND with data pending AND the speaker up.
    #[test]
    fn tx_wedge_fires_above_threshold_with_data_and_speaker() {
        assert!(is_tx_wedged(TX_WEDGE_WARN_US + 1, true, true));
        assert!(is_tx_wedged(400_000, true, true));
        assert!(is_tx_wedged(u64::MAX, true, true));
    }

    /// No pending data → an idle TX legitimately accepts nothing; never a wedge.
    #[test]
    fn tx_wedge_suppressed_without_pending_data() {
        assert!(!is_tx_wedged(u64::MAX, false, true));
    }

    /// Speaker down → no audio is ever staged, so a long zero-accept span is not a wedge.
    #[test]
    fn tx_wedge_suppressed_when_speaker_down() {
        assert!(!is_tx_wedged(u64::MAX, true, false));
    }

    /// Pin the threshold at 200 ms (STREAM_EOA_MUTE_DELAY_MS scale, well clear of per-poll gaps).
    #[test]
    fn tx_wedge_threshold_is_200ms() {
        assert_eq!(TX_WEDGE_WARN_US, 200_000);
    }

    // ── RX-deficit counter ────────────────────────────────────────────────

    /// A full-rate window (16 000 frames/s) with the expected frame count has zero deficit.
    #[test]
    fn rx_deficit_zero_when_delivered_meets_clock() {
        // 1 s window at 16 kHz → 16 000 expected.
        assert_eq!(rx_deficit_frames(1_000_000, 16_000), 0);
        // Over-delivery (read cannot inflate above the clock) still floors at 0.
        assert_eq!(rx_deficit_frames(1_000_000, 16_100), 0);
    }

    /// A large shortfall (the ~48 % starvation this step kills) is reported in full.
    #[test]
    fn rx_deficit_reports_large_shortfall() {
        // 1 s window, half the frames delivered → 8 000 deficit, far above the deadband.
        assert_eq!(rx_deficit_frames(1_000_000, 8_000), 8_000);
    }

    /// A shortfall strictly inside the 2 % jitter dead-band reads as zero; at/past it reports.
    #[test]
    fn rx_deficit_deadband_suppresses_jitter() {
        // 1 s → 16 000 expected; 2 % = 320 frames. "< 2 %" is ignored, "≥ 2 %" reported.
        assert_eq!(
            rx_deficit_frames(1_000_000, 16_000 - 319),
            0,
            "319 frames (< 2 %) is jitter, suppressed"
        );
        assert_eq!(
            rx_deficit_frames(1_000_000, 16_000 - 320),
            320,
            "320 frames (= 2 %) is at the edge, reported"
        );
        assert_eq!(
            rx_deficit_frames(1_000_000, 16_000 - 321),
            321,
            "past the deadband reports the full deficit"
        );
    }

    /// A degenerate/zero-length window never underflows or false-positives.
    #[test]
    fn rx_deficit_zero_window_is_zero() {
        assert_eq!(rx_deficit_frames(0, 0), 0);
    }

    /// Pin chunk size at 640 bytes (320 samples × 2, 16 kHz / 20 ms / S16_LE mono).
    /// Must be even — an odd size would fail `is_valid_s16le_pcm` and silently drop all chunks.
    #[test]
    fn capture_periodic_line_chunk_bytes_is_640() {
        assert_eq!(CAPTURE_PERIODIC_LINE_CHUNK_BYTES, 640);
        assert_eq!(
            CAPTURE_PERIODIC_LINE_CHUNK_BYTES % 2,
            0,
            "must be even for S16_LE"
        );
    }

    // ── Pre-roll re-arm edge predicate ─────────────────────────────────────

    /// Re-arms on the non-empty→empty transition while the DAC is active and pre-roll is
    /// not already pending — the underrun edge that escalates the pre-roll target (design §3.3).
    #[test]
    fn rearm_preroll_fires_on_transition_while_active_not_pending() {
        assert!(
            should_rearm_preroll(true, true, false),
            "a mid-stream drain-to-empty transition while the DAC is active and not pre-rolling must re-arm"
        );
    }

    /// Already pre-rolling → suppressed: re-arming mid-rebuild would re-trigger the target
    /// escalation on every empty poll instead of once per underrun edge.
    #[test]
    fn rearm_preroll_suppressed_when_already_pending() {
        assert!(!should_rearm_preroll(true, true, true));
    }

    /// DAC inactive → normal end-of-stream idle, not an underrun to re-arm on.
    #[test]
    fn rearm_preroll_suppressed_when_dac_inactive() {
        assert!(!should_rearm_preroll(true, false, false));
    }

    /// No preceding non-empty poll → no transition, nothing to re-arm.
    #[test]
    fn rearm_preroll_requires_preceding_nonempty() {
        assert!(!should_rearm_preroll(false, true, false));
        assert!(!should_rearm_preroll(false, false, false));
    }

    // ── SineSource ───────────────────────────────────────────────────────

    /// `fill_frames` packs 32-bit stereo frames: tone in the left slot, silence in the right.
    #[test]
    fn sine_source_fills_stereo_frames() {
        const FRAMES: usize = 320;
        let mut src = SineSource::new(440.0, 0.5, 16_000);
        let mut out = [0u8; FRAMES * I2S_TX_FRAME_BYTES];
        src.fill_frames(&mut out);

        let mut left_nonzero = 0usize;
        for frame in out.chunks_exact(I2S_TX_FRAME_BYTES) {
            // Low 16 bits of the left slot are always zero (MSB-aligned content).
            assert_eq!([frame[0], frame[1]], [0, 0], "left-slot low bytes not zero");
            // Right slot is silent.
            assert_eq!(
                [frame[4], frame[5], frame[6], frame[7]],
                [0, 0, 0, 0],
                "right slot not silent"
            );
            let left = i16::from_le_bytes([frame[2], frame[3]]);
            if left != 0 {
                left_nonzero += 1;
            }
        }
        assert!(
            left_nonzero > 0,
            "expected non-zero tone samples in the left slot, got all zeros"
        );
    }

    /// Amplitude > 1.0 is clamped to near i16::MAX without overflow.
    #[test]
    fn sine_source_amplitude_clamps_to_s16() {
        const FRAMES: usize = 320;
        let mut src = SineSource::new(440.0, 2.0, 16_000);
        let mut out = [0u8; FRAMES * I2S_TX_FRAME_BYTES];
        src.fill_frames(&mut out);
        let peak = out
            .chunks_exact(I2S_TX_FRAME_BYTES)
            .map(|f| i16::from_le_bytes([f[2], f[3]]).abs())
            .max()
            .unwrap_or(0);
        assert!(
            (32_000..=i16::MAX).contains(&peak),
            "expected peak in 32_000..=i16::MAX (clamped full scale), got {peak}"
        );
    }

    /// Phase is continuous across frame boundaries: sample N is identical whether
    /// produced in a single run or split across frames.
    #[test]
    fn sine_source_phase_continuity() {
        let mut reference = SineSource::new(440.0, 0.5, 16_000);
        for _ in 0..320 {
            reference.next_sample();
        }
        let expected_index_320 = reference.next_sample();

        let mut framed = SineSource::new(440.0, 0.5, 16_000);
        for _ in 0..320 {
            framed.next_sample();
        }
        let frame2_first = framed.next_sample();

        assert_eq!(
            frame2_first, expected_index_320,
            "frame-2 first sample diverged from continuous-run sample at absolute index 320 \
             — phase is not continuous across the frame boundary"
        );
    }

    /// Identical parameters produce identical streams (determinism).
    #[test]
    fn sine_source_determinism() {
        let mut src_a = SineSource::new(440.0, 0.5, 16_000);
        let mut src_b = SineSource::new(440.0, 0.5, 16_000);
        for i in 0..1024 {
            assert_eq!(
                src_a.next_sample(),
                src_b.next_sample(),
                "fresh SineSource diverged from src_a at sample {i} — non-deterministic"
            );
        }
    }

    #[test]
    fn duration_ms_to_frames_basic() {
        assert_eq!(duration_ms_to_frames(1500), 24_000, "default 1500 ms");
        assert_eq!(duration_ms_to_frames(0), 0, "0 ms must emit no frames");
        assert_eq!(duration_ms_to_frames(100), 1_600, "100 ms at 16 kHz");
    }

    #[test]
    fn fill_silence_zeroes_all_bytes() {
        let mut out = [0xABu8; 16 * I2S_TX_FRAME_BYTES];
        fill_silence(&mut out);
        assert!(
            out.iter().all(|&b| b == 0),
            "fill_silence left non-zero bytes"
        );
    }
}
