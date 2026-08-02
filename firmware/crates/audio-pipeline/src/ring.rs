//! Ring-buffer index math for the audio capture pipeline.
//!
//! This module is pure math — no I/O, no allocation, no `std`.  The actual
//! heap-allocated sample storage lives in the firmware task; this module
//! provides the index arithmetic needed to:
//!
//! - map absolute sample indices to positions in the ring slice,
//! - detect overrun (write head has lapped the read cursor),
//! - compute the pre-roll cursor position at VAD onset,
//! - check sample-index continuity frame-to-frame.
//!
//! **Coordinate system.** All "positions" are *absolute sample indices*
//! (monotonically increasing from capture start, per channel, u64).  The ring
//! capacity is `cap` samples.  The mapping from absolute index `i` to the
//! ring slot is `i % cap`.  The write head is the index of the *next* sample
//! to be written; valid written data spans `[write_head - min(written, cap),
//! write_head)`.
//!
//! Design reference: `docs/adr/2026/06/09-audio-transport/design.md` §2.3.

// ── Constants ──────────────────────────────────────────────────────────────────

/// Ring buffer duration in seconds (2 s at 16 kHz mono = 32 000 samples = 64 KB).
pub const RING_SECONDS: u32 = 2;

/// Default sample rate.
pub const SAMPLE_RATE_HZ: u32 = 16_000;

/// Default ring capacity in samples (mono).
pub const RING_CAPACITY_SAMPLES: usize = (RING_SECONDS * SAMPLE_RATE_HZ) as usize;

/// Pre-roll duration in samples: 16 000 samples (1 s = half the ring, 50 frames).
/// Frame-aligned (16 000 / 320 = 50 exactly) so the paced drain services every
/// pre-roll sample as a whole frame, leaving no steady-state tail.
pub const PREROLL_SAMPLES: u64 = SAMPLE_RATE_HZ as u64;

// ── CaptureRing ───────────────────────────────────────────────────────────────

/// The capture ring's sample storage plus the head/anchor bookkeeping that dates it,
/// held together so a reader under one lock gets a consistent snapshot.
///
/// Generic over the backing buffer so each platform supplies its own allocation:
/// `B` is expected to deref to `[i16]` of `RING_CAPACITY_SAMPLES` samples (the ESP pod
/// passes a PSRAM buffer, a Linux pod a `Box<[i16]>`). No bound is declared here — the
/// struct is plain state with no methods of its own, and the consumers that index
/// `samples` carry the `Deref`/`DerefMut` bounds they need.
///
/// Typical callers:
/// - Capture thread: locks, writes samples, advances `write_head`, updates the anchor.
///   Hold time is one chunk write (≤ 320 samples × 2 B ≈ negligible).
/// - Streamer / self-tests: lock, read samples and/or head and anchor, unlock.
pub struct CaptureRing<B> {
    /// Sample storage, capacity = [`RING_CAPACITY_SAMPLES`].
    pub samples: B,
    /// Absolute sample index of the next slot to be written (monotonically increasing).
    pub write_head: u64,
    /// Sample index at the moment `anchor_ts_us` was recorded.
    pub anchor_sample: u64,
    /// Platform monotonic µs at the moment `anchor_sample` was captured. Only offsets
    /// within a segment are wire-visible, so the epoch is the platform's own.
    pub anchor_ts_us: u64,
}

// ── Signal sanity ─────────────────────────────────────────────────────────────

/// Normalized lag-1 autocorrelation from pre-accumulated sums.
///
/// Returns r1 in [-1, 1] (0.0 when `sq_sum == 0`). The i64→f32 cast loses ≲0.01
/// relative — safe against the autocorrelation floors the capture sanity tests use
/// (0.2 vs expected ~0.68).
pub fn autocorr_lag1_from_sums(lag1_sum: i64, sq_sum: i64) -> f32 {
    if sq_sum == 0 {
        0.0
    } else {
        (lag1_sum as f32) / (sq_sum as f32)
    }
}

/// Dead-line guard: the smallest absolute peak (`max(|min|, |max|)`) a live channel may
/// show. A window at or below it is treated as a dead / all-zero line.
///
/// This is NOT a loudness floor — a healthy mic in a quiet room produces a quiet but
/// correlated signal that must PASS. Confirmed quiet-room audio reaches `max_abs` as low
/// as 38; this sits well below that and above a truly dead line (≈0 plus a few LSB of
/// interference). The real broken-versus-working discriminator is [`AUTOCORR_FLOOR`].
pub const ZERO_ABS_THRESHOLD: i32 = 16;

/// Frozen-line guard: the smallest spread (`max − min`) a live channel may show.
///
/// Autocorrelation cannot catch a frozen line on its own — a constant value has
/// `ac1 ≈ 1.0` and sails through the autocorr gate — so this small spread floor is the
/// dedicated anti-frozen guard. A frozen / 1-bit line has spread ≈ 0–2, while confirmed
/// quiet-room audio has spread ≥ 76; 32 separates the two with margin on both sides.
pub const STUCK_SPREAD_FLOOR: i32 = 32;

/// Near-full-scale magnitude counted as clipped. Below `i16::MAX` by enough margin to
/// catch sustained clipping while allowing occasional transients.
pub const SATURATION_ABS: i32 = 32_700;

/// The fraction of clipped samples at which a channel is called saturated.
pub const SATURATION_FRAC_MAX: f32 = 0.95;

/// Normalized lag-1 autocorrelation floor — the primary health gate.
///
/// This is the real broken-versus-working discriminator: full-scale random noise (the
/// two-master clock-contention failure mode) has ac1 ≈ 0, while real acoustic audio is
/// strongly correlated (confirmed across 60 quiet-room windows, ac1 0.41–0.97 every
/// window). 0.2 sits with margin below the observed acoustic minimum and well above RNG
/// noise.
///
/// These five thresholds were calibrated on the ESP pod's hardware and are shared by
/// every liveness check in the tree. A reading that fails one on a different board is a
/// value for a human to review, not a threshold to lower quietly — and when one is
/// retuned it must move for every check at once, which is why they live here.
pub const AUTOCORR_FLOOR: f32 = 0.2;

/// What one channel's capture window looks like.
///
/// Summary only: the judgement is [`defect`](Self::defect), so a caller that reports the
/// reading and a caller that gates on it cannot disagree about what "live" means.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WaveformStats {
    /// Samples the window was accumulated from.
    pub samples: usize,
    /// Smallest sample value seen (0 for an empty window).
    pub min: i32,
    /// Largest sample value seen (0 for an empty window).
    pub max: i32,
    /// Mean of the squared samples — `rms²`. The square root is left to
    /// [`rms`](Self::rms) so the accumulation stays free of `std`'s float math.
    pub mean_square: f32,
    /// Fraction of samples at or beyond [`SATURATION_ABS`].
    pub saturated_fraction: f32,
    /// Normalized lag-1 autocorrelation over the window.
    pub autocorr_lag1: f32,
}

impl WaveformStats {
    /// Summarize one contiguous window.
    pub fn of(samples: &[i16]) -> Self {
        let mut accum = WaveformAccum::new();
        for sample in samples {
            accum.push(*sample);
        }
        accum.finish()
    }

    /// Absolute peak of the window.
    pub fn max_abs(&self) -> i32 {
        self.max.abs().max(self.min.abs())
    }

    /// Distance between the extremes of the window.
    pub fn spread(&self) -> i32 {
        self.max - self.min
    }

    /// Root mean square of the window — reported, never gated on: a quiet room must
    /// pass, so there is deliberately no loudness floor.
    #[cfg(feature = "std")]
    pub fn rms(&self) -> f32 {
        self.mean_square.sqrt()
    }

    /// Why this window is not live audio, or `None` if it is.
    ///
    /// Ordered so the most specific diagnosis wins: a dead line is also uncorrelated,
    /// and reporting it as low autocorrelation would send someone looking at the wrong
    /// thing.
    pub fn defect(&self) -> Option<&'static str> {
        if self.samples == 0 {
            Some("no samples")
        } else if self.max_abs() <= ZERO_ABS_THRESHOLD {
            Some("all-zero")
        } else if self.spread() <= STUCK_SPREAD_FLOOR {
            Some("stuck-constant")
        } else if self.saturated_fraction >= SATURATION_FRAC_MAX {
            Some("saturated")
        } else if self.autocorr_lag1 <= AUTOCORR_FLOOR {
            Some("low-autocorr")
        } else {
            None
        }
    }
}

/// The reading as a human reviewing it wants to see it — every statistic, including the
/// ones nothing gates on, because an unexpected value is reviewed before it is accepted.
#[cfg(feature = "std")]
impl core::fmt::Display for WaveformStats {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "min={} max={} rms={:.0} sat={:.0}% ac1={:.3} samples={}",
            self.min,
            self.max,
            self.rms(),
            self.saturated_fraction * 100.0,
            self.autocorr_lag1,
            self.samples
        )
    }
}

/// Accumulates [`WaveformStats`] one sample at a time.
///
/// Sample-at-a-time rather than slice-at-a-time because a window read straight out of a
/// wrapping capture ring is not contiguous; a caller with a slice uses
/// [`WaveformStats::of`].
pub struct WaveformAccum {
    samples: usize,
    min: i32,
    max: i32,
    sq_sum: i64,
    lag1_sum: i64,
    saturated: usize,
    prev: i64,
}

impl Default for WaveformAccum {
    fn default() -> Self {
        Self::new()
    }
}

impl WaveformAccum {
    /// An empty window.
    pub fn new() -> Self {
        Self {
            samples: 0,
            min: i32::MAX,
            max: i32::MIN,
            sq_sum: 0,
            lag1_sum: 0,
            saturated: 0,
            prev: 0,
        }
    }

    /// Fold one sample in, in capture order — the lag-1 term pairs it with the previous
    /// one, so out-of-order pushes report a correlation that is not the signal's.
    pub fn push(&mut self, sample: i16) {
        let value = sample as i32;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        let value64 = value as i64;
        self.sq_sum += value64 * value64;
        if value.abs() >= SATURATION_ABS {
            self.saturated += 1;
        }
        if self.samples > 0 {
            self.lag1_sum += value64 * self.prev;
        }
        self.prev = value64;
        self.samples += 1;
    }

    /// The window's summary. The accumulator is left intact, so a caller polling a
    /// filling window can read it more than once.
    pub fn finish(&self) -> WaveformStats {
        // The denominator floors at 1 so an empty window reports zeros rather than NaN;
        // `samples == 0` is what `defect` keys the "no samples" diagnosis on.
        let n = self.samples.max(1) as f32;
        WaveformStats {
            samples: self.samples,
            min: if self.samples == 0 { 0 } else { self.min },
            max: if self.samples == 0 { 0 } else { self.max },
            mean_square: (self.sq_sum as f32) / n,
            saturated_fraction: self.saturated as f32 / n,
            autocorr_lag1: autocorr_lag1_from_sums(self.lag1_sum, self.sq_sum),
        }
    }
}

// ── RingIndex ─────────────────────────────────────────────────────────────────

/// Stateless ring-index helper.  All operations are pure functions of the
/// write head and ring capacity; no mutable state is stored here.
///
/// The caller is responsible for maintaining the write head and any read
/// cursors.
#[derive(Debug, Clone, Copy)]
pub struct RingIndex {
    /// Ring capacity in samples.
    cap: u64,
}

impl RingIndex {
    /// Create a new `RingIndex` for a ring of `capacity` samples.
    ///
    /// # Panics
    /// Panics if `capacity` is zero.
    pub const fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "ring capacity must be > 0");
        Self {
            cap: capacity as u64,
        }
    }

    /// Ring capacity.
    pub const fn capacity(&self) -> u64 {
        self.cap
    }

    /// Slot index (offset into the ring slice) for absolute sample index `i`.
    #[inline]
    pub fn slot(&self, sample_index: u64) -> usize {
        (sample_index % self.cap) as usize
    }

    /// Number of samples currently held in the ring, given that `write_head`
    /// is the next-to-write index (i.e. `write_head - 1` was the last written).
    ///
    /// Returns `min(write_head, cap)`.
    #[inline]
    pub fn held(&self, write_head: u64) -> u64 {
        write_head.min(self.cap)
    }

    /// Oldest valid sample index, given `write_head`.
    ///
    /// Returns `write_head - held(write_head)`.
    #[inline]
    pub fn oldest(&self, write_head: u64) -> u64 {
        write_head - self.held(write_head)
    }

    /// Returns `true` if `sample_index` is within the valid range
    /// `[oldest, write_head)`.
    #[inline]
    pub fn is_valid(&self, write_head: u64, sample_index: u64) -> bool {
        sample_index >= self.oldest(write_head) && sample_index < write_head
    }

    /// Compute the pre-roll cursor: the read cursor the streamer should set at
    /// VAD onset, targeting `preroll_samples` of history.
    ///
    /// Returns `max(oldest(write_head), write_head - preroll_samples)`.
    /// If the ring holds fewer than `preroll_samples`, the cursor is clamped to
    /// `oldest` — this happens early in a capture run before the ring fills.
    pub fn preroll_cursor(&self, write_head: u64, preroll_samples: u64) -> u64 {
        let target = write_head.saturating_sub(preroll_samples);
        target.max(self.oldest(write_head))
    }

    /// Detect overrun: returns `true` if the write head has advanced past the
    /// read cursor (i.e. the write head is strictly ahead of the cursor by more
    /// than the ring capacity, meaning the cursor's data has been overwritten).
    ///
    /// Equivalently: `write_head > cursor + cap`.
    #[inline]
    pub fn is_overrun(&self, write_head: u64, cursor: u64) -> bool {
        write_head > cursor + self.cap
    }

    /// Number of samples available to read between `cursor` and `write_head`.
    ///
    /// If `cursor >= write_head`, returns 0.
    /// Does **not** check for overrun; callers should call `is_overrun` first
    /// if the cursor may have been lapped.
    #[inline]
    pub fn available(&self, write_head: u64, cursor: u64) -> u64 {
        write_head.saturating_sub(cursor)
    }

    /// Verify sample-index continuity: returns `true` if `got_index` is the
    /// immediately expected next sample index after `last_index`.
    ///
    /// Used by the receiver to detect gaps between `AudioFrame`s.
    ///
    /// When `frame_samples == 0`, the check reduces to `last_index == got_index`
    /// (an empty frame is continuous only if the index did not move).
    #[inline]
    pub fn is_continuous(last_index: u64, frame_samples: u64, got_index: u64) -> bool {
        last_index + frame_samples == got_index
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // Ring with capacity 8 for easy arithmetic.
    fn ring8() -> RingIndex {
        RingIndex::new(8)
    }

    // ── slot ─────────────────────────────────────────────────────────────────

    #[test]
    fn slot_maps_correctly() {
        let r = ring8();
        assert_eq!(r.slot(0), 0);
        assert_eq!(r.slot(7), 7);
        assert_eq!(r.slot(8), 0); // wrap
        assert_eq!(r.slot(9), 1);
        assert_eq!(r.slot(16), 0);
    }

    // ── held / oldest ────────────────────────────────────────────────────────

    #[test]
    fn held_below_capacity() {
        let r = ring8();
        assert_eq!(r.held(0), 0);
        assert_eq!(r.held(3), 3);
        assert_eq!(r.held(7), 7);
    }

    #[test]
    fn held_at_and_above_capacity() {
        let r = ring8();
        assert_eq!(r.held(8), 8);
        assert_eq!(r.held(9), 8);
        assert_eq!(r.held(100), 8);
    }

    #[test]
    fn oldest_before_capacity() {
        let r = ring8();
        assert_eq!(r.oldest(3), 0);
    }

    #[test]
    fn oldest_after_capacity() {
        let r = ring8();
        assert_eq!(r.oldest(10), 2);
        assert_eq!(r.oldest(16), 8);
    }

    // ── is_valid ─────────────────────────────────────────────────────────────

    #[test]
    fn is_valid_inside_window() {
        let r = ring8();
        let wh = 12u64;
        assert!(r.is_valid(wh, 4));
        assert!(r.is_valid(wh, 11));
        assert!(!r.is_valid(wh, 3)); // too old — oldest = 4
        assert!(!r.is_valid(wh, 12)); // write head itself is not yet written
    }

    // ── preroll_cursor ───────────────────────────────────────────────────────

    #[test]
    fn preroll_cursor_full_ring() {
        let r = ring8();
        // Ring full: write_head=10, preroll=4 → cursor = 10-4 = 6; oldest = 2
        assert_eq!(r.preroll_cursor(10, 4), 6);
    }

    #[test]
    fn preroll_cursor_clamped_to_oldest() {
        let r = ring8();
        // Ring not yet full: write_head=3, preroll=8 → target=0, oldest=0 → clamp to 0
        assert_eq!(r.preroll_cursor(3, 8), 0);
    }

    #[test]
    fn preroll_cursor_clamped_when_preroll_exceeds_held() {
        let r = ring8();
        // write_head=10, preroll=12 → target = 10-12 saturates to 0; oldest=2 → clamp to 2
        assert_eq!(r.preroll_cursor(10, 12), 2);
    }

    #[test]
    fn preroll_cursor_exact_capacity() {
        let r = ring8();
        // write_head=16, preroll=8 = cap → cursor = 16-8 = 8 = oldest(16)
        assert_eq!(r.preroll_cursor(16, 8), 8);
    }

    // ── is_overrun ───────────────────────────────────────────────────────────

    #[test]
    fn no_overrun_when_within_capacity() {
        let r = ring8();
        // write_head = cursor + 8 → exactly at boundary, not overrun
        assert!(!r.is_overrun(8, 0));
        assert!(!r.is_overrun(10, 5));
    }

    #[test]
    fn overrun_when_write_head_laps_cursor() {
        let r = ring8();
        // write_head = cursor + 9 → one past the boundary
        assert!(r.is_overrun(9, 0));
        assert!(r.is_overrun(100, 5));
    }

    // ── available ────────────────────────────────────────────────────────────

    #[test]
    fn available_samples() {
        let r = ring8();
        assert_eq!(r.available(10, 7), 3);
        assert_eq!(r.available(10, 10), 0);
        assert_eq!(r.available(10, 11), 0); // cursor ahead of write head
    }

    // ── is_continuous ────────────────────────────────────────────────────────

    #[test]
    fn continuity_exact() {
        assert!(RingIndex::is_continuous(100, 320, 420));
    }

    #[test]
    fn continuity_gap() {
        assert!(!RingIndex::is_continuous(100, 320, 421)); // gap of 1
        assert!(!RingIndex::is_continuous(100, 320, 419)); // overlap
    }

    /// Zero frame_samples: continuous only if got_index == last_index (no movement).
    #[test]
    fn continuity_zero_frame_samples() {
        // Empty frame at the same index: continuous.
        assert!(RingIndex::is_continuous(100, 0, 100));
        // Empty frame but index moved: not continuous.
        assert!(!RingIndex::is_continuous(100, 0, 101));
    }

    // ── RING_CAPACITY_SAMPLES constant ───────────────────────────────────────

    #[test]
    fn ring_capacity_constant() {
        assert_eq!(RING_CAPACITY_SAMPLES, 32_000);
    }

    #[test]
    fn preroll_samples_constant() {
        assert_eq!(PREROLL_SAMPLES, 16_000);
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

    // ── CaptureRing ────────────────────────────────────────────────────────────

    /// The backing buffer the real pods use is a heap allocation reached through
    /// `Deref`/`DerefMut` (`PsramBuf<i16>` on the ESP, `Box<[i16]>` on Linux), so the
    /// generic is exercised here through a boxed slice. `alloc` is linked under
    /// `cfg(test)`, so this runs in the `no_std` lane too.
    #[test]
    fn capture_ring_works_over_a_deref_backing_buffer() {
        let mut ring = CaptureRing {
            samples: alloc::vec![0i16; 8].into_boxed_slice(),
            write_head: 0,
            anchor_sample: 0,
            anchor_ts_us: 0,
        };
        let r = RingIndex::new(ring.samples.len());

        for i in 0..10u64 {
            ring.samples[r.slot(i)] = (i as i16) - 5;
            ring.write_head = i + 1;
        }
        ring.anchor_sample = 10;
        ring.anchor_ts_us = 4_242;

        assert_eq!(
            ring.samples[r.slot(9)],
            4,
            "last sample readable at its slot"
        );
        assert_eq!(
            ring.samples[r.slot(8)],
            3,
            "the wrapped sample overwrote slot 0"
        );
        assert_eq!(r.oldest(ring.write_head), 2, "the first two are lapped");

        // Same shape a drain uses to copy out.
        let view: &[i16] = &ring.samples[..];
        assert_eq!(view.len(), 8);
        assert_eq!(&view[..3], &[3i16, 4, -3]);
        assert_eq!(ring.anchor_ts_us, 4_242);
    }

    /// The struct declares no bound on `B`, so a bare array is a legal backing buffer
    /// too: it is plain state, and the consumers that index `samples` carry the bounds.
    #[test]
    fn capture_ring_holds_samples_and_anchor() {
        let mut ring = CaptureRing {
            samples: [0i16; 8],
            write_head: 0,
            anchor_sample: 0,
            anchor_ts_us: 0,
        };
        ring.samples[3] = -1234;
        ring.write_head = 4;
        ring.anchor_sample = 4;
        ring.anchor_ts_us = 987_654;

        let r = RingIndex::new(ring.samples.len());
        assert_eq!(
            ring.samples[r.slot(3)],
            -1234,
            "sample readable at its slot"
        );
        assert_eq!(r.available(ring.write_head, 0), 4);
        assert_eq!(ring.anchor_ts_us, 987_654);
    }

    // ── Waveform sanity ────────────────────────────────────────────────────────

    /// A correlated, quiet-room-shaped window: a slow sine at a level a real quiet room
    /// reaches, so it must pass every gate without a loudness floor helping it.
    fn quiet_speech(out: &mut [i16]) {
        // A 40-sample period triangle: correlated, and integer-only so the fixture is
        // the same on every target.
        for (i, sample) in out.iter_mut().enumerate() {
            let phase = (i % 40) as i32;
            let value = if phase < 20 { phase - 10 } else { 30 - phase };
            *sample = (value * 12) as i16;
        }
    }

    /// Uncorrelated full-scale noise — the shape a contended clock produces.
    fn noise(out: &mut [i16]) {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        for sample in out.iter_mut() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *sample = (state >> 48) as i16;
        }
    }

    #[test]
    fn the_calibration_constants_are_the_bench_values() {
        // Pinned so a retune is visible in test output rather than only in a diff, and
        // so it has to be made once here instead of in each pod's registry.
        assert_eq!(ZERO_ABS_THRESHOLD, 16);
        assert_eq!(STUCK_SPREAD_FLOOR, 32);
        assert_eq!(SATURATION_ABS, 32_700);
        assert_eq!(SATURATION_FRAC_MAX, 0.95);
        assert_eq!(AUTOCORR_FLOOR, 0.2);
    }

    #[test]
    fn a_quiet_but_correlated_window_passes_with_no_loudness_floor() {
        let mut window = [0i16; 4_000];
        quiet_speech(&mut window);
        let stats = WaveformStats::of(&window);
        assert_eq!(stats.defect(), None, "{stats:?}");
        assert!(stats.autocorr_lag1 > AUTOCORR_FLOOR, "{stats:?}");
        // Genuinely quiet: a loudness floor anywhere in this path would have to sit
        // below this to let a real room through.
        assert!(stats.mean_square < 100.0 * 100.0, "{stats:?}");
    }

    #[test]
    fn each_broken_shape_is_named_by_its_own_defect() {
        assert_eq!(WaveformStats::of(&[0i16; 1_000]).defect(), Some("all-zero"));
        assert_eq!(
            WaveformStats::of(&[1_000i16; 1_000]).defect(),
            Some("stuck-constant"),
            "a constant line is perfectly correlated, so only the spread guard catches it"
        );

        // A square wave at the rails: correlated, loud, and clipped.
        let mut clipped = [0i16; 1_000];
        for (i, sample) in clipped.iter_mut().enumerate() {
            *sample = if i % 500 < 499 { 32_760 } else { -32_760 };
        }
        assert_eq!(WaveformStats::of(&clipped).defect(), Some("saturated"));

        let mut uncorrelated = [0i16; 4_000];
        noise(&mut uncorrelated);
        assert_eq!(
            WaveformStats::of(&uncorrelated).defect(),
            Some("low-autocorr")
        );

        assert_eq!(WaveformStats::of(&[]).defect(), Some("no samples"));
    }

    #[test]
    fn the_most_specific_diagnosis_wins() {
        // A dead line is also uncorrelated and also has no spread; reporting either of
        // those would send someone looking at the wrong thing.
        let dead = WaveformStats::of(&[0i16; 100]);
        assert!(dead.spread() <= STUCK_SPREAD_FLOOR && dead.autocorr_lag1 <= AUTOCORR_FLOOR);
        assert_eq!(dead.defect(), Some("all-zero"));
    }

    #[test]
    fn an_accumulated_window_reads_the_same_as_a_contiguous_one() {
        let mut window = [0i16; 512];
        quiet_speech(&mut window);
        let mut accum = WaveformAccum::new();
        for sample in &window {
            accum.push(*sample);
        }
        assert_eq!(accum.finish(), WaveformStats::of(&window));
        // Reading it does not consume it: a caller polling a filling window reads twice.
        assert_eq!(accum.finish(), WaveformStats::of(&window));
    }

    #[test]
    fn an_empty_window_reports_zeros_rather_than_extremes() {
        let empty = WaveformAccum::new().finish();
        assert_eq!(empty.samples, 0);
        assert_eq!((empty.min, empty.max), (0, 0));
        assert_eq!(empty.mean_square, 0.0);
        assert_eq!(empty.saturated_fraction, 0.0);
        assert_eq!(empty.autocorr_lag1, 0.0);
    }

    #[test]
    fn saturation_counts_both_rails() {
        let mut accum = WaveformAccum::new();
        accum.push(SATURATION_ABS as i16);
        accum.push(-(SATURATION_ABS as i16));
        accum.push(0);
        accum.push(0);
        let stats = accum.finish();
        assert_eq!(stats.saturated_fraction, 0.5, "both rails count as clipped");
        assert_eq!(stats.max_abs(), SATURATION_ABS);
        assert_eq!(stats.spread(), 2 * SATURATION_ABS);
    }

    #[cfg(feature = "std")]
    #[test]
    fn rms_is_the_root_of_the_mean_square() {
        let stats = WaveformStats::of(&[300i16; 64]);
        assert!((stats.rms() - 300.0).abs() < 0.01, "{stats:?}");
    }
}
