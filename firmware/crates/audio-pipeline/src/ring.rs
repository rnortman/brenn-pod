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
}
