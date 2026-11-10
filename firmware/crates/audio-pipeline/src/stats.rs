//! The device's cumulative counters and the speech gate's published state.
//!
//! One [`PodStats`] per device (`static` on the ESP pod, `Arc` on a Linux pod). The thread
//! that owns each event adds to its counter; the streamer reads them all into a status
//! frame with [`PodStats::snapshot`]. Every counter wraps at `u32::MAX`, and readers
//! difference them modularly.

use core::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use crate::wire::PodCounters;

/// Which of the counters a device cannot measure have a source on this device. A
/// counter without one is reported as `None`, never as a count of zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CounterSources {
    /// [`PodCounters::capture_deficit_frames`] has a source.
    pub capture_deficit_frames: bool,
    /// [`PodCounters::capture_xruns`] has a source.
    pub capture_xruns: bool,
    /// [`PodCounters::playback_write_failures`] has a source.
    pub playback_write_failures: bool,
    /// [`PodCounters::playback_discards`] has a source.
    pub playback_discards: bool,
    /// [`PodCounters::alloc_failures`] has a source.
    pub alloc_failures: bool,
}

/// The bit pattern of "no gate reading since the last take": a NaN no reading stores,
/// because readings that are NaN are not stored.
const GATE_ENERGY_NONE: u32 = u32::MAX;

/// Cumulative device counters, one atomic per [`PodCounters`] field, plus the gate's
/// open state and its largest input since the last status frame.
///
/// Add to a counter with `fetch_add(n, Ordering::Relaxed)` (or [`PodStats::bump`]); the
/// add wraps, which is the counter's defined behaviour.
pub struct PodStats {
    sources: CounterSources,
    /// See [`PodCounters::capture_deficit_frames`].
    pub capture_deficit_frames: AtomicU32,
    /// See [`PodCounters::capture_xruns`].
    pub capture_xruns: AtomicU32,
    /// See [`PodCounters::playback_underruns`].
    pub playback_underruns: AtomicU32,
    /// See [`PodCounters::playback_write_failures`].
    pub playback_write_failures: AtomicU32,
    /// See [`PodCounters::playback_full_stalls`].
    pub playback_full_stalls: AtomicU32,
    /// See [`PodCounters::playback_discards`].
    pub playback_discards: AtomicU32,
    /// See [`PodCounters::onsets`].
    pub onsets: AtomicU32,
    /// See [`PodCounters::segments_opened`].
    pub segments_opened: AtomicU32,
    /// See [`PodCounters::segments_dropped`].
    pub segments_dropped: AtomicU32,
    /// See [`PodCounters::ring_overruns`].
    pub ring_overruns: AtomicU32,
    /// See [`PodCounters::gate_msg_drops`].
    pub gate_msg_drops: AtomicU32,
    /// See [`PodCounters::connects`].
    pub connects: AtomicU32,
    /// See [`PodCounters::connect_failures`].
    pub connect_failures: AtomicU32,
    /// See [`PodCounters::status_skipped`].
    pub status_skipped: AtomicU32,
    /// See [`PodCounters::alloc_failures`].
    pub alloc_failures: AtomicU32,
    gate_open: AtomicBool,
    /// `f32` bits of the largest gate input since the last [`take_gate`](Self::take_gate),
    /// or [`GATE_ENERGY_NONE`].
    gate_energy_max: AtomicU32,
}

impl PodStats {
    /// All counters at zero, the gate closed and unread.
    pub const fn new(sources: CounterSources) -> Self {
        Self {
            sources,
            capture_deficit_frames: AtomicU32::new(0),
            capture_xruns: AtomicU32::new(0),
            playback_underruns: AtomicU32::new(0),
            playback_write_failures: AtomicU32::new(0),
            playback_full_stalls: AtomicU32::new(0),
            playback_discards: AtomicU32::new(0),
            onsets: AtomicU32::new(0),
            segments_opened: AtomicU32::new(0),
            segments_dropped: AtomicU32::new(0),
            ring_overruns: AtomicU32::new(0),
            gate_msg_drops: AtomicU32::new(0),
            connects: AtomicU32::new(0),
            connect_failures: AtomicU32::new(0),
            status_skipped: AtomicU32::new(0),
            alloc_failures: AtomicU32::new(0),
            gate_open: AtomicBool::new(false),
            gate_energy_max: AtomicU32::new(GATE_ENERGY_NONE),
        }
    }

    /// Add one to `counter`, wrapping.
    pub fn bump(counter: &AtomicU32) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Add `n` to `counter`, wrapping. `n` is taken modulo 2³², which leaves every
    /// modular difference a reader takes exact.
    pub fn add(counter: &AtomicU32, n: u64) {
        counter.fetch_add(n as u32, Ordering::Relaxed);
    }

    /// Every counter as it stands, with the ones this device has no source for as
    /// `None`. Each counter is read once; the set is not one atomic snapshot, which a
    /// cumulative reader does not need.
    pub fn snapshot(&self) -> PodCounters {
        let read = |c: &AtomicU32| c.load(Ordering::Relaxed);
        let opt = |has: bool, c: &AtomicU32| has.then(|| read(c));
        let s = &self.sources;
        PodCounters {
            capture_deficit_frames: opt(s.capture_deficit_frames, &self.capture_deficit_frames),
            capture_xruns: opt(s.capture_xruns, &self.capture_xruns),
            playback_underruns: read(&self.playback_underruns),
            playback_write_failures: opt(s.playback_write_failures, &self.playback_write_failures),
            playback_full_stalls: read(&self.playback_full_stalls),
            playback_discards: opt(s.playback_discards, &self.playback_discards),
            onsets: read(&self.onsets),
            segments_opened: read(&self.segments_opened),
            segments_dropped: read(&self.segments_dropped),
            ring_overruns: read(&self.ring_overruns),
            gate_msg_drops: read(&self.gate_msg_drops),
            connects: read(&self.connects),
            connect_failures: read(&self.connect_failures),
            status_skipped: read(&self.status_skipped),
            alloc_failures: opt(s.alloc_failures, &self.alloc_failures),
        }
    }

    /// The gate thread's report: whether the gate is open now, and the gate input it
    /// just read (`None` when the read failed). A NaN reading is not stored.
    pub fn publish_gate(&self, open: bool, energy: Option<f32>) {
        self.gate_open.store(open, Ordering::Relaxed);
        let Some(energy) = energy.filter(|e| !e.is_nan()) else {
            return;
        };
        let _ = self
            .gate_energy_max
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |bits| {
                if bits == GATE_ENERGY_NONE || energy > f32::from_bits(bits) {
                    Some(energy.to_bits())
                } else {
                    None
                }
            });
    }

    /// Whether the gate is open, and the largest gate input since the previous take
    /// (`None` when no reading was stored since). Resets the largest input.
    pub fn take_gate(&self) -> (bool, Option<f32>) {
        let open = self.gate_open.load(Ordering::Relaxed);
        let bits = self
            .gate_energy_max
            .swap(GATE_ENERGY_NONE, Ordering::Relaxed);
        (
            open,
            (bits != GATE_ENERGY_NONE).then(|| f32::from_bits(bits)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    extern crate std;
    use std::sync::Arc;
    use std::thread;
    use std::vec::Vec;

    const ALL: CounterSources = CounterSources {
        capture_deficit_frames: true,
        capture_xruns: true,
        playback_write_failures: true,
        playback_discards: true,
        alloc_failures: true,
    };

    #[test]
    fn snapshot_reflects_concurrent_adds() {
        let stats = Arc::new(PodStats::new(ALL));
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let stats = Arc::clone(&stats);
                thread::spawn(move || {
                    for _ in 0..10_000 {
                        PodStats::bump(&stats.playback_underruns);
                        stats.connects.fetch_add(2, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let snap = stats.snapshot();
        assert_eq!(snap.playback_underruns, 40_000);
        assert_eq!(snap.connects, 80_000);
        assert_eq!(snap.capture_xruns, Some(0));
    }

    #[test]
    fn counters_wrap_modularly() {
        let stats = PodStats::new(ALL);
        stats.status_skipped.store(u32::MAX - 1, Ordering::Relaxed);
        PodStats::bump(&stats.status_skipped);
        PodStats::bump(&stats.status_skipped);
        PodStats::bump(&stats.status_skipped);
        let after = stats.snapshot().status_skipped;
        assert_eq!(after, 1);
        assert_eq!(
            after.wrapping_sub(u32::MAX - 1),
            3,
            "a modular difference counts every add"
        );
    }

    #[test]
    fn a_wide_add_keeps_the_modular_difference() {
        let stats = PodStats::new(ALL);
        stats.capture_xruns.store(u32::MAX, Ordering::Relaxed);
        PodStats::add(&stats.capture_xruns, 3);
        assert_eq!(stats.snapshot().capture_xruns, Some(2));
        PodStats::add(&stats.capture_xruns, (1u64 << 32) + 5);
        assert_eq!(
            stats.snapshot().capture_xruns,
            Some(7),
            "an add of 2³² + 5 moves a modular counter by 5"
        );
    }

    #[test]
    fn a_counter_without_a_source_is_none_not_zero() {
        let stats = PodStats::new(CounterSources {
            capture_deficit_frames: true,
            capture_xruns: false,
            playback_write_failures: false,
            playback_discards: true,
            alloc_failures: false,
        });
        PodStats::bump(&stats.capture_xruns);
        let snap = stats.snapshot();
        assert_eq!(snap.capture_deficit_frames, Some(0));
        assert_eq!(
            snap.capture_xruns, None,
            "an add to an unsourced counter is not reported"
        );
        assert_eq!(snap.playback_write_failures, None);
        assert_eq!(snap.playback_discards, Some(0));
        assert_eq!(snap.alloc_failures, None);
    }

    #[test]
    fn gate_max_is_the_largest_since_the_last_take_and_resets() {
        let stats = PodStats::new(ALL);
        assert_eq!(stats.take_gate(), (false, None), "nothing read yet");
        stats.publish_gate(true, Some(0.2));
        stats.publish_gate(true, Some(0.9));
        stats.publish_gate(false, Some(0.4));
        assert_eq!(stats.take_gate(), (false, Some(0.9)));
        assert_eq!(stats.take_gate(), (false, None), "the take reset it");
        stats.publish_gate(true, Some(0.0));
        assert_eq!(
            stats.take_gate(),
            (true, Some(0.0)),
            "a zero reading is a reading"
        );
    }

    #[test]
    fn failed_and_nan_gate_reads_store_nothing() {
        let stats = PodStats::new(ALL);
        stats.publish_gate(true, None);
        stats.publish_gate(true, Some(f32::NAN));
        assert_eq!(stats.take_gate(), (true, None));
        stats.publish_gate(true, Some(f32::NAN));
        stats.publish_gate(true, Some(0.3));
        stats.publish_gate(true, Some(f32::NAN));
        assert_eq!(stats.take_gate(), (true, Some(0.3)));
    }
}
