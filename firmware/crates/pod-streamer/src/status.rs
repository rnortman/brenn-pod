//! The status frame: when one is due, what it carries, and how far the receiver has
//! been sent the level history.
//!
//! A pod sends [`StreamFrame::Status`](audio_pipeline::wire::StreamFrame::Status) about
//! once every [`STATUS_PERIOD_US`] for as long as it holds a connection, inside a
//! segment or not. [`StatusSchedule`] is the per-connection cursor both send sites
//! share — the idle loop ([`crate::run`]) and the segment loop ([`crate::segment`]) —
//! [`build_status`] is the one place a frame is assembled, and
//! [`begin_status_frame`] the one place either loop starts sending one.

use std::io;
use std::ops::Deref;
use std::sync::Mutex;
use std::time::Instant;

use audio_pipeline::ring::{CaptureRing, RingIndex};
use audio_pipeline::stats::PodStats;
use audio_pipeline::stream_send::FrameWriteState;
use audio_pipeline::wire::{PodGauges, STATUS_PERIOD_US, Status, StreamFrame};

use crate::idle::onset_preroll_cursor;
use crate::segment::frame_ts_us;

/// The status frame's readings only the platform can take. `None` is "not available
/// on this pod or not readable now", never zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PlatformGauges {
    /// Free heap bytes.
    pub heap_free: Option<u32>,
    /// Lowest free heap bytes since boot.
    pub heap_min: Option<u32>,
    /// Largest free heap block in bytes.
    pub heap_largest: Option<u32>,
    /// Wi-Fi RSSI in dBm.
    pub rssi_dbm: Option<i8>,
    /// The least stack, in bytes, the calling thread has had free since it started.
    /// Must be read on the thread that builds and sends the frame.
    pub stack_free_min: Option<u32>,
}

/// Where a status frame's counters and gauges come from: the gate's state from
/// `stats`, the rest of the gauges from the platform.
#[derive(Clone, Copy)]
pub struct StatusSource<'a> {
    /// The device's counters and the gate's published state.
    pub stats: &'a PodStats,
    /// The platform's point-in-time readings, taken once per frame built.
    pub gauges: &'a dyn Fn() -> PlatformGauges,
}

/// One connection's status cadence and level-history cursor.
///
/// `next_block` is the first level block the receiver has not been sent whole; it
/// advances only when a frame is wholly written, so a frame that is built and then
/// dropped leaves its blocks for the next one. A fresh connection starts at block 0,
/// which [`LevelHistory::run_from`](audio_pipeline::level::LevelHistory::run_from)
/// clamps to the oldest block held: the first frame carries the whole history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StatusSchedule {
    next_block: u64,
    due_us: u64,
}

impl StatusSchedule {
    /// The cursor for a fresh connection: the whole history unsent, a frame due now.
    pub const fn new() -> Self {
        Self {
            next_block: 0,
            due_us: 0,
        }
    }

    /// Back to [`new`](Self::new)'s state, for a connection just established.
    pub fn reset(&mut self) {
        *self = Self::new();
    }

    /// Whether a frame is owed at `now_us`.
    pub fn is_due(&self, now_us: u64) -> bool {
        now_us >= self.due_us
    }

    /// A frame came due at `now_us`: the next comes due one period later, so a loop
    /// that stalled owes one frame on waking, not a burst.
    pub fn note_due(&mut self, now_us: u64) {
        self.due_us = now_us.saturating_add(STATUS_PERIOD_US);
    }

    /// A frame whose level run ended before block `end_block` was wholly written.
    pub fn note_sent(&mut self, end_block: u64) {
        self.next_block = self.next_block.max(end_block);
    }

    /// The first level block the next frame should carry.
    pub fn next_block(&self) -> u64 {
        self.next_block
    }
}

/// One past the last level block `status` carries: what
/// [`StatusSchedule::note_sent`] takes once the frame is written.
pub fn status_end_block(status: &Status) -> u64 {
    status.levels.first_block + status.levels.blocks.len() as u64
}

/// Assemble a status frame at `now_us` from the ring, the counters and the gauges,
/// carrying every finished level block from `schedule.next_block()` on.
///
/// Takes the ring lock for one short copy. Takes the gate's largest input since the
/// previous frame, resetting it. `segment_reach` is the pre-roll an onset at the
/// frame's head would carry, read from [`onset_preroll_cursor`], the function the
/// idle loop's onset places its cursor with.
pub fn build_status<B: Deref<Target = [i16]>>(
    ring: &Mutex<Option<CaptureRing<B>>>,
    ridx: &RingIndex,
    source: &StatusSource<'_>,
    schedule: &StatusSchedule,
    now_us: u64,
) -> Status {
    let (capture_head, capture_head_ts_us, levels) = {
        let guard = ring
            .lock()
            .unwrap_or_else(|_| panic!("capture ring mutex poisoned in streamer"));
        let ring = guard.as_ref().expect("capture ring not initialized");
        let head = ring.write_head;
        let head_ts = frame_ts_us(
            ring.anchor_sample,
            ring.anchor_ts_us,
            head.saturating_sub(1),
        );
        (head, head_ts, ring.levels.run_from(schedule.next_block()))
    };
    let reach = capture_head - onset_preroll_cursor(ridx, capture_head);
    let segment_reach = u32::try_from(reach).unwrap_or(u32::MAX);
    let (gate_open, gate_energy) = source.stats.take_gate();
    let PlatformGauges {
        heap_free,
        heap_min,
        heap_largest,
        rssi_dbm,
        stack_free_min,
    } = (source.gauges)();
    Status {
        device_ts_us: now_us,
        capture_head,
        capture_head_ts_us,
        levels,
        counters: source.stats.snapshot(),
        gauges: PodGauges {
            heap_free,
            heap_min,
            heap_largest,
            rssi_dbm,
            gate_open,
            gate_energy,
            stack_free_min,
        },
        segment_reach,
    }
}

/// What [`begin_status_frame`] reads, bundled so the function takes few argument
/// words.
pub struct StatusBuild<'a, B> {
    /// The capture ring the frame's position and level run come from.
    pub ring: &'a Mutex<Option<CaptureRing<B>>>,
    /// Ring index geometry, for `segment_reach`.
    pub ridx: &'a RingIndex,
    /// The counters and gauges.
    pub source: StatusSource<'a>,
    /// The connection's level cursor: the frame carries blocks from its `next_block`.
    pub schedule: &'a StatusSchedule,
    /// Platform monotonic µs, the frame's `device_ts_us`.
    pub now_us: u64,
    /// The clock the write's budget and ceiling are armed on.
    pub now_instant: &'a dyn Fn() -> Instant,
}

/// A status frame encoded into the caller's buffer and ready to write.
pub struct BegunStatus {
    /// The frame's write cursor over the buffer.
    pub write: FrameWriteState,
    /// One past the last level block the frame carries: what
    /// [`StatusSchedule::note_sent`] takes once it is written whole.
    pub end_block: u64,
}

/// Build a status frame now and encode it into `buf`, ready to write.
///
/// Nothing returned contains a [`Status`]: the frame (about 770 bytes with a full
/// level run) lives only in this function's stack frame, which exists only while a
/// frame is being built and encoded. Never inlined, so that frame is never reserved
/// whole in a send loop's own frame — where a caller that sends no status (the
/// real-time-duplex self-test) still pays for it, and where on the streamer thread it
/// would sit under every segment. A `Status` held by value in the segment loop's frame
/// took 2.1 to 2.3 KB off the self-test thread's free stack, measured on the device.
///
/// Takes two argument words plus the return slot: the Xtensa realign guard
/// (`TODO(xtensa-realign-stack-args)`) fails the flash on a realigned function with more
/// than six.
#[inline(never)]
pub fn begin_status_frame<B: Deref<Target = [i16]>>(
    build: &StatusBuild<'_, B>,
    buf: &mut [u8],
) -> io::Result<BegunStatus> {
    let status = build_status(
        build.ring,
        build.ridx,
        &build.source,
        build.schedule,
        build.now_us,
    );
    let end_block = status_end_block(&status);
    let write = FrameWriteState::begin(&StreamFrame::Status(status), buf, build.now_instant)?;
    Ok(BegunStatus { write, end_block })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::idle::{SegmentOnset, plan_segment_start};
    use audio_pipeline::ring::{PREROLL_SAMPLES, RING_CAPACITY_SAMPLES};
    use audio_pipeline::stats::CounterSources;
    use audio_pipeline::wire::LEVEL_BLOCK_SAMPLES;

    const SOURCES: CounterSources = CounterSources {
        capture_deficit_frames: true,
        capture_xruns: false,
        playback_write_failures: true,
        playback_discards: true,
        alloc_failures: true,
    };

    const RIDX: RingIndex = RingIndex::new(RING_CAPACITY_SAMPLES);

    fn metered_ring(samples: u64) -> Mutex<Option<CaptureRing<Vec<i16>>>> {
        let ridx = RIDX;
        let mut ring = CaptureRing {
            samples: vec![0i16; RING_CAPACITY_SAMPLES],
            aux: None,
            write_head: 0,
            anchor_sample: 0,
            anchor_ts_us: 5_000_000,
            levels: Default::default(),
        };
        for i in 0..samples {
            ring.commit(&ridx, 1_000, None, i < LEVEL_BLOCK_SAMPLES as u64);
        }
        Mutex::new(Some(ring))
    }

    fn platform_gauges() -> PlatformGauges {
        PlatformGauges {
            heap_free: Some(1),
            heap_min: Some(2),
            heap_largest: Some(3),
            rssi_dbm: Some(-60),
            stack_free_min: Some(9_216),
        }
    }

    #[test]
    fn a_fresh_schedule_is_due_and_carries_the_whole_history() {
        let ring = metered_ring(5 * LEVEL_BLOCK_SAMPLES as u64 + 7);
        let stats = PodStats::new(SOURCES);
        stats.publish_gate(false, Some(0.25));
        PodStats::bump(&stats.connects);
        let source = StatusSource {
            stats: &stats,
            gauges: &platform_gauges,
        };
        let schedule = StatusSchedule::new();
        assert!(schedule.is_due(0));
        let status = build_status(&ring, &RIDX, &source, &schedule, 7_000_000);

        assert_eq!(status.device_ts_us, 7_000_000);
        assert_eq!(status.capture_head, 5 * LEVEL_BLOCK_SAMPLES as u64 + 7);
        assert_eq!(
            status.capture_head_ts_us,
            5_000_000 + (status.capture_head - 1) * 1_000_000 / 16_000,
            "the head's timestamp is the last captured sample's, from the anchor"
        );
        assert_eq!(status.levels.first_block, 0);
        assert_eq!(status.levels.blocks.len(), 5, "the open block is not sent");
        assert!(status.levels.blocks[0].playing());
        assert!(!status.levels.blocks[1].playing());
        assert_eq!(status_end_block(&status), 5);
        assert_eq!(status.counters.connects, 1);
        assert_eq!(status.counters.capture_xruns, None);
        assert_eq!(
            status.gauges,
            PodGauges {
                heap_free: Some(1),
                heap_min: Some(2),
                heap_largest: Some(3),
                rssi_dbm: Some(-60),
                gate_open: false,
                gate_energy: Some(0.25),
                stack_free_min: Some(9_216),
            },
            "the platform's readings beside the gate's"
        );
        assert_eq!(stats.take_gate(), (false, None), "the build took the max");
    }

    #[test]
    fn segment_reach_is_the_preroll_an_onset_at_the_head_would_carry() {
        let stats = PodStats::new(SOURCES);
        let source = StatusSource {
            stats: &stats,
            gauges: &platform_gauges,
        };
        let schedule = StatusSchedule::new();
        let reach = |samples: u64| {
            build_status(&metered_ring(samples), &RIDX, &source, &schedule, 0).segment_reach
        };
        assert_eq!(
            reach(0),
            0,
            "an empty ring: a segment would begin at the head"
        );
        assert_eq!(reach(4_321), 4_321, "a young ring: the held length");
        assert_eq!(reach(PREROLL_SAMPLES), PREROLL_SAMPLES as u32);
        assert_eq!(
            reach(PREROLL_SAMPLES * 3 + 17),
            PREROLL_SAMPLES as u32,
            "a ring holding more than the pre-roll: the pre-roll"
        );
    }

    /// The reach a frame reports is the pre-roll the segment an onset at its head
    /// opens with, on a ring younger than the pre-roll, one exactly as old, and one
    /// that has wrapped.
    #[test]
    fn segment_reach_equals_the_preroll_of_a_segment_opened_at_the_head() {
        let stats = PodStats::new(SOURCES);
        let source = StatusSource {
            stats: &stats,
            gauges: &platform_gauges,
        };
        let schedule = StatusSchedule::new();
        for samples in [0, 4_321, PREROLL_SAMPLES, RING_CAPACITY_SAMPLES as u64 + 17] {
            let ring = metered_ring(samples);
            let reach = build_status(&ring, &RIDX, &source, &schedule, 0).segment_reach;
            let guard = ring.lock().unwrap();
            let plan = plan_segment_start(
                guard.as_ref().unwrap(),
                &RIDX,
                &SegmentOnset {
                    segment_id: 0,
                    vad_write_head: samples,
                },
            );
            assert_eq!(
                reach, plan.start.preroll_samples,
                "{samples} samples captured"
            );
        }
    }

    #[test]
    fn the_cursor_moves_only_on_a_sent_frame() {
        let ring = metered_ring(3 * LEVEL_BLOCK_SAMPLES as u64);
        let stats = PodStats::new(SOURCES);
        let source = StatusSource {
            stats: &stats,
            gauges: &platform_gauges,
        };
        let mut schedule = StatusSchedule::new();
        let first = build_status(&ring, &RIDX, &source, &schedule, 0);
        schedule.note_due(0);
        // Dropped: nothing noted sent, so the next frame carries the same blocks.
        let again = build_status(&ring, &RIDX, &source, &schedule, 1_000_000);
        assert_eq!(again.levels, first.levels);
        schedule.note_sent(status_end_block(&again));
        let after = build_status(&ring, &RIDX, &source, &schedule, 2_000_000);
        assert_eq!(after.levels.first_block, 3);
        assert!(after.levels.blocks.is_empty());
        schedule.note_sent(2);
        assert_eq!(schedule.next_block(), 3, "the cursor never moves back");
    }

    #[test]
    fn frames_come_due_a_period_apart_and_a_stall_owes_one_frame() {
        let mut schedule = StatusSchedule::new();
        assert!(schedule.is_due(10));
        schedule.note_due(10);
        assert!(!schedule.is_due(1_000_009));
        assert!(schedule.is_due(1_000_010));
        // Noted 40 ms late: the next is a period after that.
        schedule.note_due(1_040_000);
        assert!(!schedule.is_due(2_039_999));
        assert!(schedule.is_due(2_040_000));
        // A 5 s stall: one frame owed now, the next a period later, not five at once.
        schedule.note_due(7_000_000);
        assert!(!schedule.is_due(7_999_999));
        assert!(schedule.is_due(8_000_000));
    }

    #[test]
    fn begin_status_frame_encodes_the_frame_build_status_makes_and_its_end_block() {
        let ring = metered_ring(4 * LEVEL_BLOCK_SAMPLES as u64 + 3);
        let stats = PodStats::new(SOURCES);
        let source = StatusSource {
            stats: &stats,
            gauges: &platform_gauges,
        };
        let mut schedule = StatusSchedule::new();
        schedule.note_sent(1);
        let expected = build_status(&ring, &RIDX, &source, &schedule, 3_000_000);
        let mut buf = vec![0u8; audio_pipeline::wire::MAX_FRAME_BYTES + 2];
        let begun = begin_status_frame(
            &StatusBuild {
                ring: &ring,
                ridx: &RIDX,
                source,
                schedule: &schedule,
                now_us: 3_000_000,
                now_instant: &Instant::now,
            },
            &mut buf,
        )
        .expect("a status frame encodes");
        assert_eq!(begun.end_block, 4);
        assert_eq!(begun.write.written(), 0, "begun, nothing written yet");
        assert_eq!(
            audio_pipeline::wire::decode_frame(&buf).expect("the buffer holds the frame"),
            StreamFrame::Status(expected)
        );
    }

    #[test]
    fn reset_restores_a_fresh_connection() {
        let mut schedule = StatusSchedule::new();
        schedule.note_due(5_000_000);
        schedule.note_sent(40);
        schedule.reset();
        assert_eq!(schedule, StatusSchedule::new());
        assert!(schedule.is_due(0));
    }
}
