//! Real-time pacing for the outbound audio catch-up drain.
//!
//! At a VAD onset the streamer opens a segment carrying a pre-roll backlog
//! (`ring::PREROLL_SAMPLES` of buffered history). Draining that backlog at line
//! rate writes the whole pre-roll of audio into the TX pool and TCP send queue in
//! one blast, spiking transient heap. Releasing audio frames on a fixed cadence
//! bounded to [`CATCH_UP_PACE_MULTIPLIER`]× real time keeps only a bounded number
//! of frames in flight at once, so the transient consumption stays bounded.
//!
//! The arithmetic is pure and clock-free (monotonic-microsecond inputs) so it unit
//! tests on the host under `cargo test --workspace`; the streamer supplies the
//! clock read and turns the returned schedule into a poll-wait deadline.

use crate::ring::SAMPLE_RATE_HZ;
use crate::wire::AUDIO_SAMPLES_PER_FRAME;

/// Bound on the outbound audio emit rate during catch-up, as a multiple of real
/// time. At `4`× the pre-roll (50 frames, 1 s of audio) drains in ~250 ms while the
/// steady-state stream (frames produced at 1× real time) is never throttled,
/// because production is slower than the paced cadence.
pub const CATCH_UP_PACE_MULTIPLIER: u64 = 4;

/// One audio frame's real-time span in microseconds
/// (`AUDIO_SAMPLES_PER_FRAME / SAMPLE_RATE_HZ`): 320 / 16000 = 20 ms.
pub const AUDIO_FRAME_PERIOD_US: u64 =
    AUDIO_SAMPLES_PER_FRAME as u64 * 1_000_000 / SAMPLE_RATE_HZ as u64;

/// Paced inter-frame period in microseconds: one frame's real-time span divided by
/// the pace multiplier. 20 ms / 4 = 5 ms — the minimum spacing between consecutive
/// audio-frame emissions while a backlog is being drained.
pub const CATCH_UP_PACED_FRAME_US: u64 = AUDIO_FRAME_PERIOD_US / CATCH_UP_PACE_MULTIPLIER;

/// Maximum number of paced periods the schedule may lag `now` before it resyncs.
///
/// The schedule advances on a fixed cadence, so a wake coarser than one paced
/// period (the FreeRTOS 10 ms tick spans two periods) may release several frames
/// to hold the target average rate. This bounds that per-wake catch-up to
/// `MAX_PACE_LAG_PERIODS + 1` frames: after a write stall parks the schedule far
/// behind real time, it resyncs instead of letting the recovered backlog burst out
/// uncapped. Set above the ordinary two-period tick lag so tick jitter does not
/// resync and drop below the target multiplier.
pub const MAX_PACE_LAG_PERIODS: u64 = 4;

/// Whether an audio frame may be emitted at `now_us` under the pace `schedule`.
///
/// `schedule` is the earliest-emit instant in monotonic microseconds, or `None`
/// before the first frame of a segment (the first frame is never gated).
pub fn pace_allows(schedule: Option<u64>, now_us: u64) -> bool {
    schedule.is_none_or(|earliest| now_us >= earliest)
}

/// Microseconds a frame ready at `now_us` must wait before it may be emitted, or
/// `None` if it may be emitted now.
///
/// This is the gated-caller counterpart to [`pace_allows`]: it returns `Some(wait)`
/// only while the `schedule` blocks emission, so the caller has no reachable
/// "blocked but no schedule" case to defend against — a `None` schedule (the first
/// frame of a segment) and a reached schedule both return `None` (emit now).
pub fn pace_wait_us(schedule: Option<u64>, now_us: u64) -> Option<u64> {
    schedule
        .filter(|&earliest| now_us < earliest)
        .map(|earliest| earliest - now_us)
}

/// Outcome of advancing the pace schedule by one paced period.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaceAdvance {
    /// The earliest-emit instant (monotonic µs) for the next audio frame.
    pub next_schedule_us: u64,
    /// True when the lag floor won — the schedule had parked more than
    /// [`MAX_PACE_LAG_PERIODS`] behind `now_us` (a write stall) and was floored.
    pub resynced: bool,
}

/// Advance the pace schedule by one paced period after emitting a frame at `now_us`.
///
/// Returns the earliest-emit instant (monotonic µs) for the *next* audio frame and
/// whether the advance resynced. The cadence is fixed (advance from the prior
/// schedule, not from `now_us`) so a backlog drains at the paced rate even when the
/// wake granularity is coarser than one period. If the schedule has fallen more than
/// [`MAX_PACE_LAG_PERIODS`] behind `now_us` — a write stall — it is floored to that
/// lag before advancing, capping the per-wake burst so a recovered backlog cannot
/// blast the TX queue; that flooring is reported as `resynced`.
pub fn advance_pace_us(schedule: Option<u64>, now_us: u64) -> PaceAdvance {
    let (base, resynced) = match schedule {
        Some(earliest) => {
            let lag_floor = now_us.saturating_sub(MAX_PACE_LAG_PERIODS * CATCH_UP_PACED_FRAME_US);
            (earliest.max(lag_floor), earliest < lag_floor)
        }
        None => (now_us, false),
    };
    PaceAdvance {
        next_schedule_us: base + CATCH_UP_PACED_FRAME_US,
        resynced,
    }
}

/// Wall-clock microseconds the paced drain of `frames` back-to-back audio frames
/// takes at the target multiplier: `frames * CATCH_UP_PACED_FRAME_US`.
///
/// The catch-up bound used to size host-side keep-up ceilings: a pre-roll of
/// `PREROLL_SAMPLES / AUDIO_SAMPLES_PER_FRAME` frames drains in this long, and the
/// paced cadence never exceeds it.
pub fn paced_drain_us(frames: u64) -> u64 {
    frames * CATCH_UP_PACED_FRAME_US
}

/// Frames due at `now_us` under an absolute schedule started at `start_us` with
/// `interval_us` between frames: `elapsed / interval`, capped at `total_frames`,
/// floored at `committed + 1` so every wake commits at least one frame. The
/// shortfall over `committed` is the catch-up burst that absorbs oversleep drift.
///
/// `interval_us` must be nonzero (an absolute schedule with a zero inter-frame
/// span is undefined; a zero divides by zero).
pub fn absolute_frames_due(
    now_us: u64,
    start_us: u64,
    interval_us: u64,
    committed: u64,
    total_frames: u64,
) -> u64 {
    (now_us.saturating_sub(start_us) / interval_us)
        .min(total_frames)
        .max(committed + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Real RTD-self-test parameters (net_tests.rs): 20 ms interval, 250-frame total.
    const RTD_INTERVAL_US: u64 = 20_000;
    const RTD_TOTAL: u64 = 250;

    #[test]
    fn absolute_no_drift_commits_one_per_wake() {
        let start = 1_000;
        for k in 0..5u64 {
            // Exactly on the (k+1)th deadline with k committed → k+1 due.
            let now = start + (k + 1) * RTD_INTERVAL_US;
            assert_eq!(
                absolute_frames_due(now, start, RTD_INTERVAL_US, k, RTD_TOTAL),
                k + 1,
                "on-time wake commits exactly one frame"
            );
            // At the kth boundary exactly (elapsed/interval == k) the floor lifts to k+1.
            let now_floor = start + k * RTD_INTERVAL_US;
            assert_eq!(
                absolute_frames_due(now_floor, start, RTD_INTERVAL_US, k, RTD_TOTAL),
                k + 1,
                "floor guarantees progress every wake"
            );
        }
    }

    #[test]
    fn absolute_catch_up_bursts_the_shortfall() {
        let start = 1_000;
        // Woke late: 15 intervals elapsed, only 10 committed → 15 due (burst of 5).
        let now = start + 15 * RTD_INTERVAL_US;
        assert_eq!(
            absolute_frames_due(now, start, RTD_INTERVAL_US, 10, RTD_TOTAL),
            15,
            "elapsed drives the catch-up burst"
        );
        // Mid-interval wake (15.5 intervals) still floors to 15 — no fractional frame.
        let now_mid = start + 15 * RTD_INTERVAL_US + RTD_INTERVAL_US / 2;
        assert_eq!(
            absolute_frames_due(now_mid, start, RTD_INTERVAL_US, 10, RTD_TOTAL),
            15,
            "floor division: partial interval commits no extra frame"
        );
    }

    #[test]
    fn absolute_caps_at_total_frames() {
        let start = 1_000;
        // Elapsed far beyond the schedule end → clamped to the total.
        let now = start + 400 * RTD_INTERVAL_US;
        assert_eq!(
            absolute_frames_due(now, start, RTD_INTERVAL_US, 240, RTD_TOTAL),
            RTD_TOTAL,
            "cap prevents overrunning the schedule"
        );
    }

    #[test]
    fn absolute_saturates_when_now_before_start() {
        // now_us < start_us → zero elapsed → floor committed+1.
        assert_eq!(
            absolute_frames_due(500, 1_000, RTD_INTERVAL_US, 7, RTD_TOTAL),
            8,
            "clock read before start saturates to the floor"
        );
    }

    #[test]
    fn absolute_floor_beats_cap_documents_min_max_ordering() {
        // committed == total: the floor (.max, applied last) wins over the cap.
        // Unreachable at the call site (loop guard excludes it); guards the ordering.
        assert_eq!(
            absolute_frames_due(
                1_000 + 500 * RTD_INTERVAL_US,
                1_000,
                RTD_INTERVAL_US,
                RTD_TOTAL,
                RTD_TOTAL
            ),
            RTD_TOTAL + 1,
            "floor applied after cap: committed+1 wins"
        );
    }

    #[test]
    fn constants_derive_to_expected_values() {
        assert_eq!(
            AUDIO_FRAME_PERIOD_US, 20_000,
            "320 samples / 16 kHz = 20 ms"
        );
        assert_eq!(
            CATCH_UP_PACED_FRAME_US, 5_000,
            "20 ms / 4x = 5 ms paced spacing"
        );
    }

    #[test]
    fn first_frame_is_never_gated() {
        assert!(pace_allows(None, 0), "no schedule yet → emit");
        assert!(
            pace_allows(None, 123_456),
            "no schedule yet → emit at any time"
        );
    }

    #[test]
    fn gate_blocks_until_schedule_reached() {
        let sched = Some(10_000);
        assert!(!pace_allows(sched, 9_999), "before schedule → blocked");
        assert!(pace_allows(sched, 10_000), "at schedule → allowed");
        assert!(pace_allows(sched, 10_001), "past schedule → allowed");
    }

    #[test]
    fn wait_us_none_when_allowed_some_when_gated() {
        assert_eq!(
            pace_wait_us(None, 123_456),
            None,
            "first frame ungated → emit now"
        );
        assert_eq!(
            pace_wait_us(Some(10_000), 10_000),
            None,
            "at schedule → emit now"
        );
        assert_eq!(
            pace_wait_us(Some(10_000), 10_001),
            None,
            "past schedule → emit now"
        );
        assert_eq!(
            pace_wait_us(Some(10_000), 7_000),
            Some(3_000),
            "gated → wait the remaining span to the schedule"
        );
    }

    #[test]
    fn first_advance_schedules_one_period_out() {
        // First frame emitted at t=1000 → next allowed at t=1000+5000.
        let adv = advance_pace_us(None, 1_000);
        assert_eq!(adv.next_schedule_us, 6_000);
        assert!(!adv.resynced, "first frame of a segment is never a resync");
    }

    #[test]
    fn steady_cadence_advances_fixed_not_from_now() {
        // Frame emitted exactly on schedule: the next deadline is one period past
        // the *schedule*, not past `now`, so cadence does not drift.
        let sched = 5_000;
        let adv = advance_pace_us(Some(sched), sched);
        assert_eq!(
            adv.next_schedule_us, 10_000,
            "on-time frame → +1 period from schedule"
        );
        assert!(!adv.resynced, "on-time advance does not resync");
    }

    #[test]
    fn advance_at_lag_floor_boundary_does_not_resync() {
        // earliest == lag_floor exactly: the `.max` leaves the schedule unchanged and
        // no resync occurred (the flag is strict `earliest < lag_floor`).
        let now = 1_000_000;
        let lag_floor = now - MAX_PACE_LAG_PERIODS * CATCH_UP_PACED_FRAME_US;
        let adv = advance_pace_us(Some(lag_floor), now);
        assert_eq!(
            adv.next_schedule_us,
            lag_floor + CATCH_UP_PACED_FRAME_US,
            "on the boundary the schedule advances from itself"
        );
        assert!(!adv.resynced, "tie at the floor boundary is not a resync");
    }

    #[test]
    fn fixed_cadence_holds_target_average_rate() {
        // Drive wakes on the FreeRTOS 10 ms tick (coarser than one 5 ms period) and
        // confirm the paced drain holds the 4x-real-time average: one frame per period,
        // i.e. two frames per tick, released across multiple frames per wake as needed.
        let tick_us = 10_000;
        let wakes: u64 = 50;
        let mut sched: Option<u64> = None;
        let mut released: u64 = 0;
        for k in 0..wakes {
            let now = k * tick_us + tick_us; // first wake at t = 10 ms
            while pace_allows(sched, now) {
                sched = Some(advance_pace_us(sched, now).next_schedule_us);
                released += 1;
                assert!(released <= wakes * 4, "must not spin unboundedly");
            }
        }
        let elapsed_us = wakes * tick_us;
        let expected = elapsed_us / CATCH_UP_PACED_FRAME_US;
        assert!(
            released.abs_diff(expected) <= 1,
            "released {released} frames over {elapsed_us} µs; expected ≈ {expected} at 4x real time"
        );
    }

    #[test]
    fn stall_resyncs_and_caps_the_recovered_burst() {
        // Schedule parked far in the past (a long write stall), now recovered.
        // The lag floor caps the catch-up burst to MAX_PACE_LAG_PERIODS + 1 frames.
        let now = 1_000_000;
        let mut sched: Option<u64> = Some(0); // ~200 periods behind
        let mut released = 0;
        let mut resyncs = 0;
        while pace_allows(sched, now) {
            let adv = advance_pace_us(sched, now);
            if adv.resynced {
                resyncs += 1;
            }
            sched = Some(adv.next_schedule_us);
            released += 1;
            assert!(released <= 100, "resync must bound the burst");
        }
        assert_eq!(
            released,
            (MAX_PACE_LAG_PERIODS + 1) as usize,
            "post-stall burst capped, not uncapped line-rate catch-up"
        );
        assert_eq!(
            resyncs, 1,
            "exactly the first advance of the recovered burst resyncs; \
             the rest step from the floored schedule within the lag window"
        );
    }

    #[test]
    fn paced_drain_of_preroll_matches_frame_count_times_period() {
        use crate::ring::PREROLL_SAMPLES;
        // 16 000 / 320 = 50 frames exactly (frame-aligned pre-roll, no truncated tail).
        let preroll_frames = PREROLL_SAMPLES / AUDIO_SAMPLES_PER_FRAME as u64;
        assert_eq!(preroll_frames, 50, "50-frame pre-roll");
        assert_eq!(
            paced_drain_us(preroll_frames),
            250_000,
            "50 frames x 5 ms = 250 ms paced pre-roll drain"
        );
    }
}
