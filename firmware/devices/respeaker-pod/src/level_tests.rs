//! Self-tests of the capture ring's level meter: that each 100 ms block's level is
//! the level of the ring's own samples (`LevelMeterMatchesRing`), and that a block is
//! flagged playing when audio went to the speaker output and not once it stopped, however
//! the stream ended (`LevelBlocksFlagPlayback`, `LevelBlocksFlagStreamReset`).
//!
//! All read the production `CAPTURE_RING` while the capture thread keeps writing it;
//! none quiesces capture. None issues an end-of-audio mark or resets the flag by any
//! other means: the flag tests each make their own feed and need no state from earlier
//! tests, and a flag an earlier test left set fails their before window, which is a true
//! failure of the flag's rule.

use std::time::{Duration, Instant};

use audio_pipeline::level::{block_of, check_run_against, flags_within};
use audio_pipeline::playback::{
    INBOUND_PCM_RING_BYTES, INBOUND_PCM_WRITE_UNIT_BYTES, PlaybackSink as _, WIRE_BYTES_PER_SAMPLE,
};
use audio_pipeline::ring::{RING_CAPACITY_SAMPLES, RingIndex, SAMPLE_RATE_HZ};
use audio_pipeline::wire::{LEVEL_BLOCK_SAMPLES, LevelRun};
use device_protocol::{
    Payload, Status, TestData, test_report_fail, test_report_fail_fmt, test_report_ok,
};
use esp_idf_svc::hal::delay::FreeRtos;

use crate::capture::CAPTURE_RING;
use crate::speaker::{
    I2S_DMA_DESC_NUM, I2S_DMA_FRAME_NUM, INBOUND_PCM_PRODUCER, SPEAKER_PLAYING_HOLD_MS,
    build_inbound_stream_sink, feed_saturating_playback,
};

/// Blocks `LevelMeterMatchesRing` waits for and recomputes: one second of room.
const METER_CHECK_BLOCKS: u64 = 10;

/// How long `LevelMeterMatchesRing` waits for its blocks before failing: the ten
/// blocks take one second of live capture.
const METER_CHECK_WAIT: Duration = Duration::from_secs(3);

/// One second of samples, the width of the before and after windows.
const ONE_SECOND_SAMPLES: u64 = SAMPLE_RATE_HZ as u64;

/// How long each flag test feeds the saturating playback chunk.
const FLAG_FEED_MS: u64 = 2_000;

/// How long the playback path may take to fall idle before the test begins: an earlier
/// test may have left the inbound ring full with no end-of-audio mark, and a full ring
/// takes about two seconds to play out.
const FLAG_IDLE_DRAIN_WAIT: Duration = Duration::from_secs(3);

/// How long the fed backlog may take to drain after the feed stops: the 64 KiB inbound
/// ring holds about two seconds at the real-time rate, with margin for a slower drain.
const FLAG_FEED_DRAIN_WAIT: Duration = Duration::from_secs(3);

/// How long a flag test waits for the ring to hold a second of capture before the
/// feed: the capture thread discards 1.5 s of warmup after boot.
const FLAG_CAPTURE_WAIT: Duration = Duration::from_secs(4);

/// Wait from seeing the inbound ring empty to taking the feed's start head, so that the
/// second before that head was committed with the flag clear. Seeing the ring empty is
/// not the last hand-over: the final run is still staged, and a full TX DMA takes it over
/// the next write unit's worth of audio, each acceptance restarting the hold. The flag
/// then holds for [`SPEAKER_PLAYING_HOLD_MS`]. A sample's position is committed up to
/// the RX DMA ring's depth after it was captured, so the head's position can trail the
/// wall clock by up to that depth. The wait is the before window plus those three, plus
/// one level block of margin.
const FLAG_SETTLE: Duration = Duration::from_millis(
    ONE_SECOND_SAMPLES * 1_000 / SAMPLE_RATE_HZ as u64
        + SPEAKER_PLAYING_HOLD_MS
        + (INBOUND_PCM_WRITE_UNIT_BYTES / WIRE_BYTES_PER_SAMPLE) as u64 * 1_000
            / SAMPLE_RATE_HZ as u64
        + (I2S_DMA_DESC_NUM as u64 * I2S_DMA_FRAME_NUM as u64 * 1_000) / SAMPLE_RATE_HZ as u64
        + LEVEL_BLOCK_SAMPLES as u64 * 1_000 / SAMPLE_RATE_HZ as u64,
);

/// The ring's write head and level-history position, read under its lock, or `None`
/// if the ring is not initialized.
fn ring_head() -> Option<(u64, u64)> {
    let guard = CAPTURE_RING
        .lock()
        .unwrap_or_else(|_| panic!("CAPTURE_RING mutex poisoned"));
    guard
        .as_ref()
        .map(|r| (r.write_head, r.levels.next_block()))
}

/// Wait until the ring's write head reaches `target`, or `limit` passes. Returns the
/// head reached.
fn await_head(target: u64, limit: Duration) -> Option<u64> {
    let deadline = Instant::now() + limit;
    loop {
        let (head, _) = ring_head()?;
        if head >= target {
            return Some(head);
        }
        if Instant::now() >= deadline {
            return Some(head);
        }
        FreeRtos::delay_ms(10);
    }
}

/// Level-meter identity self-test.
///
/// Notes the level history's next block, waits for ten more to finish, then under the
/// ring lock recomputes each of those ten from the ring's level-channel samples (the
/// raw-microphone `aux` buffer, the channel `commit` meters) and compares. Reports the
/// count and the largest disagreement; the host requires ten blocks and zero steps.
pub(crate) fn run_level_meter_matches_ring() -> (Status, Payload) {
    let Some((_, first_block)) = ring_head() else {
        return test_report_fail("FAIL src=level capture ring not initialized — firmware bug");
    };
    let want_end = first_block + METER_CHECK_BLOCKS;
    let deadline = Instant::now() + METER_CHECK_WAIT;
    loop {
        let Some((_, next)) = ring_head() else {
            return test_report_fail("FAIL src=level capture ring not initialized — firmware bug");
        };
        if next >= want_end {
            break;
        }
        if Instant::now() >= deadline {
            return test_report_fail_fmt(format_args!(
                "FAIL src=level reason=blocks-not-finished first_block={first_block} next={next} \
                 waited_ms={}",
                METER_CHECK_WAIT.as_millis()
            ));
        }
        FreeRtos::delay_ms(20);
    }

    let check = {
        let guard = CAPTURE_RING
            .lock()
            .unwrap_or_else(|_| panic!("CAPTURE_RING mutex poisoned"));
        let Some(ring) = guard.as_ref() else {
            return test_report_fail("FAIL src=level capture ring not initialized — firmware bug");
        };
        let ridx = RingIndex::new(RING_CAPACITY_SAMPLES);
        let held_from = ring.write_head - ridx.held(ring.write_head);
        let mut run: LevelRun = ring.levels.run_from(first_block);
        run.blocks.truncate(METER_CHECK_BLOCKS as usize);
        let level = ring.aux.as_deref().unwrap_or(&ring.samples);
        check_run_against(&run, held_from, |i| level[ridx.slot(i)])
    };
    test_report_ok(TestData::LevelMeter {
        first_block,
        blocks: check.blocks,
        max_diff_steps: check.max_diff_steps,
        quietest_half_db: check.quietest_half_db,
        loudest_half_db: check.loudest_half_db,
    })
}

/// Whether the inbound playback ring holds nothing still to play.
fn inbound_ring_empty() -> bool {
    INBOUND_PCM_PRODUCER
        .lock()
        .unwrap_or_else(|_| panic!("INBOUND_PCM_PRODUCER mutex poisoned"))
        .as_ref()
        .is_some_and(|p| p.free_total() == INBOUND_PCM_RING_BYTES)
}

/// Wait until the inbound ring is empty, or `limit` passes. Returns whether it emptied.
/// Issues no end-of-audio mark: the stream is left to end however it ends.
pub(crate) fn await_inbound_empty(limit: Duration) -> bool {
    let deadline = Instant::now() + limit;
    loop {
        if inbound_ring_empty() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        FreeRtos::delay_ms(10);
    }
}

/// How a flag test ends its feed.
#[derive(Clone, Copy)]
enum FeedEnd {
    /// Wait for the inbound ring to drain, with no end-of-audio mark: a stream whose end
    /// is never marked.
    Drain,
    /// Reset the stream through the production sink with audio still banked, as a
    /// successful reconnect does: the bank is discarded with any mark in it, and nothing
    /// follows.
    StreamReset,
}

/// Level-block playing-flag identity self-test, the stream ending unmarked.
///
/// Needs no state from earlier tests. Waits for the inbound ring to be empty and
/// [`FLAG_SETTLE`], so the second before the feed has nothing going to the speaker. Then
/// feeds the saturating playback chunk for [`FLAG_FEED_MS`] and waits for the ring to
/// drain, with no end-of-audio mark at any point. The feed window runs from the head at
/// the first chunk to the head when the ring is seen empty. Waits for the second that
/// begins one second after that window to finish, and reports, for the blocks wholly
/// inside each of the three windows, the count and how many are flagged. The host owns
/// the verdict.
pub(crate) fn run_level_blocks_flag_playback() -> (Status, Payload) {
    run_flag_test(FeedEnd::Drain)
}

/// Level-block playing-flag identity self-test, the stream reset with audio banked.
///
/// Needs no state from earlier tests. As [`run_level_blocks_flag_playback`] up to the
/// end of the feed; then, with the inbound ring still holding the backlog, calls the
/// production sink's `stream_reset`, the call a successful reconnect makes. Fails if the
/// ring was already empty at the reset. The feed window runs from the head at the first
/// chunk to the head at the reset; the after window is the second that begins one second
/// after the reset.
pub(crate) fn run_level_blocks_flag_stream_reset() -> (Status, Payload) {
    run_flag_test(FeedEnd::StreamReset)
}

/// The two flag tests' shared body; `end` says how the feed ends.
fn run_flag_test(end: FeedEnd) -> (Status, Payload) {
    if !await_inbound_empty(FLAG_IDLE_DRAIN_WAIT) {
        return test_report_fail_fmt(format_args!(
            "FAIL src=level-flags reason=playback-not-idle waited_ms={}",
            FLAG_IDLE_DRAIN_WAIT.as_millis()
        ));
    }
    FreeRtos::delay_ms(FLAG_SETTLE.as_millis() as u32);
    // Straight after boot capture is still in its warmup and the ring holds no second
    // to put before the feed.
    let held = await_head(ONE_SECOND_SAMPLES, FLAG_CAPTURE_WAIT);
    if held.is_none_or(|h| h < ONE_SECOND_SAMPLES) {
        return test_report_fail_fmt(format_args!(
            "FAIL src=level-flags reason=capture-not-running head={held:?} waited_ms={}",
            FLAG_CAPTURE_WAIT.as_millis()
        ));
    }

    let Some((feed_lo, _)) = ring_head() else {
        return test_report_fail(
            "FAIL src=level-flags capture ring not initialized — firmware bug",
        );
    };
    let feed_started = Instant::now();
    let (chunks_fed, _) = feed_saturating_playback(FLAG_FEED_MS);
    match end {
        FeedEnd::Drain => {
            if !await_inbound_empty(FLAG_FEED_DRAIN_WAIT) {
                return test_report_fail_fmt(format_args!(
                    "FAIL src=level-flags reason=feed-not-drained chunks_fed={chunks_fed} \
                     waited_ms={}",
                    FLAG_FEED_DRAIN_WAIT.as_millis()
                ));
            }
        }
        FeedEnd::StreamReset => {
            if inbound_ring_empty() {
                return test_report_fail_fmt(format_args!(
                    "FAIL src=level-flags reason=ring-empty-at-reset chunks_fed={chunks_fed}"
                ));
            }
            build_inbound_stream_sink().stream_reset();
        }
    }
    let feed_ms = feed_started.elapsed().as_millis() as u32;
    let Some((feed_hi, _)) = ring_head() else {
        return test_report_fail(
            "FAIL src=level-flags capture ring not initialized — firmware bug",
        );
    };

    let after_lo = feed_hi + ONE_SECOND_SAMPLES;
    let after_hi = after_lo + ONE_SECOND_SAMPLES;
    // The window's last block finishes when the head passes its end; allow two seconds
    // of capture plus a block's margin.
    let reached = await_head(after_hi + 1, Duration::from_millis(2_500));
    if reached.is_none_or(|h| h <= after_hi) {
        return test_report_fail_fmt(format_args!(
            "FAIL src=level-flags reason=after-window-not-captured head={reached:?} need={after_hi}"
        ));
    }

    let before_lo = feed_lo.saturating_sub(ONE_SECOND_SAMPLES);
    let run = {
        let guard = CAPTURE_RING
            .lock()
            .unwrap_or_else(|_| panic!("CAPTURE_RING mutex poisoned"));
        let Some(ring) = guard.as_ref() else {
            return test_report_fail(
                "FAIL src=level-flags capture ring not initialized — firmware bug",
            );
        };
        ring.levels.run_from(block_of(before_lo))
    };
    let (before_blocks, before_flagged) = flags_within(&run, before_lo, feed_lo);
    let (feed_blocks, feed_flagged) = flags_within(&run, feed_lo, feed_hi);
    let (after_blocks, after_flagged) = flags_within(&run, after_lo, after_hi);
    let how = match end {
        FeedEnd::Drain => "drain",
        FeedEnd::StreamReset => "stream-reset",
    };
    log::info!(
        "level-flags[{how}]: before=[{before_lo},{feed_lo}) feed=[{feed_lo},{feed_hi}) \
         after=[{after_lo},{after_hi}) chunks_fed={chunks_fed}"
    );
    test_report_ok(TestData::LevelPlaybackFlags {
        before_blocks,
        before_flagged,
        feed_blocks,
        feed_flagged,
        after_blocks,
        after_flagged,
        feed_ms,
    })
}
