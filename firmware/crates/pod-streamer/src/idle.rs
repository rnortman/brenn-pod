//! Idle-loop policy: what the streamer does between segments.
//!
//! The streamer thread spends most of its life idle — holding a connection open,
//! draining inbound playback, waiting for a VAD onset. The decisions it makes
//! there are identical on every pod, because none of them depend on the
//! transport: whether this tick may attempt a connect, how long a failure backs
//! off, what installing or dropping a socket obliges, where a fresh segment's
//! pre-roll cursor lands. Only the thing being connected and the link-state
//! source differ, and those stay with the platform's loop.
//!
//! Everything here is either a pure function of values the caller already holds
//! or a small mutator over the caller's state, so the platform loop keeps its
//! own control flow and this module keeps the rules.

use std::time::Duration;

use audio_pipeline::inbound::{FrameAccumulator, InboundConnectionState};
use audio_pipeline::playback::PlaybackSink;
use audio_pipeline::ring::{CaptureRing, RingIndex};
use audio_pipeline::wire::SegmentStart;
use wifi_reconnect::Backoff;

use crate::segment::{StreamerMsg, frame_ts_us};

// ── Reconnect pacing ──────────────────────────────────────────────────────────

/// Whether to attempt a connect on this idle tick.
#[derive(Debug, PartialEq, Eq)]
pub enum IdleConnectAction {
    /// Socket is down, link is up, and the backoff deadline has elapsed.
    Attempt,
    /// Already connected, link down, or backoff not yet elapsed.
    Skip,
}

/// Should the idle loop attempt a connect this tick?
///
/// `link_up` is tri-state because a platform may not know: `None` (unknown) is
/// treated as down. Link-down is not an audio-server failure and must not charge
/// a backoff, which is why this gate runs before any attempt is made rather than
/// being folded into the failure path.
///
/// Takes the pre-computed deadline (not the [`Backoff`]) so it never redraws
/// jitter on each tick.
pub fn should_attempt_idle_connect(
    socket_is_some: bool,
    link_up: Option<bool>,
    now_secs: u64,
    reconnect_deadline_secs: u64,
) -> IdleConnectAction {
    if !socket_is_some && link_up == Some(true) && now_secs >= reconnect_deadline_secs {
        IdleConnectAction::Attempt
    } else {
        IdleConnectAction::Skip
    }
}

/// Arm the next reconnect deadline after a failed connect or drain error.
///
/// Draws the jittered wait once and returns the absolute deadline; subsequent
/// idle ticks compare against this fixed value. `attempt_counter` is folded into
/// the jitter seed so consecutive attempts by one pod do not draw the same
/// offset, and the base seed keeps a fleet's pods from converging.
pub fn arm_reconnect_deadline(
    now_secs: u64,
    backoff: &mut Backoff,
    attempt_counter: &mut u32,
    jitter_seed_base: u32,
) -> u64 {
    backoff.record_failure();
    *attempt_counter = attempt_counter.wrapping_add(1);
    now_secs.saturating_add(backoff.next_wait_secs(jitter_seed_base ^ *attempt_counter))
}

/// Reset backoff state after a successful connect.
///
/// Every connect-success path must call this — zeroing the deadline is what makes
/// a later socket loss reconnect immediately rather than waiting out a stale
/// deadline drawn before the connection that just worked.
pub fn note_connect_success(backoff: &mut Backoff, reconnect_deadline_secs: &mut u64) {
    backoff.record_success();
    *reconnect_deadline_secs = 0;
}

// ── Provisioning park ─────────────────────────────────────────────────────────

/// How a bounded provisioning park ended.
#[derive(Debug, PartialEq, Eq)]
pub enum ParkOutcome {
    /// The park interval elapsed; the caller re-checks provisioning.
    TimedOut,
    /// The sender side was dropped; the caller exits the thread.
    Disconnected,
}

/// Drain and discard streamer messages for up to `timeout`.
///
/// While audio provisioning is missing the streamer has nothing to do, so it
/// parks here rather than sleeping: a plain sleep would let the bounded channel
/// fill and wedge the telemetry thread against it. Returns `TimedOut` when the
/// interval elapses, `Disconnected` when the sender side is dropped.
pub fn park_drain(rx: &std::sync::mpsc::Receiver<StreamerMsg>, timeout: Duration) -> ParkOutcome {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        match rx.recv_timeout(remaining) {
            Ok(_) => {
                // A steady message stream always yields `Ok`, so the deadline is
                // checked here rather than relying on the channel going quiet.
                if std::time::Instant::now() >= deadline {
                    return ParkOutcome::TimedOut;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => return ParkOutcome::TimedOut,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return ParkOutcome::Disconnected;
            }
        }
    }
}

/// Whether a provisioning failure should be logged, given the last logged cause.
///
/// The first failure logs, identical repeats stay silent, and any change of cause
/// logs again — including a return to a cause seen before the current one. A pod
/// can park for days, so an unthrottled warn would be the whole log.
pub fn should_log_provisioning_failure(last: Option<&str>, current: &str) -> bool {
    last != Some(current)
}

// ── Socket lifecycle ──────────────────────────────────────────────────────────

/// Route every socket-teardown site through one place: clear the socket, reset
/// the inbound framing state so stale partial bytes cannot corrupt the next
/// connection's first frame, and signal end-of-audio to playback so the banked
/// tail plays out and *then* the sink goes silent.
///
/// A subsequent reconnect calls [`note_socket_established`], whose stream-reset
/// discards any un-played tail and the just-pushed end-of-audio mark — so a drop
/// followed by an immediate reconnect is seamless (no spurious silence), while a
/// drop that stays down plays the banked tail out and then goes quiet.
pub fn note_socket_lost<L>(
    held_socket: &mut Option<L>,
    inbound_accum: &mut FrameAccumulator,
    inbound_state: &mut InboundConnectionState,
    inbound_sink: &mut dyn PlaybackSink,
) {
    *held_socket = None;
    inbound_accum.reset();
    inbound_state.reset();
    inbound_sink.end_of_audio();
}

/// Connection-established mirror of [`note_socket_lost`]: reset per-connection
/// inbound state, install the socket, and emit the stream boundary — in that
/// order. A fresh socket is a fresh inbound stream, so nothing decoded from the
/// previous one may survive into it.
pub fn note_socket_established<L>(
    held_socket: &mut Option<L>,
    stream: L,
    inbound_accum: &mut FrameAccumulator,
    inbound_state: &mut InboundConnectionState,
    inbound_sink: &mut dyn PlaybackSink,
) {
    inbound_accum.reset();
    inbound_state.reset();
    *held_socket = Some(stream);
    inbound_sink.stream_reset();
}

// ── Segment opening ───────────────────────────────────────────────────────────

/// The onset facts a segment's opening frame is computed from.
///
/// Bundled so [`plan_segment_start`] stays inside the Xtensa
/// realign-miscompile guard's argument-word budget.
pub struct SegmentOnset {
    /// Id for the new segment.
    pub segment_id: u32,
    /// Ring write head at the moment the VAD gate opened, as reported by the
    /// telemetry thread.
    pub vad_write_head: u64,
    /// History to include ahead of the onset, in samples.
    pub preroll_samples: u64,
}

/// A segment's opening frame plus the ring cursor its drain starts from.
pub struct SegmentPlan {
    /// Absolute sample index the segment's first frame is read from.
    pub cursor: u64,
    /// The frame to send before entering [`crate::segment::run_segment`].
    pub start: SegmentStart,
}

/// Place a new segment's pre-roll cursor and date it.
///
/// The cursor is the onset write head backed off by the pre-roll, clamped to the
/// oldest sample the ring still holds, so a segment opened before the ring has
/// filled carries whatever history exists. `preroll_samples` on the wire is the
/// history actually available, not the history requested.
///
/// `base_device_ts_us` extrapolates back from the ring's clock anchor at the
/// shared sample rate — the same rule the engine dates every frame by. An anchor
/// *older* than the cursor cannot be extrapolated forward: forward extrapolation
/// would claim a timestamp for samples the capture thread had not yet stamped, so
/// the anchor is reported as-is. That branch requires the anchor to predate the
/// onset's own write head, which a live capture thread does not produce.
///
/// The caller holds the ring lock; nothing here touches the sample storage, so
/// the lock is held only for the field reads.
pub fn plan_segment_start<B>(
    ring: &CaptureRing<B>,
    ridx: &RingIndex,
    onset: &SegmentOnset,
) -> SegmentPlan {
    let cursor = ridx.preroll_cursor(onset.vad_write_head, onset.preroll_samples);
    let base_device_ts_us = if ring.anchor_sample >= cursor {
        frame_ts_us(ring.anchor_sample, ring.anchor_ts_us, cursor)
    } else {
        ring.anchor_ts_us
    };
    SegmentPlan {
        cursor,
        start: SegmentStart {
            segment_id: onset.segment_id,
            base_sample_index: cursor,
            base_device_ts_us,
            preroll_samples: onset.vad_write_head.saturating_sub(cursor) as u32,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    use audio_pipeline::ring::RING_CAPACITY_SAMPLES;

    use crate::test_support::{RecordingSink, SinkMark, dirty_inbound};

    // ── Provisioning park ─────────────────────────────────────────────────

    /// An idle channel parks for the full interval, then reports the timeout.
    #[test]
    fn park_drain_times_out_when_idle() {
        let (_tx, rx) = std::sync::mpsc::sync_channel::<StreamerMsg>(4);
        let timeout = Duration::from_millis(120);
        let start = Instant::now();
        assert_eq!(park_drain(&rx, timeout), ParkOutcome::TimedOut);
        assert!(
            start.elapsed() >= timeout,
            "park_drain returned early: {:?}",
            start.elapsed()
        );
    }

    /// Messages arriving mid-park are drained — not merely slept through — and
    /// do not cut the park short. The channel holds one message, so a sender
    /// pushing eight only completes if the park actually consumes them.
    #[test]
    fn park_drain_discards_messages_and_still_waits() {
        let (tx, rx) = std::sync::mpsc::sync_channel::<StreamerMsg>(1);
        let timeout = Duration::from_millis(200);
        let sent = Arc::new(AtomicUsize::new(0));
        let sent_tx = Arc::clone(&sent);
        std::thread::spawn(move || {
            for _ in 0..8 {
                std::thread::sleep(Duration::from_millis(10));
                if tx.send(StreamerMsg::VadClosed).is_err() {
                    return;
                }
                sent_tx.fetch_add(1, Ordering::SeqCst);
            }
            // Hold the sender so disconnect does not race the deadline.
            std::thread::sleep(Duration::from_millis(400));
        });
        let start = Instant::now();
        assert_eq!(park_drain(&rx, timeout), ParkOutcome::TimedOut);
        assert!(
            start.elapsed() >= timeout,
            "messages shortened the park: {:?}",
            start.elapsed()
        );
        assert_eq!(
            sent.load(Ordering::SeqCst),
            8,
            "sender blocked: park did not drain the channel"
        );
    }

    /// Dropping the sender ends the park promptly with `Disconnected`, well
    /// before the park deadline.
    #[test]
    fn park_drain_reports_disconnect() {
        let (tx, rx) = std::sync::mpsc::sync_channel::<StreamerMsg>(4);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            drop(tx);
        });
        let start = Instant::now();
        assert_eq!(
            park_drain(&rx, Duration::from_secs(10)),
            ParkOutcome::Disconnected
        );
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "disconnect was not reported promptly: {:?}",
            start.elapsed()
        );
    }

    /// A sustained message flood still lets the park reach its deadline.
    #[test]
    fn park_drain_times_out_under_message_flood() {
        let (tx, rx) = std::sync::mpsc::sync_channel::<StreamerMsg>(4);
        std::thread::spawn(move || while tx.send(StreamerMsg::VadClosed).is_ok() {});
        let timeout = Duration::from_millis(150);
        let start = Instant::now();
        assert_eq!(park_drain(&rx, timeout), ParkOutcome::TimedOut);
        assert!(
            start.elapsed() >= timeout,
            "park ended early: {:?}",
            start.elapsed()
        );
    }

    // ── Provisioning failure log throttle ─────────────────────────────────

    #[test]
    fn provisioning_failure_logs_first_and_on_cause_change() {
        // First failure always logs.
        assert!(should_log_provisioning_failure(
            None,
            "audio_port not provisioned"
        ));
        // An identical repeat stays silent.
        assert!(!should_log_provisioning_failure(
            Some("audio_port not provisioned"),
            "audio_port not provisioned"
        ));
        // A changed cause logs again.
        assert!(should_log_provisioning_failure(
            Some("audio_port not provisioned"),
            "cannot open NVS — no such namespace"
        ));
        // Returning to an earlier cause logs again — this is last-cause
        // comparison, not a seen-set.
        assert!(should_log_provisioning_failure(
            Some("cannot open NVS — no such namespace"),
            "audio_port not provisioned"
        ));
    }

    // ── Idle reconnect pacing ─────────────────────────────────────────────

    use wifi_reconnect::{BACKOFF_CAP_SECS, BACKOFF_FLOOR_SECS};

    /// An established socket always skips reconnect regardless of link state.
    #[test]
    fn idle_gate_skips_when_socket_present() {
        for link in [Some(true), Some(false), None] {
            assert_eq!(
                should_attempt_idle_connect(true, link, 1_000, 0),
                IdleConnectAction::Skip,
                "socket Some must always skip (link={link:?})"
            );
        }
    }

    /// Link down/unknown → skip, regardless of deadline (no point trying without radio).
    #[test]
    fn idle_gate_skips_when_link_down_or_unknown() {
        for link in [Some(false), None] {
            assert_eq!(
                should_attempt_idle_connect(false, link, 1_000, 0),
                IdleConnectAction::Skip,
                "link down/unknown must skip even with a past deadline (link={link:?})"
            );
        }
    }

    /// Socket down + link up, but backoff not elapsed → skip.
    #[test]
    fn idle_gate_skips_before_deadline() {
        assert_eq!(
            should_attempt_idle_connect(false, Some(true), 99, 100),
            IdleConnectAction::Skip,
            "now < deadline must skip"
        );
    }

    /// Socket down + link up + deadline elapsed → attempt.
    #[test]
    fn idle_gate_attempts_when_due() {
        assert_eq!(
            should_attempt_idle_connect(false, Some(true), 100, 100),
            IdleConnectAction::Attempt,
            "now == deadline must attempt"
        );
        assert_eq!(
            should_attempt_idle_connect(false, Some(true), 101, 100),
            IdleConnectAction::Attempt,
            "now > deadline must attempt"
        );
        // Zeroed deadline (fresh boot / post-success) → immediate attempt.
        assert_eq!(
            should_attempt_idle_connect(false, Some(true), 0, 0),
            IdleConnectAction::Attempt,
            "zero deadline must attempt immediately"
        );
    }

    /// Arming advances the attempt counter and returns a deadline in the ±25% jitter band.
    #[test]
    fn arm_deadline_advances_counter_and_lands_in_jitter_band() {
        let mut backoff = Backoff::new();
        let mut attempt_counter: u32 = 0;
        let now: u64 = 1_000;

        let deadline = arm_reconnect_deadline(now, &mut backoff, &mut attempt_counter, 0xABCD);

        assert_eq!(attempt_counter, 1);
        // After one record_failure, backoff doubled from floor (2 → 4); jitter band ±25%.
        let base = BACKOFF_FLOOR_SECS * 2;
        let low = now + (base * 75 / 100).max(1);
        let high = now + base * 125 / 100;
        assert!(
            deadline >= low && deadline <= high,
            "deadline {deadline} not in jitter band [{low}, {high}]"
        );
    }

    /// The stored deadline is evaluated by value — repeated gate polls never re-jitter.
    #[test]
    fn stored_deadline_is_stable_across_repeated_gate_polls() {
        let mut backoff = Backoff::new();
        let mut attempt_counter: u32 = 0;
        let now: u64 = 1_000;

        let deadline = arm_reconnect_deadline(now, &mut backoff, &mut attempt_counter, 0x1234);

        let just_before = deadline - 1;
        for _ in 0..1_000 {
            assert_eq!(
                should_attempt_idle_connect(false, Some(true), just_before, deadline),
                IdleConnectAction::Skip,
                "gate must read the fixed stored deadline, never re-jitter"
            );
        }
        assert_eq!(attempt_counter, 1, "polling must not advance the counter");

        // Fires exactly at the armed deadline (composed arm→wait→fire boundary).
        assert_eq!(
            should_attempt_idle_connect(false, Some(true), deadline, deadline),
            IdleConnectAction::Attempt,
        );
    }

    /// After a successful connect (which zeroes the deadline), a mid-segment socket
    /// drop reconnects immediately — the zeroed deadline is already in the past.
    #[test]
    fn mid_segment_drop_reconnects_immediately_after_success_clear() {
        let mut backoff = Backoff::new();
        let mut attempt_counter: u32 = 0;

        let armed = arm_reconnect_deadline(1_000, &mut backoff, &mut attempt_counter, 0x55);
        assert!(armed > 1_000);

        let mut reconnect_deadline_secs = armed;
        note_connect_success(&mut backoff, &mut reconnect_deadline_secs);
        assert_eq!(
            reconnect_deadline_secs, 0,
            "note_connect_success must zero the deadline"
        );
        assert_eq!(
            backoff.current_secs(),
            BACKOFF_FLOOR_SECS,
            "note_connect_success must reset backoff to the floor"
        );

        assert_eq!(
            should_attempt_idle_connect(false, Some(true), 0, reconnect_deadline_secs),
            IdleConnectAction::Attempt,
            "post-success-clear mid-segment drop must reconnect on the next idle tick"
        );
    }

    /// Repeated failures climb the backoff; `record_success` resets to floor so
    /// the next re-arm draws from the floor band again.
    #[test]
    fn record_success_resets_rearm_to_floor_band() {
        let mut backoff = Backoff::new();
        let mut attempt_counter: u32 = 0;
        let now: u64 = 500;

        let mut last_base = BACKOFF_FLOOR_SECS;
        for _ in 0..5 {
            let _ = arm_reconnect_deadline(now, &mut backoff, &mut attempt_counter, 7);
            assert!(
                backoff.current_secs() >= last_base,
                "backoff must not shrink across failures"
            );
            last_base = backoff.current_secs();
        }
        assert!(
            backoff.current_secs() > BACKOFF_FLOOR_SECS,
            "backoff must have climbed above the floor after repeated failures"
        );
        assert!(
            backoff.current_secs() <= BACKOFF_CAP_SECS,
            "backoff must stay at or below the cap"
        );

        backoff.record_success();
        assert_eq!(
            backoff.current_secs(),
            BACKOFF_FLOOR_SECS,
            "record_success must reset backoff to the floor"
        );

        // Re-arm must draw from the floor band again.
        let deadline = arm_reconnect_deadline(now, &mut backoff, &mut attempt_counter, 7);
        let base = BACKOFF_FLOOR_SECS * 2;
        let low = now + (base * 75 / 100).max(1);
        let high = now + base * 125 / 100;
        assert!(
            deadline >= low && deadline <= high,
            "post-reset deadline {deadline} not in floor band [{low}, {high}]"
        );
    }

    // ── Socket lifecycle ──────────────────────────────────────────────────

    /// Teardown clears the socket, drops the stale framing state, and lets the
    /// banked playback tail drain before going quiet.
    #[test]
    fn socket_loss_clears_the_socket_resets_framing_and_ends_audio() {
        let (mut accum, mut state) = dirty_inbound();
        let mut sink = RecordingSink::new();
        let mut held = Some(7u32);

        note_socket_lost(&mut held, &mut accum, &mut state, &mut sink);

        assert!(held.is_none(), "the dead socket must be released");
        assert_eq!(
            accum.valid_len(),
            0,
            "a stale partial frame would corrupt the next connection's first frame"
        );
        assert!(!state.seen_hello(), "the handshake must not carry over");
        assert_eq!(
            sink.marks,
            vec![SinkMark::EndOfAudio],
            "teardown signals end-of-audio and nothing else"
        );
    }

    /// A fresh socket resets the framing state *before* it is installed, then
    /// marks the stream boundary — so no decode can straddle the two connections.
    #[test]
    fn socket_established_resets_framing_then_marks_the_boundary() {
        let (mut accum, mut state) = dirty_inbound();
        let mut sink = RecordingSink::new();
        let mut held: Option<u32> = None;

        note_socket_established(&mut held, 9u32, &mut accum, &mut state, &mut sink);

        assert_eq!(held, Some(9), "the new socket must be installed");
        assert_eq!(accum.valid_len(), 0, "framing state must start clean");
        assert!(!state.seen_hello(), "the new connection re-handshakes");
        assert_eq!(
            sink.marks,
            vec![SinkMark::StreamReset],
            "establishment marks the boundary and does not signal end-of-audio"
        );
    }

    /// The drop-then-reconnect pair leaves the sink with the boundary last, which
    /// is what discards the tail the teardown had just banked.
    #[test]
    fn reconnect_after_a_drop_ends_with_the_stream_boundary() {
        let mut accum = FrameAccumulator::new();
        let mut state = InboundConnectionState::new();
        let mut sink = RecordingSink::new();
        let mut held = Some(1u32);

        note_socket_lost(&mut held, &mut accum, &mut state, &mut sink);
        note_socket_established(&mut held, 2u32, &mut accum, &mut state, &mut sink);

        assert_eq!(
            sink.marks,
            vec![SinkMark::EndOfAudio, SinkMark::StreamReset],
            "the boundary must land after the end-of-audio mark it supersedes"
        );
    }

    // ── Segment opening ───────────────────────────────────────────────────

    /// A ring whose clock anchor sits at `anchor_sample`/`anchor_ts_us`. The
    /// sample storage is never read here, so it stays empty.
    fn anchored_ring(
        write_head: u64,
        anchor_sample: u64,
        anchor_ts_us: u64,
    ) -> CaptureRing<Vec<i16>> {
        CaptureRing {
            samples: Vec::new(),
            write_head,
            anchor_sample,
            anchor_ts_us,
        }
    }

    /// The steady-state case: a full pre-roll of history is available, so the
    /// cursor sits exactly `preroll_samples` behind the onset and the wire
    /// reports the full pre-roll.
    #[test]
    fn segment_plan_backs_the_cursor_off_by_a_full_preroll() {
        let ridx = RingIndex::new(RING_CAPACITY_SAMPLES);
        let ring = anchored_ring(40_000, 40_000, 5_000_000);
        let onset = SegmentOnset {
            segment_id: 3,
            vad_write_head: 40_000,
            preroll_samples: 16_000,
        };

        let plan = plan_segment_start(&ring, &ridx, &onset);

        assert_eq!(plan.cursor, 24_000);
        assert_eq!(plan.start.segment_id, 3);
        assert_eq!(plan.start.base_sample_index, 24_000);
        assert_eq!(plan.start.preroll_samples, 16_000);
        // 16 000 samples at 16 kHz = 1 s of history before the anchor.
        assert_eq!(plan.start.base_device_ts_us, 4_000_000);
    }

    /// Dating the opening frame uses the same rule the engine dates every
    /// subsequent frame by, so the segment's clock is continuous across it.
    #[test]
    fn segment_plan_dates_the_cursor_by_the_engines_frame_clock() {
        let ridx = RingIndex::new(RING_CAPACITY_SAMPLES);
        let ring = anchored_ring(31_400, 31_360, 1_960_000);
        let onset = SegmentOnset {
            segment_id: 0,
            vad_write_head: 31_400,
            preroll_samples: 16_000,
        };

        let plan = plan_segment_start(&ring, &ridx, &onset);

        assert_eq!(
            plan.start.base_device_ts_us,
            frame_ts_us(ring.anchor_sample, ring.anchor_ts_us, plan.cursor),
            "the opening frame and the engine's frames must share one clock rule"
        );
    }

    /// Early in a run the requested pre-roll exceeds the history held, so the
    /// cursor clamps to the oldest sample and the wire reports what exists.
    #[test]
    fn segment_plan_clamps_to_the_history_the_ring_actually_holds() {
        let ridx = RingIndex::new(RING_CAPACITY_SAMPLES);
        let ring = anchored_ring(4_000, 4_000, 250_000);
        let onset = SegmentOnset {
            segment_id: 1,
            vad_write_head: 4_000,
            preroll_samples: 16_000,
        };

        let plan = plan_segment_start(&ring, &ridx, &onset);

        assert_eq!(plan.cursor, 0, "nothing older than sample 0 exists");
        assert_eq!(
            plan.start.preroll_samples, 4_000,
            "the wire reports available pre-roll, not requested"
        );
        assert_eq!(plan.start.base_device_ts_us, 0);
    }

    /// A cursor beyond the ring's clock anchor cannot be dated by extrapolating
    /// forward — the anchor is reported as-is.
    #[test]
    fn segment_plan_reports_the_anchor_verbatim_when_it_predates_the_cursor() {
        let ridx = RingIndex::new(RING_CAPACITY_SAMPLES);
        let ring = anchored_ring(40_000, 10_000, 625_000);
        let onset = SegmentOnset {
            segment_id: 2,
            vad_write_head: 40_000,
            preroll_samples: 16_000,
        };

        let plan = plan_segment_start(&ring, &ridx, &onset);

        assert_eq!(plan.cursor, 24_000);
        assert_eq!(
            plan.start.base_device_ts_us, 625_000,
            "an anchor older than the cursor is not extrapolated forward"
        );
    }

    /// An onset with no capture history at all still produces a well-formed
    /// opening frame: an empty pre-roll starting at sample 0.
    #[test]
    fn segment_plan_handles_an_onset_before_any_capture() {
        let ridx = RingIndex::new(RING_CAPACITY_SAMPLES);
        let ring = anchored_ring(0, 0, 0);
        let onset = SegmentOnset {
            segment_id: 0,
            vad_write_head: 0,
            preroll_samples: 16_000,
        };

        let plan = plan_segment_start(&ring, &ridx, &onset);

        assert_eq!(plan.cursor, 0);
        assert_eq!(plan.start.preroll_samples, 0);
        assert_eq!(plan.start.base_device_ts_us, 0);
    }
}
