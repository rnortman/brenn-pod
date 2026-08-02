//! The per-segment streaming engine: one VAD-gated utterance, start to close.
//!
//! Everything between `SegmentStart` (already sent by the caller) and
//! `SegmentEnd` lives here — the drain discipline, the outbound selector, the
//! real-time pace gate, the inbound pump interleave, and the write watchdog.
//! Connect and reconnect policy stay with the caller, which reacts to the
//! returned [`SegmentExit`].
//!
//! The platform-specific things the loop needs travel in [`SegmentDeps`]: the
//! byte transport ([`crate::link::LinkStream`]), the `poll` shim
//! ([`crate::netpoll::NetPoll`]), the platform's monotonic clock in both
//! currencies the loop spends it in (microseconds for wire timestamps,
//! `Instant`s for deadline arithmetic), and two observability sinks (this
//! module's [`ObsEvent`] and the shared inbound path's [`InboundObserver`]).
//! Nothing else about the loop differs between pods, so nothing else is
//! injectable.

use std::io;
use std::ops::Deref;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use audio_pipeline::inbound::{
    FrameAccumulator, InboundConnectionState, InboundObserver, inbound_has_room, note_inbound_exit,
    pump_inbound,
};
use audio_pipeline::pace::{advance_pace_us, pace_wait_us};
use audio_pipeline::playback::PlaybackSink;
use audio_pipeline::ring::{CaptureRing, RingIndex, SAMPLE_RATE_HZ};
use audio_pipeline::stream_send::{
    FrameWriteState, SendOutcome, StepOutcome, write_frame_classified,
};
use audio_pipeline::wire::{
    AUDIO_SAMPLES_PER_FRAME, AudioFrame, EndReason, MAX_AUDIO_PAYLOAD, SegmentEnd, StreamFrame,
    Telemetry as WireTelemetry,
};

use crate::link::LinkStream;
use crate::netpoll::{
    INBOUND_STEPS_PER_WAKE, NetPoll, OUTBOUND_FRAMES_PER_WAKE, Readiness, poll_timeout,
    poll_writable,
};

// ── Streamer message channel ──────────────────────────────────────────────────

/// Messages from the telemetry/VAD thread to the streamer thread.
pub enum StreamerMsg {
    /// VAD gate just opened; carry the write-head sample index at onset time so
    /// the streamer can place the pre-roll cursor.
    VadOpened { write_head: u64 },
    /// VAD gate just closed (hangover expired).
    VadClosed,
    /// XVF3800 telemetry frame, to be forwarded in-band while a segment is open.
    Telemetry(WireTelemetry),
}

// ── Tuning constants ──────────────────────────────────────────────────────────

/// How long POLLOUT stays de-armed once the write spin guard trips. Long enough to hand
/// the TCP stack a real scheduling window, short enough that a handful of backoffs cost
/// only a slice of the 750 ms write budget — the budget, not this pause, remains the
/// terminal bound.
pub const SPIN_BACKOFF_MS: u64 = 10;

/// Cap on the per-segment `pending_telemetry` queue. Telemetry is advisory, so
/// at the cap the oldest is dropped rather than risking heap exhaustion.
pub const PENDING_TELEMETRY_CAP: usize = 8;

/// Depth of the telemetry→streamer channel every pod carries [`StreamerMsg`] over.
///
/// Not a platform tunable: this is the backpressure envelope the engine's own drop
/// accounting is calibrated against, so a retune after reading drop counts off one pod's
/// bench has to apply to both or the two pods stop being the same pipeline. At the wire's
/// 20 ms frame cadence, 64 is ~1.3 s of telemetry queued ahead of a stalled streamer —
/// long enough to ride out a reconnect, short enough that what finally arrives is still
/// about the segment it describes.
pub const STREAMER_CHAN_CAPACITY: usize = 64;

/// Wall-clock cadence of the in-production observability waypoint
/// ([`ObsEvent::Production`]) — bounds the line count regardless of how often
/// the loop wakes.
const PRODUCTION_OBS_PERIOD_US: u64 = 1_000_000;

// ── Observability seam ────────────────────────────────────────────────────────

/// A point in the segment's life the platform may want to sample.
///
/// The engine decides *when* each fires; the observer closure decides what to
/// record. [`as_str`](ObsEvent::as_str) is the stable log token, so the
/// waypoint vocabulary has one owner across platforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObsEvent {
    /// The segment loop is about to run its first wake.
    SegmentOpen,
    /// The pre-roll backlog first drained to steady state — the end of the
    /// catch-up window.
    PrerollDrained,
    /// The segment's first write-block.
    WriteBlocked,
    /// Periodic in-production sample, on a ~1 s wall-clock cadence.
    Production,
    /// The `pending_telemetry` queue hit `cap` with the outbound direction
    /// stalled, so the oldest telemetry frame was dropped.
    TelemetryDropped {
        /// The queue cap that was reached ([`PENDING_TELEMETRY_CAP`]).
        cap: usize,
    },
}

impl ObsEvent {
    /// The event's stable log token.
    pub fn as_str(&self) -> &'static str {
        match self {
            ObsEvent::SegmentOpen => "start",
            ObsEvent::PrerollDrained => "preroll-drained",
            ObsEvent::WriteBlocked => "write-blocked",
            ObsEvent::Production => "prod",
            ObsEvent::TelemetryDropped { .. } => "telemetry-dropped",
        }
    }
}

// ── Frame send helpers ────────────────────────────────────────────────────────

/// Encode and send `frame` with bounded backpressure, discarding the
/// resume-cycle count. See [`send_frame_bp_counted`] for the HIL variant
/// that returns it.
pub fn send_frame_bp<P: NetPoll + ?Sized>(
    poll: &P,
    stream: &mut dyn LinkStream,
    frame: &StreamFrame,
    buf: &mut [u8],
) -> io::Result<SendOutcome> {
    send_frame_bp_counted(poll, stream, frame, buf).0
}

/// Like [`send_frame_bp`] but also returns the resume-cycle count (completed
/// writability waits that were followed by forward progress). Used by HIL
/// self-tests to distinguish a frame that blocked and resumed from one the
/// transport accepted outright.
///
/// `poll` is generic rather than `&dyn NetPoll` to keep the incoming argument
/// words at six on the Xtensa windowed ABI: a shim is a zero-sized type, so a
/// thin `&P` costs one word where a fat `&dyn` costs two.
pub fn send_frame_bp_counted<P: NetPoll + ?Sized>(
    poll: &P,
    stream: &mut dyn LinkStream,
    frame: &StreamFrame,
    buf: &mut [u8],
) -> (io::Result<SendOutcome>, u32) {
    let fd = stream.link_fd();
    write_frame_classified(stream.as_write(), frame, buf, |deadline| {
        poll_writable(poll, fd, deadline)
    })
}

// ── The segment loop ──────────────────────────────────────────────────────────

/// Tags the single in-flight outbound frame so post-send bookkeeping knows what to
/// do: `Audio` bumps delivered counters, `SegmentEnd` exits the segment loop,
/// `Telemetry` completes silently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutboundKind {
    Audio { samples: u32 },
    Telemetry,
    SegmentEnd,
}

/// Wire timestamp for the frame whose first sample is `first_index`, extrapolated
/// from the ring's `(anchor_sample, anchor_ts_us)` pair at [`SAMPLE_RATE_HZ`].
///
/// Indices below the anchor saturate at zero rather than wrapping.
///
/// `pub(crate)` so the segment's opening frame ([`crate::idle::plan_segment_start`])
/// is dated by the same rule as every frame inside it.
pub(crate) fn frame_ts_us(anchor_sample: u64, anchor_ts_us: u64, first_index: u64) -> u64 {
    if anchor_sample >= first_index {
        let delta = anchor_sample - first_index;
        anchor_ts_us.saturating_sub(delta * 1_000_000 / SAMPLE_RATE_HZ as u64)
    } else {
        let delta = first_index - anchor_sample;
        anchor_ts_us + delta * 1_000_000 / SAMPLE_RATE_HZ as u64
    }
}

/// Append `count` samples starting at absolute index `first_index` to `pcm` as
/// little-endian `i16` pairs.
///
/// The caller holds the ring lock and has already re-checked overrun under it, so
/// the samples read here are the ones the overrun check cleared.
fn push_pcm_samples<B: Deref<Target = [i16]>>(
    ring: &CaptureRing<B>,
    ridx: &RingIndex,
    first_index: u64,
    count: usize,
    pcm: &mut heapless::Vec<u8, MAX_AUDIO_PAYLOAD>,
) {
    for i in 0..count {
        let slot = ridx.slot(first_index + i as u64);
        let bytes = ring.samples[slot].to_le_bytes();
        pcm.push(bytes[0]).expect("pcm push overflow");
        pcm.push(bytes[1]).expect("pcm push overflow");
    }
}

/// The full mutable dependency set of the post-SegmentStart streaming span.
///
/// Groups everything the `'stream` loop touches so [`run_segment`] can be driven
/// with test-supplied inputs (a HIL harness passes its own ring, channel, flag,
/// and sink) without inheriting the production connect/backoff state. The socket
/// is already connected with `SegmentStart` sent; the caller owns all reconnect
/// policy and reacts to the returned [`SegmentExit`].
///
/// One struct rather than a long argument list: `run_segment` must stay within
/// the Xtensa realign-miscompile guard's six-argument-word budget.
pub struct SegmentDeps<'a, B> {
    /// Connected, non-blocking stream with `SegmentStart` already sent.
    pub socket: &'a mut dyn LinkStream,
    /// Telemetry/VAD → streamer channel.
    pub rx: &'a std::sync::mpsc::Receiver<StreamerMsg>,
    /// Capture ring.
    pub ring: &'a Mutex<Option<CaptureRing<B>>>,
    /// Lossless VAD-closed flag.
    pub vad_closed_flag: &'a AtomicBool,
    /// Ring index geometry.
    pub ridx: &'a RingIndex,
    /// Inbound reassembly buffer.
    pub inbound_accum: &'a mut FrameAccumulator,
    /// Playback sink for decoded inbound PCM.
    pub inbound_sink: &'a mut dyn PlaybackSink,
    /// Per-connection inbound framing state.
    pub inbound_state: &'a mut InboundConnectionState,
    /// Reused encode scratch for outbound frames.
    pub outbound_buf: &'a mut Vec<u8>,
    /// The platform's `poll()` shim.
    pub poll: &'a dyn NetPoll,
    /// Platform monotonic clock in microseconds — the wire's `device_ts_us`
    /// source and the pace gate's time base. Only intra-segment differences are
    /// load-bearing, so any monotonic origin works.
    pub now_us: &'a dyn Fn() -> u64,
    /// The same monotonic clock as an [`Instant`] — every deadline the loop
    /// arms or checks (write budget and per-frame ceiling, pace gate, spin
    /// backoff, poll timeout) reads it. Production passes `Instant::now`; the
    /// two currencies are never compared against each other, so only each one's
    /// own monotonicity matters.
    pub now_instant: &'a dyn Fn() -> Instant,
    /// Where the loop's [`ObsEvent`]s go.
    pub obs: &'a mut dyn FnMut(ObsEvent),
    /// Observer for the shared inbound path (its own waypoints and the accepted
    /// playback format).
    pub inbound_obs: &'a mut dyn InboundObserver,
}

/// How a [`run_segment`] call terminated. The caller maps each onto reconnect
/// policy.
#[derive(Debug, PartialEq, Eq)]
pub enum SegmentExit {
    /// `SegmentEnd` was sent; the socket is healthy and kept.
    Completed,
    /// The segment was dropped (write backpressure or channel disconnect) but
    /// the socket is still usable and kept.
    SegmentDroppedSocketKept,
    /// The socket faulted or must be torn down; the caller runs its
    /// socket-teardown path.
    SocketLost,
}

/// Run one capture segment: drain the ring into `AudioFrame`s, interleave
/// `Telemetry`, drain inbound playback, and close with `SegmentEnd`.
///
/// The socket in `deps` is connected with `SegmentStart` already sent, and
/// `read_cursor` is the pre-roll cursor. Returns a [`SegmentExit`] the caller
/// translates into reconnect policy — all connect/backoff state stays outside.
pub fn run_segment<B>(
    deps: &mut SegmentDeps<'_, B>,
    segment_id: u32,
    read_cursor: u64,
) -> SegmentExit
where
    B: Deref<Target = [i16]>,
{
    let mut read_cursor = read_cursor;
    let mut frames_sent: u32 = 0;
    let mut samples_sent: u64 = 0;
    let mut pace_resyncs: u32 = 0;
    let mut vad_closed = false;
    let mut outbound: Option<(FrameWriteState, OutboundKind)> = None;
    let mut segment_end: Option<EndReason> = None;
    // Set when the telemetry channel disconnected mid-segment. The sender is gone, so
    // the loop stops draining the channel and only pushes the closing SegmentEnd out;
    // exit paths then keep the socket only if that frame left the wire frame-aligned.
    let mut channel_lost = false;
    // POLLOUT is armed only while a write actually blocked; otherwise writes are
    // attempted optimistically each wake. `write_blocked` implies an in-flight frame.
    let mut write_blocked = false;
    // Carries "a pump stopped with work remaining" from this wake into the next
    // wake's poll timeout: while true the loop re-polls with 0 rather than sleeping on
    // the tick. Seeded true so the segment's opening pre-roll backlog begins draining
    // immediately; the pace gate below then bounds the drain rate.
    let mut work_pending = true;
    // Earliest instant (the injected monotonic µs clock; None before the segment's first
    // frame) the next outbound audio frame may be emitted, bounding the catch-up drain to
    // CATCH_UP_PACE_MULTIPLIER × real time. Steady-state production is slower than the
    // paced cadence, so the gate binds only while a backlog is draining.
    let mut audio_pace_schedule: Option<u64> = None;
    // Set when the pace gate defers a ready audio frame; carried into the next poll so
    // the loop sleeps until the frame is due instead of busy-repolling.
    let mut pace_deadline: Option<Instant> = None;
    // Set when the write spin guard trips (poll says POLLOUT, write says WouldBlock, over
    // and over): POLLOUT stays de-armed until this instant so the loop sleeps in `poll`
    // instead of spinning, leaving the TCP stack the CPU it needs to clear the stall.
    let mut spin_backoff_deadline: Option<Instant> = None;
    let mut pending_telemetry: std::collections::VecDeque<WireTelemetry> =
        std::collections::VecDeque::new();

    // ── Intra-segment observability waypoints ────────────────────────
    // Time-bracket the transient heap dive within a segment so a gradual aggregate
    // fill-to-floor (elastic consumers expanding into headroom) is distinguishable
    // from a single-instant spike. Waypoints: segment start, the moment the pre-roll
    // backlog first drains to steady state, the first write-block of the segment, and
    // a ~1 s cadence during production. No per-frame sampling; the budget is a handful
    // of events per segment.
    let seg_start_us = (deps.now_us)();
    (deps.obs)(ObsEvent::SegmentOpen);
    let mut preroll_drain_logged = false;
    let mut first_write_blocked_logged = false;
    let mut last_periodic_wp_us = seg_start_us;

    loop {
        {
            let now_us = (deps.now_us)();
            if now_us.saturating_sub(last_periodic_wp_us) >= PRODUCTION_OBS_PERIOD_US {
                last_periodic_wp_us = now_us;
                (deps.obs)(ObsEvent::Production);
            }
        }
        // The loop never sleeps while either direction has actionable work: each
        // direction drains until WouldBlock / exhaustion / a fairness cap, and the poll
        // timeout is 0 whenever a pump stopped at its cap (`work_pending`).

        // ── Poll ─────────────────────────────────────────────────────
        // POLLIN while the accumulator has room; POLLOUT only while a write blocked —
        // writes are attempted optimistically, so arming POLLOUT while writable would
        // just busy-wake.
        let inbound_armed = inbound_has_room(deps.inbound_accum);
        let now = (deps.now_instant)();
        // Backoff expired: re-arm POLLOUT and let a fresh run of disagreement re-trip.
        if spin_backoff_deadline.is_some_and(|d| now >= d) {
            spin_backoff_deadline = None;
            if let Some((state, _)) = outbound.as_mut() {
                state.reset_spin_guard();
            }
        }
        let interest = deps.socket.poll_interest(
            inbound_armed,
            write_blocked && spin_backoff_deadline.is_none(),
        );
        let fd = deps.socket.link_fd();
        // The write deadline bounds the wait only while blocked on POLLOUT; the pace
        // deadline (set when a ready audio frame was deferred for rate-limiting) bounds
        // it while the catch-up drain is throttled. The earlier of the two wins.
        let write_deadline = if write_blocked {
            outbound.as_ref().map(|(st, _)| st.next_deadline())
        } else {
            None
        };
        // The spin backoff joins the min so the loop wakes to re-arm POLLOUT the moment it
        // expires; it can only shorten the wait, never delay a write budget/ceiling firing.
        let deadline = write_deadline
            .into_iter()
            .chain(pace_deadline)
            .chain(spin_backoff_deadline)
            .min();
        let timeout = poll_timeout(now, deadline, work_pending);
        // Consumed by this poll; the outbound pump re-arms it below if the gate still
        // defers a frame.
        pace_deadline = None;
        let ready = deps.poll.readiness(fd, interest, timeout);
        if let Readiness::Fault(e) = ready {
            log::warn!(
                "streamer: poll fault mid-segment (seg {}): {:?} — clearing socket",
                segment_id,
                e
            );
            return SegmentExit::SocketLost;
        }

        // ── Channel + VAD-flag drain ─────────────────────────────────
        while !channel_lost {
            match deps.rx.try_recv() {
                Ok(StreamerMsg::Telemetry(tel)) => {
                    if pending_telemetry.len() >= PENDING_TELEMETRY_CAP {
                        pending_telemetry.pop_front();
                        (deps.obs)(ObsEvent::TelemetryDropped {
                            cap: PENDING_TELEMETRY_CAP,
                        });
                    }
                    pending_telemetry.push_back(tel);
                }
                Ok(StreamerMsg::VadClosed) => {
                    vad_closed = true;
                }
                Ok(StreamerMsg::VadOpened { .. }) => {
                    // Re-onset during hangover — ignored (FSM-handled).
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    // Best-effort close: route SegmentEnd(InternalError) through the
                    // normal selector + pump so the in-flight frame drains first and the
                    // write budget / POLLOUT handling is not duplicated here.
                    log::error!("streamer: channel disconnected mid-segment");
                    channel_lost = true;
                    if segment_end.is_none() {
                        segment_end = Some(EndReason::InternalError);
                    }
                    break;
                }
            }
        }
        // Atomic fallback: channel message can be dropped under TCP stall.
        if !vad_closed
            && deps
                .vad_closed_flag
                .load(std::sync::atomic::Ordering::Acquire)
        {
            vad_closed = true;
        }

        // ── Inbound pump ─────────────────────────────────────────────
        // Drain inbound until WouldBlock or the per-wake cap; the gate on the first
        // call keeps the load-bearing re-offer-under-backpressure guard (`!inbound_armed`).
        let mut inbound_work = false;
        // Poll discipline rule 1: on a transport that buffers decrypted
        // plaintext, readiness under-reports what is available, so read every
        // wake instead of only when POLLIN fired.
        if ready.readable() || !inbound_armed || deps.socket.buffers_plaintext() {
            match pump_inbound(
                deps.socket.as_read(),
                deps.inbound_accum,
                deps.inbound_sink,
                deps.inbound_state,
                deps.inbound_obs,
                INBOUND_STEPS_PER_WAKE,
            ) {
                Ok(p) => inbound_work = p.hit_cap,
                Err(e) => {
                    log::warn!("streamer: inbound drain error — clearing socket: {:?}", e);
                    // Blind-window coverage: one last reading on the inbound
                    // error/exit path, before the socket is torn down. Gated on
                    // seen_hello inside the helper.
                    note_inbound_exit(deps.inbound_obs, deps.inbound_state);
                    return SegmentExit::SocketLost;
                }
            }
        }

        // ── Outbound pump ────────────────────────────────────────────
        if ready.writable() {
            write_blocked = false;
        }
        let mut outbound_work = false;
        let mut frames_this_wake: u32 = 0;
        'outbound: loop {
            if frames_this_wake >= OUTBOUND_FRAMES_PER_WAKE {
                // Stopped for fairness with more to send → re-poll with timeout 0.
                outbound_work = true;
                break 'outbound;
            }

            if outbound.is_none() {
                // ── Selector: SegmentEnd → telemetry → mic AudioFrame → partial-at-close ──
                if let Some(reason) = segment_end {
                    let now_us = (deps.now_us)();
                    let seg_end = StreamFrame::SegmentEnd(SegmentEnd {
                        segment_id,
                        device_ts_us: now_us,
                        frames_sent,
                        samples_sent,
                        reason,
                    });
                    match FrameWriteState::begin(
                        &seg_end,
                        deps.outbound_buf.as_mut_slice(),
                        deps.now_instant,
                    ) {
                        Ok(state) => outbound = Some((state, OutboundKind::SegmentEnd)),
                        Err(e) => {
                            log::warn!(
                                "streamer: SegmentEnd encode failed (seg {}): {:?} — clearing socket",
                                segment_id,
                                e
                            );
                            return SegmentExit::SocketLost;
                        }
                    }
                } else if let Some(tel) = pending_telemetry.pop_front() {
                    let tel_frame = StreamFrame::Telemetry(tel);
                    match FrameWriteState::begin(
                        &tel_frame,
                        deps.outbound_buf.as_mut_slice(),
                        deps.now_instant,
                    ) {
                        Ok(state) => outbound = Some((state, OutboundKind::Telemetry)),
                        Err(e) => {
                            log::warn!(
                                "streamer: Telemetry encode failed (seg {}): {:?} — dropping segment, keeping socket",
                                segment_id,
                                e
                            );
                            // Local fault: no bytes written, the stream stays
                            // frame-aligned, so the connection remains usable.
                            return SegmentExit::SegmentDroppedSocketKept;
                        }
                    }
                } else {
                    let (write_head, anchor_sample, anchor_ts_us) = {
                        let guard = deps
                            .ring
                            .lock()
                            .unwrap_or_else(|_| panic!("capture ring mutex poisoned in streamer"));
                        let ring = guard.as_ref().expect("capture ring not initialized");
                        (ring.write_head, ring.anchor_sample, ring.anchor_ts_us)
                    };

                    // Lapped cursor → close segment.
                    if deps.ridx.is_overrun(write_head, read_cursor) {
                        log::warn!("streamer: ring overrun in segment {}", segment_id);
                        segment_end = Some(EndReason::Overrun);
                        continue 'outbound;
                    }

                    let avail = deps.ridx.available(write_head, read_cursor);

                    if avail >= AUDIO_SAMPLES_PER_FRAME as u64 {
                        // ── Real-time pace gate ──────────────────────────────
                        // A full frame of backlog is ready. Release it no faster than
                        // the paced cadence so the pre-roll catch-up does not blast the
                        // whole backlog into the TX pool + TCP send queue at once —
                        // bounding transient heap consumption during the drain window.
                        let now_us = (deps.now_us)();
                        if let Some(wait_us) = pace_wait_us(audio_pace_schedule, now_us) {
                            pace_deadline =
                                Some((deps.now_instant)() + Duration::from_micros(wait_us));
                            break 'outbound;
                        }
                        let adv = advance_pace_us(audio_pace_schedule, now_us);
                        if adv.resynced {
                            pace_resyncs = pace_resyncs.saturating_add(1);
                        }
                        audio_pace_schedule = Some(adv.next_schedule_us);

                        let frame_first_index = read_cursor;
                        let frame_ts = frame_ts_us(anchor_sample, anchor_ts_us, frame_first_index);

                        let mut pcm: heapless::Vec<u8, MAX_AUDIO_PAYLOAD> = heapless::Vec::new();
                        {
                            let guard = deps.ring.lock().unwrap_or_else(|_| {
                                panic!("capture ring mutex poisoned in streamer")
                            });
                            let ring = guard.as_ref().expect("capture ring not initialized");
                            // Re-check overrun under the copy lock.
                            let live_head = ring.write_head;
                            if deps.ridx.is_overrun(live_head, read_cursor) {
                                drop(guard);
                                segment_end = Some(EndReason::Overrun);
                                continue 'outbound;
                            }
                            push_pcm_samples(
                                ring,
                                deps.ridx,
                                read_cursor,
                                AUDIO_SAMPLES_PER_FRAME,
                                &mut pcm,
                            );
                        }

                        let audio_frame = StreamFrame::Audio(AudioFrame {
                            segment_id,
                            first_sample_index: frame_first_index,
                            device_ts_us: frame_ts,
                            pcm,
                        });
                        match FrameWriteState::begin(
                            &audio_frame,
                            deps.outbound_buf.as_mut_slice(),
                            deps.now_instant,
                        ) {
                            Ok(state) => {
                                read_cursor += AUDIO_SAMPLES_PER_FRAME as u64;
                                outbound = Some((
                                    state,
                                    OutboundKind::Audio {
                                        samples: AUDIO_SAMPLES_PER_FRAME as u32,
                                    },
                                ));
                            }
                            Err(e) => {
                                log::warn!(
                                    "streamer: AudioFrame encode failed (seg {}): {:?} — dropping segment, keeping socket",
                                    segment_id,
                                    e
                                );
                                // Local size/serialization fault; socket untouched and
                                // frame-aligned.
                                return SegmentExit::SegmentDroppedSocketKept;
                            }
                        }
                    } else if vad_closed && avail < AUDIO_SAMPLES_PER_FRAME as u64 {
                        // VAD released with < full frame residual → drain partial, then close.
                        let partial = avail as usize;
                        if partial > 0 {
                            let frame_first_index = read_cursor;
                            let mut pcm: heapless::Vec<u8, MAX_AUDIO_PAYLOAD> =
                                heapless::Vec::new();
                            let frame_ts;
                            {
                                let guard = deps.ring.lock().unwrap_or_else(|_| {
                                    panic!("capture ring mutex poisoned in streamer")
                                });
                                let ring = guard.as_ref().expect("capture ring not initialized");
                                let live_head = ring.write_head;
                                if deps.ridx.is_overrun(live_head, read_cursor) {
                                    drop(guard);
                                    log::warn!(
                                        "streamer: ring overrun in partial-frame copy (seg {})",
                                        segment_id
                                    );
                                    segment_end = Some(EndReason::Overrun);
                                    continue 'outbound;
                                }
                                frame_ts = frame_ts_us(
                                    ring.anchor_sample,
                                    ring.anchor_ts_us,
                                    frame_first_index,
                                );
                                push_pcm_samples(ring, deps.ridx, read_cursor, partial, &mut pcm);
                            }
                            let audio_frame = StreamFrame::Audio(AudioFrame {
                                segment_id,
                                first_sample_index: frame_first_index,
                                device_ts_us: frame_ts,
                                pcm,
                            });
                            match FrameWriteState::begin(
                                &audio_frame,
                                deps.outbound_buf.as_mut_slice(),
                                deps.now_instant,
                            ) {
                                Ok(state) => {
                                    read_cursor += partial as u64;
                                    segment_end = Some(EndReason::VadRelease);
                                    outbound = Some((
                                        state,
                                        OutboundKind::Audio {
                                            samples: partial as u32,
                                        },
                                    ));
                                }
                                Err(e) => {
                                    log::warn!(
                                        "streamer: partial AudioFrame encode failed (seg {}): {:?} — dropping segment, keeping socket",
                                        segment_id,
                                        e
                                    );
                                    // Local fault; socket untouched and frame-aligned.
                                    return SegmentExit::SegmentDroppedSocketKept;
                                }
                            }
                        } else {
                            segment_end = Some(EndReason::VadRelease);
                            continue 'outbound;
                        }
                    } else {
                        // Caught up, VAD still open — nothing to send this wake. The
                        // first arrival here marks the pre-roll backlog fully drained to
                        // steady state (the end of the catch-up window).
                        if !preroll_drain_logged {
                            preroll_drain_logged = true;
                            (deps.obs)(ObsEvent::PrerollDrained);
                        }
                        break 'outbound;
                    }
                }
            }

            // ── In-flight frame: optimistic non-blocking write ──
            if write_blocked {
                // Kernel send buffer full; wait for POLLOUT (armed at the next poll).
                break 'outbound;
            }
            let Some((state, kind)) = outbound.as_mut() else {
                unreachable!("outbound is Some after the selector built a frame");
            };
            match state.step_writable(
                deps.socket.as_write(),
                deps.outbound_buf.as_slice(),
                deps.now_instant,
            ) {
                Ok(StepOutcome::WroteWhole) => {
                    match *kind {
                        OutboundKind::Audio { samples } => {
                            frames_sent += 1;
                            samples_sent += samples as u64;
                            outbound = None;
                        }
                        OutboundKind::Telemetry => {
                            outbound = None;
                        }
                        OutboundKind::SegmentEnd => {
                            log::info!(
                                "streamer: segment {} ended frames={} samples={} pace_resyncs={}",
                                segment_id,
                                frames_sent,
                                samples_sent,
                                pace_resyncs
                            );
                            // A channel-loss close is a dropped segment, not a normal
                            // completion; the socket stays only because the closing
                            // frame went out whole.
                            return if channel_lost {
                                SegmentExit::SegmentDroppedSocketKept
                            } else {
                                SegmentExit::Completed
                            };
                        }
                    }
                    frames_this_wake += 1;
                }
                // Kernel took bytes but the frame is not done — retry immediately, no poll.
                Ok(StepOutcome::WrotePartial) => {}
                Ok(StepOutcome::WouldBlock) => {
                    if !first_write_blocked_logged {
                        first_write_blocked_logged = true;
                        (deps.obs)(ObsEvent::WriteBlocked);
                    }
                    write_blocked = true;
                    if state.spin_guard_tripped() && spin_backoff_deadline.is_none() {
                        spin_backoff_deadline =
                            Some((deps.now_instant)() + Duration::from_millis(SPIN_BACKOFF_MS));
                    }
                    break 'outbound;
                }
                Err(e) => {
                    // Partial write leaves receiver mid-frame; can't resume.
                    log::warn!(
                        "streamer: outbound send failed mid-segment (seg {}): {:?} — dropping segment, clearing socket",
                        segment_id,
                        e
                    );
                    return SegmentExit::SocketLost;
                }
            }
        }

        // ── Write watchdog: enforce budget/ceiling once per wake for an in-flight frame ──
        if let Some((state, _)) = outbound.as_mut() {
            match state.check_deadlines(deps.now_instant) {
                None => {}
                Some(Ok(SendOutcome::BackpressureAligned)) => {
                    log::warn!(
                        "streamer: outbound backpressure mid-segment (seg {}) — dropping segment, keeping socket",
                        segment_id
                    );
                    return SegmentExit::SegmentDroppedSocketKept;
                }
                Some(Ok(SendOutcome::Sent)) => {
                    unreachable!(
                        "check_deadlines returned Sent, which its contract forbids — invariant violated"
                    );
                }
                Some(Err(e)) => {
                    log::warn!(
                        "streamer: outbound write ceiling/budget elapsed mid-tail (seg {}): {:?} — dropping segment, clearing socket",
                        segment_id,
                        e
                    );
                    return SegmentExit::SocketLost;
                }
            }
        }

        work_pending = inbound_work || outbound_work;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::link::PollInterest;
    use crate::test_support::{
        FakeLink, FakePoll, RecordingInboundObs, RecordingSink, decode_stream, free_running_clock,
        inbound_audio, inbound_hello, ring_with, wire_bytes,
    };
    use audio_pipeline::inbound::InboundWaypoint;
    use audio_pipeline::ring::RING_CAPACITY_SAMPLES;
    use audio_pipeline::stream_send::SPIN_GUARD_THRESHOLD;
    use audio_pipeline::wire::{DEVICE_PLAYBACK_FORMAT, MAX_FRAME_BYTES, TelemetryKind};
    use std::cell::{Cell, RefCell};
    use std::sync::atomic::Ordering;

    /// Everything a segment run needs that outlives `SegmentDeps`' borrows.
    struct Harness {
        link: FakeLink,
        ridx: RingIndex,
        vad_closed: AtomicBool,
        accum: FrameAccumulator,
        sink: RecordingSink,
        state: InboundConnectionState,
        inbound_obs: RecordingInboundObs,
        buf: Vec<u8>,
    }

    impl Harness {
        fn new() -> Self {
            Self::with_outbound_buf(MAX_FRAME_BYTES + 2)
        }

        /// A harness whose encode scratch is `buf_bytes` long. Two bytes leaves the
        /// encoder no room past the length prefix, so every `begin` fails.
        fn with_outbound_buf(buf_bytes: usize) -> Self {
            Self {
                link: FakeLink::new(),
                ridx: RingIndex::new(RING_CAPACITY_SAMPLES),
                vad_closed: AtomicBool::new(false),
                accum: FrameAccumulator::new(),
                sink: RecordingSink::new(),
                state: InboundConnectionState::new(),
                inbound_obs: RecordingInboundObs::new(),
                buf: vec![0u8; buf_bytes],
            }
        }
    }

    /// Outcome of one scripted segment run.
    struct Run {
        exit: SegmentExit,
        frames: Vec<StreamFrame>,
        /// Bytes of an incomplete trailing frame left on the wire.
        partial: usize,
        events: Vec<ObsEvent>,
    }

    /// Drive one segment against the fakes on the real `Instant` clock. `clock`
    /// supplies the µs seam.
    fn run(
        h: &mut Harness,
        ring: &Mutex<Option<CaptureRing<Box<[i16]>>>>,
        rx: &std::sync::mpsc::Receiver<StreamerMsg>,
        poll: &FakePoll,
        clock: &dyn Fn() -> u64,
        read_cursor: u64,
    ) -> Run {
        run_at(h, ring, rx, poll, clock, &Instant::now, read_cursor)
    }

    /// [`run`] with the deadline clock injected too, so budget/ceiling exits are
    /// reachable without waiting out a real 750 ms.
    fn run_at(
        h: &mut Harness,
        ring: &Mutex<Option<CaptureRing<Box<[i16]>>>>,
        rx: &std::sync::mpsc::Receiver<StreamerMsg>,
        poll: &FakePoll,
        clock: &dyn Fn() -> u64,
        instants: &dyn Fn() -> Instant,
        read_cursor: u64,
    ) -> Run {
        let events = RefCell::new(Vec::new());
        let exit = {
            let mut obs = |event: ObsEvent| events.borrow_mut().push(event);
            let mut deps = SegmentDeps {
                socket: &mut h.link,
                rx,
                ring,
                vad_closed_flag: &h.vad_closed,
                ridx: &h.ridx,
                inbound_accum: &mut h.accum,
                inbound_sink: &mut h.sink,
                inbound_state: &mut h.state,
                outbound_buf: &mut h.buf,
                poll,
                now_us: clock,
                now_instant: instants,
                obs: &mut obs,
                inbound_obs: &mut h.inbound_obs,
            };
            run_segment(&mut deps, 7, read_cursor)
        };
        let (frames, partial) = decode_stream(&h.link.written);
        Run {
            exit,
            frames,
            partial,
            events: events.into_inner(),
        }
    }

    /// An `Instant` clock advancing `step` per read from an arbitrary origin — a few
    /// reads carry it past any write budget or ceiling without a real wait.
    fn stepping_instants(step: Duration) -> impl Fn() -> Instant {
        let base = Instant::now();
        let reads = Cell::new(0u32);
        move || {
            let now = base + step * reads.get();
            reads.set(reads.get() + 1);
            now
        }
    }

    // ── Frame timestamp mapping ───────────────────────────────────────────────

    /// The anchor maps to itself, earlier samples subtract and later samples add
    /// at the platform sample rate, and an anchor too small to cover the offset
    /// floors at zero instead of wrapping.
    #[test]
    fn frame_ts_us_extrapolates_both_directions_from_the_anchor() {
        let rate = SAMPLE_RATE_HZ as u64;
        assert_eq!(frame_ts_us(1_000, 5_000_000, 1_000), 5_000_000);

        // 1 s of samples either side of the anchor is 1 s of timestamp.
        assert_eq!(frame_ts_us(rate, 5_000_000, 0), 4_000_000);
        assert_eq!(frame_ts_us(0, 5_000_000, rate), 6_000_000);

        // Sub-µs-per-sample truncation: 320 samples = 20 ms exactly.
        assert_eq!(
            frame_ts_us(AUDIO_SAMPLES_PER_FRAME as u64, 1_000_000, 0),
            980_000
        );

        assert_eq!(
            frame_ts_us(rate, 1_000, 0),
            0,
            "an anchor younger than the offset floors at zero"
        );
    }

    // ── Segment completion ────────────────────────────────────────────────────

    /// A VAD-released segment with one full frame plus a partial residual drains
    /// both and closes with `SegmentEnd{VadRelease}`, socket kept.
    #[test]
    fn vad_release_drains_full_frame_then_partial_and_completes() {
        let samples = AUDIO_SAMPLES_PER_FRAME as u64 + 100;
        let ring = ring_with(samples, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::new();
        h.vad_closed.store(true, Ordering::Release);
        let poll = FakePoll::always_writable();

        let out = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        assert_eq!(out.exit, SegmentExit::Completed);
        assert_eq!(out.frames.len(), 3, "full frame, partial frame, SegmentEnd");
        match &out.frames[0] {
            StreamFrame::Audio(a) => {
                assert_eq!(a.segment_id, 7);
                assert_eq!(a.first_sample_index, 0);
                assert_eq!(a.pcm.len(), AUDIO_SAMPLES_PER_FRAME * 2);
                assert_eq!(a.device_ts_us, frame_ts_us(samples - 1, 2_000_000, 0));
            }
            other => panic!("expected a full AudioFrame, got {other:?}"),
        }
        match &out.frames[1] {
            StreamFrame::Audio(a) => {
                assert_eq!(a.first_sample_index, AUDIO_SAMPLES_PER_FRAME as u64);
                assert_eq!(
                    a.pcm.len(),
                    100 * 2,
                    "residual is drained as a partial frame"
                );
                assert_eq!(
                    a.device_ts_us,
                    frame_ts_us(samples - 1, 2_000_000, AUDIO_SAMPLES_PER_FRAME as u64)
                );
            }
            other => panic!("expected a partial AudioFrame, got {other:?}"),
        }
        match &out.frames[2] {
            StreamFrame::SegmentEnd(e) => {
                assert_eq!(e.reason, EndReason::VadRelease);
                assert_eq!(e.frames_sent, 2);
                assert_eq!(e.samples_sent, AUDIO_SAMPLES_PER_FRAME as u64 + 100);
            }
            other => panic!("expected SegmentEnd, got {other:?}"),
        }
    }

    /// The closing `SegmentEnd`'s `device_ts_us` comes from the injected clock.
    #[test]
    fn segment_end_timestamp_comes_from_the_injected_clock() {
        const FROZEN: u64 = 123_456_789;
        let ring = ring_with(0, FROZEN);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::new();
        h.vad_closed.store(true, Ordering::Release);
        let poll = FakePoll::always_writable();

        let out = run(&mut h, &ring, &rx, &poll, &|| FROZEN, 0);

        assert_eq!(out.exit, SegmentExit::Completed);
        match out.frames.as_slice() {
            [StreamFrame::SegmentEnd(e)] => {
                assert_eq!(e.device_ts_us, FROZEN);
                assert_eq!(e.frames_sent, 0, "empty ring at VAD release sends no audio");
            }
            other => panic!("expected a lone SegmentEnd, got {other:?}"),
        }
    }

    /// A ring whose write head has lapped the read cursor closes the segment as
    /// `Overrun` rather than shipping corrupt audio.
    #[test]
    fn lapped_cursor_closes_the_segment_as_overrun() {
        let ring = ring_with(RING_CAPACITY_SAMPLES as u64 + 5_000, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::new();
        let poll = FakePoll::always_writable();

        let out = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        assert_eq!(out.exit, SegmentExit::Completed);
        match out.frames.as_slice() {
            [StreamFrame::SegmentEnd(e)] => assert_eq!(e.reason, EndReason::Overrun),
            other => panic!("expected a lone SegmentEnd(Overrun), got {other:?}"),
        }
    }

    // ── Injected seams ────────────────────────────────────────────────────────

    /// A frozen clock leaves the pace gate deferring every frame after the first,
    /// so a multi-frame backlog releases exactly one frame however many times the
    /// loop wakes. The gate reads the injected clock, not wall time.
    #[test]
    fn the_pace_gate_throttles_on_the_injected_clock() {
        let ring = ring_with(AUDIO_SAMPLES_PER_FRAME as u64 * 4, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::new();
        let poll = FakePoll::faulting_after(6);

        let out = run(&mut h, &ring, &rx, &poll, &|| 5_000_000, 0);

        assert_eq!(
            out.exit,
            SegmentExit::SocketLost,
            "the scripted fault is the only exit from a paced, VAD-open segment"
        );
        assert_eq!(
            out.frames.len(),
            1,
            "the pace gate released one frame and deferred the rest of the backlog"
        );
        assert!(matches!(out.frames[0], StreamFrame::Audio(_)));
    }

    /// A poll fault is a dead socket: the engine stops the segment and hands the
    /// teardown decision back to the caller.
    #[test]
    fn a_poll_fault_ends_the_segment_as_socket_lost() {
        let ring = ring_with(0, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::new();
        let poll = FakePoll::faulting_after(0);

        let out = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        assert_eq!(out.exit, SegmentExit::SocketLost);
        assert!(out.frames.is_empty(), "a faulted wake writes nothing");
        assert_eq!(
            out.events,
            vec![ObsEvent::SegmentOpen],
            "the open waypoint fires before the first wake"
        );
    }

    /// The poll shim sees the fd and the interest the engine armed: POLLIN while
    /// the inbound accumulator has room, no POLLOUT while nothing is blocked.
    #[test]
    fn the_poll_shim_sees_the_armed_interest() {
        let ring = ring_with(0, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::new();
        let poll = FakePoll::faulting_after(0);

        let _ = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        let seen = poll.seen.borrow();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, 42, "the engine polls the link's own fd");
        assert_eq!(seen[0].1, PollInterest::READ);
        assert_eq!(
            seen[0].2,
            Duration::ZERO,
            "the opening wake re-polls immediately (work_pending seeded true)"
        );
    }

    /// The observer sees the open waypoint, then `PrerollDrained` the first time
    /// the loop finds itself caught up with the VAD still open — and only once.
    #[test]
    fn preroll_drained_fires_once_when_the_backlog_clears() {
        let ring = ring_with(AUDIO_SAMPLES_PER_FRAME as u64, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::new();
        let poll = FakePoll::faulting_after(4);

        let out = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        assert_eq!(out.exit, SegmentExit::SocketLost);
        assert_eq!(
            out.events,
            vec![ObsEvent::SegmentOpen, ObsEvent::PrerollDrained],
            "one frame drains, then the caught-up branch reports once"
        );
    }

    /// The periodic waypoint fires on the engine's own µs cadence, taken from the
    /// injected clock — one event per elapsed second, not per wake.
    #[test]
    fn the_production_waypoint_follows_the_injected_clock() {
        let ring = ring_with(0, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::new();
        let poll = FakePoll::faulting_after(3);
        // Each clock read jumps a full cadence period, so every wake past the
        // first is due.
        let reads = Cell::new(0u64);
        let clock = move || {
            let now = reads.get() * PRODUCTION_OBS_PERIOD_US;
            reads.set(reads.get() + 1);
            now
        };

        let out = run(&mut h, &ring, &rx, &poll, &clock, 0);

        assert_eq!(out.exit, SegmentExit::SocketLost);
        assert_eq!(
            out.events,
            vec![
                ObsEvent::SegmentOpen,
                ObsEvent::Production,
                // The first wake also finds the (empty) ring caught up.
                ObsEvent::PrerollDrained,
                ObsEvent::Production,
                ObsEvent::Production,
                ObsEvent::Production,
            ],
            "one Production event per wake whose clock read is a period past the last, \
             including the wake whose poll then faults"
        );
    }

    // ── Telemetry interleave ──────────────────────────────────────────────────

    /// Queued telemetry is forwarded in-band ahead of the close, and a queue past
    /// the cap drops the oldest and reports it through the observer.
    #[test]
    fn telemetry_over_the_cap_drops_the_oldest_and_reports_it() {
        let ring = ring_with(0, 2_000_000);
        let (tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        for i in 0..(PENDING_TELEMETRY_CAP + 2) {
            tx.send(StreamerMsg::Telemetry(WireTelemetry {
                device_ts_us: i as u64,
                kind: TelemetryKind::SpEnergy {
                    values: [i as f32, 0.0, 0.0, 0.0],
                },
            }))
            .expect("channel accepts telemetry");
        }
        let mut h = Harness::new();
        h.vad_closed.store(true, Ordering::Release);
        let poll = FakePoll::always_writable();

        let out = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        assert_eq!(out.exit, SegmentExit::Completed);
        assert_eq!(
            out.events
                .iter()
                .filter(|e| matches!(e, ObsEvent::TelemetryDropped { .. }))
                .count(),
            2,
            "two frames past the cap means two drops"
        );
        assert!(out.events.contains(&ObsEvent::TelemetryDropped {
            cap: PENDING_TELEMETRY_CAP
        }));
        assert_eq!(
            out.frames.len(),
            PENDING_TELEMETRY_CAP + 1,
            "the capped queue plus the closing SegmentEnd"
        );
        // The two oldest were dropped, so the first forwarded frame is index 2.
        match &out.frames[0] {
            StreamFrame::Telemetry(t) => assert_eq!(t.device_ts_us, 2),
            other => panic!("expected Telemetry first, got {other:?}"),
        }
        assert!(matches!(
            out.frames[PENDING_TELEMETRY_CAP],
            StreamFrame::SegmentEnd(_)
        ));
    }

    /// A disconnected channel closes the segment with `InternalError` and reports
    /// it as a dropped segment — the socket is only kept because the closing frame
    /// went out whole.
    #[test]
    fn channel_disconnect_closes_with_internal_error_and_keeps_the_socket() {
        let ring = ring_with(0, 2_000_000);
        let (tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        drop(tx);
        let mut h = Harness::new();
        let poll = FakePoll::always_writable();

        let out = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        assert_eq!(out.exit, SegmentExit::SegmentDroppedSocketKept);
        match out.frames.as_slice() {
            [StreamFrame::SegmentEnd(e)] => assert_eq!(e.reason, EndReason::InternalError),
            other => panic!("expected a lone SegmentEnd(InternalError), got {other:?}"),
        }
    }

    // ── VAD close routing ─────────────────────────────────────────────────────

    /// The channel message is the primary close path (the flag is its backup under a
    /// TCP stall), so a `VadClosed` that arrives on the channel with the flag still
    /// clear must close the segment exactly as the flag does.
    #[test]
    fn a_channel_vad_close_ends_the_segment_like_the_flag() {
        let ring = ring_with(0, 2_000_000);
        let (tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        tx.send(StreamerMsg::VadClosed)
            .expect("channel accepts the close");
        let mut h = Harness::new();
        // Bounded so an ignored close arm fails on the fault instead of spinning
        // forever: the flag, the segment's other close path, is deliberately clear.
        let poll = FakePoll::faulting_after(4);

        let out = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        assert!(
            !h.vad_closed.load(Ordering::Acquire),
            "the flag stayed clear: the channel arm did the work"
        );
        assert_eq!(out.exit, SegmentExit::Completed);
        match out.frames.as_slice() {
            [StreamFrame::SegmentEnd(e)] => assert_eq!(e.reason, EndReason::VadRelease),
            other => panic!("expected a lone SegmentEnd(VadRelease), got {other:?}"),
        }
    }

    /// A re-onset during hangover reaches the streamer as `VadOpened`; the FSM owns
    /// that transition, so the segment loop must ignore it — neither closing early
    /// nor moving the read cursor to the new write head.
    #[test]
    fn a_re_onset_message_mid_segment_is_ignored() {
        let ring = ring_with(AUDIO_SAMPLES_PER_FRAME as u64, 2_000_000);
        let (tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        tx.send(StreamerMsg::VadOpened { write_head: 99_999 })
            .expect("channel accepts the re-onset");
        tx.send(StreamerMsg::VadClosed)
            .expect("channel accepts the close");
        let mut h = Harness::new();
        let poll = FakePoll::faulting_after(4);

        let out = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        assert_eq!(out.exit, SegmentExit::Completed);
        match out.frames.as_slice() {
            [StreamFrame::Audio(a), StreamFrame::SegmentEnd(e)] => {
                assert_eq!(
                    a.first_sample_index, 0,
                    "the re-onset must not move the read cursor"
                );
                assert_eq!(e.reason, EndReason::VadRelease);
                assert_eq!(e.frames_sent, 1);
            }
            other => panic!("expected one AudioFrame then SegmentEnd, got {other:?}"),
        }
    }

    // ── Write backpressure ────────────────────────────────────────────────────

    /// A blocked write reports itself once and arms POLLOUT for the next wake;
    /// the writable wake clears the block and the frame completes.
    #[test]
    fn a_blocked_write_reports_once_and_arms_pollout() {
        let ring = ring_with(0, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::new();
        h.vad_closed.store(true, Ordering::Release);
        // One refused write, then the retry takes the whole frame.
        h.link.write_script.push_back(0);
        let poll = FakePoll::always_writable();

        let out = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        assert_eq!(out.exit, SegmentExit::Completed);
        assert_eq!(out.frames.len(), 1, "the SegmentEnd resumed to completion");
        assert_eq!(
            out.events
                .iter()
                .filter(|e| **e == ObsEvent::WriteBlocked)
                .count(),
            1,
            "the first write-block reports once per segment"
        );
        assert_eq!(
            poll.write_arming(),
            vec![false, true],
            "POLLOUT is armed only on the wake that follows the block"
        );
    }

    /// A short write is resumed inside the same wake: the cursor advances and the
    /// loop retries immediately rather than going back through `poll`.
    #[test]
    fn a_short_write_resumes_without_an_extra_poll() {
        let ring = ring_with(0, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::new();
        h.vad_closed.store(true, Ordering::Release);
        h.link.write_script.push_back(5);
        let poll = FakePoll::always_writable();

        let out = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        assert_eq!(out.exit, SegmentExit::Completed);
        assert_eq!(out.frames.len(), 1);
        assert_eq!(out.partial, 0, "the frame completed byte-exactly");
        assert!(
            !out.events.contains(&ObsEvent::WriteBlocked),
            "a partial write is progress, not a block"
        );
        assert_eq!(
            poll.seen.borrow().len(),
            1,
            "the partial write resumed within the wake that started it"
        );
    }

    /// Poll insisting the socket is writable while every write refuses bytes is the
    /// spin-risk case: at `SPIN_GUARD_THRESHOLD` the loop de-arms POLLOUT for
    /// `SPIN_BACKOFF_MS` so the transport gets the CPU, then re-arms it.
    #[test]
    fn the_spin_guard_de_arms_pollout_for_one_backoff_then_re_arms() {
        let ring = ring_with(0, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::new();
        h.vad_closed.store(true, Ordering::Release);
        h.link.write_default = 0; // every write refuses bytes
        let poll = FakePoll::faulting_after(SPIN_GUARD_THRESHOLD + 4);
        // Frozen until the backoff has been observed once, then a jump past it. The
        // wake count is the only thing that advances this clock, so the backoff
        // expires on exactly one known wake.
        let base = Instant::now();
        let instants = || {
            if poll.wakes.get() > SPIN_GUARD_THRESHOLD {
                base + Duration::from_millis(SPIN_BACKOFF_MS * 2)
            } else {
                base
            }
        };

        let out = run_at(
            &mut h,
            &ring,
            &rx,
            &poll,
            &free_running_clock(),
            &instants,
            0,
        );

        assert_eq!(
            out.exit,
            SegmentExit::SocketLost,
            "the scripted fault is the only exit from a permanently blocked write"
        );
        assert!(out.frames.is_empty());
        let armed = poll.write_arming();
        let trip = SPIN_GUARD_THRESHOLD as usize;
        assert!(
            !armed[0],
            "nothing has blocked before the first write attempt"
        );
        for (wake, armed) in armed.iter().enumerate().take(trip).skip(1) {
            assert!(
                armed,
                "POLLOUT stays armed while the guard is untripped (wake {wake})"
            );
        }
        assert!(!armed[trip], "the tripped guard de-arms POLLOUT");
        assert!(
            poll.seen.borrow()[trip].2 <= Duration::from_millis(SPIN_BACKOFF_MS),
            "the backoff bounds the de-armed wake's sleep"
        );
        assert!(armed[trip + 1], "an expired backoff re-arms POLLOUT");
    }

    /// A write budget that elapses before any byte of the frame left leaves the
    /// stream frame-aligned: drop the segment, keep the socket.
    #[test]
    fn a_write_budget_elapsed_at_zero_bytes_keeps_the_socket() {
        let ring = ring_with(0, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::new();
        h.vad_closed.store(true, Ordering::Release);
        h.link.write_default = 0;
        // Bounded only so a watchdog that never fires fails instead of spinning; the
        // wake-count assertion below is what says the watchdog did the work.
        let poll = FakePoll::faulting_after(20);

        let out = run_at(
            &mut h,
            &ring,
            &rx,
            &poll,
            &free_running_clock(),
            &stepping_instants(Duration::from_millis(400)),
            0,
        );

        assert_eq!(out.exit, SegmentExit::SegmentDroppedSocketKept);
        assert!(out.frames.is_empty());
        assert_eq!(out.partial, 0, "no byte left, so the stream stays aligned");
        assert!(
            poll.seen.borrow().len() <= 5,
            "the budget, not the scripted fault, ended the segment"
        );
    }

    /// The same stall after a prefix was accepted is fatal: the receiver is mid-frame
    /// and the tail can never be delivered, so the socket goes too.
    #[test]
    fn a_write_budget_elapsed_mid_tail_clears_the_socket() {
        let ring = ring_with(0, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::new();
        h.vad_closed.store(true, Ordering::Release);
        h.link.write_script.push_back(5);
        h.link.write_default = 0;
        let poll = FakePoll::faulting_after(20);

        let out = run_at(
            &mut h,
            &ring,
            &rx,
            &poll,
            &free_running_clock(),
            &stepping_instants(Duration::from_millis(400)),
            0,
        );

        assert_eq!(out.exit, SegmentExit::SocketLost);
        assert!(out.frames.is_empty());
        assert_eq!(out.partial, 5, "the accepted prefix desynced the receiver");
        assert!(
            poll.seen.borrow().len() <= 5,
            "the budget, not the scripted fault, ended the segment"
        );
    }

    // ── Encode failures ───────────────────────────────────────────────────────

    /// A `SegmentEnd` that cannot be encoded leaves the receiver waiting on a segment
    /// that will never close, so the socket must go even though no byte was written.
    #[test]
    fn a_segment_end_that_cannot_encode_clears_the_socket() {
        let ring = ring_with(0, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::with_outbound_buf(2);
        h.vad_closed.store(true, Ordering::Release);
        let poll = FakePoll::faulting_after(8);

        let out = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        assert_eq!(out.exit, SegmentExit::SocketLost);
        assert!(
            h.link.written.is_empty(),
            "an encode fault writes nothing at all"
        );
        assert_eq!(
            poll.seen.borrow().len(),
            1,
            "the encode fault, not the scripted fault, ended the segment"
        );
    }

    /// A telemetry frame that cannot be encoded is a local fault at a frame boundary:
    /// drop the segment, keep the (still frame-aligned) socket.
    #[test]
    fn a_telemetry_frame_that_cannot_encode_keeps_the_socket() {
        let ring = ring_with(0, 2_000_000);
        let (tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        tx.send(StreamerMsg::Telemetry(WireTelemetry {
            device_ts_us: 1,
            kind: TelemetryKind::SpEnergy {
                values: [1.0, 0.0, 0.0, 0.0],
            },
        }))
        .expect("channel accepts telemetry");
        let mut h = Harness::with_outbound_buf(2);
        let poll = FakePoll::faulting_after(8);

        let out = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        assert_eq!(out.exit, SegmentExit::SegmentDroppedSocketKept);
        assert!(h.link.written.is_empty());
    }

    /// Same for an AudioFrame: nothing reached the wire, so the connection survives.
    #[test]
    fn an_audio_frame_that_cannot_encode_keeps_the_socket() {
        let ring = ring_with(AUDIO_SAMPLES_PER_FRAME as u64, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::with_outbound_buf(2);
        let poll = FakePoll::faulting_after(8);

        let out = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        assert_eq!(out.exit, SegmentExit::SegmentDroppedSocketKept);
        assert!(h.link.written.is_empty());
    }

    // ── Per-wake fairness caps ────────────────────────────────────────────────

    /// A backlog past `OUTBOUND_FRAMES_PER_WAKE` is drained one cap per wake, and the
    /// wake that stopped at the cap re-polls with a zero timeout instead of sleeping —
    /// the drain-until-blocked invariant that keeps the pre-roll ahead of the ring.
    #[test]
    fn the_outbound_cap_bounds_one_wake_and_re_polls_immediately() {
        let backlog = OUTBOUND_FRAMES_PER_WAKE as u64 + 1;
        let ring = ring_with(AUDIO_SAMPLES_PER_FRAME as u64 * backlog, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::new();
        // Fault on the second wake, so only the first wake's writes reached the wire.
        let poll = FakePoll::faulting_after(1);

        let out = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        assert_eq!(out.exit, SegmentExit::SocketLost);
        assert_eq!(
            out.frames.len(),
            OUTBOUND_FRAMES_PER_WAKE as usize,
            "one wake writes at most the per-wake cap"
        );
        assert!(
            out.frames
                .iter()
                .all(|f| matches!(f, StreamFrame::Audio(_)))
        );
        assert_eq!(
            poll.seen.borrow()[1].2,
            Duration::ZERO,
            "stopping at the cap re-polls immediately"
        );
    }

    // ── Inbound interleave ────────────────────────────────────────────────────

    /// Poll discipline rule 1: a transport that buffers decrypted plaintext
    /// under-reports readiness, so the loop must read on every wake — including one
    /// whose poll said nothing is readable.
    #[test]
    fn a_plaintext_buffering_link_is_read_on_a_wake_with_no_pollin() {
        let ring = ring_with(0, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::new();
        h.vad_closed.store(true, Ordering::Release);
        h.link.plaintext = true;
        h.link.queue_read(&wire_bytes(&inbound_hello()));
        h.link.queue_read(&wire_bytes(&inbound_audio(160)));
        let poll = FakePoll::always_writable(); // readable is false on every wake

        let out = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        assert_eq!(out.exit, SegmentExit::Completed);
        assert!(h.link.reads > 0, "the buffering link is read regardless");
        assert_eq!(
            h.sink.offers,
            vec![320],
            "the queued Audio frame reached the sink"
        );
        assert_eq!(h.inbound_obs.hellos, vec![DEVICE_PLAYBACK_FORMAT]);
    }

    /// The same script on a transport that buffers nothing is left alone: with no
    /// POLLIN and room in the accumulator there is nothing to read.
    #[test]
    fn a_non_buffering_link_is_not_read_without_pollin() {
        let ring = ring_with(0, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::new();
        h.vad_closed.store(true, Ordering::Release);
        h.link.queue_read(&wire_bytes(&inbound_hello()));
        h.link.queue_read(&wire_bytes(&inbound_audio(160)));
        let poll = FakePoll::always_writable();

        let out = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        assert_eq!(out.exit, SegmentExit::Completed);
        assert_eq!(h.link.reads, 0, "no POLLIN, room to spare, no read");
        assert!(h.sink.offers.is_empty());
    }

    /// A full accumulator de-arms POLLIN, and the `!inbound_armed` disjunct is what
    /// still runs the pump: the held frame is re-offered, which is the only way the
    /// sink's freed slot ever drains it and the TCP window reopens.
    #[test]
    fn a_full_accumulator_still_gets_its_held_frame_re_offered() {
        let ring = ring_with(0, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::new();
        h.vad_closed.store(true, Ordering::Release);

        // Fill the accumulator: a Hello, an Audio frame the sink refuses (held at the
        // head), then a length prefix declaring a frame far longer than what follows,
        // so the tail can never be consumed and the buffer stays full.
        h.sink.full = true;
        h.link.queue_read(&wire_bytes(&inbound_hello()));
        h.link.queue_read(&wire_bytes(&inbound_audio(160)));
        h.link.queue_read(&(MAX_FRAME_BYTES as u16).to_le_bytes());
        h.link.queue_read(&vec![0u8; 2 * (MAX_FRAME_BYTES + 2)]);
        for _ in 0..4 {
            if !inbound_has_room(&h.accum) {
                break;
            }
            pump_inbound(
                h.link.as_read(),
                &mut h.accum,
                &mut h.sink,
                &mut h.state,
                &mut h.inbound_obs,
                INBOUND_STEPS_PER_WAKE,
            )
            .expect("the prefill script is well-formed");
        }
        assert!(
            !inbound_has_room(&h.accum),
            "the prefill must leave the accumulator full"
        );
        // The peer sends nothing more, and the slot the sink was missing frees up.
        h.link.read_queue.clear();
        h.link.reads = 0;
        h.sink.full = false;
        h.sink.offers.clear();
        h.inbound_obs.waypoints.clear();
        let poll = FakePoll::always_writable();

        let out = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        assert_eq!(out.exit, SegmentExit::Completed);
        assert_eq!(
            h.sink.offers,
            vec![320],
            "the held frame was re-offered and accepted"
        );
        assert_eq!(
            h.inbound_obs.waypoints,
            vec![(InboundWaypoint::Periodic, 1)],
            "the accepted frame is the connection's first"
        );
        assert!(
            !poll.seen.borrow()[0].1.read,
            "a full accumulator de-arms POLLIN"
        );
    }

    /// An inbound flood that stops at the per-wake step cap reports work remaining,
    /// which turns the next poll into an immediate re-check.
    #[test]
    fn an_inbound_flood_past_the_per_wake_cap_re_polls_immediately() {
        let ring = ring_with(0, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::new();
        let audio = wire_bytes(&inbound_audio(160));
        // One frame per read, so each pump step is one drain that makes progress.
        h.link.read_chunk = audio.len();
        h.link.queue_read(&wire_bytes(&inbound_hello()));
        for _ in 0..INBOUND_STEPS_PER_WAKE + 1 {
            h.link.queue_read(&audio);
        }
        let poll = FakePoll::readable_faulting_after(1);

        let out = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        assert_eq!(out.exit, SegmentExit::SocketLost);
        assert_eq!(
            h.link.reads, INBOUND_STEPS_PER_WAKE,
            "the pump stops at the per-wake cap"
        );
        assert_eq!(
            poll.seen.borrow()[1].2,
            Duration::ZERO,
            "inbound work remaining re-polls immediately"
        );
    }

    /// A protocol fault on the inbound stream ends the connection, and the teardown
    /// sample is taken on the way out — the engine's use of the inbound observer seam.
    #[test]
    fn an_inbound_protocol_fault_clears_the_socket_and_samples_the_exit() {
        let ring = ring_with(0, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::new();
        h.link.queue_read(&wire_bytes(&inbound_hello()));
        for _ in 0..3 {
            h.link.queue_read(&wire_bytes(&inbound_audio(160)));
        }
        // A length prefix past MAX_FRAME_BYTES is unrecoverable framing.
        h.link
            .queue_read(&(MAX_FRAME_BYTES as u16 + 1).to_le_bytes());
        let poll = FakePoll::readable_faulting_after(8);

        let out = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        assert_eq!(out.exit, SegmentExit::SocketLost);
        assert_eq!(
            poll.seen.borrow().len(),
            1,
            "the inbound fault, not the scripted poll fault, ended the segment"
        );
        assert_eq!(h.sink.offers.len(), 3);
        assert_eq!(
            h.inbound_obs.waypoints,
            vec![(InboundWaypoint::Periodic, 1), (InboundWaypoint::Exit, 3)],
            "the exit sample carries the connection's accepted-frame count"
        );
    }

    /// A fault before any Hello never entered the post-Hello window, so the teardown
    /// sample is muted — the gate lives in the shared helper, but the engine must be
    /// the one calling it.
    #[test]
    fn an_inbound_fault_before_hello_samples_no_exit() {
        let ring = ring_with(0, 2_000_000);
        let (_tx, rx) = std::sync::mpsc::channel::<StreamerMsg>();
        let mut h = Harness::new();
        h.link.queue_read(&wire_bytes(&inbound_audio(160)));
        let poll = FakePoll::readable_faulting_after(8);

        let out = run(&mut h, &ring, &rx, &poll, &free_running_clock(), 0);

        assert_eq!(out.exit, SegmentExit::SocketLost);
        assert_eq!(
            poll.seen.borrow().len(),
            1,
            "the inbound fault, not the scripted poll fault, ended the segment"
        );
        assert!(h.sink.offers.is_empty(), "Audio before Hello is refused");
        assert!(
            h.inbound_obs.waypoints.is_empty(),
            "a pre-Hello fault takes no exit sample"
        );
    }

    /// `ObsEvent`'s log tokens are a stable grep vocabulary; pin them.
    #[test]
    fn obs_event_tokens_are_stable() {
        assert_eq!(ObsEvent::SegmentOpen.as_str(), "start");
        assert_eq!(ObsEvent::PrerollDrained.as_str(), "preroll-drained");
        assert_eq!(ObsEvent::WriteBlocked.as_str(), "write-blocked");
        assert_eq!(ObsEvent::Production.as_str(), "prod");
        assert_eq!(
            ObsEvent::TelemetryDropped { cap: 8 }.as_str(),
            "telemetry-dropped"
        );
    }
}
