//! The streamer thread: its platform seam, its connection maintenance, and the
//! loop that spends both.
//!
//! A pod's streamer thread differs from another pod's in what a connection *is*
//! (esp-tls on the ESP32, openssl on Linux), how it asks whether the network link
//! is up, how it describes that link when a connect fails, and which clock and
//! `poll` syscall it spends. [`StreamerPlatform`] is those things and nothing
//! else.
//!
//! Everything else about holding a connection open — when a tick may attempt a
//! connect, what a fresh socket obliges, how a failure paces the next attempt, and
//! the `Hello` that identifies the pod — is the same on every pod and lives here,
//! over the state in [`LinkState`]. The decision rules themselves are
//! [`crate::idle`]'s; this module is where they are spent.
//!
//! [`run_streamer_loop`] is the whole idle/segment cycle: maintain the
//! connection, wait for readiness or a VAD onset, drain inbound playback, and on
//! an onset open a segment and hand it to [`crate::segment::run_segment`]. A pod
//! reaches it with a [`StreamerRuntime`] holding the capture ring, the telemetry
//! channel, the playback sink and its clocks; nothing above it is shared, because
//! everything above it is how the platform got those things (NVS or a config
//! file, a FreeRTOS task or a `std` thread).

use std::fmt::Display;
use std::io;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, TryRecvError};
use std::time::Instant;

use audio_pipeline::inbound::{
    FrameAccumulator, InboundConnectionState, InboundObserver, inbound_has_room, note_inbound_exit,
    pump_inbound,
};
use audio_pipeline::playback::PlaybackSink;
use audio_pipeline::ring::{CaptureRing, PREROLL_SAMPLES, RingIndex};
use audio_pipeline::stream_send::SendOutcome;
use audio_pipeline::wire::{
    AUDIO_PROTOCOL_VERSION, ChannelSource, DEVICE_PLAYBACK_FORMAT, Hello, MAX_FRAME_BYTES,
    StreamFrame,
};
use wifi_reconnect::Backoff;

use crate::idle::{
    IdleConnectAction, SegmentOnset, arm_reconnect_deadline, note_connect_success,
    note_socket_established, note_socket_lost, plan_segment_start, should_attempt_idle_connect,
};
use crate::link::LinkStream;
use crate::netpoll::{IDLE_TICK, INBOUND_STEPS_PER_WAKE, NetPoll, Readiness, poll_timeout};
use crate::segment::{ObsEvent, SegmentDeps, SegmentExit, StreamerMsg, run_segment, send_frame_bp};

// ── The platform seam ─────────────────────────────────────────────────────────

/// What the streamer loop needs from the pod it runs on.
///
/// Every method is a query, not a policy: the implementation answers for the
/// hardware and answers cheaply, because the loop calls [`link_up`] and
/// [`now_secs`] on every idle tick. Nothing here is allowed to block for longer
/// than a connect attempt.
///
/// [`link_up`]: StreamerPlatform::link_up
/// [`now_secs`]: StreamerPlatform::now_secs
pub trait StreamerPlatform {
    /// This platform's connected byte transport.
    type Link: LinkStream;

    /// This pod's identity — both the `Hello` field and the TLS PSK identity.
    fn pod_id(&self) -> &str;

    /// The audio host this pod streams to. Used for log context; the address the
    /// connection is actually made to is [`connect`](StreamerPlatform::connect)'s
    /// business.
    fn peer(&self) -> SocketAddr;

    /// Open one connection to the audio host: TCP connect plus whatever
    /// handshake the transport needs, returning a non-blocking stream.
    ///
    /// The wire-level `Hello` is *not* sent here — [`connect_and_hello`] sends it,
    /// so both pods introduce themselves identically.
    fn connect(&self) -> io::Result<Self::Link>;

    /// Whether the network link is up: `Some(false)` for down and `None` for
    /// "this platform cannot say cheaply". Both are treated as down, and neither
    /// charges a reconnect backoff — link recovery is not an audio-host failure.
    ///
    /// `None` must be *transient* — "cannot say cheaply right now", as with a
    /// contended mutex or a radio stack mid-init. A platform with no cheap link
    /// query at all answers `Some(true)` and lets a dead carrier surface as a
    /// paced connect failure; `psk_link::LinkPlatform::link_up` is the exemplar.
    /// Answering a permanent `None` compiles and passes every host test while
    /// failing the idle-connect gate forever ([`ensure_connected`],
    /// [`crate::idle::should_attempt_idle_connect`]): the pod never opens an idle
    /// connection and never reconnects after one is lost, so it silently loses
    /// the first utterance after every disconnect.
    fn link_up(&self) -> Option<bool>;

    /// One line of link diagnostics for a failed connect's log entry. Called only
    /// on the failure path, so it may cost more than the per-tick queries.
    fn link_diag(&self) -> impl Display;

    /// Monotonic seconds — the currency reconnect deadlines are drawn in.
    fn now_secs(&self) -> u64;

    /// This platform's `poll()` shim.
    fn poll(&self) -> &dyn NetPoll;
}

// ── Connection state ─────────────────────────────────────────────────────────

/// Everything the streamer thread carries across segments about its connection.
///
/// One struct rather than a fistful of locals so the shared entry points stay
/// well inside the Xtensa realign-miscompile guard's argument-word budget, and so
/// a socket replacement cannot forget one of the three things it must reset.
///
/// The playback sink is deliberately *not* a field: it is owned by the platform's
/// audio output and is passed to the calls that must signal a stream boundary on
/// it.
///
/// The fields are crate-private: the pacing four have to move together, and
/// [`arm_backoff`](Self::arm_backoff) / [`note_connect_success`](Self::note_connect_success)
/// are the pairings that keep them coherent.
pub struct LinkState<L> {
    /// The live connection, or `None` while disconnected.
    pub(crate) held_socket: Option<L>,
    /// Inbound reassembly buffer for the held connection.
    pub(crate) inbound_accum: FrameAccumulator,
    /// Per-connection inbound framing state.
    pub(crate) inbound_state: InboundConnectionState,
    /// Outbound encode scratch, sized for a full framed frame. Thread-lifetime,
    /// so no call path allocates for encoding.
    pub(crate) encode_buf: Vec<u8>,
    /// Reconnect backoff, reset by every successful connect.
    pub(crate) backoff: Backoff,
    /// Monotonic second before which no connect is attempted. Zero means "now".
    pub(crate) reconnect_deadline_secs: u64,
    /// Consecutive failed attempts, folded into the backoff jitter seed.
    pub(crate) attempt_counter: u32,
    /// Per-pod jitter seed, so a fleet's pods do not converge on one retry beat.
    pub(crate) jitter_seed_base: u32,
}

impl<L> LinkState<L> {
    /// Disconnected, with the first connect due immediately.
    pub fn new(jitter_seed_base: u32) -> Self {
        Self {
            held_socket: None,
            inbound_accum: FrameAccumulator::new(),
            inbound_state: InboundConnectionState::new(),
            encode_buf: vec![0u8; MAX_FRAME_BYTES + 2],
            backoff: Backoff::new(),
            reconnect_deadline_secs: 0,
            attempt_counter: 0,
            jitter_seed_base,
        }
    }

    /// Install a fresh connection: see [`note_socket_established`].
    pub fn note_socket_established(&mut self, link: L, sink: &mut dyn PlaybackSink) {
        note_socket_established(
            &mut self.held_socket,
            link,
            &mut self.inbound_accum,
            &mut self.inbound_state,
            sink,
        );
    }

    /// Tear the connection down: see [`note_socket_lost`].
    pub fn note_socket_lost(&mut self, sink: &mut dyn PlaybackSink) {
        note_socket_lost(
            &mut self.held_socket,
            &mut self.inbound_accum,
            &mut self.inbound_state,
            sink,
        );
    }

    /// Charge a failed attempt to the backoff and arm the next deadline.
    pub fn arm_backoff(&mut self, now_secs: u64) {
        self.reconnect_deadline_secs = arm_reconnect_deadline(
            now_secs,
            &mut self.backoff,
            &mut self.attempt_counter,
            self.jitter_seed_base,
        );
    }

    /// Clear the backoff after a connect that worked.
    pub fn note_connect_success(&mut self) {
        note_connect_success(&mut self.backoff, &mut self.reconnect_deadline_secs);
    }
}

// ── Connect ───────────────────────────────────────────────────────────────────

/// Open a connection and introduce this pod with a `Hello`.
///
/// Returns the ready session, or an error if connect, handshake, or `Hello`
/// fails. The `Hello` goes out through the same bounded-backpressure path as
/// every other frame — the socket is non-blocking from the handoff on — and a
/// `Hello` that stalls there is treated as a failed connect: a host too backed up
/// to take 30 bytes of handshake will not keep up with a segment, so the fresh
/// connection is dropped rather than carried into one.
pub fn connect_and_hello<P: StreamerPlatform>(
    platform: &P,
    encode_buf: &mut [u8],
) -> io::Result<P::Link> {
    let mut link = platform.connect()?;
    let hello = StreamFrame::Hello(Hello {
        version: AUDIO_PROTOCOL_VERSION,
        // An id too long for the wire field yields an empty one rather than a
        // truncated one: a wrong id must not be able to look like a valid one.
        pod_id: heapless::String::try_from(platform.pod_id()).unwrap_or_default(),
        sample_rate_hz: DEVICE_PLAYBACK_FORMAT.sample_rate_hz,
        bits_per_sample: DEVICE_PLAYBACK_FORMAT.bits_per_sample,
        channels: DEVICE_PLAYBACK_FORMAT.channels,
        codec: DEVICE_PLAYBACK_FORMAT.codec,
        channel_source: ChannelSource::CommunicationBeam,
    });
    match send_frame_bp(platform.poll(), &mut link, &hello, encode_buf)? {
        SendOutcome::Sent => Ok(link),
        SendOutcome::BackpressureAligned => Err(io::Error::other(
            "Hello stalled on write backpressure — dropping the fresh connection",
        )),
    }
}

/// Idle-tick connection maintenance. Run once per streamer loop iteration.
///
/// No-op when the socket is already up. A down link skips silently, charging no
/// backoff — radio recovery is the platform's business, not an audio-host
/// failure. A failed connect arms the next deadline and returns; the caller keeps
/// idling.
pub fn ensure_connected<P: StreamerPlatform>(
    platform: &P,
    state: &mut LinkState<P::Link>,
    sink: &mut dyn PlaybackSink,
) {
    if state.held_socket.is_some() {
        return;
    }

    let now = platform.now_secs();
    if should_attempt_idle_connect(
        false,
        platform.link_up(),
        now,
        state.reconnect_deadline_secs,
    ) == IdleConnectAction::Skip
    {
        return;
    }

    match connect_and_hello(platform, &mut state.encode_buf) {
        Ok(link) => {
            state.note_socket_established(link, sink);
            state.note_connect_success();
            log::info!("streamer: idle connect established to {}", platform.peer());
        }
        Err(e) => {
            log::warn!(
                "streamer: idle connect/Hello failed: dst={} {} err={:?} — backing off",
                platform.peer(),
                platform.link_diag(),
                e
            );
            state.arm_backoff(now);
        }
    }
}

// ── The streamer loop ─────────────────────────────────────────────────────────

/// The pod-owned things the streamer loop spends, gathered so the loop itself
/// takes two argument words on the Xtensa windowed ABI.
///
/// Each field is something a platform builds before the loop starts and the loop
/// only borrows: the capture ring the capture thread fills, the channel the
/// telemetry thread publishes on, the audio output, and the clocks and
/// observability sinks. The loop's own state — the connection, the segment
/// counter, the encode scratch — is not here; it is created inside
/// [`run_streamer_loop`] and lives exactly as long as the loop does.
pub struct StreamerRuntime<'a, B> {
    /// Telemetry/VAD → streamer channel. Its sender going away ends the loop.
    pub rx: &'a Receiver<StreamerMsg>,
    /// The capture ring the capture thread writes.
    pub ring: &'a Mutex<Option<CaptureRing<B>>>,
    /// Ring index geometry, matching `ring`'s capacity.
    pub ridx: &'a RingIndex,
    /// Lossless VAD-closed flag, set by the telemetry thread on a close whose
    /// channel message may have been dropped.
    pub vad_closed_flag: &'a AtomicBool,
    /// Published `true` for the span of an onset, from acceptance through segment
    /// teardown. Pure observation — it gates no control flow here, and a platform
    /// with nothing watching it simply never reads it.
    pub segment_active_flag: &'a AtomicBool,
    /// Where decoded inbound playback PCM goes.
    pub inbound_sink: &'a mut dyn PlaybackSink,
    /// Platform monotonic microseconds — the wire's `device_ts_us` source.
    pub now_us: &'a dyn Fn() -> u64,
    /// The same clock as an [`Instant`], for deadline arithmetic.
    pub now_instant: &'a dyn Fn() -> Instant,
    /// Where the running segment's [`ObsEvent`]s go, tagged with the segment id
    /// they belong to (the loop owns the id, so the observer never tracks it).
    pub obs: &'a mut dyn FnMut(u32, ObsEvent),
    /// Observer for the shared inbound path, on the idle drain as well as inside
    /// a segment.
    pub inbound_obs: &'a mut dyn InboundObserver,
    /// Per-pod reconnect jitter seed, so a fleet's pods do not converge on one
    /// retry beat.
    pub jitter_seed: u32,
}

/// Why [`run_streamer_loop`] returned. It has no success exit: the loop runs for
/// the life of the pod.
#[derive(Debug, PartialEq, Eq)]
pub enum StreamerExit {
    /// The telemetry thread's sender was dropped, so no onset can ever arrive
    /// again. The caller lets its thread end.
    ChannelDisconnected,
}

/// Publishes the onset span on a flag for its lifetime: set on construction,
/// cleared on drop — so every drop path out of an onset clears it, including the
/// ones that never reach a segment.
struct SegmentActiveGuard<'a>(&'a AtomicBool);

impl<'a> SegmentActiveGuard<'a> {
    fn new(flag: &'a AtomicBool) -> Self {
        flag.store(true, Ordering::Release);
        SegmentActiveGuard(flag)
    }
}

impl Drop for SegmentActiveGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Run the streamer until its telemetry channel goes away.
///
/// One iteration is one idle tick: maintain the connection, wait for socket
/// readiness or a channel message, drain whatever inbound playback arrived, and —
/// if the message was a VAD onset — open a segment and run it. Every failure
/// returns here rather than unwinding the thread: a segment that cannot be
/// started or finished is dropped (real-time-or-drop; there is no queue to catch
/// up from), and a socket that cannot be trusted is torn down and reconnected on
/// a paced retry.
///
/// The `Hello`, the pre-roll placement, the drain discipline and the reconnect
/// pacing are all shared code, so two pods reaching this function behave
/// identically from here on.
pub fn run_streamer_loop<P, B>(platform: &P, rt: &mut StreamerRuntime<'_, B>) -> StreamerExit
where
    P: StreamerPlatform,
    B: Deref<Target = [i16]>,
{
    let mut state: LinkState<P::Link> = LinkState::new(rt.jitter_seed);
    let mut segment_counter: u32 = 0;
    // Carries "the idle inbound pump stopped at its cap" into the next idle poll
    // so a backlog drains with timeout-0 re-polls instead of one frame per tick.
    let mut idle_work_pending = false;

    'outer: loop {
        ensure_connected(platform, &mut state, &mut *rt.inbound_sink);

        // ── Idle readiness wait ──────────────────────────────────────────────
        // POLLIN de-armed while the accumulator is full (backpressure) to avoid
        // spinning.
        let inbound_armed = state.held_socket.is_some() && inbound_has_room(&state.inbound_accum);

        let mut readable = false;
        let onset = if let Some((fd, interest)) = state
            .held_socket
            .as_ref()
            .map(|s| (s.link_fd(), s.poll_interest(inbound_armed, false)))
        {
            let timeout = poll_timeout((rt.now_instant)(), None, idle_work_pending);
            match platform.poll().readiness(fd, interest, timeout) {
                Readiness::Fault(e) => {
                    log::warn!(
                        "streamer: idle poll fault — clearing socket, backing off: {:?}",
                        e
                    );
                    state.note_socket_lost(&mut *rt.inbound_sink);
                    state.arm_backoff(platform.now_secs());
                    continue 'outer;
                }
                ready => {
                    readable = ready.readable();
                }
            }
            match rt.rx.try_recv() {
                Ok(StreamerMsg::VadOpened { write_head }) => Some(write_head),
                Ok(StreamerMsg::VadClosed) | Ok(StreamerMsg::Telemetry(_)) => {
                    None // stale before segment open
                }
                Err(TryRecvError::Empty) => None,
                Err(TryRecvError::Disconnected) => {
                    log::error!("streamer: channel disconnected; streamer thread exiting");
                    return StreamerExit::ChannelDisconnected;
                }
            }
        } else {
            // No socket → no fd to poll; block on the channel for IDLE_TICK.
            match rt.rx.recv_timeout(IDLE_TICK) {
                Ok(StreamerMsg::VadOpened { write_head }) => Some(write_head),
                Ok(StreamerMsg::VadClosed) | Ok(StreamerMsg::Telemetry(_)) => None,
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => {
                    log::error!("streamer: channel disconnected; streamer thread exiting");
                    return StreamerExit::ChannelDisconnected;
                }
            }
        };

        // ── Idle inbound drain ───────────────────────────────────────────────
        // Pumps until the socket blocks (or the cap) rather than one frame per
        // tick — the common TTS-playback path. Also runs when POLLIN is de-armed
        // — re-offers the held frame so a freed slot re-arms POLLIN next
        // iteration. Poll discipline rule 1: a TLS session can hold decrypted
        // plaintext that no POLLIN will ever reveal, so on that transport a read
        // is attempted every wake rather than only on readiness.
        // `idle_work_pending` carries a cap-stopped pump into the next wake for
        // the same reason.
        let must_drain = readable
            || !inbound_armed
            || idle_work_pending
            || state
                .held_socket
                .as_ref()
                .is_some_and(|s| s.buffers_plaintext());
        idle_work_pending = false;
        if must_drain && let Some(ref mut s) = state.held_socket {
            match pump_inbound(
                s.as_read(),
                &mut state.inbound_accum,
                &mut *rt.inbound_sink,
                &mut state.inbound_state,
                &mut *rt.inbound_obs,
                INBOUND_STEPS_PER_WAKE,
            ) {
                Ok(p) => idle_work_pending = p.hit_cap,
                Err(e) => {
                    log::warn!(
                        "streamer: idle inbound drain error — clearing socket, backing off: {:?}",
                        e
                    );
                    // Blind-window coverage: a post-Hello idle-drain exit is
                    // inside the post-Hello window too.
                    note_inbound_exit(&mut *rt.inbound_obs, &state.inbound_state);
                    // Stale partial bytes would corrupt the next connection's
                    // first frame.
                    state.note_socket_lost(&mut *rt.inbound_sink);
                    state.arm_backoff(platform.now_secs());
                }
            }
        }

        let vad_write_head = match onset {
            Some(wh) => wh,
            None => continue 'outer,
        };

        // Publish the onset span so a platform's quiesce check can wait for it to
        // end before borrowing the ring. Clears at the end of this iteration,
        // covering every reconnect-drop path and the segment return.
        let _segment_active = SegmentActiveGuard::new(rt.segment_active_flag);

        // ── Ensure a connection (real-time-or-drop) ──────────────────────────
        // An onset connects even when the idle gate would not have: a paced
        // reconnect is for an idle pod, not for an utterance in progress.
        // `fresh_connect` gates the reconnect path below — no point retrying on a
        // socket we just opened.
        let mut fresh_connect = false;
        if state.held_socket.is_none() {
            log::info!("streamer: connecting to {}", platform.peer());
            match connect_and_hello(platform, &mut state.encode_buf) {
                Ok(stream) => {
                    // Inbound state is already clean when `held_socket` is None;
                    // the redundant reset preserves the "fresh socket = fresh
                    // inbound stream" invariant by construction.
                    state.note_socket_established(stream, &mut *rt.inbound_sink);
                    fresh_connect = true;
                    state.note_connect_success();
                }
                Err(e) => {
                    log::warn!(
                        "streamer: connect/Hello failed: dst={} {} err={:?} — dropping segment",
                        platform.peer(),
                        platform.link_diag(),
                        e
                    );
                    continue 'outer;
                }
            }
        }

        // ── Send SegmentStart ────────────────────────────────────────────────
        let segment_id = segment_counter;
        segment_counter = segment_counter.wrapping_add(1);

        let plan = {
            let guard = rt
                .ring
                .lock()
                .unwrap_or_else(|_| panic!("capture ring mutex poisoned in streamer"));
            let ring = guard.as_ref().expect("capture ring not initialized");
            plan_segment_start(
                ring,
                rt.ridx,
                &SegmentOnset {
                    segment_id,
                    vad_write_head,
                    preroll_samples: PREROLL_SAMPLES,
                },
            )
        };
        let cursor = plan.cursor;
        let preroll_count = plan.start.preroll_samples;

        let seg_start = StreamFrame::SegmentStart(plan.start);

        let seg_start_err = match send_frame_bp(
            platform.poll(),
            state.held_socket.as_mut().expect("socket is connected"),
            &seg_start,
            &mut state.encode_buf,
        ) {
            Ok(SendOutcome::Sent) => None,
            Ok(SendOutcome::BackpressureAligned) => {
                log::warn!(
                    "streamer: SegmentStart backpressure (seg {}) — dropping segment, keeping socket",
                    segment_id
                );
                continue 'outer;
            }
            Err(e) => Some(e),
        };
        if let Some(e) = seg_start_err {
            state.note_socket_lost(&mut *rt.inbound_sink);
            if fresh_connect {
                log::warn!(
                    "streamer: SegmentStart send failed on fresh connect: dst={} {} err={:?} — dropping segment",
                    platform.peer(),
                    platform.link_diag(),
                    e
                );
                continue 'outer;
            }
            log::warn!(
                "streamer: SegmentStart send failed on held socket: dst={} {} err={:?} — one reconnect attempt",
                platform.peer(),
                platform.link_diag(),
                e
            );
            match connect_and_hello(platform, &mut state.encode_buf) {
                Ok(mut stream) => {
                    let resend = send_frame_bp(
                        platform.poll(),
                        &mut stream,
                        &seg_start,
                        &mut state.encode_buf,
                    );
                    match resend {
                        Ok(outcome @ (SendOutcome::Sent | SendOutcome::BackpressureAligned)) => {
                            state.note_socket_established(stream, &mut *rt.inbound_sink);
                            state.note_connect_success();
                            if matches!(outcome, SendOutcome::BackpressureAligned) {
                                log::warn!(
                                    "streamer: SegmentStart re-send backpressure after reconnect (seg {}) — dropping segment, keeping socket",
                                    segment_id
                                );
                                continue 'outer;
                            }
                        }
                        Err(_) => {
                            log::warn!(
                                "streamer: SegmentStart re-send failed after reconnect: dst={} {} outcome={:?} — dropping segment",
                                platform.peer(),
                                platform.link_diag(),
                                resend
                            );
                            continue 'outer;
                        }
                    }
                }
                Err(e) => {
                    log::warn!(
                        "streamer: reconnect failed (SegmentStart): dst={} {} err={:?} — dropping segment",
                        platform.peer(),
                        platform.link_diag(),
                        e
                    );
                    continue 'outer;
                }
            }
        }

        log::info!(
            "streamer: segment {} started cursor={} preroll={}",
            segment_id,
            cursor,
            preroll_count
        );

        // ── Run the shared segment loop ──────────────────────────────────────
        let exit = {
            let seg_obs = &mut *rt.obs;
            let mut obs = |event: ObsEvent| seg_obs(segment_id, event);
            let mut deps = SegmentDeps {
                socket: state.held_socket.as_mut().expect("socket is connected"),
                rx: rt.rx,
                ring: rt.ring,
                vad_closed_flag: rt.vad_closed_flag,
                ridx: rt.ridx,
                inbound_accum: &mut state.inbound_accum,
                inbound_sink: &mut *rt.inbound_sink,
                inbound_state: &mut state.inbound_state,
                outbound_buf: &mut state.encode_buf,
                poll: platform.poll(),
                now_us: rt.now_us,
                now_instant: rt.now_instant,
                obs: &mut obs,
                inbound_obs: &mut *rt.inbound_obs,
            };
            run_segment(&mut deps, segment_id, cursor)
        };
        if exit == SegmentExit::SocketLost {
            // No backoff on a mid-segment loss: the next idle tick reconnects
            // immediately.
            state.note_socket_lost(&mut *rt.inbound_sink);
        }

        // Per-segment state drops here; stale mid-frame tails never carry over.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;
    use std::rc::Rc;
    use std::sync::mpsc::{Sender, channel};
    use std::time::Duration;

    use audio_pipeline::wire::{AUDIO_SAMPLES_PER_FRAME, EndReason, decode_frame};
    use wifi_reconnect::BACKOFF_FLOOR_SECS;

    use crate::link::PollInterest;
    use crate::test_support::{
        FakeLink, FakePoll, RecordingInboundObs, RecordingSink, SinkMark, Wire, decode_stream,
        dirty_inbound, free_running_clock, inbound_audio, inbound_hello, ring_with, tapped_link,
        wire_bytes,
    };
    use audio_pipeline::inbound::InboundWaypoint;

    /// A platform whose connects are scripted and whose queries are settable, so
    /// a test states the situation and then asserts what was attempted.
    struct FakePlatform {
        connects: RefCell<VecDeque<io::Result<FakeLink>>>,
        attempts: Cell<u32>,
        /// The identity this pod introduces itself with.
        pod_id: String,
        link_up: Cell<Option<bool>>,
        now_secs: Cell<u64>,
        /// Shared so a test can both hand it to the platform and read back what
        /// the loop armed on it.
        poll: Rc<dyn NetPoll>,
        /// The onset-span flag, when a test asked `connect` to sample it.
        watched_span: Option<Rc<AtomicBool>>,
        /// Whether the onset span was published at each `connect`, in order.
        span_at_connect: RefCell<Vec<bool>>,
    }

    impl FakePlatform {
        /// Link up, clock at zero, and `connects` handed out in order. A connect
        /// past the end of the script is a test bug, not a failure to report.
        fn with(connects: Vec<io::Result<FakeLink>>) -> Self {
            Self::with_poll(connects, Rc::new(FakePoll::always_writable()))
        }

        /// [`with`](FakePlatform::with) against a caller-supplied `poll` shim.
        fn with_poll(connects: Vec<io::Result<FakeLink>>, poll: Rc<dyn NetPoll>) -> Self {
            Self {
                connects: RefCell::new(connects.into()),
                attempts: Cell::new(0),
                pod_id: "pod-aabbcc".to_string(),
                link_up: Cell::new(Some(true)),
                now_secs: Cell::new(0),
                poll,
                watched_span: None,
                span_at_connect: RefCell::new(Vec::new()),
            }
        }

        /// A platform that must never be asked to connect.
        fn unconnectable() -> Self {
            Self::with(Vec::new())
        }

        /// This pod introduces itself as `pod_id`.
        fn with_pod_id(mut self, pod_id: &str) -> Self {
            self.pod_id = pod_id.to_string();
            self
        }

        /// Sample `flag` on every `connect`, so a test can assert *when* in the
        /// loop's cycle the connect happened rather than only that it happened.
        fn watching_span(mut self, flag: Rc<AtomicBool>) -> Self {
            self.watched_span = Some(flag);
            self
        }
    }

    impl StreamerPlatform for FakePlatform {
        type Link = FakeLink;

        fn pod_id(&self) -> &str {
            &self.pod_id
        }

        fn peer(&self) -> SocketAddr {
            SocketAddr::from(([10, 1, 2, 3], 9000))
        }

        fn connect(&self) -> io::Result<FakeLink> {
            self.attempts.set(self.attempts.get() + 1);
            if let Some(flag) = &self.watched_span {
                self.span_at_connect
                    .borrow_mut()
                    .push(flag.load(Ordering::Acquire));
            }
            self.connects
                .borrow_mut()
                .pop_front()
                .expect("unscripted connect attempt")
        }

        fn link_up(&self) -> Option<bool> {
            self.link_up.get()
        }

        fn link_diag(&self) -> impl Display {
            "link=up ip=10.1.2.9"
        }

        fn now_secs(&self) -> u64 {
            self.now_secs.get()
        }

        fn poll(&self) -> &dyn NetPoll {
            &*self.poll
        }
    }

    /// A link that fails every write outright — a dead socket, not a stalled one.
    fn write_erroring_link() -> FakeLink {
        FakeLink {
            write_default: 0,
            ..FakeLink::new()
        }
    }

    /// The single `Hello` the socket was introduced with.
    fn sent_hello(link: &FakeLink) -> Hello {
        let len = u16::from_le_bytes([link.written[0], link.written[1]]) as usize;
        assert_eq!(
            link.written.len(),
            len + 2,
            "the fresh connection carried more than the Hello"
        );
        match decode_frame(&link.written).expect("the Hello frame decodes") {
            StreamFrame::Hello(h) => h,
            other => panic!("first frame on a fresh connection was {other:?}"),
        }
    }

    // ── The idle gate ─────────────────────────────────────────────────────

    /// A held socket is left strictly alone: no connect, no boundary signal.
    #[test]
    fn a_held_socket_is_never_reconnected() {
        let platform = FakePlatform::unconnectable();
        let mut state = LinkState::new(0x11);
        state.held_socket = Some(FakeLink::new());
        let mut sink = RecordingSink::new();

        ensure_connected(&platform, &mut state, &mut sink);

        assert_eq!(platform.attempts.get(), 0);
        assert!(sink.marks.is_empty(), "a held socket is not a new stream");
    }

    /// A down or unknown link skips without charging backoff — the deadline and
    /// the backoff must be exactly as they were, or a radio outage would pace the
    /// first post-outage connect.
    #[test]
    fn a_down_or_unknown_link_never_attempts_and_never_charges_backoff() {
        for link in [Some(false), None] {
            let platform = FakePlatform::unconnectable();
            platform.link_up.set(link);
            platform.now_secs.set(5_000);
            let mut state = LinkState::<FakeLink>::new(0x11);
            let mut sink = RecordingSink::new();

            ensure_connected(&platform, &mut state, &mut sink);

            assert_eq!(platform.attempts.get(), 0, "link={link:?}");
            assert_eq!(
                state.reconnect_deadline_secs, 0,
                "link={link:?}: a down link must not arm a deadline"
            );
            assert_eq!(
                state.attempt_counter, 0,
                "link={link:?}: a down link is not a failed attempt"
            );
            assert_eq!(state.backoff.current_secs(), BACKOFF_FLOOR_SECS);
        }
    }

    /// An armed backoff deadline defers the attempt until the clock reaches it.
    #[test]
    fn an_armed_deadline_defers_the_attempt_until_it_elapses() {
        let platform = FakePlatform::with(vec![Ok(FakeLink::new())]);
        let mut state = LinkState::<FakeLink>::new(0x11);
        state.reconnect_deadline_secs = 100;
        let mut sink = RecordingSink::new();

        platform.now_secs.set(99);
        ensure_connected(&platform, &mut state, &mut sink);
        assert_eq!(platform.attempts.get(), 0, "99 < 100 must not attempt");

        platform.now_secs.set(100);
        ensure_connected(&platform, &mut state, &mut sink);
        assert_eq!(platform.attempts.get(), 1, "100 >= 100 must attempt");
        assert!(state.held_socket.is_some());
    }

    // ── Connect ───────────────────────────────────────────────────────────

    /// A successful connect installs the socket, introduces the pod, announces
    /// the new stream to playback, and clears the backoff.
    #[test]
    fn a_successful_connect_installs_the_socket_and_introduces_the_pod() {
        let platform = FakePlatform::with(vec![Ok(FakeLink::new())]);
        let mut state = LinkState::new(0x11);
        // An elapsed deadline from an earlier failure: the connect proceeds, and
        // the deadline must not survive it.
        state.reconnect_deadline_secs = 42;
        platform.now_secs.set(100);
        let mut sink = RecordingSink::new();

        ensure_connected(&platform, &mut state, &mut sink);

        let link = state.held_socket.as_ref().expect("socket installed");
        let hello = sent_hello(link);
        assert_eq!(hello.version, AUDIO_PROTOCOL_VERSION);
        assert_eq!(hello.pod_id.as_str(), "pod-aabbcc");
        assert_eq!(hello.sample_rate_hz, DEVICE_PLAYBACK_FORMAT.sample_rate_hz);
        assert_eq!(
            hello.bits_per_sample,
            DEVICE_PLAYBACK_FORMAT.bits_per_sample
        );
        assert_eq!(hello.channels, DEVICE_PLAYBACK_FORMAT.channels);
        assert_eq!(hello.codec, DEVICE_PLAYBACK_FORMAT.codec);
        assert_eq!(
            hello.channel_source,
            ChannelSource::CommunicationBeam,
            "both beams are chip-processed outputs, which is what this label denotes"
        );
        assert_eq!(
            sink.marks,
            vec![SinkMark::StreamReset],
            "a fresh socket is a fresh playback stream"
        );
        assert_eq!(
            state.reconnect_deadline_secs, 0,
            "a stale deadline would pace the next drop's reconnect"
        );
        assert_eq!(state.backoff.current_secs(), BACKOFF_FLOOR_SECS);
    }

    /// The `Hello` rides the bounded-backpressure path, so a transport that takes
    /// it in pieces still yields one whole frame.
    #[test]
    fn a_short_write_still_delivers_a_whole_hello() {
        let mut link = FakeLink::new();
        link.write_script.push_back(5); // first write takes 5 bytes, rest follow
        let platform = FakePlatform::with(vec![Ok(link)]);
        let mut state = LinkState::new(0x11);
        let mut sink = RecordingSink::new();

        ensure_connected(&platform, &mut state, &mut sink);

        let link = state.held_socket.as_ref().expect("socket installed");
        assert_eq!(sent_hello(link).pod_id.as_str(), "pod-aabbcc");
    }

    /// An id too long for the wire field introduces the pod with an *empty* id, never
    /// a truncated one — a truncated id is a prefix of some other pod's, and a wrong
    /// id must not be able to look like a valid one. The connect still succeeds: the
    /// rule is "empty, not fail".
    #[test]
    fn an_oversized_pod_id_yields_an_empty_one_rather_than_a_truncated_one() {
        // The capacity comes off the wire field itself, so widening it cannot make
        // this test vacuous.
        let StreamFrame::Hello(fixture) = inbound_hello() else {
            unreachable!("the fixture is a Hello")
        };
        let long_id = "p".repeat(fixture.pod_id.capacity() + 1);
        let platform = FakePlatform::with(vec![Ok(FakeLink::new())]).with_pod_id(&long_id);
        let mut state = LinkState::new(0x11);
        let mut sink = RecordingSink::new();

        ensure_connected(&platform, &mut state, &mut sink);

        let link = state
            .held_socket
            .as_ref()
            .expect("an over-long id is not a connect failure");
        assert!(
            sent_hello(link).pod_id.is_empty(),
            "an id that does not fit must not be truncated onto the wire"
        );
        assert_eq!(sink.marks, vec![SinkMark::StreamReset]);
    }

    /// A `Hello` that never gets a byte through is a failed connect: the socket is
    /// dropped rather than carried into a segment, and the attempt is paced.
    #[test]
    fn a_stalled_hello_drops_the_fresh_connection_and_paces_the_retry() {
        let platform = FakePlatform::with(vec![Ok(write_erroring_link())]);
        let mut state = LinkState::new(0x11);
        let mut sink = RecordingSink::new();
        platform.now_secs.set(1_000);

        ensure_connected(&platform, &mut state, &mut sink);

        assert_eq!(platform.attempts.get(), 1);
        assert!(
            state.held_socket.is_none(),
            "a host too stalled for a Hello must not be handed a segment"
        );
        assert!(
            state.reconnect_deadline_secs > 1_000,
            "a stalled Hello must arm the backoff"
        );
        assert_eq!(state.attempt_counter, 1);
        assert!(
            sink.marks.is_empty(),
            "no socket was installed, so playback saw no stream boundary"
        );
    }

    /// A failed connect paces the next one, and consecutive failures keep
    /// charging the backoff rather than retrying at a fixed beat.
    #[test]
    fn consecutive_connect_failures_keep_charging_the_backoff() {
        let platform = FakePlatform::with(vec![
            Err(io::Error::other("no route")),
            Err(io::Error::other("no route")),
        ]);
        let mut state = LinkState::<FakeLink>::new(0x11);
        let mut sink = RecordingSink::new();
        platform.now_secs.set(1_000);

        ensure_connected(&platform, &mut state, &mut sink);
        let first = state.reconnect_deadline_secs;
        assert!(first > 1_000, "a failure must arm a deadline");
        assert_eq!(state.attempt_counter, 1);

        // The armed deadline holds off the second attempt until it elapses.
        ensure_connected(&platform, &mut state, &mut sink);
        assert_eq!(platform.attempts.get(), 1, "the deadline must be honored");

        platform.now_secs.set(first);
        ensure_connected(&platform, &mut state, &mut sink);
        assert_eq!(platform.attempts.get(), 2);
        assert_eq!(state.attempt_counter, 2);
        assert!(
            state.backoff.current_secs() > BACKOFF_FLOOR_SECS,
            "the second failure must have climbed the backoff"
        );
        assert!(state.reconnect_deadline_secs > first);
    }

    /// A connect that works after failures clears the pacing, so a later drop
    /// reconnects immediately instead of waiting out a stale deadline.
    #[test]
    fn a_connect_after_failures_clears_the_pacing() {
        let platform =
            FakePlatform::with(vec![Err(io::Error::other("no route")), Ok(FakeLink::new())]);
        let mut state = LinkState::new(0x11);
        let mut sink = RecordingSink::new();

        ensure_connected(&platform, &mut state, &mut sink);
        platform.now_secs.set(state.reconnect_deadline_secs);
        ensure_connected(&platform, &mut state, &mut sink);

        assert!(state.held_socket.is_some());
        assert_eq!(state.reconnect_deadline_secs, 0);
        assert_eq!(state.backoff.current_secs(), BACKOFF_FLOOR_SECS);
    }

    // ── Socket lifecycle ──────────────────────────────────────────────────

    /// The state's lifecycle wrappers route through the shared rules with the
    /// right pieces: a drop empties the framing state and lets the banked tail
    /// play out, and the reconnect that follows resets the stream.
    #[test]
    fn the_lifecycle_wrappers_reset_framing_and_mark_the_stream_boundaries() {
        let platform = FakePlatform::with(vec![Ok(FakeLink::new())]);
        let mut state = LinkState::new(0x11);
        let mut sink = RecordingSink::new();

        ensure_connected(&platform, &mut state, &mut sink);
        // A completed handshake plus a partial tail, as a live connection carries.
        (state.inbound_accum, state.inbound_state) = dirty_inbound();

        state.note_socket_lost(&mut sink);

        assert!(state.held_socket.is_none());
        assert_eq!(
            state.inbound_accum.valid_len(),
            0,
            "a stale tail would corrupt the next connection's first frame"
        );
        assert!(
            !state.inbound_state.seen_hello(),
            "the previous connection's handshake must not carry over"
        );
        assert_eq!(
            sink.marks,
            vec![SinkMark::StreamReset, SinkMark::EndOfAudio],
            "the banked tail plays out, then the sink goes quiet"
        );

        state.note_socket_established(FakeLink::new(), &mut sink);
        assert!(state.held_socket.is_some());
        assert_eq!(
            sink.marks.last(),
            Some(&SinkMark::StreamReset),
            "the reconnect discards the tail the teardown banked"
        );
    }
    // ── The streamer loop ─────────────────────────────────────────────────

    /// Idle wakes [`HangupPoll`] tolerates between one onset and the next before it
    /// declares its own assumption broken. The loop may legitimately idle several
    /// times in a row (a capped inbound pump re-polls immediately, a paced reconnect
    /// defers), so the budget is loose; exceeding it means the scripted onset never
    /// reached a segment, which would otherwise spin forever in a loop whose only
    /// exit is the hangup this shim owns.
    const MAX_IDLE_WAKES_PER_HANDOFF: u32 = 64;

    /// A `poll` shim that also owns the telemetry sender, so a test controls when
    /// the channel hangs up — the loop's only exit.
    ///
    /// A test that queues its messages and drops the sender itself cannot exercise
    /// a segment that runs to completion: `run_segment` drains the same channel and
    /// treats a disconnect mid-segment as an internal error. So the sender lives
    /// here, and hand-off is keyed on the one piece of loop state a `poll` shim can
    /// observe: the onset span. Each *finished* onset — a wake that saw the span
    /// published, followed by one that does not — hands over the next scripted batch
    /// of messages, or drops the sender once the script is spent. No poll count
    /// enters the decision, so how many times the loop polls per iteration is free
    /// to change without shifting the script.
    struct HangupPoll {
        poll: FakePoll,
        tx: RefCell<Option<Sender<StreamerMsg>>>,
        /// Messages to hand over, one batch per finished onset.
        batches: RefCell<VecDeque<Vec<StreamerMsg>>>,
        segment_active: Rc<AtomicBool>,
        /// Set by a wake taken while the span was published; cleared by the
        /// hand-off it triggers.
        saw_span: Cell<bool>,
        /// Idle wakes since the last hand-off, against
        /// [`MAX_IDLE_WAKES_PER_HANDOFF`].
        idle_wakes: Cell<u32>,
    }

    impl HangupPoll {
        fn new(
            tx: Sender<StreamerMsg>,
            segment_active: Rc<AtomicBool>,
            batches: Vec<Vec<StreamerMsg>>,
        ) -> Self {
            Self {
                poll: FakePoll::always_writable(),
                tx: RefCell::new(Some(tx)),
                batches: RefCell::new(batches.into()),
                segment_active,
                saw_span: Cell::new(false),
                idle_wakes: Cell::new(0),
            }
        }

        /// Hand over the next batch once an onset has finished, or hang up when the
        /// script is spent.
        fn tick(&self) {
            if self.segment_active.load(Ordering::Acquire) {
                // Inside an onset: not an idle wake, and the span the next hand-off
                // waits on.
                self.saw_span.set(true);
                return;
            }
            if !self.saw_span.get() {
                let wakes = self.idle_wakes.get() + 1;
                self.idle_wakes.set(wakes);
                assert!(
                    wakes <= MAX_IDLE_WAKES_PER_HANDOFF,
                    "HangupPoll: {wakes} idle wakes with no onset span in between — the \
                     script never reached a segment, so the loop can never be hung up"
                );
                return;
            }
            self.saw_span.set(false);
            self.idle_wakes.set(0);
            match self.batches.borrow_mut().pop_front() {
                Some(msgs) => {
                    let tx = self.tx.borrow();
                    let tx = tx.as_ref().expect("the sender is still held");
                    for msg in msgs {
                        tx.send(msg).expect("the streamer is still receiving");
                    }
                }
                None => {
                    self.tx.borrow_mut().take();
                }
            }
        }
    }

    impl NetPoll for HangupPoll {
        fn poll_readiness(
            &self,
            fd: std::os::fd::RawFd,
            interest: PollInterest,
            timeout: std::time::Duration,
        ) -> io::Result<Readiness> {
            self.tick();
            self.poll.poll_readiness(fd, interest, timeout)
        }
    }

    /// Everything a loop run needs that outlives the runtime's borrows.
    struct LoopHarness {
        ring: Mutex<Option<CaptureRing<Box<[i16]>>>>,
        ridx: RingIndex,
        vad_closed: AtomicBool,
        segment_active: Rc<AtomicBool>,
        sink: RecordingSink,
        inbound_obs: RecordingInboundObs,
    }

    impl LoopHarness {
        /// A capture ring holding `written` samples of history, anchored at 2 s.
        fn new(written: u64) -> Self {
            Self {
                ring: ring_with(written, 2_000_000),
                ridx: RingIndex::new(audio_pipeline::ring::RING_CAPACITY_SAMPLES),
                vad_closed: AtomicBool::new(false),
                segment_active: Rc::new(AtomicBool::new(false)),
                sink: RecordingSink::new(),
                inbound_obs: RecordingInboundObs::new(),
            }
        }
    }

    /// One reading the loop's observability seam took.
    #[derive(Debug, PartialEq, Eq)]
    struct Obs {
        segment_id: u32,
        event: ObsEvent,
        /// Whether the onset span was published when the event fired.
        span_published: bool,
    }

    /// Run the loop to its channel-disconnect exit against the fakes, on a µs clock
    /// that keeps the pace gate open and the real `Instant` clock for deadlines.
    fn drive(
        h: &mut LoopHarness,
        platform: &FakePlatform,
        rx: &Receiver<StreamerMsg>,
    ) -> (StreamerExit, Vec<Obs>) {
        let flag = Rc::clone(&h.segment_active);
        let observed = RefCell::new(Vec::new());
        let clock = free_running_clock();
        let exit = {
            let mut obs = |segment_id: u32, event: ObsEvent| {
                observed.borrow_mut().push(Obs {
                    segment_id,
                    event,
                    span_published: flag.load(Ordering::Acquire),
                });
            };
            let mut rt = StreamerRuntime {
                rx,
                ring: &h.ring,
                ridx: &h.ridx,
                vad_closed_flag: &h.vad_closed,
                segment_active_flag: &h.segment_active,
                inbound_sink: &mut h.sink,
                now_us: &clock,
                now_instant: &Instant::now,
                obs: &mut obs,
                inbound_obs: &mut h.inbound_obs,
                jitter_seed: 0x11,
            };
            run_streamer_loop(platform, &mut rt)
        };
        (exit, observed.into_inner())
    }

    /// The frames a tapped connection carried, asserting none was left half-written.
    fn wire_frames(wire: &Wire) -> Vec<StreamFrame> {
        let (frames, partial) = decode_stream(&wire.borrow());
        assert_eq!(partial, 0, "a frame was left half-written on the wire");
        frames
    }

    /// A VAD onset at `write_head` followed by its release.
    fn utterance(write_head: u64) -> Vec<StreamerMsg> {
        vec![
            StreamerMsg::VadOpened { write_head },
            StreamerMsg::VadClosed,
        ]
    }

    /// An onset streams over the connection the idle tick opened: `Hello` once,
    /// then the segment's own frames, and the loop comes back to idle with the
    /// socket kept.
    #[test]
    fn an_onset_streams_a_segment_over_the_idle_connection() {
        const HISTORY: u64 = 500;
        let mut h = LoopHarness::new(HISTORY);
        let (link, wire) = tapped_link(FakeLink::new());
        let (tx, rx) = channel();
        for msg in utterance(HISTORY) {
            tx.send(msg).expect("the receiver is live");
        }
        let poll = Rc::new(HangupPoll::new(
            tx,
            Rc::clone(&h.segment_active),
            Vec::new(),
        ));
        let platform = FakePlatform::with_poll(vec![Ok(link)], Rc::clone(&poll) as Rc<dyn NetPoll>);

        let (exit, observed) = drive(&mut h, &platform, &rx);

        assert_eq!(exit, StreamerExit::ChannelDisconnected);
        assert_eq!(
            platform.attempts.get(),
            1,
            "the onset must reuse the held socket"
        );
        let frames = wire_frames(&wire);
        match frames.as_slice() {
            [
                StreamFrame::Hello(hello),
                StreamFrame::SegmentStart(start),
                StreamFrame::Audio(full),
                StreamFrame::Audio(residual),
                StreamFrame::SegmentEnd(end),
            ] => {
                assert_eq!(hello.pod_id.as_str(), "pod-aabbcc");
                assert_eq!(start.segment_id, 0, "the first segment is numbered zero");
                assert_eq!(
                    start.base_sample_index, 0,
                    "the ring holds less than the pre-roll, so the segment opens at the oldest sample"
                );
                assert_eq!(start.preroll_samples, HISTORY as u32);
                assert_eq!(full.first_sample_index, 0);
                assert_eq!(full.pcm.len(), AUDIO_SAMPLES_PER_FRAME * 2);
                assert_eq!(
                    residual.pcm.len(),
                    (HISTORY as usize - AUDIO_SAMPLES_PER_FRAME) * 2
                );
                assert_eq!(end.reason, EndReason::VadRelease);
                assert_eq!(end.samples_sent, HISTORY);
            }
            other => panic!("unexpected traffic on the connection: {other:?}"),
        }
        assert_eq!(
            h.sink.marks,
            vec![SinkMark::StreamReset],
            "one connection, so one stream boundary and no teardown"
        );
        assert!(
            observed
                .iter()
                .all(|o| o.segment_id == 0 && o.span_published),
            "every reading is tagged with the running segment and taken inside its span: {observed:?}"
        );
        assert!(
            observed.iter().any(|o| o.event == ObsEvent::SegmentOpen),
            "the segment's opening waypoint must reach the platform"
        );
        assert!(
            !h.segment_active.load(Ordering::Acquire),
            "the onset span must be cleared once the segment is done"
        );
    }

    /// Consecutive utterances reuse the one connection and are numbered in order.
    #[test]
    fn consecutive_onsets_reuse_the_connection_and_number_segments_in_order() {
        let mut h = LoopHarness::new(640);
        let (link, wire) = tapped_link(FakeLink::new());
        let (tx, rx) = channel();
        for msg in utterance(320) {
            tx.send(msg).expect("the receiver is live");
        }
        let poll = Rc::new(HangupPoll::new(
            tx,
            Rc::clone(&h.segment_active),
            vec![utterance(640)],
        ));
        let platform = FakePlatform::with_poll(vec![Ok(link)], Rc::clone(&poll) as Rc<dyn NetPoll>);

        let (exit, observed) = drive(&mut h, &platform, &rx);

        assert_eq!(exit, StreamerExit::ChannelDisconnected);
        assert_eq!(
            platform.attempts.get(),
            1,
            "one connection for both segments"
        );
        let starts: Vec<u32> = wire_frames(&wire)
            .iter()
            .filter_map(|f| match f {
                StreamFrame::SegmentStart(s) => Some(s.segment_id),
                _ => None,
            })
            .collect();
        assert_eq!(starts, vec![0, 1], "segment ids advance by one per onset");
        let ends = wire_frames(&wire)
            .iter()
            .filter(|f| matches!(f, StreamFrame::SegmentEnd(_)))
            .count();
        assert_eq!(ends, 2, "both segments closed cleanly");
        let observed_ids: Vec<u32> = observed
            .iter()
            .filter(|o| o.event == ObsEvent::SegmentOpen)
            .map(|o| o.segment_id)
            .collect();
        assert_eq!(
            observed_ids,
            vec![0, 1],
            "each segment's readings carry its own id"
        );
        assert_eq!(
            h.sink.marks,
            vec![SinkMark::StreamReset],
            "the socket survived both segments, so playback saw one boundary"
        );
    }

    /// An onset connects even when the idle gate would not have: a paced reconnect
    /// is for an idle pod, not for an utterance in progress.
    #[test]
    fn an_onset_connects_even_when_the_idle_gate_declines() {
        const HISTORY: u64 = 400;
        let mut h = LoopHarness::new(HISTORY);
        let (link, wire) = tapped_link(FakeLink::new());
        let (tx, rx) = channel();
        for msg in utterance(HISTORY) {
            tx.send(msg).expect("the receiver is live");
        }
        let poll = Rc::new(HangupPoll::new(
            tx,
            Rc::clone(&h.segment_active),
            Vec::new(),
        ));
        let platform = FakePlatform::with_poll(vec![Ok(link)], Rc::clone(&poll) as Rc<dyn NetPoll>);
        // "Cannot say" is treated as down, so no idle tick ever attempts a connect.
        platform.link_up.set(None);

        let (exit, _) = drive(&mut h, &platform, &rx);

        assert_eq!(exit, StreamerExit::ChannelDisconnected);
        assert_eq!(platform.attempts.get(), 1, "the onset connected");
        let frames = wire_frames(&wire);
        assert!(
            matches!(frames.first(), Some(StreamFrame::Hello(_))),
            "the fresh connection introduced the pod first: {frames:?}"
        );
        assert!(
            frames
                .iter()
                .any(|f| matches!(f, StreamFrame::SegmentEnd(_))),
            "the segment ran on the connection the onset opened: {frames:?}"
        );
    }

    /// An onset whose connect fails drops that utterance and keeps idling — no
    /// second attempt inside the same onset, and the loop lives on.
    ///
    /// The onset span covers the connect, not just the segment: a platform whose
    /// quiesce check waits on the span must not start reading the capture ring while
    /// an accepted onset is still dialing.
    #[test]
    fn an_onset_whose_connect_fails_drops_the_segment_and_keeps_idling() {
        let mut h = LoopHarness::new(400);
        let (tx, rx) = channel();
        tx.send(StreamerMsg::VadOpened { write_head: 400 })
            .expect("the receiver is live");
        // No segment can run here, so the sender may hang up up front.
        drop(tx);
        let platform = FakePlatform::with(vec![Err(io::Error::other("no route"))])
            .watching_span(Rc::clone(&h.segment_active));
        platform.link_up.set(None);

        let (exit, observed) = drive(&mut h, &platform, &rx);

        assert_eq!(exit, StreamerExit::ChannelDisconnected);
        assert_eq!(
            platform.attempts.get(),
            1,
            "a dropped segment retries nothing"
        );
        assert_eq!(
            platform.span_at_connect.borrow().as_slice(),
            [true],
            "the onset-path connect must happen inside a published span"
        );
        assert!(observed.is_empty(), "no segment ran: {observed:?}");
        assert!(h.sink.marks.is_empty(), "playback saw no stream boundary");
        assert!(!h.segment_active.load(Ordering::Acquire));
    }

    /// A `SegmentStart` that fails on a *held* socket gets one reconnect, and the
    /// re-sent frame opens the segment on the replacement connection.
    #[test]
    fn a_segment_start_failure_on_a_held_socket_reconnects_once_and_resends() {
        const HISTORY: u64 = 400;
        let mut h = LoopHarness::new(HISTORY);
        // The first link takes the Hello and then dies, as a socket the peer reset
        // between the idle connect and the onset does.
        let (dying, first_wire) = tapped_link(FakeLink::dying_after(1));
        let (fresh, second_wire) = tapped_link(FakeLink::new());
        let (tx, rx) = channel();
        for msg in utterance(HISTORY) {
            tx.send(msg).expect("the receiver is live");
        }
        let poll = Rc::new(HangupPoll::new(
            tx,
            Rc::clone(&h.segment_active),
            Vec::new(),
        ));
        let platform = FakePlatform::with_poll(
            vec![Ok(dying), Ok(fresh)],
            Rc::clone(&poll) as Rc<dyn NetPoll>,
        );

        let (exit, _) = drive(&mut h, &platform, &rx);

        assert_eq!(exit, StreamerExit::ChannelDisconnected);
        assert_eq!(platform.attempts.get(), 2, "exactly one reconnect");
        assert!(
            matches!(wire_frames(&first_wire).as_slice(), [StreamFrame::Hello(_)]),
            "the dead socket carried only its Hello"
        );
        match wire_frames(&second_wire).as_slice() {
            [
                StreamFrame::Hello(_),
                StreamFrame::SegmentStart(start),
                rest @ ..,
            ] => {
                assert_eq!(start.segment_id, 0, "the re-send carries the same segment");
                assert!(
                    rest.iter().any(|f| matches!(f, StreamFrame::SegmentEnd(_))),
                    "the segment ran on the replacement socket: {rest:?}"
                );
            }
            other => panic!("unexpected traffic on the replacement: {other:?}"),
        }
        assert_eq!(
            h.sink.marks,
            vec![
                SinkMark::StreamReset,
                SinkMark::EndOfAudio,
                SinkMark::StreamReset
            ],
            "the dead socket's teardown banked its tail, and the replacement reset the stream"
        );
    }

    /// A `SegmentStart` that fails on a socket we *just* opened drops the segment
    /// instead of reconnecting — retrying a connection one frame old buys nothing.
    #[test]
    fn a_segment_start_failure_on_a_fresh_connect_drops_the_segment() {
        let mut h = LoopHarness::new(400);
        let (dying, wire) = tapped_link(FakeLink::dying_after(1));
        let (tx, rx) = channel();
        tx.send(StreamerMsg::VadOpened { write_head: 400 })
            .expect("the receiver is live");
        drop(tx);
        let platform = FakePlatform::with(vec![Ok(dying)]);
        // Down link, so the socket can only come from the onset path.
        platform.link_up.set(None);

        let (exit, observed) = drive(&mut h, &platform, &rx);

        assert_eq!(exit, StreamerExit::ChannelDisconnected);
        assert_eq!(
            platform.attempts.get(),
            1,
            "a fresh connection is not reconnected"
        );
        assert!(
            matches!(wire_frames(&wire).as_slice(), [StreamFrame::Hello(_)]),
            "the SegmentStart never left"
        );
        assert!(observed.is_empty(), "no segment ran: {observed:?}");
        assert_eq!(
            h.sink.marks,
            vec![SinkMark::StreamReset, SinkMark::EndOfAudio],
            "the failed socket was torn down"
        );
    }

    /// A `SegmentStart` that stalls on write backpressure drops the segment but
    /// keeps the socket: the host is slow, not gone.
    #[test]
    fn a_stalled_segment_start_drops_the_segment_and_keeps_the_socket() {
        let mut h = LoopHarness::new(400);
        // Takes the Hello whole, then never accepts another byte.
        let (stalling, wire) = tapped_link(FakeLink {
            write_script: VecDeque::from(vec![usize::MAX]),
            write_default: 0,
            ..FakeLink::new()
        });
        let (tx, rx) = channel();
        tx.send(StreamerMsg::VadOpened { write_head: 400 })
            .expect("the receiver is live");
        drop(tx);
        let platform = FakePlatform::with(vec![Ok(stalling)]);

        let (exit, observed) = drive(&mut h, &platform, &rx);

        assert_eq!(exit, StreamerExit::ChannelDisconnected);
        assert!(
            matches!(wire_frames(&wire).as_slice(), [StreamFrame::Hello(_)]),
            "no part of the SegmentStart reached the wire"
        );
        assert!(
            observed.is_empty(),
            "the segment never opened: {observed:?}"
        );
        assert_eq!(
            h.sink.marks,
            vec![SinkMark::StreamReset],
            "a stalled host is not a lost socket, so playback saw no teardown"
        );
    }

    /// An utterance dropped after its `SegmentStart` was numbered burns that id: the
    /// next utterance is numbered past it rather than reusing it. The host splices
    /// frames by segment id, so a reused id would concatenate two unrelated utterances
    /// into one transcript.
    #[test]
    fn a_dropped_segment_does_not_lend_its_id_to_the_next_one() {
        const HISTORY: u64 = 400;
        let mut h = LoopHarness::new(HISTORY);
        // The first onset's socket takes the Hello and then dies, dropping segment 0
        // on the fresh-connect path. The second onset gets a healthy socket.
        let (dying, first_wire) = tapped_link(FakeLink::dying_after(1));
        let (fresh, second_wire) = tapped_link(FakeLink::new());
        let (tx, rx) = channel();
        tx.send(StreamerMsg::VadOpened {
            write_head: HISTORY,
        })
        .expect("the receiver is live");
        // The release belongs to the second utterance.
        for msg in utterance(HISTORY) {
            tx.send(msg).expect("the receiver is live");
        }
        let poll = Rc::new(HangupPoll::new(
            tx,
            Rc::clone(&h.segment_active),
            Vec::new(),
        ));
        let platform = FakePlatform::with_poll(
            vec![Ok(dying), Ok(fresh)],
            Rc::clone(&poll) as Rc<dyn NetPoll>,
        );
        // Down link, so a socket can only come from an onset.
        platform.link_up.set(None);

        let (exit, observed) = drive(&mut h, &platform, &rx);

        assert_eq!(exit, StreamerExit::ChannelDisconnected);
        assert_eq!(platform.attempts.get(), 2, "one connect per onset");
        assert!(
            matches!(wire_frames(&first_wire).as_slice(), [StreamFrame::Hello(_)]),
            "segment 0's SegmentStart never left, so its id is spent, not delivered"
        );
        let frames = wire_frames(&second_wire);
        let starts: Vec<u32> = frames
            .iter()
            .filter_map(|f| match f {
                StreamFrame::SegmentStart(s) => Some(s.segment_id),
                _ => None,
            })
            .collect();
        assert_eq!(
            starts,
            vec![1],
            "the surviving utterance is numbered past the dropped one"
        );
        assert!(
            frames
                .iter()
                .any(|f| matches!(f, StreamFrame::SegmentEnd(_))),
            "the second utterance streamed to completion: {frames:?}"
        );
        let observed_ids: Vec<u32> = observed
            .iter()
            .filter(|o| o.event == ObsEvent::SegmentOpen)
            .map(|o| o.segment_id)
            .collect();
        assert_eq!(
            observed_ids,
            vec![1],
            "the readings carry the same id the wire does"
        );
    }

    /// A socket lost mid-segment is torn down and reconnected on the very next
    /// idle tick — a mid-utterance loss charges no backoff.
    #[test]
    fn a_socket_lost_mid_segment_is_replaced_on_the_next_tick() {
        const HISTORY: u64 = 400;
        let mut h = LoopHarness::new(HISTORY);
        // Hello and SegmentStart get through; the segment's first audio frame does
        // not, which is a dead socket rather than a stalled one.
        let (dying, first_wire) = tapped_link(FakeLink::dying_after(2));
        let (fresh, second_wire) = tapped_link(FakeLink::new());
        let (tx, rx) = channel();
        for msg in utterance(HISTORY) {
            tx.send(msg).expect("the receiver is live");
        }
        let poll = Rc::new(HangupPoll::new(
            tx,
            Rc::clone(&h.segment_active),
            Vec::new(),
        ));
        let platform = FakePlatform::with_poll(
            vec![Ok(dying), Ok(fresh)],
            Rc::clone(&poll) as Rc<dyn NetPoll>,
        );

        let (exit, _) = drive(&mut h, &platform, &rx);

        assert_eq!(exit, StreamerExit::ChannelDisconnected);
        assert_eq!(
            platform.attempts.get(),
            2,
            "the idle tick after the loss reconnects immediately"
        );
        assert!(
            matches!(
                wire_frames(&first_wire).as_slice(),
                [StreamFrame::Hello(_), StreamFrame::SegmentStart(_)]
            ),
            "the dead socket carried the opening frames and nothing more"
        );
        assert!(
            matches!(
                wire_frames(&second_wire).as_slice(),
                [StreamFrame::Hello(_)]
            ),
            "the replacement only introduced the pod: the segment is gone, not resumed"
        );
        assert_eq!(
            h.sink.marks,
            vec![
                SinkMark::StreamReset,
                SinkMark::EndOfAudio,
                SinkMark::StreamReset
            ],
            "the lost socket's teardown banked its tail, and the replacement reset the stream"
        );
    }

    /// Between segments the loop pumps inbound playback: the handshake and the
    /// audio reach the sink, and the wake armed POLLIN and not POLLOUT.
    #[test]
    fn an_idle_wake_drains_inbound_playback_to_the_sink() {
        let mut h = LoopHarness::new(0);
        let mut link = FakeLink::new();
        link.queue_read(&wire_bytes(&inbound_hello()));
        for _ in 0..3 {
            link.queue_read(&wire_bytes(&inbound_audio(160)));
        }
        let (tx, rx) = channel();
        // A stale close: consumed by the idle tick, opening no segment, so the
        // drain below is the only thing this iteration does.
        tx.send(StreamerMsg::VadClosed)
            .expect("the receiver is live");
        drop(tx);
        let poll = Rc::new(FakePoll::readable_faulting_after(u32::MAX));
        let platform = FakePlatform::with_poll(vec![Ok(link)], Rc::clone(&poll) as Rc<dyn NetPoll>);

        let (exit, observed) = drive(&mut h, &platform, &rx);

        assert_eq!(exit, StreamerExit::ChannelDisconnected);
        assert!(observed.is_empty(), "no segment ran: {observed:?}");
        assert_eq!(
            h.sink.offers,
            vec![320, 320, 320],
            "all three inbound frames drained in the one wake"
        );
        assert_eq!(
            h.inbound_obs.hellos.len(),
            1,
            "the inbound handshake was observed once"
        );
        assert_eq!(
            poll.seen.borrow().first().map(|s| s.1),
            Some(PollInterest::READ),
            "an idle wake arms POLLIN only — there is nothing to write"
        );
        assert_eq!(
            h.sink.marks,
            vec![SinkMark::StreamReset],
            "the socket was never torn down"
        );
    }

    /// Poll discipline rule 1 at the idle level: a transport that buffers decrypted
    /// plaintext is read on every wake, including one whose poll reported nothing
    /// readable. This is the shipping pod's transport (`TlsStream::buffers_plaintext`
    /// is true), so without this disjunct inbound TTS sits in the TLS session between
    /// segments until some unrelated POLLIN happens to fire.
    #[test]
    fn an_idle_wake_drains_a_plaintext_buffering_link_without_pollin() {
        let mut h = LoopHarness::new(0);
        let mut link = FakeLink {
            plaintext: true,
            ..FakeLink::new()
        };
        link.queue_read(&wire_bytes(&inbound_hello()));
        for _ in 0..3 {
            link.queue_read(&wire_bytes(&inbound_audio(160)));
        }
        let (tx, rx) = channel();
        // A stale close, so the iteration opens no segment and the drain is the only
        // thing it does.
        tx.send(StreamerMsg::VadClosed)
            .expect("the receiver is live");
        drop(tx);
        // Writable only: no wake ever reports readable.
        let poll = Rc::new(FakePoll::always_writable());
        let platform = FakePlatform::with_poll(vec![Ok(link)], Rc::clone(&poll) as Rc<dyn NetPoll>);

        let (exit, observed) = drive(&mut h, &platform, &rx);

        assert_eq!(exit, StreamerExit::ChannelDisconnected);
        assert!(observed.is_empty(), "no segment ran: {observed:?}");
        assert_eq!(
            h.sink.offers,
            vec![320, 320, 320],
            "the buffering transport is read on a wake that reported no readability"
        );
        assert_eq!(
            h.inbound_obs.hellos.len(),
            1,
            "the inbound handshake was observed once"
        );
    }

    /// An idle pump that stops at its per-wake cap carries that into the next wake,
    /// which re-polls with a zero timeout instead of sleeping out the tick — the
    /// difference between draining a playback backlog back-to-back and one pump per
    /// 10 ms, which is audible stutter on the TTS path.
    #[test]
    fn an_idle_inbound_backlog_past_the_cap_re_polls_immediately() {
        let mut h = LoopHarness::new(0);
        let mut link = FakeLink::new();
        let audio = wire_bytes(&inbound_audio(160));
        // One frame's worth of bytes per read, so every pump step makes progress and
        // the cap is what stops the pump.
        link.read_chunk = audio.len();
        link.queue_read(&wire_bytes(&inbound_hello()));
        let queued = INBOUND_STEPS_PER_WAKE as usize + 1;
        for _ in 0..queued {
            link.queue_read(&audio);
        }
        let (tx, rx) = channel();
        // Three stale closes: three idle iterations, then the hangup.
        for _ in 0..3 {
            tx.send(StreamerMsg::VadClosed)
                .expect("the receiver is live");
        }
        drop(tx);
        let poll = Rc::new(FakePoll::readable_faulting_after(u32::MAX));
        let platform = FakePlatform::with_poll(vec![Ok(link)], Rc::clone(&poll) as Rc<dyn NetPoll>);

        let (exit, _) = drive(&mut h, &platform, &rx);

        assert_eq!(exit, StreamerExit::ChannelDisconnected);
        assert_eq!(
            h.sink.offers.len(),
            queued,
            "the whole backlog reached the sink inside the run"
        );
        let timeouts: Vec<Duration> = poll.seen.borrow().iter().map(|s| s.2).collect();
        assert_eq!(
            timeouts[0], IDLE_TICK,
            "the first wake has no carried work, so it sleeps the tick"
        );
        assert_eq!(
            timeouts[1],
            Duration::ZERO,
            "a pump stopped at its cap re-polls without sleeping"
        );
        assert_eq!(
            timeouts[2], IDLE_TICK,
            "a drained backlog stops the zero-timeout re-polls"
        );
    }

    /// A full accumulator de-arms POLLIN at the idle level, and the `!inbound_armed`
    /// disjunct is what still runs the pump: the held frame is re-offered until a slot
    /// frees, and POLLIN re-arms once it does. Losing the de-arm busy-spins at 100% CPU
    /// on a level-triggered POLLIN nobody consumes; losing the re-offer wedges idle
    /// playback for good, because the slot never frees and so POLLIN never re-arms.
    #[test]
    fn a_full_idle_accumulator_de_arms_pollin_and_re_arms_once_a_slot_frees() {
        let mut h = LoopHarness::new(0);
        // Every offer of the held frame inside the first iteration's pump is refused;
        // the first offer of the next iteration is accepted. One more refusal than the
        // pump makes in one wake, so the backpressure spans the wake boundary.
        h.sink.refuse_first = 3;
        let mut link = FakeLink::new();
        // A Hello, an Audio frame the sink refuses (held at the head), then a length
        // prefix declaring a frame far longer than the padding after it: the
        // accumulator fills to the byte with something that can never be consumed. The
        // padding runs one Hello past the accumulator's capacity, so the space freed by
        // consuming the Hello fills too — and then the peer has nothing left to send,
        // so a freed sink slot is the only thing that can change.
        let hello = wire_bytes(&inbound_hello());
        let mut bytes = hello.clone();
        bytes.extend_from_slice(&wire_bytes(&inbound_audio(160)));
        bytes.extend_from_slice(&(MAX_FRAME_BYTES as u16).to_le_bytes());
        bytes.resize(MAX_FRAME_BYTES + 2 + hello.len(), 0);
        link.queue_read(&bytes);
        let (tx, rx) = channel();
        for _ in 0..3 {
            tx.send(StreamerMsg::VadClosed)
                .expect("the receiver is live");
        }
        drop(tx);
        // Readable on the first wake only, so the drain that re-offers the held frame
        // can only have been driven by the de-armed accumulator.
        let poll = Rc::new(FakePoll::readable_for(1));
        let platform = FakePlatform::with_poll(vec![Ok(link)], Rc::clone(&poll) as Rc<dyn NetPoll>);

        let (exit, _) = drive(&mut h, &platform, &rx);

        assert_eq!(exit, StreamerExit::ChannelDisconnected);
        let armed: Vec<bool> = poll.seen.borrow().iter().map(|s| s.1.read).collect();
        let de_armed = armed
            .iter()
            .position(|a| !a)
            .expect("a full accumulator must de-arm POLLIN");
        assert!(
            armed[..de_armed].iter().all(|a| *a),
            "POLLIN stays armed while the accumulator has room: {armed:?}"
        );
        assert!(
            armed[de_armed + 1..].iter().any(|a| *a),
            "a freed slot must re-arm POLLIN: {armed:?}"
        );
        assert_eq!(
            h.sink.offers.len(),
            h.sink.refuse_first as usize + 1,
            "the held frame is re-offered every wake until a slot frees, then accepted"
        );
        assert!(
            h.sink.offers.iter().all(|n| *n == 320),
            "every offer is the same held frame: {:?}",
            h.sink.offers
        );
        assert_eq!(
            h.inbound_obs.waypoints,
            vec![(InboundWaypoint::Periodic, 1)],
            "the frame counts once, on the offer that was accepted"
        );
    }

    /// A socket fault on the idle poll clears the connection and paces the
    /// reconnect, so the very next tick does not attempt one.
    #[test]
    fn an_idle_poll_fault_clears_the_socket_and_paces_the_reconnect() {
        let mut h = LoopHarness::new(0);
        let (tx, rx) = channel();
        drop(tx);
        let poll = Rc::new(FakePoll::faulting_after(0));
        let platform = FakePlatform::with_poll(
            vec![Ok(FakeLink::new())],
            Rc::clone(&poll) as Rc<dyn NetPoll>,
        );

        let (exit, _) = drive(&mut h, &platform, &rx);

        assert_eq!(exit, StreamerExit::ChannelDisconnected);
        assert_eq!(
            platform.attempts.get(),
            1,
            "the armed backoff deadline held off an immediate reconnect"
        );
        assert_eq!(
            h.sink.marks,
            vec![SinkMark::StreamReset, SinkMark::EndOfAudio],
            "the faulted socket was torn down"
        );
    }

    /// An unrecoverable inbound stream clears the socket, samples the teardown on
    /// the way out, and paces the reconnect.
    #[test]
    fn an_idle_drain_error_clears_the_socket_and_samples_the_exit() {
        let mut h = LoopHarness::new(0);
        let mut link = FakeLink::new();
        link.queue_read(&wire_bytes(&inbound_hello()));
        link.queue_read(&wire_bytes(&inbound_audio(160)));
        // A length prefix past MAX_FRAME_BYTES is unrecoverable framing.
        link.queue_read(&(audio_pipeline::wire::MAX_FRAME_BYTES as u16 + 1).to_le_bytes());
        let (tx, rx) = channel();
        tx.send(StreamerMsg::VadClosed)
            .expect("the receiver is live");
        drop(tx);
        let poll = Rc::new(FakePoll::readable_faulting_after(u32::MAX));
        let platform = FakePlatform::with_poll(vec![Ok(link)], Rc::clone(&poll) as Rc<dyn NetPoll>);

        let (exit, _) = drive(&mut h, &platform, &rx);

        assert_eq!(exit, StreamerExit::ChannelDisconnected);
        assert_eq!(
            platform.attempts.get(),
            1,
            "the drain error armed the backoff deadline"
        );
        assert_eq!(
            h.inbound_obs.waypoints.last(),
            Some(&(InboundWaypoint::Exit, 1)),
            "the teardown sample carries the connection's accepted-frame count"
        );
        assert_eq!(
            h.sink.marks,
            vec![SinkMark::StreamReset, SinkMark::EndOfAudio],
            "stale partial bytes are dropped with the socket"
        );
    }
}
