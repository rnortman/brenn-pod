//! The Linux pod's whole client stack, end to end against an in-process host.
//!
//! Every layer below the device binary is exercised as shipped: `pod-streamer`'s
//! streamer loop and segment engine, the shared inbound playback path in
//! `audio-pipeline`, and `psk-link`'s Linux transport. The peer is a TLS-PSK
//! listener pinned by the same parameters the production server pins, validating
//! frames with `pod_ingest::SessionFsm` and the `SPINE_FORMAT` constraint.
//!
//! Nothing here depends on the `reachy-pod` binary crate, so a break in the
//! shared engine or the Linux transport fails on a workstation rather than only
//! on the bench.
//!
//! The pod runs in its own thread with a pre-filled capture ring in place of a
//! capture thread and the test itself in place of the telemetry thread, which is
//! what makes the timing deterministic: the ring's contents and write head are
//! fixed, so a segment's frame count, sample indices and PCM are all predictable.

use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{Receiver, Sender, SyncSender, sync_channel};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use audio_pipeline::inbound::{InboundObserver, InboundWaypoint};
use audio_pipeline::playback::{Accepted, PlaybackSink};
use audio_pipeline::ring::{CaptureRing, RING_CAPACITY_SAMPLES, RingIndex, SAMPLE_RATE_HZ};
use audio_pipeline::wire::{
    AUDIO_PROTOCOL_VERSION, AudioFrame, ChannelSource, DEVICE_PLAYBACK_FORMAT, EndOfAudio, Hello,
    MAX_AUDIO_PAYLOAD, PlaybackFormat, StreamFrame, Telemetry as WireTelemetry, decode_frame,
};
use openssl::ssl::{ErrorCode, Ssl, SslStream};
use pod_ingest::test_fixtures::framed;
use pod_ingest::{
    CloseCause, CrossCheck, EndReason, HostMicros, ResumeLedger, SegmentClose, SessionEvent,
    SessionFsm, TelemetryKind,
};
use pod_streamer::run::{StreamerExit, StreamerRuntime, run_streamer_loop};
use pod_streamer::segment::{ObsEvent, StreamerMsg};
use psk_link::link::LinkPlatform;
use speech_pipeline::SPINE_FORMAT;
use speech_surface::psk::{PSK_LEN, test_server_context};

/// The pod's identity — doubles as both the PSK identity and `Hello.pod_id`.
const POD_ID: &str = "pod-linux";
/// The one key both ends hold.
const PSK: [u8; PSK_LEN] = [0x5a; PSK_LEN];

/// Longest any wait in this file tolerates. Generous — these are localhost round
/// trips, and the reconnect test spends the streamer's own backoff floor (~2 s)
/// inside it — so reaching it means something is wedged, not slow.
const TEST_TIMEOUT: Duration = Duration::from_secs(15);
const POLL_SLEEP: Duration = Duration::from_millis(1);

/// Samples of history the pod's ring holds at onset: two whole 320-sample frames
/// plus a 160-sample residual, so a segment exercises both the full-frame drain
/// and the partial-frame-at-close arm.
const RING_HISTORY_SAMPLES: u64 = 800;
/// Samples per wire audio frame (20 ms at 16 kHz).
const FRAME_SAMPLES: u64 = 320;
/// Device monotonic µs the ring's clock anchor carries — a plausible uptime, so
/// the back-extrapolated segment base is a positive timestamp rather than a
/// saturated zero.
const DEVICE_ANCHOR_TS_US: u64 = 1_000_000;
/// Device-clock offset of the test's telemetry reading from the segment base.
const TELEMETRY_OFFSET_US: u64 = 10_000;
/// That offset expressed in samples — what the ingest FSM must derive from it.
const TELEMETRY_OFFSET_SAMPLES: i64 = 160;
/// The speech-energy reading the test forwards.
const SPENERGY: [f32; 4] = [0.1, 0.2, 0.3, 0.4];

// ── The in-process host ───────────────────────────────────────────────────────

/// What the host peer observed, in the order it observed it.
#[derive(Debug)]
enum HostEvent {
    /// A TLS-PSK session came up on connection `n`.
    Accepted(u32),
    /// Connection `n`'s handshake failed — a key or identity the host does not hold.
    HandshakeFailed(u32),
    /// One ingest-FSM event from connection `n`.
    Ingest(u32, SessionEvent),
    /// Connection `n` ended and its FSM was closed with this cause.
    Closed(u32, CloseCause),
}

/// What the host does once a connection's `Hello` is accepted.
#[derive(Default)]
struct HostScript {
    /// Playback frames written server→device, in order.
    playback: Vec<StreamFrame>,
    /// Close the connection immediately after accepting the `Hello`, so the pod
    /// has to notice and reconnect.
    close_after_hello: bool,
}

/// A TLS-PSK listener speaking the ingest protocol, on its own thread.
struct HostPeer {
    /// Where the pod connects.
    addr: SocketAddr,
    /// Events pulled off `rx` so far, in order.
    seen: Vec<HostEvent>,
    rx: Receiver<HostEvent>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl HostPeer {
    /// Bind an ephemeral port and start serving — one connection at a time, each
    /// with a fresh `SessionFsm` over one shared resume ledger (the production
    /// arrangement).
    fn start(script: HostScript) -> HostPeer {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind host listener");
        let addr = listener.local_addr().expect("host local addr");
        listener
            .set_nonblocking(true)
            .expect("non-blocking host listener");
        let (tx, rx) = mpsc::channel();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let handle = thread::spawn(move || serve_forever(listener, script, tx, thread_stop));
        HostPeer {
            addr,
            seen: Vec::new(),
            rx,
            stop,
            handle: Some(handle),
        }
    }

    /// Pull host events until one at index `from` or later satisfies `pred`,
    /// returning its index in [`Self::seen`]. Panics naming `what` if the budget
    /// expires or the host thread died.
    ///
    /// `from` must name an event already seen or the next one; every caller passes
    /// `previous_match_index + 1`. Clamping it instead would shift the returned
    /// index forward of the event that actually matched.
    fn wait_for(&mut self, what: &str, from: usize, pred: impl Fn(&HostEvent) -> bool) -> usize {
        let deadline = Instant::now() + TEST_TIMEOUT;
        loop {
            assert!(
                from <= self.seen.len(),
                "wait_for({what}) starts at {from} with only {} events seen",
                self.seen.len()
            );
            if let Some(offset) = self.seen[from..].iter().position(&pred) {
                return from + offset;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "host never reported {what}; saw {:?}",
                self.seen
            );
            match self.rx.recv_timeout(remaining.min(POLL_SLEEP * 50)) {
                Ok(ev) => self.seen.push(ev),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => panic!(
                    "host thread ended before reporting {what}; saw {:?}",
                    self.seen
                ),
            }
        }
    }

    /// Drain whatever the host has already reported without waiting.
    fn drain(&mut self) {
        while let Ok(ev) = self.rx.try_recv() {
            self.seen.push(ev);
        }
    }

    /// The ingest-FSM event at index `i`, which the caller has already matched as
    /// one.
    fn ingest_at(&self, i: usize) -> &SessionEvent {
        match &self.seen[i] {
            HostEvent::Ingest(_, ev) => ev,
            other => panic!("event {i} is not an ingest event: {other:?}"),
        }
    }

    /// Stop serving and join the thread. Idempotent, so a test may keep reading
    /// [`Self::seen`] afterwards.
    fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            h.join().expect("host thread");
        }
    }
}

/// Accept connections until told to stop, serving each in turn.
fn serve_forever(
    listener: TcpListener,
    script: HostScript,
    tx: Sender<HostEvent>,
    stop: Arc<AtomicBool>,
) {
    // The pinned single-identity peer the transport's own tests use: a pod that
    // cannot talk to this context cannot talk to the daemon either.
    let ctx = test_server_context(POD_ID, PSK);
    let ledger = ResumeLedger::shared();
    let mut conn = 0u32;
    while !stop.load(Ordering::Relaxed) {
        let sock = match listener.accept() {
            Ok((sock, _)) => sock,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(POLL_SLEEP);
                continue;
            }
            Err(_) => return,
        };
        let seq = conn;
        conn += 1;
        sock.set_nonblocking(true)
            .expect("non-blocking host session");
        let ssl = Ssl::new(&ctx).expect("host ssl session");
        let mut stream = SslStream::new(ssl, sock).expect("wrap host socket");
        if !accept_handshake(&mut stream, &stop) {
            let _ = tx.send(HostEvent::HandshakeFailed(seq));
            continue;
        }
        let _ = tx.send(HostEvent::Accepted(seq));

        let mut fsm = SessionFsm::new(SPINE_FORMAT, Arc::clone(&ledger));
        let cause = serve_session(&mut stream, &mut fsm, seq, &tx, &script, &stop);
        // Close the FSM so any open segment finalizes truncated — the production
        // teardown path.
        for ev in fsm.close(cause, HostMicros::now()) {
            let _ = tx.send(HostEvent::Ingest(seq, ev));
        }
        let _ = tx.send(HostEvent::Closed(seq, cause));
        let _ = stream.shutdown();
    }
}

/// Complete the server handshake on a non-blocking socket. `false` on a refused
/// key or identity, on the budget expiring, or on a stop request.
fn accept_handshake(stream: &mut SslStream<TcpStream>, stop: &AtomicBool) -> bool {
    let deadline = Instant::now() + TEST_TIMEOUT;
    loop {
        match stream.accept() {
            Ok(()) => return true,
            Err(e) => match e.code() {
                ErrorCode::WANT_READ | ErrorCode::WANT_WRITE => {
                    if Instant::now() >= deadline || stop.load(Ordering::Relaxed) {
                        return false;
                    }
                    thread::sleep(POLL_SLEEP);
                }
                _ => return false,
            },
        }
    }
}

/// Serve one accepted connection, returning the [`CloseCause`] for the FSM.
fn serve_session(
    stream: &mut SslStream<TcpStream>,
    fsm: &mut SessionFsm,
    seq: u32,
    tx: &Sender<HostEvent>,
    script: &HostScript,
    stop: &AtomicBool,
) -> CloseCause {
    let deadline = Instant::now() + TEST_TIMEOUT;
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    let mut played = false;
    loop {
        if stop.load(Ordering::Relaxed) {
            return CloseCause::Shutdown;
        }
        if Instant::now() >= deadline {
            return CloseCause::ReadError;
        }
        match stream.ssl_read(&mut chunk) {
            Ok(0) => return CloseCause::Eof,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e) => match e.code() {
                ErrorCode::WANT_READ | ErrorCode::WANT_WRITE => thread::sleep(POLL_SLEEP),
                ErrorCode::ZERO_RETURN => return CloseCause::Eof,
                _ => return CloseCause::ReadError,
            },
        }

        while let Some(frame_len) = complete_frame_len(&buf) {
            let frame = match decode_frame(&buf[..frame_len]) {
                Ok(f) => f,
                Err(_) => return CloseCause::DecodeError,
            };
            buf.drain(..frame_len);
            let events = fsm.feed(frame, HostMicros::now());
            let mut fatal = false;
            let mut accepted_hello = false;
            for ev in events {
                match &ev {
                    SessionEvent::ProtocolError { fatal: true, .. } => fatal = true,
                    SessionEvent::HelloAccepted { .. } => accepted_hello = true,
                    _ => {}
                }
                let _ = tx.send(HostEvent::Ingest(seq, ev));
            }
            if fatal {
                // The FSM is parked; feeding it again is a contract violation.
                return CloseCause::DecodeError;
            }
            if accepted_hello {
                if script.close_after_hello {
                    return CloseCause::Superseded;
                }
                if !played {
                    played = true;
                    for frame in &script.playback {
                        write_all_tls(stream, &framed(frame));
                    }
                }
            }
        }
    }
}

/// Total length of the frame at the front of `buf`, once all of it has arrived.
fn complete_frame_len(buf: &[u8]) -> Option<usize> {
    if buf.len() < 2 {
        return None;
    }
    let payload_len = u16::from_le_bytes([buf[0], buf[1]]) as usize;
    let total = 2 + payload_len;
    (buf.len() >= total).then_some(total)
}

/// Write every byte of a downlink frame, waiting out the session's `WANT_*`.
fn write_all_tls(stream: &mut SslStream<TcpStream>, bytes: &[u8]) {
    let deadline = Instant::now() + TEST_TIMEOUT;
    let mut sent = 0;
    while sent < bytes.len() {
        match stream.ssl_write(&bytes[sent..]) {
            Ok(n) => sent += n,
            Err(e) => match e.code() {
                ErrorCode::WANT_READ | ErrorCode::WANT_WRITE => {
                    assert!(Instant::now() < deadline, "host write never completed");
                    thread::sleep(POLL_SLEEP);
                }
                _ => panic!("host write failed: {e}"),
            },
        }
    }
}

/// The host's playback handshake, declaring exactly the format the device accepts.
fn playback_hello() -> StreamFrame {
    StreamFrame::Hello(Hello {
        version: AUDIO_PROTOCOL_VERSION,
        pod_id: heapless::String::try_from("host").expect("pod_id fits"),
        sample_rate_hz: DEVICE_PLAYBACK_FORMAT.sample_rate_hz,
        bits_per_sample: DEVICE_PLAYBACK_FORMAT.bits_per_sample,
        channels: DEVICE_PLAYBACK_FORMAT.channels,
        codec: DEVICE_PLAYBACK_FORMAT.codec,
        channel_source: ChannelSource::CommunicationBeam,
    })
}

/// A playback audio frame of `samples` distinguishable S16 samples starting at
/// `first`, so the bytes that reach the sink can be checked against their source.
fn playback_audio(first: u64, samples: u64) -> StreamFrame {
    let mut pcm: heapless::Vec<u8, MAX_AUDIO_PAYLOAD> = heapless::Vec::new();
    for s in sample_run(first, samples) {
        for b in s.to_le_bytes() {
            pcm.push(b).expect("pcm fits MAX_AUDIO_PAYLOAD");
        }
    }
    StreamFrame::Audio(AudioFrame {
        segment_id: 0,
        first_sample_index: first,
        device_ts_us: 0,
        pcm,
    })
}

/// The sample values at absolute indices `first..first + count` in the test's
/// synthetic waveform — the same rule the pod's ring is filled by, so uplink and
/// downlink can both be checked against it.
fn sample_run(first: u64, count: u64) -> Vec<i16> {
    (first..first + count).map(|i| (i % 4096) as i16).collect()
}

// ── The pod under test ────────────────────────────────────────────────────────

/// Playback the pod has taken in, readable while the streamer thread still owns
/// its sink.
#[derive(Default)]
struct PlaybackProgress {
    frames: AtomicU32,
    end_of_audio: AtomicU32,
}

/// The pod's playback sink: records the PCM every accepted frame carried and
/// counts each control signal the shared inbound path raised.
struct RecordingSink {
    pcm: Vec<Vec<u8>>,
    end_of_audio: u32,
    flushes: u32,
    stream_resets: u32,
    progress: Arc<PlaybackProgress>,
}

impl RecordingSink {
    fn new(progress: Arc<PlaybackProgress>) -> Self {
        Self {
            pcm: Vec::new(),
            end_of_audio: 0,
            flushes: 0,
            stream_resets: 0,
            progress,
        }
    }
}

impl PlaybackSink for RecordingSink {
    fn accept(&mut self, pcm: &[u8]) -> Accepted {
        self.pcm.push(pcm.to_vec());
        self.progress.frames.fetch_add(1, Ordering::Release);
        Accepted::Enqueued
    }

    fn end_of_audio(&mut self) {
        self.end_of_audio += 1;
        self.progress.end_of_audio.fetch_add(1, Ordering::Release);
    }

    fn flush_playback(&mut self) {
        self.flushes += 1;
    }

    fn stream_reset(&mut self) {
        self.stream_resets += 1;
    }
}

/// Records the inbound path's observability calls.
#[derive(Default)]
struct RecordingInboundObs {
    waypoints: Vec<(InboundWaypoint, u32)>,
    hellos: Vec<PlaybackFormat>,
}

impl InboundObserver for RecordingInboundObs {
    fn waypoint(&mut self, site: InboundWaypoint, frame: u32) {
        self.waypoints.push((site, frame));
    }

    fn hello_ok(&mut self, format: PlaybackFormat) {
        self.hellos.push(format);
    }
}

/// Everything the pod's own seams recorded over one run.
struct PodOutcome {
    exit: StreamerExit,
    sink: RecordingSink,
    inbound_hellos: Vec<PlaybackFormat>,
    inbound_waypoints: Vec<(InboundWaypoint, u32)>,
    /// Segment observability, as `(segment id, event token)`.
    obs: Vec<(u32, &'static str)>,
}

/// A running streamer thread and the telemetry channel that drives it.
struct PodUnderTest {
    tx: SyncSender<StreamerMsg>,
    progress: Arc<PlaybackProgress>,
    handle: JoinHandle<PodOutcome>,
}

impl PodUnderTest {
    /// Publish one telemetry-thread message.
    fn send(&self, msg: StreamerMsg) {
        self.tx.send(msg).expect("streamer thread is alive");
    }

    /// Wait until the pod has taken in `frames` playback frames and `eoa`
    /// end-of-audio marks.
    fn wait_for_playback(&self, frames: u32, eoa: u32) {
        let deadline = Instant::now() + TEST_TIMEOUT;
        loop {
            let got = (
                self.progress.frames.load(Ordering::Acquire),
                self.progress.end_of_audio.load(Ordering::Acquire),
            );
            // Both counts, separately: a tuple comparison is lexicographic, so an
            // overshoot in the frame count would satisfy it with no mark seen.
            if got.0 >= frames && got.1 >= eoa {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "pod took in {got:?} playback frames/marks, expected at least {:?}",
                (frames, eoa)
            );
            thread::sleep(POLL_SLEEP);
        }
    }

    /// Drop the telemetry sender — the loop's only exit — and collect the run.
    fn finish(self) -> PodOutcome {
        drop(self.tx);
        self.handle.join().expect("streamer thread")
    }
}

/// A capture ring holding `written` samples of the synthetic waveform, anchored
/// at [`DEVICE_ANCHOR_TS_US`] — what a capture thread would have left behind.
fn ring_with(written: u64) -> Mutex<Option<CaptureRing<Box<[i16]>>>> {
    let mut samples = vec![0i16; RING_CAPACITY_SAMPLES].into_boxed_slice();
    for (i, s) in sample_run(0, written).into_iter().enumerate() {
        samples[i % RING_CAPACITY_SAMPLES] = s;
    }
    Mutex::new(Some(CaptureRing {
        samples,
        write_head: written,
        anchor_sample: written.saturating_sub(1),
        anchor_ts_us: DEVICE_ANCHOR_TS_US,
    }))
}

/// Start a pod streaming to `addr` as [`POD_ID`] with `key`.
///
/// Wires the same seam surface the production binary does.
fn start_pod(addr: SocketAddr, key: [u8; PSK_LEN]) -> PodUnderTest {
    let (tx, rx) = sync_channel::<StreamerMsg>(8);
    let progress = Arc::new(PlaybackProgress::default());
    let thread_progress = Arc::clone(&progress);
    let handle = thread::spawn(move || {
        let ring = ring_with(RING_HISTORY_SAMPLES);
        let ridx = RingIndex::new(RING_CAPACITY_SAMPLES);
        let vad_closed_flag = AtomicBool::new(false);
        let segment_active_flag = AtomicBool::new(false);
        let mut sink = RecordingSink::new(thread_progress);
        let mut inbound_obs = RecordingInboundObs::default();
        let mut obs_log: Vec<(u32, &'static str)> = Vec::new();
        // The device clock: monotonic µs in the same domain as the ring's anchor,
        // so a segment's timestamps are the pod's own uptime rather than an epoch.
        let epoch = Instant::now();
        let now_us = move || DEVICE_ANCHOR_TS_US + epoch.elapsed().as_micros() as u64;
        let platform = LinkPlatform::new(POD_ID.to_string(), addr, key);

        let exit = {
            let mut obs =
                |segment_id: u32, event: ObsEvent| obs_log.push((segment_id, event.as_str()));
            let mut rt = StreamerRuntime {
                rx: &rx,
                ring: &ring,
                ridx: &ridx,
                vad_closed_flag: &vad_closed_flag,
                segment_active_flag: &segment_active_flag,
                inbound_sink: &mut sink,
                now_us: &now_us,
                now_instant: &Instant::now,
                obs: &mut obs,
                inbound_obs: &mut inbound_obs,
                jitter_seed: 0x5eed,
            };
            run_streamer_loop(&platform, &mut rt)
        };

        PodOutcome {
            exit,
            sink,
            inbound_hellos: inbound_obs.hellos,
            inbound_waypoints: inbound_obs.waypoints,
            obs: obs_log,
        }
    });
    PodUnderTest {
        tx,
        progress,
        handle,
    }
}

/// The `SessionEvent`s the host's FSM emitted, in order.
fn ingest_events(seen: &[HostEvent]) -> Vec<&SessionEvent> {
    seen.iter()
        .filter_map(|e| match e {
            HostEvent::Ingest(_, ev) => Some(ev),
            _ => None,
        })
        .collect()
}

/// One utterance end to end over a real TLS session: handshake, pre-roll
/// placement, audio frames in order, in-band telemetry at the right sample
/// offset, and a complete segment close with a matching cross-check.
#[test]
fn a_pods_utterance_reaches_the_ingest_fsm_intact() {
    let mut host = HostPeer::start(HostScript::default());
    let pod = start_pod(host.addr, PSK);

    host.wait_for("a TLS session", 0, |e| matches!(e, HostEvent::Accepted(0)));
    let hello_at = host.wait_for("the pod's Hello", 0, |e| {
        matches!(e, HostEvent::Ingest(_, SessionEvent::HelloAccepted { .. }))
    });
    let SessionEvent::HelloAccepted { pod_id, .. } = host.ingest_at(hello_at) else {
        unreachable!("matched above")
    };
    assert_eq!(
        pod_id, POD_ID,
        "the accepted Hello must carry the identity the handshake authenticated"
    );

    pod.send(StreamerMsg::VadOpened {
        write_head: RING_HISTORY_SAMPLES,
    });
    let opened_at = host.wait_for("the segment opening", hello_at + 1, |e| {
        matches!(e, HostEvent::Ingest(_, SessionEvent::SegmentOpened { .. }))
    });
    let SessionEvent::SegmentOpened {
        segment_id,
        base_sample_index,
        preroll_samples,
        base_device_ts,
        is_resume,
        ..
    } = host.ingest_at(opened_at)
    else {
        unreachable!("matched above")
    };
    assert_eq!(*segment_id, 0, "the pod's first segment is numbered 0");
    assert_eq!(
        *base_sample_index, 0,
        "a ring holding less than the pre-roll opens at its oldest sample"
    );
    assert_eq!(
        u64::from(*preroll_samples),
        RING_HISTORY_SAMPLES,
        "the wire pre-roll is the history actually held, not the history requested"
    );
    assert!(!is_resume, "nothing was truncated before this");
    // Back-extrapolated from the ring's anchor at the shared sample rate. The
    // value is fully determined by the fixture, so it is asserted exactly: a wrong
    // divisor or an off-by-N in the anchor delta moves the whole utterance on the
    // host's timeline, and a window wide enough to be safe would admit both.
    let base_ts = base_device_ts.0;
    let anchor_delta_samples = RING_HISTORY_SAMPLES - 1;
    let expected_base_ts =
        DEVICE_ANCHOR_TS_US - anchor_delta_samples * 1_000_000 / u64::from(SAMPLE_RATE_HZ);
    assert_eq!(
        base_ts, expected_base_ts,
        "the segment base dates the oldest pre-roll sample, {anchor_delta_samples} samples \
         before the ring's anchor"
    );

    // Dated relative to the segment base so the offset the FSM derives is exact.
    pod.send(StreamerMsg::Telemetry(WireTelemetry {
        device_ts_us: base_ts + TELEMETRY_OFFSET_US,
        kind: TelemetryKind::SpEnergy { values: SPENERGY },
    }));

    // Let the whole-frame backlog go out under an open gate before releasing the
    // VAD, so the close carries only the residual partial frame — the production
    // shape of an utterance's end.
    let last_full = host.wait_for("the second audio frame", opened_at + 1, |e| {
        matches!(
            e,
            HostEvent::Ingest(_, SessionEvent::Audio { first_sample_index, .. })
                if *first_sample_index == FRAME_SAMPLES
        )
    });
    pod.send(StreamerMsg::VadClosed);

    let closed_at = host.wait_for("the segment closing", last_full + 1, |e| {
        matches!(e, HostEvent::Ingest(_, SessionEvent::SegmentClosed { .. }))
    });
    let SessionEvent::SegmentClosed { close, .. } = host.ingest_at(closed_at) else {
        unreachable!("matched above")
    };
    assert_eq!(
        *close,
        SegmentClose::Completed {
            end_reason: EndReason::VadRelease,
            frames_sent: 3,
            samples_sent: RING_HISTORY_SAMPLES,
            cross_check: CrossCheck::Match,
        },
        "the device's own totals must agree with what the host counted"
    );

    let outcome = pod.finish();
    host.drain();
    host.stop();

    let audio: Vec<(u64, Vec<i16>)> = ingest_events(&host.seen)
        .into_iter()
        .filter_map(|ev| match ev {
            SessionEvent::Audio {
                first_sample_index,
                pcm,
                gap,
                ..
            } => {
                assert!(gap.is_none(), "a single-segment drain has no gaps: {gap:?}");
                Some((*first_sample_index, pcm.clone()))
            }
            _ => None,
        })
        .collect();
    let expected: Vec<(u64, Vec<i16>)> = vec![
        (0, sample_run(0, FRAME_SAMPLES)),
        (FRAME_SAMPLES, sample_run(FRAME_SAMPLES, FRAME_SAMPLES)),
        (
            2 * FRAME_SAMPLES,
            sample_run(2 * FRAME_SAMPLES, RING_HISTORY_SAMPLES - 2 * FRAME_SAMPLES),
        ),
    ];
    assert_eq!(
        audio, expected,
        "every captured sample must arrive once, in order, at its own index"
    );

    let telemetry: Vec<(i64, TelemetryKind)> = ingest_events(&host.seen)
        .into_iter()
        .filter_map(|ev| match ev {
            SessionEvent::Telemetry {
                sample_offset,
                kind,
                ..
            } => Some((*sample_offset, *kind)),
            _ => None,
        })
        .collect();
    assert_eq!(
        telemetry,
        vec![(
            TELEMETRY_OFFSET_SAMPLES,
            TelemetryKind::SpEnergy { values: SPENERGY }
        )],
        "the reading must land at the sample its device timestamp names"
    );

    assert_eq!(
        outcome.exit,
        StreamerExit::ChannelDisconnected,
        "the loop ends only when its telemetry channel goes away"
    );
    assert_eq!(
        outcome.obs.first(),
        Some(&(0, "start")),
        "the segment's open waypoint fires before its first wake: {:?}",
        outcome.obs
    );
    assert_eq!(
        outcome
            .obs
            .iter()
            .filter(|(_, token)| *token == "preroll-drained")
            .count(),
        1,
        "the backlog reached steady state under an open gate, once: {:?}",
        outcome.obs
    );
    assert!(
        outcome.obs.iter().all(|(id, _)| *id == 0),
        "every reading belongs to the one segment that ran: {:?}",
        outcome.obs
    );
    assert!(
        !outcome
            .obs
            .iter()
            .any(|(_, token)| *token == "telemetry-dropped"),
        "one reading in an open segment is nowhere near the queue cap: {:?}",
        outcome.obs
    );
}

/// Playback round-trip: server Hello validated, audio PCM reaches the sink
/// byte-for-byte, and `EndOfAudio` arrives as a control signal.
///
/// The pod is idle throughout — no segment — which is the production case that
/// matters: the socket is always open, so TTS playback does not wait for the
/// next utterance.
#[test]
fn host_playback_round_trips_into_the_pods_sink() {
    const PLAYBACK_FRAMES: u64 = 3;
    const PLAYBACK_SAMPLES: u64 = 160;

    let mut playback = vec![playback_hello()];
    for i in 0..PLAYBACK_FRAMES {
        playback.push(playback_audio(i * PLAYBACK_SAMPLES, PLAYBACK_SAMPLES));
    }
    playback.push(StreamFrame::EndOfAudio(EndOfAudio {}));

    let mut host = HostPeer::start(HostScript {
        playback,
        close_after_hello: false,
    });
    let pod = start_pod(host.addr, PSK);

    host.wait_for("the pod's Hello", 0, |e| {
        matches!(e, HostEvent::Ingest(_, SessionEvent::HelloAccepted { .. }))
    });
    pod.wait_for_playback(PLAYBACK_FRAMES as u32, 1);
    let outcome = pod.finish();
    host.stop();

    let expected: Vec<Vec<u8>> = (0..PLAYBACK_FRAMES)
        .map(|i| {
            sample_run(i * PLAYBACK_SAMPLES, PLAYBACK_SAMPLES)
                .into_iter()
                .flat_map(|s| s.to_le_bytes())
                .collect()
        })
        .collect();
    assert_eq!(
        outcome.sink.pcm, expected,
        "each playback frame's PCM must reach the sink intact and in order"
    );
    assert_eq!(
        outcome.sink.end_of_audio, 1,
        "EndOfAudio is a control signal on the sink, not a frame of audio"
    );
    assert_eq!(
        outcome.sink.flushes, 0,
        "nothing barged in, so nothing was flushed"
    );
    assert_eq!(
        outcome.sink.stream_resets, 1,
        "the one connection marks exactly one stream boundary"
    );
    assert_eq!(
        outcome.inbound_hellos,
        vec![DEVICE_PLAYBACK_FORMAT],
        "the accepted playback format is the device's own"
    );
    assert!(
        outcome
            .inbound_waypoints
            .contains(&(InboundWaypoint::Periodic, 1)),
        "the post-Hello playback window is sampled at its first frame: {:?}",
        outcome.inbound_waypoints
    );
    assert_eq!(outcome.exit, StreamerExit::ChannelDisconnected);
}

/// A pod whose key the host does not hold never reaches the ingest FSM: the
/// failure is in the handshake, not in a session that comes up and then cannot
/// carry audio. The pod keeps idling and paces its retries rather than dying.
///
/// Both halves of that last claim are asserted, because the opposite regressions
/// are opposite failures in the field: a pod that parks permanently after one
/// refusal never comes back when the host's key file is fixed, and one that
/// retries at line rate turns a single misprovisioned pod into a host-wide load
/// problem.
#[test]
fn a_pod_with_the_wrong_key_never_reaches_the_fsm() {
    let mut host = HostPeer::start(HostScript::default());
    let pod = start_pod(host.addr, [0xa5; PSK_LEN]);

    let first = host.wait_for("a refused handshake", 0, |e| {
        matches!(e, HostEvent::HandshakeFailed(0))
    });
    let refused_at = Instant::now();
    host.wait_for("a second attempt", first + 1, |e| {
        matches!(e, HostEvent::HandshakeFailed(1))
    });
    let spacing = refused_at.elapsed();
    let outcome = pod.finish();
    host.drain();
    host.stop();

    // The backoff after one failure is ~3-5 s of whole seconds; the floor here is
    // low enough to survive the second-granularity truncation and still be far
    // above the retry-at-line-rate shape it exists to catch.
    assert!(
        spacing >= Duration::from_secs(1),
        "the retry must be paced, not immediate; the second attempt came after {spacing:?}"
    );
    let attempts = host
        .seen
        .iter()
        .filter(|e| matches!(e, HostEvent::HandshakeFailed(_)))
        .count();
    assert!(
        attempts <= 3,
        "a paced pod makes a handful of attempts in {spacing:?}, not {attempts}"
    );

    assert!(
        ingest_events(&host.seen).is_empty(),
        "no frame may reach the FSM without a session: {:?}",
        host.seen
    );
    assert!(
        !host
            .seen
            .iter()
            .any(|e| matches!(e, HostEvent::Accepted(_))),
        "no session came up: {:?}",
        host.seen
    );
    assert_eq!(
        outcome.exit,
        StreamerExit::ChannelDisconnected,
        "a refused key paces a retry; it does not end the streamer thread"
    );
    assert!(
        outcome.sink.pcm.is_empty() && outcome.sink.stream_resets == 0,
        "a connection that never came up marks no stream boundary"
    );
}

/// A host that drops the connection after the handshake gets the pod back: the
/// pod notices the loss, paces one backoff, reconnects, and re-introduces itself.
///
/// This is the recovery path production hits constantly (the daemon restarts,
/// the pod does not), and the whole of it runs as shared code over the Linux
/// transport.
#[test]
fn a_dropped_connection_is_replaced_and_re_introduced() {
    let mut host = HostPeer::start(HostScript {
        playback: Vec::new(),
        close_after_hello: true,
    });
    let pod = start_pod(host.addr, PSK);

    let first = host.wait_for("the first Hello", 0, |e| {
        matches!(e, HostEvent::Ingest(0, SessionEvent::HelloAccepted { .. }))
    });
    let second = host.wait_for("the Hello after reconnecting", first + 1, |e| {
        matches!(e, HostEvent::Ingest(1, SessionEvent::HelloAccepted { .. }))
    });
    let SessionEvent::HelloAccepted { pod_id, .. } = host.ingest_at(second) else {
        unreachable!("matched above")
    };
    assert_eq!(
        pod_id, POD_ID,
        "the replacement connection introduces the same pod"
    );

    let outcome = pod.finish();
    host.drain();
    host.stop();

    assert!(
        host.seen
            .iter()
            .any(|e| matches!(e, HostEvent::Closed(0, CloseCause::Superseded))),
        "the first connection really was dropped by the host: {:?}",
        host.seen
    );
    assert_eq!(
        outcome.sink.stream_resets, 2,
        "each installed socket marks its own stream boundary"
    );
    assert!(
        outcome.sink.end_of_audio >= 1,
        "the lost socket's banked playback is ended before the next one's begins"
    );
    assert_eq!(outcome.exit, StreamerExit::ChannelDisconnected);
}
