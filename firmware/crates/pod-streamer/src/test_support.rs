//! Scripted stand-ins for the crate's platform seams, shared by the module tests.
//!
//! The segment engine and the streamer loop drive the same three seams — a byte
//! link, a `poll` shim, a playback sink — so their fakes live here rather than
//! once per test module. Every one is scripted in both directions: what it hands
//! back is a test input, and what it recorded is a test assertion.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::io;
use std::rc::Rc;
use std::sync::Mutex;
use std::time::Duration;

use audio_pipeline::inbound::{
    FrameAccumulator, InboundConnectionState, InboundObserver, InboundWaypoint, NoInboundObs,
    drain_inbound,
};
use audio_pipeline::pace::CATCH_UP_PACED_FRAME_US;
use audio_pipeline::playback::{Accepted, PlaybackSink};
use audio_pipeline::ring::{CaptureRing, RING_CAPACITY_SAMPLES};
use audio_pipeline::wire::{
    AUDIO_PROTOCOL_VERSION, AudioFrame, ChannelSource, DEVICE_PLAYBACK_FORMAT, Hello,
    MAX_AUDIO_PAYLOAD, MAX_FRAME_BYTES, PlaybackFormat, StreamFrame, decode_frame, encode_frame,
};

use crate::link::{LinkStream, PollInterest};
use crate::netpoll::{NetPoll, Readiness};

/// An in-memory link with both directions scripted.
///
/// Writes consume `write_script` (bytes accepted per call, `0` = `WouldBlock`)
/// and fall back to `write_default` once it runs out; reads hand back
/// `read_queue` in `read_chunk`-sized bites and `WouldBlock` when it is empty.
pub(crate) struct FakeLink {
    pub(crate) written: Vec<u8>,
    pub(crate) write_script: VecDeque<usize>,
    /// Bytes accepted per `write` past the script: `usize::MAX` takes the whole
    /// offer, `0` blocks for the rest of the run.
    pub(crate) write_default: usize,
    /// Writes past this many fail outright — a socket that died mid-connection,
    /// which is a different thing from one that only ever blocks.
    pub(crate) write_err_after: Option<u32>,
    pub(crate) writes: u32,
    pub(crate) read_queue: VecDeque<u8>,
    /// Upper bound on bytes one `read` returns, so a test can meter frames in.
    pub(crate) read_chunk: usize,
    pub(crate) reads: u32,
    /// What [`LinkStream::buffers_plaintext`] reports.
    pub(crate) plaintext: bool,
    /// Mirror of every accepted byte, outliving the link. See [`tapped_link`].
    pub(crate) tap: Option<Wire>,
}

/// A mirror of one link's outbound bytes that a test keeps after the link is
/// dropped.
pub(crate) type Wire = Rc<RefCell<Vec<u8>>>;

/// A link plus its wire mirror. The streamer loop owns its sockets, so a caller
/// that replaces one cannot read `FakeLink::written` afterwards; the tap is what
/// a test asserts a dropped connection's traffic on.
pub(crate) fn tapped_link(link: FakeLink) -> (FakeLink, Wire) {
    let wire: Wire = Rc::new(RefCell::new(Vec::new()));
    (
        FakeLink {
            tap: Some(Rc::clone(&wire)),
            ..link
        },
        wire,
    )
}

impl FakeLink {
    /// Accepts every write whole, has nothing to read, buffers no plaintext.
    pub(crate) fn new() -> Self {
        Self {
            written: Vec::new(),
            write_script: VecDeque::new(),
            write_default: usize::MAX,
            write_err_after: None,
            writes: 0,
            read_queue: VecDeque::new(),
            read_chunk: usize::MAX,
            reads: 0,
            plaintext: false,
            tap: None,
        }
    }

    /// A link that takes `ok` whole writes and then fails hard.
    pub(crate) fn dying_after(ok: u32) -> Self {
        Self {
            write_err_after: Some(ok),
            ..Self::new()
        }
    }

    /// Queue peer bytes for the inbound direction to read.
    pub(crate) fn queue_read(&mut self, bytes: &[u8]) {
        self.read_queue.extend(bytes.iter().copied());
    }
}

impl io::Read for FakeLink {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reads += 1;
        if self.read_queue.is_empty() {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        let n = buf.len().min(self.read_chunk).min(self.read_queue.len());
        for slot in buf[..n].iter_mut() {
            *slot = self.read_queue.pop_front().expect("n is within the queue");
        }
        Ok(n)
    }
}

impl io::Write for FakeLink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writes += 1;
        if self.write_err_after.is_some_and(|ok| self.writes > ok) {
            return Err(io::Error::other("scripted write error"));
        }
        let allowed = self.write_script.pop_front().unwrap_or(self.write_default);
        if allowed == 0 {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        let n = allowed.min(buf.len());
        self.written.extend_from_slice(&buf[..n]);
        if let Some(tap) = &self.tap {
            tap.borrow_mut().extend_from_slice(&buf[..n]);
        }
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl LinkStream for FakeLink {
    fn link_fd(&self) -> std::os::fd::RawFd {
        // Never handed to a real `poll`: the fake shim below only records it.
        42
    }

    fn poll_interest(&self, readable: bool, writable: bool) -> PollInterest {
        PollInterest {
            read: readable,
            write: writable,
        }
    }

    fn buffers_plaintext(&self) -> bool {
        self.plaintext
    }

    fn as_read(&mut self) -> &mut dyn io::Read {
        self
    }

    fn as_write(&mut self) -> &mut dyn io::Write {
        self
    }
}

/// A `poll` shim that reports writable (and readable for the first
/// `readable_wakes` wakes) for `fault_after` wakes and then a socket fault — the
/// fault is how a test ends a segment whose VAD never closes.
pub(crate) struct FakePoll {
    pub(crate) wakes: Cell<u32>,
    fault_after: u32,
    /// Wakes that report readable, counted from the first: `0` never, `u32::MAX`
    /// always. Scripted rather than fixed so a test can hand the loop one
    /// readable wake and then require any further drain to be driven by
    /// something other than POLLIN.
    readable_wakes: u32,
    pub(crate) seen: RefCell<Vec<(std::os::fd::RawFd, PollInterest, Duration)>>,
}

impl FakePoll {
    pub(crate) fn faulting_after(fault_after: u32) -> Self {
        Self {
            wakes: Cell::new(0),
            fault_after,
            readable_wakes: 0,
            seen: RefCell::new(Vec::new()),
        }
    }

    /// Reports both directions ready, so the inbound gate opens on POLLIN.
    pub(crate) fn readable_faulting_after(fault_after: u32) -> Self {
        Self {
            readable_wakes: u32::MAX,
            ..Self::faulting_after(fault_after)
        }
    }

    /// Reports readable on the first `wakes` wakes only, writable throughout, and
    /// never faults.
    pub(crate) fn readable_for(wakes: u32) -> Self {
        Self {
            readable_wakes: wakes,
            ..Self::always_writable()
        }
    }

    /// Enough wakes that a completing segment never reaches the fault.
    pub(crate) fn always_writable() -> Self {
        Self::faulting_after(u32::MAX)
    }

    /// The interest armed at each recorded wake, in order.
    pub(crate) fn write_arming(&self) -> Vec<bool> {
        self.seen.borrow().iter().map(|s| s.1.write).collect()
    }
}

impl NetPoll for FakePoll {
    fn poll_readiness(
        &self,
        fd: std::os::fd::RawFd,
        interest: PollInterest,
        timeout: Duration,
    ) -> io::Result<Readiness> {
        self.seen.borrow_mut().push((fd, interest, timeout));
        let wakes = self.wakes.get() + 1;
        self.wakes.set(wakes);
        if wakes > self.fault_after {
            return Ok(Readiness::Fault(io::Error::other("scripted poll fault")));
        }
        Ok(Readiness::Ready {
            readable: wakes <= self.readable_wakes,
            writable: true,
        })
    }
}

/// Playback sink that records the length of every offer and can refuse them.
#[derive(Default)]
pub(crate) struct RecordingSink {
    pub(crate) offers: Vec<usize>,
    /// While set, every offer is refused with [`Accepted::Full`].
    pub(crate) full: bool,
    /// Offers refused before the sink starts accepting — a ring with no free slot
    /// that frees one after this many attempts. Unlike [`full`](Self::full) it
    /// expires by itself, which is the only way a caller that borrows the sink for
    /// a whole loop run can see backpressure lift mid-run.
    pub(crate) refuse_first: u32,
    refusals: u32,
    /// Stream-boundary signals in the order they arrived.
    pub(crate) marks: Vec<SinkMark>,
}

/// A stream-boundary signal a [`RecordingSink`] was handed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SinkMark {
    EndOfAudio,
    FlushPlayback,
    StreamReset,
}

impl RecordingSink {
    pub(crate) fn new() -> Self {
        Self {
            offers: Vec::new(),
            full: false,
            refuse_first: 0,
            refusals: 0,
            marks: Vec::new(),
        }
    }
}

impl PlaybackSink for RecordingSink {
    fn accept(&mut self, pcm: &[u8]) -> Accepted {
        self.offers.push(pcm.len());
        if self.full || self.refusals < self.refuse_first {
            self.refusals = self.refusals.saturating_add(1);
            Accepted::Full
        } else {
            Accepted::Enqueued
        }
    }

    fn end_of_audio(&mut self) {
        self.marks.push(SinkMark::EndOfAudio);
    }

    fn flush_playback(&mut self) {
        self.marks.push(SinkMark::FlushPlayback);
    }

    fn stream_reset(&mut self) {
        self.marks.push(SinkMark::StreamReset);
    }
}

/// Records the inbound path's observability calls so a caller's use of the seam —
/// not just the seam's own gating rule — can be asserted.
#[derive(Default)]
pub(crate) struct RecordingInboundObs {
    pub(crate) waypoints: Vec<(InboundWaypoint, u32)>,
    pub(crate) hellos: Vec<PlaybackFormat>,
}

impl RecordingInboundObs {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl InboundObserver for RecordingInboundObs {
    fn waypoint(&mut self, site: InboundWaypoint, frame: u32) {
        self.waypoints.push((site, frame));
    }

    fn hello_ok(&mut self, format: PlaybackFormat) {
        self.hellos.push(format);
    }
}

// ── Wire and ring fixtures ────────────────────────────────────────────────────

/// A ring holding `written` samples of history, anchored at `anchor_ts_us`.
pub(crate) fn ring_with(written: u64, anchor_ts_us: u64) -> Mutex<Option<CaptureRing<Box<[i16]>>>> {
    let mut samples = vec![0i16; RING_CAPACITY_SAMPLES].into_boxed_slice();
    for i in 0..written {
        samples[(i % RING_CAPACITY_SAMPLES as u64) as usize] = (i % 4096) as i16;
    }
    Mutex::new(Some(CaptureRing {
        samples,
        write_head: written,
        anchor_sample: written.saturating_sub(1),
        anchor_ts_us,
    }))
}

/// Split a written byte stream back into whole frames, returning the length of
/// any incomplete trailing frame (a mid-tail stall leaves one behind).
pub(crate) fn decode_stream(bytes: &[u8]) -> (Vec<StreamFrame>, usize) {
    let mut frames = Vec::new();
    let mut at = 0usize;
    while at + 2 <= bytes.len() {
        let len = u16::from_le_bytes([bytes[at], bytes[at + 1]]) as usize;
        if at + 2 + len > bytes.len() {
            break;
        }
        frames.push(decode_frame(&bytes[at..at + 2 + len]).expect("framed frame decodes"));
        at += 2 + len;
    }
    (frames, bytes.len() - at)
}

/// Length-prefixed wire bytes for a frame, as the peer would send them.
pub(crate) fn wire_bytes(frame: &StreamFrame) -> Vec<u8> {
    let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
    let n = encode_frame(frame, &mut buf).expect("test frame encodes");
    buf.truncate(n);
    buf
}

/// An inbound Hello declaring exactly the format the device accepts.
pub(crate) fn inbound_hello() -> StreamFrame {
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

/// An inbound Audio frame of `samples` silence samples.
pub(crate) fn inbound_audio(samples: usize) -> StreamFrame {
    let mut pcm: heapless::Vec<u8, MAX_AUDIO_PAYLOAD> = heapless::Vec::new();
    for _ in 0..samples * 2 {
        pcm.push(0).expect("pcm fits MAX_AUDIO_PAYLOAD");
    }
    StreamFrame::Audio(AudioFrame {
        segment_id: 0,
        first_sample_index: 0,
        device_ts_us: 0,
        pcm,
    })
}

/// A µs clock advancing well past the pace interval on every read, so the pace
/// gate never defers.
pub(crate) fn free_running_clock() -> impl Fn() -> u64 {
    let ticks = Cell::new(0u64);
    move || {
        let now = 1_000_000 + ticks.get() * CATCH_UP_PACED_FRAME_US * 2;
        ticks.set(ticks.get() + 1);
        now
    }
}

/// A partial frame in the accumulator and a completed handshake in the
/// connection state — the two things a socket replacement must not carry over.
pub(crate) fn dirty_inbound() -> (FrameAccumulator, InboundConnectionState) {
    // The Hello lands whole (so `seen_hello` is set) followed by the first bytes
    // of a second frame, which stay buffered as a partial tail.
    let mut bytes = wire_bytes(&inbound_hello());
    bytes.extend_from_slice(&[0x10, 0x00, 0x01]);

    let mut accum = FrameAccumulator::new();
    let mut state = InboundConnectionState::new();
    let mut sink = RecordingSink::new();
    let mut reader: &[u8] = &bytes;
    drain_inbound(
        &mut reader,
        &mut accum,
        &mut sink,
        &mut state,
        &mut NoInboundObs,
    )
    .expect("a Hello plus a partial tail is not an error");
    assert!(state.seen_hello(), "the Hello must have been observed");
    assert!(
        accum.valid_len() > 0,
        "the partial tail must still be buffered"
    );
    (accum, state)
}
