//! ALSA playback into the XVF3800's USB audio interface.
//!
//! The host's speech arrives as S16 mono at 16 kHz through the shared inbound path,
//! is banked in an SPSC byte ring, and is written out by a drain thread that
//! duplicates each sample into both channels of the board's playback stream.
//!
//! Two rules shape this module:
//!
//! - Playback goes through the **board**, not through some other card the Pi
//!   happens to have. The chip's echo canceller only removes what the chip itself
//!   played, so audio that leaves by another path comes back in the microphones and
//!   the host hears the pod talking to itself.
//! - The ring is the hold buffer, and the drain does not start on the first byte. A
//!   cushion is banked first, so the cold start of a stream survives the delivery
//!   jitter of the link instead of underrunning in its first hundred milliseconds.
//!
//! The banking half (the [`PlaybackSink`] the streamer calls) and the writing half
//! ([`PlaybackDrain`]) share only the ring, so the streamer thread never blocks on
//! the sound card and the sound card never waits on the socket.

use std::fmt;
use std::time::{Duration, Instant};

use alsa::Direction;
use alsa::pcm::PCM;
use audio_pipeline::playback::{
    Accepted, INBOUND_PCM_RING_BYTES, INBOUND_PCM_WRITE_UNIT_BYTES, InboundPcmRing,
    InboundRingConsumer, InboundRingProducer, LogCountdown, PLAYBACK_LOG_CADENCE_FRAMES,
    PlaybackSink, PrerollGate as SharedPrerollGate, WIRE_BYTES_PER_SAMPLE, is_valid_s16le_pcm,
};

use crate::alsa_capture::{
    CAPTURE_PARAMS, CardInfo, PcmError, PcmParams, RecoveryBudget, Retry, enumerate_cards,
    open_pcm, select_card,
};
use crate::config::CHANNELS;

/// The parameters the board's playback stream is opened with — the same ones its
/// capture stream takes, because it is the same card and the same clock.
pub const PLAYBACK_PARAMS: PcmParams = CAPTURE_PARAMS;

/// How long the pre-roll gate waits for its target before giving up and playing
/// whatever is banked, measured from the first bytes of a stream.
///
/// The fallback is what makes a short utterance play at all: a two-frame reply never
/// reaches the target depth, and without a bound it would sit in the ring forever.
/// It starts at the first bytes rather than at the connection, so an open but silent
/// socket does not burn the budget before the host says anything.
pub const PREROLL_MAX_WAIT_MS: u64 = 500;

/// Raw mono bytes the drain moves per pass — two write units, 40 ms.
///
/// A pass is bounded so the loop returns to its own bookkeeping (boundaries,
/// generation changes, the starvation edge) at a steady cadence rather than
/// disappearing into a long burst after a refill.
pub const DRAIN_BYTES_PER_PASS: usize = 2 * INBOUND_PCM_WRITE_UNIT_BYTES;

/// How long the drain loop sleeps when there is nothing to play or the gate is still
/// filling. Half a wire frame: short enough that a stream starts within a frame of
/// its cushion arriving, long enough that an idle pod is not spinning.
pub const IDLE_POLL: Duration = Duration::from_millis(10);

/// Open the playback direction of an already-resolved card.
///
/// Takes the card rather than finding it again so both directions of a running
/// pipeline are provably the same device: resolving twice could pick two cards if
/// something else enumerated in between, and the echo canceller would then be
/// cancelling audio that was never played.
pub fn open_playback_on(card: &CardInfo) -> Result<PCM, PcmError> {
    let pcm = open_pcm(&card.hw_device(), Direction::Playback, &PLAYBACK_PARAMS)?;
    log::info!(
        "playback: {card} at {} on {}",
        PLAYBACK_PARAMS,
        card.hw_device()
    );
    Ok(pcm)
}

/// Find the board by name and open its playback stream.
pub fn open_playback() -> Result<(CardInfo, PCM), PcmError> {
    let cards = enumerate_cards().map_err(PcmError::Enumerate)?;
    let card = select_card(&cards).map_err(PcmError::NoCard)?.clone();
    let pcm = open_playback_on(&card)?;
    Ok((card, pcm))
}

/// Expand raw S16_LE mono wire bytes into interleaved stereo frames, returning the
/// frames written.
///
/// The same sample goes to both channels. The board drives one speaker per channel
/// and the host sends one beam of speech; splitting it or silencing a side would
/// halve the level for no gain.
///
/// # Panics
/// Panics if `raw` is not a whole number of samples or `out` cannot hold the frames.
/// Both would byte-shift the framing, which does not read as a glitch — it reads as
/// full-scale noise out of the speakers.
pub fn expand_mono_to_stereo(raw: &[u8], out: &mut [i16]) -> usize {
    assert!(
        raw.len().is_multiple_of(WIRE_BYTES_PER_SAMPLE),
        "playback: a run of {} bytes is not a whole number of S16 samples",
        raw.len()
    );
    let frames = raw.len() / WIRE_BYTES_PER_SAMPLE;
    assert!(
        out.len() >= frames * CHANNELS,
        "playback: {} slots cannot hold {frames} stereo frames",
        out.len()
    );
    for (frame, sample) in raw.chunks_exact(WIRE_BYTES_PER_SAMPLE).enumerate() {
        let value = i16::from_le_bytes([sample[0], sample[1]]);
        for channel in 0..CHANNELS {
            out[frame * CHANNELS + channel] = value;
        }
    }
    frames
}

/// Why a write did not place its frames.
#[derive(Debug)]
pub enum PlaybackFault {
    /// The device underran and was recovered. The frames were not taken; the caller
    /// rebuilds its cushion and tries again.
    Underrun,
    /// The stream cannot continue. The process exits on this and is restarted, which
    /// is the only recovery a vanished USB device has.
    Fatal(String),
}

impl fmt::Display for PlaybackFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Underrun => write!(f, "the device underran"),
            Self::Fatal(why) => write!(f, "playback stopped: {why}"),
        }
    }
}

impl std::error::Error for PlaybackFault {}

/// Somewhere interleaved stereo frames go — the seam between the drain's bookkeeping
/// and the sound card, so the former is testable without the latter.
pub trait StereoOut {
    /// Write `samples` (interleaved, [`CHANNELS`] per frame), returning the frames
    /// accepted, which may be short of what was offered.
    fn write_frames(&mut self, samples: &[i16]) -> Result<usize, PlaybackFault>;
}

/// The board's playback stream, with the underrun handling ALSA requires.
pub struct AlsaOut<'a> {
    io: alsa::pcm::IO<'a, i16>,
    pcm: &'a PCM,
    budget: RecoveryBudget,
}

impl<'a> AlsaOut<'a> {
    /// Wrap an open playback PCM.
    pub fn new(pcm: &'a PCM) -> Result<Self, PcmError> {
        let io = pcm.io_i16().map_err(|e| PcmError::Stream {
            reason: format!("the device will not give a 16-bit interleaved writer: {e}"),
        })?;
        Ok(Self {
            io,
            pcm,
            budget: RecoveryBudget::new(),
        })
    }

    /// Underruns recovered from since the stream opened.
    pub fn recoveries(&self) -> u64 {
        self.budget.recoveries()
    }
}

impl StereoOut for AlsaOut<'_> {
    fn write_frames(&mut self, samples: &[i16]) -> Result<usize, PlaybackFault> {
        loop {
            match self.io.writei(samples) {
                Ok(frames) => {
                    self.budget.note_success();
                    return Ok(frames);
                }
                Err(e) => match self.budget.note_failure("playback", self.pcm, e) {
                    // A signal woke the thread; the stream is untouched and the
                    // frames are still ours to place.
                    Ok(Retry::Interrupted) => continue,
                    // Recovered: the frames were not taken, so they go back to the
                    // caller, which owns rebuilding the cushion before offering
                    // them again.
                    Ok(Retry::Recovered) => return Err(PlaybackFault::Underrun),
                    Err(reason) => return Err(PlaybackFault::Fatal(reason)),
                },
            }
        }
    }
}

/// The [`PlaybackSink`] the streamer hands every inbound audio frame to.
///
/// It banks raw wire bytes and returns; the expansion to stereo happens on the drain
/// thread. The streamer thread also carries microphone capture toward the host, so
/// nothing here may block on the sound card.
pub struct RingSink {
    producer: InboundRingProducer,
    frames: u32,
    samples: u64,
    /// Times the ring had no room. Not lost audio: the frame stays with the caller,
    /// which stops reading the socket until a slot frees, so the backpressure reaches
    /// the host as TCP flow control rather than as a gap.
    full_stalls: u32,
    log_countdown: LogCountdown,
}

impl RingSink {
    /// Wrap the write end of a playback ring.
    pub fn new(producer: InboundRingProducer) -> Self {
        Self {
            producer,
            frames: 0,
            samples: 0,
            full_stalls: 0,
            log_countdown: LogCountdown::new(PLAYBACK_LOG_CADENCE_FRAMES),
        }
    }

    /// Frames banked since the sink was built.
    pub fn frames(&self) -> u32 {
        self.frames
    }

    /// Times a frame found no room.
    pub fn full_stalls(&self) -> u32 {
        self.full_stalls
    }

    fn tick_log(&mut self) {
        if self.log_countdown.tick() {
            log::info!(
                "playback sink: frames={} samples={} full_stalls={}",
                self.frames,
                self.samples,
                self.full_stalls
            );
        }
    }
}

impl PlaybackSink for RingSink {
    fn accept(&mut self, pcm: &[u8]) -> Accepted {
        if !is_valid_s16le_pcm(pcm) {
            log::warn!(
                "playback sink: inbound frame carries {} bytes, which is not whole S16 samples — \
                 discarding",
                pcm.len()
            );
            // Not retryable, so the caller must advance past it rather than hold it.
            return Accepted::Enqueued;
        }
        let banked = self.producer.write(pcm.len(), |offset, dst| {
            dst.copy_from_slice(&pcm[offset..offset + dst.len()]);
        });
        if !banked {
            self.full_stalls = self.full_stalls.wrapping_add(1);
            // Logged on this path too: under sustained backpressure it is the only
            // path, and going silent during the overload is the opposite of useful.
            self.tick_log();
            return Accepted::Full;
        }
        self.frames = self.frames.wrapping_add(1);
        self.samples = self
            .samples
            .wrapping_add((pcm.len() / WIRE_BYTES_PER_SAMPLE) as u64);
        self.tick_log();
        Accepted::Enqueued
    }

    fn end_of_audio(&mut self) {
        // A mark at the current head, so the boundary is observed when the banked
        // tail has finished playing rather than when the host said it was done.
        self.producer.mark_end_of_audio();
    }

    fn flush_playback(&mut self) {
        // Barge-in: drop what is banked and mark the emptied ring, so the drain sees
        // a boundary immediately instead of playing out audio the user interrupted.
        self.producer.reset();
        self.producer.mark_end_of_audio();
    }

    fn stream_reset(&mut self) {
        self.producer.reset();
    }
}

/// This pod's entry points onto the shared pre-roll gate.
///
/// The state and its arming rules belong to [`audio_pipeline::playback::PrerollGate`],
/// which the ESP pod arms too; what is here is the way *this* drain reaches them — one
/// fallback bound baked in, and the two edges a drain loop over a byte ring observes.
#[derive(Default)]
pub struct PrerollGate(SharedPrerollGate);

impl PrerollGate {
    /// A gate armed at the base target, with no stream seen yet.
    pub fn new() -> Self {
        Self(SharedPrerollGate::new())
    }

    /// The depth currently being waited for.
    pub fn target(&self) -> usize {
        self.0.target()
    }

    /// Whether audio is being held rather than played.
    pub fn pending(&self) -> bool {
        self.0.pending()
    }

    /// Re-arm at the base target: a fresh stream, whose predecessor's difficulties
    /// say nothing about it.
    pub fn arm_base(&mut self) {
        self.0.arm_base();
    }

    /// Re-arm with a deeper target after an underrun, returning the new target.
    pub fn rearm_escalated(&mut self) -> usize {
        self.0.rearm_escalated()
    }

    /// May the drain play, given `available` banked bytes?
    ///
    /// This drain sees every non-empty poll here, so this is also where the audio edge
    /// [`note_empty`](Self::note_empty) reads is set.
    pub fn admit(&mut self, available: usize, now: Instant) -> bool {
        if available > 0 {
            self.0.note_audio_seen();
        }
        self.0.admit(available, now, PREROLL_MAX_WAIT_MS)
    }

    /// The ring ran dry. Returns the escalated target if this was a mid-stream
    /// underrun, or `None` if the pod was simply idle.
    ///
    /// An empty ring is only an underrun when audio was playing out of it: between
    /// utterances the ring is empty for seconds at a time, and treating that as a
    /// fault would escalate the target until every reply started a second late.
    pub fn note_empty(&mut self) -> Option<usize> {
        if self.0.pending() || !self.0.saw_audio() {
            self.0.clear_audio_seen();
            return None;
        }
        Some(self.0.rearm_escalated())
    }
}

/// What one drain pass did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassOutcome {
    /// Nothing banked.
    Idle,
    /// Audio is banked but held while the cushion fills.
    Filling,
    /// Frames handed to the device.
    Played(usize),
    /// Frames handed to the device, and the end of a stream reached; the next one
    /// pre-rolls from scratch.
    Boundary(usize),
}

/// The read end of the playback ring and everything that decides what leaves it.
pub struct PlaybackDrain {
    consumer: InboundRingConsumer,
    gate: PrerollGate,
    /// The ring generation this drain has already acted on. A change means the
    /// connection was replaced and the banked tail belongs to a dead socket.
    acted_generation: u32,
    /// One write unit of raw mono bytes, reused every pass.
    raw: Vec<u8>,
    /// The same run expanded to interleaved stereo, reused every pass.
    stereo: Vec<i16>,
    played_frames: u64,
    underruns: u64,
    boundaries: u64,
}

impl PlaybackDrain {
    /// Wrap the read end of a playback ring.
    pub fn new(consumer: InboundRingConsumer) -> Self {
        let acted_generation = consumer.generation();
        Self {
            consumer,
            gate: PrerollGate::new(),
            acted_generation,
            raw: vec![0u8; INBOUND_PCM_WRITE_UNIT_BYTES],
            stereo: vec![0i16; INBOUND_PCM_WRITE_UNIT_BYTES / WIRE_BYTES_PER_SAMPLE * CHANNELS],
            played_frames: 0,
            underruns: 0,
            boundaries: 0,
        }
    }

    /// Frames written to the device since the drain started.
    pub fn played_frames(&self) -> u64 {
        self.played_frames
    }

    /// Mid-stream underruns seen.
    pub fn underruns(&self) -> u64 {
        self.underruns
    }

    /// Stream boundaries played out.
    pub fn boundaries(&self) -> u64 {
        self.boundaries
    }

    /// The gate, for a caller that wants to report what depth is being waited for.
    pub fn gate(&self) -> &PrerollGate {
        &self.gate
    }

    /// Move at most [`DRAIN_BYTES_PER_PASS`] from the ring to the device.
    pub fn pass(
        &mut self,
        out: &mut dyn StereoOut,
        now: Instant,
    ) -> Result<PassOutcome, PlaybackFault> {
        // A replaced connection first: its predecessor's banked audio must not play
        // out under the new one, and the jump is what drops it race-free.
        let generation = self.consumer.generation();
        if generation != self.acted_generation {
            self.acted_generation = self.consumer.apply_reset();
            self.gate.arm_base();
        }

        // A boundary sitting on an empty ring — a flush, or a stream that ended after
        // the tail had already played. It carries no audio, so it is observed here
        // rather than by a run.
        if self.consumer.take_mark_at_tail() {
            self.boundaries += 1;
            self.gate.arm_base();
            return Ok(PassOutcome::Boundary(0));
        }

        let available = self.consumer.available();
        if available == 0 {
            if let Some(target) = self.gate.note_empty() {
                self.underruns += 1;
                log::warn!(
                    "playback: the ring ran dry mid-stream ({} underrun(s)); rebuilding to {target} \
                     bytes before resuming",
                    self.underruns
                );
            }
            return Ok(PassOutcome::Idle);
        }
        if !self.gate.admit(available, now) {
            return Ok(PassOutcome::Filling);
        }

        let mut budget = DRAIN_BYTES_PER_PASS;
        let mut played = 0usize;
        let mut boundary = false;
        while budget > 0 {
            let run = self
                .consumer
                .copy_run_into(budget.min(self.raw.len()), &mut self.raw);
            if run.n == 0 {
                break;
            }
            let frames = expand_mono_to_stereo(&self.raw[..run.n], &mut self.stereo);
            let accepted = match out.write_frames(&self.stereo[..frames * CHANNELS]) {
                Ok(accepted) => accepted,
                Err(PlaybackFault::Underrun) => {
                    self.underruns += 1;
                    let target = self.gate.rearm_escalated();
                    log::warn!(
                        "playback: the device underran ({} total); rebuilding to {target} bytes \
                         before resuming",
                        self.underruns
                    );
                    return Ok(PassOutcome::Played(played));
                }
                Err(fatal) => return Err(fatal),
            };
            // Only what the device took is consumed; a short write leaves the rest
            // banked for the next pass rather than dropping it to keep the loop tidy.
            let bytes = accepted * WIRE_BYTES_PER_SAMPLE;
            let crossed = self.consumer.advance(bytes);
            played += accepted;
            self.played_frames += accepted as u64;
            budget -= bytes;
            if crossed {
                // The pass ends on the mark. Whatever is banked behind it belongs to
                // the next stream, and the gate is not re-armed until after this
                // loop — so staging another run here would play the next reply's
                // first frames with no cushion at all and then freeze it to rebuild
                // one, which is an audible split inside its first word.
                boundary = true;
                break;
            }
            if accepted < frames {
                break;
            }
        }

        if boundary {
            self.boundaries += 1;
            self.gate.arm_base();
            return Ok(PassOutcome::Boundary(played));
        }
        Ok(PassOutcome::Played(played))
    }
}

/// Drain until the device stops accepting audio for good.
///
/// The only exit is a fatal fault, which is the intended one: a board that has gone
/// away is recovered by the process exiting and being restarted, not by a re-probe
/// state machine here.
pub fn run_drain_loop(drain: &mut PlaybackDrain, out: &mut dyn StereoOut) -> PlaybackFault {
    loop {
        match drain.pass(out, Instant::now()) {
            Ok(PassOutcome::Idle | PassOutcome::Filling) => std::thread::sleep(IDLE_POLL),
            Ok(_) => {}
            Err(fault) => return fault,
        }
    }
}

/// A fresh playback ring, split into the end the streamer banks into and the end the
/// drain thread plays from.
pub fn playback_pair() -> (RingSink, PlaybackDrain) {
    let (producer, consumer) = InboundPcmRing::new(INBOUND_PCM_RING_BYTES).split();
    (RingSink::new(producer), PlaybackDrain::new(consumer))
}

#[cfg(test)]
mod tests {
    use audio_pipeline::playback::{PLAYBACK_PREROLL_TARGET_BYTES, next_preroll_target};

    use super::*;

    /// A device that records what it was given and can be told to misbehave.
    #[derive(Default)]
    struct FakeOut {
        written: Vec<i16>,
        /// Frames to accept per call, consumed in order; an exhausted script accepts
        /// everything offered.
        accept: std::collections::VecDeque<usize>,
        /// Calls after which an underrun is reported instead of a write.
        underrun_at: Vec<usize>,
        /// Call at which the stream dies for good.
        fatal_at: Option<usize>,
        calls: usize,
    }

    impl FakeOut {
        fn frames(&self) -> usize {
            self.written.len() / CHANNELS
        }

        /// The left channel of everything written, which is the mono stream back.
        fn left(&self) -> Vec<i16> {
            self.written.iter().step_by(CHANNELS).copied().collect()
        }
    }

    impl StereoOut for FakeOut {
        fn write_frames(&mut self, samples: &[i16]) -> Result<usize, PlaybackFault> {
            self.calls += 1;
            if Some(self.calls) == self.fatal_at {
                return Err(PlaybackFault::Fatal("the device went away".into()));
            }
            if self.underrun_at.contains(&self.calls) {
                return Err(PlaybackFault::Underrun);
            }
            let offered = samples.len() / CHANNELS;
            let accepted = self.accept.pop_front().unwrap_or(offered).min(offered);
            self.written
                .extend_from_slice(&samples[..accepted * CHANNELS]);
            Ok(accepted)
        }
    }

    /// `count` mono samples as raw wire bytes, each one its own index so a
    /// reordering or a lost run is visible rather than plausible.
    fn wire(from: i16, count: usize) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(count * WIRE_BYTES_PER_SAMPLE);
        for i in 0..count {
            bytes.extend_from_slice(&(from + i as i16).to_le_bytes());
        }
        bytes
    }

    /// A small ring, so the wrap and full paths are reachable without banking 64 KiB.
    fn small_pair(cap: usize) -> (RingSink, PlaybackDrain) {
        let (producer, consumer) = InboundPcmRing::new(cap).split();
        (RingSink::new(producer), PlaybackDrain::new(consumer))
    }

    /// A clock the test walks forward, so reaching the fallback timeout costs nothing.
    struct Clock(Instant);

    impl Clock {
        fn new() -> Self {
            Self(Instant::now())
        }

        fn now(&self) -> Instant {
            self.0
        }

        fn advance(&mut self, ms: u64) {
            self.0 += Duration::from_millis(ms);
        }
    }

    /// Get past the gate on banked audio that is short of the target: one pass starts
    /// the fallback clock, then the clock is walked to its expiry.
    ///
    /// Audio must already be banked. The clock starts at the first bytes rather than
    /// at the first pass, which is what
    /// `the_fallback_clock_starts_at_the_first_audio_not_at_the_connection` pins.
    fn open_gate(drain: &mut PlaybackDrain, out: &mut FakeOut, clock: &mut Clock) {
        let held = drain.pass(out, clock.now()).expect("no fault scripted");
        assert_eq!(held, PassOutcome::Filling, "the gate holds the first pass");
        clock.advance(PREROLL_MAX_WAIT_MS);
    }

    /// Drive passes until the outcome stops being audio, returning what was seen.
    fn drain_all(drain: &mut PlaybackDrain, out: &mut FakeOut, now: Instant) -> Vec<PassOutcome> {
        let mut seen = Vec::new();
        for _ in 0..64 {
            let outcome = drain.pass(out, now).expect("no fault scripted");
            seen.push(outcome);
            if matches!(outcome, PassOutcome::Idle | PassOutcome::Filling) {
                break;
            }
        }
        seen
    }

    #[test]
    fn every_sample_reaches_both_channels() {
        let mut out = [0i16; 6];
        let frames = expand_mono_to_stereo(&wire(100, 3), &mut out);
        assert_eq!(frames, 3);
        assert_eq!(out, [100, 100, 101, 101, 102, 102]);
    }

    #[test]
    fn an_empty_run_expands_to_nothing_and_leaves_the_buffer_alone() {
        let mut out = [7i16; 2];
        assert_eq!(expand_mono_to_stereo(&[], &mut out), 0);
        assert_eq!(out, [7, 7]);
    }

    #[test]
    #[should_panic(expected = "not a whole number of S16 samples")]
    fn a_half_sample_run_is_a_panic_rather_than_a_byte_shift() {
        let mut out = [0i16; 4];
        expand_mono_to_stereo(&[1, 2, 3], &mut out);
    }

    #[test]
    fn a_banked_frame_plays_back_byte_for_byte() {
        let (mut sink, mut drain) = small_pair(4_096);
        assert_eq!(sink.accept(&wire(1, 8)), Accepted::Enqueued);
        assert_eq!(sink.frames(), 1);

        let mut out = FakeOut::default();
        // Past the fallback, so the gate clears without a full cushion.
        let now = Instant::now();
        drain
            .pass(&mut out, now)
            .expect("first pass starts the clock");
        let later = now + Duration::from_millis(PREROLL_MAX_WAIT_MS);
        drain
            .pass(&mut out, later)
            .expect("the fallback releases it");
        assert_eq!(out.left(), (1..=8).collect::<Vec<i16>>());
    }

    #[test]
    fn a_frame_that_is_not_whole_samples_is_dropped_rather_than_held() {
        let (mut sink, _drain) = small_pair(4_096);
        // Reported as forward progress: the caller cannot fix it by retrying.
        assert_eq!(sink.accept(&[1, 2, 3]), Accepted::Enqueued);
        assert_eq!(sink.accept(&[]), Accepted::Enqueued);
        assert_eq!(sink.frames(), 0, "neither reached the ring");
    }

    #[test]
    fn a_full_ring_refuses_the_frame_instead_of_dropping_it() {
        let (mut sink, mut drain) = small_pair(64);
        assert_eq!(sink.accept(&wire(1, 32)), Accepted::Enqueued);
        assert_eq!(sink.accept(&wire(100, 1)), Accepted::Full);
        assert_eq!(sink.full_stalls(), 1);
        // Nothing of the refused frame is in the ring: the 32 banked samples are all
        // that comes back out.
        let mut out = FakeOut::default();
        let now = Instant::now();
        drain.pass(&mut out, now).expect("pass");
        drain
            .pass(&mut out, now + Duration::from_millis(PREROLL_MAX_WAIT_MS))
            .expect("pass");
        assert_eq!(out.left(), (1..=32).collect::<Vec<i16>>());
    }

    #[test]
    fn audio_is_held_until_the_cushion_is_banked() {
        let (mut sink, mut drain) = small_pair(INBOUND_PCM_RING_BYTES);
        let now = Instant::now();
        let short_of_target = PLAYBACK_PREROLL_TARGET_BYTES - INBOUND_PCM_WRITE_UNIT_BYTES;
        sink.accept(&wire(0, short_of_target / WIRE_BYTES_PER_SAMPLE));

        let mut out = FakeOut::default();
        assert_eq!(
            drain.pass(&mut out, now).expect("pass"),
            PassOutcome::Filling
        );
        assert_eq!(out.frames(), 0, "nothing plays below the target");
        assert!(drain.gate().pending());

        sink.accept(&wire(
            0,
            INBOUND_PCM_WRITE_UNIT_BYTES / WIRE_BYTES_PER_SAMPLE,
        ));
        let outcome = drain.pass(&mut out, now).expect("pass");
        assert!(matches!(outcome, PassOutcome::Played(_)), "{outcome:?}");
        assert!(!drain.gate().pending());
    }

    #[test]
    fn a_stream_too_short_to_reach_the_target_still_plays() {
        let (mut sink, mut drain) = small_pair(INBOUND_PCM_RING_BYTES);
        sink.accept(&wire(1, 16));
        let mut out = FakeOut::default();
        let now = Instant::now();
        assert_eq!(
            drain.pass(&mut out, now).expect("pass"),
            PassOutcome::Filling
        );
        let nearly = now + Duration::from_millis(PREROLL_MAX_WAIT_MS - 1);
        assert_eq!(
            drain.pass(&mut out, nearly).expect("pass"),
            PassOutcome::Filling
        );
        let elapsed = now + Duration::from_millis(PREROLL_MAX_WAIT_MS);
        assert_eq!(
            drain.pass(&mut out, elapsed).expect("pass"),
            PassOutcome::Played(16)
        );
    }

    #[test]
    fn the_fallback_clock_starts_at_the_first_audio_not_at_the_connection() {
        let mut gate = PrerollGate::new();
        let now = Instant::now();
        // A silent but open connection: polling an empty ring for a minute must not
        // spend the budget the first utterance is owed.
        assert!(!gate.admit(0, now));
        let a_minute_later = now + Duration::from_secs(60);
        assert!(!gate.admit(1, a_minute_later), "the clock starts here");
        assert!(gate.admit(
            1,
            a_minute_later + Duration::from_millis(PREROLL_MAX_WAIT_MS)
        ));
    }

    #[test]
    fn an_idle_ring_is_not_an_underrun_but_a_dry_stream_is() {
        let (mut sink, mut drain) = small_pair(INBOUND_PCM_RING_BYTES);
        let mut out = FakeOut::default();
        let now = Instant::now();

        assert_eq!(drain.pass(&mut out, now).expect("pass"), PassOutcome::Idle);
        assert_eq!(drain.underruns(), 0);
        assert_eq!(drain.gate().target(), PLAYBACK_PREROLL_TARGET_BYTES);

        let mut clock = Clock(now);
        sink.accept(&wire(1, 16));
        open_gate(&mut drain, &mut out, &mut clock);
        assert_eq!(
            drain.pass(&mut out, clock.now()).expect("pass"),
            PassOutcome::Played(16)
        );
        assert_eq!(
            drain.pass(&mut out, clock.now()).expect("pass"),
            PassOutcome::Idle
        );
        assert_eq!(drain.underruns(), 1);
        assert_eq!(
            drain.gate().target(),
            next_preroll_target(PLAYBACK_PREROLL_TARGET_BYTES),
            "the rebuild is deeper than the depth that just failed"
        );

        // Still dry on the next pass: one edge, one escalation.
        assert_eq!(
            drain.pass(&mut out, clock.now()).expect("pass"),
            PassOutcome::Idle
        );
        assert_eq!(drain.underruns(), 1);
    }

    #[test]
    fn a_device_underrun_escalates_and_keeps_the_unplayed_audio() {
        let (mut sink, mut drain) = small_pair(INBOUND_PCM_RING_BYTES);
        sink.accept(&wire(1, 16));
        let mut out = FakeOut {
            underrun_at: vec![1],
            ..Default::default()
        };
        let mut clock = Clock::new();
        open_gate(&mut drain, &mut out, &mut clock);
        assert_eq!(
            drain.pass(&mut out, clock.now()).expect("pass"),
            PassOutcome::Played(0)
        );
        assert_eq!(drain.underruns(), 1);
        assert!(drain.gate().pending(), "the cushion is rebuilt first");

        open_gate(&mut drain, &mut out, &mut clock);
        drain.pass(&mut out, clock.now()).expect("pass");
        assert_eq!(out.left(), (1..=16).collect::<Vec<i16>>());
    }

    #[test]
    fn a_pass_moves_at_most_its_budget_and_the_rest_follows_in_order() {
        let (mut sink, mut drain) = small_pair(INBOUND_PCM_RING_BYTES);
        let samples = DRAIN_BYTES_PER_PASS / WIRE_BYTES_PER_SAMPLE * 2;
        // Banked as wire-sized frames, the way the streamer delivers them.
        for chunk in wire(0, samples).chunks(INBOUND_PCM_WRITE_UNIT_BYTES) {
            assert_eq!(sink.accept(chunk), Accepted::Enqueued);
        }
        let mut out = FakeOut::default();
        let mut clock = Clock::new();
        open_gate(&mut drain, &mut out, &mut clock);
        let first = drain.pass(&mut out, clock.now()).expect("pass");
        assert_eq!(
            first,
            PassOutcome::Played(DRAIN_BYTES_PER_PASS / WIRE_BYTES_PER_SAMPLE),
            "one pass is bounded by its budget"
        );
        drain_all(&mut drain, &mut out, clock.now());
        assert_eq!(out.left(), (0..samples as i16).collect::<Vec<i16>>());
    }

    #[test]
    fn a_short_write_leaves_the_remainder_banked_for_the_next_pass() {
        let (mut sink, mut drain) = small_pair(INBOUND_PCM_RING_BYTES);
        sink.accept(&wire(1, 320));
        let mut out = FakeOut {
            accept: [4usize].into_iter().collect(),
            ..Default::default()
        };
        let mut clock = Clock::new();
        open_gate(&mut drain, &mut out, &mut clock);
        assert_eq!(
            drain.pass(&mut out, clock.now()).expect("pass"),
            PassOutcome::Played(4),
            "the pass took only what the device would take"
        );
        drain_all(&mut drain, &mut out, clock.now());
        assert_eq!(out.left(), (1..=320).collect::<Vec<i16>>());
    }

    #[test]
    fn the_ring_wraps_without_reordering_or_losing_a_sample() {
        // A cap that is not a multiple of the write unit, so runs split mid-frame.
        let (mut sink, mut drain) = small_pair(1_000);
        let mut out = FakeOut::default();
        let mut clock = Clock::new();
        let mut next: i16 = 0;
        // Each round banks less than the target and is drained to empty, so every one
        // of them opens its own gate — the shape a run of short replies has.
        for _ in 0..6 {
            assert_eq!(sink.accept(&wire(next, 200)), Accepted::Enqueued);
            next += 200;
            open_gate(&mut drain, &mut out, &mut clock);
            drain_all(&mut drain, &mut out, clock.now());
            clock.advance(1);
        }
        assert_eq!(out.left(), (0..next).collect::<Vec<i16>>());
    }

    #[test]
    fn end_of_audio_is_reported_once_the_banked_tail_has_played() {
        let (mut sink, mut drain) = small_pair(INBOUND_PCM_RING_BYTES);
        sink.accept(&wire(1, 16));
        sink.end_of_audio();
        let mut out = FakeOut::default();
        let mut clock = Clock::new();
        open_gate(&mut drain, &mut out, &mut clock);
        let outcome = drain.pass(&mut out, clock.now()).expect("pass");
        assert_eq!(outcome, PassOutcome::Boundary(16), "the audio plays first");
        assert_eq!(drain.boundaries(), 1);
        assert!(drain.gate().pending(), "the next stream pre-rolls fresh");
    }

    #[test]
    fn the_reply_after_a_boundary_is_held_for_its_own_pre_roll() {
        // Back-to-back replies on one connection: the host ends one and starts the
        // next before the first has played out, so the mark sits mid-ring with the
        // follower's audio behind it and the tail ahead of it is shorter than a
        // pass's budget. The pass must stop on the mark.
        let (mut sink, mut drain) = small_pair(INBOUND_PCM_RING_BYTES);
        let tail = 200usize;
        assert!(
            tail * WIRE_BYTES_PER_SAMPLE < DRAIN_BYTES_PER_PASS,
            "the tail must be short enough for the budget to survive it"
        );
        sink.accept(&wire(1, tail));
        sink.end_of_audio();
        sink.accept(&wire(1_000, 320));

        let mut out = FakeOut::default();
        let mut clock = Clock::new();
        open_gate(&mut drain, &mut out, &mut clock);
        let outcome = drain.pass(&mut out, clock.now()).expect("pass");
        assert_eq!(
            outcome,
            PassOutcome::Boundary(tail),
            "the pass ends on the mark, having played only the first stream's tail"
        );
        assert_eq!(
            out.left(),
            (1..=tail as i16).collect::<Vec<i16>>(),
            "not one sample of the following stream may reach the device in this pass"
        );
        assert_eq!(drain.boundaries(), 1);
        assert!(
            drain.gate().pending(),
            "the follower is held while its own cushion fills"
        );
        assert_eq!(
            drain.gate().target(),
            PLAYBACK_PREROLL_TARGET_BYTES,
            "at the base depth: the previous stream's difficulties say nothing about this one"
        );

        // And it plays in full once its gate opens, in order.
        open_gate(&mut drain, &mut out, &mut clock);
        drain_all(&mut drain, &mut out, clock.now());
        assert_eq!(
            &out.left()[tail..],
            (1_000..1_320).collect::<Vec<i16>>().as_slice()
        );
    }

    #[test]
    fn two_marks_in_one_pass_budget_are_two_boundaries() {
        // Each mark ends its own pass, so a flush landing behind a stream's own
        // end-of-audio is not collapsed into one report.
        let (mut sink, mut drain) = small_pair(INBOUND_PCM_RING_BYTES);
        sink.accept(&wire(1, 8));
        sink.end_of_audio();
        sink.accept(&wire(100, 8));
        sink.end_of_audio();

        let mut out = FakeOut::default();
        let mut clock = Clock::new();
        open_gate(&mut drain, &mut out, &mut clock);
        assert_eq!(
            drain.pass(&mut out, clock.now()).expect("pass"),
            PassOutcome::Boundary(8)
        );
        open_gate(&mut drain, &mut out, &mut clock);
        assert_eq!(
            drain.pass(&mut out, clock.now()).expect("pass"),
            PassOutcome::Boundary(8)
        );
        assert_eq!(drain.boundaries(), 2);
    }

    #[test]
    fn a_flush_drops_what_is_banked_and_reports_the_boundary_at_once() {
        let (mut sink, mut drain) = small_pair(INBOUND_PCM_RING_BYTES);
        sink.accept(&wire(1, 320));
        sink.flush_playback();
        let mut out = FakeOut::default();
        let mut clock = Clock::new();
        // No gate to open: the boundary rides an emptied ring and carries no audio.
        assert_eq!(
            drain.pass(&mut out, clock.now()).expect("pass"),
            PassOutcome::Boundary(0)
        );
        assert_eq!(out.frames(), 0, "the interrupted reply is not played");
        sink.accept(&wire(50, 8));
        open_gate(&mut drain, &mut out, &mut clock);
        drain_all(&mut drain, &mut out, clock.now());
        assert_eq!(out.left(), (50..58).collect::<Vec<i16>>());
    }

    #[test]
    fn a_replaced_connection_does_not_play_its_predecessors_tail() {
        let (mut sink, mut drain) = small_pair(INBOUND_PCM_RING_BYTES);
        sink.accept(&wire(1, 320));
        // The streamer installs a new socket: everything banked belongs to the old one.
        sink.stream_reset();
        sink.accept(&wire(-100, 8));
        let mut out = FakeOut::default();
        let mut clock = Clock::new();
        // The first pass is where the reset is applied, which is why the gate holds it.
        open_gate(&mut drain, &mut out, &mut clock);
        assert_eq!(
            drain.pass(&mut out, clock.now()).expect("pass"),
            PassOutcome::Played(8)
        );
        assert_eq!(out.left(), (-100..-92).collect::<Vec<i16>>());
        assert_eq!(
            drain.gate().target(),
            PLAYBACK_PREROLL_TARGET_BYTES,
            "a fresh connection starts at the base depth"
        );
    }

    #[test]
    fn the_loop_plays_what_is_banked_and_ends_only_on_a_fatal_fault() {
        let (mut sink, mut drain) = playback_pair();
        // A full cushion, so the loop's own clock releases the gate on depth and the
        // test does not wait out a fallback in real time.
        let per_frame = INBOUND_PCM_WRITE_UNIT_BYTES / WIRE_BYTES_PER_SAMPLE;
        let frames = PLAYBACK_PREROLL_TARGET_BYTES / INBOUND_PCM_WRITE_UNIT_BYTES;
        for frame in 0..frames {
            let first = (frame * per_frame) as i16;
            assert_eq!(sink.accept(&wire(first, per_frame)), Accepted::Enqueued);
        }
        let mut out = FakeOut {
            fatal_at: Some(2),
            ..Default::default()
        };
        let fault = run_drain_loop(&mut drain, &mut out);
        assert!(matches!(fault, PlaybackFault::Fatal(_)), "{fault}");
        assert_eq!(
            out.left(),
            (0..per_frame as i16).collect::<Vec<i16>>(),
            "everything the device took before it died, in order"
        );
        assert_eq!(drain.played_frames(), per_frame as u64);
    }

    #[test]
    fn the_wanted_playback_parameters_are_the_boards_own() {
        assert_eq!(
            PLAYBACK_PARAMS.to_string(),
            "16000 Hz S16_LE 2 ch, period 320 frames × 4"
        );
    }
}
