//! Inbound TCP audio playback path: frame reassembly, inbound-Hello handshake
//! validation, and the non-blocking drain loop that feeds decoded PCM to a
//! `PlaybackSink`.
//!
//! Extracted from `main.rs` per the module-split design (§2.1). Move-only: no logic,
//! message, or name changes. `OutboundKind` stayed in `main.rs` (streamer-owned), as
//! did `build_inbound_stream_sink`/`INBOUND_PCM_PRODUCER` (speaker-owned).

// Host view: these items exist for the tests and for the device-gated call sites.
#![cfg_attr(not(target_os = "espidf"), allow(dead_code))]

use audio_pipeline::playback::{
    is_valid_s16le_pcm, Accepted, LogCountdown, PlaybackSink, INBOUND_PCM_RING_BYTES,
    INBOUND_PCM_WRITE_UNIT_BYTES, PLAYBACK_LOG_CADENCE_FRAMES, PLAYBACK_PREROLL_TARGET_BYTES,
};

use crate::DEVICE_PLAYBACK_FORMAT;

/// Playback sink that counts frames/samples and validates PCM lengths.
/// Used by the `TcpInboundFrames` HIL test and `consume_frames` unit tests.
pub(crate) struct CountingSink {
    frames: u32,
    samples: u64,
    end_of_audio_marks: u32,
    flushes: u32,
    log_countdown: LogCountdown,
}

impl CountingSink {
    pub(crate) fn new() -> Self {
        CountingSink {
            frames: 0,
            samples: 0,
            end_of_audio_marks: 0,
            flushes: 0,
            log_countdown: LogCountdown::new(PLAYBACK_LOG_CADENCE_FRAMES),
        }
    }
}

impl PlaybackSink for CountingSink {
    fn accept(&mut self, pcm: &[u8]) -> Accepted {
        if !is_valid_s16le_pcm(pcm) {
            log::warn!(
                "streamer: inbound Audio frame has invalid PCM length {} — discarding",
                pcm.len()
            );
            return Accepted::Enqueued;
        }
        self.frames = self.frames.wrapping_add(1);
        self.samples = self.samples.wrapping_add((pcm.len() / 2) as u64);
        // Rate-limited log (~1 s cadence).
        if self.log_countdown.tick() {
            log::info!(
                "streamer: inbound playback frames={} samples={}",
                self.frames,
                self.samples
            );
        }
        Accepted::Enqueued
    }

    fn end_of_audio(&mut self) {
        self.end_of_audio_marks = self.end_of_audio_marks.wrapping_add(1);
    }

    fn flush_playback(&mut self) {
        self.flushes = self.flushes.wrapping_add(1);
    }
}

/// Delegating `PlaybackSink` that counts `Accepted::Full` returns at the socket-path
/// call site — the per-connection view of `I2sStreamSink::full_stalls`. Used by the
/// `TcpInboundBackpressure` HIL test to wrap the production sink
/// (`build_inbound_stream_sink()`) so the flood-drain handler can report how many
/// times the ring backpressured, without any `audio-pipeline` accessor change.
///
/// All three `PlaybackSink` methods delegate to `inner`. `end_of_audio` and
/// `flush_playback` are one-line forwards — required even though the flood profile
/// sends neither: the trait's default no-op bodies would otherwise silently swallow
/// any `EndOfAudio`/`Flush` control frame `inner` (`I2sStreamSink`) needs to see, a
/// latent trap for future reuse of this wrapper.
pub(crate) struct StallCountingSink<'a> {
    inner: &'a mut dyn PlaybackSink,
    /// `Accepted::Full` returns from `inner.accept`.
    pub(crate) full: u32,
}

impl<'a> StallCountingSink<'a> {
    pub(crate) fn new(inner: &'a mut dyn PlaybackSink) -> Self {
        StallCountingSink { inner, full: 0 }
    }
}

impl PlaybackSink for StallCountingSink<'_> {
    fn accept(&mut self, pcm: &[u8]) -> Accepted {
        let outcome = self.inner.accept(pcm);
        if outcome == Accepted::Full {
            self.full = self.full.wrapping_add(1);
        }
        outcome
    }

    fn end_of_audio(&mut self) {
        self.inner.end_of_audio();
    }

    fn flush_playback(&mut self) {
        self.inner.flush_playback();
    }
}

/// One decoded 20 ms audio frame's worth of playout time (320 samples at 16 kHz).
const FAKE_DAC_FRAME_DUR: core::time::Duration = core::time::Duration::from_millis(20);
/// Frames that must queue before the fake DAC begins playing — the standing buffer depth
/// that absorbs tick/network jitter. Derived from the production preroll target so the
/// model cannot drift from it: `PLAYBACK_PREROLL_TARGET_BYTES / INBOUND_PCM_WRITE_UNIT_BYTES`
/// = 12 frames (240 ms). The sink models the base cushion only — it deliberately omits the
/// escalation ladder, which makes it strictly stricter than the product.
const FAKE_DAC_PREROLL_FRAMES: u32 =
    (PLAYBACK_PREROLL_TARGET_BYTES / INBOUND_PCM_WRITE_UNIT_BYTES) as u32;
/// Queue capacity in frames; a frame arriving while the buffer holds this many is refused
/// (backpressure) rather than dropped. Derived from the production playout ring so the model
/// cannot drift: `INBOUND_PCM_RING_BYTES / INBOUND_PCM_WRITE_UNIT_BYTES` = 102 frames
/// (≈ 2 048 ms).
const FAKE_DAC_QUEUE_FRAMES: u32 = (INBOUND_PCM_RING_BYTES / INBOUND_PCM_WRITE_UNIT_BYTES) as u32;

/// Deterministic fake-DAC playback sink for the `StreamRealtimeDuplex` Scenario B duplex
/// test. Models a real speaker's playout timeline with `Instant` math instead of a real
/// DAC: frames queue on `accept`, playout starts once the queue reaches the preroll target,
/// and thereafter the playhead advances in real time. A frame that arrives after the
/// playhead has run dry is an underrun; the gap between the buffer running dry and the late
/// frame's arrival is accumulated. Threadless — all state advances lazily inside `accept`.
///
/// The discriminating property: under a loop that refills the queue every wake the standing
/// depth stays near the preroll target and never underruns; under one-frame-per-blind-tick
/// delivery the queue drains to empty between frames and every late arrival underruns.
pub(crate) struct FakeDacSink {
    /// Wall-clock instant at which all currently-buffered audio finishes playing (the moment
    /// the speaker runs dry if no more frames arrive). `None` before playout starts.
    play_end: Option<std::time::Instant>,
    /// Frames accepted while still filling the preroll (before playout starts).
    preroll_pending: u32,
    /// True once the preroll target was reached and the playhead is running.
    playout_started: bool,
    /// Total frames accepted (consumed).
    consumed: u32,
    /// Distinct underrun events after playout start.
    underruns: u32,
    /// Total accumulated underrun gap.
    total_gap: core::time::Duration,
}

impl FakeDacSink {
    pub(crate) fn new() -> Self {
        FakeDacSink {
            play_end: None,
            preroll_pending: 0,
            playout_started: false,
            consumed: 0,
            underruns: 0,
            total_gap: core::time::Duration::ZERO,
        }
    }

    pub(crate) fn consumed(&self) -> u32 {
        self.consumed
    }

    pub(crate) fn underruns(&self) -> u32 {
        self.underruns
    }

    pub(crate) fn total_gap_ms(&self) -> u64 {
        self.total_gap.as_millis() as u64
    }

    /// Accept one frame at an explicit instant — the testable core of `accept`.
    fn accept_at(&mut self, pcm: &[u8], now: std::time::Instant) -> Accepted {
        if !is_valid_s16le_pcm(pcm) {
            // Invalid PCM is discarded-and-ignored, mirroring `CountingSink`; it neither
            // fills the buffer nor counts as a consumed frame.
            return Accepted::Enqueued;
        }
        // Backpressure: refuse (hold) a frame that would overfill the buffer.
        if let Some(pe) = self.play_end {
            if pe.saturating_duration_since(now) >= FAKE_DAC_QUEUE_FRAMES * FAKE_DAC_FRAME_DUR {
                return Accepted::Full;
            }
        }
        self.consumed = self.consumed.wrapping_add(1);
        if !self.playout_started {
            self.preroll_pending += 1;
            if self.preroll_pending >= FAKE_DAC_PREROLL_FRAMES {
                // Preroll met: the banked frames play back-to-back starting now.
                self.playout_started = true;
                self.play_end = Some(now + FAKE_DAC_FRAME_DUR * self.preroll_pending);
            }
            return Accepted::Enqueued;
        }
        let pe = self
            .play_end
            .expect("playout_started implies play_end is set");
        if now > pe {
            // The speaker ran dry before this frame arrived → underrun.
            self.underruns = self.underruns.wrapping_add(1);
            self.total_gap += now - pe;
            self.play_end = Some(now + FAKE_DAC_FRAME_DUR);
        } else {
            self.play_end = Some(pe + FAKE_DAC_FRAME_DUR);
        }
        Accepted::Enqueued
    }
}

impl PlaybackSink for FakeDacSink {
    fn accept(&mut self, pcm: &[u8]) -> Accepted {
        self.accept_at(pcm, std::time::Instant::now())
    }
}

// ── Inbound frame reassembly ──────────────────────────────────────────────────

/// Reassembly buffer for inbound TCP frames. Partial reads accumulate across
/// drain polls. Reset on socket replacement.
pub(crate) struct FrameAccumulator {
    buf: Vec<u8>,
    valid: usize, // buf[..valid] are read-but-unconsumed bytes
}

impl FrameAccumulator {
    pub(crate) fn new() -> Self {
        use audio_pipeline::wire::MAX_FRAME_BYTES;
        FrameAccumulator {
            buf: vec![0u8; MAX_FRAME_BYTES + 2],
            valid: 0,
        }
    }

    /// Number of buffered, read-but-not-yet-decoded bytes. Non-zero after the final
    /// `consume_frames` call at connection teardown means either a genuine truncated
    /// tail (a partial frame) or a complete frame `consume_frames` still holds because
    /// the sink kept returning `Full` — see [`Self::has_complete_frame_held`] to tell
    /// them apart.
    pub(crate) fn valid_len(&self) -> usize {
        self.valid
    }

    /// True when the bytes at the head of the buffer form a complete, decodable frame
    /// (held there because `sink.accept` returned `Full`) rather than a partial trailing
    /// frame still waiting on more socket bytes. Distinguishes "capture-thread drain
    /// stalled" from "genuine truncated tail" when `valid_len() > 0` after EOF: a partial
    /// frame has fewer buffered bytes than its own declared length; a held complete frame
    /// has at least that many.
    pub(crate) fn has_complete_frame_held(&self) -> bool {
        if self.valid < 2 {
            return false;
        }
        let payload_len = u16::from_le_bytes([self.buf[0], self.buf[1]]) as usize;
        self.valid >= 2 + payload_len
    }

    pub(crate) fn reset(&mut self) {
        self.valid = 0;
    }
}

/// Progress made by a single `drain_inbound` call — lets [`pump_inbound`] decide
/// whether to drain again or stop.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DrainOutcome {
    /// Bytes read from the socket this call. `0` when the read was skipped (accumulator
    /// full) or returned `WouldBlock`/`TimedOut`.
    pub(crate) bytes_read: usize,
    /// Complete Audio frames decoded and accepted by the sink this call. A held frame
    /// the sink finally accepts (freed slot) counts here.
    pub(crate) frames_routed: u32,
}

impl DrainOutcome {
    /// Forward progress was made — the pump should attempt another drain. A call that
    /// read bytes (even just a partial frame) or routed a frame may have more work
    /// waiting; one that did neither has drained the socket for now.
    pub(crate) fn made_progress(&self) -> bool {
        self.bytes_read > 0 || self.frames_routed > 0
    }
}

/// Per-connection inbound state, grouped so every socket-clear path can `reset()` it
/// atomically. A fresh socket is a fresh inbound stream.
///
/// `seen_hello`: gates Audio acceptance. An Audio frame before any Hello drops the
/// connection. Set true only by the Hello arm.
pub(crate) struct InboundConnectionState {
    seen_hello: bool,
    /// Inbound audio frames accepted on this connection. Drives the post-Hello
    /// blind-window heap waypoints (first frame, then every ~10). Persists across
    /// `consume_frames` calls; a fresh socket is a fresh count.
    inbound_frames: u32,
}

impl InboundConnectionState {
    pub(crate) fn new() -> Self {
        Self {
            seen_hello: false,
            inbound_frames: 0,
        }
    }

    /// Reset to fresh-connection state. Each new socket must re-handshake.
    pub(crate) fn reset(&mut self) {
        self.seen_hello = false;
        self.inbound_frames = 0;
    }
}

/// Emit a post-Hello inbound-window heap waypoint: the same field set as the
/// streamer's intra-segment waypoints (heap_free / min_heap / largest_free) plus the
/// boot-wide allocation-failure count. Samples the heap during the post-Hello inbound
/// audio window, which the segment-cadence streamer waypoints do not reach. min_heap
/// carries the low-water mark forward, so even a coarse cadence retroactively catches
/// a transient dive.
#[cfg(target_os = "espidf")]
pub(crate) fn log_inbound_heap_wp(tag: &str, frame: u32) {
    let (free, min, largest) = crate::health::heap_waypoint();
    // Self-sample the pumping thread's stack high-water mark. In the rtd-test reproduction
    // this is the rtd-test thread inside the post-Hello suspect window; on the production
    // idle-drain pump it is the streamer thread (harmless — the emitting context
    // disambiguates). HWM is fill-pattern derived, so a skip-over excursion under-reports.
    // Permanent observability: this field localizes a stack-HWM floor trip to the
    // inbound-decode window.
    // SAFETY: pure-read FreeRTOS query; NULL = the calling task.
    let shwm = unsafe { esp_idf_svc::sys::uxTaskGetStackHighWaterMark(core::ptr::null_mut()) };
    log::info!(
        "streamer: heap wp inbound {} frame={} heap_free={} min_heap={} largest_free={} alloc_fail={} shwm={}",
        tag,
        frame,
        free,
        min,
        largest,
        crate::alloc_probe::alloc_fail_count(),
        shwm,
    );
}

/// Log the accepted inbound format plus heap headroom at inbound-stream start: one line
/// per connection from a cheap pure-read query pair; the min-ever value dates any prior
/// low-water event relative to this connection.
#[cfg(target_os = "espidf")]
fn log_inbound_hello_ok(
    sample_rate_hz: u32,
    bits_per_sample: u8,
    channels: u8,
    codec: audio_pipeline::wire::Codec,
) {
    let (free, min) = crate::health::heap_free_min();
    log::info!(
        "streamer: inbound Hello ok — {} Hz / {} bit / {} ch / {:?} heap_free={} min_heap={} alloc_fail={}",
        sample_rate_hz,
        bits_per_sample,
        channels,
        codec,
        free,
        min,
        crate::alloc_probe::alloc_fail_count(),
    );
}

/// Emit the blind-window "exit" waypoint on a socket-teardown path — but only once
/// the connection is past Hello. Both inbound pumps (segment-active and idle-drain)
/// route their `Err` arm here. Gating on `seen_hello` covers post-Hello idle-drain
/// exits and mutes pre-Hello faults (audio-before-Hello, version mismatch), which
/// never entered the post-Hello window this instrument exists to sample.
pub(crate) fn log_inbound_exit_wp(state: &InboundConnectionState) {
    if state.seen_hello {
        #[cfg(target_os = "espidf")]
        log_inbound_heap_wp("exit", state.inbound_frames);
    }
}

/// Consume complete frames from the accumulator, routing Audio to `sink`.
///
/// Validates the inbound Hello handshake and format against `DEVICE_PLAYBACK_FORMAT`.
/// Audio before Hello, or a format mismatch, returns `Err` (caller drops connection).
/// When `sink.accept` returns `Full`, the head frame stays buffered for retry next tick.
///
/// Returns `Ok(n)` = number of Audio frames routed. `Err` = protocol fault.
///
/// `pub(crate)` (not private): `run_tcp_inbound_backpressure` calls this directly to
/// finish routing frames already buffered in `accum` after the socket reports EOF —
/// `drain_inbound`'s EOF arm returns before reaching its own `consume_frames` call, so a
/// caller that needs to drain a backpressure-queued backlog after clean peer close must
/// call it separately.
pub(crate) fn consume_frames(
    accum: &mut FrameAccumulator,
    sink: &mut dyn PlaybackSink,
    state: &mut InboundConnectionState,
) -> std::io::Result<u32> {
    use audio_pipeline::wire::{
        check_inbound_format, DecodeError, FormatCheck, InboundFrame, PlaybackFormat,
        AUDIO_PROTOCOL_VERSION, MAX_FRAME_BYTES,
    };

    let mut frames_decoded: u32 = 0;
    loop {
        if accum.valid < 2 {
            break;
        }
        let payload_len = u16::from_le_bytes([accum.buf[0], accum.buf[1]]) as usize;
        if payload_len > MAX_FRAME_BYTES {
            accum.reset();
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "inbound frame: length prefix exceeds MAX_FRAME_BYTES",
            ));
        }
        let frame_len = 2 + payload_len;
        if accum.valid < frame_len {
            break;
        }

        // PCM is borrowed directly from accum.buf (zero-copy); the borrow drops
        // before the compaction copy_within below (NLL safe).
        match audio_pipeline::wire::decode_inbound(&accum.buf[..frame_len]) {
            Ok(InboundFrame::Audio { pcm, .. }) => {
                if !state.seen_hello {
                    // Audio before Hello = non-conforming peer. The device's playback
                    // format is fixed (I2S slaved at 16 kHz, S16_LE-mono), so playing
                    // audio of unknown format is unsafe. Drop the connection.
                    accum.reset();
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "inbound Audio before Hello — handshake required",
                    ));
                }
                // On Full: leave the frame buffered at the head of accum for retry
                // next tick. This backs up the TCP window and throttles the sender.
                match sink.accept(pcm) {
                    Accepted::Full => {
                        break;
                    }
                    Accepted::Enqueued => {
                        frames_decoded += 1;
                        state.inbound_frames = state.inbound_frames.wrapping_add(1);
                        // Sample the heap in the post-Hello inbound audio window: at
                        // the first frame after Hello-ok and every ~10 thereafter. The
                        // periodic streamer waypoints do not reach this window.
                        if state.inbound_frames == 1 || state.inbound_frames.is_multiple_of(10) {
                            #[cfg(target_os = "espidf")]
                            log_inbound_heap_wp("periodic", state.inbound_frames);
                        }
                    }
                }
            }
            Ok(InboundFrame::Hello {
                version,
                sample_rate_hz,
                bits_per_sample,
                channels,
                codec,
            }) => {
                // Protocol version must match on both ends (design §3.4, edge case G).
                // A stale peer in either skew direction is a fatal fault at Hello time —
                // rather than accepting the Hello and dying on the first unknown tag
                // mid-stream. Device and host deploy together from this repo; no shim.
                if version != AUDIO_PROTOCOL_VERSION {
                    accum.reset();
                    log::warn!(
                        "streamer: inbound Hello protocol version mismatch — device speaks v{AUDIO_PROTOCOL_VERSION}, peer declared v{version} — dropping connection"
                    );
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "inbound Hello protocol version mismatch: device v{AUDIO_PROTOCOL_VERSION}, peer v{version}"
                        ),
                    ));
                }
                // Validate declared format against device's fixed playback format.
                // Mismatch is fatal — the I2S clock is fixed, not renegotiable.
                state.seen_hello = true;
                let declared = PlaybackFormat {
                    sample_rate_hz,
                    bits_per_sample,
                    channels,
                    codec,
                };
                match check_inbound_format(DEVICE_PLAYBACK_FORMAT, declared) {
                    FormatCheck::Match => {
                        #[cfg(target_os = "espidf")]
                        log_inbound_hello_ok(sample_rate_hz, bits_per_sample, channels, codec);
                    }
                    FormatCheck::Mismatch {
                        field,
                        expected,
                        actual,
                    } => {
                        accum.reset();
                        log::warn!(
                            "streamer: inbound Hello format mismatch on {:?} — device expects {}, server declared {} — dropping connection",
                            field,
                            expected,
                            actual
                        );
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "inbound Hello format mismatch on {field:?}: expected {expected}, got {actual}"
                            ),
                        ));
                    }
                }
            }
            Ok(InboundFrame::EndOfAudio) => {
                // Control frames before the handshake are a protocol fault, same rule as
                // Audio: without a validated format the stream is not trusted (design §3.4).
                if !state.seen_hello {
                    accum.reset();
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "inbound EndOfAudio before Hello — handshake required",
                    ));
                }
                sink.end_of_audio();
            }
            Ok(InboundFrame::Flush) => {
                if !state.seen_hello {
                    accum.reset();
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "inbound Flush before Hello — handshake required",
                    ));
                }
                sink.flush_playback();
            }
            Ok(InboundFrame::Other(tag)) => {
                log::debug!("streamer: inbound non-Audio variant (tag {tag}) — ignored");
            }
            Err(DecodeError::OversizePcm { len }) => {
                accum.reset();
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("inbound decode: oversize pcm run {len}"),
                ));
            }
            // OversizeFrame is caught by the payload_len pre-check above;
            // this catch-all keeps the match total.
            Err(e) => {
                accum.reset();
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("inbound decode error: {e:?}"),
                ));
            }
        }

        // Per-frame memmove: O(n²) when frames coalesce, but bounded and small
        // at realistic frame sizes.
        accum.buf.copy_within(frame_len..accum.valid, 0);
        accum.valid -= frame_len;
    }

    Ok(frames_decoded)
}

/// Non-blocking drain of inbound frames on the streamer socket.
///
/// One `read` into the accumulator, then `consume_frames` to route completed frames.
/// `WouldBlock`/`TimedOut` → `Idle`. Clean EOF (`read` returns `Ok(0)`) is reported as
/// `ErrorKind::UnexpectedEof`, distinct from a genuine peer RST (which surfaces from the
/// OS as `ErrorKind::ConnectionReset`) — callers that need to tell "peer closed cleanly"
/// from "connection dropped" should match on the kind rather than treating every `Err` as
/// the same terminal condition.
///
/// **Important:** `consume_frames` runs on *every* tick, even when no new bytes
/// arrived (accumulator full or WouldBlock). This is load-bearing: under backpressure
/// the held head frame must be re-offered to the sink each tick so a freed slot can
/// drain it — otherwise the TCP window never reopens (circular dependency → livelock).
pub(crate) fn drain_inbound(
    stream: &mut dyn std::io::Read,
    accum: &mut FrameAccumulator,
    sink: &mut dyn PlaybackSink,
    state: &mut InboundConnectionState,
) -> std::io::Result<DrainOutcome> {
    let buf_len = accum.buf.len();
    // Skip the read when full (backpressure). A read(&mut []) would return Ok(0),
    // which the EOF arm misreads as disconnect. The consume_frames retry below still
    // runs to re-offer the held frame.
    let read_full = accum.valid >= buf_len;
    let mut bytes_read = 0usize;
    if !read_full {
        let free = &mut accum.buf[accum.valid..buf_len];
        match stream.read(free) {
            Ok(0) => {
                if accum.valid > 0 {
                    log::warn!(
                        "streamer: inbound EOF with {} bytes buffered — truncated frame, or a \
                         backpressure backlog the caller will still drain post-EOF",
                        accum.valid
                    );
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "inbound EOF (peer disconnected)",
                ));
            }
            Ok(n) => {
                accum.valid += n;
                bytes_read = n;
            }
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                // Nothing pending — fall through to consume_frames for held-frame retry.
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    // Always run consume_frames — even with no new bytes — to retry held frames.
    let frames_routed = consume_frames(accum, sink, state)?;
    Ok(DrainOutcome {
        bytes_read,
        frames_routed,
    })
}

/// Outcome of a [`pump_inbound`] call.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PumpOutcome {
    /// The pump stopped at `max_steps` while still making progress — more inbound work
    /// likely remains, so the caller should re-poll with timeout 0 rather than sleep.
    pub(crate) hit_cap: bool,
}

/// Drain inbound frames until the socket is quiet or `max_steps` `drain_inbound`
/// calls have run — the inbound half of the loop's drain-until-blocked discipline.
///
/// Repeats `drain_inbound` while each call makes forward progress (read bytes or
/// routed a frame). Stops on the first no-progress call — socket `WouldBlock` with no
/// held frame to route, or the sink still refusing — which is the "drained for now"
/// signal; or at `max_steps` for fairness, reported via [`PumpOutcome::hit_cap`].
///
/// The caller gates the *first* invocation (`ready.readable() || !inbound_armed`);
/// once pumping, subsequent reads are driven by progress, so an armed-but-quiet wake
/// costs at most one trailing `WouldBlock` read.
pub(crate) fn pump_inbound(
    stream: &mut dyn std::io::Read,
    accum: &mut FrameAccumulator,
    sink: &mut dyn PlaybackSink,
    state: &mut InboundConnectionState,
    max_steps: u32,
) -> std::io::Result<PumpOutcome> {
    for _ in 0..max_steps {
        if !drain_inbound(stream, accum, sink, state)?.made_progress() {
            return Ok(PumpOutcome { hit_cap: false });
        }
    }
    Ok(PumpOutcome { hit_cap: true })
}

/// POLLIN-arm gate: returns false when the accumulator is full (backpressure held
/// frame), so the event loop de-arms POLLIN to avoid spinning on unread bytes.
/// Exact negation of `drain_inbound`'s read-skip guard — they agree by construction.
pub(crate) fn inbound_has_room(accum: &FrameAccumulator) -> bool {
    accum.valid < accum.buf.len()
}

#[cfg(test)]
mod tests {
    // ── drain_inbound / consume_frames ────────────────────────────────────

    use super::{
        consume_frames, drain_inbound, inbound_has_room, pump_inbound, Accepted, FrameAccumulator,
        InboundConnectionState, PlaybackSink, StallCountingSink,
    };
    use audio_pipeline::test_support::audio_frame;
    use audio_pipeline::wire::{
        AudioFrame, EndOfAudio, FlushPlayback, SegmentStart, StreamFrame, AUDIO_SAMPLES_PER_FRAME,
        MAX_FRAME_BYTES,
    };

    /// Sink that captures the full PCM bytes of each accepted frame (never refuses).
    struct CapturingSink {
        frames: Vec<Vec<u8>>,
    }
    impl CapturingSink {
        fn new() -> Self {
            CapturingSink { frames: Vec::new() }
        }
    }
    impl PlaybackSink for CapturingSink {
        fn accept(&mut self, pcm: &[u8]) -> Accepted {
            self.frames.push(pcm.to_vec());
            Accepted::Enqueued
        }
    }

    // ── StallCountingSink ──────────────────────────────────────────────────

    /// Stub inner sink for `StallCountingSink` tests: returns a scripted sequence of
    /// `Accepted` outcomes (cycling) and counts `end_of_audio`/`flush_playback` calls
    /// so delegation can be asserted independent of the counting wrapper.
    struct ScriptedSink {
        script: Vec<Accepted>,
        next: usize,
        eoa_calls: u32,
        flush_calls: u32,
    }
    impl ScriptedSink {
        fn new(script: Vec<Accepted>) -> Self {
            ScriptedSink {
                script,
                next: 0,
                eoa_calls: 0,
                flush_calls: 0,
            }
        }
    }
    impl PlaybackSink for ScriptedSink {
        fn accept(&mut self, _pcm: &[u8]) -> Accepted {
            let outcome = self.script[self.next % self.script.len()];
            self.next += 1;
            outcome
        }
        fn end_of_audio(&mut self) {
            self.eoa_calls += 1;
        }
        fn flush_playback(&mut self) {
            self.flush_calls += 1;
        }
    }

    /// `accept` forwards to the inner sink and returns its outcome unchanged, while
    /// incrementing `full` only on `Accepted::Full` returns (Enqueued passes through
    /// uncounted).
    #[test]
    fn stall_counting_sink_counts_full_and_passes_outcomes_through() {
        let mut inner = ScriptedSink::new(vec![
            Accepted::Enqueued,
            Accepted::Full,
            Accepted::Full,
            Accepted::Enqueued,
        ]);
        let mut sink = StallCountingSink::new(&mut inner);

        assert_eq!(sink.accept(&[0u8; 4]), Accepted::Enqueued);
        assert_eq!(sink.accept(&[0u8; 4]), Accepted::Full);
        assert_eq!(sink.accept(&[0u8; 4]), Accepted::Full);
        assert_eq!(sink.accept(&[0u8; 4]), Accepted::Enqueued);

        assert_eq!(sink.full, 2, "two Full returns from inner");
    }

    /// `end_of_audio` and `flush_playback` are one-line forwards, not swallowed by the
    /// counting wrapper's own default no-op trait bodies.
    #[test]
    fn stall_counting_sink_forwards_control_frames() {
        let mut inner = ScriptedSink::new(vec![Accepted::Enqueued]);
        {
            let mut sink = StallCountingSink::new(&mut inner);
            sink.end_of_audio();
            sink.flush_playback();
            sink.end_of_audio();
        }
        assert_eq!(
            inner.eoa_calls, 2,
            "both end_of_audio calls must reach inner"
        );
        assert_eq!(inner.flush_calls, 1, "flush_playback call must reach inner");
    }

    /// Encode a `StreamFrame` into a length-prefixed `Vec<u8>`.
    fn encode(frame: &StreamFrame) -> Vec<u8> {
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let n = audio_pipeline::wire::encode_frame(frame, &mut buf)
            .expect("encode_frame in test helper");
        buf[..n].to_vec()
    }

    /// Feed raw bytes into `accum` as if they arrived from a socket read.
    fn feed(accum: &mut FrameAccumulator, bytes: &[u8]) {
        let start = accum.valid;
        accum.buf[start..start + bytes.len()].copy_from_slice(bytes);
        accum.valid += bytes.len();
    }

    /// `valid_len` mirrors the accumulator's own buffered-byte count through its
    /// lifecycle: zero fresh, the fed length after a partial write, and zero again after
    /// `reset()`.
    #[test]
    fn valid_len_tracks_buffered_bytes() {
        let mut accum = FrameAccumulator::new();
        assert_eq!(accum.valid_len(), 0, "fresh accumulator is empty");

        let frame = encode(&audio_frame(AUDIO_SAMPLES_PER_FRAME));
        let partial = &frame[..frame.len() - 1];
        feed(&mut accum, partial);
        assert_eq!(
            accum.valid_len(),
            partial.len(),
            "valid_len reflects a partial (undecodable) frame"
        );

        accum.reset();
        assert_eq!(accum.valid_len(), 0, "reset clears buffered bytes");
    }

    /// `has_complete_frame_held` tells a genuinely partial trailing frame apart from a
    /// fully-received frame held at the head of the buffer — the distinction
    /// `run_tcp_inbound_backpressure`'s post-EOF diagnosis depends on.
    #[test]
    fn has_complete_frame_held_distinguishes_partial_from_complete() {
        let mut accum = FrameAccumulator::new();
        assert!(
            !accum.has_complete_frame_held(),
            "empty accumulator holds nothing"
        );

        let frame = encode(&audio_frame(AUDIO_SAMPLES_PER_FRAME));

        feed(&mut accum, &frame[..frame.len() - 1]);
        assert!(
            !accum.has_complete_frame_held(),
            "one byte short of the declared length is a partial frame, not held-complete"
        );

        feed(&mut accum, &frame[frame.len() - 1..]);
        assert!(
            accum.has_complete_frame_held(),
            "the full declared length buffered means a complete frame is held"
        );
    }

    /// Shorthand: `consume_frames` on a connection that has already handshaken.
    /// Reassembly/decode tests need Audio to be accepted, so the state starts with
    /// `seen_hello` set; handshake tests use `consume_with_state` directly to drive
    /// the Hello themselves and inspect the state afterward.
    fn consume(accum: &mut FrameAccumulator, sink: &mut dyn PlaybackSink) -> std::io::Result<u32> {
        let mut state = InboundConnectionState::new();
        state.seen_hello = true;
        consume_with_state(accum, sink, &mut state)
    }

    /// A complete frame in one read decodes and routes the exact PCM bytes to the sink.
    /// Uses position-dependent PCM content so a wrong-offset slice would be caught.
    #[test]
    fn drain_complete_frame_one_read() {
        let mut pcm: heapless::Vec<u8, { audio_pipeline::wire::MAX_AUDIO_PAYLOAD }> =
            heapless::Vec::new();
        for i in 0..AUDIO_SAMPLES_PER_FRAME * 2 {
            let _ = pcm.push((i & 0xFF) as u8);
        }
        let pcm_expected: Vec<u8> = pcm.iter().copied().collect();
        let frame = StreamFrame::Audio(AudioFrame {
            segment_id: 0,
            first_sample_index: 0,
            device_ts_us: 0,
            pcm,
        });
        let encoded = encode(&frame);
        let mut accum = FrameAccumulator::new();
        let mut sink = CapturingSink::new();
        feed(&mut accum, &encoded);
        let n = consume(&mut accum, &mut sink).expect("consume_frames");
        assert_eq!(n, 1, "expected 1 frame decoded");
        assert_eq!(sink.frames.len(), 1);
        assert_eq!(
            sink.frames[0], pcm_expected,
            "sink must receive the exact PCM bytes, not just the right length"
        );
        assert_eq!(accum.valid, 0, "accumulator must be empty after full frame");
    }

    /// A frame split across two reads reassembles correctly.
    #[test]
    fn drain_frame_split_across_two_reads() {
        let encoded = encode(&audio_frame(AUDIO_SAMPLES_PER_FRAME));
        let split = 3;
        let mut accum = FrameAccumulator::new();
        let mut sink = CapturingSink::new();

        feed(&mut accum, &encoded[..split]);
        let n = consume(&mut accum, &mut sink).expect("first consume_frames");
        assert_eq!(n, 0, "no complete frame after partial read");
        assert!(sink.frames.is_empty());
        assert_eq!(
            accum.valid, split,
            "partial bytes must remain in accumulator"
        );

        feed(&mut accum, &encoded[split..]);
        let n = consume(&mut accum, &mut sink).expect("second consume_frames");
        assert_eq!(n, 1, "one frame decoded after second read");
        assert_eq!(sink.frames.len(), 1);
        assert_eq!(accum.valid, 0, "accumulator must be empty after full frame");
    }

    /// Only a length prefix buffered (no payload yet) → no frames, bytes preserved.
    #[test]
    fn drain_partial_prefix_only_yields_idle() {
        let encoded = encode(&audio_frame(AUDIO_SAMPLES_PER_FRAME));
        let mut accum = FrameAccumulator::new();
        let mut sink = CapturingSink::new();

        feed(&mut accum, &encoded[..2]);
        let n = consume(&mut accum, &mut sink).expect("consume_frames");
        assert_eq!(n, 0, "no frame from prefix-only data");
        assert_eq!(accum.valid, 2, "prefix bytes must remain buffered");

        feed(&mut accum, &encoded[2..]);
        let n = consume(&mut accum, &mut sink).expect("consume_frames after rest");
        assert_eq!(n, 1);
        assert_eq!(accum.valid, 0);
    }

    /// Oversize length prefix (> MAX_FRAME_BYTES) → `Err(InvalidData)`.
    #[test]
    fn drain_oversize_length_prefix_is_error() {
        let mut accum = FrameAccumulator::new();
        let mut sink = CapturingSink::new();
        let oversize = (MAX_FRAME_BYTES + 1) as u16;
        feed(&mut accum, &oversize.to_le_bytes());
        feed(&mut accum, &[0u8; 4]);
        let result = consume(&mut accum, &mut sink);
        assert!(result.is_err(), "oversize prefix must be an error");
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
        assert!(sink.frames.is_empty(), "no frames must be routed on error");
    }

    /// Non-Audio variants (SegmentStart, Telemetry, SegmentEnd) are consumed but not
    /// routed to the sink. The accumulator must advance past each variant's full encoded
    /// size so subsequent frames stay in sync.
    #[test]
    fn drain_non_audio_variant_ignored() {
        let variants = [
            StreamFrame::SegmentStart(SegmentStart {
                segment_id: 1,
                base_sample_index: 0,
                base_device_ts_us: 0,
                preroll_samples: 0,
            }),
            StreamFrame::Telemetry(audio_pipeline::wire::Telemetry {
                device_ts_us: 1_000_000,
                kind: audio_pipeline::wire::TelemetryKind::SpEnergy {
                    values: [1.0, 2.0, 3.0, 4.0],
                },
            }),
            StreamFrame::SegmentEnd(audio_pipeline::wire::SegmentEnd {
                segment_id: 1,
                device_ts_us: 5_000_000,
                frames_sent: 10,
                samples_sent: 3200,
                reason: audio_pipeline::wire::EndReason::VadRelease,
            }),
        ];
        for variant in variants {
            let encoded = encode(&variant);
            let mut accum = FrameAccumulator::new();
            let mut sink = CapturingSink::new();
            feed(&mut accum, &encoded);
            let n = consume(&mut accum, &mut sink).expect("consume_frames");
            assert_eq!(
                n, 0,
                "non-Audio variant must not increment frame count: {variant:?}"
            );
            assert!(
                sink.frames.is_empty(),
                "sink must not receive non-Audio data: {variant:?}"
            );
            assert_eq!(
                accum.valid, 0,
                "accumulator must be fully consumed even for ignored variant: {variant:?}"
            );
        }
    }

    /// Build a `Hello` frame with given format fields. `pod_id` and `channel_source`
    /// are fixed — `consume_frames` validates only the format fields.
    fn inbound_hello(
        sample_rate_hz: u32,
        bits_per_sample: u8,
        channels: u8,
        codec: audio_pipeline::wire::Codec,
    ) -> StreamFrame {
        StreamFrame::Hello(audio_pipeline::wire::Hello {
            version: audio_pipeline::wire::AUDIO_PROTOCOL_VERSION,
            pod_id: heapless::String::try_from("pod-aabbcc").unwrap(),
            sample_rate_hz,
            bits_per_sample,
            channels,
            codec,
            channel_source: audio_pipeline::wire::ChannelSource::AsrBeam,
        })
    }

    /// `consume_frames` with caller-owned state (handshake tests assert on `state.seen_hello`).
    fn consume_with_state(
        accum: &mut FrameAccumulator,
        sink: &mut dyn PlaybackSink,
        state: &mut InboundConnectionState,
    ) -> std::io::Result<u32> {
        consume_frames(accum, sink, state)
    }

    /// A matching Hello is consumed (not routed to sink) and sets `seen_hello`.
    #[test]
    fn consume_frames_hello_match_ok() {
        let encoded = encode(&inbound_hello(
            16_000,
            16,
            1,
            audio_pipeline::wire::Codec::S16Le,
        ));
        let mut accum = FrameAccumulator::new();
        let mut sink = CapturingSink::new();
        let mut state = InboundConnectionState::new();
        feed(&mut accum, &encoded);
        let n = consume_with_state(&mut accum, &mut sink, &mut state)
            .expect("matching Hello must not be an error");
        assert_eq!(n, 0, "a Hello is not an Audio frame — frame count stays 0");
        assert!(
            sink.frames.is_empty(),
            "Hello must not route PCM to the sink"
        );
        assert_eq!(accum.valid, 0, "accumulator must be fully consumed");
        assert!(
            state.seen_hello,
            "a valid Hello marks the handshake as seen"
        );
    }

    /// A Hello with a mismatched format → `Err(InvalidData)`, accumulator cleared.
    #[test]
    fn consume_frames_hello_mismatch_drops() {
        let encoded = encode(&inbound_hello(
            48_000,
            16,
            1,
            audio_pipeline::wire::Codec::S16Le,
        ));
        let mut accum = FrameAccumulator::new();
        let mut sink = CapturingSink::new();
        let mut state = InboundConnectionState::new();
        feed(&mut accum, &encoded);
        let result = consume_with_state(&mut accum, &mut sink, &mut state);
        assert!(
            result.is_err(),
            "a format-mismatch Hello must drop the connection"
        );
        assert_eq!(
            result.unwrap_err().kind(),
            std::io::ErrorKind::InvalidData,
            "mismatch is surfaced as InvalidData (same as other protocol faults)"
        );
        assert_eq!(
            accum.valid, 0,
            "accumulator must be cleared on the mismatch fault"
        );
        assert!(sink.frames.is_empty(), "no PCM routed on a mismatch");
    }

    /// An Audio frame before any Hello → `Err(InvalidData)`. No PCM routed,
    /// accumulator cleared, `seen_hello` stays false.
    #[test]
    fn consume_frames_audio_before_hello_drops() {
        let mut accum = FrameAccumulator::new();
        let mut sink = CapturingSink::new();
        let mut state = InboundConnectionState::new();

        feed(&mut accum, &encode(&audio_frame(AUDIO_SAMPLES_PER_FRAME)));
        let result = consume_with_state(&mut accum, &mut sink, &mut state);
        assert!(
            result.is_err(),
            "Audio before any Hello must drop the connection (handshake required)"
        );
        assert_eq!(
            result.unwrap_err().kind(),
            std::io::ErrorKind::InvalidData,
            "absent Hello is surfaced as InvalidData (same as other protocol faults)"
        );
        assert_eq!(
            accum.valid, 0,
            "accumulator must be cleared on the handshake-required fault"
        );
        assert!(
            sink.frames.is_empty(),
            "no PCM routed when the handshake is missing"
        );
        assert!(
            !state.seen_hello,
            "seen_hello stays false — an Audio frame never marks the handshake as seen"
        );
    }

    /// Hello followed by Audio → Audio is routed normally (happy-path handshake).
    #[test]
    fn consume_frames_hello_then_audio_ok() {
        let mut accum = FrameAccumulator::new();
        let mut sink = CapturingSink::new();
        let mut state = InboundConnectionState::new();

        feed(
            &mut accum,
            &encode(&inbound_hello(
                16_000,
                16,
                1,
                audio_pipeline::wire::Codec::S16Le,
            )),
        );
        let n = consume_with_state(&mut accum, &mut sink, &mut state)
            .expect("a matching Hello must not be an error");
        assert_eq!(n, 0, "a Hello is not an Audio frame");
        assert!(
            state.seen_hello,
            "a valid Hello marks the handshake as seen"
        );
        assert!(sink.frames.is_empty(), "Hello routes no PCM");

        // Audio after the Hello: routed normally.
        feed(&mut accum, &encode(&audio_frame(AUDIO_SAMPLES_PER_FRAME)));
        let n = consume_with_state(&mut accum, &mut sink, &mut state)
            .expect("Audio after a matching Hello must be routed");
        assert_eq!(
            n, 1,
            "the Audio frame is routed once the handshake is present"
        );
        assert_eq!(sink.frames.len(), 1);
        assert!(
            state.seen_hello,
            "flag stays set for the life of the connection"
        );
    }

    /// Build a `Hello` frame with an explicit protocol `version` (format fields fixed to
    /// the device's expected values). Lets the version-mismatch fault be exercised without
    /// depending on `AUDIO_PROTOCOL_VERSION`'s current value.
    fn inbound_hello_versioned(version: u8) -> StreamFrame {
        StreamFrame::Hello(audio_pipeline::wire::Hello {
            version,
            pod_id: heapless::String::try_from("pod-aabbcc").unwrap(),
            sample_rate_hz: 16_000,
            bits_per_sample: 16,
            channels: 1,
            codec: audio_pipeline::wire::Codec::S16Le,
            channel_source: audio_pipeline::wire::ChannelSource::AsrBeam,
        })
    }

    /// A Hello whose protocol version differs from the device's → fatal `Err(InvalidData)`,
    /// accumulator cleared, `seen_hello` never set (device-side version check, §3.4 / edge G).
    #[test]
    fn consume_frames_hello_version_mismatch_drops() {
        use audio_pipeline::wire::AUDIO_PROTOCOL_VERSION;
        let stale = AUDIO_PROTOCOL_VERSION.wrapping_sub(1);
        let encoded = encode(&inbound_hello_versioned(stale));
        let mut accum = FrameAccumulator::new();
        let mut sink = CapturingSink::new();
        let mut state = InboundConnectionState::new();
        feed(&mut accum, &encoded);
        let result = consume_with_state(&mut accum, &mut sink, &mut state);
        assert!(
            result.is_err(),
            "a protocol-version-mismatch Hello must drop the connection"
        );
        assert_eq!(
            result.unwrap_err().kind(),
            std::io::ErrorKind::InvalidData,
            "version mismatch is surfaced as InvalidData"
        );
        assert_eq!(accum.valid, 0, "accumulator cleared on the version fault");
        assert!(
            !state.seen_hello,
            "a mismatched Hello never marks the handshake as seen"
        );
        assert!(
            sink.frames.is_empty(),
            "no PCM routed on a version mismatch"
        );
    }

    /// After a valid Hello, an `EndOfAudio` control frame routes to `sink.end_of_audio()`
    /// (not counted as an Audio frame, no PCM), and `Flush` routes to `flush_playback()`.
    #[test]
    fn consume_frames_routes_control_frames_after_hello() {
        use super::CountingSink;
        let mut accum = FrameAccumulator::new();
        let mut sink = CountingSink::new();
        let mut state = InboundConnectionState::new();

        feed(
            &mut accum,
            &encode(&inbound_hello(
                16_000,
                16,
                1,
                audio_pipeline::wire::Codec::S16Le,
            )),
        );
        consume_with_state(&mut accum, &mut sink, &mut state).expect("Hello consumes cleanly");

        feed(&mut accum, &encode(&StreamFrame::EndOfAudio(EndOfAudio {})));
        feed(
            &mut accum,
            &encode(&StreamFrame::FlushPlayback(FlushPlayback {})),
        );
        let n = consume_with_state(&mut accum, &mut sink, &mut state)
            .expect("control frames after Hello are not a fault");
        assert_eq!(n, 0, "control frames are not Audio frames — count stays 0");
        assert_eq!(accum.valid, 0, "both control frames fully consumed");
        assert_eq!(sink.end_of_audio_marks, 1, "EndOfAudio routed exactly once");
        assert_eq!(sink.flushes, 1, "Flush routed exactly once");
        assert_eq!(sink.frames, 0, "no Audio frames counted");
    }

    /// An `EndOfAudio` before any Hello → `Err(InvalidData)`, nothing routed.
    #[test]
    fn consume_frames_end_of_audio_before_hello_drops() {
        use super::CountingSink;
        let mut accum = FrameAccumulator::new();
        let mut sink = CountingSink::new();
        let mut state = InboundConnectionState::new();

        feed(&mut accum, &encode(&StreamFrame::EndOfAudio(EndOfAudio {})));
        let result = consume_with_state(&mut accum, &mut sink, &mut state);
        assert!(
            result.is_err(),
            "EndOfAudio before any Hello must drop the connection"
        );
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(accum.valid, 0, "accumulator cleared on the handshake fault");
        assert_eq!(
            sink.end_of_audio_marks, 0,
            "no end-of-audio routed before the handshake"
        );
    }

    /// A `Flush` before any Hello → `Err(InvalidData)`, nothing routed.
    #[test]
    fn consume_frames_flush_before_hello_drops() {
        use super::CountingSink;
        let mut accum = FrameAccumulator::new();
        let mut sink = CountingSink::new();
        let mut state = InboundConnectionState::new();

        feed(
            &mut accum,
            &encode(&StreamFrame::FlushPlayback(FlushPlayback {})),
        );
        let result = consume_with_state(&mut accum, &mut sink, &mut state);
        assert!(
            result.is_err(),
            "Flush before any Hello must drop the connection"
        );
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(accum.valid, 0, "accumulator cleared on the handshake fault");
        assert_eq!(sink.flushes, 0, "no flush routed before the handshake");
    }

    /// Bounded sink: enqueues up to `capacity` frames, then returns `Full` until
    /// `drain_one` frees a slot. Models the real capture channel's backpressure
    /// without I2S/DMA.
    struct BoundedSink {
        frames: Vec<Vec<u8>>,
        depth: usize,
        capacity: usize,
    }
    impl BoundedSink {
        fn new(capacity: usize) -> Self {
            BoundedSink {
                frames: Vec::new(),
                depth: 0,
                capacity,
            }
        }
        fn drain_one(&mut self) {
            assert!(self.depth > 0, "drain_one with no occupied slot");
            self.depth -= 1;
        }
    }
    impl PlaybackSink for BoundedSink {
        fn accept(&mut self, pcm: &[u8]) -> Accepted {
            if self.depth >= self.capacity {
                return Accepted::Full;
            }
            self.depth += 1;
            self.frames.push(pcm.to_vec());
            Accepted::Enqueued
        }
    }

    /// When the sink is full, surplus frames stay buffered in the accumulator (not
    /// dropped). After a slot frees, the held frame enqueues from the same bytes.
    #[test]
    fn consume_frames_backpressure_holds_surplus_frame() {
        let mut accum = FrameAccumulator::new();
        let mut sink = BoundedSink::new(1);
        let mut state = InboundConnectionState::new();

        feed(
            &mut accum,
            &encode(&inbound_hello(
                16_000,
                16,
                1,
                audio_pipeline::wire::Codec::S16Le,
            )),
        );
        let n = consume_with_state(&mut accum, &mut sink, &mut state)
            .expect("matching Hello must not be an error");
        assert_eq!(n, 0, "a Hello is not an Audio frame");

        // Two frames, but the sink only has room for one.
        let first = encode(&audio_frame(AUDIO_SAMPLES_PER_FRAME));
        let second = encode(&audio_frame(AUDIO_SAMPLES_PER_FRAME));
        feed(&mut accum, &first);
        feed(&mut accum, &second);
        let bytes_before = accum.valid;

        let n = consume_with_state(&mut accum, &mut sink, &mut state)
            .expect("backpressure is not an error — it returns Ok with the enqueued count");
        assert_eq!(
            n, 1,
            "only the first Audio frame enqueued; the surplus must not be counted"
        );
        assert_eq!(sink.frames.len(), 1, "exactly one chunk reached the sink");
        assert_eq!(
            accum.valid,
            bytes_before - first.len(),
            "held frame's bytes stay in the accumulator — nothing dropped"
        );

        // Free a slot, then re-drain: the held frame enqueues from the same bytes.
        sink.drain_one();
        let n = consume_with_state(&mut accum, &mut sink, &mut state)
            .expect("the retry drain must not error");
        assert_eq!(n, 1, "the previously-held frame enqueues on retry");
        assert_eq!(
            sink.frames.len(),
            2,
            "both frames eventually reach the sink"
        );
        assert_eq!(
            accum.valid, 0,
            "the accumulator is fully consumed once the held frame enqueues"
        );
    }

    /// `inbound_has_room` boundary: empty → true, one byte free → true, exactly full → false.
    #[test]
    fn inbound_has_room_tracks_accumulator_fill() {
        let mut accum = FrameAccumulator::new();
        let cap = accum.buf.len();

        assert!(inbound_has_room(&accum), "empty accumulator has room");

        accum.valid = cap - 1;
        assert!(
            inbound_has_room(&accum),
            "one free byte still counts as room"
        );

        accum.valid = cap;
        assert!(
            !inbound_has_room(&accum),
            "full accumulator has no room — POLLIN must de-arm to avoid busy-spinning"
        );
    }

    /// End-to-end POLLIN de-arm/re-arm cycle: held frames fill the accumulator under
    /// backpressure → `inbound_has_room` flips false; freeing a slot and draining
    /// compacts a frame out → flips back to true.
    #[test]
    fn inbound_has_room_dearms_on_held_frame_rearms_on_drain() {
        let mut sink = BoundedSink::new(1);
        let mut state = InboundConnectionState::new();
        let mut accum = FrameAccumulator::new();

        // Handshake.
        feed(
            &mut accum,
            &encode(&inbound_hello(
                16_000,
                16,
                1,
                audio_pipeline::wire::Codec::S16Le,
            )),
        );
        consume_with_state(&mut accum, &mut sink, &mut state).expect("Hello consumes cleanly");
        assert_eq!(accum.valid, 0);
        assert!(inbound_has_room(&accum));

        // Fill the sink's single slot so subsequent frames back-pressure.
        let frame = encode(&audio_frame(AUDIO_SAMPLES_PER_FRAME));
        feed(&mut accum, &frame);
        consume_with_state(&mut accum, &mut sink, &mut state).expect("first frame enqueues");
        assert_eq!(accum.valid, 0);

        // Pack the accumulator to exact capacity with held frames + a raw tail.
        // De-arm requires `valid == buf.len()` exactly.
        while accum.valid + frame.len() <= accum.buf.len() {
            feed(&mut accum, &frame);
        }
        let tail = accum.buf.len() - accum.valid;
        if tail > 0 {
            feed(&mut accum, &vec![0u8; tail]);
        }
        assert_eq!(accum.valid, accum.buf.len(), "accumulator at capacity");

        consume_with_state(&mut accum, &mut sink, &mut state).expect("backpressure is Ok, not Err");
        assert_eq!(accum.valid, accum.buf.len(), "no progress — sink full");
        assert!(
            !inbound_has_room(&accum),
            "full accumulator → POLLIN de-armed"
        );

        // Free a slot and drain: one frame compacts out, restoring room.
        sink.drain_one();
        consume_with_state(&mut accum, &mut sink, &mut state)
            .expect("held-frame retry drain must not error");
        assert!(inbound_has_room(&accum), "space freed → POLLIN re-armed");
    }

    /// Held-frame retry must be driven by the tick, not by new socket data.
    /// With no further bytes sent (sender stalled), freeing a sink slot and calling
    /// `drain_inbound` again must re-offer the held frame from the accumulator.
    #[test]
    fn drain_inbound_retries_held_frame_without_new_bytes() {
        use std::io::Write;
        use std::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let addr = listener.local_addr().expect("local_addr");
        let mut sender = TcpStream::connect(addr).expect("connect loopback");
        let (mut device, _) = listener.accept().expect("accept");
        device
            .set_nonblocking(true)
            .expect("device socket non-blocking (mirrors production)");

        let hello = encode(&inbound_hello(
            16_000,
            16,
            1,
            audio_pipeline::wire::Codec::S16Le,
        ));
        let audio = encode(&audio_frame(AUDIO_SAMPLES_PER_FRAME));
        sender.write_all(&hello).expect("write Hello");
        sender.write_all(&audio).expect("write Audio 1");
        sender.write_all(&audio).expect("write Audio 2");
        sender.flush().expect("flush");

        let mut accum = FrameAccumulator::new();
        let mut sink = BoundedSink::new(1);
        let mut state = InboundConnectionState::new();

        // Spin until all loopback bytes arrive: first Audio enqueued, second held.
        let mut spins = 0;
        loop {
            drain_inbound(&mut device, &mut accum, &mut sink, &mut state)
                .expect("drain must not error on a healthy handshake + audio");
            if sink.frames.len() == 1 && accum.valid > 0 {
                break;
            }
            spins += 1;
            assert!(spins < 1000, "loopback bytes never arrived");
        }
        assert!(state.seen_hello, "Hello must have been accepted");
        let held_bytes = accum.valid;
        assert!(
            held_bytes > 0,
            "the second Audio frame must be held, not dropped"
        );

        // Free a slot and drain again with no new bytes on the socket.
        sink.drain_one();
        let outcome = drain_inbound(&mut device, &mut accum, &mut sink, &mut state)
            .expect("the tick-driven retry must not error");
        assert_eq!(
            outcome.frames_routed, 1,
            "the held frame must enqueue on the next tick even with no new socket bytes"
        );
        assert_eq!(
            outcome.bytes_read, 0,
            "the retry routed the held frame with no new socket bytes"
        );
        assert_eq!(
            sink.frames.len(),
            2,
            "both frames eventually reach the sink — no chunk lost, no livelock"
        );
        assert_eq!(
            accum.valid, 0,
            "the held frame is compacted out once it enqueues"
        );
    }

    /// A clean peer close (drop the sender half) surfaces as `UnexpectedEof`, not
    /// `ConnectionReset` — callers (`run_tcp_inbound_frames`,
    /// `run_tcp_inbound_backpressure`) match on this kind to tell "peer closed cleanly"
    /// from "connection dropped," and nothing else pins the contract in code.
    #[test]
    fn drain_inbound_clean_close_reports_unexpected_eof() {
        use std::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let addr = listener.local_addr().expect("local_addr");
        let sender = TcpStream::connect(addr).expect("connect loopback");
        let (mut device, _) = listener.accept().expect("accept");
        device
            .set_nonblocking(true)
            .expect("device socket non-blocking (mirrors production)");

        drop(sender); // clean close, no bytes written

        let mut accum = FrameAccumulator::new();
        let mut sink = BoundedSink::new(1);
        let mut state = InboundConnectionState::new();

        // The FIN may not be visible to the first non-blocking read attempt; spin until
        // it is, same tolerance as the other loopback tests in this module.
        let mut spins = 0;
        let err = loop {
            match drain_inbound(&mut device, &mut accum, &mut sink, &mut state) {
                Err(e) => break e,
                Ok(_) => {
                    spins += 1;
                    assert!(spins < 1000, "clean close never observed");
                }
            }
        };
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::UnexpectedEof,
            "clean peer close must report UnexpectedEof, not ConnectionReset or any other kind"
        );
    }

    /// Connect a loopback pair, set the device end non-blocking, and return both halves.
    fn pump_loopback() -> (std::net::TcpStream, std::net::TcpStream) {
        use std::net::{TcpListener, TcpStream};
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let addr = listener.local_addr().expect("local_addr");
        let sender = TcpStream::connect(addr).expect("connect loopback");
        let (device, _) = listener.accept().expect("accept");
        device
            .set_nonblocking(true)
            .expect("device socket non-blocking (mirrors production)");
        (sender, device)
    }

    /// `pump_inbound` drains every available frame, then a call on a quiet, drained
    /// socket makes no progress and does not report cap-limited work.
    #[test]
    fn pump_inbound_drains_then_stops_without_progress() {
        use std::io::Write;
        let (mut sender, mut device) = pump_loopback();
        let hello = encode(&inbound_hello(
            16_000,
            16,
            1,
            audio_pipeline::wire::Codec::S16Le,
        ));
        let audio = encode(&audio_frame(AUDIO_SAMPLES_PER_FRAME));
        sender.write_all(&hello).expect("write Hello");
        sender.write_all(&audio).expect("write Audio 1");
        sender.write_all(&audio).expect("write Audio 2");
        sender.flush().expect("flush");

        let mut accum = FrameAccumulator::new();
        let mut sink = CapturingSink::new();
        let mut state = InboundConnectionState::new();

        // Loopback bytes may arrive in chunks; pump until both frames route.
        let mut spins = 0;
        while sink.frames.len() < 2 {
            pump_inbound(&mut device, &mut accum, &mut sink, &mut state, 8).expect("pump");
            spins += 1;
            assert!(spins < 1000, "loopback bytes never fully arrived");
        }
        assert_eq!(sink.frames.len(), 2, "both frames drained by the pump");

        // Socket now quiet and drained → no progress, not cap-limited.
        let out = pump_inbound(&mut device, &mut accum, &mut sink, &mut state, 8).expect("pump");
        assert!(
            !out.hit_cap,
            "a drained quiet socket must not report cap-limited work"
        );
    }

    /// `pump_inbound` re-offers a held (backpressured) frame with no new socket bytes and
    /// counts its acceptance as progress — the livelock guard, driven through the pump.
    #[test]
    fn pump_inbound_reoffers_held_frame_as_progress() {
        use std::io::Write;
        let (mut sender, mut device) = pump_loopback();
        let hello = encode(&inbound_hello(
            16_000,
            16,
            1,
            audio_pipeline::wire::Codec::S16Le,
        ));
        let audio = encode(&audio_frame(AUDIO_SAMPLES_PER_FRAME));
        sender.write_all(&hello).expect("write Hello");
        sender.write_all(&audio).expect("write Audio 1");
        sender.write_all(&audio).expect("write Audio 2");
        sender.flush().expect("flush");

        let mut accum = FrameAccumulator::new();
        let mut sink = BoundedSink::new(1);
        let mut state = InboundConnectionState::new();

        // Pump until the first frame routes and the second is held (sink full).
        let mut spins = 0;
        loop {
            pump_inbound(&mut device, &mut accum, &mut sink, &mut state, 8).expect("pump");
            if sink.frames.len() == 1 && accum.valid > 0 {
                break;
            }
            spins += 1;
            assert!(spins < 1000, "held frame never established");
        }

        // Free a slot; with no new bytes the pump must re-offer and route the held frame.
        sink.drain_one();
        pump_inbound(&mut device, &mut accum, &mut sink, &mut state, 8).expect("pump");
        assert_eq!(
            sink.frames.len(),
            2,
            "the held frame is re-offered and routed by the pump"
        );
        assert_eq!(
            accum.valid, 0,
            "the held frame is compacted out once it enqueues"
        );
    }

    /// A large backlog cannot be drained within a small per-wake cap: at least one
    /// `pump_inbound` call reports `hit_cap` while frames still remain.
    #[test]
    fn pump_inbound_honors_cap_under_backlog() {
        use std::io::Write;
        let (mut sender, mut device) = pump_loopback();
        let hello = encode(&inbound_hello(
            16_000,
            16,
            1,
            audio_pipeline::wire::Codec::S16Le,
        ));
        let audio = encode(&audio_frame(AUDIO_SAMPLES_PER_FRAME));
        // 40 frames (~26 KB) fits a loopback send buffer without blocking the sender,
        // yet needs far more than a cap of 4 drain calls to consume.
        const N: usize = 40;
        sender.write_all(&hello).expect("write Hello");
        for _ in 0..N {
            sender.write_all(&audio).expect("write Audio");
        }
        sender.flush().expect("flush");

        let mut accum = FrameAccumulator::new();
        let mut sink = CapturingSink::new();
        let mut state = InboundConnectionState::new();

        let mut saw_cap = false;
        let mut spins = 0;
        while sink.frames.len() < N {
            let out =
                pump_inbound(&mut device, &mut accum, &mut sink, &mut state, 4).expect("pump");
            if out.hit_cap {
                saw_cap = true;
            }
            spins += 1;
            assert!(spins < 100_000, "backlog never fully drained");
        }
        assert!(
            saw_cap,
            "draining {N} frames with a cap of 4 must hit the cap at least once"
        );
    }

    /// Two complete frames back-to-back → both decoded and routed.
    #[test]
    fn drain_two_consecutive_frames() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&encode(&audio_frame(AUDIO_SAMPLES_PER_FRAME)));
        buf.extend_from_slice(&encode(&audio_frame(AUDIO_SAMPLES_PER_FRAME)));
        let mut accum = FrameAccumulator::new();
        let mut sink = CapturingSink::new();
        feed(&mut accum, &buf);
        let n = consume(&mut accum, &mut sink).expect("consume_frames");
        assert_eq!(n, 2, "both frames must be decoded");
        assert_eq!(sink.frames.len(), 2);
        assert_eq!(accum.valid, 0);
    }

    /// Malformed payload (valid length prefix, garbage bytes) → `Err(InvalidData)`.
    #[test]
    fn drain_malformed_payload_is_error() {
        let payload_len: u16 = 10;
        let mut accum = FrameAccumulator::new();
        let mut sink = CapturingSink::new();
        feed(&mut accum, &payload_len.to_le_bytes());
        feed(&mut accum, &[0xff_u8; 10]);
        let result = consume(&mut accum, &mut sink);
        assert!(result.is_err(), "malformed payload must produce an error");
        assert_eq!(
            result.unwrap_err().kind(),
            std::io::ErrorKind::InvalidData,
            "error kind must be InvalidData"
        );
        assert_eq!(
            accum.valid, 0,
            "accumulator must be cleared on decode error"
        );
        assert!(
            sink.frames.is_empty(),
            "no frames must be routed on decode error"
        );
    }

    /// PCM length exceeding `MAX_AUDIO_PAYLOAD` → `Err(InvalidData)`.
    #[test]
    fn drain_oversize_pcm_is_error() {
        let max_pcm = audio_pipeline::wire::MAX_AUDIO_PAYLOAD;
        let over = (max_pcm + 1) as u32;
        // Hand-build a postcard Audio frame with an oversize PCM varint.
        let mut payload: Vec<u8> = vec![
            0x02, // tag = Audio
            0x00, // segment_id
            0x00, // first_sample_index
            0x00, // device_ts_us
            (over as u8 & 0x7F) | 0x80,
            (over >> 7) as u8,
        ];
        payload.extend(std::iter::repeat_n(0xABu8, over as usize));
        assert!(
            2 + payload.len() <= MAX_FRAME_BYTES + 2,
            "test frame must fit the accumulator buffer"
        );

        let plen = payload.len() as u16;
        let mut accum = FrameAccumulator::new();
        let mut sink = CapturingSink::new();
        feed(&mut accum, &plen.to_le_bytes());
        feed(&mut accum, &payload);

        let result = consume(&mut accum, &mut sink);
        assert!(result.is_err(), "oversize PCM run must produce an error");
        assert_eq!(
            result.unwrap_err().kind(),
            std::io::ErrorKind::InvalidData,
            "error kind must be InvalidData"
        );
        assert_eq!(
            accum.valid, 0,
            "accumulator must be cleared on decode error"
        );
        assert!(
            sink.frames.is_empty(),
            "no frames must be routed on decode error"
        );
    }

    /// `CountingSink` rejects empty and odd-length PCM, accepts valid even-length.
    #[test]
    fn counting_sink_rejects_invalid_pcm_lengths() {
        use super::CountingSink;
        let mut sink = CountingSink::new();

        sink.accept(&[0u8; 3]); // odd-length: invalid S16_LE
        assert_eq!(sink.frames, 0);
        assert_eq!(sink.samples, 0);

        sink.accept(&[]); // empty
        assert_eq!(sink.frames, 0);
        assert_eq!(sink.samples, 0);

        sink.accept(&[0u8; 640]); // 320 S16_LE samples
        assert_eq!(sink.frames, 1, "valid PCM must increment frames");
        assert_eq!(sink.samples, 320, "valid PCM must count samples correctly");
    }

    /// The model's preroll/queue depths are derived from the product constants, not hand-set:
    /// 12-frame preroll (240 ms) and 102-frame cap (≈ 2 048 ms) for the current geometry.
    /// This pins the derivation so a product-constant change surfaces here.
    #[test]
    fn fake_dac_model_constants_track_product() {
        use super::{FAKE_DAC_PREROLL_FRAMES, FAKE_DAC_QUEUE_FRAMES};
        assert_eq!(
            FAKE_DAC_PREROLL_FRAMES, 12,
            "preroll = PLAYBACK_PREROLL_TARGET_BYTES / write-unit"
        );
        assert_eq!(
            FAKE_DAC_QUEUE_FRAMES, 102,
            "queue cap = INBOUND_PCM_RING_BYTES / write-unit"
        );
    }

    /// A real-time-paced arrival (one 20 ms frame every 20 ms) never underruns: playout
    /// starts after the preroll fills and the queue is refilled exactly as fast as it
    /// drains, so the playhead never catches an empty buffer.
    #[test]
    fn fake_dac_realtime_arrival_never_underruns() {
        use super::{FakeDacSink, FAKE_DAC_FRAME_DUR};
        let frame = [0u8; 640];
        let mut sink = FakeDacSink::new();
        let base = std::time::Instant::now();
        for i in 0..100u32 {
            let now = base + FAKE_DAC_FRAME_DUR * i;
            assert_eq!(sink.accept_at(&frame, now), super::Accepted::Enqueued);
        }
        assert_eq!(sink.consumed(), 100, "every real-time frame is consumed");
        assert_eq!(sink.underruns(), 0, "real-time refill must not underrun");
        assert_eq!(sink.total_gap_ms(), 0, "no gap under real-time arrival");
    }

    /// Delivery slower than real time, starting once the preroll bank has drained, drains the
    /// buffer to empty between frames and underruns on each late arrival — the field symptom
    /// the Scenario B assertion catches. Timing derived from the preroll depth so it holds
    /// for any product-constant value.
    #[test]
    fn fake_dac_slow_arrival_underruns() {
        use super::{FakeDacSink, FAKE_DAC_FRAME_DUR, FAKE_DAC_PREROLL_FRAMES};
        let frame = [0u8; 640];
        let mut sink = FakeDacSink::new();
        let base = std::time::Instant::now();
        // Fill the preroll instantly so playout starts at `base` with the banked frames
        // finishing at `base + preroll·20 ms`.
        for _ in 0..FAKE_DAC_PREROLL_FRAMES {
            sink.accept_at(&frame, base);
        }
        assert_eq!(sink.underruns(), 0, "preroll fill alone does not underrun");
        // First frame lands 20 ms after the bank runs dry, thereafter one every 3 frame
        // durations (60 ms) — delivery slower than the 20 ms playout, so every arrival finds
        // the buffer empty and underruns.
        let bank_end = FAKE_DAC_FRAME_DUR * FAKE_DAC_PREROLL_FRAMES;
        const SLOW_FRAMES: u32 = 15;
        for i in 0..SLOW_FRAMES {
            let now = base + bank_end + FAKE_DAC_FRAME_DUR + FAKE_DAC_FRAME_DUR * 3 * i;
            sink.accept_at(&frame, now);
        }
        assert_eq!(
            sink.underruns(),
            SLOW_FRAMES,
            "every late frame after the buffer drains must underrun"
        );
        assert!(
            sink.total_gap_ms() > 0,
            "underruns accumulate a positive gap"
        );
    }

    /// Overfilling the buffer beyond its queue depth is refused (backpressure), not dropped:
    /// the sink returns `Full` so the caller holds the frame. Burst sized off the queue-depth
    /// constant so it exceeds the cap regardless of its value.
    #[test]
    fn fake_dac_backpressures_when_full() {
        use super::{FakeDacSink, FAKE_DAC_QUEUE_FRAMES};
        let frame = [0u8; 640];
        let mut sink = FakeDacSink::new();
        let base = std::time::Instant::now();
        // Deliver a burst past the queue depth at t=base: the first frames start playout and
        // bank, and once the buffer reaches the queue depth further arrivals are refused.
        let mut refused = 0;
        for _ in 0..(FAKE_DAC_QUEUE_FRAMES + 10) {
            if sink.accept_at(&frame, base) == super::Accepted::Full {
                refused += 1;
            }
        }
        // Delivered − cap = 10 (preroll < cap), so exactly the overflow is refused and the
        // buffer banks precisely up to the cap first. `> 0` would pass a misplaced boundary
        // (refuse-everything, or bank-past-cap); pin both counts.
        assert_eq!(
            refused, 10,
            "exactly the frames past the queue depth are refused"
        );
        assert_eq!(
            sink.consumed(),
            FAKE_DAC_QUEUE_FRAMES,
            "the buffer banks exactly up to the queue depth before refusing"
        );
    }

    /// Invalid PCM is ignored: neither consumed nor buffered.
    #[test]
    fn fake_dac_ignores_invalid_pcm() {
        use super::FakeDacSink;
        let mut sink = FakeDacSink::new();
        let now = std::time::Instant::now();
        sink.accept_at(&[0u8; 3], now); // odd length
        sink.accept_at(&[], now); // empty
        assert_eq!(sink.consumed(), 0, "invalid PCM is not consumed");
    }
}
