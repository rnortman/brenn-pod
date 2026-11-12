//! Wire protocol for the audio transport.
//!
//! **Framing:** `u16` LE length prefix followed by a postcard-encoded `StreamFrame`
//! payload.  A frame with a length field exceeding `MAX_FRAME_BYTES` is a protocol
//! error; the receiver should log and drop the connection.
//!
//! **Bidirectional use.** The same framing and `StreamFrame` type are used in both
//! directions on the TCP connection.  The device→server direction carries the full
//! set of variants (`Hello`, `SegmentStart`, `Audio`, `Telemetry`, `SegmentEnd`,
//! `Status`).
//! The server→device direction carries a leading `Hello` followed by `Audio` frames:
//! the server MUST send a `Hello` as its **first** inbound frame so the device can
//! validate the playback format (sample rate / bit depth / channels / codec) before
//! interpreting any `Audio` payload.  Both directions therefore follow the "first frame
//! must be `Hello`" rule; each side's `Hello` validates the direction it heads.  A
//! server that streams `Audio` without a leading inbound `Hello` is a legacy /
//! non-conforming peer: the device assumes its fixed 16 kHz / 16-bit / mono S16_LE
//! format and warns that the handshake was missing (see the device-side inbound path).
//!
//! **Version:** the device requires the host's `Hello::version` to equal
//! `AUDIO_PROTOCOL_VERSION`; the host reads any uplink from `AUDIO_PROTOCOL_MIN_READABLE`
//! to `AUDIO_PROTOCOL_VERSION`.  Anything else is a fatal protocol error.  What the
//! version covers, and when it is bumped, is on [`AUDIO_PROTOCOL_VERSION`].
//!
//! **Capture extension:** frame tag 64 ([`STREAM_FRAME_TAG_CAPTURE`]) is not part of the
//! connection schema.  It carries a capture stream's own frames, encoded and decoded by
//! the `wide` module; no connection peer sends it, and [`decode_frame`] and
//! [`decode_inbound`] refuse it as an unknown tag.

use heapless::{String as HString, Vec as HVec};
use serde::{Deserialize, Serialize};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Wire protocol version embedded in `Hello`: the version of the **connection schema**,
/// the frames and field values the two peers of a device–host connection may send each
/// other.  It is bumped when either peer starts sending something the other end of an
/// existing deployment cannot decode; the device's exact-match check and the host's
/// range check guard it.
///
/// Frame tags 0 to 63 belong to the connection schema.  Tag 64 is the capture
/// extension, versioned by its own `CAPTURE_SCHEMA_VERSION` together with the enum
/// values only a capture stream uses (`Codec::S32Le`, `ChannelSource::Described`,
/// `EndReason::SourceLoss`, `EndReason::StreamEnd`).  The day a connection peer sends
/// one of those values, or a tag-64 frame, to the other peer, that is a
/// connection-schema change and bumps this version.
///
/// v2: `Hello` gained an explicit `codec: Codec` field so the PCM encoding is a
/// negotiated part of the handshake rather than an implicit S16_LE convention.
/// v3: appended `StreamFrame::EndOfAudio` (tag 5) and `StreamFrame::FlushPlayback`
/// (tag 6) — the explicit end-of-audio marker and the flush/stop control frame for
/// the device-side mute policy and barge-in.  A v2 peer would die on the first
/// unknown tag mid-stream, so the version is now checked fatally on *both* ends.
/// v4: appended `StreamFrame::Status` (tag 7), the device's once-a-second status frame.
pub const AUDIO_PROTOCOL_VERSION: u8 = 4;

/// The oldest uplink a receiver still reads. A v3 uplink is a v4 uplink that never
/// sends `Status`, so frame logs recorded under v3 stay readable. The device still
/// requires an exact match on the host's `Hello`.
pub const AUDIO_PROTOCOL_MIN_READABLE: u8 = 3;

/// Maximum postcard payload size (bytes) of a frame on a device–host connection,
/// excluding the 2-byte length prefix, and the size of the ESP pod's frame buffers.
/// A received frame length field exceeding this is a protocol violation on the
/// connection.  It is not a limit of the frame-log format.
pub const MAX_FRAME_BYTES: usize = 4096;

/// PCM samples per `AudioFrame`, per channel.  20 ms @ 16 kHz.
pub const AUDIO_SAMPLES_PER_FRAME: usize = 320;

/// Maximum raw PCM payload in bytes: 320 samples × 2 bytes × 2 channels.
/// Mono frames use half this capacity.
pub const MAX_AUDIO_PAYLOAD: usize = 1280;

// ── Top-level stream frame ────────────────────────────────────────────────────

/// Top-level discriminated union carried by every length-prefixed wire frame.
///
/// The first frame on a fresh connection MUST be `Hello`; any other order is a
/// protocol violation.
///
/// `AudioFrame` contains 1280 bytes of raw PCM, making it the dominant variant.
/// Boxing it would move the data to the heap mid-protocol, which is undesirable
/// on the embedded side; the size imbalance is intentional.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum StreamFrame {
    /// Connection handshake — device → receiver, exactly once per connection.
    Hello(Hello),
    /// Begins a new utterance segment.
    SegmentStart(SegmentStart),
    /// One 20 ms chunk of PCM audio belonging to an open segment.
    Audio(AudioFrame),
    /// XVF3800 telemetry reading, interleaved within an open segment.
    Telemetry(Telemetry),
    /// Closes a segment normally (VAD release) or on overrun.
    SegmentEnd(SegmentEnd),
    /// Server → device: the current audio stream has ended naturally.  The device
    /// plays out whatever is still banked, then mutes (see the device-side mute
    /// policy).  Empty body today; struct-typed so fields can be appended later
    /// without re-tagging.
    // TODO(fixed-beams-runtime): a server → device frame that fixes or frees the
    // chip's focused beams would be a variant here.
    EndOfAudio(EndOfAudio),
    /// Server → device: discard everything banked and go silent immediately
    /// (flush/stop, for barge-in).  Empty body today; struct-typed so fields can
    /// be appended later without re-tagging.
    FlushPlayback(FlushPlayback),
    /// Device → receiver, about once a second for as long as the connection is up,
    /// inside a segment or not. See [`Status`].
    // TODO(pod-log-over-wifi): the pod's warn-and-above log lines would travel to the
    // host beside this frame.
    Status(Status),
}

// ── Frame bodies ──────────────────────────────────────────────────────────────

/// Connection handshake.  Sent once immediately after TCP connect.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Hello {
    /// Must equal `AUDIO_PROTOCOL_VERSION`.  Receiver rejects mismatches.
    pub version: u8,
    /// DHCP hostname, e.g. `"pod-aabbcc"`.
    pub pod_id: HString<32>,
    /// I2S sample rate in Hz, e.g. 16 000.
    pub sample_rate_hz: u32,
    /// Bits per sample, e.g. 16 (S16_LE).
    pub bits_per_sample: u8,
    /// Number of interleaved PCM channels in `AudioFrame::pcm`.
    pub channels: u8,
    /// PCM sample encoding in `AudioFrame::pcm`.  Negotiated explicitly rather than
    /// assumed: today `S16Le` is the only supported value, but carrying it on the
    /// handshake lets a receiver reject an unsupported codec instead of misinterpreting
    /// the bytes.
    pub codec: Codec,
    /// What the channels carry, in interleave order.
    pub channel_source: ChannelSource,
}

/// PCM sample encoding carried in `AudioFrame::pcm`.
///
/// C-like enum with explicit, **golden-byte-pinned** discriminants — postcard encodes a
/// fieldless enum as a varint of the variant's positional index, so the discriminant
/// order is wire-visible.  Do not reorder or renumber existing variants; append new
/// codecs at the end.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum Codec {
    /// Signed 16-bit little-endian PCM.  The only encoding a connection carries.
    /// Discriminant 0x00 — **do not reorder or rename** (golden-byte pinned).
    S16Le,
    /// Signed 32-bit little-endian PCM.  Capture extension only.  Discriminant 0x01.
    S32Le,
}

/// What a stream's channels carry, in interleave order.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum ChannelSource {
    /// ASR beam (directional, noise-suppression off, preferred for STT).
    /// Discriminant 0x00 — **do not reorder or rename** (golden-byte pinned).
    AsrBeam,
    /// Conference beam (omni, AGC-compressed).
    /// Discriminant 0x01 — **do not reorder or rename** (golden-byte pinned).
    ConferenceBeam,
    /// Both channels interleaved: left, right, left, right, …
    /// Discriminant 0x02 — **do not reorder or rename** (golden-byte pinned).
    Stereo,
    /// Auto-select communication beam (noise-suppressed), on the left slot —
    /// the XVF3800 default left output (`AUDIO_MGR_OP_L = (8,0)`).
    /// Discriminant 0x03.
    CommunicationBeam,
    /// Two interleaved channels: left is the auto-select communication beam
    /// (`AUDIO_MGR_OP_L = (8,0)`), right is amplified raw microphone 0
    /// (`AUDIO_MGR_OP_R = (3,0)`). Discriminant 0x04.
    StereoBeamRawMic,
    /// The channels are named, one name per channel in interleave order, by the
    /// capture `Describe` frame that follows the hello.  Capture extension only.
    /// Discriminant 0x05.
    Described,
}

/// Begins an utterance segment.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct SegmentStart {
    /// Monotonically increasing per-boot segment counter.
    pub segment_id: u32,
    /// Absolute sample index of this segment's first sample. Includes pre-roll
    /// samples.
    ///
    /// The sample index counts samples per channel delivered by the capture
    /// source since the stream's start. A sample has one index for the life of
    /// the stream and no two samples share one. Inside a segment the index never
    /// goes backwards.
    ///
    /// - Audio the source delivered and the stream does not carry shows as a
    ///   forward jump of exactly its length. Inside a segment the jump is a
    ///   measured loss. Between two segments it is audio not sent: the gate was
    ///   closed, or the producer lost it and says so in a frame (the earlier
    ///   segment's `Overrun` end, or a capture `Loss` frame).
    /// - Audio the source never delivered is an unmeasured loss: the index does
    ///   not advance, because nothing counted the samples.
    /// - A segment may begin before the previous one ended: when a gate reopens
    ///   within the pre-roll length, the new base lies inside the previous
    ///   segment, and the samples sent again carry the indices and values they
    ///   had. A producer with no pre-roll never does this.
    pub base_sample_index: u64,
    /// Device monotonic µs at the moment `base_sample_index` was captured.
    /// Clock-mapping anchor: telemetry timestamps resolve to sample offsets via
    /// `offset = (ts_us − base_device_ts_us) × sample_rate / 1_000_000`.
    pub base_device_ts_us: u64,
    /// Number of leading samples in this segment that predate VAD onset (pre-roll).
    pub preroll_samples: u32,
}

/// One 20 ms chunk of raw PCM audio.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct AudioFrame {
    /// Must match the enclosing segment's `segment_id`.
    pub segment_id: u32,
    /// Absolute sample index of `pcm[0]`.
    pub first_sample_index: u64,
    /// Device monotonic µs when this frame's first sample was captured.
    pub device_ts_us: u64,
    /// Raw PCM bytes.  S16_LE, little-endian.  Interleaved channels if `channels > 1`.
    /// Capacity `MAX_AUDIO_PAYLOAD` = 1280 bytes (320 samples × 2 ch × 2 B).
    pub pcm: HVec<u8, MAX_AUDIO_PAYLOAD>,
}

const _: () = assert!(
    AUDIO_SAMPLES_PER_FRAME * 2 * 2 <= MAX_AUDIO_PAYLOAD,
    "a full stereo frame must fit MAX_AUDIO_PAYLOAD"
);

/// Pack `samples` as S16_LE (little-endian) mono bytes.
///
/// The single owner of the wire PCM encoding contract documented on
/// [`AudioFrame::pcm`]. Panics if `samples.len() * 2 > CAP` — a caller violating the
/// frame-size invariant fails loudly instead of truncating a frame into corrupt audio.
pub fn pack_pcm_s16le<const CAP: usize>(samples: &[i16]) -> HVec<u8, CAP> {
    assert!(
        samples.len() * 2 <= CAP,
        "pcm capacity overflow: {} samples need {} bytes, CAP = {}",
        samples.len(),
        samples.len() * 2,
        CAP,
    );
    let mut out = HVec::new();
    for &s in samples {
        let b = s.to_le_bytes();
        // Length pre-checked above; pushes cannot fail.
        let _ = out.push(b[0]);
        let _ = out.push(b[1]);
    }
    out
}

/// One XVF3800 telemetry reading, in-band within a segment.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Telemetry {
    /// Device monotonic µs when the control read completed.
    pub device_ts_us: u64,
    /// The specific register values read.
    pub kind: TelemetryKind,
}

/// Discriminated telemetry payload.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryKind {
    /// AEC_AZIMUTH_VALUES (resid 33, cmd 75): four beam azimuths in radians.
    ///
    /// `[0]` and `[1]` are the focused (slow) beams, `[2]` is free-running, and
    /// `[3]` is the auto-select: a copy of whichever of the other three the chip
    /// picks.
    ///
    /// An azimuth `α ∈ [0, π]` is the angle between the source direction and the
    /// array axis. It defines a cone about that axis, with broadside at `π/2`;
    /// front/back and up/down are folded together, and elevation is never
    /// reported. NaN is valid on indices 0, 1 and 3 when nothing is tracked.
    ///
    /// Polled at 10 Hz on the device, and sent only while a device-VAD segment
    /// is open.
    Azimuths { values: [f32; 4] },
    /// AEC_SPENERGY_VALUES (resid 33, cmd 80): four beam speech-energy readings.
    SpEnergy { values: [f32; 4] },
}

/// Closes an utterance segment.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct SegmentEnd {
    /// Must match the opened segment's `segment_id`.
    pub segment_id: u32,
    /// Device monotonic µs at close.
    pub device_ts_us: u64,
    /// Total `AudioFrame`s sent in this segment.
    pub frames_sent: u32,
    /// Total PCM samples sent per channel; receiver cross-checks against .wav length.
    pub samples_sent: u64,
    /// Why the segment ended.
    pub reason: EndReason,
}

/// Why a segment was closed.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum EndReason {
    /// VAD hangover expired — normal end of utterance.
    VadRelease,
    /// Ring buffer write-head lapped the read cursor — audio lost, stream truncated.
    Overrun,
    /// Segment ended by an internal firmware fault (e.g. telemetry channel
    /// disconnect), not by audio conditions.
    ///
    /// Appended last: postcard encodes enum tags positionally, so new variants go
    /// at the end and existing tags never move.
    InternalError,
    /// The capture source lost audio of unknown length here; the sample index does not
    /// advance across the loss.  Capture extension only.
    SourceLoss,
    /// The producer ended its stream while the segment was open; why is in the
    /// stream's capture `End` frame.  Capture extension only.
    StreamEnd,
}

/// Body of [`StreamFrame::EndOfAudio`] — the server's explicit end-of-audio marker.
///
/// Empty today: postcard encodes a fieldless struct as **zero bytes**, so an
/// `EndOfAudio` frame's postcard payload is just the one-byte enum tag (5).  Kept a
/// named struct rather than a unit variant so future fields (e.g. a stream id) can be
/// appended without changing the `StreamFrame` tag layout.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct EndOfAudio {}

/// Body of [`StreamFrame::FlushPlayback`] — the server's flush/stop control frame.
///
/// Empty today (see [`EndOfAudio`] for the zero-byte / named-struct rationale).
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct FlushPlayback {}

/// Samples per level block: 100 ms at 16 kHz.
pub const LEVEL_BLOCK_SAMPLES: u32 = 1_600;

/// Level blocks the device holds and can re-send: 30 s.
pub const LEVEL_HISTORY_BLOCKS: usize = 300;

/// Device µs between status frames.
pub const STATUS_PERIOD_US: u64 = 1_000_000;

/// [`LevelBlock::flags`] bit: audio was handed to the device's speaker output during
/// the block, or within the device's hold before it. What a receiver may conclude is on
/// [`LevelBlock::playing`].
pub const LEVEL_FLAG_PLAYING: u8 = 1 << 0;

/// Body of [`StreamFrame::Status`]: the device's position, its room level history and
/// its health, sent whether or not a segment is open.
///
/// A receiver may conclude from a `Status` that the device was alive and capturing at
/// `device_ts_us`. Every counter is cumulative since boot, so a lost frame loses no
/// count, and a total lower than an earlier frame's means the device rebooted.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Status {
    /// Device monotonic µs when the frame was built.
    pub device_ts_us: u64,
    /// The capture ring's write head when the frame was built: the absolute index of
    /// the next sample to be captured, in the `SegmentStart.base_sample_index` domain.
    pub capture_head: u64,
    /// Device µs at which sample `capture_head - 1` was captured.
    pub capture_head_ts_us: u64,
    /// Finished level blocks the receiver has not yet been sent on this connection
    /// (the whole held history on the first frame after `Hello`).
    pub levels: LevelRun,
    /// Cumulative event counts since boot.
    pub counters: PodCounters,
    /// Point-in-time readings.
    pub gauges: PodGauges,
    /// How far behind `capture_head` a segment opened now would begin: the pre-roll
    /// it would carry. A segment whose onset the gate reads after this frame was
    /// built has a `base_sample_index` no earlier than `capture_head -
    /// segment_reach`. An onset the gate had already read when this frame was built,
    /// and the streamer had not yet taken, begins its segment earlier than that by
    /// the samples captured in between: one idle wake in the ordinary case, with no
    /// bound stated here. A receiver may treat `capture_head - segment_reach` as the
    /// position before which no later segment begins, and may end on it something
    /// that waits for earlier audio, knowing it ends that much early at worst.
    pub segment_reach: u32,
}

/// Consecutive level blocks. Block `n` covers samples
/// `[n * LEVEL_BLOCK_SAMPLES, (n + 1) * LEVEL_BLOCK_SAMPLES)`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Default)]
pub struct LevelRun {
    /// Block index of `blocks[0]`.
    pub first_block: u64,
    /// Blocks `first_block`, `first_block + 1`, … in order.
    pub blocks: HVec<LevelBlock, LEVEL_HISTORY_BLOCKS>,
}

/// One 100 ms reading of the device's level channel: the raw microphone where `Hello`
/// declares [`ChannelSource::StereoBeamRawMic`], the streamed channel otherwise.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
pub struct LevelBlock {
    /// Mean-square level of the block as −2·dBFS rounded, clamped to 0..=200
    /// (0 dBFS … −100 dBFS in 0.5 dB steps).
    pub half_db: u8,
    /// [`LEVEL_FLAG_PLAYING`] and future bits; see [`LevelBlock::playing`].
    pub flags: u8,
}

impl LevelBlock {
    /// The block's level in dBFS.
    pub fn dbfs(self) -> f32 {
        -(self.half_db as f32) / 2.0
    }

    /// Whether the block carries [`LEVEL_FLAG_PLAYING`]: audio was handed to the device's
    /// speaker output during the block, or within the device's hold before it. The hold
    /// is each device's own, a fixed time of at most 200 ms after the last hand-over: 200 ms
    /// on the ESP pod, none on Reachy. With nothing handed to the output the flag clears
    /// within the hold, however the stream ended and whether or not it ended at all.
    ///
    /// What a receiver may conclude:
    ///
    /// - A flagged block may contain the device's own output.
    /// - An unflagged block had no audio handed to the speaker output during it. It may
    ///   still contain the end of what was handed over before it, for the output's
    ///   buffering depth: the flag marks the hand-over, not the sound.
    /// - On a device with no hold the flag may clear up to one capture period before the
    ///   last hand-over ended, and further, up to that device's capture buffering, when
    ///   its capture is running late; there the first unflagged blocks may also overlap
    ///   the hand-over's end.
    /// - A receiver that needs blocks free of the device's output applies its own tail
    ///   after the last flagged block; that tail is what covers both of the above.
    /// - Neither state says anything about the DAC or the amplifier.
    pub fn playing(self) -> bool {
        self.flags & LEVEL_FLAG_PLAYING != 0
    }
}

/// Cumulative counts since boot, each wrapping at `u32::MAX`.
///
/// A count one device has no source for is `None` from that device: "this device does
/// not measure it" never reads as "it measured none".
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy, Default)]
pub struct PodCounters {
    /// Capture frames the I2S clock produced and the reader did not get.
    pub capture_deficit_frames: Option<u32>,
    /// Capture overruns the driver reported and the reader recovered from; each one is
    /// a hole of unknown length in the captured audio.
    pub capture_xruns: Option<u32>,
    /// Times playback ran dry mid-stream.
    pub playback_underruns: u32,
    /// Output writes that failed and were survived.
    pub playback_write_failures: Option<u32>,
    /// Times the inbound playback ring was full and the sender was held back.
    pub playback_full_stalls: u32,
    /// Inbound audio chunks dropped.
    pub playback_discards: Option<u32>,
    /// Gate openings the streamer took as the start of a segment. One that did not
    /// become a segment was lost before its `SegmentStart` reached the host (no
    /// connection, or the link would not take the frame): `onsets − segments_opened`
    /// counts those utterances.
    pub onsets: u32,
    /// Segments the streamer opened.
    pub segments_opened: u32,
    /// Segments the streamer lost before their `SegmentEnd` (socket lost, fault).
    pub segments_dropped: u32,
    /// Segments ended because the capture ring lapped the read cursor.
    pub ring_overruns: u32,
    /// Gate → streamer messages dropped because the channel was full.
    pub gate_msg_drops: u32,
    /// Connections established.
    pub connects: u32,
    /// Connection attempts that failed.
    pub connect_failures: u32,
    /// Status frames not sent because the link would not take them.
    pub status_skipped: u32,
    /// Failed allocations.
    pub alloc_failures: Option<u32>,
}

/// Readings taken when the frame was built. `None` is "not available on this device or
/// not readable just now", never zero.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy, Default)]
pub struct PodGauges {
    /// Free heap bytes.
    pub heap_free: Option<u32>,
    /// Lowest free heap bytes since boot.
    pub heap_min: Option<u32>,
    /// Largest free heap block in bytes.
    pub heap_largest: Option<u32>,
    /// Wi-Fi RSSI in dBm.
    pub rssi_dbm: Option<i8>,
    /// Whether the speech gate was open.
    pub gate_open: bool,
    /// The largest gate input seen since the previous status frame; `None` when no
    /// gate read succeeded in that time.
    pub gate_energy: Option<f32>,
    /// The least stack, in bytes, the thread sending this frame has had free since it
    /// started. `None`: this pod does not measure it.
    pub stack_free_min: Option<u32>,
}

// ── Framing codec ─────────────────────────────────────────────────────────────

/// Encode a `StreamFrame` into `buf` as a `u16` LE length prefix followed by the
/// postcard payload.  Returns the number of bytes written (2 + payload length).
///
/// Returns `Err(EncodeError)` if the postcard payload would exceed `MAX_FRAME_BYTES`
/// or if `buf` is too small.
pub fn encode_frame(frame: &StreamFrame, buf: &mut [u8]) -> Result<usize, EncodeError> {
    if buf.len() < 2 {
        return Err(EncodeError::BufferTooSmall);
    }
    // Write payload into buf[2..], leaving room for the length prefix.
    let payload_bytes =
        postcard::to_slice(frame, &mut buf[2..]).map_err(|_| EncodeError::PostcardError)?;
    let payload_len = payload_bytes.len();
    if payload_len > MAX_FRAME_BYTES {
        return Err(EncodeError::PayloadTooLarge);
    }
    let len_u16 = payload_len as u16;
    buf[0] = (len_u16 & 0xFF) as u8;
    buf[1] = (len_u16 >> 8) as u8;
    Ok(2 + payload_len)
}

/// Decode a `StreamFrame` from a raw byte slice that starts with the `u16` LE length
/// prefix, followed by the postcard payload.
///
/// The slice must be exactly `2 + length` bytes (i.e. the full framed message).
/// Returns `Err(DecodeError)` on truncation, oversize length, or postcard failure.
pub fn decode_frame(buf: &[u8]) -> Result<StreamFrame, DecodeError> {
    let payload = strip_prefix(buf)?;
    postcard::from_bytes(payload).map_err(|_| DecodeError::PostcardError)
}

/// Validate the `u16` LE length prefix and return the framed postcard payload slice.
///
/// Single source of truth for the framing rule (prefix width, endianness, oversize
/// limit), shared by [`decode_frame`] and [`decode_inbound`] so the two decode paths
/// cannot drift apart on what they accept.  Returns `Truncated` if `buf` is shorter
/// than the declared length, `OversizeFrame` if the prefix exceeds `MAX_FRAME_BYTES`.
fn strip_prefix(buf: &[u8]) -> Result<&[u8], DecodeError> {
    if buf.len() < 2 {
        return Err(DecodeError::Truncated);
    }
    let payload_len = u16::from_le_bytes([buf[0], buf[1]]) as usize;
    if payload_len > MAX_FRAME_BYTES {
        return Err(DecodeError::OversizeFrame { len: payload_len });
    }
    buf.get(2..2 + payload_len).ok_or(DecodeError::Truncated)
}

// ── Borrowing inbound decode (device, stack-lean) ──────────────────────────────

/// Discriminant order of [`StreamFrame`], pinned for the manual postcard walk in
/// [`decode_inbound`].  Postcard encodes the enum tag as the variant's positional
/// index; these MUST match the declaration order of `StreamFrame` above.
const STREAM_FRAME_TAG_HELLO: u32 = 0;
const STREAM_FRAME_TAG_SEGMENT_START: u32 = 1;
const STREAM_FRAME_TAG_AUDIO: u32 = 2;
const STREAM_FRAME_TAG_TELEMETRY: u32 = 3;
const STREAM_FRAME_TAG_SEGMENT_END: u32 = 4;
const STREAM_FRAME_TAG_END_OF_AUDIO: u32 = 5;
const STREAM_FRAME_TAG_FLUSH_PLAYBACK: u32 = 6;
const STREAM_FRAME_TAG_STATUS: u32 = 7;
/// The capture extension's one frame.  Not a connection tag: every connection tag is
/// below 64.
pub const STREAM_FRAME_TAG_CAPTURE: u32 = 64;

/// Outcome of a borrowing inbound decode: classifies the frame and, for `Audio`,
/// borrows the PCM directly out of the caller's buffer (no owned `AudioFrame`, no
/// ~1.3 KB stack copy of the PCM payload).
///
/// This is the stack-lean decode entry point used by the device's inbound playback
/// path; the full owned [`decode_frame`] remains for the host receiver, which needs
/// every variant materialized.  See ADR `docs/adr/2026/06/17-audio-output-fullduplex`.
#[derive(Debug, PartialEq)]
pub enum InboundFrame<'a> {
    /// An `Audio` frame with its PCM payload borrowed from the input buffer.
    Audio {
        /// `AudioFrame::segment_id`.
        segment_id: u32,
        /// `AudioFrame::first_sample_index`.
        first_sample_index: u64,
        /// `AudioFrame::device_ts_us`.
        device_ts_us: u64,
        /// Raw PCM bytes, borrowed from the input buffer (not copied).
        pcm: &'a [u8],
    },
    /// The server's inbound handshake, carrying only the format descriptor the device
    /// validates before playing any `Audio`.  Unlike `Audio` (whose ~1.3 KB PCM the
    /// borrowing path exists to avoid copying), these four scalars are cheap to
    /// materialize, so the body *is* decoded here.  The `pod_id` string is not surfaced
    /// — the device only needs the playback format, not the server's identity.
    Hello {
        /// `Hello::version` — the peer's `AUDIO_PROTOCOL_VERSION`.  Surfaced so the
        /// device can reject a version mismatch fatally at `Hello` time (both skew
        /// directions), rather than dying on the first unknown tag mid-stream.
        version: u8,
        /// `Hello::sample_rate_hz`.
        sample_rate_hz: u32,
        /// `Hello::bits_per_sample`.
        bits_per_sample: u8,
        /// `Hello::channels`.
        channels: u8,
        /// `Hello::codec`.
        codec: Codec,
    },
    /// The server's explicit end-of-audio marker (`StreamFrame::EndOfAudio`, tag 5).
    /// Empty body, so nothing is decoded — the tag classification is the whole signal.
    EndOfAudio,
    /// The server's flush/stop control frame (`StreamFrame::FlushPlayback`, tag 6).
    /// Empty body, so nothing is decoded — the tag classification is the whole signal.
    Flush,
    /// Any other non-`Audio` variant (`SegmentStart`, `Telemetry`, `SegmentEnd`,
    /// `Status`).
    /// The device ignores these on the inbound direction, so the body is not decoded;
    /// the raw enum tag is carried so the caller can log *which* variant was skipped
    /// (diagnosability for an unexpected mid-stream control frame).
    Other(u32),
}

/// The four-field PCM playback format the device validates an inbound `Hello` against.
///
/// Used both to express the device's *fixed* expected format (16 kHz / 16-bit / mono /
/// S16_LE — the I2S clock is slaved to the XVF3800 and the `accept` expansion path is
/// hardwired S16_LE-mono, so the device cannot retune per stream) and to carry the
/// *declared* format pulled off an inbound `Hello`.  Comparing the two is the inbound
/// handshake gate (`check_inbound_format`).
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct PlaybackFormat {
    /// I2S sample rate in Hz, e.g. 16 000.
    pub sample_rate_hz: u32,
    /// Bits per sample, e.g. 16.
    pub bits_per_sample: u8,
    /// PCM channel count (1 = mono, 2 = stereo).
    pub channels: u8,
    /// PCM sample encoding.
    pub codec: Codec,
}

/// The one device audio format (16 kHz / 16-bit / mono / S16_LE). The I2S clock is
/// slaved to the XVF3800 and the `accept` expansion path is hardwired S16_LE-mono, so
/// the device cannot retune per stream. Every production `Hello` sender and the
/// device-side expected-format check derive from this const rather than restating the
/// scalars; a few test helpers intentionally pin the scalars as literals to guard the
/// const's own values.
pub const DEVICE_PLAYBACK_FORMAT: PlaybackFormat = PlaybackFormat {
    sample_rate_hz: 16_000,
    bits_per_sample: 16,
    channels: 1,
    codec: Codec::S16Le,
};

/// Which `PlaybackFormat` field a [`check_inbound_format`] mismatch was found on.  The
/// fields are checked in declaration order and the **first** mismatch is reported, so a
/// `Hello` that differs in several fields names the earliest-declared one.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum FormatField {
    /// `sample_rate_hz` differed.
    SampleRateHz,
    /// `bits_per_sample` differed.
    BitsPerSample,
    /// `channels` differed.
    Channels,
    /// `codec` differed.
    Codec,
}

/// Result of validating a declared inbound format against the device's fixed expected
/// format ([`check_inbound_format`]).
///
/// A `Mismatch` is unrecoverable for playback (the device cannot retune the I2S master
/// clock nor reinterpret a stereo / non-S16_LE payload through its hardwired mono S16_LE
/// `accept` path), so the inbound path warns naming the field and both values, then drops
/// the connection.  The named field plus both values give the device a precise
/// `log::warn!` without re-deriving which field was wrong.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum FormatCheck {
    /// Declared format equals the device's expected format on every field.
    Match,
    /// The declared format differs from expected.  `field` names the first differing
    /// field; `expected`/`actual` carry the device's value and the declared value as
    /// `u32` (codec carried as its discriminant index via `Codec as u32`) so a single
    /// shape serves all four fields for logging.
    Mismatch {
        /// Which field differed.
        field: FormatField,
        /// The device's fixed value for that field.
        expected: u32,
        /// The value the inbound `Hello` declared for that field.
        actual: u32,
    },
}

/// Validate a declared inbound playback format against the device's fixed expected
/// format.  Pure integer comparison — no allocation, no I/O — so it is host-unit-tested
/// directly (mirroring how `should_attempt_idle_connect` is extracted as a pure value for
/// off-target testing).
///
/// Fields are compared in declaration order (`sample_rate_hz`, `bits_per_sample`,
/// `channels`, `codec`) and the **first** difference is returned as a
/// [`FormatCheck::Mismatch`]; an all-equal format returns [`FormatCheck::Match`].  The
/// caller (`consume_frames`'s inbound `Hello` arm) passes the device's fixed
/// `PlaybackFormat` as `expected` and the scalars from an [`InboundFrame::Hello`] as
/// `declared`, then warns + drops the connection on a `Mismatch`.
pub fn check_inbound_format(expected: PlaybackFormat, declared: PlaybackFormat) -> FormatCheck {
    if declared.sample_rate_hz != expected.sample_rate_hz {
        return FormatCheck::Mismatch {
            field: FormatField::SampleRateHz,
            expected: expected.sample_rate_hz,
            actual: declared.sample_rate_hz,
        };
    }
    if declared.bits_per_sample != expected.bits_per_sample {
        return FormatCheck::Mismatch {
            field: FormatField::BitsPerSample,
            expected: u32::from(expected.bits_per_sample),
            actual: u32::from(declared.bits_per_sample),
        };
    }
    if declared.channels != expected.channels {
        return FormatCheck::Mismatch {
            field: FormatField::Channels,
            expected: u32::from(expected.channels),
            actual: u32::from(declared.channels),
        };
    }
    if declared.codec != expected.codec {
        return FormatCheck::Mismatch {
            field: FormatField::Codec,
            expected: expected.codec as u32,
            actual: declared.codec as u32,
        };
    }
    FormatCheck::Match
}

/// Decode a length-prefixed frame for the device inbound path, borrowing the PCM
/// payload from `buf` instead of materializing an owned `AudioFrame`/`StreamFrame`
/// on the stack.
///
/// `buf` must be the full framed message: a `u16` LE length prefix followed by the
/// postcard payload (same shape [`decode_frame`] expects).  Applies the identical
/// prefix / oversize / truncation / postcard validation as [`decode_frame`].
///
/// For `Audio` frames the PCM run is returned as a borrowed slice into `buf` (offset +
/// length within the postcard payload).  A `Hello` frame is decoded into
/// `InboundFrame::Hello` carrying only its format scalars (the device validates the
/// playback format from it).  Only the 44-byte `Hello` body is deserialized (via the
/// derive, off the bytes after the tag) — not the whole owned `StreamFrame` enum, which
/// is sized by its ~1.3 KB `AudioFrame` variant.  Every other variant
/// (`SegmentStart`, `Telemetry`, `SegmentEnd`) yields `InboundFrame::Other` with its body
/// **not** decoded — the device ignores them on the inbound direction and the consume
/// loop advances by the already-known framed length regardless.
///
/// The walk is hand-rolled over postcard's public [`postcard::take_from_bytes`] so the
/// PCM bytes are reached without going through the `HVec<u8>` seq deserializer (which is
/// what materializes the owned 1,280-byte payload on the stack today).  The
/// fixed-width header fields are read in declaration order: enum tag (`u32` varint),
/// `segment_id` (`u32` varint), `first_sample_index` (`u64` varint),
/// `device_ts_us` (`u64` varint), then the `Vec<u8>` length (`u32` varint — the PCM
/// run never exceeds `MAX_AUDIO_PAYLOAD`, so a `u32` varint covers it) followed by the
/// raw PCM bytes.
///
/// A PCM run longer than `MAX_AUDIO_PAYLOAD` is rejected with `DecodeError::OversizePcm`
/// — the owned path got this rejection for free from the `HVec` capacity bound, so the
/// borrowing path must enforce it explicitly.
pub fn decode_inbound(buf: &[u8]) -> Result<InboundFrame<'_>, DecodeError> {
    let payload = strip_prefix(buf)?;

    // Enum tag (postcard encodes it as a varint of the variant's positional index).
    let (tag, rest) = take_u32(payload)?;
    if tag != STREAM_FRAME_TAG_AUDIO {
        return match tag {
            // The server's inbound handshake.  Deserialize only the `Hello` body from the
            // bytes after the tag varint (already consumed above): `Hello` is 44 bytes,
            // whereas the owned `StreamFrame` enum is sized by its largest variant
            // (`AudioFrame`, ~1.3 KB), so re-running `decode_frame` over the whole frame
            // materializes 2–3 full-size enum copies on the stack.  The `Hello` derive
            // still parses the variable-length `pod_id` string and validates the
            // `Codec`/`ChannelSource` enum tags; trailing bytes after the body are
            // ignored, matching the owned path.  Extract only the format scalars.
            STREAM_FRAME_TAG_HELLO => {
                let (h, _rest) = postcard::take_from_bytes::<Hello>(rest)
                    .map_err(|_| DecodeError::PostcardError)?;
                Ok(InboundFrame::Hello {
                    version: h.version,
                    sample_rate_hz: h.sample_rate_hz,
                    bits_per_sample: h.bits_per_sample,
                    channels: h.channels,
                    codec: h.codec,
                })
            }
            // Empty-body control frames the device acts on directly — no body to walk.
            STREAM_FRAME_TAG_END_OF_AUDIO => Ok(InboundFrame::EndOfAudio),
            STREAM_FRAME_TAG_FLUSH_PLAYBACK => Ok(InboundFrame::Flush),
            // SegmentStart / Telemetry / SegmentEnd / Status — uplink-only, ignored
            // inbound.  Validate only that the tag is a known variant; do not walk the
            // body (the consume loop advances by the framed length, which is already
            // known).
            STREAM_FRAME_TAG_SEGMENT_START
            | STREAM_FRAME_TAG_TELEMETRY
            | STREAM_FRAME_TAG_SEGMENT_END
            | STREAM_FRAME_TAG_STATUS => Ok(InboundFrame::Other(tag)),
            _ => Err(DecodeError::PostcardError),
        };
    }

    // Audio body, in declaration order (AudioFrame fields).
    let (segment_id, rest) = take_u32(rest)?;
    let (first_sample_index, rest) = take_u64(rest)?;
    let (device_ts_us, rest) = take_u64(rest)?;
    // pcm: Vec<u8> — varint length prefix, then the raw run.  The run never exceeds
    // MAX_AUDIO_PAYLOAD (≤ u16), so a u32 varint always covers the length.
    let (pcm_len, rest) = take_u32(rest)?;
    let pcm_len = pcm_len as usize;
    if pcm_len > MAX_AUDIO_PAYLOAD {
        return Err(DecodeError::OversizePcm { len: pcm_len });
    }
    let pcm = rest.get(..pcm_len).ok_or(DecodeError::Truncated)?;

    Ok(InboundFrame::Audio {
        segment_id,
        first_sample_index,
        device_ts_us,
        pcm,
    })
}

/// Read one varint-`u32` from `buf` via postcard, returning the value and the
/// remaining bytes.  Postcard's `take_from_bytes` borrows the remainder from `buf`,
/// so no payload bytes are copied.
fn take_u32(buf: &[u8]) -> Result<(u32, &[u8]), DecodeError> {
    postcard::take_from_bytes::<u32>(buf).map_err(|_| DecodeError::PostcardError)
}

/// Read one varint-`u64` from `buf` via postcard, returning the value and the
/// remaining bytes.
fn take_u64(buf: &[u8]) -> Result<(u64, &[u8]), DecodeError> {
    postcard::take_from_bytes::<u64>(buf).map_err(|_| DecodeError::PostcardError)
}

/// Errors returned by `encode_frame` and the `wide` module's `encode_wide`.
#[derive(Debug, PartialEq)]
pub enum EncodeError {
    /// Output buffer is smaller than 2 bytes.
    BufferTooSmall,
    /// Postcard payload would exceed the codec's limit: `MAX_FRAME_BYTES`, or
    /// `MAX_WIDE_FRAME_BYTES` for a wide frame.
    PayloadTooLarge,
    /// Postcard serialization failed (typically a capacity overflow in a heapless container).
    PostcardError,
}

/// Errors returned by the decode functions in this module (`decode_frame` and
/// `decode_inbound`).  Note that `OversizePcm` is reachable only from the borrowing
/// `decode_inbound` path; the owned `decode_frame` path gets that rejection implicitly
/// from the `HVec` capacity bound (see the variant doc).
#[derive(Debug, PartialEq)]
pub enum DecodeError {
    /// Input buffer is shorter than the declared length.
    Truncated,
    /// Length prefix exceeds the codec's limit: `MAX_FRAME_BYTES`, or
    /// `MAX_WIDE_FRAME_BYTES` for a wide frame.
    OversizeFrame { len: usize },
    /// An `Audio` frame's declared PCM run exceeds `MAX_AUDIO_PAYLOAD`.
    /// Only the borrowing `decode_inbound` path returns this — the owned `decode_frame`
    /// path gets the same rejection implicitly from the `HVec` capacity bound.
    OversizePcm { len: usize },
    /// Postcard deserialization failed.
    PostcardError,
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    extern crate alloc;
    use alloc::vec;

    #[test]
    fn pack_pcm_s16le_known_values() {
        let out: HVec<u8, MAX_AUDIO_PAYLOAD> =
            pack_pcm_s16le(&[0, 1, -1, i16::MIN, i16::MAX, 0x1234]);
        assert_eq!(
            out.as_slice(),
            &[
                0x00, 0x00, 0x01, 0x00, 0xFF, 0xFF, 0x00, 0x80, 0xFF, 0x7F, 0x34, 0x12
            ]
        );
    }

    #[test]
    fn pack_pcm_s16le_capacity_boundary() {
        let samples = vec![0i16; MAX_AUDIO_PAYLOAD / 2];
        let out: HVec<u8, MAX_AUDIO_PAYLOAD> = pack_pcm_s16le(&samples);
        assert_eq!(out.len(), MAX_AUDIO_PAYLOAD);
    }

    #[test]
    #[should_panic(expected = "pcm capacity overflow")]
    fn pack_pcm_s16le_overflow_panics() {
        let samples = vec![0i16; MAX_AUDIO_PAYLOAD / 2 + 1];
        let _: HVec<u8, MAX_AUDIO_PAYLOAD> = pack_pcm_s16le(&samples);
    }

    #[test]
    fn pack_pcm_s16le_empty() {
        let out: HVec<u8, MAX_AUDIO_PAYLOAD> = pack_pcm_s16le(&[]);
        assert!(out.is_empty());
    }

    // Helper: encode then decode, assert round-trip equality.
    fn roundtrip(frame: &StreamFrame) -> StreamFrame {
        let mut buf = [0u8; MAX_FRAME_BYTES + 2];
        let n = encode_frame(frame, &mut buf).expect("encode");
        decode_frame(&buf[..n]).expect("decode")
    }

    // ── Hello ─────────────────────────────────────────────────────────────────

    #[test]
    fn hello_roundtrip() {
        let frame = StreamFrame::Hello(Hello {
            version: AUDIO_PROTOCOL_VERSION,
            pod_id: HString::try_from("pod-aabbcc").unwrap(),
            sample_rate_hz: 16_000,
            bits_per_sample: 16,
            channels: 1,
            codec: Codec::S16Le,
            channel_source: ChannelSource::AsrBeam,
        });
        assert_eq!(roundtrip(&frame), frame);
    }

    /// Round-trip encode+decode for CommunicationBeam — exercises the new discriminant 0x03.
    ///
    /// The golden-bytes test only calls encode_frame; this confirms postcard deserialization
    /// of the new variant is also correct (so a decode-path bug in the new discriminant
    /// would be caught here rather than only on live hardware).
    #[test]
    fn hello_communication_beam_roundtrip() {
        let frame = StreamFrame::Hello(Hello {
            version: AUDIO_PROTOCOL_VERSION,
            pod_id: HString::try_from("pod-aabbcc").unwrap(),
            sample_rate_hz: 16_000,
            bits_per_sample: 16,
            channels: 1,
            codec: Codec::S16Le,
            channel_source: ChannelSource::CommunicationBeam,
        });
        assert_eq!(roundtrip(&frame), frame);
    }

    #[test]
    fn hello_golden_bytes() {
        // Golden bytes for the canonical Hello frame — pin the wire format.
        // If this test fails after a schema change, update the golden bytes AND
        // bump AUDIO_PROTOCOL_VERSION.
        let frame = StreamFrame::Hello(Hello {
            version: 3,
            pod_id: HString::try_from("pod-aabbcc").unwrap(),
            sample_rate_hz: 16_000,
            bits_per_sample: 16,
            channels: 1,
            codec: Codec::S16Le,
            channel_source: ChannelSource::CommunicationBeam,
        });
        let mut buf = [0u8; MAX_FRAME_BYTES + 2];
        let n = encode_frame(&frame, &mut buf).unwrap();
        let payload = &buf[2..n];
        // Compute expected and print for initial pinning; assert if already known.
        // Run once to capture, then pin:
        let expected: &[u8] = &[
            0x00, // StreamFrame discriminant 0 = Hello
            0x03, // version = 3
            0x0a, // pod_id length = 10
            b'p', b'o', b'd', b'-', b'a', b'a', b'b', b'b', b'c', b'c', 0x80,
            0x7d, // sample_rate_hz = 16000 varint
            0x10, // bits_per_sample = 16
            0x01, // channels = 1
            0x00, // Codec::S16Le = 0
            0x03, // ChannelSource::CommunicationBeam = 3
        ];
        assert_eq!(
            payload, expected,
            "Hello golden bytes mismatch — bump AUDIO_PROTOCOL_VERSION"
        );
    }

    /// Golden bytes for a stereo `Hello` declaring `StereoBeamRawMic` — pins the
    /// appended discriminant 0x04 and `channels = 2`, and round-trips it.
    #[test]
    fn hello_stereo_beam_raw_mic_golden_bytes() {
        let frame = StreamFrame::Hello(Hello {
            version: 3,
            pod_id: HString::try_from("pod-aabbcc").unwrap(),
            sample_rate_hz: 16_000,
            bits_per_sample: 16,
            channels: 2,
            codec: Codec::S16Le,
            channel_source: ChannelSource::StereoBeamRawMic,
        });
        let mut buf = [0u8; MAX_FRAME_BYTES + 2];
        let n = encode_frame(&frame, &mut buf).unwrap();
        let payload = &buf[2..n];
        let expected: &[u8] = &[
            0x00, // StreamFrame discriminant 0 = Hello
            0x03, // version = 3
            0x0a, // pod_id length = 10
            b'p', b'o', b'd', b'-', b'a', b'a', b'b', b'b', b'c', b'c', 0x80,
            0x7d, // sample_rate_hz = 16000 varint
            0x10, // bits_per_sample = 16
            0x02, // channels = 2
            0x00, // Codec::S16Le = 0
            0x04, // ChannelSource::StereoBeamRawMic = 4
        ];
        assert_eq!(payload, expected);
        assert_eq!(roundtrip(&frame), frame);
    }

    /// A full stereo frame (320 samples × 2 channels × 2 B = 1280 B) fits the payload.
    #[test]
    fn full_stereo_audio_frame_roundtrips() {
        let mut pcm: HVec<u8, MAX_AUDIO_PAYLOAD> = HVec::new();
        for i in 0..AUDIO_SAMPLES_PER_FRAME * 2 * 2 {
            pcm.push(i as u8).unwrap();
        }
        assert_eq!(pcm.len(), MAX_AUDIO_PAYLOAD);
        let frame = StreamFrame::Audio(AudioFrame {
            segment_id: 1,
            first_sample_index: 0,
            device_ts_us: 0,
            pcm,
        });
        assert_eq!(roundtrip(&frame), frame);
    }

    // Renamed from hello_version_mismatch_detectable: this test only verifies that
    // postcard round-trips the version byte faithfully.  Rejecting a mismatched version
    // is the consumer's responsibility, not this wire type's.
    #[test]
    fn hello_version_field_survives_roundtrip() {
        let frame = StreamFrame::Hello(Hello {
            version: 99,
            pod_id: HString::try_from("pod-test").unwrap(),
            sample_rate_hz: 16_000,
            bits_per_sample: 16,
            channels: 1,
            codec: Codec::S16Le,
            channel_source: ChannelSource::AsrBeam,
        });
        let mut buf = [0u8; MAX_FRAME_BYTES + 2];
        let n = encode_frame(&frame, &mut buf).unwrap();
        let decoded = decode_frame(&buf[..n]).unwrap();
        if let StreamFrame::Hello(h) = decoded {
            assert_ne!(
                h.version, AUDIO_PROTOCOL_VERSION,
                "version 99 should differ from current"
            );
        } else {
            panic!("expected Hello");
        }
    }

    // ── SegmentStart ──────────────────────────────────────────────────────────

    #[test]
    fn segment_start_roundtrip() {
        let frame = StreamFrame::SegmentStart(SegmentStart {
            segment_id: 1,
            base_sample_index: 48_000,
            base_device_ts_us: 3_000_000,
            preroll_samples: 16_000,
        });
        assert_eq!(roundtrip(&frame), frame);
    }

    #[test]
    fn segment_start_golden_bytes() {
        let frame = StreamFrame::SegmentStart(SegmentStart {
            segment_id: 1,
            base_sample_index: 0,
            base_device_ts_us: 0,
            preroll_samples: 0,
        });
        let mut buf = [0u8; MAX_FRAME_BYTES + 2];
        let n = encode_frame(&frame, &mut buf).unwrap();
        let payload = &buf[2..n];
        // postcard: discriminant 1, then four varint-encoded fields (all small)
        let expected: &[u8] = &[
            0x01, // StreamFrame discriminant 1 = SegmentStart
            0x01, // segment_id = 1
            0x00, // base_sample_index = 0
            0x00, // base_device_ts_us = 0
            0x00, // preroll_samples = 0
        ];
        assert_eq!(payload, expected, "SegmentStart golden bytes mismatch");
    }

    // ── AudioFrame ────────────────────────────────────────────────────────────

    #[test]
    fn audio_frame_roundtrip() {
        let mut pcm: HVec<u8, MAX_AUDIO_PAYLOAD> = HVec::new();
        for i in 0u8..20 {
            pcm.push(i).unwrap();
        }
        let frame = StreamFrame::Audio(AudioFrame {
            segment_id: 2,
            first_sample_index: 320,
            device_ts_us: 20_000,
            pcm,
        });
        assert_eq!(roundtrip(&frame), frame);
    }

    #[test]
    fn max_audio_frame_fits_in_max_frame_bytes() {
        // A full-capacity AudioFrame (1280 bytes PCM) must encode within MAX_FRAME_BYTES.
        let mut pcm: HVec<u8, MAX_AUDIO_PAYLOAD> = HVec::new();
        for i in 0..MAX_AUDIO_PAYLOAD {
            pcm.push((i & 0xFF) as u8).unwrap();
        }
        let frame = StreamFrame::Audio(AudioFrame {
            segment_id: 0,
            first_sample_index: 0,
            device_ts_us: 0,
            pcm,
        });
        let mut buf = [0u8; MAX_FRAME_BYTES + 2];
        let n = encode_frame(&frame, &mut buf).expect("max AudioFrame must fit in MAX_FRAME_BYTES");
        let payload_len = n - 2;
        assert!(
            payload_len <= MAX_FRAME_BYTES,
            "payload {payload_len} exceeds MAX_FRAME_BYTES {MAX_FRAME_BYTES}"
        );
    }

    // ── Telemetry ─────────────────────────────────────────────────────────────

    #[test]
    fn telemetry_azimuths_roundtrip() {
        let frame = StreamFrame::Telemetry(Telemetry {
            device_ts_us: 1_000_000,
            kind: TelemetryKind::Azimuths {
                values: [0.5, f32::NAN, 1.2, f32::NAN],
            },
        });
        let decoded = roundtrip(&frame);
        if let StreamFrame::Telemetry(t) = decoded {
            if let TelemetryKind::Azimuths { values } = t.kind {
                assert!((values[0] - 0.5f32).abs() < 1e-6);
                assert!(values[1].is_nan());
                assert!((values[2] - 1.2f32).abs() < 1e-6);
                assert!(values[3].is_nan());
            } else {
                panic!("expected Azimuths");
            }
        } else {
            panic!("expected Telemetry");
        }
    }

    #[test]
    fn telemetry_spenergy_roundtrip() {
        let frame = StreamFrame::Telemetry(Telemetry {
            device_ts_us: 2_000_000,
            kind: TelemetryKind::SpEnergy {
                values: [100.0, 200.0, 150.0, 50.0],
            },
        });
        assert_eq!(roundtrip(&frame), frame);
    }

    // ── SegmentEnd ────────────────────────────────────────────────────────────

    #[test]
    fn segment_end_vad_release_roundtrip() {
        let frame = StreamFrame::SegmentEnd(SegmentEnd {
            segment_id: 1,
            device_ts_us: 5_000_000,
            frames_sent: 250,
            samples_sent: 80_000,
            reason: EndReason::VadRelease,
        });
        assert_eq!(roundtrip(&frame), frame);
    }

    #[test]
    fn segment_end_overrun_roundtrip() {
        let frame = StreamFrame::SegmentEnd(SegmentEnd {
            segment_id: 2,
            device_ts_us: 6_000_000,
            frames_sent: 10,
            samples_sent: 3_200,
            reason: EndReason::Overrun,
        });
        assert_eq!(roundtrip(&frame), frame);
    }

    /// The appended variant round-trips, and its tag sits after `Overrun` — the
    /// positional-tag guarantee that older peers keep decoding the older variants.
    #[test]
    fn segment_end_internal_error_roundtrip() {
        let frame = StreamFrame::SegmentEnd(SegmentEnd {
            segment_id: 3,
            device_ts_us: 7_000_000,
            frames_sent: 5,
            samples_sent: 1_600,
            reason: EndReason::InternalError,
        });
        assert_eq!(roundtrip(&frame), frame);

        let tag = |r: EndReason| {
            let mut buf = [0u8; 64];
            let f = StreamFrame::SegmentEnd(SegmentEnd {
                segment_id: 3,
                device_ts_us: 7_000_000,
                frames_sent: 5,
                samples_sent: 1_600,
                reason: r,
            });
            let n = encode_frame(&f, &mut buf).expect("encode");
            buf[n - 1]
        };
        assert_eq!(tag(EndReason::VadRelease), 0);
        assert_eq!(tag(EndReason::Overrun), 1);
        assert_eq!(tag(EndReason::InternalError), 2);
        assert_eq!(tag(EndReason::SourceLoss), 3);
        assert_eq!(tag(EndReason::StreamEnd), 4);
    }

    /// The capture extension's appended `Codec` and `ChannelSource` values take the next
    /// positional index; the old values keep their bytes.
    #[test]
    fn appended_hello_enum_values_take_next_index() {
        let hello_tail = |codec: Codec, channel_source: ChannelSource| {
            let bytes = frame_bytes(&StreamFrame::Hello(Hello {
                version: AUDIO_PROTOCOL_VERSION,
                pod_id: HString::try_from("p").unwrap(),
                sample_rate_hz: 16_000,
                bits_per_sample: 16,
                channels: 1,
                codec,
                channel_source,
            }));
            [bytes[bytes.len() - 2], bytes[bytes.len() - 1]]
        };
        assert_eq!(hello_tail(Codec::S16Le, ChannelSource::AsrBeam), [0, 0]);
        assert_eq!(hello_tail(Codec::S32Le, ChannelSource::AsrBeam), [1, 0]);
        assert_eq!(
            hello_tail(Codec::S16Le, ChannelSource::StereoBeamRawMic),
            [0, 4]
        );
        assert_eq!(hello_tail(Codec::S16Le, ChannelSource::Described), [0, 5]);
    }

    /// The tag a connection frame encodes with; exhaustive, so a new `StreamFrame`
    /// variant does not compile here until it has a tag.
    fn connection_tag(frame: &StreamFrame) -> u32 {
        match frame {
            StreamFrame::Hello(_) => STREAM_FRAME_TAG_HELLO,
            StreamFrame::SegmentStart(_) => STREAM_FRAME_TAG_SEGMENT_START,
            StreamFrame::Audio(_) => STREAM_FRAME_TAG_AUDIO,
            StreamFrame::Telemetry(_) => STREAM_FRAME_TAG_TELEMETRY,
            StreamFrame::SegmentEnd(_) => STREAM_FRAME_TAG_SEGMENT_END,
            StreamFrame::EndOfAudio(_) => STREAM_FRAME_TAG_END_OF_AUDIO,
            StreamFrame::FlushPlayback(_) => STREAM_FRAME_TAG_FLUSH_PLAYBACK,
            StreamFrame::Status(_) => STREAM_FRAME_TAG_STATUS,
        }
    }

    #[test]
    fn every_connection_tag_is_below_the_capture_tag() {
        let frames = [
            StreamFrame::Hello(Hello {
                version: AUDIO_PROTOCOL_VERSION,
                pod_id: HString::try_from("p").unwrap(),
                sample_rate_hz: 16_000,
                bits_per_sample: 16,
                channels: 1,
                codec: Codec::S16Le,
                channel_source: ChannelSource::AsrBeam,
            }),
            StreamFrame::SegmentStart(SegmentStart {
                segment_id: 0,
                base_sample_index: 0,
                base_device_ts_us: 0,
                preroll_samples: 0,
            }),
            StreamFrame::Audio(AudioFrame {
                segment_id: 0,
                first_sample_index: 0,
                device_ts_us: 0,
                pcm: HVec::new(),
            }),
            StreamFrame::Telemetry(Telemetry {
                device_ts_us: 0,
                kind: TelemetryKind::SpEnergy { values: [0.0; 4] },
            }),
            StreamFrame::SegmentEnd(SegmentEnd {
                segment_id: 0,
                device_ts_us: 0,
                frames_sent: 0,
                samples_sent: 0,
                reason: EndReason::VadRelease,
            }),
            StreamFrame::EndOfAudio(EndOfAudio {}),
            StreamFrame::FlushPlayback(FlushPlayback {}),
            StreamFrame::Status(Status {
                device_ts_us: 0,
                capture_head: 0,
                capture_head_ts_us: 0,
                levels: LevelRun::default(),
                counters: PodCounters::default(),
                gauges: PodGauges::default(),
                segment_reach: 0,
            }),
        ];
        for frame in &frames {
            let tag = connection_tag(frame);
            assert!(tag < STREAM_FRAME_TAG_CAPTURE, "{frame:?} has tag {tag}");
            assert_eq!(u32::from(frame_bytes(frame)[2]), tag);
        }
    }

    /// Neither connection decoder accepts the capture extension's tag.
    #[test]
    fn connection_decoders_refuse_the_capture_tag() {
        // Tag 64, then a `Read` body (nested tag 1 and three zero fields).
        let buf = [0x05, 0x00, 0x40, 0x01, 0x00, 0x00, 0x00];
        assert_eq!(decode_frame(&buf), Err(DecodeError::PostcardError));
        assert_eq!(decode_inbound(&buf), Err(DecodeError::PostcardError));
    }

    // ── EndOfAudio / FlushPlayback (v3 control frames) ─────────────────────────

    #[test]
    fn end_of_audio_roundtrip() {
        let frame = StreamFrame::EndOfAudio(EndOfAudio {});
        assert_eq!(roundtrip(&frame), frame);
    }

    #[test]
    fn flush_playback_roundtrip() {
        let frame = StreamFrame::FlushPlayback(FlushPlayback {});
        assert_eq!(roundtrip(&frame), frame);
    }

    #[test]
    fn end_of_audio_golden_bytes() {
        // An EndOfAudio frame's postcard payload is exactly its one-byte enum tag (5):
        // the body is a fieldless struct, which postcard encodes as zero bytes.
        let frame = StreamFrame::EndOfAudio(EndOfAudio {});
        let mut buf = [0u8; MAX_FRAME_BYTES + 2];
        let n = encode_frame(&frame, &mut buf).unwrap();
        assert_eq!(
            &buf[2..n],
            &[0x05],
            "EndOfAudio payload must be the single tag byte 5 (empty body)"
        );
    }

    #[test]
    fn flush_playback_golden_bytes() {
        // A FlushPlayback frame's postcard payload is exactly its one-byte enum tag (6).
        let frame = StreamFrame::FlushPlayback(FlushPlayback {});
        let mut buf = [0u8; MAX_FRAME_BYTES + 2];
        let n = encode_frame(&frame, &mut buf).unwrap();
        assert_eq!(
            &buf[2..n],
            &[0x06],
            "FlushPlayback payload must be the single tag byte 6 (empty body)"
        );
    }

    // ── Status (v4) ───────────────────────────────────────────────────────────

    /// A status frame carrying `n` blocks from `first_block`, with every optional
    /// counter and gauge `Some` when `first_block` is even and `None` when odd, so the
    /// round-trip cases cover both.
    fn status(first_block: u64, n: usize) -> Status {
        let some = first_block.is_multiple_of(2);
        let opt = |v: u32| if some { Some(v) } else { None };
        let mut blocks = HVec::new();
        for i in 0..n {
            blocks
                .push(LevelBlock {
                    half_db: (i % 201) as u8,
                    flags: if i % 3 == 0 { LEVEL_FLAG_PLAYING } else { 0 },
                })
                .unwrap();
        }
        Status {
            device_ts_us: 123_456_789,
            capture_head: 292_876_480,
            capture_head_ts_us: 123_456_700,
            levels: LevelRun {
                first_block,
                blocks,
            },
            counters: PodCounters {
                capture_deficit_frames: opt(7),
                capture_xruns: opt(1),
                playback_underruns: 3,
                playback_write_failures: opt(u32::MAX),
                playback_full_stalls: 9,
                playback_discards: opt(0),
                onsets: 44,
                segments_opened: 41,
                segments_dropped: 2,
                ring_overruns: 0,
                gate_msg_drops: 5,
                connects: 6,
                connect_failures: 1,
                status_skipped: 4,
                alloc_failures: opt(0),
            },
            gauges: PodGauges {
                heap_free: opt(81_234),
                heap_min: opt(60_000),
                heap_largest: opt(31_744),
                rssi_dbm: if some { Some(-61) } else { None },
                gate_open: some,
                gate_energy: if some { Some(0.41) } else { None },
                stack_free_min: opt(9_216),
            },
            segment_reach: if some { 16_000 } else { 4_321 },
        }
    }

    #[test]
    fn status_roundtrips_at_0_10_and_300_blocks_with_options_some_and_none() {
        for first_block in [182_000u64, 182_001] {
            for n in [0, 10, LEVEL_HISTORY_BLOCKS] {
                let frame = StreamFrame::Status(status(first_block, n));
                assert_eq!(
                    roundtrip(&frame),
                    frame,
                    "first_block {first_block}, {n} blocks"
                );
            }
        }
    }

    #[test]
    fn full_status_frame_fits_max_frame_bytes() {
        // Worst case: every varint at its widest and a full history.
        let mut s = status(u64::MAX, LEVEL_HISTORY_BLOCKS);
        s.device_ts_us = u64::MAX;
        s.capture_head = u64::MAX;
        s.capture_head_ts_us = u64::MAX;
        s.segment_reach = u32::MAX;
        s.counters = PodCounters {
            capture_deficit_frames: Some(u32::MAX),
            capture_xruns: Some(u32::MAX),
            playback_underruns: u32::MAX,
            playback_write_failures: Some(u32::MAX),
            playback_full_stalls: u32::MAX,
            playback_discards: Some(u32::MAX),
            onsets: u32::MAX,
            segments_opened: u32::MAX,
            segments_dropped: u32::MAX,
            ring_overruns: u32::MAX,
            gate_msg_drops: u32::MAX,
            connects: u32::MAX,
            connect_failures: u32::MAX,
            status_skipped: u32::MAX,
            alloc_failures: Some(u32::MAX),
        };
        s.gauges = PodGauges {
            heap_free: Some(u32::MAX),
            heap_min: Some(u32::MAX),
            heap_largest: Some(u32::MAX),
            rssi_dbm: Some(i8::MIN),
            gate_open: true,
            gate_energy: Some(f32::MAX),
            stack_free_min: Some(u32::MAX),
        };
        for b in s.levels.blocks.iter_mut() {
            *b = LevelBlock {
                half_db: 200,
                flags: 0xFF,
            };
        }
        let frame = StreamFrame::Status(s);
        let mut buf = [0u8; MAX_FRAME_BYTES + 2];
        let n = encode_frame(&frame, &mut buf).expect("a full status frame must encode");
        assert!(n - 2 <= MAX_FRAME_BYTES, "payload {} bytes", n - 2);
        assert_eq!(decode_frame(&buf[..n]).unwrap(), frame);
    }

    #[test]
    fn level_block_reads_back_dbfs_and_playing() {
        let b = LevelBlock {
            half_db: 157,
            flags: LEVEL_FLAG_PLAYING,
        };
        assert_eq!(b.dbfs(), -78.5);
        assert!(b.playing());
        assert!(
            !LevelBlock {
                half_db: 0,
                flags: 0
            }
            .playing()
        );
    }

    #[test]
    fn decode_inbound_ignores_status() {
        let bytes = frame_bytes(&StreamFrame::Status(status(4, 10)));
        assert_eq!(decode_inbound(&bytes), Ok(InboundFrame::Other(7)));
    }

    #[test]
    fn min_readable_version_is_three() {
        assert_eq!(AUDIO_PROTOCOL_VERSION, 4);
        assert_eq!(AUDIO_PROTOCOL_MIN_READABLE, 3);
    }

    #[test]
    fn decode_inbound_control_frames_classified() {
        // The two v3 control frames must classify to their dedicated InboundFrame
        // variants (not Other), so the device's consume loop can act on them directly.
        let eoa = frame_bytes(&StreamFrame::EndOfAudio(EndOfAudio {}));
        assert_eq!(decode_inbound(&eoa), Ok(InboundFrame::EndOfAudio));
        let flush = frame_bytes(&StreamFrame::FlushPlayback(FlushPlayback {}));
        assert_eq!(decode_inbound(&flush), Ok(InboundFrame::Flush));
    }

    // ── Framing error cases ───────────────────────────────────────────────────

    #[test]
    fn decode_truncated_length_prefix() {
        assert_eq!(decode_frame(&[]), Err(DecodeError::Truncated));
        assert_eq!(decode_frame(&[0x05]), Err(DecodeError::Truncated));
    }

    #[test]
    fn decode_truncated_payload() {
        // Length says 10 bytes but only 3 payload bytes present.
        let buf = [0x0A, 0x00, 0x01, 0x02, 0x03];
        assert_eq!(decode_frame(&buf), Err(DecodeError::Truncated));
    }

    #[test]
    fn decode_oversize_length_prefix_rejected() {
        // Length field = MAX_FRAME_BYTES + 1 → OversizeFrame.
        let too_big = (MAX_FRAME_BYTES + 1) as u16;
        let buf = [(too_big & 0xFF) as u8, (too_big >> 8) as u8];
        assert_eq!(
            decode_frame(&buf),
            Err(DecodeError::OversizeFrame {
                len: MAX_FRAME_BYTES + 1
            })
        );
    }

    #[test]
    fn decode_garbage_payload_returns_postcard_error() {
        // Valid length, garbage payload bytes → PostcardError.
        let payload = [0xDE, 0xAD, 0xBE, 0xEF];
        let len = payload.len() as u16;
        let mut buf = vec![0u8; 2 + payload.len()];
        buf[0] = (len & 0xFF) as u8;
        buf[1] = (len >> 8) as u8;
        buf[2..].copy_from_slice(&payload);
        assert_eq!(decode_frame(&buf), Err(DecodeError::PostcardError));
    }

    // ── Borrowing inbound decode (decode_inbound) ──────────────────────────────

    // Helper: build a full framed message (length prefix + postcard payload) for a frame.
    fn frame_bytes(frame: &StreamFrame) -> alloc::vec::Vec<u8> {
        let mut buf = [0u8; MAX_FRAME_BYTES + 2];
        let n = encode_frame(frame, &mut buf).expect("encode");
        buf[..n].to_vec()
    }

    #[test]
    fn decode_inbound_audio_full_capacity() {
        let mut pcm: HVec<u8, MAX_AUDIO_PAYLOAD> = HVec::new();
        for i in 0..MAX_AUDIO_PAYLOAD {
            pcm.push((i & 0xFF) as u8).unwrap();
        }
        let pcm_copy: alloc::vec::Vec<u8> = pcm.iter().copied().collect();
        let frame = StreamFrame::Audio(AudioFrame {
            segment_id: 7,
            first_sample_index: 12_345,
            device_ts_us: 6_789_012,
            pcm,
        });
        let bytes = frame_bytes(&frame);
        match decode_inbound(&bytes).expect("decode_inbound") {
            InboundFrame::Audio {
                segment_id,
                first_sample_index,
                device_ts_us,
                pcm,
            } => {
                assert_eq!(segment_id, 7);
                assert_eq!(first_sample_index, 12_345);
                assert_eq!(device_ts_us, 6_789_012);
                assert_eq!(pcm, &pcm_copy[..], "borrowed PCM must equal input PCM");
            }
            other => panic!("expected Audio, got {other:?}"),
        }
    }

    #[test]
    fn decode_inbound_audio_minimal() {
        let mut pcm: HVec<u8, MAX_AUDIO_PAYLOAD> = HVec::new();
        for i in 0u8..20 {
            pcm.push(i).unwrap();
        }
        let pcm_copy: alloc::vec::Vec<u8> = pcm.iter().copied().collect();
        let frame = StreamFrame::Audio(AudioFrame {
            segment_id: 2,
            first_sample_index: 320,
            device_ts_us: 20_000,
            pcm,
        });
        let bytes = frame_bytes(&frame);
        match decode_inbound(&bytes).expect("decode_inbound") {
            InboundFrame::Audio {
                segment_id,
                first_sample_index,
                device_ts_us,
                pcm,
            } => {
                assert_eq!(segment_id, 2);
                assert_eq!(first_sample_index, 320);
                assert_eq!(device_ts_us, 20_000);
                assert_eq!(pcm, &pcm_copy[..]);
            }
            other => panic!("expected Audio, got {other:?}"),
        }
    }

    #[test]
    fn decode_inbound_non_audio_variants_classified_other() {
        // `Hello` is no longer `Other` — it surfaces its format (see
        // `decode_inbound_hello_surfaces_format`).  The remaining non-`Audio` control
        // frames the device ignores on the inbound direction still classify as `Other`.
        let segment_start = StreamFrame::SegmentStart(SegmentStart {
            segment_id: 1,
            base_sample_index: 48_000,
            base_device_ts_us: 3_000_000,
            preroll_samples: 16_000,
        });
        let telemetry = StreamFrame::Telemetry(Telemetry {
            device_ts_us: 1_000_000,
            kind: TelemetryKind::SpEnergy {
                values: [100.0, 200.0, 150.0, 50.0],
            },
        });
        let segment_end = StreamFrame::SegmentEnd(SegmentEnd {
            segment_id: 1,
            device_ts_us: 5_000_000,
            frames_sent: 250,
            samples_sent: 80_000,
            reason: EndReason::VadRelease,
        });
        let expected_tags = [
            STREAM_FRAME_TAG_SEGMENT_START,
            STREAM_FRAME_TAG_TELEMETRY,
            STREAM_FRAME_TAG_SEGMENT_END,
        ];
        for (frame, tag) in [segment_start, telemetry, segment_end]
            .into_iter()
            .zip(expected_tags)
        {
            let bytes = frame_bytes(&frame);
            assert_eq!(
                decode_inbound(&bytes),
                Ok(InboundFrame::Other(tag)),
                "non-Audio frame must classify as Other({tag}): {frame:?}"
            );
        }
    }

    #[test]
    fn decode_inbound_hello_surfaces_format() {
        // An inbound `Hello` (the server's handshake) must surface its four format
        // scalars as `InboundFrame::Hello`, where it previously classified as `Other`.
        // The `pod_id` is intentionally *not* surfaced — the device needs only the
        // playback format.  Non-default field values prove each is read, not defaulted.
        let hello = StreamFrame::Hello(Hello {
            version: AUDIO_PROTOCOL_VERSION,
            pod_id: HString::try_from("pod-server01").unwrap(),
            sample_rate_hz: 48_000,
            bits_per_sample: 24,
            channels: 2,
            codec: Codec::S16Le,
            channel_source: ChannelSource::AsrBeam,
        });
        let bytes = frame_bytes(&hello);
        assert_eq!(
            decode_inbound(&bytes),
            Ok(InboundFrame::Hello {
                version: AUDIO_PROTOCOL_VERSION,
                sample_rate_hz: 48_000,
                bits_per_sample: 24,
                channels: 2,
                codec: Codec::S16Le,
            }),
            "inbound Hello must surface its version + format scalars"
        );
    }

    /// Differential parity between the body-only `decode_inbound` Hello arm and the owned
    /// `decode_frame`: for a golden Hello frame and a battery of mutations, both paths
    /// must agree on accept/reject and — when accepting — on every surfaced format
    /// scalar.  This pins the `take_from_bytes::<Hello>` decode against the owned
    /// reference so a future postcard bump cannot silently diverge (notably on
    /// trailing-byte acceptance, which both paths inherit from postcard).
    #[test]
    fn decode_inbound_hello_differential_parity() {
        // Surfaced Hello scalars as an Option: Some on accept, None on reject.
        fn owned(bytes: &[u8]) -> Option<(u8, u32, u8, u8, Codec)> {
            match decode_frame(bytes) {
                Ok(StreamFrame::Hello(h)) => Some((
                    h.version,
                    h.sample_rate_hz,
                    h.bits_per_sample,
                    h.channels,
                    h.codec,
                )),
                Ok(other) => panic!("expected Hello from decode_frame, got {other:?}"),
                Err(_) => None,
            }
        }
        fn borrowed(bytes: &[u8]) -> Option<(u8, u32, u8, u8, Codec)> {
            match decode_inbound(bytes) {
                Ok(InboundFrame::Hello {
                    version,
                    sample_rate_hz,
                    bits_per_sample,
                    channels,
                    codec,
                }) => Some((version, sample_rate_hz, bits_per_sample, channels, codec)),
                Ok(other) => panic!("expected InboundFrame::Hello, got {other:?}"),
                Err(_) => None,
            }
        }
        // Prepend a fresh u16 LE length prefix to a raw postcard payload.
        fn reframe(payload: &[u8]) -> alloc::vec::Vec<u8> {
            let mut v = vec![(payload.len() & 0xFF) as u8, (payload.len() >> 8) as u8];
            v.extend_from_slice(payload);
            v
        }

        let golden = frame_bytes(&StreamFrame::Hello(Hello {
            version: AUDIO_PROTOCOL_VERSION,
            pod_id: HString::try_from("pod-server01").unwrap(),
            sample_rate_hz: 48_000,
            bits_per_sample: 24,
            channels: 2,
            codec: Codec::S16Le,
            channel_source: ChannelSource::AsrBeam,
        }));
        let payload = golden[2..].to_vec();

        // Trailing garbage after a valid body — postcard ignores the remainder, so both
        // paths accept.
        let mut trailing = payload.clone();
        trailing.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        let trailing = reframe(&trailing);
        // Truncated body — both reject.
        let truncated = reframe(&payload[..payload.len() - 3]);
        // Bad codec tag — codec is the second-to-last body byte (channel_source last).
        let mut bad_codec = payload.clone();
        let ci = bad_codec.len() - 2;
        bad_codec[ci] = 0x05; // no such Codec discriminant
        let bad_codec = reframe(&bad_codec);
        // Bad channel_source tag — the last body byte; only 0x00..=0x05 are valid, both
        // paths deserialize the full body so both reject an out-of-range discriminant.
        let mut bad_channel_source = payload.clone();
        let si = bad_channel_source.len() - 1;
        bad_channel_source[si] = 0x7F; // no such ChannelSource discriminant
        let bad_channel_source = reframe(&bad_channel_source);
        // Oversize pod_id — declared String<32> length 40 exceeds capacity, both reject.
        let mut oversize = vec![STREAM_FRAME_TAG_HELLO as u8, AUDIO_PROTOCOL_VERSION, 0x28];
        oversize.extend_from_slice(&[b'x'; 40]);
        oversize.extend_from_slice(&[0x80, 0x7d, 0x10, 0x01, 0x00, 0x00]);
        let oversize = reframe(&oversize);

        for (name, bytes) in [
            ("golden", golden.as_slice()),
            ("trailing", trailing.as_slice()),
            ("truncated", truncated.as_slice()),
            ("bad_codec", bad_codec.as_slice()),
            ("bad_channel_source", bad_channel_source.as_slice()),
            ("oversize_pod_id", oversize.as_slice()),
        ] {
            assert_eq!(
                borrowed(bytes),
                owned(bytes),
                "decode_inbound and decode_frame disagree on Hello case `{name}`"
            );
        }

        // Pin each mutation's intent so a future encoding shift can't turn a reject-case
        // into a silent both-accept that still passes the equality above.
        assert!(borrowed(golden.as_slice()).is_some(), "golden must accept");
        assert!(
            borrowed(trailing.as_slice()).is_some(),
            "trailing bytes must be accepted (parity with owned path)"
        );
        assert!(
            borrowed(truncated.as_slice()).is_none(),
            "truncated body must reject"
        );
        assert!(
            borrowed(bad_codec.as_slice()).is_none(),
            "bad codec tag must reject"
        );
        assert!(
            borrowed(bad_channel_source.as_slice()).is_none(),
            "bad channel_source tag must reject"
        );
        assert!(
            borrowed(oversize.as_slice()).is_none(),
            "oversize pod_id must reject"
        );
    }

    #[test]
    fn check_inbound_format_all_match() {
        assert_eq!(
            check_inbound_format(DEVICE_PLAYBACK_FORMAT, DEVICE_PLAYBACK_FORMAT),
            FormatCheck::Match,
            "a Hello declaring the device's exact format must validate"
        );
    }

    #[test]
    fn check_inbound_format_sample_rate_mismatch() {
        let declared = PlaybackFormat {
            sample_rate_hz: 48_000,
            ..DEVICE_PLAYBACK_FORMAT
        };
        assert_eq!(
            check_inbound_format(DEVICE_PLAYBACK_FORMAT, declared),
            FormatCheck::Mismatch {
                field: FormatField::SampleRateHz,
                expected: 16_000,
                actual: 48_000,
            },
            "a declared sample rate ≠ 16 kHz must be reported with both values"
        );
    }

    #[test]
    fn check_inbound_format_bits_per_sample_mismatch() {
        let declared = PlaybackFormat {
            bits_per_sample: 24,
            ..DEVICE_PLAYBACK_FORMAT
        };
        assert_eq!(
            check_inbound_format(DEVICE_PLAYBACK_FORMAT, declared),
            FormatCheck::Mismatch {
                field: FormatField::BitsPerSample,
                expected: 16,
                actual: 24,
            },
            "a declared bit depth ≠ 16 must be reported with both values"
        );
    }

    #[test]
    fn check_inbound_format_channels_mismatch() {
        let declared = PlaybackFormat {
            channels: 2,
            ..DEVICE_PLAYBACK_FORMAT
        };
        assert_eq!(
            check_inbound_format(DEVICE_PLAYBACK_FORMAT, declared),
            FormatCheck::Mismatch {
                field: FormatField::Channels,
                expected: 1,
                actual: 2,
            },
            "a declared channel count ≠ 1 (mono) must be reported with both values"
        );
    }

    #[test]
    fn check_inbound_format_codec_matches_only_supported_value() {
        // `Codec` is a single-variant enum (`S16Le`) today, so a genuine codec *mismatch*
        // is unconstructible from safe code — there is no second value to differ against.
        // What is testable: the codec branch is *reached* (all earlier fields equal) and
        // returns `Match` when codecs agree, and the `Codec as u32` discriminant carry the
        // `Mismatch` arm would use is the golden-pinned 0.  When a second codec is added,
        // a true mismatch case joins the per-field set above (same `Mismatch { field:
        // FormatField::Codec, .. }` shape).
        assert_eq!(
            check_inbound_format(DEVICE_PLAYBACK_FORMAT, DEVICE_PLAYBACK_FORMAT),
            FormatCheck::Match,
            "the codec branch must pass when the only supported codec is declared"
        );
        assert_eq!(
            Codec::S16Le as u32,
            0,
            "S16Le discriminant is golden-pinned at 0 (the value the Codec Mismatch arm carries)"
        );
    }

    #[test]
    fn check_inbound_format_reports_first_field_only() {
        // A Hello differing in *several* fields must report the earliest-declared one
        // (sample_rate_hz), not later ones — the gate short-circuits on first difference.
        let declared = PlaybackFormat {
            sample_rate_hz: 8_000,
            bits_per_sample: 8,
            channels: 2,
            codec: Codec::S16Le,
        };
        assert_eq!(
            check_inbound_format(DEVICE_PLAYBACK_FORMAT, declared),
            FormatCheck::Mismatch {
                field: FormatField::SampleRateHz,
                expected: 16_000,
                actual: 8_000,
            },
            "a multi-field mismatch must name the first differing field"
        );
    }

    #[test]
    fn decode_inbound_truncated_length_prefix() {
        assert_eq!(decode_inbound(&[]), Err(DecodeError::Truncated));
        assert_eq!(decode_inbound(&[0x05]), Err(DecodeError::Truncated));
    }

    #[test]
    fn decode_inbound_truncated_payload() {
        // Length says 10 bytes but only 3 payload bytes present.
        let buf = [0x0A, 0x00, 0x01, 0x02, 0x03];
        assert_eq!(decode_inbound(&buf), Err(DecodeError::Truncated));
    }

    #[test]
    fn decode_inbound_oversize_length_prefix_rejected() {
        let too_big = (MAX_FRAME_BYTES + 1) as u16;
        let buf = [(too_big & 0xFF) as u8, (too_big >> 8) as u8];
        assert_eq!(
            decode_inbound(&buf),
            Err(DecodeError::OversizeFrame {
                len: MAX_FRAME_BYTES + 1
            })
        );
    }

    #[test]
    fn decode_inbound_garbage_payload_returns_postcard_error() {
        // Valid length, garbage payload bytes.  Every byte has its continuation bit
        // set (0x80), so the leading tag varint never terminates within the payload —
        // `take_u32` runs out of bytes and returns PostcardError.  This is the
        // *malformed-varint* path, distinct from the *valid-but-unknown-tag* path
        // covered by `decode_inbound_unknown_variant_tag_rejected`.  Keep all bytes
        // ≥ 0x80 so this never accidentally becomes a clean single-byte tag.
        let payload = [0xFF, 0xFF, 0xFF, 0xFF];
        let len = payload.len() as u16;
        let mut buf = vec![0u8; 2 + payload.len()];
        buf[0] = (len & 0xFF) as u8;
        buf[1] = (len >> 8) as u8;
        buf[2..].copy_from_slice(&payload);
        assert_eq!(decode_inbound(&buf), Err(DecodeError::PostcardError));
    }

    #[test]
    fn decode_inbound_unknown_variant_tag_rejected() {
        // A valid varint tag of 9 (no such StreamFrame variant) must be rejected,
        // not silently treated as Other.
        // postcard payload: tag varint = 9 (single byte 0x09).
        let payload = [0x09u8];
        let len = payload.len() as u16;
        let mut buf = vec![0u8; 2 + payload.len()];
        buf[0] = (len & 0xFF) as u8;
        buf[1] = (len >> 8) as u8;
        buf[2..].copy_from_slice(&payload);
        assert_eq!(decode_inbound(&buf), Err(DecodeError::PostcardError));
    }

    #[test]
    fn stream_frame_tag_constants_match_encoded_discriminants() {
        // The STREAM_FRAME_TAG_* constants are coupled-by-convention to the declaration
        // order of `StreamFrame` (postcard encodes the enum tag as a varint of the
        // variant's positional index).  If a variant is inserted/reordered/removed, the
        // derived encoder reassigns discriminants automatically while the manual
        // constants do not — silently breaking `decode_inbound`.  This test reads the
        // first postcard byte of each encoded variant and asserts it equals the matching
        // constant, so a reorder fails loudly here (host `make check`) instead of as a
        // wire-protocol misclassification on-device.  Each constant is < 128, so its
        // varint encoding is exactly one byte == the constant value.
        let cases: [(StreamFrame, u32); 8] = [
            (
                StreamFrame::Hello(Hello {
                    version: AUDIO_PROTOCOL_VERSION,
                    pod_id: HString::try_from("pod-aabbcc").unwrap(),
                    sample_rate_hz: 16_000,
                    bits_per_sample: 16,
                    channels: 1,
                    codec: Codec::S16Le,
                    channel_source: ChannelSource::AsrBeam,
                }),
                STREAM_FRAME_TAG_HELLO,
            ),
            (
                StreamFrame::SegmentStart(SegmentStart {
                    segment_id: 1,
                    base_sample_index: 0,
                    base_device_ts_us: 0,
                    preroll_samples: 0,
                }),
                STREAM_FRAME_TAG_SEGMENT_START,
            ),
            (
                StreamFrame::Audio(AudioFrame {
                    segment_id: 0,
                    first_sample_index: 0,
                    device_ts_us: 0,
                    pcm: HVec::new(),
                }),
                STREAM_FRAME_TAG_AUDIO,
            ),
            (
                StreamFrame::Telemetry(Telemetry {
                    device_ts_us: 0,
                    kind: TelemetryKind::SpEnergy { values: [0.0; 4] },
                }),
                STREAM_FRAME_TAG_TELEMETRY,
            ),
            (
                StreamFrame::SegmentEnd(SegmentEnd {
                    segment_id: 0,
                    device_ts_us: 0,
                    frames_sent: 0,
                    samples_sent: 0,
                    reason: EndReason::VadRelease,
                }),
                STREAM_FRAME_TAG_SEGMENT_END,
            ),
            (
                StreamFrame::EndOfAudio(EndOfAudio {}),
                STREAM_FRAME_TAG_END_OF_AUDIO,
            ),
            (
                StreamFrame::FlushPlayback(FlushPlayback {}),
                STREAM_FRAME_TAG_FLUSH_PLAYBACK,
            ),
            (StreamFrame::Status(status(0, 0)), STREAM_FRAME_TAG_STATUS),
        ];
        for (frame, expected_tag) in cases {
            let bytes = frame_bytes(&frame);
            // bytes[0..2] = u16 LE length prefix; bytes[2] = first postcard byte = tag varint.
            assert!(expected_tag < 128, "constant must fit a one-byte varint");
            assert_eq!(
                bytes[2] as u32, expected_tag,
                "encoded discriminant for {frame:?} must match its STREAM_FRAME_TAG_* constant; \
                 a mismatch means StreamFrame was reordered without updating the constants"
            );
        }
    }

    #[test]
    fn decode_inbound_pcm_over_capacity_rejected() {
        // Hand-build an Audio frame whose declared PCM run exceeds MAX_AUDIO_PAYLOAD.
        // This is a syntactically valid postcard Audio frame (tag + 3 small varint
        // headers + an over-long Vec<u8>); decode_inbound must reject the PCM length
        // rather than return an over-long slice.
        // tag = Audio (2), segment_id = 0, first_sample_index = 0, device_ts_us = 0,
        // then the pcm Vec<u8> length varint = MAX_AUDIO_PAYLOAD + 1 (= 1281).
        // 1281 = 0x501 → varint LE base-128: 0x81, 0x0A.
        let over = (MAX_AUDIO_PAYLOAD + 1) as u32;
        let mut payload: alloc::vec::Vec<u8> = vec![
            STREAM_FRAME_TAG_AUDIO as u8,
            0x00,
            0x00,
            0x00,
            (over as u8 & 0x7F) | 0x80,
            (over >> 7) as u8,
        ];
        // Append that many PCM bytes so the frame is otherwise well-formed and within
        // MAX_FRAME_BYTES (1281 + ~6 header bytes < 4096).
        payload.extend(core::iter::repeat_n(0xABu8, over as usize));

        let plen = payload.len() as u16;
        let mut buf = vec![0u8; 2 + payload.len()];
        buf[0] = (plen & 0xFF) as u8;
        buf[1] = (plen >> 8) as u8;
        buf[2..].copy_from_slice(&payload);

        assert_eq!(
            decode_inbound(&buf),
            Err(DecodeError::OversizePcm {
                len: MAX_AUDIO_PAYLOAD + 1
            })
        );
    }

    #[test]
    fn decode_inbound_audio_truncated_pcm_run() {
        // Declared PCM length is valid (≤ capacity) but the run is short.
        // tag = Audio, segment_id = 0, first_sample_index = 0, device_ts_us = 0,
        // pcm length = 8, but only 3 PCM bytes present.
        let mut payload: alloc::vec::Vec<u8> =
            vec![STREAM_FRAME_TAG_AUDIO as u8, 0x00, 0x00, 0x00, 0x08];
        payload.extend([0u8; 3]);

        let plen = payload.len() as u16;
        let mut buf = vec![0u8; 2 + payload.len()];
        buf[0] = (plen & 0xFF) as u8;
        buf[1] = (plen >> 8) as u8;
        buf[2..].copy_from_slice(&payload);

        assert_eq!(decode_inbound(&buf), Err(DecodeError::Truncated));
    }

    #[test]
    fn decode_inbound_equivalence_with_decode_frame() {
        // For a corpus of frames, decode_inbound's classification + PCM bytes must
        // agree with decode_frame's owned StreamFrame — proving wire compatibility.
        let mut full_pcm: HVec<u8, MAX_AUDIO_PAYLOAD> = HVec::new();
        for i in 0..MAX_AUDIO_PAYLOAD {
            full_pcm.push((i & 0xFF) as u8).unwrap();
        }
        let mut small_pcm: HVec<u8, MAX_AUDIO_PAYLOAD> = HVec::new();
        for i in 0u8..40 {
            small_pcm.push(i).unwrap();
        }
        let corpus = [
            StreamFrame::Hello(Hello {
                version: AUDIO_PROTOCOL_VERSION,
                pod_id: HString::try_from("pod-aabbcc").unwrap(),
                sample_rate_hz: 16_000,
                bits_per_sample: 16,
                channels: 1,
                codec: Codec::S16Le,
                channel_source: ChannelSource::CommunicationBeam,
            }),
            StreamFrame::SegmentStart(SegmentStart {
                segment_id: 3,
                base_sample_index: 96_000,
                base_device_ts_us: 7_000_000,
                preroll_samples: 8_000,
            }),
            StreamFrame::Audio(AudioFrame {
                segment_id: 4,
                first_sample_index: 640,
                device_ts_us: 40_000,
                pcm: small_pcm,
            }),
            StreamFrame::Audio(AudioFrame {
                segment_id: 5,
                first_sample_index: 1_280,
                device_ts_us: 80_000,
                pcm: full_pcm,
            }),
            StreamFrame::Telemetry(Telemetry {
                device_ts_us: 9_000_000,
                kind: TelemetryKind::Azimuths {
                    values: [0.1, 0.2, 0.3, 0.4],
                },
            }),
            StreamFrame::SegmentEnd(SegmentEnd {
                segment_id: 6,
                device_ts_us: 10_000_000,
                frames_sent: 100,
                samples_sent: 32_000,
                reason: EndReason::Overrun,
            }),
            StreamFrame::EndOfAudio(EndOfAudio {}),
            StreamFrame::FlushPlayback(FlushPlayback {}),
        ];
        for frame in &corpus {
            let bytes = frame_bytes(frame);
            let owned = decode_frame(&bytes).expect("decode_frame");
            let borrowed = decode_inbound(&bytes).expect("decode_inbound");
            match (&owned, borrowed) {
                (
                    StreamFrame::Audio(af),
                    InboundFrame::Audio {
                        segment_id,
                        first_sample_index,
                        device_ts_us,
                        pcm,
                    },
                ) => {
                    assert_eq!(segment_id, af.segment_id);
                    assert_eq!(first_sample_index, af.first_sample_index);
                    assert_eq!(device_ts_us, af.device_ts_us);
                    assert_eq!(pcm, &af.pcm[..]);
                }
                (StreamFrame::Audio(_), other) => {
                    panic!("decode_frame said Audio but decode_inbound said {other:?}")
                }
                (
                    StreamFrame::Hello(h),
                    InboundFrame::Hello {
                        version,
                        sample_rate_hz,
                        bits_per_sample,
                        channels,
                        codec,
                    },
                ) => {
                    assert_eq!(version, h.version);
                    assert_eq!(sample_rate_hz, h.sample_rate_hz);
                    assert_eq!(bits_per_sample, h.bits_per_sample);
                    assert_eq!(channels, h.channels);
                    assert_eq!(codec, h.codec);
                }
                (StreamFrame::Hello(_), other) => {
                    panic!("decode_frame said Hello but decode_inbound said {other:?}")
                }
                (StreamFrame::EndOfAudio(_), InboundFrame::EndOfAudio) => { /* both agree */ }
                (StreamFrame::EndOfAudio(_), other) => {
                    panic!("decode_frame said EndOfAudio but decode_inbound said {other:?}")
                }
                (StreamFrame::FlushPlayback(_), InboundFrame::Flush) => { /* both agree */ }
                (StreamFrame::FlushPlayback(_), other) => {
                    panic!("decode_frame said FlushPlayback but decode_inbound said {other:?}")
                }
                (_, InboundFrame::Other(_)) => { /* both agree: non-Audio */ }
                (_, InboundFrame::Audio { .. }) => {
                    panic!("decode_inbound said Audio for a non-Audio frame")
                }
                (_, InboundFrame::Hello { .. }) => {
                    panic!("decode_inbound said Hello for a non-Hello frame")
                }
                (_, InboundFrame::EndOfAudio) => {
                    panic!("decode_inbound said EndOfAudio for a non-EndOfAudio frame")
                }
                (_, InboundFrame::Flush) => {
                    panic!("decode_inbound said Flush for a non-Flush frame")
                }
            }
        }
    }

    #[test]
    fn encode_buffer_too_small() {
        let frame = StreamFrame::Hello(Hello {
            version: 1,
            pod_id: HString::try_from("x").unwrap(),
            sample_rate_hz: 16_000,
            bits_per_sample: 16,
            channels: 1,
            codec: Codec::S16Le,
            channel_source: ChannelSource::AsrBeam,
        });
        let mut buf = [0u8; 1]; // too small even for the prefix
        assert_eq!(
            encode_frame(&frame, &mut buf),
            Err(EncodeError::BufferTooSmall)
        );
    }
}
