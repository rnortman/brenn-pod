//! Wire protocol for the audio transport.
//!
//! **Framing:** `u16` LE length prefix followed by a postcard-encoded `StreamFrame`
//! payload.  A frame with a length field exceeding `MAX_FRAME_BYTES` is a protocol
//! error; the receiver should log and drop the connection.
//!
//! **Bidirectional use.** The same framing and `StreamFrame` type are used in both
//! directions on the TCP connection.  The device→server direction carries the full
//! set of variants (`Hello`, `SegmentStart`, `Audio`, `Telemetry`, `SegmentEnd`).
//! The server→device direction carries a leading `Hello` followed by `Audio` frames:
//! the server MUST send a `Hello` as its **first** inbound frame so the device can
//! validate the playback format (sample rate / bit depth / channels / codec) before
//! interpreting any `Audio` payload.  Both directions therefore follow the "first frame
//! must be `Hello`" rule; each side's `Hello` validates the direction it heads.  A
//! server that streams `Audio` without a leading inbound `Hello` is a legacy /
//! non-conforming peer: the device assumes its fixed 16 kHz / 16-bit / mono S16_LE
//! format and warns that the handshake was missing (see the device-side inbound path).
//!
//! **Version:** `Hello::version` must equal `AUDIO_PROTOCOL_VERSION`; a mismatch is
//! a fatal protocol error on the receiver side.  The version guards schema changes;
//! it is not bumped when the server begins sending `Audio` frames back on the
//! previously silent server→device direction, because the wire schema is unchanged.

use heapless::{String as HString, Vec as HVec};
use serde::{Deserialize, Serialize};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Wire protocol version embedded in `Hello`.  Increment on breaking schema changes.
///
/// v2: `Hello` gained an explicit `codec: Codec` field so the PCM encoding is a
/// negotiated part of the handshake rather than an implicit S16_LE convention.
/// v3: appended `StreamFrame::EndOfAudio` (tag 5) and `StreamFrame::FlushPlayback`
/// (tag 6) — the explicit end-of-audio marker and the flush/stop control frame for
/// the device-side mute policy and barge-in.  A v2 peer would die on the first
/// unknown tag mid-stream, so the version is now checked fatally on *both* ends.
pub const AUDIO_PROTOCOL_VERSION: u8 = 3;

/// Maximum postcard payload size (bytes), excluding the 2-byte length prefix.
/// A received frame length field exceeding this is a protocol violation.
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
    EndOfAudio(EndOfAudio),
    /// Server → device: discard everything banked and go silent immediately
    /// (flush/stop, for barge-in).  Empty body today; struct-typed so fields can
    /// be appended later without re-tagging.
    FlushPlayback(FlushPlayback),
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
    /// Number of PCM channels in `AudioFrame::pcm` (1 = mono, 2 = stereo).
    pub channels: u8,
    /// PCM sample encoding in `AudioFrame::pcm`.  Negotiated explicitly rather than
    /// assumed: today `S16Le` is the only supported value, but carrying it on the
    /// handshake lets a receiver reject an unsupported codec instead of misinterpreting
    /// the bytes.
    pub codec: Codec,
    /// Which XVF3800 output is captured.
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
    /// Signed 16-bit little-endian PCM.  The only currently supported encoding.
    /// Discriminant 0x00 — **do not reorder or rename** (golden-byte pinned).
    S16Le,
}

/// Which XVF3800 beam output is captured and sent.
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
}

/// Begins an utterance segment.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct SegmentStart {
    /// Monotonically increasing per-boot segment counter.
    pub segment_id: u32,
    /// Absolute sample index (since capture start) of this segment's first sample.
    /// Includes pre-roll samples.
    pub base_sample_index: u64,
    /// `esp_timer_get_time()` µs at the moment `base_sample_index` was captured.
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
    /// `esp_timer_get_time()` µs when this frame's first sample was captured.
    pub device_ts_us: u64,
    /// Raw PCM bytes.  S16_LE, little-endian.  Interleaved channels if `channels > 1`.
    /// Capacity `MAX_AUDIO_PAYLOAD` = 1280 bytes (320 samples × 2 ch × 2 B).
    pub pcm: HVec<u8, MAX_AUDIO_PAYLOAD>,
}

const _: () = assert!(
    AUDIO_SAMPLES_PER_FRAME * 2 <= MAX_AUDIO_PAYLOAD,
    "a full mono frame must fit MAX_AUDIO_PAYLOAD"
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
    /// `esp_timer_get_time()` µs when the I2C read completed.
    pub device_ts_us: u64,
    /// The specific register values read.
    pub kind: TelemetryKind,
}

/// Discriminated telemetry payload.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryKind {
    /// AEC_AZIMUTH_VALUES (resid 33, cmd 75): four tracked beam azimuths in radians.
    /// Indices: 0=focused-A, 1=focused-B, 2=free-running, 3=auto-select winner.
    /// NaN is valid on indices 0, 1, 3 when no beam is tracked.
    Azimuths { values: [f32; 4] },
    /// AEC_SPENERGY_VALUES (resid 33, cmd 80): four beam speech-energy readings.
    SpEnergy { values: [f32; 4] },
}

/// Closes an utterance segment.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct SegmentEnd {
    /// Must match the opened segment's `segment_id`.
    pub segment_id: u32,
    /// `esp_timer_get_time()` µs at close.
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
    /// Any other non-`Audio` variant (`SegmentStart`, `Telemetry`, `SegmentEnd`).
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
            // SegmentStart / Telemetry / SegmentEnd — device ignores inbound.  Validate
            // only that the tag is a known variant; do not walk the body (the consume
            // loop advances by the framed length, which is already known).
            STREAM_FRAME_TAG_SEGMENT_START
            | STREAM_FRAME_TAG_TELEMETRY
            | STREAM_FRAME_TAG_SEGMENT_END => Ok(InboundFrame::Other(tag)),
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

/// Errors returned by `encode_frame`.
#[derive(Debug, PartialEq)]
pub enum EncodeError {
    /// Output buffer is smaller than 2 bytes.
    BufferTooSmall,
    /// Postcard payload would exceed `MAX_FRAME_BYTES`.
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
    /// Length prefix exceeds `MAX_FRAME_BYTES`.
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
        // Bad channel_source tag — the last body byte; only 0x00..=0x03 are valid, both
        // paths deserialize the full body so both reject an out-of-range discriminant.
        let mut bad_channel_source = payload.clone();
        let si = bad_channel_source.len() - 1;
        bad_channel_source[si] = 0x05; // no such ChannelSource discriminant
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
        let cases: [(StreamFrame, u32); 7] = [
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
