//! The frame type a frame log holds: every connection frame, with audio of any length
//! up to the frame log's record limit, plus the capture extension.
//!
//! The framing is the connection codec's — a `u16` LE length prefix, then the postcard
//! payload — with [`MAX_WIDE_FRAME_BYTES`] as the limit.  A payload whose first byte is
//! [`STREAM_FRAME_TAG_CAPTURE`] is a [`CaptureFrame`] after that byte; any other payload
//! decodes as the connection frame with that tag, so a connection frame encodes to the
//! same bytes through [`encode_wide`] as through [`encode_frame`](crate::wire::encode_frame).
//!
//! The capture schema has its own version, [`CAPTURE_SCHEMA_VERSION`], carried in
//! [`CaptureFrame::Describe`].  It covers the capture bodies and the enum values only a
//! capture stream uses, and is bumped when a body's layout changes or a body is appended.

use serde::{Deserialize, Serialize};
use std::string::String;
use std::vec::Vec;

use crate::wire::{
    DecodeError, EncodeError, EndOfAudio, FlushPlayback, Hello, STREAM_FRAME_TAG_CAPTURE,
    SegmentEnd, SegmentStart, Status, StreamFrame, Telemetry,
};

/// The version of the capture schema a [`CaptureFrame::Describe`] states.
pub const CAPTURE_SCHEMA_VERSION: u8 = 1;

/// The most postcard bytes one wide frame holds: `u16::MAX` less the 2-byte length
/// prefix, so one framed frame is one frame-log record.
pub const MAX_WIDE_FRAME_BYTES: usize = u16::MAX as usize - 2;

/// The longest PCM run an audio frame carries within [`MAX_WIDE_FRAME_BYTES`] whatever
/// its other fields hold: the limit less the worst-case 29 bytes around the PCM (tag 1,
/// `segment_id` 5, `first_sample_index` 10, `device_ts_us` 10, and 3 for the length of
/// a run above 16383 bytes).
pub const MAX_WIDE_PCM_BYTES: usize = MAX_WIDE_FRAME_BYTES - 29;

/// A frame of a frame log.
///
/// `Status` carries its level history inline, as on the connection, so it is the
/// largest variant; a frame is decoded once and handed on, so the size is not boxed away.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, PartialEq)]
pub enum WideFrame {
    /// [`StreamFrame::Hello`].
    Hello(Hello),
    /// [`StreamFrame::SegmentStart`].
    SegmentStart(SegmentStart),
    /// [`StreamFrame::Audio`], with a PCM run of any length.
    Audio(WideAudio),
    /// [`StreamFrame::Telemetry`].
    Telemetry(Telemetry),
    /// [`StreamFrame::SegmentEnd`].
    SegmentEnd(SegmentEnd),
    /// [`StreamFrame::EndOfAudio`].
    EndOfAudio(EndOfAudio),
    /// [`StreamFrame::FlushPlayback`].
    FlushPlayback(FlushPlayback),
    /// [`StreamFrame::Status`].
    Status(Status),
    /// The capture extension, tag [`STREAM_FRAME_TAG_CAPTURE`].
    Capture(CaptureFrame),
}

/// An audio frame whose PCM run is bounded only by [`MAX_WIDE_FRAME_BYTES`].  The same
/// fields, in the same order, as [`AudioFrame`](crate::wire::AudioFrame).
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct WideAudio {
    /// The enclosing segment's `segment_id`.
    pub segment_id: u32,
    /// Absolute sample index of the first sample in `pcm`.
    pub first_sample_index: u64,
    /// Device monotonic µs when the first sample was captured.
    pub device_ts_us: u64,
    /// Interleaved PCM in the hello's codec.
    pub pcm: Vec<u8>,
}

/// The body of a capture-extension frame.  Positional and append-only under
/// [`CAPTURE_SCHEMA_VERSION`].
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum CaptureFrame {
    /// Sent once, directly after the hello.
    Describe {
        /// The capture schema the stream uses.
        schema: u8,
        /// One name per channel, in interleave order; exactly `Hello::channels` of them.
        channel_names: Vec<String>,
        /// The block size the source granted: reads are sized by it and a block is late
        /// against it.
        period_frames: u32,
        /// The monotonic reading taken directly after the source started, paired with
        /// the record's stamp.
        mono_us: u64,
    },
    /// Follows each audio frame and names it.
    Read {
        /// The `first_sample_index` of the audio frame before it.
        first_sample_index: u64,
        /// The monotonic reading when the read that delivered the block returned, paired
        /// with the record's stamp.
        mono_us: u64,
        /// Frames already waiting in the source's buffer at that moment.  The block is
        /// late when this is at least `Describe::period_frames`.
        backlog_frames: u32,
    },
    /// Audio is missing at `at_index`.
    Loss {
        /// What lost it.
        cause: LossCause,
        /// The sample index the loss is at.
        at_index: u64,
        /// The exact count when the source delivered the audio and the producer did not
        /// carry it; `None` when the source itself lost it.
        samples: Option<u64>,
        /// The monotonic reading of the loss, paired with the record's stamp.
        mono_us: u64,
        /// The source's own error text.
        detail: String,
    },
    /// The last frame of a stream that ended itself.
    End {
        /// Why it ended.
        reason: StreamEndReason,
        /// The sample index after the last sample delivered.
        at_index: u64,
        /// The monotonic reading of the end, paired with the record's stamp.
        mono_us: u64,
        /// The source's or the destination's own error text.
        detail: String,
    },
}

/// What lost audio.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum LossCause {
    /// The source delivered it and the producer could not carry it.
    WriterBacklog,
    /// The source's buffer overran.
    Overrun,
    /// The source was suspended.
    Suspend,
    /// The source's transfer failed.
    TransferError,
}

/// Why a capture stream ended.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum StreamEndReason {
    /// The producer was asked to stop.
    Stopped,
    /// The destination ran short of space.
    DestinationFull,
    /// The source stopped delivering and could not be recovered.
    StreamDied,
    /// A write to the destination failed.
    WriteFailed,
}

impl From<StreamFrame> for WideFrame {
    fn from(frame: StreamFrame) -> Self {
        match frame {
            StreamFrame::Hello(b) => WideFrame::Hello(b),
            StreamFrame::SegmentStart(b) => WideFrame::SegmentStart(b),
            StreamFrame::Audio(a) => WideFrame::Audio(WideAudio {
                segment_id: a.segment_id,
                first_sample_index: a.first_sample_index,
                device_ts_us: a.device_ts_us,
                pcm: a.pcm.to_vec(),
            }),
            StreamFrame::Telemetry(b) => WideFrame::Telemetry(b),
            StreamFrame::SegmentEnd(b) => WideFrame::SegmentEnd(b),
            StreamFrame::EndOfAudio(b) => WideFrame::EndOfAudio(b),
            StreamFrame::FlushPlayback(b) => WideFrame::FlushPlayback(b),
            StreamFrame::Status(b) => WideFrame::Status(b),
        }
    }
}

/// [`StreamFrame`]'s variants in declaration order, borrowed, so the derive encodes a
/// connection frame's tag and body exactly as the connection codec does.
#[derive(Serialize)]
enum ConnectionRef<'a> {
    Hello(&'a Hello),
    SegmentStart(&'a SegmentStart),
    Audio(&'a WideAudio),
    Telemetry(&'a Telemetry),
    SegmentEnd(&'a SegmentEnd),
    EndOfAudio(&'a EndOfAudio),
    FlushPlayback(&'a FlushPlayback),
    Status(&'a Status),
}

/// [`StreamFrame`]'s variants in declaration order, owned, for decoding.
#[allow(clippy::large_enum_variant)]
#[derive(Deserialize)]
enum ConnectionOwned {
    Hello(Hello),
    SegmentStart(SegmentStart),
    Audio(WideAudio),
    Telemetry(Telemetry),
    SegmentEnd(SegmentEnd),
    EndOfAudio(EndOfAudio),
    FlushPlayback(FlushPlayback),
    Status(Status),
}

impl From<ConnectionOwned> for WideFrame {
    fn from(frame: ConnectionOwned) -> Self {
        match frame {
            ConnectionOwned::Hello(b) => WideFrame::Hello(b),
            ConnectionOwned::SegmentStart(b) => WideFrame::SegmentStart(b),
            ConnectionOwned::Audio(b) => WideFrame::Audio(b),
            ConnectionOwned::Telemetry(b) => WideFrame::Telemetry(b),
            ConnectionOwned::SegmentEnd(b) => WideFrame::SegmentEnd(b),
            ConnectionOwned::EndOfAudio(b) => WideFrame::EndOfAudio(b),
            ConnectionOwned::FlushPlayback(b) => WideFrame::FlushPlayback(b),
            ConnectionOwned::Status(b) => WideFrame::Status(b),
        }
    }
}

/// Encode `frame` into `buf` as a `u16` LE length prefix followed by the postcard
/// payload.  Returns the number of bytes written (2 + payload length).
///
/// Refuses a payload of more than [`MAX_WIDE_FRAME_BYTES`] with
/// [`EncodeError::PayloadTooLarge`], and a `buf` too small for the frame with
/// [`EncodeError::BufferTooSmall`] (prefix) or [`EncodeError::PostcardError`] (payload).
pub fn encode_wide(frame: &WideFrame, buf: &mut [u8]) -> Result<usize, EncodeError> {
    if buf.len() < 2 {
        return Err(EncodeError::BufferTooSmall);
    }
    let body = &mut buf[2..];
    let payload_len = match frame {
        WideFrame::Capture(capture) => {
            let (tag, rest) = body.split_first_mut().ok_or(EncodeError::PostcardError)?;
            *tag = STREAM_FRAME_TAG_CAPTURE as u8;
            1 + postcard::to_slice(capture, rest)
                .map_err(|_| EncodeError::PostcardError)?
                .len()
        }
        other => postcard::to_slice(&connection_ref(other), body)
            .map_err(|_| EncodeError::PostcardError)?
            .len(),
    };
    if payload_len > MAX_WIDE_FRAME_BYTES {
        return Err(EncodeError::PayloadTooLarge);
    }
    buf[..2].copy_from_slice(&(payload_len as u16).to_le_bytes());
    Ok(2 + payload_len)
}

fn connection_ref(frame: &WideFrame) -> ConnectionRef<'_> {
    match frame {
        WideFrame::Hello(b) => ConnectionRef::Hello(b),
        WideFrame::SegmentStart(b) => ConnectionRef::SegmentStart(b),
        WideFrame::Audio(b) => ConnectionRef::Audio(b),
        WideFrame::Telemetry(b) => ConnectionRef::Telemetry(b),
        WideFrame::SegmentEnd(b) => ConnectionRef::SegmentEnd(b),
        WideFrame::EndOfAudio(b) => ConnectionRef::EndOfAudio(b),
        WideFrame::FlushPlayback(b) => ConnectionRef::FlushPlayback(b),
        WideFrame::Status(b) => ConnectionRef::Status(b),
        WideFrame::Capture(_) => unreachable!("encode_wide encodes a capture frame itself"),
    }
}

/// Decode a wide frame from a byte slice that starts with the `u16` LE length prefix.
///
/// Refuses a prefix above [`MAX_WIDE_FRAME_BYTES`] with [`DecodeError::OversizeFrame`],
/// a slice shorter than the prefix declares with [`DecodeError::Truncated`], and a
/// payload that is no frame with [`DecodeError::PostcardError`].  As with the connection
/// codec, bytes after the frame's body inside the declared length are ignored.
pub fn decode_wide(buf: &[u8]) -> Result<WideFrame, DecodeError> {
    let [lo, hi, rest @ ..] = buf else {
        return Err(DecodeError::Truncated);
    };
    let payload_len = usize::from(u16::from_le_bytes([*lo, *hi]));
    if payload_len > MAX_WIDE_FRAME_BYTES {
        return Err(DecodeError::OversizeFrame { len: payload_len });
    }
    let payload = rest.get(..payload_len).ok_or(DecodeError::Truncated)?;
    match payload.split_first() {
        Some((&tag, body)) if u32::from(tag) == STREAM_FRAME_TAG_CAPTURE => {
            postcard::from_bytes(body)
                .map(WideFrame::Capture)
                .map_err(|_| DecodeError::PostcardError)
        }
        _ => postcard::from_bytes::<ConnectionOwned>(payload)
            .map(WideFrame::from)
            .map_err(|_| DecodeError::PostcardError),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{
        AUDIO_PROTOCOL_VERSION, AudioFrame, ChannelSource, Codec, EndReason, LevelBlock, LevelRun,
        MAX_AUDIO_PAYLOAD, MAX_FRAME_BYTES, PodCounters, PodGauges, TelemetryKind, decode_frame,
        encode_frame,
    };
    use heapless::{String as HString, Vec as HVec};
    use std::vec;

    fn wide_bytes(frame: &WideFrame) -> Vec<u8> {
        let mut buf = vec![0u8; MAX_WIDE_FRAME_BYTES + 2];
        let n = encode_wide(frame, &mut buf).expect("encode_wide");
        buf.truncate(n);
        buf
    }

    fn hello(channels: u8, codec: Codec, channel_source: ChannelSource) -> Hello {
        Hello {
            version: AUDIO_PROTOCOL_VERSION,
            pod_id: HString::try_from("pod-aabbcc").unwrap(),
            sample_rate_hz: 16_000,
            bits_per_sample: 16,
            channels,
            codec,
            channel_source,
        }
    }

    /// One of every connection frame the narrow type holds, the audio frame full.
    fn connection_frames() -> Vec<StreamFrame> {
        let mut pcm: HVec<u8, MAX_AUDIO_PAYLOAD> = HVec::new();
        for i in 0..MAX_AUDIO_PAYLOAD {
            pcm.push(i as u8).unwrap();
        }
        let mut blocks = HVec::new();
        blocks
            .push(LevelBlock {
                half_db: 80,
                flags: 1,
            })
            .unwrap();
        vec![
            StreamFrame::Hello(hello(2, Codec::S16Le, ChannelSource::StereoBeamRawMic)),
            StreamFrame::SegmentStart(SegmentStart {
                segment_id: 7,
                base_sample_index: 1_234_567,
                base_device_ts_us: 9_876_543_210,
                preroll_samples: 16_000,
            }),
            StreamFrame::Audio(AudioFrame {
                segment_id: 7,
                first_sample_index: 1_234_567,
                device_ts_us: 9_876_543_210,
                pcm,
            }),
            StreamFrame::Telemetry(Telemetry {
                device_ts_us: 42,
                kind: TelemetryKind::Azimuths {
                    values: [0.5, 1.0, f32::NAN, 2.0],
                },
            }),
            StreamFrame::SegmentEnd(SegmentEnd {
                segment_id: 7,
                device_ts_us: 9_876_600_000,
                frames_sent: 3,
                samples_sent: 960,
                reason: EndReason::Overrun,
            }),
            StreamFrame::EndOfAudio(EndOfAudio {}),
            StreamFrame::FlushPlayback(FlushPlayback {}),
            StreamFrame::Status(Status {
                device_ts_us: 5,
                capture_head: 6,
                capture_head_ts_us: 7,
                levels: LevelRun {
                    first_block: 3,
                    blocks,
                },
                counters: PodCounters {
                    capture_xruns: Some(2),
                    ..PodCounters::default()
                },
                gauges: PodGauges {
                    rssi_dbm: Some(-60),
                    ..PodGauges::default()
                },
                segment_reach: 16_000,
            }),
        ]
    }

    /// The decoding mirror of a connection frame. Exhaustive over [`WideFrame`], so a
    /// variant added there fails to compile here until [`ConnectionOwned`] has it.
    fn owned_mirror(frame: WideFrame) -> ConnectionOwned {
        match frame {
            WideFrame::Hello(b) => ConnectionOwned::Hello(b),
            WideFrame::SegmentStart(b) => ConnectionOwned::SegmentStart(b),
            WideFrame::Audio(b) => ConnectionOwned::Audio(b),
            WideFrame::Telemetry(b) => ConnectionOwned::Telemetry(b),
            WideFrame::SegmentEnd(b) => ConnectionOwned::SegmentEnd(b),
            WideFrame::EndOfAudio(b) => ConnectionOwned::EndOfAudio(b),
            WideFrame::FlushPlayback(b) => ConnectionOwned::FlushPlayback(b),
            WideFrame::Status(b) => ConnectionOwned::Status(b),
            WideFrame::Capture(_) => panic!("a capture frame has no connection mirror"),
        }
    }

    #[test]
    fn connection_frames_encode_to_the_connection_codecs_bytes() {
        for frame in connection_frames() {
            let mut narrow = [0u8; MAX_FRAME_BYTES + 2];
            let n = encode_frame(&frame, &mut narrow).expect("encode_frame");
            let narrow = &narrow[..n];
            // Compare through the bytes, so a NaN azimuth compares equal to itself.
            let wide_of_narrow = decode_frame(narrow).map(WideFrame::from).unwrap();
            let through_mirror = WideFrame::from(owned_mirror(decode_wide(narrow).unwrap()));
            assert_eq!(wide_bytes(&WideFrame::from(frame)), narrow);
            assert_eq!(wide_bytes(&decode_wide(narrow).unwrap()), narrow);
            assert_eq!(wide_bytes(&wide_of_narrow), narrow);
            assert_eq!(wide_bytes(&through_mirror), narrow);
        }
    }

    #[test]
    fn decode_wide_of_a_connection_frame_equals_its_conversion() {
        let frame = StreamFrame::SegmentStart(SegmentStart {
            segment_id: 1,
            base_sample_index: 2,
            base_device_ts_us: 3,
            preroll_samples: 4,
        });
        let mut buf = [0u8; 64];
        let n = encode_frame(&frame, &mut buf).unwrap();
        assert_eq!(decode_wide(&buf[..n]).unwrap(), WideFrame::from(frame));
    }

    #[test]
    fn capture_bodies_golden_bytes() {
        let cases: [(CaptureFrame, &[u8]); 4] = [
            (
                CaptureFrame::Describe {
                    schema: CAPTURE_SCHEMA_VERSION,
                    channel_names: vec!["a".into(), "bc".into()],
                    period_frames: 320,
                    mono_us: 5,
                },
                // tag 64, Describe 0, schema 1, two names, period 320 = 0xC0 0x02, mono 5
                &[
                    0x40, 0x00, 0x01, 0x02, 0x01, b'a', 0x02, b'b', b'c', 0xC0, 0x02, 0x05,
                ],
            ),
            (
                CaptureFrame::Read {
                    first_sample_index: 320,
                    mono_us: 6,
                    backlog_frames: 7,
                },
                &[0x40, 0x01, 0xC0, 0x02, 0x06, 0x07],
            ),
            (
                CaptureFrame::Loss {
                    cause: LossCause::TransferError,
                    at_index: 8,
                    samples: Some(9),
                    mono_us: 10,
                    detail: "x".into(),
                },
                &[0x40, 0x02, 0x03, 0x08, 0x01, 0x09, 0x0A, 0x01, b'x'],
            ),
            (
                CaptureFrame::End {
                    reason: StreamEndReason::WriteFailed,
                    at_index: 11,
                    mono_us: 12,
                    detail: String::new(),
                },
                &[0x40, 0x03, 0x03, 0x0B, 0x0C, 0x00],
            ),
        ];
        for (body, payload) in cases {
            let bytes = wide_bytes(&WideFrame::Capture(body.clone()));
            assert_eq!(&bytes[..2], &(payload.len() as u16).to_le_bytes());
            assert_eq!(&bytes[2..], payload, "{body:?}");
            assert_eq!(decode_wide(&bytes).unwrap(), WideFrame::Capture(body));
        }
    }

    #[test]
    fn capture_enum_values_take_their_positional_index() {
        let loss = |cause| {
            wide_bytes(&WideFrame::Capture(CaptureFrame::Loss {
                cause,
                at_index: 0,
                samples: None,
                mono_us: 0,
                detail: String::new(),
            }))[4]
        };
        assert_eq!(loss(LossCause::WriterBacklog), 0);
        assert_eq!(loss(LossCause::Overrun), 1);
        assert_eq!(loss(LossCause::Suspend), 2);
        assert_eq!(loss(LossCause::TransferError), 3);
        let end = |reason| {
            wide_bytes(&WideFrame::Capture(CaptureFrame::End {
                reason,
                at_index: 0,
                mono_us: 0,
                detail: String::new(),
            }))[4]
        };
        assert_eq!(end(StreamEndReason::Stopped), 0);
        assert_eq!(end(StreamEndReason::DestinationFull), 1);
        assert_eq!(end(StreamEndReason::StreamDied), 2);
        assert_eq!(end(StreamEndReason::WriteFailed), 3);
    }

    #[test]
    fn capture_enums_serialize_to_snake_case_names() {
        fn json(v: &impl Serialize) -> String {
            serde_json::to_string(v).unwrap()
        }
        assert_eq!(json(&LossCause::WriterBacklog), "\"writer_backlog\"");
        assert_eq!(json(&LossCause::Overrun), "\"overrun\"");
        assert_eq!(json(&LossCause::Suspend), "\"suspend\"");
        assert_eq!(json(&LossCause::TransferError), "\"transfer_error\"");
        assert_eq!(json(&StreamEndReason::Stopped), "\"stopped\"");
        assert_eq!(
            json(&StreamEndReason::DestinationFull),
            "\"destination_full\""
        );
        assert_eq!(json(&StreamEndReason::StreamDied), "\"stream_died\"");
        assert_eq!(json(&StreamEndReason::WriteFailed), "\"write_failed\"");
    }

    #[test]
    fn six_channel_s16_audio_round_trips() {
        let pcm: Vec<u8> = (0..320 * 6 * 2).map(|i| (i % 251) as u8).collect();
        let frame = WideFrame::Audio(WideAudio {
            segment_id: 0,
            first_sample_index: 320,
            device_ts_us: 20_000,
            pcm,
        });
        assert_eq!(decode_wide(&wide_bytes(&frame)).unwrap(), frame);
        let described = WideFrame::Hello(hello(6, Codec::S32Le, ChannelSource::Described));
        assert_eq!(decode_wide(&wide_bytes(&described)).unwrap(), described);
    }

    /// The test that holds the two limits to each other: the worst-case audio frame at
    /// `MAX_WIDE_PCM_BYTES` encodes to exactly `MAX_WIDE_FRAME_BYTES`, and one more byte
    /// of PCM is refused.
    #[test]
    fn worst_case_audio_frame_fills_the_limit_exactly() {
        let worst = |pcm_len: usize| {
            WideFrame::Audio(WideAudio {
                segment_id: u32::MAX,
                first_sample_index: u64::MAX,
                device_ts_us: u64::MAX,
                pcm: vec![0xA5; pcm_len],
            })
        };
        let mut buf = vec![0u8; MAX_WIDE_FRAME_BYTES + 64];
        let frame = worst(MAX_WIDE_PCM_BYTES);
        let n = encode_wide(&frame, &mut buf).unwrap();
        assert_eq!(n, 2 + MAX_WIDE_FRAME_BYTES);
        assert_eq!(decode_wide(&buf[..n]).unwrap(), frame);
        assert_eq!(
            encode_wide(&worst(MAX_WIDE_PCM_BYTES + 1), &mut buf),
            Err(EncodeError::PayloadTooLarge)
        );
    }

    #[test]
    fn decode_wide_refusals() {
        assert_eq!(decode_wide(&[0x01]), Err(DecodeError::Truncated));
        assert_eq!(
            decode_wide(&[0x02, 0x00, 0x07]),
            Err(DecodeError::Truncated)
        );
        assert_eq!(
            decode_wide(&[0xFE, 0xFF]),
            Err(DecodeError::OversizeFrame { len: 0xFFFE })
        );
        // Tag 9 is neither a connection frame nor the capture extension.
        assert_eq!(
            decode_wide(&[0x01, 0x00, 0x09]),
            Err(DecodeError::PostcardError)
        );
        // Tag 64 with a nested tag no capture body has.
        assert_eq!(
            decode_wide(&[0x02, 0x00, 0x40, 0x04]),
            Err(DecodeError::PostcardError)
        );
    }

    #[test]
    fn encode_wide_buffer_too_small() {
        let frame = WideFrame::EndOfAudio(EndOfAudio {});
        assert_eq!(
            encode_wide(&frame, &mut [0u8; 1]),
            Err(EncodeError::BufferTooSmall)
        );
        let capture = WideFrame::Capture(CaptureFrame::Read {
            first_sample_index: 0,
            mono_us: 0,
            backlog_frames: 0,
        });
        assert_eq!(
            encode_wide(&capture, &mut [0u8; 2]),
            Err(EncodeError::PostcardError)
        );
    }
}
