//! One pass over a frame log: each record decoded as a wide frame and checked
//! against the stream's rules, yielding what the log holds in order and ending
//! with one tail verdict. Reads any rate, channel count and either sample
//! format, and keeps no audio in memory beyond the step it yields.
//!
//! The walker keeps the **position**: the sample index after the last sample
//! accounted for, set by the first `SegmentStart` or capture frame that states
//! one. Inside a segment an audio frame's index is compared with it; a `Loss`
//! with a length advances it; at a `SegmentStart` the base is compared with it
//! and the difference is the new segment's audio not sent or resent. A jump of
//! the index is reported, never corrupt.

use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::path::Path;

use audio_pipeline::wide::{
    CAPTURE_SCHEMA_VERSION, CaptureFrame, LossCause, StreamEndReason, WideFrame, decode_wide,
};
use audio_pipeline::wire::{
    AUDIO_PROTOCOL_MIN_READABLE, AUDIO_PROTOCOL_VERSION, ChannelSource, Codec, Hello, SegmentEnd,
    SegmentStart,
};

use crate::{FrameLogError, FrameLogReader, HostMicros, LogItem, LogMeta};

/// One thing a frame log holds, in order.
#[derive(Debug, PartialEq)]
pub enum Step {
    /// The stream's hello: its first frame.
    Hello {
        stamp: HostMicros,
        hello: Hello,
    },
    /// A capture's `Describe`, directly after the hello.
    Describe {
        stamp: HostMicros,
        description: Description,
    },
    /// A `SegmentStart` arrived while `segment_id` was open; that segment ends
    /// here unclosed. The `SegmentOpened` for `by` follows.
    SegmentReplaced {
        stamp: HostMicros,
        segment_id: u32,
        by: u32,
        at_frame: u64,
        /// The new segment's `base_device_ts_us`.
        device_ts_us: u64,
    },
    /// A segment opened. `not_sent_frames` is how far its base lies after the
    /// furthest index accounted for, `resent_frames` how far before; both 0
    /// when no index was known.
    SegmentOpened {
        stamp: HostMicros,
        start: SegmentStart,
        not_sent_frames: u64,
        resent_frames: u64,
    },
    /// An audio frame inside the open segment.
    Audio(AudioStep),
    /// A capture's `Read` for the audio frame before it.
    Read {
        stamp: HostMicros,
        first_frame: u64,
        /// Whole frames in the audio frame it names.
        frames: u64,
        mono_us: u64,
        backlog_frames: u32,
    },
    /// A capture's `Loss`. `segment_id` is the segment open at it, if any.
    Loss {
        stamp: HostMicros,
        cause: LossCause,
        at_frame: u64,
        samples: Option<u64>,
        mono_us: u64,
        detail: String,
        segment_id: Option<u32>,
    },
    /// The open segment closed.
    SegmentClosed {
        stamp: HostMicros,
        end: SegmentEnd,
        /// Whole frames in the segment's audio frames.
        frames_received: u64,
        audio_frames: u64,
        /// The position when it closed.
        at_frame: u64,
    },
    /// A capture's `End`.
    End {
        stamp: HostMicros,
        reason: StreamEndReason,
        at_frame: u64,
        mono_us: u64,
        detail: String,
    },
    Telemetry {
        stamp: HostMicros,
    },
    Status {
        stamp: HostMicros,
    },
}

/// A capture's `Describe`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Description {
    pub schema: u8,
    pub channel_names: Vec<String>,
    pub period_frames: u32,
    pub mono_us: u64,
}

/// An audio frame and where it lies.
#[derive(Debug, PartialEq)]
pub struct AudioStep {
    pub stamp: HostMicros,
    pub segment_id: u32,
    pub first_frame: u64,
    /// Whole frames in `pcm`.
    pub frames: u64,
    pub device_ts_us: u64,
    /// Interleaved PCM in the hello's format.
    pub pcm: Vec<u8>,
    /// The frame's index differs from the position.
    pub jump: Option<Jump>,
}

/// An index jump inside a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Jump {
    pub kind: JumpKind,
    /// The position the frame was expected at.
    pub expected: u64,
    /// How far the frame's index lies from it.
    pub frames: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JumpKind {
    /// Forward: a measured discontinuity.
    Gap,
    /// Backward.
    Overlap,
}

impl JumpKind {
    pub fn name(self) -> &'static str {
        match self {
            JumpKind::Gap => "gap",
            JumpKind::Overlap => "overlap",
        }
    }
}

/// How a frame log ends.
#[derive(Debug, Clone, PartialEq)]
pub struct Tail {
    pub verdict: Verdict,
    /// The end of the file for `ended` and `unterminated`; the start of the
    /// torn or offending record for `torn` and `corrupt`.
    pub offset: u64,
    /// The segment open when the log ended, if any.
    pub open_segment: Option<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    /// A capture `End` frame, then end of file.
    Ended,
    /// End of file on a record boundary with no `End`.
    Unterminated,
    /// End of file inside a record.
    Torn,
    /// A record that breaks the format or the stream's rules.
    Corrupt(CorruptReason),
}

impl Verdict {
    pub fn name(&self) -> &'static str {
        match self {
            Verdict::Ended => "ended",
            Verdict::Unterminated => "unterminated",
            Verdict::Torn => "torn",
            Verdict::Corrupt(_) => "corrupt",
        }
    }
}

impl Tail {
    /// The verdict's reason in words.
    pub fn reason(&self) -> String {
        match &self.verdict {
            Verdict::Ended => "an end frame, then end of file".to_owned(),
            Verdict::Unterminated => {
                "end of file on a record boundary with no end frame".to_owned()
            }
            Verdict::Torn => "end of file inside a record".to_owned(),
            Verdict::Corrupt(r) => r.to_string(),
        }
    }
}

/// Why a record is corrupt.
#[derive(Debug, Clone, PartialEq)]
pub enum CorruptReason {
    ZeroLength,
    Undecodable { message: String },
    FirstNotHello,
    Version { got: u8 },
    Schema { got: u8 },
    SampleFormat { bits: u8, codec: Codec },
    NoChannels,
    NoRate,
    PartialFrame { bytes: usize, frame_bytes: usize },
    AudioOutsideSegment,
    WrongSegment { open: u32, found: u32 },
    SegmentEndOutsideSegment,
    SecondHello,
    HostFrame,
    CaptureWithoutDescribe,
    MisplacedDescribe,
    MissingDescribe,
    ChannelNames { names: usize, channels: u8 },
    AfterEnd,
    ReadMismatch { found: u64 },
    AtIndex { expected: u64, found: u64 },
    Overflow,
}

impl fmt::Display for CorruptReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use CorruptReason::*;
        match self {
            ZeroLength => f.write_str("record length 0"),
            Undecodable { message } => write!(f, "frame does not decode: {message}"),
            FirstNotHello => f.write_str("first frame is not a hello"),
            Version { got } => write!(
                f,
                "hello version {got} outside {AUDIO_PROTOCOL_MIN_READABLE}..={AUDIO_PROTOCOL_VERSION}"
            ),
            Schema { got } => write!(
                f,
                "capture schema {got}; this reader reads 1..={CAPTURE_SCHEMA_VERSION}"
            ),
            SampleFormat { bits, codec } => {
                write!(f, "unknown sample format: {bits} bits, codec {codec:?}")
            }
            NoChannels => f.write_str("hello with zero channels"),
            NoRate => f.write_str("hello with a zero sample rate"),
            PartialFrame { bytes, frame_bytes } => write!(
                f,
                "audio PCM of {bytes} bytes is not whole {frame_bytes}-byte frames"
            ),
            AudioOutsideSegment => f.write_str("audio outside a segment"),
            WrongSegment { open, found } => {
                write!(
                    f,
                    "frame names segment {found} while segment {open} is open"
                )
            }
            SegmentEndOutsideSegment => f.write_str("segment end outside a segment"),
            SecondHello => f.write_str("a second hello"),
            HostFrame => f.write_str("a frame only the host sends"),
            CaptureWithoutDescribe => f.write_str("capture frame with no describe after the hello"),
            MisplacedDescribe => f.write_str("describe not directly after the hello"),
            MissingDescribe => f.write_str("described channels with no describe after the hello"),
            ChannelNames { names, channels } => write!(
                f,
                "describe names {names} channels where the hello has {channels}"
            ),
            AfterEnd => f.write_str("data after the end frame"),
            ReadMismatch { found } => write!(
                f,
                "read names index {found}, which is not the audio frame before it"
            ),
            AtIndex { expected, found } => {
                write!(f, "at_index {found} where the position is {expected}")
            }
            Overflow => f.write_str("sample index past its limit"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    ExpectHello,
    /// The record after the hello; `described` when the hello's channels are
    /// named by a `Describe`.
    AfterHello {
        described: bool,
    },
    Body,
    AfterEnd,
}

struct OpenSegment {
    id: u32,
    frames: u64,
    audio_frames: u64,
}

/// Walks one frame log. Iterating yields its steps; once iteration has ended
/// without an I/O error, [`Walk::tail`] holds the verdict.
pub struct Walk {
    reader: FrameLogReader,
    phase: Phase,
    channels: u8,
    frame_bytes: usize,
    capture: bool,
    /// The index the stream is at: where the next audio frame of the open
    /// segment is expected, and what a `Loss` or `End` states. A segment that
    /// resends sets it back to its base.
    position: Option<u64>,
    /// The index after the furthest sample accounted for. It never goes back;
    /// a `SegmentStart`'s base is compared with it.
    frontier: Option<u64>,
    open: Option<OpenSegment>,
    /// The audio frame in the previous record: its index and frames.
    last_audio: Option<(u64, u64)>,
    pending: VecDeque<Step>,
    tail: Option<Tail>,
    failed: bool,
}

/// Open the frame log at `path` and walk it. Fails on a header the reader
/// refuses.
pub fn walk(path: &Path) -> Result<Walk, FrameLogError> {
    Ok(Walk {
        reader: FrameLogReader::open(path)?,
        phase: Phase::ExpectHello,
        channels: 0,
        frame_bytes: 0,
        capture: false,
        position: None,
        frontier: None,
        open: None,
        last_audio: None,
        pending: VecDeque::new(),
        tail: None,
        failed: false,
    })
}

impl Walk {
    pub fn meta(&self) -> &LogMeta {
        self.reader.meta()
    }

    /// The header metadata's JSON as written, unknown keys included.
    pub fn raw_meta(&self) -> &[u8] {
        self.reader.raw_meta()
    }

    /// The verdict, once iteration has ended without an I/O error.
    pub fn tail(&self) -> Option<&Tail> {
        self.tail.as_ref()
    }

    fn finish(&mut self, verdict: Verdict, offset: u64) {
        self.tail = Some(Tail {
            verdict,
            offset,
            open_segment: self.open.as_ref().map(|s| s.id),
        });
    }

    fn step(&mut self, step: Step) -> Result<(), CorruptReason> {
        self.pending.push_back(step);
        Ok(())
    }

    fn set_position(&mut self, index: u64) {
        self.position = Some(index);
        self.frontier = Some(self.frontier.map_or(index, |f| f.max(index)));
    }

    /// Check a capture frame's `at_index` against the position, taking it as
    /// the position when none is known yet.
    fn at_position(&mut self, at: u64) -> Result<(), CorruptReason> {
        match self.position {
            Some(expected) if expected != at => Err(CorruptReason::AtIndex {
                expected,
                found: at,
            }),
            _ => {
                self.set_position(at);
                Ok(())
            }
        }
    }

    fn feed(&mut self, stamp: HostMicros, payload: &[u8]) -> Result<(), CorruptReason> {
        let frame = decode_wide(payload).map_err(|e| CorruptReason::Undecodable {
            message: format!("{e:?}"),
        })?;
        let last_audio = self.last_audio.take();
        match self.phase {
            Phase::ExpectHello => {
                let WideFrame::Hello(hello) = frame else {
                    return Err(CorruptReason::FirstNotHello);
                };
                return self.hello(stamp, hello);
            }
            Phase::AfterHello { described } => {
                self.phase = Phase::Body;
                match frame {
                    WideFrame::Capture(CaptureFrame::Describe {
                        schema,
                        channel_names,
                        period_frames,
                        mono_us,
                    }) => {
                        let description = Description {
                            schema,
                            channel_names,
                            period_frames,
                            mono_us,
                        };
                        return self.describe(stamp, description);
                    }
                    _ if described => return Err(CorruptReason::MissingDescribe),
                    _ => {}
                }
            }
            Phase::Body => {}
            Phase::AfterEnd => return Err(CorruptReason::AfterEnd),
        }
        self.body(stamp, frame, last_audio)
    }

    fn hello(&mut self, stamp: HostMicros, hello: Hello) -> Result<(), CorruptReason> {
        if !(AUDIO_PROTOCOL_MIN_READABLE..=AUDIO_PROTOCOL_VERSION).contains(&hello.version) {
            return Err(CorruptReason::Version { got: hello.version });
        }
        let sample_bytes = match (hello.bits_per_sample, hello.codec) {
            (16, Codec::S16Le) => 2,
            (32, Codec::S32Le) => 4,
            (bits, codec) => return Err(CorruptReason::SampleFormat { bits, codec }),
        };
        if hello.channels == 0 {
            return Err(CorruptReason::NoChannels);
        }
        if hello.sample_rate_hz == 0 {
            return Err(CorruptReason::NoRate);
        }
        self.channels = hello.channels;
        self.frame_bytes = sample_bytes * usize::from(hello.channels);
        self.phase = Phase::AfterHello {
            described: hello.channel_source == ChannelSource::Described,
        };
        self.step(Step::Hello { stamp, hello })
    }

    fn describe(
        &mut self,
        stamp: HostMicros,
        description: Description,
    ) -> Result<(), CorruptReason> {
        if description.schema == 0 || description.schema > CAPTURE_SCHEMA_VERSION {
            return Err(CorruptReason::Schema {
                got: description.schema,
            });
        }
        if description.channel_names.len() != usize::from(self.channels) {
            return Err(CorruptReason::ChannelNames {
                names: description.channel_names.len(),
                channels: self.channels,
            });
        }
        self.capture = true;
        self.step(Step::Describe { stamp, description })
    }

    fn body(
        &mut self,
        stamp: HostMicros,
        frame: WideFrame,
        last_audio: Option<(u64, u64)>,
    ) -> Result<(), CorruptReason> {
        match frame {
            WideFrame::Hello(_) => Err(CorruptReason::SecondHello),
            WideFrame::EndOfAudio(_) | WideFrame::FlushPlayback(_) => Err(CorruptReason::HostFrame),
            WideFrame::Telemetry(_) => self.step(Step::Telemetry { stamp }),
            WideFrame::Status(_) => self.step(Step::Status { stamp }),
            WideFrame::SegmentStart(start) => {
                let base = start.base_sample_index;
                if let Some(open) = self.open.take() {
                    self.pending.push_back(Step::SegmentReplaced {
                        stamp,
                        segment_id: open.id,
                        by: start.segment_id,
                        at_frame: self.position.unwrap_or(base),
                        device_ts_us: start.base_device_ts_us,
                    });
                }
                let (not_sent_frames, resent_frames) = match self.frontier {
                    Some(f) if base >= f => (base - f, 0),
                    Some(f) => (0, f - base),
                    None => (0, 0),
                };
                self.set_position(base);
                self.open = Some(OpenSegment {
                    id: start.segment_id,
                    frames: 0,
                    audio_frames: 0,
                });
                self.step(Step::SegmentOpened {
                    stamp,
                    start,
                    not_sent_frames,
                    resent_frames,
                })
            }
            WideFrame::Audio(audio) => {
                let Some(open) = self.open.as_mut() else {
                    return Err(CorruptReason::AudioOutsideSegment);
                };
                if audio.segment_id != open.id {
                    return Err(CorruptReason::WrongSegment {
                        open: open.id,
                        found: audio.segment_id,
                    });
                }
                if !audio.pcm.len().is_multiple_of(self.frame_bytes) {
                    return Err(CorruptReason::PartialFrame {
                        bytes: audio.pcm.len(),
                        frame_bytes: self.frame_bytes,
                    });
                }
                let frames = (audio.pcm.len() / self.frame_bytes) as u64;
                let first = audio.first_sample_index;
                let after = first.checked_add(frames).ok_or(CorruptReason::Overflow)?;
                let expected = self.position.expect("an open segment sets the position");
                let jump = match first.cmp(&expected) {
                    std::cmp::Ordering::Equal => None,
                    std::cmp::Ordering::Greater => Some(Jump {
                        kind: JumpKind::Gap,
                        expected,
                        frames: first - expected,
                    }),
                    std::cmp::Ordering::Less => Some(Jump {
                        kind: JumpKind::Overlap,
                        expected,
                        frames: expected - first,
                    }),
                };
                open.frames += frames;
                open.audio_frames += 1;
                self.set_position(after);
                self.last_audio = Some((first, frames));
                self.step(Step::Audio(AudioStep {
                    stamp,
                    segment_id: audio.segment_id,
                    first_frame: first,
                    frames,
                    device_ts_us: audio.device_ts_us,
                    pcm: audio.pcm,
                    jump,
                }))
            }
            WideFrame::SegmentEnd(end) => {
                let open = match self.open.take() {
                    None => return Err(CorruptReason::SegmentEndOutsideSegment),
                    Some(open) if open.id != end.segment_id => {
                        let reason = CorruptReason::WrongSegment {
                            open: open.id,
                            found: end.segment_id,
                        };
                        self.open = Some(open);
                        return Err(reason);
                    }
                    Some(open) => open,
                };
                self.step(Step::SegmentClosed {
                    stamp,
                    end,
                    frames_received: open.frames,
                    audio_frames: open.audio_frames,
                    at_frame: self.position.expect("an open segment sets the position"),
                })
            }
            WideFrame::Capture(capture) => {
                if !self.capture {
                    return Err(CorruptReason::CaptureWithoutDescribe);
                }
                self.capture_frame(stamp, capture, last_audio)
            }
        }
    }

    fn capture_frame(
        &mut self,
        stamp: HostMicros,
        frame: CaptureFrame,
        last_audio: Option<(u64, u64)>,
    ) -> Result<(), CorruptReason> {
        match frame {
            CaptureFrame::Describe { .. } => Err(CorruptReason::MisplacedDescribe),
            CaptureFrame::Read {
                first_sample_index,
                mono_us,
                backlog_frames,
            } => {
                let Some((first_frame, frames)) =
                    last_audio.filter(|&(first, _)| first == first_sample_index)
                else {
                    return Err(CorruptReason::ReadMismatch {
                        found: first_sample_index,
                    });
                };
                self.step(Step::Read {
                    stamp,
                    first_frame,
                    frames,
                    mono_us,
                    backlog_frames,
                })
            }
            CaptureFrame::Loss {
                cause,
                at_index,
                samples,
                mono_us,
                detail,
            } => {
                self.at_position(at_index)?;
                if let Some(n) = samples {
                    self.set_position(at_index.checked_add(n).ok_or(CorruptReason::Overflow)?);
                }
                let segment_id = self.open.as_ref().map(|s| s.id);
                self.step(Step::Loss {
                    stamp,
                    cause,
                    at_frame: at_index,
                    samples,
                    mono_us,
                    detail,
                    segment_id,
                })
            }
            CaptureFrame::End {
                reason,
                at_index,
                mono_us,
                detail,
            } => {
                self.at_position(at_index)?;
                self.phase = Phase::AfterEnd;
                self.step(Step::End {
                    stamp,
                    reason,
                    at_frame: at_index,
                    mono_us,
                    detail,
                })
            }
        }
    }
}

impl Iterator for Walk {
    type Item = io::Result<Step>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(step) = self.pending.pop_front() {
                return Some(Ok(step));
            }
            if self.tail.is_some() || self.failed {
                return None;
            }
            let offset = self.reader.offset();
            let after_end = self.phase == Phase::AfterEnd;
            match self.reader.next() {
                None if after_end => self.finish(Verdict::Ended, offset),
                None => self.finish(Verdict::Unterminated, offset),
                Some(Ok(_)) | Some(Err(FrameLogError::CorruptLength { .. })) if after_end => {
                    self.finish(Verdict::Corrupt(CorruptReason::AfterEnd), offset)
                }
                Some(Ok(LogItem::TornTail)) => self.finish(Verdict::Torn, offset),
                Some(Ok(LogItem::Record { host_rx, payload })) => {
                    if let Err(reason) = self.feed(host_rx, &payload) {
                        self.finish(Verdict::Corrupt(reason), offset);
                    }
                }
                Some(Err(FrameLogError::CorruptLength { .. })) => {
                    self.finish(Verdict::Corrupt(CorruptReason::ZeroLength), offset)
                }
                Some(Err(FrameLogError::Io(e))) => {
                    self.failed = true;
                    return Some(Err(e));
                }
                Some(Err(other)) => {
                    self.failed = true;
                    return Some(Err(io::Error::other(other)));
                }
            }
        }
    }
}
