//! Reading a capture record: the header, checked, then its items in order,
//! ending with one tail verdict that says how the file ends. Everything read
//! before the tail is usable whatever the verdict.

use std::fmt;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;

use serde::Deserialize;
use stamped_log::{HeaderError, RecordError, Records};

use crate::{
    BLOCK_PREFIX_BYTES, End, Event, FORMAT_VERSION, Gap, Header, KIND_BLOCK, KIND_EVENT, MAGIC,
};

/// One item of a capture record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Item {
    /// Frames the device delivered, read in one read.
    Block {
        /// When the read that delivered the block returned: an upper bound on
        /// when its last frame was captured.
        mono_us: u64,
        /// `CLOCK_REALTIME`, read as a pair with `mono_us`.
        wall_us: u64,
        /// The block's position: frames the device delivered before its first.
        first_frame: u64,
        /// Frames already waiting in the device's buffer when the read
        /// returned.
        backlog_frames: u32,
        /// Whole frames in `pcm`.
        frames: u64,
        /// Interleaved frames as the device delivered them.
        pcm: Vec<u8>,
    },
    /// Audio is missing at `gap.at_frame`.
    Gap { mono_us: u64, gap: Gap },
    /// The capture ended itself.
    End { mono_us: u64, end: End },
}

/// How a capture record ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tail {
    pub verdict: Verdict,
    /// The byte offset the verdict is about: the end of the file for `ended`
    /// and `unterminated`, the start of the torn or offending record for
    /// `torn` and `corrupt`.
    pub offset: u64,
}

/// The four ways a record can end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// An `end` record, then end of file.
    Ended,
    /// End of file on a record boundary with no `end` record: the capture did
    /// not stop cleanly.
    Unterminated,
    /// End of file inside a record.
    Torn,
    /// A record that breaks the format.
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
            Verdict::Ended => "an end record, then end of file".to_owned(),
            Verdict::Unterminated => {
                "end of file on a record boundary with no end record".to_owned()
            }
            Verdict::Torn => "end of file inside a record".to_owned(),
            Verdict::Corrupt(r) => r.to_string(),
        }
    }
}

/// Why a record is corrupt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CorruptReason {
    /// A record length of 0, which no record has.
    Length { len: usize },
    /// A payload kind this version does not define.
    UnknownKind { kind: u8 },
    /// A block payload too short for the block prefix.
    ShortBlock { len: usize },
    /// A block with no PCM.
    EmptyBlock,
    /// Block PCM that is not whole frames.
    PartialFrame { bytes: usize, frame_bytes: usize },
    /// An event whose `event` value this version does not define.
    UnknownEvent { name: String },
    /// An event that is not valid JSON, lacks a key, or carries a value this
    /// version does not define.
    BadEvent { message: String },
    /// A `first_frame` or `at_frame` that disagrees with the position counted
    /// so far.
    Position { expected: u64, found: u64 },
    /// A block or measured gap whose frames carry the position past `u64::MAX`.
    PositionOverflow { position: u64, frames: u64 },
    /// Anything after an `end` record.
    AfterEnd,
}

impl fmt::Display for CorruptReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CorruptReason::Length { len } => write!(f, "record length {len} out of range"),
            CorruptReason::UnknownKind { kind } => write!(f, "unknown record kind {kind:#04x}"),
            CorruptReason::ShortBlock { len } => {
                write!(f, "block payload of {len} bytes is shorter than its prefix")
            }
            CorruptReason::EmptyBlock => f.write_str("block with no PCM"),
            CorruptReason::PartialFrame { bytes, frame_bytes } => write!(
                f,
                "block PCM of {bytes} bytes is not whole {frame_bytes}-byte frames"
            ),
            CorruptReason::UnknownEvent { name } => write!(f, "unknown event {name:?}"),
            CorruptReason::BadEvent { message } => write!(f, "bad event: {message}"),
            CorruptReason::Position { expected, found } => write!(
                f,
                "position {found} where the frames counted so far reach {expected}"
            ),
            CorruptReason::PositionOverflow { position, frames } => write!(
                f,
                "{frames} frames at position {position} carry the position past its limit"
            ),
            CorruptReason::AfterEnd => f.write_str("data after the end record"),
        }
    }
}

/// Why a file could not be opened as a capture record.
#[derive(Debug)]
pub enum OpenError {
    /// The file does not start with the capture record's magic.
    BadMagic,
    /// A format version this reader does not read.
    UnsupportedVersion(u16),
    /// The metadata is not a valid header: bad JSON, a missing key, a value
    /// this version does not define, or a rule the header breaks.
    Metadata(String),
    /// The read failed, or the file ended inside the header.
    Io(io::Error),
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpenError::BadMagic => f.write_str("not a capture record (bad magic)"),
            OpenError::UnsupportedVersion(v) => write!(
                f,
                "capture record format version {v}; this reader reads {FORMAT_VERSION}"
            ),
            OpenError::Metadata(m) => write!(f, "capture record header: {m}"),
            OpenError::Io(e) => write!(f, "I/O error reading capture record header: {e}"),
        }
    }
}

impl std::error::Error for OpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            OpenError::Io(e) => Some(e),
            _ => None,
        }
    }
}

/// Reads one capture record. Iterating yields its items; once iteration has
/// ended without an I/O error, [`RecordReader::tail`] holds the verdict.
pub struct RecordReader<R: Read> {
    header: Header,
    meta: serde_json::Value,
    records: Records<R>,
    frame_bytes: usize,
    position: u64,
    ended: bool,
    /// A read failed: iteration is over and there is no verdict.
    failed: bool,
    tail: Option<Tail>,
}

impl RecordReader<BufReader<File>> {
    /// Open `path` and read and check its header.
    pub fn open(path: &Path) -> Result<Self, OpenError> {
        let file = File::open(path).map_err(OpenError::Io)?;
        RecordReader::from_reader(BufReader::new(file))
    }
}

impl<R: Read> RecordReader<R> {
    /// Read and check the header from a reader positioned at the file's start.
    pub fn from_reader(mut r: R) -> Result<Self, OpenError> {
        let raw = stamped_log::read_header(&mut r, MAGIC).map_err(|e| match e {
            // TODO(record-reader-reads-frame-logs): a frame log's magic is
            // refused here, so the reader tool reads capture records only.
            HeaderError::BadMagic => OpenError::BadMagic,
            HeaderError::Io(e) => OpenError::Io(e),
        })?;
        if raw.version != FORMAT_VERSION {
            return Err(OpenError::UnsupportedVersion(raw.version));
        }
        let meta: serde_json::Value =
            serde_json::from_slice(&raw.meta).map_err(|e| OpenError::Metadata(e.to_string()))?;
        let header = Header::deserialize(&meta).map_err(|e| OpenError::Metadata(e.to_string()))?;
        header.validate().map_err(|e| OpenError::Metadata(e.0))?;
        let frame_bytes = header.granted.frame_bytes();
        Ok(RecordReader {
            header,
            meta,
            records: Records::new(r, raw.encoded_len(), stamped_log::MAX_PAYLOAD),
            frame_bytes,
            position: 0,
            ended: false,
            failed: false,
            tail: None,
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    /// The header metadata as the file holds it, unknown keys included.
    pub fn meta(&self) -> &serde_json::Value {
        &self.meta
    }

    /// The delivered-frame position counted so far: after the last item, the
    /// final position.
    pub fn position(&self) -> u64 {
        self.position
    }

    /// The verdict, once iteration has ended without an I/O error.
    pub fn tail(&self) -> Option<&Tail> {
        self.tail.as_ref()
    }

    fn finish(&mut self, verdict: Verdict, offset: u64) {
        self.tail = Some(Tail { verdict, offset });
    }

    fn decode(&mut self, stamp: u64, payload: Vec<u8>) -> Result<Item, CorruptReason> {
        match payload[0] {
            KIND_BLOCK => self.decode_block(stamp, payload),
            KIND_EVENT => self.decode_event(stamp, &payload[1..]),
            kind => Err(CorruptReason::UnknownKind { kind }),
        }
    }

    /// Decode a block, keeping `payload`'s allocation as the block's PCM.
    fn decode_block(&mut self, mono_us: u64, mut payload: Vec<u8>) -> Result<Item, CorruptReason> {
        if payload.len() < BLOCK_PREFIX_BYTES {
            return Err(CorruptReason::ShortBlock { len: payload.len() });
        }
        let u64_at = |i: usize| {
            let mut b = [0u8; 8];
            b.copy_from_slice(&payload[i..i + 8]);
            u64::from_le_bytes(b)
        };
        let wall_us = u64_at(1);
        let first_frame = u64_at(9);
        let backlog_frames =
            u32::from_le_bytes([payload[17], payload[18], payload[19], payload[20]]);
        payload.drain(..BLOCK_PREFIX_BYTES);
        let pcm = payload;
        if pcm.is_empty() {
            return Err(CorruptReason::EmptyBlock);
        }
        if !pcm.len().is_multiple_of(self.frame_bytes) {
            return Err(CorruptReason::PartialFrame {
                bytes: pcm.len(),
                frame_bytes: self.frame_bytes,
            });
        }
        self.expect_position(first_frame)?;
        let frames = (pcm.len() / self.frame_bytes) as u64;
        self.position =
            self.position
                .checked_add(frames)
                .ok_or(CorruptReason::PositionOverflow {
                    position: self.position,
                    frames,
                })?;
        Ok(Item::Block {
            mono_us,
            wall_us,
            first_frame,
            backlog_frames,
            frames,
            pcm,
        })
    }

    fn decode_event(&mut self, mono_us: u64, json: &[u8]) -> Result<Item, CorruptReason> {
        let bad = |message: String| CorruptReason::BadEvent { message };
        let value: serde_json::Value =
            serde_json::from_slice(json).map_err(|e| bad(e.to_string()))?;
        let name = value
            .get("event")
            .and_then(|v| v.as_str())
            .ok_or_else(|| bad("no string \"event\" key".to_owned()))?;
        if !Event::NAMES.contains(&name) {
            return Err(CorruptReason::UnknownEvent {
                name: name.to_owned(),
            });
        }
        let event = Event::deserialize(&value).map_err(|e| bad(e.to_string()))?;
        event.check().map_err(bad)?;
        self.expect_position(event.at_frame())?;
        Ok(match event {
            Event::Gap(gap) => {
                if let Some(frames) = gap.frames {
                    self.position = self.position.checked_add(frames).ok_or(
                        CorruptReason::PositionOverflow {
                            position: self.position,
                            frames,
                        },
                    )?;
                }
                Item::Gap { mono_us, gap }
            }
            Event::End(end) => {
                self.ended = true;
                Item::End { mono_us, end }
            }
        })
    }

    fn expect_position(&self, found: u64) -> Result<(), CorruptReason> {
        if found == self.position {
            Ok(())
        } else {
            Err(CorruptReason::Position {
                expected: self.position,
                found,
            })
        }
    }
}

impl<R: Read> Iterator for RecordReader<R> {
    type Item = io::Result<Item>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.tail.is_some() || self.failed {
            return None;
        }
        let offset = self.records.offset();
        let next = self.records.next();
        if self.ended {
            // An end record must be the last thing in the file.
            return match next {
                None => {
                    self.finish(Verdict::Ended, offset);
                    None
                }
                Some(Err(RecordError::Io(e))) => {
                    self.failed = true;
                    Some(Err(e))
                }
                Some(_) => {
                    self.finish(Verdict::Corrupt(CorruptReason::AfterEnd), offset);
                    None
                }
            };
        }
        match next {
            None => {
                self.finish(Verdict::Unterminated, offset);
                None
            }
            Some(Ok(stamped_log::Item::TornTail)) => {
                self.finish(Verdict::Torn, offset);
                None
            }
            Some(Err(RecordError::CorruptLength { len })) => {
                self.finish(Verdict::Corrupt(CorruptReason::Length { len }), offset);
                None
            }
            Some(Err(RecordError::Io(e))) => {
                self.failed = true;
                Some(Err(e))
            }
            Some(Ok(stamped_log::Item::Record { stamp, payload })) => {
                match self.decode(stamp, payload) {
                    Ok(item) => Some(Ok(item)),
                    Err(reason) => {
                        self.finish(Verdict::Corrupt(reason), offset);
                        None
                    }
                }
            }
        }
    }
}
