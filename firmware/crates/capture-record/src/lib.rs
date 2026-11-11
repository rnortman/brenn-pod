//! The capture record: a [`stamped_log`] file holding one capture device's
//! frames, exactly as delivered, with every loss recorded where it happened.
//!
//! ```text
//! header:  b"BPCR" | format_version 1 | JSON metadata (see [`Header`])
//! record:  stamp = mono_us (CLOCK_MONOTONIC, µs, capturing machine) | payload
//! payload: 0x01 | wall_us u64 LE | first_frame u64 LE | backlog_frames u32 LE | pcm   (block)
//!          0x02 | UTF-8 JSON object                                                  (event)
//! ```
//!
//! A frame is one sample per channel. Positions count delivered frames: how
//! many frames the device had handed to the capture before the one in
//! question, whether or not they reached the file. A block's `first_frame`
//! and an event's `at_frame` are positions; a `writer_backlog` gap advances
//! the position by its exact length, an unmeasured gap does not advance it.
//! Blocks between two events are contiguous in position and in the device's
//! sample clock. A record whose last item is not an `end` event did not stop
//! cleanly.

mod event;
mod export;
mod header;
mod reader;
mod summary;
mod writer;

pub use event::{End, EndReason, Event, Gap, GapCause};
pub use export::{Basis, ExportError, ExportMap, Hole, Run, WAV_HEADER_BYTES, export_wav};
pub use header::{
    BuildSource, Clocks, Device, DeviceControl, Granted, Header, HeaderInvalid, SampleFormat,
    StartPair, WriterBuild, WriterInfo,
};
pub use reader::{CorruptReason, Item, OpenError, RecordReader, Tail, Verdict};
pub use summary::{
    EndSummary, GapSummary, LateRun, ReadError, ShortBlock, Summary, TailSummary,
    WALL_STEP_THRESHOLD_US, WallStep, summarize,
};
pub use writer::{RecordWriter, Truncate};

/// The four bytes every capture record starts with.
pub const MAGIC: &[u8; 4] = b"BPCR";

/// The one format version this crate reads and writes.
pub const FORMAT_VERSION: u16 = 1;

/// Payload kind byte of a block.
pub const KIND_BLOCK: u8 = 0x01;

/// Payload kind byte of an event.
pub const KIND_EVENT: u8 = 0x02;

/// Bytes of a block's payload before its PCM: kind (1), `wall_us` (8),
/// `first_frame` (8), `backlog_frames` (4).
pub const BLOCK_PREFIX_BYTES: usize = 21;

/// The most PCM one block can carry: one record's payload less the block
/// prefix.
pub const MAX_BLOCK_PCM_BYTES: usize = stamped_log::MAX_PAYLOAD - BLOCK_PREFIX_BYTES;
