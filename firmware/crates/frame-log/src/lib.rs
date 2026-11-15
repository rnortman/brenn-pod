//! The frame log: one file format for a stream of wire frames, each with a
//! stamp, behind a self-describing header. The voice host records a pod's
//! connection into one; `pod-capture` records a capture device into one;
//! `frame-log write-capture` writes one from given samples.
//!
//! - `container`: the framing, with no meaning given to the metadata or a
//!   record.
//! - [`HostMicros`]: a record's stamp.
//! - [`LogMeta`] and [`CaptureMeta`]: the header metadata.
//! - [`write_header`], [`write_record`], [`FrameLogWriter`] and
//!   [`FrameLogReader`]: the format in both directions.
//! - [`walk()`]: one pass over a log's frames, checked against the stream's
//!   rules, ending in a tail verdict.
//! - [`summarize`]: what a log holds, from the walker.
//! - [`export_wav`]: a log's audio as a WAV file, with a map of its holes.
//! - [`capture_stream`]: a capture's items framed and written as a log.
//! - [`write_capture()`]: a capture log written from a WAV file's samples and
//!   a spec.
//! - [`WavReader`]: a WAV file of integer PCM, read.

pub mod capture_stream;
mod container;
mod export;
mod header;
mod log;
mod new_file;
mod stamp;
mod summary;
mod unmeasured;
mod walk;
mod wav;
mod write_capture;

pub use export::{Basis, ExportError, ExportMap, Hole, Resent, Run, export_wav};
pub use header::{
    BuildSource, CaptureGranted, CaptureMeta, CaptureMetaInvalid, Clocks, CutShortWhy, Device,
    DeviceControl, DeviceState, LogMeta, NotAttemptedWhy, RegisterEntry, RegisterReading,
    RegisterRefusal, UnavailableWhy, UsbLocation, WriterBuild, WriterInfo,
};
pub use log::{FrameLogError, FrameLogReader, FrameLogWriter, LogItem, write_header, write_record};
pub use stamp::HostMicros;
pub use summary::{
    CaptureSummary, Cause, ClockPair, Discontinuity, EndSummary, LateRun, OtherFrames,
    SegmentSummary, ShortBlock, Summary, SummaryError, TailSummary, WALL_STEP_THRESHOLD_US,
    WallStep, summarize,
};
pub use walk::{
    AudioStep, CorruptReason, Description, Jump, JumpKind, Step, Tail, Verdict, Walk, walk,
};
pub use wav::{WAV_HEADER_BYTES, WavError, WavFormat, WavReader};
pub use write_capture::{
    EndSpec, LossSpec, Spec, StartPair, WriteCaptureError, Written, write_capture,
};
