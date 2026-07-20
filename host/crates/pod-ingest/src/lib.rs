//! Sans-I/O pod session ingest: session FSM, frame-log format, and clock
//! newtypes. No sockets, threads, or files live here — callers supply frames
//! and timestamps.

pub mod clock;
pub mod framelog;
pub mod readback;
pub mod segref;
pub mod session;
pub mod synth;
#[cfg(any(test, feature = "test-util"))]
pub mod test_fixtures;

pub use clock::{samples_to_micros, ClockOffsetEstimate, DeviceMicros, HostMicros};
pub use framelog::{FrameLogError, FrameLogReader, FrameLogWriter, LogItem, LogMeta};
pub use readback::{splice_log_into, SpliceOutcome, SpliceStop};
pub use segref::{resolve_open, ResolveError, Resolved, SegmentRef};
pub use session::{
    ChannelSource, CloseCause, Codec, CrossCheck, EndReason, FormatConstraint, FsmStats, Gap,
    ProtocolErrorKind, ResumeLedger, SegmentClose, SessionEvent, SessionFsm, TelemetryKind,
};
pub use synth::{synth_session, SynthError, SynthFrame, SynthParams};
