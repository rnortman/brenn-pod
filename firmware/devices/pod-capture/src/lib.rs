//! The Linux pod's recorder: one ALSA capture device, every channel, to a local
//! capture record.
//!
//! The capture loop runs on the caller's thread and never touches the file; one
//! writer thread, fed by a bounded queue, writes every record. A slow destination
//! costs queue space and, past it, a `writer_backlog` gap of exact length, never a
//! capture period. Every loss is a record at the place it happened, and every
//! state the capture can end in is one event line on stdout.
//!
//! The loop is generic over three seams, [`CaptureSource`], [`Clock`] and
//! [`Destination`], each with one production implementation, so its accounting
//! can be asserted without a sound card or a disk.

mod capture;
pub mod cli;
pub mod lines;
mod seams;
mod start;

pub use capture::{
    FLOOR_EXTRA_BYTES, FLOOR_SECONDS, Outcome, STALL_TIMEOUTS, STOP_BOUND, WAIT_TIMEOUT,
    free_space_floor, period_frames, queue_capacity, run,
};
pub use seams::{CaptureSource, Clock, Destination, SystemClock};
pub use start::{BUFFER_PERIODS, main_with};
