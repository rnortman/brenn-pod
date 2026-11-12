//! The Linux pod's recorder: one ALSA capture device, every channel, to a local
//! frame log.
//!
//! The capture loop runs on the caller's thread and never touches the file; one
//! writer thread, fed by a bounded queue, frames every item as wire frames and
//! writes them. A slow destination costs queue space and, past it, a
//! `writer_backlog` loss of exact length, never a capture period. Every loss is
//! a frame at the place it happened, and every state the capture can end in is
//! one event line on stdout.
//!
//! The loop is generic over three seams, [`CaptureSource`], [`Clock`] and
//! [`Destination`], each with one production implementation, so its accounting
//! can be asserted without a sound card or a disk.

mod capture;
pub mod cli;
mod framer;
mod granted;
pub mod lines;
mod seams;
mod start;
mod writer;

pub use capture::{
    FLOOR_EXTRA_BYTES, FLOOR_SECONDS, Outcome, Recording, STALL_TIMEOUTS, STOP_BOUND, WAIT_TIMEOUT,
    free_space_floor, period_frames, queue_capacity, run,
};
pub use granted::{Granted, SampleFormat};
pub use seams::{CaptureSource, Clock, Destination, SystemClock, Truncate};
pub use start::{BUFFER_PERIODS, main_with};
