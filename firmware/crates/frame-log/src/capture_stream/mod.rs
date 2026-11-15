//! A capture's stream written as a frame log: what a device granted, the items
//! a capture is made of (blocks of samples, losses and an end), the framer that
//! turns them into wire frames, and the writer that puts those in a log.
//! `pod-capture` records through it, and `frame-log write-capture` writes a log
//! from given samples through it, so both write one framing.

mod framer;
mod granted;
mod writer;

use std::fs::File;
use std::io::{self, Seek, SeekFrom};

pub use framer::{Block, End, Event, Framer, Gap, MAX_DETAIL_BYTES, Stamped, prelude};
pub use granted::{Granted, SampleFormat};
pub use writer::FrameWriter;

/// A destination a capture's log can be cut back on.
pub trait Truncate {
    /// Cut the destination to `len` bytes and leave the next write at `len`.
    fn truncate(&mut self, len: u64) -> io::Result<()>;
}

impl Truncate for File {
    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.set_len(len)?;
        self.seek(SeekFrom::Start(len))?;
        Ok(())
    }
}
