//! Writing a capture's frame log. The frames of one item go out in one write
//! and are flushed, so the file is always whole items and at most one torn
//! one. After a write fails the writer refuses everything until the file is
//! cut back to the end of the last whole item.

use std::io::{self, Write};

use audio_pipeline::wide::{MAX_WIDE_FRAME_BYTES, encode_wide};
use frame_log::LogMeta;

use crate::framer::{Block, Event, Framer, Stamped};
use crate::seams::Truncate;

/// Writes one capture's frame log, framing each item with a [`Framer`].
pub struct FrameWriter<D: Write> {
    dest: D,
    /// The segment state after the last item written whole.
    framer: Framer,
    /// The file offset just past the last item written whole.
    offset: u64,
    /// A write failed and may have left part of an item after `offset`.
    failed: bool,
    /// One encoded frame, before it is put in its record.
    scratch: Vec<u8>,
}

fn invalid_input(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg)
}

impl<D: Write> FrameWriter<D> {
    /// Write the header and the stream's prelude in one write, and flush. A
    /// header or frame that cannot be encoded is `InvalidData` or
    /// `InvalidInput`, and nothing is written.
    pub fn new(
        mut dest: D,
        meta: &LogMeta,
        prelude: &[Stamped],
        framer: Framer,
    ) -> io::Result<Self> {
        let mut scratch = vec![0; MAX_WIDE_FRAME_BYTES + 2];
        let mut buf = Vec::new();
        frame_log::write_header(&mut buf, meta)?;
        for (stamp, frame) in prelude {
            append(&mut buf, &mut scratch, *stamp, frame)?;
        }
        dest.write_all(&buf)?;
        dest.flush()?;
        Ok(FrameWriter {
            dest,
            framer,
            offset: buf.len() as u64,
            failed: false,
            scratch,
        })
    }

    /// Write one block's frames.
    pub fn block(&mut self, block: Block) -> io::Result<()> {
        let mut next = self.framer;
        let frames = next.block(block);
        self.write(&frames)?;
        self.framer = next;
        Ok(())
    }

    /// Write one event's frames.
    pub fn event(&mut self, mono_us: u64, event: Event) -> io::Result<()> {
        let mut next = self.framer;
        let frames = next.event(mono_us, event);
        self.write(&frames)?;
        self.framer = next;
        Ok(())
    }

    /// The index after the last frame the log accounts for.
    pub fn position(&self) -> u64 {
        self.framer.position()
    }

    pub fn get_ref(&self) -> &D {
        &self.dest
    }

    /// Encode `frames` as records and write them whole, then flush. A frame
    /// that cannot be encoded is `InvalidInput` and nothing is written; any
    /// error after that marks the writer failed, since part of the item may
    /// have reached the file.
    fn write(&mut self, frames: &[Stamped]) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other(
                "an earlier write failed; the log must be cut back to its last whole item \
                 before anything more is written",
            ));
        }
        let mut buf = Vec::new();
        for (stamp, frame) in frames {
            append(&mut buf, &mut self.scratch, *stamp, frame)?;
        }
        match self.dest.write_all(&buf).and_then(|()| self.dest.flush()) {
            Ok(()) => {
                self.offset += buf.len() as u64;
                Ok(())
            }
            Err(e) => {
                self.failed = true;
                Err(e)
            }
        }
    }
}

impl<D: Write + Truncate> FrameWriter<D> {
    /// Cut the file back to the end of the last whole item, dropping any part
    /// of one a failed write left, so the next item follows it directly. The
    /// writer accepts items again only once this succeeds.
    pub fn truncate_to_last_item(&mut self) -> io::Result<()> {
        self.dest.truncate(self.offset)?;
        self.failed = false;
        Ok(())
    }
}

/// Encode `frame` and append it to `buf` as one record stamped `stamp`.
fn append(
    buf: &mut Vec<u8>,
    scratch: &mut [u8],
    stamp: frame_log::HostMicros,
    frame: &audio_pipeline::wide::WideFrame,
) -> io::Result<()> {
    let n = encode_wide(frame, scratch)
        .map_err(|e| invalid_input(format!("encoding a frame failed: {e:?}")))?;
    frame_log::write_record(buf, stamp, &scratch[..n])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framer::{End, prelude};
    use crate::granted::{Granted, SampleFormat};
    use audio_pipeline::wide::StreamEndReason;
    use frame_log::{HostMicros, LogItem};

    fn granted() -> Granted {
        Granted {
            rate_hz: 1000,
            channels: 1,
            sample_format: SampleFormat::S16Le,
            period_frames: 10,
            buffer_frames: 160,
        }
    }

    fn meta() -> LogMeta {
        LogMeta {
            build_id: "b".to_owned(),
            created_epoch_us: HostMicros(1),
            conn_seq: 0,
            rolled_from: None,
            capture: None,
        }
    }

    /// A destination that takes `allow` writes, then refuses every one.
    struct Dest {
        bytes: Vec<u8>,
        allow: usize,
    }

    impl Write for Dest {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.allow == 0 {
                return Err(io::Error::other("refused"));
            }
            self.allow -= 1;
            self.bytes.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Truncate for Dest {
        fn truncate(&mut self, len: u64) -> io::Result<()> {
            self.bytes.truncate(len as usize);
            Ok(())
        }
    }

    fn block(first_frame: u64, bytes: usize) -> Block {
        Block {
            mono_us: 100_000,
            wall_us: 5,
            first_frame,
            backlog_frames: 0,
            pcm: vec![1; bytes],
        }
    }

    fn writer(allow: usize) -> FrameWriter<Dest> {
        let g = granted();
        let prelude = prelude("p", &g, &["m".to_owned()], 0, 1).unwrap();
        FrameWriter::new(
            Dest {
                bytes: Vec::new(),
                allow,
            },
            &meta(),
            &prelude,
            Framer::new(&g),
        )
        .unwrap()
    }

    /// The records of the destination's bytes.
    fn records(bytes: &[u8]) -> usize {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(bytes).unwrap();
        frame_log::FrameLogReader::open(f.path())
            .unwrap()
            .map(|i| match i.unwrap() {
                LogItem::Record { .. } => 1,
                LogItem::TornTail => panic!("torn"),
            })
            .sum()
    }

    #[test]
    fn the_header_and_prelude_are_one_write_and_each_item_another() {
        let mut w = writer(3);
        assert_eq!(records(&w.get_ref().bytes), 2);
        w.block(block(0, 20)).unwrap();
        w.block(block(10, 20)).unwrap();
        // start, audio, read; audio, read.
        assert_eq!(records(&w.get_ref().bytes), 7);
        assert_eq!(w.position(), 20);
    }

    #[test]
    fn a_frame_that_does_not_encode_writes_nothing_and_does_not_latch() {
        let mut w = writer(usize::MAX);
        let before = w.get_ref().bytes.len();
        let err = w.block(block(0, MAX_WIDE_FRAME_BYTES)).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(w.get_ref().bytes.len(), before);
        // The refused block opened no segment: the next one opens it.
        w.block(block(0, 20)).unwrap();
        assert_eq!(records(&w.get_ref().bytes), 5);
    }

    #[test]
    fn a_failed_write_latches_until_the_log_is_cut_back_and_keeps_the_segment_state() {
        let mut w = writer(1);
        assert!(w.block(block(0, 20)).is_err());
        assert!(!w.get_ref().bytes.is_empty());
        w.dest.allow = usize::MAX;
        assert!(w.block(block(0, 20)).is_err(), "latched");
        w.truncate_to_last_item().unwrap();
        assert_eq!(records(&w.get_ref().bytes), 2);
        // The failed block's segment never reached the log, so the end opens
        // no segment end.
        w.event(
            9,
            Event::End(End {
                reason: StreamEndReason::WriteFailed,
                at_frame: w.position(),
                wall_us: 9,
                detail: "refused".to_owned(),
            }),
        )
        .unwrap();
        assert_eq!(records(&w.get_ref().bytes), 3);
        assert_eq!(w.position(), 0);
    }
}
