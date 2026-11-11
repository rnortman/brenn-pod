//! Writing a capture record. Every record goes out in one write and is
//! flushed, so the file is always whole records and at most one torn one.
//! After a write fails the writer refuses every record until the file is cut
//! back to the last whole one.

use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;

use crate::{
    BLOCK_PREFIX_BYTES, Event, FORMAT_VERSION, Header, KIND_BLOCK, KIND_EVENT, MAGIC,
    MAX_BLOCK_PCM_BYTES,
};

/// A destination a capture record can be cut back on.
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

/// Writes one capture record.
pub struct RecordWriter<W: Write> {
    w: W,
    frame_bytes: usize,
    offset: u64,
    /// A write failed and may have left part of a record after `offset`.
    failed: bool,
}

fn invalid_input(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg)
}

impl RecordWriter<File> {
    /// Create `path`, refusing one that exists, and write the header.
    pub fn create(path: &Path, header: &Header) -> io::Result<Self> {
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        RecordWriter::new(file, header)
    }
}

impl<W: Write> RecordWriter<W> {
    /// Validate the header, write it and flush. A header that breaks the
    /// format's rules is `InvalidInput` and nothing is written.
    pub fn new(mut w: W, header: &Header) -> io::Result<Self> {
        header
            .validate()
            .map_err(|e| invalid_input(e.to_string()))?;
        let meta = serde_json::to_vec(header).map_err(|e| invalid_input(e.to_string()))?;
        let offset = stamped_log::write_header(&mut w, MAGIC, FORMAT_VERSION, &meta)?;
        w.flush()?;
        Ok(RecordWriter {
            w,
            frame_bytes: header.granted.frame_bytes(),
            offset,
            failed: false,
        })
    }

    /// Write one block: `pcm` is whole interleaved frames as the device
    /// delivered them. PCM that is empty, is not whole frames, or does not
    /// fit in one record is `InvalidInput` and nothing is written.
    pub fn block(
        &mut self,
        mono_us: u64,
        wall_us: u64,
        first_frame: u64,
        backlog_frames: u32,
        pcm: &[u8],
    ) -> io::Result<()> {
        if pcm.is_empty() {
            return Err(invalid_input("block PCM is empty".to_owned()));
        }
        if !pcm.len().is_multiple_of(self.frame_bytes) {
            return Err(invalid_input(format!(
                "block PCM is {} bytes, not whole {}-byte frames",
                pcm.len(),
                self.frame_bytes
            )));
        }
        if pcm.len() > MAX_BLOCK_PCM_BYTES {
            return Err(invalid_input(format!(
                "block PCM is {} bytes; at most {MAX_BLOCK_PCM_BYTES} fit in a record",
                pcm.len()
            )));
        }
        let mut payload = Vec::with_capacity(BLOCK_PREFIX_BYTES + pcm.len());
        payload.push(KIND_BLOCK);
        payload.extend_from_slice(&wall_us.to_le_bytes());
        payload.extend_from_slice(&first_frame.to_le_bytes());
        payload.extend_from_slice(&backlog_frames.to_le_bytes());
        payload.extend_from_slice(pcm);
        self.record(mono_us, &payload)
    }

    /// Write one event. An event that breaks the format's rules (a measured
    /// gap with no length, an unmeasured one with a length) or does not fit
    /// in one record is `InvalidInput` and nothing is written.
    pub fn event(&mut self, mono_us: u64, event: &Event) -> io::Result<()> {
        event.check().map_err(invalid_input)?;
        let mut payload = vec![KIND_EVENT];
        serde_json::to_writer(&mut payload, event).map_err(|e| invalid_input(e.to_string()))?;
        if payload.len() > stamped_log::MAX_PAYLOAD {
            return Err(invalid_input(format!(
                "event is {} bytes; at most {} fit in a record",
                payload.len(),
                stamped_log::MAX_PAYLOAD
            )));
        }
        self.record(mono_us, &payload)
    }

    /// Write one record whole and flush it. Any error marks the writer
    /// failed, since part of the record may have reached the file.
    fn record(&mut self, mono_us: u64, payload: &[u8]) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other(
                "an earlier write failed; the record must be truncated to its last whole record \
                 before anything more is written",
            ));
        }
        let written = stamped_log::write_record(&mut self.w, mono_us, payload)
            .and_then(|n| self.w.flush().map(|()| n));
        match written {
            Ok(n) => {
                self.offset += n;
                Ok(())
            }
            Err(e) => {
                self.failed = true;
                Err(e)
            }
        }
    }

    /// The file offset just past the last record written whole: where a
    /// record left torn by a failed write starts.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn get_ref(&self) -> &W {
        &self.w
    }
}

impl<W: Write + Truncate> RecordWriter<W> {
    /// Cut the file back to the end of the last whole record, dropping any
    /// part of a record a failed write left, so the next record follows it
    /// directly. The writer accepts records again only once this succeeds.
    pub fn truncate_to_last_record(&mut self) -> io::Result<()> {
        self.w.truncate(self.offset)?;
        self.failed = false;
        Ok(())
    }
}
