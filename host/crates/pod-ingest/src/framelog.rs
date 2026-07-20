//! The frame-log record format: an append-only log of raw wire frames plus
//! their host receive timestamps. The wire bytes *are* the record, so replay
//! feeds a log straight back through the same decode + FSM path that live
//! ingest uses.
//!
//! The format is policy-free: *when* to roll or prune is the embedder's call
//! (rolls are legal only between segments, which needs session state this
//! writer must not know). Buffered writes are flushed by the embedder at
//! segment close and on roll/finish; there is no fsync — a frame log is a
//! debugging/training corpus, not a durable ledger, so crash loss is bounded
//! to the in-progress segment.
//!
//! ```text
//! header:  magic  b"RSFL"        (4 bytes)
//!          format_version u16 LE  (= 1)
//!          meta_len       u16 LE
//!          meta           JSON, meta_len bytes
//! records: host_rx_us u64 LE | len u16 LE | payload (len bytes, exact wire frame)
//! ```

use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use audio_pipeline::wire::MAX_FRAME_BYTES;
use serde::{Deserialize, Serialize};

use crate::clock::HostMicros;

const MAGIC: &[u8; 4] = b"RSFL";
const FORMAT_VERSION: u16 = 1;

/// Bytes of a fixed per-record header: `host_rx_us` (8) + `len` (2).
const RECORD_HEADER_BYTES: usize = 10;

/// Self-describing header metadata, serialized as JSON so additive fields do
/// not force a format-version bump. Unknown fields are ignored on read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogMeta {
    /// Build identity of the writing process.
    pub build_id: String,
    /// Host wall-clock (microseconds since UNIX epoch) at log creation. The
    /// `HostMicros` newtype serializes transparently, so the JSON key stays a
    /// bare integer named `created_epoch_us`.
    pub created_epoch_us: HostMicros,
    /// Per-process connection sequence number.
    pub conn_seq: u64,
    /// Name of the prior log this one rolled from, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rolled_from: Option<String>,
}

/// One item yielded while reading a frame log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogItem {
    /// A complete record.
    Record {
        host_rx: HostMicros,
        payload: Vec<u8>,
    },
    /// The final record was cut short (writer crashed mid-write). The log is
    /// fully usable up to this point; this is the terminal item.
    TornTail,
}

/// Errors from opening or reading a frame log.
#[derive(Debug, thiserror::Error)]
pub enum FrameLogError {
    #[error("frame-log I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("not a frame log: bad magic")]
    BadMagic,
    #[error("unsupported frame-log format version {got}")]
    UnsupportedVersion { got: u16 },
    #[error("corrupt frame-log header metadata: {0}")]
    BadMeta(#[source] serde_json::Error),
    #[error("corrupt frame-log record: length {len} out of range")]
    CorruptLength { len: usize },
}

/// Append-only writer for one frame log. Takes explicit paths and timestamps;
/// it reads no clock and makes no roll/prune decisions.
pub struct FrameLogWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    bytes: u64,
    created: HostMicros,
    /// The connection's `Hello` record, retained so a roll can re-emit it as
    /// the first record of the new log — every rolled log replays standalone.
    hello: Option<(HostMicros, Vec<u8>)>,
}

impl FrameLogWriter {
    /// Create a new frame log at `path`, writing the header immediately.
    /// Uses `create_new`: a path collision surfaces as `AlreadyExists` rather
    /// than silently truncating an existing capture.
    pub fn create(path: &Path, meta: LogMeta) -> io::Result<Self> {
        let file = File::create_new(path)?;
        let mut writer = BufWriter::new(file);
        let created = meta.created_epoch_us;
        let bytes = write_header(&mut writer, &meta)?;
        Ok(FrameLogWriter {
            writer,
            path: path.to_path_buf(),
            bytes,
            created,
            hello: None,
        })
    }

    /// Append one frame's exact wire bytes with its host receive time. The
    /// payload must be a single wire frame (`1..=MAX_FRAME_BYTES`).
    pub fn append(&mut self, host_rx: HostMicros, payload: &[u8]) -> io::Result<()> {
        if payload.is_empty() || payload.len() > MAX_FRAME_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "frame-log payload length out of range",
            ));
        }
        self.bytes += write_record(&mut self.writer, host_rx, payload)?;
        Ok(())
    }

    /// Retain the connection's `Hello` record for re-emission on roll. Does not
    /// itself write (the `Hello` is also captured by the normal `append` tap).
    pub fn note_hello(&mut self, host_rx: HostMicros, payload: &[u8]) {
        self.hello = Some((host_rx, payload.to_vec()));
    }

    /// Rename the underlying file (e.g. the connection-scoped name → the
    /// post-`Hello` `{pod_id}_…` name). The open handle keeps writing.
    pub fn rename_to(&mut self, path: &Path) -> io::Result<()> {
        fs::rename(&self.path, path)?;
        self.path = path.to_path_buf();
        Ok(())
    }

    /// Flush buffered writes to the file.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    /// Close the current file and open a fresh one at `path`, writing a new
    /// header (with `rolled_from` set to the prior log's name) and re-emitting
    /// the retained `Hello` record first. Legal only between segments.
    pub fn roll_to(&mut self, path: &Path, mut meta: LogMeta) -> io::Result<()> {
        self.writer.flush()?;
        meta.rolled_from = Some(file_name_string(&self.path));

        let file = File::create_new(path)?;
        let mut writer = BufWriter::new(file);
        let created = meta.created_epoch_us;
        let mut bytes = write_header(&mut writer, &meta)?;
        if let Some((host_rx, payload)) = &self.hello {
            bytes += write_record(&mut writer, *host_rx, payload)?;
        }

        self.writer = writer;
        self.path = path.to_path_buf();
        self.created = created;
        self.bytes = bytes;
        Ok(())
    }

    /// Total bytes written to the current file (header + records).
    pub fn bytes_written(&self) -> u64 {
        self.bytes
    }

    /// Microseconds since this log was created, per the caller-supplied `now`.
    /// A `now` earlier than creation (NTP step) clamps to 0.
    pub fn age(&self, now: HostMicros) -> u64 {
        now.checked_delta(self.created).unwrap_or(0)
    }

    /// Flush and close.
    pub fn finish(mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

/// Streaming reader over a frame log. Validates the header at `open`; iterating
/// yields records, a terminal `TornTail` on a mid-record EOF, or a terminal
/// error on a corrupt length field.
pub struct FrameLogReader {
    reader: BufReader<File>,
    meta: LogMeta,
    done: bool,
}

impl FrameLogReader {
    /// Open and validate a frame log's header. Bad magic or an unknown format
    /// version is a hard error here.
    pub fn open(path: &Path) -> Result<Self, FrameLogError> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(FrameLogError::BadMagic);
        }

        let mut version = [0u8; 2];
        reader.read_exact(&mut version)?;
        let version = u16::from_le_bytes(version);
        if version != FORMAT_VERSION {
            return Err(FrameLogError::UnsupportedVersion { got: version });
        }

        let mut meta_len = [0u8; 2];
        reader.read_exact(&mut meta_len)?;
        let meta_len = u16::from_le_bytes(meta_len) as usize;
        let mut meta_bytes = vec![0u8; meta_len];
        reader.read_exact(&mut meta_bytes)?;
        let meta = serde_json::from_slice(&meta_bytes).map_err(FrameLogError::BadMeta)?;

        Ok(FrameLogReader {
            reader,
            meta,
            done: false,
        })
    }

    /// The header metadata.
    pub fn meta(&self) -> &LogMeta {
        &self.meta
    }
}

impl Iterator for FrameLogReader {
    type Item = Result<LogItem, FrameLogError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let mut header = [0u8; RECORD_HEADER_BYTES];
        match read_full(&mut self.reader, &mut header) {
            Ok(ReadFull::Eof) => {
                self.done = true;
                return None;
            }
            Ok(ReadFull::Partial) => {
                self.done = true;
                return Some(Ok(LogItem::TornTail));
            }
            Ok(ReadFull::Ok) => {}
            Err(e) => {
                self.done = true;
                return Some(Err(FrameLogError::Io(e)));
            }
        }

        let host_rx = HostMicros(u64::from_le_bytes(header[0..8].try_into().unwrap()));
        let len = u16::from_le_bytes(header[8..10].try_into().unwrap()) as usize;
        if len == 0 || len > MAX_FRAME_BYTES {
            self.done = true;
            return Some(Err(FrameLogError::CorruptLength { len }));
        }

        let mut payload = vec![0u8; len];
        match read_full(&mut self.reader, &mut payload) {
            Ok(ReadFull::Ok) => Some(Ok(LogItem::Record { host_rx, payload })),
            Ok(ReadFull::Eof) | Ok(ReadFull::Partial) => {
                self.done = true;
                Some(Ok(LogItem::TornTail))
            }
            Err(e) => {
                self.done = true;
                Some(Err(FrameLogError::Io(e)))
            }
        }
    }
}

/// Outcome of a fill-the-buffer read.
enum ReadFull {
    /// The whole buffer was filled.
    Ok,
    /// EOF before any byte of the buffer (a clean record boundary).
    Eof,
    /// EOF after some but not all bytes (a torn record).
    Partial,
}

fn read_full<R: Read>(r: &mut R, buf: &mut [u8]) -> io::Result<ReadFull> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => {
                return Ok(if filled == 0 {
                    ReadFull::Eof
                } else {
                    ReadFull::Partial
                });
            }
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(ReadFull::Ok)
}

fn write_header(w: &mut impl Write, meta: &LogMeta) -> io::Result<u64> {
    let json =
        serde_json::to_vec(meta).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if json.len() > u16::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame-log header metadata exceeds 64 KiB",
        ));
    }
    w.write_all(MAGIC)?;
    w.write_all(&FORMAT_VERSION.to_le_bytes())?;
    w.write_all(&(json.len() as u16).to_le_bytes())?;
    w.write_all(&json)?;
    Ok((MAGIC.len() + 2 + 2 + json.len()) as u64)
}

fn write_record(w: &mut impl Write, host_rx: HostMicros, payload: &[u8]) -> io::Result<u64> {
    w.write_all(&host_rx.0.to_le_bytes())?;
    w.write_all(&(payload.len() as u16).to_le_bytes())?;
    w.write_all(payload)?;
    Ok((RECORD_HEADER_BYTES + payload.len()) as u64)
}

fn file_name_string(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;

    fn meta(conn_seq: u64) -> LogMeta {
        LogMeta {
            build_id: "test-build".to_string(),
            created_epoch_us: HostMicros(1_700_000_000_000_000),
            conn_seq,
            rolled_from: None,
        }
    }

    fn records(path: &Path) -> Vec<LogItem> {
        FrameLogReader::open(path)
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }

    #[test]
    fn roundtrip_header_meta_and_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.framelog");

        let mut w = FrameLogWriter::create(&path, meta(1)).unwrap();
        w.append(HostMicros(10), &[1, 2, 3]).unwrap();
        w.append(HostMicros(20), &[4, 5]).unwrap();
        w.finish().unwrap();

        let reader = FrameLogReader::open(&path).unwrap();
        assert_eq!(reader.meta(), &meta(1));
        let items: Vec<_> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(
            items,
            vec![
                LogItem::Record {
                    host_rx: HostMicros(10),
                    payload: vec![1, 2, 3],
                },
                LogItem::Record {
                    host_rx: HostMicros(20),
                    payload: vec![4, 5],
                },
            ]
        );
    }

    #[test]
    fn append_rejects_empty_and_oversize_payloads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.framelog");
        let mut w = FrameLogWriter::create(&path, meta(1)).unwrap();

        assert_eq!(
            w.append(HostMicros(1), &[]).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        let oversize = vec![0u8; MAX_FRAME_BYTES + 1];
        assert_eq!(
            w.append(HostMicros(1), &oversize).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn bad_magic_rejected_at_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.framelog");
        fs::write(&path, b"XXXX\x01\x00\x00\x00").unwrap();
        assert!(matches!(
            FrameLogReader::open(&path),
            Err(FrameLogError::BadMagic)
        ));
    }

    #[test]
    fn bad_version_rejected_at_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v99.framelog");
        // Valid magic, version 99, full required meta.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&99u16.to_le_bytes());
        let json = b"{\"build_id\":\"x\",\"created_epoch_us\":0,\"conn_seq\":0}";
        bytes.extend_from_slice(&(json.len() as u16).to_le_bytes());
        bytes.extend_from_slice(json);
        fs::write(&path, &bytes).unwrap();

        assert!(matches!(
            FrameLogReader::open(&path),
            Err(FrameLogError::UnsupportedVersion { got: 99 })
        ));
    }

    #[test]
    fn oversize_len_stops_iteration_as_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.framelog");
        let mut w = FrameLogWriter::create(&path, meta(1)).unwrap();
        w.append(HostMicros(10), &[1, 2, 3]).unwrap();
        w.finish().unwrap();

        // Append a raw record header claiming an out-of-range length.
        let mut extra = Vec::new();
        extra.extend_from_slice(&99u64.to_le_bytes());
        extra.extend_from_slice(&((MAX_FRAME_BYTES + 1) as u16).to_le_bytes());
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&extra)
            .unwrap();

        let mut reader = FrameLogReader::open(&path).unwrap();
        assert_eq!(
            reader.next().unwrap().unwrap(),
            LogItem::Record {
                host_rx: HostMicros(10),
                payload: vec![1, 2, 3],
            }
        );
        assert!(matches!(
            reader.next(),
            Some(Err(FrameLogError::CorruptLength { len })) if len == MAX_FRAME_BYTES + 1
        ));
        assert!(reader.next().is_none());
    }

    #[test]
    fn torn_final_record_yields_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.framelog");
        let mut w = FrameLogWriter::create(&path, meta(1)).unwrap();
        w.append(HostMicros(10), &[1, 2, 3]).unwrap();
        w.finish().unwrap();

        // Append a partial record header (fewer than RECORD_HEADER_BYTES).
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[0xAA, 0xBB, 0xCC])
            .unwrap();

        let items = records(&path);
        assert_eq!(
            items,
            vec![
                LogItem::Record {
                    host_rx: HostMicros(10),
                    payload: vec![1, 2, 3],
                },
                LogItem::TornTail,
            ]
        );
    }

    #[test]
    fn roll_replicates_hello_sets_rolled_from_and_replays_standalone() {
        let dir = tempfile::tempdir().unwrap();
        let path_a = dir.path().join("a.framelog");
        let path_b = dir.path().join("b.framelog");

        let hello = [0xDE, 0xAD, 0xBE, 0xEF];
        let mut w = FrameLogWriter::create(&path_a, meta(1)).unwrap();
        w.append(HostMicros(1), &hello).unwrap();
        w.note_hello(HostMicros(1), &hello);
        w.append(HostMicros(2), &[7, 7, 7]).unwrap();

        w.roll_to(&path_b, meta(1)).unwrap();
        w.append(HostMicros(3), &[9, 9]).unwrap();
        w.finish().unwrap();

        // A still readable, with its own records and no rolled_from.
        let reader_a = FrameLogReader::open(&path_a).unwrap();
        assert_eq!(reader_a.meta().rolled_from, None);
        let items_a: Vec<_> = reader_a.map(|r| r.unwrap()).collect();
        assert_eq!(
            items_a,
            vec![
                LogItem::Record {
                    host_rx: HostMicros(1),
                    payload: hello.to_vec(),
                },
                LogItem::Record {
                    host_rx: HostMicros(2),
                    payload: vec![7, 7, 7],
                },
            ]
        );

        // B replays standalone: Hello re-emitted first, rolled_from set to A.
        let reader_b = FrameLogReader::open(&path_b).unwrap();
        assert_eq!(reader_b.meta().rolled_from.as_deref(), Some("a.framelog"));
        let items_b: Vec<_> = reader_b.map(|r| r.unwrap()).collect();
        assert_eq!(
            items_b,
            vec![
                LogItem::Record {
                    host_rx: HostMicros(1),
                    payload: hello.to_vec(),
                },
                LogItem::Record {
                    host_rx: HostMicros(3),
                    payload: vec![9, 9],
                },
            ]
        );
    }

    #[test]
    fn rename_to_moves_file_and_roll_chain_uses_renamed_name() {
        let dir = tempfile::tempdir().unwrap();
        let conn = dir.path().join("conn.framelog");
        let named = dir.path().join("pod_named.framelog");
        let rolled = dir.path().join("pod_named_2.framelog");

        let hello = [0xDE, 0xAD];
        let mut w = FrameLogWriter::create(&conn, meta(1)).unwrap();
        w.append(HostMicros(1), &hello).unwrap();
        w.note_hello(HostMicros(1), &hello);
        w.rename_to(&named).unwrap();
        assert!(!conn.exists());
        assert!(named.exists());
        w.append(HostMicros(2), &[7]).unwrap();

        w.roll_to(&rolled, meta(1)).unwrap();
        w.append(HostMicros(3), &[9]).unwrap();
        w.finish().unwrap();

        // rolled_from is the *renamed* name, not the pre-rename conn name.
        let rb = FrameLogReader::open(&rolled).unwrap();
        assert_eq!(rb.meta().rolled_from.as_deref(), Some("pod_named.framelog"));

        // The renamed log holds its pre- and post-rename records.
        assert_eq!(
            records(&named),
            vec![
                LogItem::Record {
                    host_rx: HostMicros(1),
                    payload: hello.to_vec(),
                },
                LogItem::Record {
                    host_rx: HostMicros(2),
                    payload: vec![7],
                },
            ]
        );
        // The rolled log replays Hello then its own post-roll record.
        assert_eq!(
            records(&rolled),
            vec![
                LogItem::Record {
                    host_rx: HostMicros(1),
                    payload: hello.to_vec(),
                },
                LogItem::Record {
                    host_rx: HostMicros(3),
                    payload: vec![9],
                },
            ]
        );
    }

    #[test]
    fn bytes_written_tracks_header_records_and_roll_reset() {
        let dir = tempfile::tempdir().unwrap();
        let path_a = dir.path().join("a.framelog");
        let path_b = dir.path().join("b.framelog");

        let m = meta(1);
        let header_a = 4 + 2 + 2 + serde_json::to_vec(&m).unwrap().len() as u64;
        let mut w = FrameLogWriter::create(&path_a, m).unwrap();
        assert_eq!(w.bytes_written(), header_a);

        w.append(HostMicros(1), &[1, 2, 3]).unwrap();
        assert_eq!(w.bytes_written(), header_a + RECORD_HEADER_BYTES as u64 + 3);
        w.append(HostMicros(2), &[4, 5]).unwrap();
        assert_eq!(
            w.bytes_written(),
            header_a + 2 * RECORD_HEADER_BYTES as u64 + 3 + 2
        );

        // Roll (no retained Hello) resets to the new header only — the prior
        // total does not carry over.
        w.roll_to(&path_b, meta(1)).unwrap();
        let mut rolled_meta = meta(1);
        rolled_meta.rolled_from = Some("a.framelog".to_string());
        let header_b = 4 + 2 + 2 + serde_json::to_vec(&rolled_meta).unwrap().len() as u64;
        assert_eq!(w.bytes_written(), header_b);
    }

    #[test]
    fn age_is_saturating_micros_since_creation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.framelog");
        let m = LogMeta {
            build_id: "b".to_string(),
            created_epoch_us: HostMicros(100),
            conn_seq: 0,
            rolled_from: None,
        };
        let w = FrameLogWriter::create(&path, m).unwrap();
        assert_eq!(w.age(HostMicros(250)), 150);
        // A now earlier than creation clamps to 0.
        assert_eq!(w.age(HostMicros(50)), 0);
    }

    #[test]
    fn bad_meta_json_rejected_at_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("badmeta.framelog");
        // Valid magic and version, but malformed JSON in the meta bytes.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        let json = b"{not valid json";
        bytes.extend_from_slice(&(json.len() as u16).to_le_bytes());
        bytes.extend_from_slice(json);
        fs::write(&path, &bytes).unwrap();

        assert!(matches!(
            FrameLogReader::open(&path),
            Err(FrameLogError::BadMeta(_))
        ));
    }

    #[test]
    fn zero_len_record_stops_iteration_as_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.framelog");
        let mut w = FrameLogWriter::create(&path, meta(1)).unwrap();
        w.append(HostMicros(10), &[1, 2, 3]).unwrap();
        w.finish().unwrap();

        // Append a raw record header explicitly claiming a zero-length payload.
        let mut extra = Vec::new();
        extra.extend_from_slice(&5u64.to_le_bytes());
        extra.extend_from_slice(&0u16.to_le_bytes());
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&extra)
            .unwrap();

        let mut reader = FrameLogReader::open(&path).unwrap();
        assert_eq!(
            reader.next().unwrap().unwrap(),
            LogItem::Record {
                host_rx: HostMicros(10),
                payload: vec![1, 2, 3],
            }
        );
        assert!(matches!(
            reader.next(),
            Some(Err(FrameLogError::CorruptLength { len: 0 }))
        ));
        assert!(reader.next().is_none());
    }

    #[test]
    fn create_refuses_to_clobber_existing_log() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.framelog");
        fs::write(&path, b"existing capture").unwrap();
        let err = FrameLogWriter::create(&path, meta(1))
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    }
}
