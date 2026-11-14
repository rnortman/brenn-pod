//! The frame log: an append-only log of framed wire frames, each with a stamp.
//! The wire bytes *are* the record, so replay feeds a log straight back
//! through the same decode path that live ingest uses.
//!
//! [`FrameLogWriter`] is the voice host's file writer. It is policy-free: *when* to roll or prune is the embedder's call
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
//! records: stamp u64 LE | len u16 LE | payload (len bytes, 1..=65535: one framed wire frame)
//! ```
//!
//! The framing is [`crate::container`]'s. A record's stamp is a [`HostMicros`]:
//! the voice host's receive time, or the wall reading of a capture's clock pair.
//! [`write_header`] and [`write_record`] are the format's encoder, for a writer
//! with its own destination and flush policy.

use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::HostMicros;
use crate::container::{self, HeaderError, Item, MAX_PAYLOAD, RecordError, Records};
use crate::header::{CaptureMetaInvalid, LogMeta};

const MAGIC: &[u8; 4] = b"RSFL";
const FORMAT_VERSION: u16 = 1;

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
    #[error("frame-log header: {0}")]
    BadCapture(#[source] CaptureMetaInvalid),
    /// A record length of zero: the bytes from here on cannot be framed.
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

    /// Append one framed wire frame's exact bytes with its host receive time.
    /// A payload outside `1..=65535` bytes is `InvalidInput` and nothing is
    /// written.
    pub fn append(&mut self, host_rx: HostMicros, payload: &[u8]) -> io::Result<()> {
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
/// error on a zero length field.
pub struct FrameLogReader {
    records: Records<BufReader<File>>,
    /// Boxed: a capture's description makes `LogMeta` large, and readers are
    /// held in enums beside unit variants.
    meta: Box<LogMeta>,
    raw_meta: Vec<u8>,
}

impl FrameLogReader {
    /// Open and validate a frame log's header. Bad magic, an unknown format
    /// version, or metadata that does not parse or whose `capture` breaks the
    /// format's rules is a hard error here.
    pub fn open(path: &Path) -> Result<Self, FrameLogError> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let header = container::read_header(&mut reader, MAGIC).map_err(|e| match e {
            HeaderError::BadMagic => FrameLogError::BadMagic,
            HeaderError::Io(e) => FrameLogError::Io(e),
        })?;
        if header.version != FORMAT_VERSION {
            return Err(FrameLogError::UnsupportedVersion {
                got: header.version,
            });
        }
        let meta: LogMeta = serde_json::from_slice(&header.meta).map_err(FrameLogError::BadMeta)?;
        if let Some(capture) = &meta.capture {
            capture.validate().map_err(FrameLogError::BadCapture)?;
        }

        Ok(FrameLogReader {
            records: Records::new(reader, header.encoded_len(), MAX_PAYLOAD),
            meta: Box::new(meta),
            raw_meta: header.meta,
        })
    }

    /// The header metadata.
    pub fn meta(&self) -> &LogMeta {
        &self.meta
    }

    /// The header metadata's JSON as written, unknown keys included.
    pub fn raw_meta(&self) -> &[u8] {
        &self.raw_meta
    }

    /// The byte offset of the next unread record. After a torn tail or a
    /// corrupt length it is where that record starts, so the file up to here
    /// is whole records.
    pub fn offset(&self) -> u64 {
        self.records.offset()
    }
}

impl Iterator for FrameLogReader {
    type Item = Result<LogItem, FrameLogError>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(match self.records.next()? {
            Ok(Item::Record { stamp, payload }) => Ok(LogItem::Record {
                host_rx: HostMicros(stamp),
                payload,
            }),
            Ok(Item::TornTail) => Ok(LogItem::TornTail),
            Err(RecordError::CorruptLength { len }) => Err(FrameLogError::CorruptLength { len }),
            Err(RecordError::Io(e)) => Err(FrameLogError::Io(e)),
        })
    }
}

/// Write a frame log's header in one `write_all`. Returns the bytes written.
/// A `capture` that breaks the format's rules, or metadata over 65535 bytes,
/// is `InvalidData` and nothing is written.
pub fn write_header(w: &mut impl Write, meta: &LogMeta) -> io::Result<u64> {
    if let Some(capture) = &meta.capture {
        capture
            .validate()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    }
    let json =
        serde_json::to_vec(meta).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    container::write_header(w, MAGIC, FORMAT_VERSION, &json)
}

/// Write one record in one `write_all`. Returns the bytes written. A payload
/// outside `1..=65535` bytes is `InvalidInput` and nothing is written.
pub fn write_record(w: &mut impl Write, stamp: HostMicros, payload: &[u8]) -> io::Result<u64> {
    container::write_record(w, stamp.0, payload)
}

fn file_name_string(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::container::RECORD_HEADER_BYTES;
    use crate::header::tests::{capture, read_state};
    use std::fs::OpenOptions;

    fn meta(conn_seq: u64) -> LogMeta {
        LogMeta {
            build_id: "test-build".to_string(),
            created_epoch_us: HostMicros(1_700_000_000_000_000),
            conn_seq,
            rolled_from: None,
            capture: None,
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
        let oversize = vec![0u8; MAX_PAYLOAD + 1];
        assert_eq!(
            w.append(HostMicros(1), &oversize).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        w.append(HostMicros(2), &vec![7u8; MAX_PAYLOAD]).unwrap();
        w.finish().unwrap();

        let items = records(&path);
        assert_eq!(
            items,
            vec![LogItem::Record {
                host_rx: HostMicros(2),
                payload: vec![7u8; MAX_PAYLOAD],
            }]
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
            capture: None,
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

    /// The exact bytes of a small log, spelled out rather than derived from the
    /// writer, so a change to the framing or the header shows up here.
    #[test]
    fn golden_bytes_of_a_two_record_log() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("golden.framelog");
        let m = LogMeta {
            build_id: "golden".to_string(),
            created_epoch_us: HostMicros(1_700_000_000_000_000),
            conn_seq: 7,
            rolled_from: None,
            capture: None,
        };
        let mut w = FrameLogWriter::create(&path, m).unwrap();
        w.append(HostMicros(0x0102_0304_0506_0708), &[0xAA, 0xBB, 0xCC])
            .unwrap();
        w.append(HostMicros(42), &[0x01]).unwrap();
        w.finish().unwrap();

        let json: &[u8] =
            b"{\"build_id\":\"golden\",\"created_epoch_us\":1700000000000000,\"conn_seq\":7}";
        assert_eq!(json.len(), 70);
        let mut expected: Vec<u8> = Vec::new();
        expected.extend_from_slice(b"RSFL");
        expected.extend_from_slice(&[0x01, 0x00]);
        expected.extend_from_slice(&[70, 0x00]);
        expected.extend_from_slice(json);
        expected.extend_from_slice(&[0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);
        expected.extend_from_slice(&[0x03, 0x00]);
        expected.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        expected.extend_from_slice(&[42, 0, 0, 0, 0, 0, 0, 0]);
        expected.extend_from_slice(&[0x01, 0x00]);
        expected.extend_from_slice(&[0x01]);

        assert_eq!(fs::read(&path).unwrap(), expected);
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

    /// A log's header bytes with the given metadata, laid through the
    /// container so a test can place metadata the writer would refuse.
    fn raw_log(json: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        container::write_header(&mut bytes, MAGIC, FORMAT_VERSION, json).unwrap();
        bytes
    }

    #[test]
    fn a_capture_header_round_trips_with_its_raw_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.framelog");
        let mut m = meta(0);
        let mut c = capture();
        c.device_state = Some(read_state());
        m.capture = Some(c);
        let mut file = Vec::new();
        let header_len = write_header(&mut file, &m).unwrap();
        write_record(&mut file, HostMicros(5), &[1, 2]).unwrap();
        fs::write(&path, &file).unwrap();

        let mut reader = FrameLogReader::open(&path).unwrap();
        assert_eq!(reader.meta(), &m);
        assert_eq!(reader.raw_meta(), serde_json::to_vec(&m).unwrap());
        assert_eq!(reader.offset(), header_len);
        assert!(matches!(reader.next(), Some(Ok(LogItem::Record { .. }))));
        assert_eq!(reader.offset(), file.len() as u64);
        assert!(reader.next().is_none());
    }

    #[test]
    fn a_header_without_capture_has_no_capture_key() {
        let json = serde_json::to_string(&meta(3)).unwrap();
        assert_eq!(
            json,
            "{\"build_id\":\"test-build\",\"created_epoch_us\":1700000000000000,\"conn_seq\":3}"
        );
    }

    #[test]
    fn unknown_keys_are_ignored_and_kept_in_the_raw_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("u.framelog");
        let mut m = serde_json::to_value(meta(0)).unwrap();
        m["later_key"] = serde_json::json!({"x": 1});
        let mut c = serde_json::to_value(capture()).unwrap();
        c["later_key"] = serde_json::json!(true);
        m["capture"] = c;
        let json = serde_json::to_vec(&m).unwrap();
        fs::write(&path, raw_log(&json)).unwrap();

        let reader = FrameLogReader::open(&path).unwrap();
        assert_eq!(reader.meta().capture, Some(capture()));
        assert_eq!(reader.raw_meta(), json);
    }

    #[test]
    fn an_invalid_capture_is_refused_on_write_and_on_read() {
        let mut m = meta(0);
        let mut c = capture();
        c.granted.period_frames = 0;
        m.capture = Some(c);

        let mut file = Vec::new();
        let err = write_header(&mut file, &m).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(file.is_empty());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.framelog");
        fs::write(&path, raw_log(&serde_json::to_vec(&m).unwrap())).unwrap();
        assert!(matches!(
            FrameLogReader::open(&path),
            Err(FrameLogError::BadCapture(e)) if e.0.contains("period_frames")
        ));
    }

    #[test]
    fn offset_after_a_torn_tail_is_the_last_whole_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.framelog");
        let mut file = Vec::new();
        write_header(&mut file, &meta(1)).unwrap();
        write_record(&mut file, HostMicros(1), &[1, 2, 3]).unwrap();
        let whole = file.len() as u64;
        file.extend_from_slice(&[0xAA; RECORD_HEADER_BYTES + 1]);
        fs::write(&path, &file).unwrap();

        let mut reader = FrameLogReader::open(&path).unwrap();
        assert!(matches!(reader.next(), Some(Ok(LogItem::Record { .. }))));
        assert_eq!(reader.next().unwrap().unwrap(), LogItem::TornTail);
        assert_eq!(reader.offset(), whole);
    }
}
