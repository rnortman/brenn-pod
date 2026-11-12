//! An append-only container of stamped records behind a self-describing header.
//!
//! ```text
//! header:  magic (4 bytes) | format_version u16 LE | meta_len u16 LE | meta (meta_len bytes)
//! records: stamp u64 LE | len u16 LE | payload (len bytes, 1..=65535)
//! ```
//!
//! This module gives no meaning to the magic, the version, the metadata, a
//! stamp or a payload: the format built on it checks its own version, parses
//! its own metadata and says what its stamps count. What it does own is the
//! framing and
//! the reading of a file that ends early: a reader can tell a clean end on a
//! record boundary from a record cut short, and a length that cannot be right
//! from either.

use std::fmt;
use std::io::{self, Read, Write};

/// Bytes of a header before its metadata: magic (4), version (2), length (2).
pub const HEADER_FIXED_BYTES: usize = 8;

/// Bytes of a record before its payload: stamp (8), length (2).
pub const RECORD_HEADER_BYTES: usize = 10;

/// The largest payload one record can carry, and the largest metadata a header
/// can carry: both lengths are `u16`.
pub const MAX_PAYLOAD: usize = u16::MAX as usize;

/// Write a header. Returns the bytes written. Metadata longer than
/// [`MAX_PAYLOAD`] is `InvalidData` and nothing is written.
pub fn write_header(
    w: &mut impl Write,
    magic: &[u8; 4],
    version: u16,
    meta: &[u8],
) -> io::Result<u64> {
    let meta_len = u16::try_from(meta.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "header metadata is {} bytes; at most {MAX_PAYLOAD} fit",
                meta.len()
            ),
        )
    })?;
    let mut buf = Vec::with_capacity(HEADER_FIXED_BYTES + meta.len());
    buf.extend_from_slice(magic);
    buf.extend_from_slice(&version.to_le_bytes());
    buf.extend_from_slice(&meta_len.to_le_bytes());
    buf.extend_from_slice(meta);
    w.write_all(&buf)?;
    Ok(buf.len() as u64)
}

/// A header as read: the version and the metadata bytes, neither checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub version: u16,
    pub meta: Vec<u8>,
}

impl Header {
    /// Bytes the header occupies in the file, which is the offset of the first
    /// record.
    pub fn encoded_len(&self) -> u64 {
        (HEADER_FIXED_BYTES + self.meta.len()) as u64
    }
}

/// Why a header could not be read.
#[derive(Debug)]
pub enum HeaderError {
    /// The first four bytes are not the expected magic.
    BadMagic,
    /// The read failed, or the file ended inside the header (`UnexpectedEof`).
    Io(io::Error),
}

impl fmt::Display for HeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HeaderError::BadMagic => f.write_str("bad magic"),
            HeaderError::Io(e) => write!(f, "I/O error reading header: {e}"),
        }
    }
}

impl std::error::Error for HeaderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            HeaderError::BadMagic => None,
            HeaderError::Io(e) => Some(e),
        }
    }
}

/// Read a header and check its magic. The version and metadata are returned
/// unchecked.
pub fn read_header(r: &mut impl Read, magic: &[u8; 4]) -> Result<Header, HeaderError> {
    let mut fixed = [0u8; HEADER_FIXED_BYTES];
    r.read_exact(&mut fixed[..4]).map_err(HeaderError::Io)?;
    if &fixed[..4] != magic {
        return Err(HeaderError::BadMagic);
    }
    r.read_exact(&mut fixed[4..]).map_err(HeaderError::Io)?;
    let version = u16::from_le_bytes([fixed[4], fixed[5]]);
    let meta_len = u16::from_le_bytes([fixed[6], fixed[7]]) as usize;
    let mut meta = vec![0u8; meta_len];
    r.read_exact(&mut meta).map_err(HeaderError::Io)?;
    Ok(Header { version, meta })
}

/// Write one record in a single `write_all`. Returns the bytes written. An
/// empty payload, or one over [`MAX_PAYLOAD`], is `InvalidInput` and nothing is
/// written.
pub fn write_record(w: &mut impl Write, stamp: u64, payload: &[u8]) -> io::Result<u64> {
    let len = match u16::try_from(payload.len()) {
        Ok(0) | Err(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "record payload is {} bytes; 1..={MAX_PAYLOAD} fit",
                    payload.len()
                ),
            ));
        }
        Ok(len) => len,
    };
    let mut buf = Vec::with_capacity(RECORD_HEADER_BYTES + payload.len());
    buf.extend_from_slice(&stamp.to_le_bytes());
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(payload);
    w.write_all(&buf)?;
    Ok(buf.len() as u64)
}

/// One item read from the records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Item {
    /// A whole record.
    Record { stamp: u64, payload: Vec<u8> },
    /// The file ended inside a record. Everything before it is whole; this is
    /// the last item.
    TornTail,
}

/// Why reading the records stopped with an error. Either is the last item.
#[derive(Debug)]
pub enum RecordError {
    /// A record's length is 0 or over the reader's `max_payload`: the bytes
    /// from here on cannot be framed.
    CorruptLength { len: usize },
    /// The read failed.
    Io(io::Error),
}

impl fmt::Display for RecordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecordError::CorruptLength { len } => {
                write!(f, "record length {len} out of range")
            }
            RecordError::Io(e) => write!(f, "I/O error reading record: {e}"),
        }
    }
}

impl std::error::Error for RecordError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RecordError::CorruptLength { .. } => None,
            RecordError::Io(e) => Some(e),
        }
    }
}

/// The records of a file, read in order from a reader positioned at the first
/// one. Iteration ends after the last whole record on a clean end of file, or
/// after one terminal [`Item::TornTail`] or [`RecordError`].
pub struct Records<R: Read> {
    reader: R,
    max_payload: usize,
    offset: u64,
    done: bool,
}

impl<R: Read> Records<R> {
    /// `offset` is the file offset the reader is positioned at (the header's
    /// [`Header::encoded_len`]); `max_payload` is the largest length a record
    /// of this format may carry, `1..=MAX_PAYLOAD`.
    ///
    /// # Panics
    ///
    /// If `max_payload` is 0 or over [`MAX_PAYLOAD`].
    pub fn new(reader: R, offset: u64, max_payload: usize) -> Self {
        assert!(
            (1..=MAX_PAYLOAD).contains(&max_payload),
            "max_payload {max_payload} outside 1..={MAX_PAYLOAD}"
        );
        Records {
            reader,
            max_payload,
            offset,
            done: false,
        }
    }

    /// The file offset of the next unread record: after a torn tail or a
    /// corrupt length, the offset where that record starts, so the file up to
    /// here is whole records.
    pub fn offset(&self) -> u64 {
        self.offset
    }
}

impl<R: Read> Iterator for Records<R> {
    type Item = Result<Item, RecordError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let mut header = [0u8; RECORD_HEADER_BYTES];
        match read_full(&mut self.reader, &mut header) {
            Ok(Fill::Whole) => {}
            Ok(Fill::Empty) => {
                self.done = true;
                return None;
            }
            Ok(Fill::Partial) => {
                self.done = true;
                return Some(Ok(Item::TornTail));
            }
            Err(e) => {
                self.done = true;
                return Some(Err(RecordError::Io(e)));
            }
        }

        let mut stamp = [0u8; 8];
        stamp.copy_from_slice(&header[..8]);
        let stamp = u64::from_le_bytes(stamp);
        let len = u16::from_le_bytes([header[8], header[9]]) as usize;
        if len == 0 || len > self.max_payload {
            self.done = true;
            return Some(Err(RecordError::CorruptLength { len }));
        }

        let mut payload = vec![0u8; len];
        match read_full(&mut self.reader, &mut payload) {
            Ok(Fill::Whole) => {
                self.offset += (RECORD_HEADER_BYTES + len) as u64;
                Some(Ok(Item::Record { stamp, payload }))
            }
            Ok(Fill::Empty | Fill::Partial) => {
                self.done = true;
                Some(Ok(Item::TornTail))
            }
            Err(e) => {
                self.done = true;
                Some(Err(RecordError::Io(e)))
            }
        }
    }
}

/// How far a fill-the-buffer read got.
enum Fill {
    Whole,
    /// End of file before the first byte.
    Empty,
    /// End of file after some bytes but not all.
    Partial,
}

fn read_full(r: &mut impl Read, buf: &mut [u8]) -> io::Result<Fill> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => {
                return Ok(if filled == 0 {
                    Fill::Empty
                } else {
                    Fill::Partial
                });
            }
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(Fill::Whole)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAGIC: &[u8; 4] = b"TEST";

    struct Sample {
        file: Vec<u8>,
        header_len: u64,
        records: Vec<(u64, Vec<u8>)>,
    }

    fn sample() -> Sample {
        let records = vec![
            (1u64, vec![0xAAu8]),
            (u64::MAX, vec![1, 2, 3, 4, 5]),
            (0, vec![9; 300]),
        ];
        let mut file = Vec::new();
        let header_len = write_header(&mut file, MAGIC, 3, b"{\"k\":1}").unwrap();
        for (stamp, payload) in &records {
            write_record(&mut file, *stamp, payload).unwrap();
        }
        Sample {
            file,
            header_len,
            records,
        }
    }

    fn read_all(bytes: &[u8], offset: u64, max: usize) -> (Vec<Result<Item, RecordError>>, u64) {
        let mut recs = Records::new(bytes, offset, max);
        let items: Vec<_> = recs.by_ref().collect();
        (items, recs.offset())
    }

    #[test]
    fn header_and_records_round_trip() {
        let Sample {
            file,
            header_len,
            records,
        } = sample();
        assert_eq!(header_len, 8 + 7);

        let mut r = &file[..];
        let header = read_header(&mut r, MAGIC).unwrap();
        assert_eq!(
            header,
            Header {
                version: 3,
                meta: b"{\"k\":1}".to_vec()
            }
        );
        assert_eq!(header.encoded_len(), header_len);

        let (items, offset) = read_all(r, header.encoded_len(), MAX_PAYLOAD);
        let items: Vec<Item> = items.into_iter().map(Result::unwrap).collect();
        let expected: Vec<Item> = records
            .into_iter()
            .map(|(stamp, payload)| Item::Record { stamp, payload })
            .collect();
        assert_eq!(items, expected);
        assert_eq!(offset, file.len() as u64);
    }

    #[test]
    fn exact_bytes_of_header_and_record() {
        let mut file = Vec::new();
        assert_eq!(write_header(&mut file, b"ABCD", 0x0102, b"xy").unwrap(), 10);
        assert_eq!(
            write_record(&mut file, 0x1122_3344_5566_7788, &[7]).unwrap(),
            11
        );
        assert_eq!(
            file,
            [
                b'A', b'B', b'C', b'D', 0x02, 0x01, 0x02, 0x00, b'x', b'y', //
                0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x01, 0x00, 0x07,
            ]
        );
    }

    #[test]
    fn bad_magic_is_refused() {
        let Sample { file, .. } = sample();
        let mut r = &file[..];
        assert!(matches!(
            read_header(&mut r, b"NOPE"),
            Err(HeaderError::BadMagic)
        ));
    }

    #[test]
    fn a_header_cut_short_is_an_io_error() {
        let Sample {
            file, header_len, ..
        } = sample();
        for cut in 0..header_len as usize {
            let mut r = &file[..cut];
            match read_header(&mut r, MAGIC) {
                Err(HeaderError::Io(e)) => assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof),
                other => panic!("cut at {cut}: {other:?}"),
            }
        }
    }

    #[test]
    fn oversize_metadata_is_refused_and_writes_nothing() {
        let mut file = Vec::new();
        let err = write_header(&mut file, MAGIC, 1, &vec![b'x'; MAX_PAYLOAD + 1]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(file.is_empty());
        assert!(write_header(&mut file, MAGIC, 1, &vec![b'x'; MAX_PAYLOAD]).is_ok());
    }

    #[test]
    fn empty_and_oversize_payloads_are_refused_and_write_nothing() {
        let mut file = Vec::new();
        let err = write_record(&mut file, 1, &[]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        let err = write_record(&mut file, 1, &vec![0; MAX_PAYLOAD + 1]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(file.is_empty());
        assert_eq!(
            write_record(&mut file, 1, &vec![0; MAX_PAYLOAD]).unwrap(),
            (RECORD_HEADER_BYTES + MAX_PAYLOAD) as u64
        );
    }

    #[test]
    fn a_zero_length_is_corrupt_at_its_offset() {
        let mut file = Vec::new();
        write_record(&mut file, 5, &[1, 2]).unwrap();
        file.extend_from_slice(&6u64.to_le_bytes());
        file.extend_from_slice(&0u16.to_le_bytes());
        let (items, offset) = read_all(&file, 100, MAX_PAYLOAD);
        assert_eq!(items.len(), 2);
        assert!(matches!(items[0], Ok(Item::Record { stamp: 5, .. })));
        assert!(matches!(
            items[1],
            Err(RecordError::CorruptLength { len: 0 })
        ));
        assert_eq!(offset, 100 + 12);
    }

    #[test]
    fn a_length_over_max_payload_is_corrupt() {
        let mut file = Vec::new();
        write_record(&mut file, 5, &[1; 4]).unwrap();
        write_record(&mut file, 6, &[1; 5]).unwrap();
        let (items, offset) = read_all(&file, 0, 4);
        assert_eq!(items.len(), 2);
        assert!(matches!(items[0], Ok(Item::Record { stamp: 5, .. })));
        assert!(matches!(
            items[1],
            Err(RecordError::CorruptLength { len: 5 })
        ));
        assert_eq!(offset, 14);
    }

    #[test]
    fn every_prefix_reads_as_a_prefix_then_nothing_or_a_torn_tail() {
        let Sample {
            file,
            header_len,
            records,
        } = sample();
        let mut boundaries = vec![header_len as usize];
        for (_, payload) in &records {
            let last = *boundaries.last().unwrap();
            boundaries.push(last + RECORD_HEADER_BYTES + payload.len());
        }

        for cut in header_len as usize..=file.len() {
            let body = &file[header_len as usize..cut];
            let (items, offset) = read_all(body, header_len, MAX_PAYLOAD);
            let whole = boundaries.iter().filter(|&&b| b <= cut).count() - 1;
            let on_boundary = boundaries.contains(&cut);

            let expected_len = whole + usize::from(!on_boundary);
            assert_eq!(items.len(), expected_len, "cut at {cut}");
            for (i, item) in items.iter().take(whole).enumerate() {
                match item {
                    Ok(Item::Record { stamp, payload }) => {
                        assert_eq!((*stamp, payload), (records[i].0, &records[i].1));
                    }
                    other => panic!("cut at {cut}, item {i}: {other:?}"),
                }
            }
            if !on_boundary {
                assert!(
                    matches!(items[whole], Ok(Item::TornTail)),
                    "cut at {cut}: {:?}",
                    items[whole]
                );
            }
            assert_eq!(offset, boundaries[whole] as u64, "cut at {cut}");
        }
    }

    #[test]
    fn iteration_stops_after_a_terminal_item() {
        let mut file = Vec::new();
        write_record(&mut file, 1, &[1]).unwrap();
        file.push(0xFF);
        let mut recs = Records::new(&file[..], 0, MAX_PAYLOAD);
        assert!(matches!(recs.next(), Some(Ok(Item::Record { .. }))));
        assert!(matches!(recs.next(), Some(Ok(Item::TornTail))));
        assert!(recs.next().is_none());
        assert!(recs.next().is_none());
    }

    /// A reader that returns `Interrupted` before every successful read.
    struct Interrupting<'a> {
        inner: &'a [u8],
        interrupt: bool,
    }

    impl Read for Interrupting<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.interrupt = !self.interrupt;
            if self.interrupt {
                return Err(io::Error::from(io::ErrorKind::Interrupted));
            }
            let n = buf.len().min(self.inner.len()).min(3);
            buf[..n].copy_from_slice(&self.inner[..n]);
            self.inner = &self.inner[n..];
            Ok(n)
        }
    }

    #[test]
    fn interrupted_reads_are_retried() {
        let mut file = Vec::new();
        write_record(&mut file, 9, &[1, 2, 3, 4, 5, 6, 7]).unwrap();
        let reader = Interrupting {
            inner: &file,
            interrupt: false,
        };
        let items: Vec<_> = Records::new(reader, 0, MAX_PAYLOAD)
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            items,
            vec![Item::Record {
                stamp: 9,
                payload: vec![1, 2, 3, 4, 5, 6, 7]
            }]
        );
    }

    #[test]
    #[should_panic(expected = "max_payload")]
    fn a_zero_max_payload_is_a_programming_error() {
        let _ = Records::new(&[][..], 0, 0);
    }
}
