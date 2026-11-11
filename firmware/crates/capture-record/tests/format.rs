//! The capture record format in both directions: what the writer writes, the
//! reader reads back, and every way a file can end.

use std::cell::Cell;
use std::io::{self, Cursor, Read, Write};
use std::rc::Rc;

mod common;

use capture_record::{
    BuildSource, CorruptReason, End, EndReason, Event, Gap, GapCause, Header, Item,
    MAX_BLOCK_PCM_BYTES, OpenError, RecordReader, RecordWriter, SampleFormat, Tail, Truncate,
    Verdict,
};
use common::header;

fn frame_bytes(h: &Header) -> usize {
    h.granted.frame_bytes()
}

/// PCM of `frames` frames whose bytes count up from `seed`.
fn pcm(h: &Header, frames: usize, seed: u8) -> Vec<u8> {
    (0..frames * frame_bytes(h))
        .map(|i| seed.wrapping_add(i as u8))
        .collect()
}

fn gap(cause: GapCause, at_frame: u64, frames: Option<u64>) -> Event {
    Event::Gap(Gap {
        cause,
        at_frame,
        frames,
        wall_us: 5,
        detail: format!("{} here", cause.name()),
    })
}

fn end(reason: EndReason, at_frame: u64) -> Event {
    Event::End(End {
        reason,
        at_frame,
        wall_us: 9,
        detail: String::new(),
    })
}

fn writer(h: &Header) -> RecordWriter<Vec<u8>> {
    RecordWriter::new(Vec::new(), h).expect("header writes")
}

/// Every item and the tail of a record held in memory.
fn read_all(bytes: &[u8]) -> (Vec<Item>, Tail) {
    let mut r = RecordReader::from_reader(Cursor::new(bytes)).expect("header reads");
    let items: Vec<Item> = r.by_ref().map(|i| i.expect("no I/O error")).collect();
    let tail = r.tail().expect("a verdict").clone();
    (items, tail)
}

/// A record with two blocks, a gap of every cause and an end, and the items
/// it should read back as.
fn full_record(h: &Header) -> (Vec<u8>, Vec<Item>) {
    let mut w = writer(h);
    let mut expect = Vec::new();
    let mut pos = 0u64;
    let mut stamp = 10u64;
    let mut block = |w: &mut RecordWriter<Vec<u8>>, pos: &mut u64, frames: usize, backlog| {
        let data = pcm(h, frames, *pos as u8);
        stamp += 20_000;
        w.block(stamp, stamp + 7, *pos, backlog, &data).unwrap();
        expect.push(Item::Block {
            mono_us: stamp,
            wall_us: stamp + 7,
            first_frame: *pos,
            backlog_frames: backlog,
            frames: frames as u64,
            pcm: data,
        });
        *pos += frames as u64;
    };
    block(&mut w, &mut pos, 320, 0);
    block(&mut w, &mut pos, 100, 400);
    let mut events = Vec::new();
    let backlog = gap(GapCause::WriterBacklog, pos, Some(640));
    w.event(100, &backlog).unwrap();
    events.push((100, backlog));
    pos += 640;
    for cause in [
        GapCause::Overrun,
        GapCause::Suspend,
        GapCause::TransferError,
    ] {
        let g = gap(cause, pos, None);
        w.event(200, &g).unwrap();
        events.push((200, g));
    }
    let mut items: Vec<Item> = std::mem::take(&mut expect);
    for (mono_us, e) in events {
        match e {
            Event::Gap(gap) => items.push(Item::Gap { mono_us, gap }),
            Event::End(_) => unreachable!(),
        }
    }
    let data = pcm(h, 320, 3);
    w.block(500_000, 500_007, pos, 0, &data).unwrap();
    items.push(Item::Block {
        mono_us: 500_000,
        wall_us: 500_007,
        first_frame: pos,
        backlog_frames: 0,
        frames: 320,
        pcm: data,
    });
    pos += 320;
    let e = end(EndReason::Stopped, pos);
    w.event(600_000, &e).unwrap();
    let Event::End(end) = e else { unreachable!() };
    items.push(Item::End {
        mono_us: 600_000,
        end,
    });
    (w.get_ref().clone(), items)
}

#[test]
fn round_trip_six_channels_s16() {
    let h = header(6, SampleFormat::S16Le);
    let (bytes, expect) = full_record(&h);
    let mut r = RecordReader::from_reader(Cursor::new(&bytes)).unwrap();
    assert_eq!(r.header(), &h);
    let items: Vec<Item> = r.by_ref().map(Result::unwrap).collect();
    assert_eq!(items, expect);
    assert_eq!(
        r.tail(),
        Some(&Tail {
            verdict: Verdict::Ended,
            offset: bytes.len() as u64
        })
    );
    assert_eq!(r.position(), 320 + 100 + 640 + 320);
}

#[test]
fn round_trip_two_channels_s32() {
    let h = header(2, SampleFormat::S32Le);
    let (bytes, expect) = full_record(&h);
    let (items, tail) = read_all(&bytes);
    assert_eq!(items, expect);
    assert_eq!(tail.verdict, Verdict::Ended);
}

#[test]
fn create_writes_a_file_open_reads_and_an_existing_path_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("r.bpcr");
    let h = header(6, SampleFormat::S16Le);
    let mut w = RecordWriter::create(&path, &h).unwrap();
    w.block(1, 2, 0, 0, &pcm(&h, 10, 0)).unwrap();
    w.event(3, &end(EndReason::Stopped, 10)).unwrap();
    assert_eq!(w.offset(), std::fs::metadata(&path).unwrap().len());
    drop(w);

    let before = std::fs::read(&path).unwrap();
    let err = RecordWriter::create(&path, &h).err().expect("refused");
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(std::fs::read(&path).unwrap(), before);

    let mut r = RecordReader::open(&path).unwrap();
    assert_eq!(r.by_ref().count(), 2);
    assert_eq!(r.tail().unwrap().verdict, Verdict::Ended);
}

#[test]
fn block_refuses_empty_partial_and_oversize_pcm_and_takes_the_largest() {
    let h = header(6, SampleFormat::S16Le);
    let fb = frame_bytes(&h);
    let largest = MAX_BLOCK_PCM_BYTES / fb;
    assert_eq!(h.granted.max_block_frames(), largest);
    let mut w = writer(&h);
    let start = w.get_ref().len();
    for bad in [Vec::new(), vec![0u8; fb + 1], pcm(&h, largest + 1, 0)] {
        let err = w.block(1, 1, 0, 0, &bad).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(w.get_ref().len(), start, "nothing written");
    }
    w.block(1, 1, 0, 0, &pcm(&h, largest, 0)).unwrap();
    let (items, tail) = read_all(w.get_ref());
    assert!(matches!(&items[..], [Item::Block { frames, .. }] if *frames == largest as u64));
    assert_eq!(tail.verdict, Verdict::Unterminated);
}

#[test]
fn event_refuses_a_gap_whose_length_contradicts_its_cause() {
    let h = header(6, SampleFormat::S16Le);
    let mut w = writer(&h);
    let start = w.get_ref().len();
    for bad in [
        gap(GapCause::WriterBacklog, 0, None),
        gap(GapCause::WriterBacklog, 0, Some(0)),
        gap(GapCause::Overrun, 0, Some(5)),
    ] {
        let err = w.event(1, &bad).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }
    assert_eq!(w.get_ref().len(), start);
}

#[test]
fn writer_refuses_a_header_that_breaks_the_rules() {
    let good = header(6, SampleFormat::S16Le);
    let mut cases = Vec::new();
    let mut h = good.clone();
    h.layout.pop();
    cases.push(h);
    let mut h = good.clone();
    h.record_id = "XYZ".to_owned();
    cases.push(h);
    let mut h = good.clone();
    h.writer.build.dirty = None;
    cases.push(h);
    let mut h = good.clone();
    h.writer.build.source = BuildSource::Declared;
    cases.push(h);
    let mut h = good.clone();
    h.clocks.unit = "ms".to_owned();
    cases.push(h);
    let mut h = good.clone();
    h.granted.period_frames = 0;
    cases.push(h);
    for h in cases {
        let err = RecordWriter::new(Vec::new(), &h).err().expect("refused");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }
    let mut h = good;
    h.writer.build.source = BuildSource::Declared;
    h.writer.build.dirty = None;
    h.device.usb_id = None;
    RecordWriter::new(Vec::new(), &h).expect("a declared build without a USB id is valid");
}

/// A header followed by nothing, and the item count and tail of a record cut
/// at every byte inside its last record.
#[test]
fn unterminated_and_torn_at_every_offset_inside_the_last_record() {
    let h = header(6, SampleFormat::S16Le);
    let mut w = writer(&h);
    let header_end = w.offset();
    w.block(1, 1, 0, 0, &pcm(&h, 320, 0)).unwrap();
    let boundary = w.offset();
    w.block(2, 2, 320, 0, &pcm(&h, 320, 1)).unwrap();
    let bytes = w.get_ref().clone();

    let (items, tail) = read_all(&bytes[..header_end as usize]);
    assert!(items.is_empty());
    assert_eq!(
        tail,
        Tail {
            verdict: Verdict::Unterminated,
            offset: header_end
        }
    );

    let (items, tail) = read_all(&bytes);
    assert_eq!(items.len(), 2);
    assert_eq!(tail.verdict, Verdict::Unterminated);
    assert_eq!(tail.offset, bytes.len() as u64);

    for cut in boundary as usize + 1..bytes.len() {
        let (items, tail) = read_all(&bytes[..cut]);
        assert_eq!(items.len(), 1, "cut at {cut}");
        assert_eq!(
            tail,
            Tail {
                verdict: Verdict::Torn,
                offset: boundary
            },
            "cut at {cut}"
        );
    }
}

/// A record built by `build`, then `raw` bytes, and the offset `raw` starts at.
fn with_raw(
    h: &Header,
    build: impl FnOnce(&mut RecordWriter<Vec<u8>>),
    raw: &[u8],
) -> (Vec<u8>, u64) {
    let mut w = writer(h);
    build(&mut w);
    let offset = w.offset();
    let mut bytes = w.get_ref().clone();
    bytes.extend_from_slice(raw);
    (bytes, offset)
}

fn raw_record(stamp: u64, payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::new();
    stamped_log::write_record(&mut v, stamp, payload).unwrap();
    v
}

fn raw_block(first_frame: u64, pcm: &[u8]) -> Vec<u8> {
    let mut p = vec![capture_record::KIND_BLOCK];
    p.extend_from_slice(&0u64.to_le_bytes());
    p.extend_from_slice(&first_frame.to_le_bytes());
    p.extend_from_slice(&0u32.to_le_bytes());
    p.extend_from_slice(pcm);
    raw_record(1, &p)
}

fn raw_event(json: &str) -> Vec<u8> {
    let mut p = vec![capture_record::KIND_EVENT];
    p.extend_from_slice(json.as_bytes());
    raw_record(1, &p)
}

#[test]
fn corrupt_for_each_reason_at_the_offending_record() {
    let h = header(6, SampleFormat::S16Le);
    let fb = frame_bytes(&h);
    let one_block = |w: &mut RecordWriter<Vec<u8>>| {
        w.block(1, 1, 0, 0, &pcm(&h, 10, 0)).unwrap();
    };
    let mut zero_len = 7u64.to_le_bytes().to_vec();
    zero_len.extend_from_slice(&0u16.to_le_bytes());
    let cases: Vec<(Vec<u8>, CorruptReason)> = vec![
        (zero_len, CorruptReason::Length { len: 0 }),
        (
            raw_record(1, &[0x03, 0]),
            CorruptReason::UnknownKind { kind: 3 },
        ),
        (
            raw_record(1, &[capture_record::KIND_BLOCK, 1, 2]),
            CorruptReason::ShortBlock { len: 3 },
        ),
        (raw_block(10, &[]), CorruptReason::EmptyBlock),
        (
            raw_block(10, &vec![0; fb + 2]),
            CorruptReason::PartialFrame {
                bytes: fb + 2,
                frame_bytes: fb,
            },
        ),
        (
            raw_block(11, &vec![0; fb]),
            CorruptReason::Position {
                expected: 10,
                found: 11,
            },
        ),
        (
            raw_event(
                r#"{"event":"gap","cause":"overrun","at_frame":9,"frames":null,"wall_us":1,"detail":""}"#,
            ),
            CorruptReason::Position {
                expected: 10,
                found: 9,
            },
        ),
        (
            raw_event(r#"{"event":"marker","at_frame":10,"wall_us":1}"#),
            CorruptReason::UnknownEvent {
                name: "marker".to_owned(),
            },
        ),
    ];
    for (raw, reason) in cases {
        let (bytes, offset) = with_raw(&h, one_block, &raw);
        let (items, tail) = read_all(&bytes);
        assert_eq!(items.len(), 1, "{reason}");
        assert_eq!(
            tail,
            Tail {
                verdict: Verdict::Corrupt(reason),
                offset
            }
        );
    }

    for json in [
        "not json",
        r#"{"at_frame":10}"#,
        r#"{"event":"gap","cause":"hiccup","at_frame":10,"frames":null,"wall_us":1,"detail":""}"#,
        r#"{"event":"gap","cause":"writer_backlog","at_frame":10,"frames":null,"wall_us":1,"detail":""}"#,
        r#"{"event":"end","reason":"bored","at_frame":10,"wall_us":1,"detail":""}"#,
        r#"{"event":"end","at_frame":10,"wall_us":1,"detail":""}"#,
    ] {
        let (bytes, offset) = with_raw(&h, one_block, &raw_event(json));
        let (items, tail) = read_all(&bytes);
        assert_eq!(items.len(), 1, "{json}");
        assert_eq!(tail.offset, offset, "{json}");
        assert!(
            matches!(
                tail.verdict,
                Verdict::Corrupt(CorruptReason::BadEvent { .. })
            ),
            "{json}: {tail:?}"
        );
    }
}

#[test]
fn anything_after_an_end_record_is_corrupt() {
    let h = header(6, SampleFormat::S16Le);
    let ended = |w: &mut RecordWriter<Vec<u8>>| {
        w.event(1, &end(EndReason::Stopped, 0)).unwrap();
    };
    for raw in [raw_block(0, &pcm(&h, 1, 0)), vec![1, 2, 3]] {
        let (bytes, offset) = with_raw(&h, ended, &raw);
        let (items, tail) = read_all(&bytes);
        assert!(matches!(&items[..], [Item::End { .. }]));
        assert_eq!(
            tail,
            Tail {
                verdict: Verdict::Corrupt(CorruptReason::AfterEnd),
                offset
            }
        );
    }
}

#[test]
fn a_measured_gap_advances_the_position_and_an_unmeasured_one_does_not() {
    let h = header(2, SampleFormat::S16Le);
    let mut w = writer(&h);
    w.block(1, 1, 0, 0, &pcm(&h, 10, 0)).unwrap();
    w.event(2, &gap(GapCause::Overrun, 10, None)).unwrap();
    w.block(3, 3, 10, 0, &pcm(&h, 10, 0)).unwrap();
    w.event(4, &gap(GapCause::WriterBacklog, 20, Some(30)))
        .unwrap();
    w.block(5, 5, 50, 0, &pcm(&h, 10, 0)).unwrap();
    let (items, tail) = read_all(w.get_ref());
    assert_eq!(items.len(), 5);
    assert_eq!(tail.verdict, Verdict::Unterminated);
}

/// Header bytes for `meta` under `version`.
fn raw_header(version: u16, meta: &serde_json::Value) -> Vec<u8> {
    let mut v = Vec::new();
    stamped_log::write_header(
        &mut v,
        capture_record::MAGIC,
        version,
        &serde_json::to_vec(meta).unwrap(),
    )
    .unwrap();
    v
}

#[test]
fn another_version_bad_magic_and_unknown_sample_format_are_refused() {
    let h = header(6, SampleFormat::S16Le);
    let meta = serde_json::to_value(&h).unwrap();

    let bytes = raw_header(2, &meta);
    assert!(matches!(
        RecordReader::from_reader(Cursor::new(bytes)),
        Err(OpenError::UnsupportedVersion(2))
    ));

    let mut bytes = raw_header(1, &meta);
    bytes[..4].copy_from_slice(b"RSFL");
    assert!(matches!(
        RecordReader::from_reader(Cursor::new(bytes)),
        Err(OpenError::BadMagic)
    ));

    let mut odd = meta.clone();
    odd["granted"]["sample_format"] = "s24_le".into();
    assert!(matches!(
        RecordReader::from_reader(Cursor::new(raw_header(1, &odd))),
        Err(OpenError::Metadata(_))
    ));

    let mut short = meta.clone();
    short["layout"] = serde_json::json!(["only_one"]);
    assert!(matches!(
        RecordReader::from_reader(Cursor::new(raw_header(1, &short))),
        Err(OpenError::Metadata(_))
    ));

    let bytes = raw_header(1, &meta);
    assert!(matches!(
        RecordReader::from_reader(Cursor::new(&bytes[..bytes.len() - 1])),
        Err(OpenError::Io(_))
    ));
}

#[test]
fn unknown_keys_are_ignored_in_the_header_and_in_events() {
    let h = header(6, SampleFormat::S16Le);
    let mut meta = serde_json::to_value(&h).unwrap();
    meta["added_later"] = serde_json::json!({"x": 1});
    meta["device"]["firmware"] = "2.0".into();
    let mut bytes = raw_header(1, &meta);
    bytes.extend(raw_event(
        r#"{"event":"gap","cause":"overrun","at_frame":0,"frames":null,"wall_us":1,"detail":"x","extra":true}"#,
    ));
    let mut r = RecordReader::from_reader(Cursor::new(bytes)).unwrap();
    assert_eq!(r.header(), &h);
    let items: Vec<Item> = r.by_ref().map(Result::unwrap).collect();
    assert!(matches!(
        &items[..],
        [Item::Gap {
            gap: Gap {
                cause: GapCause::Overrun,
                frames: None,
                ..
            },
            ..
        }]
    ));
    assert_eq!(r.tail().unwrap().verdict, Verdict::Unterminated);
}

#[test]
fn the_header_states_what_the_spec_names() {
    let h = header(6, SampleFormat::S16Le);
    let v = serde_json::to_value(&h).unwrap();
    assert_eq!(v["granted"]["sample_format"], "s16_le");
    assert_eq!(v["writer"]["build"]["source"], "git");
    assert_eq!(v["clocks"]["mono"], "CLOCK_MONOTONIC");
    assert_eq!(v["clocks"]["wall"], "CLOCK_REALTIME");
    assert_eq!(v["clocks"]["unit"], "us");
    assert_eq!(v["device_control"]["performed"], false);
    let e = serde_json::to_value(gap(GapCause::WriterBacklog, 3, Some(4))).unwrap();
    assert_eq!(e["event"], "gap");
    assert_eq!(e["cause"], "writer_backlog");
    let e = serde_json::to_value(end(EndReason::DestinationFull, 3)).unwrap();
    assert_eq!(e["event"], "end");
    assert_eq!(e["reason"], "destination_full");
}

#[test]
fn a_block_or_gap_that_carries_the_position_past_its_limit_is_corrupt() {
    let h = header(6, SampleFormat::S16Le);
    let fb = frame_bytes(&h);
    let near_max = u64::MAX - 5;
    let (bytes, offset) = with_raw(
        &h,
        |w| {
            w.block(1, 1, 0, 0, &pcm(&h, 10, 0)).unwrap();
            w.event(1, &gap(GapCause::WriterBacklog, 10, Some(near_max - 10)))
                .unwrap();
        },
        &raw_block(near_max, &vec![0; 10 * fb]),
    );
    let (items, tail) = read_all(&bytes);
    assert_eq!(items.len(), 2);
    assert_eq!(
        tail,
        Tail {
            verdict: Verdict::Corrupt(CorruptReason::PositionOverflow {
                position: near_max,
                frames: 10,
            }),
            offset
        }
    );

    let (bytes, offset) = with_raw(
        &h,
        |w| w.block(1, 1, 0, 0, &pcm(&h, 10, 0)).unwrap(),
        &raw_event(&format!(
            r#"{{"event":"gap","cause":"writer_backlog","at_frame":10,"frames":{},"wall_us":1,"detail":""}}"#,
            u64::MAX
        )),
    );
    let (items, tail) = read_all(&bytes);
    assert_eq!(items.len(), 1);
    assert_eq!(
        tail,
        Tail {
            verdict: Verdict::Corrupt(CorruptReason::PositionOverflow {
                position: 10,
                frames: u64::MAX,
            }),
            offset
        }
    );
}

/// A destination that takes `room` more bytes and then refuses every write,
/// and whose truncation can be made to fail.
struct Refusing {
    bytes: Vec<u8>,
    pos: usize,
    room: Rc<Cell<usize>>,
    truncate_ok: Rc<Cell<bool>>,
}

impl Write for Refusing {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = buf.len().min(self.room.get());
        if n == 0 {
            return Err(io::Error::other("refused"));
        }
        let end = self.pos + n;
        if self.bytes.len() < end {
            self.bytes.resize(end, 0);
        }
        self.bytes[self.pos..end].copy_from_slice(&buf[..n]);
        self.pos = end;
        self.room.set(self.room.get() - n);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Truncate for Refusing {
    fn truncate(&mut self, len: u64) -> io::Result<()> {
        if !self.truncate_ok.get() {
            return Err(io::Error::other("truncate refused"));
        }
        self.bytes.truncate(len as usize);
        self.pos = len as usize;
        Ok(())
    }
}

/// A writer on a [`Refusing`] destination that has written the header and one
/// block, then a second block torn partway by a refused write.
fn torn_by_a_refused_write(
    h: &Header,
) -> (RecordWriter<Refusing>, Rc<Cell<usize>>, Rc<Cell<bool>>) {
    let room = Rc::new(Cell::new(usize::MAX));
    let truncate_ok = Rc::new(Cell::new(true));
    let dest = Refusing {
        bytes: Vec::new(),
        pos: 0,
        room: Rc::clone(&room),
        truncate_ok: Rc::clone(&truncate_ok),
    };
    let mut w = RecordWriter::new(dest, h).unwrap();
    w.block(1, 1, 0, 0, &pcm(h, 10, 0)).unwrap();
    let whole = w.offset();
    room.set(15);
    w.block(2, 2, 10, 0, &pcm(h, 10, 1)).unwrap_err();
    assert_eq!(w.get_ref().bytes.len() as u64, whole + 15, "a torn record");
    assert_eq!(w.offset(), whole);
    room.set(usize::MAX);
    (w, room, truncate_ok)
}

#[test]
fn after_a_failed_write_the_writer_refuses_until_cut_back_then_ends_cleanly() {
    let h = header(6, SampleFormat::S16Le);
    let (mut w, _room, _truncate_ok) = torn_by_a_refused_write(&h);
    let torn_len = w.get_ref().bytes.len();
    w.event(3, &end(EndReason::WriteFailed, 10)).unwrap_err();
    w.block(3, 3, 10, 0, &pcm(&h, 10, 2)).unwrap_err();
    assert_eq!(w.get_ref().bytes.len(), torn_len, "nothing written");

    w.truncate_to_last_record().unwrap();
    w.event(3, &end(EndReason::WriteFailed, 10)).unwrap();
    let (items, tail) = read_all(&w.get_ref().bytes);
    assert!(
        matches!(&items[..], [Item::Block { .. }, Item::End { end, .. }] if end.reason == EndReason::WriteFailed),
        "{items:?}"
    );
    assert_eq!(tail.verdict, Verdict::Ended);
}

#[test]
fn a_writer_whose_truncation_failed_still_refuses() {
    let h = header(6, SampleFormat::S16Le);
    let (mut w, _room, truncate_ok) = torn_by_a_refused_write(&h);
    truncate_ok.set(false);
    w.truncate_to_last_record().unwrap_err();
    let torn_len = w.get_ref().bytes.len();
    w.event(3, &end(EndReason::WriteFailed, 10)).unwrap_err();
    assert_eq!(w.get_ref().bytes.len(), torn_len, "nothing written");
    let (items, tail) = read_all(&w.get_ref().bytes);
    assert_eq!(items.len(), 1);
    assert_eq!(tail.verdict, Verdict::Torn);
}

#[test]
fn an_event_too_large_for_a_record_is_refused_and_does_not_fail_the_writer() {
    let h = header(6, SampleFormat::S16Le);
    let mut w = writer(&h);
    let start = w.get_ref().len();
    let mut big = end(EndReason::WriteFailed, 0);
    if let Event::End(e) = &mut big {
        e.detail = "x".repeat(stamped_log::MAX_PAYLOAD);
    }
    let err = w.event(1, &big).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(w.get_ref().len(), start, "nothing written");
    w.event(1, &end(EndReason::Stopped, 0)).unwrap();
}

#[test]
fn a_file_cut_back_takes_its_next_write_at_the_cut() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cut");
    let mut f = std::fs::File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    f.write_all(b"abcdef").unwrap();
    f.truncate(3).unwrap();
    f.write_all(b"XY").unwrap();
    let mut back = Vec::new();
    std::fs::File::open(&path)
        .unwrap()
        .read_to_end(&mut back)
        .unwrap();
    assert_eq!(back, b"abcXY");
}
