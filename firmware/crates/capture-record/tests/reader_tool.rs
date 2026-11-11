//! The reader's two outputs, the summary and the WAV export, and the
//! `capture-record` binary's exit status.

use std::path::{Path, PathBuf};
use std::process::Command;

mod common;

use capture_record::{
    Basis, End, EndReason, Event, ExportError, Gap, GapCause, Header, RecordWriter, SampleFormat,
    WAV_HEADER_BYTES, export_wav, summarize,
};
use common::{PERIOD, RATE, START_MONO, START_WALL, header};

/// One period at `RATE`, in microseconds.
const PERIOD_US: u64 = 20_000;

/// Builds a record the way a capture would: blocks stamped on a clock that
/// advances a period per period, gaps and an end at the current position.
struct Rec {
    w: RecordWriter<std::fs::File>,
    frame_bytes: usize,
    pos: u64,
    mono: u64,
    /// `wall_us − mono_us`.
    offset: u64,
    seed: u8,
    /// Every block's PCM in order.
    pcm: Vec<Vec<u8>>,
}

impl Rec {
    fn new(path: &Path, h: &Header) -> Self {
        Rec {
            w: RecordWriter::create(path, h).unwrap(),
            frame_bytes: h.granted.frame_bytes(),
            pos: 0,
            mono: START_MONO,
            offset: START_WALL - START_MONO,
            seed: 1,
            pcm: Vec::new(),
        }
    }

    /// A block of `frames`, read `backlog` frames behind the device.
    fn block(&mut self, frames: u32, backlog: u32) {
        self.mono += u64::from(frames) * 1_000_000 / u64::from(RATE);
        let mono = self.mono + u64::from(backlog) * 1_000_000 / u64::from(RATE);
        let data: Vec<u8> = (0..frames as usize * self.frame_bytes)
            .map(|i| self.seed.wrapping_add(i as u8) | 1)
            .collect();
        self.seed = self.seed.wrapping_add(17);
        self.w
            .block(mono, mono + self.offset, self.pos, backlog, &data)
            .unwrap();
        self.pos += u64::from(frames);
        self.pcm.push(data);
    }

    fn gap(&mut self, cause: GapCause, frames: Option<u64>) {
        let e = Event::Gap(Gap {
            cause,
            at_frame: self.pos,
            frames,
            wall_us: self.mono + self.offset,
            detail: cause.name().to_owned(),
        });
        self.w.event(self.mono, &e).unwrap();
        self.pos += frames.unwrap_or(0);
        if let Some(n) = frames {
            self.mono += n * 1_000_000 / u64::from(RATE);
        }
    }

    /// The clock runs on by `us` with no frame delivered.
    fn pause(&mut self, us: u64) {
        self.mono += us;
    }

    fn end(mut self) -> Vec<Vec<u8>> {
        let e = Event::End(End {
            reason: EndReason::Stopped,
            at_frame: self.pos,
            wall_us: self.mono + self.offset,
            detail: String::new(),
        });
        self.w.event(self.mono, &e).unwrap();
        self.pcm
    }
}

fn tmp() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("r.bpcr");
    (dir, path)
}

#[test]
fn a_clean_record_states_no_gaps_and_agrees_on_both_durations() {
    let (_dir, path) = tmp();
    let h = header(6, SampleFormat::S16Le);
    let mut r = Rec::new(&path, &h);
    for _ in 0..50 {
        r.block(PERIOD, 0);
    }
    r.end();

    let s = summarize(&path).unwrap();
    assert_eq!(s.record_id, h.record_id);
    assert_eq!(s.header, serde_json::to_value(&h).unwrap());
    assert_eq!(s.frames_delivered, 50 * u64::from(PERIOD));
    assert_eq!(s.frames_recorded, 50 * u64::from(PERIOD));
    assert_eq!(s.blocks, 50);
    assert!((s.duration_frames_s - 1.0).abs() < 1e-9);
    assert!((s.duration_clock_s.unwrap() - 1.0).abs() < 1e-9);
    assert!(s.short_blocks.is_empty());
    assert!(s.late.is_empty());
    assert!(s.wall_steps.is_empty());
    let end = s.end.as_ref().unwrap();
    assert_eq!(end.reason, EndReason::Stopped);
    assert_eq!(end.at_frame, 50 * u64::from(PERIOD));
    assert_eq!(s.tail.verdict, "ended");

    let json = serde_json::to_value(&s).unwrap();
    assert_eq!(json["gaps"], serde_json::json!([]));
    assert_eq!(json["end"]["reason"], "stopped");
    assert_eq!(json["tail"]["verdict"], "ended");
}

#[test]
fn the_header_is_reported_as_the_file_holds_it() {
    let (_dir, path) = tmp();
    let h = header(2, SampleFormat::S32Le);
    let mut meta = serde_json::to_value(&h).unwrap();
    meta["added_later"] = serde_json::json!({"k": 1});
    let meta = serde_json::to_vec(&meta).unwrap();
    let mut bytes = Vec::new();
    stamped_log_header(&mut bytes, &meta);
    std::fs::write(&path, &bytes).unwrap();

    let s = summarize(&path).unwrap();
    assert_eq!(s.header["added_later"]["k"], 1);
    assert_eq!(s.tail.verdict, "unterminated");
    assert_eq!(s.duration_clock_s, None);
}

/// A capture record header, written by hand so the metadata can hold a key
/// the `Header` type does not know. The magic, version and length are spelled
/// out rather than taken from the crate, so this states the on-disk bytes
/// independently of the code under test.
fn stamped_log_header(out: &mut Vec<u8>, meta: &[u8]) {
    out.extend_from_slice(b"BPCR");
    out.extend_from_slice(&1u16.to_le_bytes());
    out.extend_from_slice(&(meta.len() as u16).to_le_bytes());
    out.extend_from_slice(meta);
}

#[test]
fn gaps_appear_at_their_positions_with_a_clock_estimate_for_the_unmeasured() {
    let (_dir, path) = tmp();
    let h = header(6, SampleFormat::S16Le);
    let mut r = Rec::new(&path, &h);
    for _ in 0..5 {
        r.block(PERIOD, 0);
    }
    r.gap(GapCause::WriterBacklog, Some(640));
    r.block(PERIOD, 0);
    r.gap(GapCause::Overrun, None);
    // One second of capture the device discarded.
    r.pause(1_000_000);
    r.block(PERIOD, 0);
    r.end();

    let s = summarize(&path).unwrap();
    assert_eq!(s.gaps.len(), 2);
    let measured = &s.gaps[0];
    assert_eq!(measured.cause, GapCause::WriterBacklog);
    assert_eq!(measured.at_frame, 1600);
    assert_eq!(measured.frames, Some(640));
    assert_eq!(measured.clock_estimate_frames, None);
    let overrun = &s.gaps[1];
    assert_eq!(overrun.cause, GapCause::Overrun);
    assert_eq!(overrun.at_frame, 1600 + 640 + 320);
    assert_eq!(overrun.frames, None);
    assert_eq!(overrun.clock_estimate_frames, Some(i64::from(RATE)));
    assert_eq!(overrun.detail, "overrun");
    assert_eq!(s.frames_delivered, 1600 + 640 + 320 + 320);
    assert_eq!(s.frames_recorded, 1600 + 320 + 320);
    assert!(s.wall_steps.is_empty(), "a gap is not a wall step");

    let json = serde_json::to_value(&s).unwrap();
    assert_eq!(json["gaps"][1]["frames"], serde_json::Value::Null);
    assert_eq!(json["gaps"][1]["cause"], "overrun");
}

#[test]
fn an_unmeasured_gap_among_measured_ones_is_estimated_net_of_them() {
    let (_dir, path) = tmp();
    let h = header(2, SampleFormat::S32Le);
    let mut r = Rec::new(&path, &h);
    r.block(PERIOD, 0);
    r.gap(GapCause::WriterBacklog, Some(960));
    r.gap(GapCause::Overrun, None);
    r.pause(500_000);
    r.gap(GapCause::Suspend, None);
    r.gap(GapCause::WriterBacklog, Some(320));
    r.block(PERIOD, 0);
    r.gap(GapCause::TransferError, None);
    r.end();

    let s = summarize(&path).unwrap();
    let est: Vec<Option<i64>> = s.gaps.iter().map(|g| g.clock_estimate_frames).collect();
    // The half second between the blocks, less nothing: the measured gaps'
    // frames advanced the clock by their own length.
    assert_eq!(est, vec![None, Some(8000), Some(0), None, None]);
}

#[test]
fn a_gap_before_the_first_block_is_measured_from_the_start_pair() {
    let (_dir, path) = tmp();
    let h = header(6, SampleFormat::S16Le);
    let mut r = Rec::new(&path, &h);
    r.gap(GapCause::Overrun, None);
    r.pause(100_000);
    r.block(PERIOD, 0);
    r.end();

    // The summary takes the start pair as frame 0's capture time; this
    // record's clock captures frame 0 one frame after it, so the 100 ms
    // estimate counts that frame too.
    let s = summarize(&path).unwrap();
    assert_eq!(s.gaps[0].clock_estimate_frames, Some(1601));
}

#[test]
fn late_runs_and_short_blocks_are_listed() {
    let (_dir, path) = tmp();
    let h = header(6, SampleFormat::S16Le);
    let mut r = Rec::new(&path, &h);
    r.block(PERIOD, 0);
    r.block(PERIOD, PERIOD);
    r.block(PERIOD, 3 * PERIOD);
    r.block(PERIOD, PERIOD - 1);
    r.block(100, 0);
    r.block(PERIOD, 2 * PERIOD);
    r.gap(GapCause::Overrun, None);
    r.block(PERIOD, 2 * PERIOD);
    r.end();

    let s = summarize(&path).unwrap();
    let late: Vec<(u64, u64, u32)> = s
        .late
        .iter()
        .map(|l| (l.first_frame, l.blocks, l.max_backlog_frames))
        .collect();
    assert_eq!(
        late,
        vec![
            (320, 2, 3 * PERIOD),
            (1380, 1, 2 * PERIOD),
            (1700, 1, 2 * PERIOD)
        ]
    );
    let short: Vec<(u64, u64)> = s
        .short_blocks
        .iter()
        .map(|b| (b.first_frame, b.frames))
        .collect();
    assert_eq!(short, vec![(1280, 100)]);
    // The last block's last frame was captured its backlog before its stamp.
    let frames = 6 * 320 + 100;
    assert!((s.duration_clock_s.unwrap() - f64::from(frames) / f64::from(RATE)).abs() < 1e-9);
}

#[test]
fn a_wall_step_is_a_step_and_not_a_gap() {
    let (_dir, path) = tmp();
    let h = header(6, SampleFormat::S16Le);
    let mut r = Rec::new(&path, &h);
    r.block(PERIOD, 0);
    r.block(PERIOD, 0);
    r.offset -= 5_000_000;
    r.block(PERIOD, 0);
    r.offset += 900;
    r.block(PERIOD, 0);
    r.end();

    let s = summarize(&path).unwrap();
    assert!(s.gaps.is_empty());
    assert_eq!(s.wall_steps.len(), 1);
    let step = &s.wall_steps[0];
    assert_eq!(step.at_frame, 640);
    assert_eq!(step.step_us, -5_000_000);
    assert_eq!(step.mono_us, START_MONO + 3 * PERIOD_US);
}

#[test]
fn unterminated_and_torn_tails_are_summarised_without_error() {
    let (_dir, path) = tmp();
    let h = header(6, SampleFormat::S16Le);
    let mut r = Rec::new(&path, &h);
    r.block(PERIOD, 0);
    r.block(PERIOD, 0);
    drop(r);
    let s = summarize(&path).unwrap();
    assert_eq!(s.tail.verdict, "unterminated");
    assert_eq!(s.end, None);
    assert_eq!(s.frames_delivered, 640);

    let len = std::fs::metadata(&path).unwrap().len();
    let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    f.set_len(len - 3).unwrap();
    let s = summarize(&path).unwrap();
    assert_eq!(s.tail.verdict, "torn");
    assert_eq!(s.frames_delivered, 320);
}

/// The fields of a WAV header this crate writes.
struct Wav {
    riff_len: u32,
    format_tag: u16,
    channels: u16,
    rate: u32,
    byte_rate: u32,
    block_align: u16,
    bits: u16,
    valid_bits: u16,
    channel_mask: u32,
    data_len: u32,
    data: Vec<u8>,
}

fn read_wav(path: &Path) -> Wav {
    let b = std::fs::read(path).unwrap();
    let u16_at = |i: usize| u16::from_le_bytes([b[i], b[i + 1]]);
    let u32_at = |i: usize| u32::from_le_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]]);
    assert_eq!(&b[0..4], b"RIFF");
    assert_eq!(&b[8..16], b"WAVEfmt ");
    assert_eq!(u32_at(16), 40);
    assert_eq!(u16_at(36), 22);
    assert_eq!(&b[44..48], &[1, 0, 0, 0], "PCM sub-format");
    assert_eq!(&b[60..64], b"data");
    Wav {
        riff_len: u32_at(4),
        format_tag: u16_at(20),
        channels: u16_at(22),
        rate: u32_at(24),
        byte_rate: u32_at(28),
        block_align: u16_at(32),
        bits: u16_at(34),
        valid_bits: u16_at(38),
        channel_mask: u32_at(40),
        data_len: u32_at(64),
        data: b[WAV_HEADER_BYTES as usize..].to_vec(),
    }
}

fn export_with_holes(channels: u32, sample_format: SampleFormat) {
    let (dir, path) = tmp();
    let h = header(channels, sample_format);
    let fb = h.granted.frame_bytes();
    let mut r = Rec::new(&path, &h);
    r.block(PERIOD, 0);
    r.block(PERIOD, 0);
    r.gap(GapCause::WriterBacklog, Some(640));
    r.block(PERIOD, 0);
    r.gap(GapCause::Overrun, None);
    r.pause(50_000);
    r.block(100, 0);
    r.gap(GapCause::Suspend, None);
    let pcm = r.end();

    let out = dir.path().join("r.wav");
    let map = export_wav(&path, &out).unwrap();

    let wav = read_wav(&out);
    assert_eq!(wav.format_tag, 0xfffe);
    assert_eq!(wav.channels as u32, channels);
    assert_eq!(wav.rate, RATE);
    assert_eq!(wav.bits as usize, sample_format.bytes_per_sample() * 8);
    assert_eq!(wav.valid_bits, wav.bits);
    assert_eq!(wav.block_align as usize, fb);
    assert_eq!(wav.byte_rate as usize, RATE as usize * fb);
    assert_eq!(wav.channel_mask, 0);

    let wav_frames = 3 * 320 + 640 + 800 + 100;
    assert_eq!(map.wav_frames, wav_frames);
    assert_eq!(wav.data_len as usize, wav_frames as usize * fb);
    assert_eq!(wav.riff_len, wav.data_len + 60);
    assert_eq!(wav.data.len(), wav.data_len as usize);

    let mut expect = Vec::new();
    expect.extend_from_slice(&pcm[0]);
    expect.extend_from_slice(&pcm[1]);
    expect.extend(std::iter::repeat_n(0u8, 640 * fb));
    expect.extend_from_slice(&pcm[2]);
    expect.extend(std::iter::repeat_n(0u8, 800 * fb));
    expect.extend_from_slice(&pcm[3]);
    assert_eq!(wav.data, expect);

    let runs: Vec<(u64, u64, u64)> = map
        .runs
        .iter()
        .map(|r| (r.wav_frame, r.first_frame, r.frames))
        .collect();
    assert_eq!(
        runs,
        vec![(0, 0, 640), (1280, 1280, 320), (2400, 1600, 100)]
    );
    let holes: Vec<(u64, u64, u64, Basis)> = map
        .holes
        .iter()
        .map(|h| (h.wav_frame, h.at_frame, h.filled_frames, h.basis))
        .collect();
    assert_eq!(
        holes,
        vec![
            (640, 640, 640, Basis::Measured),
            (1600, 1600, 800, Basis::ClockEstimate),
            (2500, 1700, 0, Basis::Unknown),
        ]
    );
    assert_eq!(map.tail.verdict, "ended");
    let json = serde_json::to_value(&map).unwrap();
    assert_eq!(json["holes"][1]["basis"], "clock_estimate");
}

#[test]
fn export_six_channels_s16_fills_each_hole_and_maps_it() {
    export_with_holes(6, SampleFormat::S16Le);
}

#[test]
fn export_two_channels_s32_fills_each_hole_and_maps_it() {
    export_with_holes(2, SampleFormat::S32Le);
}

#[test]
fn export_refuses_an_existing_output_and_leaves_it_alone() {
    let (dir, path) = tmp();
    let h = header(6, SampleFormat::S16Le);
    let mut r = Rec::new(&path, &h);
    r.block(PERIOD, 0);
    r.end();
    let out = dir.path().join("r.wav");
    std::fs::write(&out, b"mine").unwrap();
    match export_wav(&path, &out) {
        Err(ExportError::Output(e)) => assert_eq!(e.kind(), std::io::ErrorKind::AlreadyExists),
        other => panic!("expected AlreadyExists, got {other:?}"),
    }
    assert_eq!(std::fs::read(&out).unwrap(), b"mine");
}

#[test]
fn export_refuses_a_record_past_the_riff_limit_and_creates_nothing() {
    let (dir, path) = tmp();
    let h = header(6, SampleFormat::S16Le);
    let mut r = Rec::new(&path, &h);
    r.block(PERIOD, 0);
    // 12-byte frames: this many frames of silence pass 4 GiB.
    r.gap(GapCause::WriterBacklog, Some(400_000_000));
    r.block(PERIOD, 0);
    r.end();
    let out = dir.path().join("r.wav");
    match export_wav(&path, &out) {
        Err(e @ ExportError::TooLarge { .. }) => assert!(e.to_string().contains("4 GiB")),
        other => panic!("expected TooLarge, got {other:?}"),
    }
    assert!(!out.exists());
}

#[test]
fn export_refuses_a_byte_rate_no_wav_header_holds_and_creates_nothing() {
    let (dir, path) = tmp();
    let mut h = header(6, SampleFormat::S16Le);
    h.granted.rate_hz = u32::MAX;
    let mut r = Rec::new(&path, &h);
    r.block(PERIOD, 0);
    r.end();
    let out = dir.path().join("r.wav");
    match export_wav(&path, &out) {
        Err(ExportError::ByteRate { byte_rate }) => {
            assert_eq!(byte_rate, u64::from(u32::MAX) * 12);
        }
        other => panic!("expected ByteRate, got {other:?}"),
    }
    assert!(!out.exists());
}

fn tool(args: &[&std::ffi::OsStr]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_capture-record"))
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn the_binary_exits_0_when_it_produced_its_output_whatever_the_tail() {
    let (dir, path) = tmp();
    let h = header(6, SampleFormat::S16Le);
    let mut r = Rec::new(&path, &h);
    r.block(PERIOD, 0);
    r.gap(GapCause::Overrun, None);
    drop(r);

    let out = tool(&["summary".as_ref(), path.as_os_str()]);
    assert_eq!(out.status.code(), Some(0));
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["record_id"], h.record_id);
    assert_eq!(json["tail"]["verdict"], "unterminated");
    assert_eq!(json["gaps"][0]["cause"], "overrun");
    assert_eq!(json["header"]["layout"][5], "ch5");

    let wav = dir.path().join("r.wav");
    let out = tool(&["export-wav".as_ref(), path.as_os_str(), wav.as_os_str()]);
    assert_eq!(out.status.code(), Some(0));
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["wav_frames"], 320);
    assert_eq!(json["holes"][0]["basis"], "unknown");
    assert!(wav.exists());
}

#[test]
fn the_binary_exits_1_on_an_unreadable_header_or_failed_io() {
    let (dir, path) = tmp();
    std::fs::write(&path, b"RSFL\x01\x00\x00\x00").unwrap();
    let out = tool(&["summary".as_ref(), path.as_os_str()]);
    assert_eq!(out.status.code(), Some(1));
    assert!(out.stdout.is_empty());
    assert!(String::from_utf8_lossy(&out.stderr).contains("bad magic"));

    let missing = dir.path().join("missing.bpcr");
    let out = tool(&["summary".as_ref(), missing.as_os_str()]);
    assert_eq!(out.status.code(), Some(1));

    let rec = dir.path().join("ok.bpcr");
    let mut r = Rec::new(&rec, &header(6, SampleFormat::S16Le));
    r.block(PERIOD, 0);
    r.end();
    let wav = dir.path().join("taken.wav");
    std::fs::write(&wav, b"x").unwrap();
    let out = tool(&["export-wav".as_ref(), rec.as_os_str(), wav.as_os_str()]);
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(std::fs::read(&wav).unwrap(), b"x");
}

#[test]
fn the_binary_leaves_no_wav_when_its_map_cannot_be_written() {
    let (dir, path) = tmp();
    let mut r = Rec::new(&path, &header(6, SampleFormat::S16Le));
    r.block(PERIOD, 0);
    r.end();
    let wav = dir.path().join("r.wav");
    let full = std::fs::File::options()
        .write(true)
        .open("/dev/full")
        .unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_capture-record"))
        .args(["export-wav".as_ref(), path.as_os_str(), wav.as_os_str()])
        .stdout(full)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("writing stdout"), "{stderr}");
    assert!(stderr.contains("removed"), "{stderr}");
    assert!(!wav.exists());

    let out = tool(&["export-wav".as_ref(), path.as_os_str(), wav.as_os_str()]);
    assert_eq!(out.status.code(), Some(0));
    assert!(wav.exists());
}

#[test]
fn the_binary_exits_2_on_a_command_line_it_does_not_understand() {
    for args in [
        &[][..],
        &["summary"][..],
        &["summary", "a", "b"][..],
        &["export-wav", "a"][..],
        &["export", "a", "b"][..],
        &["--help"][..],
    ] {
        let args: Vec<&std::ffi::OsStr> = args.iter().map(|a| a.as_ref()).collect();
        let out = tool(&args);
        assert_eq!(out.status.code(), Some(2), "{args:?}");
        assert!(out.stdout.is_empty());
        assert!(String::from_utf8_lossy(&out.stderr).contains("usage"));
    }
}
