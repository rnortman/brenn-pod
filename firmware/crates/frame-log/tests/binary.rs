//! The `frame-log` binary's outputs and exit statuses.

mod common;

use std::path::Path;
use std::process::{Command, Output};

use audio_pipeline::wide::StreamEndReason;
use common::{Cap, Log, PERIOD, start};

fn frame_log(args: &[&dyn AsRef<std::ffi::OsStr>]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_frame-log"))
        .args(args.iter().map(|a| a.as_ref()))
        .output()
        .unwrap()
}

fn json(out: &Output) -> serde_json::Value {
    let stdout = std::str::from_utf8(&out.stdout).unwrap();
    assert_eq!(stdout.lines().count(), 1, "{stdout}");
    serde_json::from_str(stdout).unwrap()
}

fn capture() -> Log {
    let mut cap = Cap::new(2, 16);
    cap.block(PERIOD, 0);
    cap.end(StreamEndReason::Stopped)
}

#[test]
fn summary_prints_one_json_object_and_exits_0() {
    let log = capture();
    let out = frame_log(&[&"summary", &log.path()]);
    assert_eq!(out.status.code(), Some(0));
    let s = json(&out);
    assert_eq!(s["tail"]["verdict"], "ended");
    assert_eq!(s["hello"]["channels"], 2);
    assert_eq!(
        s["capture"]["channel_names"],
        serde_json::json!(["ch0", "ch1"])
    );
}

#[test]
fn a_torn_log_still_exits_0() {
    let mut log = capture();
    let len = log.bytes.len();
    log.bytes.truncate(len - 3);
    let out = frame_log(&[&"summary", &log.path()]);
    assert_eq!(out.status.code(), Some(0));
    assert_eq!(json(&out)["tail"]["verdict"], "torn");
}

#[test]
fn export_wav_writes_the_file_and_prints_its_map() {
    let log = capture();
    let wav = log.dir.path().join("out.wav");
    let out = frame_log(&[&"export-wav", &log.path(), &wav]);
    assert_eq!(out.status.code(), Some(0));
    let map = json(&out);
    assert_eq!(map["wav_frames"], 320);
    assert_eq!(map["holes"], serde_json::json!([]));
    assert_eq!(map["resent"], serde_json::json!([]));
    assert_eq!(map["tail"]["verdict"], "ended");
    assert_eq!(std::fs::metadata(&wav).unwrap().len(), 68 + 320 * 4);
}

fn fails(args: &[&dyn AsRef<std::ffi::OsStr>]) -> String {
    let out = frame_log(args);
    assert_eq!(out.status.code(), Some(1));
    assert!(out.stdout.is_empty());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.starts_with("frame-log: "), "{stderr}");
    stderr
}

#[test]
fn an_unreadable_log_exits_1() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing.framelog");
    fails(&[&"summary", &missing]);
    fails(&[&"export-wav", &missing, &dir.path().join("out.wav")]);
    let garbage = dir.path().join("garbage.framelog");
    std::fs::write(&garbage, b"not a frame log").unwrap();
    fails(&[&"summary", &garbage]);
}

#[test]
fn a_log_with_no_hello_exits_1() {
    let mut log = Log::new();
    log.frame(0, start(0, 0, 0));
    let stderr = fails(&[&"summary", &log.path()]);
    assert!(stderr.contains("no hello"), "{stderr}");
}

#[test]
fn an_existing_output_exits_1_and_is_untouched() {
    let log = capture();
    let wav = log.dir.path().join("out.wav");
    std::fs::write(&wav, b"mine").unwrap();
    fails(&[&"export-wav", &log.path(), &wav]);
    assert_eq!(std::fs::read(&wav).unwrap(), b"mine");
}

const CHANNELS: usize = 3;
const IN_FRAMES: usize = 100;

/// Sample `ch` of WAV frame `i` in the input: distinct for every pair.
fn sample(i: usize, ch: usize) -> i16 {
    (i * CHANNELS + ch + 1) as i16
}

/// A plain 16-bit PCM WAV of three channels at 1 kHz, `IN_FRAMES` frames.
fn input_wav() -> Vec<u8> {
    let data: Vec<u8> = (0..IN_FRAMES)
        .flat_map(|i| (0..CHANNELS).map(move |ch| sample(i, ch)))
        .flat_map(i16::to_le_bytes)
        .collect();
    let mut w = Vec::new();
    w.extend_from_slice(b"RIFF");
    w.extend_from_slice(&(36 + data.len() as u32).to_le_bytes());
    w.extend_from_slice(b"WAVEfmt ");
    w.extend_from_slice(&16u32.to_le_bytes());
    w.extend_from_slice(&1u16.to_le_bytes());
    w.extend_from_slice(&(CHANNELS as u16).to_le_bytes());
    w.extend_from_slice(&1000u32.to_le_bytes());
    w.extend_from_slice(&(1000 * 2 * CHANNELS as u32).to_le_bytes());
    w.extend_from_slice(&(2 * CHANNELS as u16).to_le_bytes());
    w.extend_from_slice(&16u16.to_le_bytes());
    w.extend_from_slice(b"data");
    w.extend_from_slice(&(data.len() as u32).to_le_bytes());
    w.extend_from_slice(&data);
    w
}

fn write_spec(end: bool) -> serde_json::Value {
    let mut spec = serde_json::json!({
        "pod_id": "synthetic",
        "layout": ["a", "b", "c"],
        "period_frames": 16,
        "buffer_frames": 256,
        "start": {"mono_us": 2_000_000, "wall_us": 1_700_000_000_000_000u64},
        "record_id": "00112233445566778899aabbccddeeff",
        "commit": "fedcba9876543210fedcba9876543210fedcba98",
        "device": {"selector": "synthetic", "card_index": 7, "card_name": "Synth",
                   "card_long_name": "Synthetic card", "pcm": "hw:7,0",
                   "usb_id": "2886:0022"},
        "device_state": {"outcome": "cut_short", "why": "deadline", "at": "bld_msg",
                         "detail": "budget spent", "kind": "xvf3800",
                         "begun_mono_us": 1_000, "ended_mono_us": 2_000, "wall_us": 3_000,
                         "registers": {"version": {"resid": 48, "cmd": 0, "bytes": "0102",
                                                   "value": [1, 2]}}},
        "losses": [
            {"at_frame": 20, "cause": "writer_backlog", "frames": 10, "detail": "queue full"},
            {"at_frame": 60, "cause": "overrun", "elapsed_frames": 25}
        ]
    });
    if end {
        spec["end"] = serde_json::json!({"reason": "stopped", "detail": "told to stop"});
    }
    spec
}

/// Write the input WAV and `spec`, and run `write-capture` into `out.framelog`.
fn write_capture(spec: &serde_json::Value) -> (tempfile::TempDir, Output) {
    let dir = tempfile::tempdir().unwrap();
    let (spec_path, wav) = (dir.path().join("spec.json"), dir.path().join("in.wav"));
    std::fs::write(&spec_path, spec.to_string()).unwrap();
    std::fs::write(&wav, input_wav()).unwrap();
    let out = frame_log(&[
        &"write-capture",
        &spec_path,
        &wav,
        &dir.path().join("out.framelog"),
    ]);
    (dir, out)
}

#[test]
fn write_capture_writes_a_log_the_reader_reads_back() {
    let spec = write_spec(true);
    let (dir, out) = write_capture(&spec);
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    assert_eq!(
        json(&out),
        serde_json::json!({"record_id": "00112233445566778899aabbccddeeff", "frames": 100})
    );
    let log = dir.path().join("out.framelog");

    let s = json(&frame_log(&[&"summary", &log]));
    let capture = &s["header"]["capture"];
    assert_eq!(capture["writer"]["program"], "frame-log");
    assert_eq!(
        capture["writer"]["build"],
        serde_json::json!({"commit": spec["commit"], "dirty": null, "source": "caller"})
    );
    assert_eq!(capture["device"]["card_long_name"], "Synthetic card");
    assert_eq!(capture["device_state"], spec["device_state"]);
    assert_eq!(capture["granted"]["period_frames"], 16);
    assert_eq!(capture["granted"]["buffer_frames"], 256);
    assert_eq!(capture["device_control"]["performed"], false);
    assert_eq!(s["record_id"], "00112233445566778899aabbccddeeff");
    assert_eq!(s["hello"]["pod_id"], "synthetic");
    assert_eq!(s["hello"]["channels"], 3);
    assert_eq!(s["capture"]["channel_names"], spec["layout"]);
    assert_eq!(s["capture"]["started"], spec["start"]);
    let losses: Vec<_> = s["discontinuities"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|d| d["kind"] == "loss")
        .map(|d| {
            (
                d["at_frame"].clone(),
                d["frames"].clone(),
                d["cause"].clone(),
                d["clock_estimate_frames"].clone(),
            )
        })
        .collect();
    assert_eq!(
        losses,
        [
            (
                20.into(),
                10.into(),
                "writer_backlog".into(),
                serde_json::Value::Null
            ),
            (
                60.into(),
                serde_json::Value::Null,
                "overrun".into(),
                25.into()
            ),
        ]
    );
    assert_eq!(s["late"], serde_json::json!([]));
    assert_eq!(s["wall_steps"], serde_json::json!([]));
    assert_eq!(s["end"]["reason"], "stopped");
    assert_eq!(s["end"]["at_frame"], 100);
    assert_eq!(s["end"]["detail"], "told to stop");
    assert_eq!(s["tail"]["verdict"], "ended");

    let wav = dir.path().join("out.wav");
    let map = json(&frame_log(&[&"export-wav", &log, &wav]));
    assert_eq!(map["wav_frames"], IN_FRAMES + 25);
    assert_eq!(
        map["holes"],
        serde_json::json!([
            {"wav_frame": 20, "at_frame": 20, "filled_frames": 10, "basis": "measured"},
            {"wav_frame": 60, "at_frame": 60, "filled_frames": 25, "basis": "clock_estimate"},
        ])
    );
    let mut r = frame_log::WavReader::open(&wav).unwrap();
    let bytes = r.read_frames(r.frames()).unwrap();
    let got: Vec<i16> = bytes
        .chunks_exact(2)
        .map(|b| i16::from_le_bytes([b[0], b[1]]))
        .collect();
    for (j, frame) in got.chunks_exact(CHANNELS).enumerate() {
        let want: Vec<i16> = match j {
            20..30 | 60..85 => vec![0; CHANNELS],
            85.. => (0..CHANNELS).map(|ch| sample(j - 25, ch)).collect(),
            _ => (0..CHANNELS).map(|ch| sample(j, ch)).collect(),
        };
        assert_eq!(frame, want, "WAV frame {j}");
    }
}

#[test]
fn write_capture_with_no_end_reads_back_unterminated() {
    let (dir, out) = write_capture(&write_spec(false));
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let s = json(&frame_log(&[&"summary", &dir.path().join("out.framelog")]));
    assert_eq!(s["end"], serde_json::Value::Null);
    assert_eq!(s["tail"]["verdict"], "unterminated");
}

#[test]
fn a_refused_spec_exits_1_and_leaves_no_log() {
    let mut spec = write_spec(true);
    spec["layout"] = serde_json::json!(["a", "b"]);
    let (dir, out) = write_capture(&spec);
    assert_eq!(out.status.code(), Some(1));
    assert!(out.stdout.is_empty());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("layout names 2 channels"), "{stderr}");
    assert!(!dir.path().join("out.framelog").exists());
}

#[test]
fn an_output_whose_result_cannot_be_printed_is_removed_and_exits_1() {
    let log = capture();
    let dir = log.dir.path();
    let (spec, wav) = (dir.join("spec.json"), dir.join("in.wav"));
    std::fs::write(&spec, write_spec(true).to_string()).unwrap();
    std::fs::write(&wav, input_wav()).unwrap();
    let (exported, written) = (dir.join("out.wav"), dir.join("out.framelog"));
    let log_path = log.path();
    let cases: [(Vec<&dyn AsRef<std::ffi::OsStr>>, &Path); 2] = [
        (vec![&"export-wav", &log_path, &exported], &exported),
        (vec![&"write-capture", &spec, &wav, &written], &written),
    ];
    for (args, out) in cases {
        // Every write to /dev/full fails with ENOSPC.
        let full = std::fs::OpenOptions::new()
            .write(true)
            .open("/dev/full")
            .unwrap();
        let got = Command::new(env!("CARGO_BIN_EXE_frame-log"))
            .args(args.iter().map(|a| a.as_ref()))
            .stdout(full)
            .output()
            .unwrap();
        assert_eq!(got.status.code(), Some(1));
        let stderr = String::from_utf8(got.stderr).unwrap();
        assert!(stderr.contains("writing stdout"), "{stderr}");
        assert!(stderr.contains("removed"), "{stderr}");
        assert!(!out.exists(), "{}", out.display());
    }
}

#[test]
fn a_command_line_it_does_not_understand_exits_2() {
    let p = Path::new("x");
    for args in [
        vec![],
        vec![&"summary" as &dyn AsRef<std::ffi::OsStr>],
        vec![&"summary", &p, &p],
        vec![&"export-wav", &p],
        vec![&"write-capture", &p, &p],
        vec![&"write-capture", &p, &p, &p, &p],
        vec![&"convert", &p],
    ] {
        let out = frame_log(&args);
        assert_eq!(out.status.code(), Some(2), "{:?}", args.len());
        assert!(String::from_utf8_lossy(&out.stderr).starts_with("usage: frame-log"));
    }
}
