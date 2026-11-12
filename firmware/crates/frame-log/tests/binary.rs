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

#[test]
fn a_command_line_it_does_not_understand_exits_2() {
    let p = Path::new("x");
    for args in [
        vec![],
        vec![&"summary" as &dyn AsRef<std::ffi::OsStr>],
        vec![&"summary", &p, &p],
        vec![&"export-wav", &p],
        vec![&"convert", &p],
    ] {
        let out = frame_log(&args);
        assert_eq!(out.status.code(), Some(2), "{:?}", args.len());
        assert!(String::from_utf8_lossy(&out.stderr).starts_with("usage: frame-log"));
    }
}
