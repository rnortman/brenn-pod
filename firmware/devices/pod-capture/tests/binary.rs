//! The binary's refusals that need no sound card: each is one `capture_failed`
//! line and exit 1, or usage and exit 2.

use std::path::Path;
use std::process::{Command, Output};

const BIN: &str = env!("CARGO_BIN_EXE_pod-capture");

/// A selector no card on any machine is named.
const NO_SUCH_CARD: &str = "no-such-card-3f9a1c";

fn capture(card: &str, out: &Path) -> Output {
    capture_layout(card, "a,b", out)
}

fn capture_layout(card: &str, layout: &str, out: &Path) -> Output {
    Command::new(BIN)
        .args(["--card", card, "--rate", "16000", "--format", "s16_le"])
        .args(["--layout", layout, "--pod-id", "bench", "--out"])
        .arg(out)
        .output()
        .expect("the pod-capture binary runs")
}

/// The one line on stdout, parsed.
fn only_line(out: &Output) -> serde_json::Value {
    let stdout = String::from_utf8(out.stdout.clone()).expect("stdout is UTF-8");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 1, "stdout:\n{stdout}");
    serde_json::from_str(lines[0]).expect("the line is JSON")
}

#[test]
fn an_unknown_card_is_refused_with_every_card_listed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("r.framelog");
    let out = capture(NO_SUCH_CARD, &path);
    assert_eq!(out.status.code(), Some(1));
    let line = only_line(&out);
    assert_eq!(line["event"], "capture_failed");
    assert_eq!(line["reason"], "no_card");
    assert!(line["record_id"].is_null());
    assert!(line["at_frame"].is_null());
    assert!(line["cards"].is_array(), "{line}");
    assert!(
        line["detail"].as_str().unwrap().contains(NO_SUCH_CARD),
        "{line}"
    );
    assert!(!path.exists(), "no log is created for a refused card");
}

#[test]
fn an_existing_destination_is_refused_and_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("r.framelog");
    std::fs::write(&path, b"somebody else's bytes").unwrap();
    let out = capture(NO_SUCH_CARD, &path);
    assert_eq!(out.status.code(), Some(1));
    let line = only_line(&out);
    assert_eq!(line["event"], "capture_failed");
    assert_eq!(line["reason"], "destination_exists");
    assert!(line.get("cards").is_none(), "{line}");
    assert_eq!(std::fs::read(&path).unwrap(), b"somebody else's bytes");
}

#[test]
fn a_dangling_link_at_the_destination_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("r.framelog");
    std::os::unix::fs::symlink(dir.path().join("nowhere"), &path).unwrap();
    let out = capture(NO_SUCH_CARD, &path);
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(only_line(&out)["reason"], "destination_exists");
    assert!(!dir.path().join("nowhere").exists());
}

#[test]
fn a_stderr_whose_reader_has_gone_costs_no_line_and_no_exit_status() {
    let closed_stderr = || {
        let (reader, writer) = std::io::pipe().unwrap();
        drop(reader);
        writer
    };
    let dir = tempfile::tempdir().unwrap();
    let out = Command::new(BIN)
        .args([
            "--card",
            NO_SUCH_CARD,
            "--rate",
            "16000",
            "--format",
            "s16_le",
        ])
        .args(["--layout", "a,b", "--pod-id", "bench", "--out"])
        .arg(dir.path().join("r.framelog"))
        .stderr(closed_stderr())
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(only_line(&out)["reason"], "no_card");
    let out = Command::new(BIN)
        .arg("--bogus")
        .stderr(closed_stderr())
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}

#[test]
fn a_command_line_it_does_not_understand_exits_2_with_usage() {
    for args in [
        &[][..],
        &["--card", "x"][..],
        &["--card", "x", "--card", "y"][..],
        &["--bogus"][..],
    ] {
        let out = Command::new(BIN).args(args).output().unwrap();
        assert_eq!(out.status.code(), Some(2), "{args:?}");
        assert!(out.stdout.is_empty(), "{args:?}");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("usage: pod-capture"), "{args:?}: {stderr}");
    }
    let dir = tempfile::tempdir().unwrap();
    for (layout, pod_id) in [
        ("a,a", "bench"),
        ("a,b", ""),
        ("a,b", "a-pod-id-of-thirty-three-bytes-xx"),
    ] {
        let out = Command::new(BIN)
            .args(["--card", "x", "--rate", "16000", "--format", "s16_le"])
            .args(["--layout", layout, "--pod-id", pod_id, "--out"])
            .arg(dir.path().join("r"))
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(2), "{layout} {pod_id:?}");
        assert!(out.stdout.is_empty());
    }
    let out = Command::new(BIN)
        .args(["--card", "x", "--rate", "16000", "--format", "s16_le"])
        .args(["--layout", "a,b", "--out"])
        .arg(dir.path().join("r"))
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2), "--pod-id is required");
    assert!(String::from_utf8_lossy(&out.stderr).contains("--pod-id is required"));
}

#[test]
fn a_layout_of_256_channels_is_refused_before_any_card_is_looked_for() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("r.framelog");
    let layout: Vec<String> = (0..256).map(|i| format!("c{i}")).collect();
    let out = capture_layout(NO_SUCH_CARD, &layout.join(","), &path);
    assert_eq!(out.status.code(), Some(1));
    let line = only_line(&out);
    assert_eq!(line["event"], "capture_failed");
    assert_eq!(line["reason"], "params");
    assert!(line["detail"].as_str().unwrap().contains("256"), "{line}");
    assert!(!path.exists());
}
