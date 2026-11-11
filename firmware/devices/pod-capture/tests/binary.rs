//! The binary's refusals that need no sound card: each is one `capture_failed`
//! line and exit 1, or usage and exit 2.

use std::path::Path;
use std::process::{Command, Output};

const BIN: &str = env!("CARGO_BIN_EXE_pod-capture");

/// A selector no card on any machine is named.
const NO_SUCH_CARD: &str = "no-such-card-3f9a1c";

fn capture(card: &str, out: &Path) -> Output {
    Command::new(BIN)
        .args(["--card", card, "--rate", "16000", "--format", "s16_le"])
        .args(["--layout", "a,b", "--out"])
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
    let path = dir.path().join("r.bpcr");
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
    assert!(!path.exists(), "no record is created for a refused card");
}

#[test]
fn an_existing_destination_is_refused_and_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("r.bpcr");
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
    let path = dir.path().join("r.bpcr");
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
        .args(["--layout", "a,b", "--out"])
        .arg(dir.path().join("r.bpcr"))
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
    let out = Command::new(BIN)
        .args(["--card", "x", "--rate", "16000", "--format", "s16_le"])
        .args(["--layout", "a,a", "--out"])
        .arg(dir.path().join("r"))
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}
