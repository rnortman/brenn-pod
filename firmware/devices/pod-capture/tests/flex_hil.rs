//! Hardware: the reSpeaker Flex XVF3800 (USB id `2886:0022`, the USB-audio
//! image's six channels at 16 kHz) recorded by the built binary. Silent: nothing
//! is played. Not in `make check` or CI; run by `make -C firmware capture-hil`
//! with the Flex attached and its capture device free.
//!
//! A capture that fails to start fails the test with its `capture_failed` line
//! printed whole: a device a sound server holds reads as `open` with ALSA's text,
//! not as a defect of the Flex. The tests do not interpret the error.
//!
//! The waveform guards were calibrated on the ESP pod's I2S image. A channel of
//! the USB image that fails one is a reading for the owner, not a threshold to
//! move.

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::thread;
use std::time::Duration;

use audio_pipeline::ring::{BEAM_GUARDS, LivenessGuards, RAW_MIC_GUARDS, WaveformStats};
use audio_pipeline::wide::{LossCause, StreamEndReason};
use frame_log::{Cause, Step, Summary, summarize, walk};

const BIN: &str = env!("CARGO_BIN_EXE_pod-capture");
const CARD: &str = "Flex XVF3800 L16K6Ch";
const LAYOUT: [&str; 6] = [
    "conference_beam",
    "asr_beam",
    "raw_mic_0",
    "raw_mic_1",
    "raw_mic_2",
    "raw_mic_3",
];
const RATE: u32 = 16_000;
const CHANNELS: usize = 6;

/// A running capture and its event lines so far.
struct Running {
    child: Child,
    stdout: BufReader<ChildStdout>,
    started: serde_json::Value,
}

fn start(out: &Path) -> Running {
    let mut child = Command::new(BIN)
        .args(["--card", CARD, "--rate", "16000", "--format", "s16_le"])
        .args([
            "--layout",
            &LAYOUT.join(","),
            "--pod-id",
            "flex-hil",
            "--out",
        ])
        .arg(out)
        .stdout(Stdio::piped())
        .spawn()
        .expect("the pod-capture binary runs");
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut first = String::new();
    stdout.read_line(&mut first).unwrap();
    let started: serde_json::Value = serde_json::from_str(first.trim_end())
        .unwrap_or_else(|e| panic!("first line is not JSON ({e}): {first:?}"));
    if started["event"] != "capture_started" {
        let _ = child.wait();
        panic!("the capture did not start:\n{first}");
    }
    println!("{}", first.trim_end());
    Running {
        child,
        stdout,
        started,
    }
}

fn signal(child: &Child, sig: libc::c_int) {
    let pid = libc::pid_t::try_from(child.id()).unwrap();
    // SAFETY: kill(2) on a child this test spawned and has not yet reaped.
    assert_eq!(unsafe { libc::kill(pid, sig) }, 0, "kill({pid}, {sig})");
}

/// SIGINT, then every remaining line and the exit status.
fn stop(mut r: Running) -> (Vec<serde_json::Value>, Option<i32>) {
    signal(&r.child, libc::SIGINT);
    let mut lines = Vec::new();
    for line in (&mut r.stdout).lines() {
        let line = line.unwrap();
        println!("{line}");
        lines.push(serde_json::from_str(&line).expect("an event line is JSON"));
    }
    let status = r.child.wait().unwrap();
    (lines, status.code())
}

fn assert_started_on_the_flex(started: &serde_json::Value) {
    let device = &started["device"];
    assert_eq!(device["usb_id"], "2886:0022", "{started}");
    assert!(
        device["card_long_name"]
            .as_str()
            .unwrap()
            .contains("reSpeaker Flex XVF3800 L16K6Ch"),
        "{started}"
    );
    let granted = &started["granted"];
    assert_eq!(granted["rate_hz"], RATE, "{started}");
    assert_eq!(granted["channels"], CHANNELS, "{started}");
    assert_eq!(granted["sample_format"], "s16_le", "{started}");
}

fn summary_of(path: &Path) -> Summary {
    let s = summarize(path).expect("the log reads back");
    println!("{}", serde_json::to_string_pretty(&s).unwrap());
    s
}

/// Each channel's samples, split into the contiguous stretches between losses.
fn stretches(path: &Path) -> Vec<[Vec<i16>; CHANNELS]> {
    let mut out: Vec<[Vec<i16>; CHANNELS]> = vec![Default::default()];
    for step in walk(path).expect("the log opens") {
        match step.expect("the log reads") {
            Step::Audio(a) => {
                let current = out.last_mut().unwrap();
                for frame in a.pcm.chunks_exact(2 * CHANNELS) {
                    for (ch, sample) in frame.chunks_exact(2).enumerate() {
                        current[ch].push(i16::from_le_bytes([sample[0], sample[1]]));
                    }
                }
            }
            Step::Loss { .. } => out.push(Default::default()),
            _ => {}
        }
    }
    out.retain(|s| !s[0].is_empty());
    out
}

fn guards(ch: usize) -> (&'static str, &'static LivenessGuards) {
    if ch < 2 {
        ("BEAM_GUARDS", &BEAM_GUARDS)
    } else {
        ("RAW_MIC_GUARDS", &RAW_MIC_GUARDS)
    }
}

/// Print every channel's statistics in every stretch, then fail on any defect.
fn assert_channels_live(path: &Path) {
    let mut defects = Vec::new();
    for (i, stretch) in stretches(path).iter().enumerate() {
        for (ch, samples) in stretch.iter().enumerate() {
            let stats = WaveformStats::of(samples);
            let (name, g) = guards(ch);
            let defect = stats.defect_under(g);
            println!(
                "stretch {i} channel {ch} ({}) under {name}: {stats} -> {}",
                LAYOUT[ch],
                defect.unwrap_or("live")
            );
            if let Some(d) = defect {
                defects.push(format!("stretch {i} channel {ch} ({}): {d}", LAYOUT[ch]));
            }
        }
    }
    assert!(defects.is_empty(), "defects: {defects:#?}");
}

fn late_blocks(s: &Summary) -> u64 {
    s.late.iter().flatten().map(|r| r.blocks).sum()
}

fn late_runs(s: &Summary) -> usize {
    s.late.as_ref().map_or(0, Vec::len)
}

/// No segment of a continuous capture lies apart from the position.
fn assert_segments_continuous(s: &Summary) {
    for seg in &s.segments {
        assert_eq!(
            (seg.not_sent_frames, seg.resent_frames),
            (0, 0),
            "segments: {:#?}",
            s.segments
        );
    }
}

#[test]
#[ignore = "needs the reSpeaker Flex attached and free; run by `make -C firmware capture-hil`"]
fn ten_seconds_of_the_flex_record_every_channel_with_no_loss() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("flex.framelog");
    let r = start(&path);
    assert_started_on_the_flex(&r.started);
    thread::sleep(Duration::from_secs(10));
    let (lines, code) = stop(r);
    assert_eq!(code, Some(0), "lines: {lines:#?}");
    assert_eq!(lines.last().unwrap()["event"], "capture_stopped");

    let s = summary_of(&path);
    println!("late blocks: {} in {} runs", late_blocks(&s), late_runs(&s));
    assert_eq!(s.tail.verdict, "ended");
    assert_eq!(
        s.end.as_ref().map(|e| e.reason),
        Some(StreamEndReason::Stopped)
    );
    assert!(
        s.discontinuities.is_empty(),
        "discontinuities: {:#?}",
        s.discontinuities
    );
    assert_eq!(s.segments.len(), 1, "segments: {:#?}", s.segments);
    assert_segments_continuous(&s);
    assert_eq!(s.wall_steps.as_deref(), Some(&[][..]), "wall steps");
    assert_eq!(
        s.capture.as_ref().map(|c| c.channel_names.clone()),
        Some(LAYOUT.map(str::to_owned).to_vec())
    );
    assert!(s.duration_frames_s >= 9.0, "{}", s.duration_frames_s);
    let clock = s.duration_clock_s.expect("blocks were recorded");
    assert!(
        (s.duration_frames_s - clock).abs() <= 0.1,
        "frames say {} s, the clock {clock} s",
        s.duration_frames_s
    );
    assert_channels_live(&path);
}

#[test]
#[ignore = "needs the reSpeaker Flex attached and free; run by `make -C firmware capture-hil`"]
fn a_one_second_pause_is_an_overrun_loss_with_a_clock_estimate() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("flex-overrun.framelog");
    let r = start(&path);
    assert_started_on_the_flex(&r.started);
    thread::sleep(Duration::from_secs(5));
    signal(&r.child, libc::SIGSTOP);
    thread::sleep(Duration::from_secs(1));
    signal(&r.child, libc::SIGCONT);
    thread::sleep(Duration::from_secs(5));
    let (lines, code) = stop(r);
    assert_eq!(code, Some(0), "lines: {lines:#?}");
    assert!(
        lines.iter().any(|l| l["event"] == "capture_gap"
            && l["cause"] == "overrun"
            && l["frames"].is_null()),
        "no capture_gap/overrun line: {lines:#?}"
    );

    let s = summary_of(&path);
    println!("late blocks: {} in {} runs", late_blocks(&s), late_runs(&s));
    assert_eq!(s.tail.verdict, "ended");
    assert_segments_continuous(&s);
    let overruns: Vec<_> = s
        .discontinuities
        .iter()
        .filter(|d| {
            d.kind == "loss"
                && d.cause == Some(Cause::Loss(LossCause::Overrun))
                && d.frames.is_none()
        })
        .collect();
    assert!(
        !overruns.is_empty(),
        "discontinuities: {:#?}",
        s.discontinuities
    );
    let estimated: i64 = s
        .discontinuities
        .iter()
        .filter_map(|d| d.clock_estimate_frames)
        .sum();
    let seconds = estimated as f64 / f64::from(RATE);
    println!("clock estimates sum to {estimated} frames, {seconds:.4} s, for a 1 s pause");
    assert!(
        (0.9..=1.2).contains(&seconds),
        "clock estimates sum to {seconds} s"
    );
    assert_channels_live(&path);
}
