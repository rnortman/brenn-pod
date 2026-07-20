//! Shared integration-test helpers: the deterministic fixture generator and
//! its expected per-segment facts. Included via `mod common;` by the
//! integration-test binaries in `tests/` (currently `replay_roundtrip.rs` and
//! `wake_integration.rs`); not every binary consumes every item.
#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use audio_pipeline::wire::{
    decode_frame, encode_frame, AudioFrame, ChannelSource, Codec, EndReason, Hello, SegmentEnd,
    SegmentStart, StreamFrame, Telemetry, TelemetryKind, AUDIO_PROTOCOL_VERSION, MAX_AUDIO_PAYLOAD,
    MAX_FRAME_BYTES,
};
use heapless::Vec as HVec;
use pod_ingest::{
    synth_session, FrameLogReader, FrameLogWriter, HostMicros, LogItem, LogMeta, SynthParams,
};
use serde_json::Value;
use speech_pipeline::SPINE_FORMAT;
use speech_surface::{Sidecar, WakeClass};
use tempfile::TempDir;

/// Pod identity carried in the fixture's `Hello`.
const POD_ID: &str = "pod-fixture";
/// Fixed log-creation epoch, so the committed bytes never move.
const FIXTURE_EPOCH_US: u64 = 1_700_000_000_000_000;
/// 20 ms of 16 kHz mono S16 audio.
const SAMPLES_PER_FRAME: usize = 320;

// Segment A — complete, VAD-released.
const SEG_A_ID: u32 = 1;
const SEG_A_FRAMES: u32 = 3;
const SEG_A_PREROLL: u32 = 160;
const SEG_A_BASE_SAMPLE: u64 = 0;
const SEG_A_BASE_DEVICE_TS: u64 = 1_000_000;
const SEG_A_SAMPLES: u64 = SEG_A_FRAMES as u64 * SAMPLES_PER_FRAME as u64;

// Segment B — one frame then EOF; the daemon finalizes it truncated.
const SEG_B_ID: u32 = 2;
const SEG_B_PREROLL: u32 = 160;
const SEG_B_BASE_SAMPLE: u64 = SEG_A_SAMPLES;
const SEG_B_BASE_DEVICE_TS: u64 = 2_000_000;

/// Segment A's single azimuth reading; index 1 is NaN (no beam tracked) so the
/// NaN→JSON-null path is covered end-to-end.
const AZIMUTHS: [f32; 4] = [0.5, f32::NAN, 0.25, 0.75];
/// Segment A's single speech-energy reading.
const SPENERGY: [f32; 4] = [0.1, 0.2, 0.3, 0.4];

/// Sample rate of the fixture's spine format; maps telemetry device-time offsets
/// to sample offsets.
const SAMPLE_RATE_HZ: u64 = 16_000;
/// Device-time offset (from segment A's base) of its azimuth reading.
const AZIMUTH_DEVICE_TS_OFFSET_US: u64 = 10_000;
/// Device-time offset (from segment A's base) of its speech-energy reading.
const SPENERGY_DEVICE_TS_OFFSET_US: u64 = 30_000;

/// The sample offset of a telemetry reading `device_ts_offset_us` past a
/// segment's base device time.
fn offset_samples(device_ts_offset_us: u64) -> u64 {
    device_ts_offset_us * SAMPLE_RATE_HZ / 1_000_000
}

/// Expected facts for one assembled segment.
pub struct SegmentFacts {
    pub segment_id: u32,
    pub base_sample_index: u64,
    pub preroll_samples: u32,
    pub samples: u64,
    pub frames: u32,
    pub truncated: bool,
}

/// The properties a round-trip test asserts against, kept beside the generator
/// so fixture and expectations move together.
pub struct FixtureFacts {
    pub pod_id: &'static str,
    pub seg_a: SegmentFacts,
    pub seg_b: SegmentFacts,
    /// Segment A's azimuth reading (index 1 is NaN).
    pub azimuths: [f32; 4],
    /// Sample offset (from segment base) of the azimuth reading.
    pub azimuth_offset_samples: u64,
    /// Segment A's speech-energy reading.
    pub spenergy: [f32; 4],
    /// Sample offset (from segment base) of the speech-energy reading.
    pub spenergy_offset_samples: u64,
    pub capture_span_us: u64,
    /// Number of frame-log records the fixture holds — `replay-pod`'s reported
    /// `frames` on a clean replay.
    pub record_count: u64,
}

/// The expected facts described by [`generate_fixture`].
pub fn fixture_facts() -> FixtureFacts {
    FixtureFacts {
        pod_id: POD_ID,
        seg_a: SegmentFacts {
            segment_id: SEG_A_ID,
            base_sample_index: SEG_A_BASE_SAMPLE,
            preroll_samples: SEG_A_PREROLL,
            samples: SEG_A_SAMPLES,
            frames: SEG_A_FRAMES,
            truncated: false,
        },
        seg_b: SegmentFacts {
            segment_id: SEG_B_ID,
            base_sample_index: SEG_B_BASE_SAMPLE,
            preroll_samples: SEG_B_PREROLL,
            samples: SAMPLES_PER_FRAME as u64,
            frames: 1,
            truncated: true,
        },
        azimuths: AZIMUTHS,
        azimuth_offset_samples: offset_samples(AZIMUTH_DEVICE_TS_OFFSET_US),
        spenergy: SPENERGY,
        spenergy_offset_samples: offset_samples(SPENERGY_DEVICE_TS_OFFSET_US),
        capture_span_us: {
            // Derive the span from the frames themselves so adding a later
            // record can never silently shrink the pacing test's lower bound.
            let frames = fixture_frames();
            frames.last().unwrap().0 - frames.first().unwrap().0
        },
        record_count: fixture_frames().len() as u64,
    }
}

/// One audio frame carrying a recognizable global sample ramp (`pcm[j]` is the
/// low 16 bits of the sample's absolute index).
fn audio_frame(
    segment_id: u32,
    first_sample_index: u64,
    device_ts_us: u64,
    n: usize,
) -> StreamFrame {
    let mut pcm: HVec<u8, MAX_AUDIO_PAYLOAD> = HVec::new();
    for j in 0..n {
        let v = ((first_sample_index + j as u64) as i16).to_le_bytes();
        pcm.push(v[0]).unwrap();
        pcm.push(v[1]).unwrap();
    }
    StreamFrame::Audio(AudioFrame {
        segment_id,
        first_sample_index,
        device_ts_us,
        pcm,
    })
}

/// The fixture's frames, each paired with its `host_rx` offset from the first
/// record. Hello → complete segment A (ramp audio + one Azimuths incl. NaN +
/// one SpEnergy interleaved) → truncated-at-EOF segment B.
fn fixture_frames() -> Vec<(u64, StreamFrame)> {
    vec![
        (
            0,
            StreamFrame::Hello(Hello {
                version: AUDIO_PROTOCOL_VERSION,
                pod_id: heapless::String::try_from(POD_ID).unwrap(),
                sample_rate_hz: 16_000,
                bits_per_sample: 16,
                channels: 1,
                codec: Codec::S16Le,
                channel_source: ChannelSource::AsrBeam,
            }),
        ),
        (
            5_000,
            StreamFrame::SegmentStart(SegmentStart {
                segment_id: SEG_A_ID,
                base_sample_index: SEG_A_BASE_SAMPLE,
                base_device_ts_us: SEG_A_BASE_DEVICE_TS,
                preroll_samples: SEG_A_PREROLL,
            }),
        ),
        (
            25_000,
            audio_frame(
                SEG_A_ID,
                SEG_A_BASE_SAMPLE,
                SEG_A_BASE_DEVICE_TS,
                SAMPLES_PER_FRAME,
            ),
        ),
        (
            27_000,
            StreamFrame::Telemetry(Telemetry {
                device_ts_us: SEG_A_BASE_DEVICE_TS + AZIMUTH_DEVICE_TS_OFFSET_US,
                kind: TelemetryKind::Azimuths { values: AZIMUTHS },
            }),
        ),
        (
            45_000,
            audio_frame(
                SEG_A_ID,
                SEG_A_BASE_SAMPLE + SAMPLES_PER_FRAME as u64,
                SEG_A_BASE_DEVICE_TS + 20_000,
                SAMPLES_PER_FRAME,
            ),
        ),
        (
            47_000,
            StreamFrame::Telemetry(Telemetry {
                device_ts_us: SEG_A_BASE_DEVICE_TS + SPENERGY_DEVICE_TS_OFFSET_US,
                kind: TelemetryKind::SpEnergy { values: SPENERGY },
            }),
        ),
        (
            65_000,
            audio_frame(
                SEG_A_ID,
                SEG_A_BASE_SAMPLE + 2 * SAMPLES_PER_FRAME as u64,
                SEG_A_BASE_DEVICE_TS + 40_000,
                SAMPLES_PER_FRAME,
            ),
        ),
        (
            70_000,
            StreamFrame::SegmentEnd(SegmentEnd {
                segment_id: SEG_A_ID,
                device_ts_us: SEG_A_BASE_DEVICE_TS + 60_000,
                frames_sent: SEG_A_FRAMES,
                samples_sent: SEG_A_SAMPLES,
                reason: EndReason::VadRelease,
            }),
        ),
        (
            280_000,
            StreamFrame::SegmentStart(SegmentStart {
                segment_id: SEG_B_ID,
                base_sample_index: SEG_B_BASE_SAMPLE,
                base_device_ts_us: SEG_B_BASE_DEVICE_TS,
                preroll_samples: SEG_B_PREROLL,
            }),
        ),
        (
            300_000,
            audio_frame(
                SEG_B_ID,
                SEG_B_BASE_SAMPLE,
                SEG_B_BASE_DEVICE_TS,
                SAMPLES_PER_FRAME,
            ),
        ),
    ]
}

/// Encode a frame to its exact wire bytes (`[u16 len][postcard]`) — the same
/// bytes the daemon taps pre-decode, so replay is byte-faithful.
fn framed(frame: &StreamFrame) -> Vec<u8> {
    let mut buf = [0u8; MAX_AUDIO_PAYLOAD + 64];
    let n = encode_frame(frame, &mut buf).expect("frame fits");
    buf[..n].to_vec()
}

/// Deterministically build the short-capture fixture and return its exact
/// frame-log bytes. Uses the real [`FrameLogWriter`] and [`encode_frame`] so a
/// frame-log-format or wire-encoding change moves these bytes and trips the
/// golden test.
pub fn generate_fixture() -> Vec<u8> {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("short-capture.framelog");
    let meta = LogMeta {
        build_id: "fixture-gen".into(),
        created_epoch_us: HostMicros(FIXTURE_EPOCH_US),
        conn_seq: 1,
        rolled_from: None,
    };
    let mut w = FrameLogWriter::create(&path, meta).expect("create fixture log");
    for (offset, frame) in fixture_frames() {
        w.append(HostMicros(FIXTURE_EPOCH_US + offset), &framed(&frame))
            .expect("append fixture frame");
    }
    w.finish().expect("finish fixture log");
    std::fs::read(&path).expect("read back fixture bytes")
}

// --- Spawned-daemon harness for the round-trip integration tests -------------

/// Default deadline for waiting on a daemon JSONL event.
pub const EVENT_DEADLINE: Duration = Duration::from_secs(10);

/// A running `speech-surface` daemon subprocess and the tempdir holding its
/// config, JSONL sink, and captured stdout/stderr. Dropping it kills the
/// process, so a panicking test never leaks a daemon.
pub struct DaemonChild {
    child: Child,
    pub jsonl_path: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    // Kept alive so the config/JSONL/capture files outlive the child.
    _dir: TempDir,
}

impl DaemonChild {
    /// Everything the daemon wrote — its own words, for a panic message.
    pub fn diagnostics(&self) -> String {
        let read = |p: &Path| std::fs::read_to_string(p).unwrap_or_default();
        format!(
            "--- daemon jsonl ({}) ---\n{}\n--- daemon stdout ---\n{}\n--- daemon stderr ---\n{}",
            self.jsonl_path.display(),
            read(&self.jsonl_path),
            read(&self.stdout_path),
            read(&self.stderr_path),
        )
    }

    /// Wait for the `listening` event and return the resolved ingest address —
    /// the only way a subprocess learns the ephemeral (`:0`) port.
    pub fn listen_addr(&self) -> String {
        let ev = wait_for_event(self, "listening", EVENT_DEADLINE, |v| {
            v["event"] == "listening"
        });
        ev["addr"]
            .as_str()
            .unwrap_or_else(|| panic!("listening event carried no addr: {ev}"))
            .to_string()
    }

    /// SIGTERM the daemon, wait for exit, and assert it exited cleanly — the
    /// graceful-shutdown path is part of what the harness proves. Takes `&mut
    /// self` (not by value) so the tempdir survives for a post-shutdown read of
    /// the final `stage_health` line.
    pub fn sigterm_and_wait(&mut self) {
        use rustix::process::{kill_process, Pid, Signal};
        let pid = Pid::from_raw(self.child.id() as i32).expect("child pid is valid");
        kill_process(pid, Signal::TERM).expect("send SIGTERM to daemon");
        let status = self.child.wait().expect("wait for daemon exit");
        assert!(
            status.success(),
            "daemon exited uncleanly ({status})\n{}",
            self.diagnostics()
        );
    }
}

impl Drop for DaemonChild {
    fn drop(&mut self) {
        // Kill if still running (a clean stop went through `sigterm_and_wait`,
        // which already reaped it; this covers panics and forgotten stops).
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Spawn the real `speech-surface` daemon with `config_toml`, its JSONL sink
/// forced to a tempdir file (via `--jsonl`) and stdout/stderr captured to files
/// surfaced in panics. The config need only carry the listen address and record
/// settings; the sink path is owned here so callers do not race on it.
pub fn spawn_daemon(config_toml: &str) -> DaemonChild {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("speech.toml");
    std::fs::write(&config_path, config_toml).expect("write daemon config");
    let jsonl_path = dir.path().join("events.jsonl");
    let stdout_path = dir.path().join("daemon.stdout");
    let stderr_path = dir.path().join("daemon.stderr");
    let stdout = std::fs::File::create(&stdout_path).expect("create stdout capture");
    let stderr = std::fs::File::create(&stderr_path).expect("create stderr capture");

    let child = Command::new(env!("CARGO_BIN_EXE_speech-surface"))
        .arg("--config")
        .arg(&config_path)
        .arg("--jsonl")
        .arg(&jsonl_path)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .expect("spawn speech-surface daemon");

    DaemonChild {
        child,
        jsonl_path,
        stdout_path,
        stderr_path,
        _dir: dir,
    }
}

/// Every parseable JSONL line the daemon has written so far.
pub fn read_events(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .collect()
}

/// Assert exactly one event named `name` is present and return it. Panics with
/// the daemon's own output on a zero-or-many count — the "one line, fetch it"
/// shape every playback scenario asserts, in one place so the count and the
/// fetch can never disagree.
pub fn expect_one<'a>(events: &'a [Value], name: &str, daemon: &DaemonChild) -> &'a Value {
    let mut matches = events.iter().filter(|v| v["event"] == name);
    let first = matches.next().unwrap_or_else(|| {
        panic!(
            "expected exactly one {name} line, found none\n{}",
            daemon.diagnostics()
        )
    });
    assert!(
        matches.next().is_none(),
        "expected exactly one {name} line, found more than one\n{}",
        daemon.diagnostics()
    );
    first
}

/// SIGTERM the daemon, then return the final at-shutdown `stage_health` line.
/// Panics with the daemon's output if none was written — the post-shutdown
/// snapshot read every playback scenario ends on, in one place.
pub fn final_stage_health(daemon: &mut DaemonChild) -> Value {
    daemon.sigterm_and_wait();
    let events = read_events(&daemon.jsonl_path);
    events
        .iter()
        .rev()
        .find(|v| v["event"] == "stage_health" && v["at_shutdown"] == true)
        .cloned()
        .unwrap_or_else(|| panic!("no final stage_health line\n{}", daemon.diagnostics()))
}

/// Poll the daemon's JSONL file until a line satisfies `pred`, returning it.
/// Panics past `deadline` with the daemon's own output — a failure to start
/// fails the test with the daemon's words, never a bare timeout. No bare test
/// sleeps: every assertion rests on an observed event.
pub fn wait_for_event(
    daemon: &DaemonChild,
    label: &str,
    deadline: Duration,
    pred: impl Fn(&Value) -> bool,
) -> Value {
    let start = Instant::now();
    loop {
        if let Some(v) = read_events(&daemon.jsonl_path)
            .into_iter()
            .find(|v| pred(v))
        {
            return v;
        }
        if start.elapsed() >= deadline {
            panic!(
                "event {label:?} did not appear within {deadline:?}\n{}",
                daemon.diagnostics()
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// The exact wire payload of every complete record in a frame log, in order. A
/// torn tail ends the sequence (the truncated final bytes are not a record).
/// Panics on a header/read error — a corrupt log fails the test loudly.
pub fn log_payloads(path: &Path) -> Vec<Vec<u8>> {
    let reader = FrameLogReader::open(path).expect("open frame log");
    let mut payloads = Vec::new();
    for item in reader {
        match item.expect("read frame-log record") {
            LogItem::Record { payload, .. } => payloads.push(payload),
            LogItem::TornTail => break,
        }
    }
    payloads
}

/// The single file with extension `ext` in `dir`. Panics unless exactly one
/// exists.
pub fn find_one_with_ext(dir: &Path, ext: &str) -> PathBuf {
    let mut matches: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("read dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == ext))
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "expected exactly one .{ext} in {}, found {matches:?}",
        dir.display()
    );
    matches.pop().unwrap()
}

/// The single `.framelog` in `dir`. Panics unless exactly one exists — the
/// re-capture test expects one connection's log and no more.
pub fn find_one_framelog(dir: &Path) -> PathBuf {
    find_one_with_ext(dir, "framelog")
}

/// Run `replay-pod` against `addr` at `pace` over one framelog, appending
/// `extra_args` before the framelog path. The single spawn point every replay
/// helper delegates to, so an invocation-flag change lands in one place.
fn run_replay_with(
    addr: &str,
    pace: &str,
    framelog: &Path,
    extra_args: &[&str],
) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_replay-pod"))
        .arg("--connect")
        .arg(addr)
        .arg("--pace")
        .arg(pace)
        .args(extra_args)
        .arg(framelog)
        .output()
        .expect("run replay-pod")
}

/// Run `replay-pod` against `addr` at `pace` over one framelog, returning the
/// finished process output (JSONL on stdout, plus exit status).
pub fn run_replay(addr: &str, pace: &str, framelog: &Path) -> std::process::Output {
    run_replay_with(addr, pace, framelog, &[])
}

/// Run `replay-pod` against `addr` at `fast` pace with `--linger-until-eoa`:
/// it stays connected past end-of-log until the daemon's playback `EndOfAudio`
/// is observed, so the playback the daemon queues actually crosses the wire and
/// the drain can tally it. Returns the finished process output.
pub fn run_replay_linger(addr: &str, framelog: &Path) -> std::process::Output {
    run_replay_with(addr, "fast", framelog, &["--linger-until-eoa"])
}

/// The named report line from `replay-pod`'s stdout (e.g. `replay_complete`),
/// parsed as JSON. Panics with the full stdout if absent — the report is the
/// tool's device-side assertion surface.
pub fn find_report_line(out: &std::process::Output, event: &str) -> Value {
    let stdout = String::from_utf8_lossy(&out.stdout);
    stdout
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .find(|v| v["event"] == event)
        .unwrap_or_else(|| panic!("no {event:?} line in replay-pod stdout:\n{stdout}"))
}

/// Assert `replay-pod` exited successfully; on failure dump both the replay
/// output and the daemon's diagnostics — the two sources a round-trip break
/// needs.
pub fn assert_replay_ok(out: &std::process::Output, daemon: &DaemonChild) {
    assert!(
        out.status.success(),
        "replay-pod exited {:?}\n--- replay stdout ---\n{}\n--- replay stderr ---\n{}\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
        daemon.diagnostics(),
    );
}

/// Compose a harness daemon config: ephemeral loopback listener, the `[record]`
/// block for `record_dir`, then `extra` appended verbatim (e.g. a `[wake]`
/// table). The single skeleton every `*_daemon_config` builder is built on.
fn daemon_config_with(record_dir: Option<&Path>, extra: &str) -> String {
    format!(
        "listen_addr = \"127.0.0.1:0\"\n{}{extra}",
        record_block(record_dir),
    )
}

/// A harness daemon config: ephemeral loopback listener, recording off unless
/// `record_dir` is given (then recording into it). No `[wake]` table.
pub fn daemon_config(record_dir: Option<&Path>) -> String {
    daemon_config_with(record_dir, "")
}

/// The `[record]` config block: recording off, or on into `dir`.
fn record_block(record_dir: Option<&Path>) -> String {
    match record_dir {
        None => "[record]\nenabled = false\n".to_string(),
        Some(dir) => format!("[record]\nenabled = true\ndir = \"{}\"\n", dir.display()),
    }
}

// --- Wake-gate integration fixtures (committed models + TTS clip) ------------

/// The committed openWakeWord mel-spectrogram model.
pub const OWW_MELSPECTROGRAM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../models/oww/melspectrogram.onnx"
);
/// The committed openWakeWord embedding model.
pub const OWW_EMBEDDING: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../models/oww/embedding_model.onnx"
);
/// The committed "Hey Jarvis" wake model.
pub const OWW_MODEL: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../models/oww/hey_jarvis_v0.1.onnx"
);
/// The committed TTS "Hey Jarvis" wake-phrase clip.
pub const WAKE_PHRASE_WAV: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../testdata/wake/wake-phrase.wav"
);
/// The committed Silero VAD model — the host endpointer's speech classifier.
pub const SILERO_MODEL: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../models/silero/silero_vad.onnx"
);

/// The `[wake]` config block running the real openWakeWord gate over the
/// committed models. Shared by every `oww_*` config builder so the model wiring
/// lives in exactly one place.
fn oww_wake_block() -> String {
    format!(
        "[wake]\n\
         mode = \"oww\"\n\
         melspectrogram = \"{OWW_MELSPECTROGRAM}\"\n\
         embedding = \"{OWW_EMBEDDING}\"\n\
         model = \"{OWW_MODEL}\"\n"
    )
}

/// The `[endpointer]` config block wiring the committed Silero model. Paired with
/// `oww_wake_block()` it stands up the streaming listener — both tables are
/// required for a listener to run, and the listener is the only utterance source.
fn endpointer_block() -> String {
    format!("[endpointer]\nmodel = \"{SILERO_MODEL}\"\n")
}

/// The `[wake]` + `[endpointer]` pair that stands up the full streaming listener
/// (real openWakeWord + Silero endpointer over the committed models). The wake
/// phrase arms openWakeWord and Silero onsets on it, so the endpointer carves the
/// utterance on its natural soft-endpoint path — the deterministic end-to-end
/// drive post-rework.
fn listener_block() -> String {
    format!("{}{}", oww_wake_block(), endpointer_block())
}

/// A harness daemon config running the streaming listener, with recording off
/// unless `record_dir` is given (then recording into it — where the sidecar wake
/// label is written).
pub fn listener_daemon_config(record_dir: Option<&Path>) -> String {
    daemon_config_with(record_dir, &listener_block())
}

/// The streaming listener plus a `wav` brain answering every carved utterance with
/// the clip at `clip`. Recording off unless `record_dir` is given. The listener
/// tables precede `[brain]` so the TOML is well-ordered.
pub fn listener_wav_brain_config(record_dir: Option<&Path>, clip: &Path) -> String {
    daemon_config_with(
        record_dir,
        &format!(
            "{}[brain]\n\
             mode = \"wav\"\n\
             clip = \"{}\"\n",
            listener_block(),
            clip.display(),
        ),
    )
}

/// A harness daemon config running the real openWakeWord gate over the committed
/// models, with recording off unless `record_dir` is given (then recording into
/// it — the only way an on-disk wake label is produced).
pub fn oww_daemon_config(record_dir: Option<&Path>) -> String {
    daemon_config_with(record_dir, &oww_wake_block())
}

// --- Fake speaches container for the parrot integration test -----------------

/// The transcript the fake speaches STT endpoint returns for every request — the
/// text `EchoBrain` reads back and the TTS then renders.
pub const FAKE_TRANSCRIPT: &str = "hello parrot";

/// A harness daemon config for parrot mode: the streaming listener (real oww +
/// Silero), an `echo` brain, and `[stt]`/`[tts]` both pointed at `speaches_url` —
/// the one-container speaches deployment the increment targets. Recording off.
/// The listener carves the utterance the STT then transcribes.
pub fn echo_parrot_config(speaches_url: &str) -> String {
    daemon_config_with(
        None,
        &format!(
            "{}[stt]\n\
             backend = \"http\"\n\
             url = \"{speaches_url}\"\n\
             model = \"test-whisper\"\n\
             [tts]\n\
             backend = \"http\"\n\
             url = \"{speaches_url}\"\n\
             model = \"test-tts\"\n\
             voice = \"test-voice\"\n\
             [brain]\n\
             mode = \"echo\"\n",
            listener_block(),
        ),
    )
}

/// An in-memory 16 kHz mono S16 WAV of `n` sample ramp — the fake TTS clip. A
/// recognizable ramp makes a mis-sized read visible; `n` is a non-multiple of
/// the 320-sample frame so the writer's ceiling framing is exercised end to end.
fn tts_wav_body(n: usize) -> Vec<u8> {
    let spec = hound::WavSpec {
        channels: 1,
        sample_rate: 16_000,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut cursor = std::io::Cursor::new(Vec::new());
    {
        let mut w = hound::WavWriter::new(&mut cursor, spec).expect("create tts wav writer");
        for i in 0..n {
            w.write_sample(i as i16).expect("write tts sample");
        }
        w.finalize().expect("finalize tts wav");
    }
    cursor.into_inner()
}

/// Spawn an in-process fake speaches container: a loopback HTTP server that
/// answers `POST /v1/audio/transcriptions` with `{"text": FAKE_TRANSCRIPT}` and
/// `POST /v1/audio/speech` with a `tts_samples`-sample 16 kHz mono S16 WAV — the
/// two endpoints the parrot pipeline calls, served from one port so the daemon's
/// `[stt]`/`[tts]` tables point at a single URL. Each request arrives on its own
/// connection (responses carry `Connection: close`). Returns the base URL; the
/// listener lives on a detached thread reaped at process exit.
pub fn spawn_fake_speaches(tts_samples: usize) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake speaches");
    let addr = listener.local_addr().expect("fake speaches addr");
    let tts_body = tts_wav_body(tts_samples);
    thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(mut s) => serve_fake_speaches(&mut s, &tts_body),
                Err(_) => break,
            }
        }
    });
    format!("http://{addr}")
}

/// Read one framed HTTP request, route on its path, and write the canned
/// response. A request to neither known endpoint gets a `404` so a mis-pathed
/// call fails loudly rather than being answered as if it hit the right route.
fn serve_fake_speaches(stream: &mut TcpStream, tts_body: &[u8]) {
    let req = read_http_request(stream);
    let head = String::from_utf8_lossy(&req);
    let request_line = head.lines().next().unwrap_or("");
    let (content_type, body): (&str, Vec<u8>) = if request_line.contains("/v1/audio/transcriptions")
    {
        (
            "application/json",
            format!("{{\"text\":\"{FAKE_TRANSCRIPT}\"}}").into_bytes(),
        )
    } else if request_line.contains("/v1/audio/speech") {
        ("audio/wav", tts_body.to_vec())
    } else {
        let resp = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let _ = stream.write_all(resp);
        let _ = stream.flush();
        return;
    };
    let mut resp = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    resp.extend_from_slice(&body);
    let _ = stream.write_all(&resp);
    let _ = stream.flush();
}

/// Read one HTTP request off `stream`: headers to the blank line, then the
/// `Content-Length` body. reqwest sends both the STT multipart and the TTS JSON
/// with a computed `Content-Length`, so framing on it reads each request whole.
fn read_http_request(stream: &mut TcpStream) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        let n = match stream.read(&mut tmp) {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        buf.extend_from_slice(&tmp[..n]);
        if let Some(hdr_end) = find_subslice(&buf, b"\r\n\r\n") {
            let content_len = parse_content_length(&buf[..hdr_end]);
            let body_start = hdr_end + 4;
            while buf.len() < body_start + content_len {
                let n = match stream.read(&mut tmp) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                buf.extend_from_slice(&tmp[..n]);
            }
            break;
        }
    }
    buf
}

/// Locate `needle` in `haystack` — used to find the header/body boundary.
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Parse the `Content-Length` value out of the request headers, `0` if absent.
fn parse_content_length(headers: &[u8]) -> usize {
    let text = String::from_utf8_lossy(headers);
    for line in text.lines() {
        let lower = line.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-length:") {
            return v.trim().parse().unwrap_or(0);
        }
    }
    0
}

/// Build a replayable `.framelog` from a committed `.wav` by running the real
/// `wav-import` bin, returning the output path inside `dir`. The clip becomes one
/// VAD segment carrying `segment_id` — the same live ingest path a captured log
/// takes, so the listener scores real synthesized frames.
pub fn import_wav_to_framelog(dir: &Path, wav: &Path, segment_id: u32) -> PathBuf {
    let out = dir.join("import.framelog");
    let status = Command::new(env!("CARGO_BIN_EXE_wav-import"))
        .arg("--input")
        .arg(wav)
        .arg("--output")
        .arg(&out)
        .arg("--segment-id")
        .arg(segment_id.to_string())
        .status()
        .expect("run wav-import");
    assert!(
        status.success(),
        "wav-import failed for {} ({status})",
        wav.display()
    );
    out
}

/// Run `segments-export` over one recorded framelog into `out_dir` (created if
/// absent), returning the finished process output. The offline export replays
/// the log through the live ingest path, so its per-segment `.wav` is byte-for-
/// byte what the daemon assembled.
pub fn run_segments_export(out_dir: &Path, framelog: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_segments-export"))
        .arg("--out-dir")
        .arg(out_dir)
        .arg(framelog)
        .output()
        .expect("run segments-export")
}

/// Block until the whole capture has drained through the segment path: the final
/// segment's `tracking` line, then the connection close — so every per-connection
/// and per-segment line is on disk before a snapshot assertion. Listener events
/// (`wake_detected`, `utterance`) surface asynchronously *after* the connection
/// closes; a scenario that expects one waits on it with [`drain_and_await_utterance`],
/// and a scenario that expects none shuts the daemon down first (`sigterm_and_wait`
/// joins the listener thread, making their absence deterministic).
pub fn wait_until_drained(daemon: &DaemonChild, last_segment_id: u32) {
    wait_for_event(daemon, "tracking (last segment)", EVENT_DEADLINE, |v| {
        v["event"] == "tracking" && v["segment_id"] == last_segment_id
    });
    wait_for_event(daemon, "conn_closed", EVENT_DEADLINE, |v| {
        v["event"] == "conn_closed"
    });
}

/// Drain the capture through the segment path, then wait for the listener's trailing
/// `utterance` line. The listener runs on its own thread behind the live feed, so the
/// carved utterance lands after the connection closes; waiting on it orders every
/// line onto disk before a snapshot.
pub fn drain_and_await_utterance(daemon: &DaemonChild, last_segment_id: u32) {
    wait_until_drained(daemon, last_segment_id);
    wait_for_event(daemon, "utterance", EVENT_DEADLINE, |v| {
        v["event"] == "utterance"
    });
}

/// The single `wake_detected` line the listener emits for an armed wake — score
/// above the configured threshold — or a panic carrying the daemon's diagnostics.
pub fn expect_wake_detected<'a>(events: &'a [Value], daemon: &DaemonChild) -> &'a Value {
    expect_one(events, "wake_detected", daemon)
}

// --- In-test fake pod for the barge-in end-to-end scenario --------------------

/// Pod identity the barge-in fake pod advertises in its `Hello`.
pub const BARGE_POD_ID: &str = "pod-barge";

/// Read a 16 kHz mono S16 `.wav` into a mono PCM buffer — the barge scenario's
/// speech source. Panics on any non-spine format or read error (the committed
/// clip is known-good, so a failure is a harness bug, not a runtime condition).
pub fn read_wav_pcm(path: &Path) -> Vec<i16> {
    let reader = hound::WavReader::open(path).expect("open wav");
    let spec = reader.spec();
    assert_eq!(spec.channels, 1, "spine wav is mono");
    assert_eq!(spec.sample_rate, 16_000, "spine wav is 16 kHz");
    reader
        .into_samples::<i16>()
        .collect::<Result<Vec<i16>, _>>()
        .expect("read wav samples")
}

/// The ordered wire frames of one VAD segment carrying `pcm`, built through the
/// real `synth_session` the daemon's ingest path decodes — `Hello` +
/// `SegmentStart` + paced `Audio` + `SegmentEnd`. `base_sample_index` places the
/// segment on the connection's continuous sample timeline so a second segment
/// follows the first without a gap.
pub fn session_frames(pcm: &[i16], segment_id: u32, base_sample_index: u64) -> Vec<StreamFrame> {
    let params = SynthParams {
        pod_id: BARGE_POD_ID.to_string(),
        sample_rate_hz: SPINE_FORMAT.sample_rate_hz,
        segment_id,
        base_sample_index,
        base_device_ts_us: base_sample_index * 1_000_000 / SPINE_FORMAT.sample_rate_hz as u64,
        preroll_samples: 0,
        channel_source: ChannelSource::AsrBeam,
    };
    synth_session(pcm, &params)
        .expect("synthesize session frames")
        .into_iter()
        .map(|f| f.frame)
        .collect()
}

/// Per-connection tally of the daemon's server→device (playback) frames the fake
/// pod's drain decoded, by variant. `flush` counts `FlushPlayback` frames — the
/// barge cut's device-side proof — which the general `replay-pod` drain buckets
/// as `other`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PlaybackTally {
    pub hello: u64,
    pub audio: u64,
    pub end_of_audio: u64,
    pub flush: u64,
    pub other: u64,
    pub decode_errors: u64,
}

/// Sticky signals from the drain thread to the test thread. `audio_seen` marks
/// the first playback `Audio` frame (the response is now audible — the barge
/// floor is open), `flush_seen` the first `FlushPlayback`, `exited` the drain's
/// return. All sticky (never cleared): a wait is a level check, so an event that
/// landed before the wait began still releases it.
#[derive(Default)]
struct PodFlags {
    audio_seen: bool,
    flush_seen: bool,
    exited: bool,
}

struct PodSignal {
    flags: Mutex<PodFlags>,
    cv: Condvar,
}

impl PodSignal {
    fn new() -> PodSignal {
        PodSignal {
            flags: Mutex::new(PodFlags::default()),
            cv: Condvar::new(),
        }
    }

    /// Wait until `pred` holds over the current flags or `timeout` elapses.
    /// Returns whether the predicate was satisfied (false on timeout).
    fn wait_for(&self, timeout: Duration, pred: impl Fn(&PodFlags) -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        let mut flags = self.flags.lock().expect("pod signal poisoned");
        loop {
            if pred(&flags) {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (guard, res) = self
                .cv
                .wait_timeout(flags, deadline - now)
                .expect("pod signal poisoned");
            flags = guard;
            if res.timed_out() && !pred(&flags) {
                return false;
            }
        }
    }
}

/// Decode the daemon's server→device stream to EOF, tallying frames by variant
/// and signalling the first `Audio` and first `FlushPlayback`. Mirrors the
/// `replay-pod` drain's `[u16 len][postcard]` framing, adding `FlushPlayback`
/// recognition the barge assertion turns on.
fn drain_playback(mut read: TcpStream, signal: Arc<PodSignal>) -> PlaybackTally {
    let mut tally = PlaybackTally::default();
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = match read.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => n,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        };
        buf.extend_from_slice(&chunk[..n]);
        let mut off = 0;
        while buf.len() - off >= 2 {
            let payload_len = u16::from_le_bytes([buf[off], buf[off + 1]]) as usize;
            let end = off + 2 + payload_len;
            if buf.len() < end {
                break;
            }
            match decode_frame(&buf[off..end]) {
                Ok(StreamFrame::Hello(_)) => tally.hello += 1,
                Ok(StreamFrame::Audio(_)) => {
                    tally.audio += 1;
                    let mut f = signal.flags.lock().expect("pod signal poisoned");
                    if !f.audio_seen {
                        f.audio_seen = true;
                        drop(f);
                        signal.cv.notify_all();
                    }
                }
                Ok(StreamFrame::EndOfAudio(_)) => tally.end_of_audio += 1,
                Ok(StreamFrame::FlushPlayback(_)) => {
                    tally.flush += 1;
                    let mut f = signal.flags.lock().expect("pod signal poisoned");
                    if !f.flush_seen {
                        f.flush_seen = true;
                        drop(f);
                        signal.cv.notify_all();
                    }
                }
                Ok(_) => tally.other += 1,
                Err(_) => tally.decode_errors += 1,
            }
            off = end;
        }
        buf.drain(..off);
    }
    let mut f = signal.flags.lock().expect("pod signal poisoned");
    f.exited = true;
    drop(f);
    signal.cv.notify_all();
    tally
}

/// A fake pod: a live TCP connection to the daemon that streams wire frames up
/// and drains the daemon's playback stream back on a thread. Unlike `replay-pod`
/// (verbatim replay, then FIN) it stays interactive, so the barge scenario can
/// inject the interrupting segment only once playback is audible.
pub struct FakePod {
    write: TcpStream,
    drain: Option<thread::JoinHandle<PlaybackTally>>,
    signal: Arc<PodSignal>,
}

impl FakePod {
    /// Connect to the daemon's ingest address and start draining its playback
    /// stream. `TCP_NODELAY` is set so injected frames are not Nagle-batched.
    pub fn connect(addr: &str) -> FakePod {
        let write = TcpStream::connect(addr).expect("connect fake pod");
        write.set_nodelay(true).ok();
        let read = write.try_clone().expect("clone read half");
        let signal = Arc::new(PodSignal::new());
        let sig = Arc::clone(&signal);
        let drain = thread::spawn(move || drain_playback(read, sig));
        FakePod {
            write,
            drain: Some(drain),
            signal,
        }
    }

    /// Encode and send each frame verbatim, in order.
    pub fn send_frames(&mut self, frames: &[StreamFrame]) {
        let mut buf = [0u8; MAX_FRAME_BYTES + 2];
        for frame in frames {
            let n = encode_frame(frame, &mut buf).expect("encode frame");
            self.write.write_all(&buf[..n]).expect("send frame");
        }
        self.write.flush().expect("flush frames");
    }

    /// Block until the daemon's playback stream carries its first `Audio` frame —
    /// the response is audible, so the barge floor is open. Returns whether it
    /// arrived within `timeout`.
    pub fn wait_playback_audio(&self, timeout: Duration) -> bool {
        self.signal.wait_for(timeout, |f| f.audio_seen)
    }

    /// Block until a `FlushPlayback` frame crosses the wire (the barge cut) or
    /// the drain closes. Returns whether a flush was observed within `timeout`.
    pub fn wait_flush(&self, timeout: Duration) -> bool {
        self.signal.wait_for(timeout, |f| f.flush_seen || f.exited);
        self.signal
            .flags
            .lock()
            .expect("pod signal poisoned")
            .flush_seen
    }

    /// FIN the write half and join the drain, returning its final playback tally.
    pub fn finish(mut self) -> PlaybackTally {
        let _ = self.write.shutdown(Shutdown::Write);
        self.drain
            .take()
            .expect("drain present")
            .join()
            .expect("join drain")
    }
}

impl Drop for FakePod {
    fn drop(&mut self) {
        // A panicking test drops the pod without `finish`; close the socket so
        // the daemon reads EOF and the drain thread exits rather than leaking.
        let _ = self.write.shutdown(Shutdown::Both);
        if let Some(h) = self.drain.take() {
            let _ = h.join();
        }
    }
}

/// After a clean shutdown, assert the on-disk sidecar labels `segment_id` with
/// `expected`. Locates the single recorded framelog, reads its sidecar, finds the
/// segment, and asserts its wake class — the post-shutdown check every recording
/// scenario runs, differing only in the expected class.
pub fn assert_sidecar_wake(record_dir: &Path, segment_id: u32, expected: WakeClass) {
    let log = find_one_framelog(record_dir);
    let sidecar = Sidecar::read(&speech_surface::sidecar_path(&log))
        .unwrap_or_else(|e| panic!("read sidecar for {}: {e}", log.display()));
    let seg = sidecar
        .segments
        .iter()
        .find(|s| s.segment_id == segment_id)
        .unwrap_or_else(|| {
            panic!(
                "sidecar missing segment {segment_id}: {:?}",
                sidecar.segments
            )
        });
    assert_eq!(
        seg.wake, expected,
        "sidecar wake class for segment {segment_id}"
    );
}
