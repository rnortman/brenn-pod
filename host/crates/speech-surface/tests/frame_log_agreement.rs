//! `frame-log`'s reader against the voice host's own: the walker and
//! `SessionFsm` agree on what a segment and an index jump are, on the committed
//! fixtures and on synthetic logs; the export places the same samples
//! `splice_log_into` does; and the voice host's readers refuse a capture.

mod common;

use std::path::{Path, PathBuf};

use audio_pipeline::wide::{CAPTURE_SCHEMA_VERSION, CaptureFrame, StreamEndReason, WideAudio};
use audio_pipeline::wide::{WideFrame, encode_wide};
use audio_pipeline::wire::{
    AUDIO_PROTOCOL_VERSION, ChannelSource, Codec, EndReason, Hello, SegmentEnd, SegmentStart,
    decode_frame,
};
use frame_log::{
    Basis, HostMicros, JumpKind, LogMeta, Step, WAV_HEADER_BYTES, export_wav, summarize, walk,
    write_header, write_record,
};
use pod_ingest::session::{CloseCause, ResumeLedger, SegmentClose, SessionEvent, SessionFsm};
use pod_ingest::{FrameLogReader, LogItem, SpliceStop, splice_log_into};
use speech_pipeline::{
    ListenerConfig, OwwConfig, OwwModels, SPINE_FORMAT, SileroConfig, SileroModel,
};
use speech_surface::replay::{ReplayListener, StopReason, replay_framelog};

const FIXTURES: [&str; 2] = ["short-capture.framelog", "short-capture-v3.framelog"];
const PERIOD: u64 = 320;

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

/// How a segment ended, as both readers can say it.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Close {
    End(EndReason),
    /// A `SegmentStart` arrived while it was open.
    Replaced,
}

/// What a reader makes of a log's segments and its index jumps inside them.
#[derive(Debug, Default, PartialEq)]
struct Reading {
    /// `(segment_id, base, how it closed)`, in order.
    segments: Vec<(u32, u64, Option<Close>)>,
    /// `(segment_id, expected index, the frame's index)`, in order.
    jumps: Vec<(u32, u64, u64)>,
}

/// The voice host's reading: every record through `decode_frame` and
/// `SessionFsm`, as `splice_log_into` and the live session read it.
fn session_reading(path: &Path) -> Reading {
    let mut fsm = SessionFsm::new(SPINE_FORMAT, ResumeLedger::shared());
    let mut r = Reading::default();
    for item in FrameLogReader::open(path).unwrap() {
        let LogItem::Record { host_rx, payload } = item.unwrap() else {
            panic!("a torn tail in {}", path.display());
        };
        for ev in fsm.feed(decode_frame(&payload).unwrap(), host_rx) {
            match ev {
                SessionEvent::SegmentOpened {
                    segment_id,
                    base_sample_index,
                    ..
                } => r.segments.push((segment_id, base_sample_index, None)),
                SessionEvent::Audio {
                    segment_id,
                    gap: Some(g),
                    ..
                } => r.jumps.push((segment_id, g.expected_index, g.got_index)),
                SessionEvent::SegmentClosed { close, .. } => {
                    let close = match close {
                        SegmentClose::Completed { end_reason, .. } => Close::End(end_reason),
                        SegmentClose::Truncated {
                            cause: CloseCause::ForcedByNewStart,
                        } => Close::Replaced,
                        other => panic!("unexpected close {other:?}"),
                    };
                    r.segments.last_mut().unwrap().2 = Some(close);
                }
                SessionEvent::ProtocolError { kind, fatal, .. } => {
                    assert!(!fatal, "fatal protocol error {kind:?}");
                }
                _ => {}
            }
        }
    }
    r
}

/// `frame-log`'s reading, from the walker.
fn walker_reading(path: &Path) -> Reading {
    let mut walk = walk(path).unwrap();
    let mut r = Reading::default();
    for step in walk.by_ref() {
        match step.unwrap() {
            Step::SegmentOpened { start, .. } => {
                r.segments
                    .push((start.segment_id, start.base_sample_index, None));
            }
            Step::Audio(a) => {
                if let Some(j) = a.jump {
                    let got = match j.kind {
                        JumpKind::Gap => j.expected + j.frames,
                        JumpKind::Overlap => j.expected - j.frames,
                    };
                    r.jumps.push((a.segment_id, j.expected, got));
                }
            }
            Step::SegmentClosed { end, .. } => {
                r.segments.last_mut().unwrap().2 = Some(Close::End(end.reason));
            }
            Step::SegmentReplaced { .. } => {
                r.segments.last_mut().unwrap().2 = Some(Close::Replaced);
            }
            _ => {}
        }
    }
    let verdict = walk.tail().unwrap().verdict.name();
    assert!(
        verdict == "unterminated" || verdict == "ended",
        "{}: {verdict}",
        path.display()
    );
    r
}

// ── Synthetic logs ──────────────────────────────────────────────────────────

/// A frame log built frame by frame from wide frames.
///
/// The builders below must produce what these tests rely on: the capture log
/// needs wide frames, capture frames and a `Described` hello, and the voice
/// logs need end reasons other than `VadRelease` and samples valued by their
/// index, so a resent pre-roll carries the values it had.
struct Log {
    bytes: Vec<u8>,
    dir: tempfile::TempDir,
    stamp: u64,
}

impl Log {
    fn new() -> Self {
        let meta = LogMeta {
            build_id: "test".to_owned(),
            created_epoch_us: HostMicros(1_700_000_000_000_000),
            conn_seq: 1,
            rolled_from: None,
            capture: None,
        };
        let mut bytes = Vec::new();
        write_header(&mut bytes, &meta).unwrap();
        Log {
            bytes,
            dir: tempfile::tempdir().unwrap(),
            stamp: 1_700_000_000_000_000,
        }
    }

    fn frame(&mut self, frame: WideFrame) -> &mut Self {
        let mut buf = vec![0u8; 65_537];
        let n = encode_wide(&frame, &mut buf).unwrap();
        self.stamp += 1_000;
        write_record(&mut self.bytes, HostMicros(self.stamp), &buf[..n]).unwrap();
        self
    }

    fn path(&self) -> PathBuf {
        let path = self.dir.path().join("a.framelog");
        std::fs::write(&path, &self.bytes).unwrap();
        path
    }
}

fn hello(channel_source: ChannelSource) -> WideFrame {
    WideFrame::Hello(Hello {
        version: AUDIO_PROTOCOL_VERSION,
        pod_id: heapless::String::try_from("pod-agree").unwrap(),
        sample_rate_hz: 16_000,
        bits_per_sample: 16,
        channels: 1,
        codec: Codec::S16Le,
        channel_source,
    })
}

fn start(segment_id: u32, base: u64) -> WideFrame {
    WideFrame::SegmentStart(SegmentStart {
        segment_id,
        base_sample_index: base,
        base_device_ts_us: base * 1_000_000 / 16_000,
        preroll_samples: 0,
    })
}

fn end(segment_id: u32, samples_sent: u64, reason: EndReason) -> WideFrame {
    WideFrame::SegmentEnd(SegmentEnd {
        segment_id,
        device_ts_us: 0,
        frames_sent: 0,
        samples_sent,
        reason,
    })
}

/// Mono audio of one period whose samples are their indices plus one, so a
/// sample resent at its index has the value it had and silence is never audio.
fn audio(segment_id: u32, first: u64) -> WideFrame {
    WideFrame::Audio(WideAudio {
        segment_id,
        first_sample_index: first,
        device_ts_us: first * 1_000_000 / 16_000,
        pcm: (first..first + PERIOD)
            .flat_map(|i| (i as i16 + 1).to_le_bytes())
            .collect(),
    })
}

/// A voice log: the hello, then each segment `(id, base, periods, reason)`
/// with its audio contiguous from its base and its end.
fn voice_log(segments: &[(u32, u64, u64, EndReason)]) -> Log {
    let mut log = Log::new();
    log.frame(hello(ChannelSource::AsrBeam));
    for &(id, base, periods, reason) in segments {
        log.frame(start(id, base));
        for k in 0..periods {
            log.frame(audio(id, base + k * PERIOD));
        }
        log.frame(end(id, periods * PERIOD, reason));
    }
    log
}

/// A segment with a forward jump inside it, then one with a backward jump.
fn gap_and_overlap_log() -> Log {
    let mut log = Log::new();
    log.frame(hello(ChannelSource::AsrBeam))
        .frame(start(0, 0))
        .frame(audio(0, 0))
        .frame(audio(0, 960))
        .frame(audio(0, 1280))
        .frame(end(0, 960, EndReason::VadRelease))
        .frame(start(1, 4000))
        .frame(audio(1, 4000))
        .frame(audio(1, 4160))
        .frame(end(1, 640, EndReason::VadRelease));
    log
}

/// A segment the ring lapped, then the next one past the lost audio.
fn overrun_log() -> Log {
    voice_log(&[
        (0, 0, 3, EndReason::Overrun),
        (1, 1600, 2, EndReason::VadRelease),
    ])
}

/// A gate that reopened within the pre-roll: segment 1's base lies inside
/// segment 0, so its first 320 samples are sent again.
fn preroll_log() -> Log {
    voice_log(&[
        (0, 0, 4, EndReason::VadRelease),
        (1, 960, 3, EndReason::VadRelease),
    ])
}

/// A `SegmentStart` while a segment is open.
fn replaced_log() -> Log {
    let mut log = Log::new();
    log.frame(hello(ChannelSource::AsrBeam))
        .frame(start(0, 0))
        .frame(audio(0, 0))
        .frame(start(1, 640))
        .frame(audio(1, 640))
        .frame(end(1, 320, EndReason::VadRelease));
    log
}

// ── The walker and SessionFsm agree ─────────────────────────────────────────

#[test]
fn the_walker_and_the_session_agree_on_the_fixtures() {
    for name in FIXTURES {
        let path = fixture(name);
        let walker = walker_reading(&path);
        assert_eq!(walker, session_reading(&path), "{name}");
        assert_eq!(
            walker.segments,
            [
                (1, 0, Some(Close::End(EndReason::VadRelease))),
                (2, 960, None)
            ],
            "{name}"
        );
        assert!(walker.jumps.is_empty(), "{name}");
    }
}

#[test]
fn the_walker_and_the_session_agree_on_a_gap_and_an_overlap() {
    let log = gap_and_overlap_log();
    let path = log.path();
    let walker = walker_reading(&path);
    assert_eq!(walker, session_reading(&path));
    assert_eq!(walker.jumps, [(0, 320, 960), (1, 4320, 4160)]);
}

#[test]
fn the_walker_and_the_session_agree_on_an_overrun_end() {
    let log = overrun_log();
    let path = log.path();
    let walker = walker_reading(&path);
    assert_eq!(walker, session_reading(&path));
    assert_eq!(
        walker.segments,
        [
            (0, 0, Some(Close::End(EndReason::Overrun))),
            (1, 1600, Some(Close::End(EndReason::VadRelease)))
        ]
    );
    assert!(walker.jumps.is_empty());
}

#[test]
fn the_walker_and_the_session_agree_on_a_pre_roll_inside_the_last_segment() {
    let log = preroll_log();
    let path = log.path();
    let walker = walker_reading(&path);
    assert_eq!(walker, session_reading(&path));
    assert_eq!(walker.segments[1].1, 960);
    assert!(walker.jumps.is_empty());
}

#[test]
fn the_walker_and_the_session_agree_on_a_replaced_segment() {
    let log = replaced_log();
    let path = log.path();
    let walker = walker_reading(&path);
    assert_eq!(walker, session_reading(&path));
    assert_eq!(
        walker.segments,
        [
            (0, 0, Some(Close::Replaced)),
            (1, 640, Some(Close::End(EndReason::VadRelease)))
        ]
    );
}

/// A mono S16 WAV's samples.
fn wav_samples(path: &Path) -> Vec<i16> {
    let bytes = std::fs::read(path).unwrap();
    bytes[WAV_HEADER_BYTES as usize..]
        .chunks_exact(2)
        .map(|b| i16::from_le_bytes([b[0], b[1]]))
        .collect()
}

#[test]
fn a_resent_pre_roll_exports_to_the_samples_the_splice_places() {
    let log = preroll_log();
    let path = log.path();
    let wav = log.dir.path().join("a.wav");
    let map = export_wav(&path, &wav).unwrap();
    assert_eq!(map.wav_frames, 1920);
    assert_eq!(map.resent.len(), 1);
    assert_eq!(
        (
            map.resent[0].segment_id,
            map.resent[0].at_frame,
            map.resent[0].frames
        ),
        (1, 960, 320)
    );

    let mut spliced = vec![0i16; 1920];
    let outcome = splice_log_into(
        FrameLogReader::open(&path).unwrap(),
        SPINE_FORMAT,
        0,
        &mut spliced,
    )
    .unwrap();
    assert_eq!(outcome.stopped, None);
    assert_eq!(outcome.protocol_errors, 0);

    let exported = wav_samples(&wav);
    assert_eq!(exported, spliced);
    let expected: Vec<i16> = (0..1920).map(|i| i as i16 + 1).collect();
    assert_eq!(exported, expected);
}

#[test]
fn each_fixture_summarizes_to_what_it_holds() {
    for (name, version) in [(FIXTURES[0], 4), (FIXTURES[1], 3)] {
        let s = summarize(&fixture(name)).unwrap();
        assert_eq!(s.hello.version, version, "{name}");
        assert_eq!(s.hello.channels, 1, "{name}");
        assert_eq!(s.hello.sample_rate_hz, 16_000, "{name}");
        assert_eq!(s.hello.bits_per_sample, 16, "{name}");
        assert!(s.capture.is_none(), "{name}");
        assert_eq!(s.record_id, None, "{name}");
        assert_eq!(s.frames_recorded, 1280, "{name}");
        assert_eq!(s.frames_delivered, 1280, "{name}");
        let segments: Vec<_> = s
            .segments
            .iter()
            .map(|g| {
                (
                    g.segment_id,
                    g.base_frame,
                    g.frames,
                    g.not_sent_frames,
                    g.resent_frames,
                )
            })
            .collect();
        assert_eq!(segments, [(1, 0, 960, 0, 0), (2, 960, 320, 0, 0)], "{name}");
        assert!(s.discontinuities.is_empty(), "{name}");
        assert_eq!(s.tail.verdict, "unterminated", "{name}");
        assert_eq!(s.tail.open_segment, Some(2), "{name}");
        assert_eq!(
            s.tail.offset,
            std::fs::metadata(fixture(name)).unwrap().len(),
            "{name}"
        );
    }
}

#[test]
fn each_fixture_exports_with_every_frame_accounted_for() {
    for name in FIXTURES {
        let dir = tempfile::tempdir().unwrap();
        let wav = dir.path().join("f.wav");
        let map = export_wav(&fixture(name), &wav).unwrap();
        let audio: u64 = map.runs.iter().map(|r| r.frames).sum();
        let silence: u64 = map.holes.iter().map(|h| h.filled_frames).sum();
        assert_eq!(audio + silence, map.wav_frames, "{name}");
        assert_eq!(audio, 1280, "{name}");
        assert!(
            map.holes.iter().all(|h| h.basis != Basis::Unknown),
            "{name}"
        );
        assert!(map.resent.is_empty(), "{name}");
        assert_eq!(wav_samples(&wav).len() as u64, map.wav_frames, "{name}");
        assert_eq!(map.tail.verdict, "unterminated", "{name}");
    }
}

/// A capture's log: a described hello, its `Describe`, one segment with an
/// audio frame and its `Read`, and the end.
fn capture_log() -> Log {
    let mut log = Log::new();
    log.frame(hello(ChannelSource::Described))
        .frame(WideFrame::Capture(CaptureFrame::Describe {
            schema: CAPTURE_SCHEMA_VERSION,
            channel_names: vec!["asr_beam".to_owned()],
            period_frames: PERIOD as u32,
            mono_us: 1_000_000,
        }))
        .frame(start(0, 0))
        .frame(audio(0, 0))
        .frame(WideFrame::Capture(CaptureFrame::Read {
            first_sample_index: 0,
            mono_us: 1_020_000,
            backlog_frames: 0,
        }))
        .frame(end(0, PERIOD, EndReason::StreamEnd))
        .frame(WideFrame::Capture(CaptureFrame::End {
            reason: StreamEndReason::Stopped,
            at_index: PERIOD,
            mono_us: 1_040_000,
            detail: String::new(),
        }));
    log
}

#[test]
fn the_capture_log_is_a_whole_capture_to_the_reader_tool() {
    let log = capture_log();
    let s = summarize(&log.path()).unwrap();
    assert_eq!(s.tail.verdict, "ended");
    assert_eq!(s.frames_recorded, PERIOD);
    assert!(s.discontinuities.is_empty());
}

#[test]
fn splice_refuses_a_capture_and_places_no_audio() {
    let log = capture_log();
    let path = log.path();
    let mut out = vec![0i16; PERIOD as usize];
    let outcome = splice_log_into(
        FrameLogReader::open(&path).unwrap(),
        SPINE_FORMAT,
        0,
        &mut out,
    )
    .unwrap();
    assert_eq!(outcome.stopped, Some(SpliceStop::ProtocolFatal));
    assert_eq!(outcome.samples_written, 0);
    assert!(out.iter().all(|&s| s == 0));
}

#[test]
fn replay_refuses_a_capture_and_hears_nothing() {
    let log = capture_log();
    let path = log.path();
    let config = ListenerConfig::default();
    let oww = OwwModels::load(&OwwConfig {
        melspectrogram: common::OWW_MELSPECTROGRAM.into(),
        embedding: common::OWW_EMBEDDING.into(),
        model: common::OWW_MODEL.into(),
        threshold: config.oww_threshold,
    })
    .unwrap();
    let silero = SileroModel::load(&SileroConfig {
        model: common::SILERO_MODEL.into(),
    })
    .unwrap();
    let mut listener = ReplayListener::new(oww, silero, config);
    let summary = replay_framelog(&path, &mut listener, 1).unwrap();
    assert_eq!(summary.stop, StopReason::ProtocolError);
    assert_eq!(summary.records, 1);
    assert!(summary.events.is_empty(), "{:?}", summary.events);
}
