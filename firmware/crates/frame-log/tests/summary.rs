//! The walker and the summary, on logs built frame by frame.

mod common;

use audio_pipeline::wide::{CaptureFrame, LossCause, StreamEndReason, WideFrame};
use audio_pipeline::wire::{ChannelSource, Codec, EndOfAudio, EndReason, Hello};
use common::{Cap, Log, PERIOD, audio, describe, hello, mono_pcm, seg_end, start, voice_log};
use frame_log::{Cause, HostMicros, Step, SummaryError, export_wav, summarize, walk, write_record};

#[test]
fn a_clean_six_channel_capture() {
    let mut cap = Cap::new(6, 16);
    for _ in 0..3 {
        cap.block(PERIOD, 0);
    }
    let s = cap.end(StreamEndReason::Stopped).summary();
    assert_eq!(s.tail.verdict, "ended");
    assert_eq!(s.tail.open_segment, None);
    assert_eq!(s.hello.channels, 6);
    let capture = s.capture.as_ref().unwrap();
    assert_eq!(capture.channel_names.len(), 6);
    assert_eq!(capture.period_frames, PERIOD);
    assert_eq!(s.record_id, None);
    assert_eq!(s.frames_delivered, 960);
    assert_eq!(s.frames_recorded, 960);
    assert_eq!(s.duration_frames_s, 0.06);
    assert_eq!(s.duration_clock_s, Some(0.06));
    assert_eq!(s.segments.len(), 1);
    assert_eq!(s.segments[0].not_sent_frames, 0);
    assert_eq!(s.segments[0].resent_frames, 0);
    assert_eq!(s.segments[0].end, Some(EndReason::StreamEnd));
    assert_eq!(s.segments[0].count_check, Some("match"));
    assert!(s.discontinuities.is_empty());
    assert_eq!(s.blocks, Some(3));
    assert_eq!(s.short_blocks, Some(vec![]));
    assert_eq!(s.late, Some(vec![]));
    assert_eq!(s.wall_steps, Some(vec![]));
    assert_eq!(s.end.as_ref().unwrap().reason, StreamEndReason::Stopped);

    let json = serde_json::to_value(&s).unwrap();
    assert_eq!(json["hello"]["codec"], "s16_le");
    assert_eq!(json["hello"]["channel_source"], "described");
    assert_eq!(json["end"]["reason"], "stopped");
    assert_eq!(json["segments"][0]["end"], "stream_end");
    assert_eq!(json["discontinuities"], serde_json::json!([]));
}

#[test]
fn a_two_channel_s32_capture_walks_back_its_pcm() {
    let mut cap = Cap::new(2, 32);
    cap.block(PERIOD, 0);
    cap.block(100, 0);
    let written = cap.pcm.clone();
    let log = cap.end(StreamEndReason::Stopped);
    let mut read = Vec::new();
    for step in walk(&log.path()).unwrap() {
        if let Step::Audio(a) = step.unwrap() {
            read.extend(a.pcm);
        }
    }
    assert_eq!(read, written);
    let s = log.summary();
    assert_eq!(s.frames_recorded, 420);
    assert_eq!(s.hello.bits_per_sample, 32);
    let short = s.short_blocks.unwrap();
    assert_eq!(short.len(), 1);
    assert_eq!((short[0].first_frame, short[0].frames), (320, 100));
}

#[test]
fn measured_and_unmeasured_losses_are_told_apart() {
    let mut cap = Cap::new(1, 16);
    cap.block(PERIOD, 0);
    cap.loss(LossCause::WriterBacklog, Some(640));
    cap.block(PERIOD, 0);
    cap.loss(LossCause::Overrun, None);
    cap.mono += 100_000;
    cap.block(PERIOD, 0);
    let s = cap.end(StreamEndReason::Stopped).summary();

    let d = &s.discontinuities;
    assert_eq!(d.len(), 2, "{d:?}");
    assert_eq!(d[0].kind, "loss");
    assert_eq!(d[0].cause, Some(Cause::Loss(LossCause::WriterBacklog)));
    assert_eq!((d[0].at_frame, d[0].frames), (320, Some(640)));
    assert_eq!(d[0].segment_id, Some(0));
    assert_eq!(d[0].clock_estimate_frames, None);
    assert_eq!(d[1].cause, Some(Cause::Loss(LossCause::Overrun)));
    assert_eq!((d[1].at_frame, d[1].frames), (1280, None));
    assert_eq!(d[1].segment_id, None);
    assert_eq!(d[1].clock_estimate_frames, Some(1600));
    let json = serde_json::to_value(&s).unwrap();
    assert_eq!(json["discontinuities"][0]["cause"], "writer_backlog");
    assert_eq!(json["discontinuities"][1]["cause"], "overrun");

    assert_eq!(s.segments.len(), 2);
    assert_eq!(s.segments[0].end, Some(EndReason::SourceLoss));
    assert_eq!(s.segments[1].base_frame, 1280);
    assert_eq!(s.segments[1].not_sent_frames, 0);
    assert_eq!(s.frames_delivered, 1600);
    assert_eq!(s.frames_recorded, 960);
}

#[test]
fn clock_estimates_before_the_first_block_and_with_no_audio_after() {
    let mut cap = Cap::new(1, 16);
    cap.mono += 50_000;
    cap.loss(LossCause::Suspend, None);
    cap.loss(LossCause::TransferError, None);
    cap.block(PERIOD, 0);
    cap.loss(LossCause::Overrun, None);
    let s = cap.end(StreamEndReason::StreamDied).summary();
    let est: Vec<_> = s
        .discontinuities
        .iter()
        .map(|d| d.clock_estimate_frames)
        .collect();
    assert_eq!(est, vec![Some(800), Some(0), None]);
    assert_eq!(s.end.unwrap().reason, StreamEndReason::StreamDied);
}

#[test]
fn late_runs_and_wall_steps() {
    let mut cap = Cap::new(1, 16);
    cap.block(PERIOD, 0);
    cap.block(PERIOD, PERIOD);
    cap.block(PERIOD, 2 * PERIOD);
    cap.block(PERIOD, 0);
    cap.wall_offset += 5_000;
    cap.block(PERIOD, PERIOD);
    let s = cap.end(StreamEndReason::Stopped).summary();
    let late = s.late.unwrap();
    assert_eq!(late.len(), 2);
    assert_eq!(
        (
            late[0].first_frame,
            late[0].blocks,
            late[0].max_backlog_frames
        ),
        (320, 2, 640)
    );
    assert_eq!(late[1].first_frame, 1280);
    let steps = s.wall_steps.unwrap();
    assert_eq!(steps.len(), 1);
    assert_eq!((steps[0].at_frame, steps[0].step_us), (1280, 5_000));
}

#[test]
fn a_gated_log_without_describe_reports_audio_not_sent() {
    let mut log = Log::new();
    voice_log(
        &mut log,
        &[
            (7, 1000, 640, EndReason::VadRelease),
            (8, 17_640, 320, EndReason::VadRelease),
        ],
    );
    let s = log.summary();
    assert!(s.capture.is_none() && s.blocks.is_none() && s.wall_steps.is_none());
    assert_eq!(s.tail.verdict, "unterminated");
    assert!(s.discontinuities.is_empty(), "{:?}", s.discontinuities);
    assert_eq!(s.segments[0].not_sent_frames, 0);
    assert_eq!(s.segments[1].not_sent_frames, 16_000);
    assert_eq!(s.frames_delivered, 16_960);
    assert_eq!(s.frames_recorded, 960);
    assert_eq!(s.duration_clock_s, Some(1.06));
}

#[test]
fn a_gated_capture_reads_like_a_gated_voice_log() {
    let mut cap = Cap::new(1, 16);
    cap.block(PERIOD, 0);
    let (id, sent) = cap.segment.take().unwrap();
    let stamp = cap.stamp();
    cap.log
        .frame(stamp, seg_end(id, sent, EndReason::VadRelease));
    cap.pos += 16_000;
    cap.mono += 1_000_000;
    cap.block(PERIOD, 0);
    let s = cap.end(StreamEndReason::Stopped).summary();
    assert!(s.discontinuities.is_empty(), "{:?}", s.discontinuities);
    assert_eq!(s.segments[1].not_sent_frames, 16_000);
    assert_eq!(s.tail.verdict, "ended");
}

#[test]
fn a_preroll_inside_the_previous_segment_is_resent_not_lost() {
    let mut log = Log::new();
    voice_log(
        &mut log,
        &[
            (1, 0, 3200, EndReason::VadRelease),
            (2, 2560, 1280, EndReason::VadRelease),
        ],
    );
    let s = log.summary();
    assert_eq!(s.segments[1].resent_frames, 640);
    assert_eq!(s.segments[1].not_sent_frames, 0);
    assert!(s.discontinuities.is_empty());
    assert_eq!(s.frames_delivered, 3840);
}

#[test]
fn a_resend_that_stops_short_does_not_count_sent_audio_as_not_sent() {
    let mut log = Log::new();
    log.frame(1, hello(1, 16, Codec::S16Le, ChannelSource::AsrBeam));
    log.frame(2, start(0, 0, 0));
    log.frame(3, audio(0, 0, 0, mono_pcm(0, 1000)));
    log.frame(4, seg_end(0, 1000, EndReason::VadRelease));
    log.frame(5, start(1, 800, 50_000));
    log.frame(6, audio(1, 800, 50_000, mono_pcm(800, 100)));
    log.frame(7, seg_end(1, 100, EndReason::VadRelease));
    log.frame(8, start(2, 1500, 93_750));
    log.frame(9, audio(2, 1500, 93_750, mono_pcm(1500, 100)));
    log.frame(10, seg_end(2, 100, EndReason::VadRelease));
    let s = log.summary();
    let gaps: Vec<_> = s
        .segments
        .iter()
        .map(|g| (g.not_sent_frames, g.resent_frames))
        .collect();
    assert_eq!(gaps, vec![(0, 0), (0, 200), (500, 0)]);
    assert!(s.discontinuities.is_empty(), "{:?}", s.discontinuities);
    assert_eq!(s.frames_delivered, 1600);
}

#[test]
fn a_measured_loss_before_the_first_block_is_delivered() {
    let mut cap = Cap::new(1, 16);
    cap.loss(LossCause::WriterBacklog, Some(640));
    cap.block(PERIOD, 0);
    let log = cap.end(StreamEndReason::Stopped);
    let s = log.summary();
    assert_eq!(s.segments[0].base_frame, 640);
    assert_eq!(s.segments[0].not_sent_frames, 0);
    assert_eq!(s.frames_delivered, 960);
    assert_eq!(s.frames_recorded, 320);
    assert_eq!(s.duration_frames_s, 0.06);
    assert_eq!(s.duration_clock_s, Some(0.06));
    let map = export_wav(&log.path(), &log.dir.path().join("out.wav")).unwrap();
    assert_eq!(map.wav_frames, s.frames_delivered);
}

#[test]
fn breaks_inside_a_segment_and_lossy_segment_ends() {
    let mut log = Log::new();
    voice_log(&mut log, &[(1, 0, 640, EndReason::Overrun)]);
    log.frame(5, start(2, 5000, 0));
    for first in [5000, 5400, 5300] {
        log.frame(6, audio(2, first, 0, vec![0; 200]));
    }
    log.frame(7, seg_end(2, 999, EndReason::VadRelease));
    log.frame(8, start(3, 6000, 0));
    log.frame(9, start(4, 7000, 0));
    let s = log.summary();
    let kinds: Vec<_> = s
        .discontinuities
        .iter()
        .map(|d| (d.kind, d.at_frame, d.frames))
        .collect();
    assert_eq!(
        kinds,
        vec![
            ("segment_end", 640, None),
            ("gap", 5100, Some(300)),
            ("overlap", 5500, Some(200)),
            ("count_mismatch", 5400, None),
            ("segment_replaced", 6000, None),
        ]
    );
    let end = &s.discontinuities[0];
    assert_eq!(end.cause, Some(Cause::SegmentEnd(EndReason::Overrun)));
    assert_eq!(end.detail, "segment closed with overrun");
    assert_eq!(
        serde_json::to_value(end).unwrap()["cause"],
        serde_json::to_value(s.segments[0].end).unwrap()
    );
    assert_eq!(s.segments[1].count_check, Some("mismatch"));
    assert_eq!(s.segments[2].end, None);
    assert_eq!(s.tail.open_segment, Some(4));
}

#[test]
fn a_capture_whose_index_outruns_its_loss_frames_shows_a_gap() {
    let mut cap = Cap::new(1, 16);
    cap.block(PERIOD, 0);
    cap.loss(LossCause::WriterBacklog, Some(320));
    cap.pos += 100;
    cap.block(PERIOD, 0);
    let s = cap.end(StreamEndReason::Stopped).summary();
    let kinds: Vec<_> = s
        .discontinuities
        .iter()
        .map(|d| (d.kind, d.at_frame, d.frames))
        .collect();
    assert_eq!(
        kinds,
        vec![("loss", 320, Some(320)), ("gap", 640, Some(100))]
    );
}

#[test]
fn torn_and_unterminated_tails_keep_what_precedes_them() {
    let mut cap = Cap::new(1, 16);
    cap.block(PERIOD, 0);
    let whole = cap.log.bytes.len() as u64;
    cap.block(PERIOD, 0);
    let mut log = cap.log;
    let s = log.summary();
    assert_eq!(s.tail.verdict, "unterminated");
    assert_eq!(s.tail.open_segment, Some(0));
    log.bytes.truncate(whole as usize + 5);
    let s = log.summary();
    assert_eq!((s.tail.verdict, s.tail.offset), ("torn", whole));
    assert_eq!(s.frames_recorded, 320);
}

fn corrupt_reason(build: impl FnOnce(&mut Log)) -> String {
    let mut log = Log::new();
    build(&mut log);
    let tail = match summarize(&log.path()) {
        Ok(s) => s.tail,
        Err(SummaryError::NoHello(tail)) => tail,
        Err(e) => panic!("{e}"),
    };
    assert_eq!(tail.verdict, "corrupt", "{tail:?}");
    tail.reason
}

type Case = (&'static str, Box<dyn FnOnce(&mut Log)>);

/// A mono S16 hello changed by `change`.
fn hello_with(change: impl FnOnce(&mut Hello)) -> WideFrame {
    let WideFrame::Hello(mut h) = hello(1, 16, Codec::S16Le, ChannelSource::AsrBeam) else {
        unreachable!()
    };
    change(&mut h);
    WideFrame::Hello(h)
}

#[test]
fn breaks_inside_a_segment_of_a_capture() {
    let mut cap = Cap::new(1, 16);
    cap.block(PERIOD, 0);
    cap.pos += 100;
    cap.block(PERIOD, 0);
    let (id, sent) = cap.segment.unwrap();
    let ts = cap.mono;
    cap.log.frame(cap.stamp(), audio(id, 500, ts, vec![0; 200]));
    cap.segment = Some((id, sent + 100));
    cap.pos = 600;
    let s = cap.end(StreamEndReason::Stopped).summary();
    let kinds: Vec<_> = s
        .discontinuities
        .iter()
        .map(|d| (d.kind, d.at_frame, d.frames, d.segment_id))
        .collect();
    assert_eq!(
        kinds,
        vec![
            ("gap", 320, Some(100), Some(0)),
            ("overlap", 740, Some(240), Some(0)),
        ]
    );
    assert_eq!(s.segments.len(), 1);
    assert_eq!(s.segments[0].count_check, Some("match"));
    assert_eq!(s.tail.verdict, "ended");
}

#[test]
fn each_corrupt_reason() {
    let described = || hello(1, 16, Codec::S16Le, ChannelSource::Described);
    let beam = || hello(1, 16, Codec::S16Le, ChannelSource::AsrBeam);
    let cases: Vec<Case> = vec![
        (
            "second hello",
            Box::new(move |l| {
                l.frame(0, beam());
                l.frame(0, beam());
            }),
        ),
        (
            "capture schema 2",
            Box::new(move |l| {
                l.frame(0, described());
                l.frame(0, describe(2, 1));
            }),
        ),
        (
            "describe names 2 channels",
            Box::new(move |l| {
                l.frame(0, described());
                l.frame(0, describe(1, 2));
            }),
        ),
        (
            "described channels with no describe",
            Box::new(move |l| {
                l.frame(0, described());
                l.frame(0, start(0, 0, 0));
            }),
        ),
        (
            "capture frame with no describe",
            Box::new(move |l| {
                l.frame(0, beam());
                l.frame(
                    0,
                    WideFrame::Capture(CaptureFrame::Loss {
                        cause: LossCause::Overrun,
                        at_index: 0,
                        samples: None,
                        mono_us: 0,
                        detail: String::new(),
                    }),
                );
            }),
        ),
        (
            "read names index 9",
            Box::new(move |l| {
                l.frame(0, described());
                l.frame(0, describe(1, 1));
                l.frame(
                    0,
                    WideFrame::Capture(CaptureFrame::Read {
                        first_sample_index: 9,
                        mono_us: 0,
                        backlog_frames: 0,
                    }),
                );
            }),
        ),
        (
            "at_index 5 where the position is 0",
            Box::new(move |l| {
                l.frame(0, described());
                l.frame(0, describe(1, 1));
                l.frame(0, start(0, 0, 0));
                l.frame(
                    0,
                    WideFrame::Capture(CaptureFrame::End {
                        reason: StreamEndReason::Stopped,
                        at_index: 5,
                        mono_us: 0,
                        detail: String::new(),
                    }),
                );
            }),
        ),
        (
            "not whole 2-byte frames",
            Box::new(move |l| {
                l.frame(0, beam());
                l.frame(0, start(0, 0, 0));
                l.frame(0, audio(0, 0, 0, vec![0; 3]));
            }),
        ),
        (
            "audio outside a segment",
            Box::new(move |l| {
                l.frame(0, beam());
                l.frame(0, audio(0, 0, 0, vec![]));
            }),
        ),
        (
            "unknown sample format",
            Box::new(move |l| {
                l.frame(0, hello(1, 24, Codec::S32Le, ChannelSource::AsrBeam));
            }),
        ),
        (
            "data after the end frame",
            Box::new(move |l| {
                l.frame(0, described());
                l.frame(0, describe(1, 1));
                l.frame(
                    0,
                    WideFrame::Capture(CaptureFrame::End {
                        reason: StreamEndReason::Stopped,
                        at_index: 0,
                        mono_us: 0,
                        detail: String::new(),
                    }),
                );
                l.frame(0, start(0, 0, 0));
            }),
        ),
        (
            "record length 0",
            Box::new(move |l| {
                l.frame(0, beam());
                l.bytes.extend_from_slice(&[0; 10]);
            }),
        ),
        (
            "a frame only the host sends",
            Box::new(move |l| {
                l.frame(0, beam());
                l.frame(0, WideFrame::EndOfAudio(EndOfAudio {}));
            }),
        ),
        (
            "describe not directly after the hello",
            Box::new(move |l| {
                l.frame(0, described());
                l.frame(0, describe(1, 1));
                l.frame(0, describe(1, 1));
            }),
        ),
        (
            "frame names segment 1 while segment 0 is open",
            Box::new(move |l| {
                l.frame(0, beam());
                l.frame(0, start(0, 0, 0));
                l.frame(0, audio(1, 0, 0, vec![0; 2]));
            }),
        ),
        (
            "frame names segment 3 while segment 0 is open",
            Box::new(move |l| {
                l.frame(0, beam());
                l.frame(0, start(0, 0, 0));
                l.frame(0, seg_end(3, 0, EndReason::VadRelease));
            }),
        ),
        (
            "segment end outside a segment",
            Box::new(move |l| {
                l.frame(0, beam());
                l.frame(0, seg_end(0, 0, EndReason::VadRelease));
            }),
        ),
        (
            "hello version 2 outside",
            Box::new(move |l| l.frame(0, hello_with(|h| h.version = 2))),
        ),
        (
            "hello with zero channels",
            Box::new(move |l| l.frame(0, hello_with(|h| h.channels = 0))),
        ),
        (
            "hello with a zero sample rate",
            Box::new(move |l| l.frame(0, hello_with(|h| h.sample_rate_hz = 0))),
        ),
        (
            "frame does not decode",
            Box::new(move |l| {
                l.frame(0, beam());
                write_record(&mut l.bytes, HostMicros(0), &[1, 0, 200]).unwrap();
            }),
        ),
        (
            "sample index past its limit",
            Box::new(move |l| {
                l.frame(0, beam());
                l.frame(0, start(0, u64::MAX, 0));
                l.frame(0, audio(0, u64::MAX, 0, vec![0; 2]));
            }),
        ),
    ];
    for (needle, build) in cases {
        let reason = corrupt_reason(build);
        assert!(reason.contains(needle), "{reason:?} lacks {needle:?}");
    }
}

#[test]
fn a_log_with_no_hello_is_refused() {
    let log = Log::new();
    assert!(matches!(
        summarize(&log.path()),
        Err(SummaryError::NoHello(t)) if t.verdict == "unterminated"
    ));
    let mut log = Log::new();
    log.frame(0, start(0, 0, 0));
    assert!(matches!(
        summarize(&log.path()),
        Err(SummaryError::NoHello(t)) if t.reason == "first frame is not a hello"
    ));
}
