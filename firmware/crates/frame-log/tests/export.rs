//! The WAV export, on logs built frame by frame.

mod common;

use std::path::{Path, PathBuf};

use audio_pipeline::wide::{LossCause, StreamEndReason};
use audio_pipeline::wire::{ChannelSource, Codec, EndReason};
use common::{Cap, Log, PERIOD, RATE, audio, hello, mono_pcm, seg_end, start, voice_log};
use frame_log::{
    Basis, ExportError, ExportMap, Hole, Resent, Run, SummaryError, WAV_HEADER_BYTES, export_wav,
};

fn out(log: &Log) -> PathBuf {
    log.dir.path().join("out.wav")
}

fn export(log: &Log) -> (ExportMap, Vec<u8>) {
    let out = out(log);
    let map = export_wav(&log.path(), &out).unwrap();
    (map, std::fs::read(&out).unwrap())
}

fn u16_at(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(b[at..at + 2].try_into().unwrap())
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}

/// Check the WAV header against the format and return the data.
fn data(wav: &[u8], channels: u16, bits: u16) -> &[u8] {
    let header = WAV_HEADER_BYTES as usize;
    assert_eq!(&wav[0..4], b"RIFF");
    assert_eq!(u32_at(wav, 4) as usize, wav.len() - 8);
    assert_eq!(&wav[8..16], b"WAVEfmt ");
    assert_eq!(u16_at(wav, 20), 0xfffe);
    assert_eq!(u16_at(wav, 22), channels);
    assert_eq!(u32_at(wav, 24), RATE);
    let block_align = channels * bits / 8;
    assert_eq!(u32_at(wav, 28), RATE * u32::from(block_align));
    assert_eq!(u16_at(wav, 32), block_align);
    assert_eq!(u16_at(wav, 34), bits);
    assert_eq!(u16_at(wav, 38), bits);
    assert_eq!(u32_at(wav, 40), 0, "no channel mask");
    assert_eq!(&wav[60..64], b"data");
    assert_eq!(u32_at(wav, 64) as usize, wav.len() - header);
    &wav[header..]
}

fn run(wav_frame: u64, first_frame: u64, frames: u64) -> Run {
    Run {
        wav_frame,
        first_frame,
        frames,
    }
}

fn hole(wav_frame: u64, at_frame: u64, filled_frames: u64, basis: Basis) -> Hole {
    Hole {
        wav_frame,
        at_frame,
        filled_frames,
        basis,
    }
}

#[test]
fn a_clean_capture_is_its_pcm() {
    let mut cap = Cap::new(6, 16);
    for _ in 0..3 {
        cap.block(PERIOD, 0);
    }
    let pcm = cap.pcm.clone();
    let (map, wav) = export(&cap.end(StreamEndReason::Stopped));
    assert_eq!(map.wav_frames, 960);
    assert_eq!(map.runs, vec![run(0, 0, 960)]);
    assert!(map.holes.is_empty() && map.resent.is_empty());
    assert_eq!(map.tail.verdict, "ended");
    assert_eq!(data(&wav, 6, 16), pcm);
}

#[test]
fn an_s32_capture_keeps_its_width() {
    let mut cap = Cap::new(2, 32);
    cap.block(PERIOD, 0);
    cap.block(100, 0);
    let pcm = cap.pcm.clone();
    let (map, wav) = export(&cap.end(StreamEndReason::Stopped));
    assert_eq!(map.wav_frames, 420);
    assert_eq!(data(&wav, 2, 32), pcm);
}

#[test]
fn measured_estimated_and_unknown_holes() {
    let mut cap = Cap::new(1, 16);
    cap.block(PERIOD, 0);
    cap.loss(LossCause::WriterBacklog, Some(640));
    cap.block(PERIOD, 0);
    cap.loss(LossCause::Overrun, None);
    cap.mono += 100_000;
    cap.block(PERIOD, 0);
    cap.loss(LossCause::Overrun, None);
    let pcm = cap.pcm.clone();
    let (map, wav) = export(&cap.end(StreamEndReason::StreamDied));
    assert_eq!(
        map.holes,
        vec![
            hole(320, 320, 640, Basis::Measured),
            hole(1280, 1280, 1600, Basis::ClockEstimate),
            hole(3200, 1600, 0, Basis::Unknown),
        ]
    );
    assert_eq!(
        map.runs,
        vec![run(0, 0, 320), run(960, 960, 320), run(2880, 1280, 320)]
    );
    assert_eq!(map.wav_frames, 3200);
    let d = data(&wav, 1, 16);
    let block = 2 * PERIOD as usize;
    assert_eq!(&d[..block], &pcm[..block]);
    assert!(d[block..3 * block].iter().all(|&b| b == 0));
    assert_eq!(&d[3 * block..4 * block], &pcm[block..2 * block]);
    assert!(d[4 * block..9 * block].iter().all(|&b| b == 0));
    assert_eq!(&d[9 * block..], &pcm[2 * block..]);
}

#[test]
fn audio_not_sent_and_a_jump_inside_a_segment_in_a_capture() {
    let mut cap = Cap::new(1, 16);
    cap.block(PERIOD, 0);
    cap.close(EndReason::VadRelease);
    cap.pos += 16_000;
    cap.mono += 1_000_000;
    cap.block(PERIOD, 0);
    cap.pos += 100;
    cap.block(PERIOD, 0);
    let (map, wav) = export(&cap.end(StreamEndReason::Stopped));
    assert_eq!(
        map.holes,
        vec![
            hole(320, 320, 16_000, Basis::NotSent),
            hole(16_640, 16_640, 100, Basis::Measured),
        ]
    );
    assert_eq!(
        map.runs,
        vec![
            run(0, 0, 320),
            run(16_320, 16_320, 320),
            run(16_740, 16_740, 320)
        ]
    );
    assert_eq!(map.wav_frames, 17_060);
    assert_eq!(data(&wav, 1, 16).len(), 17_060 * 2);
}

#[test]
fn a_voice_log_with_an_overrun_end_and_a_closed_gate() {
    let mut log = Log::new();
    voice_log(
        &mut log,
        &[
            (1, 0, 640, EndReason::Overrun),
            (2, 16_000, 320, EndReason::VadRelease),
        ],
    );
    let (map, wav) = export(&log);
    assert_eq!(
        map.holes,
        vec![
            hole(640, 640, 0, Basis::ClockEstimate),
            hole(640, 640, 15_360, Basis::NotSent),
        ]
    );
    assert_eq!(map.runs, vec![run(0, 0, 640), run(16_000, 16_000, 320)]);
    assert_eq!(map.tail.verdict, "unterminated");
    let d = data(&wav, 1, 16);
    assert_eq!(&d[..1280], mono_pcm(0, 640));
    assert_eq!(&d[32_000..], mono_pcm(16_000, 320));
}

#[test]
fn a_resent_preroll_is_written_once() {
    let mut log = Log::new();
    voice_log(
        &mut log,
        &[
            (1, 0, 3200, EndReason::VadRelease),
            (2, 2560, 1280, EndReason::VadRelease),
        ],
    );
    let (map, wav) = export(&log);
    assert_eq!(
        map.resent,
        vec![Resent {
            segment_id: 2,
            at_frame: 2560,
            frames: 640
        }]
    );
    assert!(map.holes.is_empty());
    assert_eq!(map.runs, vec![run(0, 0, 3840)]);
    assert_eq!(data(&wav, 1, 16), mono_pcm(0, 3840));
}

#[test]
fn a_resend_that_ends_inside_an_audio_frame_is_cut_there() {
    let mut log = Log::new();
    voice_log(&mut log, &[(1, 0, 640, EndReason::VadRelease)]);
    log.frame(5, start(2, 480, 30_000));
    log.frame(6, audio(2, 480, 30_000, mono_pcm(480, PERIOD)));
    let (map, wav) = export(&log);
    assert_eq!(map.resent[0].frames, 160);
    assert_eq!(map.runs, vec![run(0, 0, 800)]);
    assert_eq!(data(&wav, 1, 16), mono_pcm(0, 800));
}

/// A voice log whose first segment holds `[0, 1000)`, followed by a second
/// segment at `base` holding `frames`, each `(first, frames)`.
fn after_a_thousand(base: u64, frames: &[(u64, u32)]) -> Log {
    let mut log = Log::new();
    log.frame(1, hello(1, 16, Codec::S16Le, ChannelSource::AsrBeam));
    log.frame(2, start(0, 0, 0));
    log.frame(3, audio(0, 0, 0, mono_pcm(0, 1000)));
    log.frame(4, seg_end(0, 1000, EndReason::VadRelease));
    let ts = base * 1_000_000 / u64::from(RATE);
    log.frame(5, start(1, base, ts));
    let mut sent = 0;
    for &(first, n) in frames {
        log.frame(6, audio(1, first, ts, mono_pcm(first, n)));
        sent += u64::from(n);
    }
    log.frame(7, seg_end(1, sent, EndReason::VadRelease));
    log
}

#[test]
fn audio_not_sent_after_a_resend_that_stops_short_is_measured_from_the_furthest_index() {
    let mut log = after_a_thousand(800, &[(800, 100)]);
    log.frame(8, start(2, 1500, 93_750));
    log.frame(9, audio(2, 1500, 93_750, mono_pcm(1500, 100)));
    log.frame(10, seg_end(2, 100, EndReason::VadRelease));
    let (map, wav) = export(&log);
    assert_eq!(map.holes, vec![hole(1000, 1000, 500, Basis::NotSent)]);
    assert_eq!(map.runs, vec![run(0, 0, 1000), run(1500, 1500, 100)]);
    assert_eq!(
        map.resent,
        vec![Resent {
            segment_id: 1,
            at_frame: 800,
            frames: 200
        }]
    );
    assert_eq!(map.wav_frames, 1600);
    let mut expected = mono_pcm(0, 1000);
    expected.extend(std::iter::repeat_n(0, 1000));
    expected.extend(mono_pcm(1500, 100));
    assert_eq!(data(&wav, 1, 16), expected);
}

#[test]
fn a_gap_inside_a_resent_stretch_fills_only_what_the_wav_does_not_hold() {
    let (map, wav) = export(&after_a_thousand(
        600,
        &[(600, 100), (800, 100), (900, 200)],
    ));
    assert!(map.holes.is_empty(), "{:?}", map.holes);
    assert_eq!(map.runs, vec![run(0, 0, 1100)]);
    assert_eq!(data(&wav, 1, 16), mono_pcm(0, 1100));

    let (map, wav) = export(&after_a_thousand(600, &[(600, 100), (1050, 50)]));
    assert_eq!(map.holes, vec![hole(1000, 1000, 50, Basis::Measured)]);
    assert_eq!(map.runs, vec![run(0, 0, 1000), run(1050, 1050, 50)]);
    let mut expected = mono_pcm(0, 1000);
    expected.extend(std::iter::repeat_n(0, 100));
    expected.extend(mono_pcm(1050, 50));
    assert_eq!(data(&wav, 1, 16), expected);
}

fn refused(log: &Log) -> ExportError {
    let out = out(log);
    let err = export_wav(&log.path(), &out).unwrap_err();
    assert!(!out.exists(), "{err}");
    err
}

#[test]
fn an_overlap_inside_a_segment_is_refused_with_its_position() {
    let mut log = Log::new();
    voice_log(&mut log, &[(1, 0, 640, EndReason::VadRelease)]);
    log.frame(5, start(2, 5000, 0));
    log.frame(6, audio(2, 5000, 0, vec![0; 200]));
    log.frame(6, audio(2, 5000, 0, vec![0; 200]));
    let err = refused(&log);
    assert!(
        matches!(
            err,
            ExportError::Overlap {
                segment_id: 2,
                at_frame: 5100,
                frames: 100
            }
        ),
        "{err:?}"
    );
    assert!(err.to_string().contains("index 5100"), "{err}");
}

#[test]
fn an_export_past_the_riff_limit_is_refused() {
    let mut log = Log::new();
    voice_log(
        &mut log,
        &[
            (1, 0, 320, EndReason::VadRelease),
            (2, 1 << 31, 320, EndReason::VadRelease),
        ],
    );
    assert!(matches!(refused(&log), ExportError::TooLarge { .. }));
}

#[test]
fn a_log_with_no_hello_is_refused() {
    let log = Log::new();
    assert!(matches!(
        refused(&log),
        ExportError::Read(SummaryError::NoHello(_))
    ));
}

#[test]
fn an_existing_output_is_left_alone() {
    let mut cap = Cap::new(1, 16);
    cap.block(PERIOD, 0);
    let log = cap.end(StreamEndReason::Stopped);
    let out: &Path = &out(&log);
    std::fs::write(out, b"mine").unwrap();
    let err = export_wav(&log.path(), out).unwrap_err();
    assert!(
        matches!(&err, ExportError::Output(e) if e.kind() == std::io::ErrorKind::AlreadyExists),
        "{err:?}"
    );
    assert_eq!(std::fs::read(out).unwrap(), b"mine");
}

#[test]
fn a_torn_log_exports_what_precedes_the_tear() {
    let mut cap = Cap::new(1, 16);
    cap.block(PERIOD, 0);
    let whole = cap.log.bytes.len();
    cap.block(PERIOD, 0);
    let pcm = cap.pcm[..2 * PERIOD as usize].to_vec();
    let mut log = cap.log;
    log.bytes.truncate(whole + 5);
    let (map, wav) = export(&log);
    assert_eq!(map.tail.verdict, "torn");
    assert_eq!(map.wav_frames, 320);
    assert_eq!(data(&wav, 1, 16), pcm);
}
