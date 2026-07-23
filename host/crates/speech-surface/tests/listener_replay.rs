//! Offline listener-replay harness tests: drive a captured frame log through the
//! streaming listener in-process (no device, no daemon, no socket) via
//! `speech_surface::replay`, and assert the wake + carved-utterance it produces.
//!
//! This is the design's framelog-corpus replay row and the deafness-bug
//! regression harness. The wake-phrase clip is `wav-import`ed to a frame log and
//! replayed through a fresh listener over the committed openWakeWord + Silero
//! models: openWakeWord arms on the phrase, Silero onsets on the speech, and the
//! trailing silence soft-endpoints it into a carved utterance — the same
//! deterministic end-to-end drive the live integration suite uses, minus the
//! daemon and TCP round trip.

mod common;

use std::path::{Path, PathBuf};

use audio_pipeline::wire::{ChannelSource, MAX_FRAME_BYTES, StreamFrame, encode_frame};
use pod_ingest::{FrameLogWriter, HostMicros, LogMeta, SynthParams, synth_session};
use speech_pipeline::{
    EndpointCause, ListenerConfig, ListenerEvent, OwwConfig, OwwModels, SPINE_FORMAT, SileroConfig,
    SileroModel,
};
use speech_surface::replay::{ReplayListener, StopReason, replay_framelog};

/// Load a `ReplayListener` over the committed models at the default threshold.
fn committed_listener() -> ReplayListener {
    let oww = OwwModels::load(&OwwConfig {
        melspectrogram: common::OWW_MELSPECTROGRAM.into(),
        embedding: common::OWW_EMBEDDING.into(),
        model: common::OWW_MODEL.into(),
        threshold: 0.5,
    })
    .expect("load oww models");
    let silero = SileroModel::load(&SileroConfig {
        model: common::SILERO_MODEL.into(),
    })
    .expect("load silero model");
    let config = ListenerConfig {
        oww_threshold: 0.5,
        ..ListenerConfig::default()
    };
    ReplayListener::new(oww, silero, config)
}

/// The whole point: a wake-phrase capture replayed through the listener detects
/// the wake and carves an utterance — with no device in the loop.
#[test]
fn wake_phrase_framelog_replays_to_wake_and_carve() {
    let dir = tempfile::tempdir().expect("tempdir");
    let framelog =
        common::import_wav_to_framelog(dir.path(), Path::new(common::WAKE_PHRASE_WAV), 1);

    let mut listener = committed_listener();
    let summary = replay_framelog(&framelog, &mut listener, 1).expect("replay");

    assert_eq!(
        summary.stop,
        StopReason::Eof,
        "the synthesized log ends cleanly after its SegmentEnd"
    );
    assert!(summary.records > 0, "records were read from the log");

    let wake_score = summary
        .events
        .iter()
        .find_map(|e| match e {
            ListenerEvent::WakeDetected { score, .. } => Some(*score),
            _ => None,
        })
        .expect("openWakeWord armed on the wake phrase");
    assert!(
        wake_score > 0.5,
        "wake score {wake_score} above the 0.5 threshold"
    );

    let utterance = summary
        .events
        .iter()
        .find_map(|e| match e {
            ListenerEvent::SoftEndpoint { utterance, .. } => Some(utterance),
            _ => None,
        })
        .expect("the endpointer carved an utterance");
    assert_eq!(
        utterance.cause,
        EndpointCause::SoftEndpoint,
        "Silero onsets on the phrase and its trailing silence soft-endpoints it, so the \
         carve is the natural path — not the device-release fallback"
    );
    assert!(!utterance.pcm.is_empty(), "the carved utterance has audio");
    assert!(
        utterance.wake.is_some(),
        "a wake-gated carve carries its wake provenance"
    );
}

/// Silence (no wake phrase) arms nothing and carves nothing — the harness does not
/// manufacture events, so a negative capture stays negative.
#[test]
fn silence_framelog_replays_to_no_wake_no_utterance() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wav = dir.path().join("silence.wav");
    write_silence_wav(&wav, 16_000); // 1 s of digital silence
    let framelog = common::import_wav_to_framelog(dir.path(), &wav, 1);

    let mut listener = committed_listener();
    let summary = replay_framelog(&framelog, &mut listener, 1).expect("replay");

    assert!(
        !summary
            .events
            .iter()
            .any(|e| matches!(e, ListenerEvent::WakeDetected { .. })),
        "silence arms no wake"
    );
    assert!(
        !summary
            .events
            .iter()
            .any(|e| matches!(e, ListenerEvent::SoftEndpoint { .. })),
        "silence carves no utterance"
    );
}

/// The deafness-panic reproduction at the replay level: two back-to-back
/// transport segments whose prerolls overlap. The device stamps a segment's
/// preroll with the samples' original capture indexes, so a segment opening less
/// than one preroll after the previous close re-anchors *behind* the last
/// delivered sample. This used to trip the PCM ring's overlap assert and kill the
/// listener thread outright — every pod deaf until restart. The ring now dedupes
/// the re-sent prefix, so the replay runs to EOF and still finds the wake.
///
/// Synthesized rather than captured: the frame-level shape (a `SegmentStart` whose
/// `base_sample_index` reaches back into the previous segment's audio) is exactly
/// what the live logs showed, and it is expressible on the wire without hardware.
#[test]
fn overlapping_segment_prerolls_replay_without_killing_the_listener() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pcm = read_wav(Path::new(common::WAKE_PHRASE_WAV));
    // Segment 1 carries the whole phrase from index 0. Segment 2 opens 8 000
    // samples (500 ms) behind segment 1's end, re-sending that tail as its preroll
    // — the close-to-open gap was shorter than the preroll.
    let overlap = 8_000_u64;
    let base2 = pcm.len() as u64 - overlap;
    let framelog = write_two_segment_framelog(dir.path(), &pcm, base2, overlap);

    let mut listener = committed_listener();
    let summary = replay_framelog(&framelog, &mut listener, 1).expect("replay");

    // The premise first: without an actual overlapping push this is just a "replay
    // reaches EOF" test that can never fail for the reason it was written. Fixture
    // drift, or a harness that starts resetting on `SegmentStart`, zeroes this.
    assert_eq!(
        summary.overlap_trimmed_samples, overlap,
        "segment 2's preroll must re-send exactly segment 1's {overlap}-sample tail — \
         the overlap this test exists to survive"
    );
    assert_eq!(
        summary.stop,
        StopReason::Eof,
        "the listener survives the overlapping preroll and reads to EOF"
    );
    assert!(
        summary
            .events
            .iter()
            .any(|e| matches!(e, ListenerEvent::WakeDetected { .. })),
        "the wake in segment 1 is still detected across the boundary"
    );
    assert!(
        summary
            .events
            .iter()
            .any(|e| matches!(e, ListenerEvent::SoftEndpoint { .. })),
        "and it still carves an utterance"
    );
}

/// Write a frame log of two segments: the phrase at `[0, pcm.len())`, then a
/// successor whose `base_sample_index` is `base2` with `preroll` samples of
/// re-sent audio — the samples keep their original capture indexes, so the second
/// segment's audio overlaps the first's. The successor's `Hello` is dropped: this
/// is one connection with two segments, not a reconnect (which would reset the
/// listener and erase the overlap).
fn write_two_segment_framelog(dir: &Path, pcm: &[i16], base2: u64, preroll: u64) -> PathBuf {
    let params = |segment_id: u32, base_sample_index: u64, preroll_samples: u32| SynthParams {
        pod_id: "pod-x".to_string(),
        sample_rate_hz: SPINE_FORMAT.sample_rate_hz,
        segment_id,
        base_sample_index,
        base_device_ts_us: 0,
        preroll_samples,
        channel_source: ChannelSource::AsrBeam,
    };
    let first = synth_session(pcm, &params(1, 0, 0)).expect("synth segment 1");
    // Segment 2's audio is the re-sent tail plus a little fresh silence.
    let mut second_pcm = pcm[base2 as usize..].to_vec();
    second_pcm.extend(std::iter::repeat_n(0_i16, 1_600));
    let second = synth_session(&second_pcm, &params(2, base2, preroll as u32))
        .expect("synth segment 2")
        .into_iter()
        .filter(|sf| !matches!(sf.frame, StreamFrame::Hello(_)));

    let out = dir.join("overlap.framelog");
    let meta = LogMeta {
        build_id: "test".to_string(),
        created_epoch_us: HostMicros(0),
        conn_seq: 1,
        rolled_from: None,
    };
    let mut writer = FrameLogWriter::create(&out, meta).expect("create frame log");
    let mut buf = [0u8; MAX_FRAME_BYTES + 2];
    // Segment 2's frames are stamped after segment 1's, matching the real capture:
    // the host received them later even though they re-carry earlier samples.
    let offset2 = first.last().map(|sf| sf.host_rx_offset_us).unwrap_or(0);
    for (sf, base_us) in first
        .iter()
        .map(|sf| (sf, 0))
        .chain(second.collect::<Vec<_>>().iter().map(|sf| (sf, offset2)))
    {
        let n = encode_frame(&sf.frame, &mut buf).expect("encode frame");
        writer
            .append(HostMicros(base_us + sf.host_rx_offset_us), &buf[..n])
            .expect("append frame");
    }
    writer.finish().expect("finish frame log");
    out
}

/// Read a 16 kHz mono S16 `.wav` into PCM.
fn read_wav(path: &Path) -> Vec<i16> {
    let mut reader = hound::WavReader::open(path).expect("open wav");
    reader
        .samples::<i16>()
        .collect::<Result<Vec<_>, _>>()
        .expect("read wav samples")
}

/// Write `n` samples of 16 kHz mono S16 digital silence as a `.wav` for
/// `wav-import` to turn into a frame log.
fn write_silence_wav(path: &Path, n: usize) {
    let spec = hound::WavSpec {
        channels: 1,
        sample_rate: 16_000,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut w = hound::WavWriter::create(path, spec).expect("create silence wav");
    for _ in 0..n {
        w.write_sample(0i16).expect("write sample");
    }
    w.finalize().expect("finalize silence wav");
}

/// A missing frame log surfaces as an open error, not a panic or a silent empty
/// replay — the tuning rig must distinguish a pruned capture from a clean one.
#[test]
fn missing_framelog_is_an_open_error() {
    let mut listener = committed_listener();
    let err = replay_framelog(
        Path::new("/nonexistent/does-not-exist.framelog"),
        &mut listener,
        1,
    )
    .expect_err("a missing log is an error");
    assert!(
        matches!(err, speech_surface::replay::ReplayError::Open(_)),
        "missing input maps to an Open error, got {err:?}"
    );
}
