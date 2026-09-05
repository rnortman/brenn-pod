//! Offline listener-replay harness tests: drive a captured frame log through the
//! streaming listener in-process (no device, no daemon, no socket) via
//! `speech_surface::replay`, and assert the wake + carved-utterance it produces.
//!
//! This is the deafness-bug regression harness. The wake-phrase clip is
//! `wav-import`ed to a frame log and replayed through a fresh listener over the
//! committed openWakeWord + Silero
//! models: openWakeWord arms on the phrase, Silero onsets on the speech, and the
//! trailing silence soft-endpoints it into a carved utterance — the same
//! deterministic end-to-end drive the live integration suite uses, minus the
//! daemon and TCP round trip.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use audio_pipeline::wire::{ChannelSource, MAX_FRAME_BYTES, StreamFrame, encode_frame};
use pod_ingest::{FrameLogWriter, HostMicros, LogMeta, SynthParams, synth_session};
use speech_pipeline::{
    EndpointCause, ListenerConfig, ListenerEvent, OwwConfig, OwwModels, SPINE_FORMAT, SileroConfig,
    SileroModel,
};
use speech_surface::replay::{ReplayListener, StopReason, replay_framelog};

/// Load a `ReplayListener` with the wake-command hold off: the plain replays
/// have no command after the wake phrase, so the hold must be disabled or it
/// suppresses the carve these tests assert on.
fn committed_listener() -> ReplayListener {
    committed_listener_with(ListenerConfig {
        command_wait_samples: 0,
        ..ListenerConfig::default()
    })
}

/// Load a `ReplayListener` over the committed openWakeWord + Silero models with
/// `config` as given.
fn committed_listener_with(config: ListenerConfig) -> ReplayListener {
    let oww = OwwModels::load(&OwwConfig {
        melspectrogram: common::OWW_MELSPECTROGRAM.into(),
        embedding: common::OWW_EMBEDDING.into(),
        model: common::OWW_MODEL.into(),
        threshold: config.oww_threshold,
    })
    .expect("load oww models");
    let silero = SileroModel::load(&SileroConfig {
        model: common::SILERO_MODEL.into(),
    })
    .expect("load silero model");
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
    // 1 s of digital silence.
    speech_pipeline::write_spine_wav(&wav, &vec![0_i16; 16_000]).expect("write spine wav");
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
    // Segment 2's audio is the re-sent tail plus a little fresh silence.
    let mut second = pcm[base2 as usize..].to_vec();
    second.extend(std::iter::repeat_n(0_i16, 1_600));
    let framelog = write_two_segment_framelog(
        dir.path(),
        "overlap.framelog",
        &pcm,
        &second,
        base2,
        overlap,
    );

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

/// Write a frame log of two segments into `dir/name`: `first` at
/// `[0, first.len())`, then `second` at `[base2, base2 + second.len())` with
/// `preroll` of its samples declared as re-sent. The samples keep their original
/// capture indexes, so a `base2` behind the first segment's end overlaps it and
/// one past that end leaves a hole where the device sent nothing. The successor's
/// `Hello` is dropped: this is one connection with two segments, not a reconnect
/// (which would reset the listener and erase both shapes).
fn write_two_segment_framelog(
    dir: &Path,
    name: &str,
    first_pcm: &[i16],
    second_pcm: &[i16],
    base2: u64,
    preroll: u64,
) -> PathBuf {
    let params = |segment_id: u32, base_sample_index: u64, preroll_samples: u32| SynthParams {
        pod_id: "pod-x".to_string(),
        sample_rate_hz: SPINE_FORMAT.sample_rate_hz,
        segment_id,
        base_sample_index,
        base_device_ts_us: 0,
        preroll_samples,
        channel_source: ChannelSource::AsrBeam,
    };
    let first = synth_session(first_pcm, &params(1, 0, 0)).expect("synth segment 1");
    let second = synth_session(second_pcm, &params(2, base2, preroll as u32))
        .expect("synth segment 2")
        .into_iter()
        .filter(|sf| !matches!(sf.frame, StreamFrame::Hello(_)));

    let out = dir.join(name);
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

/// The two committed TTS clips, each loaded once per test binary.
static WAKE_CLIP: OnceLock<Arc<[i16]>> = OnceLock::new();
static COMMAND_CLIP: OnceLock<Arc<[i16]>> = OnceLock::new();

/// The committed TTS clip at `path`, loaded once per test binary and shared by
/// every case that splices it.
fn cached_clip(cell: &'static OnceLock<Arc<[i16]>>, path: &str) -> Arc<[i16]> {
    Arc::clone(cell.get_or_init(|| speech_surface::load_clip(Path::new(path)).expect("load clip")))
}

/// Splice the two committed TTS clips into one 16 kHz mono S16 `.wav` at `path`:
/// the wake phrase, `pause_samples` of digital silence, then the command phrase.
/// Returns the wake clip's length — the boundary the hold cases measure a carve
/// against, together with the pause length the caller passed in.
///
/// This is the shape the wake-command hold exists for — a speaker who says "Hey
/// Jarvis", pauses, and then says the command. One `.wav` means one device
/// segment out of `wav-import`, so the pause here stays inside a single segment;
/// the shape where the device VAD releases in the pause is
/// [`a_wake_and_a_command_in_two_device_segments_coalesce`].
fn compose_wake_pause_command(path: &Path, pause_samples: usize) -> usize {
    let wake = cached_clip(&WAKE_CLIP, common::WAKE_PHRASE_WAV);
    let command = cached_clip(&COMMAND_CLIP, common::COMMAND_PHRASE_WAV);
    let mut pcm = Vec::with_capacity(wake.len() + pause_samples + command.len());
    pcm.extend_from_slice(&wake);
    pcm.extend(std::iter::repeat_n(0_i16, pause_samples));
    pcm.extend_from_slice(&command);
    speech_pipeline::write_spine_wav(path, &pcm).expect("write spine wav");
    wake.len()
}

/// The frame log for a wake / `pause_samples` of silence / command clip, and the
/// wake clip's length. Composed and `wav-import`ed once per distinct pause and
/// then shared: cases that differ only in `ListenerConfig` replay byte-identical
/// audio, and composing plus importing it again is a subprocess per case for no
/// added signal.
///
/// The fixtures are staged at a fixed path under Cargo's per-test-binary scratch
/// directory rather than in a `tempfile::TempDir`. The cache outlives every case,
/// so it is a static, and a static is never dropped: a `TempDir` behind one is
/// never removed either, and would leave a fresh megabyte of composed audio in
/// `$TMPDIR` on every run. Rewriting the same paths instead bounds the space to
/// one copy per distinct pause, inside the build tree `cargo clean` takes.
fn wake_pause_command_framelog(pause_samples: usize) -> (PathBuf, usize) {
    static LOGS: Mutex<Option<BTreeMap<usize, (PathBuf, usize)>>> = Mutex::new(None);

    // The compose and the `wav-import` below run under this lock and can panic,
    // which poisons it. Recovering the guard leaves the real failure reported
    // once, by the case that hit it, instead of reporting it again in every
    // other case as a `PoisonError` that names nothing about the cause. The map
    // is intact either way: a panicking `or_insert_with` inserts nothing.
    let mut logs = LOGS.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let logs = logs.get_or_insert_with(BTreeMap::new);
    logs.entry(pause_samples)
        .or_insert_with(|| {
            let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
                .join("listener-replay")
                .join(format!("pause-{pause_samples}"));
            // Emptied first: `wav-import` refuses to write over a frame log, so
            // the previous run's copy is what a fixed path would otherwise hand
            // this one.
            if let Err(e) = std::fs::remove_dir_all(&dir) {
                assert_eq!(e.kind(), std::io::ErrorKind::NotFound, "clear fixture dir");
            }
            std::fs::create_dir_all(&dir).expect("fixture dir");
            let wav = dir.join("wake-pause-command.wav");
            let wake_len = compose_wake_pause_command(&wav, pause_samples);
            (common::import_wav_to_framelog(&dir, &wav, 1), wake_len)
        })
        .clone()
}

/// Replay the wake / pause / command fixture through a listener configured as
/// given, returning the summary and the wake clip's length.
fn replay_wake_pause_command(
    pause_samples: usize,
    config: ListenerConfig,
) -> (speech_surface::replay::ReplaySummary, usize) {
    let (framelog, wake_len) = wake_pause_command_framelog(pause_samples);
    let mut listener = committed_listener_with(config);
    let summary = replay_framelog(&framelog, &mut listener, 1).expect("replay");
    (summary, wake_len)
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

// --- The wake-command hold, end to end over the committed models -------------
//
// These three cases replay one spliced clip — wake phrase, silence, command
// phrase — through a real listener, so the shipped hold defaults are exercised
// against real openWakeWord and Silero scores rather than synthetic
// probabilities. The 2.0 s pause sits past the 992 ms continuation window, so
// the wake-only carve closes before the command onsets, and well inside the
// 4 s command wait.
//
// Frames matter when reading the assertions. `CarvedUtterance.start_sample` and
// `end_sample` are absolute stream indexes; `WakeConfirmation.wake_end_sample`
// and `stt_trim_samples` are relative to that `start_sample`. The absolute wake
// end appears only on `WakeDetected` and `WakeHeld`. Subtracting a relative
// offset from an absolute index would pass on this fixture for arithmetic
// reasons and assert nothing.

/// Every `SoftEndpoint` in `events` that carries wake provenance, in order.
fn wake_carves(events: &[ListenerEvent]) -> Vec<&speech_pipeline::CarvedUtterance> {
    events
        .iter()
        .filter_map(|e| match e {
            ListenerEvent::SoftEndpoint { utterance, .. } if utterance.wake.is_some() => {
                Some(utterance)
            }
            _ => None,
        })
        .collect()
}

/// The shipped default: a wake, a 2 s pause and a command replay as one utterance
/// covering all three. This is the failure the hold was built for — the wake word
/// closing its own utterance and the command arriving to no arm.
#[test]
fn a_wake_a_pause_and_a_command_coalesce_into_one_utterance() {
    let pause_len = 32_000; // 2.0 s
    let (summary, wake_len) = replay_wake_pause_command(pause_len, ListenerConfig::default());

    let wakes = summary
        .events
        .iter()
        .filter(|e| matches!(e, ListenerEvent::WakeDetected { .. }))
        .count();
    assert_eq!(
        wakes, 1,
        "the spliced clip says the wake phrase exactly once"
    );
    assert!(
        !summary
            .events
            .iter()
            .any(|e| matches!(e, ListenerEvent::ArmExpired { .. })),
        "the command arrived inside the wait, so the arm was consumed, not expired"
    );

    let held_at: Vec<usize> = summary
        .events
        .iter()
        .enumerate()
        .filter(|(_, e)| matches!(e, ListenerEvent::WakeHeld { .. }))
        .map(|(i, _)| i)
        .collect();
    assert!(
        !held_at.is_empty(),
        "the wake word alone was held back rather than published"
    );
    let carve_at: Vec<usize> = summary
        .events
        .iter()
        .enumerate()
        .filter(|(_, e)| matches!(e, ListenerEvent::SoftEndpoint { .. }))
        .map(|(i, _)| i)
        .collect();
    assert!(
        held_at.iter().max() < carve_at.iter().min(),
        "every hold precedes every carve: the holding happens first and the one \
         utterance comes out of it, at event indexes held={held_at:?} carved={carve_at:?}"
    );

    let held_start = summary
        .events
        .iter()
        .find_map(|e| match e {
            ListenerEvent::WakeHeld { start_sample, .. } => Some(*start_sample),
            _ => None,
        })
        .expect("a hold was installed");

    let carves = wake_carves(&summary.events);
    assert!(!carves.is_empty(), "the coalesced utterance was published");
    // The count is deliberately unasserted: an internal gap in the command clip
    // longer than the soft hangover re-carves the same id through `Superseded`,
    // and each re-carve still covers wake, pause and command.
    let id = &carves[0].utterance_id;
    for c in &carves {
        assert_eq!(
            &c.utterance_id, id,
            "every wake-carrying carve is the same utterance, re-carved"
        );
        assert_eq!(
            c.start_sample, held_start,
            "each carve begins at the held start, not at the command's own onset"
        );
    }
    // Every carve in the replay is one of those, so the command is not also
    // published on its own: a wake-free carve over the same audio would send it
    // to STT twice and give the brain the turn twice.
    assert_eq!(
        carve_at.len(),
        carves.len(),
        "the only utterances published are the coalesced one's re-carves: \
         {} carve(s) in all, {} of them wake-gated",
        carve_at.len(),
        carves.len()
    );

    let last = carves.last().expect("a last carve");
    assert!(
        last.end_sample > (wake_len + pause_len) as u64,
        "the dispatched carve reaches past the command's onset at {} — it ends at {}",
        wake_len + pause_len,
        last.end_sample
    );
    assert_eq!(
        last.pcm.len() as u64,
        last.end_sample - last.start_sample,
        "the PCM is the whole span, wake word and pause included"
    );
    let wake = last.wake.as_ref().expect("wake provenance");
    assert!(
        wake.stt_trim_samples < last.pcm.len(),
        "the trim boundary {} lies inside the {}-sample carve, so a trim-mode send \
         still has the command in it",
        wake.stt_trim_samples,
        last.pcm.len()
    );
}

/// The off switch: `command_wait_samples = 0` disables the hold, so the wake
/// word is published as its own utterance and the command that follows is a
/// silent wake-gated drop.
#[test]
fn the_off_switch_publishes_the_wake_word_alone_and_drops_the_command() {
    let pause_len = 32_000; // 2.0 s
    let (summary, wake_len) = replay_wake_pause_command(
        pause_len,
        ListenerConfig {
            command_wait_samples: 0,
            ..ListenerConfig::default()
        },
    );

    assert!(
        !summary
            .events
            .iter()
            .any(|e| matches!(e, ListenerEvent::WakeHeld { .. })),
        "nothing is held with the wait at zero"
    );

    let carves = wake_carves(&summary.events);
    let first = carves.first().expect("the wake word carved on its own");
    let wake = first.wake.as_ref().expect("wake provenance");
    // Both terms are carve-relative: `pcm.len()` is the carve, `wake_end_sample`
    // is the wake end within it. Their ordering is that frame stated as an
    // assertion, and asserting it first is what makes a relative/absolute mixup
    // read as the invariant it broke rather than as a subtraction overflow.
    assert!(
        wake.wake_end_sample <= first.pcm.len(),
        "the wake end {} is an offset into the {}-sample carve",
        wake.wake_end_sample,
        first.pcm.len()
    );
    let tail = first.pcm.len() - wake.wake_end_sample;
    let wake_tail = ListenerConfig::default().wake_tail_samples as usize;
    assert!(
        tail < wake_tail,
        "the published utterance ends {tail} samples after the wake, inside the \
         {wake_tail}-sample wake tail — it is the wake word and nothing else"
    );

    // The drop produces no event of its own, so what pins it is the endpointer:
    // the command onsets, and nothing is carved from it. Without this the case
    // would also pass over a listener that never heard the command at all.
    let command_onset = (wake_len + pause_len) as u64;
    assert!(
        summary.events.iter().any(|e| matches!(
            e,
            ListenerEvent::EndpointerTransition { transition, .. }
                if transition.cause == speech_pipeline::TransitionCause::Onset
                    && transition.sample_offset >= command_onset
        )),
        "the endpointer onsets on the command at {command_onset}, so what follows \
         is a wake-gated drop and not a command nobody heard"
    );
    for e in &summary.events {
        if let ListenerEvent::SoftEndpoint { utterance, .. } = e {
            assert!(
                utterance.end_sample <= command_onset,
                "no carve reaches the command at {command_onset}: one ends at {}",
                utterance.end_sample
            );
        }
    }
}

/// The trade the wait makes visible: a pause longer than `command_wait_ms` ends
/// the hold as a bare wake, and the command that follows it is lost. Nothing
/// clears the arm for a second try — the speaker says the wake word again.
#[test]
fn a_pause_past_the_wait_expires_the_arm_and_loses_the_command() {
    // 10.0 s, past the 8 s wait.
    let pause_len = 160_000;
    let (summary, wake_len) = replay_wake_pause_command(pause_len, ListenerConfig::default());

    let held_at = summary
        .events
        .iter()
        .position(|e| matches!(e, ListenerEvent::WakeHeld { .. }))
        .expect("the wake word was held");
    let expired_at = summary
        .events
        .iter()
        .position(|e| matches!(e, ListenerEvent::ArmExpired { .. }))
        .expect("the wait ran out and the arm expired");
    assert!(
        expired_at > held_at,
        "the expiry ends the hold, so it comes after it"
    );

    // The lost command has no event of its own, so what pins it is the
    // endpointer onsetting on it — without that the case also passes over audio
    // the listener never heard as speech at all. And the onset coming *after*
    // the expiry is what says the wait ran out: a segment close reaping the arm,
    // or a wait collapsed to nothing, produces the same three events in the
    // other order.
    let command_onset = (wake_len + pause_len) as u64;
    let onset_at = summary
        .events
        .iter()
        .position(|e| {
            matches!(
                e,
                ListenerEvent::EndpointerTransition { transition, .. }
                    if transition.cause == speech_pipeline::TransitionCause::Onset
                        && transition.sample_offset >= command_onset
            )
        })
        .unwrap_or_else(|| panic!("the endpointer onsets on the command at {command_onset}"));
    assert!(
        expired_at < onset_at,
        "the wait ran out before the command arrived, at event indexes \
         expired={expired_at} onset={onset_at}"
    );

    assert!(
        wake_carves(&summary.events).is_empty(),
        "no wake-gated utterance is published: the wake word was never a command, \
         and the command arrived to no arm"
    );
}

/// The pause outlasts the device's hangover: the wake phrase is in one transport
/// segment, the command in the next, and the device sent nothing in between. The
/// hold is not scoped to the segment, so the two coalesce into one utterance whose
/// clip carries a silent hole where the device was quiet.
///
/// The three cases above replay a single segment, so this is the only end-to-end
/// coverage of the cross-segment boundary over the real models and the real
/// frame-log reader.
#[test]
fn a_wake_and_a_command_in_two_device_segments_coalesce() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wake = cached_clip(&WAKE_CLIP, common::WAKE_PHRASE_WAV);
    let command = cached_clip(&COMMAND_CLIP, common::COMMAND_PHRASE_WAV);

    // Segment A: the wake phrase and the half second of quiet the device VAD held
    // on for before releasing. Segment B opens a second later — the hole is the
    // silence the device did not send — and carries the command with enough
    // trailing quiet to soft-endpoint it.
    let hangover = 8_000_usize; // 0.5 s
    let hole = 16_000_u64; // 1.0 s
    let mut segment_a = wake.to_vec();
    segment_a.extend(std::iter::repeat_n(0_i16, hangover));
    let mut segment_b = command.to_vec();
    segment_b.extend(std::iter::repeat_n(0_i16, 32_000));
    let base2 = segment_a.len() as u64 + hole;
    let framelog = write_two_segment_framelog(
        dir.path(),
        "cross-segment.framelog",
        &segment_a,
        &segment_b,
        base2,
        0,
    );

    let mut listener = committed_listener_with(ListenerConfig::default());
    let summary = replay_framelog(&framelog, &mut listener, 1).expect("replay");

    assert_eq!(
        summary.stop,
        StopReason::Eof,
        "the replay reads the whole log"
    );
    let held_start = summary
        .events
        .iter()
        .find_map(|e| match e {
            ListenerEvent::WakeHeld { start_sample, .. } => Some(*start_sample),
            _ => None,
        })
        .expect("the wake word alone was held in segment A");
    assert!(
        !summary
            .events
            .iter()
            .any(|e| matches!(e, ListenerEvent::ArmExpired { .. })),
        "neither the segment close nor the wait reaped the arm: {:?}",
        summary.events
    );

    let carves = wake_carves(&summary.events);
    assert!(
        !carves.is_empty(),
        "the command in segment B was published under the wake in segment A: {:?}",
        summary.events
    );
    let id = &carves[0].utterance_id;
    for c in &carves {
        assert_eq!(&c.utterance_id, id, "one utterance, re-carved");
        assert_eq!(c.start_sample, held_start, "each carve from the held start");
    }
    let last = carves.last().expect("a carve");
    assert!(
        last.end_sample > base2,
        "the dispatched carve reaches past the command's onset at {base2}: \
         ends at {}",
        last.end_sample
    );
    assert_eq!(
        last.pcm.len() as u64,
        last.end_sample - last.start_sample,
        "the carve's PCM spans the hole as well as the audio"
    );
    let hole_from = (segment_a.len() as u64 - held_start) as usize;
    let hole_to = (base2 - held_start) as usize;
    assert!(
        last.pcm[hole_from..hole_to].iter().all(|s| *s == 0),
        "the samples the device never sent carve as silence at their true indexes"
    );
}
