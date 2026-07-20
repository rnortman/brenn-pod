//! Listener integration tests: a spawned daemon running the real streaming
//! listener (openWakeWord + Silero endpointer) over the committed models, driven
//! by `replay-pod` over a `.framelog` that `wav-import` builds from a committed
//! `.wav`. Nothing is stubbed — the committed models score real synthesized frames
//! end to end.
//!
//! Post-rework the listener is the only utterance source, and it drives the
//! utterance on its natural path: openWakeWord arms on the wake phrase, Silero
//! onsets on the speech, and the phrase's trailing silence soft-endpoints it —
//! the host ends the utterance on its own rather than waiting for the device's
//! VAD release, which is the entire point of the endpointer.

mod common;

use std::path::Path;

use speech_surface::WakeClass;

/// The single segment `wav-import` synthesizes for the wake-phrase clip.
const WAKE_SEGMENT_ID: u32 = 1;

/// The single segment `wav-import` synthesizes for the noise clip.
const NOISE_SEGMENT_ID: u32 = 1;

/// The wake phrase, replayed against a listener-configured recording daemon, arms
/// openWakeWord (`wake_detected` above the 0.5 default threshold) and the
/// endpointer's natural onset→soft-endpoint path mints exactly one `utterance`
/// (endpoint cause `soft_endpoint`). The wake detection lands in the assembled
/// segment's span, so the on-disk sidecar labels the segment `Positive`.
#[test]
fn wake_phrase_arms_detection_carves_utterance_and_labels_sidecar() {
    let work = tempfile::tempdir().expect("work tempdir");
    let framelog = common::import_wav_to_framelog(
        work.path(),
        Path::new(common::WAKE_PHRASE_WAV),
        WAKE_SEGMENT_ID,
    );

    let record_dir = tempfile::tempdir().expect("record tempdir");
    let mut daemon = common::spawn_daemon(&common::listener_daemon_config(Some(record_dir.path())));
    let jsonl_path = daemon.jsonl_path.clone();
    let addr = daemon.listen_addr();

    let out = common::run_replay(&addr, "fast", &framelog);
    common::assert_replay_ok(&out, &daemon);

    common::drain_and_await_utterance(&daemon, WAKE_SEGMENT_ID);

    let events = common::read_events(&jsonl_path);

    // wake_detected: the wake phrase arms openWakeWord above the 0.5 threshold.
    let detected = common::expect_wake_detected(&events, &daemon);
    let score = detected["score"]
        .as_f64()
        .expect("a wake detection carries a numeric score");
    assert!(score > 0.5, "score {score} must exceed the 0.5 threshold");

    // The missed-onset fallback carve mints exactly one utterance, ended by the
    // device VAD release.
    let utterances: Vec<_> = events
        .iter()
        .filter(|v| v["event"] == "utterance")
        .collect();
    assert_eq!(
        utterances.len(),
        1,
        "one carved utterance for the wake phrase\n{}",
        daemon.diagnostics()
    );
    assert_eq!(
        utterances[0]["endpoint_cause"],
        "soft_endpoint",
        "the host endpointer ends the utterance on its own, ahead of the device VAD\n{}",
        daemon.diagnostics()
    );

    // Shut down cleanly, then assert the on-disk sidecar labels the segment
    // Positive — the wake detection landed in its span (recording was on).
    daemon.sigterm_and_wait();
    common::assert_sidecar_wake(record_dir.path(), WAKE_SEGMENT_ID, WakeClass::Positive);
}

/// A daemon with no `[wake]`/`[endpointer]` tables runs no listener (the config
/// permutations that gate listener startup are covered by the `server` unit tests):
/// the wake phrase replays, but with no listener nothing arms and nothing is carved,
/// so no `wake_detected` and no `utterance` line appears. The segment path still
/// runs, so the recorded sidecar labels the segment `Negative` (no wake detection
/// landed in its span). Shutdown joins the (absent) listener, making the absence of
/// listener lines deterministic.
#[test]
fn no_listener_config_mints_no_utterance_and_labels_negative() {
    let work = tempfile::tempdir().expect("work tempdir");
    let framelog = common::import_wav_to_framelog(
        work.path(),
        Path::new(common::WAKE_PHRASE_WAV),
        WAKE_SEGMENT_ID,
    );

    let record_dir = tempfile::tempdir().expect("record tempdir");
    // `daemon_config` carries listen_addr + [record] only — no listener tables.
    let mut daemon = common::spawn_daemon(&common::daemon_config(Some(record_dir.path())));
    let jsonl_path = daemon.jsonl_path.clone();
    let addr = daemon.listen_addr();

    let out = common::run_replay(&addr, "fast", &framelog);
    common::assert_replay_ok(&out, &daemon);

    // Drain the segment path, then shut down (joins the listener thread, if any)
    // so the absence of listener lines is deterministic, not a timing race.
    common::wait_until_drained(&daemon, WAKE_SEGMENT_ID);
    daemon.sigterm_and_wait();

    let events = common::read_events(&jsonl_path);
    assert!(
        events
            .iter()
            .all(|v| v["event"] != "wake_detected" && v["event"] != "utterance"),
        "no listener ⇒ no wake_detected and no utterance\n{}",
        daemon.diagnostics()
    );

    // The segment path still labels the recorded segment — provisionally Negative,
    // as no wake detection landed in its span.
    common::assert_sidecar_wake(record_dir.path(), WAKE_SEGMENT_ID, WakeClass::Negative);
}

/// With recording off (`record.enabled = false`) and the listener configured — the
/// tuning-box/privacy profile — the listener still scores and carves: the wake
/// phrase produces a `wake_detected` line and a carved `utterance`, but with no
/// record store the segment path dispatches no sidecar update at all, so neither
/// the soft `wake_sidecar_skipped` warning nor the hard `wake_sidecar_error` line
/// ever appears. A sanctioned configuration must not spam either channel.
#[test]
fn recording_off_listener_scores_without_sidecar_noise() {
    let work = tempfile::tempdir().expect("work tempdir");
    let framelog = common::import_wav_to_framelog(
        work.path(),
        Path::new(common::WAKE_PHRASE_WAV),
        WAKE_SEGMENT_ID,
    );

    // Recording off: `listener_daemon_config(None)` emits `[record] enabled = false`
    // alongside the real listener tables.
    let mut daemon = common::spawn_daemon(&common::listener_daemon_config(None));
    let jsonl_path = daemon.jsonl_path.clone();
    let addr = daemon.listen_addr();

    let out = common::run_replay(&addr, "fast", &framelog);
    common::assert_replay_ok(&out, &daemon);

    common::drain_and_await_utterance(&daemon, WAKE_SEGMENT_ID);

    let events = common::read_events(&jsonl_path);

    // The listener still scores: a wake detection with a numeric score.
    let detected = common::expect_wake_detected(&events, &daemon);
    assert!(
        detected["score"].as_f64().is_some(),
        "a wake detection carries a numeric score, got {}\n{}",
        detected["score"],
        daemon.diagnostics()
    );

    // No sidecar dispatch happens with recording off, so neither the soft skip
    // warning nor the hard error line is ever written.
    assert!(
        events
            .iter()
            .all(|v| v["event"] != "wake_sidecar_skipped" && v["event"] != "wake_sidecar_error"),
        "recording off must produce no sidecar warning or error lines\n{}",
        daemon.diagnostics()
    );

    daemon.sigterm_and_wait();
}

/// Deterministic pseudo-random S16 noise from a fixed LCG, so the clip — and the
/// byte-exact export round-trip below — is reproducible. Seed 1 / 32000 samples
/// is a two-second clip; openWakeWord scores it below threshold (no arm).
fn seeded_noise(seed: u64, n: usize) -> Vec<i16> {
    let mut state = seed;
    (0..n)
        .map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as i16
        })
        .collect()
}

/// Write S16 mono 16 kHz PCM to a `.wav` — the spine format `wav-import` accepts.
fn write_noise_wav(path: &Path, pcm: &[i16]) {
    let spec = hound::WavSpec {
        channels: 1,
        sample_rate: 16_000,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut w = hound::WavWriter::create(path, spec).expect("create noise wav");
    for &s in pcm {
        w.write_sample(s).expect("write noise sample");
    }
    w.finalize().expect("finalize noise wav");
}

/// Synthetic noise, replayed against a listener-configured recording daemon, does
/// not arm openWakeWord: no `wake_detected` fires, so no utterance is carved. A
/// `tracking` event still fires (tracking is unconditional, pre-listener), and the
/// on-disk sidecar labels the segment `Negative`. Finally the recorded framelog
/// exports back to byte-exact PCM.
#[test]
fn noise_does_not_arm_and_round_trips_through_export() {
    let work = tempfile::tempdir().expect("work tempdir");
    let noise = seeded_noise(1, 32_000);
    let noise_wav = work.path().join("noise.wav");
    write_noise_wav(&noise_wav, &noise);
    let framelog = common::import_wav_to_framelog(work.path(), &noise_wav, NOISE_SEGMENT_ID);

    let record_dir = tempfile::tempdir().expect("record tempdir");
    let mut daemon = common::spawn_daemon(&common::listener_daemon_config(Some(record_dir.path())));
    let jsonl_path = daemon.jsonl_path.clone();
    let addr = daemon.listen_addr();

    let out = common::run_replay(&addr, "fast", &framelog);
    common::assert_replay_ok(&out, &daemon);

    // Drain the segment path, then shut down (joins the listener) so "no wake, no
    // utterance" is deterministic rather than a timing race.
    common::wait_until_drained(&daemon, NOISE_SEGMENT_ID);

    // The tracking event fires for every segment regardless of the listener —
    // tracking is unconditional, pre-listener.
    let events = common::read_events(&jsonl_path);
    let tracked = events
        .iter()
        .any(|v| v["event"] == "tracking" && v["segment_id"] == NOISE_SEGMENT_ID);
    assert!(
        tracked,
        "a tracking event must fire even when no wake arms\n{}",
        daemon.diagnostics()
    );

    daemon.sigterm_and_wait();

    // No arm, so no wake detection and no carved utterance.
    let events = common::read_events(&jsonl_path);
    assert!(
        events
            .iter()
            .all(|v| v["event"] != "wake_detected" && v["event"] != "utterance"),
        "noise must not arm the listener: no wake_detected, no utterance\n{}",
        daemon.diagnostics()
    );

    // The on-disk sidecar labels the segment Negative (recording was on, no wake).
    common::assert_sidecar_wake(record_dir.path(), NOISE_SEGMENT_ID, WakeClass::Negative);

    // The recorded framelog exports back to byte-exact PCM.
    let log = common::find_one_framelog(record_dir.path());
    let export_dir = work.path().join("export");
    let export = common::run_segments_export(&export_dir, &log);
    assert!(
        export.status.success(),
        "segments-export exited {:?}\n--- stdout ---\n{}\n--- stderr ---\n{}",
        export.status.code(),
        String::from_utf8_lossy(&export.stdout),
        String::from_utf8_lossy(&export.stderr),
    );
    let wav = common::find_one_with_ext(&export_dir, "wav");
    let mut reader = hound::WavReader::open(&wav)
        .unwrap_or_else(|e| panic!("open exported wav {}: {e}", wav.display()));
    let exported: Vec<i16> = reader.samples::<i16>().map(|s| s.unwrap()).collect();
    assert_eq!(
        exported, noise,
        "exported PCM must round-trip the noise byte-for-byte"
    );
}
