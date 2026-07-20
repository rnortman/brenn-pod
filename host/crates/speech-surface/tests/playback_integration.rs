//! Playback integration, two scenarios:
//!
//! - **Brain-positive**: a daemon with the streaming listener and `[brain] mode =
//!   "wav"` answers a carved utterance by queueing its configured clip as paced
//!   playback back over the same TCP connection the pod streamed in on. `replay-pod
//!   --linger-until-eoa` holds the connection open through the daemon's playback
//!   and decodes the returned frames, so the assertion is device-side (the frames
//!   actually crossed the wire) as well as JSONL-side.
//! - **No-`[brain]`**: the same wake phrase through a listener daemon with no brain
//!   still carves an utterance, but nothing answers it — no playback lifecycle line
//!   is ever emitted, only the eager `playback_hello` (writers spawn regardless of
//!   brain), and the `utterance` line matches the brain-positive schema modulo
//!   the additive `timings.brain_dispatched: null`.
//!
//! The utterance is driven by the endpointer's natural onset→soft-endpoint path
//! (see `wake_integration.rs`). At playback start the daemon emits the
//! `latency_summary` line accounting for the whole cycle against t0 — host receipt
//! of the utterance's first audio — which is the end-to-end proof that the carve's
//! stamps survive every hop from the listener to the playback writer.

mod common;

use std::path::Path;

use serde_json::Value;

/// The single segment `wav-import` synthesizes for the wake-phrase clip.
const WAKE_SEGMENT_ID: u32 = 1;

/// The configured ack clip length in S16 samples — a non-multiple of the
/// 320-sample frame, so the writer's ceiling framing and final-frame
/// zero-padding are both exercised end to end.
const CLIP_SAMPLES: usize = 700;
/// 20 ms of 16 kHz mono S16 audio — the writer's frame granularity.
const FRAME_SAMPLES: usize = 320;
/// Frames the writer emits for the clip: ⌈CLIP_SAMPLES / FRAME_SAMPLES⌉.
const CLIP_FRAMES: u64 = CLIP_SAMPLES.div_ceil(FRAME_SAMPLES) as u64;

/// Write `n` samples of spine-format PCM (16 kHz mono S16) to `path` — the exact
/// format the clip loader accepts. A recognizable ramp so a mis-sized read is
/// visible in a failure.
fn write_clip_wav(path: &Path, n: usize) {
    let spec = hound::WavSpec {
        channels: 1,
        sample_rate: 16_000,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut w = hound::WavWriter::create(path, spec).expect("create clip wav");
    for i in 0..n {
        w.write_sample(i as i16).expect("write clip sample");
    }
    w.finalize().expect("finalize clip wav");
}

/// The wake phrase, replayed against a listener + `wav`-brain daemon, arms
/// openWakeWord, the endpointer carves one utterance, and the brain
/// answers it with the configured clip queued as paced playback. The daemon writes
/// the clip back over the same connection; `replay-pod --linger-until-eoa` stays
/// connected, decodes the returned `Hello`/`Audio`/`EndOfAudio`, and reports the
/// tally — so this asserts both the JSONL latency-decomposition lines and the
/// device-side wire tally.
#[test]
fn wav_brain_answers_wake_with_paced_clip_playback() {
    let work = tempfile::tempdir().expect("work tempdir");
    let framelog = common::import_wav_to_framelog(
        work.path(),
        Path::new(common::WAKE_PHRASE_WAV),
        WAKE_SEGMENT_ID,
    );
    let clip = work.path().join("ack.wav");
    write_clip_wav(&clip, CLIP_SAMPLES);

    // Recording off: the playback path is independent of the record store, so
    // the brain-positive scenario needs no record dir.
    let mut daemon = common::spawn_daemon(&common::listener_wav_brain_config(None, &clip));
    let jsonl_path = daemon.jsonl_path.clone();
    let addr = daemon.listen_addr();

    let out = common::run_replay_linger(&addr, &framelog);
    common::assert_replay_ok(&out, &daemon);

    // The linger held the connection open through playback; the daemon emits
    // `playback_finished` exactly when it writes `EndOfAudio`, so draining
    // through it orders every playback line onto disk before the snapshot.
    common::wait_for_event(&daemon, "playback_finished", common::EVENT_DEADLINE, |v| {
        v["event"] == "playback_finished"
    });

    let events = common::read_events(&jsonl_path);

    // JSONL sequence: utterance → playback_started → playback_finished, each once.
    let pos = |name: &str| {
        let idxs: Vec<usize> = events
            .iter()
            .enumerate()
            .filter(|(_, v)| v["event"] == name)
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            idxs.len(),
            1,
            "expected exactly one {name} line\n{}",
            daemon.diagnostics()
        );
        idxs[0]
    };
    let utt = pos("utterance");
    let started = pos("playback_started");
    let finished = pos("playback_finished");
    assert!(
        utt < started && started < finished,
        "sequence must be utterance({utt}) < playback_started({started}) < \
         playback_finished({finished})\n{}",
        daemon.diagnostics()
    );

    // playback_started: the firmware hangover floor is reported from its single
    // source of truth (800 ms) and the reply's sample count is the clip's.
    let summary_at = pos("latency_summary");
    let started = &events[started];
    assert_eq!(
        started["vad_hangover_floor_ms"],
        800,
        "hangover floor is the firmware constant\n{}",
        daemon.diagnostics()
    );
    assert_eq!(started["samples"], CLIP_SAMPLES as u64);

    // latency_summary: the whole cycle accounted for, end to end through a real
    // daemon. Every stage stamp survived the listener → pipeline → brain → router
    // → writer hops, so every offset and every blame delta is a real measurement.
    let s = &events[summary_at];
    assert_eq!(
        s["t0_projected"],
        false,
        "the wake opened the segment, so t0 is measured\n{}",
        daemon.diagnostics()
    );
    for field in [
        "vad_high_ms",
        "wake_ms",
        "onset_ms",
        "soft_endpoint_ms",
        "stt_start_ms",
        "brain_ms",
        "speak_rx_ms",
        "first_write_ms",
    ] {
        assert!(
            s[field].as_i64().is_some(),
            "latency_summary offset {field} must be present and numeric, got {}\n{}",
            s[field],
            daemon.diagnostics()
        );
    }
    // The stages are ordered on the axis, and t0 really is the origin.
    let offset = |f: &str| s[f].as_i64().unwrap();
    assert!(
        offset("soft_endpoint_ms") <= offset("stt_start_ms")
            && offset("stt_start_ms") <= offset("brain_ms")
            && offset("brain_ms") <= offset("speak_rx_ms")
            && offset("speak_rx_ms") <= offset("first_write_ms"),
        "the stackup must be monotonic from the soft endpoint to the first write\n{}",
        daemon.diagnostics()
    );
    assert!(
        offset("vad_high_ms") < offset("first_write_ms"),
        "the VAD went high before the response played\n{}",
        daemon.diagnostics()
    );
    for field in ["endpoint_to_stt_us", "brain_us", "speak_to_first_write_us"] {
        assert!(
            s[field].as_u64().is_some(),
            "latency_summary blame {field} must be present and numeric, got {}\n{}",
            s[field],
            daemon.diagnostics()
        );
    }
    // This daemon wires no `[stt]` and the wav brain replies with PCM, so neither
    // stage ran and neither can be blamed. `parrot_integration` is where the full
    // stackup — STT and TTS included — is asserted end to end.
    for field in [
        "stt_done_ms",
        "tts_done_ms",
        "stt_us",
        "stt_to_brain_us",
        "speak_to_synth_start_us",
        "tts_us",
        "synth_to_first_write_us",
    ] {
        assert!(
            s[field].is_null(),
            "no stt and a pcm reply leave {field} null, got {}\n{}",
            s[field],
            daemon.diagnostics()
        );
    }

    // playback_finished: EndOfAudio was written and the frame count is the
    // ceiling of clip samples over the frame size.
    let finished = &events[finished];
    assert_eq!(finished["eoa_written"], true);
    assert_eq!(finished["frames"], CLIP_FRAMES);

    // Device-side: `replay-pod`'s drain saw exactly one Hello, that many Audio
    // frames, and one EndOfAudio — the frames actually crossed the wire.
    let complete = common::find_report_line(&out, "replay_complete");
    let rx = &complete["playback_rx"];
    assert_eq!(rx["hello"], 1, "one eager Hello: {complete}");
    assert_eq!(
        rx["audio"], CLIP_FRAMES,
        "clip frames on the wire: {complete}"
    );
    assert_eq!(rx["end_of_audio"], 1, "one EndOfAudio at drain: {complete}");

    // The linger released on the observed EndOfAudio, not the timeout — the
    // termination condition is the wire event, asserted directly.
    assert_eq!(
        complete["linger"]["eoa_observed"], true,
        "linger must release on the observed EndOfAudio: {complete}"
    );

    // Shut down cleanly, then read the final `stage_health` line and assert it
    // carries the playback/brain/router counters reflecting the one reply.
    let health = common::final_stage_health(&mut daemon);
    assert_eq!(
        health["playback"]["jobs_completed"], 1,
        "one job completed: {health}"
    );
    assert_eq!(
        health["playback"]["frames_written"], CLIP_FRAMES,
        "clip frames written: {health}"
    );
    assert_eq!(
        health["playback"]["eoa_written"], 1,
        "one EndOfAudio written: {health}"
    );
    assert_eq!(
        health["brain"]["speak_send_failures"], 0,
        "the reply was delivered, not dropped: {health}"
    );
    assert_eq!(
        health["router"]["delivered"], 1,
        "one SpeakCmd routed to a live writer: {health}"
    );
}

/// The same wake phrase through a no-`[brain]` listener daemon: the listener still
/// arms and carves one utterance, but with no brain nothing answers it. Run
/// **without** `--linger-until-eoa` (no `EndOfAudio` will ever arrive, so
/// lingering would only burn the timeout). Asserts, precisely: no playback
/// lifecycle line and no brain line other than the startup `brain_absent`;
/// `playback_hello` still present (the writer spawns at registration regardless
/// of brain); the `stage_health` playback/brain/router snapshot blocks present
/// (all zero); the `utterance` line carries `timings.brain_dispatched: null`;
/// and the `tracking` line embeds no `StageTimings` at all.
#[test]
fn no_brain_config_mints_utterance_without_any_playback() {
    let work = tempfile::tempdir().expect("work tempdir");
    let framelog = common::import_wav_to_framelog(
        work.path(),
        Path::new(common::WAKE_PHRASE_WAV),
        WAKE_SEGMENT_ID,
    );

    // No `[brain]` table: the listener still carves an utterance on the wake
    // phrase, but nothing answers it — no playback is ever queued.
    let mut daemon = common::spawn_daemon(&common::listener_daemon_config(None));
    let jsonl_path = daemon.jsonl_path.clone();
    let addr = daemon.listen_addr();

    // No linger: with no brain the daemon never writes `EndOfAudio`, so lingering
    // would only burn the timeout. FIN fires at end-of-log, as it does everywhere
    // outside the brain-positive scenario.
    let out = common::run_replay(&addr, "fast", &framelog);
    common::assert_replay_ok(&out, &daemon);

    // Drain the whole capture through the pipeline and wait for the trailing
    // `utterance` line, so every per-segment line is on disk before asserting.
    common::drain_and_await_utterance(&daemon, WAKE_SEGMENT_ID);

    let events = common::read_events(&jsonl_path);
    let count = |name: &str| events.iter().filter(|v| v["event"] == name).count();

    // No playback lifecycle line: nothing ever queues a job.
    for absent in [
        "playback_started",
        "playback_finished",
        "playback_aborted",
        "playback_rejected",
        "playback_no_pod",
    ] {
        assert_eq!(
            count(absent),
            0,
            "no-brain config emits no {absent} line\n{}",
            daemon.diagnostics()
        );
    }

    // No brain line other than the single startup `brain_absent`.
    assert_eq!(
        count("brain_absent"),
        1,
        "exactly one brain_absent startup line\n{}",
        daemon.diagnostics()
    );
    for absent in ["brain_clip_loaded", "brain_sink_full"] {
        assert_eq!(
            count(absent),
            0,
            "no-brain config emits no {absent} line\n{}",
            daemon.diagnostics()
        );
    }

    // The eager `playback_hello` is present: the writer spawns at registration
    // regardless of brain.
    assert_eq!(
        count("playback_hello"),
        1,
        "one eager playback Hello (writers spawn regardless of brain)\n{}",
        daemon.diagnostics()
    );

    // The `utterance` line: one, carrying `timings.brain_dispatched` present and
    // serialized `null` — the schema-additive field records a dispatch that did
    // not happen. Presence and nullness are asserted separately: a missing field
    // (or a missing `timings` object) must fail, not silently pass as `null`.
    let utt = common::expect_one(&events, "utterance", &daemon);
    let utt_timings = utt["timings"].as_object().unwrap_or_else(|| {
        panic!(
            "utterance line carries a timings object\n{}",
            daemon.diagnostics()
        )
    });
    assert!(
        utt_timings
            .get("brain_dispatched")
            .is_some_and(Value::is_null),
        "brain_dispatched present and null with no brain, got {:?}\n{}",
        utt_timings.get("brain_dispatched"),
        daemon.diagnostics()
    );

    // The `tracking` line: one. It carries derived deltas, not raw
    // `StageTimings`, so the `timings` key is absent entirely (not present-null).
    let tracking = common::expect_one(&events, "tracking", &daemon);
    assert!(
        tracking.get("timings").is_none() && tracking.get("brain_dispatched").is_none(),
        "the tracking line embeds no StageTimings, so no brain_dispatched: {tracking}\n{}",
        daemon.diagnostics()
    );

    // Shut down cleanly, then assert the final `stage_health` line carries the
    // playback/brain/router snapshot blocks in the no-brain config too. The
    // blocks are emitted unconditionally, so an absent block is a regression,
    // never a no-brain artifact — present and all zero.
    let health = common::final_stage_health(&mut daemon);
    assert!(
        health["playback"].is_object()
            && health["brain"].is_object()
            && health["router"].is_object(),
        "stage_health carries the playback/brain/router snapshot blocks: {health}"
    );
    assert_eq!(
        health["playback"]["jobs_completed"], 0,
        "no jobs completed: {health}"
    );
    assert_eq!(
        health["playback"]["frames_written"], 0,
        "no frames written: {health}"
    );
    assert_eq!(
        health["playback"]["eoa_written"], 0,
        "no EndOfAudio written: {health}"
    );
    assert_eq!(
        health["brain"]["speak_send_failures"], 0,
        "no brain sends: {health}"
    );
    assert_eq!(
        health["router"]["delivered"], 0,
        "no SpeakCmd routed with no brain: {health}"
    );
}
