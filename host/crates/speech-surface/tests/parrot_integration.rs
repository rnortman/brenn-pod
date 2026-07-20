//! End-to-end parrot mode (the increment-5 done-when): a daemon with the wake
//! gate in `bypass`, `[brain] mode = "echo"`, and `[stt]`/`[tts]` pointed at an
//! in-process fake speaches container reads back what it "heard". `replay-pod
//! --linger-until-eoa` streams a checked-in capture and stays connected through
//! the daemon's synthesized readback, so the assertion is both JSONL-side (the
//! `utterance` → `synth` → `playback_started` → `playback_finished` sequence with
//! the per-stage latency breakdown) and device-side (the readback frames actually
//! crossed the wire back and tally to the fake TTS clip).

mod common;

use std::path::Path;

/// The single segment `wav-import` synthesizes for the replayed capture. Bypass
/// passes it, so it mints exactly one utterance.
const SEGMENT_ID: u32 = 1;

/// The fake TTS clip length in S16 samples — a non-multiple of the 320-sample
/// frame, so the writer's ceiling framing and final-frame zero-padding are both
/// exercised end to end.
const TTS_SAMPLES: usize = 700;
/// 20 ms of 16 kHz mono S16 audio — the writer's frame granularity.
const FRAME_SAMPLES: usize = 320;
/// Frames the writer emits for the clip: ⌈TTS_SAMPLES / FRAME_SAMPLES⌉.
const CLIP_FRAMES: u64 = TTS_SAMPLES.div_ceil(FRAME_SAMPLES) as u64;

/// A capture replayed against a `bypass` + `echo`-brain daemon with STT/TTS
/// pointed at a fake speaches container: the segment mints one utterance, STT
/// transcribes it (to the fake's canned text), `EchoBrain` echoes that text back,
/// TTS renders it to PCM, and the paced sender writes the clip back over the same
/// connection. `replay-pod --linger-until-eoa` stays connected, decodes the
/// returned `Hello`/`Audio`/`EndOfAudio`, and reports the tally — so this asserts
/// both the JSONL parrot sequence with its latency decomposition and the
/// device-side wire tally of the readback clip.
#[test]
fn echo_brain_reads_back_transcript_end_to_end() {
    let work = tempfile::tempdir().expect("work tempdir");
    let framelog =
        common::import_wav_to_framelog(work.path(), Path::new(common::WAKE_PHRASE_WAV), SEGMENT_ID);

    // One fake speaches container serves both endpoints; the daemon's [stt] and
    // [tts] tables point at its single URL.
    let speaches_url = common::spawn_fake_speaches(TTS_SAMPLES);
    let mut daemon = common::spawn_daemon(&common::echo_parrot_config(&speaches_url));
    let jsonl_path = daemon.jsonl_path.clone();
    let addr = daemon.listen_addr();

    let out = common::run_replay_linger(&addr, &framelog);
    common::assert_replay_ok(&out, &daemon);

    // The linger held the connection open through the readback; the daemon emits
    // `playback_finished` exactly when it writes `EndOfAudio`, so draining through
    // it orders every parrot line onto disk before the snapshot.
    common::wait_for_event(&daemon, "playback_finished", common::EVENT_DEADLINE, |v| {
        v["event"] == "playback_finished"
    });

    let events = common::read_events(&jsonl_path);

    // The parrot sequence: utterance → synth → playback_started → playback_finished,
    // each exactly once and strictly ordered.
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
    let synth = pos("synth");
    let started = pos("playback_started");
    let finished = pos("playback_finished");
    assert!(
        utt < synth && synth < started && started < finished,
        "parrot sequence must be utterance({utt}) < synth({synth}) < \
         playback_started({started}) < playback_finished({finished})\n{}",
        daemon.diagnostics()
    );

    // utterance: carries the STT transcript — the fake's canned text, threaded
    // through the pipeline onto the `utterance` line.
    let utt = &events[utt];
    assert_eq!(
        utt["transcript"]["text"],
        common::FAKE_TRANSCRIPT,
        "utterance carries the STT transcript\n{}",
        daemon.diagnostics()
    );

    // synth: the readback the echo brain requested — the transcript's character
    // count in, the fake clip's sample count out, with a measured duration.
    let synth = &events[synth];
    assert_eq!(
        synth["input_chars"],
        common::FAKE_TRANSCRIPT.chars().count() as u64,
        "synth input_chars is the echoed transcript length\n{}",
        daemon.diagnostics()
    );
    assert_eq!(
        synth["samples"],
        TTS_SAMPLES as u64,
        "synth samples is the fake TTS clip length\n{}",
        daemon.diagnostics()
    );
    assert!(
        synth["synth_us"].as_u64().is_some(),
        "synth carries a measured synth_us, got {}\n{}",
        synth["synth_us"],
        daemon.diagnostics()
    );

    // playback_started: the firmware hangover floor from its single source of
    // truth, and the reply's sample count is the fake clip's.
    let started = &events[started];
    assert_eq!(
        started["vad_hangover_floor_ms"],
        800,
        "hangover floor is the firmware constant\n{}",
        daemon.diagnostics()
    );
    assert_eq!(started["samples"], TTS_SAMPLES as u64);

    // latency_summary: parrot mode is the only integration daemon wiring both
    // `[stt]` and `[tts]`, so this is the full stackup — every stage of the
    // segment-and-response cycle stamped and blamed, end to end through a real
    // daemon, real models, and a real (fake-backed) STT and TTS round trip.
    let s = &events[pos("latency_summary")];
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
        "stt_done_ms",
        "brain_ms",
        "speak_rx_ms",
        "tts_done_ms",
        "first_write_ms",
    ] {
        assert!(
            s[field].as_i64().is_some(),
            "latency_summary offset {field} must be present and numeric, got {}\n{}",
            s[field],
            daemon.diagnostics()
        );
    }

    // The blame group partitions `soft_endpoint_rx → first_write` with no overlap
    // and no hole — the property that makes the numbers an accounting rather than
    // an assortment. Every stage stamped, so every interval is real; an echo brain
    // replies with text, so the speak span is split around the synthesis and the
    // unsplit `speak_to_first_write_us` (which would double-count TTS) is absent.
    let blame: u64 = [
        "endpoint_to_stt_us",
        "stt_us",
        "stt_to_brain_us",
        "brain_us",
        "speak_to_synth_start_us",
        "tts_us",
        "synth_to_first_write_us",
    ]
    .iter()
    .map(|f| {
        s[f].as_u64().unwrap_or_else(|| {
            panic!(
                "latency_summary blame {f} must be present and numeric, got {}\n{}",
                s[f],
                daemon.diagnostics()
            )
        })
    })
    .sum();
    assert!(
        s["speak_to_first_write_us"].is_null(),
        "a synthesized reply splits the speak span, got {}\n{}",
        s["speak_to_first_write_us"],
        daemon.diagnostics()
    );
    // The offsets are millisecond-truncated, so the microsecond blame sum matches
    // the span they describe to within one ms per endpoint.
    let span = (s["first_write_ms"].as_i64().unwrap() - s["soft_endpoint_ms"].as_i64().unwrap())
        .unsigned_abs()
        * 1_000;
    assert!(
        blame.abs_diff(span) <= 2_000,
        "blame deltas ({blame} us) must partition the endpoint→write span ({span} us)\n{}",
        daemon.diagnostics()
    );

    // playback_finished: EndOfAudio was written and the frame count is the ceiling
    // of the fake clip's samples over the frame size.
    let finished = &events[finished];
    assert_eq!(finished["eoa_written"], true);
    assert_eq!(finished["frames"], CLIP_FRAMES);

    // Device-side: `replay-pod`'s drain saw exactly one Hello, that many Audio
    // frames, and one EndOfAudio — the readback clip actually crossed the wire.
    let complete = common::find_report_line(&out, "replay_complete");
    let rx = &complete["playback_rx"];
    assert_eq!(rx["hello"], 1, "one eager Hello: {complete}");
    assert_eq!(
        rx["audio"], CLIP_FRAMES,
        "clip frames on the wire: {complete}"
    );
    assert_eq!(rx["end_of_audio"], 1, "one EndOfAudio at drain: {complete}");
    assert_eq!(
        complete["linger"]["eoa_observed"], true,
        "linger must release on the observed EndOfAudio: {complete}"
    );

    // Shut down cleanly, then read the final `stage_health` line and assert the
    // stt/tts/brain/router/playback counters reflect the one parrot round-trip.
    let health = common::final_stage_health(&mut daemon);
    assert_eq!(health["stt"]["requests"], 1, "one STT request: {health}");
    assert_eq!(
        health["stt"]["ok"], 1,
        "the STT request succeeded: {health}"
    );
    assert_eq!(health["tts"]["requests"], 1, "one TTS request: {health}");
    assert_eq!(
        health["tts"]["ok"], 1,
        "the TTS request succeeded: {health}"
    );
    assert_eq!(
        health["brain"]["no_transcript"], 0,
        "a transcript was present, so the echo brain spoke it: {health}"
    );
    assert_eq!(
        health["brain"]["speak_send_failures"], 0,
        "the readback was delivered, not dropped: {health}"
    );
    assert_eq!(
        health["router"]["delivered"], 1,
        "one SpeakCmd routed to a live writer: {health}"
    );
    assert_eq!(
        health["playback"]["jobs_completed"], 1,
        "one playback job completed: {health}"
    );
    assert_eq!(
        health["playback"]["frames_written"], CLIP_FRAMES,
        "clip frames written: {health}"
    );
}
