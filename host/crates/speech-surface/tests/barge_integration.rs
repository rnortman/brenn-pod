//! End-to-end barge-in over fake pods, exercising the full parrot loop: a
//! real daemon with the streaming listener, an `echo` brain, and `[stt]`/`[tts]`
//! pointed at a fake speaches container, driven by an in-test [`common::FakePod`]
//! that stays interactive rather than replaying a fixed log.
//!
//! The whole loop, over one connection: segment 1 (the wake phrase) mints
//! utterance 1, which the echo brain reads back as a long TTS clip; the fake pod
//! waits until that playback is audible on the wire (the barge floor is now open),
//! then injects segment 2 — sustained speech the daemon's real Silero scores past
//! the sustain guard, firing the barge trigger. The daemon flushes the in-flight
//! clip (a `FlushPlayback` frame the pod's drain decodes) and mints utterance 2
//! carrying the interrupted turn's context chain; the echo brain reads that back
//! as *"I think you interrupted me after …"*. The assertion is both JSONL-side
//! (the `barge_in` → `playback_flushed` sequence and utterance 2's chain) and
//! device-side (the `FlushPlayback` actually crossed the wire).

mod common;

use std::path::Path;
use std::time::Duration;

/// Segment ids for the two utterances on the one connection.
const UTTERANCE_SEGMENT_ID: u32 = 1;
const BARGE_SEGMENT_ID: u32 = 2;

/// The fake TTS clip length in S16 samples — long enough (3 s) that the response
/// is still playing when the barge lands, so there is a live clip to flush. The
/// fake speaches serves this same clip for every synthesis, so both the initial
/// echo and the barge readback render to it.
const TTS_SAMPLES: usize = 3 * 16_000;
/// The clip's nominal duration in ms — the interrupted turn's `total_ms`.
const TTS_TOTAL_MS: u64 = TTS_SAMPLES as u64 * 1_000 / 16_000;

/// The transcript the echo brain parrots for utterance 1, so its captured
/// response text (and therefore the chain segment's `response_text`) is known.
const PLAIN_ECHO_CHARS: u64 = common::FAKE_TRANSCRIPT.len() as u64;

/// Generous liveness bounds: every wait rests on an observed wire or JSONL event,
/// never a bare sleep, so a slow CI host waits longer rather than flaking.
const AUDIO_DEADLINE: Duration = Duration::from_secs(20);
const FLUSH_DEADLINE: Duration = Duration::from_secs(20);

#[test]
fn barge_in_flushes_playback_and_chains_the_interrupted_turn() {
    let speaches_url = common::spawn_fake_speaches(TTS_SAMPLES);
    let mut daemon = common::spawn_daemon(&common::echo_parrot_config(&speaches_url));
    let jsonl_path = daemon.jsonl_path.clone();
    let addr = daemon.listen_addr();

    // The wake phrase drives both segments: it arms the wake gate and carves
    // utterance 1, and — reused as segment 2 — its speech sustains past the barge
    // guard. The fake STT transcribes both to `FAKE_TRANSCRIPT`.
    let pcm = common::read_wav_pcm(Path::new(common::WAKE_PHRASE_WAV));
    let seg1 = common::session_frames(&pcm, UTTERANCE_SEGMENT_ID, 0);
    // Segment 2 follows segment 1 on the connection's sample timeline; the device
    // VAD boundary (not a sample gap) separates the two utterances. Its `Hello` is
    // dropped — the connection already introduced itself.
    let seg2 = common::session_frames(&pcm, BARGE_SEGMENT_ID, pcm.len() as u64);

    let mut pod = common::FakePod::connect(&addr);
    pod.send_frames(&seg1);

    // Utterance 1 carves, dispatches, and the echo response begins playing. The
    // first playback `Audio` frame on the wire means the barge floor is open.
    assert!(
        pod.wait_playback_audio(AUDIO_DEADLINE),
        "the echo response never began playing\n{}",
        daemon.diagnostics()
    );

    // Now barge: inject the sustained speech (past segment 2's `Hello`). Scored
    // against the open floor, it fires the sustain guard and cuts the clip.
    pod.send_frames(&seg2[1..]);

    assert!(
        pod.wait_flush(FLUSH_DEADLINE),
        "no FlushPlayback frame crossed the wire\n{}",
        daemon.diagnostics()
    );

    // Utterance 2 carries the barge chain; wait for its `utterance` line (the one
    // with a `barge_in` block) before tearing down.
    common::wait_for_event(&daemon, "barge utterance", common::EVENT_DEADLINE, |v| {
        v["event"] == "utterance" && v.get("barge_in").is_some()
    });

    let tally = pod.finish();
    assert!(
        tally.flush >= 1,
        "the drain decoded a FlushPlayback frame: {tally:?}"
    );

    let events = common::read_events(&jsonl_path);

    // Detection → Mouth: one `barge_in` trigger, then the flush lands on the wire.
    let barge = common::expect_one(&events, "barge_in", &daemon);
    assert_eq!(
        barge["pod"],
        common::BARGE_POD_ID,
        "the barge names the pod that fired it\n{}",
        daemon.diagnostics()
    );
    let flushed = common::expect_one(&events, "playback_flushed", &daemon);
    assert_eq!(
        flushed["was_playing"],
        true,
        "the flush cut the playing clip, not only evicted queued jobs\n{}",
        daemon.diagnostics()
    );

    // The barge utterance: minted with the interrupted turn's context chain. The
    // single segment names utterance 1's transcript and echoed response, and where
    // it was cut.
    let barge_utt = events
        .iter()
        .filter(|v| v["event"] == "utterance")
        .find(|v| v.get("barge_in").is_some())
        .unwrap_or_else(|| panic!("no barge utterance line\n{}", daemon.diagnostics()));
    let chain = barge_utt["barge_in"]["chain"]
        .as_array()
        .unwrap_or_else(|| panic!("barge_in.chain is an array\n{}", daemon.diagnostics()));
    assert_eq!(
        chain.len(),
        1,
        "one interrupted turn in the chain\n{}",
        daemon.diagnostics()
    );
    let seg = &chain[0];
    assert_eq!(
        seg["transcript"],
        common::FAKE_TRANSCRIPT,
        "the chain carries the interrupted turn's transcript\n{}",
        daemon.diagnostics()
    );
    assert_eq!(
        seg["response_text"],
        common::FAKE_TRANSCRIPT,
        "the chain carries the interrupted turn's echoed response\n{}",
        daemon.diagnostics()
    );
    assert_eq!(
        seg["interrupted"]["total_ms"],
        TTS_TOTAL_MS,
        "the cut names the whole clip's duration\n{}",
        daemon.diagnostics()
    );
    let heard = seg["interrupted"]["heard_ms"]
        .as_u64()
        .unwrap_or_else(|| panic!("heard_ms is numeric\n{}", daemon.diagnostics()));
    assert!(
        heard <= TTS_TOTAL_MS,
        "heard_ms ({heard}) cannot exceed the clip ({TTS_TOTAL_MS})\n{}",
        daemon.diagnostics()
    );

    // Mind demonstration: two synths — utterance 1's plain echo of the transcript,
    // and utterance 2's barge readback, which is far longer than a plain echo
    // (the `I think you interrupted me after "…"` scaffolding alone dwarfs it).
    // The readback text is not itself on the JSONL, so its character count stands
    // in for it: only the barge branch produces a reply this long.
    let synth_chars: Vec<u64> = events
        .iter()
        .filter(|v| v["event"] == "synth")
        .filter_map(|v| v["input_chars"].as_u64())
        .collect();
    assert!(
        synth_chars.contains(&PLAIN_ECHO_CHARS),
        "utterance 1 echoed the transcript verbatim ({PLAIN_ECHO_CHARS} chars), got {synth_chars:?}\n{}",
        daemon.diagnostics()
    );
    assert!(
        synth_chars.iter().any(|&c| c > PLAIN_ECHO_CHARS + 40),
        "utterance 2's readback is far longer than a plain echo, got {synth_chars:?}\n{}",
        daemon.diagnostics()
    );

    let health = common::final_stage_health(&mut daemon);
    assert!(
        health["playback"]["jobs_flushed"].as_u64().unwrap_or(0) >= 1,
        "the writer recorded the flush: {health}"
    );
    assert_eq!(
        health["router"]["interrupted"].as_u64().unwrap_or(0),
        0,
        "no queued or in-flight cmd needed eviction in this single-cmd-per-turn run: {health}"
    );
}
