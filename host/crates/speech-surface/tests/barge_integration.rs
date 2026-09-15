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
//! device-side (the `FlushPlayback` actually crossed the wire). The test waits
//! for the readback to reach the wire before it closes the pod, so the JSONL it
//! reads after teardown already holds utterance 2's synthesis.

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

/// How long the fake TTS holds each `/v1/audio/speech` response. Sized so that,
/// were the test to read the JSONL before utterance 2's `synth` line lands, it
/// would do so every run rather than one run in three: the window between the
/// `utterance` line and the `synth` line is otherwise single-digit ms, the same
/// order as a poll interval plus a close and a file read; 250 ms dwarfs that
/// under any load the gate produces, and is also the order of a real TTS
/// backend's latency, which a zero-latency fake misrepresents.
const TTS_DELAY: Duration = Duration::from_millis(250);

/// How long a reply that must not be cut is watched for a flush after the
/// detection it provokes has been reported. The cut, were it to happen, is made
/// on the detection itself and is on the wire within a frame of it, so this is a
/// margin over a decision already taken and not a race against one pending.
const NO_CUT_WINDOW: Duration = Duration::from_millis(500);

#[test]
fn barge_in_flushes_playback_and_chains_the_interrupted_turn() {
    let speaches_url = common::spawn_fake_speaches_with_tts_delay(TTS_SAMPLES, TTS_DELAY);
    // This case injects sustained speech, not the wake phrase: it is the speech
    // rule's end-to-end pin, and it names the mode that runs that rule.
    let config = format!(
        "{}[barge]\nmode = \"speech\"\n",
        common::echo_parrot_config(&speaches_url)
    );
    let mut daemon = common::spawn_daemon(&config);
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

    // The barge utterance is the `utterance` line carrying a `barge_in` block;
    // its daemon-minted id is read from that line rather than assumed, so the
    // wait below keys on the turn the barge actually carved.
    let barge_utt =
        common::wait_for_event(&daemon, "barge utterance", common::EVENT_DEADLINE, |v| {
            v["event"] == "utterance" && v.get("barge_in").is_some()
        });
    let barge_id = barge_utt["id"]
        .as_u64()
        .unwrap_or_else(|| panic!("barge utterance id is numeric\n{}", daemon.diagnostics()));

    // Wait for that utterance's readback to reach the wire before tearing down.
    // The `synth` assertion below reads the JSONL after teardown, so the JSONL
    // must hold the barge utterance's `synth` line before the pod closes.
    // `playback_started` implies `synth`, `utterance`, and `speak_rx` have
    // already been written (one ordered sink, `src/jsonl.rs`); waiting on
    // `utterance` alone would leave the synthesis round trip racing the close.
    common::wait_for_event(
        &daemon,
        "barge readback playing",
        common::EVENT_DEADLINE,
        |v| v["event"] == "playback_started" && v["utterance"] == barge_id,
    );

    let tally = pod.finish();
    assert!(
        tally.flush >= 1,
        "the drain decoded a FlushPlayback frame: {tally:?}"
    );

    let events = common::read_events(&jsonl_path);

    // Detection → Mouth: one `barge_in` trigger, then the flush lands on the wire.
    let barge = common::expect_one(&events, "barge_in", &daemon);
    assert_eq!(
        barge["cause"],
        "speech",
        "the sustained-speech rule is what cut it\n{}",
        daemon.diagnostics()
    );
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
    let synths: Vec<&serde_json::Value> = events.iter().filter(|v| v["event"] == "synth").collect();
    let synth_chars: Vec<u64> = synths
        .iter()
        .filter_map(|v| v["input_chars"].as_u64())
        .collect();
    // The fake TTS holds every response for `TTS_DELAY`; each synth's measured
    // round trip must show at least that hold, or the fake is not taking the
    // real time the teardown ordering above is proved against.
    let synth_us: Vec<u64> = synths
        .iter()
        .filter_map(|v| v["synth_us"].as_u64())
        .collect();
    assert_eq!(
        synth_us.len(),
        synths.len(),
        "every synth line carries a measured synth_us\n{}",
        daemon.diagnostics()
    );
    assert!(
        !synth_us.is_empty()
            && synth_us
                .iter()
                .all(|&us| us >= TTS_DELAY.as_micros() as u64),
        "the fake TTS holds each response for {TTS_DELAY:?}, so every synth must take at least that long, got synth_us {synth_us:?}\n{}",
        daemon.diagnostics()
    );
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
    common::assert_lossless(&health);
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

/// The same loop under the shipping default, where the wake word is the only
/// thing that cuts: the second segment is the wake phrase, and the detection
/// itself — not any sustain run behind it — flushes the reply that is playing.
#[test]
fn a_wake_over_the_readback_cuts_it() {
    let speaches_url = common::spawn_fake_speaches_with_tts_delay(TTS_SAMPLES, TTS_DELAY);
    let daemon = common::spawn_daemon(&common::echo_parrot_config(&speaches_url));
    let jsonl_path = daemon.jsonl_path.clone();
    let addr = daemon.listen_addr();

    let pcm = common::read_wav_pcm(Path::new(common::WAKE_PHRASE_WAV));
    let seg1 = common::session_frames(&pcm, UTTERANCE_SEGMENT_ID, 0);
    let seg2 = common::session_frames(&pcm, BARGE_SEGMENT_ID, pcm.len() as u64);

    let mut pod = common::FakePod::connect(&addr);
    pod.send_frames(&seg1);
    assert!(
        pod.wait_playback_audio(AUDIO_DEADLINE),
        "the echo response never began playing\n{}",
        daemon.diagnostics()
    );

    // The reply's own words are the transcript echoed back, which is not the wake
    // phrase — so the detection this segment provokes is a person's, and cuts.
    pod.send_frames(&seg2[1..]);
    assert!(
        pod.wait_flush(FLUSH_DEADLINE),
        "no FlushPlayback frame crossed the wire\n{}",
        daemon.diagnostics()
    );

    let tally = pod.finish();
    assert!(
        tally.flush >= 1,
        "the drain decoded a FlushPlayback frame: {tally:?}"
    );

    let events = common::read_events(&jsonl_path);
    let barge = common::expect_one(&events, "barge_in", &daemon);
    assert_eq!(
        barge["cause"],
        "wake",
        "the wake word is what cut it, under a mode that judges no speech\n{}",
        daemon.diagnostics()
    );
    let flushed = common::expect_one(&events, "playback_flushed", &daemon);
    assert_eq!(
        flushed["was_playing"],
        true,
        "the flush cut the playing clip\n{}",
        daemon.diagnostics()
    );
}

/// The exemption, end to end, over the one wire between the configured
/// `wake.phrase` and the router that marks a reply.
///
/// The parrot reads back what it heard, so a fake STT that transcribes the wake
/// phrase makes the reply's own words the wake phrase. The second segment's
/// detection is then the machine hearing itself: it is scored, armed and
/// reported, and it cuts nothing. Were the phrase not to reach the router this is
/// the self-conversation the mode exists to prevent — the robot cuts itself off
/// and answers itself — with every unit test still passing.
#[test]
fn a_reply_that_says_the_wake_phrase_is_not_cut_by_its_own_words() {
    let speaches_url =
        common::spawn_fake_speaches_saying(common::WAKE_PHRASE, TTS_SAMPLES, TTS_DELAY);
    let daemon = common::spawn_daemon(&common::echo_parrot_config(&speaches_url));
    let jsonl_path = daemon.jsonl_path.clone();
    let addr = daemon.listen_addr();

    let pcm = common::read_wav_pcm(Path::new(common::WAKE_PHRASE_WAV));
    let seg1 = common::session_frames(&pcm, UTTERANCE_SEGMENT_ID, 0);
    let seg2 = common::session_frames(&pcm, BARGE_SEGMENT_ID, pcm.len() as u64);

    let mut pod = common::FakePod::connect(&addr);
    pod.send_frames(&seg1);
    assert!(
        pod.wait_playback_audio(AUDIO_DEADLINE),
        "the echo response never began playing\n{}",
        daemon.diagnostics()
    );

    // The same phrase again, over the reply that is now saying it back. The wait
    // is on the detection itself — segment 2 lies past segment 1 on the sample
    // timeline, so its wake end is the one beyond the first segment's audio.
    pod.send_frames(&seg2[1..]);
    let second = pcm.len() as u64;
    common::wait_for_event(&daemon, "the second wake detection", FLUSH_DEADLINE, |v| {
        v["event"] == "wake_detected" && v["wake_end_sample"].as_u64().is_some_and(|s| s >= second)
    });
    assert!(
        !pod.wait_flush(NO_CUT_WINDOW),
        "the reply was cut by its own words\n{}",
        daemon.diagnostics()
    );

    let tally = pod.finish();
    assert_eq!(tally.flush, 0, "no FlushPlayback frame crossed: {tally:?}");

    let events = common::read_events(&jsonl_path);
    assert!(
        events.iter().all(|e| e["event"] != "barge_in"),
        "and no rule claims to have cut it\n{}",
        daemon.diagnostics()
    );
}
