//! Replay round-trip integration tests: the golden fixture guard, the
//! recording-off round-trip, the recording-on re-capture fidelity check, and
//! the realtime-pacing lower bound — all through a spawned daemon.

mod common;

use std::path::Path;

use serde_json::Value;
use speech_surface::WakeClass;

/// The committed fixture bytes the generator must reproduce exactly.
const FIXTURE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/short-capture.framelog"
);

/// The generator's output must byte-equal the committed fixture. A mismatch
/// means the frame-log format, the wire encoding, or `AUDIO_PROTOCOL_VERSION`
/// changed — captures already on disk would no longer read the same, so the
/// change must be a reviewed decision. Rerun with `REGEN_FIXTURES=1` to rewrite
/// the file and review the resulting `git diff`.
#[test]
fn generated_fixture_matches_committed_bytes() {
    let generated = common::generate_fixture();

    if std::env::var_os("REGEN_FIXTURES").is_some() {
        std::fs::write(FIXTURE_PATH, &generated).expect("rewrite fixture");
        eprintln!(
            "REGEN_FIXTURES: rewrote {FIXTURE_PATH} ({} bytes)",
            generated.len()
        );
        return;
    }

    let committed = std::fs::read(FIXTURE_PATH).unwrap_or_else(|e| {
        panic!("read fixture {FIXTURE_PATH}: {e}; rerun with REGEN_FIXTURES=1 to create it")
    });

    assert_eq!(
        generated, committed,
        "generated fixture differs from the committed bytes at {FIXTURE_PATH}. \
         If this is an intentional wire/frame-log encoding change, rerun with \
         REGEN_FIXTURES=1 to regenerate and review the git diff."
    );
}

/// End-to-end round trip with recording off: spawn the real daemon, replay the
/// committed fixture through the real `replay-pod`, and assert the full JSONL
/// vocabulary — session/segment lifecycle, ingest-stage timings, and DoA/energy
/// propagation — keyed to the fixture's known facts. Then SIGTERM and assert a
/// clean shutdown with a final `stage_health` line.
#[test]
fn roundtrip_recording_off_asserts_jsonl_timings_and_doa() {
    let facts = common::fixture_facts();
    let mut daemon = common::spawn_daemon(&common::daemon_config(None));
    let jsonl_path = daemon.jsonl_path.clone();
    let addr = daemon.listen_addr();

    let out = common::run_replay(&addr, "fast", Path::new(FIXTURE_PATH));
    common::assert_replay_ok(&out, &daemon);

    // Drain the whole capture through the pipeline — including segment B's gate
    // line, which the pipeline emits *after* its tracking line — before the
    // snapshot, so every per-connection and per-segment line is on disk.
    common::wait_until_drained(&daemon, facts.seg_b.segment_id);

    // replay-pod's own JSONL contract on stdout: the connect line, the per-log
    // completion with its counts, and the run summary, keyed to the fixture.
    let replay_events: Vec<Value> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .collect();
    let replay_named = |name: &str| -> Vec<&Value> {
        replay_events
            .iter()
            .filter(|v| v["event"] == name)
            .collect()
    };
    let connected = replay_named("replay_connected");
    assert_eq!(connected.len(), 1);
    assert_eq!(connected[0]["pace"], "fast");
    let log_done = replay_named("replay_log_done");
    assert_eq!(log_done.len(), 1);
    assert_eq!(log_done[0]["frames"], facts.record_count);
    assert!(log_done[0]["bytes"].as_u64().is_some());
    assert_eq!(log_done[0]["pace"], "fast");
    let complete = replay_named("replay_complete");
    assert_eq!(complete.len(), 1);
    assert_eq!(complete[0]["logs"], 1);
    assert_eq!(complete[0]["frames"], facts.record_count);

    let events = common::read_events(&jsonl_path);
    let named =
        |name: &str| -> Vec<&Value> { events.iter().filter(|v| v["event"] == name).collect() };
    let by_segment = |name: &str, segment_id: u32| -> Value {
        named(name)
            .into_iter()
            .find(|v| v["segment_id"] == segment_id)
            .unwrap_or_else(|| {
                panic!(
                    "no {name} for segment {segment_id}\n{}",
                    daemon.diagnostics()
                )
            })
            .clone()
    };

    // conn_hello: the fixture pod, unmapped (no pod→room map in the config).
    let hello = named("conn_hello");
    assert_eq!(hello.len(), 1);
    assert_eq!(hello[0]["pod_id"], facts.pod_id);
    assert_eq!(hello[0]["unmapped"], true);

    // Two segments opened; A carries its preroll.
    assert_eq!(named("segment_opened").len(), 2);
    let opened_a = by_segment("segment_opened", facts.seg_a.segment_id);
    assert_eq!(opened_a["preroll"], facts.seg_a.preroll_samples);

    // segment_closed A: clean VAD release; every ingest-stage stamp and the
    // receive→assembled delta present.
    assert_eq!(named("segment_closed").len(), 2);
    let closed_a = by_segment("segment_closed", facts.seg_a.segment_id);
    assert_eq!(closed_a["end_cause"], "vad_release");
    assert_eq!(closed_a["truncated"], false);
    assert_eq!(closed_a["gap_count"], 0);
    assert_eq!(closed_a["samples"], facts.seg_a.samples);
    assert!(closed_a["timings"]["first_frame_rx"].as_u64().is_some());
    assert!(closed_a["timings"]["segment_end_rx"].as_u64().is_some());
    assert!(closed_a["timings"]["assembled"].as_u64().is_some());
    assert!(closed_a["rx_to_assembled_us"].as_u64().is_some());

    // segment_closed B: truncated at EOF (capture ends mid-segment).
    let closed_b = by_segment("segment_closed", facts.seg_b.segment_id);
    assert_eq!(closed_b["truncated"], true);

    // tracking ×2; A carries the DoA track (NaN slot → JSON null), the energy
    // track, and the assembled→tracking delta — the end-to-end DoA assertion.
    assert_eq!(named("tracking").len(), 2);
    let track_a = by_segment("tracking", facts.seg_a.segment_id);
    assert_eq!(track_a["doa"][0][0], facts.azimuth_offset_samples);
    assert_eq!(track_a["doa"][0][1][0], facts.azimuths[0] as f64);
    assert!(
        track_a["doa"][0][1][1].is_null(),
        "NaN azimuth slot → JSON null"
    );
    assert_eq!(track_a["doa"][0][1][2], facts.azimuths[2] as f64);
    assert_eq!(track_a["energy"][0][0], facts.spenergy_offset_samples);
    let energy0 = track_a["energy"][0][1][0]
        .as_f64()
        .expect("energy reading is a number");
    assert!(
        (energy0 - facts.spenergy[0] as f64).abs() < 1e-6,
        "energy reading {energy0} ≈ {}",
        facts.spenergy[0]
    );
    assert!(track_a["assembled_to_tracking_us"].as_u64().is_some());

    // No listener is configured (no [wake]/[endpointer] tables), so nothing arms
    // and nothing is carved: no wake detection and no utterance. Segments are the
    // transport/tracking unit only.
    assert_eq!(named("wake_detected").len(), 0);
    assert_eq!(named("utterance").len(), 0);

    // Clean EOF close.
    assert_eq!(named("conn_closed")[0]["cause"], "eof");

    // Graceful shutdown: SIGTERM, clean exit, and the final stage_health line.
    daemon.sigterm_and_wait();
    let after = common::read_events(&jsonl_path);
    assert!(
        after.iter().any(|v| v["event"] == "stage_health"),
        "final stage_health after shutdown; events: {after:?}"
    );
}

/// Re-capture fidelity with recording on: replay the fixture into a daemon that
/// records, shut it down cleanly, then prove the re-captured frame log's record
/// **payload sequence byte-equals the fixture's** (the daemon taps the read
/// buffer pre-decode, so replay → re-capture is the identity the architecture
/// promises — capture, replay, identical re-capture). `host_rx` values differ
/// (a fresh capture on a fresh clock), so only payloads are compared. The
/// sidecar must list both segments as `negative` (no listener is configured, so no
/// wake detection lands in either span — the segment path labels each provisionally
/// negative) with B truncated.
#[test]
fn recapture_recording_on_matches_fixture_payloads_and_sidecar() {
    let facts = common::fixture_facts();
    let record_dir = tempfile::tempdir().expect("record tempdir");
    let mut daemon = common::spawn_daemon(&common::daemon_config(Some(record_dir.path())));
    let addr = daemon.listen_addr();

    let out = common::run_replay(&addr, "fast", Path::new(FIXTURE_PATH));
    common::assert_replay_ok(&out, &daemon);

    // Drain the whole capture, then shut down cleanly so the frame log is
    // flushed and the sidecar finalized.
    common::wait_until_drained(&daemon, facts.seg_b.segment_id);
    daemon.sigterm_and_wait();

    // Payload-sequence identity: fixture bytes in, identical bytes on disk.
    let framelog = common::find_one_framelog(record_dir.path());
    let recaptured = common::log_payloads(&framelog);
    let original = common::log_payloads(Path::new(FIXTURE_PATH));
    assert_eq!(
        recaptured,
        original,
        "re-captured payload sequence must byte-equal the fixture's ({} vs {} records)\n{}",
        recaptured.len(),
        original.len(),
        daemon.diagnostics(),
    );

    // Sidecar: pod identity, both segments negative (no listener ⇒ no wake landed
    // in either span), B truncated.
    let sidecar = speech_surface::Sidecar::read(&speech_surface::sidecar_path(&framelog))
        .unwrap_or_else(|e| panic!("read sidecar for {}: {e}", framelog.display()));
    assert_eq!(sidecar.pod_id, facts.pod_id);
    assert_eq!(sidecar.segments.len(), 2, "sidecar lists both segments");
    let seg = |id: u32| {
        sidecar
            .segments
            .iter()
            .find(|s| s.segment_id == id)
            .unwrap_or_else(|| panic!("sidecar segment {id} missing: {:?}", sidecar.segments))
    };
    let sc_a = seg(facts.seg_a.segment_id);
    assert_eq!(sc_a.wake, WakeClass::Negative);
    assert!(!sc_a.truncated, "segment A is a clean VAD release");
    let sc_b = seg(facts.seg_b.segment_id);
    assert_eq!(sc_b.wake, WakeClass::Negative);
    assert!(sc_b.truncated, "segment B truncated at EOF");
}

/// Realtime pacing, lower bound only: replay the fixture at the default
/// `realtime` pace and assert the send loop actually slept. The bound is
/// strict, not a heuristic — the inter-record delays sum to the capture span
/// (first record sends immediately; each later delay is the `host_rx` delta),
/// and `sleep` guarantees at least its duration, so `wall_us` ≥ the span always
/// holds; slow CI only adds time. The tool's own `wall_us` (send-loop only,
/// excluding spawn and post-EOF finalize) is used rather than external process
/// timing so spawn overhead can never satisfy the bound in place of real
/// sleeps. No upper bound (that would assert machine speed). The pacing math
/// itself is covered by the `Pacer` unit tests; this proves the loop sleeps.
#[test]
fn realtime_pacing_wall_time_meets_capture_span_lower_bound() {
    let facts = common::fixture_facts();
    let daemon = common::spawn_daemon(&common::daemon_config(None));
    let addr = daemon.listen_addr();

    let out = common::run_replay(&addr, "realtime", Path::new(FIXTURE_PATH));
    common::assert_replay_ok(&out, &daemon);

    let done = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .find(|v| v["event"] == "replay_log_done")
        .unwrap_or_else(|| {
            panic!(
                "no replay_log_done in replay-pod output\n--- stdout ---\n{}",
                String::from_utf8_lossy(&out.stdout)
            )
        });

    let wall_us = done["wall_us"]
        .as_u64()
        .expect("wall_us present and numeric");
    assert!(
        wall_us >= facts.capture_span_us,
        "realtime wall {wall_us} us must be ≥ capture span {} us (the loop must sleep the \
         inter-record deltas)",
        facts.capture_span_us
    );
}

/// An unusable key file stops the tool before it costs a connection attempt,
/// with a machine-readable reason and a hard-failure exit — the only key-loading
/// failure path an operator can hit, and the one every other test's valid
/// `--psk-file` hides. The key never reaches the message; a missing file has
/// none, and the 63-character case must be named without echoing its contents.
#[test]
fn unusable_psk_file_exits_hard_with_a_named_reason() {
    let dir = tempfile::tempdir().expect("tempdir");
    let missing = dir.path().join("absent.hex");
    let short_path = dir.path().join("short.hex");
    let short_hex = "a".repeat(63);
    std::fs::write(&short_path, &short_hex).expect("write short key file");

    for (path, expect_in_detail) in [(&missing, "absent.hex"), (&short_path, "63 characters")] {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_replay-pod"))
            .arg("--connect")
            .arg("127.0.0.1:9")
            .arg("--pod-id")
            .arg("pod-replay")
            .arg("--psk-file")
            .arg(path)
            .arg(FIXTURE_PATH)
            .output()
            .expect("run replay-pod");
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert_eq!(
            out.status.code(),
            Some(1),
            "hard failure for {}\n{stdout}",
            path.display()
        );
        let lines: Vec<Value> = stdout
            .lines()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .collect();
        let unusable: Vec<&Value> = lines
            .iter()
            .filter(|v| v["event"] == "psk_unusable")
            .collect();
        assert_eq!(unusable.len(), 1, "exactly one psk_unusable line: {stdout}");
        let detail = unusable[0]["detail"].as_str().expect("detail is a string");
        assert!(
            detail.contains(path.to_str().unwrap()) && detail.contains(expect_in_detail),
            "detail names the file and the reason: {detail}"
        );
        assert!(
            lines.iter().all(|v| v["event"] != "replay_connected"),
            "no connection was attempted: {stdout}"
        );
        assert!(
            !stdout.contains(&short_hex),
            "no key material in the output: {stdout}"
        );
    }
}
