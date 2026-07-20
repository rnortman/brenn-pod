//! Human console view: a compact, always-on narration of the event stream,
//! rendered to stderr while the full JSONL stream goes to its own sink (or
//! nowhere).
//!
//! [`wants`] is the cheap name filter run on every emit: it decides whether an
//! event is worth a console line at all, rejecting the high-rate `tracking`
//! stream before any serialization. [`Renderer`] turns an accepted event into a
//! line — a lossy projection of the JSON, keyed on the event name plus tolerant
//! `Value` field access. It is pure and synchronous, so it lives in the single
//! console task and needs no locking.

use serde_json::Value;
use speech_pipeline::SPINE_FORMAT;
use std::collections::HashMap;

/// Events that always warrant a console line even though their names carry no
/// error token: the startup/progress narration a human wants to watch. Any
/// other event is shown only when its name matches an error token (see
/// [`has_error_token`]); everything else stays file-only.
const CONSOLE_INFO: &[&str] = &[
    "daemon_start",
    "listening",
    "wake_bypassed",
    "brain_absent",
    "brain_clip_loaded",
    "brain_echo",
    "stt_absent",
    "stt_configured",
    "tts_absent",
    "tts_configured",
    "conn_hello",
    "conn_superseded",
    "conn_closed",
    "segment_opened",
    "segment_closed",
    "wake_decision",
    "wake_detected",
    "wake_command_absent",
    // The listener's utterance lifecycle. All fire at speech-boundary rate (a
    // handful per utterance), never per-chunk, so the console stays bounded.
    "endpointer_transition",
    "utterance_superseded",
    "utterance_closed",
    "arm_expired",
    // Bounded by construction: one line per model per ~8 s of audio per pod, plus
    // one per transition. Never per-chunk.
    "model_stats",
    "utterance",
    // The segment-and-response cycle's stage instants, and the one line that
    // references them all to t0 at the moment the response starts playing. One
    // each per dispatched utterance.
    "stt_started",
    "brain_dispatched",
    "speak_rx",
    "synth",
    "playback_started",
    "latency_summary",
    "playback_finished",
    "stage_health",
];

/// Name substrings that mark an event as a failure or warning. An event whose
/// name contains any of these is rendered loud even without an explicit
/// mapping, so a new failure event named after in-repo precedent is loud from
/// day one. The trailing entries exist because the vocabulary is not
/// token-uniform: `brain_sink_full`, `*_no_*`, `*_skipped`, and
/// `playback_aborted` are failure/warning events the earlier tokens miss. The
/// `console_classification_is_exhaustive` test pins the current mapping: it
/// fails if any event the `EVENTS` table classifies `Loud` stops rendering loud
/// (a token deleted here, an explicit mapping dropped). It cannot catch a
/// brand-new failure event that is never added to that table — that step stays
/// convention-dependent.
const ERROR_TOKENS: &[&str] = &[
    "error",
    "failed",
    "fatal",
    "panic",
    "dead",
    "dropped",
    "rejected",
    "halted",
    "corrupt",
    "unsupported",
    "exited",
    "full",
    "skipped",
    "aborted",
    "_no_",
];

/// Long string fields (transcript, error detail) truncate here; the file keeps
/// the full text. Counted in characters, not bytes.
const MAX_FIELD_CHARS: usize = 200;

/// The `stage_health` counter leaves that name a failure or anomaly. Matched by
/// bare leaf name at any nesting depth, so a future stage with a
/// conventionally-named counter is covered without touching this list. The leaf
/// name decides *membership* only; the delta snapshot keys on the full dotted
/// path (see [`collect_counter_deltas`]) because these names are not unique —
/// `failed` and `timeouts` occur under both `stt` and `tts`, and a bare-name key
/// would let one stage's value mask the other's.
const HEALTH_COUNTER_LEAVES: &[&str] = &[
    "dropped_oldest",
    "errors",
    "jobs_rejected_full",
    "jobs_rejected_dead",
    "jobs_aborted",
    "write_timeouts",
    "eoa_write_failures",
    "speak_send_failures",
    "send_failures",
    "no_transcript",
    "no_pod",
    "unsupported",
    "failed",
    "timeouts",
    "jsonl_dropped",
    "console_dropped",
    "ledger_evictions",
    "telemetry_outside_segment",
    "clock_step_clamps",
];

/// True for events the console should render. The hot path is one string
/// comparison for the rejected high-rate `tracking` stream.
pub(crate) fn wants(event: &str) -> bool {
    if event == "tracking" {
        return false;
    }
    CONSOLE_INFO.contains(&event) || has_error_token(event)
}

fn has_error_token(event: &str) -> bool {
    ERROR_TOKENS.iter().any(|token| event.contains(token))
}

/// Whether an event renders loud (`!!! ` prefix, red on a terminal). Error-token
/// names are loud; `wake_bypassed` is a loud warning (the gate is open); a
/// `wake_decision` is loud unless its outcome is one of the known calm verdicts,
/// so an `error` or any unrecognized future outcome surfaces loudly rather than
/// as a calm line.
fn is_loud(event: &str, fields: &Value) -> bool {
    if event == "wake_bypassed" {
        return true;
    }
    if event == "wake_decision" {
        return !matches!(
            fields.get("outcome").and_then(Value::as_str),
            Some("positive") | Some("negative") | Some("bypassed")
        );
    }
    has_error_token(event)
}

/// Renders accepted events into console lines. Pure and synchronous; owned by
/// the single console task so its state (added as the mapping grows) needs no
/// mutex.
pub(crate) struct Renderer {
    /// Previous values of the curated `stage_health` counters, keyed by full
    /// dotted JSON path (`stt.failed`), for delta detection across periodic
    /// health lines. Advances only when a health line is actually delivered to
    /// this task, so after a console blackout the next line reports the
    /// cumulative movement since the last delivered one.
    last_counters: HashMap<String, u64>,
    /// Emit ANSI bold-red around loud lines. Set from whether stderr is a
    /// terminal; loudness never depends on it (the `!!! ` prefix survives).
    color: bool,
}

impl Renderer {
    pub(crate) fn new(color: bool) -> Self {
        Renderer {
            last_counters: HashMap::new(),
            color,
        }
    }

    /// Project one event onto console output, or `None` to render nothing. The
    /// return is one or more newline-separated console lines (the `at_shutdown`
    /// health line yields two: the calm run summary plus the loud delta); the
    /// writer emits the string verbatim with a trailing newline, so an embedded
    /// `\n` produces two terminated lines. Never panics on a malformed event:
    /// missing or mistyped fields degrade to `?` rather than aborting the
    /// console task.
    pub(crate) fn render(&mut self, ts_ms: u64, event: &str, fields: &Value) -> Option<String> {
        // `wants` is the single owner of which events reach the console; render
        // re-checks it so a directly-constructed Renderer also rejects an
        // unwanted event (e.g. the high-rate `tracking` stream).
        if !wants(event) {
            return None;
        }
        // `stage_health` is never narrated as prose; it surfaces only when a
        // curated error counter moves (the loud backstop below).
        if event == "stage_health" {
            return self.render_stage_health(ts_ms, fields);
        }

        let tag = tag_string(fields);
        let loud = is_loud(event, fields);

        let mut body = String::new();
        if loud {
            // Loud lines keep the generic shape — event name plus every field —
            // so no diagnostic detail is lost regardless of bespoke prose.
            body.push_str("!!! ");
            body.push_str(&sanitize(event));
            if !tag.is_empty() {
                body.push(' ');
                body.push_str(&tag);
            }
            let kvs = kv_string(fields);
            if !kvs.is_empty() {
                body.push(' ');
                body.push_str(&kvs);
            }
        } else if let Some(prose) = narrate(event, fields) {
            if !tag.is_empty() {
                body.push_str(&tag);
                body.push(' ');
            }
            body.push_str(&prose);
        } else {
            if !tag.is_empty() {
                body.push_str(&tag);
                body.push(' ');
            }
            body.push_str(&sanitize(event));
            let kvs = kv_string(fields);
            if !kvs.is_empty() {
                body.push(' ');
                body.push_str(&kvs);
            }
        }

        Some(self.finish(ts_ms, body, loud))
    }

    /// Wrap a rendered body into the final line: bold-red when the line is loud
    /// and color is on, then the `HH:MM:SS.mmm` timestamp prefix. Single owner of
    /// the loud-line presentation so every loud line — generic, bespoke, and the
    /// `stage_health` backstop — stays visually identical.
    fn finish(&self, ts_ms: u64, body: String, loud: bool) -> String {
        let ts = format_tod(ts_ms);
        let body = if loud && self.color {
            format!("\x1b[1;31m{body}\x1b[0m")
        } else {
            body
        };
        format!("{ts} {body}")
    }

    /// The periodic `stage_health` backstop: surface the counter-only failures
    /// that have no discrete event (a `write_timeout`, a dropped loud line). The
    /// periodic line stays file-only while healthy; a positive delta on any
    /// curated counter since the last delivered health line produces one loud
    /// line naming each mover, sorted by path (`!!! stage_health: jsonl_dropped
    /// +5, stt.failed +2`). The first line of a run diffs against zero, so a counter already
    /// nonzero at first emission is reported. The snapshot advances on every
    /// delivered line, so after a console blackout the next line reports the
    /// cumulative movement since the pre-blackout snapshot.
    ///
    /// The final line (`at_shutdown: true`) always renders a calm one-line run
    /// summary, followed by the loud delta line when any error counter moved
    /// since the last report — so a session ends with a recap even when nothing
    /// failed.
    fn render_stage_health(&mut self, ts_ms: u64, fields: &Value) -> Option<String> {
        let mut movers = Vec::new();
        collect_counter_deltas(fields, "", &mut self.last_counters, &mut movers);
        // Sort so the mover order is a property of this code, not of
        // serde_json's map iteration (which a `preserve_order` feature
        // unification elsewhere in the workspace could flip to insertion order).
        movers.sort();
        let delta = (!movers.is_empty()).then(|| {
            self.finish(
                ts_ms,
                format!("!!! stage_health: {}", movers.join(", ")),
                true,
            )
        });

        if fields.get("at_shutdown").and_then(Value::as_bool) != Some(true) {
            return delta;
        }
        let summary = self.finish(ts_ms, run_summary(fields), false);
        Some(match delta {
            Some(delta) => format!("{summary}\n{delta}"),
            None => summary,
        })
    }
}

/// The final `at_shutdown` line's run recap:
/// `run summary — 12 segments, wake 3 detected / 8 rejected, 3 playback jobs,
/// 0 drops`. A calm, always-rendered tally of the run's throughput — segments
/// pushed, wake verdicts, completed playback jobs, and segments dropped at the
/// ingest overflow boundary. Each field degrades to `?` when absent or mistyped.
fn run_summary(fields: &Value) -> String {
    let queue = fields.get("segment_queue");
    let wake = fields.get("wake");
    let pushed = fmt_u64(queue.and_then(|q| q.get("pushed")));
    let detected = fmt_u64(wake.and_then(|w| w.get("detected")));
    let rejected = fmt_u64(wake.and_then(|w| w.get("rejected")));
    let jobs = fmt_u64(fields.get("playback").and_then(|p| p.get("jobs_completed")));
    let drops = fmt_u64(queue.and_then(|q| q.get("dropped_oldest")));
    format!(
        "run summary — {pushed} segments, wake {detected} detected / {rejected} rejected, {jobs} playback jobs, {drops} drops"
    )
}

/// Walk a `stage_health` `Value` tree, updating `snapshot` (keyed by full dotted
/// path) for every curated counter leaf and recording each positive delta in
/// `movers` as `path +N`. A leaf whose bare name is in [`HEALTH_COUNTER_LEAVES`]
/// is a counter regardless of nesting depth; the dotted path keys the snapshot
/// so two stages sharing a leaf name (`stt.failed` / `tts.failed`) never mask
/// each other. Non-object values, non-numeric leaves, and non-curated leaves are
/// skipped, so a malformed tree never panics.
fn collect_counter_deltas(
    value: &Value,
    path: &str,
    snapshot: &mut HashMap<String, u64>,
    movers: &mut Vec<String>,
) {
    let Some(obj) = value.as_object() else {
        return;
    };
    for (key, child) in obj {
        // Keys are compile-time serde field names today, but the walk sweeps
        // curated leaves at any depth, so a future map-shaped section keyed by an
        // off-host identifier (a pod id) would flow its key into the loud line.
        // Sanitize here so the snapshot key and mover string cannot carry terminal
        // control codes regardless of where the key came from.
        let key = sanitize(key);
        let child_path = if path.is_empty() {
            key.clone()
        } else {
            format!("{path}.{key}")
        };
        if child.is_object() {
            collect_counter_deltas(child, &child_path, snapshot, movers);
        } else if HEALTH_COUNTER_LEAVES.contains(&key.as_str()) {
            if let Some(current) = child.as_u64() {
                let previous = snapshot.insert(child_path.clone(), current).unwrap_or(0);
                if current > previous {
                    movers.push(format!("{child_path} +{}", current - previous));
                }
            }
        }
    }
}

/// Bespoke calm-narration prose for events that read better than the generic
/// `event key=value` line. `None` means "no bespoke form" — the caller falls
/// back to the generic line. Only non-loud outcomes narrate here; loud outcomes
/// (e.g. a `wake_decision` error) render through the loud path with full fields.
///
/// Each narrator reads only the event's own fields; the caller prepends the
/// `[room/pod]` tag. Every field degrades to `?` when absent or the wrong type
/// (via [`fmt_str`] / [`fmt_u64`] / [`fmt_score`] / [`fmt_duration_s`] /
/// [`fmt_ms_from_us`]), so a malformed event still yields a line rather than
/// panicking.
fn narrate(event: &str, fields: &Value) -> Option<String> {
    match event {
        "daemon_start" => Some(narrate_daemon_start(fields)),
        "listening" => Some(narrate_listening(fields)),
        "wake_decision" => narrate_wake_decision(fields),
        "wake_detected" => Some(narrate_wake_detected(fields)),
        "endpointer_transition" => Some(narrate_endpointer_transition(fields)),
        "model_stats" => Some(narrate_model_stats(fields)),
        "utterance_superseded" => Some(narrate_utterance_superseded(fields)),
        "utterance_closed" => Some(narrate_utterance_closed(fields)),
        "arm_expired" => Some(narrate_arm_expired(fields)),
        "wake_command_absent" => Some(narrate_wake_command_absent(fields)),
        "utterance" => Some(narrate_utterance(fields)),
        "segment_opened" => Some(narrate_segment_opened(fields)),
        "segment_closed" => Some(narrate_segment_closed(fields)),
        "stt_started" => Some(narrate_stt_started(fields)),
        "brain_dispatched" => Some(narrate_brain_dispatched(fields)),
        "speak_rx" => Some(narrate_speak_rx(fields)),
        "synth" => Some(narrate_synth(fields)),
        "playback_started" => Some(narrate_playback_started(fields)),
        "latency_summary" => Some(narrate_latency_summary(fields)),
        "playback_finished" => Some(narrate_playback_finished(fields)),
        "conn_hello" => Some(narrate_conn_hello(fields)),
        "conn_superseded" => Some(narrate_conn_superseded(fields)),
        "conn_closed" => Some(narrate_conn_closed(fields)),
        "brain_absent" => Some("brain: none".to_string()),
        "brain_echo" => Some("brain: echo".to_string()),
        "brain_clip_loaded" => Some(narrate_brain_clip(fields)),
        "stt_absent" => Some("stt: none".to_string()),
        "stt_configured" => Some(narrate_stt_configured(fields)),
        "tts_absent" => Some("tts: none".to_string()),
        "tts_configured" => Some(narrate_tts_configured(fields)),
        _ => None,
    }
}

/// The configured text-to-speech stage as prose:
/// `tts configured — http://10.0.0.5:8000 model=speaches-ai/Kokoro-82M
/// voice=af_heart`. The `tts_absent` sibling is the fixed label `tts: none`. The
/// endpoint, model, and voice degrade to `?` when absent or the wrong type.
fn narrate_tts_configured(fields: &Value) -> String {
    let url = fmt_str(fields.get("url"));
    let model = fmt_str(fields.get("model"));
    let voice = fmt_str(fields.get("voice"));
    format!("tts configured — {url} model={model} voice={voice}")
}

/// The configured speech-to-text stage as prose:
/// `stt configured — http://10.0.0.5:8000 model=Systran/faster-whisper-small
/// lang=en`. The `stt_absent` sibling is the fixed label `stt: none`. The
/// endpoint and model degrade to `?` when absent or the wrong type; the language
/// is optional (`[stt] language` may be unset), so a null/absent value renders
/// `lang=auto` rather than `?`.
fn narrate_stt_configured(fields: &Value) -> String {
    let url = fmt_str(fields.get("url"));
    let model = fmt_str(fields.get("model"));
    // Absent or JSON `null` is the intentional "no language pin" case and renders
    // `auto`; a wrong-typed value is a malformed event and degrades to `?`, matching
    // every other narrator rather than masquerading as a valid `auto`.
    let lang = match fields.get("language") {
        None | Some(Value::Null) => "auto".to_string(),
        Some(Value::String(l)) => sanitize(l),
        Some(_) => "?".to_string(),
    };
    format!("stt configured — {url} model={model} lang={lang}")
}

/// The configured brain as prose. `brain_absent` / `brain_echo` are fixed
/// labels (`brain: none` / `brain: echo`); `brain_clip_loaded` names the clip
/// and its duration, taken from the event's own `duration_ms` (the single source
/// the emit site already computed): `brain: clip ./ack.wav (1.9s)`. The clip path
/// degrades to `?` and the duration to `?` when absent or the wrong type.
fn narrate_brain_clip(fields: &Value) -> String {
    let clip = fmt_str(fields.get("clip"));
    let dur = fmt_duration_ms(fields.get("duration_ms"), 1);
    format!("brain: clip {clip} ({dur})")
}

/// The daemon startup header as prose:
/// `speech-surface starting — listen 10.0.0.5:7380, record ./framelogs (on),
/// events → /var/log/speech.jsonl`. The record kill-switch renders `(on)` /
/// `(off)`, and the events destination is the configured JSONL sink label
/// (`none` / `stdout` / path). Each field degrades to `?` when absent or the
/// wrong type; the `max_connections` field is left to the file stream.
fn narrate_daemon_start(fields: &Value) -> String {
    let listen = fmt_str(fields.get("listen_addr"));
    let record = fmt_str(fields.get("record_dir"));
    let enabled = match fields.get("record_enabled").and_then(Value::as_bool) {
        Some(true) => "on",
        Some(false) => "off",
        None => "?",
    };
    let sink = fmt_str(fields.get("jsonl_sink"));
    format!(
        "speech-surface starting — listen {listen}, record {record} ({enabled}), events → {sink}"
    )
}

/// The resolved listen address as prose: `listening on 10.0.0.5:7380`. The
/// address degrades to `?` when absent or non-string (the rare `local_addr`
/// error path emits a `null` address plus a `detail`), and that `detail` is
/// appended when present so the failure-path diagnostic survives on the calm
/// line.
fn narrate_listening(fields: &Value) -> String {
    let addr = fmt_str(fields.get("addr"));
    let mut line = format!("listening on {addr}");
    if let Some(detail) = fields.get("detail").and_then(Value::as_str) {
        line.push_str(&format!(" ({})", sanitize(detail)));
    }
    line
}

/// A superseding reconnect as prose: `reconnected, superseding conn 6`. The pod
/// identity is carried by the `[pod]` tag; the connection number named here is
/// the older one being displaced (`old_conn_seq`), which degrades to `?` when
/// absent or non-numeric.
fn narrate_conn_superseded(fields: &Value) -> String {
    let old = fmt_u64(fields.get("old_conn_seq"));
    format!("reconnected, superseding conn {old}")
}

/// A closed pod connection as prose: `disconnected (conn 7, eof)`. The close
/// cause is the socket-level or protocol reason carried on the event; it
/// degrades to `?` when absent or non-string.
fn narrate_conn_closed(fields: &Value) -> String {
    let seq = fmt_u64(fields.get("conn_seq"));
    let cause = fmt_str(fields.get("cause"));
    format!("disconnected (conn {seq}, {cause})")
}

/// A pod handshake as prose: `connected (conn 1)`, plus a ` — room unmapped`
/// warning when the pod's room is not configured (its audio is then captured
/// under the raw id, not a room).
fn narrate_conn_hello(fields: &Value) -> String {
    let seq = fmt_u64(fields.get("conn_seq"));
    let warn = if fields.get("unmapped").and_then(Value::as_bool) == Some(true) {
        " — room unmapped"
    } else {
        ""
    };
    format!("connected (conn {seq}){warn}")
}

/// Playback completion as prose:
/// `■ playback finished (reply to #3, 96 frames, EOA sent)`, or with
/// `unsolicited` in place of the reply clause for a playback that answers no
/// utterance (a `null` `utterance` field). The EOA phrase degrades to `EOA ?`
/// when the flag is absent or non-boolean.
fn narrate_playback_finished(fields: &Value) -> String {
    let frames = fmt_u64(fields.get("frames"));
    let eoa = match fields.get("eoa_written").and_then(Value::as_bool) {
        Some(true) => "EOA sent",
        Some(false) => "no EOA",
        None => "EOA ?",
    };
    let reply = reply_clause(fields.get("utterance"));
    format!("■ playback finished ({reply}, {frames} frames, {eoa})")
}

/// Playback start as prose: `▶ playback started (reply to #3, 1.9s audio)`, or
/// with `unsolicited` in place of the reply clause for a playback that answers
/// no utterance (a `null` `utterance` field). The duration is derived from the
/// sample count at the spine sample rate.
fn narrate_playback_started(fields: &Value) -> String {
    let dur = fmt_duration_s(fields.get("samples"), 1);
    let reply = reply_clause(fields.get("utterance"));
    format!("▶ playback started ({reply}, {dur} audio)")
}

/// A completed text-to-speech synthesis as prose:
/// `synthesized (reply to #3, 27 chars → 1.9s audio, 312 ms)`. The reply clause
/// mirrors the playback lines (a `null` `utterance` reads `unsolicited`); the
/// input length, resulting audio duration (from the sample count at the spine
/// rate), and synthesis latency each degrade to `?` when absent or mistyped.
fn narrate_synth(fields: &Value) -> String {
    let reply = reply_clause(fields.get("utterance"));
    let chars = fmt_u64(fields.get("input_chars"));
    let dur = fmt_duration_s(fields.get("samples"), 1);
    let synth_ms = fmt_ms_from_us(fields.get("synth_us"));
    format!("synthesized ({reply}, {chars} chars → {dur} audio, {synth_ms})")
}

/// A speculative STT spawn as prose: `stt started (seq 4, 2.7s audio)`. The
/// audio duration is derived from the carved sample count at the spine rate; both
/// fields degrade to `?` when absent or mistyped.
fn narrate_stt_started(fields: &Value) -> String {
    let seq = fmt_u64(fields.get("utterance_seq"));
    let dur = fmt_duration_s(fields.get("samples"), 1);
    format!("stt started (seq {seq}, {dur} audio)")
}

/// A brain dispatch as prose: `utterance #3 → brain`. Brain begin; the reply
/// shows later as `speak_rx`. The id degrades to `?` when absent or mistyped.
fn narrate_brain_dispatched(fields: &Value) -> String {
    let id = fmt_u64(fields.get("utterance"));
    format!("utterance #{id} → brain")
}

/// A brain reply reaching the router as prose: `speak received (reply to #3,
/// text)`. Brain end. The body kind is the event's own label and degrades to `?`
/// when absent or mistyped.
fn narrate_speak_rx(fields: &Value) -> String {
    let reply = reply_clause(fields.get("utterance"));
    let body = fmt_str(fields.get("body"));
    format!("speak received ({reply}, {body})")
}

/// The whole segment-and-response cycle on one line, every stage referenced to
/// t0 (host receipt of the utterance's first audio):
///
/// ```text
/// latency #2: vad~-38ms | 0 audio_rx | +224 wake | +1381 endpoint | +1382 stt |
/// +1731 stt_done(349) | +1732 brain | +1740 speak(8) | +2094 tts(354) | +2101 play(7)
/// ```
///
/// A parenthesized number is that stage's own ms of blame. `~` marks a fuzzy
/// value: on `vad` always (it is projected off the device clock), and on the axis
/// origin (`0~ audio_rx`) when t0 itself was projected. Absent stages render `?`.
/// A `Pcm` response never synthesizes, so its `tts` reads `?` and `play` blames
/// the whole `speak → first write` span — the stage it actually follows. The
/// file-only `onset_ms` is left to the JSONL line; the console shows the stages a
/// human watching for lag reads.
fn narrate_latency_summary(fields: &Value) -> String {
    let id = fmt_u64(fields.get("utterance"));
    let vad = fmt_signed_ms(fields.get("vad_high_ms"));
    // The origin is 0 by construction, so the only thing to say about it is
    // whether it is a measurement or an estimate.
    let t0 = match fields.get("t0_projected").and_then(Value::as_bool) {
        Some(true) => "0~".to_string(),
        Some(false) => "0".to_string(),
        None => "?".to_string(),
    };
    let at = |field| fmt_signed_ms(fields.get(field));
    let blame = |field| fmt_ms_from_us_bare(fields.get(field));
    // Playback's blame is the span since the stage it follows: the synthesis for
    // a synthesized clip, the brain's reply for a `Pcm` one.
    let play_blame = match fields.get("synth_to_first_write_us") {
        Some(v) if v.as_u64().is_some() => blame("synth_to_first_write_us"),
        _ => blame("speak_to_first_write_us"),
    };
    format!(
        "latency #{id}: vad~{vad}ms | {t0} audio_rx | {} wake | {} endpoint | {} stt | \
         {} stt_done({}) | {} brain | {} speak({}) | {} tts({}) | {} play({play_blame})",
        at("wake_ms"),
        at("soft_endpoint_ms"),
        at("stt_start_ms"),
        at("stt_done_ms"),
        blame("stt_us"),
        at("brain_ms"),
        at("speak_rx_ms"),
        blame("brain_us"),
        at("tts_done_ms"),
        blame("tts_us"),
        at("first_write_ms"),
    )
}

/// A signed millisecond offset with an explicit sign (`+224`, `-38`), or `?` when
/// absent or non-numeric. Signed because the axis genuinely has stages before its
/// origin — a `-` here is a reading, not a fault.
fn fmt_signed_ms(value: Option<&Value>) -> String {
    match value.and_then(Value::as_i64) {
        Some(n) => format!("{n:+}"),
        None => "?".to_string(),
    }
}

/// A microsecond field as bare whole milliseconds (no unit suffix), or `?` when
/// absent or non-numeric. For the latency stackup, where the line's own shape
/// says the numbers are ms and a per-stage `ms` suffix would be noise.
fn fmt_ms_from_us_bare(value: Option<&Value>) -> String {
    match value.and_then(Value::as_u64) {
        Some(us) => (us / 1_000).to_string(),
        None => "?".to_string(),
    }
}

/// The reply clause of a playback line. A numeric `utterance` id renders
/// `reply to #N`; JSON `null` is the intentional encoding of a playback that
/// answers no utterance and renders `unsolicited`. An absent or non-numeric
/// field renders `reply to #?` — a genuine malformed event, kept distinct from
/// the legitimate `null`.
fn reply_clause(value: Option<&Value>) -> String {
    if matches!(value, Some(Value::Null)) {
        return "unsolicited".to_string();
    }
    format!("reply to #{}", fmt_u64(value))
}

/// A closed capture segment as prose:
/// `segment 7 closed — vad_release, 2.46s, 39360 samples`. The duration is
/// derived from the sample count at the spine sample rate. Anomaly markers
/// (`truncated`, `resumed`, `N gaps`) append only when present, so a clean
/// close stays terse while a degraded one is visible on the default console.
fn narrate_segment_closed(fields: &Value) -> String {
    let seg = fmt_u64(fields.get("segment_id"));
    let cause = fmt_str(fields.get("end_cause"));
    let dur = fmt_duration_s(fields.get("samples"), 2);
    let samples = fmt_u64(fields.get("samples"));
    let mut line = format!("segment {seg} closed — {cause}, {dur}, {samples} samples");
    if fields.get("truncated").and_then(Value::as_bool) == Some(true) {
        line.push_str(", truncated");
    }
    if fields.get("resumed").and_then(Value::as_bool) == Some(true) {
        line.push_str(", resumed");
    }
    if let Some(gaps) = fields
        .get("gap_count")
        .and_then(Value::as_u64)
        .filter(|&g| g > 0)
    {
        line.push_str(&format!(", {gaps} gaps"));
    }
    line
}

/// A newly opened capture segment as prose: `segment 7 opened (preroll 4800)`,
/// with a `resume` marker prepended when the segment resumes a prior capture.
fn narrate_segment_opened(fields: &Value) -> String {
    let seg = fmt_u64(fields.get("segment_id"));
    let preroll = fmt_u64(fields.get("preroll"));
    let resume = if fields.get("is_resume").and_then(Value::as_bool) == Some(true) {
        "resume, "
    } else {
        ""
    };
    format!("segment {seg} opened ({resume}preroll {preroll})")
}

/// A dispatched utterance as prose: `utterance #3 — "turn on the kitchen
/// lights"`, or `utterance #N — (no transcript)` when no transcript was minted
/// (the null-transcript path). The transcript text is sanitized and truncated;
/// the file keeps the full text. A missing id degrades to `?`.
fn narrate_utterance(fields: &Value) -> String {
    let id = fmt_u64(fields.get("id"));
    let transcript = fields.get("transcript");
    let line = match transcript
        .and_then(|t| t.get("text"))
        .and_then(Value::as_str)
    {
        Some(text) => format!("utterance #{id} — \"{}\"", sanitize(text)),
        None => format!("utterance #{id} — (no transcript)"),
    };
    match transcript
        .and_then(|t| t.get("confidence"))
        .filter(|c| c.is_object())
    {
        Some(conf) => format!("{line} {}", confidence_suffix(conf)),
        None => line,
    }
}

/// The STT confidence tail appended to an utterance line when the transcript
/// carries a `verbose_json` summary: `conf: logprob=-0.23 no_speech=0.02
/// compress=1.4`. Each field degrades to `?` when absent or non-numeric so a
/// malformed summary still renders. For live threshold-tuning, not a decision.
fn confidence_suffix(conf: &Value) -> String {
    let logprob = fmt_f64(conf.get("avg_logprob"), 2);
    let no_speech = fmt_f64(conf.get("no_speech_prob"), 2);
    let compress = fmt_f64(conf.get("compression_ratio"), 1);
    format!("conf: logprob={logprob} no_speech={no_speech} compress={compress}")
}

/// A floating-point field to `decimals` places, or `?` when absent or
/// non-numeric (defensive: a malformed event still renders a line).
fn fmt_f64(value: Option<&Value>, decimals: usize) -> String {
    match value.and_then(Value::as_f64) {
        Some(n) => format!("{n:.decimals$}"),
        None => "?".to_string(),
    }
}

/// A wake-with-no-command as calm prose: `utterance #7 — wake, no command
/// (score 0.998)` for an empty transcript, or `utterance #7 — wake, no command,
/// low confidence no_speech=0.37 logprob=-0.99 (score 0.998)` when STT confidence
/// gated a likely hallucination. A calm line either way, not a failure — the
/// segment is on disk for later re-transcription. The id and score degrade to `?`
/// when absent or mistyped; an absent/unknown reason degrades to the bare line.
fn narrate_wake_command_absent(fields: &Value) -> String {
    let id = fmt_u64(fields.get("utterance"));
    let score = fmt_score(fields.get("score"));
    let cause = match fields.get("reason").and_then(Value::as_str) {
        Some("low_confidence") => {
            let no_speech = fmt_f64(fields.get("no_speech"), 2);
            let logprob = fmt_f64(fields.get("logprob"), 2);
            format!(", low confidence no_speech={no_speech} logprob={logprob}")
        }
        _ => String::new(),
    };
    format!("utterance #{id} — wake, no command{cause} (score {score})")
}

/// The wake verdict as prose: `✓ wake positive — score 0.874 ≥ 0.500 (infer
/// 31 ms)`, `✗ wake negative — score 0.121 < 0.500`, or `✓ wake bypassed (no
/// gate)`. The bypass gate applies no threshold, so none is shown. An `error`
/// (or unknown) outcome returns `None` to render loud through the generic path.
fn narrate_wake_decision(fields: &Value) -> Option<String> {
    let outcome = fields.get("outcome").and_then(Value::as_str)?;
    let score = fmt_score(fields.get("score"));
    let threshold = fmt_score(fields.get("threshold"));
    match outcome {
        "positive" => {
            // Infer time is a nice-to-have: absent, its clause is omitted
            // entirely rather than degrading to `? ms`.
            let infer = fields
                .get("infer_us")
                .filter(|v| v.as_u64().is_some())
                .map(|v| format!(" (infer {})", fmt_ms_from_us(Some(v))))
                .unwrap_or_default();
            Some(format!(
                "✓ wake positive — score {score} ≥ {threshold}{infer}"
            ))
        }
        "negative" => Some(format!("✗ wake negative — score {score} < {threshold}")),
        "bypassed" => Some("✓ wake bypassed (no gate)".to_string()),
        _ => None,
    }
}

/// A wake detection as prose: `✓ wake detected — score 0.836 @52256640`. The
/// listener arms on this; whether a command follows shows as a later `utterance`
/// or `arm_expired`. Score and sample index degrade to `?` when absent or
/// mistyped.
fn narrate_wake_detected(fields: &Value) -> String {
    let score = fmt_score(fields.get("score"));
    let at = fmt_u64(fields.get("wake_end_sample"));
    format!("✓ wake detected — score {score} @{at}")
}

/// One endpointer FSM transition as prose:
/// `endpointer speech → soft_endpointed (soft_endpoint) @52256640`. The states
/// and cause are the event's own snake_case labels; each field degrades to `?`
/// when absent or mistyped.
fn narrate_endpointer_transition(fields: &Value) -> String {
    let from = fmt_str(fields.get("from"));
    let to = fmt_str(fields.get("to"));
    let cause = fmt_str(fields.get("cause"));
    let at = fmt_u64(fields.get("sample_offset"));
    format!("endpointer {from} → {to} ({cause}) @{at}")
}

/// One model's score distribution as prose:
/// `silero p x256: min 0.001 max 0.031 mean 0.004 median 0.002 (periodic) @52256640`.
/// The reading a transition line cannot give — an endpointer that never fires says
/// nothing about what the model returned. Each field degrades to `?` when absent
/// or mistyped.
fn narrate_model_stats(fields: &Value) -> String {
    let model = fmt_str(fields.get("model"));
    let chunks = fmt_u64(fields.get("chunks"));
    let min = fmt_score(fields.get("min"));
    let max = fmt_score(fields.get("max"));
    let mean = fmt_score(fields.get("mean"));
    let median = fmt_score(fields.get("median"));
    let cause = fmt_str(fields.get("cause"));
    let at = fmt_u64(fields.get("last_chunk_end"));
    format!("{model} p x{chunks}: min {min} max {max} mean {mean} median {median} ({cause}) @{at}")
}

/// A superseded utterance as prose: `utterance seq 4 superseded — speech
/// resumed, re-transcribing`. Speech resumed inside the continuation window, so
/// the in-flight transcription is abandoned and the whole utterance re-runs.
fn narrate_utterance_superseded(fields: &Value) -> String {
    let seq = fmt_u64(utterance_seq(fields));
    format!("utterance seq {seq} superseded — speech resumed, re-transcribing")
}

/// A closed utterance as prose: `utterance seq 4 closed`. The continuation
/// window elapsed with no resume; the utterance is final.
fn narrate_utterance_closed(fields: &Value) -> String {
    let seq = fmt_u64(utterance_seq(fields));
    format!("utterance seq {seq} closed")
}

/// An expired wake arm as prose:
/// `wake armed but no command followed — score 0.836, span 52240640..52256640`.
/// The "wake, no follow" case: the wake fired, no utterance passed the gate.
fn narrate_arm_expired(fields: &Value) -> String {
    let score = fmt_score(fields.get("score"));
    let start = fmt_u64(fields.get("start_sample"));
    let end = fmt_u64(fields.get("end_sample"));
    format!("wake armed but no command followed — score {score}, span {start}..{end}")
}

/// The `seq` inside a nested `utterance_id` object. The listener's id is
/// `(pod, epoch, seq)`; the pod is already in the line's tag and the epoch is
/// file-only detail, so the console shows the seq alone.
fn utterance_seq(fields: &Value) -> Option<&Value> {
    fields.get("utterance_id").and_then(|id| id.get("seq"))
}

/// A wake score or threshold to three decimals, or `?` when the field is absent
/// or non-numeric (defensive: a malformed event still renders a line).
fn fmt_score(value: Option<&Value>) -> String {
    fmt_f64(value, 3)
}

/// An unsigned integer field as text, or `?` when absent or non-numeric
/// (defensive: a malformed event still renders a line).
fn fmt_u64(value: Option<&Value>) -> String {
    match value.and_then(Value::as_u64) {
        Some(n) => n.to_string(),
        None => "?".to_string(),
    }
}

/// A string field sanitized for the terminal, or `?` when absent or non-string
/// (defensive: a malformed event still renders a line).
fn fmt_str(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_str)
        .map(sanitize)
        .unwrap_or_else(|| "?".to_string())
}

/// A microsecond field as whole milliseconds (`312 ms`), or `? ms` when absent
/// or non-numeric. Truncates toward zero, so a sub-millisecond value reads
/// `0 ms`.
fn fmt_ms_from_us(value: Option<&Value>) -> String {
    match value.and_then(Value::as_u64) {
        Some(us) => format!("{} ms", us / 1000),
        None => "? ms".to_string(),
    }
}

/// A millisecond-duration field as seconds to `decimals` places (e.g. `1.9s`),
/// or `?` when absent or non-numeric. Used when the event already carries a
/// computed duration rather than a raw sample count.
fn fmt_duration_ms(value: Option<&Value>, decimals: usize) -> String {
    match value.and_then(Value::as_u64) {
        Some(ms) => format!("{:.*}s", decimals, ms as f64 / 1000.0),
        None => "?".to_string(),
    }
}

/// A sample-count field as a duration in seconds at the spine sample rate, to
/// `decimals` places (e.g. `2.46s`), or `?` when absent or non-numeric.
fn fmt_duration_s(value: Option<&Value>, decimals: usize) -> String {
    match value.and_then(Value::as_u64) {
        Some(n) => format!(
            "{:.*}s",
            decimals,
            n as f64 / f64::from(SPINE_FORMAT.sample_rate_hz)
        ),
        None => "?".to_string(),
    }
}

/// `HH:MM:SS.mmm` UTC time-of-day from a Unix-epoch millisecond stamp, by
/// integer math so no time/timezone dependency is pulled in. The date is
/// dropped; correlation with the JSONL file (which keeps raw `ts_ms`) is exact
/// because both sinks share one stamp.
fn format_tod(ts_ms: u64) -> String {
    let tod = ts_ms % 86_400_000;
    let hours = tod / 3_600_000;
    let minutes = (tod / 60_000) % 60;
    let seconds = (tod / 1000) % 60;
    let millis = tod % 1000;
    format!("{hours:02}:{minutes:02}:{seconds:02}.{millis:03}")
}

/// The `[room/pod]` identity tag when the event carries either field, else
/// empty. Both parts are sanitized so a hostile pod name or room cannot inject
/// terminal control codes.
fn tag_string(fields: &Value) -> String {
    let room = fields.get("room").and_then(Value::as_str).map(sanitize);
    // Emit sites name the pod either `pod_id` or `pod`; both are the identity.
    let pod = fields
        .get("pod_id")
        .or_else(|| fields.get("pod"))
        .and_then(Value::as_str)
        .map(sanitize);
    match (room, pod) {
        (Some(r), Some(p)) => format!("[{r}/{p}]"),
        (Some(r), None) => format!("[{r}]"),
        (None, Some(p)) => format!("[{p}]"),
        (None, None) => String::new(),
    }
}

/// The event's remaining fields as compact `key=value` pairs (identity fields
/// pulled into the tag are omitted), so no diagnostic detail is lost even when
/// the renderer has no bespoke prose for the event. `serde_json`'s map is
/// key-sorted, so the order is deterministic.
fn kv_string(fields: &Value) -> String {
    let Some(obj) = fields.as_object() else {
        return String::new();
    };
    obj.iter()
        .filter(|(key, _)| !matches!(key.as_str(), "room" | "pod_id" | "pod"))
        .map(|(key, value)| format!("{key}={}", render_value(value)))
        .collect::<Vec<_>>()
        .join(" ")
}

fn render_value(value: &Value) -> String {
    match value {
        Value::String(s) => sanitize(s),
        other => sanitize(&other.to_string()),
    }
}

/// Replace every character that could corrupt or spoof the terminal line with
/// the replacement char, then truncate. Every string that reaches the terminal —
/// off-host pod id and room, ML transcript, remote error detail — passes through
/// here.
fn sanitize(s: &str) -> String {
    let cleaned: String = s
        .chars()
        .map(|c| if is_unsafe(c) { '\u{fffd}' } else { c })
        .collect();
    if cleaned.chars().count() <= MAX_FIELD_CHARS {
        cleaned
    } else {
        let head: String = cleaned.chars().take(MAX_FIELD_CHARS).collect();
        format!("{head}…")
    }
}

/// True for characters that must not reach the terminal verbatim: ASCII/Unicode
/// control chars (including the ESC that starts an ANSI sequence); the
/// structural chars `[`, `]`, and `"` that the renderer itself uses to frame the
/// identity tag and quote a transcript (so an untrusted value cannot forge a tag
/// or break out of the quotes); and the invisible bidi/format/zero-width
/// characters behind Trojan-Source visual reordering and homoglyph identity
/// spoofing. `char::is_control` covers only the Cc category, so the Cf-category
/// format characters, the variation selectors, and the Unicode tag block are
/// enumerated explicitly (plus the line/paragraph separators U+2028/U+2029),
/// keeping legitimate visible non-ASCII text (e.g. a foreign-language
/// transcript) intact.
fn is_unsafe(c: char) -> bool {
    c.is_control()
        || matches!(c, '[' | ']' | '"')
        || matches!(c,
            '\u{00AD}'                  // soft hyphen
            | '\u{0600}'..='\u{0605}'   // Arabic number/format signs
            | '\u{061C}'                // Arabic letter mark
            | '\u{06DD}'                // Arabic end of ayah
            | '\u{070F}'                // Syriac abbreviation mark
            | '\u{08E2}'                // Arabic disputed end of ayah
            | '\u{180E}'                // Mongolian vowel separator
            | '\u{200B}'..='\u{200F}'   // zero-width space/joiners + directional marks
            | '\u{202A}'..='\u{202E}'   // bidi embeddings and overrides
            | '\u{2028}' | '\u{2029}'   // line and paragraph separators
            | '\u{2060}'..='\u{2064}'   // word joiner + invisible operators
            | '\u{2066}'..='\u{206F}'   // bidi isolates + deprecated format controls
            | '\u{FE00}'..='\u{FE0F}'   // variation selectors
            | '\u{FEFF}'                // zero-width no-break space / BOM
            | '\u{FFF9}'..='\u{FFFB}'   // interlinear annotation controls
            | '\u{1D173}'..='\u{1D17A}' // musical symbol beam/phrase controls
            | '\u{E0000}'..='\u{E007F}' // Unicode tag block
            | '\u{E0100}'..='\u{E01EF}' // variation selectors supplement
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn wants_rejects_tracking_and_accepts_console_and_error_events() {
        assert!(!wants("tracking"));
        assert!(wants("segment_opened"));
        assert!(wants("stt_configured"));
        assert!(wants("foo_failed"));
        assert!(wants("signal_handler_failed"));
        assert!(wants("playback_no_pod"));
        assert!(wants("brain_sink_full"));
        assert!(wants("wake_sidecar_skipped"));
        // Unmapped, no error token: file-only.
        assert!(!wants("foo_info"));
        assert!(!wants("conn_accepted"));
        assert!(!wants("record_rolled"));
    }

    #[test]
    fn time_of_day_is_integer_math_utc() {
        // 18:02:11.301 UTC on an arbitrary day.
        let ts = 86_400_000 * 19_000 + (18 * 3_600 + 2 * 60 + 11) * 1000 + 301;
        assert_eq!(format_tod(ts), "18:02:11.301");
        assert_eq!(format_tod(0), "00:00:00.000");
    }

    #[test]
    fn tag_and_kv_render_identity_and_omit_it_from_fields() {
        // The generic tag+fields machinery, tested directly on `tag_string` /
        // `kv_string` so it does not churn as events gain bespoke prose.
        let fields = json!({ "room": "kitchen", "pod": "pod-a1b2c3", "model": "small" });
        assert_eq!(tag_string(&fields), "[kitchen/pod-a1b2c3]");
        let kv = kv_string(&fields);
        assert!(kv.contains("model=small"), "{kv}");
        // Identity fields shown in the tag are not duplicated in the kv tail.
        assert!(!kv.contains("room="), "{kv}");
        assert!(!kv.contains("pod="), "{kv}");
    }

    /// A `latency_summary` for a synthesized response: every stage stamped, t0
    /// measured, the VAD-high estimate before t0.
    fn latency_fields() -> Value {
        json!({
            "pod": "pod-fbe2f8", "utterance": 2, "t0_projected": false,
            "vad_high_ms": -38, "wake_ms": 224, "onset_ms": 300,
            "soft_endpoint_ms": 1381, "stt_start_ms": 1382, "stt_done_ms": 1731,
            "brain_ms": 1732, "speak_rx_ms": 1740, "tts_done_ms": 2094,
            "first_write_ms": 2101,
            "endpoint_to_stt_us": 1_000, "stt_us": 349_000, "stt_to_brain_us": 1_000,
            "brain_us": 8_000, "speak_to_synth_start_us": 0, "tts_us": 354_000,
            "synth_to_first_write_us": 7_000, "speak_to_first_write_us": null,
        })
    }

    #[test]
    fn latency_summary_narrates_the_whole_stackup() {
        let mut r = Renderer::new(false);
        let line = r.render(0, "latency_summary", &latency_fields()).unwrap();
        assert!(
            line.ends_with(
                "[pod-fbe2f8] latency #2: vad~-38ms | 0 audio_rx | +224 wake | +1381 endpoint | \
                 +1382 stt | +1731 stt_done(349) | +1732 brain | +1740 speak(8) | \
                 +2094 tts(354) | +2101 play(7)"
            ),
            "{line}"
        );
        assert!(!line.contains("!!!"), "{line}");
    }

    #[test]
    fn latency_summary_marks_a_projected_t0_and_renders_absent_stages() {
        let mut r = Renderer::new(false);
        // A wake into an already-open segment with no transcriber wired: the axis
        // origin is an estimate (`0~`) and the stages that never ran read `?`.
        let mut fields = latency_fields();
        fields["t0_projected"] = json!(true);
        fields["stt_done_ms"] = Value::Null;
        fields["stt_us"] = Value::Null;
        let line = r.render(0, "latency_summary", &fields).unwrap();
        assert!(line.contains("0~ audio_rx"), "{line}");
        assert!(line.contains("? stt_done(?)"), "{line}");
        // A malformed/absent t0 is distinct from a projected one.
        fields["t0_projected"] = Value::Null;
        let no_t0 = r.render(0, "latency_summary", &fields).unwrap();
        assert!(no_t0.contains("? audio_rx"), "{no_t0}");
    }

    #[test]
    fn latency_summary_for_a_pcm_body_blames_play_for_the_whole_speak_span() {
        let mut r = Renderer::new(false);
        // No synthesis: `tts` has nothing to report, and playback's blame is the
        // span since the stage it actually follows — the brain's reply.
        let mut fields = latency_fields();
        for f in ["tts_done_ms", "tts_us", "synth_to_first_write_us"] {
            fields[f] = Value::Null;
        }
        fields["speak_to_first_write_us"] = json!(361_000);
        let line = r.render(0, "latency_summary", &fields).unwrap();
        assert!(line.contains("? tts(?)"), "{line}");
        assert!(line.ends_with("+2101 play(361)"), "{line}");
    }

    #[test]
    fn stt_started_speak_rx_and_brain_dispatched_narrate() {
        let mut r = Renderer::new(false);
        // 43520 samples / 16000 Hz = 2.7 s.
        let stt = r
            .render(
                0,
                "stt_started",
                &json!({ "pod": "pod-a1b2c3", "utterance_seq": 4, "samples": 43520 }),
            )
            .unwrap();
        assert!(
            stt.ends_with("[pod-a1b2c3] stt started (seq 4, 2.7s audio)"),
            "{stt}"
        );

        let brain = r
            .render(
                0,
                "brain_dispatched",
                &json!({ "pod": "pod-a1b2c3", "utterance": 3 }),
            )
            .unwrap();
        assert!(
            brain.ends_with("[pod-a1b2c3] utterance #3 → brain"),
            "{brain}"
        );

        let speak = r
            .render(
                0,
                "speak_rx",
                &json!({ "pod": "pod-a1b2c3", "utterance": 3, "body": "text" }),
            )
            .unwrap();
        assert!(
            speak.ends_with("[pod-a1b2c3] speak received (reply to #3, text)"),
            "{speak}"
        );
        for line in [&stt, &brain, &speak] {
            assert!(!line.contains("!!!"), "{line}");
        }
    }

    #[test]
    fn synth_narrates() {
        let mut r = Renderer::new(false);
        // 30400 samples / 16000 Hz = 1.9 s; synth_us 312000 → 312 ms.
        let line = r
            .render(
                0,
                "synth",
                &json!({
                    "pod": "pod-a1b2c3", "utterance": 3, "input_chars": 27,
                    "samples": 30400, "synth_us": 312000
                }),
            )
            .unwrap();
        assert!(
            line.ends_with("[pod-a1b2c3] synthesized (reply to #3, 27 chars → 1.9s audio, 312 ms)"),
            "{line}"
        );
        assert!(!line.contains("!!!"), "{line}");
        // A `null` utterance is an intentional non-reply, not a malformed event.
        let unsolicited = r
            .render(
                0,
                "synth",
                &json!({
                    "utterance": null, "input_chars": 4, "samples": 30400,
                    "synth_us": 100000
                }),
            )
            .unwrap();
        assert!(
            unsolicited.ends_with("synthesized (unsolicited, 4 chars → 1.9s audio, 100 ms)"),
            "{unsolicited}"
        );
        // Missing fields degrade to `?` rather than panicking.
        let bare = r.render(0, "synth", &json!({})).unwrap();
        assert!(
            bare.ends_with("synthesized (reply to #?, ? chars → ? audio, ? ms)"),
            "{bare}"
        );
    }

    #[test]
    fn daemon_start_narrates() {
        let mut r = Renderer::new(false);
        let line = r
            .render(
                0,
                "daemon_start",
                &json!({
                    "listen_addr": "10.0.0.5:7380", "record_enabled": true,
                    "record_dir": "./framelogs", "max_connections": 8,
                    "jsonl_sink": "/var/log/speech.jsonl"
                }),
            )
            .unwrap();
        assert!(
            line.ends_with(
                "speech-surface starting — listen 10.0.0.5:7380, record ./framelogs (on), events → /var/log/speech.jsonl"
            ),
            "{line}"
        );
        assert!(!line.contains("!!!"), "{line}");
        // Recording off + no persisted event stream.
        let off = r
            .render(
                0,
                "daemon_start",
                &json!({
                    "listen_addr": "10.0.0.5:7380", "record_enabled": false,
                    "record_dir": "./framelogs", "jsonl_sink": "none"
                }),
            )
            .unwrap();
        assert!(
            off.ends_with("record ./framelogs (off), events → none"),
            "{off}"
        );
        // Missing fields degrade to `?` rather than panicking.
        let bare = r.render(0, "daemon_start", &json!({})).unwrap();
        assert!(
            bare.ends_with("listen ?, record ? (?), events → ?"),
            "{bare}"
        );
    }

    #[test]
    fn listening_narrates() {
        let mut r = Renderer::new(false);
        let line = r
            .render(0, "listening", &json!({ "addr": "10.0.0.5:7380" }))
            .unwrap();
        assert!(line.ends_with("listening on 10.0.0.5:7380"), "{line}");
        assert!(!line.contains("!!!"), "{line}");
        // The rare `local_addr` error emits a null address plus a detail; the
        // calm line degrades the address to `?` and keeps the diagnostic.
        let errored = r
            .render(
                0,
                "listening",
                &json!({ "addr": null, "detail": "address in use" }),
            )
            .unwrap();
        assert!(
            errored.ends_with("listening on ? (address in use)"),
            "{errored}"
        );
        assert!(!errored.contains("!!!"), "{errored}");
        // Missing fields degrade without panicking.
        let bare = r.render(0, "listening", &json!({})).unwrap();
        assert!(bare.ends_with("listening on ?"), "{bare}");
    }

    #[test]
    fn conn_superseded_narrates() {
        let mut r = Renderer::new(false);
        // The real emit carries `pod_id`, `old_conn_seq`, `new_conn_seq` — no room.
        let line = r
            .render(
                0,
                "conn_superseded",
                &json!({ "pod_id": "pod-a1b2c3", "old_conn_seq": 6, "new_conn_seq": 7 }),
            )
            .unwrap();
        assert!(
            line.ends_with("[pod-a1b2c3] reconnected, superseding conn 6"),
            "{line}"
        );
        assert!(!line.contains("!!!"), "{line}");
        // Missing conn seq degrades to `?` rather than panicking.
        let bare = r.render(0, "conn_superseded", &json!({})).unwrap();
        assert!(bare.ends_with("reconnected, superseding conn ?"), "{bare}");
    }

    #[test]
    fn conn_closed_narrates() {
        let mut r = Renderer::new(false);
        // The real emit carries `pod_id`, `peer`, `conn_seq`, `cause`; the pod id
        // supplies the identity tag.
        let line = r
            .render(
                0,
                "conn_closed",
                &json!({ "pod_id": "pod-a1b2c3", "peer": "10.0.0.5:54312", "conn_seq": 7, "cause": "eof" }),
            )
            .unwrap();
        assert!(
            line.ends_with("[pod-a1b2c3] disconnected (conn 7, eof)"),
            "{line}"
        );
        assert!(!line.contains("!!!"), "{line}");
        // A pre-`Hello` close carries a null pod id and renders tag-less.
        let prehello = r
            .render(
                0,
                "conn_closed",
                &json!({ "pod_id": null, "peer": "10.0.0.5:54312", "conn_seq": 7, "cause": "eof" }),
            )
            .unwrap();
        assert!(
            prehello.ends_with("disconnected (conn 7, eof)"),
            "{prehello}"
        );
        // Missing fields degrade to `?` rather than panicking.
        let bare = r.render(0, "conn_closed", &json!({})).unwrap();
        assert!(bare.ends_with("disconnected (conn ?, ?)"), "{bare}");
    }

    #[test]
    fn segment_opened_narrates() {
        let mut r = Renderer::new(false);
        let line = r
            .render(
                0,
                "segment_opened",
                &json!({
                    "room": "kitchen", "pod": "pod-a1b2c3", "segment_id": 7,
                    "is_resume": false, "preroll": 4800
                }),
            )
            .unwrap();
        assert!(
            line.ends_with("[kitchen/pod-a1b2c3] segment 7 opened (preroll 4800)"),
            "{line}"
        );
        assert!(!line.contains("!!!"), "{line}");
        // A resumed segment carries the `resume` marker.
        let resumed = r
            .render(
                0,
                "segment_opened",
                &json!({ "pod": "pod-a1b2c3", "segment_id": 8, "is_resume": true, "preroll": 4800 }),
            )
            .unwrap();
        assert!(
            resumed.ends_with("segment 8 opened (resume, preroll 4800)"),
            "{resumed}"
        );
        // Missing fields degrade to `?` rather than panicking or dropping.
        let bare = r.render(0, "segment_opened", &json!({})).unwrap();
        assert!(bare.ends_with("segment ? opened (preroll ?)"), "{bare}");
    }

    #[test]
    fn segment_closed_narrates() {
        let mut r = Renderer::new(false);
        // 39360 samples / 16000 Hz = 2.46 s.
        let line = r
            .render(
                0,
                "segment_closed",
                &json!({
                    "room": "kitchen", "pod": "pod-a1b2c3", "segment_id": 7,
                    "end_cause": "vad_release", "samples": 39360
                }),
            )
            .unwrap();
        assert!(
            line.ends_with(
                "[kitchen/pod-a1b2c3] segment 7 closed — vad_release, 2.46s, 39360 samples"
            ),
            "{line}"
        );
        assert!(!line.contains("!!!"), "{line}");
        // Anomaly markers append only when present; a degraded close is visible.
        let degraded = r
            .render(
                0,
                "segment_closed",
                &json!({
                    "pod": "pod-a1b2c3", "segment_id": 9, "end_cause": "vad_release",
                    "samples": 39360, "truncated": true, "resumed": true, "gap_count": 3
                }),
            )
            .unwrap();
        assert!(
            degraded.ends_with(
                "segment 9 closed — vad_release, 2.46s, 39360 samples, truncated, resumed, 3 gaps"
            ),
            "{degraded}"
        );
        // Missing cause and sample count degrade to `?` (duration too, since it
        // is derived from the absent sample count) rather than panicking.
        let bare = r.render(0, "segment_closed", &json!({})).unwrap();
        assert!(
            bare.ends_with("segment ? closed — ?, ?, ? samples"),
            "{bare}"
        );
        // A same-shape event with wrong-typed leaves renders `?`, not a panic.
        let mistyped = r
            .render(
                0,
                "segment_closed",
                &json!({ "segment_id": "seven", "end_cause": 3, "samples": "not-a-number" }),
            )
            .unwrap();
        assert!(
            mistyped.ends_with("segment ? closed — ?, ?, ? samples"),
            "{mistyped}"
        );
    }

    #[test]
    fn wake_command_absent_narrates_empty_and_low_confidence() {
        let mut r = Renderer::new(false);
        // The empty cause reads as the bare wake-no-command line.
        let empty = r
            .render(
                0,
                "wake_command_absent",
                &json!({ "utterance": 7, "score": 0.998, "reason": "empty" }),
            )
            .unwrap();
        assert!(
            empty.ends_with("utterance #7 — wake, no command (score 0.998)"),
            "{empty}"
        );
        // The low-confidence cause names itself and carries the offending numbers.
        let low = r
            .render(
                0,
                "wake_command_absent",
                &json!({
                    "utterance": 7, "score": 0.998,
                    "reason": "low_confidence", "no_speech": 0.37, "logprob": -0.99
                }),
            )
            .unwrap();
        assert!(
            low.ends_with(
                "utterance #7 — wake, no command, low confidence no_speech=0.37 logprob=-0.99 \
                 (score 0.998)"
            ),
            "{low}"
        );
        assert!(!low.contains("!!!"), "a calm line, not loud: {low}");
    }

    #[test]
    fn wake_command_absent_missing_reason_degrades_to_bare_line() {
        // A line with no `reason` key (an older replay, or a future/unknown reason)
        // renders the bare wake-no-command line rather than inventing a cause.
        let mut r = Renderer::new(false);
        let line = r
            .render(
                0,
                "wake_command_absent",
                &json!({ "utterance": 7, "score": 0.998 }),
            )
            .unwrap();
        assert!(
            line.ends_with("utterance #7 — wake, no command (score 0.998)"),
            "{line}"
        );
    }

    #[test]
    fn playback_started_narrates() {
        let mut r = Renderer::new(false);
        // 30400 samples / 16000 Hz = 1.9 s.
        // The real emit site sets only `pod` (no `room`), so the tag is pod-only.
        let line = r
            .render(
                0,
                "playback_started",
                &json!({ "pod": "pod-a1b2c3", "utterance": 3, "samples": 30400 }),
            )
            .unwrap();
        assert!(
            line.ends_with("[pod-a1b2c3] ▶ playback started (reply to #3, 1.9s audio)"),
            "{line}"
        );
        assert!(!line.contains("!!!"), "{line}");
        // A `null` utterance is an intentional non-reply, not a malformed event:
        // it reads `unsolicited`, not `reply to #?`.
        let unsolicited = r
            .render(
                0,
                "playback_started",
                &json!({ "pod": "pod-a1b2c3", "utterance": null, "samples": 30400 }),
            )
            .unwrap();
        assert!(
            unsolicited.ends_with("▶ playback started (unsolicited, 1.9s audio)"),
            "{unsolicited}"
        );
        // Missing reply id and sample count degrade to `?` rather than panicking.
        let bare = r.render(0, "playback_started", &json!({})).unwrap();
        assert!(
            bare.ends_with("▶ playback started (reply to #?, ? audio)"),
            "{bare}"
        );
    }

    #[test]
    fn playback_finished_narrates() {
        let mut r = Renderer::new(false);
        // The real emit site sets only `pod` (no `room`), so the tag is pod-only.
        let line = r
            .render(
                0,
                "playback_finished",
                &json!({
                    "pod": "pod-a1b2c3", "utterance": 3,
                    "frames": 96, "eoa_written": true
                }),
            )
            .unwrap();
        assert!(
            line.ends_with("[pod-a1b2c3] ■ playback finished (reply to #3, 96 frames, EOA sent)"),
            "{line}"
        );
        assert!(!line.contains("!!!"), "{line}");
        // A `null` utterance reads `unsolicited`, distinct from a missing id.
        let unsolicited = r
            .render(
                0,
                "playback_finished",
                &json!({ "utterance": null, "frames": 8, "eoa_written": true }),
            )
            .unwrap();
        assert!(
            unsolicited.ends_with("■ playback finished (unsolicited, 8 frames, EOA sent)"),
            "{unsolicited}"
        );
        // A run that never wrote the EOA marker says so.
        let no_eoa = r
            .render(
                0,
                "playback_finished",
                &json!({ "utterance": 5, "frames": 12, "eoa_written": false }),
            )
            .unwrap();
        assert!(
            no_eoa.ends_with("■ playback finished (reply to #5, 12 frames, no EOA)"),
            "{no_eoa}"
        );
        // Missing fields degrade to `?` rather than panicking.
        let bare = r.render(0, "playback_finished", &json!({})).unwrap();
        assert!(
            bare.ends_with("■ playback finished (reply to #?, ? frames, EOA ?)"),
            "{bare}"
        );
    }

    #[test]
    fn playback_aborted_is_wanted_and_loud() {
        assert!(wants("playback_aborted"));
        let mut r = Renderer::new(false);
        let line = r
            .render(0, "playback_aborted", &json!({ "reply_to": 3 }))
            .unwrap();
        assert!(line.contains("!!! playback_aborted"), "{line}");
    }

    #[test]
    fn wake_bypassed_renders_loud() {
        let mut plain = Renderer::new(false);
        assert!(plain
            .render(0, "wake_bypassed", &json!({}))
            .unwrap()
            .contains("!!!"));
        let mut colored = Renderer::new(true);
        assert!(colored
            .render(0, "wake_bypassed", &json!({}))
            .unwrap()
            .contains('\x1b'));
    }

    #[test]
    fn wake_decision_score_fallback_renders_question_marks() {
        let mut r = Renderer::new(false);
        // A `positive` verdict missing its numeric score/threshold still renders
        // a calm line via the `?` fallback rather than panicking or dropping.
        let line = r
            .render(0, "wake_decision", &json!({ "outcome": "positive" }))
            .unwrap();
        assert!(line.contains("score ? ≥ ?"), "{line}");
        assert!(!line.contains("!!!"), "{line}");
    }

    /// The listener's utterance lifecycle narrates as prose, not as the generic
    /// name+fields fallback: an endpointer transition names both states, its cause,
    /// and the sample offset the tuning rig reads timing from.
    #[test]
    fn endpointer_transition_narrates_states_cause_and_offset() {
        let mut r = Renderer::new(false);
        let line = r
            .render(
                0,
                "endpointer_transition",
                &json!({
                    "pod": "pod-fbe2f8",
                    "epoch": 1,
                    "from": "speech",
                    "to": "soft_endpointed",
                    "cause": "soft_endpoint",
                    "sample_offset": 52_256_640_u64,
                }),
            )
            .unwrap();
        assert!(
            line.contains(
                "[pod-fbe2f8] endpointer speech → soft_endpointed (soft_endpoint) @52256640"
            ),
            "{line}"
        );
        assert!(!line.contains("!!!"), "a transition is calm: {line}");
    }

    /// Model stats narrate the whole distribution, not a bare count: the reading a
    /// silent room needs is *what the model returned*, so min/max/mean/median all
    /// reach the console line.
    #[test]
    fn model_stats_narrate_the_score_distribution() {
        let mut r = Renderer::new(false);
        let line = r
            .render(
                0,
                "model_stats",
                &json!({
                    "pod": "pod-fbe2f8",
                    "epoch": 1,
                    "model": "silero",
                    "cause": "periodic",
                    "first_chunk_end": 52_125_568_u64,
                    "last_chunk_end": 52_256_640_u64,
                    "chunks": 256,
                    "min": 0.001,
                    "max": 0.031,
                    "mean": 0.004,
                    "median": 0.002,
                }),
            )
            .unwrap();
        assert!(
            line.contains(
                "[pod-fbe2f8] silero p x256: min 0.001 max 0.031 mean 0.004 median 0.002 \
                 (periodic) @52256640"
            ),
            "{line}"
        );
        assert!(!line.contains("!!!"), "stats are calm: {line}");
    }

    /// The rest of the lifecycle: a wake detection (JSONL-only until now), a
    /// supersede and close correlatable by utterance seq, and an arm expiry. All
    /// calm, all prose.
    #[test]
    fn listener_lifecycle_events_narrate_calmly() {
        let mut r = Renderer::new(false);
        let uid = json!({ "pod": "pod-x", "epoch": 0, "seq": 4 });

        let wake = r
            .render(
                0,
                "wake_detected",
                &json!({ "pod": "pod-x", "score": 0.836, "wake_end_sample": 52_256_640_u64 }),
            )
            .unwrap();
        assert!(
            wake.contains("wake detected — score 0.836 @52256640"),
            "{wake}"
        );

        let sup = r
            .render(
                0,
                "utterance_superseded",
                &json!({ "pod": "pod-x", "utterance_id": uid }),
            )
            .unwrap();
        assert!(sup.contains("utterance seq 4 superseded"), "{sup}");

        let closed = r
            .render(
                0,
                "utterance_closed",
                &json!({ "pod": "pod-x", "utterance_id": uid }),
            )
            .unwrap();
        assert!(closed.contains("utterance seq 4 closed"), "{closed}");

        let expired = r
            .render(
                0,
                "arm_expired",
                &json!({
                    "pod": "pod-x",
                    "score": 0.836,
                    "start_sample": 52_240_640_u64,
                    "end_sample": 52_256_640_u64,
                }),
            )
            .unwrap();
        assert!(
            expired.contains(
                "wake armed but no command followed — score 0.836, span 52240640..52256640"
            ),
            "{expired}"
        );
        for line in [&wake, &sup, &closed, &expired] {
            assert!(!line.contains("!!!"), "calm: {line}");
        }
    }

    /// A malformed listener event still renders: every field degrades to `?`
    /// rather than panicking the console task or dropping the line.
    #[test]
    fn listener_event_fields_degrade_to_question_marks() {
        let mut r = Renderer::new(false);
        let line = r.render(0, "endpointer_transition", &json!({})).unwrap();
        assert!(line.contains("endpointer ? → ? (?) @?"), "{line}");
        // A `utterance_id` that is not an object (or lacks `seq`) degrades too.
        let closed = r
            .render(0, "utterance_closed", &json!({ "utterance_id": 7 }))
            .unwrap();
        assert!(closed.contains("utterance seq ? closed"), "{closed}");
    }

    #[test]
    fn structural_and_bidi_chars_are_neutralized() {
        let mut r = Renderer::new(false);
        // A hostile pod id trying to break out of the `[room/pod]` tag, tested
        // directly on `tag_string` so it does not depend on which event happens
        // to lack a bespoke arm.
        let tag = tag_string(&json!({ "pod": "evil]tail" }));
        assert!(!tag.contains("evil]"), "bracket neutralized: {tag}");
        // A bidi override (Trojan-Source) inside a transcript, through the full
        // utterance narration (a stable bespoke arm).
        let bidi = r
            .render(
                0,
                "utterance",
                &json!({ "id": 1, "transcript": { "text": "a\u{202e}b\"c" } }),
            )
            .unwrap();
        assert!(!bidi.contains('\u{202e}'), "bidi neutralized: {bidi}");
        // The renderer's own quotes frame the transcript; an embedded `"` cannot
        // add a second pair.
        assert_eq!(bidi.matches('"').count(), 2, "{bidi}");
    }

    #[test]
    fn invisible_format_chars_are_neutralized() {
        // An invisible word joiner appended to a pod id would otherwise render
        // identical to the legitimate id, letting a hostile pod spoof identity on
        // the console. It is a Cf-category char `char::is_control` does not catch.
        let mut r = Renderer::new(false);
        let line = r
            .render(
                0,
                "conn_hello",
                &json!({ "pod_id": "pod-a1b2c3\u{2060}", "conn_seq": 1 }),
            )
            .unwrap();
        assert!(!line.contains('\u{2060}'), "{line}");
        // Soft hyphen, a variation selector, and a tag-block char are likewise
        // stripped, while legitimate visible non-ASCII text survives.
        assert_eq!(
            sanitize("a\u{00AD}b\u{FE0F}c\u{E0041}"),
            "a\u{fffd}b\u{fffd}c\u{fffd}"
        );
        assert_eq!(sanitize("café"), "café");
    }

    #[test]
    fn loud_line_has_bang_prefix_without_color() {
        let mut r = Renderer::new(false);
        let line = r
            .render(0, "stt_failed", &json!({ "detail": "connection refused" }))
            .unwrap();
        assert!(line.contains("!!! stt_failed"), "{line}");
        assert!(line.contains("detail=connection refused"), "{line}");
        assert!(!line.contains('\x1b'), "no ANSI when color off: {line}");
    }

    #[test]
    fn color_wraps_loud_lines_only() {
        let mut r = Renderer::new(true);
        assert!(r
            .render(0, "stt_failed", &json!({}))
            .unwrap()
            .contains('\x1b'));
        assert!(!r
            .render(0, "segment_opened", &json!({}))
            .unwrap()
            .contains('\x1b'));
    }

    #[test]
    fn wake_decision_is_loud_only_on_error_outcome() {
        let mut r = Renderer::new(false);
        assert!(r
            .render(0, "wake_decision", &json!({ "outcome": "positive" }))
            .unwrap()
            .contains("wake positive"));
        assert!(!r
            .render(0, "wake_decision", &json!({ "outcome": "positive" }))
            .unwrap()
            .contains("!!!"));
        assert!(r
            .render(0, "wake_decision", &json!({ "outcome": "error" }))
            .unwrap()
            .contains("!!!"));
    }

    #[test]
    fn wake_decision_narrates_per_outcome() {
        let mut r = Renderer::new(false);
        let pos = r
            .render(
                0,
                "wake_decision",
                &json!({
                    "room": "kitchen", "pod": "pod-a1b2c3", "segment_id": 7,
                    "outcome": "positive", "score": 0.874, "threshold": 0.5,
                    "infer_us": 31000
                }),
            )
            .unwrap();
        assert!(
            pos.ends_with(
                "[kitchen/pod-a1b2c3] ✓ wake positive — score 0.874 ≥ 0.500 (infer 31 ms)"
            ),
            "{pos}"
        );

        let neg = r
            .render(
                0,
                "wake_decision",
                &json!({ "outcome": "negative", "score": 0.121, "threshold": 0.5 }),
            )
            .unwrap();
        assert!(
            neg.ends_with("✗ wake negative — score 0.121 < 0.500"),
            "{neg}"
        );

        // Bypass applies no threshold; none is invented.
        let byp = r
            .render(
                0,
                "wake_decision",
                &json!({ "outcome": "bypassed", "threshold": null }),
            )
            .unwrap();
        assert!(byp.ends_with("✓ wake bypassed (no gate)"), "{byp}");
        assert!(!byp.contains("!!!"), "{byp}");

        let err = r
            .render(
                0,
                "wake_decision",
                &json!({ "outcome": "error", "error": "sidecar timeout" }),
            )
            .unwrap();
        assert!(err.contains("!!! wake_decision"), "{err}");
        assert!(err.contains("error=sidecar timeout"), "{err}");
    }

    #[test]
    fn utterance_narrates_with_and_without_transcript() {
        let mut r = Renderer::new(false);
        let with = r
            .render(
                0,
                "utterance",
                &json!({
                    "room": "kitchen", "pod": "pod-a1b2c3", "id": 3,
                    "transcript": { "text": "turn on the kitchen lights" }
                }),
            )
            .unwrap();
        assert!(
            with.ends_with("[kitchen/pod-a1b2c3] utterance #3 — \"turn on the kitchen lights\""),
            "{with}"
        );
        assert!(!with.contains("!!!"), "{with}");

        // Null transcript (no transcriber wired, or STT failed) is calm, not loud.
        let without = r
            .render(0, "utterance", &json!({ "id": 4, "transcript": null }))
            .unwrap();
        assert!(
            without.ends_with("utterance #4 — (no transcript)"),
            "{without}"
        );
        assert!(!without.contains("!!!"), "{without}");
    }

    #[test]
    fn utterance_appends_confidence_when_present() {
        let mut r = Renderer::new(false);
        let line = r
            .render(
                0,
                "utterance",
                &json!({
                    "id": 5,
                    "transcript": {
                        "text": "turn on the lights",
                        "confidence": {
                            "avg_logprob": -0.23, "no_speech_prob": 0.02,
                            "compression_ratio": 1.4, "segments": 2
                        }
                    }
                }),
            )
            .unwrap();
        assert!(
            line.ends_with(
                "utterance #5 — \"turn on the lights\" conf: logprob=-0.23 no_speech=0.02 compress=1.4"
            ),
            "{line}"
        );
        assert!(!line.contains("!!!"), "{line}");
        // A transcript without a confidence summary (plain-json backend) keeps the
        // bare line — no dangling `conf:` tail.
        let bare = r
            .render(
                0,
                "utterance",
                &json!({ "id": 6, "transcript": { "text": "hi" } }),
            )
            .unwrap();
        assert!(bare.ends_with("utterance #6 — \"hi\""), "{bare}");
        assert!(!bare.contains("conf:"), "{bare}");
    }

    #[test]
    fn utterance_confidence_suffix_degrades_missing_fields_to_question_marks() {
        let mut r = Renderer::new(false);
        // A partial confidence object (only avg_logprob present) still renders a
        // full tail; the absent numerics degrade to `?` rather than dropping the
        // line or the whole tail.
        let line = r
            .render(
                0,
                "utterance",
                &json!({
                    "id": 8,
                    "transcript": {
                        "text": "hi",
                        "confidence": { "avg_logprob": -0.2 }
                    }
                }),
            )
            .unwrap();
        assert!(
            line.ends_with("conf: logprob=-0.20 no_speech=? compress=?"),
            "{line}"
        );
        // A confidence value that is present but not an object is ignored: no
        // dangling `conf:` tail from a malformed shape.
        let non_object = r
            .render(
                0,
                "utterance",
                &json!({
                    "id": 9,
                    "transcript": { "text": "hi", "confidence": "oops" }
                }),
            )
            .unwrap();
        assert!(
            non_object.ends_with("utterance #9 — \"hi\""),
            "{non_object}"
        );
        assert!(!non_object.contains("conf:"), "{non_object}");
    }

    #[test]
    fn brain_startup_narrates_per_mode() {
        let mut r = Renderer::new(false);
        // The fixed-label modes are tag-less calm lines.
        let echo = r.render(0, "brain_echo", &json!({})).unwrap();
        assert!(echo.ends_with("brain: echo"), "{echo}");
        assert!(!echo.contains("!!!"), "{echo}");
        let absent = r
            .render(
                0,
                "brain_absent",
                &json!({ "reason": "no [brain] table configured" }),
            )
            .unwrap();
        assert!(absent.ends_with("brain: none"), "{absent}");
        assert!(!absent.contains("!!!"), "{absent}");
        // 30400 samples / 16000 Hz = 1.9 s.
        let clip = r
            .render(
                0,
                "brain_clip_loaded",
                &json!({ "clip": "./ack.wav", "samples": 30400, "duration_ms": 1900 }),
            )
            .unwrap();
        assert!(clip.ends_with("brain: clip ./ack.wav (1.9s)"), "{clip}");
        assert!(!clip.contains("!!!"), "{clip}");
        // Missing clip fields degrade to `?` rather than panicking.
        let bare = r.render(0, "brain_clip_loaded", &json!({})).unwrap();
        assert!(bare.ends_with("brain: clip ? (?)"), "{bare}");
    }

    #[test]
    fn stt_startup_narrates() {
        let mut r = Renderer::new(false);
        // Tag-less calm lines: the startup emits carry no room/pod.
        let configured = r
            .render(
                0,
                "stt_configured",
                &json!({
                    "url": "http://10.0.0.5:8000",
                    "model": "Systran/faster-whisper-small", "language": "en"
                }),
            )
            .unwrap();
        assert!(
            configured.ends_with(
                "stt configured — http://10.0.0.5:8000 model=Systran/faster-whisper-small lang=en"
            ),
            "{configured}"
        );
        assert!(!configured.contains("!!!"), "{configured}");
        let absent = r
            .render(
                0,
                "stt_absent",
                &json!({ "reason": "no [stt] table configured" }),
            )
            .unwrap();
        assert!(absent.ends_with("stt: none"), "{absent}");
        assert!(!absent.contains("!!!"), "{absent}");
        // An unset language renders `auto`, not `?`; a missing url/model degrades.
        let auto = r
            .render(
                0,
                "stt_configured",
                &json!({ "url": "http://h:8000", "model": "m", "language": null }),
            )
            .unwrap();
        assert!(auto.ends_with("model=m lang=auto"), "{auto}");
        let bare = r.render(0, "stt_configured", &json!({})).unwrap();
        assert!(
            bare.ends_with("stt configured — ? model=? lang=auto"),
            "{bare}"
        );
    }

    #[test]
    fn tts_startup_narrates() {
        let mut r = Renderer::new(false);
        // Tag-less calm lines: the startup emits carry no room/pod.
        let configured = r
            .render(
                0,
                "tts_configured",
                &json!({
                    "url": "http://10.0.0.5:8000",
                    "model": "speaches-ai/Kokoro-82M", "voice": "af_heart"
                }),
            )
            .unwrap();
        assert!(
            configured.ends_with(
                "tts configured — http://10.0.0.5:8000 model=speaches-ai/Kokoro-82M voice=af_heart"
            ),
            "{configured}"
        );
        assert!(!configured.contains("!!!"), "{configured}");
        let absent = r
            .render(
                0,
                "tts_absent",
                &json!({ "reason": "no [tts] table configured" }),
            )
            .unwrap();
        assert!(absent.ends_with("tts: none"), "{absent}");
        assert!(!absent.contains("!!!"), "{absent}");
        // Missing url/model/voice degrade to `?` rather than panicking.
        let bare = r.render(0, "tts_configured", &json!({})).unwrap();
        assert!(
            bare.ends_with("tts configured — ? model=? voice=?"),
            "{bare}"
        );
    }

    #[test]
    fn conn_hello_narrates_with_unmapped_warning() {
        let mut r = Renderer::new(false);
        let mapped = r
            .render(
                0,
                "conn_hello",
                &json!({
                    "room": "kitchen", "pod_id": "pod-a1b2c3", "conn_seq": 1,
                    "unmapped": false
                }),
            )
            .unwrap();
        assert!(
            mapped.ends_with("[kitchen/pod-a1b2c3] connected (conn 1)"),
            "{mapped}"
        );
        assert!(!mapped.contains("!!!"), "{mapped}");

        // A pod whose room is not configured is captured under its raw id; the
        // calm line flags the unmapped room without going loud.
        let unmapped = r
            .render(
                0,
                "conn_hello",
                &json!({ "pod_id": "pod-x", "conn_seq": 2, "unmapped": true }),
            )
            .unwrap();
        assert!(
            unmapped.ends_with("[pod-x] connected (conn 2) — room unmapped"),
            "{unmapped}"
        );
        assert!(!unmapped.contains("!!!"), "{unmapped}");

        // Missing conn_seq degrades to `?` rather than panicking.
        let bare = r.render(0, "conn_hello", &json!({})).unwrap();
        assert!(bare.ends_with("connected (conn ?)"), "{bare}");
    }

    #[test]
    fn tracking_and_stage_health_render_nothing() {
        let mut r = Renderer::new(false);
        assert!(r.render(0, "tracking", &json!({ "doa": [1, 2] })).is_none());
        assert!(r.render(0, "stage_health", &json!({})).is_none());
    }

    #[test]
    fn stage_health_healthy_line_is_silent() {
        let mut r = Renderer::new(false);
        // All curated counters at zero: nothing moved, nothing to say. Nested
        // (stt/tts) and top-level counters together, plus non-counter leaves
        // (`at_shutdown`) that must be ignored, never mistaken for a mover.
        let healthy = json!({
            "segment_queue": { "dropped_oldest": 0 },
            "wake": { "errors": 0 },
            "stt": { "failed": 0, "timeouts": 0 },
            "tts": { "failed": 0, "timeouts": 0 },
            "jsonl_dropped": 0,
            "console_dropped": 0,
            "at_shutdown": false
        });
        assert!(r.render(0, "stage_health", &healthy).is_none());
        // A second identical line is still silent (unchanged nonzero would be
        // too, exercised below).
        assert!(r.render(0, "stage_health", &healthy).is_none());
    }

    #[test]
    fn stage_health_first_line_reports_nonzero_against_zero() {
        let mut r = Renderer::new(false);
        // Counters already nonzero at first emission diff against zero and are
        // reported — the run may have accumulated failures before the first
        // health tick.
        let line = r
            .render(
                0,
                "stage_health",
                &json!({ "jsonl_dropped": 5, "console_dropped": 0 }),
            )
            .unwrap();
        assert!(
            line.ends_with("!!! stage_health: jsonl_dropped +5"),
            "{line}"
        );
    }

    #[test]
    fn stage_health_reports_queue_send_failures_as_a_mover() {
        let mut r = Renderer::new(false);
        r.render(
            0,
            "stage_health",
            &json!({ "segment_queue": { "send_failures": 0 } }),
        );
        let line = r
            .render(
                0,
                "stage_health",
                &json!({ "segment_queue": { "send_failures": 3 } }),
            )
            .unwrap();
        assert!(
            line.ends_with("!!! stage_health: segment_queue.send_failures +3"),
            "{line}"
        );
    }

    #[test]
    fn stage_health_reports_only_movers_by_delta() {
        let mut r = Renderer::new(false);
        // Establish a baseline snapshot. The first line diffs against zero, so
        // it reports these initial values; we only care that it advances the
        // snapshot for the delta check below.
        r.render(
            0,
            "stage_health",
            &json!({ "stt": { "failed": 1 }, "jsonl_dropped": 2 }),
        );
        // stt.failed +2 and jsonl_dropped +5; the delta is reported, not the
        // absolute value. Keys are emitted in sorted-path order (`jsonl_dropped`
        // before `stt.failed`), so the line is deterministic.
        let line = r
            .render(
                0,
                "stage_health",
                &json!({ "stt": { "failed": 3 }, "jsonl_dropped": 7 }),
            )
            .unwrap();
        assert!(
            line.ends_with("!!! stage_health: jsonl_dropped +5, stt.failed +2"),
            "{line}"
        );
        // Nothing further moves: silent again despite the counters being nonzero.
        assert!(r
            .render(
                0,
                "stage_health",
                &json!({ "stt": { "failed": 3 }, "jsonl_dropped": 7 }),
            )
            .is_none());
    }

    #[test]
    fn stage_health_surfaces_eoa_write_failures() {
        let mut r = Renderer::new(false);
        // A non-timeout EOA drain failure has no discrete loud event — its only
        // trace is a calm `playback_finished … no EOA` line and this counter. The
        // backstop must name it, or a writer-dead failure is invisible.
        let line = r
            .render(
                0,
                "stage_health",
                &json!({ "playback": { "eoa_write_failures": 1 } }),
            )
            .unwrap();
        assert!(
            line.ends_with("!!! stage_health: playback.eoa_write_failures +1"),
            "{line}"
        );
    }

    #[test]
    fn stt_configured_mistyped_language_degrades_to_question_mark() {
        let mut r = Renderer::new(false);
        // A wrong-typed `language` is a malformed event; it degrades to `?` like
        // every other narrator, not to a valid-looking `auto`.
        let line = r
            .render(
                0,
                "stt_configured",
                &json!({ "url": "http://h:8000", "model": "m", "language": 7 }),
            )
            .unwrap();
        assert!(line.ends_with("model=m lang=?"), "{line}");
    }

    #[test]
    fn stage_health_paths_are_independent_across_stages() {
        let mut r = Renderer::new(false);
        // `failed` occurs under both `stt` and `tts`. Keying by full dotted path
        // (not bare leaf name) keeps their snapshots separate. The first line
        // reports both against zero; it just seeds the snapshot here.
        r.render(
            0,
            "stage_health",
            &json!({ "stt": { "failed": 1 }, "tts": { "failed": 1 } }),
        );
        // Only stt moves; tts is unchanged. A bare-leaf-keyed snapshot would let
        // the tts value overwrite stt's and mask (or misattribute) the increment.
        let line = r
            .render(
                0,
                "stage_health",
                &json!({ "stt": { "failed": 3 }, "tts": { "failed": 1 } }),
            )
            .unwrap();
        assert!(line.ends_with("!!! stage_health: stt.failed +2"), "{line}");
        assert!(!line.contains("tts"), "{line}");
    }

    #[test]
    fn stage_health_shutdown_always_renders_run_summary() {
        let mut r = Renderer::new(false);
        // A clean run: no counter moved, so no loud delta line — but the
        // `at_shutdown` line still renders the calm one-line recap.
        let clean = r
            .render(
                0,
                "stage_health",
                &json!({
                    "segment_queue": { "pushed": 12, "dropped_oldest": 0 },
                    "wake": { "detected": 3, "rejected": 8 },
                    "playback": { "jobs_completed": 3 },
                    "jsonl_dropped": 0,
                    "at_shutdown": true
                }),
            )
            .unwrap();
        assert!(
            clean.ends_with(
                "run summary — 12 segments, wake 3 detected / 8 rejected, 3 playback jobs, 0 drops"
            ),
            "{clean}"
        );
        assert!(!clean.contains("!!!"), "{clean}");
        assert_eq!(clean.lines().count(), 1, "{clean}");
        // Missing sections degrade to `?` rather than panicking.
        let bare = r
            .render(0, "stage_health", &json!({ "at_shutdown": true }))
            .unwrap();
        assert!(
            bare.ends_with(
                "run summary — ? segments, wake ? detected / ? rejected, ? playback jobs, ? drops"
            ),
            "{bare}"
        );
    }

    #[test]
    fn stage_health_shutdown_appends_loud_delta_when_counters_moved() {
        let mut r = Renderer::new(false);
        // A shutdown line where an error counter is nonzero: two lines — the calm
        // summary, then the loud delta (first line diffs against zero).
        let out = r
            .render(
                0,
                "stage_health",
                &json!({
                    "segment_queue": { "pushed": 5, "dropped_oldest": 2 },
                    "wake": { "detected": 1, "rejected": 0 },
                    "playback": { "jobs_completed": 1 },
                    "jsonl_dropped": 4,
                    "at_shutdown": true
                }),
            )
            .unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 2, "{out}");
        assert!(
            lines[0].ends_with(
                "run summary — 5 segments, wake 1 detected / 0 rejected, 1 playback jobs, 2 drops"
            ),
            "{out}"
        );
        assert!(!lines[0].contains("!!!"), "{out}");
        // Both curated movers, named by full dotted path in sorted-path order.
        assert!(
            lines[1]
                .ends_with("!!! stage_health: jsonl_dropped +4, segment_queue.dropped_oldest +2"),
            "{out}"
        );
    }

    #[test]
    fn stage_health_color_wraps_the_backstop_line() {
        let mut r = Renderer::new(true);
        let line = r
            .render(0, "stage_health", &json!({ "jsonl_dropped": 1 }))
            .unwrap();
        assert!(line.contains('\x1b'), "{line}");
    }

    #[test]
    fn stage_health_ignores_malformed_and_non_counter_leaves() {
        let mut r = Renderer::new(false);
        // A non-object value, a curated leaf with a non-numeric value, and a
        // non-curated leaf: none panics, none is a mover.
        assert!(r.render(0, "stage_health", &Value::Null).is_none());
        assert!(r
            .render(
                0,
                "stage_health",
                &json!({ "stt": { "failed": "lots" }, "unrelated": 9 }),
            )
            .is_none());
    }

    #[test]
    fn malformed_event_never_panics() {
        let mut r = Renderer::new(false);
        // Non-object fields, and a missing outcome on wake_decision.
        assert!(r.render(0, "segment_opened", &Value::Null).is_some());
        assert!(r.render(0, "wake_decision", &json!({})).is_some());
    }

    #[test]
    fn control_characters_are_neutralized() {
        let mut r = Renderer::new(false);
        let line = r
            .render(0, "utterance", &json!({ "text": "hi\x1b[31mred\x07\n" }))
            .unwrap();
        assert!(!line.contains('\x1b'), "{line}");
        assert!(!line.contains('\x07'), "{line}");
    }

    #[test]
    fn long_fields_truncate_with_ellipsis() {
        let mut r = Renderer::new(false);
        let long = "x".repeat(500);
        let line = r
            .render(0, "stt_failed", &json!({ "detail": long }))
            .unwrap();
        assert!(line.contains('…'), "{line}");
        let xs = line.chars().filter(|&c| c == 'x').count();
        assert!(xs == MAX_FIELD_CHARS, "xs={xs} line={line}");
    }

    /// How an emitted event surfaces on the console.
    #[derive(Clone, Copy)]
    enum Class {
        /// Renders loud (`!!! ` prefix, red on a terminal): a failure or warning.
        Loud,
        /// Renders a calm console line: startup/progress narration.
        Calm,
        /// Stays file-only: high-rate or routine, no console line.
        FileOnly,
    }

    /// Every event name the daemon emits, each classified by how it surfaces on
    /// the console. Enumerated from the emit sites in `server.rs`, `pipeline.rs`,
    /// `playback_router.rs`, `bin/speech-surface.rs`, and the two synthetic
    /// events `jsonl.rs` raises (`jsonl_encode_error`, `console_sink_failed`).
    /// Maintained alongside the mapping above: adding an emit site means adding
    /// its name here with a class, at which point
    /// `console_classification_is_exhaustive` pins that class against the real
    /// `wants`/`is_loud` behavior in both directions. It cannot force an author
    /// to list a brand-new emit site at all — that step is convention-dependent
    /// — but once listed, a `Loud` event that stops rendering loud, a `Calm` one
    /// that starts, or a `FileOnly` one that leaks to the console fails the test.
    const EVENTS: &[(&str, Class)] = &[
        ("arm_expired", Class::Calm),
        ("brain_absent", Class::Calm),
        ("brain_clip_loaded", Class::Calm),
        ("brain_dispatched", Class::Calm),
        ("brain_echo", Class::Calm),
        ("brain_no_transcript", Class::Loud),
        ("brain_sink_full", Class::Loud),
        ("conn_accept_error", Class::Loud),
        ("conn_accepted", Class::FileOnly),
        ("conn_closed", Class::Calm),
        ("conn_hello", Class::Calm),
        ("conn_rejected", Class::Loud),
        ("conn_superseded", Class::Calm),
        ("console_sink_failed", Class::Loud),
        ("daemon_start", Class::Calm),
        ("endpointer_transition", Class::Calm),
        ("model_stats", Class::Calm),
        ("jsonl_encode_error", Class::Loud),
        ("latency_summary", Class::Calm),
        ("listener_event_dropped_overflow", Class::Loud),
        ("listener_thread_panicked", Class::Loud),
        ("listening", Class::Calm),
        ("pipeline_fatal", Class::Loud),
        ("playback_aborted", Class::Loud),
        ("playback_finished", Class::Calm),
        ("playback_hello", Class::FileOnly),
        ("playback_hello_failed", Class::Loud),
        ("playback_no_pod", Class::Loud),
        ("playback_rejected", Class::Loud),
        ("playback_router_exited", Class::Loud),
        ("playback_started", Class::Calm),
        ("playback_writer_dead", Class::Loud),
        ("protocol_error", Class::Loud),
        ("prune_delete_error", Class::Loud),
        ("prune_error", Class::Loud),
        ("prune_halted", Class::Loud),
        ("prune_sidecar_corrupt", Class::Loud),
        ("record_error", Class::Loud),
        ("record_pruned", Class::FileOnly),
        ("record_rolled", Class::FileOnly),
        ("segment_closed", Class::Calm),
        ("segment_dropped_overflow", Class::Loud),
        ("segment_opened", Class::Calm),
        ("speak_rx", Class::Calm),
        ("speak_unsupported", Class::Loud),
        ("stage_health", Class::Calm),
        ("stage_health_emitter_exited", Class::Loud),
        ("stt_absent", Class::Calm),
        ("stt_configured", Class::Calm),
        ("stt_failed", Class::Loud),
        ("stt_started", Class::Calm),
        ("synth", Class::Calm),
        ("synth_failed", Class::Loud),
        ("tracking", Class::FileOnly),
        ("tts_absent", Class::Calm),
        ("tts_configured", Class::Calm),
        ("utterance", Class::Calm),
        ("utterance_closed", Class::Calm),
        ("utterance_superseded", Class::Calm),
        ("wake_bypassed", Class::Loud),
        ("wake_command_absent", Class::Calm),
        ("wake_decision", Class::Loud),
        ("wake_detected", Class::Calm),
        ("wake_sidecar_error", Class::Loud),
        ("wake_sidecar_skipped", Class::Loud),
        ("wake_stage_panicked", Class::Loud),
    ];

    #[test]
    fn console_classification_is_exhaustive() {
        // A `wake_decision` is loud only for a non-verdict outcome; every other
        // classified event's loudness is outcome-independent. The error outcome
        // exercises the one that cares; empty fields exercise the calm path.
        let error_outcome = json!({ "outcome": "error" });
        let calm_fields = json!({});
        for &(event, class) in EVENTS {
            match class {
                Class::Loud => {
                    assert!(
                        wants(event),
                        "{event} is classified Loud but the console filter drops it"
                    );
                    assert!(
                        is_loud(event, &error_outcome),
                        "{event} is classified Loud but renders calm — add an \
                         ERROR_TOKENS entry or an explicit loud mapping"
                    );
                }
                Class::Calm => {
                    assert!(
                        wants(event),
                        "{event} is classified Calm but the console filter drops it"
                    );
                    assert!(
                        !is_loud(event, &calm_fields),
                        "{event} is classified Calm but renders loud — its name \
                         matches an ERROR_TOKENS entry; reclassify or rename"
                    );
                }
                Class::FileOnly => {
                    assert!(
                        !wants(event),
                        "{event} is classified FileOnly but the console filter accepts it"
                    );
                }
            }
        }
    }
}
