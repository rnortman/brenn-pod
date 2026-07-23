//! `listener-replay`: drive captured frame logs through the streaming listener
//! (openWakeWord + Silero endpointer) offline — no device, no daemon — and print
//! the wake detections and endpoint decisions as JSONL.
//!
//! This is the endpointer/OWW threshold-tuning rig and the deafness-bug
//! regression harness: the `[wake]` + `[endpointer]` tables of a daemon config
//! choose the models and thresholds, each frame log replays through a fresh
//! per-pod listener exactly as the live server would feed it, and every
//! `ListenerEvent` (wake detection, carved utterance, supersede, close, arm
//! expiry) is reported. Re-run with different thresholds to tune; assert on the
//! output to guard a regression.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;
use serde_json::json;

use speech_pipeline::{EndpointCause, ListenerEvent};
use speech_surface::config::Config;
use speech_surface::pipeline::event_line;
use speech_surface::replay::{ReplayError, ReplayListener, StopReason, replay_framelog};
use speech_surface::{emit_line as emit, exit};

#[derive(Parser)]
#[command(
    name = "listener-replay",
    about = "Replay recorded frame logs through the streaming listener offline (no device)"
)]
struct Cli {
    /// Daemon config TOML supplying the `[wake]` + `[endpointer]` tables (models
    /// and thresholds). Only those tables are read; the rest is ignored.
    #[arg(long)]
    config: PathBuf,
    /// Frame logs to replay, in order. Each replays through its own fresh listener.
    #[arg(required = true)]
    framelogs: Vec<PathBuf>,
}

/// The connection epoch stamped onto every replayed utterance id. A replay is one
/// connection per log; the value is arbitrary (the pipeline uses it only to drop
/// events from a superseded connection, which a single offline replay never has).
const REPLAY_EPOCH: u64 = 1;

fn main() -> ExitCode {
    let cli = Cli::parse();

    let config = match Config::load(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            emit(
                "config_error",
                json!({ "config": cli.config.display().to_string(), "detail": e.to_string() }),
            );
            return ExitCode::from(exit::HARD_FAILURE);
        }
    };

    let mut listener = match ReplayListener::from_config(&config) {
        Ok(Some(l)) => l,
        Ok(None) => {
            emit(
                "no_listener",
                json!({
                    "config": cli.config.display().to_string(),
                    "detail": "config has no streaming listener ([wake] mode=oww + [endpointer] both required)",
                }),
            );
            return ExitCode::from(exit::HARD_FAILURE);
        }
        Err(e) => {
            emit(
                "model_load_error",
                json!({ "config": cli.config.display().to_string(), "detail": format!("{e:?}") }),
            );
            return ExitCode::from(exit::HARD_FAILURE);
        }
    };

    let mut logs = 0u64;
    let mut wakes = 0u64;
    let mut utterances = 0u64;
    let mut any_missing = false;
    let mut any_error = false;

    for path in &cli.framelogs {
        match replay_log(path, &mut listener) {
            Ok(counts) => {
                logs += 1;
                wakes += counts.wakes;
                utterances += counts.utterances;
            }
            Err(ReplayError::Open(e)) if is_not_found(&e) => {
                emit(
                    "input_missing",
                    json!({ "log": path.display().to_string(), "detail": "pruned or never existed" }),
                );
                any_missing = true;
            }
            Err(e) => {
                emit(
                    "replay_error",
                    json!({ "log": path.display().to_string(), "detail": e.to_string() }),
                );
                any_error = true;
            }
        }
    }

    emit(
        "replay_complete",
        json!({ "logs": logs, "wakes": wakes, "utterances": utterances }),
    );

    if any_error {
        ExitCode::from(exit::HARD_FAILURE)
    } else if any_missing {
        ExitCode::from(exit::MISSING_INPUT)
    } else {
        ExitCode::SUCCESS
    }
}

/// Per-log tallies rolled into the run summary.
struct LogCounts {
    wakes: u64,
    utterances: u64,
}

/// Replay one frame log and print every listener event as JSONL.
fn replay_log(path: &Path, listener: &mut ReplayListener) -> Result<LogCounts, ReplayError> {
    let log_name = path.display().to_string();
    let summary = replay_framelog(path, listener, REPLAY_EPOCH)?;

    let mut wakes = 0u64;
    let mut utterances = 0u64;
    for ev in &summary.events {
        match ev {
            ListenerEvent::WakeDetected {
                score,
                wake_end_sample,
                ..
            } => {
                wakes += 1;
                emit(
                    "wake_detected",
                    json!({ "log": log_name, "score": score, "wake_end_sample": wake_end_sample }),
                );
            }
            ListenerEvent::SoftEndpoint { utterance, .. } => {
                utterances += 1;
                emit(
                    "soft_endpoint",
                    json!({
                        "log": log_name,
                        "seq": utterance.utterance_id.seq,
                        "start_sample": utterance.start_sample,
                        "end_sample": utterance.end_sample,
                        "samples": utterance.pcm.len(),
                        "stt_trim_samples": utterance.stt_trim_samples,
                        "cause": cause_str(utterance.cause),
                        "wake_score": utterance.wake.as_ref().map(|w| w.score),
                    }),
                );
            }
            // A framelog carries no playback state, so the floor never opens on
            // replay and this arm is unreachable today. It stays for the day a log
            // records playback: a barge is exactly the event a tuning run wants.
            ListenerEvent::BargeIn { trigger_sample, .. } => {
                emit(
                    "barge_in",
                    json!({ "log": log_name, "trigger_sample": trigger_sample }),
                );
            }
            ListenerEvent::Superseded { utterance_id, .. } => {
                emit(
                    "superseded",
                    json!({ "log": log_name, "seq": utterance_id.seq }),
                );
            }
            ListenerEvent::UtteranceClosed { utterance_id, .. } => {
                emit(
                    "utterance_closed",
                    json!({ "log": log_name, "seq": utterance_id.seq }),
                );
            }
            ListenerEvent::ArmExpired {
                wake,
                start_sample,
                end_sample,
                ..
            } => {
                emit(
                    "arm_expired",
                    json!({
                        "log": log_name,
                        "score": wake.score,
                        "start_sample": start_sample,
                        "end_sample": end_sample,
                    }),
                );
            }
            ListenerEvent::EndpointerTransition { transition, .. } => {
                // Endpointer timing against the framelog: the whole point of the
                // tuning rig, so these print alongside the carve events. Shares the
                // daemon's line builder, so both outputs carry one schema.
                emit(
                    "endpointer_transition",
                    event_line(json!({ "log": log_name }), transition),
                );
            }
            ListenerEvent::ModelStats {
                model,
                cause,
                summary,
                ..
            } => {
                // Model-output distributions against the framelog. Offline analysis
                // of a captured room reads these to answer what the models returned
                // — the reason no separate capture-analysis tool is needed.
                emit(
                    "model_stats",
                    event_line(
                        json!({ "log": log_name, "model": model, "cause": cause }),
                        summary,
                    ),
                );
            }
        }
    }

    emit(
        "log_replayed",
        json!({
            "log": log_name,
            "records": summary.records,
            "stop": stop_str(summary.stop),
            "wakes": wakes,
            "utterances": utterances,
            "overlap_trimmed_samples": summary.overlap_trimmed_samples,
        }),
    );

    Ok(LogCounts { wakes, utterances })
}

fn cause_str(cause: EndpointCause) -> &'static str {
    match cause {
        EndpointCause::SoftEndpoint => "soft_endpoint",
        EndpointCause::Capped => "capped",
        EndpointCause::DeviceVadRelease => "device_vad_release",
    }
}

fn stop_str(stop: StopReason) -> &'static str {
    match stop {
        StopReason::Eof => "eof",
        StopReason::TornTail => "torn_tail",
        StopReason::DecodeError => "decode_error",
        StopReason::CorruptRecord => "corrupt_record",
        StopReason::ProtocolError => "protocol_error",
    }
}

/// Whether an open error is a missing-file error (reported as a pruned-input miss
/// rather than a hard failure).
fn is_not_found(e: &pod_ingest::FrameLogError) -> bool {
    matches!(e, pod_ingest::FrameLogError::Io(io) if io.kind() == std::io::ErrorKind::NotFound)
}
