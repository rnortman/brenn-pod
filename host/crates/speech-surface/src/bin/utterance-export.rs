//! `utterance-export`: export one utterance's recorded audio to a `.wav`.
//!
//! The first production consumer of `AudioSpan::resolve` — everything else
//! this binary does is thin CLI/IO plumbing around that library call, so it
//! proves the readback API rather than growing a second decode path.
//!
//! ```text
//! utterance-export --store-root <dir> --out <file.wav> [<span.json>]
//! ```
//!
//! `<span.json>` (a file argument, or stdin when omitted) is one JSON value:
//! either a bare `AudioSpan`, or any object with an `audio_ref` field holding
//! one — so a captured utterance JSONL line (the daemon's serialized
//! `Utterance`) works directly, with `jq` selecting the line upstream.

use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::Parser;
use serde_json::json;

use speech_pipeline::{write_spine_wav, AudioSpan, ResolvedSpanAudio};
use speech_surface::{emit_line as emit, exit};

#[derive(Parser)]
#[command(
    name = "utterance-export",
    about = "Export a carved utterance's recorded audio to a .wav"
)]
struct Cli {
    /// Record-store root the span's log names are relative to.
    #[arg(long)]
    store_root: PathBuf,
    /// Output `.wav` path (16 kHz mono S16).
    #[arg(long)]
    out: PathBuf,
    /// JSON input file: a bare `AudioSpan`, or an object with an `audio_ref`
    /// field. Omit to read from stdin.
    input: Option<PathBuf>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli) {
        Ok(code) => code,
        Err(e) => {
            emit("export_error", json!({ "error": format!("{e:#}") }));
            ExitCode::from(exit::HARD_FAILURE)
        }
    }
}

fn run(cli: &Cli) -> Result<ExitCode> {
    let input = read_input(cli.input.as_deref())?;
    let value: serde_json::Value = serde_json::from_str(&input).context("parse input JSON")?;
    let span = extract_span(value)?;
    let resolved = resolve_and_write(&cli.store_root, &cli.out, &span)?;

    emit(
        "utterance_exported",
        json!({
            "samples": resolved.pcm.len(),
            "covered_samples": resolved.covered_samples,
            "pruned": resolved.pruned,
            "stopped": resolved.stopped,
            "protocol_errors": resolved.protocol_errors,
            "wav": cli.out.display().to_string(),
        }),
    );

    Ok(ExitCode::from(exit_code_for(&resolved)))
}

/// Map a resolve outcome to the tool's exit code, worst-thing-first: a
/// pruned covering log is `MISSING_INPUT` — part of the span's audio was
/// reclaimed. A present span that only covers a wire gap (`covered_samples ==
/// 0`, `pruned` empty) is `SUCCESS` (`0`): every present input was processed
/// cleanly, which is not the same as missing input. `stopped` (a torn tail,
/// corrupt record, or fatal protocol error) is diagnostic only and never
/// changes the exit code — failing the run over a partial splice would undo
/// the splice's partial-beats-nothing stance. Returns the raw code (not
/// `ExitCode`, which is opaque and unassertable) so the decision is testable
/// in isolation.
fn exit_code_for(resolved: &ResolvedSpanAudio) -> u8 {
    if resolved.pruned.is_empty() {
        0
    } else {
        exit::MISSING_INPUT
    }
}

/// Read the input JSON text from `path`, or from stdin when `None`.
fn read_input(path: Option<&Path>) -> Result<String> {
    match path {
        Some(p) => {
            std::fs::read_to_string(p).with_context(|| format!("read input {}", p.display()))
        }
        None => {
            let mut s = String::new();
            std::io::stdin()
                .read_to_string(&mut s)
                .context("read stdin")?;
            Ok(s)
        }
    }
}

/// Pull an `AudioSpan` out of the input value: an `audio_ref` field if the
/// value is an object carrying one (a captured `Utterance` JSONL line), else
/// the value itself (a bare `AudioSpan`).
fn extract_span(value: serde_json::Value) -> Result<AudioSpan> {
    let span_value = match value.get("audio_ref") {
        Some(v) => v.clone(),
        None => value,
    };
    serde_json::from_value(span_value).context("deserialize AudioSpan")
}

/// Resolve `span`'s audio against `store_root` and write it out as a 16 kHz
/// mono S16 `.wav`. Returns the resolve outcome so the caller can report it
/// and derive the exit code.
fn resolve_and_write(store_root: &Path, out: &Path, span: &AudioSpan) -> Result<ResolvedSpanAudio> {
    let resolved = span.resolve(store_root).context("resolve span")?;
    write_spine_wav(out, &resolved.pcm).with_context(|| format!("write wav {}", out.display()))?;
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use audio_pipeline::wire::StreamFrame;
    use pod_ingest::test_fixtures::{audio, seg_end, seg_start, write_log};
    use pod_ingest::SegmentRef;

    fn hello() -> StreamFrame {
        pod_ingest::test_fixtures::hello("pod-utterance")
    }

    fn sample_span(log_name: &str) -> AudioSpan {
        AudioSpan {
            log: log_name.to_string(),
            start_sample: 0,
            end_sample: 320,
            segments: vec![SegmentRef {
                log: log_name.to_string(),
                segment_id: 5,
                part: 0,
            }],
        }
    }

    #[test]
    fn utterance_shaped_input_round_trips_to_wav() {
        let dir = tempfile::tempdir().unwrap();
        let log_name = "pod-utterance_0.framelog";
        write_log(
            &dir.path().join(log_name),
            &[hello(), seg_start(5, 0), audio(5, 0, 320), seg_end(5, 320)],
        );

        let span = sample_span(log_name);
        // An utterance JSONL line: an object carrying `audio_ref` plus other
        // fields the export tool must ignore.
        let line = json!({
            "id": 1,
            "pod": "pod-utterance",
            "audio_ref": span,
        });
        let extracted = extract_span(line).unwrap();
        assert_eq!(extracted, span);

        let out = dir.path().join("out.wav");
        let resolved = resolve_and_write(dir.path(), &out, &extracted).unwrap();
        assert_eq!(resolved.pcm.len(), 320);
        assert!(resolved.pruned.is_empty());
        assert_eq!(resolved.covered_samples, 320);

        let reader = hound::WavReader::open(&out).unwrap();
        let spec = reader.spec();
        assert_eq!(spec.channels, 1);
        assert_eq!(spec.sample_rate, 16_000);
        let samples: Vec<i16> = reader.into_samples::<i16>().map(|s| s.unwrap()).collect();
        assert_eq!(samples.len(), 320);
        assert_eq!(samples[0], 1);
        assert_eq!(samples[319], 320);
    }

    #[test]
    fn bare_audio_span_input_is_accepted() {
        let span = sample_span("pod-utterance_0.framelog");
        let value = serde_json::to_value(&span).unwrap();
        let extracted = extract_span(value).unwrap();
        assert_eq!(extracted, span);
    }

    #[test]
    fn all_pruned_store_yields_nonempty_pruned() {
        let dir = tempfile::tempdir().unwrap();
        // No log written: the store is empty, so the span's log is pruned.
        let span = sample_span("gone.framelog");
        let out = dir.path().join("out.wav");

        let resolved = resolve_and_write(dir.path(), &out, &span).unwrap();
        assert!(!resolved.pruned.is_empty(), "pruned log must be reported");
        assert_eq!(resolved.covered_samples, 0);
        assert_eq!(resolved.pcm, vec![0i16; 320]);
        assert_eq!(exit_code_for(&resolved), exit::MISSING_INPUT);
    }

    #[test]
    fn present_but_all_silence_span_exits_success() {
        let dir = tempfile::tempdir().unwrap();
        let log_name = "pod-utterance_0.framelog";
        // The log is present, but no segment covers the requested range: a
        // pure wire gap, not missing input.
        write_log(&dir.path().join(log_name), &[hello()]);

        let span = AudioSpan {
            log: log_name.to_string(),
            start_sample: 1_000,
            end_sample: 1_320,
            segments: vec![],
        };
        let out = dir.path().join("out.wav");
        let resolved = resolve_and_write(dir.path(), &out, &span).unwrap();
        assert!(resolved.pruned.is_empty());
        assert_eq!(resolved.covered_samples, 0);
        assert_eq!(
            exit_code_for(&resolved),
            0,
            "present-but-silent must exit SUCCESS, not MISSING_INPUT"
        );
    }

    #[test]
    fn bad_input_json_is_a_run_error() {
        // `run`'s only error path is `main`'s HARD_FAILURE mapping (the sole
        // `Err` arm in `main`), so asserting `run` errors on bad input is
        // sufficient to cover the HARD_FAILURE outcome.
        let dir = tempfile::tempdir().unwrap();
        let input_path = dir.path().join("bad.json");
        std::fs::write(&input_path, b"not json").unwrap();
        let cli = Cli {
            store_root: dir.path().to_path_buf(),
            out: dir.path().join("out.wav"),
            input: Some(input_path),
        };
        assert!(run(&cli).is_err());
    }
}
