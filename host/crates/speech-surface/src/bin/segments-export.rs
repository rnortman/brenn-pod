//! `segments-export`: turn recorded frame logs into per-segment `.wav` files
//! plus a JSON sidecar of segment metadata (end info, gap count, DoA/energy
//! tracks, timings, source reference).
//!
//! It replays each log through the *live* ingest code path minus tokio —
//! `FrameLogReader` → `decode_frame` → `SessionFsm` → `SegmentAssembler` — so
//! an exported segment is byte-for-byte what the daemon would have assembled.
//! Multiple logs are processed in argument order sharing one `ResumeLedger`, so
//! a truncation in log N resumed in log N+1 reproduces live resume semantics; a
//! resume tail exported alone keeps its `Mismatch` cross-check rather than
//! suppressing it. A missing input file is reported as a `Pruned`-style miss
//! (explicit message, distinct exit code), never a bare I/O trace.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use audio_pipeline::wire::decode_frame;
use clap::Parser;
use serde::Serialize;
use serde_json::json;

use pod_ingest::{
    CloseCause, DeviceMicros, FrameLogError, FrameLogReader, HostMicros, LogItem, ResumeLedger,
    SegmentRef, SessionEvent, SessionFsm,
};
use speech_pipeline::{
    AssemblerLimits, DoaTrack, PodId, RoomId, SPINE_FORMAT, Segment, SegmentAssembler,
    SegmentEndInfo, StageTimings, tracking_event, write_spine_wav,
};
use speech_surface::{UNMAPPED_ROOM, emit_line as emit, exit, iso8601_ms, sanitize_filename};

#[derive(Parser)]
#[command(
    name = "segments-export",
    about = "Export recorded frame logs to per-segment .wav + JSON sidecars"
)]
struct Cli {
    /// Directory to write per-segment `.wav` + `.json` into (created if absent).
    #[arg(long)]
    out_dir: PathBuf,
    /// Frame logs to export, in order. A shared resume ledger spans them, so a
    /// truncation resumed in a later log reproduces live semantics.
    #[arg(required = true)]
    framelogs: Vec<PathBuf>,
}

/// The JSON sidecar written beside each exported `.wav`. Excludes the PCM (that
/// is the `.wav`); the DoA/energy tracks are split out via the same
/// tracking-event emitter the live pipeline uses.
#[derive(Serialize)]
struct ExportMeta<'a> {
    pod: &'a PodId,
    room: &'a RoomId,
    segment_id: u32,
    base_sample_index: u64,
    preroll_samples: u32,
    /// Decoded S16 mono sample count (equals the `.wav` frame count).
    samples: usize,
    device_ts: DeviceMicros,
    host_rx: HostMicros,
    end: &'a SegmentEndInfo,
    timings: &'a StageTimings,
    /// Sample-offset-indexed azimuth readings.
    doa: DoaTrack,
    /// Sample-offset-indexed speech-energy readings.
    energy: Vec<(i64, [f32; 4])>,
    audio_ref: &'a SegmentRef,
}

/// Outcome of exporting one frame log.
enum LogOutcome {
    /// The log was read and its segments exported; carries the segment count.
    Exported(u64),
    /// The input file was absent — a `Pruned`-style miss, already reported.
    Missing,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    if let Err(e) = std::fs::create_dir_all(&cli.out_dir) {
        eprintln!("cannot create out-dir {}: {e}", cli.out_dir.display());
        return ExitCode::FAILURE;
    }

    // One ledger shared across every log in argument order, so a segment
    // truncated in one log resumes in a later one.
    let ledger = ResumeLedger::shared();
    let limits = AssemblerLimits::default();
    let mut total = 0u64;
    let mut any_missing = false;

    for path in &cli.framelogs {
        match export_log(path, &cli.out_dir, ledger.clone(), limits) {
            Ok(LogOutcome::Exported(n)) => total += n,
            Ok(LogOutcome::Missing) => any_missing = true,
            Err(e) => {
                emit(
                    "export_error",
                    json!({ "log": path.display().to_string(), "error": format!("{e:#}") }),
                );
                return ExitCode::FAILURE;
            }
        }
    }

    emit("export_complete", json!({ "segments": total }));
    if any_missing {
        ExitCode::from(exit::MISSING_INPUT)
    } else {
        ExitCode::SUCCESS
    }
}

/// Replay one frame log through the live ingest path and export each assembled
/// segment. Open segments left at a torn tail, a decode error, or clean EOF are
/// exported as truncated.
fn export_log(
    path: &Path,
    out_dir: &Path,
    ledger: Arc<Mutex<ResumeLedger>>,
    limits: AssemblerLimits,
) -> Result<LogOutcome> {
    let reader = match FrameLogReader::open(path) {
        Ok(r) => r,
        Err(FrameLogError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            emit(
                "input_missing",
                json!({ "log": path.display().to_string(), "detail": "pruned or never existed" }),
            );
            return Ok(LogOutcome::Missing);
        }
        Err(e) => {
            return Err(anyhow::Error::new(e))
                .with_context(|| format!("open frame log {}", path.display()));
        }
    };

    let log_name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();

    let mut fsm = SessionFsm::new(SPINE_FORMAT, ledger);
    let mut assembler: Option<SegmentAssembler> = None;
    let mut count = 0u64;
    let mut last_host_rx = HostMicros(0);

    // The loop breaks with the cause to close the session under; clean EOF (the
    // loop runs out) falls through to `Eof`. A single finalize follows so every
    // termination path shares one close+drain. A fatal protocol error parks the
    // FSM inside `feed`, so its post-loop close is a no-op (no open segment).
    let mut close_cause = CloseCause::Eof;
    for item in reader {
        match item {
            Ok(LogItem::Record { host_rx, payload }) => {
                last_host_rx = host_rx;
                match decode_frame(&payload) {
                    Ok(frame) => {
                        let events = fsm.feed(frame, host_rx);
                        let (n, fatal) =
                            drain(&events, &mut assembler, &log_name, out_dir, limits)?;
                        count += n;
                        if fatal {
                            break;
                        }
                    }
                    Err(e) => {
                        // A captured-but-undecodable frame ends the log here, as
                        // the live path drops the connection on a decode error.
                        // Report it so the resulting truncation is attributable
                        // and the early stop is not silent.
                        emit(
                            "decode_error",
                            json!({
                                "log": log_name,
                                "host_rx_us": host_rx.0,
                                "detail": format!("{e:?}"),
                            }),
                        );
                        close_cause = CloseCause::DecodeError;
                        break;
                    }
                }
            }
            Ok(LogItem::TornTail) => {
                close_cause = CloseCause::Eof;
                break;
            }
            Err(e) => {
                // A corrupt record mid-log: report and finalize what we have.
                emit(
                    "log_corrupt",
                    json!({ "log": log_name, "detail": e.to_string() }),
                );
                close_cause = CloseCause::ReadError;
                break;
            }
        }
    }

    // Single finalize for every path: close the session (truncating any open
    // segment) and export whatever it yields.
    let events = fsm.close(close_cause, last_host_rx);
    count += drain(&events, &mut assembler, &log_name, out_dir, limits)?.0;

    emit(
        "log_exported",
        json!({ "log": log_name, "segments": count }),
    );
    Ok(LogOutcome::Exported(count))
}

/// Feed a batch of session events into the assembler, exporting any segment that
/// completes. Constructs the assembler on `HelloAccepted` (the pod identity is
/// unknown before then). Returns the number of segments exported and whether a
/// fatal protocol error parked the FSM.
fn drain(
    events: &[SessionEvent],
    assembler: &mut Option<SegmentAssembler>,
    log_name: &str,
    out_dir: &Path,
    limits: AssemblerLimits,
) -> Result<(u64, bool)> {
    let mut count = 0u64;
    let mut fatal = false;
    for ev in events {
        if let SessionEvent::HelloAccepted { pod_id, .. } = ev {
            // Offline export has no pod→room config; the room is unmapped.
            *assembler = Some(SegmentAssembler::new(
                PodId(pod_id.clone()),
                RoomId(UNMAPPED_ROOM.to_string()),
                limits,
            ));
        }
        if let SessionEvent::ProtocolError { fatal: true, .. } = ev {
            fatal = true;
        }
        if let Some(a) = assembler.as_mut()
            && let Some(seg) = a.on_event(ev, log_name)
        {
            export_segment(&seg, out_dir)?;
            count += 1;
        }
    }
    Ok((count, fatal))
}

/// Write one segment's `.wav` (16 kHz mono S16) and JSON sidecar.
fn export_segment(seg: &Segment, out_dir: &Path) -> Result<()> {
    let stem = format!(
        "{}_{}_{}",
        sanitize_filename(&seg.pod.0),
        iso8601_ms(seg.host_rx.0),
        seg.segment_id
    );
    let wav_path = out_dir.join(format!("{stem}.wav"));
    let json_path = out_dir.join(format!("{stem}.json"));

    write_spine_wav(&wav_path, &seg.pcm)
        .with_context(|| format!("write wav {}", wav_path.display()))?;

    // Split telemetry into DoA/energy tracks via the same emitter the live
    // pipeline uses, so the sidecar's tracks match the tracking event exactly.
    let track = tracking_event(seg);
    let meta = ExportMeta {
        pod: &seg.pod,
        room: &seg.room,
        segment_id: seg.segment_id,
        base_sample_index: seg.base_sample_index,
        preroll_samples: seg.preroll_samples,
        samples: seg.pcm.len(),
        device_ts: seg.device_ts,
        host_rx: seg.host_rx,
        end: &seg.end,
        timings: &seg.timings,
        doa: track.doa,
        energy: track.energy,
        audio_ref: &seg.audio_ref,
    };
    let bytes = serde_json::to_vec_pretty(&meta).context("serialize sidecar")?;
    std::fs::write(&json_path, bytes)
        .with_context(|| format!("write sidecar {}", json_path.display()))?;

    emit(
        "segment_exported",
        json!({
            "pod": seg.pod.0,
            "segment_id": seg.segment_id,
            "samples": seg.pcm.len(),
            "truncated": seg.end.truncated,
            "wav": wav_path.display().to_string(),
            "sidecar": json_path.display().to_string(),
        }),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use audio_pipeline::wire::{
        AudioFrame, MAX_AUDIO_PAYLOAD, SegmentEnd, SegmentStart, StreamFrame, Telemetry,
    };
    use audio_pipeline::wire::{EndReason, TelemetryKind};
    use heapless::Vec as HVec;
    use pod_ingest::test_fixtures::write_log_at;

    fn hello() -> StreamFrame {
        pod_ingest::test_fixtures::hello("pod-export")
    }

    fn audio(segment_id: u32, first: u64, n_samples: usize) -> StreamFrame {
        let mut pcm: HVec<u8, MAX_AUDIO_PAYLOAD> = HVec::new();
        for i in 0..n_samples {
            // Distinct little-endian S16 samples so the .wav is checkable.
            let v = (i as i16).to_le_bytes();
            pcm.push(v[0]).unwrap();
            pcm.push(v[1]).unwrap();
        }
        StreamFrame::Audio(AudioFrame {
            segment_id,
            first_sample_index: first,
            device_ts_us: 0,
            pcm,
        })
    }

    fn write_log(path: &Path, frames: &[StreamFrame]) {
        write_log_at(path, 1_700_000_000_000_000, frames);
    }

    #[test]
    fn exports_wav_and_sidecar_with_doa_track() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("cap.framelog");
        let out = dir.path().join("out");
        std::fs::create_dir_all(&out).unwrap();

        write_log(
            &log,
            &[
                hello(),
                StreamFrame::SegmentStart(SegmentStart {
                    segment_id: 5,
                    base_sample_index: 0,
                    base_device_ts_us: 1_000_000,
                    preroll_samples: 160,
                }),
                audio(5, 0, 320),
                StreamFrame::Telemetry(Telemetry {
                    device_ts_us: 1_020_000,
                    kind: TelemetryKind::Azimuths {
                        values: [0.5, f32::NAN, 0.25, 0.75],
                    },
                }),
                audio(5, 320, 320),
                StreamFrame::SegmentEnd(SegmentEnd {
                    segment_id: 5,
                    device_ts_us: 1_040_000,
                    frames_sent: 2,
                    samples_sent: 640,
                    reason: EndReason::VadRelease,
                }),
            ],
        );

        let outcome = export_log(
            &log,
            &out,
            ResumeLedger::shared(),
            AssemblerLimits::default(),
        )
        .unwrap();
        assert!(matches!(outcome, LogOutcome::Exported(1)));

        // Exactly one wav and one json in the out dir.
        let wavs: Vec<_> = std::fs::read_dir(&out)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|x| x == "wav"))
            .collect();
        assert_eq!(wavs.len(), 1);

        // The wav carries exactly the 640 accumulated samples, values intact.
        let reader = hound::WavReader::open(&wavs[0]).unwrap();
        let spec = reader.spec();
        assert_eq!(spec.channels, 1);
        assert_eq!(spec.sample_rate, 16_000);
        assert_eq!(spec.bits_per_sample, 16);
        let samples: Vec<i16> = reader.into_samples::<i16>().map(|s| s.unwrap()).collect();
        assert_eq!(samples.len(), 640);
        assert_eq!(samples[0], 0);
        assert_eq!(samples[319], 319);
        assert_eq!(samples[320], 0); // second frame restarts the ramp

        // The sidecar carries the DoA track (NaN preserved) and end info.
        let json_path = wavs[0].with_extension("json");
        let meta: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&json_path).unwrap()).unwrap();
        assert_eq!(meta["segment_id"], 5);
        assert_eq!(meta["samples"], 640);
        assert_eq!(meta["end"]["cause"], "vad_release");
        assert_eq!(meta["end"]["truncated"], false);
        let doa = meta["doa"].as_array().expect("doa track");
        assert_eq!(doa.len(), 1);
        assert_eq!(doa[0][0], 320); // 20 ms at 16 kHz
        assert_eq!(doa[0][1][0], 0.5);
        assert!(doa[0][1][1].is_null(), "NaN serializes as JSON null");
    }

    #[test]
    fn open_at_eof_segment_exports_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("cut.framelog");
        let out = dir.path().join("out");
        std::fs::create_dir_all(&out).unwrap();

        // A segment that never receives its SegmentEnd: clean EOF finalizes it
        // as truncated.
        write_log(
            &log,
            &[
                hello(),
                StreamFrame::SegmentStart(SegmentStart {
                    segment_id: 1,
                    base_sample_index: 0,
                    base_device_ts_us: 0,
                    preroll_samples: 0,
                }),
                audio(1, 0, 160),
            ],
        );

        let outcome = export_log(
            &log,
            &out,
            ResumeLedger::shared(),
            AssemblerLimits::default(),
        )
        .unwrap();
        assert!(matches!(outcome, LogOutcome::Exported(1)));

        let json_path = std::fs::read_dir(&out)
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.extension().is_some_and(|x| x == "json"))
            .expect("a sidecar");
        let meta: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&json_path).unwrap()).unwrap();
        assert_eq!(meta["end"]["truncated"], true);
        assert_eq!(meta["end"]["cause"], "truncated");
        assert_eq!(meta["samples"], 160);
    }

    #[test]
    fn resume_across_logs_shares_the_ledger() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        std::fs::create_dir_all(&out).unwrap();
        let log_a = dir.path().join("a.framelog");
        let log_b = dir.path().join("b.framelog");

        // Log A: open segment 9, some audio, then cut off (no SegmentEnd).
        write_log_at(
            &log_a,
            1_700_000_000_000_000,
            &[
                hello(),
                StreamFrame::SegmentStart(SegmentStart {
                    segment_id: 9,
                    base_sample_index: 0,
                    base_device_ts_us: 0,
                    preroll_samples: 0,
                }),
                audio(9, 0, 160),
            ],
        );
        // Log B: resume segment 9 and complete it (a later wall-clock instant, so
        // its output file name differs from A's truncated tail).
        write_log_at(
            &log_b,
            1_700_000_060_000_000,
            &[
                hello(),
                StreamFrame::SegmentStart(SegmentStart {
                    segment_id: 9,
                    base_sample_index: 160,
                    base_device_ts_us: 0,
                    preroll_samples: 0,
                }),
                audio(9, 160, 160),
                StreamFrame::SegmentEnd(SegmentEnd {
                    segment_id: 9,
                    device_ts_us: 0,
                    frames_sent: 2,
                    samples_sent: 320,
                    reason: EndReason::VadRelease,
                }),
            ],
        );

        let ledger = ResumeLedger::shared();
        let limits = AssemblerLimits::default();
        // A truncates segment 9 into the shared ledger.
        assert!(matches!(
            export_log(&log_a, &out, ledger.clone(), limits).unwrap(),
            LogOutcome::Exported(1)
        ));
        // B's SegmentStart(9) hits the ledger → resume; its completed close
        // carries a SkippedResume cross-check (not a Mismatch).
        assert!(matches!(
            export_log(&log_b, &out, ledger, limits).unwrap(),
            LogOutcome::Exported(1)
        ));

        // Find B's resumed sidecar: the one with resumed = true.
        let resumed = std::fs::read_dir(&out)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|x| x == "json"))
            .map(|p| {
                serde_json::from_slice::<serde_json::Value>(&std::fs::read(&p).unwrap()).unwrap()
            })
            .find(|m| m["end"]["resumed"] == true)
            .expect("a resumed segment");
        assert_eq!(resumed["end"]["cross_check"], "skipped_resume");
    }

    #[test]
    fn missing_input_is_pruned_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        std::fs::create_dir_all(&out).unwrap();
        let absent = dir.path().join("nope.framelog");

        let outcome = export_log(
            &absent,
            &out,
            ResumeLedger::shared(),
            AssemblerLimits::default(),
        )
        .unwrap();
        assert!(matches!(outcome, LogOutcome::Missing));
    }
}
