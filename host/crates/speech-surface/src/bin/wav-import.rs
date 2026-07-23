//! `wav-import`: turn a 16 kHz mono S16 `.wav` into a replayable `.framelog` —
//! the inverse of `segments-export`.
//!
//! It reads the clip, synthesizes the wire frames one VAD segment would carry
//! (`pod_ingest::synth_session` — `Hello` + `SegmentStart` + paced `Audio` +
//! `SegmentEnd`), and writes them through the real `FrameLogWriter`. The result
//! replays through the daemon's live accept/ingest path exactly as a captured
//! log would, so any laptop-recorded audio becomes a no-hardware test/tuning
//! input. Output is deterministic for a fixed clip and metadata.
//!
//! Only the spine's format is accepted — 16 kHz mono S16; any other `.wav` is a
//! precise error, never silently reinterpreted.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result, anyhow, bail};
use audio_pipeline::wire::{ChannelSource, MAX_FRAME_BYTES, encode_frame};
use clap::Parser;
use serde_json::json;

use pod_ingest::{FrameLogWriter, HostMicros, LogMeta, SynthParams, synth_session};
use speech_pipeline::SPINE_FORMAT;
use speech_surface::{check_spine_format, emit_line as emit};

const BUILD_ID: &str = concat!("wav-import/", env!("CARGO_PKG_VERSION"));

#[derive(Parser)]
#[command(
    name = "wav-import",
    about = "Convert a 16 kHz mono S16 .wav into a replayable .framelog"
)]
struct Cli {
    /// Input `.wav` — must be 16 kHz mono S16 PCM.
    #[arg(long)]
    input: PathBuf,
    /// Output frame-log path. Must not already exist (an existing capture is
    /// never overwritten).
    #[arg(long)]
    output: PathBuf,
    /// Pod identity carried in the synthesized `Hello` (≤ 32 bytes).
    #[arg(long, default_value = "wav-import")]
    pod_id: String,
    /// Segment counter for the synthesized segment.
    #[arg(long, default_value_t = 0)]
    segment_id: u32,
    /// Leading samples marked as preroll (predating VAD onset). Must not exceed
    /// the clip length.
    #[arg(long, default_value_t = 0)]
    preroll_samples: u32,
    /// Absolute sample index (since capture start) of the first sample.
    #[arg(long, default_value_t = 0)]
    base_sample_index: u64,
    /// Base host-clock epoch (µs since UNIX epoch) added to each frame's paced
    /// offset and recorded as the log's creation time. Defaults to 0 so output
    /// is byte-deterministic; pass a real wall-clock for a lifelike capture.
    #[arg(long, default_value_t = 0)]
    base_epoch_us: u64,
    /// Per-process connection sequence recorded in the log header.
    #[arg(long, default_value_t = 1)]
    conn_seq: u64,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match import(&cli) {
        Ok(frames) => {
            emit(
                "wav_imported",
                json!({
                    "input": cli.input.display().to_string(),
                    "output": cli.output.display().to_string(),
                    "pod_id": cli.pod_id,
                    "segment_id": cli.segment_id,
                    "frames": frames,
                }),
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            emit(
                "import_error",
                json!({
                    "input": cli.input.display().to_string(),
                    "error": format!("{e:#}"),
                }),
            );
            ExitCode::FAILURE
        }
    }
}

/// Read the clip, synthesize its session frames, and write the frame log.
/// Returns the number of frames written.
fn import(cli: &Cli) -> Result<usize> {
    let pcm = read_wav(&cli.input)?;

    let params = SynthParams {
        pod_id: cli.pod_id.clone(),
        sample_rate_hz: SPINE_FORMAT.sample_rate_hz,
        segment_id: cli.segment_id,
        base_sample_index: cli.base_sample_index,
        base_device_ts_us: 0,
        preroll_samples: cli.preroll_samples,
        channel_source: ChannelSource::AsrBeam,
    };
    let frames = synth_session(&pcm, &params).context("synthesize session frames")?;
    let frame_count = frames.len();

    let meta = LogMeta {
        build_id: BUILD_ID.to_string(),
        created_epoch_us: HostMicros(cli.base_epoch_us),
        conn_seq: cli.conn_seq,
        rolled_from: None,
    };
    let writer = FrameLogWriter::create(&cli.output, meta)
        .with_context(|| format!("create frame log {}", cli.output.display()))?;

    // The log file now exists (create_new proved the path was free). If any
    // append or the final flush fails, remove it: a retry of the same command
    // must not trip the create_new guard, and a torn framelog reads as a valid
    // (but silently truncated) capture — leaving one would defeat the "never
    // overwrite a capture" protection by guarding the wrong artifact.
    let result = (move || -> Result<()> {
        let mut writer = writer;
        let mut buf = [0u8; MAX_FRAME_BYTES + 2];
        for sf in &frames {
            let n = encode_frame(&sf.frame, &mut buf)
                .map_err(|e| anyhow!("encode synthesized frame: {e:?}"))?;
            writer
                .append(
                    HostMicros(cli.base_epoch_us + sf.host_rx_offset_us),
                    &buf[..n],
                )
                .context("append frame to log")?;
        }
        writer.finish().context("finish frame log")
    })();

    if let Err(e) = result {
        let _ = std::fs::remove_file(&cli.output);
        return Err(e);
    }

    Ok(frame_count)
}

/// Read a 16 kHz mono S16 `.wav` into a mono PCM buffer, rejecting any other
/// format with a precise error rather than reinterpreting its bytes.
fn read_wav(path: &Path) -> Result<Vec<i16>> {
    let reader =
        hound::WavReader::open(path).with_context(|| format!("open wav {}", path.display()))?;
    let spec = reader.spec();
    if let Err(violation) = check_spine_format(&spec) {
        bail!("{violation}");
    }
    reader
        .into_samples::<i16>()
        .collect::<Result<Vec<i16>, _>>()
        .context("read wav samples")
}

#[cfg(test)]
mod tests {
    use super::*;
    use audio_pipeline::wire::decode_frame;
    use pod_ingest::{FrameLogReader, LogItem, ResumeLedger, SessionFsm};
    use speech_pipeline::{AssemblerLimits, PodId, RoomId, SegmentAssembler};
    use speech_surface::UNMAPPED_ROOM;

    /// A distinct-valued ramp so a round-tripped PCM buffer is checkable.
    fn ramp(n: usize) -> Vec<i16> {
        (0..n).map(|i| i as i16).collect()
    }

    fn write_wav(path: &Path, pcm: &[i16]) {
        let spec = hound::WavSpec {
            channels: 1,
            sample_rate: SPINE_FORMAT.sample_rate_hz,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };
        let mut w = hound::WavWriter::create(path, spec).unwrap();
        for &s in pcm {
            w.write_sample(s).unwrap();
        }
        w.finalize().unwrap();
    }

    fn cli(input: &Path, output: &Path) -> Cli {
        Cli {
            input: input.to_path_buf(),
            output: output.to_path_buf(),
            pod_id: "pod-wav".into(),
            segment_id: 3,
            preroll_samples: 160,
            base_sample_index: 0,
            base_epoch_us: 0,
            conn_seq: 1,
        }
    }

    #[test]
    fn deterministic_bytes_for_fixed_input() {
        let dir = tempfile::tempdir().unwrap();
        let wav = dir.path().join("in.wav");
        write_wav(&wav, &ramp(1000));

        let out_a = dir.path().join("a.framelog");
        let out_b = dir.path().join("b.framelog");
        import(&cli(&wav, &out_a)).unwrap();
        import(&cli(&wav, &out_b)).unwrap();

        let a = std::fs::read(&out_a).unwrap();
        let b = std::fs::read(&out_b).unwrap();
        assert_eq!(a, b, "same clip + metadata must produce identical bytes");
        assert!(!a.is_empty());
    }

    #[test]
    fn framelog_round_trips_pcm_through_live_ingest() {
        let dir = tempfile::tempdir().unwrap();
        let pcm = ramp(1000);
        let wav = dir.path().join("in.wav");
        write_wav(&wav, &pcm);
        let out = dir.path().join("cap.framelog");
        import(&cli(&wav, &out)).unwrap();

        // Replay the log through the same decode + FSM + assembler path the
        // daemon uses, and assert the assembled segment's PCM equals the input.
        let reader = FrameLogReader::open(&out).unwrap();
        let mut fsm = SessionFsm::new(SPINE_FORMAT, ResumeLedger::shared());
        let mut assembler: Option<SegmentAssembler> = None;
        let mut assembled: Option<Vec<i16>> = None;

        for item in reader {
            let LogItem::Record { host_rx, payload } = item.unwrap() else {
                break;
            };
            let frame = decode_frame(&payload).unwrap();
            for ev in fsm.feed(frame, host_rx) {
                if let pod_ingest::SessionEvent::HelloAccepted { pod_id, .. } = &ev {
                    assembler = Some(SegmentAssembler::new(
                        PodId(pod_id.clone()),
                        RoomId(UNMAPPED_ROOM.to_string()),
                        AssemblerLimits::default(),
                    ));
                }
                if let Some(a) = assembler.as_mut()
                    && let Some(seg) = a.on_event(&ev, "cap.framelog")
                {
                    assembled = Some(seg.pcm);
                }
            }
        }

        assert_eq!(assembled.expect("one assembled segment"), pcm);
    }

    #[test]
    fn rejects_stereo_wav() {
        let dir = tempfile::tempdir().unwrap();
        let wav = dir.path().join("stereo.wav");
        let spec = hound::WavSpec {
            channels: 2,
            sample_rate: SPINE_FORMAT.sample_rate_hz,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };
        let mut w = hound::WavWriter::create(&wav, spec).unwrap();
        for i in 0..100i16 {
            w.write_sample(i).unwrap();
            w.write_sample(i).unwrap();
        }
        w.finalize().unwrap();

        let err = read_wav(&wav).unwrap_err();
        assert!(err.to_string().contains("mono"), "got: {err}");
    }

    #[test]
    fn rejects_wrong_sample_rate() {
        let dir = tempfile::tempdir().unwrap();
        let wav = dir.path().join("48k.wav");
        let spec = hound::WavSpec {
            channels: 1,
            sample_rate: 48_000,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };
        let mut w = hound::WavWriter::create(&wav, spec).unwrap();
        for i in 0..100i16 {
            w.write_sample(i).unwrap();
        }
        w.finalize().unwrap();

        let err = read_wav(&wav).unwrap_err();
        assert!(err.to_string().contains("Hz"), "got: {err}");
    }
}
