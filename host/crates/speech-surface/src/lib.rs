//! `speech-surface`: the daemon and offline tools over the `pod-ingest` and
//! `speech-pipeline` libraries. This library crate holds the I/O and wiring —
//! config, server, recorder, pruning, JSONL sink, and the pipeline task — that
//! the binaries assemble.

mod barge;
pub mod clip;
pub mod config;
mod console;
pub mod exit;
pub mod jsonl;
pub mod pipeline;
pub mod playback_router;
pub mod prune;
pub mod psk;
pub mod recorder;
pub mod replay;
pub mod server;

pub use clip::{check_spine_format, load_clip, ClipError, SpineFormatViolation};
pub use config::{
    Config, ConfigError, JsonlConfig, JsonlSink, PipelineConfig, PodConfig, RecordConfig,
    RoomLookup, UNMAPPED_ROOM,
};
pub use jsonl::{emit_line, format_line, JsonlHandle};
pub use prune::{prune, PruneFailure, PruneHalt, PruneOutcome, PruneRequest, PruneTier, PrunedLog};
pub use recorder::{
    iso8601_ms, sanitize_filename, set_pinned, sidecar_path, Sidecar, SidecarError, SidecarSegment,
    WakeClass,
};
pub use replay::{replay_framelog, ReplayError, ReplayListener, ReplaySummary, StopReason};

/// Shared `Segment` builder for the crate's test modules. `Segment` grows a
/// field per increment (`StageTimings` especially), so keeping one constructor
/// means a new field is added in one place, not once per test module.
#[cfg(test)]
pub(crate) mod test_support {
    use std::path::Path;

    use pod_ingest::{DeviceMicros, HostMicros, SegmentRef};
    use speech_pipeline::{PodId, RoomId, Segment, SegmentEndInfo, SegmentTelemetry, StageTimings};

    /// Write `samples` as a `SPINE_FORMAT` (16 kHz mono S16) `.wav` at `path`.
    /// Delegates to the production writer (`speech_pipeline::write_spine_wav`)
    /// so the crate's tests exercise the same on-disk format the outbound chain
    /// writes, not a hand-rolled twin of it.
    pub(crate) fn write_spine_wav(path: &Path, samples: &[i16]) {
        speech_pipeline::write_spine_wav(path, samples).unwrap();
    }

    /// A `Segment` for `pod-x` in `kitchen` with the given id, PCM length,
    /// telemetry, and end info. Timings are fixed sentinel stamps
    /// (`assembled` just after `segment_end_rx`); `tracking_emitted` is unset so
    /// the pipeline task stamps it live.
    pub(crate) fn segment(
        segment_id: u32,
        samples: usize,
        telemetry: Vec<SegmentTelemetry>,
        end: SegmentEndInfo,
    ) -> Segment {
        Segment {
            pod: PodId("pod-x".into()),
            room: RoomId("kitchen".into()),
            segment_id,
            base_sample_index: 0,
            preroll_samples: 0,
            pcm: vec![0i16; samples],
            device_ts: DeviceMicros(0),
            host_rx: HostMicros(1_000),
            end,
            telemetry,
            audio_ref: SegmentRef {
                log: "pod-x_0.framelog".into(),
                segment_id,
                part: 0,
            },
            timings: StageTimings {
                first_frame_rx: Some(HostMicros(1_000)),
                segment_end_rx: Some(HostMicros(5_000)),
                assembled: Some(HostMicros(5_001)),
                tracking_emitted: None,
                ..StageTimings::default()
            },
        }
    }
}
