//! The batch openWakeWord gate: a replay/parity oracle over an assembled
//! `Segment`, derived from the streaming listener core (`listener::oww_stream`).
//!
//! `OwwGate` (`wake::oww`) drives a fresh streaming pass over a whole segment and
//! takes the max-score verdict after a complete real embedding history. A segment
//! too short to build that history produces no score at all. Live wake detection
//! runs in the continuous listener; this gate survives as a replay tool.

pub mod oww;

pub use oww::{OwwConfig, OwwGate};

/// Verdict for one batch-scored segment. Three cases, three variants: the model
/// accepted, the model rejected, or the model never ran.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WakeOutcome {
    /// The gate passed on a score above threshold. Sidecar class `positive`.
    Detected {
        score: f32,
        /// Offset into the segment's `pcm` of the end of the scoring window
        /// that produced the maximum score. The wake model peaks as the phrase
        /// completes, so this approximates the end of the wake phrase — the
        /// point after which the spoken command begins. Clamped to the
        /// unpadded PCM length by the detector; no safety margin applied here
        /// (the consumer subtracts one before cutting).
        wake_end_sample: usize,
    },
    /// The gate dropped the segment on a score below threshold. Sidecar
    /// class `negative`. `score` is a real model output.
    Rejected { score: f32 },
    /// The wake head never ran on this segment, so there is no score to report.
    /// The segment did not wake, but it is not evidence that the audio scores
    /// low: anything reading these verdicts as a score distribution — corpus
    /// scoring, threshold tuning — must exclude this case rather than fold a
    /// stand-in zero into it.
    Unscored { reason: UnscoredReason },
}

/// Why a segment carries no wake score.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnscoredReason {
    /// The segment carried no audio.
    Empty,
    /// The segment was shorter than the wake model's readiness window
    /// ([`WAKE_READINESS_SAMPLES`](crate::listener::oww_stream::WAKE_READINESS_SAMPLES)),
    /// so no embedding history complete enough to score ever formed.
    ShorterThanReadinessWindow,
}

/// A wake-gate failure: model/session load, runtime inference, or a non-finite
/// score. Load failures are fatal at startup; runtime failures fail the segment
/// closed (see the pipeline's error handling).
#[derive(Debug, thiserror::Error)]
pub enum WakeError {
    /// A model or ONNX session failed to load. Names the offending file and the
    /// underlying reason so startup failure is diagnosable.
    #[error("failed to load wake model {model}: {detail}")]
    Load { model: String, detail: String },
    /// Inference failed at runtime for one segment.
    #[error("wake inference failed: {0}")]
    Inference(String),
    /// The detector produced a non-finite (e.g. NaN) score, treated as an error
    /// rather than silently comparing false against the threshold.
    #[error("wake detector produced a non-finite score")]
    NonFiniteScore,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wake_error_display_names_the_cause() {
        let load = WakeError::Load {
            model: "melspectrogram.onnx".to_string(),
            detail: "no such file".to_string(),
        };
        assert!(load.to_string().contains("melspectrogram.onnx"));
        assert!(load.to_string().contains("no such file"));
        assert!(
            WakeError::Inference("session run".to_string())
                .to_string()
                .contains("session run")
        );
        assert!(WakeError::NonFiniteScore.to_string().contains("non-finite"));
    }
}
