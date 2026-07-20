//! The batch openWakeWord gate: a replay/parity oracle over an assembled
//! `Segment`, derived from the streaming listener core (`listener::oww_stream`).
//!
//! `OwwGate` (`wake::oww`) drives a fresh streaming pass over a whole segment and
//! takes the max-score verdict, so batch behaviour is derived-from-streaming and
//! parity holds by construction. Live wake detection runs in the continuous
//! listener; this gate survives only as the replay/parity tool the framelog corpus
//! is scored through.

pub mod oww;

pub use oww::{OwwConfig, OwwGate};

/// Verdict for one batch-scored segment: a scored accept (`positive`) or a scored
/// reject (`negative`). The batch gate is a replay/parity oracle, so it always
/// scores — there is no bypass verdict.
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
    /// class `negative`.
    Rejected { score: f32 },
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
        assert!(WakeError::Inference("session run".to_string())
            .to_string()
            .contains("session run"));
        assert!(WakeError::NonFiniteScore.to_string().contains("non-finite"));
    }
}
