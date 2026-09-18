//! `OwwGate`: the batch openWakeWord wake gate, now a thin wrapper over the
//! streaming core ([`crate::listener::oww_stream`]).
//!
//! Segment-batch scoring feeds a fresh [`OwwStream`] chunk-by-chunk and compares
//! the maximum per-step sigmoid score with the threshold. The stream suppresses
//! its warm-up chunks, so a segment shorter than the readiness window comes back
//! [`WakeOutcome::Unscored`] rather than carrying an invented score.
//! Fresh state per call means no bleed across segments or pods.
//!
//! `OwwGate` is retired once the pipeline rework routes wake through the listener
//! thread; until then it keeps the segment-shaped [`WakeGate`] seam working.
//! Nothing outside this module reads its verdicts today:
//! TODO(batch-wake-gate-fate) — name the consumer or delete the gate.

use super::{UnscoredReason, WakeError, WakeOutcome};
use crate::listener::oww_stream::{OwwModels, ScoredChunk};
use crate::types::Segment;

pub use crate::listener::oww_stream::OwwConfig;

/// The openWakeWord gate: the three ONNX sessions loaded once at construction,
/// run per segment on the dedicated wake thread through a fresh streaming pass.
pub struct OwwGate {
    models: OwwModels,
    threshold: f32,
}

impl OwwGate {
    /// Load all three models. Fails with a precise [`WakeError::Load`] naming the
    /// offending file — the daemon treats this as fatal at startup.
    pub fn load(config: &OwwConfig) -> Result<OwwGate, WakeError> {
        Ok(OwwGate {
            models: OwwModels::load(config)?,
            threshold: config.threshold,
        })
    }

    /// Batch-score a whole segment through a fresh streaming pass, returning the
    /// max-score verdict. The replay/parity entry point.
    pub fn gate(&mut self, seg: &Segment) -> Result<WakeOutcome, WakeError> {
        // An empty segment has nothing to score.
        if seg.pcm.is_empty() {
            return Ok(WakeOutcome::Unscored {
                reason: UnscoredReason::Empty,
            });
        }

        // Fresh streaming state per segment: no bleed across segments or pods.
        let mut stream = crate::listener::OwwStream::new(self.threshold);
        let mut best: Option<ScoredChunk> = None;
        let keep_best = |best: &mut Option<ScoredChunk>, sc: ScoredChunk| {
            if best.is_none_or(|b| sc.score > b.score) {
                *best = Some(sc);
            }
        };
        for sc in stream.push(&mut self.models, &seg.pcm)?.scored {
            keep_best(&mut best, sc);
        }
        for sc in stream.flush(&mut self.models)?.scored {
            keep_best(&mut best, sc);
        }
        // A segment too short to outlast the stream's warm-up cannot be scored.
        let Some(best) = best else {
            return Ok(WakeOutcome::Unscored {
                reason: UnscoredReason::ShorterThanReadinessWindow,
            });
        };
        Ok(if best.score > self.threshold {
            WakeOutcome::Detected {
                score: best.score,
                // `end_sample` is the offset into the segment PCM at the end of
                // the scoring window; clamp a zero-padded tail to the real audio.
                wake_end_sample: (best.end_sample as usize).min(seg.pcm.len()),
            }
        } else {
            WakeOutcome::Rejected { score: best.score }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{oww_config, oww_model_dir, seeded_noise, wake_phrase_pcm};
    use crate::types::{Segment, test_segment};

    fn test_gate_with_threshold(threshold: f32) -> OwwGate {
        OwwGate::load(&oww_config(threshold)).expect("load committed models")
    }

    fn test_gate() -> OwwGate {
        test_gate_with_threshold(0.5)
    }

    fn seg_with_pcm(pcm: Vec<i16>) -> Segment {
        let mut seg = test_segment(vec![]);
        seg.pcm = pcm;
        seg
    }

    #[test]
    fn noise_scores_finite_and_below_threshold() {
        let mut gate = test_gate();
        let outcome = gate.gate(&seg_with_pcm(seeded_noise(1, 32_000))).unwrap();
        match outcome {
            WakeOutcome::Rejected { score } => {
                assert!(score.is_finite(), "score must be finite, got {score}");
                assert!(
                    (0.0..=1.0).contains(&score),
                    "sigmoid score out of range: {score}"
                );
                assert!(score <= 0.5, "noise must not exceed threshold, got {score}");
            }
            other => panic!("noise must be rejected, got {other:?}"),
        }
    }

    #[test]
    fn state_resets_between_segments() {
        let mut gate = test_gate();
        let a = seg_with_pcm(seeded_noise(1, 24_000));
        let b = seg_with_pcm(seeded_noise(2, 24_000));
        // Fresh streaming state per gate call ⇒ B scores identically whether or
        // not A ran first: no bleed.
        let _ = gate.gate(&a).unwrap();
        let after_a = gate.gate(&b).unwrap();
        let fresh = test_gate().gate(&b).unwrap();
        assert_eq!(
            after_a, fresh,
            "per-segment state must not carry across gate calls"
        );
    }

    #[test]
    fn sub_chunk_segment_is_unscored_not_a_zero() {
        let mut gate = test_gate();
        // A segment inside the warm-up never reaches the wake head, and the
        // verdict says so rather than reporting a score nothing produced.
        let outcome = gate.gate(&seg_with_pcm(seeded_noise(3, 100))).unwrap();
        assert_eq!(
            outcome,
            WakeOutcome::Unscored {
                reason: UnscoredReason::ShorterThanReadinessWindow
            }
        );
    }

    #[test]
    fn sub_chunk_does_not_detect_with_negative_threshold() {
        // A negative threshold cannot bypass the warm-up.
        let mut gate = test_gate_with_threshold(-1.0);
        assert_eq!(
            gate.gate(&seg_with_pcm(seeded_noise(3, 100))).unwrap(),
            WakeOutcome::Unscored {
                reason: UnscoredReason::ShorterThanReadinessWindow
            }
        );
    }

    /// The readiness boundary through the batch surface: a segment exactly as
    /// long as the warm-up carries no score, and one chunk more carries a real
    /// one.
    #[test]
    fn the_warmup_boundary_decides_scored_from_unscored() {
        use crate::listener::oww_stream::{CHUNK, WARMUP_CHUNKS};

        let warmup = WARMUP_CHUNKS as usize;
        let mut gate = test_gate();
        assert_eq!(
            gate.gate(&seg_with_pcm(seeded_noise(4, warmup * CHUNK)))
                .unwrap(),
            WakeOutcome::Unscored {
                reason: UnscoredReason::ShorterThanReadinessWindow
            }
        );
        match gate
            .gate(&seg_with_pcm(seeded_noise(4, (warmup + 1) * CHUNK)))
            .unwrap()
        {
            WakeOutcome::Rejected { score } => assert!(
                score.is_finite() && score <= 0.5,
                "noise past the warm-up scores, low: {score}"
            ),
            other => panic!("one chunk past the warm-up must be scored, got {other:?}"),
        }
    }

    #[test]
    fn empty_segment_is_unscored_without_panic() {
        let mut gate = test_gate();
        assert_eq!(
            gate.gate(&seg_with_pcm(vec![])).unwrap(),
            WakeOutcome::Unscored {
                reason: UnscoredReason::Empty
            }
        );
    }

    #[test]
    fn wake_phrase_fixture_scores_above_threshold() {
        // The empirical pin on the openWakeWord constants and the committed
        // "Hey Jarvis" fixture: it must wake the stock model at 0.5 through the
        // streaming-derived batch path.
        let mut gate = test_gate();
        let pcm = wake_phrase_pcm();
        let pcm_len = pcm.len();
        match gate.gate(&seg_with_pcm(pcm)).unwrap() {
            WakeOutcome::Detected {
                score,
                wake_end_sample,
            } => {
                assert!(score.is_finite(), "score must be finite, got {score}");
                assert!(
                    score > 0.5,
                    "wake phrase must exceed threshold, got {score}"
                );
                assert!(
                    wake_end_sample <= pcm_len,
                    "wake_end_sample {wake_end_sample} exceeds pcm len {pcm_len}"
                );
                assert!(
                    wake_end_sample > 0,
                    "wake_end_sample must be past the segment start"
                );
            }
            other => panic!("wake phrase must be detected, got {other:?}"),
        }
    }

    #[test]
    fn missing_model_is_a_load_error() {
        let mut config = oww_config(0.5);
        config.melspectrogram = oww_model_dir().join("does-not-exist.onnx");
        let result = OwwGate::load(&config);
        match result {
            Err(WakeError::Load { model, .. }) => assert!(model.contains("does-not-exist.onnx")),
            Err(other) => panic!("expected Load error, got {other:?}"),
            Ok(_) => panic!("expected load failure for a missing model file"),
        }
    }
}
