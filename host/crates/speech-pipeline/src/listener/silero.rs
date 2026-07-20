//! `SileroVad`: the host endpointer's speech-vs-noise classifier.
//!
//! Silero VAD runs on the live audio the device streams, emitting P(speech) per
//! 512-sample (32 ms at 16 kHz) chunk. Its raison d'être is exactly the axis the
//! device's energy VAD fails on — music and noise held the device VAD open
//! through lyrics; Silero classifies those as non-speech. The endpointer
//! ([`super::endpointer`]) runs its hangover state machine over these
//! probabilities.
//!
//! Same session/state split as [`OwwModels`](super::oww_stream::OwwModels): the
//! ONNX session is loaded once and shared across pods ([`SileroModel`]); only the
//! recurrent LSTM state tensor and the carried context are per-pod
//! ([`SileroVad`]). One owner (the listener thread) drives every pod's stream
//! serially through the shared model.
//!
//! The v5 invocation contract has one trap worth stating up front: the model is
//! fed [`SILERO_CONTEXT`] + [`SILERO_CHUNK`] = 576 samples per step, not the 512
//! it advances by. Feeding the bare chunk makes it score speech at P≈0.003 —
//! confident, stable, and wrong — and nothing complains, because the graph's
//! `input` dims are dynamic.

use std::path::PathBuf;

use ort::session::Session;
use ort::value::Tensor;

use super::ort_util::load_session;
use crate::wake::WakeError;

/// Audio samples per Silero chunk (32 ms at 16 kHz). This is the *hop*: the
/// caller feeds exactly this many new samples per step; other lengths are an
/// error.
pub const SILERO_CHUNK: usize = 512;
/// Samples of the previous chunk carried in front of the current one. The v5
/// model is fed `SILERO_CONTEXT + SILERO_CHUNK` samples per step even though it
/// advances by `SILERO_CHUNK`; without the carried context it scores speech as
/// confident non-speech. The graph's `input` dims are dynamic, so a
/// context-less tensor is accepted silently rather than rejected.
pub const SILERO_CONTEXT: usize = 64;
/// Audio sample rate the model is fed, in Hz.
pub const SILERO_SAMPLE_RATE: i64 = 16_000;
/// Flattened length of the recurrent state tensor (`[2, 1, 128]`).
pub const SILERO_STATE_LEN: usize = 2 * 128;

/// Path to the committed Silero VAD model. Built by the server from the
/// `[endpointer]` config table; `speech-pipeline` stays free of the surface
/// crate's config types (mirrors [`OwwConfig`](super::oww_stream::OwwConfig)).
#[derive(Debug, Clone)]
pub struct SileroConfig {
    pub model: PathBuf,
}

/// The Silero VAD ONNX session, loaded once and shared across pods. The recurrent
/// state that makes classification stateful lives in [`SileroVad`], so one
/// `SileroModel` serves every pod on the listener thread.
pub struct SileroModel {
    session: Session,
}

impl SileroModel {
    /// Load the model. Fails with a [`WakeError::Load`] naming the file and the
    /// underlying `ort` reason — fatal at startup, never a silently-degraded
    /// endpointer.
    pub fn load(config: &SileroConfig) -> Result<SileroModel, WakeError> {
        Ok(SileroModel {
            session: load_session(&config.model)?,
        })
    }

    /// Run one `SILERO_CONTEXT + SILERO_CHUNK` sample buffer through the model
    /// given the current recurrent state, returning `(P(speech), next_state)`.
    /// The caller stores `next_state` back into the per-pod [`SileroVad`].
    fn run(&mut self, samples: &[f32], state: &[f32]) -> Result<(f32, Vec<f32>), WakeError> {
        let input = Tensor::from_array((
            vec![1_i64, (SILERO_CONTEXT + SILERO_CHUNK) as i64],
            samples.to_vec(),
        ))
        .map_err(|e| inference("input", e))?;
        let state_tensor = Tensor::from_array((vec![2_i64, 1, 128], state.to_vec()))
            .map_err(|e| inference("state", e))?;
        let sr = Tensor::from_array((Vec::<i64>::new(), vec![SILERO_SAMPLE_RATE]))
            .map_err(|e| inference("sr", e))?;

        let outputs = self
            .session
            .run(ort::inputs!["input" => input, "state" => state_tensor, "sr" => sr])
            .map_err(|e| inference("run", e))?;

        // Fallible name lookup: `Index` panics on a missing output name (a model
        // whose outputs are named differently), which would take down the shared
        // listener thread. Fail closed to an inference error instead.
        let (_shape, prob) = outputs
            .get("output")
            .ok_or_else(|| inference("output", "model has no output named `output`"))?
            .try_extract_tensor::<f32>()
            .map_err(|e| inference("output", e))?;
        let Some(&p) = prob.first() else {
            return Err(inference(
                "output",
                "model produced an empty probability tensor",
            ));
        };
        if !p.is_finite() {
            return Err(WakeError::NonFiniteScore);
        }

        let (_shape, next) = outputs
            .get("stateN")
            .ok_or_else(|| inference("stateN", "model has no output named `stateN`"))?
            .try_extract_tensor::<f32>()
            .map_err(|e| inference("stateN", e))?;
        if next.len() != SILERO_STATE_LEN {
            return Err(inference(
                "stateN",
                format!(
                    "state tensor length {} is not the expected {SILERO_STATE_LEN}",
                    next.len()
                ),
            ));
        }
        Ok((p, next.to_vec()))
    }
}

/// One pod's Silero recurrent state. Cold-starts from zeros; drive it with
/// [`push`](SileroVad::push) as 512-sample chunks arrive and [`reset`](SileroVad::reset)
/// on a discontinuity so classification never carries state across a hole.
pub struct SileroVad {
    state: Vec<f32>,
    /// Trailing [`SILERO_CONTEXT`] samples of the previous chunk, prepended to
    /// the next one. Zeros at cold start.
    context: Vec<f32>,
}

impl Default for SileroVad {
    fn default() -> SileroVad {
        SileroVad::new()
    }
}

impl SileroVad {
    /// A fresh classifier with zeroed recurrent state and context.
    pub fn new() -> SileroVad {
        SileroVad {
            state: vec![0.0; SILERO_STATE_LEN],
            context: vec![0.0; SILERO_CONTEXT],
        }
    }

    /// Clear the recurrent state and the carried context back to cold-start.
    /// Called on a pod reconnect or a sample-index discontinuity: the context is
    /// audio from before the hole, so carrying it across would prepend unrelated
    /// samples to the first chunk after it.
    pub fn reset(&mut self) {
        self.state.iter_mut().for_each(|s| *s = 0.0);
        self.context.iter_mut().for_each(|s| *s = 0.0);
    }

    /// Classify one 512-sample chunk, advancing the recurrent state, and return
    /// P(speech). The chunk must be exactly [`SILERO_CHUNK`] samples; a shorter
    /// tail is the caller's to zero-pad. Samples are normalized to `[-1, 1]`
    /// (Silero, unlike openWakeWord, expects normalized input) and the previous
    /// chunk's trailing [`SILERO_CONTEXT`] samples are prepended before
    /// inference — the model reads `SILERO_CONTEXT + SILERO_CHUNK` samples per
    /// step while advancing by `SILERO_CHUNK`.
    pub fn push(&mut self, model: &mut SileroModel, chunk: &[i16]) -> Result<f32, WakeError> {
        if chunk.len() != SILERO_CHUNK {
            return Err(inference(
                "input",
                format!(
                    "chunk length {} is not the required {SILERO_CHUNK} samples",
                    chunk.len()
                ),
            ));
        }
        let mut samples: Vec<f32> = Vec::with_capacity(SILERO_CONTEXT + SILERO_CHUNK);
        samples.extend_from_slice(&self.context);
        samples.extend(chunk.iter().map(|&s| f32::from(s) / 32_768.0));

        let (p, next) = model.run(&samples, &self.state)?;
        self.state = next;
        // Carry this chunk's tail; an inference failure above leaves the context
        // untouched alongside the state, so the pair stays consistent.
        self.context
            .copy_from_slice(&samples[samples.len() - SILERO_CONTEXT..]);
        Ok(p)
    }
}

/// Map a runtime `ort` failure — or an unexpected output shape — to
/// [`WakeError::Inference`], tagged with the stage that produced it. A shape
/// surprise fails closed rather than panicking the listener thread.
fn inference(stage: &str, e: impl std::fmt::Display) -> WakeError {
    WakeError::Inference(format!("silero {stage}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::listener::endpointer::EndpointerConfig;
    use crate::test_support::wake_phrase_pcm;

    /// The endpointer's shipped onset predicate, read from its own config rather
    /// than restated — a threshold change must move this test with it, not leave
    /// it pinning a number the FSM no longer uses.
    fn onset_predicate() -> (f32, usize) {
        let config = EndpointerConfig::default();
        (config.onset_thresh, config.onset_chunks as usize)
    }

    fn model_path() -> PathBuf {
        // `speech-pipeline` crate dir → `host/crates/speech-pipeline`; the model
        // lives at `host/models/silero`.
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../models/silero/silero_vad.onnx")
    }

    fn test_model() -> SileroModel {
        SileroModel::load(&SileroConfig {
            model: model_path(),
        })
        .expect("load committed silero model")
    }

    /// A 440 Hz sine at speech-band amplitude — deterministic, no fixture, and
    /// enough tonal energy to distinguish from digital silence.
    fn tone(chunks: usize) -> Vec<i16> {
        let n = chunks * SILERO_CHUNK;
        (0..n)
            .map(|i| {
                let t = i as f32 / SILERO_SAMPLE_RATE as f32;
                (8000.0 * (2.0 * std::f32::consts::PI * 440.0 * t).sin()) as i16
            })
            .collect()
    }

    /// Round-trip pins the model's I/O contract against the committed file:
    /// `push` returns a finite probability in `[0, 1]` and advances the recurrent
    /// state (a shape or name mismatch surfaces here as a load/inference error,
    /// not a silent wrong answer).
    #[test]
    fn push_returns_probability_and_advances_state() {
        let mut model = test_model();
        let mut vad = SileroVad::new();
        let audio = tone(10);
        let before = vad.state.clone();
        for chunk in audio.chunks(SILERO_CHUNK) {
            let p = vad.push(&mut model, chunk).unwrap();
            assert!(p.is_finite(), "P(speech) must be finite, got {p}");
            assert!((0.0..=1.0).contains(&p), "P(speech) {p} out of range");
        }
        assert_ne!(vad.state, before, "recurrent state must advance");
    }

    /// Silence classifies well below any reasonable onset threshold — the model
    /// is doing real speech discrimination, not returning a constant.
    #[test]
    fn silence_scores_low() {
        let mut model = test_model();
        let mut vad = SileroVad::new();
        let mut last = 1.0;
        for chunk in vec![0_i16; 10 * SILERO_CHUNK].chunks(SILERO_CHUNK) {
            last = vad.push(&mut model, chunk).unwrap();
        }
        assert!(last < 0.5, "digital silence should score low, got {last}");
    }

    /// The property everything downstream rests on: speech scores high. The
    /// endpointer's onset predicate is not "P peaks above threshold once" but
    /// "`onset_chunks` consecutive chunks clear `onset_thresh`", so that is what
    /// this asserts — a model-file swap or a preprocessing regression (the
    /// `/32_768.0` normalization above) fails here rather than manifesting as a
    /// silent room. Paired with `silence_scores_low`, the two pin the
    /// discrimination in both directions.
    #[test]
    fn speech_scores_high_enough_to_onset() {
        let (thresh, onset_chunks) = onset_predicate();
        let mut model = test_model();
        let mut vad = SileroVad::new();
        // Whole chunks only — production re-blocks and carries the sub-chunk tail.
        let scores: Vec<f32> = wake_phrase_pcm()
            .chunks_exact(SILERO_CHUNK)
            .map(|chunk| vad.push(&mut model, chunk).unwrap())
            .collect();

        let max = scores.iter().copied().fold(f32::MIN, f32::max);
        assert!(
            max >= thresh,
            "speech must reach the onset threshold: max P {max} < {thresh}\nscores: {scores:?}"
        );

        let run = scores
            .split(|&p| p < thresh)
            .map(<[f32]>::len)
            .max()
            .unwrap_or(0);
        assert!(
            run >= onset_chunks,
            "speech must hold above the onset threshold for {onset_chunks} consecutive \
             chunks (the endpointer's actual onset predicate); longest run was {run}\n\
             scores: {scores:?}"
        );
    }

    /// The normalized trailing [`SILERO_CONTEXT`] samples of `chunk` — what the
    /// next step must prepend.
    fn normalized_tail(chunk: &[i16]) -> Vec<f32> {
        chunk[SILERO_CHUNK - SILERO_CONTEXT..]
            .iter()
            .map(|&s| f32::from(s) / 32_768.0)
            .collect()
    }

    /// The context is the *previous chunk's tail* — the one item the local model
    /// README got wrong and the sole divergence from the reference contract. The
    /// coarse mutations are already caught (no context at all fails
    /// `speech_scores_high_enough_to_onset`; a never-populated one fails
    /// `reset_restores_cold_start`), but a mis-offset — the buffer's head, the
    /// chunk's head, a stale tail — leaves both green while feeding the model
    /// misaligned audio. So pin the content directly rather than through a
    /// threshold proxy on one fixture.
    #[test]
    fn context_carries_the_previous_chunks_tail() {
        let mut model = test_model();
        let mut vad = SileroVad::new();
        let audio = tone(2);
        let (first, second) = audio.split_at(SILERO_CHUNK);
        assert_ne!(
            normalized_tail(first),
            normalized_tail(second),
            "the two tails must differ, or tracking cannot be observed"
        );

        vad.push(&mut model, first).unwrap();
        assert_eq!(
            vad.context,
            normalized_tail(first),
            "context is the chunk's trailing {SILERO_CONTEXT} samples, normalized"
        );

        vad.push(&mut model, second).unwrap();
        assert_eq!(
            vad.context,
            normalized_tail(second),
            "and tracks the latest chunk rather than sticking at the first"
        );
    }

    /// `reset` returns the recurrent state *and* the carried context to
    /// cold-start, so a reused classifier scores a stream bit-identically to a
    /// fresh one — neither the state nor the previous chunk's tail bleeds across
    /// a discontinuity. The priming feed is non-silent so both would differ from
    /// zeros if either were left behind.
    #[test]
    fn reset_restores_cold_start() {
        let mut model = test_model();
        let a = tone(3);
        let b = tone(4);

        let mut reused = SileroVad::new();
        for chunk in a.chunks(SILERO_CHUNK) {
            reused.push(&mut model, chunk).unwrap();
        }
        assert_ne!(
            reused.context,
            vec![0.0; SILERO_CONTEXT],
            "priming must leave a non-zero context, or this test proves nothing"
        );
        reused.reset();
        let mut reused_scores = Vec::new();
        for chunk in b.chunks(SILERO_CHUNK) {
            reused_scores.push(reused.push(&mut model, chunk).unwrap());
        }

        let mut fresh = SileroVad::new();
        let mut fresh_scores = Vec::new();
        for chunk in b.chunks(SILERO_CHUNK) {
            fresh_scores.push(fresh.push(&mut model, chunk).unwrap());
        }
        assert_eq!(reused_scores, fresh_scores, "reset must clear all state");
    }

    /// A wrong chunk length is a precise error, never a silent misclassification.
    #[test]
    fn wrong_chunk_length_is_an_error() {
        let mut model = test_model();
        let mut vad = SileroVad::new();
        match vad.push(&mut model, &vec![0; SILERO_CHUNK - 1]) {
            Err(WakeError::Inference(msg)) => assert!(msg.contains("511")),
            other => panic!("expected an inference error, got {other:?}"),
        }
    }

    #[test]
    fn missing_model_is_a_load_error() {
        let config = SileroConfig {
            model: model_path().with_file_name("does-not-exist.onnx"),
        };
        match SileroModel::load(&config) {
            Err(WakeError::Load { model, .. }) => assert!(model.contains("does-not-exist.onnx")),
            Err(other) => panic!("expected a load error, got {other:?}"),
            Ok(_) => panic!("expected load failure for a missing model file"),
        }
    }
}
