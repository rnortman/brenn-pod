//! `OwwStream`: the streaming openWakeWord core.
//!
//! Where the retired batch [`OwwGate`](crate::wake::OwwGate) ran the whole
//! assembled segment through the mel model in one pass at segment close, the
//! streaming core scores incrementally as audio arrives, carrying persistent
//! per-pod rolling state so wake becomes a live stream event decoupled from
//! segment boundaries. This is the substrate the continuous listener taps.
//!
//! Two pieces, split so the ONNX sessions can be shared across pods while the
//! rolling windows stay per-pod:
//!
//! - [`OwwModels`] owns the three ONNX sessions (mel, embedding, wake). Loaded
//!   once. `run` needs `&mut`, so one owner (the listener thread) drives them
//!   serially for every pod.
//! - [`OwwStream`] holds one pod's rolling state: the raw-PCM mel lookback, the
//!   persistent 76-frame mel window, the 16-embedding window, the frame cursor,
//!   and the wake refractory. It borrows the models per step.
//!
//! **Mel contiguity — the one hard invariant.** The mel model uses valid framing
//! (window [`MEL_STFT_WINDOW`], hop [`SAMPLES_PER_MEL_FRAME`], no edge padding),
//! so `run_mel` over a growing buffer is *prefix-stable*: appending audio only
//! adds trailing frames, never disturbs earlier ones. The streaming core exploits
//! that. Each chunk runs the mel session over (raw-PCM lookback + new chunk) and
//! appends only the frames that are genuinely new — the ones the lookback alone
//! did not already cover ([`mel_frame_count`]). Because the lookback carries the
//! window's left context, those frames have the same framing as a whole-segment
//! pass. Frames then drive one embedding per processed chunk, on the same
//! cadence as the batch path, so batch scoring — reconstructed by feeding a fresh
//! `OwwStream` chunk-by-chunk (the [`OwwGate`](crate::wake::OwwGate) wrapper) —
//! replays the same chunked-stream decisions once its real embedding history is
//! ready. Streamed mel values can differ slightly from a whole-segment pass. The mel window cold-starts from ones and the embedding
//! window from zeros, but the wake model never sees placeholder embeddings. The
//! first chunk yields 5 frames (no left context to fill the window); every chunk
//! after adds exactly 8, with one embedding produced for each chunk.

use std::collections::VecDeque;
use std::path::PathBuf;

use ort::session::Session;
use ort::value::Tensor;

use super::ort_util::load_session;
use crate::wake::WakeError;

/// Mel bins per frame.
pub(crate) const MEL_BINS: usize = 32;
/// Mel frames per embedding-model input window.
pub(crate) const EMB_WINDOW: usize = 76;
/// Dimensions of one embedding.
pub(crate) const EMB_DIM: usize = 96;
/// Embeddings per wake-model input window.
pub(crate) const WAKE_WINDOW: usize = 16;
/// Samples per processing chunk (80 ms at 16 kHz).
pub(crate) const CHUNK: usize = 1280;
/// Mel frames between successive embeddings after the first chunk (one 80 ms
/// chunk of audio). One embedding is produced per processed chunk.
pub(crate) const EMB_STEP: usize = 8;
/// Audio samples one mel frame advances (10 ms at 16 kHz): the mel model's STFT
/// hop.
pub(crate) const SAMPLES_PER_MEL_FRAME: usize = CHUNK / EMB_STEP;
/// The mel model's STFT window, in samples. Empirically pinned against the
/// committed model by `mel_frame_count_matches_model`: `run_mel` uses valid
/// framing, emitting `(n - MEL_STFT_WINDOW) / SAMPLES_PER_MEL_FRAME + 1` frames
/// for `n >= MEL_STFT_WINDOW` and none below. A model change breaks that test.
pub(crate) const MEL_STFT_WINDOW: usize = 640;

/// Raw-PCM samples of lookback prepended to each chunk before the mel pass. This
/// carries the preceding 30 ms of audio used by the streaming mel pipeline.
pub(crate) const MEL_LOOKBACK_SAMPLES: usize = 3 * SAMPLES_PER_MEL_FRAME;

/// Samples after a detection during which further detections are suppressed
/// (~2 s at 16 kHz), so one spoken phrase arms the wake once, not repeatedly.
pub(crate) const REFRACTORY_SAMPLES: u64 = 32_000;
/// Samples by which the threshold-crossing cursor leads phrase completion.
pub(crate) const WAKE_END_LAG_SAMPLES: u64 = CHUNK as u64;

/// Frames `run_mel` emits for `n` raw samples under the model's valid framing.
/// The join between "which frames has the lookback already contributed" and
/// "which are new this chunk".
pub(crate) fn mel_frame_count(n: usize) -> usize {
    if n < MEL_STFT_WINDOW {
        0
    } else {
        (n - MEL_STFT_WINDOW) / SAMPLES_PER_MEL_FRAME + 1
    }
}

/// Paths to the three openWakeWord models plus the wake threshold. Built by the
/// server from the `[wake]` config table; `speech-pipeline` stays free of the
/// surface crate's config types.
#[derive(Debug, Clone)]
pub struct OwwConfig {
    pub melspectrogram: PathBuf,
    pub embedding: PathBuf,
    pub model: PathBuf,
    /// Sigmoid score strictly above which a chunk wakes.
    pub threshold: f32,
}

/// One embedding step's wake score plus the chunk-derived sample offset at which
/// its scoring window ends (relative to the stream's last reset). The listener
/// adds the pod's segment base to reach a pod-absolute index; the batch wrapper
/// treats it as an offset into the segment PCM.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoredChunk {
    pub score: f32,
    pub end_sample: u64,
}

/// A threshold-crossing wake, armed past the refractory window.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WakeDetected {
    pub score: f32,
    /// Sample offset (stream-relative) at which the scoring window ends —
    /// approximately where the wake phrase completes.
    pub wake_end_sample: u64,
}

/// The three openWakeWord ONNX sessions, loaded once and shared across pods. The
/// rolling state that makes scoring incremental lives in [`OwwStream`], not here,
/// so one `OwwModels` drives every pod on the listener thread.
pub struct OwwModels {
    mel: Session,
    embedding: Session,
    wake: Session,
}

impl OwwModels {
    /// Load all three models. Fails with a precise [`WakeError::Load`] naming the
    /// offending file and the underlying `ort` reason — the daemon treats this as
    /// fatal at startup, never a silently-degraded detector.
    pub fn load(config: &OwwConfig) -> Result<OwwModels, WakeError> {
        Ok(OwwModels {
            mel: load_session(&config.melspectrogram)?,
            embedding: load_session(&config.embedding)?,
            wake: load_session(&config.model)?,
        })
    }

    /// Run the mel model over the given raw f32 samples, returning the scaled mel
    /// frames (`mel/10 + 2`). The model input is the raw sample magnitudes
    /// (openWakeWord does not normalize to `[-1, 1]`).
    pub(crate) fn run_mel(&mut self, samples: &[f32]) -> Result<Vec<[f32; MEL_BINS]>, WakeError> {
        let n = samples.len();
        let tensor = Tensor::from_array((vec![1_i64, n as i64], samples.to_vec()))
            .map_err(|e| inference("mel", e))?;
        let outputs = self
            .mel
            .run(ort::inputs![tensor])
            .map_err(|e| inference("mel", e))?;
        if outputs.len() == 0 {
            return Err(inference("mel", "model produced no outputs"));
        }
        let (_shape, data) = outputs[0]
            .try_extract_tensor::<f32>()
            .map_err(|e| inference("mel", e))?;
        if data.len() % MEL_BINS != 0 {
            return Err(inference(
                "mel",
                format!(
                    "output length {} is not a multiple of {MEL_BINS} mel bins",
                    data.len()
                ),
            ));
        }
        let frames = data.len() / MEL_BINS;
        let mut out = Vec::with_capacity(frames);
        for f in 0..frames {
            let mut frame = [0.0_f32; MEL_BINS];
            for (b, cell) in frame.iter_mut().enumerate() {
                *cell = data[f * MEL_BINS + b] / 10.0 + 2.0;
            }
            out.push(frame);
        }
        Ok(out)
    }

    /// Run the embedding model over the current 76-frame mel window → a 96-dim
    /// embedding.
    pub(crate) fn run_embedding(
        &mut self,
        mel_window: &VecDeque<[f32; MEL_BINS]>,
    ) -> Result<[f32; EMB_DIM], WakeError> {
        let mut flat = Vec::with_capacity(EMB_WINDOW * MEL_BINS);
        for frame in mel_window {
            flat.extend_from_slice(frame);
        }
        let tensor = Tensor::from_array((vec![1_i64, EMB_WINDOW as i64, MEL_BINS as i64, 1], flat))
            .map_err(|e| inference("embedding", e))?;
        let outputs = self
            .embedding
            .run(ort::inputs![tensor])
            .map_err(|e| inference("embedding", e))?;
        if outputs.len() == 0 {
            return Err(inference("embedding", "model produced no outputs"));
        }
        let (_shape, data) = outputs[0]
            .try_extract_tensor::<f32>()
            .map_err(|e| inference("embedding", e))?;
        if data.len() < EMB_DIM {
            return Err(inference(
                "embedding",
                format!(
                    "output length {} is shorter than the {EMB_DIM}-dim embedding",
                    data.len()
                ),
            ));
        }
        let mut emb = [0.0_f32; EMB_DIM];
        emb.copy_from_slice(&data[..EMB_DIM]);
        Ok(emb)
    }

    /// Run the wake model over the current 16-embedding window → one sigmoid
    /// score. A non-finite score is an error, never a silent below-threshold.
    pub(crate) fn run_wake(
        &mut self,
        emb_window: &VecDeque<[f32; EMB_DIM]>,
    ) -> Result<f32, WakeError> {
        let mut flat = Vec::with_capacity(WAKE_WINDOW * EMB_DIM);
        for emb in emb_window {
            flat.extend_from_slice(emb);
        }
        let tensor = Tensor::from_array((vec![1_i64, WAKE_WINDOW as i64, EMB_DIM as i64], flat))
            .map_err(|e| inference("wake", e))?;
        let outputs = self
            .wake
            .run(ort::inputs![tensor])
            .map_err(|e| inference("wake", e))?;
        if outputs.len() == 0 {
            return Err(inference("wake", "model produced no outputs"));
        }
        let (_shape, data) = outputs[0]
            .try_extract_tensor::<f32>()
            .map_err(|e| inference("wake", e))?;
        let Some(&score) = data.first() else {
            return Err(inference("wake", "model produced an empty score tensor"));
        };
        if !score.is_finite() {
            return Err(WakeError::NonFiniteScore);
        }
        Ok(score)
    }
}

/// One pod's rolling openWakeWord state. The mel window cold-starts from ones
/// and the embedding window from zeros; drive it with
/// [`push`](OwwStream::push) as audio arrives, [`flush`](OwwStream::flush) at a
/// segment's trailing partial chunk, and [`reset`](OwwStream::reset) on a
/// discontinuity. [`arm`](OwwStream::arm) applies the threshold + refractory to a
/// scored step.
pub struct OwwStream {
    /// Last `MEL_LOOKBACK_SAMPLES` raw samples, prepended to the next chunk for
    /// mel left context. Empty at cold-start, so the first chunk matches a
    /// whole-segment pass with no leading padding.
    lookback: Vec<f32>,
    /// Real samples not yet forming a whole chunk.
    pending: VecDeque<f32>,
    /// Persistent 76-frame mel window (cold-started from ones).
    mel_window: VecDeque<[f32; MEL_BINS]>,
    /// Persistent 16-embedding window (cold-started from zeros).
    emb_window: VecDeque<[f32; EMB_DIM]>,
    /// Number of real embeddings in `emb_window`; wake scoring waits for a full
    /// history so zero placeholders never reach the wake model.
    real_embeddings: usize,
    /// Number of complete audio chunks processed since the last reset.
    total_chunks: u64,
    /// Sigmoid threshold strictly above which `arm` fires.
    threshold: f32,
    /// No detection arms while `end_sample < refractory_until`.
    refractory_until: u64,
}

impl OwwStream {
    /// A fresh stream with rolling model state and the given wake threshold.
    pub fn new(threshold: f32) -> OwwStream {
        OwwStream {
            lookback: Vec::new(),
            pending: VecDeque::new(),
            mel_window: VecDeque::from(vec![[1.0; MEL_BINS]; EMB_WINDOW]),
            emb_window: VecDeque::from(vec![[0.0; EMB_DIM]; WAKE_WINDOW]),
            real_embeddings: 0,
            total_chunks: 0,
            threshold,
            refractory_until: 0,
        }
    }

    /// Clear all rolling state back to cold-start. Called on a pod reconnect or a
    /// sample-index discontinuity so scoring never runs across a hole.
    pub fn reset(&mut self) {
        self.lookback.clear();
        self.pending.clear();
        self.mel_window = VecDeque::from(vec![[1.0; MEL_BINS]; EMB_WINDOW]);
        self.emb_window = VecDeque::from(vec![[0.0; EMB_DIM]; WAKE_WINDOW]);
        self.real_embeddings = 0;
        self.total_chunks = 0;
        self.refractory_until = 0;
    }

    /// Feed real PCM. Processes every whole chunk now available, returning a
    /// [`ScoredChunk`] for each embedding step that completed (roughly one per
    /// chunk, with no wake score until the first 15 embeddings). A trailing partial chunk stays buffered
    /// for the next `push` or a `flush`.
    pub fn push(
        &mut self,
        models: &mut OwwModels,
        pcm: &[i16],
    ) -> Result<Vec<ScoredChunk>, WakeError> {
        self.pending.extend(pcm.iter().map(|&s| f32::from(s)));
        let mut out = Vec::new();
        while self.pending.len() >= CHUNK {
            let chunk: Vec<f32> = self.pending.drain(..CHUNK).collect();
            out.extend(self.step(models, &chunk)?);
        }
        Ok(out)
    }

    /// Score a trailing partial chunk, zero-padded up to a whole chunk (the batch
    /// tail-padding). Returns the embedding step completed by the padded chunk;
    /// nothing when the buffer is empty.
    pub fn flush(&mut self, models: &mut OwwModels) -> Result<Vec<ScoredChunk>, WakeError> {
        if self.pending.is_empty() {
            return Ok(Vec::new());
        }
        let mut chunk: Vec<f32> = self.pending.drain(..).collect();
        chunk.resize(CHUNK, 0.0);
        self.step(models, &chunk)
    }

    /// Apply the threshold + refractory to a freshly-scored step. Fires (and
    /// re-arms the refractory) on a threshold crossing outside the refractory
    /// window; provenance is reported one chunk before the scoring cursor.
    pub fn arm(&mut self, chunk: &ScoredChunk) -> Option<WakeDetected> {
        if self.real_embeddings < WAKE_WINDOW {
            return None;
        }
        if chunk.score > self.threshold && chunk.end_sample >= self.refractory_until {
            self.refractory_until = chunk.end_sample + REFRACTORY_SAMPLES;
            Some(WakeDetected {
                score: chunk.score,
                wake_end_sample: chunk.end_sample.saturating_sub(WAKE_END_LAG_SAMPLES),
            })
        } else {
            None
        }
    }

    /// One chunk step: mel over (lookback + chunk), append only the genuinely new
    /// frames (those the lookback did not already cover), and drive the
    /// embedding/wake windows once per processed chunk. Updates the rolling
    /// windows and lookback.
    fn step(
        &mut self,
        models: &mut OwwModels,
        chunk: &[f32],
    ) -> Result<Vec<ScoredChunk>, WakeError> {
        debug_assert_eq!(chunk.len(), CHUNK);
        let prev_lookback = self.lookback.len();
        let mut input = Vec::with_capacity(prev_lookback + chunk.len());
        input.extend_from_slice(&self.lookback);
        input.extend_from_slice(chunk);

        let frames = models.run_mel(&input)?;
        // Prefix-stable framing: the first `mel_frame_count(prev_lookback)` frames
        // repeat what the lookback already contributed; the rest are new.
        let already = mel_frame_count(prev_lookback).min(frames.len());
        let mut scores = Vec::new();
        for frame in &frames[already..] {
            self.mel_window.pop_front();
            self.mel_window.push_back(*frame);
        }
        self.total_chunks += 1;
        let emb = models.run_embedding(&self.mel_window)?;
        self.emb_window.pop_front();
        self.emb_window.push_back(emb);
        self.real_embeddings = (self.real_embeddings + 1).min(WAKE_WINDOW);
        if self.real_embeddings == WAKE_WINDOW {
            let score = models.run_wake(&self.emb_window)?;
            scores.push(ScoredChunk {
                score,
                end_sample: self.total_chunks * CHUNK as u64,
            });
        }

        let keep = input.len().min(MEL_LOOKBACK_SAMPLES);
        self.lookback = input[input.len() - keep..].to_vec();
        Ok(scores)
    }
}

/// Map a runtime `ort` failure — or an unexpected output shape — during scoring
/// to [`WakeError::Inference`], tagged with the model stage (`mel`/`embedding`/
/// `wake`) that produced it. A shape surprise fails closed rather than panicking
/// the listener thread.
fn inference(stage: &str, e: impl std::fmt::Display) -> WakeError {
    WakeError::Inference(format!("{stage}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        oww_config as test_config, oww_model_dir, oww_models, seeded_noise, wake_phrase_pcm,
    };

    fn test_models() -> OwwModels {
        oww_models()
    }

    fn model_dir() -> PathBuf {
        oww_model_dir()
    }

    /// Reference batch scorer over a complete real embedding history. Streaming
    /// must preserve the decision; incomplete histories produce no score.
    fn batch_reference_max(models: &mut OwwModels, pcm: &[i16]) -> Option<f32> {
        let mut mel_window: VecDeque<[f32; MEL_BINS]> =
            VecDeque::from(vec![[1.0; MEL_BINS]; EMB_WINDOW]);
        let mut emb_window: VecDeque<[f32; EMB_DIM]> =
            VecDeque::from(vec![[0.0; EMB_DIM]; WAKE_WINDOW]);
        let mut samples: Vec<f32> = pcm.iter().map(|&s| f32::from(s)).collect();
        let target = samples.len().max(1).div_ceil(CHUNK) * CHUNK;
        samples.resize(target, 0.0);

        let frames = models.run_mel(&samples).unwrap();
        let mut frame_count = 0usize;
        let mut real_embeddings = 0usize;
        let mut best: Option<f32> = None;
        for frame in frames {
            mel_window.pop_front();
            mel_window.push_back(frame);
            frame_count += 1;
            if frame_count == 5 || (frame_count > 5 && (frame_count - 5).is_multiple_of(EMB_STEP)) {
                let emb = models.run_embedding(&mel_window).unwrap();
                emb_window.pop_front();
                emb_window.push_back(emb);
                real_embeddings += 1;
                if real_embeddings >= WAKE_WINDOW {
                    let score = models.run_wake(&emb_window).unwrap();
                    best = Some(best.map_or(score, |b| b.max(score)));
                }
            }
        }
        best
    }

    /// Feed a whole segment through a fresh stream (push + flush)
    /// and return the maximum score — the batch verdict, derived from streaming.
    fn stream_max(models: &mut OwwModels, pcm: &[i16]) -> Option<f32> {
        let mut stream = OwwStream::new(0.5);
        let mut best: Option<f32> = None;
        let fold = |b: &mut Option<f32>, s: f32| *b = Some(b.map_or(s, |x: f32| x.max(s)));
        for sc in stream.push(models, pcm).unwrap() {
            fold(&mut best, sc.score);
        }
        for sc in stream.flush(models).unwrap() {
            fold(&mut best, sc.score);
        }
        best
    }

    /// Pins the mel model's valid-framing geometry (`MEL_STFT_WINDOW`, hop): the
    /// analytic [`mel_frame_count`] must equal the model's output for a spread of
    /// lengths. Any model swap that changes the framing breaks here, before it can
    /// corrupt the streaming frame accounting.
    #[test]
    fn mel_frame_count_matches_model() {
        let mut models = test_models();
        for n in [MEL_STFT_WINDOW, CHUNK, 2 * CHUNK, 3 * CHUNK, 25 * CHUNK] {
            let actual = models.run_mel(&vec![0.0; n]).unwrap().len();
            assert_eq!(
                actual,
                mel_frame_count(n),
                "mel({n}) produced {actual} frames, formula said {}",
                mel_frame_count(n)
            );
        }
    }

    /// The mel-contiguity pin: the first chunk over an empty lookback yields 5
    /// frames, and every steady-state chunk adds exactly `EMB_STEP` (8) — the
    /// cadence that makes one embedding fire per chunk.
    #[test]
    fn steady_state_adds_eight_frames_per_chunk() {
        assert_eq!(mel_frame_count(CHUNK), 5, "first chunk (no lookback)");
        assert_eq!(
            mel_frame_count(MEL_LOOKBACK_SAMPLES + CHUNK) - mel_frame_count(MEL_LOOKBACK_SAMPLES),
            EMB_STEP,
            "steady-state chunk over a full lookback"
        );
    }

    /// Streaming and batch agree on the wake decision. Streamed mel values can
    /// differ slightly from whole-segment values near phrase edges.
    #[test]
    fn stream_matches_batch_on_wake_phrase() {
        let mut models = test_models();
        let pcm = wake_phrase_pcm();
        let reference = batch_reference_max(&mut models, &pcm).unwrap();
        let streamed = stream_max(&mut models, &pcm).unwrap();
        assert!(
            reference > 0.5,
            "batch oracle must detect the wake phrase, got {reference}"
        );
        assert!(
            streamed > 0.5,
            "streaming must detect the wake phrase, got {streamed}"
        );
        assert!(
            // Streaming mel values may differ near phrase edges; 0.01 preserves
            // the accept/reject parity check while allowing that model variance.
            (reference - streamed).abs() < 0.01,
            "streaming score {streamed} diverges materially from batch {reference}"
        );
    }

    /// Streaming reproduces the batch oracle on noise: both reject, scores equal.
    #[test]
    fn stream_matches_batch_on_noise() {
        let mut models = test_models();
        let pcm = seeded_noise(1, 32_000);
        let reference = batch_reference_max(&mut models, &pcm).unwrap();
        let streamed = stream_max(&mut models, &pcm).unwrap();
        assert!(
            reference <= 0.5,
            "noise must not wake the oracle: {reference}"
        );
        assert!(streamed <= 0.5, "noise must not wake streaming: {streamed}");
        assert!(
            (reference - streamed).abs() < 1e-3,
            "streaming score {streamed} diverges from batch {reference}"
        );
    }

    /// Scores are finite and in `[0, 1]`; a 32 000-sample feed produces one score
    /// per processed chunk (none from the first fifteen), each end-sample a whole
    /// chunk further along.
    #[test]
    fn push_scores_on_the_embedding_cadence() {
        let mut models = test_models();
        let mut stream = OwwStream::new(0.5);
        let pcm = seeded_noise(2, 32_000);
        let scored = stream.push(&mut models, &pcm).unwrap();
        let chunk_count = 32_000_usize.div_ceil(CHUNK);
        assert_eq!(
            scored.len(),
            chunk_count.saturating_sub(WAKE_WINDOW - 1),
            "one score per chunk after {WAKE_WINDOW} real embeddings over {chunk_count} chunks"
        );
        for (i, sc) in scored.iter().enumerate() {
            assert!(sc.score.is_finite(), "score {} not finite: {}", i, sc.score);
            assert!(
                (0.0..=1.0).contains(&sc.score),
                "sigmoid score {} out of range: {}",
                i,
                sc.score
            );
            assert_eq!(
                sc.end_sample,
                (i as u64 + WAKE_WINDOW as u64) * CHUNK as u64,
                "score {i} window-end cursor"
            );
        }
    }

    /// Wake scoring starts only after sixteen real embeddings, at the exact
    /// sample cursor of the sixteenth embedding.
    #[test]
    fn first_score_waits_for_sixteen_real_embeddings() {
        let mut models = test_models();
        let mut stream = OwwStream::new(0.5);
        let pcm = seeded_noise(8, 17 * CHUNK);
        let scored = stream.push(&mut models, &pcm).unwrap();
        assert_eq!(scored.len(), 2, "embeddings 16 and 17 are scored");
        assert_eq!(
            scored[0].end_sample,
            WAKE_WINDOW as u64 * CHUNK as u64,
            "the first score ends at the sixteenth embedding"
        );
    }

    /// Pins per-step scores captured from Python openWakeWord 0.6.0
    /// `Model.predict` on 1,280-sample chunks using the committed models.
    #[test]
    fn python_per_step_scores_regression() {
        let mut models = test_models();
        let mut stream = OwwStream::new(0.5);
        let mut pcm = vec![0_i16; 32_000];
        pcm.extend(wake_phrase_pcm());
        let mut scored = stream.push(&mut models, &pcm).unwrap();
        scored.extend(stream.flush(&mut models).unwrap());
        let expected = [
            (20_480, 0.000010639),
            (43_520, 0.022298783),
            (46_080, 0.197_546_24),
            (47_360, 0.609_890_46),
            (48_640, 0.852_638_8),
            (49_920, 0.578_628_96),
            (51_200, 0.989_247_9),
            (53_760, 0.994_896_7),
        ];
        for (end_sample, expected_score) in expected {
            let actual = scored
                .iter()
                .find(|sc| sc.end_sample == end_sample)
                .unwrap_or_else(|| panic!("missing score at {end_sample}"))
                .score;
            assert!(
                (actual - expected_score).abs() <= 0.001,
                "score at {end_sample}: expected {expected_score}, got {actual}"
            );
        }
    }

    /// A sub-chunk feed computes one embedding on flush but cannot be wake-scored.
    #[test]
    fn sub_chunk_does_not_score() {
        let mut models = test_models();
        let mut stream = OwwStream::new(0.5);
        let pcm = seeded_noise(3, 100);
        assert!(
            stream.push(&mut models, &pcm).unwrap().is_empty(),
            "100 samples is under one chunk"
        );
        assert!(
            stream.flush(&mut models).unwrap().is_empty(),
            "one embedding is below the sixteen-embedding wake history"
        );
        assert!(stream.push(&mut models, &[]).unwrap().is_empty());
    }

    /// Chunks feed identically whether delivered whole or split at ragged offsets:
    /// the pending buffer stitches the split, so the scores match.
    #[test]
    fn split_pushes_match_single_push() {
        let mut models = test_models();
        let pcm = seeded_noise(5, 4 * CHUNK);

        let mut whole = OwwStream::new(0.5);
        let one_shot = whole.push(&mut models, &pcm).unwrap();

        let mut split = OwwStream::new(0.5);
        let mut split_scores = Vec::new();
        for part in pcm.chunks(700) {
            split_scores.extend(split.push(&mut models, part).unwrap());
        }
        assert_eq!(
            one_shot, split_scores,
            "chunking must be delivery-invariant"
        );
    }

    /// `reset` returns the stream to cold-start, including wake readiness.
    #[test]
    fn reset_restores_cold_start() {
        let mut models = test_models();
        let a = seeded_noise(1, 3 * CHUNK);
        let b = seeded_noise(2, 3 * CHUNK);

        let mut reused = OwwStream::new(0.5);
        let _ = reused.push(&mut models, &a).unwrap();
        reused.reset();
        let after_reset = reused.push(&mut models, &b).unwrap();

        let mut fresh = OwwStream::new(0.5);
        let fresh_scores = fresh.push(&mut models, &b).unwrap();
        assert_eq!(
            after_reset, fresh_scores,
            "reset must clear all rolling state"
        );
    }

    #[test]
    fn reset_clears_wake_readiness() {
        let mut models = test_models();
        let mut stream = OwwStream::new(-1.0);
        let chunk_count = WAKE_WINDOW;
        assert_eq!(
            stream
                .push(&mut models, &seeded_noise(8, chunk_count * CHUNK))
                .unwrap()
                .len(),
            1,
            "a warmed stream scores"
        );
        stream.reset();
        let chunk_count = WAKE_WINDOW - 1;
        assert!(
            stream
                .push(&mut models, &seeded_noise(9, chunk_count * CHUNK))
                .unwrap()
                .is_empty()
        );
        let scored = stream.push(&mut models, &seeded_noise(10, CHUNK)).unwrap();
        assert_eq!(scored.len(), 1, "reset requires sixteen fresh embeddings");
    }

    #[test]
    fn arm_rejects_synthetic_chunk_before_ready() {
        let mut stream = OwwStream::new(-1.0);
        assert!(
            stream
                .arm(&ScoredChunk {
                    score: 1.0,
                    end_sample: 0
                })
                .is_none()
        );
    }

    #[test]
    fn digital_silence_stays_unarmed_across_reset() {
        let mut models = test_models();
        let mut stream = OwwStream::new(0.5);
        let silence = vec![0_i16; 32_000];
        for scored in stream.push(&mut models, &silence).unwrap() {
            assert!(stream.arm(&scored).is_none());
        }
        stream.reset();
        for scored in stream.push(&mut models, &silence).unwrap() {
            assert!(stream.arm(&scored).is_none());
        }
    }

    /// `arm` fires on a threshold crossing, then suppresses further crossings for
    /// `REFRACTORY_SAMPLES`, then fires again once the window elapses.
    #[test]
    fn arm_enforces_threshold_and_refractory() {
        let mut stream = OwwStream::new(0.5);
        stream.real_embeddings = WAKE_WINDOW;
        // Below threshold: no arm, refractory untouched.
        assert_eq!(
            stream.arm(&ScoredChunk {
                score: 0.4,
                end_sample: CHUNK as u64
            }),
            None
        );
        // First crossing arms.
        let first = stream
            .arm(&ScoredChunk {
                score: 0.9,
                end_sample: 2 * CHUNK as u64,
            })
            .expect("crossing arms");
        assert_eq!(first.wake_end_sample, CHUNK as u64);
        // A crossing inside the refractory window is suppressed.
        assert_eq!(
            stream.arm(&ScoredChunk {
                score: 0.95,
                end_sample: 2 * CHUNK as u64 + REFRACTORY_SAMPLES - 1,
            }),
            None,
            "double-fire on one phrase must be suppressed"
        );
        // Past the window, a new crossing arms again.
        assert!(
            stream
                .arm(&ScoredChunk {
                    score: 0.8,
                    end_sample: 2 * CHUNK as u64 + REFRACTORY_SAMPLES,
                })
                .is_some(),
            "a crossing past the refractory window arms again"
        );
    }

    #[test]
    fn missing_model_is_a_load_error() {
        let mut config = test_config(0.5);
        config.melspectrogram = model_dir().join("does-not-exist.onnx");
        match OwwModels::load(&config) {
            Err(WakeError::Load { model, .. }) => assert!(model.contains("does-not-exist.onnx")),
            Err(other) => panic!("expected Load error, got {other:?}"),
            Ok(_) => panic!("expected load failure for a missing model file"),
        }
    }
}
