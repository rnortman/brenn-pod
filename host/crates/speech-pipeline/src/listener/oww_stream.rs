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
//!   persistent 76-frame mel window, the 16-embedding window, the chunk cursor,
//!   and the wake refractory. It borrows the models per step.
//!
//! **Upstream parity is the governing constraint.** Every geometry choice here
//! — the 480-sample raw lookback, the ones-filled mel window, one embedding per
//! 1 280-sample chunk, no wake score until sixteen real embeddings — reproduces
//! openWakeWord 0.6.0's `Model.predict` step for step. `python_per_step_scores_
//! regression` in this module's tests is that parity's pin: per-step scores
//! captured from the Python implementation on the committed models. Change a
//! constant here and that test is the thing that tells you the front end has
//! drifted.
//!
//! **The framing invariant.** The mel model uses valid framing (window
//! [`MEL_STFT_WINDOW`], hop [`SAMPLES_PER_MEL_FRAME`], no edge padding), so a
//! chunk run over (lookback + chunk) emits a predictable frame count: 5 for the
//! first chunk (empty lookback) and exactly [`EMB_STEP`] = 8 for every chunk
//! after, over a full [`MEL_LOOKBACK_SAMPLES`] lookback. Those 8 frames per
//! chunk are what keep one embedding firing per chunk. The lookback is shorter
//! than the STFT window, so a streamed frame is *not* bit-identical to the same
//! frame from a whole-segment mel pass — it differs by the left context the
//! 480-sample lookback does not carry. That is upstream's behaviour too, and it
//! is why the whole-segment comparison in the tests carries a tolerance.
//!
//! The mel window cold-starts from [`MEL_COLD_FILL`] and the embedding window
//! from zeros, but the wake model never sees a placeholder embedding: scoring
//! waits for [`WAKE_WINDOW`] real embeddings.

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
/// Mel frames a steady-state chunk appends to the mel window: 8, one per 10 ms
/// hop across an 80 ms chunk. One embedding is produced per processed chunk, so
/// this is the window's advance between successive embeddings.
pub(crate) const EMB_STEP: usize = 8;
/// Audio samples one mel frame advances (10 ms at 16 kHz): the mel model's STFT
/// hop.
pub(crate) const SAMPLES_PER_MEL_FRAME: usize = CHUNK / EMB_STEP;
/// The mel model's STFT window, in samples. Empirically pinned against the
/// committed model by `mel_frame_count_matches_model`: `run_mel` uses valid
/// framing, emitting `(n - MEL_STFT_WINDOW) / SAMPLES_PER_MEL_FRAME + 1` frames
/// for `n >= MEL_STFT_WINDOW` and none below. A model change breaks that test.
pub(crate) const MEL_STFT_WINDOW: usize = 640;

/// Raw-PCM samples of lookback prepended to each chunk before the mel pass: 3
/// mel hops, 30 ms. Not a free parameter — openWakeWord 0.6.0 keeps exactly 3
/// hops of raw lookback across chunks, and matching it is what makes our
/// per-step scores equal `Model.predict`'s. It is deliberately *shorter* than
/// [`MEL_STFT_WINDOW`], so the first frames of a chunk see less left context
/// than a whole-segment pass would give them.
pub(crate) const MEL_LOOKBACK_SAMPLES: usize = 3 * SAMPLES_PER_MEL_FRAME;

// `step` appends every frame the mel pass returns, which is only correct while
// the lookback is too short to complete a frame of its own.
const _: () = assert!(MEL_LOOKBACK_SAMPLES < MEL_STFT_WINDOW);

/// Value the persistent mel window is filled with at cold start. Ones, not
/// zeros: openWakeWord 0.6.0 initialises its melspectrogram buffer to ones, and
/// the embedding model is only in distribution for a window shaped like that.
/// A "tidy-up" back to zeros changes every warm-up embedding and silently
/// breaks per-step parity with the Python implementation.
pub(crate) const MEL_COLD_FILL: f32 = 1.0;

/// Samples after a detection during which further detections are suppressed
/// (~2 s at 16 kHz), so one spoken phrase arms the wake once, not repeatedly.
pub(crate) const REFRACTORY_SAMPLES: u64 = 32_000;

/// Samples subtracted from the scoring cursor to place a detection's
/// `wake_end_sample`.
///
/// A step's `end_sample` is the exact end of the last mel frame that step fed:
/// after `N` chunks the window holds `8N - 3` frames, the last of which ends at
/// `(8N - 4) * 160 + 640 = N * 1280`. So `end_sample` is where the *audio the
/// score saw* ends, not where the phrase ends — the wake head goes on climbing
/// for several chunks after the phrase is over (see the tail of
/// `python_per_step_scores_regression`, whose maximum lands past the end of the
/// committed phrase).
///
/// The crossing is therefore observed no earlier than the chunk in which it
/// happened, and one chunk is backed off so the carve cursor names the start of
/// that chunk rather than its end: the listener never eats the chunk whose
/// scoring produced the arm. It is "one chunk", not "80 ms" — the quantity
/// being undone is the step's own granularity, so it tracks `CHUNK`.
/// `first_arm_lands_inside_the_wake_phrase` pins the resulting cursor against
/// the committed fixture.
pub(crate) const WAKE_END_LAG_SAMPLES: u64 = CHUNK as u64;

/// Audio a stream must see after a reset before it can produce any wake score:
/// [`WAKE_WINDOW`] chunks, 1.28 s at 16 kHz. Public because it is a contract
/// with whoever supplies the audio — a segment shorter than this is never
/// scored, and a wake phrase whose head sits inside this window is only
/// detected because the head's score peaks well after the phrase begins.
///
/// TODO(wake-readiness-preroll-coupling): nothing ties this to the device's
/// VAD-onset preroll (`audio-pipeline`'s `PREROLL_SAMPLES`, 16 000), which is
/// shorter, and the listener re-enters this window on every `SegmentOpened`.
pub const WAKE_READINESS_SAMPLES: u64 = (WAKE_WINDOW * CHUNK) as u64;

/// Frames `run_mel` emits for `n` raw samples under the model's valid framing.
/// The analytic form of the geometry `mel_frame_count_matches_model` pins
/// against the committed model.
///
/// Test-only. `step` does not consult it: the lookback is too short to complete
/// a frame, so every frame a chunk's pass returns is new and there is nothing
/// to subtract. It exists to state the framing the module depends on in a form
/// a test can compare against the model itself.
#[cfg(test)]
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

/// One pod's rolling openWakeWord state. The mel window cold-starts from
/// [`MEL_COLD_FILL`] and the embedding window from zeros; drive it with
/// [`push`](OwwStream::push) as audio arrives, [`flush`](OwwStream::flush) at a
/// segment's trailing partial chunk, and [`reset`](OwwStream::reset) on a
/// discontinuity. [`arm`](OwwStream::arm) applies the threshold + refractory to a
/// scored step.
pub struct OwwStream {
    /// Last `MEL_LOOKBACK_SAMPLES` raw samples, prepended to the next chunk for
    /// mel left context. Empty at cold-start, so the first chunk is framed from
    /// its own first sample and yields 5 frames instead of 8.
    lookback: Vec<f32>,
    /// Real samples not yet forming a whole chunk.
    pending: VecDeque<f32>,
    /// Persistent 76-frame mel window (cold-started from [`MEL_COLD_FILL`]).
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
            mel_window: VecDeque::from(vec![[MEL_COLD_FILL; MEL_BINS]; EMB_WINDOW]),
            emb_window: VecDeque::from(vec![[0.0; EMB_DIM]; WAKE_WINDOW]),
            real_embeddings: 0,
            total_chunks: 0,
            threshold,
            refractory_until: 0,
        }
    }

    /// Clear all rolling state back to cold-start. Called on a pod reconnect or a
    /// sample-index discontinuity so scoring never runs across a hole.
    ///
    /// Re-runs the constructor rather than clearing field by field: a field
    /// added to `OwwStream` cannot then be initialised in one place and
    /// forgotten in the other, which would leak state across a re-anchor.
    pub fn reset(&mut self) {
        *self = OwwStream::new(self.threshold);
    }

    /// Feed real PCM. Processes every whole chunk now available, returning one
    /// [`ScoredChunk`] per processed chunk, starting with the chunk that
    /// completes [`WAKE_WINDOW`] real embeddings — the first
    /// [`WAKE_READINESS_SAMPLES`] after a reset yield none. A trailing partial
    /// chunk stays buffered for the next `push` or a `flush`.
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

    /// Whole chunks processed since the last reset. The denominator the listener
    /// needs to report how much audio the wake model consumed, including the
    /// chunks consumed before the stream was ready to score.
    pub fn chunks_processed(&self) -> u64 {
        self.total_chunks
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
    /// window; provenance is reported [`WAKE_END_LAG_SAMPLES`] before the
    /// scoring cursor.
    ///
    /// Readiness is not re-checked here. [`step`](OwwStream::step) is the single
    /// gate: no `ScoredChunk` exists at all until the embedding history is real,
    /// so a second guard on this side could only ever disagree with the first.
    pub fn arm(&mut self, chunk: &ScoredChunk) -> Option<WakeDetected> {
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

    /// One chunk step: mel over (lookback + chunk), append every frame the pass
    /// produced, and drive the embedding/wake windows once per processed chunk.
    /// Updates the rolling windows and lookback.
    ///
    /// Every frame is new: the lookback is shorter than [`MEL_STFT_WINDOW`], so
    /// it contributes no complete frame of its own and only supplies left
    /// context. That is what makes the count 5 for the first chunk and
    /// [`EMB_STEP`] for every chunk after.
    fn step(
        &mut self,
        models: &mut OwwModels,
        chunk: &[f32],
    ) -> Result<Vec<ScoredChunk>, WakeError> {
        debug_assert_eq!(chunk.len(), CHUNK);
        let mut input = Vec::with_capacity(self.lookback.len() + chunk.len());
        input.extend_from_slice(&self.lookback);
        input.extend_from_slice(chunk);

        let frames = models.run_mel(&input)?;
        let mut scores = Vec::new();
        for frame in &frames {
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

    /// Re-derive the maximum wake score from a single whole-segment mel pass,
    /// sliding the same windows over the contiguous frames.
    ///
    /// **Not an oracle.** It reimplements this module's own choices (the cold
    /// fill, the 5-then-8 cadence, the sixteen-embedding gate), so it can only
    /// catch a streaming/batch *self*-consistency break — a chunking bug in
    /// `push`/`flush` — never a wrong choice made in both places. The external
    /// ground truth is `python_per_step_scores_regression`, which pins values
    /// produced by openWakeWord 0.6.0 rather than by this file.
    fn whole_segment_max(models: &mut OwwModels, pcm: &[i16]) -> Option<f32> {
        let mut mel_window: VecDeque<[f32; MEL_BINS]> =
            VecDeque::from(vec![[MEL_COLD_FILL; MEL_BINS]; EMB_WINDOW]);
        let mut emb_window: VecDeque<[f32; EMB_DIM]> =
            VecDeque::from(vec![[0.0; EMB_DIM]; WAKE_WINDOW]);
        let mut samples: Vec<f32> = pcm.iter().map(|&s| f32::from(s)).collect();
        let target = samples.len().max(1).div_ceil(CHUNK) * CHUNK;
        samples.resize(target, 0.0);

        let frames = models.run_mel(&samples).unwrap();
        // The first chunk contributes `mel_frame_count(CHUNK)` frames; every
        // chunk after contributes `EMB_STEP`. An embedding fires on each.
        let first = mel_frame_count(CHUNK);
        let mut frame_count = 0usize;
        let mut real_embeddings = 0usize;
        let mut best: Option<f32> = None;
        for frame in frames {
            mel_window.pop_front();
            mel_window.push_back(frame);
            frame_count += 1;
            if frame_count == first
                || (frame_count > first && (frame_count - first).is_multiple_of(EMB_STEP))
            {
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

    /// Streaming and a whole-segment re-derivation agree on the wake decision
    /// for the committed phrase, and on the score to within the framing
    /// difference between them.
    #[test]
    fn stream_matches_batch_on_wake_phrase() {
        let mut models = test_models();
        let pcm = wake_phrase_pcm();
        let reference = whole_segment_max(&mut models, &pcm).unwrap();
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
            // The bound is the cost of `MEL_LOOKBACK_SAMPLES` (480) being
            // shorter than `MEL_STFT_WINDOW` (640): the first frames of each
            // streamed chunk see less left context than the same frames of a
            // single contiguous pass, so the two mel spectra differ slightly at
            // every chunk boundary. Tighten this only by lengthening the
            // lookback — which would break parity with openWakeWord.
            (reference - streamed).abs() < 0.01,
            "streaming score {streamed} diverges materially from batch {reference}"
        );
    }

    /// Streaming reproduces the whole-segment pass on noise: both reject, scores
    /// equal.
    #[test]
    fn stream_matches_batch_on_noise() {
        let mut models = test_models();
        let pcm = seeded_noise(1, 32_000);
        let reference = whole_segment_max(&mut models, &pcm).unwrap();
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

    /// What [`WAKE_END_LAG_SAMPLES`] actually buys, measured against the
    /// committed phrase rather than restated from the constant.
    ///
    /// The fixture is 32 000 samples of silence then the 20 507-sample "Hey
    /// Jarvis" clip, so the phrase occupies 32 000..52 507. The first
    /// threshold crossing must therefore name a cursor *inside* the phrase —
    /// past enough of it to be the phrase that scored, and short of its end so
    /// the command audio after it is never eaten. The head goes on climbing
    /// past the end of the clip (`python_per_step_scores_regression` peaks at
    /// 53 760), which is exactly why the cursor is backed off.
    #[test]
    fn first_arm_lands_inside_the_wake_phrase() {
        let mut models = test_models();
        let mut stream = OwwStream::new(0.5);
        let lead = 32_000_usize;
        let phrase = wake_phrase_pcm();
        let phrase_end = lead + phrase.len();
        let mut pcm = vec![0_i16; lead];
        pcm.extend_from_slice(&phrase);

        let mut scored = stream.push(&mut models, &pcm).unwrap();
        scored.extend(stream.flush(&mut models).unwrap());
        let first = scored
            .iter()
            .find_map(|sc| stream.arm(sc))
            .expect("the committed phrase arms");

        assert_eq!(
            first.wake_end_sample, 46_080,
            "the first crossing's cursor moved; scores: {scored:?}"
        );
        assert!(
            first.wake_end_sample > lead as u64,
            "cursor {} precedes the phrase at {lead}",
            first.wake_end_sample
        );
        assert!(
            first.wake_end_sample < phrase_end as u64,
            "cursor {} runs past the phrase end {phrase_end}",
            first.wake_end_sample
        );
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

    /// The cold-start guarantee, end to end through the public surface: a
    /// stream fed nothing but digital silence offers `arm` no chunk it can fire
    /// on, even with the threshold pushed below every possible score. Readiness
    /// is enforced by there being no `ScoredChunk` to arm, which is why `arm`
    /// needs no guard of its own.
    #[test]
    fn a_threshold_below_zero_still_cannot_wake_on_cold_silence() {
        let mut models = test_models();
        let mut stream = OwwStream::new(-1.0);
        let scored = stream
            .push(
                &mut models,
                &vec![0_i16; WAKE_READINESS_SAMPLES as usize - 1],
            )
            .unwrap();
        assert!(
            scored.is_empty(),
            "under {WAKE_READINESS_SAMPLES} samples cannot be scored: {scored:?}"
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
