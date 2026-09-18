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
//! 1 280-sample chunk, the noise-seeded embedding history, the five suppressed
//! warm-up chunks — reproduces openWakeWord 0.6.0's `Model.predict` step for
//! step. `python_per_step_scores_regression` in this module's tests is that
//! parity's pin: per-step scores captured from the Python implementation on the
//! committed models. Change a constant here and that test is the thing that
//! tells you the front end has drifted.
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
//! **Cold start.** The mel window cold-starts from [`MEL_COLD_FILL`]; the
//! embedding window is seeded, on the stream's first step, with embeddings the
//! models compute from a fixed clip of low-amplitude noise
//! ([`cold_seed_noise`], [`OwwModels::cold_history`]). The wake head therefore
//! never sees a placeholder embedding — every window it scores is sixteen
//! embeddings of real audio, seed or live. The first [`WARMUP_CHUNKS`] chunks
//! after a reset are not scored at all, which is where upstream zeroes its
//! first five predictions; the first score lands at
//! [`WAKE_READINESS_SAMPLES`], inside the audio a device sends ahead of the
//! speech its VAD opened on.

use std::collections::VecDeque;
use std::path::PathBuf;

use ort::session::Session;
use ort::value::Tensor;

use super::ort_util::load_session;
use crate::types::SPINE_FORMAT;
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

/// Chunks after a reset whose scores are suppressed. openWakeWord 0.6.0's
/// `Model.predict` zeroes a model's first five predictions after construction
/// or `reset()`; we emit no [`ScoredChunk`] for them at all rather than a
/// fabricated zero, so nothing downstream can mistake a warm-up for a
/// measurement.
pub(crate) const WARMUP_CHUNKS: u64 = 5;

/// Audio a stream must see after a reset before it can produce any wake score:
/// the end of the first chunk past [`WARMUP_CHUNKS`], 0.48 s at 16 kHz. Public
/// because it is a contract with whoever supplies the audio — a segment shorter
/// than this is never scored.
///
/// The requirement it encodes, pinned below: the first scored chunk lies wholly
/// inside the device's VAD-onset preroll, so every chunk that touches the
/// phrase — the chunk containing the VAD onset included — is scored. Residual:
/// within the first ~2 s of a capture run the device ring is not yet full and
/// ships less than a full preroll, so the first chunks of a phrase spoken then
/// go unscored and detection rests on the head scoring later in the phrase.
pub const WAKE_READINESS_SAMPLES: u64 = (WARMUP_CHUNKS + 1) * CHUNK as u64;

const _: () = assert!(WAKE_READINESS_SAMPLES <= audio_pipeline::ring::PREROLL_SAMPLES);

/// Samples of noise whose embeddings seed a cold stream's embedding history:
/// 4 s of spine audio, openWakeWord 0.6.0's `AudioFeatures.reset` draw.
pub(crate) const COLD_SEED_SAMPLES: usize = 4 * SPINE_FORMAT.sample_rate_hz as usize;

/// Amplitude bound of that noise, uniform over `[-COLD_SEED_AMPLITUDE,
/// COLD_SEED_AMPLITUDE)` — upstream's `randint(-1000, 1000, 16000*4)`.
pub(crate) const COLD_SEED_AMPLITUDE: i16 = 1000;

/// Seed of the LCG [`cold_seed_noise`] draws from. Upstream's draw is unseeded
/// `np.random`, redrawn on every reset; ours is one fixed clip so the history
/// is reproducible and the Python capture script reproduces it bit for bit.
const COLD_SEED_LCG_SEED: u64 = 12_345;

/// SHA-256 of [`cold_seed_noise`]'s output as little-endian S16 bytes. The
/// oracle contract with `host/tools/oww-oracle`: both sides carry this literal
/// and refuse to run on a mismatch, so a drift in either copy of the generator
/// fails loudly instead of silently re-blessing different scores. Test-only on
/// this side: `cold_seed_matches_pinned_digest` is what enforces it, and the
/// production path takes the clip straight from the generator.
#[cfg(test)]
pub(crate) const COLD_SEED_SHA256: &str =
    "be23a5a494c0650aea77466547dafe8f1abcb00fa6a5be1f079bc8061cbdefde";

/// Multiplier of the 64-bit LCG every deterministic noise clip in this crate is
/// drawn from (Knuth's MMIX constants).
pub(crate) const LCG_MULTIPLIER: u64 = 6364136223846793005;
/// Increment of that LCG.
pub(crate) const LCG_INCREMENT: u64 = 1442695040888963407;

/// `n` samples of deterministic uniform noise over `[-amplitude, amplitude)`,
/// from a 64-bit LCG: `state = state * LCG_MULTIPLIER + LCG_INCREMENT`
/// (mod 2^64) from `seed`, each sample mapped
/// `((state >> 33) % (2 * amplitude)) - amplitude`.
///
/// The recurrence, the seed and the mapping are part of the oracle contract —
/// `host/tools/oww-oracle` carries the same generator in Python and the two
/// clips' SHA-256 pins are what keep the copies honest. One generator here, so
/// a fix to it (the modulo bias, the extraction width) reaches every clip
/// rather than one of them.
pub(crate) fn bounded_lcg_noise(seed: u64, n: usize, amplitude: i16) -> Vec<i16> {
    let mut state = seed;
    let span = 2 * i64::from(amplitude);
    (0..n)
        .map(|_| {
            state = state
                .wrapping_mul(LCG_MULTIPLIER)
                .wrapping_add(LCG_INCREMENT);
            (((state >> 33) as i64 % span) - i64::from(amplitude)) as i16
        })
        .collect()
}

/// The fixed noise clip whose embeddings seed a cold embedding history: the
/// shared generator at [`COLD_SEED_LCG_SEED`]. Change the seed, the generator or
/// the amplitude and the pinned per-step scores for the chunks the seed still
/// occupies change with them.
pub(crate) fn cold_seed_noise() -> Vec<i16> {
    bounded_lcg_noise(COLD_SEED_LCG_SEED, COLD_SEED_SAMPLES, COLD_SEED_AMPLITUDE)
}

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

/// The leading run of chunks one [`push`](OwwStream::push) or
/// [`flush`](OwwStream::flush) consumed without producing a score — the stream's
/// warm-up, seen from the caller's side. Carries both ends of the run, so an
/// observer reporting it names the span it actually covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnscoredRun {
    /// Chunks in the run.
    pub chunks: u32,
    /// Stream-relative sample index at which the run's first chunk ends.
    pub first_chunk_end: u64,
    /// Stream-relative sample index at which the run's last chunk ends.
    pub last_chunk_end: u64,
}

/// What one [`push`](OwwStream::push) or [`flush`](OwwStream::flush) did: the
/// steps it scored, and the leading run it consumed without scoring.
///
/// The stream reports its own cursors rather than leaving a caller to rebuild
/// them from [`CHUNK`] and a chunk count: the chunk-ordinal → sample-cursor
/// mapping is this module's, and a second copy of it elsewhere drifts silently
/// the moment the cadence changes.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PushReport {
    /// One entry per chunk that scored, in order.
    pub scored: Vec<ScoredChunk>,
    /// The warm-up chunks this call consumed, if any. Readiness is monotone, so
    /// these are always the leading chunks of the call.
    pub unscored: Option<UnscoredRun>,
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
    /// Embedding history a cold [`OwwStream`] starts from: the last
    /// [`WAKE_WINDOW`] embeddings of [`cold_seed_noise`], computed at load.
    /// Derived from the models rather than committed as a tensor, so a model
    /// swap re-derives it and it cannot go stale.
    pub(crate) cold_history: [[f32; EMB_DIM]; WAKE_WINDOW],
}

impl OwwModels {
    /// Load all three models and compute the cold embedding history. Fails with
    /// a precise [`WakeError::Load`] naming the offending file and the
    /// underlying `ort` reason — the daemon treats this as fatal at startup,
    /// never a silently-degraded detector. A seeding failure is
    /// [`WakeError::Inference`] and equally fatal: a stream is never left
    /// un-seeded.
    pub fn load(config: &OwwConfig) -> Result<OwwModels, WakeError> {
        let mut mel = load_session(&config.melspectrogram)?;
        let mut embedding = load_session(&config.embedding)?;
        let wake = load_session(&config.model)?;
        // Seeded before the struct exists, so there is no moment at which
        // `cold_history` holds the placeholder the seeding replaces.
        let cold_history = compute_cold_history(&mut mel, &mut embedding)?;
        Ok(OwwModels {
            mel,
            embedding,
            wake,
            cold_history,
        })
    }

    /// Run the mel model over the given raw f32 samples, returning the scaled mel
    /// frames (`mel/10 + 2`). The model input is the raw sample magnitudes
    /// (openWakeWord does not normalize to `[-1, 1]`).
    pub(crate) fn run_mel(&mut self, samples: &[f32]) -> Result<Vec<[f32; MEL_BINS]>, WakeError> {
        run_mel_session(&mut self.mel, samples)
    }

    /// Run the embedding model over the current 76-frame mel window → a 96-dim
    /// embedding.
    pub(crate) fn run_embedding(
        &mut self,
        mel_window: &VecDeque<[f32; MEL_BINS]>,
    ) -> Result<[f32; EMB_DIM], WakeError> {
        run_embedding_session(&mut self.embedding, mel_window)
    }
}

/// Embed [`cold_seed_noise`] the way openWakeWord 0.6.0's `_get_embeddings`
/// does: one mel pass over the whole clip, then [`EMB_WINDOW`]-frame windows at
/// stride [`EMB_STEP`] — the hard-coded stride upstream uses there, independent
/// of the streaming cadence — keeping the last [`WAKE_WINDOW`] of them, which is
/// the slice upstream's `get_features` hands the wake head.
///
/// Over the two sessions rather than over [`OwwModels`], so it can run before
/// one exists.
fn compute_cold_history(
    mel: &mut Session,
    embedding: &mut Session,
) -> Result<[[f32; EMB_DIM]; WAKE_WINDOW], WakeError> {
    let seed: Vec<f32> = cold_seed_noise().iter().map(|&s| f32::from(s)).collect();
    let frames = run_mel_session(mel, &seed)?;
    let first = cold_history_first_window(frames.len())?;
    let mut history = [[0.0; EMB_DIM]; WAKE_WINDOW];
    for (i, slot) in history.iter_mut().enumerate() {
        let start = first + i * EMB_STEP;
        let window: VecDeque<[f32; MEL_BINS]> =
            frames[start..start + EMB_WINDOW].iter().copied().collect();
        *slot = run_embedding_session(embedding, &window)?;
    }
    Ok(history)
}

/// Mel-frame offset of the first of the [`WAKE_WINDOW`] windows the cold
/// history keeps, for a seed clip whose mel pass produced `frames` frames:
/// [`EMB_WINDOW`]-frame windows at stride [`EMB_STEP`], the last sixteen of
/// them. Fewer than sixteen windows is [`WakeError::Inference`] — a model whose
/// framing cannot seed a history fails at load rather than running un-seeded.
///
/// Pure frame-count arithmetic, so the window selection and the shortfall are
/// checkable without a model.
fn cold_history_first_window(frames: usize) -> Result<usize, WakeError> {
    let windows = frames
        .checked_sub(EMB_WINDOW)
        .map_or(0, |span| span / EMB_STEP + 1);
    if windows < WAKE_WINDOW {
        return Err(inference(
            "mel",
            format!(
                "the {COLD_SEED_SAMPLES}-sample seed clip yielded {frames} frames, \
                 only {windows} of the {WAKE_WINDOW} windows the cold history needs"
            ),
        ));
    }
    Ok((windows - WAKE_WINDOW) * EMB_STEP)
}

/// The mel pass over one session: raw f32 samples → scaled mel frames
/// (`mel/10 + 2`). The model input is the raw sample magnitudes (openWakeWord
/// does not normalize to `[-1, 1]`).
fn run_mel_session(mel: &mut Session, samples: &[f32]) -> Result<Vec<[f32; MEL_BINS]>, WakeError> {
    let n = samples.len();
    let tensor = Tensor::from_array((vec![1_i64, n as i64], samples.to_vec()))
        .map_err(|e| inference("mel", e))?;
    let outputs = mel
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

/// The embedding pass over one session: a 76-frame mel window → a 96-dim
/// embedding.
fn run_embedding_session(
    embedding: &mut Session,
    mel_window: &VecDeque<[f32; MEL_BINS]>,
) -> Result<[f32; EMB_DIM], WakeError> {
    let mut flat = Vec::with_capacity(EMB_WINDOW * MEL_BINS);
    for frame in mel_window {
        flat.extend_from_slice(frame);
    }
    let tensor = Tensor::from_array((vec![1_i64, EMB_WINDOW as i64, MEL_BINS as i64, 1], flat))
        .map_err(|e| inference("embedding", e))?;
    let outputs = embedding
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

impl OwwModels {
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

/// One pod's rolling openWakeWord state. Drive it with
/// [`push`](OwwStream::push) as audio arrives, [`flush`](OwwStream::flush) at a
/// segment's trailing partial chunk, and [`reset`](OwwStream::reset) on a
/// discontinuity. [`arm`](OwwStream::arm) applies the threshold + refractory to a
/// scored step.
///
/// The invariant that keeps a placeholder out of the wake head: `emb_window` is
/// either empty — no chunk stepped since the last reset — or a full
/// [`WAKE_WINDOW`] of real embeddings, the first step installing
/// [`OwwModels::cold_history`] before it pushes its own. A partially-filled
/// window is a state the type never enters, and
/// [`run_wake`](OwwModels::run_wake) is reached only from
/// [`step`](OwwStream::step), after that install.
pub struct OwwStream {
    /// Last `MEL_LOOKBACK_SAMPLES` raw samples, prepended to the next chunk for
    /// mel left context. Empty at cold-start, so the first chunk is framed from
    /// its own first sample and yields 5 frames instead of 8.
    lookback: Vec<f32>,
    /// Real samples not yet forming a whole chunk.
    pending: VecDeque<f32>,
    /// Persistent 76-frame mel window (cold-started from [`MEL_COLD_FILL`]).
    mel_window: VecDeque<[f32; MEL_BINS]>,
    /// Persistent 16-embedding window. Empty until the first step installs the
    /// models' seed history in front of that step's own embedding; full from
    /// then on.
    emb_window: VecDeque<[f32; EMB_DIM]>,
    /// Number of complete audio chunks processed since the last reset.
    total_chunks: u64,
    /// Sigmoid threshold strictly above which `arm` fires.
    threshold: f32,
    /// No detection arms while `end_sample < refractory_until`.
    refractory_until: u64,
}

impl OwwStream {
    /// A fresh stream with rolling model state and the given wake threshold.
    /// Takes no models: the embedding history is installed by the first
    /// [`step`](OwwStream::step), which already holds them.
    pub fn new(threshold: f32) -> OwwStream {
        OwwStream {
            lookback: Vec::new(),
            pending: VecDeque::new(),
            mel_window: VecDeque::from(vec![[MEL_COLD_FILL; MEL_BINS]; EMB_WINDOW]),
            emb_window: VecDeque::new(),
            total_chunks: 0,
            threshold,
            refractory_until: 0,
        }
    }

    /// Clear all rolling state back to cold-start — empty embedding window, so
    /// the next step re-installs the seed history and the warm-up starts over.
    /// Called on a pod reconnect or a sample-index discontinuity so scoring
    /// never runs across a hole.
    ///
    /// Re-runs the constructor rather than clearing field by field: a field
    /// added to `OwwStream` cannot then be initialised in one place and
    /// forgotten in the other, which would leak state across a re-anchor.
    pub fn reset(&mut self) {
        *self = OwwStream::new(self.threshold);
    }

    /// Feed real PCM. Processes every whole chunk now available, reporting one
    /// [`ScoredChunk`] per processed chunk from the first chunk past
    /// [`WARMUP_CHUNKS`] — the first [`WAKE_READINESS_SAMPLES`] after a reset
    /// score none and are reported as the run's
    /// [`unscored`](PushReport::unscored) instead. A trailing partial chunk stays
    /// buffered for the next `push` or a `flush`.
    pub fn push(&mut self, models: &mut OwwModels, pcm: &[i16]) -> Result<PushReport, WakeError> {
        self.pending.extend(pcm.iter().map(|&s| f32::from(s)));
        let mut report = PushReport::default();
        while self.pending.len() >= CHUNK {
            let chunk: Vec<f32> = self.pending.drain(..CHUNK).collect();
            self.step(models, &chunk, &mut report)?;
        }
        Ok(report)
    }

    /// Whole chunks processed since the last reset. Test-only: the cursors a
    /// caller needs come off [`PushReport`], so nothing outside reconstructs
    /// them from a count.
    #[cfg(test)]
    pub(crate) fn chunks_processed(&self) -> u64 {
        self.total_chunks
    }

    /// The embedding window as it stands. Test-only: it lets the seed-then-shift
    /// order be pinned directly rather than inferred from scores.
    #[cfg(test)]
    pub(crate) fn embedding_window(&self) -> Vec<[f32; EMB_DIM]> {
        self.emb_window.iter().copied().collect()
    }

    /// Score a trailing partial chunk, zero-padded up to a whole chunk (the batch
    /// tail-padding). Reports the embedding step completed by the padded chunk;
    /// an empty report when the buffer is empty.
    pub fn flush(&mut self, models: &mut OwwModels) -> Result<PushReport, WakeError> {
        let mut report = PushReport::default();
        if self.pending.is_empty() {
            return Ok(report);
        }
        let mut chunk: Vec<f32> = self.pending.drain(..).collect();
        chunk.resize(CHUNK, 0.0);
        self.step(models, &chunk, &mut report)?;
        Ok(report)
    }

    /// Apply the threshold + refractory to a freshly-scored step. Fires (and
    /// re-arms the refractory) on a threshold crossing outside the refractory
    /// window; provenance is reported [`WAKE_END_LAG_SAMPLES`] before the
    /// scoring cursor.
    ///
    /// Readiness is not re-checked here. [`step`](OwwStream::step) is the single
    /// gate: no `ScoredChunk` exists at all for the warm-up chunks, so a second
    /// guard on this side could only ever disagree with the first.
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

    /// One chunk step: install the seed embedding history if this is the first
    /// step since a reset, mel over (lookback + chunk), append every frame the
    /// pass produced, and drive the embedding/wake windows once per processed
    /// chunk. Updates the rolling windows and lookback.
    ///
    /// Every frame is new: the lookback is shorter than [`MEL_STFT_WINDOW`], so
    /// it contributes no complete frame of its own and only supplies left
    /// context. That is what makes the count 5 for the first chunk and
    /// [`EMB_STEP`] for every chunk after.
    fn step(
        &mut self,
        models: &mut OwwModels,
        chunk: &[f32],
        report: &mut PushReport,
    ) -> Result<(), WakeError> {
        debug_assert_eq!(chunk.len(), CHUNK);
        if self.emb_window.is_empty() {
            self.emb_window.extend(models.cold_history.iter().copied());
        }
        let mut input = Vec::with_capacity(self.lookback.len() + chunk.len());
        input.extend_from_slice(&self.lookback);
        input.extend_from_slice(chunk);

        let frames = models.run_mel(&input)?;
        for frame in &frames {
            self.mel_window.pop_front();
            self.mel_window.push_back(*frame);
        }
        self.total_chunks += 1;
        let emb = models.run_embedding(&self.mel_window)?;
        self.emb_window.pop_front();
        self.emb_window.push_back(emb);
        let end_sample = self.total_chunks * CHUNK as u64;
        if self.total_chunks > WARMUP_CHUNKS {
            let score = models.run_wake(&self.emb_window)?;
            report.scored.push(ScoredChunk { score, end_sample });
        } else {
            report.note_unscored(end_sample);
        }

        let keep = input.len().min(MEL_LOOKBACK_SAMPLES);
        self.lookback = input[input.len() - keep..].to_vec();
        Ok(())
    }
}

impl PushReport {
    /// Extend the run of unscored chunks to one ending at `chunk_end`.
    fn note_unscored(&mut self, chunk_end: u64) {
        match &mut self.unscored {
            Some(run) => {
                run.chunks += 1;
                run.last_chunk_end = chunk_end;
            }
            None => {
                self.unscored = Some(UnscoredRun {
                    chunks: 1,
                    first_chunk_end: chunk_end,
                    last_chunk_end: chunk_end,
                });
            }
        }
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
        QUIET_NOISE_AMPLITUDE, QUIET_NOISE_SAMPLES, QUIET_NOISE_SHA256, oww_config as test_config,
        oww_model_dir, oww_models, quiet_noise_pcm, seeded_noise, wake_phrase_pcm,
    };

    /// SHA-256 of a clip as little-endian S16 bytes — the form both sides of the
    /// oracle hash, so a digest asserted here is comparable to the script's.
    fn sha256_s16(pcm: &[i16]) -> String {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        for sample in pcm {
            hasher.update(sample.to_le_bytes());
        }
        format!("{:x}", hasher.finalize())
    }

    /// Relative agreement the two implementations of the chunked pipeline are
    /// held to. Both sides feed the same ONNX graphs the same 1 280-sample
    /// chunks, so the gap is float reassociation between two onnxruntime builds,
    /// nothing about framing. Measured across the three captured fixtures: worst
    /// relative disagreement 4.4e-6 on the scores above 1e-3.
    ///
    /// Relative, not absolute: most pinned scores are below 1e-4, and a single
    /// absolute bound wide enough for the peak (the old 1e-3) is satisfied by
    /// *any* score below 1e-3 — which is the whole cold-start range these pins
    /// cover.
    const ORACLE_REL_TOLERANCE: f32 = 1e-4;

    /// Floor under the relative bound. The two runtimes disagree by a roughly
    /// constant amount in the head's pre-sigmoid output, so at scores near zero
    /// — where the sigmoid is its own exponent — that shows up as a relative
    /// error of a percent or two on an absolute difference under 1e-7 (worst
    /// measured: 9.0e-8, at a pinned 1.5e-5). The floor covers that range and
    /// still holds the settled silence score, ~5e-6, to about a tenth of itself.
    const ORACLE_ABS_FLOOR: f32 = 5e-7;

    /// Check a run's scores against a table captured from the Python oracle.
    ///
    /// Every scored chunk must appear in the table and every table row must have
    /// been scored: a pin that quietly skipped rows would let the warm-up
    /// boundary or the chunk cadence move without failing anything.
    fn assert_matches_oracle(scored: &[ScoredChunk], expected: &[(u64, f32)]) {
        assert_eq!(
            scored.len(),
            expected.len(),
            "scored {} chunks against a table of {}",
            scored.len(),
            expected.len()
        );
        for (sc, (end_sample, expected_score)) in scored.iter().zip(expected) {
            assert_eq!(
                sc.end_sample, *end_sample,
                "step cursor: expected {end_sample}, got {}",
                sc.end_sample
            );
            let bound = (ORACLE_REL_TOLERANCE * expected_score.abs()).max(ORACLE_ABS_FLOOR);
            assert!(
                (sc.score - expected_score).abs() <= bound,
                "score at {end_sample}: expected {expected_score} +/- {bound}, got {}",
                sc.score
            );
        }
    }

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
    /// fill, the 5-then-8 cadence, the seed history, the warm-up), so it can
    /// only catch a streaming/batch *self*-consistency break — a chunking bug in
    /// `push`/`flush` — never a wrong choice made in both places. The external
    /// ground truth is `python_per_step_scores_regression`, which pins values
    /// produced by openWakeWord 0.6.0 rather than by this file.
    fn whole_segment_max(models: &mut OwwModels, pcm: &[i16]) -> Option<f32> {
        let mut mel_window: VecDeque<[f32; MEL_BINS]> =
            VecDeque::from(vec![[MEL_COLD_FILL; MEL_BINS]; EMB_WINDOW]);
        let mut emb_window: VecDeque<[f32; EMB_DIM]> =
            models.cold_history.iter().copied().collect();
        let mut samples: Vec<f32> = pcm.iter().map(|&s| f32::from(s)).collect();
        let target = samples.len().max(1).div_ceil(CHUNK) * CHUNK;
        samples.resize(target, 0.0);

        let frames = models.run_mel(&samples).unwrap();
        // The first chunk contributes `mel_frame_count(CHUNK)` frames; every
        // chunk after contributes `EMB_STEP`. An embedding fires on each.
        let first = mel_frame_count(CHUNK);
        let mut frame_count = 0usize;
        let mut embeddings = 0u64;
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
                embeddings += 1;
                if embeddings > WARMUP_CHUNKS {
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
        for sc in stream.push(models, pcm).unwrap().scored {
            fold(&mut best, sc.score);
        }
        for sc in stream.flush(models).unwrap().scored {
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

    /// Streaming reproduces the whole-segment pass on full-scale noise: both
    /// reject, and the two maxima agree to well within the framing difference
    /// between them.
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
        // What makes the bound below mean anything: this clip drives the head
        // well up its range (measured 0.17), so agreement to 1e-3 is agreement
        // to under a percent of the value. Were the maximum to collapse toward
        // zero — a fixture change, a head swap — the comparison would pass on
        // any two small numbers, so the premise is asserted rather than assumed.
        assert!(
            reference > 0.05,
            "the reference score {reference} is too small for the bound below to constrain anything"
        );
        assert!(
            // Not bit-equality: `MEL_LOOKBACK_SAMPLES` (480) is shorter than
            // `MEL_STFT_WINDOW` (640), so a streamed frame sees less left
            // context than the same frame of one contiguous pass. On this clip
            // that costs 5e-7; the bound is loose enough not to pin a float
            // detail and tight enough to catch a chunking regression.
            (reference - streamed).abs() < 1e-3,
            "streaming score {streamed} diverges from batch {reference}"
        );
    }

    /// Scores are finite and in `[0, 1]`; a 32 000-sample feed produces one score
    /// per processed chunk (none from the warm-up chunks), each end-sample a
    /// whole chunk further along.
    ///
    /// The one unit case where a single `push` straddles the warm-up boundary,
    /// which is the live path — the listener pushes whatever a frame carries —
    /// so the run's cursors are asserted here rather than only through the
    /// runtime's accounting one level up.
    #[test]
    fn push_scores_on_the_embedding_cadence() {
        let mut models = test_models();
        let mut stream = OwwStream::new(0.5);
        let pcm = seeded_noise(2, 32_000);
        let report = stream.push(&mut models, &pcm).unwrap();
        assert_eq!(
            report.unscored,
            Some(UnscoredRun {
                chunks: WARMUP_CHUNKS as u32,
                first_chunk_end: CHUNK as u64,
                last_chunk_end: WARMUP_CHUNKS * CHUNK as u64,
            }),
            "the warm-up run this push consumed before it started scoring"
        );
        let scored = report.scored;
        let chunk_count = 32_000_usize.div_ceil(CHUNK);
        assert_eq!(
            scored.len(),
            chunk_count.saturating_sub(WARMUP_CHUNKS as usize),
            "one score per chunk past the {WARMUP_CHUNKS} warm-up chunks over {chunk_count} chunks"
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
                WAKE_READINESS_SAMPLES + i as u64 * CHUNK as u64,
                "score {i} window-end cursor"
            );
        }
    }

    /// Wake scoring starts on the first chunk past the warm-up, at that chunk's
    /// own sample cursor — upstream's five suppressed predictions, reproduced by
    /// producing no `ScoredChunk` at all for them.
    #[test]
    fn first_score_waits_for_warmup() {
        let mut models = test_models();
        let mut stream = OwwStream::new(0.5);
        let warmup = WARMUP_CHUNKS as usize;

        let warm = stream
            .push(&mut models, &seeded_noise(8, warmup * CHUNK))
            .unwrap();
        assert!(
            warm.scored.is_empty(),
            "the first {WARMUP_CHUNKS} chunks are suppressed"
        );
        assert_eq!(
            warm.unscored,
            Some(UnscoredRun {
                chunks: WARMUP_CHUNKS as u32,
                first_chunk_end: CHUNK as u64,
                last_chunk_end: WARMUP_CHUNKS * CHUNK as u64,
            }),
            "and the stream reports the run it consumed, both ends of it"
        );
        let report = stream.push(&mut models, &seeded_noise(9, CHUNK)).unwrap();
        assert_eq!(report.scored.len(), 1, "the next chunk scores");
        assert_eq!(
            report.scored[0].end_sample, WAKE_READINESS_SAMPLES,
            "the first score ends at the readiness cursor"
        );
        assert_eq!(
            report.unscored, None,
            "a warmed push consumes nothing blind"
        );
    }

    /// The seed clip is exactly the bytes both this crate and the Python capture
    /// script hash before they use it: a drift in either copy of the generator
    /// fails here rather than quietly re-blessing different scores.
    #[test]
    fn cold_seed_matches_pinned_digest() {
        let seed = cold_seed_noise();
        assert_eq!(seed.len(), COLD_SEED_SAMPLES, "seed clip length");
        assert!(
            seed.iter()
                .all(|&s| (-COLD_SEED_AMPLITUDE..COLD_SEED_AMPLITUDE).contains(&s)),
            "every sample is within the upstream amplitude bound"
        );
        assert_eq!(
            sha256_s16(&seed),
            COLD_SEED_SHA256,
            "the seed clip's bytes moved; the oracle's pinned scores no longer describe this stream"
        );
    }

    /// The cold history is a function of the models and the seed clip alone: two
    /// loads produce the same one, so a stream's starting point does not depend
    /// on which `OwwModels` it happens to be driven by.
    #[test]
    fn cold_history_is_deterministic() {
        let a = test_models();
        let b = test_models();
        assert_eq!(a.cold_history, b.cold_history);
        assert!(
            a.cold_history.iter().flatten().any(|v| *v != 0.0),
            "a history of zeros would be the placeholder this replaces"
        );
    }

    /// The embedding a cold stream's first chunk must produce, re-derived from
    /// the models: the mel pass over the chunk alone (no lookback yet) appended
    /// to a [`MEL_COLD_FILL`] window, embedded once.
    ///
    /// Named provenance, not a value: it is what lets the seed-install test
    /// assert *which* embedding lands in the window's last slot rather than
    /// merely that something other than the oldest seed entry did.
    fn first_chunk_embedding(models: &mut OwwModels, pcm: &[i16]) -> [f32; EMB_DIM] {
        assert_eq!(pcm.len(), CHUNK, "the first step consumes one whole chunk");
        let mut mel_window: VecDeque<[f32; MEL_BINS]> =
            VecDeque::from(vec![[MEL_COLD_FILL; MEL_BINS]; EMB_WINDOW]);
        let samples: Vec<f32> = pcm.iter().map(|&s| f32::from(s)).collect();
        for frame in models.run_mel(&samples).unwrap() {
            mel_window.pop_front();
            mel_window.push_back(frame);
        }
        models.run_embedding(&mel_window).unwrap()
    }

    /// Which sixteen windows of the seed clip become the cold history, at the
    /// boundaries of the arithmetic that picks them: exactly enough frames for
    /// sixteen windows starts at 0, the committed clip's 397 frames start at
    /// 200 (upstream's window starts 200, 208, …, 320), and a frame count one
    /// window short — or below a single window — is a shortfall that names both
    /// counts rather than seeding a stream from whatever it has.
    #[test]
    fn cold_history_window_arithmetic_holds_at_its_boundaries() {
        for (frames, first) in [
            (EMB_WINDOW + (WAKE_WINDOW - 1) * EMB_STEP, 0),
            (396, 200),
            (397, 200),
        ] {
            assert_eq!(
                cold_history_first_window(frames).expect("enough frames to seed"),
                first,
                "{frames} frames"
            );
        }
        assert_eq!(
            cold_history_first_window(mel_frame_count(COLD_SEED_SAMPLES))
                .expect("the seed clip seeds"),
            200,
            "the committed seed clip's own frame count"
        );

        for frames in [0, EMB_WINDOW - 1, EMB_WINDOW, 195] {
            match cold_history_first_window(frames) {
                Err(WakeError::Inference(msg)) => {
                    assert!(
                        msg.contains(&format!("{frames} frames")),
                        "the shortfall names the frames it got: {msg}"
                    );
                    assert!(
                        msg.contains(&format!("of the {WAKE_WINDOW} windows")),
                        "and the number it needed: {msg}"
                    );
                }
                other => panic!("{frames} frames cannot seed a history, got {other:?}"),
            }
        }
    }

    /// The seed-then-shift order, pinned directly: a fresh stream holds nothing,
    /// and its first step leaves the seed history minus its oldest entry with
    /// that step's own embedding at the end. Same after a reset.
    ///
    /// The last slot is checked against the embedding the models actually
    /// produce for that chunk, not merely against the seed entry it displaced:
    /// a placeholder, a duplicated seed entry or an uninitialised slot all
    /// differ from the displaced entry too, and a placeholder reaching the wake
    /// head is the regression this seeding exists to prevent.
    #[test]
    fn first_step_installs_seed_history() {
        let mut models = test_models();
        let history = models.cold_history;
        let mut stream = OwwStream::new(0.5);
        assert_eq!(stream.chunks_processed(), 0);
        assert!(
            stream.embedding_window().is_empty(),
            "nothing before a step"
        );

        let pcm = seeded_noise(11, CHUNK);
        let expected = first_chunk_embedding(&mut models, &pcm);
        assert!(
            expected.iter().any(|v| *v != 0.0),
            "the re-derived embedding is not itself a placeholder"
        );
        assert_ne!(expected, history[0], "nor a copy of the displaced seed");

        let check_one_step = |stream: &mut OwwStream, models: &mut OwwModels| {
            stream.push(models, &pcm).unwrap();
            let window = stream.embedding_window();
            assert_eq!(
                window.len(),
                WAKE_WINDOW,
                "the window is full after one step"
            );
            assert_eq!(
                &window[..WAKE_WINDOW - 1],
                &history[1..],
                "the seed history shifted by one"
            );
            assert_eq!(
                window[WAKE_WINDOW - 1],
                expected,
                "the last slot holds this chunk's own embedding"
            );
        };

        check_one_step(&mut stream, &mut models);
        stream.reset();
        assert!(
            stream.embedding_window().is_empty(),
            "reset returns the window to empty"
        );
        check_one_step(&mut stream, &mut models);
    }

    /// The oracle's scores over the 32 000 samples of digital silence that open
    /// both the wake-phrase fixture and the silence fixture: chunks 6..=25, the
    /// stream's cold start scored step for step.
    ///
    /// Named once and shared by both tables below, because they are the same
    /// measurement: a drift in the cold start — the failure this seeding exists
    /// to prevent — then fails both tests and can only be re-blessed in one
    /// place, rather than being pasted stale into the longer table.
    ///
    /// Captured with `host/tools/oww-oracle/capture_scores.py --fixture silence`
    /// over the committed models.
    const SILENT_PREFIX_SCORES: [(u64, f32); 20] = [
        (7_680, 0.000002563),
        (8_960, 0.000003457),
        (10_240, 0.000006527),
        (11_520, 0.000013530),
        (12_800, 0.000015259),
        (14_080, 0.000028193),
        (15_360, 0.000025392),
        (16_640, 0.000019789),
        (17_920, 0.000029117),
        (19_200, 0.000022173),
        (20_480, 0.000010639),
        (21_760, 0.000005692),
        (23_040, 0.000004858),
        (24_320, 0.000004649),
        (25_600, 0.000004619),
        (26_880, 0.000004381),
        (28_160, 0.000004292),
        (29_440, 0.000004351),
        (30_720, 0.000004858),
        (32_000, 0.000005037),
    ];

    /// What digital silence settles to once the seed embeddings have left the
    /// window: every chunk from 26 on scores the same value, so the silence
    /// table names it once instead of repeating it 25 times.
    const SETTLED_SILENCE_SCORE: f32 = 0.000005037;

    /// Pins every scored step on the committed phrase behind silence against
    /// Python openWakeWord 0.6.0 `Model.predict`, chunk for chunk: the shape of
    /// the whole climb, not just its peak, so a front end that drifts early and
    /// still happens to cross the threshold late fails here.
    ///
    /// Captured with `host/tools/oww-oracle/capture_scores.py --fixture
    /// wake-phrase --wake-phrase-wav host/testdata/wake/wake-phrase.wav` over
    /// the committed models; its README says what forces a re-capture.
    #[test]
    fn python_per_step_scores_regression() {
        let mut models = test_models();
        let mut stream = OwwStream::new(0.5);
        let mut pcm = vec![0_i16; 32_000];
        pcm.extend(wake_phrase_pcm());
        let mut scored = stream.push(&mut models, &pcm).unwrap().scored;
        scored.extend(stream.flush(&mut models).unwrap().scored);
        // The phrase's own climb, from where the fixture's leading silence ends.
        let phrase = [
            (33_280, 0.000002891),
            (34_560, 0.000002235),
            (35_840, 0.000001907),
            (37_120, 0.000001818),
            (38_400, 0.000001639),
            (39_680, 0.000002205),
            (40_960, 0.000009954),
            (42_240, 0.000315994),
            (43_520, 0.022298783),
            (44_800, 0.101_052_37),
            (46_080, 0.197_546_42),
            (47_360, 0.609_890_8),
            (48_640, 0.852_638_7),
            (49_920, 0.578_628_06),
            (51_200, 0.989_247_8),
            (52_480, 0.983_769_95),
            (53_760, 0.994_896_65),
        ];
        let expected: Vec<(u64, f32)> =
            SILENT_PREFIX_SCORES.iter().copied().chain(phrase).collect();
        assert_matches_oracle(&scored, &expected);
    }

    /// Digital silence, the input the cold-start bug turned into a 0.9999
    /// detection on another head: every scored step pinned against the oracle,
    /// and — independently of the table, because a re-capture could move it —
    /// nothing anywhere near a detection, before or after a reset.
    ///
    /// Captured with `host/tools/oww-oracle/capture_scores.py --fixture
    /// silence` over the committed models.
    #[test]
    fn python_silence_scores_regression() {
        let mut models = test_models();
        let mut stream = OwwStream::new(0.5);
        // Chunks 6..=25 are the shared cold start; 26..=50 are the settled tail
        // of a clip that is silence all the way through.
        let expected: Vec<(u64, f32)> = SILENT_PREFIX_SCORES
            .iter()
            .copied()
            .chain((26..=50).map(|chunk: u64| (chunk * CHUNK as u64, SETTLED_SILENCE_SCORE)))
            .collect();
        let silence = vec![0_i16; COLD_SEED_SAMPLES];

        for pass in ["cold", "after reset"] {
            let scored = stream.push(&mut models, &silence).unwrap().scored;
            assert_matches_oracle(&scored, &expected);
            for sc in &scored {
                assert!(
                    sc.score < 0.01,
                    "{pass}: silence scored {} at {}",
                    sc.score,
                    sc.end_sample
                );
                assert!(
                    stream.arm(sc).is_none(),
                    "{pass}: silence armed at {}",
                    sc.end_sample
                );
            }
            stream.reset();
        }
    }

    /// Quiet uniform noise at the level of a quiet room: pinned against the
    /// oracle step for step, and never close to arming. The seeded history is
    /// itself noise, so this is where a head that reacts to its own cold start
    /// would show up as a score that keeps climbing instead of settling.
    ///
    /// Level-matched only — it is not a room recording, and no room recording
    /// enters this tree. The durable check is the pin; a real capture is run
    /// through the same script by hand.
    ///
    /// Captured with `host/tools/oww-oracle/capture_scores.py --fixture
    /// quiet-noise` over the committed models.
    #[test]
    fn quiet_noise_never_arms() {
        let mut models = test_models();
        let mut stream = OwwStream::new(0.5);
        let expected = [
            (7_680, 0.000003427),
            (8_960, 0.000012606),
            (10_240, 0.000103831),
            (11_520, 0.000860631),
            (12_800, 0.001726121),
            (14_080, 0.004702747),
            (15_360, 0.006756723),
            (16_640, 0.006465554),
            (17_920, 0.017_624_23),
            (19_200, 0.040_995_09),
            (20_480, 0.014019519),
            (21_760, 0.000898659),
            (23_040, 0.000226647),
            (24_320, 0.000097662),
            (25_600, 0.000077516),
            (26_880, 0.000063598),
            (28_160, 0.000036955),
            (29_440, 0.000039160),
            (30_720, 0.000019312),
            (32_000, 0.000013590),
            (33_280, 0.000008404),
            (34_560, 0.000011951),
            (35_840, 0.000018835),
            (37_120, 0.000025332),
            (38_400, 0.000039846),
            (39_680, 0.000057817),
            (40_960, 0.000040531),
            (42_240, 0.000024974),
            (43_520, 0.000026226),
            (44_800, 0.000025094),
            (46_080, 0.000026315),
            (47_360, 0.000033617),
            (48_640, 0.000045478),
            (49_920, 0.000079632),
            (51_200, 0.000137925),
            (52_480, 0.000124246),
            (53_760, 0.000106305),
            (55_040, 0.000105709),
            (56_320, 0.000134289),
            (57_600, 0.000150889),
            (58_880, 0.000105023),
            (60_160, 0.000054926),
            (61_440, 0.000030249),
            (62_720, 0.000021309),
            (64_000, 0.000013262),
        ];
        let scored = stream.push(&mut models, &quiet_noise_pcm()).unwrap().scored;
        assert_matches_oracle(&scored, &expected);
        for sc in &scored {
            assert!(
                stream.arm(sc).is_none(),
                "quiet noise armed at {} with {}",
                sc.end_sample,
                sc.score
            );
        }
    }

    /// The quiet-noise stand-in is exactly the clip the capture script
    /// generates: same length, same amplitude bound, same bytes. Drift in either
    /// copy of the generator fails here rather than moving the scores pinned
    /// above under everyone.
    #[test]
    fn quiet_noise_matches_pinned_digest() {
        let pcm = quiet_noise_pcm();
        assert_eq!(pcm.len(), QUIET_NOISE_SAMPLES, "quiet-noise clip length");
        assert!(
            pcm.iter()
                .all(|&s| (-QUIET_NOISE_AMPLITUDE..QUIET_NOISE_AMPLITUDE).contains(&s)),
            "every sample is within the stand-in's amplitude bound"
        );
        assert_eq!(
            sha256_s16(&pcm),
            QUIET_NOISE_SHA256,
            "the quiet-noise clip's bytes moved; `quiet_noise_never_arms`'s pins no longer describe it"
        );
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

        let mut scored = stream.push(&mut models, &pcm).unwrap().scored;
        scored.extend(stream.flush(&mut models).unwrap().scored);
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
        assert_eq!(
            stream.push(&mut models, &pcm).unwrap(),
            PushReport::default(),
            "100 samples is under one chunk: nothing scored, nothing consumed"
        );
        let flushed = stream.flush(&mut models).unwrap();
        assert!(
            flushed.scored.is_empty(),
            "the padded chunk is the first of the {WARMUP_CHUNKS} warm-up chunks"
        );
        assert_eq!(
            flushed.unscored.map(|run| run.chunks),
            Some(1),
            "and it is reported as consumed"
        );
        assert_eq!(
            stream.push(&mut models, &[]).unwrap(),
            PushReport::default()
        );
    }

    /// Chunks feed identically whether delivered whole or split at ragged offsets:
    /// the pending buffer stitches the split, so the scores match.
    #[test]
    fn split_pushes_match_single_push() {
        let mut models = test_models();
        let pcm = seeded_noise(5, 4 * CHUNK);

        let mut whole = OwwStream::new(0.5);
        let one_shot = whole.push(&mut models, &pcm).unwrap().scored;

        let mut split = OwwStream::new(0.5);
        let mut split_scores = Vec::new();
        for part in pcm.chunks(700) {
            split_scores.extend(split.push(&mut models, part).unwrap().scored);
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
            after_reset.unscored, fresh_scores.unscored,
            "including the warm-up run each reports"
        );
        assert_eq!(
            after_reset, fresh_scores,
            "reset must clear all rolling state"
        );
    }

    #[test]
    fn reset_clears_wake_readiness() {
        let mut models = test_models();
        let mut stream = OwwStream::new(-1.0);
        let warmup = WARMUP_CHUNKS as usize;
        assert_eq!(
            stream
                .push(&mut models, &seeded_noise(8, (warmup + 1) * CHUNK))
                .unwrap()
                .scored
                .len(),
            1,
            "a warmed stream scores"
        );
        stream.reset();
        assert!(
            stream
                .push(&mut models, &seeded_noise(9, warmup * CHUNK))
                .unwrap()
                .scored
                .is_empty()
        );
        let scored = stream
            .push(&mut models, &seeded_noise(10, CHUNK))
            .unwrap()
            .scored;
        assert_eq!(
            scored.len(),
            1,
            "reset makes the stream serve its warm-up again"
        );
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
            .unwrap()
            .scored;
        assert!(
            scored.is_empty(),
            "under {WAKE_READINESS_SAMPLES} samples cannot be scored: {scored:?}"
        );
    }

    /// A reset drops the sub-chunk remainder `push` left buffered, so the next
    /// chunk boundary is counted from the re-anchor rather than from audio on
    /// the far side of the hole.
    ///
    /// The feed is sized to tell the two apart: 6 980 samples after the reset
    /// is five whole chunks and a remainder, which scores nothing. Carried
    /// across, the stale 700 samples would complete a sixth chunk and produce a
    /// score — one whose cursor names audio the stream was re-anchored away
    /// from.
    #[test]
    fn reset_discards_the_buffered_partial_chunk() {
        let mut models = test_models();
        let mut stream = OwwStream::new(0.5);
        let stale = 700_usize;
        assert!(
            stream
                .push(&mut models, &seeded_noise(12, stale))
                .unwrap()
                .scored
                .is_empty(),
            "a sub-chunk push buffers and scores nothing"
        );

        stream.reset();
        let after = (WARMUP_CHUNKS as usize + 1) * CHUNK - stale;
        let report = stream.push(&mut models, &seeded_noise(13, after)).unwrap();
        assert!(
            report.scored.is_empty(),
            "the stale remainder must not complete a sixth chunk: {:?}",
            report.scored
        );
        assert_eq!(
            report.unscored.map(|run| run.last_chunk_end),
            Some(WARMUP_CHUNKS * CHUNK as u64),
            "and the chunk cursor counts from the reset, not from the stale audio"
        );
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
