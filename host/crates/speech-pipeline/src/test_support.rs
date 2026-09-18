//! Shared test fixtures: the committed wake-phrase audio, the device-shaped
//! primed wake fixture, deterministic noise, and the OWW/Silero model loaders.
//! One home so a fixture-path or spec change lands in a single place — and so
//! every consumer gets the same spec assertions on the wake fixture, not a
//! silently assertion-stripped copy.
//!
//! Compiled for this crate's own tests and, behind the `test-util` feature, for
//! the integration tests of crates downstream of it: the preroll shape a fixture
//! has to carry is one definition, not one per crate.

use std::path::PathBuf;
use std::sync::OnceLock;

use crate::listener::oww_stream::{
    LCG_INCREMENT, LCG_MULTIPLIER, OwwConfig, OwwModels, bounded_lcg_noise,
};
use crate::listener::silero::{SileroConfig, SileroModel};
use crate::types::SPINE_FORMAT;

/// The `speech-pipeline` crate dir (`host/crates/speech-pipeline`); models and
/// testdata hang off `host/`.
fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Directory of the committed openWakeWord ONNX models (`host/models/oww`).
pub fn oww_model_dir() -> PathBuf {
    crate_dir().join("../../models/oww")
}

/// An [`OwwConfig`] over the committed models at the given wake threshold.
pub fn oww_config(threshold: f32) -> OwwConfig {
    let dir = oww_model_dir();
    OwwConfig {
        melspectrogram: dir.join("melspectrogram.onnx"),
        embedding: dir.join("embedding_model.onnx"),
        model: dir.join("hey_jarvis_v0.1.onnx"),
        threshold,
    }
}

/// The three committed openWakeWord sessions, loaded.
pub fn oww_models() -> OwwModels {
    OwwModels::load(&oww_config(0.5)).expect("load committed oww models")
}

/// The committed Silero VAD session, loaded.
pub fn silero_model() -> SileroModel {
    let path = crate_dir().join("../../models/silero/silero_vad.onnx");
    SileroModel::load(&SileroConfig { model: path }).expect("load committed silero model")
}

/// The committed 16 kHz mono S16 "Hey Jarvis" TTS fixture as PCM. The spec
/// asserts fail loudly on a mis-encoded fixture rather than scoring garbage.
///
/// Decoded once per process: the fixture feeds most of the wake tests, and
/// re-reading it per call is a file read inside every one of them.
pub fn wake_phrase_pcm() -> Vec<i16> {
    static CLIP: OnceLock<Vec<i16>> = OnceLock::new();
    CLIP.get_or_init(|| {
        let path = crate_dir().join("../../testdata/wake/wake-phrase.wav");
        let mut reader = hound::WavReader::open(&path).expect("open wake-phrase fixture");
        let spec = reader.spec();
        assert_eq!(spec.channels, 1, "fixture must be mono");
        assert_eq!(
            spec.sample_rate, SPINE_FORMAT.sample_rate_hz,
            "fixture must be at the spine rate"
        );
        assert_eq!(spec.bits_per_sample, 16, "fixture must be S16");
        reader.samples::<i16>().map(|s| s.unwrap()).collect()
    })
    .clone()
}

/// Digital silence a real-audio fixture puts in front of the wake phrase: the
/// device's VAD-onset preroll itself, so the fixture models what a device sends
/// rather than restating a number. A fixture that omits it gives the wake stream
/// a cold start no real segment produces.
pub const WAKE_PREROLL_SAMPLES: usize = audio_pipeline::ring::PREROLL_SAMPLES as usize;

/// The committed wake phrase behind [`WAKE_PREROLL_SAMPLES`] of silence — the
/// shape a live segment carries. One definition for every crate that needs it:
/// the preroll length carries a safety argument (the compile-time
/// `WAKE_READINESS_SAMPLES <= PREROLL_SAMPLES` pin), and a second copy of the
/// fixture is a second place for that argument to be updated in.
pub fn primed_wake_pcm() -> Vec<i16> {
    let phrase = wake_phrase_pcm();
    let mut pcm = Vec::with_capacity(WAKE_PREROLL_SAMPLES + phrase.len());
    pcm.extend(std::iter::repeat_n(0_i16, WAKE_PREROLL_SAMPLES));
    pcm.extend_from_slice(&phrase);
    pcm
}

/// Samples in the quiet-noise stand-in: 4 s of spine audio.
pub const QUIET_NOISE_SAMPLES: usize = 4 * SPINE_FORMAT.sample_rate_hz as usize;

/// Amplitude bound of the quiet-noise stand-in, uniform over
/// `[-QUIET_NOISE_AMPLITUDE, QUIET_NOISE_AMPLITUDE)`. Uniform noise has RMS
/// `amplitude / sqrt(3)`, so 87 puts the clip at RMS ~= 50 — the level of the
/// quiet room audio a wake head has to sit through without arming. It matches
/// that audio in level only, not in spectrum: a level-matched sanity pin, not a
/// stand-in for a real capture.
pub const QUIET_NOISE_AMPLITUDE: i16 = 87;

/// Seed of the LCG [`quiet_noise_pcm`] draws from.
const QUIET_NOISE_LCG_SEED: u64 = 54_321;

/// SHA-256 of [`quiet_noise_pcm`]'s output as little-endian S16 bytes, pinned
/// on both sides of the oracle: `host/tools/oww-oracle` generates the same clip
/// and refuses to run when its copy of the generator has drifted from this one.
pub const QUIET_NOISE_SHA256: &str =
    "d61a8caaccba050630e7ad191d2ecdf8e4cd138099176ae89fa567bd3cef9513";

/// The quiet-noise stand-in: 4 s of uniform noise at RMS ~= 50, from the same
/// generator as the wake stream's cold seed with its own seed and
/// amplitude. The recurrence, the seed and the mapping are part of the oracle
/// contract: change any of them and the scores pinned against this clip change
/// with them.
pub fn quiet_noise_pcm() -> Vec<i16> {
    bounded_lcg_noise(
        QUIET_NOISE_LCG_SEED,
        QUIET_NOISE_SAMPLES,
        QUIET_NOISE_AMPLITUDE,
    )
}

/// Deterministic pseudo-random S16 noise (the same LCG recurrence, no `rand`
/// dep, exactly reproducible below-threshold assertions). Unbounded: the raw
/// extraction truncated to `i16`, which is why it is not
/// [`bounded_lcg_noise`] with a full-scale amplitude.
pub fn seeded_noise(seed: u64, n: usize) -> Vec<i16> {
    let mut state = seed;
    (0..n)
        .map(|_| {
            state = state
                .wrapping_mul(LCG_MULTIPLIER)
                .wrapping_add(LCG_INCREMENT);
            (state >> 33) as i16
        })
        .collect()
}
