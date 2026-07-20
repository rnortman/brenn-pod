//! Shared test fixtures: the committed wake-phrase audio, deterministic noise,
//! and the OWW/Silero model loaders. One home so a fixture-path or spec change
//! lands in a single place — and so every consumer gets the same spec assertions
//! on the wake fixture, not a silently assertion-stripped copy.

use std::path::PathBuf;

use crate::listener::oww_stream::{OwwConfig, OwwModels};
use crate::listener::silero::{SileroConfig, SileroModel};

/// The `speech-pipeline` crate dir (`host/crates/speech-pipeline`); models and
/// testdata hang off `host/`.
fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Directory of the committed openWakeWord ONNX models (`host/models/oww`).
pub(crate) fn oww_model_dir() -> PathBuf {
    crate_dir().join("../../models/oww")
}

/// An [`OwwConfig`] over the committed models at the given wake threshold.
pub(crate) fn oww_config(threshold: f32) -> OwwConfig {
    let dir = oww_model_dir();
    OwwConfig {
        melspectrogram: dir.join("melspectrogram.onnx"),
        embedding: dir.join("embedding_model.onnx"),
        model: dir.join("hey_jarvis_v0.1.onnx"),
        threshold,
    }
}

/// The three committed openWakeWord sessions, loaded.
pub(crate) fn oww_models() -> OwwModels {
    OwwModels::load(&oww_config(0.5)).expect("load committed oww models")
}

/// The committed Silero VAD session, loaded.
pub(crate) fn silero_model() -> SileroModel {
    let path = crate_dir().join("../../models/silero/silero_vad.onnx");
    SileroModel::load(&SileroConfig { model: path }).expect("load committed silero model")
}

/// The committed 16 kHz mono S16 "Hey Jarvis" TTS fixture as PCM. The spec
/// asserts fail loudly on a mis-encoded fixture rather than scoring garbage.
pub(crate) fn wake_phrase_pcm() -> Vec<i16> {
    let path = crate_dir().join("../../testdata/wake/wake-phrase.wav");
    let mut reader = hound::WavReader::open(&path).expect("open wake-phrase fixture");
    let spec = reader.spec();
    assert_eq!(spec.channels, 1, "fixture must be mono");
    assert_eq!(spec.sample_rate, 16_000, "fixture must be 16 kHz");
    assert_eq!(spec.bits_per_sample, 16, "fixture must be S16");
    reader.samples::<i16>().map(|s| s.unwrap()).collect()
}

/// Deterministic pseudo-random S16 noise (a small LCG — no `rand` dep, exactly
/// reproducible below-threshold assertions).
pub(crate) fn seeded_noise(seed: u64, n: usize) -> Vec<i16> {
    let mut state = seed;
    (0..n)
        .map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as i16
        })
        .collect()
}
