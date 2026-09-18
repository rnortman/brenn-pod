#!/usr/bin/env python3
"""Capture per-chunk openWakeWord scores from the Python implementation.

This is the oracle the Rust streaming core is pinned against: it drives
openWakeWord 0.6.0's `Model.predict` over 1,280-sample chunks of a fixture and
prints one JSON object per chunk, so the numbers in `oww_stream.rs`'s regression
tables have a procedure behind them instead of a one-off capture.

It starts the wake model from the same cold state the Rust stream does: the
embedding history is overwritten with the embeddings of a fixed low-amplitude
noise clip (stock openWakeWord draws a fresh unseeded clip on every
construction and reset, which no test could pin), and the first five
predictions are the ones `Model.predict` zeroes during initialisation.

Requires `openwakeword==0.6.0` and `onnxruntime`; refuses to run under any
other openWakeWord. See README.md for the commands and the model paths.
"""

import argparse
import hashlib
import json
import sys
from importlib import metadata

import numpy as np
from scipy.io import wavfile

# The exact openWakeWord the Rust constants were captured against. The
# preprocessor internals this script reaches into are private, and the framing
# and warm-up it reproduces are version-specific, so a different release is a
# different oracle and must not be able to re-bless a table silently.
OPENWAKEWORD_VERSION = "0.6.0"

SAMPLE_RATE = 16_000
CHUNK = 1_280

# The LCG the Rust `cold_seed_noise` draws from: 64-bit state, the recurrence
# below, each sample `((state >> 33) % 2000) - 1000`. The recurrence, the seed
# and the mapping are the contract — change any of them on either side and the
# pinned scores for the chunks the seed still occupies change with them.
LCG_MULTIPLIER = 6364136223846793005
LCG_INCREMENT = 1442695040888963407
LCG_MODULUS = 1 << 64

COLD_SEED_SAMPLES = 4 * SAMPLE_RATE
COLD_SEED_AMPLITUDE = 1000
COLD_SEED_LCG_SEED = 12_345
COLD_SEED_SHA256 = "be23a5a494c0650aea77466547dafe8f1abcb00fa6a5be1f079bc8061cbdefde"

# The quiet-noise stand-in: uniform noise at the level of a quiet room capture
# (RMS = amplitude / sqrt(3) ~= 50). Level-matched only — white noise is not a
# recorded room — so it is a sanity pin, not a substitute for running real
# captures through this script.
QUIET_NOISE_SAMPLES = 4 * SAMPLE_RATE
QUIET_NOISE_AMPLITUDE = 87
QUIET_NOISE_LCG_SEED = 54_321
QUIET_NOISE_SHA256 = "d61a8caaccba050630e7ad191d2ecdf8e4cd138099176ae89fa567bd3cef9513"

# Silence ahead of the committed wake phrase, matching the Rust fixture.
WAKE_PHRASE_LEAD_SAMPLES = 32_000

# Predictions `Model.predict` zeroes after construction or `reset()`. Reported
# as `suppressed` rather than dropped: what the Rust stream must reproduce is
# which chunks carry no measurement, and that is only visible if they are shown.
WARMUP_CHUNKS = 5


def lcg_noise(seed, count, amplitude):
    """The Rust generators' output: uniform over [-amplitude, amplitude)."""
    span = 2 * amplitude
    state = seed
    out = np.empty(count, dtype=np.int16)
    for i in range(count):
        state = (state * LCG_MULTIPLIER + LCG_INCREMENT) % LCG_MODULUS
        out[i] = ((state >> 33) % span) - amplitude
    return out


def digest(pcm):
    """SHA-256 of the clip as little-endian S16 bytes, as Rust hashes it."""
    return hashlib.sha256(pcm.astype("<i2").tobytes()).hexdigest()


def require_digest(pcm, expected, what):
    actual = digest(pcm)
    if actual != expected:
        sys.exit(
            f"{what} digest mismatch: expected {expected}, got {actual}.\n"
            "One of the two generators has drifted. Fix the side that moved; do "
            "not re-capture scores from a clip the Rust tests are not checking."
        )


def read_wav(path):
    """Read a 16 kHz mono S16 WAV, or refuse it by name.

    `scipy.io.wavfile` rather than the standard library's `wave`, which rejects
    WAVE_FORMAT_EXTENSIBLE — the container many recorders write, including the
    captures this script exists to score. scipy ships as an openWakeWord
    dependency, so it is present wherever this can run at all.
    """
    rate, pcm = wavfile.read(str(path))
    if pcm.ndim != 1:
        sys.exit(f"{path}: must be mono")
    if rate != SAMPLE_RATE:
        sys.exit(f"{path}: must be {SAMPLE_RATE} Hz, got {rate}")
    if pcm.dtype != np.int16:
        sys.exit(f"{path}: must be S16, got {pcm.dtype}")
    return pcm


def fixture_silence(_args):
    return np.zeros(4 * SAMPLE_RATE, dtype=np.int16)


def fixture_wake_phrase(args):
    if args.wake_phrase_wav is None:
        sys.exit("--wake-phrase-wav is required for the wake-phrase fixture")
    lead = np.zeros(WAKE_PHRASE_LEAD_SAMPLES, dtype=np.int16)
    return np.concatenate((lead, read_wav(args.wake_phrase_wav)))


def fixture_quiet_noise(_args):
    pcm = lcg_noise(QUIET_NOISE_LCG_SEED, QUIET_NOISE_SAMPLES, QUIET_NOISE_AMPLITUDE)
    require_digest(pcm, QUIET_NOISE_SHA256, "quiet-noise fixture")
    return pcm


FIXTURES = {
    "silence": fixture_silence,
    "wake-phrase": fixture_wake_phrase,
    "quiet-noise": fixture_quiet_noise,
}


def build_model(args):
    import openwakeword.model

    return openwakeword.model.Model(
        wakeword_models=[str(args.wake)],
        inference_framework="onnx",
        melspec_model_path=str(args.melspec),
        embedding_model_path=str(args.embedding),
    )


def seed_history(model, seed_clip):
    """Replace the random embedding history with the fixed seed clip's."""
    model.preprocessor.feature_buffer = model.preprocessor._get_embeddings(seed_clip)


def score_chunks(model, pcm, label):
    """Feed `pcm` chunk by chunk, printing one JSON line per chunk.

    A trailing partial chunk is zero-padded to a full one, which is what the
    Rust stream's flush does with the samples left over at the end of a
    segment.
    """
    name = next(iter(model.models))
    for index in range(0, len(pcm), CHUNK):
        chunk = pcm[index : index + CHUNK]
        if len(chunk) < CHUNK:
            chunk = np.concatenate(
                (chunk, np.zeros(CHUNK - len(chunk), dtype=np.int16))
            )
        score = float(model.predict(chunk)[name])
        chunk_ordinal = index // CHUNK + 1
        print(
            json.dumps(
                {
                    "fixture": label,
                    "end_sample": chunk_ordinal * CHUNK,
                    "score": score,
                    "suppressed": chunk_ordinal <= WARMUP_CHUNKS,
                }
            ),
            flush=True,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--melspec", required=True, help="melspectrogram.onnx")
    parser.add_argument("--embedding", required=True, help="embedding_model.onnx")
    parser.add_argument("--wake", required=True, help="the wake head .onnx")
    parser.add_argument(
        "--fixture",
        action="append",
        default=[],
        choices=sorted(FIXTURES),
        help="a built-in fixture (repeatable)",
    )
    parser.add_argument(
        "--wav",
        action="append",
        default=[],
        help="a 16 kHz mono S16 WAV to score (repeatable)",
    )
    parser.add_argument(
        "--wake-phrase-wav",
        help="the committed wake-phrase WAV, for the wake-phrase fixture",
    )
    args = parser.parse_args()

    installed = metadata.version("openwakeword")
    if installed != OPENWAKEWORD_VERSION:
        sys.exit(
            f"openwakeword {installed} is installed; this oracle is "
            f"{OPENWAKEWORD_VERSION}. Install the pinned version."
        )
    if not args.fixture and not args.wav:
        sys.exit("nothing to score: pass --fixture and/or --wav")

    # Before anything is constructed: a seed clip that does not hash to the
    # value the Rust side asserts is a different cold start, and every score
    # below it would be wrong in a way no comparison could reveal.
    seed_clip = lcg_noise(COLD_SEED_LCG_SEED, COLD_SEED_SAMPLES, COLD_SEED_AMPLITUDE)
    require_digest(seed_clip, COLD_SEED_SHA256, "cold seed clip")

    model = build_model(args)
    for name in args.fixture:
        seed_history(model, seed_clip)
        score_chunks(model, FIXTURES[name](args), name)
        model.reset()
    for path in args.wav:
        seed_history(model, seed_clip)
        score_chunks(model, read_wav(path), path)
        model.reset()


if __name__ == "__main__":
    main()
