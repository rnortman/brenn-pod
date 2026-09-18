# oww-oracle — the Python side of the wake front end's parity pins

`capture_scores.py` runs openWakeWord 0.6.0's own `Model.predict` over
1,280-sample chunks and prints one JSON object per chunk. Its output is where
the pinned score tables in
`host/crates/speech-pipeline/src/listener/oww_stream.rs` come from:
`python_per_step_scores_regression`, `python_silence_scores_regression` and
`quiet_noise_never_arms` all hold numbers copied out of a run of this script.
The Rust streaming core is only a reimplementation of that oracle, so the tables
are the mechanism that catches it drifting away — and they are trustworthy only
as long as anyone can regenerate them.

Nothing here runs in CI: it needs a Python environment and the model weights,
which `host/models/fetch.sh` pulls but does not redistribute. The durable
artefact is the Rust pins.

## Running it

```sh
python3 -m venv .venv && .venv/bin/pip install 'openwakeword==0.6.0' onnxruntime
.venv/bin/python host/tools/oww-oracle/capture_scores.py \
    --melspec host/models/oww/melspectrogram.onnx \
    --embedding host/models/oww/embedding_model.onnx \
    --wake host/models/oww/hey_jarvis_v0.1.onnx \
    --fixture silence --fixture quiet-noise \
    --fixture wake-phrase --wake-phrase-wav host/testdata/wake/wake-phrase.wav
```

The three built-in fixtures are the same audio the Rust tests build: 4 s of
digital silence, 4 s of quiet uniform noise at RMS ~= 50, and the committed
"Hey Jarvis" clip behind 32,000 samples of silence. `--wav` scores any 16 kHz
mono S16 file instead — a recorded phrase, a room capture, a turn pulled off a
device — and `--wake` takes whichever head is being investigated, not just the
committed one. Each chunk prints as

```json
{"fixture": "silence", "end_sample": 7680, "score": 2.563e-06, "suppressed": false}
```

`suppressed` marks the first five chunks, whose predictions `Model.predict`
zeroes while the model initialises; the Rust stream emits no score for them at
all, so only the unsuppressed rows appear in the tables.

## Why the digests

The script draws the cold-start seed clip from a reimplementation of the Rust
`cold_seed_noise` generator — same recurrence, same seed, same mapping — and
overwrites openWakeWord's randomly drawn embedding history with that clip's
embeddings. Two copies of one generator drift, so both sides carry the same
SHA-256 of the clip: Rust's `cold_seed_matches_pinned_digest` asserts it, and
this script hashes its own clip and refuses to start on a mismatch. The
quiet-noise fixture is pinned the same way on both sides. A re-capture can
therefore only ever be taken from a clip the Rust tests are also checking — a
drifted generator fails loudly instead of quietly re-blessing a different cold
start.

## When to re-capture

Re-run this script and copy the new tables in whenever any of the following
moves, and read the diff before trusting it — a changed score is a claim about
the front end, not a formality:

- the seed generator: its recurrence, its seed, its amplitude, or the clip
  length;
- the mel window's cold fill or the raw mel lookback;
- any of the three ONNX models, including a swap to a different wake head;
- the openWakeWord version (the script refuses to run on anything but the
  pinned one, which is the signal to stop and think rather than a hurdle).
