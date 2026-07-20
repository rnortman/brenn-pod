# Silero VAD model

Committed ONNX model for the host endpointer (`speech-pipeline`'s `SileroVad`).
Silero VAD discriminates speech from music/noise on live audio — the axis the
device's energy-based VAD fails on — emitting P(speech) per 512-sample (32 ms
at 16 kHz) chunk with a per-pod recurrent state tensor.

- Source: https://github.com/snakers4/silero-vad (v5 graph), file
  `src/silero_vad/data/silero_vad.onnx`.
- License: MIT.
- Graph: inputs `input` (f32 `[1, 576]`, normalized to `[-1, 1]`), `state`
  (f32 `[2, 1, 128]`, the recurrent LSTM state), `sr` (int64 scalar, `16000`);
  outputs `output` (f32 `[1, 1]`, P(speech)) and `stateN` (f32 `[2, 1, 128]`,
  the next state).

The `input` length deserves emphasis, because getting it wrong is silent. The
graph *declares* `input` dims dynamic (`[0, 0]`), so ONNX Runtime accepts any
length without complaint — but every reference implementation feeds **576 = 64
samples of context + a 512-sample chunk**, where the context is the previous
chunk's trailing 64 samples (zeros at cold start) and 512 is the hop. Feeding
the bare 512-sample chunk yields a confident, stable, wrong P≈0.003 on clear
speech. This README previously recorded the contract as `[1, 512]`, and the
wrapper was written faithfully to that wrong record; the resulting deafness cost
a debugging cycle. The reference contract and the measurement are documented in
`docs/adr/2026/07/13-continuous-listener-roadmap/exploration-silero-reference.md`.

Re-fetch is mechanical: download the file from the source above and confirm its
sha256 below.

| File | sha256 | bytes |
|---|---|---|
| `silero_vad.onnx` | `1a153a22f4509e292a94e67d6f9b85e8deb25b4988682b7e174c65279d8788e3` | 2327524 |

Verify: `sha256sum -c` against this table, or

```
cd host/models/silero && sha256sum *.onnx
```

The ONNX Runtime binary trust decision (`ort` `download-binaries`) is shared
with the wake models — see `host/models/oww/README.md`.
