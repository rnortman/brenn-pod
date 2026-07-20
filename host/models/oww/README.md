# openWakeWord models

ONNX models for the wake gate (`speech-pipeline`'s `OwwGate`). The three-model
openWakeWord graph: raw audio → mel spectrogram → embedding → wake score.

**These weights are NOT committed to this repository** (see the license note
below). Fetch them with `make -C host fetch-models`, which downloads and
sha256-verifies them against the table below.

- Source: https://github.com/dscripka/openWakeWord (release `v0.5.1`),
  download base
  `https://github.com/dscripka/openWakeWord/releases/download/v0.5.1/`.
- License: the openWakeWord **code** is Apache-2.0, but the **pre-trained
  models** (all three files here) are Creative Commons
  Attribution-NonCommercial-ShareAlike 4.0 (CC BY-NC-SA 4.0) — their training
  data includes datasets with unknown or restrictive licensing. The
  NonCommercial term is incompatible with this repo's Apache-2.0 license, so the
  weights are fetched from upstream rather than redistributed here.
- Wake phrase: `hey_jarvis_v0.1.onnx` detects "Hey Jarvis" — the household
  default. Swapping phrases means replacing this one file and regenerating the
  wake-phrase fixture; `melspectrogram.onnx` and `embedding_model.onnx` are
  phrase-independent and shared by every openWakeWord model.

`make -C host fetch-models` downloads each file from the base URL above and
verifies its sha256 against the table below.

| File | sha256 | bytes |
|---|---|---|
| `melspectrogram.onnx` | `ba2b0e0f8b7b875369a2c89cb13360ff53bac436f2895cced9f479fa65eb176f` | 1087958 |
| `embedding_model.onnx` | `70d164290c1d095d1d4ee149bc5e00543250a7316b59f31d056cff7bd3075c1f` | 1326578 |
| `hey_jarvis_v0.1.onnx` | `94a13cfe60075b132f6a472e7e462e8123ee70861bc3fb58434a73712ee0d2cb` | 1271370 |

Verify: `sha256sum -c` against this table, or

```
cd host/models/oww && sha256sum *.onnx
```

## ONNX Runtime binary (`ort` `download-binaries`) — accepted trust decision

The `ort` crate (workspace dep, exact-pinned `=2.0.0-rc.12`) is used with its
default `download-binaries` feature: at first build, `ort-sys`'s build script
fetches a prebuilt native ONNX Runtime from pyke's CDN and links it into the
LAN-facing daemon. This is a deliberate, accepted trust decision:

- **Integrity is pinned**: `ort-sys` hash-verifies the download against a digest
  embedded in the crate, and `ort-sys` itself is checksum-locked in
  `host/Cargo.lock` — a CDN/MITM swap is caught. Keep the `ort` pin exact so the
  lockfile hash gate stays meaningful.
- **Residual risk is provenance** (not integrity): the binary is built by pyke's
  pipeline, not Microsoft or this repo, so an upstream build-pipeline compromise
  *before* the pinned hash was published would run native code in the daemon.
  Low likelihood, high blast radius; accepted for now.
- **Operational note**: first builds require network egress to pyke.io. To
  eliminate both the provenance risk and the network dependency, switch to
  building ONNX Runtime from source or `ort`'s `load-dynamic` against a
  distro-packaged `libonnxruntime`.
