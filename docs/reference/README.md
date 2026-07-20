# brenn-pod firmware reference

Living, firmware-facing reference for the pod hardware — the Seeed reSpeaker
Flex (XVF3800) paired with a XIAO ESP32-S3. These docs consolidate the scattered
bring-up research into durable, implementation-oriented fact sheets.

The dated ADR / research docs that are the provenance for everything here are
not published with this repository, so citations of the form `doc:line` refer to
sources you will not find in this tree. Within the public repo these fact sheets
are the reference; where one disagrees with the hardware, the hardware wins —
confirm against the device and fix the doc.

## Topics

- [`xvf3800-control-protocol.md`](xvf3800-control-protocol.md) — XMOS VocalFusion
  control command map (DoA, per-beam azimuths, per-beam speech energy), the
  vendor control-transfer mechanics used in the spike, value semantics, VID/PID,
  DFU. **Includes the on-device control-transport question, resolved empirically
  during firmware bring-up.**
- [`audio-and-channels.md`](audio-and-channels.md) — I2S vs USB firmware modes,
  the confirmed 6-channel map, sample rates/formats, codec (TLV320AIC3104) and
  HP-out routing, what stock 2-ch I2S firmware exposes.
- [`firmware-toolchain.md`](firmware-toolchain.md) — Rust-on-ESP-IDF target
  (ESP32-S3 / XIAO), toolchain (`espup` / `cargo-espflash` / `xtask`), formatBCE
  port relevance, repo/workspace expectations.

## SCOPE

These docs cover **firmware-facing facts only**: what runs on the XVF3800 and on
the XIAO ESP32-S3, and the control/audio interfaces between them and the rest of
the system.

**Out of firmware scope** (and deliberately excluded here): the off-board DoA
arbitrator, multi-talker tracking, beam-attribution-over-time, the homelab
STT/LLM/TTS pipeline, and transport/arbitration policy. Where a control command
produces telemetry that an off-board tracker consumes (e.g. per-tracker
azimuths), this reference documents *the command and its wire semantics* and
stops there. The higher-level logic lives outside this repository.

Confidence labels used throughout: **[confirmed]** = verified empirically on the
device during the spike; **[documented]** = stated in vendor docs / source but
not independently verified here; **[unknown]** = not resolved by available
sources.
