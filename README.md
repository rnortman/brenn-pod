# respeaker

A Rust firmware platform for home-built smart-home devices — and the devices built on it.

## What this is

We're building our own firmware ecosystem for ESP32-class smart-home devices, on real, testable Rust code rather than ESPHome's YAML. ESPHome hands you firmware-as-config, OTA, and discovery for free — but at the cost of framework intrusion and a config programming model that doesn't scale to custom audio and sensor logic. We replace it with a single-language Rust codebase we fully control.

The first device — and the immediate driver of the platform — is a voice node built on the Seeed reSpeaker Flex (XVF3800 mic array + XIAO ESP32-S3).

## Two horizons

**Immediate — bring up the reSpeaker board.** (Largely complete)
Get the XVF3800 + XIAO pod working end-to-end as a voice node: mic capture, beamforming / direction-of-arrival off the XVF3800, audio streamed to the homelab, playback. This is where the platform's primitives — drivers, transport, OTA — get built and proven on real hardware. Bring-up is HIL-first / assertions-as-probes: we probe unknown hardware by writing failing HIL self-tests that assert expected reality, so every bring-up discovery becomes a permanent regression test rather than throwaway code. 

**Long-term — a firmware base for a family of devices.**
A shared Rust foundation that multiple device types build on. The family is TBD but likely includes:
- a speaker + mic + mmWave-presence pod (the voice node above),
- the same without a speaker (sensor / mic only),
- a variant that also carries a wide-angle camera.

Stretch goal: a **WASM runtime** on the ESP32s — sandboxed plugin / dynamic-scripting capability, so device behavior can be extended without reflashing.

## The application these devices serve

The voice devices feed a homelab-hosted voice assistant with custom multi-pod arbitration that fuses direction-of-arrival and presence across pods. The arbitration and presence features are part of this repo; the actual LLM voice assistant backend is not.

## Repository layout

- `firmware/` — the Rust firmware workspace
- `host/` — the host-side code
- `crates/*` — shared, host-testable libraries.

## Current status

- **Milestone 0 — DONE.** Rust-on-ESP-IDF toolchain proven on hardware: an `esp-idf-svc` blink + serial banner flashed to the XIAO ESP32-S3, plus a host test loop on stock stable Rust. Both halves of the "real code beats ESPHome YAML" bet validated; the C++ fallback wasn't needed.
- **Next — Milestone 1:** bring up the XVF3800 control transport from Rust (read DoA, validate against the Stage-1 Python tooling).
