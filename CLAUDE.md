# brenn-pod

A Rust firmware platform for home-built smart-home devices, and the devices built on it. We're replacing what ESPHome provides (firmware-as-YAML on ESP32, OTA, discovery) with a real-code, single-language, testable ecosystem we fully control.

**Two horizons:**
- **Now:** bring up the Seeed reSpeaker Flex (XVF3800 mic array + XIAO ESP32-S3) as the first device — a voice node.
- **Long-term:** a shared firmware base for a family of devices (a speaker+mic+mmWave pod, a no-speaker variant, a wide-angle-camera variant — list TBD), with a possible WASM runtime for sandboxed on-device plugins.

Product-grade code — write every line as if it ships. Full charter: `README.md`.

## Hardware Bring-Up

- We bring up new hardware — or untried features of hardware we already use — by writing **HIL self-tests that ASSERT the expected behavior and letting them FAIL**, not by writing throwaway probe code. The failure output is the discovery. This is the default; reach for a throwaway probe only when an assertion genuinely cannot express the question.
- Once an observed value is confirmed correct-and-expected, bake it into the test. It then stays in the self-test registry (run by `crates/hil-host`) permanently as a regression guard — the same expensive hardware round-trip yields a durable asset instead of a discarded script.
- Guardrail: an UNEXPECTED reading gets human review before you make the test pass. Do not let make-it-green launder an unexpected value into accepted truth. Keep presence-tests (does the device ACK at address X) separate from identity-tests (register Y reads value Z).

## Reachy Fault Management

The fault-management doctrine for the Reachy motion stack (including
`reachy-motiond` in this repo) lives at `docs/fault-management.md` in the
sibling `brenn-reachy` checkout (`../brenn-reachy/docs/fault-management.md`).
Read it before touching anything that arms, disarms, or handles a motion
fault. Only the two invariants that hold whatever the response are restated
here: the Minimum Risk Condition is *stowed and de-torqued*, **nothing ever
gates de-torquing**, and holding torque is never a fault response — stowed
with torque held is that machine's only pinch hazard. Which motors a given
response covers, what latches, and how a session recovers are in that
document and only there; a second summary of them in this repo is a copy that
drifts.

## Device Deployment Doctrine (dev cycles)

During development iteration, **nothing we push touches a device's eMMC**.
Binaries, configs, tokens, secrets — all of it lands in RAM (tmpfs) and is
re-pushed after a reboot by one command. This is the brenn-os design: the only
flash-resident state is fundamental remote-access credentials and identity.
Flash-backed ("baked") placement of anything else is a release-hardening act
performed on a stable release — never a dev-cycle convenience. Do not propose
persisting app state to `/persistent` (or anywhere else on flash) to make a
dev workflow nicer; fix the deploy command instead.

## TODO System

Two pieces that stay in sync:
- `TODO.md` at the repo root — master list. Each entry has a slug, a description, and the deferral context.
- `TODO(slug)` comments in code — mark the spot where the work needs to happen.

Slugs are the join key. Adding a TODO requires both an entry in `TODO.md` and a `TODO(slug)` comment at the relevant location. Don't use TODOs for vague aspirations — every TODO should describe a concrete thing that needs to happen, in a place where "done" is obvious. `TODO.md` is for code and design work only — never for operational tasks (running a suite, performing a bench run, deploying something); those are asks made to the user directly, not entries in this file.
