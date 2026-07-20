# respeaker

A Rust firmware platform for home-built smart-home devices, and the devices built on it. We're replacing what ESPHome provides (firmware-as-YAML on ESP32, OTA, discovery) with a real-code, single-language, testable ecosystem we fully control.

**Two horizons:**
- **Now:** bring up the Seeed reSpeaker Flex (XVF3800 mic array + XIAO ESP32-S3) as the first device — a voice node.
- **Long-term:** a shared firmware base for a family of devices (a speaker+mic+mmWave pod, a no-speaker variant, a wide-angle-camera variant — list TBD), with a possible WASM runtime for sandboxed on-device plugins.

Product-grade code — write every line as if it ships. Full charter: `README.md`.

## Hardware Bring-Up

- We bring up new hardware — or untried features of hardware we already use — by writing **HIL self-tests that ASSERT the expected behavior and letting them FAIL**, not by writing throwaway probe code. The failure output is the discovery. This is the default; reach for a throwaway probe only when an assertion genuinely cannot express the question.
- Once an observed value is confirmed correct-and-expected, bake it into the test. It then stays in the self-test registry (run by `crates/hil-host`) permanently as a regression guard — the same expensive hardware round-trip yields a durable asset instead of a discarded script.
- Guardrail: an UNEXPECTED reading gets human review before you make the test pass. Do not let make-it-green launder an unexpected value into accepted truth. Keep presence-tests (does the device ACK at address X) separate from identity-tests (register Y reads value Z).

## TODO System

Two pieces that stay in sync:
- `TODO.md` at the repo root — master list. Each entry has a slug, a description, and the deferral context.
- `TODO(slug)` comments in code — mark the spot where the work needs to happen.

Slugs are the join key. Adding a TODO requires both an entry in `TODO.md` and a `TODO(slug)` comment at the relevant location. Don't use TODOs for vague aspirations — every TODO should describe a concrete thing that needs to happen, in a place where "done" is obvious.
