# Firmware toolchain and workspace

Target, toolchain, and repo expectations for the pod firmware. Source:
firmware-toolchain-and-repo.md; language/port decisions from
architecture-arc-and-deferred-paths.md. Status: design — no firmware code yet;
toolchain bring-up can start against a bare XIAO ESP32-S3 + Flex Linear in I2S
mode. (firmware-toolchain-and-repo.md:5-7)

## 1. Target and language

- **MCU:** ESP32-S3 on a **Seeed XIAO ESP32-S3**, hosting the XVF3800 (over I2S
  for audio + on-device control transport) plus LD2450 (mmWave), LD2412
  (presence), BME280, BH1750. (firmware-toolchain-and-repo.md:12-16)
- **Language/framework:** **pure Rust on ESP-IDF** (`esp-idf-svc`). Decision
  rationale: memory safety on novel code, high AI-iterable host-testable
  surface, single-language codebase, no FFI debugging surface.
  (architecture-arc-and-deferred-paths.md:152,171,187-188)
- **Target triple:** `xtensa-esp32s3-espidf`. Only the `pod-firmware` crate
  builds for it; everything else builds/tests on the host.
  (firmware-toolchain-and-repo.md:36-37,64-72)

## 2. Toolchain (firmware-toolchain-and-repo.md:82-97)

| Component          | Pin / channel                          | Role |
|--------------------|----------------------------------------|------|
| rustup stable      | latest (host crates)                   | host build/test |
| Xtensa Rust        | via `espup install` (LLVM fork)        | `pod-firmware` only |
| `espup`            | latest                                 | installs/updates Xtensa toolchain |
| `cargo-espflash`   | ≥ 3.x                                  | flash + serial monitor |
| `ldproxy`          | latest                                 | ESP-IDF linker shim |
| `embuild`          | transitive via `esp-idf-svc`           | builds ESP-IDF in cargo build |
| ESP-IDF            | **v5.5.3** (bring-up finding; see §Milestone 0) | RTOS + Wi-Fi + MQTT + OTA |
| `esp-idf-svc`      | **0.52.1** (pinned; see §Milestone 0)  | std + FreeRTOS + Wi-Fi + MQTT + OTA |
| `probe-rs`         | latest (optional)                      | USB-JTAG debug via ESP32-S3 built-in JTAG |

ESP-IDF v5.5.3 is the resolved pin (bring-up finding; see §Milestone 0). v5.3.x
LTS was attempted but incompatible with esp-idf-hal 0.46.2 (rmt driver struct
rename). Requirements are stable `esp_https_ota`, MQTT5, current `esp-idf-svc`
compatibility. (firmware-toolchain-and-repo.md:95-97)

## 3. Workspace layout (firmware-toolchain-and-repo.md:20-50)

Cargo workspace. Driver/logic crates are host-testable; only `pod-firmware`
touches hardware:

```
respeaker-pod/
├── Cargo.toml            # workspace; [workspace.dependencies]; default-members
├── rust-toolchain.toml   # stable for host; esp toolchain via espup
├── crates/
│   ├── wire-types/       # MQTT topics, event/manifest schemas, audio framing (no_std-friendly)
│   ├── xvf3800/          # driver, generic over embedded-hal-async I2C + reset-pin trait
│   ├── ld2450/           # mmWave x/y parser over embedded-io-async serial
│   ├── ld2412/           # presence parser
│   ├── audio-pipeline/   # I2S read loop, VAD-gate state machine, frame builder
│   └── pod-firmware/     # ONLY crate depending on esp-idf-svc / hardware
├── tools/                # homelab-listener, ota-server, trace-replay (host)
├── xtask/                # cargo xtask helpers
├── hil-tests/            # opt-in; needs attached hardware
└── testdata/             # I2C/UART trace replays, PCM fixtures
```

- `default-members` **excludes** `pod-firmware`, so `cargo test --workspace`
  runs host tests for the driver/logic crates with **no Xtensa toolchain**.
  Firmware compiles only via explicit `cargo build -p pod-firmware`.
  (firmware-toolchain-and-repo.md:64-72)
- All non-trivial deps pinned in `[workspace.dependencies]`; per-crate
  `Cargo.toml` uses `{ workspace = true, features = [...] }`.
  (firmware-toolchain-and-repo.md:74-79)

## 4. HAL boundary (firmware-toolchain-and-repo.md:104-122)

Drivers consume `embedded-hal` 1.0 + async at their public boundary:

- I2C: `embedded-hal-async::i2c::I2c` — **this is the XVF3800 control transport**
  on-device (see xvf3800-control-protocol.md §6: the concrete I2C framing is an
  open question confirmed empirically during firmware bring-up).
- UART/serial: `embedded-io-async::{Read, Write}`.
- XVF3800 reset line: `embedded-hal::digital::OutputPin`.
- `pod-firmware` composes concrete `esp-idf-svc` peripherals into these traits;
  drivers stay host-testable via `embedded-hal-mock` (async) and UART trace
  replay. Audio I2S has no standard embedded-hal trait yet; `audio-pipeline`
  defines its own reader trait (shape TBD).

## 5. formatBCE port relevance

The XVF3800 driver is a **port of the community formatBCE ESPHome integration**
(`Respeaker-XVF3800-ESPHome-integration`, C++, MIT, single-maintainer) into Rust,
with the C++ as reference (~1500 LoC).
(architecture-arc-and-deferred-paths.md:147-152,188)

Why port rather than reuse: every hybrid (Rust↔C++ FFI) requires the same
extraction of formatBCE from its ESPHome scaffolding, so a clean Rust port is
competitive and avoids a permanent FFI surface.
(architecture-arc-and-deferred-paths.md:147-152)

What formatBCE provides as reference:
- I2C control of the XVF3800 ("XMOS transport protocol", I2C addr `0x2C` in its
  build) — the concrete on-device control framing the Rust driver must
  reproduce (xvf3800-control-protocol.md §4, §6).
- `aic3104` codec driver (TLV320AIC3104 at I2C `0x18`) for direct codec control
  in I2S mode (audio-and-channels.md §5).
- DFU state machine, mute switch, LED-beam sensor patterns.
(research-step0.md:218-261)

**Validate the Rust port against the formatBCE binary on HIL**
(architecture-arc-and-deferred-paths.md:150).

Fallback if Rust disappoints: modern C++20 on ESP-IDF (lifts formatBCE intact)
is the strongest fallback; then C++-hosts-Rust-libs; then Rust-hosts-C++.
ESPHome is now only a low-priority fallback.
(architecture-arc-and-deferred-paths.md:210-226)

## 6. xtask / ops (firmware-toolchain-and-repo.md:124-136)

`cargo xtask` proxies operations: `check` (clippy -D warnings), `test` (host
crates), `fw-build [--release]`, `fw-flash <serial>` (espflash), `fw-monitor
<serial>`, `trace-replay <crate>`.

## Milestone 0 — resolved toolchain versions (2026-06-05)

Host-binary versions that `Cargo.lock` does not capture. Recorded at first successful
`cargo build --release` of `firmware/devices/respeaker-pod`.

| Component | Version | Notes |
|-----------|---------|-------|
| `espup` | 0.17.1 | `cargo install espup` |
| `rustup` esp channel (`rustc`) | 1.95.0-nightly (95e5bda86 2026-04-15) (1.95.0.0) | installed by `espup install` |
| Xtensa LLVM/clang fork | esp-20.1.1_20250829 | `~/.rustup/toolchains/esp/xtensa-esp32-elf-clang/` |
| Xtensa GCC toolchain | esp-15.2.0_20250920 | `~/.rustup/toolchains/esp/xtensa-esp-elf/` |
| `cargo-espflash` | 4.4.0 | `cargo install cargo-espflash` |
| `ldproxy` | 0.3.4 | `cargo install ldproxy` |
| `rustup` stable channel (`rustc`) | 1.96.0 (ac68faa20 2026-05-25) | host/library crates |
| ESP-IDF | v5.5.3 | cloned at build time to `~/.espressif/esp-idf/v5.5.3/`; pinned via `ESP_IDF_VERSION=v5.5.3` in `.cargo/config.toml`; v5.3 LTS was attempted but incompatible with esp-idf-hal 0.46.2 (see implementation-report.md) |
| `esp-idf-svc` crate | 0.52.1 | pinned in `firmware/Cargo.lock` |
| `embuild` crate | 0.33.1 | transitive via `esp-idf-svc`; declared in `respeaker-pod/Cargo.toml` build-deps |

**Export file:** `~/export-esp.sh` (written by `espup install`) sets `LIBCLANG_PATH` and
prepends the Xtensa GCC toolchain to `PATH`. Must be sourced before `cargo build`.

**Note on `~/.espressif` vs `~/.rustup`:** espup 0.17.x installs Rust/LLVM components
under `~/.rustup/toolchains/esp/` rather than `~/.espressif/`. The `~/.espressif/`
directory is created by `embuild` at first `cargo build` and holds the cloned ESP-IDF
source, compiled ESP-IDF tools (cmake, ninja, xtensa-esp-elf, openocd, etc.), and the
Python venv (`idf5.5_py3.10_env` for v5.5.3). `ESP_IDF_TOOLS_INSTALL_DIR=global` in
`firmware/devices/respeaker-pod/.cargo/config.toml` points embuild there.

**Python venv caveat:** On Ubuntu 22.04, `python3.10-venv` may not be installed.
`embuild` uses `python3 -m venv --upgrade-deps` which fails without `ensurepip`.
Workaround: `pip3 install --target ~/.espressif/python_env/idf5.5_py3.10_env/lib/python3.10/site-packages pip`
then re-run `cargo build`. Permanent fix: `sudo apt install python3.10-venv`.
Note: adjust `idf5.5` to match your ESP-IDF major.minor (e.g. `idf5.5` for v5.5.x)
and `python3.10` to match `python3 --version` on your host.

---

## 7. Open firmware-skeleton questions (architecture-arc-and-deferred-paths.md:321-337)

Resolve as the skeleton is built; none block toolchain bring-up:
async runtime (`std::thread`+channels vs `smol` vs Embassy); ESP-IDF version
pin; audio transport (RTP/Opus vs WSS/PCM); auth scheme (TLS-PSK vs HMAC vs
SRTP-PSK); logging policy; panic handler; heap/stack sizing; I2C bus sharing
(XVF3800 + BME280 + BH1750 all I2C on tight XIAO pins — resolve before PCB
layout).
