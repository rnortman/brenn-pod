# Firmware setup

Prerequisites, build/flash/monitor steps, and dev-loop reference for the `respeaker-pod` device crate.
Target: Seeed XIAO ESP32-S3 over USB-C (USB-serial-JTAG, no external probe).

---

## Prerequisites and assumptions

### Already assumed present

The following are assumed present on the developer's Linux host. This doc was verified on Ubuntu 22.04.

- `git`
- `curl`
- `build-essential` (C compiler and standard headers — required by ESP-IDF's native component builds)

### OS-level packages to install before the Rust toolchain

ESP-IDF's build (driven by `esp-idf-sys`/`embuild`) creates a Python virtual environment and installs its own tooling into it. Without `python3-venv`, the venv creation fails with a missing `ensurepip` error — this was hit on Ubuntu 22.04 where `python3.10-venv` was not installed by default, leaving the venv with a working Python binary but no `pip`.

Install before proceeding:

```sh
sudo apt install python3 python3-venv dfu-util
```

`dfu-util` is needed to flash the XVF3800 (XMOS) firmware over its USB-C port (see
`firmware/vendor/xvf3800/README.md`); it is not used for the ESP32-S3. The ESP-IDF build
downloads and manages its own copies of CMake, Ninja, compiler toolchains, and Python
packages into `~/.espressif/`; no additional apt packages are needed for those.

### Device-access group (udev prerequisite)

The udev rules (see "udev rule" below) grant no-sudo device access via the `plugdev`
group. A fresh or headless Ubuntu user is **not** guaranteed to be in `plugdev`. Check and
add yourself once per host:

```sh
id -Gn | tr ' ' '\n' | grep -qx plugdev || sudo usermod -aG plugdev "$USER"
```

Group membership only takes effect in a new login session — log out and back in (or
reboot) after adding it. Without `plugdev`, `cargo espflash` and `dfu-util` silently fail
to open the device even with the udev rules installed.

### Rust + ESP toolchain

Install these in order:

```sh
# Install rustup if not present
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install espup and use it to install the Xtensa/RISC-V ESP toolchain
cargo install espup
espup install

# Install flash/monitor tooling and the ESP-IDF linker shim
cargo install cargo-espflash ldproxy
```

After `espup install`, source the generated environment file to set `LIBCLANG_PATH`
and add the Xtensa toolchain to `PATH`:

```sh
source ~/export-esp.sh
```

Add that line to your shell profile (`~/.bashrc`, `~/.zshrc`, etc.) so it runs
automatically in new shells.

---

## Dev loop: checks and tests

All Makefile targets below run from `firmware/` (the workspace root). The workspace
`rust-toolchain.toml` pins stable Rust, so host targets run on stock stable with no
espup required. Only the device targets need the esp channel (resolved from
`devices/respeaker-pod/rust-toolchain.toml`).

### `make check` (default)

The normal inner loop. Runs four steps in order:

1. `cargo fmt --all --check` — format check across all workspace members (host toolchain)
2. `cargo clippy -- -D warnings` — host clippy on `default-members` (stable, no espup)
3. `cargo test` — host unit tests on `default-members` (stable, no espup)
4. `cd devices/respeaker-pod && cargo clippy -- -D warnings` — device clippy using the esp
   channel; warm run is ~1 second. Needs `espup install` and `source ~/export-esp.sh`
   but does NOT require attached hardware.

### `make check-host`

Host-only lane — skips step 4. Use this when espup is not installed (CI without the esp
toolchain, or a new machine before running `espup install`). Covers fmt, host clippy,
and host tests only.

### `make fix`

Auto-fix lane. Runs `cargo fmt --all`, then `cargo clippy --fix` on host crates, then
`cargo clippy --fix` on the device crate. Passes `--allow-dirty --allow-staged` so it's
usable mid-edit; review the diff before committing.

### `make build-firmware`

Builds the optimized release image for flashing:

```sh
cd devices/respeaker-pod && cargo build --release
```

Requires espup. Not part of the default check loop — release builds are slow (10–30 min
on first run; incremental thereafter).

### `make install-hooks` (once per clone)

Wires the repo-local `.githooks/` directory so the pre-commit hook travels with the repo:

```sh
git config core.hooksPath .githooks
```

The hook runs `make -C firmware check` whenever firmware files are staged; it does nothing
for docs-only commits. Bypass for a specific commit when the toolchain is unavailable:

```sh
git commit --no-verify
```

### Running just the host unit tests

The host-tested library is `crates/pod-banner`. To run its tests directly:

```sh
cd firmware
cargo test
```

These run on stock stable Rust with no espup and no hardware. The test suite currently
has 2 tests (`banner_contains_load_bearing_phrase`, `banner_includes_version_field`).

---

## Build

The device crate MUST be built from its own directory — `rustup` resolves
`rust-toolchain.toml` from the invocation directory upward, so only running
from `firmware/devices/respeaker-pod/` picks up the `esp` channel override.

```sh
source ~/export-esp.sh   # if not in shell profile
cd firmware/devices/respeaker-pod
cargo build --release
```

Or equivalently from the workspace root:

```sh
make build-firmware
```

The first build clones and compiles ESP-IDF v5.5.4 — expect 10–30 minutes
depending on network and CPU. Subsequent builds are incremental.

**Note:** ESP-IDF v5.3 LTS is not compatible with `esp-idf-hal 0.46.x` (struct
rename in RMT driver). Using v5.5.4 per the crate ecosystem's own test matrix.
See `docs/adr/2026/06/05-rust-toolchain-milestone0/implementation-report.md`.

The release image lands at:
`target/xtensa-esp32s3-espidf/release/respeaker-pod`

---

## Flash

Connect the XIAO over USB-C. From the device crate directory:

```sh
cargo espflash flash --release
```

If the port is not auto-detected, pass it explicitly:

```sh
cargo espflash flash --release --port /dev/ttyACM0
```

---

## Monitor serial output

```sh
cargo espflash monitor
```

Or combined flash + monitor:

```sh
cargo espflash flash --monitor --release
```

**Headless/SSH note:** `cargo espflash monitor` requires an interactive terminal. Over SSH or in any headless context it fails with `Error: Failed to initialize input reader` and prints no app output. To read the serial banner without a TTY (empirically confirmed on hardware):

```sh
stty -F /dev/ttyACM0 raw -echo
timeout 10 cat /dev/ttyACM0
```

The ESP32-S3 native USB-serial-JTAG is USB-CDC; baud rate is irrelevant.

Expected output (among ESP-IDF boot messages):

```
Hello from Rust on ESP-IDF!
Hello from Rust on ESP-IDF!
...
```

---

## udev rules (no-sudo device access)

Without the udev rules, `cargo espflash` and `dfu-util` require `sudo`. Both rule files
live in `firmware/udev/`. Install both once per host:

```sh
sudo cp firmware/udev/99-xiao-esp32s3.rules firmware/udev/99-respeaker-xvf3800.rules /etc/udev/rules.d/
sudo udevadm control --reload-rules && sudo udevadm trigger
```

Re-plug the USB-C cable. Subsequent flash/monitor/DFU runs need no `sudo` for a user in
the `plugdev` group (see "Device-access group" above; check with `id -Gn`).

`99-xiao-esp32s3.rules` (XIAO ESP32-S3, VID `0x303a`) covers:
- PID `0x1001` — app running (USB-serial-JTAG)
- PID `0x0002` — ROM download mode (DFU)

`99-respeaker-xvf3800.rules` (XVF3800/XMOS, VID `0x2886`) covers:
- PID `0x0022` — app mode (USB-audio firmware)
- PID `0x801c` — Safe/DFU mode (used by `dfu-util` to flash XMOS firmware)

---

## Recovery (brick prevention)

The ESP32-S3 has a ROM-level USB download mode that cannot be disabled by
application firmware. If the board appears bricked:

1. Hold the BOOT button while pressing RESET (or power-cycling).
2. The board enumerates as VID:PID `0x303a:0x0002`.
3. Flash a known-good image with `cargo espflash flash --release`.
