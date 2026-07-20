# firmware

Rust firmware workspace for the reSpeaker project. Target: Seeed XIAO ESP32-S3.

## Quickstart

```sh
sudo apt install python3 python3-venv dfu-util
sudo usermod -aG plugdev "$USER"   # device access; re-login to take effect
cargo install espup && espup install
cargo install cargo-espflash ldproxy
echo 'source ~/export-esp.sh' >> ~/.bashrc && source ~/export-esp.sh
make install-hooks
make check
```

Full setup, build, and flash instructions: [docs/setup.md](docs/setup.md).

## Workspace layout

- `devices/` — per-device binary crates (e.g. `respeaker-pod` for the XIAO ESP32-S3)
- `crates/` — shared libraries with host-runnable unit tests (e.g. `pod-banner`)
