//! The Reachy pod — the Linux (Raspberry Pi CM4) voice node.
//!
//! The XVF3800 mic-array board hangs off USB here rather than off I2C/I2S, so this
//! crate holds the platform half of the pipeline: the USB control transport, the
//! ALSA capture and playback ends, and the bring-up self-tests. Everything that is
//! protocol or policy — the wire format, the VAD gate, the segment engine, the idle
//! loop, the TLS-PSK link — is shared with the ESP32 pod through `audio-pipeline`,
//! `pod-streamer`, `xvf3800-ctrl` and `psk-link`.
//!
//! The binary is a thin dispatcher over these modules; they are `pub` so the
//! workstation test lane can drive them off the device.

pub mod alsa_capture;
pub mod beam;
pub mod chip;
pub mod cli;
pub mod config;
pub mod logging;
pub mod playback;
mod regs;
pub mod run;
pub mod selftest;
mod test_support;
pub mod tls;
pub mod usb_ctrl;
