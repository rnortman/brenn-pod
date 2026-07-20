//! Audio pipeline shared types — wire schema, framing codec, VAD state machine,
//! ring-buffer index math.
//!
//! `no_std` by default; enable the `std` feature for host-side use.
//!
//! All schema types derive Serialize/Deserialize.  Postcard encodes them; a `u16`
//! LE length prefix is prepended by the framing layer.  Framing lives in the
//! `wire` module; VAD in `vad`; ring-buffer math in `ring`.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(test)]
extern crate alloc;

/// Real-time pacing arithmetic for the outbound audio catch-up drain. Pure and
/// clock-free so it unit tests on the host; the streamer supplies the clock read.
pub mod pace;
/// TX-side playback sink and PCM expansion logic (speaker-rx-audio design §6).
/// `std`-only: the inbound playback sink and its channel use `std::sync::mpsc` and
/// `Vec` (mirroring `stream_send`).  Relocated out of the device crate so its unit
/// tests run on the host under `cargo test --workspace`.
#[cfg(feature = "std")]
pub mod playback;
pub mod ring;
/// Host-testable streamer send-loop core (partial-write-fix design §2.3a).
/// `std`-only: drives a `dyn std::io::Write` with `poll`-backed backpressure.
#[cfg(feature = "std")]
pub mod stream_send;
/// Shared test-support helpers (partial-write-fix design §2.3a, design-2).
/// Test-only: enabled by this crate's own tests (`#[cfg(test)]`) or downstream via
/// the `test-support` feature; never part of a production build.
///
/// NOT included in `std`-only builds: enabling `std` alone (the typical host build)
/// gives `stream_send` but **not** this module.  A downstream
/// crate that needs these helpers must opt in via `features = ["test-support"]` in its
/// `[dev-dependencies]` (quality-3).
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
pub mod vad;
pub mod wire;
