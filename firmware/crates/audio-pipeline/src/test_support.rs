//! Shared test-support helpers (partial-write-fix design §2.3a, design-2).
//!
//! `audio_frame` was originally defined in the device crate
//! `respeaker-pod/src/main.rs` `mod tests` and shared between the send-loop tests
//! (now relocated to [`crate::stream_send`]) and the device-crate `drain_*` tests
//! that stay.  To keep **one** definition (no duplication, no drift — the hazard
//! the §2.3a `OQ1` fallback rejects), it lives here and both callers import it.
//!
//! This module is **test-only**: it is gated behind the `test-support` feature so
//! it never reaches a production build.  `audio-pipeline`'s own tests get it via
//! `[dev-dependencies] features = ["std"]` plus the crate's own
//! `#[cfg(test)]`-implied build; the device crate enables it as a
//! `[dev-dependencies]` feature so its `#[cfg(test)]` `drain_*` tests can call it
//! without dragging the helper into the device's production link.
//!
//! Requires `std` (the body builds a `heapless::Vec` via `std`-test machinery and
//! is only ever compiled for host/test targets).

use crate::wire::{AudioFrame, StreamFrame, MAX_AUDIO_PAYLOAD};

/// Build a minimal `StreamFrame::Audio` with `n_samples` silence samples
/// (mono S16_LE — 2 bytes per sample).
///
/// `n_samples` must not exceed `MAX_AUDIO_PAYLOAD / 2` — the backing
/// `heapless::Vec` holds at most `MAX_AUDIO_PAYLOAD` bytes, so a larger request
/// would silently truncate the frame. The assertion below fails loudly instead of
/// returning a short frame that could let a test pass against the wrong payload.
pub fn audio_frame(n_samples: usize) -> StreamFrame {
    assert!(
        n_samples * 2 <= MAX_AUDIO_PAYLOAD,
        "audio_frame: n_samples ({n_samples}) * 2 exceeds MAX_AUDIO_PAYLOAD ({MAX_AUDIO_PAYLOAD}); \
         the frame would be silently truncated"
    );
    let mut pcm: heapless::Vec<u8, MAX_AUDIO_PAYLOAD> = heapless::Vec::new();
    for _ in 0..n_samples {
        // The assertion above guarantees the buffer has room; `expect` turns any
        // future bound-check regression into a loud invariant-violation crash with a
        // diagnosable message rather than a silently-truncated frame (errhandling-1).
        pcm.push(0u8).unwrap_or_else(|_| {
            panic!(
                "audio_frame: PCM buffer exceeded MAX_AUDIO_PAYLOAD ({MAX_AUDIO_PAYLOAD}) \
                 at n_samples={n_samples} — caller violated the n_samples bound"
            )
        });
        pcm.push(0u8).unwrap_or_else(|_| {
            panic!(
                "audio_frame: PCM buffer exceeded MAX_AUDIO_PAYLOAD ({MAX_AUDIO_PAYLOAD}) \
                 at n_samples={n_samples} — caller violated the n_samples bound"
            )
        });
    }
    StreamFrame::Audio(AudioFrame {
        segment_id: 0,
        first_sample_index: 0,
        device_ts_us: 0,
        pcm,
    })
}
