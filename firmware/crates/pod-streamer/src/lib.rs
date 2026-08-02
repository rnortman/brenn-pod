//! Transport-independent pod audio streamer.
//!
//! The streaming engine's policy — poll discipline, pacing, backpressure
//! classification, the per-segment drain loop — is identical on every pod; only
//! the byte transport, the `poll()` syscall, the monotonic clock, and where
//! observability readings go differ. Those are seams here
//! ([`link::LinkStream`], [`netpoll::NetPoll`], and the clock/observer fields of
//! [`segment::SegmentDeps`]), supplied per platform: the ESP32 firmware over
//! esp-tls, `esp_idf_svc::sys::poll` and `esp_timer_get_time`, the Linux pod
//! over openssl, `libc::poll` and `CLOCK_MONOTONIC`.
//!
//! Contains no platform crate dependency of its own, which is what keeps the two
//! implementations from drifting: a behavior change here lands on both pods or
//! neither.

pub mod idle;
pub mod link;
pub mod netpoll;
pub mod run;
pub mod segment;
pub mod telemetry;

#[cfg(test)]
mod test_support;
