//! Device-side observability for the shared inbound playback path.
//!
//! The reassembly, handshake, and drain logic live in `audio_pipeline::inbound`; this
//! module supplies the ESP32's [`audio_pipeline::inbound::InboundObserver`] — heap and
//! stack-headroom samples taken inside the post-Hello inbound audio window, which the
//! segment-cadence streamer waypoints do not reach.

// Host view: every item here serves a device-gated call site.
#![cfg_attr(not(target_os = "espidf"), allow(dead_code))]

use audio_pipeline::inbound::{InboundObserver, InboundWaypoint};
use audio_pipeline::wire::PlaybackFormat;

/// The ESP pod's inbound observer. Every hook is a pure-read heap/stack query plus one
/// log line, so it is safe to call from the streamer thread on the audio path.
pub(crate) struct HeapWaypointObs;

impl InboundObserver for HeapWaypointObs {
    fn waypoint(&mut self, site: InboundWaypoint, frame: u32) {
        #[cfg(target_os = "espidf")]
        log_inbound_heap_wp(site, frame);
        #[cfg(not(target_os = "espidf"))]
        let _ = (site, frame);
    }

    fn hello_ok(&mut self, format: PlaybackFormat) {
        #[cfg(target_os = "espidf")]
        log_inbound_hello_ok(format);
        #[cfg(not(target_os = "espidf"))]
        let _ = format;
    }
}

/// Emit a post-Hello inbound-window heap waypoint: the same field set as the
/// streamer's intra-segment waypoints (heap_free / min_heap / largest_free) plus the
/// boot-wide allocation-failure count. min_heap carries the low-water mark forward, so
/// even a coarse cadence retroactively catches a transient dive.
#[cfg(target_os = "espidf")]
fn log_inbound_heap_wp(site: InboundWaypoint, frame: u32) {
    let (free, min, largest) = crate::health::heap_waypoint();
    // Self-sample the pumping thread's stack high-water mark. On the production
    // idle-drain pump that is the streamer thread; under the rtd test it is the rtd
    // thread inside the post-Hello suspect window (harmless — the emitting context
    // disambiguates). HWM is fill-pattern derived, so a skip-over excursion
    // under-reports. Permanent observability: this field localizes a stack-HWM floor
    // trip to the inbound-decode window.
    // SAFETY: pure-read FreeRTOS query; NULL = the calling task.
    let shwm = unsafe { esp_idf_svc::sys::uxTaskGetStackHighWaterMark(core::ptr::null_mut()) };
    log::info!(
        "streamer: heap wp inbound {} frame={} heap_free={} min_heap={} largest_free={} alloc_fail={} shwm={}",
        site.as_str(),
        frame,
        free,
        min,
        largest,
        crate::alloc_probe::alloc_fail_count(),
        shwm,
    );
}

/// Log the accepted inbound format plus heap headroom at inbound-stream start: one line
/// per connection from a cheap pure-read query pair; the min-ever value dates any prior
/// low-water event relative to this connection.
#[cfg(target_os = "espidf")]
fn log_inbound_hello_ok(format: PlaybackFormat) {
    let (free, min) = crate::health::heap_free_min();
    log::info!(
        "streamer: inbound Hello ok — {} Hz / {} bit / {} ch / {:?} heap_free={} min_heap={} alloc_fail={}",
        format.sample_rate_hz,
        format.bits_per_sample,
        format.channels,
        format.codec,
        free,
        min,
        crate::alloc_probe::alloc_fail_count(),
    );
}
