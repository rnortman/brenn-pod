//! Clock newtypes. Two distinct time domains that must never be mixed:
//! `DeviceMicros` (the pod's clock, for intra-segment sample math only) and
//! `HostMicros` (microseconds since the UNIX epoch on the host). Cross-domain
//! arithmetic does not compile — each type exposes only same-domain deltas.
//!
//! [`ClockOffsetEstimate`] is the one sanctioned crossing, and it is a type
//! rather than a call-site expression precisely so the crossing stays countable:
//! casual mixing still does not compile, and the estimate's fuzziness is
//! documented where it is produced instead of at each consumer.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Device-clock microseconds. Meaningful only relative to other `DeviceMicros`
/// from the same connection; used for intra-segment sample-offset math, never
/// for host-side latency measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DeviceMicros(pub u64);

/// Host-clock microseconds since the UNIX epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct HostMicros(pub u64);

impl DeviceMicros {
    /// Microseconds elapsed from `earlier` to `self`, or `None` if `earlier`
    /// is later (the caller counts and clamps rather than panicking).
    pub fn checked_delta(self, earlier: DeviceMicros) -> Option<u64> {
        self.0.checked_sub(earlier.0)
    }

    /// This instant advanced by `micros` on the same clock — the intra-segment
    /// sample-offset math this domain exists for (e.g. "the device time of the
    /// sample `n` samples past the segment base"). Saturating: the device clock
    /// is monotonic microseconds since boot, so overflow is unreachable.
    pub fn advanced_by(self, micros: u64) -> DeviceMicros {
        DeviceMicros(self.0.saturating_add(micros))
    }
}

/// Microseconds of audio `samples` occupy at `sample_rate_hz`. A zero rate
/// yields 0 rather than dividing by zero (the format gate rejects a zero-rate
/// `Hello`, so this is a guard, not a case).
pub fn samples_to_micros(samples: u64, sample_rate_hz: u32) -> u64 {
    if sample_rate_hz == 0 {
        return 0;
    }
    samples.saturating_mul(1_000_000) / u64::from(sample_rate_hz)
}

/// The device-boot-clock → host-clock offset, estimated from audio chunks the
/// pod already sends. **The only sanctioned `DeviceMicros` → `HostMicros`
/// crossing.**
///
/// The device runs no time sync — `device_ts` is microseconds since *boot* — so
/// there is no shared wall clock to compare against. What is available is the
/// arrival record: for each chunk, `host_rx − device_ts` is the boot-clock
/// offset plus that chunk's transport delay. A min filter over many chunks keeps
/// the least-delayed observation, which approaches (boot→host offset) + (minimum
/// one-way transport delay).
///
/// `device_ts` stamps a chunk's **first** sample, while the chunk cannot be sent
/// before its **last** sample is captured, so each observation subtracts the
/// chunk's own span; without that term the estimate carries a systematic
/// one-frame lateness that reads as transport delay but is not.
///
/// **Fuzziness (stated once, here):** a projected instant is *late* by the
/// minimum one-way transport delay (wifi — typically tens of ms) and jittered by
/// scheduling. Intra-segment clock drift is ppm-negligible. Chunks sent faster
/// than real time (a segment's preroll backlog draining at 4×) can only *raise*
/// `host_rx − device_ts`, so the min filter is robust to them; the caller
/// excludes them anyway to keep the inputs homogeneous.
///
/// An estimator only exists once it holds an observation — hence no `new()` and
/// an infallible [`project`](ClockOffsetEstimate::project). A caller with no
/// observations holds `None` and has no instant to project, which is the honest
/// shape of "no estimate yet".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockOffsetEstimate {
    sample_rate_hz: u32,
    /// Least observed `host_rx − device_ts − chunk_span`, in µs. Signed: a
    /// synthetic or pre-epoch host clock can put it below zero, and clamping it
    /// at the observation would silently bias every projection.
    d_min: i64,
}

impl ClockOffsetEstimate {
    /// Start an estimate from its first chunk observation.
    pub fn from_observation(
        host_rx: HostMicros,
        device_ts: DeviceMicros,
        chunk_samples: u64,
        sample_rate_hz: u32,
    ) -> ClockOffsetEstimate {
        let mut est = ClockOffsetEstimate {
            sample_rate_hz,
            d_min: i64::MAX,
        };
        est.observe(host_rx, device_ts, chunk_samples);
        est
    }

    /// Fold one chunk's arrival into the estimate, narrowing it if this chunk was
    /// the least delayed seen so far.
    pub fn observe(&mut self, host_rx: HostMicros, device_ts: DeviceMicros, chunk_samples: u64) {
        let span = samples_to_micros(chunk_samples, self.sample_rate_hz) as i64;
        let d = (host_rx.0 as i64)
            .saturating_sub(device_ts.0 as i64)
            .saturating_sub(span);
        self.d_min = self.d_min.min(d);
    }

    /// Project a device instant onto the host clock. Late by the minimum
    /// transport delay — see the type's fuzziness note.
    pub fn project(&self, device_ts: DeviceMicros) -> HostMicros {
        HostMicros(
            self.d_min
                .saturating_add(device_ts.0 as i64)
                .max(0)
                .unsigned_abs(),
        )
    }
}

impl HostMicros {
    /// Current host time as microseconds since the UNIX epoch. A pre-epoch
    /// clock yields `0` (`unwrap_or_default`), matching the host-side epoch
    /// convention used elsewhere in the codebase.
    pub fn now() -> HostMicros {
        HostMicros(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64,
        )
    }

    /// Microseconds elapsed from `earlier` to `self`, or `None` if `earlier`
    /// is later — e.g. an NTP step moved the clock backward between stamps.
    pub fn checked_delta(self, earlier: HostMicros) -> Option<u64> {
        self.0.checked_sub(earlier.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_delta_zero() {
        assert_eq!(DeviceMicros(42).checked_delta(DeviceMicros(42)), Some(0));
    }

    #[test]
    fn device_delta_positive() {
        assert_eq!(
            DeviceMicros(1_000).checked_delta(DeviceMicros(600)),
            Some(400)
        );
    }

    #[test]
    fn device_delta_negative_is_none() {
        assert_eq!(DeviceMicros(5).checked_delta(DeviceMicros(9)), None);
    }

    #[test]
    fn host_delta_positive() {
        assert_eq!(
            HostMicros(2_000_000).checked_delta(HostMicros(1_999_000)),
            Some(1_000)
        );
    }

    #[test]
    fn host_delta_negative_is_none() {
        // NTP step backward between two stamps.
        assert_eq!(HostMicros(10).checked_delta(HostMicros(11)), None);
    }

    #[test]
    fn ordering() {
        assert!(HostMicros(1) < HostMicros(2));
        assert!(DeviceMicros(9) > DeviceMicros(8));
    }

    #[test]
    fn now_is_after_epoch() {
        // Any real host clock is well past the epoch.
        assert!(HostMicros::now() > HostMicros(0));
    }

    #[test]
    fn advanced_by_is_same_domain_offset() {
        assert_eq!(DeviceMicros(1_000).advanced_by(500), DeviceMicros(1_500));
        assert_eq!(
            DeviceMicros(u64::MAX).advanced_by(1),
            DeviceMicros(u64::MAX)
        );
    }

    #[test]
    fn samples_to_micros_converts_at_the_spine_rate() {
        assert_eq!(samples_to_micros(16_000, 16_000), 1_000_000);
        assert_eq!(samples_to_micros(512, 16_000), 32_000);
        assert_eq!(samples_to_micros(0, 16_000), 0);
        // Guard, not a case: the format gate rejects a zero-rate Hello.
        assert_eq!(samples_to_micros(512, 0), 0);
    }

    /// The estimate is `host_rx − device_ts − chunk_span`: without the span term
    /// it would report the chunk's own duration as transport delay.
    #[test]
    fn observation_subtracts_the_chunk_span() {
        // A 512-sample chunk (32 ms) whose first sample was captured at device
        // 1_000_000 and which arrived at host 5_000_100_000: the chunk's last
        // sample existed at device 1_032_000, so the true offset is
        // 5_000_100_000 − 1_032_000.
        let est = ClockOffsetEstimate::from_observation(
            HostMicros(5_000_100_000),
            DeviceMicros(1_000_000),
            512,
            16_000,
        );
        assert_eq!(
            est.project(DeviceMicros(1_032_000)),
            HostMicros(5_000_100_000)
        );
    }

    /// Later, more-delayed chunks never widen the estimate; a less-delayed one
    /// narrows it. This is what makes the preroll backlog (sent at 4× real time)
    /// harmless.
    #[test]
    fn min_filter_keeps_the_least_delayed_observation() {
        let mut est = ClockOffsetEstimate::from_observation(
            HostMicros(1_100_000),
            DeviceMicros(0),
            0,
            16_000,
        );
        // 200 ms later on the device, but 500 ms later on the host: 300 ms of
        // extra delay. Ignored.
        est.observe(HostMicros(1_600_000), DeviceMicros(200_000), 0);
        assert_eq!(est.project(DeviceMicros(0)), HostMicros(1_100_000));
        // A chunk that beat the incumbent by 40 ms narrows the estimate.
        est.observe(HostMicros(1_360_000), DeviceMicros(300_000), 0);
        assert_eq!(est.project(DeviceMicros(0)), HostMicros(1_060_000));
    }

    #[test]
    fn project_round_trips_an_observed_instant() {
        let est = ClockOffsetEstimate::from_observation(
            HostMicros(1_700_000_000_000_000),
            DeviceMicros(42_000_000),
            160,
            16_000,
        );
        // The observed chunk's *end* is the instant the observation pins.
        assert_eq!(
            est.project(DeviceMicros(42_010_000)),
            HostMicros(1_700_000_000_000_000)
        );
        // Projection is affine in the device instant.
        assert_eq!(
            est.project(DeviceMicros(42_011_000)),
            HostMicros(1_700_000_000_001_000)
        );
    }

    /// A device instant far enough before the offset to project below the epoch
    /// floors at 0 rather than wrapping — only reachable with a synthetic or
    /// pre-epoch host clock.
    #[test]
    fn project_floors_at_the_epoch() {
        let est =
            ClockOffsetEstimate::from_observation(HostMicros(0), DeviceMicros(9_000), 0, 16_000);
        assert_eq!(est.project(DeviceMicros(0)), HostMicros(0));
        assert_eq!(est.project(DeviceMicros(9_500)), HostMicros(500));
    }
}
