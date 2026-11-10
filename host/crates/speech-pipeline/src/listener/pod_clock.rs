//! The pod's capture position, projected through host time: the listener's one
//! source of "now" for anything it dates.
//!
//! Audio reaches the listener only while the pod's gate holds a segment open, so
//! the end of the last audio received stops moving the moment a segment closes —
//! while the brain thinks, while a reply plays — and a deadline dated from it lies
//! in the past by the length of that silence. The pod's status frame states its
//! capture position about once a second whether or not a segment is open; the
//! clock anchors on that and on audio, and projects forward at the capture rate
//! for the host time elapsed since the anchor.

use pod_ingest::HostMicros;

use crate::types::SPINE_FORMAT;

/// Where the pod's capture was at a host instant, and the projection from there.
/// Pure: every host time is supplied by the caller.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PodClock {
    /// `(absolute sample index, host time it was the capture position)`. `None`
    /// until this connection has said where its capture is.
    anchor: Option<(u64, HostMicros)>,
}

impl PodClock {
    /// Forget the anchor. A new connection may follow a pod reboot, which restarts
    /// the sample domain, so a position from the previous connection says nothing
    /// about this one.
    pub(crate) fn clear(&mut self) {
        self.anchor = None;
    }

    /// The pod's own statement of its position: `capture_head` was the next sample
    /// it would capture when the frame received at `host_rx` was built. Always
    /// adopted — it is the authority — so a projection that ran ahead (a host clock
    /// step forward) or behind is corrected here.
    pub(crate) fn anchor_status(&mut self, capture_head: u64, host_rx: HostMicros) {
        self.anchor = Some((capture_head, host_rx));
    }

    /// Audio ending at `end_sample` (exclusive) was received at `host_rx`. Adopted
    /// only when it is ahead of the projection: a segment's pre-roll describes
    /// audio captured up to a second before it is sent, and must not pull the clock
    /// back. With no anchor it is adopted as the first one.
    pub(crate) fn anchor_audio(&mut self, end_sample: u64, host_rx: HostMicros) {
        if self.project(host_rx).is_none_or(|now| end_sample > now) {
            self.anchor = Some((end_sample, host_rx));
        }
    }

    /// The capture position at host time `at`, or `None` with no anchor. A host
    /// time before the anchor's projects no elapsed time: the host stamp is the
    /// wall clock, a step backward would otherwise run the projection backward, and
    /// the next status frame re-anchors within a second.
    pub(crate) fn project(&self, at: HostMicros) -> Option<u64> {
        let (sample, anchored_at) = self.anchor?;
        let elapsed_us = at.checked_delta(anchored_at).unwrap_or(0);
        Some(sample.saturating_add(samples_in(elapsed_us)))
    }
}

/// Samples the pod captures in `micros` at the capture rate, rounded down.
fn samples_in(micros: u64) -> u64 {
    let rate = u128::from(SPINE_FORMAT.sample_rate_hz);
    u64::try_from(u128::from(micros) * rate / 1_000_000).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    const T0: HostMicros = HostMicros(1_700_000_000_000_000);

    fn after(t: HostMicros, ms: u64) -> HostMicros {
        HostMicros(t.0 + ms * 1_000)
    }

    #[test]
    fn no_anchor_projects_nothing() {
        assert_eq!(PodClock::default().project(T0), None);
    }

    #[test]
    fn a_status_anchor_projects_at_sixteen_samples_a_millisecond() {
        let mut clock = PodClock::default();
        clock.anchor_status(1_000_000, T0);
        assert_eq!(clock.project(T0), Some(1_000_000));
        assert_eq!(clock.project(after(T0, 8_500)), Some(1_136_000));
        assert_eq!(
            clock.project(HostMicros(T0.0 + 999)),
            Some(1_000_015),
            "rounded down"
        );
    }

    #[test]
    fn a_host_time_before_the_anchor_projects_no_elapsed_time() {
        let mut clock = PodClock::default();
        clock.anchor_status(1_000_000, T0);
        assert_eq!(clock.project(HostMicros(T0.0 - 5_000_000)), Some(1_000_000));
    }

    #[test]
    fn a_status_frame_always_re_anchors_even_backward() {
        let mut clock = PodClock::default();
        clock.anchor_status(1_000_000, T0);
        // A host clock that stepped forward ran the projection ahead; the pod's
        // own word replaces it.
        clock.anchor_status(1_010_000, after(T0, 60_000));
        assert_eq!(clock.project(after(T0, 60_000)), Some(1_010_000));
    }

    #[test]
    fn audio_behind_the_projection_does_not_move_the_clock() {
        let mut clock = PodClock::default();
        clock.anchor_status(2_000_000, T0);
        // A pre-roll frame received 100 ms later describes audio a second old.
        clock.anchor_audio(1_985_600, after(T0, 100));
        assert_eq!(clock.project(after(T0, 100)), Some(2_001_600));
    }

    #[test]
    fn audio_ahead_of_the_projection_re_anchors() {
        let mut clock = PodClock::default();
        clock.anchor_status(2_000_000, T0);
        clock.anchor_audio(2_003_200, after(T0, 100));
        assert_eq!(clock.project(after(T0, 100)), Some(2_003_200));
        assert_eq!(clock.project(after(T0, 200)), Some(2_004_800));
    }

    #[test]
    fn audio_with_no_anchor_is_the_first_anchor() {
        let mut clock = PodClock::default();
        clock.anchor_audio(512, T0);
        assert_eq!(clock.project(after(T0, 1_000)), Some(16_512));
    }

    #[test]
    fn clear_forgets_the_anchor() {
        let mut clock = PodClock::default();
        clock.anchor_status(2_000_000, T0);
        clock.clear();
        assert_eq!(clock.project(T0), None);
    }
}
