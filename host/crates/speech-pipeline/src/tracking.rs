//! The tracking-event emitter: turns an assembled `Segment` into the
//! `TrackingEvent` the passive-speaker-tracking path consumes.
//!
//! Emitted for **every** assembled segment, before and regardless of the wake
//! gate — the future arbitrator triangulates speakers from `(pod, DoA, time)`
//! whether or not the wake word fired. Telemetry loss is real (the device drops
//! oldest telemetry under pressure), so a segment with no telemetry yields an
//! event with empty tracks rather than none: DoA absence is itself data.

use pod_ingest::TelemetryKind;

use crate::types::{DoaTrack, Segment, TrackingEvent};

/// Build the `TrackingEvent` for an assembled segment.
///
/// Splits `Segment.telemetry` by kind: `Azimuths` readings become the DoA
/// track, `SpEnergy` readings the energy track, each carrying its sample
/// offset. Identity, end info, and the audio reference are copied through.
pub fn tracking_event(seg: &Segment) -> TrackingEvent {
    let mut energy = Vec::new();
    for t in &seg.telemetry {
        if let TelemetryKind::SpEnergy { values } = t.kind {
            energy.push((t.sample_offset, values));
        }
    }
    TrackingEvent {
        pod: seg.pod.clone(),
        room: seg.room.clone(),
        segment_id: seg.segment_id,
        // Host-clock segment bounds: first-frame arrival and the recorded
        // `SegmentEnd` receive time. A host-capped segment has no
        // `segment_end_rx` (it is finalized before the real `SegmentEnd`), so
        // fall back to the start rather than fabricate a time.
        start: seg.host_rx,
        end: seg.timings.segment_end_rx.unwrap_or(seg.host_rx),
        end_info: seg.end.clone(),
        doa: DoaTrack::from_telemetry(&seg.telemetry),
        energy,
        audio_ref: seg.audio_ref.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pod_ingest::HostMicros;

    use crate::types::{PodId, SegmentTelemetry, test_segment};

    #[test]
    fn splits_doa_and_energy_by_kind() {
        let seg = test_segment(vec![
            SegmentTelemetry {
                sample_offset: 0,
                kind: TelemetryKind::Azimuths {
                    values: [1.0, f32::NAN, 2.0, 3.0],
                },
            },
            SegmentTelemetry {
                sample_offset: 160,
                kind: TelemetryKind::SpEnergy {
                    values: [0.1, 0.2, 0.3, 0.4],
                },
            },
            SegmentTelemetry {
                sample_offset: 320,
                kind: TelemetryKind::Azimuths {
                    values: [4.0, 5.0, 6.0, 7.0],
                },
            },
        ]);
        let ev = tracking_event(&seg);

        assert_eq!(ev.doa.0.len(), 2);
        assert_eq!(ev.doa.0[0].0, 0);
        assert_eq!(ev.doa.0[0].1[0], 1.0);
        assert!(ev.doa.0[0].1[1].is_nan());
        assert_eq!(ev.doa.0[1].0, 320);

        assert_eq!(ev.energy.len(), 1);
        assert_eq!(ev.energy[0], (160, [0.1, 0.2, 0.3, 0.4]));

        // Identity and bounds copy through.
        assert_eq!(ev.pod, PodId("pod-x".into()));
        assert_eq!(ev.segment_id, 7);
        assert_eq!(ev.start, HostMicros(1_000));
        assert_eq!(ev.end, HostMicros(5_000));
    }

    #[test]
    fn empty_telemetry_yields_empty_tracks() {
        let ev = tracking_event(&test_segment(vec![]));
        assert!(ev.doa.0.is_empty());
        assert!(ev.energy.is_empty());
        // The event is still emitted with full identity — absence is data.
        assert_eq!(ev.segment_id, 7);
    }

    #[test]
    fn missing_segment_end_falls_back_to_start() {
        let mut seg = test_segment(vec![]);
        seg.timings.segment_end_rx = None;
        let ev = tracking_event(&seg);
        assert_eq!(ev.end, seg.host_rx);
    }
}
