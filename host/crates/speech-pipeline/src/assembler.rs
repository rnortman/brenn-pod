//! The segment assembler: a synchronous, per-connection state machine that
//! folds a connection's `SessionEvent`s into whole `Segment`s.
//!
//! It is pure and embeddable — `speech-surface` calls `on_event` inline in the
//! ingest task, so ingest→assembler backpressure is TCP backpressure by
//! construction (a stalled assembler stalls the socket read, never drops
//! mid-segment audio).
//!
//! The assembler reads no clock: every host-clock stamp it records comes from
//! the event timestamps the FSM already carried. The caller stamps
//! `timings.assembled = HostMicros::now()` on the returned `Segment` (assembly
//! is synchronous, so that is also the ingest-side handoff time).

use pod_ingest::{DeviceMicros, EndReason, HostMicros, SegmentClose, SegmentRef, SessionEvent};

use crate::types::{
    PodId, RoomId, Segment, SegmentEndCause, SegmentEndInfo, SegmentTelemetry, StageTimings,
};

/// Bounds the assembler enforces on a single segment. Both guard memory: the
/// wire places no bound on segment duration or telemetry rate.
#[derive(Debug, Clone, Copy)]
pub struct AssemblerLimits {
    /// Host-side length cap. At this many accumulated samples the current part is
    /// finalized with [`SegmentEndCause::HostCapped`] and a successor part opens
    /// immediately (same `segment_id`, `part + 1`, contiguous `base_sample_index`),
    /// so audio past the cap keeps flowing instead of being discarded.
    pub max_segment_samples: u64,
    /// Cap on retained telemetry readings per segment; further readings are
    /// dropped and counted.
    pub max_telemetry_readings: usize,
}

impl Default for AssemblerLimits {
    fn default() -> Self {
        Self {
            // 60 s at 16 kHz mono.
            max_segment_samples: 960_000,
            max_telemetry_readings: 4096,
        }
    }
}

/// Observability counters not carried on the returned `Segment`, surfaced into
/// `stage_health` so silent loss is visible.
#[derive(Debug, Default, Clone, Copy)]
pub struct AssemblerStats {
    /// Telemetry readings dropped because a segment hit `max_telemetry_readings`.
    pub telemetry_dropped: u64,
    /// Events that violated the FSM ordering contract the assembler assumes
    /// (audio/telemetry/close for a segment that is not open, or a `SegmentOpened`
    /// over an unfinished segment). Zero in correct operation; non-zero means an
    /// upstream invariant broke and data was dropped.
    pub contract_violations: u64,
}

/// A segment being accumulated.
#[derive(Debug)]
struct InProgress {
    segment_id: u32,
    /// Cap-rollover part index within `segment_id`; `0` for the first part.
    part: u16,
    base_sample_index: u64,
    preroll_samples: u32,
    base_device_ts: DeviceMicros,
    /// Host receive time of the segment's first frame (its `SegmentStart`).
    first_frame_rx: HostMicros,
    is_resume: bool,
    pcm: Vec<i16>,
    telemetry: Vec<SegmentTelemetry>,
    gap_count: u32,
}

/// What the assembler is currently doing.
#[derive(Debug)]
enum Current {
    /// No segment open.
    Idle,
    /// A segment (part) is accumulating.
    Open(InProgress),
}

/// Folds one connection's `SessionEvent`s into `Segment`s. One instance per
/// connection, driven inline by the ingest task.
#[derive(Debug)]
pub struct SegmentAssembler {
    pod: PodId,
    room: RoomId,
    limits: AssemblerLimits,
    current: Current,
    stats: AssemblerStats,
}

impl SegmentAssembler {
    /// Create an assembler for one connection's pod/room identity.
    pub fn new(pod: PodId, room: RoomId, limits: AssemblerLimits) -> Self {
        Self {
            pod,
            room,
            limits,
            current: Current::Idle,
            stats: AssemblerStats::default(),
        }
    }

    /// Observability counters not carried on returned segments.
    pub fn stats(&self) -> AssemblerStats {
        self.stats
    }

    /// Fold one session event. Returns a finished `Segment` when this event
    /// completes one (a `SegmentClosed`, or the host-side length cap firing on
    /// an `Audio` event). `audio_ref_log` is the current frame-log file name,
    /// stamped into the segment's `audio_ref`.
    pub fn on_event(&mut self, ev: &SessionEvent, audio_ref_log: &str) -> Option<Segment> {
        match ev {
            SessionEvent::SegmentOpened {
                segment_id,
                base_sample_index,
                preroll_samples,
                base_device_ts,
                is_resume,
                host_rx,
            } => {
                // The FSM force-truncates (and the assembler resolves) any prior
                // segment before opening a new one, so `current` is always `Idle`
                // here; a non-`Idle` state means an upstream ordering break that
                // would silently discard an unfinished segment's audio.
                if !matches!(self.current, Current::Idle) {
                    debug_assert!(false, "SegmentOpened while a segment was still open");
                    self.stats.contract_violations += 1;
                }
                self.current = Current::Open(InProgress {
                    segment_id: *segment_id,
                    part: 0,
                    base_sample_index: *base_sample_index,
                    preroll_samples: *preroll_samples,
                    base_device_ts: *base_device_ts,
                    first_frame_rx: *host_rx,
                    is_resume: *is_resume,
                    pcm: Vec::new(),
                    telemetry: Vec::new(),
                    gap_count: 0,
                });
                None
            }
            SessionEvent::Audio {
                segment_id,
                first_sample_index,
                pcm,
                gap,
                device_ts,
                host_rx,
            } => {
                let cap = self.limits.max_segment_samples;
                match &mut self.current {
                    Current::Open(ip) if ip.segment_id == *segment_id => {
                        ip.pcm.extend_from_slice(pcm);
                        if gap.is_some() {
                            ip.gap_count += 1;
                        }
                        if ip.pcm.len() as u64 >= cap {
                            // Finalize this part as HostCapped and immediately open a
                            // contiguous successor part, so audio past the cap keeps
                            // flowing instead of being discarded until VadRelease.
                            let successor = InProgress {
                                segment_id: ip.segment_id,
                                part: ip.part + 1,
                                // Anchor on the capping chunk's absolute index
                                // domain (`first_sample_index + chunk len`), not the
                                // accumulated `pcm.len()`: a mid-segment gap bumps
                                // `gap_count` but splices no samples, so `pcm.len()`
                                // trails the absolute index by the dropped-sample
                                // count. The capping event carries the exact base.
                                base_sample_index: *first_sample_index + pcm.len() as u64,
                                preroll_samples: 0,
                                // The successor's first sample is the end of the
                                // capping chunk; anchor its clocks on that chunk's
                                // event stamps (the assembler reads no clock, so this
                                // is the nearest available anchor — off by up to one
                                // chunk from the exact base_sample_index).
                                base_device_ts: *device_ts,
                                first_frame_rx: *host_rx,
                                is_resume: false,
                                pcm: Vec::new(),
                                telemetry: Vec::new(),
                                gap_count: 0,
                            };
                            let Current::Open(ip) =
                                std::mem::replace(&mut self.current, Current::Open(successor))
                            else {
                                unreachable!("guarded by the Open match arm")
                            };
                            let end = SegmentEndInfo::new(
                                SegmentEndCause::HostCapped,
                                ip.is_resume,
                                ip.gap_count,
                                None,
                            );
                            // No `SegmentEnd` has arrived, so segment_end_rx stays None.
                            return Some(self.build_segment(ip, end, None, audio_ref_log));
                        }
                        None
                    }
                    _ => {
                        debug_assert!(false, "Audio for a segment that is not open");
                        self.stats.contract_violations += 1;
                        None
                    }
                }
            }
            SessionEvent::Telemetry {
                segment_id,
                sample_offset,
                kind,
                ..
            } => {
                let max = self.limits.max_telemetry_readings;
                match &mut self.current {
                    Current::Open(ip) if ip.segment_id == *segment_id => {
                        if ip.telemetry.len() < max {
                            ip.telemetry.push(SegmentTelemetry {
                                sample_offset: *sample_offset,
                                kind: *kind,
                            });
                        } else {
                            self.stats.telemetry_dropped += 1;
                        }
                    }
                    _ => {
                        debug_assert!(false, "Telemetry for a segment that is not open");
                        self.stats.contract_violations += 1;
                    }
                }
                None
            }
            SessionEvent::SegmentClosed {
                segment_id,
                close,
                host_rx,
            } => match &self.current {
                Current::Open(ip) if ip.segment_id == *segment_id => {
                    let Current::Open(ip) = std::mem::replace(&mut self.current, Current::Idle)
                    else {
                        unreachable!("guarded by the Open match arm")
                    };
                    let end = end_info_from_close(close, ip.gap_count, ip.is_resume);
                    Some(self.build_segment(ip, end, Some(*host_rx), audio_ref_log))
                }
                _ => {
                    debug_assert!(false, "SegmentClosed for a segment that is not open");
                    self.stats.contract_violations += 1;
                    None
                }
            },
            // Handshake and protocol-error events are the caller's concern, not
            // the assembler's.
            SessionEvent::HelloAccepted { .. } | SessionEvent::ProtocolError { .. } => None,
        }
    }

    fn build_segment(
        &self,
        ip: InProgress,
        end: SegmentEndInfo,
        segment_end_rx: Option<HostMicros>,
        audio_ref_log: &str,
    ) -> Segment {
        Segment {
            pod: self.pod.clone(),
            room: self.room.clone(),
            segment_id: ip.segment_id,
            base_sample_index: ip.base_sample_index,
            preroll_samples: ip.preroll_samples,
            pcm: ip.pcm,
            device_ts: ip.base_device_ts,
            host_rx: ip.first_frame_rx,
            end,
            telemetry: ip.telemetry,
            audio_ref: SegmentRef {
                log: audio_ref_log.to_string(),
                segment_id: ip.segment_id,
                part: ip.part,
            },
            timings: StageTimings {
                first_frame_rx: Some(ip.first_frame_rx),
                segment_end_rx,
                // Stamped by the caller (HostMicros::now()) on return.
                assembled: None,
                tracking_emitted: None,
                // The rest are an utterance's stamps, not a transport segment's.
                ..StageTimings::default()
            },
        }
    }
}

/// Build the `SegmentEndInfo` for a wire close (`Completed`/`Truncated`).
fn end_info_from_close(close: &SegmentClose, gap_count: u32, resumed: bool) -> SegmentEndInfo {
    match close {
        SegmentClose::Completed {
            end_reason,
            cross_check,
            ..
        } => SegmentEndInfo::new(
            match end_reason {
                EndReason::VadRelease => SegmentEndCause::VadRelease,
                EndReason::Overrun => SegmentEndCause::Overrun,
                EndReason::InternalError => SegmentEndCause::InternalError,
            },
            resumed,
            gap_count,
            Some(*cross_check),
        ),
        // A truncated close carries no device counters to compare against.
        SegmentClose::Truncated { .. } => {
            SegmentEndInfo::new(SegmentEndCause::Truncated, resumed, gap_count, None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pod_ingest::{CrossCheck, Gap, TelemetryKind};

    /// Every wire `EndReason` maps to its own distinct `SegmentEndCause`, and none of
    /// them collapses into `Truncated` — a device-stated close is diagnosable, a
    /// truncation is not. Pins the mapping as a whole so a future wildcard arm
    /// (`_ => …`) added to quiet a new-variant error fails here.
    #[test]
    fn end_reason_maps_to_distinct_cause() {
        for (reason, want) in [
            (EndReason::VadRelease, SegmentEndCause::VadRelease),
            (EndReason::Overrun, SegmentEndCause::Overrun),
            (EndReason::InternalError, SegmentEndCause::InternalError),
        ] {
            let close = SegmentClose::Completed {
                end_reason: reason,
                frames_sent: 0,
                samples_sent: 0,
                cross_check: CrossCheck::Match,
            };
            let info = end_info_from_close(&close, 0, false);
            assert_eq!(info.cause, want, "wrong cause for {reason:?}");
            assert_ne!(
                info.cause,
                SegmentEndCause::Truncated,
                "{reason:?} must never fold into Truncated"
            );
        }
    }

    fn pod() -> PodId {
        PodId("pod-test".into())
    }
    fn room() -> RoomId {
        RoomId("kitchen".into())
    }

    fn assembler() -> SegmentAssembler {
        SegmentAssembler::new(pod(), room(), AssemblerLimits::default())
    }

    fn opened(segment_id: u32, base: u64, host_rx: u64, is_resume: bool) -> SessionEvent {
        SessionEvent::SegmentOpened {
            segment_id,
            base_sample_index: base,
            preroll_samples: 160,
            base_device_ts: DeviceMicros(1_000_000),
            is_resume,
            host_rx: HostMicros(host_rx),
        }
    }

    fn audio(segment_id: u32, first: u64, n: usize, gap: Option<Gap>) -> SessionEvent {
        SessionEvent::Audio {
            segment_id,
            first_sample_index: first,
            pcm: vec![0i16; n],
            device_ts: DeviceMicros(0),
            gap,
            host_rx: HostMicros(first),
        }
    }

    fn telemetry(segment_id: u32, offset: i64) -> SessionEvent {
        SessionEvent::Telemetry {
            segment_id,
            sample_offset: offset,
            kind: TelemetryKind::Azimuths {
                values: [0.1, f32::NAN, 0.3, 0.4],
            },
            device_ts: DeviceMicros(0),
            host_rx: HostMicros(0),
        }
    }

    fn completed(segment_id: u32, samples_sent: u64, received: u64, host_rx: u64) -> SessionEvent {
        SessionEvent::SegmentClosed {
            segment_id,
            close: SegmentClose::Completed {
                end_reason: EndReason::VadRelease,
                frames_sent: 1,
                samples_sent,
                cross_check: if samples_sent == received {
                    CrossCheck::Match
                } else {
                    CrossCheck::Mismatch {
                        sent: samples_sent,
                        received,
                    }
                },
            },
            host_rx: HostMicros(host_rx),
        }
    }

    fn truncated(segment_id: u32, host_rx: u64) -> SessionEvent {
        SessionEvent::SegmentClosed {
            segment_id,
            close: SegmentClose::Truncated {
                cause: pod_ingest::CloseCause::ReadError,
            },
            host_rx: HostMicros(host_rx),
        }
    }

    #[test]
    fn accumulates_pcm_and_yields_on_close() {
        let mut a = assembler();
        assert!(a.on_event(&opened(1, 0, 10, false), "log-a").is_none());
        assert!(a.on_event(&audio(1, 0, 320, None), "log-a").is_none());
        assert!(a.on_event(&audio(1, 320, 320, None), "log-a").is_none());
        let seg = a
            .on_event(&completed(1, 640, 640, 99), "log-a")
            .expect("close yields a segment");

        assert_eq!(seg.segment_id, 1);
        assert_eq!(seg.pcm.len(), 640);
        assert_eq!(seg.pod.0, "pod-test");
        assert_eq!(seg.room.0, "kitchen");
        assert_eq!(seg.base_sample_index, 0);
        assert_eq!(seg.preroll_samples, 160);
        assert_eq!(seg.audio_ref.log, "log-a");
        assert_eq!(seg.audio_ref.segment_id, 1);
        assert_eq!(seg.end.cause, SegmentEndCause::VadRelease);
        assert!(!seg.end.truncated);
        assert!(!seg.end.resumed);
        assert_eq!(seg.end.gap_count, 0);
        assert_eq!(seg.end.cross_check, Some(CrossCheck::Match));
    }

    #[test]
    fn timing_stamps_come_from_events() {
        let mut a = assembler();
        a.on_event(&opened(1, 0, 10, false), "log-a");
        a.on_event(&audio(1, 0, 320, None), "log-a");
        let seg = a.on_event(&completed(1, 320, 320, 99), "log-a").unwrap();

        // First-frame stamp is the SegmentOpened host_rx; end stamp is the close's.
        assert_eq!(seg.host_rx, HostMicros(10));
        assert_eq!(seg.timings.first_frame_rx, Some(HostMicros(10)));
        assert_eq!(seg.timings.segment_end_rx, Some(HostMicros(99)));
        // The caller stamps these; the assembler leaves them unset.
        assert_eq!(seg.timings.assembled, None);
        assert_eq!(seg.timings.tracking_emitted, None);
        // Device anchor carried from the SegmentOpened event.
        assert_eq!(seg.device_ts, DeviceMicros(1_000_000));
    }

    #[test]
    fn gap_count_accumulates() {
        let mut a = assembler();
        a.on_event(&opened(1, 0, 10, false), "log-a");
        a.on_event(&audio(1, 0, 320, None), "log-a");
        a.on_event(
            &audio(
                1,
                640,
                320,
                Some(Gap {
                    expected_index: 320,
                    got_index: 640,
                }),
            ),
            "log-a",
        );
        a.on_event(
            &audio(
                1,
                500,
                320,
                Some(Gap {
                    expected_index: 960,
                    got_index: 500,
                }),
            ),
            "log-a",
        );
        let seg = a.on_event(&completed(1, 960, 960, 99), "log-a").unwrap();
        assert_eq!(seg.end.gap_count, 2);
    }

    #[test]
    fn telemetry_attaches_and_caps_with_count() {
        let limits = AssemblerLimits {
            max_segment_samples: 960_000,
            max_telemetry_readings: 2,
        };
        let mut a = SegmentAssembler::new(pod(), room(), limits);
        a.on_event(&opened(1, 0, 10, false), "log-a");
        a.on_event(&telemetry(1, 0), "log-a");
        a.on_event(&telemetry(1, 320), "log-a");
        // Third reading is over the cap: dropped and counted.
        a.on_event(&telemetry(1, 640), "log-a");
        assert_eq!(a.stats().telemetry_dropped, 1);

        let seg = a.on_event(&completed(1, 0, 0, 99), "log-a").unwrap();
        assert_eq!(seg.telemetry.len(), 2);
        assert_eq!(seg.telemetry[0].sample_offset, 0);
        assert_eq!(seg.telemetry[1].sample_offset, 320);
    }

    #[test]
    fn truncated_and_resumed_flags() {
        let mut a = assembler();
        a.on_event(&opened(4, 0, 10, true), "log-a");
        a.on_event(&audio(4, 0, 320, None), "log-a");
        let seg = a.on_event(&truncated(4, 50), "log-a").unwrap();

        assert_eq!(seg.end.cause, SegmentEndCause::Truncated);
        assert!(seg.end.truncated);
        assert!(seg.end.resumed);
        assert_eq!(seg.end.cross_check, None);
    }

    #[test]
    fn host_cap_rolls_over_into_a_successor_part() {
        let limits = AssemblerLimits {
            max_segment_samples: 640,
            max_telemetry_readings: 4096,
        };
        let mut a = SegmentAssembler::new(pod(), room(), limits);
        a.on_event(&opened(1, 0, 10, false), "log-a");
        assert!(a.on_event(&audio(1, 0, 320, None), "log-a").is_none());
        // This frame reaches the cap: part 0 finalizes as HostCapped.
        let part0 = a
            .on_event(&audio(1, 320, 320, None), "log-a")
            .expect("cap yields a part");
        assert_eq!(part0.segment_id, 1);
        assert_eq!(part0.audio_ref.part, 0);
        assert_eq!(part0.end.cause, SegmentEndCause::HostCapped);
        assert!(part0.end.truncated);
        assert_eq!(part0.end.cross_check, None);
        assert_eq!(part0.pcm.len(), 640);
        // No real SegmentEnd yet, so the end stamp is unset.
        assert_eq!(part0.timings.segment_end_rx, None);

        // Audio past the cap now flows into the successor part instead of being
        // discarded, and telemetry lands there too.
        assert!(a.on_event(&audio(1, 640, 320, None), "log-a").is_none());
        assert!(a.on_event(&telemetry(1, 960), "log-a").is_none());
        assert_eq!(a.stats().telemetry_dropped, 0);

        // The real SegmentEnd closes the live successor part and yields it.
        let part1 = a
            .on_event(&completed(1, 1280, 1280, 99), "log-a")
            .expect("close yields the successor part");
        assert_eq!(part1.segment_id, 1);
        assert_eq!(part1.audio_ref.part, 1);
        // Contiguous base: part 0 held 640 samples starting at 0.
        assert_eq!(part1.base_sample_index, 640);
        assert_eq!(part1.pcm.len(), 320);
        assert_eq!(part1.telemetry.len(), 1);
        assert_eq!(part1.end.cause, SegmentEndCause::VadRelease);
        assert!(!part1.end.resumed);
        assert_eq!(part1.timings.segment_end_rx, Some(HostMicros(99)));

        // A new wire segment assembles cleanly afterward, back at part 0.
        a.on_event(&opened(2, 1280, 200, false), "log-a");
        let seg2 = a.on_event(&completed(2, 0, 0, 300), "log-a").unwrap();
        assert_eq!(seg2.segment_id, 2);
        assert_eq!(seg2.audio_ref.part, 0);
    }

    #[test]
    fn cap_rollover_can_chain_multiple_parts() {
        let limits = AssemblerLimits {
            max_segment_samples: 320,
            max_telemetry_readings: 4096,
        };
        let mut a = SegmentAssembler::new(pod(), room(), limits);
        a.on_event(&opened(1, 0, 10, false), "log-a");
        // Three consecutive cap-hitting chunks → parts 0, 1, 2.
        let p0 = a.on_event(&audio(1, 0, 320, None), "log-a").unwrap();
        let p1 = a.on_event(&audio(1, 320, 320, None), "log-a").unwrap();
        let p2 = a.on_event(&audio(1, 640, 320, None), "log-a").unwrap();
        assert_eq!(
            (p0.audio_ref.part, p1.audio_ref.part, p2.audio_ref.part),
            (0, 1, 2)
        );
        assert_eq!(
            (
                p0.base_sample_index,
                p1.base_sample_index,
                p2.base_sample_index
            ),
            (0, 320, 640)
        );
        assert!(p0.end.truncated && p1.end.truncated && p2.end.truncated);
    }

    /// A mid-segment gap (dropped samples that bump `gap_count` but splice no PCM)
    /// before the cap: the successor part's `base_sample_index` tracks the absolute
    /// sample-index domain of the capping chunk, not the shorter accumulated
    /// `pcm.len()`. Otherwise every later part is shifted behind the wire indices
    /// the listener/replay resolver key on.
    #[test]
    fn cap_rollover_successor_base_is_absolute_across_a_gap() {
        let limits = AssemblerLimits {
            max_segment_samples: 640,
            max_telemetry_readings: 4096,
        };
        let mut a = SegmentAssembler::new(pod(), room(), limits);
        a.on_event(&opened(1, 0, 10, false), "log-a");
        // 320 samples at [0, 320), then a 180-sample gap: the next chunk starts at
        // absolute 500, not 320. Splicing no silence, `pcm.len()` reaches 640 here.
        assert!(a.on_event(&audio(1, 0, 320, None), "log-a").is_none());
        let part0 = a
            .on_event(
                &audio(
                    1,
                    500,
                    320,
                    Some(Gap {
                        expected_index: 320,
                        got_index: 500,
                    }),
                ),
                "log-a",
            )
            .expect("cap yields part 0");
        assert_eq!(part0.pcm.len(), 640);
        assert_eq!(part0.end.gap_count, 1);
        // The capping chunk ended at absolute 500 + 320 = 820; the successor's base
        // is that, not 0 + 640.
        let part1 = a
            .on_event(&completed(1, 820, 820, 99), "log-a")
            .expect("close yields the successor");
        assert_eq!(part1.audio_ref.part, 1);
        assert_eq!(part1.base_sample_index, 820);
    }

    #[test]
    fn two_independent_segments() {
        let mut a = assembler();
        a.on_event(&opened(1, 0, 10, false), "log-a");
        a.on_event(&audio(1, 0, 320, None), "log-a");
        let s1 = a.on_event(&completed(1, 320, 320, 20), "log-a").unwrap();
        assert_eq!(s1.segment_id, 1);

        a.on_event(&opened(2, 400, 30, false), "log-a");
        a.on_event(&audio(2, 400, 160, None), "log-a");
        let s2 = a.on_event(&completed(2, 160, 160, 40), "log-a").unwrap();
        assert_eq!(s2.segment_id, 2);
        assert_eq!(s2.pcm.len(), 160);
    }
}
