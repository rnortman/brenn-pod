//! The `pod_status` JSONL line: one connection's status frames, summarised.
//!
//! A pod sends a status frame about once a second (`StreamFrame::Status`). Writing
//! each as a line would be a line a second per pod, almost all of them saying
//! nothing new, so [`PodStatusLog`] folds the frames in and writes a line only when
//! something a reader acts on changed — a counter moved, the speech gate opened or
//! closed — and otherwise every [`POD_STATUS_LINE_PERIOD_US`], so a quiet pod is
//! still on the record. The level blocks of every frame folded in are summarised
//! on the next line, so the lines together are a continuous record of the room.

use pod_ingest::{HostMicros, LevelBlock, PodCounters, SessionEvent};
use serde_json::{Map, Value, json};
use speech_pipeline::level::percentile;

/// Host µs after which a `pod_status` line is written even though nothing changed.
pub(crate) const POD_STATUS_LINE_PERIOD_US: u64 = 10_000_000;

/// Every [`PodCounters`] field, by its line name. Listed by hand so each `counters`
/// and `delta` key is a deliberate interface choice; `counter_fields_name_every_wire_field`
/// fails when a wire field is missing here or named differently.
fn counter_fields(c: &PodCounters) -> [(&'static str, Option<u32>); 15] {
    [
        ("capture_deficit_frames", c.capture_deficit_frames),
        ("capture_xruns", c.capture_xruns),
        ("playback_underruns", Some(c.playback_underruns)),
        ("playback_write_failures", c.playback_write_failures),
        ("playback_full_stalls", Some(c.playback_full_stalls)),
        ("playback_discards", c.playback_discards),
        ("onsets", Some(c.onsets)),
        ("segments_opened", Some(c.segments_opened)),
        ("segments_dropped", Some(c.segments_dropped)),
        ("ring_overruns", Some(c.ring_overruns)),
        ("gate_msg_drops", Some(c.gate_msg_drops)),
        ("connects", Some(c.connects)),
        ("connect_failures", Some(c.connect_failures)),
        ("status_skipped", Some(c.status_skipped)),
        ("alloc_failures", c.alloc_failures),
    ]
}

/// What the previous line said, for the change tests.
#[derive(Debug)]
struct LastLine {
    counters: PodCounters,
    gate_open: bool,
    at: HostMicros,
}

/// Frames folded in since the previous line.
#[derive(Debug, Default)]
struct Pending {
    /// `half_db` of each new level block.
    half_db: Vec<u8>,
    /// How many of those blocks carried the playing flag.
    playing: u32,
    /// Status frames folded in.
    frames: u32,
}

/// One connection's `pod_status` line state. A new connection starts a new log, so
/// the first line of a connection has no previous line to compare with.
#[derive(Debug, Default)]
pub(crate) struct PodStatusLog {
    last: Option<LastLine>,
    pending: Pending,
    /// The block index after the newest block folded in. A pod re-sends blocks a
    /// frame it could not finish already carried; those are not counted twice.
    next_block: Option<u64>,
}

impl PodStatusLog {
    /// Fold one session event in. Returns the `pod_status` line's fields when `ev`
    /// is a status frame and a line is due; `None` otherwise.
    ///
    /// The line, for a reader:
    ///
    /// - Its presence means the pod `pod` was alive and capturing at `device_ts_us`
    ///   (its own clock), with its capture write head at `capture_head`.
    /// - `counters` holds every counter the pod reports, each a total since the pod
    ///   booted, wrapping at `u32::MAX`. A total lower than the pod's previous line
    ///   means the pod rebooted. A counter that is `null` is one this pod does not
    ///   measure: a reader may conclude nothing from it, and in particular not that
    ///   the count is zero.
    /// - `delta` names the counters that moved since the previous line on the same
    ///   connection, each with how much it moved (modulo 2³²); a counter that did not
    ///   move is absent, and a `null` counter never appears. A non-empty
    ///   `delta.playback_underruns` means the pod's speaker output ran dry in that
    ///   interval. `delta.onsets` minus `delta.segments_opened` (an absent key is 0)
    ///   is how many utterances the pod's gate opened on in that interval and lost
    ///   before a segment reached the host. `delta` is `null` on a connection's
    ///   first line, which has no earlier line to compare with; compare its
    ///   `counters` with the pod's previous line instead.
    /// - `gauges` are the pod's readings when the frame was built; `null` is "not
    ///   available on this pod or not readable then", never zero. `gate_open` is
    ///   whether the pod's speech gate was open, and `gate_energy` the largest gate
    ///   input since the pod's previous frame. `stack_free_min` is the least free
    ///   stack, in bytes, of the pod's streamer thread since the pod booted; within one
    ///   boot it never rises, so a reader may alarm on it falling toward zero and may
    ///   take a rise as a reboot; `null` means this pod does not measure it and says
    ///   nothing about its margin. No gauge causes a line by itself but `gate_open`.
    /// - `levels` summarises every 100 ms room-level block received since the
    ///   previous line: how many (`blocks`), their 10th, 50th and 90th percentile in
    ///   dBFS (`null` when `blocks` is 0), and how many carried the pod's playing
    ///   flag (`playing`). Taken together the lines are the continuous record of the
    ///   room.
    /// - `frames` is how many status frames the line covers.
    ///
    /// A line is written on a connection's first status frame, whenever a counter
    /// moved or `gate_open` changed since the previous line, and otherwise once
    /// [`POD_STATUS_LINE_PERIOD_US`] of host time has passed since it (or the host
    /// clock stepped back past it).
    pub(crate) fn observe(&mut self, pod: Option<&str>, ev: &SessionEvent) -> Option<Value> {
        let SessionEvent::Status {
            host_rx,
            device_ts,
            capture_head,
            first_block,
            levels,
            counters,
            gauges,
            ..
        } = ev
        else {
            return None;
        };
        self.fold_levels(*first_block, levels);
        self.pending.frames += 1;

        let delta = self
            .last
            .as_ref()
            .map(|last| delta(&last.counters, counters));
        let due = match &self.last {
            None => true,
            Some(last) => {
                delta.as_ref().is_some_and(|d| !d.is_empty())
                    || last.gate_open != gauges.gate_open
                    || host_rx
                        .checked_delta(last.at)
                        .is_none_or(|d| d >= POD_STATUS_LINE_PERIOD_US)
            }
        };
        if !due {
            return None;
        }

        let pending = std::mem::take(&mut self.pending);
        self.last = Some(LastLine {
            counters: *counters,
            gate_open: gauges.gate_open,
            at: *host_rx,
        });
        let all: Map<String, Value> = counter_fields(counters)
            .into_iter()
            .map(|(name, v)| (name.to_string(), json!(v)))
            .collect();
        Some(json!({
            "pod": pod,
            "device_ts_us": device_ts.0,
            "capture_head": capture_head,
            "counters": all,
            "delta": delta.map(Value::Object),
            "gauges": gauges,
            "levels": level_summary(&pending),
            "frames": pending.frames,
        }))
    }

    /// Add the blocks of a run not already folded in.
    fn fold_levels(&mut self, first_block: u64, levels: &[LevelBlock]) {
        let skip = self
            .next_block
            .map_or(0, |next| next.saturating_sub(first_block));
        let fresh = levels
            .iter()
            .skip(usize::try_from(skip).unwrap_or(usize::MAX));
        for b in fresh {
            self.pending.half_db.push(b.half_db);
            if b.playing() {
                self.pending.playing += 1;
            }
        }
        let end = first_block + levels.len() as u64;
        self.next_block = Some(self.next_block.map_or(end, |next| next.max(end)));
    }
}

/// The counters measured on both sides that differ, with the wrapped difference.
fn delta(prev: &PodCounters, now: &PodCounters) -> Map<String, Value> {
    counter_fields(prev)
        .into_iter()
        .zip(counter_fields(now))
        .filter_map(|((name, before), (_, after))| match (before, after) {
            (Some(b), Some(a)) if a != b => Some((name.to_string(), json!(a.wrapping_sub(b)))),
            _ => None,
        })
        .collect()
}

/// The `levels` object of a line.
fn level_summary(p: &Pending) -> Value {
    let mut dbfs: Vec<f32> = p
        .half_db
        .iter()
        .map(|&h| {
            LevelBlock {
                half_db: h,
                flags: 0,
            }
            .dbfs()
        })
        .collect();
    json!({
        "blocks": dbfs.len(),
        "p10_dbfs": percentile(&mut dbfs, 10),
        "p50_dbfs": percentile(&mut dbfs, 50),
        "p90_dbfs": percentile(&mut dbfs, 90),
        "playing": p.playing,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pod_ingest::{DeviceMicros, PodGauges};

    const S: u64 = 1_000_000;

    fn counters() -> PodCounters {
        PodCounters {
            capture_deficit_frames: Some(0),
            alloc_failures: None,
            ..PodCounters::default()
        }
    }

    fn status(at_s: u64, first_block: u64, levels: &[(u8, bool)], c: PodCounters) -> SessionEvent {
        status_gate(at_s, first_block, levels, c, false)
    }

    fn status_gate(
        at_s: u64,
        first_block: u64,
        levels: &[(u8, bool)],
        counters: PodCounters,
        gate_open: bool,
    ) -> SessionEvent {
        SessionEvent::Status {
            host_rx: HostMicros(1_000 * S + at_s * S),
            device_ts: DeviceMicros(at_s * S),
            capture_head: first_block * 1_600,
            capture_head_device_ts: DeviceMicros(at_s * S),
            first_block,
            levels: levels
                .iter()
                .map(|&(half_db, playing)| LevelBlock {
                    half_db,
                    flags: u8::from(playing),
                })
                .collect(),
            counters,
            gauges: PodGauges {
                gate_open,
                rssi_dbm: Some(-61),
                stack_free_min: Some(9_000 - u32::try_from(at_s).unwrap()),
                ..PodGauges::default()
            },
            segment_reach: 16_000,
        }
    }

    fn ten(half_db: u8) -> Vec<(u8, bool)> {
        vec![(half_db, false); 10]
    }

    #[test]
    fn counter_fields_name_every_wire_field() {
        let wire = serde_json::to_value(PodCounters::default()).unwrap();
        let mut wire_names: Vec<&str> = wire
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        let mut ours: Vec<&str> = counter_fields(&PodCounters::default())
            .iter()
            .map(|(n, _)| *n)
            .collect();
        wire_names.sort_unstable();
        ours.sort_unstable();
        assert_eq!(ours, wire_names);
    }

    #[test]
    fn first_frame_writes_a_line_with_no_delta() {
        let mut log = PodStatusLog::default();
        let line = log
            .observe(Some("pod-a"), &status(0, 0, &ten(150), counters()))
            .expect("first frame writes");
        assert_eq!(line["pod"], "pod-a");
        assert_eq!(line["device_ts_us"], 0);
        assert!(line["delta"].is_null());
        assert_eq!(line["frames"], 1);
        assert_eq!(line["levels"]["blocks"], 10);
        assert_eq!(line["levels"]["p50_dbfs"], -75.0);
        assert_eq!(line["gauges"]["rssi_dbm"], -61);
        assert_eq!(line["gauges"]["gate_open"], false);
        assert_eq!(line["gauges"]["stack_free_min"], 9_000);
        assert!(line["gauges"]["heap_free"].is_null());
    }

    #[test]
    fn the_stack_margin_rides_gauges_and_never_causes_a_line() {
        let mut log = PodStatusLog::default();
        assert!(
            log.observe(None, &status(0, 0, &ten(150), counters()))
                .is_some()
        );
        // Each frame reports a lower margin; none of them is due on that alone.
        for s in 1..10 {
            assert!(
                log.observe(None, &status(s, s * 10, &ten(150), counters()))
                    .is_none(),
                "no line at {s} s"
            );
        }
        let mut unmeasured = status(10, 100, &ten(150), counters());
        if let SessionEvent::Status { gauges, .. } = &mut unmeasured {
            gauges.stack_free_min = None;
        }
        let line = log.observe(None, &unmeasured).expect("line at 10 s");
        assert!(
            line["gauges"]["stack_free_min"].is_null(),
            "not measured is null, never 0"
        );
    }

    #[test]
    fn an_unchanged_pod_writes_every_ten_seconds() {
        let mut log = PodStatusLog::default();
        assert!(
            log.observe(None, &status(0, 0, &ten(150), counters()))
                .is_some()
        );
        for s in 1..10 {
            assert!(
                log.observe(None, &status(s, s * 10, &ten(150), counters()))
                    .is_none(),
                "no line at {s} s"
            );
        }
        let line = log
            .observe(None, &status(10, 100, &ten(150), counters()))
            .expect("line at 10 s");
        assert_eq!(line["frames"], 10);
        assert_eq!(line["levels"]["blocks"], 100);
        assert_eq!(line["delta"], json!({}));
    }

    #[test]
    fn a_host_clock_step_back_writes_a_line() {
        let mut log = PodStatusLog::default();
        log.observe(None, &status(5, 0, &[], counters()));
        assert!(log.observe(None, &status(1, 0, &[], counters())).is_some());
    }

    #[test]
    fn a_counter_change_writes_a_line_naming_only_what_moved() {
        let mut log = PodStatusLog::default();
        log.observe(None, &status(0, 0, &[], counters()));
        let mut c = counters();
        c.playback_underruns = 2;
        c.connects = 1;
        let line = log
            .observe(None, &status(1, 0, &[], c))
            .expect("counter change writes");
        assert_eq!(
            line["delta"],
            json!({"playback_underruns": 2, "connects": 1})
        );
        assert_eq!(line["counters"]["playback_underruns"], 2);
        assert_eq!(line["counters"]["capture_deficit_frames"], 0);
        // The next frame with the same totals writes nothing.
        assert!(log.observe(None, &status(2, 0, &[], c)).is_none());
    }

    #[test]
    fn a_counter_not_measured_is_null_and_never_moves() {
        let mut log = PodStatusLog::default();
        let line = log.observe(None, &status(0, 0, &[], counters())).unwrap();
        assert!(line["counters"]["alloc_failures"].is_null());
        assert!(line["counters"]["capture_xruns"].is_null());
        assert_eq!(
            line["counters"].as_object().unwrap().len(),
            15,
            "every counter is named, measured or not"
        );
        // A counter that stops being measured does not by itself write a line, and
        // never appears in `delta`.
        let mut c = counters();
        c.capture_deficit_frames = None;
        assert!(log.observe(None, &status(1, 0, &[], c)).is_none());
        c.connects = 3;
        let line = log.observe(None, &status(2, 0, &[], c)).unwrap();
        assert_eq!(line["delta"], json!({"connects": 3}));
    }

    #[test]
    fn a_wrapped_total_is_reported_as_it_is() {
        let mut log = PodStatusLog::default();
        let mut c = counters();
        c.status_skipped = u32::MAX - 1;
        log.observe(None, &status(0, 0, &[], c));
        c.status_skipped = 1;
        let line = log.observe(None, &status(1, 0, &[], c)).unwrap();
        assert_eq!(line["counters"]["status_skipped"], 1);
        assert_eq!(line["delta"]["status_skipped"], 3);
    }

    #[test]
    fn a_lower_total_on_a_new_connection_is_carried_through() {
        let mut c = counters();
        c.segments_opened = 40;
        let mut before = PodStatusLog::default();
        before.observe(None, &status(0, 0, &[], c));
        c.segments_opened = 2;
        let mut after = PodStatusLog::default();
        let line = after.observe(None, &status(1, 0, &[], c)).unwrap();
        assert_eq!(line["counters"]["segments_opened"], 2);
        assert!(line["delta"].is_null());
    }

    #[test]
    fn a_gate_change_writes_a_line() {
        let mut log = PodStatusLog::default();
        log.observe(None, &status_gate(0, 0, &[], counters(), false));
        let line = log
            .observe(None, &status_gate(1, 0, &[], counters(), true))
            .expect("gate opening writes");
        assert_eq!(line["gauges"]["gate_open"], true);
        assert!(
            log.observe(None, &status_gate(2, 0, &[], counters(), true))
                .is_none()
        );
        assert!(
            log.observe(None, &status_gate(3, 0, &[], counters(), false))
                .is_some()
        );
    }

    #[test]
    fn levels_summarise_every_new_block_once() {
        let mut log = PodStatusLog::default();
        // Blocks 0..10: nine at −80 dBFS and one at −60 dBFS, two flagged playing.
        let mut first: Vec<(u8, bool)> = vec![(160, false); 9];
        first.push((120, true));
        first[0].1 = true;
        log.observe(None, &status(0, 0, &first, counters()));
        // A re-sent frame repeats blocks 5..10 and adds 10..20 at −70 dBFS.
        let mut resent: Vec<(u8, bool)> = first[5..].to_vec();
        resent.extend(vec![(140, false); 10]);
        assert!(
            log.observe(None, &status(1, 5, &resent, counters()))
                .is_none()
        );
        let line = log.observe(None, &status(10, 20, &[], counters())).unwrap();
        // Only the ten new blocks, not the five repeated.
        assert_eq!(line["levels"]["blocks"], 10);
        assert_eq!(line["levels"]["playing"], 0);
        assert_eq!(line["levels"]["p50_dbfs"], -70.0);
        assert_eq!(line["frames"], 2);
    }

    #[test]
    fn level_percentiles_are_nearest_rank() {
        let mut log = PodStatusLog::default();
        // Ten blocks at −50 … −95 dBFS in 5 dB steps.
        let blocks: Vec<(u8, bool)> = (0..10).map(|i| (100 + 10 * i, i == 3)).collect();
        let line = log
            .observe(None, &status(0, 0, &blocks, counters()))
            .unwrap();
        assert_eq!(line["levels"]["p10_dbfs"], -95.0);
        assert_eq!(line["levels"]["p50_dbfs"], -75.0);
        assert_eq!(line["levels"]["p90_dbfs"], -55.0);
        assert_eq!(line["levels"]["playing"], 1);
    }

    #[test]
    fn a_line_with_no_blocks_has_null_percentiles() {
        let mut log = PodStatusLog::default();
        let line = log.observe(None, &status(0, 0, &[], counters())).unwrap();
        assert_eq!(line["levels"]["blocks"], 0);
        assert!(line["levels"]["p10_dbfs"].is_null());
        assert!(line["levels"]["p50_dbfs"].is_null());
        assert!(line["levels"]["p90_dbfs"].is_null());
    }

    #[test]
    fn other_events_are_ignored() {
        let mut log = PodStatusLog::default();
        let ev = SessionEvent::SegmentClosed {
            segment_id: 1,
            close: pod_ingest::SegmentClose::Truncated {
                cause: pod_ingest::CloseCause::Eof,
            },
            host_rx: HostMicros(0),
        };
        assert!(log.observe(None, &ev).is_none());
        // And they do not count as frames.
        let line = log.observe(None, &status(0, 0, &[], counters())).unwrap();
        assert_eq!(line["frames"], 1);
    }
}
