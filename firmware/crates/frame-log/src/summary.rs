//! What a frame log holds, in one pass of the walker: the hello and a
//! capture's description, segments with what lies between them, every loss
//! and every break inside a segment, a capture's blocks, late runs and wall
//! steps, the end and the tail.

use std::fmt;
use std::io;
use std::path::Path;

use audio_pipeline::wide::{LossCause, StreamEndReason};
use audio_pipeline::wire::{EndReason, Hello};
use serde::Serialize;

use crate::FrameLogError;
use crate::unmeasured::{SegmentEndHole, Unmeasured};
use crate::walk::{AudioStep, Step, Tail, Walk, walk};

/// A change of more than this, in microseconds, in stamp minus `mono_us`
/// between consecutive capture frames is a wall-clock step.
pub const WALL_STEP_THRESHOLD_US: i64 = 1_000;

/// Why a log could not be summarised.
#[derive(Debug)]
pub enum SummaryError {
    /// The header could not be read or is refused.
    Open(FrameLogError),
    /// A read failed after the header.
    Io(io::Error),
    /// The log holds no hello; the tail says where it stopped.
    NoHello(TailSummary),
}

impl fmt::Display for SummaryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SummaryError::Open(e) => e.fmt(f),
            SummaryError::Io(e) => write!(f, "I/O error reading frame log: {e}"),
            SummaryError::NoHello(t) => write!(
                f,
                "no hello: the log is {} at byte {} ({})",
                t.verdict, t.offset, t.reason
            ),
        }
    }
}

impl std::error::Error for SummaryError {}

#[derive(Debug, Serialize)]
pub struct Summary {
    /// The header metadata as written.
    pub header: serde_json::Value,
    /// `header.capture.record_id`, if the log has one.
    pub record_id: Option<String>,
    pub hello: Hello,
    pub capture: Option<CaptureSummary>,
    /// From the first position to the furthest, measured losses included.
    pub frames_delivered: u64,
    /// Frames held in audio frames.
    pub frames_recorded: u64,
    /// `frames_delivered` at the hello's rate, in seconds.
    pub duration_frames_s: f64,
    /// From the start pair (or, with no `Describe`, the first segment's base
    /// time) to the end of the last audio frame, on the frames'
    /// `device_ts_us`; `None` with no audio.
    pub duration_clock_s: Option<f64>,
    pub segments: Vec<SegmentSummary>,
    /// Every loss and every break inside a segment. Empty states that there
    /// were none.
    pub discontinuities: Vec<Discontinuity>,
    /// `None` for a log with no `Describe`, which does not measure them.
    pub blocks: Option<u64>,
    pub short_blocks: Option<Vec<ShortBlock>>,
    pub late: Option<Vec<LateRun>>,
    pub wall_steps: Option<Vec<WallStep>>,
    pub other_frames: OtherFrames,
    pub end: Option<EndSummary>,
    pub tail: TailSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CaptureSummary {
    pub schema: u8,
    pub channel_names: Vec<String>,
    pub period_frames: u32,
    /// The clock pair read directly after the source started.
    pub started: ClockPair,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ClockPair {
    pub mono_us: u64,
    pub wall_us: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SegmentSummary {
    pub segment_id: u32,
    pub base_frame: u64,
    /// How far the base lies after the position: audio the stream did not
    /// send before this segment.
    pub not_sent_frames: u64,
    /// How far the base lies before the position: audio this segment sends
    /// again as pre-roll.
    pub resent_frames: u64,
    pub frames: u64,
    pub audio_frames: u64,
    /// The reason it closed; `None` if it never did.
    pub end: Option<EndReason>,
    /// `match` or `mismatch` of `samples_sent` against the frames received;
    /// `None` if it never closed.
    pub count_check: Option<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Discontinuity {
    /// `loss`, `gap`, `overlap`, `segment_end`, `segment_replaced` or
    /// `count_mismatch`.
    pub kind: &'static str,
    pub at_frame: u64,
    /// The length; `None` where it was not measured.
    pub frames: Option<u64>,
    /// For an unmeasured loss: the hole's length from `device_ts_us` on either
    /// side at the nominal rate, less the index's advance across it. Where
    /// several lie between the same two audio frames the first carries it and
    /// the others 0. `None` with no audio after it. Can be negative: the
    /// clock's error is not hidden.
    pub clock_estimate_frames: Option<i64>,
    pub segment_id: Option<u32>,
    pub cause: Option<Cause>,
    pub mono_us: u64,
    /// The record's stamp.
    pub wall_us: u64,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ShortBlock {
    pub first_frame: u64,
    pub frames: u64,
}

/// Consecutive late blocks with no other block, loss or segment boundary
/// between them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LateRun {
    pub first_frame: u64,
    pub blocks: u64,
    pub max_backlog_frames: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WallStep {
    pub at_frame: u64,
    pub mono_us: u64,
    /// How far the wall clock moved against the monotonic one; negative is
    /// backwards.
    pub step_us: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct OtherFrames {
    pub telemetry: u64,
    pub status: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EndSummary {
    pub reason: StreamEndReason,
    pub at_frame: u64,
    pub mono_us: u64,
    pub wall_us: u64,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TailSummary {
    /// `ended`, `unterminated`, `torn` or `corrupt`.
    pub verdict: &'static str,
    pub offset: u64,
    pub reason: String,
    pub open_segment: Option<u32>,
}

impl From<&Tail> for TailSummary {
    fn from(t: &Tail) -> Self {
        TailSummary {
            verdict: t.verdict.name(),
            offset: t.offset,
            reason: t.reason(),
            open_segment: t.open_segment,
        }
    }
}

/// A loss's cause, or the reason a segment closed, under the enum's own
/// serialized name.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Cause {
    Loss(LossCause),
    SegmentEnd(EndReason),
}

impl fmt::Display for Cause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match serde_json::to_value(self) {
            Ok(serde_json::Value::String(name)) => f.write_str(&name),
            _ => Err(fmt::Error),
        }
    }
}

/// The capture-only parts, present when the log has a `Describe`.
struct CaptureState {
    period: u64,
    blocks: u64,
    short_blocks: Vec<ShortBlock>,
    late: Vec<LateRun>,
    in_late_run: bool,
    wall_steps: Vec<WallStep>,
    /// Stamp minus `mono_us` of the last capture frame.
    wall_offset: i64,
}

impl CaptureState {
    fn note_pair(&mut self, at_frame: u64, stamp: u64, mono_us: u64) {
        let offset = stamp as i64 - mono_us as i64;
        let step_us = offset - self.wall_offset;
        if step_us.abs() > WALL_STEP_THRESHOLD_US {
            self.wall_steps.push(WallStep {
                at_frame,
                mono_us,
                step_us,
            });
        }
        self.wall_offset = offset;
    }
}

/// The running state of one summary.
struct Acc {
    capture: Option<CaptureState>,
    capture_summary: Option<CaptureSummary>,
    first_position: Option<u64>,
    furthest: u64,
    frames_recorded: u64,
    clock_start_us: Option<f64>,
    segments: Vec<SegmentSummary>,
    discontinuities: Vec<Discontinuity>,
    unmeasured: Unmeasured,
    /// Unmeasured discontinuities since the last audio frame.
    awaiting_estimate: Vec<usize>,
    other: OtherFrames,
    end: Option<EndSummary>,
}

impl Acc {
    fn reach(&mut self, index: u64) {
        self.first_position.get_or_insert(index);
        self.furthest = self.furthest.max(index);
    }

    fn push(&mut self, d: Discontinuity, unmeasured: bool) {
        if unmeasured {
            self.awaiting_estimate.push(self.discontinuities.len());
        }
        self.discontinuities.push(d);
    }

    fn segment_end_hole(&mut self, h: SegmentEndHole) {
        let cause = Cause::SegmentEnd(h.reason);
        self.push(
            Discontinuity {
                kind: "segment_end",
                at_frame: h.at_frame,
                frames: None,
                clock_estimate_frames: None,
                segment_id: Some(h.segment_id),
                cause: Some(cause),
                mono_us: h.device_ts_us,
                wall_us: h.stamp.0,
                detail: format!("segment closed with {cause}"),
            },
            true,
        );
    }

    fn end_late_run(&mut self) {
        if let Some(c) = self.capture.as_mut() {
            c.in_late_run = false;
        }
    }

    fn audio(&mut self, a: &AudioStep) {
        self.reach(a.first_frame + a.frames);
        self.frames_recorded += a.frames;
        if let Some(seg) = self.segments.last_mut() {
            seg.frames += a.frames;
            seg.audio_frames += 1;
        }
        if self.clock_start_us.is_none() {
            self.clock_start_us = Some(a.device_ts_us as f64);
        }
        if let Some(j) = a.jump {
            self.push(
                Discontinuity {
                    kind: j.kind.name(),
                    at_frame: j.expected,
                    frames: Some(j.frames),
                    clock_estimate_frames: None,
                    segment_id: Some(a.segment_id),
                    cause: None,
                    mono_us: a.device_ts_us,
                    wall_us: a.stamp.0,
                    detail: format!(
                        "audio at index {} where the position is {}",
                        a.first_frame, j.expected
                    ),
                },
                false,
            );
        }
        let estimates = self.unmeasured.resolve(Some(a));
        for (i, estimate) in self.awaiting_estimate.drain(..).zip(estimates) {
            self.discontinuities[i].clock_estimate_frames = estimate;
        }
    }

    fn take(&mut self, step: Step) {
        if let Some(h) = self.unmeasured.step(&step) {
            self.segment_end_hole(h);
        }
        match step {
            Step::Hello { .. } => {}
            Step::Describe { stamp, description } => {
                self.capture = Some(CaptureState {
                    period: u64::from(description.period_frames),
                    blocks: 0,
                    short_blocks: Vec::new(),
                    late: Vec::new(),
                    in_late_run: false,
                    wall_steps: Vec::new(),
                    wall_offset: stamp.0 as i64 - description.mono_us as i64,
                });
                self.clock_start_us = Some(description.mono_us as f64);
                self.capture_summary = Some(CaptureSummary {
                    schema: description.schema,
                    channel_names: description.channel_names,
                    period_frames: description.period_frames,
                    started: ClockPair {
                        mono_us: description.mono_us,
                        wall_us: stamp.0,
                    },
                });
            }
            Step::SegmentReplaced {
                stamp,
                segment_id,
                by,
                at_frame,
                device_ts_us,
            } => {
                self.end_late_run();
                self.push(
                    Discontinuity {
                        kind: "segment_replaced",
                        at_frame,
                        frames: None,
                        clock_estimate_frames: None,
                        segment_id: Some(segment_id),
                        cause: None,
                        mono_us: device_ts_us,
                        wall_us: stamp.0,
                        detail: format!("segment {segment_id} replaced by segment {by}"),
                    },
                    false,
                );
            }
            Step::SegmentOpened {
                start,
                not_sent_frames,
                resent_frames,
                ..
            } => {
                self.end_late_run();
                self.reach(start.base_sample_index);
                if self.clock_start_us.is_none() {
                    self.clock_start_us = Some(start.base_device_ts_us as f64);
                }
                self.segments.push(SegmentSummary {
                    segment_id: start.segment_id,
                    base_frame: start.base_sample_index,
                    not_sent_frames,
                    resent_frames,
                    frames: 0,
                    audio_frames: 0,
                    end: None,
                    count_check: None,
                });
            }
            Step::Audio(a) => self.audio(&a),
            Step::Read {
                stamp,
                first_frame,
                frames,
                mono_us,
                backlog_frames,
            } => {
                if let Some(c) = self.capture.as_mut() {
                    c.note_pair(first_frame, stamp.0, mono_us);
                    c.blocks += 1;
                    if frames < c.period {
                        c.short_blocks.push(ShortBlock {
                            first_frame,
                            frames,
                        });
                    }
                    if u64::from(backlog_frames) >= c.period {
                        match c.late.last_mut() {
                            Some(run) if c.in_late_run => {
                                run.blocks += 1;
                                run.max_backlog_frames = run.max_backlog_frames.max(backlog_frames);
                            }
                            _ => c.late.push(LateRun {
                                first_frame,
                                blocks: 1,
                                max_backlog_frames: backlog_frames,
                            }),
                        }
                        c.in_late_run = true;
                    } else {
                        c.in_late_run = false;
                    }
                }
            }
            Step::Loss {
                stamp,
                cause,
                at_frame,
                samples,
                mono_us,
                detail,
                segment_id,
            } => {
                self.end_late_run();
                if let Some(c) = self.capture.as_mut() {
                    c.note_pair(at_frame, stamp.0, mono_us);
                }
                self.reach(at_frame);
                self.reach(at_frame + samples.unwrap_or(0));
                self.push(
                    Discontinuity {
                        kind: "loss",
                        at_frame,
                        frames: samples,
                        clock_estimate_frames: None,
                        segment_id,
                        cause: Some(Cause::Loss(cause)),
                        mono_us,
                        wall_us: stamp.0,
                        detail,
                    },
                    samples.is_none(),
                );
            }
            Step::SegmentClosed {
                stamp,
                end,
                frames_received,
                at_frame,
                ..
            } => {
                self.end_late_run();
                let matched = end.samples_sent == frames_received;
                if let Some(seg) = self.segments.last_mut() {
                    seg.end = Some(end.reason);
                    seg.count_check = Some(if matched { "match" } else { "mismatch" });
                }
                if !matched {
                    self.push(
                        Discontinuity {
                            kind: "count_mismatch",
                            at_frame,
                            frames: None,
                            clock_estimate_frames: None,
                            segment_id: Some(end.segment_id),
                            cause: None,
                            mono_us: end.device_ts_us,
                            wall_us: stamp.0,
                            detail: format!(
                                "segment end says {} samples sent; {frames_received} received",
                                end.samples_sent
                            ),
                        },
                        false,
                    );
                }
            }
            Step::End {
                stamp,
                reason,
                at_frame,
                mono_us,
                detail,
            } => {
                self.end_late_run();
                if let Some(c) = self.capture.as_mut() {
                    c.note_pair(at_frame, stamp.0, mono_us);
                }
                self.reach(at_frame);
                self.end = Some(EndSummary {
                    reason,
                    at_frame,
                    mono_us,
                    wall_us: stamp.0,
                    detail,
                });
            }
            Step::Telemetry { .. } => self.other.telemetry += 1,
            Step::Status { .. } => self.other.status += 1,
        }
    }
}

/// The walk's first step, which is its hello when it has one.
pub(crate) fn first_hello(walk: &mut Walk) -> Result<Hello, SummaryError> {
    match walk.next() {
        Some(Ok(Step::Hello { hello, .. })) => Ok(hello),
        Some(Ok(_)) => unreachable!("the walker yields the hello first"),
        Some(Err(e)) => Err(SummaryError::Io(e)),
        None => {
            let tail = walk.tail().expect("iteration ended without an I/O error");
            Err(SummaryError::NoHello(tail.into()))
        }
    }
}

/// Summarise the frame log at `path`.
pub fn summarize(path: &Path) -> Result<Summary, SummaryError> {
    let mut walk = walk(path).map_err(SummaryError::Open)?;
    let header: serde_json::Value = serde_json::from_slice(walk.raw_meta())
        .map_err(|e| SummaryError::Open(FrameLogError::BadMeta(e)))?;
    let record_id = walk.meta().capture.as_ref().map(|c| c.record_id.clone());
    let hello = first_hello(&mut walk)?;

    let rate = f64::from(hello.sample_rate_hz);
    let mut acc = Acc {
        capture: None,
        capture_summary: None,
        first_position: None,
        furthest: 0,
        frames_recorded: 0,
        clock_start_us: None,
        segments: Vec::new(),
        discontinuities: Vec::new(),
        unmeasured: Unmeasured::new(rate),
        awaiting_estimate: Vec::new(),
        other: OtherFrames::default(),
        end: None,
    };
    for step in walk.by_ref() {
        acc.take(step.map_err(SummaryError::Io)?);
    }
    if let Some(h) = acc.unmeasured.finish() {
        acc.segment_end_hole(h);
    }
    let tail = TailSummary::from(walk.tail().expect("iteration ended without an I/O error"));

    let frames_delivered = acc
        .first_position
        .map_or(0, |first| acc.furthest.saturating_sub(first));
    let duration_clock_s = match (acc.clock_start_us, acc.unmeasured.clock_us()) {
        (Some(start), Some(end)) if acc.frames_recorded > 0 => Some((end - start) / 1e6),
        _ => None,
    };
    let (blocks, short_blocks, late, wall_steps) = match acc.capture {
        Some(c) => (
            Some(c.blocks),
            Some(c.short_blocks),
            Some(c.late),
            Some(c.wall_steps),
        ),
        None => (None, None, None, None),
    };
    Ok(Summary {
        header,
        record_id,
        capture: acc.capture_summary,
        frames_delivered,
        frames_recorded: acc.frames_recorded,
        duration_frames_s: frames_delivered as f64 / rate,
        duration_clock_s,
        segments: acc.segments,
        discontinuities: acc.discontinuities,
        blocks,
        short_blocks,
        late,
        wall_steps,
        other_frames: acc.other,
        end: acc.end,
        tail,
        hello,
    })
}
