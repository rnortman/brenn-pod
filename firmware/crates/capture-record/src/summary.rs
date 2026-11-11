//! What a capture record holds, in one pass: counts, durations on the frame
//! count and on the clock, short blocks, late runs, every gap with a clock
//! estimate for the unmeasured ones, wall-clock steps, the end and the tail.

use std::fmt;
use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;

use serde::Serialize;

use crate::{EndReason, GapCause, Header, Item, OpenError, RecordReader, Tail};

/// A difference between `wall_us − mono_us` of two consecutive records larger
/// than this, in microseconds, is reported as a wall-clock step.
pub const WALL_STEP_THRESHOLD_US: i64 = 1_000;

/// Why a record could not be read through.
#[derive(Debug)]
pub enum ReadError {
    /// The header could not be read or is not a capture record's.
    Open(OpenError),
    /// A read failed after the header.
    Io(io::Error),
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadError::Open(e) => e.fmt(f),
            ReadError::Io(e) => write!(f, "I/O error reading capture record: {e}"),
        }
    }
}

impl std::error::Error for ReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ReadError::Open(e) => Some(e),
            ReadError::Io(e) => Some(e),
        }
    }
}

/// The summary of one capture record.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Summary {
    pub record_id: String,
    /// The header metadata as the file holds it.
    pub header: serde_json::Value,
    /// The final delivered-frame position.
    pub frames_delivered: u64,
    /// Frames the record holds in blocks.
    pub frames_recorded: u64,
    /// `frames_delivered` at the granted rate, in seconds.
    pub duration_frames_s: f64,
    /// From `started.mono_us` to the capture of the last block's last frame
    /// (its `mono_us` less its backlog), in seconds; `None` with no block.
    pub duration_clock_s: Option<f64>,
    pub blocks: u64,
    /// Blocks shorter than a period.
    pub short_blocks: Vec<ShortBlock>,
    /// Runs of consecutive late blocks: `backlog_frames >= period_frames`.
    pub late: Vec<LateRun>,
    /// Every gap, in order. Empty states that there were none.
    pub gaps: Vec<GapSummary>,
    pub wall_steps: Vec<WallStep>,
    pub end: Option<EndSummary>,
    pub tail: TailSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ShortBlock {
    pub first_frame: u64,
    pub frames: u64,
}

/// Consecutive late blocks, with no shorter-backlog block and no event
/// between them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LateRun {
    /// The first late block's position.
    pub first_frame: u64,
    pub blocks: u64,
    pub max_backlog_frames: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GapSummary {
    pub cause: GapCause,
    pub at_frame: u64,
    /// The measured length; `None` for a gap that was not measured.
    pub frames: Option<u64>,
    /// For an unmeasured gap: the hole's length estimated from the capture
    /// times of the frames on either side of it, at the granted rate, less
    /// every measured gap between the same two blocks. Where several
    /// unmeasured gaps lie between the same two blocks the first carries the
    /// estimate and the others 0. `None` for a measured gap, and for one with
    /// no block after it. A gap before the first block is measured from
    /// `started.mono_us`, taken as frame 0's capture time. It can be negative:
    /// the clock's error is not hidden.
    pub clock_estimate_frames: Option<i64>,
    pub mono_us: u64,
    pub wall_us: u64,
    pub detail: String,
}

/// A change of more than [`WALL_STEP_THRESHOLD_US`] in `wall_us − mono_us`
/// between a record and the one before it (the start pair for the first).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WallStep {
    /// The later record's position.
    pub at_frame: u64,
    /// The later record's `mono_us`.
    pub mono_us: u64,
    /// How far the wall clock moved against the monotonic one; negative is
    /// backwards.
    pub step_us: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EndSummary {
    pub reason: EndReason,
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
}

impl From<&Tail> for TailSummary {
    fn from(t: &Tail) -> Self {
        TailSummary {
            verdict: t.verdict.name(),
            offset: t.offset,
            reason: t.reason(),
        }
    }
}

/// Summarise the capture record at `path`.
pub fn summarize(path: &Path) -> Result<Summary, ReadError> {
    Ok(scan(path)?.summary)
}

/// A summary and the number of items it was taken over, so a second pass
/// reads exactly as far as the first.
pub(crate) struct Scan {
    pub summary: Summary,
    pub header: Header,
    pub items: u64,
}

pub(crate) fn open(path: &Path) -> Result<RecordReader<BufReader<File>>, ReadError> {
    RecordReader::open(path).map_err(ReadError::Open)
}

/// Seconds' worth of `frames` at `rate_hz`, in microseconds.
fn frames_us(frames: u64, rate_hz: u32) -> f64 {
    frames as f64 * 1e6 / f64::from(rate_hz)
}

pub(crate) fn scan(path: &Path) -> Result<Scan, ReadError> {
    let mut reader = open(path)?;
    let header = reader.header().clone();
    let meta = reader.meta().clone();
    let rate = header.granted.rate_hz;
    let period = u64::from(header.granted.period_frames);

    let mut items = 0u64;
    let mut frames_recorded = 0u64;
    let mut blocks = 0u64;
    let mut short_blocks = Vec::new();
    let mut late: Vec<LateRun> = Vec::new();
    let mut in_late_run = false;
    let mut gaps: Vec<GapSummary> = Vec::new();
    let mut wall_steps = Vec::new();
    let mut end = None;
    let mut last_capture_us: Option<f64> = None;

    // The capture time of the last frame before the gaps pending an estimate;
    // the start pair stands for frame 0, so "frame −1" is a frame earlier.
    let mut before_us = header.started.mono_us as f64 - frames_us(1, rate);
    // Unmeasured gaps since the last block, and the measured frames among
    // them.
    let mut pending: Vec<usize> = Vec::new();
    let mut pending_measured = 0u64;
    let mut prev_offset = header.started.wall_us as i64 - header.started.mono_us as i64;

    let mut note_pair = |at_frame: u64, mono_us: u64, wall_us: u64| {
        let offset = wall_us as i64 - mono_us as i64;
        let step_us = offset - prev_offset;
        if step_us.abs() > WALL_STEP_THRESHOLD_US {
            wall_steps.push(WallStep {
                at_frame,
                mono_us,
                step_us,
            });
        }
        prev_offset = offset;
    };

    for item in reader.by_ref() {
        let item = item.map_err(ReadError::Io)?;
        items += 1;
        match item {
            Item::Block {
                mono_us,
                wall_us,
                first_frame,
                backlog_frames,
                frames,
                ..
            } => {
                note_pair(first_frame, mono_us, wall_us);
                blocks += 1;
                frames_recorded += frames;
                if frames < period {
                    short_blocks.push(ShortBlock {
                        first_frame,
                        frames,
                    });
                }
                if u64::from(backlog_frames) >= period {
                    match late.last_mut() {
                        Some(run) if in_late_run => {
                            run.blocks += 1;
                            run.max_backlog_frames = run.max_backlog_frames.max(backlog_frames);
                        }
                        _ => late.push(LateRun {
                            first_frame,
                            blocks: 1,
                            max_backlog_frames: backlog_frames,
                        }),
                    }
                    in_late_run = true;
                } else {
                    in_late_run = false;
                }
                let last_us = mono_us as f64 - frames_us(u64::from(backlog_frames), rate);
                if let Some(&first) = pending.first() {
                    // Capture time of the frame just before this block's first.
                    let after_us = last_us - frames_us(frames, rate);
                    let hole = ((after_us - before_us) * f64::from(rate) / 1e6).round() as i64;
                    let estimate = hole - pending_measured as i64;
                    for &i in &pending {
                        gaps[i].clock_estimate_frames = Some(if i == first { estimate } else { 0 });
                    }
                }
                pending.clear();
                pending_measured = 0;
                before_us = last_us;
                last_capture_us = Some(last_us);
            }
            Item::Gap { mono_us, gap } => {
                note_pair(gap.at_frame, mono_us, gap.wall_us);
                in_late_run = false;
                match gap.frames {
                    Some(n) => pending_measured += n,
                    None => pending.push(gaps.len()),
                }
                gaps.push(GapSummary {
                    cause: gap.cause,
                    at_frame: gap.at_frame,
                    frames: gap.frames,
                    clock_estimate_frames: None,
                    mono_us,
                    wall_us: gap.wall_us,
                    detail: gap.detail,
                });
            }
            Item::End { mono_us, end: e } => {
                note_pair(e.at_frame, mono_us, e.wall_us);
                end = Some(EndSummary {
                    reason: e.reason,
                    at_frame: e.at_frame,
                    mono_us,
                    wall_us: e.wall_us,
                    detail: e.detail,
                });
            }
        }
    }
    let tail = TailSummary::from(reader.tail().expect("iteration ended without an I/O error"));
    let frames_delivered = reader.position();
    let summary = Summary {
        record_id: header.record_id.clone(),
        header: meta,
        frames_delivered,
        frames_recorded,
        duration_frames_s: frames_delivered as f64 / f64::from(rate),
        duration_clock_s: last_capture_us.map(|t| (t - header.started.mono_us as f64) / 1e6),
        blocks,
        short_blocks,
        late,
        gaps,
        wall_steps,
        end,
        tail,
    };
    Ok(Scan {
        summary,
        header,
        items,
    })
}
