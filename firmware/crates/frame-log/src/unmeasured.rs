//! Which holes in a log's index have no measured length, and how long the
//! device clock says each is. The summary and the export both read a log's
//! unmeasured holes through [`Unmeasured`], so they agree on where audio of
//! unknown length was lost and on each hole's estimate.

use audio_pipeline::wire::EndReason;

use crate::HostMicros;
use crate::walk::{AudioStep, Step};

/// Whether a segment closed for this reason lost audio of unknown length.
fn lossy_end(reason: EndReason) -> bool {
    matches!(
        reason,
        EndReason::Overrun | EndReason::InternalError | EndReason::SourceLoss
    )
}

/// A segment closed with a lossy reason and no `Loss` step just before or just
/// after it: a hole of unknown length at `at_frame`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SegmentEndHole {
    pub stamp: HostMicros,
    pub segment_id: u32,
    pub reason: EndReason,
    /// The position when the segment closed.
    pub at_frame: u64,
    pub device_ts_us: u64,
}

/// Follows a walk and states its unmeasured holes: each `Loss` step with no
/// length, and each [`SegmentEndHole`]. Every step goes through
/// [`Unmeasured::step`] before the reader handles it, and every audio step
/// through [`Unmeasured::resolve`].
pub(crate) struct Unmeasured {
    rate: f64,
    /// The device time and index just after the last audio frame, or the start
    /// pair and index 0 before any.
    before: Option<(f64, u64)>,
    /// A lossy segment end, held for one step to see whether a `Loss` follows.
    held_end: Option<SegmentEndHole>,
    last_was_loss: bool,
}

impl Unmeasured {
    pub fn new(rate: f64) -> Self {
        Unmeasured {
            rate,
            before: None,
            held_end: None,
            last_was_loss: false,
        }
    }

    /// Note `step`, before the reader handles it. Returns the segment end held
    /// from the step before when it is a hole, which it is unless `step` is a
    /// `Loss`; the hole lies before `step`.
    pub fn step(&mut self, step: &Step) -> Option<SegmentEndHole> {
        let is_loss = matches!(step, Step::Loss { .. });
        let released = self.held_end.take().filter(|_| !is_loss);
        match step {
            Step::Describe { description, .. } => {
                self.before = Some((description.mono_us as f64, 0));
            }
            Step::SegmentClosed {
                stamp,
                end,
                at_frame,
                ..
            } if lossy_end(end.reason) && !self.last_was_loss => {
                self.held_end = Some(SegmentEndHole {
                    stamp: *stamp,
                    segment_id: end.segment_id,
                    reason: end.reason,
                    at_frame: *at_frame,
                    device_ts_us: end.device_ts_us,
                });
            }
            _ => {}
        }
        self.last_was_loss = is_loss;
        released
    }

    /// The segment end still held when the walk ends, which is a hole.
    pub fn finish(&mut self) -> Option<SegmentEndHole> {
        self.held_end.take()
    }

    /// The estimates of the unmeasured holes since the last audio, in order,
    /// when the audio after them is `next` (`None`: no audio follows). The
    /// first takes the clock's hole at the nominal rate less the index's
    /// advance across it, which is negative when the clock says less than the
    /// index; the others 0. All are `None` with no audio after them or no
    /// clock before them. Moves the clock past `next`.
    pub fn resolve(
        &mut self,
        next: Option<&AudioStep>,
    ) -> impl Iterator<Item = Option<i64>> + use<> {
        let first = match (next, self.before) {
            (Some(a), Some((before_us, before_index))) => {
                let hole = ((a.device_ts_us as f64 - before_us) * self.rate / 1e6).round() as i64;
                Some(hole - (a.first_frame as i64 - before_index as i64))
            }
            _ => None,
        };
        if let Some(a) = next {
            self.before = Some((
                a.device_ts_us as f64 + a.frames as f64 * 1e6 / self.rate,
                a.first_frame + a.frames,
            ));
        }
        std::iter::once(first).chain(std::iter::repeat(first.map(|_| 0)))
    }

    /// The device time just after the last audio frame, or the start pair's
    /// before any.
    pub fn clock_us(&self) -> Option<f64> {
        self.before.map(|(us, _)| us)
    }
}
