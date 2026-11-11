//! Event records: a JSON object whose `event` key names it. Unknown keys are
//! ignored; an unknown `event`, cause or reason is refused, because a reader
//! that skipped what it does not understand could not vouch for the capture's
//! continuity.

use serde::{Deserialize, Serialize};

/// An event record's body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    Gap(Gap),
    End(End),
}

/// Audio is missing at `at_frame`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Gap {
    pub cause: GapCause,
    pub at_frame: u64,
    /// The hole's length when it was measured (`writer_backlog`, which then
    /// advances the position by exactly this much); `None` when it was not
    /// (every other cause, which leaves the position where it was).
    pub frames: Option<u64>,
    pub wall_us: u64,
    pub detail: String,
}

/// Why audio is missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GapCause {
    /// Blocks were dropped because the destination was not taking them fast
    /// enough. The device delivered the frames, so the length is exact.
    WriterBacklog,
    /// The stream overran (ALSA `EPIPE`) and was restarted.
    Overrun,
    /// The stream was suspended (ALSA `ESTRPIPE`) and was restarted.
    Suspend,
    /// Any other transfer error recovery cleared.
    TransferError,
}

impl GapCause {
    /// Whether a gap of this cause carries a measured length.
    pub fn is_measured(self) -> bool {
        matches!(self, GapCause::WriterBacklog)
    }

    pub fn name(self) -> &'static str {
        match self {
            GapCause::WriterBacklog => "writer_backlog",
            GapCause::Overrun => "overrun",
            GapCause::Suspend => "suspend",
            GapCause::TransferError => "transfer_error",
        }
    }
}

/// The last record of a capture that ended itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct End {
    pub reason: EndReason,
    pub at_frame: u64,
    pub wall_us: u64,
    pub detail: String,
}

/// Why the capture ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndReason {
    /// Told to stop.
    Stopped,
    /// Free space on the destination fell under the floor.
    DestinationFull,
    /// The stream failed past recovery, or delivered nothing for too long.
    StreamDied,
    /// A write to the destination failed.
    WriteFailed,
}

impl Event {
    /// Every value of the `event` key: what a reader tells apart as a known
    /// event from an unknown one. Held equal to serde's tags by a test.
    pub(crate) const NAMES: [&str; 2] = ["gap", "end"];

    /// The position the event happened at.
    pub fn at_frame(&self) -> u64 {
        match self {
            Event::Gap(g) => g.at_frame,
            Event::End(e) => e.at_frame,
        }
    }

    /// Check what serde cannot: a measured gap carries a positive length and
    /// an unmeasured one carries none.
    pub(crate) fn check(&self) -> Result<(), String> {
        if let Event::Gap(g) = self {
            match (g.cause.is_measured(), g.frames) {
                (true, None) => {
                    return Err(format!("a {} gap has no frames", g.cause.name()));
                }
                (true, Some(0)) => {
                    return Err(format!("a {} gap of 0 frames", g.cause.name()));
                }
                (false, Some(n)) => {
                    return Err(format!(
                        "an {} gap states {n} frames; it cannot be measured",
                        g.cause.name()
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every cause, in declaration order. The match fails to compile until a
    /// new cause is given an arm, and the arm's index fails the test until the
    /// cause is listed.
    fn every_cause() -> [GapCause; 4] {
        let causes = [
            GapCause::WriterBacklog,
            GapCause::Overrun,
            GapCause::Suspend,
            GapCause::TransferError,
        ];
        for (i, c) in causes.iter().enumerate() {
            let at = match c {
                GapCause::WriterBacklog => 0,
                GapCause::Overrun => 1,
                GapCause::Suspend => 2,
                GapCause::TransferError => 3,
            };
            assert_eq!(at, i);
        }
        causes
    }

    #[test]
    fn hand_written_names_are_the_serde_names() {
        for c in every_cause() {
            assert_eq!(serde_json::to_value(c).unwrap(), c.name());
        }
        let gap = Event::Gap(Gap {
            cause: GapCause::Overrun,
            at_frame: 0,
            frames: None,
            wall_us: 0,
            detail: String::new(),
        });
        let end = Event::End(End {
            reason: EndReason::Stopped,
            at_frame: 0,
            wall_us: 0,
            detail: String::new(),
        });
        for e in [gap, end] {
            let at = match e {
                Event::Gap(_) => 0,
                Event::End(_) => 1,
            };
            assert_eq!(serde_json::to_value(&e).unwrap()["event"], Event::NAMES[at]);
        }
    }
}
