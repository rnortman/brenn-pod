//! Scripted stand-ins for the hardware the self-tests talk to.
//!
//! The registry reaches two interfaces — the XVF3800's control endpoint and the
//! board's capture stream — and both the unattended cases and the bench case are
//! judged off-hardware by writing what those interfaces do. They live here rather
//! than in either module's test module so a card that stalls or a transport that
//! dies behaves the same way in every case that scripts one.

#![cfg(test)]

use std::cell::Cell;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

use xvf3800_ctrl::{ControlTransport, STATUS_DONE};

use crate::alsa_capture::PcmError;
use crate::run::PeriodSource;
use crate::selftest::Outcome;

/// A transport that answers reads from a script.
///
/// Answers are consumed in order and the last one repeats, so a single-answer
/// script serves a whole retry budget while a multi-answer one drives a sequence of
/// reads of registers of different lengths.
pub struct Scripted {
    answers: VecDeque<(u8, Vec<u8>)>,
    error: Option<&'static str>,
    /// The `(resid, cmd)` every read asked for, in order — what says a case read
    /// the register its name promises.
    pub registers: Vec<(u8, u8)>,
    pub reads: u32,
    pub delays: u32,
}

impl Scripted {
    pub fn answering(status: u8, payload: Vec<u8>) -> Self {
        Self::sequenced(vec![(status, payload)])
    }

    pub fn sequenced(answers: Vec<(u8, Vec<u8>)>) -> Self {
        assert!(!answers.is_empty(), "a script needs at least one answer");
        Self {
            answers: answers.into(),
            error: None,
            registers: Vec::new(),
            reads: 0,
            delays: 0,
        }
    }

    pub fn failing(error: &'static str) -> Self {
        Self {
            error: Some(error),
            ..Self::answering(STATUS_DONE, Vec::new())
        }
    }

    pub fn f32x4(values: [f32; 4]) -> Self {
        Self::answering(STATUS_DONE, f32x4_bytes(values))
    }
}

/// Four little-endian f32s, as the chip returns an AEC reading.
pub fn f32x4_bytes(values: [f32; 4]) -> Vec<u8> {
    let mut payload = Vec::new();
    for v in values {
        payload.extend_from_slice(&v.to_le_bytes());
    }
    payload
}

impl ControlTransport for Scripted {
    type Error = &'static str;

    fn control_read_once(
        &mut self,
        resid: u8,
        cmd: u8,
        payload: &mut [u8],
        _attempt: u32,
    ) -> Result<u8, Self::Error> {
        self.reads += 1;
        self.registers.push((resid, cmd));
        if let Some(e) = self.error {
            return Err(e);
        }
        let (status, answer) = if self.answers.len() > 1 {
            self.answers.pop_front().expect("checked non-empty")
        } else {
            self.answers.front().cloned().expect("checked non-empty")
        };
        assert_eq!(payload.len(), answer.len(), "script length");
        payload.copy_from_slice(&answer);
        Ok(status)
    }

    fn control_write_once(
        &mut self,
        _resid: u8,
        _cmd: u8,
        _payload: &[u8],
        _attempt: u32,
    ) -> Result<u8, Self::Error> {
        unreachable!("no case writes")
    }

    fn delay_ms(&mut self, _ms: u32) {
        self.delays += 1;
    }
}

/// A period source the test writes the card's behavior into.
///
/// Periods are handed out in order. `ready` scripts what `wait_ready` answers, so a
/// card that never delivers is a source whose waits all time out; an exhausted
/// script ends the stream, which is the read-error arm.
pub struct ScriptedCard {
    periods: VecDeque<Vec<i16>>,
    current: Vec<i16>,
    /// Answers to `wait_ready`, in order; an exhausted script keeps answering with
    /// its last value.
    ready: VecDeque<bool>,
    /// Every timeout `wait_ready` was given.
    pub waits: Vec<Duration>,
    /// Whether an exhausted queue reports nothing ready rather than reporting ready
    /// and then failing the read. A real card that has delivered all it is going to
    /// for now does the former; the latter is the stopped-stream case.
    quiet_when_drained: bool,
    /// Whether every read counts an xrun the stream recovered from.
    recover_each_read: bool,
    pub recoveries: u64,
}

/// Waits one card will answer before the fixture calls it a runaway.
///
/// No case in the registry polls a card anywhere near this often for one window, so
/// a run that reaches it is a collection whose bound stopped working — and a panic
/// names that where a hung test would say nothing at all.
const MAX_WAITS: usize = 10_000;

impl ScriptedCard {
    pub fn delivering(periods: Vec<Vec<i16>>) -> Self {
        Self {
            periods: periods.into(),
            current: Vec::new(),
            ready: [true].into(),
            waits: Vec::new(),
            quiet_when_drained: false,
            recover_each_read: false,
            recoveries: 0,
        }
    }

    /// A card that opened, took the parameters, and then never delivered.
    pub fn stalled() -> Self {
        Self {
            ready: [false].into(),
            ..Self::delivering(Vec::new())
        }
    }

    /// The same card, but idle once its periods are gone instead of broken.
    pub fn quiet_when_drained(mut self) -> Self {
        self.quiet_when_drained = true;
        self
    }

    /// A card recovering from an xrun on every read.
    pub fn recovering_each_read(mut self) -> Self {
        self.recover_each_read = true;
        self
    }

    /// What its waits answer, in order, with the last answer repeating. A `false` is
    /// a timeout — or an xrun recovered from while waiting, which reaches the caller
    /// the same way.
    pub fn answering_waits(mut self, answers: Vec<bool>) -> Self {
        assert!(!answers.is_empty(), "a script needs at least one answer");
        self.ready = answers.into();
        self
    }

    /// Periods it has not handed out yet — what says a caller took the audio it
    /// needed and stopped.
    pub fn remaining(&self) -> usize {
        self.periods.len()
    }
}

impl PeriodSource for ScriptedCard {
    fn read_period(&mut self) -> Result<&[i16], PcmError> {
        match self.periods.pop_front() {
            Some(period) => {
                if self.recover_each_read {
                    self.recoveries += 1;
                }
                self.current = period;
                Ok(&self.current)
            }
            None => Err(PcmError::Stream {
                reason: "the card stopped delivering".to_string(),
            }),
        }
    }

    fn wait_ready(&mut self, timeout: Duration) -> Result<bool, PcmError> {
        assert!(
            self.waits.len() < MAX_WAITS,
            "a collection asked this card for {MAX_WAITS} waits: its bound is not bounding"
        );
        self.waits.push(timeout);
        if self.quiet_when_drained && self.periods.is_empty() {
            return Ok(false);
        }
        if self.ready.len() > 1 {
            Ok(self.ready.pop_front().expect("checked non-empty"))
        } else {
            Ok(*self.ready.front().expect("a script needs one answer"))
        }
    }

    fn recoveries(&self) -> u64 {
        self.recoveries
    }
}

/// A clock the test walks, so a bound measured in seconds costs no wall time.
pub struct Clock(Cell<Instant>);

impl Clock {
    pub fn new() -> Self {
        Self(Cell::new(Instant::now()))
    }

    pub fn now(&self) -> Instant {
        self.0.get()
    }

    pub fn advance(&self, d: Duration) {
        self.0.set(self.0.get() + d);
    }
}

/// One outcome's detail, whichever kind it is — what an assertion about a reading
/// reads.
pub fn detail(outcome: &Outcome) -> String {
    match outcome {
        Outcome::Pass(d) | Outcome::NotRun(d) => d.clone(),
        Outcome::Fail(lines) => lines.join(" / "),
    }
}
