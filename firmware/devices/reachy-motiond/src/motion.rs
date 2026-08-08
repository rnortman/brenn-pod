//! The motion thread: the standing gates, the armed machine, and the loop that
//! keeps the head where the lease says it should be.
//!
//! This is the owned loop the motion libraries do not have. It blocks, it holds
//! the serial port for the life of the daemon, and it awaits nothing. Its whole
//! cycle is: watch the machine for a short dwell, look at the lease, and move
//! between the two postures when the two disagree. Watching is not idling — a
//! monitored hold is what keeps position reads, the tracking monitor, the
//! read-loss budget and the hardware-health sweep running on a machine nobody is
//! looking at, so a fault on an unattended unit is found within one dwell rather
//! than at the next intent.
//!
//! Three things are deliberate about the shape:
//!
//! - **The gates come before the port.** A green self-test record and a resolved
//!   crank datum are the same two gates the operator tool runs, called as the
//!   same function rather than reimplemented, and they are answered from files
//!   before anything is acquired.
//! - **A refusal is absorbing.** Any refusal out of the motion libraries stops
//!   this thread commanding, at once and for good. Torque is untouched, the
//!   servos hold their last goal, the port stays held, and nothing here retries,
//!   re-arms or parks the head. The recovery is an operator.
//! - **Torque comes off on exactly one path.** The stow, the nine-position
//!   verify and the servo-by-servo release run only for [`Stop::Operator`], on a
//!   run an operator is watching. Every other ending leaves the machine holding.
//!
//! The loop is written against [`Head`] rather than against the armed session
//! directly. The session's behaviour belongs to the motion libraries and is
//! tested there; what belongs here is which posture is commanded when, and that
//! is assertable with no machine, no port and no protocol in the way.
//!
//! What the loop does reaches both of the daemon's streams: the motion
//! libraries' own narration for whoever is watching, and a structured line at
//! each move's start, each move's end, and any fault — so a capture says what
//! the machine did and when, not only what it was asked for. A dwell's share of
//! that narration goes through [`DwellNarration`] first, because five identical
//! reports a second are not something anybody can read.

use std::fmt;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use presence_proto::PresenceState;
use reachy_bench::commands::{Session, neutral_targets, stow_pose_targets};
use reachy_bench::config::{self, Resolved, arm_gates, record_path_beside};
use reachy_bench::pump::{MonotonicClock, PumpError, TickEvent};
use reachy_bus::{BusPort, OpenError, SerialBusPort};
use reachy_motion::JointTargets;
use serde_json::json;
use thiserror::Error;

use crate::cells::{FaultReport, FaultStage, Shared, Stop};
use crate::report::Sink;

/// How often a parked thread looks to see whether it may exit.
///
/// It commands nothing while it waits, so this is only the latency of a signal
/// arriving at a daemon that has already faulted. Slow enough to cost nothing on
/// a machine that may sit parked for hours.
const PARK_POLL: Duration = Duration::from_millis(100);

/// What the motion libraries refused, as they rendered it.
///
/// Text rather than the libraries' own error type, and for the same reason the
/// fault cell carries text: the consumers are a log line and an alert, and the
/// refusals already name the phase, the servo, the register and both values.
/// Rendering at the site that has the error keeps the daemon's own vocabulary
/// free of the machine's.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{0}")]
pub struct Refusal(String);

impl Refusal {
    /// A refusal from whatever produced it.
    pub fn new(detail: impl Into<String>) -> Self {
        Self(detail.into())
    }
}

impl From<PumpError> for Refusal {
    fn from(error: PumpError) -> Self {
        Self(error.to_string())
    }
}

/// Why the daemon could not take the machine at all.
///
/// Everything here happens before a servo is commanded, and each variant is a
/// different thing for an operator to go and do: fix the configuration, produce
/// a self-test record, or find out what else has the bus.
#[derive(Debug, Error)]
pub enum StartupError {
    /// The configuration did not resolve, or the self-test record does not admit
    /// arming. Rendered whole, because the gate's own message is what says which
    /// of the two it was and what produces the missing one.
    #[error("{0}")]
    Gates(String),

    /// The serial device could not be opened, or something else already holds
    /// it.
    #[error(transparent)]
    Port(#[from] OpenError),
}

/// The machine this daemon commands, past its standing gates.
///
/// Holding one is the evidence that the configuration resolved — the crank datum
/// included — and that a self-test record admitting arming is on disk. It is
/// deliberately separate from the port and from the session: the gates are
/// answered from files, and answering them is the last thing that happens before
/// the daemon starts acquiring anything.
#[derive(Debug)]
pub struct Machine {
    resolved: Resolved,
}

impl Machine {
    /// Run the two standing gates over the bench configuration at `path`.
    ///
    /// The same file the operator tool reads on this unit, and the same gate
    /// function, so the daemon cannot admit a machine the bench would refuse.
    /// The self-test record is looked for where the bench looks for it: beside
    /// the configuration it describes a run of.
    pub fn gated(path: &Path) -> Result<Self, StartupError> {
        let cfg = config::load(path).map_err(render)?;
        let resolved = arm_gates(&cfg, &Self::record_path(path)).map_err(render)?;
        Ok(Self { resolved })
    }

    /// The serial device the configuration names.
    #[must_use]
    pub fn device(&self) -> &str {
        &self.resolved.device
    }

    /// Where the record for the configuration at `path` is looked for: beside
    /// the configuration it describes a run of, which is where the operator tool
    /// writes it. [`Machine::gated`] answers its second gate from here.
    #[must_use]
    pub fn record_path(path: &Path) -> PathBuf {
        record_path_beside(path)
    }

    /// Open the servo bus, exclusively.
    ///
    /// A second opener — the operator tool, or a second daemon — is refused by
    /// name rather than made to share: two speakers on a half-duplex chain is
    /// not a thing that degrades gracefully.
    pub fn open(&self) -> Result<SerialBusPort, StartupError> {
        Ok(SerialBusPort::open(
            &self.resolved.device,
            self.resolved.timing.baud,
        )?)
    }

    /// Drive the whole arm sequence over `port` and take the machine holding
    /// where it stands.
    ///
    /// The sequence tolerates a machine that is already torqued — each joint
    /// keeps the goal it is holding — and a servo with a latched hardware error
    /// refuses the arm rather than being resumed past.
    pub fn arm<P: BusPort>(
        &self,
        port: P,
        line: &mut dyn FnMut(&str),
    ) -> Result<SessionHead<'_, P>, Refusal> {
        let mut clock = MonotonicClock::new();
        let session = Session::arm(&self.resolved, port, &mut clock, line)?;
        Ok(SessionHead {
            session,
            clock,
            up: self.resolved.up_duration,
            stow: self.resolved.stow_duration,
        })
    }
}

/// An error from the gate, rendered whole.
///
/// `{:#}` rather than `{}`: the errors are a chain, and the alternate form
/// carries the whole of it onto one line. Generic over the error to avoid a
/// dependency on the gate's own error type purely to re-render it.
fn render<E: fmt::Display>(error: E) -> StartupError {
    StartupError::Gates(format!("{error:#}"))
}

/// The two postures and the moves between them.
///
/// The loop's contact with the machine, and all of it. Written as a trait
/// because the daemon's decisions — which posture, and when — are what this
/// crate owns, while what a move does to nine servos belongs to the motion
/// libraries and is tested against a scripted machine there.
pub trait Head {
    /// Carry the head to `posture` and leave it holding there.
    fn move_to(
        &mut self,
        posture: PresenceState,
        line: &mut dyn FnMut(&str),
    ) -> Result<(), Refusal>;

    /// Watch the machine hold for `dwell`, commanding nothing.
    ///
    /// Typed events rather than rendered lines: what a dwell is worth saying
    /// about is this daemon's policy, and keying on the event kind is what
    /// keeps that policy off the wording another repository chose.
    fn hold(&mut self, dwell: Duration, event: &mut dyn FnMut(TickEvent)) -> Result<(), Refusal>;

    /// Verify the machine is at stow and release torque servo by servo.
    ///
    /// Consumes the head: a released machine is limp, and anything that
    /// commanded it afterwards would pump goal frames at servos that cannot
    /// follow them.
    fn release(self, line: &mut dyn FnMut(&str)) -> Result<(), Refusal>
    where
        Self: Sized;
}

/// The armed machine, as the loop sees it.
///
/// Owns the clock as well as the session, because the clock is the loop's: every
/// move and every dwell is paced by the same monotonic source, on the one thread
/// that is allowed to block.
pub struct SessionHead<'a, P: BusPort> {
    session: Session<'a, P>,
    clock: MonotonicClock,
    up: Duration,
    stow: Duration,
}

/// The pose a posture means, paired with the duration the move to it runs over.
///
/// The one place a presence symbol becomes a pose, kept out of the trait
/// implementation so it is assertable with no port and no servo: swapping the two
/// arms inverts the whole feature, and swapping the two durations runs every move
/// at the wrong speed, neither of which any envelope check refuses.
#[must_use]
pub fn targets_for(
    posture: PresenceState,
    up: Duration,
    stow: Duration,
) -> (JointTargets, Duration) {
    match posture {
        PresenceState::Engaged => (neutral_targets(), up),
        PresenceState::Idle => (stow_pose_targets(), stow),
    }
}

impl<P: BusPort> Head for SessionHead<'_, P> {
    fn move_to(
        &mut self,
        posture: PresenceState,
        line: &mut dyn FnMut(&str),
    ) -> Result<(), Refusal> {
        let (targets, duration) = targets_for(posture, self.up, self.stow);
        self.session
            .move_to(targets, duration, &mut self.clock, line)?;
        Ok(())
    }

    fn hold(&mut self, dwell: Duration, event: &mut dyn FnMut(TickEvent)) -> Result<(), Refusal> {
        // The summary is dropped: a 200 ms window's period counts and jitter say
        // nothing a reader or this loop can act on, and the conditions worth
        // knowing about all arrive as events.
        self.session.hold_events(dwell, &mut self.clock, event)?;
        Ok(())
    }

    fn release(mut self, line: &mut dyn FnMut(&str)) -> Result<(), Refusal> {
        // Never forced: a machine measured anywhere but stow refuses here, and
        // the head stays up with torque on. Dropping it is a bench command an
        // operator gives with the machine in hand.
        self.session.release(false, &mut self.clock, line)?;
        Ok(())
    }
}

/// How the motion thread ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Stowed, verified and released on an operator's signal. The only ending in
    /// which torque comes off, and the only one that is not a failure.
    Released,

    /// Stowed and left holding torque, because the daemon has no source of
    /// intents left. The servos keep their goals with the process gone; an
    /// operator releases them.
    Parked,

    /// The machine stopped taking commands. Torque untouched, position held,
    /// nothing commanded since.
    Faulted(FaultReport),
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Released => f.write_str("released at stow"),
            Self::Parked => f.write_str("stowed, torque held"),
            Self::Faulted(report) => write!(f, "{report}"),
        }
    }
}

impl Outcome {
    /// Whether this ending is the clean one. The daemon's exit status, and the
    /// difference between a run an operator ended and a run that ended itself.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        matches!(self, Self::Released)
    }
}

/// What a dwell is allowed to put on the terminal.
///
/// Each dwell is a fresh run of the motion libraries, and their per-run
/// bookkeeping starts over with it: a register that stopped answering is
/// announced again, a loop running late is announced again. At five dwells a
/// second that is a dozen lines of nothing, with the lines that matter buried in
/// it — on the stream an operator reads while the head moves.
///
/// So a dwell narrates news and nothing else:
///
/// - an event the previous dwell also produced is dropped, and the dwells it
///   spans are counted. When something finally changes, the count is stated and
///   the new event printed. A register failing for an hour is one episode with a
///   span, not eighteen thousand identical lines.
/// - the hold's disposition is dropped: a dwell holds, which is the one thing
///   this loop already knows. A *move* still narrates in full, timing report
///   included — it is rare, it is the interesting one, and its numbers are the
///   ones worth reading.
///
/// A dwell's news is held until the dwell ends, because whether the dwell
/// continued a running episode is not known until every one of its events is in:
/// a dwell that repeats one condition *and* reports a new one has done both, and
/// stating the span before the last event arrived would leave the repeat counted
/// in no span at all.
///
/// Recognition is by event kind, through [`dedup_key`], and the wording is
/// nobody's business here: what reaches the terminal is the event's own
/// rendering, which the motion libraries own and this crate takes at build time.
#[derive(Debug, Default)]
pub struct DwellNarration {
    /// The previous dwell's events, as [`dedup_key`] compares them — the repeat
    /// test. A set rather than a single last event, so two conditions reported
    /// together in either order are still recognised as the same pair.
    ///
    /// A dwell whose events are a strict subset of its predecessor's reads as
    /// unchanged. That is sound only because an event stops appearing when its
    /// condition stops being reported at all, which for the one-shot-per-change
    /// events means nothing changed — an invariant of the motion libraries' own
    /// per-run event semantics, not something enforced here.
    prev: Vec<TickEvent>,
    /// What this dwell has produced so far, in the same form.
    current: Vec<TickEvent>,
    /// This dwell's events that the one before it did not produce, whole and in
    /// order, waiting for the dwell to end.
    news: Vec<TickEvent>,
    /// Whether this dwell has repeated an event of the one before it.
    repeated: bool,
    /// Dwells that carried an event of the running episode, since it was stated.
    repeats: u64,
}

impl DwellNarration {
    /// One event out of a dwell: dropped, or held for the end of the dwell.
    pub fn observe(&mut self, event: &TickEvent) {
        if matches!(event, TickEvent::Command(_)) {
            // The disposition of a dwell is the one thing this loop asked for
            // and so the one thing that is never news.
            return;
        }
        let key = dedup_key(event);
        // A fault is always said, even mid-episode: it also ends the dwell with
        // a refusal into the park path, and the two lines belong together.
        let repeat = !matches!(event, TickEvent::Faulted(_)) && self.prev.contains(&key);
        self.current.push(key);
        if repeat {
            self.repeated = true;
            return;
        }
        self.news.push(*event);
    }

    /// The dwell is over: say what it said that was new, and rotate what it said
    /// into the repeat test.
    pub fn end_dwell(&mut self, out: &mut dyn FnMut(&str)) {
        if self.repeated {
            // The episode ran through this dwell, whether or not the dwell also
            // carried news of its own.
            self.repeats += 1;
        }
        if self.news.is_empty() {
            if !self.repeated {
                // Silence ends an episode as surely as a change does, and
                // nothing is coming that would state its span.
                self.close(out);
            }
        } else {
            self.close(out);
            self.say_news(out);
        }
        std::mem::swap(&mut self.prev, &mut self.current);
        self.current.clear();
        self.news.clear();
        self.repeated = false;
    }

    /// Close the books before something that is not a dwell narrates.
    ///
    /// A move, an ending or a fault is about to print, and it must not print
    /// under a running episode's count. Forgets the repeat test too: the same
    /// event after a move is news again, because the machine did something in
    /// between.
    pub fn flush(&mut self, out: &mut dyn FnMut(&str)) {
        // Span first, then anything a dwell interrupted mid-flight was holding:
        // that news is still this dwell's and belongs after the span of the
        // episode it ended.
        self.close(out);
        self.say_news(out);
        self.prev.clear();
        self.current.clear();
        self.repeated = false;
    }

    /// Print what this dwell had that was new, in the events' own words.
    ///
    /// The two-space indent is the framing every nested line of this daemon's
    /// narration carries, the span line included.
    fn say_news(&mut self, out: &mut dyn FnMut(&str)) {
        for event in std::mem::take(&mut self.news) {
            out(&format!("  {event}"));
        }
    }

    /// State the span of a run of identical dwells, if there was one.
    fn close(&mut self, out: &mut dyn FnMut(&str)) {
        if self.repeats > 0 {
            out(&format!(
                "  ... unchanged across {} further dwell(s)",
                self.repeats
            ));
            self.repeats = 0;
        }
    }
}

/// The form of an event the repeat test compares.
///
/// A condition that persists is reported afresh by every dwell, and a dwell is a
/// fresh run: the counters below are counted from the start of that run, so the
/// same condition arrives carrying a different number every time. Compared whole
/// those events never match, and the condition is announced five times a second
/// forever — which is the one thing this filter exists to stop. The worst of them
/// is the overrun, and a loaded Pi is exactly where it fires.
///
/// So the counts of the occasion come out of the key and nothing else does.
/// Servo ids, error bits and the sets of servos that fell short stay in it: those
/// are what a *change* of condition looks like, and a key that elided them would
/// collapse a change into the episode that preceded it — a swallowed fault,
/// which is the failure this filter is not allowed to have.
///
/// Matched variant by variant with no catch-all, so a variant added upstream
/// fails this build at the next pin bump rather than being keyed by a rule
/// nobody chose for it.
fn dedup_key(event: &TickEvent) -> TickEvent {
    match *event {
        TickEvent::Overrun { .. } => TickEvent::Overrun {
            tick: 0,
            late: Duration::ZERO,
        },
        TickEvent::ReadRestored { .. } => TickEvent::ReadRestored { after: 0 },
        TickEvent::HealthRestored { .. } => TickEvent::HealthRestored { after: 0 },
        event @ (TickEvent::Command(_)
        | TickEvent::ReadLost { .. }
        | TickEvent::HealthLost { .. }
        | TickEvent::Health(_)
        | TickEvent::Completed
        | TickEvent::Faulted(_)) => event,
    }
}

/// Hold the machine at the posture the lease asks for until something stops it.
///
/// Starts by stowing, whatever the lease currently says: the resting posture
/// under this daemon is stowed with torque on, and a daemon that came up in the
/// middle of a conversation should be stowed until the publisher's next refresh
/// says otherwise. Then the cycle — read the stop signal, read the lease, move
/// if the two postures disagree, otherwise dwell.
///
/// A transition is atomic: `move_to` blocks, and the lease is read only between
/// moves. An intent that arrives mid-move applies when the move completes: at
/// worst one move duration of lag, never a queue and never a refusal.
pub fn run<H: Head>(head: H, shared: &Shared, dwell: Duration, sink: &dyn Sink) -> Outcome {
    let outcome = cycle(head, shared, dwell, sink);
    // Last, and after every ending: the bus thread keeps the attachment up until
    // this is set, so a fault taken during the ending still has somewhere to
    // send its alert.
    shared.end_motion();
    outcome
}

/// The loop itself. Separate from [`run`] only so every one of its endings goes
/// through the one place that notes the machine is no longer being touched.
fn cycle<H: Head>(mut head: H, shared: &Shared, dwell: Duration, sink: &dyn Sink) -> Outcome {
    let mut line = |text: &str| sink.line(text);
    // Dwells narrate through this; moves and endings narrate straight through.
    let mut dwells = DwellNarration::default();

    line("stow: the posture this daemon rests in, torque on");
    started(sink, None, PresenceState::Idle, "startup");
    if let Err(refusal) = head.move_to(PresenceState::Idle, &mut line) {
        return park(head, shared, FaultStage::InitialStow, refusal, sink);
    }
    let mut posture = PresenceState::Idle;
    reached(sink, posture);

    loop {
        if let Some(stop) = shared.stopping() {
            dwells.flush(&mut line);
            return finish(head, shared, posture, stop, sink);
        }

        let desired = shared.desired(Instant::now());
        if desired != posture {
            dwells.flush(&mut line);
            line(&format!("presence: {} -> {}", name(posture), name(desired)));
            started(sink, Some(posture), desired, "intent");
            // TODO(presence-retarget): a move already in flight when the intent
            // changes runs to its endpoint before the reverse move starts.
            if let Err(refusal) = head.move_to(desired, &mut line) {
                return park(head, shared, FaultStage::Presence, refusal, sink);
            }
            posture = desired;
            reached(sink, posture);
            continue;
        }

        let held = head.hold(dwell, &mut |event| dwells.observe(&event));
        dwells.end_dwell(&mut line);
        if let Err(refusal) = held {
            dwells.flush(&mut line);
            return park(head, shared, FaultStage::Presence, refusal, sink);
        }
    }
}

/// A move as it starts. The timestamp on this line is what a capture measures
/// wake-to-motion against, so it is emitted before the move is commanded and not
/// after it lands.
fn started(sink: &dyn Sink, from: Option<PresenceState>, to: PresenceState, reason: &str) {
    sink.event(
        "presence_move",
        &json!({
            "from": from.map(PresenceState::as_str),
            "to": to.as_str(),
            "reason": reason,
        }),
    );
}

/// A move that landed. The pair with [`started`] is what makes a move's duration
/// readable off the capture rather than off the narration.
fn reached(sink: &dyn Sink, posture: PresenceState) {
    sink.event("presence_posture", &json!({ "state": posture.as_str() }));
}

/// Take the machine out of service the way `stop` asks for.
///
/// Both reasons stow first — a head left up is a head that falls when something
/// later releases it, and the stow is commanded while this thread still owns the
/// port. Only the operator's reason goes on to release torque.
fn finish<H: Head>(
    mut head: H,
    shared: &Shared,
    posture: PresenceState,
    stop: Stop,
    sink: &dyn Sink,
) -> Outcome {
    let mut line = |text: &str| sink.line(text);
    line(&format!("stopping on {stop}"));

    if posture != PresenceState::Idle {
        started(sink, Some(posture), PresenceState::Idle, "shutdown");
        if let Err(refusal) = head.move_to(PresenceState::Idle, &mut line) {
            // No park: something is already asking this thread to end, and a
            // parked thread would ignore it. The fault is recorded, torque stays
            // on, and the daemon exits saying so.
            return faulted(shared, FaultStage::Shutdown, refusal, sink);
        }
        reached(sink, PresenceState::Idle);
    }

    match stop {
        Stop::Operator => match head.release(&mut line) {
            Ok(()) => {
                line("released: torque is off and the machine is at rest");
                sink.event("motion_released", &json!({ "at": "stow" }));
                Outcome::Released
            }
            // A failed verify is not a reason to drop the head: the machine is
            // somewhere other than stow, or the release itself was refused, and
            // either way it stays where it is with torque on.
            Err(refusal) => faulted(shared, FaultStage::Shutdown, refusal, sink),
        },
        Stop::Detached => {
            line("stowed; torque stays on — no operator is present to catch the head");
            Outcome::Parked
        }
    }
}

/// The ending for an arm sequence that refused: the machine was never taken.
///
/// Whatever torque the sequence applied stays applied and the servos hold —
/// releasing is an operator's act and this ending has no operator in front of it,
/// which is why the reason recorded is [`Stop::Detached`] and never
/// [`Stop::Operator`]. There is no motion loop on this path, so the reason is
/// here only to wake the bus thread, and the ending has to be noted here too:
/// the bus thread waits for one before it closes the attachment the alert
/// travels over, and no loop is coming that would set it.
pub fn arm_failed(shared: &Shared, refusal: Refusal, sink: &dyn Sink) -> Outcome {
    let outcome = faulted(shared, FaultStage::Arm, refusal, sink);
    shared.request_stop(Stop::Detached);
    shared.end_motion();
    outcome
}

/// Record a fault and stop commanding, holding the port and the machine.
///
/// `head` is taken by value and kept alive for the whole wait: dropping it would
/// close the port, and the port being held is what keeps a second speaker off
/// the bus while an operator decides what to do. Nothing is commanded here — not
/// a stow, not a re-arm — and the wait ends only when something asks the daemon
/// to stop, at which point it exits without commanding either.
fn park<H: Head>(
    head: H,
    shared: &Shared,
    stage: FaultStage,
    refusal: Refusal,
    sink: &dyn Sink,
) -> Outcome {
    let outcome = faulted(shared, stage, refusal, sink);
    while shared.stopping().is_none() {
        thread::sleep(PARK_POLL);
    }
    sink.line("stopping a faulted daemon: nothing is commanded and torque stays on");
    drop(head);
    outcome
}

/// Write the fault down, say it once, and answer with it.
///
/// The event is emitted here, on the thread that has the refusal, rather than by
/// the thread that later reads the cell: a fault taken while the daemon is
/// already shutting down would otherwise reach the capture after the reader is
/// gone, or not at all. The bridge alert stays the bus thread's — it is the one
/// with an attachment.
fn faulted(shared: &Shared, stage: FaultStage, refusal: Refusal, sink: &dyn Sink) -> Outcome {
    let report = FaultReport::new(stage, refusal.to_string());
    shared.set_fault(report.clone());
    sink.event(
        "motion_fault",
        &json!({ "stage": report.stage.to_string(), "detail": report.detail }),
    );
    sink.line(&format!(
        "fault: {report}. commanding has stopped; torque is untouched and the servos hold \
         their last goal. an operator decides what happens next."
    ));
    Outcome::Faulted(report)
}

/// A posture as a log line names it.
fn name(posture: PresenceState) -> &'static str {
    match posture {
        PresenceState::Engaged => "up",
        PresenceState::Idle => "stow",
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::rc::Rc;
    use std::sync::Arc;

    use presence_proto::PresenceBody;
    use reachy_bench::pump::ReadFailures;
    use reachy_bus::IdOutcome;
    use reachy_motion::{CommandDisposition, Fault, JointId, ServoHealth};

    use super::*;
    use crate::report::Collect;

    const POD: &str = "reachy00";
    const TTL: Duration = Duration::from_secs(15);
    const DWELL: Duration = Duration::from_millis(200);

    /// What the loop did to the machine, in order.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Act {
        Move(PresenceState),
        Hold,
        Release,
    }

    /// What the world does to the daemon while it is dwelling.
    ///
    /// One per dwell, applied at the top of the hold, which is where every
    /// change the daemon can observe arrives in life too: the bus thread writes
    /// these cells while this thread is watching the machine.
    #[derive(Debug, Clone, Copy)]
    enum Event {
        /// An engaged intent arrives, with its full term ahead of it.
        Engage,
        /// An engaged intent arrives whose term has already run out — what the
        /// loop sees when the publisher stops refreshing.
        Lapse,
        /// An explicit idle arrives.
        Idle,
        /// Something asks the daemon to stop.
        Stop(Stop),
        /// Nothing happens, and the machine refuses this dwell.
        Refuse,
    }

    /// A head that records what it was asked to do and refuses on demand.
    struct Fake {
        shared: Arc<Shared>,
        acts: Rc<RefCell<Vec<Act>>>,
        events: VecDeque<Event>,
        /// The move, counted from zero, that refuses.
        refuse_move: Option<usize>,
        /// Whether the release refuses.
        refuse_release: bool,
        moves: usize,
        /// What the machine reports on each dwell, one entry per dwell. A dwell
        /// past the end of the script reports nothing.
        says: VecDeque<Vec<TickEvent>>,
        /// What it narrates on each move, one entry per move counted from the
        /// opening stow. Text, because a move still narrates through the motion
        /// libraries' own rendering.
        says_moving: VecDeque<Vec<&'static str>>,
    }

    impl Fake {
        fn new(shared: &Arc<Shared>, events: impl IntoIterator<Item = Event>) -> Self {
            Self {
                shared: Arc::clone(shared),
                acts: Rc::new(RefCell::new(Vec::new())),
                events: events.into_iter().collect(),
                refuse_move: None,
                refuse_release: false,
                moves: 0,
                says: VecDeque::new(),
                says_moving: VecDeque::new(),
            }
        }

        /// What the motion libraries report, dwell by dwell.
        fn saying(mut self, script: impl IntoIterator<Item = Vec<TickEvent>>) -> Self {
            self.says = script.into_iter().collect();
            self
        }

        /// The same for what they narrate while moving, move by move.
        fn saying_moving(mut self, script: impl IntoIterator<Item = Vec<&'static str>>) -> Self {
            self.says_moving = script.into_iter().collect();
            self
        }

        /// A refusal out of the nth move, which also stops the daemon: a parked
        /// thread waits for that, and a test that never sent one would wait with
        /// it.
        fn refusing_move(mut self, nth: usize) -> Self {
            self.refuse_move = Some(nth);
            self
        }

        fn refusing_release(mut self) -> Self {
            self.refuse_release = true;
            self
        }

        fn acts(&self) -> Rc<RefCell<Vec<Act>>> {
            Rc::clone(&self.acts)
        }

        fn record(&self, act: Act) {
            self.acts.borrow_mut().push(act);
        }

        /// The world's next move, applied to the shared cells.
        fn advance(&mut self) -> Option<Event> {
            let event = self.events.pop_front();
            let now = Instant::now();
            match event {
                Some(Event::Engage) => {
                    self.shared
                        .apply(&PresenceBody::new(POD, PresenceState::Engaged, 1), now, TTL);
                }
                Some(Event::Lapse) => {
                    let arrived = now
                        .checked_sub(Duration::from_secs(60))
                        .expect("a monotonic clock a minute past its start");
                    self.shared.apply(
                        &PresenceBody::new(POD, PresenceState::Engaged, 2),
                        arrived,
                        TTL,
                    );
                }
                Some(Event::Idle) => {
                    self.shared
                        .apply(&PresenceBody::new(POD, PresenceState::Idle, 3), now, TTL);
                }
                Some(Event::Stop(stop)) => {
                    self.shared.request_stop(stop);
                }
                Some(Event::Refuse) | None => {}
            }
            event
        }
    }

    impl Head for Fake {
        fn move_to(
            &mut self,
            posture: PresenceState,
            line: &mut dyn FnMut(&str),
        ) -> Result<(), Refusal> {
            let nth = self.moves;
            self.moves += 1;
            let says = self.says_moving.pop_front().unwrap_or_default();
            if self.refuse_move == Some(nth) {
                self.shared.request_stop(Stop::Operator);
                return Err(Refusal::new("the tick faulted: envelope on path"));
            }
            self.record(Act::Move(posture));
            for text in says {
                line(text);
            }
            Ok(())
        }

        fn hold(
            &mut self,
            _dwell: Duration,
            event: &mut dyn FnMut(TickEvent),
        ) -> Result<(), Refusal> {
            self.record(Act::Hold);
            for reported in self.says.pop_front().unwrap_or_default() {
                event(reported);
            }
            match self.advance() {
                Some(Event::Refuse) => {
                    self.shared.request_stop(Stop::Operator);
                    Err(Refusal::new("servo 13: timed out"))
                }
                // A dwell with nothing scripted behind it means the test wrote a
                // sequence the loop ran past; stopping is kinder than spinning.
                None => {
                    self.shared.request_stop(Stop::Operator);
                    Ok(())
                }
                _ => Ok(()),
            }
        }

        fn release(self, _line: &mut dyn FnMut(&str)) -> Result<(), Refusal> {
            self.record(Act::Release);
            if self.refuse_release {
                return Err(Refusal::new("the machine is not at stow"));
            }
            Ok(())
        }
    }

    /// Run the loop against `head`, and hand back what it did and how it ended.
    fn drive(shared: &Shared, head: Fake) -> (Outcome, Vec<Act>) {
        let (outcome, acts, _) = driven(shared, head);
        (outcome, acts)
    }

    /// The same, keeping what the run said as well as what it did.
    fn driven(shared: &Shared, head: Fake) -> (Outcome, Vec<Act>, Collect) {
        let acts = head.acts();
        let sink = Collect::default();
        let outcome = run(head, shared, DWELL, &sink);
        let done = acts.borrow().clone();
        (outcome, done, sink)
    }

    /// The resting posture is stowed with torque on, and it is commanded before
    /// anything is heard from the bus — a daemon that came up mid-conversation
    /// stows first and waits to be told otherwise.
    #[test]
    fn the_daemon_stows_before_it_listens() {
        let shared = Arc::new(Shared::new(POD));
        let head = Fake::new(&shared, [Event::Stop(Stop::Operator)]);

        let (outcome, acts) = drive(&shared, head);

        assert_eq!(outcome, Outcome::Released);
        assert_eq!(
            acts,
            [Act::Move(PresenceState::Idle), Act::Hold, Act::Release]
        );
    }

    /// An engaged lease lifts the head, and the head stays up while the lease
    /// does: a dwell that changes nothing commands nothing.
    #[test]
    fn an_engaged_lease_lifts_the_head_and_holds_it_there() {
        let shared = Arc::new(Shared::new(POD));
        let head = Fake::new(
            &shared,
            [Event::Engage, Event::Engage, Event::Stop(Stop::Operator)],
        );

        let (outcome, acts) = drive(&shared, head);

        assert_eq!(outcome, Outcome::Released);
        assert_eq!(
            acts,
            [
                Act::Move(PresenceState::Idle),
                Act::Hold,
                Act::Move(PresenceState::Engaged),
                Act::Hold,
                Act::Hold,
                Act::Move(PresenceState::Idle),
                Act::Release,
            ]
        );
    }

    /// The publisher stops refreshing and the term runs out. Nothing said stow;
    /// the absence of instruction is what stows.
    #[test]
    fn a_lapsed_lease_stows_the_head() {
        let shared = Arc::new(Shared::new(POD));
        let head = Fake::new(
            &shared,
            [Event::Engage, Event::Lapse, Event::Stop(Stop::Operator)],
        );

        let (outcome, acts) = drive(&shared, head);

        assert_eq!(outcome, Outcome::Released);
        assert_eq!(
            acts,
            [
                Act::Move(PresenceState::Idle),
                Act::Hold,
                Act::Move(PresenceState::Engaged),
                Act::Hold,
                Act::Move(PresenceState::Idle),
                Act::Hold,
                // Already at stow when the signal arrives: nothing is commanded
                // on the way out but the release itself.
                Act::Release,
            ]
        );
    }

    /// An explicit idle does not wait for a term to run out.
    #[test]
    fn an_explicit_idle_stows_at_the_next_dwell() {
        let shared = Arc::new(Shared::new(POD));
        let head = Fake::new(
            &shared,
            [Event::Engage, Event::Idle, Event::Stop(Stop::Operator)],
        );

        let (outcome, acts) = drive(&shared, head);

        assert_eq!(outcome, Outcome::Released);
        assert_eq!(
            acts,
            [
                Act::Move(PresenceState::Idle),
                Act::Hold,
                Act::Move(PresenceState::Engaged),
                Act::Hold,
                Act::Move(PresenceState::Idle),
                Act::Hold,
                Act::Release,
            ]
        );
    }

    /// A head that is up when the operator signals is stowed first, and only
    /// then released. The release is the last thing that happens to the machine.
    #[test]
    fn an_operators_signal_stows_before_it_releases() {
        let shared = Arc::new(Shared::new(POD));
        let head = Fake::new(&shared, [Event::Engage, Event::Stop(Stop::Operator)]);

        let (outcome, acts) = drive(&shared, head);

        assert_eq!(outcome, Outcome::Released);
        assert_eq!(
            acts,
            [
                Act::Move(PresenceState::Idle),
                Act::Hold,
                Act::Move(PresenceState::Engaged),
                Act::Hold,
                Act::Move(PresenceState::Idle),
                Act::Release,
            ]
        );
    }

    /// The bridge gives up. The head is parked, but nobody is standing there to
    /// catch it, so torque stays on and the daemon says so by not ending clean.
    #[test]
    fn a_lost_intent_source_stows_and_keeps_torque() {
        let shared = Arc::new(Shared::new(POD));
        let head = Fake::new(&shared, [Event::Engage, Event::Stop(Stop::Detached)]);

        let (outcome, acts) = drive(&shared, head);

        assert_eq!(outcome, Outcome::Parked);
        assert!(!outcome.is_clean());
        assert!(
            !acts.contains(&Act::Release),
            "torque came off with no operator: {acts:?}"
        );
        assert_eq!(acts.last(), Some(&Act::Move(PresenceState::Idle)));
    }

    /// The stow that every run starts with is a place a machine can refuse, and
    /// the fault says so — an operator reading the alert should not have to
    /// guess whether the daemon ever got going.
    #[test]
    fn a_refused_initial_stow_faults_at_that_stage() {
        let shared = Arc::new(Shared::new(POD));
        let head = Fake::new(&shared, []).refusing_move(0);

        let (outcome, acts) = drive(&shared, head);

        let Outcome::Faulted(report) = outcome else {
            panic!("the initial stow refused");
        };
        assert_eq!(report.stage, FaultStage::InitialStow);
        assert_eq!(shared.fault(), Some(&report));
        assert!(acts.is_empty(), "{acts:?}");
    }

    /// A refusal inside the loop is absorbing. Nothing is commanded after it —
    /// not the stow the pending signal would otherwise ask for, and above all
    /// not the release.
    #[test]
    fn a_refusal_in_the_loop_stops_commanding_for_good() {
        let shared = Arc::new(Shared::new(POD));
        let head = Fake::new(&shared, [Event::Engage]).refusing_move(1);

        let (outcome, acts) = drive(&shared, head);

        let Outcome::Faulted(report) = outcome else {
            panic!("the move up refused");
        };
        assert_eq!(report.stage, FaultStage::Presence);
        assert!(report.detail.contains("envelope on path"), "{report}");
        assert_eq!(
            acts,
            [Act::Move(PresenceState::Idle), Act::Hold],
            "something was commanded after the fault",
        );
    }

    /// The same for a dwell that refuses: the monitored hold is where an
    /// unattended machine's faults are found, and finding one stops the daemon
    /// commanding rather than starting a recovery.
    #[test]
    fn a_refused_dwell_faults_and_holds_torque() {
        let shared = Arc::new(Shared::new(POD));
        let head = Fake::new(&shared, [Event::Refuse]);

        let (outcome, acts) = drive(&shared, head);

        let Outcome::Faulted(report) = outcome else {
            panic!("the dwell refused");
        };
        assert_eq!(report.stage, FaultStage::Presence);
        assert!(report.detail.contains("servo 13"), "{report}");
        assert_eq!(acts, [Act::Move(PresenceState::Idle), Act::Hold]);
    }

    /// A release that cannot verify the machine at stow leaves it exactly where
    /// it is, torque on, and ends unclean. The head is never dropped to satisfy
    /// a shutdown.
    #[test]
    fn a_release_that_cannot_verify_leaves_torque_on() {
        let shared = Arc::new(Shared::new(POD));
        let head = Fake::new(&shared, [Event::Stop(Stop::Operator)]).refusing_release();

        let (outcome, acts) = drive(&shared, head);

        assert!(!outcome.is_clean());
        let Outcome::Faulted(report) = outcome else {
            panic!("the release refused");
        };
        assert_eq!(report.stage, FaultStage::Shutdown);
        assert_eq!(
            acts,
            [Act::Move(PresenceState::Idle), Act::Hold, Act::Release]
        );
    }

    /// Intents that arrive after a fault change nothing: the lease stops being
    /// folded, so there is no posture waiting to be commanded if anything ever
    /// resumed.
    #[test]
    fn intents_after_a_fault_do_not_move_the_lease() {
        let shared = Arc::new(Shared::new(POD));
        let head = Fake::new(&shared, [Event::Refuse]);

        let (outcome, _) = drive(&shared, head);

        assert!(matches!(outcome, Outcome::Faulted(_)));
        let now = Instant::now();
        shared.apply(&PresenceBody::new(POD, PresenceState::Engaged, 9), now, TTL);
        assert_eq!(shared.desired(now), PresenceState::Idle);
    }

    /// Every ending says so, whatever it was. The bus thread holds the
    /// attachment open until this is set, so a run that ended without setting it
    /// would be a daemon that never shuts its bus side down.
    #[test]
    fn every_ending_notes_that_the_machine_is_no_longer_being_touched() {
        for head in [
            Fake::new(&Arc::new(Shared::new(POD)), [Event::Stop(Stop::Operator)]),
            Fake::new(&Arc::new(Shared::new(POD)), [Event::Stop(Stop::Detached)]),
            Fake::new(&Arc::new(Shared::new(POD)), [Event::Refuse]),
        ] {
            let shared = Arc::clone(&head.shared);
            let (outcome, _) = drive(&shared, head);
            assert!(shared.motion_ended(), "{outcome} did not say it had ended");
        }
    }

    /// What the machine did reaches the capture, not only what it was asked for:
    /// a move's start carries where it is going and why, and its landing is a
    /// line of its own so the pair bounds the move.
    #[test]
    fn each_move_is_bounded_by_two_lines_in_the_capture() {
        let shared = Arc::new(Shared::new(POD));
        let head = Fake::new(&shared, [Event::Engage, Event::Stop(Stop::Operator)]);

        let (_, _, sink) = driven(&shared, head);

        let moves: Vec<_> = sink
            .said()
            .events
            .into_iter()
            .filter(|(name, _)| name == "presence_move" || name == "presence_posture")
            .collect();
        let names: Vec<&str> = moves.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(
            names,
            [
                "presence_move",
                "presence_posture",
                "presence_move",
                "presence_posture",
                "presence_move",
                "presence_posture",
            ],
            "{moves:?}"
        );
        assert_eq!(moves[0].1["from"], serde_json::Value::Null);
        assert_eq!(moves[0].1["to"], json!("idle"));
        assert_eq!(moves[0].1["reason"], json!("startup"));
        assert_eq!(moves[2].1["from"], json!("idle"));
        assert_eq!(moves[2].1["to"], json!("engaged"));
        assert_eq!(moves[2].1["reason"], json!("intent"));
        assert_eq!(moves[4].1["reason"], json!("shutdown"));
        assert!(sink.saw("motion_released"));
    }

    /// The fault is written to the capture by the thread that took it. Left to
    /// the bus thread's poll it would be late at best, and on the shutdown path
    /// it would be written after the reader had gone.
    #[test]
    fn a_fault_reaches_the_capture_from_the_thread_that_took_it() {
        let shared = Arc::new(Shared::new(POD));
        let head = Fake::new(&shared, [Event::Refuse]);

        let (_, _, sink) = driven(&shared, head);

        let fields = sink.fields("motion_fault").expect("the fault is reported");
        assert_eq!(fields["stage"], json!("the presence loop"));
        assert!(
            fields["detail"].as_str().is_some_and(|d| d.contains("13")),
            "{fields}"
        );
    }

    /// Nothing is acquired before the gates answer: a configuration that is not
    /// there refuses without a port, a record or a servo being touched.
    #[test]
    fn the_gates_refuse_a_configuration_that_is_not_there() {
        let refused = Machine::gated(Path::new("/nonexistent/reachy-bench.toml"))
            .expect_err("there is no configuration there");

        let StartupError::Gates(detail) = refused else {
            panic!("a missing configuration is a gate refusal");
        };
        assert!(
            detail.contains("/nonexistent/reachy-bench.toml"),
            "{detail}"
        );
    }

    /// The record is looked for where the operator tool writes it — beside the
    /// configuration it describes a run of, under the bench's own name for it —
    /// so the daemon and the bench cannot be reading different records of the
    /// same machine. The name is asserted against the bench's constant rather
    /// than a copy of it, which is what makes this a guard on the shared record
    /// rather than a restatement of the daemon's own spelling.
    #[test]
    fn the_record_is_the_one_the_bench_writes_beside_the_configuration() {
        let path = Machine::record_path(Path::new("/etc/reachy/reachy-bench.toml"));
        assert_eq!(
            path,
            Path::new("/etc/reachy").join(reachy_bench::config::RECORD_NAME)
        );
    }

    /// The only place a presence symbol becomes a pose. Swapping the arms
    /// inverts the feature — stow on wake, head up when the interaction ends —
    /// and swapping the durations runs every move at the wrong pace; the
    /// envelope check refuses neither, because both are legal.
    #[test]
    fn each_posture_names_its_own_pose_and_its_own_pace() {
        let up = Duration::from_millis(1_200);
        let stow = Duration::from_millis(2_500);
        assert_ne!(
            up, stow,
            "a fixture that cannot tell the two pairings apart"
        );
        assert_ne!(
            neutral_targets(),
            stow_pose_targets(),
            "a machine whose two postures are the same pose"
        );

        assert_eq!(
            targets_for(PresenceState::Engaged, up, stow),
            (neutral_targets(), up)
        );
        assert_eq!(
            targets_for(PresenceState::Idle, up, stow),
            (stow_pose_targets(), stow)
        );
    }

    /// An arm sequence that refused never took the machine. Four things follow,
    /// and every one of them is safety posture: the fault is recorded so the bus
    /// thread alerts, the stop reason is the one that does *not* release torque,
    /// the ending is noted so the bus thread stops waiting for a motion loop that
    /// is not coming, and the run ends faulted.
    #[test]
    fn an_arm_that_refused_faults_without_asking_for_a_release() {
        let shared = Shared::new(POD);
        let sink = Collect::default();

        let outcome = arm_failed(&shared, Refusal::new("servo 21 answered nothing"), &sink);

        let Outcome::Faulted(report) = &outcome else {
            panic!("an arm that refused is a fault");
        };
        assert_eq!(report.stage, FaultStage::Arm);
        assert_eq!(shared.fault(), Some(report));
        assert_eq!(
            shared.stopping(),
            Some(Stop::Detached),
            "nobody is present, so this ending must not take torque off"
        );
        assert!(
            shared.motion_ended(),
            "the bus thread waits for this before it closes the attachment"
        );
        let fields = sink.fields("motion_fault").expect("the fault is reported");
        assert_eq!(fields["stage"], json!("arming"));
        assert_eq!(fields["detail"], json!("servo 21 answered nothing"));
    }

    /// The two lines the motion libraries close a *move* with. Only a move
    /// narrates as text, so this is a fixture for `says_moving` alone.
    const REPORT: [&str; 2] = [
        "  40 period(s), 1 commanding, 9 frame(s), 0 blind, 0 overrun(s), \
         worst jitter 1.2 ms, 0.20 s",
        "  worst lag [0.010, 0.011, 0.009, 0.012, 0.008, 0.010, 0.011, 0.009, 0.010] deg",
    ];

    /// The health sweep failing. The set of servos is empty because what this
    /// fixture is for is the kind of the event and its sameness across dwells.
    fn lost() -> TickEvent {
        TickEvent::HealthLost {
            failed: ReadFailures::default(),
        }
    }

    /// The health sweep failing with `id` the servo that kept it from
    /// completing. Which servos fell short is part of the condition, so two of
    /// these naming different servos are two conditions.
    fn lost_on(id: u8) -> TickEvent {
        TickEvent::HealthLost {
            failed: ReadFailures::from_verdicts(&[(id, IdOutcome::Timeout)], 0),
        }
    }

    /// The position read failing — a different condition from [`lost`], so a
    /// dwell reporting both reports two things.
    fn read_lost() -> TickEvent {
        TickEvent::ReadLost {
            failed: ReadFailures::default(),
        }
    }

    /// A health sweep of nine servos, `bits` latched on the second of them.
    fn sweep(bits: u8) -> TickEvent {
        let mut servos = [ServoHealth::default(); JointId::COUNT];
        for (index, servo) in servos.iter_mut().enumerate() {
            servo.id = 11 + u8::try_from(index).expect("nine servos fit in a byte");
        }
        if let Some(servo) = servos.get_mut(1) {
            servo.bits = bits;
        }
        TickEvent::Health(servos)
    }

    /// What the daemon puts on the terminal for `event`: the event's own words,
    /// in the frame every nested narration line carries. Asserted against
    /// rather than spelled out, so no upstream wording is pinned in this repo.
    fn rendered(event: TickEvent) -> String {
        format!("  {event}")
    }

    /// Feed the filter a script, one entry per dwell, and hand back what got
    /// past it. The closing flush is the one a move or an ending would do.
    fn through(script: impl IntoIterator<Item = Vec<TickEvent>>) -> Vec<String> {
        let mut said = Vec::new();
        let mut narration = DwellNarration::default();
        for dwell in script {
            for event in dwell {
                narration.observe(&event);
            }
            narration.end_dwell(&mut |line| said.push(line.to_owned()));
        }
        narration.flush(&mut |line| said.push(line.to_owned()));
        said
    }

    /// The disposition of a dwell is that it is holding — the one thing the
    /// loop asked for, and the only event every dwell is guaranteed to carry.
    /// Five of those a second is the bulk of what an operator would see and
    /// none of it is news.
    #[test]
    fn a_dwell_never_narrates_the_disposition_it_asked_for() {
        let held = TickEvent::Command(CommandDisposition::Held);
        let said = through([vec![held], vec![held], vec![held]]);

        assert!(said.is_empty(), "a quiet dwell said: {said:?}");
    }

    /// News is printed in the event's own words. The wording belongs to the
    /// motion libraries and reaches this daemon at build time, so the assertion
    /// renders the event rather than spelling its text out here.
    #[test]
    fn news_is_printed_in_the_events_own_words() {
        let event = TickEvent::ReadRestored { after: 7 };
        let said = through([vec![event]]);

        assert_eq!(said, [rendered(event)]);
        assert!(
            !event.to_string().is_empty(),
            "an event that says nothing would make the line above the frame alone"
        );
    }

    /// A condition that clears twice inside one dwell cleared twice, and both
    /// are said. The two key alike — the count of the occasion comes out of the
    /// key — but the repeat test is against the dwell before, not against the
    /// dwell in hand: a bus dropping and coming back twice in 200 ms is a
    /// flapping bus, and a terminal that said it once would be describing a
    /// quieter machine than the one in front of the operator.
    #[test]
    fn a_condition_clearing_twice_within_one_dwell_is_said_twice() {
        let first = TickEvent::ReadRestored { after: 1 };
        let again = TickEvent::ReadRestored { after: 3 };
        let said = through([vec![first, again]]);

        assert_eq!(said, [rendered(first), rendered(again)]);
    }

    /// The servos that fell short are the condition, not the occasion: a sweep
    /// that starts failing on a different servo is a different failure, and
    /// absorbing it into the running episode would be the swallowed fault this
    /// filter is not allowed to have.
    #[test]
    fn a_changed_set_of_failed_servos_is_not_absorbed_into_the_episode() {
        let said = through([vec![lost_on(11)], vec![lost_on(11)], vec![lost_on(13)]]);

        assert_eq!(
            said,
            [
                rendered(lost_on(11)),
                "  ... unchanged across 1 further dwell(s)".to_owned(),
                rendered(lost_on(13)),
            ]
        );
    }

    /// A fault is never absorbed into a running episode, whatever else the
    /// dwell repeated. It is defence in depth — a faulted hold also refuses
    /// into the park path — and the one event this filter must not be clever
    /// about.
    #[test]
    fn a_fault_is_printed_even_in_the_middle_of_an_episode() {
        let faulted = TickEvent::Faulted(Fault::ReadLoss { misses: 12 });
        let said = through([
            vec![lost()],
            vec![lost()],
            vec![lost(), faulted],
            vec![lost(), faulted],
        ]);

        assert_eq!(
            said,
            [
                rendered(lost()),
                "  ... unchanged across 2 further dwell(s)".to_owned(),
                rendered(faulted),
                "  ... unchanged across 1 further dwell(s)".to_owned(),
                // Said again: the dwell before it said the very same thing, and
                // a fault is the one event never absorbed into an episode.
                rendered(faulted),
            ]
        );
    }

    /// The heart of it: a register that stops answering is announced on the
    /// dwell that first saw it and not again, and the span it lasted is stated
    /// when it clears. One episode, not one line per dwell.
    #[test]
    fn a_repeated_dwell_event_is_one_episode_with_its_span() {
        let restored = TickEvent::HealthRestored { after: 4 };
        let said = through([
            vec![lost()],
            vec![lost()],
            vec![lost()],
            vec![lost()],
            vec![restored, sweep(0)],
            vec![sweep(0)],
        ]);

        assert_eq!(
            said,
            [
                rendered(lost()),
                "  ... unchanged across 3 further dwell(s)".to_owned(),
                rendered(restored),
                rendered(sweep(0)),
                "  ... unchanged across 1 further dwell(s)".to_owned(),
            ]
        );
    }

    /// A loop running late says so once per dwell, and says it with a different
    /// period number and a different lateness every time. Compared whole those
    /// never repeat, so sustained scheduling pressure — the state this Pi is in
    /// when it is sharing itself with the audio pipeline, and the state an
    /// operator most needs a readable terminal for — would bury everything else
    /// under one line per dwell. It is one episode.
    #[test]
    fn an_overrun_reported_afresh_every_dwell_is_one_episode() {
        let late = |tick, micros| TickEvent::Overrun {
            tick,
            late: Duration::from_micros(micros),
        };
        let said = through([
            vec![late(3, 1_400)],
            vec![late(7, 2_900)],
            vec![late(1, 11_000)],
        ]);

        assert_eq!(
            said,
            [
                rendered(late(3, 1_400)),
                "  ... unchanged across 2 further dwell(s)".to_owned(),
            ]
        );
    }

    /// The counters elided from the key are counts of the occasion. What an
    /// event says about the *machine* — which servo, which bits — stays in it,
    /// so a changed reading is news even though its kind did not change.
    #[test]
    fn a_changed_reading_is_not_folded_into_the_episode_before_it() {
        let latched = 0x20;
        let said = through([vec![sweep(0)], vec![sweep(0)], vec![sweep(latched)]]);

        assert_eq!(
            said,
            [
                rendered(sweep(0)),
                "  ... unchanged across 1 further dwell(s)".to_owned(),
                rendered(sweep(latched)),
            ]
        );
    }

    /// The three events whose payload counts the occasion rather than the
    /// condition, keyed alike across occasions and apart from each other. The
    /// pairs are built from the library's own type, so a variant that grew a
    /// counter fails to compile here rather than announcing itself five times a
    /// second.
    #[test]
    fn the_counters_of_the_occasion_are_the_only_thing_the_key_drops() {
        let pairs = [
            (
                TickEvent::Overrun {
                    tick: 3,
                    late: Duration::from_micros(1_400),
                },
                TickEvent::Overrun {
                    tick: 91,
                    late: Duration::from_micros(12_600),
                },
            ),
            (
                TickEvent::ReadRestored { after: 1 },
                TickEvent::ReadRestored { after: 40 },
            ),
            (
                TickEvent::HealthRestored { after: 1 },
                TickEvent::HealthRestored { after: 12 },
            ),
        ];

        for (one, other) in pairs {
            assert_ne!(one, other, "the fixture pair is the same event twice");
            assert_eq!(
                dedup_key(&one),
                dedup_key(&other),
                "two occasions of one condition key differently: {one:?} / {other:?}"
            );
        }
        for (index, (one, _)) in pairs.iter().enumerate() {
            for (other, _) in pairs.iter().skip(index + 1) {
                assert_ne!(
                    dedup_key(one),
                    dedup_key(other),
                    "two conditions share a key: {one:?} / {other:?}"
                );
            }
        }
    }

    /// Two conditions reported together are one repeat whichever order the
    /// sweep happens to report them in — the test is what the dwell said, not
    /// the sequence it said it in.
    #[test]
    fn the_repeat_test_does_not_depend_on_the_order_within_a_dwell() {
        let said = through([
            vec![lost(), read_lost()],
            vec![read_lost(), lost()],
            vec![lost(), read_lost()],
        ]);

        assert_eq!(
            said,
            [
                rendered(lost()),
                rendered(read_lost()),
                "  ... unchanged across 2 further dwell(s)".to_owned(),
            ]
        );
    }

    /// A dwell that carries news *and* repeats a running condition did both:
    /// the span it is part of has to count it, or a reader correlating an
    /// episode's stated length against the timeline is short by one dwell for
    /// every dwell that also had something to say.
    #[test]
    fn a_dwell_that_repeats_and_reports_at_once_is_counted_in_the_span() {
        let said = through([
            vec![lost()],
            vec![lost(), read_lost()],
            vec![lost(), read_lost()],
        ]);

        assert_eq!(
            said,
            [
                rendered(lost()),
                // The dwell that produced the read loss still carried the health
                // loss, so it is inside the episode the health loss opened.
                "  ... unchanged across 1 further dwell(s)".to_owned(),
                rendered(read_lost()),
                "  ... unchanged across 1 further dwell(s)".to_owned(),
            ]
        );
    }

    /// A dwell that reports less than the one before it reads as unchanged, and
    /// that is deliberate: an event stops appearing when its condition stops
    /// being reported at all, which for the one-shot-per-change events means
    /// nothing changed. Comparing the two dwells as sets instead would open a
    /// fresh episode every time a sporadic event failed to recur.
    #[test]
    fn a_dwell_that_reports_less_than_the_one_before_is_still_unchanged() {
        let said = through([
            vec![lost(), read_lost()],
            vec![lost(), read_lost()],
            vec![lost()],
            vec![lost()],
        ]);

        assert_eq!(
            said,
            [
                rendered(lost()),
                rendered(read_lost()),
                "  ... unchanged across 3 further dwell(s)".to_owned(),
            ]
        );
    }

    /// A dwell that goes quiet ends the episode there and then: nothing else is
    /// coming that would state its span. A dwell carrying only the disposition
    /// it was asked for is exactly that quiet dwell.
    #[test]
    fn silence_closes_a_running_episode() {
        let held = TickEvent::Command(CommandDisposition::Held);
        let said = through([vec![lost()], vec![lost()], vec![held], vec![lost()]]);

        assert_eq!(
            said,
            [
                rendered(lost()),
                "  ... unchanged across 1 further dwell(s)".to_owned(),
                // Said again, because the dwell in between said nothing at all:
                // this is a fresh episode, not the same one continuing.
                rendered(lost()),
            ]
        );
    }

    /// A move ends the repeat test as well as the episode. The machine did
    /// something in between, so the same reading afterwards is news again.
    #[test]
    fn a_move_ends_the_episode_and_the_repeat_test() {
        let mut said = Vec::new();
        let mut narration = DwellNarration::default();
        let mut push = |line: &str| said.push(line.to_owned());

        narration.observe(&lost());
        narration.end_dwell(&mut push);
        narration.observe(&lost());
        narration.end_dwell(&mut push);
        narration.flush(&mut push);
        narration.observe(&lost());
        narration.end_dwell(&mut push);

        assert_eq!(
            said,
            [
                rendered(lost()),
                "  ... unchanged across 1 further dwell(s)".to_owned(),
                rendered(lost()),
            ]
        );
    }

    /// A move narrates in full, timing report and all: it is rare, and the
    /// period counts, the jitter and the per-joint lag are what the durations
    /// and the tracking threshold are judged against on a supervised run. A
    /// dwell is never handed those numbers at all — it takes typed events, and
    /// the report is not one of them.
    #[test]
    fn a_move_narrates_its_full_report() {
        let shared = Arc::new(Shared::new(POD));
        let head = Fake::new(&shared, [Event::Engage, Event::Stop(Stop::Operator)])
            // The opening stow says nothing; the move up says what every move
            // says at the end of its run.
            .saying_moving([vec![], REPORT.to_vec()])
            // Every dwell reports the disposition it was asked for, and nothing
            // else — the one thing a dwell always carries.
            .saying([
                vec![TickEvent::Command(CommandDisposition::Held)],
                vec![TickEvent::Command(CommandDisposition::Held)],
            ]);

        let (outcome, _, sink) = driven(&shared, head);

        assert_eq!(outcome, Outcome::Released);
        let said = sink.said().lines;
        for report in REPORT {
            assert_eq!(
                said.iter().filter(|line| *line == report).count(),
                1,
                "the move's report reaches the terminal exactly once: {said:?}"
            );
        }
        assert!(
            !said.contains(&rendered(TickEvent::Command(CommandDisposition::Held))),
            "a dwell narrated the disposition it asked for: {said:?}"
        );
    }

    /// The flush on the way into a move is load-bearing twice over: the span of
    /// the episode the move interrupts has to be stated before the move's own
    /// narration prints under it, and the same reading after the move is news
    /// again because the machine did something in between.
    #[test]
    fn a_move_states_the_span_it_interrupts_and_reopens_the_episode() {
        let shared = Arc::new(Shared::new(POD));
        let head = Fake::new(
            &shared,
            [Event::Idle, Event::Engage, Event::Stop(Stop::Operator)],
        )
        .saying([vec![lost()], vec![lost()], vec![lost()]]);

        let (outcome, _, sink) = driven(&shared, head);

        assert_eq!(outcome, Outcome::Released);
        assert_eq!(
            sink.said().lines,
            [
                "stow: the posture this daemon rests in, torque on".to_owned(),
                rendered(lost()),
                "  ... unchanged across 1 further dwell(s)".to_owned(),
                "presence: stow -> up".to_owned(),
                rendered(lost()),
                "stopping on an operator's signal".to_owned(),
                "released: torque is off and the machine is at rest".to_owned(),
            ]
        );
    }

    /// A dwell that repeats and then refuses states the span before the fault
    /// prints. That span is the whole of the context a bring-up run has for
    /// deciding whether the fault and the condition running underneath it are
    /// the same event.
    #[test]
    fn a_fault_states_the_span_of_the_episode_it_interrupts() {
        let shared = Arc::new(Shared::new(POD));
        let head = Fake::new(&shared, [Event::Idle, Event::Idle, Event::Refuse]).saying([
            vec![lost()],
            vec![lost()],
            vec![lost()],
        ]);

        let (outcome, _, sink) = driven(&shared, head);

        assert!(matches!(outcome, Outcome::Faulted(_)), "{outcome:?}");
        let said = sink.said().lines;
        let span = said
            .iter()
            .position(|line| line == "  ... unchanged across 2 further dwell(s)")
            .unwrap_or_else(|| panic!("the span of the episode is stated: {said:?}"));
        let fault = said
            .iter()
            .position(|line| line.starts_with("fault: "))
            .unwrap_or_else(|| panic!("the fault is narrated: {said:?}"));
        assert!(span < fault, "the span prints under the fault: {said:?}");
    }

    /// End to end through the loop: a health register failing across
    /// consecutive dwells reaches the operator's terminal once. The daemon calls
    /// the motion libraries afresh every dwell and their own de-duplication
    /// resets with it, so this filter is the only thing between a persistent
    /// failure and five identical lines a second for as long as it lasts.
    #[test]
    fn a_health_register_failing_across_dwells_logs_one_episode() {
        let shared = Arc::new(Shared::new(POD));
        let held = TickEvent::Command(CommandDisposition::Held);
        let head = Fake::new(
            &shared,
            [
                Event::Engage,
                Event::Engage,
                Event::Engage,
                Event::Stop(Stop::Operator),
            ],
        )
        .saying([
            vec![held, sweep(0)],
            vec![held, lost()],
            vec![held, lost()],
            vec![held, lost()],
        ]);

        let (outcome, _, sink) = driven(&shared, head);

        assert_eq!(outcome, Outcome::Released);
        let said = sink.said().lines;
        assert_eq!(
            said.iter()
                .filter(|line| **line == rendered(lost()))
                .count(),
            1,
            "one episode, not one line per dwell: {said:?}"
        );
        assert!(
            !said.contains(&rendered(TickEvent::Command(CommandDisposition::Held))),
            "a dwell's disposition reached the terminal: {said:?}"
        );
        assert!(
            said.iter()
                .any(|line| line == "  ... unchanged across 2 further dwell(s)"),
            "the span of the episode is stated when the run ends: {said:?}"
        );
    }
}
