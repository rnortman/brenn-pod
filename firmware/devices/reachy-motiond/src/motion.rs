//! The motion thread: commissioning, the resting watch, and the loop that keeps
//! the head where the running script says it should be.
//!
//! This is the owned loop the motion libraries do not have. It blocks, it holds
//! the serial port for the life of the daemon, and it awaits nothing.
//!
//! The machine spends almost all of its life at the **minimum risk condition** —
//! stowed, torque off, port held — and the loop is built around that:
//!
//! - **Resting.** The machine is limp. Every `rest_poll` the resting watch takes
//!   a sweep, which keeps the pose an engage would plan from current under a
//!   hand that moves the head, and keeps the supply and the error bits current
//!   so the two torque-on gates are arithmetic rather than transactions. A
//!   script asking for the head up is what ends it.
//! - **Active.** Torque is on and the head is executing the running script:
//!   move when the schedule asks for a posture the machine is not in, watch it
//!   hold otherwise. Watching is not idling — a monitored hold is what keeps
//!   position reads, the tracking monitor, the read-loss budget and the
//!   hardware-health sweep running on a machine nobody is looking at. Reaching
//!   stow starts the rest delay, and torque comes off at the end of it.
//! - **Parked.** A fault has taken the machine limp and the daemon commands
//!   nothing further. The port stays held — that is what keeps a second speaker
//!   off the bus — until something asks the process to stop.
//!
//! A dwell is a ceiling, not a period. The schedule knows when its own next step
//! comes due, so each dwell is cut to that instant: a script asking for a stow
//! 6,740 ms after it lands gets one then, not at the next multiple of the
//! configured dwell.
//!
//! Three things are deliberate about the shape:
//!
//! - **Nothing gates torque coming off.** Every ending writes it off: the
//!   orderly one settles, measures and reports where the machine was before it
//!   lets go, and a fault writes the nine releases immediately and looks at
//!   nothing. A machine that cannot be measured, or is not where it was told to
//!   be, is released anyway and said so.
//! - **A fault is absorbing.** The machine goes limp, the daemon parks, and
//!   nothing here retries, re-engages or recovers. The recovery is an operator.
//! - **A refused engage is not a fault.** The two torque-on gates — the supply
//!   floor and the latched error bits — write nothing when they refuse, so the
//!   machine is exactly as it was and the next script may simply ask again.
//!
//! The loop is written against [`Rest`] and [`Active`] rather than against the
//! motion libraries' own typestate. What a move does to nine servos belongs to
//! those libraries and is tested against a scripted machine there; what belongs
//! here is which posture is commanded when, and when torque comes on and off,
//! and that is assertable with no machine, no port and no protocol in the way.
//!
//! What the loop does reaches both of the daemon's streams: the motion
//! libraries' own narration for whoever is watching, and a structured line at
//! each move's start, each move's end, each engage and any fault — so a capture
//! says what the machine did and when, not only what it was asked for. A dwell's
//! share of that narration goes through [`DwellNarration`] first, because five
//! identical reports a second are not something anybody can read.

use std::fmt;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use motion_proto::{Desired, Posture};
use reachy_bench::commands::{
    Commissioned, Engaged, commission, neutral_targets, stow_pose_targets,
};
use reachy_bench::config::{self, Resolved, resolve_for_commanding};
use reachy_bench::pump::{MonotonicClock, PumpError, TickEvent};
use reachy_bus::{BusPort, OpenError, SerialBusPort};
use reachy_motion::{JointTargets, MoveDurations, PollCadence, at_stow};
use serde_json::json;
use thiserror::Error;

use crate::cells::{FaultReport, FaultStage, Shared, Stop};
use crate::config::Overrides;
use crate::report::Sink;

/// The shortest dwell the loop will ask for.
///
/// A dwell is cut to the running script's next boundary, and a boundary a
/// handful of milliseconds away would otherwise ask the machine to hold for
/// almost no time at all — a hold that measures nothing, in a loop that would
/// spend its time starting and stopping runs. Twenty milliseconds is one
/// control period at the tick rates this platform runs, and it bounds how late
/// a step can be executed at far less than the tolerance the schema is designed
/// around.
const MIN_DWELL: Duration = Duration::from_millis(20);

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
/// Everything here happens before a servo is touched, and each variant is a
/// different thing for an operator to go and do: fix the configuration, or find
/// out what else has the bus.
#[derive(Debug, Error)]
pub enum StartupError {
    /// The bench configuration did not resolve. Rendered whole, because its own
    /// message is what names the key or the missing datum.
    #[error("{0}")]
    Config(String),

    /// The serial device could not be opened, or something else already holds
    /// it.
    #[error(transparent)]
    Port(#[from] OpenError),
}

/// The machine this daemon commands, as its configuration describes it.
///
/// Holding one is the evidence that the bench configuration resolved, the crank
/// datum included — calibration, not a gate: nothing about a self-test record
/// stands between this daemon and a machine that answers. It is deliberately
/// separate from the port and from the commissioned bus, because resolving is
/// answered from a file and is the last thing that happens before the daemon
/// starts acquiring anything.
#[derive(Debug)]
pub struct Machine {
    resolved: Resolved,
    clocks: Clocks,
}

impl Machine {
    /// Resolve the bench configuration at `path`, with the daemon's own move
    /// durations laid over it.
    ///
    /// The same file the operator tool reads on this unit, resolved by the same
    /// function, so the daemon and the bench cannot describe different
    /// platforms. The durations are the deliberate exception, and `overrides` is
    /// the whole of it: presence pace is daemon policy, tuned by pushing this
    /// daemon's configuration and restarting it, while the machine's truth stays
    /// in the bench file.
    pub fn resolve(path: &Path, overrides: Overrides) -> Result<Self, StartupError> {
        let cfg = config::load(path).map_err(render)?;
        let resolved = resolve_for_commanding(&cfg).map_err(render)?;
        let clocks = Clocks::resolve(&resolved, overrides);
        Ok(Self { resolved, clocks })
    }

    /// The serial device the configuration names.
    #[must_use]
    pub fn device(&self) -> &str {
        &self.resolved.device
    }

    /// What the daemon's moves will run at, and where each number came from.
    #[must_use]
    pub fn clocks(&self) -> &Clocks {
        &self.clocks
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

    /// Run the once-per-process ceremony over `port` and hand back a resting
    /// machine.
    ///
    /// Presence, identity, the provisioned registers, the supply, the error bits
    /// and the gains — around two hundred transactions, none of which touches
    /// torque. What comes back is limp and stays that way until a script asks
    /// for the head up.
    pub fn commission<P: BusPort>(
        &self,
        port: P,
        line: &mut dyn FnMut(&str),
    ) -> Result<SessionRest<'_, P>, Refusal> {
        let mut clock = MonotonicClock::new();
        let machine = commission(&self.resolved, port, &mut clock, line)?;
        Ok(SessionRest {
            machine,
            resolved: &self.resolved,
            up: self.clocks.up_durations(),
            stow: self.clocks.stow_durations(),
            clock,
            rail_every: rail_period(self.resolved.health_poll_hz),
            last_rail: None,
        })
    }
}

/// Which file a resolved duration came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// The bench configuration — the machine's own file, shared with the
    /// operator tool.
    Bench,
    /// This daemon's configuration, overriding it.
    Daemon,
}

impl Source {
    /// The file, as the startup line names it.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Bench => "bench",
            Self::Daemon => "daemon",
        }
    }
}

/// One resolved duration and the file it came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Clock {
    /// How long the move takes.
    pub duration: Duration,
    /// Which file said so.
    pub from: Source,
}

/// What the daemon's two moves run at, per mechanical group.
///
/// The head group — the head pose and the body yaw, which the legs follow — and
/// the antennas are independent joints, so they carry independent clocks: a lift
/// tuned to be quick has no business being floored by an antenna arc long enough
/// to stay inside its own per-tick step bound.
///
/// Resolved once, at startup, from two files. Narrated at startup too, sources
/// included, because a head moving at a pace nobody expects is otherwise a
/// question that takes two files and a guess to answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Clocks {
    /// The raise's head group.
    pub up: Clock,
    /// The fold's head group.
    pub stow: Clock,
    /// The antennas, or `None` when neither file gives them one and they run on
    /// whichever head-group clock the move is using.
    pub antennas: Option<Clock>,
}

impl Clocks {
    /// Lay the daemon's durations over the machine's.
    #[must_use]
    pub fn resolve(resolved: &Resolved, overrides: Overrides) -> Self {
        Self::lay_over(
            resolved.up_duration,
            resolved.stow_duration,
            resolved.antenna_duration,
            overrides,
        )
    }

    /// The resolution itself, in the three numbers the machine's file
    /// contributes.
    ///
    /// Split out from [`Clocks::resolve`] because a `Resolved` is a whole bench
    /// configuration — a servo map, an envelope and a measured datum — and what
    /// is worth asserting here is which of two files won, which needs none of
    /// that.
    fn lay_over(
        bench_up: Duration,
        bench_stow: Duration,
        bench_antennas: Option<Duration>,
        overrides: Overrides,
    ) -> Self {
        Self {
            up: pick(bench_up, overrides.up),
            stow: pick(bench_stow, overrides.stow),
            antennas: match (overrides.antennas, bench_antennas) {
                (Some(duration), _) => Some(Clock {
                    duration,
                    from: Source::Daemon,
                }),
                (None, Some(duration)) => Some(Clock {
                    duration,
                    from: Source::Bench,
                }),
                (None, None) => None,
            },
        }
    }

    /// The raise's per-group durations.
    #[must_use]
    pub fn up_durations(&self) -> MoveDurations {
        self.durations(self.up.duration)
    }

    /// The fold's per-group durations.
    #[must_use]
    pub fn stow_durations(&self) -> MoveDurations {
        self.durations(self.stow.duration)
    }

    /// `head` for the head group, and the antennas' own clock beside it.
    fn durations(&self, head: Duration) -> MoveDurations {
        MoveDurations {
            head,
            antennas: self.antennas.map_or(head, |clock| clock.duration),
        }
    }

    /// The resolved durations, for the capture.
    #[must_use]
    pub fn json(&self) -> serde_json::Value {
        json!({
            "up_ms": millis(self.up.duration),
            "up_from": self.up.from.as_str(),
            "stow_ms": millis(self.stow.duration),
            "stow_from": self.stow.from.as_str(),
            "antenna_ms": self.antennas.map(|clock| millis(clock.duration)),
            "antenna_from": self.antennas.map(|clock| clock.from.as_str()),
        })
    }
}

impl fmt::Display for Clocks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "up {} ({}), stow {} ({})",
            secs(self.up.duration),
            self.up.from.as_str(),
            secs(self.stow.duration),
            self.stow.from.as_str()
        )?;
        match self.antennas {
            Some(clock) => write!(
                f,
                ", antennas {} ({})",
                secs(clock.duration),
                clock.from.as_str()
            ),
            None => f.write_str(", antennas on the head group's clock"),
        }
    }
}

/// A stated duration, or the machine's own where nothing states one.
fn pick(bench: Duration, stated: Option<Duration>) -> Clock {
    match stated {
        Some(duration) => Clock {
            duration,
            from: Source::Daemon,
        },
        None => Clock {
            duration: bench,
            from: Source::Bench,
        },
    }
}

/// A duration in seconds, as the startup line writes it.
fn secs(duration: Duration) -> String {
    format!("{:.3} s", duration.as_secs_f64())
}

/// How long between two resting sweeps that re-read the supply and the error
/// bits, at a configured health-poll rate.
///
/// Zero is floored to one rather than divided by, because the bench
/// configuration is what refuses a zero rate and this daemon is not the place
/// to discover it: a panic here would take down the thread that owns the port.
/// One sweep a second is the slowest this can degrade to.
fn rail_period(health_poll_hz: u32) -> Duration {
    Duration::from_secs(1) / health_poll_hz.max(1)
}

/// Whether this sweep re-reads the supply and the error bits.
///
/// The two torque-on gates are evaluated from whatever the last such sweep
/// read, so this decides how stale their inputs may be: never re-reading leaves
/// them judging an engage against commissioning-time numbers, and re-reading on
/// every sweep makes the resting watch most of the traffic on the wire. Daemon
/// policy, so it is arithmetic here rather than a branch buried in a transaction.
fn rail_due(last: Option<Instant>, now: Instant, every: Duration) -> bool {
    last.is_none_or(|last| now.duration_since(last) >= every)
}

/// A configuration error, rendered whole.
///
/// `{:#}` rather than `{}`: the errors are a chain, and the alternate form
/// carries the whole of it onto one line. Generic over the error to avoid a
/// dependency on the resolver's own error type purely to re-render it.
fn render<E: fmt::Display>(error: E) -> StartupError {
    StartupError::Config(format!("{error:#}"))
}

/// Where the resting watch found the machine.
///
/// Never a verdict: a head somebody turned is a measurement, and the only thing
/// the loop does with it is decide whether there is anything to put right.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Standing {
    /// Folded, within the release tolerance of the stow pose.
    AtStow,
    /// Somewhere else — a crash, a hand, or a fault that let the head settle
    /// where it fell.
    Elsewhere,
}

/// What the orderly release measured on its way out.
///
/// Torque comes off whatever this says — nothing here is a condition — but it
/// is the last thing anybody measures before the machine is left alone for
/// hours, and it is the one fact that decides whether a hand can go near the
/// head. It reaches the capture and, when the fold missed, an operator.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Verdict {
    /// Whether every joint was measured and every one inside the stow
    /// tolerance.
    pub at_stow: bool,
    /// How far the joint furthest from its stow angle was, degrees.
    pub worst_deg: f64,
    /// How many joints the release looked at and could not read at all. A joint
    /// nobody could read is why `at_stow` can be false with nothing visibly out
    /// of place.
    pub unreadable: usize,
}

impl Verdict {
    /// Where the machine was left, in the one word the capture carries.
    #[must_use]
    pub fn at(&self) -> &'static str {
        if self.at_stow { "stow" } else { "elsewhere" }
    }

    /// Where a release leaves the machine standing, for the next engage to plan
    /// from.
    #[must_use]
    pub fn standing(&self) -> Standing {
        if self.at_stow {
            Standing::AtStow
        } else {
            Standing::Elsewhere
        }
    }
}

/// Why an engage did not happen.
///
/// The distinction the whole unattended lifecycle turns on: one of these leaves
/// a machine to bring back to the minimum risk condition and the other does
/// not.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EngageFailed {
    /// One of the two torque-on gates refused — the supply below its floor, or
    /// a latched hardware error. Nothing was written, the machine is limp
    /// exactly where it was, and the next script may ask again.
    #[error("{0}")]
    Gate(Refusal),

    /// Anything else. The engage path takes the machine limp on its way out, so
    /// what is left is a fault to report and a daemon to park.
    #[error("{0}")]
    Fault(Refusal),
}

impl From<PumpError> for EngageFailed {
    fn from(error: PumpError) -> Self {
        if error.is_gate_refusal() {
            Self::Gate(error.into())
        } else {
            Self::Fault(error.into())
        }
    }
}

/// The machine at rest: limp, watched, and ready to be taken hold of.
///
/// The loop's contact with a resting machine, and all of it. Written as a trait
/// because the daemon's decisions — when to look, when to engage, when to let
/// go — are what this crate owns, while what a sweep or an enable does to nine
/// servos belongs to the motion libraries.
pub trait Rest {
    /// This machine once torque is on. Borrows the resting form, so the bus and
    /// the port never change hands across an engage/release cycle.
    type Active<'e>: Active
    where
        Self: 'e;

    /// Take one sweep of the resting watch: where the machine is standing, and
    /// on the slower cadence what its supply and error bits read.
    fn watch(&mut self, line: &mut dyn FnMut(&str)) -> Result<Standing, Refusal>;

    /// Pin every joint where the last sweep found it and enable torque.
    fn engage(&mut self, line: &mut dyn FnMut(&str)) -> Result<Self::Active<'_>, EngageFailed>;
}

/// The machine holding torque: the postures, the moves between them, and the
/// two ways torque comes off.
pub trait Active {
    /// Carry the head to `posture`, and leave it holding wherever the move
    /// finally ended.
    ///
    /// `retarget` is asked at every control period whether `posture` is still
    /// what is wanted; answering `Some` turns the head around from where it has
    /// got to, and the answer becomes the move. What comes back is the posture
    /// the machine is actually holding, which is the last one answered and not
    /// necessarily the one asked for.
    ///
    /// A script can land while the head is halfway up, and a move that had to
    /// finish before the reverse one started would put the head down a whole
    /// move late — which is the thing a conversation notices.
    ///
    /// `retarget` must answer `None` for the posture already in flight: a
    /// caller that keeps answering with it would keep restarting the same
    /// trajectory and the head would never arrive.
    fn move_to(
        &mut self,
        posture: Posture,
        line: &mut dyn FnMut(&str),
        retarget: &mut dyn FnMut() -> Option<Posture>,
    ) -> Result<Posture, Refusal>;

    /// Watch the machine hold for `dwell`, commanding nothing.
    ///
    /// Typed events rather than rendered lines: what a dwell is worth saying
    /// about is this daemon's policy, and keying on the event kind is what
    /// keeps that policy off the wording another repository chose.
    fn hold(&mut self, dwell: Duration, event: &mut dyn FnMut(TickEvent)) -> Result<(), Refusal>;

    /// Settle, measure where the machine came to rest, and release torque.
    ///
    /// The expected ending. The measurement is a report and never a condition:
    /// a machine found away from stow is released and said so, which is what
    /// the [`Verdict`] carries. An `Err` means the orderly path did not
    /// complete cleanly, but torque was still written off — a machine returned
    /// from this method is always limp. Consumes the engagement, because
    /// commanding a limp machine would pump goal frames at servos that cannot
    /// follow them.
    fn disengage(self, line: &mut dyn FnMut(&str)) -> Result<Verdict, Refusal>
    where
        Self: Sized;

    /// Write torque off to all nine servos now, and do nothing else.
    ///
    /// The fault ending. No settle and no measurement: the head falls gently
    /// into near-stow under gearbox resistance from wherever it is. An `Err`
    /// says what the release itself had to report — a servo that never
    /// acknowledged its own torque-off — and never that the release did not
    /// happen.
    fn disengage_now(self, line: &mut dyn FnMut(&str)) -> Result<(), Refusal>
    where
        Self: Sized;
}

/// The commissioned machine at rest, as the loop sees it.
///
/// Owns the clock as well as the bus, because the clock is the loop's: every
/// sweep, move and dwell is paced by the same monotonic source, on the one
/// thread that is allowed to block.
pub struct SessionRest<'a, P: BusPort> {
    machine: Commissioned<'a, P>,
    resolved: &'a Resolved,
    /// The raise's per-group durations, resolved once at startup from the two
    /// files that have a say in them.
    up: MoveDurations,
    /// The fold's, likewise.
    stow: MoveDurations,
    clock: MonotonicClock,
    /// How long between two sweeps that re-read the supply and the error bits.
    /// The positions move under a hand; those change on the timescale of a power
    /// supply, and reading them every sweep would be most of the resting
    /// traffic on the wire.
    rail_every: Duration,
    /// When a sweep last read them, or `None` when none has since
    /// commissioning.
    last_rail: Option<Instant>,
}

/// The same machine holding torque.
pub struct SessionActive<'m, 'a, P: BusPort> {
    engaged: Engaged<'m, 'a, P>,
    clock: MonotonicClock,
    up: MoveDurations,
    // TODO(recovery-move-clock): both clocks are configuration, resolved once
    // and sized for the spans a presence move covers. The startup stow runs on
    // `stow` from wherever the machine was left, and a body left more than about
    // 153° round at the shipped value is further than that clock can carry
    // within the per-tick step bound — the move faults partway and de-torques.
    stow: MoveDurations,
}

/// The pose a posture means, paired with the durations the move to it runs
/// over.
///
/// The one place a script's posture becomes a pose, kept out of the trait
/// implementation so it is assertable with no port and no servo: swapping the two
/// arms inverts the whole feature, and swapping the two durations runs every move
/// at the wrong speed, neither of which any envelope check refuses.
#[must_use]
pub fn targets_for(
    posture: Posture,
    up: MoveDurations,
    stow: MoveDurations,
) -> (JointTargets, MoveDurations) {
    match posture {
        Posture::Up => (neutral_targets(), up),
        Posture::Stow => (stow_pose_targets(), stow),
    }
}

impl<'a, P: BusPort> Rest for SessionRest<'a, P> {
    type Active<'e>
        = SessionActive<'e, 'a, P>
    where
        Self: 'e;

    fn watch(&mut self, line: &mut dyn FnMut(&str)) -> Result<Standing, Refusal> {
        let now = Instant::now();
        let rail_due = rail_due(self.last_rail, now, self.rail_every);
        let cadence = if rail_due {
            PollCadence::PositionsAndRail
        } else {
            PollCadence::Positions
        };
        let sweep = self.machine.poll(cadence, &mut self.clock, line)?;
        if rail_due {
            self.last_rail = Some(now);
        }
        Ok(if at_stow(&self.resolved.disarm, &sweep.present) {
            Standing::AtStow
        } else {
            Standing::Elsewhere
        })
    }

    fn engage(&mut self, line: &mut dyn FnMut(&str)) -> Result<Self::Active<'_>, EngageFailed> {
        let (up, stow) = (self.up, self.stow);
        let mut clock = self.clock;
        let engaged = self.machine.engage(&mut clock, line)?;
        Ok(SessionActive {
            engaged,
            clock,
            up,
            stow,
        })
    }
}

impl<P: BusPort> Active for SessionActive<'_, '_, P> {
    fn move_to(
        &mut self,
        posture: Posture,
        line: &mut dyn FnMut(&str),
        retarget: &mut dyn FnMut() -> Option<Posture>,
    ) -> Result<Posture, Refusal> {
        let (up, stow) = (self.up, self.stow);
        let (targets, durations) = targets_for(posture, up, stow);
        let mut arrived = posture;
        self.engaged
            .move_retargeting(targets, durations, &mut self.clock, line, &mut || {
                let next = retarget()?;
                arrived = next;
                Some(targets_for(next, up, stow))
            })?;
        Ok(arrived)
    }

    fn hold(&mut self, dwell: Duration, event: &mut dyn FnMut(TickEvent)) -> Result<(), Refusal> {
        // The summary is dropped: a 200 ms window's period counts and jitter say
        // nothing a reader or this loop can act on, and the conditions worth
        // knowing about all arrive as events.
        self.engaged.hold_events(dwell, &mut self.clock, event)?;
        Ok(())
    }

    fn disengage(self, line: &mut dyn FnMut(&str)) -> Result<Verdict, Refusal> {
        let Self {
            engaged, mut clock, ..
        } = self;
        let summary = engaged.disengage(&mut clock, line)?;
        Ok(Verdict {
            at_stow: summary.at_stow,
            worst_deg: summary.worst_deviation().1.to_degrees(),
            unreadable: summary.unreadable().count(),
        })
    }

    fn disengage_now(self, line: &mut dyn FnMut(&str)) -> Result<(), Refusal> {
        let Self {
            engaged, mut clock, ..
        } = self;
        engaged.disengage_now(&mut clock, line)?;
        Ok(())
    }
}

/// How the motion thread ended.
///
/// Two endings, because there are two states a machine can be left in and both
/// of them are limp. What differs is whether the daemon got there on purpose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Stowed and released, on an ending the daemon was asked for. The reason
    /// rides along because it is what the exit status distinguishes: a stop is
    /// a clean end, and a bridge that will never carry another script is a
    /// configuration problem wearing the same posture.
    Released(Stop),

    /// The machine stopped taking commands and torque was written off
    /// immediately. The head has settled into near-stow and the daemon parked
    /// at the minimum risk condition.
    Faulted(FaultReport),
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Released(Stop::Operator) => f.write_str("released at stow"),
            Self::Released(Stop::Detached) => f.write_str("detached, released at stow"),
            Self::Faulted(report) => write!(f, "{report}"),
        }
    }
}

impl Outcome {
    /// Whether this ending is the clean one. The daemon's exit status, and the
    /// difference between a run something ended and a run that ended itself.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        matches!(self, Self::Released(Stop::Operator))
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

/// The three periods the loop is paced by.
///
/// All three are daemon policy and all three are configuration: how long a
/// monitored hold runs before the schedule is read again, how often a limp
/// machine is looked at, and how long the head stays torqued at stow before it
/// is let go of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timing {
    /// The longest one monitored hold runs for while the machine is engaged.
    pub dwell: Duration,
    /// Between two sweeps of the resting watch.
    pub rest_poll: Duration,
    /// How long the machine holds at stow before torque comes off. A re-wake
    /// inside the window retargets the head up with no release and no engage in
    /// between, which is the quick follow-up case.
    pub rest_delay: Duration,
}

/// What the loop carries from one phase to the next.
#[derive(Debug, Default)]
struct Watch {
    /// Dwells narrate through this; moves and endings narrate straight through.
    dwells: DwellNarration,
    /// The script whose lapse has already been reported. Expiry is the answer
    /// at every boundary from then until a new script lands, and a run that
    /// said so each time would bury the one line that matters.
    lapse_reported: Option<u64>,
    /// The script whose engage a torque-on gate refused.
    ///
    /// The refusal is retried by the *next* script rather than at the next
    /// resting sweep: a chronically sagging rail against a script asking for
    /// the head up would otherwise be ten refusals a second, and the accepted
    /// rate is one per script.
    engage_refused_for: Option<u64>,
}

/// Why a phase handed control back.
///
/// Both endings leave the machine limp. What differs is whether the daemon put
/// it there deliberately, and therefore whether the process exits or parks.
#[derive(Debug)]
enum Ending {
    /// Something asked the daemon to stop, and the machine has been released.
    Stopped(Stop),
    /// The machine stopped taking commands. Torque has been written off; the
    /// daemon parks holding the port.
    Faulted(FaultStage, Refusal),
}

/// Keep the head where the running script asks for it until something stops the
/// daemon.
///
/// The machine arrives commissioned and limp. Startup normalisation looks at
/// where it is standing and folds it if a crash or a hand left it somewhere
/// else, and from then on the cycle is Resting → Active → Resting: a script
/// asking for the head up engages, its timeline is executed, reaching stow
/// starts the rest delay, and the release at the end of that puts the machine
/// back at the minimum risk condition.
///
/// The schedule is read between dwells and, while a move is travelling, at
/// every one of its control periods: a script that arrives mid-move turns that
/// move around from where the head has got to. Never a queue and never a
/// refusal — the latest script is the only one there is.
pub fn run<R: Rest>(machine: R, shared: &Shared, timing: Timing, sink: &dyn Sink) -> Outcome {
    let outcome = cycle(machine, shared, timing, sink);
    // Last, and after every ending: the bus thread keeps the attachment up until
    // this is set, so a fault taken during the ending still has somewhere to
    // send its alert.
    shared.end_motion();
    outcome
}

/// The loop itself. Separate from [`run`] only so every one of its endings goes
/// through the one place that notes the machine is no longer being touched.
fn cycle<R: Rest>(mut machine: R, shared: &Shared, timing: Timing, sink: &dyn Sink) -> Outcome {
    let mut watch = Watch::default();
    match phases(&mut machine, shared, timing, &mut watch, sink) {
        Ending::Stopped(stop) => {
            sink.line(&format!(
                "stopped on {stop}: the machine is at rest, torque off"
            ));
            Outcome::Released(stop)
        }
        Ending::Faulted(stage, refusal) => park(machine, shared, stage, refusal, sink),
    }
}

/// Normalisation once, then Resting and Active in turn until one of them ends
/// the run.
fn phases<R: Rest>(
    machine: &mut R,
    shared: &Shared,
    timing: Timing,
    watch: &mut Watch,
    sink: &dyn Sink,
) -> Ending {
    // Where the machine is standing follows the loop from phase to phase: the
    // resting watch measures it, a release reports it, and an engage plans the
    // first move of the next turn from it. Assuming it instead is how a head
    // gets released from wherever it happens to be standing.
    let mut standing = match normalise(machine, shared, watch, sink) {
        Ok(standing) => standing,
        Err(ending) => return ending,
    };
    loop {
        standing = match resting(machine, shared, timing, watch, sink, standing) {
            Ok(standing) => standing,
            Err(ending) => return ending,
        };
        standing = match active(machine, shared, timing, watch, sink, standing) {
            Ok(standing) => standing,
            Err(ending) => return ending,
        };
    }
}

/// Put the machine at the minimum risk condition before anything else happens.
///
/// A crash, a fault or a hand can leave the head anywhere, and the answer is to
/// measure it and fold it — never to refuse it. Where the machine is standing
/// is physical reality: the only thing that can be done about it is to plan a
/// move out of it, and this is that move.
fn normalise<R: Rest>(
    machine: &mut R,
    shared: &Shared,
    watch: &mut Watch,
    sink: &dyn Sink,
) -> Result<Standing, Ending> {
    let standing = machine
        .watch(&mut |text| sink.line(text))
        .map_err(|refusal| Ending::Faulted(FaultStage::Startup, refusal))?;
    sink.event(
        "motion_startup",
        &json!({ "at_stow": standing == Standing::AtStow }),
    );
    if standing == Standing::AtStow {
        sink.line("startup: the machine is folded already; leaving it limp");
        return Ok(standing);
    }

    sink.line("startup: the machine is not at stow. taking hold to fold it, then letting go.");
    let mut head = match take_hold(machine, shared, watch, sink) {
        Ok(head) => head,
        // Limp and crooked is still limp: the gate wrote nothing, the machine
        // is at no more risk than it was, and the next script's engage plans
        // from wherever it is standing — which is what the answer says.
        Err(EngageFailed::Gate(_)) => return Ok(Standing::Elsewhere),
        Err(EngageFailed::Fault(refusal)) => {
            return Err(Ending::Faulted(FaultStage::Engage, refusal));
        }
    };
    started(sink, None, Posture::Stow, "startup");
    // Not steered: normalisation is one sequence with one ending, and a machine
    // whose pose nobody has ever commanded is not the one to start splicing
    // trajectories on. A script that lands during the fold is executed by the
    // loop this returns into, from a known pose.
    let folded = head.move_to(Posture::Stow, &mut |text| sink.line(text), &mut || None);
    if let Err(refusal) = folded {
        return Err(fault_now(head, FaultStage::Startup, refusal, sink));
    }
    reached(sink, Posture::Stow);
    fold_and_rest(head, shared, FaultStage::Startup, Some("startup"), sink)
}

/// The minimum risk condition, watched.
///
/// Nothing is commanded here and torque is off. The sweep is what keeps the
/// pose an engage plans from current — a hand can turn a limp head — and what
/// keeps the two torque-on gates' readings in hand, so a wake costs an enable
/// and not a diagnosis. Answers `Ok` when a script wants the head up.
fn resting<R: Rest>(
    machine: &mut R,
    shared: &Shared,
    timing: Timing,
    watch: &mut Watch,
    sink: &dyn Sink,
    entered: Standing,
) -> Result<Standing, Ending> {
    sink.line("resting: torque off, the port held, watching the machine");
    sink.event(
        "motion_resting",
        &json!({ "poll_ms": millis(timing.rest_poll) }),
    );
    let mut standing = entered;
    loop {
        if let Some(stop) = shared.stopping() {
            // Nothing to command and nothing to release: the machine is already
            // where every ending is trying to get it.
            return Err(Ending::Stopped(stop));
        }
        if wants_up(shared, watch) {
            return Ok(standing);
        }
        standing = machine
            .watch(&mut |text| sink.line(text))
            .map_err(|refusal| Ending::Faulted(FaultStage::Resting, refusal))?;
        thread::sleep(timing.rest_poll);
    }
}

/// Whether a script is asking for the head up, and this daemon may act on it.
///
/// Only the raise wakes the machine. A script asking for stow is asking for the
/// posture a resting machine is already in, and an expiry is the end of
/// instruction — neither is a reason to put torque on nine servos.
fn wants_up(shared: &Shared, watch: &Watch) -> bool {
    matches!(
        shared.desired(Instant::now()),
        Desired::Posture(Posture::Up)
    ) && watch.engage_refused_for != shared.accepted_seq()
}

/// Torque on: execute the running script until it is spent, the daemon is
/// stopped, or the machine refuses.
///
/// Answers `Ok` when the head has been folded and released back to Resting.
fn active<R: Rest>(
    machine: &mut R,
    shared: &Shared,
    timing: Timing,
    watch: &mut Watch,
    sink: &dyn Sink,
    standing: Standing,
) -> Result<Standing, Ending> {
    let mut head = match take_hold(machine, shared, watch, sink) {
        Ok(head) => head,
        Err(EngageFailed::Gate(_)) => return Ok(standing),
        Err(EngageFailed::Fault(refusal)) => {
            return Err(Ending::Faulted(FaultStage::Engage, refusal));
        }
    };

    // Engaging pins the machine where the resting watch found it, so the
    // posture the loop starts from is that measurement and not an assumption. A
    // machine found standing has no posture at all — no desired posture equals
    // it, so the first pass commands the fold rather than skipping it and
    // releasing a head from wherever it happens to be.
    let mut posture = match standing {
        Standing::AtStow => Some(Posture::Stow),
        Standing::Elsewhere => None,
    };
    // When the head reached stow with nothing else to do, which is what the
    // rest delay is measured from.
    let mut settled: Option<Instant> = None;

    loop {
        if let Some(stop) = shared.stopping() {
            watch.dwells.flush(&mut |text| sink.line(text));
            return Err(release_for(head, posture, stop, shared, sink));
        }

        let (desired, reason) = match wanted(shared, watch, sink) {
            Some(wanted) => wanted,
            // Nothing asks for a change, and a machine whose posture is unknown
            // still has to be folded: the fold is the change.
            None => (posture.unwrap_or(Posture::Stow), "script"),
        };
        if Some(desired) != posture {
            watch.dwells.flush(&mut |text| sink.line(text));
            sink.line(&format!("motion: {} -> {desired}", from(posture)));
            started(sink, posture, desired, reason);
            // The move is steered rather than waited out: the schedule is
            // written by the bus thread while this one is on the wire, and a
            // raise that had to finish before the fold could start would put
            // the head down a whole move after it was asked for.
            let mut in_flight = desired;
            let outcome = head.move_to(desired, &mut |text| sink.line(text), &mut || {
                let (next, reason) = retarget_to(shared, watch, sink, in_flight)?;
                sink.line(&format!("motion: {in_flight} -> {next}, mid-move"));
                started(sink, Some(in_flight), next, reason);
                in_flight = next;
                Some(next)
            });
            let arrived = match outcome {
                Ok(arrived) => arrived,
                Err(refusal) => return Err(fault_now(head, FaultStage::Motion, refusal, sink)),
            };
            posture = Some(arrived);
            settled = None;
            reached(sink, arrived);
            continue;
        }

        // Folded with nothing asking otherwise: hold for the rest delay, so a
        // quick follow-up costs no release and no engage, and then let go.
        let mut until = None;
        if posture == Some(Posture::Stow) {
            let since = *settled.get_or_insert_with(Instant::now);
            let ends_at = since + timing.rest_delay;
            if Instant::now() >= ends_at {
                return fold_and_rest(head, shared, FaultStage::Release, Some(reason), sink);
            }
            until = Some(ends_at);
        }

        let held = head.hold(dwell_for(shared, timing.dwell, until), &mut |event| {
            watch.dwells.observe(&event);
        });
        watch.dwells.end_dwell(&mut |text| sink.line(text));
        if let Err(refusal) = held {
            watch.dwells.flush(&mut |text| sink.line(text));
            return Err(fault_now(head, FaultStage::Motion, refusal, sink));
        }
    }
}

/// The posture a move is leaving, in the narration's words.
fn from(posture: Option<Posture>) -> &'static str {
    posture.map_or("wherever it was left", Posture::as_str)
}

/// Pin the machine where it stands and enable torque, timing it and saying so.
///
/// The engage wall clock is on every engage because a wake word is supposed to
/// reach the servos in tens of milliseconds, and a capture is where that
/// number is read off.
fn take_hold<'e, R: Rest>(
    machine: &'e mut R,
    shared: &Shared,
    watch: &mut Watch,
    sink: &dyn Sink,
) -> Result<R::Active<'e>, EngageFailed> {
    // Read before the attempt, not after it. An engage takes tens of
    // milliseconds and the bus thread writes the schedule the whole time, so a
    // script that lands during the attempt would otherwise have this refusal —
    // which was about the script before it — recorded against it, and its own
    // raise dropped without a word.
    let asking = shared.accepted_seq();
    let began = Instant::now();
    let outcome = machine.engage(&mut |text| sink.line(text));
    let took = millis(began.elapsed());
    match outcome {
        Ok(head) => {
            sink.line(&format!("engaged: torque on, {took} ms"));
            sink.event("motion_engaged", &json!({ "ms": took }));
            Ok(head)
        }
        Err(EngageFailed::Gate(refusal)) => {
            // Not a fault: nothing was written, so there is nothing to undo and
            // nothing to park for. It is still worth waking somebody up about —
            // a machine that cannot take torque is a machine that will not
            // answer the next wake word either.
            watch.engage_refused_for = asking;
            sink.line(&format!(
                "engage refused: {refusal}. torque was not written; the machine is limp where \
                 it stands and the next script tries again."
            ));
            sink.event(
                "motion_engage_refused",
                &json!({
                    "detail": refusal.to_string(),
                    "seq": asking,
                    "ms": took,
                }),
            );
            shared.refuse_engage(refusal.to_string());
            Err(EngageFailed::Gate(refusal))
        }
        Err(fault) => Err(fault),
    }
}

/// What the schedule asks of the machine now, or `None` when it asks for no
/// change.
///
/// The lapse is said once per script rather than once per boundary: expiry is
/// the answer from the moment it happens until another script lands.
fn wanted(shared: &Shared, watch: &mut Watch, sink: &dyn Sink) -> Option<(Posture, &'static str)> {
    match shared.desired(Instant::now()) {
        Desired::Unchanged => None,
        Desired::Posture(wanted) => Some((wanted, "script")),
        Desired::Expired => {
            let seq = shared.accepted_seq();
            if watch.lapse_reported != seq {
                watch.lapse_reported = seq;
                watch.dwells.flush(&mut |text| sink.line(text));
                sink.line("script: lapsed; folding the head and going back to rest");
                sink.event("motion_script_expired", &json!({ "seq": seq }));
            }
            Some((Posture::Stow, "timeout"))
        }
    }
}

/// What a move already carrying the head to `in_flight` should become, or
/// `None` when it is still the right move.
///
/// Asked at every control period of a move, which is why it answers `None` for
/// the posture already in flight rather than re-commanding it: the tick would
/// take that as a replacement and shape a fresh trajectory from the setpoint
/// every period, and the head would creep instead of arriving.
///
/// A stop is answered first, and answered with the fold. A daemon asked to stop
/// while the head is on its way up has no reason to finish the raise — the
/// shutdown path's own stow is the move this becomes, run early and while the
/// loop is still the thing driving it.
fn retarget_to(
    shared: &Shared,
    watch: &mut Watch,
    sink: &dyn Sink,
    in_flight: Posture,
) -> Option<(Posture, &'static str)> {
    let (next, reason) = match shared.stopping() {
        Some(_) => (Posture::Stow, "shutdown"),
        None => wanted(shared, watch, sink)?,
    };
    (next != in_flight).then_some((next, reason))
}

/// How long the next dwell watches for: the configured ceiling, cut short by
/// the running script's next boundary and by `until` if there is one.
///
/// What makes a step land on the script's own clock rather than on a multiple of
/// the dwell. Floored at [`MIN_DWELL`] so a boundary that has just passed — or
/// one a millisecond away — cannot turn the loop into a spin of empty runs; the
/// posture it is about is read on the very next pass either way.
fn dwell_for(shared: &Shared, ceiling: Duration, until: Option<Instant>) -> Duration {
    let now = Instant::now();
    [shared.next_boundary(now), until]
        .into_iter()
        .flatten()
        .fold(ceiling, |dwell, edge| {
            dwell.min(edge.saturating_duration_since(now))
        })
        .max(MIN_DWELL)
}

/// A move as it starts. The timestamp on this line is what a capture measures
/// wake-to-motion against, so it is emitted before the move is commanded and not
/// after it lands.
fn started(sink: &dyn Sink, from: Option<Posture>, to: Posture, reason: &str) {
    sink.event(
        "motion_move",
        &json!({
            "from": from.map(Posture::as_str),
            "to": to.as_str(),
            "reason": reason,
        }),
    );
}

/// A move that landed. The pair with [`started`] is what makes a move's duration
/// readable off the capture rather than off the narration.
fn reached(sink: &dyn Sink, posture: Posture) {
    sink.event("motion_posture", &json!({ "state": posture.as_str() }));
}

/// Fold the head if it is not folded, then release: the expected ending, and
/// the one every stop takes.
///
/// The stow is commanded while this thread still owns the port and the machine
/// still has torque, because a head released where it stands is a head that
/// falls the rest of the way. A refusal on the way down does not keep torque
/// on — it takes the immediate release instead.
fn release_for<A: Active>(
    mut head: A,
    posture: Option<Posture>,
    stop: Stop,
    shared: &Shared,
    sink: &dyn Sink,
) -> Ending {
    sink.line(&format!("stopping on {stop}"));
    if posture != Some(Posture::Stow) {
        started(sink, posture, Posture::Stow, "shutdown");
        // Nothing may divert this one: the daemon is on its way out and the
        // fold is the last thing it owes the machine.
        let folded = head.move_to(Posture::Stow, &mut |text| sink.line(text), &mut || None);
        if let Err(refusal) = folded {
            return fault_now(head, FaultStage::Shutdown, refusal, sink);
        }
        reached(sink, Posture::Stow);
    }
    match head.disengage(&mut |text| sink.line(text)) {
        Ok(verdict) => {
            released(shared, sink, Some("shutdown"), verdict);
            Ending::Stopped(stop)
        }
        Err(refusal) => Ending::Faulted(FaultStage::Shutdown, refusal),
    }
}

/// Let torque go and answer `Ok`, so the caller goes back to resting.
///
/// `reason` is the script's word for why the head came down, carried onto the
/// released event: a stow step and a lapsed timeout end the same way and are
/// different things to read in a capture.
fn fold_and_rest<A: Active>(
    head: A,
    shared: &Shared,
    stage: FaultStage,
    reason: Option<&str>,
    sink: &dyn Sink,
) -> Result<Standing, Ending> {
    match head.disengage(&mut |text| sink.line(text)) {
        Ok(verdict) => {
            released(shared, sink, reason, verdict);
            Ok(verdict.standing())
        }
        Err(refusal) => Err(Ending::Faulted(stage, refusal)),
    }
}

/// The one line and the one event a release is worth, and the alert it owes
/// when the fold was not where it was supposed to be.
///
/// The verdict is the last measurement anybody takes before the machine is left
/// alone, so it is reported rather than assumed: a capture that said `at: stow`
/// about a head released away from its fold would be positively wrong about the
/// one fact that decides whether a hand can go near it. Torque came off either
/// way — that is the doctrine, and a miss is a report, never a refusal.
fn released(shared: &Shared, sink: &dyn Sink, reason: Option<&str>, verdict: Verdict) {
    if verdict.at_stow {
        sink.line("released: torque is off and the machine is at the minimum risk condition");
    } else {
        let detail = format!(
            "released away from stow: {:.1}° off at the worst joint, {} joint(s) unreadable. \
             torque is off, so the head is limp wherever it is standing.",
            verdict.worst_deg, verdict.unreadable
        );
        sink.line(&detail);
        shared.note_stow_miss(detail);
    }
    sink.event(
        "motion_released",
        &json!({
            "at": verdict.at(),
            "reason": reason,
            "worst_deg": verdict.worst_deg,
            "unreadable": verdict.unreadable,
        }),
    );
}

/// The fault response: torque off now, and nothing else.
///
/// No stow attempt first. A fault means motor control or position feedback is
/// no longer trusted, so a commanded move is exactly what cannot be relied on;
/// the head falls gently into near-stow under gearbox resistance instead. What
/// the release has to say about itself — a servo that never acknowledged its
/// own torque-off — is carried into the fault report, because that is the one
/// thing that decides whether a hand can go on the head.
fn fault_now<A: Active>(head: A, stage: FaultStage, refusal: Refusal, sink: &dyn Sink) -> Ending {
    sink.line("fault: writing torque off now; the head settles into near-stow on its own");
    let detail = match head.disengage_now(&mut |text| sink.line(text)) {
        Ok(()) => refusal.to_string(),
        Err(unacked) => format!("{refusal} — and the release reported: {unacked}"),
    };
    Ending::Faulted(stage, Refusal::new(detail))
}

/// The ending for a commissioning that refused: the machine was never taken.
///
/// Nothing was torqued, so nothing has to be released — the machine is already
/// at the minimum risk condition, which is where it was found. There is no
/// motion loop on this path, so the stop reason is here only to wake the bus
/// thread, and the ending has to be noted here too: the bus thread waits for one
/// before it closes the attachment the alert travels over, and no loop is coming
/// that would set it.
pub fn commission_failed(shared: &Shared, refusal: Refusal, sink: &dyn Sink) -> Outcome {
    let outcome = faulted(shared, FaultStage::Commission, refusal, sink);
    shared.request_stop(Stop::Detached);
    shared.end_motion();
    outcome
}

/// Record a fault and stop commanding, holding the port.
///
/// `machine` is taken by value and kept alive for the whole wait: dropping it
/// would close the port, and the port being held is what keeps a second speaker
/// off the bus while an operator decides what to do. Nothing is commanded here —
/// not a stow, not a re-engage — and the wait ends only when something asks the
/// daemon to stop, at which point it exits without commanding either. Torque is
/// already off: getting it there is what happened before this was called.
fn park<R>(
    machine: R,
    shared: &Shared,
    stage: FaultStage,
    refusal: Refusal,
    sink: &dyn Sink,
) -> Outcome {
    let outcome = faulted(shared, stage, refusal, sink);
    while shared.stopping().is_none() {
        thread::sleep(PARK_POLL);
    }
    sink.line("stopping a faulted daemon: nothing is commanded and torque is already off");
    drop(machine);
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
        "fault: {report}. commanding has stopped and torque is off: the machine is at the \
         minimum risk condition. an operator decides what happens next."
    ));
    Outcome::Faulted(report)
}

/// Whole milliseconds of `span`, for the capture.
fn millis(span: Duration) -> u64 {
    u64::try_from(span.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::rc::Rc;
    use std::sync::Arc;

    use motion_proto::{MotionScript, Step};
    use reachy_bench::pump::ReadFailures;
    use reachy_bus::IdOutcome;
    use reachy_motion::{
        CommandDisposition, Fault, JointId, RegId, SeqError, SeqStep, ServoHealth, StepContext,
    };

    use super::*;
    use crate::cells::Delivered;
    use crate::report::Collect;

    const POD: &str = "reachy00";
    /// Short enough that a test which really sleeps through one is not slow,
    /// long enough that a boundary cutting it is visible.
    const DWELL: Duration = Duration::from_millis(200);
    const REST_POLL: Duration = Duration::from_millis(1);
    const REST_DELAY: Duration = Duration::from_millis(30);
    /// Long enough that no test's script lapses while the test is running.
    const TIMEOUT_MS: u64 = 30_000;
    /// Where [`Event::Turn`]'s stow step sits. Short enough that an engage the
    /// fixture is told to take its time over outlasts it.
    const TURN_STOW_MS: u64 = 40;
    /// How many times one move may be replaced before the fixture calls the
    /// loop broken. No test asks the world to change more than once inside a
    /// move, so anything past that is the loop answering with the posture it is
    /// already carrying — which on a real machine is a head that creeps and
    /// never arrives, and here would be an endless test.
    const MAX_RETARGETS: usize = 4;

    fn timing() -> Timing {
        Timing {
            dwell: DWELL,
            rest_poll: REST_POLL,
            rest_delay: REST_DELAY,
        }
    }

    /// What the loop did to the machine, in order.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Act {
        /// A sweep of the resting watch.
        Watch,
        /// Torque on.
        Engage,
        Move(Posture),
        /// A move replaced while it was running, and the posture it became.
        Retarget(Posture),
        Hold,
        /// The orderly release: settle, measure, torque off.
        Release,
        /// The fault release: nine writes and nothing else.
        ReleaseNow,
    }

    /// A script asking for one posture from the moment it lands.
    fn holding(seq: u64, posture: Posture) -> MotionScript {
        MotionScript::new(POD, seq, vec![Step::new(0, posture)], TIMEOUT_MS)
            .expect("a lawful script")
    }

    /// What the world does to the daemon while it is watching the machine.
    ///
    /// One per sweep and one per dwell, applied at the top of each, which is
    /// where every change the daemon can observe arrives in life too: the bus
    /// thread writes these cells while this thread is looking at the machine.
    #[derive(Debug, Clone, Copy)]
    enum Event {
        /// A script arrives asking for the head up, with its whole timeout ahead
        /// of it.
        Raise,
        /// A script arrives whose timeout has already run out — what the loop
        /// sees when the scripter dies mid-conversation.
        Lapse,
        /// A script arrives asking for the head down.
        Lower,
        /// A script arrives carrying a whole turn: up as it lands, stow a short
        /// way in. The nominal shape, and the one where the timeline can move on
        /// while an engage is still on the wire.
        Turn,
        /// Something asks the daemon to stop.
        Stop(Stop),
        /// Nothing happens at all — a sweep or a dwell the world spends doing
        /// nothing, which is what a script's own timeline needs in order to run.
        Wait,
        /// Nothing happens, and the machine refuses this dwell.
        Refuse,
    }

    /// A machine that records what it was asked to do and refuses on demand.
    ///
    /// One struct for both halves of the typestate: the engaged form borrows
    /// this and writes into the same log, which is what makes the order of
    /// engage, move, hold and release assertable as one sequence.
    struct Fake {
        shared: Arc<Shared>,
        acts: Rc<RefCell<Vec<Act>>>,
        events: VecDeque<Event>,
        /// The next script's sequence number. Scripts are ordered by it and one
        /// at or below the last accepted is dropped, so a fixture that reused a
        /// number would silently stop delivering.
        seq: u64,
        /// Where the resting watch says the machine is standing.
        standing: Standing,
        /// The sweep, counted from zero, that refuses.
        refuse_watch: Option<usize>,
        /// The engage, counted from zero, that a torque-on gate refuses.
        gate_engage: Option<usize>,
        /// The engage, counted from zero, that fails outright.
        fault_engage: Option<usize>,
        /// The move, counted from zero, that refuses.
        refuse_move: Option<usize>,
        /// What the world does in the middle of a move, and which move.
        interrupt: Option<(usize, Event)>,
        /// Whether a refused move also asks the daemon to stop. On by default,
        /// because the ending a refusal produces parks and a parked thread waits
        /// for a stop; off is how the wait itself is observed.
        refusal_stops: bool,
        /// How long an engage takes. Zero by default: only the tests about what
        /// changes underneath an engage in flight need it to take any time.
        engage_takes: Duration,
        /// Whether the orderly release refuses.
        refuse_release: bool,
        /// Whether the orderly release measures the machine away from its fold.
        release_off_stow: bool,
        /// Whether the fault release reports an unacknowledged servo.
        unacked_release: bool,
        /// Whether a dwell really spends the time it was given. Off by default:
        /// almost every test is about the order of what was commanded, and
        /// sleeping through it would only make the suite slow.
        sleeps: bool,
        /// How long each dwell asked to watch for, in order.
        dwells: Rc<RefCell<Vec<Duration>>>,
        watches: usize,
        engages: usize,
        moves: usize,
        /// What the machine reports on each dwell, one entry per dwell. A dwell
        /// past the end of the script reports nothing.
        says: VecDeque<Vec<TickEvent>>,
        /// What it narrates on each move, one entry per move counted from the
        /// first. Text, because a move still narrates through the motion
        /// libraries' own rendering.
        says_moving: VecDeque<Vec<&'static str>>,
    }

    impl Fake {
        fn new(shared: &Arc<Shared>, events: impl IntoIterator<Item = Event>) -> Self {
            Self {
                shared: Arc::clone(shared),
                acts: Rc::new(RefCell::new(Vec::new())),
                events: events.into_iter().collect(),
                seq: 0,
                standing: Standing::AtStow,
                refuse_watch: None,
                gate_engage: None,
                fault_engage: None,
                refuse_move: None,
                interrupt: None,
                refusal_stops: true,
                engage_takes: Duration::ZERO,
                refuse_release: false,
                release_off_stow: false,
                unacked_release: false,
                sleeps: false,
                dwells: Rc::new(RefCell::new(Vec::new())),
                watches: 0,
                engages: 0,
                moves: 0,
                says: VecDeque::new(),
                says_moving: VecDeque::new(),
            }
        }

        /// A machine a crash or a hand left somewhere other than its fold.
        fn standing_elsewhere(mut self) -> Self {
            self.standing = Standing::Elsewhere;
            self
        }

        /// What the motion libraries report, dwell by dwell.
        fn saying(mut self, per_dwell: impl IntoIterator<Item = Vec<TickEvent>>) -> Self {
            self.says = per_dwell.into_iter().collect();
            self
        }

        /// The same for what they narrate while moving, move by move.
        fn saying_moving(mut self, per_move: impl IntoIterator<Item = Vec<&'static str>>) -> Self {
            self.says_moving = per_move.into_iter().collect();
            self
        }

        /// A refusal out of the nth move, which also stops the daemon: a parked
        /// thread waits for that, and a test that never sent one would wait with
        /// it.
        fn refusing_move(mut self, nth: usize) -> Self {
            self.refuse_move = Some(nth);
            self
        }

        /// `event` happening while the nth move is still travelling, counted
        /// from the first move. What a script landing mid-raise looks like from
        /// the loop's side.
        fn interrupting(mut self, nth: usize, event: Event) -> Self {
            self.interrupt = Some((nth, event));
            self
        }

        fn refusing_watch(mut self, nth: usize) -> Self {
            self.refuse_watch = Some(nth);
            self
        }

        /// A torque-on gate refusing the nth engage: nothing written, nothing to
        /// undo.
        fn gating_engage(mut self, nth: usize) -> Self {
            self.gate_engage = Some(nth);
            self
        }

        /// The nth engage failing for any other reason. The library takes the
        /// machine limp on its way out, so the fixture models an engage that
        /// leaves nothing holding.
        fn faulting_engage(mut self, nth: usize) -> Self {
            self.fault_engage = Some(nth);
            self
        }

        /// The same, leaving the daemon to be stopped by something else — which
        /// is what a fault does in life, and the only way the parked wait is
        /// observable.
        fn refusing_move_unstopped(mut self, nth: usize) -> Self {
            self.refuse_move = Some(nth);
            self.refusal_stops = false;
            self
        }

        /// An engage that takes real time, so the world can change between the
        /// request and the answer as it does on a sagging rail.
        fn engage_taking(mut self, span: Duration) -> Self {
            self.engage_takes = span;
            self
        }

        fn refusing_release(mut self) -> Self {
            self.refuse_release = true;
            self
        }

        /// A release whose measurement puts the head somewhere other than its
        /// fold. Torque still comes off; what changes is what is reported and
        /// where the next engage thinks the machine is.
        fn releasing_off_stow(mut self) -> Self {
            self.release_off_stow = true;
            self
        }

        /// A fault release that gets every write out and hears back from eight
        /// of the nine.
        fn unacked_release(mut self) -> Self {
            self.unacked_release = true;
            self
        }

        /// Let each dwell take the time the loop asked for, so a script's own
        /// timeline is what advances the run.
        fn sleeping(mut self) -> Self {
            self.sleeps = true;
            self
        }

        fn dwells(&self) -> Rc<RefCell<Vec<Duration>>> {
            Rc::clone(&self.dwells)
        }

        fn acts(&self) -> Rc<RefCell<Vec<Act>>> {
            Rc::clone(&self.acts)
        }

        fn record(&self, act: Act) {
            self.acts.borrow_mut().push(act);
        }

        /// The next script's number, strictly above every one before it.
        fn next_seq(&mut self) -> u64 {
            self.seq += 1;
            self.seq
        }

        /// The world's next move, applied to the shared cells.
        fn advance(&mut self) -> Option<Event> {
            let event = self.events.pop_front();
            self.apply(event);
            event
        }

        /// The same, for a change the world makes somewhere other than at the
        /// top of a sweep or a dwell — in the middle of a move, which is where
        /// a script that has to be spliced arrives.
        fn apply(&mut self, event: Option<Event>) {
            let now = Instant::now();
            match event {
                Some(Event::Raise) => {
                    let script = holding(self.next_seq(), Posture::Up);
                    self.shared.accept(&script, now);
                }
                Some(Event::Lapse) => {
                    let arrived = now
                        .checked_sub(Duration::from_secs(60))
                        .expect("a monotonic clock a minute past its start");
                    let script = holding(self.next_seq(), Posture::Up);
                    self.shared.accept(&script, arrived);
                }
                Some(Event::Lower) => {
                    let script = holding(self.next_seq(), Posture::Stow);
                    self.shared.accept(&script, now);
                }
                Some(Event::Turn) => {
                    let script = MotionScript::new(
                        POD,
                        self.next_seq(),
                        vec![
                            Step::new(0, Posture::Up),
                            Step::new(TURN_STOW_MS, Posture::Stow),
                        ],
                        TIMEOUT_MS,
                    )
                    .expect("a lawful script");
                    self.shared.accept(&script, now);
                }
                Some(Event::Stop(stop)) => {
                    self.shared.request_stop(stop);
                }
                Some(Event::Wait | Event::Refuse) | None => {}
            }
        }

        /// Nothing scripted behind this sweep or dwell means the test wrote a
        /// sequence the loop ran past; stopping is kinder than spinning.
        fn stop_if_spent(&mut self, event: Option<Event>) {
            if event.is_none() {
                self.shared.request_stop(Stop::Operator);
            }
        }
    }

    /// The engaged form: the same machine, holding torque.
    struct Held<'e> {
        machine: &'e mut Fake,
    }

    impl Rest for Fake {
        type Active<'e>
            = Held<'e>
        where
            Self: 'e;

        fn watch(&mut self, _line: &mut dyn FnMut(&str)) -> Result<Standing, Refusal> {
            let nth = self.watches;
            self.watches += 1;
            self.record(Act::Watch);
            if self.refuse_watch == Some(nth) {
                self.shared.request_stop(Stop::Operator);
                return Err(Refusal::new("servo 11: timed out"));
            }
            let event = self.advance();
            self.stop_if_spent(event);
            Ok(self.standing)
        }

        fn engage(&mut self, _line: &mut dyn FnMut(&str)) -> Result<Held<'_>, EngageFailed> {
            let nth = self.engages;
            self.engages += 1;
            thread::sleep(self.engage_takes);
            if self.gate_engage == Some(nth) {
                return Err(EngageFailed::Gate(Refusal::new(
                    "the supply is below the floor: 5.5 V against 6.0 V",
                )));
            }
            if self.fault_engage == Some(nth) {
                // As with a refused move: the ending parks, and a parked thread
                // waits to be stopped.
                self.shared.request_stop(Stop::Operator);
                return Err(EngageFailed::Fault(Refusal::new(
                    "servo 14: no answer to the enable",
                )));
            }
            self.record(Act::Engage);
            Ok(Held { machine: self })
        }
    }

    impl Active for Held<'_> {
        fn move_to(
            &mut self,
            posture: Posture,
            line: &mut dyn FnMut(&str),
            retarget: &mut dyn FnMut() -> Option<Posture>,
        ) -> Result<Posture, Refusal> {
            let nth = self.machine.moves;
            self.machine.moves += 1;
            let says = self.machine.says_moving.pop_front().unwrap_or_default();
            if self.machine.refuse_move == Some(nth) {
                if self.machine.refusal_stops {
                    self.machine.shared.request_stop(Stop::Operator);
                }
                return Err(Refusal::new("the tick faulted: envelope on path"));
            }
            self.machine.record(Act::Move(posture));
            for text in says {
                line(text);
            }
            // What the real move does over its control periods, compressed: the
            // world changes once if the test said so, and then the loop is asked
            // until it stops answering — which is what the machine does every
            // period until the head arrives.
            if let Some((at, event)) = self.machine.interrupt
                && at == nth
            {
                self.machine.interrupt = None;
                self.machine.apply(Some(event));
            }
            let mut arrived = posture;
            for _ in 0..MAX_RETARGETS {
                let Some(next) = retarget() else {
                    return Ok(arrived);
                };
                self.machine.record(Act::Retarget(next));
                arrived = next;
            }
            panic!(
                "the loop replaced one move {MAX_RETARGETS} times over: it is answering with the \
                 posture already in flight"
            );
        }

        fn hold(
            &mut self,
            dwell: Duration,
            event: &mut dyn FnMut(TickEvent),
        ) -> Result<(), Refusal> {
            self.machine.record(Act::Hold);
            self.machine.dwells.borrow_mut().push(dwell);
            if self.machine.sleeps {
                thread::sleep(dwell);
            }
            for reported in self.machine.says.pop_front().unwrap_or_default() {
                event(reported);
            }
            let next = self.machine.advance();
            self.machine.stop_if_spent(next);
            match next {
                Some(Event::Refuse) => {
                    self.machine.shared.request_stop(Stop::Operator);
                    Err(Refusal::new("servo 13: timed out"))
                }
                _ => Ok(()),
            }
        }

        fn disengage(self, _line: &mut dyn FnMut(&str)) -> Result<Verdict, Refusal> {
            self.machine.record(Act::Release);
            if self.machine.refuse_release {
                // As with a refused move: the ending parks, and a parked thread
                // waits to be stopped.
                self.machine.shared.request_stop(Stop::Operator);
                return Err(Refusal::new("servo 12 did not acknowledge torque off"));
            }
            if self.machine.release_off_stow {
                return Ok(Verdict {
                    at_stow: false,
                    worst_deg: 14.5,
                    unreadable: 0,
                });
            }
            Ok(Verdict {
                at_stow: true,
                worst_deg: 0.2,
                unreadable: 0,
            })
        }

        fn disengage_now(self, _line: &mut dyn FnMut(&str)) -> Result<(), Refusal> {
            self.machine.record(Act::ReleaseNow);
            if self.machine.unacked_release {
                return Err(Refusal::new("servo 12 did not acknowledge torque off"));
            }
            Ok(())
        }
    }

    /// Run the loop against `machine`, and hand back what it did and how it
    /// ended.
    fn drive(shared: &Shared, machine: Fake) -> (Outcome, Vec<Act>) {
        let (outcome, acts, _) = driven(shared, machine);
        (outcome, acts)
    }

    /// The same, keeping what the run said as well as what it did.
    fn driven(shared: &Shared, machine: Fake) -> (Outcome, Vec<Act>, Collect) {
        let acts = machine.acts();
        let sink = Collect::default();
        let outcome = run(machine, shared, timing(), &sink);
        let done = acts.borrow().clone();
        (outcome, done, sink)
    }

    /// The resting posture is folded with the motors unpowered, and a machine
    /// found there is left exactly as it is: nothing is commanded, nothing is
    /// torqued, and the daemon watches.
    #[test]
    fn a_machine_found_folded_is_left_limp() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Stop(Stop::Operator)]);

        let (outcome, acts) = drive(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert_eq!(
            acts,
            [Act::Watch],
            "a machine already at the minimum risk condition was touched"
        );
    }

    /// A machine a crash or a hand left standing is measured and folded, and
    /// then released. Never refused: where the head is, is not a question the
    /// daemon is allowed to have an opinion about.
    #[test]
    fn a_machine_found_standing_is_folded_and_let_go_of() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Stop(Stop::Operator)]).standing_elsewhere();

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert_eq!(
            acts,
            [
                Act::Watch,
                Act::Engage,
                Act::Move(Posture::Stow),
                Act::Release,
            ]
        );
        let fields = sink.fields("motion_startup").expect("the look is reported");
        assert_eq!(fields["at_stow"], json!(false));
    }

    /// The wake: a script asking for the head up engages the machine, raises it,
    /// and holds it there while that script is what is running.
    #[test]
    fn a_script_asking_for_the_head_up_engages_and_lifts_it() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [Event::Raise, Event::Wait, Event::Stop(Stop::Operator)],
        );

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert_eq!(
            acts,
            [
                // The startup look, which is where the script lands. Torque goes
                // on for the raise and comes off only on the way out.
                Act::Watch,
                Act::Engage,
                Act::Move(Posture::Up),
                Act::Hold,
                Act::Hold,
                Act::Move(Posture::Stow),
                Act::Release,
            ]
        );
        assert!(
            sink.fields("motion_engaged").is_some(),
            "every engage carries its wall clock"
        );
    }

    /// The whole point of the lifecycle: the head comes down, waits out the rest
    /// delay in case another turn follows, and then torque comes off — with the
    /// daemon carrying on rather than exiting.
    #[test]
    fn reaching_stow_rests_the_machine_and_the_daemon_carries_on() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [
                Event::Raise,
                Event::Lower,
                Event::Wait,
                Event::Wait,
                Event::Stop(Stop::Operator),
            ],
        )
        .sleeping();

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert_eq!(
            acts,
            [
                Act::Watch,
                Act::Engage,
                Act::Move(Posture::Up),
                Act::Hold,
                Act::Move(Posture::Stow),
                // The rest delay, then torque off — and back to watching a limp
                // machine rather than exiting.
                Act::Hold,
                Act::Release,
                Act::Watch,
                Act::Watch,
            ]
        );
        // `fields` answers with the first release, which is this one: a stow the
        // script asked for, told apart in a capture from a scripter that died.
        let fields = sink
            .fields("motion_released")
            .expect("the release is captured");
        assert_eq!(fields["reason"], json!("script"));
    }

    /// A script that lands while the head is still on its way up turns that
    /// move around, rather than being served after it finishes.
    ///
    /// A raise takes seconds; without mid-move retargeting an instruction
    /// arriving mid-raise delays the fold by a whole move. There is no second
    /// `Move` here — the fold *is* the raise, redirected part way.
    #[test]
    fn a_script_landing_mid_raise_turns_that_move_around() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [Event::Raise, Event::Wait, Event::Stop(Stop::Operator)],
        )
        .interrupting(0, Event::Lower);

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert_eq!(
            acts,
            [
                Act::Watch,
                Act::Engage,
                Act::Move(Posture::Up),
                Act::Retarget(Posture::Stow),
                // Already folded when the move returned, so the rest delay
                // starts here and no fold is commanded on the way out.
                Act::Hold,
                Act::Hold,
                Act::Release,
            ]
        );
        let moves = sink.all_fields("motion_move");
        assert_eq!(
            moves.len(),
            2,
            "the splice is a move of its own in the capture: {moves:?}"
        );
        assert_eq!(moves[1]["from"], json!("up"));
        assert_eq!(moves[1]["to"], json!("stow"));
        assert_eq!(moves[1]["reason"], json!("script"));
    }

    /// A stop arriving mid-raise folds the head from where it got to, and the
    /// shutdown path finds nothing left to command.
    ///
    /// `TimeoutStopSec` bounds a `systemctl stop`, and a daemon that finished
    /// raising the head before it started lowering it would spend two moves of
    /// that budget to end where one move gets it.
    #[test]
    fn a_stop_mid_raise_folds_the_head_without_finishing_the_raise() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Raise, Event::Wait])
            .interrupting(0, Event::Stop(Stop::Operator));

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert_eq!(
            acts,
            [
                Act::Watch,
                Act::Engage,
                Act::Move(Posture::Up),
                Act::Retarget(Posture::Stow),
                Act::Release,
            ]
        );
        let moves = sink.all_fields("motion_move");
        assert_eq!(moves.len(), 2, "{moves:?}");
        assert_eq!(moves[1]["reason"], json!("shutdown"));
    }

    /// A script landing during the shutdown fold does not turn the head back up.
    ///
    /// The fold is the last thing the daemon owes the machine, and torque comes
    /// off at the end of it. A raise accepted there would report `stow` for a
    /// head that is standing, and then let go of it — a fall, narrated as an
    /// orderly release.
    #[test]
    fn a_script_landing_during_the_shutdown_fold_is_not_served() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Raise, Event::Wait]).interrupting(1, Event::Raise);

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert_eq!(
            acts,
            [
                Act::Watch,
                Act::Engage,
                Act::Move(Posture::Up),
                Act::Hold,
                Act::Hold,
                // Move 1: the shutdown fold, with a raise landing inside it.
                Act::Move(Posture::Stow),
                Act::Release,
            ]
        );
        assert!(
            !acts.iter().any(|act| matches!(act, Act::Retarget(_))),
            "the fold was diverted: {acts:?}"
        );
        let reached = sink.all_fields("motion_posture");
        assert_eq!(
            reached.last().expect("the fold arrived")["state"],
            json!("stow"),
            "the last thing reported is where the machine was let go: {reached:?}"
        );
    }

    /// The same for the startup fold: a machine found standing is measured and
    /// folded as one sequence, and a script arriving mid-fold waits for the loop
    /// that sequence returns into.
    ///
    /// The pose here is one nobody has ever commanded — a crash, or a hand — so
    /// there is no setpoint worth splicing a new trajectory onto. A raise
    /// accepted here would also leave `normalise` reporting a fold it did not
    /// make, and seeding the loop's posture bookkeeping with it.
    #[test]
    fn a_script_landing_during_the_startup_fold_is_not_served() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [Event::Wait, Event::Wait, Event::Stop(Stop::Operator)],
        )
        .standing_elsewhere()
        .interrupting(0, Event::Raise);

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert_eq!(
            acts,
            [
                Act::Watch,
                Act::Engage,
                // Move 0: the normalising fold, with a raise landing inside it.
                Act::Move(Posture::Stow),
                Act::Release,
                Act::Engage,
                Act::Move(Posture::Up),
                Act::Hold,
                Act::Hold,
                Act::Move(Posture::Stow),
                Act::Release,
            ]
        );
        assert!(
            !acts.iter().any(|act| matches!(act, Act::Retarget(_))),
            "nothing was spliced: {acts:?}"
        );
        let startup = sink
            .fields("motion_startup")
            .expect("the startup verdict is captured");
        assert_eq!(startup["at_stow"], json!(false));
    }

    /// The other direction at loop level: a wake landing inside a fold the loop
    /// commanded turns the head back up without waiting the fold out.
    ///
    /// The fold `active` commands is the steerable one — by then the loop is the
    /// executor and the pose is one it commanded — and a follow-up question
    /// arriving as the head starts down is the ordinary case for it.
    #[test]
    fn a_wake_landing_mid_fold_turns_the_head_back_up() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [Event::Raise, Event::Lower, Event::Stop(Stop::Operator)],
        )
        .interrupting(1, Event::Raise);

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert_eq!(
            acts,
            [
                Act::Watch,
                Act::Engage,
                Act::Move(Posture::Up),
                Act::Hold,
                Act::Move(Posture::Stow),
                Act::Retarget(Posture::Up),
                Act::Hold,
                // The stop, and the fold it takes from a head that is up again.
                Act::Move(Posture::Stow),
                Act::Release,
            ]
        );
        let moves = sink.all_fields("motion_move");
        assert_eq!(moves[2]["from"], json!("stow"));
        assert_eq!(moves[2]["to"], json!("up"));
        assert_eq!(moves[2]["reason"], json!("script"));
    }

    /// The loop never answers a move with the posture that move is already
    /// carrying, and a stop outranks the schedule.
    ///
    /// Asked once per control period, so re-commanding the posture in flight
    /// would shape a fresh trajectory from the setpoint fifty times a second:
    /// the head would creep and never arrive. The stop's precedence is what
    /// makes the shutdown fold start where the machine is rather than after the
    /// raise it interrupts.
    #[test]
    fn a_move_is_never_replaced_by_the_posture_it_is_already_carrying() {
        let shared = Shared::new(POD);
        let sink = Collect::default();
        let mut watch = Watch::default();
        shared.accept(&holding(1, Posture::Up), Instant::now());

        assert_eq!(retarget_to(&shared, &mut watch, &sink, Posture::Up), None);
        assert_eq!(
            retarget_to(&shared, &mut watch, &sink, Posture::Stow),
            Some((Posture::Up, "script"))
        );

        shared.request_stop(Stop::Operator);
        assert_eq!(
            retarget_to(&shared, &mut watch, &sink, Posture::Up),
            Some((Posture::Stow, "shutdown")),
            "a stop is answered with the fold, whatever the script still says"
        );
        assert_eq!(retarget_to(&shared, &mut watch, &sink, Posture::Stow), None);
    }

    /// A wake inside the rest delay retargets the head up with no release and no
    /// engage in between. The quick follow-up case, and the reason the rest
    /// delay exists at all.
    #[test]
    fn a_wake_inside_the_rest_delay_retargets_without_letting_go() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [
                Event::Raise,
                Event::Lower,
                Event::Raise,
                Event::Stop(Stop::Operator),
            ],
        );

        let (outcome, acts) = drive(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert_eq!(
            acts,
            [
                Act::Watch,
                Act::Engage,
                Act::Move(Posture::Up),
                Act::Hold,
                Act::Move(Posture::Stow),
                // The wake lands on this dwell, inside the rest delay: the head
                // goes back up with no release and no engage between the two.
                Act::Hold,
                Act::Move(Posture::Up),
                Act::Hold,
                Act::Move(Posture::Stow),
                Act::Release,
            ]
        );
        assert_eq!(
            acts.iter().filter(|act| **act == Act::Engage).count(),
            1,
            "the head was let go of and taken hold of again inside the window"
        );
    }

    /// The scripter stops refreshing and the timeout runs out. Nothing said
    /// stow; the end of instruction is what stows, and it is said once rather
    /// than at every dwell that meets the same lapsed script.
    #[test]
    fn a_lapsed_script_stows_the_head_and_rests_it() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [
                Event::Raise,
                Event::Lapse,
                Event::Wait,
                Event::Stop(Stop::Operator),
            ],
        );

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        let said = sink.said();
        assert_eq!(
            said.events
                .iter()
                .filter(|(name, _)| name == "motion_script_expired")
                .count(),
            1,
            "a lapse is one line, not one per dwell: {:?}",
            said.events
        );
        assert_eq!(
            acts,
            [
                Act::Watch,
                Act::Engage,
                Act::Move(Posture::Up),
                Act::Hold,
                // The lapse: nothing said stow, the timeout did.
                Act::Move(Posture::Stow),
                Act::Hold,
                Act::Hold,
                Act::Release,
            ]
        );
    }

    /// A lapse that runs all the way to the release: the reason on the released
    /// event is what tells a scripted stow from a scripter that died, and it is
    /// the only thing in a capture that does.
    #[test]
    fn a_release_after_a_lapse_is_captured_as_a_timeout() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [
                Event::Raise,
                Event::Lapse,
                Event::Wait,
                Event::Wait,
                Event::Wait,
            ],
        )
        .sleeping();

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert!(
            acts.contains(&Act::Release),
            "the rest delay never elapsed: {acts:?}"
        );
        let fields = sink
            .fields("motion_released")
            .expect("the release is captured");
        assert_eq!(fields["reason"], json!("timeout"));
    }

    /// The reported defect's shape, and the whole point of a script: one
    /// message carries the raise *and* the stow, and the stow happens at the
    /// offset it named with nothing else arriving in between. The dwells are
    /// real here, so it is the script's own timeline that advances the run.
    #[test]
    fn a_timed_step_executes_on_the_scripts_own_clock_with_no_further_message() {
        let shared = Arc::new(Shared::new(POD));
        let whole_turn = MotionScript::new(
            POD,
            1,
            vec![Step::new(0, Posture::Up), Step::new(60, Posture::Stow)],
            TIMEOUT_MS,
        )
        .expect("a lawful script");
        shared.accept(&whole_turn, Instant::now());
        let machine = Fake::new(
            &shared,
            [Event::Wait, Event::Wait, Event::Stop(Stop::Operator)],
        )
        .sleeping();
        let dwells = machine.dwells();

        let (outcome, acts) = drive(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert_eq!(
            acts,
            [
                Act::Watch,
                Act::Engage,
                Act::Move(Posture::Up),
                Act::Hold,
                Act::Move(Posture::Stow),
                Act::Hold,
                Act::Release,
            ]
        );
        let asked = dwells.borrow().clone();
        assert!(
            asked[0] < DWELL,
            "the dwell ran past the step it was supposed to stop at: {asked:?}"
        );
    }

    /// A dwell is a ceiling cut by the script's own next boundary and by the end
    /// of the rest delay, and floored so a boundary that has just gone by cannot
    /// spin the loop through empty runs.
    #[test]
    fn a_dwell_is_cut_to_the_next_boundary_and_floored() {
        let shared = Shared::new(POD);
        assert_eq!(
            dwell_for(&shared, DWELL, None),
            DWELL,
            "nothing is scheduled, so the ceiling is the whole answer"
        );

        let now = Instant::now();
        let soon = MotionScript::new(
            POD,
            1,
            vec![Step::new(0, Posture::Up), Step::new(80, Posture::Stow)],
            TIMEOUT_MS,
        )
        .expect("a lawful script");
        shared.accept(&soon, now);
        let cut = dwell_for(&shared, DWELL, None);
        assert!(cut < DWELL && cut > MIN_DWELL, "{cut:?}");

        // The rest delay ending sooner than the script's next boundary cuts it
        // further: the release is due then, not at the boundary.
        let sooner = dwell_for(&shared, DWELL, Some(now + Duration::from_millis(40)));
        assert!(sooner < cut, "{sooner:?} against {cut:?}");

        // A step that came due while the last move was running: the boundary is
        // already behind, and the answer is still a dwell somebody can hold.
        let overdue = MotionScript::new(POD, 2, vec![Step::new(1, Posture::Stow)], TIMEOUT_MS)
            .expect("a lawful script");
        shared.accept(&overdue, now);
        assert_eq!(dwell_for(&shared, DWELL, None), MIN_DWELL);
    }

    /// A head that is up when the stop arrives is folded first, and only then
    /// released. The fold is commanded while the machine still has torque,
    /// because a head released where it stands is a head that falls the rest of
    /// the way.
    #[test]
    fn a_stop_folds_the_head_before_it_releases() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Raise, Event::Stop(Stop::Operator)]);

        let (outcome, acts) = drive(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert_eq!(
            acts,
            [
                Act::Watch,
                Act::Engage,
                Act::Move(Posture::Up),
                Act::Hold,
                Act::Move(Posture::Stow),
                Act::Release,
            ]
        );
    }

    /// The bridge gives up. The head still comes down and torque still comes
    /// off — nothing about a lost script source is a reason to leave a machine
    /// torqued — and the run ends nonzero so a supervisor can tell it from a
    /// clean stop.
    #[test]
    fn a_lost_script_source_folds_the_head_and_releases_it() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Raise, Event::Stop(Stop::Detached)]);

        let (outcome, acts) = drive(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Detached));
        assert!(!outcome.is_clean());
        assert_eq!(acts.last(), Some(&Act::Release));
    }

    /// A stop that arrives while the machine is resting commands nothing at all:
    /// it is already limp and folded, which is where every ending is trying to
    /// get it.
    #[test]
    fn a_stop_while_resting_touches_nothing() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Stop(Stop::Operator), Event::Wait]);

        let (outcome, acts) = drive(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert!(
            !acts.contains(&Act::Engage) && !acts.contains(&Act::Release),
            "a resting machine was touched on the way out: {acts:?}"
        );
    }

    /// The single most important test in this design: a fault takes torque off
    /// *now*, with no stow attempt in front of it, and only then parks.
    #[test]
    fn a_fault_mid_move_writes_torque_off_before_it_parks() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Raise, Event::Wait]).refusing_move(0);

        let (outcome, acts) = drive(&shared, machine);

        let Outcome::Faulted(report) = outcome else {
            panic!("the move up refused");
        };
        assert_eq!(report.stage, FaultStage::Motion);
        assert!(report.detail.contains("envelope on path"), "{report}");
        assert_eq!(
            acts,
            [Act::Watch, Act::Engage, Act::ReleaseNow],
            "the fault ending is the nine writes and nothing else",
        );
        assert_eq!(shared.fault(), Some(&report));
    }

    /// A dwell that refuses is where an unattended machine's faults are found,
    /// and finding one takes the machine limp rather than starting a recovery.
    #[test]
    fn a_refused_dwell_takes_torque_off_and_parks() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Raise, Event::Refuse]);

        let (outcome, acts) = drive(&shared, machine);

        let Outcome::Faulted(report) = outcome else {
            panic!("the dwell refused");
        };
        assert_eq!(report.stage, FaultStage::Motion);
        assert!(report.detail.contains("servo 13"), "{report}");
        assert_eq!(acts.last(), Some(&Act::ReleaseNow));
    }

    /// A servo that never acknowledged its torque-off is named in the fault
    /// report. It is the one fact that decides whether a hand can go on the
    /// head, so it cannot be left to the terminal.
    #[test]
    fn an_unacknowledged_release_is_carried_into_the_fault_report() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Raise, Event::Refuse]).unacked_release();

        let (outcome, _) = drive(&shared, machine);

        let Outcome::Faulted(report) = outcome else {
            panic!("the dwell refused");
        };
        assert!(report.detail.contains("servo 13"), "{report}");
        assert!(
            report.detail.contains("servo 12 did not acknowledge"),
            "{report}"
        );
    }

    /// A torque-on gate refusing is an expected error, not a fault: nothing was
    /// written, the machine goes on resting, and the daemon does not park. The
    /// refusal is alerted on and the *next* script tries again — not the next
    /// sweep, which would be ten refusals a second.
    #[test]
    fn a_refused_engage_leaves_the_machine_resting_and_alerts() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [
                Event::Raise,
                Event::Wait,
                Event::Wait,
                Event::Stop(Stop::Operator),
            ],
        )
        .gating_engage(0);

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert!(!acts.contains(&Act::Engage), "{acts:?}");
        assert!(!acts.contains(&Act::Move(Posture::Up)), "{acts:?}");
        assert_eq!(shared.fault(), None, "a gate refusal is not a fault");
        let (detail, count) = shared
            .take_engage_refusal()
            .expect("a refused engage owes an alert");
        assert!(detail.contains("below the floor"), "{detail}");
        assert_eq!(count, 1, "one refusal per script, not one per sweep");
        let fields = sink
            .fields("motion_engage_refused")
            .expect("the refusal is captured");
        assert_eq!(fields["seq"], json!(1));
    }

    /// An engage that fails for any other reason has already left the machine
    /// limp inside the library, so what is left is a fault to report and a
    /// daemon to park.
    #[test]
    fn an_engage_that_fails_outright_faults() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Raise, Event::Wait]).faulting_engage(0);

        let (outcome, acts) = drive(&shared, machine);

        let Outcome::Faulted(report) = outcome else {
            panic!("the engage failed");
        };
        assert_eq!(report.stage, FaultStage::Engage);
        assert!(!acts.contains(&Act::ReleaseNow), "{acts:?}");
    }

    /// The resting watch is the only thing looking at an idle machine, so a
    /// sweep that stops answering is a fault. The machine is already limp; what
    /// the daemon does about it is stop and say so.
    #[test]
    fn a_sweep_that_stops_answering_faults() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Wait, Event::Wait]).refusing_watch(1);

        let (outcome, _) = drive(&shared, machine);

        let Outcome::Faulted(report) = outcome else {
            panic!("the sweep refused");
        };
        assert_eq!(report.stage, FaultStage::Resting);
    }

    /// A release that cannot report itself complete still happened — the
    /// library falls through to the immediate form rather than leaving a machine
    /// half-released — so the daemon reports it and ends unclean.
    #[test]
    fn a_release_that_reports_trouble_ends_unclean() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Stop(Stop::Operator)])
            .standing_elsewhere()
            .refusing_release();

        let (outcome, acts) = drive(&shared, machine);

        assert!(!outcome.is_clean());
        let Outcome::Faulted(report) = outcome else {
            panic!("the release reported trouble");
        };
        assert_eq!(report.stage, FaultStage::Startup);
        assert_eq!(acts.last(), Some(&Act::Release));
    }

    /// Scripts that arrive after a fault change nothing: the schedule stops
    /// taking them, so the timeline the daemon stopped commanding on is the last
    /// one there is and nothing later can move it.
    #[test]
    fn scripts_after_a_fault_do_not_move_the_schedule() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Raise, Event::Refuse]);

        let (outcome, _) = drive(&shared, machine);

        assert!(matches!(outcome, Outcome::Faulted(_)));
        let running = shared.accepted_seq();
        assert_eq!(
            shared.accept(&holding(9, Posture::Up), Instant::now()),
            Delivered::Faulted
        );
        assert_eq!(
            shared.accepted_seq(),
            running,
            "a script offered to a faulted daemon reached the schedule"
        );
    }

    /// Every ending says so, whatever it was. The bus thread holds the
    /// attachment open until this is set, so a run that ended without setting it
    /// would be a daemon that never shuts its bus side down.
    #[test]
    fn every_ending_notes_that_the_machine_is_no_longer_being_touched() {
        for machine in [
            Fake::new(&Arc::new(Shared::new(POD)), [Event::Stop(Stop::Operator)]),
            Fake::new(&Arc::new(Shared::new(POD)), [Event::Stop(Stop::Detached)]),
            Fake::new(&Arc::new(Shared::new(POD)), [Event::Raise, Event::Refuse]),
        ] {
            let shared = Arc::clone(&machine.shared);
            let (outcome, _) = drive(&shared, machine);
            assert!(shared.motion_ended(), "{outcome} did not say it had ended");
        }
    }

    /// What the machine did reaches the capture, not only what it was asked for:
    /// a move's start carries where it is going and why, and its landing is a
    /// line of its own so the pair bounds the move.
    #[test]
    fn each_move_is_bounded_by_two_lines_in_the_capture() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Raise, Event::Stop(Stop::Operator)]);

        let (_, _, sink) = driven(&shared, machine);

        let moves: Vec<_> = sink
            .said()
            .events
            .into_iter()
            .filter(|(name, _)| name == "motion_move" || name == "motion_posture")
            .collect();
        let names: Vec<&str> = moves.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(
            names,
            [
                "motion_move",
                "motion_posture",
                "motion_move",
                "motion_posture",
            ],
            "{moves:?}"
        );
        assert_eq!(moves[0].1["from"], json!("stow"));
        assert_eq!(moves[0].1["to"], json!("up"));
        assert_eq!(moves[0].1["reason"], json!("script"));
        assert_eq!(moves[2].1["reason"], json!("shutdown"));
        let released = sink
            .fields("motion_released")
            .expect("the release is captured");
        assert_eq!(released["reason"], json!("shutdown"));
        assert_eq!(released["at"], json!("stow"));
    }

    /// The fault is written to the capture by the thread that took it. Left to
    /// the bus thread's poll it would be late at best, and on the shutdown path
    /// it would be written after the reader had gone.
    #[test]
    fn a_fault_reaches_the_capture_from_the_thread_that_took_it() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Raise, Event::Refuse]);

        let (_, _, sink) = driven(&shared, machine);

        let fields = sink.fields("motion_fault").expect("the fault is reported");
        assert_eq!(fields["stage"], json!("the motion loop"));
        assert!(
            fields["detail"].as_str().is_some_and(|d| d.contains("13")),
            "{fields}"
        );
    }

    /// The other half of the one-refusal-per-script rule: the *next* script
    /// clears the suppression and the engage is tried again.
    ///
    /// Without this the suite cannot tell "one wake lost to a sagging rail" from
    /// "deaf for the rest of the process" — both produce a run with no engage in
    /// it, and the second one still reports a clean stop.
    #[test]
    fn a_refused_engage_is_retried_by_the_next_script() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [
                Event::Raise,
                Event::Wait,
                Event::Raise,
                Event::Wait,
                Event::Stop(Stop::Operator),
            ],
        )
        .gating_engage(0);

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        let engaged = acts
            .iter()
            .position(|act| *act == Act::Engage)
            .expect("the second script's engage was tried");
        assert!(
            acts[engaged..].contains(&Act::Move(Posture::Up)),
            "the head never came up for the script that followed the refusal: {acts:?}"
        );
        assert_eq!(
            sink.said()
                .events
                .iter()
                .filter(|(name, _)| name == "motion_engage_refused")
                .count(),
            1,
            "one refusal per script, not one per resting sweep"
        );
        let (_, count) = shared
            .take_engage_refusal()
            .expect("the refusal owes an alert");
        assert_eq!(count, 1);
    }

    /// A gate refusal is recorded against the script it refused, not against
    /// whatever landed while the engage was on the wire.
    ///
    /// An engage takes tens of milliseconds and the bus thread writes the
    /// schedule the whole time. Reading the number afterwards blames the new
    /// script for the old one's refusal, and the suppression then swallows the
    /// raise it was asking for — a turn where the head never acknowledges the
    /// wake, in exactly the window a barge or a fast follow-up lands in.
    #[test]
    fn a_gate_refusal_is_recorded_against_the_script_it_refused() {
        let shared = Arc::new(Shared::new(POD));
        shared.accept(&holding(1, Posture::Up), Instant::now());
        let machine = Fake::new(
            &shared,
            [Event::Wait, Event::Wait, Event::Stop(Stop::Operator)],
        )
        .gating_engage(0)
        .engage_taking(Duration::from_millis(120));

        let racing = Arc::clone(&shared);
        let lands_mid_engage = thread::spawn(move || {
            thread::sleep(Duration::from_millis(40));
            racing.accept(&holding(5, Posture::Up), Instant::now());
        });

        let (outcome, acts, sink) = driven(&shared, machine);
        lands_mid_engage.join().expect("the racing script lands");

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        let fields = sink
            .fields("motion_engage_refused")
            .expect("the refusal is captured");
        assert_eq!(
            fields["seq"],
            json!(1),
            "the refusal was blamed on a script that never reached an engage"
        );
        assert!(
            acts.contains(&Act::Move(Posture::Up)),
            "the script that landed mid-engage never got its raise: {acts:?}"
        );
    }

    /// A boot into a sagging rail, on a machine a crash left standing. Nothing
    /// was written, so the head is limp exactly where it was and the daemon has
    /// no business parking over it: it rests, and the next script asks again.
    #[test]
    fn a_startup_gate_refusal_leaves_a_crooked_machine_resting() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [
                Event::Wait,
                Event::Raise,
                Event::Wait,
                Event::Stop(Stop::Operator),
            ],
        )
        .standing_elsewhere()
        .gating_engage(0);

        let (outcome, acts) = drive(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert_eq!(shared.fault(), None, "a gate refusal is not a fault");
        let engaged = acts
            .iter()
            .position(|act| *act == Act::Engage)
            .expect("a later script still engages");
        assert!(
            !acts[..engaged].contains(&Act::Move(Posture::Stow)),
            "the startup fold was commanded on a machine that never took torque: {acts:?}"
        );
        assert!(
            acts[..engaged]
                .iter()
                .filter(|act| **act == Act::Watch)
                .count()
                >= 2,
            "the run did not go on to the resting watch: {acts:?}"
        );
    }

    /// The head is crooked and the script has moved on to its stow by the time
    /// torque is on. The fold has to be commanded: engaging pins the machine
    /// where it stands, and a head released from there falls the rest of the way.
    #[test]
    fn a_crooked_machine_is_folded_even_when_the_script_already_says_stow() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [
                Event::Wait,
                Event::Turn,
                Event::Wait,
                Event::Stop(Stop::Operator),
            ],
        )
        // The startup fold is refused, so the head stays standing and limp.
        .standing_elsewhere()
        .gating_engage(0)
        // The engage outlasts the turn's stow step, so the first thing the loop
        // is asked for once torque is on is the posture it would have assumed
        // the machine was already in.
        .engage_taking(Duration::from_millis(TURN_STOW_MS * 3));

        let (outcome, acts) = drive(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        let engaged = acts
            .iter()
            .position(|act| *act == Act::Engage)
            .expect("the turn engages the machine");
        assert_eq!(
            acts.get(engaged + 1),
            Some(&Act::Move(Posture::Stow)),
            "a head standing where a crash left it was released without being folded: {acts:?}"
        );
    }

    /// The routine end-of-conversation release refusing — the release that runs
    /// at the end of every turn. It is a fault at the release stage, and the
    /// daemon parks rather than reporting a clean stop over it.
    #[test]
    fn a_refused_release_at_the_end_of_a_turn_faults_at_the_release_stage() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [Event::Raise, Event::Lower, Event::Wait, Event::Wait],
        )
        .sleeping()
        .refusing_release();

        let (outcome, acts) = drive(&shared, machine);

        let Outcome::Faulted(report) = outcome else {
            panic!("the release refused");
        };
        assert_eq!(report.stage, FaultStage::Release);
        assert_eq!(acts.last(), Some(&Act::Release));
    }

    /// A stop with the head up, and the fold on the way out refusing. The
    /// doctrine's fall-through on the path every SIGTERM takes: a refused stow
    /// still ends in torque off, not in an error return over a torqued machine
    /// with no loop driving it.
    #[test]
    fn a_refused_shutdown_fold_still_takes_torque_off() {
        let shared = Arc::new(Shared::new(POD));
        // The second move is the shutdown fold; the first is the raise.
        let machine =
            Fake::new(&shared, [Event::Raise, Event::Stop(Stop::Operator)]).refusing_move(1);

        let (outcome, acts) = drive(&shared, machine);

        let Outcome::Faulted(report) = outcome else {
            panic!("the shutdown fold refused");
        };
        assert_eq!(report.stage, FaultStage::Shutdown);
        assert_eq!(
            acts.last(),
            Some(&Act::ReleaseNow),
            "a refused shutdown fold left the machine torqued: {acts:?}"
        );
    }

    /// The same ending, with the fold landing and the release itself refusing.
    /// Still a shutdown-stage fault, and the release was still attempted.
    #[test]
    fn a_refused_shutdown_release_faults_at_the_shutdown_stage() {
        let shared = Arc::new(Shared::new(POD));
        let machine =
            Fake::new(&shared, [Event::Raise, Event::Stop(Stop::Operator)]).refusing_release();

        let (outcome, acts) = drive(&shared, machine);

        let Outcome::Faulted(report) = outcome else {
            panic!("the shutdown release refused");
        };
        assert_eq!(report.stage, FaultStage::Shutdown);
        assert_eq!(acts.last(), Some(&Act::Release));
    }

    /// The startup fold refusing, which is the other move `refusing_move(0)` can
    /// name: a machine found standing, and the stow out of it faulting.
    #[test]
    fn a_refused_startup_fold_takes_torque_off_at_the_startup_stage() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Wait])
            .standing_elsewhere()
            .refusing_move(0);

        let (outcome, acts) = drive(&shared, machine);

        let Outcome::Faulted(report) = outcome else {
            panic!("the startup fold refused");
        };
        assert_eq!(report.stage, FaultStage::Startup);
        assert_eq!(acts, [Act::Watch, Act::Engage, Act::ReleaseNow]);
    }

    /// A parked daemon holds the port until something stops it.
    ///
    /// Holding it is what keeps a second speaker off a half-duplex chain while
    /// an operator decides what to do, and it is also the window the bus thread
    /// needs to get the fault alert out. Every other fault fixture hands the loop
    /// a stop before the refusal, so this is the only test that can tell `park`
    /// from a function that returns.
    #[test]
    fn a_parked_daemon_waits_to_be_stopped_before_it_lets_the_port_go() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Raise, Event::Wait]).refusing_move_unstopped(0);

        let waited = Duration::from_millis(250);
        let stopper = Arc::clone(&shared);
        let releases_it = thread::spawn(move || {
            thread::sleep(waited);
            stopper.request_stop(Stop::Operator);
        });

        let began = Instant::now();
        let (outcome, acts) = drive(&shared, machine);
        let took = began.elapsed();
        releases_it.join().expect("the stop is sent");

        assert!(
            matches!(outcome, Outcome::Faulted(_)),
            "the move refused: {outcome}"
        );
        assert!(
            took >= waited - PARK_POLL,
            "the parked daemon let the port go without being stopped: {took:?}"
        );
        assert_eq!(
            acts.last(),
            Some(&Act::ReleaseNow),
            "a parked daemon commanded something: {acts:?}"
        );
    }

    /// The release measured the head away from its fold. Torque still comes off —
    /// nothing gates that — but the capture says where the machine actually is,
    /// and an operator is told, because this is the state a hand might go near.
    #[test]
    fn a_release_that_misses_the_fold_reports_it_and_alerts() {
        let shared = Arc::new(Shared::new(POD));
        let machine =
            Fake::new(&shared, [Event::Raise, Event::Stop(Stop::Operator)]).releasing_off_stow();

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert_eq!(acts.last(), Some(&Act::Release), "torque came off anyway");
        let fields = sink
            .fields("motion_released")
            .expect("the release is captured");
        assert_eq!(
            fields["at"],
            json!("elsewhere"),
            "the capture claimed the head was folded: {fields}"
        );
        assert_eq!(fields["worst_deg"], json!(14.5));
        let (detail, count) = shared
            .take_stow_miss()
            .expect("a release away from stow owes an alert");
        assert!(detail.contains("away from stow"), "{detail}");
        assert_eq!(count, 1);
    }

    /// The ordinary release says where it found the machine, and says nothing to
    /// an operator about it: a fold that landed is not news.
    #[test]
    fn a_release_that_finds_the_fold_says_so_and_alerts_nobody() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Raise, Event::Stop(Stop::Operator)]);

        let (_, _, sink) = driven(&shared, machine);

        let fields = sink
            .fields("motion_released")
            .expect("the release is captured");
        assert_eq!(fields["at"], json!("stow"));
        assert_eq!(shared.take_stow_miss(), None);
    }

    /// The supply and the error bits are what the two torque-on gates read, so
    /// how stale they may be is this daemon's policy and not a property of a
    /// sweep. Never due after the first is a gate judging every engage on
    /// commissioning-time numbers; due every sweep is the resting watch becoming
    /// most of the traffic on the wire.
    #[test]
    fn the_rail_is_re_read_on_its_own_cadence_and_not_every_sweep() {
        let now = Instant::now();
        let every = Duration::from_millis(500);

        assert!(
            rail_due(None, now, every),
            "the first sweep since commissioning has no rail reading to carry forward"
        );
        assert!(!rail_due(
            Some(now),
            now + Duration::from_millis(499),
            every
        ));
        assert!(rail_due(Some(now), now + every, every));
        assert!(rail_due(Some(now), now + Duration::from_secs(3), every));
    }

    /// The health-poll rate becomes a period by division, so a zero rate is the
    /// one configuration between this and a panic on the thread that owns the
    /// port. The bench configuration refuses zero; this is what happens if one
    /// ever reaches here anyway.
    #[test]
    fn a_health_poll_rate_of_zero_still_yields_a_period() {
        assert_eq!(rail_period(10), Duration::from_millis(100));
        assert_eq!(rail_period(1), Duration::from_secs(1));
        assert_eq!(rail_period(0), Duration::from_secs(1));
    }

    /// The classification the whole unattended lifecycle turns on. A servo that
    /// stopped answering *during* an engage is a fault, not a gate refusal: the
    /// machine may be part way through taking torque, and "nothing was written"
    /// would be a lie about it.
    #[test]
    fn only_a_torque_on_gate_makes_an_engage_failure_expected() {
        let context = StepContext::reg(SeqStep::PinAndEnable, 13, RegId::TorqueEnable);
        let mid_flight = EngageFailed::from(PumpError::Sequence(SeqError::NoAnswer { context }));
        assert!(
            matches!(mid_flight, EngageFailed::Fault(_)),
            "{mid_flight:?}"
        );

        let gate = EngageFailed::from(PumpError::Sequence(SeqError::SupplyBelowFloor {
            context,
            readings: [5.5; JointId::COUNT],
            lowest: 5.5,
            limit: 6.0,
        }));
        assert!(matches!(gate, EngageFailed::Gate(_)), "{gate:?}");
    }

    /// Nothing is acquired before the configuration resolves: a file that is not
    /// there refuses without a port or a servo being touched.
    #[test]
    fn a_configuration_that_is_not_there_is_refused() {
        let refused = Machine::resolve(
            Path::new("/nonexistent/reachy-bench.toml"),
            Overrides::default(),
        )
        .expect_err("there is no configuration there");

        let StartupError::Config(detail) = refused else {
            panic!("a missing configuration is a startup refusal");
        };
        assert!(
            detail.contains("/nonexistent/reachy-bench.toml"),
            "{detail}"
        );
    }

    /// The three numbers a bench configuration contributes, as the shipped
    /// example resolves them: a three-second raise, a two-second fold, and no
    /// antenna clock of its own.
    fn bench() -> (Duration, Duration, Option<Duration>) {
        (Duration::from_secs(3), Duration::from_secs(2), None)
    }

    /// A daemon whose file says nothing about the durations moves at exactly the
    /// pace the operator tool does — the property that keeps one machine from
    /// having two descriptions of itself.
    #[test]
    fn nothing_stated_leaves_every_clock_to_the_machine() {
        let (up, stow, antennas) = bench();
        let clocks = Clocks::lay_over(up, stow, antennas, Overrides::default());

        assert_eq!(
            clocks.up,
            Clock {
                duration: up,
                from: Source::Bench
            }
        );
        assert_eq!(
            clocks.stow,
            Clock {
                duration: stow,
                from: Source::Bench
            }
        );
        // No antenna clock anywhere is the antennas running on whichever head
        // group clock the move is using — so the two moves differ, which is what
        // one scalar warp for everything amounts to.
        assert_eq!(clocks.antennas, None);
        assert_eq!(clocks.up_durations(), MoveDurations::uniform(up));
        assert_eq!(clocks.stow_durations(), MoveDurations::uniform(stow));
    }

    /// Each of the three is overridden alone. Presence pace is what this file
    /// exists to tune, and a daemon that took the raise and quietly moved the
    /// fold with it would be tuning the machine behind the bench file's back.
    #[test]
    fn a_stated_clock_overrides_the_machines_and_the_others_stand() {
        let (up, stow, antennas) = bench();
        let stated = Duration::from_millis(1_400);

        let raised = Clocks::lay_over(
            up,
            stow,
            antennas,
            Overrides {
                up: Some(stated),
                ..Overrides::default()
            },
        );
        assert_eq!(
            raised.up,
            Clock {
                duration: stated,
                from: Source::Daemon
            }
        );
        assert_eq!(raised.stow.duration, stow);
        assert_eq!(raised.stow.from, Source::Bench);

        let folded = Clocks::lay_over(
            up,
            stow,
            antennas,
            Overrides {
                stow: Some(stated),
                ..Overrides::default()
            },
        );
        assert_eq!(
            folded.stow,
            Clock {
                duration: stated,
                from: Source::Daemon
            }
        );
        assert_eq!(folded.up.duration, up);
        assert_eq!(folded.up.from, Source::Bench);
    }

    /// The antennas are mechanically independent of the head and sweep much
    /// further, so their clock is independent too: one number, the same on both
    /// moves, leaving each head group at its own pace. This is the whole point of
    /// the split — a raise tuned to be quick is not floored by an antenna arc.
    #[test]
    fn the_antenna_clock_is_the_same_on_both_moves_and_floors_neither_head_group() {
        let (up, stow, _) = bench();
        let stated = Duration::from_millis(1_500);
        let clocks = Clocks::lay_over(
            up,
            stow,
            None,
            Overrides {
                antennas: Some(stated),
                ..Overrides::default()
            },
        );

        assert_eq!(
            clocks.antennas,
            Some(Clock {
                duration: stated,
                from: Source::Daemon
            })
        );
        assert_eq!(
            clocks.up_durations(),
            MoveDurations {
                head: up,
                antennas: stated
            }
        );
        assert_eq!(
            clocks.stow_durations(),
            MoveDurations {
                head: stow,
                antennas: stated
            }
        );
    }

    /// Where both files state an antenna clock, this one wins — the same rule as
    /// the head group's, and it has to be the same rule or an operator tuning one
    /// number would get a different answer depending on which one it was.
    #[test]
    fn a_stated_antenna_clock_beats_the_machines_own() {
        let (up, stow, _) = bench();
        let clocks = Clocks::lay_over(
            up,
            stow,
            Some(Duration::from_secs(1)),
            Overrides {
                antennas: Some(Duration::from_millis(1_500)),
                ..Overrides::default()
            },
        );

        assert_eq!(
            clocks.antennas,
            Some(Clock {
                duration: Duration::from_millis(1_500),
                from: Source::Daemon
            })
        );
    }

    /// The machine's own antenna clock reaches the moves, and is reported as the
    /// machine's. A bench file that already split the two groups is not a file
    /// this daemon has to be told about twice.
    #[test]
    fn the_machines_antenna_clock_is_used_and_attributed() {
        let (up, stow, _) = bench();
        let clocks = Clocks::lay_over(up, stow, Some(Duration::from_secs(1)), Overrides::default());

        assert_eq!(
            clocks.antennas,
            Some(Clock {
                duration: Duration::from_secs(1),
                from: Source::Bench
            })
        );
        assert_eq!(clocks.up_durations().antennas, Duration::from_secs(1));
    }

    /// What the startup line and the capture say. Both numbers and both files:
    /// the override is invisible in the bench configuration, so a head moving at
    /// a pace nobody expects is otherwise two files and a guess to explain.
    #[test]
    fn the_startup_line_names_every_clock_and_the_file_it_came_from() {
        let (up, stow, _) = bench();
        let clocks = Clocks::lay_over(
            up,
            stow,
            None,
            Overrides {
                up: Some(Duration::from_millis(1_400)),
                antennas: Some(Duration::from_millis(1_500)),
                ..Overrides::default()
            },
        );

        assert_eq!(
            clocks.to_string(),
            "up 1.400 s (daemon), stow 2.000 s (bench), antennas 1.500 s (daemon)"
        );
        assert_eq!(
            clocks.json(),
            json!({
                "up_ms": 1_400,
                "up_from": "daemon",
                "stow_ms": 2_000,
                "stow_from": "bench",
                "antenna_ms": 1_500,
                "antenna_from": "daemon",
            })
        );

        // And with no antenna clock anywhere, the line says what the antennas do
        // instead rather than leaving a number out.
        let plain = Clocks::lay_over(up, stow, None, Overrides::default());
        assert_eq!(
            plain.to_string(),
            "up 3.000 s (bench), stow 2.000 s (bench), antennas on the head group's clock"
        );
        assert_eq!(plain.json()["antenna_ms"], serde_json::Value::Null);
        assert_eq!(plain.json()["antenna_from"], serde_json::Value::Null);
    }

    /// The only place a script's posture becomes a pose. Swapping the arms
    /// inverts the feature — stow on wake, head up when the interaction ends —
    /// and swapping the durations runs every move at the wrong pace; the
    /// envelope check refuses neither, because both are legal.
    #[test]
    fn each_posture_names_its_own_pose_and_its_own_pace() {
        let up = MoveDurations::uniform(Duration::from_millis(1_200));
        let stow = MoveDurations::uniform(Duration::from_millis(2_500));
        assert_ne!(
            up, stow,
            "a fixture that cannot tell the two pairings apart"
        );
        assert_ne!(
            neutral_targets(),
            stow_pose_targets(),
            "a machine whose two postures are the same pose"
        );

        assert_eq!(targets_for(Posture::Up, up, stow), (neutral_targets(), up));
        assert_eq!(
            targets_for(Posture::Stow, up, stow),
            (stow_pose_targets(), stow)
        );
    }

    /// A commissioning that refused never took the machine. Four things follow,
    /// and every one of them is safety posture: the fault is recorded so the bus
    /// thread alerts, a stop is requested so nothing waits on a motion loop that
    /// is not coming, the ending is noted so the bus thread closes down, and the
    /// run ends faulted. Nothing is released, because nothing was ever torqued.
    #[test]
    fn a_commissioning_that_refused_faults_without_touching_torque() {
        let shared = Shared::new(POD);
        let sink = Collect::default();

        let outcome = commission_failed(&shared, Refusal::new("servo 21 answered nothing"), &sink);

        let Outcome::Faulted(report) = &outcome else {
            panic!("a commissioning that refused is a fault");
        };
        assert_eq!(report.stage, FaultStage::Commission);
        assert_eq!(shared.fault(), Some(report));
        assert!(shared.stopping().is_some());
        assert!(
            shared.motion_ended(),
            "the bus thread waits for this before it closes the attachment"
        );
        let fields = sink.fields("motion_fault").expect("the fault is reported");
        assert_eq!(fields["stage"], json!("commissioning"));
        assert_eq!(fields["detail"], json!("servo 21 answered nothing"));
    }

    /// Only a move narrates as text, so this is a fixture for `says_moving`
    /// alone.
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
        let head = Fake::new(&shared, [Event::Raise, Event::Stop(Stop::Operator)])
            // The opening stow says nothing; the move up says what every move
            // says at the end of its run.
            .saying_moving([REPORT.to_vec()])
            // Every dwell reports the disposition it was asked for, and nothing
            // else — the one thing a dwell always carries.
            .saying([
                vec![TickEvent::Command(CommandDisposition::Held)],
                vec![TickEvent::Command(CommandDisposition::Held)],
            ]);

        let (outcome, _, sink) = driven(&shared, head);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
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
            [
                Event::Raise,
                Event::Wait,
                Event::Lower,
                Event::Wait,
                Event::Stop(Stop::Operator),
            ],
        )
        .saying([vec![lost()], vec![lost()], vec![lost()], vec![lost()]]);

        let (outcome, _, sink) = driven(&shared, head);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        // The engage line carries a wall clock, so what is compared is the
        // narration this filter owns plus the moves it has to print under.
        let narration: Vec<String> = sink
            .said()
            .lines
            .into_iter()
            .filter(|line| line.starts_with("  ") || line.starts_with("motion: "))
            .collect();
        assert_eq!(narration[0], "motion: stow -> up");
        assert_eq!(narration[1], rendered(lost()));
        assert_eq!(narration[2], "  ... unchanged across 1 further dwell(s)");
        assert_eq!(narration[3], "motion: up -> stow");
        assert_eq!(
            narration[4],
            rendered(lost()),
            "the same reading after a move is news again: {narration:?}"
        );
    }

    /// A dwell that repeats and then refuses states the span before the fault
    /// prints. That span is the whole of the context a bring-up run has for
    /// deciding whether the fault and the condition running underneath it are
    /// the same event.
    #[test]
    fn a_fault_states_the_span_of_the_episode_it_interrupts() {
        let shared = Arc::new(Shared::new(POD));
        let head = Fake::new(
            &shared,
            [Event::Raise, Event::Wait, Event::Wait, Event::Refuse],
        )
        .saying([vec![lost()], vec![lost()], vec![lost()]]);

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
                Event::Raise,
                Event::Raise,
                Event::Wait,
                Event::Wait,
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

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
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
