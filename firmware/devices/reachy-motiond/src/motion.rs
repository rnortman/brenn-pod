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
//! Four things are deliberate about the shape:
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
//! - **A pre-torque sweep never faults, however long it fails.** A limp machine
//!   is already at the minimum risk condition, so a sweep that stops answering
//!   costs the daemon its picture of the machine and not its control of it:
//!   there is nothing a fault response could make safer, and parking would
//!   forfeit a recovery that otherwise costs nothing. The startup look and the
//!   resting watch both keep sweeping, say so once per run, and refuse an
//!   engage until a sweep answers. Deliberately asymmetric with the engaged
//!   path, whose read-loss budget guards a machine under torque and does fault.
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

use std::cell::Cell;
use std::fmt;
use std::path::Path;
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::{Duration, Instant};

use motion_proto::{Desired, Posture};
use reachy_bench::commands::{
    Commissioned, Engaged, StreamBase, commission, neutral_targets, stow_pose_targets, wind_down,
};
use reachy_bench::config::{self, Resolved, resolve_for_commanding};
use reachy_bench::pump::{
    Disposition, ErrorClass, MonotonicClock, Phase as TorquePhase, PumpError, TickEvent,
};
use reachy_bus::{BusPort, OpenError, SerialBusPort};
use reachy_clips::{ClipLimits, compose};
use reachy_motion::{
    CommandRejection, Entry, Fault, JointGroup, JointSet, JointTargets, Maneuver, MoveDurations,
    Outcome as TimelineOutcome, PollCadence, at_stow,
};
use serde_json::json;
use thiserror::Error;

use crate::cells::{Antennas, FaultReport, FaultStage, Overlaid, Shared, Stop, condition, story};
use crate::config::Overrides;
use crate::overlay::{self, Overlays};
use crate::report::Sink;
use crate::state::{Phase, Surface, Watching};

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

/// What the motion libraries refused: what it asks of the machine, the
/// condition it names, and how they worded it.
///
/// The class is derived here and nowhere else. It is the whole of what decides
/// whether this daemon stows the head under control, takes torque off on the
/// spot, or parks and waits for a person — so deriving it twice is two answers
/// to that question, and deriving it from the wording is an answer that changes
/// when somebody rewords an error. The text is still text, for the same reason
/// as ever: the consumers are a log line and an alert, and the libraries'
/// refusals already name the phase, the servo, the register and both values.
#[derive(Debug, Clone, PartialEq, Error)]
#[error("{text}")]
pub struct Refusal {
    /// What answering this ending asks of the machine.
    class: ErrorClass,
    /// The condition of the machine the ending names, when it names one. A
    /// refusal, a planner defect and an exhausted budget all name none: they
    /// are statements about what was asked for, not about the platform.
    ///
    /// Behind a pointer: a `Fault` carrying a sequencer verdict is an order of
    /// magnitude wider than the rest of this struct, it is absent on the
    /// ordinary refusals, and a refusal is returned as the error arm of most of
    /// this module's calls.
    fault: Option<Box<Fault>>,
    /// Whether the session's own record already carries that condition. The
    /// tick records what it raises; a bus that stopped carrying is seen by the
    /// layer holding the wire, and the record owes an entry for it.
    recorded: bool,
    /// The libraries' own rendering.
    text: String,
}

impl Refusal {
    /// A refusal of `class`, worded by whatever produced it.
    pub fn new(class: ErrorClass, detail: impl Into<String>) -> Self {
        Self {
            class,
            fault: None,
            recorded: true,
            text: detail.into(),
        }
    }

    /// A refusal of `class` naming `condition`, which the session's record does
    /// not have yet.
    ///
    /// The shape of an ending the layer holding the wire found rather than the
    /// tick: a bus that stopped carrying, a torque-off nobody acknowledged.
    /// Whoever answers it is what puts the condition into the record.
    pub fn naming(class: ErrorClass, condition: Fault, detail: impl Into<String>) -> Self {
        Self {
            class,
            fault: Some(Box::new(condition)),
            recorded: false,
            text: detail.into(),
        }
    }

    /// A library ending classified as though torque were on.
    ///
    /// The honest question to ask of anything the drive raised: the engage is
    /// the one path that crosses the torque line mid-run, and by the time an
    /// ending reaches this daemon the library has already released a machine it
    /// half-enabled.
    #[must_use]
    pub fn under_torque(error: &PumpError) -> Self {
        Self::classified(error, TorquePhase::UnderTorque)
    }

    /// A library ending from a path where torque is off by construction —
    /// commissioning, and the sweeps a limp machine is measured by.
    ///
    /// Nothing was energized, so the machine is exactly as safe as it was and
    /// asking again later is the whole of the answer.
    #[must_use]
    pub fn pre_torque(error: &PumpError) -> Self {
        Self::classified(error, TorquePhase::PreTorque)
    }

    fn classified(error: &PumpError, phase: TorquePhase) -> Self {
        let fault = error.fault(phase);
        Self {
            class: error.class(phase),
            fault: fault.map(Box::new),
            // Everything the tick raises arrives already recorded; what the
            // wire-holding layer found does not.
            recorded: fault.is_some() && error.unrecorded_fault(phase).is_none(),
            text: error.to_string(),
        }
    }

    /// What answering this ending asks of the machine.
    #[must_use]
    pub fn class(&self) -> ErrorClass {
        self.class
    }

    /// The condition the session's record does not have yet, if any.
    #[must_use]
    pub fn unrecorded(&self) -> Option<Fault> {
        (!self.recorded)
            .then_some(self.fault.as_deref().copied())
            .flatten()
    }

    /// The same ending, with its condition now in the record.
    ///
    /// What putting it there earns: an ending whose condition has been noted
    /// once must not be noted again when a further ending folds in beside it and
    /// names nothing of its own.
    #[must_use]
    fn noted(mut self) -> Self {
        self.recorded = true;
        self
    }

    /// This ending with what happened next folded into it.
    ///
    /// The disposition is the sticky maximum: a stow defeated by a servo
    /// dropping out, or a torque-off nobody acknowledged, latches an ending
    /// that would otherwise have rested. The condition named is the later one
    /// when it named one, because that is the one an operator is looking for.
    #[must_use]
    fn and(self, next: Self) -> Self {
        let named_by_next = next.fault.is_some();
        Self {
            // The doctrine's own ranking, from the crate that owns the classes:
            // which of two endings the machine is judged by is not a question
            // this daemon gets a second opinion about.
            class: self.class.worse(next.class),
            fault: next.fault.or(self.fault),
            recorded: if named_by_next {
                next.recorded
            } else {
                self.recorded
            },
            text: format!("{} — and then: {}", self.text, next.text),
        }
    }
}

impl From<PumpError> for Refusal {
    fn from(error: PumpError) -> Self {
        Self::under_torque(&error)
    }
}

/// The session's record as it happens, pushed by the motion libraries.
///
/// Held beside the engagement rather than inside it, and for one reason: the
/// last entries of an incident are appended by the release that consumes the
/// engagement — the maneuver completing, a servo that never acknowledged its
/// torque-off — so a reader living on the engagement would miss exactly the
/// ending it exists to report.
///
/// Typed entries, never parsed back out of a rendered line: what reaches the
/// fault cell and the capture is this record, and the words are made of it at
/// the sink and nowhere earlier.
#[derive(Debug)]
pub struct Incident {
    /// What has been taken off the channel so far, oldest first.
    kept: Vec<Entry>,
    pushed: Receiver<Entry>,
}

impl Incident {
    /// A record fed by `pushed`.
    #[must_use]
    pub fn new(pushed: Receiver<Entry>) -> Self {
        Self {
            kept: Vec::new(),
            pushed,
        }
    }

    /// The record of a session there never was, which stays empty.
    ///
    /// What an ending before the first torque write is answered with: a
    /// commissioning that refused, an engage that never completed. Nothing ever
    /// held the machine, so nothing ever recorded anything about it — and a
    /// response reads the record the same way whether or not there was a session
    /// behind it.
    #[must_use]
    pub fn unrecorded() -> Self {
        let (_, pushed) = channel();
        Self::new(pushed)
    }

    /// Everything the session has recorded so far, oldest first.
    ///
    /// Accumulating rather than draining: a mid-session condition is read out
    /// while the head is still up, and the ending that follows it has to report
    /// the whole story and not the tail of it.
    pub fn entries(&mut self) -> &[Entry] {
        self.kept.extend(self.pushed.try_iter());
        &self.kept
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

    /// The bounds a motion document is derived and validated against on this
    /// unit: this machine's geometry, envelope and per-tick step bounds.
    ///
    /// Read off the bench configuration rather than the library's defaults, so
    /// a clip's speed ceiling and its blend floors are what *this* machine's
    /// numbers imply. A daemon deriving against defaults would admit a clip its
    /// own tick then refuses every period.
    #[must_use]
    pub fn clip_limits(&self) -> ClipLimits {
        ClipLimits::from_motion_config(&self.resolved.motion)
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
        // Pre-torque by construction: commissioning reads presence, identity,
        // the provisioned registers, the supply and the gains, and writes torque
        // in neither direction.
        let machine = commission(&self.resolved, port, &mut clock, line)
            .map_err(|error| Refusal::pre_torque(&error))?;
        Ok(SessionRest {
            machine,
            resolved: &self.resolved,
            up: self.clocks.up_durations(),
            stow: self.clocks.stow_durations(),
            clock,
            rail: Rail::new(rail_period(self.resolved.health_poll_hz)),
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
/// to stay inside its own per-tick step bound. The antennas carry a clock each,
/// because the pair's two tips cross inboard of the head and what parts them
/// there is one side reaching the crossing ahead of the other.
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
    /// Each antenna, right then left; `None` for a side neither file gives a
    /// clock, which runs on whichever head-group clock the move is using.
    pub antennas: [Option<Clock>; 2],
}

impl Clocks {
    /// Lay the daemon's durations over the machine's.
    #[must_use]
    pub fn resolve(resolved: &Resolved, overrides: Overrides) -> Self {
        Self::lay_over(
            resolved.up_duration,
            resolved.stow_duration,
            resolved.antenna_duration,
            resolved.antenna_durations,
            overrides,
        )
    }

    /// The resolution itself, in the numbers the machine's file contributes.
    ///
    /// Split out from [`Clocks::resolve`] because a `Resolved` is a whole bench
    /// configuration — a servo map, an envelope and a measured datum — and what
    /// is worth asserting here is which of two files won, which needs none of
    /// that.
    fn lay_over(
        bench_up: Duration,
        bench_stow: Duration,
        bench_antennas: Option<Duration>,
        bench_sides: [Option<Duration>; 2],
        overrides: Overrides,
    ) -> Self {
        Self {
            up: pick(bench_up, overrides.up),
            stow: pick(bench_stow, overrides.stow),
            antennas: [0, 1].map(|side| {
                antenna(
                    overrides.antenna_sides[side].or(overrides.antennas),
                    bench_sides[side].or(bench_antennas),
                )
            }),
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

    /// `head` for the head group, and each antenna's own clock beside it.
    ///
    /// Where a side is unstated the library decides what it runs on, asked
    /// rather than restated: what an absent antenna clock means is the same
    /// question for this daemon and for the operator tool, and two answers to
    /// it would be two staggers.
    fn durations(&self, head: Duration) -> MoveDurations {
        // Both shared keys are already folded into the two sides, with the file
        // each came from recorded, so there is no shared clock left to state.
        MoveDurations::resolved(
            head,
            None,
            self.antennas.map(|side| side.map(|c| c.duration)),
        )
    }

    /// The resolved durations, for the capture.
    #[must_use]
    pub fn json(&self) -> serde_json::Value {
        let [right, left] = self.antennas;
        json!({
            "up_ms": millis(self.up.duration),
            "up_from": self.up.from.as_str(),
            "stow_ms": millis(self.stow.duration),
            "stow_from": self.stow.from.as_str(),
            "antenna_right_ms": right.map(|clock| millis(clock.duration)),
            "antenna_right_from": right.map(|clock| clock.from.as_str()),
            "antenna_left_ms": left.map(|clock| millis(clock.duration)),
            "antenna_left_from": left.map(|clock| clock.from.as_str()),
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
            [None, None] => f.write_str(", antennas on the head group's clock"),
            [right, left] => write!(
                f,
                ", antennas right {}, left {}",
                antenna_text(right),
                antenna_text(left)
            ),
        }
    }
}

/// One antenna's clock as the startup line words it.
fn antenna_text(clock: Option<Clock>) -> String {
    match clock {
        Some(clock) => format!("{} ({})", secs(clock.duration), clock.from.as_str()),
        None => "on the head group's clock".to_string(),
    }
}

/// One antenna's clock: the first file to state one for that side, and which
/// file that was.
///
/// Each file has already been asked for the side's own key before its shared
/// one, so what arrives here is what each file has to say about *this* antenna.
/// The daemon's answer wins, the same way round as every other duration —
/// otherwise an operator moving one number would get a different answer
/// depending on which number it was. `None` for a side neither file spoke for.
fn antenna(stated: Option<Duration>, bench: Option<Duration>) -> Option<Clock> {
    match (stated, bench) {
        (Some(duration), _) => Some(Clock {
            duration,
            from: Source::Daemon,
        }),
        (None, Some(duration)) => Some(Clock {
            duration,
            from: Source::Bench,
        }),
        (None, None) => None,
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

/// When a sweep last read the supply and the error bits, and how often one
/// must.
///
/// The two torque-on gates are evaluated from whatever the last such sweep
/// read, so this decides how stale their inputs may be: never re-reading leaves
/// them judging an engage against commissioning-time numbers, and re-reading on
/// every sweep makes the resting watch most of the traffic on the wire. Daemon
/// policy, so it is arithmetic here rather than a branch buried in a
/// transaction.
#[derive(Debug, Clone, Copy)]
struct Rail {
    /// The longest a gate's inputs may be carried forward.
    every: Duration,
    /// When a sweep last read them, or `None` when the next sweep must.
    last: Option<Instant>,
}

impl Rail {
    /// A machine whose gates have nothing but commissioning behind them: the
    /// first sweep reads.
    fn new(every: Duration) -> Self {
        Self { every, last: None }
    }

    /// What a sweep taken at `now` reads.
    fn cadence(&self, now: Instant) -> PollCadence {
        if self
            .last
            .is_none_or(|last| now.duration_since(last) >= self.every)
        {
            PollCadence::PositionsAndRail
        } else {
            PollCadence::Positions
        }
    }

    /// A sweep taken at `now` answered, having read whatever `cadence` asked
    /// for.
    fn read(&mut self, now: Instant, cadence: PollCadence) {
        if matches!(cadence, PollCadence::PositionsAndRail) {
            self.last = Some(now);
        }
    }

    /// A sweep failed: the next one that answers reads the rail, whenever that
    /// is.
    ///
    /// A positions-only sweep carries the previous rail reading forward, so
    /// without this an engage after an outage of any length would judge its two
    /// torque-on gates against a supply and error bits measured before it —
    /// arbitrarily long before it. The cost is nine extra reads on a path that
    /// is already a recovery.
    fn lost(&mut self) {
        self.last = None;
    }
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
#[derive(Debug, Clone, PartialEq, Error)]
pub enum EngageFailed {
    /// A refusal from before torque: one of the two torque-on gates — the
    /// supply below its floor, or a latched hardware error — or a remedial
    /// sweep that could not measure the machine an engage has to pin. Nothing
    /// was written, the machine is limp exactly where it was, and the next
    /// script may ask again.
    #[error("{0}")]
    Gate(Refusal),

    /// Anything else. The engage path takes the machine limp on its way out, so
    /// what is left is a fault to report and a daemon to park.
    #[error("{0}")]
    Fault(Refusal),
}

impl From<PumpError> for EngageFailed {
    fn from(error: PumpError) -> Self {
        // The one derivation, read rather than repeated: what answers `Refuse`
        // under torque is what is judged before any transaction writes torque —
        // the two gates among them — so nothing was written and the next script
        // may ask again.
        let refusal = Refusal::under_torque(&error);
        if refusal.class() == ErrorClass::Refuse {
            Self::Gate(refusal)
        } else {
            Self::Fault(refusal)
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
    ///
    /// A posture no sweep has confirmed since is measured again first: pinning
    /// a limp head at where it *was* is the slam that measurement prevents. A
    /// machine that cannot be measured refuses the engage rather than faulting
    /// — torque is still off, so there is nothing to undo.
    ///
    /// The record comes back beside the engagement rather than inside it: a
    /// session's story ends with the release that consumes the engagement, so
    /// whatever reads it has to outlive that.
    fn engage(
        &mut self,
        line: &mut dyn FnMut(&str),
    ) -> Result<(Self::Active<'_>, Incident), EngageFailed>;
}

/// What the base layer does for the length of one streamed run.
///
/// The overlay loop's half of what a posture step means: the same two answers
/// the dwell path has — go somewhere, or stay — said once at the top of a run
/// rather than at every boundary, because a run ends the moment either answer
/// changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasePlan {
    /// Hold the configuration the machine is already commanding. What a `keep`
    /// means, and what an overlay over a posture already reached rides on.
    Held,
    /// Carry the base to this posture while the overlays ride it.
    To(Posture),
}

/// Where the base layer is on one control period.
#[derive(Debug, Clone, Copy)]
pub struct BaseAt {
    /// The configuration the base commands now.
    pub targets: JointTargets,
    /// Whether the base has nothing further to command: always for a held
    /// base, and for a transition once its trajectory is spent.
    pub arrived: bool,
    /// The control period this run is being driven at, which is what an
    /// overlay's own clock advances by. The machine's, not the loop's: the tick
    /// rate is a property of the machine that was resolved at startup.
    pub period: Duration,
}

/// How a streamed run ended.
#[derive(Debug, Clone, PartialEq)]
pub enum Streamed {
    /// The daemon stopped supplying setpoints. The machine holds the last one
    /// it was given.
    Ended,
    /// The machine would not take a composed setpoint. Nothing was commanded on
    /// that period and nothing faulted — per the doctrine a plan of ours the
    /// tick refuses parks nothing and latches nothing — and the head is where
    /// the last accepted period put it.
    ///
    /// The tick's own refusal, carried whole rather than worded: this is the
    /// failure a live machine is expected to produce, and which class it was
    /// and which joint it named are what the report is asked for afterwards.
    Refused(CommandRejection),
}

/// The machine holding torque: the postures, the moves between them, and the
/// two ways torque comes off.
pub trait Active {
    /// Carry the head to `posture`, measured: the move ends when the machine is
    /// found there, and a stow it cannot reach comes back as a refusal rather
    /// than as an arrival.
    ///
    /// Nothing diverts it. The three callers are the unattended fold flows —
    /// startup normalisation, the shutdown stow and the wind-down stow — where
    /// the fold is the last thing the daemon owes the machine and measured
    /// arrival is what routes the response to a stow that was defeated. Every
    /// base drive a script asks for goes through [`Self::stream`] instead.
    ///
    /// `line` is the run as the motion library words it, for the console;
    /// `event` is the same run typed, for the facts this daemon has to record
    /// as more than prose. Both, because the library's per-period report has no
    /// typed form and a rendered line is not something a capture can key on.
    fn move_to(
        &mut self,
        posture: Posture,
        line: &mut dyn FnMut(&str),
        event: &mut dyn FnMut(TickEvent),
    ) -> Result<(), Refusal>;

    /// Watch the machine hold for `dwell`, commanding nothing.
    ///
    /// Typed events rather than rendered lines: what a dwell is worth saying
    /// about is this daemon's policy, and keying on the event kind is what
    /// keeps that policy off the wording another repository chose.
    fn hold(&mut self, dwell: Duration, event: &mut dyn FnMut(TickEvent)) -> Result<(), Refusal>;

    /// Command one setpoint per control period, composed by `compose`, over a
    /// base this machine samples.
    ///
    /// The tick-granular entry point, and the only one: the daemon owns the
    /// overlays and the composition, the machine owns the reference they ride
    /// on and the period they ride at. `compose` is asked once per period with
    /// where the base is, and answers the setpoint to command or `None` to end
    /// the run — which is how a schedule that changed under the run, an
    /// overlay that faded out and a stop all end it, at the granularity of one
    /// period rather than one dwell.
    ///
    /// Every composed setpoint faces the same envelope check, per-tick step
    /// bound and antenna mask a planned sample faces; none of that is bypassed
    /// and none of it is duplicated here. A setpoint the machine will not take
    /// comes back as [`Streamed::Refused`] rather than as an `Err`, because
    /// only the caller knows what rode the refused setpoint: a refusal an
    /// overlay was composed into is answered by dropping the overlays and
    /// re-acquiring, while a bare one ends the session. Either way nothing was
    /// commanded, nothing faulted, and the head holds where the last accepted
    /// period put it.
    ///
    /// A [`BasePlan::To`] the machine will not plan at all is also a
    /// [`Streamed::Refused`], reported before any setpoint goes out.
    fn stream(
        &mut self,
        base: BasePlan,
        line: &mut dyn FnMut(&str),
        event: &mut dyn FnMut(TickEvent),
        compose: &mut dyn FnMut(BaseAt) -> Option<JointTargets>,
    ) -> Result<Streamed, Refusal>;

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
    /// The fault ending for a machine that can no longer be trusted to command.
    /// No settle and no measurement: the head falls gently into near-stow under
    /// gearbox resistance from wherever it is. An `Err` says what the release
    /// itself had to report — a servo that never acknowledged its own torque-off
    /// — and never that the release did not happen.
    fn disengage_now(self, line: &mut dyn FnMut(&str)) -> Result<(), Refusal>
    where
        Self: Sized;

    /// Stow on what still commands, then release everything.
    ///
    /// The answer to a servo that dropped out mid-move: it is already torqued
    /// off and out of every check by the time this is reached, so the head comes
    /// down on the five that are left rather than going limp on the spot. A
    /// further servo going expands the maneuver instead of ending it, on what is
    /// left of the one stow clock this started with.
    ///
    /// What comes back is what the machine is left waiting for, as the maneuver
    /// itself reported it. This daemon routes on that answer rather than
    /// deriving one of its own: how far the mask grew is a fact of the maneuver,
    /// and a second derivation up here is a second answer that can disagree with
    /// the record.
    ///
    /// `deadline` is when the one stow clock this maneuver is part of is spent,
    /// on the session's own scale ([`Self::stow_deadline`] opens one). Handed in
    /// rather than opened here, because a stow this daemon commanded itself and
    /// had defeated is the *same* maneuver expanding: a fresh clock would drive
    /// the head for a second whole stow window against whatever defeated the
    /// first, and the window is sized so that a person holding the head is not
    /// pushed indefinitely.
    ///
    /// `event` gets the stow's tick events as values, as [`Self::move_to`] does
    /// for the moves this daemon commands: the pair leaving the moves on the way
    /// down reaches the cells that answer for it from here or from nowhere.
    fn masked_stow(
        self,
        deadline: Duration,
        line: &mut dyn FnMut(&str),
        event: &mut dyn FnMut(TickEvent),
    ) -> Disposition
    where
        Self: Sized;

    /// When one stow maneuver started now would be out of clock.
    ///
    /// Read at the moment a controlled response begins and carried through every
    /// escalation of it, so however many servos drop out the head is commanded
    /// for one stow window and not one per attempt.
    fn stow_deadline(&self) -> Duration;

    /// The maneuver already answering this session, if one is.
    ///
    /// The escalation ladder's rule as a question, asked of the session's own
    /// record: a maneuver still open is the one that absorbs whatever happens
    /// next, and nothing starts a second answer to a machine already being
    /// answered.
    fn open_maneuver(&self) -> Option<Maneuver>;

    /// The joints this session is no longer commanding.
    ///
    /// One set, whichever way a joint got into it: the engage-time health gate
    /// seeds it with servos that were already flagging, and a condition raised
    /// mid-session inserts into the same set. What the daemon reports about a
    /// degraded machine is read from here rather than from the event that
    /// happened to announce it, because only one of those two is true at every
    /// instant of a session.
    fn out_of_service(&self) -> JointSet;

    /// Put a condition only this layer could have seen into the session's
    /// record.
    ///
    /// The tick records what it raises. A bus that stopped carrying commands is
    /// found by the layer holding the wire, and the record owes an entry for it
    /// wherever the ending is answered — otherwise the one incident an operator
    /// is sent to read has no condition in it at all.
    fn note(&mut self, fault: Fault);

    /// Put how far a maneuver this daemon commanded itself has got into the
    /// session's record.
    ///
    /// The other half of [`Self::note`], and the record is only a story with
    /// both: a condition is what happened and a maneuver is what answered it. The
    /// controlled stow the unattended daemon runs is commanded here rather than
    /// inside the library, so this is the only layer that can say it started and
    /// how it ended — and the one place the same event on the bench is a full
    /// story while here it is half of one.
    ///
    /// Which maneuver it is comes from the ending's own class, never from a
    /// caller's opinion of it.
    fn note_response(&mut self, maneuver: Maneuver, outcome: TimelineOutcome);
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
    /// When the supply and the error bits were last read, and how long they may
    /// be carried forward. The positions move under a hand; those change on the
    /// timescale of a power supply, and reading them every sweep would be most
    /// of the resting traffic on the wire.
    rail: Rail,
}

/// The same machine holding torque.
pub struct SessionActive<'m, 'a, P: BusPort> {
    engaged: Engaged<'m, 'a, P>,
    clock: MonotonicClock,
    up: MoveDurations,
    // Both clocks are configuration, resolved once and sized for the spans a
    // presence move covers. The startup stow runs on `stow` from wherever the
    // machine was left, which no configuration can size for; the motion library
    // right-sizes a clock too short for the span it actually covers before the
    // move is commanded, and says so, which is what `motion_clock_stretched`
    // carries.
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

/// A sweep taken while torque is off, with the rail bookkeeping around it.
///
/// Both pre-torque sweeps — the resting watch and the remedial one an engage
/// takes past a stale posture — agree about what a failure costs. Nothing was
/// commanded, so the machine is exactly as safe as it was; what is lost is the
/// rail reading, which no longer describes anything, because nothing bounds how
/// long the outage lasts. Written once so the two cannot come to disagree.
fn pre_torque_sweep<T>(
    rail: &mut Rail,
    now: Instant,
    cadence: PollCadence,
    sweep: impl FnOnce(PollCadence) -> Result<T, PumpError>,
) -> Result<T, PumpError> {
    match sweep(cadence) {
        Ok(measured) => {
            rail.read(now, cadence);
            Ok(measured)
        }
        Err(error) => {
            rail.lost();
            Err(error)
        }
    }
}

/// The sweep an engage takes when the posture it would pin is stale, and what
/// its failure means.
///
/// Taken here rather than left to the library, because the classification is
/// the whole point and only this side knows torque is still off: this sweep and
/// the enable walk after it raise the same refusals, so the error value alone
/// cannot say which side of a torque write it came from. On this side nothing
/// has been written, so a failure is a refusal the next script may simply ask
/// again after — never a fault to park a limp daemon for, which is the
/// difference between a daemon that recovers in five seconds and one that waits
/// for a person.
///
/// Always the rail as well as the positions: an engage that follows an outage
/// would otherwise judge its two torque-on gates against a supply and error
/// bits measured before the outage began.
fn remedial_sweep<T>(
    rail: &mut Rail,
    now: Instant,
    sweep: impl FnOnce(PollCadence) -> Result<T, PumpError>,
) -> Result<T, EngageFailed> {
    pre_torque_sweep(rail, now, PollCadence::PositionsAndRail, sweep)
        .map_err(|error| EngageFailed::Gate(Refusal::pre_torque(&error)))
}

impl<'a, P: BusPort> Rest for SessionRest<'a, P> {
    type Active<'e>
        = SessionActive<'e, 'a, P>
    where
        Self: 'e;

    fn watch(&mut self, line: &mut dyn FnMut(&str)) -> Result<Standing, Refusal> {
        let now = Instant::now();
        let cadence = self.rail.cadence(now);
        let (machine, clock) = (&mut self.machine, &mut self.clock);
        let sweep = pre_torque_sweep(&mut self.rail, now, cadence, |cadence| {
            machine.poll(cadence, clock, line)
        })
        .map_err(|error| Refusal::pre_torque(&error))?;
        Ok(if at_stow(&self.resolved.disarm, &sweep.present) {
            Standing::AtStow
        } else {
            Standing::Elsewhere
        })
    }

    fn engage(
        &mut self,
        line: &mut dyn FnMut(&str),
    ) -> Result<(Self::Active<'_>, Incident), EngageFailed> {
        let (up, stow) = (self.up, self.stow);
        let mut clock = self.clock;
        if !self.machine.fresh() {
            let machine = &mut self.machine;
            remedial_sweep(&mut self.rail, Instant::now(), |cadence| {
                machine.poll(cadence, &mut clock, line)
            })?;
        }
        let mut engaged = self.machine.engage(&mut clock, line)?;
        // Subscribed before the first goal goes out, so nothing a session raises
        // can be raised before there is anywhere for it to arrive.
        let incident = Incident::new(engaged.subscribe_timeline());
        Ok((
            SessionActive {
                engaged,
                clock,
                up,
                stow,
            },
            incident,
        ))
    }
}

impl<P: BusPort> Active for SessionActive<'_, '_, P> {
    fn move_to(
        &mut self,
        posture: Posture,
        line: &mut dyn FnMut(&str),
        event: &mut dyn FnMut(TickEvent),
    ) -> Result<(), Refusal> {
        let (targets, durations) = targets_for(posture, self.up, self.stow);
        self.engaged
            .move_events(targets, durations, &mut self.clock, line, event)?;
        Ok(())
    }

    fn hold(&mut self, dwell: Duration, event: &mut dyn FnMut(TickEvent)) -> Result<(), Refusal> {
        // The summary is dropped: a 200 ms window's period counts and jitter say
        // nothing a reader or this loop can act on, and the conditions worth
        // knowing about all arrive as events.
        self.engaged.hold_events(dwell, &mut self.clock, event)?;
        Ok(())
    }

    fn stream(
        &mut self,
        base: BasePlan,
        line: &mut dyn FnMut(&str),
        event: &mut dyn FnMut(TickEvent),
        compose: &mut dyn FnMut(BaseAt) -> Option<JointTargets>,
    ) -> Result<Streamed, Refusal> {
        let base = match base {
            BasePlan::Held => StreamBase::Held,
            BasePlan::To(posture) => {
                let (targets, durations) = targets_for(posture, self.up, self.stow);
                StreamBase::Toward(targets, durations)
            }
        };
        let period = self.engaged.period();
        let outcome = self.engaged.stream_layered(
            base,
            &mut self.clock,
            line,
            event,
            &mut |targets, arrived| {
                compose(BaseAt {
                    targets: *targets,
                    arrived,
                    period,
                })
            },
        );
        match outcome {
            Ok(()) => Ok(Streamed::Ended),
            // The one ending this answers with rather than raises: the tick
            // would not take what we composed, and by the doctrine that is a
            // bad plan of ours and not a condition of the machine.
            Err(PumpError::Rejected(why)) => Ok(Streamed::Refused(why)),
            Err(error) => Err(Refusal::under_torque(&error)),
        }
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

    fn masked_stow(
        self,
        deadline: Duration,
        line: &mut dyn FnMut(&str),
        event: &mut dyn FnMut(TickEvent),
    ) -> Disposition {
        let Self {
            engaged, mut clock, ..
        } = self;
        // The library's own maneuver, on the library's own state and on the clock
        // the response started with: the record it hands back is already on this
        // session's channel, so what is taken from here is the disposition alone.
        let (_, disposition) = wind_down(
            engaged,
            ErrorClass::MaskedSlowStowToPark,
            deadline,
            &mut clock,
            line,
            event,
        );
        disposition
    }

    fn stow_deadline(&self) -> Duration {
        // The budget is the machine's own, never this daemon's `stow` clock: a
        // policy file may command a longer stow, but it may not lengthen how
        // long a *defeated* one keeps driving a head somebody is holding.
        // TODO(stow-budget-source-unpinned): no test here holds that source.
        self.now().saturating_add(self.engaged.stow_budget())
    }

    fn open_maneuver(&self) -> Option<Maneuver> {
        self.engaged.timeline().open_maneuver()
    }

    fn out_of_service(&self) -> JointSet {
        self.engaged.out_of_service()
    }

    fn note(&mut self, fault: Fault) {
        let at = self.now();
        self.engaged.record_fault(fault, at);
    }

    fn note_response(&mut self, maneuver: Maneuver, outcome: TimelineOutcome) {
        let at = self.now();
        self.engaged.record_response(maneuver, outcome, at);
    }
}

impl<P: BusPort> SessionActive<'_, '_, P> {
    /// The session clock's reading, for the record and for a maneuver's deadline.
    ///
    /// Named through the library's own trait: this crate has a `Clock` of its own
    /// and it is a configured duration, not a source of time.
    fn now(&self) -> Duration {
        reachy_bench::pump::Clock::now(&self.clock)
    }
}

/// How the motion thread ended.
///
/// Two endings, because there are two states a machine can be left in and both
/// of them are limp. What differs is whether the daemon got there on purpose.
#[derive(Debug, Clone, PartialEq)]
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
        // A stretch, a settle verdict and a filled trace buffer are a move's
        // events, so they never reach a dwell's filter at all. Keyed whole for
        // the day that changes: two right-sizings of the same clock are two
        // facts about the configuration, and a key that dropped the durations
        // would swallow the second one.
        //
        // The two endings a move can carry — a pair taken out of service, a
        // move abandoned — are keyed whole for the reason a fault is: what they
        // name is the condition, and two of them are two incidents.
        event @ (TickEvent::Command(_)
        | TickEvent::ReadLost { .. }
        | TickEvent::HealthLost { .. }
        | TickEvent::Health(_)
        | TickEvent::Stretched(_)
        | TickEvent::Completed
        | TickEvent::Settled { .. }
        | TickEvent::Unsettled { .. }
        | TickEvent::TraceFull { .. }
        | TickEvent::AntennasDegraded(_)
        | TickEvent::Aborted(_)
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
    /// The script whose `keep` is the live base command, once it has been said.
    ///
    /// A `keep` is the answer at every boundary it covers, so the line is said
    /// on the transition into it and not once a dwell: the operator wants to
    /// know the daemon saw the command, not that it is still seeing it. Cleared
    /// by any other answer, so a script that keeps, raises and keeps again says
    /// so twice.
    keeping: Option<u64>,
    /// The script whose ask for the head up has already been answered — by a
    /// torque-on gate refusing the engage, or by an ending that answered the
    /// raise itself and put the machine back down.
    ///
    /// One field for both because the rule is one rule: a condition that ended
    /// one raise ends the next, so only a *new* ask changes the answer. The
    /// retry is therefore the next script's rather than the next resting sweep's
    /// — a chronically sagging rail, or a hand on the head, against a script
    /// asking for the head up would otherwise be ten refusals a second, or a
    /// machine cycling torque against the hand for the whole of the script's
    /// window.
    ///
    /// This is the gate on nine servos taking torque ([`wants_up`] is its only
    /// reader), so clearing it on anything other than a new ask re-enables
    /// exactly that cycling.
    raise_answered_for: Option<u64>,
    /// The run of pre-torque sweeps that are failing, if any are.
    sweeps: SweepRun,
}

/// A run of pre-torque sweeps that stopped answering.
///
/// A dead bus fails a sweep every `rest_poll` for as long as it is dead, so
/// what is reported is the run and not the sweep: the first failure is
/// narrated, evented and alerted, the ones behind it are counted, and the sweep
/// that finally answers says how many there were.
#[derive(Debug, Default)]
struct SweepRun {
    /// Failed sweeps since the last one that answered.
    failures: u64,
}

impl SweepRun {
    /// Note a sweep that failed, and answer whether it opened the run.
    fn failed(&mut self) -> bool {
        self.failures += 1;
        self.failures == 1
    }

    /// Note a sweep that answered, and answer how many failures it ended — or
    /// `None` when nothing was failing, which is the ordinary case and says
    /// nothing.
    fn recovered(&mut self) -> Option<u64> {
        (self.failures > 0).then(|| std::mem::take(&mut self.failures))
    }
}

/// Why a phase handed control back.
///
/// Both endings leave the machine limp. What differs is whether the daemon put
/// it there deliberately, and therefore whether the process exits or parks.
///
/// Not every ending under torque is one of these: a condition the machine
/// recovers from by itself — an obstruction met, a plan of ours the tick would
/// not run — winds the head down and hands the loop back to Resting, which is
/// an `Ok` and not an ending at all.
#[derive(Debug)]
enum Ending {
    /// Something asked the daemon to stop, and the machine has been released.
    Stopped(Stop),
    /// The machine stopped taking commands, and nothing engages it again until
    /// an operator has been. Torque has been written off; the daemon parks
    /// holding the port, carrying the session's own record of what happened.
    Faulted(FaultStage, Refusal, Vec<Entry>),
}

/// Whether a controlled stow is still worth commanding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fold {
    /// The head is somewhere a stow would bring it down from, and nothing has
    /// tried yet.
    Owed,
    /// Already folded, or a fold has just been defeated. Nothing controlled is
    /// attempted twice: a machine that would not stow once is not asked again,
    /// it is released.
    Spent,
}

impl Fold {
    /// What a machine holding `posture` still owes.
    ///
    /// Named for the question rather than as a conversion: a `From` impl on an
    /// `Option<Posture>` would read as a spelling of the posture rather than as
    /// the judgement about the fold that it is.
    fn owed_by(posture: Option<Posture>) -> Self {
        if posture == Some(Posture::Stow) {
            Self::Spent
        } else {
            Self::Owed
        }
    }
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
///
/// Every phase this passes through is published to `surface`, best-effort and
/// after the fact, so something outside the process can tell a resting daemon
/// from a parked one. Nothing here waits on it or checks whether it worked.
pub fn run<R: Rest>(
    machine: R,
    shared: &Shared,
    timing: Timing,
    sink: &dyn Sink,
    surface: &Surface,
) -> Outcome {
    let outcome = cycle(machine, shared, timing, sink, surface);
    // Last, and after every ending: the bus thread keeps the attachment up until
    // this is set, so a fault taken during the ending still has somewhere to
    // send its alert.
    shared.end_motion();
    outcome
}

/// The loop itself. Separate from [`run`] only so every one of its endings goes
/// through the one place that notes the machine is no longer being touched.
fn cycle<R: Rest>(
    mut machine: R,
    shared: &Shared,
    timing: Timing,
    sink: &dyn Sink,
    surface: &Surface,
) -> Outcome {
    let mut watch = Watch::default();
    match phases(&mut machine, shared, timing, &mut watch, sink, surface) {
        Ending::Stopped(stop) => {
            sink.line(&format!(
                "stopped on {stop}: the machine is at rest, torque off"
            ));
            Outcome::Released(stop)
        }
        Ending::Faulted(stage, refusal, record) => {
            park(machine, shared, stage, refusal, record, sink, surface)
        }
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
    surface: &Surface,
) -> Ending {
    // Where the machine is standing follows the loop from phase to phase: the
    // resting watch measures it, a release reports it, and an engage plans the
    // first move of the next turn from it. Assuming it instead is how a head
    // gets released from wherever it happens to be standing.
    let mut standing = match normalise(machine, shared, timing, watch, sink, surface) {
        Ok(standing) => standing,
        Err(ending) => return ending,
    };
    loop {
        standing = match resting(machine, shared, timing, watch, sink, surface, standing) {
            Ok(standing) => standing,
            Err(ending) => return ending,
        };
        standing = match active(machine, shared, timing, watch, sink, surface, standing) {
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
///
/// The look that precedes the fold is retried rather than faulted: a daemon
/// that came up over a flaky bus is a limp machine over a flaky bus, which is
/// nobody's hazard, and this is the unattended-at-boot case where parking would
/// cost the most. Everything from the moment torque goes on keeps its fault
/// handling.
fn normalise<R: Rest>(
    machine: &mut R,
    shared: &Shared,
    timing: Timing,
    watch: &mut Watch,
    sink: &dyn Sink,
    surface: &Surface,
) -> Result<Standing, Ending> {
    let standing = loop {
        if let Some(standing) = watched_sweep(machine, shared, watch, sink, surface) {
            break standing;
        }
        if let Some(stop) = shared.stopping() {
            // Nothing was ever taken hold of, so there is nothing to fold and
            // nothing to release: the machine is limp where the bus left it.
            surface.phase(Phase::Stopping, sink);
            return Err(Ending::Stopped(stop));
        }
        thread::sleep(timing.rest_poll);
    };
    sink.event(
        "motion_startup",
        &json!({ "at_stow": standing == Standing::AtStow }),
    );
    if standing == Standing::AtStow {
        sink.line("startup: the machine is folded already; leaving it limp");
        return Ok(standing);
    }

    sink.line("startup: the machine is not at stow. taking hold to fold it, then letting go.");
    let (mut head, mut incident) = match take_hold(machine, shared, watch, sink, surface) {
        Ok(held) => held,
        // Limp and crooked is still limp: the gate wrote nothing, the machine
        // is at no more risk than it was, and the next script's engage plans
        // from wherever it is standing — which is what the answer says.
        Err(EngageFailed::Gate(_)) => return Ok(Standing::Elsewhere),
        // The engage path takes the machine limp on its way out, so there is no
        // maneuver left to run: what is left is where the ending leaves the
        // daemon.
        Err(EngageFailed::Fault(refusal)) => {
            let mut unheld = Incident::unrecorded();
            return already_limp(
                refusal,
                &mut Responding {
                    incident: &mut unheld,
                    stage: FaultStage::Engage,
                    shared,
                    sink,
                    surface,
                },
            );
        }
    };
    let mut ctx = Responding {
        incident: &mut incident,
        stage: FaultStage::Startup,
        shared,
        sink,
        surface,
    };
    started(sink, Base::Unknown, Posture::Stow, "startup");
    // The measured move, not the stream: normalisation is one sequence with one
    // ending, and a machine whose pose nobody has ever commanded is not the one
    // to start splicing trajectories on. A script that lands during the fold is
    // executed by the loop this returns into, from a known pose.
    let folded = head.move_to(Posture::Stow, &mut |text| sink.line(text), &mut |event| {
        ticked(event, shared, sink, surface)
    });
    if let Err(refusal) = folded {
        // The fold that just failed *was* the controlled stow: it is not asked
        // for twice.
        return respond(head, Fold::Spent, refusal, &mut ctx);
    }
    reached(sink, Posture::Stow);
    fold_and_rest(head, Some("startup"), &mut ctx)
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
    surface: &Surface,
    entered: Standing,
) -> Result<Standing, Ending> {
    sink.line("resting: torque off, the port held, watching the machine");
    sink.event(
        "motion_resting",
        &json!({ "poll_ms": millis(timing.rest_poll) }),
    );
    surface.phase(Phase::Resting, sink);
    // The last word on the session that just ended: a pair taken out of service
    // during a wind-down is the case the motion loop's own passes cannot cover,
    // and it stands until the next engage retries the antennas.
    state_antennas(shared, sink, surface);
    let mut standing = entered;
    loop {
        if let Some(stop) = shared.stopping() {
            // Nothing to command and nothing to release: the machine is already
            // where every ending is trying to get it.
            surface.phase(Phase::Stopping, sink);
            return Err(Ending::Stopped(stop));
        }
        if wants_up(shared, watch) {
            return Ok(standing);
        }
        // A sweep that failed leaves the last one's answer standing: where the
        // machine was found is still the best thing known about it, and a
        // machine nobody is commanding does not move by itself. What a hand
        // does to it while the bus is down is what the sweep after the recovery
        // is for.
        if let Some(seen) = watched_sweep(machine, shared, watch, sink, surface) {
            standing = seen;
        }
        thread::sleep(timing.rest_poll);
    }
}

/// One sweep of a limp machine, where a failure is an expected error.
///
/// Never an ending. The machine here has no torque on it — it is at the minimum
/// risk condition already — so a sweep that stops answering costs the daemon
/// its picture of the machine and nothing else: no fault response would make a
/// limp machine safer, and parking on it would forfeit the recovery that
/// arrives free with the next sweep that answers. `None` says the sweep failed
/// and the caller keeps whatever it knew before.
///
/// Reported on the edges of a failure run, because a dead bus fails a sweep
/// every `rest_poll` for as long as it is dead and a line per sweep would bury
/// the run that matters underneath itself.
fn watched_sweep<R: Rest>(
    machine: &mut R,
    shared: &Shared,
    watch: &mut Watch,
    sink: &dyn Sink,
    surface: &Surface,
) -> Option<Standing> {
    match machine.watch(&mut |text| sink.line(text)) {
        Ok(standing) => {
            watch_answered(watch, shared, sink, surface);
            Some(standing)
        }
        Err(refusal) => {
            if watch.sweeps.failed() {
                surface.watching(Watching::Failing, sink);
                sink.line(&format!(
                    "the watch cannot read the machine: {refusal}. torque is off and stays off, \
                     so nothing is at risk; sweeping on, and no script will raise the head until \
                     a sweep answers."
                ));
                sink.event(
                    "resting_watch_lost",
                    &json!({ "detail": refusal.to_string() }),
                );
                shared.note_watch_lost(refusal.to_string());
            }
            None
        }
    }
}

/// Close a failure run because the machine answered, wherever the sweep that
/// answered happened to be taken.
///
/// The resting watch is not the only pre-torque sweep: an engage past a stale
/// posture takes a remedial one of its own, and torque only goes on once it has
/// answered. A run left open by that sweep would leave the state file saying
/// the daemon cannot read a machine it has just measured and put the head up
/// on — for the whole of a session, since nothing else looks until the release
/// — and the probe reading it would call a working robot degraded.
fn watch_answered(watch: &mut Watch, shared: &Shared, sink: &dyn Sink, surface: &Surface) {
    let Some(failures) = watch.sweeps.recovered() else {
        return;
    };
    surface.watching(Watching::Ok, sink);
    sink.line(&format!(
        "the watch is reading again after {failures} failed sweep(s); the head can take hold once \
         more"
    ));
    sink.event("resting_watch_restored", &json!({ "failures": failures }));
    shared.note_watch_restored();
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
    ) && watch.raise_answered_for != shared.accepted_seq()
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
    surface: &Surface,
    standing: Standing,
) -> Result<Standing, Ending> {
    let (mut head, mut incident) = match take_hold(machine, shared, watch, sink, surface) {
        Ok(held) => held,
        Err(EngageFailed::Gate(_)) => return Ok(standing),
        Err(EngageFailed::Fault(refusal)) => {
            let mut unheld = Incident::unrecorded();
            return already_limp(
                refusal,
                &mut Responding {
                    incident: &mut unheld,
                    stage: FaultStage::Engage,
                    shared,
                    sink,
                    surface,
                },
            );
        }
    };
    // Said once torque is on and not before: a refused engage leaves the machine
    // resting, and a surface that had already claimed active would have to be
    // taken back.
    surface.phase(Phase::Active, sink);
    // Everything a response to an ending from this session needs. Built once and
    // handed whole to whatever answers, so where the state line or the record is
    // written is decided by what the response does.
    let mut ctx = Responding {
        incident: &mut incident,
        stage: FaultStage::Motion,
        shared,
        sink,
        surface,
    };

    // Engaging pins the machine where the resting watch found it, so the base
    // state the loop starts from is that measurement and not an assumption. A
    // machine found standing is at no posture at all — no desired posture
    // equals it, so the first pass commands the fold rather than skipping it
    // and releasing a head from wherever it happens to be.
    let mut base = match standing {
        Standing::AtStow => Base::At(Posture::Stow),
        Standing::Elsewhere => Base::Unknown,
    };
    // When the head reached stow with nothing else to do, which is what the
    // rest delay is measured from.
    let mut settled: Option<Instant> = None;
    // The overlays playing over that base, across passes: a base command that
    // changes ends the streamed run, and the players have to survive it —
    // rebuilding them would ramp a full-weight overlay back up from zero.
    let mut players = Overlays::none();

    loop {
        if let Some(stop) = shared.stopping() {
            watch.dwells.flush(&mut |text| sink.line(text));
            surface.phase(Phase::Stopping, sink);
            return Err(release_for(head, base, stop, &mut ctx));
        }

        // Which script this pass is answering, read before the schedule it
        // plans from. The bus thread writes both while this thread works, and a
        // pass that ends in a refusal marks the script it answered: read at
        // response time instead, a replacement landing in between would be
        // marked answered for a plan it never asked for and would never raise
        // the head. Erring older is the safe side — a script marked one too
        // early is tried again, one marked too late is lost.
        let asking = shared.accepted_seq();
        let (opening, ask, playing) = composing(shared, watch, sink);
        // Whatever this pass does next, players belonging to a script that is
        // no longer running are gone as of this read. Here rather than in the
        // streamed pass alone: a replacement or a lapse that opens no window of
        // its own never reaches the pass, and the motion it cut short would go
        // unsaid or be blamed on whichever script eventually played something.
        players.forget(playing.seq, sink);
        // Where the base stood as this pass began, which is where a drive this
        // pass starts is leaving from. Taken before the ask is read: a fresh
        // base command drops the record of the travel it interrupted, and the
        // drive it interrupted is exactly what its own narration owes.
        let entering = base;
        let (desired, reason) = match ask {
            Some(Ask::Posture(wanted, reason)) => {
                base = base.without_travel();
                (Some(wanted), reason)
            }
            // `keep` asks for no move at all, whatever the machine is doing and
            // whatever its posture is known to be. An unknown posture holds as
            // an unknown posture: folding it here would answer the one base
            // command that means "do not move the base" with a stow.
            Some(Ask::Hold) => {
                // A `keep` landing on a base that was still travelling is the
                // freeze, and it is said here — before the ordinary keep line,
                // which the freeze counts as having said.
                match base {
                    Base::Between { toward } => froze(shared, watch, sink, toward),
                    _ => said_keep(shared, watch, sink),
                }
                base = base.without_travel();
                (base.at(), "script")
            }
            // Nothing asks for a change, and a machine whose posture is unknown
            // still has to be folded: the fold is the change.
            None => {
                base = base.without_travel();
                (Some(base.at().unwrap_or(Posture::Stow)), "script")
            }
        };

        let plan = match desired.filter(|wanted| Some(*wanted) != base.at()) {
            Some(wanted) => BasePlan::To(wanted),
            None => BasePlan::Held,
        };
        // Base work of any kind takes the pass: the loop runs at the machine's
        // own control period, composing the base and whatever players are open
        // into one setpoint each period, and the dwell regime is what it comes
        // back to. One arm for every scripted drive, so a window opening while
        // the head is on its way somewhere is picked up by the run's own
        // per-period sync rather than waiting out a move.
        if players.wants(&playing) || plan != BasePlan::Held {
            if plan != BasePlan::Held {
                watch.dwells.flush(&mut |text| sink.line(text));
            }
            settled = None;
            let leaving = entering;
            // Said on the period the first setpoint goes out, in the words that
            // are true then: a run that starts bare and is joined by a clip
            // later is one base transition, narrated once, with the join
            // reported by the overlay sync.
            let mut say = |sink: &dyn Sink, composing: bool| {
                if let BasePlan::To(wanted) = plan {
                    if composing {
                        sink.line(&format!(
                            "motion: {} -> {wanted}, under a motion",
                            leaving.leaving()
                        ));
                    } else {
                        sink.line(&format!("motion: {} -> {wanted}", leaving.leaving()));
                    }
                    started(sink, leaving, wanted, reason);
                }
            };
            let pass = Pass {
                plan,
                opening,
                say: &mut say,
            };
            let mut refused = false;
            match overlaid(&mut head, pass, &mut players, shared, sink, surface) {
                Ok(Composed::Arrived(arrived)) => {
                    base = Base::At(arrived);
                    reached(sink, arrived);
                }
                Ok(Composed::Refused { moved }) => {
                    refused = true;
                    // Nowhere nameable, and no travel recorded either: a `keep`
                    // after a refusal holds silently rather than reporting a
                    // freeze for a drive the machine ended. A run refused
                    // before it commanded anything moved nothing, so the base
                    // is where the pass found it.
                    if moved {
                        base = Base::Unknown;
                    }
                }
                Ok(Composed::Travelling) => {
                    // The head is between two postures, which is nowhere the
                    // loop can name. The target it was carried toward is kept
                    // so that a `keep` reaching the next pass can say what it
                    // abandoned.
                    if let BasePlan::To(wanted) = plan {
                        base = Base::Between { toward: wanted };
                    }
                }
                // Empty: this pass moved nothing and must not forget the base.
                Ok(Composed::Untouched) => {}
                Err(refusal) => {
                    script_answered(watch, asking);
                    return respond(head, Fold::Owed, refusal, &mut ctx);
                }
            }
            // A pass whose layering was refused falls through to the dwell
            // instead of starting the next run at once. The base command under
            // those overlays is still the base command, so an immediate retry
            // is the same plan re-offered as fast as the loop can build it — a
            // hot spin, with nothing on the console to say how often it is
            // happening. The dwell paces the bare re-acquisition and costs one
            // pass.
            if !refused {
                continue;
            }
        }

        // Folded with nothing asking otherwise: hold for the rest delay, so a
        // quick follow-up costs no release and no engage, and then let go.
        let mut until = None;
        if base == Base::At(Posture::Stow) {
            let since = *settled.get_or_insert_with(Instant::now);
            let ends_at = since + timing.rest_delay;
            if Instant::now() >= ends_at {
                ctx.stage = FaultStage::Release;
                return fold_and_rest(head, Some(reason), &mut ctx);
            }
            until = Some(ends_at);
        }

        let held = head.hold(dwell_for(shared, timing.dwell, until), &mut |event| {
            ticked(event, shared, sink, surface);
            watch.dwells.observe(&event);
        });
        watch.dwells.end_dwell(&mut |text| sink.line(text));
        if let Err(refusal) = held {
            watch.dwells.flush(&mut |text| sink.line(text));
            script_answered(watch, asking);
            return respond(head, Fold::owed_by(base.at()), refusal, &mut ctx);
        }
    }
}

/// Where the loop believes the base is, between passes.
///
/// One value rather than a posture beside a travel record: "at stow" and
/// "somewhere on the way to stow" are the same fact answered two ways, and a
/// pair of locals holding them can be written into the combination that says
/// both at once. Every ending a pass has produces exactly one of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Base {
    /// Standing at a posture the loop has seen it reach.
    At(Posture),
    /// Between two postures, carried toward `toward` by a run that ended short
    /// of it. Only a `keep` reads the target, and only to say what it froze:
    /// neither posture names where the head stands.
    Between { toward: Posture },
    /// Nowhere the loop can name, and nothing to say about where it was going.
    /// A machine found standing at engage, and a base whose run the machine
    /// refused part way.
    Unknown,
}

impl Base {
    /// The posture the head is standing at, if the loop knows one.
    fn at(self) -> Option<Posture> {
        match self {
            Self::At(posture) => Some(posture),
            Self::Between { .. } | Self::Unknown => None,
        }
    }

    /// The same base with any record of a travel in progress dropped.
    ///
    /// What a fresh base command leaves behind it: the target an earlier run
    /// abandoned is only ever read by the `keep` that lands on it, and one that
    /// lands after any other ask is a keep about a drive nothing is still
    /// carrying.
    fn without_travel(self) -> Self {
        match self {
            Self::Between { .. } => Self::Unknown,
            other => other,
        }
    }

    /// This base in the narration's words, as the place a move is leaving.
    fn leaving(self) -> String {
        match self {
            Self::At(posture) => posture.as_str().to_owned(),
            Self::Between { toward } => format!("wherever it was left toward {toward}"),
            Self::Unknown => "wherever it was left".to_owned(),
        }
    }
}

/// What a streamed pass left the base as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Composed {
    /// The base reached the posture the plan named, with the overlays riding
    /// it.
    Arrived(Posture),
    /// The run ended with the base still travelling: the head is between two
    /// postures and the next pass commands from where it stands.
    Travelling,
    /// The machine would not take what the pass composed over the base. The
    /// overlays are dropped and the head is where the last accepted period put
    /// it; `moved` says whether there was one, because a run refused before it
    /// commanded anything left the posture as true as it found it.
    ///
    /// Layered refusals only: a refused setpoint no overlay was riding comes
    /// back as an `Err` and ends the session.
    ///
    /// No arrival survives this ending. A run whose base has arrived and whose
    /// overlays have all faded ends on that period, so a refusal reached after
    /// arrival is one composed with an overlay still riding the base — and the
    /// machine is holding the posture plus that sample's delta, which is not
    /// the posture. Told apart from the other endings because a plan the
    /// machine has just refused is not one to re-offer on the next instruction.
    Refused { moved: bool },
    /// The run ended before it commanded a single setpoint. The head has not
    /// moved, so whatever the loop knew about its posture is still true.
    Untouched,
}

/// What the loop decided about one streamed pass before it began.
struct Pass<'a> {
    /// What the base does for the length of the run.
    plan: BasePlan,
    /// The schedule read `plan` was decided from. Handed in rather than read
    /// again: a replacement landing between the two reads would leave the run's
    /// per-period guard comparing the new script's answer against a plan built
    /// from the old one, and the base would carry on toward a posture nothing
    /// was asking for any more. The run is right for as long as this stands.
    opening: Desired,
    /// The narration a base transition owes, said on the period the first
    /// setpoint goes out. A run can end before it commands anything, and a
    /// transition line for a head that never moved is a transition that did not
    /// happen.
    ///
    /// The flag says whether anything is playing over the base on that period,
    /// which is what decides the wording: the run is asked rather than the
    /// players, because the players are borrowed by the composition when the
    /// question is put.
    say: &'a mut dyn FnMut(&dyn Sink, bool),
}

/// Run one pass at the machine's control period: the base under the pass's
/// plan, the overlays composed on top of it, one setpoint per period.
///
/// The run ends the moment the schedule's base answer changes, so the loop
/// decides what a new base command means in the one place that decides it for
/// every other pass.
///
/// A refused setpoint an overlay was riding drops this script's overlays and
/// nothing else: the experiment that failed is the layering, so the base
/// survives it and the loop re-acquires it on a later pass, after a dwell.
/// A refused setpoint the base was carrying alone ends the session instead —
/// the rejection comes back as an `Err`, and the caller winds the machine down
/// to rest under control. Both are plans of ours the tick refused, so neither
/// parks anything and neither latches anything.
fn overlaid<A: Active>(
    head: &mut A,
    pass: Pass<'_>,
    players: &mut Overlays,
    shared: &Shared,
    sink: &dyn Sink,
    surface: &Surface,
) -> Result<Composed, Refusal> {
    let Pass { plan, opening, say } = pass;
    // Where the base had got to on the period this closure last saw.
    let observed = Cell::new(false);
    // Whether the setpoint most recently offered carried an overlay sample.
    // What decides who a refusal belongs to, and the refused setpoint is the
    // last one offered: a bare setpoint is a bare base drive whatever the run
    // played earlier, and the tick's answer to it says nothing about any clip.
    let layered = Cell::new(false);
    // Setpoints offered, not setpoints taken: the machine's answer to the last
    // one is the run's ending, and it is read afterwards.
    let offers = Cell::new(0_u32);
    let outcome = head.stream(
        plan,
        &mut |text| sink.line(text),
        &mut |event| ticked(event, shared, sink, surface),
        &mut |base| {
            observed.set(base.arrived);
            if shared.stopping().is_some() {
                return None;
            }
            let (desired, playing) = shared.composing(Instant::now());
            // A schedule with nothing due of its own — a replacement whose
            // first step is still in the future — is the absence of an
            // instruction, and whatever the machine is doing stands. Only a
            // schedule that asks for something different diverts the run.
            if desired != opening && desired != Desired::Unchanged {
                return None;
            }
            players.sync(&playing, sink);
            // Nothing left to say: every overlay has faded and the base has
            // stopped moving. The machine holds what this period gave it and
            // the loop goes back to its dwells.
            if players.is_empty() && base.arrived {
                return None;
            }
            if offers.get() == 0 {
                say(sink, !players.is_empty());
            }
            offers.set(offers.get() + 1);
            let samples = players.advance(base.period);
            layered.set(!samples.is_empty());
            Some(compose(base.targets, samples))
        },
    )?;
    let refused = matches!(outcome, Streamed::Refused(_));
    if let Streamed::Refused(why) = outcome {
        // Whose setpoint the machine refused decides who hears about it. A
        // refused setpoint an overlay rode is a layering, and the script's
        // overlays are dropped whole because the same clip over the same base
        // composes the same setpoint. A refused setpoint no overlay rode is the
        // bare base — including one offered after the run's windows have all
        // spent: there is nothing to drop, and reporting it as a dropped overlay
        // would name a layering that does not exist and stop the script's later
        // windows from ever playing.
        //
        // Dropping the overlays changes the next offer, which is what makes
        // re-acquisition a recovery. A bare plan has nothing left to change:
        // the rejection is arithmetic over that plan and the measured pose, so
        // re-offering it gets the same answer, and a loop that kept re-offering
        // would hold the head torqued in place against a defect for as long as
        // the script asked for that posture. The session ends instead.
        if layered.get() {
            players.refuse(&why, sink);
        } else {
            base_refused(sink, plan, &why);
            return Err(Refusal::from(PumpError::Rejected(why)));
        }
    }
    Ok(match plan {
        _ if refused => Composed::Refused {
            // The refused offer is not one of them, so a run whose very first
            // composed setpoint the machine would not take moved nothing.
            moved: offers.get() > 1,
        },
        _ if offers.get() == 0 => Composed::Untouched,
        BasePlan::To(posture) if observed.get() => Composed::Arrived(posture),
        _ => Composed::Travelling,
    })
}

/// Mark `asking` — the script the pass planned from — as answered, so a
/// wind-down back to Resting does not raise the head again for the script that
/// has just failed.
///
/// The same mark a refused engage leaves, and for the same reason: a condition
/// that ended one raise will end the next one, and without this the loop would
/// re-engage at the top of every pass for as long as the script asks for the
/// head up — a machine cycling torque on and off against a hand instead of
/// waiting for the ask to change.
///
/// The number is the pass's own, taken when it read its schedule, rather than
/// whatever is accepted now: everything between that read and this call — a
/// streamed run, and a response that takes seconds — is time a replacement can
/// land in, and any script this pass never planned from is a fresh ask that
/// has to be tried.
fn script_answered(watch: &mut Watch, asking: Option<u64>) {
    watch.raise_answered_for = asking;
}

/// Pin the machine where it stands and enable torque, timing it and saying so.
///
/// The engage wall clock is on every engage because a wake word is supposed to
/// reach the servos in tens of milliseconds, and a capture is where that
/// number is read off.
///
/// An engage that succeeded measured the machine, whether it took a remedial
/// sweep of its own or found the resting watch's last one still current, so it
/// is also where a failure run can end.
fn take_hold<'e, R: Rest>(
    machine: &'e mut R,
    shared: &Shared,
    watch: &mut Watch,
    sink: &dyn Sink,
    surface: &Surface,
) -> Result<(R::Active<'e>, Incident), EngageFailed> {
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
        Ok(held) => {
            sink.line(&format!("engaged: torque on, {took} ms"));
            sink.event("motion_engaged", &json!({ "ms": took }));
            watch_answered(watch, shared, sink, surface);
            // Judged here because this is where a session begins: the health
            // gate lets a machine engage on antenna bits alone rather than
            // refusing the wake over them, so a session can start with the pair
            // already out of service and no event will ever announce it. Stated
            // in both directions, so the engage that gets both antennas back
            // says so.
            let out = held.0.out_of_service();
            antennas_now(
                antennas_of(out),
                &format!("out of service when torque went on: {out}"),
                shared,
                sink,
            );
            state_antennas(shared, sink, surface);
            Ok(held)
        }
        Err(EngageFailed::Gate(refusal)) => {
            // Not a fault: nothing was written, so there is nothing to undo and
            // nothing to park for. It is still worth waking somebody up about —
            // a machine that cannot take torque is a machine that will not
            // answer the next wake word either.
            watch.raise_answered_for = asking;
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

/// What the schedule asks of the machine now — the read itself, the ask it
/// becomes (`None` when nothing asks for a change), and the overlays the same
/// read found open.
///
/// The read comes back with the answer because the pass acts on it over several
/// periods and has to know *which* read its plan came from: a second read of
/// its own could be a different script's answer. All three from one read for the
/// same reason: a replacement landing between a base read and an overlay read
/// would have the pass enter a streamed run on the new script's windows with a
/// plan built from the old script's base — a run that ends on its own first
/// period having commanded nothing, after saying it was moving the head.
///
/// The lapse is said once per script rather than once per boundary: expiry is
/// the answer from the moment it happens until another script lands, and so is
/// a `keep`.
fn composing(
    shared: &Shared,
    watch: &mut Watch,
    sink: &dyn Sink,
) -> (Desired, Option<Ask>, Overlaid) {
    let (desired, playing) = shared.composing(Instant::now());
    (desired, asked(desired, shared, watch, sink), playing)
}

/// The ask `desired` becomes, with the reporting a change of answer owes.
fn asked(desired: Desired, shared: &Shared, watch: &mut Watch, sink: &dyn Sink) -> Option<Ask> {
    match desired {
        Desired::Unchanged => {
            watch.keeping = None;
            None
        }
        // `keep` commands no move and changes no posture state: the base stays
        // where it is, known posture or not.
        //
        // Said nowhere here: a keep on a head already at a posture holds what
        // it holds, and a keep on a base still travelling froze a drive
        // between two postures. Those are different statements and the loop
        // says whichever is true of the pass it is on.
        Desired::Keep => Some(Ask::Hold),
        Desired::Posture(wanted) => {
            watch.keeping = None;
            Some(Ask::Posture(wanted, "script"))
        }
        Desired::Expired => {
            watch.keeping = None;
            let seq = shared.accepted_seq();
            if watch.lapse_reported != seq {
                watch.lapse_reported = seq;
                watch.dwells.flush(&mut |text| sink.line(text));
                sink.line("script: lapsed; folding the head and going back to rest");
                sink.event("motion_script_expired", &json!({ "seq": seq }));
            }
            Some(Ask::Posture(Posture::Stow, "timeout"))
        }
    }
}

/// Say that a `keep` is the live base command, once per transition into it.
///
/// Called from the loop's hold arm alone, which is the one place the statement
/// is true: the head is holding what it holds, no move was issued, and none
/// will be while this answer stands.
fn said_keep(shared: &Shared, watch: &mut Watch, sink: &dyn Sink) {
    let seq = shared.accepted_seq();
    if watch.keeping == seq {
        return;
    }
    watch.keeping = seq;
    watch.dwells.flush(&mut |text| sink.line(text));
    sink.line("script: keep; the base holds where it is");
    sink.event("motion_keep", &json!({ "seq": seq }));
}

/// Say that a `keep` stopped a base drive where it had got to.
///
/// The freeze's own line and event rather than a posture: the head is between
/// `in_flight` and wherever it started, so there is no state to report reaching
/// and `motion_posture` keeps its two-value domain. Counts as this script's
/// keep having been said, so the hold arm does not say it a second time on the
/// next boundary.
///
/// The physical freeze is not this function's: the run ended the moment the
/// schedule's answer changed, and the pass that says this plans a held base
/// from where the head stands.
fn froze(shared: &Shared, watch: &mut Watch, sink: &dyn Sink, in_flight: Posture) {
    watch.keeping = shared.accepted_seq();
    watch.dwells.flush(&mut |text| sink.line(text));
    sink.line(&format!(
        "motion: {in_flight} abandoned mid-move; keep holds the base where it is"
    ));
    sink.event(
        "motion_keep_froze",
        &json!({
            "seq": shared.accepted_seq(),
            "abandoned": in_flight.as_str(),
        }),
    );
}

/// What the schedule asks of the machine, when it asks for anything.
///
/// Two answers rather than one, because "hold what you are doing" and "nothing
/// was asked" are different asks with different consequences and only one of
/// them folds a machine whose posture is unknown.
enum Ask {
    /// Carry the head to this posture, for this reason.
    Posture(Posture, &'static str),
    /// Command nothing. What the machine is holding is what it holds.
    Hold,
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
/// A head between two postures is at neither, so `from` is null for it and the
/// target it was carried toward is stated separately. That pair is what makes a
/// turnaround readable off a capture: which drive the new one interrupted is the
/// question an incident asks of a script that changed its mind.
fn started(sink: &dyn Sink, from: Base, to: Posture, reason: &str) {
    let toward = match from {
        Base::Between { toward } => Some(toward.as_str()),
        Base::At(_) | Base::Unknown => None,
    };
    sink.event(
        "motion_move",
        &json!({
            "from": from.at().map(Posture::as_str),
            "from_toward": toward,
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

/// Say that the machine would not take a base plan no overlay was riding.
///
/// The bare half of the streamed refusal, and its own line and event because it
/// is a different fact for an operator: the trajectory the daemon planned for
/// the base is what the tick rejected, so the clip library and the compositor
/// have nothing to answer for. Classified by the same reason and joint as an
/// overlay drop, so a session's refusals aggregate together whichever half
/// produced them. Said before the ending it belongs to; this line carries only
/// which plan and that the base rather than the layering is what the machine
/// would not take — the rejection detail travels with the ending itself.
fn base_refused(sink: &dyn Sink, plan: BasePlan, why: &CommandRejection) {
    let plan = match plan {
        BasePlan::To(posture) => posture.as_str(),
        BasePlan::Held => "held",
    };
    sink.line(&format!(
        "motion: the machine would not take the base ({plan}). winding down."
    ));
    sink.event(
        "motion_base_refused",
        &json!({
            "plan": plan,
            "reason": overlay::reason(why),
            "joint": overlay::joint(why).map(|joint| joint.to_string()),
            "detail": why.to_string(),
        }),
    );
}

/// What a move or a dwell says while it runs, as this daemon records it beyond
/// the words.
///
/// Two events are worth more than their line, and both of them reach the console
/// already: a move narrates every event the library reports, and a dwell's go
/// through [`DwellNarration`]. What is added here is the part prose cannot carry.
///
/// A clock too short for the span the move actually covers is right-sized before
/// the move is commanded — the head travels slower than the configuration asked
/// for rather than stepping past the guard and dropping — and the pair of
/// durations is the only sign that a configured value never fitted the case it
/// met. It is its own capture line rather than a field on [`started`]'s: that one
/// is emitted before the move is commanded, because its timestamp is what a
/// capture measures wake-to-motion against, and the stretch is not known until
/// the command is accepted. An antenna clock lengthened to part the pair at their
/// crossing arrives the same way and is recorded with the same fields, `dephased`
/// telling the two apart: one says a configured value never fitted its span, the
/// other says the pair as configured would have swept mirrored through the
/// contact band.
///
/// The antennas leaving the moves is the other one, and it is a change of what
/// the machine can do rather than a remark about a clock: it goes into the cell
/// every surface answers from, and the flip into degraded is what the capture and
/// the operator's alert are keyed on. The state file is written from here, where
/// the change happens, so a probe reads it as soon as the pair leaves the moves
/// rather than whenever the loop next passes a boundary holding a surface.
fn ticked(event: TickEvent, shared: &Shared, sink: &dyn Sink, surface: &Surface) {
    if let TickEvent::AntennasDegraded(fault) = event {
        antennas_now(Antennas::Degraded, &fault.to_string(), shared, sink);
        state_antennas(shared, sink, surface);
    }
    if let TickEvent::Stretched(stretch) = event {
        sink.event(
            "motion_clock_stretched",
            &json!({
                "requested_head_s": stretch.requested.head.as_secs_f64(),
                "effective_head_s": stretch.effective.head.as_secs_f64(),
                "requested_antennas_s": secs_each(stretch.requested.antennas),
                "effective_antennas_s": secs_each(stretch.effective.antennas),
                "dephased": stretch.dephased,
                "separation_rad": stretch.separation.map(|pair| pair.offset),
                // What that separation was judged against, because the bar is
                // configuration: the measurement alone says nothing about
                // whether the pair cleared it.
                "separation_required_rad": stretch.separation_required,
            }),
        );
    }
}

/// A pair of antenna clocks as the capture writes them: right then left, the
/// order every per-side value in this daemon and in the motion library takes.
fn secs_each(durations: [Duration; 2]) -> [f64; 2] {
    durations.map(|duration| duration.as_secs_f64())
}

/// Whether the antenna pair is being commanded, from the joints a session has
/// taken out of service.
///
/// The one predicate behind every degraded surface. Membership and not
/// emptiness: a head servo on its way to a park-class fault lands in the same
/// set, and painting the antennas degraded over it would send an operator to the
/// two joints that are fine. Asked of the group rather than of two named joints,
/// so which bus rows are antennas has one owner.
fn antennas_of(out_of_service: JointSet) -> Antennas {
    let degraded = JointGroup::Antennas
        .joints()
        .iter()
        .any(|antenna| out_of_service.contains(antenna));
    if degraded {
        Antennas::Degraded
    } else {
        Antennas::Ok
    }
}

/// Record what the antennas are doing, and say so once when they stop doing it.
///
/// The cell is the record: a delivered script, the state file and the alert all
/// answer from it, so there is one judgement rather than three that can drift
/// apart. What the flip into degraded adds is the capture line — the pair going
/// out of service is an event of the session — and the alert the bus thread
/// raises from the same cell. Both fire once per flip, whichever source flipped
/// it: a latch the machine carries across three wakes is one standing condition,
/// and the state file is what reports a standing condition.
///
/// No narration line: the tick's own words are already on the console, through a
/// move's narration or through a dwell's.
fn antennas_now(standing: Antennas, detail: &str, shared: &Shared, sink: &dyn Sink) {
    if !shared.note_antennas(standing, detail) {
        return;
    }
    sink.event("antennas_degraded", &json!({ "detail": detail }));
}

/// Mirror the antenna standing into the state file.
///
/// Taken from the cell and not judged again: a probe reading `antennas=` has to
/// be reading the same answer a delivered script gets, not a second judgement
/// taken on a different clock. Written wherever the standing can change — an
/// engage, the tick event that takes the pair out mid-session, the return to
/// rest, a park — and a write that says nothing new writes nothing.
fn state_antennas(shared: &Shared, sink: &dyn Sink, surface: &Surface) {
    surface.antennas(shared.antennas(), sink);
}

/// Fold the head if it is not folded, then release: the expected ending, and
/// the one every stop takes.
///
/// The stow is commanded while this thread still owns the port and the machine
/// still has torque, because a head released where it stands is a head that
/// falls the rest of the way. A refusal on the way down does not keep torque
/// on — the ending's own class decides what does come off and how, and a stop
/// that got the machine to rest is still the stop it was asked for.
fn release_for<A: Active>(mut head: A, base: Base, stop: Stop, ctx: &mut Responding) -> Ending {
    // Whatever the loop was doing, an ending from here is the shutdown's.
    ctx.stage = FaultStage::Shutdown;
    ctx.line(&format!("stopping on {stop}"));
    started(ctx.sink, base, Posture::Stow, "shutdown");
    // Issued whatever the loop believed the posture was. A scripted arrival is
    // trajectory-clock arithmetic and never a measurement, so "already at stow"
    // is a belief, and this is the one move that puts the machine at the
    // minimum risk condition with the measurement to prove it. From a head
    // genuinely there it settles at once. Nothing may divert it either: the
    // daemon is on its way out and the fold is the last thing it owes.
    let folded = head.move_to(
        Posture::Stow,
        &mut |text| ctx.sink.line(text),
        &mut |event| ticked(event, ctx.shared, ctx.sink, ctx.surface),
    );
    if let Err(refusal) = folded {
        // The fold that just failed was this ending's controlled stow, so
        // nothing asks for a second one.
        return match respond(head, Fold::Spent, refusal, ctx) {
            // Limp at rest is where the stop was trying to get the machine:
            // it arrived, by a route nobody asked for.
            Ok(_) => Ending::Stopped(stop),
            Err(ending) => ending,
        };
    }
    reached(ctx.sink, Posture::Stow);
    match head.disengage(&mut |text| ctx.sink.line(text)) {
        Ok(verdict) => {
            released(ctx.shared, ctx.sink, Some("shutdown"), verdict);
            Ending::Stopped(stop)
        }
        Err(refusal) => match already_limp(refusal, ctx) {
            Ok(_) => Ending::Stopped(stop),
            Err(ending) => ending,
        },
    }
}

/// Let torque go and answer `Ok`, so the caller goes back to resting.
///
/// `reason` is the script's word for why the head came down, carried onto the
/// released event: a stow step and a lapsed timeout end the same way and are
/// different things to read in a capture.
fn fold_and_rest<A: Active>(
    head: A,
    reason: Option<&str>,
    ctx: &mut Responding,
) -> Result<Standing, Ending> {
    match head.disengage(&mut |text| ctx.sink.line(text)) {
        Ok(verdict) => {
            released(ctx.shared, ctx.sink, reason, verdict);
            Ok(verdict.standing())
        }
        // Torque came off inside the library whatever this says, so there is no
        // maneuver left: what remains is whether the machine may be engaged
        // again.
        Err(refusal) => already_limp(refusal, ctx),
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

/// Everything a response needs that is the same at every step of it.
struct Responding<'a> {
    /// The session's record, which the response both reads and adds to.
    incident: &'a mut Incident,
    /// Where the daemon was when the ending arrived.
    stage: FaultStage,
    shared: &'a Shared,
    sink: &'a dyn Sink,
    /// The state file, so the antenna standing can be stated from wherever it
    /// changes rather than mirrored from somewhere that still holds a surface.
    surface: &'a Surface,
}

impl Responding<'_> {
    /// One narration line.
    fn line(&self, text: &str) {
        self.sink.line(text);
    }

    /// The record as an ending carries it: owned, because it outlives the
    /// session it came from.
    fn record(&mut self) -> Vec<Entry> {
        self.incident.entries().to_vec()
    }
}

/// Answer an ending that arrived with the machine holding torque, and say where
/// it leaves the daemon.
///
/// The one place an ending's class decides anything. Every response terminates
/// at torque off; what the class chooses between is how the head gets there —
/// stowed under control by motors that still command, stowed on what is left of
/// them, or limp on the spot where control is no longer trusted — and whether
/// the next script may ask again or a person has to look first. Nothing here
/// reads the wording of an error, and nothing outside here decides a response.
///
/// `fold` says whether a controlled stow is still owed: a head already folded,
/// or one whose fold has just been defeated, is released rather than commanded
/// again.
fn respond<A: Active>(
    mut head: A,
    fold: Fold,
    refusal: Refusal,
    ctx: &mut Responding,
) -> Result<Standing, Ending> {
    // Before the response, because the response is what the record is about: a
    // bus that stopped carrying is seen by the layer holding the wire, and this
    // is where the session's story gets the condition that ends it.
    let refusal = note_condition(&mut head, refusal);
    match refusal.class() {
        // The machine is healthy and still commanding: a declined ask, or a plan
        // of ours the tick would not run. It goes down the way every ordinary
        // ending goes down.
        ErrorClass::Refuse | ErrorClass::SlowStowToRest => {
            stow_and_release(head, fold, refusal, ctx)
        }
        ErrorClass::MaskedSlowStowToPark => {
            let deadline = head.stow_deadline();
            masked_stow(head, refusal, deadline, ctx)
        }
        ErrorClass::ImmediateAllTorqueOffToRest | ErrorClass::ImmediateAllTorqueOffToPark => {
            torque_off_now(head, refusal, ctx)
        }
    }
}

/// Put an ending's condition into the session's record, if it is not there
/// already, and say so on the ending.
///
/// Every ending this layer answers passes through here once. What is left for it
/// to do is narrow and load-bearing: the tick records what it raises, and the
/// library records what it consumes inside a call of its own — a release, a
/// masked wind-down — so the conditions that would otherwise reach no record at
/// all are the ones the layer holding the wire hands *back*, out of a move this
/// daemon commanded itself. A record missing the condition that ended the
/// incident is published under the name of whatever condition preceded it.
fn note_condition<A: Active>(head: &mut A, ending: Refusal) -> Refusal {
    match ending.unrecorded() {
        Some(fault) => {
            head.note(fault);
            ending.noted()
        }
        None => ending,
    }
}

/// Stow under control, then take the measured release.
///
/// The response for every ending that leaves the motors commanding. The stow is
/// commanded on the same live state the ending left — the tick abandoned its
/// move and holds at the last goal it emitted — so this is a wind-down and not a
/// fresh engage of a machine nobody has looked at. A hand pushing the head down
/// is met by this and not by going limp.
///
/// The release at the end of it is the orderly one, which is what makes this
/// worth commanding at all: it measures where the machine came to rest and says
/// so, and that measurement is what the next engage plans from.
///
/// The stow is bracketed into the session's record — started, then completed —
/// because this is the layer that commands it and so the only one that can say
/// how far it got. A record with the condition and no maneuver beside it is half
/// the story, on the one machine nobody is watching.
///
/// The maneuver also opens the clock every escalation of it shares: what is left
/// of one stow window is what a defeated stow gets, never a second window.
fn stow_and_release<A: Active>(
    mut head: A,
    fold: Fold,
    refusal: Refusal,
    ctx: &mut Responding,
) -> Result<Standing, Ending> {
    let deadline = head.stow_deadline();
    if fold == Fold::Owed {
        ctx.line(&format!(
            "winding down under control: {refusal}. the motors still command, so the head is \
             stowed rather than dropped."
        ));
        started(ctx.sink, Base::Unknown, Posture::Stow, "wind-down");
        // Whose maneuver this is belongs to the class, not to this caller — and
        // only if nothing has one open already: the ladder never begins a second
        // answer to a machine already being answered.
        let maneuver = head
            .open_maneuver()
            .is_none()
            .then(|| refusal.class().maneuver())
            .flatten();
        if let Some(maneuver) = maneuver {
            head.note_response(maneuver, TimelineOutcome::Started);
        }
        // Nothing may divert this one: it is the response to an ending, and a
        // script arriving mid-way does not get to turn a wind-down around.
        let folded = head.move_to(
            Posture::Stow,
            &mut |text| ctx.sink.line(text),
            &mut |event| ticked(event, ctx.shared, ctx.sink, ctx.surface),
        );
        if let Err(defeated) = folded {
            // A wire that stopped carrying under this stow is a condition of the
            // machine that nothing else will record: the tick did not raise it
            // and the library handed it back rather than consuming it. Noted
            // here or the incident is published under the condition the stow was
            // answering, which is over.
            let ended = note_condition(&mut head, refusal.and(defeated));
            // The maneuver stays open: whatever answers next is this one
            // expanding, and it is that answer which closes the entry.
            return defeated_stow(head, ended, deadline, ctx);
        }
        if let Some(maneuver) = maneuver {
            head.note_response(maneuver, TimelineOutcome::Completed);
        }
        reached(ctx.sink, Posture::Stow);
    }
    match head.disengage(&mut |text| ctx.sink.line(text)) {
        Ok(verdict) => {
            released(ctx.shared, ctx.sink, Some("wind-down"), verdict);
            let disposition = refusal.class().disposition();
            settled(disposition, refusal, verdict.standing(), ctx)
        }
        // Torque is off on every path out of `disengage`, so what is left is
        // what it had to report, and whether that latches. Nothing is noted from
        // here: the release consumed the engagement, and a condition found
        // inside a call that consumes it is recorded by that call — a torque-off
        // nobody acknowledged is already on this session's record by the time it
        // is handed back.
        Err(unacked) => {
            let ended = refusal.and(unacked);
            let disposition = ended.class().disposition();
            settled(disposition, ended, Standing::Elsewhere, ctx)
        }
    }
}

/// What a defeated wind-down becomes.
///
/// One escalation and never a second wind-down: a servo that dropped out is
/// already off and out of every check, so the stow carries on without it and the
/// ending latches; anything else that defeated the stow gets the immediate
/// release. Wildcard-free, so a class added to the doctrine is answered here or
/// does not compile.
///
/// `deadline` is the defeated stow's own, carried in: the expansion is the same
/// maneuver continuing, so it gets what is left of that clock.
fn defeated_stow<A: Active>(
    head: A,
    ended: Refusal,
    deadline: Duration,
    ctx: &mut Responding,
) -> Result<Standing, Ending> {
    match ended.class() {
        ErrorClass::MaskedSlowStowToPark => masked_stow(head, ended, deadline, ctx),
        ErrorClass::Refuse
        | ErrorClass::SlowStowToRest
        | ErrorClass::ImmediateAllTorqueOffToRest
        | ErrorClass::ImmediateAllTorqueOffToPark => torque_off_now(head, ended, ctx),
    }
}

/// Stow on what still commands, then release everything.
///
/// The answer to a servo that dropped out: it is already torqued off and out of
/// every check, so the head comes down on the joints that are left rather than
/// going limp all at once. What the machine is left waiting for is the
/// maneuver's own answer — the mask growing latches — and this daemon carries it
/// rather than deriving a second one.
///
/// The stow reports itself the way the daemon's own moves do: a pair leaving the
/// moves on the way down is a change of what the machine can do, and the surfaces
/// that answer from that have to be written wherever it happens.
fn masked_stow<A: Active>(
    head: A,
    refusal: Refusal,
    deadline: Duration,
    ctx: &mut Responding,
) -> Result<Standing, Ending> {
    ctx.line(&format!(
        "a servo has dropped out: {refusal}. it is off already; stowing on what still commands."
    ));
    let disposition = head.masked_stow(deadline, &mut |text| ctx.sink.line(text), &mut |event| {
        ticked(event, ctx.shared, ctx.sink, ctx.surface)
    });
    settled(
        disposition,
        refusal,
        // Nothing measured where the head came to rest, and a joint is out of
        // service: the next engage plans from a fresh sweep, not from this.
        Standing::Elsewhere,
        ctx,
    )
}

/// Torque off now, and nothing else.
///
/// The response where motor control or position feedback is no longer trusted,
/// so a commanded move is exactly what cannot be relied on; the head falls
/// gently into near-stow under gearbox resistance instead. What the release has
/// to say about itself — a servo that never acknowledged its own torque-off — is
/// folded in, because that is the one thing that decides whether a hand can go
/// on the head. Folded in and not noted: the release consumed the engagement, and
/// a condition found inside a call that consumes it is recorded by that call.
fn torque_off_now<A: Active>(
    head: A,
    refusal: Refusal,
    ctx: &mut Responding,
) -> Result<Standing, Ending> {
    ctx.line(&format!(
        "{refusal}. writing torque off now; the head settles into near-stow on its own."
    ));
    let ended = match head.disengage_now(&mut |text| ctx.sink.line(text)) {
        Ok(()) => refusal,
        Err(unacked) => refusal.and(unacked),
    };
    let disposition = ended.class().disposition();
    settled(disposition, ended, Standing::Elsewhere, ctx)
}

/// Where an ending leaves the daemon once torque is off and no maneuver is left.
///
/// Torque is already off on every path here — an engage that failed after its
/// first enable, a release that reported something about itself — so there is
/// nothing to command and nothing to choose but whether the next script may ask
/// again.
fn already_limp(refusal: Refusal, ctx: &mut Responding) -> Result<Standing, Ending> {
    let disposition = refusal.class().disposition();
    settled(disposition, refusal, Standing::Elsewhere, ctx)
}

/// The last question every response answers: may the next script engage this
/// machine, or does a person have to look first.
///
/// The disposition and nothing else decides it, and it arrives from whatever ran
/// the maneuver. A rest disposition writes no fault cell — a daemon that stops
/// taking scripts because a hand met the head is an outage over an event the
/// machine recovers from by itself — and the loop goes back to Resting with the
/// record in the capture. A park writes the cell, once, and the daemon waits.
fn settled(
    disposition: Disposition,
    refusal: Refusal,
    standing: Standing,
    ctx: &mut Responding,
) -> Result<Standing, Ending> {
    let record = ctx.record();
    match disposition {
        Disposition::Rest => {
            wound_down(&refusal, &record, ctx);
            Ok(standing)
        }
        Disposition::Park => Err(Ending::Faulted(ctx.stage, refusal, record)),
    }
}

/// The report a wind-down back to rest owes: what happened, what it named, and
/// that the machine is at the minimum risk condition with nothing latched.
///
/// Its own event rather than `motion_fault`: that one says a daemon has parked
/// and nothing will move again, and a capture that used one word for both would
/// make the difference unreadable exactly where it matters most.
///
/// What is said about the head is the record's to say and not this function's.
/// Every rest-class ending arrives here — the controlled stow that finished, a
/// stow on what was left of the joints, a machine dropped limp where it stood,
/// and an engage that never took the machine at all — and the one thing true of
/// all of them is that torque is off and nothing latched. Which maneuver brought
/// the head down is in the entries, so the entries go out with the report.
fn wound_down(refusal: &Refusal, record: &[Entry], ctx: &Responding) {
    let stage = ctx.stage;
    let sink = ctx.sink;
    let slug = condition(record);
    sink.event(
        "motion_incident",
        &json!({
            "stage": stage.to_string(),
            "slug": slug,
            "detail": refusal.to_string(),
            "incident": story(record),
            "disposition": "rest",
        }),
    );
    sink.line(&format!(
        "the run did not finish: {refusal}. torque is off and nothing is latched, so nothing has \
         to be cleared before the next script."
    ));
    // Alerted on as well as narrated, and this is the reason: nothing latched,
    // so from outside the daemon this is indistinguishable from a run in which
    // nothing ever went wrong. The one thing that says a hand met the head, or
    // that a plan of ours would not run, is somebody being told.
    //
    // With the record, because this alert is the whole of the evidence anybody
    // gets: it is what says whether the head was stowed under control, stowed on
    // what was left of it, or dropped where it stood.
    ctx.shared.note_incident(format!(
        "at {stage}: {refusal}{}{}",
        slug.map(|slug| format!(" [{slug}]")).unwrap_or_default(),
        story(record)
            .map(|story| format!(". the record: {story}"))
            .unwrap_or_default()
    ));
}

/// The ending for a commissioning that refused: the machine was never taken.
///
/// Nothing was torqued, so nothing has to be released — the machine is already
/// at the minimum risk condition, which is where it was found. There is no
/// motion loop on this path, so the stop reason is here only to wake the bus
/// thread, and the ending has to be noted here too: the bus thread waits for one
/// before it closes the attachment the alert travels over, and no loop is coming
/// that would set it.
pub fn commission_failed(
    shared: &Shared,
    refusal: Refusal,
    sink: &dyn Sink,
    surface: &Surface,
) -> Outcome {
    // No session and so no record: nothing was ever taken hold of, and the
    // refusal itself is the whole of what happened.
    let outcome = faulted(
        shared,
        FaultStage::Commission,
        refusal,
        Vec::new(),
        sink,
        surface,
    );
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
    record: Vec<Entry>,
    sink: &dyn Sink,
    surface: &Surface,
) -> Outcome {
    let outcome = faulted(shared, stage, refusal, record, sink, surface);
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
///
/// The state surface is written here too, and here is after: torque came off
/// before this was reached, so nothing about a file has ever stood between the
/// machine and the minimum risk condition.
///
/// The condition the machine is left in is read off `record` rather than out of
/// the refusal's wording, and the latest one at that: a wind-down that began
/// over a grabbed head and latched because a servo dropped out on the way down
/// is an incident about the servo, and that is the word an alert rule and an
/// operator both need.
fn faulted(
    shared: &Shared,
    stage: FaultStage,
    refusal: Refusal,
    record: Vec<Entry>,
    sink: &dyn Sink,
    surface: &Surface,
) -> Outcome {
    let report = FaultReport::recorded(stage, refusal.to_string(), record);
    shared.set_fault(report.clone());
    // Before the parked record, so the one file an operator reads carries what
    // the session had left of the machine as well as what stopped it.
    state_antennas(shared, sink, surface);
    surface.parked(&report, sink);
    sink.event(
        "motion_fault",
        &json!({
            "stage": report.stage.to_string(),
            "slug": report.slug,
            "detail": report.detail,
            "incident": report.story(),
        }),
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
    use std::path::PathBuf;
    use std::rc::Rc;
    use std::sync::Arc;

    use std::sync::mpsc::{Sender, channel};

    use motion_proto::{Acceptance, MotionScript, Play, Step};
    use reachy_bench::pump::ReadFailures;
    use reachy_bus::{IdOutcome, XactError};
    use reachy_clips::{interpolate_pose, lerp};
    use reachy_motion::{
        AntennaPhaseConfig, BusFailureSource, ClockStretch, CommandDisposition, JointId, Maneuver,
        Outcome as TimelineOutcome, PhaseSeparation, RegId, SeqError, SeqStep, ServoHealth,
        StepContext, WireFailure,
    };

    use super::*;
    use crate::cells::Delivered;
    use crate::report::Collect;
    use crate::state::{state_in, temp_dir, value_of};

    const POD: &str = "reachy00";
    /// Short enough that a test which really sleeps through one is not slow,
    /// long enough that a boundary cutting it is visible.
    const DWELL: Duration = Duration::from_millis(200);
    const REST_POLL: Duration = Duration::from_millis(1);
    /// How long a folded head is held before it is let go.
    ///
    /// Wide for the same reason [`SCRIPT_STEP_MS`] is: the tests that turn on
    /// this window need something to land inside it, so the window is their
    /// whole margin. A wake-up late by more than the delay closes it early — a
    /// wake read as a fresh session, a stop read as a release that has already
    /// happened. Nothing asserts the number itself.
    ///
    /// Under [`DWELL`] rather than over it, so a fixture that sleeps its way to
    /// the end of the delay still gets there in one dwell and the act sequences
    /// that count holds are unmoved.
    const REST_DELAY: Duration = Duration::from_millis(150);
    /// Long enough that no test's script lapses while the test is running.
    const TIMEOUT_MS: u64 = 30_000;
    /// How far apart the steps of a script whose own timeline advances the run
    /// sit — [`Event::Turn`]'s stow step, and every step of the `keep` scripts.
    ///
    /// The gap is the whole margin these fixtures have. Their dwells really
    /// sleep, so the loop reads its schedule about once per boundary, and each
    /// ask is live only until the next step is due: a sleep that returns late
    /// by more than one gap steps straight past an ask, and the run never sees
    /// it — a raise that never happens, or two keeps read as one. So the gap is
    /// an order of magnitude above what a loaded machine adds to a sleep, which
    /// is what it costs to be a margin rather than a coin toss.
    ///
    /// Deliberately not at the [`FAKE_PERIOD`] scale the paced fixtures work
    /// in: nothing here is counting control periods.
    const SCRIPT_STEP_MS: u64 = 400;
    /// Where [`Event::PlayThenLower`]'s second base step sits: a few paced
    /// streamed periods past the play step, and well inside the motion's own
    /// window, so the base command changes while the overlay is still playing.
    const BASE_CHANGE_MS: u64 = 80;
    /// What the fixture says one stow maneuver is allowed, end to end.
    const FAKE_STOW_BUDGET: Duration = Duration::from_secs(3);
    /// How far the fixture's session clock advances per move commanded. Enough
    /// of the budget above that a second one would be visible as a second one.
    const FAKE_MOVE_STEP: Duration = Duration::from_secs(1);
    /// How many periods the fixture's base transition takes. Enough that a
    /// motion joining it composes over a reference that is genuinely moving,
    /// short enough that a test reads its whole series.
    ///
    /// This and [`FAKE_PERIOD`] between them are the whole margin the paced
    /// fixtures have: a step or a window that has to land *inside* a drive has
    /// this many periods of real sleeping to land in, and a wake-up late by
    /// more than that lands outside it.
    // TODO(loop-fixture-paced-jitter)
    const FAKE_BASE_PERIODS: u32 = 8;
    /// The control period the fixture reports. The tick rate everything is
    /// floored at, which is also what a clip's frames are sampled at.
    const FAKE_PERIOD: Duration = Duration::from_millis(20);
    /// How many periods one streamed run may take before the fixture calls the
    /// loop broken. A run ends when the caller stops answering; anything past
    /// this is a caller that never will.
    const MAX_STREAM_PERIODS: u32 = 400;
    /// How many streamed runs one fixture may ask for. No test drives the base
    /// more than a handful of times, so anything past this is the loop starting
    /// a run per pass without ever dwelling — which on a real machine is a hot
    /// spin against a refusing tick, and here would eat the workstation.
    const MAX_STREAM_RUNS: usize = 64;
    /// Where a play step that is *not* due when the loop first looks sits: a
    /// few paced streamed periods past the base step it rides, and well inside
    /// the base drive, so the window opens with the head already moving.
    const LATE_PLAY_MS: u64 = 60;
    /// How late a window may be picked up before a test calls it late.
    ///
    /// A run is paced by real sleeps against a real clock, so a period is one
    /// period plus whatever the machine running the suite adds to a 20 ms
    /// sleep. Padded rather than exact for that reason alone: the behaviour
    /// this bounds out is a window waited out until the drive it belongs to
    /// finished, which is [`FAKE_BASE_PERIODS`] periods and a dwell away.
    const JOIN_SLACK_MS: u64 = 3 * FAKE_PERIOD.as_millis() as u64;
    /// Where a base step far enough out sits that a paced drive under way when
    /// its script lands finishes before it comes due.
    const LATE_STEP_MS: u64 = 400;
    /// Where a play step sits on the timelines these tests write. Not zero,
    /// because step offsets ascend strictly and the base step it rides is at
    /// zero — a play step is a step like any other.
    const PLAY_STEP_MS: u64 = 1;
    /// The period of [`Event::SpendThenPlay`]'s raise that is streaming bare:
    /// past the two-frame motion's whole length and blend-out, and short of
    /// [`FAKE_BASE_PERIODS`], where the drive ends.
    const BARE_AFTER_SPEND_PERIOD: u32 = 6;
    /// The period of a held run on which a two-frame motion's player spends.
    ///
    /// A held base arrives on every period, so the run ends on the first period
    /// with no player left: the period the last one spends on is the only one
    /// that offers the held base bare, and it is a count of periods rather than
    /// wall time because a player is advanced by the period it is handed.
    const HELD_SPEND_PERIOD: u32 = 2;
    /// Where [`Event::SpendThenPlay`]'s second play step sits: several paced
    /// periods past [`BARE_AFTER_SPEND_PERIOD`], so the window opens on the
    /// drive that re-acquires the base rather than on the refused one.
    const SECOND_WINDOW_MS: u64 = 260;
    /// How long before it is read a script carrying a play step is taken to
    /// have landed.
    ///
    /// A script arrives on the bus thread and is read by this one on its next
    /// pass, and these fixtures run in microseconds — so a play step a
    /// millisecond in would not be due yet at the instant the loop looked. One
    /// control period back is the smallest offset that makes the timeline
    /// answer what it answers in life.
    fn landed(now: Instant) -> Instant {
        now.checked_sub(FAKE_PERIOD)
            .expect("a monotonic clock a period past its start")
    }

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
        /// A measured move: the fold flows, and nothing else.
        Move(Posture),
        /// A streamed run: one composed setpoint per control period, over the
        /// base this plan names.
        Stream(BasePlan),
        Hold,
        /// The orderly release: settle, measure, torque off.
        Release,
        /// The fault release: nine writes and nothing else.
        ReleaseNow,
        /// The masked wind-down: stow on what still commands, then release
        /// everything.
        MaskedStow,
    }

    /// A wire that stopped carrying commands under torque: control not trusted,
    /// torque off on the spot, and a person before the next engage.
    ///
    /// Found by the layer holding the wire and not by the tick, which is why the
    /// record does not have it yet — the response is what puts it there.
    fn bus_failure() -> Refusal {
        Refusal::naming(
            ErrorClass::ImmediateAllTorqueOffToPark,
            bus_silent(),
            "the bus is not carrying commands: servo 12: silence",
        )
    }

    /// The condition [`bus_failure`] names, as a record carries it.
    fn bus_silent() -> Fault {
        Fault::BusFailure {
            source: BusFailureSource::Transaction {
                id: 12,
                kind: WireFailure::Silent,
            },
        }
    }

    /// A plan of ours the tick would not run. The platform is healthy and still
    /// commanding, so the head is stowed under control and nothing latches, and
    /// no condition of the machine is named at all.
    fn move_aborted() -> Refusal {
        Refusal::new(
            ErrorClass::SlowStowToRest,
            "the move was abandoned: the step for leg 3 is past the bound",
        )
    }

    /// A hand or a snag on the head. The motors still command, so the answer is
    /// a controlled stow back to rest — and the tick raised it, so it is already
    /// in the record.
    fn head_obstructed() -> Refusal {
        Refusal::new(
            ErrorClass::SlowStowToRest,
            "leg 3 is 0.4000 rad from its goal and not closing",
        )
    }

    /// A leg servo dropping out mid-move: it is off already, the stow carries on
    /// without it, and the ending latches.
    fn head_servo_fault() -> Refusal {
        Refusal::new(
            ErrorClass::MaskedSlowStowToPark,
            "leg 3 (servo 13) reports hardware error 0x20",
        )
    }

    /// The configuration a posture means, with no clock beside it.
    fn posture_targets(posture: Posture) -> JointTargets {
        match posture {
            Posture::Up => neutral_targets(),
            Posture::Stow => stow_pose_targets(),
        }
    }

    /// The configuration `s` of the way from `from` to `to`.
    fn between(from: &JointTargets, to: &JointTargets, s: f64) -> JointTargets {
        JointTargets {
            head_pose_body: interpolate_pose(&from.head_pose_body, &to.head_pose_body, s),
            body_yaw: lerp(from.body_yaw, to.body_yaw, s),
            antennas: [
                lerp(from.antennas[0], to.antennas[0], s),
                lerp(from.antennas[1], to.antennas[1], s),
            ],
        }
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
        /// A script arrives whose only base step is a stow well in the future.
        /// Lawful on the wire and the absence of an instruction until that step
        /// comes due: nothing is asked for, so whatever the machine is doing
        /// stands.
        LaterLower,
        /// A script arrives asking for the head up, and for the base to stay
        /// where it is a short way in. What a publisher changing overlays
        /// mid-motion sends, and the only way `keep` reaches a machine at all:
        /// it never wakes one by itself.
        RaiseThenKeep,
        /// A script arrives that keeps twice with a raise between them, so the
        /// second keep is a fresh transition into the answer rather than the
        /// same one still standing.
        KeepRaiseKeep,
        /// A script arrives asking for the head up and playing this motion from
        /// the moment it lands: the overlay rides the raise it asked for, which
        /// is the layered-over-a-moving-reference case.
        Play(&'static str),
        /// A script arrives asking for the head up, playing the first motion
        /// from the moment it lands and the second one long after the first has
        /// spent. The two-window shape: the drive goes on streaming bare
        /// between them, so a refusal landing in that stretch belongs to the
        /// base and the second window is none of its business.
        SpendThenPlay(&'static str, &'static str),
        /// A script arrives that leaves the base where it is and plays this
        /// motion over it. What a publisher changing overlays mid-conversation
        /// sends, and the held-base case.
        KeepAndPlay(&'static str),
        /// A script arrives asking for the head up, playing this motion from
        /// the moment it lands, and lowering the base part-way through it. One
        /// script, two base commands: the case where a streamed run ends on a
        /// base change and the players have to survive it.
        PlayThenLower(&'static str),
        /// A script arrives asking for the head up now and playing this motion
        /// a few streamed periods later. The plainly-authored first-contact
        /// shape: the window opens while the base is still on its way, so the
        /// run the base is already in is the one that has to pick it up.
        RaiseThenPlay(&'static str),
        /// A script arrives carrying a whole turn — up now, stow a short way in
        /// — and playing this motion just after the stow step. The showcase
        /// shape: the clip joins the second of two base drives.
        TurnThenPlay(&'static str),
        /// A script arrives whose only step is a `keep` — lawful on the wire,
        /// since `keep` defines the base as much as a named posture does, and
        /// the one script that must never put torque on a resting machine.
        KeepOnly,
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
        /// The sweeps, counted from zero, that refuse: the first of the run and
        /// how many there are.
        refuse_watch: Option<(usize, usize)>,
        /// Whether the last sweep failed, which is the state a real engage's
        /// remedial sweep would find the bus in.
        watch_failing: bool,
        /// Whether an engage refuses while the bus is not answering, as the real
        /// one does: past a stale posture it sweeps first, and a sweep that
        /// cannot measure is the refusal. Off by default, because a bus that
        /// came back between the last failed sweep and the wake is the other
        /// case worth driving.
        unreadable_engage: bool,
        /// The engage, counted from zero, that a torque-on gate refuses.
        gate_engage: Option<usize>,
        /// The engage, counted from zero, that fails outright.
        fault_engage: Option<usize>,
        /// The move, counted from zero, that refuses, and what it answers with.
        ///
        /// The refusal carries the class, because the class is the whole of what
        /// the loop decides on: the same fixture answering
        /// `ImmediateAllTorqueOffToPark` and answering `SlowStowToRest` is a
        /// parked daemon and a daemon back at rest.
        refuse_move: Vec<(usize, Refusal)>,
        /// What the tick recorded before the move refused, if a test says so.
        /// Pushed onto the session's own channel, which is where the real one
        /// arrives from.
        raises: Vec<Entry>,
        /// The joints the session has out of service, as the engagement reports
        /// them at any moment. A session engaged onto latched antenna bits starts
        /// with them already in here and no event ever announces it, which is one
        /// of the two ways a machine ends up commanding the head alone.
        out_of_service: JointSet,
        /// Where this session's record is being pushed, once there is a session.
        pushes: Option<Sender<Entry>>,
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
        /// The session clock, advanced one [`FAKE_MOVE_STEP`] per move
        /// commanded, so a deadline taken before a stow is a different number
        /// from one taken after it — which is the whole of what a maneuver
        /// inheriting its clock means.
        elapsed: Duration,
        /// The maneuver the record has open, as this fixture's own entries left
        /// it.
        open: Option<Maneuver>,
        /// The deadline each masked stow was handed, in order.
        stow_deadlines: Rc<RefCell<Vec<Duration>>>,
        /// Where the machine is standing, as the fixture's own moves and
        /// streams left it. What a streamed base is sampled from and toward.
        at: JointTargets,
        /// The base sample and the composed setpoint of every period any
        /// streamed run commanded, in order. The only record of what layering
        /// actually produced.
        composed: Rc<RefCell<Vec<(JointTargets, JointTargets)>>>,
        /// Where in [`Self::composed`] each streamed run began, so a test can
        /// read one run's periods apart from the next one's.
        stream_marks: Rc<RefCell<Vec<usize>>>,
        /// Whether a streamed period costs a period of wall time. What a run
        /// whose own script changes its base part-way through needs: the
        /// timeline is read against the real clock, and a fixture that streams
        /// in microseconds never reaches a step written in milliseconds.
        stream_paced: bool,
        /// The streamed run, counted from zero, that fails outright, and what
        /// it answers with — the fault path, where a composed setpoint the tick
        /// merely refuses is [`Self::refuse_setpoint`].
        refuse_stream: Vec<(usize, Refusal)>,
        /// The streamed runs, and the period of each, whose composed setpoint
        /// the tick will not take. Not a fault: nothing is commanded and
        /// nothing latches. A list rather than one entry because the two
        /// refusal responses compose — an overlay drop leaves the base running,
        /// so the run after it is the one that can be refused bare.
        refuse_setpoint: Vec<(usize, u32)>,
        /// What the world does in the middle of a streamed run, and at which
        /// period.
        stream_interrupt: Option<(u32, Event)>,
        /// Which streamed run that happens in, when the test cares. `None` is
        /// whichever run reaches the period first.
        stream_interrupt_run: Option<usize>,
        watches: usize,
        engages: usize,
        moves: usize,
        streams: usize,
        /// What the machine reports on each dwell, one entry per dwell. A dwell
        /// past the end of the script reports nothing.
        says: VecDeque<Vec<TickEvent>>,
        /// What it narrates on each move, one entry per move counted from the
        /// first. Text, because a move still narrates through the motion
        /// libraries' own rendering.
        says_moving: VecDeque<Vec<&'static str>>,
        /// What it reports as values on each move, one entry per move. The
        /// typed half of the same run: the events a move carries that this
        /// daemon records as facts rather than as prose.
        reports_moving: VecDeque<Vec<TickEvent>>,
        /// The state file to read at every act, when a test is watching it.
        ///
        /// The surface holds one record and replaces it, so the sequence it
        /// passed through is observable only from inside the run. This is that
        /// inside: the machine is the only thing the loop calls on its way
        /// between phases.
        trail: Option<PathBuf>,
        /// What the state file said at each recorded act, in the same order.
        seen: Rc<RefCell<Vec<String>>>,
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
                watch_failing: false,
                unreadable_engage: false,
                gate_engage: None,
                fault_engage: None,
                refuse_move: Vec::new(),
                raises: Vec::new(),
                out_of_service: JointSet::EMPTY,
                pushes: None,
                interrupt: None,
                refusal_stops: true,
                engage_takes: Duration::ZERO,
                refuse_release: false,
                release_off_stow: false,
                unacked_release: false,
                sleeps: false,
                dwells: Rc::new(RefCell::new(Vec::new())),
                elapsed: Duration::ZERO,
                open: None,
                stow_deadlines: Rc::new(RefCell::new(Vec::new())),
                at: stow_pose_targets(),
                composed: Rc::new(RefCell::new(Vec::new())),
                stream_marks: Rc::new(RefCell::new(Vec::new())),
                stream_paced: false,
                refuse_stream: Vec::new(),
                refuse_setpoint: Vec::new(),
                stream_interrupt: None,
                stream_interrupt_run: None,
                watches: 0,
                engages: 0,
                moves: 0,
                streams: 0,
                says: VecDeque::new(),
                says_moving: VecDeque::new(),
                reports_moving: VecDeque::new(),
                trail: None,
                seen: Rc::new(RefCell::new(Vec::new())),
            }
        }

        /// Read `path` at every act, so what the state surface said as the run
        /// went through it can be asserted as a sequence.
        fn watching_state(mut self, path: impl Into<PathBuf>) -> Self {
            self.trail = Some(path.into());
            self
        }

        fn seen(&self) -> Rc<RefCell<Vec<String>>> {
            Rc::clone(&self.seen)
        }

        /// The deadlines the masked stow was handed, readable after the run.
        fn stow_deadlines(&self) -> Rc<RefCell<Vec<Duration>>> {
            Rc::clone(&self.stow_deadlines)
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

        /// The same for what they report as values while moving, move by move.
        fn reporting_moving(mut self, per_move: impl IntoIterator<Item = Vec<TickEvent>>) -> Self {
            self.reports_moving = per_move.into_iter().collect();
            self
        }

        /// A refusal out of the nth move, which also stops the daemon: a parked
        /// thread waits for that, and a test that never sent one would wait with
        /// it.
        ///
        /// Control not trusted: the wire stopped carrying, torque comes off on
        /// the spot and the daemon parks.
        fn refusing_move(self, nth: usize) -> Self {
            self.refusing_move_with(nth, bus_failure())
        }

        /// The same, answering with `refusal` — which is what decides the
        /// response, the disposition and where the loop goes next.
        fn refusing_move_with(mut self, nth: usize, refusal: Refusal) -> Self {
            self.refuse_move.push((nth, refusal));
            self
        }

        /// A refusal that does not also stop the daemon, so what the loop does
        /// *after* an ending is observable.
        fn unstopped(mut self) -> Self {
            self.refusal_stops = false;
            self
        }

        /// What the tick had already recorded by the time the move refused.
        fn having_raised(mut self, entries: impl IntoIterator<Item = Entry>) -> Self {
            self.raises = entries.into_iter().collect();
            self
        }

        /// A machine whose engagements start with `joints` already out of
        /// service: the health gate let the wake through on bits that had latched
        /// before it, so there is nothing for an event to announce.
        fn engaging_without(mut self, joints: impl IntoIterator<Item = JointId>) -> Self {
            for joint in joints {
                self.out_of_service.insert(joint);
            }
            self
        }

        /// Every period a streamed run commanded: where the base was, and what
        /// the composition made of it.
        fn composed(&self) -> Rc<RefCell<Vec<(JointTargets, JointTargets)>>> {
            Rc::clone(&self.composed)
        }

        /// Where in [`Self::composed`] each streamed run began.
        fn stream_marks(&self) -> Rc<RefCell<Vec<usize>>> {
            Rc::clone(&self.stream_marks)
        }

        /// A streamed period costing a period of wall time, so a script whose
        /// own timeline changes the base part-way through gets there.
        fn pacing_stream(mut self) -> Self {
            self.stream_paced = true;
            self
        }

        /// The tick refusing the composed setpoint of one period of one streamed
        /// run. A plan of ours the machine would not take: nothing was
        /// commanded, nothing faulted. Keyed to named runs, because a fixture
        /// that refused every run would only ever be testing the loop's patience
        /// with a machine that says no forever.
        fn refusing_setpoint(mut self, nth: usize, period: u32) -> Self {
            self.refuse_setpoint.push((nth, period));
            self
        }

        /// The nth streamed run failing on a wire that stopped carrying, which
        /// also stops the daemon: a parked thread waits for that, and a test
        /// that never sent one would wait with it.
        fn refusing_stream(self, nth: usize) -> Self {
            self.refusing_stream_with(nth, bus_failure())
        }

        /// The nth streamed run failing outright, which also stops the daemon.
        fn refusing_stream_with(mut self, nth: usize, refusal: Refusal) -> Self {
            self.refuse_stream.push((nth, refusal));
            self
        }

        /// `event` happening at the nth period of a streamed run. What a script
        /// landing mid-motion looks like from the loop's side.
        fn interrupting_stream(mut self, period: u32, event: Event) -> Self {
            self.stream_interrupt = Some((period, event));
            self
        }

        /// The same, in the nth streamed run and no earlier one: what a script
        /// landing during the second of two drives looks like.
        fn interrupting_run(mut self, nth: usize, period: u32, event: Event) -> Self {
            self.stream_interrupt_run = Some(nth);
            self.interrupting_stream(period, event)
        }

        /// `event` happening while the nth measured move is still travelling,
        /// counted from the first. What a script landing during a fold looks
        /// like from the loop's side.
        fn interrupting(mut self, nth: usize, event: Event) -> Self {
            self.interrupt = Some((nth, event));
            self
        }

        /// A run of `count` sweeps refusing, from the nth: a bus that goes away
        /// and comes back, which is the shape the daemon is built to ride out.
        fn refusing_watch(mut self, nth: usize, count: usize) -> Self {
            self.refuse_watch = Some((nth, count));
            self
        }

        /// An engage refusing for as long as the sweeps are failing, which is
        /// what the real one does: past a stale posture it takes a remedial
        /// sweep of its own, and a sweep that cannot measure the machine is a
        /// refusal — nothing written, nothing to undo.
        fn unreadable_engage(mut self) -> Self {
            self.unreadable_engage = true;
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
        fn refusing_stream_unstopped(mut self, nth: usize) -> Self {
            self.refuse_stream.push((nth, bus_failure()));
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
            if let Some(path) = &self.trail {
                let read = |key| value_of(path, key).unwrap_or_else(|| "absent".to_owned());
                self.seen
                    .borrow_mut()
                    .push(format!("{}/{}", read("state"), read("watch")));
            }
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
                Some(Event::Play(motion)) => {
                    let script = MotionScript::new(
                        POD,
                        self.next_seq(),
                        vec![
                            Step::new(0, Posture::Up),
                            Step::play(PLAY_STEP_MS, Play::new(motion)),
                        ],
                        TIMEOUT_MS,
                    )
                    .expect("a lawful script");
                    self.shared.accept(&script, landed(now));
                }
                Some(Event::SpendThenPlay(first, second)) => {
                    let script = MotionScript::new(
                        POD,
                        self.next_seq(),
                        vec![
                            Step::new(0, Posture::Up),
                            Step::play(PLAY_STEP_MS, Play::new(first)),
                            Step::play(SECOND_WINDOW_MS, Play::new(second)),
                        ],
                        TIMEOUT_MS,
                    )
                    .expect("a lawful script");
                    self.shared.accept(&script, landed(now));
                }
                Some(Event::KeepAndPlay(motion)) => {
                    let script = MotionScript::new(
                        POD,
                        self.next_seq(),
                        vec![Step::keep(0), Step::play(PLAY_STEP_MS, Play::new(motion))],
                        TIMEOUT_MS,
                    )
                    .expect("a lawful script");
                    self.shared.accept(&script, landed(now));
                }
                Some(Event::RaiseThenPlay(motion)) => {
                    let script = MotionScript::new(
                        POD,
                        self.next_seq(),
                        vec![
                            Step::new(0, Posture::Up),
                            Step::play(LATE_PLAY_MS, Play::new(motion)),
                        ],
                        TIMEOUT_MS,
                    )
                    .expect("a lawful script");
                    self.shared.accept(&script, now);
                }
                Some(Event::TurnThenPlay(motion)) => {
                    let script = MotionScript::new(
                        POD,
                        self.next_seq(),
                        vec![
                            Step::new(0, Posture::Up),
                            Step::new(BASE_CHANGE_MS, Posture::Stow),
                            Step::play(BASE_CHANGE_MS + LATE_PLAY_MS, Play::new(motion)),
                        ],
                        TIMEOUT_MS,
                    )
                    .expect("a lawful script");
                    self.shared.accept(&script, now);
                }
                Some(Event::PlayThenLower(motion)) => {
                    let script = MotionScript::new(
                        POD,
                        self.next_seq(),
                        vec![
                            Step::new(0, Posture::Up),
                            Step::play(PLAY_STEP_MS, Play::new(motion)),
                            Step::new(BASE_CHANGE_MS, Posture::Stow),
                        ],
                        TIMEOUT_MS,
                    )
                    .expect("a lawful script");
                    self.shared.accept(&script, landed(now));
                }
                Some(Event::Lower) => {
                    let script = holding(self.next_seq(), Posture::Stow);
                    self.shared.accept(&script, now);
                }
                Some(Event::LaterLower) => {
                    let script = MotionScript::new(
                        POD,
                        self.next_seq(),
                        vec![Step::new(LATE_STEP_MS, Posture::Stow)],
                        TIMEOUT_MS,
                    )
                    .expect("a lawful script");
                    self.shared.accept(&script, now);
                }
                Some(Event::RaiseThenKeep) => {
                    let script = MotionScript::new(
                        POD,
                        self.next_seq(),
                        vec![Step::new(0, Posture::Up), Step::keep(SCRIPT_STEP_MS)],
                        TIMEOUT_MS,
                    )
                    .expect("a lawful script");
                    self.shared.accept(&script, now);
                }
                Some(Event::KeepRaiseKeep) => {
                    let script = MotionScript::new(
                        POD,
                        self.next_seq(),
                        vec![
                            Step::new(0, Posture::Up),
                            Step::keep(SCRIPT_STEP_MS),
                            Step::new(SCRIPT_STEP_MS * 2, Posture::Up),
                            Step::keep(SCRIPT_STEP_MS * 3),
                        ],
                        TIMEOUT_MS,
                    )
                    .expect("a lawful script");
                    self.shared.accept(&script, now);
                }
                Some(Event::KeepOnly) => {
                    let script =
                        MotionScript::new(POD, self.next_seq(), vec![Step::keep(0)], TIMEOUT_MS)
                            .expect("a lawful script");
                    self.shared.accept(&script, now);
                }
                Some(Event::Turn) => {
                    let script = MotionScript::new(
                        POD,
                        self.next_seq(),
                        vec![
                            Step::new(0, Posture::Up),
                            Step::new(SCRIPT_STEP_MS, Posture::Stow),
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

    impl Held<'_> {
        /// Put an entry on this session's record, as the libraries would.
        ///
        /// Best-effort, exactly like the real subscriber: a record nobody is
        /// reading is not an error on the reporting path of a machine in trouble.
        fn push(&self, entry: Entry) {
            if let Some(pushes) = &self.machine.pushes {
                let _ = pushes.send(entry);
            }
        }
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
            // The world moves whether or not the machine answered: a failing
            // sweep does not stop scripts arriving, and it must not stop a test
            // running out of events either — a failure is no longer an ending.
            let event = self.advance();
            self.stop_if_spent(event);
            self.watch_failing = self
                .refuse_watch
                .is_some_and(|(from, count)| (from..from + count).contains(&nth));
            if self.watch_failing {
                return Err(Refusal::new(ErrorClass::Refuse, "servo 11: timed out"));
            }
            Ok(self.standing)
        }

        fn engage(
            &mut self,
            _line: &mut dyn FnMut(&str),
        ) -> Result<(Held<'_>, Incident), EngageFailed> {
            let nth = self.engages;
            self.engages += 1;
            thread::sleep(self.engage_takes);
            // The remedial sweep first, as the real one does: it is the reason
            // an engage over a bus that is not answering refuses rather than
            // faults, and it happens before any of the torque-on gates.
            if self.unreadable_engage && self.watch_failing {
                return Err(EngageFailed::Gate(Refusal::new(
                    ErrorClass::Refuse,
                    "servo 11: timed out",
                )));
            }
            if self.gate_engage == Some(nth) {
                return Err(EngageFailed::Gate(Refusal::new(
                    ErrorClass::Refuse,
                    "the supply is below the floor: 5.5 V against 6.0 V",
                )));
            }
            if self.fault_engage == Some(nth) {
                // As with a refused move: the ending parks, and a parked thread
                // waits to be stopped.
                self.shared.request_stop(Stop::Operator);
                return Err(EngageFailed::Fault(Refusal::new(
                    ErrorClass::ImmediateAllTorqueOffToPark,
                    "servo 14: no answer to the enable",
                )));
            }
            self.record(Act::Engage);
            let (pushes, pushed) = channel();
            self.pushes = Some(pushes);
            Ok((Held { machine: self }, Incident::new(pushed)))
        }
    }

    impl Active for Held<'_> {
        fn move_to(
            &mut self,
            posture: Posture,
            line: &mut dyn FnMut(&str),
            event: &mut dyn FnMut(TickEvent),
        ) -> Result<(), Refusal> {
            let nth = self.machine.moves;
            self.machine.moves += 1;
            // Commanded time passes whether or not the move finishes: a stow a
            // servo dropped out of still spent most of its clock.
            self.machine.elapsed = self.machine.elapsed.saturating_add(FAKE_MOVE_STEP);
            let says = self.machine.says_moving.pop_front().unwrap_or_default();
            let reports = self.machine.reports_moving.pop_front().unwrap_or_default();
            if let Some((_, refusal)) = self.machine.refuse_move.iter().find(|(at, _)| *at == nth) {
                let refusal = refusal.clone();
                if self.machine.refusal_stops {
                    self.machine.shared.request_stop(Stop::Operator);
                }
                // As the real one does: whatever the tick raised is on the
                // session's channel before the ending reaches the loop.
                let raised = std::mem::take(&mut self.machine.raises);
                for entry in raised {
                    self.push(entry);
                }
                return Err(refusal);
            }
            self.machine.record(Act::Move(posture));
            for text in says {
                line(text);
            }
            // After the lines, which is where the real one falls: the library
            // right-sizes the clock as it accepts the command, so a stretch is
            // the first thing a move has to say.
            for reported in reports {
                event(reported);
            }
            // What the real move does over its control periods, compressed: the
            // world changes once if the test said so, and the move runs to its
            // endpoint whatever the world now says — nothing diverts a fold.
            if let Some((at, event)) = self.machine.interrupt
                && at == nth
            {
                self.machine.interrupt = None;
                self.machine.apply(Some(event));
            }
            self.machine.at = posture_targets(posture);
            Ok(())
        }

        fn stream(
            &mut self,
            base: BasePlan,
            _line: &mut dyn FnMut(&str),
            event: &mut dyn FnMut(TickEvent),
            compose: &mut dyn FnMut(BaseAt) -> Option<JointTargets>,
        ) -> Result<Streamed, Refusal> {
            let nth = self.machine.streams;
            self.machine.streams += 1;
            // Commanded time passes whether or not the run reaches its target,
            // exactly as a measured move's does: a deadline taken before a
            // drive is a different number from one taken after it.
            self.machine.elapsed = self.machine.elapsed.saturating_add(FAKE_MOVE_STEP);
            assert!(
                nth < MAX_STREAM_RUNS,
                "the loop asked for {MAX_STREAM_RUNS} streamed runs: it is starting a run per \
                 pass and never dwelling"
            );
            self.machine.record(Act::Stream(base));
            let mark = self.machine.composed.borrow().len();
            self.machine.stream_marks.borrow_mut().push(mark);
            let reports = self.machine.reports_moving.pop_front().unwrap_or_default();
            if let Some((_, refusal)) = self.machine.refuse_stream.iter().find(|(at, _)| *at == nth)
            {
                let refusal = refusal.clone();
                if self.machine.refusal_stops {
                    self.machine.shared.request_stop(Stop::Operator);
                }
                // As the real one does: whatever the tick raised is on the
                // session's channel before the ending reaches the loop.
                let raised = std::mem::take(&mut self.machine.raises);
                for entry in raised {
                    self.push(entry);
                }
                return Err(refusal);
            }
            for reported in reports {
                event(reported);
            }
            let start = self.machine.at;
            let end = match base {
                BasePlan::Held => start,
                BasePlan::To(posture) => posture_targets(posture),
            };
            for period in 0..MAX_STREAM_PERIODS {
                if self.machine.stream_paced {
                    thread::sleep(FAKE_PERIOD);
                }
                // The world changes at a period the test names, which is where
                // a script landing mid-motion arrives in life too.
                if let Some((at, happening)) = self.machine.stream_interrupt
                    && at == period
                    && self
                        .machine
                        .stream_interrupt_run
                        .is_none_or(|run| run == nth)
                {
                    self.machine.stream_interrupt = None;
                    self.machine.apply(Some(happening));
                }
                let travelled = match base {
                    BasePlan::Held => 1.0,
                    BasePlan::To(_) => {
                        f64::from(period.min(FAKE_BASE_PERIODS)) / f64::from(FAKE_BASE_PERIODS)
                    }
                };
                let targets = between(&start, &end, travelled);
                let arrived = travelled >= 1.0;
                let Some(composed) = compose(BaseAt {
                    targets,
                    arrived,
                    period: FAKE_PERIOD,
                }) else {
                    self.machine.at = targets;
                    return Ok(Streamed::Ended);
                };
                if self.machine.refuse_setpoint.contains(&(nth, period)) {
                    return Ok(Streamed::Refused(CommandRejection::StepTooLarge {
                        joint: JointId::AntennaRight,
                        delta: 0.6,
                    }));
                }
                self.machine.composed.borrow_mut().push((targets, composed));
                self.machine.at = composed;
            }
            panic!(
                "the loop streamed {MAX_STREAM_PERIODS} periods without ending: it is composing \
                 over a base that never arrives"
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
                    Err(Refusal::new(
                        ErrorClass::ImmediateAllTorqueOffToPark,
                        "servo 13: timed out",
                    ))
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
                return Err(Refusal::new(
                    ErrorClass::ImmediateAllTorqueOffToPark,
                    "servo 12 did not acknowledge torque off",
                ));
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

        fn masked_stow(
            self,
            deadline: Duration,
            line: &mut dyn FnMut(&str),
            event: &mut dyn FnMut(TickEvent),
        ) -> Disposition {
            self.machine.record(Act::MaskedStow);
            self.machine.stow_deadlines.borrow_mut().push(deadline);
            if self.machine.elapsed >= deadline {
                line("  the stow clock is spent; releasing torque now");
            }
            // The maneuver's own stow is a commanded move like any other, and it
            // reports what it saw: the fixture takes the next move's events for
            // it, in the order the moves were commanded.
            for reported in self.machine.reports_moving.pop_front().unwrap_or_default() {
                event(reported);
            }
            // As the library's own maneuver does: an expansion closes the
            // maneuver that was already open rather than opening a second one.
            let maneuver = self.machine.open.unwrap_or(Maneuver::MaskedSlowStow);
            let at = self.machine.elapsed;
            self.push(Entry::Response {
                maneuver,
                outcome: TimelineOutcome::Completed,
                at,
            });
            // What the library's own maneuver answers for this class: the mask
            // only grows, so a stow that masked anything latches.
            Disposition::Park
        }

        fn stow_deadline(&self) -> Duration {
            self.machine.elapsed.saturating_add(FAKE_STOW_BUDGET)
        }

        fn open_maneuver(&self) -> Option<Maneuver> {
            self.machine.open
        }

        fn out_of_service(&self) -> JointSet {
            self.machine.out_of_service
        }

        fn note(&mut self, fault: Fault) {
            let at = self.machine.elapsed;
            self.push(Entry::Fault { fault, at });
        }

        fn note_response(&mut self, maneuver: Maneuver, outcome: TimelineOutcome) {
            // The record's own rule, as the library keeps it: an outcome that
            // ends a maneuver leaves nothing open.
            self.machine.open = (!outcome.ends()).then_some(maneuver);
            let at = self.machine.elapsed;
            self.push(Entry::Response {
                maneuver,
                outcome,
                at,
            });
        }

        fn disengage_now(self, _line: &mut dyn FnMut(&str)) -> Result<(), Refusal> {
            self.machine.record(Act::ReleaseNow);
            if self.machine.unacked_release {
                // Named, and recorded here, exactly as the library does it: a
                // minimum risk condition believed rather than known is a
                // condition of the machine, and the call that consumed the
                // engagement is the only layer that could have seen it. The
                // ending still says the record does not have it — that is a fact
                // about the error's type, not about this session — so anything
                // above that notes what it is handed would write it twice.
                let fault = Fault::TorqueOffUnconfirmed { id: 12 };
                self.push(Entry::Fault {
                    fault,
                    at: self.machine.elapsed,
                });
                return Err(Refusal::naming(
                    ErrorClass::ImmediateAllTorqueOffToPark,
                    fault,
                    "servo 12 did not acknowledge torque off",
                ));
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
    ///
    /// The state surface writes into a temporary directory that lasts exactly as
    /// long as the run: every test drives the real writer, so a transition that
    /// panicked or a path the loop got wrong is a failure here rather than on a
    /// device.
    fn driven(shared: &Shared, machine: Fake) -> (Outcome, Vec<Act>, Collect) {
        let dir = temp_dir();
        let (outcome, acts, _, sink) = stated(&state_in(&dir), shared, machine);
        (outcome, acts, sink)
    }

    /// The base transitions a run narrated, in order.
    ///
    /// The console's own half of a drive: the capture is asserted event by
    /// event elsewhere, and the two are only the same statement for as long as
    /// something checks the words as well.
    fn motion_lines(sink: &Collect) -> Vec<String> {
        sink.said()
            .lines
            .iter()
            .filter(|line| line.starts_with("motion: "))
            .cloned()
            .collect()
    }

    /// A session that streamed `plan`, wound down under control, and then
    /// watched — the whole shape of a refused bare base's ending.
    ///
    /// One statement rather than a hand-rolled prefix in each test that pins it,
    /// because the ending is the loop's answer to every bare refusal and it
    /// grows whenever the response ladder does: two copies drift into two
    /// different pins, one of which fails while the other stays green and stops
    /// meaning what its message says.
    fn wound_down_acts(acts: &[Act], plan: BasePlan) {
        let expected = [
            Act::Watch,
            Act::Engage,
            Act::Stream(plan),
            Act::Move(Posture::Stow),
            Act::Release,
        ];
        assert_eq!(
            acts.get(..expected.len()),
            Some(&expected[..]),
            "the refused bare base did not wind the session down under \
             control: {acts:?}"
        );
        assert!(
            // The rest of the run is the resting watch and nothing else: the
            // script still asks for its posture, and a loop that engaged again
            // would cycle torque against whatever produced the rejection.
            acts[expected.len()..].iter().all(|act| *act == Act::Watch),
            "the answered script was driven again: {acts:?}"
        );
    }

    /// The same again, against a state surface at `path`, and keeping what that
    /// surface said as the run passed through it.
    ///
    /// The surface is opened through [`Surface::opening`], which is what the
    /// binary calls too: `starting` is published before the loop begins because
    /// commissioning is over by the time [`run`] is reached.
    fn stated(
        path: &Path,
        shared: &Shared,
        machine: Fake,
    ) -> (Outcome, Vec<Act>, Vec<String>, Collect) {
        let acts = machine.acts();
        let seen = machine.seen();
        let sink = Collect::default();
        let surface = Surface::opening(path, &sink);
        let outcome = run(machine, shared, timing(), &sink, &surface);
        let done = acts.borrow().clone();
        let trail = seen.borrow().clone();
        (outcome, done, trail, sink)
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

    /// `keep` means "do not move the base", and it means it against a machine
    /// whose posture nobody knows as much as against one holding a named
    /// posture. A run engaged onto a machine found standing has no posture at
    /// all, and the fold that answers *no* ask must not answer this one: a
    /// `keep` that folded would move the head to stow on the one command whose
    /// whole content is that the head does not move.
    #[test]
    fn a_keep_against_an_unknown_posture_holds_instead_of_folding() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [
                Event::Wait,
                Event::RaiseThenKeep,
                Event::Wait,
                Event::Stop(Stop::Operator),
            ],
        )
        // The startup fold is refused, so the head is still standing crooked
        // when the script wakes it and the engage pins it there: the loop opens
        // with no posture at all.
        .standing_elsewhere()
        .gating_engage(0)
        // The engage outlasts the raise step, so the first thing the loop is
        // asked for once torque is on is the keep.
        .engage_taking(Duration::from_millis(SCRIPT_STEP_MS * 3));

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        let engaged = acts
            .iter()
            .rposition(|act| *act == Act::Engage)
            .expect("the script engages the machine");
        // The first thing torque does is hold. The stow at the end of the run is
        // the shutdown's own fold, which every ending owes whatever was running.
        assert_eq!(acts[engaged + 1], Act::Hold, "{acts:?}");
        let stopped = acts
            .iter()
            .rposition(|act| *act == Act::Release)
            .expect("the run releases");
        assert!(
            !acts[engaged..stopped - 1].contains(&Act::Move(Posture::Stow)),
            "a keep commanded the fold it exists to prevent: {acts:?}"
        );
        assert!(sink.saw("motion_keep"));
    }

    /// The keep is said once, when it becomes the live base command — not once
    /// a dwell for as long as it is the answer, which is the rule the lapse
    /// line already follows.
    #[test]
    fn a_keep_is_said_once_and_not_once_a_dwell() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [
                Event::Wait,
                Event::RaiseThenKeep,
                Event::Wait,
                Event::Wait,
                Event::Wait,
                Event::Stop(Stop::Operator),
            ],
        )
        .standing_elsewhere()
        .gating_engage(0)
        .engage_taking(Duration::from_millis(SCRIPT_STEP_MS * 3));

        let (_, _, sink) = driven(&shared, machine);

        let said = sink.all_fields("motion_keep");
        assert_eq!(said.len(), 1, "{said:?}");
        assert_eq!(said[0]["seq"], json!(1));
        assert_eq!(
            sink.said()
                .lines
                .iter()
                .filter(|line| line.contains("keep"))
                .count(),
            1,
        );
    }

    /// The canonical case, and the one the other keep tests skip past: the raise
    /// completes, the head is at `Up`, and *then* the keep comes due. What makes
    /// it a no-op is that the loop offers its own posture back and the filter
    /// drops it; a filter that re-issued the move would re-plan a move to where
    /// the head already is at every keep boundary, which is a twitch on the
    /// bench and nothing at all in a suite that never lets a raise finish.
    #[test]
    fn a_keep_against_a_posture_the_head_is_holding_commands_nothing() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [
                Event::RaiseThenKeep,
                Event::Wait,
                Event::Wait,
                Event::Stop(Stop::Operator),
            ],
        )
        .sleeping();

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        let raised = acts
            .iter()
            .position(|act| *act == Act::Stream(BasePlan::To(Posture::Up)))
            .expect("the script raises the head");
        let stopped = acts
            .iter()
            .rposition(|act| *act == Act::Release)
            .expect("the run releases");
        // Everything between the raise and the shutdown's own fold is a hold.
        assert!(
            acts[raised + 1..stopped - 1]
                .iter()
                .all(|act| *act == Act::Hold),
            "the keep commanded a move: {acts:?}"
        );
        let said = sink.all_fields("motion_keep");
        assert_eq!(said.len(), 1, "{said:?}");
    }

    /// Said once per transition into the answer, not once per script: a script
    /// that keeps, raises and keeps again says so twice, which is what clearing
    /// the watch on every other answer buys.
    ///
    /// The waits outnumber the script's steps because a dwell is cut to
    /// [`DWELL`] as well as to the next boundary, so a [`SCRIPT_STEP_MS`] gap
    /// takes more than one of them to cross. The stop has to arrive after the
    /// last keep comes due or the run ends before the thing under test happens.
    #[test]
    fn a_second_keep_after_a_raise_is_said_again() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [
                Event::KeepRaiseKeep,
                Event::Wait,
                Event::Wait,
                Event::Wait,
                Event::Wait,
                Event::Wait,
                Event::Wait,
                Event::Wait,
                Event::Wait,
                Event::Wait,
                Event::Stop(Stop::Operator),
            ],
        )
        .sleeping();

        let (_, _, sink) = driven(&shared, machine);

        let said = sink.all_fields("motion_keep");
        assert_eq!(said.len(), 2, "{said:?}");
    }

    /// A `keep`-only script reaching a resting machine puts torque on nothing.
    ///
    /// `wants_up` matches the raise and only the raise, and this is the test of
    /// that: the wire's other base command means "do not move", and answering it
    /// by engaging nine servos would be the opposite of what it says. Nothing in
    /// the code says so but a doc comment, so a widened match — waking on any
    /// ask that is not `Unchanged`, say — would go unnoticed without it.
    #[test]
    fn a_keep_only_script_never_wakes_a_resting_machine() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [
                Event::KeepOnly,
                Event::Wait,
                Event::Wait,
                Event::Stop(Stop::Operator),
            ],
        );

        let (outcome, acts, _) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert!(
            !acts.contains(&Act::Engage),
            "a keep woke a resting machine: {acts:?}"
        );
    }

    /// A `keep` that lands mid-move stops the move where it has got to.
    ///
    /// The whole point of the base command: a publisher changing overlays
    /// mid-motion wants the base left alone, and a raise that ran on to its
    /// target would move the base a whole move further than the publisher ever
    /// asked for. The head then holds between the two postures — a pose no
    /// posture names — so no `motion_posture` is claimed for it and the freeze
    /// reports itself instead, once.
    #[test]
    fn a_keep_mid_move_freezes_the_head_where_it_is() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [Event::Raise, Event::Wait, Event::Stop(Stop::Operator)],
        )
        // The keep replaces the running script while the raise is still
        // streaming, which is the only way the freeze ever happens.
        .interrupting_stream(2, Event::KeepOnly);

        let (_, acts, sink) = driven(&shared, machine);

        let raised = acts
            .iter()
            .position(|act| *act == Act::Stream(BasePlan::To(Posture::Up)))
            .expect("the script raises the head");
        assert_eq!(
            acts[raised + 1],
            Act::Hold,
            "the raise was stopped where it had got to and the head holds there \
             rather than being driven again: {acts:?}"
        );
        let lines = &sink.said().lines;
        assert_eq!(
            lines.iter().filter(|line| line.contains("keep")).count(),
            1,
            "the keep is claimed once, by the freeze that made it true: {lines:?}"
        );
        assert_eq!(
            sink.fields("motion_keep_froze")
                .expect("the freeze is its own event")["abandoned"],
            json!("up"),
            "the freeze says which move it stopped"
        );
        assert!(
            !sink.saw("motion_keep"),
            "the hold arm claimed a keep the freeze had already claimed"
        );
        assert_eq!(
            sink.all_fields("motion_posture")
                .iter()
                .map(|fields| fields["state"].clone())
                .collect::<Vec<_>>(),
            [json!("stow")],
            "the frozen raise reported reaching a posture the head is not at"
        );
    }

    /// A posture restated after a freeze moves the head again.
    ///
    /// The freeze leaves the loop's posture unknown, which is the state the
    /// fold default and every later ask already handle: the head is somewhere
    /// between two postures, so the next named one is a move and not a no-op —
    /// and the tick shapes it from the setpoint the freeze left, which is what
    /// makes restating `up` a way to carry on rather than a step.
    #[test]
    fn a_posture_restated_after_a_freeze_moves_the_head_again() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [
                Event::Raise,
                Event::Raise,
                Event::Wait,
                Event::Stop(Stop::Operator),
            ],
        )
        .interrupting_stream(2, Event::KeepOnly);

        let (_, acts, _) = driven(&shared, machine);

        let froze = acts
            .iter()
            .position(|act| *act == Act::Hold)
            .expect("the keep stops the raise and the head holds");
        assert!(
            acts[froze..].contains(&Act::Stream(BasePlan::To(Posture::Up))),
            "the restated raise was taken for a posture the head already held: {acts:?}"
        );
    }

    /// A script that expires after freezing the head still folds it.
    ///
    /// The lapse is the daemon's leave to go back to the minimum risk
    /// condition, and a frozen head is no more exempt from it than a holding
    /// one: the unknown posture is not stow, so the stow is commanded and the
    /// ordinary rest follows it.
    #[test]
    fn a_script_that_expires_after_a_freeze_stows_the_head() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [
                Event::Raise,
                Event::Lapse,
                Event::Wait,
                Event::Wait,
                Event::Stop(Stop::Operator),
            ],
        )
        .interrupting_stream(2, Event::KeepOnly)
        .sleeping();

        let (_, acts, sink) = driven(&shared, machine);

        let froze = acts
            .iter()
            .position(|act| *act == Act::Hold)
            .expect("the keep stops the raise and the head holds");
        assert!(
            acts[froze..].contains(&Act::Stream(BasePlan::To(Posture::Stow))),
            "the lapse left the head frozen where it was: {acts:?}"
        );
        assert!(acts.contains(&Act::Release), "and it went back to rest");
        assert!(sink.saw("motion_script_expired"));
    }

    /// The layering tests, and the fixtures that belong to them alone.
    mod overlays {
        use super::*;

        /// The motion an antennas-only clip is named by in these tests.
        const WIGGLE: &str = "test/wiggle";
        /// A head-only one, for what a motion that drives the head does to a base.
        const NOD: &str = "test/nod";
        /// A head-only motion of two frames: short enough that it spends well
        /// inside a paced base drive, leaving the rest of that drive streaming
        /// bare.
        const BLINK: &str = "test/blink";

        /// A daemon holding both fixture motions, and the schedule they play
        /// against.
        ///
        /// The library is read off a real directory by the real walk, because a
        /// daemon that plays a motion is a daemon that read a document.
        fn with_motions() -> (tempfile::TempDir, Arc<Shared>) {
            let sink = Collect::default();
            let (dir, motions) = crate::library::fixtures::loaded(
                &[
                    (
                        "wiggle.json",
                        crate::library::fixtures::clip(WIGGLE, 10, 1.0),
                    ),
                    (
                        "nod.json",
                        crate::library::fixtures::head_clip(NOD, 10, 0.002),
                    ),
                    (
                        "blink.json",
                        crate::library::fixtures::head_clip(BLINK, 2, 0.0002),
                    ),
                ],
                &sink,
            );
            (dir, Arc::new(Shared::with_motions(POD, motions)))
        }

        /// A machine whose script plays `motion` from the top and is then stopped.
        fn playing(shared: &Arc<Shared>, motion: &'static str) -> Fake {
            Fake::new(
                shared,
                [
                    Event::Play(motion),
                    Event::Wait,
                    Event::Stop(Stop::Operator),
                ],
            )
        }

        /// A motion plays over the base its own script commands, composed into one
        /// setpoint per control period.
        ///
        /// The layered case end to end: the script raises the head and plays a
        /// motion from the same instant, so the overlay rides a reference that is
        /// itself moving — the case that needs no tracker to exercise, and the one
        /// the whole design is shaped around.
        #[test]
        fn a_motion_plays_over_the_base_its_script_commands() {
            let (_dir, shared) = with_motions();
            let machine = playing(&shared, WIGGLE);
            let composed = machine.composed();

            let (outcome, acts, sink) = driven(&shared, machine);

            assert_eq!(outcome, Outcome::Released(Stop::Operator));
            assert!(
                acts.contains(&Act::Stream(BasePlan::To(Posture::Up))),
                "the raise was commanded on its own rather than under the motion: {acts:?}"
            );
            assert!(
                !acts.contains(&Act::Move(Posture::Up)),
                "the base was commanded twice: {acts:?}"
            );
            assert_eq!(
                sink.fields("motion_play").expect("the motion is announced")["name"],
                json!(WIGGLE)
            );
            let periods = composed.borrow();
            assert!(
                periods.len() > 8,
                "the motion played fewer periods than its base transition took: {}",
                periods.len()
            );
            assert!(
                periods
                    .iter()
                    .any(|(base, _)| base.head_pose_body != periods[0].0.head_pose_body),
                "the base never moved, so nothing was layered over a moving reference"
            );
            assert!(
                periods
                    .iter()
                    .any(|(base, out)| out.antennas != base.antennas),
                "the motion drove nothing"
            );
        }

        /// An antennas-only motion says nothing at all about the head or the body.
        ///
        /// The mask is exact, not approximate: the channels no overlay drives are
        /// the base's own numbers, bit for bit. Anything else is a motion that
        /// pins a head somebody else is steering — the failure the whole delta-and-
        /// mask representation exists to prevent.
        #[test]
        fn an_antennas_only_motion_leaves_the_head_and_the_body_untouched() {
            let (_dir, shared) = with_motions();
            let machine = Fake::new(
                &shared,
                [
                    Event::Raise,
                    Event::KeepAndPlay(WIGGLE),
                    Event::Wait,
                    Event::Stop(Stop::Operator),
                ],
            );
            let composed = machine.composed();

            let (_, acts, _) = driven(&shared, machine);

            assert!(
                acts.contains(&Act::Stream(BasePlan::Held)),
                "the motion moved the base it was asked to leave alone: {acts:?}"
            );
            let periods = composed.borrow();
            assert!(!periods.is_empty(), "nothing was composed");
            for (base, out) in periods.iter() {
                assert_eq!(
                    out.head_pose_body, base.head_pose_body,
                    "an antennas-only motion moved the head"
                );
                assert_eq!(
                    out.body_yaw, base.body_yaw,
                    "an antennas-only motion turned the body"
                );
            }
            assert!(
                periods
                    .iter()
                    .any(|(base, out)| out.antennas != base.antennas),
                "the antennas were never driven either"
            );
        }

        /// A head-driving motion composes with a base transition without pinning
        /// the channels it does not drive.
        ///
        /// The other half of the mask: the head is the base's pose *and* the
        /// motion's delta at once, while the antennas and the body stay the base's
        /// alone.
        #[test]
        fn a_head_motion_composes_with_the_base_it_rides() {
            let (_dir, shared) = with_motions();
            let machine = playing(&shared, NOD);
            let composed = machine.composed();

            let (_, _, _) = driven(&shared, machine);

            let periods = composed.borrow();
            assert!(
                periods
                    .iter()
                    .any(|(base, out)| out.head_pose_body != base.head_pose_body),
                "the head motion contributed nothing to the composition"
            );
            for (base, out) in periods.iter() {
                assert_eq!(
                    out.antennas, base.antennas,
                    "a head motion moved an antenna"
                );
                assert_eq!(out.body_yaw, base.body_yaw, "a head motion turned the body");
            }
        }

        /// A composed setpoint the machine will not take drops the overlays and
        /// keeps the base.
        ///
        /// The doctrine's own disposition for a plan of ours the tick refuses:
        /// nothing parks, nothing latches, and the experiment that failed is the
        /// layering rather than the script. The base is re-acquired bare on a
        /// later pass, and the overlays of that script are not tried again — a
        /// refusal that re-entered would refuse once a period for as long as the
        /// window stayed open.
        ///
        /// The dwell between the two runs is the other half of it: the refused
        /// plan is still the plan, so a loop that retried it on the next
        /// instruction would spin against a machine that keeps saying no.
        #[test]
        fn a_refused_setpoint_drops_the_overlays_and_keeps_the_base() {
            let (_dir, shared) = with_motions();
            let machine = Fake::new(
                &shared,
                [
                    Event::Play(WIGGLE),
                    Event::Wait,
                    Event::Wait,
                    Event::Stop(Stop::Operator),
                ],
            )
            .refusing_setpoint(0, 3);

            let (outcome, acts, sink) = driven(&shared, machine);

            assert_eq!(
                outcome,
                Outcome::Released(Stop::Operator),
                "a refused setpoint faulted the daemon"
            );
            let streamed = acts
                .iter()
                .position(|act| matches!(act, Act::Stream(_)))
                .expect("the motion started");
            assert_eq!(
                acts[streamed + 1],
                Act::Hold,
                "the refused plan was re-offered without a dwell between: {acts:?}"
            );
            assert!(
                acts[streamed + 1..].contains(&Act::Stream(BasePlan::To(Posture::Up))),
                "the base was never re-acquired after the refusal: {acts:?}"
            );
            assert_eq!(
                sink.fields("motion_overlays_dropped")
                    .expect("the drop is reported")["motions"],
                json!([WIGGLE]),
                "the drop does not say which motion was playing"
            );
            assert_eq!(
                sink.all_fields("motion_play").len(),
                1,
                "the refused overlays were played again"
            );
            assert!(
                !sink.saw("motion_fault"),
                "a plan the tick refused was answered as a fault"
            );
            assert_eq!(
                motion_lines(&sink),
                [
                    "motion: stow -> up, under a motion",
                    "motion: wherever it was left -> up",
                ],
                "the re-acquisition names a posture the head is not at, or a \
                 bare drive was narrated as a composed one"
            );
        }

        /// A refusal on the run's very first composed setpoint leaves the base
        /// where the pass found it.
        ///
        /// The machine took nothing, so nothing moved: the loop asks for the
        /// next setpoint only once the last one is behind it, and a run whose
        /// first offer is refused never gets a second period. Forgetting the
        /// posture there would cost the next drive its starting point for a
        /// head that never left it.
        #[test]
        fn a_refusal_before_the_first_setpoint_lands_keeps_the_posture() {
            let (_dir, shared) = with_motions();
            let machine = Fake::new(
                &shared,
                [
                    Event::Play(WIGGLE),
                    Event::Wait,
                    Event::Wait,
                    Event::Stop(Stop::Operator),
                ],
            )
            .refusing_setpoint(0, 0);

            let (outcome, _acts, sink) = driven(&shared, machine);

            assert_eq!(outcome, Outcome::Released(Stop::Operator));
            let moves = sink.all_fields("motion_move");
            assert_eq!(
                moves[1]["from"],
                json!("stow"),
                "the re-acquisition forgot a posture the head never left: {moves:?}"
            );
            assert_eq!(moves[1]["to"], json!("up"));
        }

        /// A refusal of a base drive no window had reached is not the overlays'.
        ///
        /// With one arm for every scripted drive, a bare base runs through the
        /// same stream a layered one does, and the tick's answer to it says
        /// nothing about any clip: reporting it as a dropped overlay names a
        /// layering that never existed, and the ending it earns is the base's
        /// ending rather than a drop of clips that were not there.
        ///
        /// The refusal here lands on the run's first setpoint, so the head is
        /// still at the posture it started from: the owed stow is commanded
        /// from a folded machine and settles at once.
        #[test]
        fn a_bare_base_refusal_is_not_blamed_on_the_overlays() {
            let (_dir, shared) = with_motions();
            let machine = Fake::new(
                &shared,
                [
                    Event::RaiseThenPlay(NOD),
                    Event::Wait,
                    Event::Wait,
                    Event::Stop(Stop::Operator),
                ],
            )
            .pacing_stream()
            // The first period of the raise: the play step is still a few
            // periods out, so nothing is composed over the base.
            .refusing_setpoint(0, 0);

            let (outcome, acts, sink) = driven(&shared, machine);

            assert_eq!(outcome, Outcome::Released(Stop::Operator));
            let refused = sink
                .fields("motion_base_refused")
                .expect("the bare base drive's refusal is reported as its own");
            assert_eq!(refused["plan"], json!("up"));
            assert_eq!(refused["reason"], json!("step_too_large"));
            assert_eq!(
                refused["joint"],
                json!(JointId::AntennaRight.to_string()),
                "the base's refusal names its joint in a form a drop's does not, \
                 so the two halves no longer aggregate: {refused:?}"
            );
            assert!(
                !sink.saw("motion_overlays_dropped"),
                "a bare base refusal was reported as a dropped overlay: {sink:?}"
            );
            let said = sink.said().lines;
            assert!(
                said.contains(
                    &"motion: the machine would not take the base (up). winding down.".to_owned()
                ),
                "the console does not say the session is ending: {said:?}"
            );
            assert!(
                !said.iter().any(|line| line.contains("offered again")),
                "the console promises a re-offer the loop no longer makes: {said:?}"
            );
            wound_down_acts(&acts, BasePlan::To(Posture::Up));
            assert!(
                !sink.saw("motion_play"),
                "the script went on playing through the ending its base earned: {sink:?}"
            );
            assert_eq!(
                sink.fields("motion_incident")
                    .expect("the ending is recorded")["disposition"],
                json!("rest"),
                "a plan the tick refused latched something: {sink:?}"
            );
            let events = sink.said().events;
            let rested = events
                .iter()
                .rposition(|(name, _)| name == "motion_resting")
                .expect("the loop came back to rest");
            let ended = events
                .iter()
                .position(|(name, _)| name == "motion_incident")
                .expect("the ending is recorded");
            assert!(
                rested > ended,
                "the last rest is not the one the wind-down ended at: {events:?}"
            );
        }

        /// A refusal landing after the run's window has spent is the base's too.
        ///
        /// A drive goes on streaming until the base arrives, so a brief motion
        /// early in a long move leaves the rest of that move bare — and the
        /// setpoint the machine refuses there carried no overlay. Blaming the
        /// clips for it names a layering that had already ended, and drops
        /// overlays that had nothing to do with the ending: what the machine
        /// refused was the base, and the base's refusal ends the session.
        #[test]
        fn a_refusal_after_a_window_has_spent_belongs_to_the_base() {
            let (_dir, shared) = with_motions();
            let machine = Fake::new(
                &shared,
                [
                    Event::SpendThenPlay(BLINK, NOD),
                    Event::Wait,
                    Event::Wait,
                    Event::Wait,
                    Event::Wait,
                    Event::Stop(Stop::Operator),
                ],
            )
            .pacing_stream()
            // Late in the raise: the two-frame motion has spent several periods
            // back and the second window is a third of a second out, so the
            // refused setpoint is the bare base.
            .refusing_setpoint(0, BARE_AFTER_SPEND_PERIOD);

            let (outcome, acts, sink) = driven(&shared, machine);

            assert_eq!(outcome, Outcome::Released(Stop::Operator));
            assert!(
                sink.saw("motion_base_refused"),
                "a refusal of a bare setpoint went unreported as the base's: {sink:?}"
            );
            assert!(
                !sink.saw("motion_overlays_dropped"),
                "a spent window was dropped again by a refusal it was not in: {sink:?}"
            );
            let played: Vec<_> = sink
                .all_fields("motion_play")
                .into_iter()
                .map(|fields| fields["name"].clone())
                .collect();
            assert_eq!(
                played,
                vec![json!(BLINK)],
                "a window the session had already ended still played: {sink:?}"
            );
            wound_down_acts(&acts, BasePlan::To(Posture::Up));
        }

        /// A refused setpoint under a base that was only holding ends the
        /// session too.
        ///
        /// The plan shape where the ending is least obvious and most needed: a
        /// `keep` asks the base to stand still, so a refusal there costs no
        /// travel to re-offer and would sit torqued against the same rejection
        /// once a period for as long as the script kept asking. The period the
        /// script's last window spends on is where a held base is offered bare.
        #[test]
        fn a_refused_held_base_ends_the_session() {
            let (_dir, shared) = with_motions();
            let machine = Fake::new(
                &shared,
                [
                    Event::Raise,
                    Event::KeepAndPlay(BLINK),
                    Event::Wait,
                    Event::Wait,
                    Event::Stop(Stop::Operator),
                ],
            )
            .refusing_setpoint(1, HELD_SPEND_PERIOD);

            let (outcome, acts, sink) = driven(&shared, machine);

            assert_eq!(outcome, Outcome::Released(Stop::Operator));
            let refused = sink
                .fields("motion_base_refused")
                .expect("a held base the machine would not take is reported as the base's");
            assert_eq!(
                refused["plan"],
                json!("held"),
                "the report names a posture for a base that was asked to stand still"
            );
            assert!(
                !sink.saw("motion_overlays_dropped"),
                "a spent window was blamed for a held base's refusal: {sink:?}"
            );
            let expected = [
                Act::Watch,
                Act::Engage,
                Act::Stream(BasePlan::To(Posture::Up)),
                Act::Hold,
                Act::Stream(BasePlan::Held),
                Act::Move(Posture::Stow),
                Act::Release,
            ];
            assert_eq!(
                acts.get(..expected.len()),
                Some(&expected[..]),
                "a refused held base was re-offered instead of winding the \
                 session down: {acts:?}"
            );
            assert!(
                acts[expected.len()..].iter().all(|act| *act == Act::Watch),
                "the answered script was driven again: {acts:?}"
            );
        }

        /// A bare re-acquisition the machine also refuses ends the session.
        ///
        /// The two responses in sequence, which is the whole of the layering:
        /// dropping the overlays changes the next offer, so it earns one
        /// recovery; the bare base it re-offers has nothing left to change, so
        /// its refusal is terminal. A loop that gave the second one a dwell too
        /// would hold the head torqued against a plan the machine keeps
        /// rejecting.
        #[test]
        fn a_bare_re_acquisition_the_machine_also_refuses_ends_the_session() {
            let (_dir, shared) = with_motions();
            let machine = Fake::new(
                &shared,
                [
                    Event::Play(WIGGLE),
                    Event::Wait,
                    Event::Wait,
                    Event::Stop(Stop::Operator),
                ],
            )
            // The layered refusal, then the bare re-acquisition it hands to the
            // next pass, refused on its first setpoint.
            .refusing_setpoint(0, 3)
            .refusing_setpoint(1, 0);

            let (outcome, acts, sink) = driven(&shared, machine);

            assert_eq!(outcome, Outcome::Released(Stop::Operator));
            assert_eq!(
                sink.all_fields("motion_overlays_dropped").len(),
                1,
                "the overlays were dropped for a refusal they were not in: {sink:?}"
            );
            let events = sink.said().events;
            let dropped = events
                .iter()
                .position(|(name, _)| name == "motion_overlays_dropped")
                .expect("the layered refusal drops the overlays");
            let bare = events
                .iter()
                .position(|(name, _)| name == "motion_base_refused")
                .expect("the bare re-acquisition's refusal is the base's");
            assert!(
                bare > dropped,
                "the bare refusal is not the one that followed the drop: {events:?}"
            );
            let expected = [
                Act::Watch,
                Act::Engage,
                Act::Stream(BasePlan::To(Posture::Up)),
                Act::Hold,
                Act::Stream(BasePlan::To(Posture::Up)),
                Act::Move(Posture::Stow),
                Act::Release,
            ];
            assert_eq!(
                acts.get(..expected.len()),
                Some(&expected[..]),
                "the twice-refused base was offered a third time: {acts:?}"
            );
            assert!(
                acts[expected.len()..].iter().all(|act| *act == Act::Watch),
                "the answered script was driven again: {acts:?}"
            );
        }

        /// A script that lands as the base is being refused is not marked
        /// answered.
        ///
        /// The mark exists to stop the loop cycling torque against the script
        /// whose plan just failed. Taken from whatever is accepted at response
        /// time it names the wrong script: a replacement that landed during the
        /// run never asked for the plan the machine rejected, and marking it
        /// would leave the head resting through a script nothing ever answers.
        #[test]
        fn a_script_landing_as_the_base_is_refused_is_still_tried() {
            let (_dir, shared) = with_motions();
            let machine = Fake::new(
                &shared,
                [
                    Event::RaiseThenPlay(NOD),
                    Event::Wait,
                    Event::Wait,
                    Event::Wait,
                    Event::Stop(Stop::Operator),
                ],
            )
            .pacing_stream()
            // The replacement lands on the period the machine refuses the bare
            // base, which is the window the mark can be read in.
            .interrupting_stream(0, Event::Raise)
            .refusing_setpoint(0, 0);

            let (outcome, acts, sink) = driven(&shared, machine);

            assert_eq!(outcome, Outcome::Released(Stop::Operator));
            assert!(
                sink.saw("motion_base_refused"),
                "the run did not end on the bare refusal this test is about: {sink:?}"
            );
            let released = acts
                .iter()
                .position(|act| *act == Act::Release)
                .expect("the refused base wound the session down");
            assert!(
                acts[released..].contains(&Act::Engage),
                "the script that landed while the old one was being refused was \
                 marked answered, and the head never rose for it: {acts:?}"
            );
            assert!(
                acts[released..].contains(&Act::Stream(BasePlan::To(Posture::Up))),
                "the fresh script was engaged for and then not driven: {acts:?}"
            );
        }

        /// A `keep` landing after a refusal holds silently.
        ///
        /// The freeze says which command stopped the head, and a refusal is not
        /// the keep: attributing the machine's own answer to the script would
        /// send an incident looking at the publisher instead of at the step
        /// bounds that produced it.
        #[test]
        fn a_keep_after_a_refusal_holds_without_claiming_a_freeze() {
            let (_dir, shared) = with_motions();
            let machine = Fake::new(
                &shared,
                [
                    Event::Play(WIGGLE),
                    Event::KeepOnly,
                    Event::Wait,
                    Event::Stop(Stop::Operator),
                ],
            )
            .refusing_setpoint(0, 3);

            let (outcome, _acts, sink) = driven(&shared, machine);

            assert_eq!(outcome, Outcome::Released(Stop::Operator));
            assert!(
                !sink.saw("motion_keep_froze"),
                "the keep claimed a drive the machine's refusal had already ended"
            );
            assert!(
                sink.saw("motion_keep"),
                "the keep went unsaid altogether: {:?}",
                sink.said().lines
            );
        }

        /// A stop asked for while a motion is playing ends the run on the period it
        /// arrives on.
        ///
        /// The loop's own stop check is at the top of a pass, which a streamed run
        /// does not reach until it ends by itself — so the check inside the compose
        /// closure is the whole of what gets a stop answered promptly. Without it a
        /// stop landing a period into a long motion would be sat on for the rest of
        /// the motion and its blend-out, with the head still being commanded
        /// composed setpoints after the daemon had been told to go to the minimum
        /// risk condition.
        #[test]
        fn a_stop_during_a_motion_ends_the_run_on_the_period_it_arrives() {
            let (_dir, shared) = with_motions();
            let machine =
                playing(&shared, WIGGLE).interrupting_stream(3, Event::Stop(Stop::Operator));
            let composed = machine.composed();

            let (outcome, acts, sink) = driven(&shared, machine);

            assert_eq!(outcome, Outcome::Released(Stop::Operator));
            assert_eq!(
                composed.borrow().len(),
                3,
                "the run carried on past the stop"
            );
            // Deliberately silent about the motion it cut. The truncation
            // reports exist so a capture can tell a clip a republish or a lapse
            // truncated from one that played out — both leave the daemon
            // running and the trace otherwise unmarked. A stop, like a fault,
            // ends the session and narrates itself; the clip stopping is what
            // stopping means, and one more line about it would be noise on the
            // way out.
            assert!(
                !sink.saw("motion_overlays_replaced") && !sink.saw("motion_overlays_lapsed"),
                "the stop reported the motion it ended as a script disposition"
            );
            let streamed = acts
                .iter()
                .position(|act| matches!(act, Act::Stream(_)))
                .expect("the motion started");
            assert!(
                acts[streamed..].contains(&Act::Move(Posture::Stow)),
                "the stop did not fold the head: {acts:?}"
            );
            assert!(
                acts.contains(&Act::Release),
                "and it did not go back to rest: {acts:?}"
            );
        }

        /// A base command the running script changes ends the streamed run and the
        /// players ride through it.
        ///
        /// One script, two base commands: the run is right only for as long as the
        /// base answer it was planned from stands, so it ends and the loop plans the
        /// new base in the one place that decides that for every other pass. The
        /// overlay is not part of what changed — rebuilding the players would ramp a
        /// full-weight motion back up from zero, which on a machine is a visible
        /// jerk and a motion that starts over instead of finishing.
        #[test]
        fn a_base_change_ends_the_run_and_the_motion_rides_through_it() {
            let (_dir, shared) = with_motions();
            let machine = Fake::new(
                &shared,
                [
                    Event::PlayThenLower(WIGGLE),
                    Event::Wait,
                    Event::Stop(Stop::Operator),
                ],
            )
            .pacing_stream();
            let composed = machine.composed();
            let marks = machine.stream_marks();

            let (_, acts, sink) = driven(&shared, machine);

            let streams: Vec<&Act> = acts
                .iter()
                .filter(|act| matches!(act, Act::Stream(_)))
                .collect();
            assert_eq!(
                streams,
                vec![
                    &Act::Stream(BasePlan::To(Posture::Up)),
                    &Act::Stream(BasePlan::To(Posture::Stow)),
                ],
                "the base change did not end the run and start the new base: {acts:?}"
            );
            assert_eq!(
                sink.all_fields("motion_play").len(),
                1,
                "the players were rebuilt across the base change"
            );
            let boundary = marks.borrow()[1];
            let periods = composed.borrow();
            let (base, out) = periods[boundary];
            assert_ne!(
                out.antennas, base.antennas,
                "the motion came back at zero weight after the base change"
            );
            assert_eq!(
                motion_lines(&sink),
                [
                    "motion: stow -> up, under a motion",
                    "motion: wherever it was left toward up -> stow, under a motion",
                ],
                "a composed transition was narrated as a bare one"
            );
        }

        /// A replacement landing mid-motion drops the players of the script it
        /// replaced and plays its own.
        ///
        /// Replacement is the wire's own update model, and an overlay is no
        /// exception to it: the timeline that opened these windows is gone, so the
        /// players are gone with it — not faded, which would have two scripts'
        /// motions on the head at once, and not carried, which would play a motion
        /// nothing is asking for any more. The base is untouched by any of it: the
        /// replacement names the posture the head is already holding, so the run it
        /// entered on stays right and no second base command is issued.
        #[test]
        fn a_replacement_mid_motion_drops_the_players_and_plays_its_own() {
            let (_dir, shared) = with_motions();
            let machine =
                playing(&shared, WIGGLE).interrupting_stream(REPLACED_AT, Event::Play(NOD));
            let composed = machine.composed();
            let marks = machine.stream_marks();

            let (outcome, acts, sink) = driven(&shared, machine);

            assert_eq!(outcome, Outcome::Released(Stop::Operator));
            assert_eq!(
                acts.iter()
                    .filter(|act| matches!(act, Act::Stream(_)))
                    .count(),
                1,
                "the base was commanded again for a replacement that asked for the same posture: \
             {acts:?}"
            );
            assert_eq!(
                played(&sink),
                vec![WIGGLE.to_owned(), NOD.to_owned()],
                "the replacement did not take over the overlays"
            );
            cut_short(&sink, WIGGLE);
            let started = acts
                .iter()
                .position(|act| matches!(act, Act::Stream(_)))
                .expect("the first motion started");
            assert!(
                !acts[started..].contains(&Act::Move(Posture::Up)),
                "the head re-travelled to the posture it was already holding: {acts:?}"
            );
            let start = marks.borrow()[0] + REPLACED_AT as usize;
            let periods = composed.borrow();
            let after = periods
                .get(start..)
                .expect("the run played on past the replacement");
            took_over(after);
        }

        /// A replacement that opens with `keep` starts its motion over the base
        /// exactly where the replaced script left it.
        ///
        /// The on-the-fly overlay change the `keep` base command exists for: the
        /// publisher swaps the motion without restating a posture, so the run ends
        /// on the changed base answer, no base move is issued at all, and the new
        /// motion rides the setpoint the last period commanded.
        #[test]
        fn a_replacement_that_keeps_the_base_plays_over_where_it_was_left() {
            let (_dir, shared) = with_motions();
            let machine =
                playing(&shared, WIGGLE).interrupting_stream(REPLACED_AT, Event::KeepAndPlay(NOD));
            let composed = machine.composed();
            let marks = machine.stream_marks();

            let (outcome, acts, sink) = driven(&shared, machine);

            assert_eq!(outcome, Outcome::Released(Stop::Operator));
            let streams: Vec<&Act> = acts
                .iter()
                .filter(|act| matches!(act, Act::Stream(_)))
                .collect();
            assert_eq!(
                streams,
                vec![
                    &Act::Stream(BasePlan::To(Posture::Up)),
                    &Act::Stream(BasePlan::Held),
                ],
                "the replacement's keep did not hold the base where it was: {acts:?}"
            );
            let started = acts
                .iter()
                .position(|act| matches!(act, Act::Stream(_)))
                .expect("the first motion started");
            assert!(
                !acts[started..].contains(&Act::Move(Posture::Up)),
                "a keep issued a base move: {acts:?}"
            );
            assert_eq!(
                played(&sink),
                vec![WIGGLE.to_owned(), NOD.to_owned()],
                "the replacement did not take over the overlays"
            );
            cut_short(&sink, WIGGLE);
            let start = marks.borrow()[1];
            let periods = composed.borrow();
            let after = periods
                .get(start..)
                .expect("the run played on past the replacement");
            took_over(after);
        }

        /// The streamed period a replacement lands on in the tests above: far
        /// enough in that the motion it replaces is at full weight, and early
        /// enough that the run has periods left to show what the replacement did.
        const REPLACED_AT: u32 = 3;

        /// The motions announced as started, in the order they started.
        fn played(sink: &Collect) -> Vec<String> {
            sink.all_fields("motion_play")
                .iter()
                .map(|fields| {
                    fields["name"]
                        .as_str()
                        .expect("a play event names its motion")
                        .to_owned()
                })
                .collect()
        }

        /// What the composed periods from a replacement onwards say about the
        /// takeover: the replaced script's antennas motion contributes nothing any
        /// more, and the replacement's own head motion reaches the head.
        fn took_over(after: &[(JointTargets, JointTargets)]) {
            for (base, out) in after {
                assert_eq!(
                    out.antennas, base.antennas,
                    "the replaced script's antennas motion was still being commanded"
                );
            }
            assert!(
                after
                    .iter()
                    .any(|(base, out)| out.head_pose_body != base.head_pose_body),
                "the replacement's own motion never reached the head"
            );
        }

        /// The replacement named the motion it cut short, so a capture can tell a
        /// truncated motion from one that played out.
        fn cut_short(sink: &Collect, motion: &str) {
            assert_eq!(
                sink.fields("motion_overlays_replaced")
                    .expect("the replacement says what it cut short")["motions"],
                json!([motion])
            );
        }

        /// A replacement landing on a streamed run's own first period leaves the
        /// posture the loop is still holding.
        ///
        /// The base plan and the overlay windows come from one read of the schedule,
        /// so a run that enters is right about both — but a replacement can still
        /// land between that read and the run's first period, and the run then ends
        /// having commanded nothing. The head has not moved: forgetting its posture
        /// would have the next pass re-command the posture it is already holding,
        /// and the transition line and event would describe a move that never ran.
        #[test]
        fn a_run_that_commands_nothing_leaves_the_posture_alone() {
            let (_dir, shared) = with_motions();
            let machine = playing(&shared, WIGGLE).interrupting_stream(0, Event::Lower);
            let composed = machine.composed();

            let (outcome, acts, sink) = driven(&shared, machine);

            assert_eq!(outcome, Outcome::Released(Stop::Operator));
            assert!(
                composed.borrow().is_empty(),
                "the run commanded a setpoint after all"
            );
            assert!(
                !acts.contains(&Act::Stream(BasePlan::To(Posture::Stow))),
                "the loop forgot a posture it was holding and re-commanded it: {acts:?}"
            );
            assert!(
                sink.all_fields("motion_move")
                    .iter()
                    .all(|fields| fields["reason"] == json!("shutdown")),
                "a transition that never ran was announced"
            );
        }

        /// A machine that stops taking commands while a motion is playing parks the
        /// daemon, exactly as one that stops mid-move does.
        ///
        /// The line between the two refusals a streamed run can end with: a
        /// composed setpoint the tick would not take is ours and costs the
        /// overlays, while a machine that has stopped answering is the platform and
        /// costs the session.
        #[test]
        fn a_machine_that_stops_answering_mid_motion_parks() {
            let (_dir, shared) = with_motions();
            let machine = Fake::new(&shared, [Event::Play(WIGGLE), Event::Wait])
                .refusing_stream_with(0, bus_failure());

            let (outcome, acts, _) = driven(&shared, machine);

            assert!(
                matches!(outcome, Outcome::Faulted(_)),
                "a bus that stopped carrying commands under a motion was survived: {outcome:?}"
            );
            assert!(
                acts.contains(&Act::ReleaseNow),
                "torque was not written off on the spot: {acts:?}"
            );
        }

        /// A script that lapses while a motion is playing folds the head, and says
        /// which motion the lapse cut short.
        ///
        /// Expiry is the daemon's leave to return to the minimum risk condition and
        /// an overlay is no exemption from it: the players are dropped where they
        /// are, the stow is commanded, and the ordinary rest follows.
        ///
        /// Reported as a lapse rather than as a replacement, and reported at all:
        /// a lapse mid-motion truncates a clip exactly as a republish does, and it
        /// reaches no `sync` — the script it belonged to is gone, so nothing opens
        /// a window again — which is why the loop asks about it at the top of every
        /// pass.
        #[test]
        fn a_script_that_lapses_mid_motion_folds_the_head() {
            let (_dir, shared) = with_motions();
            let machine = Fake::new(
                &shared,
                [
                    Event::Play(WIGGLE),
                    Event::Wait,
                    Event::Wait,
                    Event::Stop(Stop::Operator),
                ],
            )
            .interrupting_stream(3, Event::Lapse)
            .sleeping();

            let (_, acts, sink) = driven(&shared, machine);

            let streamed = acts
                .iter()
                .position(|act| matches!(act, Act::Stream(_)))
                .expect("the motion started");
            assert!(
                acts[streamed..].contains(&Act::Stream(BasePlan::To(Posture::Stow))),
                "the lapse left the head up with a motion playing: {acts:?}"
            );
            assert!(acts.contains(&Act::Release), "and it went back to rest");
            assert!(sink.saw("motion_script_expired"));
            let fields = sink
                .fields("motion_overlays_lapsed")
                .expect("the lapse says what it cut short");
            assert_eq!(fields["motions"], json!([WIGGLE]));
            assert!(
                !sink.saw("motion_overlays_replaced"),
                "a lapse was reported as a republish: {sink:?}"
            );
        }

        /// A play step that comes due while the base is already streaming joins
        /// the run that is under way, at its first frame.
        ///
        /// The plainly-authored script: raise the head, and a moment later play
        /// something over it. Nothing about that shape says "wait for the
        /// base".
        #[test]
        fn a_play_step_due_mid_drive_joins_the_run_already_under_way() {
            let (_dir, shared) = with_motions();
            let machine = Fake::new(
                &shared,
                [
                    Event::RaiseThenPlay(NOD),
                    Event::Wait,
                    Event::Stop(Stop::Operator),
                ],
            )
            .pacing_stream();
            let composed = machine.composed();

            let (outcome, acts, sink) = driven(&shared, machine);

            assert_eq!(outcome, Outcome::Released(Stop::Operator));
            assert_eq!(
                acts.iter()
                    .filter(|act| **act == Act::Stream(BasePlan::To(Posture::Up)))
                    .count(),
                1,
                "the raise was driven twice to pick the window up: {acts:?}"
            );
            let played = sink
                .fields("motion_play")
                .expect("the window opened and the run took it up");
            assert!(
                played["joined_ms"]
                    .as_u64()
                    .is_some_and(|joined| joined <= JOIN_SLACK_MS),
                "the clip joined late: {played}"
            );
            assert_eq!(
                motion_lines(&sink),
                ["motion: stow -> up"],
                "one drive, narrated once, in the words that were true when it started"
            );
            assert!(
                composed
                    .borrow()
                    .iter()
                    .any(|(base, out)| out.head_pose_body != base.head_pose_body),
                "the clip never reached the head it joined"
            );
        }

        /// The showcase shape: a clip due just after a posture change joins the
        /// drive that change started.
        ///
        /// One script, two base drives and a window that opens inside the
        /// second.
        #[test]
        fn a_clip_due_after_a_posture_change_joins_that_drive() {
            let (_dir, shared) = with_motions();
            let machine = Fake::new(
                &shared,
                [
                    Event::TurnThenPlay(NOD),
                    Event::Wait,
                    Event::Wait,
                    Event::Stop(Stop::Operator),
                ],
            )
            .pacing_stream();
            let marks = machine.stream_marks();
            let composed = machine.composed();

            let (outcome, acts, sink) = driven(&shared, machine);

            assert_eq!(outcome, Outcome::Released(Stop::Operator));
            let raised = acts
                .iter()
                .position(|act| *act == Act::Stream(BasePlan::To(Posture::Up)))
                .expect("the turn raises the head");
            assert!(
                acts[raised + 1..].contains(&Act::Stream(BasePlan::To(Posture::Stow))),
                "the stow step never became a drive of its own: {acts:?}"
            );
            let played = sink
                .fields("motion_play")
                .expect("the window opened and the run took it up");
            assert!(
                played["joined_ms"]
                    .as_u64()
                    .is_some_and(|joined| joined <= JOIN_SLACK_MS),
                "the clip joined late: {played}"
            );
            // The composition it reached is the *second* drive's: the window
            // opened after the stow step, so anything the clip did to a period
            // of the first one would be a window played before it was open.
            let second = *marks.borrow().get(1).expect("two drives");
            assert!(
                composed.borrow()[..second]
                    .iter()
                    .all(|(base, out)| out.head_pose_body == base.head_pose_body),
                "the clip reached a drive that ended before its window opened"
            );
            assert!(
                composed.borrow()[second..]
                    .iter()
                    .any(|(base, out)| out.head_pose_body != base.head_pose_body),
                "the clip never reached the drive it was due in"
            );
        }
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
                Act::Stream(BasePlan::To(Posture::Up)),
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
                Act::Stream(BasePlan::To(Posture::Up)),
                Act::Hold,
                Act::Stream(BasePlan::To(Posture::Stow)),
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

    /// A script that lands while the head is still on its way up turns it
    /// around, rather than being served after the raise finishes.
    ///
    /// A raise takes seconds; a daemon that waited one out would put the head
    /// down a whole move after it was asked for. The run ends on the period the
    /// answer changed and the next one drives from where the head has got to,
    /// which is the turnaround.
    #[test]
    fn a_script_landing_mid_raise_turns_that_move_around() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [Event::Raise, Event::Wait, Event::Stop(Stop::Operator)],
        )
        .interrupting_stream(2, Event::Lower);

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        let raised = acts
            .iter()
            .position(|act| *act == Act::Stream(BasePlan::To(Posture::Up)))
            .expect("the script raises the head");
        assert_eq!(
            acts[raised + 1],
            Act::Stream(BasePlan::To(Posture::Stow)),
            "the raise ran on to its target before the fold started: {acts:?}"
        );
        let moves = sink.all_fields("motion_move");
        assert_eq!(
            moves.len(),
            3,
            "the turnaround is a move of its own in the capture: {moves:?}"
        );
        assert_eq!(moves[1]["from"], json!(null));
        assert_eq!(
            moves[1]["from_toward"],
            json!("up"),
            "the capture cannot say which drive the turnaround interrupted: {moves:?}"
        );
        assert_eq!(moves[1]["to"], json!("stow"));
        assert_eq!(moves[1]["reason"], json!("script"));
        assert_eq!(
            motion_lines(&sink),
            [
                "motion: stow -> up",
                "motion: wherever it was left toward up -> stow",
            ],
            "the console names a posture the head is not at, or loses the \
             drive the turnaround interrupted"
        );
    }

    /// A replacement with nothing due yet leaves the drive under way alone.
    ///
    /// A script whose first step is in the future asks for nothing until that
    /// step comes due, and the absence of an instruction is not an instruction:
    /// the raise it landed on runs on to its target. Ending the run on it would
    /// leave the head between two postures with no script speaking, which the
    /// loop answers by folding — a visible reversal toward stow that no script
    /// asked for, and one the script's own first step would turn around again.
    #[test]
    fn a_replacement_with_nothing_due_yet_leaves_the_drive_alone() {
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
        .pacing_stream()
        .interrupting_stream(2, Event::LaterLower);

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert!(
            !acts.contains(&Act::Stream(BasePlan::To(Posture::Stow))),
            "a replacement with nothing due folded the head: {acts:?}"
        );
        assert_eq!(
            sink.fields("motion_posture").expect("the raise arrived")["state"],
            json!("up"),
            "the drive never reached the posture the script asked for"
        );
        assert_eq!(
            motion_lines(&sink),
            ["motion: stow -> up"],
            "the gap before the replacement's first step was narrated as a drive"
        );
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
            .interrupting_stream(2, Event::Stop(Stop::Operator));

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert_eq!(
            acts,
            [
                Act::Watch,
                Act::Engage,
                Act::Stream(BasePlan::To(Posture::Up)),
                // The shutdown fold, measured, from where the raise had got to.
                Act::Move(Posture::Stow),
                Act::Release,
            ]
        );
        let moves = sink.all_fields("motion_move");
        assert_eq!(moves.len(), 2, "{moves:?}");
        assert_eq!(moves[1]["reason"], json!("shutdown"));
        assert_eq!(
            moves[1]["from"],
            json!(null),
            "the fold claimed to start from a posture the head was not at"
        );
        assert_eq!(
            moves[1]["from_toward"],
            json!("up"),
            "the shutdown fold does not say which drive the stop interrupted: {moves:?}"
        );
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
        let machine = Fake::new(&shared, [Event::Raise, Event::Wait]).interrupting(0, Event::Raise);

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert_eq!(
            acts,
            [
                Act::Watch,
                Act::Engage,
                Act::Stream(BasePlan::To(Posture::Up)),
                Act::Hold,
                Act::Hold,
                // Move 0: the shutdown fold, with a raise landing inside it.
                Act::Move(Posture::Stow),
                Act::Release,
            ]
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
                Act::Stream(BasePlan::To(Posture::Up)),
                Act::Hold,
                Act::Hold,
                Act::Move(Posture::Stow),
                Act::Release,
            ]
        );
        let startup = sink
            .fields("motion_startup")
            .expect("the startup verdict is captured");
        assert_eq!(startup["at_stow"], json!(false));
    }

    /// The other direction at loop level: a wake landing inside a fold the loop
    /// commanded turns the head back up without waiting the fold out.
    ///
    /// A follow-up question arriving as the head starts down is the ordinary
    /// case: the fold's run ends on the period the answer changed and the raise
    /// is driven from where the head is, not from stow.
    #[test]
    fn a_wake_landing_mid_fold_turns_the_head_back_up() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [Event::Raise, Event::Lower, Event::Stop(Stop::Operator)],
        )
        .interrupting_run(1, 2, Event::Raise);

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert_eq!(
            acts,
            [
                Act::Watch,
                Act::Engage,
                Act::Stream(BasePlan::To(Posture::Up)),
                Act::Hold,
                Act::Stream(BasePlan::To(Posture::Stow)),
                Act::Stream(BasePlan::To(Posture::Up)),
                Act::Hold,
                // The stop, and the fold it takes from a head that is up again.
                Act::Move(Posture::Stow),
                Act::Release,
            ]
        );
        let moves = sink.all_fields("motion_move");
        assert_eq!(moves[2]["from"], json!(null));
        assert_eq!(moves[2]["to"], json!("up"));
        assert_eq!(moves[2]["reason"], json!("script"));
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
                Act::Stream(BasePlan::To(Posture::Up)),
                Act::Hold,
                Act::Stream(BasePlan::To(Posture::Stow)),
                // The wake lands on this dwell, inside the rest delay: the head
                // goes back up with no release and no engage between the two.
                Act::Hold,
                Act::Stream(BasePlan::To(Posture::Up)),
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
                Act::Stream(BasePlan::To(Posture::Up)),
                Act::Hold,
                // The lapse: nothing said stow, the timeout did.
                Act::Stream(BasePlan::To(Posture::Stow)),
                Act::Hold,
                Act::Hold,
                // The stop's own fold, measured, from a head the loop believes
                // is already there.
                Act::Move(Posture::Stow),
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
                Act::Stream(BasePlan::To(Posture::Up)),
                Act::Hold,
                Act::Stream(BasePlan::To(Posture::Stow)),
                Act::Hold,
                Act::Move(Posture::Stow),
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
                Act::Stream(BasePlan::To(Posture::Up)),
                Act::Hold,
                Act::Move(Posture::Stow),
                Act::Release,
            ]
        );
    }

    /// The shutdown stow is commanded even when the loop believes the head is
    /// already folded.
    ///
    /// A scripted arrival is trajectory-clock arithmetic and never a
    /// measurement, so "already at stow" is a belief this daemon holds and not
    /// a fact it has checked. The shutdown fold is the act that puts the
    /// machine at the minimum risk condition with a measurement behind it, and
    /// from a head genuinely there it costs one settle window.
    #[test]
    fn the_shutdown_stow_is_commanded_from_a_head_the_loop_believes_is_folded() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [Event::Raise, Event::Lower, Event::Stop(Stop::Operator)],
        );

        let (outcome, acts) = drive(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        let folded = acts
            .iter()
            .position(|act| *act == Act::Stream(BasePlan::To(Posture::Stow)))
            .expect("the script lowers the head");
        assert!(
            acts[folded..].contains(&Act::Move(Posture::Stow)),
            "the stop released a head on an arrival nobody measured: {acts:?}"
        );
        assert_eq!(acts.last(), Some(&Act::Release));
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
    fn a_fault_mid_drive_writes_torque_off_before_it_parks() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Raise, Event::Wait]).refusing_stream(0);

        let (outcome, acts) = drive(&shared, machine);

        let Outcome::Faulted(report) = outcome else {
            panic!("the move up refused");
        };
        assert_eq!(report.stage, FaultStage::Motion);
        assert!(report.detail.contains("not carrying commands"), "{report}");
        assert_eq!(
            acts,
            [
                Act::Watch,
                Act::Engage,
                Act::Stream(BasePlan::To(Posture::Up)),
                Act::ReleaseNow
            ],
            "the fault ending is the nine writes and nothing else",
        );
        assert_eq!(shared.fault(), Some(&report));
    }

    /// Every class of ending takes the maneuver its class asks for, and leaves
    /// the daemon where that class says.
    ///
    /// The table this whole routing exists to be: one representative ending per
    /// class, each driven through the real loop, asserted on what reached the
    /// machine and on whether the daemon may take another script afterwards. A
    /// class routed to the wrong maneuver is a head dropped where it should have
    /// been stowed, or an outage over an event the machine recovers from by
    /// itself, and nothing else in this file would notice either.
    #[test]
    fn every_class_of_ending_takes_the_maneuver_its_class_asks_for() {
        // (what the move answered, what reached the machine after the engage,
        //  whether a person has to look before the next engage)
        let table: [(Refusal, &[Act], bool); 5] = [
            // Nothing changed and nothing is wrong: the head still comes down,
            // because this daemon is nobody's operator standing next to a bench.
            (
                Refusal::new(ErrorClass::Refuse, "the tick would not take the command"),
                &[Act::Move(Posture::Stow), Act::Release],
                false,
            ),
            (
                move_aborted(),
                &[Act::Move(Posture::Stow), Act::Release],
                false,
            ),
            (
                Refusal::new(
                    ErrorClass::ImmediateAllTorqueOffToRest,
                    "the wind-down did not finish",
                ),
                &[Act::ReleaseNow],
                false,
            ),
            (head_servo_fault(), &[Act::MaskedStow], true),
            (bus_failure(), &[Act::ReleaseNow], true),
        ];

        for (refusal, maneuver, latches) in table {
            let shared = Arc::new(Shared::new(POD));
            let machine =
                Fake::new(&shared, [Event::Raise, Event::Wait]).refusing_stream_with(0, refusal);

            let (outcome, acts) = drive(&shared, machine);

            let named = format!("{acts:?} / {outcome}");
            assert_eq!(
                &acts[..3],
                [
                    Act::Watch,
                    Act::Engage,
                    Act::Stream(BasePlan::To(Posture::Up))
                ],
                "{named}"
            );
            assert_eq!(&acts[3..], maneuver, "{named}");
            assert_eq!(
                matches!(outcome, Outcome::Faulted(_)),
                latches,
                "the wrong disposition: {named}"
            );
            assert_eq!(
                shared.fault().is_some(),
                latches,
                "the fault cell is for park-class endings and no other: {named}"
            );
        }
    }

    /// A hand on the head is met by a controlled stow, and the daemon goes back
    /// to work.
    ///
    /// The outage this routing exists to end: before it, a grab took torque off
    /// on the spot and parked the daemon until somebody restarted it, over an
    /// event that is over the moment the hand comes away.
    #[test]
    fn a_grabbed_head_stows_under_control_and_is_still_taking_scripts() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Raise, Event::Wait])
            .refusing_stream_with(0, head_obstructed())
            .having_raised([Entry::Fault {
                fault: Fault::HeadObstructed {
                    joint: JointId::Leg(3),
                    error: 0.4,
                },
                at: Duration::from_millis(5),
            }]);

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator), "{acts:?}");
        assert_eq!(
            &acts[1..],
            [
                Act::Engage,
                Act::Stream(BasePlan::To(Posture::Up)),
                Act::Move(Posture::Stow),
                Act::Release
            ],
            "the head was stowed under control and released, not dropped: {acts:?}"
        );
        assert_eq!(shared.fault(), None, "nothing latched over a grab");
        assert_eq!(
            shared.accept(&holding(9, Posture::Up), Instant::now()),
            Delivered::Scheduled(Acceptance::Accepted),
            "the daemon is still taking scripts"
        );
        assert_eq!(
            shared.antennas(),
            Antennas::Ok,
            "and with both antennas: a grab took nothing out of service"
        );
        let fields = sink
            .fields("motion_incident")
            .expect("a wind-down back to rest is reported");
        assert_eq!(fields["slug"], json!("head_obstructed"));
        assert_eq!(fields["disposition"], json!("rest"));
        // And somebody is told, because nothing else here would tell them: the
        // fault cell is untouched and the daemon is taking scripts, so a run
        // that stowed a grabbed head looks from outside like one that did not.
        let (told, count) = shared
            .take_incident()
            .expect("a wind-down owes an operator the news");
        assert!(told.contains("head_obstructed"), "{told}");
        assert_eq!(count, 1);
        // The whole story, not half of it: a condition is what happened and a
        // maneuver is what answered it, and the daemon is the layer that
        // commanded this one, so the daemon is the only thing that can say it
        // started and that it finished. Asserted end to end, because the record
        // for the incident an unattended machine takes most often is the whole
        // of the evidence anybody gets.
        assert_eq!(
            fields["incident"].as_str(),
            Some(
                "head_obstructed: leg 4 is 0.4000 rad from its goal and not closing → slow_stow \
                 started → slow_stow completed"
            ),
            "{fields:?}"
        );
        assert!(
            sink.fields("motion_fault").is_none(),
            "an ending nothing latched must not be reported as a park"
        );
    }

    /// A wind-down the wire defeats is reported as the wire, not as the hand it
    /// set out to answer.
    ///
    /// Two conditions in one incident, and what the machine is left by is the
    /// later one: the tick raised the grab, this daemon commanded the controlled
    /// stow, and the stow's own transactions stopped coming back. Nothing else
    /// sees that second condition — the tick did not raise it, and the library
    /// handed it back rather than consuming it — so a record without it publishes
    /// the park under a hand that is long gone, and an alert rule watching for a
    /// wire that died under torque never fires for the one case it exists for.
    #[test]
    fn a_stow_the_wire_defeats_is_named_by_the_wire() {
        let shared = Arc::new(Shared::new(POD));
        let grab = Fault::HeadObstructed {
            joint: JointId::Leg(3),
            error: 0.4,
        };
        let machine = Fake::new(&shared, [Event::Raise, Event::Wait])
            .refusing_stream_with(0, head_obstructed())
            .refusing_move_with(0, bus_failure())
            .having_raised([Entry::Fault {
                fault: grab,
                at: Duration::from_millis(5),
            }]);

        let (outcome, acts, sink) = driven(&shared, machine);

        let Outcome::Faulted(report) = outcome else {
            panic!("the wire stopped carrying under the stow: {acts:?}");
        };
        assert_eq!(
            &acts[1..],
            // The stow was commanded and refused, so nothing records it as a
            // move; what the machine saw next is the immediate release.
            [
                Act::Engage,
                Act::Stream(BasePlan::To(Posture::Up)),
                Act::ReleaseNow
            ],
            "the defeated stow escalated rather than being re-commanded: {acts:?}"
        );
        assert_eq!(report.slug, Some("bus_failure"));
        assert_eq!(
            report
                .record
                .iter()
                .filter_map(|entry| match entry {
                    Entry::Fault { fault, .. } => Some(*fault),
                    Entry::Response { .. } => None,
                })
                .collect::<Vec<_>>(),
            [grab, bus_silent()],
            "both conditions, each once, in the order they happened: {:?}",
            report.record
        );
        assert!(
            report
                .story()
                .expect("a story")
                .ends_with("bus_failure: the bus is not carrying commands: servo 12: silence"),
            "the story ends on the condition the machine was left in: {report:?}"
        );
        assert_eq!(
            sink.fields("motion_fault").expect("a park is reported")["slug"],
            json!("bus_failure")
        );
    }

    /// A wind-down back to Resting does not re-raise the head for the script
    /// that just failed, and does raise it for the next one.
    ///
    /// The other half of not latching: a daemon that went back to Resting and
    /// immediately re-engaged for the same ask would cycle torque on and off
    /// against whatever ended the raise, for as long as the script asked for the
    /// head up. What changes the answer is a new ask, not another pass.
    #[test]
    fn a_wind_down_leaves_the_head_down_until_a_new_script_asks() {
        let engages = |acts: &[Act]| acts.iter().filter(|act| **act == Act::Engage).count();

        let shared = Arc::new(Shared::new(POD));
        let once = Fake::new(&shared, [Event::Raise, Event::Wait, Event::Wait])
            .refusing_stream_with(0, head_obstructed())
            .unstopped();
        let (_, acts) = drive(&shared, once);
        assert_eq!(
            engages(&acts),
            1,
            "the script that failed asked again: {acts:?}"
        );

        let shared = Arc::new(Shared::new(POD));
        let again = Fake::new(
            &shared,
            [Event::Raise, Event::Wait, Event::Raise, Event::Wait],
        )
        .refusing_stream_with(0, head_obstructed())
        .unstopped();
        let (_, acts) = drive(&shared, again);
        assert_eq!(
            engages(&acts),
            2,
            "a fresh script is a fresh ask and is tried: {acts:?}"
        );
    }

    /// A servo dropping out during a wind-down that began at rest latches it.
    ///
    /// The sticky maximum: the stow carries on without the servo that went — the
    /// head is not dropped — but a servo that dropped out is not something the
    /// next wake may engage past, so the ending parks. The disposition comes back
    /// from the maneuver rather than being re-derived up here.
    #[test]
    fn a_servo_dropping_out_of_a_wind_down_latches_an_ending_that_would_have_rested() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Raise, Event::Wait])
            // The raise meets a hand; the stow that answers it loses a servo.
            .refusing_stream_with(0, head_obstructed())
            .refusing_move_with(0, head_servo_fault())
            .having_raised([Entry::Fault {
                fault: Fault::HeadServoFault {
                    joint: JointId::Leg(3),
                    id: 13,
                    bits: 0x20,
                },
                at: Duration::from_millis(5),
            }]);

        let (outcome, acts, sink) = driven(&shared, machine);

        let Outcome::Faulted(report) = outcome else {
            panic!("a servo dropped out of the wind-down: {acts:?}");
        };
        assert_eq!(
            &acts[1..],
            // The stow it re-commanded is the move that refused, so nothing
            // records it; what the machine saw next is the masked wind-down
            // rather than the immediate release.
            [
                Act::Engage,
                Act::Stream(BasePlan::To(Posture::Up)),
                Act::MaskedStow
            ],
            "the stow carried on masked rather than being given up on: {acts:?}"
        );
        assert_eq!(report.slug, Some("head_servo_fault"));
        assert!(
            report.detail.contains("not closing") && report.detail.contains("hardware error"),
            "both endings are in the detail: {report}"
        );
        no_wind_down_was_reported(&shared, &sink);
    }

    /// The escalation of a defeated stow runs on the clock that stow started on.
    ///
    /// One stow window covers the whole controlled response however many servos
    /// drop out of it. A daemon that let the library open a fresh budget here
    /// would drive the head for two whole stow windows against whatever defeated
    /// the first — and the window is sized so that a hand holding the head is not
    /// pushed indefinitely. The bench answers this same event with one window;
    /// nothing but this compares them.
    #[test]
    fn a_defeated_stow_escalates_on_the_clock_it_started_with() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Raise, Event::Wait])
            .refusing_stream_with(0, head_obstructed())
            .refusing_move_with(0, head_servo_fault());
        let deadlines = machine.stow_deadlines();

        let (outcome, acts) = drive(&shared, machine);

        assert!(matches!(outcome, Outcome::Faulted(_)), "{acts:?}");
        // The raise ran one step of the fixture's clock before the hand stopped
        // it, so the response opened its window there: that step plus the whole
        // of one stow budget, and not a step further for the stow the servo
        // dropped out of.
        assert_eq!(
            deadlines.borrow().as_slice(),
            [FAKE_MOVE_STEP + FAKE_STOW_BUDGET],
            "the expansion was handed a second stow window instead of the \
             remainder of the first"
        );
    }

    /// The park report carries the condition and the story as values, and the
    /// words are made of them at the sink.
    ///
    /// What an alert rule keys on and what the person it woke reads are the same
    /// entries, so neither can be a sentence somebody parsed back apart.
    #[test]
    fn a_park_carries_the_condition_and_the_story_it_recorded() {
        let raised = Entry::Fault {
            fault: Fault::BusFailure {
                source: BusFailureSource::Transaction {
                    id: 12,
                    kind: WireFailure::Silent,
                },
            },
            // The raise this refused had run its clock: the fixture's session
            // time is one move old when the ending is answered.
            at: FAKE_MOVE_STEP,
        };
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Raise, Event::Wait]).refusing_stream(0);

        let (outcome, _, sink) = driven(&shared, machine);

        let Outcome::Faulted(report) = outcome else {
            panic!("the wire stopped carrying");
        };
        assert_eq!(report.slug, Some("bus_failure"));
        assert_eq!(
            report.record,
            [raised],
            "the condition the wire-holding layer found reached the record as a value"
        );
        let fields = sink.fields("motion_fault").expect("a park is reported");
        assert_eq!(fields["slug"], json!("bus_failure"));
        assert_eq!(fields["incident"], json!(report.story().expect("a story")));
        assert_eq!(
            report.story().as_deref(),
            Some("bus_failure: the bus is not carrying commands: servo 12: silence"),
            "rendered from the entries, once, here"
        );
        no_wind_down_was_reported(&shared, &sink);
    }

    /// A park is not also a wind-down, on either surface.
    ///
    /// The two are structurally exclusive today — one disposition chooses between
    /// them — and nothing holds that but this. A regression that noted both would
    /// page an operator twice off one ending: once to say nothing will move again
    /// until somebody restarts the daemon, and then, on the next chore, to say the
    /// machine needs nothing. The second contradicts the first about whether anybody
    /// has to get up.
    fn no_wind_down_was_reported(shared: &Shared, sink: &Collect) {
        assert_eq!(
            shared.take_incident(),
            None,
            "a park was also reported as an ending that rested"
        );
        assert!(
            sink.fields("motion_incident").is_none(),
            "a park was captured as an ending that rested"
        );
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
    ///
    /// And it names the incident: a minimum risk condition believed rather than
    /// known is the condition the machine was left in, whatever preceded it. Once
    /// in the record and not twice — the release consumed the engagement and
    /// recorded it on the way out, so this layer noting what it was handed would
    /// tell the story twice over.
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
        assert_eq!(report.slug, Some("torque_off_unconfirmed"));
        assert_eq!(
            report
                .record
                .iter()
                .filter(|entry| matches!(
                    entry,
                    Entry::Fault {
                        fault: Fault::TorqueOffUnconfirmed { .. },
                        ..
                    }
                ))
                .count(),
            1,
            "the condition is in the record once: {:?}",
            report.record
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
        assert!(
            !acts.contains(&Act::Stream(BasePlan::To(Posture::Up))),
            "{acts:?}"
        );
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

    /// A limp machine nobody can read is at the minimum risk condition already,
    /// so the watch losing it is a picture lost and not control lost. The
    /// daemon keeps sweeping, says so once, and picks the machine up again on
    /// the first sweep that answers — no fault, no park, no operator.
    #[test]
    fn a_watch_that_stops_answering_keeps_sweeping_and_recovers_by_itself() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [
                Event::Wait,
                Event::Wait,
                Event::Wait,
                Event::Wait,
                Event::Stop(Stop::Operator),
            ],
        )
        .refusing_watch(1, 3);

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert!(!shared.faulted(), "a failing watch parked the daemon");
        assert!(
            acts.iter().filter(|act| **act == Act::Watch).count() >= 5,
            "the watch stopped sweeping: {acts:?}"
        );
        assert_eq!(
            sink.all_fields("resting_watch_lost").len(),
            1,
            "a run of failures is one piece of news, not one per sweep"
        );
        let restored = sink
            .fields("resting_watch_restored")
            .expect("the recovery is captured");
        assert_eq!(restored["failures"], json!(3));
    }

    /// The alert half of the same thing: somebody is told the head cannot be
    /// raised, once per run of failures, while the fault cell stays empty.
    #[test]
    fn a_failing_watch_owes_an_alarm_and_not_a_fault() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [Event::Wait, Event::Wait, Event::Stop(Stop::Operator)],
        )
        .refusing_watch(0, 2);

        let (outcome, _) = drive(&shared, machine);

        assert!(outcome.is_clean(), "{outcome}");
        let alarm = shared.take_watch_alarm().expect("a failing watch owes one");
        assert_eq!(alarm.runs, 1, "one alarm for the run, not one per sweep");
        assert!(alarm.detail.contains("servo 11"), "{}", alarm.detail);
        assert_eq!(shared.fault(), None);
    }

    /// The startup look is the same decision at the worst moment for a park:
    /// boot, unattended, over a bus that is not answering yet. It retries at the
    /// resting cadence and folds the machine on the first sweep that answers.
    #[test]
    fn a_startup_look_that_cannot_read_the_machine_retries_instead_of_parking() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [
                Event::Wait,
                Event::Wait,
                Event::Wait,
                Event::Stop(Stop::Operator),
            ],
        )
        .standing_elsewhere()
        .refusing_watch(0, 2);

        let (outcome, acts, sink) = driven(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert_eq!(
            acts,
            [
                Act::Watch,
                Act::Watch,
                Act::Watch,
                Act::Engage,
                Act::Move(Posture::Stow),
                Act::Release,
                Act::Watch,
            ],
            "the fold waited for a sweep that answered, then ran"
        );
        assert!(sink.saw("resting_watch_lost"));
        assert_eq!(
            sink.fields("motion_startup").expect("the look is reported")["at_stow"],
            json!(false)
        );
    }

    /// A daemon stopped while it is still trying to read a machine over a dead
    /// bus exits rather than retrying forever. Nothing was ever taken hold of,
    /// so there is nothing to fold: the machine is limp already.
    #[test]
    fn a_stop_during_a_startup_retry_ends_the_run_cleanly() {
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Wait, Event::Stop(Stop::Operator)])
            .standing_elsewhere()
            .refusing_watch(0, 9);

        let (outcome, acts) = drive(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert_eq!(acts, [Act::Watch, Act::Watch], "the retry ignored the stop");
    }

    /// The head still refuses to come up while the machine cannot be measured —
    /// an engage plans the pin from a sweep — but a refusal is a refusal: the
    /// daemon stays resting and the next script asks again.
    ///
    /// The refusal here is the unreadable bus and nothing else, so what is
    /// pinned is the loop's answer to it: no fault, no raise, an alert owed, and
    /// a run that goes on to end the ordinary way.
    #[test]
    fn a_wake_over_a_bus_that_cannot_be_read_is_refused_and_not_faulted() {
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
        .refusing_watch(1, 2)
        .unreadable_engage();

        let (outcome, acts) = drive(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert!(!shared.faulted());
        assert!(
            !acts.contains(&Act::Stream(BasePlan::To(Posture::Up))),
            "{acts:?}"
        );
        let (detail, _) = shared
            .take_engage_refusal()
            .expect("a refused engage owes an alert");
        assert!(detail.contains("timed out"), "{detail}");
    }

    /// A gate refusal while the sweeps happen to be failing is still a gate
    /// refusal: two independent reasons an engage can decline, neither of them
    /// a fault, and the one that answered is the one reported.
    #[test]
    fn a_torque_on_gate_refusing_over_a_failing_watch_still_does_not_park() {
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
        .refusing_watch(1, 2)
        .gating_engage(0);

        let (outcome, acts) = drive(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert!(!shared.faulted());
        assert!(
            !acts.contains(&Act::Stream(BasePlan::To(Posture::Up))),
            "{acts:?}"
        );
        let (detail, _) = shared
            .take_engage_refusal()
            .expect("a refused engage owes an alert");
        assert!(detail.contains("below the floor"), "{detail}");
    }

    /// The bus can come back between the last failed sweep and the wake that
    /// follows it, and then the engage's own remedial sweep is what finds it
    /// answering. The failure run ends there.
    ///
    /// Otherwise the surface would say `active/failing` for the whole of a
    /// session — nothing else sweeps until the release — and the probe reading
    /// it would call a robot with its head demonstrably up unable to raise it.
    #[test]
    fn an_engage_that_measures_the_machine_ends_the_failure_run() {
        let dir = temp_dir();
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [
                Event::Wait,
                Event::Raise,
                Event::Lower,
                Event::Wait,
                Event::Wait,
                Event::Stop(Stop::Operator),
            ],
        )
        .refusing_watch(1, 1)
        .watching_state(state_in(&dir));

        let (outcome, acts, trail, sink) = stated(&state_in(&dir), &shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert!(
            acts.contains(&Act::Stream(BasePlan::To(Posture::Up))),
            "{acts:?}"
        );
        assert!(
            trail.contains(&"resting/failing".to_owned()),
            "the run was not open when the wake arrived: {trail:?}"
        );
        assert!(
            !trail
                .iter()
                .any(|seen| seen.ends_with("/failing") && seen.starts_with("active")),
            "a session ran with the surface still saying the machine cannot be read: {trail:?}"
        );
        let fields = sink
            .fields("resting_watch_restored")
            .expect("the run that the engage closed is reported like any other");
        assert_eq!(fields["failures"], json!(1));
    }

    /// The state surface follows the loop rather than describing it afterwards:
    /// the file is replaced in place, so the only way to see the sequence it
    /// passed through is from inside the run, which is what the fixture reads at
    /// every act.
    ///
    /// `active` is deliberately not said until torque is on — a refused engage
    /// leaves the machine resting, and a surface that had already claimed active
    /// would have to be taken back.
    #[test]
    fn the_surface_follows_the_loop_through_every_phase() {
        let dir = temp_dir();
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
        .sleeping()
        .watching_state(state_in(&dir));

        let (outcome, acts, trail, _) = stated(&state_in(&dir), &shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert_eq!(acts.len(), trail.len(), "{acts:?} {trail:?}");
        assert_eq!(
            trail,
            [
                // The startup look, before the loop has said anything.
                "starting/ok",
                // Resting, and the engage that ends it: torque goes on during
                // this act, so the phase is still the honest one.
                "resting/ok",
                "active/ok",
                "active/ok",
                "active/ok",
                "active/ok",
                "active/ok",
                // Released, and watching a limp machine again.
                "resting/ok",
                "resting/ok",
            ]
        );
        assert_eq!(
            value_of(&state_in(&dir), "state").as_deref(),
            Some("stopping"),
            "the last thing a stopped daemon says is that it is stopping"
        );
    }

    /// The failure this surface exists for: a parked daemon does not exit, so
    /// `systemctl is-active` calls it running over a journal that has gone
    /// quiet. The file says otherwise, and carries what stopped it.
    #[test]
    fn a_parked_daemon_says_so_and_says_what_stopped_it() {
        let dir = temp_dir();
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(&shared, [Event::Raise, Event::Wait]).refusing_stream(0);

        let (outcome, _, _, _) = stated(&state_in(&dir), &shared, machine);

        assert!(matches!(outcome, Outcome::Faulted(_)), "{outcome}");
        assert_eq!(
            value_of(&state_in(&dir), "state").as_deref(),
            Some("parked")
        );
        assert_eq!(
            value_of(&state_in(&dir), "fault_stage").as_deref(),
            Some("the motion loop")
        );
        assert!(
            value_of(&state_in(&dir), "fault_detail")
                .is_some_and(|detail| detail.contains("not carrying commands")),
            "the fault detail is what an operator reads before deciding anything"
        );
        assert_eq!(
            value_of(&state_in(&dir), "fault_slug").as_deref(),
            Some("bus_failure"),
            "the condition is named in the one word a probe can key on"
        );
    }

    /// A machine nobody can read is resting, safely, and cannot raise its head.
    /// Both facts are in the file at once, because a probe that saw only the
    /// phase would call a robot that will not answer a wake word ready.
    #[test]
    fn a_failing_watch_shows_on_the_surface_without_moving_the_phase() {
        let dir = temp_dir();
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [
                Event::Wait,
                Event::Wait,
                Event::Wait,
                Event::Wait,
                Event::Stop(Stop::Operator),
            ],
        )
        .refusing_watch(1, 2)
        .watching_state(state_in(&dir));

        let (outcome, _, trail, _) = stated(&state_in(&dir), &shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert!(
            trail.contains(&"resting/failing".to_owned()),
            "the failing watch never reached the surface: {trail:?}"
        );
        assert!(
            trail.iter().all(|seen| !seen.starts_with("parked")),
            "a failing watch parked the daemon: {trail:?}"
        );
        assert_eq!(
            trail.last().map(String::as_str),
            Some("resting/ok"),
            "the flag did not come back with the reads: {trail:?}"
        );
    }

    /// Nothing about this surface may ever stand between the machine and the
    /// minimum risk condition. A run with nowhere to write commands exactly what
    /// the same run with somewhere to write commands, and says why once.
    #[test]
    fn a_surface_that_cannot_be_written_changes_nothing_the_machine_does() {
        let dir = temp_dir();
        let nowhere = state_in(&dir).join("no-such-directory").join("state");
        let events = [
            Event::Raise,
            Event::Lower,
            Event::Wait,
            Event::Stop(Stop::Operator),
        ];

        let shared = Arc::new(Shared::new(POD));
        let (wrote, expected, _) = driven(&shared, Fake::new(&shared, events).sleeping());

        let shared = Arc::new(Shared::new(POD));
        let (outcome, acts, _, sink) =
            stated(&nowhere, &shared, Fake::new(&shared, events).sleeping());

        assert_eq!(outcome, wrote);
        assert_eq!(acts, expected);
        assert_eq!(
            sink.said()
                .lines
                .iter()
                .filter(|line| line.contains("state file"))
                .count(),
            1,
            "a directory that is not there stays not there"
        );
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
            acts[engaged..].contains(&Act::Stream(BasePlan::To(Posture::Up))),
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
        .engage_taking(Duration::from_millis(SCRIPT_STEP_MS));

        // Half way through the engage, so the fixture keeps a margin as wide as
        // the half on either side: the second script has to land after the
        // engage starts and before it answers, and both ends of that window are
        // real sleeps on a machine that may be doing anything else.
        let racing = Arc::clone(&shared);
        let lands_mid_engage = thread::spawn(move || {
            thread::sleep(Duration::from_millis(SCRIPT_STEP_MS / 2));
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
            acts.contains(&Act::Stream(BasePlan::To(Posture::Up))),
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
        .engage_taking(Duration::from_millis(SCRIPT_STEP_MS * 3));

        let (outcome, acts) = drive(&shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        let engaged = acts
            .iter()
            .position(|act| *act == Act::Engage)
            .expect("the turn engages the machine");
        assert_eq!(
            acts.get(engaged + 1),
            Some(&Act::Stream(BasePlan::To(Posture::Stow))),
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
        // The shutdown fold is the only measured move a scripted run makes.
        let machine =
            Fake::new(&shared, [Event::Raise, Event::Stop(Stop::Operator)]).refusing_move(0);

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

    /// The startup fold refusing: a machine found standing, and the stow out of
    /// it faulting before any loop runs.
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
        let machine = Fake::new(&shared, [Event::Raise, Event::Wait]).refusing_stream_unstopped(0);

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

    /// Which joints being out of service means the antennas are, asked of the
    /// whole vocabulary rather than of the two joints anybody would think to
    /// name.
    ///
    /// Membership and not emptiness. A head servo on its way to a park-class
    /// fault lands in the same set, and a predicate that read the set's emptiness
    /// would paint `antennas=degraded` over it — sending an operator to the two
    /// joints on the machine that are fine.
    #[test]
    fn the_antennas_are_degraded_by_an_antenna_and_by_nothing_else() {
        for joint in JointId::ALL {
            let mut out = JointSet::EMPTY;
            out.insert(joint);
            let expected = match joint {
                JointId::AntennaRight | JointId::AntennaLeft => Antennas::Degraded,
                JointId::BodyYaw | JointId::Leg(_) => Antennas::Ok,
            };
            assert_eq!(antennas_of(out), expected, "with {joint} out of service");
        }
        assert_eq!(antennas_of(JointSet::EMPTY), Antennas::Ok);
    }

    /// A session engaged onto antenna bits that had already latched: the health
    /// gate lets the wake through on them rather than refusing presence over two
    /// antennas, so nothing in the session ever announces it and the engage's own
    /// look at the joints out of service is the only thing that can.
    ///
    /// Every surface answers from that one look — the file a probe reads, the
    /// answer a script's sender gets, the alert — and the head goes up and comes
    /// down as it would on a whole machine.
    #[test]
    fn a_session_engaged_without_its_antennas_says_so_everywhere() {
        let dir = temp_dir();
        let path = state_in(&dir);
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [Event::Raise, Event::Wait, Event::Stop(Stop::Operator)],
        )
        .engaging_without([JointId::AntennaRight, JointId::AntennaLeft])
        .watching_state(&path);
        let seen = machine.seen();

        let (outcome, acts, _, sink) = stated(&path, &shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator), "{acts:?}");
        assert!(
            acts.contains(&Act::Stream(BasePlan::To(Posture::Up))),
            "the head kept its presence: {acts:?}"
        );
        assert_eq!(
            value_of(&path, "antennas").as_deref(),
            Some("degraded"),
            "the state file carries it for the probe"
        );
        assert_eq!(
            shared.accept(&holding(9, Posture::Up), Instant::now()),
            Delivered::Scheduled(Acceptance::Accepted),
        );
        assert_eq!(
            shared.antennas(),
            Antennas::Degraded,
            "the one asking us to move the antennas hears it"
        );
        let fields = sink
            .fields("antennas_degraded")
            .expect("the pair leaving the moves is an event of the session");
        assert!(
            fields["detail"]
                .as_str()
                .is_some_and(|detail| detail.contains("right antenna")),
            "{fields}"
        );
        assert!(
            shared.take_antennas_alarm().is_some(),
            "an operator is owed the news once"
        );
        assert!(!shared.faulted(), "two limp antennas are not a fault");
        assert!(
            seen.borrow().iter().any(|record| record.contains("active")),
            "the run really passed through the surface: {:?}",
            seen.borrow()
        );
    }

    /// The other source: the pair leaves the moves in the middle of a session,
    /// which the tick announces. The same cell, the same file, the same answer to
    /// a delivered script — and said once, however many periods report it.
    #[test]
    fn a_pair_that_leaves_the_moves_mid_session_reaches_every_surface_once() {
        let dir = temp_dir();
        let path = state_in(&dir);
        let shared = Arc::new(Shared::new(POD));
        let snagged = TickEvent::AntennasDegraded(Fault::AntennaObstructed {
            joint: JointId::AntennaRight,
            error: 0.4,
        });
        let flagged = TickEvent::AntennasDegraded(Fault::AntennaServoFault {
            joint: JointId::AntennaLeft,
            id: 21,
            bits: 0x20,
        });
        let machine = Fake::new(
            &shared,
            [
                Event::Raise,
                Event::Wait,
                Event::Wait,
                Event::Stop(Stop::Operator),
            ],
        )
        .reporting_moving([vec![snagged]])
        .saying([vec![flagged]]);

        let (outcome, acts, _, sink) = stated(&path, &shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator), "{acts:?}");
        assert_eq!(shared.antennas(), Antennas::Degraded);
        assert_eq!(
            value_of(&path, "antennas").as_deref(),
            Some("degraded"),
            "the mid-session degrade reached the file a probe reads"
        );
        let events = sink.all_fields("antennas_degraded");
        assert_eq!(
            events.len(),
            1,
            "the pair is out of service once, whatever else the tick reports: {events:?}"
        );
        assert!(
            events[0]["detail"]
                .as_str()
                .is_some_and(|detail| detail.contains("right antenna")),
            "{events:?}"
        );
        assert!(
            shared.take_antennas_alarm().is_some(),
            "one flip, one alert"
        );
        let lines = sink.said().lines;
        assert!(
            lines.iter().any(|line| line.contains("left antenna")),
            "the second report still reached the console in the tick's own words: {lines:?}"
        );
        assert!(!shared.faulted());
    }

    /// The next engage retries the pair, so the surfaces have to be able to say
    /// it came back: a record that could only ever go one way would send
    /// somebody to a machine that is whole.
    #[test]
    fn a_re_engage_that_gets_the_pair_back_says_so() {
        let dir = temp_dir();
        let path = state_in(&dir);
        let shared = Arc::new(Shared::new(POD));
        let machine = Fake::new(
            &shared,
            [
                Event::Raise,
                Event::Wait,
                Event::Lower,
                Event::Wait,
                Event::Raise,
                Event::Wait,
                Event::Stop(Stop::Operator),
            ],
        )
        .sleeping()
        .reporting_moving([vec![TickEvent::AntennasDegraded(
            Fault::AntennaObstructed {
                joint: JointId::AntennaRight,
                error: 0.4,
            },
        )]]);

        let (outcome, acts, _, sink) = stated(&path, &shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator), "{acts:?}");
        assert!(
            sink.saw("antennas_degraded"),
            "the first session really lost the pair"
        );
        assert_eq!(
            acts.iter().filter(|act| **act == Act::Engage).count(),
            2,
            "the second wake engaged again: {acts:?}"
        );
        assert_eq!(shared.antennas(), Antennas::Ok);
        assert_eq!(value_of(&path, "antennas").as_deref(), Some("ok"));
    }

    /// The pair leaving the moves during the stow that ends the session still
    /// reaches every surface.
    ///
    /// The case the surfaces are written where the change happens for: this
    /// session is over by the time anything else would state it — no engage
    /// follows to judge the pair again, and the loop is on its way back to
    /// Resting — so a standing condition that arrived on the last move the machine
    /// ever made would be a probe reading `antennas=ok` off a machine that has
    /// lost them.
    #[test]
    fn a_pair_lost_during_the_wind_down_still_reaches_every_surface() {
        let dir = temp_dir();
        let path = state_in(&dir);
        let shared = Arc::new(Shared::new(POD));
        let snagged = TickEvent::AntennasDegraded(Fault::AntennaObstructed {
            joint: JointId::AntennaRight,
            error: 0.4,
        });
        let machine = Fake::new(&shared, [Event::Raise, Event::Wait])
            // The raise meets a hand, and the controlled stow answering it is
            // where the pair goes: the raise's own entry is the first, so the
            // second is the stow's.
            .refusing_stream_with(0, head_obstructed())
            .reporting_moving([vec![], vec![snagged]]);

        let (outcome, acts, _, sink) = stated(&path, &shared, machine);

        assert_eq!(outcome, Outcome::Released(Stop::Operator), "{acts:?}");
        assert_eq!(shared.antennas(), Antennas::Degraded);
        assert_eq!(
            value_of(&path, "antennas").as_deref(),
            Some("degraded"),
            "the session ended without the pair reaching the file a probe reads"
        );
        assert_eq!(sink.all_fields("antennas_degraded").len(), 1);
        assert!(
            shared.take_antennas_alarm().is_some(),
            "one flip, one alert, whichever move it happened on"
        );
        assert!(!shared.faulted(), "a snagged pair is not a park");
    }

    /// And the pair leaving the moves during the *masked* stow, which the library
    /// commands rather than this daemon.
    ///
    /// The maneuver is the library's, so its tick events arrive through the hook
    /// it is handed and nowhere else. A parked machine that shows `antennas=ok`
    /// beside a record naming an antenna is one machine described two ways, on the
    /// file an operator reads before they walk over to it.
    #[test]
    fn a_pair_lost_during_the_masked_stow_reaches_the_parked_surfaces() {
        let dir = temp_dir();
        let path = state_in(&dir);
        let shared = Arc::new(Shared::new(POD));
        let snagged = TickEvent::AntennasDegraded(Fault::AntennaObstructed {
            joint: JointId::AntennaRight,
            error: 0.4,
        });
        let machine = Fake::new(&shared, [Event::Raise, Event::Wait])
            // A hand on the head, then a servo dropping out of the stow that
            // answers it: the response carries on masked, and the pair goes on
            // the way down. Two refused moves pop nothing, so the third entry is
            // the masked stow's.
            .refusing_stream_with(0, head_obstructed())
            .refusing_move_with(0, head_servo_fault())
            .reporting_moving([vec![], vec![], vec![snagged]]);

        let (outcome, acts, _, sink) = stated(&path, &shared, machine);

        assert!(matches!(outcome, Outcome::Faulted(_)), "{acts:?}");
        assert_eq!(acts.last(), Some(&Act::MaskedStow), "{acts:?}");
        assert_eq!(shared.antennas(), Antennas::Degraded);
        assert_eq!(
            value_of(&path, "antennas").as_deref(),
            Some("degraded"),
            "the parked file says the machine still has a pair it has not got"
        );
        assert_eq!(sink.all_fields("antennas_degraded").len(), 1);
        assert!(shared.take_antennas_alarm().is_some());
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
        let mut rail = Rail::new(every);

        assert_eq!(
            rail.cadence(now),
            PollCadence::PositionsAndRail,
            "the first sweep since commissioning has no rail reading to carry forward"
        );
        rail.read(now, PollCadence::PositionsAndRail);

        let soon = now + Duration::from_millis(499);
        assert_eq!(rail.cadence(soon), PollCadence::Positions);
        rail.read(soon, PollCadence::Positions);
        assert_eq!(rail.cadence(now + every), PollCadence::PositionsAndRail);
        assert_eq!(
            rail.cadence(now + Duration::from_secs(3)),
            PollCadence::PositionsAndRail
        );
    }

    /// A failed sweep does not merely fail to read the rail — it makes the
    /// reading already in hand worthless, because nothing bounds how long the
    /// outage lasts. The engage that follows a recovery must not judge its two
    /// torque-on gates on a supply measured before the bus went away.
    #[test]
    fn a_failed_sweep_makes_the_next_one_that_answers_re_read_the_rail() {
        let now = Instant::now();
        let every = Duration::from_millis(500);
        let mut rail = Rail::new(every);
        rail.read(now, PollCadence::PositionsAndRail);
        assert_eq!(rail.cadence(now), PollCadence::Positions);

        rail.lost();

        assert_eq!(rail.cadence(now), PollCadence::PositionsAndRail);
    }

    /// A pre-torque sweep that failed leaves the rail to be read again, and one
    /// that answered leaves what it read standing.
    ///
    /// The wiring, not the arithmetic: `Rail` knows what a lost reading means,
    /// and this is what says the sweeps call it. Dropped from the failure arm,
    /// an engage after an hour-long outage would judge its two torque-on gates
    /// against a supply measured before the outage began, and nothing else
    /// would notice.
    #[test]
    fn a_pre_torque_sweep_carries_the_rail_with_it() {
        let now = Instant::now();
        let mut rail = Rail::new(Duration::from_millis(500));
        rail.read(now, PollCadence::PositionsAndRail);
        assert_eq!(rail.cadence(now), PollCadence::Positions);

        let failed: Result<(), PumpError> =
            pre_torque_sweep(&mut rail, now, PollCadence::Positions, |_| {
                Err(PumpError::TorqueOffUnacked { id: 11 })
            });

        assert!(failed.is_err());
        assert_eq!(
            rail.cadence(now),
            PollCadence::PositionsAndRail,
            "the reading the failed sweep invalidated is still being carried"
        );

        pre_torque_sweep(&mut rail, now, PollCadence::PositionsAndRail, |_| {
            Ok::<(), PumpError>(())
        })
        .expect("a sweep that answered");

        assert_eq!(
            rail.cadence(now),
            PollCadence::Positions,
            "the sweep that answered read the rail and the reading did not stick"
        );
    }

    /// The remedial sweep an engage takes past a stale posture measures the
    /// supply as well as the positions, and a failure refuses the engage rather
    /// than faulting the daemon.
    ///
    /// One word, and the whole unattended lifecycle turns on it: this sweep and
    /// the enable walk after it raise the same refusals, so nothing about the
    /// error says which side of the torque write it came from. Classified
    /// `Fault`, a wake arriving during a bus outage would park the daemon and
    /// wait for a person — which is the availability hole a pre-torque sweep
    /// that never faults exists to close.
    #[test]
    fn a_remedial_sweep_that_cannot_measure_refuses_the_engage() {
        let now = Instant::now();
        let mut rail = Rail::new(Duration::from_millis(500));
        rail.read(now, PollCadence::PositionsAndRail);

        let refused: Result<(), EngageFailed> = remedial_sweep(&mut rail, now, |_| {
            Err(PumpError::TorqueOffUnacked { id: 11 })
        });

        assert!(
            matches!(refused, Err(EngageFailed::Gate(_))),
            "a sweep taken before torque parked the daemon: {refused:?}"
        );
        assert_eq!(rail.cadence(now), PollCadence::PositionsAndRail);

        let mut asked = None;
        remedial_sweep(&mut rail, now, |cadence| {
            asked = Some(cadence);
            Ok::<(), PumpError>(())
        })
        .expect("a sweep that answered");

        assert_eq!(
            asked,
            Some(PollCadence::PositionsAndRail),
            "an engage recovering from an outage judged its gates on positions alone"
        );
    }

    /// The run and not the sweep is what gets reported: a dead bus fails one
    /// every `rest_poll`, and the sweep that finally answers is what says how
    /// many there were.
    #[test]
    fn a_failure_run_is_opened_once_and_closed_with_its_count() {
        let mut run = SweepRun::default();
        assert_eq!(run.recovered(), None, "nothing was failing");

        assert!(run.failed(), "the first failure opens the run");
        assert!(!run.failed());
        assert!(!run.failed());

        assert_eq!(run.recovered(), Some(3));
        assert_eq!(run.recovered(), None, "the run was closed twice");
        assert!(run.failed(), "the next failure opens a fresh run");
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
    ///
    /// What is expected is everything the library judges before a transaction
    /// writes torque — the supply gate, the health gate, and the voltage poll
    /// behind them. Nothing was written for any of the three, the machine is
    /// limp exactly where it stood, and the next script's engage may ask again;
    /// parking a daemon over an unreadable rail is a person's evening for a
    /// condition that clears itself.
    #[test]
    fn only_a_pre_torque_refusal_makes_an_engage_failure_expected() {
        let context = StepContext::reg(SeqStep::PinAndEnable, 13, RegId::TorqueEnable);
        let mid_flight = EngageFailed::from(PumpError::Sequence(SeqError::NoAnswer { context }));
        assert!(
            matches!(mid_flight, EngageFailed::Fault(_)),
            "{mid_flight:?}"
        );

        for (what, refusal) in [
            (
                "the supply gate",
                SeqError::SupplyBelowFloor {
                    context,
                    readings: [5.5; JointId::COUNT],
                    lowest: 5.5,
                    limit: 6.0,
                },
            ),
            (
                "the health gate",
                SeqError::UnhealthyServo {
                    context,
                    bits: 0x20,
                },
            ),
            (
                "the voltage poll behind them",
                SeqError::VoltageLow {
                    context,
                    readings: [5.5; JointId::COUNT],
                    lowest: 5.5,
                    limit: 6.0,
                    waited: Duration::from_secs(30),
                },
            ),
        ] {
            let refused = EngageFailed::from(PumpError::Sequence(refusal));
            assert!(
                matches!(refused, EngageFailed::Gate(_)),
                "{what}: {refused:?}"
            );
        }
    }

    /// The phase an ending is classified at changes both the answer and the
    /// story, so the three sites that choose it are load-bearing.
    ///
    /// A sweep that cannot read the machine while it is resting is a refusal:
    /// nothing is energized, the machine is as safe as it was, and asking again at
    /// the next poll is the whole of the answer — which is what lets this daemon
    /// ride out a bus that goes away for five seconds instead of waiting for a
    /// person. The same failure with the head held up is the wire no longer
    /// carrying commands: torque comes off on the spot, the condition goes into
    /// the record, and the daemon parks. `Machine::commission`, `remedial_sweep`
    /// and the resting `watch` are the pre-torque sites; every other ending in
    /// this module arrives through `From`, which is under torque.
    #[test]
    fn the_phase_an_ending_is_classified_at_decides_what_it_asks_for() {
        let context = StepContext::reg(SeqStep::PinAndEnable, 11, RegId::PresentPosition);
        for error in [
            PumpError::Bus {
                id: 11,
                source: XactError::Timeout {
                    id: 11,
                    waited: Duration::from_millis(20),
                },
            },
            PumpError::Sequence(SeqError::NoAnswer { context }),
        ] {
            let resting = Refusal::pre_torque(&error);
            assert_eq!(
                resting.class(),
                ErrorClass::Refuse,
                "nothing was energized, so asking again later is the answer: {error}"
            );
            assert_eq!(
                resting.unrecorded(),
                None,
                "a refusal names no condition of the machine: {error}"
            );

            let holding = Refusal::under_torque(&error);
            assert_eq!(
                holding.class(),
                ErrorClass::ImmediateAllTorqueOffToPark,
                "the wire stopped carrying under a head this daemon is holding up: {error}"
            );
            assert!(
                matches!(holding.unrecorded(), Some(Fault::BusFailure { .. })),
                "the record is owed the condition, whichever layer found it: {error}"
            );
            assert_eq!(
                resting.to_string(),
                holding.to_string(),
                "the wording is the library's either way; the phase decides the answer"
            );
        }
    }

    /// Which side of the Gate/Fault split each ending class falls on, asked of
    /// the whole class vocabulary rather than of a hand-picked few.
    ///
    /// The predicate this daemon routes on belongs to the library — Gate
    /// exactly when the ending classifies as `Refuse` with torque on — so what
    /// has to be pinned here is the split itself and the classification of the
    /// endings an engage can produce. A sixth `ErrorClass` stops `class_slot`
    /// below from compiling rather than quietly landing every ending it covers
    /// on the Fault side, and an ending reclassified across the line fails its
    /// own row.
    #[test]
    fn every_ending_class_falls_on_the_side_of_the_split_it_belongs_to() {
        let context = StepContext::reg(SeqStep::PinAndEnable, 13, RegId::TorqueEnable);
        // One ending per class. The rest-class immediate torque-off is the one
        // class no `PumpError` carries today: every fault that asks for an
        // immediate release asks for the park with it, and there is nothing to
        // route until one does not.
        let table: [(ErrorClass, Option<PumpError>); 5] = [
            (
                ErrorClass::Refuse,
                Some(PumpError::Sequence(SeqError::SupplyBelowFloor {
                    context,
                    readings: [5.5; JointId::COUNT],
                    lowest: 5.5,
                    limit: 6.0,
                })),
            ),
            (
                ErrorClass::SlowStowToRest,
                Some(PumpError::Fault(Fault::HeadObstructed {
                    joint: JointId::Leg(0),
                    error: 0.5,
                })),
            ),
            (ErrorClass::ImmediateAllTorqueOffToRest, None),
            (
                ErrorClass::MaskedSlowStowToPark,
                Some(PumpError::Fault(Fault::HeadServoFault {
                    joint: JointId::Leg(0),
                    id: 13,
                    bits: 0x20,
                })),
            ),
            (
                ErrorClass::ImmediateAllTorqueOffToPark,
                Some(PumpError::TorqueOffUnacked { id: 13 }),
            ),
        ];

        let mut judged = [false; 5];
        for (class, ending) in table {
            judged[class_slot(class)] = true;
            let Some(ending) = ending else { continue };
            let carried = ending.class(TorquePhase::UnderTorque);
            let named = ending.to_string();
            assert_eq!(carried, class, "{named}");
            assert_eq!(
                matches!(EngageFailed::from(ending), EngageFailed::Gate(_)),
                class == ErrorClass::Refuse,
                "{named} classifies as {class:?}, and an engage put it on the wrong side of \
                 nothing-was-written"
            );
        }
        assert!(
            judged.iter().all(|seen| *seen),
            "an ending class named no representative: {judged:?}"
        );
    }

    /// Which class this is, as a slot in the coverage above.
    ///
    /// Wildcard-free, so a class added to the doctrine cannot be left out of
    /// the table by the table simply not mentioning it.
    fn class_slot(class: ErrorClass) -> usize {
        match class {
            ErrorClass::Refuse => 0,
            ErrorClass::SlowStowToRest => 1,
            ErrorClass::ImmediateAllTorqueOffToRest => 2,
            ErrorClass::MaskedSlowStowToPark => 3,
            ErrorClass::ImmediateAllTorqueOffToPark => 4,
        }
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

    /// The head-group numbers a bench configuration contributes, read off the
    /// bench's own defaults rather than restated here — the raise moved once
    /// already when the machine was measured, and a fixture that transcribes it
    /// is a fixture describing a file it no longer matches.
    fn bench() -> (Duration, Duration) {
        let machine = config::MotionSection::default();
        (
            Duration::from_secs_f64(machine.up_duration_s),
            Duration::from_secs_f64(machine.stow_duration_s),
        )
    }

    /// Neither antenna named by the file being spoken of.
    const NEITHER: [Option<Duration>; 2] = [None, None];

    /// The two files' antenna keys laid over each other, on the bench head-group
    /// clocks above — the only thing these cases vary.
    fn laid(
        bench_antennas: Option<Duration>,
        bench_sides: [Option<Duration>; 2],
        overrides: Overrides,
    ) -> Clocks {
        let (up, stow) = bench();
        Clocks::lay_over(up, stow, bench_antennas, bench_sides, overrides)
    }

    /// One antenna clock, stated by one file.
    fn from(duration: Duration, source: Source) -> Option<Clock> {
        Some(Clock {
            duration,
            from: source,
        })
    }

    /// A daemon whose file says nothing about the durations moves at exactly the
    /// pace the operator tool does — the property that keeps one machine from
    /// having two descriptions of itself.
    #[test]
    fn nothing_stated_leaves_every_clock_to_the_machine() {
        let (up, stow) = bench();
        let clocks = laid(None, NEITHER, Overrides::default());

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
        assert_eq!(clocks.antennas, NEITHER.map(|_| None));
        assert_eq!(clocks.up_durations(), MoveDurations::uniform(up));
        assert_eq!(clocks.stow_durations(), MoveDurations::uniform(stow));
    }

    /// Each of the head-group clocks is overridden alone. Presence pace is what
    /// this file exists to tune, and a daemon that took the raise and quietly
    /// moved the fold with it would be tuning the machine behind the bench
    /// file's back.
    #[test]
    fn a_stated_clock_overrides_the_machines_and_the_others_stand() {
        let (up, stow) = bench();
        let stated = Duration::from_millis(1_400);

        let raised = laid(
            None,
            NEITHER,
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

        let folded = laid(
            None,
            NEITHER,
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
    /// further, so their clocks are independent too: a shared number reaches
    /// both sides on both moves and floors neither head group. This is the whole
    /// point of the split — a raise tuned to be quick is not floored by an
    /// antenna arc.
    #[test]
    fn a_shared_antenna_clock_reaches_both_sides_of_both_moves() {
        let (up, stow) = bench();
        let stated = Duration::from_millis(1_500);
        let clocks = laid(
            None,
            NEITHER,
            Overrides {
                antennas: Some(stated),
                ..Overrides::default()
            },
        );

        assert_eq!(
            clocks.antennas,
            [from(stated, Source::Daemon), from(stated, Source::Daemon)]
        );
        assert_eq!(
            clocks.up_durations(),
            MoveDurations {
                head: up,
                antennas: [stated; 2]
            }
        );
        assert_eq!(
            clocks.stow_durations(),
            MoveDurations {
                head: stow,
                antennas: [stated; 2]
            }
        );
    }

    /// The pair's two tips cross inboard of the head, and a pair sweeping
    /// mirror-symmetrically meets there — so each side takes its own clock, and
    /// a side stated alone must not pull the other side with it. The stated one
    /// runs at what this file says; the other runs at what the pair's shared key
    /// says, and where there is none, at the head group's clock.
    #[test]
    fn each_antenna_takes_its_own_clock_and_leaves_the_other_side_alone() {
        let (up, _) = bench();
        let right = Duration::from_millis(700);
        let left = Duration::from_millis(300);

        let one_side = laid(
            None,
            NEITHER,
            Overrides {
                antenna_sides: [Some(right), None],
                ..Overrides::default()
            },
        );
        assert_eq!(one_side.antennas, [from(right, Source::Daemon), None]);
        assert_eq!(
            one_side.up_durations(),
            MoveDurations {
                head: up,
                antennas: [right, up]
            }
        );

        let staggered = laid(
            None,
            NEITHER,
            Overrides {
                antenna_sides: [Some(right), Some(left)],
                ..Overrides::default()
            },
        );
        assert_eq!(
            staggered.antennas,
            [from(right, Source::Daemon), from(left, Source::Daemon)]
        );
        assert_eq!(
            staggered.up_durations(),
            MoveDurations {
                head: up,
                antennas: [right, left]
            }
        );
    }

    /// Within one file, a side's own key beats that file's shared one — the
    /// chain the motion library resolves for the operator tool, which is where
    /// it is stated and where it stays stated.
    #[test]
    fn a_sides_own_key_beats_the_shared_one_in_the_file_that_states_both() {
        let side = Duration::from_millis(300);
        let shared = Duration::from_millis(1_500);

        let daemon = laid(
            None,
            NEITHER,
            Overrides {
                antennas: Some(shared),
                antenna_sides: [None, Some(side)],
                ..Overrides::default()
            },
        );
        assert_eq!(
            daemon.antennas,
            [from(shared, Source::Daemon), from(side, Source::Daemon)]
        );

        let machine = laid(Some(shared), [None, Some(side)], Overrides::default());
        assert_eq!(
            machine.antennas,
            [from(shared, Source::Bench), from(side, Source::Bench)]
        );
    }

    /// Where both files state an antenna clock, this one wins — the same rule as
    /// the head group's, and it has to be the same rule or an operator tuning one
    /// number would get a different answer depending on which one it was. A
    /// daemon that states a pace for the pair states it for the pair: its shared
    /// key answers for a side the machine's file named, because otherwise the
    /// tuning an operator wrote here would silently reach one antenna only.
    #[test]
    fn a_stated_antenna_clock_beats_the_machines_own() {
        let stated = Duration::from_millis(1_500);
        let clocks = laid(
            Some(Duration::from_secs(1)),
            [Some(Duration::from_millis(700)), None],
            Overrides {
                antennas: Some(stated),
                ..Overrides::default()
            },
        );

        assert_eq!(
            clocks.antennas,
            [from(stated, Source::Daemon), from(stated, Source::Daemon)]
        );
    }

    /// The machine's own antenna clocks reach the moves, and are reported as the
    /// machine's. A bench file that already split the two groups — or the pair —
    /// is not a file this daemon has to be told about twice.
    #[test]
    fn the_machines_antenna_clocks_are_used_and_attributed() {
        let shared = Duration::from_secs(1);
        let left = Duration::from_millis(300);
        let clocks = laid(Some(shared), [None, Some(left)], Overrides::default());

        assert_eq!(
            clocks.antennas,
            [from(shared, Source::Bench), from(left, Source::Bench)]
        );
        assert_eq!(clocks.up_durations().antennas, [shared, left]);
    }

    /// What the startup line and the capture say. Every number and the file
    /// each came from: the override is invisible in the bench configuration, so
    /// a head moving at a pace nobody expects is otherwise two files and a guess
    /// to explain — and which side of the pair is running slow is a question the
    /// crossing makes worth asking.
    #[test]
    fn the_startup_line_names_every_clock_and_the_file_it_came_from() {
        let clocks = laid(
            Some(Duration::from_millis(1_500)),
            NEITHER,
            Overrides {
                up: Some(Duration::from_millis(1_400)),
                antenna_sides: [None, Some(Duration::from_millis(300))],
                ..Overrides::default()
            },
        );

        assert_eq!(
            clocks.to_string(),
            "up 1.400 s (daemon), stow 2.000 s (bench), \
             antennas right 1.500 s (bench), left 0.300 s (daemon)"
        );
        assert_eq!(
            clocks.json(),
            json!({
                "up_ms": 1_400,
                "up_from": "daemon",
                "stow_ms": 2_000,
                "stow_from": "bench",
                "antenna_right_ms": 1_500,
                "antenna_right_from": "bench",
                "antenna_left_ms": 300,
                "antenna_left_from": "daemon",
            })
        );

        // And with no antenna clock anywhere, the line says what the antennas do
        // instead rather than leaving a number out.
        let plain = laid(None, NEITHER, Overrides::default());
        let (up, stow) = bench();
        assert_eq!(
            plain.to_string(),
            format!(
                "up {:.3} s (bench), stow {:.3} s (bench), antennas on the head group's clock",
                up.as_secs_f64(),
                stow.as_secs_f64()
            )
        );
        for absent in [
            "antenna_right_ms",
            "antenna_right_from",
            "antenna_left_ms",
            "antenna_left_from",
        ] {
            assert_eq!(plain.json()[absent], serde_json::Value::Null, "{absent}");
        }

        // One side stated and the other not is the line saying both, because a
        // pair reported as one number is the mirrored sweep this daemon is
        // meant to make visible.
        let half = laid(
            None,
            NEITHER,
            Overrides {
                antenna_sides: [Some(Duration::from_millis(700)), None],
                ..Overrides::default()
            },
        );
        assert_eq!(
            half.to_string(),
            format!(
                "up {:.3} s (bench), stow {:.3} s (bench), \
                 antennas right 0.700 s (daemon), left on the head group's clock",
                up.as_secs_f64(),
                stow.as_secs_f64()
            )
        );
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

    /// A commissioning that refused never took the machine. Five things follow,
    /// and every one of them is safety posture: the fault is recorded so the bus
    /// thread alerts, a stop is requested so nothing waits on a motion loop that
    /// is not coming, the ending is noted so the bus thread closes down, the
    /// state surface says parked, and the run ends faulted. Nothing is released,
    /// because nothing was ever torqued.
    #[test]
    fn a_commissioning_that_refused_faults_without_touching_torque() {
        let shared = Shared::new(POD);
        let sink = Collect::default();
        let dir = temp_dir();
        let surface = Surface::at(state_in(&dir));

        let outcome = commission_failed(
            &shared,
            Refusal::new(ErrorClass::Refuse, "servo 21 answered nothing"),
            &sink,
            &surface,
        );

        assert_eq!(
            value_of(&state_in(&dir), "state").as_deref(),
            Some("parked")
        );
        assert_eq!(
            value_of(&state_in(&dir), "fault_stage").as_deref(),
            Some("commissioning")
        );

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
        let faulted = TickEvent::Faulted(Fault::PositionFeedbackLost { misses: 12 });
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

    /// A move whose clock the library had to right-size says so as a fact, not
    /// only as a line.
    ///
    /// The case is the startup fold out of a machine somebody left most of a
    /// turn round: the configured fold clock was never sized for that span, the
    /// library stretches it rather than stepping past the guard and dropping the
    /// head, and the pair of durations is the only sign that a configured value
    /// did not fit what it met. Nobody is at the console for a boot, so it has
    /// to be in the capture.
    #[test]
    fn a_stretched_clock_is_recorded_with_both_durations() {
        let shared = Arc::new(Shared::new(POD));
        let stretch = ClockStretch {
            requested: MoveDurations::uniform(Duration::from_millis(2_000)),
            effective: MoveDurations {
                head: Duration::from_millis(3_400),
                antennas: [Duration::from_millis(2_000); 2],
            },
            separation: None,
            separation_required: AntennaPhaseConfig::default().separation_rad,
            dephased: false,
        };
        let head = Fake::new(&shared, [Event::Stop(Stop::Operator)])
            // Left most of a turn round, so the boot folds it: the move whose
            // span no configured clock was sized for.
            .standing_elsewhere()
            .reporting_moving([vec![TickEvent::Stretched(stretch)]]);

        let (outcome, _, sink) = driven(&shared, head);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        let stretches = sink.all_fields("motion_clock_stretched");
        assert_eq!(
            stretches.len(),
            1,
            "the fold's stretch is stated once: {stretches:?}"
        );
        assert_eq!(stretches[0]["requested_head_s"], json!(2.0));
        assert_eq!(stretches[0]["effective_head_s"], json!(3.4));
        assert_eq!(stretches[0]["requested_antennas_s"], json!([2.0, 2.0]));
        assert_eq!(
            stretches[0]["effective_antennas_s"],
            json!([2.0, 2.0]),
            "the group that fitted is reported as it was asked for: {stretches:?}"
        );
        assert_eq!(stretches[0]["dephased"], json!(false));
        assert_eq!(stretches[0]["separation_rad"], serde_json::Value::Null);
    }

    /// A clock lengthened to part the antennas at their crossing is the same
    /// event and is told apart by its own field.
    ///
    /// The two are different facts about a configuration: one says a duration
    /// was never sized for the span it met, the other says the pair as
    /// configured would have swept mirror-symmetrically through the band where
    /// the tips can touch — the collision that latched two antenna servos on
    /// the bench. A capture that reported them alike would leave the second
    /// looking like an over-cautious floor.
    #[test]
    fn a_de_phased_pair_says_which_side_was_held_and_what_it_bought() {
        let shared = Arc::new(Shared::new(POD));
        let asked = Duration::from_millis(800);
        let stretch = ClockStretch {
            requested: MoveDurations::uniform(asked),
            effective: MoveDurations {
                head: asked,
                antennas: [Duration::from_millis(970), asked],
            },
            separation: Some(PhaseSeparation {
                offset: 0.63,
                later: JointId::AntennaRight,
                at: Duration::from_millis(410),
                leader_rate: 4.5,
            }),
            separation_required: 0.6,
            dephased: true,
        };
        let head = Fake::new(&shared, [Event::Raise, Event::Stop(Stop::Operator)])
            .reporting_moving([vec![TickEvent::Stretched(stretch)]]);

        let (outcome, _, sink) = driven(&shared, head);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        let stretches = sink.all_fields("motion_clock_stretched");
        assert_eq!(stretches.len(), 1, "{stretches:?}");
        assert_eq!(stretches[0]["dephased"], json!(true));
        assert_eq!(stretches[0]["separation_rad"], json!(0.63));
        assert_eq!(
            stretches[0]["separation_required_rad"],
            json!(0.6),
            "the measurement is read against the bar it was judged by: {stretches:?}"
        );
        assert_eq!(
            stretches[0]["effective_antennas_s"],
            json!([0.97, 0.8]),
            "the side that was held is readable off the pair: {stretches:?}"
        );
        assert_eq!(
            stretches[0]["effective_head_s"],
            json!(0.8),
            "de-phasing the pair never touches the head's clock: {stretches:?}"
        );
    }

    /// A pair the resolver could not part is recorded too, on clocks it left
    /// exactly as they were asked for.
    ///
    /// The third shape of this event and the only one that is not about a
    /// duration changing: the move swept both tips through the crossing under
    /// the bar, and no delay would have parted them — a leader already stopped,
    /// a crossing at the very start of the path, a side already at its cap. The
    /// clocks are the answer to nothing, so the row is the whole record that it
    /// happened, and on a fielded pod the capture is the only place it lands.
    /// Suppressing the event when the clocks come back unchanged would delete
    /// every converging sweep from that record.
    #[test]
    fn a_pair_nothing_could_part_is_recorded_on_the_clocks_it_ran() {
        let shared = Arc::new(Shared::new(POD));
        let asked = Duration::from_millis(800);
        let stretch = ClockStretch {
            requested: MoveDurations::uniform(asked),
            effective: MoveDurations::uniform(asked),
            separation: Some(PhaseSeparation {
                offset: 0.09,
                later: JointId::AntennaLeft,
                at: Duration::from_millis(120),
                leader_rate: 0.0,
            }),
            separation_required: 0.6,
            dephased: false,
        };
        let head = Fake::new(&shared, [Event::Raise, Event::Stop(Stop::Operator)])
            .reporting_moving([vec![TickEvent::Stretched(stretch)]]);

        let (outcome, _, sink) = driven(&shared, head);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        let stretches = sink.all_fields("motion_clock_stretched");
        assert_eq!(
            stretches.len(),
            1,
            "a sweep that ran under the bar left no record of it: {stretches:?}"
        );
        assert_eq!(stretches[0]["separation_rad"], json!(0.09));
        assert_eq!(stretches[0]["separation_required_rad"], json!(0.6));
        assert_eq!(
            stretches[0]["dephased"],
            json!(false),
            "nothing was de-phased, and the row must not read as though it was: {stretches:?}"
        );
        assert_eq!(
            stretches[0]["effective_antennas_s"], stretches[0]["requested_antennas_s"],
            "the pair ran on the clocks it was asked for: {stretches:?}"
        );
    }

    /// A move the library did not have to touch says nothing about its clock:
    /// the event is the exception, and a line on every ordinary move would make
    /// the exception unreadable.
    #[test]
    fn an_ordinary_move_records_no_stretch() {
        let shared = Arc::new(Shared::new(POD));
        let head = Fake::new(&shared, [Event::Raise, Event::Stop(Stop::Operator)]);

        let (outcome, _, sink) = driven(&shared, head);

        assert_eq!(outcome, Outcome::Released(Stop::Operator));
        assert!(
            sink.all_fields("motion_clock_stretched").is_empty(),
            "nothing was stretched"
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
