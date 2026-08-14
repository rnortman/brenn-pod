//! Where the two threads meet: the schedule, the stop signal, and the fault.
//!
//! The bus thread is async and owns the attachment; the motion thread blocks and
//! owns the port. Neither can call into the other, and neither shares a data
//! structure with the other beyond what is here. Nine cells, and each one goes
//! in a single direction:
//!
//! - **The schedule** — written by the bus thread as scripts arrive, read by the
//!   motion thread at every dwell boundary. It is the only thing the bus can
//!   make the machine do, and all it can say is which posture the running
//!   script asks for at this instant, or that the script has lapsed.
//! - **The stop signal** — set on a signal or when the daemon loses its source
//!   of scripts, read by the motion thread at the top of every dwell and every
//!   resting sweep. It is what turns a signal into a fold, a measurement and a
//!   release, all of which happen on the thread that owns the port. It carries
//!   *why*, because the two reasons exit with different statuses.
//! - **The engage refusals** — written by the motion thread when a torque-on
//!   gate refuses, drained by the bus thread to alert on. Deliberately apart
//!   from the fault: nothing was written, the machine is limp where it was, and
//!   the daemon goes on resting.
//! - **The watch alarm** — written by the motion thread when the pre-torque
//!   sweeps stop answering and again when they come back, drained by the bus
//!   thread to alert on. Also not a fault: a machine nobody can read while it
//!   lies limp is already at the minimum risk condition, so the daemon keeps
//!   sweeping and sends the news instead of parking. What it costs meanwhile is
//!   presence — the head cannot engage from a posture nobody has measured.
//! - **The stow misses** — written by the motion thread when an orderly release
//!   measured the machine somewhere other than its fold, drained the same way.
//!   Also not a fault: torque came off, which is the whole doctrine, and the
//!   daemon carries on. What it is, is the one thing an operator has to know
//!   before putting a hand near a head that has been left alone for hours.
//! - **The antenna pair's standing** — written by the motion thread every time
//!   it judges which joints the session has out of service, read by the bus
//!   thread to answer a delivered script with and to alert on the moment the
//!   pair goes out. Not a fault either: the head is unaffected, the session
//!   carries on, and the next engage retries the pair.
//! - **The incidents** — written by the motion thread when an ending wound the
//!   head down and handed the loop back to rest, drained by the bus thread to
//!   alert on. Nothing is latched and the next script is taken, which is
//!   exactly why it needs a cell: the daemon looks identical to one that never
//!   had the head grabbed at all.
//! - **The fault** — written once by the motion thread when the machine stops
//!   taking commands, read by the bus thread so it can alert. Write-once because
//!   a fault is not a state that improves: the first thing that went wrong is
//!   the thing worth reporting, and everything after it is a consequence.
//! - **The ending** — written once by the motion thread when it has finished
//!   with the machine, read by the bus thread as its own leave to end the
//!   attachment. A fold, a settle, a nine-position sweep and a servo-by-servo
//!   release take seconds, and any of them can fault; an attachment closed on
//!   the stop
//!   signal instead would be gone before the alert that fault owes could travel
//!   over it.
//!
//! The fault is also what stops the schedule mattering. Once it is set the bus
//! thread stops accepting scripts, because a machine that has faulted takes no
//! commands at all — not a fold, not a re-engage, nothing until an operator
//! acts. Torque is already off by then: reaching the minimum risk condition is
//! what the motion thread does before it parks.

use std::fmt;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Instant;

use motion_proto::{Acceptance, Desired, MotionScript, Schedule};
use reachy_clips::Motion;
use reachy_motion::{Entry, Fault, Story, last_fault};

use crate::library::Motions;

/// Why the motion thread is being asked to stop.
///
/// Both endings take the same maneuver — fold the head, then take torque off —
/// because the minimum risk condition does not depend on who asked. What the
/// reason decides is the exit status: a signal is a clean stop, and an
/// attachment that will never carry another script is a configuration problem
/// that a restart would only repeat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    /// A signal — an operator's, or a service manager's; the daemon does not
    /// distinguish them and has no reason to.
    Operator,
    /// The daemon has no source of scripts left: the attachment ended in an
    /// outcome no reconnection follows.
    Detached,
}

impl fmt::Display for Stop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Operator => "a stop signal",
            Self::Detached => "the loss of the script source",
        })
    }
}

/// Where the daemon was when it stopped commanding.
///
/// Carried on the alert because the same refusal means different things at
/// different points: during commissioning the machine was never taken, during
/// the resting watch it was lying limp, and during the motion loop it was up or
/// on its way somewhere. Every one of them ends the same way — torque off — so
/// what this says is where to go looking, not what state the machine is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultStage {
    /// The once-per-process ceremony — the machine was never taken.
    Commission,
    /// The startup fold, from the moment torque goes on: a crash or a hand left
    /// the head somewhere else and it is being put back. The look that decides
    /// whether a fold is needed happens before torque and never faults — it
    /// retries until the machine answers.
    Startup,
    /// Taking hold — pinning the joints and enabling torque.
    Engage,
    /// The steady state: dwelling, reading the schedule, moving between
    /// postures.
    Motion,
    /// The release that puts an engaged machine back at rest.
    Release,
    /// The fold and release a stop signal asks for.
    Shutdown,
}

impl fmt::Display for FaultStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Commission => "commissioning",
            Self::Startup => "startup normalisation",
            Self::Engage => "taking hold",
            Self::Motion => "the motion loop",
            Self::Release => "the release back to rest",
            Self::Shutdown => "shutdown",
        })
    }
}

/// What the motion thread stopped on: where it was, what condition the machine
/// is in, how the libraries worded it, and the whole story behind it.
///
/// The detail stays text — the site that has the error is the one that can word
/// it, and the consumers are an alert and a log line. The slug and the record
/// are not: an alert rule keys on one word for a condition, and a story assembled
/// out of prose is one nobody can query. So both arrive typed and are rendered at
/// the sink, never parsed back out of a sentence.
#[derive(Debug, Clone, PartialEq)]
pub struct FaultReport {
    /// Where the daemon was.
    pub stage: FaultStage,
    /// The condition the machine was left in, as the doctrine names it, when the
    /// ending named one. A refusal names none: nothing about the platform was
    /// found wanting.
    pub slug: Option<&'static str>,
    /// What the motion libraries refused, as they rendered it.
    pub detail: String,
    /// Everything the session raised and everything done about it, in order.
    pub record: Vec<Entry>,
}

impl FaultReport {
    /// A report of an ending that left no record — nothing was ever engaged, so
    /// there was no session to keep one.
    pub fn new(stage: FaultStage, detail: impl Into<String>) -> Self {
        Self {
            stage,
            slug: None,
            detail: detail.into(),
            record: Vec::new(),
        }
    }

    /// A report of an ending a session recorded, named by the last condition in
    /// that record.
    ///
    /// The last and not the first: a wind-down that began over a grabbed head and
    /// latched because a servo dropped out on the way down is an incident about
    /// the servo, which is the word an operator is looking for and the one an
    /// alert rule keys on.
    pub fn recorded(stage: FaultStage, detail: impl Into<String>, record: Vec<Entry>) -> Self {
        Self {
            stage,
            slug: condition(&record),
            detail: detail.into(),
            record,
        }
    }

    /// The session's record as one line, or `None` when there is nothing in it.
    #[must_use]
    pub fn story(&self) -> Option<String> {
        story(&self.record)
    }
}

/// The condition a record ends on, if it names one.
///
/// The motion library's own reverse scan, not a second one: which entry names an
/// incident is a fact about the record's shape, and this daemon reading it
/// differently from the session that wrote it is how two surfaces of one machine
/// come to call one event by two names.
#[must_use]
pub fn condition(record: &[Entry]) -> Option<&'static str> {
    last_fault(record).as_ref().map(Fault::slug)
}

/// A record as the one line an operator quotes, or `None` when nothing went
/// wrong at all.
///
/// The words are made of the entries once, at this end, for whoever is going to
/// read words — and by the library's own rendering, because the line a session
/// prints as it ends and the report this daemon attaches afterwards are greppable
/// as one format or as neither.
#[must_use]
pub fn story(record: &[Entry]) -> Option<String> {
    (!record.is_empty()).then(|| Story(record).to_string())
}

impl fmt::Display for FaultReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} stopped at {}", self.stage, self.detail)
    }
}

/// Whether the antenna pair is being commanded.
///
/// One word for one predicate. The motion thread judges it from the joints the
/// live session has out of service, and every surface that reports it — the
/// state file, the alert, the answer a delivered script gets — reads that
/// judgement instead of making one of its own. Two surfaces judging separately
/// would sooner or later disagree about a pair that is either being commanded or
/// is not, and the one asking us to move the antennas would be the last to know.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Antennas {
    /// Both antennas are in service.
    #[default]
    Ok,
    /// The pair has been torqued off and left out of the moves for the rest of
    /// this session. The head is unaffected; the next engage retries the pair.
    Degraded,
}

impl Antennas {
    /// The standing as the state file and the capture spell it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Degraded => "degraded",
        }
    }

    /// Whether the pair is out of service.
    #[must_use]
    pub fn degraded(self) -> bool {
        matches!(self, Self::Degraded)
    }
}

/// What a delivery did.
///
/// The schedule's verdict and nothing else. Whatever else a sender is owed about
/// the machine — that the antennas are out of service this session, that a leg is
/// masked — is read from the cell that holds it, where the answer is built:
/// welding a second cell onto this one buys no snapshot (two locks taken in
/// sequence are two reads whichever value carries them) and makes the schedule's
/// answer impossible to construct without a machine standing behind it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivered {
    /// Offered to the schedule, with what the schedule made of it.
    Scheduled(Acceptance),
    /// Dropped: the machine has faulted and takes no further scripts.
    Faulted,
}

/// A run of repeated refusals collapsed into one thing to report: the most
/// recent, and how many there have been since the last drain.
///
/// One shape for every condition that recurs faster than anybody can be told
/// about it — a rail that has been sagging for an hour, a scripter emitting a
/// body this daemon cannot execute on its refresh cadence. The latest rather
/// than the first, so the report describes the machine now; the count is what
/// says how long it has been like this. Whether a drain may happen more than
/// once is the draining caller's decision and not this type's: the two users
/// differ on exactly that, and on nothing else.
#[derive(Debug, Default)]
pub struct Collapsed {
    latest: Option<String>,
    since_drained: u64,
}

impl Collapsed {
    /// Note one more occurrence, in the caller's rendering.
    pub fn note(&mut self, detail: impl Into<String>) {
        self.latest = Some(detail.into());
        self.since_drained += 1;
    }

    /// Take what is owed a report — the most recent occurrence, and how many
    /// there have been since this last answered — or `None` when there is
    /// nothing owed.
    pub fn take(&mut self) -> Option<(String, u64)> {
        let detail = self.latest.take()?;
        Some((detail, std::mem::take(&mut self.since_drained)))
    }
}

/// A run of pre-torque sweeps that stopped answering, as the alert reads it.
///
/// Deliberately not a [`Collapsed`]: how many sweeps failed is not a number
/// anybody can act on — a dead bus fails ten a second for as long as it is dead
/// — and it is not what makes this worth an alert either. What is worth
/// reporting is that reads went away, whether they have come back since, and
/// what the machine said the last time it refused. So the motion thread writes
/// the edges of a failure run and this counts those.
#[derive(Debug, Default)]
pub struct WatchNotice {
    /// What the first sweep of the most recent failure run reported.
    latest: Option<String>,
    /// Failure runs begun since the last drain.
    runs: u64,
    /// Runs ended by reads coming back, since the last drain.
    restores: u64,
    /// Whether sweeps are failing as of the last edge written.
    failing: bool,
}

impl WatchNotice {
    /// A run of failing sweeps has begun, with what its first failure said.
    fn lost(&mut self, detail: impl Into<String>) {
        self.latest = Some(detail.into());
        self.runs += 1;
        self.failing = true;
    }

    /// Reads have come back.
    fn restored(&mut self) {
        self.restores += 1;
        self.failing = false;
    }

    /// Take what is owed an alert, or `None` when no failure run has begun
    /// since this last answered.
    ///
    /// A restore on its own is not alerted on: reads coming back is the thing
    /// nobody has to be told about, and the count of them rides on the next
    /// alarm as the evidence that the bus is flapping rather than dead.
    fn take(&mut self) -> Option<WatchAlarm> {
        let detail = (self.runs > 0).then(|| self.latest.take())??;
        Some(WatchAlarm {
            detail,
            runs: std::mem::take(&mut self.runs),
            restores: std::mem::take(&mut self.restores),
            failing: self.failing,
        })
    }
}

/// The antenna pair's standing, and what took it out of service if something
/// has.
///
/// The standing is a level: a delivered script is answered from whatever it says
/// at the moment it arrives. The alert is an edge — a pair left out of service
/// across three wakes is one standing condition, and an alert per wake would
/// bury the one that said something new. So every judgement writes the level and
/// only the judgement that changed it leaves an alert owed.
#[derive(Debug, Default)]
struct AntennaNotice {
    /// What the motion thread last judged.
    standing: Antennas,
    /// What the pair went out of service over, until an alert has taken it.
    degraded_by: Option<String>,
}

impl AntennaNotice {
    /// Judge the pair, and answer whether this judgement took it out of service.
    fn judged(&mut self, standing: Antennas, detail: &str) -> bool {
        let flipped = standing.degraded() && !self.standing.degraded();
        self.standing = standing;
        if flipped {
            self.degraded_by = Some(detail.to_owned());
        }
        flipped
    }

    /// Take what the flip owes an alert, or `None` when the standing has not
    /// changed into degraded since this last answered.
    fn take(&mut self) -> Option<String> {
        self.degraded_by.take()
    }
}

/// What a drained [`WatchNotice`] owes an alert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchAlarm {
    /// What the machine said the last time a sweep started failing.
    pub detail: String,
    /// Failure runs begun since the last alarm was taken.
    pub runs: u64,
    /// Runs that ended in reads coming back, over the same span.
    pub restores: u64,
    /// Whether sweeps are still failing now.
    pub failing: bool,
}

/// One overlay the running script has open, as the motion thread reads it.
///
/// Owned rather than borrowed from the schedule: what answers this holds a
/// lock, and a caller that kept a reference would hold it for the whole of a
/// control period. The motion is the library's own handle rather than the
/// name off the wire — this is built afresh every control period, and an
/// `Arc` costs a counter where a name costs an allocation and a second lookup
/// in whoever reads it.
#[derive(Debug, Clone)]
pub struct Playing {
    /// Which wire step started it, which is also its composition order.
    pub index: usize,
    /// The motion it plays.
    pub motion: Arc<Motion>,
    /// The invocation speed.
    pub speed: f64,
    /// How long the timeline says it has been running.
    pub elapsed_ms: u64,
}

impl Playing {
    /// The name the wire addressed this motion by.
    #[must_use]
    pub fn name(&self) -> &str {
        self.motion.name()
    }
}

/// Two overlays are the same overlay when they are the same motion at the same
/// point of the same step. By name rather than by handle: a library reloaded
/// from the same directory holds different `Arc`s for the same vocabulary, and
/// what a reader means to ask is whether the motion is the same one.
impl PartialEq for Playing {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
            && self.speed == other.speed
            && self.elapsed_ms == other.elapsed_ms
            && self.motion.name() == other.motion.name()
    }
}

/// What the running script is playing at an instant, and which script that is.
///
/// The two together because they are read together: a sequence number that
/// changed means a different timeline, and every player belonging to the old
/// one is dropped whatever the names say.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Overlaid {
    /// The script the overlays belong to, if one is running.
    pub seq: Option<u64>,
    /// The overlays open now, in composition order.
    pub plays: Vec<Playing>,
}

/// The nine cells and the motion vocabulary, held behind one handle both
/// threads clone.
#[derive(Debug)]
pub struct Shared {
    /// The machine a script has to be addressed to for this daemon to have any
    /// business with it.
    ///
    /// Beside the schedule's own copy rather than read through it, because the
    /// question is asked before the schedule is: everything the bus thread does
    /// with a script — screening it against this machine's library and its
    /// machine-derived speed ceilings, reporting what it made of it — is only
    /// this daemon's to do for a script addressed to this daemon.
    pod: String,
    schedule: Mutex<Schedule>,
    /// The motions a `play` step can name.
    ///
    /// Not a cell: nothing mutates it after startup, and both threads read it
    /// for different halves of the same question — the bus thread screens an
    /// arriving script against it, the motion thread plays out of it. Held here
    /// so the schedule and the library are joined under one lock in
    /// [`Self::playing`] rather than by two calls a replacement can land
    /// between.
    motions: Motions,
    /// Torque-on gate refusals the bus thread has not alerted on yet.
    ///
    /// Not the fault cell: a refused engage is an expected error — nothing was
    /// written and the machine is exactly as it was — so it must not park the
    /// daemon or claim the one write-once fault slot. It is still worth waking
    /// somebody about, because a machine that will not take torque will not
    /// answer the next wake word either. Drained as often as there is something
    /// in it: a rail that comes back up and sags again is news each time.
    engage_refusals: Mutex<Collapsed>,
    /// Orderly releases that measured the machine away from its fold, which the
    /// bus thread has not alerted on yet.
    ///
    /// A verify miss is a report and never a refusal — torque comes off
    /// regardless — but the head is then limp somewhere it was not supposed to
    /// be, on a machine nobody is watching. Collapsed for the same reason as the
    /// engage refusals: a machine that misses its fold once will miss it every
    /// turn until somebody looks.
    stow_misses: Mutex<Collapsed>,
    /// Pre-torque sweeps that stopped answering, on the edges the bus thread
    /// alerts on.
    ///
    /// The counterpart of the engage refusals for the phase before an engage is
    /// even asked for: the machine is limp, so a sweep nobody can complete is a
    /// picture lost and not control lost. The daemon keeps sweeping and
    /// recovers by itself; this is how somebody hears about the wire meanwhile.
    watch: Mutex<WatchNotice>,
    /// The antenna pair's standing, as the motion thread last judged the joints
    /// the session has out of service.
    ///
    /// Not the fault cell and not a refusal: the head keeps its presence and the
    /// script runs, so parking over it would cost a conversation to save two
    /// antennas that are already limp. The next engage retries the pair, which is
    /// what makes this a level worth reading rather than a verdict worth
    /// latching.
    antennas: Mutex<AntennaNotice>,
    /// Endings that wound the head down and handed the loop back to rest, which
    /// the bus thread has not alerted on yet.
    ///
    /// Collapsed for the same reason the stow misses are: a hand on the head at
    /// every wake is the same news each time and the latest is the one that
    /// describes the machine. Not the fault cell, deliberately — nothing latched
    /// and the next script is taken — which is exactly why somebody has to be
    /// told: a daemon that wound down over an obstruction and one that had a
    /// quiet afternoon look identical from outside.
    incidents: Mutex<Collapsed>,
    stop: OnceLock<Stop>,
    fault: OnceLock<FaultReport>,
    ended: OnceLock<()>,
}

impl Shared {
    /// A daemon that has heard nothing, is not shutting down, and has not
    /// faulted.
    ///
    /// The schedule starts empty, so a daemon that comes up in the middle of a
    /// conversation asks for no posture change until somebody scripts one.
    pub fn new(pod: impl Into<String>) -> Self {
        Self::with_motions(pod, Motions::none())
    }

    /// The same, holding `motions` as the vocabulary a `play` step names.
    ///
    /// The daemon's own constructor. [`Self::new`] is the posture-only machine,
    /// which is a real configuration rather than a default worth hiding.
    pub fn with_motions(pod: impl Into<String>, motions: Motions) -> Self {
        let pod = pod.into();
        Self {
            motions,
            schedule: Mutex::new(Schedule::new(pod.clone())),
            pod,
            engage_refusals: Mutex::new(Collapsed::default()),
            stow_misses: Mutex::new(Collapsed::default()),
            watch: Mutex::new(WatchNotice::default()),
            antennas: Mutex::new(AntennaNotice::default()),
            incidents: Mutex::new(Collapsed::default()),
            stop: OnceLock::new(),
            fault: OnceLock::new(),
            ended: OnceLock::new(),
        }
    }

    /// Note that a torque-on gate refused an engage.
    ///
    /// Written by the motion thread, which carries on resting: this is a thing
    /// to tell somebody about, not a thing that stops the daemon.
    pub fn refuse_engage(&self, detail: impl Into<String>) {
        self.engage_refusals().note(detail);
    }

    /// Take the engage refusals owed an alert: the most recent one, and how many
    /// there have been since the last time this answered.
    pub fn take_engage_refusal(&self) -> Option<(String, u64)> {
        self.engage_refusals().take()
    }

    /// Note that an orderly release left the machine away from its fold.
    ///
    /// Written by the motion thread after torque is already off: this describes
    /// where the head was found, not something that could have been prevented.
    pub fn note_stow_miss(&self, detail: impl Into<String>) {
        self.stow_misses().note(detail);
    }

    /// Take the stow misses owed an alert, the most recent and the count.
    pub fn take_stow_miss(&self) -> Option<(String, u64)> {
        self.stow_misses().take()
    }

    /// Note that the pre-torque watch has stopped answering, with what the
    /// first sweep of the run reported.
    ///
    /// Written on the edge, by the motion thread, which goes on sweeping: this
    /// is degraded presence, not a fault, and the daemon needs nobody's
    /// permission to recover from it.
    pub fn note_watch_lost(&self, detail: impl Into<String>) {
        self.watch().lost(detail);
    }

    /// Note that a sweep has answered again after a run of failures.
    pub fn note_watch_restored(&self) {
        self.watch().restored();
    }

    /// Take the watch alarm owed an alert, or `None` when no run of failures
    /// has begun since this last answered.
    pub fn take_watch_alarm(&self) -> Option<WatchAlarm> {
        self.watch().take()
    }

    /// State the antenna pair's standing, with what took it out of service if
    /// something did, and answer whether this is the judgement that took it out.
    ///
    /// Written in both directions by every engage, so a pair that came back is
    /// reported as being back: the answer is what the joints out of service say
    /// now, not the worst they have ever said.
    pub fn note_antennas(&self, standing: Antennas, detail: &str) -> bool {
        self.antenna_notice().judged(standing, detail)
    }

    /// The antenna pair's standing, for whoever is being answered about it.
    pub fn antennas(&self) -> Antennas {
        self.antenna_notice().standing
    }

    /// Take what the pair going out of service owes an alert, or `None` when
    /// nothing has taken it out since this last answered.
    pub fn take_antennas_alarm(&self) -> Option<String> {
        self.antenna_notice().take()
    }

    /// Note an ending that wound the head down and left the daemon resting.
    ///
    /// Written by the motion thread after torque is off: this describes a
    /// machine that has recovered by itself, and the report is the only sign it
    /// ever happened.
    pub fn note_incident(&self, detail: impl Into<String>) {
        self.incidents().note(detail);
    }

    /// Take the wind-downs owed an alert, the most recent and the count.
    pub fn take_incident(&self) -> Option<(String, u64)> {
        self.incidents().take()
    }

    /// Offer one script to the schedule, arriving at `now`.
    ///
    /// Refused once the machine has faulted: the motion thread has stopped
    /// commanding, and a timeline that kept running underneath it would only
    /// mean the daemon's log disagreed with the machine.
    pub fn accept(&self, script: &MotionScript, now: Instant) -> Delivered {
        if self.faulted() {
            return Delivered::Faulted;
        }
        Delivered::Scheduled(self.schedule().accept(script, now))
    }

    /// What the running script asks of the machine as of `now`.
    ///
    /// This is the timeline's answer and nothing else. It says nothing about
    /// whether the machine may be commanded — a faulted daemon must not act on
    /// it, and the motion thread checks [`Self::fault`] first.
    pub fn desired(&self, now: Instant) -> Desired {
        self.schedule().desired(now)
    }

    /// The motions this daemon holds.
    pub fn motions(&self) -> &Motions {
        &self.motions
    }

    /// Whether `script` is addressed to some other machine.
    ///
    /// The same question [`motion_proto::Schedule::accept`] answers with
    /// [`Acceptance::Foreign`], asked before the schedule is reached: a channel
    /// may carry more than one machine's traffic, and everything this daemon
    /// does with a script it is not authoritative for — screening it against a
    /// library another machine's script was never written against, reporting
    /// what this machine made of it — is noise about somebody else's timeline.
    pub fn foreign(&self, script: &MotionScript) -> bool {
        script.pod() != self.pod
    }

    /// What the running script has playing as of `now`, against this daemon's
    /// own library.
    ///
    /// The overlay half of [`Self::desired`], and read the same way: the
    /// timeline's answer and nothing else. An expired script plays nothing, for
    /// the same reason it asks for no posture — a lapse is the end of
    /// instruction.
    pub fn playing(&self, now: Instant) -> Overlaid {
        let schedule = self.schedule();
        self.overlaid(&schedule, now)
    }

    /// The base command and the open windows, from one read of the schedule.
    ///
    /// What a composed control period asks for, and it asks for both at once on
    /// purpose: a replacement landing between two reads would have the base
    /// answered from one script and the overlays from the next, and the run
    /// would carry on toward a posture nothing was asking for any more.
    pub fn composing(&self, now: Instant) -> (Desired, Overlaid) {
        let schedule = self.schedule();
        let desired = schedule.desired(now);
        let overlaid = self.overlaid(&schedule, now);
        (desired, overlaid)
    }

    /// The open windows of the script this schedule is running, under a lock
    /// the caller already holds.
    fn overlaid(&self, schedule: &Schedule, now: Instant) -> Overlaid {
        let Some((script, elapsed_ms)) = schedule.running_at(now) else {
            return Overlaid::default();
        };
        let plays = script
            .overlays_at(elapsed_ms, |play| self.motions.window(play))
            .into_iter()
            .filter_map(|overlay| {
                // Only a motion this daemon holds: acceptance resolves every
                // name before a script runs at all, and the window arithmetic
                // above has already passed over anything else.
                let motion = self.motions.motion(&overlay.play.name)?;
                Some(Playing {
                    index: overlay.index,
                    motion: Arc::clone(motion),
                    speed: overlay.play.speed,
                    elapsed_ms: overlay.elapsed_ms,
                })
            })
            .collect();
        Overlaid {
            seq: Some(script.seq()),
            plays,
        }
    }

    /// The next instant at which [`Self::desired`] can change by itself.
    ///
    /// What the motion thread shortens a dwell against, so a step at an
    /// arbitrary offset is executed on the script's own timeline rather than at
    /// the next multiple of the dwell.
    pub fn next_boundary(&self, now: Instant) -> Option<Instant> {
        self.schedule().next_boundary(now)
    }

    /// The number of the script in force, if one is.
    ///
    /// The motion thread's way of telling one script's lapse from the next's:
    /// an expiry is reported once per script, and the timeline itself has no
    /// other identity a reader could key on.
    pub fn accepted_seq(&self) -> Option<u64> {
        self.schedule().accepted_seq()
    }

    /// Ask the motion thread to stop, and say why.
    ///
    /// Returns whether `stop` is the reason that was recorded. The first one
    /// wins: a detachment noticed while an operator's signal is already being
    /// acted on must not turn a release into a torque-held exit, and an
    /// operator's second signal must not restart a shutdown that is under way.
    pub fn request_stop(&self, stop: Stop) -> bool {
        self.stop.set(stop).is_ok()
    }

    /// Why the daemon is stopping, if it is.
    pub fn stopping(&self) -> Option<Stop> {
        self.stop.get().copied()
    }

    /// Note that the motion thread has finished with the machine.
    ///
    /// Set on every path that leaves the machine unattended — the orderly
    /// release, the park a fault leaves at the minimum risk condition, a fault
    /// taken on the way out, and a commissioning that never took the machine at
    /// all. Until it is set the bus thread keeps the attachment up, because a
    /// fault raised during the ending still owes an alert.
    pub fn end_motion(&self) {
        let _ = self.ended.set(());
    }

    /// Whether the motion thread has finished with the machine.
    pub fn motion_ended(&self) -> bool {
        self.ended.get().is_some()
    }

    /// Record that the machine stopped taking commands.
    ///
    /// Returns whether `report` is the one that was kept. A later fault is
    /// dropped rather than overwriting: what stopped the machine first is the
    /// diagnosis, and anything raised afterwards is a symptom of a daemon that
    /// is already parked.
    pub fn set_fault(&self, report: FaultReport) -> bool {
        self.fault.set(report).is_ok()
    }

    /// What stopped the machine, if anything has.
    pub fn fault(&self) -> Option<&FaultReport> {
        self.fault.get()
    }

    /// Whether the machine has faulted.
    pub fn faulted(&self) -> bool {
        self.fault.get().is_some()
    }

    /// The schedule, with a poisoned lock recovered rather than propagated.
    ///
    /// The schedule is a value, not an invariant spread over several fields:
    /// every operation on it either completes or leaves it exactly as it was, so
    /// a thread that panicked while holding this lock cannot have left half a
    /// timeline behind. Propagating the poison instead would stop the motion
    /// thread polling, and the poll is what keeps fault detection alive on a
    /// machine nobody is watching.
    fn schedule(&self) -> std::sync::MutexGuard<'_, Schedule> {
        self.schedule.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The engage refusals, with a poisoned lock recovered the same way and for
    /// the same reason: two fields written together, never half written.
    fn engage_refusals(&self) -> std::sync::MutexGuard<'_, Collapsed> {
        self.engage_refusals
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The stow misses, recovered the same way and for the same reason.
    fn stow_misses(&self) -> std::sync::MutexGuard<'_, Collapsed> {
        self.stow_misses
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The watch notice, recovered the same way. The reason is stronger here
    /// than anywhere else: this lock is taken by the thread that sweeps a limp
    /// machine, and a poisoned lock that stopped it sweeping would turn a
    /// panicked bus thread into a head that never engages again.
    fn watch(&self) -> std::sync::MutexGuard<'_, WatchNotice> {
        self.watch.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The antenna notice, recovered the same way: a level and an edge written
    /// together, never half written.
    fn antenna_notice(&self) -> std::sync::MutexGuard<'_, AntennaNotice> {
        self.antennas.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The wind-down reports, recovered the same way and for the same reason as
    /// the other [`Collapsed`] cells.
    fn incidents(&self) -> std::sync::MutexGuard<'_, Collapsed> {
        self.incidents
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use motion_proto::{Posture, Step};
    use reachy_motion::{Fault, JointId, Maneuver, Outcome};

    use super::*;

    const POD: &str = "reachy00";

    fn shared() -> Shared {
        Shared::new(POD)
    }

    /// The nominal script: up now, stow when the speech is over.
    fn script(seq: u64) -> MotionScript {
        MotionScript::new(
            POD,
            seq,
            vec![Step::new(0, Posture::Up), Step::new(6_740, Posture::Stow)],
            30_000,
        )
        .expect("a lawful script")
    }

    fn ms(count: u64) -> Duration {
        Duration::from_millis(count)
    }

    /// What a delivery the schedule saw comes back as.
    fn taken(acceptance: Acceptance) -> Delivered {
        Delivered::Scheduled(acceptance)
    }

    /// What the timeline is playing is read the same way as what it asks of the
    /// posture: the script's own arithmetic, against this daemon's library, and
    /// nothing at all once the script has lapsed.
    #[test]
    fn what_is_playing_is_the_timeline_answering_until_it_lapses() {
        let sink = crate::report::Collect::default();
        let (_dir, motions) = crate::library::fixtures::loaded(
            &[(
                "wiggle.json",
                crate::library::fixtures::clip("test/wiggle", 10, 1.0),
            )],
            &sink,
        );
        let shared = Shared::with_motions(POD, motions);
        let script = MotionScript::new(
            POD,
            7,
            vec![
                Step::new(0, Posture::Up),
                Step::play(100, motion_proto::Play::new("test/wiggle")),
            ],
            30_000,
        )
        .expect("a lawful script");
        let now = Instant::now();
        shared.accept(&script, now);

        assert_eq!(
            shared.playing(now).plays,
            Vec::new(),
            "a window that has not opened is playing"
        );
        let open = shared.playing(now + ms(150));
        assert_eq!(open.seq, Some(7));
        assert_eq!(open.plays.len(), 1);
        assert_eq!(open.plays[0].name(), "test/wiggle");
        assert_eq!(open.plays[0].index, 1, "the step that opened it");
        assert_eq!(
            open.plays[0].elapsed_ms, 50,
            "a daemon reading late joins where the timeline says"
        );
        assert_eq!(
            shared.playing(now + ms(30_000)),
            Overlaid::default(),
            "a lapsed script kept playing"
        );

        let at = now + ms(150);
        let (desired, playing) = shared.composing(at);
        assert_eq!(desired, shared.desired(at));
        assert_eq!(playing, shared.playing(at));
    }

    /// A daemon holding no library plays nothing, whatever a script names.
    #[test]
    fn a_daemon_with_no_library_plays_nothing() {
        let shared = shared();
        let script = MotionScript::new(
            POD,
            1,
            vec![
                Step::new(0, Posture::Up),
                Step::play(100, motion_proto::Play::new("test/wiggle")),
            ],
            30_000,
        )
        .expect("a lawful script");
        let now = Instant::now();
        shared.accept(&script, now);

        assert!(shared.playing(now + ms(150)).plays.is_empty());
        assert!(shared.motions().is_empty());
    }

    /// A daemon that has heard nothing asks for no posture change. Resting is
    /// the default state of the machine, so silence is not an instruction and
    /// does not need to be one.
    #[test]
    fn a_fresh_daemon_asks_for_nothing() {
        let shared = shared();
        assert_eq!(shared.desired(Instant::now()), Desired::Unchanged);
        assert_eq!(shared.next_boundary(Instant::now()), None);
        assert_eq!(shared.accepted_seq(), None);
        assert_eq!(shared.stopping(), None);
        assert!(!shared.faulted());
        assert_eq!(shared.fault(), None);
        assert!(!shared.motion_ended());
    }

    /// One message carries the whole timeline: the head goes up as it lands and
    /// comes down when the audio it was scheduled against ends.
    #[test]
    fn one_script_carries_the_whole_timeline() {
        let shared = shared();
        let now = Instant::now();

        assert_eq!(shared.accept(&script(1), now), taken(Acceptance::Accepted));
        assert_eq!(shared.accepted_seq(), Some(1));
        assert_eq!(shared.desired(now), Desired::Posture(Posture::Up));
        assert_eq!(
            shared.desired(now + ms(6_740)),
            Desired::Posture(Posture::Stow)
        );
        assert_eq!(shared.desired(now + ms(30_000)), Desired::Expired);
        assert_eq!(shared.next_boundary(now), Some(now + ms(6_740)));
        assert_eq!(
            shared.next_boundary(now + ms(6_740)),
            Some(now + ms(30_000))
        );
    }

    /// A later script wholly replaces the one running, and a redelivery of an
    /// overtaken one changes nothing.
    #[test]
    fn the_latest_script_replaces_the_one_running_and_a_stale_one_is_dropped() {
        let shared = shared();
        let now = Instant::now();

        shared.accept(&script(7), now);
        let stow_now = MotionScript::new(POD, 9, vec![Step::new(0, Posture::Stow)], 30_000)
            .expect("a lawful script");
        assert_eq!(shared.accept(&stow_now, now), taken(Acceptance::Accepted));
        assert_eq!(shared.desired(now), Desired::Posture(Posture::Stow));

        assert_eq!(
            shared.accept(&script(7), now),
            taken(Acceptance::Stale {
                seq: 7,
                accepted: 9
            })
        );
        assert_eq!(shared.desired(now), Desired::Posture(Posture::Stow));
    }

    /// Another machine's script moves nothing here, and says so, so the daemon
    /// can report it rather than infer it from a timeline that did not change.
    #[test]
    fn another_pods_script_is_reported_and_dropped() {
        let shared = shared();
        let now = Instant::now();
        let elsewhere = MotionScript::new("reachy01", 1, vec![Step::new(0, Posture::Up)], 30_000)
            .expect("a lawful script");

        assert_eq!(shared.accept(&elsewhere, now), taken(Acceptance::Foreign));
        assert_eq!(shared.desired(now), Desired::Unchanged);
        assert_eq!(shared.accepted_seq(), None);
    }

    /// The first reason wins. The one that matters is a detachment landing on
    /// top of an operator's signal: it must not turn the release the operator
    /// asked for into a torque-held exit.
    #[test]
    fn the_first_stop_reason_is_the_one_that_is_acted_on() {
        let shared = shared();
        assert!(shared.request_stop(Stop::Operator));
        assert_eq!(shared.stopping(), Some(Stop::Operator));

        assert!(!shared.request_stop(Stop::Detached));
        assert!(!shared.request_stop(Stop::Operator));
        assert_eq!(shared.stopping(), Some(Stop::Operator));
    }

    /// The other order, which is the ordinary one: the bridge gives up, and an
    /// operator who then signals the parked daemon does not get a release out of
    /// it either.
    #[test]
    fn a_detachment_is_not_overtaken_by_a_later_signal() {
        let shared = shared();
        assert!(shared.request_stop(Stop::Detached));
        assert!(!shared.request_stop(Stop::Operator));
        assert_eq!(shared.stopping(), Some(Stop::Detached));
    }

    /// What stopped the machine first is the diagnosis; a refusal raised after
    /// the daemon has already parked is a symptom.
    #[test]
    fn the_first_fault_is_the_one_that_is_kept() {
        let shared = shared();
        let first = FaultReport::new(FaultStage::Motion, "the tick faulted: tracking lost");

        assert!(shared.set_fault(first.clone()));
        assert!(!shared.set_fault(FaultReport::new(FaultStage::Shutdown, "servo 4: timed out")));
        assert_eq!(shared.fault(), Some(&first));
        assert!(shared.faulted());
    }

    /// A faulted machine takes no further scripts. The timeline it was running
    /// is left exactly as it was — nothing about a fault means the head should
    /// move, in either direction.
    #[test]
    fn a_faulted_daemon_stops_accepting_scripts() {
        let shared = shared();
        let now = Instant::now();

        shared.accept(&script(1), now);
        shared.set_fault(FaultReport::new(FaultStage::Motion, "read loss"));

        let stow_now = MotionScript::new(POD, 2, vec![Step::new(0, Posture::Stow)], 30_000)
            .expect("a lawful script");
        assert_eq!(shared.accept(&stow_now, now), Delivered::Faulted);
        assert_eq!(shared.desired(now), Desired::Posture(Posture::Up));
    }

    /// The ending is separate from the stop that asked for it, and it is what
    /// the bus thread waits on: the stow, the verify and the release happen
    /// between the two, and a fault taken in there still owes an alert.
    #[test]
    fn the_ending_is_not_the_stop_that_asked_for_it() {
        let shared = shared();
        shared.request_stop(Stop::Operator);
        assert!(!shared.motion_ended());

        shared.end_motion();
        assert!(shared.motion_ended());
        shared.end_motion();
        assert!(shared.motion_ended());
    }

    #[test]
    fn a_fault_report_names_where_and_what() {
        let report = FaultReport::new(FaultStage::Commission, "servo 7 answered nothing");
        assert_eq!(
            report.to_string(),
            "commissioning stopped at servo 7 answered nothing"
        );
        assert_eq!(report.slug, None, "nothing about the platform was found");
        assert_eq!(report.story(), None);
    }

    /// A report is named by the last condition its record holds, not the first.
    ///
    /// A wind-down that began over a grabbed head and latched because a servo
    /// dropped out on the way down is an incident about the servo: naming it by
    /// the grab would send an operator looking for a hand that is long gone, and
    /// would key the alert on a condition the doctrine does not latch.
    #[test]
    fn a_report_is_named_by_the_last_condition_in_its_record() {
        let grabbed = Entry::Fault {
            fault: Fault::HeadObstructed {
                joint: JointId::Leg(1),
                error: 0.4,
            },
            at: Duration::from_millis(10),
        };
        let dropped_out = Entry::Fault {
            fault: Fault::HeadServoFault {
                joint: JointId::Leg(3),
                id: 13,
                bits: 0x20,
            },
            at: Duration::from_millis(300),
        };
        let stowed = Entry::Response {
            maneuver: Maneuver::MaskedSlowStow,
            outcome: Outcome::Completed,
            at: Duration::from_millis(900),
        };

        let report = FaultReport::recorded(
            FaultStage::Motion,
            "leg 3 (servo 13) reports hardware error 0x20",
            vec![grabbed, dropped_out, stowed],
        );

        assert_eq!(report.slug, Some("head_servo_fault"));
        let told = report.story().expect("three entries");
        assert_eq!(told.matches(" → ").count(), 2, "{told}");
        assert!(told.starts_with("head_obstructed: "), "{told}");
        assert!(told.ends_with("masked_slow_stow completed"), "{told}");
        assert_eq!(condition(&[stowed]), None, "a maneuver is not a condition");
        assert_eq!(story(&[]), None, "nothing went wrong, so there is no story");
    }

    /// A refused engage is told to the bus thread without touching the fault:
    /// the daemon goes on resting, and the alert says how many refusals stand
    /// behind the one being reported.
    #[test]
    fn engage_refusals_are_drained_with_their_count_and_are_not_faults() {
        let shared = shared();
        assert_eq!(shared.take_engage_refusal(), None);

        shared.refuse_engage("the supply is below the floor");
        shared.refuse_engage("the supply is still below the floor");
        assert!(!shared.faulted(), "a refused engage is not a fault");

        let (detail, count) = shared.take_engage_refusal().expect("two refusals stand");
        assert_eq!(detail, "the supply is still below the floor");
        assert_eq!(count, 2);
        assert_eq!(
            shared.take_engage_refusal(),
            None,
            "a drained refusal is not alerted on twice"
        );
    }

    /// The watch is reported on its edges, not per sweep: a run of failures is
    /// one alarm however many sweeps it spans, and it carries what the machine
    /// said when it started and whether reads are back.
    #[test]
    fn a_run_of_failing_sweeps_is_one_alarm_that_says_whether_reads_came_back() {
        let shared = shared();
        assert_eq!(shared.take_watch_alarm(), None);

        shared.note_watch_lost("servo 11: timed out");
        let alarm = shared.take_watch_alarm().expect("a failing watch owes one");
        assert_eq!(alarm.detail, "servo 11: timed out");
        assert_eq!((alarm.runs, alarm.restores), (1, 0));
        assert!(alarm.failing);
        assert!(!shared.faulted(), "a failing watch is not a fault");
        assert_eq!(
            shared.take_watch_alarm(),
            None,
            "an alarm already taken is alerted on twice"
        );

        shared.note_watch_restored();
        assert_eq!(
            shared.take_watch_alarm(),
            None,
            "reads coming back is not itself an alert"
        );
    }

    /// A bus that flaps: the runs and the recoveries between them are both
    /// counted, so one alarm can say the wire is unreliable rather than dead.
    #[test]
    fn the_alarm_counts_the_runs_and_the_recoveries_it_stands_for() {
        let shared = shared();

        shared.note_watch_lost("servo 11: timed out");
        shared.note_watch_restored();
        shared.note_watch_lost("servo 12: timed out");
        shared.note_watch_restored();

        let alarm = shared.take_watch_alarm().expect("two runs stand");
        assert_eq!(alarm.detail, "servo 12: timed out");
        assert_eq!((alarm.runs, alarm.restores), (2, 2));
        assert!(!alarm.failing, "the second run ended in reads coming back");
    }

    /// The pair going out of service is one alert, however many wakes it stands
    /// for, and the standing itself is readable the whole time.
    ///
    /// A latch that survives three wakes is one condition: alerting on each of
    /// them would bury the alert that said something. What every one of those
    /// wakes *does* owe is a true answer to the script it is executing, which is
    /// what the level is for.
    #[test]
    fn the_pair_going_out_of_service_is_one_alert_and_a_standing_answer() {
        let shared = shared();
        assert_eq!(shared.antennas(), Antennas::Ok);
        assert_eq!(shared.take_antennas_alarm(), None);

        assert!(shared.note_antennas(Antennas::Degraded, "right antenna: snagged"));
        assert_eq!(shared.antennas(), Antennas::Degraded);
        assert!(!shared.faulted(), "a degraded pair is not a fault");
        assert_eq!(
            shared.take_antennas_alarm().as_deref(),
            Some("right antenna: snagged")
        );
        assert_eq!(
            shared.take_antennas_alarm(),
            None,
            "one flip is not alerted on twice"
        );

        assert!(
            !shared.note_antennas(Antennas::Degraded, "right antenna: snagged again"),
            "the pair was already out of service, so nothing changed"
        );
        assert_eq!(shared.take_antennas_alarm(), None);
        assert_eq!(shared.antennas(), Antennas::Degraded);
    }

    /// A pair that came back is reported as being back, and going out again is
    /// news again: the next engage retries the antennas, so the standing is what
    /// they are doing now and not the worst they have ever done.
    #[test]
    fn a_pair_that_came_back_is_said_to_be_back() {
        let shared = shared();
        shared.note_antennas(Antennas::Degraded, "right antenna: snagged");
        shared.take_antennas_alarm();

        assert!(!shared.note_antennas(Antennas::Ok, "both answered the engage"));
        assert_eq!(shared.antennas(), Antennas::Ok);
        assert_eq!(
            shared.take_antennas_alarm(),
            None,
            "coming back is not itself an alert"
        );

        assert!(shared.note_antennas(Antennas::Degraded, "left antenna: hardware error 0x20"));
        assert_eq!(
            shared.take_antennas_alarm().as_deref(),
            Some("left antenna: hardware error 0x20")
        );
    }

    /// Offering a script to the schedule neither reads the antenna cell nor
    /// disturbs it.
    ///
    /// The two are deliberately not welded together: the schedule's verdict is
    /// about sequence numbers and timeouts, and what the sender is owed about the
    /// machine — that this session will move the head alone — is read from the
    /// cell where the answer is worded, one lock at a time. So the acceptance a
    /// delivery comes back with must not depend on the standing, and a delivery
    /// must not become a judgement of the pair: the standing is the motion
    /// thread's to write. (What a delivered script is *answered* with is
    /// asserted where that answer is built, in `bus`.)
    #[test]
    fn accepting_a_script_neither_reads_nor_writes_the_pairs_standing() {
        let shared = shared();
        let now = Instant::now();

        shared.note_antennas(Antennas::Degraded, "right antenna: snagged");
        assert!(
            shared.take_antennas_alarm().is_some(),
            "the flip owes an alert before the deliveries below"
        );

        assert_eq!(
            shared.accept(&script(2), now),
            taken(Acceptance::Accepted),
            "a degraded pair is not the schedule's business: the head still moves"
        );
        assert_eq!(shared.antennas(), Antennas::Degraded);
        assert_eq!(
            shared.accept(&script(1), now),
            taken(Acceptance::Stale {
                seq: 1,
                accepted: 2
            }),
            "and a refused delivery is refused for the schedule's own reason"
        );
        assert_eq!(
            shared.antennas(),
            Antennas::Degraded,
            "a delivery judged the pair"
        );
        assert_eq!(
            shared.take_antennas_alarm(),
            None,
            "a delivery left an alert owed about a standing nothing changed"
        );
    }

    /// A wind-down is told to the bus thread without touching the fault cell:
    /// the daemon is resting and taking scripts, which is why somebody has to be
    /// told at all — from outside, nothing about it looks different from a quiet
    /// afternoon.
    #[test]
    fn wind_downs_are_drained_with_their_count_and_are_not_faults() {
        let shared = shared();
        assert_eq!(shared.take_incident(), None);

        shared.note_incident("head_obstructed: leg 3 is 0.4000 rad from its goal");
        shared.note_incident("head_obstructed: leg 3 is 0.5000 rad from its goal");
        assert!(!shared.faulted(), "a wind-down back to rest is not a fault");

        let (detail, count) = shared.take_incident().expect("two wind-downs stand");
        assert!(
            detail.contains("0.5000"),
            "the latest describes the machine"
        );
        assert_eq!(count, 2);
        assert_eq!(shared.take_incident(), None);
    }

    /// The whole point of the type: one handle, two threads, no other contact
    /// between them.
    #[test]
    fn both_ends_reach_the_same_cells_across_threads() {
        let shared = Arc::new(shared());
        let now = Instant::now();

        let bus = Arc::clone(&shared);
        thread::spawn(move || {
            bus.accept(&script(1), now);
            bus.request_stop(Stop::Detached);
        })
        .join()
        .expect("the bus end runs");

        assert_eq!(shared.desired(now), Desired::Posture(Posture::Up));
        assert_eq!(shared.stopping(), Some(Stop::Detached));

        let motion = Arc::clone(&shared);
        thread::spawn(move || {
            motion.set_fault(FaultReport::new(FaultStage::Motion, "envelope on path"));
        })
        .join()
        .expect("the motion end runs");

        assert_eq!(
            shared.fault().map(|report| report.stage),
            Some(FaultStage::Motion)
        );
    }
}
