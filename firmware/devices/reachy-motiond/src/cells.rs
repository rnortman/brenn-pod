//! Where the two threads meet: the schedule, the stop signal, and the fault.
//!
//! The bus thread is async and owns the attachment; the motion thread blocks and
//! owns the port. Neither can call into the other, and neither shares a data
//! structure with the other beyond what is here. Six cells, and each one goes
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
//! - **The stow misses** — written by the motion thread when an orderly release
//!   measured the machine somewhere other than its fold, drained the same way.
//!   Also not a fault: torque came off, which is the whole doctrine, and the
//!   daemon carries on. What it is, is the one thing an operator has to know
//!   before putting a hand near a head that has been left alone for hours.
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
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::Instant;

use motion_proto::{Acceptance, Desired, MotionScript, Schedule};

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
    /// The look at where the machine is standing, and the fold that follows it
    /// when a crash or a hand left the head somewhere else.
    Startup,
    /// The resting watch: the machine limp, being looked at.
    Resting,
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
            Self::Resting => "the resting watch",
            Self::Engage => "taking hold",
            Self::Motion => "the motion loop",
            Self::Release => "the release back to rest",
            Self::Shutdown => "shutdown",
        })
    }
}

/// What the motion thread stopped on.
///
/// The detail is text rather than the motion libraries' own error type, and
/// deliberately: the only consumer is an alert and a log line, and rendering at
/// the site that has the error keeps this cell — the one thing both threads
/// touch — free of the machine's vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultReport {
    /// Where the daemon was.
    pub stage: FaultStage,
    /// What the motion libraries refused, as they rendered it.
    pub detail: String,
}

impl FaultReport {
    pub fn new(stage: FaultStage, detail: impl Into<String>) -> Self {
        Self {
            stage,
            detail: detail.into(),
        }
    }
}

impl fmt::Display for FaultReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} stopped at {}", self.stage, self.detail)
    }
}

/// What a delivery did.
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

/// The six cells, held behind one handle both threads clone.
#[derive(Debug)]
pub struct Shared {
    schedule: Mutex<Schedule>,
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
        Self {
            schedule: Mutex::new(Schedule::new(pod)),
            engage_refusals: Mutex::new(Collapsed::default()),
            stow_misses: Mutex::new(Collapsed::default()),
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use motion_proto::{Posture, Step};

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

        assert_eq!(
            shared.accept(&script(1), now),
            Delivered::Scheduled(Acceptance::Accepted)
        );
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
        assert_eq!(
            shared.accept(&stow_now, now),
            Delivered::Scheduled(Acceptance::Accepted)
        );
        assert_eq!(shared.desired(now), Desired::Posture(Posture::Stow));

        assert_eq!(
            shared.accept(&script(7), now),
            Delivered::Scheduled(Acceptance::Stale {
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

        assert_eq!(
            shared.accept(&elsewhere, now),
            Delivered::Scheduled(Acceptance::Foreign)
        );
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
