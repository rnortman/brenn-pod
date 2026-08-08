//! Where the two threads meet: the lease, the stop signal, and the fault.
//!
//! The bus thread is async and owns the attachment; the motion thread blocks and
//! owns the port. Neither can call into the other, and neither shares a data
//! structure with the other beyond what is here. Four cells, and each one goes
//! in a single direction:
//!
//! - **The lease** — written by the bus thread as intents arrive, read by the
//!   motion thread between moves. It is the only thing the bus can make the
//!   machine do, and it can only ever say *engaged* or *idle*.
//! - **The stop signal** — set on an operator's signal or when the daemon loses
//!   its source of intents, read by the motion thread at the top of every dwell.
//!   It is what turns a signal into a stow, a verify and a release, all of which
//!   happen on the thread that owns the port. It carries *why*, because only one
//!   of the two reasons may take torque off.
//! - **The fault** — written once by the motion thread when the machine stops
//!   taking commands, read by the bus thread so it can alert. Write-once because
//!   a fault is not a state that improves: the first thing that went wrong is
//!   the thing worth reporting, and everything after it is a consequence.
//! - **The ending** — written once by the motion thread when it has finished
//!   with the machine, read by the bus thread as its own leave to end the
//!   attachment. A stow, a nine-position verify and a servo-by-servo release
//!   take seconds, and any of them can fault; an attachment closed on the stop
//!   signal instead would be gone before the alert that fault owes could travel
//!   over it.
//!
//! The fault is also what stops the lease mattering. Once it is set the bus
//! thread stops folding intents in, because a machine that has faulted takes no
//! commands at all — not a stow, not a park, nothing until an operator acts.
//! Torque stays on and the servos hold where they were left.

use std::fmt;
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use presence_proto::{Lease, PresenceBody, PresenceState, Reduction};

/// Why the motion thread is being asked to stop, and so what it does on the way
/// out.
///
/// The two endings differ in exactly one thing — whether torque comes off — and
/// that difference is not the daemon's to infer. Releasing drops the head unless
/// the machine is verified at stow, and it is only ever an explicit operator
/// action; a daemon that has merely run out of things to obey has no operator in
/// front of it and must leave the servos holding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    /// An operator signalled a foreground run. Stow, verify the nine positions
    /// against the stow pose, and release torque servo by servo.
    Operator,
    /// The daemon has no source of intents left — the attachment ended in an
    /// outcome no reconnection follows. Stow, and leave torque on: the head is
    /// parked deliberately, but nobody is present to catch it.
    Detached,
}

impl fmt::Display for Stop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Operator => "an operator's signal",
            Self::Detached => "the loss of the intent source",
        })
    }
}

/// Where the daemon was when it stopped commanding.
///
/// Carried on the alert because the same refusal means different things at
/// different points: during arming the machine was never taken, during the
/// presence loop it was up or on its way somewhere, and during shutdown it is
/// holding torque with an operator waiting on a prompt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultStage {
    /// The arm sequence — the machine was never taken.
    Arm,
    /// The move to stow that every run starts with, once armed.
    InitialStow,
    /// The steady state: dwelling, polling the lease, moving between postures.
    Presence,
    /// The stow, verify and release an operator's signal asks for.
    Shutdown,
}

impl fmt::Display for FaultStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Arm => "arming",
            Self::InitialStow => "the initial stow",
            Self::Presence => "the presence loop",
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
    /// Folded into the lease, with what it did to it.
    Reduced(Reduction),
    /// Dropped: the machine has faulted and takes no further intents.
    Faulted,
}

/// The four cells, held behind one handle both threads clone.
#[derive(Debug)]
pub struct Shared {
    lease: Mutex<Lease>,
    stop: OnceLock<Stop>,
    fault: OnceLock<FaultReport>,
    ended: OnceLock<()>,
}

impl Shared {
    /// A daemon that has heard nothing, is not shutting down, and has not
    /// faulted.
    ///
    /// The lease starts released, so a daemon that comes up in the middle of a
    /// conversation wants the head stowed until the next refresh says otherwise.
    pub fn new(pod: impl Into<String>) -> Self {
        Self {
            lease: Mutex::new(Lease::new(pod)),
            stop: OnceLock::new(),
            fault: OnceLock::new(),
            ended: OnceLock::new(),
        }
    }

    /// Fold one delivery into the lease, as of `now`, with `ttl` as the term.
    ///
    /// Refused once the machine has faulted: the motion thread has stopped
    /// commanding, and a lease that kept moving underneath it would only mean
    /// the daemon's log disagreed with the machine.
    pub fn apply(&self, body: &PresenceBody, now: Instant, ttl: Duration) -> Delivered {
        if self.faulted() {
            return Delivered::Faulted;
        }
        Delivered::Reduced(self.lease().apply(body, now, ttl))
    }

    /// The posture the machine should be in as of `now`.
    ///
    /// This is the lease's answer and nothing else. It says nothing about
    /// whether the machine may be commanded — a faulted daemon must not act on
    /// it, and the motion thread checks [`Self::fault`] first.
    pub fn desired(&self, now: Instant) -> PresenceState {
        self.lease().desired(now)
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
    /// Set on every path that leaves the machine unattended — the release, the
    /// torque-held park, a fault taken on the way out, and an arm sequence that
    /// never took the machine at all. Until it is set the bus thread keeps the
    /// attachment up, because a fault raised during the ending still owes an
    /// alert.
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

    /// The lease, with a poisoned lock recovered rather than propagated.
    ///
    /// The lease is a value, not an invariant spread over several fields: every
    /// operation on it either completes or leaves it exactly as it was, so a
    /// thread that panicked while holding this lock cannot have left a
    /// half-reduced state behind. Propagating the poison instead would stop the
    /// motion thread polling, and the poll is what keeps fault detection alive
    /// on a machine nobody is watching.
    fn lease(&self) -> std::sync::MutexGuard<'_, Lease> {
        self.lease.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::*;

    const TTL: Duration = Duration::from_secs(15);
    const POD: &str = "reachy00";

    fn shared() -> Shared {
        Shared::new(POD)
    }

    fn engaged(seq: u64) -> PresenceBody {
        PresenceBody::new(POD, PresenceState::Engaged, seq)
    }

    fn idle(seq: u64) -> PresenceBody {
        PresenceBody::new(POD, PresenceState::Idle, seq)
    }

    /// A daemon that has heard nothing wants the head stowed. This is the state
    /// every failure of the system converges back to, so it is also the state it
    /// starts in.
    #[test]
    fn a_fresh_daemon_is_idle() {
        let shared = shared();
        assert_eq!(shared.desired(Instant::now()), PresenceState::Idle);
        assert_eq!(shared.stopping(), None);
        assert!(!shared.faulted());
        assert_eq!(shared.fault(), None);
        assert!(!shared.motion_ended());
    }

    #[test]
    fn an_engaged_intent_holds_a_term_and_then_lapses() {
        let shared = shared();
        let now = Instant::now();

        let Delivered::Reduced(Reduction::Engaged { until }) = shared.apply(&engaged(1), now, TTL)
        else {
            panic!("an engaged body takes the lease");
        };
        assert_eq!(until, now + TTL);
        assert_eq!(shared.desired(now + TTL / 2), PresenceState::Engaged);
        assert_eq!(shared.desired(now + TTL), PresenceState::Idle);
    }

    #[test]
    fn a_refresh_extends_the_term_from_when_it_arrived() {
        let shared = shared();
        let now = Instant::now();

        shared.apply(&engaged(1), now, TTL);
        let arrived = now + Duration::from_secs(5);
        let Delivered::Reduced(Reduction::Engaged { until }) =
            shared.apply(&engaged(2), arrived, TTL)
        else {
            panic!("a refresh takes the lease again");
        };

        assert_eq!(until, arrived + TTL);
        assert_eq!(shared.desired(now + TTL), PresenceState::Engaged);
        assert_eq!(shared.desired(arrived + TTL), PresenceState::Idle);
    }

    #[test]
    fn an_idle_intent_releases_at_once() {
        let shared = shared();
        let now = Instant::now();

        shared.apply(&engaged(1), now, TTL);
        assert_eq!(
            shared.apply(&idle(2), now, TTL),
            Delivered::Reduced(Reduction::Idle)
        );
        assert_eq!(shared.desired(now), PresenceState::Idle);
    }

    /// Another machine's intent moves nothing here, and says so, so the daemon
    /// can report it rather than infer it from a lease that did not change.
    #[test]
    fn another_pods_intent_is_reported_and_dropped() {
        let shared = shared();
        let now = Instant::now();
        let elsewhere = PresenceBody::new("reachy01", PresenceState::Engaged, 1);

        assert_eq!(
            shared.apply(&elsewhere, now, TTL),
            Delivered::Reduced(Reduction::Foreign)
        );
        assert_eq!(shared.desired(now), PresenceState::Idle);
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
        let first = FaultReport::new(FaultStage::Presence, "the tick faulted: tracking lost");

        assert!(shared.set_fault(first.clone()));
        assert!(!shared.set_fault(FaultReport::new(FaultStage::Shutdown, "servo 4: timed out")));
        assert_eq!(shared.fault(), Some(&first));
        assert!(shared.faulted());
    }

    /// A faulted machine takes no further intents. The lease it was holding is
    /// left exactly as it was — nothing about a fault means the head should
    /// move, in either direction.
    #[test]
    fn a_faulted_daemon_stops_reducing_intents() {
        let shared = shared();
        let now = Instant::now();

        shared.apply(&engaged(1), now, TTL);
        shared.set_fault(FaultReport::new(FaultStage::Presence, "read loss"));

        assert_eq!(shared.apply(&idle(2), now, TTL), Delivered::Faulted);
        assert_eq!(shared.desired(now), PresenceState::Engaged);
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
        let report = FaultReport::new(FaultStage::Arm, "servo 7 answered nothing");
        assert_eq!(
            report.to_string(),
            "arming stopped at servo 7 answered nothing"
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
            bus.apply(&engaged(1), now, TTL);
            bus.request_stop(Stop::Detached);
        })
        .join()
        .expect("the bus end runs");

        assert_eq!(shared.desired(now), PresenceState::Engaged);
        assert_eq!(shared.stopping(), Some(Stop::Detached));

        let motion = Arc::clone(&shared);
        thread::spawn(move || {
            motion.set_fault(FaultReport::new(FaultStage::Presence, "envelope on path"));
        })
        .join()
        .expect("the motion end runs");

        assert_eq!(
            shared.fault().map(|report| report.stage),
            Some(FaultStage::Presence)
        );
    }
}
