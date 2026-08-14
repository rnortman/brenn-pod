//! The executor's state: deliveries in, one desired posture out.
//!
//! A daemon holds one [`Schedule`] for the pod it is. Every delivery goes
//! through [`Schedule::accept`], and the motion side asks [`Schedule::desired`]
//! at every tick or dwell boundary. Nothing else is state — there is no
//! conversation here, no turn, and no notion of why the head is going anywhere.
//!
//! Three rules, and they are the whole protocol:
//!
//! - **Sequence orders scripts.** One numbered at or below the last accepted is
//!   dropped, which makes a redelivery a no-op. Because that makes `seq`
//!   authority rather than observability, the scripter's numbers must survive
//!   its own restarts; [`crate::seq::SeqSource`] is how.
//! - **The latest accepted script wholly replaces the previous one.** There is
//!   no merging, no residue of the old timeline, and therefore no ordering
//!   invariant between two messages in flight.
//! - **Every script lapses.** At its expiry the schedule says so, and the
//!   daemon stows and rests. This is the loss-of-instruction bound: a scripter
//!   that dies mid-conversation leaves the head up for a bounded time and no
//!   longer. The bound is the script's timeout, full stop — a timeline that
//!   reached it was refused before it ever became a [`MotionScript`], so no
//!   step can be waiting when the lapse arrives.
//!
//! Offsets are measured from the moment a script *arrived*, on the caller's own
//! monotonic clock. Two hosts' wall clocks are never compared here, and one of
//! them has no battery-backed clock to compare with.

use std::time::{Duration, Instant};

use crate::script::{Base, MotionScript, Posture};

/// What a delivery did to the schedule.
///
/// Returned so the daemon can log the fact without inspecting the schedule
/// afterwards and inferring it. Every variant is normal operation; none is a
/// failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Acceptance {
    /// Accepted. It replaces whatever timeline was running.
    Accepted,
    /// Dropped: its number is at or below the last accepted one, so it is a
    /// redelivery or an overtaken message.
    Stale {
        /// The number the script carried.
        seq: u64,
        /// The number already accepted.
        accepted: u64,
    },
    /// The script was addressed to another pod. Nothing changed.
    Foreign,
}

/// What the schedule asks of the machine at an instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Desired {
    /// Nothing to change: no script is running, or none of its steps has come
    /// due yet. Whatever the machine is doing stands.
    Unchanged,
    /// The posture the running script names as of now.
    Posture(Posture),
    /// The running script asks for the base to stay where it is commanded now.
    ///
    /// Distinct from `Unchanged`, which is the absence of an instruction: this
    /// is an instruction, and one a machine mid-transition answers by stopping
    /// where it is rather than by carrying on. Folding it in with "nothing was
    /// asked" would turn a freeze into whatever the default is.
    Keep,
    /// The running script has lapsed. Stow and rest.
    ///
    /// Distinct from `Posture(Stow)` because it is also the daemon's leave to
    /// go back to the minimum risk condition: a stow *step* is a posture, while
    /// an expiry is the end of instruction.
    Expired,
}

/// The script currently in force, and when it landed.
#[derive(Debug, Clone)]
struct Running {
    script: MotionScript,
    received: Instant,
}

impl Running {
    /// Milliseconds since this script arrived, saturating rather than
    /// wrapping — a schedule that has been running for 585 million years reads
    /// as expired, which is the right answer anyway.
    fn elapsed_ms(&self, now: Instant) -> u64 {
        u64::try_from(now.saturating_duration_since(self.received).as_millis()).unwrap_or(u64::MAX)
    }

    /// The instant `offset_ms` past arrival, saturating at the far end of the
    /// clock's range.
    fn at(&self, offset_ms: u64) -> Instant {
        self.received
            .checked_add(Duration::from_millis(offset_ms))
            .unwrap_or(self.received)
    }
}

/// The script executor's state for one pod.
#[derive(Debug, Clone)]
pub struct Schedule {
    pod: String,
    /// The script in force, if one is.
    running: Option<Running>,
    /// The highest sequence number accepted so far. Forgotten on restart, which
    /// is the safe direction: the next script is accepted.
    high_water: Option<u64>,
}

impl Schedule {
    /// An empty schedule for `pod`.
    ///
    /// Nothing is running, so a daemon that has heard nothing — including one
    /// that just started in the middle of a conversation — changes no posture
    /// until somebody scripts one. Resting is the default state.
    pub fn new(pod: impl Into<String>) -> Self {
        Self {
            pod: pod.into(),
            running: None,
            high_water: None,
        }
    }

    /// The pod whose scripts this schedule obeys.
    #[must_use]
    pub fn pod(&self) -> &str {
        &self.pod
    }

    /// The highest sequence number accepted so far, if any.
    #[must_use]
    pub fn accepted_seq(&self) -> Option<u64> {
        self.high_water
    }

    /// The script in force, if one is.
    #[must_use]
    pub fn running(&self) -> Option<&MotionScript> {
        self.running.as_ref().map(|running| &running.script)
    }

    /// When the running script lapses, if one is running.
    #[must_use]
    pub fn expires_at(&self) -> Option<Instant> {
        self.running
            .as_ref()
            .map(|running| running.at(running.script.expiry_ms()))
    }

    /// Take one delivery, arriving at `now`.
    ///
    /// A script for another pod changes nothing — the channel may carry more
    /// than one machine's traffic, and obeying somebody else's timeline would
    /// move the wrong head.
    pub fn accept(&mut self, script: &MotionScript, now: Instant) -> Acceptance {
        if script.pod() != self.pod {
            return Acceptance::Foreign;
        }
        if let Some(accepted) = self.high_water
            && script.seq() <= accepted
        {
            return Acceptance::Stale {
                seq: script.seq(),
                accepted,
            };
        }
        self.high_water = Some(script.seq());
        self.running = Some(Running {
            script: script.clone(),
            received: now,
        });
        Acceptance::Accepted
    }

    /// What the machine should be doing as of `now`.
    ///
    /// This is the timeline's answer and nothing else. It says nothing about
    /// whether the machine may be commanded — a faulted daemon must not act on
    /// it.
    #[must_use]
    pub fn desired(&self, now: Instant) -> Desired {
        let Some(running) = self.running.as_ref() else {
            return Desired::Unchanged;
        };
        let elapsed = running.elapsed_ms(now);
        if elapsed >= running.script.expiry_ms() {
            return Desired::Expired;
        }
        match running.script.base_at(elapsed) {
            None => Desired::Unchanged,
            Some(Base::Posture(posture)) => Desired::Posture(posture),
            Some(Base::Keep) => Desired::Keep,
        }
    }

    /// The script in force at `now` and how long it has been running, or
    /// `None` when nothing is running or the timeline has lapsed.
    ///
    /// What a caller resolving overlays needs and [`Self::desired`] cannot
    /// carry: the windows are the script's own arithmetic against a library
    /// only the caller holds, so the script and its clock come out and the
    /// resolution happens there. The lapse is applied here, so an expired
    /// script has no overlays for the same reason it has no posture.
    #[must_use]
    pub fn running_at(&self, now: Instant) -> Option<(&MotionScript, u64)> {
        let running = self.running.as_ref()?;
        let elapsed = running.elapsed_ms(now);
        (elapsed < running.script.expiry_ms()).then_some((&running.script, elapsed))
    }

    /// The next instant at which [`Self::desired`] can change by itself, if
    /// there is one.
    ///
    /// The next step, or the expiry once the steps are spent. What the motion
    /// thread sizes a dwell against, so a script's own timeline is what wakes
    /// it rather than a poll interval it has to guess at.
    #[must_use]
    pub fn next_boundary(&self, now: Instant) -> Option<Instant> {
        let running = self.running.as_ref()?;
        let elapsed = running.elapsed_ms(now);
        let expiry = running.script.expiry_ms();
        if elapsed >= expiry {
            return None;
        }
        let offset = match running.script.next_step_ms(elapsed) {
            Some(step_ms) if step_ms < expiry => step_ms,
            _ => expiry,
        };
        Some(running.at(offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::script::{Play, Step};

    const POD: &str = "reachy00";

    fn script(seq: u64, steps: Vec<Step>, timeout_ms: u64) -> MotionScript {
        MotionScript::new(POD, seq, steps, timeout_ms).expect("a lawful script")
    }

    /// The nominal script: the head goes up now and stows when the speech is
    /// over, in one message.
    fn nominal(seq: u64) -> MotionScript {
        script(
            seq,
            vec![Step::new(0, Posture::Up), Step::new(6740, Posture::Stow)],
            30_000,
        )
    }

    fn ms(count: u64) -> Duration {
        Duration::from_millis(count)
    }

    /// A daemon that has heard nothing changes nothing. Resting is the default
    /// state; no message is needed to keep it there.
    #[test]
    fn an_empty_schedule_asks_for_nothing() {
        let schedule = Schedule::new(POD);
        assert_eq!(schedule.desired(Instant::now()), Desired::Unchanged);
        assert_eq!(schedule.next_boundary(Instant::now()), None);
        assert_eq!(schedule.accepted_seq(), None);
        assert_eq!(schedule.expires_at(), None);
        assert!(schedule.running().is_none());
        assert_eq!(schedule.pod(), POD);
    }

    /// The whole nominal case, walked in time: up at receipt, up through the
    /// speech, stowed at the scheduled instant, and lapsed at the timeout.
    #[test]
    fn a_script_is_executed_against_the_clock_that_received_it() {
        let start = Instant::now();
        let mut schedule = Schedule::new(POD);
        assert_eq!(schedule.accept(&nominal(1), start), Acceptance::Accepted);

        assert_eq!(schedule.desired(start), Desired::Posture(Posture::Up));
        assert_eq!(
            schedule.desired(start + ms(6739)),
            Desired::Posture(Posture::Up)
        );
        assert_eq!(
            schedule.desired(start + ms(6740)),
            Desired::Posture(Posture::Stow)
        );
        assert_eq!(
            schedule.desired(start + ms(29_999)),
            Desired::Posture(Posture::Stow)
        );
        assert_eq!(schedule.desired(start + ms(30_000)), Desired::Expired);
        assert_eq!(schedule.desired(start + ms(600_000)), Desired::Expired);
        assert_eq!(schedule.expires_at(), Some(start + ms(30_000)));
    }

    /// The overlay caller's view of the same timeline: nothing before a script
    /// lands, the script and its own elapsed clock while it runs, and nothing
    /// once it lapses — the same instant `desired` starts answering `Expired`.
    #[test]
    fn the_running_script_and_its_clock_come_out_until_the_lapse() {
        let start = Instant::now();
        let mut schedule = Schedule::new(POD);
        assert!(schedule.running_at(start).is_none());

        schedule.accept(&nominal(1), start);

        let (script, elapsed) = schedule.running_at(start).expect("a running script");
        assert_eq!(script.seq(), 1);
        assert_eq!(elapsed, 0);

        let (_, elapsed) = schedule
            .running_at(start + ms(6_740))
            .expect("still running");
        assert_eq!(elapsed, 6_740);

        let (_, elapsed) = schedule
            .running_at(start + ms(29_999))
            .expect("running to the last millisecond");
        assert_eq!(elapsed, 29_999);

        assert!(schedule.running_at(start + ms(30_000)).is_none());
        assert!(schedule.running_at(start + ms(600_000)).is_none());
    }

    /// A step still ahead of the clock does not disturb the machine. A hold
    /// script's `up@0` is immediate; a closing script's stow is a future the
    /// daemon waits out.
    #[test]
    fn a_script_whose_first_step_is_ahead_changes_nothing_yet() {
        let start = Instant::now();
        let mut schedule = Schedule::new(POD);
        schedule.accept(
            &script(1, vec![Step::new(8_000, Posture::Stow)], 30_000),
            start,
        );

        assert_eq!(schedule.desired(start), Desired::Unchanged);
        assert_eq!(schedule.desired(start + ms(7_999)), Desired::Unchanged);
        assert_eq!(
            schedule.desired(start + ms(8_000)),
            Desired::Posture(Posture::Stow)
        );
    }

    /// An empty timeline is lawful and inert: it commands no posture, and its
    /// only effect is the expiry that ends it.
    #[test]
    fn an_empty_script_only_expires() {
        let start = Instant::now();
        let mut schedule = Schedule::new(POD);
        assert_eq!(
            schedule.accept(&script(1, vec![], 5_000), start),
            Acceptance::Accepted
        );

        assert_eq!(schedule.desired(start), Desired::Unchanged);
        assert_eq!(schedule.desired(start + ms(4_999)), Desired::Unchanged);
        assert_eq!(schedule.desired(start + ms(5_000)), Desired::Expired);
        assert_eq!(schedule.next_boundary(start), Some(start + ms(5_000)));
    }

    /// The replacement rule, which is what makes a re-emit cheap: the new
    /// script's timeline is the whole answer, measured from when *it* landed,
    /// and nothing of the old one survives.
    #[test]
    fn a_later_script_wholly_replaces_the_one_running() {
        let start = Instant::now();
        let mut schedule = Schedule::new(POD);
        schedule.accept(&nominal(1), start);

        // A continuation: the same conversation, stowing later.
        let later = start + ms(3_000);
        let extended = script(
            2,
            vec![Step::new(0, Posture::Up), Step::new(9_000, Posture::Stow)],
            30_000,
        );
        assert_eq!(schedule.accept(&extended, later), Acceptance::Accepted);

        // The old script's 6.74 s stow is gone, not merely postponed.
        assert_eq!(
            schedule.desired(start + ms(6_740)),
            Desired::Posture(Posture::Up)
        );
        assert_eq!(
            schedule.desired(later + ms(9_000)),
            Desired::Posture(Posture::Stow)
        );
        assert_eq!(schedule.expires_at(), Some(later + ms(30_000)));
        assert_eq!(schedule.running().map(MotionScript::seq), Some(2));
    }

    /// A redelivery, and a message overtaken in flight, are both dropped — and
    /// say so, so the daemon reports the drop rather than inferring it from a
    /// schedule that did not change.
    #[test]
    fn a_script_at_or_below_the_accepted_number_is_dropped() {
        let start = Instant::now();
        let mut schedule = Schedule::new(POD);
        schedule.accept(&nominal(7), start);

        assert_eq!(
            schedule.accept(&nominal(7), start + ms(10)),
            Acceptance::Stale {
                seq: 7,
                accepted: 7
            }
        );
        assert_eq!(
            schedule.accept(&nominal(3), start + ms(20)),
            Acceptance::Stale {
                seq: 3,
                accepted: 7
            }
        );
        // Dropped means dropped: the running script's own clock did not move.
        assert_eq!(schedule.expires_at(), Some(start + ms(30_000)));
        assert_eq!(schedule.accepted_seq(), Some(7));
    }

    /// The host restart case the wall-clock seq rule exists for: a fresh
    /// scripter's first number is above the mark this daemon is holding, so its
    /// first script lands instead of being deaf-dropped forever.
    #[test]
    fn a_restarted_scripter_is_heard_because_its_numbers_climb() {
        let start = Instant::now();
        let mut schedule = Schedule::new(POD);
        schedule.accept(&nominal(1_786_543_210_123), start);

        let after_restart = nominal(1_786_543_299_000);
        assert_eq!(
            schedule.accept(&after_restart, start + ms(50)),
            Acceptance::Accepted
        );
        assert_eq!(schedule.accepted_seq(), Some(1_786_543_299_000));
    }

    /// A daemon restart runs the other way: the mark is forgotten, and the next
    /// script — whatever it is numbered — is accepted.
    #[test]
    fn a_restarted_daemon_has_no_mark_to_be_deaf_behind() {
        let start = Instant::now();
        let mut schedule = Schedule::new(POD);
        assert_eq!(schedule.accept(&nominal(2), start), Acceptance::Accepted);

        let restarted = Schedule::new(POD).accepted_seq();
        assert_eq!(restarted, None);
    }

    /// Another machine's script moves nothing here, and is not recorded as
    /// something this schedule has seen.
    #[test]
    fn another_pods_script_is_reported_and_ignored() {
        let start = Instant::now();
        let mut schedule = Schedule::new(POD);
        schedule.accept(&nominal(1), start);

        let elsewhere =
            MotionScript::new("reachy01", 99, vec![Step::new(0, Posture::Stow)], 30_000)
                .expect("a lawful script");

        assert_eq!(
            schedule.accept(&elsewhere, start + ms(10)),
            Acceptance::Foreign
        );
        assert_eq!(
            schedule.desired(start + ms(10)),
            Desired::Posture(Posture::Up)
        );
        assert_eq!(schedule.accepted_seq(), Some(1), "not this stream");
    }

    /// A script that lands after its own steps were due collapses to the one
    /// posture that matters. A daemon that was busy for a second does not
    /// replay a timeline at the machine.
    #[test]
    fn past_due_steps_collapse_to_the_last_one() {
        let start = Instant::now();
        let mut schedule = Schedule::new(POD);
        schedule.accept(&nominal(1), start);

        assert_eq!(
            schedule.desired(start + ms(20_000)),
            Desired::Posture(Posture::Stow),
            "the up step is history; only the stow still stands"
        );
    }

    /// What the motion thread sizes a dwell against: the next step while there
    /// is one, then the expiry, then nothing at all.
    #[test]
    fn the_next_boundary_walks_the_timeline_then_the_expiry() {
        let start = Instant::now();
        let mut schedule = Schedule::new(POD);
        schedule.accept(&nominal(1), start);

        assert_eq!(schedule.next_boundary(start), Some(start + ms(6_740)));
        assert_eq!(
            schedule.next_boundary(start + ms(6_740)),
            Some(start + ms(30_000))
        );
        assert_eq!(schedule.next_boundary(start + ms(30_000)), None);
    }

    /// A step one millisecond inside the timeout is executed, and the lapse
    /// arrives the millisecond after it. That margin is why validation demands
    /// a *strictly* smaller last step: level with the timeout, the expiry check
    /// running first would answer `Expired` at the instant the step came due
    /// and the script's last instruction would never be seen.
    #[test]
    fn the_last_step_inside_the_timeout_is_reached_before_the_lapse() {
        let start = Instant::now();
        let mut schedule = Schedule::new(POD);
        schedule.accept(
            &script(
                1,
                vec![Step::new(0, Posture::Up), Step::new(8_999, Posture::Stow)],
                9_000,
            ),
            start,
        );

        assert_eq!(
            schedule.desired(start + ms(8_998)),
            Desired::Posture(Posture::Up)
        );
        assert_eq!(
            schedule.desired(start + ms(8_999)),
            Desired::Posture(Posture::Stow)
        );
        assert_eq!(schedule.desired(start + ms(9_000)), Desired::Expired);
        assert_eq!(schedule.next_boundary(start), Some(start + ms(8_999)));
        assert_eq!(
            schedule.next_boundary(start + ms(8_999)),
            Some(start + ms(9_000))
        );
    }

    /// A last step that is not a stow is executed too. Today every timeline
    /// ends in one, so a swallowed final step would be invisible; the schema's
    /// growth path is new postures, and this is the case that would break.
    #[test]
    fn a_final_step_that_is_not_a_stow_is_reached_before_the_lapse() {
        let start = Instant::now();
        let mut schedule = Schedule::new(POD);
        schedule.accept(
            &script(1, vec![Step::new(9_000, Posture::Up)], 9_001),
            start,
        );

        assert_eq!(
            schedule.desired(start + ms(9_000)),
            Desired::Posture(Posture::Up)
        );
        assert_eq!(schedule.desired(start + ms(9_001)), Desired::Expired);
    }

    /// A `keep` step asks for something, and what it asks for is distinct from
    /// the absence of an instruction: a machine mid-transition answers a freeze
    /// to one and carries on for the other.
    #[test]
    fn a_keep_step_asks_for_the_base_to_stay_where_it_is() {
        let start = Instant::now();
        let mut schedule = Schedule::new(POD);
        schedule.accept(
            &script(
                1,
                vec![
                    Step::new(0, Posture::Up),
                    Step::keep(2_000),
                    Step::new(4_000, Posture::Stow),
                ],
                30_000,
            ),
            start,
        );

        assert_eq!(schedule.desired(start), Desired::Posture(Posture::Up));
        assert_eq!(
            schedule.desired(start + ms(1_999)),
            Desired::Posture(Posture::Up)
        );
        assert_eq!(schedule.desired(start + ms(2_000)), Desired::Keep);
        assert_eq!(schedule.desired(start + ms(3_999)), Desired::Keep);
        assert_eq!(
            schedule.desired(start + ms(4_000)),
            Desired::Posture(Posture::Stow)
        );
        // A script holding via `keep` still lapses like every other one.
        assert_eq!(schedule.desired(start + ms(30_000)), Desired::Expired);
    }

    /// The steps that start overlays are not base steps: the base is what the
    /// last due *base* step said, however many motions have started since, and
    /// a play step still moves the boundary the motion thread waits on.
    #[test]
    fn play_steps_move_the_clock_without_moving_the_base() {
        let start = Instant::now();
        let mut schedule = Schedule::new(POD);
        schedule.accept(
            &script(
                1,
                vec![
                    Step::new(0, Posture::Up),
                    Step::play(400, Play::new("pod/wiggle")),
                    Step::new(9_000, Posture::Stow),
                ],
                30_000,
            ),
            start,
        );

        assert_eq!(
            schedule.desired(start + ms(400)),
            Desired::Posture(Posture::Up)
        );
        assert_eq!(schedule.next_boundary(start), Some(start + ms(400)));
        assert_eq!(
            schedule.next_boundary(start + ms(400)),
            Some(start + ms(9_000))
        );
    }
}
