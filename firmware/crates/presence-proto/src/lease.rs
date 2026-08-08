//! The reducer: deliveries in, one desired posture out.
//!
//! A consumer holds one [`Lease`] for the pod it is. Every delivery goes
//! through [`Lease::apply`], and the motion side asks [`Lease::desired`]
//! whenever it is between moves. Nothing else is state.
//!
//! An engaged intent takes a lease that runs out; the publisher keeps it alive
//! by republishing well inside the term. An idle intent ends the lease at once.
//! Both directions are idempotent, so a duplicate delivery is a no-op that
//! happens to refresh the clock, which is exactly what a duplicate should be.
//!
//! The deadline is `now + ttl` on the caller's own monotonic clock, where `now`
//! is when the message *arrived* — not any timestamp inside it. Two hosts'
//! wall clocks are never compared, and one of them has no battery-backed clock
//! to compare with.

use std::time::{Duration, Instant};

use crate::body::{PresenceBody, PresenceState};

/// What a delivery did to the lease.
///
/// Returned so the consumer can log the fact without inspecting the lease
/// afterwards and inferring it. Every variant is normal operation; none is a
/// failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reduction {
    /// The lease is held, and runs out at this instant unless refreshed.
    Engaged {
        /// When the lease lapses if nothing else arrives.
        until: Instant,
    },
    /// The lease is released, effective immediately.
    Idle,
    /// The body was addressed to another pod. Nothing changed.
    Foreign,
}

/// The desired-presence state machine for one pod.
#[derive(Debug, Clone)]
pub struct Lease {
    pod: String,
    /// When the engaged lease lapses. `None` is idle — released, or never held.
    until: Option<Instant>,
    /// The last sequence number this lease accepted, for observability only.
    seq: Option<u64>,
}

impl Lease {
    /// A released lease for `pod`.
    ///
    /// The starting posture is idle, so a consumer that has heard nothing yet —
    /// including one that just started in the middle of a conversation — wants
    /// the head stowed until somebody says otherwise.
    pub fn new(pod: impl Into<String>) -> Self {
        Self {
            pod: pod.into(),
            until: None,
            seq: None,
        }
    }

    /// The pod whose intents this lease obeys.
    #[must_use]
    pub fn pod(&self) -> &str {
        &self.pod
    }

    /// The last accepted sequence number, if any has been accepted.
    #[must_use]
    pub fn seq(&self) -> Option<u64> {
        self.seq
    }

    /// When the engaged lease lapses, if it is held.
    ///
    /// A held lease whose instant is already past reads as idle from
    /// [`Self::desired`]; this is the raw deadline, for a consumer that wants
    /// to say how long is left.
    #[must_use]
    pub fn until(&self) -> Option<Instant> {
        self.until
    }

    /// Fold one delivery in, as of `now`, with `ttl` as the term of an engaged
    /// lease.
    ///
    /// A body for another pod changes nothing — the channel may carry more than
    /// one machine's traffic, and obeying somebody else's intent would move the
    /// wrong head.
    pub fn apply(&mut self, body: &PresenceBody, now: Instant, ttl: Duration) -> Reduction {
        if body.pod != self.pod {
            return Reduction::Foreign;
        }
        self.seq = Some(body.seq);
        match body.state {
            PresenceState::Engaged => {
                // A term the clock cannot represent yields a deadline already
                // past, which reads as idle. The unrepresentable case is a
                // misconfiguration, and the safe answer to one is the stowed
                // head, not a lease that never lapses.
                let until = now.checked_add(ttl).unwrap_or(now);
                self.until = Some(until);
                Reduction::Engaged { until }
            }
            PresenceState::Idle => {
                self.until = None;
                Reduction::Idle
            }
        }
    }

    /// The posture the machine should be in as of `now`.
    ///
    /// Engaged only while a lease is held and has not lapsed. Everything else —
    /// never engaged, explicitly released, lapsed — is idle, which is the
    /// posture every failure of this system converges on.
    #[must_use]
    pub fn desired(&self, now: Instant) -> PresenceState {
        match self.until {
            Some(until) if now < until => PresenceState::Engaged,
            _ => PresenceState::Idle,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fixed instant to reason from. Named so the arithmetic below reads as
    /// times rather than as offsets from whenever the test happened to run.
    fn origin() -> Instant {
        Instant::now()
    }

    const TTL: Duration = Duration::from_secs(15);

    fn engaged(seq: u64) -> PresenceBody {
        PresenceBody::new("reachy00", PresenceState::Engaged, seq)
    }

    fn idle(seq: u64) -> PresenceBody {
        PresenceBody::new("reachy00", PresenceState::Idle, seq)
    }

    /// Nothing has been heard, so nothing is up. A consumer that starts mid
    /// conversation stows until an intent arrives, rather than assuming the
    /// state it was in before it restarted.
    #[test]
    fn a_fresh_lease_is_idle() {
        let lease = Lease::new("reachy00");
        assert_eq!(lease.desired(origin()), PresenceState::Idle);
        assert_eq!(lease.seq(), None);
        assert_eq!(lease.until(), None);
        assert_eq!(lease.pod(), "reachy00");
    }

    /// The lease is a term, not a latch: engaged until the term runs out, idle
    /// after. This is the property the whole failure story rests on — a
    /// publisher that dies stops refreshing, and the head comes down by itself.
    #[test]
    fn an_engaged_lease_runs_out() {
        let start = origin();
        let mut lease = Lease::new("reachy00");

        assert_eq!(
            lease.apply(&engaged(1), start, TTL),
            Reduction::Engaged { until: start + TTL }
        );
        assert_eq!(lease.desired(start), PresenceState::Engaged);
        assert_eq!(
            lease.desired(start + TTL - Duration::from_millis(1)),
            PresenceState::Engaged
        );
        // The boundary belongs to idle: at the deadline the term is spent.
        assert_eq!(lease.desired(start + TTL), PresenceState::Idle);
        assert_eq!(
            lease.desired(start + TTL + Duration::from_secs(60)),
            PresenceState::Idle
        );
    }

    /// A refresh inside the term extends it from when it arrived, so a
    /// conversation that keeps going keeps the head up without the publisher
    /// having to know how long the term is.
    #[test]
    fn a_refresh_extends_the_term_from_its_arrival() {
        let start = origin();
        let mut lease = Lease::new("reachy00");
        lease.apply(&engaged(1), start, TTL);

        let refreshed_at = start + Duration::from_secs(5);
        assert_eq!(
            lease.apply(&engaged(2), refreshed_at, TTL),
            Reduction::Engaged {
                until: refreshed_at + TTL
            }
        );
        // Past the original deadline, still up, because the refresh moved it.
        assert_eq!(lease.desired(start + TTL), PresenceState::Engaged);
        assert_eq!(lease.desired(refreshed_at + TTL), PresenceState::Idle);
    }

    /// A duplicate delivery is not an event. It refreshes the clock and says
    /// the same thing, which is what makes a live-only subscription with a
    /// republishing publisher safe to reduce naively.
    #[test]
    fn duplicates_are_idempotent() {
        let start = origin();
        let mut lease = Lease::new("reachy00");

        lease.apply(&engaged(1), start, TTL);
        lease.apply(&engaged(1), start, TTL);
        assert_eq!(lease.desired(start), PresenceState::Engaged);
        assert_eq!(lease.until(), Some(start + TTL));

        lease.apply(&idle(2), start, TTL);
        lease.apply(&idle(2), start, TTL);
        assert_eq!(lease.desired(start), PresenceState::Idle);
    }

    /// An explicit idle does not wait for the term to run out. The common case
    /// — a conversation that ended — should stow now, not in fifteen seconds.
    #[test]
    fn an_explicit_idle_ends_the_lease_at_once() {
        let start = origin();
        let mut lease = Lease::new("reachy00");
        lease.apply(&engaged(1), start, TTL);

        assert_eq!(lease.apply(&idle(2), start, TTL), Reduction::Idle);
        assert_eq!(lease.desired(start), PresenceState::Idle);
        assert_eq!(lease.until(), None);
    }

    /// A later engaged intent re-takes a lease that was released. Nothing about
    /// going idle is terminal — the next wake word raises the head again.
    #[test]
    fn a_released_lease_can_be_retaken() {
        let start = origin();
        let mut lease = Lease::new("reachy00");
        lease.apply(&engaged(1), start, TTL);
        lease.apply(&idle(2), start, TTL);

        let again = start + Duration::from_secs(30);
        lease.apply(&engaged(3), again, TTL);
        assert_eq!(lease.desired(again), PresenceState::Engaged);
    }

    /// Another machine's intent moves nothing here, in either direction: it
    /// neither raises this head nor stows it, and it is not recorded as
    /// something this lease has seen.
    #[test]
    fn another_pods_intent_is_reported_and_ignored() {
        let start = origin();
        let mut lease = Lease::new("reachy00");
        lease.apply(&engaged(1), start, TTL);

        let foreign_engaged = PresenceBody::new("reachy01", PresenceState::Engaged, 99);
        let foreign_idle = PresenceBody::new("reachy01", PresenceState::Idle, 100);

        assert_eq!(
            lease.apply(&foreign_idle, start, TTL),
            Reduction::Foreign,
            "somebody else's idle does not stow this head"
        );
        assert_eq!(lease.desired(start), PresenceState::Engaged);
        assert_eq!(lease.seq(), Some(1), "a foreign body is not this stream");

        lease.apply(&idle(2), start, TTL);
        assert_eq!(
            lease.apply(&foreign_engaged, start, TTL),
            Reduction::Foreign
        );
        assert_eq!(
            lease.desired(start),
            PresenceState::Idle,
            "somebody else's engaged does not raise this head"
        );
    }

    /// The sequence number is observable — a consumer can log the stream and
    /// see a gap — and it is not authority. An intent numbered below the last
    /// one still applies, because a publisher that restarted counts from zero
    /// and a seq-ordered consumer would ignore it forever.
    #[test]
    fn the_sequence_number_is_observable_and_not_authority() {
        let start = origin();
        let mut lease = Lease::new("reachy00");

        lease.apply(&engaged(41), start, TTL);
        assert_eq!(lease.seq(), Some(41));

        lease.apply(&idle(42), start, TTL);
        assert_eq!(lease.seq(), Some(42));

        // A restarted publisher, counting again from the beginning.
        lease.apply(&engaged(0), start, TTL);
        assert_eq!(lease.seq(), Some(0));
        assert_eq!(lease.desired(start), PresenceState::Engaged);
    }

    /// A term the clock cannot represent lands the head where every other
    /// failure lands it. The alternative — saturating to the far future — is a
    /// lease that never lapses, which is the one outcome this design refuses.
    #[test]
    fn an_unrepresentable_term_is_idle_rather_than_forever() {
        let start = origin();
        let mut lease = Lease::new("reachy00");

        let reduction = lease.apply(&engaged(1), start, Duration::MAX);
        assert_eq!(reduction, Reduction::Engaged { until: start });
        assert_eq!(lease.desired(start), PresenceState::Idle);
    }
}
