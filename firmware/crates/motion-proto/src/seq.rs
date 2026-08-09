//! The scripter's side of the ordering rule.
//!
//! Sequence numbers are authority in this protocol: the daemon drops anything
//! numbered at or below the last script it accepted. That makes a process-local
//! counter unusable — a scripter that restarts would count from zero again, and
//! a daemon holding a high mark would be deaf to every script it ever sent
//! afterwards.
//!
//! So the number is seeded from the wall clock in milliseconds and clamped
//! strictly increasing in-process: `seq = max(now_ms, last_sent + 1)`. A
//! restarted scripter resumes above its old high-water mark with nothing
//! persisted, and a clock that fails to advance between two emissions — a
//! coarse timer, or one stepped backwards — still yields numbers that climb.
//!
//! The residual, accepted for the MVP: a host wall clock stepped *backwards*
//! across a restart can leave scripts dropped until the clock passes the old
//! mark. It is self-healing and bounded by the size of the step.
//!
//! No clock is read here. The caller passes the instant it is emitting at, so
//! this stays testable and the crate keeps having no clock of its own.

use std::time::{SystemTime, UNIX_EPOCH};

/// Wall-clock milliseconds since the unix epoch, as the seq rule counts them.
///
/// Times before the epoch — a clock that has not been set at all — read as
/// zero rather than wrapping, and the in-process clamp carries the numbers
/// upward from there.
#[must_use]
pub fn unix_millis(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH).map_or(0, |since| {
        u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
    })
}

/// The scripter's sequence number source for one stream of scripts.
#[derive(Debug, Clone, Copy, Default)]
pub struct SeqSource {
    last: Option<u64>,
}

impl SeqSource {
    /// A source that has issued nothing yet.
    #[must_use]
    pub const fn new() -> Self {
        Self { last: None }
    }

    /// The number issued most recently, if any.
    #[must_use]
    pub const fn last(&self) -> Option<u64> {
        self.last
    }

    /// The next number, for a script being emitted at `now_ms`.
    #[must_use]
    pub fn next(&mut self, now_ms: u64) -> u64 {
        let seq = match self.last {
            Some(last) => now_ms.max(last.saturating_add(1)),
            None => now_ms,
        };
        self.last = Some(seq);
        seq
    }

    /// The next number, for a script being emitted at `at`.
    #[must_use]
    pub fn next_at(&mut self, at: SystemTime) -> u64 {
        self.next(unix_millis(at))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// The ordinary case: the number is the wall clock, so it is comparable
    /// across restarts without anything being persisted.
    #[test]
    fn a_number_is_the_wall_clock_when_the_clock_is_moving() {
        let mut source = SeqSource::new();
        assert_eq!(source.next(1_786_543_210_123), 1_786_543_210_123);
        assert_eq!(source.next(1_786_543_215_123), 1_786_543_215_123);
        assert_eq!(source.last(), Some(1_786_543_215_123));
    }

    /// Two emissions inside one millisecond still climb. The refresh cadence
    /// leaves millisecond granularity ample, but a burst — a barge and the hold
    /// script it causes — must not collide.
    #[test]
    fn numbers_climb_even_when_the_clock_does_not() {
        let mut source = SeqSource::new();
        let now = 1_786_543_210_123;

        assert_eq!(source.next(now), now);
        assert_eq!(source.next(now), now + 1);
        assert_eq!(source.next(now), now + 2);
        assert_eq!(source.next(now + 1), now + 3, "the clamp is still ahead");
        assert_eq!(source.next(now + 10), now + 10, "the clock catches up");
    }

    /// A clock stepped backwards mid-process does not silence the scripter:
    /// the clamp carries the numbers on from where they were.
    #[test]
    fn a_clock_stepped_backwards_does_not_stall_the_stream() {
        let mut source = SeqSource::new();
        let now = 1_786_543_210_123;
        assert_eq!(source.next(now), now);

        assert_eq!(source.next(now - 60_000), now + 1);
        assert_eq!(source.next(now - 60_000), now + 2);
    }

    /// A fresh source after a restart resumes above the numbers the old one
    /// issued, because both read the same clock.
    #[test]
    fn a_restarted_source_resumes_above_its_old_mark() {
        let mut before = SeqSource::new();
        let old = before.next(1_786_543_210_123);

        let mut after_restart = SeqSource::new();
        let fresh = after_restart.next(1_786_543_299_000);

        assert!(fresh > old, "{fresh} follows {old}");
    }

    /// The clock conversion, including the unset clock that reads as zero
    /// rather than as an error the scripter would have to handle.
    #[test]
    fn the_epoch_conversion_is_millis_and_never_fails() {
        assert_eq!(unix_millis(UNIX_EPOCH), 0);
        assert_eq!(unix_millis(UNIX_EPOCH + Duration::from_millis(1500)), 1500);
        assert_eq!(unix_millis(UNIX_EPOCH - Duration::from_secs(10)), 0);

        let mut source = SeqSource::new();
        assert_eq!(
            source.next_at(UNIX_EPOCH + Duration::from_millis(42)),
            42,
            "the same rule, taken from a `SystemTime`"
        );
    }
}
