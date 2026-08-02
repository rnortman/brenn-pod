//! Poll-readiness seam for the streamer event loop.
//!
//! The pacing tick, per-wake step budgets, timeout arithmetic, readiness
//! classification and the bounded writability wait are all platform-independent.
//! The `poll()` call itself is not — the event-mask constants and the syscall
//! differ per platform — so it sits behind [`NetPoll`], which each platform
//! implements over its own shim.

use std::io;
use std::os::fd::RawFd;
use std::time::{Duration, Instant};

use audio_pipeline::stream_send::Writable;

use crate::link::PollInterest;

/// Streamer idle/pacing tick period. Single source of truth for in-segment pacing
/// and the idle-loop `recv_timeout`.
pub const IDLE_TICK: Duration = Duration::from_millis(10);

/// Maximum `drain_inbound` calls one inbound pump performs per poll wake before
/// yielding for fairness. A pump that stops here re-polls with timeout 0, so
/// throughput is not sacrificed; the cap only bounds per-wake work under a flood.
pub const INBOUND_STEPS_PER_WAKE: u32 = 8;

/// Maximum completed outbound frames one wake writes before yielding for fairness.
/// Dwarfs the ~50 frames/s production rate, so the ring's overrun deadline is met
/// comfortably even when inbound is serviced first.
pub const OUTBOUND_FRAMES_PER_WAKE: u32 = 16;

/// Per-direction readiness from one `poll()` wake.
// No `PartialEq`/`Eq`: `Fault` carries `std::io::Error` which is not comparable.
#[derive(Debug)]
pub enum Readiness {
    /// One or both directions ready (no fault). At least one bool is true.
    Ready {
        /// The transport is readable this wake.
        readable: bool,
        /// The transport is writable this wake.
        writable: bool,
    },
    /// No direction ready and no fault — timeout or spurious wake.
    TimedOut,
    /// Socket fault (`POLLERR`/`POLLHUP`/`POLLNVAL` or a poll error) — treat as dead.
    Fault(io::Error),
}

impl Readiness {
    /// Inbound data can be read this wake. `false` for `TimedOut`/`Fault`.
    pub fn readable(&self) -> bool {
        matches!(self, Readiness::Ready { readable: true, .. })
    }

    /// The transport has TX room this wake. `false` for `TimedOut`/`Fault`.
    pub fn writable(&self) -> bool {
        matches!(self, Readiness::Ready { writable: true, .. })
    }
}

/// Classify one wake from the bits a platform's `poll` reported.
///
/// `fault` is `Some(revents)` when the wake carried any of
/// `POLLERR`/`POLLHUP`/`POLLNVAL`; it wins over the direction bits, because a
/// half-dead socket that also reports readable must not be treated as usable.
/// The raw mask travels into the error text: which fault bit fired is the whole
/// diagnostic value of the event.
///
/// Every [`NetPoll`] impl ends here, so the classification rule has one owner
/// across platforms whose mask constants differ.
pub fn classify_wake(readable: bool, writable: bool, fault: Option<u32>) -> Readiness {
    if let Some(revents) = fault {
        return Readiness::Fault(io::Error::other(format!(
            "poll reported socket fault (revents={revents:#x})"
        )));
    }
    if readable || writable {
        Readiness::Ready { readable, writable }
    } else {
        Readiness::TimedOut
    }
}

/// One platform's `poll()` shim.
///
/// The only platform-specific part of the event loop's wait: a caller hands an
/// fd, the directions it cares about, and a timeout, and gets back per-direction
/// readiness. Implementations map [`PollInterest`] to their own event mask and
/// finish through [`classify_wake`].
pub trait NetPoll {
    /// Wait on `fd` for `interest`, up to `timeout`.
    ///
    /// `Err` is reserved for the poll call itself failing (an errno); a socket
    /// fault reported *by* a successful poll is `Ok(Readiness::Fault)`. A zero
    /// timeout is a non-blocking check, not "wait forever".
    fn poll_readiness(
        &self,
        fd: RawFd,
        interest: PollInterest,
        timeout: Duration,
    ) -> io::Result<Readiness>;

    /// [`poll_readiness`](NetPoll::poll_readiness) with the errno folded into
    /// [`Readiness::Fault`] — the form the event loop wants, since it treats both
    /// as the same dead-socket outcome. Provided; implementations override
    /// `poll_readiness` only.
    fn readiness(&self, fd: RawFd, interest: PollInterest, timeout: Duration) -> Readiness {
        match self.poll_readiness(fd, interest, timeout) {
            Ok(readiness) => readiness,
            Err(e) => Readiness::Fault(e),
        }
    }
}

/// Wait for a non-blocking transport's fd to become writable, bounded by `deadline`.
///
/// Maps the wake to [`Writable`]: writable → `Ready`, timeout or a wake in
/// neither direction → `TimedOut`, fault → `Fault`. A non-positive remaining
/// budget means the deadline already passed, which becomes a zero timeout —
/// correct, since a poll that finds nothing then reports `TimedOut`.
/// `poll` is generic (and `?Sized`, so a `&dyn NetPoll` still works) because the
/// callers that pass a zero-sized platform shim get a thin reference — one
/// argument word on the Xtensa windowed ABI where a fat `&dyn` costs two.
pub fn poll_writable<P: NetPoll + ?Sized>(poll: &P, fd: RawFd, deadline: Instant) -> Writable {
    let remaining = deadline.saturating_duration_since(Instant::now());
    match poll.readiness(fd, PollInterest::WRITE, remaining) {
        Readiness::Fault(e) => Writable::Fault(e),
        Readiness::Ready { writable: true, .. } => {
            // Enforce the deadline as a hard bound even when poll claims writable,
            // to prevent a busy-spin if writability fires while the transport still
            // refuses bytes.
            if Instant::now() >= deadline {
                Writable::TimedOut
            } else {
                Writable::Ready
            }
        }
        Readiness::Ready { .. } | Readiness::TimedOut => Writable::TimedOut,
    }
}

/// Compute the timeout for the event loop's readiness wait from pending
/// housekeeping deadlines.
///
/// Returns `min(time-to-earliest-deadline, IDLE_TICK)`, clamped to
/// `[1 ms, IDLE_TICK]`. The 1 ms floor prevents a busy-spin when a deadline is
/// already due (the loop's housekeeping step — not the poll — clears due
/// deadlines). The `IDLE_TICK` ceiling ensures channel-delivered messages are
/// picked up within one tick.
///
/// Pure function: `now` and deadlines are passed in for deterministic unit testing.
pub fn timeout_to_next_deadline(
    now: Instant,
    deadlines: impl IntoIterator<Item = Instant>,
) -> Duration {
    // Fold from `now + IDLE_TICK` so IDLE_TICK acts as both the default and the cap.
    let target = deadlines
        .into_iter()
        .fold(now + IDLE_TICK, |earliest, d| earliest.min(d));

    let remaining = target.saturating_duration_since(now);
    // Whole milliseconds, because that is the resolution `poll()` takes: a
    // sub-millisecond remainder would truncate to a 0 timeout, i.e. a spin.
    let ms = remaining.as_millis().clamp(1, IDLE_TICK.as_millis());
    Duration::from_millis(ms as u64)
}

/// Poll timeout for the event loop's readiness wait, folding in whether the loop
/// already holds actionable work.
///
/// `work_pending` — a direction's pump stopped at its per-wake cap, or the outbound
/// selector still has a buildable frame — yields zero: re-poll immediately rather
/// than sleep on the tick while work remains (the loop's drain-until-blocked
/// invariant). Otherwise fall back to the `[1 ms, IDLE_TICK]` clamp against the
/// optional write deadline (the caught-up / blocked-on-writability case).
pub fn poll_timeout(now: Instant, deadline: Option<Instant>, work_pending: bool) -> Duration {
    if work_pending {
        Duration::ZERO
    } else {
        timeout_to_next_deadline(now, deadline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A [`NetPoll`] that answers from a script rather than a socket, recording
    /// what it was asked for.
    struct FakePoll {
        answer: std::cell::RefCell<Option<io::Result<Readiness>>>,
        seen: std::cell::Cell<Option<(RawFd, PollInterest, Duration)>>,
    }

    impl FakePoll {
        fn new(answer: io::Result<Readiness>) -> Self {
            Self {
                answer: std::cell::RefCell::new(Some(answer)),
                seen: std::cell::Cell::new(None),
            }
        }
    }

    impl NetPoll for FakePoll {
        fn poll_readiness(
            &self,
            fd: RawFd,
            interest: PollInterest,
            timeout: Duration,
        ) -> io::Result<Readiness> {
            self.seen.set(Some((fd, interest, timeout)));
            self.answer
                .borrow_mut()
                .take()
                .expect("fake polled more than once")
        }
    }

    // ── timeout_to_next_deadline ──────────────────────────────────────────

    #[test]
    fn timeout_caps_at_idle_tick_when_no_deadline_pending() {
        let now = Instant::now();
        assert_eq!(timeout_to_next_deadline(now, std::iter::empty()), IDLE_TICK);
    }

    #[test]
    fn timeout_caps_at_idle_tick_even_for_a_far_future_deadline() {
        let now = Instant::now();
        let far = now + Duration::from_millis(1000);
        assert_eq!(
            timeout_to_next_deadline(now, [far]),
            IDLE_TICK,
            "a far-future deadline must not extend the wait past IDLE_TICK"
        );
    }

    #[test]
    fn timeout_takes_the_minimum_when_a_deadline_is_sooner_than_a_tick() {
        let now = Instant::now();
        let soon = now + Duration::from_millis(3);
        let mid = now + Duration::from_millis(7);
        let far = now + Duration::from_millis(500);
        assert_eq!(
            timeout_to_next_deadline(now, [far, soon, mid]),
            Duration::from_millis(3),
            "earliest deadline wins when it's under IDLE_TICK"
        );
    }

    /// An already-elapsed deadline floors at 1 ms (not zero) to avoid busy-spinning.
    #[test]
    fn timeout_floors_an_already_due_deadline_at_one_ms_not_zero() {
        let now = Instant::now();
        let overdue = now - Duration::from_millis(50);
        assert_eq!(
            timeout_to_next_deadline(now, [overdue]),
            Duration::from_millis(1),
            "elapsed deadline must floor at 1 ms, not 0"
        );
    }

    /// A sub-millisecond wait also floors at 1 ms: truncating to zero would turn
    /// the wait into a spin.
    #[test]
    fn timeout_floors_a_sub_millisecond_deadline_at_one_ms() {
        let now = Instant::now();
        let soon = now + Duration::from_micros(200);
        assert_eq!(
            timeout_to_next_deadline(now, [soon]),
            Duration::from_millis(1)
        );
    }

    // ── poll_timeout ──────────────────────────────────────────────────────

    /// Pending work forces an immediate re-poll regardless of any deadline.
    #[test]
    fn poll_timeout_is_zero_when_work_pending() {
        let now = Instant::now();
        assert_eq!(
            poll_timeout(now, None, true),
            Duration::ZERO,
            "no deadline, work pending → 0"
        );
        assert_eq!(
            poll_timeout(now, Some(now + Duration::from_millis(500)), true),
            Duration::ZERO,
            "a far deadline does not extend the wait when work is pending"
        );
    }

    /// With no pending work, `poll_timeout` is the `[1 ms, IDLE_TICK]` clamp.
    #[test]
    fn poll_timeout_falls_back_to_clamp_when_no_work() {
        let now = Instant::now();
        assert_eq!(
            poll_timeout(now, None, false),
            IDLE_TICK,
            "no work, no deadline → the IDLE_TICK ceiling"
        );
        assert_eq!(
            poll_timeout(now, Some(now + Duration::from_millis(3)), false),
            Duration::from_millis(3),
            "no work, a sub-tick deadline → the deadline wins"
        );
        assert_eq!(
            poll_timeout(now, Some(now - Duration::from_millis(10)), false),
            Duration::from_millis(1),
            "no work, an already-due deadline floors at 1 ms — 0 is reserved for work_pending"
        );
    }

    // ── Readiness / classify_wake ─────────────────────────────────────────

    #[test]
    fn readiness_ready_reports_each_direction_bit_independently() {
        let r_in = Readiness::Ready {
            readable: true,
            writable: false,
        };
        assert!(r_in.readable(), "readable-only Ready is readable");
        assert!(!r_in.writable(), "readable-only Ready is not writable");

        let r_out = Readiness::Ready {
            readable: false,
            writable: true,
        };
        assert!(!r_out.readable(), "writable-only Ready is not readable");
        assert!(r_out.writable(), "writable-only Ready is writable");

        let r_both = Readiness::Ready {
            readable: true,
            writable: true,
        };
        assert!(
            r_both.readable() && r_both.writable(),
            "both bits set → both true"
        );
    }

    #[test]
    fn readiness_timed_out_is_neither_readable_nor_writable() {
        let r = Readiness::TimedOut;
        assert!(!r.readable(), "TimedOut must not report readable");
        assert!(!r.writable(), "TimedOut must not report writable");
    }

    #[test]
    fn readiness_fault_is_neither_readable_nor_writable() {
        let r = Readiness::Fault(io::Error::other("dead socket"));
        assert!(!r.readable(), "Fault must not report readable");
        assert!(!r.writable(), "Fault must not report writable");
    }

    #[test]
    fn classify_wake_maps_direction_bits_and_the_empty_wake() {
        assert!(matches!(
            classify_wake(true, false, None),
            Readiness::Ready {
                readable: true,
                writable: false
            }
        ));
        assert!(matches!(
            classify_wake(false, true, None),
            Readiness::Ready {
                readable: false,
                writable: true
            }
        ));
        assert!(matches!(
            classify_wake(true, true, None),
            Readiness::Ready {
                readable: true,
                writable: true
            }
        ));
        assert!(matches!(
            classify_wake(false, false, None),
            Readiness::TimedOut
        ));
    }

    /// A fault wins over the direction bits, and the raw mask reaches the message.
    #[test]
    fn classify_wake_faults_even_when_a_direction_is_ready() {
        let Readiness::Fault(e) = classify_wake(true, true, Some(0x0018)) else {
            panic!("fault bits must classify as Fault regardless of direction bits");
        };
        assert!(e.to_string().contains("0x18"), "{e}");
    }

    // ── NetPoll::readiness ────────────────────────────────────────────────

    /// A poll errno and a poll-reported fault are the same outcome to the loop,
    /// so the adapter collapses them.
    #[test]
    fn readiness_adapter_folds_an_errno_into_fault() {
        let poll = FakePoll::new(Err(io::Error::other("poll exploded")));
        let r = poll.readiness(7, PollInterest::BOTH, Duration::from_millis(5));
        let Readiness::Fault(e) = r else {
            panic!("an errno must arrive as Fault");
        };
        assert!(e.to_string().contains("poll exploded"), "{e}");
    }

    #[test]
    fn readiness_adapter_passes_a_successful_wake_through_unchanged() {
        let poll = FakePoll::new(Ok(Readiness::Ready {
            readable: false,
            writable: true,
        }));
        let r = poll.readiness(9, PollInterest::WRITE, Duration::from_millis(2));
        assert!(r.writable() && !r.readable());
        assert_eq!(
            poll.seen.get(),
            Some((9, PollInterest::WRITE, Duration::from_millis(2))),
            "fd, interest and timeout reach the shim verbatim"
        );
    }

    // ── poll_writable ─────────────────────────────────────────────────────

    #[test]
    fn poll_writable_asks_only_for_writability_and_reports_ready() {
        let poll = FakePoll::new(Ok(Readiness::Ready {
            readable: false,
            writable: true,
        }));
        let deadline = Instant::now() + Duration::from_millis(500);
        assert!(matches!(poll_writable(&poll, 3, deadline), Writable::Ready));
        let (fd, interest, timeout) = poll.seen.get().expect("polled");
        assert_eq!((fd, interest), (3, PollInterest::WRITE));
        assert!(
            timeout <= Duration::from_millis(500),
            "the wait is bounded by the remaining budget, got {timeout:?}"
        );
    }

    /// Writability reported after the deadline has already passed is still a
    /// timeout — the deadline is a hard bound, not a hint.
    #[test]
    fn poll_writable_times_out_when_the_deadline_passed_despite_writability() {
        let poll = FakePoll::new(Ok(Readiness::Ready {
            readable: false,
            writable: true,
        }));
        let deadline = Instant::now() - Duration::from_millis(1);
        assert!(matches!(
            poll_writable(&poll, 3, deadline),
            Writable::TimedOut
        ));
        let (_, _, timeout) = poll.seen.get().expect("polled");
        assert_eq!(timeout, Duration::ZERO, "an expired budget polls at 0");
    }

    /// A readable-only wake makes no write progress possible, so it is a timeout
    /// rather than a spurious `Ready`.
    #[test]
    fn poll_writable_treats_a_readable_only_wake_as_timed_out() {
        let poll = FakePoll::new(Ok(Readiness::Ready {
            readable: true,
            writable: false,
        }));
        let deadline = Instant::now() + Duration::from_millis(500);
        assert!(matches!(
            poll_writable(&poll, 3, deadline),
            Writable::TimedOut
        ));
    }

    #[test]
    fn poll_writable_reports_a_timeout_and_a_fault_distinctly() {
        let deadline = Instant::now() + Duration::from_millis(500);

        let timed_out = FakePoll::new(Ok(Readiness::TimedOut));
        assert!(matches!(
            poll_writable(&timed_out, 3, deadline),
            Writable::TimedOut
        ));

        let faulted = FakePoll::new(Ok(Readiness::Fault(io::Error::other("hup"))));
        let Writable::Fault(e) = poll_writable(&faulted, 3, deadline) else {
            panic!("a socket fault must not be reported as a timeout");
        };
        assert!(e.to_string().contains("hup"), "{e}");
    }
}
