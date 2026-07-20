//! Socket poll-readiness helpers for the streamer event loop.
//!
//! Non-blocking `poll()` wrappers (`poll_writable`, `poll_one`, `poll_readiness`)
//! plus the idle-tick pacing constant and the `timeout_to_next_deadline` clamp.
//! Move-only extraction from `main.rs`; see `design.md` §2.1.

// Host view: these items exist for the tests and for the device-gated call sites.
#![cfg_attr(not(target_os = "espidf"), allow(dead_code))]

#[cfg(target_os = "espidf")]
use audio_pipeline::stream_send::Writable;

/// Streamer idle/pacing tick period. Single source of truth for in-segment pacing
/// and future idle-loop recv_timeout.
pub(crate) const IDLE_TICK: std::time::Duration = std::time::Duration::from_millis(10);

/// Maximum `drain_inbound` calls one inbound pump performs per poll wake before
/// yielding for fairness. A pump that stops here re-polls with timeout 0, so
/// throughput is not sacrificed; the cap only bounds per-wake work under a flood.
#[cfg(target_os = "espidf")]
pub(crate) const INBOUND_STEPS_PER_WAKE: u32 = 8;

/// Maximum completed outbound frames one wake writes before yielding for fairness.
/// Dwarfs the ~50 frames/s production rate, so the ring's overrun deadline is met
/// comfortably even when inbound is serviced first.
#[cfg(target_os = "espidf")]
pub(crate) const OUTBOUND_FRAMES_PER_WAKE: u32 = 16;

/// Wait (via `poll(POLLOUT)`) for a non-blocking socket fd to become writable,
/// bounded by `deadline`.
///
/// Maps `poll` return codes to [`Writable`]: `POLLOUT` → `Ready`, timeout → `TimedOut`,
/// error/`POLLERR`/`POLLHUP`/`POLLNVAL` → `Fault`.
#[cfg(target_os = "espidf")]
pub(crate) fn poll_writable(fd: std::os::fd::RawFd, deadline: std::time::Instant) -> Writable {
    use esp_idf_svc::sys::{poll, pollfd, POLLERR, POLLHUP, POLLNVAL, POLLOUT};

    // Remaining budget toward the WRITE_TIMEOUT_MS deadline.  `poll` takes the
    // timeout in milliseconds as a c_int; a non-positive remaining budget means
    // the deadline already passed, which `poll` treats as a non-blocking poll
    // (timeout 0) — correct: it will return TimedOut if not already writable.
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    let timeout_ms =
        remaining.as_millis().min(std::os::raw::c_int::MAX as u128) as std::os::raw::c_int;

    let mut pfd = pollfd {
        fd,
        events: POLLOUT as std::os::raw::c_short,
        revents: 0,
    };
    // SAFETY: `pfd` is a single valid, initialized `pollfd` and we pass `nfds = 1`
    // to match; `poll` only reads `fd`/`events` and writes `revents`.
    let rc = unsafe { poll(&mut pfd, 1, timeout_ms) };
    if rc < 0 {
        return Writable::Fault(std::io::Error::last_os_error());
    }
    if rc == 0 {
        return Writable::TimedOut;
    }
    let revents = pfd.revents as u32;
    if revents & (POLLERR | POLLHUP | POLLNVAL) != 0 {
        return Writable::Fault(std::io::Error::other("poll(POLLOUT) reported socket fault"));
    }
    if revents & POLLOUT != 0 {
        // Enforce deadline as hard bound even if poll claims writable, to prevent
        // a busy-spin if POLLOUT fires while the socket still refuses bytes.
        if std::time::Instant::now() >= deadline {
            return Writable::TimedOut;
        }
        return Writable::Ready;
    }
    // Neither POLLOUT nor a fault bit — treat as not-yet-writable.
    Writable::TimedOut
}

/// Issue one `poll()` on a single fd, returning the raw `revents` bitmask.
/// A timeout (rc == 0) yields `revents == 0`.
#[cfg(target_os = "espidf")]
pub(crate) fn poll_one(
    fd: std::os::fd::RawFd,
    events: u32,
    timeout_ms: std::os::raw::c_int,
) -> std::io::Result<u32> {
    use esp_idf_svc::sys::{poll, pollfd};

    let mut pfd = pollfd {
        fd,
        events: events as std::os::raw::c_short,
        revents: 0,
    };
    // SAFETY: `pfd` is a single valid, initialized `pollfd` and we pass `nfds = 1` to match;
    // `poll` only reads `fd`/`events` and writes `revents`.
    let rc = unsafe { poll(&mut pfd, 1, timeout_ms) };
    if rc < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // rc == 0 → timeout, revents is 0; rc > 0 → revents carries the ready bits.
    Ok(pfd.revents as u32)
}

/// Per-direction readiness from one `poll()` wake.
// No `PartialEq`/`Eq`: `Fault` carries `std::io::Error` which is not comparable.
#[derive(Debug)]
pub(crate) enum Readiness {
    /// One or both directions ready (no fault). At least one bool is true.
    Ready { readable: bool, writable: bool },
    /// No direction ready and no fault — timeout or spurious wake.
    TimedOut,
    /// Socket fault (POLLERR/POLLHUP/POLLNVAL or poll errno) — treat as dead.
    Fault(std::io::Error),
}

impl Readiness {
    /// Inbound socket data can be read this wake (`POLLIN` was set). `false` for
    /// `TimedOut`/`Fault`.
    pub(crate) fn readable(&self) -> bool {
        matches!(self, Readiness::Ready { readable: true, .. })
    }

    /// The socket TX buffer has room this wake (`POLLOUT` was set). `false` for
    /// `TimedOut`/`Fault`.
    pub(crate) fn writable(&self) -> bool {
        matches!(self, Readiness::Ready { writable: true, .. })
    }
}

/// Wait on one non-blocking socket fd for per-direction readiness, up to `timeout_ms`.
///
/// Wraps [`poll_one`] with fault/timeout classification:
/// - Negative rc or fault bits (POLLERR/POLLHUP/POLLNVAL) → `Fault`.
/// - POLLIN/POLLOUT set → `Ready` with matching direction bools.
/// - Neither direction nor fault → `TimedOut`.
#[cfg(target_os = "espidf")]
pub(crate) fn poll_readiness(
    fd: std::os::fd::RawFd,
    events: u32,
    timeout_ms: std::os::raw::c_int,
) -> Readiness {
    use esp_idf_svc::sys::{POLLERR, POLLHUP, POLLIN, POLLNVAL, POLLOUT};

    let revents = match poll_one(fd, events, timeout_ms) {
        Ok(revents) => revents,
        Err(e) => return Readiness::Fault(e),
    };
    if revents & (POLLERR | POLLHUP | POLLNVAL) != 0 {
        return Readiness::Fault(std::io::Error::other(format!(
            "poll reported socket fault (revents={revents:#x})"
        )));
    }
    let readable = revents & POLLIN != 0;
    let writable = revents & POLLOUT != 0;
    if readable || writable {
        Readiness::Ready { readable, writable }
    } else {
        Readiness::TimedOut
    }
}

/// Compute `timeout_ms` for the event-loop's [`poll_readiness`] wait from pending
/// housekeeping deadlines.
///
/// Returns `min(time-to-earliest-deadline, IDLE_TICK)`, clamped to `[1, IDLE_TICK]` ms.
/// The 1 ms floor prevents a busy-spin when a deadline is already due (the loop's
/// housekeeping step — not the poll — clears due deadlines). The IDLE_TICK ceiling
/// ensures channel-delivered messages are picked up within one tick.
///
/// Pure function: `now` and deadlines are passed in for deterministic unit testing.
pub(crate) fn timeout_to_next_deadline(
    now: std::time::Instant,
    deadlines: impl IntoIterator<Item = std::time::Instant>,
) -> std::os::raw::c_int {
    // Fold from `now + IDLE_TICK` so IDLE_TICK acts as both the default and the cap.
    let target = deadlines
        .into_iter()
        .fold(now + IDLE_TICK, |earliest, d| earliest.min(d));

    let remaining = target.saturating_duration_since(now);

    // `as c_int` is safe: value is bounded by IDLE_TICK (10 ms), well within c_int::MAX.
    let idle_tick_ms = IDLE_TICK.as_millis() as std::os::raw::c_int;
    let ms = remaining.as_millis() as std::os::raw::c_int;
    ms.clamp(1, idle_tick_ms)
}

/// Poll timeout for the event loop's readiness wait, folding in whether the loop
/// already holds actionable work.
///
/// `work_pending` — a direction's pump stopped at its per-wake cap, or the outbound
/// selector still has a buildable frame — yields `0`: re-poll immediately rather than
/// sleep on the tick while work remains (the loop's drain-until-blocked invariant).
/// Otherwise fall back to the `[1, IDLE_TICK]` clamp against the optional write
/// deadline (the caught-up / blocked-on-POLLOUT case).
pub(crate) fn poll_timeout(
    now: std::time::Instant,
    deadline: Option<std::time::Instant>,
    work_pending: bool,
) -> std::os::raw::c_int {
    if work_pending {
        0
    } else {
        timeout_to_next_deadline(now, deadline)
    }
}

#[cfg(test)]
mod tests {
    // ── timeout_to_next_deadline ──────────────────────────────────────────

    #[test]
    fn timeout_caps_at_idle_tick_when_no_deadline_pending() {
        use super::{timeout_to_next_deadline, IDLE_TICK};
        use std::time::Instant;
        let now = Instant::now();
        let ms = timeout_to_next_deadline(now, std::iter::empty());
        assert_eq!(ms, IDLE_TICK.as_millis() as std::os::raw::c_int);
    }

    #[test]
    fn timeout_caps_at_idle_tick_even_for_a_far_future_deadline() {
        use super::{timeout_to_next_deadline, IDLE_TICK};
        use std::time::{Duration, Instant};
        let now = Instant::now();
        let far = now + Duration::from_millis(1000);
        let ms = timeout_to_next_deadline(now, [far]);
        assert_eq!(
            ms,
            IDLE_TICK.as_millis() as std::os::raw::c_int,
            "a far-future deadline must not extend the wait past IDLE_TICK"
        );
    }

    #[test]
    fn timeout_takes_the_minimum_when_a_deadline_is_sooner_than_a_tick() {
        use super::timeout_to_next_deadline;
        use std::time::{Duration, Instant};
        let now = Instant::now();
        let soon = now + Duration::from_millis(3);
        let mid = now + Duration::from_millis(7);
        let far = now + Duration::from_millis(500);
        let ms = timeout_to_next_deadline(now, [far, soon, mid]);
        assert_eq!(ms, 3, "earliest deadline wins when it's under IDLE_TICK");
    }

    /// An already-elapsed deadline floors at 1 ms (not 0) to avoid busy-spinning.
    #[test]
    fn timeout_floors_an_already_due_deadline_at_one_ms_not_zero() {
        use super::timeout_to_next_deadline;
        use std::time::{Duration, Instant};
        let now = Instant::now();
        let overdue = now - Duration::from_millis(50);
        let ms = timeout_to_next_deadline(now, [overdue]);
        assert_eq!(ms, 1, "elapsed deadline must floor at 1 ms, not 0");
    }

    // ── poll_timeout ──────────────────────────────────────────────────────

    /// Pending work forces an immediate re-poll (0) regardless of any deadline.
    #[test]
    fn poll_timeout_is_zero_when_work_pending() {
        use super::poll_timeout;
        use std::time::{Duration, Instant};
        let now = Instant::now();
        assert_eq!(
            poll_timeout(now, None, true),
            0,
            "no deadline, work pending → 0"
        );
        assert_eq!(
            poll_timeout(now, Some(now + Duration::from_millis(500)), true),
            0,
            "a far deadline does not extend the wait when work is pending"
        );
    }

    /// With no pending work, `poll_timeout` is the existing `[1, IDLE_TICK]` clamp.
    #[test]
    fn poll_timeout_falls_back_to_clamp_when_no_work() {
        use super::{poll_timeout, IDLE_TICK};
        use std::time::{Duration, Instant};
        let now = Instant::now();
        assert_eq!(
            poll_timeout(now, None, false),
            IDLE_TICK.as_millis() as std::os::raw::c_int,
            "no work, no deadline → the IDLE_TICK ceiling"
        );
        assert_eq!(
            poll_timeout(now, Some(now + Duration::from_millis(3)), false),
            3,
            "no work, a sub-tick deadline → the deadline wins"
        );
        assert_eq!(
            poll_timeout(now, Some(now - Duration::from_millis(10)), false),
            1,
            "no work, an already-due deadline floors at 1 — 0 is reserved for work_pending"
        );
    }

    // ── Readiness ────────────────────────────────────────────────────────

    #[test]
    fn readiness_ready_reports_each_direction_bit_independently() {
        use super::Readiness;
        let r_in = Readiness::Ready {
            readable: true,
            writable: false,
        };
        assert!(r_in.readable(), "POLLIN-only Ready is readable");
        assert!(!r_in.writable(), "POLLIN-only Ready is not writable");

        let r_out = Readiness::Ready {
            readable: false,
            writable: true,
        };
        assert!(!r_out.readable(), "POLLOUT-only Ready is not readable");
        assert!(r_out.writable(), "POLLOUT-only Ready is writable");

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
        use super::Readiness;
        let r = Readiness::TimedOut;
        assert!(!r.readable(), "TimedOut must not report readable");
        assert!(!r.writable(), "TimedOut must not report writable");
    }

    #[test]
    fn readiness_fault_is_neither_readable_nor_writable() {
        use super::Readiness;
        let r = Readiness::Fault(std::io::Error::other("dead socket"));
        assert!(!r.readable(), "Fault must not report readable");
        assert!(!r.writable(), "Fault must not report writable");
    }
}
