//! ESP32 `poll()` shim behind the shared streamer's poll seam.
//!
//! Readiness classification, the pacing tick, the per-wake step budgets, the
//! timeout clamp and the bounded writability wait all live in
//! `pod_streamer::netpoll`. What cannot be shared is the syscall itself and the
//! esp-idf event-mask constants, so that is all this module holds: `poll_one` (a
//! raw-mask wrapper the network self-tests also use) and the [`NetPoll`] impl the
//! streamer threads inject.

use pod_streamer::link::PollInterest;
use pod_streamer::netpoll::{NetPoll, Readiness, classify_wake};

/// Issue one `poll()` on a single fd, returning the raw `revents` bitmask.
/// A timeout (rc == 0) yields `revents == 0`.
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

/// The device's poll shim: esp-idf's `poll()` over the lwIP socket stack.
///
/// Zero-sized — every wake's state travels in the arguments — so the streamer
/// threads can name it inline wherever the shared engine wants a `&dyn NetPoll`.
pub(crate) struct EspPoll;

impl NetPoll for EspPoll {
    fn poll_readiness(
        &self,
        fd: std::os::fd::RawFd,
        interest: PollInterest,
        timeout: std::time::Duration,
    ) -> std::io::Result<Readiness> {
        use esp_idf_svc::sys::{POLLERR, POLLHUP, POLLIN, POLLNVAL, POLLOUT};

        let mut events = 0;
        if interest.read {
            events |= POLLIN;
        }
        if interest.write {
            events |= POLLOUT;
        }
        // `poll` takes whole milliseconds as a `c_int`; the clamp only guards the
        // cast, since every caller's budget is far below `c_int::MAX` ms.
        let timeout_ms =
            timeout.as_millis().min(std::os::raw::c_int::MAX as u128) as std::os::raw::c_int;

        let revents = poll_one(fd, events, timeout_ms)?;
        let fault = (revents & (POLLERR | POLLHUP | POLLNVAL) != 0).then_some(revents);
        Ok(classify_wake(
            revents & POLLIN != 0,
            revents & POLLOUT != 0,
            fault,
        ))
    }
}
