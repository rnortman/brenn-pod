//! Host-testable streamer send-loop core (partial-write-fix design §2.3a).
//!
//! This module holds the **pure**, transport-agnostic part of the device
//! streamer's backpressure-aware frame send: the `written`-tracking write loop,
//! the per-wait/per-frame budget bookkeeping, and the aligned-vs-mid-tail-dead
//! classification.  It is parameterized over a `wait_writable` closure and an
//! injectable monotonic clock, so the classification and budget-reset logic is
//! unit-testable off-target without a real socket *and* without a real clock.
//!
//! The two socket-bound pieces stay in the device crate:
//! - `poll_writable` — the `poll(POLLOUT)` waiter (ESP-only `esp_idf_svc::sys`);
//! - `send_frame_bp` / `send_frame_bp_counted` — the `TcpStream` wrappers that
//!   build the `|deadline| poll_writable(fd, deadline)` closure and call
//!   [`write_frame_classified`].
//!
//! The module is gated `#[cfg(feature = "std")]`: it uses `std::io`, `std::time`,
//! and (transitively) socket I/O via the `dyn std::io::Write` it drives.  The
//! device crate depends on `audio-pipeline` with the `std` feature enabled so the
//! production hot path links against these items.

/// TCP write timeout for the audio streamer (ms).
///
/// Deliberately under the 1 s ring slack so write-timeout detection leaves
/// reconnect headroom before the ring laps the cursor (design §2.3, §3
/// half-open-socket edge case).
///
/// After the partial-write fix this is the *per-wait* budget: each `poll(POLLOUT)`
/// wait gets the full `WRITE_TIMEOUT_MS`, and it is **reset on every forward
/// progress** (`Ok(m>0)`).  A peer that grants any byte inside each window keeps
/// the frame alive; only a wait that elapses with zero progress is a stall
/// (partial-write-fix design §2.1).
pub const WRITE_TIMEOUT_MS: u64 = 750;

/// Absolute per-frame wall-clock ceiling for a single `write_frame_classified`
/// call (ms).
///
/// The per-wait `WRITE_TIMEOUT_MS` budget resets on forward progress, so a
/// slow-but-progressing peer could otherwise keep a frame in the write loop
/// indefinitely.  This ceiling caps the *total* time any one frame can spend in
/// the loop, independently of the per-wait resets, so every send site stays
/// bounded even though the overrun guard gates only the AudioFrame drain loop
/// (partial-write-fix design §2.1).  Set to 1.0 s, at or under the ring slack
/// (`RING_CAPACITY_SAMPLES` − the pre-roll backlog: 32000 − 16000 product
/// pre-roll = 16000 samples = 1.0 s at 16 kHz; a smaller pre-roll only widens
/// the slack), so it never fires before the AudioFrame overrun path would.
pub const FRAME_WALL_CLOCK_MAX_MS: u64 = 1000;

/// Outcome of a backpressure-aware frame send (design §3.2/§4; partial-write-fix
/// design §2.2).
///
/// A partial write is *resumable* on the same TCP socket — the write loop finishes
/// the frame across one or more `poll(POLLOUT)` waits as the peer drains — so there
/// is no normal-operation "partial write desynced the receiver" outcome.  The only
/// non-`Sent`, non-`Err` outcome is `BackpressureAligned`, which by construction can
/// only be reached at a frame boundary (`written == 0`).  The peer-dead-mid-tail
/// case (a prefix was accepted, then no progress for a full budget / the per-frame
/// ceiling fired with `written > 0`) folds into a fatal `Err` (drop the socket and
/// reconnect — the unsent tail is undeliverable so the socket is unsafe to reuse).
#[derive(Debug, PartialEq, Eq)]
pub enum SendOutcome {
    /// The whole frame reached the kernel send buffer.  A frame that took one or
    /// more partial writes and resumed to completion is also `Sent`.
    Sent,
    /// A full no-progress per-wait budget (or the per-frame wall-clock ceiling)
    /// elapsed before *any* byte of this frame was written.  The byte stream is
    /// still frame-aligned at the frame boundary; the segment is dropped but the
    /// socket may be reused (partial-write-fix design §2.2, `written == 0`).
    BackpressureAligned,
}

/// Result of waiting for a non-blocking socket to become writable (design §3.1).
///
/// The real implementation drives this via `poll(POLLOUT)` against a
/// `WRITE_TIMEOUT_MS` deadline; the write-loop core
/// ([`write_frame_classified`]) is parameterized over this so its
/// classification logic is unit-testable off-target without a real socket.
// No `PartialEq`/`Eq`: the `Fault` variant carries an `std::io::Error`, which is
// not comparable.  Tests construct `Writable` to pass *in* to the waiter; they
// never compare two `Writable` values.
#[derive(Debug)]
pub enum Writable {
    /// The socket became writable within the remaining budget — resume writing.
    Ready,
    /// The write budget was exhausted before the socket became writable.
    TimedOut,
    /// A genuine socket fault occurred while waiting (poll error /
    /// `POLLERR`/`POLLHUP`/`POLLNVAL`) — treat as a dead socket.
    Fault(std::io::Error),
}

/// Encode `frame` into `buf`, then write the whole frame to `writer` with an
/// explicit `write` loop, calling `wait_writable` to wait out transient
/// backpressure (design §3.2).
///
/// This is the off-target-testable core of `send_frame_bp`: it owns the
/// `written`-tracking write loop and the aligned-vs-desynced classification but
/// delegates the actual writability wait to `wait_writable`, which the
/// production path backs with `poll(POLLOUT)` (via the device crate's
/// `poll_writable`) and tests back with a deterministic stub.
///
/// `wait_writable` receives the loop-owned per-wait `deadline` (an `Instant`) and
/// returns a [`Writable`].  The *loop* — not the closure — owns the deadline so it
/// can reset it on forward progress (the closure cannot see loop progress); the
/// production closure is `|deadline| poll_writable(fd, deadline)` (partial-write-fix
/// design §2.2a).
///
/// Classification (partial-write-fix design §2.2):
/// - whole frame written (possibly across resumed partial writes) → `Sent`;
/// - a full no-progress per-wait budget (`Writable::TimedOut` with no byte of
///   progress since the previous wait) or the per-frame wall-clock ceiling
///   (`FRAME_WALL_CLOCK_MAX_MS`) elapsing:
///     - with `written == 0` → `BackpressureAligned` (frame-aligned, keep socket);
///     - with `written > 0` → fatal `Err` (peer dead mid-tail: the unsent tail is
///       undeliverable, so the socket is unsafe to reuse — drop and reconnect);
/// - `Writable::Fault` or any non-`WouldBlock`/`TimedOut` write error → `Err`.
///
/// Returns the [`SendOutcome`] plus a **resume-cycle count** — the number of genuine
/// *resumes*: each time a writability wait (`poll(POLLOUT)`) completes and finds `written`
/// advanced past the previous counted resume.  It is counted when a wait returns
/// (`Ready`/`Progressed`), NOT on the bare partial `write`: a partial write immediately
/// followed by another accepting write never went through a wait, so it is not a resume —
/// counting it would let two back-to-back accepting writes report `resume_cycles=1` and
/// make the HIL adversary-A "resumed via poll(POLLOUT)" probe pass vacuously
/// (error-handling review errhandling-1).  The high-water-mark key still prevents a
/// poll/write-readiness spin (a `Ready` that produces no advance) from inflating the count
/// (correctness review).  `Sent` alone does not prove resumability (a frame that fit
/// immediately is also `Sent`), so the count is the measured fact the HIL self-test asserts
/// on (partial-write-fix design §4 "resume signal").  A fatal `Err` for the dead-mid-tail
/// case is reported with `resume_cycles ≥ 1` so the caller can label it distinctly from a
/// `written == 0` aligned outcome.
pub fn write_frame_classified(
    writer: &mut dyn std::io::Write,
    frame: &crate::wire::StreamFrame,
    buf: &mut [u8],
    wait_writable: impl FnMut(std::time::Instant) -> Writable,
) -> (std::io::Result<SendOutcome>, u32) {
    // The production caller takes the per-frame wall-clock ceiling at frame entry and
    // reads time from the real monotonic clock.  The ceiling-and-clock-injecting
    // `write_frame_classified_at` is the testable seam: a test can pass a ceiling already
    // in the past to drive the ceiling exit deterministically (test review test-1), or a
    // hand-advanced fake clock to prove the per-wait budget re-arms on every
    // forward-progress step with zero hardware coupling (partial-write-fix §2.3/§4).
    let frame_ceiling =
        std::time::Instant::now() + std::time::Duration::from_millis(FRAME_WALL_CLOCK_MAX_MS);
    write_frame_classified_at(
        writer,
        frame,
        buf,
        wait_writable,
        frame_ceiling,
        std::time::Instant::now,
    )
}

/// Ceiling-and-clock-injecting core of [`write_frame_classified`]: identical behaviour
/// but takes the absolute per-frame `frame_ceiling` instant *and* a monotonic clock
/// source `now` explicitly so unit tests can drive both bounds deterministically.  A
/// ceiling already in the past drives the `FRAME_WALL_CLOCK_MAX_MS` give-up path without
/// sleeping (test review test-1); a hand-advanced fake `now` proves the per-wait budget
/// re-arms on *every* forward-progress step — the repeatability the HIL adversary A no
/// longer pins (partial-write-fix §2.3/§4).  All wall-clock reads in the loop (the
/// per-wait `deadline` computation and the ceiling check) go through `now`, so a test
/// that advances `now` by `WRITE_TIMEOUT_MS − ε` before each `Ready` and a partial write
/// between proves the frame completes without ever hitting the stall arm even though total
/// elapsed time far exceeds a single budget — and a regression that resets the budget
/// once but not again FAILs deterministically (the bug class the dropped HIL `≥2` floor
/// guarded).  The production wrapper passes `now + FRAME_WALL_CLOCK_MAX_MS` and the real
/// `Instant::now`.
pub fn write_frame_classified_at(
    writer: &mut dyn std::io::Write,
    frame: &crate::wire::StreamFrame,
    buf: &mut [u8],
    mut wait_writable: impl FnMut(std::time::Instant) -> Writable,
    frame_ceiling: std::time::Instant,
    now: impl Fn() -> std::time::Instant,
) -> (std::io::Result<SendOutcome>, u32) {
    use crate::wire::encode_frame;
    use std::time::Duration;

    let n = match encode_frame(frame, buf) {
        Ok(n) => n,
        Err(_) => return (Err(std::io::Error::other("encode_frame failed")), 0),
    };

    // Bound on consecutive `wait_writable() == Ready` cycles that make zero write
    // progress.  The production waiter (`poll_writable`) already enforces the
    // per-wait `WRITE_TIMEOUT_MS` deadline as a hard bound, but a platform where
    // `poll` keeps reporting POLLOUT while `write` keeps refusing bytes (a
    // poll/write readiness disagreement the design flags as unproven — §3.1/§5)
    // could otherwise busy-spin without yielding.  This iteration cap is a
    // belt-and-suspenders bound that terminates such a spin even if the waiter ever
    // returns `Ready` without honoring the deadline: it classifies a persistent
    // no-progress state the same as a budget timeout, routing into the same
    // aligned-vs-mid-tail-dead classification below.
    //
    // This cap guards the blocking `write_frame_classified` path only; the lifted
    // `FrameWriteState` + event-loop pump handles the same disagreement with
    // `SPIN_GUARD_THRESHOLD` + a backoff tick instead of termination.
    const MAX_ZERO_PROGRESS_READY: u32 = 1024;

    // `resume_cycles` counts genuine *resumes*: a writability wait that returned and
    // found `written` advanced past the last counted resume.  It is counted when a wait
    // completes (`Ready`/`Progressed`), NOT on the partial `write` itself: a partial
    // write that is immediately followed by another accepting write — with no
    // intervening `wait_writable` — never exercised `poll(POLLOUT)` and so is not a
    // resume.  Counting on the bare partial write would let two back-to-back accepting
    // writes report `resume_cycles=1` and make the HIL adversary-A "resumed via
    // poll(POLLOUT)" probe pass vacuously (error-handling review errhandling-1).  The
    // high-water-mark key (`written_at_resume`) still ensures a poll/write spin (a wait
    // that produces no advance) cannot inflate the count (correctness review).
    let mut resume_cycles: u32 = 0;
    let mut written = 0usize;
    // High-water mark of `written` at the last counted resume cycle: a wait only counts a
    // new resume once `written` has advanced past this, so a poll/write spin (a wait with
    // no advance) cannot inflate `resume_cycles` (correctness review).
    let mut written_at_resume = 0usize;
    let mut zero_progress_ready: u32 = 0;
    // Per-wait budget (loop-owned, resettable on progress); the absolute per-frame
    // wall-clock ceiling (`frame_ceiling`, NOT reset by progress) is injected by the
    // caller — partial-write-fix §2.1.
    // Per-wait deadline is clamped to never overshoot `frame_ceiling`: a `poll`
    // blocks until its deadline with no mid-wait ceiling re-check, so an unclamped
    // per-wait budget could overshoot the per-frame ceiling by a full
    // `WRITE_TIMEOUT_MS` (the loop-top ceiling check fires only between waits).
    // Clamping makes the per-frame total bound tight (≤ FRAME_WALL_CLOCK_MAX_MS, not
    // + WRITE_TIMEOUT_MS) — the production guarantee the ceiling exists to give the
    // five non-AudioFrame send sites, and what keeps the HIL adversary-C give-up time
    // inside its asserted window (correctness review).
    let mut deadline = (now() + Duration::from_millis(WRITE_TIMEOUT_MS)).min(frame_ceiling);
    // Bytes written as of the previous `Writable::TimedOut`: lets a `TimedOut` that
    // followed forward progress *continue* (reset + retry) instead of terminating,
    // distinguishing a slow-but-alive peer from a dead one (partial-write-fix §2.2a).
    let mut written_at_last_wait = 0usize;

    // Terminal no-progress classification (shared by the ceiling, the
    // no-progress-budget, and the spin-cap paths): a frame-boundary stall keeps the
    // socket; a mid-tail stall is fatal (partial-write-fix design §2.2).
    let classify_no_progress = |written: usize| {
        if written == 0 {
            Ok(SendOutcome::BackpressureAligned)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "send_frame_bp: peer accepted a prefix then made no progress for a \
                 full budget (mid-tail dead) — socket undeliverable, dropping",
            ))
        }
    };

    while written < n {
        // Absolute per-frame ceiling: caps total time in the loop regardless of how
        // often the per-wait budget resets, so every send site stays bounded even
        // those with no overrun guard (partial-write-fix design §2.1).  Read through the
        // injected `now` so a fake clock can drive this exit deterministically.
        if now() >= frame_ceiling {
            return (classify_no_progress(written), resume_cycles);
        }
        // A `wait_writable` that returned `Ready` but produced no forward progress
        // is the spin-risk case; classify it the same as a no-progress budget once
        // it persists past the cap rather than re-issuing the same write forever.
        if zero_progress_ready >= MAX_ZERO_PROGRESS_READY {
            return (classify_no_progress(written), resume_cycles);
        }
        match writer.write(&buf[written..n]) {
            Ok(0) => {
                // The writer accepted no bytes without signalling an error.  On a
                // non-blocking socket this is a degenerate would-block; wait for
                // writability rather than spin.  Both no-byte arms fall through to the
                // one shared wait below.
            }
            Ok(m) => {
                written += m;
                zero_progress_ready = 0;
                // The resume-cycle count is taken when a writability wait completes (in
                // the shared wait below), not here on the bare partial write: a partial
                // write that is immediately followed by another accepting write never
                // went through `poll(POLLOUT)`, so it is not a resume (errhandling-1).
                if written < n {
                    // Forward progress → reset the per-wait budget (partial-write-fix
                    // §2.1): a peer that grants any byte keeps the budget fresh.  Only
                    // a wait that still follows needs the reset; the write that
                    // completes the frame exits the loop and never reads `deadline`
                    // again (efficiency review).  Clamped to the per-frame ceiling so a
                    // subsequent wait cannot overshoot it (correctness review).  Read
                    // through the injected `now` so a fake clock drives the reset.
                    deadline = (now() + Duration::from_millis(WRITE_TIMEOUT_MS)).min(frame_ceiling);
                }
                continue;
            }
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                // Transient backpressure: the kernel send buffer is full.  Wait for
                // the socket to become writable, then resume the same write loop
                // (never re-issue already-written bytes — that is what the `written`
                // cursor protects against, design §3.2).  Falls through to the shared
                // wait below.
            }
            Err(e) => {
                // Genuine socket fault (BrokenPipe, ConnectionReset, …).
                return (Err(e), resume_cycles);
            }
        }

        // Shared post-no-byte wait for the `Ok(0)` and `WouldBlock`/`TimedOut` arms —
        // both wait for writability then resume, and differ only in *why* no byte was
        // written, not in what to do next (quality review: keep one copy of the wait
        // policy).  A future editor adding a third no-byte arm must route it to this
        // fall-through, not paste a copy.
        //
        // NOTE on `resume_cycles` (error-handling review errhandling-1, correctness
        // review): a resume cycle is *a writability wait that completed and found
        // `written` advanced past the last counted resume* — i.e. a partial write whose
        // continuation genuinely went through `poll(POLLOUT)`.  It is counted here, when a
        // wait returns `Ready`/`Progressed`, NOT on the partial `write` itself: a partial
        // write immediately followed by another accepting write (no wait in between) never
        // exercised the poll path, so it must not count (errhandling-1 — otherwise the HIL
        // adversary-A "resumed via poll(POLLOUT)" probe passes vacuously).  The count is
        // keyed on the `written_at_resume` high-water mark so a poll/write-readiness spin
        // (a `Ready` that produces no advance — the `MAX_ZERO_PROGRESS_READY` failure
        // mode) cannot inflate it (correctness review): a wait that did not advance
        // `written` finds `written == written_at_resume` and is not counted.
        match wait_for_writable(
            &mut wait_writable,
            written,
            &mut written_at_last_wait,
            &mut deadline,
            frame_ceiling,
            &now,
        ) {
            // Both `Ready` and `Progressed` continue the write loop.  A wait that
            // returned with `written` advanced past the last counted resume is a
            // genuine resume cycle: a partial write was made and its continuation went
            // through this wait (errhandling-1).  Count it once per high-water advance.
            // A `Ready` wait advances the spin cap; a `Progressed` wait found forward
            // progress, so any prior zero-progress-`Ready` run is broken and the spin
            // cap resets (mirroring the reset on `Ok(m)` in the main loop) so
            // `zero_progress_ready` provably counts only *consecutive* no-progress
            // `Ready` cycles (quality-1).
            WaitStep::Ready => zero_progress_ready += 1,
            WaitStep::Progressed => zero_progress_ready = 0,
            WaitStep::Stalled => return (classify_no_progress(written), resume_cycles),
            WaitStep::Fault(e) => return (Err(e), resume_cycles),
        }
        // Resume counting is shared by both continuing arms (errhandling-1): a wait
        // that returned with `written` advanced past the last counted resume is a
        // genuine resume cycle. Count it once per high-water advance.
        if written < n && written > written_at_resume {
            resume_cycles += 1;
            written_at_resume = written;
        }
    }

    (Ok(SendOutcome::Sent), resume_cycles)
}

/// The result of one `wait_writable` cycle inside [`write_frame_classified`]
/// (partial-write-fix design §2.2a).
enum WaitStep {
    /// The socket became writable — resume the write.
    Ready,
    /// The wait timed out, but `written` advanced since the previous wait; the
    /// budget is reset and the write continues.
    Progressed,
    /// The wait timed out with no byte of progress for a full budget — terminal.
    Stalled,
    /// A genuine socket fault while waiting.
    Fault(std::io::Error),
}

/// Run one writability wait against `*deadline`, then interpret the result with the
/// progress-reset semantics of partial-write-fix design §2.2a:
/// - `Writable::Ready` → `WaitStep::Ready`;
/// - `Writable::TimedOut` with `written` advanced since the last wait → reset the
///   budget (`*deadline`, clamped to `frame_ceiling`), record the new high-water
///   mark, return `WaitStep::Progressed` (continue — a `TimedOut` is no longer
///   terminal when progress was made since the prior wait);
/// - `Writable::TimedOut` with no progress for a full budget → `WaitStep::Stalled`;
/// - `Writable::Fault` → `WaitStep::Fault`.
///
/// `frame_ceiling` clamps the reset budget so a subsequent wait cannot overshoot the
/// absolute per-frame ceiling (correctness review).
fn wait_for_writable(
    wait_writable: &mut impl FnMut(std::time::Instant) -> Writable,
    written: usize,
    written_at_last_wait: &mut usize,
    deadline: &mut std::time::Instant,
    frame_ceiling: std::time::Instant,
    now: &impl Fn() -> std::time::Instant,
) -> WaitStep {
    use std::time::Duration;
    match wait_writable(*deadline) {
        Writable::Ready => WaitStep::Ready,
        Writable::TimedOut => {
            if written > *written_at_last_wait {
                // Progressed since the last wait → not a stall: reset & retry.  The
                // reset budget is clamped to the per-frame ceiling so the next wait
                // cannot overshoot it (correctness review).  Read through the injected
                // `now` so a fake clock drives the reset.
                *written_at_last_wait = written;
                *deadline = (now() + Duration::from_millis(WRITE_TIMEOUT_MS)).min(frame_ceiling);
                WaitStep::Progressed
            } else {
                // No byte of progress for a full budget → terminal.
                WaitStep::Stalled
            }
        }
        Writable::Fault(e) => WaitStep::Fault(e),
    }
}

// ── Cross-iteration send-cursor state machine (design §2.4) ─────────────────────
//
// `write_frame_classified[_at]` above owns the whole write loop *and* the
// `poll(POLLOUT)` wait internally: it parks in `wait_writable` until the frame is
// `Sent`, the budget elapses, or the socket faults.  That is correct for today's
// blocking-per-frame caller, but the event-loop architecture (design §2.1) must drive
// the *same* `written`-cursor + two-tier-budget + classification logic **one
// `POLLOUT`-gated attempt at a time**, with the cursor and budget state persisting
// **across loop iterations** instead of inside one call (design §2.4).  The loop's own
// `poll(fd, POLLOUT)` wake — not an internal `wait_writable` — is what gates each
// resume; between wakes the loop services inbound and housekeeping (design §2.1), so the
// outbound write can never park the thread (eliminating starvation site 2, design §1.1).
//
// This section provides that lifted state machine.  It reuses the byte-cursor framing
// (`written` never re-issues sent bytes), the per-wait `WRITE_TIMEOUT_MS` budget reset
// on forward progress, the per-frame `FRAME_WALL_CLOCK_MAX_MS` ceiling, and the
// three-way `BackpressureAligned`/`Sent`/fatal-`Err` classification — **migrated, not
// collapsed** (design §2.4).  No internal `poll(POLLOUT)` park; one non-blocking `write`
// attempt per call.  The existing `write_frame_classified[_at]` is left untouched for
// today's blocking send sites; the event loop (a later increment) selects this API.

/// The outcome of one [`FrameWriteState::step_writable`] non-blocking write attempt,
/// driven by the event loop's own `poll(POLLOUT)` wake (design §2.4).  The loop maps
/// these to its iteration control flow (the §2.1 sketch's `WroteWhole` /
/// `WouldBlockMidFrame` / `Err`):
#[derive(Debug, PartialEq, Eq)]
pub enum StepOutcome {
    /// The whole frame reached the kernel send buffer (across this and any prior
    /// resumes).  The loop advances to the next mic frame.
    WroteWhole,
    /// `write` accepted some bytes but the frame is not yet complete; the cursor has
    /// advanced and the per-wait budget was re-armed.  The loop re-polls `POLLOUT`.
    WrotePartial,
    /// `write` returned `WouldBlock`/`Ok(0)`/`TimedOut` with no byte accepted this
    /// attempt — the kernel send buffer is full.  The cursor is unchanged; the loop
    /// re-polls `POLLOUT` (and, per §2.4, the budget/ceiling are enforced separately by
    /// [`FrameWriteState::check_deadlines`] on each wake, not by parking here).
    WouldBlock,
}

/// Consecutive zero-progress write attempts after which the event-loop pump de-arms POLLOUT
/// for one backoff tick. Small by design: unlike the blocking path's 1024-iteration cap — a
/// terminal belt-and-suspenders bound — each trip here costs a real poll+write syscall pair
/// on-device, and the remedy is a ~10 ms pause, not termination. A handful of pauses fit
/// comfortably inside the 750 ms write budget, and ≥ 2 keeps a one-off poll/write
/// disagreement from tripping it at all.
pub const SPIN_GUARD_THRESHOLD: u32 = 8;

/// The resumable outbound-frame send cursor, lifted out of `write_frame_classified`'s
/// internal loop so it persists **across event-loop iterations** (design §2.4).
///
/// The event loop holds **at most one** of these — the current in-flight AudioFrame /
/// SegmentEnd / Telemetry being written (design §2.3: "at most one in-flight outbound
/// frame", not a queue).  Lifecycle:
/// 1. [`FrameWriteState::begin`] — encode the frame into the caller-held buffer, arm the
///    per-frame ceiling and the first per-wait budget.
/// 2. [`FrameWriteState::step_writable`] — on each `POLLOUT` wake, one non-blocking
///    `write` attempt; never parks.  Returns [`StepOutcome`] (or fatal `Err`).
/// 3. [`FrameWriteState::check_deadlines`] — in the loop's housekeeping step (design
///    §2.1 step 6 / §2.4 "evaluated in the housekeeping step on every `poll` wake"),
///    enforce the two-tier budget/ceiling.  Returns the migrated three-way
///    classification when a budget elapses.
///
/// **Cursor lifetime obligation (design §2.4, §5 risk #5).** A `FrameWriteState` holding
/// a mid-frame `written > 0` must be **discarded on teardown/reconnect**, never carried
/// onto a fresh socket — a stale tail would corrupt the first frame of the next
/// connection.  This type owns no socket, so dropping it is the discard; the loop must
/// not reuse a `FrameWriteState` across a `held_socket` clear.
pub struct FrameWriteState {
    /// Encoded frame length in the caller-held buffer (`buf[..n]` is the frame).
    n: usize,
    /// Bytes already written to the socket — never re-issued (the framing invariant).
    written: usize,
    /// High-water mark of `written` at the last counted resume cycle (mirrors
    /// `write_frame_classified`'s `written_at_resume`): a wake counts a new resume only
    /// once `written` advances past this, so a `poll`/`write` readiness disagreement
    /// (a `POLLOUT` wake that produces no advance) cannot inflate the count.
    written_at_resume: usize,
    /// Genuine resume cycles: `POLLOUT`-gated wakes that advanced `written` past the
    /// previous counted resume (the design §4 test-#4 / HIL resume signal).
    resume_cycles: u32,
    /// Bytes written as of the last per-wait-budget arming — lets a budget that elapsed
    /// *after* forward progress reset-and-continue rather than terminate, distinguishing
    /// a slow-but-alive peer from a dead one (the `written_at_last_wait` semantics of
    /// `wait_for_writable`, design §2.4 budget-resets-on-progress).
    written_at_budget: usize,
    /// Absolute per-frame wall-clock ceiling (`FRAME_WALL_CLOCK_MAX_MS`), NOT reset by
    /// progress (design §2.4 tier 2 / the mic-ring-lap watchdog).
    frame_ceiling: std::time::Instant,
    /// Absolute per-wait budget deadline (`WRITE_TIMEOUT_MS`), reset on forward progress,
    /// clamped to `frame_ceiling` so a wait cannot overshoot the per-frame total.
    budget_deadline: std::time::Instant,
    /// Total non-blocking `write` attempts on this frame (one per `step_writable` call).
    /// With `would_blocks` this is the mid-tail-dead post-mortem forensic, read per this
    /// mapping: `write_attempts` climbing with `would_blocks ≈ write_attempts`
    /// means POLLOUT kept firing but `write` kept refusing — a poll/write readiness
    /// disagreement (the classic lwIP `ERR_MEM` presentation under heap exhaustion). A
    /// count *flat* since the prefix means POLLOUT never rose (the send buffer stayed full);
    /// it does NOT by itself indicate a loop arming bug — a correct POLLOUT-gated loop shows
    /// flat attempts too when POLLOUT never rises. Splitting a transport stall from device
    /// resource starvation needs the pcap and the heap trough alongside, not this count alone.
    write_attempts: u32,
    /// `step_writable` attempts that accepted no byte (`WouldBlock`/`Ok(0)`).
    would_blocks: u32,
    /// Consecutive zero-progress write attempts — reset the moment any byte is accepted.
    /// Drives [`spin_guard_tripped`](Self::spin_guard_tripped).
    consecutive_no_progress: u32,
    /// Wall-clock start of this frame (armed in `begin_at`), for the post-mortem's
    /// elapsed-since-begin.
    began_at: std::time::Instant,
}

impl FrameWriteState {
    /// Begin a new in-flight frame: encode it into the caller-held `buf` and arm the
    /// per-frame ceiling + first per-wait budget against `now` (design §2.4).  `buf` must
    /// outlive the state — the loop re-passes the same `&buf[..n]` to each
    /// [`step_writable`](Self::step_writable).  Returns the encoded length on success so
    /// the caller can keep the matching slice, or an encode error (fatal — drop the frame).
    ///
    /// The clock is injected (matching `write_frame_classified_at`) so the budget/ceiling
    /// arithmetic is unit-testable with a fake clock; the production caller passes
    /// `std::time::Instant::now`.
    pub fn begin(
        frame: &crate::wire::StreamFrame,
        buf: &mut [u8],
        now: impl Fn() -> std::time::Instant,
    ) -> std::io::Result<Self> {
        use std::time::Duration;
        let frame_ceiling = now() + Duration::from_millis(FRAME_WALL_CLOCK_MAX_MS);
        Self::begin_at(frame, buf, now, frame_ceiling)
    }

    /// Ceiling-injecting core of [`begin`](Self::begin): identical but takes the absolute
    /// per-frame `frame_ceiling` explicitly so unit tests can isolate the per-wait budget
    /// (a far-future ceiling) or drive the ceiling exit deterministically (a near/past
    /// ceiling) — the same testable seam `write_frame_classified_at` exposes (design §2.4 /
    /// §4 test #4).  The production [`begin`](Self::begin) passes
    /// `now() + FRAME_WALL_CLOCK_MAX_MS`.
    pub fn begin_at(
        frame: &crate::wire::StreamFrame,
        buf: &mut [u8],
        now: impl Fn() -> std::time::Instant,
        frame_ceiling: std::time::Instant,
    ) -> std::io::Result<Self> {
        use crate::wire::encode_frame;
        use std::time::Duration;
        let n =
            encode_frame(frame, buf).map_err(|_| std::io::Error::other("encode_frame failed"))?;
        let started = now();
        let budget_deadline =
            (started + Duration::from_millis(WRITE_TIMEOUT_MS)).min(frame_ceiling);
        Ok(FrameWriteState {
            n,
            written: 0,
            written_at_resume: 0,
            resume_cycles: 0,
            written_at_budget: 0,
            frame_ceiling,
            budget_deadline,
            write_attempts: 0,
            would_blocks: 0,
            consecutive_no_progress: 0,
            began_at: started,
        })
    }

    /// Bytes written so far (the cursor); `written == 0` is the frame-aligned boundary the
    /// `BackpressureAligned` classification keys on (design §2.4).
    pub fn written(&self) -> usize {
        self.written
    }

    /// Genuine resume cycles taken so far — `POLLOUT`-gated wakes that advanced the
    /// cursor (design §4 test #4 / the HIL resume signal).
    pub fn resume_cycles(&self) -> u32 {
        self.resume_cycles
    }

    /// Non-blocking `write` attempts made on this frame (mid-tail-dead post-mortem forensic).
    pub fn write_attempts(&self) -> u32 {
        self.write_attempts
    }

    /// Attempts that accepted no byte — `WouldBlock`/`Ok(0)` (mid-tail-dead post-mortem).
    pub fn would_blocks(&self) -> u32 {
        self.would_blocks
    }

    /// True once `SPIN_GUARD_THRESHOLD` consecutive write attempts have accepted no byte:
    /// `poll` keeps reporting POLLOUT while `write` keeps refusing. The event-loop pump
    /// responds by de-arming POLLOUT for a short backoff tick, yielding the CPU to the
    /// TCP stack that needs it to clear the stall. Any accepted byte clears the condition.
    pub fn spin_guard_tripped(&self) -> bool {
        self.consecutive_no_progress >= SPIN_GUARD_THRESHOLD
    }

    /// Clear the consecutive-no-progress run — the pump calls this when its backoff tick
    /// expires, so the guard re-arms only on a fresh run of disagreement.
    pub fn reset_spin_guard(&mut self) {
        self.consecutive_no_progress = 0;
    }

    /// One non-blocking `write` attempt against `writer`, driven by the event loop's own
    /// `POLLOUT` wake — **never parks** (design §2.4).  `buf` must be the same buffer
    /// [`begin`](Self::begin) encoded into; `buf[self.written..self.n]` is the unsent
    /// tail.  `now` is the injected clock (budget re-arm on progress).
    ///
    /// Returns:
    /// - `Ok(StepOutcome::WroteWhole)` — frame complete; advance to the next frame;
    /// - `Ok(StepOutcome::WrotePartial)` — cursor advanced, budget re-armed; re-poll;
    /// - `Ok(StepOutcome::WouldBlock)` — send buffer full, no byte this attempt; re-poll;
    /// - `Err(e)` — a genuine socket fault (BrokenPipe/ConnectionReset/…) → drop socket.
    ///
    /// The budget/ceiling *give-up* classification is NOT done here (that would require
    /// parking semantics); it is done in [`check_deadlines`](Self::check_deadlines) on the
    /// loop's housekeeping step (design §2.1 step 6 / §2.4), so a still-progressing peer is
    /// never torn down by a single attempt and the loop stays non-parking.
    pub fn step_writable(
        &mut self,
        writer: &mut dyn std::io::Write,
        buf: &[u8],
        now: impl Fn() -> std::time::Instant,
    ) -> std::io::Result<StepOutcome> {
        use std::time::Duration;
        self.write_attempts += 1;
        match writer.write(&buf[self.written..self.n]) {
            // `Ok(0)` is a degenerate would-block on a non-blocking socket (not EOF for a
            // writer), classified identically to `WouldBlock` — no byte accepted, cursor
            // unchanged (matches `write_frame_classified`'s `Ok(0)` arm).
            Ok(0) => {
                self.would_blocks += 1;
                self.consecutive_no_progress += 1;
                Ok(StepOutcome::WouldBlock)
            }
            Ok(m) => {
                self.consecutive_no_progress = 0;
                self.written += m;
                if self.written < self.n {
                    // A partial that did not finish the frame and advanced past the last
                    // counted resume is a genuine resume cycle: a `POLLOUT`-gated wake
                    // drove forward progress on the unsent tail (mirrors
                    // `write_frame_classified`'s `written < n && written > written_at_resume`
                    // guard — a write that *completes* the frame is NOT a resume, and a
                    // frame that fit in one attempt never resumed).
                    if self.written > self.written_at_resume {
                        self.resume_cycles += 1;
                        self.written_at_resume = self.written;
                    }
                    // Forward progress → re-arm the per-wait budget (design §2.4 tier 1),
                    // clamped to the per-frame ceiling so a subsequent wait cannot overshoot
                    // it.
                    self.written_at_budget = self.written;
                    self.budget_deadline =
                        (now() + Duration::from_millis(WRITE_TIMEOUT_MS)).min(self.frame_ceiling);
                    Ok(StepOutcome::WrotePartial)
                } else {
                    Ok(StepOutcome::WroteWhole)
                }
            }
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                // Transient backpressure: send buffer full, no byte accepted.  Cursor
                // unchanged; the loop re-polls POLLOUT.  No park — the give-up budget is
                // enforced by `check_deadlines` on the housekeeping step (design §2.4).
                self.would_blocks += 1;
                self.consecutive_no_progress += 1;
                Ok(StepOutcome::WouldBlock)
            }
            // Genuine socket fault (BrokenPipe, ConnectionReset, …) → drop the socket.
            Err(e) => Err(e),
        }
    }

    /// Housekeeping deadline check, run on every `poll` wake (design §2.1 step 6 / §2.4
    /// "evaluated in the housekeeping step on every `poll` wake"): enforce the two-tier
    /// budget/ceiling and, when one elapses, return the migrated three-way classification.
    ///
    /// Returns:
    /// - `None` — the frame is still within both the per-wait budget and the per-frame
    ///   ceiling; keep waiting on `POLLOUT`;
    /// - `Some(Ok(SendOutcome::BackpressureAligned))` — a budget/ceiling elapsed with
    ///   `written == 0`: the byte stream is still frame-aligned → **drop the segment, keep
    ///   the socket** (design §2.4 tier-3 `written == 0`);
    /// - `Some(Err(_))` — a budget/ceiling elapsed with `written > 0` (mid-tail dead): the
    ///   unsent tail is undeliverable → **drop the segment and clear the socket** (design
    ///   §2.4 tier-3 `written > 0`).
    ///
    /// The per-wait budget is treated exactly as `wait_for_writable` does: a budget that
    /// elapsed *after* forward progress since it was armed (`written > written_at_budget`)
    /// is **not** a stall — it re-arms and returns `None` (the loop keeps the frame alive;
    /// design §2.4 "resets on forward progress").  Only a per-wait budget that elapsed with
    /// the cursor unchanged since arming, or the absolute per-frame ceiling, is terminal.
    pub fn check_deadlines(
        &mut self,
        now: impl Fn() -> std::time::Instant,
    ) -> Option<std::io::Result<SendOutcome>> {
        use std::time::Duration;
        let t = now();
        let elapsed_ms = t.saturating_duration_since(self.began_at).as_millis() as u64;
        // Absolute per-frame ceiling (tier 2) — never reset by progress.  Terminal.
        if t >= self.frame_ceiling {
            return Some(self.classify_no_progress(elapsed_ms));
        }
        // Per-wait budget (tier 1) — reset on forward progress.
        if t >= self.budget_deadline {
            if self.written > self.written_at_budget {
                // Progressed since the budget was armed → not a stall: re-arm and continue
                // (clamped to the per-frame ceiling so the next wait cannot overshoot it).
                self.written_at_budget = self.written;
                self.budget_deadline =
                    (t + Duration::from_millis(WRITE_TIMEOUT_MS)).min(self.frame_ceiling);
                None
            } else {
                // No byte of progress for a full budget → terminal.
                Some(self.classify_no_progress(elapsed_ms))
            }
        } else {
            None
        }
    }

    /// Absolute deadline (the *earlier* of the per-wait budget and the per-frame ceiling)
    /// at which this in-flight frame must next be re-checked — folded into the event
    /// loop's `timeout_to_next_deadline` so the give-up classification fires on time even
    /// while `POLLOUT` never becomes writable (design §2.6 deadline set, §2.4 watchdog).
    pub fn next_deadline(&self) -> std::time::Instant {
        self.budget_deadline.min(self.frame_ceiling)
    }

    /// Terminal no-progress classification — the migrated tier-3 three-way split (design
    /// §2.4): `written == 0` is frame-aligned (`BackpressureAligned`, keep socket);
    /// `written > 0` is mid-tail dead (fatal `Err`, clear socket).  Identical to
    /// `write_frame_classified`'s `classify_no_progress` closure.
    fn classify_no_progress(&self, elapsed_ms: u64) -> std::io::Result<SendOutcome> {
        if self.written == 0 {
            Ok(SendOutcome::BackpressureAligned)
        } else {
            // The post-mortem reaches the device log through the streamer warn's `{:?}` on
            // this error, so a recurrence is self-diagnosing without a rerun.  This is a
            // failure-path-only allocation; under heap starvation the diagnostic alloc could
            // itself fail, but the segment is already fatally lost on this path.
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                mid_tail_dead_message(
                    self.written,
                    self.n,
                    self.resume_cycles,
                    self.write_attempts,
                    self.would_blocks,
                    elapsed_ms,
                ),
            ))
        }
    }
}

/// Format the mid-tail-dead post-mortem message with the forensic counter block **first**.
///
/// This message reaches the device log wrapped as the streamer warn's `{:?}` on an
/// `io::Error`; that prefix (including a wrapping-`u32` segment id, up to 10 digits) plus the
/// `Custom { kind: TimedOut, error: "` wrapper spend ~110 of the 200-byte `LogFrame` message
/// budget before this string starts, leaving under ~90 bytes on the wire.  The counter block
/// therefore leads so the decisive forensics survive the cap even when the trailing prose is
/// truncated; the prose is expendable.  The bracket labels are deliberately terse for the same
/// reason: if a counter or prose change ever pushes the closing `]` past the cap, shorten the
/// labels further — never the counters (which the forensic mapping reads).  The
/// closing-bracket-within-200-bytes property, modeled at a worst-case 10-digit segment id, is
/// pinned by `mid_tail_dead_post_mortem_fits_log_cap`.
fn mid_tail_dead_message(
    written: usize,
    n: usize,
    resume_cycles: u32,
    write_attempts: u32,
    would_blocks: u32,
    elapsed_ms: u64,
) -> String {
    format!(
        "[written={written}/{n} resumes={resume_cycles} attempts={write_attempts} \
         would_blocks={would_blocks} elapsed_ms={elapsed_ms}] mid-tail dead — prefix accepted \
         then no progress a full budget; socket undeliverable, dropping"
    )
}

#[cfg(test)]
mod tests {
    // ── write_frame_classified unit tests (design §6; partial-write-fix §4) ────
    //
    // Relocated from the device crate `respeaker-pod/src/main.rs` `mod tests`
    // (partial-write-fix design §2.3a/§4): they now live in `audio-pipeline` so
    // they **execute** under `cargo test --workspace` (`make check`) rather than
    // being compile-checked only on the device clippy lane.  They exercise the
    // off-target-testable core of `send_frame_bp`: the `written`-tracking write
    // loop and the aligned-vs-resumed-vs-mid-tail-dead classification.  The
    // `poll(POLLOUT)` writability wait is stubbed by a deterministic
    // `wait_writable` closure (which takes the loop-owned `Instant` deadline —
    // partial-write-fix §2.2a); the real `poll(POLLOUT)` path is the subject of
    // the HIL self-test (design §3.1/§6), not these tests.
    //
    // After the partial-write fix a partial write is *resumable* (it resumes to
    // `Sent`); the only socket-dropping outcomes are the mid-tail-dead `Err` (a
    // prefix accepted then no progress for a full budget) and a genuine fault.

    use super::{
        write_frame_classified, write_frame_classified_at, SendOutcome, Writable, WRITE_TIMEOUT_MS,
    };
    use crate::test_support::audio_frame;
    use crate::wire::{AUDIO_SAMPLES_PER_FRAME, MAX_FRAME_BYTES};
    use std::io::Write;

    /// A `Write` driven by a scripted sequence of per-call behaviors, so a test
    /// can force `WouldBlock` after an exact byte count (mirrors the mock the
    /// design §6 calls for).  Used only by the send-loop tests in this module, so
    /// it lives here rather than in the shared `test_support` module.
    enum WriteStep {
        /// Accept up to `usize` bytes of the offered slice (capped at slice len).
        Accept(usize),
        /// Return `WouldBlock` without writing.
        WouldBlock,
        /// Return `Ok(0)` (accepted no bytes, no error) — the degenerate
        /// would-block the production loop treats like `WouldBlock`.
        AcceptZero,
        /// Return an error of this kind without writing.
        Err(std::io::ErrorKind),
    }

    struct ScriptedWriter {
        steps: std::collections::VecDeque<WriteStep>,
        written: usize,
    }
    impl ScriptedWriter {
        fn new(steps: Vec<WriteStep>) -> Self {
            ScriptedWriter {
                steps: steps.into(),
                written: 0,
            }
        }
    }
    impl Write for ScriptedWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            match self.steps.pop_front() {
                Some(WriteStep::Accept(n)) => {
                    let n = n.min(buf.len());
                    self.written += n;
                    Ok(n)
                }
                Some(WriteStep::WouldBlock) => {
                    Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
                }
                Some(WriteStep::AcceptZero) => Ok(0),
                Some(WriteStep::Err(kind)) => Err(std::io::Error::from(kind)),
                // No script left: accept everything (drains the frame to Sent).
                None => {
                    self.written += buf.len();
                    Ok(buf.len())
                }
            }
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Write completes immediately (no backpressure) → `Sent`.
    #[test]
    fn send_classified_completes_immediately() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        // Empty script → writer accepts the whole frame in one call.
        let mut writer = ScriptedWriter::new(vec![]);
        let (result, resume_cycles) = write_frame_classified(&mut writer, &frame, &mut buf, |_| {
            panic!("wait_writable must not be called when the write completes")
        });
        let outcome = result.expect("classification");
        assert_eq!(outcome, SendOutcome::Sent);
        assert_eq!(
            resume_cycles, 0,
            "a frame that fit immediately never resumed"
        );
        let n = crate::wire::encode_frame(&frame, &mut vec![0u8; MAX_FRAME_BYTES + 2])
            .expect("encode for length check");
        assert_eq!(writer.written, n, "the whole frame must be written");
    }

    /// `WouldBlock` on the very first write (zero bytes), then the budget is
    /// exhausted → `BackpressureAligned`, socket stays frame-aligned.
    #[test]
    fn send_classified_wouldblock_zero_bytes_is_aligned() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let mut writer = ScriptedWriter::new(vec![WriteStep::WouldBlock]);
        let mut waits = 0;
        let (result, resume_cycles) =
            write_frame_classified(&mut writer, &frame, &mut buf, |_deadline| {
                waits += 1;
                Writable::TimedOut
            });
        let outcome = result.expect("classification");
        assert_eq!(outcome, SendOutcome::BackpressureAligned);
        assert_eq!(waits, 1, "wait_writable must be called once on WouldBlock");
        assert_eq!(
            resume_cycles, 0,
            "no partial write occurred → no resume cycle"
        );
        assert_eq!(writer.written, 0, "no bytes written → stream stays aligned");
    }

    /// A partial write (N>0) followed by *no progress for a full budget* (the writer
    /// keeps `WouldBlock`ing across two `TimedOut` waits) → fatal `Err`
    /// (mid-tail-dead), socket must be dropped.  After the partial-write fix this
    /// replaces the old `BackpressureDesynced` assertion: a single `TimedOut` after
    /// progress is *not* terminal (it is a `Progressed` continue); only a budget that
    /// elapses with `written` unchanged is the stall (partial-write-fix §2.2/§2.2a).
    #[test]
    fn send_classified_partial_then_no_progress_is_mid_tail_dead_err() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        // Accept 5 bytes, then WouldBlock forever.  First wait: written (5) advanced
        // since written_at_last_wait (0) → Progressed (reset + retry).  Second wait:
        // written (5) == written_at_last_wait (5) → Stalled → mid-tail-dead Err.
        let mut writer = ScriptedWriter::new(vec![
            WriteStep::Accept(5),
            WriteStep::WouldBlock,
            WriteStep::WouldBlock,
        ]);
        let mut waits = 0;
        let (result, resume_cycles) =
            write_frame_classified(&mut writer, &frame, &mut buf, |_deadline| {
                waits += 1;
                Writable::TimedOut
            });
        let err = result.expect_err("no progress for a full budget after a prefix is fatal");
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::TimedOut,
            "mid-tail-dead surfaces as a TimedOut Err (the call site clears the socket)"
        );
        assert_eq!(
            waits, 2,
            "first TimedOut continues (progress made); second is the stall"
        );
        assert!(
            resume_cycles >= 1,
            "a partial write occurred → the Err carries resume_cycles ≥ 1 \
             (distinguishes mid-tail-dead from a written==0 aligned outcome)"
        );
        assert_eq!(writer.written, 5, "partial bytes already committed");
    }

    /// A partial write, then the socket becomes writable, then the frame completes →
    /// `Sent` (the core fix: a partial write is *resumable*, not a desync).  The
    /// resume-cycle count is ≥1 because the loop took a `Ready` after a partial write.
    #[test]
    fn send_classified_partial_then_writable_completes_is_sent() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let n = crate::wire::encode_frame(&frame, &mut vec![0u8; MAX_FRAME_BYTES + 2])
            .expect("encode for length");
        // Accept 5, WouldBlock → wait returns Ready → (no script) accept the rest.
        let mut writer = ScriptedWriter::new(vec![WriteStep::Accept(5), WriteStep::WouldBlock]);
        let (result, resume_cycles) =
            write_frame_classified(&mut writer, &frame, &mut buf, |_deadline| Writable::Ready);
        let outcome = result.expect("classification");
        assert_eq!(outcome, SendOutcome::Sent);
        assert!(
            resume_cycles >= 1,
            "a Ready taken after a 0<written<n partial write is a resume cycle"
        );
        assert!(
            writer.written >= n,
            "the whole frame must be written, past the prefix"
        );
    }

    /// `Writable::Ready` resumes the write loop and the frame completes → `Sent`,
    /// across two distinct partial-write edges; the resume-cycle count reports exactly 2.
    /// The lighter smoke companion to the many-cycle repeatability test below
    /// (partial-write-fix §4).  This asserts an EXACT count (test review test-2): two
    /// `Accept`+`WouldBlock` edges → two resume cycles in a correct implementation, so a
    /// regression that counts only the first edge FAILs here.  This is a deterministic
    /// off-target bound on a fixed two-edge script, NOT the calibrated hardware claim the
    /// HIL adversary-A bar relaxed to ≥1 (`notes-design-backpressure-pw-user-2.md`).
    #[test]
    fn send_classified_resumes_after_writable() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let n = crate::wire::encode_frame(&frame, &mut vec![0u8; MAX_FRAME_BYTES + 2])
            .expect("encode for length");
        // Accept 4 bytes, WouldBlock, accept 6 bytes, WouldBlock, then (no script)
        // accept the rest → Sent.  Two waits, both Ready.
        let mut writer = ScriptedWriter::new(vec![
            WriteStep::Accept(4),
            WriteStep::WouldBlock,
            WriteStep::Accept(6),
            WriteStep::WouldBlock,
        ]);
        let mut waits = 0;
        let (result, resume_cycles) =
            write_frame_classified(&mut writer, &frame, &mut buf, |_deadline| {
                waits += 1;
                Writable::Ready
            });
        let outcome = result.expect("classification");
        assert_eq!(outcome, SendOutcome::Sent);
        assert_eq!(waits, 2, "two WouldBlock waits before the frame completed");
        assert_eq!(
            resume_cycles, 2,
            "two distinct Accept+WouldBlock edges → exactly two resume cycles \
             (each wait that follows a partial-write advance counts once)"
        );
        assert_eq!(writer.written, n, "whole frame eventually written");
    }

    /// **Many-cycle slow drain — the deterministic repeatability proof
    /// (partial-write-fix §4, `notes-design-backpressure-pw-user-2.md`).**
    ///
    /// This is the off-target test that carries the property the HIL adversary A no longer
    /// pins: that the per-wait budget re-arms on **every** forward-progress step, not just
    /// the first.  A mock writer accepts only a few bytes per `write` call, carving the
    /// frame into many (≫2) distinct intermediate partial writes, each separated by a
    /// `wait_writable` that returns `Ready`.  A **hand-advanced fake clock** drives the
    /// loop: before each `Ready` the test advances `now` by `WRITE_TIMEOUT_MS − ε` (most
    /// of, but not all of, a budget).  The waiter closure faithfully mirrors the
    /// production `poll_writable` deadline contract — it returns `Ready` only while
    /// `now < deadline`, else `TimedOut` — so the loop survives **only** if it re-armed
    /// the deadline on the preceding partial write.
    ///
    /// Asserts: (1) `Sent` and `written` monotone to `n`; (2) `resume_cycles` equals the
    /// number of distinct intermediate partial writes (≥2 — the repeatability evidence);
    /// (3) every wait saw a `deadline` strictly ahead of the current fake `now` (the
    /// budget was re-armed each step) even though total simulated elapsed time
    /// (`waits × (WRITE_TIMEOUT_MS − ε)`) far exceeds a single budget.  A regression that
    /// re-arms the budget once but not again would, from the second wait on, hand the
    /// closure a `deadline ≤ now`, the closure would return `TimedOut`, and with no byte
    /// of progress *that wait* the frame misclassifies as a no-progress stall → `Err` —
    /// this test FAILs on that regression, deterministically and with no hardware.
    #[test]
    fn send_classified_many_cycle_slow_drain_budget_rearms_each_step() {
        use std::cell::Cell;
        use std::time::{Duration, Instant};

        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let n = crate::wire::encode_frame(&frame, &mut vec![0u8; MAX_FRAME_BYTES + 2])
            .expect("encode for length");

        // Carve the frame into many small partial writes: accept K bytes, WouldBlock,
        // repeat.  K ≪ n forces ~n/K distinct intermediate advances (well above the ≥2
        // the repeatability claim needs).  The trailing (no-script) write accepts the
        // remainder to Sent.
        const K: usize = 8;
        assert!(
            n > 3 * K,
            "frame must be large enough to force ≥3 partial writes"
        );
        let mut script = Vec::new();
        // Leave the last K bytes for the no-script trailing write so the loop ends on a
        // completing write rather than another WouldBlock.
        let edges = (n - K) / K;
        for _ in 0..edges {
            script.push(WriteStep::Accept(K));
            script.push(WriteStep::WouldBlock);
        }
        let mut writer = ScriptedWriter::new(script);

        // Fake clock the test advances by hand.  It starts at a real `Instant` and only
        // ever moves when the waiter closure advances it — no real time passes.
        let base = Instant::now();
        let clock = Cell::new(base);
        // `now` and the waiter closure (below) intentionally alias the same `Cell`: the
        // waiter advances it, `now` reads it — the single-threaded shared-clock mechanism
        // (quality review quality-2).  Both hold shared `&Cell` refs, which Rust permits;
        // do not "fix" the apparent double-borrow by splitting the clock — that breaks the
        // happens-before the test relies on (each `now()` must see the waiter's last set).
        let now = || clock.get();
        // A per-frame ceiling far in the (fake) future so the many-step drain completes
        // under it — the ceiling's own bound is exercised by the dedicated ceiling tests;
        // this test isolates the per-wait reset.
        let frame_ceiling = base + Duration::from_secs(3600);
        // Most of a budget, never the whole budget: with the budget re-armed each step the
        // waiter always sees `now < deadline`.
        let step = Duration::from_millis(WRITE_TIMEOUT_MS - 1);

        let mut waits: u32 = 0;
        let mut deadline_always_ahead = true;
        let (result, resume_cycles) = write_frame_classified_at(
            &mut writer,
            &frame,
            &mut buf,
            |deadline| {
                waits += 1;
                // Advance the fake clock by most-of-a-budget, simulating a long wait.
                clock.set(clock.get() + step);
                let t = clock.get();
                // Record whether the loop re-armed the budget: a correct loop hands a
                // deadline strictly ahead of `now`; a once-but-not-again regression would
                // hand a stale (already-passed) deadline from the second wait on.
                if t >= deadline {
                    deadline_always_ahead = false;
                }
                // Faithful `poll_writable` deadline contract: writable only while the
                // budget has not elapsed.
                if t < deadline {
                    Writable::Ready
                } else {
                    Writable::TimedOut
                }
            },
            frame_ceiling,
            now,
        );

        let outcome = result.expect("a slow-but-progressing peer must complete the frame");
        assert_eq!(outcome, SendOutcome::Sent);
        assert_eq!(
            writer.written, n,
            "the whole frame must be written, monotone to n"
        );
        assert!(
            waits as usize >= edges,
            "every WouldBlock edge must take a wait (got {waits} for {edges} edges)"
        );
        assert!(
            resume_cycles as usize >= edges,
            "resume_cycles must equal the number of distinct intermediate partial writes \
             (≥{edges}, the repeatability evidence — the reset fired more than once); got \
             {resume_cycles}"
        );
        assert!(
            deadline_always_ahead,
            "the per-wait budget must be re-armed on EVERY forward-progress step: each wait \
             must be handed a deadline strictly ahead of the current clock, even though \
             total simulated elapsed time ({} ms) far exceeds one WRITE_TIMEOUT_MS budget \
             ({WRITE_TIMEOUT_MS} ms) — a once-but-not-again reset regression trips here",
            (waits as u64) * (WRITE_TIMEOUT_MS - 1),
        );
    }

    /// A `Writable::TimedOut` that arrives *after* forward progress is no longer
    /// terminal: the loop records the new high-water mark, resets the budget, and
    /// continues to completion → `Sent`.  This distinguishes the §2.2a loop from the
    /// shallow timeout-closure tweak the design warns against (a `TimedOut` would have
    /// been terminal on first occurrence) — partial-write-fix §4.
    #[test]
    fn send_classified_timedout_after_progress_continues() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let n = crate::wire::encode_frame(&frame, &mut vec![0u8; MAX_FRAME_BYTES + 2])
            .expect("encode for length");
        // Accept 9, WouldBlock → wait returns TimedOut (but written advanced 0→9, so
        // it is Progressed, not a stall) → (no script) accept the rest → Sent.
        let mut writer = ScriptedWriter::new(vec![WriteStep::Accept(9), WriteStep::WouldBlock]);
        let mut waits = 0;
        let (result, resume_cycles) =
            write_frame_classified(&mut writer, &frame, &mut buf, |_deadline| {
                waits += 1;
                Writable::TimedOut
            });
        let outcome = result.expect("a TimedOut after progress must not be terminal");
        assert_eq!(outcome, SendOutcome::Sent);
        assert_eq!(waits, 1, "one wait; it Progressed rather than Stalled");
        assert_eq!(writer.written, n, "whole frame eventually written");
        // The partial write (Accept(9)) before the wait is a genuine forward-progress
        // resume cycle and must be counted, so the HIL A sub-case can prove the reset is
        // repeatable (test review test-3).
        assert!(
            resume_cycles >= 1,
            "a partial write that advanced before the TimedOut-after-progress must count \
             as a resume cycle (got {resume_cycles})"
        );
    }

    /// A genuine socket fault (`BrokenPipe`) → `Err`, never a backpressure outcome.
    #[test]
    fn send_classified_brokenpipe_is_err() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let mut writer = ScriptedWriter::new(vec![WriteStep::Err(std::io::ErrorKind::BrokenPipe)]);
        let (result, _resume_cycles) =
            write_frame_classified(&mut writer, &frame, &mut buf, |_| {
                panic!("wait_writable must not be called on a fatal error")
            });
        let err = result.expect_err("BrokenPipe must surface as Err");
        assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
    }

    /// `ConnectionReset` mid-frame → `Err`, even after a partial write.
    #[test]
    fn send_classified_connreset_is_err() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let mut writer = ScriptedWriter::new(vec![
            WriteStep::Accept(8),
            WriteStep::Err(std::io::ErrorKind::ConnectionReset),
        ]);
        let (result, _resume_cycles) =
            write_frame_classified(&mut writer, &frame, &mut buf, |_| {
                panic!("wait_writable must not be called on a fatal error")
            });
        let err = result.expect_err("ConnectionReset must surface as Err");
        assert_eq!(err.kind(), std::io::ErrorKind::ConnectionReset);
    }

    /// A `Writable::Fault` from the waiter (e.g. poll(POLLOUT) error / POLLHUP)
    /// is surfaced as `Err`, dropping the socket (design §5).
    #[test]
    fn send_classified_writer_fault_is_err() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let mut writer = ScriptedWriter::new(vec![WriteStep::WouldBlock]);
        let (result, _resume_cycles) =
            write_frame_classified(&mut writer, &frame, &mut buf, |_| {
                Writable::Fault(std::io::Error::from(std::io::ErrorKind::ConnectionAborted))
            });
        let err = result.expect_err("poll fault must surface as Err");
        assert_eq!(err.kind(), std::io::ErrorKind::ConnectionAborted);
    }

    /// `ErrorKind::TimedOut` is a `WouldBlock` synonym (design §3.2): a `TimedOut`
    /// write error on the first write reaches `wait_writable` (not the fatal arm)
    /// and a budget timeout there → `BackpressureAligned`.  Guards against a refactor
    /// that drops the `TimedOut` clause from the transient-backpressure match.
    #[test]
    fn send_classified_timedout_is_wouldblock_synonym() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let mut writer = ScriptedWriter::new(vec![WriteStep::Err(std::io::ErrorKind::TimedOut)]);
        let mut waits = 0;
        let (result, _resume_cycles) =
            write_frame_classified(&mut writer, &frame, &mut buf, |_deadline| {
                waits += 1;
                Writable::TimedOut
            });
        let outcome = result.expect("TimedOut must be treated as transient, not fatal");
        assert_eq!(outcome, SendOutcome::BackpressureAligned);
        assert_eq!(
            waits, 1,
            "TimedOut must reach wait_writable, proving it is a WouldBlock synonym"
        );
    }

    /// `Ok(0)` (writer accepted no bytes, no error) is a degenerate would-block: it
    /// must reach `wait_writable` and, on a budget timeout with zero bytes written,
    /// classify as `BackpressureAligned`.
    #[test]
    fn send_classified_ok_zero_is_aligned() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let mut writer = ScriptedWriter::new(vec![WriteStep::AcceptZero]);
        let mut waits = 0;
        let (result, _resume_cycles) =
            write_frame_classified(&mut writer, &frame, &mut buf, |_deadline| {
                waits += 1;
                Writable::TimedOut
            });
        let outcome = result.expect("Ok(0) must be treated as a degenerate would-block, not EOF");
        assert_eq!(outcome, SendOutcome::BackpressureAligned);
        assert_eq!(waits, 1, "Ok(0) must reach wait_writable");
        assert_eq!(writer.written, 0, "no bytes written → stream stays aligned");
    }

    /// `Ok(0)` (degenerate would-block) after a partial write, then *no progress for a
    /// full budget* → fatal `Err` (mid-tail-dead).  After the partial-write fix the
    /// `Ok(0)` path routes through the same progress-reset logic as `WouldBlock`: a
    /// single `TimedOut` after the partial write Progressed; a second with `written`
    /// unchanged is the stall that drops the socket (partial-write-fix §2.2/§3
    /// "`Ok(0)` from the writer").
    #[test]
    fn send_classified_ok_zero_after_partial_is_mid_tail_dead_err() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        // Accept 7, then Ok(0) forever (never accepts another byte).
        let mut writer = ScriptedWriter::new(vec![
            WriteStep::Accept(7),
            WriteStep::AcceptZero,
            WriteStep::AcceptZero,
        ]);
        let mut waits = 0;
        let (result, resume_cycles) =
            write_frame_classified(&mut writer, &frame, &mut buf, |_deadline| {
                waits += 1;
                Writable::TimedOut
            });
        let err = result.expect_err("Ok(0) with no progress after a prefix is fatal");
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        assert_eq!(waits, 2, "first TimedOut Progressed; second is the stall");
        assert!(
            resume_cycles >= 1,
            "a partial write occurred before the stall"
        );
        assert_eq!(writer.written, 7, "partial bytes already committed");
    }

    /// A `wait_writable` that keeps returning `Ready` while the writer makes no
    /// progress (a poll/write readiness disagreement — design §3.1/§5) must NOT
    /// spin forever: the consecutive-zero-progress cap terminates it with a
    /// timeout-equivalent classification (`BackpressureAligned` when nothing was
    /// written).  Guards the busy-spin bound the efficiency/correctness reviews flag.
    #[test]
    fn send_classified_ready_no_progress_is_bounded() {
        /// A writer that always returns `WouldBlock` — it never accepts a byte, so
        /// without the consecutive-Ready cap the loop would spin forever.
        struct AlwaysWouldBlock;
        impl std::io::Write for AlwaysWouldBlock {
            fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let mut writer = AlwaysWouldBlock;
        let mut waits: u64 = 0;
        // The waiter always claims the socket is `Ready`; only the cap can break the loop.
        let (result, resume_cycles) =
            write_frame_classified(&mut writer, &frame, &mut buf, |_deadline| {
                waits += 1;
                Writable::Ready
            });
        let outcome = result.expect("a bounded spin must terminate cleanly, not error");
        assert_eq!(
            outcome,
            SendOutcome::BackpressureAligned,
            "zero-progress spin with no bytes written classifies as aligned"
        );
        assert_eq!(
            resume_cycles, 0,
            "no byte was ever written → no resume cycle (written stayed 0)"
        );
        // The loop must terminate *exactly* at the cap, not run unbounded and not one
        // call too late.  `MAX_ZERO_PROGRESS_READY` is 1024 and the cap is checked at the
        // top of the loop (`zero_progress_ready >= MAX_ZERO_PROGRESS_READY`), so the
        // waiter is consulted on the 1024 iterations where `zero_progress_ready` is
        // 0..=1023, then the 1025th top-of-loop check fires before another wait.  An exact
        // assertion catches an off-by-one in the cap predicate (`>` vs `>=`) that the
        // prior loose `<= 1025` bound silently tolerated (test review test-2).
        assert_eq!(
            waits, 1024,
            "consecutive-Ready/no-progress cycles must be bounded at exactly \
             MAX_ZERO_PROGRESS_READY (1024); was {waits}"
        );
    }

    /// The per-frame wall-clock ceiling with `written == 0` → `BackpressureAligned`,
    /// `resume_cycles == 0`.  Injecting a ceiling already in the past drives the
    /// top-of-loop ceiling exit deterministically without sleeping (test review test-1):
    /// the buffer-full boundary frame whose first write WouldBlocks must keep the socket
    /// (still frame-aligned) when the ceiling — not the per-wait budget — is what fires.
    #[test]
    fn send_classified_ceiling_at_zero_written_is_aligned() {
        use std::time::Instant;
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        // Writer never accepts a byte; a ceiling in the past fires on the first loop top.
        let mut writer = ScriptedWriter::new(vec![WriteStep::WouldBlock]);
        let mut waits = 0;
        let past_ceiling = Instant::now() - std::time::Duration::from_millis(1);
        let (result, resume_cycles) = write_frame_classified_at(
            &mut writer,
            &frame,
            &mut buf,
            |_deadline| {
                waits += 1;
                Writable::Ready
            },
            past_ceiling,
            Instant::now,
        );
        let outcome = result.expect("ceiling with written==0 keeps the socket (aligned)");
        assert_eq!(outcome, SendOutcome::BackpressureAligned);
        assert_eq!(
            resume_cycles, 0,
            "no partial write before the ceiling fired"
        );
        assert_eq!(
            waits, 0,
            "the ceiling fires at the loop top before any wait"
        );
        assert_eq!(writer.written, 0, "no bytes written → stream stays aligned");
    }

    /// The per-frame wall-clock ceiling with `written > 0` → fatal `Err` (mid-tail-dead),
    /// `resume_cycles >= 1`.  Accept a partial prefix, then let the next loop top hit a
    /// ceiling in the past: the unsent tail is undeliverable so the socket must be
    /// dropped, distinctly from the `written==0` aligned case (test review test-1).
    #[test]
    fn send_classified_ceiling_mid_tail_is_err() {
        use std::cell::Cell;
        use std::time::{Duration, Instant};
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        // Accept 5 bytes (partial), then WouldBlock; the wait returns Ready so the loop
        // re-enters the top, where the now-elapsed ceiling fires with written>0.
        let mut writer = ScriptedWriter::new(vec![WriteStep::Accept(5), WriteStep::WouldBlock]);
        // Hand-advanced fake clock instead of a real sleep (test review test-3): the
        // injected `now` is what makes the "ceiling elapsed on re-entry" condition
        // deterministic without coupling the test to wall time.  The clock starts at
        // `base` (so iteration 1's top-check passes and Accept(5) runs, counting a resume),
        // then the wait closure jumps it past the ceiling so iteration 2's top-check fires
        // the mid-tail-dead arm.  `now` and the waiter closure intentionally alias the same
        // `Cell`: the waiter advances it, `now` reads it (single-threaded shared clock).
        let base = Instant::now();
        let ceiling = base + Duration::from_millis(2);
        let clock = Cell::new(base);
        let now = || clock.get();
        let (result, resume_cycles) = write_frame_classified_at(
            &mut writer,
            &frame,
            &mut buf,
            |_deadline| {
                // Jump the fake clock well past the ceiling; the next loop-top check sees
                // an elapsed ceiling with written>0 → mid-tail-dead.
                clock.set(ceiling + Duration::from_millis(1));
                Writable::Ready
            },
            ceiling,
            now,
        );
        let err = result.expect_err("ceiling with written>0 is mid-tail-dead (fatal Err)");
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        assert!(
            resume_cycles >= 1,
            "a partial write occurred before the ceiling fired (got {resume_cycles})"
        );
        assert_eq!(writer.written, 5, "the accepted prefix stays committed");
    }
}

#[cfg(test)]
mod host_socket_tests {
    // ── Host-socket integration test (partial-write-fix design §4, revision-3 item 2)
    //
    // Where fine-grained behavioral verification of the send/resume loop now lives.
    // The scripted-mock unit tests above prove the *classification and budget* logic
    // with a deterministic stub `wait_writable`; this test proves the **real**
    // non-blocking-`write` + `poll(POLLOUT)` + partial-write/resume loop against a
    // **real localhost TCP stack** — not a mock, not lwIP — so the actual socket path
    // is exercised on every `make check` run.  It binds to the §2.3a seam
    // `write_frame_classified_at(writer, frame, buf, wait_writable, frame_ceiling,
    // now)` with `writer` = a real `TcpStream` and `wait_writable` = a host
    // `poll(POLLOUT)` closure (the host counterpart of the device's `poll_writable`).
    //
    // It is NOT a clock/budget test (that is the deterministic fake-clock unit test
    // above) and NOT the bounded-give-up test (the per-frame ceiling against a real
    // socket has unavoidable real-time slop; its correctness is the unit test, its
    // behaviour on real lwIP is the HIL essential).  Its job is exactly revision-3
    // item 2: "the real-socket non-blocking-write + poll + resume loop completes the
    // frame and never desyncs."

    use super::{write_frame_classified_at, SendOutcome, Writable};
    use crate::test_support::audio_frame;
    use crate::wire::{encode_frame, MAX_AUDIO_PAYLOAD, MAX_FRAME_BYTES};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::os::fd::AsRawFd;
    use std::time::{Duration, Instant};

    // ── Why a `Write` adapter rather than kernel-fragmented localhost ──────────────
    //
    // The design (§4 "Server side") sketched forcing partials by shrinking
    // `SO_RCVBUF`/`SO_SNDBUF` and withholding reads so the kernel returns `0 < m < n`.
    // Verified against the real Linux host (observe-then-bake, CLAUDE.md doctrine):
    // that does NOT work for a single audio frame.  One `StreamFrame::Audio` encodes
    // to ~1.3 KB (`MAX_AUDIO_PAYLOAD` = 1280 PCM bytes + framing), but Linux floors
    // `SO_SNDBUF` at 4608 bytes (a 1024-byte request is doubled-and-floored to 4608)
    // and `SO_RCVBUF` at 2304, so the send buffer always has room for a whole frame
    // the instant the peer reads even one byte — across every (sndbuf, rcvbuf, drain
    // pace) combination probed, `write` returned the full frame in one call and
    // `resume_cycles` was 0.  A sub-frame straddle on lwIP (5760-byte buffer, frames
    // landing on a sub-MSS remainder) simply has no localhost analogue for a frame
    // this small.
    //
    // So the partial is forced at the `Write` boundary instead, with everything else
    // real: `ChunkedSocket` wraps a real non-blocking localhost `TcpStream` and caps
    // each `write` at `chunk` bytes, injecting one `WouldBlock` between chunks.  The
    // loop then takes the real `WouldBlock` arm → calls the real `poll(POLLOUT)` on
    // the real fd (which returns `Ready` because the socket genuinely is writable) →
    // resumes the tail.  Real socket, real fd, real `poll(POLLOUT)`, real TCP
    // delivery, real byte-exact round-trip — only the per-`write` admission count is
    // made deterministic, reproducing exactly the lwIP `0 < m < n` the loop must
    // handle.  This realises the design's stated goal ("the real-socket
    // non-blocking-write + poll + resume loop completes the frame and never desyncs")
    // by the one mechanism that is deterministic on a stack that refuses to fragment
    // a small frame.

    /// Generous per-frame ceiling for the host-socket test — seconds, NOT the 1.0 s
    /// production `FRAME_WALL_CLOCK_MAX_MS` (partial-write-fix design §4 design-5).
    /// Every wait here is a real `poll(POLLOUT)` that returns `Ready` almost
    /// immediately (the socket is genuinely writable), so a slow drain can never trip
    /// the terminal `Err`/`BackpressureAligned` arm; this test does not exercise the
    /// ceiling (that is the deterministic unit test + HIL C sub-case).
    const TEST_FRAME_CEILING: Duration = Duration::from_secs(30);

    /// Read timeout the server arms so a regressed client that never sends the tail
    /// FAILs the test (the loop-read returns short) rather than hanging it
    /// (design §4 "Read/write deadlock on the byte-exact compare").
    const SERVER_READ_TIMEOUT: Duration = Duration::from_secs(10);

    /// A `Write` over a real non-blocking `TcpStream` that caps each `write` at
    /// `chunk` bytes and returns one `WouldBlock` between successive accepting writes.
    /// This deterministically reproduces the lwIP `0 < m < n` partial-count behaviour
    /// (which a small frame cannot trigger on localhost — see the module comment) while
    /// keeping every other limb of the path real: the bytes traverse the real socket,
    /// and the `WouldBlock` drives the loop into the real `poll(POLLOUT)` wait on the
    /// real fd.
    struct ChunkedSocket {
        stream: TcpStream,
        chunk: usize,
        /// True when the next `write` should return `WouldBlock` (forcing the loop into
        /// `wait_writable` → real `poll(POLLOUT)`), false when it should accept up to
        /// `chunk` bytes.  Toggled after every call so partials and waits alternate.
        block_next: bool,
    }
    impl ChunkedSocket {
        fn new(stream: TcpStream, chunk: usize) -> Self {
            ChunkedSocket {
                stream,
                chunk: chunk.max(1),
                block_next: false,
            }
        }
    }
    impl Write for ChunkedSocket {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.block_next {
                // Force the loop into wait_writable → real poll(POLLOUT) on the real fd.
                self.block_next = false;
                return Err(std::io::Error::from(std::io::ErrorKind::WouldBlock));
            }
            let end = self.chunk.min(buf.len());
            // Write a capped slice to the REAL socket.  The underlying non-blocking
            // stream may itself return `WouldBlock` if the kernel buffer is full; pass
            // that straight through (the loop handles it identically).
            let m = self.stream.write(&buf[..end])?;
            // After a successful partial write, force the next call to wait so the loop
            // exercises a genuine poll(POLLOUT)-gated resume (not back-to-back accepting
            // writes, which would not count as resumes — errhandling-1).
            if m > 0 {
                self.block_next = true;
            }
            Ok(m)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.stream.flush()
        }
    }

    /// Host counterpart of the device `poll_writable`: block on `poll(POLLOUT)` until
    /// the socket `fd` is writable or `deadline` elapses, mapping the result to
    /// [`Writable`].  Confined to this test module (the production waiter is the
    /// device crate's ESP `poll_writable`).
    fn poll_writable_host(fd: i32, deadline: Instant) -> Writable {
        let now = Instant::now();
        let remaining = deadline.saturating_duration_since(now);
        // poll(2) takes a c_int millisecond timeout; clamp to i32::MAX and round up so
        // a sub-ms remaining budget still polls at least 1 ms rather than spinning.
        let timeout_ms: i32 = if remaining.is_zero() {
            0
        } else {
            let ms = remaining.as_millis().max(1);
            ms.min(i32::MAX as u128) as i32
        };
        let mut pfd = libc::pollfd {
            fd,
            events: libc::POLLOUT,
            revents: 0,
        };
        // SAFETY: `pfd` is a single valid pollfd for the duration of the call; `poll`
        // reads `nfds=1` entries and writes `revents`.
        let rc = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
        if rc < 0 {
            return Writable::Fault(std::io::Error::last_os_error());
        }
        if rc == 0 {
            return Writable::TimedOut;
        }
        if pfd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
            return Writable::Fault(std::io::Error::other(format!(
                "poll(POLLOUT) reported error revents 0x{:x}",
                pfd.revents
            )));
        }
        if pfd.revents & libc::POLLOUT != 0 {
            return Writable::Ready;
        }
        // Spurious wakeup: poll reported rc>0 with neither POLLOUT nor an error bit set
        // on a localhost socket — unexpected enough to log for diagnosis so a flaky CI
        // run is traced to "bad revents", not a misleading "resume_cycles==0"/timeout
        // (errhandling-2).  Treat as not-yet-writable.
        eprintln!(
            "poll_writable_host: spurious wakeup, unexpected revents=0x{:x}",
            pfd.revents
        );
        Writable::TimedOut
    }

    /// Drive one host-socket send through `write_frame_classified_at` against a real
    /// localhost server, with the client's `write` capped at `chunk` bytes (via
    /// [`ChunkedSocket`]) so the loop takes several real `poll(POLLOUT)`-gated resumes.
    /// The server drains to the full encoded-frame length and the test asserts
    /// byte-exact delivery.  Returns `(outcome, resume_cycles)`.
    fn run_host_socket_send(chunk: usize) -> (SendOutcome, u32) {
        let frame = audio_frame(MAX_AUDIO_PAYLOAD / 2);
        let mut encoded = vec![0u8; MAX_FRAME_BYTES + 2];
        let n = encode_frame(&frame, &mut encoded).expect("encode frame for length/compare");
        let expected: Vec<u8> = encoded[..n].to_vec();

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral localhost port");
        let server_addr = listener.local_addr().expect("server local addr");

        // Server-drain thread: accept, then drain to the full encoded length.  A read
        // timeout means a client that never sends the tail makes the loop-read return
        // short → the byte-exact compare fails, not a hang (design §4 "Read/write
        // deadlock on the byte-exact compare").  It drains concurrently with the
        // client's resume loop, so the client's `poll(POLLOUT)` waits are satisfied by a
        // genuinely-writable socket.
        let server = std::thread::spawn(move || -> (Vec<u8>, bool, bool) {
            let (mut conn, _peer) = listener.accept().expect("server accept");
            conn.set_read_timeout(Some(SERVER_READ_TIMEOUT))
                .expect("arm server read timeout");
            let mut received = Vec::with_capacity(n);
            let mut rbuf = vec![0u8; 256];
            let mut read_timed_out = false;
            let mut peer_closed_early = false;
            while received.len() < n {
                match conn.read(&mut rbuf) {
                    Ok(0) => {
                        // Peer closed before sending the whole frame.  Flag it so the
                        // post-join assert blames "client closed mid-frame" rather than
                        // the byte-exact "dropped tail" message, which would point a
                        // regression diagnosis at the wrong path (errhandling-3).
                        peer_closed_early = true;
                        break;
                    }
                    Ok(m) => received.extend_from_slice(&rbuf[..m]),
                    Err(ref e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            || e.kind() == std::io::ErrorKind::TimedOut =>
                    {
                        // Read timeout: a regressed client stalled and never sent the
                        // tail.  Flag it so the post-join assert blames "client stalled,
                        // server timed out" rather than the byte-exact "dropped tail"
                        // message, which would point a regression diagnosis at the wrong
                        // path (slop-3).
                        read_timed_out = true;
                        break;
                    }
                    Err(e) => panic!("server read error: {e}"),
                }
            }
            (received, read_timed_out, peer_closed_early)
        });

        // Client side (under test): a real non-blocking TcpStream wrapped in the
        // chunk-capping adapter, driven through the §2.3a seam.
        let stream = TcpStream::connect(server_addr).expect("client connect to server");
        stream
            .set_nonblocking(true)
            .expect("client set_nonblocking");
        let fd = stream.as_raw_fd();
        let mut client = ChunkedSocket::new(stream, chunk);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let frame_ceiling = Instant::now() + TEST_FRAME_CEILING;
        let (result, resume_cycles) = write_frame_classified_at(
            &mut client,
            &frame,
            &mut buf,
            |deadline| poll_writable_host(fd, deadline),
            frame_ceiling,
            Instant::now,
        );
        let outcome = result.expect("real-socket send must complete (resume), not error");

        // Drop the client so the server's drain loop sees EOF after the full frame.
        drop(client);
        let (received, read_timed_out, peer_closed_early) = server.join().expect("server thread");
        assert!(
            !peer_closed_early,
            "server saw EOF before the full frame arrived ({}/{} bytes): the client closed \
             the connection mid-frame (e.g. an unhandled write Err or a drop-ordering bug) \
             — distinct from a stall (read_timed_out) and from a byte-mismatch, which the \
             next assert checks (errhandling-3)",
            received.len(),
            n
        );
        assert!(
            !read_timed_out,
            "server read timed out before the full frame arrived: the client stalled \
             mid-frame and never sent the tail (a regressed resume loop) — distinct from \
             a byte-mismatch, which the next assert checks (slop-3)"
        );
        assert_eq!(
            received, expected,
            "byte-exact, in-order delivery on a real socket: no duplicated prefix, no \
             dropped tail (the invariant the deleted BackpressureDesynced protected)"
        );
        (outcome, resume_cycles)
    }

    /// **Resume to completion + no desync on a real socket (the core integration
    /// proof, partial-write-fix design §4 assertions 1 & 2).**  The capped-`write`
    /// adapter forces a partial count; the loop must resume the tail through the real
    /// `poll(POLLOUT)` on the real fd to `Sent` (`resume_cycles ≥ 1`) and the server
    /// must receive the frame byte-for-byte.
    #[test]
    fn host_socket_partial_write_resumes_to_sent_and_no_desync() {
        // 256-byte cap on a ~1.3 KB frame ⇒ several capped writes, each followed by a
        // real poll(POLLOUT)-gated resume.
        let (outcome, resume_cycles) = run_host_socket_send(256);
        assert_eq!(
            outcome,
            SendOutcome::Sent,
            "the frame must resume to completion on a real socket"
        );
        assert!(
            resume_cycles >= 1,
            "at least one genuine partial write must have gone through poll(POLLOUT) and \
             resumed (a frame that fit immediately would be Sent with resume_cycles==0); \
             got {resume_cycles}.  If this fails the adapter stopped forcing partials — \
             the test proved nothing and must FAIL loudly, not silently pass (design §4 \
             'Fragmentation-count variance' / the HIL never-withheld guard)"
        );
    }

    /// **Many-partial robustness on a real socket (companion case, design §4
    /// assertion 3).**  A smaller `write` cap drives several partial writes for one
    /// frame; the received bytes must still equal the encoded frame exactly and the
    /// outcome is `Sent`.  This exercises the multi-resume path against a real stack
    /// (the deterministic many-cycle unit test proves the per-step budget *reset*; this
    /// proves the per-step *byte-stream correctness* on a real socket).
    #[test]
    fn host_socket_many_partials_stay_byte_exact() {
        // 64-byte cap ⇒ ~20 capped writes, each a real poll(POLLOUT)-gated resume.  The
        // byte-exact compare is inside `run_host_socket_send` and holds regardless of
        // how many partials occur (the always-deterministic property — not an over-fit
        // count).
        let (outcome, resume_cycles) = run_host_socket_send(64);
        assert_eq!(outcome, SendOutcome::Sent);
        // A 64-byte cap on a ~1.3 KB frame yields ceil(n/64)-1 ≈ 19 poll(POLLOUT)-gated
        // resumes.  Pin the floor at 10 (well below the expected ~19, well above the
        // companion `>= 1` test) so this case actually catches "the adapter stopped
        // fragmenting / merged chunks" — a `>= 2` floor was satisfied even by a single
        // batch and was no tighter than the companion test (test-1).
        assert!(
            resume_cycles >= 10,
            "a 64-byte cap on a ~1.3 KB frame must force ~19 poll(POLLOUT)-gated resumes \
             (got {resume_cycles}); a count this low means the adapter stopped fragmenting \
             and the multi-resume path was not exercised — the test proved nothing"
        );
    }
}

#[cfg(test)]
mod cross_iteration_tests {
    // ── FrameWriteState cross-iteration cursor tests (design §2.4 / §4 test #4) ─────
    //
    // The `FrameWriteState` API lifts the `written` cursor + two-tier budget +
    // classification out of `write_frame_classified`'s internal `wait_writable` loop so
    // the event loop drives the *same* logic one `POLLOUT`-gated attempt at a time, with
    // the cursor persisting across iterations (design §2.4).  These tests pin the migrated
    // semantics §4 test #4 calls for, with a hand-advanced fake clock so the budget/ceiling
    // arithmetic is deterministic off-target (no real sleeps, no socket — the real
    // `poll(POLLOUT)` is the HIL self-test's job, §4 test #1):
    //  (a) the per-wait WRITE_TIMEOUT_MS budget RESETS on forward progress (a peer granting
    //      one byte per sub-budget window stays alive — NOT a `last_flush` deadline);
    //  (b) the per-frame FRAME_WALL_CLOCK_MAX_MS ceiling still caps total in-flight time;
    //  (c) `written == 0` at budget/ceiling elapse → BackpressureAligned (keep socket) and
    //      `written > 0` → fatal Err (clear socket).

    use super::{
        FrameWriteState, SendOutcome, StepOutcome, FRAME_WALL_CLOCK_MAX_MS, WRITE_TIMEOUT_MS,
    };
    use crate::test_support::audio_frame;
    use crate::wire::{encode_frame, AUDIO_SAMPLES_PER_FRAME, MAX_FRAME_BYTES};
    use std::cell::Cell;
    use std::io::Write;
    use std::time::{Duration, Instant};

    /// A `Write` driven by a scripted per-call admission count, mirroring the
    /// `ScriptedWriter` in the sibling module but with one-step semantics: each entry is
    /// the byte count this `write` accepts (0 → returns `WouldBlock`).  An empty/exhausted
    /// script accepts the whole offered slice (drains the frame).
    struct StepWriter {
        steps: std::collections::VecDeque<usize>,
        written: usize,
    }
    impl StepWriter {
        fn new(steps: Vec<usize>) -> Self {
            StepWriter {
                steps: steps.into(),
                written: 0,
            }
        }
    }
    impl Write for StepWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            match self.steps.pop_front() {
                Some(0) => Err(std::io::Error::from(std::io::ErrorKind::WouldBlock)),
                Some(k) => {
                    let m = k.min(buf.len());
                    self.written += m;
                    Ok(m)
                }
                None => {
                    self.written += buf.len();
                    Ok(buf.len())
                }
            }
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn frame_len() -> usize {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        encode_frame(&frame, &mut vec![0u8; MAX_FRAME_BYTES + 2]).expect("encode for length")
    }

    /// A frame that fits in one attempt → `WroteWhole`, no resume cycle, cursor at `n`.
    #[test]
    fn step_completes_in_one_attempt() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let n = frame_len();
        let now = Instant::now;
        let mut st = FrameWriteState::begin(&frame, &mut buf, now).expect("begin");
        let mut w = StepWriter::new(vec![]); // accept the whole frame
        let out = st.step_writable(&mut w, &buf, now).expect("step");
        assert_eq!(out, StepOutcome::WroteWhole);
        assert_eq!(st.written(), n, "cursor reached the full frame");
        assert_eq!(
            st.resume_cycles(),
            0,
            "a frame that fit immediately never resumed"
        );
        assert_eq!(w.written, n, "the whole frame was written to the socket");
    }

    /// The cursor persists across attempts: a partial then a completing attempt → the
    /// second attempt writes only the unsent tail (no re-issued prefix), reaching
    /// `WroteWhole`.  This is the genuinely-new "cursor survives across loop iterations"
    /// property (design §2.4 / §4 test #4 / §5 risk #5 framing-across-iterations).
    #[test]
    fn cursor_persists_across_attempts_no_reissue() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let n = frame_len();
        let now = Instant::now;
        let mut st = FrameWriteState::begin(&frame, &mut buf, now).expect("begin");
        // Attempt 1: accept 10 bytes. Attempt 2 (no script): accept the rest.
        let mut w = StepWriter::new(vec![10]);
        let out1 = st.step_writable(&mut w, &buf, now).expect("step 1");
        assert_eq!(out1, StepOutcome::WrotePartial);
        assert_eq!(st.written(), 10, "cursor advanced to the partial prefix");
        assert_eq!(
            st.resume_cycles(),
            1,
            "the advancing first attempt is a resume cycle"
        );
        let out2 = st.step_writable(&mut w, &buf, now).expect("step 2");
        assert_eq!(out2, StepOutcome::WroteWhole);
        assert_eq!(st.written(), n, "cursor reached the full frame");
        assert_eq!(
            w.written, n,
            "exactly n bytes written total — the prefix was NOT re-issued"
        );
    }

    /// A `write` that returns `WouldBlock` (send buffer full) → `WouldBlock`, cursor
    /// unchanged, never parks.  A subsequent attempt resumes from the same offset.
    #[test]
    fn wouldblock_leaves_cursor_unchanged() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let now = Instant::now;
        let mut st = FrameWriteState::begin(&frame, &mut buf, now).expect("begin");
        // Attempt 1: WouldBlock (0). Attempt 2: accept 5. Attempt 3 (no script): finish.
        let mut w = StepWriter::new(vec![0, 5]);
        let out1 = st.step_writable(&mut w, &buf, now).expect("step 1");
        assert_eq!(out1, StepOutcome::WouldBlock);
        assert_eq!(st.written(), 0, "WouldBlock advanced nothing");
        assert_eq!(st.resume_cycles(), 0, "no byte accepted → no resume cycle");
        let out2 = st.step_writable(&mut w, &buf, now).expect("step 2");
        assert_eq!(out2, StepOutcome::WrotePartial);
        assert_eq!(st.written(), 5);
        assert_eq!(
            st.resume_cycles(),
            1,
            "one POLLOUT-gated advancing wake after the WouldBlock, not two — the \
             non-advancing WouldBlock attempt must NOT be counted as a resume cycle (test-2)"
        );
        let out3 = st.step_writable(&mut w, &buf, now).expect("step 3");
        assert_eq!(out3, StepOutcome::WroteWhole);
        assert_eq!(
            st.resume_cycles(),
            1,
            "completing the frame does not add a resume cycle — still exactly one (test-2)"
        );
    }

    /// **(a) The per-wait budget RESETS on forward progress** (design §2.4 tier 1; the
    /// correction to the "relocate to a `last_flush` deadline" framing).  A hand-advanced
    /// fake clock advances by most-of-a-budget between each progressing attempt; the budget
    /// must re-arm every step so `check_deadlines` never declares a stall even though total
    /// elapsed time far exceeds one `WRITE_TIMEOUT_MS`.  A reset-once-but-not-again
    /// regression would have `check_deadlines` fire a stall on the second window → FAIL.
    #[test]
    fn budget_resets_on_forward_progress() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let n = frame_len();
        let base = Instant::now();
        let clock = Cell::new(base);
        let now = || clock.get();
        // Per-frame ceiling far in the future so only the per-wait budget is in play (the
        // ceiling has its own dedicated test below).
        let far_ceiling = base + Duration::from_secs(3600);
        let mut st = FrameWriteState::begin_at(&frame, &mut buf, now, far_ceiling).expect("begin");
        // Carve into small accepts so many sub-budget windows elapse.
        const K: usize = 8;
        let edges = (n - K) / K; // leave the last chunk for the no-script finishing write
                                 // `edges` capped accepts, then an empty script: the finishing `step_writable` hits
                                 // the no-script "accept everything" path and drains the remainder to `WroteWhole`.
        let script: Vec<usize> = (0..edges).map(|_| K).collect();
        let mut w = StepWriter::new(script);
        for step_i in 0..edges {
            // Advance the clock by most-of-a-budget BEFORE the attempt: if the budget had
            // not re-armed on the prior progress, check_deadlines here would see it elapsed
            // with no further progress and stall.
            clock.set(clock.get() + Duration::from_millis(WRITE_TIMEOUT_MS - 1));
            // Housekeeping check first (as the loop does): must NOT stall — the prior
            // attempt's progress re-armed the budget.
            assert!(
                st.check_deadlines(now).is_none(),
                "budget must not stall after forward progress (step {step_i}): the per-wait \
                 budget re-arms each progressing attempt, so total elapsed time far exceeding \
                 one WRITE_TIMEOUT_MS is not a stall (design §2.4)"
            );
            let out = st
                .step_writable(&mut w, &buf, now)
                .expect("progressing step");
            assert_eq!(
                out,
                StepOutcome::WrotePartial,
                "each capped accept is partial"
            );
        }
        // Finish the frame.
        let out = st.step_writable(&mut w, &buf, now).expect("finishing step");
        assert_eq!(out, StepOutcome::WroteWhole);
        assert_eq!(st.written(), n);
        assert!(
            (st.resume_cycles() as usize) >= edges,
            "every progressing attempt counts as a resume cycle (≥{edges}); got {}",
            st.resume_cycles()
        );
    }

    /// **(c) `written == 0` at per-wait-budget elapse → BackpressureAligned (keep
    /// socket).**  No byte ever accepted; advance the fake clock past the per-wait budget
    /// with the cursor at 0 → `check_deadlines` returns the frame-aligned outcome.
    #[test]
    fn budget_elapsed_written_zero_is_aligned_keep_socket() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let base = Instant::now();
        let clock = Cell::new(base);
        let now = || clock.get();
        let mut st = FrameWriteState::begin(&frame, &mut buf, now).expect("begin");
        // One WouldBlock attempt (cursor stays 0), then the budget elapses with no progress.
        let mut w = StepWriter::new(vec![0]);
        let out = st.step_writable(&mut w, &buf, now).expect("step");
        assert_eq!(out, StepOutcome::WouldBlock);
        // Advance just past the per-wait budget.
        clock.set(base + Duration::from_millis(WRITE_TIMEOUT_MS + 1));
        let verdict = st
            .check_deadlines(now)
            .expect("budget elapsed → terminal classification");
        assert_eq!(
            verdict.expect("written==0 is a non-error outcome"),
            SendOutcome::BackpressureAligned,
            "no byte written at budget elapse → frame-aligned, keep the socket (design §2.4)"
        );
    }

    /// **(c) `written > 0` at per-wait-budget elapse → fatal Err (clear socket).**  A
    /// prefix was accepted, then no further progress for a full budget → mid-tail dead.
    #[test]
    fn budget_elapsed_mid_tail_is_fatal_clear_socket() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let base = Instant::now();
        let clock = Cell::new(base);
        let now = || clock.get();
        let mut st = FrameWriteState::begin(&frame, &mut buf, now).expect("begin");
        // Accept 6, then WouldBlock; the budget then elapses with the cursor stuck at 6.
        let mut w = StepWriter::new(vec![6, 0]);
        assert_eq!(
            st.step_writable(&mut w, &buf, now).expect("partial"),
            StepOutcome::WrotePartial
        );
        // The progressing attempt re-armed the budget at this clock; advance past the
        // re-armed budget WITHOUT further progress (one WouldBlock attempt, cursor stuck).
        assert_eq!(
            st.step_writable(&mut w, &buf, now).expect("wouldblock"),
            StepOutcome::WouldBlock
        );
        clock.set(clock.get() + Duration::from_millis(WRITE_TIMEOUT_MS + 1));
        let verdict = st
            .check_deadlines(now)
            .expect("budget elapsed mid-tail → terminal");
        let err = verdict.expect_err("written>0 at budget elapse is fatal (mid-tail dead)");
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::TimedOut,
            "mid-tail dead surfaces as TimedOut Err so the call site clears the socket"
        );
        assert!(st.written() > 0, "the accepted prefix is committed");
    }

    /// **(b) The per-frame ceiling caps total in-flight time even while the per-wait
    /// budget keeps re-arming** (design §2.4 tier 2 / mic-ring-lap watchdog).  A frame that
    /// makes a tiny bit of progress every sub-budget window would never stall on tier 1
    /// alone; the absolute `FRAME_WALL_CLOCK_MAX_MS` ceiling must terminate it.  With
    /// `written > 0` the ceiling outcome is the mid-tail-dead fatal `Err`.
    #[test]
    fn ceiling_caps_total_in_flight_time() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let base = Instant::now();
        let clock = Cell::new(base);
        let now = || clock.get();
        let mut st = FrameWriteState::begin(&frame, &mut buf, now).expect("begin");
        // One small accept so the cursor is > 0 (mid-tail), then jump the clock past the
        // per-frame ceiling.  Even though the per-wait budget re-armed on that progress,
        // the absolute ceiling must fire.
        let mut w = StepWriter::new(vec![4]);
        assert_eq!(
            st.step_writable(&mut w, &buf, now).expect("partial"),
            StepOutcome::WrotePartial
        );
        clock.set(base + Duration::from_millis(FRAME_WALL_CLOCK_MAX_MS + 1));
        let verdict = st
            .check_deadlines(now)
            .expect("per-frame ceiling elapsed → terminal even with budget re-armed");
        let err = verdict.expect_err("ceiling with written>0 is mid-tail-dead fatal Err");
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
    }

    /// A `Write` that *captures* every byte handed to it, so a test can assert the exact
    /// wire bytes a frame produced.  Unlike `StepWriter` (which only counts), this keeps the
    /// stream so the cursor-discard test can prove the fresh frame's bytes start at offset 0
    /// with no stale prefix.  An optional first-call cap forces a partial write; subsequent
    /// calls accept the whole offered slice.
    struct CapturingWriter {
        captured: Vec<u8>,
        first_cap: Option<usize>,
    }
    impl CapturingWriter {
        fn new(first_cap: Option<usize>) -> Self {
            CapturingWriter {
                captured: Vec::new(),
                first_cap,
            }
        }
    }
    impl Write for CapturingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let take = match self.first_cap.take() {
                Some(cap) => cap.min(buf.len()),
                None => buf.len(),
            };
            self.captured.extend_from_slice(&buf[..take]);
            Ok(take)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Build an Audio frame tagged with a distinct `segment_id` so two frames produce
    /// *different* header bytes — the distinguisher the cursor-discard test relies on (a
    /// stale tail of frame A prepended to frame B would show up as B's captured stream not
    /// equalling `encode_frame(B)`).
    fn audio_frame_tagged(seg: u32) -> crate::wire::StreamFrame {
        let mut frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        if let crate::wire::StreamFrame::Audio(ref mut a) = frame {
            a.segment_id = seg;
        }
        frame
    }

    /// **Cursor discard on teardown/reconnect** (design §4 test #2 part (b); §5 risk #5).
    /// Part (a) — the cursor persisting *across* iterations with no re-issue — is covered by
    /// `cursor_persists_across_attempts_no_reissue` above.  This test covers the genuinely
    /// distinct discard obligation: a `FrameWriteState` holding a mid-frame `written > 0`
    /// must be **dropped** on teardown, never carried onto the fresh socket — a stale tail
    /// would corrupt the first frame of the next connection.
    ///
    /// The `FrameWriteState` owns no socket, so dropping it *is* the discard (the type's
    /// doc-contract); the structural guarantee in the loop is that the per-segment locals are
    /// dropped on every `break 'stream` and `outbound` is reset to `None` on reconnect.  This
    /// pins that guarantee as a byte-exact regression: frame A is partially written, the
    /// state is dropped (simulated teardown), then a *fresh* `FrameWriteState` for a distinct
    /// frame B drains onto a *fresh* capturing writer — and B's captured bytes must equal
    /// `encode_frame(B)` exactly, starting at B's header at offset 0 with none of A's
    /// in-flight tail prepended.  A regression that carried A's cursor/buffer onto the new
    /// connection would prepend A's unsent tail (or A's header) and fail the byte-equality.
    #[test]
    fn cursor_discarded_on_teardown_fresh_frame_starts_at_offset_zero() {
        let now = Instant::now;

        // ── Connection 1: begin frame A, accept a partial prefix, then "tear down". ──
        // The per-segment state lives in an inner scope; teardown/reconnect is modelled by
        // letting that scope END — the loop drops the per-segment `FrameWriteState` (and its
        // buffer) rather than flushing the held tail onto a fresh socket (no explicit `drop`
        // call, which clippy flags on these non-`Drop` types; scope exit is the discard).
        {
            let frame_a = audio_frame_tagged(0xAAAA_AAAA);
            let mut buf_a = vec![0u8; MAX_FRAME_BYTES + 2];
            let mut st_a = FrameWriteState::begin(&frame_a, &mut buf_a, now).expect("begin A");
            // Accept only the first 7 bytes of A — the state now holds a mid-frame tail
            // (`written == 7`, the dangerous stale-cursor condition of §5 risk #5).
            let mut w_a = CapturingWriter::new(Some(7));
            assert_eq!(
                st_a.step_writable(&mut w_a, &buf_a, now)
                    .expect("partial A"),
                StepOutcome::WrotePartial
            );
            assert_eq!(
                st_a.written(),
                7,
                "frame A left mid-flight with a held tail"
            );
            // Scope ends here: `st_a` (the mid-frame cursor), `w_a`, and `buf_a` are all
            // dropped — modelling the teardown where the loop discards the in-flight state
            // instead of carrying it onto the next connection.
        }

        // ── Connection 2: a FRESH FrameWriteState for a DISTINCT frame B on a fresh socket. ──
        let frame_b = audio_frame_tagged(0xBBBB_BBBB);
        let mut buf_b = vec![0u8; MAX_FRAME_BYTES + 2];
        let mut st_b = FrameWriteState::begin(&frame_b, &mut buf_b, now).expect("begin B");
        let mut w_b = CapturingWriter::new(None); // accept the whole frame in one attempt
        assert_eq!(
            st_b.step_writable(&mut w_b, &buf_b, now).expect("write B"),
            StepOutcome::WroteWhole
        );

        // The fresh socket must carry exactly B's wire bytes — B's header at offset 0, no
        // stale tail from A prepended.
        let mut expected = vec![0u8; MAX_FRAME_BYTES + 2];
        let nb = encode_frame(&frame_b, &mut expected).expect("encode B");
        assert_eq!(
            w_b.captured,
            expected[..nb],
            "the fresh connection's first frame must be byte-exact frame B from offset 0 — \
             a discarded cursor leaves no stale tail (design §4 test #2(b) / §5 risk #5)"
        );
    }

    /// The `write_attempts` / `would_blocks` forensic counters track every `step_writable`
    /// call and every no-byte attempt — the Defect-2 write-path bookkeeping the mid-tail-dead
    /// post-mortem reports.
    #[test]
    fn counters_track_attempts_and_would_blocks() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let now = Instant::now;
        let mut st = FrameWriteState::begin(&frame, &mut buf, now).expect("begin");
        // WouldBlock, partial(5), WouldBlock, then finish (no script).
        let mut w = StepWriter::new(vec![0, 5, 0]);
        assert_eq!(
            st.step_writable(&mut w, &buf, now).expect("s1"),
            StepOutcome::WouldBlock
        );
        assert_eq!(
            st.step_writable(&mut w, &buf, now).expect("s2"),
            StepOutcome::WrotePartial
        );
        assert_eq!(
            st.step_writable(&mut w, &buf, now).expect("s3"),
            StepOutcome::WouldBlock
        );
        assert_eq!(
            st.step_writable(&mut w, &buf, now).expect("s4"),
            StepOutcome::WroteWhole
        );
        assert_eq!(st.write_attempts(), 4, "one attempt per step_writable call");
        assert_eq!(st.would_blocks(), 2, "exactly the two no-byte attempts");
    }

    /// The spin guard trips only on a run of `SPIN_GUARD_THRESHOLD` *consecutive*
    /// zero-progress attempts, and any accepted byte clears the run.
    #[test]
    fn spin_guard_trips_on_consecutive_no_progress_and_resets_on_a_byte() {
        use super::SPIN_GUARD_THRESHOLD;
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let now = Instant::now;
        let mut st = FrameWriteState::begin(&frame, &mut buf, now).expect("begin");

        // One byte short of the threshold, then a single accepted byte clears the run.
        let mut script = vec![0usize; SPIN_GUARD_THRESHOLD as usize - 1];
        script.push(1);
        script.extend(std::iter::repeat_n(0usize, SPIN_GUARD_THRESHOLD as usize));
        let mut w = StepWriter::new(script);

        for i in 1..SPIN_GUARD_THRESHOLD {
            st.step_writable(&mut w, &buf, now).expect("no-progress");
            assert!(
                !st.spin_guard_tripped(),
                "{i} consecutive no-progress attempts is under the threshold"
            );
        }
        st.step_writable(&mut w, &buf, now).expect("one byte");
        assert!(
            !st.spin_guard_tripped(),
            "an accepted byte resets the consecutive run"
        );
        for _ in 0..SPIN_GUARD_THRESHOLD - 1 {
            st.step_writable(&mut w, &buf, now).expect("no-progress");
            assert!(!st.spin_guard_tripped(), "run restarted from zero");
        }
        st.step_writable(&mut w, &buf, now).expect("no-progress");
        assert!(
            st.spin_guard_tripped(),
            "the full threshold run of zero-progress attempts trips the guard"
        );

        st.reset_spin_guard();
        assert!(
            !st.spin_guard_tripped(),
            "reset_spin_guard clears the trip (the pump's backoff-expiry re-arm)"
        );
    }

    /// The mid-tail-dead error string embeds the frame post-mortem — the enriched message
    /// that reaches the device log via the streamer warn's `{:?}` (delta-2 §5 D1).
    #[test]
    fn mid_tail_dead_error_embeds_post_mortem() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let base = Instant::now();
        let clock = Cell::new(base);
        let now = || clock.get();
        let mut st = FrameWriteState::begin(&frame, &mut buf, now).expect("begin");
        // Accept 6 (partial), then WouldBlock; the budget elapses with the cursor stuck at 6.
        let mut w = StepWriter::new(vec![6, 0]);
        assert_eq!(
            st.step_writable(&mut w, &buf, now).expect("partial"),
            StepOutcome::WrotePartial
        );
        assert_eq!(
            st.step_writable(&mut w, &buf, now).expect("wouldblock"),
            StepOutcome::WouldBlock
        );
        clock.set(base + Duration::from_millis(WRITE_TIMEOUT_MS + 1));
        let verdict = st
            .check_deadlines(now)
            .expect("budget elapsed mid-tail → terminal");
        let err = verdict.expect_err("written>0 at budget elapse is fatal");
        let msg = err.to_string();
        for needle in [
            "written=6/",
            "resumes=",
            "attempts=",
            "would_blocks=",
            "elapsed_ms=",
        ] {
            assert!(
                msg.contains(needle),
                "post-mortem must embed `{needle}`; got: {msg}"
            );
        }
    }

    /// The mid-tail-dead post-mortem counter block survives the log transport's 200-byte
    /// `LogFrame.message` cap: with worst-case-width counters, the complete counter block
    /// (up to and including its closing `]`) lands within the first 200 bytes of the *full*
    /// streamer warn line — the prefix + `io::Error` `{:?}` wrapper modeled exactly, not just
    /// the bare error string.  This is what keeps the "self-diagnosing without a rerun"
    /// promise from silently re-breaking: a wider counter field or fatter prose that pushed
    /// the `]` past byte 200 (so the forensics truncate on the wire) FAILs here.
    #[test]
    fn mid_tail_dead_post_mortem_fits_log_cap() {
        // Worst-case counter widths: cursor near a full frame, a multi-digit resume count,
        // 7-digit write_attempts/would_blocks (a deliberately pessimistic width — the spin
        // guard's backoff holds real counts far below this), 4-digit elapsed_ms (the
        // per-frame ceiling is ~1000 ms).
        let msg = super::mid_tail_dead_message(4096, 4096, 9_999, 9_999_999, 9_999_999, 1_000);
        let err = std::io::Error::new(std::io::ErrorKind::TimedOut, msg);
        // Model the exact device transport: the streamer warn (respeaker-pod
        // `src/streamer.rs`, the `outbound write ceiling/budget elapsed mid-tail` warn) wraps
        // this error in `{:?}` with the segment id (`Custom { kind: TimedOut, error: "…" }`);
        // the LogFrame caps the whole message at 200 bytes. The segment id is `segment_counter`,
        // an unbounded wrapping `u32` — model its worst case (`u32::MAX`, 10 digits), not a
        // short id, so the cap check reflects a long-lived device.
        let warn_line = format!(
            "streamer: outbound write ceiling/budget elapsed mid-tail (seg {}): {:?} — dropping segment, clearing socket",
            u32::MAX, err,
        );
        let close = warn_line
            .find(']')
            .expect("the counter block's closing bracket must be present");
        assert!(
            close < 200,
            "the complete forensic counter block must end within the 200-byte LogFrame cap; \
             its `]` is at byte {close} of the warn line: {warn_line}"
        );
        // Every counter field must precede that bracket — inside the block, not spilled into
        // the truncatable prose.
        let block = &warn_line[..=close];
        for needle in [
            "written=4096/4096",
            "resumes=9999",
            "attempts=9999999",
            "would_blocks=9999999",
            "elapsed_ms=1000",
        ] {
            assert!(
                block.contains(needle),
                "counter block must carry `{needle}` before its closing bracket; block: {block}"
            );
        }
    }

    /// `next_deadline` reports the earlier of the per-wait budget and the per-frame
    /// ceiling — the value the event loop folds into `timeout_to_next_deadline` (design
    /// §2.6) so the give-up fires on time while POLLOUT never becomes writable.
    #[test]
    fn next_deadline_is_earlier_of_budget_and_ceiling() {
        let frame = audio_frame(AUDIO_SAMPLES_PER_FRAME);
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let base = Instant::now();
        let st = FrameWriteState::begin(&frame, &mut buf, || base).expect("begin");
        // WRITE_TIMEOUT_MS (750) < FRAME_WALL_CLOCK_MAX_MS (1000), so the per-wait budget is
        // the earlier deadline at frame start.
        let expected = base + Duration::from_millis(WRITE_TIMEOUT_MS);
        assert_eq!(
            st.next_deadline(),
            expected,
            "at frame start the per-wait budget is the earlier deadline"
        );
    }
}
