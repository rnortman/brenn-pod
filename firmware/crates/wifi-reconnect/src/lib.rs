//! WiFi supervisor backoff logic.
//!
//! This crate is host-buildable (no ESP-IDF dependencies) so the backoff helpers
//! can be unit-tested in the host lane without the Xtensa toolchain.
//!
//! # Overview
//!
//! [`Backoff`] tracks the state for exponential backoff with jitter between WiFi
//! re-association attempts.  The supervisor calls [`Backoff::record_failure`] on each
//! failed attempt and [`Backoff::record_success`] (or [`Backoff::record_provisioning`])
//! on success or a new provisioning signal to reset the backoff to its floor.
//!
//! [`Backoff::next_wait_secs`] returns the delay to wait *between* attempts (in whole
//! seconds), folded into the supervisor's single `recv_timeout` call so backoff is
//! doorbell-interruptible.
//!
//! [`compute_wake_deadline_secs`] gives the absolute time the supervisor intends to act,
//! and [`compute_wait_secs`] is that deadline minus now — the `recv_timeout` duration.
//!
//! [`should_absorb_wake`] decides whether a doorbell wake that arrived before the
//! deadline should be resumed-from rather than acted on, which is what keeps attempt
//! spacing intact when a failing association rings its own doorbell.

#![no_std]

/// Backoff floor (seconds): recover fast from a transient blip.
pub const BACKOFF_FLOOR_SECS: u64 = 2;

/// Backoff cap (seconds): do not spin the radio on a genuine credential mismatch.
pub const BACKOFF_CAP_SECS: u64 = 60;

/// Slow-lane latch threshold: after this many consecutive failures the supervisor
/// logs a distinct "check credentials/AP" warning.  Does not change the cap
/// behaviour — the cap is already applied after several doublings — but triggers a
/// single diagnostic log so the condition is visible.
pub const SLOW_LANE_THRESHOLD: u32 = 10;

/// Periodic health-tick interval (seconds): backstop cadence for event-invisible
/// losses.  Combined with one ~15 s association attempt, worst-case event-missed
/// outage is ~45 s.
pub const TICK_INTERVAL_SECS: u64 = 30;

/// Number of consecutive fully-failed gateway probes required before the supervisor
/// forces a radio re-association on an apparently-up link.
///
/// Each probe already tolerates 1–2 lost ICMP replies (`received >= 1` of 3 echoes —
/// see the production `ping_reachable` configuration).  This threshold adds a second
/// layer: three *consecutive* probes that receive zero replies in three attempts each
/// before the re-associate fires.  On the 30 s tick the effective detection time is
/// `GW_UNREACHABLE_THRESHOLD × TICK_INTERVAL_SECS` ≈ 90 s of sustained total gateway
/// loss — deliberately conservative so a brief AP hiccup (all three echoes dropped in
/// one probe window) does not bounce a healthy radio.
///
/// A lower value (e.g. 1) would bounce the radio on a single unlucky ~3 s window;
/// 3 is the smallest value that treats a fully-failed single probe as noise.
pub const GW_UNREACHABLE_THRESHOLD: u32 = 3;

/// Jitter fraction numerator / denominator (±25%).
const JITTER_NUM: u64 = 25;
const JITTER_DEN: u64 = 100;

/// Backoff state for the WiFi supervisor.
///
/// All fields are monotonic counters or bounded values; no heap allocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Backoff {
    /// Current backoff interval in whole seconds (FIFO: floor → doubled → cap).
    current_secs: u64,
    /// Count of consecutive failures since the last success or provisioning signal.
    consecutive_failures: u32,
}

impl Backoff {
    /// Construct a fresh [`Backoff`] at the floor.
    pub const fn new() -> Self {
        Self {
            current_secs: BACKOFF_FLOOR_SECS,
            consecutive_failures: 0,
        }
    }

    /// Record a successful association or a provisioning signal.
    ///
    /// Resets backoff to the floor and clears the consecutive-failure count.
    pub fn record_success(&mut self) {
        self.current_secs = BACKOFF_FLOOR_SECS;
        self.consecutive_failures = 0;
    }

    /// Alias for [`record_success`](Self::record_success): a provisioning signal
    /// clears the slow-lane latch and resets backoff identically.
    pub fn record_provisioning(&mut self) {
        self.record_success();
    }

    /// Record a failed association attempt.
    ///
    /// Doubles the backoff (capped at [`BACKOFF_CAP_SECS`]) and increments the
    /// consecutive-failure count.
    pub fn record_failure(&mut self) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        // Double, saturating at the cap.
        self.current_secs = (self.current_secs * 2).min(BACKOFF_CAP_SECS);
    }

    /// Returns the current backoff interval in whole seconds (post-failure, pre-jitter).
    ///
    /// This is the *base* value before jitter is applied; use [`next_wait_secs`] for
    /// the jittered wait.
    pub fn current_secs(&self) -> u64 {
        self.current_secs
    }

    /// True when the slow-lane latch is active (consecutive failures ≥ threshold).
    ///
    /// The supervisor logs a distinct warning when this first becomes true so the
    /// condition is diagnosable without polling.
    pub fn is_slow_lane(&self) -> bool {
        self.consecutive_failures >= SLOW_LANE_THRESHOLD
    }

    /// Consecutive failure count since last success or provisioning.
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    /// Compute the jittered backoff wait (seconds) for the next iteration.
    ///
    /// Applies ±25% random jitter derived from `rng_seed` to de-synchronize a fleet
    /// that drops together on an AP reboot.  The jitter value is in
    /// `[0.75 * current, 1.25 * current]` (clamped to a minimum of 1).
    ///
    /// `rng_seed` should be a cheap but non-constant entropy source (e.g. the lower
    /// 32 bits of the STA MAC XOR'd with a call counter, or `esp_random()`).  No
    /// cryptographic quality is required — de-sync jitter only.
    pub fn next_wait_secs(&self, rng_seed: u32) -> u64 {
        jittered(self.current_secs, rng_seed)
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new()
    }
}

/// Apply ±25% jitter to `base_secs` using `seed` as a cheap random source.
///
/// Returns a value in `[max(1, base * 0.75), base * 1.25]`.
fn jittered(base_secs: u64, seed: u32) -> u64 {
    // LCG step: cheap, seed-deterministic, no alloc.
    let rng = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
    // Map rng to a jitter offset in [0, base/2], signed to ±25%.
    // jitter_range = base * 25 / 100 (integer, floor)
    let jitter_range = base_secs * JITTER_NUM / JITTER_DEN;
    // rng % (2*jitter_range+1) gives [0, 2*range]; subtract range to get [-range, +range].
    let jitter_offset: i64 = if jitter_range == 0 {
        0
    } else {
        let span = 2 * jitter_range + 1;
        ((rng as u64 % span) as i64) - jitter_range as i64
    };
    let jittered = (base_secs as i64).saturating_add(jitter_offset) as u64;
    jittered.max(1)
}

/// Compute the absolute time (seconds since the epoch) at which the supervisor
/// intends to act on this iteration.
///
/// Combines the backoff deadline and the periodic-tick deadline, both anchored at
/// `last_attempt_secs`:
///
/// ```text
/// deadline = max(last_attempt + jittered_backoff, last_attempt + tick_interval)
/// ```
///
/// All times are relative to an arbitrary epoch (e.g. `esp_timer_get_time()` µs
/// converted to seconds); only the differences matter.  Arithmetic saturates.
///
/// # Parameters
///
/// - `last_attempt_secs` — time of the most recent association attempt (or boot if
///   no attempt yet).
/// - `jittered_backoff_secs` — output of [`Backoff::next_wait_secs`] for this
///   iteration.
/// - `tick_interval_secs` — [`TICK_INTERVAL_SECS`] in normal use; injectable for
///   testing.
///
/// The result is a pure function of its inputs, so a supervisor that re-enters its
/// loop with the same inputs re-derives the same deadline: the remaining wait shrinks
/// but the instant it targets does not move.
pub fn compute_wake_deadline_secs(
    last_attempt_secs: u64,
    jittered_backoff_secs: u64,
    tick_interval_secs: u64,
) -> u64 {
    let backoff_deadline = last_attempt_secs.saturating_add(jittered_backoff_secs);
    let tick_deadline = last_attempt_secs.saturating_add(tick_interval_secs);
    // Wake at whichever deadline comes later.
    backoff_deadline.max(tick_deadline)
}

/// Compute the supervisor's `recv_timeout` duration (seconds) for one iteration:
/// [`compute_wake_deadline_secs`] minus `now_secs`, saturating at 0.
///
/// Returns the wait duration (seconds, ≥ 0).  A return of 0 means "act immediately."
/// See [`compute_wake_deadline_secs`] for the parameter meanings.
pub fn compute_wait_secs(
    now_secs: u64,
    last_attempt_secs: u64,
    jittered_backoff_secs: u64,
    tick_interval_secs: u64,
) -> u64 {
    compute_wake_deadline_secs(last_attempt_secs, jittered_backoff_secs, tick_interval_secs)
        .saturating_sub(now_secs)
}

/// Whether a supervisor wake should be absorbed — i.e. the loop should resume waiting
/// for the remainder of the interval instead of acting on the wake.
///
/// A failing association attempt drives the radio through a disconnect transition of
/// its own, and that event's callback rings the doorbell asynchronously, often after
/// the attempt has already returned.  Acting on such a ring restarts the radio
/// immediately and collapses the attempt spacing the backoff just computed.  Absorbing
/// it costs one loop iteration and leaves the deadline where it was.
///
/// Absorb iff all four hold:
///
/// - `doorbell_rang` — a timeout wake is *never* absorbed, so the periodic tick stays
///   an unconditional backstop and no path can wait forever.
/// - `!urgent` — the ringer did not declare that it needs prompt action (a config
///   change, a forced disconnect, or the boot kick always does).
/// - `consecutive_failures > 0` — the link has not been up since the last failed
///   attempt, as far as this [`Backoff`] has been told.  Only in that regime is attempt
///   spacing at stake, and only then does a disconnect ring carry no information the
///   supervisor does not already have; the first disconnect of a healthy link is acted
///   on promptly.  A link brought up by some path that never reports its success to this
///   [`Backoff`] leaves the count positive on an up link, so that link's next disconnect
///   is deferred to the deadline instead of being acted on at once.
/// - `now_secs < wake_deadline_secs` — the interval has not yet elapsed.
pub fn should_absorb_wake(
    doorbell_rang: bool,
    urgent: bool,
    consecutive_failures: u32,
    now_secs: u64,
    wake_deadline_secs: u64,
) -> bool {
    doorbell_rang && !urgent && consecutive_failures > 0 && now_secs < wake_deadline_secs
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Backoff::record_failure / record_success ────────────────────────────────

    #[test]
    fn floor_on_fresh() {
        let b = Backoff::new();
        assert_eq!(b.current_secs(), BACKOFF_FLOOR_SECS);
        assert_eq!(b.consecutive_failures(), 0);
        assert!(!b.is_slow_lane());
    }

    #[test]
    fn doubles_on_failure() {
        let mut b = Backoff::new();
        b.record_failure(); // 2 → 4
        assert_eq!(b.current_secs(), 4);
        b.record_failure(); // 4 → 8
        assert_eq!(b.current_secs(), 8);
        b.record_failure(); // 8 → 16
        assert_eq!(b.current_secs(), 16);
        b.record_failure(); // 16 → 32
        assert_eq!(b.current_secs(), 32);
        b.record_failure(); // 32 → 60 (cap)
        assert_eq!(b.current_secs(), BACKOFF_CAP_SECS);
        b.record_failure(); // stays at 60
        assert_eq!(b.current_secs(), BACKOFF_CAP_SECS);
    }

    #[test]
    fn capped_at_max() {
        let mut b = Backoff::new();
        for _ in 0..20 {
            b.record_failure();
        }
        assert_eq!(b.current_secs(), BACKOFF_CAP_SECS);
    }

    #[test]
    fn reset_to_floor_on_success() {
        let mut b = Backoff::new();
        for _ in 0..6 {
            b.record_failure();
        }
        assert!(b.current_secs() > BACKOFF_FLOOR_SECS);
        b.record_success();
        assert_eq!(b.current_secs(), BACKOFF_FLOOR_SECS);
        assert_eq!(b.consecutive_failures(), 0);
        assert!(!b.is_slow_lane());
    }

    #[test]
    fn provisioning_resets_like_success() {
        let mut b = Backoff::new();
        for _ in 0..15 {
            b.record_failure();
        }
        assert!(b.is_slow_lane());
        b.record_provisioning();
        assert_eq!(b.current_secs(), BACKOFF_FLOOR_SECS);
        assert!(!b.is_slow_lane());
    }

    // ── Slow-lane latch ────────────────────────────────────────────────────────

    #[test]
    fn slow_lane_activates_at_threshold() {
        let mut b = Backoff::new();
        for i in 0..SLOW_LANE_THRESHOLD {
            assert!(!b.is_slow_lane(), "should not be slow_lane at {i} failures");
            b.record_failure();
        }
        assert!(b.is_slow_lane());
    }

    #[test]
    fn slow_lane_clears_on_success() {
        let mut b = Backoff::new();
        for _ in 0..SLOW_LANE_THRESHOLD + 5 {
            b.record_failure();
        }
        assert!(b.is_slow_lane());
        b.record_success();
        assert!(!b.is_slow_lane());
    }

    // ── Jitter bounds ──────────────────────────────────────────────────────────

    #[test]
    fn jitter_within_25_percent() {
        // Test many seeds over both floor and cap.
        for base in [BACKOFF_FLOOR_SECS, 8, 16, 32, BACKOFF_CAP_SECS] {
            let low = base * 75 / 100; // floor(0.75 * base)
            let high = base * 125 / 100; // floor(1.25 * base)
            for seed in (0u32..256).chain(u32::MAX - 255..=u32::MAX) {
                let w = jittered(base, seed);
                assert!(
                    w >= low.max(1) && w <= high,
                    "jittered({base}, {seed}) = {w}, expected [{}, {high}]",
                    low.max(1)
                );
            }
        }
    }

    /// `jittered(1, seed)` — jitter_range = 0, so result must always be 1 (no jitter,
    /// and the `max(1)` clamp is confirmed not to return 0).
    #[test]
    fn jitter_at_base_one_always_returns_one() {
        // With base_secs = 1, jitter_range = 1 * 25 / 100 = 0 (integer floor),
        // so jitter_offset is forced to 0 and the result is base_secs = 1.
        // The max(1) clamp ensures it never drops to 0.
        for seed in [0u32, 1, 42, 0xDEAD_BEEF, u32::MAX] {
            let w = jittered(1, seed);
            assert_eq!(w, 1, "jittered(1, {seed}) must be 1 (zero jitter range)");
        }
    }

    #[test]
    fn next_wait_secs_within_jitter_bounds() {
        let mut b = Backoff::new();
        for _ in 0..5 {
            b.record_failure(); // reach 64 → capped at 60
        }
        // Use a spread of seeds.
        for seed in [0u32, 1, 42, 0xDEAD_BEEF, u32::MAX] {
            let w = b.next_wait_secs(seed);
            let base = b.current_secs();
            let low = base * 75 / 100;
            let high = base * 125 / 100;
            assert!(
                w >= low.max(1) && w <= high,
                "seed={seed}: {w} not in [{low},{high}]"
            );
        }
    }

    // ── GW_UNREACHABLE_THRESHOLD guard ────────────────────────────────────────

    /// Pin GW_UNREACHABLE_THRESHOLD at 3.  A lower value (e.g. 1) would bounce the
    /// radio on a single unlucky probe window; a higher value extends stuck-link
    /// detection time beyond the intended ~90 s.  Change requires a conscious update
    /// to this test.
    #[test]
    fn gw_unreachable_threshold_is_three() {
        assert_eq!(GW_UNREACHABLE_THRESHOLD, 3);
    }

    // ── compute_wait_secs ──────────────────────────────────────────────────────

    #[test]
    fn healthy_wait_governed_by_tick() {
        // When backoff is at floor (2 s) and tick is 30 s, wait is governed by tick.
        let last_attempt = 100u64;
        let now = 100u64; // just attempted
        let w = compute_wait_secs(now, last_attempt, BACKOFF_FLOOR_SECS, TICK_INTERVAL_SECS);
        // tick_deadline = 100 + 30 = 130; backoff_deadline = 100 + 2 = 102 → max = 130
        assert_eq!(w, 30);
    }

    #[test]
    fn latched_wait_governed_by_backoff_not_tick() {
        // When backoff is at cap (60 s) and tick is 30 s, backoff governs.
        let last_attempt = 100u64;
        let now = 100u64;
        let w = compute_wait_secs(now, last_attempt, BACKOFF_CAP_SECS, TICK_INTERVAL_SECS);
        // tick_deadline = 130; backoff_deadline = 160 → max = 160 → wait = 60
        assert_eq!(w, BACKOFF_CAP_SECS);
    }

    #[test]
    fn elapsed_time_reduces_wait() {
        // If 20 s have already passed since last_attempt with backoff=60, 40 s remain.
        let last_attempt = 100u64;
        let now = 120u64;
        let w = compute_wait_secs(now, last_attempt, BACKOFF_CAP_SECS, TICK_INTERVAL_SECS);
        // backoff_deadline = 160; now = 120 → wait = 40
        assert_eq!(w, 40);
    }

    #[test]
    fn past_deadline_returns_zero() {
        // If the deadline is in the past, act immediately.
        let last_attempt = 100u64;
        let now = 200u64; // well past both deadlines
        let w = compute_wait_secs(now, last_attempt, BACKOFF_CAP_SECS, TICK_INTERVAL_SECS);
        assert_eq!(w, 0);
    }

    #[test]
    fn tick_deadline_when_both_in_future() {
        // Healthy: backoff=2 s, tick=30 s, 10 s elapsed.
        let last_attempt = 0u64;
        let now = 10u64;
        let w = compute_wait_secs(now, last_attempt, BACKOFF_FLOOR_SECS, TICK_INTERVAL_SECS);
        // backoff_deadline=2 (past); tick_deadline=30 → max=30 → wait=20
        assert_eq!(w, 20);
    }

    // ── compute_wake_deadline_secs ────────────────────────────────────────────

    #[test]
    fn wake_deadline_governed_by_tick_at_floor_backoff() {
        // backoff_deadline = 100 + 2 = 102; tick_deadline = 100 + 30 = 130.
        let d = compute_wake_deadline_secs(100, BACKOFF_FLOOR_SECS, TICK_INTERVAL_SECS);
        assert_eq!(d, 130);
    }

    #[test]
    fn wake_deadline_governed_by_backoff_at_cap() {
        // backoff_deadline = 100 + 60 = 160; tick_deadline = 130.
        let d = compute_wake_deadline_secs(100, BACKOFF_CAP_SECS, TICK_INTERVAL_SECS);
        assert_eq!(d, 160);
    }

    #[test]
    fn wake_deadline_saturates_near_max() {
        // Both addends overflow; saturation pins the deadline at u64::MAX rather
        // than wrapping to a deadline in the past.
        let d = compute_wake_deadline_secs(u64::MAX - 1, BACKOFF_CAP_SECS, TICK_INTERVAL_SECS);
        assert_eq!(d, u64::MAX);
        // With `now` at the clock's top too, the saturated deadline still leaves the 1 s
        // that genuinely remains — it does not wrap to an instant already past (which
        // would return 0 and retry immediately).
        assert_eq!(
            compute_wait_secs(
                u64::MAX - 1,
                u64::MAX - 1,
                BACKOFF_CAP_SECS,
                TICK_INTERVAL_SECS
            ),
            1
        );
        // From an early `now` the same saturated deadline yields the whole clock range.
        assert_eq!(
            compute_wait_secs(0, u64::MAX - 1, BACKOFF_CAP_SECS, TICK_INTERVAL_SECS),
            u64::MAX
        );
    }

    // ── should_absorb_wake ────────────────────────────────────────────────────

    #[test]
    fn absorbs_non_urgent_premature_ring_in_failure_regime() {
        // All four conditions hold: doorbell ring, not urgent, cf > 0, before deadline.
        assert!(should_absorb_wake(true, false, 1, 117, 130));
    }

    #[test]
    fn each_condition_independently_prevents_absorption() {
        // Timeout wake: the tick backstop is never deferred.
        assert!(!should_absorb_wake(false, false, 1, 117, 130));
        // Urgent ring (config change, forced disconnect, boot kick): act promptly.
        assert!(!should_absorb_wake(true, true, 1, 117, 130));
        // Healthy regime: the first disconnect of an up link is real news.
        assert!(!should_absorb_wake(true, false, 0, 117, 130));
        // Deadline reached: the wait is over, so proceed.
        assert!(!should_absorb_wake(true, false, 1, 130, 130));
        assert!(!should_absorb_wake(true, false, 1, 131, 130));
    }

    #[test]
    fn absorption_holds_across_the_failure_regime() {
        for cf in [1u32, 2, 9, 10, u32::MAX] {
            assert!(
                should_absorb_wake(true, false, cf, 117, 130),
                "cf={cf} should still absorb a premature non-urgent ring"
            );
        }
    }

    /// The absorption loop terminates only because `next_wait_secs` is a pure function of
    /// `(backoff state, seed)`: an absorbed wake re-derives its deadline from unchanged
    /// inputs, so that instant must not move.  A jitter source that consulted a call
    /// counter, the clock, or a hardware RNG would satisfy the bounds tests above while
    /// re-rolling the deadline on every absorbed ring, and rolls that land later defer the
    /// retry indefinitely — the timeout backstop never fires because the wait restarts.
    #[test]
    fn next_wait_secs_is_deterministic_in_state_and_seed() {
        let mut b = Backoff::new();
        b.record_failure();
        b.record_failure();
        for seed in [0u32, 0x1234, u32::MAX] {
            let first = b.next_wait_secs(seed);
            for _ in 0..8 {
                assert_eq!(
                    b.next_wait_secs(seed),
                    first,
                    "seed={seed}: jitter re-rolled across repeated calls"
                );
            }
            assert_eq!(
                b.clone().next_wait_secs(seed),
                first,
                "seed={seed}: jitter differs between identical Backoff states"
            );
        }
    }

    /// The bug this guard exists to close: an attempt starting at T fails ~17 s later
    /// and its own disconnect event rings the doorbell.  Before the fix that ring made
    /// the next attempt start at T+17; now it is absorbed and the retry lands on the
    /// deadline the backoff already chose.
    #[test]
    fn premature_wake_after_failed_attempt_does_not_collapse_spacing() {
        let attempt_start = 1_000u64;
        let mut backoff = Backoff::new();
        // The attempt failed, so the supervisor is in the failure regime.
        backoff.record_failure();
        let jittered = backoff.next_wait_secs(0);
        let deadline = compute_wake_deadline_secs(attempt_start, jittered, TICK_INTERVAL_SECS);
        // Backoff is 4 s here, so the 30 s tick governs the deadline.
        assert_eq!(deadline, attempt_start + TICK_INTERVAL_SECS);

        // Stray ring at T+17: absorbed, 13 s of the interval remain.
        let stray = attempt_start + 17;
        assert!(should_absorb_wake(
            true,
            false,
            backoff.consecutive_failures(),
            stray,
            deadline
        ));
        assert_eq!(
            compute_wait_secs(stray, attempt_start, jittered, TICK_INTERVAL_SECS),
            13
        );

        // A second stray at T+22, re-deriving the jitter as the loop does on an absorbed
        // iteration: same deadline instant, strictly smaller remainder — repeated strays
        // cannot push the retry out.
        let stray2 = attempt_start + 22;
        let rederived = backoff.next_wait_secs(0);
        assert_eq!(
            compute_wake_deadline_secs(attempt_start, rederived, TICK_INTERVAL_SECS),
            deadline
        );
        assert!(should_absorb_wake(
            true,
            false,
            backoff.consecutive_failures(),
            stray2,
            deadline
        ));
        assert_eq!(
            compute_wait_secs(stray2, attempt_start, rederived, TICK_INTERVAL_SECS),
            8
        );

        // At the deadline the wait expires; the timeout wake proceeds to associate,
        // so consecutive attempt starts are 30 s apart, not 17.
        let timeout_wake = attempt_start + TICK_INTERVAL_SECS;
        assert_eq!(
            compute_wait_secs(timeout_wake, attempt_start, jittered, TICK_INTERVAL_SECS),
            0
        );
        assert!(!should_absorb_wake(
            false,
            false,
            backoff.consecutive_failures(),
            timeout_wake,
            deadline
        ));
        // Even a ring landing exactly then is not absorbed.
        assert!(!should_absorb_wake(
            true,
            false,
            backoff.consecutive_failures(),
            timeout_wake,
            deadline
        ));
    }

    #[test]
    fn monotonic_until_cap() {
        // After repeated failures, each next_wait_secs (seed=0) is non-decreasing
        // until the cap is hit, then stays at cap.
        let mut b = Backoff::new();
        let mut prev = b.next_wait_secs(0);
        for _ in 0..15 {
            b.record_failure();
            let next = b.next_wait_secs(0);
            assert!(next >= prev, "not monotonic: {prev} → {next}");
            prev = next;
        }
        // Last value must be at cap (with no jitter added above cap).
        // With seed=0 jitter is deterministic; just assert it's ≥ cap.
        assert!(prev >= BACKOFF_CAP_SECS);
    }
}
