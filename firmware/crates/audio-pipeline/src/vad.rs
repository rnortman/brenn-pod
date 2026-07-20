//! VAD (Voice Activity Detection) state machine.
//!
//! Pure, `no_std`-compatible.  Hosts a simple two-state FSM driven by successive
//! `update()` calls.  The gating statistic is delivered by a `VadSource` so the
//! SPENERGY-based gate and a PCM-RMS software fallback share the same FSM.
//!
//! Design constants (firmware/crates/audio-pipeline, design §2.3):
//! - `VAD_ONSET_COUNT = 2` consecutive above-threshold polls to open.
//! - `VAD_HANGOVER_MS = 800` ms of below-threshold readings before close.
//!
//! The hangover period is expressed in poll ticks, not wall-clock ms, because the
//! FSM itself has no clock.  Callers convert: `hangover_ticks = ceil(VAD_HANGOVER_MS /
//! poll_interval_ms)`.  A convenience constant `vad_hangover_ticks(poll_hz)` is
//! provided.

/// Returns the hangover duration in ticks for a given poll rate, using the
/// compile-time default hangover.
///
/// `hangover_ticks = ceil(VAD_HANGOVER_MS * poll_hz / 1000)`.
pub const fn vad_hangover_ticks(poll_hz: u32) -> u32 {
    vad_hangover_ticks_ms(VAD_HANGOVER_MS, poll_hz)
}

/// Returns the hangover duration in ticks for a runtime hangover (milliseconds)
/// at a given poll rate.
///
/// `hangover_ticks = ceil(hangover_ms * poll_hz / 1000)`. Used to convert an
/// NVS-provisioned hangover to poll ticks at boot.
pub const fn vad_hangover_ticks_ms(hangover_ms: u32, poll_hz: u32) -> u32 {
    (hangover_ms * poll_hz).div_ceil(1000)
}

/// Largest VAD hangover accepted from a runtime (NVS) provision, in milliseconds.
/// The deployment policy is a few seconds; a larger value is a misprovision and
/// would also risk overflowing the `hangover_ms * poll_hz` product in
/// [`vad_hangover_ticks_ms`], so a provision beyond this falls back to the
/// compile-time [`VAD_HANGOVER_MS`] default.
pub const VAD_HANGOVER_MS_MAX: u32 = 60_000;

/// Decode and range-check a little-endian `u32` VAD-hangover blob (the NVS
/// `"vad_hangover_ms"` value). Returns `None` — the caller uses the compile-time
/// [`VAD_HANGOVER_MS`] default — when the blob is not exactly four bytes, or the
/// decoded value is zero or beyond [`VAD_HANGOVER_MS_MAX`]. Keeping the decode and
/// bound here (host-compiled) makes them unit-testable off the device.
pub fn decode_vad_hangover_ms(blob: &[u8]) -> Option<u32> {
    let bytes: [u8; 4] = blob.try_into().ok()?;
    let ms = u32::from_le_bytes(bytes);
    (1..=VAD_HANGOVER_MS_MAX).contains(&ms).then_some(ms)
}

/// Whether a VAD gate threshold value is acceptable: finite and non-negative.
/// The shared guard used by both the NVS blob decoder ([`decode_vad_threshold`])
/// and the set-path validation on the device.
pub fn vad_threshold_ok(t: f32) -> bool {
    t.is_finite() && t >= 0.0
}

/// Decode and range-check a little-endian `f32` VAD-threshold blob (the NVS
/// `"vad_threshold"` value). Returns `None` — the caller uses the compile-time
/// default — when the blob is not exactly four bytes, or the decoded value is not
/// finite-and-non-negative ([`vad_threshold_ok`]). Mirrors the hangover sibling
/// [`decode_vad_hangover_ms`]; keeping the decode and guard here (host-compiled)
/// makes them unit-testable off the device.
pub fn decode_vad_threshold(blob: &[u8]) -> Option<f32> {
    let bytes: [u8; 4] = blob.try_into().ok()?;
    let t = f32::from_le_bytes(bytes);
    vad_threshold_ok(t).then_some(t)
}

/// Consecutive above-threshold polls required to open the VAD gate.
pub const VAD_ONSET_COUNT: u32 = 2;

/// Hangover duration in milliseconds — how long the gate stays open after the
/// signal drops below threshold.
pub const VAD_HANGOVER_MS: u32 = 800;

// ── VadSource trait ────────────────────────────────────────────────────────────

/// Provides a single scalar "energy" reading to the VAD FSM each tick.
///
/// Implement this for SPENERGY-based gating or PCM-RMS software gating.
pub trait VadSource {
    /// Current energy sample.  Compared against the threshold passed to
    /// `VadStateMachine::new`.
    fn energy(&self) -> f32;
}

// ── State machine ──────────────────────────────────────────────────────────────

/// VAD FSM state (not `pub`; callers observe via `VadTransition`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Gate is closed; counting consecutive above-threshold polls.
    Closed { consecutive_above: u32 },
    /// Gate is open; counting consecutive below-threshold polls (hangover counter).
    Open { below_ticks: u32 },
}

/// Outcome of a single `update()` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VadTransition {
    /// No state change.
    Unchanged,
    /// VAD just opened (onset).
    Opened,
    /// VAD just closed (release after hangover).
    Closed,
}

/// Two-state (Closed ↔ Open) VAD gate driven by successive energy samples.
///
/// # Example
/// ```rust
/// use audio_pipeline::vad::{VadStateMachine, VadTransition, VadSource};
///
/// struct FixedEnergy(f32);
/// impl VadSource for FixedEnergy {
///     fn energy(&self) -> f32 { self.0 }
/// }
///
/// let hangover_ticks = 16; // e.g. 800 ms / 50 ms poll
/// let mut fsm = VadStateMachine::new(10.0, hangover_ticks);
/// assert!(!fsm.is_open());
/// ```
pub struct VadStateMachine {
    threshold: f32,
    hangover_ticks: u32,
    state: State,
}

impl VadStateMachine {
    /// Create a new FSM starting in the Closed state.
    ///
    /// - `threshold`: energy value above which a poll is "above threshold".
    /// - `hangover_ticks`: number of below-threshold ticks before the gate closes
    ///   after VAD onset.  Derive from `vad_hangover_ticks(poll_hz)`.
    pub fn new(threshold: f32, hangover_ticks: u32) -> Self {
        Self {
            threshold,
            hangover_ticks,
            state: State::Closed {
                consecutive_above: 0,
            },
        }
    }

    /// Whether the VAD gate is currently open.
    pub fn is_open(&self) -> bool {
        matches!(self.state, State::Open { .. })
    }

    /// Replace the gate threshold in place, preserving FSM state (open/closed,
    /// consecutive/hangover counters). The next `update()` compares against the
    /// new value. Forward-compat for a runtime threshold setter; the per-poll
    /// `update()` already reads `self.threshold` each call.
    pub fn set_threshold(&mut self, threshold: f32) {
        self.threshold = threshold;
    }

    /// Replace the hangover length (in poll ticks) in place, preserving FSM state.
    /// If the gate is open and its hangover counter already exceeds the new value,
    /// the next below-threshold `update()` closes the gate. Forward-compat for a
    /// runtime hangover setter; the hangover is fixed at boot today, so this is
    /// unused at runtime — the twin of `set_threshold`.
    pub fn set_hangover(&mut self, hangover_ticks: u32) {
        self.hangover_ticks = hangover_ticks;
    }

    /// Feed one energy sample.  Returns the transition that occurred (if any).
    pub fn update<S: VadSource>(&mut self, source: &S) -> VadTransition {
        let above = source.energy() > self.threshold;
        match self.state {
            State::Closed { consecutive_above } => {
                if above {
                    let count = consecutive_above + 1;
                    if count >= VAD_ONSET_COUNT {
                        self.state = State::Open { below_ticks: 0 };
                        VadTransition::Opened
                    } else {
                        self.state = State::Closed {
                            consecutive_above: count,
                        };
                        VadTransition::Unchanged
                    }
                } else {
                    // Reset consecutive count on any below-threshold poll.
                    self.state = State::Closed {
                        consecutive_above: 0,
                    };
                    VadTransition::Unchanged
                }
            }
            State::Open { below_ticks } => {
                if above {
                    // Above threshold while open: reset hangover counter.
                    self.state = State::Open { below_ticks: 0 };
                    VadTransition::Unchanged
                } else {
                    let ticks = below_ticks + 1;
                    if ticks >= self.hangover_ticks {
                        self.state = State::Closed {
                            consecutive_above: 0,
                        };
                        VadTransition::Closed
                    } else {
                        self.state = State::Open { below_ticks: ticks };
                        VadTransition::Unchanged
                    }
                }
            }
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixed(f32);
    impl VadSource for Fixed {
        fn energy(&self) -> f32 {
            self.0
        }
    }

    const THRESHOLD: f32 = 10.0;
    const HANGOVER: u32 = 4; // ticks for tests

    fn fsm() -> VadStateMachine {
        VadStateMachine::new(THRESHOLD, HANGOVER)
    }

    // ── Onset tests ───────────────────────────────────────────────────────────

    #[test]
    fn one_above_does_not_open() {
        let mut fsm = fsm();
        let t = fsm.update(&Fixed(20.0));
        assert_eq!(t, VadTransition::Unchanged);
        assert!(!fsm.is_open());
    }

    #[test]
    fn two_consecutive_above_opens() {
        let mut fsm = fsm();
        assert_eq!(fsm.update(&Fixed(20.0)), VadTransition::Unchanged);
        assert_eq!(fsm.update(&Fixed(20.0)), VadTransition::Opened);
        assert!(fsm.is_open());
    }

    #[test]
    fn below_resets_consecutive_count() {
        // above, below, above, above → needs two more consecutive after reset
        let mut fsm = fsm();
        fsm.update(&Fixed(20.0)); // count=1
        fsm.update(&Fixed(0.0)); // reset → count=0
        assert_eq!(fsm.update(&Fixed(20.0)), VadTransition::Unchanged); // count=1
        assert!(!fsm.is_open());
        assert_eq!(fsm.update(&Fixed(20.0)), VadTransition::Opened); // count=2 → open
        assert!(fsm.is_open());
    }

    // ── Hangover / release tests ───────────────────────────────────────────────

    #[test]
    fn hangover_holds_open() {
        let mut fsm = fsm();
        // Open the gate
        fsm.update(&Fixed(20.0));
        fsm.update(&Fixed(20.0));
        assert!(fsm.is_open());

        // Feed below-threshold ticks up to (hangover - 1): still open
        for _ in 0..(HANGOVER - 1) {
            let t = fsm.update(&Fixed(0.0));
            assert_eq!(t, VadTransition::Unchanged, "should still be open");
            assert!(fsm.is_open());
        }

        // One more below-threshold tick hits the hangover limit → closes
        assert_eq!(fsm.update(&Fixed(0.0)), VadTransition::Closed);
        assert!(!fsm.is_open());
    }

    #[test]
    fn above_during_hangover_resets_hangover() {
        let mut fsm = fsm();
        // Open
        fsm.update(&Fixed(20.0));
        fsm.update(&Fixed(20.0));

        // HANGOVER-1 below-threshold ticks
        for _ in 0..(HANGOVER - 1) {
            fsm.update(&Fixed(0.0));
        }
        // One above-threshold tick resets hangover counter
        assert_eq!(fsm.update(&Fixed(20.0)), VadTransition::Unchanged);
        assert!(fsm.is_open());

        // Must now survive another full hangover period before closing
        for _ in 0..(HANGOVER - 1) {
            assert_eq!(fsm.update(&Fixed(0.0)), VadTransition::Unchanged);
            assert!(fsm.is_open());
        }
        assert_eq!(fsm.update(&Fixed(0.0)), VadTransition::Closed);
        assert!(!fsm.is_open());
    }

    #[test]
    fn flap_inside_hangover_extends() {
        // Repeated above-threshold samples during hangover keep the gate open.
        let mut fsm = fsm();
        fsm.update(&Fixed(20.0));
        fsm.update(&Fixed(20.0));

        // Alternate above/below many times — gate should stay open throughout
        for _ in 0..20 {
            fsm.update(&Fixed(0.0)); // below — below_ticks climbs but reset next
            fsm.update(&Fixed(20.0)); // above — resets below_ticks to 0
            assert!(fsm.is_open());
        }
    }

    #[test]
    fn release_after_full_hangover() {
        // Full end-to-end: open, then exactly hangover ticks of silence → closed.
        let mut fsm = fsm();
        fsm.update(&Fixed(20.0));
        fsm.update(&Fixed(20.0));
        for _ in 0..HANGOVER {
            fsm.update(&Fixed(0.0));
        }
        assert!(!fsm.is_open());
    }

    // ── set_threshold tests ───────────────────────────────────────────────────

    /// set_threshold while Closed: energy previously below old threshold but above
    /// new threshold opens the gate after VAD_ONSET_COUNT polls.
    #[test]
    fn set_threshold_while_closed_uses_new_value() {
        // Old threshold = 10.0; energy 5.0 is below it.
        let mut fsm = VadStateMachine::new(10.0, HANGOVER);
        // Confirm it does not open with old threshold at 5.0 energy.
        assert_eq!(fsm.update(&Fixed(5.0)), VadTransition::Unchanged);
        assert!(!fsm.is_open());
        // Reset to Closed with consecutive_above=0 (below resets).
        fsm.update(&Fixed(0.0));

        // Lower threshold so 5.0 is above it.
        fsm.set_threshold(2.0);

        // Now VAD_ONSET_COUNT consecutive polls at 5.0 should open.
        assert_eq!(fsm.update(&Fixed(5.0)), VadTransition::Unchanged); // count=1
        assert_eq!(fsm.update(&Fixed(5.0)), VadTransition::Opened); // count=2 → open
        assert!(fsm.is_open());
    }

    /// set_threshold while Open: changing the threshold does NOT reset open/hangover
    /// state (forward-compat: in-place update preserves FSM state).
    #[test]
    fn set_threshold_while_open_preserves_fsm_state() {
        let mut fsm = VadStateMachine::new(THRESHOLD, HANGOVER);
        // Open the gate.
        fsm.update(&Fixed(20.0));
        fsm.update(&Fixed(20.0));
        assert!(fsm.is_open());

        // Raise threshold above current energy while open.
        fsm.set_threshold(100.0);

        // Gate is still open immediately after set_threshold (state preserved).
        assert!(fsm.is_open());

        // Now energy 20.0 is below new threshold 100.0, so hangover ticks start.
        for _ in 0..(HANGOVER - 1) {
            assert_eq!(fsm.update(&Fixed(20.0)), VadTransition::Unchanged);
            assert!(fsm.is_open());
        }
        // One more below-threshold tick closes the gate.
        assert_eq!(fsm.update(&Fixed(20.0)), VadTransition::Closed);
        assert!(!fsm.is_open());
    }

    /// set_threshold while Closed with consecutive_above > 0: the counter is
    /// preserved, so one more above-threshold poll (not two) opens the gate.
    #[test]
    fn set_threshold_while_closed_preserves_consecutive_above() {
        // Old threshold = 10.0; deliver one above-threshold poll to set consecutive_above=1.
        let mut fsm = VadStateMachine::new(10.0, HANGOVER);
        // 5.0 < 10.0: below old threshold, so this does NOT increment consecutive_above.
        // We need energy above 10.0 to increment the counter, then lower threshold.
        assert_eq!(fsm.update(&Fixed(15.0)), VadTransition::Unchanged); // consecutive_above=1
        assert!(!fsm.is_open());

        // Lower threshold to 2.0: 15.0 > 2.0, but we want to verify that after set_threshold
        // the counter is still 1, so a single additional above-threshold poll opens the gate.
        fsm.set_threshold(2.0);
        // One more poll: consecutive_above was 1, now 2 → should open (VAD_ONSET_COUNT=2).
        assert_eq!(fsm.update(&Fixed(5.0)), VadTransition::Opened);
        assert!(fsm.is_open());
    }

    /// set_threshold while Open with energy above the new (lower) threshold:
    /// below_ticks counter resets to 0, so a full hangover is required to close.
    #[test]
    fn set_threshold_while_open_lowers_threshold_resets_hangover() {
        let mut fsm = VadStateMachine::new(THRESHOLD, HANGOVER);
        // Open the gate.
        fsm.update(&Fixed(20.0));
        fsm.update(&Fixed(20.0));
        assert!(fsm.is_open());

        // Tick once below threshold: below_ticks becomes 1.
        assert_eq!(fsm.update(&Fixed(0.0)), VadTransition::Unchanged);

        // Lower the threshold so current energy (20.0) would now be above it.
        fsm.set_threshold(5.0);

        // One poll at 20.0 > new threshold 5.0: gate stays open and below_ticks resets to 0.
        assert_eq!(fsm.update(&Fixed(20.0)), VadTransition::Unchanged);
        assert!(fsm.is_open());

        // Now require a full hangover to close, confirming below_ticks was reset.
        for _ in 0..(HANGOVER - 1) {
            assert_eq!(fsm.update(&Fixed(0.0)), VadTransition::Unchanged);
            assert!(fsm.is_open());
        }
        assert_eq!(fsm.update(&Fixed(0.0)), VadTransition::Closed);
        assert!(!fsm.is_open());
    }

    /// set_threshold with the exact old value: FSM state is completely unchanged.
    #[test]
    fn set_threshold_noop_leaves_fsm_unchanged() {
        let mut fsm = VadStateMachine::new(THRESHOLD, HANGOVER);
        // Deliver one above-threshold poll: consecutive_above=1, still closed.
        assert_eq!(fsm.update(&Fixed(20.0)), VadTransition::Unchanged);
        assert!(!fsm.is_open());

        // Set the same threshold — must not alter is_open or behavior.
        fsm.set_threshold(THRESHOLD);
        assert!(!fsm.is_open());

        // One more above-threshold poll should still open (counter is preserved at 1).
        assert_eq!(fsm.update(&Fixed(20.0)), VadTransition::Opened);
        assert!(fsm.is_open());
    }

    // ── Utility ───────────────────────────────────────────────────────────────

    #[test]
    fn hangover_ticks_const_fn() {
        // 800 ms @ 20 Hz = 16 ticks
        assert_eq!(vad_hangover_ticks(20), 16);
        // 800 ms @ 10 Hz = 8 ticks
        assert_eq!(vad_hangover_ticks(10), 8);
        // 800 ms @ 3 Hz → ceil(2.4) = 3 ticks
        assert_eq!(vad_hangover_ticks(3), 3);
    }

    #[test]
    fn decode_hangover_accepts_valid_blob() {
        // 3000 ms little-endian.
        assert_eq!(decode_vad_hangover_ms(&3000u32.to_le_bytes()), Some(3000));
        // Boundary: exactly the max is accepted.
        assert_eq!(
            decode_vad_hangover_ms(&VAD_HANGOVER_MS_MAX.to_le_bytes()),
            Some(VAD_HANGOVER_MS_MAX)
        );
    }

    #[test]
    fn decode_hangover_rejects_wrong_length() {
        assert_eq!(decode_vad_hangover_ms(&[0u8; 3]), None);
        assert_eq!(decode_vad_hangover_ms(&[0u8; 5]), None);
        assert_eq!(decode_vad_hangover_ms(&[]), None);
    }

    #[test]
    fn decode_hangover_rejects_out_of_range() {
        // Zero is degenerate (immediate close); reject it.
        assert_eq!(decode_vad_hangover_ms(&0u32.to_le_bytes()), None);
        // A value that would overflow `ms * poll_hz` is a misprovision.
        assert_eq!(
            decode_vad_hangover_ms(&(VAD_HANGOVER_MS_MAX + 1).to_le_bytes()),
            None
        );
        assert_eq!(decode_vad_hangover_ms(&u32::MAX.to_le_bytes()), None);
    }

    #[test]
    fn accepted_hangover_never_overflows_ticks() {
        // The whole point of the bound: any accepted value converts without
        // overflowing the u32 product at the real poll rate.
        let ms = decode_vad_hangover_ms(&VAD_HANGOVER_MS_MAX.to_le_bytes()).unwrap();
        assert_eq!(
            vad_hangover_ticks_ms(ms, 20),
            (60_000 * 20u32).div_ceil(1000)
        );
    }

    #[test]
    fn hangover_ticks_ms_matches_default() {
        // The default helper is the runtime helper at VAD_HANGOVER_MS.
        assert_eq!(
            vad_hangover_ticks_ms(VAD_HANGOVER_MS, 20),
            vad_hangover_ticks(20)
        );
        // 3000 ms @ 20 Hz = 60 ticks
        assert_eq!(vad_hangover_ticks_ms(3000, 20), 60);
        // 3000 ms @ 3 Hz → ceil(9.0) = 9 ticks
        assert_eq!(vad_hangover_ticks_ms(3000, 3), 9);
        // 100 ms @ 3 Hz → ceil(0.3) = 1 tick
        assert_eq!(vad_hangover_ticks_ms(100, 3), 1);
    }

    #[test]
    fn decode_threshold_accepts_valid_blob() {
        assert_eq!(decode_vad_threshold(&1.5f32.to_le_bytes()), Some(1.5));
        // Zero is a valid threshold (gate opens on any energy above 0).
        assert_eq!(decode_vad_threshold(&0.0f32.to_le_bytes()), Some(0.0));
    }

    #[test]
    fn decode_threshold_rejects_wrong_length() {
        assert_eq!(decode_vad_threshold(&[0u8; 3]), None);
        assert_eq!(decode_vad_threshold(&[0u8; 5]), None);
        assert_eq!(decode_vad_threshold(&[]), None);
    }

    #[test]
    fn decode_threshold_rejects_non_finite_and_negative() {
        assert_eq!(decode_vad_threshold(&f32::NAN.to_le_bytes()), None);
        assert_eq!(decode_vad_threshold(&f32::INFINITY.to_le_bytes()), None);
        assert_eq!(decode_vad_threshold(&f32::NEG_INFINITY.to_le_bytes()), None);
        assert_eq!(decode_vad_threshold(&(-1.0f32).to_le_bytes()), None);
    }

    #[test]
    fn vad_threshold_ok_matches_guard() {
        assert!(vad_threshold_ok(0.0));
        assert!(vad_threshold_ok(3.25));
        assert!(!vad_threshold_ok(-0.001));
        assert!(!vad_threshold_ok(f32::NAN));
        assert!(!vad_threshold_ok(f32::INFINITY));
    }

    /// set_hangover while Open: a longer hangover holds the gate open past the old
    /// limit; state (open/hangover counter) is preserved across the change.
    #[test]
    fn set_hangover_extends_hold_while_open() {
        let mut fsm = VadStateMachine::new(THRESHOLD, HANGOVER);
        fsm.update(&Fixed(20.0));
        fsm.update(&Fixed(20.0));
        assert!(fsm.is_open());

        // Raise the hangover to 2 * HANGOVER before the old limit is reached.
        fsm.set_hangover(HANGOVER * 2);

        // The old HANGOVER ticks of silence no longer close the gate.
        for _ in 0..HANGOVER {
            assert_eq!(fsm.update(&Fixed(0.0)), VadTransition::Unchanged);
            assert!(fsm.is_open());
        }
        // A full second hangover span is now required.
        for _ in 0..(HANGOVER - 1) {
            assert_eq!(fsm.update(&Fixed(0.0)), VadTransition::Unchanged);
            assert!(fsm.is_open());
        }
        assert_eq!(fsm.update(&Fixed(0.0)), VadTransition::Closed);
        assert!(!fsm.is_open());
    }

    #[test]
    fn starts_closed() {
        assert!(!fsm().is_open());
    }
}
