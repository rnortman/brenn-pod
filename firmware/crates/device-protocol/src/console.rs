//! Device console/TX-sink pure logic shared between device firmware and host tests.
//!
//! The device's USB-serial-JTAG sink classifies the return value of the
//! all-or-nothing `usb_serial_jtag_write_bytes` primitive. That classification is
//! pure (`usize` in, enum out) and lives here so the host harness exercises the
//! real function instead of a hand-maintained copy.

/// Classification of a frame write against the all-or-nothing TX primitive.
///
/// The primitive returns exactly `0` (ring lacked space) or exactly `len` (frame
/// pushed). A value strictly between is structurally impossible; if encountered it
/// is treated as a drop for defense-in-depth.
#[derive(Debug, PartialEq, Eq)]
pub enum WriteOutcome {
    /// Frame fully pushed to the TX ring.
    Sent,
    /// Frame not pushed (ring full); the whole frame was dropped atomically.
    Dropped,
}

/// Classify the return value of `usb_serial_jtag_write_bytes` for a frame of `len`
/// bytes: `n == len` is a clean push, anything else is a drop.
pub fn classify_write(n: usize, len: usize) -> WriteOutcome {
    if n == len {
        WriteOutcome::Sent
    } else {
        // n == 0 (normal drop) or 0 < n < len (structurally impossible from the
        // primitive; treat as drop for defense-in-depth).
        WriteOutcome::Dropped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `n == len` → Sent (frame fully pushed).
    #[test]
    fn classify_write_n_eq_len_is_sent() {
        let len = 50usize;
        assert_eq!(classify_write(len, len), WriteOutcome::Sent);
    }

    /// `n == 0` → Dropped (ring lacked room; TX_WRITE_FAILURES incremented on device).
    #[test]
    fn classify_write_n_zero_is_dropped() {
        let len = 50usize;
        assert_eq!(classify_write(0, len), WriteOutcome::Dropped);
    }

    /// `0 < n < len` cannot occur from `usb_serial_jtag_write_bytes` but is treated
    /// as Dropped for defense-in-depth (see classify_write doc).
    #[test]
    fn classify_write_partial_is_dropped_defense_in_depth() {
        let len = 50usize;
        let n = 13usize; // partial — structurally impossible from the primitive
        assert_eq!(classify_write(n, len), WriteOutcome::Dropped);
    }
}
