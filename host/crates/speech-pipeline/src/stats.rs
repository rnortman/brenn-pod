//! Small shared primitives for stage stats counters.

use std::sync::atomic::{AtomicU64, Ordering};

/// A monotonic high-water mark backed by a relaxed atomic. `bump` raises the
/// mark to the larger of its current value and the argument; `load` reads it.
/// Relaxed ordering suffices: these are observability counters with no
/// happens-before relationship to other state.
#[derive(Debug, Default)]
pub(crate) struct HighWater(AtomicU64);

impl HighWater {
    /// Raise the mark to `value` if it exceeds the current mark.
    pub(crate) fn bump(&self, value: u64) {
        self.0.fetch_max(value, Ordering::Relaxed);
    }

    /// The current high-water value.
    pub(crate) fn load(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}
