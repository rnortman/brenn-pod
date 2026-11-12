//! The record stamp: microseconds since the UNIX epoch on the machine that
//! wrote the record.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Wall-clock microseconds since the UNIX epoch on the writing machine: the
/// type of a record's stamp. Serializes as a bare integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct HostMicros(pub u64);

impl HostMicros {
    /// Current wall time as microseconds since the UNIX epoch. A pre-epoch
    /// clock yields `0`.
    pub fn now() -> HostMicros {
        HostMicros(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64,
        )
    }

    /// Microseconds elapsed from `earlier` to `self`, or `None` if `earlier`
    /// is later — e.g. an NTP step moved the clock backward between stamps.
    pub fn checked_delta(self, earlier: HostMicros) -> Option<u64> {
        self.0.checked_sub(earlier.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_positive() {
        assert_eq!(
            HostMicros(2_000_000).checked_delta(HostMicros(1_999_000)),
            Some(1_000)
        );
    }

    #[test]
    fn delta_negative_is_none() {
        // NTP step backward between two stamps.
        assert_eq!(HostMicros(10).checked_delta(HostMicros(11)), None);
    }

    #[test]
    fn ordering() {
        assert!(HostMicros(1) < HostMicros(2));
    }

    #[test]
    fn now_is_after_epoch() {
        assert!(HostMicros::now() > HostMicros(0));
    }

    #[test]
    fn serializes_as_a_bare_integer() {
        assert_eq!(serde_json::to_string(&HostMicros(42)).unwrap(), "42");
    }
}
