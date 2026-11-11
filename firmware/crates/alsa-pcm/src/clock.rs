//! The capture clocks: `CLOCK_MONOTONIC` alone, and a monotonic reading paired with
//! the wall clock.

/// Read one clock in microseconds.
fn clock_us(id: libc::clockid_t, name: &str) -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a live, correctly typed timespec for the duration of the call.
    let rc = unsafe { libc::clock_gettime(id, &mut ts) };
    if rc != 0 {
        // Both clocks are always available; a failure here is a kernel that has
        // stopped keeping time, and audio dated from a frozen clock is worse than
        // none.
        panic!("clock_gettime({name}) failed");
    }
    let secs = u64::try_from(ts.tv_sec)
        .unwrap_or_else(|_| panic!("clock_gettime({name}) read before 1970"));
    secs * 1_000_000 + (ts.tv_nsec as u64) / 1_000
}

/// The platform's monotonic clock, in microseconds — the epoch every timestamp in a
/// segment is an offset from.
///
/// `CLOCK_MONOTONIC` rather than the wall clock: a board with no battery-backed
/// clock believes it is 1970 until NTP lands, and a segment whose timestamps jump
/// when that happens is a segment the host cannot reassemble.
pub fn monotonic_us() -> u64 {
    clock_us(libc::CLOCK_MONOTONIC, "CLOCK_MONOTONIC")
}

/// The wall clock, `CLOCK_REALTIME`, in microseconds since the Unix epoch.
pub fn realtime_us() -> u64 {
    clock_us(libc::CLOCK_REALTIME, "CLOCK_REALTIME")
}

/// A monotonic and a wall reading taken as a pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockPair {
    /// `CLOCK_MONOTONIC`, microseconds: the midpoint of the two monotonic readings
    /// that bracketed the wall reading.
    pub mono_us: u64,
    /// `CLOCK_REALTIME`, microseconds since the Unix epoch.
    pub wall_us: u64,
}

/// Bracket width at which a pair is taken without retrying.
const PAIR_BRACKET_US: u64 = 50;

/// Further attempts after the first when a bracket is wider than
/// [`PAIR_BRACKET_US`].
const PAIR_RETRIES: usize = 4;

/// Read the two clocks as a pair: monotonic, wall, monotonic again.
///
/// A preemption between the reads widens the bracket, so the read is retried up to
/// four times until the two monotonic readings are within 50 µs, and the tightest
/// bracket seen is kept. The monotonic value is the bracket's midpoint, so the wall
/// reading was taken within half the bracket of it.
pub fn clock_pair() -> ClockPair {
    bracketed(monotonic_us, realtime_us)
}

fn bracketed(mut mono: impl FnMut() -> u64, mut wall: impl FnMut() -> u64) -> ClockPair {
    let mut best: Option<(u64, ClockPair)> = None;
    for _ in 0..=PAIR_RETRIES {
        let before = mono();
        let wall_us = wall();
        let after = mono();
        let width = after - before;
        let pair = ClockPair {
            mono_us: before + width / 2,
            wall_us,
        };
        if best.is_none_or(|(w, _)| width < w) {
            best = Some((width, pair));
        }
        if width <= PAIR_BRACKET_US {
            break;
        }
    }
    best.expect("at least one attempt is made").1
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn the_monotonic_clock_advances_and_never_goes_backwards() {
        let first = monotonic_us();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let second = monotonic_us();
        assert!(second > first, "{first} then {second}");
        // Sane magnitude: microseconds, not nanoseconds or seconds. Two milliseconds
        // of sleep must not read as two thousand seconds.
        assert!((second - first) < 1_000_000, "{}", second - first);
    }

    #[test]
    fn a_pair_lies_within_the_bracket_around_it_and_the_wall_is_after_2020() {
        let before = monotonic_us();
        let pair = clock_pair();
        let after = monotonic_us();
        assert!(
            before <= pair.mono_us && pair.mono_us <= after,
            "{before} <= {} <= {after}",
            pair.mono_us
        );
        // 2020-01-01T00:00:00Z.
        assert!(pair.wall_us > 1_577_836_800_000_000, "{}", pair.wall_us);
    }

    /// A monotonic clock that returns the scripted readings in order.
    fn scripted(readings: &'static [u64]) -> impl FnMut() -> u64 {
        let mut readings = readings.iter().copied();
        move || readings.next().expect("a scripted reading")
    }

    #[test]
    fn a_tight_first_bracket_is_taken_at_once() {
        let mut walls = 0;
        let pair = bracketed(scripted(&[1_000, 1_040]), || {
            walls += 1;
            7
        });
        assert_eq!(walls, 1);
        assert_eq!(
            pair,
            ClockPair {
                mono_us: 1_020,
                wall_us: 7
            }
        );
    }

    #[test]
    fn wide_brackets_are_retried_four_times_and_the_tightest_is_kept() {
        // Five attempts, none within 50 µs; the third is the tightest.
        let mono = scripted(&[
            0, 500, 1_000, 1_300, 2_000, 2_090, 3_000, 3_200, 4_000, 4_400,
        ]);
        let mut wall = 100;
        let pair = bracketed(mono, || {
            wall += 1;
            wall
        });
        assert_eq!(wall, 105, "one first attempt and four retries");
        assert_eq!(
            pair,
            ClockPair {
                mono_us: 2_045,
                wall_us: 103
            }
        );
    }

    #[test]
    fn retrying_stops_at_the_first_bracket_within_50_us() {
        let mono = scripted(&[0, 500, 1_000, 1_050]);
        let pair = bracketed(mono, || 9);
        assert_eq!(pair.mono_us, 1_025);
    }
}
