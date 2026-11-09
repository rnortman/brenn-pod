//! The capture-level meter: one dBFS reading per received audio frame, and the
//! percentile the listener reduces a window of readings with.
//!
//! Every level here is decibels relative to 16-bit full scale (a full-scale
//! square wave reads 0 dBFS, a full-scale sine about −3 dBFS), floored at
//! [`LEVEL_FLOOR_DBFS`] so digital silence has a finite reading.

/// The reading for digital silence, and the floor of every reading.
pub const LEVEL_FLOOR_DBFS: f32 = -100.0;

/// At or below this a frame reads as a dead channel rather than a quiet room: a
/// live microphone's self-noise in the quietest room sits tens of dB above it,
/// and an unrouted I2S slot is digital zero.
pub const DEAD_CHANNEL_DBFS: f32 = -95.0;

/// The level of one received audio frame.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FrameLevel {
    /// Mean-square level of the frame, dBFS, floored at [`LEVEL_FLOOR_DBFS`].
    pub dbfs: f32,
}

impl FrameLevel {
    /// Measure `pcm`: `10·log10(mean_square / 32768²)`, floored. An empty frame
    /// reads as the floor.
    pub fn measure(pcm: &[i16]) -> FrameLevel {
        if pcm.is_empty() {
            return FrameLevel {
                dbfs: LEVEL_FLOOR_DBFS,
            };
        }
        let sum_sq: f64 = pcm.iter().map(|&s| f64::from(s) * f64::from(s)).sum();
        let mean_sq = sum_sq / pcm.len() as f64;
        let full_scale_sq = 32_768.0_f64 * 32_768.0;
        let dbfs = if mean_sq == 0.0 {
            f64::from(LEVEL_FLOOR_DBFS)
        } else {
            (10.0 * (mean_sq / full_scale_sq).log10()).max(f64::from(LEVEL_FLOOR_DBFS))
        };
        FrameLevel { dbfs: dbfs as f32 }
    }

    /// Whether this frame reads as a dead channel ([`DEAD_CHANNEL_DBFS`]).
    pub fn is_dead(self) -> bool {
        self.dbfs <= DEAD_CHANNEL_DBFS
    }
}

/// The nearest-rank `pct`-th percentile (`0..=100`) of `values`; `None` when
/// `values` is empty. Reorders `values`.
pub fn percentile(values: &mut [f32], pct: u32) -> Option<f32> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f32::total_cmp);
    let n = values.len();
    // Nearest rank: the smallest value with at least `pct`% of readings at or
    // below it — rank ceil(pct·n/100), 1-based, at least 1.
    let rank = (pct.min(100) as usize * n).div_ceil(100).max(1);
    Some(values[rank - 1])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sine(amplitude: f64, n: usize) -> Vec<i16> {
        (0..n)
            .map(|i| {
                let phase = 2.0 * std::f64::consts::PI * 440.0 * i as f64 / 16_000.0;
                (amplitude * phase.sin()).round() as i16
            })
            .collect()
    }

    #[test]
    fn a_minus_20_dbfs_sine_measures_within_a_tenth_of_a_db() {
        // A sine's RMS is its peak / √2, so a −20 dBFS (RMS) sine peaks at
        // 32768 · 10^(−20/20) · √2.
        let peak = 32_768.0 * 10f64.powf(-20.0 / 20.0) * std::f64::consts::SQRT_2;
        let level = FrameLevel::measure(&sine(peak, 16_000));
        assert!((level.dbfs + 20.0).abs() < 0.1, "measured {}", level.dbfs);
    }

    #[test]
    fn silence_and_empty_measure_the_floor() {
        assert_eq!(FrameLevel::measure(&[0; 320]).dbfs, LEVEL_FLOOR_DBFS);
        assert_eq!(FrameLevel::measure(&[]).dbfs, LEVEL_FLOOR_DBFS);
        assert!(FrameLevel::measure(&[0; 320]).is_dead());
    }

    #[test]
    fn full_scale_square_is_zero_dbfs_and_one_lsb_is_floored_not_dead() {
        assert!(FrameLevel::measure(&[i16::MIN; 320]).dbfs.abs() < 1e-3);
        // ±1 LSB is −90.3 dBFS: a live, very quiet channel, above the dead line.
        let lsb: Vec<i16> = (0..320).map(|i| if i % 2 == 0 { 1 } else { -1 }).collect();
        let level = FrameLevel::measure(&lsb);
        assert!((level.dbfs + 90.31).abs() < 0.01, "measured {}", level.dbfs);
        assert!(!level.is_dead());
    }

    #[test]
    fn nearest_rank_percentiles() {
        let mut v: Vec<f32> = (1..=10).map(|i| i as f32).collect();
        assert_eq!(percentile(&mut v.clone(), 80), Some(8.0));
        assert_eq!(percentile(&mut v.clone(), 20), Some(2.0));
        assert_eq!(percentile(&mut v.clone(), 0), Some(1.0));
        assert_eq!(percentile(&mut v, 100), Some(10.0));
        assert_eq!(percentile(&mut [5.0], 80), Some(5.0));
        assert_eq!(percentile(&mut [], 80), None);
        // Order of input does not matter.
        assert_eq!(percentile(&mut [3.0, 1.0, 2.0], 50), Some(2.0));
    }
}
