//! The digital gain stage applied to every reply before it is paced to a pod.
//!
//! Gain is expressed in decibels throughout; the one linear conversion is the
//! sample multiply here.

use std::sync::Arc;

/// Q15 fixed-point unity: the linear factor is carried as `factor * 2^15` so the
/// multiply stays in integer arithmetic with round-half-away-from-zero.
const Q15_SHIFT: u32 = 15;

/// The linear amplitude factor for `db` decibels: `10^(db/20)`.
pub fn db_to_linear(db: f32) -> f32 {
    10f32.powf(db / 20.0)
}

/// Scale `pcm` by `db` decibels (`10^(db/20)` linear), saturating to `i16`.
///
/// `db == 0.0` returns the input `Arc` unchanged — the identity is exact and
/// allocation-free. Otherwise every sample is multiplied in fixed point with
/// rounding and saturated; the returned count is how many scaled samples fell
/// outside `i16` and were saturated. No limiter and no soft knee: the count is
/// reported, never hidden.
///
/// A non-finite `db` is a caller bug; it is treated as 0 dB rather than turning a
/// reply into silence or full-scale noise.
// TODO(reply-gain-no-limiter): limit the peak rather than hard-clip it.
pub fn apply_gain_db(pcm: &Arc<[i16]>, db: f32) -> (Arc<[i16]>, u32) {
    if db == 0.0 || !db.is_finite() {
        return (Arc::clone(pcm), 0);
    }
    // Saturate the factor itself so an absurd gain cannot overflow the i64
    // product; anything above 2^16 already rails every non-zero sample.
    let factor = (f64::from(db_to_linear(db)) * f64::from(1u32 << Q15_SHIFT))
        .round()
        .clamp(0.0, f64::from(1u32 << 31)) as i64;
    let half = 1i64 << (Q15_SHIFT - 1);
    let mut clipped = 0u32;
    let out: Arc<[i16]> = pcm
        .iter()
        .map(|&s| {
            let prod = i64::from(s) * factor;
            let rounded = if prod >= 0 {
                (prod + half) >> Q15_SHIFT
            } else {
                -((-prod + half) >> Q15_SHIFT)
            };
            let sat = rounded.clamp(i64::from(i16::MIN), i64::from(i16::MAX));
            if sat != rounded {
                clipped = clipped.saturating_add(1);
            }
            sat as i16
        })
        .collect();
    (out, clipped)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arc(v: &[i16]) -> Arc<[i16]> {
        Arc::from(v)
    }

    #[test]
    fn zero_db_is_pointer_identity() {
        let pcm = arc(&[1, -2, 3, i16::MAX, i16::MIN]);
        let (out, clipped) = apply_gain_db(&pcm, 0.0);
        assert!(Arc::ptr_eq(&pcm, &out));
        assert_eq!(clipped, 0);
    }

    #[test]
    fn minus_six_db_halves_full_scale_within_rounding() {
        let pcm = arc(&[i16::MAX, i16::MIN, 1000, -1000, 0]);
        let (out, clipped) = apply_gain_db(&pcm, -6.0206);
        assert_eq!(clipped, 0);
        for (a, b) in pcm.iter().zip(out.iter()) {
            let want = f64::from(*a) / 2.0;
            assert!(
                (f64::from(*b) - want).abs() <= 1.0,
                "{a} -> {b}, want ~{want}"
            );
        }
    }

    #[test]
    fn plus_six_db_on_half_scale_ramp_clips_expected_count() {
        // A ramp over [-16384, 16383]; +6.0206 dB doubles. Doubling saturates
        // exactly the samples whose double falls outside i16: below -16384 none
        // (−16384·2 = −32768 fits), above 16383 none (16383·2 = 32766 fits) — so
        // extend the ramp by 100 samples each side past the half-scale points.
        let ramp: Vec<i16> = (-16_484..=16_483).map(|v| v as i16).collect();
        let pcm: Arc<[i16]> = Arc::from(ramp);
        let (out, clipped) = apply_gain_db(&pcm, 6.0206);
        // Samples below −16384 (100 of them) and above 16383 (100 of them) clip.
        assert_eq!(clipped, 200);
        assert_eq!(out[0], i16::MIN);
        assert_eq!(out[out.len() - 1], i16::MAX);
    }

    #[test]
    fn saturation_never_wraps() {
        let pcm = arc(&[i16::MAX, i16::MIN, 20_000, -20_000, 1, -1]);
        let (out, clipped) = apply_gain_db(&pcm, 24.0);
        assert_eq!(&out[..4], &[i16::MAX, i16::MIN, i16::MAX, i16::MIN]);
        // ±1 × 15.85 stays in range.
        assert_eq!(&out[4..], &[16, -16]);
        assert_eq!(clipped, 4);
    }

    #[test]
    fn huge_gain_saturates_rather_than_overflowing() {
        let pcm = arc(&[1, -1, 0]);
        let (out, clipped) = apply_gain_db(&pcm, 400.0);
        assert_eq!(&out[..], &[i16::MAX, i16::MIN, 0]);
        assert_eq!(clipped, 2);
    }

    #[test]
    fn non_finite_gain_is_identity() {
        let pcm = arc(&[5, -5]);
        for db in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let (out, clipped) = apply_gain_db(&pcm, db);
            assert!(Arc::ptr_eq(&pcm, &out));
            assert_eq!(clipped, 0);
        }
    }

    #[test]
    fn large_cut_rounds_toward_zero_not_negative_one() {
        let pcm = arc(&[3, -3]);
        let (out, _) = apply_gain_db(&pcm, -60.0);
        assert_eq!(&out[..], &[0, 0]);
    }
}
