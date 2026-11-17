//! The signal analysis of the routing hardware tests: the noise they play,
//! whether a capture channel carries it, the delay between two signals window
//! by window and its steps, runs of zero frames, and levels. Correlation is
//! computed directly over a bounded range of lags; nothing here needs an FFT.
//!
//! Its own tests run on signals made in code and are not ignored.

/// The rate every signal here is at.
pub const RATE_HZ: u32 = 16_000;

/// The played noise's level: −20 dBFS RMS, as a fraction of full scale.
pub const NOISE_RMS: f64 = 0.1;

/// The played noise's band, in Hz.
pub const NOISE_BAND_HZ: (f64, f64) = (100.0, 6_000.0);

/// The figure's window and hop, in frames: 0.25 s and 0.125 s.
pub const WINDOW: usize = 4_000;
pub const HOP: usize = 2_000;

/// How far either side of the coarse lag a window's peak is searched, in frames.
pub const FINE: usize = 64;

/// How far either side of the nominal position the coarse lag is searched, in
/// frames.
pub const COARSE: usize = 8_000;

/// A channel carries the played signal at a figure of at least this, above every
/// raw microphone's.
pub const CARRIES_AT: f64 = 0.8;

/// A channel does not carry the played signal at a figure of at most this.
pub const ABSENT_AT: f64 = 0.2;

/// The length of the band-pass filter that shapes the noise, in taps (odd): a
/// Blackman window this long has a transition about 60 Hz wide at 16 kHz,
/// narrow enough for the 100 Hz edge.
const TAPS: usize = 1_501;

/// A seeded generator: splitmix64.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in (0, 1].
    fn unit(&mut self) -> f64 {
        ((self.next_u64() >> 11) as f64 + 1.0) / (1u64 << 53) as f64
    }

    /// Standard normal, by Box–Muller.
    fn gaussian(&mut self) -> f64 {
        let (u, v) = (self.unit(), self.unit());
        (-2.0 * u.ln()).sqrt() * (2.0 * std::f64::consts::PI * v).cos()
    }
}

fn sinc(x: f64) -> f64 {
    if x == 0.0 {
        1.0
    } else {
        (std::f64::consts::PI * x).sin() / (std::f64::consts::PI * x)
    }
}

/// A linear-phase band-pass filter from `lo` to `hi` Hz: a Blackman-windowed
/// difference of two sincs.
fn band_pass(lo: f64, hi: f64) -> Vec<f64> {
    let (fl, fh) = (lo / f64::from(RATE_HZ), hi / f64::from(RATE_HZ));
    let mid = (TAPS / 2) as f64;
    (0..TAPS)
        .map(|n| {
            let m = n as f64 - mid;
            let phase = 2.0 * std::f64::consts::PI * n as f64 / (TAPS - 1) as f64;
            let window = 0.42 - 0.5 * phase.cos() + 0.08 * (2.0 * phase).cos();
            window * (2.0 * fh * sinc(2.0 * fh * m) - 2.0 * fl * sinc(2.0 * fl * m))
        })
        .collect()
}

/// `frames` of Gaussian noise band-limited to [`NOISE_BAND_HZ`] at [`NOISE_RMS`],
/// from `seed`, as 16-bit samples. Panics if a sample would clip: the level is
/// the test's to state, never something trimmed to fit.
pub fn noise(seed: u64, frames: usize) -> Vec<i16> {
    let mut rng = Rng(seed);
    let white: Vec<f64> = (0..frames + TAPS - 1).map(|_| rng.gaussian()).collect();
    let h = band_pass(NOISE_BAND_HZ.0, NOISE_BAND_HZ.1);
    let shaped: Vec<f64> = (0..frames)
        .map(|n| h.iter().zip(&white[n..n + TAPS]).map(|(a, b)| a * b).sum())
        .collect();
    let rms = (shaped.iter().map(|s| s * s).sum::<f64>() / frames as f64).sqrt();
    let scale = NOISE_RMS * 32_767.0 / rms;
    shaped
        .iter()
        .map(|s| {
            let v = (s * scale).round();
            assert!(v.abs() <= 32_767.0, "the noise clips at sample value {v}");
            v as i16
        })
        .collect()
}

/// `samples` as `f64`.
pub fn widen(samples: &[i16]) -> Vec<f64> {
    samples.iter().map(|&s| f64::from(s)).collect()
}

/// The sum of `a[n] · b[at + n]` over every `n` for which both exist, `at` being
/// signed.
fn dot_at(a: &[f64], b: &[f64], at: isize) -> f64 {
    let start = usize::try_from(-at).unwrap_or(0);
    let end = usize::try_from(b.len() as isize - at)
        .unwrap_or(0)
        .min(a.len());
    (start..end)
        .map(|n| a[n] * b[(at + n as isize) as usize])
        .sum()
}

/// Where in `channel` the `reference` lies: the offset, within `search` frames
/// of `nominal`, at which the magnitude of their cross-correlation peaks.
pub fn coarse_offset(reference: &[f64], channel: &[f64], nominal: isize, search: usize) -> isize {
    let search = search as isize;
    (nominal - search..=nominal + search)
        .map(|at| (at, dot_at(reference, channel, at).abs()))
        .fold((nominal, f64::NEG_INFINITY), |best, (at, c)| {
            if c > best.1 { (at, c) } else { best }
        })
        .0
}

/// The normalised cross-correlation of `a` with the stretch of `b` starting at
/// `at`, or `None` when that stretch is not wholly inside `b` or either side is
/// silent.
fn normalised(a: &[f64], b: &[f64], at: isize) -> Option<f64> {
    let at = usize::try_from(at).ok()?;
    let b = b.get(at..at + a.len())?;
    let (mut ab, mut aa, mut bb) = (0.0, 0.0, 0.0);
    for (x, y) in a.iter().zip(b) {
        ab += x * y;
        aa += x * x;
        bb += y * y;
    }
    (aa > 0.0 && bb > 0.0).then(|| ab / (aa * bb).sqrt())
}

/// The median of `values`, which must not be empty.
pub fn median(values: &[f64]) -> f64 {
    assert!(!values.is_empty(), "the median of nothing");
    let mut v = values.to_vec();
    v.sort_by(f64::total_cmp);
    let mid = v.len() / 2;
    if v.len() % 2 == 1 {
        v[mid]
    } else {
        (v[mid - 1] + v[mid]) / 2.0
    }
}

/// How well `channel` carries `reference`, placed at `offset` in it: over
/// windows of [`WINDOW`] frames at a hop of [`HOP`], the peak magnitude of the
/// normalised cross-correlation within [`FINE`] frames of the offset, and the
/// median over the windows. A window the channel does not wholly hold at some
/// lag, or in which it is silent, counts as zero.
pub fn figure(reference: &[f64], channel: &[f64], offset: isize) -> f64 {
    assert!(
        reference.len() >= WINDOW,
        "a reference of {} frames is shorter than one window",
        reference.len()
    );
    let fine = FINE as isize;
    let peaks: Vec<f64> = (0..=reference.len() - WINDOW)
        .step_by(HOP)
        .map(|w| {
            let piece = &reference[w..w + WINDOW];
            (-fine..=fine)
                .filter_map(|d| normalised(piece, channel, offset + w as isize + d))
                .map(f64::abs)
                .fold(0.0, f64::max)
        })
        .collect();
    median(&peaks)
}

/// How much of a long play's start places it first: two seconds.
pub const PLACE_SPAN: usize = 2 * RATE_HZ as usize;

/// Below this peak magnitude of normalised correlation a window is not placed:
/// two unrelated signals over [`WINDOW`] frames correlate below 0.1 at every
/// lag searched.
pub const PLACED_AT: f64 = 0.1;

/// Where in `channel` the whole of `reference` lies: placed first by its first
/// [`PLACE_SPAN`] frames within `search` frames of `nominal`, then by all of it
/// within [`FINE`] frames of that.
pub fn place_whole(reference: &[f64], channel: &[f64], nominal: isize, search: usize) -> isize {
    let head = &reference[..reference.len().min(PLACE_SPAN)];
    let first = coarse_offset(head, channel, nominal, search);
    coarse_offset(reference, channel, first, FINE)
}

/// How far a window's correlogram is shifted against the template, either way,
/// in frames.
pub const SHIFT: usize = 40;

/// The normalised cross-correlation of `a[from..from + len]` with `b`, at each
/// lag from `lag - FINE` to `lag + FINE`, `b` being read at the stretch's own
/// position in `a` plus the lag; zero at a lag at which `b` does not wholly hold
/// the stretch or is silent. `None` when `a` does not wholly hold the stretch.
pub fn correlogram(a: &[f64], from: usize, len: usize, b: &[f64], lag: isize) -> Option<Vec<f64>> {
    let fine = FINE as isize;
    let piece = a.get(from..from + len)?;
    Some(
        (lag - fine..=lag + fine)
            .map(|d| normalised(piece, b, from as isize + d).unwrap_or(0.0))
            .collect(),
    )
}

/// The lag of a correlogram centred on `lag` at which its magnitude peaks.
pub fn peak_lag(correlogram: &[f64], lag: isize) -> isize {
    let i = (0..correlogram.len())
        .max_by(|&i, &j| correlogram[i].abs().total_cmp(&correlogram[j].abs()))
        .expect("an empty correlogram");
    lag - FINE as isize + i as isize
}

/// The shift, within [`SHIFT`] frames either way, at which `c` best matches
/// `template`: the shift `s` maximising the normalised correlation of `c[i]`
/// with `template[i - s]` over the indices both hold.
fn best_shift(c: &[f64], template: &[f64]) -> isize {
    let shift = SHIFT as isize;
    let n = c.len() as isize;
    let score = |s: isize| {
        let (mut ct, mut cc, mut tt) = (0.0, 0.0, 0.0);
        for i in s.max(0)..n.min(n + s) {
            let (x, y) = (c[i as usize], template[(i - s) as usize]);
            ct += x * y;
            cc += x * x;
            tt += y * y;
        }
        if cc > 0.0 && tt > 0.0 {
            ct / (cc * tt).sqrt()
        } else {
            f64::NEG_INFINITY
        }
    };
    (-shift..=shift)
        .map(|s| (s, score(s)))
        .fold((0, f64::NEG_INFINITY), |best, (s, v)| {
            if v > best.1 { (s, v) } else { best }
        })
        .0
}

/// The delay of `b` behind `a`, window by window, read against `template`, a
/// [`correlogram`] of the same two signals centred on the same `lag`. For each
/// window of [`WINDOW`] frames at a hop of [`HOP`] over `a[from..from + len]`,
/// the window's own correlogram is matched against the template shifted by up
/// to [`SHIFT`] frames, and the delay is the template's [`peak_lag`] plus the
/// best shift. Matching the whole correlogram rather than taking its peak
/// keeps a window's delay where it is when two arrivals of the signal in `b`
/// are of nearly equal strength and the peak would jump between them. `None`
/// for a window `a` does not wholly hold, or whose correlogram peaks below
/// [`PLACED_AT`].
pub fn track(
    a: &[f64],
    from: usize,
    len: usize,
    b: &[f64],
    lag: isize,
    template: &[f64],
) -> Vec<Option<isize>> {
    assert_eq!(template.len(), 2 * FINE + 1, "a template of another length");
    let at = peak_lag(template, lag);
    (0..=len.saturating_sub(WINDOW))
        .step_by(HOP)
        .map(|w| {
            let c = correlogram(a, from + w, WINDOW, b, lag)?;
            let peak = c.iter().fold(0.0, |m: f64, x| m.max(x.abs()));
            (peak >= PLACED_AT).then(|| at + best_shift(&c, template))
        })
        .collect()
}

/// A change in a tracked delay: the window it is first seen in, and its size in
/// frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Step {
    pub window: usize,
    pub size: isize,
}

/// The steps of a tracked delay: each window whose delay differs by more than
/// one frame from the level held since the last step (or since the first placed
/// window), the level then becoming that window's delay. A change of one frame
/// is not a step, so a peak that flickers between adjacent lags is none.
pub fn steps(delays: &[Option<isize>]) -> Vec<Step> {
    let mut level: Option<isize> = None;
    let mut found = Vec::new();
    for (window, d) in delays.iter().enumerate() {
        let Some(d) = *d else { continue };
        match level {
            Some(l) if (d - l).abs() > 1 => {
                found.push(Step {
                    window,
                    size: d - l,
                });
                level = Some(d);
            }
            Some(_) => {}
            None => level = Some(d),
        }
    }
    found
}

/// The runs of at least `min` frames in which every one of `channels` is zero,
/// as (first frame, length). The channels must be of one length.
pub fn zero_runs(channels: &[&[i16]], min: usize) -> Vec<(usize, usize)> {
    let len = channels.first().map_or(0, |c| c.len());
    assert!(
        channels.iter().all(|c| c.len() == len),
        "channels of different lengths"
    );
    let mut runs = Vec::new();
    let mut start = None;
    for n in 0..=len {
        let zero = n < len && channels.iter().all(|c| c[n] == 0);
        match (zero, start) {
            (true, None) => start = Some(n),
            (false, Some(s)) => {
                if n - s >= min {
                    runs.push((s, n - s));
                }
                start = None;
            }
            _ => {}
        }
    }
    runs
}

/// The mean square of `x`; zero for an empty `x`.
pub fn power(x: &[f64]) -> f64 {
    if x.is_empty() {
        return 0.0;
    }
    x.iter().map(|s| s * s).sum::<f64>() / x.len() as f64
}

/// The power `num` against the power `den` in dB, or `None` unless both are
/// positive.
pub fn db(num: f64, den: f64) -> Option<f64> {
    (num > 0.0 && den > 0.0).then(|| 10.0 * (num / den).log10())
}

/// Whether a channel carries the played signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// A figure of at least [`CARRIES_AT`], above every raw microphone's.
    Carries,
    /// A figure of at most [`ABSENT_AT`].
    DoesNotCarry,
    /// Anything else.
    Ambiguous,
}

/// The class of a channel with `figure`, against the raw microphones' figures in
/// the same capture.
pub fn classify(figure: f64, microphones: &[f64]) -> Class {
    if figure >= CARRIES_AT && microphones.iter().all(|&m| figure > m) {
        Class::Carries
    } else if figure <= ABSENT_AT {
        Class::DoesNotCarry
    } else {
        Class::Ambiguous
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `reference` delayed by `delay` frames into a buffer of `len`, zero
    /// elsewhere.
    fn delayed(reference: &[i16], delay: usize, len: usize) -> Vec<f64> {
        let mut out = vec![0.0; len];
        for (n, &s) in reference.iter().enumerate() {
            if let Some(slot) = out.get_mut(delay + n) {
                *slot = f64::from(s);
            }
        }
        out
    }

    /// [`track`] against a template taken over the first [`PLACE_SPAN`] frames
    /// tracked, or all of them if fewer.
    fn track_from_start(
        a: &[f64],
        from: usize,
        len: usize,
        b: &[f64],
        lag: isize,
    ) -> Vec<Option<isize>> {
        let template = correlogram(a, from, len.min(PLACE_SPAN), b, lag).unwrap();
        track(a, from, len, b, lag, &template)
    }

    /// The magnitude of the band-pass filter's response at `hz`.
    fn gain_at(hz: f64) -> f64 {
        let w = 2.0 * std::f64::consts::PI * hz / f64::from(RATE_HZ);
        let (re, im) = band_pass(NOISE_BAND_HZ.0, NOISE_BAND_HZ.1)
            .iter()
            .enumerate()
            .fold((0.0, 0.0), |(re, im), (n, h)| {
                (re + h * (w * n as f64).cos(), im - h * (w * n as f64).sin())
            });
        f64::hypot(re, im)
    }

    #[test]
    fn the_noise_is_at_its_level_and_repeats_from_its_seed() {
        let a = noise(7, 32_000);
        assert_eq!(a, noise(7, 32_000));
        assert_ne!(a, noise(8, 32_000));
        let x = widen(&a);
        let rms = (x.iter().map(|s| s * s).sum::<f64>() / x.len() as f64).sqrt() / 32_767.0;
        assert!((rms - NOISE_RMS).abs() < 1e-3, "RMS {rms}");
    }

    #[test]
    fn the_filter_passes_the_band_and_stops_outside_it() {
        for inside in [200.0, 1_000.0, 5_000.0] {
            let g = gain_at(inside);
            assert!((g - 1.0).abs() < 0.01, "{inside} Hz: gain {g}");
        }
        for outside in [0.0, 20.0, 7_000.0, 7_900.0] {
            let g = gain_at(outside);
            assert!(g < 1e-3, "{outside} Hz: gain {g}");
        }
    }

    #[test]
    fn a_known_delay_is_found() {
        let reference = noise(1, 8_000);
        let channel = delayed(&reference, 1_237, 12_000);
        let at = coarse_offset(&widen(&reference), &channel, 1_200, 100);
        assert_eq!(at, 1_237);
    }

    #[test]
    fn an_inverted_copy_is_found_at_its_delay() {
        let reference = noise(2, 8_000);
        let channel: Vec<f64> = delayed(&reference, 300, 10_000)
            .iter()
            .map(|s| -s)
            .collect();
        let r = widen(&reference);
        assert_eq!(coarse_offset(&r, &channel, 250, 100), 300);
        assert!(figure(&r, &channel, 300) > 0.99);
    }

    #[test]
    fn a_delayed_copy_with_one_slip_carries_the_signal() {
        let reference = noise(3, 32_000);
        // Delayed by 300 frames; 8 frames dropped one second in.
        let mut slipped: Vec<i16> = vec![0; 300];
        slipped.extend_from_slice(&reference[..16_000]);
        slipped.extend_from_slice(&reference[16_008..]);
        slipped.resize(34_000, 0);
        let channel = widen(&slipped);
        let r = widen(&reference);
        let at = coarse_offset(&r, &channel, 0, 400);
        assert!((292..=300).contains(&at), "coarse offset {at}");
        let f = figure(&r, &channel, at);
        assert!(f > 0.95, "figure {f}");
        assert_eq!(classify(f, &[0.3, 0.4]), Class::Carries);
    }

    #[test]
    fn unrelated_noise_does_not_carry_the_signal() {
        let reference = widen(&noise(4, 32_000));
        let channel = widen(&noise(5, 34_000));
        let at = coarse_offset(&reference, &channel, 1_000, 400);
        let f = figure(&reference, &channel, at);
        assert!(f < 0.1, "figure {f}");
        assert_eq!(classify(f, &[0.3]), Class::DoesNotCarry);
    }

    #[test]
    fn a_silent_channel_has_a_figure_of_zero() {
        let reference = widen(&noise(6, 8_000));
        assert_eq!(figure(&reference, &[0.0; 10_000], 100), 0.0);
    }

    #[test]
    fn the_classes_follow_the_thresholds_and_the_microphones() {
        assert_eq!(classify(0.9, &[0.5, 0.6]), Class::Carries);
        assert_eq!(classify(0.9, &[0.5, 0.95]), Class::Ambiguous);
        assert_eq!(classify(0.8, &[]), Class::Carries);
        assert_eq!(classify(0.5, &[0.1]), Class::Ambiguous);
        assert_eq!(classify(0.2, &[0.0]), Class::DoesNotCarry);
    }

    #[test]
    fn a_long_play_is_placed_by_its_start_and_then_as_a_whole() {
        let reference = noise(9, 40_000);
        let channel = delayed(&reference, 2_345, 45_000);
        assert_eq!(place_whole(&widen(&reference), &channel, 2_300, 100), 2_345);
    }

    /// The step positions in [`a_delay_that_steps_is_tracked_window_by_window`],
    /// in frames of the reference, with the delay after each.
    const STEPPED: [(usize, isize); 3] = [(20_000, 92), (41_000, 116), (61_000, 119)];

    #[test]
    fn a_delay_that_steps_is_tracked_window_by_window() {
        let reference = noise(10, 80_000);
        // Delayed 100 frames; then 8 frames dropped (−8), 24 zero frames
        // inserted (+24), and 3 frames repeated (+3).
        let (p1, p2, p3) = (STEPPED[0].0, STEPPED[1].0, STEPPED[2].0);
        let mut stepped: Vec<i16> = vec![0; 100];
        stepped.extend_from_slice(&reference[..p1]);
        stepped.extend_from_slice(&reference[p1 + 8..p2]);
        stepped.extend_from_slice(&[0; 24]);
        stepped.extend_from_slice(&reference[p2..p3]);
        stepped.extend_from_slice(&reference[p3 - 3..]);
        stepped.resize(82_000, 0);
        let r = widen(&reference);
        let delays = track_from_start(&r, 0, r.len(), &widen(&stepped), 100);
        assert_eq!(delays[0], Some(100));
        assert_eq!(delays.last().copied().flatten(), Some(119));
        let found = steps(&delays);
        let sizes: Vec<isize> = found.iter().map(|s| s.size).collect();
        assert_eq!(sizes, [-8, 24, 3], "steps {found:?}");
        for (step, (at, after)) in found.iter().zip(STEPPED) {
            let start = step.window * HOP;
            assert!(
                start <= at && at < start + WINDOW,
                "step {step:?} is not in a window holding frame {at}"
            );
            assert_eq!(delays[step.window], Some(after), "delay after {step:?}");
        }
    }

    #[test]
    fn a_delay_that_holds_has_no_step() {
        let reference = noise(11, 40_000);
        let channel = delayed(&reference, 37, 42_000);
        let r = widen(&reference);
        let delays = track_from_start(&r, 0, r.len(), &channel, 30);
        assert!(delays.iter().all(|&d| d == Some(37)), "{delays:?}");
        assert_eq!(steps(&delays), []);
    }

    /// Two arrivals of the reference 26 and 29 frames late, of nearly equal
    /// strength, under independent noise twice their combined level, with a
    /// slip of `slip` frames (negative: dropped; positive: repeated) at frame
    /// 40 000 of the reference: tracked, the delay holds on each side of the
    /// slip and the slip is the one step found, in a window that holds it.
    fn two_nearly_equal_arrivals_hold_their_delay_across_a_slip(slip: isize) {
        const AT: usize = 40_000;
        let reference = noise(17, 80_000);
        let mut room = Rng(18);
        let echo = |n: usize| {
            let at = |d: usize| {
                n.checked_sub(d)
                    .and_then(|i| reference.get(i))
                    .map_or(0.0, |&s| f64::from(s))
            };
            0.5 * at(26) + 0.45 * at(29)
        };
        let resume = (AT + 26).checked_add_signed(-slip).unwrap();
        let channel: Vec<f64> = (0..AT + 26)
            .map(echo)
            .chain((resume..82_000).map(echo))
            .map(|s| s + 2.0 * 0.95 * NOISE_RMS * 32_767.0 * room.gaussian())
            .collect();
        let r = widen(&reference);
        let delays = track_from_start(&r, 0, r.len(), &channel, 27);
        let found = steps(&delays);
        assert_eq!(found.len(), 1, "steps {found:?} in {delays:?}");
        assert_eq!(found[0].size, slip, "{delays:?}");
        let start = found[0].window * HOP;
        assert!(start <= AT && AT < start + WINDOW, "{found:?}");
        let before = delays[0].unwrap();
        assert!(before == 26 || before == 29, "delay before {before}");
        assert!(
            delays[..found[0].window - 1]
                .iter()
                .all(|&d| d == Some(before)),
            "{delays:?}"
        );
        assert!(
            delays[found[0].window + 1..]
                .iter()
                .all(|&d| d == Some(before + slip)),
            "{delays:?}"
        );
    }

    #[test]
    fn two_nearly_equal_arrivals_in_noise_hold_their_delay_until_a_slip() {
        two_nearly_equal_arrivals_hold_their_delay_across_a_slip(-8);
    }

    #[test]
    fn a_slip_of_the_two_arrivals_own_spacing_is_one_step_of_that_size() {
        two_nearly_equal_arrivals_hold_their_delay_across_a_slip(3);
    }

    #[test]
    fn a_correlogram_peaks_at_the_delay() {
        let reference = noise(19, 10_000);
        let channel = delayed(&reference, 45, 11_000);
        let c = correlogram(&widen(&reference), 0, 8_000, &channel, 40).unwrap();
        assert_eq!(c.len(), 2 * FINE + 1);
        assert_eq!(peak_lag(&c, 40), 45);
        assert!(c[FINE + 5] > 0.99);
        assert_eq!(
            correlogram(&widen(&reference), 5_000, 8_000, &channel, 40),
            None
        );
    }

    #[test]
    fn a_delay_tracked_from_inside_a_channel_is_relative_to_it() {
        let reference = noise(12, 20_000);
        let a = delayed(&reference, 500, 22_000);
        let b = delayed(&reference, 531, 22_000);
        let delays = track_from_start(&a, 500, 20_000, &b, 25);
        assert!(delays.iter().all(|&d| d == Some(31)), "{delays:?}");
    }

    #[test]
    fn an_unrelated_or_silent_window_is_not_placed() {
        let a = widen(&noise(13, 12_000));
        let itself = correlogram(&a, 0, a.len(), &a, 0).unwrap();
        assert!(
            track(&a, 0, a.len(), &widen(&noise(14, 12_000)), 0, &itself)
                .iter()
                .all(Option::is_none)
        );
        assert!(
            track(&a, 0, a.len(), &[0.0; 12_000], 0, &itself)
                .iter()
                .all(Option::is_none)
        );
        // Windows past the end of `a` are not placed either.
        assert_eq!(
            track(&a, 10_000, 8_000, &a, 0, &itself)[..],
            [None, None, None]
        );
    }

    #[test]
    fn a_one_frame_flicker_is_not_a_step() {
        let delays = [
            Some(10),
            Some(11),
            None,
            Some(10),
            Some(11),
            Some(13),
            Some(12),
        ];
        assert_eq!(steps(&delays), [Step { window: 5, size: 3 }]);
    }

    #[test]
    fn a_zero_run_on_every_channel_is_found_to_the_frame() {
        let mut a = noise(15, 10_000);
        let mut b = noise(16, 10_000);
        a[5_000..5_024].fill(0);
        b[5_000..5_024].fill(0);
        // Shorter than the minimum: not a run.
        a[7_000..7_023].fill(0);
        b[7_000..7_023].fill(0);
        // Zero on one channel only: not a run.
        a[8_000..8_100].fill(0);
        assert_eq!(zero_runs(&[&a, &b], 24), [(5_000, 24)]);
        // A run that reaches the end is found.
        a[9_970..].fill(0);
        b[9_970..].fill(0);
        assert_eq!(zero_runs(&[&a, &b], 24), [(5_000, 24), (9_970, 30)]);
    }

    #[test]
    fn levels_are_powers_in_db() {
        assert_eq!(power(&[3.0, -3.0]), 9.0);
        assert_eq!(power(&[]), 0.0);
        let d = db(power(&[10.0]), power(&[1.0])).unwrap();
        assert!((d - 20.0).abs() < 1e-12, "{d}");
        assert_eq!(db(0.0, 1.0), None);
        assert_eq!(db(1.0, -1.0), None);
    }

    #[test]
    fn the_median_of_an_even_count_is_the_middle_pair_s_mean() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), 2.0);
        assert_eq!(median(&[4.0, 1.0, 3.0, 2.0]), 2.5);
    }
}
