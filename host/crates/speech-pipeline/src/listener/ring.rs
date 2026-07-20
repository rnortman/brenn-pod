//! `PcmRing`: the listener's rolling PCM buffer, keyed on absolute sample index.
//!
//! The endpointer decides utterance boundaries as sample-index spans; the ring is
//! where those spans are turned back into audio — [`carve`](PcmRing::carve) slices
//! `[start, end)` out of the retained audio for STT, and continuation-concat reads
//! the same way (a resumed utterance carves from its original start). Audio is
//! stored as contiguous *runs* of `(base_index, samples)` rather than one flat
//! buffer, so a gap in the stream (a dropped chunk, or a pause that outlived the
//! device hangover so two segments don't abut) is preserved as a real hole and
//! carved back as silence — never as a splice that shifts every later sample.
//!
//! Capacity bounds the retained *stored* samples (gaps cost nothing); the oldest
//! audio is evicted first. Sized by the listener to `max_utterance + preroll_pad`.
//!
//! Pushes may *overlap* retained audio: a transport segment's preroll is stamped
//! with the samples' original capture indexes, so a segment opening less than one
//! preroll after the previous one closed re-sends audio the ring already holds.
//! The overlapping region is by construction the same capture samples (a genuinely
//! different stream arrives as a reconnect/discontinuity, which resets the ring),
//! so [`push`](PcmRing::push) dedupes it first-write-wins and reports the trimmed
//! count for the listener's stats.

use std::collections::VecDeque;
use std::sync::Arc;

/// One contiguous run of PCM at a known absolute start index. `end()` is one past
/// the last sample. Samples live in a `VecDeque` so steady-state head eviction
/// (the sustained-audio case, where a run fills the ring and every push trims its
/// head) is O(evicted) rather than memmoving the whole run each chunk.
#[derive(Debug, Clone)]
struct Run {
    start: u64,
    samples: VecDeque<i16>,
}

impl Run {
    fn end(&self) -> u64 {
        self.start + self.samples.len() as u64
    }
}

/// A rolling PCM buffer of `(index, pcm)` runs. Push contiguous audio as it
/// arrives; carve absolute-index spans back out. Discontiguous pushes start a new
/// run (a preserved gap); [`reset`](PcmRing::reset) clears everything on a stream
/// discontinuity.
pub struct PcmRing {
    capacity: usize,
    stored: usize,
    runs: Vec<Run>,
}

impl PcmRing {
    /// A ring retaining at most `capacity` stored samples.
    pub fn new(capacity: usize) -> PcmRing {
        PcmRing {
            capacity,
            stored: 0,
            runs: Vec::new(),
        }
    }

    /// Drop all retained audio. Called on a pod reconnect or sample-index
    /// discontinuity so a carve never spans a hole left by stale state.
    pub fn reset(&mut self) {
        self.runs.clear();
        self.stored = 0;
    }

    /// Total retained samples (excludes gaps).
    pub fn len(&self) -> usize {
        self.stored
    }

    /// Whether any audio is retained.
    pub fn is_empty(&self) -> bool {
        self.stored == 0
    }

    /// Append `pcm` at absolute index `first_sample_index`. Audio contiguous with
    /// the last run extends it; a forward jump starts a new run (preserving the
    /// gap). Then evict oldest audio down to `capacity`.
    ///
    /// An index reaching back into retained audio is a re-send (segment preroll),
    /// not a caller bug: the duplicate prefix `[first_sample_index, last.end())` is
    /// dropped and only the new suffix appended, so retained samples win. A push
    /// wholly covered by retained audio is a no-op. Overlap is judged against the
    /// last run's end alone — run starts are monotonic, so it is the maximum — which
    /// means a re-send reaching back past an older inter-run gap is discarded along
    /// with the prefix: the gap stays silence, matching what STT already heard.
    ///
    /// Returns the number of duplicate samples trimmed. The listener accumulates it
    /// as a stat: routine preroll dedup is a steady, explainable rate, so a spike
    /// surfaces a device re-sending a *different* range under the same indexes, or a
    /// host index-math regression. Since the dedup is silent either way, that counter
    /// is the only tripwire for those.
    pub fn push(&mut self, first_sample_index: u64, pcm: &[i16]) -> usize {
        if pcm.is_empty() {
            return 0;
        }
        let mut start = first_sample_index;
        let mut pcm = pcm;
        let mut trimmed = 0;
        if let Some(last) = self.runs.last() {
            if start < last.end() {
                trimmed = (last.end() - start).min(pcm.len() as u64) as usize;
                pcm = &pcm[trimmed..];
                start += trimmed as u64;
                if pcm.is_empty() {
                    return trimmed;
                }
            }
        }
        match self.runs.last_mut() {
            Some(last) if start == last.end() => {
                last.samples.extend(pcm.iter().copied());
            }
            _ => self.runs.push(Run {
                start,
                samples: pcm.iter().copied().collect(),
            }),
        }
        self.stored += pcm.len();
        self.evict();
        trimmed
    }

    /// Evict oldest audio until stored samples fit `capacity`, trimming the head
    /// of the oldest run when a whole-run drop would overshoot.
    fn evict(&mut self) {
        while self.stored > self.capacity {
            let overflow = self.stored - self.capacity;
            let front = &mut self.runs[0];
            if front.samples.len() <= overflow {
                self.stored -= front.samples.len();
                self.runs.remove(0);
            } else {
                front.samples.drain(..overflow);
                front.start += overflow as u64;
                self.stored -= overflow;
            }
        }
    }

    /// Carve `[start, end)` into a fresh PCM buffer of length `end - start`.
    /// Indexes covered by a retained run take that run's samples; every other
    /// index — a gap between runs, or audio already evicted — is silence (`0`).
    /// This is the exact splice rule replay uses, so STT hears what the carve
    /// reproduces. An empty or inverted span yields an empty buffer.
    pub fn carve(&self, start: u64, end: u64) -> Arc<[i16]> {
        if end <= start {
            return Arc::from(Vec::new());
        }
        let mut out = vec![0_i16; (end - start) as usize];
        for run in &self.runs {
            let lo = run.start.max(start);
            let hi = run.end().min(end);
            if lo >= hi {
                continue;
            }
            let dst = (lo - start) as usize;
            let src = (lo - run.start) as usize;
            let n = (hi - lo) as usize;
            // A `VecDeque`'s storage is two contiguous slices; the logical range
            // `[src, src+n)` may straddle the split.
            let (a, b) = run.samples.as_slices();
            for k in 0..n {
                let idx = src + k;
                out[dst + k] = if idx < a.len() {
                    a[idx]
                } else {
                    b[idx - a.len()]
                };
            }
        }
        Arc::from(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Contiguous pushes coalesce into one run; a carve within it returns exactly
    /// those samples.
    #[test]
    fn contiguous_push_and_carve() {
        let mut ring = PcmRing::new(1000);
        ring.push(0, &[1, 2, 3, 4]);
        ring.push(4, &[5, 6, 7, 8]);
        assert_eq!(ring.len(), 8);
        assert_eq!(&*ring.carve(2, 6), &[3, 4, 5, 6]);
        assert_eq!(&*ring.carve(0, 8), &[1, 2, 3, 4, 5, 6, 7, 8]);
    }

    /// A forward jump leaves a gap; the carve splices it as silence and keeps
    /// absolute alignment on both sides.
    #[test]
    fn gap_is_carved_as_silence() {
        let mut ring = PcmRing::new(1000);
        ring.push(0, &[1, 2, 3, 4]);
        ring.push(8, &[5, 6, 7, 8]); // gap at [4, 8)
                                     // Carve spanning the gap: 4 real, 4 silence, 4 real.
        assert_eq!(&*ring.carve(0, 12), &[1, 2, 3, 4, 0, 0, 0, 0, 5, 6, 7, 8]);
        // Carve entirely inside the gap is all silence.
        assert_eq!(&*ring.carve(5, 7), &[0, 0]);
    }

    /// A carve reaching before the earliest retained sample (evicted or never
    /// stored) pads the missing prefix with silence.
    #[test]
    fn carve_before_retained_is_silence() {
        let mut ring = PcmRing::new(1000);
        ring.push(100, &[9, 9]);
        assert_eq!(&*ring.carve(98, 104), &[0, 0, 9, 9, 0, 0]);
    }

    /// Capacity eviction drops the oldest audio; carving evicted indexes returns
    /// silence while retained tail audio survives.
    #[test]
    fn capacity_evicts_oldest() {
        let mut ring = PcmRing::new(4);
        ring.push(0, &[1, 2, 3, 4]);
        ring.push(4, &[5, 6, 7, 8]); // total 8 > cap 4 → oldest 4 evicted
        assert_eq!(ring.len(), 4);
        // [0,4) evicted → silence; [4,8) retained.
        assert_eq!(&*ring.carve(0, 8), &[0, 0, 0, 0, 5, 6, 7, 8]);
    }

    /// Eviction can trim the head of a run without dropping it whole.
    #[test]
    fn eviction_trims_run_head() {
        let mut ring = PcmRing::new(3);
        ring.push(0, &[1, 2, 3, 4, 5]); // 5 > cap 3 → head 2 trimmed
        assert_eq!(ring.len(), 3);
        assert_eq!(&*ring.carve(0, 5), &[0, 0, 3, 4, 5]);
    }

    /// An inverted or empty span carves nothing.
    #[test]
    fn empty_span_is_empty() {
        let mut ring = PcmRing::new(100);
        ring.push(0, &[1, 2, 3]);
        assert!(ring.carve(2, 2).is_empty());
        assert!(ring.carve(3, 1).is_empty());
    }

    /// Reset clears all runs; a post-reset carve of previously-stored indexes is
    /// silence.
    #[test]
    fn reset_clears() {
        let mut ring = PcmRing::new(100);
        ring.push(0, &[1, 2, 3, 4]);
        ring.reset();
        assert!(ring.is_empty());
        assert_eq!(&*ring.carve(0, 4), &[0, 0, 0, 0]);
    }

    /// An empty push is a no-op.
    #[test]
    fn empty_push_is_noop() {
        let mut ring = PcmRing::new(100);
        assert_eq!(ring.push(0, &[]), 0);
        assert!(ring.is_empty());
    }

    /// The segment-preroll case: a push straddling the last run's end keeps the
    /// retained samples for the overlap and appends only the new suffix, reporting
    /// the trimmed count. Storage accounting counts the suffix only.
    #[test]
    fn overlapping_push_trims_the_duplicate_prefix() {
        let mut ring = PcmRing::new(1000);
        ring.push(0, &[1, 2, 3, 4]);
        // Re-sends [2,4) (as 9s — first write wins, so 3,4 survive) then adds [4,6).
        assert_eq!(ring.push(2, &[9, 9, 5, 6]), 2);
        assert_eq!(ring.len(), 6);
        assert_eq!(&*ring.carve(0, 6), &[1, 2, 3, 4, 5, 6]);
    }

    /// A push wholly covered by retained audio stores nothing and reports its whole
    /// length as trimmed.
    #[test]
    fn fully_covered_push_is_a_noop() {
        let mut ring = PcmRing::new(1000);
        ring.push(0, &[1, 2, 3, 4]);
        assert_eq!(ring.push(1, &[9, 9]), 2);
        assert_eq!(ring.len(), 4);
        assert_eq!(&*ring.carve(0, 4), &[1, 2, 3, 4]);
    }

    /// Overlap is judged against the last run's end, so a re-send reaching back
    /// across an older inter-run gap is discarded with the duplicate prefix — the
    /// gap stays silence rather than being back-filled.
    #[test]
    fn overlap_past_a_gap_leaves_the_gap_silent() {
        let mut ring = PcmRing::new(1000);
        ring.push(0, &[1, 2, 3, 4]);
        ring.push(8, &[5, 6, 7, 8]); // gap at [4, 8)
                                     // Re-send reaching back to 6: [6,12) overlaps the last run's [8,12)…
        assert_eq!(ring.push(6, &[7, 7, 9, 9, 9, 9, 10, 11]), 6);
        // …so only [12,14) is new; the [4,8) gap is untouched silence.
        assert_eq!(
            &*ring.carve(0, 14),
            &[1, 2, 3, 4, 0, 0, 0, 0, 5, 6, 7, 8, 10, 11]
        );
    }
}
