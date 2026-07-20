//! [`ScoreStats`]: the per-model score accumulator behind the `model_stats`
//! observability line.
//!
//! The endpointer's transition events are silent exactly when the FSM never
//! transitions — a room where Silero returns 0.003 forever produces no line at
//! all, which is precisely the failure that needs diagnosing. This accumulator
//! closes that hole: it collects each model's per-chunk score and periodically
//! summarizes the distribution, so "what was the model actually returning" is
//! answerable from the log rather than from a rebuild.
//!
//! Bounded and never per-chunk. Scores accumulate until a flush point drains
//! them ([`ScoreStats::flush`]); the runtime caps the accumulation at
//! [`MODEL_STATS_FLUSH_CHUNKS`], which both bounds memory (~1 KiB) and
//! guarantees a heartbeat through long transition-free stretches.

use serde::Serialize;

/// Chunks after which the runtime force-flushes both accumulators. Keyed off
/// Silero's 32 ms chunk cadence — the finer of the two models — so this is
/// ~8.2 s of audio. A constant, not a config knob: it is an observability
/// cadence, not a tuning parameter.
pub const MODEL_STATS_FLUSH_CHUNKS: usize = 256;

/// One model's scores since the last flush, plus the sample span they cover.
#[derive(Debug, Default)]
pub struct ScoreStats {
    /// Absolute end index of the first accumulated chunk. Meaningless while
    /// `scores` is empty; set by the first [`record`](ScoreStats::record).
    first_chunk_end: u64,
    /// Absolute end index of the most recent accumulated chunk.
    last_chunk_end: u64,
    scores: Vec<f32>,
}

/// A drained accumulator's summary — the `model_stats` line's payload. Serialized
/// directly onto the wire, so this type is the line's single schema source.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct ScoreSummary {
    /// Absolute end index of the first chunk this summary covers.
    pub first_chunk_end: u64,
    /// Absolute end index of the last chunk this summary covers.
    pub last_chunk_end: u64,
    pub chunks: u32,
    pub min: f32,
    pub max: f32,
    pub mean: f32,
    pub median: f32,
}

impl ScoreStats {
    /// Accumulate one chunk's score, ending at absolute index `chunk_end`.
    pub fn record(&mut self, score: f32, chunk_end: u64) {
        if self.scores.is_empty() {
            self.first_chunk_end = chunk_end;
        }
        self.last_chunk_end = chunk_end;
        self.scores.push(score);
    }

    /// Chunks accumulated since the last flush.
    pub fn len(&self) -> usize {
        self.scores.len()
    }

    /// Whether nothing has been accumulated since the last flush. An empty
    /// accumulator flushes to nothing — a pod feeding one model but not the other
    /// (a synthetic-probability test drives Silero with no OWW pushes) emits only
    /// the model it actually ran.
    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }

    /// Drain the accumulator into a summary, or `None` when empty. Clears either
    /// way, so a flush point never re-reports chunks a previous one covered.
    pub fn flush(&mut self) -> Option<ScoreSummary> {
        if self.scores.is_empty() {
            return None;
        }
        let mut sorted = std::mem::take(&mut self.scores);
        // `f32` is not `Ord` (NaN); the models' scores are checked finite at the
        // inference boundary, so `total_cmp` orders them exactly and cannot panic
        // the way a `partial_cmp().unwrap()` would on a score that slipped through.
        sorted.sort_by(f32::total_cmp);
        let chunks = sorted.len();
        let sum: f32 = sorted.iter().sum();
        let median = if chunks.is_multiple_of(2) {
            (sorted[chunks / 2 - 1] + sorted[chunks / 2]) / 2.0
        } else {
            sorted[chunks / 2]
        };
        Some(ScoreSummary {
            first_chunk_end: self.first_chunk_end,
            last_chunk_end: self.last_chunk_end,
            chunks: chunks as u32,
            min: sorted[0],
            max: sorted[chunks - 1],
            mean: sum / chunks as f32,
            median,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record_all(stats: &mut ScoreStats, scores: &[f32]) {
        for (i, &s) in scores.iter().enumerate() {
            stats.record(s, (i as u64 + 1) * 512);
        }
    }

    /// An odd count takes the middle element; min/max/mean come off the same
    /// drain. Recorded out of order, so a summary that read `scores` unsorted
    /// would show it.
    #[test]
    fn odd_count_summarizes_the_middle_element() {
        let mut stats = ScoreStats::default();
        record_all(&mut stats, &[0.5, 0.1, 0.9, 0.3, 0.7]);
        let s = stats.flush().expect("non-empty accumulator flushes");
        assert_eq!(s.chunks, 5);
        assert_eq!(s.min, 0.1);
        assert_eq!(s.max, 0.9);
        assert_eq!(s.median, 0.5);
        assert!((s.mean - 0.5).abs() < 1e-6, "mean {}", s.mean);
        assert_eq!(
            s.first_chunk_end, 512,
            "span starts at the first chunk's end"
        );
        assert_eq!(s.last_chunk_end, 5 * 512, "and ends at the last one's");
    }

    /// An even count averages the two middles rather than picking a side.
    #[test]
    fn even_count_averages_the_two_middles() {
        let mut stats = ScoreStats::default();
        record_all(&mut stats, &[0.2, 0.8, 0.4, 0.6]);
        let s = stats.flush().unwrap();
        assert_eq!(s.chunks, 4);
        assert!((s.median - 0.5).abs() < 1e-6, "median {}", s.median);
    }

    /// A single chunk is its own min, max, mean, and median — the degenerate case
    /// the middle-index arithmetic must not slip on.
    #[test]
    fn single_element_is_its_own_summary() {
        let mut stats = ScoreStats::default();
        stats.record(0.42, 1_024);
        let s = stats.flush().unwrap();
        assert_eq!(
            (s.chunks, s.min, s.max, s.mean, s.median),
            (1, 0.42, 0.42, 0.42, 0.42)
        );
        assert_eq!((s.first_chunk_end, s.last_chunk_end), (1_024, 1_024));
    }

    /// Flushing clears: a second flush reports nothing, so no flush point can
    /// double-count chunks another already summarized.
    #[test]
    fn flush_clears_the_accumulator() {
        let mut stats = ScoreStats::default();
        record_all(&mut stats, &[0.1, 0.2]);
        assert!(stats.flush().is_some());
        assert!(stats.is_empty(), "drained");
        assert!(
            stats.flush().is_none(),
            "a drained accumulator emits nothing"
        );
    }

    /// An accumulator that never recorded emits nothing — the property that keeps
    /// a synthetic-`P` run (Silero only, no OWW pushes) from emitting an empty
    /// OWW line at every flush point.
    #[test]
    fn empty_accumulator_emits_nothing() {
        assert!(ScoreStats::default().flush().is_none());
    }

    /// The cap is a count the runtime can compare against exactly: `len` tracks
    /// every recorded chunk, so the runtime's `>= MODEL_STATS_FLUSH_CHUNKS` check
    /// fires on the 256th and bounds `scores` there.
    #[test]
    fn len_tracks_records_up_to_the_flush_cap() {
        let mut stats = ScoreStats::default();
        record_all(&mut stats, &vec![0.5; MODEL_STATS_FLUSH_CHUNKS]);
        assert_eq!(stats.len(), MODEL_STATS_FLUSH_CHUNKS);
        let s = stats.flush().unwrap();
        assert_eq!(s.chunks as usize, MODEL_STATS_FLUSH_CHUNKS);
        assert_eq!(stats.len(), 0, "the cap flush bounds the buffer");
    }
}
