//! [`ScoreStats`]: the per-model score accumulator behind the `model_stats`
//! observability line.
//!
//! The endpointer's transition events are silent exactly when the FSM never
//! transitions — a room where Silero returns 0.003 forever produces no line at
//! all, which is precisely the failure that needs diagnosing. This accumulator
//! closes that hole: it collects each model's per-chunk score and periodically
//! summarizes the distribution, so "what was the model actually returning" is
//! answerable from the log rather than from a rebuild. A chunk the model
//! consumed without returning a score counts too ([`ScoreStats::record_unscored`]):
//! the wake model is deaf for its first [`WAKE_READINESS_SAMPLES`] after a
//! reset, and a line saying so is the difference between "wake ran and scored
//! low" and "wake never ran" during an incident.
//!
//! [`WAKE_READINESS_SAMPLES`]: crate::listener::oww_stream::WAKE_READINESS_SAMPLES
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

/// One model's scores since the last flush, plus the sample span they cover and
/// the chunks it consumed without producing one.
#[derive(Debug, Default)]
pub struct ScoreStats {
    /// Absolute end index of the first accumulated chunk. Meaningless while
    /// `scores` is empty; set by the first [`record`](ScoreStats::record).
    first_chunk_end: u64,
    /// Absolute end index of the most recent accumulated chunk.
    last_chunk_end: u64,
    scores: Vec<f32>,
    /// Chunks the model consumed since the last flush that yielded no score.
    unscored: u32,
}

/// The distribution of the scores one flush covers. One value or none: the four
/// numbers are computed together from the same drain, so they are present
/// together or not at all. Flattened onto the `model_stats` line, so the wire
/// carries `min`/`max`/`mean`/`median` as plain fields and omits all four when
/// there is no distribution.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct ScoreDistribution {
    pub min: f32,
    pub max: f32,
    pub mean: f32,
    pub median: f32,
}

/// A drained accumulator's summary — the `model_stats` line's payload. Serialized
/// directly onto the wire, so this type is the line's single schema source.
///
/// A reader of one line is entitled to conclude that the named model consumed
/// `chunks + unscored_chunks` chunks of audio over the span, that `chunks` of
/// them produced the distribution reported, and that `unscored_chunks` of them
/// produced nothing — the wake model warming up after a reset is the case that
/// makes the second number non-zero. The distribution is present exactly when
/// `chunks` is non-zero; a line with `chunks: 0` and a non-zero
/// `unscored_chunks` says the model ran and could not yet score, which is a
/// different fact from the model never having run (no line at all).
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct ScoreSummary {
    /// Absolute end index of the first chunk this summary covers.
    pub first_chunk_end: u64,
    /// Absolute end index of the last chunk this summary covers.
    pub last_chunk_end: u64,
    /// Chunks that produced a score — the count the distribution is over.
    pub chunks: u32,
    /// Chunks consumed that produced no score.
    pub unscored_chunks: u32,
    /// The scored chunks' distribution, absent when none of them scored.
    #[serde(flatten)]
    pub distribution: Option<ScoreDistribution>,
}

impl ScoreStats {
    /// Accumulate one chunk's score, ending at absolute index `chunk_end`.
    pub fn record(&mut self, score: f32, chunk_end: u64) {
        self.note_span(chunk_end);
        self.scores.push(score);
    }

    /// Accumulate a run of `n` chunks the model consumed without scoring, the
    /// first ending at absolute index `first_chunk_end` and the last at
    /// `last_chunk_end`. Both ends, because the summary's span has to cover the
    /// whole run: a run collapsed to one cursor reports a line whose chunk count
    /// does not fit inside the span it names. The wake model's readiness window
    /// is the reason this exists at all — without it, a warming stream and a
    /// stream that is never fed are the same absence in the log.
    pub fn record_unscored(&mut self, n: u32, first_chunk_end: u64, last_chunk_end: u64) {
        if n == 0 {
            return;
        }
        self.note_span(first_chunk_end);
        // Counted between the two, so the second `note_span` sees a non-empty
        // accumulator and extends the span rather than re-opening it.
        self.unscored += n;
        self.note_span(last_chunk_end);
    }

    /// Extend the covered span to include a chunk ending at `chunk_end`.
    fn note_span(&mut self, chunk_end: u64) {
        if self.is_empty() {
            self.first_chunk_end = chunk_end;
        }
        self.last_chunk_end = chunk_end;
    }

    /// Chunks accumulated since the last flush, scored and unscored alike — the
    /// count the runtime compares against the flush cap.
    pub fn len(&self) -> usize {
        self.scores.len() + self.unscored as usize
    }

    /// Whether nothing has been accumulated since the last flush. An empty
    /// accumulator flushes to nothing — a pod feeding one model but not the other
    /// (a synthetic-probability test drives Silero with no OWW pushes) emits only
    /// the model it actually ran.
    pub fn is_empty(&self) -> bool {
        self.scores.is_empty() && self.unscored == 0
    }

    /// Drain the accumulator into a summary, or `None` when empty. Clears either
    /// way, so a flush point never re-reports chunks a previous one covered.
    pub fn flush(&mut self) -> Option<ScoreSummary> {
        if self.is_empty() {
            return None;
        }
        let unscored_chunks = std::mem::take(&mut self.unscored);
        let mut sorted = std::mem::take(&mut self.scores);
        // `f32` is not `Ord` (NaN); the models' scores are checked finite at the
        // inference boundary, so `total_cmp` orders them exactly and cannot panic
        // the way a `partial_cmp().unwrap()` would on a score that slipped through.
        sorted.sort_by(f32::total_cmp);
        let chunks = sorted.len();
        let distribution = (chunks > 0).then(|| {
            let sum: f32 = sorted.iter().sum();
            let median = if chunks.is_multiple_of(2) {
                (sorted[chunks / 2 - 1] + sorted[chunks / 2]) / 2.0
            } else {
                sorted[chunks / 2]
            };
            ScoreDistribution {
                min: sorted[0],
                max: sorted[chunks - 1],
                mean: sum / chunks as f32,
                median,
            }
        });
        Some(ScoreSummary {
            first_chunk_end: self.first_chunk_end,
            last_chunk_end: self.last_chunk_end,
            chunks: chunks as u32,
            unscored_chunks,
            distribution,
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
        assert_eq!(s.unscored_chunks, 0);
        let d = s.distribution.expect("a scored summary has a distribution");
        assert_eq!((d.min, d.max, d.median), (0.1, 0.9, 0.5));
        let mean = d.mean;
        assert!((mean - 0.5).abs() < 1e-6, "mean {mean}");
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
        let median = s
            .distribution
            .expect("a scored summary has a distribution")
            .median;
        assert!((median - 0.5).abs() < 1e-6, "median {median}");
    }

    /// A single chunk is its own min, max, mean, and median — the degenerate case
    /// the middle-index arithmetic must not slip on.
    ///
    /// Asserted on the wire as well as on the struct: the distribution is
    /// flattened, so a lost or mistyped `flatten` would nest the four numbers
    /// under a `distribution` object and every console line would render
    /// `min ? max ? mean ? median ?` while this file's field reads stayed green.
    /// That line is the one an operator has to answer "what was the model
    /// returning" from.
    #[test]
    fn single_element_is_its_own_summary() {
        let mut stats = ScoreStats::default();
        stats.record(0.42, 1_024);
        let s = stats.flush().unwrap();
        assert_eq!(
            (s.chunks, s.distribution),
            (
                1,
                Some(ScoreDistribution {
                    min: 0.42,
                    max: 0.42,
                    mean: 0.42,
                    median: 0.42
                })
            )
        );
        assert_eq!((s.first_chunk_end, s.last_chunk_end), (1_024, 1_024));

        let json = serde_json::to_value(s).expect("summary serializes");
        for field in ["min", "max", "mean", "median"] {
            let v = json[field]
                .as_f64()
                .unwrap_or_else(|| panic!("{field} is a top-level number on the line: {json}"));
            assert!(
                (v - 0.42).abs() < 1e-6,
                "{field} carries the score, not a placeholder: {json}"
            );
        }
        assert_eq!(json["chunks"], 1);
        assert_eq!(json["unscored_chunks"], 0);
        assert_eq!(json["first_chunk_end"], 1_024);
        assert_eq!(json["last_chunk_end"], 1_024);
        assert!(
            json.get("distribution").is_none(),
            "the distribution is flattened onto the line, not an object on it: {json}"
        );
    }

    /// A flush carrying only unscored chunks still reports: it names the span
    /// and the count, and omits a distribution it does not have. This is the
    /// warming wake stream's line — the one whose absence used to make a
    /// warming stream and a dead one look identical.
    #[test]
    fn unscored_only_flush_reports_the_count_and_no_distribution() {
        let mut stats = ScoreStats::default();
        stats.record_unscored(1, 1_280, 1_280);
        stats.record_unscored(1, 2_560, 2_560);
        assert_eq!(stats.len(), 2, "unscored chunks count toward the cap");
        let s = stats.flush().expect("unscored chunks are still a reading");
        assert_eq!((s.chunks, s.unscored_chunks), (0, 2));
        assert_eq!((s.first_chunk_end, s.last_chunk_end), (1_280, 2_560));
        assert_eq!(s.distribution, None);
        let json = serde_json::to_value(s).expect("summary serializes");
        assert!(
            json.get("min").is_none(),
            "an absent distribution is absent from the line, not zeroed: {json}"
        );
        assert_eq!(json["unscored_chunks"], 2);
    }

    /// A flush spanning the readiness boundary carries both halves: the chunks
    /// that could not be scored and the distribution of the ones that could.
    /// The span covers the whole unscored run, from its first chunk's end —
    /// a summary that named only the run's last chunk would claim 16 chunks of
    /// audio inside a single chunk's worth of samples.
    #[test]
    fn a_flush_spanning_readiness_carries_both_counts() {
        let mut stats = ScoreStats::default();
        stats.record_unscored(15, 1_280, 15 * 1_280);
        stats.record(0.01, 16 * 1_280);
        let s = stats.flush().unwrap();
        assert_eq!((s.chunks, s.unscored_chunks), (1, 15));
        assert_eq!(s.distribution.map(|d| d.max), Some(0.01));
        assert_eq!((s.first_chunk_end, s.last_chunk_end), (1_280, 16 * 1_280));
    }

    /// `record_unscored(0, ..)` is a no-op: a fully-scored push must not widen
    /// the span or make an empty accumulator look non-empty.
    #[test]
    fn zero_unscored_chunks_record_nothing() {
        let mut stats = ScoreStats::default();
        stats.record_unscored(0, 9_999, 9_999);
        assert!(stats.is_empty());
        assert!(stats.flush().is_none());
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
