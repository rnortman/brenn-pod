//! The device's room-level meter: 100 ms mean-square blocks of the level channel,
//! with a rolling history the status frame re-sends from.
//!
//! The history lives in the capture ring ([`crate::ring::CaptureRing::levels`]) and is
//! fed one sample at a time by [`crate::ring::CaptureRing::commit`], so every block is
//! index-exact with the audio and is read under the lock the streamer already takes.
//!
//! Pure integer and `f64` arithmetic: no `std`, no allocation.

use heapless::Vec as HVec;

use crate::wire::{
    LEVEL_BLOCK_SAMPLES, LEVEL_FLAG_PLAYING, LEVEL_HISTORY_BLOCKS, LevelBlock, LevelRun,
};

/// `half_db` of a block at or below −100 dBFS, including digital silence.
pub const HALF_DB_FLOOR: u8 = 200;

/// Full-scale power: a full-scale square wave's mean square, `32768²`.
const FULL_SCALE_SQ: f64 = 1_073_741_824.0;

/// `HALF_DB_BOUNDS[k]` is the mean-square ratio (to [`FULL_SCALE_SQ`]) at which `half_db`
/// rounds from `k` up to `k + 1`: `10^(−(k + 0.5) / 20)`. Decreasing in `k`.
///
/// Built by repeated multiplication from two exact-to-the-last-digit constants; 200
/// steps accumulate a relative error near 1e-14, far below a rounding boundary.
const HALF_DB_BOUNDS: [f64; HALF_DB_FLOOR as usize] = {
    // 10^(−0.5/20) and 10^(−1/20).
    const FIRST: f64 = 0.944_060_876_285_923_4;
    const STEP: f64 = 0.891_250_938_133_745_6;
    let mut table = [0.0f64; HALF_DB_FLOOR as usize];
    let mut value = FIRST;
    let mut k = 0;
    while k < table.len() {
        table[k] = value;
        value *= STEP;
        k += 1;
    }
    table
};

/// The `half_db` of a block whose samples' squares sum to `sum_sq` over `count`
/// samples: `round(−2 · 10·log10(mean_square / 32768²))`, clamped to `0..=200`.
///
/// A block of no samples reads as the floor.
pub fn half_db_of(sum_sq: u64, count: u32) -> u8 {
    if count == 0 {
        return HALF_DB_FLOOR;
    }
    let ratio = sum_sq as f64 / (count as f64 * FULL_SCALE_SQ);
    // The number of boundaries the ratio is at or below is the rounded half-dB step.
    HALF_DB_BOUNDS.partition_point(|&bound| ratio <= bound) as u8
}

/// The block that sample index `sample` belongs to.
pub const fn block_of(sample: u64) -> u64 {
    sample / LEVEL_BLOCK_SAMPLES as u64
}

/// Finished level blocks for the last [`LEVEL_HISTORY_BLOCKS`] blocks, and the block in
/// progress.
///
/// Held blocks are always consecutive: `[oldest_block(), next_block())`. A sample whose
/// block is neither the one in progress nor its successor — the write head moved
/// without being metered, as a test producer writing the ring directly does — clears
/// the history and starts again at that sample's block, so no held run ever spans a
/// stretch nobody metered.
pub struct LevelHistory {
    /// Block index of the block in progress.
    open_block: u64,
    /// The index a contiguous next sample has. Only a sample at any other index
    /// pays for the division that locates it in its block.
    expected_index: u64,
    /// Samples from the expected index to the end of the block in progress, the
    /// expected one included.
    left_in_block: u32,
    /// Sum of squared level samples in the block in progress.
    sum_sq: u64,
    /// Samples metered into the block in progress.
    count: u32,
    /// OR of the `playing` flag over the block in progress.
    playing: bool,
    /// Finished blocks, block `n` at slot `n % LEVEL_HISTORY_BLOCKS`.
    blocks: [LevelBlock; LEVEL_HISTORY_BLOCKS],
    /// Finished blocks held, at most `LEVEL_HISTORY_BLOCKS`.
    held: usize,
    /// One past the newest finished block.
    next_block: u64,
}

impl Default for LevelHistory {
    fn default() -> Self {
        Self::new()
    }
}

impl LevelHistory {
    /// An empty history whose first block is block 0.
    pub const fn new() -> Self {
        Self {
            open_block: 0,
            expected_index: 0,
            left_in_block: LEVEL_BLOCK_SAMPLES,
            sum_sq: 0,
            count: 0,
            playing: false,
            blocks: [LevelBlock {
                half_db: HALF_DB_FLOOR,
                flags: 0,
            }; LEVEL_HISTORY_BLOCKS],
            held: 0,
            next_block: 0,
        }
    }

    /// Meter the level sample at absolute index `index`. `playing` is whether the
    /// speaker output is active as it is captured. The block closes on its last sample.
    ///
    /// Called once per captured sample, so a contiguous sample costs no division.
    pub fn push(&mut self, index: u64, level: i16, playing: bool) {
        if index != self.expected_index {
            let block = block_of(index);
            if block != self.open_block {
                self.restart_at(block);
            }
            self.left_in_block = LEVEL_BLOCK_SAMPLES - (index % LEVEL_BLOCK_SAMPLES as u64) as u32;
        }
        self.expected_index = index.wrapping_add(1);
        let v = level as i64;
        self.sum_sq += (v * v) as u64;
        self.count += 1;
        self.playing |= playing;
        self.left_in_block -= 1;
        if self.left_in_block == 0 {
            self.close_open_block();
            self.left_in_block = LEVEL_BLOCK_SAMPLES;
        }
    }

    /// The oldest finished block held (equal to [`next_block`](Self::next_block) when
    /// none is).
    pub fn oldest_block(&self) -> u64 {
        self.next_block - self.held as u64
    }

    /// One past the newest finished block.
    pub fn next_block(&self) -> u64 {
        self.next_block
    }

    /// Every finished block at or after `block`, clamped to the oldest held. Empty when
    /// `block` is at or past [`next_block`](Self::next_block); its `first_block` is then
    /// `block`.
    pub fn run_from(&self, block: u64) -> LevelRun {
        let start = block.max(self.oldest_block());
        let mut blocks = HVec::new();
        for n in start..self.next_block {
            // At most `held ≤ LEVEL_HISTORY_BLOCKS` iterations, the run's capacity.
            let _ = blocks.push(self.blocks[(n % LEVEL_HISTORY_BLOCKS as u64) as usize]);
        }
        LevelRun {
            first_block: start,
            blocks,
        }
    }

    /// Finish the block in progress and open its successor.
    fn close_open_block(&mut self) {
        let slot = (self.open_block % LEVEL_HISTORY_BLOCKS as u64) as usize;
        self.blocks[slot] = LevelBlock {
            half_db: half_db_of(self.sum_sq, self.count),
            flags: if self.playing { LEVEL_FLAG_PLAYING } else { 0 },
        };
        self.next_block = self.open_block + 1;
        self.held = (self.held + 1).min(LEVEL_HISTORY_BLOCKS);
        self.open_block += 1;
        self.reset_accum();
    }

    /// A sample arrived for a block other than the one in progress: the head moved
    /// without being metered. Drop the held history and the partial block.
    fn restart_at(&mut self, block: u64) {
        self.open_block = block;
        self.next_block = block;
        self.held = 0;
        self.reset_accum();
    }

    fn reset_accum(&mut self) {
        self.sum_sq = 0;
        self.count = 0;
        self.playing = false;
    }
}

/// What [`check_run_against`] found: how many blocks it recomputed, the largest
/// disagreement with the meter, and the meter's quietest and loudest block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeterCheck {
    /// Blocks recomputed.
    pub blocks: u32,
    /// Largest `|meter − recomputed|` over those blocks, in half-dB steps.
    pub max_diff_steps: u32,
    /// The meter's quietest block among them, as `half_db` ([`HALF_DB_FLOOR`] when none).
    pub quietest_half_db: u8,
    /// The meter's loudest block among them, as `half_db` ([`HALF_DB_FLOOR`] when none).
    pub loudest_half_db: u8,
}

/// Recompute every block of `run` whose first sample is at or after `held_from` from
/// `level_at` — the level sample at an absolute index — and compare it with the
/// meter's `half_db`. A block starting before `held_from` is skipped: its samples are
/// no longer where `level_at` reads them.
pub fn check_run_against(
    run: &LevelRun,
    held_from: u64,
    level_at: impl Fn(u64) -> i16,
) -> MeterCheck {
    let mut check = MeterCheck {
        blocks: 0,
        max_diff_steps: 0,
        quietest_half_db: 0,
        loudest_half_db: HALF_DB_FLOOR,
    };
    for (n, block) in run.blocks.iter().enumerate() {
        let first = (run.first_block + n as u64) * LEVEL_BLOCK_SAMPLES as u64;
        if first < held_from {
            continue;
        }
        let sum_sq: u64 = (first..first + LEVEL_BLOCK_SAMPLES as u64)
            .map(|i| {
                let v = level_at(i) as i64;
                (v * v) as u64
            })
            .sum();
        let recomputed = half_db_of(sum_sq, LEVEL_BLOCK_SAMPLES);
        check.blocks += 1;
        check.max_diff_steps = check
            .max_diff_steps
            .max(u32::from(block.half_db.abs_diff(recomputed)));
        check.quietest_half_db = check.quietest_half_db.max(block.half_db);
        check.loudest_half_db = check.loudest_half_db.min(block.half_db);
    }
    if check.blocks == 0 {
        check.quietest_half_db = HALF_DB_FLOOR;
    }
    check
}

/// Of the blocks of `run` that lie wholly inside the samples `[lo, hi)`, how many
/// there are and how many carry the playing flag.
pub fn flags_within(run: &LevelRun, lo: u64, hi: u64) -> (u32, u32) {
    let mut blocks = 0;
    let mut flagged = 0;
    for (n, block) in run.blocks.iter().enumerate() {
        let first = (run.first_block + n as u64) * LEVEL_BLOCK_SAMPLES as u64;
        let end = first + LEVEL_BLOCK_SAMPLES as u64;
        if first >= lo && end <= hi {
            blocks += 1;
            flagged += u32::from(block.playing());
        }
    }
    (blocks, flagged)
}

#[cfg(test)]
mod tests {
    use super::*;

    const N: u64 = LEVEL_BLOCK_SAMPLES as u64;

    /// Push `samples` samples of a square wave of amplitude `amp`, starting at
    /// sample `from`, `playing` on every sample.
    fn push_square(h: &mut LevelHistory, from: u64, samples: u64, amp: i16, playing: bool) {
        for i in from..from + samples {
            let s = if i % 2 == 0 {
                amp
            } else {
                amp.saturating_neg()
            };
            h.push(i, s, playing);
        }
    }

    /// A square wave of amplitude `amp` at sample `i`, as `push_square` writes it.
    fn square_at(i: u64, amp: i16) -> i16 {
        if i.is_multiple_of(2) {
            amp
        } else {
            amp.saturating_neg()
        }
    }

    #[test]
    fn a_run_checked_against_its_own_samples_agrees_exactly() {
        let mut h = LevelHistory::new();
        // Blocks 0..4 at four levels, read back from a closure that knows them.
        let amp_of = |i: u64| [1_036i16, 4, 3_277, 1][(i / N) as usize];
        for i in 0..4 * N {
            h.push(i, square_at(i, amp_of(i)), false);
        }
        let check = check_run_against(&h.run_from(0), 0, |i| square_at(i, amp_of(i)));
        assert_eq!(
            check,
            MeterCheck {
                blocks: 4,
                max_diff_steps: 0,
                quietest_half_db: 181,
                loudest_half_db: 40,
            }
        );
    }

    #[test]
    fn a_disagreement_is_reported_in_steps() {
        let mut h = LevelHistory::new();
        push_square(&mut h, 0, 2 * N, 1_036, false);
        // The samples now read 6 dB quieter in block 1: 12 half-dB steps.
        let check = check_run_against(&h.run_from(0), 0, |i| {
            square_at(i, if i < N { 1_036 } else { 518 })
        });
        assert_eq!((check.blocks, check.max_diff_steps), (2, 12));
    }

    #[test]
    fn blocks_whose_samples_the_ring_no_longer_holds_are_skipped() {
        let mut h = LevelHistory::new();
        push_square(&mut h, 0, 3 * N, 1_036, false);
        // Held from mid-block 1: only block 2 starts at or after it.
        let check = check_run_against(&h.run_from(0), N + 1, |i| square_at(i, 1_036));
        assert_eq!((check.blocks, check.max_diff_steps), (1, 0));
        let none = check_run_against(&h.run_from(0), 3 * N, |_| 0);
        assert_eq!((none.blocks, none.quietest_half_db), (0, HALF_DB_FLOOR));
    }

    #[test]
    fn flags_are_counted_over_blocks_wholly_inside_the_window() {
        let mut h = LevelHistory::new();
        push_square(&mut h, 0, 2 * N, 100, false);
        push_square(&mut h, 2 * N, 3 * N, 100, true);
        push_square(&mut h, 5 * N, 2 * N, 100, false);
        let run = h.run_from(0);
        assert_eq!(flags_within(&run, 0, 2 * N), (2, 0));
        assert_eq!(flags_within(&run, 2 * N, 5 * N), (3, 3));
        // A window edge inside a block leaves that block out.
        assert_eq!(flags_within(&run, 2 * N + 1, 5 * N - 1), (1, 1));
        assert_eq!(flags_within(&run, N, 6 * N), (5, 3));
        assert_eq!(flags_within(&run, 7 * N, 9 * N), (0, 0));
    }

    #[test]
    fn known_square_waves_give_exact_half_db() {
        // 20·log10(A / 32768) for each amplitude, doubled and negated, rounded.
        let cases: [(i16, u8); 6] = [
            (i16::MAX, 0), // −0.0003 dBFS
            (16_384, 12),  // −6.02 dBFS
            (3_277, 40),   // −19.9995 dBFS
            (1_036, 60),   // −30.0018 dBFS
            (4, 157),      // −78.27 dBFS
            (1, 181),      // −90.31 dBFS
        ];
        for (amp, want) in cases {
            let mut h = LevelHistory::new();
            push_square(&mut h, 0, N, amp, false);
            let run = h.run_from(0);
            assert_eq!(run.blocks.len(), 1);
            assert_eq!(run.blocks[0].half_db, want, "amplitude {amp}");
        }
    }

    #[test]
    fn full_scale_is_zero_and_silence_is_the_floor() {
        let mut h = LevelHistory::new();
        push_square(&mut h, 0, N, i16::MIN, false);
        push_square(&mut h, N, N, 0, false);
        let run = h.run_from(0);
        assert_eq!(run.blocks[0].half_db, 0);
        assert_eq!(run.blocks[1].half_db, HALF_DB_FLOOR);
    }

    #[test]
    fn below_minus_100_dbfs_clamps_to_the_floor() {
        // One sample of 1 LSB in a block: mean square 1/1600, about −122 dBFS.
        assert_eq!(half_db_of(1, LEVEL_BLOCK_SAMPLES), HALF_DB_FLOOR);
        assert_eq!(half_db_of(0, 0), HALF_DB_FLOOR);
    }

    #[test]
    fn rounding_boundaries_sit_at_quarter_db() {
        // Just louder than −0.25 dBFS rounds to 0; just quieter rounds to 1 (−0.5).
        let at = |db: f64| {
            let ratio = pow10(db / 10.0);
            half_db_of((ratio * FULL_SCALE_SQ * 1_000_000.0) as u64, 1_000_000)
        };
        assert_eq!(at(-0.249), 0);
        assert_eq!(at(-0.251), 1);
        assert_eq!(at(-39.749), 79);
        assert_eq!(at(-39.751), 80);
        assert_eq!(at(-99.74), 199);
        assert_eq!(at(-99.76), 200);
    }

    /// `10^x` for the test's dB arithmetic, through `std`'s float math (tests link std).
    fn pow10(x: f64) -> f64 {
        extern crate std;
        std::primitive::f64::powf(10.0, x)
    }

    #[test]
    fn a_block_is_flagged_playing_when_any_one_sample_was() {
        let mut h = LevelHistory::new();
        for i in 0..N {
            h.push(i, 100, i == 777);
        }
        push_square(&mut h, N, N, 100, false);
        let run = h.run_from(0);
        assert!(run.blocks[0].playing());
        assert!(!run.blocks[1].playing(), "the flag does not leak forward");
    }

    #[test]
    fn blocks_close_on_multiples_of_the_block_size() {
        let mut h = LevelHistory::new();
        push_square(&mut h, 0, N - 1, 100, false);
        assert_eq!(h.next_block(), 0, "one sample short of a block");
        h.push(N - 1, 100, false);
        assert_eq!(h.next_block(), 1);
        push_square(&mut h, N, 2 * N + 5, 100, false);
        assert_eq!(h.next_block(), 3);
        assert_eq!(h.run_from(0).blocks.len(), 3);
    }

    #[test]
    fn a_mid_block_start_meters_the_partial_block_and_aligns_after() {
        let mut h = LevelHistory::new();
        // The first sample arrives mid-block 2: block 2 closes on sample 3N - 1 with
        // the samples it got, and block 3 is whole.
        push_square(&mut h, 2 * N + 400, 2 * N - 400, 1_036, false);
        let run = h.run_from(0);
        assert_eq!(run.first_block, 2);
        assert_eq!(run.blocks.len(), 2);
        assert_eq!(run.blocks[0].half_db, 60);
        assert_eq!(run.blocks[1].half_db, 60);
    }

    #[test]
    fn an_unmetered_jump_restarts_the_history_at_the_new_block() {
        let mut h = LevelHistory::new();
        push_square(&mut h, 0, 3 * N, 100, false);
        assert_eq!(h.oldest_block(), 0);
        // The head moved to block 10 without being metered.
        push_square(&mut h, 10 * N, N, 100, false);
        assert_eq!(h.oldest_block(), 10);
        assert_eq!(h.next_block(), 11);
        let run = h.run_from(0);
        assert_eq!((run.first_block, run.blocks.len()), (10, 1));
    }

    #[test]
    fn an_unmetered_jump_into_mid_block_meters_the_partial_block_and_aligns_after() {
        let mut h = LevelHistory::new();
        push_square(&mut h, 0, 3 * N, 100, false);
        // The head moved to sample 700 of block 10 without being metered.
        push_square(&mut h, 10 * N + 700, N - 701, 1_036, false);
        assert_eq!(h.next_block(), 10, "one sample short of block 10's end");
        h.push(11 * N - 1, 1_036, false);
        push_square(&mut h, 11 * N, N, 1_036, false);
        let run = h.run_from(0);
        assert_eq!((run.first_block, run.blocks.len()), (10, 2));
        assert_eq!(run.blocks[0].half_db, 60);
        assert_eq!(run.blocks[1].half_db, 60);
    }

    #[test]
    fn a_gap_inside_the_open_block_keeps_it_and_closes_it_on_its_last_sample() {
        let mut h = LevelHistory::new();
        push_square(&mut h, 0, N, 100, false);
        // Block 1 skips samples 100..200; it is still block 1 and still closes on 2N - 1.
        push_square(&mut h, N, 100, 1_036, false);
        push_square(&mut h, N + 200, N - 201, 1_036, false);
        assert_eq!(h.next_block(), 1);
        h.push(2 * N - 1, 1_036, false);
        assert_eq!(h.next_block(), 2);
        let run = h.run_from(0);
        assert_eq!((run.first_block, run.blocks.len()), (0, 2));
        assert_eq!(run.blocks[1].half_db, 60);
    }

    #[test]
    fn history_holds_the_last_300_blocks() {
        let mut h = LevelHistory::new();
        let total = LEVEL_HISTORY_BLOCKS as u64 + 50;
        for b in 0..total {
            // A distinct level per block so slots are told apart.
            let amp = 1 + (b % 200) as i16 * 100;
            push_square(&mut h, b * N, N, amp, false);
        }
        assert_eq!(h.oldest_block(), 50);
        assert_eq!(h.next_block(), total);
        let run = h.run_from(0);
        assert_eq!(run.first_block, 50, "clamped to the oldest held");
        assert_eq!(run.blocks.len(), LEVEL_HISTORY_BLOCKS);
        let mut reference = LevelHistory::new();
        push_square(&mut reference, 0, N, 1 + 50 * 100, false);
        assert_eq!(run.blocks[0], reference.run_from(0).blocks[0]);
    }

    #[test]
    fn run_from_returns_the_tail_and_nothing_past_the_newest() {
        let mut h = LevelHistory::new();
        push_square(&mut h, 0, 20 * N, 100, false);
        let tail = h.run_from(15);
        assert_eq!((tail.first_block, tail.blocks.len()), (15, 5));
        let at_end = h.run_from(20);
        assert_eq!((at_end.first_block, at_end.blocks.len()), (20, 0));
        let past = h.run_from(25);
        assert_eq!((past.first_block, past.blocks.len()), (25, 0));
    }

    #[test]
    fn empty_history_returns_an_empty_run() {
        let h = LevelHistory::new();
        let run = h.run_from(0);
        assert_eq!((run.first_block, run.blocks.len()), (0, 0));
    }
}
