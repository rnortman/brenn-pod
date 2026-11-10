//! The room's level before an utterance, read from the pod's 100 ms level blocks.
//!
//! The pod meters its level channel in blocks of [`LEVEL_BLOCK_SAMPLES`] whether
//! or not a segment is open, and sends them in its status frames; this keeps the
//! last minute of them per connection and reduces them to one [`AmbientReading`]
//! for each utterance it is asked about. Only the pod's own voice and speech
//! addressed to the pod are excluded: other people talking, a television, typing
//! are the room a reply has to be heard over.

use std::collections::VecDeque;

use audio_pipeline::wire::LEVEL_BLOCK_SAMPLES;
use pod_ingest::{HostMicros, LevelBlock};

use crate::level::DEAD_CHANNEL_DBFS;
use crate::playback::audio_ms;
use crate::types::AmbientReading;

/// Level blocks kept per connection: 60 s.
pub(crate) const ROOM_HISTORY_BLOCKS: usize = 600;

/// Blocks ending later than this many samples before the speech are not room:
/// 300 ms, so the speech's own onset, which the endpointer dates late, stays out.
pub(crate) const AMBIENT_GUARD_SAMPLES: u64 = 4_800;

/// Blocks after a playing block that are not room either: the pod's own output
/// still in the microphone, 400 ms (the listener's mute tail).
pub(crate) const AMBIENT_PLAYBACK_TAIL_BLOCKS: u64 = 4;

/// The reading is the median of the newest this-many usable blocks: 10 s of room.
pub(crate) const AMBIENT_WINDOW_BLOCKS: usize = 100;

/// Spans of minted utterances kept for exclusion.
const ADDRESSED_SPANS: usize = 8;

const BLOCK: u64 = LEVEL_BLOCK_SAMPLES as u64;

/// The last reading taken from blocks, and when.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Carried {
    reading: AmbientReading,
    at: HostMicros,
}

/// One pod's room history: its level blocks and the spans that were speech
/// addressed to it (both per connection), and the last good reading (which
/// outlives a connection, since it holds no sample index).
#[derive(Debug, Default)]
pub(crate) struct RoomHistory {
    /// `(block index, block)`, ascending by index, at most
    /// [`ROOM_HISTORY_BLOCKS`].
    blocks: VecDeque<(u64, LevelBlock)>,
    /// `[start, end)` sample spans of the last [`ADDRESSED_SPANS`] utterances.
    addressed: VecDeque<(u64, u64)>,
    carried: Option<Carried>,
}

impl RoomHistory {
    /// Take a status frame's run: `levels[i]` is block `first_block + i`. A block
    /// index already held is replaced, so a re-sent run is harmless.
    pub(crate) fn ingest(&mut self, first_block: u64, levels: &[LevelBlock]) {
        for (index, block) in (first_block..).zip(levels.iter().copied()) {
            match self.blocks.back() {
                Some(&(newest, _)) if newest < index => self.blocks.push_back((index, block)),
                None => self.blocks.push_back((index, block)),
                Some(_) => match self.blocks.binary_search_by_key(&index, |&(i, _)| i) {
                    Ok(at) => self.blocks[at].1 = block,
                    Err(at) => self.blocks.insert(at, (index, block)),
                },
            }
        }
        while self.blocks.len() > ROOM_HISTORY_BLOCKS {
            self.blocks.pop_front();
        }
    }

    /// Record the span of an utterance addressed to the pod. A re-carve of the
    /// newest span (same start, later end) extends it rather than adding one.
    pub(crate) fn note_addressed(&mut self, start: u64, end: u64) {
        if let Some(last) = self.addressed.back_mut()
            && last.0 == start
        {
            last.1 = last.1.max(end);
            return;
        }
        if self.addressed.len() == ADDRESSED_SPANS {
            self.addressed.pop_front();
        }
        self.addressed.push_back((start, end));
    }

    /// A new connection: its sample domain may restart (a reboot), so blocks
    /// and spans go; the carried reading stays.
    pub(crate) fn new_connection(&mut self) {
        self.blocks.clear();
        self.addressed.clear();
    }

    /// The room before speech beginning at `speech_lo`, measured at host time
    /// `at`. The newest [`AMBIENT_WINDOW_BLOCKS`] usable blocks, wherever they
    /// lie in the history; their median, how many, and how old the newest is
    /// against `speech_lo`. With no usable block, the last good reading aged by
    /// the host time since it was taken; with none of those either, `None`.
    pub(crate) fn read(&mut self, speech_lo: u64, at: HostMicros) -> Option<AmbientReading> {
        let limit = speech_lo.saturating_sub(AMBIENT_GUARD_SAMPLES);
        let mut used: Vec<f32> = Vec::with_capacity(AMBIENT_WINDOW_BLOCKS);
        let mut newest_end: Option<u64> = None;
        for (pos, &(index, block)) in self.blocks.iter().enumerate().rev() {
            if used.len() == AMBIENT_WINDOW_BLOCKS {
                break;
            }
            let start = index.saturating_mul(BLOCK);
            let end = start.saturating_add(BLOCK);
            if end > limit || !self.usable(pos, index, start, end, block) {
                continue;
            }
            newest_end.get_or_insert(end);
            used.push(block.dbfs());
        }
        let Some(newest_end) = newest_end else {
            return self.carried.map(|c| {
                let grown_ms = at.checked_delta(c.at).unwrap_or(0) / 1_000;
                AmbientReading {
                    age_ms: saturate_u32(u64::from(c.reading.age_ms).saturating_add(grown_ms)),
                    carried: true,
                    ..c.reading
                }
            });
        };
        let reading = AmbientReading {
            dbfs: median(&mut used),
            blocks: u16::try_from(used.len()).unwrap_or(u16::MAX),
            age_ms: saturate_u32(audio_ms(speech_lo.saturating_sub(newest_end))),
            carried: false,
        };
        self.carried = Some(Carried { reading, at });
        Some(reading)
    }

    /// Whether the block at deque position `pos` (index `index`, samples
    /// `[start, end)`) is room: not the pod playing, not inside the playback tail
    /// after a playing block, not speech addressed to the pod, not a dead channel.
    fn usable(&self, pos: usize, index: u64, start: u64, end: u64, block: LevelBlock) -> bool {
        if block.playing() || block.dbfs() <= DEAD_CHANNEL_DBFS {
            return false;
        }
        let tail_from = index.saturating_sub(AMBIENT_PLAYBACK_TAIL_BLOCKS);
        let in_tail = self
            .blocks
            .range(..pos)
            .rev()
            .take_while(|&&(i, _)| i >= tail_from)
            .any(|&(_, b)| b.playing());
        if in_tail {
            return false;
        }
        !self.addressed.iter().any(|&(s, e)| start < e && s < end)
    }
}

/// The median of `values` (non-empty): the middle value, or the mean of the two
/// middle values of an even count. Reorders `values`.
fn median(values: &mut [f32]) -> f32 {
    values.sort_by(f32::total_cmp);
    let n = values.len();
    if n % 2 == 1 {
        values[n / 2]
    } else {
        (values[n / 2 - 1] + values[n / 2]) / 2.0
    }
}

fn saturate_u32(v: u64) -> u32 {
    u32::try_from(v).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use audio_pipeline::wire::LEVEL_FLAG_PLAYING;

    const T0: HostMicros = HostMicros(1_700_000_000_000_000);

    fn block(dbfs: f32) -> LevelBlock {
        LevelBlock {
            // The wire's encoding: −2·dBFS, rounded.
            half_db: (-dbfs * 2.0).round() as u8,
            flags: 0,
        }
    }

    fn playing(dbfs: f32) -> LevelBlock {
        LevelBlock {
            flags: LEVEL_FLAG_PLAYING,
            ..block(dbfs)
        }
    }

    /// A history of `levels` as blocks `0..`, read for speech starting right after
    /// the last block plus the guard.
    fn read_after(levels: &[LevelBlock]) -> (RoomHistory, Option<AmbientReading>) {
        let mut room = RoomHistory::default();
        room.ingest(0, levels);
        let speech_lo = levels.len() as u64 * BLOCK + AMBIENT_GUARD_SAMPLES;
        let reading = room.read(speech_lo, T0);
        (room, reading)
    }

    #[test]
    fn steady_blocks_read_their_level_on_a_full_window_and_a_small_age() {
        let (_, r) = read_after(&vec![block(-70.0); 150]);
        let r = r.expect("a reading");
        assert_eq!(r.dbfs, -70.0);
        assert_eq!(r.blocks, 100);
        assert_eq!(r.age_ms, 300, "the guard is the only gap");
        assert!(!r.carried);
    }

    /// The median's two sides: noise present less than half the time does not
    /// move it, noise present more than half the time sets it.
    #[test]
    fn the_median_follows_what_is_present_more_than_half_the_time() {
        let mixed = |loud_every: &dyn Fn(usize) -> bool| -> Vec<LevelBlock> {
            (0..100)
                .map(|i| block(if loud_every(i) { -50.0 } else { -70.0 }))
                .collect()
        };
        let (_, forty) = read_after(&mixed(&|i| i % 10 < 4));
        assert_eq!(forty.unwrap().dbfs, -70.0);
        let (_, seventy) = read_after(&mixed(&|i| i % 10 < 7));
        assert_eq!(seventy.unwrap().dbfs, -50.0);
    }

    #[test]
    fn a_burst_barely_moves_the_reading() {
        let mut levels = vec![block(-70.0); 100];
        let (_, quiet) = read_after(&levels);
        levels[50] = block(-40.0);
        let (_, burst) = read_after(&levels);
        assert!((burst.unwrap().dbfs - quiet.unwrap().dbfs).abs() < 0.5);
    }

    #[test]
    fn playing_blocks_and_the_four_after_them_are_excluded() {
        // 20 room blocks at -70, then 10 playing, then 4 tail blocks at -40 (the
        // pod's echo), then 6 room blocks at -60.
        let mut levels = vec![block(-70.0); 20];
        levels.extend(vec![playing(-30.0); 10]);
        levels.extend(vec![block(-40.0); 4]);
        levels.extend(vec![block(-60.0); 6]);
        let (_, r) = read_after(&levels);
        let r = r.unwrap();
        assert_eq!(r.blocks, 26);
        assert_eq!(r.dbfs, -70.0);
        // The fifth block after the playback is room again.
        let mut levels = vec![playing(-30.0)];
        levels.extend(vec![block(-40.0); 4]);
        levels.push(block(-65.0));
        let (_, r) = read_after(&levels);
        assert_eq!((r.unwrap().blocks, r.unwrap().dbfs), (1, -65.0));
    }

    #[test]
    fn an_addressed_span_is_excluded() {
        let mut room = RoomHistory::default();
        let mut levels = vec![block(-70.0); 30];
        levels[10..20].fill(block(-30.0));
        room.ingest(0, &levels);
        // The utterance covered blocks 10..20 exactly; a span touching a block
        // by one sample excludes it.
        room.note_addressed(10 * BLOCK, 20 * BLOCK - 1);
        let r = room.read(30 * BLOCK + AMBIENT_GUARD_SAMPLES, T0).unwrap();
        assert_eq!((r.blocks, r.dbfs), (20, -70.0));
        // A re-carve of the same utterance extends its span.
        room.note_addressed(10 * BLOCK, 25 * BLOCK);
        let r = room.read(30 * BLOCK + AMBIENT_GUARD_SAMPLES, T0).unwrap();
        assert_eq!(r.blocks, 15);
    }

    #[test]
    fn three_usable_blocks_are_a_reading() {
        let mut levels = vec![playing(-30.0); 50];
        levels.extend(vec![block(-40.0); 4]);
        levels.extend([block(-71.0), block(-72.0), block(-73.0)]);
        let (_, r) = read_after(&levels);
        let r = r.unwrap();
        assert_eq!((r.blocks, r.dbfs), (3, -72.0));
    }

    #[test]
    fn dead_blocks_are_not_room() {
        let (_, r) = read_after(&[block(-100.0); 50]);
        assert_eq!(r, None);
        let mut levels = vec![block(-96.0); 50];
        levels.push(block(-75.0));
        let (_, r) = read_after(&levels);
        assert_eq!(r.unwrap().blocks, 1);
    }

    /// Blocks inside the guard before the speech, or past it, are not read.
    #[test]
    fn the_guard_keeps_the_speech_onset_out() {
        let mut room = RoomHistory::default();
        let mut levels = vec![block(-70.0); 40];
        levels.extend(vec![block(-20.0); 10]);
        room.ingest(0, &levels);
        // Speech at block 43: blocks 40..43 lie inside the 3-block guard, and
        // the ones after it are the speech itself.
        let r = room.read(43 * BLOCK, T0).unwrap();
        assert_eq!((r.blocks, r.dbfs), (40, -70.0));
    }

    #[test]
    fn with_nothing_usable_the_last_reading_is_carried_and_aged() {
        let mut room = RoomHistory::default();
        room.ingest(0, &[block(-66.0); 20]);
        let first = room.read(20 * BLOCK + AMBIENT_GUARD_SAMPLES, T0).unwrap();
        assert!(!first.carried);
        // A long reply: every block since is the pod's own voice.
        room.ingest(20, &vec![playing(-20.0); 600]);
        let later = HostMicros(T0.0 + 45_000_000);
        let r = room
            .read(620 * BLOCK + AMBIENT_GUARD_SAMPLES, later)
            .expect("the carried reading");
        assert_eq!(
            r,
            AmbientReading {
                dbfs: -66.0,
                blocks: 20,
                age_ms: first.age_ms + 45_000,
                carried: true,
            }
        );
        // A host clock behind the measurement grows nothing.
        let r = room
            .read(620 * BLOCK + AMBIENT_GUARD_SAMPLES, HostMicros(T0.0 - 5))
            .unwrap();
        assert_eq!(r.age_ms, first.age_ms);
    }

    #[test]
    fn nothing_usable_and_nothing_carried_is_no_reading() {
        let (_, r) = read_after(&[playing(-30.0); 20]);
        assert_eq!(r, None);
        assert_eq!(RoomHistory::default().read(1_000_000, T0), None);
    }

    #[test]
    fn the_carried_reading_survives_a_new_connection_and_blocks_do_not() {
        let (mut room, first) = read_after(&[block(-61.0); 10]);
        room.note_addressed(0, 100);
        room.new_connection();
        assert!(room.blocks.is_empty() && room.addressed.is_empty());
        let r = room.read(5_000_000, T0).unwrap();
        assert!(r.carried);
        assert_eq!(r.dbfs, first.unwrap().dbfs);
    }

    /// After a long reply the room is what it was before the reply, and the
    /// reading says how old that is.
    #[test]
    fn the_newest_usable_blocks_are_used_wherever_they_lie() {
        let mut levels = vec![block(-80.0); 50];
        levels.extend(vec![playing(-30.0); 200]);
        let (_, r) = read_after(&levels);
        let r = r.unwrap();
        assert_eq!((r.blocks, r.dbfs, r.carried), (50, -80.0, false));
        assert_eq!(r.age_ms, 20_000 + 300);
    }

    #[test]
    fn a_resent_run_replaces_and_the_history_is_bounded() {
        let mut room = RoomHistory::default();
        room.ingest(0, &[block(-70.0); 10]);
        room.ingest(5, &[block(-50.0); 10]);
        assert_eq!(room.blocks.len(), 15);
        assert!(
            room.blocks
                .iter()
                .zip(0u64..)
                .all(|(&(i, _), want)| i == want),
            "contiguous, ascending, each index once"
        );
        assert_eq!(room.blocks[5].1, block(-50.0));
        // An older block that was missing is slotted in place.
        let mut gap = RoomHistory::default();
        gap.ingest(0, &[block(-70.0)]);
        gap.ingest(2, &[block(-70.0)]);
        gap.ingest(1, &[block(-60.0)]);
        let indexes: Vec<u64> = gap.blocks.iter().map(|&(i, _)| i).collect();
        assert_eq!(indexes, [0, 1, 2]);
        room.ingest(15, &vec![block(-70.0); 1_000]);
        assert_eq!(room.blocks.len(), ROOM_HISTORY_BLOCKS);
        assert_eq!(room.blocks.back().unwrap().0, 1_014);
    }

    #[test]
    fn median_of_odd_and_even_counts() {
        assert_eq!(median(&mut [3.0, 1.0, 2.0]), 2.0);
        assert_eq!(median(&mut [4.0, 1.0, 2.0, 3.0]), 2.5);
    }
}
