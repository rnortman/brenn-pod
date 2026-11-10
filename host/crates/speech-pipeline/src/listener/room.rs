//! The room's level before an utterance, read from the pod's 100 ms level blocks.
//!
//! The pod meters its level channel in blocks of [`LEVEL_BLOCK_SAMPLES`] whether
//! or not a segment is open, and sends them in its status frames; this keeps the
//! last minute of them per connection and reduces them to one [`AmbientReading`]
//! for each utterance it is asked about. Only the pod's own voice and speech
//! addressed to the pod are excluded: other people talking, a television, typing
//! are the room a reply has to be heard over.
//!
//! The pod's own voice is excluded on two records, either one sufficing: the
//! pod's playing flag on each block, and this host's own record of when it was
//! playing to the pod. A block wrongly excluded costs a reading built on fewer or
//! older blocks; a block wrongly included counts the pod's reply as room noise
//! and feeds it back into the next reply's gain. Where the two records disagree
//! past their known errors, the history says so ([`FlagDispute`]).

use std::collections::VecDeque;

use audio_pipeline::wire::LEVEL_BLOCK_SAMPLES;
use pod_ingest::{HostMicros, LevelBlock};

use crate::level::DEAD_CHANNEL_DBFS;
use crate::listener::event::FlagDisputeKind;
use crate::playback::audio_ms;
use crate::types::{AmbientReading, RoomSurvey};

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

/// This host's playback spans kept for exclusion.
const PLAYED_SPANS: usize = 16;

/// How long after this host's record says the pod is quiet a flagged block is
/// still explained: 5 s of capture. Several times what a pod can still be
/// playing after this host's playback ends — a second of bank and the playout
/// hop on an abort, a stall and most of a second after an underrun.
pub(crate) const PLAYING_FLAG_SLACK_SAMPLES: u64 = 80_000;

/// After a `flag_without_playback` line, the next waits for this much of the
/// pod's capture: one minute.
pub(crate) const PLAYING_FLAG_REPORT_SAMPLES: u64 = 960_000;

/// The start of a playback span not judged for missing flags: the pod's
/// pre-roll hold, one second.
const UNFLAGGED_SKIP_SAMPLES: u64 = 16_000;

/// A playback span shorter than this is not judged for missing flags: 1.5 s.
const UNFLAGGED_MIN_SPAN_SAMPLES: u64 = 24_000;

/// Blocks a playback span must wholly hold, past its skipped start, to be
/// judged for missing flags.
const UNFLAGGED_MIN_BLOCKS: u32 = 5;

const BLOCK: u64 = LEVEL_BLOCK_SAMPLES as u64;

/// The last reading taken from blocks, and when.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Carried {
    reading: AmbientReading,
    at: HostMicros,
}

/// A span of samples over which this host's playback floor was up for the pod.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Played {
    start: u64,
    /// `None` while the floor is still up.
    end: Option<u64>,
    /// Already judged for blocks it holds with no flag.
    judged: bool,
}

/// The `flag_without_playback` lines' pacing on this connection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct FlagReport {
    /// The first sample of the block at which the last line was written.
    last_line: Option<u64>,
    /// Unexplained flagged blocks since that line.
    pending: u32,
    /// The first sample of the newest of them.
    newest: u64,
}

/// The pod's playing flag and this host's playback record disagree; what
/// [`ListenerEvent::PlayingFlagDisputed`](crate::ListenerEvent::PlayingFlagDisputed)
/// reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FlagDispute {
    pub(crate) kind: FlagDisputeKind,
    pub(crate) blocks: u32,
    pub(crate) at_sample: u64,
}

/// Why a block is not room.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetAside {
    Playing,
    Addressed,
    Dead,
}

/// One pod's room history: its level blocks, the spans that were speech
/// addressed to it and the spans this host played to it (all per connection),
/// and the last good reading (which outlives a connection, since it holds no
/// sample index).
#[derive(Debug)]
pub(crate) struct RoomHistory {
    /// `(block index, block)`, ascending by index, at most
    /// [`ROOM_HISTORY_BLOCKS`].
    blocks: VecDeque<(u64, LevelBlock)>,
    /// `[start, end)` sample spans of the last [`ADDRESSED_SPANS`] utterances.
    addressed: VecDeque<(u64, u64)>,
    /// The last [`PLAYED_SPANS`] spans of this host's playback, oldest first.
    played: VecDeque<Played>,
    /// How far past a played span's end its blocks are still not room: the
    /// listener's mute tail.
    played_tail: u64,
    /// The connection's first stated capture position.
    first_position: Option<u64>,
    flag_report: FlagReport,
    carried: Option<Carried>,
}

impl RoomHistory {
    /// An empty history whose played spans exclude `played_tail` samples past
    /// their end.
    pub(crate) fn new(played_tail: u64) -> RoomHistory {
        RoomHistory {
            blocks: VecDeque::new(),
            addressed: VecDeque::new(),
            played: VecDeque::new(),
            played_tail,
            first_position: None,
            flag_report: FlagReport::default(),
            carried: None,
        }
    }

    /// Take a status frame's run: `levels[i]` is block `first_block + i`. A block
    /// index already held is replaced, so a re-sent run is harmless.
    ///
    /// Returns where the run shows the pod's flag and this host's playback
    /// record disagreeing. Each block is judged for a flag without playback once,
    /// when it is first newer than every block held; each closed played span is
    /// judged for playback without a flag once, by the first run reaching past
    /// its end.
    pub(crate) fn ingest(&mut self, first_block: u64, levels: &[LevelBlock]) -> Vec<FlagDispute> {
        let mut disputes = Vec::new();
        for (index, block) in (first_block..).zip(levels.iter().copied()) {
            match self.blocks.back().map(|&(newest, _)| newest) {
                Some(newest) if newest >= index => {
                    match self.blocks.binary_search_by_key(&index, |&(i, _)| i) {
                        Ok(at) => self.blocks[at].1 = block,
                        Err(at) => self.blocks.insert(at, (index, block)),
                    }
                }
                _ => {
                    self.judge_flag(index, block, &mut disputes);
                    self.blocks.push_back((index, block));
                }
            }
        }
        while self.blocks.len() > ROOM_HISTORY_BLOCKS {
            self.blocks.pop_front();
        }
        self.judge_spans(&mut disputes);
        disputes
    }

    /// Count a block newly at the head against this host's record, and pace the
    /// lines: one at the first unexplained flagged block, then one each time a
    /// minute has passed since the last with any counted in it.
    fn judge_flag(&mut self, index: u64, block: LevelBlock, disputes: &mut Vec<FlagDispute>) {
        let start = index.saturating_mul(BLOCK);
        let unexplained = block.playing() && self.flag_unexplained(start);
        let report = &mut self.flag_report;
        if unexplained {
            if report.last_line.is_none() {
                report.last_line = Some(start);
                disputes.push(FlagDispute {
                    kind: FlagDisputeKind::FlagWithoutPlayback,
                    blocks: 1,
                    at_sample: start,
                });
                return;
            }
            report.pending = report.pending.saturating_add(1);
            report.newest = start;
        }
        if let Some(last) = report.last_line
            && report.pending > 0
            && start >= last.saturating_add(PLAYING_FLAG_REPORT_SAMPLES)
        {
            disputes.push(FlagDispute {
                kind: FlagDisputeKind::FlagWithoutPlayback,
                blocks: report.pending,
                at_sample: report.newest,
            });
            report.last_line = Some(start);
            report.pending = 0;
        }
    }

    /// Whether a flagged block beginning at `start` is more than the slack past
    /// everything this host knows of the pod's playback: the floor is down, and
    /// the block begins more than [`PLAYING_FLAG_SLACK_SAMPLES`] after both the
    /// connection's first position and the end of the newest played span. With
    /// no position nothing is judged.
    fn flag_unexplained(&self, start: u64) -> bool {
        let Some(first) = self.first_position else {
            return false;
        };
        let since = match self.played.back() {
            Some(Played { end: None, .. }) => return false,
            Some(Played { end: Some(end), .. }) => first.max(*end),
            None => first,
        };
        start > since.saturating_add(PLAYING_FLAG_SLACK_SAMPLES)
    }

    /// Judge every closed span the newest block now reaches past: one long
    /// enough that holds [`UNFLAGGED_MIN_BLOCKS`] blocks wholly inside it, past
    /// its skipped start, and not one of them flagged, is disputed.
    fn judge_spans(&mut self, disputes: &mut Vec<FlagDispute>) {
        let Some(&(newest, _)) = self.blocks.back() else {
            return;
        };
        let newest_end = newest.saturating_add(1).saturating_mul(BLOCK);
        for i in 0..self.played.len() {
            let span = self.played[i];
            let Some(end) = span.end else { continue };
            if span.judged || newest_end <= end {
                continue;
            }
            self.played[i].judged = true;
            if end.saturating_sub(span.start) < UNFLAGGED_MIN_SPAN_SAMPLES {
                continue;
            }
            let from = span.start.saturating_add(UNFLAGGED_SKIP_SAMPLES);
            let inside = self.blocks.iter().filter(|&&(index, _)| {
                let start = index.saturating_mul(BLOCK);
                start >= from && start.saturating_add(BLOCK) <= end
            });
            let (mut count, mut flagged) = (0u32, false);
            for &(_, block) in inside {
                count = count.saturating_add(1);
                flagged |= block.playing();
            }
            if count >= UNFLAGGED_MIN_BLOCKS && !flagged {
                disputes.push(FlagDispute {
                    kind: FlagDisputeKind::PlaybackWithoutFlag,
                    blocks: count,
                    at_sample: span.start,
                });
            }
        }
    }

    /// The connection's capture position, as each anchor states it. The first
    /// is kept; returns whether this was it.
    pub(crate) fn note_position(&mut self, position: u64) -> bool {
        if self.first_position.is_some() {
            return false;
        }
        self.first_position = Some(position);
        true
    }

    /// This host's playback floor went up at `at`. A span already open stays
    /// the one span.
    pub(crate) fn playback_started(&mut self, at: u64) {
        if matches!(self.played.back(), Some(Played { end: None, .. })) {
            return;
        }
        self.push_played(Played {
            start: at,
            end: None,
            judged: false,
        });
    }

    /// This host's playback floor went down at `at`, closing the open span.
    pub(crate) fn playback_ended(&mut self, at: u64) {
        if let Some(span) = self.played.back_mut()
            && span.end.is_none()
        {
            span.end = Some(at.max(span.start));
        }
    }

    /// This host's playback went up and down before the connection had a
    /// position, which is `at`: a zero-length span there, whose tail is the
    /// exclusion.
    pub(crate) fn played_before(&mut self, at: u64) {
        self.push_played(Played {
            start: at,
            end: Some(at),
            judged: false,
        });
    }

    fn push_played(&mut self, span: Played) {
        if self.played.len() == PLAYED_SPANS {
            self.played.pop_front();
        }
        self.played.push_back(span);
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

    /// A new connection: its sample domain may restart (a reboot), so blocks,
    /// spans, the first position and the report pacing go; the carried reading
    /// stays.
    pub(crate) fn new_connection(&mut self) {
        self.blocks.clear();
        self.addressed.clear();
        self.played.clear();
        self.first_position = None;
        self.flag_report = FlagReport::default();
    }

    /// The room before speech beginning at `speech_lo`, measured at host time
    /// `at`. The newest [`AMBIENT_WINDOW_BLOCKS`] usable blocks, wherever they
    /// lie in the history; their median, how many, and how old the newest is
    /// against `speech_lo`. With no usable block, the last good reading aged by
    /// the host time since it was taken; with none of those either, `None`.
    ///
    /// Beside it, the survey of every block held that ends before the guard.
    pub(crate) fn read(
        &mut self,
        speech_lo: u64,
        at: HostMicros,
    ) -> (Option<AmbientReading>, RoomSurvey) {
        let limit = speech_lo.saturating_sub(AMBIENT_GUARD_SAMPLES);
        let mut used: Vec<f32> = Vec::with_capacity(AMBIENT_WINDOW_BLOCKS);
        let mut newest_end: Option<u64> = None;
        let mut survey = RoomSurvey::default();
        for (pos, &(index, block)) in self.blocks.iter().enumerate().rev() {
            let start = index.saturating_mul(BLOCK);
            let end = start.saturating_add(BLOCK);
            if end > limit {
                continue;
            }
            survey.held = survey.held.saturating_add(1);
            let count = match self.set_aside(pos, index, start, end, block) {
                Some(SetAside::Playing) => &mut survey.playing,
                Some(SetAside::Addressed) => &mut survey.addressed,
                Some(SetAside::Dead) => &mut survey.dead,
                None => {
                    if used.len() < AMBIENT_WINDOW_BLOCKS {
                        newest_end.get_or_insert(end);
                        used.push(block.dbfs());
                    }
                    continue;
                }
            };
            *count = count.saturating_add(1);
        }
        let Some(newest_end) = newest_end else {
            let carried = self.carried.map(|c| {
                let grown_ms = at.checked_delta(c.at).unwrap_or(0) / 1_000;
                AmbientReading {
                    age_ms: saturate_u32(u64::from(c.reading.age_ms).saturating_add(grown_ms)),
                    carried: true,
                    ..c.reading
                }
            });
            return (carried, survey);
        };
        let reading = AmbientReading {
            dbfs: median(&mut used),
            blocks: u16::try_from(used.len()).unwrap_or(u16::MAX),
            age_ms: saturate_u32(audio_ms(speech_lo.saturating_sub(newest_end))),
            carried: false,
        };
        self.carried = Some(Carried { reading, at });
        (Some(reading), survey)
    }

    /// Why the block at deque position `pos` (index `index`, samples
    /// `[start, end)`) is not room, first reason first: the pod playing — the
    /// block flagged, inside the playback tail after a flagged block, or
    /// overlapping a played span or its tail — then speech addressed to the
    /// pod, then a dead channel. `None` when it is room.
    fn set_aside(
        &self,
        pos: usize,
        index: u64,
        start: u64,
        end: u64,
        block: LevelBlock,
    ) -> Option<SetAside> {
        if block.playing() {
            return Some(SetAside::Playing);
        }
        let tail_from = index.saturating_sub(AMBIENT_PLAYBACK_TAIL_BLOCKS);
        let in_tail = self
            .blocks
            .range(..pos)
            .rev()
            .take_while(|&&(i, _)| i >= tail_from)
            .any(|&(_, b)| b.playing());
        if in_tail {
            return Some(SetAside::Playing);
        }
        let in_played = self.played.iter().any(|span| {
            span.start < end
                && span
                    .end
                    .is_none_or(|e| start <= e.saturating_add(self.played_tail))
        });
        if in_played {
            return Some(SetAside::Playing);
        }
        if self.addressed.iter().any(|&(s, e)| start < e && s < end) {
            return Some(SetAside::Addressed);
        }
        if block.dbfs() <= DEAD_CHANNEL_DBFS {
            return Some(SetAside::Dead);
        }
        None
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

    /// The listener's default mute tail, 400 ms.
    const TAIL: u64 = 6_400;

    fn history() -> RoomHistory {
        RoomHistory::new(TAIL)
    }

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
        let mut room = history();
        room.ingest(0, levels);
        let speech_lo = levels.len() as u64 * BLOCK + AMBIENT_GUARD_SAMPLES;
        let reading = room.read(speech_lo, T0).0;
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
        let mut room = history();
        let mut levels = vec![block(-70.0); 30];
        levels[10..20].fill(block(-30.0));
        room.ingest(0, &levels);
        // The utterance covered blocks 10..20 exactly; a span touching a block
        // by one sample excludes it.
        room.note_addressed(10 * BLOCK, 20 * BLOCK - 1);
        let r = room.read(30 * BLOCK + AMBIENT_GUARD_SAMPLES, T0).0.unwrap();
        assert_eq!((r.blocks, r.dbfs), (20, -70.0));
        // A re-carve of the same utterance extends its span.
        room.note_addressed(10 * BLOCK, 25 * BLOCK);
        let r = room.read(30 * BLOCK + AMBIENT_GUARD_SAMPLES, T0).0.unwrap();
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
        let mut room = history();
        let mut levels = vec![block(-70.0); 40];
        levels.extend(vec![block(-20.0); 10]);
        room.ingest(0, &levels);
        // Speech at block 43: blocks 40..43 lie inside the 3-block guard, and
        // the ones after it are the speech itself.
        let r = room.read(43 * BLOCK, T0).0.unwrap();
        assert_eq!((r.blocks, r.dbfs), (40, -70.0));
    }

    #[test]
    fn with_nothing_usable_the_last_reading_is_carried_and_aged() {
        let mut room = history();
        room.ingest(0, &[block(-66.0); 20]);
        let first = room.read(20 * BLOCK + AMBIENT_GUARD_SAMPLES, T0).0.unwrap();
        assert!(!first.carried);
        // A long reply: every block since is the pod's own voice.
        room.ingest(20, &vec![playing(-20.0); 600]);
        let later = HostMicros(T0.0 + 45_000_000);
        let r = room
            .read(620 * BLOCK + AMBIENT_GUARD_SAMPLES, later)
            .0
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
            .0
            .unwrap();
        assert_eq!(r.age_ms, first.age_ms);
    }

    #[test]
    fn nothing_usable_and_nothing_carried_is_no_reading() {
        let (_, r) = read_after(&[playing(-30.0); 20]);
        assert_eq!(r, None);
        assert_eq!(history().read(1_000_000, T0).0, None);
    }

    #[test]
    fn the_carried_reading_survives_a_new_connection_and_blocks_do_not() {
        let (mut room, first) = read_after(&[block(-61.0); 10]);
        room.note_addressed(0, 100);
        room.new_connection();
        assert!(room.blocks.is_empty() && room.addressed.is_empty());
        let r = room.read(5_000_000, T0).0.unwrap();
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
        let mut room = history();
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
        let mut gap = history();
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

    /// Blocks `[from, to)` at `dbfs`, flagged or not.
    fn run(room: &mut RoomHistory, from: u64, to: u64, b: LevelBlock) -> Vec<FlagDispute> {
        let n = usize::try_from(to - from).unwrap();
        room.ingest(from, &vec![b; n])
    }

    /// A history with a position at sample 0, as a connection's first anchor
    /// gives it.
    fn positioned() -> RoomHistory {
        let mut room = history();
        assert!(room.note_position(0));
        room
    }

    /// The first block not set aside by a span `[from, to]` (in blocks) of this
    /// host's playback, read for speech well after it.
    #[test]
    fn a_played_span_and_its_tail_are_excluded_with_no_flag() {
        let mut room = positioned();
        run(&mut room, 0, 10, block(-70.0));
        room.playback_started(10 * BLOCK);
        run(&mut room, 10, 20, block(-30.0));
        room.playback_ended(20 * BLOCK);
        // 400 ms of tail: blocks 20..24 end inside it, block 24 begins on its
        // last sample.
        run(&mut room, 20, 25, block(-40.0));
        run(&mut room, 25, 26, block(-65.0));
        let (r, survey) = room.read(26 * BLOCK + AMBIENT_GUARD_SAMPLES, T0);
        let r = r.unwrap();
        assert_eq!((r.blocks, r.dbfs), (11, -70.0));
        assert_eq!(r.age_ms, 300, "the block past the tail is the newest used");
        assert_eq!(
            survey,
            RoomSurvey {
                held: 26,
                playing: 15,
                addressed: 0,
                dead: 0
            }
        );
    }

    #[test]
    fn an_open_span_excludes_every_block_from_its_start() {
        let mut room = positioned();
        run(&mut room, 0, 10, block(-70.0));
        room.playback_started(10 * BLOCK + 1);
        run(&mut room, 10, 200, block(-71.0));
        let (r, survey) = room.read(200 * BLOCK + AMBIENT_GUARD_SAMPLES, T0);
        let r = r.unwrap();
        assert_eq!((r.blocks, r.dbfs), (10, -70.0));
        assert_eq!((survey.held, survey.playing), (200, 190));
        // A floor that passes from one turn to the next without dropping is the
        // one span.
        room.playback_started(150 * BLOCK);
        room.playback_ended(200 * BLOCK);
        assert_eq!(room.played.len(), 1);
        assert_eq!(room.played[0].start, 10 * BLOCK + 1);
    }

    /// The union of the two records: a span this host closed early, with the
    /// pod still flagging past it, excludes through the last flagged block and
    /// its tail.
    #[test]
    fn flag_and_span_together_exclude_their_union() {
        let mut room = positioned();
        run(&mut room, 0, 10, block(-70.0));
        room.playback_started(10 * BLOCK);
        room.playback_ended(15 * BLOCK);
        run(&mut room, 10, 30, playing(-30.0));
        run(&mut room, 30, 34, block(-40.0));
        run(&mut room, 34, 40, block(-60.0));
        let (r, survey) = room.read(40 * BLOCK + AMBIENT_GUARD_SAMPLES, T0);
        let r = r.unwrap();
        assert_eq!(r.blocks, 16);
        assert_eq!(r.dbfs, -70.0, "10 blocks at -70, 6 at -60");
        assert_eq!(survey.playing, 24);
    }

    #[test]
    fn all_blocks_flagged_and_no_span_is_no_reading_and_says_so() {
        let mut room = history();
        run(&mut room, 0, 300, playing(-75.0));
        let (r, survey) = room.read(300 * BLOCK + AMBIENT_GUARD_SAMPLES, T0);
        assert_eq!(r, None);
        assert_eq!(survey.held, 300);
        assert_eq!(survey.held, survey.playing);
    }

    /// Each block counts once, under the first reason that applies, and
    /// blocks inside the guard are not held.
    #[test]
    fn the_survey_counts_a_mixed_history() {
        let mut room = history();
        let mut levels = vec![block(-70.0); 40];
        levels[2] = playing(-30.0); // blocks 3..7 are its tail
        levels[20] = playing(-30.0); // flagged and addressed: playing
        levels[30] = block(-100.0); // dead
        levels[31] = block(-100.0); // dead and addressed: addressed
        room.ingest(0, &levels);
        room.note_addressed(18 * BLOCK, 20 * BLOCK + 1);
        room.note_addressed(31 * BLOCK, 33 * BLOCK);
        // Speech at block 40: blocks 37..40 are inside the guard.
        let (r, survey) = room.read(40 * BLOCK, T0);
        assert_eq!(
            survey,
            RoomSurvey {
                held: 37,
                playing: 1 + 4 + 1 + 4,
                addressed: 2 + 2,
                dead: 1,
            }
        );
        let used = 37 - 10 - 4 - 1;
        assert_eq!(r.unwrap().blocks, used);
    }

    /// The bench run that found the stuck flag, as a regression: the
    /// connection's history all flagged, then the pod reading the room on
    /// either side of a 2 s reply, the follow-up 3 s after it. The reading
    /// comes from the unflagged blocks on both sides of the reply.
    #[test]
    fn the_bench_run_reads_the_room_on_both_sides_of_the_reply() {
        let mut room = history();
        run(&mut room, 0, 300, playing(-77.0));
        assert!(room.note_position(300 * BLOCK));
        run(&mut room, 300, 400, block(-78.0));
        room.playback_started(400 * BLOCK);
        run(&mut room, 400, 420, playing(-30.0));
        room.playback_ended(420 * BLOCK);
        run(&mut room, 420, 450, block(-76.0));
        let speech = 450 * BLOCK;
        let (r, survey) = room.read(speech, T0);
        let r = r.expect("a reading");
        assert!(!r.carried);
        // 100 blocks: the 22 between the reply's tail and the guard, then 78
        // before the reply.
        assert_eq!(r.blocks, 100);
        assert_eq!(r.dbfs, -78.0);
        assert_eq!(r.age_ms, 300);
        // The history and its flag tail; the reply and the span's tail through
        // the block beginning on its last sample.
        assert_eq!(survey.playing, 300 + 4 + 20 + 5);
    }

    /// No block before the connection's first position is judged, nor one
    /// within the slack after it; past it, the first unexplained flagged block
    /// is a line, the next minute's are counted, and the first block a minute
    /// after the line writes them.
    #[test]
    fn flags_without_playback_are_paced_from_the_first_position() {
        let mut room = history();
        assert_eq!(run(&mut room, 0, 300, playing(-70.0)), [], "history");
        room.note_position(300 * BLOCK);
        let slack_end = 300 + PLAYING_FLAG_SLACK_SAMPLES / BLOCK;
        assert_eq!(run(&mut room, 300, slack_end + 1, playing(-70.0)), []);
        let first = run(&mut room, slack_end + 1, slack_end + 2, playing(-70.0));
        assert_eq!(
            first,
            [FlagDispute {
                kind: FlagDisputeKind::FlagWithoutPlayback,
                blocks: 1,
                at_sample: (slack_end + 1) * BLOCK,
            }]
        );
        let minute = PLAYING_FLAG_REPORT_SAMPLES / BLOCK;
        let line_at = slack_end + 1;
        // Flagged for 10 s, then quiet for the rest of the minute.
        assert_eq!(
            run(&mut room, line_at + 1, line_at + 101, playing(-70.0)),
            []
        );
        assert_eq!(
            run(&mut room, line_at + 101, line_at + minute, block(-70.0)),
            []
        );
        let second = run(
            &mut room,
            line_at + minute,
            line_at + minute + 1,
            block(-70.0),
        );
        assert_eq!(
            second,
            [FlagDispute {
                kind: FlagDisputeKind::FlagWithoutPlayback,
                blocks: 100,
                at_sample: (line_at + 100) * BLOCK,
            }]
        );
        // Nothing counted since: nothing more.
        assert_eq!(
            run(
                &mut room,
                line_at + minute + 1,
                line_at + 3 * minute,
                block(-70.0)
            ),
            []
        );
    }

    /// While the floor is up nothing is judged, and after it drops the slack
    /// runs from the span's end.
    #[test]
    fn flags_during_and_just_after_playback_are_explained() {
        let mut room = positioned();
        let slack = PLAYING_FLAG_SLACK_SAMPLES / BLOCK;
        room.playback_started(2 * slack * BLOCK);
        assert_eq!(run(&mut room, 0, 4 * slack, playing(-70.0)), []);
        room.playback_ended(4 * slack * BLOCK);
        assert_eq!(run(&mut room, 4 * slack, 5 * slack + 1, playing(-70.0)), []);
        assert_eq!(
            run(&mut room, 5 * slack + 1, 5 * slack + 2, playing(-70.0)).len(),
            1
        );
    }

    /// A span 1.5 s or longer whose blocks, its first second skipped, carry no
    /// flag is judged once, on the first run reaching past its end; one flagged
    /// block inside, or a span too short, is no line.
    #[test]
    fn playback_without_flags_is_judged_once_per_span() {
        let judge = |len_blocks: u64, flag_at: Option<u64>| {
            let mut room = positioned();
            room.playback_started(10 * BLOCK);
            run(&mut room, 0, 10 + len_blocks, block(-70.0));
            if let Some(i) = flag_at {
                room.ingest(10 + i, &[playing(-30.0)]);
            }
            room.playback_ended((10 + len_blocks) * BLOCK);
            let at_end = run(
                &mut room,
                10 + len_blocks,
                10 + len_blocks + 1,
                block(-70.0),
            );
            let later = run(
                &mut room,
                10 + len_blocks + 1,
                10 + len_blocks + 40,
                block(-70.0),
            );
            assert_eq!(later, [], "judged once");
            at_end
        };
        assert_eq!(
            judge(30, None),
            [FlagDispute {
                kind: FlagDisputeKind::PlaybackWithoutFlag,
                blocks: 20,
                at_sample: 10 * BLOCK,
            }]
        );
        assert_eq!(judge(30, Some(25)), []);
        assert_eq!(judge(10, None), [], "a 1 s span is not judged");
        // A run that ends on the span's last sample does not reach past it.
        let mut room = positioned();
        room.playback_started(0);
        room.playback_ended(30 * BLOCK);
        assert_eq!(run(&mut room, 0, 30, block(-70.0)), []);
        assert_eq!(run(&mut room, 30, 31, block(-70.0)).len(), 1);
    }

    /// A long span whose history holds fewer than five blocks past its skipped
    /// start, as runs that skip indices leave it, is no line; five are one,
    /// counting only the blocks held.
    #[test]
    fn playback_without_flags_needs_five_blocks_held() {
        let judge = |held: &[u64]| {
            let mut room = positioned();
            let start = 10 * BLOCK;
            let end = start + 3 * 16_000;
            room.playback_started(start);
            run(&mut room, 0, 20, block(-70.0));
            for &i in held {
                assert_eq!(room.ingest(i, &[block(-70.0)]), []);
            }
            room.playback_ended(end);
            room.ingest(end / BLOCK, &[block(-70.0)])
        };
        assert_eq!(judge(&[22, 25, 28, 33]), [], "four blocks held");
        assert_eq!(
            judge(&[22, 25, 28, 33, 39]),
            [FlagDispute {
                kind: FlagDisputeKind::PlaybackWithoutFlag,
                blocks: UNFLAGGED_MIN_BLOCKS,
                at_sample: 10 * BLOCK,
            }]
        );
    }

    /// A span of exactly the minimum length with every block held is judged; one
    /// a sample shorter is not. With the skip and the five-block minimum as they
    /// are, the shorter span also holds only four whole blocks, so this pins the
    /// minimum's edge but cannot tell which of the two rules refused the shorter
    /// span.
    #[test]
    fn playback_without_flags_judges_from_the_minimum_span() {
        let judge = |len: u64| {
            let mut room = positioned();
            let start = 10 * BLOCK;
            room.playback_started(start);
            let end = start + len;
            let past = end.div_ceil(BLOCK) + 1;
            run(&mut room, 0, end / BLOCK, block(-70.0));
            room.playback_ended(end);
            run(&mut room, end / BLOCK, past, block(-70.0))
        };
        assert_eq!(
            judge(UNFLAGGED_MIN_SPAN_SAMPLES),
            [FlagDispute {
                kind: FlagDisputeKind::PlaybackWithoutFlag,
                blocks: UNFLAGGED_MIN_BLOCKS,
                at_sample: 10 * BLOCK,
            }]
        );
        assert_eq!(judge(UNFLAGGED_MIN_SPAN_SAMPLES - 1), []);
    }

    #[test]
    fn a_new_connection_forgets_spans_the_position_and_the_pacing() {
        let mut room = positioned();
        room.playback_started(0);
        room.flag_report.pending = 3;
        room.new_connection();
        assert!(room.played.is_empty());
        assert_eq!(room.first_position, None);
        assert_eq!(room.flag_report, FlagReport::default());
        assert!(room.note_position(5));
        assert!(!room.note_position(9), "the first is kept");
        // A zero-length span from before the position excludes its tail.
        room.played_before(5 * BLOCK);
        run(&mut room, 0, 20, block(-70.0));
        let (_, survey) = room.read(20 * BLOCK + AMBIENT_GUARD_SAMPLES, T0);
        assert_eq!(survey.playing, 5, "blocks 5..10");
    }
}
