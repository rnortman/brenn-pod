//! The capture loop and the writer thread it feeds.

use std::collections::VecDeque;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::thread;
use std::time::{Duration, Instant};

use alsa_pcm::{ClockPair, Read, Recovery, Wait};
use capture_record::{
    End, EndReason, Event, Gap, GapCause, Granted, Header, MAX_BLOCK_PCM_BYTES, RecordWriter,
    StartPair,
};

use crate::lines::{
    CaptureFailed, CaptureGap, CaptureLate, CaptureStarted, CaptureStopped, CardLine, FailReason,
    Lines, note,
};
use crate::seams::{CaptureSource, Clock, Destination};

/// How long one wait for a period lasts before the stop flag is looked at again.
pub const WAIT_TIMEOUT: Duration = Duration::from_millis(100);

/// Consecutive timed-out waits that end the capture: two seconds with no frame
/// from a device that owes one every period. Only a delivered period restarts
/// the count.
pub const STALL_TIMEOUTS: u32 = 20;

/// From the moment the stream stops, how long the hand-over of the last records
/// and the writer's drain, sync and close may take together.
pub const STOP_BOUND: Duration = Duration::from_secs(5);

/// Seconds of capture the free-space floor holds back, at the granted
/// parameters, beside [`FLOOR_EXTRA_BYTES`].
pub const FLOOR_SECONDS: u64 = 5;

/// Bytes the free-space floor holds back beside [`FLOOR_SECONDS`] of capture.
pub const FLOOR_EXTRA_BYTES: u64 = 1 << 20;

/// Seconds of periods the queue between the loop and the writer holds.
const QUEUE_SECONDS: u64 = 2;

/// How often the writer reads the destination's free space.
const FREE_SPACE_INTERVAL: Duration = Duration::from_secs(1);

/// How often the ending hand-over retries a full queue.
const HAND_OVER_POLL: Duration = Duration::from_millis(2);

const BACKLOG_DETAIL: &str = "the destination was not taking blocks fast enough; the device \
    delivered these frames and they were dropped before reaching the record";

/// The period demanded of a device at `rate_hz`: a 20 ms slice, or the largest
/// whole number of frames that fits in one block where that is smaller. `None`
/// when no frame fits in a block, or the rate gives a period of no frames.
pub fn period_frames(rate_hz: u32, frame_bytes: usize) -> Option<u32> {
    if frame_bytes == 0 {
        return None;
    }
    let fits = u32::try_from(MAX_BLOCK_PCM_BYTES / frame_bytes).unwrap_or(u32::MAX);
    let period = (rate_hz / 50).min(fits);
    (period > 0).then_some(period)
}

/// Periods the queue holds: two seconds of them, at least one.
pub fn queue_capacity(granted: &Granted) -> usize {
    let periods = QUEUE_SECONDS * u64::from(granted.rate_hz) / u64::from(granted.period_frames);
    usize::try_from(periods).unwrap_or(usize::MAX).max(1)
}

/// Free bytes under which the writer ends the record with `destination_full`:
/// five seconds of capture at the granted parameters plus 1 MiB, so the end
/// record still fits.
pub fn free_space_floor(granted: &Granted) -> u64 {
    let per_second = u64::from(granted.rate_hz).saturating_mul(granted.frame_bytes() as u64);
    FLOOR_SECONDS
        .saturating_mul(per_second)
        .saturating_add(FLOOR_EXTRA_BYTES)
}

/// How a capture ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Told to stop; the record ends with `end`/`stopped`, synced and closed.
    Stopped,
    /// Anything else. One `capture_failed` line says what.
    Failed,
}

impl Outcome {
    pub fn exit_code(self) -> u8 {
        match self {
            Outcome::Stopped => 0,
            Outcome::Failed => 1,
        }
    }
}

/// Record the stream from `source` into `dest` until `stop` is set, the stream
/// dies or stalls, or the destination fails or runs low.
///
/// The stream is triggered here and the start pair taken directly after;
/// `header.started` is overwritten with it before the header is written. Every
/// outcome is one terminal line on `lines`: `capture_stopped`, or
/// `capture_failed`. `stop_bound` is [`STOP_BOUND`] outside tests.
#[allow(clippy::too_many_arguments)]
pub fn run<S, C, D, W>(
    source: &mut S,
    clock: C,
    dest: D,
    mut header: Header,
    path: &str,
    stop: &AtomicBool,
    lines: &mut Lines<W>,
    stop_bound: Duration,
) -> Outcome
where
    S: CaptureSource,
    C: Clock,
    D: Destination,
    W: Write,
{
    let record_id = header.record_id.clone();
    // Until the header is written no record exists, so the lines below name none
    // and no position in one: a file at `path` holds no header carrying
    // `record_id`.
    if let Err(e) = source.start() {
        let pair = clock.pair();
        failed(
            lines,
            None,
            FailReason::StreamDied,
            &e.to_string(),
            None,
            pair,
            None,
        );
        return Outcome::Failed;
    }
    let start = clock.pair();
    header.started = StartPair {
        mono_us: start.mono_us,
        wall_us: start.wall_us,
    };
    let w = match RecordWriter::new(dest, &header) {
        Ok(w) => w,
        Err(e) => {
            if let Err(e) = source.stop() {
                note(format_args!("{e}"));
            }
            failed(
                lines,
                None,
                FailReason::Destination,
                &format!("writing the record's header failed: {e}"),
                None,
                clock.pair(),
                None,
            );
            return Outcome::Failed;
        }
    };
    lines.emit(
        "capture_started",
        &CaptureStarted {
            record_id: &record_id,
            path,
            device: &header.device,
            granted: &header.granted,
            layout: &header.layout,
            build: &header.writer.build,
            frame: 0,
            mono_us: start.mono_us,
            wall_us: start.wall_us,
        },
    );

    let granted = &header.granted;
    let (tx, rx) = mpsc::sync_channel(queue_capacity(granted));
    let (done_tx, done_rx) = mpsc::channel();
    let writer = Writer {
        w,
        clock: clock.clone(),
        floor: free_space_floor(granted),
        frame_bytes: granted.frame_bytes(),
        position: 0,
    };
    thread::Builder::new()
        .name("pod-capture-writer".to_owned())
        .spawn(move || {
            if let Some(end) = writer.run(rx) {
                // The capture may have stopped listening (a stalled stop); there
                // is no one left to tell.
                let _ = done_tx.send(end);
            }
        })
        .expect("the OS refused to start the writer thread");

    let mut cap = Capture {
        tx,
        pending: VecDeque::new(),
        writer_gone: false,
        position: 0,
        gaps: 0,
        timeouts: 0,
        late: false,
        period_frames: granted.period_frames,
        record_id: &record_id,
        clock: &clock,
        lines,
    };

    let ending = loop {
        let waited = source.wait(WAIT_TIMEOUT);
        if stop.load(Ordering::SeqCst) {
            break Ending::Stopped;
        }
        if cap.writer_gone {
            break Ending::WriterEnded;
        }
        match waited {
            Err(e) => break Ending::StreamDied(e.to_string()),
            Ok(Wait::TimedOut) => {
                cap.timeouts += 1;
                if cap.timeouts >= STALL_TIMEOUTS {
                    let waited_ms = WAIT_TIMEOUT.as_millis() * u128::from(STALL_TIMEOUTS);
                    break Ending::StreamDied(format!(
                        "the device delivered nothing for {waited_ms} ms ({STALL_TIMEOUTS} \
                         waits in a row timed out)"
                    ));
                }
            }
            Ok(Wait::Recovered(r)) => cap.recovered(r),
            Ok(Wait::Ready) => match source.read() {
                Err(e) => break Ending::StreamDied(e.to_string()),
                Ok(Read::Recovered(r)) => cap.recovered(r),
                // Nothing delivered: nothing to record, and not a period that
                // restarts the stall count.
                Ok(Read::Period(p)) if p.frames == 0 => {}
                Ok(Read::Period(p)) => {
                    let pcm = p.bytes.to_vec();
                    let (frames, backlog) = (p.frames as u64, p.backlog_frames);
                    let pair = cap.clock.pair();
                    cap.period(pcm, frames, backlog, pair);
                }
            },
        }
    };

    // Stop means now: frames still in the device's buffer are not read.
    let stopped_at = Instant::now();
    if let Err(e) = source.stop() {
        note(format_args!("{e}"));
    }
    let deadline = stopped_at + stop_bound;
    let end = match &ending {
        Ending::Stopped => Some((EndReason::Stopped, "told to stop".to_owned())),
        Ending::StreamDied(detail) => Some((EndReason::StreamDied, detail.clone())),
        Ending::WriterEnded => None,
    };
    if let Some((reason, detail)) = end {
        let pair = cap.clock.pair();
        cap.pending.push_back(Item::Event {
            mono_us: pair.mono_us,
            event: Event::End(End {
                reason,
                at_frame: cap.position,
                wall_us: pair.wall_us,
                detail,
            }),
        });
        // The stream is no longer read, so waiting on the queue costs no capture
        // period: the pending list and `end` are handed over, never dropped.
        loop {
            cap.queue_pending();
            if cap.pending.is_empty() || cap.writer_gone || Instant::now() >= deadline {
                break;
            }
            thread::sleep(HAND_OVER_POLL);
        }
    }
    let writer_end = if cap.pending.is_empty() || cap.writer_gone {
        done_rx.recv_timeout(deadline.saturating_duration_since(Instant::now()))
    } else {
        Err(RecvTimeoutError::Timeout)
    };

    let pair = cap.clock.pair();
    let at_frame = Some(cap.position);
    let (reason, detail) = match (writer_end, ending) {
        (Ok(WriterEnd::Finished), Ending::Stopped) => {
            cap.lines.emit(
                "capture_stopped",
                &CaptureStopped {
                    record_id: cap.record_id,
                    frames: cap.position,
                    gaps: cap.gaps,
                    mono_us: pair.mono_us,
                    wall_us: pair.wall_us,
                },
            );
            return Outcome::Stopped;
        }
        (Ok(WriterEnd::Finished), Ending::StreamDied(detail)) => (FailReason::StreamDied, detail),
        (Ok(WriterEnd::Finished), Ending::WriterEnded) => (
            FailReason::WriteFailed,
            "the writer finished before it was handed an end record".to_owned(),
        ),
        (Ok(WriterEnd::DestinationFull { detail }), _) => (
            FailReason::DestinationFull,
            format!("{detail}; the record ends with end/destination_full"),
        ),
        (
            Ok(WriterEnd::WriteFailed {
                detail,
                end_written,
            }),
            _,
        ) => (
            FailReason::WriteFailed,
            if end_written {
                format!("{detail}; the record ends with end/write_failed")
            } else {
                format!("{detail}; the record has no end record")
            },
        ),
        (Ok(WriterEnd::SyncFailed { detail }), _) => (
            FailReason::WriteFailed,
            format!(
                "{detail}; the record holds its end record, which may not have reached the \
                 storage device"
            ),
        ),
        (Err(RecvTimeoutError::Timeout), _) => (
            FailReason::WriteStalled,
            format!(
                "the writer had not reported {} ms after the stream stopped; it may be \
                 held on a write or on the sync after its end record, so whether the \
                 record ends is for its reader to say",
                stop_bound.as_millis()
            ),
        ),
        (Err(RecvTimeoutError::Disconnected), _) => (
            FailReason::WriteFailed,
            "the writer thread ended without reporting; the record may have no end record"
                .to_owned(),
        ),
    };
    failed(
        cap.lines,
        Some(cap.record_id),
        reason,
        &detail,
        at_frame,
        pair,
        None,
    );
    Outcome::Failed
}

/// Emit `capture_failed`, then say on stderr why the capture failed or was
/// refused.
pub(crate) fn failed<W: Write>(
    lines: &mut Lines<W>,
    record_id: Option<&str>,
    reason: FailReason,
    detail: &str,
    at_frame: Option<u64>,
    pair: ClockPair,
    cards: Option<Vec<CardLine>>,
) {
    lines.emit(
        "capture_failed",
        &CaptureFailed {
            record_id,
            reason,
            detail,
            at_frame,
            mono_us: pair.mono_us,
            wall_us: pair.wall_us,
            cards,
        },
    );
    note(format_args!("capture failed ({reason:?}): {detail}"));
}

/// Why the capture loop stopped reading.
enum Ending {
    Stopped,
    StreamDied(String),
    /// The writer ended the record itself (floor or failed write).
    WriterEnded,
}

/// What the loop hands the writer.
enum Item {
    Block {
        mono_us: u64,
        wall_us: u64,
        first_frame: u64,
        backlog_frames: u32,
        pcm: Vec<u8>,
    },
    Event {
        mono_us: u64,
        event: Event,
    },
}

/// The capture loop's state between turns.
struct Capture<'a, C, W: Write> {
    tx: SyncSender<Item>,
    /// Events not yet queued, in order. A block is never here: a block that
    /// finds the queue full, or this list non-empty, is dropped into the
    /// `writer_backlog` gap at its tail.
    pending: VecDeque<Item>,
    /// The writer has stopped taking items.
    writer_gone: bool,
    /// The delivered-frame count.
    position: u64,
    /// Gap records made so far.
    gaps: u64,
    /// Consecutive timed-out waits.
    timeouts: u32,
    /// Inside a run of late blocks queued for the record, as the reader counts
    /// them: a non-late block or any gap ends it, a dropped block included.
    late: bool,
    period_frames: u32,
    record_id: &'a str,
    clock: &'a C,
    lines: &'a mut Lines<W>,
}

impl<C: Clock, W: Write> Capture<'_, C, W> {
    /// Frames the device delivered: handed to the writer, or dropped into a
    /// `writer_backlog` gap. Either way the position advances.
    fn period(&mut self, pcm: Vec<u8>, frames: u64, backlog_frames: u32, pair: ClockPair) {
        self.timeouts = 0;
        let first_frame = self.position;
        self.position += frames;
        let late = backlog_frames >= self.period_frames;
        self.queue_pending();
        if self.pending.is_empty() && !self.writer_gone {
            let block = Item::Block {
                mono_us: pair.mono_us,
                wall_us: pair.wall_us,
                first_frame,
                backlog_frames,
                pcm,
            };
            match self.tx.try_send(block) {
                Ok(()) => {
                    // A late run starts at a block the record holds.
                    if late && !self.late {
                        self.lines.emit(
                            "capture_late",
                            &CaptureLate {
                                record_id: self.record_id,
                                at_frame: first_frame,
                                backlog_frames,
                                mono_us: pair.mono_us,
                                wall_us: pair.wall_us,
                            },
                        );
                    }
                    self.late = late;
                    return;
                }
                Err(TrySendError::Full(_)) => {}
                Err(TrySendError::Disconnected(_)) => {
                    self.writer_gone = true;
                    return;
                }
            }
        }
        self.drop_block(first_frame, frames, pair);
    }

    /// A block the queue would not take: its frames join the `writer_backlog`
    /// gap at the tail of the pending list, or start a new one there.
    fn drop_block(&mut self, first_frame: u64, frames: u64, pair: ClockPair) {
        self.late = false;
        if let Some(Item::Event {
            event:
                Event::Gap(Gap {
                    cause: GapCause::WriterBacklog,
                    frames: Some(n),
                    ..
                }),
            ..
        }) = self.pending.back_mut()
        {
            *n += frames;
            return;
        }
        self.gaps += 1;
        self.pending.push_back(Item::Event {
            mono_us: pair.mono_us,
            event: Event::Gap(Gap {
                cause: GapCause::WriterBacklog,
                at_frame: first_frame,
                frames: Some(frames),
                wall_us: pair.wall_us,
                detail: BACKLOG_DETAIL.to_owned(),
            }),
        });
    }

    /// The stream failed and was restarted: an unmeasured gap here, announced
    /// now. The position does not advance.
    fn recovered(&mut self, r: Recovery) {
        let pair = self.clock.pair();
        let cause = match r.errno {
            libc::EPIPE => GapCause::Overrun,
            libc::ESTRPIPE => GapCause::Suspend,
            _ => GapCause::TransferError,
        };
        self.late = false;
        self.gaps += 1;
        let gap = Gap {
            cause,
            at_frame: self.position,
            frames: None,
            wall_us: pair.wall_us,
            detail: r.detail,
        };
        self.lines.emit(
            "capture_gap",
            &CaptureGap::of(self.record_id, pair.mono_us, &gap),
        );
        self.pending.push_back(Item::Event {
            mono_us: pair.mono_us,
            event: Event::Gap(gap),
        });
        self.queue_pending();
    }

    /// Queue the pending events in order, as far as the queue takes them. A
    /// `writer_backlog` gap is announced when it is queued, which is when its
    /// length is final.
    fn queue_pending(&mut self) {
        while let Some(item) = self.pending.pop_front() {
            let backlog = match &item {
                Item::Event {
                    mono_us,
                    event: Event::Gap(g),
                } if g.cause == GapCause::WriterBacklog => Some((*mono_us, g.clone())),
                _ => None,
            };
            match self.tx.try_send(item) {
                Ok(()) => {
                    if let Some((mono_us, g)) = backlog {
                        self.lines
                            .emit("capture_gap", &CaptureGap::of(self.record_id, mono_us, &g));
                    }
                }
                Err(TrySendError::Full(item)) => {
                    self.pending.push_front(item);
                    return;
                }
                Err(TrySendError::Disconnected(item)) => {
                    self.pending.push_front(item);
                    self.writer_gone = true;
                    return;
                }
            }
        }
    }
}

/// How the writer stopped, when it stopped on its own account or on `end`.
enum WriterEnd {
    /// `end` was written and the record synced.
    Finished,
    /// Free space fell under the floor; `end`/`destination_full` was written.
    DestinationFull { detail: String },
    /// A write, a sync or a free-space reading failed. The record was cut back
    /// to its last whole record and one attempt made to append
    /// `end`/`write_failed`.
    WriteFailed { detail: String, end_written: bool },
    /// The `end` the capture handed over was written, and syncing the record
    /// then failed.
    SyncFailed { detail: String },
}

/// The writer thread: writes each item through the record writer and does
/// nothing else.
struct Writer<C, D: Destination> {
    w: RecordWriter<D>,
    clock: C,
    floor: u64,
    frame_bytes: usize,
    /// The delivered-frame position the record has reached, counted as its
    /// reader counts it.
    position: u64,
}

impl<C: Clock, D: Destination> Writer<C, D> {
    /// `None` when the capture dropped its end of the queue without an `end`:
    /// it has stopped waiting for this thread.
    fn run(mut self, rx: Receiver<Item>) -> Option<WriterEnd> {
        let mut checked: Option<Instant> = None;
        loop {
            if checked.is_none_or(|t| t.elapsed() >= FREE_SPACE_INTERVAL) {
                checked = Some(Instant::now());
                match self.w.get_ref().free_bytes() {
                    Ok(free) if free < self.floor => return Some(self.end_full(free)),
                    Ok(_) => {}
                    Err(e) => {
                        return Some(
                            self.fail(format!("reading the destination's free space failed: {e}")),
                        );
                    }
                }
            }
            let item = match rx.recv_timeout(FREE_SPACE_INTERVAL) {
                Ok(item) => item,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return None,
            };
            match item {
                Item::Block {
                    mono_us,
                    wall_us,
                    first_frame,
                    backlog_frames,
                    pcm,
                } => {
                    if let Err(e) =
                        self.w
                            .block(mono_us, wall_us, first_frame, backlog_frames, &pcm)
                    {
                        return Some(self.fail(format!("writing a block failed: {e}")));
                    }
                    self.position = first_frame + (pcm.len() / self.frame_bytes) as u64;
                }
                Item::Event { mono_us, event } => {
                    if let Err(e) = self.w.event(mono_us, &event) {
                        return Some(self.fail(format!("writing an event failed: {e}")));
                    }
                    match event {
                        Event::Gap(Gap {
                            at_frame,
                            frames: Some(n),
                            ..
                        }) => self.position = at_frame + n,
                        Event::Gap(_) => {}
                        Event::End(_) => {
                            return Some(match self.w.get_ref().sync() {
                                Ok(()) => WriterEnd::Finished,
                                Err(e) => WriterEnd::SyncFailed {
                                    detail: format!("syncing the record failed: {e}"),
                                },
                            });
                        }
                    }
                }
            }
        }
    }

    fn end_full(&mut self, free: u64) -> WriterEnd {
        let detail = format!(
            "{free} bytes free on the destination, under the floor of {} bytes",
            self.floor
        );
        let pair = self.clock.pair();
        let end = Event::End(End {
            reason: EndReason::DestinationFull,
            at_frame: self.position,
            wall_us: pair.wall_us,
            detail: detail.clone(),
        });
        if let Err(e) = self.w.event(pair.mono_us, &end) {
            return self.fail(format!("writing the destination_full end failed: {e}"));
        }
        if let Err(e) = self.w.get_ref().sync() {
            note(format_args!(
                "syncing the record after destination_full failed: {e}"
            ));
        }
        WriterEnd::DestinationFull { detail }
    }

    /// Cut the record back to its last whole record and make one attempt to
    /// end it with `write_failed`.
    fn fail(&mut self, detail: String) -> WriterEnd {
        let end_written = match self.w.truncate_to_last_record() {
            Err(e) => {
                note(format_args!(
                    "cutting the record back after a failed write failed: {e}"
                ));
                false
            }
            Ok(()) => {
                let pair = self.clock.pair();
                let end = Event::End(End {
                    reason: EndReason::WriteFailed,
                    at_frame: self.position,
                    wall_us: pair.wall_us,
                    detail: detail.clone(),
                });
                match self.w.event(pair.mono_us, &end) {
                    Ok(()) => {
                        if let Err(e) = self.w.get_ref().sync() {
                            note(format_args!(
                                "syncing the record after write_failed failed: {e}"
                            ));
                        }
                        true
                    }
                    Err(e) => {
                        note(format_args!("writing the write_failed end failed: {e}"));
                        false
                    }
                }
            }
        };
        WriterEnd::WriteFailed {
            detail,
            end_written,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use capture_record::SampleFormat;

    fn granted(rate_hz: u32, channels: u32, period_frames: u32) -> Granted {
        Granted {
            rate_hz,
            channels,
            sample_format: SampleFormat::S16Le,
            period_frames,
            buffer_frames: period_frames * 16,
        }
    }

    #[test]
    fn the_period_is_twenty_milliseconds_where_it_fits_in_a_block() {
        assert_eq!(period_frames(16_000, 12), Some(320));
        assert_eq!(period_frames(48_000, 8), Some(960));
        assert_eq!(period_frames(50, 2), Some(1));
    }

    #[test]
    fn the_period_is_the_largest_that_fits_where_twenty_milliseconds_does_not() {
        // 192 kHz of 32 channels of s32_le: 3840 frames of 128 bytes is 491520
        // bytes; 511 frames is the most that fit in 65514.
        assert_eq!(period_frames(192_000, 128), Some(511));
        assert_eq!(period_frames(16_000, MAX_BLOCK_PCM_BYTES), Some(1));
    }

    #[test]
    fn no_period_where_no_frame_fits_or_the_rate_gives_none() {
        assert_eq!(period_frames(16_000, MAX_BLOCK_PCM_BYTES + 1), None);
        assert_eq!(period_frames(49, 2), None);
        assert_eq!(period_frames(16_000, 0), None);
    }

    #[test]
    fn the_queue_holds_two_seconds_of_periods() {
        assert_eq!(queue_capacity(&granted(16_000, 6, 320)), 100);
        assert_eq!(queue_capacity(&granted(100, 1, 1000)), 1);
    }

    #[test]
    fn the_floor_is_five_seconds_of_capture_and_a_mebibyte() {
        assert_eq!(
            free_space_floor(&granted(16_000, 6, 320)),
            5 * 16_000 * 12 + (1 << 20)
        );
    }

    #[test]
    fn exit_codes() {
        assert_eq!(Outcome::Stopped.exit_code(), 0);
        assert_eq!(Outcome::Failed.exit_code(), 1);
    }
}
