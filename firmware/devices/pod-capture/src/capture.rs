//! The capture loop and the writer thread it feeds.

use std::collections::VecDeque;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::thread;
use std::time::{Duration, Instant};

use alsa_pcm::{ClockPair, Read, Recovery, Wait};
use audio_pipeline::wide::{LossCause, MAX_WIDE_PCM_BYTES, StreamEndReason};
use frame_log::{
    CaptureGranted, CaptureMeta, Clocks, Device, DeviceControl, HostMicros, LogMeta, WriterInfo,
};

use crate::framer::{Block, End, Event, Framer, Gap, prelude};
use crate::granted::Granted;
use crate::lines::{
    CaptureFailed, CaptureGap, CaptureLate, CaptureStarted, CaptureStopped, CardLine, FailReason,
    Lines, note,
};
use crate::seams::{CaptureSource, Clock, Destination};
use crate::writer::FrameWriter;

/// How long one wait for a period lasts before the stop flag is looked at again.
pub const WAIT_TIMEOUT: Duration = Duration::from_millis(100);

/// Consecutive timed-out waits that end the capture: two seconds with no frame
/// from a device that owes one every period. Only a delivered period restarts
/// the count.
pub const STALL_TIMEOUTS: u32 = 20;

/// From the moment the stream stops, how long the hand-over of the last items
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
    delivered these frames and they were dropped before reaching the log";

/// The period demanded of a device at `rate_hz`: a 20 ms slice, or the largest
/// whole number of frames that fits in one audio frame where that is smaller.
/// `None` when no frame fits in an audio frame, or the rate gives a period of
/// no frames.
pub fn period_frames(rate_hz: u32, frame_bytes: usize) -> Option<u32> {
    if frame_bytes == 0 {
        return None;
    }
    let fits = u32::try_from(MAX_WIDE_PCM_BYTES / frame_bytes).unwrap_or(u32::MAX);
    let period = (rate_hz / 50).min(fits);
    (period > 0).then_some(period)
}

/// Periods the queue holds: two seconds of them, at least one.
pub fn queue_capacity(granted: &Granted) -> usize {
    let periods = QUEUE_SECONDS * u64::from(granted.rate_hz) / u64::from(granted.period_frames);
    usize::try_from(periods).unwrap_or(usize::MAX).max(1)
}

/// Free bytes under which the writer ends the log with `destination_full`:
/// five seconds of capture at the granted parameters plus 1 MiB, so the end
/// still fits.
pub fn free_space_floor(granted: &Granted) -> u64 {
    let per_second = u64::from(granted.rate_hz).saturating_mul(granted.frame_bytes() as u64);
    FLOOR_SECONDS
        .saturating_mul(per_second)
        .saturating_add(FLOOR_EXTRA_BYTES)
}

/// How a capture ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Told to stop; the log ends with `End{stopped}`, synced and closed.
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

/// What a capture records and how it names it: everything its header, hello
/// and `Describe` state but the start pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recording {
    /// 32 lowercase hex digits.
    pub record_id: String,
    /// The hello's `pod_id`: 1 to 32 bytes.
    pub pod_id: String,
    pub writer: WriterInfo,
    pub device: Device,
    pub granted: Granted,
    /// One name per channel, in interleave order.
    pub layout: Vec<String>,
    pub device_control: DeviceControl,
}

impl Recording {
    /// The header metadata, with the start pair's wall reading as its
    /// creation time.
    fn meta(&self, start_wall_us: u64) -> LogMeta {
        LogMeta {
            build_id: self.writer.build.commit.clone(),
            created_epoch_us: HostMicros(start_wall_us),
            conn_seq: 0,
            rolled_from: None,
            capture: Some(CaptureMeta {
                record_id: self.record_id.clone(),
                writer: self.writer.clone(),
                device: self.device.clone(),
                granted: CaptureGranted {
                    period_frames: self.granted.period_frames,
                    buffer_frames: self.granted.buffer_frames,
                },
                clocks: Clocks::v1(),
                device_control: self.device_control.clone(),
            }),
        }
    }
}

/// Record the stream from `source` into `dest` until `stop` is set, the stream
/// dies or stalls, or the destination fails or runs low.
///
/// The stream is triggered here and the start pair taken directly after; the
/// header, the hello and the `Describe` carrying it are written in one write.
/// Every outcome is one terminal line on `lines`: `capture_stopped`, or
/// `capture_failed`. `stop_bound` is [`STOP_BOUND`] outside tests.
#[allow(clippy::too_many_arguments)]
pub fn run<S, C, D, W>(
    source: &mut S,
    clock: C,
    dest: D,
    rec: Recording,
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
    let record_id = rec.record_id.clone();
    // Until the header is written no log exists, so the lines below name none
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
    let opened = prelude(
        &rec.pod_id,
        &rec.granted,
        &rec.layout,
        start.mono_us,
        start.wall_us,
    )
    .map_err(|e| (FailReason::Params, e))
    .and_then(|prelude| {
        FrameWriter::new(
            dest,
            &rec.meta(start.wall_us),
            &prelude,
            Framer::new(&rec.granted),
        )
        .map_err(|e| {
            (
                FailReason::Destination,
                format!("writing the log's header failed: {e}"),
            )
        })
    });
    let w = match opened {
        Ok(w) => w,
        Err((reason, detail)) => {
            if let Err(e) = source.stop() {
                note(format_args!("{e}"));
            }
            failed(lines, None, reason, &detail, None, clock.pair(), None);
            return Outcome::Failed;
        }
    };
    lines.emit(
        "capture_started",
        &CaptureStarted {
            record_id: &record_id,
            path,
            device: &rec.device,
            granted: &rec.granted,
            layout: &rec.layout,
            build: &rec.writer.build,
            frame: 0,
            mono_us: start.mono_us,
            wall_us: start.wall_us,
        },
    );

    let granted = &rec.granted;
    let (tx, rx) = mpsc::sync_channel(queue_capacity(granted));
    let (done_tx, done_rx) = mpsc::channel();
    let writer = Writer {
        w,
        clock: clock.clone(),
        floor: free_space_floor(granted),
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
        Ending::Stopped => Some((StreamEndReason::Stopped, "told to stop".to_owned())),
        Ending::StreamDied(detail) => Some((StreamEndReason::StreamDied, detail.clone())),
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
            "the writer finished before it was handed an end".to_owned(),
        ),
        (Ok(WriterEnd::DestinationFull { detail }), _) => (
            FailReason::DestinationFull,
            format!("{detail}; the log ends with End{{destination_full}}"),
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
                format!("{detail}; the log ends with End{{write_failed}}")
            } else {
                format!("{detail}; the log has no end")
            },
        ),
        (Ok(WriterEnd::SyncFailed { detail }), _) => (
            FailReason::WriteFailed,
            format!(
                "{detail}; the log holds its end, which may not have reached the storage \
                 device"
            ),
        ),
        (Err(RecvTimeoutError::Timeout), _) => (
            FailReason::WriteStalled,
            format!(
                "the writer had not reported {} ms after the stream stopped; it may be \
                 held on a write or on the sync after its end, so whether the log ends \
                 is for its reader to say",
                stop_bound.as_millis()
            ),
        ),
        (Err(RecvTimeoutError::Disconnected), _) => (
            FailReason::WriteFailed,
            "the writer thread ended without reporting; the log may have no end".to_owned(),
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
    /// The writer ended the log itself (floor or failed write).
    WriterEnded,
}

/// What the loop hands the writer.
enum Item {
    Block(Block),
    Event { mono_us: u64, event: Event },
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
    /// Inside a run of late blocks queued for the log, as the reader counts
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
            let block = Item::Block(Block {
                mono_us: pair.mono_us,
                wall_us: pair.wall_us,
                first_frame,
                backlog_frames,
                pcm,
            });
            match self.tx.try_send(block) {
                Ok(()) => {
                    // A late run starts at a block the log holds.
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
                    cause: LossCause::WriterBacklog,
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
                cause: LossCause::WriterBacklog,
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
            libc::EPIPE => LossCause::Overrun,
            libc::ESTRPIPE => LossCause::Suspend,
            _ => LossCause::TransferError,
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
                } if g.cause == LossCause::WriterBacklog => Some((*mono_us, g.clone())),
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
    /// The end was written and the log synced.
    Finished,
    /// Free space fell under the floor; `End{destination_full}` was written.
    DestinationFull { detail: String },
    /// A write, a sync or a free-space reading failed. The log was cut back to
    /// its last whole item and one attempt made to append `End{write_failed}`.
    WriteFailed { detail: String, end_written: bool },
    /// The end the capture handed over was written, and syncing the log then
    /// failed.
    SyncFailed { detail: String },
}

/// The writer thread: writes each item through the frame writer and does
/// nothing else.
struct Writer<C, D: Destination> {
    w: FrameWriter<D>,
    clock: C,
    floor: u64,
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
                Item::Block(block) => {
                    if let Err(e) = self.w.block(block) {
                        return Some(self.fail(format!("writing a block failed: {e}")));
                    }
                }
                Item::Event { mono_us, event } => {
                    let is_end = matches!(event, Event::End(_));
                    if let Err(e) = self.w.event(mono_us, event) {
                        return Some(self.fail(format!("writing an event failed: {e}")));
                    }
                    if is_end {
                        return Some(match self.w.get_ref().sync() {
                            Ok(()) => WriterEnd::Finished,
                            Err(e) => WriterEnd::SyncFailed {
                                detail: format!("syncing the log failed: {e}"),
                            },
                        });
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
            reason: StreamEndReason::DestinationFull,
            at_frame: self.w.position(),
            wall_us: pair.wall_us,
            detail: detail.clone(),
        });
        if let Err(e) = self.w.event(pair.mono_us, end) {
            return self.fail(format!("writing the destination_full end failed: {e}"));
        }
        if let Err(e) = self.w.get_ref().sync() {
            note(format_args!(
                "syncing the log after destination_full failed: {e}"
            ));
        }
        WriterEnd::DestinationFull { detail }
    }

    /// Cut the log back to its last whole item and make one attempt to end it
    /// with `write_failed`.
    fn fail(&mut self, detail: String) -> WriterEnd {
        let end_written = match self.w.truncate_to_last_item() {
            Err(e) => {
                note(format_args!(
                    "cutting the log back after a failed write failed: {e}"
                ));
                false
            }
            Ok(()) => {
                let pair = self.clock.pair();
                let end = Event::End(End {
                    reason: StreamEndReason::WriteFailed,
                    at_frame: self.w.position(),
                    wall_us: pair.wall_us,
                    detail: detail.clone(),
                });
                match self.w.event(pair.mono_us, end) {
                    Ok(()) => {
                        if let Err(e) = self.w.get_ref().sync() {
                            note(format_args!(
                                "syncing the log after write_failed failed: {e}"
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
    use crate::granted::SampleFormat;

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
        // bytes; 511 frames is the most that fit in 65504.
        assert_eq!(period_frames(192_000, 128), Some(511));
        assert_eq!(period_frames(16_000, MAX_WIDE_PCM_BYTES), Some(1));
        assert_eq!(period_frames(16_000, 2), Some(320));
        assert_eq!(period_frames(4_000_000, 2), Some(32_752));
    }

    #[test]
    fn no_period_where_no_frame_fits_or_the_rate_gives_none() {
        assert_eq!(period_frames(16_000, MAX_WIDE_PCM_BYTES + 1), None);
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
