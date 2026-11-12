//! The capture loop through its seams: a scripted source, a stepped clock and an
//! in-memory destination that can block, refuse or tear writes and run out of
//! space. Every log is read back with `frame-log`'s walker and summary, and
//! every summary is held to what a continuous capture writes: no audio unsent
//! or resent between segments, no break inside one, and no tail but `ended`,
//! `unterminated` or `torn`.

use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use alsa_pcm::{ClockPair, PcmError, Period, Read, Recovery, Wait};
use audio_pipeline::wide::{LossCause, StreamEndReason};
use audio_pipeline::wire::{ChannelSource, EndReason};
use frame_log::{
    BuildSource, Cause, Device, DeviceControl, Step as LogStep, Summary, WriterBuild, WriterInfo,
    summarize, walk,
};
use pod_capture::lines::Lines;
use pod_capture::{
    CaptureSource, Clock, Destination, Granted, Outcome, Recording, STALL_TIMEOUTS, SampleFormat,
    Truncate, run,
};
use serde_json::Value;

const RATE: u32 = 1000;
const PERIOD: u32 = 20;
const RECORD_ID: &str = "0123456789abcdef0123456789abcdef";
const BOUND: Duration = Duration::from_millis(400);

enum Step {
    /// A read that delivers `frames` with `backlog` frames waiting behind it.
    Period {
        frames: usize,
        backlog: u32,
    },
    /// A read that fails and is recovered with `errno`.
    ReadRecovered(i32),
    /// A wait that fails and is recovered with `errno`.
    WaitRecovered(i32),
    TimedOut,
    /// A read that fails past recovery.
    Die,
    /// First in the script: the trigger fails.
    StartFails,
    /// Run between turns, invisible to the loop.
    Do(Box<dyn FnMut() + Send>),
}

fn periods(n: usize) -> impl Iterator<Item = Step> {
    (0..n).map(|_| Step::Period {
        frames: PERIOD as usize,
        backlog: 0,
    })
}

fn act(f: impl FnMut() + Send + 'static) -> Step {
    Step::Do(Box::new(f))
}

/// Delivers the steps in order, then sets the stop flag. Every delivered byte
/// is its index in the delivered stream modulo 251, so a block's bytes say
/// which frames they are.
struct Script {
    steps: VecDeque<Step>,
    ready: Option<Step>,
    stop: Arc<AtomicBool>,
    frame_bytes: usize,
    delivered_bytes: usize,
    buf: Vec<u8>,
    started: bool,
    stopped: bool,
}

impl Script {
    fn new(steps: Vec<Step>, frame_bytes: usize, stop: Arc<AtomicBool>) -> Self {
        Script {
            steps: steps.into(),
            ready: None,
            stop,
            frame_bytes,
            delivered_bytes: 0,
            buf: Vec::new(),
            started: false,
            stopped: false,
        }
    }
}

fn recovery(errno: i32) -> Recovery {
    Recovery {
        errno,
        detail: format!("scripted failure, errno {errno}"),
    }
}

impl CaptureSource for Script {
    fn start(&mut self) -> Result<(), PcmError> {
        if matches!(self.steps.front(), Some(Step::StartFails)) {
            self.steps.pop_front();
            return Err(PcmError::Stream {
                reason: "scripted: the stream would not start".to_owned(),
            });
        }
        self.started = true;
        Ok(())
    }

    fn wait(&mut self, _timeout: Duration) -> Result<Wait, PcmError> {
        assert!(self.started, "waited on a stream that was never started");
        loop {
            match self.steps.pop_front() {
                None => {
                    self.stop.store(true, Ordering::SeqCst);
                    return Ok(Wait::TimedOut);
                }
                Some(Step::Do(mut f)) => f(),
                Some(Step::TimedOut) => return Ok(Wait::TimedOut),
                Some(Step::WaitRecovered(errno)) => return Ok(Wait::Recovered(recovery(errno))),
                Some(step) => {
                    self.ready = Some(step);
                    return Ok(Wait::Ready);
                }
            }
        }
    }

    fn read(&mut self) -> Result<Read<'_>, PcmError> {
        match self.ready.take().expect("read without a Ready wait") {
            Step::Period { frames, backlog } => {
                let n = frames * self.frame_bytes;
                self.buf = (self.delivered_bytes..self.delivered_bytes + n)
                    .map(|i| (i % 251) as u8)
                    .collect();
                self.delivered_bytes += n;
                Ok(Read::Period(Period {
                    bytes: &self.buf,
                    frames,
                    backlog_frames: backlog,
                }))
            }
            Step::ReadRecovered(errno) => Ok(Read::Recovered(recovery(errno))),
            Step::Die => Err(PcmError::Stream {
                reason: "scripted: recovery failed".to_owned(),
            }),
            _ => unreachable!("only reads are made ready"),
        }
    }

    fn stop(&mut self) -> Result<(), PcmError> {
        self.stopped = true;
        Ok(())
    }
}

fn expected_bytes(first_frame: u64, frames: u64, frame_bytes: usize) -> Vec<u8> {
    let start = first_frame as usize * frame_bytes;
    (start..start + frames as usize * frame_bytes)
        .map(|i| (i % 251) as u8)
        .collect()
}

/// Monotonic time advances 1 ms per reading; wall time is monotonic time plus
/// an offset a step can move.
#[derive(Clone)]
struct StepClock {
    mono: Arc<AtomicU64>,
    offset: Arc<AtomicU64>,
}

const MONO_START: u64 = 5_000_000;
const WALL_OFFSET: u64 = 1_794_000_000_000_000;

impl StepClock {
    fn new() -> Self {
        StepClock {
            mono: Arc::new(AtomicU64::new(MONO_START)),
            offset: Arc::new(AtomicU64::new(WALL_OFFSET)),
        }
    }
}

impl Clock for StepClock {
    fn pair(&self) -> ClockPair {
        let mono_us = self.mono.fetch_add(1000, Ordering::SeqCst);
        ClockPair {
            mono_us,
            wall_us: mono_us + self.offset.load(Ordering::SeqCst),
        }
    }
}

#[derive(Default)]
struct DestState {
    data: Mutex<Vec<u8>>,
    /// Writes block while this is true.
    closed: Mutex<bool>,
    opened: Condvar,
    /// Writes waiting on the gate now.
    blocked: AtomicU32,
    /// Write calls that succeeded: the header, then one per record.
    writes: AtomicU32,
    /// Write calls still to refuse; `u32::MAX` refuses every one.
    refuse: AtomicU32,
    /// A refused write first lands half its bytes.
    tear: AtomicBool,
    free: AtomicU64,
    /// Every sync fails while this is set.
    sync_fails: AtomicBool,
    /// Every free-space reading fails while this is set.
    free_fails: AtomicBool,
    /// Every sync waits while this is set.
    sync_held: AtomicBool,
}

#[derive(Clone)]
struct MemDest(Arc<DestState>);

impl MemDest {
    fn new() -> Self {
        let state = DestState::default();
        state.free.store(u64::MAX, Ordering::SeqCst);
        MemDest(Arc::new(state))
    }

    fn close(&self) {
        *self.0.closed.lock().unwrap() = true;
    }

    /// Wait until a write is held at the closed gate.
    fn until_blocked(&self) {
        while self.0.blocked.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Wait until `n` writes have succeeded.
    fn until_writes(&self, n: u32) {
        while self.0.writes.load(Ordering::SeqCst) < n {
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn open(&self) {
        *self.0.closed.lock().unwrap() = false;
        self.0.opened.notify_all();
    }

    fn bytes(&self) -> Vec<u8> {
        self.0.data.lock().unwrap().clone()
    }
}

impl Write for MemDest {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut closed = self.0.closed.lock().unwrap();
        while *closed {
            self.0.blocked.fetch_add(1, Ordering::SeqCst);
            closed = self.0.opened.wait(closed).unwrap();
            self.0.blocked.fetch_sub(1, Ordering::SeqCst);
        }
        drop(closed);
        let refuse = self.0.refuse.load(Ordering::SeqCst);
        if refuse > 0 {
            if refuse != u32::MAX {
                self.0.refuse.store(refuse - 1, Ordering::SeqCst);
            }
            if self.0.tear.load(Ordering::SeqCst) {
                self.0
                    .data
                    .lock()
                    .unwrap()
                    .extend_from_slice(&buf[..buf.len() / 2]);
            }
            return Err(io::Error::other("scripted: no space left on device"));
        }
        self.0.data.lock().unwrap().extend_from_slice(buf);
        self.0.writes.fetch_add(1, Ordering::SeqCst);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Truncate for MemDest {
    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.0.data.lock().unwrap().truncate(len as usize);
        Ok(())
    }
}

impl Destination for MemDest {
    fn free_bytes(&self) -> io::Result<u64> {
        if self.0.free_fails.load(Ordering::SeqCst) {
            return Err(io::Error::other("scripted: the file system is gone"));
        }
        Ok(self.0.free.load(Ordering::SeqCst))
    }

    fn sync(&self) -> io::Result<()> {
        while self.0.sync_held.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(1));
        }
        if self.0.sync_fails.load(Ordering::SeqCst) {
            return Err(io::Error::other("scripted: sync failed"));
        }
        Ok(())
    }
}

fn recording(channels: u32, format: SampleFormat, period_frames: u32) -> Recording {
    Recording {
        record_id: RECORD_ID.to_owned(),
        pod_id: "bench".to_owned(),
        writer: WriterInfo {
            program: "pod-capture".to_owned(),
            build: WriterBuild {
                commit: "13a513d6b38a14194ecb2e1853a7a55d9e7f7dab".to_owned(),
                dirty: Some(false),
                source: BuildSource::Git,
            },
        },
        device: Device {
            selector: "Flex".to_owned(),
            card_index: 2,
            card_name: "reSpeaker Flex".to_owned(),
            card_long_name: "Seeed Studio reSpeaker Flex at usb-1".to_owned(),
            pcm: "hw:2,0".to_owned(),
            usb_id: Some("2886:0022".to_owned()),
        },
        granted: Granted {
            rate_hz: RATE,
            channels,
            sample_format: format,
            period_frames,
            buffer_frames: period_frames * 16,
        },
        layout: (0..channels).map(|c| format!("ch{c}")).collect(),
        device_control: DeviceControl { performed: false },
    }
}

struct Ran {
    outcome: Outcome,
    lines: Vec<Value>,
    record: Vec<u8>,
    script: Script,
    frame_bytes: usize,
    elapsed: Duration,
}

impl Ran {
    fn events(&self) -> Vec<&str> {
        self.lines
            .iter()
            .map(|l| l["event"].as_str().unwrap())
            .collect()
    }

    fn line(&self, event: &str) -> &Value {
        let found: Vec<&Value> = self.lines.iter().filter(|l| l["event"] == event).collect();
        assert_eq!(found.len(), 1, "one {event} line in {:?}", self.events());
        found[0]
    }

    fn file(&self) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&self.record).unwrap();
        f
    }

    fn summary(&self) -> Summary {
        summary_of(&self.record)
    }

    /// Every step the walker yields, then the tail's verdict.
    fn steps(&self) -> (Vec<LogStep>, &'static str) {
        let f = self.file();
        let mut w = walk(f.path()).unwrap();
        let steps: Vec<LogStep> = w.by_ref().map(|s| s.unwrap()).collect();
        (steps, w.tail().unwrap().verdict.name())
    }

    /// Every audio frame's bytes are the frames its index names.
    fn assert_blocks_are_the_delivered_frames(&self) {
        let (steps, _) = self.steps();
        for step in &steps {
            if let LogStep::Audio(a) = step {
                assert_eq!(
                    a.pcm,
                    expected_bytes(a.first_frame, a.frames, self.frame_bytes),
                    "audio at {}",
                    a.first_frame
                );
            }
        }
    }
}

/// The summary of `bytes`, held to what this recorder writes.
fn summary_of(bytes: &[u8]) -> Summary {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(bytes).unwrap();
    let s = summarize(f.path()).unwrap();
    assert!(
        matches!(s.tail.verdict, "ended" | "unterminated" | "torn"),
        "{:?}",
        s.tail
    );
    assert_eq!(s.hello.channel_source, ChannelSource::Described);
    assert_eq!(s.hello.pod_id.as_str(), "bench");
    for seg in &s.segments {
        assert_eq!((seg.not_sent_frames, seg.resent_frames), (0, 0), "{seg:?}");
        assert!(seg.count_check.is_none_or(|c| c == "match"), "{seg:?}");
    }
    for d in &s.discontinuities {
        assert_eq!(d.kind, "loss", "only a Loss frame marks a loss: {d:?}");
    }
    s
}

fn run_script(
    channels: u32,
    format: SampleFormat,
    steps: Vec<Step>,
    dest: MemDest,
    clock: StepClock,
) -> Ran {
    run_recording(recording(channels, format, PERIOD), steps, dest, clock)
}

fn run_recording(rec: Recording, steps: Vec<Step>, dest: MemDest, clock: StepClock) -> Ran {
    let frame_bytes = rec.granted.frame_bytes();
    let stop = Arc::new(AtomicBool::new(false));
    let mut script = Script::new(steps, frame_bytes, stop.clone());
    let mut lines = Lines::new(Vec::new());
    let t0 = Instant::now();
    let outcome = run(
        &mut script,
        clock,
        dest.clone(),
        rec,
        "/records/test.framelog",
        &stop,
        &mut lines,
        BOUND,
    );
    let elapsed = t0.elapsed();
    let lines = String::from_utf8(lines.into_inner())
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    Ran {
        outcome,
        lines,
        record: dest.bytes(),
        script,
        frame_bytes,
        elapsed,
    }
}

fn run_steps(steps: Vec<Step>) -> Ran {
    run_script(
        6,
        SampleFormat::S16Le,
        steps,
        MemDest::new(),
        StepClock::new(),
    )
}

/// Every loss: its cause, where and how long.
fn gaps(s: &Summary) -> Vec<(LossCause, u64, Option<u64>)> {
    s.discontinuities
        .iter()
        .map(|d| match d.cause {
            Some(Cause::Loss(c)) => (c, d.at_frame, d.frames),
            other => panic!("a loss with cause {other:?}"),
        })
        .collect()
}

fn end_reason(s: &Summary) -> Option<StreamEndReason> {
    s.end.as_ref().map(|e| e.reason)
}

/// Each segment's base, frames and end.
fn segments(s: &Summary) -> Vec<(u64, u64, Option<EndReason>)> {
    s.segments
        .iter()
        .map(|g| (g.base_frame, g.frames, g.end))
        .collect()
}

fn every_frame_and_a_clean_stop(channels: u32, format: SampleFormat) {
    let ran = run_script(
        channels,
        format,
        periods(50).collect(),
        MemDest::new(),
        StepClock::new(),
    );
    assert_eq!(ran.outcome, Outcome::Stopped);
    assert!(ran.script.stopped, "the stream is stopped");
    assert_eq!(ran.events(), ["capture_started", "capture_stopped"]);
    let started = ran.line("capture_started");
    let stopped = ran.line("capture_stopped");
    assert_eq!(started["record_id"], RECORD_ID);
    assert_eq!(stopped["record_id"], RECORD_ID);
    assert_eq!(started["frame"], 0);
    assert_eq!(started["path"], "/records/test.framelog");
    assert_eq!(started["granted"]["channels"], channels);
    assert_eq!(started["device"]["usb_id"], "2886:0022");
    assert_eq!(started["build"]["source"], "git");
    assert_eq!(stopped["frames"], 50 * PERIOD);
    assert_eq!(stopped["gaps"], 0);

    let s = ran.summary();
    assert_eq!(s.record_id.as_deref(), Some(RECORD_ID));
    assert_eq!(s.header["build_id"], started["build"]["commit"]);
    assert_eq!(s.header["created_epoch_us"], started["wall_us"]);
    assert_eq!(s.header["capture"]["device"], started["device"]);
    assert_eq!(s.header["capture"]["granted"]["period_frames"], PERIOD);
    assert_eq!(s.hello.sample_rate_hz, RATE);
    assert_eq!(u32::from(s.hello.channels), channels);
    assert_eq!(s.hello.codec, format.wire().1);
    assert_eq!(s.hello.bits_per_sample, format.wire().0);
    let capture = s.capture.as_ref().unwrap();
    assert_eq!(
        capture.channel_names,
        started["layout"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n.as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    );
    assert_eq!(capture.period_frames, PERIOD);
    assert_eq!(capture.started.mono_us, started["mono_us"]);
    assert_eq!(capture.started.wall_us, started["wall_us"]);
    assert_eq!(s.frames_delivered, u64::from(50 * PERIOD));
    assert_eq!(s.frames_recorded, u64::from(50 * PERIOD));
    assert_eq!(
        segments(&s),
        [(0, u64::from(50 * PERIOD), Some(EndReason::StreamEnd))]
    );
    assert_eq!(s.blocks, Some(50));
    assert!(s.discontinuities.is_empty());
    assert_eq!(s.wall_steps.as_deref(), Some(&[][..]));
    assert_eq!(end_reason(&s), Some(StreamEndReason::Stopped));
    assert_eq!(s.end.as_ref().unwrap().at_frame, u64::from(50 * PERIOD));
    assert_eq!(s.tail.verdict, "ended");
    ran.assert_blocks_are_the_delivered_frames();
}

#[test]
fn six_channel_s16_records_every_frame_and_stops_cleanly() {
    every_frame_and_a_clean_stop(6, SampleFormat::S16Le);
}

#[test]
fn two_channel_s32_records_every_frame_and_stops_cleanly() {
    every_frame_and_a_clean_stop(2, SampleFormat::S32Le);
}

#[test]
fn an_overrun_is_an_unmeasured_gap_at_its_position() {
    let steps = periods(5)
        .chain([Step::ReadRecovered(libc::EPIPE)])
        .chain(periods(5))
        .collect();
    let ran = run_steps(steps);
    assert_eq!(ran.outcome, Outcome::Stopped);
    let s = ran.summary();
    assert_eq!(gaps(&s), [(LossCause::Overrun, 100, None)]);
    assert!(s.discontinuities[0].detail.contains("errno"));
    // The segment closes at the loss and the next opens where it happened:
    // the index does not advance over audio nothing counted.
    assert_eq!(
        segments(&s),
        [
            (0, 100, Some(EndReason::SourceLoss)),
            (100, 100, Some(EndReason::StreamEnd))
        ]
    );
    assert_eq!(s.frames_delivered, 200);
    let gap = ran.line("capture_gap");
    assert_eq!(gap["cause"], "overrun");
    assert_eq!(gap["at_frame"], 100);
    assert!(gap["frames"].is_null());
    assert_eq!(ran.line("capture_stopped")["gaps"], 1);
    ran.assert_blocks_are_the_delivered_frames();
}

#[test]
fn each_errno_maps_to_its_cause() {
    let steps = periods(1)
        .chain([Step::ReadRecovered(libc::ESTRPIPE)])
        .chain(periods(1))
        .chain([Step::ReadRecovered(libc::EIO)])
        .chain(periods(1))
        .collect();
    let s = run_steps(steps).summary();
    assert_eq!(
        gaps(&s),
        [
            (LossCause::Suspend, 20, None),
            (LossCause::TransferError, 40, None)
        ]
    );
}

#[test]
fn a_recovery_from_wait_is_a_gap_like_one_from_read() {
    let via = |step: Step| {
        let ran = run_steps(periods(3).chain([step]).chain(periods(2)).collect());
        let s = ran.summary();
        (
            gaps(&s),
            s.frames_delivered,
            ran.line("capture_gap")["at_frame"].clone(),
        )
    };
    let from_wait = via(Step::WaitRecovered(libc::EPIPE));
    assert_eq!(from_wait.0, [(LossCause::Overrun, 60, None)]);
    assert_eq!(from_wait, via(Step::ReadRecovered(libc::EPIPE)));
}

#[test]
fn a_short_read_is_a_short_block_and_no_gap() {
    let steps = periods(2)
        .chain([Step::Period {
            frames: 7,
            backlog: 0,
        }])
        .chain(periods(2))
        .collect();
    let ran = run_steps(steps);
    let s = ran.summary();
    assert!(s.discontinuities.is_empty());
    let short = s.short_blocks.as_ref().unwrap();
    assert_eq!(short.len(), 1);
    assert_eq!(short[0].first_frame, 40);
    assert_eq!(short[0].frames, 7);
    assert_eq!(s.frames_delivered, 87);
    ran.assert_blocks_are_the_delivered_frames();
}

/// Close the destination and deliver one period, then wait until the writer
/// is holding that block at the gate. The queue's two seconds (100 periods at
/// these parameters) then fill from the next period on, so the first dropped
/// block is period 101, at frame 2020.
fn held_writer(dest: &MemDest) -> impl Iterator<Item = Step> {
    let (close, held) = (dest.clone(), dest.clone());
    [act(move || close.close())]
        .into_iter()
        .chain(periods(1))
        .chain([act(move || held.until_blocked())])
}

#[test]
fn a_blocked_destination_drops_blocks_into_one_exact_backlog_gap() {
    let dest = MemDest::new();
    let open = dest.clone();
    let steps = held_writer(&dest)
        .chain(periods(149))
        .chain([act(move || {
            open.open();
            // The header, the held block and the hundred queued behind it.
            open.until_writes(102);
        })])
        .chain(periods(5))
        .collect();
    let ran = run_script(6, SampleFormat::S16Le, steps, dest, StepClock::new());
    assert_eq!(ran.outcome, Outcome::Stopped);
    let s = ran.summary();
    assert_eq!(gaps(&s), [(LossCause::WriterBacklog, 2020, Some(49 * 20))]);
    let gap = &s.discontinuities[0];
    let dropped = gap.frames.unwrap();
    assert_eq!(s.frames_delivered, 155 * u64::from(PERIOD));
    assert_eq!(s.frames_recorded + dropped, s.frames_delivered);
    // A measured loss is a jump of the index inside the one segment.
    assert_eq!(gap.segment_id, Some(0));
    assert_eq!(s.segments.len(), 1);
    let (steps, verdict) = ran.steps();
    assert_eq!(verdict, "ended");
    let after = steps
        .iter()
        .skip_while(|i| !matches!(i, LogStep::Loss { .. }))
        .find_map(|i| match i {
            LogStep::Audio(a) => Some((a.first_frame, a.jump)),
            _ => None,
        })
        .unwrap();
    let after = match after {
        (first_frame, None) => first_frame,
        (_, jump) => panic!("the Loss frame accounts for the jump: {jump:?}"),
    };
    assert_eq!(after, gap.at_frame + dropped);
    let line = ran.line("capture_gap");
    assert_eq!(line["cause"], "writer_backlog");
    assert_eq!(line["frames"], dropped);
    assert_eq!(line["at_frame"], gap.at_frame);
    ran.assert_blocks_are_the_delivered_frames();
}

#[test]
fn an_overrun_behind_a_full_queue_stays_in_order_between_backlog_gaps() {
    let dest = MemDest::new();
    let open = dest.clone();
    let steps = held_writer(&dest)
        .chain(periods(149))
        .chain([Step::ReadRecovered(libc::EPIPE)])
        .chain(periods(50))
        .chain([act(move || open.open())])
        .collect();
    let ran = run_script(6, SampleFormat::S16Le, steps, dest, StepClock::new());
    assert_eq!(ran.outcome, Outcome::Stopped);
    let s = ran.summary();
    // The overrun sits where the first backlog gap ends, and the second starts
    // there: the position did not move across the overrun.
    assert_eq!(
        gaps(&s),
        [
            (LossCause::WriterBacklog, 2020, Some(49 * 20)),
            (LossCause::Overrun, 3000, None),
            (LossCause::WriterBacklog, 3000, Some(50 * 20)),
        ]
    );
    // The overrun closes the one segment; the backlog after it has none open.
    assert_eq!(
        segments(&s),
        [(0, 101 * u64::from(PERIOD), Some(EndReason::SourceLoss))]
    );
    assert_eq!(s.discontinuities[2].segment_id, None);
    assert_eq!(end_reason(&s), Some(StreamEndReason::Stopped));
    assert_eq!(s.end.as_ref().unwrap().at_frame, 200 * u64::from(PERIOD));
    assert_eq!(s.tail.verdict, "ended");
    assert_eq!(ran.line("capture_stopped")["gaps"], 3);
    let gap_lines: Vec<&str> = ran
        .lines
        .iter()
        .filter(|l| l["event"] == "capture_gap")
        .map(|l| l["cause"].as_str().unwrap())
        .collect();
    // The overrun is announced when it happens; each backlog gap when it is
    // queued, at the stop.
    assert_eq!(gap_lines, ["overrun", "writer_backlog", "writer_backlog"]);
}

#[test]
fn twenty_timeouts_in_a_row_end_the_stream() {
    let steps = periods(3)
        .chain((0..STALL_TIMEOUTS).map(|_| Step::TimedOut))
        .chain(periods(3))
        .collect();
    let ran = run_steps(steps);
    assert_eq!(ran.outcome, Outcome::Failed);
    let failed = ran.line("capture_failed");
    assert_eq!(failed["reason"], "stream_died");
    assert!(
        failed["detail"]
            .as_str()
            .unwrap()
            .contains("delivered nothing"),
        "{failed}"
    );
    assert_eq!(failed["at_frame"], 60);
    let s = ran.summary();
    assert_eq!(end_reason(&s), Some(StreamEndReason::StreamDied));
    assert_eq!(s.frames_delivered, 60);
    assert_eq!(s.tail.verdict, "ended");
}

#[test]
fn a_delivered_period_restarts_the_timeout_count() {
    let steps = (0..STALL_TIMEOUTS - 1)
        .map(|_| Step::TimedOut)
        .chain(periods(1))
        .chain((0..STALL_TIMEOUTS - 1).map(|_| Step::TimedOut))
        .chain(periods(1))
        .collect();
    let ran = run_steps(steps);
    assert_eq!(ran.outcome, Outcome::Stopped);
    assert_eq!(end_reason(&ran.summary()), Some(StreamEndReason::Stopped));
}

#[test]
fn a_recovery_does_not_restart_the_timeout_count() {
    let steps = (0..STALL_TIMEOUTS - 1)
        .map(|_| Step::TimedOut)
        .chain([Step::WaitRecovered(libc::EPIPE), Step::TimedOut])
        .chain(periods(1))
        .collect();
    let ran = run_steps(steps);
    assert_eq!(ran.outcome, Outcome::Failed);
    assert_eq!(ran.line("capture_failed")["reason"], "stream_died");
}

#[test]
fn a_stream_that_dies_ends_the_record_with_stream_died() {
    let ran = run_steps(periods(4).chain([Step::Die]).chain(periods(4)).collect());
    assert_eq!(ran.outcome, Outcome::Failed);
    let failed = ran.line("capture_failed");
    assert_eq!(failed["reason"], "stream_died");
    assert!(
        failed["detail"]
            .as_str()
            .unwrap()
            .contains("recovery failed")
    );
    let s = ran.summary();
    assert_eq!(end_reason(&s), Some(StreamEndReason::StreamDied));
    assert_eq!(s.frames_delivered, 80);
    assert_eq!(s.tail.verdict, "ended");
    assert!(ran.script.stopped);
}

#[test]
fn a_refused_write_ends_the_log_at_the_last_whole_block() {
    let dest = MemDest::new();
    let refuse = dest.clone();
    let steps = periods(5)
        .chain([act(move || {
            // The header and five blocks: the refusal meets the next block,
            // which lands half its bytes before failing.
            refuse.until_writes(6);
            refuse.0.tear.store(true, Ordering::SeqCst);
            refuse.0.refuse.store(1, Ordering::SeqCst);
        })])
        .chain(periods(5))
        .collect();
    let ran = run_script(6, SampleFormat::S16Le, steps, dest, StepClock::new());
    assert_eq!(ran.outcome, Outcome::Failed);
    let failed = ran.line("capture_failed");
    assert_eq!(failed["reason"], "write_failed");
    let detail = failed["detail"].as_str().unwrap();
    assert!(detail.contains("no space left"), "{detail}");
    assert!(detail.contains("ends with End{write_failed}"), "{detail}");
    let s = ran.summary();
    assert_eq!(s.blocks, Some(5));
    assert_eq!(end_reason(&s), Some(StreamEndReason::WriteFailed));
    assert_eq!(s.end.as_ref().unwrap().at_frame, 100);
    assert_eq!(segments(&s), [(0, 100, Some(EndReason::StreamEnd))]);
    assert_eq!(s.tail.verdict, "ended");
}

#[test]
fn a_refused_end_too_leaves_the_log_unterminated() {
    let dest = MemDest::new();
    let refuse = dest.clone();
    let steps = periods(5)
        .chain([act(move || {
            refuse.until_writes(6);
            refuse.0.refuse.store(u32::MAX, Ordering::SeqCst);
        })])
        .chain(periods(5))
        .collect();
    let ran = run_script(6, SampleFormat::S16Le, steps, dest, StepClock::new());
    assert_eq!(ran.outcome, Outcome::Failed);
    let failed = ran.line("capture_failed");
    assert_eq!(failed["reason"], "write_failed");
    assert!(failed["detail"].as_str().unwrap().contains("has no end"));
    let s = ran.summary();
    assert_eq!(s.blocks, Some(5));
    assert_eq!(s.end, None);
    assert_eq!(s.tail.verdict, "unterminated");
    assert_eq!(s.tail.open_segment, Some(0));
}

#[test]
fn a_destination_under_the_floor_ends_with_destination_full() {
    let dest = MemDest::new();
    dest.0.free.store(1 << 20, Ordering::SeqCst);
    let ran = run_script(
        6,
        SampleFormat::S16Le,
        periods(10).collect(),
        dest,
        StepClock::new(),
    );
    assert_eq!(ran.outcome, Outcome::Failed);
    let failed = ran.line("capture_failed");
    assert_eq!(failed["reason"], "destination_full");
    let s = ran.summary();
    assert_eq!(end_reason(&s), Some(StreamEndReason::DestinationFull));
    assert!(s.end.as_ref().unwrap().detail.contains("under the floor"));
    assert_eq!(s.tail.verdict, "ended");
}

#[test]
fn a_destination_that_never_returns_at_stop_is_write_stalled_within_the_bound() {
    let dest = MemDest::new();
    let close = dest.clone();
    let steps = periods(3)
        .chain([act(move || {
            close.until_writes(4);
            close.close();
        })])
        .chain(periods(3))
        .collect();
    let ran = run_script(
        6,
        SampleFormat::S16Le,
        steps,
        dest.clone(),
        StepClock::new(),
    );
    assert_eq!(ran.outcome, Outcome::Failed);
    let failed = ran.line("capture_failed");
    assert_eq!(failed["reason"], "write_stalled");
    let detail = failed["detail"].as_str().unwrap();
    assert!(detail.contains("for its reader to say"), "{detail}");
    assert!(
        ran.elapsed < BOUND + Duration::from_secs(1),
        "{:?}",
        ran.elapsed
    );
    let s = ran.summary();
    assert_eq!(s.end, None);
    assert_eq!(s.tail.verdict, "unterminated");
    dest.open();
}

#[test]
fn a_wall_clock_step_is_a_step_and_not_a_gap() {
    let clock = StepClock::new();
    let offset = clock.offset.clone();
    let steps = periods(5)
        .chain([act(move || {
            offset.fetch_add(3_000_000, Ordering::SeqCst);
        })])
        .chain(periods(5))
        .collect();
    let ran = run_script(6, SampleFormat::S16Le, steps, MemDest::new(), clock);
    let s = ran.summary();
    assert!(s.discontinuities.is_empty());
    let steps = s.wall_steps.as_ref().unwrap();
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].at_frame, 100);
    assert_eq!(steps[0].step_us, 3_000_000);
}

#[test]
fn a_late_block_carries_its_backlog_and_each_late_run_is_announced_once() {
    let late = |backlog| Step::Period {
        frames: PERIOD as usize,
        backlog,
    };
    let steps = periods(2)
        .chain([late(PERIOD), late(3 * PERIOD), late(PERIOD)])
        .chain(periods(2))
        .chain([late(2 * PERIOD), late(PERIOD - 1)])
        .collect();
    let ran = run_steps(steps);
    let late_lines: Vec<(u64, u64)> = ran
        .lines
        .iter()
        .filter(|l| l["event"] == "capture_late")
        .map(|l| {
            (
                l["at_frame"].as_u64().unwrap(),
                l["backlog_frames"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(late_lines, [(40, 20), (140, 40)]);
    let s = ran.summary();
    let late = s.late.as_ref().unwrap();
    assert_eq!(late.len(), 2);
    assert_eq!(late[0].blocks, 3);
    assert_eq!(late[0].max_backlog_frames, 3 * PERIOD);
    assert!(s.discontinuities.is_empty());
    let (steps, _) = ran.steps();
    let backlogs: Vec<u32> = steps
        .iter()
        .filter_map(|i| match i {
            LogStep::Read { backlog_frames, .. } => Some(*backlog_frames),
            _ => None,
        })
        .collect();
    assert_eq!(backlogs, [0, 0, 20, 60, 20, 0, 0, 40, 19]);
}

#[test]
fn a_copy_taken_mid_run_reads_back_without_error() {
    let dest = MemDest::new();
    let copier = dest.clone();
    let copy = Arc::new(Mutex::new(Vec::new()));
    let copy_out = copy.clone();
    let steps = periods(20)
        .chain([act(move || {
            std::thread::sleep(Duration::from_millis(20));
            *copy_out.lock().unwrap() = copier.bytes();
        })])
        .chain(periods(20))
        .collect();
    run_script(6, SampleFormat::S16Le, steps, dest, StepClock::new());
    let copy = copy.lock().unwrap().clone();
    for cut in [copy.len(), copy.len() - 1, copy.len() - 7] {
        let s = summary_of(&copy[..cut]);
        assert!(
            matches!(s.tail.verdict, "unterminated" | "torn"),
            "cut at {cut}: {}",
            s.tail.verdict
        );
        assert!(s.blocks.unwrap() > 0);
        assert_eq!(s.end, None);
    }
}

#[test]
fn a_header_the_destination_refuses_is_a_destination_failure() {
    let dest = MemDest::new();
    dest.0.refuse.store(1, Ordering::SeqCst);
    let ran = run_script(
        6,
        SampleFormat::S16Le,
        periods(3).collect(),
        dest,
        StepClock::new(),
    );
    assert_eq!(ran.outcome, Outcome::Failed);
    assert_eq!(ran.events(), ["capture_failed"]);
    let failed = ran.line("capture_failed");
    assert_eq!(failed["reason"], "destination");
    assert!(failed["record_id"].is_null(), "{failed}");
    assert!(failed["at_frame"].is_null(), "{failed}");
    assert!(ran.script.stopped);
}

#[test]
fn a_failed_sync_after_the_end_is_a_write_failure_and_says_the_end_is_there() {
    let dest = MemDest::new();
    dest.0.sync_fails.store(true, Ordering::SeqCst);
    let ran = run_script(
        6,
        SampleFormat::S16Le,
        periods(3).collect(),
        dest,
        StepClock::new(),
    );
    assert_eq!(ran.outcome, Outcome::Failed);
    let failed = ran.line("capture_failed");
    assert_eq!(failed["reason"], "write_failed");
    let detail = failed["detail"].as_str().unwrap();
    assert!(detail.contains("sync failed"), "{detail}");
    assert!(detail.contains("holds its end"), "{detail}");
    assert_eq!(end_reason(&ran.summary()), Some(StreamEndReason::Stopped));
}

#[test]
fn a_stream_that_will_not_start_is_refused_before_any_record() {
    let ran = run_steps([Step::StartFails].into_iter().chain(periods(3)).collect());
    assert_eq!(ran.outcome, Outcome::Failed);
    assert_eq!(ran.events(), ["capture_failed"]);
    let failed = ran.line("capture_failed");
    assert_eq!(failed["reason"], "stream_died");
    assert!(failed["record_id"].is_null(), "{failed}");
    assert!(failed["at_frame"].is_null(), "{failed}");
    assert!(
        failed["detail"]
            .as_str()
            .unwrap()
            .contains("would not start")
    );
    assert!(ran.record.is_empty(), "no header follows a refused trigger");
}

#[test]
fn a_read_that_delivers_nothing_is_no_block_and_no_period() {
    let empty = || Step::Period {
        frames: 0,
        backlog: 0,
    };
    // Twenty timeouts with an empty read between each: the empty reads do not
    // restart the count.
    let steps = periods(2)
        .chain((0..STALL_TIMEOUTS).flat_map(|_| [empty(), Step::TimedOut]))
        .chain(periods(2))
        .collect();
    let ran = run_steps(steps);
    assert_eq!(ran.outcome, Outcome::Failed);
    assert_eq!(ran.line("capture_failed")["reason"], "stream_died");
    let s = ran.summary();
    assert_eq!(s.blocks, Some(2));
    assert_eq!(s.short_blocks.as_deref(), Some(&[][..]));
    assert_eq!(s.frames_delivered, 40);
    assert_eq!(end_reason(&s), Some(StreamEndReason::StreamDied));
}

#[test]
fn a_free_space_reading_that_fails_is_a_failed_write() {
    let dest = MemDest::new();
    dest.0.free_fails.store(true, Ordering::SeqCst);
    let ran = run_script(
        6,
        SampleFormat::S16Le,
        periods(5).collect(),
        dest,
        StepClock::new(),
    );
    assert_eq!(ran.outcome, Outcome::Failed);
    let failed = ran.line("capture_failed");
    assert_eq!(failed["reason"], "write_failed");
    let detail = failed["detail"].as_str().unwrap();
    assert!(detail.contains("free space"), "{detail}");
    assert!(detail.contains("ends with End{write_failed}"), "{detail}");
    let s = ran.summary();
    assert_eq!(end_reason(&s), Some(StreamEndReason::WriteFailed));
    assert_eq!(s.tail.verdict, "ended");
}

#[test]
fn a_destination_that_falls_under_the_floor_mid_capture_ends_where_the_log_does() {
    let dest = MemDest::new();
    let low = dest.clone();
    let steps = periods(5)
        .chain([act(move || {
            // The header and five blocks, then the next one-second check finds
            // the floor and writes the end.
            low.until_writes(6);
            low.0.free.store(1 << 20, Ordering::SeqCst);
            low.until_writes(7);
        })])
        .chain(periods(5))
        .collect();
    let ran = run_script(6, SampleFormat::S16Le, steps, dest, StepClock::new());
    assert_eq!(ran.outcome, Outcome::Failed);
    let failed = ran.line("capture_failed");
    assert_eq!(failed["reason"], "destination_full");
    // The capture had read one more period when it found the writer gone.
    assert_eq!(failed["at_frame"], 6 * PERIOD);
    let s = ran.summary();
    assert_eq!(end_reason(&s), Some(StreamEndReason::DestinationFull));
    assert_eq!(s.end.as_ref().unwrap().at_frame, 5 * u64::from(PERIOD));
    assert_eq!(s.frames_delivered, 5 * u64::from(PERIOD));
    assert_eq!(s.blocks, Some(5));
    assert_eq!(s.tail.verdict, "ended");
}

#[test]
fn a_stop_held_in_the_final_sync_is_write_stalled_with_the_end_in_the_log() {
    let dest = MemDest::new();
    dest.0.sync_held.store(true, Ordering::SeqCst);
    let ran = run_script(
        6,
        SampleFormat::S16Le,
        periods(3).collect(),
        dest.clone(),
        StepClock::new(),
    );
    dest.0.sync_held.store(false, Ordering::SeqCst);
    assert_eq!(ran.outcome, Outcome::Failed);
    let failed = ran.line("capture_failed");
    assert_eq!(failed["reason"], "write_stalled");
    let detail = failed["detail"].as_str().unwrap();
    assert!(!detail.contains("no end"), "{detail}");
    let s = ran.summary();
    assert_eq!(end_reason(&s), Some(StreamEndReason::Stopped));
    assert_eq!(s.tail.verdict, "ended");
}

fn late(backlog: u32) -> Step {
    Step::Period {
        frames: PERIOD as usize,
        backlog,
    }
}

fn late_lines(ran: &Ran) -> Vec<u64> {
    ran.lines
        .iter()
        .filter(|l| l["event"] == "capture_late")
        .map(|l| l["at_frame"].as_u64().unwrap())
        .collect()
}

#[test]
fn a_gap_between_late_blocks_starts_a_new_late_run() {
    let steps = periods(1)
        .chain([late(PERIOD), Step::ReadRecovered(libc::EPIPE), late(PERIOD)])
        .chain(periods(1))
        .collect();
    let ran = run_steps(steps);
    assert_eq!(late_lines(&ran), [20, 40]);
    let s = ran.summary();
    let runs: Vec<u64> = s.late.unwrap().iter().map(|r| r.first_frame).collect();
    assert_eq!(runs, [20, 40]);
}

#[test]
fn a_late_block_the_queue_drops_announces_no_late_run() {
    let dest = MemDest::new();
    let open = dest.clone();
    // The held block and a hundred more fill the queue; the late block after
    // them, at frame 2020, is dropped. The late block after the destination
    // opens again, at 2040, is the record's only late run.
    let steps = held_writer(&dest)
        .chain(periods(100))
        .chain([late(PERIOD)])
        .chain([act(move || {
            open.open();
            open.until_writes(102);
        })])
        .chain([late(PERIOD)])
        .chain(periods(2))
        .collect();
    let ran = run_script(6, SampleFormat::S16Le, steps, dest, StepClock::new());
    assert_eq!(ran.outcome, Outcome::Stopped);
    let s = ran.summary();
    assert_eq!(gaps(&s), [(LossCause::WriterBacklog, 2020, Some(20))]);
    assert_eq!(late_lines(&ran), [2040]);
    let runs: Vec<u64> = s.late.unwrap().iter().map(|r| r.first_frame).collect();
    assert_eq!(runs, [2040]);
}

#[test]
fn a_block_is_late_against_the_granted_period_not_the_one_asked_for() {
    // 1 kHz asks for a 20-frame period; the device granted 25. A backlog of
    // 20 is not a period behind, one of 25 is.
    let read = |backlog| Step::Period {
        frames: 25,
        backlog,
    };
    let steps = vec![read(0), read(20), read(25), read(0)];
    let ran = run_recording(
        recording(6, SampleFormat::S16Le, 25),
        steps,
        MemDest::new(),
        StepClock::new(),
    );
    assert_eq!(ran.outcome, Outcome::Stopped);
    assert_eq!(late_lines(&ran), [50]);
    assert_eq!(ran.line("capture_started")["granted"]["period_frames"], 25);
    let s = ran.summary();
    assert_eq!(s.capture.as_ref().unwrap().period_frames, 25);
    let late = s.late.unwrap();
    assert_eq!(late.len(), 1);
    assert_eq!((late[0].first_frame, late[0].blocks), (50, 1));
    assert_eq!(late[0].max_backlog_frames, 25);
    assert_eq!(s.short_blocks.as_deref(), Some(&[][..]));
}

#[test]
fn an_audio_frame_s_time_is_its_first_frame_s_capture_time() {
    // The clock reads 1 ms apart: the start pair, then one per read. A
    // 20-frame read at 1 kHz with 40 frames behind it began 60 ms before
    // the read returned.
    let steps = vec![Step::Period {
        frames: 20,
        backlog: 40,
    }];
    let ran = run_steps(steps);
    let (steps, _) = ran.steps();
    let read_mono = steps
        .iter()
        .find_map(|s| match s {
            LogStep::Read { mono_us, .. } => Some(*mono_us),
            _ => None,
        })
        .unwrap();
    let audio_ts = steps
        .iter()
        .find_map(|s| match s {
            LogStep::Audio(a) => Some(a.device_ts_us),
            _ => None,
        })
        .unwrap();
    assert_eq!(audio_ts, read_mono - 60_000);
    assert_eq!(ran.summary().segments[0].base_frame, 0);
}
