//! The paced playback sender: one `PlaybackWriter` task per pod connection,
//! writing outbound speech back down the same TCP connection the pod streams in
//! on.
//!
//! The task writes one leading `Hello`, then chunks each queued clip into 20 ms
//! `Audio` frames paced at real-time rate plus a small fixed lead, and marks each
//! drained stream with `EndOfAudio`. Every frame write is timeout-bounded: a
//! wedged playback direction aborts its jobs and dies loudly rather than parking
//! forever or stalling the ingest read half. Pacing runs on the monotonic
//! `tokio::time` clock, so the audio-ahead-of-real-time bound holds regardless of
//! wall-clock (NTP) steps — that bound is also what a future flush queues behind.
//!
//! Generic over `AsyncWrite`: production passes the connection's write half; tests
//! pass a `tokio::io::duplex` fake device.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use audio_pipeline::wire::{
    encode_frame, AudioFrame, ChannelSource, EndOfAudio, FlushPlayback, Hello, StreamFrame,
    AUDIO_PROTOCOL_VERSION, AUDIO_SAMPLES_PER_FRAME, MAX_AUDIO_PAYLOAD, MAX_FRAME_BYTES,
};
use futures::future::BoxFuture;
use heapless::{String as HString, Vec as HVec};
use pod_ingest::HostMicros;
use serde::Serialize;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::types::{InterruptProgress, PodId, StageTimings, UtteranceId, SPINE_FORMAT};

/// Wall-clock duration of one `Audio` frame. One `AUDIO_SAMPLES_PER_FRAME` chunk at
/// 16 kHz is 20 ms; the assert ties the constant to the frame size so a frame-size
/// change cannot silently desynchronize the pacer. Public so the surface's
/// `lead_ms` floor validates against this single guarded source, not a copied literal.
pub const FRAME_MS: u64 = 20;
const _: () = assert!(
    AUDIO_SAMPLES_PER_FRAME as u64 * 1000 == FRAME_MS * SPINE_FORMAT.sample_rate_hz as u64,
    "FRAME_MS must equal one AUDIO_SAMPLES_PER_FRAME at the spine sample rate",
);

/// `pod_id` the playback `Hello` advertises. Names the sender (the surface), not a
/// pod; the device keys nothing off it, validating only the format scalars.
const SENDER_POD_ID: &str = "speech-surface";

/// Tunables for the pacer and its per-write budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PacerConfig {
    /// Audio permitted ahead of real time before the pacer sleeps, in milliseconds.
    pub lead_ms: u64,
    /// Per-frame-write budget before the write is treated as wedged, in milliseconds.
    pub write_timeout_ms: u64,
    /// Jobs that may wait in the queue beyond the one playing.
    pub job_queue_depth: usize,
}

impl Default for PacerConfig {
    fn default() -> Self {
        Self {
            lead_ms: audio_pipeline::playback::PLAYBACK_BURST_LEAD_MS,
            write_timeout_ms: 1000,
            job_queue_depth: 2,
        }
    }
}

/// One unit of outbound playback: a ready PCM clip plus the originating utterance's
/// stamps, for the latency-decomposition line the surface emits.
#[derive(Debug, Clone)]
pub struct PlaybackJob {
    /// 16 kHz mono S16 samples (`SPINE_FORMAT`).
    pub pcm: Arc<[i16]>,
    /// The utterance this playback answers, if any.
    pub in_reply_to: Option<UtteranceId>,
    /// Whether speech detected during this job may flush it. Copied from the
    /// originating `SpeakCmd`; a false here makes [`PlaybackHandle::flush`] reject.
    pub interruptible: bool,
    /// The originating utterance's pipeline stamps.
    pub timings: StageTimings,
    /// The router's `SpeakCmd`-receipt stamp.
    pub speak_rx: HostMicros,
}

/// Why a job was aborted. Serializes to the `write_error` / `write_timeout` /
/// `cancelled` reason the surface's `playback_aborted` line reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AbortReason {
    /// A frame write returned an I/O error (peer gone / connection reset).
    WriteError,
    /// A frame write exceeded `write_timeout_ms` (peer alive but not draining).
    WriteTimeout,
    /// The owning connection's cancellation token fired (supersede / shutdown).
    Cancelled,
}

/// A typed playback event. The surface adapts each variant to one JSONL line and
/// computes latency deltas from the raw stamps `Started` carries.
#[derive(Debug, Clone)]
pub enum PlaybackEvent {
    /// The leading `Hello` was written — one per writer spawn.
    HelloWritten { pod: PodId },
    /// The leading `Hello` write failed; the writer served no job and exits. Makes
    /// a registration-time write failure loud, mirroring `HelloWritten`.
    HelloFailed { pod: PodId, reason: AbortReason },
    /// The first `Audio` frame of a job went out. Carries raw stamps (not deltas)
    /// so the surface computes deltas through the one shared delta function. The
    /// timings are boxed: they carry a stamp per stage of the whole
    /// segment-and-response cycle, which would otherwise make this variant several
    /// times the size of every other one and pay for itself on every event.
    Started {
        pod: PodId,
        in_reply_to: Option<UtteranceId>,
        timings: Box<StageTimings>,
        speak_rx: HostMicros,
        first_write: HostMicros,
        samples: u64,
        /// Whether speech may flush this job. Rides the event so the surface can
        /// tell the listener whether the barge-in floor is open for this playback.
        interruptible: bool,
    },
    /// A job's audio was fully written. `eoa_written` is true when this job drained
    /// the stream and an `EndOfAudio` followed it.
    Finished {
        pod: PodId,
        in_reply_to: Option<UtteranceId>,
        frames: u64,
        samples: u64,
        eoa_written: bool,
        /// The stream-drain `EndOfAudio` write failed, so the writer is exiting
        /// after this otherwise-completed job. A job that plays out fully is a
        /// clean completion whether or not it drained the stream — a clip finishing
        /// with another job queued behind it writes no `EndOfAudio` yet delivered
        /// all its audio — so this death shape is the only unclean `Finished`.
        writer_dying: bool,
    },
    /// A job was aborted (write failure or cancellation); its audio did not finish.
    Aborted {
        pod: PodId,
        in_reply_to: Option<UtteranceId>,
        reason: AbortReason,
    },
    /// A job was cut by a barge-in flush. Distinct from `Aborted`: nothing failed
    /// and the writer stays alive to play the next turn's response.
    Flushed {
        pod: PodId,
        in_reply_to: Option<UtteranceId>,
        /// The playing job that was cut. `false` for a queued job evicted behind it.
        was_playing: bool,
        frames_written: u64,
        /// The playing job's progress at the cut; zeros for an evicted job, which
        /// was never audible.
        progress: InterruptProgress,
    },
}

/// The sink each writer emits its events into. `Arc`'d so one closure serves every
/// writer; the surface owns the adapter that turns events into JSONL lines.
///
/// Async because the adapter's fan-out reaches the listener's feed channel, whose
/// marker sends wait for room. Each writer awaits its own emissions in order, so a
/// pod's playback events reach the listener in the order they happened.
pub type PlaybackEventFn = Arc<dyn Fn(PlaybackEvent) -> BoxFuture<'static, ()> + Send + Sync>;

/// Shared, atomically-updated playback counters. One process-wide instance is read
/// for `stage_health` via [`PlaybackStats::snapshot`]; the atomics stay private so
/// the synchronization detail never leaks to the writer tasks (the `WakeStats`
/// idiom).
#[derive(Debug, Default)]
pub struct PlaybackStats {
    jobs_completed: AtomicU64,
    jobs_rejected_full: AtomicU64,
    jobs_rejected_dead: AtomicU64,
    jobs_aborted: AtomicU64,
    jobs_flushed: AtomicU64,
    frames_written: AtomicU64,
    write_timeouts: AtomicU64,
    eoa_written: AtomicU64,
    eoa_write_failures: AtomicU64,
}

/// A point-in-time copy of [`PlaybackStats`], for `stage_health` reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct PlaybackStatsSnapshot {
    /// Jobs whose audio was fully written.
    pub jobs_completed: u64,
    /// Jobs rejected because a writer's queue was full.
    pub jobs_rejected_full: u64,
    /// Jobs rejected because the writer task had already exited.
    pub jobs_rejected_dead: u64,
    /// Jobs aborted by a write failure or cancellation.
    pub jobs_aborted: u64,
    /// Jobs cut or evicted by a barge-in flush.
    pub jobs_flushed: u64,
    /// Total `Audio` frames written across all writers.
    pub frames_written: u64,
    /// Frame writes that hit the per-write timeout.
    pub write_timeouts: u64,
    /// `EndOfAudio` frames written at stream drains.
    pub eoa_written: u64,
    /// `EndOfAudio` writes that failed (timeout or error), leaving the writer dead
    /// after an otherwise-completed job. Distinct from `write_timeouts` so a
    /// non-timeout drain failure is not invisible.
    pub eoa_write_failures: u64,
}

impl PlaybackStats {
    fn record_completed(&self) {
        self.jobs_completed.fetch_add(1, Ordering::Relaxed);
    }
    fn record_rejected_full(&self) {
        self.jobs_rejected_full.fetch_add(1, Ordering::Relaxed);
    }
    fn record_rejected_dead(&self) {
        self.jobs_rejected_dead.fetch_add(1, Ordering::Relaxed);
    }
    fn record_aborted(&self) {
        self.jobs_aborted.fetch_add(1, Ordering::Relaxed);
    }
    fn record_flushed(&self) {
        self.jobs_flushed.fetch_add(1, Ordering::Relaxed);
    }
    fn record_frame(&self) {
        self.frames_written.fetch_add(1, Ordering::Relaxed);
    }
    fn record_write_timeout(&self) {
        self.write_timeouts.fetch_add(1, Ordering::Relaxed);
    }
    fn record_eoa(&self) {
        self.eoa_written.fetch_add(1, Ordering::Relaxed);
    }
    fn record_eoa_failure(&self) {
        self.eoa_write_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// A `Copy` snapshot of the counters, read for `stage_health`.
    pub fn snapshot(&self) -> PlaybackStatsSnapshot {
        PlaybackStatsSnapshot {
            jobs_completed: self.jobs_completed.load(Ordering::Relaxed),
            jobs_rejected_full: self.jobs_rejected_full.load(Ordering::Relaxed),
            jobs_rejected_dead: self.jobs_rejected_dead.load(Ordering::Relaxed),
            jobs_aborted: self.jobs_aborted.load(Ordering::Relaxed),
            jobs_flushed: self.jobs_flushed.load(Ordering::Relaxed),
            frames_written: self.frames_written.load(Ordering::Relaxed),
            write_timeouts: self.write_timeouts.load(Ordering::Relaxed),
            eoa_written: self.eoa_written.load(Ordering::Relaxed),
            eoa_write_failures: self.eoa_write_failures.load(Ordering::Relaxed),
        }
    }
}

/// Why a `try_play` was refused. Mirrors `ResponseSink::try_send`: non-blocking, and
/// a full queue never disturbs the playing audio.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlayRejected {
    /// The writer's job queue is full; the new job is dropped, playing audio intact.
    QueueFull,
    /// The writer task has exited (timeout/error/cancel); no more jobs are accepted.
    WriterDead,
}

/// The writer's currently-playing job, as the flush path needs to see it.
#[derive(Debug, Clone, Copy)]
struct CurrentJob {
    turn: Option<UtteranceId>,
    interruptible: bool,
    total_samples: u64,
    /// Monotonic instant the job's first frame went out — the origin the heard
    /// estimate measures from. `None` before that frame lands: the job is current
    /// (and so flushable) from the moment the writer takes it, but nothing of it
    /// is audible yet.
    first_write: Option<Instant>,
}

/// The writer's currently-playing job, readable by the flush path. Written by the
/// writer at job start and end; read under the mutex by [`PlaybackHandle::flush`].
/// The per-frame hot path costs one relaxed atomic add.
#[derive(Debug, Default)]
struct JobProgress {
    /// `None` between jobs.
    current: Mutex<Option<CurrentJob>>,
    /// Frames of the current job written so far; reset at each job start.
    frames_written: AtomicU64,
}

impl JobProgress {
    /// The heard/total estimate for `job` right now.
    ///
    /// `heard_ms` is the lesser of elapsed wall time and the audio actually
    /// written: the pacer front-loads up to `lead_ms`, so frames-written alone
    /// overshoots by up to that lead, while elapsed time alone overshoots inside
    /// the sub-lead startup window before the first frame lands.
    fn snapshot(&self, job: &CurrentJob) -> InterruptProgress {
        let elapsed_ms = job
            .first_write
            .map(|t| Instant::now().duration_since(t).as_millis() as u64)
            .unwrap_or(0);
        let written_ms = self.frames_written.load(Ordering::Relaxed) * FRAME_MS;
        InterruptProgress {
            heard_ms: elapsed_ms.min(written_ms),
            total_ms: job.total_samples * 1000 / SPINE_FORMAT.sample_rate_hz as u64,
        }
    }
}

/// The flush request handed from a [`PlaybackHandle`] to its writer.
#[derive(Debug, Default)]
struct FlushSignal {
    /// The turn to flush, set by `flush`, taken by the writer.
    target: Mutex<Option<UtteranceId>>,
    notify: Notify,
}

/// Why a [`PlaybackHandle::flush`] was refused. Every variant is a no-op at the
/// writer: a stale interrupt never cuts the wrong response. Serializes to the
/// reason string the surface's `barge_in_stale` line reports, mirroring
/// [`AbortReason`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FlushRejected {
    /// No job is playing right now.
    NotPlaying,
    /// A different turn's job is playing (the named turn already finished).
    WrongTurn,
    /// The playing job is marked non-interruptible (an alert).
    NotInterruptible,
    /// The writer task has exited.
    WriterDead,
}

/// Handle to a spawned [`PlaybackWriter`]: enqueue jobs without blocking.
pub struct PlaybackHandle {
    tx: mpsc::Sender<PlaybackJob>,
    stats: Arc<PlaybackStats>,
    progress: Arc<JobProgress>,
    flush_signal: Arc<FlushSignal>,
}

impl PlaybackHandle {
    /// Enqueue a job for playback. Non-blocking: a full queue rejects the new job
    /// (playing audio runs to completion), a dead writer rejects everything.
    pub fn try_play(&self, job: PlaybackJob) -> Result<(), PlayRejected> {
        match self.tx.try_send(job) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.stats.record_rejected_full();
                Err(PlayRejected::QueueFull)
            }
            Err(TrySendError::Closed(_)) => {
                self.stats.record_rejected_dead();
                Err(PlayRejected::WriterDead)
            }
        }
    }

    /// The turn whose job is playing right now, or `None` between jobs (and for a
    /// job with no originating utterance). The key a caller passes to [`flush`].
    ///
    /// [`flush`]: PlaybackHandle::flush
    pub fn current_turn(&self) -> Option<UtteranceId> {
        self.progress
            .current
            .lock()
            .expect("job progress mutex")
            .and_then(|c| c.turn)
    }

    /// Request a flush of the playback for `turn`.
    ///
    /// Returns the progress snapshot when `turn` names the currently-playing
    /// interruptible job — the writer will cut it, evict any queued jobs for the
    /// same turn, and send `FlushPlayback` on the wire, after which the device
    /// discards its banked audio and mutes. Every other case is a
    /// `FlushRejected` with no side effects: a stale interrupt (the turn already
    /// finished, or a different turn is playing) is a no-op by construction,
    /// never a flush of the wrong response.
    ///
    /// **Flush promptness.** The frame queues on the TCP stream behind whatever
    /// audio is already written, but the pacer bounds that to `lead_ms` of audio
    /// (~32 KB at the spine format for the 1 s default) — single-digit
    /// milliseconds of LAN transit. If measurement ever shows otherwise, clamping
    /// `SO_SNDBUF` on the pod socket is the follow-up knob; it is not worth the
    /// write-stall risk for an unmeasured win.
    pub fn flush(&self, turn: UtteranceId) -> Result<InterruptProgress, FlushRejected> {
        if self.tx.is_closed() {
            return Err(FlushRejected::WriterDead);
        }
        let current = self.progress.current.lock().expect("job progress mutex");
        let job = current.ok_or(FlushRejected::NotPlaying)?;
        if job.turn != Some(turn) {
            return Err(FlushRejected::WrongTurn);
        }
        if !job.interruptible {
            return Err(FlushRejected::NotInterruptible);
        }
        let progress = self.progress.snapshot(&job);
        // Publish the target before dropping the job lock, so the writer cannot
        // observe a notify with no target behind it.
        *self.flush_signal.target.lock().expect("flush target mutex") = Some(turn);
        drop(current);
        self.flush_signal.notify.notify_one();
        Ok(progress)
    }
}

/// Spawns and owns one per-pod paced writer task.
pub struct PlaybackWriter;

impl PlaybackWriter {
    /// Spawn a writer over `io` for `pod`. The task writes the leading `Hello`
    /// eagerly (validating the write path at registration), then serves jobs until
    /// the returned handle drops (queue closes) or `cancel` fires.
    pub fn spawn<W>(
        io: W,
        pod: PodId,
        cfg: PacerConfig,
        stats: Arc<PlaybackStats>,
        events: PlaybackEventFn,
        cancel: CancellationToken,
    ) -> PlaybackHandle
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (tx, rx) = mpsc::channel::<PlaybackJob>(cfg.job_queue_depth);
        let progress = Arc::new(JobProgress::default());
        let flush_signal = Arc::new(FlushSignal::default());
        let writer = Writer {
            io,
            pod,
            cfg,
            stats: Arc::clone(&stats),
            events,
            cancel,
            progress: Arc::clone(&progress),
            flush_signal: Arc::clone(&flush_signal),
            buf: [0u8; MAX_FRAME_BYTES + 2],
            anchor: None,
            frames_in_stream: 0,
            deferred: VecDeque::new(),
        };
        tokio::spawn(writer.run(rx));
        PlaybackHandle {
            tx,
            stats,
            progress,
            flush_signal,
        }
    }
}

/// One pod connection's paced writer: the per-writer state the job loop threads
/// through every step, plus the stream clock it keeps across back-to-back jobs.
struct Writer<W> {
    io: W,
    pod: PodId,
    cfg: PacerConfig,
    stats: Arc<PlaybackStats>,
    events: PlaybackEventFn,
    cancel: CancellationToken,
    progress: Arc<JobProgress>,
    flush_signal: Arc<FlushSignal>,
    buf: [u8; MAX_FRAME_BYTES + 2],
    /// Stream clock: anchored at the first frame of an idle→busy transition, reset
    /// only after a drain or a flush, so back-to-back jobs ride one continuous
    /// clock.
    anchor: Option<Instant>,
    /// Frames written since the current stream's anchor.
    frames_in_stream: u64,
    /// Jobs pulled off the queue during a flush's selective eviction but belonging
    /// to another turn. Served before the queue so their order is preserved.
    deferred: VecDeque<PlaybackJob>,
}

/// How one frame write ended.
enum WriteFail {
    Error,
    Timeout,
    Cancelled,
}

impl From<WriteFail> for AbortReason {
    fn from(f: WriteFail) -> AbortReason {
        match f {
            WriteFail::Error => AbortReason::WriteError,
            WriteFail::Timeout => AbortReason::WriteTimeout,
            WriteFail::Cancelled => AbortReason::Cancelled,
        }
    }
}

/// One 20 ms `Audio` frame from `chunk`, zero-padded to a full frame. Sentinel
/// `segment_id`/`first_sample_index`/`device_ts_us` = 0, as the device's inbound
/// sink expects on the server→device direction.
///
/// Panics if `chunk` is longer than one frame.
fn build_audio_frame(chunk: &[i16]) -> StreamFrame {
    assert!(
        chunk.len() <= AUDIO_SAMPLES_PER_FRAME,
        "chunk of {} samples exceeds one frame ({AUDIO_SAMPLES_PER_FRAME} samples)",
        chunk.len(),
    );
    let mut padded = [0i16; AUDIO_SAMPLES_PER_FRAME];
    padded[..chunk.len()].copy_from_slice(chunk);
    let pcm: HVec<u8, MAX_AUDIO_PAYLOAD> = audio_pipeline::wire::pack_pcm_s16le(&padded);
    StreamFrame::Audio(AudioFrame {
        segment_id: 0,
        first_sample_index: 0,
        device_ts_us: 0,
        pcm,
    })
}

/// Outcome of playing one job's audio.
enum JobResult {
    Completed {
        frames: u64,
        samples: u64,
    },
    Aborted(AbortReason),
    /// A flush named this job: writing stopped early, and the writer stays alive.
    /// The progress is snapshotted at the cut, while the job is still current.
    Flushed {
        frames: u64,
        progress: InterruptProgress,
    },
}

/// What the writer may do next, once the pacer's slot for a frame arrives.
enum FrameSlot {
    /// Write the frame.
    Ready,
    /// `cancel` fired during the wait.
    Cancelled,
    /// A flush naming the current job arrived; stop writing it.
    Flush,
}

impl<W> Writer<W>
where
    W: AsyncWrite + Unpin,
{
    /// Encode `frame` and write it whole, bounded by the per-write budget and
    /// interruptible by `cancel`. Cancellation wins over the write so a mid-gap
    /// cancel aborts promptly.
    async fn write_frame(&mut self, frame: &StreamFrame) -> Result<(), WriteFail> {
        // Encode is an internal invariant (frame ≤ MAX_FRAME_BYTES, buf sized for
        // it), never a peer condition — a break is a code/schema bug, so crash
        // loudly here rather than mislabel it a peer-gone write error.
        let n = encode_frame(frame, &mut self.buf)
            .expect("frame encodes within buf (MAX_FRAME_BYTES + 2)");
        let timeout = Duration::from_millis(self.cfg.write_timeout_ms);
        let r = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => return Err(WriteFail::Cancelled),
            r = tokio::time::timeout(timeout, self.io.write_all(&self.buf[..n])) => r,
        };
        match r {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(WriteFail::Error),
            Err(_) => Err(WriteFail::Timeout),
        }
    }

    /// Take a pending flush request if it names `turn`. A signal for any other turn
    /// is dropped: the handle already told its caller the outcome, so a race
    /// between the handle's check and this take resolves to a no-op.
    fn take_flush_for(&self, turn: Option<UtteranceId>) -> bool {
        let mut target = self.flush_signal.target.lock().expect("flush target mutex");
        match *target {
            Some(t) if Some(t) == turn => {
                *target = None;
                true
            }
            // A target that no longer names the current turn is stale by
            // construction — the handle verified the playing job before setting it,
            // so a mismatch means the writer has moved past that job. Clear it too,
            // so a non-matching signal is genuinely dropped rather than lingering.
            Some(_) => {
                *target = None;
                false
            }
            None => false,
        }
    }

    /// Wait for the pacer's slot for the frame at the current stream index, so
    /// banked audio stays at most `lead_ms` ahead of real time. Cancellation and a
    /// flush for `turn` both cut the wait short.
    async fn wait_frame_slot(&mut self, anchor: Instant, turn: Option<UtteranceId>) -> FrameSlot {
        if self.take_flush_for(turn) {
            return FrameSlot::Flush;
        }
        let banked = Duration::from_millis(self.frames_in_stream * FRAME_MS);
        let ahead = (anchor + banked).saturating_duration_since(Instant::now());
        let lead = Duration::from_millis(self.cfg.lead_ms);
        if ahead <= lead {
            return FrameSlot::Ready;
        }
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => FrameSlot::Cancelled,
            // The target mutex is the authority; this only wakes the nap early.
            _ = self.flush_signal.notify.notified() => {
                if self.take_flush_for(turn) { FrameSlot::Flush } else { FrameSlot::Ready }
            }
            _ = tokio::time::sleep(ahead - lead) => FrameSlot::Ready,
        }
    }

    /// Chunk and pace a job's PCM out as `Audio` frames, emitting `Started` at the
    /// first frame and publishing progress the flush path reads.
    async fn play_job(&mut self, job: &PlaybackJob, anchor: Instant) -> JobResult {
        let samples = job.pcm.len() as u64;
        // Current from the moment the writer takes the job, not from its first
        // frame: a barge landing while the pacer holds this job back must still cut
        // it (the stream's banked audio is what the user is hearing).
        *self.progress.current.lock().expect("job progress mutex") = Some(CurrentJob {
            turn: job.in_reply_to,
            interruptible: job.interruptible,
            total_samples: samples,
            first_write: None,
        });
        self.progress.frames_written.store(0, Ordering::Relaxed);

        let mut job_frames = 0u64;
        let mut started = false;

        for chunk in job.pcm.chunks(AUDIO_SAMPLES_PER_FRAME) {
            match self.wait_frame_slot(anchor, job.in_reply_to).await {
                FrameSlot::Ready => {}
                FrameSlot::Cancelled => return JobResult::Aborted(AbortReason::Cancelled),
                FrameSlot::Flush => {
                    // Snapshot while the job is still current — the loop below
                    // clears it as soon as this returns.
                    let current = self.progress.current.lock().expect("job progress mutex");
                    let progress = current
                        .as_ref()
                        .map(|c| self.progress.snapshot(c))
                        .expect("the playing job is current until play_job returns");
                    return JobResult::Flushed {
                        frames: job_frames,
                        progress,
                    };
                }
            }
            let first_write = (!started).then(HostMicros::now);
            let frame = build_audio_frame(chunk);
            if let Err(f) = self.write_frame(&frame).await {
                if matches!(f, WriteFail::Timeout) {
                    self.stats.record_write_timeout();
                }
                return JobResult::Aborted(f.into());
            }
            self.stats.record_frame();
            self.progress.frames_written.fetch_add(1, Ordering::Relaxed);
            self.frames_in_stream += 1;
            job_frames += 1;
            if !started {
                started = true;
                if let Some(cur) = self
                    .progress
                    .current
                    .lock()
                    .expect("job progress mutex")
                    .as_mut()
                {
                    cur.first_write = Some(Instant::now());
                }
                (self.events)(PlaybackEvent::Started {
                    pod: self.pod.clone(),
                    in_reply_to: job.in_reply_to,
                    timings: Box::new(job.timings.clone()),
                    speak_rx: job.speak_rx,
                    first_write: first_write.expect("stamped on the first frame"),
                    samples,
                    interruptible: job.interruptible,
                })
                .await;
            }
        }

        JobResult::Completed {
            frames: job_frames,
            samples,
        }
    }

    /// Emit `Aborted` for every job still queued, counting each. Used on a write
    /// failure or cancellation to fail the whole backlog loudly rather than
    /// silently.
    async fn drain_aborted(&mut self, rx: &mut mpsc::Receiver<PlaybackJob>, reason: AbortReason) {
        // Close first so the drain-then-exit is atomic from a sender's view: a job
        // that races the drain is rejected as `WriterDead` rather than accepted and
        // then destroyed by the receiver drop with no terminal event.
        rx.close();
        let queued = std::mem::take(&mut self.deferred)
            .into_iter()
            .chain(std::iter::from_fn(|| rx.try_recv().ok()));
        for job in queued {
            self.stats.record_aborted();
            (self.events)(PlaybackEvent::Aborted {
                pod: self.pod.clone(),
                in_reply_to: job.in_reply_to,
                reason,
            })
            .await;
        }
    }

    /// Cut the flushed turn: report the playing job, evict the turn's queued jobs,
    /// and end the stream on the wire with `FlushPlayback`.
    ///
    /// No `EndOfAudio` follows: the device's flush already discards its banked
    /// audio and mutes, so an end-of-audio mark after it would be a redundant
    /// second one. The writer stays alive — unlike a cancel, a flush is a mid-life
    /// event, and the barge-in's own response plays next on this connection.
    async fn handle_flush(
        &mut self,
        job: &PlaybackJob,
        frames: u64,
        progress: InterruptProgress,
        rx: &mut mpsc::Receiver<PlaybackJob>,
    ) -> Result<(), WriteFail> {
        self.stats.record_flushed();
        (self.events)(PlaybackEvent::Flushed {
            pod: self.pod.clone(),
            in_reply_to: job.in_reply_to,
            was_playing: true,
            frames_written: frames,
            progress,
        })
        .await;

        // Evict the flushed turn's queued jobs; anything for another turn is
        // deferred, keeping its order, and plays after the flush on a fresh stream.
        while let Ok(queued) = rx.try_recv() {
            if queued.in_reply_to == job.in_reply_to {
                self.stats.record_flushed();
                (self.events)(PlaybackEvent::Flushed {
                    pod: self.pod.clone(),
                    in_reply_to: queued.in_reply_to,
                    was_playing: false,
                    frames_written: 0,
                    progress: InterruptProgress {
                        heard_ms: 0,
                        total_ms: 0,
                    },
                })
                .await;
            } else {
                self.deferred.push_back(queued);
            }
        }

        self.write_frame(&StreamFrame::FlushPlayback(FlushPlayback {}))
            .await?;
        // The flush ended the stream the way a drain's EndOfAudio does.
        self.anchor = None;
        self.frames_in_stream = 0;
        Ok(())
    }

    /// The next job to play: deferred jobs first (they were queued before anything
    /// still in the channel), then the queue.
    async fn next_job(&mut self, rx: &mut mpsc::Receiver<PlaybackJob>) -> Option<PlaybackJob> {
        if let Some(job) = self.deferred.pop_front() {
            return Some(job);
        }
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => {
                self.drain_aborted(rx, AbortReason::Cancelled).await;
                None
            }
            j = rx.recv() => j, // `None`: all senders dropped, queue drained.
        }
    }

    /// The writer task body: leading `Hello`, then the paced job loop.
    async fn run(mut self, mut rx: mpsc::Receiver<PlaybackJob>) {
        // Hello first: one per connection, validating the write path at
        // registration.
        let hello = StreamFrame::Hello(Hello {
            version: AUDIO_PROTOCOL_VERSION,
            pod_id: HString::try_from(SENDER_POD_ID).expect("SENDER_POD_ID fits pod_id capacity"),
            sample_rate_hz: SPINE_FORMAT.sample_rate_hz,
            bits_per_sample: SPINE_FORMAT.bits_per_sample,
            channels: SPINE_FORMAT.channels,
            codec: SPINE_FORMAT.codec,
            channel_source: ChannelSource::CommunicationBeam,
        });
        if let Err(f) = self.write_frame(&hello).await {
            if matches!(f, WriteFail::Timeout) {
                self.stats.record_write_timeout();
            }
            let reason = AbortReason::from(f);
            (self.events)(PlaybackEvent::HelloFailed {
                pod: self.pod.clone(),
                reason,
            })
            .await;
            self.drain_aborted(&mut rx, reason).await;
            return;
        }
        (self.events)(PlaybackEvent::HelloWritten {
            pod: self.pod.clone(),
        })
        .await;

        while let Some(job) = self.next_job(&mut rx).await {
            let anchor = *self.anchor.get_or_insert_with(Instant::now);
            let result = self.play_job(&job, anchor).await;
            // The job is no longer current whatever its outcome, so a flush racing
            // the boundary finds nothing to cut and resolves to a no-op.
            *self.progress.current.lock().expect("job progress mutex") = None;

            match result {
                JobResult::Completed { frames, samples } => {
                    self.stats.record_completed();
                    // A drained queue ends the stream: mark it with EndOfAudio, then
                    // re-anchor the next stream. A new job already waiting continues
                    // the same stream with no intervening EndOfAudio.
                    let drained = self.deferred.is_empty() && rx.is_empty();
                    let mut eoa_written = false;
                    if drained {
                        let eoa = StreamFrame::EndOfAudio(EndOfAudio {});
                        match self.write_frame(&eoa).await {
                            Ok(()) => {
                                self.stats.record_eoa();
                                eoa_written = true;
                            }
                            Err(f) => {
                                self.stats.record_eoa_failure();
                                if matches!(f, WriteFail::Timeout) {
                                    self.stats.record_write_timeout();
                                }
                                let reason = AbortReason::from(f);
                                // The job itself completed; report it. The failed
                                // EndOfAudio leaves the writer dead (the device mutes
                                // on the disconnect it is about to observe), so fail
                                // any job that raced into the queue during the write
                                // loudly rather than dropping it on the receiver
                                // teardown.
                                (self.events)(PlaybackEvent::Finished {
                                    pod: self.pod.clone(),
                                    in_reply_to: job.in_reply_to,
                                    frames,
                                    samples,
                                    eoa_written: false,
                                    writer_dying: true,
                                })
                                .await;
                                self.drain_aborted(&mut rx, reason).await;
                                return;
                            }
                        }
                        self.anchor = None;
                        self.frames_in_stream = 0;
                    }
                    (self.events)(PlaybackEvent::Finished {
                        pod: self.pod.clone(),
                        in_reply_to: job.in_reply_to,
                        frames,
                        samples,
                        eoa_written,
                        writer_dying: false,
                    })
                    .await;
                }
                JobResult::Flushed { frames, progress } => {
                    if let Err(f) = self.handle_flush(&job, frames, progress, &mut rx).await {
                        // The flush frame could not be written: the peer is gone
                        // mid-flush, which mutes the device anyway, so the cut still
                        // happens. Fail the backlog loudly and die, as any write
                        // failure does.
                        if matches!(f, WriteFail::Timeout) {
                            self.stats.record_write_timeout();
                        }
                        self.drain_aborted(&mut rx, AbortReason::from(f)).await;
                        return;
                    }
                }
                JobResult::Aborted(reason) => {
                    self.stats.record_aborted();
                    (self.events)(PlaybackEvent::Aborted {
                        pod: self.pod.clone(),
                        in_reply_to: job.in_reply_to,
                        reason,
                    })
                    .await;
                    // No EndOfAudio on abort: the device mutes on the disconnect the
                    // failure or cancel implies. Fail the rest of the backlog loudly.
                    self.drain_aborted(&mut rx, reason).await;
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use audio_pipeline::wire::{decode_frame, Codec};
    use tokio::io::{duplex, AsyncReadExt, DuplexStream};

    #[test]
    fn build_audio_frame_pads_short_chunk_with_zeros() {
        let StreamFrame::Audio(f) = build_audio_frame(&[0x1234i16; 10]) else {
            panic!("expected Audio");
        };
        assert_eq!(f.pcm.len(), AUDIO_SAMPLES_PER_FRAME * 2);
        assert_eq!(&f.pcm[..20], [0x34, 0x12].repeat(10).as_slice());
        assert!(f.pcm[20..].iter().all(|&b| b == 0));
    }

    #[test]
    fn build_audio_frame_full_chunk_round_trips() {
        let chunk: Vec<i16> = (0..AUDIO_SAMPLES_PER_FRAME).map(|i| i as i16).collect();
        let StreamFrame::Audio(f) = build_audio_frame(&chunk) else {
            panic!("expected Audio");
        };
        assert_eq!(f.pcm.len(), AUDIO_SAMPLES_PER_FRAME * 2);
        let back: Vec<i16> = f
            .pcm
            .chunks_exact(2)
            .map(|b| i16::from_le_bytes([b[0], b[1]]))
            .collect();
        assert_eq!(back, chunk);
    }

    #[test]
    #[should_panic(expected = "exceeds one frame")]
    fn build_audio_frame_rejects_oversize_chunk() {
        build_audio_frame(&[0i16; AUDIO_SAMPLES_PER_FRAME + 1]);
    }

    /// A `PlaybackEventFn` that collects emitted events for assertion.
    fn event_collector() -> (PlaybackEventFn, Arc<Mutex<Vec<PlaybackEvent>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let f: PlaybackEventFn = Arc::new(move |e| {
            sink.lock().unwrap().push(e);
            Box::pin(std::future::ready(()))
        });
        (f, seen)
    }

    fn job(pcm: Vec<i16>) -> PlaybackJob {
        job_with_id(pcm, 1)
    }

    fn job_with_id(pcm: Vec<i16>, id: u64) -> PlaybackJob {
        PlaybackJob {
            pcm: Arc::from(pcm.as_slice()),
            in_reply_to: Some(UtteranceId(id)),
            interruptible: true,
            timings: StageTimings::default(),
            speak_rx: HostMicros(1_000),
        }
    }

    /// Read every complete length-prefixed frame off `r` until EOF.
    async fn read_all_frames(mut r: DuplexStream) -> Vec<StreamFrame> {
        let mut bytes = Vec::new();
        let mut tmp = [0u8; 4096];
        loop {
            let n = r.read(&mut tmp).await.expect("read");
            if n == 0 {
                break;
            }
            bytes.extend_from_slice(&tmp[..n]);
        }
        decode_all(&bytes)
    }

    /// Decode every complete length-prefixed frame in `bytes`.
    fn decode_all(bytes: &[u8]) -> Vec<StreamFrame> {
        let mut frames = Vec::new();
        let mut pos = 0;
        while pos + 2 <= bytes.len() {
            let len = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
            let end = pos + 2 + len;
            if end > bytes.len() {
                break;
            }
            frames.push(decode_frame(&bytes[pos..end]).expect("decode"));
            pos = end;
        }
        frames
    }

    /// Drive the runtime forward until `cond` holds, yielding to spawned tasks.
    async fn run_until(cond: impl Fn() -> bool) {
        for _ in 0..10_000 {
            if cond() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition not reached");
    }

    #[tokio::test]
    async fn hello_is_first_with_declared_format_and_sentinel_audio_ids() {
        let (dev, host) = duplex(1 << 16);
        let (events, _seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let handle = PlaybackWriter::spawn(
            dev,
            PodId("pod-x".into()),
            PacerConfig::default(),
            Arc::clone(&stats),
            events,
            CancellationToken::new(),
        );
        // Two full frames of audio.
        handle
            .try_play(job(vec![7i16; AUDIO_SAMPLES_PER_FRAME * 2]))
            .expect("queued");
        drop(handle);

        let frames = read_all_frames(host).await;
        match &frames[0] {
            StreamFrame::Hello(h) => {
                assert_eq!(h.version, AUDIO_PROTOCOL_VERSION);
                assert_eq!(h.sample_rate_hz, 16_000);
                assert_eq!(h.bits_per_sample, 16);
                assert_eq!(h.channels, 1);
                assert_eq!(h.codec, Codec::S16Le);
                assert_eq!(h.channel_source, ChannelSource::CommunicationBeam);
            }
            other => panic!("first frame must be Hello, got {other:?}"),
        }
        let audio: Vec<_> = frames
            .iter()
            .filter_map(|f| match f {
                StreamFrame::Audio(a) => Some(a),
                _ => None,
            })
            .collect();
        assert_eq!(audio.len(), 2, "two audio frames written");
        for a in &audio {
            assert_eq!(a.segment_id, 0);
            assert_eq!(a.first_sample_index, 0);
            assert_eq!(a.device_ts_us, 0);
            assert_eq!(a.pcm.len(), AUDIO_SAMPLES_PER_FRAME * 2);
        }
        assert!(matches!(frames.last(), Some(StreamFrame::EndOfAudio(_))));
        assert_eq!(stats.snapshot().frames_written, 2);
        assert_eq!(stats.snapshot().jobs_completed, 1);
        assert_eq!(stats.snapshot().eoa_written, 1);
    }

    #[tokio::test]
    async fn final_partial_frame_is_zero_padded() {
        let (dev, host) = duplex(1 << 16);
        let (events, _seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let handle = PlaybackWriter::spawn(
            dev,
            PodId("pod-x".into()),
            PacerConfig::default(),
            Arc::clone(&stats),
            events,
            CancellationToken::new(),
        );
        // One and a half frames: 320 + 10 samples → two frames, the second padded.
        let mut pcm = vec![5i16; AUDIO_SAMPLES_PER_FRAME];
        pcm.extend(std::iter::repeat_n(9i16, 10));
        handle.try_play(job(pcm)).expect("queued");
        drop(handle);

        let frames = read_all_frames(host).await;
        let audio: Vec<_> = frames
            .iter()
            .filter_map(|f| match f {
                StreamFrame::Audio(a) => Some(a),
                _ => None,
            })
            .collect();
        assert_eq!(audio.len(), 2);
        // Second frame: 10 real samples then zero padding to a full 320-sample frame.
        let last = audio[1];
        assert_eq!(last.pcm.len(), AUDIO_SAMPLES_PER_FRAME * 2);
        for (i, chunk) in last.pcm.chunks_exact(2).enumerate() {
            let s = i16::from_le_bytes([chunk[0], chunk[1]]);
            if i < 10 {
                assert_eq!(s, 9, "real sample {i}");
            } else {
                assert_eq!(s, 0, "padding sample {i}");
            }
        }
    }

    #[tokio::test]
    async fn back_to_back_jobs_share_one_stream_and_single_eoa() {
        let (dev, host) = duplex(1 << 16);
        let (events, _seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let handle = PlaybackWriter::spawn(
            dev,
            PodId("pod-x".into()),
            PacerConfig::default(),
            Arc::clone(&stats),
            events,
            CancellationToken::new(),
        );
        // Both jobs enqueue before the writer's first recv (current-thread runtime,
        // no await between), so the queue is non-empty when job 1 completes.
        handle
            .try_play(job(vec![1i16; AUDIO_SAMPLES_PER_FRAME]))
            .unwrap();
        handle
            .try_play(job(vec![2i16; AUDIO_SAMPLES_PER_FRAME]))
            .unwrap();
        drop(handle);

        let frames = read_all_frames(host).await;
        let eoa = frames
            .iter()
            .filter(|f| matches!(f, StreamFrame::EndOfAudio(_)))
            .count();
        assert_eq!(
            eoa, 1,
            "one EndOfAudio at the single drain, not between jobs"
        );
        // The one EndOfAudio is the final frame — no audio follows it.
        assert!(matches!(frames.last(), Some(StreamFrame::EndOfAudio(_))));
        let audio = frames
            .iter()
            .filter(|f| matches!(f, StreamFrame::Audio(_)))
            .count();
        assert_eq!(audio, 2);
        assert_eq!(stats.snapshot().jobs_completed, 2);
        assert_eq!(stats.snapshot().eoa_written, 1);
    }

    #[tokio::test]
    async fn started_and_finished_events_per_job() {
        let (dev, host) = duplex(1 << 16);
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let handle = PlaybackWriter::spawn(
            dev,
            PodId("pod-x".into()),
            PacerConfig::default(),
            Arc::clone(&stats),
            events,
            CancellationToken::new(),
        );
        handle
            .try_play(job(vec![1i16; AUDIO_SAMPLES_PER_FRAME]))
            .unwrap();
        drop(handle);
        let _ = read_all_frames(host).await;

        let seen = seen.lock().unwrap();
        assert!(matches!(seen[0], PlaybackEvent::HelloWritten { .. }));
        let started = seen
            .iter()
            .find(|e| matches!(e, PlaybackEvent::Started { .. }))
            .expect("Started emitted");
        match started {
            PlaybackEvent::Started {
                in_reply_to,
                samples,
                ..
            } => {
                assert_eq!(*in_reply_to, Some(UtteranceId(1)));
                assert_eq!(*samples, AUDIO_SAMPLES_PER_FRAME as u64);
            }
            _ => unreachable!(),
        }
        let finished = seen
            .iter()
            .find(|e| matches!(e, PlaybackEvent::Finished { .. }))
            .expect("Finished emitted");
        match finished {
            PlaybackEvent::Finished {
                frames,
                eoa_written,
                ..
            } => {
                assert_eq!(*frames, 1);
                assert!(*eoa_written);
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn queue_full_rejects_the_newest_job() {
        // The read half is kept but never drained and is one byte, so the writer
        // parks in its Hello write and never consumes a job — the queue only fills.
        let (_dev_read, host) = duplex(1);
        let cfg = PacerConfig {
            // A large write timeout keeps the parked Hello write from firing during
            // the synchronous try_play calls below.
            write_timeout_ms: 60_000,
            job_queue_depth: 1,
            ..PacerConfig::default()
        };
        let (events, _seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let handle = PlaybackWriter::spawn(
            host,
            PodId("pod-x".into()),
            cfg,
            Arc::clone(&stats),
            events,
            CancellationToken::new(),
        );
        handle
            .try_play(job(vec![0i16]))
            .expect("first fills the queue");
        assert_eq!(
            handle.try_play(job(vec![0i16])),
            Err(PlayRejected::QueueFull),
            "depth-1 queue rejects the second job",
        );
        assert_eq!(stats.snapshot().jobs_rejected_full, 1);
    }

    #[tokio::test]
    async fn write_error_aborts_the_backlog() {
        let (dev_read, host) = duplex(1 << 16);
        drop(dev_read); // peer gone: writes fail.
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let handle = PlaybackWriter::spawn(
            host,
            PodId("pod-x".into()),
            PacerConfig::default(),
            Arc::clone(&stats),
            events,
            CancellationToken::new(),
        );
        handle
            .try_play(job(vec![0i16; AUDIO_SAMPLES_PER_FRAME]))
            .unwrap();
        drop(handle);

        run_until(|| stats.snapshot().jobs_aborted > 0).await;
        let seen = seen.lock().unwrap();
        assert!(
            seen.iter().any(|e| matches!(
                e,
                PlaybackEvent::Aborted {
                    reason: AbortReason::WriteError,
                    ..
                }
            )),
            "a write error aborts with WriteError",
        );
    }

    #[tokio::test]
    async fn cancellation_aborts_with_cancelled() {
        let (dev, _host) = duplex(1 << 16);
        let cancel = CancellationToken::new();
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let handle = PlaybackWriter::spawn(
            dev,
            PodId("pod-x".into()),
            PacerConfig::default(),
            Arc::clone(&stats),
            events,
            cancel.clone(),
        );
        // A long clip parks the writer in the pacer; cancelling interrupts it.
        handle
            .try_play(job(vec![0i16; AUDIO_SAMPLES_PER_FRAME * 200]))
            .unwrap();
        cancel.cancel();

        run_until(|| stats.snapshot().jobs_aborted > 0).await;
        let seen = seen.lock().unwrap();
        assert!(
            seen.iter().any(|e| matches!(
                e,
                PlaybackEvent::Aborted {
                    reason: AbortReason::Cancelled,
                    ..
                }
            )),
            "cancellation aborts with Cancelled",
        );
        assert_eq!(stats.snapshot().eoa_written, 0, "no EndOfAudio on cancel");
    }

    #[tokio::test(start_paused = true)]
    async fn pacing_keeps_audio_within_lead_of_real_time() {
        let (dev, mut host) = duplex(1 << 16);
        let lead_ms = 250u64;
        let cfg = PacerConfig {
            lead_ms,
            write_timeout_ms: 60_000,
            job_queue_depth: 1,
        };
        let (events, _seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());

        // A reader draining to EOF keeps the writer off socket backpressure, so the
        // only thing moving the (paused) clock is the pacer's own sleeps.
        let start = Instant::now();
        let reader = tokio::spawn(async move {
            let mut tmp = [0u8; 4096];
            while host.read(&mut tmp).await.expect("read") != 0 {}
        });

        let handle = PlaybackWriter::spawn(
            dev,
            PodId("pod-x".into()),
            cfg,
            Arc::clone(&stats),
            events,
            CancellationToken::new(),
        );
        // 60 frames = 1.2 s of audio, well past the 250 ms lead.
        let n_frames = 60usize;
        handle
            .try_play(job(vec![0i16; AUDIO_SAMPLES_PER_FRAME * n_frames]))
            .unwrap();
        drop(handle);

        reader.await.expect("reader");
        // The whole stream drained under paused time: total wall time advanced is the
        // pacer's, which must be at least (audio − lead − one frame) and at most the
        // full audio duration. This bounds the aggregate pacing without per-frame
        // plumbing: had the pacer free-run, elapsed would be ~0; had it lagged,
        // elapsed would exceed the audio duration.
        let elapsed_ms = Instant::now().duration_since(start).as_millis() as u64;
        let audio_ms = n_frames as u64 * FRAME_MS;
        assert!(
            elapsed_ms + lead_ms + FRAME_MS >= audio_ms,
            "paced too slow: elapsed {elapsed_ms} ms, audio {audio_ms} ms",
        );
        assert!(
            elapsed_ms <= audio_ms,
            "paced ahead of real time: elapsed {elapsed_ms} ms, audio {audio_ms} ms",
        );
        assert_eq!(stats.snapshot().frames_written, n_frames as u64);
    }

    #[tokio::test(start_paused = true)]
    async fn write_timeout_aborts_the_job() {
        // Small buffer, no reader: the Hello fits but the first Audio frame's write
        // parks for capacity, then the per-write timeout fires under paused time.
        let (_dev_read, host) = duplex(128);
        let cfg = PacerConfig {
            write_timeout_ms: 1_000,
            ..PacerConfig::default()
        };
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let handle = PlaybackWriter::spawn(
            host,
            PodId("pod-x".into()),
            cfg,
            Arc::clone(&stats),
            events,
            CancellationToken::new(),
        );
        handle
            .try_play(job(vec![0i16; AUDIO_SAMPLES_PER_FRAME]))
            .unwrap();
        drop(handle);

        // Let the writer send Hello and park in the audio write, then step past the
        // per-write timeout.
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(1_001)).await;
        run_until(|| stats.snapshot().jobs_aborted > 0).await;

        assert_eq!(stats.snapshot().write_timeouts, 1);
        let seen = seen.lock().unwrap();
        assert!(
            seen.iter().any(|e| matches!(
                e,
                PlaybackEvent::Aborted {
                    reason: AbortReason::WriteTimeout,
                    ..
                }
            )),
            "a stalled write aborts with WriteTimeout",
        );
    }

    #[tokio::test]
    async fn try_play_after_writer_death_is_writer_dead() {
        // Cancel drives the writer task to exit; a later enqueue must be rejected as
        // WriterDead (not silently accepted into a reader-less channel) and counted.
        let (dev, _host) = duplex(1 << 16);
        let cancel = CancellationToken::new();
        let (events, _seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let handle = PlaybackWriter::spawn(
            dev,
            PodId("pod-x".into()),
            PacerConfig::default(),
            Arc::clone(&stats),
            events,
            cancel.clone(),
        );
        cancel.cancel();
        run_until(|| handle.try_play(job(vec![0i16])) == Err(PlayRejected::WriterDead)).await;
        assert_eq!(stats.snapshot().jobs_rejected_dead, 1);
    }

    #[tokio::test]
    async fn abort_drains_the_whole_backlog_with_per_job_events() {
        // Peer gone: the eager Hello write fails, and drain_aborted must abort every
        // job still queued — not just one — each with its own in_reply_to.
        let (dev_read, host) = duplex(1 << 16);
        drop(dev_read);
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let handle = PlaybackWriter::spawn(
            host,
            PodId("pod-x".into()),
            PacerConfig::default(),
            Arc::clone(&stats),
            events,
            CancellationToken::new(),
        );
        handle
            .try_play(job_with_id(vec![0i16; AUDIO_SAMPLES_PER_FRAME], 10))
            .unwrap();
        handle
            .try_play(job_with_id(vec![0i16; AUDIO_SAMPLES_PER_FRAME], 11))
            .unwrap();
        drop(handle);

        run_until(|| stats.snapshot().jobs_aborted >= 2).await;
        let seen = seen.lock().unwrap();
        let aborted: Vec<_> = seen
            .iter()
            .filter_map(|e| match e {
                PlaybackEvent::Aborted { in_reply_to, .. } => Some(*in_reply_to),
                _ => None,
            })
            .collect();
        assert!(aborted.contains(&Some(UtteranceId(10))));
        assert!(aborted.contains(&Some(UtteranceId(11))));
        assert_eq!(stats.snapshot().jobs_aborted, 2);
        assert!(
            seen.iter()
                .any(|e| matches!(e, PlaybackEvent::HelloFailed { .. })),
            "the failed eager Hello is reported",
        );
    }

    /// Every `Flushed` event seen, as `(utterance, was_playing, progress)`.
    fn flushed_events(
        seen: &Arc<Mutex<Vec<PlaybackEvent>>>,
    ) -> Vec<(Option<UtteranceId>, bool, InterruptProgress)> {
        seen.lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                PlaybackEvent::Flushed {
                    in_reply_to,
                    was_playing,
                    progress,
                    ..
                } => Some((*in_reply_to, *was_playing, *progress)),
                _ => None,
            })
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn flush_cuts_the_playing_job_and_keeps_the_writer_alive() {
        // A long clip parks the writer in the pacer; the flush must cut it, put
        // FlushPlayback (and no EndOfAudio) on the wire, and leave the writer able
        // to play the next turn's response.
        let (dev, mut host) = duplex(1 << 16);
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let handle = PlaybackWriter::spawn(
            dev,
            PodId("pod-x".into()),
            PacerConfig {
                lead_ms: 100,
                ..PacerConfig::default()
            },
            Arc::clone(&stats),
            events,
            CancellationToken::new(),
        );
        // A reader keeps the writer off socket backpressure.
        let read = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&read);
        let reader = tokio::spawn(async move {
            let mut tmp = [0u8; 4096];
            while let Ok(n) = host.read(&mut tmp).await {
                if n == 0 {
                    break;
                }
                sink.lock().unwrap().extend_from_slice(&tmp[..n]);
            }
        });

        // 5 s of audio: far more than the writer can bank at a 100 ms lead.
        handle
            .try_play(job_with_id(vec![3i16; AUDIO_SAMPLES_PER_FRAME * 250], 42))
            .unwrap();
        run_until(|| stats.snapshot().frames_written > 0).await;
        // Advance into the clip so the heard estimate is non-trivial.
        tokio::time::advance(Duration::from_millis(200)).await;
        run_until(|| stats.snapshot().frames_written >= 10).await;

        let progress = handle.flush(UtteranceId(42)).expect("playing turn flushes");
        assert!(
            progress.heard_ms > 0 && progress.heard_ms <= 300,
            "heard {} ms is within the advanced time plus the lead",
            progress.heard_ms,
        );
        assert_eq!(progress.total_ms, 5_000, "clip is 5 s of audio");

        run_until(|| stats.snapshot().jobs_flushed > 0).await;
        assert_eq!(
            flushed_events(&seen),
            vec![(Some(UtteranceId(42)), true, progress)],
            "only the playing job is flushed, carrying its progress",
        );
        assert_eq!(
            stats.snapshot().jobs_completed,
            0,
            "the clip never completed"
        );

        // The writer lives: a job for the next turn still plays.
        handle
            .try_play(job_with_id(vec![7i16; AUDIO_SAMPLES_PER_FRAME], 43))
            .expect("writer still accepts jobs after a flush");
        run_until(|| stats.snapshot().jobs_completed > 0).await;
        drop(handle);
        reader.await.expect("reader");

        let bytes = read.lock().unwrap().clone();
        let frames = decode_all(&bytes);
        let flush_at = frames
            .iter()
            .position(|f| matches!(f, StreamFrame::FlushPlayback(_)))
            .expect("FlushPlayback written");
        assert!(
            frames[..flush_at]
                .iter()
                .all(|f| !matches!(f, StreamFrame::EndOfAudio(_))),
            "no EndOfAudio precedes the flush: the flush itself ends the stream",
        );
        assert!(
            matches!(frames[flush_at + 1], StreamFrame::Audio(_)),
            "the next turn's audio follows the flush on the same connection",
        );
        assert!(
            matches!(frames.last(), Some(StreamFrame::EndOfAudio(_))),
            "the next turn drains normally with its own EndOfAudio",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn flush_evicts_same_turn_queue_and_keeps_other_turns() {
        // Three jobs: the playing one and a queued one for the flushed turn, plus a
        // newer turn's job that must survive and play afterward.
        let (dev, mut host) = duplex(1 << 16);
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let handle = PlaybackWriter::spawn(
            dev,
            PodId("pod-x".into()),
            PacerConfig {
                lead_ms: 100,
                job_queue_depth: 4,
                ..PacerConfig::default()
            },
            Arc::clone(&stats),
            events,
            CancellationToken::new(),
        );
        let reader = tokio::spawn(async move {
            let mut tmp = [0u8; 4096];
            while host.read(&mut tmp).await.expect("read") != 0 {}
        });

        handle
            .try_play(job_with_id(vec![1i16; AUDIO_SAMPLES_PER_FRAME * 250], 42))
            .unwrap();
        handle
            .try_play(job_with_id(vec![2i16; AUDIO_SAMPLES_PER_FRAME], 42))
            .unwrap();
        handle
            .try_play(job_with_id(vec![3i16; AUDIO_SAMPLES_PER_FRAME], 99))
            .unwrap();
        run_until(|| stats.snapshot().frames_written > 0).await;

        handle.flush(UtteranceId(42)).expect("flushed");
        // The newer turn's job completes, proving it was not evicted.
        run_until(|| stats.snapshot().jobs_completed > 0).await;
        drop(handle);
        reader.await.expect("reader");

        let flushed = flushed_events(&seen);
        assert_eq!(flushed.len(), 2, "both of turn 42's jobs are flushed");
        assert!(flushed[0].1, "the playing job reports was_playing");
        assert_eq!(
            (flushed[1].0, flushed[1].1, flushed[1].2.heard_ms),
            (Some(UtteranceId(42)), false, 0),
            "the evicted queued job was never audible",
        );
        assert_eq!(stats.snapshot().jobs_flushed, 2);
        assert_eq!(
            stats.snapshot().jobs_completed,
            1,
            "turn 99's job played to completion",
        );
        let finished: Vec<_> = seen
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                PlaybackEvent::Finished { in_reply_to, .. } => Some(*in_reply_to),
                _ => None,
            })
            .collect();
        assert_eq!(finished, vec![Some(UtteranceId(99))]);
    }

    #[tokio::test(start_paused = true)]
    async fn flush_rejections_are_side_effect_free() {
        let (dev, mut host) = duplex(1 << 16);
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let handle = PlaybackWriter::spawn(
            dev,
            PodId("pod-x".into()),
            PacerConfig {
                lead_ms: 100,
                ..PacerConfig::default()
            },
            Arc::clone(&stats),
            events,
            CancellationToken::new(),
        );
        let reader = tokio::spawn(async move {
            let mut tmp = [0u8; 4096];
            while host.read(&mut tmp).await.expect("read") != 0 {}
        });

        // Nothing playing yet: no job is current.
        assert_eq!(handle.flush(UtteranceId(1)), Err(FlushRejected::NotPlaying));
        assert_eq!(handle.current_turn(), None);

        // A non-interruptible job (an alert) refuses the flush.
        let mut alert = job_with_id(vec![0i16; AUDIO_SAMPLES_PER_FRAME * 250], 7);
        alert.interruptible = false;
        handle.try_play(alert).unwrap();
        run_until(|| stats.snapshot().frames_written > 0).await;
        assert_eq!(handle.current_turn(), Some(UtteranceId(7)));
        assert_eq!(
            handle.flush(UtteranceId(7)),
            Err(FlushRejected::NotInterruptible),
        );
        // A turn that is not the playing one never cuts the playing response.
        assert_eq!(handle.flush(UtteranceId(8)), Err(FlushRejected::WrongTurn));

        // Every rejection left the playback untouched.
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        assert!(
            flushed_events(&seen).is_empty(),
            "no flush event was emitted"
        );
        assert_eq!(stats.snapshot().jobs_flushed, 0);

        // A dead writer rejects everything.
        let cancel = CancellationToken::new();
        let (dev2, _host2) = duplex(1 << 16);
        let (events2, _seen2) = event_collector();
        let dead = PlaybackWriter::spawn(
            dev2,
            PodId("pod-y".into()),
            PacerConfig::default(),
            Arc::new(PlaybackStats::default()),
            events2,
            cancel.clone(),
        );
        cancel.cancel();
        run_until(|| dead.flush(UtteranceId(1)) == Err(FlushRejected::WriterDead)).await;

        drop(handle);
        reader.await.expect("reader");
    }

    #[tokio::test(start_paused = true)]
    async fn flush_for_a_finished_turn_is_a_no_op() {
        // The signal names a turn whose job already ended: the writer must drop it
        // rather than cut whatever plays next.
        let (dev, mut host) = duplex(1 << 16);
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let handle = PlaybackWriter::spawn(
            dev,
            PodId("pod-x".into()),
            PacerConfig {
                lead_ms: 100,
                job_queue_depth: 4,
                ..PacerConfig::default()
            },
            Arc::clone(&stats),
            events,
            CancellationToken::new(),
        );
        let reader = tokio::spawn(async move {
            let mut tmp = [0u8; 4096];
            while host.read(&mut tmp).await.expect("read") != 0 {}
        });

        // Turn 42's job plays and finishes.
        handle
            .try_play(job_with_id(vec![1i16; AUDIO_SAMPLES_PER_FRAME], 42))
            .unwrap();
        run_until(|| stats.snapshot().jobs_completed == 1).await;
        // Its flush now arrives late — the turn is over and nothing is current.
        assert_eq!(
            handle.flush(UtteranceId(42)),
            Err(FlushRejected::NotPlaying)
        );

        // Turn 43's long job must play unharmed.
        handle
            .try_play(job_with_id(vec![2i16; AUDIO_SAMPLES_PER_FRAME * 5], 43))
            .unwrap();
        run_until(|| stats.snapshot().jobs_completed == 2).await;
        drop(handle);
        reader.await.expect("reader");

        assert!(
            flushed_events(&seen).is_empty(),
            "a flush for a finished turn cuts nothing",
        );
        assert_eq!(stats.snapshot().jobs_flushed, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn heard_ms_is_capped_by_frames_written() {
        // The pacer front-loads up to `lead_ms`, so frames written run ahead of
        // audible audio; before the first frame lands nothing is heard at all.
        let (dev, mut host) = duplex(1 << 16);
        let (events, _seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let lead_ms = 1_000u64;
        let handle = PlaybackWriter::spawn(
            dev,
            PodId("pod-x".into()),
            PacerConfig {
                lead_ms,
                ..PacerConfig::default()
            },
            Arc::clone(&stats),
            events,
            CancellationToken::new(),
        );
        let reader = tokio::spawn(async move {
            let mut tmp = [0u8; 4096];
            while host.read(&mut tmp).await.expect("read") != 0 {}
        });

        handle
            .try_play(job_with_id(vec![0i16; AUDIO_SAMPLES_PER_FRAME * 250], 42))
            .unwrap();
        // The writer banks a full lead's worth of frames without time advancing.
        run_until(|| stats.snapshot().frames_written * FRAME_MS >= lead_ms).await;
        let progress = handle.flush(UtteranceId(42)).expect("flushed");
        assert_eq!(
            progress.heard_ms,
            0,
            "no wall time has passed, so nothing is heard despite {} banked frames",
            stats.snapshot().frames_written,
        );
        drop(handle);
        reader.await.expect("reader");
    }

    #[tokio::test(start_paused = true)]
    async fn eoa_write_failure_reports_finished_without_eoa_and_drains_racer() {
        // Size the pipe to hold Hello + one audio frame but not the trailing
        // EndOfAudio: the audio write succeeds, then the EndOfAudio write parks (no
        // reader) until it times out. A second job that races into the queue during
        // that window must be aborted loudly, not dropped on the receiver teardown.
        let mut sizing = [0u8; MAX_FRAME_BYTES + 2];
        let hello = StreamFrame::Hello(Hello {
            version: AUDIO_PROTOCOL_VERSION,
            pod_id: HString::try_from(SENDER_POD_ID).unwrap(),
            sample_rate_hz: SPINE_FORMAT.sample_rate_hz,
            bits_per_sample: SPINE_FORMAT.bits_per_sample,
            channels: SPINE_FORMAT.channels,
            codec: SPINE_FORMAT.codec,
            channel_source: ChannelSource::CommunicationBeam,
        });
        let hello_len = encode_frame(&hello, &mut sizing).unwrap();
        let audio = build_audio_frame(&[0i16; AUDIO_SAMPLES_PER_FRAME]);
        let audio_len = encode_frame(&audio, &mut sizing).unwrap();

        let (_dev_read, host) = duplex(hello_len + audio_len);
        let cfg = PacerConfig {
            write_timeout_ms: 1_000,
            job_queue_depth: 2,
            ..PacerConfig::default()
        };
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let handle = PlaybackWriter::spawn(
            host,
            PodId("pod-x".into()),
            cfg,
            Arc::clone(&stats),
            events,
            CancellationToken::new(),
        );
        handle
            .try_play(job_with_id(vec![0i16; AUDIO_SAMPLES_PER_FRAME], 10))
            .unwrap();
        // Let the writer send Hello + audio and park in the EndOfAudio write.
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        // The racer arrives while the EndOfAudio write is parked.
        handle
            .try_play(job_with_id(vec![0i16; AUDIO_SAMPLES_PER_FRAME], 11))
            .unwrap();
        drop(handle);
        tokio::time::advance(Duration::from_millis(1_001)).await;
        run_until(|| stats.snapshot().jobs_aborted > 0).await;

        let seen = seen.lock().unwrap();
        assert!(
            seen.iter().any(|e| matches!(
                e,
                PlaybackEvent::Finished {
                    in_reply_to: Some(UtteranceId(10)),
                    eoa_written: false,
                    ..
                }
            )),
            "the completed job is Finished with eoa_written:false",
        );
        assert!(
            seen.iter().any(|e| matches!(
                e,
                PlaybackEvent::Aborted {
                    in_reply_to: Some(UtteranceId(11)),
                    reason: AbortReason::WriteTimeout,
                    ..
                }
            )),
            "the raced-in job is aborted, not silently dropped",
        );
        let snap = stats.snapshot();
        assert_eq!(snap.jobs_completed, 1);
        assert_eq!(snap.eoa_written, 0);
        assert_eq!(snap.jobs_aborted, 1);
        assert_eq!(snap.write_timeouts, 1);
        assert_eq!(snap.eoa_write_failures, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn flush_write_failure_drains_the_backlog_and_kills_the_writer() {
        // The flush frame cannot be written — the peer is gone mid-flush. Size the
        // pipe to hold Hello plus the pacer's whole initial burst but nothing more,
        // with no reader draining it, so the audio frames succeed but the trailing
        // `FlushPlayback` parks with no room and times out. The playing job's
        // `Flushed` fires before the write is attempted; a deferred other-turn job
        // must then be aborted loudly rather than dropped on the receiver teardown.
        let mut sizing = [0u8; MAX_FRAME_BYTES + 2];
        let hello = StreamFrame::Hello(Hello {
            version: AUDIO_PROTOCOL_VERSION,
            pod_id: HString::try_from(SENDER_POD_ID).unwrap(),
            sample_rate_hz: SPINE_FORMAT.sample_rate_hz,
            bits_per_sample: SPINE_FORMAT.bits_per_sample,
            channels: SPINE_FORMAT.channels,
            codec: SPINE_FORMAT.codec,
            channel_source: ChannelSource::CommunicationBeam,
        });
        let hello_len = encode_frame(&hello, &mut sizing).unwrap();
        let audio = build_audio_frame(&[0i16; AUDIO_SAMPLES_PER_FRAME]);
        let audio_len = encode_frame(&audio, &mut sizing).unwrap();

        // At a 100 ms lead the pacer banks frames while `frames_in_stream * 20 ms`
        // stays ≤ 100 ms — six frames (indices 0..=5) — then parks before the
        // seventh. Sizing the pipe to Hello + those six frames leaves no room for
        // the `FlushPlayback` that the flush then tries to write.
        let lead_ms = 100u64;
        let burst = lead_ms / FRAME_MS + 1;
        let (_dev_read, host) = duplex(hello_len + burst as usize * audio_len);
        let cfg = PacerConfig {
            lead_ms,
            write_timeout_ms: 1_000,
            job_queue_depth: 2,
        };
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let handle = PlaybackWriter::spawn(
            host,
            PodId("pod-x".into()),
            cfg,
            Arc::clone(&stats),
            events,
            CancellationToken::new(),
        );
        // The playing turn (long, so it is mid-play and current when the flush
        // lands) and a queued job for a different turn behind it.
        handle
            .try_play(job_with_id(vec![1i16; AUDIO_SAMPLES_PER_FRAME * 250], 42))
            .unwrap();
        handle
            .try_play(job_with_id(vec![2i16; AUDIO_SAMPLES_PER_FRAME], 99))
            .unwrap();
        // The writer banks its burst and parks in the pacer with the pipe full.
        run_until(|| stats.snapshot().frames_written >= burst).await;

        handle
            .flush(UtteranceId(42))
            .expect("the playing turn flushes");
        // Let the writer take the flush, emit the playing job's `Flushed`, defer the
        // other turn, and park in the `FlushPlayback` write against the full pipe.
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(1_001)).await;
        run_until(|| stats.snapshot().jobs_aborted > 0).await;

        {
            let seen = seen.lock().unwrap();
            assert!(
                seen.iter().any(|e| matches!(
                    e,
                    PlaybackEvent::Flushed {
                        in_reply_to: Some(UtteranceId(42)),
                        was_playing: true,
                        ..
                    }
                )),
                "the playing job's Flushed fires before the write is attempted",
            );
            assert!(
                seen.iter().any(|e| matches!(
                    e,
                    PlaybackEvent::Aborted {
                        in_reply_to: Some(UtteranceId(99)),
                        reason: AbortReason::WriteTimeout,
                        ..
                    }
                )),
                "the deferred other-turn job is aborted, not silently dropped",
            );
        }
        let snap = stats.snapshot();
        assert_eq!(snap.jobs_flushed, 1, "only the playing job was flushed");
        assert_eq!(snap.jobs_aborted, 1);
        assert_eq!(snap.write_timeouts, 1);

        // The writer died on the failed flush write: it accepts nothing further.
        run_until(|| handle.try_play(job(vec![0i16])) == Err(PlayRejected::WriterDead)).await;
    }

    #[tokio::test]
    async fn take_flush_for_drops_a_signal_naming_another_turn() {
        // The writer-side re-check: a flush signal could race a job boundary and end
        // up naming a turn other than the one now playing. Such a signal is dropped
        // (never taken) and cleared, so it cannot later cut the wrong response.
        let (dev, _host) = duplex(1 << 16);
        let writer = Writer {
            io: dev,
            pod: PodId("pod-x".into()),
            cfg: PacerConfig::default(),
            stats: Arc::new(PlaybackStats::default()),
            events: Arc::new(|_| Box::pin(std::future::ready(()))),
            cancel: CancellationToken::new(),
            progress: Arc::new(JobProgress::default()),
            flush_signal: Arc::new(FlushSignal::default()),
            buf: [0u8; MAX_FRAME_BYTES + 2],
            anchor: None,
            frames_in_stream: 0,
            deferred: VecDeque::new(),
        };

        *writer.flush_signal.target.lock().unwrap() = Some(UtteranceId(2));
        assert!(
            !writer.take_flush_for(Some(UtteranceId(1))),
            "a signal for another turn is not taken",
        );
        assert!(
            writer.flush_signal.target.lock().unwrap().is_none(),
            "the stale target is cleared, not left to fire on a later job",
        );

        // A signal naming the current turn is taken and consumed.
        *writer.flush_signal.target.lock().unwrap() = Some(UtteranceId(5));
        assert!(writer.take_flush_for(Some(UtteranceId(5))));
        assert!(writer.flush_signal.target.lock().unwrap().is_none());
    }
}
