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
//! The pacer owns the stream clock, so it is the one place that knows when a job
//! has finished *playing* rather than finished being written: it keeps every job
//! from its first write until its audible end in `pending`, reports the last write
//! as `Written` and the audible end as `Finished`, and treats the front of
//! `pending` — the job audible now — as the one a barge-in flush cuts. That front
//! is also what the listener's playback floor follows, reported as `Audible`
//! whenever it changes, because no per-job event can name a per-pod state once
//! several jobs share one stream.
//!
//! Generic over `AsyncWrite`: production passes the connection's write half; tests
//! pass a `tokio::io::duplex` fake device.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use audio_pipeline::wire::{
    AUDIO_PROTOCOL_VERSION, AUDIO_SAMPLES_PER_FRAME, AudioFrame, ChannelSource, EndOfAudio,
    FlushPlayback, Hello, MAX_AUDIO_PAYLOAD, MAX_FRAME_BYTES, StreamFrame, encode_frame,
};
use futures::future::BoxFuture;
use heapless::{String as HString, Vec as HVec};
use pod_ingest::HostMicros;
use serde::Serialize;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::types::{InterruptProgress, PodId, SPINE_FORMAT, StageTimings, UtteranceId};

/// Wall-clock duration of one `Audio` frame. One `AUDIO_SAMPLES_PER_FRAME` chunk at
/// 16 kHz is 20 ms; the assert ties the constant to the frame size so a frame-size
/// change cannot silently desynchronize the pacer. Public so the surface's
/// `lead_ms` floor validates against this single guarded source, not a copied literal.
pub const FRAME_MS: u64 = 20;
const _: () = assert!(
    AUDIO_SAMPLES_PER_FRAME as u64 * 1000 == FRAME_MS * SPINE_FORMAT.sample_rate_hz as u64,
    "FRAME_MS must equal one AUDIO_SAMPLES_PER_FRAME at the spine sample rate",
);

/// How long `samples` of spine-format audio plays for, in milliseconds. Nominal:
/// the count is what the job carries, not what a device has emitted, so this is a
/// duration of audio rather than a wall span. The one conversion from a sample
/// count to a playout duration.
pub const fn audio_ms(samples: u64) -> u64 {
    samples * 1000 / SPINE_FORMAT.sample_rate_hz as u64
}

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
    /// A job's audio has all been handed to the device. `eoa_written` is true when
    /// this job drained the stream and an `EndOfAudio` followed it. Not terminal:
    /// up to the pacer's lead of this job is still banked on the device, and the
    /// `Finished` below is when the last of it is heard.
    Written {
        pod: PodId,
        in_reply_to: Option<UtteranceId>,
        frames: u64,
        samples: u64,
        eoa_written: bool,
        /// Whether this pass re-wrote a job the stream already carried before a
        /// flush discarded the device's bank. Such a job repeats no `Started`, so
        /// without this a reader cannot tell one reply written twice from two
        /// replies, and the counters cannot separate write passes from answers.
        rewritten: bool,
        /// The pacer's estimate of when the last of this job is heard: the stream
        /// anchor, the device's playout hop, and every frame written on this stream
        /// so far. A lower bound, for the reasons
        /// `audio_pipeline::playback::PLAYBACK_PLAYOUT_HOP_MS` gives.
        plays_until: Instant,
    },
    /// A job's audio has been heard to its end — its `plays_until` has passed.
    /// Terminal and clean: the ledger settles on it, and a job that played out is a
    /// clean completion whether or not it drained the stream (a clip finishing with
    /// another queued behind it writes no `EndOfAudio` yet delivered all its audio).
    /// A job whose stream-drain `EndOfAudio` write failed never reaches this: it
    /// gets `Written` and then `Aborted`, because the host has lost the stream and
    /// cannot know whether the tail was heard — the device plays out its bank on a
    /// dropped connection and discards it on a reconnect — so it reports what it
    /// saw, which is that it did not see the job through.
    Finished {
        pod: PodId,
        in_reply_to: Option<UtteranceId>,
        frames: u64,
        samples: u64,
        eoa_written: bool,
    },
    /// What the pod is audibly playing changed: the turn and interruptibility of
    /// the job at the front of the writer's `pending`, or nothing when the device
    /// holds no audio at all. Emitted when that value changes and only then — a
    /// first write onto an empty stream, a front retiring to a job of another turn
    /// banked behind it, the stream emptying (heard out, flushed, or aborted), and
    /// a re-deferred job's re-write onto the fresh stream after a flush. A front
    /// retiring to the next clip of the *same* turn is not a change.
    ///
    /// This is what the listener's playback floor follows, and the per-job
    /// lifecycle events are not: the floor is one state per pod, the lifecycle
    /// events interleave across the jobs sharing a stream, and a job re-written
    /// after a flush repeats none of them. Emitted after whichever lifecycle event
    /// caused the change.
    Audible { pod: PodId, job: Option<AudibleJob> },
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

/// The audible job [`PlaybackEvent::Audible`] names. Only the fields the floor
/// carries: the identity is the payload, so two clips of one reply hand over with
/// no event and the barge guard's sustain run survives the boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudibleJob {
    /// The utterance the audible job answers, if any.
    pub in_reply_to: Option<UtteranceId>,
    /// Whether speech over it may flush it.
    pub interruptible: bool,
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
    jobs_rewritten: AtomicU64,
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
    /// Write passes whose audio was fully written. A job re-written after a flush
    /// counts once per pass, so `jobs_completed - jobs_rewritten` is the number of
    /// distinct answers that reached the device whole.
    pub jobs_completed: u64,
    /// Write passes that re-wrote a job whose banked frames a flush discarded.
    pub jobs_rewritten: u64,
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
    fn record_rewritten(&self) {
        self.jobs_rewritten.fetch_add(1, Ordering::Relaxed);
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
            jobs_rewritten: self.jobs_rewritten.load(Ordering::Relaxed),
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

/// The job audible right now — the front of the writer's `pending` queue — as the
/// flush path needs to see it.
#[derive(Debug, Clone, Copy)]
struct CurrentJob {
    turn: Option<UtteranceId>,
    interruptible: bool,
    total_samples: u64,
    /// The pacer's estimate of when this job's first sample is heard: the stream
    /// anchor plus the device's playout hop plus the audio banked ahead of it. The
    /// origin the heard estimate measures from, and in the future while the device
    /// is still working through the hop.
    starts_at: Instant,
}

impl CurrentJob {
    /// The heard/total estimate for this job right now. `heard_ms` runs from the
    /// job's audible start, so it is zero while the device is still banking the
    /// playout hop and never counts audio the pacer has only written; it is capped
    /// at the clip's own length, which the estimate can otherwise exceed by however
    /// late the job's retirement was serviced.
    fn progress(&self) -> InterruptProgress {
        let total_ms = audio_ms(self.total_samples);
        let heard_ms = Instant::now()
            .saturating_duration_since(self.starts_at)
            .as_millis() as u64;
        InterruptProgress {
            heard_ms: heard_ms.min(total_ms),
            total_ms,
        }
    }
}

/// The audible job, readable by the flush path. `pending` is its only writer: set
/// when a job becomes the front, handed over when the front retires, cleared when
/// `pending` empties. Read under the mutex by [`PlaybackHandle::flush`].
#[derive(Debug, Default)]
struct JobProgress {
    /// `None` when nothing is banked on the device.
    current: Mutex<Option<CurrentJob>>,
}

/// One job on the stream, from its first write until its audible end.
struct Pending {
    /// The job itself, PCM included: a job whose frames the device has not yet
    /// played can be written again after a flush discards the device's bank, so
    /// the writer holds at most the pacer's lead of audio per pod here.
    job: PlaybackJob,
    /// Frames of this job written so far.
    frames: u64,
    /// The whole clip's sample count, however much of it has been written.
    samples: u64,
    /// Whether an `EndOfAudio` followed this job's last frame. Meaningful only
    /// once `ends_at` is set.
    eoa_written: bool,
    /// When this job's first sample is heard.
    starts_at: Instant,
    /// When its last sample is heard; `None` until its last frame is written, so
    /// only the back of `pending` can lack it.
    ends_at: Option<Instant>,
}

impl Pending {
    /// This job as the flush path sees it.
    fn as_current(&self) -> CurrentJob {
        CurrentJob {
            turn: self.job.in_reply_to,
            interruptible: self.job.interruptible,
            total_samples: self.samples,
            starts_at: self.starts_at,
        }
    }
}

/// A job waiting to be written again after a flush took the stream out from under
/// it, or evicted from the queue by one.
struct Deferred {
    job: PlaybackJob,
    /// Whether this job already reported a `Started`. True for a job the flush
    /// pulled back off the stream: it had written frames, so its first write is
    /// already dated and already counted, and the device discarding those frames
    /// unheard does not make it a second reply.
    started: bool,
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
    /// Nothing is audible right now — no job's audio is banked on the device.
    NotPlaying,
    /// A different turn's job is the audible one (the named turn has played out).
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

    /// The turn whose job is audible right now, or `None` when nothing is banked
    /// on the device (and for a job with no originating utterance). The key a
    /// caller passes to [`flush`].
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
    /// Returns the progress snapshot when `turn` names the audible interruptible
    /// job — the writer will cut it, evict any queued jobs for the same turn, and
    /// send `FlushPlayback` on the wire, after which the device discards its banked
    /// audio and mutes. Any later job of another turn already on the stream is
    /// written again on the fresh stream, none of it having been heard. Every other
    /// case is a `FlushRejected` with no side effects: a stale interrupt (the
    /// turn has played out, or another turn is audible) is a no-op by
    /// construction, never a flush of the wrong response.
    ///
    /// Audible, not being written: a barge arriving in the last of a reply — after
    /// its final frame, while up to the pacer's lead of it is still coming out of
    /// the speaker — cuts it, which is when a listener who has heard enough is
    /// most likely to speak.
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
        let progress = job.progress();
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
            pending: VecDeque::new(),
            deferred: VecDeque::new(),
            queue_closed: false,
            last_audible: None,
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
    /// when `pending` empties or a flush discards the device's bank, so back-to-back
    /// jobs — including one arriving while the previous one's tail is still audible
    /// — ride one continuous clock instead of bursting a second lead on top of
    /// audio the device has not played.
    anchor: Option<Instant>,
    /// Frames written since the current stream's anchor.
    frames_in_stream: u64,
    /// Every job on the stream, oldest first: each from its first write until its
    /// audible end. The front is the job audible now (and the one a flush cuts);
    /// the back is the job being written. Empty means nothing is banked.
    pending: VecDeque<Pending>,
    /// Jobs pulled off the queue or the stream during a flush's selective eviction
    /// but belonging to another turn. Served before the queue so their order is
    /// preserved.
    deferred: VecDeque<Deferred>,
    /// The job queue has closed (every sender dropped) and been drained. The writer
    /// stays alive past it until `pending` is heard out.
    queue_closed: bool,
    /// The audible job as last reported by [`PlaybackEvent::Audible`]. Compared
    /// against the front of `pending` to emit on changes only.
    last_audible: Option<AudibleJob>,
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

/// Outcome of writing one job's audio. `frames` is how many of its frames went
/// out, which is also whether it reached `pending`: a job is on the stream from its
/// first successful write.
enum JobResult {
    Completed {
        frames: u64,
    },
    Aborted {
        reason: AbortReason,
        frames: u64,
    },
    /// A flush for `turn` — the audible job's turn, which may be an earlier job's —
    /// arrived while this one was being written. The writer stays alive.
    Flushed {
        turn: Option<UtteranceId>,
        frames: u64,
    },
}

/// What the writer may do next, once the pacer's slot for a frame arrives.
enum FrameSlot {
    /// Write the frame.
    Ready,
    /// `cancel` fired during the wait.
    Cancelled,
    /// A flush naming the audible job's turn arrived; stop writing.
    Flush(Option<UtteranceId>),
}

/// What the writer's between-jobs wait produced.
enum WriterStep {
    /// Play this job. Boxed: a `PlaybackJob` dwarfs every other variant, and this
    /// enum is returned once per job rather than per frame.
    Job(Box<Deferred>),
    /// A flush for the audible job's turn arrived; cut it.
    Flush(Option<UtteranceId>),
    /// A pending job's audible end came due, or a wait woke for nothing. Either
    /// way the loop re-enters, which retires whatever is due.
    Retire,
    /// `cancel` fired.
    Cancelled,
    /// The queue is closed and drained. The writer exits once `pending` is heard
    /// out.
    Closed,
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

    /// Take a pending flush request if it names the turn of the stream's audible
    /// job — the front of `pending` — and return that turn.
    ///
    /// The audible job, not the one being written: a target naming the front while
    /// a later job of another turn is being written is exactly the legitimate
    /// barge. A target naming any other turn is stale by construction — the handle
    /// verified the audible job before setting it, so a mismatch means that job has
    /// since retired and nobody can still hear it. It is cleared too, so a
    /// non-matching signal is genuinely dropped rather than lingering.
    ///
    /// TODO(flush-stale-target-interrupt): the handle answered `Ok` before the
    /// drop, so its caller has already marked the turn interrupted for a reply
    /// that was heard whole.
    fn take_flush_for(&self) -> Option<Option<UtteranceId>> {
        let front = self.pending.front().and_then(|p| p.job.in_reply_to);
        let mut target = self.flush_signal.target.lock().expect("flush target mutex");
        match *target {
            Some(t) if Some(t) == front => {
                *target = None;
                Some(front)
            }
            Some(_) => {
                *target = None;
                None
            }
            None => None,
        }
    }

    /// The stream clock's anchor, starting the stream at `now` if nothing is banked.
    fn stream_anchor(&mut self) -> Instant {
        *self.anchor.get_or_insert_with(Instant::now)
    }

    /// When the frame at the current stream index is heard: the anchor, the device's
    /// playout hop, and every frame written on this stream so far. Evaluated before
    /// a job's first frame this is that job's audible start; evaluated after its
    /// last it is its audible end.
    fn stream_position(&mut self) -> Instant {
        let banked = Duration::from_millis(
            audio_pipeline::playback::PLAYBACK_PLAYOUT_HOP_MS + self.frames_in_stream * FRAME_MS,
        );
        self.stream_anchor() + banked
    }

    /// The earliest audible end waiting to be serviced, or `None` when the front of
    /// `pending` is still being written (only the back can lack an end, so a front
    /// without one means `pending` holds just that job).
    fn next_audible_end(&self) -> Option<Instant> {
        self.pending.front().and_then(|p| p.ends_at)
    }

    /// Republish the audible job for the flush path: the front of `pending`, or
    /// nothing when the device's bank is empty.
    fn publish_current(&self) {
        *self.progress.current.lock().expect("job progress mutex") =
            self.pending.front().map(Pending::as_current);
    }

    /// Report the audible job if it has changed since the last report: the front
    /// of `pending`, by turn and interruptibility. Called after each lifecycle
    /// event that can move the front, so a change always trails the event that
    /// caused it.
    ///
    /// Separate from [`Writer::publish_current`], which stays synchronous with
    /// every mutation of `pending` so the flush handle never reads a `current`
    /// that lags the queue across an await.
    async fn emit_audible(&mut self) {
        let job = self.pending.front().map(|p| AudibleJob {
            in_reply_to: p.job.in_reply_to,
            interruptible: p.job.interruptible,
        });
        if job == self.last_audible {
            return;
        }
        self.last_audible = job.clone();
        (self.events)(PlaybackEvent::Audible {
            pod: self.pod.clone(),
            job,
        })
        .await;
    }

    /// Retire every pending job whose audible end has passed, emitting its
    /// `Finished` and handing the audible job over to the next one. With nothing
    /// left banked the stream clock resets, so the next job anchors a fresh stream.
    async fn retire_heard(&mut self) {
        while self
            .pending
            .front()
            .is_some_and(|p| p.ends_at.is_some_and(|end| end <= Instant::now()))
        {
            let done = self.pending.pop_front().expect("front was just inspected");
            self.publish_current();
            (self.events)(PlaybackEvent::Finished {
                pod: self.pod.clone(),
                in_reply_to: done.job.in_reply_to,
                frames: done.frames,
                samples: done.samples,
                eoa_written: done.eoa_written,
            })
            .await;
            self.emit_audible().await;
        }
        if self.pending.is_empty() {
            self.anchor = None;
            self.frames_in_stream = 0;
        }
    }

    /// Wait for the pacer's slot for the frame at the current stream index, so
    /// banked audio stays at most `lead_ms` ahead of real time. Cancellation and a
    /// flush for the audible turn both cut the wait short, and a pending job's
    /// audible end is serviced during it.
    async fn wait_frame_slot(&mut self) -> FrameSlot {
        loop {
            self.retire_heard().await;
            if let Some(turn) = self.take_flush_for() {
                return FrameSlot::Flush(turn);
            }
            let banked = Duration::from_millis(self.frames_in_stream * FRAME_MS);
            let ahead = (self.stream_anchor() + banked).saturating_duration_since(Instant::now());
            let lead = Duration::from_millis(self.cfg.lead_ms);
            if ahead <= lead {
                return FrameSlot::Ready;
            }
            let end = self.next_audible_end();
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return FrameSlot::Cancelled,
                // The target mutex is the authority; this only wakes the nap early.
                _ = self.flush_signal.notify.notified() => {
                    if let Some(turn) = self.take_flush_for() {
                        return FrameSlot::Flush(turn);
                    }
                }
                // A retirement came due mid-nap: the loop above services it and
                // re-computes the slot.
                _ = tokio::time::sleep_until(end.unwrap_or_else(Instant::now)),
                    if end.is_some() => {}
                _ = tokio::time::sleep(ahead - lead) => return FrameSlot::Ready,
            }
        }
    }

    /// Chunk and pace a job's PCM out as `Audio` frames, emitting `Started` at the
    /// first frame and putting the job on the stream — into `pending`, where the
    /// flush path can see it — from that frame on.
    ///
    /// `rewritten` marks a job the stream already carried before a flush discarded
    /// the device's bank: it reports no second `Started`, which would count a second
    /// cmd in the ledger and a second reply in the log for one answer. Its `Audible`
    /// is emitted either way — nobody heard the frames the flush threw away, so the
    /// floor really does open again here.
    async fn play_job(&mut self, job: &PlaybackJob, rewritten: bool) -> JobResult {
        let samples = job.pcm.len() as u64;
        let mut job_frames = 0u64;

        for chunk in job.pcm.chunks(AUDIO_SAMPLES_PER_FRAME) {
            match self.wait_frame_slot().await {
                FrameSlot::Ready => {}
                FrameSlot::Cancelled => {
                    return JobResult::Aborted {
                        reason: AbortReason::Cancelled,
                        frames: job_frames,
                    };
                }
                FrameSlot::Flush(turn) => {
                    return JobResult::Flushed {
                        turn,
                        frames: job_frames,
                    };
                }
            }
            // The audible start is the stream position *before* this frame: the
            // device plays what is already banked ahead of it first.
            let starts_at = (job_frames == 0).then(|| self.stream_position());
            let first_write = (job_frames == 0).then(HostMicros::now);
            let frame = build_audio_frame(chunk);
            if let Err(f) = self.write_frame(&frame).await {
                if matches!(f, WriteFail::Timeout) {
                    self.stats.record_write_timeout();
                }
                return JobResult::Aborted {
                    reason: f.into(),
                    frames: job_frames,
                };
            }
            self.stats.record_frame();
            self.frames_in_stream += 1;
            job_frames += 1;
            match starts_at {
                Some(starts_at) => {
                    self.pending.push_back(Pending {
                        job: job.clone(),
                        frames: 1,
                        samples,
                        eoa_written: false,
                        starts_at,
                        ends_at: None,
                    });
                    self.publish_current();
                    if !rewritten {
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
                    self.emit_audible().await;
                }
                None => {
                    self.pending
                        .back_mut()
                        .expect("the job being written is the back of pending")
                        .frames += 1;
                }
            }
        }

        JobResult::Completed { frames: job_frames }
    }

    /// Emit `Aborted` for every job still on the stream or still queued, counting
    /// each. Used on a write failure or cancellation to fail the whole backlog
    /// loudly rather than silently. A pending job aborts with the rest: the writer
    /// is losing the stream, and what the device does with a bank it still holds
    /// depends on something the host cannot see — a connection that stays down
    /// plays the tail out, a reconnect discards it — so the honest report is that
    /// the writer did not see the job through, and the ledger settles it unclean.
    /// Reporting it heard would claim a tail a reconnect threw away.
    async fn drain_aborted(&mut self, rx: &mut mpsc::Receiver<PlaybackJob>, reason: AbortReason) {
        // Close first so the drain-then-exit is atomic from a sender's view: a job
        // that races the drain is rejected as `WriterDead` rather than accepted and
        // then destroyed by the receiver drop with no terminal event.
        rx.close();
        let banked = std::mem::take(&mut self.pending)
            .into_iter()
            .map(|p| p.job)
            .collect::<Vec<_>>();
        self.publish_current();
        let queued = banked
            .into_iter()
            .chain(
                std::mem::take(&mut self.deferred)
                    .into_iter()
                    .map(|d| d.job),
            )
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
        self.emit_audible().await;
    }

    /// Report one job the flush evicted: audio that was banked or queued and that
    /// nobody heard, whatever it cost to get there. Every eviction shape goes
    /// through here, so they cannot drift in what they count or what they say the
    /// clip's length was.
    async fn report_evicted(
        &mut self,
        in_reply_to: Option<UtteranceId>,
        frames_written: u64,
        samples: u64,
    ) {
        self.stats.record_flushed();
        (self.events)(PlaybackEvent::Flushed {
            pod: self.pod.clone(),
            in_reply_to,
            was_playing: false,
            frames_written,
            progress: InterruptProgress {
                heard_ms: 0,
                total_ms: audio_ms(samples),
            },
        })
        .await;
    }

    /// Cut `turn`: report the audible job, evict the turn's banked, parked and
    /// queued jobs,
    /// re-defer every other turn's banked job, and end the stream on the wire with
    /// `FlushPlayback`.
    ///
    /// Only the front of `pending` was audible, and `FlushPlayback` discards the
    /// device's whole bank, so a later job of another turn has been heard by nobody:
    /// it goes back to the front of `deferred` and is written again from its first
    /// frame on the fresh stream. Its `Started` is not repeated — that event dated
    /// the first write, and its readers tolerate the pacer's lead as noise already
    /// — and it gets no `Flushed`, because nobody barged its turn.
    ///
    /// No `EndOfAudio` follows: the device's flush already discards its banked
    /// audio and mutes, so an end-of-audio mark after it would be a redundant
    /// second one. The writer stays alive — unlike a cancel, a flush is a mid-life
    /// event, and the barge-in's own response plays next on this connection.
    ///
    /// `unwritten` is the job the writer had taken but had not put a frame on the
    /// stream for when the flush landed. It is newer than everything banked, so it
    /// is re-deferred behind them rather than ahead: one pass over the stream
    /// decides the order the replies come back in.
    async fn handle_flush(
        &mut self,
        turn: Option<UtteranceId>,
        unwritten: Option<PlaybackJob>,
        rx: &mut mpsc::Receiver<PlaybackJob>,
    ) -> Result<(), WriteFail> {
        if let Some(audible) = self.pending.pop_front() {
            self.stats.record_flushed();
            (self.events)(PlaybackEvent::Flushed {
                pod: self.pod.clone(),
                in_reply_to: audible.job.in_reply_to,
                was_playing: true,
                frames_written: audible.frames,
                progress: audible.as_current().progress(),
            })
            .await;
        }

        // The rest of the stream, oldest first: the flushed turn's own jobs report
        // audio nobody heard, other turns' jobs go back ahead of everything already
        // deferred, in order.
        let mut requeue = Vec::new();
        for banked in std::mem::take(&mut self.pending) {
            if banked.job.in_reply_to == turn {
                self.report_evicted(banked.job.in_reply_to, banked.frames, banked.samples)
                    .await;
            } else {
                requeue.push(Deferred {
                    job: banked.job,
                    started: true,
                });
            }
        }
        if let Some(job) = unwritten {
            if job.in_reply_to == turn {
                self.report_evicted(job.in_reply_to, 0, job.pcm.len() as u64)
                    .await;
            } else {
                requeue.push(Deferred {
                    job,
                    started: false,
                });
            }
        }
        // Jobs an earlier flush parked, which `next_step` plays ahead of everything
        // else: the flushed turn's own are evicted here too. A multi-clip reply
        // banked behind another turn's tail lands there whole, so leaving them
        // would let the second clip of a barged reply speak right after the cut.
        for parked in std::mem::take(&mut self.deferred) {
            if parked.job.in_reply_to == turn {
                self.report_evicted(parked.job.in_reply_to, 0, parked.job.pcm.len() as u64)
                    .await;
            } else {
                self.deferred.push_back(parked);
            }
        }
        for deferred in requeue.into_iter().rev() {
            self.deferred.push_front(deferred);
        }
        self.publish_current();

        // Evict the flushed turn's queued jobs; anything for another turn is
        // deferred, keeping its order, and plays after the flush on a fresh stream.
        while let Ok(queued) = rx.try_recv() {
            if queued.in_reply_to == turn {
                self.report_evicted(queued.in_reply_to, 0, queued.pcm.len() as u64)
                    .await;
            } else {
                self.deferred.push_back(Deferred {
                    job: queued,
                    started: false,
                });
            }
        }

        self.write_frame(&StreamFrame::FlushPlayback(FlushPlayback {}))
            .await?;
        // The device's bank is gone, so the stream clock starts over.
        self.anchor = None;
        self.frames_in_stream = 0;
        // Nothing is audible now. A job re-deferred above reports itself audible
        // again at its first write on the fresh stream, which is the one thing
        // that tells the floor a reply nobody barged is being heard from the top.
        self.emit_audible().await;
        Ok(())
    }

    /// What to do next when no job is being written: deferred jobs first (they were
    /// queued or banked before anything still in the channel), then a flush for the
    /// audible turn, a pending job's audible end, a queued job, or the queue's end.
    ///
    /// The flush branch is what makes a barge in a reply's last lead cut it: the
    /// writer is idle here with the tail audible, and without it the cut would wait
    /// out the audio it was meant to stop.
    async fn next_step(&mut self, rx: &mut mpsc::Receiver<PlaybackJob>) -> WriterStep {
        if let Some(d) = self.deferred.pop_front() {
            return WriterStep::Job(Box::new(d));
        }
        if let Some(turn) = self.take_flush_for() {
            return WriterStep::Flush(turn);
        }
        if self.queue_closed && self.pending.is_empty() {
            return WriterStep::Closed;
        }
        let end = self.next_audible_end();
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => WriterStep::Cancelled,
            // The target mutex is the authority; this only wakes the wait early.
            _ = self.flush_signal.notify.notified() => match self.take_flush_for() {
                Some(turn) => WriterStep::Flush(turn),
                None => WriterStep::Retire,
            },
            _ = tokio::time::sleep_until(end.unwrap_or_else(Instant::now)),
                if end.is_some() => WriterStep::Retire,
            // `None`: all senders dropped, queue drained. The writer stays for
            // whatever is still coming out of the speaker.
            j = rx.recv(), if !self.queue_closed => match j {
                Some(job) => WriterStep::Job(Box::new(Deferred {
                    job,
                    started: false,
                })),
                None => WriterStep::Closed,
            },
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

        loop {
            self.retire_heard().await;
            let (job, rewritten) = match self.next_step(&mut rx).await {
                WriterStep::Job(d) => (d.job, d.started),
                WriterStep::Retire => continue,
                WriterStep::Flush(turn) => {
                    // Idle between jobs: the writer holds nothing it has not
                    // written, so there is no unwritten job to place.
                    if let Err(f) = self.handle_flush(turn, None, &mut rx).await {
                        self.die_on_flush_write(&mut rx, f).await;
                        return;
                    }
                    continue;
                }
                WriterStep::Cancelled => {
                    self.drain_aborted(&mut rx, AbortReason::Cancelled).await;
                    return;
                }
                WriterStep::Closed => {
                    // Every sender is gone, but the writer stays until the last
                    // banked frame has been heard and reported.
                    self.queue_closed = true;
                    if self.pending.is_empty() {
                        return;
                    }
                    continue;
                }
            };

            match self.play_job(&job, rewritten).await {
                JobResult::Completed { frames } => {
                    self.stats.record_completed();
                    if rewritten {
                        self.stats.record_rewritten();
                    }
                    // A drained queue ends the stream: mark it with EndOfAudio. A new
                    // job already waiting continues the same stream with no
                    // intervening EndOfAudio.
                    let drained = self.deferred.is_empty() && rx.is_empty();
                    let mut eoa_written = false;
                    let mut eoa_failure = None;
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
                                eoa_failure = Some(AbortReason::from(f));
                            }
                        }
                    }
                    // The job's audio is all out: date its audible end on the stream
                    // clock and report the write. An empty clip never reached the
                    // stream, so it is written and heard in the same instant.
                    let plays_until = self.stream_position();
                    match self.pending.back_mut() {
                        Some(last) if frames > 0 => {
                            last.eoa_written = eoa_written;
                            last.ends_at = Some(plays_until);
                        }
                        _ => {}
                    }
                    (self.events)(PlaybackEvent::Written {
                        pod: self.pod.clone(),
                        in_reply_to: job.in_reply_to,
                        frames,
                        samples: job.pcm.len() as u64,
                        eoa_written,
                        rewritten,
                        plays_until,
                    })
                    .await;
                    // An empty clip is heard in the instant it is written, so it
                    // reports its own end here — unless the drain's mark failed
                    // under it, in which case the abort below is its one terminal
                    // event, as it is for every other job on this stream.
                    if frames == 0 && eoa_failure.is_none() {
                        (self.events)(PlaybackEvent::Finished {
                            pod: self.pod.clone(),
                            in_reply_to: job.in_reply_to,
                            frames,
                            samples: job.pcm.len() as u64,
                            eoa_written,
                        })
                        .await;
                    }
                    if let Some(reason) = eoa_failure {
                        // The failed EndOfAudio leaves the writer dead, and with
                        // the stream gone the host cannot know whether the device
                        // played its bank out or discarded it on a reconnect. So
                        // this job aborts rather than finishing, and everything
                        // else banked or queued goes with it.
                        self.stats.record_aborted();
                        // Only a job with frames out is on the stream; popping for
                        // an empty one would take the previous job's entry and
                        // leave that job with no terminal event at all.
                        if frames > 0 {
                            self.pending.pop_back();
                        }
                        self.publish_current();
                        (self.events)(PlaybackEvent::Aborted {
                            pod: self.pod.clone(),
                            in_reply_to: job.in_reply_to,
                            reason,
                        })
                        .await;
                        self.drain_aborted(&mut rx, reason).await;
                        return;
                    }
                }
                JobResult::Flushed { turn, frames } => {
                    // A job taken but not yet written is not on the stream, so the
                    // flush's re-deferral cannot find it there; it is handed over
                    // instead, and re-deferred behind the banked jobs it is newer
                    // than or reported evicted if its own turn was the flushed one.
                    let unwritten = (frames == 0).then(|| job.clone());
                    if let Err(f) = self.handle_flush(turn, unwritten, &mut rx).await {
                        self.die_on_flush_write(&mut rx, f).await;
                        return;
                    }
                }
                JobResult::Aborted { reason, frames } => {
                    // A job with frames out is the back of `pending`; it reports its
                    // own abort here rather than through the drain below, which takes
                    // the rest of the stream and the backlog.
                    if frames > 0 {
                        self.pending.pop_back();
                        self.publish_current();
                    }
                    self.stats.record_aborted();
                    (self.events)(PlaybackEvent::Aborted {
                        pod: self.pod.clone(),
                        in_reply_to: job.in_reply_to,
                        reason,
                    })
                    .await;
                    // No EndOfAudio on abort: the write that would carry it is the
                    // one that just failed, or a cancel is tearing the stream down.
                    // Fail the rest of the backlog loudly.
                    self.drain_aborted(&mut rx, reason).await;
                    return;
                }
            }
        }
    }

    /// The `FlushPlayback` frame could not be written: the cut was not delivered,
    /// so the device still holds up to the pacer's lead of the barged reply and
    /// will play it out unless it reconnects. The writer no longer knows what the
    /// device plays, so it fails the backlog loudly and dies, as any write failure
    /// does; the floor closes with it, because the host has nothing to tell the
    /// listener about audio it cannot see.
    async fn die_on_flush_write(&mut self, rx: &mut mpsc::Receiver<PlaybackJob>, f: WriteFail) {
        if matches!(f, WriteFail::Timeout) {
            self.stats.record_write_timeout();
        }
        self.drain_aborted(rx, AbortReason::from(f)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use audio_pipeline::wire::{Codec, decode_frame};
    use tokio::io::{AsyncReadExt, DuplexStream, duplex};

    /// The one sample-count-to-duration conversion, including what it does with
    /// a count that is not a whole number of milliseconds: it truncates. Both
    /// callers — the motion scripter's audio horizon and the playback
    /// narration — are estimating a playout that has already been rounded to
    /// frames, so a fraction of a millisecond short is the harmless direction,
    /// but it is a decision and not an accident.
    #[test]
    fn audio_ms_is_whole_milliseconds_of_spine_audio() {
        let rate = u64::from(SPINE_FORMAT.sample_rate_hz);
        assert_eq!(audio_ms(0), 0);
        assert_eq!(audio_ms(rate), 1000);
        assert_eq!(audio_ms(rate / 1000), 1, "one millisecond");
        assert_eq!(audio_ms(rate / 1000 - 1), 0, "one sample short of it");
        assert_eq!(
            audio_ms(rate + rate / 2000),
            1000,
            "half a millisecond past a second is still a second"
        );
    }

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
        let tags = event_tags(&seen);
        assert!(
            tags.iter().any(|t| t.starts_with("aborted")),
            "cancellation aborts: {tags:?}",
        );
        assert!(
            !tags
                .iter()
                .any(|t| t.starts_with("audible") || t == "silent"),
            "nothing was ever banked, so there is no floor move to report: {tags:?}",
        );
        assert_eq!(stats.snapshot().eoa_written, 0, "no EndOfAudio on cancel");
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_with_a_bank_silences_the_pod_after_the_whole_aborted_set() {
        // The device is holding audio the host can no longer account for: one
        // silence, and it comes after every abort, so nothing downstream sees a
        // closed floor with jobs still reporting themselves.
        let cancel = CancellationToken::new();
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let (dev, mut host) = duplex(1 << 16);
        let reader = tokio::spawn(async move {
            let mut tmp = [0u8; 4096];
            while host.read(&mut tmp).await.expect("read") != 0 {}
        });
        let handle = PlaybackWriter::spawn(
            dev,
            PodId("pod-x".into()),
            PacerConfig {
                lead_ms: 1_000,
                job_queue_depth: 4,
                ..PacerConfig::default()
            },
            Arc::clone(&stats),
            events,
            cancel.clone(),
        );

        handle
            .try_play(job_with_id(vec![1i16; AUDIO_SAMPLES_PER_FRAME * 3], 42))
            .unwrap();
        handle
            .try_play(job_with_id(vec![2i16; AUDIO_SAMPLES_PER_FRAME * 3], 43))
            .unwrap();
        // Both are banked inside the lead with no time advanced, so neither has been
        // heard out when the cancel lands.
        run_until(|| stats.snapshot().jobs_completed == 2).await;
        cancel.cancel();
        run_until(|| stats.snapshot().jobs_aborted == 2).await;

        let tags = event_tags(&seen);
        assert_eq!(
            tags.iter().filter(|t| *t == "silent").count(),
            1,
            "one silence: {tags:?}",
        );
        assert_eq!(tags.last().map(String::as_str), Some("silent"), "{tags:?}");
        assert_eq!(
            tags.iter().filter(|t| t.starts_with("aborted")).count(),
            2,
            "both banked jobs abort: {tags:?}",
        );
        drop(handle);
        reader.await.expect("reader");
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
        // The whole stream drained under paused time, and the writer stayed until
        // its bank was heard out, so the wall time advanced is the pacer's plus the
        // device's playout hop: at least (audio − lead − one frame), at most the
        // audio duration plus the hop. This bounds the aggregate pacing without
        // per-frame plumbing: had the pacer free-run, elapsed would be ~0; had it
        // lagged, elapsed would exceed the audio duration by more than the hop.
        let elapsed_ms = Instant::now().duration_since(start).as_millis() as u64;
        let audio_ms = n_frames as u64 * FRAME_MS;
        let hop_ms = audio_pipeline::playback::PLAYBACK_PLAYOUT_HOP_MS;
        assert!(
            elapsed_ms + lead_ms + FRAME_MS >= audio_ms,
            "paced too slow: elapsed {elapsed_ms} ms, audio {audio_ms} ms",
        );
        assert!(
            elapsed_ms <= audio_ms + hop_ms,
            "paced ahead of real time: elapsed {elapsed_ms} ms, audio {audio_ms} ms \
             plus a {hop_ms} ms hop",
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
        // Advance past the device's playout hop and into the clip, so the heard
        // estimate is non-trivial: before the hop elapses the device has banked
        // audio and played none of it.
        let advanced = audio_pipeline::playback::PLAYBACK_PLAYOUT_HOP_MS + 200;
        tokio::time::advance(Duration::from_millis(advanced)).await;
        run_until(|| stats.snapshot().frames_written >= 10).await;

        let progress = handle.flush(UtteranceId(42)).expect("playing turn flushes");
        assert!(
            progress.heard_ms > 0
                && progress.heard_ms
                    <= advanced - audio_pipeline::playback::PLAYBACK_PLAYOUT_HOP_MS,
            "heard {} ms is the advanced time less the hop the device spends banking",
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

    /// Every event the writer emitted, as `kind:turn` tags in order. The whole
    /// point of the `Audible` checks is where the event falls in the sequence, so
    /// the assertion is the sequence.
    fn event_tags(seen: &Arc<Mutex<Vec<PlaybackEvent>>>) -> Vec<String> {
        fn turn(id: &Option<UtteranceId>) -> String {
            id.map_or_else(|| "-".to_string(), |u| u.0.to_string())
        }
        seen.lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                PlaybackEvent::Started { in_reply_to, .. } => {
                    Some(format!("started:{}", turn(in_reply_to)))
                }
                PlaybackEvent::Written { in_reply_to, .. } => {
                    Some(format!("written:{}", turn(in_reply_to)))
                }
                PlaybackEvent::Finished { in_reply_to, .. } => {
                    Some(format!("finished:{}", turn(in_reply_to)))
                }
                PlaybackEvent::Aborted { in_reply_to, .. } => {
                    Some(format!("aborted:{}", turn(in_reply_to)))
                }
                PlaybackEvent::Flushed {
                    in_reply_to,
                    was_playing,
                    ..
                } => Some(format!(
                    "flushed{}:{}",
                    if *was_playing { "" } else { "-evicted" },
                    turn(in_reply_to)
                )),
                PlaybackEvent::Audible { job, .. } => Some(match job {
                    Some(j) => format!("audible:{}", turn(&j.in_reply_to)),
                    None => "silent".to_string(),
                }),
                PlaybackEvent::HelloWritten { .. } | PlaybackEvent::HelloFailed { .. } => None,
            })
            .collect()
    }

    /// A writer whose device end is drained to EOF by a background task, so nothing
    /// in these checks is waiting on socket backpressure.
    fn drained_writer(
        cfg: PacerConfig,
        stats: &Arc<PlaybackStats>,
        events: PlaybackEventFn,
    ) -> (PlaybackHandle, tokio::task::JoinHandle<()>) {
        let (dev, mut host) = duplex(1 << 16);
        let reader = tokio::spawn(async move {
            let mut tmp = [0u8; 4096];
            while host.read(&mut tmp).await.expect("read") != 0 {}
        });
        let handle = PlaybackWriter::spawn(
            dev,
            PodId("pod-x".into()),
            cfg,
            Arc::clone(stats),
            events,
            CancellationToken::new(),
        );
        (handle, reader)
    }

    /// A writer parked mid-stream: reply 42 audible, 43 banked behind it inside the
    /// lead, 44 taken and waiting on the pacer's slot for its first frame, and a
    /// second clip of 42 still in the queue. The shape a barge for 42 has to sort
    /// out, and the only one where the writer holds a job it has not written.
    async fn parked_three_deep(
        stats: &Arc<PlaybackStats>,
        events: PlaybackEventFn,
    ) -> (PlaybackHandle, tokio::task::JoinHandle<()>) {
        // 42's and 43's frames fill the lead, so 44 parks on the slot for its first
        // frame and 42's second clip never leaves the queue.
        parked_over(stats, events, &[(42, 3), (43, 3), (44, 3), (42, 2)]).await
    }

    /// A writer parked mid-stream over `clips`, each `(turn, frames)` in the order
    /// they were queued: with a 100 ms lead and 20 ms frames, the first two fill it
    /// and everything after them is taken-but-unwritten or still in the queue. The
    /// shape a barge has to sort out, over whatever mix of turns the case is about.
    async fn parked_over(
        stats: &Arc<PlaybackStats>,
        events: PlaybackEventFn,
        clips: &[(u64, usize)],
    ) -> (PlaybackHandle, tokio::task::JoinHandle<()>) {
        let (handle, reader) = drained_writer(
            PacerConfig {
                lead_ms: 100,
                job_queue_depth: 8,
                ..PacerConfig::default()
            },
            stats,
            events,
        );
        for (id, frames) in clips {
            handle
                .try_play(job_with_id(
                    vec![1i16; AUDIO_SAMPLES_PER_FRAME * frames],
                    *id,
                ))
                .unwrap();
        }
        run_until(|| stats.snapshot().jobs_completed == 2).await;
        (handle, reader)
    }

    /// Every `Flushed` for audio nobody heard, as `(turn, frames_written,
    /// total_ms)`. The frame count separates a clip the device already held from
    /// one that never reached the stream.
    fn evicted(seen: &Arc<Mutex<Vec<PlaybackEvent>>>) -> Vec<(Option<u64>, u64, u64)> {
        seen.lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                PlaybackEvent::Flushed {
                    in_reply_to,
                    was_playing: false,
                    frames_written,
                    progress,
                    ..
                } => Some((in_reply_to.map(|u| u.0), *frames_written, progress.total_ms)),
                _ => None,
            })
            .collect()
    }

    /// The clip lengths `evicted` reports for `frames` frames of audio.
    fn clip_ms(frames: u64) -> u64 {
        audio_ms(frames * AUDIO_SAMPLES_PER_FRAME as u64)
    }

    /// Every write pass in the run, as `turn`s in order.
    fn writes(seen: &Arc<Mutex<Vec<PlaybackEvent>>>) -> Vec<Option<u64>> {
        seen.lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                PlaybackEvent::Written { in_reply_to, .. } => Some(in_reply_to.map(|u| u.0)),
                _ => None,
            })
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn a_barge_evicts_the_banked_clips_of_the_reply_it_cuts() {
        // The ordinary barge on a multi-clip reply: clip 1 is audible, clip 2 is
        // already in the device's bank, and another reply is waiting behind them.
        // The cut has to take the whole reply — a banked clip left on the stream is
        // the robot talking on after being stopped — and it reports the audio nobody
        // heard with that clip's own length, not a zero.
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let (handle, reader) = parked_over(&stats, events, &[(42, 3), (42, 3), (43, 3)]).await;

        handle.flush(UtteranceId(42)).expect("42 is audible");
        run_until(|| stats.snapshot().jobs_completed == 3).await;

        drop(handle);
        reader.await.expect("reader");

        assert_eq!(
            evicted(&seen),
            [(Some(42), 3, clip_ms(3))],
            "the banked second clip is thrown away with the reply it belongs to, \
             and its frames were written: {:?}",
            event_tags(&seen),
        );
        assert_eq!(
            writes(&seen),
            [Some(42), Some(42), Some(43)],
            "nothing of 42 is written after the cut, and 43 is written once",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_barge_evicts_the_clip_the_writer_had_taken_but_not_written() {
        // The same reply's next clip can also be the job the writer holds and has
        // not put a frame on the stream for. It is evicted with its own length and
        // no frames, while the other turn banked ahead of it is re-written.
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let (handle, reader) = parked_over(&stats, events, &[(42, 3), (43, 3), (42, 2)]).await;

        handle.flush(UtteranceId(42)).expect("42 is audible");
        run_until(|| stats.snapshot().jobs_completed == 3).await;

        drop(handle);
        reader.await.expect("reader");

        assert_eq!(
            evicted(&seen),
            [(Some(42), 0, clip_ms(2))],
            "a clip that never reached the stream still reports the audio it was: \
             {:?}",
            event_tags(&seen),
        );
        assert_eq!(
            writes(&seen),
            [Some(42), Some(43), Some(43)],
            "43 is written again after the cut and 42's unwritten clip never is",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_barge_evicts_the_reply_an_earlier_barge_parked() {
        // A reply parked by an earlier cut waits in `deferred`, which `next_step`
        // plays before anything else. Barging *that* reply has to reach it there:
        // its first clip is audible and its second is parked, and a cut that only
        // walked the stream would speak the second one immediately after silencing
        // the first.
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let (handle, reader) = drained_writer(
            PacerConfig {
                lead_ms: 100,
                job_queue_depth: 8,
                ..PacerConfig::default()
            },
            &stats,
            events,
        );
        for (id, frames) in [(41, 3), (42, 20), (42, 2)] {
            handle
                .try_play(job_with_id(
                    vec![1i16; AUDIO_SAMPLES_PER_FRAME * frames],
                    id,
                ))
                .unwrap();
        }
        // 41 is heard out of the way and 42's first clip — too long for the lead —
        // is banked behind it with its second clip still queued. The cut parks both.
        run_until(|| {
            let snap = stats.snapshot();
            snap.jobs_completed == 1 && snap.frames_written > 3
        })
        .await;
        handle.flush(UtteranceId(41)).expect("41 is audible");
        run_until(|| handle.current_turn() == Some(UtteranceId(42))).await;

        handle.flush(UtteranceId(42)).expect("42 is audible again");
        drop(handle);
        reader.await.expect("reader");

        assert_eq!(
            evicted(&seen),
            [(Some(42), 0, clip_ms(2))],
            "the parked second clip is cut with the reply it belongs to: {:?}",
            event_tags(&seen),
        );
        assert_eq!(
            stats.snapshot().jobs_completed,
            1,
            "only 41 was ever written through; both passes over 42's first clip \
             were cut and its second clip was never written: {:?}",
            event_tags(&seen),
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_flush_replays_the_banked_job_before_the_one_it_had_not_written() {
        // The cut takes the stream out from under two replies at once: 43's frames
        // were banked and 44 had been taken but not written. 44 is the newer of the
        // two, so it plays second — a reply order the speaker gives away and no
        // single-overlap test can see.
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let (handle, reader) = parked_three_deep(&stats, events).await;

        handle.flush(UtteranceId(42)).expect("42 is audible");
        run_until(|| stats.snapshot().jobs_completed == 4).await;

        drop(handle);
        reader.await.expect("reader");

        let tags = event_tags(&seen);
        let writes: Vec<&String> = tags.iter().filter(|t| t.starts_with("written:")).collect();
        assert_eq!(
            writes,
            ["written:42", "written:43", "written:43", "written:44"],
            "43 was on the stream before 44 was taken, so it is spoken first \
             again: {tags:?}",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_evicted_clip_reports_the_length_nobody_heard() {
        // Every eviction shape reports the clip it threw away the same way, so a
        // consumer reading `playback_flushed` is not told a length for one and a
        // zero for another.
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let (handle, reader) = parked_three_deep(&stats, events).await;

        handle.flush(UtteranceId(42)).expect("42 is audible");
        run_until(|| stats.snapshot().jobs_completed == 4).await;

        drop(handle);
        reader.await.expect("reader");

        assert_eq!(
            evicted(&seen),
            [(Some(42), 0, clip_ms(2))],
            "the queued second clip of the barged reply is the only audio the cut \
             threw away, and its length is its own",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_re_written_job_says_so_on_its_second_write() {
        // A job whose banked frames the flush discarded repeats no `Started`, so its
        // second `Written` is the only place a reader can learn that two write
        // passes were one answer.
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let (handle, reader) = parked_three_deep(&stats, events).await;

        handle.flush(UtteranceId(42)).expect("42 is audible");
        run_until(|| stats.snapshot().jobs_completed == 4).await;

        drop(handle);
        reader.await.expect("reader");

        let passes: Vec<(Option<u64>, bool)> = seen
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                PlaybackEvent::Written {
                    in_reply_to,
                    rewritten,
                    ..
                } => Some((in_reply_to.map(|u| u.0), *rewritten)),
                _ => None,
            })
            .collect();
        assert_eq!(
            passes,
            [
                (Some(42), false),
                (Some(43), false),
                (Some(43), true),
                (Some(44), false)
            ],
            "only 43's second pass is a re-write; 44 had never reached the stream",
        );
        let snap = stats.snapshot();
        assert_eq!(
            (snap.jobs_completed, snap.jobs_rewritten),
            (4, 1),
            "four write passes, three answers",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn audible_opens_at_the_first_write_and_closes_at_the_audible_end() {
        // The floor the listener runs on: the pod is heard from the first frame
        // written until the last one is played, which trails the write by the
        // device's playout hop. Nothing else in the run moves it.
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let (handle, reader) = drained_writer(PacerConfig::default(), &stats, events);

        handle
            .try_play(job_with_id(vec![1i16; AUDIO_SAMPLES_PER_FRAME * 3], 42))
            .unwrap();
        run_until(|| stats.snapshot().jobs_completed == 1).await;
        assert_eq!(
            event_tags(&seen),
            ["started:42", "audible:42", "written:42"],
            "written, and still being heard",
        );

        drop(handle);
        reader.await.expect("reader");
        assert_eq!(
            event_tags(&seen),
            [
                "started:42",
                "audible:42",
                "written:42",
                "finished:42",
                "silent",
            ],
            "the pod falls silent at the audible end, after the event that dates it",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn two_clips_of_one_reply_move_the_floor_once() {
        // A reply split into clips is one stretch of speech. The hand-over emits no
        // `Audible` at all, so the barge guard's sustain run survives the boundary
        // instead of being reset mid-sentence.
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let (handle, reader) = drained_writer(PacerConfig::default(), &stats, events);

        handle
            .try_play(job_with_id(vec![1i16; AUDIO_SAMPLES_PER_FRAME * 3], 42))
            .unwrap();
        handle
            .try_play(job_with_id(vec![2i16; AUDIO_SAMPLES_PER_FRAME * 3], 42))
            .unwrap();
        drop(handle);
        reader.await.expect("reader");

        let tags = event_tags(&seen);
        assert_eq!(
            tags.iter().filter(|t| t.starts_with("audible")).count(),
            1,
            "one open for the whole reply: {tags:?}",
        );
        assert_eq!(
            tags.iter().filter(|t| *t == "silent").count(),
            1,
            "one close, at the second clip's audible end: {tags:?}",
        );
        assert_eq!(tags.first().map(String::as_str), Some("started:42"));
        assert_eq!(tags.last().map(String::as_str), Some("silent"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_hand_over_to_another_turn_is_reported_at_the_first_ones_audible_end() {
        // Two replies back to back on one stream. The second's first write is not a
        // change — the first is still being heard — so the floor moves at the
        // hand-over, which is the boundary a listener hears rather than the one the
        // pacer writes. A short lead keeps the second clip being written across it.
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let (handle, reader) = drained_writer(
            PacerConfig {
                lead_ms: 100,
                job_queue_depth: 4,
                ..PacerConfig::default()
            },
            &stats,
            events,
        );

        handle
            .try_play(job_with_id(vec![1i16; AUDIO_SAMPLES_PER_FRAME * 3], 42))
            .unwrap();
        handle
            .try_play(job_with_id(vec![2i16; AUDIO_SAMPLES_PER_FRAME * 30], 43))
            .unwrap();
        drop(handle);
        reader.await.expect("reader");

        assert_eq!(
            event_tags(&seen),
            [
                "started:42",
                "audible:42",
                "written:42",
                "started:43",
                "finished:42",
                "audible:43",
                "written:43",
                "finished:43",
                "silent",
            ],
            "each `Audible` trails the event that caused it, and 43's first write \
             changes nothing while 42 is still being heard",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_audible_job_is_published_before_the_event_that_hands_it_over() {
        // `publish_current` is synchronous and runs ahead of the lifecycle event it
        // belongs to, so a flush landing between that event and the `Audible` behind
        // it reads the new front rather than the job that just retired. Moving the
        // publish after the event's `.await` — tempting, since `emit_audible`
        // recomputes the same front — makes a barge at a first write refuse as
        // `NotPlaying` and one at a hand-over target a job `take_flush_for` then
        // drops. Both are a cut that silently does nothing, and the event-order
        // checks stay green through either.
        let seen = Arc::new(Mutex::new(Vec::new()));
        let progress: Arc<Mutex<Option<Arc<JobProgress>>>> = Arc::new(Mutex::new(None));
        // Each lifecycle event, with the turn `progress.current` named as it was
        // emitted.
        type AskedAt = Arc<Mutex<Vec<(String, Option<u64>)>>>;
        let asked: AskedAt = Arc::new(Mutex::new(Vec::new()));
        let events: PlaybackEventFn = {
            let (sink, published, at) = (seen.clone(), progress.clone(), asked.clone());
            Arc::new(move |e| {
                let tag = match &e {
                    PlaybackEvent::Started { in_reply_to, .. } => {
                        Some(format!("started:{}", in_reply_to.expect("a reply").0))
                    }
                    PlaybackEvent::Finished { in_reply_to, .. } => {
                        Some(format!("finished:{}", in_reply_to.expect("a reply").0))
                    }
                    _ => None,
                };
                if let Some(tag) = tag {
                    // What a flush arriving right here would target.
                    let turn = published.lock().unwrap().as_ref().and_then(|p| {
                        p.current
                            .lock()
                            .expect("job progress mutex")
                            .and_then(|c| c.turn)
                            .map(|u| u.0)
                    });
                    at.lock().unwrap().push((tag, turn));
                }
                sink.lock().unwrap().push(e);
                Box::pin(std::future::ready(()))
            })
        };

        let stats = Arc::new(PlaybackStats::default());
        let (handle, reader) = drained_writer(
            PacerConfig {
                lead_ms: 100,
                job_queue_depth: 4,
                ..PacerConfig::default()
            },
            &stats,
            events,
        );
        *progress.lock().unwrap() = Some(Arc::clone(&handle.progress));

        handle
            .try_play(job_with_id(vec![1i16; AUDIO_SAMPLES_PER_FRAME * 3], 42))
            .unwrap();
        handle
            .try_play(job_with_id(vec![2i16; AUDIO_SAMPLES_PER_FRAME * 30], 43))
            .unwrap();
        drop(handle);
        reader.await.expect("reader");

        let recorded = asked.lock().unwrap();
        assert_eq!(
            *recorded,
            [
                ("started:42".to_string(), Some(42)),
                ("started:43".to_string(), Some(42)),
                ("finished:42".to_string(), Some(43)),
                ("finished:43".to_string(), None),
            ],
            "the first write publishes before its `Started`, and the hand-over \
             publishes 43 before 42's `Finished`: {:?}",
            event_tags(&seen),
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_flush_silences_the_pod_and_a_re_written_job_speaks_again() {
        // A barge on reply 42 while reply 43 sits banked behind it. The device's
        // whole bank goes, so 43 was heard by nobody and is written again from its
        // first frame — reported as audible a second time, with no second `Started`
        // for the ledger or the analyzer to trip over.
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let (handle, reader) = drained_writer(
            PacerConfig {
                lead_ms: 1_000,
                job_queue_depth: 4,
                ..PacerConfig::default()
            },
            &stats,
            events,
        );

        handle
            .try_play(job_with_id(vec![1i16; AUDIO_SAMPLES_PER_FRAME * 3], 42))
            .unwrap();
        handle
            .try_play(job_with_id(vec![2i16; AUDIO_SAMPLES_PER_FRAME * 3], 43))
            .unwrap();
        // Both are banked inside the lead with no time advanced, so 42 is still the
        // audible one and 43 has been written but heard by nobody.
        run_until(|| stats.snapshot().jobs_completed == 2).await;
        handle.flush(UtteranceId(42)).expect("42 is audible");
        run_until(|| stats.snapshot().jobs_flushed == 1).await;

        drop(handle);
        reader.await.expect("reader");

        let tags = event_tags(&seen);
        assert_eq!(
            tags.iter().filter(|t| *t == "started:43").count(),
            1,
            "the re-written job starts once, whatever the device threw away: {tags:?}",
        );
        let audible: Vec<&String> = tags
            .iter()
            .filter(|t| t.starts_with("audible") || *t == "silent")
            .collect();
        assert_eq!(
            audible,
            ["audible:42", "silent", "audible:43", "silent"],
            "the cut silences the pod and 43's re-write is what opens the floor for \
             it: {tags:?}",
        );
        assert!(
            tags.iter()
                .position(|t| t == "flushed:42")
                .zip(tags.iter().position(|t| t == "silent"))
                .is_some_and(|(cut, quiet)| cut < quiet),
            "the silence trails the cut that caused it: {tags:?}",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn flush_after_the_last_write_cuts_the_audible_tail() {
        // A barge in a reply's last lead: every frame is written, the writer is idle
        // between jobs, and up to a lead of the clip is still coming out of the
        // speaker. The cut has to land there — that tail is exactly the audio the
        // barge is trying to stop.
        let (dev, mut host) = duplex(1 << 16);
        let (events, seen) = event_collector();
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

        // 500 ms of audio inside a 1 s lead: the whole clip is written in one burst
        // with no time advancing, so `Written` lands and `Finished` does not.
        handle
            .try_play(job_with_id(vec![5i16; AUDIO_SAMPLES_PER_FRAME * 25], 42))
            .unwrap();
        run_until(|| stats.snapshot().eoa_written == 1).await;
        // Into the audible part of the tail, still short of its end.
        tokio::time::advance(Duration::from_millis(
            audio_pipeline::playback::PLAYBACK_PLAYOUT_HOP_MS + 100,
        ))
        .await;
        run_until(|| stats.snapshot().jobs_completed == 1).await;

        let progress = handle
            .flush(UtteranceId(42))
            .expect("the tail is still audible, so the turn flushes");
        assert!(
            progress.heard_ms > 0 && progress.heard_ms < 500,
            "heard {} ms: part of the clip, not all of it",
            progress.heard_ms,
        );
        run_until(|| stats.snapshot().jobs_flushed == 1).await;
        assert_eq!(
            flushed_events(&seen),
            vec![(Some(UtteranceId(42)), true, progress)],
            "the tail is reported cut, with what was heard of it",
        );
        assert!(
            !seen
                .lock()
                .unwrap()
                .iter()
                .any(|e| matches!(e, PlaybackEvent::Finished { .. })),
            "a cut tail is never heard to its end",
        );

        drop(handle);
        reader.await.expect("reader");
    }

    #[tokio::test(start_paused = true)]
    async fn flush_after_the_audible_end_is_a_no_op() {
        // The signal names a turn whose job has been heard out: the writer must drop
        // it rather than cut whatever plays next.
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

        // Turn 42's job plays and is heard out: the clock has to pass its audible
        // end, which trails the last write by the device's playout hop.
        handle
            .try_play(job_with_id(vec![1i16; AUDIO_SAMPLES_PER_FRAME], 42))
            .unwrap();
        run_until(|| stats.snapshot().jobs_completed == 1).await;
        tokio::time::advance(Duration::from_millis(
            audio_pipeline::playback::PLAYBACK_PLAYOUT_HOP_MS + FRAME_MS,
        ))
        .await;
        // Wait for the retirement itself, not for the flush to start refusing:
        // asking early would set a target the writer then has to drop.
        run_until(|| {
            seen.lock()
                .unwrap()
                .iter()
                .any(|e| matches!(e, PlaybackEvent::Finished { .. }))
        })
        .await;
        assert_eq!(
            handle.flush(UtteranceId(42)),
            Err(FlushRejected::NotPlaying),
            "nothing is audible once the clip has been heard out",
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
            "a flush for a turn heard out cuts nothing",
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
    async fn eoa_write_failure_reports_written_then_aborted_and_drains_racer() {
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
        assert_eq!(
            event_tags(&seen).last().map(String::as_str),
            Some("silent"),
            "one silence, after the whole aborted set",
        );

        let seen = seen.lock().unwrap();
        let kinds: Vec<&str> = seen
            .iter()
            .filter_map(|e| match e {
                PlaybackEvent::Written {
                    in_reply_to: Some(UtteranceId(10)),
                    eoa_written: false,
                    ..
                } => Some("written"),
                PlaybackEvent::Aborted {
                    in_reply_to: Some(UtteranceId(10)),
                    ..
                } => Some("aborted"),
                PlaybackEvent::Finished { .. } => Some("finished"),
                _ => None,
            })
            .collect();
        assert_eq!(
            kinds,
            ["written", "aborted"],
            "the job's audio was all written and then lost with the stream: no \
             `Finished` claims a tail the host cannot see the end of",
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
        assert_eq!(snap.jobs_completed, 1, "every frame of it went out");
        assert_eq!(snap.eoa_written, 0);
        assert_eq!(
            snap.jobs_aborted, 2,
            "the job whose stream was lost, and the racer behind it"
        );
        assert_eq!(snap.write_timeouts, 1);
        assert_eq!(snap.eoa_write_failures, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_empty_clips_eoa_failure_leaves_the_banked_job_its_own_ending() {
        // An empty clip never reaches the stream, so it is not the job on the back
        // of `pending` — the previous job, still audible, is. When the drain's
        // `EndOfAudio` fails under an empty clip, the ledger's one invariant still
        // has to hold: every job that started gets exactly one terminal event, and
        // it is the one describing what happened to *it*.
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

        // Room for Hello and the one audio frame, and nothing for the EndOfAudio.
        let (_dev_read, host) = duplex(hello_len + audio_len);
        let (events, seen) = event_collector();
        let stats = Arc::new(PlaybackStats::default());
        let handle = PlaybackWriter::spawn(
            host,
            PodId("pod-x".into()),
            PacerConfig {
                write_timeout_ms: 1_000,
                job_queue_depth: 4,
                ..PacerConfig::default()
            },
            Arc::clone(&stats),
            events,
            CancellationToken::new(),
        );
        // Both queued before the writer runs, so 10 has a job behind it and writes
        // no end-of-audio of its own; 11's drain is the one that fails.
        handle
            .try_play(job_with_id(vec![0i16; AUDIO_SAMPLES_PER_FRAME], 10))
            .unwrap();
        handle.try_play(job_with_id(Vec::new(), 11)).unwrap();
        drop(handle);
        // Let the writer send Hello + audio and park in the EndOfAudio write.
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(1_001)).await;
        run_until(|| stats.snapshot().jobs_aborted == 2).await;

        let tags = event_tags(&seen);
        let terminal: Vec<&String> = tags
            .iter()
            .filter(|t| t.starts_with("finished:") || t.starts_with("aborted:"))
            .collect();
        assert_eq!(
            terminal,
            ["aborted:11", "aborted:10"],
            "one ending each, and the banked job's is its own: {tags:?}",
        );
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

    /// A bare writer over `dev`, for the unit-level checks on `take_flush_for`.
    fn bare_writer(dev: DuplexStream) -> Writer<DuplexStream> {
        Writer {
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
            pending: VecDeque::new(),
            deferred: VecDeque::new(),
            queue_closed: false,
            last_audible: None,
        }
    }

    /// One banked job for `turn`, as `pending` holds it: audible from `now`, its
    /// end not yet dated.
    fn banked(turn: u64) -> Pending {
        Pending {
            job: job_with_id(vec![0i16; AUDIO_SAMPLES_PER_FRAME], turn),
            frames: 1,
            samples: AUDIO_SAMPLES_PER_FRAME as u64,
            eoa_written: false,
            starts_at: Instant::now(),
            ends_at: None,
        }
    }

    #[tokio::test]
    async fn take_flush_for_drops_a_signal_naming_another_turn() {
        // The writer-side re-check: a flush signal could race a retirement and end
        // up naming a turn nobody can still hear. Such a signal is dropped (never
        // taken) and cleared, so it cannot later cut the wrong response.
        let (dev, _host) = duplex(1 << 16);
        let mut writer = bare_writer(dev);
        writer.pending.push_back(banked(1));

        *writer.flush_signal.target.lock().unwrap() = Some(UtteranceId(2));
        assert_eq!(
            writer.take_flush_for(),
            None,
            "a signal for a turn other than the audible one is not taken",
        );
        assert!(
            writer.flush_signal.target.lock().unwrap().is_none(),
            "the stale target is cleared, not left to fire on a later job",
        );

        // A signal naming the audible turn is taken and consumed.
        *writer.flush_signal.target.lock().unwrap() = Some(UtteranceId(1));
        assert_eq!(writer.take_flush_for(), Some(Some(UtteranceId(1))));
        assert!(writer.flush_signal.target.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn take_flush_for_matches_the_audible_job_not_the_one_being_written() {
        // The case the front-of-queue match exists for: clip A's tail is still
        // audible while clip B of a later turn is being written. A barge against A
        // is the legitimate one, and comparing against the job under the write head
        // would swallow it.
        let (dev, _host) = duplex(1 << 16);
        let mut writer = bare_writer(dev);
        writer.pending.push_back(banked(1));
        writer.pending.push_back(banked(2));

        *writer.flush_signal.target.lock().unwrap() = Some(UtteranceId(2));
        assert_eq!(
            writer.take_flush_for(),
            None,
            "the turn being written is not the audible one",
        );
        *writer.flush_signal.target.lock().unwrap() = Some(UtteranceId(1));
        assert_eq!(
            writer.take_flush_for(),
            Some(Some(UtteranceId(1))),
            "the audible turn matches even with a later turn on the stream",
        );
    }

    #[tokio::test]
    async fn take_flush_for_with_nothing_banked_drops_the_signal() {
        // Nothing is audible, so no target can be legitimate; the handle would
        // have answered `NotPlaying` before setting one.
        let (dev, _host) = duplex(1 << 16);
        let writer = bare_writer(dev);
        *writer.flush_signal.target.lock().unwrap() = Some(UtteranceId(1));
        assert_eq!(writer.take_flush_for(), None);
        assert!(writer.flush_signal.target.lock().unwrap().is_none());
    }
}
