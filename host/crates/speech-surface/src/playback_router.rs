//! The playback router: one task owning the `SpeakCmd` receiver, resolving each
//! command's target pod to its live `PlaybackWriter` and enqueueing the clip.
//!
//! `SpeakBody::Pcm` is stamped with a `speak_rx` receipt time, turned into a
//! `PlaybackJob`, and routed to the target pod's handle in the `PlaybackRegistry`:
//! a hit enqueues (the writer emits the playback-lifecycle lines from there); a
//! miss drops the job as stale (`playback_no_pod`) rather than holding speech for
//! an absent pod. A full or dead writer surfaces as `playback_rejected` /
//! `playback_writer_dead`. `SpeakBody::Text` is synthesized to PCM when a
//! synthesizer is wired (emitting a `synth` line, then falling into the `Pcm`
//! path) and is a counted `speak_unsupported` rejection when one is not — an
//! explicit seam, not a panic. The task exits when the channel closes (pipeline
//! ended) or the shutdown token fires.
//!
//! Every way a command ends without reaching a writer settles it against the turn
//! ledger and reports the turn's accounting to the motion scripter. A command a
//! writer took is settled by the playback event that ends it; one no writer took
//! has no such event, so nothing else would ever account for it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use audio_pipeline::vad::VAD_HANGOVER_MS;
use futures::StreamExt;
use futures::channel::mpsc;
use futures::future::BoxFuture;
use pod_ingest::HostMicros;
use serde::Serialize;
use serde_json::json;
use speech_pipeline::{
    FRAME_MS, Feed, PlayRejected, PlaybackEvent, PlaybackEventFn, PlaybackJob, PodId, SpeakBody,
    SpeakCmd, StageTimings, SynthesisError, Synthesizer, UtteranceId, signed_offset_us,
    stage_delta_us,
};
use tokio_util::sync::CancellationToken;

use crate::barge::{TurnAudio, TurnLedger};
use crate::jsonl::JsonlHandle;
use crate::scripter::{ScriptHandle, ScriptInput};
use crate::server::{PlaybackRegistry, playback_try_play};

/// Router-side counters, atomics-only with a `Copy` snapshot for `stage_health`
/// (the `WakeStats` idiom). The writer-side rejections (`QueueFull`/`WriterDead`)
/// are counted in `PlaybackStats` at `try_play`; these are the outcomes only the
/// router sees.
#[derive(Debug, Default)]
pub struct RouterStats {
    delivered: AtomicU64,
    no_pod: AtomicU64,
    unsupported: AtomicU64,
    interrupted: AtomicU64,
}

/// A point-in-time copy of [`RouterStats`], for `stage_health` reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RouterStatsSnapshot {
    /// `SpeakCmd`s enqueued onto a live writer.
    pub delivered: u64,
    /// `SpeakCmd`s dropped because the target pod had no registered writer.
    pub no_pod: u64,
    /// `SpeakCmd`s rejected because their body was `Text` with no synthesizer wired.
    pub unsupported: u64,
    /// `SpeakCmd`s dropped because the turn they reply to was barged in on.
    pub interrupted: u64,
}

impl RouterStats {
    fn record_delivered(&self) {
        self.delivered.fetch_add(1, Ordering::Relaxed);
    }
    fn record_no_pod(&self) {
        self.no_pod.fetch_add(1, Ordering::Relaxed);
    }
    fn record_unsupported(&self) {
        self.unsupported.fetch_add(1, Ordering::Relaxed);
    }
    fn record_interrupted(&self) {
        self.interrupted.fetch_add(1, Ordering::Relaxed);
    }

    /// A `Copy` snapshot of the counters, read for `stage_health`.
    pub fn snapshot(&self) -> RouterStatsSnapshot {
        RouterStatsSnapshot {
            delivered: self.delivered.load(Ordering::Relaxed),
            no_pod: self.no_pod.load(Ordering::Relaxed),
            unsupported: self.unsupported.load(Ordering::Relaxed),
            interrupted: self.interrupted.load(Ordering::Relaxed),
        }
    }
}

/// The playback router task: owns the loop-invariant state (the registry, stats,
/// JSONL handle, shutdown token, and optional synthesizer) so adding state does
/// not thread another positional argument through `run` and `route`. Built once
/// at spawn, then driven by [`Router::run`]. This mirrors the `PipelineCtx` shape
/// the pipeline uses for the same reason.
pub(crate) struct Router {
    registry: PlaybackRegistry,
    stats: Arc<RouterStats>,
    jsonl: JsonlHandle,
    cancel: CancellationToken,
    synthesizer: Option<Arc<dyn Synthesizer>>,
    ledger: Arc<TurnLedger>,
    /// The motion scripter, when a presence channel is configured. The head's
    /// ending is scheduled from the turn accounting this router changes every
    /// time it gives up on a command, so the tap belongs here as much as on the
    /// playback fan-out.
    scripter: Option<ScriptHandle>,
}

impl Router {
    pub(crate) fn new(
        registry: PlaybackRegistry,
        stats: Arc<RouterStats>,
        jsonl: JsonlHandle,
        cancel: CancellationToken,
        synthesizer: Option<Arc<dyn Synthesizer>>,
        ledger: Arc<TurnLedger>,
        scripter: Option<ScriptHandle>,
    ) -> Self {
        Self {
            registry,
            stats,
            jsonl,
            cancel,
            synthesizer,
            ledger,
            scripter,
        }
    }

    /// Run the router loop until the `SpeakCmd` channel closes or `cancel` fires.
    pub(crate) async fn run(self, mut rx: mpsc::Receiver<SpeakCmd>) {
        loop {
            let cmd = tokio::select! {
                biased;
                _ = self.cancel.cancelled() => break,
                next = rx.next() => match next {
                    Some(cmd) => cmd,
                    None => break,
                },
            };
            self.route(cmd).await;
        }
    }

    /// Route one `SpeakCmd`: stamp receipt time, resolve the target, and emit the
    /// outcome line plus counter. A `Text` body is first synthesized to PCM when a
    /// synthesizer is wired; the synth await races `cancel` so shutdown is never
    /// held hostage by a slow backend (the client timeout bounds it anyway).
    ///
    /// The turn is checked against the barge-in ledger three times — on dequeue,
    /// during the synthesis await, and once more after it returns — because an
    /// interrupt can land at any of those moments and the response of a turn the
    /// user cut off should never reach the pod.
    ///
    /// Every way out of here that is not a delivered job goes through
    /// [`Router::abandon`]: the command is settled and the turn's accounting
    /// reported, so a turn whose speech partly played still gets its ending
    /// scheduled from the part that did.
    async fn route(&self, cmd: SpeakCmd) {
        let speak_rx = HostMicros::now();
        let mut timings = cmd.timings;
        // Brain end: the reply is in hand. TTS begin is `synth_started`, stamped
        // inside the synthesis await below, not here.
        self.jsonl.emit(
            "speak_rx",
            &json!({
                "pod": cmd.target,
                "utterance": cmd.in_reply_to,
                "body": match &cmd.body {
                    SpeakBody::Text(_) => "text",
                    SpeakBody::Pcm(_) => "pcm",
                },
            }),
        );
        // Queued behind the barge: the interrupt landed while this command sat in
        // the channel.
        if self.ledger.is_interrupted(&cmd.target, cmd.in_reply_to) {
            self.drop_interrupted(&cmd.target, cmd.in_reply_to, "queue");
            return;
        }
        let pcm = match cmd.body {
            SpeakBody::Pcm(pcm) => pcm,
            SpeakBody::Text(text) => {
                let Some(synthesizer) = self.synthesizer.as_ref() else {
                    self.stats.record_unsupported();
                    self.jsonl.emit(
                        "speak_unsupported",
                        &json!({ "pod": cmd.target, "utterance": cmd.in_reply_to }),
                    );
                    self.abandon(&cmd.target, cmd.in_reply_to);
                    return;
                };
                let started = Instant::now();
                timings.synth_started = Some(HostMicros::now());
                let result = match self
                    .synthesize_interruptible(synthesizer, &text, &cmd.target, cmd.in_reply_to)
                    .await
                {
                    SynthOutcome::Done(r) => r,
                    // The HTTP future is dropped mid-flight; the backend's work is
                    // for a turn nobody is listening to any more.
                    SynthOutcome::Interrupted => {
                        self.drop_interrupted(&cmd.target, cmd.in_reply_to, "synth");
                        return;
                    }
                    // Process shutdown: the loop breaks next, and the commands
                    // still in the channel are abandoned unsettled with it.
                    // Settling this one alone would say nothing about the turn,
                    // and no head is waiting on the answer — the daemon's own
                    // script timeout stows it.
                    SynthOutcome::Cancelled => return,
                };
                let synth_us = started.elapsed().as_micros() as u64;
                timings.synth_completed = Some(HostMicros::now());
                match result {
                    Ok(pcm) => {
                        self.jsonl.emit(
                            "synth",
                            &json!({
                                "pod": cmd.target,
                                "utterance": cmd.in_reply_to,
                                "input_chars": text.chars().count(),
                                "samples": pcm.len(),
                                "synth_us": synth_us,
                            }),
                        );
                        pcm
                    }
                    Err(e) => {
                        self.jsonl.emit(
                            "synth_failed",
                            &json!({
                                "pod": cmd.target,
                                "utterance": cmd.in_reply_to,
                                "detail": e.to_string(),
                                "elapsed_us": synth_us,
                            }),
                        );
                        self.abandon(&cmd.target, cmd.in_reply_to);
                        return;
                    }
                }
            }
        };
        // Synthesis completed just as the interrupt landed: the notify fired before
        // this await ever parked, or after it resolved.
        if self.ledger.is_interrupted(&cmd.target, cmd.in_reply_to) {
            self.drop_interrupted(&cmd.target, cmd.in_reply_to, "post_synth");
            return;
        }
        let job = PlaybackJob {
            pcm,
            in_reply_to: cmd.in_reply_to,
            interruptible: cmd.interruptible,
            timings,
            speak_rx,
        };
        let outcome = playback_try_play(&self.registry, &cmd.target, job);
        let delivered = emit_outcome(
            &cmd.target,
            cmd.in_reply_to,
            outcome,
            &self.stats,
            &self.jsonl,
        );
        if !delivered {
            self.abandon(&cmd.target, cmd.in_reply_to);
        }
    }

    /// Give up on one command that will never play: settle it against the turn
    /// ledger and report the turn's accounting to the scripter.
    ///
    /// A command a writer accepted is settled by the playback event that ends it.
    /// One no writer accepted — no synthesizer for a `Text` body, a synthesis
    /// failure, an absent pod, a full or dead writer — produces no playback event
    /// at all, so without this the turn's `awaiting_start` never falls to zero:
    /// the closing script never goes out and the head waits out the hold script's
    /// timeout instead of stowing after the speech that did play.
    ///
    /// `clean` is false on every one of these paths — nothing reached the user,
    /// so the pod's barge chain must not clear on them.
    ///
    /// A turn the ledger no longer holds answers `None` and reports nothing; a
    /// barged turn is exactly that case, and the barge has already raised the head
    /// on its own account.
    fn abandon(&self, pod: &PodId, turn: Option<UtteranceId>) {
        let audio = self.ledger.settle_job(pod, turn, false);
        tell_scripter(self.scripter.as_ref(), pod, turn, audio);
    }

    /// Synthesize `text`, racing the interrupt of `turn` and process shutdown. The
    /// notify is armed *before* the mark is re-checked, so an interrupt landing in
    /// the gap between the two wakes the select rather than being missed.
    async fn synthesize_interruptible(
        &self,
        synthesizer: &Arc<dyn Synthesizer>,
        text: &str,
        pod: &PodId,
        turn: Option<UtteranceId>,
    ) -> SynthOutcome {
        let mut synth = std::pin::pin!(synthesize_text(synthesizer, text));
        loop {
            let notified = self.ledger.interrupted_notify().notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();
            if self.ledger.is_interrupted(pod, turn) {
                return SynthOutcome::Interrupted;
            }
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return SynthOutcome::Cancelled,
                // Some turn was interrupted; the loop's re-check says whether it
                // was this one, and re-arms the notify if it was not.
                _ = notified.as_mut() => continue,
                r = synth.as_mut() => return SynthOutcome::Done(r),
            }
        }
    }

    /// Drop one command for a barged-in turn: the line, the counter, and the same
    /// give-up ending every other unplayable command takes. `during` names which
    /// of the three checks caught it.
    fn drop_interrupted(&self, pod: &PodId, turn: Option<UtteranceId>, during: &str) {
        self.stats.record_interrupted();
        self.jsonl.emit(
            "speak_interrupted",
            &json!({ "pod": pod, "utterance": turn, "during": during }),
        );
        self.abandon(pod, turn);
    }
}

/// How a [`Router::synthesize_interruptible`] await ended.
enum SynthOutcome {
    Done(Result<Arc<[i16]>, SynthesisError>),
    /// The turn was barged in on; the synthesis future was dropped.
    Interrupted,
    /// Process shutdown.
    Cancelled,
}

/// Drive a synthesizer stream to completion, concatenating its chunks into one
/// PCM buffer. Returns the terminal `Err` if the stream yields one, or a
/// `Decode` failure if the stream ends without a chunk (an implementation bug,
/// still handled — never an empty, EOA-only playback job). The single-chunk case
/// (the only shape the HTTP backend produces today) hands the chunk's `Arc` on
/// with no data copy; multi-chunk streams fall back to accumulation.
async fn synthesize_text(
    synthesizer: &Arc<dyn Synthesizer>,
    text: &str,
) -> Result<Arc<[i16]>, SynthesisError> {
    let mut stream = synthesizer.synthesize(text);
    let first = match stream.next().await {
        Some(Ok(chunk)) => chunk.pcm,
        Some(Err(e)) => return Err(e),
        None => {
            return Err(SynthesisError::Decode(
                "stream ended without a chunk".into(),
            ));
        }
    };
    // Peek for a second chunk: a stream that ends here (today's sole shape) reuses
    // the first chunk's `Arc` directly. Only a genuine multi-chunk stream pays the
    // Vec accumulation and the final `Arc::from` copy.
    let second = match stream.next().await {
        None => {
            if first.is_empty() {
                return Err(SynthesisError::Decode(
                    "stream produced only empty chunks".into(),
                ));
            }
            return Ok(first);
        }
        Some(Ok(chunk)) => chunk.pcm,
        Some(Err(e)) => return Err(e),
    };
    let mut samples: Vec<i16> = Vec::with_capacity(first.len() + second.len());
    samples.extend_from_slice(&first);
    samples.extend_from_slice(&second);
    while let Some(item) = stream.next().await {
        match item {
            Ok(chunk) => samples.extend_from_slice(&chunk.pcm),
            Err(e) => return Err(e),
        }
    }
    if samples.is_empty() {
        return Err(SynthesisError::Decode(
            "stream produced only empty chunks".into(),
        ));
    }
    Ok(Arc::from(samples))
}

/// Emit the JSONL line and bump the counter for one resolution result, and answer
/// whether a writer took the job. `Delivered` is silent on the wire — the writer
/// emits the playback-lifecycle lines from there; the `QueueFull`/`WriterDead`
/// counters are already bumped inside `try_play`, so here only their lines are the
/// router's half.
///
/// The answer is what tells the caller whether any playback event will ever name
/// this job: a refused one produces none, and its turn has to be settled here
/// instead.
fn emit_outcome(
    target: &PodId,
    in_reply_to: Option<UtteranceId>,
    outcome: Option<Result<(), PlayRejected>>,
    stats: &RouterStats,
    jsonl: &JsonlHandle,
) -> bool {
    match outcome {
        Some(Ok(())) => {
            stats.record_delivered();
            return true;
        }
        None => {
            stats.record_no_pod();
            jsonl.emit(
                "playback_no_pod",
                &json!({ "pod": target, "utterance": in_reply_to }),
            );
        }
        Some(Err(PlayRejected::QueueFull)) => jsonl.emit(
            "playback_rejected",
            &json!({ "pod": target, "utterance": in_reply_to }),
        ),
        Some(Err(PlayRejected::WriterDead)) => jsonl.emit(
            "playback_writer_dead",
            &json!({ "pod": target, "utterance": in_reply_to }),
        ),
    }
    false
}

/// Hands one `Feed` to the listener. A closure rather than the `ListenerHandle`
/// itself: the handle owns a live inference thread, so taking the narrow thing the
/// fan-out actually needs is what makes the floor's behaviour testable without one.
pub(crate) type FeedFn = Arc<dyn Fn(PodId, Feed) -> BoxFuture<'static, ()> + Send + Sync>;

/// What the playback-event adapter fans each event out to, beyond its JSONL line:
/// the listener's playback floor and the barge-in ledger's settlement accounting.
/// Absent in pipelines with no listener and no barge-in path (the replay rigs).
pub(crate) struct PlaybackFanout {
    pub(crate) feed: FeedFn,
    pub(crate) ledger: Arc<TurnLedger>,
    /// The motion scripter, when a presence channel is configured. It reads the
    /// same accounting the ledger keeps: when a turn's speech started and how
    /// long it is, which is what schedules the head's ending.
    pub(crate) scripter: Option<ScriptHandle>,
}

/// Build the `PlaybackEventFn` handed to every `PlaybackWriter` at spawn: the
/// closure that turns each writer-emitted `PlaybackEvent` into one JSONL line, and
/// — when `fanout` is wired — drives the listener's playback floor and the ledger's
/// settlement accounting from the same events. Emitting from the closure (i.e. from
/// the writer task itself) keeps playback lifecycle lines off the router loop's
/// critical path. `clock_step_clamps` is the process-wide counter every
/// `stage_delta_us` shares, so a clamped backward clock step in a latency line is
/// corroborated against the `stage_health` count.
///
/// This is the single place playback lifecycle fans out: detection's view of
/// whether a pod is speaking, and the ledger's view of whether a turn made it out
/// intact, both ride the events the JSONL lines already ride.
pub(crate) fn playback_event_adapter(
    jsonl: JsonlHandle,
    clock_step_clamps: Arc<AtomicU64>,
    fanout: Option<PlaybackFanout>,
) -> PlaybackEventFn {
    // `Arc`'d so each emitted event's future owns a cheap handle rather than
    // borrowing the closure's capture.
    let fanout = Arc::new(fanout);
    Arc::new(move |event| {
        let fanout = Arc::clone(&fanout);
        let jsonl = jsonl.clone();
        let clock_step_clamps = Arc::clone(&clock_step_clamps);
        Box::pin(async move {
            if let Some(fanout) = fanout.as_ref() {
                fan_out_playback_event(&event, fanout).await;
            }
            emit_playback_event(event, &jsonl, &clock_step_clamps);
        })
    })
}

/// Report a turn's cmd accounting to the scripter, as the ledger call that
/// changed it answered. The one home for that report: the playback fan-out uses
/// it for every terminal event, and the router for every command it gives up on.
///
/// Tapped at the event: the horizon this carries is dated from when the clip
/// started, not from when anybody heard about it, and the head's timings are
/// seconds, so the pacer's lead between the first write and the first heard
/// sample is noise at that scale.
///
/// A turn the ledger no longer holds — interrupted, or completed and retired —
/// answers nothing, and there is nothing to say about it: the barge that cut it
/// has already raised the head on its own account.
fn tell_scripter(
    scripter: Option<&ScriptHandle>,
    pod: &PodId,
    turn: Option<UtteranceId>,
    audio: Option<TurnAudio>,
) {
    if let (Some(scripter), Some(turn), Some(audio)) = (scripter, turn, audio) {
        scripter.send(ScriptInput::Audio {
            pod: pod.clone(),
            turn,
            audio,
        });
    }
}

/// Drive the listener floor and the ledger from one playback event.
///
/// The floor tells detection whether the pod is speaking, which is what gates the
/// barge trigger. It follows `Audible` and no other event: the floor is one state
/// per pod, and the job-lifecycle events cannot express it once several jobs share
/// a stream — a reply's second clip starts before the first one's audible end, and
/// a job re-written after a flush repeats no lifecycle event at all. The pacer
/// holds the answer and says so.
///
/// The ledger settles every terminal event, so a turn's cmds account for themselves
/// whatever became of them; `clean` is true for a job heard to its end and false
/// for an abort or a cut. `Started` is recorded there too, with the job's sample
/// count: that is what dates the end of the turn's audio while it is still playing,
/// rather than at the ending that reports it after the fact.
///
/// The last write itself — `Written` — moves neither: the audio it hands over is
/// still coming out of the speaker, which is the whole reason the two events are
/// separate. It carries one JSONL line and nothing else.
async fn fan_out_playback_event(event: &PlaybackEvent, fanout: &PlaybackFanout) {
    match event {
        PlaybackEvent::Started {
            pod,
            in_reply_to,
            samples,
            ..
        } => {
            // The tokio clock, not the std one this module measures spans with: the
            // horizon this dates is a deadline something will later sleep until.
            let audio = fanout.ledger.record_started(
                pod,
                *in_reply_to,
                *samples,
                tokio::time::Instant::now(),
            );
            tell_scripter(fanout.scripter.as_ref(), pod, *in_reply_to, audio);
        }
        PlaybackEvent::Audible { pod, job } => {
            (fanout.feed)(
                pod.clone(),
                Feed::PlaybackState {
                    active: job.is_some(),
                    interruptible: job.as_ref().is_some_and(|j| j.interruptible),
                },
            )
            .await;
        }
        // The audio is banked on the device, not heard: nothing to settle and
        // nothing to tell the listener.
        PlaybackEvent::Written { .. } => {}
        PlaybackEvent::Finished {
            pod, in_reply_to, ..
        } => {
            // A job heard to its end settles clean whether or not it drained the
            // stream: a clip that finishes with another queued behind it writes no
            // end-of-audio yet delivered all its audio. Whether the pod fell silent
            // here is the `Audible` behind this event's business, not this arm's.
            let audio = fanout.ledger.settle_job(pod, *in_reply_to, true);
            tell_scripter(fanout.scripter.as_ref(), pod, *in_reply_to, audio);
        }
        PlaybackEvent::Aborted {
            pod, in_reply_to, ..
        } => {
            let audio = fanout.ledger.settle_job(pod, *in_reply_to, false);
            tell_scripter(fanout.scripter.as_ref(), pod, *in_reply_to, audio);
        }
        PlaybackEvent::Flushed {
            pod, in_reply_to, ..
        } => {
            // Both halves of a cut settle the same way: the job that was audible and
            // every job of its turn evicted behind it delivered no whole reply.
            let audio = fanout.ledger.settle_job(pod, *in_reply_to, false);
            tell_scripter(fanout.scripter.as_ref(), pod, *in_reply_to, audio);
        }
        PlaybackEvent::HelloWritten { .. } | PlaybackEvent::HelloFailed { .. } => {}
    }
}

/// The `latency_summary` line: the whole segment-and-response cycle accounted for
/// at the instant the response starts playing. Two field groups with different
/// clock-step semantics.
///
/// **Offsets** (`*_ms`, signed) put every stage on one axis anchored at t0 —
/// `first_audio_rx`, host receipt of the utterance's first audio. `vad_high_ms`
/// and `wake_ms` are legitimately negative (the device preroll precedes its own
/// segment; the arm window accepts a wake before the utterance starts), so they
/// are computed signed and unclamped — see [`signed_offset_us`]. `t0_projected`
/// says whether the axis origin was measured or projected off the device clock.
///
/// **Blame** (`*_us`) is the consecutive-stage contributions, clamped by
/// [`stage_delta_us`] because a negative one there really is a clock step. The
/// intervals partition `soft_endpoint_rx → first_write` without overlap, so they
/// sum to `first_write_ms − soft_endpoint_ms`: `speak_rx → first_write` *contains*
/// the synthesis await, so it is split around it rather than reported alongside
/// `tts_us`. A `Pcm` body synthesizes nothing and carries the unsplit
/// `speak_to_first_write_us` instead, with the three synth-era fields `null`.
/// Everything before the soft endpoint is speech plus designed hangover, not
/// blameable pipeline latency; the offsets group still shows it.
fn latency_summary(
    pod: &PodId,
    in_reply_to: Option<UtteranceId>,
    timings: &StageTimings,
    speak_rx: HostMicros,
    first_write: HostMicros,
    clamps: &AtomicU64,
) -> serde_json::Value {
    let t0 = timings.first_audio_rx;
    let offset_ms = |stamp| signed_offset_us(t0, stamp).map(|us| us / 1_000);
    // A `Pcm` body never entered the synthesis await, so it has no synth stamps
    // to split `speak_rx → first_write` around.
    let synthesized = timings.synth_started.is_some();
    json!({
        "pod": pod,
        "utterance": in_reply_to,
        "t0_projected": timings.t0_projected,
        "vad_high_ms": offset_ms(timings.vad_high_est),
        "wake_ms": offset_ms(timings.wake_detected_rx),
        "onset_ms": offset_ms(timings.onset_rx),
        "soft_endpoint_ms": offset_ms(timings.soft_endpoint_rx),
        "stt_start_ms": offset_ms(timings.stt_started),
        "stt_done_ms": offset_ms(timings.transcribed),
        "brain_ms": offset_ms(timings.brain_dispatched),
        "speak_rx_ms": offset_ms(Some(speak_rx)),
        "tts_done_ms": offset_ms(timings.synth_completed),
        "first_write_ms": offset_ms(Some(first_write)),
        "endpoint_to_stt_us":
            stage_delta_us(timings.soft_endpoint_rx, timings.stt_started, clamps),
        "stt_us": stage_delta_us(timings.stt_started, timings.transcribed, clamps),
        "stt_to_brain_us": stage_delta_us(timings.transcribed, timings.brain_dispatched, clamps),
        "brain_us": stage_delta_us(timings.brain_dispatched, Some(speak_rx), clamps),
        "speak_to_synth_start_us":
            stage_delta_us(Some(speak_rx), timings.synth_started, clamps),
        "tts_us": stage_delta_us(timings.synth_started, timings.synth_completed, clamps),
        "synth_to_first_write_us":
            stage_delta_us(timings.synth_completed, Some(first_write), clamps),
        "speak_to_first_write_us": (!synthesized)
            .then(|| stage_delta_us(Some(speak_rx), Some(first_write), clamps))
            .flatten(),
    })
}

/// Map one `PlaybackEvent` to its JSONL line. One-to-one, no silent variants: the
/// adapter is the only place playback lifecycle events reach the wire.
fn emit_playback_event(event: PlaybackEvent, jsonl: &JsonlHandle, clamps: &AtomicU64) {
    match event {
        PlaybackEvent::HelloWritten { pod } => {
            jsonl.emit("playback_hello", &json!({ "pod": pod }));
        }
        PlaybackEvent::HelloFailed { pod, reason } => {
            jsonl.emit(
                "playback_hello_failed",
                &json!({ "pod": pod, "reason": reason }),
            );
        }
        PlaybackEvent::Started {
            pod,
            in_reply_to,
            timings,
            speak_rx,
            first_write,
            samples,
            interruptible,
        } => {
            // The hangover floor comes from the firmware constant's single source
            // of truth. First played sample trails first written sample by the
            // device playout hop, which is not measurable here.
            jsonl.emit(
                "playback_started",
                &json!({
                    "pod": pod,
                    "utterance": in_reply_to,
                    "samples": samples,
                    "interruptible": interruptible,
                    "vad_hangover_floor_ms": VAD_HANGOVER_MS,
                }),
            );
            // First audio byte written to the pod: the response is real, so the
            // whole cycle can be accounted for.
            jsonl.emit(
                "latency_summary",
                &latency_summary(&pod, in_reply_to, &timings, speak_rx, first_write, clamps),
            );
        }
        PlaybackEvent::Written {
            pod,
            in_reply_to,
            frames,
            samples,
            eoa_written,
            rewritten,
            plays_until,
        } => {
            jsonl.emit(
                "playback_written",
                &json!({
                    "pod": pod,
                    "utterance": in_reply_to,
                    "frames": frames,
                    "samples": samples,
                    "eoa_written": eoa_written,
                    // A second pass over a job whose banked frames a flush threw
                    // away. It repeats no `playback_started`, so this is what says
                    // two of these lines for one reply are one answer written
                    // twice rather than two answers.
                    "rewritten": rewritten,
                    // The lead still outstanding at the last write: how much of this
                    // clip the device holds and has not played. The pacer front-loads
                    // up to `lead_ms` of audio, so this is what separates this line's
                    // timestamp from `playback_finished`'s, and it is the number that
                    // says whether the audible-end estimate is worth refining.
                    "banked_ms": plays_until
                        .saturating_duration_since(tokio::time::Instant::now())
                        .as_millis() as u64,
                }),
            );
        }
        PlaybackEvent::Finished {
            pod,
            in_reply_to,
            frames,
            samples,
            eoa_written,
        } => {
            jsonl.emit(
                "playback_finished",
                &json!({
                    "pod": pod,
                    "utterance": in_reply_to,
                    "frames": frames,
                    "samples": samples,
                    "eoa_written": eoa_written,
                    // Nominal audio duration: frame count times one frame's playout
                    // span. Not a measured wall span — the line's own timestamp dates
                    // the estimated audible end, and the pacer's estimate of that is
                    // a lower bound.
                    "nominal_audio_ms": frames * FRAME_MS,
                }),
            );
        }
        PlaybackEvent::Audible { pod, job } => {
            // The floor's own record. Every other line dates a job's lifecycle;
            // this one dates what the pod is heard to be saying, which is the thing
            // detection is gated on — and the only line at all for a hand-over to
            // another turn or for a job re-written after a flush.
            jsonl.emit(
                "playback_audible",
                &json!({
                    "pod": pod,
                    "active": job.is_some(),
                    "utterance": job.as_ref().and_then(|j| j.in_reply_to),
                    "interruptible": job.as_ref().map(|j| j.interruptible),
                }),
            );
        }
        PlaybackEvent::Aborted {
            pod,
            in_reply_to,
            reason,
        } => {
            jsonl.emit(
                "playback_aborted",
                &json!({ "pod": pod, "utterance": in_reply_to, "reason": reason }),
            );
        }
        PlaybackEvent::Flushed {
            pod,
            in_reply_to,
            was_playing,
            frames_written,
            progress,
        } => {
            jsonl.emit(
                "playback_flushed",
                &json!({
                    "pod": pod,
                    "utterance": in_reply_to,
                    "was_playing": was_playing,
                    "frames_written": frames_written,
                    "heard_ms": progress.heard_ms,
                    "total_ms": progress.total_ms,
                }),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    use serde_json::Value;
    use speech_pipeline::{
        AbortReason, AudibleJob, InterruptProgress, PacerConfig, PcmChunk, PlaybackEventFn,
        PlaybackStats, PlaybackWriter, StageTimings,
    };

    use crate::config::JsonlSink;
    use crate::server::playback_register;

    /// A `SpeakCmd` carrying one PCM clip to `target`, replying to `utterance`.
    fn pcm_cmd(target: &str, utterance: u64, pcm: &[i16]) -> SpeakCmd {
        SpeakCmd {
            target: PodId(target.into()),
            in_reply_to: Some(UtteranceId(utterance)),
            body: SpeakBody::Pcm(Arc::from(pcm)),
            interruptible: true,
            timings: StageTimings::default(),
        }
    }

    /// A `SpeakCmd` carrying text (the unsupported body) to `target`.
    fn text_cmd(target: &str, utterance: u64) -> SpeakCmd {
        SpeakCmd {
            target: PodId(target.into()),
            in_reply_to: Some(UtteranceId(utterance)),
            body: SpeakBody::Text("hello".into()),
            interruptible: true,
            timings: StageTimings::default(),
        }
    }

    fn empty_registry() -> PlaybackRegistry {
        Arc::new(Mutex::new(HashMap::new()))
    }

    /// Feed `cmds` through the router against `registry`, returning `(lines, stats)`.
    /// The `SpeakCmd` sender is dropped before `run`, so the channel closes and the
    /// loop drains its buffer and exits without needing the cancel token.
    async fn run_router(
        registry: PlaybackRegistry,
        cmds: Vec<SpeakCmd>,
    ) -> (Vec<Value>, RouterStatsSnapshot) {
        run_router_with_synth(registry, cmds, None).await
    }

    /// Like [`run_router`] but with a synthesizer wired, so `Text` bodies are
    /// synthesized rather than rejected.
    async fn run_router_with_synth(
        registry: PlaybackRegistry,
        cmds: Vec<SpeakCmd>,
        synthesizer: Option<Arc<dyn Synthesizer>>,
    ) -> (Vec<Value>, RouterStatsSnapshot) {
        run_router_full(registry, cmds, synthesizer, Arc::new(TurnLedger::new())).await
    }

    /// Like [`run_router_with_synth`] but against a caller-supplied ledger, so a
    /// test can mark a turn interrupted before the router ever dequeues it.
    async fn run_router_full(
        registry: PlaybackRegistry,
        cmds: Vec<SpeakCmd>,
        synthesizer: Option<Arc<dyn Synthesizer>>,
        ledger: Arc<TurnLedger>,
    ) -> (Vec<Value>, RouterStatsSnapshot) {
        run_router_scripted(registry, cmds, synthesizer, ledger, None).await
    }

    /// Like [`run_router_full`] but with the scripter tap the server wires in, so
    /// a test can watch what a give-up path tells the head.
    async fn run_router_scripted(
        registry: PlaybackRegistry,
        cmds: Vec<SpeakCmd>,
        synthesizer: Option<Arc<dyn Synthesizer>>,
        ledger: Arc<TurnLedger>,
        scripter: Option<ScriptHandle>,
    ) -> (Vec<Value>, RouterStatsSnapshot) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let (jsonl, writer_join) = crate::jsonl::spawn_quiet(&JsonlSink::File(path.clone()))
            .await
            .unwrap();

        let (mut tx, rx) = mpsc::channel::<SpeakCmd>(cmds.len().max(1));
        for cmd in cmds {
            tx.try_send(cmd).expect("test channel has room");
        }
        drop(tx); // Close the channel so `run` returns once drained.

        let stats = Arc::new(RouterStats::default());
        Router::new(
            registry,
            Arc::clone(&stats),
            jsonl.clone(),
            CancellationToken::new(),
            synthesizer,
            ledger,
            scripter,
        )
        .run(rx)
        .await;

        drop(jsonl);
        writer_join.await.unwrap();
        let lines = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        (lines, stats.snapshot())
    }

    #[tokio::test]
    async fn routes_pcm_to_the_registered_pods_writer() {
        // A live writer over a duplex whose peer reader is held open so the writer
        // stays alive; the router should enqueue the clip and stay silent (the
        // writer, not the router, emits the lifecycle lines).
        let (_peer, device) = tokio::io::duplex(64 * 1024);
        let stats = Arc::new(PlaybackStats::default());
        let noop: PlaybackEventFn = Arc::new(|_| Box::pin(std::future::ready(())));
        let handle = PlaybackWriter::spawn(
            device,
            PodId("pod-x".into()),
            PacerConfig::default(),
            stats,
            noop,
            CancellationToken::new(),
        );
        let registry = empty_registry();
        playback_register(&registry, "pod-x".into(), 1, handle);

        let (lines, router_stats) =
            run_router(registry, vec![pcm_cmd("pod-x", 7, &[1, 2, 3])]).await;

        assert_eq!(router_stats.delivered, 1);
        assert_eq!(router_stats.no_pod, 0);
        assert!(
            !lines.iter().any(|v| {
                let e = v["event"].as_str().unwrap();
                e.starts_with("playback_") || e == "speak_unsupported"
            }),
            "router stays silent on a delivered job: {lines:?}"
        );
    }

    /// An announcement's shape, routed through the production path: it plays,
    /// and neither the barge ledger nor the motion scripter hears about it.
    ///
    /// The property the surface's announcement seam rests on. `in_reply_to:
    /// None` is what buys it — there is no turn to settle and none to schedule
    /// a head movement for — so a robot saying its head is dead does not try to
    /// nod while saying so.
    #[tokio::test]
    async fn an_announcement_shaped_command_tells_the_ledger_and_the_scripter_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (jsonl, join) = crate::jsonl::spawn_quiet(&JsonlSink::File(dir.path().join("e")))
            .await
            .unwrap();
        let (scripter, mut inbox) = crate::scripter::channel(jsonl.clone());
        let ledger = Arc::new(TurnLedger::new());

        let (_peer, device) = tokio::io::duplex(64 * 1024);
        let noop: PlaybackEventFn = Arc::new(|_| Box::pin(std::future::ready(())));
        let handle = PlaybackWriter::spawn(
            device,
            PodId("pod-x".into()),
            PacerConfig::default(),
            Arc::new(PlaybackStats::default()),
            noop,
            CancellationToken::new(),
        );
        let registry = empty_registry();
        playback_register(&registry, "pod-x".into(), 1, handle);

        let announcement = SpeakCmd {
            target: PodId("pod-x".into()),
            in_reply_to: None,
            body: SpeakBody::Text("my head is not moving".into()),
            interruptible: false,
            timings: StageTimings::default(),
        };
        let synth: Arc<dyn Synthesizer> = Arc::new(FakeSynth::Chunks(vec![vec![1, 2, 3]]));
        let (_lines, stats) = run_router_scripted(
            registry,
            vec![announcement],
            Some(synth),
            Arc::clone(&ledger),
            Some(scripter.clone()),
        )
        .await;

        assert_eq!(stats.delivered, 1, "the sentence reached the writer");
        assert!(
            ledger.chain(&PodId("pod-x".into())).is_none(),
            "no turn was named, so the ledger has nothing to chain"
        );

        drop(scripter);
        drop(jsonl);
        join.await.unwrap();
        let mut seen = Vec::new();
        while let Some(input) = inbox.recv().await {
            seen.push(input);
        }
        assert!(
            seen.is_empty(),
            "an announcement schedules no head movement: {seen:?}"
        );
    }

    #[tokio::test]
    async fn absent_pod_emits_no_pod_line_and_counter() {
        let (lines, stats) = run_router(empty_registry(), vec![pcm_cmd("ghost", 4, &[9])]).await;

        assert_eq!(stats.no_pod, 1);
        assert_eq!(stats.delivered, 0);
        let line = lines
            .iter()
            .find(|v| v["event"] == "playback_no_pod")
            .expect("a playback_no_pod line");
        assert_eq!(line["pod"], "ghost");
        assert_eq!(line["utterance"], 4);
    }

    #[tokio::test]
    async fn text_body_without_synthesizer_emits_speak_unsupported_line_and_counter() {
        let (lines, stats) = run_router(empty_registry(), vec![text_cmd("pod-x", 5)]).await;

        assert_eq!(stats.unsupported, 1);
        let line = lines
            .iter()
            .find(|v| v["event"] == "speak_unsupported")
            .expect("a speak_unsupported line");
        assert_eq!(line["pod"], "pod-x");
        assert_eq!(line["utterance"], 5);
    }

    /// How a `FakeSynthesizer` responds: yield chunks, yield one chunk only after
    /// a measurable delay, fail terminally, hang, or end without ever yielding a
    /// chunk (the "implementation bug" shape).
    enum FakeSynth {
        Chunks(Vec<Vec<i16>>),
        Slow(std::time::Duration, Vec<i16>),
        Fail,
        Hang,
        Empty,
    }

    impl Synthesizer for FakeSynth {
        fn synthesize(
            &self,
            _text: &str,
        ) -> futures::stream::BoxStream<'static, Result<PcmChunk, SynthesisError>> {
            match self {
                FakeSynth::Chunks(chunks) => {
                    let items: Vec<_> = chunks
                        .iter()
                        .map(|c| {
                            Ok(PcmChunk {
                                pcm: Arc::from(c.as_slice()),
                            })
                        })
                        .collect();
                    futures::stream::iter(items).boxed()
                }
                FakeSynth::Slow(delay, pcm) => {
                    let delay = *delay;
                    let pcm: Arc<[i16]> = Arc::from(pcm.as_slice());
                    futures::stream::once(async move {
                        tokio::time::sleep(delay).await;
                        Ok(PcmChunk { pcm })
                    })
                    .boxed()
                }
                FakeSynth::Fail => {
                    futures::stream::once(async { Err(SynthesisError::Connect("boom".into())) })
                        .boxed()
                }
                FakeSynth::Hang => futures::stream::pending().boxed(),
                FakeSynth::Empty => futures::stream::empty().boxed(),
            }
        }
    }

    #[tokio::test]
    async fn text_body_with_synthesizer_emits_synth_line_and_routes_pcm() {
        // A live writer for the target pod; the synthesized clip should route
        // through as a delivered `PlaybackJob`, and a `synth` line should carry the
        // input chars, the concatenated sample count, and a measured `synth_us`.
        let (_peer, device) = tokio::io::duplex(64 * 1024);
        let stats = Arc::new(speech_pipeline::PlaybackStats::default());
        let noop: PlaybackEventFn = Arc::new(|_| Box::pin(std::future::ready(())));
        let handle = PlaybackWriter::spawn(
            device,
            PodId("pod-x".into()),
            PacerConfig::default(),
            stats,
            noop,
            CancellationToken::new(),
        );
        let registry = empty_registry();
        playback_register(&registry, "pod-x".into(), 1, handle);

        let synth: Arc<dyn Synthesizer> =
            Arc::new(FakeSynth::Chunks(vec![vec![1, 2, 3], vec![4, 5]]));
        let (lines, router_stats) =
            run_router_with_synth(registry, vec![text_cmd("pod-x", 5)], Some(synth)).await;

        assert_eq!(router_stats.delivered, 1);
        assert_eq!(router_stats.unsupported, 0);
        let synth = lines
            .iter()
            .find(|v| v["event"] == "synth")
            .expect("a synth line");
        assert_eq!(synth["pod"], "pod-x");
        assert_eq!(synth["utterance"], 5);
        assert_eq!(synth["input_chars"], 5); // "hello"
        assert_eq!(synth["samples"], 5); // 3 + 2 concatenated
        assert!(synth["synth_us"].as_u64().is_some());
        assert!(
            !lines.iter().any(|v| v["event"] == "speak_unsupported"),
            "a wired synthesizer never emits speak_unsupported: {lines:?}"
        );
    }

    #[tokio::test]
    async fn synth_failure_emits_synth_failed_and_routes_no_job() {
        let synth: Arc<dyn Synthesizer> = Arc::new(FakeSynth::Fail);
        let (lines, router_stats) =
            run_router_with_synth(empty_registry(), vec![text_cmd("pod-x", 9)], Some(synth)).await;

        assert_eq!(router_stats.delivered, 0);
        assert_eq!(router_stats.no_pod, 0);
        assert_eq!(router_stats.unsupported, 0);
        let failed = lines
            .iter()
            .find(|v| v["event"] == "synth_failed")
            .expect("a synth_failed line");
        assert_eq!(failed["pod"], "pod-x");
        assert_eq!(failed["utterance"], 9);
        assert_eq!(failed["detail"], "connect: boom");
        assert!(failed["elapsed_us"].as_u64().is_some());
        assert!(
            !lines.iter().any(|v| v["event"] == "synth"),
            "a failed synth emits no synth line: {lines:?}"
        );
    }

    #[tokio::test]
    async fn synth_empty_stream_emits_synth_failed_decode() {
        // A stream that ends without a chunk (and without an error) must surface
        // as a `Decode` `synth_failed`, never an empty EOA-only playback job.
        let synth: Arc<dyn Synthesizer> = Arc::new(FakeSynth::Empty);
        let (lines, router_stats) =
            run_router_with_synth(empty_registry(), vec![text_cmd("pod-x", 3)], Some(synth)).await;

        assert_eq!(router_stats.delivered, 0);
        let failed = lines
            .iter()
            .find(|v| v["event"] == "synth_failed")
            .expect("a synth_failed line");
        assert_eq!(failed["detail"], "decode: stream ended without a chunk");
        assert!(
            !lines.iter().any(|v| v["event"] == "synth"),
            "an empty synth emits no synth line: {lines:?}"
        );
    }

    #[tokio::test]
    async fn synth_empty_chunks_emit_synth_failed_decode() {
        // A stream that yields only zero-sample chunks decodes to no audio; it must
        // surface as a `Decode` `synth_failed`, not an empty EOA-only playback job.
        // Covers both the single-chunk fast path and the multi-chunk accumulation.
        for chunks in [vec![vec![]], vec![vec![], vec![]]] {
            let synth: Arc<dyn Synthesizer> = Arc::new(FakeSynth::Chunks(chunks));
            let (lines, router_stats) =
                run_router_with_synth(empty_registry(), vec![text_cmd("pod-x", 3)], Some(synth))
                    .await;

            assert_eq!(router_stats.delivered, 0);
            let failed = lines
                .iter()
                .find(|v| v["event"] == "synth_failed")
                .expect("a synth_failed line");
            assert_eq!(
                failed["detail"],
                "decode: stream produced only empty chunks"
            );
            assert!(
                !lines.iter().any(|v| v["event"] == "synth"),
                "an empty-chunk synth emits no synth line: {lines:?}"
            );
        }
    }

    #[tokio::test]
    async fn cancel_during_slow_synth_exits_router() {
        // A synthesizer that never yields; the router parks in the synth await. A
        // cancel fired while it is parked must end `run` promptly (the biased select
        // resolves on the token) with no synth/synth_failed line.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let (jsonl, writer_join) = crate::jsonl::spawn_quiet(&JsonlSink::File(path.clone()))
            .await
            .unwrap();

        let (mut tx, rx) = mpsc::channel::<SpeakCmd>(4);
        tx.try_send(text_cmd("pod-x", 1))
            .expect("test channel has room");

        let cancel = CancellationToken::new();
        let stats = Arc::new(RouterStats::default());
        let synth: Arc<dyn Synthesizer> = Arc::new(FakeSynth::Hang);
        let handle = tokio::spawn(
            Router::new(
                empty_registry(),
                Arc::clone(&stats),
                jsonl.clone(),
                cancel.clone(),
                Some(synth),
                Arc::new(TurnLedger::new()),
                None,
            )
            .run(rx),
        );

        // Let the router receive the command and park in the synth await, then cancel.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        cancel.cancel();

        tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .expect("run returns promptly after cancel during a slow synth")
            .unwrap();

        // The sender stays alive until here.
        drop(tx);
        drop(jsonl);
        writer_join.await.unwrap();

        let contents = std::fs::read_to_string(&path).unwrap();
        assert!(
            !contents.contains("synth"),
            "no synth/synth_failed line for the cancelled command: {contents:?}"
        );
    }

    /// A minimal `PlaybackJob` for exercising a writer handle directly.
    fn dummy_job() -> PlaybackJob {
        PlaybackJob {
            pcm: Arc::from(&[0i16][..]),
            in_reply_to: None,
            interruptible: true,
            timings: StageTimings::default(),
            speak_rx: HostMicros::now(),
        }
    }

    #[tokio::test]
    async fn queue_full_routes_through_to_playback_rejected_line() {
        // A writer whose peer never reads parks on the eager Hello write (an 8-byte
        // duplex the Hello frame overflows), so it never drains its job queue. With
        // `job_queue_depth: 1`, the first routed command enqueues (delivered) and the
        // second overflows to a real `QueueFull` — surfacing as `playback_rejected`
        // through the full `route` → `playback_try_play` → `emit_outcome` path, not by
        // calling `emit_outcome` with a hand-built `Err`.
        let (_peer, device) = tokio::io::duplex(8);
        let cfg = PacerConfig {
            lead_ms: 250,
            write_timeout_ms: 60_000,
            job_queue_depth: 1,
        };
        let stats = Arc::new(PlaybackStats::default());
        let noop: PlaybackEventFn = Arc::new(|_| Box::pin(std::future::ready(())));
        let handle = PlaybackWriter::spawn(
            device,
            PodId("pod-x".into()),
            cfg,
            stats,
            noop,
            CancellationToken::new(),
        );
        let registry = empty_registry();
        playback_register(&registry, "pod-x".into(), 1, handle);

        let (lines, router_stats) = run_router(
            registry,
            vec![
                pcm_cmd("pod-x", 1, &[1, 2, 3]),
                pcm_cmd("pod-x", 2, &[4, 5, 6]),
            ],
        )
        .await;

        assert_eq!(router_stats.delivered, 1);
        let rejected = lines
            .iter()
            .find(|v| v["event"] == "playback_rejected")
            .expect("a playback_rejected line");
        assert_eq!(rejected["pod"], "pod-x");
        assert_eq!(rejected["utterance"], 2);
    }

    #[tokio::test]
    async fn writer_dead_routes_through_to_playback_writer_dead_line() {
        // Drop the writer's peer so its eager Hello write errors and the task exits;
        // once its job-queue receiver is gone, a routed command resolves to a real
        // `WriterDead` and surfaces as `playback_writer_dead` through the full route
        // path.
        let (peer, device) = tokio::io::duplex(64 * 1024);
        drop(peer);
        let stats = Arc::new(PlaybackStats::default());
        let noop: PlaybackEventFn = Arc::new(|_| Box::pin(std::future::ready(())));
        let handle = PlaybackWriter::spawn(
            device,
            PodId("pod-x".into()),
            PacerConfig::default(),
            stats,
            noop,
            CancellationToken::new(),
        );

        // Wait for the writer to observe the broken pipe and exit before routing.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if matches!(handle.try_play(dummy_job()), Err(PlayRejected::WriterDead)) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the writer becomes dead");

        let registry = empty_registry();
        playback_register(&registry, "pod-x".into(), 1, handle);

        let (lines, router_stats) =
            run_router(registry, vec![pcm_cmd("pod-x", 9, &[1, 2, 3])]).await;

        assert_eq!(router_stats.delivered, 0);
        let dead = lines
            .iter()
            .find(|v| v["event"] == "playback_writer_dead")
            .expect("a playback_writer_dead line");
        assert_eq!(dead["pod"], "pod-x");
        assert_eq!(dead["utterance"], 9);
    }

    #[tokio::test]
    async fn cancel_stops_run_and_drops_pending_without_routing() {
        // Fire the cancel token before `run` polls: the `biased` cancel branch wins
        // deterministically, so `run` returns promptly even though the `SpeakCmd`
        // sender is still alive (channel open), and the buffered command is dropped
        // rather than routed.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let (jsonl, writer_join) = crate::jsonl::spawn_quiet(&JsonlSink::File(path.clone()))
            .await
            .unwrap();

        let (mut tx, rx) = mpsc::channel::<SpeakCmd>(4);
        tx.try_send(pcm_cmd("ghost", 1, &[1, 2, 3]))
            .expect("test channel has room");

        let cancel = CancellationToken::new();
        cancel.cancel();
        let stats = Arc::new(RouterStats::default());

        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            Router::new(
                empty_registry(),
                Arc::clone(&stats),
                jsonl.clone(),
                cancel,
                None,
                Arc::new(TurnLedger::new()),
                None,
            )
            .run(rx),
        )
        .await
        .expect("run returns promptly after cancel despite an open channel");

        // The sender stays alive until here — only the cancel branch ended `run`.
        drop(tx);
        drop(jsonl);
        writer_join.await.unwrap();

        assert_eq!(stats.snapshot().no_pod, 0);
        let contents = std::fs::read_to_string(&path).unwrap();
        assert!(
            contents.is_empty(),
            "no line for the dropped pending command: {contents:?}"
        );
    }

    /// Feed `events` through the `PlaybackEvent`→JSONL adapter, returning
    /// `(lines, clock_step_clamps)`.
    async fn run_adapter(events: Vec<PlaybackEvent>) -> (Vec<Value>, u64) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let (jsonl, join) = crate::jsonl::spawn_quiet(&JsonlSink::File(path.clone()))
            .await
            .unwrap();
        let clamps = Arc::new(AtomicU64::new(0));
        let adapter = playback_event_adapter(jsonl.clone(), Arc::clone(&clamps), None);
        for e in events {
            adapter(e).await;
        }
        drop(adapter);
        drop(jsonl);
        join.await.unwrap();
        let lines = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        (lines, clamps.load(Ordering::Relaxed))
    }

    fn find<'a>(lines: &'a [Value], event: &str) -> &'a Value {
        lines
            .iter()
            .find(|v| v["event"] == event)
            .unwrap_or_else(|| panic!("a {event} line in {lines:?}"))
    }

    #[tokio::test]
    async fn hello_audible_finished_aborted_map_to_their_lines() {
        let pod = PodId("pod-x".into());
        let (lines, _) = run_adapter(vec![
            PlaybackEvent::HelloWritten { pod: pod.clone() },
            PlaybackEvent::HelloFailed {
                pod: pod.clone(),
                reason: AbortReason::WriteTimeout,
            },
            PlaybackEvent::Finished {
                pod: pod.clone(),
                in_reply_to: Some(UtteranceId(3)),
                frames: 3,
                samples: 960,
                eoa_written: true,
            },
            PlaybackEvent::Audible {
                pod: pod.clone(),
                job: Some(AudibleJob {
                    in_reply_to: Some(UtteranceId(3)),
                    interruptible: false,
                }),
            },
            PlaybackEvent::Aborted {
                pod,
                in_reply_to: Some(UtteranceId(4)),
                reason: AbortReason::WriteError,
            },
        ])
        .await;

        assert_eq!(find(&lines, "playback_hello")["pod"], "pod-x");

        let failed = find(&lines, "playback_hello_failed");
        assert_eq!(failed["pod"], "pod-x");
        assert_eq!(failed["reason"], "write_timeout");

        let finished = find(&lines, "playback_finished");
        assert_eq!(finished["utterance"], 3);
        assert_eq!(finished["frames"], 3);
        assert_eq!(finished["samples"], 960);
        assert_eq!(finished["eoa_written"], true);
        // Nominal audio duration: 3 frames × 20 ms/frame.
        assert_eq!(finished["nominal_audio_ms"], 60);

        let audible = find(&lines, "playback_audible");
        assert_eq!(audible["pod"], "pod-x");
        assert_eq!(audible["active"], true);
        assert_eq!(audible["utterance"], 3);
        assert_eq!(audible["interruptible"], false);

        let aborted = find(&lines, "playback_aborted");
        assert_eq!(aborted["utterance"], 4);
        assert_eq!(aborted["reason"], "write_error");
    }

    /// `banked_ms` is the only derived number on any of these lines, and it is what
    /// an operator is told to read as the audio the device still holds. A reversed
    /// subtraction or a units slip would leave a plausible constant on every line,
    /// which no ordering or presence check can see.
    #[tokio::test(start_paused = true)]
    async fn the_written_line_carries_the_lead_still_outstanding() {
        let pod = PodId("pod-x".into());
        let (lines, _) = run_adapter(vec![
            PlaybackEvent::Written {
                pod: pod.clone(),
                in_reply_to: Some(UtteranceId(3)),
                frames: 96,
                samples: 30_720,
                eoa_written: true,
                rewritten: true,
                plays_until: tokio::time::Instant::now() + std::time::Duration::from_millis(800),
            },
            // A second pass whose audible end is already behind it: the estimate is
            // a lower bound, so this is reachable, and it reads as nothing banked
            // rather than as a wrapped duration.
            PlaybackEvent::Written {
                pod,
                in_reply_to: None,
                frames: 1,
                samples: 320,
                eoa_written: false,
                rewritten: false,
                plays_until: tokio::time::Instant::now() - std::time::Duration::from_millis(50),
            },
        ])
        .await;

        let written: Vec<&Value> = lines
            .iter()
            .filter(|v| v["event"] == "playback_written")
            .collect();
        assert_eq!(written.len(), 2, "{lines:?}");
        assert_eq!(written[0]["pod"], "pod-x");
        assert_eq!(written[0]["utterance"], 3);
        assert_eq!(written[0]["frames"], 96);
        assert_eq!(written[0]["samples"], 30_720);
        assert_eq!(written[0]["eoa_written"], true);
        assert_eq!(written[0]["rewritten"], true);
        assert_eq!(written[0]["banked_ms"], 800);
        assert!(written[1]["utterance"].is_null());
        assert_eq!(written[1]["eoa_written"], false);
        assert_eq!(written[1]["rewritten"], false);
        assert_eq!(written[1]["banked_ms"], 0, "a past end banks nothing");
    }

    /// Silence has no job to name, so the two job-shaped fields are null rather
    /// than carrying the turn that just stopped.
    #[tokio::test]
    async fn a_silent_pod_emits_an_audible_line_with_no_job() {
        let (lines, _) = run_adapter(vec![PlaybackEvent::Audible {
            pod: PodId("pod-x".into()),
            job: None,
        }])
        .await;
        let audible = find(&lines, "playback_audible");
        assert_eq!(audible["active"], false);
        assert!(audible["utterance"].is_null());
        assert!(audible["interruptible"].is_null());
    }

    /// t0 for the timing fixtures: host receipt of the utterance's first audio.
    /// Every other stamp is expressed as a round ms offset from it, so an
    /// assertion reads as the offset the summary should report.
    const T0: u64 = 1_000_000;

    fn at_ms(ms: i64) -> Option<HostMicros> {
        Some(HostMicros((T0 as i64 + ms * 1_000) as u64))
    }

    /// A complete carved-world `StageTimings` for a synthesized (`Text`) response:
    /// a measured t0, every stage stamped, `vad_high_est` legitimately before t0
    /// (the device preroll) and the wake before the utterance start (arm slack).
    fn full_timings() -> StageTimings {
        StageTimings {
            first_audio_rx: at_ms(0),
            t0_projected: Some(false),
            vad_high_est: at_ms(-38),
            wake_detected_rx: at_ms(224),
            onset_rx: at_ms(300),
            soft_endpoint_rx: at_ms(1_381),
            stt_started: at_ms(1_382),
            transcribed: at_ms(1_731),
            brain_dispatched: at_ms(1_732),
            synth_started: at_ms(1_740),
            synth_completed: at_ms(2_094),
            ..StageTimings::default()
        }
    }

    /// The `Started` event for `timings`, with `speak_rx`/`first_write` on the
    /// same t0-relative axis.
    fn started_event(timings: StageTimings) -> PlaybackEvent {
        PlaybackEvent::Started {
            pod: PodId("pod-x".into()),
            in_reply_to: Some(UtteranceId(2)),
            timings: Box::new(timings),
            speak_rx: at_ms(1_740).unwrap(),
            first_write: at_ms(2_101).unwrap(),
            samples: 320,
            interruptible: true,
        }
    }

    #[tokio::test]
    async fn started_line_carries_pod_utterance_samples_and_hangover_floor() {
        let (lines, _) = run_adapter(vec![started_event(full_timings())]).await;

        let started = find(&lines, "playback_started");
        assert_eq!(started["pod"], "pod-x");
        assert_eq!(started["utterance"], 2);
        assert_eq!(started["samples"], 320);
        // The firmware floor, reported from its single source of truth.
        assert_eq!(started["vad_hangover_floor_ms"], 800);
        // The latency decomposition lives on `latency_summary`; this line carries
        // none of it.
        assert!(started["segment_end_to_first_write_us"].is_null());
    }

    #[tokio::test]
    async fn latency_summary_stacks_every_stage_against_t0() {
        let (lines, clamps) = run_adapter(vec![started_event(full_timings())]).await;

        let s = find(&lines, "latency_summary");
        assert_eq!(s["pod"], "pod-x");
        assert_eq!(s["utterance"], 2);
        assert_eq!(s["t0_projected"], false);

        // The offsets group: every stage on one axis anchored at t0.
        assert_eq!(s["vad_high_ms"], -38); // Before t0: the device preroll.
        assert_eq!(s["wake_ms"], 224);
        assert_eq!(s["onset_ms"], 300);
        assert_eq!(s["soft_endpoint_ms"], 1_381);
        assert_eq!(s["stt_start_ms"], 1_382);
        assert_eq!(s["stt_done_ms"], 1_731);
        assert_eq!(s["brain_ms"], 1_732);
        assert_eq!(s["speak_rx_ms"], 1_740);
        assert_eq!(s["tts_done_ms"], 2_094);
        assert_eq!(s["first_write_ms"], 2_101);

        // The blame group: consecutive-stage contributions.
        assert_eq!(s["endpoint_to_stt_us"], 1_000);
        assert_eq!(s["stt_us"], 349_000);
        assert_eq!(s["stt_to_brain_us"], 1_000);
        assert_eq!(s["brain_us"], 8_000);
        assert_eq!(s["speak_to_synth_start_us"], 0);
        assert_eq!(s["tts_us"], 354_000);
        assert_eq!(s["synth_to_first_write_us"], 7_000);
        // A synthesized body splits `speak_rx → first_write` around the synthesis,
        // so the unsplit span (which would double-count TTS) is absent.
        assert!(s["speak_to_first_write_us"].is_null());

        // Every stamp is forward, so no clock step was clamped — in particular the
        // two negative offsets went through the signed path, not `stage_delta_us`.
        assert_eq!(clamps, 0);
    }

    /// The blame group must partition `soft_endpoint_rx → first_write` with no
    /// overlap and no hole — for both body shapes. This is the property that makes
    /// the numbers an accounting rather than an assortment.
    #[tokio::test]
    async fn latency_summary_blame_deltas_sum_to_the_endpoint_to_first_write_span() {
        for (label, timings) in [
            ("text", full_timings()),
            (
                "pcm",
                StageTimings {
                    synth_started: None,
                    synth_completed: None,
                    ..full_timings()
                },
            ),
        ] {
            let (lines, _) = run_adapter(vec![started_event(timings)]).await;
            let s = find(&lines, "latency_summary");

            let blame: u64 = [
                "endpoint_to_stt_us",
                "stt_us",
                "stt_to_brain_us",
                "brain_us",
                "speak_to_synth_start_us",
                "tts_us",
                "synth_to_first_write_us",
                "speak_to_first_write_us",
            ]
            .iter()
            .filter_map(|f| s[f].as_u64())
            .sum();

            let span = (s["first_write_ms"].as_i64().unwrap()
                - s["soft_endpoint_ms"].as_i64().unwrap()) as u64
                * 1_000;
            assert_eq!(blame, span, "{label} blame deltas partition the span");
        }
    }

    #[tokio::test]
    async fn latency_summary_for_a_pcm_body_carries_the_unsplit_speak_span() {
        // A `Pcm` body never enters the synthesis await, so there is nothing to
        // split `speak_rx → first_write` around and no TTS to blame.
        let timings = StageTimings {
            synth_started: None,
            synth_completed: None,
            ..full_timings()
        };
        let (lines, _) = run_adapter(vec![started_event(timings)]).await;

        let s = find(&lines, "latency_summary");
        assert_eq!(s["speak_to_first_write_us"], 361_000); // 2101 − 1740 ms
        assert!(s["speak_to_synth_start_us"].is_null());
        assert!(s["tts_us"].is_null());
        assert!(s["synth_to_first_write_us"].is_null());
        assert!(s["tts_done_ms"].is_null());
    }

    #[tokio::test]
    async fn latency_summary_marks_a_projected_t0_and_nulls_absent_stages() {
        // A wake arriving into an already-open segment: t0 is projected off the
        // device clock, and the VAD went high well before the utterance's own
        // audio — the large negative offset is the reading, not a fault. No
        // transcriber and no brain wired, so those stages never stamped.
        let timings = StageTimings {
            t0_projected: Some(true),
            vad_high_est: at_ms(-4_000),
            onset_rx: None,
            transcribed: None,
            brain_dispatched: None,
            ..full_timings()
        };
        let (lines, clamps) = run_adapter(vec![started_event(timings)]).await;

        let s = find(&lines, "latency_summary");
        assert_eq!(s["t0_projected"], true);
        assert_eq!(s["vad_high_ms"], -4_000);
        // The missed-onset fallback carve never onset.
        assert!(s["onset_ms"].is_null());
        assert!(s["stt_done_ms"].is_null());
        assert!(s["brain_ms"].is_null());
        // Blame deltas touching an absent stamp are null on both sides of it.
        assert!(s["stt_us"].is_null());
        assert!(s["stt_to_brain_us"].is_null());
        assert!(s["brain_us"].is_null());
        // A negative offset is expected here, so it is never counted as a step.
        assert_eq!(clamps, 0);
    }

    #[tokio::test]
    async fn latency_summary_without_t0_nulls_every_offset_but_keeps_blame() {
        // No segment-open record covered the carve (a dropped `SegmentOpened`
        // marker), so there is no axis to reference — but the blame group is
        // anchored on the soft endpoint, not on t0, and still accounts.
        let timings = StageTimings {
            first_audio_rx: None,
            t0_projected: None,
            ..full_timings()
        };
        let (lines, _) = run_adapter(vec![started_event(timings)]).await;

        let s = find(&lines, "latency_summary");
        assert!(s["t0_projected"].is_null());
        for field in [
            "vad_high_ms",
            "wake_ms",
            "soft_endpoint_ms",
            "first_write_ms",
        ] {
            assert!(s[field].is_null(), "{field} has no axis to sit on");
        }
        assert_eq!(s["stt_us"], 349_000);
        assert_eq!(s["tts_us"], 354_000);
    }

    #[tokio::test]
    async fn latency_summary_clamps_and_counts_a_backward_clock_step_in_the_blame_group() {
        // A backward host-clock step between the soft endpoint and the STT spawn:
        // the blame delta clamps to 0 and counts, because a negative *duration*
        // there is a clock correction, not a reading.
        let timings = StageTimings {
            stt_started: at_ms(1_000), // Before the soft endpoint at +1381.
            ..full_timings()
        };
        let (lines, clamps) = run_adapter(vec![started_event(timings)]).await;

        let s = find(&lines, "latency_summary");
        assert_eq!(s["endpoint_to_stt_us"], 0);
        assert_eq!(clamps, 1);
        // The offset axis reports the stamp as it was, unclamped.
        assert_eq!(s["stt_start_ms"], 1_000);
    }

    #[tokio::test]
    async fn speak_rx_line_marks_brain_end_with_the_body_kind() {
        let (lines, _) = run_router(
            empty_registry(),
            vec![pcm_cmd("pod-x", 7, &[1, 2, 3]), text_cmd("pod-y", 8)],
        )
        .await;

        let pcm = lines
            .iter()
            .find(|v| v["event"] == "speak_rx" && v["pod"] == "pod-x")
            .expect("a speak_rx line for the pcm body");
        assert_eq!(pcm["utterance"], 7);
        assert_eq!(pcm["body"], "pcm");

        // Emitted at route entry, so an unsupported body is still marked received.
        let text = lines
            .iter()
            .find(|v| v["event"] == "speak_rx" && v["pod"] == "pod-y")
            .expect("a speak_rx line for the text body");
        assert_eq!(text["body"], "text");
    }

    /// Capture the `StageTimings` off the real `PlaybackEvent::Started` the writer
    /// emits for a routed job — not off an intermediate.
    async fn route_and_capture_timings(
        cmds: Vec<SpeakCmd>,
        synthesizer: Option<Arc<dyn Synthesizer>>,
    ) -> StageTimings {
        let (_peer, device) = tokio::io::duplex(64 * 1024);
        let seen: Arc<Mutex<Vec<StageTimings>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let capture: PlaybackEventFn = Arc::new(move |e| {
            if let PlaybackEvent::Started { timings, .. } = e {
                sink.lock().unwrap().push(*timings);
            }
            Box::pin(std::future::ready(()))
        });
        let handle = PlaybackWriter::spawn(
            device,
            PodId("pod-x".into()),
            PacerConfig::default(),
            Arc::new(PlaybackStats::default()),
            capture,
            CancellationToken::new(),
        );
        let registry = empty_registry();
        playback_register(&registry, "pod-x".into(), 1, handle);

        run_router_with_synth(registry, cmds, synthesizer).await;

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while seen.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the writer starts the job");

        seen.lock().unwrap().pop().unwrap()
    }

    #[tokio::test]
    async fn synthesis_stamps_bracket_the_await_on_the_routed_job() {
        // The stamps the summary blames TTS with are taken around the synthesis
        // await itself, so they must bracket it on the job that reaches the writer.
        // A synthesizer that resolves synchronously cannot show that: both stamps
        // taken side by side anywhere in `route` would bracket it just as well. So
        // the fixture spends a known interval *inside* the await, and `tts_us` has
        // to contain it — the assertion a hoisted stamp fails.
        const SYNTH_DELAY: std::time::Duration = std::time::Duration::from_millis(40);
        let synth: Arc<dyn Synthesizer> = Arc::new(FakeSynth::Slow(SYNTH_DELAY, vec![1, 2, 3]));
        let before = HostMicros::now();
        let t = route_and_capture_timings(vec![text_cmd("pod-x", 5)], Some(synth)).await;
        let after = HostMicros::now();

        let started = t.synth_started.expect("a text body stamps synth_started");
        let completed = t
            .synth_completed
            .expect("a text body stamps synth_completed");
        assert!(started >= before && completed >= started && completed <= after);
        // Half the sleep, so timer coarseness cannot flake it while a stamp pair
        // that skips the await (which would read ~0) still fails.
        let tts_us = completed
            .checked_delta(started)
            .expect("completed ≥ started");
        assert!(
            tts_us >= SYNTH_DELAY.as_micros() as u64 / 2,
            "tts_us must contain the synthesis await, got {tts_us}µs"
        );
    }

    #[tokio::test]
    async fn a_pcm_body_leaves_the_synth_stamps_unset() {
        let t = route_and_capture_timings(vec![pcm_cmd("pod-x", 5, &[1, 2, 3])], None).await;
        assert!(t.synth_started.is_none());
        assert!(t.synth_completed.is_none());
    }

    #[tokio::test]
    async fn a_queued_cmd_for_an_interrupted_turn_is_dropped_and_settled() {
        // The eviction the flush cannot do: commands already sitting in the channel
        // when the barge landed are dropped as they surface. A newer turn's command
        // rides straight past the mark.
        let ledger = Arc::new(TurnLedger::new());
        ledger.record_dispatch(&PodId("pod-x".into()), UtteranceId(1), None);
        ledger.record_cmd(&PodId("pod-x".into()), UtteranceId(1), None);
        ledger.interrupt(
            &PodId("pod-x".into()),
            UtteranceId(1),
            InterruptProgress {
                heard_ms: 100,
                total_ms: 900,
            },
        );

        let (lines, stats) = run_router_full(
            empty_registry(),
            vec![
                pcm_cmd("pod-x", 1, &[1, 2, 3]),
                pcm_cmd("pod-x", 2, &[4, 5, 6]),
            ],
            None,
            Arc::clone(&ledger),
        )
        .await;

        assert_eq!(stats.interrupted, 1);
        let dropped = lines
            .iter()
            .find(|v| v["event"] == "speak_interrupted")
            .expect("a speak_interrupted line");
        assert_eq!(dropped["utterance"], 1);
        assert_eq!(dropped["during"], "queue");
        // The newer turn reached the (absent) pod, so it was routed, not evicted.
        assert_eq!(stats.no_pod, 1);
        assert!(
            lines
                .iter()
                .any(|v| v["event"] == "playback_no_pod" && v["utterance"] == 2)
        );
    }

    #[tokio::test]
    async fn an_interrupt_during_synthesis_aborts_the_await() {
        // A synthesizer that never returns: only the interrupt can free the router,
        // so the test would hang if the notify branch were not wired.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let (jsonl, writer_join) = crate::jsonl::spawn_quiet(&JsonlSink::File(path.clone()))
            .await
            .unwrap();

        let (mut tx, rx) = mpsc::channel::<SpeakCmd>(4);
        tx.try_send(text_cmd("pod-x", 1)).unwrap();
        drop(tx);

        let ledger = Arc::new(TurnLedger::new());
        let stats = Arc::new(RouterStats::default());
        let synth: Arc<dyn Synthesizer> = Arc::new(FakeSynth::Hang);
        let join = tokio::spawn(
            Router::new(
                empty_registry(),
                Arc::clone(&stats),
                jsonl.clone(),
                CancellationToken::new(),
                Some(synth),
                Arc::clone(&ledger),
                None,
            )
            .run(rx),
        );

        // Let the router park inside the synthesis await, then barge.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        ledger.interrupt(
            &PodId("pod-x".into()),
            UtteranceId(1),
            InterruptProgress {
                heard_ms: 0,
                total_ms: 0,
            },
        );

        tokio::time::timeout(std::time::Duration::from_secs(5), join)
            .await
            .expect("the interrupt frees the synthesis await")
            .unwrap();

        drop(jsonl);
        writer_join.await.unwrap();
        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(stats.snapshot().interrupted, 1);
        assert_eq!(stats.snapshot().delivered, 0);
        assert!(
            contents.contains(r#""during":"synth""#),
            "the drop is attributed to the synth await: {contents}"
        );
    }

    #[tokio::test]
    async fn an_interrupt_landing_after_synthesis_still_drops_the_job() {
        // The post-synth check: synthesis completed just as the barge landed, so no
        // await was there to cancel and only the final re-check can catch it.
        let ledger = Arc::new(TurnLedger::new());
        let pod = PodId("pod-x".into());
        let marker = Arc::clone(&ledger);
        // A synthesizer that interrupts the turn as it produces the clip: the
        // interrupt is guaranteed to land after the await resolved.
        struct InterruptOnSynth {
            ledger: Arc<TurnLedger>,
            pod: PodId,
        }
        impl Synthesizer for InterruptOnSynth {
            fn synthesize(
                &self,
                _text: &str,
            ) -> futures::stream::BoxStream<'static, Result<PcmChunk, SynthesisError>> {
                self.ledger.interrupt(
                    &self.pod,
                    UtteranceId(1),
                    InterruptProgress {
                        heard_ms: 5,
                        total_ms: 500,
                    },
                );
                futures::stream::once(async {
                    Ok(PcmChunk {
                        pcm: Arc::from(&[1i16, 2, 3][..]),
                    })
                })
                .boxed()
            }
        }
        let synth: Arc<dyn Synthesizer> = Arc::new(InterruptOnSynth {
            ledger: marker,
            pod: pod.clone(),
        });

        let (lines, stats) = run_router_full(
            empty_registry(),
            vec![text_cmd("pod-x", 1)],
            Some(synth),
            Arc::clone(&ledger),
        )
        .await;

        assert_eq!(stats.interrupted, 1);
        assert_eq!(stats.delivered, 0);
        assert_eq!(stats.no_pod, 0, "the job never reached the registry");
        let dropped = lines
            .iter()
            .find(|v| v["event"] == "speak_interrupted")
            .expect("a speak_interrupted line");
        assert_eq!(dropped["during"], "post_synth");
    }

    /// One way a command can end up unplayable without a barge having cut it.
    /// Each is a separate return out of `route`, and each has to settle its
    /// command and report the turn: no playback event will ever name it.
    #[derive(Debug, Clone, Copy)]
    enum GiveUp {
        /// A `Text` body with no synthesizer wired.
        NoSynthesizer,
        /// The synthesizer answered with an error.
        SynthesisFailure,
        /// The target pod has no registered writer.
        NoPod,
        /// The pod's writer queue is full.
        QueueFull,
        /// The pod's writer has exited.
        DeadWriter,
    }

    /// Every shape, so the two tests below cannot drift apart or fall behind the
    /// enum.
    const GIVE_UP_SHAPES: [GiveUp; 5] = [
        GiveUp::NoSynthesizer,
        GiveUp::SynthesisFailure,
        GiveUp::NoPod,
        GiveUp::QueueFull,
        GiveUp::DeadWriter,
    ];

    /// Everything one give-up shape needs routed.
    ///
    /// The last command is the one that gives up; anything before it is setup the
    /// shape needs on the wire, and it names a turn the ledger never heard of so
    /// the accounting the shape reports is the give-up turn's alone.
    struct GiveUpWiring {
        registry: PlaybackRegistry,
        synthesizer: Option<Arc<dyn Synthesizer>>,
        cmds: Vec<SpeakCmd>,
        /// A duplex peer the shape needs kept open. Dropping it fails the
        /// writer's eager `Hello` write, which turns a parked writer into a dead
        /// one — a different shape.
        _peer: Option<tokio::io::DuplexStream>,
    }

    /// The registry, synthesizer and commands that produce one give-up shape.
    async fn give_up_wiring(shape: GiveUp) -> GiveUpWiring {
        let plain = |registry, synthesizer, cmds| GiveUpWiring {
            registry,
            synthesizer,
            cmds,
            _peer: None,
        };
        match shape {
            GiveUp::NoSynthesizer => plain(empty_registry(), None, vec![text_cmd("pod-x", 1)]),
            GiveUp::SynthesisFailure => plain(
                empty_registry(),
                Some(Arc::new(FakeSynth::Fail) as Arc<dyn Synthesizer>),
                vec![text_cmd("pod-x", 1)],
            ),
            GiveUp::NoPod => plain(
                empty_registry(),
                None,
                vec![pcm_cmd("pod-x", 1, &[1, 2, 3])],
            ),
            GiveUp::QueueFull => {
                // A writer whose peer never reads parks on the eager Hello write
                // (an 8-byte duplex the frame overflows), so it never drains its
                // job queue; with a depth of one, the command ahead fills it and
                // the turn's own command overflows.
                let (peer, device) = tokio::io::duplex(8);
                let cfg = PacerConfig {
                    lead_ms: 250,
                    write_timeout_ms: 60_000,
                    job_queue_depth: 1,
                };
                let noop: PlaybackEventFn = Arc::new(|_| Box::pin(std::future::ready(())));
                let handle = PlaybackWriter::spawn(
                    device,
                    PodId("pod-x".into()),
                    cfg,
                    Arc::new(PlaybackStats::default()),
                    noop,
                    CancellationToken::new(),
                );
                let registry = empty_registry();
                playback_register(&registry, "pod-x".into(), 1, handle);
                GiveUpWiring {
                    registry,
                    synthesizer: None,
                    cmds: vec![
                        pcm_cmd("pod-x", 99, &[1, 2, 3]),
                        pcm_cmd("pod-x", 1, &[4, 5, 6]),
                    ],
                    _peer: Some(peer),
                }
            }
            GiveUp::DeadWriter => {
                // The peer is dropped, so the writer's eager Hello write errors and
                // the task exits; wait for it before routing.
                let (peer, device) = tokio::io::duplex(64 * 1024);
                drop(peer);
                let noop: PlaybackEventFn = Arc::new(|_| Box::pin(std::future::ready(())));
                let handle = PlaybackWriter::spawn(
                    device,
                    PodId("pod-x".into()),
                    PacerConfig::default(),
                    Arc::new(PlaybackStats::default()),
                    noop,
                    CancellationToken::new(),
                );
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    loop {
                        if matches!(handle.try_play(dummy_job()), Err(PlayRejected::WriterDead)) {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("the writer becomes dead");
                let registry = empty_registry();
                playback_register(&registry, "pod-x".into(), 1, handle);
                plain(registry, None, vec![pcm_cmd("pod-x", 1, &[1, 2, 3])])
            }
        }
    }

    /// Route one command that cannot play, against a ledger already told the turn
    /// dispatched it and finished, and hand back everything the scripter tap saw.
    /// Production wiring: the same ledger the router settles into and the same
    /// scripter handle.
    async fn give_up_reports(shape: GiveUp) -> Vec<ScriptInput> {
        let dir = tempfile::tempdir().unwrap();
        let (jsonl, join) = crate::jsonl::spawn_quiet(&JsonlSink::File(dir.path().join("e")))
            .await
            .unwrap();
        let (handle, mut inbox) = crate::scripter::channel(jsonl.clone());
        let ledger = Arc::new(TurnLedger::new());
        let pod = PodId("pod-x".into());
        ledger.record_dispatch(&pod, UtteranceId(1), None);
        ledger.record_cmd(&pod, UtteranceId(1), None);
        ledger.dispatch_done(&pod, UtteranceId(1));

        let wiring = give_up_wiring(shape).await;
        run_router_scripted(
            wiring.registry,
            wiring.cmds,
            wiring.synthesizer,
            Arc::clone(&ledger),
            Some(handle.clone()),
        )
        .await;

        drop(handle);
        drop(jsonl);
        join.await.unwrap();
        let mut seen = Vec::new();
        while let Some(input) = inbox.recv().await {
            seen.push(input);
        }
        seen
    }

    /// Every way the router gives up on a command that is not a barge settles it
    /// and reports the turn. Without the report the turn keeps awaiting a start
    /// that will never come, and the head's ending is never scheduled.
    #[tokio::test]
    async fn every_give_up_path_resolves_its_turn() {
        for shape in GIVE_UP_SHAPES {
            let seen = give_up_reports(shape).await;
            let [
                ScriptInput::Audio {
                    pod: at,
                    turn,
                    audio,
                },
            ] = &seen[..]
            else {
                panic!("{shape:?} said {seen:?}");
            };
            assert_eq!(
                (at, *turn),
                (&PodId("pod-x".into()), UtteranceId(1)),
                "{shape:?}"
            );
            assert_eq!(audio.cmds_sent, 1, "{shape:?}");
            assert!(audio.dispatch_done, "{shape:?}");
            assert_eq!(
                audio.awaiting_start, 0,
                "the command resolved without starting: {shape:?}"
            );
            assert!(audio.horizon.is_none(), "nothing ever played: {shape:?}");
        }
    }

    /// The consequence, end to end: the same reports drive a real scripter, and
    /// the turn's closing script goes out. Without the report, the head would
    /// wait out the hold script's timeout instead.
    #[tokio::test]
    async fn a_turn_the_router_gave_up_on_still_gets_its_closing_script() {
        use crate::scripter::{Cause, Now, ScriptTiming, Scripter};
        use speech_pipeline::TurnEnd;
        use std::time::Duration;

        let timing = ScriptTiming {
            refresh: Duration::from_secs(5),
            linger: Duration::from_secs(8),
            max_engaged: Duration::from_secs(30),
            stow_margin: Duration::from_millis(500),
        };
        for shape in GIVE_UP_SHAPES {
            let mut scripter = Scripter::new(timing);
            let pod = PodId("pod-x".into());
            scripter.apply(
                ScriptInput::TurnStarted {
                    pod: pod.clone(),
                    turn: UtteranceId(1),
                },
                Now::read(),
            );
            scripter.apply(
                ScriptInput::TurnEnded {
                    pod: pod.clone(),
                    turn: UtteranceId(1),
                    end: TurnEnd::Closed,
                },
                Now::read(),
            );
            let mut closing = None;
            for input in give_up_reports(shape).await {
                closing = scripter.apply(input, Now::read()).or(closing);
            }
            let publish = closing.unwrap_or_else(|| panic!("{shape:?} scheduled no ending"));
            assert_eq!(publish.cause, Cause::Closing, "{shape:?}");
            let steps = publish.script.steps();
            assert_eq!(steps.len(), 2, "{shape:?}: up now, down at the margin");
            assert_eq!(
                steps[1].action.base().and_then(motion_proto::Base::posture),
                Some(motion_proto::Posture::Stow),
                "{shape:?}"
            );
            assert_eq!(
                steps[1].after_ms, 500,
                "{shape:?}: nothing played, so the margin runs from now"
            );
        }
    }

    /// A feed sink recording the `PlaybackState` changes the adapter drives, in
    /// place of the real listener (which owns an inference thread).
    fn spy_feed() -> (FeedFn, tokio::sync::mpsc::UnboundedReceiver<Feed>) {
        // A real bounded feed channel, so the permit path under test is the one
        // production takes; a forwarder republishes onto an unbounded receiver so
        // assertions never have to keep up with the adapter.
        let (tx, mut raw_rx) = tokio::sync::mpsc::channel::<(PodId, Feed)>(8);
        let (fwd, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some((_pod, f)) = raw_rx.recv().await {
                let _ = fwd.send(f);
            }
        });
        let sender = speech_pipeline::FeedSender::detached_for_tests(tx);
        let feed_sender = sender.clone();
        let feed: FeedFn = Arc::new(move |pod, f| {
            let sender = feed_sender.clone();
            Box::pin(async move { sender.feed(pod, f).await })
        });
        (feed, rx)
    }

    /// Feed `events` through an adapter wired to a spy listener and a ledger,
    /// returning the floor changes it fed and the ledger it settled against.
    async fn run_fanout(
        events: Vec<PlaybackEvent>,
    ) -> (tokio::sync::mpsc::UnboundedReceiver<Feed>, Arc<TurnLedger>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let (jsonl, join) = crate::jsonl::spawn_quiet(&JsonlSink::File(path))
            .await
            .unwrap();
        let (feed, rx) = spy_feed();
        let ledger = Arc::new(TurnLedger::new());
        let adapter = playback_event_adapter(
            jsonl.clone(),
            Arc::new(AtomicU64::new(0)),
            Some(PlaybackFanout {
                feed,
                ledger: Arc::clone(&ledger),
                scripter: None,
            }),
        );
        for e in events {
            adapter(e).await;
        }
        drop(adapter);
        drop(jsonl);
        join.await.unwrap();
        (rx, ledger)
    }

    /// Feed `events` through an adapter whose fanout has a scripter tap, and
    /// hand back everything that tap sent. The floor and the ledger are wired
    /// as ever, so what reaches the scripter is what reaches it in production.
    ///
    /// `cmds` is how many commands the turn's sink tap saw before any of this,
    /// because the ledger answers nothing about a turn it holds no records for
    /// — which is a case of its own, driven by passing zero.
    async fn script_inputs_from_fanout(
        events: Vec<PlaybackEvent>,
        cmds: usize,
    ) -> Vec<ScriptInput> {
        let dir = tempfile::tempdir().unwrap();
        let (jsonl, join) = crate::jsonl::spawn_quiet(&JsonlSink::File(dir.path().join("e")))
            .await
            .unwrap();
        let (handle, mut inbox) = crate::scripter::channel(jsonl.clone());
        let (feed, _rx) = spy_feed();
        let ledger = Arc::new(TurnLedger::new());
        for _ in 0..cmds {
            ledger.record_cmd(&PodId("pod-x".into()), UtteranceId(1), None);
        }
        let adapter = playback_event_adapter(
            jsonl.clone(),
            Arc::new(AtomicU64::new(0)),
            Some(PlaybackFanout {
                feed,
                ledger,
                scripter: Some(handle.clone()),
            }),
        );
        for e in events {
            adapter(e).await;
        }
        drop(adapter);
        drop(handle);
        drop(jsonl);
        join.await.unwrap();

        let mut seen = Vec::new();
        while let Some(input) = inbox.recv().await {
            seen.push(input);
        }
        seen
    }

    /// The fixture's `Started`, re-addressed to the turn the scripter tests
    /// account for.
    fn started_for(turn: u64) -> PlaybackEvent {
        let PlaybackEvent::Started {
            pod,
            timings,
            speak_rx,
            first_write,
            samples,
            interruptible,
            ..
        } = started_event(full_timings())
        else {
            unreachable!("started_event builds a Started")
        };
        PlaybackEvent::Started {
            pod,
            in_reply_to: Some(UtteranceId(turn)),
            timings,
            speak_rx,
            first_write,
            samples,
            interruptible,
        }
    }

    /// The head's ending is scheduled from this tap and nothing else, so every
    /// arm is load-bearing: an arm that says nothing leaves a cmd awaiting a
    /// start that has already happened, and the closing script never goes out.
    /// Each case drives one cmd of a two-cmd turn, so what comes back is the
    /// accounting with that one event in and the other cmd still to come.
    #[tokio::test]
    async fn every_playback_arm_reports_the_turns_accounting() {
        let pod = PodId("pod-x".into());
        let cases = [
            started_for(1),
            finished(1, true),
            PlaybackEvent::Aborted {
                pod: pod.clone(),
                in_reply_to: Some(UtteranceId(1)),
                reason: AbortReason::WriteError,
            },
            PlaybackEvent::Flushed {
                pod: pod.clone(),
                in_reply_to: Some(UtteranceId(1)),
                was_playing: true,
                frames_written: 4,
                progress: InterruptProgress {
                    heard_ms: 80,
                    total_ms: 900,
                },
            },
            PlaybackEvent::Flushed {
                pod: pod.clone(),
                in_reply_to: Some(UtteranceId(1)),
                was_playing: false,
                frames_written: 0,
                progress: InterruptProgress {
                    heard_ms: 0,
                    total_ms: 0,
                },
            },
        ];
        for event in cases {
            let label = format!("{event:?}");
            let seen = script_inputs_from_fanout(vec![event], 2).await;
            let [
                ScriptInput::Audio {
                    pod: at,
                    turn,
                    audio,
                },
            ] = &seen[..]
            else {
                panic!("{label} said {seen:?}");
            };
            assert_eq!((at, *turn), (&pod, UtteranceId(1)), "{label}");
            assert_eq!(audio.cmds_sent, 2, "{label}");
            assert_eq!(audio.awaiting_start, 1, "one cmd is still to come: {label}");
        }
    }

    /// A `Started` is the one arm that dates the turn's audio, which is what the
    /// stow is scheduled from. The rest resolve the awaited set and leave the
    /// horizon where it stands.
    #[tokio::test]
    async fn only_a_started_dates_the_turns_audio() {
        let started = script_inputs_from_fanout(vec![started_for(1)], 1).await;
        let [ScriptInput::Audio { audio, .. }] = &started[..] else {
            panic!("{started:?}");
        };
        assert!(audio.horizon.is_some(), "the clip's own end");
        assert_eq!(audio.awaiting_start, 0, "the turn's only cmd started");

        let settled = script_inputs_from_fanout(vec![finished(1, true)], 1).await;
        let [ScriptInput::Audio { audio, .. }] = &settled[..] else {
            panic!("{settled:?}");
        };
        assert!(audio.horizon.is_none(), "nothing ever played");
        assert_eq!(audio.awaiting_start, 0, "the cmd resolved without starting");
    }

    /// A turn the ledger no longer holds — interrupted, or completed and retired
    /// — answers nothing, and there is nothing to say about it either: the barge
    /// that cut it has already raised the head on its own account.
    #[tokio::test]
    async fn a_retired_turn_says_nothing_to_the_head() {
        let seen = script_inputs_from_fanout(vec![finished(1, true)], 0).await;
        assert!(seen.is_empty(), "{seen:?}");
    }

    fn finished(utterance: u64, eoa_written: bool) -> PlaybackEvent {
        PlaybackEvent::Finished {
            pod: PodId("pod-x".into()),
            in_reply_to: Some(UtteranceId(utterance)),
            frames: 3,
            samples: 960,
            eoa_written,
        }
    }

    /// The pacer's report that `utterance`'s clip is what the pod is heard saying.
    fn audible(utterance: u64, interruptible: bool) -> PlaybackEvent {
        PlaybackEvent::Audible {
            pod: PodId("pod-x".into()),
            job: Some(AudibleJob {
                in_reply_to: Some(UtteranceId(utterance)),
                interruptible,
            }),
        }
    }

    /// The pacer's report that the pod's bank is empty.
    fn silent() -> PlaybackEvent {
        PlaybackEvent::Audible {
            pod: PodId("pod-x".into()),
            job: None,
        }
    }

    /// Assert the floor does not move for `ms`. A closed channel counts as quiet:
    /// the adapter is gone, so nothing can move it. `what` names the event under
    /// test, so a case that regresses says which one it was.
    async fn floor_quiet(rx: &mut tokio::sync::mpsc::UnboundedReceiver<Feed>, ms: u64, what: &str) {
        match tokio::time::timeout(std::time::Duration::from_millis(ms), rx.recv()).await {
            Err(_) | Ok(None) => {}
            Ok(Some(feed)) => panic!("{what} moved the floor: {feed:?}"),
        }
    }

    /// The next `PlaybackState` the listener was fed, or a failure if the floor did
    /// not move within the timeout.
    async fn next_floor(rx: &mut tokio::sync::mpsc::UnboundedReceiver<Feed>) -> (bool, bool) {
        let feed = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("the floor moves")
            .expect("the listener handle is alive");
        match feed {
            Feed::PlaybackState {
                active,
                interruptible,
            } => (active, interruptible),
            other => panic!("expected a PlaybackState feed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_audible_job_opens_the_floor_with_its_interruptibility() {
        // The floor is what gates detection; a non-interruptible job (an alert)
        // opens it closed to barge-in, which is the flag's whole purpose. A pod with
        // an empty bank closes it, with nothing to be interruptible about.
        for interruptible in [true, false] {
            let (mut rx, _) = run_fanout(vec![audible(1, interruptible)]).await;
            assert_eq!(next_floor(&mut rx).await, (true, interruptible));
        }
        let (mut rx, _) = run_fanout(vec![silent()]).await;
        assert_eq!(next_floor(&mut rx).await, (false, false));
    }

    #[tokio::test]
    async fn the_floor_follows_audible_and_no_other_event_moves_it() {
        // A per-pod state cannot be driven off per-job events: a reply's second clip
        // starts before the first one's audible end, a job re-written after a flush
        // repeats no lifecycle event, and a cut that never reached the device is not
        // a silence the host can vouch for. Every one of these carries its JSONL
        // line and its settlement and says nothing to the listener.
        let others = [
            started_event(full_timings()),
            PlaybackEvent::Written {
                pod: PodId("pod-x".into()),
                in_reply_to: Some(UtteranceId(1)),
                frames: 3,
                samples: 960,
                eoa_written: true,
                rewritten: false,
                plays_until: tokio::time::Instant::now(),
            },
            finished(1, true),
            finished(1, false),
            PlaybackEvent::Aborted {
                pod: PodId("pod-x".into()),
                in_reply_to: Some(UtteranceId(1)),
                reason: AbortReason::WriteError,
            },
            PlaybackEvent::Flushed {
                pod: PodId("pod-x".into()),
                in_reply_to: Some(UtteranceId(1)),
                was_playing: true,
                frames_written: 10,
                progress: InterruptProgress {
                    heard_ms: 200,
                    total_ms: 1_000,
                },
            },
            PlaybackEvent::Flushed {
                pod: PodId("pod-x".into()),
                in_reply_to: Some(UtteranceId(1)),
                was_playing: false,
                frames_written: 0,
                progress: InterruptProgress {
                    heard_ms: 0,
                    total_ms: 0,
                },
            },
        ];
        for event in others {
            let label = format!("{event:?}");
            let (mut rx, _) = run_fanout(vec![event]).await;
            floor_quiet(&mut rx, 150, &label).await;
        }
    }

    /// The floor's opens and closes reach the listener in the order they
    /// happened, over the same bounded channel production uses. A close that
    /// overtook the open behind it would blind detection for a whole response.
    #[tokio::test]
    async fn floor_moves_stay_ordered_over_the_real_feed_channel() {
        // A barge and the reply that answers it: closed, then open for the new
        // turn. The events also carry a hand-over between two clips of one reply,
        // which the pacer reports as no change at all.
        let (mut rx, _) =
            run_fanout(vec![audible(1, true), silent(), audible(2, true), silent()]).await;
        let mut seen = Vec::new();
        for _ in 0..4 {
            seen.push(next_floor(&mut rx).await.0);
        }
        assert_eq!(seen, [true, false, true, false], "floor moves in order");
    }

    #[tokio::test]
    async fn a_played_out_job_settles_clean_even_when_it_did_not_drain() {
        // Settlement is what decides a turn completed and the chain can drop, so
        // each terminal shape has to carry the right verdict. A job that played out
        // is clean whether it drained the stream (`eoa_written: true`) or finished
        // with another job queued behind it (`eoa_written: false`, no end-of-audio
        // yet all its audio delivered). An abort or a flush settles unclean — a job
        // whose end-of-audio write failed is one of the aborts, because the host
        // lost the stream and cannot say the tail was heard.
        let cases = [
            (finished(1, true), true),
            (finished(1, false), true),
            (
                PlaybackEvent::Aborted {
                    pod: PodId("pod-x".into()),
                    in_reply_to: Some(UtteranceId(1)),
                    reason: AbortReason::WriteError,
                },
                false,
            ),
            (
                PlaybackEvent::Flushed {
                    pod: PodId("pod-x".into()),
                    in_reply_to: Some(UtteranceId(1)),
                    was_playing: true,
                    frames_written: 4,
                    progress: InterruptProgress {
                        heard_ms: 80,
                        total_ms: 900,
                    },
                },
                false,
            ),
        ];
        for (event, clean) in cases {
            let label = format!("{event:?}");
            let pod = PodId("pod-x".into());
            // A pod with an older barge on the chain and one turn fully dispatched
            // but for its clip: the chain drops only if this event settles clean.
            let ledger = Arc::new(TurnLedger::new());
            ledger.interrupt(
                &pod,
                UtteranceId(99),
                InterruptProgress {
                    heard_ms: 1,
                    total_ms: 2,
                },
            );
            ledger.record_dispatch(&pod, UtteranceId(1), None);
            ledger.record_cmd(&pod, UtteranceId(1), None);
            ledger.dispatch_done(&pod, UtteranceId(1));

            let dir = tempfile::tempdir().unwrap();
            let (jsonl, join) = crate::jsonl::spawn_quiet(&JsonlSink::File(dir.path().join("e")))
                .await
                .unwrap();
            let (feed, _rx2) = spy_feed();
            let adapter = playback_event_adapter(
                jsonl.clone(),
                Arc::new(AtomicU64::new(0)),
                Some(PlaybackFanout {
                    feed,
                    ledger: Arc::clone(&ledger),
                    scripter: None,
                }),
            );
            adapter(event).await;
            drop(adapter);
            drop(jsonl);
            join.await.unwrap();

            assert_eq!(
                ledger.chain(&pod).is_none(),
                clean,
                "{label} settles clean={clean}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_started_job_dates_the_turns_audio_from_its_sample_count() {
        // The turn's audio is dated when it *starts*, from how much of it there is
        // — the whole reason `Started` carries the count.
        let pod = PodId("pod-x".into());
        let ledger = Arc::new(TurnLedger::new());
        ledger.record_dispatch(&pod, UtteranceId(2), None);
        ledger.record_cmd(&pod, UtteranceId(2), None);
        let t0 = tokio::time::Instant::now();

        let dir = tempfile::tempdir().unwrap();
        let (jsonl, join) = crate::jsonl::spawn_quiet(&JsonlSink::File(dir.path().join("e")))
            .await
            .unwrap();
        let (feed, _rx) = spy_feed();
        let adapter = playback_event_adapter(
            jsonl.clone(),
            Arc::new(AtomicU64::new(0)),
            Some(PlaybackFanout {
                feed,
                ledger: Arc::clone(&ledger),
                scripter: None,
            }),
        );
        adapter(PlaybackEvent::Started {
            pod: pod.clone(),
            in_reply_to: Some(UtteranceId(2)),
            timings: Box::new(full_timings()),
            speak_rx: at_ms(1_740).unwrap(),
            first_write: at_ms(2_101).unwrap(),
            // Three seconds at the spine rate.
            samples: 48_000,
            interruptible: true,
        })
        .await;
        drop(adapter);
        drop(jsonl);
        join.await.unwrap();

        let audio = ledger.dispatch_done(&pod, UtteranceId(2));
        assert_eq!(audio.awaiting_start, 0, "the turn's one clip is playing");
        assert_eq!(
            audio.horizon,
            Some(t0 + std::time::Duration::from_secs(3)),
            "the clip's own end, not the event's arrival"
        );
    }
}
