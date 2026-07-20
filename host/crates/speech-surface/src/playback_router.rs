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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use audio_pipeline::vad::VAD_HANGOVER_MS;
use futures::channel::mpsc;
use futures::future::BoxFuture;
use futures::StreamExt;
use pod_ingest::HostMicros;
use serde::Serialize;
use serde_json::json;
use speech_pipeline::{
    signed_offset_us, stage_delta_us, Feed, FeedPermit, PlayRejected, PlaybackEvent,
    PlaybackEventFn, PlaybackJob, PodId, SpeakBody, SpeakCmd, StageTimings, SynthesisError,
    Synthesizer, UtteranceId, FRAME_MS,
};
use tokio_util::sync::CancellationToken;

use crate::barge::TurnLedger;
use crate::jsonl::JsonlHandle;
use crate::server::{playback_try_play, PlaybackRegistry};

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
}

impl Router {
    pub(crate) fn new(
        registry: PlaybackRegistry,
        stats: Arc<RouterStats>,
        jsonl: JsonlHandle,
        cancel: CancellationToken,
        synthesizer: Option<Arc<dyn Synthesizer>>,
        ledger: Arc<TurnLedger>,
    ) -> Self {
        Self {
            registry,
            stats,
            jsonl,
            cancel,
            synthesizer,
            ledger,
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
        emit_outcome(
            &cmd.target,
            cmd.in_reply_to,
            outcome,
            &self.stats,
            &self.jsonl,
        );
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

    /// Drop one command for a barged-in turn: the line, the counter, and the
    /// settle that keeps the turn's accounting converging even though this command
    /// will never play. `during` names which of the three checks caught it.
    fn drop_interrupted(&self, pod: &PodId, turn: Option<UtteranceId>, during: &str) {
        self.stats.record_interrupted();
        self.jsonl.emit(
            "speak_interrupted",
            &json!({ "pod": pod, "utterance": turn, "during": during }),
        );
        self.ledger.settle_job(pod, turn, false);
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
            ))
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

/// Emit the JSONL line and bump the counter for one resolution result. `Delivered`
/// is silent on the wire — the writer emits the playback-lifecycle lines from
/// there; the `QueueFull`/`WriterDead` counters are already bumped inside
/// `try_play`, so here only their lines are the router's half.
fn emit_outcome(
    target: &PodId,
    in_reply_to: Option<UtteranceId>,
    outcome: Option<Result<(), PlayRejected>>,
    stats: &RouterStats,
    jsonl: &JsonlHandle,
) {
    match outcome {
        Some(Ok(())) => stats.record_delivered(),
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
}

/// Hands one `Feed` to the listener. A closure rather than the `ListenerHandle`
/// itself: the handle owns a live inference thread, so taking the narrow thing the
/// fan-out actually needs is what makes the floor's behaviour testable without one.
pub(crate) type FeedFn = Arc<dyn Fn(PodId, Feed) -> BoxFuture<'static, ()> + Send + Sync>;

/// Reserves one listener feed slot, for the floor-close timer: it must decide and
/// feed atomically under the generation lock, and a permit's send is the only
/// non-awaiting way to reach the channel. `None` means the listener is gone or
/// wedged and the close is abandoned.
pub(crate) type ReserveFn = Arc<dyn Fn() -> BoxFuture<'static, Option<FeedPermit>> + Send + Sync>;

/// What the playback-event adapter fans each event out to, beyond its JSONL line:
/// the listener's playback floor and the barge-in ledger's settlement accounting.
/// Absent in pipelines with no listener and no barge-in path (the replay rigs).
pub(crate) struct PlaybackFanout {
    pub(crate) feed: FeedFn,
    pub(crate) reserve: ReserveFn,
    pub(crate) ledger: Arc<TurnLedger>,
    /// The pacer's lead, which is how long the floor stays open past the last
    /// write. See [`schedule_floor_close`].
    pub(crate) lead_ms: u64,
}

/// The per-pod floor-close generation, latest-wins: every event that moves a pod's
/// floor bumps its generation, and a scheduled close captures the generation live
/// at scheduling and feeds the close only if it is still current when the timer
/// fires. `JoinHandle::abort` alone cannot retract a timer whose final poll is
/// already running, so a bare abort-on-supersede leaves a window where a stale
/// close lands after the next job's `Started` opened the floor and blinds barge
/// detection for that whole response. The generation re-check closes that window.
type FloorGens = Arc<Mutex<HashMap<PodId, u64>>>;

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
    let gens: FloorGens = Arc::new(Mutex::new(HashMap::new()));
    // `Arc`'d so each emitted event's future owns a cheap handle rather than
    // borrowing the closure's capture.
    let fanout = Arc::new(fanout);
    Arc::new(move |event| {
        let fanout = Arc::clone(&fanout);
        let gens = Arc::clone(&gens);
        let jsonl = jsonl.clone();
        let clock_step_clamps = Arc::clone(&clock_step_clamps);
        Box::pin(async move {
            if let Some(fanout) = fanout.as_ref() {
                fan_out_playback_event(&event, fanout, &gens).await;
            }
            emit_playback_event(event, &jsonl, &clock_step_clamps);
        })
    })
}

/// Drive the listener floor and the ledger from one playback event.
///
/// The floor tells detection whether the pod is speaking, which is what gates the
/// barge trigger. It opens at `Started` and closes on every way a job can end. The
/// ledger settles every terminal event, so a turn's cmds account for themselves
/// whatever became of them; `clean` is true only for a job that played out and
/// wrote its end-of-audio marker.
async fn fan_out_playback_event(event: &PlaybackEvent, fanout: &PlaybackFanout, gens: &FloorGens) {
    match event {
        PlaybackEvent::Started {
            pod, interruptible, ..
        } => {
            cancel_floor_close(gens, pod);
            (fanout.feed)(
                pod.clone(),
                Feed::PlaybackState {
                    active: true,
                    interruptible: *interruptible,
                },
            )
            .await;
        }
        PlaybackEvent::Finished {
            pod,
            in_reply_to,
            writer_dying,
            ..
        } => {
            // A fully-played job settles clean whether or not it drained the
            // stream: a clip that finishes with another queued behind it writes no
            // end-of-audio yet delivered all its audio. Only a writer dying on a
            // failed end-of-audio write settles unclean. The floor closes on every
            // `Finished` shape regardless — a live writer's next `Started`
            // supersedes the scheduled close, and a dying writer needs it (possibly
            // no `Aborted` follows, its queue may be empty), or the floor stays open
            // until the next reconnect and sustained room speech mints a wake-less
            // dispatch.
            fanout.ledger.settle_job(pod, *in_reply_to, !*writer_dying);
            schedule_floor_close(fanout, gens, pod);
        }
        PlaybackEvent::Aborted {
            pod, in_reply_to, ..
        } => {
            fanout.ledger.settle_job(pod, *in_reply_to, false);
            schedule_floor_close(fanout, gens, pod);
        }
        PlaybackEvent::Flushed {
            pod,
            in_reply_to,
            was_playing,
            ..
        } => {
            fanout.ledger.settle_job(pod, *in_reply_to, false);
            if *was_playing {
                // The barge already happened and the device has discarded its bank;
                // nothing is audible, so the floor closes now rather than on the
                // lead delay a natural ending needs.
                cancel_floor_close(gens, pod);
                close_floor(fanout, pod).await;
            }
        }
        PlaybackEvent::HelloWritten { .. } | PlaybackEvent::HelloFailed { .. } => {}
    }
}

/// Close the pod's floor `lead_ms` after the job ended, latest-wins.
///
/// `Finished` fires at the last *write*, and the pacer runs up to `lead_ms` ahead
/// of real time — so up to a second of audio is still coming out of the speaker.
/// Closing the floor at the event would blind detection for the response's final
/// second, which is exactly when a user who has heard enough speaks up.
fn schedule_floor_close(fanout: &PlaybackFanout, gens: &FloorGens, pod: &PodId) {
    let generation = bump_generation(gens, pod);
    let delay = std::time::Duration::from_millis(fanout.lead_ms);
    let reserve = Arc::clone(&fanout.reserve);
    let gens = Arc::clone(gens);
    let target = pod.clone();
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        // Reserve the channel slot *before* taking the lock: the send under the
        // lock must not await, and a permit's send cannot.
        let Some(permit) = reserve().await else {
            return;
        };
        // Re-check under the same lock every floor move takes to bump the
        // generation: only feed the close if it is still the pod's current
        // generation, and feed it while holding the lock so a concurrent `Started`
        // (which bumps the generation before it opens the floor) cannot interleave
        // its open between this check and this feed. A superseded close feeds
        // nothing.
        let map = gens.lock().expect("floor generations poisoned");
        if map.get(&target) == Some(&generation) {
            permit.send(
                target.clone(),
                Feed::PlaybackState {
                    active: false,
                    interruptible: false,
                },
            );
        }
        // A superseded close drops the permit here, releasing the slot.
    });
}

/// Bump `pod`'s floor generation, invalidating any pending close, and return the
/// new value for a freshly scheduled close to capture.
fn bump_generation(gens: &FloorGens, pod: &PodId) -> u64 {
    let mut map = gens.lock().expect("floor generations poisoned");
    let g = map.entry(pod.clone()).or_insert(0);
    *g = g.wrapping_add(1);
    *g
}

/// Drop any pending floor close for `pod` without closing the floor: bumping the
/// generation makes an in-flight close timer for the pod feed nothing when it wakes.
fn cancel_floor_close(gens: &FloorGens, pod: &PodId) {
    bump_generation(gens, pod);
}

async fn close_floor(fanout: &PlaybackFanout, pod: &PodId) {
    (fanout.feed)(
        pod.clone(),
        Feed::PlaybackState {
            active: false,
            interruptible: false,
        },
    )
    .await;
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
        PlaybackEvent::Finished {
            pod,
            in_reply_to,
            frames,
            samples,
            eoa_written,
            writer_dying: _,
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
                    // span. Not the measured wall time the writer spent — the pacer
                    // front-loads up to `lead_ms` of audio in one burst before any
                    // sleep, so the real first-write-to-EndOfAudio wall span is
                    // shorter by up to `lead_ms`. The event carries no wall
                    // timestamps, so only the nominal duration is available here.
                    "nominal_audio_ms": frames * FRAME_MS,
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
        AbortReason, InterruptProgress, PacerConfig, PcmChunk, PlaybackEventFn, PlaybackStats,
        PlaybackWriter, StageTimings,
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
    async fn hello_finished_aborted_map_to_their_lines() {
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
                writer_dying: false,
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

        let aborted = find(&lines, "playback_aborted");
        assert_eq!(aborted["utterance"], 4);
        assert_eq!(aborted["reason"], "write_error");
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
        let t = seen.lock().unwrap().pop().unwrap();
        t
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
        assert!(lines
            .iter()
            .any(|v| v["event"] == "playback_no_pod" && v["utterance"] == 2));
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

    /// A fanout whose feed and reserve hooks are supplied by the caller, for the
    /// floor-close timer paths that never reach the adapter.
    fn fanout_with(feed: FeedFn, reserve: ReserveFn, lead_ms: u64) -> PlaybackFanout {
        PlaybackFanout {
            feed,
            reserve,
            ledger: Arc::new(TurnLedger::new()),
            lead_ms,
        }
    }

    /// A wedged or dead listener hands back no permit; the timer abandons the
    /// close rather than panicking or feeding a stale `active: false`.
    #[tokio::test]
    async fn floor_close_timer_abandons_the_close_without_a_permit() {
        let (feed, _reserve, mut rx) = spy_feed();
        let reserve: ReserveFn = Arc::new(|| Box::pin(async { None }));
        let fanout = fanout_with(feed, reserve, 1);
        let gens: FloorGens = Arc::new(Mutex::new(HashMap::new()));
        let pod = PodId("pod-x".into());

        schedule_floor_close(&fanout, &gens, &pod);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert!(
            rx.try_recv().is_err(),
            "no floor move is fed when the reserve fails"
        );
    }

    /// Every superseded close gives its reserved slot back: without that, a long
    /// session leaks one channel slot per supersede until markers start timing out.
    #[tokio::test]
    async fn superseded_floor_closes_release_their_permits() {
        // A bounded channel with no consumer: only permit *release* can keep
        // capacity available across repeated superseded closes.
        let (tx, _raw_rx) = tokio::sync::mpsc::channel::<(PodId, Feed)>(2);
        let sender = speech_pipeline::FeedSender::detached_for_tests(tx);
        let feed_sender = sender.clone();
        let feed: FeedFn = Arc::new(move |pod, f| {
            let sender = feed_sender.clone();
            Box::pin(async move { sender.feed(pod, f).await })
        });
        let reserve_sender = sender.clone();
        let reserve: ReserveFn = Arc::new(move || {
            let sender = reserve_sender.clone();
            Box::pin(async move { sender.reserve_marker().await })
        });
        let fanout = fanout_with(feed, reserve, 1);
        let gens: FloorGens = Arc::new(Mutex::new(HashMap::new()));
        let pod = PodId("pod-x".into());

        for _ in 0..8 {
            schedule_floor_close(&fanout, &gens, &pod);
            // Supersede it before its permit is sent.
            bump_generation(&gens, &pod);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let permit = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            sender.reserve_marker(),
        )
        .await
        .expect("capacity is still available, so reserving is immediate");
        assert!(permit.is_some(), "the channel is neither full nor closed");
    }

    /// A feed sink recording the `PlaybackState` changes the adapter drives, in
    /// place of the real listener (which owns an inference thread).
    fn spy_feed() -> (
        FeedFn,
        ReserveFn,
        tokio::sync::mpsc::UnboundedReceiver<Feed>,
    ) {
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
        let reserve: ReserveFn = Arc::new(move || {
            let sender = sender.clone();
            Box::pin(async move { sender.reserve_marker().await })
        });
        (feed, reserve, rx)
    }

    /// Feed `events` through an adapter wired to a spy listener and a ledger,
    /// returning the floor changes it fed and the ledger it settled against.
    async fn run_fanout(
        events: Vec<PlaybackEvent>,
        lead_ms: u64,
    ) -> (tokio::sync::mpsc::UnboundedReceiver<Feed>, Arc<TurnLedger>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let (jsonl, join) = crate::jsonl::spawn_quiet(&JsonlSink::File(path))
            .await
            .unwrap();
        let (feed, reserve, rx) = spy_feed();
        let ledger = Arc::new(TurnLedger::new());
        let adapter = playback_event_adapter(
            jsonl.clone(),
            Arc::new(AtomicU64::new(0)),
            Some(PlaybackFanout {
                feed,
                reserve,
                ledger: Arc::clone(&ledger),
                lead_ms,
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

    fn finished(utterance: u64, eoa_written: bool) -> PlaybackEvent {
        PlaybackEvent::Finished {
            pod: PodId("pod-x".into()),
            in_reply_to: Some(UtteranceId(utterance)),
            frames: 3,
            samples: 960,
            eoa_written,
            writer_dying: false,
        }
    }

    /// A `Finished` for a job that played out but whose end-of-audio write failed:
    /// the writer is exiting, and the turn settles unclean.
    fn finished_writer_dying(utterance: u64) -> PlaybackEvent {
        PlaybackEvent::Finished {
            pod: PodId("pod-x".into()),
            in_reply_to: Some(UtteranceId(utterance)),
            frames: 3,
            samples: 960,
            eoa_written: false,
            writer_dying: true,
        }
    }

    /// Assert the floor does not move for `ms`. A closed channel counts as quiet:
    /// the adapter and any timer it spawned are gone, so nothing can move it.
    async fn floor_quiet(rx: &mut tokio::sync::mpsc::UnboundedReceiver<Feed>, ms: u64) {
        match tokio::time::timeout(std::time::Duration::from_millis(ms), rx.recv()).await {
            Err(_) | Ok(None) => {}
            Ok(Some(feed)) => panic!("the floor moved: {feed:?}"),
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
    async fn a_started_job_opens_the_floor_with_its_interruptibility() {
        // The floor is what gates detection; a non-interruptible job (an alert)
        // opens it closed to barge-in, which is the flag's whole purpose.
        for interruptible in [true, false] {
            let (mut rx, _) = run_fanout(
                vec![PlaybackEvent::Started {
                    pod: PodId("pod-x".into()),
                    in_reply_to: Some(UtteranceId(1)),
                    timings: Box::new(full_timings()),
                    speak_rx: at_ms(1_740).unwrap(),
                    first_write: at_ms(2_101).unwrap(),
                    samples: 320,
                    interruptible,
                }],
                50,
            )
            .await;

            assert_eq!(next_floor(&mut rx).await, (true, interruptible));
        }
    }

    #[tokio::test]
    async fn every_way_a_job_ends_eventually_closes_the_floor() {
        // Including a writer dying on a failed end-of-audio write, which may have no
        // `Aborted` behind it: a floor left open there would let sustained room
        // speech mint a wake-less dispatch until the next reconnect. A plain
        // not-drained `Finished` closes the floor too (its scheduled close stands
        // when no next `Started` supersedes it).
        let ends = [
            finished(1, true),
            finished(1, false),
            finished_writer_dying(1),
            PlaybackEvent::Aborted {
                pod: PodId("pod-x".into()),
                in_reply_to: Some(UtteranceId(1)),
                reason: AbortReason::WriteError,
            },
        ];
        for end in ends {
            let label = format!("{end:?}");
            let (mut rx, _) = run_fanout(vec![end], 1).await;
            assert_eq!(next_floor(&mut rx).await, (false, false), "{label}");
        }
    }

    /// The floor's opens and closes reach the listener in the order they
    /// happened, over the same bounded channel production uses. A close that
    /// overtook the open behind it would blind detection for a whole response.
    #[tokio::test]
    async fn floor_moves_stay_ordered_over_the_real_feed_channel() {
        let flushed_after_barge = PlaybackEvent::Flushed {
            pod: PodId("pod-x".into()),
            in_reply_to: Some(UtteranceId(2)),
            was_playing: true,
            frames_written: 3,
            progress: InterruptProgress {
                heard_ms: 10,
                total_ms: 20,
            },
        };
        let (mut rx, _) = run_fanout(
            vec![
                started_event(full_timings()),
                flushed_after_barge,
                started_event(full_timings()),
            ],
            1,
        )
        .await;
        let mut seen = Vec::new();
        for _ in 0..3 {
            seen.push(next_floor(&mut rx).await.0);
        }
        assert_eq!(seen, [true, false, true], "floor moves in order");
    }

    #[tokio::test]
    async fn the_floor_stays_open_for_the_pacers_lead_after_the_last_write() {
        // `Finished` fires at the last *write*, up to `lead_ms` before the last
        // audible sample. Closing the floor there would blind detection for the
        // response's final second — exactly when someone who has heard enough
        // speaks up.
        let (mut rx, _) = run_fanout(vec![finished(1, true)], 300).await;

        floor_quiet(&mut rx, 100).await;
        assert_eq!(next_floor(&mut rx).await, (false, false));
    }

    #[tokio::test]
    async fn a_new_job_supersedes_a_pending_floor_close() {
        // Back-to-back clips: the first one's pending close must not land during
        // the second one's playback and blind detection mid-response.
        let (mut rx, _) = run_fanout(
            vec![
                finished(1, true),
                PlaybackEvent::Started {
                    pod: PodId("pod-x".into()),
                    in_reply_to: Some(UtteranceId(2)),
                    timings: Box::new(full_timings()),
                    speak_rx: at_ms(1_740).unwrap(),
                    first_write: at_ms(2_101).unwrap(),
                    samples: 320,
                    interruptible: true,
                },
            ],
            50,
        )
        .await;

        assert_eq!(next_floor(&mut rx).await, (true, true));
        // The first clip's pending close was superseded, not merely delayed.
        floor_quiet(&mut rx, 150).await;
    }

    #[tokio::test]
    async fn a_flush_closes_the_floor_at_once() {
        // The barge already happened and the device has discarded its bank, so
        // there is no lead left to wait out.
        let (mut rx, _) = run_fanout(
            vec![PlaybackEvent::Flushed {
                pod: PodId("pod-x".into()),
                in_reply_to: Some(UtteranceId(1)),
                was_playing: true,
                frames_written: 10,
                progress: InterruptProgress {
                    heard_ms: 200,
                    total_ms: 1_000,
                },
            }],
            60_000,
        )
        .await;

        // The lead is a minute; only the immediate path can produce this.
        assert_eq!(next_floor(&mut rx).await, (false, false));
    }

    #[tokio::test]
    async fn an_evicted_job_settles_without_touching_the_floor() {
        // A queued job flushed behind the playing one was never audible, so it has
        // nothing to say about whether the pod is speaking.
        let (mut rx, _) = run_fanout(
            vec![PlaybackEvent::Flushed {
                pod: PodId("pod-x".into()),
                in_reply_to: Some(UtteranceId(1)),
                was_playing: false,
                frames_written: 0,
                progress: InterruptProgress {
                    heard_ms: 0,
                    total_ms: 0,
                },
            }],
            50,
        )
        .await;

        floor_quiet(&mut rx, 150).await;
    }

    #[tokio::test]
    async fn a_played_out_job_settles_clean_even_when_it_did_not_drain() {
        // Settlement is what decides a turn completed and the chain can drop, so
        // each terminal shape has to carry the right verdict. A job that played out
        // is clean whether it drained the stream (`eoa_written: true`) or finished
        // with another job queued behind it (`eoa_written: false`, no end-of-audio
        // yet all its audio delivered). Only a writer dying on a failed end-of-audio
        // write, an abort, or a flush settles unclean.
        let cases = [
            (finished(1, true), true),
            (finished(1, false), true),
            (finished_writer_dying(1), false),
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
            let (feed, reserve, _rx2) = spy_feed();
            let adapter = playback_event_adapter(
                jsonl.clone(),
                Arc::new(AtomicU64::new(0)),
                Some(PlaybackFanout {
                    feed,
                    reserve,
                    ledger: Arc::clone(&ledger),
                    lead_ms: 1,
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
}
