//! The pipeline task: the single consumer of the assembler→pipeline queue, now
//! carrying both assembled `Segment`s and the continuous listener's
//! `ListenerEvent`s as [`PipelineItem`]s.
//!
//! Segments are demoted to recording/tracking artifacts: for each one the task
//! stamps the tracking-emit time, emits the DoA-bearing `TrackingEvent`, and
//! labels the record-store sidecar. Sidecar wake-class labeling is *inverted* —
//! the task keeps a short per-pod list of recent wake detections and segments, so
//! a segment is `positive` when a detection's `wake_end_sample` falls in its span
//! (a late detection upgrades a provisional `negative`), never scored inline.
//!
//! Utterance semantics come from the listener:
//!
//! - `SoftEndpoint` spawns an abortable speculative STT on the carved PCM; at most
//!   one in-flight per pod, and a new soft endpoint (id ≥ the in-flight one) aborts
//!   the previous before spawning.
//! - `Superseded` aborts that pod's in-flight STT (a continuation re-STTs the whole
//!   utterance on its next soft endpoint).
//! - STT completing runs the dispatch-delay seam (trivially zero today), mints the
//!   `Utterance` from the carve plus the pod's recent-segment tracking, applies the
//!   confidence gate, and dispatches to the brain.
//!
//! A dead listener/brain is not this task's fault to detect; `run` returns
//! `PipelineFatal` only on its own internal faults.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

use futures::channel::mpsc as fmpsc;
use futures::{FutureExt, StreamExt};
use pod_ingest::{HostMicros, SegmentRef};
use serde::Serialize;
use serde_json::json;
use speech_pipeline::{
    AudioSpan, Brain, BrainEvent, BrainEventFn, BrainStats, CarveTiming, CarvedUtterance,
    ConfidenceGate, DoaTrack, EndpointCause, FlushRejected, GateReject, InterruptProgress,
    ListenerEvent, ListenerUtteranceId, PodId, ResponseSink, RoomId, SPINE_FORMAT, Segment,
    SegmentAudio, SegmentTelemetry, SpeakBody, SpeakCmd, StageTimings, TrackingEvent,
    TranscribeError, Transcriber, Transcript, Utterance, UtteranceId, WakeCommandReason,
    WakeConfirmation, stage_delta_us, tracking_event,
};
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

use crate::barge::TurnLedger;
use crate::jsonl::JsonlHandle;
use crate::recorder::{
    WakeClass, WakeClassUpdate, sanitize_filename, set_wake_class, sidecar_path,
};

/// The pipeline exited on an unrecoverable fault. The server renders this to a
/// `pipeline_fatal` JSONL line and a nonzero exit.
#[derive(Debug)]
pub struct PipelineFatal {
    pub detail: String,
}

/// One item on the pipeline queue: an assembled transport segment (tracking +
/// sidecar) or a listener event (wake detection + carved-utterance lifecycle).
///
/// A segment carries its connection `epoch` alongside the assembled audio — the
/// same per-pod connection sequence the listener stamps its events with, so both
/// arms of the queue agree on which connection's index space an item belongs to.
#[derive(Debug)]
pub enum PipelineItem {
    Segment { seg: Segment, epoch: u64 },
    Listener(ListenerEvent),
}

/// The brain and the response channel it writes into. `None` in `PipelineCtx.brain`
/// is the no-brain pipeline: STT still runs and utterances still mint and emit, but
/// nothing is dispatched.
pub struct BrainWiring {
    pub brain: Arc<dyn Brain>,
    /// The shared response channel; a fresh clone becomes each utterance's sink.
    pub speak_tx: fmpsc::Sender<SpeakCmd>,
    /// The brain's event sink, shared with the brain itself (one no-command story).
    pub events: BrainEventFn,
    pub stats: Arc<BrainStats>,
}

/// Resolves a pod to its live playback writer and requests a flush of whatever it
/// is playing, returning the turn that was cut and how much of it the user heard.
/// A closure so the `PlaybackRegistry` stays private to the surface, mirroring the
/// way `playback_try_play` wraps routing.
pub(crate) type FlushFn =
    Arc<dyn Fn(&PodId) -> Result<(UtteranceId, InterruptProgress), FlushRejected> + Send + Sync>;

/// The barge-in wiring: the ledger the interrupted turn is chained in, and the
/// flush entry point that cuts its audio.
pub(crate) struct BargeWiring {
    pub(crate) ledger: Arc<TurnLedger>,
    pub(crate) flush: FlushFn,
}

/// Pass-through configuration and shared counters for [`run`].
pub struct PipelineCtx {
    /// The record-store directory, or `None` when recording is disabled.
    pub record_dir: Option<PathBuf>,
    /// Backward host-clock steps clamped in a stage delta, counted for `stage_health`.
    pub clock_step_clamps: Arc<AtomicU64>,
    /// The wired transcriber, or `None` for the no-STT pipeline (null transcript).
    pub transcriber: Option<Arc<dyn Transcriber>>,
    /// The wired brain and its response channel, or `None` for the no-brain pipeline.
    pub brain: Option<BrainWiring>,
    /// STT-confidence gate applied before brain dispatch; fail-open (no summary).
    pub confidence_gate: ConfidenceGate,
    /// Barge-in wiring, or `None` in a pipeline with no playback path (the replay
    /// rigs), where a detected barge-in is a log line and nothing more.
    pub(crate) barge: Option<BargeWiring>,
}

/// How many recent segments and wake detections to retain per pod for sidecar
/// labeling and audio-span resolution. A wake and its containing segment arrive
/// through different tasks, so a small window absorbs their relative reordering.
const RECENT_WINDOW: usize = 16;

/// Everything captured at carve time that the STT task carries back so the loop
/// can mint the `Utterance` once transcription settles.
#[derive(Debug, Clone)]
struct Carve {
    id: ListenerUtteranceId,
    start_sample: u64,
    end_sample: u64,
    wake: Option<WakeConfirmation>,
    cause: EndpointCause,
    /// This carve is the speech that barged in on playback, so the mint attaches
    /// the pod's context chain to it.
    barge_in: bool,
    /// The listener's host-receipt stamps for this utterance's audio, from t0 to
    /// the carve. Copied onto the minted `Utterance`'s `StageTimings`.
    timing: CarveTiming,
}

/// A completed (or no-op) speculative STT, sent back into the loop.
struct SttDone {
    pod: PodId,
    /// Identifies the exact spawn, so a stale completion racing a supersede/respawn
    /// (a continuation reuses its id) is dropped rather than dispatched.
    nonce: u64,
    carve: Carve,
    /// `None` when no transcriber is wired; `Some(Err)` on STT failure (mint anyway).
    result: Option<Result<Transcript, TranscribeError>>,
    elapsed_us: u64,
}

/// A pod's in-flight speculative STT: the spawn nonce (matched on completion), the
/// utterance id (compared against a later soft endpoint), and the abort handle.
struct InFlight {
    id: ListenerUtteranceId,
    nonce: u64,
    abort: AbortHandle,
    /// When this spawn went out, for the minted utterance's `stt_started`. Held
    /// here rather than sent through the STT task, which measures its own in-task
    /// elapsed time and has no use for the host stamp.
    stt_started: HostMicros,
}

/// A recently-assembled segment, retained so a listener utterance carved across it
/// resolves to real audio and a late wake detection can upgrade its sidecar label.
struct RecentSegment {
    segment_id: u32,
    base: u64,
    len: u64,
    seg_ref: SegmentRef,
    room: RoomId,
    telemetry: Vec<SegmentTelemetry>,
    /// The sidecar class last written for this segment (upgraded `negative→positive`).
    class: WakeClass,
}

impl RecentSegment {
    fn contains(&self, sample: u64) -> bool {
        sample >= self.base && sample < self.base.saturating_add(self.len)
    }
}

/// Per-pod pipeline state.
#[derive(Default)]
struct PodState {
    recent_segments: VecDeque<RecentSegment>,
    recent_wakes: VecDeque<u64>,
    in_flight: Option<InFlight>,
    /// Highest connection epoch seen. Every segment and listener event carries the
    /// epoch of the connection that produced it; a rise means the pod reconnected
    /// (and its absolute sample-index space restarted with it), a fall means the
    /// event is a straggler from a superseded connection.
    epoch: u64,
    /// Per-pod spawn nonce mint.
    spawn_seq: u64,
}

impl PodState {
    /// Adopt `epoch` for this pod, reporting whether the event carrying it is
    /// current. A rise is a reconnect: the absolute sample-index space restarts
    /// with the connection, so the tracking keyed on it goes — a pre-reconnect wake
    /// must not label a post-reconnect segment (corpus corruption), a pre-reconnect
    /// `SegmentRef` must not stitch into a post-reconnect utterance's span (wrong
    /// replay audio), and the in-flight STT belonged to the old connection.
    ///
    /// The epoch is the reconnect signal precisely because sample indexes are not:
    /// a segment's preroll is stamped with its samples' original capture indexes, so
    /// a segment opening within one preroll of the previous close legitimately bases
    /// *behind* the prior segment's end (and re-scores a wake there), all within one
    /// connection. Inferring a reconnect from a backward index would fire on that
    /// every time.
    fn adopt_epoch(&mut self, epoch: u64) -> bool {
        if epoch < self.epoch {
            return false;
        }
        if epoch > self.epoch {
            self.recent_segments.clear();
            self.recent_wakes.clear();
            if let Some(f) = self.in_flight.take() {
                f.abort.abort();
            }
            self.epoch = epoch;
        }
        true
    }
}

/// Consume the pipeline queue until every sender drops and it drains. Segments are
/// tracked and sidecar-labeled; listener events drive speculative STT and dispatch.
pub async fn run(
    mut rx: speech_pipeline::Receiver<PipelineItem>,
    ctx: PipelineCtx,
    jsonl: JsonlHandle,
) -> Result<(), PipelineFatal> {
    let PipelineCtx {
        record_dir,
        clock_step_clamps,
        transcriber,
        brain,
        confidence_gate,
        barge,
    } = ctx;

    let mut pods: HashMap<PodId, PodState> = HashMap::new();
    // One `Utterance` id per dispatched utterance; unique within this loop (the
    // single minter), scoped locally so concurrent pipelines never interleave.
    let mut next_utterance_id: u64 = 1;
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<SttDone>();

    // Once the queue closes, keep servicing STT completions until every pod's
    // in-flight speculative STT has settled — a completion arrives after the item
    // that spawned it, so breaking on the closed queue alone would drop the last
    // utterance's dispatch.
    let mut queue_closed = false;
    loop {
        if queue_closed && !pods.values().any(|s| s.in_flight.is_some()) {
            break;
        }
        tokio::select! {
            item = rx.recv(), if !queue_closed => match item {
                None => queue_closed = true,
                Some(PipelineItem::Segment { seg, epoch }) => {
                    handle_segment(seg, epoch, &mut pods, record_dir.as_deref(), &clock_step_clamps, &jsonl)
                        .await;
                }
                Some(PipelineItem::Listener(ev)) => {
                    handle_listener(
                        ev,
                        &mut pods,
                        record_dir.as_deref(),
                        transcriber.as_ref(),
                        &done_tx,
                        &mut next_utterance_id,
                        brain.as_ref(),
                        barge.as_ref(),
                        &jsonl,
                    )
                    .await;
                }
            },
            Some(done) = done_rx.recv() => {
                handle_stt_done(
                    done,
                    &mut pods,
                    &mut next_utterance_id,
                    &confidence_gate,
                    brain.as_ref(),
                    barge.as_ref(),
                    &jsonl,
                )
                .await;
            }
        }
    }
    Ok(())
}

/// Track and sidecar-label one assembled segment (recording/tracking only — no
/// wake scoring, no utterance minting).
async fn handle_segment(
    mut seg: Segment,
    epoch: u64,
    pods: &mut HashMap<PodId, PodState>,
    record_dir: Option<&Path>,
    clock_step_clamps: &AtomicU64,
    jsonl: &JsonlHandle,
) {
    seg.timings.tracking_emitted = Some(HostMicros::now());
    let event = tracking_event(&seg);
    jsonl.emit(
        "tracking",
        &TrackingLine {
            event: &event,
            assembled_to_tracking_us: assembled_to_tracking_us(&seg.timings, clock_step_clamps),
        },
    );

    let state = pods.entry(seg.pod.clone()).or_default();
    // Tracking above is unconditional (it describes the segment, not the pod's
    // state); everything below is keyed on the connection's index space, so a
    // straggler from a superseded connection stops here rather than labeling or
    // stitching against the live one.
    if !state.adopt_epoch(epoch) {
        return;
    }
    let base = seg.base_sample_index;
    // Sample indexes are wire-controlled; saturate so a near-`u64::MAX` base cannot
    // overflow the end (panic in debug, wrap in release).
    let end = base.saturating_add(seg.pcm.len() as u64);
    // A segment is `positive` if any recent wake detection lands in its span; else
    // provisionally `negative` (a later detection can upgrade it, never downgrade).
    let positive = state.recent_wakes.iter().any(|&w| w >= base && w < end);
    let class = if positive {
        WakeClass::Positive
    } else {
        WakeClass::Negative
    };

    let log = seg.audio_ref.log.clone();
    let recent = RecentSegment {
        segment_id: seg.segment_id,
        base,
        len: seg.pcm.len() as u64,
        seg_ref: seg.audio_ref.clone(),
        room: seg.room.clone(),
        telemetry: seg.telemetry.clone(),
        class,
    };
    push_bounded(&mut state.recent_segments, recent);

    if let Some(dir) = record_dir {
        label_sidecar(
            dir,
            &log,
            seg.segment_id,
            seg.audio_ref.part,
            &seg.pod.0,
            class,
            jsonl,
        )
        .await;
    }
}

/// Build a listener-event payload: `envelope`'s caller context (the daemon stamps
/// pod + epoch, the replay rig its log name) merged with `payload`'s own fields,
/// serialized from the payload type itself.
///
/// The payload type is the single schema source, so a field added to it reaches
/// both the daemon's JSONL line and the tuning rig with no edit here — a
/// hand-mapped literal at each site would instead compile clean while silently
/// dropping the new field. Both inputs serialize to JSON objects, so the merge is
/// total; a non-object envelope would simply contribute nothing.
pub fn event_line(envelope: serde_json::Value, payload: &impl Serialize) -> serde_json::Value {
    let mut line = serde_json::to_value(payload).unwrap_or(serde_json::Value::Null);
    if let (serde_json::Value::Object(fields), serde_json::Value::Object(env)) =
        (&mut line, envelope)
    {
        fields.extend(env);
    }
    line
}

/// Route one listener event: record wakes (and upgrade sidecar labels), spawn or
/// abort speculative STT.
#[allow(clippy::too_many_arguments)]
async fn handle_listener(
    ev: ListenerEvent,
    pods: &mut HashMap<PodId, PodState>,
    record_dir: Option<&Path>,
    transcriber: Option<&Arc<dyn Transcriber>>,
    done_tx: &mpsc::UnboundedSender<SttDone>,
    next_utterance_id: &mut u64,
    brain: Option<&BrainWiring>,
    barge: Option<&BargeWiring>,
    jsonl: &JsonlHandle,
) {
    match ev {
        ListenerEvent::WakeDetected {
            pod,
            epoch,
            score,
            wake_end_sample,
        } => {
            jsonl.emit(
                "wake_detected",
                &json!({ "pod": pod.0, "epoch": epoch, "score": score, "wake_end_sample": wake_end_sample }),
            );
            let state = pods.entry(pod.clone()).or_default();
            if !state.adopt_epoch(epoch) {
                return; // Stale: a reconnect superseded this epoch.
            }
            push_bounded(&mut state.recent_wakes, wake_end_sample);
            // Upgrade any already-labeled segment this detection now lands in: the
            // wake surfaced after its containing segment was assembled and provisionally
            // labeled `negative`. `positive` never downgrades.
            if let Some(dir) = record_dir {
                let upgrades: Vec<(String, u32, u16)> = state
                    .recent_segments
                    .iter_mut()
                    .filter(|s| s.class == WakeClass::Negative && s.contains(wake_end_sample))
                    .map(|s| {
                        s.class = WakeClass::Positive;
                        (s.seg_ref.log.clone(), s.segment_id, s.seg_ref.part)
                    })
                    .collect();
                for (log, segment_id, part) in upgrades {
                    label_sidecar(
                        dir,
                        &log,
                        segment_id,
                        part,
                        &pod.0,
                        WakeClass::Positive,
                        jsonl,
                    )
                    .await;
                }
            }
        }
        ListenerEvent::SoftEndpoint { pod, utterance } => {
            let uid = utterance.utterance_id.clone();
            let state = pods.entry(pod.clone()).or_default();
            if !state.adopt_epoch(uid.epoch) {
                return; // Stale: a reconnect superseded this epoch.
            }
            // Abort any in-flight STT at id ≤ the arriving one (a continuation reuses
            // its id, so this covers the re-STT-the-whole-utterance case), then spawn.
            if let Some(f) = state.in_flight.take() {
                if f.id.order_key() <= uid.order_key() {
                    f.abort.abort();
                } else {
                    state.in_flight = Some(f); // A newer utterance already in flight.
                    return;
                }
            }
            state.spawn_seq += 1;
            let nonce = state.spawn_seq;
            // Only when there is an STT to start: with no transcriber wired the
            // spawned task completes immediately with no transcript, and the line
            // would narrate inference that never ran on a daemon that announced
            // `stt_absent` at startup. The stamp below is taken regardless — it
            // measures the listener → pipeline hop, which is real either way.
            if transcriber.is_some() {
                jsonl.emit(
                    "stt_started",
                    &json!({
                        "pod": pod.0,
                        "utterance_seq": uid.seq,
                        "samples": utterance.pcm.len(),
                    }),
                );
            }
            let stt_started = HostMicros::now();
            let abort = spawn_stt(pod.clone(), nonce, utterance, transcriber.cloned(), done_tx);
            state.in_flight = Some(InFlight {
                id: uid,
                nonce,
                abort,
                stt_started,
            });
        }
        ListenerEvent::EndpointerTransition {
            pod,
            epoch,
            transition,
        } => {
            // Pure observability: the endpointer's timing is what the tuning rig
            // and a live-latency investigation read. No per-pod state effect.
            jsonl.emit(
                "endpointer_transition",
                &event_line(json!({ "pod": pod.0, "epoch": epoch }), &transition),
            );
        }
        ListenerEvent::BargeIn {
            pod,
            epoch,
            trigger_sample,
            host_rx,
        } => {
            // The trigger's own line, ahead of anything it drives: a run with no
            // playback wiring (replay, tuning) otherwise leaves detection with no
            // trace at all, which is exactly the thing being tuned.
            jsonl.emit(
                "barge_in",
                &json!({
                    "pod": pod.0,
                    "epoch": epoch,
                    "trigger_sample": trigger_sample,
                    "host_rx_us": host_rx.0,
                }),
            );
            let Some(barge) = barge else {
                return;
            };
            // Mouth first: the audio the user is talking over stops before anything
            // else is decided.
            let (turn, progress) = match (barge.flush)(&pod) {
                Ok(cut) => cut,
                Err(reason) => {
                    // Playback ended in the gap between the trigger and here, or a
                    // non-interruptible job raced in. Nothing to cut, so nothing to
                    // chain — but the user did speak, and their utterance still
                    // carves and dispatches on its own.
                    jsonl.emit("barge_in_stale", &json!({ "pod": pod.0, "reason": reason }));
                    return;
                }
            };
            // Throat: mark the turn before anything else, so a `SpeakCmd` for it can
            // never land after the flush has already cut its audio.
            barge.ledger.interrupt(&pod, turn, progress);
            jsonl.emit(
                "playback_interrupted",
                &json!({
                    "pod": pod.0,
                    "utterance": turn,
                    "heard_ms": progress.heard_ms,
                    "total_ms": progress.total_ms,
                }),
            );
            // Mind: a no-op in every brain today. The seam exists so the interrupt
            // has somewhere to go the moment a brain wants it.
            if let Some(wiring) = brain {
                wiring.brain.interrupt(turn, progress);
            }
        }
        ListenerEvent::ModelStats {
            pod,
            epoch,
            model,
            cause,
            summary,
        } => {
            // Pure observability, like the transition above: what the models were
            // returning, which is the reading a room that never transitions needs.
            jsonl.emit(
                "model_stats",
                &event_line(
                    json!({ "pod": pod.0, "epoch": epoch, "model": model, "cause": cause }),
                    &summary,
                ),
            );
        }
        ListenerEvent::Superseded { pod, utterance_id } => {
            // Emitted before the abort so a supersede is correlatable by utterance
            // id; the transition line alone names no utterance.
            jsonl.emit(
                "utterance_superseded",
                &json!({ "pod": pod.0, "utterance_id": utterance_id }),
            );
            let Some(state) = pods.get_mut(&pod) else {
                return;
            };
            if utterance_id.epoch < state.epoch {
                return;
            }
            if let Some(f) = state.in_flight.as_ref()
                && f.id == utterance_id
            {
                f.abort.abort();
                state.in_flight = None;
            }
        }
        ListenerEvent::UtteranceClosed { pod, utterance_id } => {
            // Dispatch happens on STT completion, not on close, so this drives
            // nothing — but it is the utterance's final boundary, worth a line.
            jsonl.emit(
                "utterance_closed",
                &json!({ "pod": pod.0, "utterance_id": utterance_id }),
            );
        }
        ListenerEvent::ArmExpired {
            pod,
            wake,
            start_sample,
            end_sample,
        } => {
            // Emitted ahead of the brain gate below: a brainless run — the tuning
            // and replay setting — otherwise leaves arm expiry with no trace at all.
            jsonl.emit(
                "arm_expired",
                &json!({
                    "pod": pod.0,
                    "score": wake.score,
                    "start_sample": start_sample,
                    "end_sample": end_sample,
                }),
            );
            // "Wake, no follow": the wake fired but no command followed. Accounted
            // for through the same `WakeCommandAbsent` vocabulary as an empty or
            // low-confidence command — only meaningful with a brain wired (the
            // event sink + counter), the same as the confidence-gate decline. STT
            // never ran, so there is no transcript to attach.
            let Some(wiring) = brain else {
                return;
            };
            let state = pods.entry(pod.clone()).or_default();
            let audio_ref =
                build_audio_span(&state.recent_segments, start_sample, end_sample, &pod);
            let id = UtteranceId(*next_utterance_id);
            *next_utterance_id += 1;
            (wiring.events)(BrainEvent::wake_command_absent(
                id,
                audio_ref,
                &wake,
                WakeCommandReason::ArmExpired,
            ));
            wiring.stats.record_wake_command_absent();
        }
    }
}

/// Spawn the speculative STT for a carved utterance, returning its abort handle.
/// When no transcriber is wired the task reports a null-transcript completion, so
/// the dispatch path (supersede, gate, brain) is one shape regardless of STT.
fn spawn_stt(
    pod: PodId,
    nonce: u64,
    utterance: CarvedUtterance,
    transcriber: Option<Arc<dyn Transcriber>>,
    done_tx: &mpsc::UnboundedSender<SttDone>,
) -> AbortHandle {
    let CarvedUtterance {
        utterance_id,
        pcm,
        start_sample,
        end_sample,
        wake,
        stt_trim_samples,
        cause,
        barge_in,
        timing,
    } = utterance;
    let carve = Carve {
        id: utterance_id,
        start_sample,
        end_sample,
        wake,
        cause,
        // STT runs on the audio the same way whatever opened the floor; the mark
        // rides through so the mint on the far side can chain the interrupted turns.
        barge_in,
        timing,
    };
    let done_tx = done_tx.clone();
    let handle = tokio::spawn(async move {
        let started = Instant::now();
        // Catch a panic in the inference path: a dead transcriber then surfaces as
        // an STT error completion (which clears the pod's in-flight slot) rather
        // than a dropped `SttDone` that would leave the slot occupied and wedge the
        // shutdown drain.
        let outcome = std::panic::AssertUnwindSafe(async {
            match transcriber {
                None => None,
                Some(t) => {
                    // `stt_trim_samples` is carve-relative and must not exceed the
                    // carved PCM; clamp defensively (an empty tail beats a panic)
                    // and assert the invariant loudly in debug.
                    debug_assert!(
                        stt_trim_samples <= pcm.len(),
                        "stt_trim_samples exceeds carved PCM length",
                    );
                    let trim = stt_trim_samples.min(pcm.len());
                    Some(transcribe_pcm(&t, &pcm[trim..]).await)
                }
            }
        })
        .catch_unwind()
        .await;
        let result = outcome
            .unwrap_or_else(|_| Some(Err(TranscribeError::Decode("stt task panicked".into()))));
        let _ = done_tx.send(SttDone {
            pod,
            nonce,
            carve,
            result,
            elapsed_us: started.elapsed().as_micros() as u64,
        });
    });
    handle.abort_handle()
}

/// The dispatch mode for the dispatch-delay seam. Only `Command` today; chat mode
/// (a longer per-turn delay) attaches here later.
#[derive(Debug, Clone, Copy)]
pub enum DispatchMode {
    Command,
}

/// The dispatch-delay seam: how long to wait after STT settles before dispatching
/// to the brain, so an "uh…" tail can extend an incomplete command. The transcript
/// is available because STT ran speculatively — the whole point of the seam. The
/// trivial body ships zero for command mode; a nonzero delay would schedule a
/// cancelable timer that a `Superseded` aborts.
fn dispatch_delay(_transcript_tail: &str, _silence_ms: u32, mode: DispatchMode) -> Duration {
    match mode {
        DispatchMode::Command => Duration::ZERO,
    }
}

/// Handle a settled speculative STT: verify it still owns the pod's in-flight slot
/// (a supersede/respawn would have replaced it), then run the dispatch-delay seam,
/// mint the utterance from the carve plus recent-segment tracking, apply the
/// confidence gate, and dispatch.
async fn handle_stt_done(
    done: SttDone,
    pods: &mut HashMap<PodId, PodState>,
    next_utterance_id: &mut u64,
    confidence_gate: &ConfidenceGate,
    brain: Option<&BrainWiring>,
    barge: Option<&BargeWiring>,
    jsonl: &JsonlHandle,
) {
    let Some(state) = pods.get_mut(&done.pod) else {
        return;
    };
    // Only the current spawn dispatches: a stale completion (superseded, or a
    // continuation re-spawn reusing the same id) never reaches the brain.
    let stt_started = match state.in_flight.as_ref() {
        Some(f) if f.nonce == done.nonce => f.stt_started,
        _ => return,
    };
    state.in_flight = None;

    // The in-task measurement, which excludes the completion-queue delay that
    // `stt_started → transcribed` includes. Only a success carries it onto the
    // utterance line; a failure already reports it on its own line.
    let mut stt_elapsed_us = None;
    let (transcript, transcribed) = match done.result {
        None => (None, false),
        Some(Ok(t)) => {
            stt_elapsed_us = Some(done.elapsed_us);
            (Some(t), true)
        }
        Some(Err(e)) => {
            jsonl.emit(
                "stt_failed",
                &SttFailedLine {
                    pod: &done.pod.0,
                    utterance_seq: done.carve.id.seq,
                    detail: e.to_string(),
                    elapsed_us: done.elapsed_us,
                },
            );
            (None, false)
        }
    };

    // The dispatch-delay seam. Zero today, so dispatch proceeds inline; the assert
    // makes a future nonzero body loud until the cancelable timer is wired (honoring
    // a nonzero value inline here would not be cancellation-safe).
    let tail = transcript.as_ref().map(|t| t.text.as_str()).unwrap_or("");
    let delay = dispatch_delay(tail, 0, DispatchMode::Command);
    debug_assert!(
        delay.is_zero(),
        "dispatch_delay returned nonzero but the cancelable timer is not wired",
    );
    let _ = delay;

    // Resolve the carved span against the pod's recent segments for the wire
    // reference, room, and DoA.
    let audio_ref = build_audio_span(
        &state.recent_segments,
        done.carve.start_sample,
        done.carve.end_sample,
        &done.pod,
    );
    let (room, doa) = span_context(&state.recent_segments, &done.carve);

    let id = *next_utterance_id;
    *next_utterance_id += 1;
    // The carve's host-receipt stamps become the utterance's; everything the
    // latency summary reports is referenced to `first_audio_rx` as t0, so the
    // provenance flag rides along with the stamp it qualifies.
    let t = done.carve.timing;
    let mut timings = StageTimings {
        first_audio_rx: t.first_audio_rx,
        t0_projected: t.first_audio_rx.map(|_| t.t0_projected),
        vad_high_est: t.vad_high_est,
        wake_detected_rx: t.wake_detected_rx,
        onset_rx: t.onset_rx,
        soft_endpoint_rx: t.soft_endpoint_rx,
        stt_started: Some(stt_started),
        ..StageTimings::default()
    };
    if transcribed {
        timings.transcribed = Some(HostMicros::now());
    }
    if brain.is_some() {
        timings.brain_dispatched = Some(HostMicros::now());
    }
    // A carve that barged in on playback carries the chain of every turn cut since
    // the last clean completion — but only when there is one. A barge whose flush
    // was stale, or whose previous turn completed cleanly, finds the chain already
    // cleared and dispatches as a plain utterance, so no consumer ever has to
    // reason about an empty chain.
    let barge_context = (done.carve.barge_in)
        .then(|| barge.and_then(|b| b.ledger.chain(&done.pod)))
        .flatten();
    let utterance = Utterance {
        id: UtteranceId(id),
        pod: done.pod.clone(),
        room,
        speaker: None,
        doa,
        audio_ref,
        transcript,
        timings,
        endpoint_cause: done.carve.cause,
        wake: done.carve.wake,
        barge_in: barge_context,
    };
    jsonl.emit(
        "utterance",
        &UtteranceLine {
            utterance: &utterance,
            stt_elapsed_us,
        },
    );

    let Some(wiring) = brain else {
        return;
    };
    // STT-confidence gate: a trigger whose text trips the gate is a likely
    // hallucination — declined as a no-command outcome, never echoed. Fail-open on a
    // missing summary; a bypassed (no-wake, no-barge) or empty transcript is never
    // gated. A scored wake accept is gated through its wake provenance; a barge-in
    // utterance has no wake word, so a second arm keyed on the barge mark declines
    // the barging speech that transcribed to nothing — the playback is already cut.
    let confidence_reject = utterance
        .transcript
        .as_ref()
        .filter(|t| !t.text.trim().is_empty())
        .and_then(|t| t.confidence.as_ref())
        .and_then(|conf| confidence_gate.evaluate(conf));
    let gate = match (utterance.wake, confidence_reject) {
        (Some(wake), Some(reject)) => GateOutcome::DeclineWake(wake, reject),
        (None, Some(reject)) if done.carve.barge_in => GateOutcome::DeclineBarge(reject),
        _ => GateOutcome::Dispatch,
    };
    match gate {
        GateOutcome::DeclineWake(wake, reject) => {
            decline_low_confidence(&utterance, &wake, reject, wiring)
        }
        GateOutcome::DeclineBarge(reject) => {
            decline_barge_low_confidence(&utterance, reject, wiring)
        }
        GateOutcome::Dispatch => {
            // Brain begin gets its own console instant. Emitted here, past the
            // gate, so the line marks a real dispatch — unlike `brain_dispatched`
            // in the timings, which is stamped before the gate decides.
            jsonl.emit(
                "brain_dispatched",
                &json!({ "pod": utterance.pod.0, "utterance": utterance.id }),
            );
            let (pod, id) = (utterance.pod.clone(), utterance.id);
            // Recorded at every dispatch, barge or not: this turn is what the *next*
            // interrupt would chain.
            let sink = match barge {
                Some(barge) => {
                    barge.ledger.record_dispatch(
                        &pod,
                        id,
                        utterance.transcript.as_ref().map(|t| t.text.clone()),
                    );
                    let ledger = Arc::clone(&barge.ledger);
                    let (tap_pod, tap_id) = (pod.clone(), id);
                    ResponseSink::with_tap(
                        wiring.speak_tx.clone(),
                        Arc::new(move |cmd: &SpeakCmd| {
                            ledger.record_cmd(
                                &tap_pod,
                                tap_id,
                                match &cmd.body {
                                    SpeakBody::Text(text) => Some(text.clone()),
                                    // A synthesized clip has no words to read back;
                                    // it still counts toward the turn's settlement.
                                    SpeakBody::Pcm(_) => None,
                                },
                            );
                        }),
                    )
                }
                None => ResponseSink::new(wiring.speak_tx.clone()),
            };
            wiring.brain.handle(utterance, sink).await;
            if let Some(barge) = barge {
                // Dispatch awaits the brain inline, so returning here is the proof
                // that no further command is coming for this turn — which is what
                // lets its settlement complete.
                barge.ledger.dispatch_done(&pod, id);
            }
        }
    }
}

/// Build an `AudioSpan` for `[start_sample, end_sample)` from the pod's recent
/// segments: every segment overlapping the range is a covering part, in order.
/// When no segment covers the span (carved before any close landed), the span
/// still names the range with no covering parts and the pod's last known log, and
/// resolves to spliced silence.
fn build_audio_span(
    recent: &VecDeque<RecentSegment>,
    start_sample: u64,
    end_sample: u64,
    pod: &PodId,
) -> AudioSpan {
    let mut covering: Vec<&RecentSegment> = recent
        .iter()
        .filter(|s| s.base < end_sample && s.base.saturating_add(s.len) > start_sample)
        .collect();
    covering.sort_by_key(|s| s.base);
    let log = covering
        .first()
        .map(|s| s.seg_ref.log.clone())
        .or_else(|| recent.back().map(|s| s.seg_ref.log.clone()))
        .unwrap_or_else(|| format!("{}.framelog", sanitize_filename(&pod.0)));
    AudioSpan {
        log,
        start_sample,
        end_sample,
        segments: covering.iter().map(|s| s.seg_ref.clone()).collect(),
    }
}

/// Room and DoA for a carved utterance, taken from the segment covering its onset
/// (falling back to the most recent segment). DoA/room are pod-scoped context; a
/// carve with no covering segment gets the pod's last room and an empty DoA track.
fn span_context(recent: &VecDeque<RecentSegment>, carve: &Carve) -> (RoomId, DoaTrack) {
    let seg = recent
        .iter()
        .find(|s| s.contains(carve.start_sample))
        .or_else(|| recent.back());
    match seg {
        Some(s) => (s.room.clone(), DoaTrack::from_telemetry(&s.telemetry)),
        None => (
            RoomId(crate::config::UNMAPPED_ROOM.to_string()),
            DoaTrack::default(),
        ),
    }
}

/// Push onto a bounded deque, evicting the oldest when full.
fn push_bounded<T>(dq: &mut VecDeque<T>, item: T) {
    if dq.len() == RECENT_WINDOW {
        dq.pop_front();
    }
    dq.push_back(item);
}

/// What the STT-confidence gate decided for a minted utterance: dispatch it, or
/// decline it as a likely hallucination — through the wake provenance for a scored
/// wake accept, or through the barge mark for a barge-in utterance with no wake.
enum GateOutcome {
    Dispatch,
    DeclineWake(WakeConfirmation, GateReject),
    DeclineBarge(GateReject),
}

/// Report a confidence-gated scored-wake accept through the brain's event/counter
/// vocabulary without dispatching it — a `WakeCommandAbsent` carrying the
/// low-confidence reason. A non-error outcome; the phantom text is never echoed.
fn decline_low_confidence(
    utterance: &Utterance,
    wake: &WakeConfirmation,
    reject: GateReject,
    wiring: &BrainWiring,
) {
    (wiring.events)(BrainEvent::wake_command_absent(
        utterance.id,
        utterance.audio_ref.clone(),
        wake,
        WakeCommandReason::LowConfidence {
            no_speech_prob: reject.no_speech_prob,
            avg_logprob: reject.avg_logprob,
        },
    ));
    wiring.stats.record_wake_command_absent();
}

/// Report a confidence-gated barge-in utterance without dispatching it — a
/// `BargeCommandAbsent` carrying the offending signals in place of wake provenance.
/// A non-error outcome: the barge already cut the playback, and the barging speech
/// transcribed to likely hallucination, so the phantom text is never echoed.
fn decline_barge_low_confidence(utterance: &Utterance, reject: GateReject, wiring: &BrainWiring) {
    (wiring.events)(BrainEvent::BargeCommandAbsent {
        utterance: utterance.id,
        audio_ref: utterance.audio_ref.clone(),
        no_speech_prob: reject.no_speech_prob,
        avg_logprob: reject.avg_logprob,
    });
    wiring.stats.record_barge_command_absent();
}

/// Label one segment's sidecar entry as a single locked read-modify-write; awaited
/// so the on-disk label lands before any observer. Soft outcomes are counted-warning
/// lines, a real I/O failure is a loud error line; never fatal.
#[allow(clippy::too_many_arguments)]
async fn label_sidecar(
    record_dir: &Path,
    log: &str,
    segment_id: u32,
    part: u16,
    pod: &str,
    class: WakeClass,
    jsonl: &JsonlHandle,
) {
    let sidecar = sidecar_path(&record_dir.join(log));
    let pod = pod.to_string();
    let result =
        tokio::task::spawn_blocking(move || set_wake_class(&sidecar, segment_id, part, class))
            .await;
    match result {
        Ok(Ok(WakeClassUpdate::Updated)) => {}
        Ok(Ok(WakeClassUpdate::NoSidecar)) => jsonl.emit(
            "wake_sidecar_skipped",
            &json!({ "pod": pod, "segment_id": segment_id, "part": part, "reason": "no_sidecar" }),
        ),
        Ok(Ok(WakeClassUpdate::NoSuchSegment)) => jsonl.emit(
            "wake_sidecar_skipped",
            &json!({ "pod": pod, "segment_id": segment_id, "part": part, "reason": "no_such_segment" }),
        ),
        Ok(Err(e)) => jsonl.emit(
            "wake_sidecar_error",
            &json!({ "pod": pod, "segment_id": segment_id, "part": part, "detail": e.to_string() }),
        ),
        Err(e) => jsonl.emit(
            "wake_sidecar_error",
            &json!({
                "pod": pod,
                "segment_id": segment_id,
                "part": part,
                "detail": format!("sidecar task panicked: {e}"),
            }),
        ),
    }
}

/// Same-domain assembled→tracking latency for the `tracking` JSONL line.
fn assembled_to_tracking_us(t: &StageTimings, clamps: &AtomicU64) -> Option<u64> {
    stage_delta_us(t.assembled, t.tracking_emitted, clamps)
}

/// Drive a transcriber's stream to completion for one PCM buffer, returning the
/// settled [`Transcript`] or the terminal error. A stream ending with neither a
/// final event nor an error is an implementation bug, reported as `Decode`.
async fn transcribe_pcm(
    transcriber: &Arc<dyn Transcriber>,
    pcm: &[i16],
) -> Result<Transcript, TranscribeError> {
    let audio = SegmentAudio {
        pcm: Arc::from(pcm),
        sample_rate_hz: SPINE_FORMAT.sample_rate_hz,
    };
    let mut stream = transcriber.transcribe(audio);
    while let Some(item) = stream.next().await {
        match item {
            Ok(event) => {
                if event.is_final {
                    return Ok(Transcript {
                        text: event.text,
                        confidence: event.confidence,
                    });
                }
            }
            Err(e) => return Err(e),
        }
    }
    Err(TranscribeError::Decode(
        "stream ended without a final event".into(),
    ))
}

/// The `stt_failed` JSONL line: identity plus the truncated error detail and the
/// locally-measured elapsed time of the failed attempt.
#[derive(Serialize)]
struct SttFailedLine<'a> {
    pod: &'a str,
    utterance_seq: u64,
    detail: String,
    elapsed_us: u64,
}

/// The `utterance` JSONL line: the full `Utterance` flattened in, plus the STT
/// attempt's own measured duration. `elapsed_us` belongs to the attempt, not to
/// the utterance type, so it rides on the line rather than in `StageTimings`
/// (which carries the completion *receipt* as `transcribed`; the two differ by
/// the completion-queue delay). `null` unless STT succeeded.
#[derive(Serialize)]
struct UtteranceLine<'a> {
    #[serde(flatten)]
    utterance: &'a Utterance,
    stt_elapsed_us: Option<u64>,
}

/// The `tracking` JSONL line: the full `TrackingEvent` flattened in, plus the
/// assembled→tracking latency delta.
#[derive(Serialize)]
struct TrackingLine<'a> {
    #[serde(flatten)]
    event: &'a TrackingEvent,
    assembled_to_tracking_us: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use futures::future::BoxFuture;
    use futures::stream::BoxStream;
    use serde_json::Value;
    use speech_pipeline::{
        CarveTiming, DropOldestQueue, EndpointState, EndpointTransition, InterruptProgress,
        ScoreSummary, SegmentEndCause, SegmentEndInfo, SpeakBody, StatsFlushCause, StatsModel,
        TranscriptConfidence, TranscriptEvent, TransitionCause, WakeConfirmation,
    };
    use std::sync::Mutex;

    use crate::config::JsonlSink;
    use crate::test_support::segment as build_segment;

    fn pod() -> PodId {
        PodId("pod-x".into())
    }

    /// A `pod-x` segment (id, `samples`-long ramp PCM) based at `base_sample_index`.
    fn seg_at(segment_id: u32, base: u64, samples: usize) -> Segment {
        let mut seg = build_segment(
            segment_id,
            samples,
            vec![],
            SegmentEndInfo::new(SegmentEndCause::VadRelease, false, 0, None),
        );
        seg.base_sample_index = base;
        seg.pcm = (0..samples).map(|i| i as i16).collect();
        seg
    }

    fn uid(seq: u64) -> ListenerUtteranceId {
        ListenerUtteranceId {
            pod: pod(),
            epoch: 1,
            seq,
        }
    }

    /// A carved utterance over `[start, end)` with the given id and optional wake.
    fn carved(seq: u64, start: u64, end: u64, wake: Option<WakeConfirmation>) -> CarvedUtterance {
        let len = (end - start) as usize;
        CarvedUtterance {
            utterance_id: uid(seq),
            pcm: Arc::from((0..len).map(|i| i as i16).collect::<Vec<_>>()),
            start_sample: start,
            end_sample: end,
            wake,
            stt_trim_samples: 0,
            cause: EndpointCause::SoftEndpoint,
            barge_in: false,
            timing: CarveTiming::default(),
        }
    }

    fn soft_endpoint(u: CarvedUtterance) -> PipelineItem {
        PipelineItem::Listener(ListenerEvent::SoftEndpoint {
            pod: pod(),
            utterance: u,
        })
    }

    /// A carve's host-receipt stamps as the listener would supply them: t0
    /// measured, the wake slightly before it (arm slack), the rest after.
    fn carve_timing() -> CarveTiming {
        CarveTiming {
            first_audio_rx: Some(HostMicros(1_000_000)),
            t0_projected: false,
            wake_detected_rx: Some(HostMicros(900_000)),
            onset_rx: Some(HostMicros(1_300_000)),
            soft_endpoint_rx: Some(HostMicros(2_381_000)),
            vad_high_est: Some(HostMicros(962_000)),
        }
    }

    fn carved_with_timing(seq: u64, timing: CarveTiming) -> CarvedUtterance {
        CarvedUtterance {
            timing,
            ..carved(seq, 0, 16, None)
        }
    }

    /// A segment on the same connection every other default-built item belongs to
    /// (`uid`'s epoch 1) — the shape every test that is not *about* reconnects wants.
    fn segment(seg: Segment) -> PipelineItem {
        segment_at_epoch(seg, 1)
    }

    fn segment_at_epoch(seg: Segment, epoch: u64) -> PipelineItem {
        PipelineItem::Segment { seg, epoch }
    }

    fn wake_detected(epoch: u64, wake_end_sample: u64) -> PipelineItem {
        PipelineItem::Listener(ListenerEvent::WakeDetected {
            pod: pod(),
            epoch,
            score: 0.9,
            wake_end_sample,
        })
    }

    struct EchoTestBrain;
    impl Brain for EchoTestBrain {
        fn handle(&self, u: Utterance, mut out: ResponseSink) -> BoxFuture<'static, ()> {
            let cmd = SpeakCmd {
                target: u.pod.clone(),
                in_reply_to: Some(u.id),
                body: SpeakBody::Text("ack".into()),
                interruptible: true,
                timings: u.timings.clone(),
            };
            let _ = out.try_send(cmd);
            futures::future::ready(()).boxed()
        }
        fn interrupt(&self, _id: UtteranceId, _progress: InterruptProgress) {}
    }

    struct FakeTranscriber(Option<(String, Option<TranscriptConfidence>)>);
    impl Transcriber for FakeTranscriber {
        fn transcribe(
            &self,
            _audio: SegmentAudio,
        ) -> BoxStream<'static, Result<TranscriptEvent, TranscribeError>> {
            match &self.0 {
                Some((text, confidence)) => {
                    let event = TranscriptEvent {
                        text: text.clone(),
                        is_final: true,
                        confidence: *confidence,
                    };
                    futures::stream::once(async move { Ok(event) }).boxed()
                }
                None => {
                    futures::stream::once(async { Err(TranscribeError::Connect("boom".into())) })
                        .boxed()
                }
            }
        }
    }

    fn conf(no_speech_prob: f32, avg_logprob: f32) -> TranscriptConfidence {
        TranscriptConfidence {
            avg_logprob,
            no_speech_prob,
            compression_ratio: 0.8,
            segments: 1,
        }
    }

    struct Harness {
        record_dir: Option<PathBuf>,
        transcriber: Option<Arc<dyn Transcriber>>,
        brain: bool,
        confidence_gate: ConfidenceGate,
        events: Arc<Mutex<Vec<BrainEvent>>>,
        stats: Arc<BrainStats>,
        barge: Option<(Arc<TurnLedger>, FlushFn)>,
    }

    impl Harness {
        fn new() -> Harness {
            Harness {
                record_dir: None,
                transcriber: None,
                brain: false,
                confidence_gate: ConfidenceGate::OFF,
                events: Arc::new(Mutex::new(Vec::new())),
                stats: Arc::new(BrainStats::default()),
                barge: None,
            }
        }
        /// Wire barge-in against `ledger` and a flush entry point that returns
        /// `flush` — the writer's answer, faked, so the pipeline's own ordering is
        /// what is under test rather than the writer's.
        fn barge(
            mut self,
            ledger: Arc<TurnLedger>,
            flush: Result<(UtteranceId, InterruptProgress), FlushRejected>,
        ) -> Harness {
            let f: FlushFn = Arc::new(move |_pod: &PodId| flush);
            self.barge = Some((ledger, f));
            self
        }
        fn transcriber(mut self, t: FakeTranscriber) -> Harness {
            self.transcriber = Some(Arc::new(t));
            self
        }
        fn brain(mut self) -> Harness {
            self.brain = true;
            self
        }
        fn gate(mut self, g: ConfidenceGate) -> Harness {
            self.confidence_gate = g;
            self
        }
        fn record(mut self, dir: PathBuf) -> Harness {
            self.record_dir = Some(dir);
            self
        }

        async fn run(self, items: Vec<PipelineItem>) -> (Vec<Value>, Vec<SpeakCmd>) {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("events.jsonl");
            let (jsonl, writer_join) = crate::jsonl::spawn_quiet(&JsonlSink::File(path.clone()))
                .await
                .unwrap();

            let (speak_tx, mut speak_rx) = fmpsc::channel::<SpeakCmd>(16);
            let brain = if self.brain {
                let sink = self.events.clone();
                let events: BrainEventFn = Arc::new(move |e| sink.lock().unwrap().push(e));
                Some(BrainWiring {
                    brain: Arc::new(EchoTestBrain),
                    speak_tx,
                    events,
                    stats: self.stats.clone(),
                })
            } else {
                None
            };

            let (tx, rx) = DropOldestQueue::<PipelineItem>::new(32);
            for item in items {
                tx.send(item);
            }
            drop(tx);

            run(
                rx,
                PipelineCtx {
                    record_dir: self.record_dir.clone(),
                    clock_step_clamps: Arc::new(AtomicU64::new(0)),
                    transcriber: self.transcriber.clone(),
                    brain,
                    confidence_gate: self.confidence_gate,
                    barge: self
                        .barge
                        .map(|(ledger, flush)| BargeWiring { ledger, flush }),
                },
                jsonl.clone(),
            )
            .await
            .unwrap();
            drop(jsonl);
            writer_join.await.unwrap();

            let lines = std::fs::read_to_string(&path)
                .unwrap()
                .lines()
                .map(|l| serde_json::from_str(l).unwrap())
                .collect();
            let mut cmds = Vec::new();
            while let Ok(cmd) = speak_rx.try_recv() {
                cmds.push(cmd);
            }
            (lines, cmds)
        }
    }

    fn events(lines: &[Value]) -> Vec<&str> {
        lines.iter().map(|v| v["event"].as_str().unwrap()).collect()
    }

    #[tokio::test]
    async fn segment_emits_tracking_only() {
        let (lines, _) = Harness::new().run(vec![segment(seg_at(1, 0, 16))]).await;
        assert_eq!(events(&lines), ["tracking"]);
    }

    /// The carve's stamps are the utterance's: every listener-domain field of the
    /// carve reaches `StageTimings` unaltered, which is what makes the latency
    /// summary's axis real rather than a `null` block.
    #[tokio::test]
    async fn carve_stamps_land_on_the_minted_utterances_timings() {
        let (lines, _) = Harness::new()
            .transcriber(FakeTranscriber(Some(("hi".into(), None))))
            .run(vec![soft_endpoint(carved_with_timing(1, carve_timing()))])
            .await;

        let t = &lines.iter().find(|v| v["event"] == "utterance").unwrap()["timings"];
        assert_eq!(t["first_audio_rx"], 1_000_000);
        assert_eq!(t["t0_projected"], false);
        assert_eq!(t["vad_high_est"], 962_000);
        assert_eq!(t["wake_detected_rx"], 900_000);
        assert_eq!(t["onset_rx"], 1_300_000);
        assert_eq!(t["soft_endpoint_rx"], 2_381_000);
    }

    /// `t0_projected` qualifies `first_audio_rx`, so it is present exactly when
    /// the stamp it describes is — never a bare `false` implying a measurement
    /// that never happened.
    #[tokio::test]
    async fn t0_provenance_rides_with_the_stamp_it_qualifies() {
        let projected = CarveTiming {
            t0_projected: true,
            ..carve_timing()
        };
        let no_t0 = CarveTiming {
            first_audio_rx: None,
            ..carve_timing()
        };
        for (timing, expect) in [(projected, Some(true)), (no_t0, None)] {
            let (lines, _) = Harness::new()
                .run(vec![soft_endpoint(carved_with_timing(1, timing))])
                .await;
            let t = &lines.iter().find(|v| v["event"] == "utterance").unwrap()["timings"];
            assert_eq!(t["t0_projected"].as_bool(), expect);
        }
    }

    /// `stt_started` is stamped in the pipeline task around the spawn, so it must
    /// sit between the instants bracketing the whole run — and after the carve's
    /// soft endpoint, which is the audio it followed.
    #[tokio::test]
    async fn stt_started_is_stamped_around_the_spawn() {
        let before = HostMicros::now();
        let (lines, _) = Harness::new()
            .transcriber(FakeTranscriber(Some(("hi".into(), None))))
            .run(vec![soft_endpoint(carved_with_timing(1, carve_timing()))])
            .await;
        let after = HostMicros::now();

        let t = &lines.iter().find(|v| v["event"] == "utterance").unwrap()["timings"];
        let stt_started = t["stt_started"].as_u64().expect("a stamped spawn");
        assert!(stt_started >= before.0 && stt_started <= after.0);
        // The receipt, not the completion: STT ran, so `transcribed` is later.
        assert!(t["transcribed"].as_u64().unwrap() >= stt_started);
    }

    /// With no transcriber wired, no STT starts — so no `stt_started` line claims
    /// one did. A daemon that announced `stt_absent` at startup must not then
    /// narrate inference it cannot run. The stamp is kept regardless: it measures
    /// the listener → pipeline hop, which happens either way.
    #[tokio::test]
    async fn no_stt_started_line_without_a_transcriber_but_the_stamp_stands() {
        let (lines, _) = Harness::new()
            .run(vec![soft_endpoint(carved_with_timing(1, carve_timing()))])
            .await;

        assert!(
            !lines.iter().any(|v| v["event"] == "stt_started"),
            "no transcriber, no STT to announce: {lines:?}"
        );
        let t = &lines.iter().find(|v| v["event"] == "utterance").unwrap()["timings"];
        assert!(
            t["stt_started"].as_u64().is_some(),
            "the spawn-hop stamp still lands on the utterance: {t}"
        );
    }

    /// The `stt_started` line marks the spawn on the console at the moment it
    /// happens, rather than leaving STT invisible until the `utterance` line
    /// seconds later.
    #[tokio::test]
    async fn stt_started_line_names_the_utterance_and_its_audio() {
        let (lines, _) = Harness::new()
            .transcriber(FakeTranscriber(Some(("hi".into(), None))))
            .run(vec![soft_endpoint(carved(4, 0, 16, None))])
            .await;

        let line = lines
            .iter()
            .find(|v| v["event"] == "stt_started")
            .expect("an stt_started line");
        assert_eq!(line["pod"], "pod-x");
        assert_eq!(line["utterance_seq"], 4);
        assert_eq!(line["samples"], 16);
        // It precedes the completion it announces.
        let names = events(&lines);
        let spawn = names.iter().position(|e| *e == "stt_started").unwrap();
        let done = names.iter().position(|e| *e == "utterance").unwrap();
        assert!(spawn < done);
    }

    /// The in-task STT measurement, which the `stt_failed` line reports for
    /// failures, lands on the `utterance` line for successes.
    #[tokio::test]
    async fn utterance_line_carries_stt_elapsed_us_on_success_only() {
        let (ok, _) = Harness::new()
            .transcriber(FakeTranscriber(Some(("hi".into(), None))))
            .run(vec![soft_endpoint(carved(1, 0, 16, None))])
            .await;
        let elapsed = ok.iter().find(|v| v["event"] == "utterance").unwrap()["stt_elapsed_us"]
            .as_u64()
            .expect("a measured success");
        // The completion receipt includes the queue delay the measurement excludes.
        let t = &ok.iter().find(|v| v["event"] == "utterance").unwrap()["timings"];
        assert!(elapsed <= t["transcribed"].as_u64().unwrap() - t["stt_started"].as_u64().unwrap());

        // A failure already reports its elapsed time on its own line, so the
        // utterance line leaves the field null rather than repeating it.
        let (failed, _) = Harness::new()
            .transcriber(FakeTranscriber(None))
            .run(vec![soft_endpoint(carved(1, 0, 16, None))])
            .await;
        assert!(
            failed.iter().find(|v| v["event"] == "utterance").unwrap()["stt_elapsed_us"].is_null()
        );
        assert!(
            failed.iter().find(|v| v["event"] == "stt_failed").unwrap()["elapsed_us"]
                .as_u64()
                .is_some()
        );

        // No transcriber wired: nothing was attempted, so there is nothing to time.
        let (none, _) = Harness::new()
            .run(vec![soft_endpoint(carved(1, 0, 16, None))])
            .await;
        assert!(
            none.iter().find(|v| v["event"] == "utterance").unwrap()["stt_elapsed_us"].is_null()
        );
    }

    /// Brain begin gets its own instant, emitted past the gate so the line marks a
    /// real dispatch.
    #[tokio::test]
    async fn brain_dispatched_line_marks_a_real_dispatch() {
        let (lines, cmds) = Harness::new()
            .transcriber(FakeTranscriber(Some(("hello".into(), None))))
            .brain()
            .run(vec![soft_endpoint(carved(1, 0, 16, None))])
            .await;

        assert_eq!(cmds.len(), 1);
        let line = lines
            .iter()
            .find(|v| v["event"] == "brain_dispatched")
            .expect("a brain_dispatched line");
        assert_eq!(line["pod"], "pod-x");
        assert_eq!(line["utterance"], 1);
    }

    /// A gate decline never reaches the brain, so it emits no `brain_dispatched`
    /// line — even though the timings stamp `brain_dispatched` before the gate
    /// runs. The stamp without a line is harmless: a decline reaches no playback,
    /// so no latency summary reads it.
    #[tokio::test]
    async fn a_gate_decline_emits_no_brain_dispatched_line() {
        let wake = WakeConfirmation {
            score: 0.9,
            wake_end_sample: 0,
            stt_trim_samples: 0,
        };
        let (lines, cmds) = Harness::new()
            .transcriber(FakeTranscriber(Some((
                "phantom".into(),
                Some(conf(0.37, -0.99)),
            ))))
            .brain()
            .gate(ConfidenceGate {
                no_speech_max: 0.2,
                avg_logprob_min: None,
            })
            .run(vec![soft_endpoint(carved(1, 0, 16, Some(wake)))])
            .await;

        assert!(cmds.is_empty(), "the gate declined");
        assert!(!events(&lines).contains(&"brain_dispatched"));
        let t = &lines.iter().find(|v| v["event"] == "utterance").unwrap()["timings"];
        assert!(t["brain_dispatched"].as_u64().is_some());
    }

    #[tokio::test]
    async fn soft_endpoint_transcribes_and_dispatches() {
        let (lines, cmds) = Harness::new()
            .transcriber(FakeTranscriber(Some(("hello world".into(), None))))
            .brain()
            .run(vec![soft_endpoint(carved(1, 0, 16, None))])
            .await;
        let utt = lines.iter().find(|v| v["event"] == "utterance").unwrap();
        assert_eq!(utt["transcript"]["text"], "hello world");
        assert_eq!(utt["endpoint_cause"], "soft_endpoint");
        assert_eq!(cmds.len(), 1, "the utterance reached the brain");
    }

    #[tokio::test]
    async fn superseded_aborts_the_in_flight_stt() {
        // A slow STT superseded before it completes never dispatches.
        struct SlowTranscriber;
        impl Transcriber for SlowTranscriber {
            fn transcribe(
                &self,
                _audio: SegmentAudio,
            ) -> BoxStream<'static, Result<TranscriptEvent, TranscribeError>> {
                futures::stream::once(async {
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    Ok(TranscriptEvent {
                        text: "late".into(),
                        is_final: true,
                        confidence: None,
                    })
                })
                .boxed()
            }
        }
        let mut h = Harness::new().brain();
        h.transcriber = Some(Arc::new(SlowTranscriber));
        let (lines, cmds) = h
            .run(vec![
                soft_endpoint(carved(1, 0, 16, None)),
                PipelineItem::Listener(ListenerEvent::Superseded {
                    pod: pod(),
                    utterance_id: uid(1),
                }),
            ])
            .await;
        assert!(
            lines.iter().all(|v| v["event"] != "utterance"),
            "a superseded utterance never mints"
        );
        assert!(cmds.is_empty(), "nothing dispatched");
    }

    /// A same-id follow-up soft endpoint (a continuation re-STT) aborts the first
    /// in-flight STT and dispatches only the second (longer) carve — the implicit
    /// supersede that makes an explicit `Superseded` a fast-path, not a correctness
    /// dependency.
    #[tokio::test]
    async fn same_id_soft_endpoint_supersedes_and_dispatches_second() {
        // Transcribes to the carved PCM length, so the two carves are
        // distinguishable ("16" vs "64").
        struct LenTranscriber;
        impl Transcriber for LenTranscriber {
            fn transcribe(
                &self,
                audio: SegmentAudio,
            ) -> BoxStream<'static, Result<TranscriptEvent, TranscribeError>> {
                let text = audio.pcm.len().to_string();
                futures::stream::once(async move {
                    Ok(TranscriptEvent {
                        text,
                        is_final: true,
                        confidence: None,
                    })
                })
                .boxed()
            }
        }
        let mut h = Harness::new().brain();
        h.transcriber = Some(Arc::new(LenTranscriber));
        let (lines, cmds) = h
            .run(vec![
                soft_endpoint(carved(1, 0, 16, None)),
                soft_endpoint(carved(1, 0, 64, None)), // same id, longer carve
            ])
            .await;
        let utts: Vec<_> = lines.iter().filter(|v| v["event"] == "utterance").collect();
        assert_eq!(
            utts.len(),
            1,
            "only the surviving utterance mints: {utts:?}"
        );
        assert_eq!(
            utts[0]["transcript"]["text"], "64",
            "the second (longer) carve's transcript dispatches"
        );
        assert_eq!(cmds.len(), 1);
    }

    /// A settled STT whose spawn nonce no longer matches the pod's in-flight slot
    /// (a superseded/respawned task that finished after its slot was replaced) is
    /// dropped by `handle_stt_done`, never dispatched. White-box: the pre-filled
    /// queue harness always aborts a stale task before it can deliver, so the
    /// delivered-then-dropped branch is exercised by driving the handler directly.
    #[tokio::test]
    async fn stale_nonce_stt_done_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let (jsonl, writer_join) = crate::jsonl::spawn_quiet(&JsonlSink::File(path.clone()))
            .await
            .unwrap();

        let mut pods: HashMap<PodId, PodState> = HashMap::new();
        let state = pods.entry(pod()).or_default();
        // The pod's live in-flight STT is nonce 2; a stale completion carries nonce 1.
        state.in_flight = Some(InFlight {
            id: uid(1),
            nonce: 2,
            abort: tokio::spawn(async {}).abort_handle(),
            stt_started: HostMicros::now(),
        });
        let mut next_id: u64 = 1;
        let done = SttDone {
            pod: pod(),
            nonce: 1,
            carve: Carve {
                id: uid(1),
                start_sample: 0,
                end_sample: 16,
                wake: None,
                cause: EndpointCause::SoftEndpoint,
                barge_in: false,
                timing: CarveTiming::default(),
            },
            result: Some(Ok(Transcript {
                text: "stale".into(),
                confidence: None,
            })),
            elapsed_us: 0,
        };
        handle_stt_done(
            done,
            &mut pods,
            &mut next_id,
            &ConfidenceGate::OFF,
            None,
            None,
            &jsonl,
        )
        .await;

        assert_eq!(next_id, 1, "no utterance minted for a stale completion");
        let slot = &pods[&pod()].in_flight;
        assert!(
            matches!(slot, Some(f) if f.nonce == 2),
            "the live in-flight slot is untouched",
        );
        drop(jsonl);
        writer_join.await.unwrap();
        let contents = std::fs::read_to_string(&path).unwrap();
        assert!(
            !contents.contains("\"utterance\""),
            "a stale completion never dispatches: {contents}"
        );
    }

    /// An equal-base reboot (the new connection's first segment re-bases at an index
    /// a prior connection's segment already occupied — indistinguishable by index)
    /// reads as a restart on the epoch alone: the old segment is dropped, so a new
    /// carve resolves only to the new segment's log, never stitching the pre-reboot
    /// segment's ref.
    #[tokio::test]
    async fn equal_base_reconnect_drops_stale_segment() {
        let mut old = seg_at(9, 0, 16);
        old.audio_ref.log = "old.framelog".into();
        let mut new = seg_at(9, 0, 16);
        new.audio_ref.log = "new.framelog".into();
        let (lines, _cmds) = Harness::new()
            .transcriber(FakeTranscriber(Some(("hi".into(), None))))
            .brain()
            .run(vec![
                segment_at_epoch(old, 1),
                wake_detected(1, 8),
                segment_at_epoch(new, 2),
                // `carved`'s epoch-1 id would be stale against the epoch-2 segment;
                // this carve belongs to the new connection.
                {
                    let mut u = carved(1, 0, 16, None);
                    u.utterance_id.epoch = 2;
                    soft_endpoint(u)
                },
            ])
            .await;
        let utt = lines.iter().find(|v| v["event"] == "utterance").unwrap();
        assert_eq!(
            utt["audio_ref"]["log"], "new.framelog",
            "the carve resolves to the post-reboot log, not the stale one"
        );
        let segs = utt["audio_ref"]["segments"].as_array().unwrap();
        assert_eq!(
            segs.len(),
            1,
            "only the post-reboot segment covers: {segs:?}"
        );
    }

    /// The preroll-overlap common case, which has every index signature of a reboot
    /// and is none: a segment re-sends the previous segment's tail under its original
    /// capture indexes, so it bases *behind* the prior segment's end, and the wake it
    /// re-scores off the re-anchored chunk grid can land a chunk *before* the first
    /// fire. Within one epoch none of that is a reconnect: the in-flight STT survives
    /// (the command is not silently lost) and the recent-segment tracking still covers
    /// the earlier segment.
    #[tokio::test]
    async fn preroll_overlap_double_fire_is_not_a_reconnect() {
        let mut first = seg_at(1, 100, 40);
        first.audio_ref.log = "live.framelog".into();
        // Opens 24 samples behind the first segment's end (140) — its preroll.
        let mut second = seg_at(2, 116, 40);
        second.audio_ref.log = "live.framelog".into();
        let (lines, cmds) = Harness::new()
            .transcriber(FakeTranscriber(Some(("lights on".into(), None))))
            .brain()
            .run(vec![
                segment(first),
                wake_detected(1, 130),
                // The duplicate fire off the shifted grid, 2 samples earlier.
                wake_detected(1, 128),
                segment(second),
                soft_endpoint(carved(1, 120, 150, None)),
            ])
            .await;
        assert_eq!(cmds.len(), 1, "the command survives the overlap boundary");
        let utt = lines.iter().find(|v| v["event"] == "utterance").unwrap();
        let segs = utt["audio_ref"]["segments"].as_array().unwrap();
        assert_eq!(
            segs.len(),
            2,
            "both segments still cover the carve — no tracking was wiped: {segs:?}"
        );
    }

    /// `build_audio_span` lists every covering segment, in base order, on a real
    /// dispatch — the replay join key. Two segments span the carve.
    #[tokio::test]
    async fn soft_endpoint_audio_ref_lists_covering_segments_in_order() {
        let (lines, _cmds) = Harness::new()
            .transcriber(FakeTranscriber(Some(("hi".into(), None))))
            .brain()
            .run(vec![
                segment(seg_at(1, 0, 16)),
                segment(seg_at(2, 16, 16)),
                soft_endpoint(carved(1, 8, 24, None)),
            ])
            .await;
        let utt = lines.iter().find(|v| v["event"] == "utterance").unwrap();
        assert_eq!(utt["audio_ref"]["start_sample"], 8);
        assert_eq!(utt["audio_ref"]["end_sample"], 24);
        let segs = utt["audio_ref"]["segments"].as_array().unwrap();
        assert_eq!(segs.len(), 2, "both covering segments listed: {segs:?}");
        assert_eq!(segs[0]["segment_id"], 1, "covering parts in base order");
        assert_eq!(segs[1]["segment_id"], 2);
    }

    /// No covering segment (`recent` empty): the fallback log name is the raw
    /// wire `pod_id` sanitized the same way the recorder sanitizes it for a
    /// real file, so the minted `AudioSpan.log` always passes
    /// `is_single_normal_component` and never fails `resolve_open` as
    /// `InvalidRef` for an honest "no audio recorded yet" span.
    #[test]
    fn build_audio_span_fallback_sanitizes_a_dirty_pod_id() {
        let dirty = PodId("../evil/pod".into());
        let span = build_audio_span(&VecDeque::new(), 0, 100, &dirty);
        assert_eq!(span.log, "___evil_pod.framelog");
        assert!(span.segments.is_empty());
    }

    #[tokio::test]
    async fn low_confidence_wake_is_declined_not_dispatched() {
        let h = Harness::new()
            .transcriber(FakeTranscriber(Some((
                "phantom".into(),
                Some(conf(0.37, -0.99)),
            ))))
            .brain()
            .gate(ConfidenceGate {
                no_speech_max: 0.2,
                avg_logprob_min: None,
            });
        let events_seen = h.events.clone();
        let stats = h.stats.clone();
        let wake = Some(WakeConfirmation {
            score: 0.9,
            wake_end_sample: 0,
            stt_trim_samples: 0,
        });
        let (_lines, cmds) = h.run(vec![soft_endpoint(carved(1, 0, 16, wake))]).await;
        assert!(cmds.is_empty(), "a gated hallucination never dispatches");
        assert_eq!(stats.snapshot().wake_command_absent, 1);
        assert_eq!(events_seen.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn stale_epoch_soft_endpoint_is_dropped() {
        // A newer-epoch utterance advances the pod epoch; a straggler from the old
        // epoch is dropped rather than dispatched.
        let mut old = carved(5, 0, 16, None);
        old.utterance_id.epoch = 1;
        let mut newer = carved(1, 0, 16, None);
        newer.utterance_id.epoch = 2;
        let (lines, cmds) = Harness::new()
            .transcriber(FakeTranscriber(Some(("hi".into(), None))))
            .brain()
            .run(vec![soft_endpoint(newer), soft_endpoint(old)])
            .await;
        let utts: Vec<_> = lines.iter().filter(|v| v["event"] == "utterance").collect();
        assert_eq!(utts.len(), 1, "only the current-epoch utterance mints");
        assert_eq!(cmds.len(), 1);
    }

    #[tokio::test]
    async fn stt_failure_still_mints_null_transcript() {
        let (lines, cmds) = Harness::new()
            .transcriber(FakeTranscriber(None))
            .brain()
            .run(vec![soft_endpoint(carved(1, 0, 16, None))])
            .await;
        assert!(lines.iter().any(|v| v["event"] == "stt_failed"));
        let utt = lines.iter().find(|v| v["event"] == "utterance").unwrap();
        assert!(utt["transcript"].is_null());
        assert_eq!(cmds.len(), 1, "dispatched despite STT failure");
    }

    #[tokio::test]
    async fn wake_detection_labels_segment_positive_then_upgrades_late() {
        // A segment arriving before its wake is provisionally negative; the late
        // WakeDetected landing in its span upgrades it to positive.
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().to_path_buf();
        let framelog = store.join("pod-x_0.framelog");
        let mut sc = crate::recorder::Sidecar::new("pod-x");
        sc.push(crate::recorder::SidecarSegment {
            segment_id: 7,
            part: 0,
            wake: WakeClass::Ungated,
            start_epoch_us: 1,
            end_epoch_us: 2,
            end_cause: SegmentEndCause::VadRelease,
            truncated: false,
            resumed: false,
            gap_count: 0,
            samples: 16,
        });
        sc.write_atomic(&sidecar_path(&framelog)).unwrap();

        let (_lines, _cmds) = Harness::new()
            .record(store.clone())
            .run(vec![segment(seg_at(7, 0, 16)), wake_detected(1, 8)])
            .await;
        let read = crate::recorder::Sidecar::read(&sidecar_path(&framelog)).unwrap();
        assert_eq!(read.segments[0].wake, WakeClass::Positive);
    }

    #[tokio::test]
    async fn arm_expired_emits_wake_command_absent() {
        // A "wake, no follow": the listener's arm expired with no command. The
        // pipeline mints the no-command accounting (never a dispatch) with the
        // arm-expiry reason.
        let h = Harness::new().brain();
        let events_seen = h.events.clone();
        let stats = h.stats.clone();
        let wake = WakeConfirmation {
            score: 0.8,
            wake_end_sample: 8_000,
            stt_trim_samples: 4_800,
        };
        let (_lines, cmds) = h
            .run(vec![PipelineItem::Listener(ListenerEvent::ArmExpired {
                pod: pod(),
                wake,
                start_sample: 0,
                end_sample: 16_000,
            })])
            .await;
        assert!(cmds.is_empty(), "a wake-no-follow dispatches nothing");
        assert_eq!(stats.snapshot().wake_command_absent, 1);
        let evs = events_seen.lock().unwrap();
        assert_eq!(evs.len(), 1, "one accounting event");
        assert!(
            matches!(
                &evs[0],
                BrainEvent::WakeCommandAbsent {
                    reason: WakeCommandReason::ArmExpired,
                    ..
                }
            ),
            "arm-expiry reason: {:?}",
            evs[0]
        );
    }

    #[tokio::test]
    async fn arm_expired_without_brain_still_logs() {
        // No brain wired — the tuning/replay setting: the brain-side accounting
        // sink does not exist, so no `BrainEvent` is minted, but the arm expiry is
        // still visible. The line precedes the brain gate for exactly this case.
        let (lines, cmds) = Harness::new()
            .run(vec![PipelineItem::Listener(ListenerEvent::ArmExpired {
                pod: pod(),
                wake: WakeConfirmation {
                    score: 0.8,
                    wake_end_sample: 0,
                    stt_trim_samples: 0,
                },
                start_sample: 0,
                end_sample: 16,
            })])
            .await;
        assert!(cmds.is_empty(), "a wake-no-follow dispatches nothing");
        assert_eq!(lines.len(), 1, "the arm expiry is traced: {lines:?}");
        assert_eq!(lines[0]["event"], "arm_expired");
        assert_eq!(lines[0]["score"], f64::from(0.8_f32));
        assert_eq!(lines[0]["start_sample"], 0);
        assert_eq!(lines[0]["end_sample"], 16);
    }

    #[tokio::test]
    async fn endpointer_transition_emits_a_line_and_nothing_else() {
        // Pure observability: a transition drives no dispatch and no per-pod state,
        // but every field the tuning rig reads reaches the line.
        let (lines, cmds) = Harness::new()
            .run(vec![PipelineItem::Listener(
                ListenerEvent::EndpointerTransition {
                    pod: pod(),
                    epoch: 3,
                    transition: EndpointTransition {
                        from: EndpointState::Speech,
                        to: EndpointState::SoftEndpointed,
                        cause: TransitionCause::SoftEndpoint,
                        sample_offset: 52_256_640,
                    },
                },
            )])
            .await;
        assert!(cmds.is_empty());
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert_eq!(lines[0]["event"], "endpointer_transition");
        assert_eq!(lines[0]["pod"], "pod-x");
        assert_eq!(lines[0]["epoch"], 3);
        assert_eq!(lines[0]["from"], "speech");
        assert_eq!(lines[0]["to"], "soft_endpointed");
        assert_eq!(lines[0]["cause"], "soft_endpoint");
        assert_eq!(lines[0]["sample_offset"], 52_256_640);
    }

    #[tokio::test]
    async fn model_stats_emits_a_line_and_nothing_else() {
        // Pure observability, like the transition above — but this is the line that
        // exists for the case the transition stream cannot describe, so every field
        // an investigation reads must reach it.
        let (lines, cmds) = Harness::new()
            .run(vec![PipelineItem::Listener(ListenerEvent::ModelStats {
                pod: pod(),
                epoch: 3,
                model: StatsModel::Silero,
                cause: StatsFlushCause::Periodic,
                summary: ScoreSummary {
                    first_chunk_end: 52_125_568,
                    last_chunk_end: 52_256_640,
                    chunks: 256,
                    min: 0.001,
                    max: 0.031,
                    mean: 0.004,
                    median: 0.002,
                },
            })])
            .await;
        assert!(cmds.is_empty(), "stats dispatch nothing");
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert_eq!(lines[0]["event"], "model_stats");
        assert_eq!(lines[0]["pod"], "pod-x");
        assert_eq!(lines[0]["epoch"], 3);
        assert_eq!(lines[0]["model"], "silero");
        assert_eq!(lines[0]["cause"], "periodic");
        assert_eq!(lines[0]["first_chunk_end"], 52_125_568_u64);
        assert_eq!(lines[0]["last_chunk_end"], 52_256_640_u64);
        assert_eq!(lines[0]["chunks"], 256);
        assert_eq!(lines[0]["min"], f64::from(0.001_f32));
        assert_eq!(lines[0]["max"], f64::from(0.031_f32));
        assert_eq!(lines[0]["mean"], f64::from(0.004_f32));
        assert_eq!(lines[0]["median"], f64::from(0.002_f32));
    }

    #[test]
    fn transition_line_merges_any_envelope_with_the_serialized_transition() {
        // The replay rig's envelope (log name, no pod/epoch) over the same builder:
        // both callers get the transition's fields from the type, so the tuning rig
        // and the daemon cannot drift apart.
        let line = event_line(
            json!({ "log": "frames.jsonl" }),
            &EndpointTransition {
                from: EndpointState::Idle,
                to: EndpointState::Speech,
                cause: TransitionCause::Onset,
                sample_offset: 4_096,
            },
        );
        assert_eq!(
            line,
            json!({
                "log": "frames.jsonl",
                "from": "idle",
                "to": "speech",
                "cause": "onset",
                "sample_offset": 4_096,
            })
        );
    }

    #[tokio::test]
    async fn superseded_and_closed_emit_their_lines() {
        // Both were consumed silently before: a supersede is correlatable by
        // utterance id (the transition line names no utterance), and a close is the
        // utterance's final boundary.
        let uid = ListenerUtteranceId {
            pod: pod(),
            epoch: 0,
            seq: 4,
        };
        let (lines, _cmds) = Harness::new()
            .run(vec![
                PipelineItem::Listener(ListenerEvent::Superseded {
                    pod: pod(),
                    utterance_id: uid.clone(),
                }),
                PipelineItem::Listener(ListenerEvent::UtteranceClosed {
                    pod: pod(),
                    utterance_id: uid,
                }),
            ])
            .await;
        let names: Vec<_> = lines.iter().map(|l| l["event"].clone()).collect();
        assert_eq!(names, ["utterance_superseded", "utterance_closed"]);
        for line in &lines {
            assert_eq!(line["pod"], "pod-x");
            assert_eq!(line["utterance_id"]["seq"], 4);
        }
    }

    #[tokio::test]
    async fn wake_upgrade_labels_the_matching_part() {
        // Two cap-rolled parts share segment_id 7 (part 0 and part 1). A wake
        // landing in part 1's span upgrades part 1's sidecar entry, not part 0's —
        // the `(segment_id, part)` key disambiguates them.
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().to_path_buf();
        let framelog = store.join("pod-x_0.framelog");
        let entry = |part: u16, cause| crate::recorder::SidecarSegment {
            segment_id: 7,
            part,
            wake: WakeClass::Ungated,
            start_epoch_us: 1,
            end_epoch_us: 2,
            end_cause: cause,
            truncated: matches!(cause, SegmentEndCause::HostCapped),
            resumed: part > 0,
            gap_count: 0,
            samples: 16,
        };
        let mut sc = crate::recorder::Sidecar::new("pod-x");
        sc.push(entry(0, SegmentEndCause::HostCapped));
        sc.push(entry(1, SegmentEndCause::VadRelease));
        sc.write_atomic(&sidecar_path(&framelog)).unwrap();

        // Part 1 is based at sample 16 (part 0 spanned [0, 16)); the wake lands at
        // sample 20, inside part 1.
        let mut part1 = seg_at(7, 16, 16);
        part1.audio_ref.part = 1;
        let (_lines, _cmds) = Harness::new()
            .record(store.clone())
            .run(vec![segment(part1), wake_detected(1, 20)])
            .await;
        let read = crate::recorder::Sidecar::read(&sidecar_path(&framelog)).unwrap();
        assert_eq!(
            read.segments[0].wake,
            WakeClass::Ungated,
            "part 0 untouched"
        );
        assert_eq!(read.segments[1].wake, WakeClass::Positive, "part 1 labeled");
    }

    /// The `BargeIn` event as the listener emits it for the default test pod.
    fn barge_in_event() -> PipelineItem {
        PipelineItem::Listener(ListenerEvent::BargeIn {
            pod: pod(),
            epoch: 1,
            trigger_sample: 4_800,
            host_rx: HostMicros(2_000_000),
        })
    }

    fn cut(heard_ms: u64, total_ms: u64) -> InterruptProgress {
        InterruptProgress { heard_ms, total_ms }
    }

    #[tokio::test]
    async fn a_barge_in_flushes_the_turn_then_marks_it_interrupted() {
        // The whole point of the ordering: by the time the ledger carries the mark,
        // the audio is already cut, so no `SpeakCmd` for the turn can slip out
        // behind the flush.
        let ledger = Arc::new(TurnLedger::new());
        ledger.record_dispatch(&pod(), UtteranceId(1), Some("what time is it".into()));
        ledger.record_cmd(&pod(), UtteranceId(1), Some("it is half past three".into()));

        let (lines, _) = Harness::new()
            .brain()
            .barge(Arc::clone(&ledger), Ok((UtteranceId(1), cut(400, 1_000))))
            .run(vec![barge_in_event()])
            .await;

        let trigger = lines
            .iter()
            .find(|v| v["event"] == "barge_in")
            .expect("the trigger's own line");
        assert_eq!(trigger["trigger_sample"], 4_800);

        let interrupted = lines
            .iter()
            .find(|v| v["event"] == "playback_interrupted")
            .expect("a playback_interrupted line");
        assert_eq!(interrupted["utterance"], 1);
        assert_eq!(interrupted["heard_ms"], 400);
        assert_eq!(interrupted["total_ms"], 1_000);

        assert!(ledger.is_interrupted(&pod(), Some(UtteranceId(1))));
        let chain = ledger.chain(&pod()).expect("the cut turn is chained");
        assert_eq!(chain.chain.len(), 1);
        assert_eq!(
            chain.chain[0].response_text.as_deref(),
            Some("it is half past three")
        );
        assert_eq!(chain.chain[0].interrupted.heard_ms, 400);
    }

    #[tokio::test]
    async fn a_rejected_flush_is_stale_and_touches_nothing() {
        // Playback ended between the trigger and here: there is nothing to cut, so
        // there is no turn to mark and no link to chain.
        let ledger = Arc::new(TurnLedger::new());
        ledger.record_dispatch(&pod(), UtteranceId(1), Some("hi".into()));

        let (lines, _) = Harness::new()
            .brain()
            .barge(Arc::clone(&ledger), Err(FlushRejected::NotPlaying))
            .run(vec![barge_in_event()])
            .await;

        let stale = lines
            .iter()
            .find(|v| v["event"] == "barge_in_stale")
            .expect("a barge_in_stale line");
        assert_eq!(stale["reason"], "not_playing");
        assert!(lines.iter().any(|v| v["event"] == "barge_in"));
        assert!(
            !lines.iter().any(|v| v["event"] == "playback_interrupted"),
            "a stale barge interrupts nothing: {lines:?}"
        );
        assert!(!ledger.is_interrupted(&pod(), Some(UtteranceId(1))));
        assert!(ledger.chain(&pod()).is_none());
    }

    #[tokio::test]
    async fn a_barge_in_with_no_wiring_is_log_only() {
        // The replay and tuning rigs have no playback path at all; detection must
        // still leave its trace, which is the thing they exist to tune.
        let (lines, _) = Harness::new().brain().run(vec![barge_in_event()]).await;

        assert!(lines.iter().any(|v| v["event"] == "barge_in"));
        assert!(!lines.iter().any(|v| v["event"] == "barge_in_stale"));
    }

    #[tokio::test]
    async fn a_barge_utterance_carries_the_chain_and_a_plain_one_does_not() {
        let ledger = Arc::new(TurnLedger::new());
        ledger.record_dispatch(&pod(), UtteranceId(1), Some("what time is it".into()));
        ledger.record_cmd(&pod(), UtteranceId(1), Some("it is half past three".into()));

        let barge_carve = CarvedUtterance {
            barge_in: true,
            ..carved(1, 0, 16, None)
        };
        let (lines, _) = Harness::new()
            .brain()
            .transcriber(FakeTranscriber(Some(("no, cancel that".into(), None))))
            .barge(Arc::clone(&ledger), Ok((UtteranceId(1), cut(400, 1_000))))
            .run(vec![barge_in_event(), soft_endpoint(barge_carve)])
            .await;

        let u = lines
            .iter()
            .find(|v| v["event"] == "utterance")
            .expect("the barging speech mints its own utterance");
        let chain = &u["barge_in"]["chain"];
        assert_eq!(chain[0]["utterance"], 1);
        assert_eq!(chain[0]["transcript"], "what time is it");
        assert_eq!(chain[0]["response_text"], "it is half past three");
        assert_eq!(chain[0]["interrupted"]["heard_ms"], 400);
        // The barge word is heard on its own terms: no wake, nothing trimmed.
        assert!(u["wake"].is_null());
    }

    #[tokio::test]
    async fn a_barge_utterance_whose_chain_is_empty_dispatches_plain() {
        // The previous turn completed cleanly, so the chain was dropped; a barge
        // that finds nothing to chain must not mint an empty one, or every consumer
        // would have to reason about a chain with no last segment.
        let ledger = Arc::new(TurnLedger::new());
        let barge_carve = CarvedUtterance {
            barge_in: true,
            ..carved(1, 0, 16, None)
        };
        let (lines, _) = Harness::new()
            .brain()
            .transcriber(FakeTranscriber(Some(("hello again".into(), None))))
            .barge(Arc::clone(&ledger), Err(FlushRejected::NotPlaying))
            .run(vec![barge_in_event(), soft_endpoint(barge_carve)])
            .await;

        let u = lines.iter().find(|v| v["event"] == "utterance").unwrap();
        assert!(
            u["barge_in"].is_null(),
            "an empty chain is left off entirely: {u:?}"
        );
    }

    #[tokio::test]
    async fn a_dispatch_records_the_turn_and_its_response_for_the_next_interrupt() {
        // The chain link the *next* barge would read is assembled by the dispatch
        // itself: the transcript from the pipeline, the response text from the tap.
        let ledger = Arc::new(TurnLedger::new());
        let (_, cmds) = Harness::new()
            .brain()
            .transcriber(FakeTranscriber(Some(("hello there".into(), None))))
            .barge(Arc::clone(&ledger), Err(FlushRejected::NotPlaying))
            .run(vec![soft_endpoint(carved(1, 0, 16, None))])
            .await;
        assert_eq!(cmds.len(), 1, "the echo brain replies once");

        // Nothing has settled the reply yet, so the turn is mid-flight and its
        // capture is live: interrupt it and read what was captured.
        let ctx = ledger.interrupt(&pod(), UtteranceId(1), cut(10, 20));
        assert_eq!(ctx.chain[0].transcript.as_deref(), Some("hello there"));
        assert_eq!(
            ctx.chain[0].response_text.as_deref(),
            Some("ack"),
            "the tap captured the brain's text reply"
        );
    }

    #[tokio::test]
    async fn a_turn_that_plays_out_clean_clears_the_chain() {
        // The full completion path through the real pipeline: dispatch, tap, the
        // brain returning, and the job settling — only all four together drop the
        // chain a previous barge left.
        let ledger = Arc::new(TurnLedger::new());
        ledger.interrupt(&pod(), UtteranceId(99), cut(100, 1_000));

        Harness::new()
            .brain()
            .transcriber(FakeTranscriber(Some(("hello there".into(), None))))
            .barge(Arc::clone(&ledger), Err(FlushRejected::NotPlaying))
            .run(vec![soft_endpoint(carved(1, 0, 16, None))])
            .await;

        assert!(
            ledger.chain(&pod()).is_some(),
            "the reply has not played yet"
        );
        ledger.settle_job(&pod(), Some(UtteranceId(1)), true);
        assert!(
            ledger.chain(&pod()).is_none(),
            "an output completed without barge-in drops every segment"
        );
    }

    #[tokio::test]
    async fn a_barge_utterance_that_trips_the_gate_is_declined_not_dispatched() {
        // The sustained speech that cut playback transcribed to hallucination. The
        // wake-keyed gate can't cover it (a barge has no wake), so the barge arm
        // declines it — the playback is already cut, so this is the honest outcome.
        let ledger = Arc::new(TurnLedger::new());
        let barge_carve = CarvedUtterance {
            barge_in: true,
            ..carved(1, 0, 16, None)
        };
        let h = Harness::new()
            .brain()
            .transcriber(FakeTranscriber(Some((
                "phantom".into(),
                Some(conf(0.37, -0.99)),
            ))))
            .barge(Arc::clone(&ledger), Err(FlushRejected::NotPlaying))
            .gate(ConfidenceGate {
                no_speech_max: 0.2,
                avg_logprob_min: None,
            });
        let events_seen = h.events.clone();
        let stats = h.stats.clone();
        let (_lines, cmds) = h.run(vec![soft_endpoint(barge_carve)]).await;

        assert!(
            cmds.is_empty(),
            "a gated barge hallucination never dispatches"
        );
        assert_eq!(stats.snapshot().barge_command_absent, 1);
        assert_eq!(stats.snapshot().wake_command_absent, 0);
        let evs = events_seen.lock().unwrap();
        assert_eq!(evs.len(), 1);
        assert!(
            matches!(evs[0], BrainEvent::BargeCommandAbsent { .. }),
            "the decline carries the barge mark: {:?}",
            evs[0]
        );
    }

    #[tokio::test]
    async fn a_confident_barge_utterance_still_dispatches() {
        // The same gate, but a confident transcript: a real barge command must pass
        // — the gate declines only the hallucinations.
        let ledger = Arc::new(TurnLedger::new());
        let barge_carve = CarvedUtterance {
            barge_in: true,
            ..carved(1, 0, 16, None)
        };
        let h = Harness::new()
            .brain()
            .transcriber(FakeTranscriber(Some((
                "no cancel that".into(),
                Some(conf(0.01, -0.15)),
            ))))
            .barge(Arc::clone(&ledger), Err(FlushRejected::NotPlaying))
            .gate(ConfidenceGate {
                no_speech_max: 0.2,
                avg_logprob_min: None,
            });
        let stats = h.stats.clone();
        let (_lines, cmds) = h.run(vec![soft_endpoint(barge_carve)]).await;

        assert_eq!(cmds.len(), 1, "a confident barge dispatches");
        assert_eq!(stats.snapshot().barge_command_absent, 0);
    }
}
