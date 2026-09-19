//! The pipeline task: the single consumer of the assembler→pipeline queue,
//! carrying assembled `Segment`s, the continuous listener's `ListenerEvent`s, and
//! each connection's own announcement of its room and frame log, all as
//! [`PipelineItem`]s.
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

use futures::FutureExt;
use futures::channel::mpsc as fmpsc;
use motion_proto::{MAX_SPEED, MAX_TIMEOUT_MS, MIN_SPEED, Play, PlayWindow, STOW_POSE};
use pod_ingest::{HostMicros, SegmentRef};
use serde::Serialize;
use serde_json::json;
use speech_pipeline::{
    AudioSpan, Brain, BrainEvent, BrainEventFn, BrainStats, CarveTiming, CarvedUtterance,
    ConfidenceGate, Cue, CueTap, DoaTrack, EndpointCause, Feed, FlushRejected, GateReject,
    InterruptProgress, ListenerEvent, ListenerUtteranceId, PodId, ResponseSink, RoomId, Segment,
    SegmentTelemetry, SpeakBody, SpeakCmd, StageTimings, TrackingEvent, TranscribeError,
    Transcriber, Transcript, Utterance, UtteranceId, WakeCommandReason, WakeConfirmation,
    stage_delta_us, tracking_event, transcribe_pcm,
};
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

use crate::barge::TurnLedger;
use crate::config::{CueLibrary, WakeWordInStt};
use crate::jsonl::JsonlHandle;
use crate::playback_router::FeedFn;
use crate::recorder::{
    WakeClass, WakeClassUpdate, sanitize_filename, set_wake_class, sidecar_path,
};
use crate::scripter::{MotionCue, Raise, ScriptHandle, ScriptInput};

/// The pipeline exited on an unrecoverable fault. The server renders this to a
/// `pipeline_fatal` JSONL line and a nonzero exit.
#[derive(Debug)]
pub struct PipelineFatal {
    pub detail: String,
}

/// One item on the pipeline queue: an assembled transport segment (tracking +
/// sidecar), a listener event (wake detection + carved-utterance lifecycle), or a
/// connection announcing the context its first utterances are carved under.
///
/// Every arm carries its connection `epoch` — the same per-pod connection sequence
/// the listener stamps its events with, so all three agree on which connection's
/// index space an item belongs to.
#[derive(Debug)]
pub enum PipelineItem {
    /// Boxed: a `Segment` carries its PCM and telemetry inline and is several
    /// times the size of every other arm, so an unboxed one would set the size
    /// of every item on the queue.
    Segment {
        seg: Box<Segment>,
        epoch: u64,
    },
    Listener(ListenerEvent),
    /// A pod's connection announced itself: the room the configuration resolved
    /// for it and the frame log its capture is being written to, both known at
    /// `Hello` and neither derivable from a segment until one closes.
    ///
    /// The first utterance of a connection is carved while the first segment is
    /// still open, so without this the span it mints has no room and no log to
    /// name. Control, not bulk, for the same reason a wake is: losing it costs
    /// the attribution of exactly those utterances.
    Connected {
        pod: PodId,
        epoch: u64,
        room: RoomId,
        log: String,
    },
}

impl PipelineItem {
    /// Whether this item is a control event — one whose loss is a lost wake or a
    /// lost utterance, so it rides the queue's reliable lane rather than the
    /// sheddable one. Segments are bulk data and are sheddable.
    ///
    /// The lane policy as data, for callers that route a `PipelineItem` whose
    /// variant they do not know statically.
    pub fn is_control(&self) -> bool {
        match self {
            PipelineItem::Listener(_) | PipelineItem::Connected { .. } => true,
            PipelineItem::Segment { .. } => false,
        }
    }

    /// The pod this item belongs to. Every arm carries one, and a caller that
    /// reports an item it did not construct needs it without matching.
    pub fn pod(&self) -> &PodId {
        use ListenerEvent::*;
        match self {
            PipelineItem::Segment { seg, .. } => &seg.pod,
            PipelineItem::Connected { pod, .. } => pod,
            PipelineItem::Listener(ev) => match ev {
                WakeDetected { pod, .. }
                | WakeMuted { pod, .. }
                | BargeIn { pod, .. }
                | SoftEndpoint { pod, .. }
                | Superseded { pod, .. }
                | UtteranceClosed { pod, .. }
                | WakeHeld { pod, .. }
                | ArmExpired { pod, .. }
                | EndpointerTransition { pod, .. }
                | ModelStats { pod, .. }
                | ListenOpened { pod, .. }
                | ListenRestored { pod, .. }
                | ListenHeard { pod, .. }
                | ListenExpired { pod, .. } => pod,
            },
        }
    }

    /// What a log line names this item: one word per variant, listener events
    /// included.
    ///
    /// Beside the variants rather than at the reporting site, so the exhaustiveness
    /// check bites where a variant is added and a new arm cannot be reported under
    /// an older one's name.
    pub fn kind(&self) -> &'static str {
        use ListenerEvent::*;
        match self {
            PipelineItem::Segment { .. } => "segment",
            PipelineItem::Connected { .. } => "connected",
            PipelineItem::Listener(ev) => match ev {
                WakeDetected { .. } => "wake_detected",
                WakeMuted { .. } => "wake_muted",
                BargeIn { .. } => "barge_in",
                SoftEndpoint { .. } => "soft_endpoint",
                Superseded { .. } => "superseded",
                UtteranceClosed { .. } => "utterance_closed",
                WakeHeld { .. } => "wake_held",
                ArmExpired { .. } => "arm_expired",
                EndpointerTransition { .. } => "endpointer_transition",
                ModelStats { .. } => "model_stats",
                ListenOpened { .. } => "listen_opened",
                ListenRestored { .. } => "listen_restored",
                ListenHeard { .. } => "listen_heard",
                ListenExpired { .. } => "listen_expired",
            },
        }
    }
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

/// How a `<listen/>` reply's capture window is opened: the listener feed and how
/// long the window runs, in samples.
///
/// One struct and not two fields because a window length with nothing to feed is
/// not a configuration — the two are wired together or not at all. Shared by
/// both openers so the window's length cannot depend on which of two races won.
pub(crate) struct ListenWiring {
    /// The same listener feed the playback fan-out drives the floor with.
    pub(crate) feed: FeedFn,
    /// How long the window stays open, in samples at the capture rate. Dated by
    /// the listener from its own cursor, so nothing here is a wall clock.
    pub(crate) window_samples: u64,
}

impl ListenWiring {
    /// Open the window on `pod`. The one place a `Feed::Listen` is built, so the
    /// two openers cannot come to disagree about its length.
    pub(crate) async fn open(&self, pod: PodId) {
        (self.feed)(
            pod,
            Feed::Listen {
                window_samples: self.window_samples,
            },
        )
        .await;
    }

    /// Report that the gate declined the candidate `id`: it became no turn. Said
    /// for every non-dispatch outcome and whatever the candidate's provenance,
    /// because whether a decline gives a capture window back is the listener's
    /// decision and not this task's — the pipeline knows what the gate did, the
    /// listener knows what the window was.
    pub(crate) async fn declined(&self, pod: PodId, id: ListenerUtteranceId) {
        (self.feed)(pod, Feed::CandidateDeclined { id }).await;
    }
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
    /// Whether the wake word is cut out of the clip before transcription.
    pub wake_word: WakeWordInStt,
    /// Barge-in wiring, or `None` in a pipeline with no playback path (the replay
    /// rigs), where a detected barge-in is a log line and nothing more.
    pub(crate) barge: Option<BargeWiring>,
    /// How a `<listen/>` reply opens its capture window, or `None` with no
    /// listener wired. One of the two openers lives here; the other is the
    /// playback fan-out, and whichever of the brain's return and the last clip's
    /// settle comes second is the one that fires.
    pub(crate) listen: Option<Arc<ListenWiring>>,
    /// Where the interaction's lifecycle points are reported for the head, or
    /// `None` when no presence channel is configured. Every tap is a
    /// non-blocking send; nothing in this task waits on it.
    pub(crate) scripter: Option<ScriptHandle>,
    /// The poses and motions a reply may name, or `None` when the deployment
    /// configured no library — in which case no reply can move the head, since
    /// nothing here could tell an offered name from an invented one, and every
    /// cue is refused with a line saying so.
    pub(crate) cues: Option<Arc<CueLibrary>>,
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
    /// This carve's speech was heard over the pod's own playback, whether or not
    /// it cut it. The gate below makes such a carve prove it is speech.
    over_playback: bool,
    /// This carve was heard inside an open capture window: the person kept talking
    /// after a reply that asked them to, with no wake word. May hold alongside
    /// `barge_in` when that speech went on to cut a reply; the gate reads the
    /// barge first.
    follow_up: bool,
    /// The listener's host-receipt stamps for this utterance's audio, from t0 to
    /// the carve. Copied onto the minted `Utterance`'s `StageTimings`.
    timing: CarveTiming,
    /// Where transcription actually started in the carved PCM, or `None` when no
    /// transcriber was wired. Rides back so the `utterance` line reports the clip
    /// STT heard alongside the boundary the listener computed.
    sent_from: Option<usize>,
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

/// Wall-clock fallback for a standing wake hold. The listener's own hold is in the
/// sample domain and only advances when audio arrives, so a quiet room can leave it
/// standing indefinitely; this is the head's deadline for treating the wake as
/// unanswered regardless.
struct HoldRelease {
    /// When the standing hold is treated as unanswered for the head.
    at: tokio::time::Instant,
    /// The hold's own deadline, carried so the release line joins its `wake_held`.
    deadline_sample: u64,
}

/// Wall-clock fallback for a restored capture window, the [`HoldRelease`] of the
/// microphone. The listener's window is in the sample domain too, and its deadline
/// falling in a silence no audio crosses is the ordinary quiet room rather than a
/// pathology: the cursor stops, the expiry is not observed, and the head that
/// speech inside the window held up has nothing to bring it down. This is the
/// head's own deadline for the window, and it moves only the head — the microphone
/// stays the listener's, whose `ListenExpired` still arrives when audio next does.
struct ListenRelease {
    /// When the window's grant is treated as over for the head.
    at: tokio::time::Instant,
    /// The window's deadline, carried so the release line joins its
    /// `listen_restored`.
    deadline_sample: u64,
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
    /// The room the current connection resolved to, from its `Connected` item.
    /// `None` until one lands — a pod whose audio arrived before its hello did,
    /// or a rig that feeds listener events with no connection behind them.
    ///
    /// TODO(pipeline-room-from-config): a pod's room is a pure function of the
    /// configuration and the pod id, so holding it per connection gives
    /// `unmapped` a second meaning ("no hello has landed yet") beside the one it
    /// had ("the `[pods]` table does not name this pod"), and no consumer can
    /// tell the two apart.
    room: Option<RoomId>,
    /// The frame log the current connection is capturing into, from the same
    /// item. Named whether or not recording is on, which is also what a closed
    /// segment's `SegmentRef.log` carries at a site that records nothing.
    log: Option<String>,
    /// Per-pod spawn nonce mint.
    spawn_seq: u64,
    /// The head's release for a wake hold standing on this pod, armed on
    /// `WakeHeld` and cancelled when the listener resolves the hold either way.
    hold_release: Option<HoldRelease>,
    /// The head's release for a capture window restored on this pod, armed on
    /// `ListenRestored` and cancelled by anything that says the window's ending
    /// is somebody else's — speech inside it, a mint, a wake hold, or the
    /// listener's own expiry.
    listen_release: Option<ListenRelease>,
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
            // The room and the log belonged to the connection that just went, and
            // the new one announces its own; keeping them would attribute a fresh
            // connection's first utterance to the old connection's log.
            self.room = None;
            self.log = None;
            // A reconnected pod's presence starts over with its next wake, and
            // the window the old connection was listening through went with it.
            self.hold_release = None;
            self.listen_release = None;
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
        // The earliest head release standing on any pod, of either kind.
        let next_release = pods
            .values()
            .flat_map(|s| {
                [
                    s.hold_release.as_ref().map(|h| h.at),
                    s.listen_release.as_ref().map(|l| l.at),
                ]
            })
            .flatten()
            .min();
        tokio::select! {
            () = sleep_until_opt(next_release), if !queue_closed => {
                release_due(&mut pods, ctx.scripter.as_ref(), &jsonl);
            }
            item = rx.recv(), if !queue_closed => match item {
                None => {
                    queue_closed = true;
                    // No more listener events can resolve a hold or a window, and
                    // the loop is now waiting on in-flight STT alone.
                    for state in pods.values_mut() {
                        state.hold_release = None;
                        state.listen_release = None;
                    }
                }
                Some(PipelineItem::Segment { seg, epoch }) => {
                    handle_segment(*seg, epoch, &mut pods, ctx.record_dir.as_deref(), &ctx.clock_step_clamps, &jsonl)
                        .await;
                }
                Some(PipelineItem::Connected { pod, epoch, room, log }) => {
                    handle_connected(pod, epoch, room, log, &mut pods, &jsonl);
                }
                Some(PipelineItem::Listener(ev)) => {
                    handle_listener(ev, &mut pods, &done_tx, &mut next_utterance_id, &ctx, &jsonl)
                        .await;
                }
            },
            Some(done) = done_rx.recv() => {
                handle_stt_done(done, &mut pods, &mut next_utterance_id, &ctx, &jsonl).await;
            }
        }
    }
    Ok(())
}

/// Sleep until `at`, or forever when there is none — a select arm with nothing to
/// wait for.
async fn sleep_until_opt(at: Option<tokio::time::Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Tell the head that every wait whose wall clock has run out is over: a hold
/// nothing answered, and a capture window whose deadline passed in a silence the
/// listener's own clock never crossed. Presence only: a release against a head
/// that is already down is a no-op.
fn release_due(
    pods: &mut HashMap<PodId, PodState>,
    scripter: Option<&ScriptHandle>,
    jsonl: &JsonlHandle,
) {
    let now = tokio::time::Instant::now();
    for (pod, state) in pods.iter_mut() {
        if let Some(release) = state.hold_release.take_if(|r| r.at <= now) {
            // Distinguishable in the records from a head that came down on
            // `arm_expired`, and joined to its `wake_held` by the deadline.
            jsonl.emit(
                "wake_hold_released",
                &json!({ "pod": pod.0, "deadline_sample": release.deadline_sample }),
            );
            if let Some(scripter) = scripter {
                scripter.send(ScriptInput::Unanswered(pod.clone()));
            }
        }
        if let Some(release) = state.listen_release.take_if(|r| r.at <= now) {
            // The restored window's deadline passed on the wall clock with no audio
            // arriving to carry the listener's cursor past it, and the head was
            // brought down for that reason without waiting for the listener. A
            // reader is entitled to conclude that this window's grant has ended —
            // speech is no longer heard without a wake word on its account, its
            // deadline being behind it — and that the head came down. What this
            // line does not say is whether a wake hold stands: a `wake_held` not
            // yet resolved by an `utterance`, an `arm_expired` or a
            // `wake_hold_released` is that hold's own grant and its own record.
            //
            // The listener's `listen_expired` for the same window follows when
            // audio next arrives, at a reply start, or at a reconnect, so the
            // window's own lines stay balanced; this one is the head's record,
            // not the window's. Distinguishable from a head that came down on
            // `listen_expired`, and joined to its `listen_restored` by the
            // deadline.
            jsonl.emit(
                "listen_released",
                &json!({ "pod": pod.0, "deadline_sample": release.deadline_sample }),
            );
            if let Some(scripter) = scripter {
                scripter.send(ScriptInput::ListenExpired(pod.clone()));
            }
        }
    }
}

/// Adopt a connection's room and frame log for the pod, so an utterance carved
/// before that connection's first segment closes still names both.
///
/// Under the same epoch check as everything else: a hello from a connection the
/// pipeline has already superseded describes an index space nothing live is in,
/// and its log would send a replay at the wrong audio.
///
/// A stale one gets a line of its own. The server sends a hello only after the
/// connection it supersedes has drained, so a hello arriving behind a later epoch
/// is that ordering broken — and the console would otherwise show only its
/// consequence, a first utterance reading `unmapped` for no stated reason.
fn handle_connected(
    pod: PodId,
    epoch: u64,
    room: RoomId,
    log: String,
    pods: &mut HashMap<PodId, PodState>,
    jsonl: &JsonlHandle,
) {
    let state = pods.entry(pod.clone()).or_default();
    if !state.adopt_epoch(epoch) {
        jsonl.emit(
            "connected_stale",
            &json!({
                "pod": pod.0,
                "epoch": epoch,
                "live_epoch": state.epoch,
            }),
        );
        return;
    }
    state.room = Some(room);
    state.log = Some(log);
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
async fn handle_listener(
    ev: ListenerEvent,
    pods: &mut HashMap<PodId, PodState>,
    done_tx: &mpsc::UnboundedSender<SttDone>,
    next_utterance_id: &mut u64,
    ctx: &PipelineCtx,
    jsonl: &JsonlHandle,
) {
    // Destructured: six optional handles, and bare `None`s in positional
    // arguments are easy to swap silently.
    let PipelineCtx {
        record_dir,
        transcriber,
        wake_word,
        brain,
        barge,
        scripter,
        ..
    } = ctx;
    let wake_word = *wake_word;
    let (record_dir, transcriber, brain, barge, scripter) = (
        record_dir.as_deref(),
        transcriber.as_ref(),
        brain.as_ref(),
        barge.as_ref(),
        scripter.as_ref(),
    );
    match ev {
        ListenerEvent::WakeMuted {
            pod,
            epoch,
            score,
            wake_end_sample,
        } => {
            // A line and nothing else: the detection was discarded in the
            // listener, so nothing here has an arm, a turn or a head to move.
            // It is the record that the phrase did fire while the pod was
            // muted — the reading behind both "the wake word did not work
            // during the reply" and "the echo still trips the detector".
            jsonl.emit(
                "wake_muted",
                &json!({ "pod": pod.0, "epoch": epoch, "score": score, "wake_end_sample": wake_end_sample }),
            );
        }
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
            // Past the epoch check, so a superseded connection's wake never nudges a
            // brain. Advisory and non-blocking by contract — a brain that pre-warms a
            // remote peer starts the round trip here, before the command has even been
            // spoken.
            if let Some(wiring) = brain {
                wiring.brain.wake(&pod);
            }
            // Past the epoch check for the same reason: a superseded
            // connection's wake is not an interaction, and it must not raise a
            // head.
            if let Some(scripter) = scripter {
                scripter.send(ScriptInput::Wake(pod.clone()));
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
            // Nothing is minted under a hold, so a carve on this pod is either the
            // hold consumed or a fresh wake's own: the wait the head is waiting out
            // is over either way. A mint also spends whatever window this pod had,
            // so the window's own wall-clock release is over with it — a decline
            // re-arms it from the restore.
            state.hold_release = None;
            state.listen_release = None;
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
            // What the transcriber will actually be handed, so the line names the
            // clip rather than the carve: `samples` stays the pre-trim carve length.
            let stt_trim_samples = utterance.wake.map_or(0, |w| w.stt_trim_samples);
            // A boundary past the carve is a listener bug and cannot happen from a
            // wake arm; `stt_sent_from` clamps rather than panicking, and this is
            // the line that keeps the clamp from being silent when it does.
            if stt_trim_samples > utterance.pcm.len() {
                jsonl.emit(
                    "stt_trim_out_of_range",
                    &json!({
                        "pod": pod.0,
                        "utterance_seq": uid.seq,
                        "stt_trim_samples": stt_trim_samples,
                        "samples": utterance.pcm.len(),
                    }),
                );
            }
            let sent_from = transcriber
                .is_some()
                .then(|| stt_sent_from(wake_word, stt_trim_samples, utterance.pcm.len()));
            if let Some(sent_from) = sent_from {
                jsonl.emit(
                    "stt_started",
                    &json!({
                        "pod": pod.0,
                        "utterance_seq": uid.seq,
                        "samples": utterance.pcm.len(),
                        "sent_from_sample": sent_from,
                    }),
                );
            }
            let stt_started = HostMicros::now();
            let abort = spawn_stt(
                pod.clone(),
                nonce,
                utterance,
                transcriber.cloned(),
                sent_from,
                done_tx,
            );
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
            cause,
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
                    "cause": cause,
                    "trigger_sample": trigger_sample,
                    "host_rx_us": host_rx.0,
                }),
            );
            // Ahead of the playback wiring, like the line above: somebody spoke
            // over the pod, which is a live interaction whether or not there is
            // anything left to cut.
            if let Some(scripter) = scripter {
                scripter.send(ScriptInput::Barge(pod.clone()));
            }
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
        ListenerEvent::ListenOpened {
            pod,
            epoch,
            deadline_sample,
        } => {
            jsonl.emit(
                "listen_opened",
                &json!({ "pod": pod.0, "epoch": epoch, "deadline_sample": deadline_sample }),
            );
        }
        ListenerEvent::ListenRestored {
            pod,
            epoch,
            deadline_sample,
            at_sample,
        } => {
            // The line first and ungated, like the open it repeats: the window is
            // the listener's own state, a reader tracking the microphone reads this
            // exactly as it reads `listen_opened`, and dropping a superseded
            // connection's line would leave a window that did come back unrecorded.
            jsonl.emit(
                "listen_restored",
                &json!({
                    "pod": pod.0,
                    "epoch": epoch,
                    "deadline_sample": deadline_sample,
                    "at_sample": at_sample,
                }),
            );
            let state = pods.entry(pod.clone()).or_default();
            if !state.adopt_epoch(epoch) {
                return; // Stale: a reconnect superseded this epoch.
            }
            // The head's wall-clock deadline for what is left of the window. Armed
            // under the hold's rules and for the hold's reasons: with no scripter
            // there is no head to bring down and a timer over a replay faster than
            // real time would be a fiction, and the cursor lags the wall clock by
            // the hangover and transport latency, so this instant lands after the
            // listener's own deadline — where audio keeps arriving the listener's
            // `ListenExpired` gets there first and cancels it.
            //
            // Nothing is armed with no time left: the listener ran its own expiry
            // check at this restore and kept the window, so the room is not idle —
            // audio is arriving, its clock is live, and a zero-length arm would
            // stow the head now over an onset run or a resumed answer the carve
            // will accept. Nothing is armed under a standing hold either: the head
            // is on the hold's schedule, which has an ending of its own.
            if scripter.is_some() && state.hold_release.is_none() && deadline_sample > at_sample {
                let wait = Duration::from_millis(
                    (deadline_sample - at_sample) / crate::config::SAMPLES_PER_MS,
                );
                state.listen_release = Some(ListenRelease {
                    at: tokio::time::Instant::now() + wait,
                    deadline_sample,
                });
            }
        }
        ListenerEvent::ListenHeard { pod, epoch } => {
            jsonl.emit("listen_heard", &json!({ "pod": pod.0, "epoch": epoch }));
            let state = pods.entry(pod.clone()).or_default();
            if !state.adopt_epoch(epoch) {
                return; // Stale: a reconnect superseded this epoch.
            }
            // Speech is running inside the window, so the sample domain is live and
            // will produce a mint or an expiry of its own: the head's wall-clock
            // fallback for a room that went quiet has nothing to answer for.
            state.listen_release = None;
            // Past the epoch check, with the wake and the carve: this moves the
            // head, and a superseded connection's speech is not an interaction.
            //
            // The microphone is busy, so the head waits where the reply left it
            // until the window itself ends. Without this the ending dated from
            // that reply runs out mid-follow-up and the head starts down while
            // the person is still talking.
            if let Some(scripter) = scripter {
                scripter.send(ScriptInput::Heard(pod.clone()));
            }
        }
        ListenerEvent::ListenExpired { pod, epoch } => {
            jsonl.emit("listen_expired", &json!({ "pod": pod.0, "epoch": epoch }));
            let state = pods.entry(pod.clone()).or_default();
            if !state.adopt_epoch(epoch) {
                return; // Stale: a reconnect superseded this epoch.
            }
            // The listener said the window is over, so the head's own fallback for
            // saying it has nothing left to say.
            state.listen_release = None;
            // Past the epoch check for the same reason as the heard above: this
            // stows a head. A reconnect's own expiry is emitted under the epoch
            // it is closing, before the listener adopts the new one, so it is
            // current here and the head comes down with the connection.
            //
            // The window is what held the head up after a listening reply, so
            // its end is what brings the head down. A turn in flight overrides
            // that, which the scripter decides from the facts it already holds.
            if let Some(scripter) = scripter {
                scripter.send(ScriptInput::ListenExpired(pod.clone()));
            }
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
        ListenerEvent::WakeHeld {
            pod,
            epoch,
            start_sample,
            end_sample,
            wake_end_sample,
            deadline_sample,
        } => {
            // The wake word arrived without its command yet and the listener is
            // waiting. Nothing is dispatched and no brain hears of it — the turn is
            // still open, and resolves as a published utterance or as `arm_expired`.
            jsonl.emit(
                "wake_held",
                &json!({
                    "pod": pod.0,
                    "start_sample": start_sample,
                    "end_sample": end_sample,
                    "wake_end_sample": wake_end_sample,
                    "deadline_sample": deadline_sample,
                }),
            );
            let state = pods.entry(pod.clone()).or_default();
            if !state.adopt_epoch(epoch) {
                return; // Stale: a reconnect superseded this epoch.
            }
            // A hold now stands and owns the head's ending — the mirror of the
            // window release's own arm rule, which stands down under a hold.
            state.listen_release = None;
            // Past the epoch check because the arm touches per-pod state and the
            // `Unanswered` it eventually sends is not a no-op against a live turn.
            // With no scripter wired (replay, brainless tuning) nothing is armed:
            // there is no head to release, and a wall-clock timer over a replay that
            // runs faster than real time would be a fiction.
            if scripter.is_some() {
                let wait = Duration::from_millis(
                    deadline_sample.saturating_sub(end_sample) / crate::config::SAMPLES_PER_MS,
                );
                // Receipt lands a soft hangover plus transport latency after
                // `end_sample`, so this instant is later than the listener's own
                // deadline: where audio keeps arriving, `ArmExpired` fires first and
                // cancels it. A refreshed hold overwrites the entry with its later
                // deadline.
                state.hold_release = Some(HoldRelease {
                    at: tokio::time::Instant::now() + wait,
                    deadline_sample,
                });
            }
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
            // The hold resolved on the listener's clock, so the head's wall-clock
            // release is not needed: the `Unanswered` below is the one it gets.
            if let Some(state) = pods.get_mut(&pod) {
                state.hold_release = None;
            }
            // The usual false-positive-wake path: the head goes back down a
            // linger after this, and it does so with or without a brain wired.
            if let Some(scripter) = scripter {
                scripter.send(ScriptInput::Unanswered(pod.clone()));
            }
            // "Wake, no follow": the wake fired but no command followed. Accounted
            // for through the same `WakeCommandAbsent` vocabulary as an empty or
            // low-confidence command — only meaningful with a brain wired (the
            // event sink + counter), the same as the confidence-gate decline. STT
            // never ran, so there is no transcript to attach.
            let Some(wiring) = brain else {
                return;
            };
            let state = pods.entry(pod.clone()).or_default();
            let audio_ref = build_audio_span(state, start_sample, end_sample, &pod);
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

/// Where transcription starts inside a carved utterance: the listener's
/// wake-trim boundary under `Trim`, the head of the carve under `Keep`. The
/// boundary is carve-relative and must not exceed the carved PCM; clamp
/// defensively — an empty tail beats a panic that would take the whole pipeline
/// loop down over one utterance. Callers must report a boundary past the carve on
/// their own line; otherwise the clamp is silent.
fn stt_sent_from(mode: WakeWordInStt, stt_trim_samples: usize, pcm_len: usize) -> usize {
    match mode {
        WakeWordInStt::Keep => 0,
        WakeWordInStt::Trim => stt_trim_samples.min(pcm_len),
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
    sent_from: Option<usize>,
    done_tx: &mpsc::UnboundedSender<SttDone>,
) -> AbortHandle {
    let CarvedUtterance {
        utterance_id,
        pcm,
        start_sample,
        end_sample,
        wake,
        cause,
        barge_in,
        over_playback,
        follow_up,
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
        over_playback,
        follow_up,
        timing,
        sent_from,
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
                Some(t) => Some(transcribe_pcm(t.as_ref(), &pcm[sent_from.unwrap_or(0)..]).await),
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
#[allow(clippy::too_many_arguments)]
async fn handle_stt_done(
    done: SttDone,
    pods: &mut HashMap<PodId, PodState>,
    next_utterance_id: &mut u64,
    ctx: &PipelineCtx,
    jsonl: &JsonlHandle,
) {
    // The wiring this dispatch reaches for, named once: five of the ctx's
    // optional handles are used here, and threading them in one by one is how a
    // call site becomes a row of bare `None`s that an argument-order mistake
    // typechecks straight through.
    let PipelineCtx {
        confidence_gate,
        brain,
        barge,
        listen,
        scripter,
        cues,
        ..
    } = ctx;
    let (brain, barge, listen, scripter, cues) = (
        brain.as_ref(),
        barge.as_ref(),
        listen.as_ref(),
        scripter.as_ref(),
        cues.as_ref(),
    );
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
        state,
        done.carve.start_sample,
        done.carve.end_sample,
        &done.pod,
    );
    let (room, doa) = span_context(state, &done.carve);

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
        over_playback: done.carve.over_playback,
    };
    jsonl.emit(
        "utterance",
        &UtteranceLine {
            utterance: &utterance,
            stt_elapsed_us,
            stt_trim_samples: utterance.wake.map(|w| w.stt_trim_samples),
            stt_sent_from_sample: done.carve.sent_from,
            follow_up: done.carve.follow_up,
        },
    );

    let Some(wiring) = brain else {
        return;
    };
    // The gate, in two tests. First and unconditionally: an utterance with no
    // usable text is no turn. That is a fact about the speech, not about any
    // brain — a bypassed gate hearing the room, a wake word with nothing behind
    // it, an STT attempt that failed — so it is decided here, once, before a
    // brain is reached.
    //
    // Then the STT-confidence gate: a trigger whose text trips it is a likely
    // hallucination — declined as a no-command outcome, never echoed. Fail-open
    // on a missing summary; an utterance with no wake, no barge and no overlap
    // with the pod's own voice is never gated. Past the emptiness test every
    // transcript here carries text, so a score attached to an empty one can no
    // longer gate anything.
    //
    // The carve's provenance is classified once, above both tests, and both
    // declines read that one classification — a carve can carry more than one
    // mark, and two readings of it would report the same speech two ways.
    let from = provenance(&done.carve);
    let gate = if utterance.spoken_text().is_none() {
        GateOutcome::DeclineEmpty(from)
    } else {
        let confidence_reject = utterance
            .transcript
            .as_ref()
            .and_then(|t| t.confidence.as_ref())
            .and_then(|conf| confidence_gate.evaluate(conf));
        // Every declining arm binds the reject, which is what keeps the arm order
        // immaterial: an arm matching `..` in that position would decline a
        // provenance whose transcript passed the moment the dispatch arm moved
        // below it.
        match (from, confidence_reject) {
            (_, None) => GateOutcome::Dispatch,
            (Provenance::Bypassed, Some(_)) => GateOutcome::Dispatch,
            (Provenance::Wake(wake), Some(reject)) => GateOutcome::DeclineWake(wake, reject),
            (Provenance::Barge, Some(reject)) => GateOutcome::DeclineBarge(reject),
            (Provenance::FollowUp, Some(reject)) => GateOutcome::DeclineFollowUp(reject),
            (Provenance::OverPlayback, Some(reject)) => GateOutcome::DeclineEcho(reject),
        }
    };
    // Whether the head starts its settle is one read of the one classification.
    // A declined wake or barge is a raise that produced no turn: the head is up
    // and nothing will follow, so the settle starts here rather than waiting for
    // the engagement's ceiling. A carve in a quiet room that said nothing is the
    // same: under a bypassed wake gate nothing else will end the engagement, so
    // the settle starts here too. A declined follow-up is not — its capture
    // window is handed back below and owns the head until it expires, and folding
    // a linger from here would date the head's ending off noise instead of off
    // the window. A decline over the pod's own voice is not a raise either:
    // nobody raised, and `Unanswered` clears the pod's current turn, which would
    // cut short the script of the very reply the echo came from.
    //
    // Both declines answer this off the value they were classified under, so no
    // decline's head response can drift from its report.
    let starts_the_settle = !matches!(gate, GateOutcome::Dispatch)
        && matches!(
            from,
            Provenance::Wake(_) | Provenance::Barge | Provenance::Bypassed
        );
    if let Some(scripter) = scripter
        && starts_the_settle
    {
        scripter.send(ScriptInput::Unanswered(utterance.pod.clone()));
    }
    // Every decline, uniformly and whatever its reason: this candidate produced no
    // turn. Said before the accounting below so a capture window the candidate spent
    // is back as early as the verdict allows — the person answering a `<listen/>`
    // reply is talking into the gap this closes.
    if let Some(listen) = listen
        && !matches!(gate, GateOutcome::Dispatch)
    {
        listen
            .declined(utterance.pod.clone(), done.carve.id.clone())
            .await;
    }
    match gate {
        GateOutcome::DeclineEmpty(from) => match from {
            // A scored wake accept with nothing behind it. Its own non-failure
            // category, carrying the wake score and the segment reference so a
            // follow-up tool can re-fetch the audio for retro-transcription.
            Provenance::Wake(wake) => {
                decline_wake_command(&utterance, &wake, WakeCommandReason::Empty, wiring)
            }
            Provenance::Barge => {
                decline_no_transcript(&utterance, wiring);
                // The playback is already cut and `handle` will never run for this
                // utterance, so this is the brain's only chance to hear that its
                // response was interrupted with nothing usable said in its place.
                // Non-blocking by contract, like `interrupt`.
                wiring.brain.barge_declined(&utterance);
            }
            Provenance::FollowUp | Provenance::OverPlayback | Provenance::Bypassed => {
                decline_no_transcript(&utterance, wiring)
            }
        },
        GateOutcome::DeclineWake(wake, reject) => decline_wake_command(
            &utterance,
            &wake,
            WakeCommandReason::LowConfidence {
                no_speech_prob: reject.no_speech_prob,
                avg_logprob: reject.avg_logprob,
            },
            wiring,
        ),
        GateOutcome::DeclineBarge(reject) => {
            decline_barge_low_confidence(&utterance, reject, false, wiring);
            // The playback is already cut and `handle` will never run for this
            // utterance, so this is the brain's only chance to hear that its response
            // was interrupted with nothing usable said in its place. Non-blocking by
            // contract, like `interrupt`.
            wiring.brain.barge_declined(&utterance);
        }
        GateOutcome::DeclineFollowUp(reject) => {
            // No `barge_declined`: nothing was interrupted. That is the
            // classification's guarantee rather than a fact about windows —
            // speech that did cut a reply is a barge above, whatever window it
            // began in — so what tripped the gate here is the room. A reader
            // tuning barge thresholds off this count needs to know these are not
            // barges.
            decline_barge_low_confidence(&utterance, reject, true, wiring);
        }
        GateOutcome::DeclineEcho(reject) => decline_echo(&utterance, reject, wiring),
        GateOutcome::Dispatch => {
            // Brain begin gets its own console instant. Emitted here, past the
            // gate, so the line marks a real dispatch — unlike `brain_dispatched`
            // in the timings, which is stamped before the gate decides.
            jsonl.emit(
                "brain_dispatched",
                &json!({ "pod": utterance.pod.0, "utterance": utterance.id }),
            );
            let (pod, id) = (utterance.pod.clone(), utterance.id);
            // The movements this turn's reply may ask for. Always wired, even
            // where nothing could carry them out: a reply that asks a
            // deployment with no vocabulary or no head to move is a
            // misconfiguration somewhere, and the tap is what says so.
            let cue_tap = Some(cue_tap(
                cues.map(Arc::clone),
                scripter.cloned(),
                jsonl.clone(),
                pod.clone(),
                id,
            ));
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
                    ResponseSink::with_taps(
                        wiring.speak_tx.clone(),
                        Some(Arc::new(move |cmd: &SpeakCmd| {
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
                        })),
                        cue_tap,
                    )
                }
                None => ResponseSink::with_taps(wiring.speak_tx.clone(), None, cue_tap),
            };
            // Around the await, not inside the barge arm below: a turn is in
            // flight for as long as the brain has it, and that is what keeps
            // the head up through a long think in a pipeline with no playback
            // path at all. This is also the turn every later fact names.
            if let Some(scripter) = scripter {
                scripter.send(ScriptInput::TurnStarted {
                    pod: pod.clone(),
                    turn: id,
                });
            }
            let end = wiring.brain.handle(utterance, sink).await;
            if let Some(scripter) = scripter {
                scripter.send(ScriptInput::TurnEnded {
                    pod: pod.clone(),
                    turn: id,
                    end,
                });
            }
            if let Some(barge) = barge {
                // Dispatch awaits the brain inline, so returning here is the proof
                // that no further command is coming for this turn — which is what
                // lets its settlement complete, and one of the three facts the
                // head's ending is scheduled from.
                let audio = barge.ledger.dispatch_done(&pod, id, end);
                if let Some(scripter) = scripter {
                    scripter.send(ScriptInput::Audio {
                        pod: pod.clone(),
                        turn: id,
                        audio,
                    });
                }
                // The opener for a reply whose last clip was already heard out
                // when the brain returned — a chain whose final segment carries
                // no speech at all settles before this call. The fan-out is the
                // opener for the ordinary case; `listen_open` is true on exactly
                // one of the two, so the window opens once.
                if let Some(listen) = listen
                    && audio.listen_open
                {
                    listen.open(pod.clone()).await;
                }
            }
        }
    }
}

/// Resolve one movement a reply named against the deployed library, or say why
/// it cannot be made.
///
/// Every name must resolve before it reaches the wire: an unresolvable name in
/// a script costs every other movement in that script, because the daemon
/// refuses the script whole. The speed range is checked here too, because this
/// is where the number is turned into the absolute pace the wire carries.
///
/// The returned reason is what the refusal line reports: short, stable, and
/// about the cue rather than about the reply.
fn resolve_cue(library: &CueLibrary, cue: &Cue) -> Result<MotionCue, &'static str> {
    let (name, speed) = match cue {
        Cue::Pose { name, speed } | Cue::Motion { name, speed } => (name.as_str(), *speed),
    };
    if let Some(speed) = speed
        && !(speed.is_finite() && (MIN_SPEED..=MAX_SPEED).contains(&speed))
    {
        return Err("speed_out_of_range");
    }
    match cue {
        Cue::Pose { .. } => {
            // Rest is not a pose a reply gets to command: the stow is the
            // ending the fault ladder and the script compiler both treat
            // structurally, and a reply that wants the head down simply stops
            // asking it to stay up.
            if name == STOW_POSE {
                return Err("stow_not_cueable");
            }
            let (pose, duration_ms) = library.pose(name).ok_or("unknown_pose")?;
            // Silence is the library's own pace, so a cue at unit speed states
            // no pace at all — the same thing a presence raise does.
            let move_ms = match speed {
                None => None,
                Some(speed) if (speed - 1.0).abs() < f64::EPSILON => None,
                Some(speed) => {
                    #[expect(
                        clippy::cast_precision_loss,
                        clippy::cast_possible_truncation,
                        clippy::cast_sign_loss,
                        reason = "a duration in milliseconds, divided by a speed in 0.25..=2.0"
                    )]
                    let move_ms = (duration_ms as f64 / speed).ceil() as u64;
                    // The wire's own ceiling, checked where the number is made.
                    // Refused rather than clamped: a pace nobody asked for is
                    // not a repair.
                    if move_ms > MAX_TIMEOUT_MS {
                        return Err("move_too_long");
                    }
                    Some(move_ms)
                }
            };
            Ok(MotionCue::Pose(Raise { pose, move_ms }))
        }
        Cue::Motion { .. } => {
            let (motion, span) = library.motion(name).ok_or("unknown_motion")?;
            let play = match speed {
                Some(speed) => Play::at_speed(motion.as_ref(), speed),
                None => Play::new(motion.as_ref()),
            };
            let span_ms = PlayWindow {
                duration_ms: span.duration_ms,
                blend_out_ms: span.blend_out_ms,
            }
            .span_ms(play.speed);
            // The wire's own ceiling again, for the same reason as the pose's
            // pace: the span becomes the timeout of every script that restates
            // this play, and a timeout past the bound is a script the wire
            // refuses to build at all. Checked where the number is made, and
            // with the one millisecond the render puts the play's step at.
            if span_ms.saturating_add(1) > MAX_TIMEOUT_MS {
                return Err("motion_too_long");
            }
            Ok(MotionCue::Motion { play, span_ms })
        }
    }
}

/// Write the line that says a cue asked for did not happen: which movement, and
/// why. The one place a refusal is narrated, so every reason reads the same.
fn emit_cue_refused(jsonl: &JsonlHandle, pod: &PodId, turn: UtteranceId, cue: &Cue, reason: &str) {
    let (kind, name) = match cue {
        Cue::Pose { name, .. } => ("pose", name),
        Cue::Motion { name, .. } => ("motion", name),
    };
    jsonl.emit(
        "cue_refused",
        &json!({
            "pod": pod.0,
            "utterance": turn,
            "kind": kind,
            "name": name,
            "reason": reason,
        }),
    );
}

/// The tap the brain hands each response message's movements to: resolve them,
/// say which were refused, and send the rest to the head as one input.
///
/// One input per message and not one per cue, because the movements of one
/// reply are one decision — the last pose and the last motion in it are what
/// the head ends up doing, and splitting them would make the head act out an
/// ordering the reply never meant.
///
/// `library` and `scripter` are each absent in a deployment that configured no
/// cue vocabulary or no head at all. The tap is built anyway: a reply asking
/// such a deployment to move is narrated as a refusal rather than dropped in
/// silence, because it is the one failure an operator's own edit produces and
/// the log is the only place it shows.
fn cue_tap(
    library: Option<Arc<CueLibrary>>,
    scripter: Option<ScriptHandle>,
    jsonl: JsonlHandle,
    pod: PodId,
    turn: UtteranceId,
) -> CueTap {
    Arc::new(move |cues: Vec<Cue>| {
        let (library, scripter) = match (&library, &scripter) {
            (Some(library), Some(scripter)) => (library, scripter),
            (library, _) => {
                let reason = if library.is_none() {
                    "no_library"
                } else {
                    "no_head"
                };
                for cue in &cues {
                    emit_cue_refused(&jsonl, &pod, turn, cue, reason);
                }
                return;
            }
        };
        let mut resolved = Vec::with_capacity(cues.len());
        for cue in cues {
            match resolve_cue(library, &cue) {
                Ok(resolved_cue) => resolved.push(resolved_cue),
                // Dropped, not corrected: the movement the reply asked for
                // cannot be made, and the nearest one it did not ask for is
                // not an improvement. The rest of the message's cues stand.
                Err(reason) => emit_cue_refused(&jsonl, &pod, turn, &cue, reason),
            }
        }
        if !resolved.is_empty() {
            scripter.send(ScriptInput::Cues {
                pod: pod.clone(),
                turn,
                cues: resolved,
            });
        }
    })
}

/// Build an `AudioSpan` for `[start_sample, end_sample)` from the pod's recent
/// segments: every segment overlapping the range is a covering part, in order.
/// When no segment covers the span (carved before any close landed), the span
/// still names the range with no covering parts and the pod's last known log, and
/// resolves to spliced silence.
///
/// With no closed segment at all — every first utterance of a connection — the log
/// is the one the connection announced, which is the file that carve's audio is
/// actually in. The synthesized `<pod>.framelog` is the last resort, for a carve
/// with no connection behind it.
fn build_audio_span(
    state: &PodState,
    start_sample: u64,
    end_sample: u64,
    pod: &PodId,
) -> AudioSpan {
    let recent = &state.recent_segments;
    let mut covering: Vec<&RecentSegment> = recent
        .iter()
        .filter(|s| s.base < end_sample && s.base.saturating_add(s.len) > start_sample)
        .collect();
    covering.sort_by_key(|s| s.base);
    let log = covering
        .first()
        .map(|s| s.seg_ref.log.clone())
        .or_else(|| recent.back().map(|s| s.seg_ref.log.clone()))
        .or_else(|| state.log.clone())
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
///
/// With no closed segment at all, the room is the one the connection announced —
/// resolved through the same `[pods]` lookup the connection line prints, so a pod
/// the table does not name still reads `unmapped`. `UNMAPPED_ROOM` means
/// "no connection has said", not "no segment has closed".
fn span_context(state: &PodState, carve: &Carve) -> (RoomId, DoaTrack) {
    let seg = state
        .recent_segments
        .iter()
        .find(|s| s.contains(carve.start_sample))
        .or_else(|| state.recent_segments.back());
    match seg {
        Some(s) => (s.room.clone(), DoaTrack::from_telemetry(&s.telemetry)),
        None => (
            state
                .room
                .clone()
                .unwrap_or_else(|| RoomId(crate::config::UNMAPPED_ROOM.to_string())),
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

/// What the gate decided for a minted utterance: dispatch it, decline it for
/// carrying no usable text at all, or decline it as a likely hallucination —
/// through the wake provenance for a scored wake accept, through the barge mark
/// for a barge-in utterance with no wake, through the follow-up mark for one
/// carved inside an open capture window, or through the overlap mark for one
/// carved over the pod's own voice that cut nothing.
enum GateOutcome {
    Dispatch,
    /// The utterance transcribed to nothing usable — absent, empty, or
    /// whitespace-only. Decided first and for every provenance: no text is no
    /// turn, whatever the room was doing, so no brain is called. How it is
    /// reported and what the head does still follow the provenance, which it
    /// carries so that both read the one classification.
    DeclineEmpty(Provenance),
    DeclineWake(WakeConfirmation, GateReject),
    DeclineBarge(GateReject),
    /// Speech carved inside a `<listen/>` window that transcribed to likely
    /// hallucination. The window is wake-less by construction, so without this
    /// an open room's noise would reach the brain ungated for the whole window.
    DeclineFollowUp(GateReject),
    DeclineEcho(GateReject),
}

/// What the room was doing when a candidate was carved. Classified once, by
/// [`provenance`], and read by both of the gate's declines — how each is
/// reported, and whether the head starts its settle.
#[derive(Clone, Copy)]
enum Provenance {
    /// A scored wake accept.
    Wake(WakeConfirmation),
    /// Speech that cut the pod's own reply. Over playback by construction — the
    /// listener asserts it — and a barge whatever window it began in: the cut is
    /// what the decline has to answer for.
    Barge,
    /// Speech carved inside a `<listen/>` capture window that cut nothing.
    FollowUp,
    /// Speech over the pod's own voice that neither cut it nor began in a window:
    /// the echo, or somebody talking across the reply.
    OverPlayback,
    /// Anything else — a carve in a quiet room, which under a bypassed wake gate
    /// is every carve.
    Bypassed,
}

/// Read the carve's marks once, in the order they imply. A wake arm is the
/// provenance whatever else the room did. A barge carries the overlap mark and
/// may carry the window mark, and is a barge either way, because the reply it
/// cut is what the decline owes an answer for. Past it the window mark speaks
/// for speech that cut nothing, and the overlap mark for speech that neither cut
/// nor began in a window. The rest is the room.
fn provenance(carve: &Carve) -> Provenance {
    match carve.wake {
        Some(wake) => Provenance::Wake(wake),
        None if carve.barge_in => Provenance::Barge,
        None if carve.follow_up => Provenance::FollowUp,
        None if carve.over_playback => Provenance::OverPlayback,
        None => Provenance::Bypassed,
    }
}

/// Report a declined scored-wake accept through the brain's event/counter
/// vocabulary without dispatching it — a `WakeCommandAbsent` carrying `reason`,
/// the wake score and the audio span. A non-error outcome: the wake word fired
/// and no command came of it, either because nothing was said or because what
/// came back was a likely hallucination. Neither the phantom text nor the empty
/// one is ever echoed.
fn decline_wake_command(
    utterance: &Utterance,
    wake: &WakeConfirmation,
    reason: WakeCommandReason,
    wiring: &BrainWiring,
) {
    (wiring.events)(BrainEvent::wake_command_absent(
        utterance.id,
        utterance.audio_ref.clone(),
        wake,
        reason,
    ));
    wiring.stats.record_wake_command_absent();
}

/// Report an utterance the gate declined for carrying no usable text, with no
/// wake provenance to report it through. A non-error outcome: noise through a
/// bypassed gate, a barge that captured nothing, an STT attempt that failed.
fn decline_no_transcript(utterance: &Utterance, wiring: &BrainWiring) {
    (wiring.events)(BrainEvent::NoTranscript {
        utterance: utterance.id,
    });
    wiring.stats.record_no_transcript();
}

/// Report a confidence-gated wake-less utterance without dispatching it — a
/// `BargeCommandAbsent` carrying the offending signals in place of wake provenance,
/// and `follow_up` saying which wake-less provenance it had. A non-error outcome:
/// the speech transcribed to likely hallucination, so the phantom text is never
/// echoed.
fn decline_barge_low_confidence(
    utterance: &Utterance,
    reject: GateReject,
    follow_up: bool,
    wiring: &BrainWiring,
) {
    (wiring.events)(BrainEvent::BargeCommandAbsent {
        utterance: utterance.id,
        audio_ref: utterance.audio_ref.clone(),
        no_speech_prob: reject.no_speech_prob,
        avg_logprob: reject.avg_logprob,
        follow_up,
    });
    wiring.stats.record_barge_command_absent();
}

/// Report an utterance carved over the pod's own playback whose transcript tripped
/// the gate. No brain hook: nothing was cut, so there is no interrupted turn to
/// tell the brain about.
fn decline_echo(utterance: &Utterance, reject: GateReject, wiring: &BrainWiring) {
    (wiring.events)(BrainEvent::EchoDeclined {
        utterance: utterance.id,
        audio_ref: utterance.audio_ref.clone(),
        no_speech_prob: reject.no_speech_prob,
        avg_logprob: reject.avg_logprob,
    });
    wiring.stats.record_echo_declined();
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
/// `stt_trim_samples` and `stt_sent_from_sample` are carve-relative sample
/// offsets: the wake-trim boundary the listener computed (`null` with no wake),
/// and where transcription actually began (`null` with no transcriber wired).
/// They differ whenever the wake word is kept in the clip. `Utterance.wake` is
/// skipped by the type's own `Serialize`, so a dispatched turn's line carries the
/// boundary only because it is restated here.
#[derive(Serialize)]
struct UtteranceLine<'a> {
    #[serde(flatten)]
    utterance: &'a Utterance,
    stt_elapsed_us: Option<u64>,
    stt_trim_samples: Option<usize>,
    stt_sent_from_sample: Option<usize>,
    /// Whether this utterance was heard inside a capture window rather than on a
    /// wake word: it carries no wake provenance and none was needed. This answers
    /// where the speech *began*. What the gate makes of the candidate answers what
    /// the speech *did*, so a `brain_barge_command_absent` for this same utterance
    /// may carry `follow_up: false` — the speech began in a window and went on to
    /// cut the reply that opened it.
    follow_up: bool,
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
    use futures::StreamExt;
    use futures::future::BoxFuture;
    use futures::stream::BoxStream;
    use serde_json::Value;
    use speech_pipeline::{
        BargeCause, CarveTiming, DropOldestQueue, EndpointState, EndpointTransition,
        InterruptProgress, ScoreDistribution, ScoreSummary, SegmentAudio, SegmentEndCause,
        SegmentEndInfo, SpeakBody, StatsFlushCause, StatsModel, TranscriptConfidence,
        TranscriptEvent, TransitionCause, TurnEnd, WakeConfirmation,
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
            cause: EndpointCause::SoftEndpoint,
            barge_in: false,
            over_playback: false,
            follow_up: false,
            timing: CarveTiming::default(),
        }
    }

    /// A barge carve, the shape the listener produces for speech that cut the
    /// pod's reply: both marks, never the barge alone. The listener sets the
    /// overlap with the barge at the mint and asserts the pair, so a fixture
    /// carrying only `barge_in` pins a candidate production cannot make — and the
    /// rules that read the overlap mark would be tested against the wrong room.
    fn barged(seq: u64, start: u64, end: u64) -> CarvedUtterance {
        CarvedUtterance {
            barge_in: true,
            over_playback: true,
            ..carved(seq, start, end, None)
        }
    }

    /// A wake-gated carve, the shape the listener produces for a scored accept:
    /// the wake provenance carries the trim boundary.
    fn carved_trimmed(seq: u64, start: u64, end: u64, wake: WakeConfirmation) -> CarvedUtterance {
        carved(seq, start, end, Some(wake))
    }

    /// Re-stamp a carve onto `epoch`, for a test that drives a reconnect.
    fn at_epoch(mut u: CarvedUtterance, epoch: u64) -> CarvedUtterance {
        u.utterance_id.epoch = epoch;
        u
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
        PipelineItem::Segment {
            seg: Box::new(seg),
            epoch,
        }
    }

    /// Write a `pod-x` sidecar into `store` holding one `Ungated` entry per
    /// `(part, end_cause)` of `segment_id`, and return the framelog path the
    /// entries belong to. `truncated` follows `HostCapped` and `resumed` follows
    /// a non-zero part, as the recorder writes them.
    fn seed_sidecar(store: &Path, segment_id: u32, parts: &[(u16, SegmentEndCause)]) -> PathBuf {
        let framelog = store.join("pod-x_0.framelog");
        let mut sc = crate::recorder::Sidecar::new("pod-x");
        for &(part, end_cause) in parts {
            sc.push(crate::recorder::SidecarSegment {
                segment_id,
                part,
                wake: WakeClass::Ungated,
                start_epoch_us: 1,
                end_epoch_us: 2,
                end_cause,
                truncated: matches!(end_cause, SegmentEndCause::HostCapped),
                resumed: part > 0,
                gap_count: 0,
                samples: 16,
            });
        }
        sc.write_atomic(&sidecar_path(&framelog)).unwrap();
        framelog
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
        fn handle(&self, u: Utterance, mut out: ResponseSink) -> BoxFuture<'static, TurnEnd> {
            let cmd = SpeakCmd {
                target: u.pod.clone(),
                in_reply_to: Some(u.id),
                body: SpeakBody::Text("ack".into()),
                interruptible: true,
                timings: u.timings.clone(),
            };
            let _ = out.try_send(cmd);
            futures::future::ready(TurnEnd::Closed).boxed()
        }
        fn interrupt(&self, _id: UtteranceId, _progress: InterruptProgress) {}
    }

    /// The advisory nudges a brain was handed, in order. A gate-declined barge
    /// records the chain it carried, since "with the interrupted turn attached" is the
    /// part that matters.
    #[derive(Default)]
    struct NudgeLog {
        wakes: Vec<PodId>,
        barge_declined: Vec<(UtteranceId, Option<UtteranceId>)>,
    }

    /// `EchoTestBrain` plus a record of the two non-dispatch seams, so a test can
    /// assert what the pipeline nudged without a real link, and the disposition it
    /// answers every turn with.
    struct RecordingBrain {
        log: Arc<Mutex<NudgeLog>>,
        end: TurnEnd,
        /// When set, the turn's one cmd is started and settled clean against
        /// this ledger before `handle` returns — the chain whose last segment
        /// carries no speech, where playback is over before the brain is.
        settle_first: Option<Arc<TurnLedger>>,
        /// The movements this brain's reply asks for, handed to the cue tap
        /// ahead of the speech as a real reply's are.
        cues: Vec<Cue>,
    }

    impl Brain for RecordingBrain {
        fn handle(&self, u: Utterance, out: ResponseSink) -> BoxFuture<'static, TurnEnd> {
            let (pod, id) = (u.pod.clone(), u.id);
            if !self.cues.is_empty() {
                out.cue(self.cues.clone());
            }
            let spoken = EchoTestBrain.handle(u, out);
            let end = self.end;
            let settle_first = self.settle_first.clone();
            async move {
                spoken.await;
                if let Some(ledger) = settle_first {
                    ledger.record_started(&pod, Some(id), 960, tokio::time::Instant::now());
                    ledger.settle_job(&pod, Some(id), true);
                }
                end
            }
            .boxed()
        }
        fn interrupt(&self, _id: UtteranceId, _progress: InterruptProgress) {}
        fn wake(&self, pod: &PodId) {
            self.log.lock().unwrap().wakes.push(pod.clone());
        }
        fn barge_declined(&self, u: &Utterance) {
            let cut = u
                .barge_in
                .as_ref()
                .and_then(|b| b.chain.last())
                .map(|seg| seg.utterance);
            self.log.lock().unwrap().barge_declined.push((u.id, cut));
        }
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

    /// A transcriber that takes wall-clock time to answer, so a case can hold an
    /// STT in flight while the clock runs.
    struct SlowTranscriber(Duration);
    impl Transcriber for SlowTranscriber {
        fn transcribe(
            &self,
            _audio: SegmentAudio,
        ) -> BoxStream<'static, Result<TranscriptEvent, TranscribeError>> {
            let delay = self.0;
            futures::stream::once(async move {
                tokio::time::sleep(delay).await;
                Ok(TranscriptEvent {
                    text: "late".into(),
                    is_final: true,
                    confidence: None,
                })
            })
            .boxed()
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
        listen: Option<ListenWiring>,
        settle_first: Option<Arc<TurnLedger>>,
        wake_word: WakeWordInStt,
        nudges: Arc<Mutex<NudgeLog>>,
        scripter: Option<ScriptHandle>,
        cues: Option<Arc<CueLibrary>>,
        reply_cues: Vec<Cue>,
        turn_end: TurnEnd,
        queue_depth: usize,
    }

    impl Harness {
        fn new() -> Harness {
            Harness {
                record_dir: None,
                transcriber: None,
                brain: false,
                confidence_gate: ConfidenceGate::OFF,
                wake_word: WakeWordInStt::Trim,
                events: Arc::new(Mutex::new(Vec::new())),
                stats: Arc::new(BrainStats::default()),
                barge: None,
                listen: None,
                settle_first: None,
                nudges: Arc::new(Mutex::new(NudgeLog::default())),
                turn_end: TurnEnd::Closed,
                scripter: None,
                cues: None,
                reply_cues: Vec::new(),
                queue_depth: 32,
            }
        }
        /// Bound the queue's sheddable lane, so a test can pin a depth narrower
        /// than the burst it pre-loads.
        fn queue_depth(mut self, depth: usize) -> Harness {
            self.queue_depth = depth;
            self
        }
        /// Settle the turn's playback against `ledger` before the brain returns,
        /// so `dispatch_done` is the last of the two facts rather than the first.
        fn settle_first(mut self, ledger: Arc<TurnLedger>) -> Harness {
            self.settle_first = Some(ledger);
            self
        }
        /// Wire the `<listen/>` opener to `feed`, with a window of
        /// `window_samples`, so a test can read what the listener is told.
        fn listen(mut self, feed: FeedFn, window_samples: u64) -> Harness {
            self.listen = Some(ListenWiring {
                feed,
                window_samples,
            });
            self
        }
        /// Wire the head's taps to `handle`, so a test can read the
        /// interaction lifecycle as the scripter receives it.
        fn scripter(mut self, handle: ScriptHandle) -> Harness {
            self.scripter = Some(handle);
            self
        }
        /// Give the run a cue vocabulary, so a reply's movements resolve into
        /// the inputs the head receives.
        fn cues(mut self, library: CueLibrary) -> Harness {
            self.cues = Some(Arc::new(library));
            self
        }
        /// What this harness's brain asks the head to do in its reply.
        fn reply_cues(mut self, cues: Vec<Cue>) -> Harness {
            self.reply_cues = cues;
            self
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
        fn wake_word(mut self, mode: WakeWordInStt) -> Harness {
            self.wake_word = mode;
            self
        }
        fn brain(mut self) -> Harness {
            self.brain = true;
            self
        }
        /// How every turn this harness's brain takes ends.
        fn turn_end(mut self, end: TurnEnd) -> Harness {
            self.turn_end = end;
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

        /// Run the pipeline over `items` to the end of its queue.
        async fn run(self, items: Vec<PipelineItem>) -> (Vec<Value>, Vec<SpeakCmd>) {
            self.start(items).await.finish().await
        }

        /// Start the pipeline over `items` with the queue's sender still open, so a
        /// case can feed more items or let the clock run before closing it. The
        /// items are queued before the task is spawned, so a burst a case means to
        /// overflow the sheddable lane sheds against the depth, not against the
        /// reader's pace.
        async fn start(self, items: Vec<PipelineItem>) -> RunningPipeline {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("events.jsonl");
            let (jsonl, writer_join) = crate::jsonl::spawn_quiet(&JsonlSink::File(path.clone()))
                .await
                .unwrap();

            let (speak_tx, speak_rx) = fmpsc::channel::<SpeakCmd>(16);
            let brain = if self.brain {
                let sink = self.events.clone();
                let events: BrainEventFn = Arc::new(move |e| sink.lock().unwrap().push(e));
                Some(BrainWiring {
                    brain: Arc::new(RecordingBrain {
                        log: self.nudges.clone(),
                        end: self.turn_end,
                        settle_first: self.settle_first.clone(),
                        cues: self.reply_cues.clone(),
                    }),
                    speak_tx,
                    events,
                    stats: self.stats.clone(),
                })
            } else {
                None
            };

            let (tx, rx) = DropOldestQueue::<PipelineItem>::new(self.queue_depth);
            // The lane split comes off the item itself, so a new variant cannot
            // ride a lane here that it does not ride in the daemon.
            for item in items {
                send_on_its_lane(&tx, item);
            }

            let ctx = PipelineCtx {
                cues: self.cues.clone(),
                record_dir: self.record_dir.clone(),
                clock_step_clamps: Arc::new(AtomicU64::new(0)),
                transcriber: self.transcriber.clone(),
                brain,
                confidence_gate: self.confidence_gate,
                wake_word: self.wake_word,
                barge: self
                    .barge
                    .map(|(ledger, flush)| BargeWiring { ledger, flush }),
                listen: self.listen.map(Arc::new),
                scripter: self.scripter.clone(),
            };
            let loop_jsonl = jsonl.clone();
            let join = tokio::task::spawn(async move {
                run(rx, ctx, loop_jsonl).await.unwrap();
            });
            RunningPipeline {
                tx: Some(tx),
                join,
                jsonl,
                writer_join,
                path,
                speak_rx,
                _dir: dir,
            }
        }
    }

    fn send_on_its_lane(tx: &speech_pipeline::Sender<PipelineItem>, item: PipelineItem) {
        if item.is_control() {
            tx.send_reliable(item);
        } else {
            tx.send_sheddable(item);
        }
    }

    /// A pipeline running with its queue still open, and the sinks its answers are
    /// read out of when it ends.
    struct RunningPipeline {
        tx: Option<speech_pipeline::Sender<PipelineItem>>,
        join: tokio::task::JoinHandle<()>,
        jsonl: JsonlHandle,
        writer_join: tokio::task::JoinHandle<()>,
        path: PathBuf,
        speak_rx: fmpsc::Receiver<SpeakCmd>,
        _dir: tempfile::TempDir,
    }

    impl RunningPipeline {
        /// Feed one more item and let the loop take it, so what follows in the case
        /// happens after it rather than beside it.
        async fn feed(&mut self, item: PipelineItem) {
            send_on_its_lane(self.tx.as_ref().expect("queue open"), item);
            self.settle().await;
        }

        /// Move the paused clock forward and let anything that came due run.
        async fn advance(&mut self, by: Duration) {
            tokio::time::advance(by).await;
            self.settle().await;
        }

        /// Hand the runtime enough turns for the pipeline task to reach its next
        /// await on an empty queue.
        async fn settle(&mut self) {
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
        }

        /// Close the queue and collect what the run wrote and said.
        async fn finish(mut self) -> (Vec<Value>, Vec<SpeakCmd>) {
            drop(self.tx.take());
            self.join.await.unwrap();
            drop(self.jsonl);
            self.writer_join.await.unwrap();

            let lines = std::fs::read_to_string(&self.path)
                .unwrap()
                .lines()
                .map(|l| serde_json::from_str(l).unwrap())
                .collect();
            let mut cmds = Vec::new();
            while let Ok(cmd) = self.speak_rx.try_recv() {
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

    /// Drain everything the taps sent. The handle is dropped first so the
    /// pipeline's own clone is gone and the queue ends.
    async fn script_inputs(
        handle: ScriptHandle,
        mut inbox: crate::scripter::ScriptInbox,
    ) -> Vec<ScriptInput> {
        drop(handle);
        let mut seen = Vec::new();
        while let Some(input) = inbox.recv().await {
            seen.push(input);
        }
        seen
    }

    /// The happy path's taps, in the order the interaction happened: the wake
    /// raises, and the brain holding the turn is what keeps the head up across a
    /// long think.
    #[tokio::test]
    async fn a_dispatched_turn_brackets_itself_for_the_head() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        Harness::new()
            .transcriber(FakeTranscriber(Some(("hello world".into(), None))))
            .brain()
            .scripter(handle.clone())
            .run(vec![
                wake_detected(1, 8),
                soft_endpoint(carved(1, 0, 16, None)),
            ])
            .await;

        assert_eq!(
            script_inputs(handle, rx).await,
            vec![
                ScriptInput::Wake(pod()),
                ScriptInput::TurnStarted {
                    pod: pod(),
                    turn: UtteranceId(1),
                },
                ScriptInput::TurnEnded {
                    pod: pod(),
                    turn: UtteranceId(1),
                    end: TurnEnd::Closed,
                },
            ]
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    /// The third of the closing trigger's facts: dispatch returning is what says
    /// no further command is coming, and the scripter cannot schedule the head's
    /// ending without it. It rides the same tap as the other two, dated with the
    /// accounting the ledger answered.
    #[tokio::test]
    async fn a_returned_dispatch_reports_the_turns_accounting_to_the_head() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        let ledger = Arc::new(TurnLedger::new());
        Harness::new()
            .transcriber(FakeTranscriber(Some(("hello world".into(), None))))
            .brain()
            .barge(ledger, Err(FlushRejected::NotPlaying))
            .scripter(handle.clone())
            .run(vec![
                wake_detected(1, 8),
                soft_endpoint(carved(1, 0, 16, None)),
            ])
            .await;

        let seen = script_inputs(handle, rx).await;
        let Some(ScriptInput::Audio {
            pod: at,
            turn,
            audio,
        }) = seen.last()
        else {
            panic!("dispatch reported nothing: {seen:?}");
        };
        assert_eq!((at, *turn), (&pod(), UtteranceId(1)));
        assert!(audio.dispatch_done, "no further cmd is coming");
        // The echo brain queues one reply, and nothing plays it in this harness.
        assert_eq!(audio.cmds_sent, 1);
        assert_eq!(audio.awaiting_start, 1);
        drop(jsonl);
        writer.await.unwrap();
    }

    /// The brain's disposition is the pipeline's to carry, not to decide: a turn
    /// the response asked to keep listening after reaches the head as `Open`.
    #[tokio::test]
    async fn a_turn_left_open_says_so_at_the_tap() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        Harness::new()
            .transcriber(FakeTranscriber(Some(("hello world".into(), None))))
            .brain()
            .turn_end(TurnEnd::Open)
            .scripter(handle.clone())
            .run(vec![
                wake_detected(1, 8),
                soft_endpoint(carved(1, 0, 16, None)),
            ])
            .await;

        assert_eq!(
            script_inputs(handle, rx).await.last(),
            Some(&ScriptInput::TurnEnded {
                pod: pod(),
                turn: UtteranceId(1),
                end: TurnEnd::Open,
            })
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    /// The false-positive wake: the arm dies with no command, and the head is
    /// told so — with no brain wired, which is where the tap sits in the arm.
    #[tokio::test]
    async fn an_expired_arm_settles_the_head() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        Harness::new()
            .scripter(handle.clone())
            .run(vec![
                wake_detected(1, 8),
                PipelineItem::Listener(ListenerEvent::ArmExpired {
                    pod: pod(),
                    wake: WakeConfirmation {
                        score: 0.8,
                        wake_end_sample: 0,
                        stt_trim_samples: 0,
                    },
                    start_sample: 0,
                    end_sample: 16,
                }),
            ])
            .await;

        assert_eq!(
            script_inputs(handle, rx).await,
            vec![ScriptInput::Wake(pod()), ScriptInput::Unanswered(pod())]
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    /// A wake whose command transcribed to a likely hallucination: the gate
    /// declines it, no turn is dispatched, and the head settles on the decline
    /// rather than waiting out its engagement ceiling.
    #[tokio::test]
    async fn a_gate_decline_settles_the_head() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        let wake = WakeConfirmation {
            score: 0.9,
            wake_end_sample: 0,
            stt_trim_samples: 0,
        };
        Harness::new()
            .transcriber(FakeTranscriber(Some((
                "thank you".into(),
                Some(conf(0.9, -1.0)),
            ))))
            .brain()
            .gate(ConfidenceGate {
                no_speech_max: 0.2,
                avg_logprob_min: None,
            })
            .scripter(handle.clone())
            .run(vec![soft_endpoint(carved(1, 0, 16, Some(wake)))])
            .await;

        assert_eq!(
            script_inputs(handle, rx).await,
            vec![ScriptInput::Unanswered(pod())]
        );
        drop(jsonl);
        writer.await.unwrap();
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
                over_playback: false,
                follow_up: false,
                timing: CarveTiming::default(),
                sent_from: Some(0),
            },
            result: Some(Ok(Transcript {
                text: "stale".into(),
                confidence: None,
            })),
            elapsed_us: 0,
        };
        let ctx = PipelineCtx {
            record_dir: None,
            clock_step_clamps: Arc::new(AtomicU64::new(0)),
            transcriber: None,
            brain: None,
            confidence_gate: ConfidenceGate::OFF,
            wake_word: WakeWordInStt::default(),
            barge: None,
            listen: None,
            scripter: None,
            cues: None,
        };
        handle_stt_done(done, &mut pods, &mut next_id, &ctx, &jsonl).await;

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
        let span = build_audio_span(&PodState::default(), 0, 100, &dirty);
        assert_eq!(span.log, "___evil_pod.framelog");
        assert!(span.segments.is_empty());
    }

    // ── The connection's own context ──────────────────────────────────────────

    /// A pod's connection announcing its room and frame log.
    fn connected(epoch: u64, room: &str, log: &str) -> PipelineItem {
        PipelineItem::Connected {
            pod: pod(),
            epoch,
            room: RoomId(room.into()),
            log: log.into(),
        }
    }

    /// The lane a connection announcement rides is the same one a wake rides:
    /// losing it costs the room and the log of that connection's first
    /// utterances, which is exactly the loss the reliable lane exists for.
    #[test]
    fn connected_is_a_control_item() {
        assert!(connected(1, "r-office", "a.framelog").is_control());
        assert!(!segment(seg_at(1, 0, 16)).is_control());
    }

    /// The name a dropped item is reported under, and the pod it is charged to,
    /// come off the variant itself — so a new variant cannot be reported under an
    /// older one's name at a site that never matched on it.
    #[test]
    fn every_item_names_itself_and_its_pod() {
        let listener = |ev| PipelineItem::Listener(ev);
        let wake = WakeConfirmation {
            score: 0.8,
            wake_end_sample: 8_000,
            stt_trim_samples: 0,
        };
        // One row per variant: the compiler catches a variant nobody named, not a
        // variant named as another one, and the name is what a drop line sends its
        // reader after.
        let items = [
            (segment(seg_at(1, 0, 16)), "segment"),
            (connected(1, "r-office", "a.framelog"), "connected"),
            (soft_endpoint(carved(1, 0, 100, None)), "soft_endpoint"),
            (wake_detected(1, 8), "wake_detected"),
            (
                listener(ListenerEvent::BargeIn {
                    pod: pod(),
                    epoch: 1,
                    cause: BargeCause::Speech,
                    trigger_sample: 16,
                    host_rx: HostMicros(1),
                }),
                "barge_in",
            ),
            (
                listener(ListenerEvent::Superseded {
                    pod: pod(),
                    utterance_id: uid(1),
                }),
                "superseded",
            ),
            (
                listener(ListenerEvent::UtteranceClosed {
                    pod: pod(),
                    utterance_id: uid(1),
                }),
                "utterance_closed",
            ),
            (
                listener(ListenerEvent::ArmExpired {
                    pod: pod(),
                    wake,
                    start_sample: 0,
                    end_sample: 16,
                }),
                "arm_expired",
            ),
            (
                listener(ListenerEvent::EndpointerTransition {
                    pod: pod(),
                    epoch: 1,
                    transition: EndpointTransition {
                        from: EndpointState::Speech,
                        to: EndpointState::SoftEndpointed,
                        cause: TransitionCause::SoftEndpoint,
                        sample_offset: 16,
                    },
                }),
                "endpointer_transition",
            ),
            (
                listener(ListenerEvent::ModelStats {
                    pod: pod(),
                    epoch: 1,
                    model: StatsModel::Silero,
                    cause: StatsFlushCause::Transition,
                    summary: ScoreSummary {
                        first_chunk_end: 0,
                        last_chunk_end: 16,
                        chunks: 1,
                        unscored_chunks: 0,
                        distribution: Some(ScoreDistribution {
                            min: 0.0,
                            max: 1.0,
                            mean: 0.5,
                            median: 0.5,
                        }),
                    },
                }),
                "model_stats",
            ),
        ];
        for (item, kind) in items {
            assert_eq!(item.kind(), kind);
            assert_eq!(item.pod(), &pod(), "{kind}");
        }
    }

    /// With no closed segment, the connected log is the file the carve's audio is
    /// in — the synthesized `<pod>.framelog` is only for a carve with no
    /// connection behind it at all.
    #[test]
    fn build_audio_span_takes_the_connected_log_with_no_segments() {
        let state = PodState {
            log: Some("20260903T225630_657Z_1.framelog".into()),
            ..PodState::default()
        };
        let span = build_audio_span(&state, 0, 100, &pod());
        assert_eq!(span.log, "20260903T225630_657Z_1.framelog");
        assert!(span.segments.is_empty());
    }

    /// Same for the room: `UNMAPPED_ROOM` means "no connection has said", not
    /// "no segment has closed".
    #[test]
    fn span_context_takes_the_connected_room_with_no_segments() {
        let carve = Carve {
            id: uid(1),
            start_sample: 0,
            end_sample: 100,
            wake: None,
            cause: EndpointCause::SoftEndpoint,
            barge_in: false,
            over_playback: false,
            follow_up: false,
            timing: CarveTiming::default(),
            sent_from: Some(0),
        };
        let connected = PodState {
            room: Some(RoomId("r-office".into())),
            ..PodState::default()
        };
        let (room, doa) = span_context(&connected, &carve);
        assert_eq!(room.0, "r-office");
        assert!(doa.0.is_empty(), "no segment, so no bearings");
        let (fallback, _) = span_context(&PodState::default(), &carve);
        assert_eq!(fallback.0, crate::config::UNMAPPED_ROOM);
    }

    /// End to end: the first utterance of a connection is carved while segment 0
    /// is still open, and it names the room and the log the hello resolved.
    #[tokio::test]
    async fn first_utterance_of_a_connection_names_its_room_and_log() {
        let (lines, _cmds) = Harness::new()
            .transcriber(FakeTranscriber(Some(("test one".into(), None))))
            .brain()
            .run(vec![
                connected(1, "r-office", "20260903T225630_657Z_1.framelog"),
                soft_endpoint(carved(1, 16128, 59968, None)),
            ])
            .await;
        let utt = lines.iter().find(|v| v["event"] == "utterance").unwrap();
        assert_eq!(utt["room"], "r-office");
        assert_eq!(utt["audio_ref"]["log"], "20260903T225630_657Z_1.framelog");
    }

    /// A pod the `[pods]` table does not name resolves to `unmapped` at the
    /// connection, so the utterance reads `unmapped` for the reason it always
    /// meant rather than for want of a closed segment.
    #[tokio::test]
    async fn an_unnamed_pods_first_utterance_is_still_unmapped() {
        let (lines, _cmds) = Harness::new()
            .transcriber(FakeTranscriber(Some(("test one".into(), None))))
            .brain()
            .run(vec![
                connected(1, crate::config::UNMAPPED_ROOM, "conn.framelog"),
                soft_endpoint(carved(1, 0, 100, None)),
            ])
            .await;
        let utt = lines.iter().find(|v| v["event"] == "utterance").unwrap();
        assert_eq!(utt["room"], "unmapped");
        assert_eq!(utt["audio_ref"]["log"], "conn.framelog");
    }

    /// No connection announced: the pre-existing fallbacks stand.
    #[tokio::test]
    async fn an_utterance_with_no_connection_falls_back_as_before() {
        let (lines, _cmds) = Harness::new()
            .transcriber(FakeTranscriber(Some(("test one".into(), None))))
            .brain()
            .run(vec![soft_endpoint(carved(1, 0, 100, None))])
            .await;
        let utt = lines.iter().find(|v| v["event"] == "utterance").unwrap();
        assert_eq!(utt["room"], "unmapped");
        assert_eq!(utt["audio_ref"]["log"], "pod-x.framelog");
    }

    /// A closed segment still outranks the connection: it is the covering audio,
    /// and it carries the bearings the connection has none of.
    #[tokio::test]
    async fn a_covering_segment_outranks_the_connection() {
        let mut seg = seg_at(1, 0, 200);
        seg.room = RoomId("r-lab".into());
        seg.audio_ref.log = "segment.framelog".into();
        let (lines, _cmds) = Harness::new()
            .transcriber(FakeTranscriber(Some(("test one".into(), None))))
            .brain()
            .run(vec![
                connected(1, "r-office", "conn.framelog"),
                segment(seg),
                soft_endpoint(carved(1, 0, 100, None)),
            ])
            .await;
        let utt = lines.iter().find(|v| v["event"] == "utterance").unwrap();
        assert_eq!(utt["room"], "r-lab");
        assert_eq!(utt["audio_ref"]["log"], "segment.framelog");
    }

    /// A hello from a connection the pipeline has already superseded describes an
    /// index space nothing live is in; its log would send a replay at the wrong
    /// audio.
    #[tokio::test]
    async fn a_stale_connection_does_not_replace_the_live_one() {
        let (lines, _cmds) = Harness::new()
            .transcriber(FakeTranscriber(Some(("test one".into(), None))))
            .brain()
            .run(vec![
                connected(1, "r-office", "live.framelog"),
                connected(0, "r-lab", "stale.framelog"),
                soft_endpoint(carved(1, 0, 100, None)),
            ])
            .await;
        let utt = lines.iter().find(|v| v["event"] == "utterance").unwrap();
        assert_eq!(utt["room"], "r-office");
        assert_eq!(utt["audio_ref"]["log"], "live.framelog");
        // The drop is a line: a hello can only arrive behind a later epoch if the
        // server sent it out of order, and the only other trace is an utterance
        // reading `unmapped` for no stated reason.
        let stale = lines
            .iter()
            .find(|v| v["event"] == "connected_stale")
            .expect("the stale hello is named: {lines:?}");
        assert_eq!(stale["pod"], "pod-x");
        assert_eq!(stale["epoch"], 0);
        assert_eq!(stale["live_epoch"], 1);
    }

    /// A reconnect takes the previous connection's room and log with the tracking
    /// it clears, so an utterance carved after it and before the new hello lands
    /// falls back rather than naming the connection that went.
    #[tokio::test]
    async fn a_reconnect_drops_the_previous_connections_context() {
        let (lines, _cmds) = Harness::new()
            .transcriber(FakeTranscriber(Some(("test one".into(), None))))
            .brain()
            .run(vec![
                connected(1, "r-office", "old.framelog"),
                wake_detected(2, 50),
                soft_endpoint(at_epoch(carved(1, 0, 100, None), 2)),
            ])
            .await;
        let utt = lines.iter().find(|v| v["event"] == "utterance").unwrap();
        assert_eq!(utt["room"], "unmapped");
        assert_eq!(utt["audio_ref"]["log"], "pod-x.framelog");
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
        let h = Harness::new().transcriber(FakeTranscriber(None)).brain();
        let stats = h.stats.clone();
        let (lines, cmds) = h.run(vec![soft_endpoint(carved(1, 0, 16, None))]).await;
        assert!(lines.iter().any(|v| v["event"] == "stt_failed"));
        let utt = lines.iter().find(|v| v["event"] == "utterance").unwrap();
        assert!(utt["transcript"].is_null());
        // The line is still minted — the audio and its stamps are the record —
        // but a failed STT leaves no text, and no text is no turn.
        assert!(cmds.is_empty(), "nothing to answer: {lines:?}");
        assert_eq!(stats.snapshot().no_transcript, 1);
    }

    #[tokio::test]
    async fn wake_detection_labels_segment_positive_then_upgrades_late() {
        // A segment arriving before its wake is provisionally negative; the late
        // WakeDetected landing in its span upgrades it to positive.
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().to_path_buf();
        let framelog = seed_sidecar(&store, 7, &[(0, SegmentEndCause::VadRelease)]);

        let (_lines, _cmds) = Harness::new()
            .record(store.clone())
            .run(vec![segment(seg_at(7, 0, 16)), wake_detected(1, 8)])
            .await;
        let read = crate::recorder::Sidecar::read(&sidecar_path(&framelog)).unwrap();
        assert_eq!(read.segments[0].wake, WakeClass::Positive);
    }

    #[tokio::test]
    async fn a_close_burst_wider_than_the_segment_queue_reaches_the_sink() {
        // One ordinary segment close puts nine listener events on the queue in a
        // few milliseconds of inference; a consumer that is off-CPU for that span
        // sees them all arrive first. With the control events on a sheddable lane
        // the oldest — the wake — was silently evicted, and the pod never woke.
        // Depth 1 makes that window deterministic: the whole burst is wider than
        // the segment budget, and every line still has to land.
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().to_path_buf();
        let framelog = seed_sidecar(&store, 1, &[(0, SegmentEndCause::VadRelease)]);

        let stats = |model: StatsModel, cause: StatsFlushCause| {
            PipelineItem::Listener(ListenerEvent::ModelStats {
                pod: pod(),
                epoch: 1,
                model,
                cause,
                summary: ScoreSummary {
                    first_chunk_end: 2_560,
                    last_chunk_end: 19_456,
                    chunks: 4,
                    unscored_chunks: 0,
                    distribution: Some(ScoreDistribution {
                        min: 0.007,
                        max: 0.999,
                        mean: 0.7,
                        median: 0.9,
                    }),
                },
            })
        };
        let transition = |from: EndpointState, to: EndpointState, cause: TransitionCause| {
            PipelineItem::Listener(ListenerEvent::EndpointerTransition {
                pod: pod(),
                epoch: 1,
                transition: EndpointTransition {
                    from,
                    to,
                    cause,
                    sample_offset: 19_456,
                },
            })
        };

        let (lines, _cmds) = Harness::new()
            .record(store.clone())
            .queue_depth(1)
            .run(vec![
                segment(seg_at(1, 0, 16)),
                wake_detected(1, 8),
                stats(StatsModel::Silero, StatsFlushCause::Transition),
                stats(StatsModel::Oww, StatsFlushCause::Transition),
                transition(
                    EndpointState::Speech,
                    EndpointState::SoftEndpointed,
                    TransitionCause::SoftEndpoint,
                ),
                stats(StatsModel::Silero, StatsFlushCause::SegmentClose),
                stats(StatsModel::Oww, StatsFlushCause::SegmentClose),
                soft_endpoint(carved(1, 0, 16, None)),
                transition(
                    EndpointState::SoftEndpointed,
                    EndpointState::Idle,
                    TransitionCause::DeviceReleaseClosed,
                ),
                PipelineItem::Listener(ListenerEvent::UtteranceClosed {
                    pod: pod(),
                    utterance_id: uid(1),
                }),
            ])
            .await;

        let named = |event: &str| lines.iter().filter(|l| l["event"] == event).count();
        assert_eq!(named("wake_detected"), 1, "{lines:?}");
        assert_eq!(named("utterance"), 1, "{lines:?}");
        assert_eq!(named("utterance_closed"), 1, "{lines:?}");
        let read = crate::recorder::Sidecar::read(&sidecar_path(&framelog)).unwrap();
        assert_eq!(read.segments[0].wake, WakeClass::Positive);
    }

    /// Transcribes to the length of the audio it was handed, so a test can read
    /// off how much of a carve reached the transcriber.
    struct LengthTranscriber;
    impl Transcriber for LengthTranscriber {
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

    /// A wake-gated carve under a `[stt]` config, run through both wake-word
    /// modes: what the transcriber receives, and what the two lines say was sent.
    async fn wake_word_mode_run(mode: WakeWordInStt) -> Vec<Value> {
        let wake = WakeConfirmation {
            score: 0.8,
            wake_end_sample: 5_000,
            stt_trim_samples: 4,
        };
        let mut h = Harness::new().wake_word(mode);
        h.transcriber = Some(Arc::new(LengthTranscriber));
        let (lines, _cmds) = h
            .run(vec![soft_endpoint(carved_trimmed(1, 0, 16, wake))])
            .await;
        lines
    }

    /// `trim` (the default) cuts the wake word: the transcriber sees the carve
    /// from the boundary, and both lines name the same offset.
    #[tokio::test]
    async fn trim_sends_the_carve_from_the_wake_boundary() {
        let lines = wake_word_mode_run(WakeWordInStt::Trim).await;
        let started = lines.iter().find(|l| l["event"] == "stt_started").unwrap();
        assert_eq!(started["samples"], 16, "the carve, not the clip: {lines:?}");
        assert_eq!(started["sent_from_sample"], 4, "{lines:?}");
        let utt = lines.iter().find(|l| l["event"] == "utterance").unwrap();
        assert_eq!(utt["stt_trim_samples"], 4, "{lines:?}");
        assert_eq!(utt["stt_sent_from_sample"], 4, "{lines:?}");
        assert_eq!(
            utt["transcript"]["text"], "12",
            "16 carved less 4: {lines:?}"
        );
    }

    /// `keep` leaves it in: the whole carve is transcribed, and the boundary is
    /// still computed and still logged — which is what an offline comparison of
    /// the two variants reads.
    #[tokio::test]
    async fn keep_sends_the_whole_carve_and_still_logs_the_boundary() {
        let lines = wake_word_mode_run(WakeWordInStt::Keep).await;
        let started = lines.iter().find(|l| l["event"] == "stt_started").unwrap();
        assert_eq!(started["sent_from_sample"], 0, "{lines:?}");
        let utt = lines.iter().find(|l| l["event"] == "utterance").unwrap();
        assert_eq!(utt["stt_trim_samples"], 4, "the boundary stands: {lines:?}");
        assert_eq!(utt["stt_sent_from_sample"], 0, "{lines:?}");
        assert_eq!(utt["transcript"]["text"], "16", "{lines:?}");
    }

    /// A trim boundary past the end of the carve cannot come from a wake arm, so
    /// it is a listener bug. The pipeline neither panics nor eats it: the clip is
    /// clamped empty, the turn still completes, and a line names both numbers.
    #[tokio::test]
    async fn a_boundary_past_the_carve_is_reported_and_clamped() {
        let wake = WakeConfirmation {
            score: 0.8,
            wake_end_sample: 5_000,
            stt_trim_samples: 40,
        };
        let mut h = Harness::new().wake_word(WakeWordInStt::Trim);
        h.transcriber = Some(Arc::new(LengthTranscriber));
        let (lines, _cmds) = h
            .run(vec![soft_endpoint(carved_trimmed(1, 0, 16, wake))])
            .await;
        let out = lines
            .iter()
            .find(|l| l["event"] == "stt_trim_out_of_range")
            .unwrap_or_else(|| panic!("the violation is reported: {lines:?}"));
        assert_eq!(out["stt_trim_samples"], 40, "{lines:?}");
        assert_eq!(out["samples"], 16, "{lines:?}");
        let started = lines.iter().find(|l| l["event"] == "stt_started").unwrap();
        assert_eq!(
            started["sent_from_sample"], 16,
            "clamped to the carve's end: {lines:?}"
        );
        let utt = lines.iter().find(|l| l["event"] == "utterance").unwrap();
        assert_eq!(
            utt["transcript"]["text"], "0",
            "an empty clip, not a panic: {lines:?}"
        );
    }

    /// With no transcriber there is no clip, so the line says so rather than
    /// reporting an offset into audio nothing read.
    #[tokio::test]
    async fn no_transcriber_reports_a_null_sent_from() {
        let wake = WakeConfirmation {
            score: 0.8,
            wake_end_sample: 5_000,
            stt_trim_samples: 4,
        };
        let (lines, _cmds) = Harness::new()
            .run(vec![soft_endpoint(carved_trimmed(1, 0, 16, wake))])
            .await;
        let utt = lines.iter().find(|l| l["event"] == "utterance").unwrap();
        assert_eq!(utt["stt_trim_samples"], 4, "{lines:?}");
        assert!(utt["stt_sent_from_sample"].is_null(), "{lines:?}");
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

    /// The other call site of the connected-log fallback: a wake with no command
    /// following it. The first wake of a connection has to name the log the frames
    /// are in, not a synthesized one — it is the event an operator replays.
    #[tokio::test]
    async fn arm_expired_takes_the_connected_log_before_any_segment_closes() {
        let h = Harness::new().brain();
        let events_seen = h.events.clone();
        h.run(vec![
            connected(1, "r-office", "conn.framelog"),
            PipelineItem::Listener(ListenerEvent::ArmExpired {
                pod: pod(),
                wake: WakeConfirmation {
                    score: 0.8,
                    wake_end_sample: 8_000,
                    stt_trim_samples: 4_800,
                },
                start_sample: 0,
                end_sample: 16_000,
            }),
        ])
        .await;
        let evs = events_seen.lock().unwrap();
        let BrainEvent::WakeCommandAbsent { audio_ref, .. } = &evs[0] else {
            panic!("expected a wake-command-absent, got {:?}", evs[0]);
        };
        assert_eq!(audio_ref.log, "conn.framelog");
        assert!(
            audio_ref.segments.is_empty(),
            "no segment closed: {audio_ref:?}"
        );
    }

    /// The paired case: with no connection announced at all the same arm falls
    /// back to the synthesized name, which is what says the fallback above came
    /// from the hello and not from the pod id.
    #[tokio::test]
    async fn arm_expired_with_no_connection_still_synthesizes_a_log() {
        let h = Harness::new().brain();
        let events_seen = h.events.clone();
        h.run(vec![PipelineItem::Listener(ListenerEvent::ArmExpired {
            pod: pod(),
            wake: WakeConfirmation {
                score: 0.8,
                wake_end_sample: 8_000,
                stt_trim_samples: 4_800,
            },
            start_sample: 0,
            end_sample: 16_000,
        })])
        .await;
        let evs = events_seen.lock().unwrap();
        let BrainEvent::WakeCommandAbsent { audio_ref, .. } = &evs[0] else {
            panic!("expected a wake-command-absent, got {:?}", evs[0]);
        };
        assert_eq!(audio_ref.log, "pod-x.framelog");
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

    /// A wake-only carve the listener is holding: `end_sample` is its speech end
    /// and the wake end with it, the start a preroll-padded half second earlier,
    /// and `deadline_sample` where the listener's wait runs out.
    fn wake_held(epoch: u64, end_sample: u64, deadline_sample: u64) -> PipelineItem {
        PipelineItem::Listener(ListenerEvent::WakeHeld {
            pod: pod(),
            epoch,
            start_sample: end_sample.saturating_sub(8_000),
            end_sample,
            wake_end_sample: end_sample,
            deadline_sample,
        })
    }

    #[tokio::test]
    async fn wake_held_traces_the_wait_and_dispatches_nothing() {
        let h = Harness::new().brain();
        let events_seen = h.events.clone();
        let (lines, cmds) = h.run(vec![wake_held(2, 15_360, 79_360)]).await;
        assert!(cmds.is_empty(), "a held wake dispatches nothing");
        assert!(
            events_seen.lock().unwrap().is_empty(),
            "and tells the brain nothing yet"
        );
        assert_eq!(lines.len(), 1, "the wait is traced: {lines:?}");
        assert_eq!(lines[0]["event"], "wake_held");
        assert_eq!(lines[0]["pod"], "pod-x");
        assert_eq!(lines[0]["start_sample"], 7_360);
        assert_eq!(lines[0]["end_sample"], 15_360);
        assert_eq!(lines[0]["wake_end_sample"], 15_360);
        assert_eq!(lines[0]["deadline_sample"], 79_360);
    }

    /// A bare wake in a room that then goes quiet: the listener keeps its hold and
    /// hears no more audio to resolve it with, so the head comes down on the wall
    /// clock instead of waiting for the room's next sound.
    #[tokio::test(start_paused = true)]
    async fn a_held_wake_with_nothing_after_it_settles_the_head() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        // 8_000 samples of wait: 500 ms at the spine rate.
        let mut run = Harness::new()
            .scripter(handle.clone())
            .start(vec![wake_detected(1, 15_360), wake_held(1, 15_360, 23_360)])
            .await;
        run.settle().await;
        run.advance(Duration::from_millis(600)).await;
        let (lines, _) = run.finish().await;

        assert_eq!(
            script_inputs(handle, rx).await,
            vec![ScriptInput::Wake(pod()), ScriptInput::Unanswered(pod())]
        );
        let released: Vec<&Value> = lines
            .iter()
            .filter(|l| l["event"] == "wake_hold_released")
            .collect();
        assert_eq!(released.len(), 1, "the release is traced once: {lines:?}");
        assert_eq!(released[0]["pod"], "pod-x");
        assert_eq!(
            released[0]["deadline_sample"], 23_360,
            "joined to its wake_held: {lines:?}"
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    /// The listener resolving the hold itself is the ordinary case: the head
    /// settles on `arm_expired`, and the timer that would have said the same thing
    /// is cancelled rather than saying it twice.
    #[tokio::test(start_paused = true)]
    async fn a_held_wake_the_listener_expires_settles_the_head_once() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        let mut run = Harness::new()
            .scripter(handle.clone())
            .start(vec![wake_detected(1, 15_360), wake_held(1, 15_360, 23_360)])
            .await;
        run.settle().await;
        run.advance(Duration::from_millis(100)).await;
        run.feed(PipelineItem::Listener(ListenerEvent::ArmExpired {
            pod: pod(),
            wake: WakeConfirmation {
                score: 0.8,
                wake_end_sample: 15_360,
                stt_trim_samples: 0,
            },
            start_sample: 7_360,
            end_sample: 15_360,
        }))
        .await;
        run.advance(Duration::from_millis(600)).await;
        let (lines, _) = run.finish().await;

        assert_eq!(
            script_inputs(handle, rx).await,
            vec![ScriptInput::Wake(pod()), ScriptInput::Unanswered(pod())]
        );
        assert!(
            !lines.iter().any(|l| l["event"] == "wake_hold_released"),
            "the listener answered first: {lines:?}"
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    /// The command arrived: the hold was consumed, the turn is live, and nothing
    /// tells the head the wake went unanswered.
    #[tokio::test(start_paused = true)]
    async fn a_consumed_hold_does_not_release_the_head() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        let mut run = Harness::new()
            .scripter(handle.clone())
            .start(vec![wake_detected(1, 15_360), wake_held(1, 15_360, 23_360)])
            .await;
        run.settle().await;
        run.advance(Duration::from_millis(100)).await;
        run.feed(soft_endpoint(carved(1, 7_360, 23_360, None)))
            .await;
        run.advance(Duration::from_millis(600)).await;
        let (lines, _) = run.finish().await;

        let inputs = script_inputs(handle, rx).await;
        assert!(
            !inputs.contains(&ScriptInput::Unanswered(pod())),
            "the wake was answered: {inputs:?}"
        );
        assert!(
            !lines.iter().any(|l| l["event"] == "wake_hold_released"),
            "{lines:?}"
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    /// A continuation extends the wake-only carve, so the listener refreshes the
    /// hold with a later deadline; the head's release moves with it.
    #[tokio::test(start_paused = true)]
    async fn a_refreshed_hold_moves_its_release() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, mut rx) = crate::scripter::channel(jsonl.clone());
        // 500 ms of wait, then a refresh carrying 1500 ms.
        let mut run = Harness::new()
            .scripter(handle.clone())
            .start(vec![
                wake_detected(1, 15_360),
                wake_held(1, 15_360, 23_360),
                wake_held(1, 16_000, 40_000),
            ])
            .await;
        run.settle().await;
        run.advance(Duration::from_millis(1_000)).await;
        let mut seen = Vec::new();
        while let Some(input) = rx.try_recv() {
            seen.push(input);
        }
        assert!(
            !seen.contains(&ScriptInput::Unanswered(pod())),
            "past the first deadline, which no longer governs: {seen:?}"
        );
        run.advance(Duration::from_millis(1_000)).await;
        let (lines, _) = run.finish().await;

        seen.extend(script_inputs(handle, rx).await);
        assert_eq!(
            seen,
            vec![ScriptInput::Wake(pod()), ScriptInput::Unanswered(pod())]
        );
        assert_eq!(
            lines
                .iter()
                .filter(|l| l["event"] == "wake_hold_released")
                .count(),
            1,
            "released once, on the refreshed deadline: {lines:?}"
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    /// A replay or a brainless tuning run: no head to release, and a wall-clock
    /// timer over audio replayed faster than real time would be a fiction.
    #[tokio::test(start_paused = true)]
    async fn no_scripter_arms_no_release() {
        let mut run = Harness::new()
            .start(vec![wake_detected(1, 15_360), wake_held(1, 15_360, 23_360)])
            .await;
        run.settle().await;
        run.advance(Duration::from_millis(600)).await;
        let (lines, _) = run.finish().await;
        assert!(
            !lines.iter().any(|l| l["event"] == "wake_hold_released"),
            "{lines:?}"
        );
    }

    /// A straggler from a superseded connection: its release would send an
    /// `Unanswered` against whatever the live connection is doing, so it arms
    /// nothing. The line it writes is accounting, and stays.
    #[tokio::test(start_paused = true)]
    async fn a_stale_wake_held_arms_nothing() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        let mut run = Harness::new()
            .scripter(handle.clone())
            .start(vec![wake_detected(2, 15_360), wake_held(1, 15_360, 23_360)])
            .await;
        run.settle().await;
        run.advance(Duration::from_millis(600)).await;
        let (lines, _) = run.finish().await;

        assert_eq!(
            script_inputs(handle, rx).await,
            vec![ScriptInput::Wake(pod())]
        );
        assert!(
            !lines.iter().any(|l| l["event"] == "wake_hold_released"),
            "{lines:?}"
        );
        assert!(
            lines.iter().any(|l| l["event"] == "wake_held"),
            "the straggler is still traced: {lines:?}"
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    /// The queue closing ends the wait: no further listener event can resolve the
    /// hold, and the loop is down to its in-flight STT. That STT is what keeps the
    /// loop alive here — a run that ends the moment the queue does would state
    /// nothing about a standing release — and under a paused clock the wait it
    /// spends is auto-advanced through, so a release that survived the close would
    /// fire inside it.
    #[tokio::test(start_paused = true)]
    async fn a_closed_queue_drops_a_standing_hold_release() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        // Six seconds of wait behind a transcription that takes ten.
        let mut h = Harness::new().scripter(handle.clone());
        h.transcriber = Some(Arc::new(SlowTranscriber(Duration::from_secs(10))));
        let (lines, _) = h
            .run(vec![
                wake_detected(1, 15_360),
                soft_endpoint(carved(1, 0, 16, None)),
                wake_held(1, 15_360, 111_360),
            ])
            .await;

        assert_eq!(
            script_inputs(handle, rx).await,
            vec![ScriptInput::Wake(pod())]
        );
        assert!(
            !lines.iter().any(|l| l["event"] == "wake_hold_released"),
            "{lines:?}"
        );
        drop(jsonl);
        writer.await.unwrap();
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
                    unscored_chunks: 0,
                    distribution: Some(ScoreDistribution {
                        min: 0.001,
                        max: 0.031,
                        mean: 0.004,
                        median: 0.002,
                    }),
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
        let framelog = seed_sidecar(
            &store,
            7,
            &[
                (0, SegmentEndCause::HostCapped),
                (1, SegmentEndCause::VadRelease),
            ],
        );

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
            cause: BargeCause::Speech,
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

    /// The cut's own line says which rule fired. A wake barge and a speech barge
    /// are handled identically from here on, so the line is the only place the
    /// difference is on the record — and under the default mode a `speech` cause
    /// is a deployment running the other one.
    #[tokio::test]
    async fn the_barge_line_names_the_rule_that_fired() {
        for (cause, name) in [(BargeCause::Speech, "speech"), (BargeCause::Wake, "wake")] {
            let event = PipelineItem::Listener(ListenerEvent::BargeIn {
                pod: pod(),
                epoch: 1,
                cause,
                trigger_sample: 4_800,
                host_rx: HostMicros(2_000_000),
            });
            let (lines, _) = Harness::new().run(vec![event]).await;
            let barge = lines
                .iter()
                .find(|v| v["event"] == "barge_in")
                .expect("a barge_in line");
            assert_eq!(barge["cause"], name);
        }
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
        //
        // The head raise is the other thing that has to survive a missing
        // playback path: barge is the only raise with no wake word in front of
        // it, so a tap sunk below the wiring guard loses the head for every
        // interaction on a pod whose writer is gone.
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        let (lines, _) = Harness::new()
            .brain()
            .scripter(handle.clone())
            .run(vec![barge_in_event()])
            .await;

        assert!(lines.iter().any(|v| v["event"] == "barge_in"));
        assert!(!lines.iter().any(|v| v["event"] == "barge_in_stale"));
        assert_eq!(
            script_inputs(handle, rx).await,
            vec![ScriptInput::Barge(pod())]
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn a_barge_utterance_carries_the_chain_and_a_plain_one_does_not() {
        let ledger = Arc::new(TurnLedger::new());
        ledger.record_dispatch(&pod(), UtteranceId(1), Some("what time is it".into()));
        ledger.record_cmd(&pod(), UtteranceId(1), Some("it is half past three".into()));

        let barge_carve = barged(1, 0, 16);
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
        let barge_carve = barged(1, 0, 16);
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
        let barge_carve = barged(1, 0, 16);
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
        let barge_carve = barged(1, 0, 16);
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

    #[tokio::test]
    async fn an_echo_of_the_pods_own_reply_is_declined_not_dispatched() {
        // The residual of a reply leaking back through the mic: it never sustained
        // enough to cut the playback, so it carries no barge mark and — under a
        // bypassed wake gate — no wake either. Before the overlap arm it reached the
        // brain on the strength of its own echo.
        let echo = CarvedUtterance {
            over_playback: true,
            ..carved(1, 0, 16, None)
        };
        let h = Harness::new()
            .brain()
            .transcriber(FakeTranscriber(Some((
                "the weather today is".into(),
                Some(conf(0.37, -0.99)),
            ))))
            .gate(ConfidenceGate {
                no_speech_max: 0.2,
                avg_logprob_min: None,
            });
        let events_seen = h.events.clone();
        let stats = h.stats.clone();
        let nudges = h.nudges.clone();
        let (lines, cmds) = h.run(vec![soft_endpoint(echo)]).await;

        assert!(cmds.is_empty(), "the robot does not answer itself");
        assert!(
            !lines.iter().any(|v| v["event"] == "brain_dispatched"),
            "and nothing was dispatched: {lines:?}"
        );
        assert_eq!(stats.snapshot().echo_declined, 1);
        assert_eq!(stats.snapshot().barge_command_absent, 0);
        let evs = events_seen.lock().unwrap();
        assert_eq!(evs.len(), 1);
        assert!(
            matches!(evs[0], BrainEvent::EchoDeclined { .. }),
            "the decline carries the overlap mark: {:?}",
            evs[0]
        );
        assert!(
            nudges.lock().unwrap().barge_declined.is_empty(),
            "nothing was cut, so the brain hears nothing about a cut turn"
        );
        // The `utterance` line carries the mark the gate read.
        let u = lines.iter().find(|v| v["event"] == "utterance").unwrap();
        assert_eq!(u["over_playback"], true);
    }

    #[tokio::test]
    async fn a_confident_utterance_over_playback_still_dispatches() {
        // A person genuinely talking over the parrot scores like speech. The arm
        // declines hallucinations, not overlap.
        let over = CarvedUtterance {
            over_playback: true,
            ..carved(1, 0, 16, None)
        };
        let h = Harness::new()
            .brain()
            .transcriber(FakeTranscriber(Some((
                "stop talking".into(),
                Some(conf(0.05, -0.20)),
            ))))
            .gate(ConfidenceGate {
                no_speech_max: 0.2,
                avg_logprob_min: None,
            });
        let stats = h.stats.clone();
        let (_lines, cmds) = h.run(vec![soft_endpoint(over)]).await;

        assert_eq!(cmds.len(), 1, "confident speech over a reply is answered");
        assert_eq!(stats.snapshot().echo_declined, 0);
    }

    #[tokio::test]
    async fn an_echo_decline_is_not_a_raise_and_a_barge_decline_still_is() {
        // `Unanswered` clears the pod's current turn. For a barge that is right — a
        // raise produced no turn. For an echo it would cut short the script of the
        // very reply the echo came from, and nobody raised in the first place.
        //
        // A barge carve always carries the overlap mark too (the `debug_assert` in
        // `carve_utterance` says so), so it is the only carve that can tell the two
        // arms apart. Each case reads its own counter and brain event as well as the
        // scripter, or a reordering of the arms would re-file every barge decline as
        // an echo and read as a scripter bug.
        for (carve, expected, barges, echoes) in [
            (
                CarvedUtterance {
                    over_playback: true,
                    ..carved(1, 0, 16, None)
                },
                vec![],
                0,
                1,
            ),
            (barged(1, 0, 16), vec![ScriptInput::Unanswered(pod())], 1, 0),
        ] {
            let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
            let (handle, rx) = crate::scripter::channel(jsonl.clone());
            let h = Harness::new()
                .brain()
                .transcriber(FakeTranscriber(Some((
                    "phantom".into(),
                    Some(conf(0.37, -0.99)),
                ))))
                .barge(Arc::new(TurnLedger::new()), Err(FlushRejected::NotPlaying))
                .gate(ConfidenceGate {
                    no_speech_max: 0.2,
                    avg_logprob_min: None,
                })
                .scripter(handle.clone());
            let events_seen = h.events.clone();
            let stats = h.stats.clone();
            h.run(vec![soft_endpoint(carve)]).await;

            assert_eq!(script_inputs(handle, rx).await, expected);
            let snap = stats.snapshot();
            assert_eq!(
                (snap.barge_command_absent, snap.echo_declined),
                (barges, echoes),
                "the decline is filed under the arm that made it",
            );
            {
                let evs = events_seen.lock().unwrap();
                assert_eq!(evs.len(), 1, "{evs:?}");
                if barges == 1 {
                    assert!(
                        matches!(evs[0], BrainEvent::BargeCommandAbsent { .. }),
                        "a cut turn is what the brain is told about: {:?}",
                        evs[0]
                    );
                } else {
                    assert!(
                        matches!(evs[0], BrainEvent::EchoDeclined { .. }),
                        "nothing was cut: {:?}",
                        evs[0]
                    );
                }
            }
            drop(jsonl);
            writer.await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_wake_carve_over_playback_declines_through_its_wake_provenance() {
        // The arms are ordered, and the gated policy's outcomes do not move: a
        // scored wake accept that happens to overlap a reply is still a wake
        // decline, with the wake context its consumers expect.
        let wake = Some(WakeConfirmation {
            score: 0.99,
            wake_end_sample: 8,
            stt_trim_samples: 0,
        });
        let carve = CarvedUtterance {
            over_playback: true,
            ..carved(1, 0, 16, wake)
        };
        let h = Harness::new()
            .brain()
            .transcriber(FakeTranscriber(Some((
                "phantom".into(),
                Some(conf(0.37, -0.99)),
            ))))
            .gate(ConfidenceGate {
                no_speech_max: 0.2,
                avg_logprob_min: None,
            });
        let events_seen = h.events.clone();
        let stats = h.stats.clone();
        h.run(vec![soft_endpoint(carve)]).await;

        assert_eq!(stats.snapshot().wake_command_absent, 1);
        assert_eq!(stats.snapshot().echo_declined, 0);
        let evs = events_seen.lock().unwrap();
        assert!(
            matches!(evs[0], BrainEvent::WakeCommandAbsent { .. }),
            "the wake arm comes first: {:?}",
            evs[0]
        );
    }

    #[tokio::test]
    async fn a_confirmed_wake_nudges_the_brain_once() {
        // The pre-warm seam: a brain that talks to a remote peer wants to know a
        // command is coming before it arrives.
        let h = Harness::new().brain();
        let nudges = h.nudges.clone();
        h.run(vec![wake_detected(1, 8)]).await;

        assert_eq!(nudges.lock().unwrap().wakes, [pod()]);
    }

    #[tokio::test]
    async fn a_stale_epoch_wake_does_not_nudge_the_brain() {
        // The nudge sits past the epoch check, so a superseded connection's detection
        // cannot pre-warm a peer for a command that will never be dispatched.
        let h = Harness::new().brain();
        let nudges = h.nudges.clone();
        h.run(vec![wake_detected(2, 8), wake_detected(1, 8)]).await;

        assert_eq!(
            nudges.lock().unwrap().wakes,
            [pod()],
            "only the live epoch's wake nudges"
        );
    }

    /// The presence half of the same placement rule, and the one only a
    /// negative assertion can hold: a superseded connection's wake is not an
    /// interaction, and it must not raise a head. Hoisting the tap above the
    /// epoch guard would leave every positive tap test green.
    #[tokio::test]
    async fn a_stale_epoch_wake_does_not_raise_the_head() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        Harness::new()
            .brain()
            .scripter(handle.clone())
            .run(vec![wake_detected(2, 8), wake_detected(1, 8)])
            .await;

        assert_eq!(
            script_inputs(handle, rx).await,
            vec![ScriptInput::Wake(pod())],
            "the live epoch's wake raises, the superseded one does not"
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    /// A listener feed that records what it was handed, in place of the real
    /// listener (which owns an inference thread).
    fn spy_listen_feed() -> (FeedFn, Arc<Mutex<Vec<Feed>>>) {
        let log: Arc<Mutex<Vec<Feed>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&log);
        let feed: FeedFn = Arc::new(move |_pod, f| {
            sink.lock().unwrap().push(f);
            Box::pin(std::future::ready(()))
        });
        (feed, log)
    }

    /// How long a test's capture window runs. Any number, as long as it is the
    /// one that comes out the other end.
    const TEST_LISTEN_WINDOW: u64 = 96_000;

    /// The pipeline-side opener. A reply whose last clip was heard out before
    /// the brain returned leaves `dispatch_done` the call that completes the
    /// turn, so the window is this task's to open — the fan-out already saw its
    /// settle and had nothing to open on.
    #[tokio::test]
    async fn a_reply_settled_before_dispatch_returns_opens_the_window_here() {
        let ledger = Arc::new(TurnLedger::new());
        let (feed, fed) = spy_listen_feed();
        Harness::new()
            .transcriber(FakeTranscriber(Some(("hello world".into(), None))))
            .brain()
            .turn_end(TurnEnd::Open)
            .barge(Arc::clone(&ledger), Err(FlushRejected::NotPlaying))
            .settle_first(Arc::clone(&ledger))
            .listen(feed, TEST_LISTEN_WINDOW)
            .run(vec![
                wake_detected(1, 8),
                soft_endpoint(carved(1, 0, 16, None)),
            ])
            .await;

        let fed = fed.lock().unwrap();
        assert!(
            matches!(
                fed.as_slice(),
                [Feed::Listen {
                    window_samples: TEST_LISTEN_WINDOW
                }]
            ),
            "one window, as long as the configuration says: {fed:?}",
        );
    }

    /// The same reply, with nothing asking to keep listening: no window. The
    /// disposition is the whole difference.
    #[tokio::test]
    async fn a_closed_turn_opens_no_window_at_dispatch() {
        let ledger = Arc::new(TurnLedger::new());
        let (feed, fed) = spy_listen_feed();
        Harness::new()
            .transcriber(FakeTranscriber(Some(("hello world".into(), None))))
            .brain()
            .turn_end(TurnEnd::Closed)
            .barge(Arc::clone(&ledger), Err(FlushRejected::NotPlaying))
            .settle_first(Arc::clone(&ledger))
            .listen(feed, TEST_LISTEN_WINDOW)
            .run(vec![
                wake_detected(1, 8),
                soft_endpoint(carved(1, 0, 16, None)),
            ])
            .await;

        assert!(fed.lock().unwrap().is_empty(), "the reply said nothing");
    }

    /// Speech heard inside the window reaches the head, so its ending is moved
    /// out past the follow-up that is still being spoken and transcribed.
    #[tokio::test]
    async fn speech_inside_the_window_reaches_the_head() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        let (lines, _) = Harness::new()
            .scripter(handle.clone())
            .run(vec![PipelineItem::Listener(ListenerEvent::ListenHeard {
                pod: pod(),
                epoch: 1,
            })])
            .await;

        assert_eq!(
            script_inputs(handle, rx).await,
            vec![ScriptInput::Heard(pod())]
        );
        assert_eq!(
            lines
                .iter()
                .filter(|l| l["event"] == "listen_heard")
                .count(),
            1,
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    /// The window's end reaches the head too, and is what brings it down after a
    /// listening reply: the microphone is wake-gated again, so the promise the
    /// raised head was making is over.
    #[tokio::test]
    async fn the_windows_end_reaches_the_head() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        let (lines, _) = Harness::new()
            .scripter(handle.clone())
            .run(vec![PipelineItem::Listener(ListenerEvent::ListenExpired {
                pod: pod(),
                epoch: 1,
            })])
            .await;

        assert_eq!(
            script_inputs(handle, rx).await,
            vec![ScriptInput::ListenExpired(pod())]
        );
        assert_eq!(
            lines
                .iter()
                .filter(|l| l["event"] == "listen_expired")
                .count(),
            1,
            "and the line stands whatever the head did with it",
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    /// Both of the window's head-moving events sit past the epoch check, for the
    /// wake's reason: a superseded connection's room is not an interaction, and
    /// neither its speech may hold a head up nor its expiry bring one down.
    #[tokio::test]
    async fn a_stale_epoch_window_event_moves_no_head() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        Harness::new()
            .scripter(handle.clone())
            .run(vec![
                wake_detected(2, 8),
                PipelineItem::Listener(ListenerEvent::ListenHeard {
                    pod: pod(),
                    epoch: 1,
                }),
                PipelineItem::Listener(ListenerEvent::ListenExpired {
                    pod: pod(),
                    epoch: 1,
                }),
            ])
            .await;

        assert_eq!(
            script_inputs(handle, rx).await,
            vec![ScriptInput::Wake(pod())],
            "the live epoch's wake raises; the superseded window moves nothing",
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    /// The window's two accounting lines are lines and nothing else, so they are
    /// written whatever epoch they carry — dropping them would leave a window
    /// that did open, or did come back, unrecorded in the one log a reader has.
    /// Neither says anything to the head directly: the open is accounting, and
    /// the restore's only effect is the wall-clock release it arms, which a
    /// superseded connection's line does not get either.
    #[tokio::test(start_paused = true)]
    async fn a_stale_epoch_window_still_writes_its_line() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        let mut run = Harness::new()
            .scripter(handle.clone())
            .start(vec![
                wake_detected(2, 8),
                PipelineItem::Listener(ListenerEvent::ListenOpened {
                    pod: pod(),
                    epoch: 1,
                    deadline_sample: 128_000,
                }),
                PipelineItem::Listener(ListenerEvent::ListenRestored {
                    pod: pod(),
                    epoch: 1,
                    deadline_sample: 128_000,
                    at_sample: 112_000,
                }),
            ])
            .await;
        run.settle().await;
        // Past the restore's remaining second, had it armed anything.
        run.advance(Duration::from_secs(2)).await;
        let (lines, _) = run.finish().await;

        for name in ["listen_opened", "listen_restored"] {
            assert!(
                lines.iter().any(|l| l["event"] == name && l["epoch"] == 1),
                "a {name} line under the superseded epoch: {lines:?}",
            );
        }
        assert!(
            !lines.iter().any(|l| l["event"] == "listen_released"),
            "and the superseded restore arms no release: {lines:?}",
        );
        assert_eq!(
            script_inputs(handle, rx).await,
            vec![ScriptInput::Wake(pod())],
            "the live epoch's wake raises; the window's lines move no head",
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    // --- the window's wall-clock release ----------------------------------

    /// A restored window with `samples` left on it, and nothing else.
    fn listen_restored(epoch: u64, deadline_sample: u64, at_sample: u64) -> PipelineItem {
        PipelineItem::Listener(ListenerEvent::ListenRestored {
            pod: pod(),
            epoch,
            deadline_sample,
            at_sample,
        })
    }

    /// The headline quiet room: a cough inside the window was declined, the window
    /// came back, and then nothing more was ever said. The listener's clock stops
    /// with the audio, so the deadline passes unobserved — the surface's own wall
    /// clock is what brings the head down, at the deadline rather than at the
    /// engagement ceiling half a minute later.
    #[tokio::test(start_paused = true)]
    async fn a_restored_window_nobody_speaks_into_brings_the_head_down_at_its_deadline() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, mut rx) = crate::scripter::channel(jsonl.clone());
        // 8_000 samples left: 500 ms at the spine rate.
        let mut run = Harness::new()
            .scripter(handle.clone())
            .start(vec![listen_restored(1, 24_000, 16_000)])
            .await;
        run.settle().await;
        run.advance(Duration::from_millis(400)).await;
        assert!(
            rx.try_recv().is_none(),
            "the window's own time is not up yet",
        );
        run.advance(Duration::from_millis(200)).await;
        let (lines, _) = run.finish().await;

        assert_eq!(
            script_inputs(handle, rx).await,
            vec![ScriptInput::ListenExpired(pod())],
        );
        let released: Vec<&Value> = lines
            .iter()
            .filter(|l| l["event"] == "listen_released")
            .collect();
        assert_eq!(released.len(), 1, "released once: {lines:?}");
        assert_eq!(released[0]["pod"], "pod-x");
        assert_eq!(
            released[0]["deadline_sample"], 24_000,
            "joined to its listen_restored: {lines:?}"
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    /// Everything that says the window's ending belongs to somebody else cancels
    /// the release: speech inside it (the sample domain is live again), a mint (the
    /// window is spent and a verdict is owed), a wake hold (the hold's own ending
    /// takes over), and the listener saying it itself.
    #[tokio::test(start_paused = true)]
    async fn anything_that_owns_the_windows_ending_cancels_the_release() {
        for (what, cancel) in [
            (
                "speech inside the window",
                PipelineItem::Listener(ListenerEvent::ListenHeard {
                    pod: pod(),
                    epoch: 1,
                }),
            ),
            ("a mint", soft_endpoint(carved(1, 0, 16, None))),
            ("a wake hold", wake_held(1, 15_360, 111_360)),
            (
                "the listener's own expiry",
                PipelineItem::Listener(ListenerEvent::ListenExpired {
                    pod: pod(),
                    epoch: 1,
                }),
            ),
            (
                "a newer connection",
                PipelineItem::Listener(ListenerEvent::ListenExpired {
                    pod: pod(),
                    epoch: 2,
                }),
            ),
        ] {
            let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
            let (handle, rx) = crate::scripter::channel(jsonl.clone());
            let mut run = Harness::new()
                .scripter(handle.clone())
                .start(vec![listen_restored(1, 24_000, 16_000)])
                .await;
            run.settle().await;
            run.feed(cancel).await;
            run.advance(Duration::from_millis(600)).await;
            let (lines, _) = run.finish().await;

            assert!(
                !lines.iter().any(|l| l["event"] == "listen_released"),
                "{what} owns the ending: {lines:?}"
            );
            // The `ListenExpired` cases send the head one of their own, which is
            // the point: what must not happen is the release saying it twice.
            assert!(
                script_inputs(handle, rx)
                    .await
                    .iter()
                    .filter(|i| matches!(i, ScriptInput::ListenExpired(_)))
                    .count()
                    <= 1,
                "{what}: the head is told once",
            );
            drop(jsonl);
            writer.await.unwrap();
        }
    }

    /// A restore with the deadline already behind it arms nothing. The listener ran
    /// its own expiry check at that restore and kept the window open, so the room is
    /// not idle — audio is arriving and its clock is live. A zero-length arm would
    /// stow the head at once over an onset run or a resumed answer the carve will
    /// still accept.
    #[tokio::test(start_paused = true)]
    async fn a_restore_with_no_time_left_arms_nothing() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        let mut run = Harness::new()
            .scripter(handle.clone())
            .start(vec![listen_restored(1, 16_000, 24_000)])
            .await;
        run.settle().await;
        run.advance(Duration::from_secs(5)).await;
        let (lines, _) = run.finish().await;

        assert!(
            !lines.iter().any(|l| l["event"] == "listen_released"),
            "{lines:?}"
        );
        assert!(script_inputs(handle, rx).await.is_empty());
        drop(jsonl);
        writer.await.unwrap();
    }

    /// A wake word held inside the window, then the cough's decline landing under
    /// it. The hold is the interaction now and has an ending of its own, so the
    /// window's release stands down and the hold's release is the only thing the
    /// head hears.
    #[tokio::test(start_paused = true)]
    async fn a_restore_under_a_standing_hold_arms_nothing() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        // The hold's wait is 500 ms; the window's remaining time is 1 s, so a
        // release that armed would be distinguishable from the hold's.
        let mut run = Harness::new()
            .scripter(handle.clone())
            .start(vec![
                wake_detected(1, 15_360),
                wake_held(1, 15_360, 23_360),
                listen_restored(1, 32_000, 16_000),
            ])
            .await;
        run.settle().await;
        run.advance(Duration::from_millis(1_500)).await;
        let (lines, _) = run.finish().await;

        assert_eq!(
            script_inputs(handle, rx).await,
            vec![ScriptInput::Wake(pod()), ScriptInput::Unanswered(pod())],
            "the hold's own ending, and nothing from the window",
        );
        assert!(
            !lines.iter().any(|l| l["event"] == "listen_released"),
            "{lines:?}"
        );
        assert_eq!(
            lines
                .iter()
                .filter(|l| l["event"] == "wake_hold_released")
                .count(),
            1,
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    /// A replay or a brainless tuning run, as for the hold: no head to bring down,
    /// and a wall-clock timer over audio replayed faster than real time would be a
    /// fiction.
    #[tokio::test(start_paused = true)]
    async fn no_scripter_arms_no_window_release() {
        let mut run = Harness::new()
            .start(vec![listen_restored(1, 24_000, 16_000)])
            .await;
        run.settle().await;
        run.advance(Duration::from_millis(600)).await;
        let (lines, _) = run.finish().await;
        assert!(
            !lines.iter().any(|l| l["event"] == "listen_released"),
            "{lines:?}"
        );
    }

    /// The window's own lines and the mute's. Each is an interface: `listen_opened`
    /// tells a reader when the microphone was opened without a wake word and
    /// through which sample, `listen_restored` that a window an utterance closed
    /// came back because no turn came of that utterance, `listen_expired` that it
    /// is closed for good, and `wake_muted` that the phrase did fire and the mute
    /// is why nothing came of it. The names are pinned by the console's tables; the
    /// fields are pinned here.
    #[tokio::test]
    async fn the_window_and_the_mute_write_their_lines() {
        let (lines, _) = Harness::new()
            .run(vec![
                PipelineItem::Listener(ListenerEvent::ListenOpened {
                    pod: pod(),
                    epoch: 1,
                    deadline_sample: 128_000,
                }),
                PipelineItem::Listener(ListenerEvent::ListenRestored {
                    pod: pod(),
                    epoch: 1,
                    deadline_sample: 128_000,
                    at_sample: 96_000,
                }),
                PipelineItem::Listener(ListenerEvent::ListenExpired {
                    pod: pod(),
                    epoch: 1,
                }),
                PipelineItem::Listener(ListenerEvent::WakeMuted {
                    pod: pod(),
                    epoch: 1,
                    score: 0.87,
                    wake_end_sample: 4_096,
                }),
            ])
            .await;

        let found = |name: &str| {
            lines
                .iter()
                .find(|l| l["event"] == name)
                .unwrap_or_else(|| panic!("a {name} line: {lines:?}"))
                .clone()
        };
        let opened = found("listen_opened");
        assert_eq!(opened["pod"], pod().0);
        assert_eq!(opened["epoch"], 1);
        assert_eq!(opened["deadline_sample"], 128_000);
        let restored = found("listen_restored");
        assert_eq!(restored["pod"], pod().0);
        assert_eq!(restored["epoch"], 1);
        assert_eq!(
            restored["deadline_sample"], 128_000,
            "the deadline the window opened with, never re-dated"
        );
        assert_eq!(
            restored["at_sample"], 96_000,
            "and where the listener's clock stood, so a reader can tell what is left"
        );
        let expired = found("listen_expired");
        assert_eq!(expired["pod"], pod().0);
        assert_eq!(expired["epoch"], 1);
        let muted = found("wake_muted");
        assert_eq!(muted["pod"], pod().0);
        assert_eq!(muted["epoch"], 1);
        assert_eq!(muted["wake_end_sample"], 4_096);
        assert!((muted["score"].as_f64().unwrap() - 0.87).abs() < 1e-6);
    }

    /// The window is wake-less, so the confidence gate is all that stands
    /// between an open room and the brain. A follow-up that transcribes to
    /// hallucination is declined, the window it spent is handed back, and the
    /// head is left to that window — nothing was interrupted, so the brain
    /// hears no cut either.
    #[tokio::test]
    async fn a_gated_follow_up_declines_without_telling_the_brain_it_was_cut() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        let follow_up = CarvedUtterance {
            follow_up: true,
            ..carved(1, 0, 16, None)
        };
        let (feed, fed) = spy_listen_feed();
        let h = Harness::new()
            .brain()
            .scripter(handle.clone())
            .transcriber(FakeTranscriber(Some((
                "phantom".into(),
                Some(conf(0.37, -0.99)),
            ))))
            .gate(ConfidenceGate {
                no_speech_max: 0.2,
                avg_logprob_min: None,
            })
            .listen(feed, TEST_LISTEN_WINDOW);
        let nudges = h.nudges.clone();
        let stats = h.stats.clone();
        let events_seen = h.events.clone();
        let (lines, cmds) = h.run(vec![soft_endpoint(follow_up)]).await;

        assert!(cmds.is_empty(), "the phantom never reached the brain");
        assert_eq!(stats.snapshot().barge_command_absent, 1);
        assert!(
            nudges.lock().unwrap().barge_declined.is_empty(),
            "no reply was cut by a follow-up",
        );
        // The decline says which wake-less provenance it had: a reader tuning
        // barge thresholds off this event must not count a window's noise, and
        // under the mute no barge is even possible.
        assert!(
            matches!(
                events_seen.lock().unwrap().as_slice(),
                [BrainEvent::BargeCommandAbsent {
                    follow_up: true,
                    ..
                }]
            ),
            "declined as a follow-up: {:?}",
            events_seen.lock().unwrap()
        );
        assert!(
            lines
                .iter()
                .any(|l| l["event"] == "utterance" && l["follow_up"] == true),
            "and the utterance line carries the provenance: {lines:?}"
        );
        assert!(
            matches!(fed.lock().unwrap().as_slice(), [Feed::CandidateDeclined { id }] if *id == uid(1)),
            "the window the phantom spent is handed back",
        );
        assert!(
            script_inputs(handle, rx).await.is_empty(),
            "and the head is the restored window's to end, not this decline's",
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    /// The same follow-up, transcribing to something: it dispatches like any
    /// other utterance. The gate declines hallucinations, not follow-ups.
    #[tokio::test]
    async fn a_clean_follow_up_dispatches() {
        let follow_up = CarvedUtterance {
            follow_up: true,
            ..carved(1, 0, 16, None)
        };
        let (lines, cmds) = Harness::new()
            .brain()
            .transcriber(FakeTranscriber(Some((
                "and another thing".into(),
                Some(conf(0.01, -0.2)),
            ))))
            .gate(ConfidenceGate {
                no_speech_max: 0.2,
                avg_logprob_min: None,
            })
            .run(vec![soft_endpoint(follow_up)])
            .await;

        assert_eq!(cmds.len(), 1, "the follow-up is answered");
        // The only per-utterance record that this turn reached the brain with no
        // wake word behind it.
        let utterance = lines
            .iter()
            .find(|l| l["event"] == "utterance")
            .unwrap_or_else(|| panic!("an utterance line: {lines:?}"));
        assert_eq!(utterance["follow_up"], true);
    }

    /// The mirror: an ordinary wake-gated turn is not a follow-up, so the line
    /// that distinguishes the two cannot be a constant.
    #[tokio::test]
    async fn a_wake_gated_utterance_is_not_a_follow_up_on_the_line() {
        let (lines, cmds) = Harness::new()
            .brain()
            .transcriber(FakeTranscriber(Some(("hello world".into(), None))))
            .run(vec![
                wake_detected(1, 8),
                soft_endpoint(carved(1, 0, 16, None)),
            ])
            .await;

        assert_eq!(cmds.len(), 1);
        let utterance = lines
            .iter()
            .find(|l| l["event"] == "utterance")
            .unwrap_or_else(|| panic!("an utterance line: {lines:?}"));
        assert_eq!(utterance["follow_up"], false);
    }

    /// One carve, both marks: speech that began inside a `<listen/>` window and
    /// went on to cut the reply it drew. The listener produces exactly this
    /// shape (`a_follow_up_that_cuts_its_own_reply_carries_both_marks`), and the
    /// gate classifies it once — as a barge, under either decline, because the
    /// cut is what the decline has to answer for. A transcript that passes still
    /// dispatches, which is the arm the provenance must not swallow.
    #[tokio::test]
    async fn a_follow_up_that_cut_the_reply_is_declined_as_a_barge() {
        enum Said {
            Nothing,
            Phantom,
            Something,
        }

        for (said, transcriber) in [
            (Said::Nothing, says("", None)),
            (Said::Phantom, says("phantom", Some(conf(0.97, -1.5)))),
            (Said::Something, says("yes please", Some(conf(0.01, -0.2)))),
        ] {
            let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
            let (handle, rx) = crate::scripter::channel(jsonl.clone());
            let both = CarvedUtterance {
                follow_up: true,
                ..barged(1, 0, 16)
            };
            let (feed, fed) = spy_listen_feed();
            let h = Harness::new()
                .brain()
                .scripter(handle.clone())
                .transcriber(transcriber)
                .gate(ConfidenceGate {
                    no_speech_max: 0.2,
                    avg_logprob_min: None,
                })
                .listen(feed, TEST_LISTEN_WINDOW);
            let nudges = h.nudges.clone();
            let events_seen = h.events.clone();
            let stats = h.stats.clone();
            let (lines, cmds) = h.run(vec![soft_endpoint(both)]).await;
            let dispatched = lines.iter().any(|l| l["event"] == "brain_dispatched");
            let cut_notices = nudges.lock().unwrap().barge_declined.len();
            let handed_back = fed.lock().unwrap().clone();
            let inputs = script_inputs(handle, rx).await;
            let events_seen = events_seen.lock().unwrap().clone();

            match said {
                Said::Nothing | Said::Phantom => {
                    assert!(cmds.is_empty(), "the gate declined: {lines:?}");
                    assert!(!dispatched, "and no dispatch was announced: {lines:?}");
                    assert_eq!(cut_notices, 1, "the brain is owed word of the cut");
                    assert_eq!(
                        inputs,
                        vec![ScriptInput::Unanswered(pod())],
                        "and the head settles: the reply it was up for will not finish",
                    );
                    assert!(
                        matches!(
                            handed_back.as_slice(),
                            [Feed::CandidateDeclined { id }] if *id == uid(1)
                        ),
                        "the candidate goes back whatever the decline's reason. \
                         Whether a window comes back is the listener's call, and \
                         in this room it does not: the reply this speech cut took \
                         the spent window when it started: {handed_back:?}",
                    );
                }
                Said::Something => {
                    assert_eq!(cmds.len(), 1, "a barge that said something is answered");
                    assert!(dispatched, "and dispatched as one: {lines:?}");
                    assert_eq!(cut_notices, 0, "nothing was declined to report");
                    assert!(handed_back.is_empty(), "{handed_back:?}");
                    // This carve's provenance is `Barge`, which the settle rule
                    // names; only the dispatch keeps the head off the settle.
                    // A barge that said something is answered, so the turn owns
                    // the head and nothing settles.
                    assert_eq!(
                        inputs,
                        vec![
                            ScriptInput::TurnStarted {
                                pod: pod(),
                                turn: UtteranceId(1),
                            },
                            ScriptInput::TurnEnded {
                                pod: pod(),
                                turn: UtteranceId(1),
                                end: TurnEnd::Closed,
                            },
                        ],
                        "the turn brackets the head and nothing settles",
                    );
                }
            }
            match said {
                Said::Nothing => {
                    assert!(
                        matches!(events_seen.as_slice(), [BrainEvent::NoTranscript { .. }]),
                        "no text is no turn, whatever cut what: {events_seen:?}",
                    );
                    assert_eq!(stats.snapshot().no_transcript, 1);
                }
                Said::Phantom => {
                    assert!(
                        matches!(
                            events_seen.as_slice(),
                            [BrainEvent::BargeCommandAbsent {
                                follow_up: false,
                                ..
                            }]
                        ),
                        "reported as the barge it was, not as the window it began \
                         in — the reader's repair is the barge thresholds: \
                         {events_seen:?}",
                    );
                    assert_eq!(stats.snapshot().barge_command_absent, 1);
                    assert!(
                        lines
                            .iter()
                            .any(|l| l["event"] == "utterance" && l["follow_up"] == true),
                        "the two lines answer different questions about one id: the \
                         `utterance` line says where the speech began, the decline \
                         says what it did, and this carve is the one where they \
                         disagree: {lines:?}",
                    );
                }
                Said::Something => assert!(events_seen.is_empty(), "{events_seen:?}"),
            }

            drop(jsonl);
            writer.await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_gated_barge_tells_the_brain_its_response_was_cut() {
        // The playback is already gone and `handle` never runs, so this is the brain's
        // one chance to hear about the cut — with the interrupted turn attached.
        let ledger = Arc::new(TurnLedger::new());
        ledger.interrupt(&pod(), UtteranceId(99), cut(100, 1_000));
        let barge_carve = barged(1, 0, 16);
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
        let nudges = h.nudges.clone();
        h.run(vec![soft_endpoint(barge_carve)]).await;

        assert_eq!(
            nudges.lock().unwrap().barge_declined,
            [(UtteranceId(1), Some(UtteranceId(99)))]
        );
    }

    #[tokio::test]
    async fn a_gated_wake_and_a_plain_dispatch_report_no_declined_barge() {
        // The two arms are disjoint: a wake utterance carries no barge, and a
        // dispatched one reaches `handle`, which reports the interruption itself.
        let wake = Some(WakeConfirmation {
            score: 0.99,
            wake_end_sample: 8,
            stt_trim_samples: 0,
        });
        for (carve, transcript, dispatches) in [
            (
                carved(1, 0, 16, wake),
                ("phantom".into(), Some(conf(0.37, -0.99))),
                false,
            ),
            (carved(1, 0, 16, None), ("hello there".into(), None), true),
        ] {
            let h = Harness::new()
                .brain()
                .transcriber(FakeTranscriber(Some(transcript)))
                .gate(ConfidenceGate {
                    no_speech_max: 0.2,
                    avg_logprob_min: None,
                });
            let nudges = h.nudges.clone();
            let stats = h.stats.clone();
            let (_lines, cmds) = h.run(vec![soft_endpoint(carve)]).await;

            // Each arm asserts it reached the path it is named for first: an
            // absence proves nothing about an input that never got there.
            if dispatches {
                assert_eq!(cmds.len(), 1, "the plain utterance reached `handle`");
            } else {
                assert!(cmds.is_empty(), "the gated wake never dispatches");
                assert_eq!(stats.snapshot().wake_command_absent, 1);
            }
            assert!(nudges.lock().unwrap().barge_declined.is_empty());
        }
    }

    // --- the gate's emptiness test ----------------------------------------

    /// A transcriber that answers with the given text and confidence — the
    /// empty-transcript cases say `""` or whitespace.
    fn says(text: &str, confidence: Option<TranscriptConfidence>) -> FakeTranscriber {
        FakeTranscriber(Some((text.into(), confidence)))
    }

    #[tokio::test]
    async fn an_empty_transcript_never_reaches_the_brain() {
        // The one rule, before any provenance is read: no text is no turn. The
        // dispatch's own bookkeeping — the console line, `TurnStarted` — must not
        // happen either, or the head raises for a turn that never was.
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        let h = Harness::new()
            .brain()
            .scripter(handle.clone())
            .transcriber(says("   \n ", None));
        let stats = h.stats.clone();
        let (lines, cmds) = h.run(vec![soft_endpoint(carved(1, 0, 16, None))]).await;

        assert!(cmds.is_empty(), "nothing was said, so nothing was answered");
        assert!(
            !lines.iter().any(|l| l["event"] == "brain_dispatched"),
            "no dispatch line for a candidate that produced no turn: {lines:?}"
        );
        assert_eq!(stats.snapshot().no_transcript, 1);
        assert!(
            !script_inputs(handle, rx)
                .await
                .iter()
                .any(|i| matches!(i, ScriptInput::TurnStarted { .. })),
            "the head never raises for a turn that was not started",
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn an_empty_wake_is_command_absent_under_every_brain() {
        // The wake word fired and nothing followed. Reported through the wake
        // provenance — the score and the audio span are what a retro-transcription
        // pass needs — never as a bare `NoTranscript`. The head was raised by the
        // wake and no turn is coming, so the settle starts here.
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        let wake = Some(WakeConfirmation {
            score: 0.99,
            wake_end_sample: 8,
            stt_trim_samples: 0,
        });
        let h = Harness::new()
            .brain()
            .scripter(handle.clone())
            .transcriber(says("", None));
        let events_seen = h.events.clone();
        let stats = h.stats.clone();
        let (_lines, cmds) = h.run(vec![soft_endpoint(carved(1, 0, 16, wake))]).await;

        assert!(cmds.is_empty());
        assert!(
            matches!(
                events_seen.lock().unwrap().as_slice(),
                [BrainEvent::WakeCommandAbsent {
                    reason: WakeCommandReason::Empty,
                    score: 0.99,
                    ..
                }]
            ),
            "reported through the wake provenance: {:?}",
            events_seen.lock().unwrap()
        );
        assert_eq!(stats.snapshot().wake_command_absent, 1);
        assert_eq!(stats.snapshot().no_transcript, 0);
        assert_eq!(
            script_inputs(handle, rx).await,
            vec![ScriptInput::Unanswered(pod())],
            "a raise that produced no turn folds a linger from here",
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn an_empty_barge_still_tells_the_brain_its_response_was_cut() {
        // The reply is already cut and `handle` will never run, so the notice here
        // is the peer's only way to learn it was interrupted — the same obligation
        // the confidence decline carries, now for speech that said nothing at all.
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        let ledger = Arc::new(TurnLedger::new());
        ledger.interrupt(&pod(), UtteranceId(99), cut(100, 1_000));
        let barge_carve = barged(1, 0, 16);
        let h = Harness::new()
            .brain()
            .scripter(handle.clone())
            .transcriber(says(" ", None))
            .barge(Arc::clone(&ledger), Err(FlushRejected::NotPlaying));
        let nudges = h.nudges.clone();
        let events_seen = h.events.clone();
        let stats = h.stats.clone();
        let (_lines, cmds) = h.run(vec![soft_endpoint(barge_carve)]).await;

        assert!(cmds.is_empty());
        assert_eq!(
            nudges.lock().unwrap().barge_declined,
            [(UtteranceId(1), Some(UtteranceId(99)))]
        );
        assert!(
            matches!(
                events_seen.lock().unwrap().as_slice(),
                [BrainEvent::NoTranscript { .. }]
            ),
            "a barge carries no wake provenance to report it through: {:?}",
            events_seen.lock().unwrap()
        );
        assert_eq!(stats.snapshot().no_transcript, 1);
        assert_eq!(
            script_inputs(handle, rx).await,
            vec![ScriptInput::Unanswered(pod())],
            "the barge raised the head and nothing came of it",
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn an_empty_echo_of_the_pods_own_reply_moves_no_head() {
        // Audio over the pod's own playback that said nothing. Nobody raised for
        // it, and the reply whose echo it is still owns the head — `Unanswered`
        // here would cut that reply's own script short.
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        let echo = CarvedUtterance {
            over_playback: true,
            ..carved(1, 0, 16, None)
        };
        let h = Harness::new()
            .brain()
            .scripter(handle.clone())
            .transcriber(says("", None));
        let stats = h.stats.clone();
        let (_lines, cmds) = h.run(vec![soft_endpoint(echo)]).await;

        assert!(cmds.is_empty());
        assert_eq!(stats.snapshot().no_transcript, 1);
        assert_eq!(
            stats.snapshot().echo_declined,
            0,
            "the emptiness test decided it, not the overlap arm"
        );
        assert_eq!(
            script_inputs(handle, rx).await,
            vec![],
            "the reply's own script is live and untouched",
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn an_empty_follow_up_leaves_the_head_to_the_window() {
        // Speech inside a `<listen/>` window that transcribed to nothing. The head
        // is up because the microphone is open; the window's own ending is what
        // brings it down, so the decline says nothing to the scripter.
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        let follow_up = CarvedUtterance {
            follow_up: true,
            ..carved(1, 0, 16, None)
        };
        let (feed, fed) = spy_listen_feed();
        let h = Harness::new()
            .brain()
            .scripter(handle.clone())
            .listen(feed, TEST_LISTEN_WINDOW)
            .transcriber(says("", None));
        let stats = h.stats.clone();
        let (lines, cmds) = h.run(vec![soft_endpoint(follow_up)]).await;

        assert!(cmds.is_empty());
        assert_eq!(stats.snapshot().no_transcript, 1);
        assert_eq!(
            stats.snapshot().barge_command_absent,
            0,
            "an empty follow-up is not a confidence decline"
        );
        assert!(
            matches!(fed.lock().unwrap().as_slice(), [Feed::CandidateDeclined { id }] if *id == uid(1)),
            "and the window the noise spent is handed back — the whole point: {:?}",
            fed.lock().unwrap(),
        );
        assert!(
            lines
                .iter()
                .any(|l| l["event"] == "utterance" && l["follow_up"] == true),
            "the utterance line still carries the provenance: {lines:?}"
        );
        assert_eq!(script_inputs(handle, rx).await, vec![]);
        drop(jsonl);
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn an_empty_carve_in_a_quiet_room_starts_the_settle() {
        // A bypassed wake gate mints every carve with no wake, no barge and no
        // follow-up mark. Nothing else will end this engagement, so the decline
        // has to: otherwise the head sits at its ceiling over a room gone quiet.
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        let h = Harness::new()
            .brain()
            .scripter(handle.clone())
            .transcriber(says("", None));
        let stats = h.stats.clone();
        let (_lines, cmds) = h.run(vec![soft_endpoint(carved(1, 0, 16, None))]).await;

        assert!(cmds.is_empty());
        assert_eq!(stats.snapshot().no_transcript, 1);
        assert_eq!(
            script_inputs(handle, rx).await,
            vec![ScriptInput::Unanswered(pod())]
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    /// The whole gate contract for a `WakePolicy::Bypass` deployment, where the
    /// listener mints every carve with no wake, no barge and no overlap: such a
    /// carve is never gated, whatever STT thought of its own transcript. If this
    /// regresses, the pod declines all speech and answers nothing, and the
    /// symptom reads as an STT fault rather than a gate one.
    #[tokio::test]
    async fn a_bypassed_carve_is_never_gated_on_confidence() {
        let h = Harness::new()
            .brain()
            .transcriber(says("phantom", Some(conf(0.97, -1.5))))
            .gate(ConfidenceGate {
                no_speech_max: 0.2,
                avg_logprob_min: None,
            });
        let stats = h.stats.clone();
        let (lines, cmds) = h.run(vec![soft_endpoint(carved(1, 0, 16, None))]).await;

        assert_eq!(cmds.len(), 1, "the carve reached `handle`: {lines:?}");
        assert!(
            lines.iter().any(|l| l["event"] == "brain_dispatched"),
            "and was announced as a turn: {lines:?}"
        );
        let snap = stats.snapshot();
        assert_eq!(
            (
                snap.wake_command_absent,
                snap.barge_command_absent,
                snap.echo_declined,
                snap.no_transcript
            ),
            (0, 0, 0, 0),
            "a score the gate would reject declines nothing here",
        );
    }

    /// A `[brain]` with no `[stt]`: a deployment shape an operator can still
    /// write, and the one the gate's rule decided against validating against —
    /// the reporting is supposed to make it self-evident instead. Every utterance
    /// carries a null transcript, so every one is declined, no brain is ever
    /// called, and each decline is a `brain_no_transcript` line with its window
    /// handed back.
    #[tokio::test]
    async fn a_brain_with_no_transcriber_answers_nothing() {
        let (feed, fed) = spy_listen_feed();
        let h = Harness::new().brain().listen(feed, TEST_LISTEN_WINDOW);
        let stats = h.stats.clone();
        let events_seen = h.events.clone();
        let (lines, cmds) = h.run(vec![soft_endpoint(carved(1, 0, 16, None))]).await;

        assert!(cmds.is_empty(), "nothing was transcribed to answer");
        assert!(
            !lines.iter().any(|l| l["event"] == "brain_dispatched"),
            "the brain is never called: {lines:?}"
        );
        assert_eq!(stats.snapshot().no_transcript, 1);
        assert!(
            matches!(
                events_seen.lock().unwrap().as_slice(),
                [BrainEvent::NoTranscript { .. }]
            ),
            "and the per-utterance line says why: {:?}",
            events_seen.lock().unwrap()
        );
        assert!(
            matches!(fed.lock().unwrap().as_slice(), [Feed::CandidateDeclined { id }] if *id == uid(1)),
            "the candidate became no turn: {:?}",
            fed.lock().unwrap(),
        );
    }

    #[tokio::test]
    async fn empty_text_carrying_a_gate_tripping_score_is_declined_as_empty() {
        // STT returned nothing and scored its own silence badly. The emptiness
        // test runs first, so this is a `NoTranscript` — a confidence decline
        // would claim a hallucination where there was no text to hallucinate.
        let follow_up = CarvedUtterance {
            follow_up: true,
            ..carved(1, 0, 16, None)
        };
        let h = Harness::new()
            .brain()
            .transcriber(says("   ", Some(conf(0.97, -1.5))))
            .gate(ConfidenceGate {
                no_speech_max: 0.2,
                avg_logprob_min: None,
            });
        let events_seen = h.events.clone();
        let stats = h.stats.clone();
        let (_lines, cmds) = h.run(vec![soft_endpoint(follow_up)]).await;

        assert!(cmds.is_empty());
        assert_eq!(stats.snapshot().no_transcript, 1);
        assert_eq!(stats.snapshot().barge_command_absent, 0);
        assert!(
            matches!(
                events_seen.lock().unwrap().as_slice(),
                [BrainEvent::NoTranscript { .. }]
            ),
            "the emptiness test got there first: {:?}",
            events_seen.lock().unwrap()
        );
    }

    // --- handing a declined candidate back to the listener ----------------

    /// Run one carve through the gate with a spy listener feed wired, and return
    /// what the listener was told. The confidence gate is set to reject the
    /// phantom text the low-confidence cases transcribe to, so the case chooses
    /// its outcome by what it says rather than by how it is wired.
    async fn fed_by_the_gate(carve: CarvedUtterance, transcriber: FakeTranscriber) -> Vec<Feed> {
        let (feed, fed) = spy_listen_feed();
        Harness::new()
            .brain()
            .transcriber(transcriber)
            .gate(ConfidenceGate {
                no_speech_max: 0.2,
                avg_logprob_min: None,
            })
            .listen(feed, TEST_LISTEN_WINDOW)
            .run(vec![soft_endpoint(carve)])
            .await;
        let fed = fed.lock().unwrap();
        fed.clone()
    }

    /// Every gate outcome that is not a dispatch says so to the listener, in the
    /// same words and whatever the candidate's provenance: this became no turn.
    /// The pipeline knows what the gate did and nothing about windows; the
    /// listener knows what window the candidate spent and nothing about the gate,
    /// and it is the one that decides whether anything comes back.
    #[tokio::test]
    async fn every_decline_hands_the_candidate_back_to_the_listener() {
        let wake = Some(WakeConfirmation {
            score: 0.99,
            wake_end_sample: 8,
            stt_trim_samples: 0,
        });
        let phantom = || says("phantom", Some(conf(0.97, -1.5)));
        let marked = |mark: fn(&mut CarvedUtterance)| {
            let mut c = carved(1, 0, 16, None);
            mark(&mut c);
            c
        };

        for (what, carve, transcriber) in [
            (
                "an empty transcript",
                carved(1, 0, 16, None),
                says("", None),
            ),
            ("a low-confidence wake", carved(1, 0, 16, wake), phantom()),
            ("a low-confidence barge", barged(1, 0, 16), phantom()),
            (
                "a low-confidence follow-up",
                marked(|c| c.follow_up = true),
                phantom(),
            ),
            (
                "an echo of the pod's own reply",
                marked(|c| c.over_playback = true),
                phantom(),
            ),
        ] {
            let fed = fed_by_the_gate(carve, transcriber).await;
            assert!(
                matches!(fed.as_slice(), [Feed::CandidateDeclined { id }] if *id == uid(1)),
                "{what} is handed back by its own id: {fed:?}"
            );
        }
    }

    /// The other half of the same rule, and the one only a negative assertion can
    /// hold: a candidate that *became* a turn spent the window it was carved in,
    /// and nothing tells the listener otherwise.
    #[tokio::test]
    async fn a_dispatched_candidate_is_never_handed_back() {
        let fed = fed_by_the_gate(
            carved(1, 0, 16, None),
            says("hello world", Some(conf(0.01, -0.2))),
        )
        .await;

        assert!(fed.is_empty(), "the turn keeps the window: {fed:?}");
    }

    // --- the cue tap ------------------------------------------------------

    /// The vocabulary the cue cases resolve against: one cueable pose at 800 ms,
    /// the stow no reply may command, and a motion with its own blend-out.
    fn cue_library() -> CueLibrary {
        CueLibrary::parse(
            r#"{
              "poses": [
                { "name": "peek", "duration_ms": 800 },
                { "name": "stow", "duration_ms": 2000 }
              ],
              "motions": [
                { "name": "bench/nod", "duration_ms": 1000, "blend_out_ms": 200 }
              ]
            }"#,
        )
        .expect("the fixture sidecar parses")
    }

    /// A speed factor becomes the absolute pace the wire carries, and unit speed
    /// says nothing at all — the library's own pace, which is what a presence
    /// raise states too.
    #[test]
    fn a_cued_poses_speed_becomes_the_pace_the_wire_carries() {
        let library = cue_library();
        let paced = |speed: Option<f64>| match resolve_cue(
            &library,
            &Cue::Pose {
                name: "peek".into(),
                speed,
            },
        ) {
            Ok(MotionCue::Pose(raise)) => raise.move_ms,
            other => panic!("a pose resolves to a pose: {other:?}"),
        };
        assert_eq!(paced(Some(2.0)), Some(400), "twice as fast is half as long");
        assert_eq!(paced(Some(0.25)), Some(3_200));
        assert_eq!(paced(Some(1.0)), None, "unit speed states no pace");
        assert_eq!(paced(None), None);
    }

    /// The blend-out is the overlay's own exit ramp and runs on the wall clock:
    /// speeding a motion up must not shorten the fade that ends it.
    #[test]
    fn a_cued_motions_span_is_the_library_duration_at_speed_plus_its_blend_out() {
        let library = cue_library();
        let span = |speed: Option<f64>| match resolve_cue(
            &library,
            &Cue::Motion {
                name: "bench/nod".into(),
                speed,
            },
        ) {
            Ok(MotionCue::Motion { play, span_ms }) => (play.speed, span_ms),
            other => panic!("a motion resolves to a motion: {other:?}"),
        };
        assert_eq!(span(Some(0.5)), (0.5, 2_200));
        assert_eq!(span(None), (1.0, 1_200));
        assert_eq!(span(Some(2.0)), (2.0, 700));
    }

    /// Every way a cue can fail to resolve, and the reason each reports. A
    /// refused cue is dropped rather than corrected: the nearest movement the
    /// reply did not ask for is not an improvement.
    #[test]
    fn a_cue_this_deployment_cannot_make_is_refused_with_its_reason() {
        let library = cue_library();
        let pose = |name: &str, speed: Option<f64>| {
            resolve_cue(
                &library,
                &Cue::Pose {
                    name: name.into(),
                    speed,
                },
            )
            .expect_err("refused")
        };
        assert_eq!(pose("stow", None), "stow_not_cueable");
        assert_eq!(pose("nowhere", None), "unknown_pose");
        assert_eq!(pose("peek", Some(0.1)), "speed_out_of_range");
        assert_eq!(pose("peek", Some(2.5)), "speed_out_of_range");
        assert_eq!(pose("peek", Some(f64::NAN)), "speed_out_of_range");
        assert_eq!(pose("peek", Some(f64::INFINITY)), "speed_out_of_range");
        assert_eq!(
            resolve_cue(
                &library,
                &Cue::Motion {
                    name: "bench/perk".into(),
                    speed: None,
                },
            )
            .expect_err("refused"),
            "unknown_motion",
        );
    }

    /// The wire's ceiling, on both derived numbers. A library copy is edited by
    /// hand and can name a duration this deployment has never played; the pose's
    /// pace and the motion's span are both made here, and a number past the
    /// wire's bound is refused here rather than reaching a script the wire
    /// cannot build.
    #[test]
    fn a_cue_whose_derived_span_or_pace_passes_the_wires_ceiling_is_refused() {
        let library = CueLibrary::parse(
            r#"{
              "poses": [{ "name": "slow", "duration_ms": 400000 }],
              "motions": [{ "name": "bench/epic", "duration_ms": 400000,
                            "blend_out_ms": 200 }]
            }"#,
        )
        .expect("the fixture sidecar parses");
        assert_eq!(
            resolve_cue(
                &library,
                &Cue::Pose {
                    name: "slow".into(),
                    speed: Some(0.25),
                },
            )
            .expect_err("refused"),
            "move_too_long",
        );
        assert_eq!(
            resolve_cue(
                &library,
                &Cue::Motion {
                    name: "bench/epic".into(),
                    speed: Some(0.25),
                },
            )
            .expect_err("refused"),
            "motion_too_long",
        );
        // The exact edge, on the two sides of one millisecond: the render puts the
        // play at `after_ms` 1, so the longest span a script can carry is one less
        // than the wire's ceiling. The scripter's own timeout is capped at that
        // same ceiling, and this is the one input where the two rules meet — an
        // off-by-one in either turns a legal cue into a panic in the script task.
        let edge = |duration_ms: u64| {
            let library = CueLibrary::parse(&format!(
                r#"{{ "poses": [], "motions": [{{ "name": "edge", "duration_ms": {duration_ms},
                      "blend_out_ms": 0 }}] }}"#
            ))
            .expect("the fixture sidecar parses");
            resolve_cue(
                &library,
                &Cue::Motion {
                    name: "edge".into(),
                    speed: None,
                },
            )
        };
        assert_eq!(
            edge(MAX_TIMEOUT_MS).expect_err("refused"),
            "motion_too_long",
            "a span at the ceiling leaves no room for the play's own step",
        );
        assert!(
            matches!(
                edge(MAX_TIMEOUT_MS - 1).expect("admitted"),
                MotionCue::Motion { span_ms, .. } if span_ms == MAX_TIMEOUT_MS - 1
            ),
            "and one millisecond under it is the longest motion the wire carries",
        );

        // And the same library at unit speed is ordinary: the bound is on the
        // number, not on the entry.
        assert!(
            resolve_cue(
                &library,
                &Cue::Motion {
                    name: "bench/epic".into(),
                    speed: None,
                },
            )
            .is_ok()
        );
    }

    /// A reply's movements reach the head as one input, resolved: the name
    /// checked against the library and the speed already turned into the
    /// numbers the scripter puts on the wire.
    #[tokio::test]
    async fn a_replys_cues_reach_the_head_resolved() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        Harness::new()
            .transcriber(FakeTranscriber(Some(("hello world".into(), None))))
            .brain()
            .scripter(handle.clone())
            .cues(cue_library())
            .reply_cues(vec![
                Cue::Pose {
                    name: "peek".into(),
                    speed: Some(2.0),
                },
                Cue::Motion {
                    name: "bench/nod".into(),
                    speed: None,
                },
            ])
            .run(vec![
                wake_detected(1, 8),
                soft_endpoint(carved(1, 0, 16, None)),
            ])
            .await;

        let cues = script_inputs(handle, rx)
            .await
            .into_iter()
            .find_map(|input| match input {
                ScriptInput::Cues { cues, .. } => Some(cues),
                _ => None,
            })
            .expect("the reply's movements reached the head");
        assert_eq!(
            cues,
            vec![
                MotionCue::Pose(Raise {
                    pose: "peek".into(),
                    move_ms: Some(400),
                }),
                MotionCue::Motion {
                    play: Play::new("bench/nod"),
                    span_ms: 1_200,
                },
            ],
            "in the order the reply named them",
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    /// A name this deployment does not hold never reaches the wire: an
    /// unresolvable name makes the daemon refuse the whole script it rides in,
    /// so the other movements in the same reply would go with it.
    #[tokio::test]
    async fn an_invented_name_is_refused_at_the_tap_and_the_rest_still_moves() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        let (lines, _cmds) = Harness::new()
            .transcriber(FakeTranscriber(Some(("hello world".into(), None))))
            .brain()
            .scripter(handle.clone())
            .cues(cue_library())
            .reply_cues(vec![
                Cue::Motion {
                    name: "invented/flourish".into(),
                    speed: None,
                },
                Cue::Pose {
                    name: "peek".into(),
                    speed: None,
                },
            ])
            .run(vec![
                wake_detected(1, 8),
                soft_endpoint(carved(1, 0, 16, None)),
            ])
            .await;

        let refused = lines
            .iter()
            .find(|v| v["event"] == "cue_refused")
            .expect("the refusal is narrated");
        assert_eq!(refused["name"], "invented/flourish");
        assert_eq!(refused["kind"], "motion");
        assert_eq!(refused["reason"], "unknown_motion");
        assert_eq!(refused["utterance"], 1);

        let cues = script_inputs(handle, rx)
            .await
            .into_iter()
            .find_map(|input| match input {
                ScriptInput::Cues { cues, .. } => Some(cues),
                _ => None,
            })
            .expect("what did resolve still reached the head");
        assert_eq!(
            cues,
            vec![MotionCue::Pose(Raise {
                pose: "peek".into(),
                move_ms: None,
            })],
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    /// With no vocabulary configured nothing can move the head: content moves it
    /// only where the operator has said what it may be moved to. It is said out
    /// loud, because the operator's own edit is what produced it and the marker
    /// never appears in the speech to hint at what went missing.
    #[tokio::test]
    async fn a_reply_cues_nothing_when_no_library_is_configured() {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, rx) = crate::scripter::channel(jsonl.clone());
        let (lines, _cmds) = Harness::new()
            .transcriber(FakeTranscriber(Some(("hello world".into(), None))))
            .brain()
            .scripter(handle.clone())
            .reply_cues(vec![Cue::Pose {
                name: "peek".into(),
                speed: None,
            }])
            .run(vec![
                wake_detected(1, 8),
                soft_endpoint(carved(1, 0, 16, None)),
            ])
            .await;

        let refused = lines
            .iter()
            .find(|v| v["event"] == "cue_refused")
            .expect("the missing vocabulary is narrated");
        assert_eq!(refused["name"], "peek");
        assert_eq!(refused["kind"], "pose");
        assert_eq!(refused["reason"], "no_library");

        assert!(
            !script_inputs(handle, rx)
                .await
                .iter()
                .any(|input| matches!(input, ScriptInput::Cues { .. })),
            "no library, no movement",
        );
        drop(jsonl);
        writer.await.unwrap();
    }

    /// A deployment with a vocabulary but no head is the other half of the same
    /// misconfiguration, and reads the same way in the log.
    #[tokio::test]
    async fn a_reply_cues_nothing_when_no_head_is_scripted() {
        let (lines, _cmds) = Harness::new()
            .transcriber(FakeTranscriber(Some(("hello world".into(), None))))
            .brain()
            .cues(cue_library())
            .reply_cues(vec![Cue::Motion {
                name: "bench/nod".into(),
                speed: None,
            }])
            .run(vec![
                wake_detected(1, 8),
                soft_endpoint(carved(1, 0, 16, None)),
            ])
            .await;

        let refused = lines
            .iter()
            .find(|v| v["event"] == "cue_refused")
            .expect("the missing head is narrated");
        assert_eq!(refused["name"], "bench/nod");
        assert_eq!(refused["kind"], "motion");
        assert_eq!(refused["reason"], "no_head");
    }
}
