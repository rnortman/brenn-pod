//! The tokio server: an accept loop plus one ingest task per pod connection.
//!
//! Each connection task owns the socket read half, a per-connection
//! `FrameLogWriter` (the recorder tap, written *before* decode so a decode bug
//! can never corrupt what was captured), a `SessionFsm`, and a
//! `SegmentAssembler`. It reads length-prefixed wire frames, taps them to the
//! frame log, decodes, feeds the FSM, and folds the resulting events into
//! `Segment`s that flow onto the drop-oldest queue the pipeline task drains.
//!
//! Backpressure is TCP backpressure by construction: the assembler runs inline
//! on the read task, so a stalled downstream stalls the socket read rather than
//! dropping mid-segment audio.
//!
//! Recording never gates the data plane. A recorder I/O error latches recording
//! off process-wide (a loud `record_error` line) and the pipeline continues; a
//! rename failure is narrower — capture continues under the connection-scoped
//! name, since capture continuity beats naming.

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use audio_pipeline::wire::{decode_frame, MAX_FRAME_BYTES};
use serde::Serialize;
use serde_json::json;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use futures::channel::mpsc;
use pod_ingest::{
    CloseCause, CrossCheck, HostMicros, ResumeLedger, SegmentRef, SessionEvent, SessionFsm,
};
use speech_pipeline::{
    stage_delta_us, AssemblerLimits, Brain, BrainEvent, BrainEventFn, BrainStats,
    BrainStatsSnapshot, BuildError, ConfidenceGate, DropOldestQueue, EchoBrain, FeedSender,
    FlushRejected, HttpSynthesizer, HttpTranscriber, InterruptProgress, Listener, ListenerConfig,
    ListenerEvent, ListenerHandle, ListenerStats, ListenerStatsSnapshot, OwwModels, PacerConfig,
    PlayRejected, PlaybackEventFn, PlaybackHandle, PlaybackJob, PlaybackStats,
    PlaybackStatsSnapshot, PlaybackWriter, PodId, QueueStats, RoomId, Segment, SegmentAssembler,
    SegmentEndCause, Sender, SileroModel, SpeakCmd, StageTimings, StatsHandle, SttParams, SttStats,
    SttStatsSnapshot, Synthesizer, Transcriber, TtsParams, TtsStats, TtsStatsSnapshot, UtteranceId,
    WakeCommandReason, WakeError, WavBrain, SPINE_FORMAT,
};

use crate::barge::TurnLedger;
use crate::clip::{load_clip, ClipError};
use crate::config::{BrainMode, Config, SttBackend, SttConfig, TtsBackend};
use crate::iso8601_ms;
use crate::jsonl::JsonlHandle;
use crate::pipeline::{BargeWiring, BrainWiring, PipelineFatal};
use crate::playback_router::{
    self, playback_event_adapter, PlaybackFanout, RouterStats, RouterStatsSnapshot,
};
use crate::prune::{prune, PruneOutcome, PruneRequest};
use crate::recorder::{OpenLogs, Recorder, RecorderShared};

/// Deadline for an accepted connection to send its first decodable frame. Until
/// a valid `Hello` lands a connection holds an accept-gate permit but cannot be
/// superseded (its pod id is unknown), so an unidentified peer that connects and
/// then stalls — sending nothing, dribbling bytes, or a length prefix with no
/// payload — would otherwise pin a permit until the OS TCP timeout. Bounding the
/// pre-`Hello` wait closes that unauthenticated permit-exhaustion path. A real
/// pod sends `Hello` immediately, so this is generous.
const HELLO_DEADLINE: Duration = Duration::from_secs(10);

/// Once a frame's length prefix has been read, its payload must arrive within
/// this window. A peer that sends a prefix and then stalls mid-payload (a
/// half-open TCP connection with no FIN/RST) is closed as a read error rather
/// than parking on the read forever. Whole frames are small, so this is ample.
const PAYLOAD_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Backoff after a failed `accept()`. A persistent resource error (fd
/// exhaustion: EMFILE/ENFILE/ENOBUFS) recurs immediately, so retrying with no
/// delay busy-spins a core and floods the observability sink; a short pause
/// paces it into a legible error stream without meaningfully delaying recovery.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// One live pod connection in the supersede registry. `cancel` terminates the
/// task (it may be parked in `read_exact`); `finished` fires once that task has
/// fully closed — finalized any open segment as truncated and written its
/// `ResumeLedger` note — so a superseding task can await it before performing a
/// resume lookup.
#[derive(Clone)]
struct ConnEntry {
    conn_seq: u64,
    cancel: CancellationToken,
    finished: CancellationToken,
}

/// Per-pod single-connection policy: a new connection from a pod supersedes the
/// old. Keyed by pod id (known only after `Hello`), so pre-`Hello` connections
/// never register.
type Registry = Arc<Mutex<HashMap<String, ConnEntry>>>;

/// Remove `key` from a per-pod registry, but only if the stored entry is still
/// owned by `conn_seq`. A superseding successor may have already replaced the
/// slot, and must keep it; `seq_of` reads the stored owner. Shared by both the
/// supersede [`Registry`] and the [`PlaybackRegistry`], which hold the same
/// conn_seq-guarded invariant over structurally identical maps.
fn guarded_remove<V>(
    reg: &Mutex<HashMap<String, V>>,
    key: &str,
    conn_seq: u64,
    seq_of: impl Fn(&V) -> u64,
) {
    let mut map = reg.lock().expect("connection registry poisoned");
    if map.get(key).map(&seq_of) == Some(conn_seq) {
        map.remove(key);
    }
}

/// A live per-pod playback writer handle plus the `conn_seq` that installed it,
/// so a superseded connection's close can tell whether the slot is still its own
/// before removing it — the same conn_seq guard `ConnEntry` uses.
pub(crate) struct PlaybackEntry {
    conn_seq: u64,
    // Held both for its RAII effect (keeping the writer's queue open so its task
    // stays alive awaiting jobs; deregister drops it, closing the queue and ending
    // the task) and as the router's routing target: `playback_try_play` resolves a
    // pod here and enqueues onto this handle.
    handle: PlaybackHandle,
}

/// Per-pod live playback writers, keyed by pod id. A connection installs its
/// handle at the post-`Hello` writer spawn and removes it at close, so a target
/// pod resolves to its writer through this map. Sits beside the supersede
/// [`Registry`].
pub(crate) type PlaybackRegistry = Arc<Mutex<HashMap<String, PlaybackEntry>>>;

/// Resolve `target` in the playback registry and enqueue `job` on its writer.
/// `None` when no writer is registered for the pod (never connected, pre-`Hello`,
/// or disconnected — the caller drops the job as stale); `Some(Ok)` when the job
/// was queued; `Some(Err)` when the writer rejected it (`QueueFull`/`WriterDead`,
/// each counted inside `try_play`). The lock is held only for the non-blocking
/// `try_send` inside `try_play`.
pub(crate) fn playback_try_play(
    reg: &PlaybackRegistry,
    target: &PodId,
    job: PlaybackJob,
) -> Option<Result<(), PlayRejected>> {
    reg.lock()
        .expect("connection registry poisoned")
        .get(&target.0)
        .map(|entry| entry.handle.try_play(job))
}

/// Cut whatever `pod` is playing: resolve its writer, read the turn the writer
/// says is current, and ask it to flush that turn. Returns the turn that was cut
/// and how much of its clip the user heard.
///
/// Reading the turn and flushing it are two steps, deliberately keyed on the turn
/// id rather than on "whatever is playing now": a job boundary between them makes
/// the flush a `WrongTurn` no-op instead of cutting a response the user never
/// barged in on. No writer registered (never connected, or disconnected) reads as
/// `WriterDead` — either way there is nothing playing to cut.
///
/// The lock spans both handle calls, which are each non-blocking (a mutex and a
/// notify), the way `playback_try_play` holds it across its `try_send`.
pub(crate) fn playback_flush(
    reg: &PlaybackRegistry,
    pod: &PodId,
) -> Result<(UtteranceId, InterruptProgress), FlushRejected> {
    let reg = reg.lock().expect("connection registry poisoned");
    let entry = reg.get(&pod.0).ok_or(FlushRejected::WriterDead)?;
    let turn = entry
        .handle
        .current_turn()
        .ok_or(FlushRejected::NotPlaying)?;
    entry.handle.flush(turn).map(|progress| (turn, progress))
}

/// Install a connection's playback handle under its pod id, replacing any stale
/// predecessor entry (which drops, closing that queue).
pub(crate) fn playback_register(
    reg: &PlaybackRegistry,
    pod: String,
    conn_seq: u64,
    handle: PlaybackHandle,
) {
    reg.lock()
        .expect("connection registry poisoned")
        .insert(pod, PlaybackEntry { conn_seq, handle });
}

/// Remove a connection's playback handle at close, guarded on `conn_seq` so a
/// superseding successor's entry is never evicted. The removed handle drops,
/// closing its queue.
fn playback_deregister(reg: &PlaybackRegistry, pod: &str, conn_seq: u64) {
    guarded_remove(reg, pod, conn_seq, |e| e.conn_seq);
}

/// Project the registry to `pod -> conn_seq`, the only observable surface a
/// lifecycle test needs. Locks the map briefly and clones out plain data; the
/// private `PlaybackEntry`/`PlaybackHandle` internals stay private.
#[cfg(test)]
fn playback_registry_conn_seqs(reg: &PlaybackRegistry) -> HashMap<String, u64> {
    reg.lock()
        .expect("connection registry poisoned")
        .iter()
        .map(|(pod, entry)| (pod.clone(), entry.conn_seq))
        .collect()
}

/// Serializes background prune passes so at most one runs at a time — the
/// design's at-most-one-pass invariant — and coalesces a burst of rolls into a
/// single follow-up pass instead of one redundant full-store scan per roll.
/// Without serialization two concurrent passes over the same directory each miss
/// the other's deletions (a failed `remove_file` keeps the bytes counted), so
/// their union can overshoot the cap and delete logs that should have survived.
struct PruneCoordinator {
    state: Mutex<PruneCoordState>,
    record_dir: PathBuf,
    cap_bytes: u64,
    pod_cap_bytes: u64,
    open_logs: OpenLogs,
    jsonl: JsonlHandle,
}

#[derive(Default)]
struct PruneCoordState {
    /// A prune task is currently running.
    running: bool,
    /// A roll arrived while a pass ran; the runner loops once more so the roll's
    /// bytes are accounted for rather than deferred to the next roll.
    pending: bool,
}

impl PruneCoordinator {
    fn new(
        record_dir: PathBuf,
        cap_bytes: u64,
        pod_cap_bytes: u64,
        open_logs: OpenLogs,
        jsonl: JsonlHandle,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(PruneCoordState::default()),
            record_dir,
            cap_bytes,
            pod_cap_bytes,
            open_logs,
            jsonl,
        })
    }

    /// Request a prune pass. Spawns a single runner if none is active; otherwise
    /// marks a rerun so the active runner picks up this request when it finishes.
    fn schedule(self: &Arc<Self>) {
        {
            let mut s = self.state.lock().expect("prune coordinator poisoned");
            if s.running {
                s.pending = true;
                return;
            }
            s.running = true;
        }
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                run_prune(
                    this.record_dir.clone(),
                    this.cap_bytes,
                    this.pod_cap_bytes,
                    this.open_logs.clone(),
                    this.jsonl.clone(),
                )
                .await;
                let mut s = this.state.lock().expect("prune coordinator poisoned");
                if s.pending {
                    s.pending = false;
                    // Loop: run one more pass for the roll(s) that arrived mid-pass.
                } else {
                    s.running = false;
                    break;
                }
            }
        });
    }
}

/// A bound TCP listener plus the shared config and observability sink. Split
/// from `run` so an ephemeral-port bind exposes its address before serving
/// (the accept loop can then be driven and shut down in tests).
pub struct Server {
    listener: TcpListener,
    config: Arc<Config>,
    jsonl: JsonlHandle,
    // A test-supplied router join handle that stands in for the real router task,
    // so a mid-run router exit can be driven without a live brain and channel.
    #[cfg(test)]
    router_override: Option<tokio::task::JoinHandle<()>>,
    // A test-supplied playback registry the test also holds a clone of, so it can
    // observe register/deregister through a real connection lifecycle. When set,
    // `run` uses it instead of minting its own.
    #[cfg(test)]
    playback_registry_override: Option<PlaybackRegistry>,
}

impl Server {
    /// Bind the listener at `config.listen_addr`. A bind failure is fatal.
    pub async fn bind(config: Arc<Config>, jsonl: JsonlHandle) -> std::io::Result<Server> {
        let listener = TcpListener::bind(config.listen_addr).await?;
        Ok(Server {
            listener,
            config,
            jsonl,
            #[cfg(test)]
            router_override: None,
            #[cfg(test)]
            playback_registry_override: None,
        })
    }

    /// Replace the spawned router task with a test-supplied join handle, so a
    /// test can drive a mid-run panic or clean exit through the supervision arm.
    #[cfg(test)]
    fn with_router_override(mut self, router: tokio::task::JoinHandle<()>) -> Self {
        self.router_override = Some(router);
        self
    }

    /// Inject a playback registry the caller retains a clone of, so a test can
    /// observe the map through a real connection lifecycle. `run` uses it in place
    /// of the fresh one it would otherwise mint.
    #[cfg(test)]
    fn with_playback_registry_override(mut self, reg: PlaybackRegistry) -> Self {
        self.playback_registry_override = Some(reg);
        self
    }

    /// The address the listener actually bound (resolves an ephemeral `:0`).
    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    /// Run the accept loop until `shutdown` completes, then stop accepting,
    /// cancel every in-flight connection so any open segment finalizes truncated
    /// and its log flushes, await those connections, and drain the pipeline. A
    /// startup prune pass runs first — before anything is accepted — so a restart
    /// under a full store recovers with nothing streaming; thereafter each
    /// between-segment roll fires a background prune.
    pub async fn run(self, shutdown: impl Future<Output = ()>) -> std::io::Result<()> {
        #[cfg(test)]
        let router_override = self.router_override;
        #[cfg(test)]
        let playback_registry_override = self.playback_registry_override;
        let Server {
            listener,
            config,
            jsonl,
            ..
        } = self;

        // Announce the resolved listen address the instant serving begins, so a
        // subprocess-spawning test or operator learns the real port even under an
        // ephemeral `:0` bind (where `daemon_start` only knows the configured
        // string). Consumers key on this event's presence, so it must fire even if
        // `local_addr` errors — then with a null address and a detail.
        match listener.local_addr() {
            Ok(addr) => jsonl.emit("listening", &json!({ "addr": addr.to_string() })),
            Err(e) => jsonl.emit(
                "listening",
                &json!({ "addr": null, "detail": e.to_string() }),
            ),
        }

        let (item_tx, item_rx) = DropOldestQueue::<crate::pipeline::PipelineItem>::new(
            config.pipeline.segment_queue_depth,
        );

        // Process-wide observability tallies not centralized on the queue, JSONL
        // sink, or ledger: telemetry frames discarded outside a segment (summed
        // from each connection's FSM at close) and backward clock steps clamped
        // to 0 in a stage delta. Both feed the periodic `stage_health` line.
        let telemetry_outside_segment = Arc::new(AtomicU64::new(0));
        let clock_step_clamps = Arc::new(AtomicU64::new(0));

        // One process-wide playback-stats instance shared by every per-pod
        // writer, so the counters aggregate across pods for `stage_health`, which
        // reads its snapshot to make writer death visible in the health record.
        let playback_stats = Arc::new(PlaybackStats::default());

        // Per-pod live playback writers, so a target pod resolves to its writer.
        // A connection installs its handle here at spawn and removes it at close;
        // the router (below) resolves a `SpeakCmd`'s target through this map.
        #[cfg(test)]
        let playback_registry: PlaybackRegistry =
            playback_registry_override.unwrap_or_else(|| Arc::new(Mutex::new(HashMap::new())));
        #[cfg(not(test))]
        let playback_registry: PlaybackRegistry = Arc::new(Mutex::new(HashMap::new()));

        // Cancelled at shutdown to break every in-flight connection out of its
        // read park, so open segments finalize truncated and logs flush. Also
        // stops the router task promptly at shutdown, per §4.2.
        let shutdown_token = CancellationToken::new();

        // Recording off → no record dir → no per-segment sidecar dispatch at all.
        let record_dir = if config.record.enabled {
            Some(config.record.dir.clone())
        } else {
            None
        };

        // The continuous listener — streaming OWW + Silero endpointer — owns
        // utterance semantics: it runs on the live pre-assembly audio of every
        // connection and emits the wake detections and carved-utterance lifecycle
        // the pipeline consumes. Spawned only when `[wake] mode=oww` and
        // `[endpointer]` are both configured; otherwise no listener runs and no
        // utterances are produced (tracking + recording still work). A model-load
        // failure is fatal at startup. A forwarder task moves each `ListenerEvent`
        // onto the pipeline queue as a `PipelineItem::Listener`, so segments and
        // listener events share one drop-oldest queue and one consumer.
        let (listener_handle, listener_stats, listener_forwarder) = match build_listener(&config)
            .map_err(|e| std::io::Error::other(e.to_string()))?
        {
            Some((oww_models, silero_model, listener_config)) => {
                let (ev_tx, mut ev_rx) = tokio::sync::mpsc::unbounded_channel::<ListenerEvent>();
                let handle = Listener::spawn(oww_models, silero_model, listener_config, ev_tx)?;
                let stats = handle.stats_shared();
                let fwd_tx = item_tx.clone();
                let forwarder = tokio::spawn(async move {
                    while let Some(ev) = ev_rx.recv().await {
                        // A drop-oldest eviction here is the same failure class as a
                        // segment eviction (flood territory); the drop is silent.
                        let _ = fwd_tx.send(crate::pipeline::PipelineItem::Listener(ev));
                    }
                });
                (Some(Arc::new(handle)), Some(stats), Some(forwarder))
            }
            None => (None, None, None),
        };

        // Barge-in bookkeeping, shared by the router (which evicts an interrupted
        // turn's responses), the playback adapter (which settles every job), and
        // the pipeline (which chains the interrupted turns onto the barging
        // utterance).
        let turn_ledger = Arc::new(TurnLedger::new());

        // Turns each writer's `PlaybackEvent`s into JSONL lines, and fans the same
        // events out to the listener's playback floor and the ledger. Built once
        // and cloned into every connection's writer spawn; emitting from the writer
        // task (not this loop) keeps playback lifecycle lines off any shared
        // critical path. Shares `clock_step_clamps` so a clamped backward clock
        // step in a latency line is corroborated by the `stage_health` count. With
        // no listener wired there is no floor to drive and no barge-in path, so the
        // adapter is lines-only.
        let playback_events = playback_event_adapter(
            jsonl.clone(),
            clock_step_clamps.clone(),
            listener_handle.as_ref().map(|listener| PlaybackFanout {
                feed: {
                    let sender = weak_feed_sender(listener);
                    Arc::new(move |pod, feed| match sender() {
                        Some(sender) => Box::pin(async move { sender.feed(pod, feed).await }),
                        None => Box::pin(std::future::ready(())),
                    })
                },
                reserve: {
                    let sender = weak_feed_sender(listener);
                    Arc::new(move || match sender() {
                        Some(sender) => Box::pin(async move { sender.reserve_marker().await }),
                        None => Box::pin(std::future::ready(None)),
                    })
                },
                ledger: turn_ledger.clone(),
                lead_ms: config.playback.lead_ms,
            }),
        );

        // Build the brain from `[brain]` config; a clip-load failure is fatal at
        // startup, before anything is accepted. `brain_stats` exists in both cases
        // (zeros with no brain) so `stage_health` reports the dropped-reply count
        // (`speak_send_failures`) uniformly across configs.
        let (brain, brain_events, brain_stats) =
            build_brain(&config, &jsonl).map_err(|e| std::io::Error::other(e.to_string()))?;
        // Build the transcriber from `[stt]`; a malformed endpoint or a client that
        // will not build is fatal at startup, before anything is accepted. No
        // transcriber wired (absent `[stt]`) mints utterances with a null
        // transcript, unchanged.
        // The stage and its stats handle are split out: the stage drives the
        // pipeline, the stats clone feeds `stage_health` (`None` with no `[stt]`).
        let (transcriber, stt_stats) = build_transcriber(&config, &jsonl)
            .map_err(|e| std::io::Error::other(e.to_string()))?
            .unzip();
        // Build the synthesizer from `[tts]`; a malformed endpoint or a client
        // that will not build is fatal at startup. No synthesizer wired (absent
        // `[tts]`) leaves a `Text` reply as a counted `speak_unsupported`
        // rejection in the router, unchanged.
        let (synthesizer, tts_stats) = build_synthesizer(&config, &jsonl)
            .map_err(|e| std::io::Error::other(e.to_string()))?
            .unzip();
        // The router's routing-outcome counters (delivered/no_pod/unsupported),
        // shared with `stage_health`. Created unconditionally so the health line
        // carries the fields with no brain (all zero); a clone moves into the
        // router task when one is spawned.
        let router_stats = Arc::new(RouterStats::default());
        // A brain wired: the bounded `SpeakCmd` channel it replies through, and
        // the router task that resolves each reply to its target pod's writer.
        // No brain: no channel, no router task — the increment-3
        // mint-and-emit-utterance behavior, unchanged.
        let (brain_wiring, router_join) = match brain {
            Some(brain) => {
                let (speak_tx, speak_rx) =
                    mpsc::channel::<SpeakCmd>(config.playback.speak_queue_depth);
                let join = tokio::spawn(
                    playback_router::Router::new(
                        playback_registry.clone(),
                        router_stats.clone(),
                        jsonl.clone(),
                        shutdown_token.clone(),
                        // A wired synthesizer renders `Text` replies to PCM; absent
                        // `[tts]`, `Text` bodies stay a counted `speak_unsupported`
                        // rejection.
                        synthesizer.clone(),
                        turn_ledger.clone(),
                    )
                    .run(speak_rx),
                );
                (
                    Some(BrainWiring {
                        brain,
                        speak_tx,
                        // The confidence gate declines an utterance before dispatch;
                        // it reports through the same event adapter instance and
                        // counters the brain uses, so a gated no-command reads
                        // identically to one the brain itself declined.
                        events: brain_events,
                        stats: brain_stats.clone(),
                    }),
                    Some(join),
                )
            }
            None => (None, None),
        };
        // A test override stands in for the spawned handle; the real one (if any)
        // is dropped, its task stopping on the shutdown token as usual.
        #[cfg(test)]
        let router_join = router_override.or(router_join);
        let mut router_join = router_join;

        let mut pipeline = tokio::spawn(crate::pipeline::run(
            item_rx,
            crate::pipeline::PipelineCtx {
                record_dir,
                clock_step_clamps: clock_step_clamps.clone(),
                transcriber,
                brain: brain_wiring,
                // Absent `[stt]` wires the never-fires OFF gate (not the `0.2`
                // default): the gate is consulted only when a transcript carries a
                // confidence summary, and a no-STT pipeline produces none, so it is
                // unreachable here anyway.
                confidence_gate: config
                    .stt
                    .as_ref()
                    .map(SttConfig::confidence_gate)
                    .unwrap_or(ConfidenceGate::OFF),
                // Barge-in needs a listener to detect it and a writer to cut, so it
                // is wired exactly when detection is: the same condition the
                // playback fan-out above uses.
                barge: listener_handle.as_ref().map(|_| BargeWiring {
                    ledger: turn_ledger.clone(),
                    flush: {
                        let reg = playback_registry.clone();
                        Arc::new(move |pod: &PodId| playback_flush(&reg, pod))
                    },
                }),
            },
            jsonl.clone(),
        ));

        let ledger = ResumeLedger::shared();
        let registry: Registry = Arc::new(Mutex::new(HashMap::new()));
        let open_logs = OpenLogs::default();
        let prune_coord = PruneCoordinator::new(
            config.record.dir.clone(),
            config.record.cap_bytes,
            config.record.resolved_pod_cap_bytes(),
            open_logs.clone(),
            jsonl.clone(),
        );
        let sem = Arc::new(Semaphore::new(config.max_connections));
        // The tracker lets `run` await in-flight connection tasks (cancelled via
        // the `shutdown_token` built above) before draining the pipeline.
        let conns = tokio_util::task::TaskTracker::new();

        // Periodic `stage_health` emitter. It reads the queue counters through a
        // non-sender `StatsHandle` (so it never stalls the pipeline drain),
        // stops on the shutdown token, and joins before the final line fires.
        let health = HealthSources {
            stats: item_tx.stats_handle(),
            listener_stats: listener_stats.clone(),
            playback_stats: playback_stats.clone(),
            brain_stats: brain_stats.clone(),
            router_stats: router_stats.clone(),
            stt_stats: stt_stats.clone(),
            tts_stats: tts_stats.clone(),
            jsonl: jsonl.clone(),
            ledger: ledger.clone(),
            telemetry_outside_segment: telemetry_outside_segment.clone(),
            clock_step_clamps: clock_step_clamps.clone(),
        };
        let health_join = tokio::spawn(stage_health_emitter(
            Duration::from_secs(config.jsonl.stage_health_period_s),
            health.clone(),
            shutdown_token.clone(),
        ));
        // Latched true once any recorder write fails, process-wide. The
        // config-disabled case is carried by the `config.record.enabled`
        // conjunction at each reader, so this flag means only "a write failed" —
        // it is never pre-set for a deliberately-disabled recorder.
        let recording_failed = Arc::new(AtomicBool::new(false));
        let mut conn_seq: u64 = 0;

        // Startup prune pass: enforce the store cap before accepting, so a
        // restart under a full store starts clean with nothing streaming (the
        // open set is empty here). Awaited, unlike the fire-and-forget on-roll
        // pass. Skipped when recording is off — there is no store to manage.
        if config.record.enabled {
            run_prune(
                config.record.dir.clone(),
                config.record.cap_bytes,
                config.record.resolved_pod_cap_bytes(),
                open_logs.clone(),
                jsonl.clone(),
            )
            .await;
        }

        tokio::pin!(shutdown);
        // Set on a pipeline fault (dead wake stage / panic) so `run` exits nonzero.
        let mut pipeline_fatal: Option<PipelineFatal> = None;
        // The pipeline's `JoinHandle` was already consumed by the select branch.
        let mut pipeline_joined = false;
        // Set once the router-supervision arm has consumed `router_join`, so the
        // shutdown-path join below neither re-polls the completed handle nor
        // re-emits. Single source of truth for which side observed the exit.
        let mut router_joined = false;
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                result = &mut pipeline => {
                    // The pipeline drains only once every segment sender drops,
                    // which cannot happen before shutdown — so an early exit is a
                    // fault. Record it, then fall into the shutdown path to cancel
                    // and drain the connections via the shutdown token.
                    handle_pipeline_result(result, &jsonl, &mut pipeline_fatal);
                    pipeline_joined = true;
                    break;
                }
                result = async { router_join.as_mut().expect("guarded by is_some").await },
                        if router_join.is_some() && !router_joined => {
                    // The router only exits mid-run when it dies: its senders
                    // dropped or it panicked. Either way playback is permanently
                    // dead for this process, so report it now and fall into the
                    // shutdown path — the shutdown token has not fired yet, so this
                    // can never be a clean shutdown-induced exit.
                    router_joined = true;
                    // A router exit is often a downstream symptom of the pipeline
                    // dying first: the pipeline task holds `BrainWiring`'s
                    // `SpeakCmd` sender, so its death drops that sender and the
                    // router then exits cleanly. When both handles are ready in
                    // the same poll, `select!` picks one arbitrarily. If the
                    // pipeline has also finished it is the root fault, so report
                    // it first — `get_or_insert` is first-writer-wins, so joining
                    // the pipeline here lets its detail own the nonzero-exit
                    // string instead of the router's symptom detail.
                    if !pipeline_joined && pipeline.is_finished() {
                        let pipeline_result = (&mut pipeline).await;
                        handle_pipeline_result(pipeline_result, &jsonl, &mut pipeline_fatal);
                        pipeline_joined = true;
                    }
                    handle_router_exit_midrun(result, &jsonl, &mut pipeline_fatal);
                    break;
                }
                accepted = listener.accept() => {
                    let (stream, peer) = match accepted {
                        Ok(pair) => pair,
                        Err(e) => {
                            jsonl.emit("conn_accept_error", &json!({ "detail": e.to_string() }));
                            // Pace retries: a persistent resource error would
                            // otherwise busy-spin and flood the sink.
                            tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                            continue;
                        }
                    };
                    conn_seq += 1;
                    let seq = conn_seq;
                    jsonl.emit(
                        "conn_accepted",
                        &json!({ "peer": peer.to_string(), "conn_seq": seq }),
                    );
                    let permit = match sem.clone().try_acquire_owned() {
                        Ok(p) => p,
                        Err(_) => {
                            jsonl.emit(
                                "conn_rejected",
                                &json!({ "peer": peer.to_string(), "conn_seq": seq }),
                            );
                            continue;
                        }
                    };
                    conns.spawn(connection(
                        stream,
                        peer,
                        seq,
                        config.clone(),
                        jsonl.clone(),
                        ledger.clone(),
                        registry.clone(),
                        open_logs.clone(),
                        prune_coord.clone(),
                        recording_failed.clone(),
                        item_tx.clone(),
                        shutdown_token.clone(),
                        telemetry_outside_segment.clone(),
                        clock_step_clamps.clone(),
                        playback_stats.clone(),
                        playback_events.clone(),
                        playback_registry.clone(),
                        listener_handle.clone(),
                        permit,
                    ));
                }
            }
        }

        // Graceful shutdown: accepting has stopped (the loop broke). Cancel every
        // in-flight connection so it leaves its read park and runs its close path
        // — any open segment finalizes truncated, its ledger note lands, its log
        // flushes — then await all connection tasks. Each drops its `seg_tx` clone
        // as it finishes, so waiting here before dropping the server's handle
        // guarantees those final truncated segments reach the still-running
        // pipeline before it drains.
        conns.close();
        shutdown_token.cancel();
        conns.wait().await;
        // The emitter observes the same shutdown token; join it so the periodic
        // line and the final line below never race on the sink. A non-`Ok` join
        // means the emitter panicked mid-run — the health channel went dark
        // silently, the exact failure it exists to prevent — so report it rather
        // than discarding the `JoinError`, mirroring the pipeline-task handling.
        if let Err(e) = health_join.await {
            jsonl.emit(
                "stage_health_emitter_exited",
                &json!({ "detail": e.to_string() }),
            );
        }

        // Join the listener thread before draining the pipeline: every connection
        // dropped its handle clone at `conns.wait`, so this task holds the last
        // reference. `join` closes the feed channel and waits, surfacing a panic;
        // the listener thread then drops its event sender, which closes the
        // forwarder's `ev_rx`. The forwarder must end (dropping its pipeline-queue
        // sender) before the pipeline can drain, so it is joined here rather than
        // after — otherwise the pipeline would wait on a sender that waits on this
        // join.
        if let Some(listener_handle) = listener_handle {
            if let Some(handle) = Arc::into_inner(listener_handle) {
                if handle.join().is_err() {
                    jsonl.emit("listener_thread_panicked", &json!({}));
                    pipeline_fatal.get_or_insert(PipelineFatal {
                        detail: "listener thread panicked".to_string(),
                    });
                }
            }
        }
        if let Some(forwarder) = listener_forwarder {
            let _ = forwarder.await;
        }

        // Drop the server's queue handle so the pipeline drains once every other
        // cloned sender is gone (connection clones at `conns.wait`, the forwarder's
        // clone just above). `health.stats` is a non-sender view that survives this
        // drop, so the final line below still reports the queue counters.
        drop(item_tx);
        if !pipeline_joined {
            // Graceful path: the pipeline was not consumed by the select branch.
            // Await it now — a fatal result (panic) propagates to `run`'s return the
            // same way an early exit does.
            let result = (&mut pipeline).await;
            handle_pipeline_result(result, &jsonl, &mut pipeline_fatal);
        }

        // The router task exits once its `SpeakCmd` channel closes (the pipeline
        // has ended, dropping `BrainWiring`'s sender) or `shutdown_token` fired,
        // whichever comes first. A non-`Ok` join means it panicked mid-run.
        if !router_joined {
            if let Some(join) = router_join {
                handle_router_exit(join.await, &jsonl);
            }
        }

        // Final `stage_health` after the pipeline drained, before the caller
        // drops the JSONL handle and joins the sink writer.
        emit_stage_health(&health, true);
        match pipeline_fatal {
            Some(fatal) => Err(std::io::Error::other(fatal.detail)),
            None => Ok(()),
        }
    }
}

/// Assemble the continuous listener's shared models and per-pod config from the
/// `[wake]` (openWakeWord models) and `[endpointer]` (Silero + timing) tables.
/// Returns `None` unless both are present in the forms the streaming listener
/// needs — `mode = "oww"` with its three model paths, plus an `[endpointer]` table
/// naming the Silero model — so a bypass, model-less, or endpointer-less config
/// runs with no listener and the batch wake path is untouched. A model-load
/// failure is fatal at startup, never a silently-degraded listener. Shares its
/// config gating and model loading with the offline replay harness.
#[allow(clippy::type_complexity)]
fn build_listener(
    config: &Config,
) -> Result<Option<(OwwModels, SileroModel, ListenerConfig)>, WakeError> {
    Ok(crate::replay::ReplayListener::from_config(config)?
        .map(crate::replay::ReplayListener::into_parts))
}

/// A weak accessor for the listener's [`FeedSender`], for the playback fanout hooks.
///
/// Weak, not a clone: shutdown joins the listener thread through `Arc::into_inner`,
/// which needs the server task to hold the last reference. A strong clone in the
/// adapter (or in every floor-close timer it spawns) would leave the thread
/// unjoinable and the shutdown drain waiting on it forever. A listener already gone
/// has no floor left to move, hence `None`.
///
/// The upgrade is dropped before returning, so only a `FeedSender` clone can live
/// across a marker await; that delays the thread join by at most the marker timeout.
fn weak_feed_sender(listener: &Arc<ListenerHandle>) -> impl Fn() -> Option<FeedSender> {
    let listener = Arc::downgrade(listener);
    move || listener.upgrade().map(|l| l.feed_sender())
}

/// Forward one session event to the listener as a [`Feed`], when a listener is
/// wired. The listener taps the live pre-assembly stream: `HelloAccepted` opens a
/// fresh per-pod epoch (the connection sequence, unique across reconnects), audio
/// and segment boundaries stream through, and the decoded PCM is handed over as one
/// `Vec→Arc<[i16]>` conversion (the assembler keeps its own copy — one decode, two
/// consumers). The event→feed mapping is shared with the offline replay harness.
/// Pre-`Hello` events carry no pod identity and are skipped.
///
/// Awaited by the read loop: marker delivery is reliable, so a saturated feed
/// channel backpressures this pod's reads (bounded by the marker timeout) rather
/// than losing the marker. Audio still drops on a full channel and never waits.
async fn tap_listener(
    listener: Option<&Arc<ListenerHandle>>,
    pod: &mut Option<PodId>,
    ev: &SessionEvent,
    epoch: u64,
) {
    let Some(listener) = listener else {
        return;
    };
    if let Some(feed) = crate::replay::session_event_to_feed(ev, pod, epoch) {
        if let Some(id) = pod.as_ref() {
            listener.feed(id.clone(), feed).await;
        }
    }
}

/// What `build_brain` produces: the optional brain implementation, the shared
/// event adapter (the same instance the brain emits through, so the pipeline's
/// confidence-gate decline reports through it too), and the shared `BrainStats`
/// counters read by `stage_health`; all created in both cases so they report
/// zeros even with no brain.
type BuiltBrain = (Option<Arc<dyn Brain>>, BrainEventFn, Arc<BrainStats>);

/// Build the brain from config. An absent `[brain]` table wires no brain — the
/// mint-and-emit-utterance behavior with no dispatch and a null `brain_dispatched`
/// stamp — plus a startup `brain_absent` info line (the `wake_bypassed` idiom).
/// `mode = "wav"` loads and format-validates the configured clip (a load failure
/// is fatal at startup, naming the path and the offending property) and builds a
/// `WavBrain` answering every utterance with it, emitting a `brain_clip_loaded`
/// line carrying the clip's sample count and duration. Returns the brain (or
/// `None`), the shared event adapter (the same instance the brain emits through,
/// reused by the pipeline's confidence-gate decline), and the shared `BrainStats`
/// counters read by `stage_health`; all created in both cases so they report zeros
/// with no brain.
fn build_brain(config: &Config, jsonl: &JsonlHandle) -> Result<BuiltBrain, ClipError> {
    let stats = Arc::new(BrainStats::default());
    // One event adapter, built once and shared: the brain emits through it, and
    // the returned clone lets the pipeline's confidence-gate decline report a
    // no-command outcome through the same sink and counters — one no-command
    // story regardless of which stage decided.
    let events = brain_event_adapter(jsonl.clone());
    match &config.brain {
        None => {
            jsonl.emit(
                "brain_absent",
                &json!({ "reason": "no [brain] table configured" }),
            );
            Ok((None, events, stats))
        }
        Some(brain) => match brain.mode {
            BrainMode::Wav => {
                // `Config::validate` rejects a wav-mode config with no clip, but the
                // library entry points (`bind`/`run`) do not re-validate — so a caller
                // driving the server without validating reaches here with no clip.
                // Return the same actionable error rather than panicking.
                let path = brain.clip.clone().ok_or(ClipError::MissingPath)?;
                let clip = load_clip(&path)?;
                let samples = clip.len();
                let duration_ms = (samples as u64) * 1000 / u64::from(SPINE_FORMAT.sample_rate_hz);
                jsonl.emit(
                    "brain_clip_loaded",
                    &json!({
                        "clip": path.display().to_string(),
                        "samples": samples,
                        "duration_ms": duration_ms,
                    }),
                );
                let brain = WavBrain::new(clip, events.clone(), stats.clone());
                Ok((Some(Arc::new(brain) as Arc<dyn Brain>), events, stats))
            }
            BrainMode::Echo => {
                // Echo reads the transcript back; it needs no clip. The transcript
                // arrives on the utterance and the synthesizer belongs to the
                // router, both wired separately — the brain itself needs only the
                // shared event adapter and stats.
                jsonl.emit("brain_echo", &json!({}));
                let brain = EchoBrain::new(events.clone(), stats.clone());
                Ok((Some(Arc::new(brain) as Arc<dyn Brain>), events, stats))
            }
        },
    }
}

/// A wired transcriber and the stats handle `stage_health` snapshots. The stage
/// drives the pipeline; the stats clone is read by the health emitter.
type BuiltTranscriber = (Arc<dyn Transcriber>, Arc<SttStats>);

/// A wired synthesizer and the stats handle `stage_health` snapshots.
type BuiltSynthesizer = (Arc<dyn Synthesizer>, Arc<TtsStats>);

/// Build the transcriber from `[stt]` config. An absent `[stt]` table wires no
/// transcriber — the utterance mints with a null transcript, unchanged — plus a
/// startup `stt_absent` info line (the `brain_absent` idiom). `backend = "http"`
/// builds an `HttpTranscriber` against the configured speaches endpoint and emits
/// an `stt_configured` line naming the URL, model, and language. A malformed URL or a client
/// that will not build is fatal at startup (`BuildError`), not a per-request
/// failure. Deliberately no startup network probe: a container down at boot but up
/// at first utterance degrades per-request (a loud `stt_failed` line), never
/// holding the daemon hostage.
fn build_transcriber(
    config: &Config,
    jsonl: &JsonlHandle,
) -> Result<Option<BuiltTranscriber>, BuildError> {
    match &config.stt {
        None => {
            jsonl.emit(
                "stt_absent",
                &json!({ "reason": "no [stt] table configured" }),
            );
            Ok(None)
        }
        Some(stt) => match stt.backend {
            SttBackend::Http => {
                // Validation guarantees url + model in http mode. The library entry
                // points (`bind`/`run`) do not re-validate, so name the invariant.
                let url = stt.url.clone().expect("stt.url present when backend=http");
                let model = stt
                    .model
                    .clone()
                    .expect("stt.model present when backend=http");
                let params = SttParams {
                    url: url.clone(),
                    model: model.clone(),
                    language: stt.language.clone(),
                    timeout: Duration::from_millis(stt.timeout_ms),
                    connect_timeout: Duration::from_millis(stt.connect_timeout_ms),
                };
                // The stats handle is shared: the stage counts into it, and a clone
                // is held for `stage_health` to snapshot.
                let stats = Arc::new(SttStats::default());
                let transcriber = HttpTranscriber::new(params, stats.clone())?;
                jsonl.emit(
                    "stt_configured",
                    &json!({ "url": url, "model": model, "language": stt.language.clone() }),
                );
                Ok(Some((Arc::new(transcriber) as Arc<dyn Transcriber>, stats)))
            }
        },
    }
}

/// Build the synthesizer from `[tts]`. Mirrors `build_transcriber`: absent
/// `[tts]` yields no synthesizer (a `Text` reply stays a counted
/// `speak_unsupported` rejection in the router); a malformed endpoint or a
/// client that will not build is fatal at startup. No startup network probe —
/// a container that is down at boot but up at first reply degrades per-request
/// via the router's `synth_failed` path.
fn build_synthesizer(
    config: &Config,
    jsonl: &JsonlHandle,
) -> Result<Option<BuiltSynthesizer>, BuildError> {
    match &config.tts {
        None => {
            jsonl.emit(
                "tts_absent",
                &json!({ "reason": "no [tts] table configured" }),
            );
            Ok(None)
        }
        Some(tts) => match tts.backend {
            TtsBackend::Http => {
                // Validation guarantees url + model + voice in http mode. The
                // library entry points do not re-validate, so name the invariant.
                // TODO(config-backend-parse-dont-validate): presence is enforced by
                // a distant `validate()` and re-asserted with `expect` here, once
                // per builder. When the next backend or table lands, have the `http`
                // variant carry a struct with non-optional fields so builders
                // destructure instead of `expect`ing.
                let url = tts.url.clone().expect("tts.url present when backend=http");
                let model = tts
                    .model
                    .clone()
                    .expect("tts.model present when backend=http");
                let voice = tts
                    .voice
                    .clone()
                    .expect("tts.voice present when backend=http");
                let params = TtsParams {
                    url: url.clone(),
                    model: model.clone(),
                    voice: voice.clone(),
                    timeout: Duration::from_millis(tts.timeout_ms),
                    connect_timeout: Duration::from_millis(tts.connect_timeout_ms),
                };
                // The stats handle is shared: the stage counts into it, and a clone
                // is held for `stage_health` to snapshot.
                let stats = Arc::new(TtsStats::default());
                let synthesizer = HttpSynthesizer::new(params, stats.clone())?;
                jsonl.emit(
                    "tts_configured",
                    &json!({ "url": url, "model": model, "voice": voice }),
                );
                Ok(Some((Arc::new(synthesizer) as Arc<dyn Synthesizer>, stats)))
            }
        },
    }
}

/// Turn `WavBrain`'s typed events into JSONL lines. One-to-one, no silent
/// variants — the adapter is the only place brain events reach the wire.
fn brain_event_adapter(jsonl: JsonlHandle) -> BrainEventFn {
    Arc::new(move |event| match event {
        BrainEvent::SinkFull { utterance } => {
            jsonl.emit("brain_sink_full", &json!({ "utterance": utterance }));
        }
        BrainEvent::NoTranscript { utterance } => {
            jsonl.emit("brain_no_transcript", &json!({ "utterance": utterance }));
        }
        BrainEvent::WakeCommandAbsent {
            utterance,
            audio_ref,
            score,
            wake_end_sample,
            stt_trim_samples,
            reason,
        } => {
            let mut line = json!({
                "utterance": utterance,
                "log": audio_ref.log,
                "start_sample": audio_ref.start_sample,
                "end_sample": audio_ref.end_sample,
                "segments": audio_ref.segments,
                "score": score,
                "wake_end_sample": wake_end_sample,
                "stt_trim_samples": stt_trim_samples,
            });
            let map = line.as_object_mut().expect("json object");
            match reason {
                WakeCommandReason::Empty => {
                    map.insert("reason".into(), json!("empty"));
                }
                WakeCommandReason::LowConfidence {
                    no_speech_prob,
                    avg_logprob,
                } => {
                    map.insert("reason".into(), json!("low_confidence"));
                    map.insert("no_speech".into(), json!(no_speech_prob));
                    map.insert("logprob".into(), json!(avg_logprob));
                }
                WakeCommandReason::ArmExpired => {
                    map.insert("reason".into(), json!("arm_expired"));
                }
            }
            jsonl.emit("wake_command_absent", &line);
        }
        BrainEvent::BargeCommandAbsent {
            utterance,
            audio_ref,
            no_speech_prob,
            avg_logprob,
        } => {
            jsonl.emit(
                "barge_command_absent",
                &json!({
                    "utterance": utterance,
                    "log": audio_ref.log,
                    "start_sample": audio_ref.start_sample,
                    "end_sample": audio_ref.end_sample,
                    "segments": audio_ref.segments,
                    "reason": "low_confidence",
                    "no_speech": no_speech_prob,
                    "logprob": avg_logprob,
                }),
            );
        }
    })
}

/// Emit the `playback_router_exited` line with a cause `reason` and a detail
/// string. Shared by the shutdown-path and mid-run reporters so the line shape
/// stays in one place.
fn emit_router_exited(jsonl: &JsonlHandle, reason: &str, detail: &str) {
    jsonl.emit(
        "playback_router_exited",
        &json!({ "reason": reason, "detail": detail }),
    );
}

/// Report the router task's terminal join result at the shutdown-path join: a
/// clean exit (channel closed or shutdown token fired) is silent; a panic
/// surfaces as `playback_router_exited`. Reached only when the supervision arm
/// did not already observe the exit.
fn handle_router_exit(result: Result<(), tokio::task::JoinError>, jsonl: &JsonlHandle) {
    if let Err(e) = result {
        // A `JoinError` is a panic unless the task was aborted. Nothing aborts
        // the router today, but label a cancellation honestly rather than
        // reporting it as a panic if that ever changes.
        let reason = if e.is_cancelled() {
            "cancelled"
        } else {
            "panic"
        };
        emit_router_exited(jsonl, reason, &e.to_string());
    }
}

/// Report a router exit observed mid-run by the supervision arm. Both a panic and
/// a clean return are faults here — before shutdown, either means every
/// `SpeakCmd` sender is gone and playback is permanently dead — so both emit
/// `playback_router_exited` (distinguished by `reason`) and latch `pipeline_fatal`
/// so `run` returns `Err` and a supervisor restarts the process.
fn handle_router_exit_midrun(
    result: Result<(), tokio::task::JoinError>,
    jsonl: &JsonlHandle,
    fatal: &mut Option<PipelineFatal>,
) {
    let (reason, detail) = match result {
        Ok(()) => ("clean_exit", "playback router exited mid-run".to_string()),
        Err(e) => ("panic", format!("playback router panicked mid-run: {e}")),
    };
    emit_router_exited(jsonl, reason, &detail);
    fatal.get_or_insert(PipelineFatal { detail });
}

/// Render a completed pipeline task's result: a clean drain is silent; a
/// `PipelineFatal` or a panic emits `pipeline_fatal` and latches the fatal so
/// `run` returns `Err` and the daemon exits nonzero.
fn handle_pipeline_result(
    result: Result<Result<(), PipelineFatal>, tokio::task::JoinError>,
    jsonl: &JsonlHandle,
    fatal: &mut Option<PipelineFatal>,
) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(f)) => {
            jsonl.emit("pipeline_fatal", &json!({ "detail": f.detail }));
            fatal.get_or_insert(f);
        }
        Err(e) => {
            let detail = format!("pipeline task panicked: {e}");
            jsonl.emit("pipeline_fatal", &json!({ "detail": detail }));
            fatal.get_or_insert(PipelineFatal { detail });
        }
    }
}

/// One pod connection: read frames, tap them to the frame log, decode, feed the
/// FSM, assemble segments, and hand them to the pipeline queue.
#[allow(clippy::too_many_arguments)]
async fn connection(
    stream: TcpStream,
    peer: std::net::SocketAddr,
    conn_seq: u64,
    config: Arc<Config>,
    jsonl: JsonlHandle,
    ledger: Arc<Mutex<ResumeLedger>>,
    registry: Registry,
    open_logs: OpenLogs,
    prune_coord: Arc<PruneCoordinator>,
    recording_failed: Arc<AtomicBool>,
    item_tx: Sender<crate::pipeline::PipelineItem>,
    shutdown: CancellationToken,
    telemetry_outside_segment: Arc<AtomicU64>,
    clock_step_clamps: Arc<AtomicU64>,
    playback_stats: Arc<PlaybackStats>,
    playback_events: PlaybackEventFn,
    playback_registry: PlaybackRegistry,
    listener: Option<Arc<ListenerHandle>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
) {
    let recording_on = config.record.enabled && !recording_failed.load(Ordering::Relaxed);

    // Supersede plumbing: `cancel` lets a later same-pod connection terminate
    // this task; `finished` fires once this task has fully closed. Registration
    // in `registry` happens on `Hello` (pod id known); a pre-`Hello` death never
    // registers but still fires `finished` (nobody awaits it).
    let cancel = CancellationToken::new();
    let finished = CancellationToken::new();
    // Fire `finished` even on an unwind. A superseding connection blocks on this
    // token; a bare trailing `finished.cancel()` would be skipped by a panic
    // (dropping a `CancellationToken` does not cancel it), permanently wedging
    // the next connection for this pod. The drop guard cancels on any exit,
    // panic included; the explicit `cancel()` at the clean end keeps the
    // ledger-note-before-signal ordering and makes the guard a no-op there.
    let _finished_guard = finished.clone().drop_guard();
    let mut registered_pod: Option<String> = None;

    // Split the socket: the ingest loop reads from `rd`; the write half is handed
    // to a per-pod paced playback writer once `Hello` identifies the pod.
    let (mut rd, wr) = stream.into_split();
    let mut write_half = Some(wr);
    // Cancels this connection's playback writer on close, aborting any in-flight
    // job. The writer is spawned post-`Hello`; a pre-`Hello` death never spawns.
    // The writer's handle lives in `playback_registry`, keyed by pod id; this
    // connection installs it at spawn and removes it at close.
    let playback_cancel = CancellationToken::new();
    // Cancel the writer on any exit, panic included. A bare trailing `cancel()`
    // is skipped on unwind (dropping a token does not cancel it), and the handle
    // lives in the shared registry so it is not dropped on unwind either — the
    // writer would then outlive its connection holding the socket's write half,
    // leaving the connection half-open. The guard cancels regardless; the explicit
    // `cancel()` at the clean end keeps the ordered teardown and no-ops the guard.
    let _playback_guard = playback_cancel.clone().drop_guard();

    // Frame log opens at accept under a connection-scoped name (capture from
    // byte 0, no dependence on decoding `Hello`); renamed to a pod-named form
    // once `Hello` decodes.
    let accept_iso = iso8601_ms(HostMicros::now().0);
    let mut recorder = Recorder::start(
        config.record.dir.clone(),
        conn_seq,
        &accept_iso,
        recording_on,
        RecorderShared {
            open_logs,
            recording_failed: recording_failed.clone(),
            jsonl: jsonl.clone(),
        },
    );

    let mut fsm = SessionFsm::new(SPINE_FORMAT, ledger.clone());
    let limits = AssemblerLimits {
        max_segment_samples: config
            .pipeline
            .max_segment_seconds
            .saturating_mul(u64::from(SPINE_FORMAT.sample_rate_hz)),
        max_telemetry_readings: AssemblerLimits::default().max_telemetry_readings,
    };
    let mut assembler: Option<SegmentAssembler> = None;
    let mut current_room = String::new();
    // The pod identity the listener feed is keyed on, set once `Hello` decodes.
    // Pre-`Hello` events carry none and are not tapped.
    let mut listener_pod: Option<PodId> = None;

    // Terminal cause for the post-loop `fsm.close`. A read/decode failure sets a
    // more specific cause; a fatal protocol error parks the FSM (its close is a
    // no-op) so the default is fine.
    let mut close_cause = CloseCause::Eof;
    // A fatal protocol error breaks the loop with `close_cause` still `Eof` (the
    // FSM is parked, so its close is a no-op and the cause it receives is
    // irrelevant). Track the fatal exit separately so the `conn_closed` line
    // reports it distinctly instead of masquerading as a clean disconnect.
    let mut fatal_close = false;
    let mut framed: Vec<u8> = Vec::with_capacity(2 + MAX_FRAME_BYTES);
    // How much of this connection's telemetry-outside-segment tally has already
    // been folded into the process aggregate. Folded incrementally after every
    // `feed` (not once at close), so a long-lived pod steadily discarding
    // telemetry outside segments shows up in the *periodic* `stage_health` line
    // rather than only when its connection finally ends.
    let mut telemetry_reported: u64 = 0;

    'read: loop {
        let mut len_bytes = [0u8; 2];
        // The between-frames park point: a supersede cancels here while the pod
        // is quiet, so watch the token alongside the length-prefix read. Before
        // `Hello` the wait is also deadline-bounded — an unidentified peer must
        // not pin an accept-gate permit — but after `Hello` a registered pod may
        // legitimately fall silent between utterances, so the read is unbounded.
        let read = if registered_pod.is_some() {
            tokio::select! {
                _ = shutdown.cancelled() => { close_cause = CloseCause::Shutdown; break 'read; }
                _ = cancel.cancelled() => { close_cause = CloseCause::Superseded; break 'read; }
                r = rd.read_exact(&mut len_bytes) => r,
            }
        } else {
            tokio::select! {
                _ = shutdown.cancelled() => { close_cause = CloseCause::Shutdown; break 'read; }
                _ = cancel.cancelled() => { close_cause = CloseCause::Superseded; break 'read; }
                r = tokio::time::timeout(HELLO_DEADLINE, rd.read_exact(&mut len_bytes)) => match r {
                    Ok(inner) => inner,
                    Err(_elapsed) => { close_cause = CloseCause::ReadError; break 'read; }
                },
            }
        };
        match read {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                close_cause = CloseCause::Eof;
                break 'read;
            }
            Err(_) => {
                close_cause = CloseCause::ReadError;
                break 'read;
            }
        }
        let payload_len = u16::from_le_bytes(len_bytes) as usize;
        if payload_len == 0 || payload_len > MAX_FRAME_BYTES {
            close_cause = CloseCause::ReadError;
            break 'read;
        }
        framed.clear();
        framed.extend_from_slice(&len_bytes);
        framed.resize(2 + payload_len, 0);
        // A mid-payload EOF (device reset after the length prefix) is a
        // truncation, not a protocol bug; a supersede cancels here too, and a
        // peer that stalls mid-payload is bounded by the payload timeout so it
        // cannot pin a permit on a half-open socket. All three close as a read
        // error and let any open segment finalize truncated.
        let payload = tokio::select! {
            _ = shutdown.cancelled() => { close_cause = CloseCause::Shutdown; break 'read; }
            _ = cancel.cancelled() => { close_cause = CloseCause::Superseded; break 'read; }
            r = tokio::time::timeout(PAYLOAD_READ_TIMEOUT, rd.read_exact(&mut framed[2..])) => r,
        };
        if !matches!(payload, Ok(Ok(_))) {
            close_cause = CloseCause::ReadError;
            break 'read;
        }

        let host_rx = HostMicros::now();

        // Honor the process-wide recording latch: once any connection's write
        // fails, recording is off for the whole process, so a connection that is
        // still holding a writer stops here rather than continuing to record (and
        // only future connections observing the flag at setup).
        recorder.honor_latch();

        // Recorder tap, pre-decode: the bytes are captured before anything can
        // reject them.
        recorder.tap(host_rx, &framed);

        let frame = match decode_frame(&framed) {
            Ok(f) => f,
            Err(_) => {
                close_cause = CloseCause::DecodeError;
                break 'read;
            }
        };

        // The FSM holds its own shared ledger handle and locks it only inside the
        // boundary arms (`SegmentStart`/close) that touch it; the O(samples) audio
        // decode runs lock-free, so per-pod decode does not serialize.
        let events = fsm.feed(frame, host_rx);

        // Fold any newly-discarded telemetry-outside-segment into the process
        // aggregate now, so the periodic health line reflects a live misbehaving
        // pod rather than lagging until the connection closes.
        fold_telemetry_outside(&fsm, &telemetry_outside_segment, &mut telemetry_reported);

        let mut fatal = false;
        let mut segment_finalized = false;
        // A wire `SegmentClosed` this feed, tracked independently of assembler
        // output: a host-capped segment's real `SegmentEnd` closes the wire
        // segment but yields no `Segment` from the assembler, and the
        // between-segment flush/roll boundary must still fire at that close.
        let mut saw_segment_closed = false;
        for ev in &events {
            // Tap the live pre-assembly stream to the continuous listener (a no-op
            // when none is wired), before the assembler consumes the same event.
            tap_listener(listener.as_ref(), &mut listener_pod, ev, conn_seq).await;
            match ev {
                SessionEvent::HelloAccepted { pod_id, .. } => {
                    let room = config.room_for(pod_id);
                    current_room = room.room().to_string();
                    recorder.on_hello(pod_id, &accept_iso, conn_seq, host_rx, &framed);
                    assembler = Some(SegmentAssembler::new(
                        PodId(pod_id.clone()),
                        RoomId(current_room.clone()),
                        limits,
                    ));
                    jsonl.emit(
                        "conn_hello",
                        &json!({
                            "pod_id": pod_id,
                            "room": current_room,
                            "conn_seq": conn_seq,
                            "unmapped": room.is_unmapped(),
                        }),
                    );

                    // Supersede: replace any live entry for this pod, then await
                    // the old task's close before reading any further frame. That
                    // ordering is load-bearing — the old task's `close()` writes
                    // the truncation note into the shared `ResumeLedger` (locking
                    // the ledger internally), and that write must strictly precede
                    // this connection's first resume lookup (also under the ledger
                    // lock, inside `open_segment`), or a genuine resume
                    // intermittently misreads as fresh. The await, not the mutex,
                    // enforces the cross-task ordering: each side's ledger access
                    // is atomic under the interior lock, and the old task finishes
                    // its note before the await releases this one.
                    let superseded = {
                        let mut reg = registry.lock().expect("supersede registry poisoned");
                        reg.insert(
                            pod_id.clone(),
                            ConnEntry {
                                conn_seq,
                                cancel: cancel.clone(),
                                finished: finished.clone(),
                            },
                        )
                    };
                    registered_pod = Some(pod_id.clone());
                    if let Some(old) = superseded {
                        jsonl.emit(
                            "conn_superseded",
                            &json!({
                                "pod_id": pod_id,
                                "old_conn_seq": old.conn_seq,
                                "new_conn_seq": conn_seq,
                            }),
                        );
                        old.cancel.cancel();
                        // Await the old task's close — its ledger truncation note
                        // must strictly precede our resume lookup — but abandon
                        // the wait if we are ourselves superseded meanwhile, so a
                        // superseded-while-superseding task cannot hang forever
                        // holding its permit.
                        tokio::select! {
                            _ = old.finished.cancelled() => {}
                            _ = cancel.cancelled() => {
                                close_cause = CloseCause::Superseded;
                                break 'read;
                            }
                        }
                    }

                    // Spawn this connection's paced playback writer on the socket
                    // write half, now the pod identity is known. Its eager `Hello`
                    // validates the outbound path at registration; the handle is
                    // held for the connection's life. A duplicate `Hello` finds the
                    // write half already taken and spawns no second writer.
                    if let Some(wr) = write_half.take() {
                        let handle = PlaybackWriter::spawn(
                            wr,
                            PodId(pod_id.clone()),
                            PacerConfig {
                                lead_ms: config.playback.lead_ms,
                                write_timeout_ms: config.playback.write_timeout_ms,
                                job_queue_depth: config.playback.job_queue_depth,
                            },
                            playback_stats.clone(),
                            playback_events.clone(),
                            playback_cancel.clone(),
                        );
                        playback_register(&playback_registry, pod_id.clone(), conn_seq, handle);
                    }
                }
                SessionEvent::SegmentOpened {
                    segment_id,
                    preroll_samples,
                    is_resume,
                    ..
                } => {
                    jsonl.emit(
                        "segment_opened",
                        &json!({
                            "pod": fsm.pod_id(),
                            "room": current_room,
                            "segment_id": segment_id,
                            "is_resume": is_resume,
                            "preroll": preroll_samples,
                        }),
                    );
                }
                SessionEvent::ProtocolError { kind, fatal: f, .. } => {
                    jsonl.emit(
                        "protocol_error",
                        &json!({
                            "pod": fsm.pod_id(),
                            "kind": kind,
                            "fatal": f,
                        }),
                    );
                    if *f {
                        fatal = true;
                    }
                }
                SessionEvent::Audio { .. } | SessionEvent::Telemetry { .. } => {}
                SessionEvent::SegmentClosed { .. } => {
                    saw_segment_closed = true;
                }
            }

            if let Some(a) = assembler.as_mut() {
                if let Some(seg) = a.on_event(ev, recorder.log_name()) {
                    finalize_segment(
                        seg,
                        conn_seq,
                        &mut recorder,
                        &jsonl,
                        &item_tx,
                        &clock_step_clamps,
                    );
                    segment_finalized = true;
                }
            }
        }

        // Between-segment maintenance: after a segment closes and none is open,
        // flush the writer and roll the log if it is past its size/age threshold.
        // Rolls happen only here, so a `SegmentRef` never spans logs. A roll
        // reclaims the just-closed log as a deletion candidate, so it fires a
        // background prune pass — off the connection task, never inline.
        if (segment_finalized || saw_segment_closed) && !fsm.segment_open() {
            if let Some(pod) = fsm.pod_id().map(str::to_owned) {
                let rolled = recorder.maybe_roll(
                    &pod,
                    conn_seq,
                    config.record.roll_max_bytes,
                    config.record.roll_max_age_s,
                );
                if rolled {
                    // Coalesced + serialized: at most one prune pass runs at a
                    // time, and a burst of rolls collapses into one follow-up
                    // pass rather than one redundant full-store scan per roll.
                    prune_coord.schedule();
                }
            }
        }

        if fatal {
            fatal_close = true;
            break 'read;
        }
    }

    // Single finalize path: close the session (truncating any open segment) and
    // export whatever it yields.
    let events = fsm.close(close_cause, HostMicros::now());
    for ev in &events {
        // The device close reaches the listener as its outer-boundary fallback
        // (missed-onset carve / arm clear), so tap the close-path events too.
        tap_listener(listener.as_ref(), &mut listener_pod, ev, conn_seq).await;
        if let Some(a) = assembler.as_mut() {
            if let Some(seg) = a.on_event(ev, recorder.log_name()) {
                finalize_segment(
                    seg,
                    conn_seq,
                    &mut recorder,
                    &jsonl,
                    &item_tx,
                    &clock_step_clamps,
                );
            }
        }
    }

    // Final reconcile: fold any telemetry-outside-segment counted since the last
    // in-loop fold (the close path adds none today, but this keeps the aggregate
    // exact regardless).
    fold_telemetry_outside(&fsm, &telemetry_outside_segment, &mut telemetry_reported);

    recorder.finish();
    jsonl.emit(
        "conn_closed",
        &json!({
            "pod_id": registered_pod.as_deref(),
            "peer": peer.to_string(),
            "conn_seq": conn_seq,
            "cause": if fatal_close { json!("fatal_protocol") } else { json!(close_cause) },
        }),
    );

    // Deregister, but only if still the current entry: a superseding connection
    // has already replaced us, and must keep owning the slot.
    if let Some(pod) = &registered_pod {
        guarded_remove(&registry, pod, conn_seq, |e| e.conn_seq);
    }

    // Tear down this connection's paced playback writer: cancelling aborts any
    // in-flight job, and removing the handle from the registry drops it, closing
    // its queue so the task exits. Removal is guarded — a superseding successor
    // may already own the slot and must keep it.
    playback_cancel.cancel();
    if let Some(pod) = &registered_pod {
        playback_deregister(&playback_registry, pod, conn_seq);
    }

    // Signal any superseding task waiting on our close — the ledger note above
    // has now landed, so its resume lookup is safe to proceed.
    finished.cancel();
}

/// Stamp, record the sidecar for, log, and enqueue one completed segment. `epoch`
/// is the connection sequence — the same one the listener feed stamps its events
/// with, so the pipeline reads both arms of its queue in one index domain.
fn finalize_segment(
    seg: Segment,
    epoch: u64,
    recorder: &mut Recorder,
    jsonl: &JsonlHandle,
    item_tx: &Sender<crate::pipeline::PipelineItem>,
    clock_step_clamps: &AtomicU64,
) {
    let mut seg = seg;
    // Assembly is synchronous, so this is also the ingest-side handoff time.
    seg.timings.assembled = Some(HostMicros::now());

    // Sidecar (recorder half): the recorder writes its own segment entry and
    // latches recording off on failure.
    recorder.record_segment(&seg);

    let rx_to_assembled_us = stage_delta_us(
        seg.timings.first_frame_rx,
        seg.timings.assembled,
        clock_step_clamps,
    );
    jsonl.emit(
        "segment_closed",
        &SegmentClosedLine {
            pod: &seg.pod.0,
            room: &seg.room.0,
            segment_id: seg.segment_id,
            end_cause: seg.end.cause,
            truncated: seg.end.truncated,
            resumed: seg.end.resumed,
            gap_count: seg.end.gap_count,
            cross_check: seg.end.cross_check,
            samples: seg.pcm.len(),
            audio_ref: &seg.audio_ref,
            timings: &seg.timings,
            rx_to_assembled_us,
        },
    );

    if let Some(displaced) = item_tx.send(crate::pipeline::PipelineItem::Segment { seg, epoch }) {
        let depth = item_tx.stats().depth;
        match &displaced {
            crate::pipeline::PipelineItem::Segment { seg: s, .. } => jsonl.emit(
                "segment_dropped_overflow",
                &json!({ "pod": s.pod.0, "segment_id": s.segment_id, "depth": depth }),
            ),
            crate::pipeline::PipelineItem::Listener(ev) => {
                // A dropped listener event is a lost utterance or wake, not an
                // anonymous segment drop; name the pod and the event kind.
                use speech_pipeline::ListenerEvent::*;
                let (pod, kind) = match ev {
                    WakeDetected { pod, .. } => (&pod.0, "wake_detected"),
                    BargeIn { pod, .. } => (&pod.0, "barge_in"),
                    SoftEndpoint { pod, .. } => (&pod.0, "soft_endpoint"),
                    Superseded { pod, .. } => (&pod.0, "superseded"),
                    UtteranceClosed { pod, .. } => (&pod.0, "utterance_closed"),
                    ArmExpired { pod, .. } => (&pod.0, "arm_expired"),
                    EndpointerTransition { pod, .. } => (&pod.0, "endpointer_transition"),
                    ModelStats { pod, .. } => (&pod.0, "model_stats"),
                };
                jsonl.emit(
                    "listener_event_dropped_overflow",
                    &json!({ "pod": pod, "kind": kind, "depth": depth }),
                );
            }
        }
    }
}

/// Fold the delta of a connection's cumulative telemetry-outside-segment tally
/// into the process aggregate. `reported` tracks how much has already been added,
/// so repeated calls add only the increment since the last call — cheap (one
/// `Copy` `stats()` snapshot and a compare per call).
fn fold_telemetry_outside(fsm: &SessionFsm, aggregate: &AtomicU64, reported: &mut u64) {
    let total = fsm.stats().telemetry_outside_segment;
    if total > *reported {
        aggregate.fetch_add(total - *reported, Ordering::Relaxed);
        *reported = total;
    }
}

/// Run one prune pass over the record store on a blocking thread and render the
/// outcome to JSONL. Fire-and-forget on each roll; awaited once at startup.
/// Never call the pruner inline on a connection task — a full-store pass is
/// unbounded filesystem work that would stall the read loop.
async fn run_prune(
    record_dir: PathBuf,
    cap_bytes: u64,
    pod_cap_bytes: u64,
    open_logs: OpenLogs,
    jsonl: JsonlHandle,
) {
    let result = tokio::task::spawn_blocking(move || {
        // Pass the live open set (not a pass-start snapshot) so `prune` can
        // re-check membership immediately before each deletion — a log opened
        // after this pass began must never be unlinked mid-write.
        prune(&PruneRequest {
            store_dir: &record_dir,
            cap_bytes,
            pod_cap_bytes,
            open_logs: open_logs.as_set(),
        })
    })
    .await;
    match result {
        Ok(Ok(outcome)) => emit_prune_outcome(&outcome, &jsonl),
        // A store scan I/O error and a prune-task panic are distinct faults;
        // the `cause` discriminator keeps them apart in the stream.
        Ok(Err(e)) => jsonl.emit(
            "prune_error",
            &json!({ "cause": "scan", "detail": e.to_string() }),
        ),
        Err(e) => jsonl.emit(
            "prune_error",
            &json!({ "cause": "join", "detail": e.to_string() }),
        ),
    }
}

/// Render a completed prune pass: one `record_pruned` per deletion (carrying its
/// `pod_id` and `reason`), a `prune_delete_error` per failed deletion, a
/// `prune_pod_over_quota` per bucket left over its per-pod quota, and
/// `prune_halted` if the store stayed over the global cap because only
/// pinned/open/undeletable logs remained.
fn emit_prune_outcome(outcome: &PruneOutcome, jsonl: &JsonlHandle) {
    for p in &outcome.pruned {
        jsonl.emit(
            "record_pruned",
            &json!({
                "framelog": p.framelog.display().to_string(),
                "sidecar": p.sidecar.as_ref().map(|s| s.display().to_string()),
                "bytes": p.bytes,
                "tier": p.tier,
                // `pod_id` is null for the shared unattributed bucket; `reason`
                // says which phase (per-pod quota vs global cap) deleted it.
                "pod_id": p.pod_id,
                "reason": p.reason,
            }),
        );
    }
    for f in &outcome.failed {
        jsonl.emit(
            "prune_delete_error",
            &json!({
                "framelog": f.framelog.display().to_string(),
                "error": f.error,
            }),
        );
    }
    for c in &outcome.kept_corrupt {
        // A present-but-unreadable sidecar protected its log from deletion; the
        // daemon's loud channel is the JSONL stream, so the error lands here
        // rather than on a detached stderr.
        jsonl.emit(
            "prune_sidecar_corrupt",
            &json!({
                "framelog": c.framelog.display().to_string(),
                "error": c.error,
            }),
        );
    }
    for q in &outcome.over_quota {
        // A pod bucket stayed over its per-pod quota because its residue is
        // pinned/open/corrupt. Warn-level, analogous to `prune_halted` but
        // scoped to one bucket and not implying the global cap was breached.
        jsonl.emit(
            "prune_pod_over_quota",
            &json!({
                "pod_id": q.pod_id,
                "remaining_bytes": q.remaining_bytes,
                "pod_cap_bytes": q.pod_cap_bytes,
            }),
        );
    }
    if let Some(halt) = &outcome.halted {
        jsonl.emit(
            "prune_halted",
            &json!({
                "remaining_bytes": halt.remaining_bytes,
                "cap_bytes": halt.cap_bytes,
            }),
        );
    }
}

/// The counter sources every `stage_health` line reads: the segment-queue
/// boundary view, the per-stage counters, and the process-wide observability
/// tallies. Built once in `run_with_gate`, cloned into the periodic emitter task
/// and read again for the final at-shutdown line — one handle set feeds both, so
/// a new counter source is added in one place rather than across two signatures.
#[derive(Clone)]
struct HealthSources {
    stats: StatsHandle<crate::pipeline::PipelineItem>,
    // `None` when no continuous listener is wired (no `[wake] oww` + `[endpointer]`);
    // the health line omits the block rather than fabricating zero counters.
    listener_stats: Option<Arc<ListenerStats>>,
    playback_stats: Arc<PlaybackStats>,
    brain_stats: Arc<BrainStats>,
    router_stats: Arc<RouterStats>,
    // `None` when no `[stt]`/`[tts]` stage is wired; the health line omits the
    // block rather than fabricating zero counters for an absent stage.
    stt_stats: Option<Arc<SttStats>>,
    tts_stats: Option<Arc<TtsStats>>,
    jsonl: JsonlHandle,
    ledger: Arc<Mutex<ResumeLedger>>,
    telemetry_outside_segment: Arc<AtomicU64>,
    clock_step_clamps: Arc<AtomicU64>,
}

/// Periodically emit a `stage_health` line aggregating every boundary's
/// counters until shutdown. A zero period disables the periodic pass (the final
/// shutdown line still fires); otherwise the immediate first interval tick is
/// consumed so the first emit is one period in, not at startup. Cancelled by the
/// shutdown token so `run` can join it before emitting the final line.
async fn stage_health_emitter(
    period: Duration,
    sources: HealthSources,
    shutdown: CancellationToken,
) {
    if period.is_zero() {
        // Periodic health disabled; still await shutdown so the task joins.
        shutdown.cancelled().await;
        return;
    }
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await; // Consume the immediate first tick (fires at t=0).
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = interval.tick() => emit_stage_health(&sources, false),
        }
    }
}

/// Emit one `stage_health` line: the segment-queue boundary counters, wake stage
/// counters, playback/brain/router counters, the stt/tts request counters (when
/// those stages are wired), JSONL and console sink drops, resume-ledger evictions,
/// telemetry-outside-segment count, and clock-step clamp count. `at_shutdown`
/// marks the final line emitted after the pipeline drains.
fn emit_stage_health(sources: &HealthSources, at_shutdown: bool) {
    let ledger_evictions = sources
        .ledger
        .lock()
        .expect("resume ledger mutex poisoned")
        .evictions();
    sources.jsonl.emit(
        "stage_health",
        &StageHealthLine {
            segment_queue: sources.stats.stats(),
            listener: sources.listener_stats.as_ref().map(|s| s.snapshot()),
            playback: sources.playback_stats.snapshot(),
            brain: sources.brain_stats.snapshot(),
            router: sources.router_stats.snapshot(),
            stt: sources.stt_stats.as_ref().map(|s| s.snapshot()),
            tts: sources.tts_stats.as_ref().map(|s| s.snapshot()),
            jsonl_dropped: sources.jsonl.dropped(),
            console_dropped: sources.jsonl.console_dropped(),
            ledger_evictions,
            telemetry_outside_segment: sources.telemetry_outside_segment.load(Ordering::Relaxed),
            clock_step_clamps: sources.clock_step_clamps.load(Ordering::Relaxed),
            at_shutdown,
        },
    );
}

/// The `stage_health` JSONL line: the segment-queue boundary counters, the wake
/// stage counters, the playback/brain/router counters, the stt/tts request
/// counters (omitted when the stage is absent), plus the process-wide
/// observability tallies.
#[derive(Serialize)]
struct StageHealthLine {
    segment_queue: QueueStats,
    // Omitted when no continuous listener is wired — an absent listener has no
    // counters, distinct from a wired one that has seen no feeds.
    #[serde(skip_serializing_if = "Option::is_none")]
    listener: Option<ListenerStatsSnapshot>,
    playback: PlaybackStatsSnapshot,
    brain: BrainStatsSnapshot,
    router: RouterStatsSnapshot,
    // Omitted from the line when no `[stt]`/`[tts]` stage is wired — an absent
    // HTTP backend has no counters, distinct from a wired stage that has served
    // zero requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    stt: Option<SttStatsSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tts: Option<TtsStatsSnapshot>,
    jsonl_dropped: u64,
    console_dropped: u64,
    ledger_evictions: u64,
    telemetry_outside_segment: u64,
    clock_step_clamps: u64,
    at_shutdown: bool,
}

/// The `segment_closed` JSONL line: identity, end info, and the ingest-stage
/// timings (stamps plus the receive→assembled delta).
#[derive(Serialize)]
struct SegmentClosedLine<'a> {
    pod: &'a str,
    room: &'a str,
    segment_id: u32,
    end_cause: SegmentEndCause,
    truncated: bool,
    resumed: bool,
    gap_count: u32,
    cross_check: Option<CrossCheck>,
    samples: usize,
    audio_ref: &'a SegmentRef,
    timings: &'a StageTimings,
    rx_to_assembled_us: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    use audio_pipeline::wire::{
        encode_frame, AudioFrame, ChannelSource, Codec, EndReason, Hello, SegmentEnd, SegmentStart,
        StreamFrame, Telemetry, TelemetryKind, AUDIO_PROTOCOL_VERSION, MAX_AUDIO_PAYLOAD,
    };
    use serde_json::Value;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    use crate::config::Config;
    use crate::jsonl::{self, JsonlHandle};
    use crate::recorder::{sidecar_path, Sidecar, SidecarSegment};

    /// The listener's marker-timeout counter reaches the `stage_health` line
    /// operators read, under that exact name — the counter exists to make a wedged
    /// listener visible, which a serialization gap would silently defeat.
    #[test]
    fn stage_health_reports_the_marker_timeout_counter() {
        let queue = QueueStats {
            depth: 0,
            high_water: 0,
            pushed: 0,
            dropped_oldest: 0,
            send_failures: 0,
        };
        let line = StageHealthLine {
            segment_queue: queue,
            listener: Some(ListenerStats::default().snapshot()),
            playback: speech_pipeline::playback::PlaybackStats::default().snapshot(),
            brain: speech_pipeline::brain::BrainStats::default().snapshot(),
            router: crate::playback_router::RouterStats::default().snapshot(),
            stt: None,
            tts: None,
            jsonl_dropped: 0,
            console_dropped: 0,
            ledger_evictions: 0,
            telemetry_outside_segment: 0,
            clock_step_clamps: 0,
            at_shutdown: false,
        };
        let value = serde_json::to_value(&line).expect("serializes");
        assert_eq!(
            value["listener"]["marker_send_timeouts"],
            Value::from(0_u64),
            "line: {value}"
        );
    }

    /// The exact on-wire frame (`[u16 len][postcard]`), which is what both the
    /// recorder tap and `decode_frame` consume.
    fn framed(frame: &StreamFrame) -> Vec<u8> {
        let mut buf = [0u8; MAX_AUDIO_PAYLOAD + 64];
        let n = encode_frame(frame, &mut buf).expect("frame fits");
        buf[..n].to_vec()
    }

    fn hello(source: ChannelSource) -> StreamFrame {
        StreamFrame::Hello(Hello {
            version: AUDIO_PROTOCOL_VERSION,
            pod_id: heapless::String::try_from("pod-srv").unwrap(),
            sample_rate_hz: 16_000,
            bits_per_sample: 16,
            channels: 1,
            codec: Codec::S16Le,
            channel_source: source,
        })
    }

    fn audio(segment_id: u32, first: u64, n: usize) -> StreamFrame {
        let mut pcm: heapless::Vec<u8, MAX_AUDIO_PAYLOAD> = heapless::Vec::new();
        for i in 0..n {
            let v = (i as i16).to_le_bytes();
            pcm.push(v[0]).unwrap();
            pcm.push(v[1]).unwrap();
        }
        StreamFrame::Audio(AudioFrame {
            segment_id,
            first_sample_index: first,
            device_ts_us: 0,
            pcm,
        })
    }

    /// Read one length-prefixed wire frame off `client` and decode it — the
    /// server's half of the socket (playback writes, in particular).
    async fn read_frame(client: &mut TcpStream) -> StreamFrame {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut len = [0u8; 2];
            client.read_exact(&mut len).await.unwrap();
            let payload_len = u16::from_le_bytes(len) as usize;
            let mut buf = vec![0u8; 2 + payload_len];
            buf[..2].copy_from_slice(&len);
            client.read_exact(&mut buf[2..]).await.unwrap();
            decode_frame(&buf).expect("decode server frame")
        })
        .await
        .expect("server frame within timeout")
    }

    /// Build a config bound to an ephemeral loopback port, recording into `dir`.
    fn config(dir: &Path, record: bool) -> Arc<Config> {
        let text = format!(
            "listen_addr = \"127.0.0.1:0\"\n\
             [record]\nenabled = {record}\ndir = {:?}\n\
             [pods.pod-srv]\nroom = \"kitchen\"\n",
            dir.to_str().unwrap()
        );
        Arc::new(Config::parse(&text).expect("config parses"))
    }

    /// Spawn a server on an ephemeral port, returning its address, the JSONL
    /// handle, and a shutdown trigger + the run join handle.
    /// Bind a server (optionally with a test router override) and create its
    /// stop channel, returning the pieces the two spawn helpers share. Only the
    /// run-task's error handling (`expect` vs returned `io::Result`) differs
    /// between them, so that stays at each call site.
    async fn bind_with_stop(
        config: Arc<Config>,
        jsonl: JsonlHandle,
        router: Option<tokio::task::JoinHandle<()>>,
    ) -> (
        Server,
        std::net::SocketAddr,
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut server = Server::bind(config, jsonl).await.expect("bind");
        if let Some(router) = router {
            server = server.with_router_override(router);
        }
        let addr = server.local_addr().unwrap();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        (server, addr, stop_tx, stop_rx)
    }

    /// Bind, spawn the run task, and return the handles. When `reg` is `Some`,
    /// the run task uses that injected playback registry so the caller can observe
    /// register/deregister through a real lifecycle.
    async fn spawn_server_inner(
        config: Arc<Config>,
        jsonl: JsonlHandle,
        reg: Option<PlaybackRegistry>,
    ) -> (
        std::net::SocketAddr,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let (mut server, addr, stop_tx, stop_rx) = bind_with_stop(config, jsonl, None).await;
        if let Some(reg) = reg {
            server = server.with_playback_registry_override(reg);
        }
        let join = tokio::spawn(async move {
            server
                .run(async move {
                    let _ = stop_rx.await;
                })
                .await
                .expect("run");
        });
        (addr, stop_tx, join)
    }

    async fn spawn_server(
        config: Arc<Config>,
        jsonl: JsonlHandle,
    ) -> (
        std::net::SocketAddr,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        spawn_server_inner(config, jsonl, None).await
    }

    /// Like `spawn_server`, but injects a playback registry the caller retains,
    /// so the test observes register/deregister through a real lifecycle.
    async fn spawn_server_with_playback_registry(
        config: Arc<Config>,
        jsonl: JsonlHandle,
        reg: PlaybackRegistry,
    ) -> (
        std::net::SocketAddr,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        spawn_server_inner(config, jsonl, Some(reg)).await
    }

    /// Spawn a file-backed JSONL sink, returning the emit handle, the writer's
    /// join handle (await it after dropping every handle to flush), and the path.
    async fn jsonl_file(dir: &Path) -> (JsonlHandle, tokio::task::JoinHandle<()>, PathBuf) {
        let path = dir.join("events.jsonl");
        let (handle, join) = jsonl::spawn_quiet(&crate::config::JsonlSink::File(path.clone()))
            .await
            .unwrap();
        (handle, join, path)
    }

    use crate::test_support::write_spine_wav;

    /// A `[brain] mode = "wav"` config recording into `store`, with `clip` present
    /// only when given — so the loaded, missing-file, and missing-path build_brain
    /// tests share one TOML shape instead of three near-identical copies.
    fn wav_brain_config(store: &Path, clip: Option<&Path>) -> Arc<Config> {
        let clip_line = match clip {
            Some(p) => format!("clip = {:?}\n", p.to_str().unwrap()),
            None => String::new(),
        };
        let text = format!(
            "listen_addr = \"127.0.0.1:0\"\n\
             [record]\nenabled = false\ndir = {:?}\n\
             [brain]\nmode = \"wav\"\n{clip_line}",
            store.to_str().unwrap(),
        );
        Arc::new(Config::parse(&text).expect("config parses"))
    }

    #[tokio::test]
    async fn build_brain_absent_returns_none_and_emits_line() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(&dir.path().join("store"), false);
        let (handle, join, path) = jsonl_file(dir.path()).await;

        let (brain, _, stats) = build_brain(&cfg, &handle).unwrap();
        assert!(brain.is_none());
        assert_eq!(stats.snapshot().speak_send_failures, 0);

        drop(handle);
        join.await.unwrap();
        let lines = read_lines(&path);
        assert_eq!(events_named(&lines, "brain_absent").len(), 1);
        assert!(events_named(&lines, "brain_clip_loaded").is_empty());
    }

    #[tokio::test]
    async fn build_brain_wav_loads_clip_and_emits_line() {
        let dir = tempfile::tempdir().unwrap();
        let clip_path = dir.path().join("ack.wav");
        // 1600 samples at 16 kHz = 100 ms.
        write_spine_wav(&clip_path, &vec![0i16; 1600]);
        let cfg = wav_brain_config(&dir.path().join("store"), Some(&clip_path));
        let (handle, join, path) = jsonl_file(dir.path()).await;

        let (brain, _, _stats) = build_brain(&cfg, &handle).unwrap();
        assert!(brain.is_some());

        // The brain's event closure holds its own clone of the sink (so it can
        // emit `brain_sink_full` later); drop it too, or the writer's channel
        // never sees its last sender go and `join` hangs forever.
        drop(brain);
        drop(handle);
        join.await.unwrap();
        let lines = read_lines(&path);
        let loaded = events_named(&lines, "brain_clip_loaded");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0]["samples"], 1600);
        assert_eq!(loaded[0]["duration_ms"], 100);
        assert!(events_named(&lines, "brain_absent").is_empty());
    }

    #[tokio::test]
    async fn build_brain_wav_missing_clip_is_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope.wav");
        let cfg = wav_brain_config(&dir.path().join("store"), Some(&missing));
        let (handle, _join, _path) = jsonl_file(dir.path()).await;

        // `Arc<dyn Brain>` is not `Debug`, so match rather than `unwrap_err`.
        let result = build_brain(&cfg, &handle);
        assert!(matches!(result, Err(ClipError::Open { .. })));
    }

    #[tokio::test]
    async fn build_brain_wav_without_clip_errors_rather_than_panics() {
        // `Config::validate` rejects this, but the library path into `build_brain`
        // does not re-validate: a wav-mode config with no clip must return a clean
        // `MissingPath` error, not panic on an internal `expect`.
        let dir = tempfile::tempdir().unwrap();
        let cfg = wav_brain_config(&dir.path().join("store"), None);
        let (handle, _join, _path) = jsonl_file(dir.path()).await;

        assert!(matches!(
            build_brain(&cfg, &handle),
            Err(ClipError::MissingPath)
        ));
    }

    #[tokio::test]
    async fn brain_event_adapter_maps_sink_full() {
        use speech_pipeline::UtteranceId;

        let dir = tempfile::tempdir().unwrap();
        let (handle, join, path) = jsonl_file(dir.path()).await;

        let adapter = brain_event_adapter(handle.clone());
        adapter(BrainEvent::SinkFull {
            utterance: UtteranceId(7),
        });

        // The adapter closure holds its own clone of the sink; drop it too, or
        // the writer's channel never sees its last sender go and `join` hangs.
        drop(adapter);
        drop(handle);
        join.await.unwrap();
        let lines = read_lines(&path);
        let sink_full = events_named(&lines, "brain_sink_full");
        assert_eq!(sink_full.len(), 1);
        assert_eq!(sink_full[0]["utterance"], 7);
    }

    #[tokio::test]
    async fn brain_event_adapter_maps_no_transcript() {
        use speech_pipeline::UtteranceId;

        let dir = tempfile::tempdir().unwrap();
        let (handle, join, path) = jsonl_file(dir.path()).await;

        let adapter = brain_event_adapter(handle.clone());
        adapter(BrainEvent::NoTranscript {
            utterance: UtteranceId(11),
        });

        drop(adapter);
        drop(handle);
        join.await.unwrap();
        let lines = read_lines(&path);
        let no_transcript = events_named(&lines, "brain_no_transcript");
        assert_eq!(no_transcript.len(), 1);
        assert_eq!(no_transcript[0]["utterance"], 11);
    }

    #[tokio::test]
    async fn brain_event_adapter_maps_wake_command_absent() {
        use pod_ingest::SegmentRef;
        use speech_pipeline::{AudioSpan, UtteranceId};

        let dir = tempfile::tempdir().unwrap();
        let (handle, join, path) = jsonl_file(dir.path()).await;

        let adapter = brain_event_adapter(handle.clone());
        adapter(BrainEvent::WakeCommandAbsent {
            utterance: UtteranceId(7),
            audio_ref: AudioSpan {
                log: "pod-fbe2f8_0.framelog".into(),
                start_sample: 1_000,
                end_sample: 41_000,
                segments: vec![SegmentRef {
                    log: "pod-fbe2f8_0.framelog".into(),
                    segment_id: 8,
                    part: 0,
                }],
            },
            score: 0.998,
            wake_end_sample: 39_040,
            stt_trim_samples: 35_840,
            reason: WakeCommandReason::Empty,
        });

        drop(adapter);
        drop(handle);
        join.await.unwrap();
        let lines = read_lines(&path);
        // Its own event name, never the generic no-transcript error path, and it
        // carries the wake context plus the audio-span reference for retrieval.
        assert!(events_named(&lines, "brain_no_transcript").is_empty());
        let absent = events_named(&lines, "wake_command_absent");
        assert_eq!(absent.len(), 1);
        assert_eq!(absent[0]["utterance"], 7);
        assert_eq!(absent[0]["log"], "pod-fbe2f8_0.framelog");
        assert_eq!(absent[0]["start_sample"], 1_000);
        assert_eq!(absent[0]["end_sample"], 41_000);
        assert_eq!(absent[0]["segments"][0]["segment_id"], 8);
        // The score rides as an `f32` widened to JSON `f64`; compare with tolerance.
        assert!((absent[0]["score"].as_f64().unwrap() - 0.998).abs() < 1e-6);
        assert_eq!(absent[0]["wake_end_sample"], 39_040);
        assert_eq!(absent[0]["stt_trim_samples"], 35_840);
        // The empty cause is labelled and carries no confidence numbers.
        assert_eq!(absent[0]["reason"], "empty");
        assert!(absent[0].get("no_speech").is_none());
    }

    #[tokio::test]
    async fn brain_event_adapter_labels_low_confidence_with_numbers() {
        use pod_ingest::SegmentRef;
        use speech_pipeline::{AudioSpan, UtteranceId};

        let dir = tempfile::tempdir().unwrap();
        let (handle, join, path) = jsonl_file(dir.path()).await;

        let adapter = brain_event_adapter(handle.clone());
        adapter(BrainEvent::WakeCommandAbsent {
            utterance: UtteranceId(9),
            audio_ref: AudioSpan {
                log: "pod-fbe2f8_0.framelog".into(),
                start_sample: 1_000,
                end_sample: 41_000,
                segments: vec![SegmentRef {
                    log: "pod-fbe2f8_0.framelog".into(),
                    segment_id: 8,
                    part: 0,
                }],
            },
            score: 0.981,
            wake_end_sample: 39_040,
            stt_trim_samples: 35_840,
            reason: WakeCommandReason::LowConfidence {
                no_speech_prob: 0.37,
                avg_logprob: -0.99,
            },
        });

        drop(adapter);
        drop(handle);
        join.await.unwrap();
        let lines = read_lines(&path);
        // Still the non-error no-command event, but the reason and the offending
        // signals ride the line so it is distinguishable from the empty case.
        assert!(events_named(&lines, "brain_no_transcript").is_empty());
        let absent = events_named(&lines, "wake_command_absent");
        assert_eq!(absent.len(), 1);
        assert_eq!(absent[0]["reason"], "low_confidence");
        assert!((absent[0]["no_speech"].as_f64().unwrap() - 0.37).abs() < 1e-6);
        assert!((absent[0]["logprob"].as_f64().unwrap() - -0.99).abs() < 1e-6);
    }

    #[tokio::test]
    async fn brain_event_adapter_maps_barge_command_absent() {
        use pod_ingest::SegmentRef;
        use speech_pipeline::{AudioSpan, UtteranceId};

        let dir = tempfile::tempdir().unwrap();
        let (handle, join, path) = jsonl_file(dir.path()).await;

        let adapter = brain_event_adapter(handle.clone());
        adapter(BrainEvent::BargeCommandAbsent {
            utterance: UtteranceId(11),
            audio_ref: AudioSpan {
                log: "pod-fbe2f8_0.framelog".into(),
                start_sample: 2_000,
                end_sample: 42_000,
                segments: vec![SegmentRef {
                    log: "pod-fbe2f8_0.framelog".into(),
                    segment_id: 9,
                    part: 0,
                }],
            },
            no_speech_prob: 0.42,
            avg_logprob: -1.10,
        });

        drop(adapter);
        drop(handle);
        join.await.unwrap();
        let lines = read_lines(&path);
        // Its own event name, carrying the barge mark and offending signals — never
        // the wake vocabulary (a barge has no wake score to report).
        assert!(events_named(&lines, "wake_command_absent").is_empty());
        let absent = events_named(&lines, "barge_command_absent");
        assert_eq!(absent.len(), 1);
        assert_eq!(absent[0]["utterance"], 11);
        assert_eq!(absent[0]["log"], "pod-fbe2f8_0.framelog");
        assert_eq!(absent[0]["start_sample"], 2_000);
        assert_eq!(absent[0]["end_sample"], 42_000);
        assert_eq!(absent[0]["segments"][0]["segment_id"], 9);
        assert_eq!(absent[0]["reason"], "low_confidence");
        assert!((absent[0]["no_speech"].as_f64().unwrap() - 0.42).abs() < 1e-6);
        assert!((absent[0]["logprob"].as_f64().unwrap() - -1.10).abs() < 1e-6);
        assert!(absent[0].get("score").is_none());
    }

    #[tokio::test]
    async fn build_brain_echo_builds_and_emits_line() {
        let dir = tempfile::tempdir().unwrap();
        let text = format!(
            "listen_addr = \"127.0.0.1:0\"\n\
             [record]\nenabled = false\ndir = {:?}\n\
             [brain]\nmode = \"echo\"\n",
            dir.path().join("store").to_str().unwrap(),
        );
        let cfg = Arc::new(Config::parse(&text).expect("config parses"));
        let (handle, join, path) = jsonl_file(dir.path()).await;

        let (brain, _, _stats) = build_brain(&cfg, &handle).unwrap();
        assert!(brain.is_some());

        // The event closure holds its own clone of the sink; drop the brain too, or
        // the writer's channel never sees its last sender go and `join` hangs.
        drop(brain);
        drop(handle);
        join.await.unwrap();
        let lines = read_lines(&path);
        assert_eq!(events_named(&lines, "brain_echo").len(), 1);
        assert!(events_named(&lines, "brain_clip_loaded").is_empty());
    }

    #[tokio::test]
    async fn build_transcriber_absent_returns_none_and_emits_line() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(&dir.path().join("store"), false);
        let (handle, join, path) = jsonl_file(dir.path()).await;

        let transcriber = build_transcriber(&cfg, &handle).unwrap();
        assert!(transcriber.is_none());

        drop(handle);
        join.await.unwrap();
        let lines = read_lines(&path);
        assert_eq!(events_named(&lines, "stt_absent").len(), 1);
        assert!(events_named(&lines, "stt_configured").is_empty());
    }

    #[tokio::test]
    async fn build_transcriber_http_returns_some_and_emits_configured_line() {
        let dir = tempfile::tempdir().unwrap();
        let text = format!(
            "listen_addr = \"127.0.0.1:0\"\n\
             [record]\nenabled = false\ndir = {:?}\n\
             [stt]\nbackend = \"http\"\nurl = \"http://127.0.0.1:8000\"\n\
             model = \"whisper-small\"\nlanguage = \"en\"\n",
            dir.path().join("store").to_str().unwrap(),
        );
        let cfg = Arc::new(Config::parse(&text).expect("config parses"));
        let (handle, join, path) = jsonl_file(dir.path()).await;

        let transcriber = build_transcriber(&cfg, &handle).unwrap();
        assert!(transcriber.is_some());

        drop(transcriber);
        drop(handle);
        join.await.unwrap();
        let lines = read_lines(&path);
        let configured = events_named(&lines, "stt_configured");
        assert_eq!(configured.len(), 1);
        assert_eq!(configured[0]["url"], "http://127.0.0.1:8000");
        assert_eq!(configured[0]["model"], "whisper-small");
        assert!(events_named(&lines, "stt_absent").is_empty());
    }

    #[tokio::test]
    async fn build_synthesizer_absent_returns_none_and_emits_line() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(&dir.path().join("store"), false);
        let (handle, join, path) = jsonl_file(dir.path()).await;

        let synthesizer = build_synthesizer(&cfg, &handle).unwrap();
        assert!(synthesizer.is_none());

        drop(handle);
        join.await.unwrap();
        let lines = read_lines(&path);
        assert_eq!(events_named(&lines, "tts_absent").len(), 1);
        assert!(events_named(&lines, "tts_configured").is_empty());
    }

    #[tokio::test]
    async fn build_synthesizer_http_returns_some_and_emits_configured_line() {
        let dir = tempfile::tempdir().unwrap();
        let text = format!(
            "listen_addr = \"127.0.0.1:0\"\n\
             [record]\nenabled = false\ndir = {:?}\n\
             [tts]\nbackend = \"http\"\nurl = \"http://127.0.0.1:8000\"\n\
             model = \"kokoro\"\nvoice = \"af_heart\"\n",
            dir.path().join("store").to_str().unwrap(),
        );
        let cfg = Arc::new(Config::parse(&text).expect("config parses"));
        let (handle, join, path) = jsonl_file(dir.path()).await;

        let synthesizer = build_synthesizer(&cfg, &handle).unwrap();
        assert!(synthesizer.is_some());

        drop(synthesizer);
        drop(handle);
        join.await.unwrap();
        let lines = read_lines(&path);
        let configured = events_named(&lines, "tts_configured");
        assert_eq!(configured.len(), 1);
        assert_eq!(configured[0]["url"], "http://127.0.0.1:8000");
        assert_eq!(configured[0]["model"], "kokoro");
        assert!(events_named(&lines, "tts_absent").is_empty());
    }

    #[tokio::test]
    async fn handle_router_exit_reports_a_panicked_router() {
        let dir = tempfile::tempdir().unwrap();
        let (handle, join, path) = jsonl_file(dir.path()).await;

        // A task that panics stands in for a mid-run router panic; its `JoinError`
        // is exactly what `router_join.await` yields at shutdown.
        let panicker = tokio::spawn(async { panic!("router boom") });
        handle_router_exit(panicker.await, &handle);

        drop(handle);
        join.await.unwrap();
        let lines = read_lines(&path);
        let exited = events_named(&lines, "playback_router_exited");
        assert_eq!(exited.len(), 1);
        assert!(
            exited[0]["detail"].as_str().unwrap().contains("panic"),
            "detail names the panic: {:?}",
            exited[0]["detail"]
        );
    }

    /// Spawn a server with a test-supplied router handle, returning its address,
    /// a shutdown trigger, and the run join handle — whose `io::Result` is
    /// returned (not `expect`ed) so a mid-run fault's `Err` exit is assertable.
    async fn spawn_server_with_router(
        config: Arc<Config>,
        jsonl: JsonlHandle,
        router: tokio::task::JoinHandle<()>,
    ) -> (
        std::net::SocketAddr,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<std::io::Result<()>>,
    ) {
        let (server, addr, stop_tx, stop_rx) = bind_with_stop(config, jsonl, Some(router)).await;
        let join = tokio::spawn(async move {
            server
                .run(async move {
                    let _ = stop_rx.await;
                })
                .await
        });
        (addr, stop_tx, join)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_panic_midrun_is_prompt_and_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("store");
        let (handle, join, jsonl_path) = jsonl_file(dir.path()).await;

        // A panicking router stands in for a mid-run router death. The
        // supervision arm observes it and reports promptly — no shutdown is ever
        // requested.
        let router = tokio::spawn(async { panic!("router boom") });
        let (_addr, _stop, run_join) =
            spawn_server_with_router(config(&store, false), handle.clone(), router).await;

        let exited = wait_for_event(&jsonl_path, "playback_router_exited", |v| {
            v["event"] == "playback_router_exited" && v["reason"] == "panic"
        })
        .await;
        // The detail must carry the underlying panic message, not just the tag —
        // a garbled/empty detail would defeat the diagnostic report.
        assert!(
            exited["detail"].as_str().unwrap().contains("router boom"),
            "panic detail names the payload: {:?}",
            exited["detail"]
        );

        // The fault tears the server down on its own and exits nonzero.
        let result = run_join.await.unwrap();
        assert!(result.is_err(), "mid-run router death is a nonzero exit");

        drop(handle);
        join.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_clean_exit_midrun_is_prompt_and_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("store");
        let (handle, join, jsonl_path) = jsonl_file(dir.path()).await;

        // A router that returns cleanly mid-run — the dropped-sender case — is
        // just as fatal as a panic and must be reported promptly.
        let router = tokio::spawn(async {});
        let (_addr, _stop, run_join) =
            spawn_server_with_router(config(&store, false), handle.clone(), router).await;

        let exited = wait_for_event(&jsonl_path, "playback_router_exited", |v| {
            v["event"] == "playback_router_exited" && v["reason"] == "clean_exit"
        })
        .await;
        assert_eq!(
            exited["detail"], "playback router exited mid-run",
            "clean-exit detail is the fixed string"
        );

        let result = run_join.await.unwrap();
        assert!(result.is_err(), "mid-run clean exit is a nonzero exit");

        drop(handle);
        join.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn healthy_router_shutdown_stays_silent() {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("store");
        let clip_path = dir.path().join("ack.wav");
        write_spine_wav(&clip_path, &vec![0i16; 1600]);
        let (handle, join, jsonl_path) = jsonl_file(dir.path()).await;

        // A real wired brain spawns the real router, which exits cleanly on the
        // shutdown token — the shutdown-path join stays silent.
        let cfg = wav_brain_config(&store, Some(&clip_path));
        let (_addr, stop, run_join) = spawn_server(cfg, handle.clone()).await;

        wait_for_event(&jsonl_path, "listening", |v| v["event"] == "listening").await;
        stop.send(()).unwrap();
        run_join.await.unwrap();

        drop(handle);
        join.await.unwrap();
        let lines = read_lines(&jsonl_path);
        assert!(
            events_named(&lines, "playback_router_exited").is_empty(),
            "a clean shutdown emits no router-exit line"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_brain_router_arm_is_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("store");
        let (handle, join, jsonl_path) = jsonl_file(dir.path()).await;

        // No brain wired: no router task, the arm's `is_some` guard keeps it
        // disabled, and start/stop is clean with no emission.
        let (_addr, stop, run_join) = spawn_server(config(&store, false), handle.clone()).await;
        wait_for_event(&jsonl_path, "listening", |v| v["event"] == "listening").await;
        stop.send(()).unwrap();
        run_join.await.unwrap();

        drop(handle);
        join.await.unwrap();
        let lines = read_lines(&jsonl_path);
        assert!(events_named(&lines, "playback_router_exited").is_empty());
    }

    /// Read every JSONL line the file sink captured, as parsed values.
    fn read_lines(path: &Path) -> Vec<Value> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    fn events_named<'a>(lines: &'a [Value], event: &str) -> Vec<&'a Value> {
        lines.iter().filter(|v| v["event"] == event).collect()
    }

    /// Poll `probe` every 10ms up to 500 times, yielding its first `Some`, or
    /// `None` if it never produced one. Callers that want a plain assertion use
    /// `poll_until`; callers with extra failure context to report handle the
    /// `None` themselves.
    async fn try_poll_until<T>(probe: impl Fn() -> Option<T>) -> Option<T> {
        for _ in 0..500 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            if let Some(v) = probe() {
                return Some(v);
            }
        }
        None
    }

    async fn poll_until<T>(what: &str, probe: impl Fn() -> Option<T>) -> T {
        match try_poll_until(probe).await {
            Some(v) => v,
            None => panic!("{what} never appeared"),
        }
    }

    /// Poll the JSONL file until a line satisfying `pred` appears, so a test's
    /// setup ordering (a connection registered, or a segment opened, before the
    /// next step) rests on the observed event rather than a bare sleep. Reads
    /// leniently: a concurrently-flushing writer can leave a torn tail line,
    /// which parses to `None` and is skipped.
    async fn wait_for_event(path: &Path, what: &str, pred: impl Fn(&Value) -> bool) -> Value {
        poll_until(what, || {
            let text = std::fs::read_to_string(path).unwrap_or_default();
            text.lines()
                .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                .find(|v| pred(v))
        })
        .await
    }

    /// Poll the playback registry snapshot until `pred` holds. Register and
    /// deregister emit no JSONL, and every candidate fence event fires before its
    /// mutation lands, so a bare read after a `wait_for_event` barrier is racy;
    /// this bounded poll (same discipline as `wait_for_event`) makes the read wait
    /// for the terminal map state within its window.
    async fn wait_for_registry(
        reg: &PlaybackRegistry,
        what: &str,
        pred: impl Fn(&HashMap<String, u64>) -> bool,
    ) {
        // On timeout, report the final snapshot: the registry leaves no on-disk
        // artifact to inspect afterwards the way the JSONL waits do, and the
        // snapshot is what distinguishes a missing entry from a mis-keyed one.
        if try_poll_until(|| pred(&playback_registry_conn_seqs(reg)).then_some(()))
            .await
            .is_none()
        {
            let snap = playback_registry_conn_seqs(reg);
            panic!("{what} never appeared; last snapshot: {snap:?}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_emits_listening_with_resolved_ephemeral_addr() {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("store");
        let jsonl_path = dir.path().join("events.jsonl");
        let (handle, join) =
            jsonl::spawn_quiet(&crate::config::JsonlSink::File(jsonl_path.clone()))
                .await
                .unwrap();

        let (addr, stop, run_join) = spawn_server(config(&store, false), handle.clone()).await;

        // The `:0` bind resolved to a concrete port; the event reports it, not
        // the configured `127.0.0.1:0` string.
        wait_for_event(&jsonl_path, "listening", |v| v["event"] == "listening").await;

        stop.send(()).unwrap();
        run_join.await.unwrap();
        drop(handle);
        join.await.unwrap();

        let lines = read_lines(&jsonl_path);
        let listening = events_named(&lines, "listening");
        assert_eq!(listening.len(), 1);
        assert_eq!(listening[0]["addr"], addr.to_string());
        assert_ne!(addr.port(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connection_writer_sends_eager_hello() {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("store");
        let jsonl_path = dir.path().join("events.jsonl");
        let (handle, join) =
            jsonl::spawn_quiet(&crate::config::JsonlSink::File(jsonl_path.clone()))
                .await
                .unwrap();

        let (addr, stop, run_join) = spawn_server(config(&store, false), handle.clone()).await;

        // A pod connects and identifies itself; the server registers it and spawns
        // a paced playback writer on the write half, which writes one leading
        // `Hello` back before any playback.
        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&framed(&hello(ChannelSource::AsrBeam)))
            .await
            .unwrap();

        // Read the server's eager `Hello` frame back off the same socket.
        let frame = read_frame(&mut client).await;

        match frame {
            StreamFrame::Hello(h) => {
                // The writer names the surface as the sender, not the pod, and
                // advertises the spine format on the communication beam.
                assert_eq!(h.version, AUDIO_PROTOCOL_VERSION);
                assert_eq!(h.pod_id.as_str(), "speech-surface");
                assert_eq!(h.sample_rate_hz, 16_000);
                assert_eq!(h.bits_per_sample, 16);
                assert_eq!(h.channels, 1);
                assert_eq!(h.codec, Codec::S16Le);
                assert_eq!(h.channel_source, ChannelSource::CommunicationBeam);
            }
            other => panic!("expected server Hello, got {other:?}"),
        }

        stop.send(()).unwrap();
        run_join.await.unwrap();
        drop(client);
        drop(handle);
        join.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connection_emits_playback_hello_line() {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("store");
        let jsonl_path = dir.path().join("events.jsonl");
        let (handle, join) =
            jsonl::spawn_quiet(&crate::config::JsonlSink::File(jsonl_path.clone()))
                .await
                .unwrap();

        let (addr, stop, run_join) = spawn_server(config(&store, false), handle.clone()).await;

        // A pod connects and identifies itself; the writer spawned on its write
        // half emits a `HelloWritten` event, which the wired adapter turns into a
        // `playback_hello` JSONL line naming the pod.
        let client = TcpStream::connect(addr).await.unwrap();
        {
            let mut c = client;
            c.write_all(&framed(&hello(ChannelSource::AsrBeam)))
                .await
                .unwrap();
            wait_for_event(&jsonl_path, "playback_hello for pod-srv", |v| {
                v["event"] == "playback_hello" && v["pod"] == "pod-srv"
            })
            .await;

            stop.send(()).unwrap();
            run_join.await.unwrap();
            drop(c);
        }
        drop(handle);
        join.await.unwrap();

        let lines = read_lines(&jsonl_path);
        let hellos = events_named(&lines, "playback_hello");
        assert_eq!(
            hellos.len(),
            1,
            "exactly one playback_hello per writer spawn"
        );
        assert_eq!(hellos[0]["pod"], "pod-srv");
    }

    #[tokio::test]
    async fn playback_registry_register_and_guarded_deregister() {
        let reg: PlaybackRegistry = Arc::new(Mutex::new(HashMap::new()));

        // Each writer needs a live peer read half held for the duration, or the
        // task would hit a broken-pipe write and die — irrelevant to the map
        // mechanics under test, but keeping the peers alive avoids the noise.
        let mut peers = Vec::new();
        let mut spawn_handle = || {
            let (io, peer) = tokio::io::duplex(64 * 1024);
            peers.push(peer);
            PlaybackWriter::spawn(
                io,
                PodId("pod-a".to_string()),
                PacerConfig::default(),
                Arc::new(PlaybackStats::default()),
                Arc::new(|_event| {
                    Box::pin(std::future::ready(())) as futures::future::BoxFuture<'static, ()>
                }) as PlaybackEventFn,
                CancellationToken::new(),
            )
        };

        // A connection installs its handle under its pod id.
        playback_register(&reg, "pod-a".to_string(), 1, spawn_handle());
        assert!(reg.lock().unwrap().contains_key("pod-a"));

        // A superseding connection replaces the slot with its own handle+conn_seq.
        playback_register(&reg, "pod-a".to_string(), 2, spawn_handle());
        assert_eq!(
            reg.lock().unwrap().get("pod-a").map(|e| e.conn_seq),
            Some(2)
        );

        // The superseded connection's close must not evict the successor's entry.
        playback_deregister(&reg, "pod-a", 1);
        assert_eq!(
            reg.lock().unwrap().get("pod-a").map(|e| e.conn_seq),
            Some(2)
        );

        // The current connection's close removes its own entry.
        playback_deregister(&reg, "pod-a", 2);
        assert!(!reg.lock().unwrap().contains_key("pod-a"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn happy_path_records_and_emits_tracking() {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("store");
        let jsonl_path = dir.path().join("events.jsonl");
        let (handle, join) =
            jsonl::spawn_quiet(&crate::config::JsonlSink::File(jsonl_path.clone()))
                .await
                .unwrap();

        let (addr, stop, run_join) = spawn_server(config(&store, true), handle.clone()).await;

        let mut client = TcpStream::connect(addr).await.unwrap();
        for f in [
            hello(ChannelSource::AsrBeam),
            StreamFrame::SegmentStart(SegmentStart {
                segment_id: 5,
                base_sample_index: 0,
                base_device_ts_us: 1_000_000,
                preroll_samples: 160,
            }),
            audio(5, 0, 320),
            StreamFrame::Telemetry(Telemetry {
                device_ts_us: 1_020_000,
                kind: TelemetryKind::Azimuths {
                    values: [0.5, f32::NAN, 0.25, 0.75],
                },
            }),
            audio(5, 320, 320),
            StreamFrame::SegmentEnd(SegmentEnd {
                segment_id: 5,
                device_ts_us: 1_040_000,
                frames_sent: 2,
                samples_sent: 640,
                reason: EndReason::VadRelease,
            }),
        ] {
            client.write_all(&framed(&f)).await.unwrap();
        }
        client.shutdown().await.unwrap();
        drop(client);

        // Wait until the segment has flowed all the way through (its `tracking`
        // line), so shutdown cannot race the connection's acceptance/processing.
        wait_for_event(&jsonl_path, "tracking for segment 5", |v| {
            v["event"] == "tracking" && v["segment_id"] == 5
        })
        .await;
        // Stop the server and flush JSONL.
        stop.send(()).unwrap();
        run_join.await.unwrap();
        drop(handle);
        join.await.unwrap();

        let lines = read_lines(&jsonl_path);
        assert_eq!(events_named(&lines, "conn_hello").len(), 1);
        let opened = events_named(&lines, "segment_opened");
        assert_eq!(opened.len(), 1);
        assert_eq!(opened[0]["segment_id"], 5);
        assert_eq!(opened[0]["preroll"], 160);

        let closed = events_named(&lines, "segment_closed");
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0]["samples"], 640);
        assert_eq!(closed[0]["end_cause"], "vad_release");
        assert_eq!(closed[0]["cross_check"], "match");
        assert!(closed[0]["timings"]["first_frame_rx"].as_u64().is_some());
        assert!(closed[0]["rx_to_assembled_us"].as_u64().is_some());

        let tracking = events_named(&lines, "tracking");
        assert_eq!(tracking.len(), 1);
        // DoA propagated wire → Segment.telemetry → tracking event.
        assert_eq!(tracking[0]["doa"][0][0], 320);
        assert_eq!(tracking[0]["doa"][0][1][0], 0.5);
        assert!(tracking[0]["doa"][0][1][1].is_null());

        // Frame log + sidecar landed on disk under the pod-named stem.
        let framelogs: Vec<PathBuf> = std::fs::read_dir(&store)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|x| x == "framelog"))
            .collect();
        assert_eq!(framelogs.len(), 1);
        assert!(framelogs[0]
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("pod-srv_"));
        let sidecar = framelogs[0].with_extension("sidecar.json");
        assert!(sidecar.exists());
        let sc: Value = serde_json::from_slice(&std::fs::read(&sidecar).unwrap()).unwrap();
        assert_eq!(sc["pod_id"], "pod-srv");
        assert_eq!(sc["segments"][0]["segment_id"], 5);
    }

    // The brain-replies-with-clip-over-the-wire behavior this test drove in-process
    // through the retired batch path is now covered end-to-end through the real
    // streaming listener by `wav_brain_answers_wake_with_paced_clip_playback` in
    // `tests/playback_integration.rs`: an utterance can no longer be minted from a
    // synthetic segment (the listener is the only utterance source and needs a real
    // openWakeWord arm), so the coverage moved to the integration harness that loads
    // the committed models and drives the missed-onset fallback carve.

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_emits_final_stage_health_with_boundary_counters() {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("store");
        let jsonl_path = dir.path().join("events.jsonl");
        let (handle, join) =
            jsonl::spawn_quiet(&crate::config::JsonlSink::File(jsonl_path.clone()))
                .await
                .unwrap();
        let (addr, stop, run_join) = spawn_server(config(&store, true), handle.clone()).await;

        // One complete segment, so the queue's `pushed` counter is non-zero and
        // a telemetry-outside-segment frame (before any SegmentStart) is tallied.
        let mut client = TcpStream::connect(addr).await.unwrap();
        for f in [
            hello(ChannelSource::AsrBeam),
            // Telemetry with no segment open → discarded-but-counted by the FSM.
            StreamFrame::Telemetry(Telemetry {
                device_ts_us: 0,
                kind: TelemetryKind::SpEnergy {
                    values: [0.1, 0.2, 0.3, 0.4],
                },
            }),
            StreamFrame::SegmentStart(SegmentStart {
                segment_id: 5,
                base_sample_index: 0,
                base_device_ts_us: 0,
                preroll_samples: 0,
            }),
            audio(5, 0, 320),
            StreamFrame::SegmentEnd(SegmentEnd {
                segment_id: 5,
                device_ts_us: 0,
                frames_sent: 1,
                samples_sent: 320,
                reason: EndReason::VadRelease,
            }),
        ] {
            client.write_all(&framed(&f)).await.unwrap();
        }
        client.shutdown().await.unwrap();
        drop(client);

        wait_for_event(&jsonl_path, "tracking for segment 5", |v| {
            v["event"] == "tracking" && v["segment_id"] == 5
        })
        .await;
        stop.send(()).unwrap();
        run_join.await.unwrap();
        drop(handle);
        join.await.unwrap();

        let lines = read_lines(&jsonl_path);
        let health = events_named(&lines, "stage_health");
        // At least the final line fires (the periodic pass may add more).
        assert!(!health.is_empty(), "a stage_health line was emitted");
        let final_line = health
            .iter()
            .find(|v| v["at_shutdown"] == true)
            .expect("a final at_shutdown stage_health line");
        // The segment flowed through the queue and the pipeline drained it.
        assert!(final_line["segment_queue"]["pushed"].as_u64().unwrap() >= 1);
        assert_eq!(final_line["segment_queue"]["depth"], 0);
        // The consumer lives for the whole run, so no send hits a closed queue.
        assert_eq!(final_line["segment_queue"]["send_failures"], 0);
        // The pre-segment telemetry frame was discarded-but-counted.
        assert_eq!(final_line["telemetry_outside_segment"], 1);
        // All the named boundary fields are present.
        assert!(final_line["jsonl_dropped"].as_u64().is_some());
        assert!(final_line["ledger_evictions"].as_u64().is_some());
        assert!(final_line["clock_step_clamps"].as_u64().is_some());
        // No `[wake] oww` + `[endpointer]` in this config, so no continuous listener
        // is wired and its health block is omitted.
        assert!(final_line["listener"].is_null(), "no listener block wired");
        // The playback/brain/router snapshot blocks are present in every config;
        // this run has no `[brain]` and queues no playback job, so every counter
        // is deterministically zero (a writer spawns on the connection regardless
        // of brain, but nothing is ever sent to it).
        assert_eq!(final_line["playback"]["frames_written"], 0);
        assert_eq!(final_line["playback"]["write_timeouts"], 0);
        assert_eq!(final_line["playback"]["jobs_aborted"], 0);
        assert_eq!(final_line["brain"]["speak_send_failures"], 0);
        assert_eq!(final_line["router"]["delivered"], 0);
        assert_eq!(final_line["router"]["no_pod"], 0);
        assert_eq!(final_line["router"]["unsupported"], 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn between_segment_rolling_starts_a_fresh_standalone_log() {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("store");
        let jsonl_path = dir.path().join("events.jsonl");
        // A large first segment then a tiny second, with the roll threshold
        // between them: the log rolls once (after segment 5, over the threshold),
        // and the second log (tiny segment 6) stays under it — exactly one roll,
        // two logs.
        let text = format!(
            "listen_addr = \"127.0.0.1:0\"\n\
             [record]\nenabled = true\ndir = {:?}\nroll_max_bytes = 2000\n\
             [pods.pod-srv]\nroom = \"kitchen\"\n",
            store.to_str().unwrap()
        );
        let config = Arc::new(Config::parse(&text).unwrap());
        let (handle, join) =
            jsonl::spawn_quiet(&crate::config::JsonlSink::File(jsonl_path.clone()))
                .await
                .unwrap();
        let (addr, stop, run_join) = spawn_server(config, handle.clone()).await;

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&framed(&hello(ChannelSource::AsrBeam)))
            .await
            .unwrap();

        // Segment 5: five 400-sample audio frames (~4 KB payload), past the 2 KB
        // threshold, so its close triggers a roll.
        client
            .write_all(&framed(&StreamFrame::SegmentStart(SegmentStart {
                segment_id: 5,
                base_sample_index: 0,
                base_device_ts_us: 0,
                preroll_samples: 0,
            })))
            .await
            .unwrap();
        for k in 0..5u64 {
            client
                .write_all(&framed(&audio(5, k * 400, 400)))
                .await
                .unwrap();
        }
        client
            .write_all(&framed(&StreamFrame::SegmentEnd(SegmentEnd {
                segment_id: 5,
                device_ts_us: 0,
                frames_sent: 5,
                samples_sent: 2000,
                reason: EndReason::VadRelease,
            })))
            .await
            .unwrap();

        // Segment 6: one tiny audio frame, so the fresh log stays under threshold
        // and does not roll again.
        for f in [
            StreamFrame::SegmentStart(SegmentStart {
                segment_id: 6,
                base_sample_index: 0,
                base_device_ts_us: 0,
                preroll_samples: 0,
            }),
            audio(6, 0, 16),
            StreamFrame::SegmentEnd(SegmentEnd {
                segment_id: 6,
                device_ts_us: 0,
                frames_sent: 1,
                samples_sent: 16,
                reason: EndReason::VadRelease,
            }),
        ] {
            client.write_all(&framed(&f)).await.unwrap();
        }
        client.shutdown().await.unwrap();
        drop(client);

        // Wait for the connection to fully close before shutdown, so both
        // segments (and the roll between them) are processed deterministically.
        wait_for_event(&jsonl_path, "conn 1 closed", |v| {
            v["event"] == "conn_closed" && v["conn_seq"] == 1
        })
        .await;
        stop.send(()).unwrap();
        run_join.await.unwrap();
        drop(handle);
        join.await.unwrap();

        let lines = read_lines(&jsonl_path);
        let rolled = events_named(&lines, "record_rolled");
        assert_eq!(rolled.len(), 1);
        assert_eq!(rolled[0]["cause"], "bytes");

        // Two logs on disk; each replays standalone (first record is `Hello`),
        // and exactly one carries `rolled_from`.
        let framelogs: Vec<PathBuf> = std::fs::read_dir(&store)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|x| x == "framelog"))
            .collect();
        assert_eq!(framelogs.len(), 2);

        let mut with_rolled_from = 0;
        for path in &framelogs {
            let mut reader = pod_ingest::FrameLogReader::open(path).unwrap();
            let payload = match reader.next().unwrap().unwrap() {
                pod_ingest::LogItem::Record { payload, .. } => payload,
                pod_ingest::LogItem::TornTail => panic!("log is empty, no Hello"),
            };
            assert!(matches!(
                decode_frame(&payload).unwrap(),
                StreamFrame::Hello(_)
            ));
            if reader.meta().rolled_from.is_some() {
                with_rolled_from += 1;
            }
        }
        assert_eq!(
            with_rolled_from, 1,
            "exactly the rolled log carries rolled_from"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_finalizes_open_segment_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("store");
        let jsonl_path = dir.path().join("events.jsonl");
        let (handle, join) =
            jsonl::spawn_quiet(&crate::config::JsonlSink::File(jsonl_path.clone()))
                .await
                .unwrap();
        let (addr, stop, run_join) = spawn_server(config(&store, true), handle.clone()).await;

        // Open a segment and stream audio, but never send `SegmentEnd`: the
        // segment is still open on the server, and the client stays connected
        // (the server task is parked in its read) when shutdown arrives.
        let mut client = TcpStream::connect(addr).await.unwrap();
        for f in [
            hello(ChannelSource::AsrBeam),
            StreamFrame::SegmentStart(SegmentStart {
                segment_id: 9,
                base_sample_index: 0,
                base_device_ts_us: 0,
                preroll_samples: 0,
            }),
            audio(9, 0, 320),
        ] {
            client.write_all(&framed(&f)).await.unwrap();
        }
        wait_for_event(&jsonl_path, "segment 9 opened", |v| {
            v["event"] == "segment_opened" && v["segment_id"] == 9
        })
        .await;

        // Graceful shutdown while the segment is open.
        stop.send(()).unwrap();
        run_join.await.unwrap();
        drop(client);
        drop(handle);
        join.await.unwrap();

        let lines = read_lines(&jsonl_path);
        // The open segment finalized as truncated on shutdown.
        let closed = events_named(&lines, "segment_closed");
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0]["segment_id"], 9);
        assert_eq!(closed[0]["truncated"], true);
        // It still reached the pipeline before the drain — proof the connection
        // was awaited before the queue was closed.
        let tracking = events_named(&lines, "tracking");
        assert_eq!(tracking.len(), 1);
        assert_eq!(tracking[0]["segment_id"], 9);
        // The connection closed with the shutdown cause, not a clean `eof`.
        let conn_closed = events_named(&lines, "conn_closed");
        assert_eq!(conn_closed.len(), 1);
        assert_eq!(conn_closed[0]["cause"], "shutdown");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stereo_hello_is_fatal_protocol_error() {
        let dir = tempfile::tempdir().unwrap();
        let jsonl_path = dir.path().join("events.jsonl");
        let (handle, join) =
            jsonl::spawn_quiet(&crate::config::JsonlSink::File(jsonl_path.clone()))
                .await
                .unwrap();
        let (addr, stop, run_join) =
            spawn_server(config(&dir.path().join("store"), false), handle.clone()).await;

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&framed(&hello(ChannelSource::Stereo)))
            .await
            .unwrap();
        // Server drops the connection; the read returns 0.
        let mut buf = [0u8; 1];
        let _ = client.read(&mut buf).await;
        drop(client);

        stop.send(()).unwrap();
        run_join.await.unwrap();
        drop(handle);
        join.await.unwrap();

        let lines = read_lines(&jsonl_path);
        let errs = events_named(&lines, "protocol_error");
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0]["fatal"], true);
        assert_eq!(errs[0]["kind"], "format_mismatch");
        // The connection closes with a distinct fatal-protocol cause, not a
        // clean-disconnect `eof`.
        let closed = events_named(&lines, "conn_closed");
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0]["cause"], "fatal_protocol");
        // No segment was assembled.
        assert!(events_named(&lines, "segment_closed").is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recorder_failure_degrades_but_pipeline_continues() {
        let dir = tempfile::tempdir().unwrap();
        let jsonl_path = dir.path().join("events.jsonl");
        // Point the record dir at a path shadowed by a regular file, so
        // create_dir_all fails and recording latches off.
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, b"not a dir").unwrap();
        let store = blocker.join("store");

        let (handle, join) =
            jsonl::spawn_quiet(&crate::config::JsonlSink::File(jsonl_path.clone()))
                .await
                .unwrap();
        let (addr, stop, run_join) = spawn_server(config(&store, true), handle.clone()).await;

        let mut client = TcpStream::connect(addr).await.unwrap();
        for f in [
            hello(ChannelSource::AsrBeam),
            StreamFrame::SegmentStart(SegmentStart {
                segment_id: 1,
                base_sample_index: 0,
                base_device_ts_us: 0,
                preroll_samples: 0,
            }),
            audio(1, 0, 160),
            StreamFrame::SegmentEnd(SegmentEnd {
                segment_id: 1,
                device_ts_us: 0,
                frames_sent: 1,
                samples_sent: 160,
                reason: EndReason::VadRelease,
            }),
        ] {
            client.write_all(&framed(&f)).await.unwrap();
        }
        client.shutdown().await.unwrap();
        drop(client);

        // Wait for the connection to fully close before shutdown, so the segment
        // is processed and cannot race the server stop.
        wait_for_event(&jsonl_path, "conn 1 closed", |v| {
            v["event"] == "conn_closed" && v["conn_seq"] == 1
        })
        .await;
        stop.send(()).unwrap();
        run_join.await.unwrap();
        drop(handle);
        join.await.unwrap();

        let lines = read_lines(&jsonl_path);
        // Exactly one latch event: the failure is announced once, not per write.
        assert_eq!(events_named(&lines, "record_error").len(), 1);
        // The pipeline still assembled and tracked the segment.
        assert_eq!(events_named(&lines, "segment_closed").len(), 1);
        assert_eq!(events_named(&lines, "tracking").len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connection_cap_rejects_beyond_limit() {
        let dir = tempfile::tempdir().unwrap();
        let jsonl_path = dir.path().join("events.jsonl");
        let text = format!(
            "listen_addr = \"127.0.0.1:0\"\nmax_connections = 1\n\
             [record]\nenabled = false\ndir = {:?}\n",
            dir.path().join("store").to_str().unwrap()
        );
        let config = Arc::new(Config::parse(&text).unwrap());
        let (handle, join) =
            jsonl::spawn_quiet(&crate::config::JsonlSink::File(jsonl_path.clone()))
                .await
                .unwrap();
        let (addr, stop, run_join) = spawn_server(config, handle.clone()).await;

        // First connection holds its permit (opened, kept idle).
        let hold = TcpStream::connect(addr).await.unwrap();
        hold.set_nodelay(true).unwrap();
        // Give the accept loop time to take the only permit.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Second connection is accepted then rejected for lack of a permit.
        let rejected = TcpStream::connect(addr).await.unwrap();
        let mut rejected = rejected;
        let mut buf = [0u8; 1];
        let _ = rejected.read(&mut buf).await; // server closes it
        drop(rejected);
        drop(hold);

        stop.send(()).unwrap();
        run_join.await.unwrap();
        drop(handle);
        join.await.unwrap();

        let lines = read_lines(&jsonl_path);
        // Exactly one rejection: two connections against a cap of one.
        assert_eq!(events_named(&lines, "conn_rejected").len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn second_connection_supersedes_first() {
        let dir = tempfile::tempdir().unwrap();
        let jsonl_path = dir.path().join("events.jsonl");
        let (handle, join) =
            jsonl::spawn_quiet(&crate::config::JsonlSink::File(jsonl_path.clone()))
                .await
                .unwrap();
        // Inject a registry the test also holds, so it can observe the playback
        // map through this real connect/supersede/close flow.
        let reg: PlaybackRegistry = Arc::new(Mutex::new(HashMap::new()));
        let (addr, stop, run_join) = spawn_server_with_playback_registry(
            config(&dir.path().join("store"), false),
            handle.clone(),
            reg.clone(),
        )
        .await;

        // First connection says hello and stays parked (socket held open).
        let mut first = TcpStream::connect(addr).await.unwrap();
        first
            .write_all(&framed(&hello(ChannelSource::AsrBeam)))
            .await
            .unwrap();
        // Wait for the first connection to register (its `conn_hello`) before the
        // second arrives, so the supersede ordering is deterministic, not timed.
        wait_for_event(&jsonl_path, "conn_hello for conn 1", |v| {
            v["event"] == "conn_hello" && v["conn_seq"] == 1
        })
        .await;

        // Registry read 1: conn 1's register landed, keyed to conn 1. The
        // `conn_hello` fence precedes `playback_register` in program order, so the
        // poll waits for the insert. Bounding this before conn 2 connects makes a
        // false pass from conn 2's later entry impossible.
        wait_for_registry(&reg, "registry keyed to conn 1", |m| {
            m.len() == 1 && m.get("pod-srv") == Some(&1)
        })
        .await;

        // Second connection, same pod id, supersedes the first.
        let mut second = TcpStream::connect(addr).await.unwrap();
        second
            .write_all(&framed(&hello(ChannelSource::AsrBeam)))
            .await
            .unwrap();
        // Barrier on conn 1's own `conn_closed` before tearing down. The cancel
        // drives conn 1 to close on its own (no client-side drop needed), and
        // waiting for that close — not merely the `conn_superseded` emit — orders
        // its `CloseCause::Superseded` before the assertion reads it. Waiting on
        // the cancel alone would not: conn 1 might not yet have polled its select
        // and latched the cause, and the following `drop(first)` makes its read
        // branch ready too, which a non-`biased` select could then take
        // (`read_error`). A bare read on `first` is no barrier either — its eager
        // playback `Hello` returns a byte immediately rather than blocking.
        wait_for_event(&jsonl_path, "conn_closed for conn 1", |v| {
            v["event"] == "conn_closed" && v["conn_seq"] == 1
        })
        .await;

        // Registry read 2: the supersede moved the slot to conn 2. Conn 2's
        // `playback_register` runs only after conn 1's `finished` fires, which
        // conn 1 cancels after its own `playback_deregister` — so the slot is
        // genuinely removed and then reinserted, and the map transits {1} -> {} ->
        // {2}. The poll absorbs the transient empty state. Note this interleaving
        // does not exercise `playback_deregister`'s conn_seq guard: conn 1 removes
        // its own entry here, so an unconditional remove would look identical.
        // That guard is covered by `playback_registry_register_and_guarded_deregister`.
        // Read before conn 2 closes.
        wait_for_registry(&reg, "registry moved to conn 2", |m| {
            m.len() == 1 && m.get("pod-srv") == Some(&2)
        })
        .await;

        drop(first);
        second.shutdown().await.unwrap();
        drop(second);

        stop.send(()).unwrap();
        run_join.await.unwrap();
        drop(handle);
        join.await.unwrap();

        // Registry read 3: both connections closed and the run task finished, so
        // conn 2's final `playback_deregister` removed the slot and the map is
        // empty.
        wait_for_registry(&reg, "registry emptied after both close", |m| m.is_empty()).await;

        let lines = read_lines(&jsonl_path);
        let superseded = events_named(&lines, "conn_superseded");
        assert_eq!(superseded.len(), 1);
        assert_eq!(superseded[0]["old_conn_seq"], 1);
        assert_eq!(superseded[0]["new_conn_seq"], 2);
        // The first connection closes as Superseded.
        let closed = events_named(&lines, "conn_closed");
        let first_close = closed
            .iter()
            .find(|v| v["conn_seq"] == 1)
            .expect("first conn_closed");
        assert_eq!(first_close["cause"], "superseded");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn supersede_resumes_open_segment_deterministically() {
        let dir = tempfile::tempdir().unwrap();
        let jsonl_path = dir.path().join("events.jsonl");
        let (handle, join) =
            jsonl::spawn_quiet(&crate::config::JsonlSink::File(jsonl_path.clone()))
                .await
                .unwrap();
        let (addr, stop, run_join) =
            spawn_server(config(&dir.path().join("store"), false), handle.clone()).await;

        // First connection opens segment 7 and leaves it open (no SegmentEnd).
        let mut first = TcpStream::connect(addr).await.unwrap();
        for f in [
            hello(ChannelSource::AsrBeam),
            StreamFrame::SegmentStart(SegmentStart {
                segment_id: 7,
                base_sample_index: 0,
                base_device_ts_us: 0,
                preroll_samples: 0,
            }),
            audio(7, 0, 320),
        ] {
            first.write_all(&framed(&f)).await.unwrap();
        }
        // Wait until segment 7 is open on the first connection (only one exists),
        // so the second connection's supersede truncates a genuinely-open segment
        // into the ledger — deterministic, not timing-dependent.
        wait_for_event(&jsonl_path, "segment 7 open", |v| {
            v["event"] == "segment_opened" && v["segment_id"] == 7
        })
        .await;

        // Second connection, same pod, resumes segment 7. The server awaits the
        // first task's truncating close (ledger note) before reading this
        // SegmentStart, so the resume is recognized deterministically.
        let mut second = TcpStream::connect(addr).await.unwrap();
        for f in [
            hello(ChannelSource::AsrBeam),
            StreamFrame::SegmentStart(SegmentStart {
                segment_id: 7,
                base_sample_index: 320,
                base_device_ts_us: 0,
                preroll_samples: 0,
            }),
            audio(7, 320, 320),
            StreamFrame::SegmentEnd(SegmentEnd {
                segment_id: 7,
                device_ts_us: 0,
                frames_sent: 1,
                samples_sent: 320,
                reason: EndReason::VadRelease,
            }),
        ] {
            second.write_all(&framed(&f)).await.unwrap();
        }
        let mut buf = [0u8; 1];
        let _ = first.read(&mut buf).await;
        drop(first);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        second.shutdown().await.unwrap();
        drop(second);

        stop.send(()).unwrap();
        run_join.await.unwrap();
        drop(handle);
        join.await.unwrap();

        let lines = read_lines(&jsonl_path);
        let opened_seg7: Vec<&Value> = events_named(&lines, "segment_opened")
            .into_iter()
            .filter(|v| v["segment_id"] == 7)
            .collect();
        assert_eq!(opened_seg7.len(), 2);
        let resumes = opened_seg7
            .iter()
            .filter(|v| v["is_resume"] == true)
            .count();
        assert_eq!(resumes, 1, "exactly one open recognized as a resume");
        // The resumed segment's completed close skips the cross-check.
        let resumed_close = events_named(&lines, "segment_closed")
            .into_iter()
            .find(|v| v["resumed"] == true)
            .expect("a resumed segment_closed");
        assert_eq!(resumed_close["cross_check"], "skipped_resume");
    }

    /// Seed a store with a framelog of `size` bytes plus an `ungated` sidecar
    /// labelling one segment starting at `start_epoch_us`, optionally pinned.
    fn seed_log(dir: &Path, name: &str, size: usize, start_epoch_us: u64, pinned: bool) -> PathBuf {
        std::fs::create_dir_all(dir).unwrap();
        let framelog = dir.join(format!("{name}.framelog"));
        std::fs::write(&framelog, vec![0u8; size]).unwrap();
        let mut sc = Sidecar::new("pod-seed");
        sc.pinned = pinned;
        sc.push(SidecarSegment {
            segment_id: 0,
            part: 0,
            wake: crate::recorder::WakeClass::Ungated,
            start_epoch_us,
            end_epoch_us: start_epoch_us + 1,
            end_cause: SegmentEndCause::VadRelease,
            truncated: false,
            resumed: false,
            gap_count: 0,
            samples: 16_000,
        });
        sc.write_atomic(&sidecar_path(&framelog)).unwrap();
        framelog
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn startup_prune_evicts_over_cap_and_keeps_pins() {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("store");
        let jsonl_path = dir.path().join("events.jsonl");

        // A store already over cap: one pinned old log and one loose old log.
        let pinned = seed_log(&store, "pinned", 2000, 50, true);
        let loose = seed_log(&store, "loose", 2000, 100, false);

        let text = format!(
            "listen_addr = \"127.0.0.1:0\"\n\
             [record]\nenabled = true\ndir = {:?}\ncap_bytes = 2500\n\
             [pods.pod-srv]\nroom = \"kitchen\"\n",
            store.to_str().unwrap()
        );
        let config = Arc::new(Config::parse(&text).unwrap());
        let (handle, join) =
            jsonl::spawn_quiet(&crate::config::JsonlSink::File(jsonl_path.clone()))
                .await
                .unwrap();
        // The startup prune pass runs before the accept loop; the immediate
        // shutdown only stops accepting afterward.
        let (_addr, stop, run_join) = spawn_server(config, handle.clone()).await;
        stop.send(()).unwrap();
        run_join.await.unwrap();
        drop(handle);
        join.await.unwrap();

        let lines = read_lines(&jsonl_path);
        let pruned = events_named(&lines, "record_pruned");
        assert_eq!(pruned.len(), 1);
        assert!(pruned[0]["framelog"].as_str().unwrap().contains("loose"));
        // The rendered line carries pod attribution and the deletion reason. Both
        // logs share pod `pod-seed`; the resolved quota (cap_bytes / 2 = 1250) is
        // well under the bucket total, so phase 1 drains the loose log.
        assert_eq!(pruned[0]["pod_id"].as_str(), Some("pod-seed"));
        assert_eq!(pruned[0]["reason"].as_str(), Some("pod_quota"));
        // The pinned residue keeps the bucket over quota, reported (not halted)
        // via a per-pod line naming the same pod.
        let over = events_named(&lines, "prune_pod_over_quota");
        assert_eq!(over.len(), 1);
        assert_eq!(over[0]["pod_id"].as_str(), Some("pod-seed"));
        assert_eq!(over[0]["pod_cap_bytes"].as_u64(), Some(1250));
        // The pinned log (and its sidecar) survives; the loose one is gone.
        assert!(pinned.exists());
        assert!(sidecar_path(&pinned).exists());
        assert!(!loose.exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn roll_fires_background_prune_that_evicts_old_logs() {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("store");
        let jsonl_path = dir.path().join("events.jsonl");

        // An old crash-leftover log (no sidecar → ungated, age 0) fills much of
        // the store. Cap is set so startup keeps it (under cap alone) but a roll
        // pushes the store over cap; the roll-triggered prune then evicts it.
        std::fs::create_dir_all(&store).unwrap();
        let crash = store.join("old_crash.framelog");
        std::fs::write(&crash, vec![0u8; 5000]).unwrap();

        let text = format!(
            "listen_addr = \"127.0.0.1:0\"\n\
             [record]\nenabled = true\ndir = {:?}\nroll_max_bytes = 2000\ncap_bytes = 5500\n\
             [pods.pod-srv]\nroom = \"kitchen\"\n",
            store.to_str().unwrap()
        );
        let config = Arc::new(Config::parse(&text).unwrap());
        let (handle, join) =
            jsonl::spawn_quiet(&crate::config::JsonlSink::File(jsonl_path.clone()))
                .await
                .unwrap();
        let (addr, stop, run_join) = spawn_server(config, handle.clone()).await;

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&framed(&hello(ChannelSource::AsrBeam)))
            .await
            .unwrap();
        // A large segment (~4 KB) past the 2 KB roll threshold: its close rolls
        // the log and fires the background prune.
        client
            .write_all(&framed(&StreamFrame::SegmentStart(SegmentStart {
                segment_id: 5,
                base_sample_index: 0,
                base_device_ts_us: 0,
                preroll_samples: 0,
            })))
            .await
            .unwrap();
        for k in 0..5u64 {
            client
                .write_all(&framed(&audio(5, k * 400, 400)))
                .await
                .unwrap();
        }
        client
            .write_all(&framed(&StreamFrame::SegmentEnd(SegmentEnd {
                segment_id: 5,
                device_ts_us: 0,
                frames_sent: 5,
                samples_sent: 2000,
                reason: EndReason::VadRelease,
            })))
            .await
            .unwrap();

        // Poll for the fire-and-forget prune to evict the crash leftover. Read
        // leniently — the writer flushes concurrently, so a torn tail line is
        // possible and simply skipped.
        let mut pruned_crash = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let text = std::fs::read_to_string(&jsonl_path).unwrap_or_default();
            if text
                .lines()
                .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                .any(|v| {
                    v["event"] == "record_pruned"
                        && v["framelog"]
                            .as_str()
                            .is_some_and(|s| s.contains("old_crash"))
                })
            {
                pruned_crash = true;
                break;
            }
        }
        assert!(
            pruned_crash,
            "a roll should fire a prune that evicts the old crash log"
        );

        client.shutdown().await.unwrap();
        drop(client);
        stop.send(()).unwrap();
        run_join.await.unwrap();
        drop(handle);
        join.await.unwrap();

        assert!(!crash.exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mid_frame_eof_truncates_then_sequential_reconnect_resumes() {
        let dir = tempfile::tempdir().unwrap();
        let jsonl_path = dir.path().join("events.jsonl");
        let (handle, join) =
            jsonl::spawn_quiet(&crate::config::JsonlSink::File(jsonl_path.clone()))
                .await
                .unwrap();
        let (addr, stop, run_join) =
            spawn_server(config(&dir.path().join("store"), false), handle.clone()).await;

        // First connection: open segment 5 with one full audio frame, then send
        // only the 2-byte length prefix of a second frame and close the socket —
        // a mid-frame tear. The payload read hits EOF → `ReadError`, and the open
        // segment finalizes truncated (a ledger note for the resume).
        let mut first = TcpStream::connect(addr).await.unwrap();
        for f in [
            hello(ChannelSource::AsrBeam),
            StreamFrame::SegmentStart(SegmentStart {
                segment_id: 5,
                base_sample_index: 0,
                base_device_ts_us: 0,
                preroll_samples: 0,
            }),
            audio(5, 0, 320),
        ] {
            first.write_all(&framed(&f)).await.unwrap();
        }
        let partial = framed(&audio(5, 320, 320));
        first.write_all(&partial[..2]).await.unwrap();
        first.shutdown().await.unwrap();
        drop(first);

        // Wait for the first connection's close (ledger note has landed) before
        // the sequential reconnect — a plain reconnect, not a superseding one.
        wait_for_event(&jsonl_path, "conn 1 closed", |v| {
            v["event"] == "conn_closed" && v["conn_seq"] == 1
        })
        .await;

        // Second, independent connection resumes segment 5.
        let mut second = TcpStream::connect(addr).await.unwrap();
        for f in [
            hello(ChannelSource::AsrBeam),
            StreamFrame::SegmentStart(SegmentStart {
                segment_id: 5,
                base_sample_index: 320,
                base_device_ts_us: 0,
                preroll_samples: 0,
            }),
            audio(5, 320, 320),
            StreamFrame::SegmentEnd(SegmentEnd {
                segment_id: 5,
                device_ts_us: 0,
                frames_sent: 1,
                samples_sent: 320,
                reason: EndReason::VadRelease,
            }),
        ] {
            second.write_all(&framed(&f)).await.unwrap();
        }
        second.shutdown().await.unwrap();
        drop(second);

        // Wait for the resuming connection to finish before shutdown.
        wait_for_event(&jsonl_path, "conn 2 closed", |v| {
            v["event"] == "conn_closed" && v["conn_seq"] == 2
        })
        .await;
        stop.send(()).unwrap();
        run_join.await.unwrap();
        drop(handle);
        join.await.unwrap();

        let lines = read_lines(&jsonl_path);
        // The torn first connection closed as a read error.
        let first_close = events_named(&lines, "conn_closed")
            .into_iter()
            .find(|v| v["conn_seq"] == 1)
            .expect("first conn_closed");
        assert_eq!(first_close["cause"], "read_error");
        // Its open segment finalized truncated.
        let truncated = events_named(&lines, "segment_closed")
            .into_iter()
            .find(|v| v["truncated"] == true)
            .expect("a truncated segment_closed");
        assert_eq!(truncated["segment_id"], 5);
        // The sequential reconnect is recognized as a resume, cross-check skipped.
        let resumed_open = events_named(&lines, "segment_opened")
            .into_iter()
            .find(|v| v["segment_id"] == 5 && v["is_resume"] == true)
            .expect("a resumed segment_opened");
        assert_eq!(resumed_open["is_resume"], true);
        let resumed_close = events_named(&lines, "segment_closed")
            .into_iter()
            .find(|v| v["resumed"] == true)
            .expect("a resumed segment_closed");
        assert_eq!(resumed_close["cross_check"], "skipped_resume");
    }

    #[tokio::test]
    async fn finalize_segment_overflow_emits_dropped_event() {
        use speech_pipeline::SegmentEndInfo;

        let dir = tempfile::tempdir().unwrap();
        let jsonl_path = dir.path().join("events.jsonl");
        let (jsonl, writer_join) =
            jsonl::spawn_quiet(&crate::config::JsonlSink::File(jsonl_path.clone()))
                .await
                .unwrap();

        // A depth-1 queue, pre-filled so the next enqueue must displace it.
        let (seg_tx, _seg_rx) = DropOldestQueue::<crate::pipeline::PipelineItem>::new(1);
        let end = SegmentEndInfo::new(SegmentEndCause::VadRelease, false, 0, None);
        let displaced_id = 1;
        seg_tx.send(crate::pipeline::PipelineItem::Segment {
            seg: crate::test_support::segment(displaced_id, 16, vec![], end.clone()),
            epoch: 1,
        });

        // Finalize a second segment. `recording_on: false` makes `start` a no-op
        // (no filesystem side effects, `writer: None`), so `record_segment`
        // writes nothing. The enqueue displaces the oldest, and the call renders
        // the displaced identity into `segment_dropped_overflow`.
        let mut recorder = Recorder::start(
            dir.path().to_path_buf(),
            1,
            "unused",
            false,
            RecorderShared {
                open_logs: OpenLogs::default(),
                recording_failed: Arc::new(AtomicBool::new(false)),
                jsonl: jsonl.clone(),
            },
        );
        finalize_segment(
            crate::test_support::segment(2, 16, vec![], end),
            1,
            &mut recorder,
            &jsonl,
            &seg_tx,
            &AtomicU64::new(0),
        );

        // Drop the recorder's `jsonl` clone too, or the sink channel never closes.
        drop(recorder);
        drop(jsonl);
        writer_join.await.unwrap();

        let lines = read_lines(&jsonl_path);
        let dropped = events_named(&lines, "segment_dropped_overflow");
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0]["segment_id"], displaced_id);
        assert_eq!(dropped[0]["depth"], 1);
    }

    /// Build a `HealthSources` with default zero counters. `stats` and `jsonl` are
    /// the two handles a test must supply — they bind to the live segment queue and
    /// the writer under assertion; every other field defaults. Override only the
    /// fields under test via struct-update:
    /// `HealthSources { stt_stats: Some(..), ..health_sources(stats, jsonl) }`.
    fn health_sources(
        stats: StatsHandle<crate::pipeline::PipelineItem>,
        jsonl: JsonlHandle,
    ) -> HealthSources {
        HealthSources {
            stats,
            listener_stats: None,
            playback_stats: Arc::new(PlaybackStats::default()),
            brain_stats: Arc::new(BrainStats::default()),
            router_stats: Arc::new(RouterStats::default()),
            stt_stats: None,
            tts_stats: None,
            jsonl,
            ledger: ResumeLedger::shared(),
            telemetry_outside_segment: Arc::new(AtomicU64::new(0)),
            clock_step_clamps: Arc::new(AtomicU64::new(0)),
        }
    }

    #[test]
    fn build_listener_requires_oww_and_endpointer() {
        // No `[wake]` table: no streaming wake, so no listener (models never load).
        let c = Config::parse("listen_addr = \"10.0.0.5:7380\"").unwrap();
        assert!(
            build_listener(&c).unwrap().is_none(),
            "no wake ⇒ no listener"
        );

        // Bypass wake + an endpointer: the batch bypass path has no OWW to stream,
        // so the listener stays unwired even with Silero configured.
        let c = Config::parse(
            "listen_addr = \"10.0.0.5:7380\"\n[wake]\nmode = \"bypass\"\n[endpointer]\nmodel = \"/m/s.onnx\"",
        )
        .unwrap();
        assert!(
            build_listener(&c).unwrap().is_none(),
            "bypass wake ⇒ no listener"
        );

        // OWW wake but no `[endpointer]`: no Silero endpointer to carve utterances,
        // so no listener — and it returns before attempting any model load.
        let c = Config::parse(
            "listen_addr = \"10.0.0.5:7380\"\n[wake]\nmode = \"oww\"\nmelspectrogram = \"/m/mel.onnx\"\nembedding = \"/m/emb.onnx\"\nmodel = \"/m/w.onnx\"",
        )
        .unwrap();
        assert!(
            build_listener(&c).unwrap().is_none(),
            "oww without endpointer ⇒ no listener"
        );
    }

    #[test]
    fn feed_end_cause_maps_close_kinds() {
        assert_eq!(
            crate::replay::feed_end_cause(&pod_ingest::SegmentClose::Truncated {
                cause: CloseCause::Eof
            }),
            SegmentEndCause::Truncated
        );
    }

    /// Drive the periodic half of `stage_health_emitter` directly (the full
    /// server tests only ever see the separate at-shutdown line): a short period
    /// must produce repeated `at_shutdown: false` lines until the token cancels.
    #[tokio::test(flavor = "multi_thread")]
    async fn stage_health_emitter_emits_periodic_lines_until_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("health.jsonl");
        let (handle, writer_join) =
            jsonl::spawn_quiet(&crate::config::JsonlSink::File(path.clone()))
                .await
                .unwrap();

        let (seg_tx, _seg_rx) = DropOldestQueue::<crate::pipeline::PipelineItem>::new(2);
        let token = CancellationToken::new();
        let emitter = tokio::spawn(stage_health_emitter(
            Duration::from_millis(20),
            health_sources(seg_tx.stats_handle(), handle.clone()),
            token.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(75)).await;
        token.cancel();
        emitter.await.unwrap();
        drop(handle);
        writer_join.await.unwrap();

        let lines = read_lines(&path);
        let periodic = lines
            .iter()
            .filter(|v| v["event"] == "stage_health" && v["at_shutdown"] == false)
            .count();
        assert!(
            periodic >= 2,
            "expected >= 2 periodic stage_health lines, got {periodic}"
        );
    }

    /// A zero period disables the periodic loop entirely: the emitter parks on
    /// the shutdown token (does not return early, does not busy-loop) and emits
    /// no `stage_health` line of its own.
    #[tokio::test(flavor = "multi_thread")]
    async fn stage_health_emitter_zero_period_is_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("health.jsonl");
        let (handle, writer_join) =
            jsonl::spawn_quiet(&crate::config::JsonlSink::File(path.clone()))
                .await
                .unwrap();

        let (seg_tx, _seg_rx) = DropOldestQueue::<crate::pipeline::PipelineItem>::new(2);
        let token = CancellationToken::new();
        let emitter = tokio::spawn(stage_health_emitter(
            Duration::ZERO,
            health_sources(seg_tx.stats_handle(), handle.clone()),
            token.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(
            !emitter.is_finished(),
            "zero-period emitter must park until shutdown, not return early"
        );
        token.cancel();
        emitter.await.unwrap();
        drop(handle);
        writer_join.await.unwrap();

        let lines = read_lines(&path);
        let periodic = lines
            .iter()
            .filter(|v| v["event"] == "stage_health")
            .count();
        assert_eq!(periodic, 0, "zero period emits no periodic lines");
    }

    /// `stage_health` carries an `stt`/`tts` block when those stages are wired
    /// (reporting their request counters) and omits both when they are absent —
    /// a wired-but-idle stage is distinct from no stage at all.
    #[tokio::test]
    async fn stage_health_reports_stt_tts_blocks_only_when_wired() {
        let dir = tempfile::tempdir().unwrap();

        // Wired: both stats handles present. The block appears with its counters.
        let path_wired = dir.path().join("wired.jsonl");
        let (handle, writer_join) =
            jsonl::spawn_quiet(&crate::config::JsonlSink::File(path_wired.clone()))
                .await
                .unwrap();
        let (seg_tx, _seg_rx) = DropOldestQueue::<crate::pipeline::PipelineItem>::new(2);
        let sources = HealthSources {
            stt_stats: Some(Arc::new(SttStats::default())),
            tts_stats: Some(Arc::new(TtsStats::default())),
            ..health_sources(seg_tx.stats_handle(), handle.clone())
        };
        emit_stage_health(&sources, true);
        drop(sources);
        drop(handle);
        writer_join.await.unwrap();
        let wired = read_lines(&path_wired);
        let line = &events_named(&wired, "stage_health")[0];
        assert!(line["stt"].is_object(), "wired stt block present");
        assert_eq!(line["stt"]["requests"], 0, "idle stt has zero requests");
        assert!(line["tts"].is_object(), "wired tts block present");
        assert_eq!(line["tts"]["requests"], 0, "idle tts has zero requests");

        // Absent: no stats handles. The block is omitted entirely.
        let path_absent = dir.path().join("absent.jsonl");
        let (handle, writer_join) =
            jsonl::spawn_quiet(&crate::config::JsonlSink::File(path_absent.clone()))
                .await
                .unwrap();
        let (seg_tx, _seg_rx) = DropOldestQueue::<crate::pipeline::PipelineItem>::new(2);
        let sources = health_sources(seg_tx.stats_handle(), handle.clone());
        emit_stage_health(&sources, true);
        drop(sources);
        drop(handle);
        writer_join.await.unwrap();
        let absent = read_lines(&path_absent);
        let line = &events_named(&absent, "stage_health")[0];
        assert!(line["stt"].is_null(), "absent stt block omitted");
        assert!(line["tts"].is_null(), "absent tts block omitted");
    }
}
