//! Offline listener replay: drive a captured frame log through the streaming
//! listener (openWakeWord + Silero endpointer) with no device and no daemon,
//! surfacing the [`ListenerEvent`]s it produces.
//!
//! The live path (`server::connection` → [`SessionFsm`] → `tap_listener` →
//! listener thread) is reproduced in-process: a frame log's records decode into
//! [`SessionEvent`]s exactly as they did on the wire, each maps to a listener
//! [`Feed`] through [`session_event_to_feed`] (the same mapping `tap_listener`
//! uses live), and the feed drives a [`ListenerState`] directly — no thread, no
//! bounded channel, no drops. That makes a replay deterministic and independent
//! of a running server, so it doubles as the endpointer/OWW threshold-tuning
//! rig: point it at captured frame logs, read the wake scores and endpoint
//! decisions, adjust the config, re-run.

use std::path::Path;
use std::sync::Arc;

use audio_pipeline::wire::decode_frame;
use pod_ingest::{
    CloseCause, FrameLogError, FrameLogReader, HostMicros, LogItem, ResumeLedger, SegmentClose,
    SessionEvent, SessionFsm,
};
use speech_pipeline::{
    Feed, ListenerConfig, ListenerEvent, ListenerState, OwwConfig, OwwModels, PodId, SPINE_FORMAT,
    SegmentEndCause, SileroConfig, SileroModel, WakeError,
};

use crate::config::{Config, EndpointerConfig, WakeConfig, WakeMode};

/// The listener's outer-boundary cause for a session close. The listener treats
/// every device close as the same authoritative outer boundary (it does not
/// branch on the cause today), so this carries the cause for forward
/// compatibility rather than driving behavior.
pub(crate) fn feed_end_cause(close: &SegmentClose) -> SegmentEndCause {
    match close {
        SegmentClose::Completed { .. } => SegmentEndCause::VadRelease,
        SegmentClose::Truncated { .. } => SegmentEndCause::Truncated,
    }
}

/// Map one [`SessionEvent`] to the listener [`Feed`] it feeds, tracking the pod
/// identity across the connection. `HelloAccepted` establishes `pod` and opens a
/// fresh epoch; audio and segment boundaries stream through; other events (
/// telemetry, protocol errors) produce no feed. The single mapping shared by the
/// live `tap_listener` and the offline replay engine, so the two can never drift.
pub(crate) fn session_event_to_feed(
    ev: &SessionEvent,
    pod: &mut Option<PodId>,
    epoch: u64,
) -> Option<Feed> {
    match ev {
        SessionEvent::HelloAccepted { pod_id, .. } => {
            *pod = Some(PodId(pod_id.clone()));
            Some(Feed::Connected { epoch })
        }
        SessionEvent::SegmentOpened {
            base_sample_index,
            preroll_samples,
            base_device_ts,
            ..
        } => Some(Feed::SegmentOpened {
            base_sample_index: *base_sample_index,
            preroll_samples: *preroll_samples,
            base_device_ts: *base_device_ts,
        }),
        SessionEvent::Audio {
            first_sample_index,
            pcm,
            gap,
            device_ts,
            host_rx,
            ..
        } => Some(Feed::Audio {
            first_sample_index: *first_sample_index,
            gap: *gap,
            pcm: Arc::from(pcm.as_slice()),
            device_ts: *device_ts,
            host_rx: *host_rx,
        }),
        SessionEvent::SegmentClosed { close, host_rx, .. } => Some(Feed::SegmentClosed {
            end: feed_end_cause(close),
            host_rx: *host_rx,
        }),
        _ => None,
    }
}

/// Why a replay stopped reading records. Every path finalizes the session (an
/// open segment truncates), so an in-progress utterance still gets its
/// device-release fallback carve regardless of how the log ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// The reader ran out of complete records (clean end of log).
    Eof,
    /// A trailing partial record — a normal capture tail, not a failure.
    TornTail,
    /// A captured-but-undecodable frame; the live path drops the connection on a
    /// decode error, so the replay stops here too.
    DecodeError,
    /// A corrupt record header mid-log (bad length/short read).
    CorruptRecord,
    /// A fatal protocol violation parked the FSM.
    ProtocolError,
}

/// The result of replaying one frame log through the listener.
#[derive(Debug)]
pub struct ReplaySummary {
    /// Records read before stopping (excludes the torn/corrupt terminal item).
    pub records: u64,
    /// Why reading stopped.
    pub stop: StopReason,
    /// Every listener event, in order.
    pub events: Vec<ListenerEvent>,
    /// Duplicate samples the PCM ring trimmed from overlapping pushes — the
    /// offline twin of `ListenerStats::overlap_trimmed_samples`. A log whose
    /// segments open within one preroll of the previous close re-sends that tail
    /// under its original capture indexes; this is how much. Nonzero is expected
    /// and explainable, not a fault.
    pub overlap_trimmed_samples: u64,
}

/// What can go wrong opening or driving a replay. A per-record decode error or a
/// corrupt record is *not* an error — it ends the log cleanly (recorded in
/// [`StopReason`]), matching the live connection's drop-on-decode-error; only an
/// unreadable log or a listener-inference failure surfaces here.
#[derive(Debug)]
pub enum ReplayError {
    /// The frame log could not be opened (including a missing file).
    Open(FrameLogError),
    /// A listener inference call failed (model error).
    Listener(WakeError),
}

impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplayError::Open(e) => write!(f, "open frame log: {e}"),
            ReplayError::Listener(e) => write!(f, "listener inference: {e:?}"),
        }
    }
}

impl std::error::Error for ReplayError {}

/// The shared models plus per-pod config a replay drives. The `ort` sessions load
/// once and are reused across every frame log a run replays; each replay gets its
/// own fresh [`ListenerState`] (a frame log is one connection).
pub struct ReplayListener {
    oww: OwwModels,
    silero: SileroModel,
    config: ListenerConfig,
}

/// The per-pod [`ListenerConfig`] a `[wake]` + `[endpointer]` pair describes.
fn listener_config(
    wake: &WakeConfig,
    endpointer: &EndpointerConfig,
    max_utterance_samples: u64,
) -> ListenerConfig {
    let (wake_tail_samples, command_wait_samples) = wake.hold_to_listener();
    ListenerConfig {
        oww_threshold: wake.threshold,
        endpointer: endpointer.to_listener(max_utterance_samples),
        wake_tail_samples,
        command_wait_samples,
        default_policy: wake.policy.to_listener(),
        ..ListenerConfig::default()
    }
}

impl ReplayListener {
    /// Build from already-loaded models and a per-pod config.
    pub fn new(oww: OwwModels, silero: SileroModel, config: ListenerConfig) -> ReplayListener {
        ReplayListener {
            oww,
            silero,
            config,
        }
    }

    /// Build from a daemon [`Config`]'s `[wake]` + `[endpointer]` tables, loading
    /// the models. Returns `None` unless both tables are present in the streaming
    /// listener's required form (`mode = "oww"` with its three model paths plus an
    /// `[endpointer]` table) — the same gating the live daemon applies, so a config
    /// missing either table replays nothing.
    pub fn from_config(config: &Config) -> Result<Option<ReplayListener>, WakeError> {
        let Some(wake) = config.wake.as_ref() else {
            return Ok(None);
        };
        // `Oww` is the only mode today; the guard is what a future mode that does
        // not stream OWW falls through, rather than loading models it has none of.
        if wake.mode != WakeMode::Oww {
            return Ok(None);
        }
        let Some(endpointer) = config.endpointer.as_ref() else {
            return Ok(None);
        };
        let oww = OwwModels::load(&OwwConfig {
            melspectrogram: wake
                .melspectrogram
                .clone()
                .expect("oww melspectrogram path present"),
            embedding: wake.embedding.clone().expect("oww embedding path present"),
            model: wake.model.clone().expect("oww model path present"),
            threshold: wake.threshold,
        })?;
        let silero = SileroModel::load(&SileroConfig {
            model: endpointer.model.clone(),
        })?;
        let max_utterance_samples =
            config.pipeline.max_segment_seconds * u64::from(SPINE_FORMAT.sample_rate_hz);
        let listener_config = listener_config(wake, endpointer, max_utterance_samples);
        Ok(Some(ReplayListener::new(oww, silero, listener_config)))
    }

    /// The per-pod listener config a replay drives with.
    pub fn config(&self) -> &ListenerConfig {
        &self.config
    }

    /// Consume into the loaded models and config — the live daemon's listener
    /// spawn builds from the same pieces.
    pub fn into_parts(self) -> (OwwModels, SileroModel, ListenerConfig) {
        (self.oww, self.silero, self.config)
    }
}

/// Replay one frame log through the listener with no device or daemon, returning
/// every [`ListenerEvent`] it produced. `epoch` stamps the connection (the live
/// daemon uses the connection sequence). The session is always finalized (an open
/// segment truncates), so an utterance in flight at end-of-log still gets its
/// device-release fallback carve.
pub fn replay_framelog(
    path: &Path,
    listener: &mut ReplayListener,
    epoch: u64,
) -> Result<ReplaySummary, ReplayError> {
    let reader = FrameLogReader::open(path).map_err(ReplayError::Open)?;

    // Replay is single-connection, so the ledger handle is private to this FSM;
    // it exists only to satisfy the resume/truncate bookkeeping.
    let mut fsm = SessionFsm::new(SPINE_FORMAT, ResumeLedger::shared());
    let mut state = ListenerState::new(listener.config);
    let mut pod: Option<PodId> = None;
    let mut events: Vec<ListenerEvent> = Vec::new();
    let mut records = 0u64;
    let mut last_host_rx = HostMicros(0);

    // Every exit closes the session under `close_cause` so an open segment
    // truncates and the listener still sees its outer boundary. A clean run-out
    // falls through to `Eof`.
    let mut stop = StopReason::Eof;
    let mut close_cause = CloseCause::Eof;
    for item in reader {
        match item {
            Ok(LogItem::Record { host_rx, payload }) => {
                last_host_rx = host_rx;
                records += 1;
                match decode_frame(&payload) {
                    Ok(frame) => {
                        let session_events = fsm.feed(frame, host_rx);
                        let fatal = drive(
                            &session_events,
                            &mut state,
                            &mut pod,
                            epoch,
                            &mut listener.oww,
                            &mut listener.silero,
                            &mut events,
                        )?;
                        if fatal {
                            stop = StopReason::ProtocolError;
                            break;
                        }
                    }
                    Err(_) => {
                        stop = StopReason::DecodeError;
                        close_cause = CloseCause::DecodeError;
                        break;
                    }
                }
            }
            Ok(LogItem::TornTail) => {
                stop = StopReason::TornTail;
                break;
            }
            Err(_) => {
                stop = StopReason::CorruptRecord;
                close_cause = CloseCause::ReadError;
                break;
            }
        }
    }

    // Single finalize for every path: close the session (truncating any open
    // segment) and feed the resulting boundary through the listener.
    let close_events = fsm.close(close_cause, last_host_rx);
    drive(
        &close_events,
        &mut state,
        &mut pod,
        epoch,
        &mut listener.oww,
        &mut listener.silero,
        &mut events,
    )?;

    Ok(ReplaySummary {
        records,
        stop,
        events,
        overlap_trimmed_samples: state.take_overlap_trimmed(),
    })
}

/// Feed a batch of session events into the listener state, collecting emitted
/// events. Returns whether a fatal protocol error parked the FSM (the caller
/// stops reading). Mirrors the live `connection` loop's per-event tap.
#[allow(clippy::too_many_arguments)]
fn drive(
    session_events: &[SessionEvent],
    state: &mut ListenerState,
    pod: &mut Option<PodId>,
    epoch: u64,
    oww: &mut OwwModels,
    silero: &mut SileroModel,
    out: &mut Vec<ListenerEvent>,
) -> Result<bool, ReplayError> {
    let mut fatal = false;
    for ev in session_events {
        if let SessionEvent::ProtocolError { fatal: true, .. } = ev {
            fatal = true;
        }
        if let Some(feed) = session_event_to_feed(ev, pod, epoch)
            && let Some(id) = pod.as_ref()
        {
            let emitted = state
                .handle(id, feed, oww, silero)
                .map_err(ReplayError::Listener)?;
            out.extend(emitted);
        }
    }
    Ok(fatal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::wake_table_toml;
    use speech_pipeline::WakePolicy;

    /// Every knob the assembly sets lands in its own field. The two ms values are
    /// mutually distinguishable on purpose: the sample counts arrive as a tuple,
    /// so equal values would hide a swapped destructuring — and the live daemon
    /// is built through this same function.
    #[test]
    fn listener_config_carries_every_wake_and_endpointer_knob() {
        let config = Config::parse(&wake_table_toml(
            "threshold = 0.42\nwake_tail_ms = 750\ncommand_wait_ms = 2000",
        ))
        .expect("parse");
        let wake = config.wake.as_ref().expect("wake table");
        let endpointer = config.endpointer.as_ref().expect("endpointer table");
        let built = listener_config(wake, endpointer, 16_000);
        assert_eq!(built.oww_threshold, 0.42, "the configured wake threshold");
        assert_eq!(built.wake_tail_samples, 12_000, "750 ms at 16 kHz");
        assert_eq!(built.command_wait_samples, 32_000, "2000 ms at 16 kHz");
        assert_eq!(
            built.endpointer,
            endpointer.to_listener(16_000),
            "the endpointer sub-config is that table's own conversion"
        );
    }

    /// The mapping half this module owns: a parsed `[wake] policy` reaches the
    /// listener config the replay and the live daemon are both built from. The
    /// spellings themselves are `config`'s to test.
    #[test]
    fn configured_policy_reaches_the_listener_config() {
        for (extra, want) in [
            ("policy = \"bypass\"", WakePolicy::Bypass),
            ("policy = \"gated\"", WakePolicy::WakeGated),
            ("", WakePolicy::WakeGated),
        ] {
            let config = Config::parse(&wake_table_toml(extra)).expect("parse");
            let wake = config.wake.as_ref().expect("wake table");
            let endpointer = config.endpointer.as_ref().expect("endpointer table");
            assert_eq!(
                listener_config(wake, endpointer, 16_000).default_policy,
                want,
                "policy line: {extra:?}"
            );
        }
    }
}
