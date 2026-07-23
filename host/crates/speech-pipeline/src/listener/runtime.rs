//! The listener runtime: per-pod [`ListenerState`] and the one shared OS thread
//! that drives every pod's streaming wake + endpointing off the async runtime.
//!
//! [`ListenerState`] assembles the four leaf components — [`OwwStream`], [`SileroVad`],
//! [`Endpointer`], [`PcmRing`] — into a live [`Feed`] → [`ListenerEvent`] machine.
//! Blocking ONNX inference must never run on a tokio worker, so (like the retired
//! `WakeStage`) it runs on a dedicated thread pulling `(PodId, Feed)` from a bounded
//! channel, holding a `HashMap<PodId, ListenerState>`. The ONNX sessions
//! ([`OwwModels`], [`SileroModel`]) are shared and driven serially — one thread
//! comfortably serves the pods we own; sharding by `PodId` hash is the scaling
//! escape hatch, needing no design change.
//!
//! What the state owns, and the rules it enforces:
//!
//! - **Streaming continuity.** Audio within a transport segment feeds the rolling
//!   OWW/Silero windows contiguously; a segment boundary or an in-stream gap
//!   re-anchors them so inference never runs across a hole. The PCM ring keeps its
//!   runs across a forward gap (carved silent) but is cleared on a reconnect.
//! - **The wake arm.** A threshold-crossing OWW detection arms the pod; the arm
//!   gates utterances under [`WakePolicy::WakeGated`] and is consumed by the
//!   utterance that passes, or by the device-release missed-onset fallback.
//! - **Utterance identity.** A fresh utterance mints a new [`ListenerUtteranceId`];
//!   a continuation reuses it (and its wake provenance). Terminal endpoints
//!   (`Capped`/`DeviceVadRelease`) and the continuation-window close return to a
//!   no-utterance state.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use pod_ingest::{ClockOffsetEstimate, DeviceMicros, HostMicros, samples_to_micros};
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use super::endpointer::{
    EndpointCause, EndpointEvent, Endpointer, EndpointerConfig, TransitionCause,
};
use super::event::{
    CarveTiming, CarvedUtterance, Feed, ListenerEvent, ListenerUtteranceId, StatsFlushCause,
    StatsModel, WakePolicy,
};
use super::oww_stream::{OwwModels, OwwStream};
use super::ring::PcmRing;
use super::silero::{SILERO_CHUNK, SileroModel, SileroVad};
use super::stats::{MODEL_STATS_FLUSH_CHUNKS, ScoreStats};
use crate::types::{PodId, SPINE_FORMAT, WakeConfirmation};
use crate::wake::WakeError;

/// Depth of the shared `(PodId, Feed)` channel. Audio priority belongs to
/// recording (a separate path); a full listener channel drops the chunk and counts
/// it rather than blocking the connection task.
const FEED_CHANNEL_DEPTH: usize = 512;

/// How long a marker send waits for room in the feed channel before giving up.
/// The channel drains at listener processing speed, so 512 entries clear in well
/// under a second in any healthy state; this only fires when the consumer is
/// genuinely wedged, bounding the stall a wedged listener imposes on a
/// connection task.
const MARKER_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Per-pod listener knobs. `Copy` so the thread hands a fresh copy to each new pod.
#[derive(Debug, Clone, Copy)]
pub struct ListenerConfig {
    /// Sigmoid score strictly above which a chunk arms a wake.
    pub oww_threshold: f32,
    /// Endpointer timing/threshold knobs (also sizes the PCM ring).
    pub endpointer: EndpointerConfig,
    /// A wake arms an utterance whose start falls within this many samples after
    /// the wake end (`wake_end ∈ [start − arm_slack, soft_endpoint]`).
    pub arm_slack_samples: u64,
    /// Samples of margin kept before the wake end when trimming for STT.
    pub stt_margin_samples: usize,
    /// Wake-gating policy applied to a pod on first sight.
    pub default_policy: WakePolicy,
    /// Barge-in trigger knobs. The guard fires only while playback is active and
    /// interruptible for the pod.
    pub barge_in: BargeInConfig,
}

impl Default for ListenerConfig {
    fn default() -> ListenerConfig {
        ListenerConfig {
            oww_threshold: 0.5,
            endpointer: EndpointerConfig::default(),
            arm_slack_samples: 16_000,
            stt_margin_samples: 3_200,
            default_policy: WakePolicy::WakeGated,
            barge_in: BargeInConfig::default(),
        }
    }
}

/// Barge-in detection knobs: sustained confident speech during interruptible
/// playback is the trigger. The sustain run is what keeps a dog bark, a TV burst,
/// or AEC residual from killing a response mid-sentence.
#[derive(Debug, Clone, Copy)]
pub struct BargeInConfig {
    /// P(speech) at/above which a chunk counts toward the sustain guard. Must be
    /// ≥ `EndpointerConfig::onset_thresh` (see [`BargeInConfig::validate`]).
    pub sustain_thresh: f32,
    /// Consecutive qualifying Silero chunks (32 ms each) required to fire. 8
    /// chunks = 256 ms. Must be ≥ `EndpointerConfig::onset_chunks`.
    pub sustain_chunks: u32,
}

impl Default for BargeInConfig {
    fn default() -> BargeInConfig {
        BargeInConfig {
            sustain_thresh: 0.60,
            sustain_chunks: 8,
        }
    }
}

impl BargeInConfig {
    /// Panics unless the trigger is strictly lazier than the endpointer's onset,
    /// on both axes. The floor-open path depends on it: the endpointer must have
    /// onset before the trigger fires, so the barging speech is already being
    /// tracked — and ring-buffered from its preroll-padded start — by the time the
    /// utterance takes the barge mark. A config that fired first would trigger on
    /// speech no utterance ever carries.
    fn validate(&self, endpointer: &EndpointerConfig) {
        assert!(
            self.sustain_thresh >= endpointer.onset_thresh,
            "barge_in.sustain_thresh ({}) must be >= endpointer.onset_thresh ({})",
            self.sustain_thresh,
            endpointer.onset_thresh,
        );
        assert!(
            self.sustain_chunks >= endpointer.onset_chunks,
            "barge_in.sustain_chunks ({}) must be >= endpointer.onset_chunks ({})",
            self.sustain_chunks,
            endpointer.onset_chunks,
        );
    }
}

/// What the listener knows about this pod's playback, and the trigger state it
/// gates. Default: nothing playing, so the guard is closed.
#[derive(Debug, Clone, Copy, Default)]
struct PlaybackFloor {
    active: bool,
    interruptible: bool,
    /// Consecutive chunks at/above `sustain_thresh`; any lower chunk resets it.
    sustain_run: u32,
    /// One trigger per playback session: set when it fires, cleared when playback
    /// goes inactive or a fresh `active: true` arrives.
    fired: bool,
}

impl PlaybackFloor {
    /// Whether the guard may count chunks: something interruptible is audible and
    /// this session has not already barged.
    fn open(&self) -> bool {
        self.active && self.interruptible && !self.fired
    }
}

/// The last threshold-crossing wake, in absolute sample indexes.
#[derive(Debug, Clone, Copy)]
struct WakeArm {
    score: f32,
    wake_end_sample: u64,
    /// Host receipt of the chunk whose scoring completed this detection.
    detected_rx: Option<HostMicros>,
}

/// What an open transport segment tells the listener about time. Rebuilt on every
/// `SegmentOpened`: the clock offset is estimated per segment (intra-segment drift
/// is ppm-negligible), and an utterance never spans segments — the close finalizes
/// any in-progress one — so the enclosing segment's record is always the right one
/// at carve time.
#[derive(Debug, Clone, Copy)]
struct SegmentOpen {
    base_sample_index: u64,
    /// The device VAD went high at `base_sample_index + preroll_samples`.
    preroll_samples: u32,
    base_device_ts: DeviceMicros,
    /// Host receipt of the segment's first audio frame — t0 for an utterance that
    /// opened this segment.
    first_audio_rx: Option<HostMicros>,
    /// Device→host clock offset, estimated over this segment's post-preroll
    /// chunks. `None` until one arrives.
    offset: Option<ClockOffsetEstimate>,
}

impl SegmentOpen {
    /// The device instant the sample at `index` was captured.
    fn device_ts_at(&self, index: u64) -> DeviceMicros {
        let offset_samples = index.saturating_sub(self.base_sample_index);
        self.base_device_ts.advanced_by(samples_to_micros(
            offset_samples,
            SPINE_FORMAT.sample_rate_hz,
        ))
    }

    /// Estimated host instant the device VAD went high: the projection of the
    /// first post-preroll sample's capture time.
    fn vad_high_est(&self) -> Option<HostMicros> {
        let est = self.offset.as_ref()?;
        Some(
            est.project(self.base_device_ts.advanced_by(samples_to_micros(
                u64::from(self.preroll_samples),
                SPINE_FORMAT.sample_rate_hz,
            ))),
        )
    }
}

/// An utterance's axis origin, derived once at its first carve and reused by every
/// later carve under the same id.
///
/// A projected t0 (and `vad_high_est`) reads the segment's clock-offset estimate,
/// whose min filter keeps narrowing as frames arrive. Re-deriving them per carve
/// would move one utterance's axis origin between its successive `utterance` lines;
/// freezing keeps the stamps stable across a continuation's re-carve, so an
/// utterance has one origin no matter how many times it is re-carved.
#[derive(Clone, Copy)]
struct CarveAnchor {
    first_audio_rx: Option<HostMicros>,
    t0_projected: bool,
    vad_high_est: Option<HostMicros>,
}

/// One pod's continuous-listener state. Drive it with [`handle`](ListenerState::handle);
/// it borrows the shared models per call.
pub struct ListenerState {
    config: ListenerConfig,
    policy: WakePolicy,
    oww: OwwStream,
    silero: SileroVad,
    endpointer: Endpointer,
    ring: PcmRing,
    /// Buffered samples not yet forming a whole Silero chunk.
    silero_pending: Vec<i16>,
    /// Absolute index one past the last sample fed to Silero.
    silero_cursor: u64,
    /// Absolute index the OWW stream's position 0 maps to (its last reset point).
    oww_base: u64,
    /// Absolute index the next contiguous audio sample is expected at.
    expected_next: Option<u64>,
    /// The armed wake, if any (consumed by the gated utterance).
    wake: Option<WakeArm>,
    /// The utterance currently accumulating (a continuation reuses it).
    current_id: Option<ListenerUtteranceId>,
    /// The current utterance's wake provenance, reused across continuations.
    current_wake: Option<WakeConfirmation>,
    /// This pod's playback state and barge-in trigger state.
    playback: PlaybackFloor,
    /// A fired trigger no utterance has taken yet: the next carve consumes it.
    /// Needed because utterance identity is minted at first *carve*, not at onset,
    /// so at trigger time there is often no id to mark.
    barge_pending: bool,
    /// The barge mark on the utterance currently accumulating, reused across
    /// continuations exactly as `current_wake` is.
    current_barge: bool,
    /// The open transport segment's timing anchors, if one is open.
    segment: Option<SegmentOpen>,
    /// Host receipt of the chunk that drove the current utterance's `Onset`, and
    /// of the wake that gated it — carried from the arm so a continuation (which
    /// re-carves under the same id) keeps the original provenance.
    current_onset_rx: Option<HostMicros>,
    current_wake_rx: Option<HostMicros>,
    /// The current utterance's axis origin, frozen at its first carve.
    current_anchor: Option<CarveAnchor>,
    /// Per-pod monotonic utterance sequence.
    utterance_seq: u64,
    /// Connection epoch, stamped onto every minted id.
    epoch: u64,
    /// Duplicate samples the ring trimmed since the last [`take_overlap_trimmed`]
    /// (`ListenerState::take_overlap_trimmed`). Routine segment-preroll re-sends
    /// land here; the thread drains it into the shared stats.
    overlap_trimmed: u64,
    /// Silero P(speech) since the last flush — the diagnostic the endpointer's
    /// transitions cannot give, because a model returning a flat low score
    /// transitions never.
    silero_stats: ScoreStats,
    /// OWW wake-head scores since the last flush. Empty (and so silent) on a pod
    /// fed synthetic probabilities, which performs no OWW pushes.
    oww_stats: ScoreStats,
}

impl ListenerState {
    /// A fresh state at epoch 0 (a `Connected` feed adopts the real epoch).
    pub fn new(config: ListenerConfig) -> ListenerState {
        config.barge_in.validate(&config.endpointer);
        let capacity = (config.endpointer.max_utterance_samples
            + config.endpointer.preroll_pad_samples) as usize;
        ListenerState {
            config,
            policy: config.default_policy,
            oww: OwwStream::new(config.oww_threshold),
            silero: SileroVad::new(),
            endpointer: Endpointer::new(config.endpointer),
            ring: PcmRing::new(capacity),
            silero_pending: Vec::new(),
            silero_cursor: 0,
            oww_base: 0,
            expected_next: None,
            wake: None,
            current_id: None,
            current_wake: None,
            playback: PlaybackFloor::default(),
            barge_pending: false,
            current_barge: false,
            segment: None,
            current_onset_rx: None,
            current_wake_rx: None,
            current_anchor: None,
            utterance_seq: 0,
            epoch: 0,
            overlap_trimmed: 0,
            silero_stats: ScoreStats::default(),
            oww_stats: ScoreStats::default(),
        }
    }

    /// Take the duplicate-sample count the ring has trimmed since the last call.
    /// Segment preroll re-sends audio the ring already holds (the samples keep
    /// their original capture indexes), so a steady rate here is expected; a spike
    /// means an index-domain anomaly worth looking at.
    pub fn take_overlap_trimmed(&mut self) -> u64 {
        std::mem::take(&mut self.overlap_trimmed)
    }

    /// Process one [`Feed`] item for `pod`, returning any [`ListenerEvent`]s it
    /// produced. An inference error propagates (the thread counts and drops it).
    pub fn handle(
        &mut self,
        pod: &PodId,
        feed: Feed,
        oww_models: &mut OwwModels,
        silero_model: &mut SileroModel,
    ) -> Result<Vec<ListenerEvent>, WakeError> {
        match feed {
            Feed::Connected { epoch } => {
                // A pending arm the prior connection never resolved into a command
                // expires with the connection.
                let mut events = Vec::new();
                let expiry = self.expected_next.unwrap_or(self.silero_cursor);
                self.expire_unconsumed_arm(pod, expiry, &mut events);
                // Flushes the models' scores under the old epoch (`full_reset` →
                // `reset_stream`), which is the only epoch they mean anything in.
                // Anchored at the teardown position, not the new stream's 0: the
                // reset transition below is stamped in the *old* connection's index
                // domain, which is the only domain it means anything in. The anchor
                // itself is overwritten by the `SegmentOpened` (or the discontinuity
                // branch in `handle_audio`) that precedes any new-connection audio.
                self.full_reset(pod, expiry, &mut events);
                // Drain under the old epoch: a reset transition closes out the
                // connection being torn down, before the new epoch is adopted.
                self.drain_transitions(pod, None, &mut events);
                self.epoch = epoch;
                self.expected_next = None;
                Ok(events)
            }
            Feed::SegmentOpened {
                base_sample_index,
                preroll_samples,
                base_device_ts,
            } => {
                // A new transport segment follows real device-VAD silence; the
                // prior close already finalized any in-progress utterance, so
                // re-anchor the streaming inference state to the new base.
                let mut events = Vec::new();
                self.reset_stream(pod, base_sample_index, &mut events);
                self.expected_next = Some(base_sample_index);
                // The timing anchors are per segment: the clock offset re-estimates
                // over this segment's chunks, and t0 for an utterance that opens it
                // is this segment's first-audio receipt.
                self.segment = Some(SegmentOpen {
                    base_sample_index,
                    preroll_samples,
                    base_device_ts,
                    first_audio_rx: None,
                    offset: None,
                });
                self.drain_transitions(pod, None, &mut events);
                Ok(events)
            }
            Feed::Audio {
                first_sample_index,
                gap,
                pcm,
                device_ts,
                host_rx,
            } => self.handle_audio(
                pod,
                first_sample_index,
                gap.is_some(),
                &pcm,
                device_ts,
                host_rx,
                oww_models,
                silero_model,
            ),
            Feed::PlaybackState {
                active,
                interruptible,
            } => {
                self.set_playback(active, interruptible);
                Ok(Vec::new())
            }
            Feed::SegmentClosed { host_rx, .. } => self.handle_close(pod, host_rx),
        }
    }

    /// Full reset for a reconnect: streaming state, the ring, the wake arm, the
    /// in-progress utterance, and the segment's timing anchors all go (the new
    /// connection's index and device-clock domains are unrelated to the old one's).
    /// Re-anchors inference to `anchor`.
    fn full_reset(&mut self, pod: &PodId, anchor: u64, events: &mut Vec<ListenerEvent>) {
        self.reset_stream(pod, anchor, events);
        self.ring.reset();
        self.wake = None;
        self.current_id = None;
        self.current_wake = None;
        // A reconnect killed the writer, so the floor is genuinely closed — and
        // the pending trigger belongs to a response nobody can still be hearing.
        self.playback = PlaybackFloor::default();
        self.barge_pending = false;
        self.current_barge = false;
        self.segment = None;
        self.current_onset_rx = None;
        self.current_wake_rx = None;
        self.current_anchor = None;
    }

    /// Reset the rolling inference state (OWW/Silero windows, endpointer, Silero
    /// buffer) and re-anchor its sample cursors to `anchor`. Leaves the ring, the
    /// wake arm, and utterance identity untouched.
    ///
    /// Flushes the model accumulators first: chunks scored before a discontinuity,
    /// reconnect, or segment re-anchor are as diagnostic as any others, and the
    /// reset is about to make their sample indexes meaningless.
    fn reset_stream(&mut self, pod: &PodId, anchor: u64, events: &mut Vec<ListenerEvent>) {
        self.flush_model_stats(pod, StatsFlushCause::Reset, events);
        self.oww.reset();
        self.silero.reset();
        self.endpointer.reset(anchor);
        self.silero_pending.clear();
        self.oww_base = anchor;
        self.silero_cursor = anchor;
    }

    /// Drain both model accumulators into [`ListenerEvent::ModelStats`] events.
    /// Every flush point drains both, so wherever real audio feeds both models the
    /// lines pair up; an accumulator with nothing in it emits nothing, which is
    /// what keeps a synthetic-probability run (Silero only) from emitting empty
    /// OWW lines. Flush points compose rather than double-emit: a second flush
    /// reaching an already-drained accumulator is a no-op.
    fn flush_model_stats(
        &mut self,
        pod: &PodId,
        cause: StatsFlushCause,
        events: &mut Vec<ListenerEvent>,
    ) {
        let epoch = self.epoch;
        let drained = [
            (StatsModel::Silero, self.silero_stats.flush()),
            (StatsModel::Oww, self.oww_stats.flush()),
        ];
        for (model, summary) in drained {
            if let Some(summary) = summary {
                events.push(ListenerEvent::ModelStats {
                    pod: pod.clone(),
                    epoch,
                    model,
                    cause,
                    summary,
                });
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_audio(
        &mut self,
        pod: &PodId,
        first_sample_index: u64,
        flagged_gap: bool,
        pcm: &[i16],
        device_ts: DeviceMicros,
        host_rx: HostMicros,
        oww_models: &mut OwwModels,
        silero_model: &mut SileroModel,
    ) -> Result<Vec<ListenerEvent>, WakeError> {
        let mut events = Vec::new();
        self.observe_arrival(first_sample_index, pcm.len() as u64, device_ts, host_rx);

        // A hole (dropped chunk / channel-full) or a fresh anchor: never run
        // inference across it.
        let discontinuity = flagged_gap || self.expected_next != Some(first_sample_index);
        if discontinuity {
            // Abandon any in-progress utterance — its STT would span the hole.
            if let Some(id) = self.take_utterance() {
                events.push(ListenerEvent::UtteranceClosed {
                    pod: pod.clone(),
                    utterance_id: id,
                });
            }
            // A backward jump can't index the ring; drop it. A forward gap keeps
            // its runs (the ring carves the hole as silence).
            if matches!(self.expected_next, Some(e) if first_sample_index < e) {
                self.ring.reset();
            }
            self.reset_stream(pod, first_sample_index, &mut events);
            self.drain_transitions(pod, None, &mut events);
        }

        // A segment's preroll is stamped with the samples' original capture
        // indexes, so a segment opening within one preroll of the previous close
        // re-sends retained audio: the ring dedupes it and reports the overlap.
        // `expected_next` still advances past the whole push — inference was
        // re-anchored by the `SegmentOpened` that preceded it and re-scores the
        // duplicate.
        self.overlap_trimmed += self.ring.push(first_sample_index, pcm) as u64;
        self.expected_next = Some(first_sample_index + pcm.len() as u64);

        // Streaming wake. Each step's score is recorded at the same pod-absolute
        // translation the arm path uses, so "wake fires fine" becomes a number
        // sitting next to the Silero one rather than an anecdote.
        for scored in self.oww.push(oww_models, pcm)? {
            self.oww_stats
                .record(scored.score, self.oww_base + scored.end_sample);
            if let Some(det) = self.oww.arm(&scored) {
                let wake_end_sample = self.oww_base + det.wake_end_sample;
                // A prior unconsumed arm is superseded by this fresh wake — it
                // fired with no command in between.
                self.expire_unconsumed_arm(pod, wake_end_sample, &mut events);
                self.wake = Some(WakeArm {
                    score: det.score,
                    wake_end_sample,
                    // This frame's receipt: its scoring is what completed the
                    // detection.
                    detected_rx: Some(host_rx),
                });
                events.push(ListenerEvent::WakeDetected {
                    pod: pod.clone(),
                    epoch: self.epoch,
                    score: det.score,
                    wake_end_sample,
                });
            }
        }

        // Silero → endpointer, on the 512-sample chunk cadence.
        self.silero_pending.extend_from_slice(pcm);
        while self.silero_pending.len() >= SILERO_CHUNK {
            let chunk: Vec<i16> = self.silero_pending.drain(..SILERO_CHUNK).collect();
            let p = self.silero.push(silero_model, &chunk)?;
            let chunk_end = self.silero_cursor + SILERO_CHUNK as u64;
            self.silero_cursor = chunk_end;
            // A Silero chunk re-blocked across two frames completes on receipt of
            // this one, so this frame's `host_rx` is when its audio was all in
            // hand — the nearest true receipt for the chunk.
            self.drive_probability(pod, p, chunk_end, host_rx, &mut events);
        }

        Ok(events)
    }

    /// Fold one audio frame's arrival into the open segment's timing anchors: the
    /// first frame's receipt is t0 for an utterance that opened the segment, and
    /// post-preroll frames feed the clock-offset estimate.
    ///
    /// Preroll frames are excluded to keep the filter's inputs homogeneous: the
    /// preroll backlog drains at 4× real time. (The min filter would survive them
    /// regardless — sending late can only raise `host_rx − device_ts`.)
    fn observe_arrival(
        &mut self,
        first_sample_index: u64,
        samples: u64,
        device_ts: DeviceMicros,
        host_rx: HostMicros,
    ) {
        let Some(seg) = self.segment.as_mut() else {
            return;
        };
        if seg.first_audio_rx.is_none() {
            seg.first_audio_rx = Some(host_rx);
        }
        if first_sample_index < seg.base_sample_index + u64::from(seg.preroll_samples) {
            return;
        }
        match seg.offset.as_mut() {
            Some(est) => est.observe(host_rx, device_ts, samples),
            None => {
                seg.offset = Some(ClockOffsetEstimate::from_observation(
                    host_rx,
                    device_ts,
                    samples,
                    SPINE_FORMAT.sample_rate_hz,
                ))
            }
        }
    }

    /// Feed one Silero probability through the endpointer: record it, push, drain
    /// the transitions it recorded, then apply any boundary event. The record and
    /// the drain belong to the push — keeping them inseparable here is what lets
    /// [`drive_probability_for_test`] exercise production wiring rather than a
    /// copy of it. Recording at the model-push site in `handle_audio` instead
    /// would leave every synthetic-probability test blind to the stats.
    fn drive_probability(
        &mut self,
        pod: &PodId,
        p: f32,
        chunk_end_sample: u64,
        host_rx: HostMicros,
        events: &mut Vec<ListenerEvent>,
    ) {
        self.silero_stats.record(p, chunk_end_sample);
        self.drive_barge_guard(pod, p, chunk_end_sample, host_rx, events);
        let ev = self.endpointer.push(p, chunk_end_sample);
        // Flushes (inside the drain) before the transition events, so a transition
        // line arrives with the stats of the chunks that led to it — this one
        // included. The drain is also where an `Onset` gets its receipt stamp:
        // `push` is the only call that can produce one.
        self.drain_transitions(pod, Some(host_rx), events);
        if let Some(ev) = ev {
            self.apply_endpoint_event(pod, ev, host_rx, events);
        }
        // The heartbeat: a stretch with no transitions is exactly where transition
        // logging goes silent, so cap the accumulation. Checked after the drain, so
        // a transition on this very chunk reports it under `Transition` and leaves
        // nothing for the cap to emit.
        if self.silero_stats.len() >= MODEL_STATS_FLUSH_CHUNKS {
            self.flush_model_stats(pod, StatsFlushCause::Periodic, events);
        }
    }

    /// Adopt a new playback state, resetting the trigger with it. A fresh start
    /// re-arms the latch: each response is interruptible on its own, and so is the
    /// barge readback that follows one. A stop closes the guard, and the run a
    /// half-counted burst left behind means nothing to the next response.
    fn set_playback(&mut self, active: bool, interruptible: bool) {
        self.playback = PlaybackFloor {
            active,
            interruptible,
            sustain_run: 0,
            fired: false,
        };
    }

    /// The barge-in guard, one Silero chunk at a time: count consecutive chunks at
    /// or above `sustain_thresh` while interruptible playback is audible, and fire
    /// once the run is long enough. Any chunk below the threshold resets the run —
    /// a bark or a burst of TV never accumulates 256 ms.
    ///
    /// The run counts only while the floor is open, so speech that started before
    /// playback (or continues past a non-interruptible job's start) has to sustain
    /// itself *during* the response to cut it.
    fn drive_barge_guard(
        &mut self,
        pod: &PodId,
        p: f32,
        chunk_end_sample: u64,
        host_rx: HostMicros,
        events: &mut Vec<ListenerEvent>,
    ) {
        if !self.playback.open() {
            return;
        }
        if p < self.config.barge_in.sustain_thresh {
            self.playback.sustain_run = 0;
            return;
        }
        self.playback.sustain_run += 1;
        if self.playback.sustain_run < self.config.barge_in.sustain_chunks {
            return;
        }
        self.playback.fired = true;
        events.push(ListenerEvent::BargeIn {
            pod: pod.clone(),
            epoch: self.epoch,
            trigger_sample: chunk_end_sample,
            host_rx,
        });
        // The barging speech is already tracked (the config invariant puts the
        // endpointer's onset at or before this chunk), but its identity is minted
        // at the first carve. An utterance already accumulating — a continuation
        // resumed during playback — takes the mark now; otherwise the next carve
        // consumes the pending trigger.
        match self.current_id.is_some() {
            true => self.current_barge = true,
            false => self.barge_pending = true,
        }
    }

    fn handle_close(
        &mut self,
        pod: &PodId,
        host_rx: HostMicros,
    ) -> Result<Vec<ListenerEvent>, WakeError> {
        let mut events = Vec::new();
        // What the models saw across the whole segment — in a room where the FSM
        // never transitions, this is the only line that says so.
        self.flush_model_stats(pod, StatsFlushCause::SegmentClose, &mut events);
        let close_sample = self.expected_next.unwrap_or(self.silero_cursor);
        let armed_wake_end = self.wake.map(|a| a.wake_end_sample);
        let ev = self
            .endpointer
            .on_device_release(close_sample, armed_wake_end);
        self.drain_transitions(pod, None, &mut events);
        if let Some(ev) = ev {
            // The close is what drove this carve, so its receipt is the endpoint
            // stamp.
            self.apply_endpoint_event(pod, ev, host_rx, &mut events);
        }
        // The device boundary ends the arm's life. An arm the missed-onset fallback
        // carve above did not consume expired with the segment — the wake got no
        // command. Any utterance identity that survived a wake-gated-drop was never
        // minted, so nothing to emit for it (the terminal event above already closed
        // a minted one).
        self.expire_unconsumed_arm(pod, close_sample, &mut events);
        self.clear_utterance();
        Ok(events)
    }

    /// Turn an endpointer boundary event into listener events, threading the
    /// utterance-identity rules through it.
    fn apply_endpoint_event(
        &mut self,
        pod: &PodId,
        ev: EndpointEvent,
        host_rx: HostMicros,
        events: &mut Vec<ListenerEvent>,
    ) {
        match ev {
            EndpointEvent::SoftEndpoint {
                start_sample,
                end_sample,
                cause,
            } => {
                if let Some(utt) =
                    self.carve_utterance(pod, start_sample, end_sample, cause, host_rx)
                {
                    events.push(ListenerEvent::SoftEndpoint {
                        pod: pod.clone(),
                        utterance: utt,
                    });
                }
                // A terminal endpoint returned the endpointer to `Idle`; a natural
                // soft endpoint awaits continuation and keeps the id.
                if matches!(
                    cause,
                    EndpointCause::Capped | EndpointCause::DeviceVadRelease
                ) {
                    self.clear_utterance();
                }
            }
            EndpointEvent::Superseded => {
                if let Some(id) = self.current_id.clone() {
                    events.push(ListenerEvent::Superseded {
                        pod: pod.clone(),
                        utterance_id: id,
                    });
                }
            }
            EndpointEvent::UtteranceClosed => {
                if let Some(id) = self.take_utterance() {
                    events.push(ListenerEvent::UtteranceClosed {
                        pod: pod.clone(),
                        utterance_id: id,
                    });
                }
            }
        }
    }

    /// Carve the utterance's PCM and attach identity + wake provenance. Returns
    /// `None` under [`WakePolicy::WakeGated`] when no armed wake falls in the arm
    /// window (the utterance stays internal). A continuation (identity already
    /// present) is past the gate and always carves.
    fn carve_utterance(
        &mut self,
        pod: &PodId,
        start: u64,
        end: u64,
        cause: EndpointCause,
        host_rx: HostMicros,
    ) -> Option<CarvedUtterance> {
        let (id, wake) = match self.current_id.clone() {
            Some(id) => (id, self.current_wake),
            None => {
                // A fired trigger is consumed by the utterance that passes on it,
                // exactly as a wake arm is. Taken before the gate so it cannot leak
                // into a later, unrelated utterance.
                let barge = std::mem::take(&mut self.barge_pending);
                let wake = match self.policy {
                    WakePolicy::Bypass => None,
                    WakePolicy::WakeGated => {
                        let lo = start.saturating_sub(self.config.arm_slack_samples);
                        let armed = self
                            .wake
                            .filter(|a| a.wake_end_sample >= lo && a.wake_end_sample <= end);
                        match armed {
                            Some(arm) => {
                                self.wake = None;
                                // The arm's receipt travels with the provenance it
                                // belongs to, so a continuation re-carving under the
                                // same id still reports when the wake was actually
                                // heard.
                                self.current_wake_rx = arm.detected_rx;
                                let wake_end_rel =
                                    arm.wake_end_sample.saturating_sub(start) as usize;
                                Some(WakeConfirmation {
                                    score: arm.score,
                                    wake_end_sample: wake_end_rel,
                                    stt_trim_samples: wake_end_rel
                                        .saturating_sub(self.config.stt_margin_samples),
                                })
                            }
                            // Playback of a response *to you* is an open floor: the
                            // speech that cut it is heard without a wake word, and
                            // carries no wake provenance.
                            None if barge => None,
                            None => return None,
                        }
                    }
                };
                self.utterance_seq += 1;
                let id = ListenerUtteranceId {
                    pod: pod.clone(),
                    epoch: self.epoch,
                    seq: self.utterance_seq,
                };
                self.current_id = Some(id.clone());
                self.current_wake = wake;
                self.current_barge = barge;
                (id, wake)
            }
        };
        let pcm = self.ring.carve(start, end);
        let stt_trim_samples = wake.map_or(0, |w| w.stt_trim_samples);
        // Derived once per utterance and reused: a continuation moves the endpoint,
        // never the origin the endpoint is measured from.
        let anchor = match self.current_anchor {
            Some(anchor) => anchor,
            None => {
                let (first_audio_rx, t0_projected) = self.t0_for(start);
                let anchor = CarveAnchor {
                    first_audio_rx,
                    t0_projected,
                    vad_high_est: self.segment.as_ref().and_then(SegmentOpen::vad_high_est),
                };
                self.current_anchor = Some(anchor);
                anchor
            }
        };
        Some(CarvedUtterance {
            utterance_id: id,
            pcm,
            start_sample: start,
            end_sample: end,
            wake,
            stt_trim_samples,
            cause,
            barge_in: self.current_barge,
            timing: CarveTiming {
                first_audio_rx: anchor.first_audio_rx,
                t0_projected: anchor.t0_projected,
                wake_detected_rx: wake.and(self.current_wake_rx),
                onset_rx: self.current_onset_rx,
                soft_endpoint_rx: Some(host_rx),
                vad_high_est: anchor.vad_high_est,
            },
        })
    }

    /// t0 for an utterance starting at `start`: host receipt of its first audio,
    /// and whether that receipt is projected rather than measured.
    ///
    /// An utterance whose audio begins inside the device preroll is the reason the
    /// device VAD went high, so it opened the segment and the segment's first-audio
    /// receipt is *its* first-audio receipt — a measurement. An utterance starting
    /// later (a second command inside one segment, or a wake arriving while music
    /// held the VAD open) was never separately received: its start is projected off
    /// the device clock, and the estimator is always populated by then — a
    /// mid-segment onset is preceded by post-preroll chunks by definition.
    ///
    /// A borderline misclassification is benign: near the segment head the two
    /// agree to within the projection's transport bias, so the branch needs no
    /// slack term.
    fn t0_for(&self, start: u64) -> (Option<HostMicros>, bool) {
        // No open segment: a dropped `SegmentOpened` marker. There is no receipt
        // to name and nothing to project from.
        let Some(seg) = self.segment.as_ref() else {
            return (None, false);
        };
        if start <= seg.base_sample_index + u64::from(seg.preroll_samples) {
            return (seg.first_audio_rx, false);
        }
        (
            seg.offset
                .as_ref()
                .map(|e| e.project(seg.device_ts_at(start))),
            true,
        )
    }

    /// Emit the "wake, no follow" accounting ([`ListenerEvent::ArmExpired`]) when
    /// an armed wake is dropped without any utterance consuming it. A consumed arm
    /// (`self.wake` already `None`, cleared by [`carve_utterance`]) is a no-op.
    /// `expiry_sample` bounds the fallback span's end. The wake offsets are made
    /// relative to the span start (`wake_end − preroll_pad`), the same framing a
    /// carved utterance uses.
    fn expire_unconsumed_arm(
        &mut self,
        pod: &PodId,
        expiry_sample: u64,
        events: &mut Vec<ListenerEvent>,
    ) {
        let Some(arm) = self.wake.take() else {
            return;
        };
        let start = arm
            .wake_end_sample
            .saturating_sub(self.config.endpointer.preroll_pad_samples);
        let wake_end_rel = arm.wake_end_sample.saturating_sub(start) as usize;
        events.push(ListenerEvent::ArmExpired {
            pod: pod.clone(),
            wake: WakeConfirmation {
                score: arm.score,
                wake_end_sample: wake_end_rel,
                stt_trim_samples: wake_end_rel.saturating_sub(self.config.stt_margin_samples),
            },
            start_sample: start,
            end_sample: expiry_sample.max(arm.wake_end_sample),
        });
    }

    /// Mark the stream discontinuous after a mid-chunk inference error. When
    /// `handle_audio` fails partway, the streaming cursors (`silero_cursor`,
    /// `silero_pending`, the OWW window) can end up out of step with the audio
    /// they actually classified — a drained Silero chunk lost or an OWW step
    /// skipped — while `expected_next` already advanced past it. Left as-is, every
    /// later endpointer span and carve would be silently shifted. Dropping
    /// `expected_next` routes the next audio chunk through the same discontinuity
    /// recovery a dropped-chunk gap uses: it abandons any in-progress utterance
    /// (emitting `UtteranceClosed`) and re-anchors the cursors, while keeping the
    /// ring's retained audio.
    pub fn note_inference_error(&mut self) {
        self.expected_next = None;
    }

    /// Drain the endpointer's recorded FSM transitions into `endpointer_transition`
    /// listener events, stamped with the pod and current epoch. Called after every
    /// endpointer mutation (`push`/`on_device_release`/`reset`) so transitions
    /// surface in the order they occurred.
    ///
    /// A non-empty drain also flushes the model accumulators first, so each
    /// transition line arrives behind the scores that explain it. The emptiness
    /// check is load-bearing: this runs once per Silero chunk and usually drains
    /// nothing, so flushing unconditionally would emit `model_stats` per chunk —
    /// the one thing this observability must never become.
    /// `rx` is the host receipt of the audio being processed, or `None` for the
    /// callers that cannot produce an `Onset` (segment boundaries, resets, device
    /// release) — only `Endpointer::push` onsets, and only `drive_probability`
    /// calls it.
    fn drain_transitions(
        &mut self,
        pod: &PodId,
        rx: Option<HostMicros>,
        events: &mut Vec<ListenerEvent>,
    ) {
        let drained = self.endpointer.drain_transitions();
        if drained.is_empty() {
            return;
        }
        self.flush_model_stats(pod, StatsFlushCause::Transition, events);
        let epoch = self.epoch;
        for transition in drained {
            if transition.cause == TransitionCause::Onset {
                self.current_onset_rx = rx;
            }
            events.push(ListenerEvent::EndpointerTransition {
                pod: pod.clone(),
                epoch,
                transition,
            });
        }
    }

    /// Clear and return the in-progress utterance identity.
    fn take_utterance(&mut self) -> Option<ListenerUtteranceId> {
        let id = self.current_id.take();
        self.clear_utterance();
        id
    }

    /// Drop the in-progress utterance's identity, wake provenance, receipt stamps,
    /// and axis origin together — they are one utterance's state, so nothing
    /// outlives it into the next.
    fn clear_utterance(&mut self) {
        self.current_id = None;
        self.current_wake = None;
        self.current_barge = false;
        self.current_onset_rx = None;
        self.current_wake_rx = None;
        self.current_anchor = None;
    }

    #[cfg(test)]
    fn arm_wake_for_test(&mut self, score: f32, wake_end_sample: u64) {
        self.wake = Some(WakeArm {
            score,
            wake_end_sample,
            detected_rx: Some(rx_at(wake_end_sample)),
        });
    }

    /// Store audio directly in the ring at an absolute index, so a synthetic-`P`
    /// carve has real samples to slice. This + [`drive_probability_for_test`]
    /// exercise the listener's own endpointer wiring on synthetic probabilities,
    /// which is the right tool for exhaustive FSM-edge coverage regardless: model
    /// inference per edge case would be slow and imprecise.
    ///
    /// The natural onset→endpoint path has its own real-model coverage in
    /// [`real_audio_drives_onset_to_soft_endpoint_carve`](tests::real_audio_drives_onset_to_soft_endpoint_carve);
    /// this helper is for the FSM edges that path cannot reach deterministically.
    #[cfg(test)]
    fn push_ring_for_test(&mut self, index: u64, pcm: &[i16]) {
        // Accumulates the trim count exactly as `handle_audio` does, so the ring's
        // overlap accounting is the same here as in production.
        self.overlap_trimmed += self.ring.push(index, pcm) as u64;
    }

    /// Feed one synthetic Silero probability directly into the endpointer wiring —
    /// the exact `handle_audio` inner step, minus the model call. Delegates to
    /// [`drive_probability`](Self::drive_probability), so the step it exercises is
    /// production's by construction, not by copy. The receipt stamp is synthesized
    /// from the chunk's own index by [`rx_at`], the same rule the synthetic feed
    /// helpers use, so stamps stay consistent across a mixed test.
    #[cfg(test)]
    fn drive_probability_for_test(
        &mut self,
        pod: &PodId,
        p: f32,
        chunk_end_sample: u64,
    ) -> Vec<ListenerEvent> {
        let mut events = Vec::new();
        self.drive_probability(
            pod,
            p,
            chunk_end_sample,
            rx_at(chunk_end_sample),
            &mut events,
        );
        events
    }
}

/// The synthetic device/host clocks the tests feed. A pod boots at device time 0
/// and captures at the spine rate, the host clock sits at [`HOST_EPOCH_US`], and
/// every frame takes exactly [`TRANSPORT_US`] to arrive — so a projection off the
/// synthetic stamps is exactly `HOST_EPOCH_US + TRANSPORT_US + device_ts`, and the
/// timing assertions can be exact rather than approximate.
#[cfg(test)]
const HOST_EPOCH_US: u64 = 1_700_000_000_000_000;
#[cfg(test)]
const TRANSPORT_US: u64 = 5_000;

/// Device capture time of the sample at `index`.
#[cfg(test)]
fn dev_at(index: u64) -> DeviceMicros {
    DeviceMicros(samples_to_micros(index, SPINE_FORMAT.sample_rate_hz))
}

/// Host receipt of a frame or chunk *ending* at `index`: a frame cannot be sent
/// before its last sample is captured, so this is that capture instant plus the
/// transport delay.
#[cfg(test)]
fn rx_at(index: u64) -> HostMicros {
    HostMicros(HOST_EPOCH_US + samples_to_micros(index, SPINE_FORMAT.sample_rate_hz) + TRANSPORT_US)
}

/// Shared, atomically-updated listener counters, for `stage_health` reporting.
/// One `feeds` bump per received item, plus event tallies and the channel-full
/// drop count.
#[derive(Debug, Default)]
pub struct ListenerStats {
    feeds: AtomicU64,
    dropped: AtomicU64,
    /// Feeds dropped because the channel was *closed* — the listener thread
    /// exited (panic or shutdown). Counted apart from `dropped` (a full channel
    /// under load) so a dead listener is distinguishable from ordinary overflow.
    channel_closed: AtomicU64,
    /// Markers abandoned after waiting `MARKER_SEND_TIMEOUT` for channel room —
    /// the listener is wedged, not merely loaded. Counted apart from `dropped`
    /// (audio overflow, expected under load) because a non-zero value here means
    /// listener state has been corrupted, not just thinned.
    marker_send_timeouts: AtomicU64,
    wakes: AtomicU64,
    utterances: AtomicU64,
    superseded: AtomicU64,
    closed: AtomicU64,
    /// Duplicate samples the ring trimmed from overlapping pushes. Segment preroll
    /// re-sends already-retained audio, so this climbs at a steady, explainable
    /// rate; a spike means a device re-sending a different range under the same
    /// indexes, or a host index-math regression — the ring dedupes either way, so
    /// this counter is the only tripwire.
    overlap_trimmed_samples: AtomicU64,
    errors: AtomicU64,
    /// The most recent inference error's message. The `WakeError` carries precise
    /// provenance (`mel`/`embedding`/`silero stateN`/… + the `ort` reason); kept
    /// here so an endpointer that goes quietly dead in production surfaces *what*
    /// failed through the same handle `stage_health` reads, not just a count.
    last_error: Mutex<Option<String>>,
}

/// A `Copy` snapshot of [`ListenerStats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ListenerStatsSnapshot {
    pub feeds: u64,
    pub dropped: u64,
    pub channel_closed: u64,
    pub marker_send_timeouts: u64,
    pub wakes: u64,
    pub utterances: u64,
    pub superseded: u64,
    pub closed: u64,
    pub overlap_trimmed_samples: u64,
    pub errors: u64,
}

impl ListenerStats {
    fn record_events(&self, events: &[ListenerEvent]) {
        for ev in events {
            match ev {
                ListenerEvent::WakeDetected { .. } => {
                    self.wakes.fetch_add(1, Ordering::Relaxed);
                }
                ListenerEvent::SoftEndpoint { .. } => {
                    self.utterances.fetch_add(1, Ordering::Relaxed);
                }
                ListenerEvent::Superseded { .. } => {
                    self.superseded.fetch_add(1, Ordering::Relaxed);
                }
                ListenerEvent::UtteranceClosed { .. } => {
                    self.closed.fetch_add(1, Ordering::Relaxed);
                }
                // The "wake, no follow" accounting is counted downstream at the
                // pipeline (`BrainStats::wake_command_absent`); no listener counter.
                ListenerEvent::ArmExpired { .. } => {}
                // A barge is counted where its consequences are: the flush lands on
                // `PlaybackStats::jobs_flushed`, and the speech that caused it is
                // counted by the `SoftEndpoint` it carves like any other utterance.
                ListenerEvent::BargeIn { .. } => {}
                // Pure observability, no stage-health counter.
                ListenerEvent::EndpointerTransition { .. } | ListenerEvent::ModelStats { .. } => {}
            }
        }
    }

    /// Drain `state`'s trimmed-duplicate count into the shared counter. Called for
    /// every handled feed *regardless of outcome* — the ring push precedes
    /// inference, so a scoring error must not lose the accounting. Zero for every
    /// push but the handful at a segment boundary, so the shared atomic stays off
    /// the per-chunk hot path.
    fn accumulate_overlap(&self, state: &mut ListenerState) {
        let trimmed = state.take_overlap_trimmed();
        if trimmed != 0 {
            self.overlap_trimmed_samples
                .fetch_add(trimmed, Ordering::Relaxed);
        }
    }

    /// Count an inference error and retain its message. Called on the listener
    /// thread's error path so the diagnostic the `WakeError` computed is not
    /// destroyed at the point of capture.
    fn record_error(&self, err: &WakeError) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut slot) = self.last_error.lock() {
            *slot = Some(err.to_string());
        }
    }

    /// The most recent inference error's message, if any — the diagnostic behind
    /// the `errors` counter, for `stage_health`/operator surfacing.
    pub fn last_error(&self) -> Option<String> {
        self.last_error.lock().ok().and_then(|slot| slot.clone())
    }

    /// A `Copy` snapshot of the counters.
    pub fn snapshot(&self) -> ListenerStatsSnapshot {
        ListenerStatsSnapshot {
            feeds: self.feeds.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            channel_closed: self.channel_closed.load(Ordering::Relaxed),
            marker_send_timeouts: self.marker_send_timeouts.load(Ordering::Relaxed),
            wakes: self.wakes.load(Ordering::Relaxed),
            utterances: self.utterances.load(Ordering::Relaxed),
            superseded: self.superseded.load(Ordering::Relaxed),
            closed: self.closed.load(Ordering::Relaxed),
            overlap_trimmed_samples: self.overlap_trimmed_samples.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}

/// Spawns and owns the shared listener thread.
pub struct Listener;

impl Listener {
    /// Start the listener thread owning the shared models, returning a handle to
    /// feed pods. Emits [`ListenerEvent`]s on `events`. A thread-spawn failure is
    /// returned so the caller reports it through the daemon's fatal channel rather
    /// than panicking.
    pub fn spawn(
        mut oww_models: OwwModels,
        mut silero_model: SileroModel,
        config: ListenerConfig,
        events: mpsc::UnboundedSender<ListenerEvent>,
    ) -> std::io::Result<ListenerHandle> {
        let (feed_tx, mut feed_rx) = mpsc::channel::<(PodId, Feed)>(FEED_CHANNEL_DEPTH);
        let stats = Arc::new(ListenerStats::default());
        let thread_stats = Arc::clone(&stats);
        let thread = thread::Builder::new()
            .name("listener".to_string())
            .spawn(move || {
                let mut states: HashMap<PodId, ListenerState> = HashMap::new();
                while let Some((pod, feed)) = feed_rx.blocking_recv() {
                    thread_stats.feeds.fetch_add(1, Ordering::Relaxed);
                    let state = states
                        .entry(pod.clone())
                        .or_insert_with(|| ListenerState::new(config));
                    let handled = state.handle(&pod, feed, &mut oww_models, &mut silero_model);
                    thread_stats.accumulate_overlap(state);
                    match handled {
                        Ok(evs) => {
                            thread_stats.record_events(&evs);
                            for ev in evs {
                                // The pipeline may be gone (shutdown); a lost event
                                // is not the listener's concern.
                                if events.send(ev).is_err() {
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            thread_stats.record_error(&e);
                            // The mid-chunk failure may have torn this pod's
                            // streaming cursors; re-anchor on the next chunk.
                            state.note_inference_error();
                        }
                    }
                }
            })?;
        Ok(ListenerHandle {
            sender: FeedSender { tx: feed_tx, stats },
            thread,
        })
    }
}

/// A permit for one reserved slot in the feed channel. Sending through it is
/// synchronous and cannot block, so a caller may hold a `std::sync::Mutex` across
/// the send. Dropping the permit unused releases the slot.
pub struct FeedPermit {
    permit: mpsc::OwnedPermit<(PodId, Feed)>,
}

impl FeedPermit {
    /// Place the item in the reserved slot. Never blocks.
    pub fn send(self, pod: PodId, feed: Feed) {
        self.permit.send((pod, feed));
    }
}

/// A cloneable sender into the listener's feed channel, independent of the
/// [`ListenerHandle`] that owns the thread. Holding one across an await delays
/// channel close — and hence thread exit at shutdown — by at most
/// `MARKER_SEND_TIMEOUT`, whereas holding the handle itself would block the join
/// outright.
#[derive(Clone)]
pub struct FeedSender {
    tx: mpsc::Sender<(PodId, Feed)>,
    stats: Arc<ListenerStats>,
}

impl FeedSender {
    /// A sender over `tx` with its own private counters and no listener thread
    /// behind it — for harnesses that must exercise the real reserve/permit and
    /// drop-vs-wait semantics without paying for ONNX inference.
    pub fn detached_for_tests(tx: mpsc::Sender<(PodId, Feed)>) -> FeedSender {
        FeedSender {
            tx,
            stats: Arc::new(ListenerStats::default()),
        }
    }

    /// Forward one [`Feed`] for `pod`, with delivery semantics split by variant.
    ///
    /// `Audio` is lossy and non-blocking: a full channel drops the chunk and
    /// counts it (`dropped`), since a gap self-heals via the discontinuity check
    /// and audio priority belongs to recording, a separate path.
    ///
    /// Every other variant is a control marker whose loss corrupts listener state
    /// (a stale epoch, a missed segment re-anchor or fallback carve, a stale
    /// barge-in floor), so it is delivered reliably: the send awaits channel room
    /// for up to `MARKER_SEND_TIMEOUT`, then bumps `marker_send_timeouts` and
    /// drops the marker rather than stalling the caller forever. Markers travel
    /// the same channel as audio, so their order relative to surrounding audio
    /// from the same producer is preserved.
    ///
    /// A closed channel (the thread exited) is counted separately
    /// (`channel_closed`) so a dead listener is distinguishable from load drops;
    /// the caller keeps running in every case.
    pub async fn feed(&self, pod: PodId, feed: Feed) {
        if matches!(feed, Feed::Audio { .. }) {
            match self.tx.try_send((pod, feed)) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    self.stats.dropped.fetch_add(1, Ordering::Relaxed);
                }
                // A closed channel means the listener thread has exited (panic or
                // shutdown) — count it apart from load drops so a dead listener is
                // visible as itself, not laundered into the overflow counter.
                Err(TrySendError::Closed(_)) => {
                    self.stats.channel_closed.fetch_add(1, Ordering::Relaxed);
                }
            }
            return;
        }
        if let Some(permit) = self.reserve_marker().await {
            permit.send(pod, feed);
        }
    }

    /// Reserve one channel slot for a marker, waiting up to `MARKER_SEND_TIMEOUT`.
    /// `None` means the slot could not be had — a wedged consumer
    /// (`marker_send_timeouts`) or an exited thread (`channel_closed`) — and the
    /// caller should abandon the marker.
    pub async fn reserve_marker(&self) -> Option<FeedPermit> {
        match tokio::time::timeout(MARKER_SEND_TIMEOUT, self.tx.clone().reserve_owned()).await {
            Ok(Ok(permit)) => Some(FeedPermit { permit }),
            Ok(Err(_)) => {
                self.stats.channel_closed.fetch_add(1, Ordering::Relaxed);
                None
            }
            Err(_) => {
                self.stats
                    .marker_send_timeouts
                    .fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }
}

/// Handle to the listener thread: feeds pods and, at shutdown, joins the thread.
pub struct ListenerHandle {
    sender: FeedSender,
    thread: JoinHandle<()>,
}

impl ListenerHandle {
    /// Forward one [`Feed`] for `pod`. See [`FeedSender::feed`] for the delivery
    /// guarantee, which differs by variant.
    pub async fn feed(&self, pod: PodId, feed: Feed) {
        self.sender.feed(pod, feed).await;
    }

    /// A cloneable sender for callers that must not hold the handle itself across
    /// an await (shutdown joins the thread through sole ownership of the handle).
    pub fn feed_sender(&self) -> FeedSender {
        self.sender.clone()
    }

    /// A shared handle to the live counters, for `stage_health` reporting.
    pub fn stats_shared(&self) -> Arc<ListenerStats> {
        Arc::clone(&self.sender.stats)
    }

    /// Close the feed channel and join the thread, surfacing a panic as `Err`.
    pub fn join(self) -> thread::Result<()> {
        let ListenerHandle { sender, thread } = self;
        drop(sender);
        thread.join()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::listener::endpointer::TransitionCause;
    use crate::listener::stats::ScoreSummary;
    use crate::test_support::{oww_models, silero_model, wake_phrase_pcm};

    fn pod() -> PodId {
        PodId("pod-x".into())
    }

    fn test_config(policy: WakePolicy) -> ListenerConfig {
        ListenerConfig {
            default_policy: policy,
            ..ListenerConfig::default()
        }
    }

    /// Compact endpointer windows for the synthetic-`P` tests: onset 2 chunks, soft
    /// hangover 3, continuation 4, 100-sample preroll.
    fn synth_config(policy: WakePolicy) -> ListenerConfig {
        ListenerConfig {
            endpointer: EndpointerConfig {
                onset_chunks: 2,
                soft_hangover_chunks: 3,
                continuation_chunks: 4,
                preroll_pad_samples: 100,
                max_utterance_samples: 20 * 512,
                ..EndpointerConfig::default()
            },
            default_policy: policy,
            ..ListenerConfig::default()
        }
    }

    /// Drive `count` synthetic Silero chunks of probability `p`, advancing the
    /// shared chunk-end cursor by 512 each.
    fn drive(
        state: &mut ListenerState,
        p: f32,
        count: usize,
        cursor: &mut u64,
    ) -> Vec<ListenerEvent> {
        let mut evs = Vec::new();
        for _ in 0..count {
            *cursor += 512;
            evs.extend(state.drive_probability_for_test(&pod(), p, *cursor));
        }
        evs
    }

    /// The `(cause, sample_offset, epoch)` of every transition in `events`, in
    /// emission order — the join `drain_transitions` is responsible for.
    fn transitions(events: &[ListenerEvent]) -> Vec<(TransitionCause, u64, u64)> {
        events
            .iter()
            .filter_map(|e| match e {
                ListenerEvent::EndpointerTransition {
                    epoch, transition, ..
                } => Some((transition.cause, transition.sample_offset, *epoch)),
                _ => None,
            })
            .collect()
    }

    /// The `(model, cause, summary)` of every `ModelStats` in `events`, in
    /// emission order.
    fn model_stats(events: &[ListenerEvent]) -> Vec<(StatsModel, StatsFlushCause, ScoreSummary)> {
        events
            .iter()
            .filter_map(|e| match e {
                ListenerEvent::ModelStats {
                    model,
                    cause,
                    summary,
                    ..
                } => Some((*model, *cause, *summary)),
                _ => None,
            })
            .collect()
    }

    /// [`feed_audio_chunkwise`], keeping each frame's own events: `(frame end
    /// index, events that frame produced)`. Which frame emitted an event is what
    /// makes "the receipt of the frame that caused this stage" assertable without
    /// restating the models' internal lag as arithmetic.
    fn feed_audio_by_frame(
        state: &mut ListenerState,
        index: u64,
        pcm: &[i16],
        oww: &mut OwwModels,
        silero: &mut SileroModel,
    ) -> Vec<(u64, Vec<ListenerEvent>)> {
        pcm.chunks(512)
            .enumerate()
            .map(|(i, chunk)| {
                let start = index + (i * 512) as u64;
                let evs = feed_audio(state, start, chunk, oww, silero);
                (start + chunk.len() as u64, evs)
            })
            .collect()
    }

    /// The end index of the first frame whose handling emitted a matching event.
    fn frame_emitting(
        per_frame: &[(u64, Vec<ListenerEvent>)],
        pred: impl Fn(&ListenerEvent) -> bool,
    ) -> u64 {
        per_frame
            .iter()
            .find(|(_, evs)| evs.iter().any(&pred))
            .map(|(end, _)| *end)
            .expect("no frame emitted a matching event")
    }

    fn soft_endpoints(events: &[ListenerEvent]) -> Vec<&CarvedUtterance> {
        events
            .iter()
            .filter_map(|e| match e {
                ListenerEvent::SoftEndpoint { utterance, .. } => Some(utterance),
                _ => None,
            })
            .collect()
    }

    /// An `Audio` frame stamped off the synthetic clocks ([`dev_at`]/[`rx_at`]):
    /// captured at `index`, received once its last sample was captured plus the
    /// transport delay.
    fn audio_feed(index: u64, pcm: Vec<i16>) -> Feed {
        let len = pcm.len() as u64;
        Feed::Audio {
            first_sample_index: index,
            gap: None,
            pcm: Arc::from(pcm),
            device_ts: dev_at(index),
            host_rx: rx_at(index + len),
        }
    }

    /// A `SegmentOpened` on the synthetic clocks: the device VAD went high
    /// `preroll` samples past `base`.
    fn opened_feed(base: u64, preroll: u32) -> Feed {
        Feed::SegmentOpened {
            base_sample_index: base,
            preroll_samples: preroll,
            base_device_ts: dev_at(base),
        }
    }

    /// A `SegmentClosed` on the synthetic clocks, stamped as received at `index`.
    fn closed_feed(index: u64) -> Feed {
        Feed::SegmentClosed {
            end: crate::types::SegmentEndCause::VadRelease,
            host_rx: rx_at(index),
        }
    }

    /// Feed a whole PCM buffer as one `Audio` frame starting at `index`, stamped
    /// off the synthetic clocks ([`dev_at`]/[`rx_at`]) so every test's stamps are
    /// consistent and exactly predictable.
    fn feed_audio(
        state: &mut ListenerState,
        index: u64,
        pcm: &[i16],
        oww: &mut OwwModels,
        silero: &mut SileroModel,
    ) -> Vec<ListenerEvent> {
        state
            .handle(&pod(), audio_feed(index, pcm.to_vec()), oww, silero)
            .expect("audio feed")
    }

    /// Feed `pcm` as the live path delivers it: 512-sample frames from `index`,
    /// interleaving OWW and Silero steps the way the wire does. One whole-buffer
    /// `feed_audio` instead runs every OWW step before any Silero step, which
    /// scrambles the arm timeline against the endpointer — so any test whose
    /// subject is the two models' relative timing must use this.
    fn feed_audio_chunkwise(
        state: &mut ListenerState,
        index: u64,
        pcm: &[i16],
        oww: &mut OwwModels,
        silero: &mut SileroModel,
    ) -> Vec<ListenerEvent> {
        let mut events = Vec::new();
        for (i, chunk) in pcm.chunks(512).enumerate() {
            events.extend(feed_audio(
                state,
                index + (i * 512) as u64,
                chunk,
                oww,
                silero,
            ));
        }
        events
    }

    /// Samples at the head of `pcm` below audible level. Digital-silence padding
    /// is non-speech to Silero, so this is a *lower* bound on the endpointer's
    /// view of the run's leading gap.
    fn leading_quiet(pcm: &[i16]) -> u64 {
        pcm.iter().take_while(|s| s.abs() <= 64).count() as u64
    }

    /// [`leading_quiet`] from the tail.
    fn trailing_quiet(pcm: &[i16]) -> u64 {
        pcm.iter().rev().take_while(|s| s.abs() <= 64).count() as u64
    }

    fn open(state: &mut ListenerState, base: u64, oww: &mut OwwModels, silero: &mut SileroModel) {
        open_with_preroll(state, base, 0, oww, silero);
    }

    /// `open`, with the device's preroll declared: the device VAD went high
    /// `preroll` samples past `base`, which is what separates a measured t0 from a
    /// projected one.
    fn open_with_preroll(
        state: &mut ListenerState,
        base: u64,
        preroll: u32,
        oww: &mut OwwModels,
        silero: &mut SileroModel,
    ) {
        state
            .handle(&pod(), Feed::Connected { epoch: 1 }, oww, silero)
            .unwrap();
        open_segment(state, base, preroll, oww, silero);
    }

    /// A `SegmentOpened` on the synthetic clocks, without the `Connected` reset.
    fn open_segment(
        state: &mut ListenerState,
        base: u64,
        preroll: u32,
        oww: &mut OwwModels,
        silero: &mut SileroModel,
    ) -> Vec<ListenerEvent> {
        state
            .handle(&pod(), opened_feed(base, preroll), oww, silero)
            .expect("segment-open feed")
    }

    /// A `SegmentClosed` on the synthetic clocks, stamped at `index`.
    fn close_segment(
        state: &mut ListenerState,
        index: u64,
        oww: &mut OwwModels,
        silero: &mut SileroModel,
    ) -> Vec<ListenerEvent> {
        state
            .handle(&pod(), closed_feed(index), oww, silero)
            .expect("close feed")
    }

    /// Silence never wakes and never opens an utterance. It is not *event*-silent:
    /// the close reports what both models scored across it, which is the whole
    /// point — a quiet room is the case the transition stream cannot describe.
    #[test]
    fn silence_is_inert() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(test_config(WakePolicy::WakeGated));
        open(&mut state, 0, &mut oww, &mut silero);
        let mut events = Vec::new();
        for i in 0..20 {
            events.extend(feed_audio(
                &mut state,
                i * 512,
                &vec![0_i16; 512],
                &mut oww,
                &mut silero,
            ));
        }
        events.extend(close_segment(&mut state, 20 * 512, &mut oww, &mut silero));
        let semantic: Vec<_> = events
            .iter()
            .filter(|e| !matches!(e, ListenerEvent::ModelStats { .. }))
            .collect();
        assert!(
            semantic.is_empty(),
            "silence must wake nothing and carve nothing: {semantic:?}"
        );
        // And the observability that makes a silent room legible rather than
        // indistinguishable from a dead listener.
        let stats = model_stats(&events);
        assert_eq!(stats.len(), 2, "both models report on the close: {stats:?}");
        for (model, cause, s) in stats {
            assert_eq!(cause, StatsFlushCause::SegmentClose, "{model:?}");
            assert!(
                s.max < 0.5,
                "{model:?} must score silence low, got max {}",
                s.max
            );
        }
    }

    /// The wake phrase arms the pod and emits a `WakeDetected` (absolute index).
    /// The phrase carries its own trailing silence, so the endpointer soft-endpoints
    /// within the same feed and the carve consumes the arm — hence the arm is gone
    /// by the end, having been spent rather than dropped.
    #[test]
    fn wake_phrase_arms_and_emits_detection() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(test_config(WakePolicy::WakeGated));
        open(&mut state, 0, &mut oww, &mut silero);
        let phrase = wake_phrase_pcm();
        let events = feed_audio_chunkwise(&mut state, 0, &phrase, &mut oww, &mut silero);
        let detections: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                ListenerEvent::WakeDetected {
                    wake_end_sample, ..
                } => Some(*wake_end_sample),
                _ => None,
            })
            .collect();
        assert!(!detections.is_empty(), "wake phrase must arm a wake");
        assert!(
            detections[0] > 0 && detections[0] <= phrase.len() as u64,
            "wake end {} within the phrase",
            detections[0]
        );
        let carved = soft_endpoints(&events);
        assert_eq!(carved.len(), 1, "the phrase carves once: {events:?}");
        assert!(
            carved[0].wake.is_some(),
            "and the arm gated that carve rather than going unused"
        );
        assert!(state.wake.is_none(), "the carve consumed the arm");
    }

    /// The natural path, end to end, on real models and real audio: no synthetic
    /// probabilities anywhere. Silence preroll (reproducing the live cold reset —
    /// `reset_stream` has just zeroed Silero's recurrent state and context before
    /// speech arrives) → the wake phrase → trailing silence past the soft hangover.
    ///
    /// This is the seam the synthetic-`P` suite structurally cannot cover: real
    /// audio → `SileroVad::push` → real P → onset. It was red for a whole cycle
    /// while the wrapper fed the model a 512-sample tensor instead of the
    /// reference's 576 (64 context + 512 chunk), which scored clear speech at
    /// P≈0.003 and left the natural path unreachable — every utterance limped in on
    /// the missed-onset device-release fallback instead.
    #[test]
    fn real_audio_drives_onset_to_soft_endpoint_carve() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let config = test_config(WakePolicy::WakeGated);
        let preroll_pad = config.endpointer.preroll_pad_samples;
        let onset_run_samples = u64::from(config.endpointer.onset_chunks) * 512;
        let mut state = ListenerState::new(config);
        open(&mut state, 0, &mut oww, &mut silero);

        // ~1 s of device preroll, then the phrase, then silence past the hangover —
        // chunk-wise, as the live feed arrives, since the wake gating this asserts
        // turns on the arm landing in the right place relative to the onset.
        let silence = vec![0_i16; 16_000];
        let phrase = wake_phrase_pcm();
        let mut audio: Vec<i16> = Vec::new();
        audio.extend_from_slice(&silence);
        let phrase_start = silence.len() as u64;
        audio.extend_from_slice(&phrase);
        audio.extend_from_slice(&silence);
        let events = feed_audio_chunkwise(&mut state, 0, &audio, &mut oww, &mut silero);

        assert!(
            events
                .iter()
                .any(|e| matches!(e, ListenerEvent::WakeDetected { .. })),
            "the phrase arms a wake: {events:?}"
        );

        let causes: Vec<_> = transitions(&events).iter().map(|t| t.0).collect();
        assert!(
            causes.contains(&TransitionCause::Onset),
            "Silero must onset on real speech: {causes:?}"
        );
        assert!(
            !causes.contains(&TransitionCause::MissedOnsetCarve),
            "the natural path carries the utterance — no fallback carve: {causes:?}"
        );

        let carved = soft_endpoints(&events);
        assert_eq!(carved.len(), 1, "one utterance carved: {events:?}");
        let u = carved[0];
        assert_eq!(u.cause, EndpointCause::SoftEndpoint);
        assert!(
            u.wake.is_some(),
            "wake-gated: the phrase gates its own carve"
        );

        // Span sanity. The `Onset` transition is stamped at the chunk that *confirms*
        // the run, but the carve anchors on the run's *first* chunk — so the start is
        // a preroll pad ahead of that anchor, not of the transition.
        let onset = transitions(&events)
            .iter()
            .find(|t| t.0 == TransitionCause::Onset)
            .expect("onset transition")
            .1;
        assert!(
            onset > phrase_start && onset < phrase_start + phrase.len() as u64,
            "onset {onset} falls inside the phrase"
        );
        assert_eq!(
            u.start_sample,
            onset - onset_run_samples - preroll_pad,
            "carve opens a preroll pad ahead of the onset run's first chunk"
        );
        assert!(
            u.end_sample > onset && u.end_sample <= phrase_start + phrase.len() as u64,
            "carve ends at the soft endpoint, within the phrase: {}..{}",
            u.start_sample,
            u.end_sample
        );
    }

    /// t0 is **measured** for the utterance that opened its segment: the device
    /// VAD went high *because of* this speech, so the utterance's audio begins
    /// inside the preroll and the segment's first-audio receipt is its own. Every
    /// other stamp is the receipt of the frame that actually caused it — not of
    /// the event the listener emitted about it.
    #[test]
    fn measured_t0_and_causal_stamps_for_a_segment_opening_utterance() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(test_config(WakePolicy::WakeGated));
        // The device's ~1 s preroll: the VAD went high one second into the stream.
        let preroll = 16_000_u32;
        open_with_preroll(&mut state, 0, preroll, &mut oww, &mut silero);

        let silence = vec![0_i16; preroll as usize];
        let phrase = wake_phrase_pcm();
        let mut audio: Vec<i16> = Vec::new();
        audio.extend_from_slice(&silence);
        audio.extend_from_slice(&phrase);
        audio.extend_from_slice(&vec![0_i16; 16_000]);
        let per_frame = feed_audio_by_frame(&mut state, 0, &audio, &mut oww, &mut silero);
        let events: Vec<ListenerEvent> = per_frame
            .iter()
            .flat_map(|(_, evs)| evs.iter().cloned())
            .collect();

        let carved = soft_endpoints(&events);
        assert_eq!(carved.len(), 1, "one utterance carved: {events:?}");
        let t = carved[0].timing;

        assert!(
            carved[0].start_sample <= u64::from(preroll),
            "the utterance's audio begins inside the preroll — it opened the segment"
        );
        assert!(!t.t0_projected, "so its t0 is measured, not projected");
        assert_eq!(
            t.first_audio_rx,
            Some(rx_at(512)),
            "t0 is when the segment's first frame arrived"
        );

        // Each stamp is the receipt of the frame that *caused* the stage — found
        // by which frame's handling emitted the corresponding event, so this
        // asserts the contract rather than restating the models' internal lag.
        assert_eq!(
            t.wake_detected_rx,
            Some(rx_at(frame_emitting(&per_frame, |e| matches!(
                e,
                ListenerEvent::WakeDetected { .. }
            )))),
            "the wake stamp is the receipt of the frame whose scoring detected it"
        );
        assert_eq!(
            t.onset_rx,
            Some(rx_at(frame_emitting(&per_frame, |e| matches!(
                e,
                ListenerEvent::EndpointerTransition { transition, .. }
                    if transition.cause == TransitionCause::Onset
            )))),
            "the onset stamp is the receipt of the frame that drove the onset"
        );
        assert_eq!(
            t.soft_endpoint_rx,
            Some(rx_at(frame_emitting(&per_frame, |e| matches!(
                e,
                ListenerEvent::SoftEndpoint { .. }
            )))),
            "the endpoint stamp is the receipt of the frame that drove the carve"
        );

        // The VAD-high estimate is the projection of the first post-preroll
        // sample's capture — under the synthetic clocks, exactly its receipt.
        assert_eq!(t.vad_high_est, Some(rx_at(u64::from(preroll))));
    }

    /// t0 is **projected** for an utterance that starts inside an already-open
    /// segment (music holding the VAD open, or a later command in one segment):
    /// its first audio was never separately received, so the host instant comes
    /// off the device clock through the offset estimate.
    #[test]
    fn projected_t0_for_an_utterance_that_starts_mid_segment() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(test_config(WakePolicy::WakeGated));
        // The VAD went high far earlier than this speech: only 1 024 samples of
        // this segment are preroll, and the phrase is a second in.
        let preroll = 1_024_u32;
        open_with_preroll(&mut state, 0, preroll, &mut oww, &mut silero);

        let phrase = wake_phrase_pcm();
        let mut audio: Vec<i16> = Vec::new();
        audio.extend_from_slice(&vec![0_i16; 16_000]);
        audio.extend_from_slice(&phrase);
        audio.extend_from_slice(&vec![0_i16; 16_000]);
        let events = feed_audio_chunkwise(&mut state, 0, &audio, &mut oww, &mut silero);

        let carved = soft_endpoints(&events);
        assert_eq!(carved.len(), 1, "one utterance carved: {events:?}");
        let u = carved[0];
        assert!(
            u.start_sample > u64::from(preroll),
            "the utterance starts past the preroll: the VAD was already open"
        );
        assert!(u.timing.t0_projected, "so its t0 is projected");
        // The synthetic clocks put every frame's receipt exactly one transport
        // delay past its last sample's capture, so a correct projection of the
        // utterance's first sample lands exactly on that sample's `rx_at`.
        assert_eq!(
            u.timing.first_audio_rx,
            Some(rx_at(u.start_sample)),
            "t0 projects the utterance's own first sample onto the host clock"
        );
        assert_eq!(u.timing.vad_high_est, Some(rx_at(u64::from(preroll))));
        assert!(
            u.timing.vad_high_est < u.timing.first_audio_rx,
            "the VAD went high well before this utterance's speech did"
        );
    }

    /// A continuation re-carves the whole utterance under the same id, so it keeps
    /// the original t0, wake and onset — only the endpoint moves.
    #[test]
    fn continuation_keeps_the_utterances_stamps_and_moves_its_endpoint() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(synth_config(WakePolicy::WakeGated));
        open(&mut state, 0, &mut oww, &mut silero);
        state.push_ring_for_test(0, &vec![5_i16; 8_192]);
        state.arm_wake_for_test(0.8, 900);

        let mut cursor = 0u64;
        let mut events = drive(&mut state, 0.9, 2, &mut cursor); // onset at 1024
        events.extend(drive(&mut state, 0.1, 3, &mut cursor)); // soft endpoint at 2560
        events.extend(drive(&mut state, 0.9, 2, &mut cursor)); // resumes: continuation
        events.extend(drive(&mut state, 0.1, 3, &mut cursor)); // soft endpoint at 5120

        let carved = soft_endpoints(&events);
        assert_eq!(carved.len(), 2, "carved once, then re-carved: {events:?}");
        let (first, again) = (carved[0].timing, carved[1].timing);
        assert_eq!(
            carved[0].utterance_id, carved[1].utterance_id,
            "one utterance, one id"
        );

        assert_eq!(first.wake_detected_rx, Some(rx_at(900)));
        assert_eq!(first.onset_rx, Some(rx_at(1_024)));
        assert_eq!(first.soft_endpoint_rx, Some(rx_at(2_560)));
        assert_eq!(
            (again.wake_detected_rx, again.onset_rx),
            (first.wake_detected_rx, first.onset_rx),
            "the continuation kept the utterance's wake and onset"
        );
        assert_eq!(
            again.soft_endpoint_rx,
            Some(rx_at(5_120)),
            "and moved only the endpoint, to the chunk that drove the re-carve"
        );
        assert_eq!(
            (again.first_audio_rx, again.t0_projected, again.vad_high_est),
            (first.first_audio_rx, first.t0_projected, first.vad_high_est),
            "and kept the axis origin the endpoint is measured from"
        );
    }

    /// The same persistence, on the branch that can actually break it: a
    /// **projected** t0 reads the segment's offset estimate, whose min filter keeps
    /// narrowing as frames arrive. Re-deriving it per carve would move one
    /// utterance's axis origin between its own `utterance` lines, so it is frozen
    /// at the first carve instead.
    #[test]
    fn projected_t0_freezes_at_the_first_carve_though_the_offset_narrows() {
        // Every frame arrives late, by an amount that shrinks with each one, so the
        // offset's min filter narrows monotonically across the whole feed — and so
        // across the gap between the two carves, wherever they land. That makes the
        // drift observable without pinning the carves' frame positions.
        const LATE_US: u64 = 200_000;
        const NARROW_PER_FRAME_US: u64 = 1_000;

        let mut oww = oww_models();
        let mut silero = silero_model();
        let config = test_config(WakePolicy::WakeGated);
        let hangover = u64::from(config.endpointer.soft_hangover_chunks) * 512;
        let continuation = u64::from(config.endpointer.continuation_chunks) * 512;
        let phrase = wake_phrase_pcm();
        // The pause sandwich of `real_audio_pause_supersedes_and_recarves_one_utterance`:
        // long enough to endpoint, short enough to continue the same utterance.
        let fixture_silence = trailing_quiet(&phrase) + leading_quiet(&phrase);
        let pause = (hangover + continuation / 2).saturating_sub(fixture_silence);
        let tail = hangover + continuation + 512;
        // Leading silence puts the speech well past the preroll, which is what makes
        // t0 projected rather than measured.
        let lead = 16_000_u64;
        let preroll = 1_024_u32;

        let mut audio: Vec<i16> = vec![0_i16; lead as usize];
        audio.extend_from_slice(&phrase);
        audio.extend(std::iter::repeat_n(0_i16, pause as usize));
        audio.extend_from_slice(&phrase);
        audio.extend(std::iter::repeat_n(0_i16, tail as usize));

        let frames = audio.len().div_ceil(512) as u64;
        assert!(
            frames * NARROW_PER_FRAME_US < LATE_US,
            "the delay must still be shrinking at the last frame: {frames} frames"
        );

        let mut state = ListenerState::new(config);
        open_with_preroll(&mut state, 0, preroll, &mut oww, &mut silero);
        let mut events = Vec::new();
        for (i, chunk) in audio.chunks(512).enumerate() {
            let index = (i * 512) as u64;
            let late = LATE_US - (i as u64) * NARROW_PER_FRAME_US;
            events.extend(
                state
                    .handle(
                        &pod(),
                        Feed::Audio {
                            first_sample_index: index,
                            gap: None,
                            pcm: Arc::from(chunk.to_vec()),
                            device_ts: dev_at(index),
                            host_rx: HostMicros(rx_at(index + chunk.len() as u64).0 + late),
                        },
                        &mut oww,
                        &mut silero,
                    )
                    .expect("audio feed"),
            );
        }

        let carved = soft_endpoints(&events);
        assert_eq!(carved.len(), 2, "carved once, then re-carved: {events:?}");
        assert_eq!(
            carved[0].utterance_id, carved[1].utterance_id,
            "one utterance, one id"
        );
        let (first, again) = (carved[0].timing, carved[1].timing);
        assert!(first.t0_projected, "the speech starts past the preroll");
        assert!(
            first.first_audio_rx.is_some(),
            "a projected t0 still names an instant"
        );
        assert_eq!(
            (again.first_audio_rx, again.vad_high_est),
            (first.first_audio_rx, first.vad_high_est),
            "the continuation kept the utterance's projected origin"
        );

        // The freeze is load-bearing rather than incidental: re-deriving t0 now, as
        // a per-carve derivation would, lands somewhere else.
        let (live_t0, _) = state.t0_for(carved[1].start_sample);
        assert_ne!(
            live_t0, first.first_audio_rx,
            "the offset narrowed across the utterance, so a re-derived t0 moves"
        );
    }

    /// A reconnect drops the segment's timing anchors, so a carve on the new
    /// connection reports no origin rather than one off the old connection's clock.
    ///
    /// The pod reboots into a fresh device clock and a fresh sample-index domain.
    /// Keeping the old `SegmentOpen` would let a carve project t0 and `vad_high_est`
    /// off the previous connection's `base_device_ts` and offset — a confidently
    /// numeric axis wrong by the pod's whole uptime, where `null` is the honest
    /// answer.
    #[test]
    fn a_reconnect_clears_the_segments_timing_anchors() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(test_config(WakePolicy::WakeGated));
        // A segment whose anchors are fully established: a declared preroll, a
        // first-audio receipt, and post-preroll frames feeding the offset estimate.
        open_with_preroll(&mut state, 0, 512, &mut oww, &mut silero);
        let mut idx = 0u64;
        for _ in 0..4 {
            feed_audio(&mut state, idx, &vec![7_i16; 512], &mut oww, &mut silero);
            idx += 512;
        }
        assert!(
            state
                .segment
                .as_ref()
                .and_then(SegmentOpen::vad_high_est)
                .is_some(),
            "the fixture must establish the anchors it then expects to be cleared"
        );

        // The pod reconnects, and no new segment opens before the next carve.
        state
            .handle(&pod(), Feed::Connected { epoch: 2 }, &mut oww, &mut silero)
            .expect("connected feed");
        assert!(state.segment.is_none(), "the reconnect dropped the anchors");

        // Drive a carve anyway: an armed wake with the endpointer idle at close is
        // the missed-onset fallback, which carves without needing an onset.
        state.arm_wake_for_test(0.9, 1_600);
        let events = close_segment(&mut state, idx, &mut oww, &mut silero);
        let carved = soft_endpoints(&events);
        assert_eq!(carved.len(), 1, "the fallback carves once: {events:?}");
        let t = carved[0].timing;
        assert_eq!(
            t.first_audio_rx, None,
            "no segment, no receipt to name as t0"
        );
        assert!(
            !t.t0_projected,
            "an absent t0 is not a projected one — nothing was projected"
        );
        assert_eq!(
            t.vad_high_est, None,
            "and nothing to project the VAD-high estimate from"
        );
    }

    /// An utterance's onset and wake receipts do not outlive it. Once one utterance
    /// closes, a later carve that never onset must report `onset_rx: None` — the
    /// `?` the console renders — rather than the previous utterance's onset receipt,
    /// which would read as a plausible measurement of a thing that never happened.
    #[test]
    fn a_closed_utterances_stamps_do_not_leak_into_the_next_carve() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(synth_config(WakePolicy::WakeGated));
        open(&mut state, 0, &mut oww, &mut silero);
        state.push_ring_for_test(0, &vec![5_i16; 16_384]);

        // Utterance 1: a full onset → soft endpoint → close cycle, so it both
        // stamps an onset receipt and then ends.
        state.arm_wake_for_test(0.8, 900);
        let mut cursor = 0u64;
        let mut events = drive(&mut state, 0.9, 2, &mut cursor); // onset at 1024
        events.extend(drive(&mut state, 0.1, 3, &mut cursor)); // soft endpoint
        let first = soft_endpoints(&events);
        assert_eq!(first.len(), 1, "utterance 1 carved: {events:?}");
        assert_eq!(
            first[0].timing.onset_rx,
            Some(rx_at(1_024)),
            "utterance 1 onset: the stamp that must not be reused"
        );
        let first_id = first[0].utterance_id.clone();
        // Past the continuation window: utterance 1 is closed and gone.
        let closed = drive(&mut state, 0.1, 5, &mut cursor);
        assert!(
            closed
                .iter()
                .any(|e| matches!(e, ListenerEvent::UtteranceClosed { .. })),
            "utterance 1 must close for this test to mean anything: {closed:?}"
        );

        // Utterance 2: a fresh wake, and a close with the endpointer idle — the
        // missed-onset fallback, which never onsets.
        let wake_end = cursor - 512;
        state.arm_wake_for_test(0.7, wake_end);
        // The synthetic driver bypasses `handle_audio`, which is what tracks the
        // stream position a close is stamped at; stand in for it.
        state.expected_next = Some(cursor);
        let events = state.handle_close(&pod(), rx_at(cursor)).expect("close");
        let second = soft_endpoints(&events);
        assert_eq!(second.len(), 1, "the fallback carves once: {events:?}");
        assert_ne!(
            second[0].utterance_id, first_id,
            "a new utterance, not a continuation of the closed one"
        );
        assert_eq!(
            second[0].timing.onset_rx, None,
            "utterance 2 never onset, so it reports no onset receipt"
        );
        assert_eq!(
            second[0].timing.wake_detected_rx,
            Some(rx_at(wake_end)),
            "and reports its own wake's receipt, not the closed utterance's"
        );
    }

    /// The clock-offset estimate ignores preroll frames. The device drains a
    /// segment's preroll backlog at 4× real time, so those frames arrive far
    /// earlier relative to their capture than any live frame can — folding them in
    /// would drag the estimate below the real minimum transport delay and make
    /// every projection early.
    #[test]
    fn preroll_backlog_does_not_bias_the_clock_offset() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(test_config(WakePolicy::WakeGated));
        let preroll = 1_024_u32;
        open_with_preroll(&mut state, 0, preroll, &mut oww, &mut silero);

        // Two preroll frames, arriving 30 ms "ahead" of a live frame's schedule.
        for i in 0..2u64 {
            let index = i * 512;
            state
                .handle(
                    &pod(),
                    Feed::Audio {
                        first_sample_index: index,
                        gap: None,
                        pcm: Arc::from(vec![0_i16; 512]),
                        device_ts: dev_at(index),
                        host_rx: HostMicros(rx_at(index + 512).0 - 30_000),
                    },
                    &mut oww,
                    &mut silero,
                )
                .expect("preroll feed");
        }
        // Live post-preroll frames on the synthetic schedule.
        for i in 2..8u64 {
            feed_audio(
                &mut state,
                i * 512,
                &vec![0_i16; 512],
                &mut oww,
                &mut silero,
            );
        }

        let seg = state.segment.expect("segment open");
        assert_eq!(
            seg.vad_high_est(),
            Some(rx_at(u64::from(preroll))),
            "the estimate reflects the live frames only"
        );
        assert_eq!(
            seg.first_audio_rx,
            Some(HostMicros(rx_at(512).0 - 30_000)),
            "t0 is still a plain measurement — when the first frame actually arrived"
        );
    }

    /// The continuation contract on real models and real audio: speech, a pause
    /// longer than the soft hangover but inside the continuation window, then more
    /// speech. The first soft endpoint carves, the resume supersedes it, and the
    /// re-carve **reuses the utterance id** while spanning the whole utterance from
    /// its original start — one utterance, one id, re-STT'd whole rather than
    /// stitched from fragments.
    ///
    /// Synthesizable only since the Silero context fix: before it, Silero never
    /// onset on this fixture and the pause corpus was assumed to need a real-room
    /// recording. The real captures are still wanted for tuning against genuine
    /// speech rhythms; this pins the state-machine contract meanwhile.
    #[test]
    fn real_audio_pause_supersedes_and_recarves_one_utterance() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let config = test_config(WakePolicy::WakeGated);
        let hangover = u64::from(config.endpointer.soft_hangover_chunks) * 512;
        let continuation = u64::from(config.endpointer.continuation_chunks) * 512;
        let phrase = wake_phrase_pcm();
        // The whole test hangs on a sandwich: a gap between the two speech runs
        // that outlives the soft hangover (so the first run endpoints) but stays
        // inside the continuation window (so the resume continues the same
        // utterance). The gap is *not* just the inserted pause — the fixture's own
        // trailing silence starts the endpointer's clock before the pause begins,
        // and the second copy's leading silence extends it after. Budget for all
        // three, and aim the total at the middle of the window.
        let fixture_silence = trailing_quiet(&phrase) + leading_quiet(&phrase);
        let pause = (hangover + continuation / 2).saturating_sub(fixture_silence);
        let gap = fixture_silence + pause;
        // The defaults are tuned values and will move; fail here, at the cause,
        // rather than in the assertions.
        assert!(
            gap > hangover,
            "sandwich needs a gap that endpoints: gap {gap} <= hangover {hangover}"
        );
        assert!(
            gap - hangover < continuation,
            "sandwich needs the resume inside the continuation window: gap {gap} - \
             hangover {hangover} >= continuation {continuation} (fixture contributes \
             {fixture_silence} of the gap)"
        );
        // Trailing silence past hangover + window, so the utterance closes.
        let tail = hangover + continuation + 512;
        let mut state = ListenerState::new(config);
        open(&mut state, 0, &mut oww, &mut silero);

        // phrase | pause | phrase | trailing silence past the continuation window.
        let mut audio: Vec<i16> = Vec::new();
        audio.extend_from_slice(&phrase);
        audio.extend(std::iter::repeat_n(0_i16, pause as usize));
        audio.extend_from_slice(&phrase);
        audio.extend(std::iter::repeat_n(0_i16, tail as usize));

        let events = feed_audio_chunkwise(&mut state, 0, &audio, &mut oww, &mut silero);

        let causes: Vec<_> = transitions(&events).iter().map(|t| t.0).collect();
        assert!(
            causes.contains(&TransitionCause::Continuation),
            "the pause is a continuation, not a new utterance: {causes:?}"
        );

        let carved = soft_endpoints(&events);
        assert_eq!(carved.len(), 2, "carve, resume, re-carve: {events:?}");
        assert_eq!(
            carved[0].utterance_id, carved[1].utterance_id,
            "the continuation reuses the id — one utterance, one id"
        );
        assert!(
            events.iter().any(
                |e| matches!(e, ListenerEvent::Superseded { utterance_id, .. }
                    if *utterance_id == carved[0].utterance_id)
            ),
            "the resume supersedes the first carve's in-flight STT: {events:?}"
        );
        assert_eq!(
            carved[1].start_sample, carved[0].start_sample,
            "the re-carve spans from the original start, not the resume"
        );
        assert!(
            carved[1].end_sample > carved[0].end_sample,
            "and runs past the first endpoint: {}..{} then {}..{}",
            carved[0].start_sample,
            carved[0].end_sample,
            carved[1].start_sample,
            carved[1].end_sample
        );
        assert!(
            carved[1].pcm.len() > carved[0].pcm.len(),
            "the re-carve carries the whole concatenated utterance for re-STT"
        );
    }

    /// Wake-gated onset→soft-endpoint (synthetic P): an armed wake in the arm
    /// window admits the utterance, mints id seq 1, attaches the wake (PCM-relative
    /// offsets), carves real PCM from the ring, and consumes the arm.
    #[test]
    fn wakegated_onset_endpoint_carves_and_consumes_arm() {
        let mut state = ListenerState::new(synth_config(WakePolicy::WakeGated));
        // Ring holds audio over the whole span the carve will slice.
        state.push_ring_for_test(0, &(0..4096).map(|i| (i % 97) as i16).collect::<Vec<_>>());
        state.arm_wake_for_test(0.9, 500);
        let mut cursor = 0u64;
        let mut events = drive(&mut state, 0.9, 2, &mut cursor); // onset → Speech
        events.extend(drive(&mut state, 0.1, 3, &mut cursor)); // 3 lows → soft endpoint
        let carved = soft_endpoints(&events);
        assert_eq!(carved.len(), 1, "one soft endpoint: {events:?}");
        let utt = carved[0];
        assert_eq!(utt.utterance_id.seq, 1, "first utterance mints seq 1");
        assert_eq!(utt.cause, EndpointCause::SoftEndpoint);
        let wake = utt.wake.expect("armed wake attached");
        assert_eq!(
            wake.wake_end_sample, 500,
            "wake end is PCM-relative (start 0)"
        );
        assert!(!utt.pcm.is_empty(), "carved PCM is non-empty");
        assert!(
            state.wake.is_none(),
            "the passing utterance consumed the arm"
        );
    }

    /// Bypass carves every utterance with no arm and `wake: None`.
    #[test]
    fn bypass_carves_without_wake() {
        let mut state = ListenerState::new(synth_config(WakePolicy::Bypass));
        state.push_ring_for_test(0, &vec![5_i16; 4096]);
        let mut cursor = 0u64;
        let mut events = drive(&mut state, 0.9, 2, &mut cursor);
        events.extend(drive(&mut state, 0.1, 3, &mut cursor));
        let carved = soft_endpoints(&events);
        assert_eq!(carved.len(), 1, "bypass carves the utterance: {events:?}");
        assert!(carved[0].wake.is_none(), "bypass attaches no wake");
    }

    /// A continuation reuses the utterance id and its wake provenance: soft
    /// endpoint (id 1, arm consumed), resume supersedes, second soft endpoint is
    /// still id 1 and still wake-confirmed though the arm is long gone.
    #[test]
    fn continuation_reuses_id_and_wake() {
        let mut state = ListenerState::new(synth_config(WakePolicy::WakeGated));
        state.push_ring_for_test(0, &vec![3_i16; 8192]);
        state.arm_wake_for_test(0.9, 400);
        let mut cursor = 0u64;
        let mut events = drive(&mut state, 0.9, 2, &mut cursor);
        events.extend(drive(&mut state, 0.1, 3, &mut cursor)); // soft endpoint (id 1)
        events.extend(drive(&mut state, 0.9, 1, &mut cursor)); // resume → Superseded
        events.extend(drive(&mut state, 0.1, 3, &mut cursor)); // soft endpoint again
        let carved = soft_endpoints(&events);
        assert_eq!(carved.len(), 2, "two soft endpoints: {events:?}");
        assert_eq!(carved[0].utterance_id.seq, 1);
        assert_eq!(carved[1].utterance_id.seq, 1, "continuation reuses the id");
        assert!(
            carved[1].wake.is_some(),
            "continuation keeps wake confirmation past arm consumption"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ListenerEvent::Superseded { .. })),
            "the resume superseded: {events:?}"
        );
    }

    /// [`synth_config`] with a compact barge guard: 3 chunks (96 ms) of sustain,
    /// still lazier than the synthetic endpointer's 2-chunk onset, so the invariant
    /// the config asserts holds here exactly as it does in production.
    fn barge_config(policy: WakePolicy) -> ListenerConfig {
        ListenerConfig {
            barge_in: BargeInConfig {
                sustain_thresh: 0.6,
                sustain_chunks: 3,
            },
            ..synth_config(policy)
        }
    }

    fn barge_triggers(events: &[ListenerEvent]) -> Vec<u64> {
        events
            .iter()
            .filter_map(|e| match e {
                ListenerEvent::BargeIn { trigger_sample, .. } => Some(*trigger_sample),
                _ => None,
            })
            .collect()
    }

    /// Sustained confident speech over interruptible playback fires exactly one
    /// trigger per playback session — the latch — and the next response re-arms it.
    /// A response that could be cut twice by one breath would flush the reply to
    /// the barge as well as the barge's target.
    #[test]
    fn sustained_speech_over_interruptible_playback_triggers_once_per_session() {
        let mut state = ListenerState::new(barge_config(WakePolicy::WakeGated));
        state.push_ring_for_test(0, &vec![5_i16; 16_384]);
        state.set_playback(true, true);
        let mut cursor = 0u64;

        let events = drive(&mut state, 0.9, 3, &mut cursor);
        assert_eq!(
            barge_triggers(&events),
            vec![3 * 512],
            "the third sustained chunk fires, stamped at its end: {events:?}"
        );

        let more = drive(&mut state, 0.9, 6, &mut cursor);
        assert!(
            barge_triggers(&more).is_empty(),
            "the session is latched — one cut per response: {more:?}"
        );

        // The next response is its own to interrupt.
        state.set_playback(true, true);
        let next = drive(&mut state, 0.9, 3, &mut cursor);
        assert_eq!(
            barge_triggers(&next).len(),
            1,
            "a fresh playback start re-arms the trigger: {next:?}"
        );
    }

    /// Everything that must *not* cut a response: a burst shorter than the sustain
    /// run (a bark, a door), speech with nothing playing, and speech over a
    /// non-interruptible job (an alert). The run also resets across a burst rather
    /// than accumulating — two sub-threshold-length bursts are not one barge.
    #[test]
    fn sub_sustain_bursts_and_closed_floors_never_trigger() {
        let mut state = ListenerState::new(barge_config(WakePolicy::WakeGated));
        state.push_ring_for_test(0, &vec![5_i16; 16_384]);
        let mut cursor = 0u64;

        state.set_playback(true, true);
        let mut events = drive(&mut state, 0.9, 2, &mut cursor);
        events.extend(drive(&mut state, 0.1, 1, &mut cursor));
        events.extend(drive(&mut state, 0.9, 2, &mut cursor));
        assert!(
            barge_triggers(&events).is_empty(),
            "two 2-chunk bursts are not one 3-chunk barge: {events:?}"
        );

        state.set_playback(false, true);
        let idle = drive(&mut state, 0.9, 8, &mut cursor);
        assert!(
            barge_triggers(&idle).is_empty(),
            "nothing is playing — there is nothing to barge in on: {idle:?}"
        );

        state.set_playback(true, false);
        let alert = drive(&mut state, 0.9, 8, &mut cursor);
        assert!(
            barge_triggers(&alert).is_empty(),
            "a non-interruptible job is not cut by speech: {alert:?}"
        );
        assert!(
            !state.barge_pending,
            "and no utterance is armed to be heard as a barge"
        );
    }

    /// A job that starts non-interruptible mid-count stops the run: the `Started`
    /// feed's floor is what counts, not the one the chunks began under.
    #[test]
    fn a_non_interruptible_start_stops_a_run_in_progress() {
        let mut state = ListenerState::new(barge_config(WakePolicy::WakeGated));
        state.push_ring_for_test(0, &vec![5_i16; 16_384]);
        let mut cursor = 0u64;
        state.set_playback(true, true);
        drive(&mut state, 0.9, 2, &mut cursor);
        state.set_playback(true, false);
        let events = drive(&mut state, 0.9, 8, &mut cursor);
        assert!(
            barge_triggers(&events).is_empty(),
            "the alert closed the floor the run was counting under: {events:?}"
        );
    }

    /// The floor-open path: the barging speech carves as a wake-less utterance
    /// under `WakeGated` — no wake word was spoken, and none is trimmed — and a
    /// continuation of it keeps the mark, exactly as a wake-gated utterance keeps
    /// its provenance.
    #[test]
    fn barge_trigger_carves_a_wakeless_utterance_and_the_mark_survives_continuation() {
        let mut state = ListenerState::new(barge_config(WakePolicy::WakeGated));
        state.push_ring_for_test(0, &vec![7_i16; 16_384]);
        state.set_playback(true, true);
        let mut cursor = 0u64;

        let mut events = drive(&mut state, 0.9, 3, &mut cursor); // onset, then trigger
        assert_eq!(barge_triggers(&events).len(), 1, "the barge fired");
        events.extend(drive(&mut state, 0.1, 3, &mut cursor)); // soft endpoint
        events.extend(drive(&mut state, 0.9, 1, &mut cursor)); // resume → Superseded
        events.extend(drive(&mut state, 0.1, 3, &mut cursor)); // soft endpoint again

        let carved = soft_endpoints(&events);
        assert_eq!(carved.len(), 2, "two carves of one utterance: {events:?}");
        assert!(
            carved[0].barge_in,
            "the speech that cut the response is dispatched, marked as the barge"
        );
        assert!(
            carved[0].wake.is_none() && carved[0].stt_trim_samples == 0,
            "on the barge's own gate pass: no wake provenance, nothing to trim"
        );
        assert_eq!(carved[1].utterance_id.seq, carved[0].utterance_id.seq);
        assert!(carved[1].barge_in, "the continuation is still the barge");
        assert!(
            !state.barge_pending,
            "the carve consumed the trigger — it cannot leak into a later utterance"
        );
    }

    /// An utterance already accumulating when the trigger fires — the user resumed
    /// mid-response — takes the mark directly: there is an id to mark, so nothing
    /// is left pending for a later, unrelated carve to pick up.
    #[test]
    fn a_trigger_marks_the_utterance_already_in_progress() {
        let mut state = ListenerState::new(barge_config(WakePolicy::Bypass));
        state.push_ring_for_test(0, &vec![7_i16; 16_384]);
        let mut cursor = 0u64;
        // An utterance opens and carves (minting the id) with nothing playing.
        let mut events = drive(&mut state, 0.9, 2, &mut cursor);
        events.extend(drive(&mut state, 0.1, 3, &mut cursor));
        assert!(
            !soft_endpoints(&events)[0].barge_in,
            "no playback, no barge"
        );

        // Playback starts; the same utterance continues and sustains through it.
        state.set_playback(true, true);
        let mut resumed = drive(&mut state, 0.9, 3, &mut cursor);
        assert_eq!(barge_triggers(&resumed).len(), 1);
        assert!(
            !state.barge_pending,
            "the in-progress utterance took the mark; nothing pends"
        );
        resumed.extend(drive(&mut state, 0.1, 3, &mut cursor));
        let carved = soft_endpoints(&resumed);
        assert_eq!(carved.len(), 1);
        assert!(
            carved[0].barge_in,
            "the continuation carries the barge mark"
        );
    }

    /// Speech with the floor closed stays wake-gated: no trigger means no gate
    /// pass, so room noise during silence is dropped exactly as before.
    #[test]
    fn unwaked_speech_without_a_barge_stays_internal() {
        let mut state = ListenerState::new(barge_config(WakePolicy::WakeGated));
        state.push_ring_for_test(0, &vec![5_i16; 8_192]);
        let mut cursor = 0u64;
        let mut events = drive(&mut state, 0.9, 8, &mut cursor);
        events.extend(drive(&mut state, 0.1, 3, &mut cursor));
        assert!(
            soft_endpoints(&events).is_empty(),
            "no wake, no barge, no dispatch: {events:?}"
        );
    }

    /// A reconnect kills the writer, so the floor is closed by construction — and
    /// the trigger it left pending belongs to a response nobody can still hear.
    #[test]
    fn connect_resets_the_floor_the_latch_and_the_pending_mark() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(barge_config(WakePolicy::WakeGated));
        state.push_ring_for_test(0, &vec![5_i16; 8_192]);
        state.set_playback(true, true);
        let mut cursor = 0u64;
        let fired = drive(&mut state, 0.9, 3, &mut cursor);
        assert_eq!(barge_triggers(&fired).len(), 1);
        assert!(state.barge_pending, "a trigger is pending the next carve");

        state
            .handle(&pod(), Feed::Connected { epoch: 2 }, &mut oww, &mut silero)
            .expect("connected feed");
        assert!(
            !state.playback.active,
            "the writer died with the connection"
        );
        assert!(!state.playback.fired);
        assert_eq!(state.playback.sustain_run, 0);
        assert!(
            !state.barge_pending && !state.current_barge,
            "and no mark survives into the new connection"
        );
    }

    /// The `PlaybackState` feed reaches the same floor the guard reads — the wiring
    /// the synthetic tests drive through [`ListenerState::set_playback`].
    #[test]
    fn the_playback_feed_sets_the_floor() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(barge_config(WakePolicy::WakeGated));
        let events = state
            .handle(
                &pod(),
                Feed::PlaybackState {
                    active: true,
                    interruptible: true,
                },
                &mut oww,
                &mut silero,
            )
            .expect("playback feed");
        assert!(events.is_empty(), "a state change emits nothing by itself");
        assert!(state.playback.open(), "the guard may now count");
    }

    /// The trigger must be strictly lazier than the endpointer's onset on both
    /// axes: a trigger that fired first would cut a response on speech no
    /// utterance is tracking, and the barge would be heard as nothing.
    #[test]
    #[should_panic(expected = "sustain_thresh")]
    fn a_trigger_threshold_below_the_onset_threshold_is_rejected() {
        let config = ListenerConfig {
            barge_in: BargeInConfig {
                sustain_thresh: 0.1,
                ..BargeInConfig::default()
            },
            ..ListenerConfig::default()
        };
        assert!(config.barge_in.sustain_thresh < config.endpointer.onset_thresh);
        ListenerState::new(config);
    }

    #[test]
    #[should_panic(expected = "sustain_chunks")]
    fn a_sustain_run_shorter_than_the_onset_run_is_rejected() {
        let config = ListenerConfig {
            barge_in: BargeInConfig {
                sustain_chunks: 1,
                ..BargeInConfig::default()
            },
            endpointer: EndpointerConfig {
                onset_chunks: 2,
                ..EndpointerConfig::default()
            },
            ..ListenerConfig::default()
        };
        ListenerState::new(config);
    }

    /// The continuation window elapsing with no resume closes the utterance.
    #[test]
    fn continuation_window_closes_utterance() {
        let mut state = ListenerState::new(synth_config(WakePolicy::Bypass));
        state.push_ring_for_test(0, &vec![1_i16; 4096]);
        let mut cursor = 0u64;
        let mut events = drive(&mut state, 0.9, 2, &mut cursor);
        events.extend(drive(&mut state, 0.1, 3, &mut cursor)); // soft endpoint
        events.extend(drive(&mut state, 0.1, 4, &mut cursor)); // continuation elapses
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ListenerEvent::UtteranceClosed { .. })),
            "the window elapsed to a close: {events:?}"
        );
        assert!(state.current_id.is_none(), "utterance identity cleared");
    }

    /// The runtime's half of "every endpointer transition reaches a JSONL line":
    /// the endpointer unit tests prove the FSM *records* transitions and the
    /// pipeline tests prove an event *emits* one — this pins the join. Asserts the
    /// whole returned vector, not a filtered subset, over onset → soft endpoint →
    /// close: every transition surfaces, with the right cause, absolute offset and
    /// connection epoch, and each precedes the boundary event it explains.
    #[test]
    fn runtime_emits_every_endpointer_transition_in_order() {
        let mut state = ListenerState::new(synth_config(WakePolicy::Bypass));
        state.epoch = 7;
        state.push_ring_for_test(0, &vec![4_i16; 8192]);
        let mut cursor = 0u64;
        let mut events = drive(&mut state, 0.9, 2, &mut cursor); // onset at 1024
        events.extend(drive(&mut state, 0.1, 3, &mut cursor)); // soft endpoint at 2560
        // The synthetic driver bypasses `handle_audio`, which is what tracks the
        // stream position a close is stamped at; stand in for it.
        state.expected_next = Some(cursor);
        events.extend(state.handle_close(&pod(), rx_at(cursor)).unwrap()); // close inside the window

        assert_eq!(
            transitions(&events),
            vec![
                (TransitionCause::Onset, 1_024, 7),
                (TransitionCause::SoftEndpoint, 2_560, 7),
                (TransitionCause::DeviceReleaseClosed, 2_560, 7),
            ],
            "every transition, stamped with the live epoch: {events:?}"
        );

        // Ordering: a transition is the explanation for the boundary event that
        // follows it, so draining after `apply_endpoint_event` would misorder both
        // the JSONL and the replay rig's narration.
        let kinds: Vec<&str> = events
            .iter()
            .map(|e| match e {
                ListenerEvent::EndpointerTransition { .. } => "transition",
                ListenerEvent::ModelStats { .. } => "stats",
                ListenerEvent::SoftEndpoint { .. } => "soft_endpoint",
                ListenerEvent::UtteranceClosed { .. } => "closed",
                other => panic!("unexpected event: {other:?}"),
            })
            .collect();
        assert_eq!(
            kinds,
            vec![
                "stats",         // the chunks that led to the onset…
                "transition",    // …onset
                "stats",         // the chunks that led to the soft endpoint…
                "transition",    // …soft endpoint
                "soft_endpoint", // …explained by the transition before it
                // No "stats" here: the soft-endpoint flush drained every chunk
                // scored so far and none followed, so the close's flush finds the
                // accumulators empty and adds nothing. Flush points compose.
                "transition", // device release closed
                "closed",
            ],
            "each transition precedes the event it explains, behind the scores \
             that explain the transition: {events:?}"
        );
    }

    /// Every transition line arrives behind the scores that led to it: a
    /// `ModelStats(Transition)` precedes each `EndpointerTransition` and covers
    /// exactly the chunks since the previous flush — the transition-causing chunk
    /// included, which is what recording inside `drive_probability` (rather than at
    /// the model-push site) buys. A synthetic-`P` run performs no OWW pushes, so no
    /// OWW line appears.
    #[test]
    fn each_transition_flushes_the_silero_chunks_that_led_to_it() {
        let mut state = ListenerState::new(synth_config(WakePolicy::Bypass));
        state.epoch = 3;
        state.push_ring_for_test(0, &vec![4_i16; 8192]);
        let mut cursor = 0u64;

        // Onset at chunk 2 (onset_chunks = 2): the flush covers chunks 1..=2.
        let events = drive(&mut state, 0.9, 2, &mut cursor);
        let stats = model_stats(&events);
        assert_eq!(stats.len(), 1, "one flush, silero only: {events:?}");
        let (model, cause, s) = stats[0];
        assert_eq!(
            (model, cause),
            (StatsModel::Silero, StatsFlushCause::Transition)
        );
        assert_eq!(s.chunks, 2, "the onset-causing chunk is included");
        assert_eq!((s.first_chunk_end, s.last_chunk_end), (512, 1_024));
        assert_eq!((s.min, s.max, s.mean, s.median), (0.9, 0.9, 0.9, 0.9));
        // Ordering: the stats explain the transition, so they precede it.
        let kinds: Vec<&str> = events
            .iter()
            .map(|e| match e {
                ListenerEvent::ModelStats { .. } => "stats",
                ListenerEvent::EndpointerTransition { .. } => "transition",
                other => panic!("unexpected event: {other:?}"),
            })
            .collect();
        assert_eq!(kinds, vec!["stats", "transition"]);

        // Soft endpoint 3 chunks later: the next flush covers only chunks 3..=5 —
        // no chunk is reported twice.
        let events = drive(&mut state, 0.1, 3, &mut cursor);
        let stats = model_stats(&events);
        assert_eq!(stats.len(), 1, "one flush: {events:?}");
        let (_, cause, s) = stats[0];
        assert_eq!(cause, StatsFlushCause::Transition);
        assert_eq!(s.chunks, 3);
        assert_eq!(
            (s.first_chunk_end, s.last_chunk_end),
            (1_536, 2_560),
            "picks up exactly where the previous flush left off"
        );
        assert_eq!(
            s.max, 0.1,
            "and reports the release chunks, not the onset ones"
        );
    }

    /// The emptiness guard on the transition flush. `drain_transitions` runs once
    /// per Silero chunk and usually drains nothing; a flush there that ignored that
    /// would emit `model_stats` per chunk — the firehose this design forbids.
    #[test]
    fn a_chunk_that_drains_no_transition_flushes_nothing() {
        let mut state = ListenerState::new(synth_config(WakePolicy::Bypass));
        // One onset chunk short of the 2-chunk onset run: nothing transitions.
        let mut cursor = 0u64;
        let events = drive(&mut state, 0.9, 1, &mut cursor);
        assert!(
            events.is_empty(),
            "a transition-free chunk emits nothing at all: {events:?}"
        );
        assert_eq!(state.silero_stats.len(), 1, "but the score is accumulating");
    }

    /// The heartbeat: a long stretch with no transitions is exactly where
    /// transition logging goes silent, so the cap flushes on its own — and bounds
    /// the accumulator while it is at it.
    #[test]
    fn a_transition_free_stretch_emits_periodic_heartbeats() {
        let mut state = ListenerState::new(synth_config(WakePolicy::Bypass));
        let mut cursor = 0u64;
        // Sub-onset probability: the FSM sits in Idle and never transitions.
        let events = drive(&mut state, 0.1, MODEL_STATS_FLUSH_CHUNKS, &mut cursor);
        let stats = model_stats(&events);
        assert_eq!(
            stats.len(),
            1,
            "exactly one heartbeat at the cap: {stats:?}"
        );
        let (model, cause, s) = stats[0];
        assert_eq!(
            (model, cause),
            (StatsModel::Silero, StatsFlushCause::Periodic)
        );
        assert_eq!(s.chunks as usize, MODEL_STATS_FLUSH_CHUNKS);
        assert_eq!(
            (s.first_chunk_end, s.last_chunk_end),
            (512, (MODEL_STATS_FLUSH_CHUNKS as u64) * 512)
        );
        assert!(
            transitions(&events).is_empty(),
            "no transition explained it: {events:?}"
        );
        assert_eq!(state.silero_stats.len(), 0, "the cap bounds the buffer");

        // And it keeps beating.
        let events = drive(&mut state, 0.1, MODEL_STATS_FLUSH_CHUNKS, &mut cursor);
        assert_eq!(model_stats(&events).len(), 1, "a second heartbeat follows");
    }

    /// Segment close reports what the models saw across the segment — in a room
    /// where the FSM never transitions, the only line that says so. The
    /// `drain_transitions` inside `handle_close` then finds the accumulators empty
    /// and adds nothing: flush points compose, they do not double-emit.
    #[test]
    fn segment_close_flushes_the_segments_scores_once() {
        let mut state = ListenerState::new(synth_config(WakePolicy::Bypass));
        let mut cursor = 0u64;
        drive(&mut state, 0.1, 4, &mut cursor); // Idle throughout: no transitions
        state.expected_next = Some(cursor);

        let events = state.handle_close(&pod(), rx_at(cursor)).unwrap();
        let stats = model_stats(&events);
        assert_eq!(stats.len(), 1, "one flush, not two: {events:?}");
        let (model, cause, s) = stats[0];
        assert_eq!(
            (model, cause),
            (StatsModel::Silero, StatsFlushCause::SegmentClose)
        );
        assert_eq!(s.chunks, 4);
        assert_eq!((s.first_chunk_end, s.last_chunk_end), (512, 2_048));
    }

    /// A discontinuity re-anchors the stream; the chunks scored before it must not
    /// vanish with the state that indexed them.
    #[test]
    fn a_discontinuity_flushes_the_scores_it_is_about_to_orphan() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(synth_config(WakePolicy::Bypass));
        open(&mut state, 0, &mut oww, &mut silero);
        let mut cursor = 0u64;
        drive(&mut state, 0.1, 3, &mut cursor);

        // A frame past `expected_next`: the hole no inference may cross.
        let events = feed_audio(&mut state, 60_000, &vec![0_i16; 512], &mut oww, &mut silero);
        let reset: Vec<_> = model_stats(&events)
            .into_iter()
            .filter(|(_, cause, _)| *cause == StatsFlushCause::Reset)
            .collect();
        assert_eq!(reset.len(), 1, "the pre-reset scores surface: {events:?}");
        let (model, _, s) = reset[0];
        assert_eq!(model, StatsModel::Silero);
        assert_eq!(s.chunks, 3, "exactly the chunks scored before the hole");
        assert_eq!((s.first_chunk_end, s.last_chunk_end), (512, 1_536));
    }

    /// The pairing property, on the only thing that can prove it: real audio
    /// driving both models. Wherever both run, both report — so the console shows
    /// the OWW score next to the Silero one instead of leaving "wake fires fine" an
    /// anecdote. This asserts only that the stats surface, not what they say, which
    /// is what keeps it independent of the models' actual readings.
    ///
    /// Asserted as co-occurrence at *a* flush rather than pinned to the close
    /// batch: the fixture onsets, so mid-feed `Transition` flushes drain the
    /// accumulators and which model has refilled by the close is an artifact of the
    /// flush schedule. Mere presence somewhere in the run would not do — that holds
    /// even if Silero only ever reported at closes and OWW only ever at
    /// transitions, i.e. in a world where they never pair, which is the failure
    /// this test is named for.
    #[test]
    fn real_audio_pairs_silero_and_oww_stats_at_a_flush() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(test_config(WakePolicy::WakeGated));
        open(&mut state, 0, &mut oww, &mut silero);
        let phrase = wake_phrase_pcm();
        let mut events = feed_audio(&mut state, 0, &phrase, &mut oww, &mut silero);

        events.extend(close_segment(
            &mut state,
            phrase.len() as u64,
            &mut oww,
            &mut silero,
        ));
        let stats = model_stats(&events);
        // `flush_model_stats` drains Silero then OWW into adjacent events under one
        // cause, so a flush carrying both is exactly an adjacent same-cause pair.
        assert!(
            stats.windows(2).any(|w| {
                (w[0].0, w[1].0) == (StatsModel::Silero, StatsModel::Oww) && w[0].1 == w[1].1
            }),
            "some flush must drain both accumulators, not each model separately: {stats:?}"
        );
        for (model, _cause, s) in stats {
            assert!(s.chunks > 0, "{model:?} scored the fixture");
            assert!(
                s.min <= s.median && s.median <= s.max,
                "{model:?} summary is ordered: {s:?}"
            );
            assert!(
                s.last_chunk_end <= phrase.len() as u64,
                "{model:?} span stays inside the fed audio: {s:?}"
            );
        }
    }

    /// `Feed::Connected` mid-utterance: the reset transition closes out the
    /// connection being torn down, so it is stamped with the *old* epoch and the
    /// *old* index domain's teardown position — a `@0` under a millions-index epoch
    /// would be the one line a reconnect investigation misreads. The new epoch is
    /// adopted only after.
    #[test]
    fn connected_drains_reset_under_the_old_epoch_and_position() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(synth_config(WakePolicy::Bypass));
        state.epoch = 4;
        state.push_ring_for_test(0, &vec![2_i16; 8192]);
        let mut cursor = 0u64;
        let events = drive(&mut state, 0.9, 2, &mut cursor); // into Speech at 1024
        assert_eq!(
            transitions(&events),
            vec![(TransitionCause::Onset, 1_024, 4)]
        );
        // Stand in for `handle_audio`, which the synthetic driver bypasses: the old
        // connection's stream had reached sample 1024.
        state.expected_next = Some(cursor);

        let events = state
            .handle(&pod(), Feed::Connected { epoch: 5 }, &mut oww, &mut silero)
            .unwrap();
        assert_eq!(
            transitions(&events),
            vec![(TransitionCause::Reset, 1_024, 4)],
            "old epoch, old domain's teardown position: {events:?}"
        );
        assert_eq!(state.epoch, 5, "the new epoch is adopted after the drain");
    }

    /// `SegmentOpened` re-anchors the endpointer, and that reset is a transition
    /// like any other — dropping its drain would lose the line explaining why the
    /// next utterance's indexes jumped.
    #[test]
    fn segment_opened_drains_its_reset_transition() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(synth_config(WakePolicy::Bypass));
        state.epoch = 2;
        state.push_ring_for_test(0, &vec![6_i16; 8192]);
        let mut cursor = 0u64;
        drive(&mut state, 0.9, 2, &mut cursor); // into Speech

        let events = open_segment(&mut state, 9_000, 0, &mut oww, &mut silero);
        assert_eq!(
            transitions(&events),
            vec![(TransitionCause::Reset, 9_000, 2)],
            "the re-anchor records its reset at the new base: {events:?}"
        );
    }

    /// A discontinuity (dropped chunk) resets the endpointer mid-utterance; the
    /// reset transition surfaces alongside the `UtteranceClosed` it explains.
    #[test]
    fn discontinuity_drains_its_reset_transition() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(synth_config(WakePolicy::Bypass));
        open(&mut state, 0, &mut oww, &mut silero);
        state.epoch = 3;
        state.push_ring_for_test(0, &vec![8_i16; 8192]);
        let mut cursor = 0u64;
        drive(&mut state, 0.9, 2, &mut cursor); // into Speech

        // A frame arriving past `expected_next`: the hole no inference may cross.
        let events = feed_audio(&mut state, 60_000, &vec![0_i16; 512], &mut oww, &mut silero);
        assert_eq!(
            transitions(&events),
            vec![(TransitionCause::Reset, 60_000, 3)],
            "the discontinuity's reset is recorded at the re-anchor: {events:?}"
        );
    }

    /// The preroll-overlap reproduction. A segment opening less than one preroll
    /// after the previous one closed re-anchors to a `base_sample_index` *behind*
    /// the last pushed index — the device stamps preroll with the samples' original
    /// capture indexes. `SegmentOpened` sets `expected_next` to exactly that base,
    /// so the audio is not a discontinuity and reaches the ring overlapping. This
    /// used to trip the ring's overlap assert and kill the listener thread (every
    /// pod deaf until restart); the ring now dedupes and counts.
    #[test]
    fn segment_preroll_overlap_dedupes_without_panicking() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(test_config(WakePolicy::WakeGated));
        open(&mut state, 0, &mut oww, &mut silero);
        // Segment 1: 20 chunks of a recognizable constant, [0, 10240).
        let mut idx = 0u64;
        for _ in 0..20 {
            feed_audio(&mut state, idx, &vec![7_i16; 512], &mut oww, &mut silero);
            idx += 512;
        }
        assert_eq!(state.expected_next, Some(10_240));
        close_segment(&mut state, 10_240, &mut oww, &mut silero);
        assert_eq!(state.take_overlap_trimmed(), 0, "no overlap yet");

        // Segment 2 opens 4 chunks (2048 samples) *behind* the last pushed index:
        // its preroll reaches back into audio segment 1 already delivered.
        let base = 8_192;
        open_segment(&mut state, base, 2_048, &mut oww, &mut silero);
        // The re-sent preroll (as 7s again — same capture samples) plus 4 chunks of
        // genuinely new audio.
        let mut idx = base;
        for _ in 0..4 {
            feed_audio(&mut state, idx, &vec![7_i16; 512], &mut oww, &mut silero);
            idx += 512;
        }
        for _ in 0..4 {
            feed_audio(&mut state, idx, &vec![9_i16; 512], &mut oww, &mut silero);
            idx += 512;
        }

        assert_eq!(
            state.take_overlap_trimmed(),
            2_048,
            "the four re-sent preroll chunks are trimmed as duplicates"
        );
        assert_eq!(
            state.expected_next,
            Some(12_288),
            "expected_next advances past the whole push, overlap included"
        );
        // The carve across the boundary is contiguous and correctly ordered: no
        // duplication, no hole.
        let carved = state.ring.carve(10_240 - 512, 10_240 + 512);
        assert_eq!(&*carved, &[vec![7_i16; 512], vec![9_i16; 512]].concat());
    }

    /// The wake-in-overlap boundary: a wake fires late in segment N and segment
    /// N+1's preroll re-sends that audio. Nothing is lost and nothing
    /// double-accounts — the phrase carves once on the natural soft-endpoint path,
    /// the re-sent preroll dedupes, and a wake phrase spoken in the new segment
    /// arms fresh and gates its own utterance.
    #[test]
    fn wake_in_segment_overlap_resolves_once_and_rearms() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(test_config(WakePolicy::WakeGated));
        let phrase = wake_phrase_pcm();
        // Segment 1 carries the wake phrase and closes right after it.
        open(&mut state, 0, &mut oww, &mut silero);
        let mut events = feed_audio_chunkwise(&mut state, 0, &phrase, &mut oww, &mut silero);
        let wakes = events
            .iter()
            .filter(|e| matches!(e, ListenerEvent::WakeDetected { .. }))
            .count();
        assert_eq!(wakes, 1, "the phrase arms once in segment 1: {events:?}");
        // The phrase's own trailing silence soft-endpoints it inside the feed, so
        // the arm is consumed by that carve — nothing is left for the close, and
        // no `ArmExpired` fires anywhere in this segment.
        assert_eq!(
            soft_endpoints(&events).len(),
            1,
            "the phrase carves once on the natural path: {events:?}"
        );
        assert!(state.wake.is_none(), "the carve consumed the arm");
        events = close_segment(&mut state, phrase.len() as u64, &mut oww, &mut silero);
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ListenerEvent::SoftEndpoint { .. })),
            "the close has nothing left to carve: {events:?}"
        );

        // Segment 2 opens with a preroll covering the tail of the phrase: the same
        // audio, re-sent under its original indexes.
        let overlap = 8_192.min(phrase.len() as u64);
        let base = phrase.len() as u64 - overlap;
        open_segment(&mut state, base, overlap as u32, &mut oww, &mut silero);
        let trimmed_before = state.take_overlap_trimmed();
        assert_eq!(trimmed_before, 0);
        events = feed_audio_chunkwise(
            &mut state,
            base,
            &phrase[base as usize..],
            &mut oww,
            &mut silero,
        );
        assert_eq!(
            state.take_overlap_trimmed(),
            overlap,
            "the whole re-sent preroll is a duplicate — the ring keeps its own"
        );
        // The re-anchored OWW re-scores the duplicate audio from a cold window, so
        // a partial phrase in the preroll does not re-detect: the boundary neither
        // double-arms nor double-accounts the arm the close already consumed.
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ListenerEvent::WakeDetected { .. })),
            "a cold-anchored partial phrase does not re-detect: {events:?}"
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ListenerEvent::ArmExpired { .. })),
            "the segment-1 arm was consumed by its carve, not expired: {events:?}"
        );
        assert!(state.wake.is_none(), "no stale arm survives the boundary");

        // The recovery that matters: a phrase spoken *in* the new segment arms
        // fresh across the overlap boundary and gates its own carve.
        let idx = phrase.len() as u64;
        events = feed_audio(&mut state, idx, &phrase, &mut oww, &mut silero);
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, ListenerEvent::WakeDetected { .. }))
                .count(),
            1,
            "the new segment's phrase arms fresh: {events:?}"
        );
        let carved = soft_endpoints(&events);
        assert_eq!(carved.len(), 1, "the new command carves once: {events:?}");
        assert!(
            carved[0].wake.is_some(),
            "and it gated on the fresh wake, not the consumed one"
        );
        // A distinct utterance from segment 1's, so the boundary minted rather than
        // continued — the arm never crossed it.
        assert_eq!(carved[0].utterance_id.seq, 2);
    }

    /// Device release with an armed wake but Silero idle (missed onset) carves the
    /// fallback utterance from `[wake_end − preroll, close]`.
    #[test]
    fn device_release_missed_onset_carves_fallback() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(test_config(WakePolicy::WakeGated));
        open(&mut state, 0, &mut oww, &mut silero);
        // Feed silence so Silero stays idle, but store real audio in the ring.
        let mut idx = 0u64;
        for _ in 0..30 {
            feed_audio(&mut state, idx, &vec![7_i16; 512], &mut oww, &mut silero);
            idx += 512;
        }
        // Arm a wake as if OWW had fired mid-segment.
        state.arm_wake_for_test(0.9, 10_000);
        let events = close_segment(&mut state, idx, &mut oww, &mut silero);
        let carved: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                ListenerEvent::SoftEndpoint { utterance, .. } => Some(utterance),
                _ => None,
            })
            .collect();
        assert_eq!(
            carved.len(),
            1,
            "missed-onset fallback carves once: {events:?}"
        );
        assert_eq!(
            carved[0].cause,
            EndpointCause::DeviceVadRelease,
            "fallback is device-release-caused"
        );
        assert!(carved[0].wake.is_some(), "fallback carries the armed wake");
        // The fallback exists because Silero never onset, so there is no onset
        // receipt to report; the close is what drove the carve, so its receipt is
        // the endpoint stamp.
        assert_eq!(carved[0].timing.onset_rx, None);
        assert_eq!(carved[0].timing.soft_endpoint_rx, Some(rx_at(idx)));
        assert!(
            state.wake.is_none(),
            "the fallback consumed the arm at close"
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ListenerEvent::ArmExpired { .. })),
            "a consumed arm is carved, not expired: {events:?}"
        );
    }

    /// A segment closing with an armed wake the fallback carve cannot consume (its
    /// end sits past the close, so the missed-onset span is inverted and dropped)
    /// expires the arm as "wake, no follow", spanning `[wake_end − preroll, close]`.
    #[test]
    fn unconsumed_arm_expires_on_segment_close() {
        let mut state = ListenerState::new(synth_config(WakePolicy::WakeGated));
        state.arm_wake_for_test(0.8, 5_000);
        state.silero_cursor = 1_000; // close_sample source (expected_next is None)
        let events = state.handle_close(&pod(), rx_at(1_000)).unwrap();
        let expired: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                ListenerEvent::ArmExpired {
                    wake,
                    start_sample,
                    end_sample,
                    ..
                } => Some((*wake, *start_sample, *end_sample)),
                _ => None,
            })
            .collect();
        assert_eq!(expired.len(), 1, "the unconsumed arm expires: {events:?}");
        let (wake, start, end) = expired[0];
        assert_eq!(
            start,
            5_000 - 100,
            "span starts a preroll before the wake end"
        );
        assert_eq!(end, 5_000, "expiry clamps to at least the wake end");
        assert_eq!(wake.score, 0.8);
        assert_eq!(wake.wake_end_sample, 100, "wake end is span-relative");
        assert!(state.wake.is_none(), "the arm is cleared");
    }

    /// A fresh wake detection expires a prior unconsumed arm before arming the new
    /// one — the earlier wake fired with no command in between.
    #[test]
    fn fresh_wake_expires_the_prior_unconsumed_arm() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(test_config(WakePolicy::WakeGated));
        open(&mut state, 0, &mut oww, &mut silero);
        // A stale arm from earlier, never consumed by any utterance.
        state.arm_wake_for_test(0.5, 1_000);
        let phrase = wake_phrase_pcm();
        let events = feed_audio(&mut state, 0, &phrase, &mut oww, &mut silero);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ListenerEvent::ArmExpired { .. })),
            "the fresh wake expired the stale arm: {events:?}"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ListenerEvent::WakeDetected { .. })),
            "the fresh wake still detects: {events:?}"
        );
    }

    /// A reconnect (`Connected`) expires a pending arm the prior connection never
    /// resolved into a command.
    #[test]
    fn reconnect_expires_a_pending_arm() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(test_config(WakePolicy::WakeGated));
        state.arm_wake_for_test(0.9, 8_000);
        state.silero_cursor = 9_000;
        let events = state
            .handle(&pod(), Feed::Connected { epoch: 2 }, &mut oww, &mut silero)
            .unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ListenerEvent::ArmExpired { .. })),
            "reconnect expires the pending arm: {events:?}"
        );
        assert_eq!(state.epoch, 2, "the new epoch is adopted");
        assert!(state.wake.is_none(), "the arm is cleared on reconnect");
    }

    /// A wake-gated utterance with no armed wake stays internal — nothing carves,
    /// and the endpointer's later continuation events reference no id.
    #[test]
    fn wakegated_without_arm_drops_utterance() {
        let mut state = ListenerState::new(synth_config(WakePolicy::WakeGated));
        state.push_ring_for_test(0, &vec![1_i16; 4096]);
        // No arm.
        let mut cursor = 0u64;
        let mut events = drive(&mut state, 0.9, 2, &mut cursor);
        events.extend(drive(&mut state, 0.1, 3, &mut cursor)); // would-be soft endpoint
        events.extend(drive(&mut state, 0.9, 1, &mut cursor)); // would-be resume
        events.extend(drive(&mut state, 0.1, 3, &mut cursor));
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ListenerEvent::SoftEndpoint { .. })),
            "no arm ⇒ no carved utterance: {events:?}"
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ListenerEvent::Superseded { .. })),
            "no minted id ⇒ no Superseded emitted: {events:?}"
        );
    }

    /// A wake armed but *outside* the arm window (its end sits past the soft
    /// endpoint) fails the gate: nothing carves and the arm is not consumed — the
    /// rejection half of the wake-gating boundary against non-command speech.
    #[test]
    fn wakegated_arm_outside_window_drops_and_keeps_arm() {
        let mut state = ListenerState::new(synth_config(WakePolicy::WakeGated));
        state.push_ring_for_test(0, &vec![1_i16; 8192]);
        // The soft endpoint lands near sample ~2560; an arm ending far past it is
        // out of window (`wake_end <= end` fails).
        state.arm_wake_for_test(0.9, 1_000_000);
        let mut cursor = 0u64;
        let mut events = drive(&mut state, 0.9, 2, &mut cursor);
        events.extend(drive(&mut state, 0.1, 3, &mut cursor));
        assert!(
            soft_endpoints(&events).is_empty(),
            "an out-of-window arm carves nothing: {events:?}"
        );
        assert!(
            state.wake.is_some(),
            "the out-of-window arm is not consumed",
        );
        assert!(state.current_id.is_none(), "no utterance identity minted");
    }

    /// A non-contiguous audio index re-anchors inference and abandons any
    /// in-progress utterance (its STT would span the hole). Audio feeds carry the
    /// real (silent) chunks so this exercises the `handle_audio` discontinuity
    /// branch; the utterance is opened via synthetic P first.
    #[test]
    fn discontinuity_abandons_utterance_and_reanchors() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(synth_config(WakePolicy::Bypass));
        open(&mut state, 0, &mut oww, &mut silero);
        // Open an utterance and leave it soft-endpointed-awaiting-continuation.
        let mut cursor = 0u64;
        drive(&mut state, 0.9, 2, &mut cursor);
        drive(&mut state, 0.1, 3, &mut cursor);
        assert!(state.current_id.is_some(), "an utterance is in progress");
        // A forward index jump (dropped chunk) beyond the fed stream.
        let events = feed_audio(&mut state, 20_000, &vec![0_i16; 512], &mut oww, &mut silero);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ListenerEvent::UtteranceClosed { .. })),
            "a discontinuity closes the in-progress utterance: {events:?}"
        );
        assert!(
            state.current_id.is_none(),
            "utterance abandoned on the hole"
        );
        assert_eq!(state.oww_base, 20_000, "OWW re-anchored to the new index");
    }

    /// End to end through the shared thread: a Connected/SegmentOpened/wake-phrase/
    /// silence/close sequence produces a WakeDetected on the event channel and the
    /// stats reflect it. (Silero does not classify the TTS fixture as speech, so no
    /// natural soft endpoint fires here — that path is the replay harness's, driven
    /// by real captures; the endpointer wiring is covered by the synthetic-P tests.)
    #[tokio::test]
    async fn thread_runs_a_pod_end_to_end() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let handle = Listener::spawn(
            oww_models(),
            silero_model(),
            test_config(WakePolicy::WakeGated),
            tx,
        )
        .unwrap();
        let p = pod();
        handle.feed(p.clone(), Feed::Connected { epoch: 1 }).await;
        handle.feed(p.clone(), opened_feed(0, 0)).await;
        let phrase = wake_phrase_pcm();
        handle.feed(p.clone(), audio_feed(0, phrase.clone())).await;
        let mut idx = phrase.len() as u64;
        for _ in 0..40 {
            handle
                .feed(p.clone(), audio_feed(idx, vec![0_i16; 512]))
                .await;
            idx += 512;
        }
        handle.feed(p.clone(), closed_feed(idx)).await;

        let mut saw_wake = false;
        loop {
            match tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv()).await {
                Ok(Some(ListenerEvent::WakeDetected { .. })) => {
                    saw_wake = true;
                    break;
                }
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(_) => panic!("listener produced no events in time"),
            }
        }
        assert!(saw_wake, "thread emitted a wake detection");
        let stats = handle.stats_shared().snapshot();
        assert!(stats.feeds >= 1 && stats.wakes >= 1, "stats: {stats:?}");
        assert!(handle.join().is_ok());
    }

    /// A burst of feeds far exceeding the channel depth, faster than the thread's
    /// per-chunk inference, overflows the bounded channel — the drops are counted,
    /// never blocking the caller.
    #[tokio::test]
    async fn overfull_channel_drops_are_counted() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let handle = Listener::spawn(
            oww_models(),
            silero_model(),
            test_config(WakePolicy::WakeGated),
            tx,
        )
        .unwrap();
        let p = pod();
        handle.feed(p.clone(), Feed::Connected { epoch: 1 }).await;
        handle.feed(p.clone(), opened_feed(0, 0)).await;
        // Non-blocking feeds pile up far past FEED_CHANNEL_DEPTH while the thread
        // grinds ONNX per chunk.
        for i in 0..(FEED_CHANNEL_DEPTH as u64 * 8) {
            handle
                .feed(p.clone(), audio_feed(i * 512, vec![0_i16; 512]))
                .await;
        }
        assert!(
            handle.stats_shared().snapshot().dropped > 0,
            "an overfull channel must count drops"
        );
    }

    /// A sender wired to a plain receiver — the delivery semantics under test are
    /// the channel's, not the inference thread's, so no listener thread is spawned.
    fn test_sender() -> (FeedSender, mpsc::Receiver<(PodId, Feed)>) {
        let (tx, rx) = mpsc::channel::<(PodId, Feed)>(FEED_CHANNEL_DEPTH);
        (FeedSender::detached_for_tests(tx), rx)
    }

    /// Fill every slot so the next audio feed must drop and the next marker feed
    /// must wait.
    async fn saturate(sender: &FeedSender, p: &PodId) {
        for i in 0..FEED_CHANNEL_DEPTH as u64 {
            sender
                .feed(p.clone(), audio_feed(i * 512, vec![0_i16; 512]))
                .await;
        }
    }

    /// The split guarantee: on a full channel audio is dropped and counted, while a
    /// marker waits for room and is delivered.
    #[tokio::test]
    async fn marker_survives_full_channel_while_audio_drops() {
        let (sender, mut rx) = test_sender();
        let p = pod();
        saturate(&sender, &p).await;
        sender
            .feed(p.clone(), audio_feed(1 << 20, vec![0_i16; 512]))
            .await;
        let snap = sender.stats.snapshot();
        assert_eq!(snap.dropped, 1, "the overflow audio chunk was dropped");
        assert_eq!(snap.marker_send_timeouts, 0);

        let marker_sender = sender.clone();
        let marker_pod = p.clone();
        let marker = tokio::spawn(async move {
            marker_sender
                .feed(marker_pod, Feed::Connected { epoch: 1 })
                .await;
        });
        // Draining makes room; the blocked marker then lands.
        let mut saw_connected = false;
        for _ in 0..FEED_CHANNEL_DEPTH + 1 {
            match rx.recv().await {
                Some((_, Feed::Connected { epoch })) => {
                    assert_eq!(epoch, 1);
                    saw_connected = true;
                    break;
                }
                Some(_) => {}
                None => break,
            }
        }
        marker.await.unwrap();
        assert!(saw_connected, "the marker was delivered, not dropped");
        let snap = sender.stats.snapshot();
        assert_eq!(snap.dropped, 1, "the marker added no drop");
        assert_eq!(snap.marker_send_timeouts, 0);
    }

    /// Markers and audio from one producer arrive in send order even when the
    /// channel is saturated and drains slowly — the property the whole design
    /// rests on.
    #[tokio::test]
    async fn ordering_preserved_under_saturation() {
        let (sender, mut rx) = test_sender();
        let p = pod();
        saturate(&sender, &p).await;

        let producer_sender = sender.clone();
        let producer_pod = p.clone();
        let producer = tokio::spawn(async move {
            producer_sender
                .feed(producer_pod.clone(), opened_feed(1 << 20, 0))
                .await;
            producer_sender
                .feed(producer_pod.clone(), audio_feed(1 << 20, vec![0_i16; 512]))
                .await;
            producer_sender
                .feed(producer_pod, closed_feed((1 << 20) + 512))
                .await;
        });

        // Drain until the producer's last marker appears. Bounded by a timeout so
        // a lost item fails loudly instead of parking the test forever.
        let mut seen: Vec<&'static str> = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), async {
            while let Some((_, feed)) = rx.recv().await {
                let label = match feed {
                    Feed::SegmentOpened { .. } => "opened",
                    Feed::SegmentClosed { .. } => "closed",
                    Feed::Audio { .. } => "audio",
                    _ => "other",
                };
                seen.push(label);
                if label == "closed" {
                    break;
                }
            }
        })
        .await
        .expect("every produced item is delivered");
        producer.await.unwrap();
        let tail = &seen[seen.len() - 3..];
        assert_eq!(tail, ["opened", "audio", "closed"], "seen: {seen:?}");
    }

    /// A consumer that never drains must not hang the producer forever: the marker
    /// send returns after the bound, counting the abandoned marker as itself.
    #[tokio::test(start_paused = true)]
    async fn wedged_consumer_bounds_the_marker_wait() {
        let (sender, _rx) = test_sender();
        let p = pod();
        saturate(&sender, &p).await;
        let before = tokio::time::Instant::now();
        sender.feed(p, Feed::Connected { epoch: 1 }).await;
        assert!(
            before.elapsed() >= MARKER_SEND_TIMEOUT,
            "the full bound elapsed"
        );
        let snap = sender.stats.snapshot();
        assert_eq!(snap.marker_send_timeouts, 1);
        assert_eq!(
            snap.channel_closed, 0,
            "a wedged consumer is not a dead one"
        );
    }

    /// The listener thread exiting mid-await wakes the sender promptly and is
    /// counted apart from load drops.
    #[tokio::test]
    async fn closed_channel_releases_a_waiting_marker() {
        let (sender, mut rx) = test_sender();
        let p = pod();
        saturate(&sender, &p).await;
        let marker_sender = sender.clone();
        let (about_to_send, parked) = tokio::sync::oneshot::channel();
        let marker = tokio::spawn(async move {
            let _ = about_to_send.send(());
            marker_sender.feed(p, Feed::Connected { epoch: 1 }).await;
        });
        // Wait for the task to reach its send, then let it park on the full channel
        // before killing the consumer, so this exercises close-mid-await.
        parked.await.unwrap();
        tokio::task::yield_now().await;
        let before = tokio::time::Instant::now();
        rx.close();
        drop(rx);
        marker.await.unwrap();
        assert!(
            before.elapsed() < MARKER_SEND_TIMEOUT,
            "close wakes the waiter rather than letting it time out"
        );
        let snap = sender.stats.snapshot();
        assert_eq!(snap.channel_closed, 1);
        assert_eq!(snap.marker_send_timeouts, 0);
    }

    /// A reserved permit sends synchronously, and an unused one gives the slot back.
    #[tokio::test]
    async fn permit_sends_synchronously_and_releases_when_dropped() {
        let (sender, mut rx) = test_sender();
        let p = pod();
        let permit = sender.reserve_marker().await.expect("room is available");
        drop(permit);

        let permit = sender.reserve_marker().await.expect("the slot came back");
        permit.send(
            p,
            Feed::PlaybackState {
                active: false,
                interruptible: false,
            },
        );
        assert!(matches!(
            rx.recv().await,
            Some((_, Feed::PlaybackState { active: false, .. }))
        ));
    }

    /// After an inference error marks the stream discontinuous, the next
    /// *contiguous* chunk still re-anchors and abandons the in-progress utterance
    /// — the torn cursors never reach a carve.
    #[test]
    fn inference_error_reanchors_next_chunk() {
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(synth_config(WakePolicy::Bypass));
        open(&mut state, 0, &mut oww, &mut silero);
        // Contiguous silence syncs the stream and advances `expected_next`.
        let mut idx = 0u64;
        for _ in 0..4 {
            feed_audio(&mut state, idx, &vec![0_i16; 512], &mut oww, &mut silero);
            idx += 512;
        }
        // Open an utterance via synthetic P over the ring audio.
        let mut cursor = idx;
        drive(&mut state, 0.9, 2, &mut cursor);
        drive(&mut state, 0.1, 3, &mut cursor);
        assert!(state.current_id.is_some(), "an utterance is in progress");
        assert!(
            state.expected_next.is_some(),
            "stream synced before the error"
        );

        state.note_inference_error();
        assert_eq!(
            state.expected_next, None,
            "an inference error marks the stream discontinuous"
        );

        let events = feed_audio(&mut state, idx, &vec![0_i16; 512], &mut oww, &mut silero);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ListenerEvent::UtteranceClosed { .. })),
            "post-error recovery closes the in-progress utterance: {events:?}"
        );
        assert!(
            state.current_id.is_none(),
            "utterance abandoned after the error"
        );
        assert_eq!(
            state.oww_base, idx,
            "stream re-anchored to the recovery chunk"
        );
    }

    /// The listener thread's error path retains the failing `WakeError`'s message
    /// behind the counter, so a quietly-dead endpointer surfaces *what* failed.
    #[test]
    fn record_error_retains_message() {
        let stats = ListenerStats::default();
        assert!(stats.last_error().is_none(), "no error yet");
        stats.record_error(&WakeError::NonFiniteScore);
        assert_eq!(stats.snapshot().errors, 1, "the error is counted");
        let msg = stats.last_error().expect("the message is retained");
        assert!(!msg.is_empty(), "a non-empty diagnostic survives capture");
    }

    /// The ring's dedup is silent by design, so this counter is its only tripwire —
    /// a device re-sending a different range under the same indexes, or a host
    /// index-math regression, shows up here or nowhere. Pin the whole hop: state →
    /// shared counter → snapshot, including the zero-guard (which must not swallow
    /// a real count) and the drain's idempotence.
    #[test]
    fn overlap_trim_accumulates_from_state_into_the_snapshot() {
        let stats = ListenerStats::default();
        let mut state = ListenerState::new(synth_config(WakePolicy::Bypass));
        assert_eq!(stats.snapshot().overlap_trimmed_samples, 0);

        // No overlap: the guard skips the atomic and the snapshot stays put.
        stats.accumulate_overlap(&mut state);
        assert_eq!(stats.snapshot().overlap_trimmed_samples, 0);

        // A push overlapping retained audio by 4 samples.
        state.push_ring_for_test(0, &[1, 2, 3, 4, 5, 6, 7, 8]);
        state.push_ring_for_test(4, &[5, 6, 7, 8, 9, 10]);
        stats.accumulate_overlap(&mut state);
        assert_eq!(
            stats.snapshot().overlap_trimmed_samples,
            4,
            "the trimmed duplicate prefix reaches the snapshot"
        );

        // Drained, not re-read: a second accumulate must not double-count.
        stats.accumulate_overlap(&mut state);
        assert_eq!(stats.snapshot().overlap_trimmed_samples, 4);

        // And it accumulates across feeds rather than replacing.
        state.push_ring_for_test(8, &[9, 10, 11]);
        stats.accumulate_overlap(&mut state);
        assert_eq!(stats.snapshot().overlap_trimmed_samples, 6);
    }

    /// The accounting survives the error branch: the ring push precedes inference,
    /// so a chunk that trims duplicates and *then* fails scoring must still report
    /// the trim — otherwise the tripwire goes dark exactly when the listener is
    /// sick. An `ort` failure can't be provoked from here, so this drives the real
    /// overlap through `handle` and then the recovery the error arm performs
    /// (`record_error` + `note_inference_error`), asserting the count is intact
    /// after both. What it pins: no error-path step clears the pending trim, and
    /// the drain is not reachable only through the `Ok` arm.
    #[test]
    fn overlap_trim_survives_the_inference_error_path() {
        let stats = ListenerStats::default();
        let mut oww = oww_models();
        let mut silero = silero_model();
        let mut state = ListenerState::new(test_config(WakePolicy::Bypass));
        open(&mut state, 0, &mut oww, &mut silero);
        feed_audio(&mut state, 0, &vec![3_i16; 1024], &mut oww, &mut silero);
        stats.accumulate_overlap(&mut state);
        assert_eq!(stats.snapshot().overlap_trimmed_samples, 0);

        // Re-anchor behind the last pushed index (the preroll case), then feed
        // overlapping audio through the same path the thread drives.
        open_segment(&mut state, 512, 512, &mut oww, &mut silero);
        feed_audio(&mut state, 512, &vec![3_i16; 1024], &mut oww, &mut silero);
        // The thread's order: accumulate, *then* dispatch on the outcome.
        stats.accumulate_overlap(&mut state);
        stats.record_error(&WakeError::NonFiniteScore);
        state.note_inference_error();
        assert_eq!(
            stats.snapshot().overlap_trimmed_samples,
            512,
            "the 512 re-sent samples are counted whatever the inference outcome"
        );
    }
}
