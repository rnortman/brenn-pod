//! The listener⇄pipeline contract: the [`Feed`] a connection pushes in, and the
//! [`ListenerEvent`]s the listener emits back out.
//!
//! The listener taps the live `SessionEvent` stream *pre-assembly* (segments stay
//! recording/tracking artifacts), so its input is chunk-granular: a [`Feed`] per
//! decoded audio frame plus the boundary markers (`Connected`/`SegmentOpened`/
//! `SegmentClosed`) it keys its per-pod state resets on. Its output is the
//! utterance-semantic events the reworked pipeline consumes: wake detections
//! (for sidecar labeling), and carved utterances with their supersede/close
//! lifecycle.

use std::sync::Arc;

use pod_ingest::{DeviceMicros, Gap, HostMicros};
use serde::Serialize;

use super::endpointer::{EndpointCause, EndpointTransition};
use super::stats::ScoreSummary;
use crate::types::{PodId, SegmentEndCause, UtteranceId, WakeConfirmation};

/// One item the connection task forwards to the listener for a pod. Sample indexes
/// are absolute (the `SessionEvent::Audio.first_sample_index` domain); the listener
/// keys discontinuity handling on the FSM's already-computed [`Gap`] rather than
/// re-deriving it.
///
/// The time stamps are **supplied by the caller**, never read by the listener — the
/// same contract the ingest `SessionFsm` keeps, and what leaves the listener pure
/// (no clock, no I/O) and replay-testable. Every stamped field is copied verbatim
/// off the corresponding `SessionEvent`.
#[derive(Debug, Clone)]
pub enum Feed {
    /// A new connection for this pod — reset all per-pod state and adopt `epoch`
    /// (stamped onto every utterance id so stale events from a prior connection are
    /// droppable downstream).
    Connected { epoch: u64 },
    /// A transport segment opened at `base_sample_index`. Segments are separated by
    /// real device-VAD silence, so this re-anchors the streaming inference state.
    SegmentOpened {
        base_sample_index: u64,
        /// Leading pre-VAD-onset samples the device included: the device VAD went
        /// high at `base_sample_index + preroll_samples`.
        preroll_samples: u32,
        /// Device-clock capture time of the segment's first sample — the anchor
        /// for sample-offset → device-time math within this segment.
        base_device_ts: DeviceMicros,
    },
    /// A decoded PCM chunk. `first_sample_index` is the absolute index of `pcm[0]`;
    /// `gap` is set when this frame broke continuity with the previous one.
    Audio {
        first_sample_index: u64,
        gap: Option<Gap>,
        pcm: Arc<[i16]>,
        /// Device-clock capture time of `pcm[0]`.
        device_ts: DeviceMicros,
        /// Host receipt of this frame — the measurement every downstream latency
        /// number is referenced to.
        host_rx: HostMicros,
    },
    /// Playback state for this pod changed. Fed by the surface's playback-event
    /// adapter; ordering relative to audio feeds is inherently fuzzy (independent
    /// tasks), which the ± `lead_ms` accuracy of the progress estimate already
    /// absorbs. `interruptible` mirrors the playing job's flag: a
    /// non-interruptible response (alerts) never opens the barge-in floor.
    PlaybackState {
        active: bool,
        interruptible: bool,
        /// Whether the audible reply's own words carry the wake phrase. A wake
        /// detection over such a reply is the machine hearing itself say the
        /// phrase, so it never becomes a barge; the detection is still scored,
        /// armed and reported.
        may_wake: bool,
        /// The turn this reply answers, when it answers one. The floor is per
        /// turn: a report naming the turn already audible refines it and leaves
        /// the latch alone, while a different turn is a new reply to cut.
        turn: Option<UtteranceId>,
    },
    /// Open a capture window: for the next `window_samples` of audio, speech may be
    /// carved with no wake word. Fed by the surface when a reply that asked to keep
    /// listening has finished sounding.
    ///
    /// One-shot. The window closes on the first utterance minted under it, whatever
    /// its provenance, and on the deadline passing with the endpointer idle.
    Listen { window_samples: u64 },
    /// The transport segment closed (the authoritative outer boundary). Finalizes
    /// any in-progress utterance and clears the wake arm.
    SegmentClosed {
        end: SegmentEndCause,
        /// Host receipt of the close — the endpoint stamp for a carve the device
        /// boundary forces.
        host_rx: HostMicros,
    },
}

/// A listener-layer utterance identity: `(pod, epoch, seq)`. The supersede/abort
/// join key. `seq` is per-pod monotonic within an `epoch`; a continuation reuses
/// its id, and a fresh utterance (after the endpointer returns to `Idle`) mints a
/// new one. `(epoch, seq)` orders utterances within a pod (the pipeline's
/// "abort in-flight STT with id ≤ arriving id" rule).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct ListenerUtteranceId {
    pub pod: PodId,
    pub epoch: u64,
    pub seq: u64,
}

impl ListenerUtteranceId {
    /// The `(epoch, seq)` order key: monotonic within a pod, so the pipeline's
    /// "abort any in-flight STT with id ≤ arriving id" rule (and epoch-stale event
    /// filtering) is a plain tuple comparison.
    pub fn order_key(&self) -> (u64, u64) {
        (self.epoch, self.seq)
    }
}

/// When, on the host clock, each stage of an utterance's *audio* was received.
///
/// Every field is the host-receipt time of the audio that caused the stage —
/// deliberately not the emission time of the corresponding [`ListenerEvent`]. The
/// difference between the two is listener channel + inference lag, which is a
/// number worth having rather than one to conflate into the rest.
#[derive(Debug, Clone, Copy, Default)]
pub struct CarveTiming {
    /// **t0**: host receipt of the utterance's first audio (its preroll-padded
    /// start). Every latency the pipeline reports is referenced to this.
    ///
    /// `None` only when no segment-open record covers the carve — reachable when
    /// a `SegmentOpened` feed was dropped (see `ListenerHandle::feed`), never in
    /// the ordinary path.
    pub first_audio_rx: Option<HostMicros>,
    /// Whether `first_audio_rx` is projected rather than measured. An utterance
    /// whose audio begins inside the device preroll opened the segment, so the
    /// segment's first-audio receipt *is* its first-audio receipt (measured). An
    /// utterance starting later in an already-open segment (music holding the VAD
    /// open, or a second command inside one segment) has no receipt of its own —
    /// its start is projected off the device clock and carries that estimate's
    /// fuzziness.
    pub t0_projected: bool,
    /// Host receipt of the chunk whose scoring completed the wake detection.
    /// `None` for an unwaked (bypass-policy) utterance. May precede t0: the arm
    /// window accepts a wake up to `arm_slack_samples` before the utterance
    /// starts.
    pub wake_detected_rx: Option<HostMicros>,
    /// Host receipt of the chunk that drove the endpointer's `Onset`. `None` on
    /// the missed-onset fallback carve, which never onset.
    pub onset_rx: Option<HostMicros>,
    /// Host receipt of the audio that drove this carve: the soft-endpointing
    /// chunk, or the `SegmentClosed` for a device-release carve. A continuation's
    /// later carve overwrites this; the other stamps persist.
    pub soft_endpoint_rx: Option<HostMicros>,
    /// Estimated host instant the device VAD went high (the segment's first
    /// post-preroll sample), projected off the device clock. Fuzzy — late by the
    /// minimum transport delay; see `ClockOffsetEstimate`.
    pub vad_high_est: Option<HostMicros>,
}

/// An utterance carved from the PCM ring at a soft endpoint: the audio STT will
/// run on, plus the provenance the pipeline needs to mint an `Utterance`. Carries
/// the raw absolute sample span; the pipeline resolves it to covering `SegmentRef`s
/// (its recent-segment tracking) when it builds the wire `audio_ref`.
#[derive(Debug, Clone)]
pub struct CarvedUtterance {
    pub utterance_id: ListenerUtteranceId,
    /// Carved PCM: `[start_sample, end_sample)` from the ring, gaps spliced silent.
    pub pcm: Arc<[i16]>,
    /// Absolute utterance start (preroll-padded onset).
    pub start_sample: u64,
    /// Absolute utterance end (the soft endpoint).
    pub end_sample: u64,
    /// Wake provenance for a wake-gated utterance; `None` under `Bypass`.
    pub wake: Option<WakeConfirmation>,
    /// Why this utterance's audio ends where it does.
    pub cause: EndpointCause,
    /// This utterance is the speech that barged in on active playback: it passed
    /// the wake gate on the barge-in trigger rather than on a wake arm, so `wake`
    /// is `None` and nothing is trimmed (there is no wake word to trim).
    pub barge_in: bool,
    /// This utterance's speech was heard over this pod's own playback at some
    /// point in its life: the floor was open on a chunk between its onset and its
    /// endpoint. `barge_in` implies this.
    pub over_playback: bool,
    /// This utterance was carved inside an open capture window — the person kept
    /// talking after a reply that asked them to. It passed the wake gate on the
    /// window rather than on a wake arm, so `wake` is `None` and nothing is
    /// trimmed (there is no wake word to trim).
    pub follow_up: bool,
    /// Host-receipt stamps for this utterance's audio, from t0 to the carve.
    pub timing: CarveTiming,
}

/// Which rule cut a reply.
///
/// The two are not alternatives and are not ordered: the wake word cuts whatever
/// else the machine believes, and the speech rule cuts a wake-less interruption
/// in the mode that runs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BargeCause {
    /// Sustained confident speech, under the mode that trusts it.
    Speech,
    /// The wake phrase, detected while an interruptible reply was audible.
    Wake,
}

/// What the listener emits back to the pipeline.
#[derive(Debug, Clone)]
pub enum ListenerEvent {
    /// A wake phrase crossed threshold. Emitted for every detection regardless of
    /// wake-gating policy so the pipeline can label the sidecar corpus; the arm
    /// itself lives inside the listener. `wake_end_sample` is absolute, and is only
    /// comparable against other indexes carrying the same `epoch` — the connection
    /// the detection belongs to, since the index space restarts with each one.
    WakeDetected {
        pod: PodId,
        epoch: u64,
        score: f32,
        wake_end_sample: u64,
    },
    /// A wake phrase crossed threshold and was discarded unheard, because the pod
    /// was muted — its own playback was sounding, or its tail had not yet passed.
    /// Nothing was armed, reported as a detection, or cut.
    ///
    /// A reader is entitled to conclude that the wake word did fire in this
    /// stretch and that the mute is why nothing came of it: with `Mute`
    /// configured, this line is the difference between "the detector never
    /// scored the phrase" and "it did, on the robot's own voice or over it". It
    /// says nothing about whose voice — only that the machine was talking.
    WakeMuted {
        pod: PodId,
        epoch: u64,
        score: f32,
        /// Absolute index one past the last sample of the discarded phrase.
        wake_end_sample: u64,
    },
    /// An interruption crossed the barge-in guard while interruptible playback was
    /// active for this pod: cut the response. Fires at most once per playback
    /// session (the latch re-arms when playback next starts). The speech that
    /// triggered it goes on to carve as an ordinary utterance, marked
    /// [`CarvedUtterance::barge_in`].
    BargeIn {
        pod: PodId,
        epoch: u64,
        /// Which of the two rules fired.
        cause: BargeCause,
        /// Absolute index one past the last sample of the chunk that completed the
        /// sustain run, or of the wake phrase that cut.
        trigger_sample: u64,
        /// Host receipt of the audio that drove the trigger.
        host_rx: HostMicros,
    },
    /// An utterance soft-endpointed and its PCM is carved — STT may start.
    SoftEndpoint {
        pod: PodId,
        utterance: CarvedUtterance,
    },
    /// Speech resumed inside the continuation window: abort that utterance's
    /// in-flight STT. The same id keeps accumulating; a later `SoftEndpoint`
    /// carries the whole concatenation.
    Superseded {
        pod: PodId,
        utterance_id: ListenerUtteranceId,
    },
    /// The continuation window elapsed with no resume — the utterance is final.
    UtteranceClosed {
        pod: PodId,
        utterance_id: ListenerUtteranceId,
    },
    /// A wake-gated utterance ended within a wake-tail of the wake end, so it held
    /// the wake word and nothing else: it is not published, the arm is kept, and the
    /// listener waits until `deadline_sample` for the command to onset. The
    /// utterance that follows inside the wait is carved from `start_sample`, so one
    /// utterance carries wake word, pause and command.
    ///
    /// Purely the accounting for that decision — the interaction is still open, so
    /// nothing downstream acts on it. A hold resolves either into an ordinary
    /// `SoftEndpoint` or into [`ListenerEvent::ArmExpired`].
    WakeHeld {
        pod: PodId,
        epoch: u64,
        /// Absolute start of the held (and of the eventual coalesced) carve.
        start_sample: u64,
        /// Absolute end of the held carve's speech.
        end_sample: u64,
        /// Absolute index one past the wake phrase, as the arm recorded it.
        wake_end_sample: u64,
        /// `end_sample` plus the command wait: past this, with the endpointer idle,
        /// the wake was a bare wake.
        deadline_sample: u64,
    },
    /// An armed wake was cleared without any utterance passing the policy — a
    /// "wake, no follow": the wake fired but no command followed (the transport
    /// segment closed, a fresh wake replaced the arm, or the connection reset).
    /// Carries the fallback audio span `[wake_end − preroll_pad, expiry]` and the
    /// wake provenance so the pipeline emits the same `WakeCommandAbsent`
    /// accounting an empty/low-confidence command produces. `wake`'s offsets are
    /// relative to `start_sample`.
    ArmExpired {
        pod: PodId,
        wake: WakeConfirmation,
        /// Absolute span start (`wake_end − preroll_pad`).
        start_sample: u64,
        /// Absolute span end (the expiry point, never before `wake_end`).
        end_sample: u64,
    },
    /// A host-endpointer FSM state transition, surfaced purely for timing
    /// observability (no utterance payload). Carries the pod, the connection
    /// `epoch`, and the transition itself (from/to state, cause, absolute sample
    /// offset) so the pipeline can emit an `endpointer_transition` line.
    EndpointerTransition {
        pod: PodId,
        epoch: u64,
        transition: EndpointTransition,
    },
    /// A summary of one model's per-chunk scores since the previous flush, surfaced
    /// purely for observability. `EndpointerTransition` says what the FSM *did*,
    /// which is silent exactly when it does nothing; this says what the models were
    /// *returning*, which is the reading a silent room needs. Never per-chunk — a
    /// flush point drains an accumulator covering many chunks.
    ModelStats {
        pod: PodId,
        epoch: u64,
        model: StatsModel,
        cause: StatsFlushCause,
        summary: ScoreSummary,
    },
    /// A capture window opened: speech beginning at or before `deadline_sample`
    /// carves with no wake word. Accounting only — the window is the listener's own
    /// state and nothing downstream acts on this.
    ListenOpened {
        pod: PodId,
        epoch: u64,
        /// The last absolute sample index at which speech may still begin inside
        /// the window.
        deadline_sample: u64,
    },
    /// Speech was heard inside an open capture window — at its onset, and again at
    /// the carve that closes the window. A reader is entitled to conclude that a
    /// person is talking to this pod right now and to hold off anything that would
    /// end the interaction; it says nothing about what was said, which only the
    /// utterance that follows can.
    ListenHeard { pod: PodId, epoch: u64 },
    /// A capture window ended with nothing carved under it: the wake word gates
    /// the microphone again. Its deadline passed with the endpointer idle, or the
    /// window outlived its reason — a new reply began over it, the stream
    /// re-anchored past its deadline, or the connection went. One of these follows
    /// every `ListenOpened` that no utterance closed, so a reader may balance the
    /// two. Accounting only.
    ListenExpired { pod: PodId, epoch: u64 },
}

/// Which model a [`ListenerEvent::ModelStats`] summarizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StatsModel {
    /// Silero P(speech), one score per 512-sample (32 ms) chunk.
    Silero,
    /// openWakeWord's wake-head score, one per embedding step (80 ms).
    Oww,
}

/// Why a [`ListenerEvent::ModelStats`] flushed. Every flush drains both
/// accumulators, so the cause names the seam, not the model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StatsFlushCause {
    /// The endpointer transitioned: these are the chunks that led to it (the
    /// transition-causing chunk included).
    Transition,
    /// The accumulation cap was reached — the heartbeat through a long stretch
    /// with no transitions.
    Periodic,
    /// The transport segment closed: what the models saw across the whole segment.
    SegmentClose,
    /// The stream was re-anchored (reconnect, discontinuity, or a new segment's
    /// base). Chunks scored before the reset must not vanish silently.
    Reset,
}

/// Which utterances the listener forwards to STT. The policy seam: layered
/// per-pod over the same listener runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WakePolicy {
    /// Default: an utterance is forwarded only when an armed wake's end falls in
    /// `[utterance_start − arm_slack, soft_endpoint]`. Non-wake speech stays
    /// internal (CPU + flood containment).
    #[default]
    WakeGated,
    /// Every utterance is forwarded, no wake required. Tests / the future
    /// floor-open policies.
    Bypass,
}
