//! The spine value types: the typed messages that flow through the pipeline,
//! plus the identity newtypes and per-stage timing record they carry.
//!
//! Every type is `Debug + Clone + Serialize` — the pipeline serializes them to
//! JSONL today and (later) onto the Brenn surface bus, so the shapes here are
//! the wire-facing envelope, not internal scratch.
//!
//! Two clock domains stay distinct (see `pod_ingest::clock`): `DeviceMicros`
//! is intra-segment sample math only; `HostMicros` is host wall-clock, used for
//! every latency stamp. They never mix.

use std::path::Path;
use std::sync::Arc;

pub use pod_ingest::Codec;
use pod_ingest::{
    resolve_open, splice_log_into, CrossCheck, DeviceMicros, FormatConstraint, HostMicros,
    ResolveError, Resolved, SegmentRef, SpliceStop, TelemetryKind,
};
use serde::{Deserialize, Serialize};

/// The one PCM/handshake format the speech spine accepts: 16 kHz mono S16, one
/// of the mono beam variants (stereo rejected). Supplied to the ingest FSM's
/// format gate so a wire-legal but spine-incompatible `Hello` (stereo, wrong
/// rate) is rejected at the door rather than half-interpreted downstream.
pub const SPINE_FORMAT: FormatConstraint = FormatConstraint {
    sample_rate_hz: 16_000,
    bits_per_sample: 16,
    channels: 1,
    codec: Codec::S16Le,
    mono_beam_only: true,
};

/// A pod's stable identity, from `Hello.pod_id`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct PodId(pub String);

/// A room name, from the host-side pod→room config map (`unmapped` when absent).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct RoomId(pub String);

/// A host-minted utterance identifier (monotonic within a process).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct UtteranceId(pub u64);

/// A speaker identity, when known (speaker-id is best-effort and often absent).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct SpeakerId(pub String);

/// One assembled VAD segment: the PCM plus every piece of context the pipeline
/// and record store need. Built by the segment assembler from a connection's
/// `SessionEvent`s.
#[derive(Debug, Clone, Serialize)]
pub struct Segment {
    pub pod: PodId,
    pub room: RoomId,
    pub segment_id: u32,
    /// Absolute sample index of the segment's first sample.
    pub base_sample_index: u64,
    /// Leading pre-VAD-onset samples included in the segment.
    pub preroll_samples: u32,
    /// Decoded mono S16 samples for the whole segment.
    pub pcm: Vec<i16>,
    /// Device-clock anchor for the segment base.
    pub device_ts: DeviceMicros,
    /// Host receive time of the segment's first frame.
    pub host_rx: HostMicros,
    pub end: SegmentEndInfo,
    /// DoA/energy readings interleaved through the segment, sample-offset-indexed.
    pub telemetry: Vec<SegmentTelemetry>,
    /// Reference to the recorded frame log this segment came from.
    pub audio_ref: SegmentRef,
    pub timings: StageTimings,
}

/// How and why a segment ended, with the flags downstream uses to qualify its
/// confidence in a truncated or capped segment.
#[derive(Debug, Clone, Serialize)]
pub struct SegmentEndInfo {
    pub cause: SegmentEndCause,
    /// The segment was cut short (mid-segment disconnect or host cap).
    pub truncated: bool,
    /// The segment reopened a previously-truncated one.
    pub resumed: bool,
    /// Number of intra-segment sample-index discontinuities observed.
    pub gap_count: u32,
    /// The device/receiver sample-count cross-check, or `None` when none ran:
    /// a truncated close carries no device counters to compare, and a
    /// host-capped segment is finalized before its real `SegmentEnd` arrives.
    /// JSONL serializes `None` as `null`, distinguishing "not run" from any
    /// verdict.
    pub cross_check: Option<CrossCheck>,
}

impl SegmentEndInfo {
    /// Build a `SegmentEndInfo`, deriving `truncated` from `cause` so the two can
    /// never disagree. `truncated` is retained as a serialized field for JSONL
    /// consumers, but every construction site goes through here.
    pub fn new(
        cause: SegmentEndCause,
        resumed: bool,
        gap_count: u32,
        cross_check: Option<CrossCheck>,
    ) -> Self {
        let truncated = matches!(
            cause,
            SegmentEndCause::Truncated | SegmentEndCause::HostCapped
        );
        Self {
            cause,
            truncated,
            resumed,
            gap_count,
            cross_check,
        }
    }
}

/// Why a segment ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentEndCause {
    /// The device's on-board VAD released (normal end).
    VadRelease,
    /// The device's ring lapped under sustained backpressure.
    Overrun,
    /// The segment was truncated (disconnect).
    Truncated,
    /// The device closed the segment on an internal firmware fault — distinct from
    /// `Truncated`, which means the connection died with no close at all.
    InternalError,
    /// The host-side length cap fired before the device's real `SegmentEnd`.
    HostCapped,
}

/// One telemetry reading attached to a segment: the DoA-azimuth or speech-energy
/// payload plus its sample offset from the segment base.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct SegmentTelemetry {
    /// Sample offset from the segment base (from the device timestamp).
    pub sample_offset: i64,
    pub kind: TelemetryKind,
}

/// The DoA-bearing, pre-wake-gate event emitted for every assembled segment —
/// the passive-speaker-tracking signal the future arbitrator triangulates from.
/// Emitted regardless of the wake decision; empty tracks are valid (DoA is
/// best-effort and telemetry loss is real).
#[derive(Debug, Clone, Serialize)]
pub struct TrackingEvent {
    pub pod: PodId,
    pub room: RoomId,
    pub segment_id: u32,
    /// Host-clock segment start.
    pub start: HostMicros,
    /// Host-clock segment end.
    pub end: HostMicros,
    pub end_info: SegmentEndInfo,
    pub doa: DoaTrack,
    /// Sample-offset-indexed speech-energy readings (four beams).
    pub energy: Vec<(i64, [f32; 4])>,
    pub audio_ref: SegmentRef,
}

/// A sequence of sample-offset-indexed azimuth readings (four tracked beams,
/// radians). `NaN` is valid on indices 0/1/3 when no beam is tracked.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(transparent)]
pub struct DoaTrack(pub Vec<(i64, [f32; 4])>);

impl DoaTrack {
    /// Build a DoA track from a segment's telemetry: the `Azimuths` readings,
    /// each carrying its sample offset. Non-azimuth telemetry is ignored. Shared
    /// by the tracking-event emitter and the `Utterance`-minting wake stage so
    /// both derive the same DoA track from one place.
    pub fn from_telemetry(telemetry: &[SegmentTelemetry]) -> DoaTrack {
        DoaTrack(
            telemetry
                .iter()
                .filter_map(|t| match t.kind {
                    TelemetryKind::Azimuths { values } => Some((t.sample_offset, values)),
                    TelemetryKind::SpEnergy { .. } => None,
                })
                .collect(),
        )
    }
}

/// An outbound speak command to a pod. Constructed by the playback path (a
/// later increment); defined here so the type graph is complete.
#[derive(Debug, Clone, Serialize)]
pub struct SpeakCmd {
    // TODO(pod-identity-trust): `target` is a self-asserted, unauthenticated pod
    // identity — any LAN peer can claim it via `Hello`, and the supersede registry
    // hands routing to the latest claimant, so synthesized, household-derived audio
    // for a pod id goes to whichever socket last claimed it. Disposition: accepted on
    // a trusted single-household LAN — the same envelope under which raw mic audio
    // already flows inbound unauthenticated (the more sensitive direction) — with no
    // per-pod token for now; a takeover is loud because the supersede is already
    // JSONL-visible. Mitigation path is the deferred `audio-auth` (TLS/PSK) work plus
    // `pod-auth-threat-model`. Slug stays open pending that.
    pub target: PodId,
    pub in_reply_to: Option<UtteranceId>,
    pub body: SpeakBody,
    /// Whether a new incoming segment may barge in and flush this playback.
    pub interruptible: bool,
    /// The originating utterance's stamps, carried to the playback side for
    /// latency decomposition. `Default` when there is no originating utterance.
    pub timings: StageTimings,
}

/// The payload of a `SpeakCmd`: text to synthesize, or ready PCM to play.
#[derive(Debug, Clone, Serialize)]
pub enum SpeakBody {
    Text(String),
    Pcm(Arc<[i16]>),
}

/// A completed transcript for an utterance.
#[derive(Debug, Clone, Serialize)]
pub struct Transcript {
    pub text: String,
    /// Aggregate STT quality signals, when the backend returned a `verbose_json`
    /// response with per-segment fields. `None` for a plain-`json` backend that
    /// carries no segments — the transcript text still stands. Carried for live
    /// observation only; no decision reads it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<TranscriptConfidence>,
}

/// Aggregate speech-to-text quality signals for one transcript, summarized across
/// the `verbose_json` response's segments. `avg_logprob` is duration-weighted
/// (higher — closer to zero — is more confident); `no_speech_prob` and
/// `compression_ratio` are the worst (maximum) segment values, since one
/// silence-like or repetition-looped segment is the tell worth surfacing. Used
/// for live observation today; no routing or rejection reads it.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct TranscriptConfidence {
    /// Duration-weighted mean of per-segment `avg_logprob`. Whisper reports this
    /// negative; nearer zero is more confident.
    pub avg_logprob: f32,
    /// Largest per-segment `no_speech_prob` — the most silence-like segment.
    pub no_speech_prob: f32,
    /// Largest per-segment `compression_ratio` — the most repetitive segment,
    /// a hallucination-loop tell.
    pub compression_ratio: f32,
    /// Number of segments the aggregate summarizes.
    pub segments: u32,
}

/// STT-confidence gate: thresholds that flag a transcript as a likely
/// hallucination rather than a real command. Whisper emits fluent phantom text on
/// wake-word-only-in-noise segments; its confidence signals separate the two.
///
/// Live-hardware basis for the defaults: real commands measured `no_speech_prob`
/// in {0.01, 0.01, 0.04} and `avg_logprob` in {−0.28, −0.55, −0.64}; wake-in-noise
/// hallucinations measured `no_speech_prob` in {0.35, 0.37, 0.39} and `avg_logprob`
/// in {−0.97, −0.99, −1.05}. `no_speech_prob` is a clean separator (~9× gap), so it
/// is the primary gate with a default of `0.2` — squarely in the empty band.
/// `avg_logprob` overlaps less cleanly, so its gate is opt-in (default disabled).
/// `compression_ratio` does not separate the two bands and is deliberately unused.
#[derive(Debug, Clone, Copy)]
pub struct ConfidenceGate {
    /// Reject when `no_speech_prob` exceeds this. Default `0.2`.
    pub no_speech_max: f32,
    /// When `Some(t)`, also reject when the duration-weighted `avg_logprob` is
    /// below `t`. `None` disables this secondary gate. Default `None`.
    pub avg_logprob_min: Option<f32>,
}

impl ConfidenceGate {
    /// A gate that never fires: `no_speech_prob` cannot exceed `1.0` and the
    /// logprob gate is disabled. The off-position used where no `[stt]` gate is
    /// configured and by tests not exercising gating.
    pub const OFF: ConfidenceGate = ConfidenceGate {
        no_speech_max: 1.0,
        avg_logprob_min: None,
    };

    /// Evaluate the gate against a transcript's confidence summary. `None` passes;
    /// `Some(reject)` rejects and carries the offending signals for the log line.
    /// The caller consults this only when a summary is present — a missing summary
    /// fails open (never gated).
    pub fn evaluate(&self, conf: &TranscriptConfidence) -> Option<GateReject> {
        let no_speech_hit = conf.no_speech_prob > self.no_speech_max;
        let logprob_hit = self
            .avg_logprob_min
            .is_some_and(|min| conf.avg_logprob < min);
        (no_speech_hit || logprob_hit).then_some(GateReject {
            no_speech_prob: conf.no_speech_prob,
            avg_logprob: conf.avg_logprob,
        })
    }
}

/// The offending confidence signals from a gate rejection, carried onto the
/// no-command log line so the reason (`low_confidence no_speech=… logprob=…`) is
/// legible next to the empty-transcript case.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GateReject {
    pub no_speech_prob: f32,
    pub avg_logprob: f32,
}

/// How far through a response *clip* the user was when they interrupted it.
/// Granularity is one `SpeakCmd` (one playback job): `total_ms` is the
/// currently-playing clip's nominal duration and `heard_ms` counts within that
/// clip. For today's single-cmd-per-turn brains, clip == response.
///
/// A host-side approximation from the paced writer: accurate to ± the pacer lead
/// plus the device playout hop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct InterruptProgress {
    /// Estimated milliseconds of the clip the user actually heard.
    pub heard_ms: u64,
    /// Nominal duration of the whole clip.
    pub total_ms: u64,
}

/// One link in a barge-in context chain: an interrupted turn's transcript,
/// response, and where it was cut.
#[derive(Debug, Clone, Serialize)]
pub struct ContextSegment {
    pub utterance: UtteranceId,
    /// The interrupted turn's STT text (`None` when it had no transcript).
    pub transcript: Option<String>,
    /// The interrupted turn's brain output (`SpeakBody::Text`; `None` for Pcm).
    pub response_text: Option<String>,
    pub interrupted: InterruptProgress,
}

/// The chain of interrupted turns leading to a barge-in utterance, oldest first.
/// Bounded at [`MAX_CONTEXT_SEGMENTS`] (drop-oldest); cleared whenever a response
/// completes without barge-in.
#[derive(Debug, Clone, Serialize)]
pub struct BargeInContext {
    pub chain: Arc<[ContextSegment]>,
}

/// Upper bound on a pod's barge-in context chain, drop-oldest past it. Nobody
/// holds a multi-hour conversation barging in at every step; the bound exists so
/// a pathological session cannot grow the ledger without limit.
pub const MAX_CONTEXT_SEGMENTS: usize = 256;

/// Wake-gate provenance for a scored-accept utterance: the confirming score, the
/// detected wake-phrase end (a sample offset into the segment's PCM), and the
/// count of leading samples the pipeline cut before STT. `None` for a bypassed
/// utterance — no gate scored it. Lets a transcript-driven brain tell a wake that
/// produced no follow-on command (empty transcript after a scored accept) apart
/// from generic no-transcript noise reaching a bypassed gate.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct WakeConfirmation {
    pub score: f32,
    pub wake_end_sample: usize,
    pub stt_trim_samples: usize,
}

/// A self-contained reference to the recorded audio an utterance was carved from:
/// the frame log, the absolute sample range, and the covering segment parts in
/// order. Unlike a bare `SegmentRef` (one wire segment), a span can cross
/// cap-rollover part boundaries and begin mid-segment, so a listener utterance
/// carved across parts still resolves to exactly the audio STT heard. Kept
/// self-contained (log + span + parts) so the Brenn envelope needs nothing
/// segment-shaped; `segments` is the replay join key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioSpan {
    /// Frame-log file name (store-root-relative), shared by every covering part.
    pub log: String,
    /// Absolute index of the first sample (inclusive).
    pub start_sample: u64,
    /// Absolute index one past the last sample (exclusive).
    pub end_sample: u64,
    /// Covering segment parts, in order.
    pub segments: Vec<SegmentRef>,
}

impl AudioSpan {
    /// The span covering a whole assembled segment: its `[base, base + len)`
    /// sample range and its single `SegmentRef`. The reference an utterance
    /// carries when it maps to exactly one wire segment rather than a carved
    /// sub-span.
    pub fn whole_segment(seg: &Segment) -> AudioSpan {
        AudioSpan {
            log: seg.audio_ref.log.clone(),
            start_sample: seg.base_sample_index,
            end_sample: seg.base_sample_index + seg.pcm.len() as u64,
            segments: vec![seg.audio_ref.clone()],
        }
    }

    /// Resolve this span's audio back to PCM: decode every covering log's
    /// frames, splice each `Audio` event at its absolute sample index, and
    /// slice to `[start_sample, end_sample)`. Uncovered ranges (a pruned log,
    /// a wire gap, or a span carved before any segment closed) stay silence.
    ///
    /// The logs read are `self.log` first, then each `segments[i].log` not
    /// already seen — the refs are provenance and pruned-accounting, not a
    /// filter, so a span with no covering parts (`self.segments` empty) still
    /// recovers real audio from `self.log` whenever the frames are on disk.
    ///
    /// A pruned log is reported in `pruned` (never a hard error — the pruner
    /// is an independent actor). A store fault (`ResolveError::Fault`) aborts
    /// with `Err`: the caller must never mistake an outage for silence.
    pub fn resolve(&self, store_root: &Path) -> Result<ResolvedSpanAudio, SpanResolveError> {
        if self.end_sample < self.start_sample
            || self.end_sample - self.start_sample > MAX_RESOLVE_SAMPLES
        {
            return Err(SpanResolveError::InvalidSpan {
                start: self.start_sample,
                end: self.end_sample,
            });
        }
        let len = (self.end_sample - self.start_sample) as usize;
        let mut pcm = vec![0i16; len];
        let mut covered_samples = 0u64;
        let mut pruned = Vec::new();
        let mut stopped = Vec::new();
        let mut protocol_errors = 0u64;

        // Ordered, deduplicated logs to read: `self.log` first (paired with
        // every ref that names it, if any), then each further
        // `segments[i].log` not yet seen, paired with its refs.
        let mut logs: Vec<(String, Vec<SegmentRef>)> = vec![(self.log.clone(), Vec::new())];
        for r in &self.segments {
            match logs.iter_mut().find(|(log, _)| *log == r.log) {
                Some(entry) => entry.1.push(r.clone()),
                None => logs.push((r.log.clone(), vec![r.clone()])),
            }
        }

        for (log_name, refs) in logs {
            let synth_ref = SegmentRef {
                log: log_name.clone(),
                segment_id: 0,
                part: 0,
            };
            let ref_to_open = refs.first().unwrap_or(&synth_ref);
            match resolve_open(store_root, ref_to_open) {
                Ok(Resolved::Pruned) => {
                    if refs.is_empty() {
                        pruned.push(synth_ref);
                    } else {
                        pruned.extend(refs);
                    }
                }
                Ok(Resolved::Found(reader)) => {
                    match splice_log_into(reader, SPINE_FORMAT, self.start_sample, &mut pcm) {
                        Ok(outcome) => {
                            covered_samples += outcome.samples_written;
                            protocol_errors += outcome.protocol_errors;
                            if let Some(stop) = outcome.stopped {
                                stopped.push((log_name, stop));
                            }
                        }
                        Err(e) => {
                            return Err(SpanResolveError::Resolve {
                                log: log_name,
                                source: ResolveError::Fault(e),
                            });
                        }
                    }
                }
                Err(source) => {
                    return Err(SpanResolveError::Resolve {
                        log: log_name,
                        source,
                    });
                }
            }
        }

        Ok(ResolvedSpanAudio {
            pcm,
            covered_samples,
            pruned,
            stopped,
            protocol_errors,
        })
    }
}

/// Guards [`AudioSpan::resolve`]'s allocation against an untrusted
/// `end_sample` (`AudioSpan` deserializes from sidecars/JSONL): 10 minutes at
/// 16 kHz mono, about 19 MiB.
pub const MAX_RESOLVE_SAMPLES: u64 = 16_000 * 60 * 10;

/// PCM decoded back from an `AudioSpan`'s covering logs, via
/// [`AudioSpan::resolve`].
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedSpanAudio {
    /// `end_sample - start_sample` S16 samples at [`SPINE_FORMAT`] (16 kHz
    /// mono). Uncovered ranges are silence (zeros).
    pub pcm: Vec<i16>,
    /// Real (non-silence) samples spliced in, summed across logs. An upper
    /// bound, not an exact count: every overlapping copy is counted, whether
    /// the overlap is cross-log (same-domain audio from two logs overwrites
    /// rather than deduplicates) or within one log (the FSM tolerates a
    /// backward `first_sample_index` jump and keeps splicing, re-copying an
    /// already-written range). Can exceed `pcm.len()`.
    pub covered_samples: u64,
    /// Covering parts (or a synthesized ref for `self.log` when no part
    /// names it) whose log was pruned.
    pub pruned: Vec<SegmentRef>,
    /// Logs whose replay stopped before clean EOF or before the span's end:
    /// (log name, stop cause).
    pub stopped: Vec<(String, SpliceStop)>,
    /// Non-fatal protocol errors encountered while replaying, summed across
    /// logs. Each one leaves its samples' range as silence, indistinguishable
    /// from a wire gap without this count.
    pub protocol_errors: u64,
}

/// Errors from [`AudioSpan::resolve`].
#[derive(Debug, thiserror::Error)]
pub enum SpanResolveError {
    /// `end_sample < start_sample`, or the span's length exceeds
    /// `MAX_RESOLVE_SAMPLES`.
    #[error("invalid span [{start}, {end})")]
    InvalidSpan { start: u64, end: u64 },
    /// A covering log failed validation, or the store faulted while opening
    /// it.
    #[error("resolving log {log} failed: {source}")]
    Resolve {
        log: String,
        #[source]
        source: ResolveError,
    },
}

/// Why an utterance's audio ends where it does — carried from the host endpointer
/// through the carved utterance onto the `Utterance` so downstream (STT trim,
/// sidecar, the Brenn envelope) knows the boundary's provenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointCause {
    /// Silero P(speech) stayed low through the soft-hangover window.
    SoftEndpoint,
    /// The utterance hit the `max_utterance` length cap.
    Capped,
    /// The device closed the transport segment (VAD release) — the outer-boundary
    /// fallback, covering both a missed release and a missed onset.
    DeviceVadRelease,
}

/// An assembled, wake-confirmed utterance: the future Brenn surface envelope
/// (room/pod context, DoA, nullable speaker identity, audio reference). Carries
/// the transcript once STT runs.
#[derive(Debug, Clone, Serialize)]
pub struct Utterance {
    pub id: UtteranceId,
    pub pod: PodId,
    pub room: RoomId,
    pub speaker: Option<SpeakerId>,
    pub doa: DoaTrack,
    pub audio_ref: AudioSpan,
    pub transcript: Option<Transcript>,
    pub timings: StageTimings,
    /// Why the host endpointer ended this utterance's audio where it did.
    pub endpoint_cause: EndpointCause,
    /// Wake provenance for a scored accept; `None` for a bypassed utterance.
    /// Internal routing input for the brain (a scored accept with an empty
    /// transcript is a wake-with-no-command, not a failure) — skipped from the
    /// wire envelope, where the wake detection line already carries the same numbers.
    #[serde(skip)]
    pub wake: Option<WakeConfirmation>,
    /// Present when this utterance barged in on active playback. Carries the
    /// context chain of every interrupted turn since the last clean completion.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub barge_in: Option<BargeInContext>,
}

/// Host-clock stamps at each pipeline boundary, one `Option` per stage. Grows a
/// field per increment as stages land; a `None` stamp is a stage a segment did
/// not reach. JSONL reports the stamps and their same-domain deltas (a negative
/// delta from an NTP step is clamped to 0 and counted, never a bogus latency).
///
/// Two families live here. The segment-era stamps (`first_frame_rx` through
/// `tracking_emitted`) belong to an assembled transport segment and drive the
/// tracking/sidecar path. The rest belong to one carved utterance's
/// segment-and-response cycle, referenced to `first_audio_rx` as t0; the
/// listener-domain stamps among them are *host-receipt times of the audio* that
/// caused each stage, copied from the carve, not the emission times of the
/// listener events that reported them.
#[derive(Debug, Clone, Default, Serialize)]
pub struct StageTimings {
    /// First frame of the segment arrived.
    pub first_frame_rx: Option<HostMicros>,
    /// The `SegmentEnd` arrived.
    pub segment_end_rx: Option<HostMicros>,
    /// The assembler produced the `Segment`.
    pub assembled: Option<HostMicros>,
    /// The tracking event was emitted.
    pub tracking_emitted: Option<HostMicros>,
    /// **t0**: host receipt of the utterance's first audio (its preroll-padded
    /// start). Every offset the latency summary reports is referenced to this.
    pub first_audio_rx: Option<HostMicros>,
    /// Whether `first_audio_rx` was projected off the device clock rather than
    /// measured — carried with the stamp it qualifies, so it is `Some` exactly
    /// when `first_audio_rx` is. A projected t0 is fuzzy: late by the minimum
    /// one-way transport delay.
    pub t0_projected: Option<bool>,
    /// Estimated host instant the device VAD went high. Fuzzy, in the same
    /// direction as a projected t0. Legitimately *before* t0: the device holds a
    /// preroll of audio captured before it opened the segment.
    pub vad_high_est: Option<HostMicros>,
    /// Host receipt of the audio whose scoring completed the wake detection. May
    /// legitimately precede t0 — the arm window accepts a wake that lands up to
    /// `arm_slack_samples` before the utterance starts.
    pub wake_detected_rx: Option<HostMicros>,
    /// Host receipt of the audio that drove the endpointer's onset. `None` on a
    /// missed-onset fallback carve, which never onset.
    pub onset_rx: Option<HostMicros>,
    /// Host receipt of the audio this carve ends on: the soft-endpointing chunk,
    /// or the segment close for a device-release carve. Everything earlier is
    /// speech plus designed hangover; the blameable pipeline latency starts here.
    pub soft_endpoint_rx: Option<HostMicros>,
    /// The pipeline spawned the speculative STT.
    pub stt_started: Option<HostMicros>,
    /// The transcriber produced a transcript. Stamped only when a transcriber is
    /// wired and STT succeeded; stays `None` (serialized `null`) when no
    /// transcriber is wired or STT failed. It means "stage produced a transcript",
    /// not "stage was attempted" — a failed attempt leaves this `None` and carries
    /// its own elapsed time on the failure line.
    pub transcribed: Option<HostMicros>,
    /// The pipeline dispatched the utterance to the brain. Stamped only when a
    /// brain is wired; stays `None` (serialized `null`) otherwise. Stamped before
    /// the confidence gate, so a gate decline leaves this set with no matching
    /// `brain_dispatched` line — harmless, since a decline reaches no playback and
    /// so emits no latency summary.
    pub brain_dispatched: Option<HostMicros>,
    /// The router entered the synthesis await for this utterance's response.
    /// `None` for a `Pcm` response body, which synthesizes nothing.
    pub synth_started: Option<HostMicros>,
    /// The synthesizer returned the response PCM. `None` for a `Pcm` body.
    pub synth_completed: Option<HostMicros>,
}

/// Same-domain latency between two pipeline-boundary stamps, clamped to 0 on a
/// backward clock step (an NTP correction) so a bogus huge value never reaches
/// JSONL. `None` when either stamp is absent (a stage the segment did not reach).
/// The one place every per-boundary JSONL emitter computes a delta.
///
/// Each clamped backward step increments `clamps`, so a run of suspicious
/// zero-latency lines can be corroborated against the `stage_health` clock-step
/// count rather than read as a stalled-then-instant pipeline. The counter is a
/// mandatory parameter — there is no uncounted variant — so no caller can
/// silently drop a clock step on the floor.
pub fn stage_delta_us(
    earlier: Option<HostMicros>,
    later: Option<HostMicros>,
    clamps: &std::sync::atomic::AtomicU64,
) -> Option<u64> {
    match (earlier, later) {
        (Some(e), Some(l)) => Some(match l.checked_delta(e) {
            Some(d) => d,
            None => {
                clamps.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                0
            }
        }),
        _ => None,
    }
}

/// Signed same-domain offset of `stamp` from `origin`, for reporting a set of
/// stages on one axis anchored at t0. The signed companion to [`stage_delta_us`]:
/// no clamping and no clock-step counter, because a negative offset here is
/// *expected*, not an anomaly — the estimated VAD-high instant precedes the first
/// audio receipt by the device preroll, and a wake may land before the utterance
/// it arms. Clamping those to 0 would zero the very numbers the axis exists to
/// show, and counting them would flood `stage_health` with phantom clock steps.
///
/// Use [`stage_delta_us`] for a consecutive-stage duration, where a negative
/// value genuinely is a backward clock step. `None` when either stamp is absent.
///
/// Both stamps are microseconds since the UNIX epoch (~1.7e15), three orders of
/// magnitude below `i64::MAX`, so neither cast wraps.
pub fn signed_offset_us(origin: Option<HostMicros>, stamp: Option<HostMicros>) -> Option<i64> {
    match (origin, stamp) {
        (Some(o), Some(s)) => Some(s.0 as i64 - o.0 as i64),
        _ => None,
    }
}

/// A representative assembled `Segment` for tests across the crate's pipeline
/// stages, overriding only `telemetry`. One shared shape so every stage's tests
/// exercise the same "typical segment" and gain new fields in one place.
#[cfg(test)]
pub(crate) fn test_segment(telemetry: Vec<SegmentTelemetry>) -> Segment {
    Segment {
        pod: PodId("pod-x".into()),
        room: RoomId("kitchen".into()),
        segment_id: 7,
        base_sample_index: 0,
        preroll_samples: 0,
        pcm: vec![],
        device_ts: DeviceMicros(0),
        host_rx: HostMicros(1_000),
        end: SegmentEndInfo::new(SegmentEndCause::VadRelease, false, 0, None),
        telemetry,
        audio_ref: SegmentRef {
            log: "pod-x_0.framelog".into(),
            segment_id: 7,
            part: 0,
        },
        timings: StageTimings {
            first_frame_rx: Some(HostMicros(1_000)),
            segment_end_rx: Some(HostMicros(5_000)),
            assembled: Some(HostMicros(5_001)),
            tracking_emitted: None,
            ..StageTimings::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spine_format_is_16k_mono_s16_beam_only() {
        // Accept only 16 kHz mono S16 from a mono beam; stereo is rejected.
        assert_eq!(
            SPINE_FORMAT,
            FormatConstraint {
                sample_rate_hz: 16_000,
                bits_per_sample: 16,
                channels: 1,
                codec: Codec::S16Le,
                mono_beam_only: true,
            }
        );
    }

    #[test]
    fn id_newtypes_serialize_transparently() {
        assert_eq!(
            serde_json::to_string(&PodId("pod-a1b2c3".into())).unwrap(),
            "\"pod-a1b2c3\""
        );
        assert_eq!(serde_json::to_string(&UtteranceId(7)).unwrap(), "7");
    }

    #[test]
    fn doa_track_serializes_as_bare_array() {
        let doa = DoaTrack(vec![(0, [1.0, f32::NAN, 2.0, 3.0])]);
        // Transparent newtype: the wrapper does not appear in the JSON.
        let json = serde_json::to_string(&doa).unwrap();
        assert!(json.starts_with('['), "expected bare array, got {json}");
    }

    #[test]
    fn cross_check_none_serializes_as_null() {
        let end = SegmentEndInfo {
            cause: SegmentEndCause::Truncated,
            truncated: true,
            resumed: false,
            gap_count: 0,
            cross_check: None,
        };
        let json = serde_json::to_string(&end).unwrap();
        assert!(json.contains("\"cross_check\":null"), "got {json}");
    }

    #[test]
    fn brain_dispatched_none_serializes_as_null() {
        // No `skip_serializing_if`: an absent brain dispatch is an explicit
        // `null` field, keeping the crate's null-stamp uniformity.
        let json = serde_json::to_string(&StageTimings::default()).unwrap();
        assert!(json.contains("\"brain_dispatched\":null"), "got {json}");
    }

    #[test]
    fn utterance_serde_round_trips() {
        let utt = Utterance {
            id: UtteranceId(1),
            pod: PodId("pod-x".into()),
            room: RoomId("kitchen".into()),
            speaker: None,
            doa: DoaTrack::default(),
            audio_ref: AudioSpan {
                log: "pod-x_0.framelog".into(),
                start_sample: 0,
                end_sample: 16,
                segments: vec![SegmentRef {
                    log: "pod-x_0.framelog".into(),
                    segment_id: 3,
                    part: 0,
                }],
            },
            transcript: Some(Transcript {
                text: "hello".into(),
                confidence: None,
            }),
            timings: StageTimings::default(),
            endpoint_cause: EndpointCause::SoftEndpoint,
            wake: None,
            barge_in: None,
        };
        // Round-trips through serde (the surface-envelope requirement).
        let json = serde_json::to_string(&utt).unwrap();
        assert!(
            json.contains("\"transcript\":{\"text\":\"hello\"}"),
            "{json}"
        );
        // The audio reference serializes as the self-contained span (log + range
        // + covering parts), not a bare segment reference.
        assert!(
            json.contains("\"audio_ref\":{\"log\":\"pod-x_0.framelog\",\"start_sample\":0,\"end_sample\":16,\"segments\":[")
                && json.contains("\"segment_id\":3"),
            "{json}"
        );
    }

    #[test]
    fn audio_span_whole_segment_covers_the_segment() {
        // A whole-segment span runs `[base, base + len)` and carries the one
        // covering `SegmentRef`.
        let mut seg = test_segment(vec![]);
        seg.base_sample_index = 1_000;
        seg.pcm = vec![0i16; 320];
        let span = AudioSpan::whole_segment(&seg);
        assert_eq!(span.log, seg.audio_ref.log);
        assert_eq!(span.start_sample, 1_000);
        assert_eq!(span.end_sample, 1_320);
        assert_eq!(span.segments, vec![seg.audio_ref.clone()]);
    }

    #[test]
    fn audio_span_serde_round_trips() {
        let span = AudioSpan {
            log: "pod-x_0.framelog".into(),
            start_sample: 4,
            end_sample: 64,
            segments: vec![
                SegmentRef {
                    log: "pod-x_0.framelog".into(),
                    segment_id: 7,
                    part: 0,
                },
                SegmentRef {
                    log: "pod-x_0.framelog".into(),
                    segment_id: 7,
                    part: 1,
                },
            ],
        };
        let json = serde_json::to_string(&span).unwrap();
        assert_eq!(serde_json::from_str::<AudioSpan>(&json).unwrap(), span);
    }

    // ── AudioSpan::resolve ────────────────────────────────────────────────

    use pod_ingest::test_fixtures::{audio, hello, seg_end, seg_start, write_log};
    use pod_ingest::SpliceStop;

    #[test]
    fn multi_part_span_resolves_seamless_across_part_boundary() {
        // A cap-rolled span whose two parts share the one framelog: the
        // absolute-index splice must yield seamless audio across the part
        // boundary, proving `part` no longer addresses a separate file.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pod-x_7.framelog");
        write_log(
            &path,
            &[
                hello("pod-x"),
                seg_start(7, 0),
                audio(7, 0, 320),
                audio(7, 320, 320),
                seg_end(7, 640),
            ],
        );
        let span = AudioSpan {
            log: "pod-x_7.framelog".into(),
            start_sample: 0,
            end_sample: 640,
            segments: vec![
                SegmentRef {
                    log: "pod-x_7.framelog".into(),
                    segment_id: 7,
                    part: 0,
                },
                SegmentRef {
                    log: "pod-x_7.framelog".into(),
                    segment_id: 7,
                    part: 1,
                },
            ],
        };
        let resolved = span.resolve(dir.path()).unwrap();
        assert_eq!(resolved.covered_samples, 640);
        assert!(resolved.pruned.is_empty());
        assert!(resolved.stopped.is_empty());
        assert_eq!(resolved.pcm[0], 1);
        assert_eq!(resolved.pcm[319], 320);
        assert_eq!(resolved.pcm[320], 1); // second part restarts the ramp
        assert_eq!(resolved.pcm[639], 320);
    }

    #[test]
    fn span_across_two_logs_splices_both() {
        let dir = tempfile::tempdir().unwrap();
        let log_a = "pod-x_a.framelog";
        let log_b = "pod-x_b.framelog";
        write_log(
            &dir.path().join(log_a),
            &[
                hello("pod-x"),
                seg_start(1, 0),
                audio(1, 0, 100),
                seg_end(1, 100),
            ],
        );
        write_log(
            &dir.path().join(log_b),
            &[
                hello("pod-x"),
                seg_start(2, 100),
                audio(2, 100, 100),
                seg_end(2, 100),
            ],
        );
        let span = AudioSpan {
            log: log_a.into(),
            start_sample: 0,
            end_sample: 200,
            segments: vec![
                SegmentRef {
                    log: log_a.into(),
                    segment_id: 1,
                    part: 0,
                },
                SegmentRef {
                    log: log_b.into(),
                    segment_id: 2,
                    part: 0,
                },
            ],
        };
        let resolved = span.resolve(dir.path()).unwrap();
        assert_eq!(resolved.covered_samples, 200);
        assert!(resolved.pruned.is_empty());
        assert_eq!(resolved.pcm[0], 1);
        assert_eq!(resolved.pcm[100], 1);
        assert_eq!(resolved.pcm[199], 100);
    }

    /// An `Audio` frame whose samples are `value_base + 1 ..= value_base + n`,
    /// so two logs covering the same absolute range are distinguishable —
    /// `test_fixtures::audio` always ramps from `1`, which two overlapping
    /// logs would produce identically.
    fn audio_at(
        segment_id: u32,
        first: u64,
        value_base: i16,
        n: usize,
    ) -> audio_pipeline::wire::StreamFrame {
        use heapless::Vec as HVec;

        let mut pcm: HVec<u8, { audio_pipeline::wire::MAX_AUDIO_PAYLOAD }> = HVec::new();
        for i in 0..n {
            let v = (value_base + i as i16 + 1).to_le_bytes();
            pcm.push(v[0]).unwrap();
            pcm.push(v[1]).unwrap();
        }
        audio_pipeline::wire::StreamFrame::Audio(audio_pipeline::wire::AudioFrame {
            segment_id,
            first_sample_index: first,
            device_ts_us: 0,
            pcm,
        })
    }

    #[test]
    fn span_across_two_logs_with_overlap_last_write_wins() {
        // Two logs both cover [0, 100): a resumed segment re-sending a range.
        // Step-3 order (`self.log` first, then `segments` in order) means
        // `log_a` is opened first and `log_b` second, so `log_b`'s values
        // must win the overlap.
        let dir = tempfile::tempdir().unwrap();
        let log_a = "pod-x_a.framelog";
        let log_b = "pod-x_b.framelog";
        write_log(
            &dir.path().join(log_a),
            &[
                hello("pod-x"),
                seg_start(1, 0),
                audio_at(1, 0, 0, 100),
                seg_end(1, 100),
            ],
        );
        write_log(
            &dir.path().join(log_b),
            &[
                hello("pod-x"),
                seg_start(2, 0),
                audio_at(2, 0, 1_000, 100),
                seg_end(2, 100),
            ],
        );
        let span = AudioSpan {
            log: log_a.into(),
            start_sample: 0,
            end_sample: 100,
            segments: vec![
                SegmentRef {
                    log: log_a.into(),
                    segment_id: 1,
                    part: 0,
                },
                SegmentRef {
                    log: log_b.into(),
                    segment_id: 2,
                    part: 0,
                },
            ],
        };
        let resolved = span.resolve(dir.path()).unwrap();
        assert!(resolved.pruned.is_empty());
        // `log_b`'s values (base 1000) overwrite `log_a`'s (base 0) at every
        // overlapping index.
        assert_eq!(resolved.pcm[0], 1_001);
        assert_eq!(resolved.pcm[99], 1_100);
        // Each log's full write is counted, so the sum exceeds `pcm.len()` —
        // the documented upper-bound behavior for overlapping coverage.
        assert_eq!(resolved.covered_samples, 200);
        assert!(resolved.covered_samples > resolved.pcm.len() as u64);
    }

    #[test]
    fn pruned_ref_log_is_silence_and_listed() {
        let dir = tempfile::tempdir().unwrap();
        let span = AudioSpan {
            log: "gone.framelog".into(),
            start_sample: 0,
            end_sample: 64,
            segments: vec![SegmentRef {
                log: "gone.framelog".into(),
                segment_id: 7,
                part: 0,
            }],
        };
        let resolved = span.resolve(dir.path()).unwrap();
        assert_eq!(resolved.covered_samples, 0);
        assert_eq!(resolved.pcm, vec![0i16; 64]);
        assert_eq!(
            resolved.pruned,
            vec![SegmentRef {
                log: "gone.framelog".into(),
                segment_id: 7,
                part: 0,
            }]
        );
    }

    #[test]
    fn pruned_log_shared_by_multiple_refs_lists_every_ref() {
        // A cap-rolled span whose parts all share one (pruned) log: every
        // covering ref must be reported, not just the first.
        let dir = tempfile::tempdir().unwrap();
        let refs = vec![
            SegmentRef {
                log: "gone.framelog".into(),
                segment_id: 7,
                part: 0,
            },
            SegmentRef {
                log: "gone.framelog".into(),
                segment_id: 7,
                part: 1,
            },
        ];
        let span = AudioSpan {
            log: "gone.framelog".into(),
            start_sample: 0,
            end_sample: 64,
            segments: refs.clone(),
        };
        let resolved = span.resolve(dir.path()).unwrap();
        assert_eq!(resolved.pruned, refs);
    }

    #[test]
    fn empty_segments_present_self_log_recovers_audio() {
        // A span carved before any covering segment closed: `segments` is
        // empty, but `self.log` is still read and its audio recovered.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pod-x_9.framelog");
        write_log(&path, &[hello("pod-x"), seg_start(9, 0), audio(9, 0, 64)]);
        let span = AudioSpan {
            log: "pod-x_9.framelog".into(),
            start_sample: 0,
            end_sample: 64,
            segments: vec![],
        };
        let resolved = span.resolve(dir.path()).unwrap();
        assert_eq!(resolved.covered_samples, 64);
        assert!(resolved.pruned.is_empty());
        assert_eq!(resolved.pcm[0], 1);
    }

    #[test]
    fn empty_segments_pruned_self_log_is_all_silence() {
        let dir = tempfile::tempdir().unwrap();
        let span = AudioSpan {
            log: "gone.framelog".into(),
            start_sample: 0,
            end_sample: 32,
            segments: vec![],
        };
        let resolved = span.resolve(dir.path()).unwrap();
        assert_eq!(resolved.covered_samples, 0);
        assert_eq!(resolved.pcm, vec![0i16; 32]);
        assert_eq!(
            resolved.pruned,
            vec![SegmentRef {
                log: "gone.framelog".into(),
                segment_id: 0,
                part: 0,
            }]
        );
    }

    #[test]
    fn faulting_log_among_refs_aborts_with_no_partial_result() {
        // The outage-is-never-silence guarantee, tested at the span layer:
        // one covering log is unreadable (a store fault, not a prune), so
        // resolve must return `Err`, never a partial `ResolvedSpanAudio`.
        let dir = tempfile::tempdir().unwrap();
        let bad = dir.path().join("bad.framelog");
        std::fs::write(&bad, b"XXXX\x01\x00\x00\x00").unwrap();
        let span = AudioSpan {
            log: "bad.framelog".into(),
            start_sample: 0,
            end_sample: 32,
            segments: vec![],
        };
        let err = span.resolve(dir.path()).unwrap_err();
        assert!(matches!(err, SpanResolveError::Resolve { .. }));
    }

    #[test]
    fn torn_tail_log_among_refs_partial_pcm_and_stopped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pod-x_5.framelog");
        write_log(&path, &[hello("pod-x"), seg_start(5, 0), audio(5, 0, 320)]);
        std::io::Write::write_all(
            &mut std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap(),
            &[0xAA, 0xBB, 0xCC],
        )
        .unwrap();

        let span = AudioSpan {
            log: "pod-x_5.framelog".into(),
            start_sample: 0,
            end_sample: 320,
            segments: vec![],
        };
        let resolved = span.resolve(dir.path()).unwrap();
        assert_eq!(resolved.covered_samples, 320);
        assert_eq!(
            resolved.stopped,
            vec![("pod-x_5.framelog".to_string(), SpliceStop::TornTail)]
        );
    }

    #[test]
    fn invalid_span_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let backwards = AudioSpan {
            log: "x.framelog".into(),
            start_sample: 10,
            end_sample: 5,
            segments: vec![],
        };
        assert!(matches!(
            backwards.resolve(dir.path()),
            Err(SpanResolveError::InvalidSpan { start: 10, end: 5 })
        ));

        let oversize = AudioSpan {
            log: "x.framelog".into(),
            start_sample: 0,
            end_sample: MAX_RESOLVE_SAMPLES + 1,
            segments: vec![],
        };
        assert!(matches!(
            oversize.resolve(dir.path()),
            Err(SpanResolveError::InvalidSpan { .. })
        ));
    }

    #[test]
    fn transcript_confidence_serializes_with_expected_keys() {
        // The JSONL observability payload the console tail reads: a present
        // confidence summary serializes under the exact field names the renderer
        // (and downstream JSONL consumers) look up. Pins producer and consumer to
        // the same key strings so a rename can't silently break both at once.
        let t = Transcript {
            text: "hello".into(),
            confidence: Some(TranscriptConfidence {
                avg_logprob: -0.23,
                no_speech_prob: 0.02,
                compression_ratio: 1.4,
                segments: 2,
            }),
        };
        let v = serde_json::to_value(&t).unwrap();
        let c = &v["confidence"];
        assert!(
            (c["avg_logprob"].as_f64().unwrap() - (-0.23)).abs() < 1e-6,
            "{v}"
        );
        assert!(
            (c["no_speech_prob"].as_f64().unwrap() - 0.02).abs() < 1e-6,
            "{v}"
        );
        assert!(
            (c["compression_ratio"].as_f64().unwrap() - 1.4).abs() < 1e-6,
            "{v}"
        );
        assert_eq!(c["segments"].as_u64().unwrap(), 2);

        // A plain-json transcript omits the key entirely (skip_serializing_if).
        let bare = serde_json::to_value(&Transcript {
            text: "hi".into(),
            confidence: None,
        })
        .unwrap();
        assert!(bare.get("confidence").is_none(), "{bare}");
    }

    fn conf_at(no_speech_prob: f32, avg_logprob: f32) -> TranscriptConfidence {
        TranscriptConfidence {
            avg_logprob,
            no_speech_prob,
            compression_ratio: 0.8,
            segments: 1,
        }
    }

    #[test]
    fn confidence_gate_no_speech_boundary() {
        let gate = ConfidenceGate {
            no_speech_max: 0.2,
            avg_logprob_min: None,
        };
        // Strictly above the max rejects, carrying the offending signals.
        let reject = gate.evaluate(&conf_at(0.21, -0.5)).expect("rejected");
        assert_eq!(
            reject,
            GateReject {
                no_speech_prob: 0.21,
                avg_logprob: -0.5,
            }
        );
        // At and below the max pass — the bound is exclusive.
        assert!(gate.evaluate(&conf_at(0.2, -0.5)).is_none());
        assert!(gate.evaluate(&conf_at(0.04, -0.5)).is_none());
    }

    #[test]
    fn confidence_gate_optional_logprob() {
        // no_speech passes; only the logprob gate can fire.
        let off = ConfidenceGate {
            no_speech_max: 0.2,
            avg_logprob_min: None,
        };
        assert!(off.evaluate(&conf_at(0.05, -1.05)).is_none());

        let on = ConfidenceGate {
            no_speech_max: 0.2,
            avg_logprob_min: Some(-0.9),
        };
        // Below the floor rejects; at/above passes (the bound is exclusive).
        assert!(on.evaluate(&conf_at(0.05, -0.99)).is_some());
        assert!(on.evaluate(&conf_at(0.05, -0.9)).is_none());
        assert!(on.evaluate(&conf_at(0.05, -0.28)).is_none());
    }

    #[test]
    fn confidence_gate_either_bound_rejects() {
        // OR semantics: a no_speech hit alone rejects even when logprob is healthy.
        let gate = ConfidenceGate {
            no_speech_max: 0.2,
            avg_logprob_min: Some(-1.5),
        };
        assert!(gate.evaluate(&conf_at(0.37, -0.28)).is_some());
    }

    #[test]
    fn confidence_gate_nan_signals_fail_open() {
        // A NaN confidence field must never mis-gate: every comparison against NaN
        // is false, so the gate fails open (no reject) rather than fire on garbage.
        let gate = ConfidenceGate {
            no_speech_max: 0.2,
            avg_logprob_min: Some(-0.9),
        };
        assert!(gate.evaluate(&conf_at(f32::NAN, -0.5)).is_none());
        assert!(gate.evaluate(&conf_at(0.05, f32::NAN)).is_none());
    }

    #[test]
    fn speak_body_pcm_serializes_via_rc_feature() {
        let cmd = SpeakCmd {
            target: PodId("pod-x".into()),
            in_reply_to: Some(UtteranceId(1)),
            body: SpeakBody::Pcm(Arc::from(vec![1i16, -2, 3].as_slice())),
            interruptible: true,
            timings: StageTimings::default(),
        };
        // Serializes at all only because serde's `rc` feature is enabled.
        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("[1,-2,3]"), "got {json}");
    }

    #[test]
    fn stage_delta_us_counts_a_backward_clock_step() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let clamps = AtomicU64::new(0);
        // `later` precedes `earlier` (NTP stepped the clock backward): the delta
        // clamps to 0 and the clamp is counted, so a suspicious zero is
        // corroborable rather than read as genuine instant latency.
        let d = stage_delta_us(Some(HostMicros(5_000)), Some(HostMicros(1_000)), &clamps);
        assert_eq!(d, Some(0));
        assert_eq!(clamps.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn stage_delta_us_forward_delta_does_not_count() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let clamps = AtomicU64::new(0);
        let d = stage_delta_us(Some(HostMicros(1_000)), Some(HostMicros(5_000)), &clamps);
        assert_eq!(d, Some(4_000));
        assert_eq!(clamps.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn stage_delta_us_absent_stamp_is_none_and_uncounted() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let clamps = AtomicU64::new(0);
        assert_eq!(stage_delta_us(None, Some(HostMicros(1)), &clamps), None);
        assert_eq!(stage_delta_us(Some(HostMicros(1)), None, &clamps), None);
        assert_eq!(clamps.load(Ordering::Relaxed), 0);
    }
}
