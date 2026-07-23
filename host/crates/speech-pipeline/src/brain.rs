//! Shared `Brain` observability: the typed event, its emit-sink alias, and the
//! atomic counters every `Brain` implementation reports through. The concrete
//! brains (`WavBrain`, and the `EchoBrain` to come) live in their own modules
//! and share these so `stage_health` and the JSONL adapter see one vocabulary
//! regardless of which brain is wired.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

use crate::traits::ResponseSink;
use crate::types::{AudioSpan, SpeakCmd, UtteranceId, WakeConfirmation};

/// A typed brain event; the speech surface adapts it to JSONL.
///
/// No `Eq`: `WakeCommandAbsent` carries the wake score as an `f32`. `PartialEq`
/// (which `f32` supports) is enough for the tests that compare emitted events. No
/// `Serialize`: the surface hand-builds each JSONL line in `brain_event_adapter`,
/// so a derived shape would be a second, divergent wire format for the same event.
#[derive(Debug, Clone, PartialEq)]
pub enum BrainEvent {
    /// The response sink was full or disconnected; the utterance went unanswered.
    SinkFull { utterance: UtteranceId },
    /// The utterance carried no usable transcript (absent, or whitespace-only), so
    /// a transcript-driven brain declined to answer. Not a failure: noise reaching
    /// a bypassed wake gate legitimately transcribes to nothing.
    NoTranscript { utterance: UtteranceId },
    /// A scored wake accept with no usable command: either the transcript came
    /// back empty, or STT confidence flagged it as a likely hallucination (the
    /// wake word fired on noise and Whisper invented fluent phantom text). Not a
    /// failure — distinct from `NoTranscript` (bypassed-gate noise) and from an
    /// `stt_failed` error. Carries the wake score, the detected wake-phrase end,
    /// the trimmed-sample count, and the audio-span reference so follow-up work can
    /// re-fetch the utterance audio (pre-roll included) for retro-transcription. The
    /// `reason` keeps the two no-command causes distinguishable downstream.
    WakeCommandAbsent {
        utterance: UtteranceId,
        audio_ref: AudioSpan,
        score: f32,
        wake_end_sample: usize,
        stt_trim_samples: usize,
        reason: WakeCommandReason,
    },
    /// A barge-in utterance whose STT confidence tripped the gate: the sustained
    /// speech that cut playback transcribed to likely hallucination, so it is
    /// declined rather than echoed. The barge already cut the audio, so declining
    /// the phantom text is the honest outcome of speech that said nothing. No wake
    /// provenance — a barge carries no wake word — so this carries the barge mark
    /// in its place, plus the audio span for retro-transcription and the offending
    /// confidence signals for the log line.
    BargeCommandAbsent {
        utterance: UtteranceId,
        audio_ref: AudioSpan,
        no_speech_prob: f32,
        avg_logprob: f32,
    },
}

impl BrainEvent {
    /// Build the no-command event for a scored wake accept, packing the wake
    /// context and segment reference and tagging it with `reason`. Shared by the
    /// brain's empty-transcript path and the pipeline's confidence-gate decline so
    /// the two no-command sites pack identical fields and only the reason differs.
    pub fn wake_command_absent(
        utterance: UtteranceId,
        audio_ref: AudioSpan,
        wake: &WakeConfirmation,
        reason: WakeCommandReason,
    ) -> BrainEvent {
        BrainEvent::WakeCommandAbsent {
            utterance,
            audio_ref,
            score: wake.score,
            wake_end_sample: wake.wake_end_sample,
            stt_trim_samples: wake.stt_trim_samples,
            reason,
        }
    }
}

/// Why a scored wake accept produced no command to act on. Rendered onto the
/// JSONL line by `brain_event_adapter`, not by serde — see [`BrainEvent`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WakeCommandReason {
    /// The transcript came back empty or whitespace-only — the wake word fired
    /// with no follow-on speech.
    Empty,
    /// The transcript carried text but STT confidence tripped the gate — a likely
    /// hallucination. Carries the offending signals for the log line.
    LowConfidence {
        no_speech_prob: f32,
        avg_logprob: f32,
    },
    /// The wake armed but its window closed with no utterance passing the policy —
    /// a "wake, no follow": the transport segment ended, a fresh wake replaced the
    /// arm, or the connection reset, with no command to act on. There is no
    /// transcript at all (STT never ran).
    ArmExpired,
}

/// The sink a brain emits its events into. `Arc`'d so one closure serves every
/// call; the surface owns the adapter that turns events into JSONL lines.
pub type BrainEventFn = Arc<dyn Fn(BrainEvent) + Send + Sync>;

/// Shared, atomically-updated brain counters. Read for `stage_health` via
/// [`BrainStats::snapshot`]; the atomics stay private so the synchronization
/// detail never leaks to the emit site (the `WakeStats` idiom).
#[derive(Debug, Default)]
pub struct BrainStats {
    speak_send_failures: AtomicU64,
    no_transcript: AtomicU64,
    wake_command_absent: AtomicU64,
    barge_command_absent: AtomicU64,
}

/// A point-in-time copy of [`BrainStats`], for `stage_health` reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct BrainStatsSnapshot {
    /// Responses dropped because the sink was full or disconnected.
    pub speak_send_failures: u64,
    /// Utterances declined because they carried no usable transcript.
    pub no_transcript: u64,
    /// Scored wake accepts whose transcript came back empty — the wake word fired
    /// with no follow-on command. Deliberately not a failure counter: a follow-up
    /// tool goes back for these segments; it is not an error rate to alarm on.
    pub wake_command_absent: u64,
    /// Barge-in utterances declined because STT confidence flagged the barging
    /// speech as likely hallucination. Not a failure: the playback was already
    /// cut, and declining the phantom text is the honest outcome.
    pub barge_command_absent: u64,
}

impl BrainStats {
    /// Count a response dropped because its sink was full or disconnected.
    pub fn record_send_failure(&self) {
        self.speak_send_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Count an utterance declined for lack of a usable transcript.
    pub fn record_no_transcript(&self) {
        self.no_transcript.fetch_add(1, Ordering::Relaxed);
    }

    /// Count a scored wake accept whose transcript came back empty.
    pub fn record_wake_command_absent(&self) {
        self.wake_command_absent.fetch_add(1, Ordering::Relaxed);
    }

    /// Count a barge-in utterance declined for tripping the confidence gate.
    pub fn record_barge_command_absent(&self) {
        self.barge_command_absent.fetch_add(1, Ordering::Relaxed);
    }

    /// A `Copy` snapshot of the counters, read for `stage_health`.
    pub fn snapshot(&self) -> BrainStatsSnapshot {
        BrainStatsSnapshot {
            speak_send_failures: self.speak_send_failures.load(Ordering::Relaxed),
            no_transcript: self.no_transcript.load(Ordering::Relaxed),
            wake_command_absent: self.wake_command_absent.load(Ordering::Relaxed),
            barge_command_absent: self.barge_command_absent.load(Ordering::Relaxed),
        }
    }
}

/// Queue a brain's response, reporting a full or disconnected sink through the
/// shared event plus counter. Every `Brain` routes its send through here so the
/// sink-failure contract stays identical across implementations.
pub(crate) fn send_or_report(
    out: &mut ResponseSink,
    cmd: SpeakCmd,
    utterance: UtteranceId,
    events: &BrainEventFn,
    stats: &BrainStats,
) {
    if out.try_send(cmd).is_err() {
        (*events)(BrainEvent::SinkFull { utterance });
        stats.record_send_failure();
    }
}
