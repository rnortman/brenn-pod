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
    /// the gate declined it and no brain was called. Not a failure: noise reaching
    /// a bypassed wake gate legitimately transcribes to nothing, and an STT attempt
    /// that failed leaves the same absent transcript (`stt_failed` names that
    /// cause separately).
    ///
    /// A reader is entitled to conclude that this utterance produced no turn: no
    /// `brain_dispatched` follows it and nothing was spoken. The JSONL name
    /// `brain_no_transcript` is kept for readers that key on it. A *wake-gated*
    /// empty is never reported here — it is a `WakeCommandAbsent` with
    /// `WakeCommandReason::Empty`, under every brain.
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
    /// A wake-less utterance whose STT confidence tripped the gate: speech heard
    /// without a wake word transcribed to likely hallucination, so it is declined
    /// rather than echoed. No wake provenance — neither provenance carries a wake
    /// word — so this carries the audio span for retro-transcription and the
    /// offending confidence signals in its place.
    ///
    /// `follow_up` says which wake-less provenance it was, and a reader is
    /// entitled to act on that distinction: `false` is a barge, speech that cut an
    /// audible reply, and the repair is the barge thresholds or the echo path;
    /// `true` is speech inside a `<listen/>` capture window, nothing was
    /// interrupted, and the repair is the confidence gate or the room. A
    /// deployment where nothing can barge reads every one of these as `true`.
    ///
    /// Speech that began inside a window and then cut the reply it drew is
    /// reported `false`: the cut is what the reader is entitled to act on, and
    /// the barge thresholds are its repair. `true` is reserved for a window's
    /// speech that cut nothing.
    BargeCommandAbsent {
        utterance: UtteranceId,
        audio_ref: AudioSpan,
        no_speech_prob: f32,
        avg_logprob: f32,
        follow_up: bool,
    },
    /// An utterance carved over the pod's own playback without cutting it, whose
    /// STT confidence tripped the gate: the residual of a reply leaking back
    /// through the mic, declined rather than answered. Nothing was interrupted
    /// and the reply is still playing or has played to its end, so unlike a barge
    /// this reaches no brain hook — it is a record and a counter.
    EchoDeclined {
        utterance: UtteranceId,
        audio_ref: AudioSpan,
        no_speech_prob: f32,
        avg_logprob: f32,
    },
    /// A brain that answers over a link could not hand the utterance to its peer;
    /// the turn is over with nothing said (beyond a configured spoken fallback).
    /// `detail` is the transport's own rendering of the refusal.
    LinkPublishFailed {
        utterance: UtteranceId,
        detail: String,
    },
    /// A brain that answers over a link waited out its response window. With
    /// `continuation: false` no response arrived at all; with `continuation: true` a
    /// promised follow-up segment never came after part of the response was already
    /// spoken, so the user heard a truncated answer.
    LinkResponseTimeout {
        utterance: UtteranceId,
        waited_ms: u64,
        continuation: bool,
    },
    /// A response carried a tag-shaped island the codec does not know (an
    /// unrecognized name, or one mangled past parsing). It was stripped rather than
    /// spoken. Loud by contract: it means the peer's vocabulary and ours have
    /// diverged. `tag` is the raw text as it appeared.
    LinkTagStripped { utterance: UtteranceId, tag: String },
    /// A response carried no turn-correlation marker and was accepted for the one
    /// pending turn anyway. Benign — with a single pending slot there is only one
    /// turn it could belong to — but worth seeing.
    LinkReplyAssumed { utterance: UtteranceId },
    /// A response chain kept promising continuations past the safety bound. The
    /// capping segment was spoken and the turn ended as if it were terminal.
    LinkContinuationCapped {
        utterance: UtteranceId,
        segments: u32,
    },
}

impl BrainEvent {
    /// Build the no-command event for a scored wake accept, packing the wake
    /// context and segment reference and tagging it with `reason`. Shared by the
    /// gate's empty-transcript and confidence declines so the two no-command sites
    /// pack identical fields and only the reason differs.
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
    /// with no follow-on speech, or STT failed and left no transcript at all.
    /// The one report for a wake-gated empty, whatever brain is configured: the
    /// gate decides it, so no brain's own vocabulary can vary it.
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
    echo_declined: AtomicU64,
    link_publish_failures: AtomicU64,
    link_response_timeouts: AtomicU64,
    link_tags_stripped: AtomicU64,
    link_replies_assumed: AtomicU64,
    link_continuations_capped: AtomicU64,
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
    /// Wake-less utterances — barges, and follow-ups inside a `<listen/>` window
    /// — declined because STT confidence flagged the speech as likely
    /// hallucination. Not a failure: nothing usable was said, and declining the
    /// phantom text is the honest outcome. Which provenance a given decline had
    /// is on its event's `follow_up`, not in this total.
    pub barge_command_absent: u64,
    /// Utterances carved over the pod's own playback and declined because STT
    /// confidence flagged them as likely hallucination — the residual of a reply
    /// leaking back through the mic. Not a failure: nothing was interrupted, and
    /// a count that tracks the replies is the leak rate showing itself.
    pub echo_declined: u64,
    /// Turns whose utterance the link refused to carry to the peer.
    pub link_publish_failures: u64,
    /// Turns that waited out a response window — initial or continuation.
    pub link_response_timeouts: u64,
    /// Tag-shaped islands stripped from a response because the codec did not know
    /// them. A nonzero count means the peer's tag vocabulary has drifted from ours.
    pub link_tags_stripped: u64,
    /// Responses accepted for the pending turn despite carrying no correlation
    /// marker. Not a failure counter — the reply policy is deliberately optimistic.
    pub link_replies_assumed: u64,
    /// Response chains cut off at the continuation safety bound.
    pub link_continuations_capped: u64,
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

    /// Count a wake-less utterance — a barge or a follow-up — declined for
    /// tripping the confidence gate.
    pub fn record_barge_command_absent(&self) {
        self.barge_command_absent.fetch_add(1, Ordering::Relaxed);
    }

    /// Count an utterance over the pod's own playback declined by the gate.
    pub fn record_echo_declined(&self) {
        self.echo_declined.fetch_add(1, Ordering::Relaxed);
    }

    /// Count a turn whose utterance the link refused to carry.
    pub fn record_link_publish_failure(&self) {
        self.link_publish_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Count a turn that waited out a response window, initial or continuation.
    pub fn record_link_response_timeout(&self) {
        self.link_response_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    /// Count a tag-shaped island stripped from a response as unknown or mangled.
    pub fn record_link_tag_stripped(&self) {
        self.link_tags_stripped.fetch_add(1, Ordering::Relaxed);
    }

    /// Count a response accepted for the pending turn with no correlation marker.
    pub fn record_link_reply_assumed(&self) {
        self.link_replies_assumed.fetch_add(1, Ordering::Relaxed);
    }

    /// Count a response chain cut off at the continuation safety bound.
    pub fn record_link_continuation_capped(&self) {
        self.link_continuations_capped
            .fetch_add(1, Ordering::Relaxed);
    }

    /// A `Copy` snapshot of the counters, read for `stage_health`.
    pub fn snapshot(&self) -> BrainStatsSnapshot {
        BrainStatsSnapshot {
            speak_send_failures: self.speak_send_failures.load(Ordering::Relaxed),
            no_transcript: self.no_transcript.load(Ordering::Relaxed),
            wake_command_absent: self.wake_command_absent.load(Ordering::Relaxed),
            barge_command_absent: self.barge_command_absent.load(Ordering::Relaxed),
            echo_declined: self.echo_declined.load(Ordering::Relaxed),
            link_publish_failures: self.link_publish_failures.load(Ordering::Relaxed),
            link_response_timeouts: self.link_response_timeouts.load(Ordering::Relaxed),
            link_tags_stripped: self.link_tags_stripped.load(Ordering::Relaxed),
            link_replies_assumed: self.link_replies_assumed.load(Ordering::Relaxed),
            link_continuations_capped: self.link_continuations_capped.load(Ordering::Relaxed),
        }
    }
}

/// Queue a brain's response, reporting a full or disconnected sink through the
/// shared event plus counter. Every reply producer — each `Brain`, and the
/// surface's offline reply — routes its send through here so the sink-failure
/// contract stays identical across them.
pub fn send_or_report(
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
