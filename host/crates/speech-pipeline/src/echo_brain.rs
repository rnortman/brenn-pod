//! `EchoBrain` — the parrot `Brain`. It answers every wake-gated utterance that
//! carries a usable transcript by queueing that same text back as
//! `SpeakBody::Text`; the router's synthesizer renders it to speech. No clip, no
//! transcriber inside the brain: the transcript rides in on the utterance.
//!
//! An utterance with no transcript — absent, or trimmed to nothing (noise reaching
//! a bypassed wake gate legitimately transcribes to `""` or punctuation-only) — is
//! declined here, in the brain, not gated out upstream: a future audio-native brain
//! consumes the raw segment and needs no transcript, so the pipeline must never make
//! a transcript a dispatch precondition. A full or disconnected response sink drops
//! the reply (the utterance's audio still lives in the record store, so a lost
//! readback costs responsiveness, not data). Both cases are made loud via a typed
//! event plus a counter.

use futures::future::BoxFuture;
use futures::FutureExt;
use std::sync::Arc;

use crate::brain::{send_or_report, BrainEvent, BrainEventFn, BrainStats, WakeCommandReason};
use crate::traits::{Brain, ResponseSink};
use crate::types::{
    ContextSegment, InterruptProgress, SpeakBody, SpeakCmd, Utterance, UtteranceId,
};

/// Most words of the interrupted response the readback names back before "…and
/// then you said". A longer clip would otherwise read its own first half aloud.
const READBACK_TAIL_WORDS: usize = 8;

/// The parrot `Brain`: transcript in, the same text back out as `SpeakBody::Text`.
pub struct EchoBrain {
    events: BrainEventFn,
    stats: Arc<BrainStats>,
}

impl EchoBrain {
    /// Build an `EchoBrain` over the shared event sink and counters.
    pub fn new(events: BrainEventFn, stats: Arc<BrainStats>) -> Self {
        Self { events, stats }
    }
}

/// The parrot's barge-in readback: name where the last response was cut, then
/// parrot the new transcript (kept last so the parrot still parrots). A segment
/// with response text reads *I think you interrupted me after "<tail>", and then
/// you said "<transcript>". <transcript>*; a Pcm segment (no words to quote)
/// degrades to the tail-less form.
fn barge_readback(seg: &ContextSegment, transcript: &str) -> String {
    match barge_tail(seg) {
        Some(tail) => format!(
            "I think you interrupted me after \"{tail}\", and then you said \
             \"{transcript}\". {transcript}"
        ),
        None => {
            format!("I think you interrupted me, and then you said \"{transcript}\". {transcript}")
        }
    }
}

/// The last few words of the interrupted response the user actually heard: cut the
/// response text at the heard fraction of its characters, drop a trailing partial
/// word, and keep at most [`READBACK_TAIL_WORDS`]. `None` when there is nothing to
/// quote: a segment with no response text (a Pcm reply), or a cut so early no whole
/// word was heard.
fn barge_tail(seg: &ContextSegment) -> Option<String> {
    let response = seg.response_text.as_deref()?;
    let chars: Vec<char> = response.chars().collect();
    let total = seg.interrupted.total_ms.max(1);
    let heard = seg.interrupted.heard_ms.min(total);
    let cut = (chars.len() as u128 * heard as u128 / total as u128) as usize;
    // A cut that lands inside a word would quote half of it; drop that word.
    let mid_word = cut > 0
        && cut < chars.len()
        && !chars[cut - 1].is_whitespace()
        && !chars[cut].is_whitespace();
    let heard_prefix: String = chars[..cut].iter().collect();
    let mut words: Vec<&str> = heard_prefix.split_whitespace().collect();
    if mid_word {
        words.pop();
    }
    let start = words.len().saturating_sub(READBACK_TAIL_WORDS);
    let tail = words[start..].join(" ");
    // A cut landing before the first whole word (a barge at the very start of the
    // response, or a near-zero heard estimate) leaves nothing to quote; degrade to
    // the tail-less readback rather than speaking an empty quote.
    (!tail.is_empty()).then_some(tail)
}

impl Brain for EchoBrain {
    fn handle(&self, u: Utterance, mut out: ResponseSink) -> BoxFuture<'static, ()> {
        let utterance = u.id;
        let text = u
            .transcript
            .as_ref()
            .map(|t| t.text.trim())
            .filter(|t| !t.is_empty());

        match text {
            Some(text) => {
                // A barge-in utterance reads back where it cut the last response
                // before parroting the new transcript; a plain utterance just
                // parrots. The chain is non-empty by construction when present, so
                // `last` is the turn this speech interrupted.
                let reply = match u.barge_in.as_ref().and_then(|b| b.chain.last()) {
                    Some(seg) => barge_readback(seg, text),
                    None => text.to_string(),
                };
                let cmd = SpeakCmd {
                    target: u.pod,
                    in_reply_to: Some(u.id),
                    body: SpeakBody::Text(reply),
                    interruptible: true,
                    timings: u.timings.clone(),
                };
                send_or_report(&mut out, cmd, utterance, &self.events, &self.stats);
            }
            None => match u.wake {
                // A scored wake accept with nothing to echo: the wake word fired
                // but no command followed. Its own non-failure category, carrying
                // the wake context and segment reference for retro-transcription.
                Some(w) => {
                    (self.events)(BrainEvent::wake_command_absent(
                        utterance,
                        u.audio_ref,
                        &w,
                        WakeCommandReason::Empty,
                    ));
                    self.stats.record_wake_command_absent();
                }
                // A bypassed gate transcribing to nothing: legitimate noise, the
                // pre-existing declined-for-no-transcript path.
                None => {
                    (self.events)(BrainEvent::NoTranscript { utterance });
                    self.stats.record_no_transcript();
                }
            },
        }
        // The reply is queued synchronously above; nothing remains to await.
        futures::future::ready(()).boxed()
    }

    fn interrupt(&self, _id: UtteranceId, _progress: InterruptProgress) {
        // No-op: the barge-in state the parrot reads back rides the next
        // utterance's context chain, so there is nothing to record here.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use futures::channel::mpsc;
    use pod_ingest::SegmentRef;

    use crate::types::{
        AudioSpan, DoaTrack, PodId, RoomId, StageTimings, Transcript, WakeConfirmation,
    };

    /// The audio-span reference shared by the test utterance and the events it is
    /// expected to emit, so equality assertions compare identical spans.
    fn test_audio_span() -> AudioSpan {
        AudioSpan {
            log: "pod-x_0.framelog".into(),
            start_sample: 0,
            end_sample: 16,
            segments: vec![SegmentRef {
                log: "pod-x_0.framelog".into(),
                segment_id: 3,
                part: 0,
            }],
        }
    }

    /// A `BrainEventFn` that collects emitted events for assertion.
    fn event_collector() -> (BrainEventFn, Arc<Mutex<Vec<BrainEvent>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let f: BrainEventFn = Arc::new(move |e| sink.lock().unwrap().push(e));
        (f, seen)
    }

    fn utterance_with(transcript: Option<&str>) -> Utterance {
        utterance_full(transcript, None)
    }

    fn utterance_full(transcript: Option<&str>, wake: Option<WakeConfirmation>) -> Utterance {
        Utterance {
            id: UtteranceId(42),
            pod: PodId("pod-x".into()),
            room: RoomId("kitchen".into()),
            speaker: None,
            doa: DoaTrack::default(),
            audio_ref: test_audio_span(),
            transcript: transcript.map(|text| Transcript {
                text: text.into(),
                confidence: None,
            }),
            timings: StageTimings::default(),
            endpoint_cause: crate::types::EndpointCause::SoftEndpoint,
            wake,
            barge_in: None,
        }
    }

    #[test]
    fn echoes_the_transcript_as_text() {
        let (events, seen) = event_collector();
        let stats = Arc::new(BrainStats::default());
        let brain = EchoBrain::new(events, Arc::clone(&stats));

        let (tx, mut rx) = mpsc::channel::<SpeakCmd>(1);
        drop(brain.handle(utterance_with(Some("hello there")), ResponseSink::new(tx)));

        let cmd = rx.try_recv().expect("a SpeakCmd was queued");
        assert_eq!(cmd.target, PodId("pod-x".into()));
        assert_eq!(cmd.in_reply_to, Some(UtteranceId(42)));
        assert!(cmd.interruptible);
        match cmd.body {
            SpeakBody::Text(text) => assert_eq!(text, "hello there"),
            SpeakBody::Pcm(_) => panic!("EchoBrain queues Text, never Pcm"),
        }
        assert!(seen.lock().unwrap().is_empty(), "no failure on a live sink");
        assert_eq!(stats.snapshot().no_transcript, 0);
        assert_eq!(stats.snapshot().speak_send_failures, 0);
    }

    #[test]
    fn trims_surrounding_whitespace_before_echoing() {
        let (events, _seen) = event_collector();
        let stats = Arc::new(BrainStats::default());
        let brain = EchoBrain::new(events, stats);

        let (tx, mut rx) = mpsc::channel::<SpeakCmd>(1);
        drop(brain.handle(
            utterance_with(Some("  spaced out  ")),
            ResponseSink::new(tx),
        ));

        let cmd = rx.try_recv().expect("a SpeakCmd was queued");
        match cmd.body {
            SpeakBody::Text(text) => assert_eq!(text, "spaced out"),
            SpeakBody::Pcm(_) => panic!("EchoBrain queues Text"),
        }
    }

    #[test]
    fn absent_transcript_declines_and_counts() {
        let (events, seen) = event_collector();
        let stats = Arc::new(BrainStats::default());
        let brain = EchoBrain::new(events, Arc::clone(&stats));

        let (tx, mut rx) = mpsc::channel::<SpeakCmd>(1);
        drop(brain.handle(utterance_with(None), ResponseSink::new(tx)));

        assert!(rx.try_recv().is_err(), "no reply for an absent transcript");
        assert_eq!(
            *seen.lock().unwrap(),
            vec![BrainEvent::NoTranscript {
                utterance: UtteranceId(42)
            }]
        );
        assert_eq!(stats.snapshot().no_transcript, 1);
    }

    #[test]
    fn whitespace_only_transcript_declines_and_counts() {
        let (events, seen) = event_collector();
        let stats = Arc::new(BrainStats::default());
        let brain = EchoBrain::new(events, Arc::clone(&stats));

        let (tx, mut rx) = mpsc::channel::<SpeakCmd>(1);
        drop(brain.handle(utterance_with(Some("   \t ")), ResponseSink::new(tx)));

        assert!(rx.try_recv().is_err(), "no reply for a blank transcript");
        assert_eq!(
            *seen.lock().unwrap(),
            vec![BrainEvent::NoTranscript {
                utterance: UtteranceId(42)
            }]
        );
        assert_eq!(stats.snapshot().no_transcript, 1);
    }

    #[test]
    fn wake_positive_empty_transcript_is_command_absent_not_no_transcript() {
        let (events, seen) = event_collector();
        let stats = Arc::new(BrainStats::default());
        let brain = EchoBrain::new(events, Arc::clone(&stats));

        let wake = Some(WakeConfirmation {
            score: 0.998,
            wake_end_sample: 39_040,
            stt_trim_samples: 35_840,
        });
        let (tx, mut rx) = mpsc::channel::<SpeakCmd>(1);
        drop(brain.handle(utterance_full(Some("   "), wake), ResponseSink::new(tx)));

        assert!(rx.try_recv().is_err(), "no reply for an empty command");
        assert_eq!(
            *seen.lock().unwrap(),
            vec![BrainEvent::WakeCommandAbsent {
                utterance: UtteranceId(42),
                audio_ref: test_audio_span(),
                score: 0.998,
                wake_end_sample: 39_040,
                stt_trim_samples: 35_840,
                reason: WakeCommandReason::Empty,
            }]
        );
        // Routed to its own non-failure counter, never the generic error path.
        assert_eq!(stats.snapshot().wake_command_absent, 1);
        assert_eq!(stats.snapshot().no_transcript, 0);
    }

    #[test]
    fn bypassed_empty_transcript_stays_no_transcript() {
        // No wake provenance (bypassed gate): an empty transcript is legitimate
        // noise, the generic declined-for-no-transcript path — not a wake-no-follow.
        let (events, seen) = event_collector();
        let stats = Arc::new(BrainStats::default());
        let brain = EchoBrain::new(events, Arc::clone(&stats));

        let (tx, _rx) = mpsc::channel::<SpeakCmd>(1);
        drop(brain.handle(utterance_full(Some(""), None), ResponseSink::new(tx)));

        assert_eq!(
            *seen.lock().unwrap(),
            vec![BrainEvent::NoTranscript {
                utterance: UtteranceId(42)
            }]
        );
        assert_eq!(stats.snapshot().no_transcript, 1);
        assert_eq!(stats.snapshot().wake_command_absent, 0);
    }

    #[test]
    fn wake_positive_with_a_real_transcript_still_echoes() {
        // Wake provenance present but a usable transcript wins: the command-absent
        // split only fires on an empty transcript.
        let (events, seen) = event_collector();
        let stats = Arc::new(BrainStats::default());
        let brain = EchoBrain::new(events, Arc::clone(&stats));

        let wake = Some(WakeConfirmation {
            score: 0.9,
            wake_end_sample: 16_000,
            stt_trim_samples: 12_800,
        });
        let (tx, mut rx) = mpsc::channel::<SpeakCmd>(1);
        drop(brain.handle(
            utterance_full(Some("turn on the lights"), wake),
            ResponseSink::new(tx),
        ));

        let cmd = rx.try_recv().expect("a SpeakCmd was queued");
        match cmd.body {
            SpeakBody::Text(text) => assert_eq!(text, "turn on the lights"),
            SpeakBody::Pcm(_) => panic!("EchoBrain queues Text"),
        }
        assert!(seen.lock().unwrap().is_empty());
        assert_eq!(stats.snapshot().wake_command_absent, 0);
        assert_eq!(stats.snapshot().no_transcript, 0);
    }

    #[test]
    fn full_sink_emits_event_and_counts() {
        // A fresh per-clone sender always has its one guaranteed slot, so to see a
        // full sink both the buffer (0) and that slot must be exhausted first.
        let (mut tx, _rx) = mpsc::channel::<SpeakCmd>(0);
        tx.try_send(SpeakCmd {
            target: PodId("filler".into()),
            in_reply_to: None,
            body: SpeakBody::Text(String::new()),
            interruptible: false,
            timings: StageTimings::default(),
        })
        .expect("the sender's one guaranteed slot accepts the first send");

        let (events, seen) = event_collector();
        let stats = Arc::new(BrainStats::default());
        let brain = EchoBrain::new(events, Arc::clone(&stats));

        drop(brain.handle(utterance_with(Some("hi")), ResponseSink::new(tx)));

        assert_eq!(
            *seen.lock().unwrap(),
            vec![BrainEvent::SinkFull {
                utterance: UtteranceId(42)
            }]
        );
        assert_eq!(stats.snapshot().speak_send_failures, 1);
        assert_eq!(stats.snapshot().no_transcript, 0);
    }

    fn barge_utterance(transcript: &str, chain: Vec<ContextSegment>) -> Utterance {
        use crate::types::BargeInContext;
        Utterance {
            barge_in: Some(BargeInContext {
                chain: chain.into(),
            }),
            ..utterance_with(Some(transcript))
        }
    }

    fn segment(response: Option<&str>, heard_ms: u64, total_ms: u64) -> ContextSegment {
        ContextSegment {
            utterance: UtteranceId(7),
            transcript: Some("earlier question".into()),
            response_text: response.map(String::from),
            interrupted: InterruptProgress { heard_ms, total_ms },
        }
    }

    fn only_reply(brain: &EchoBrain, u: Utterance) -> String {
        let (tx, mut rx) = mpsc::channel::<SpeakCmd>(1);
        drop(brain.handle(u, ResponseSink::new(tx)));
        match rx.try_recv().expect("a SpeakCmd was queued").body {
            SpeakBody::Text(text) => text,
            SpeakBody::Pcm(_) => panic!("EchoBrain queues Text"),
        }
    }

    #[test]
    fn barge_reads_back_the_cut_response_then_parrots() {
        let (events, _seen) = event_collector();
        let brain = EchoBrain::new(events, Arc::new(BrainStats::default()));

        // Heard half of a four-word response: the tail is the heard prefix, the
        // new transcript is quoted and then parroted at the end.
        let u = barge_utterance(
            "no cancel that",
            vec![segment(Some("the weather today is sunny"), 500, 1_000)],
        );
        assert_eq!(
            only_reply(&brain, u),
            "I think you interrupted me after \"the weather\", and then you said \
             \"no cancel that\". no cancel that"
        );
    }

    #[test]
    fn barge_readback_caps_the_tail_at_eight_words() {
        let (events, _seen) = event_collector();
        let brain = EchoBrain::new(events, Arc::new(BrainStats::default()));

        // The whole ten-word response was heard before the barge; only the last
        // eight words are read back, so a long clip never recites its own opening.
        let response = "one two three four five six seven eight nine ten";
        let u = barge_utterance("stop", vec![segment(Some(response), 100, 100)]);
        assert_eq!(
            only_reply(&brain, u),
            "I think you interrupted me after \"three four five six seven eight nine ten\", \
             and then you said \"stop\". stop"
        );
    }

    #[test]
    fn barge_on_a_pcm_response_degrades_to_the_tailless_form() {
        let (events, _seen) = event_collector();
        let brain = EchoBrain::new(events, Arc::new(BrainStats::default()));

        // A Pcm reply has no words to quote; the readback drops the "after" clause.
        let u = barge_utterance("wait", vec![segment(None, 300, 1_000)]);
        assert_eq!(
            only_reply(&brain, u),
            "I think you interrupted me, and then you said \"wait\". wait"
        );
    }

    #[test]
    fn barge_with_nothing_heard_degrades_to_the_tailless_form() {
        let (events, _seen) = event_collector();
        let brain = EchoBrain::new(events, Arc::new(BrainStats::default()));

        // The barge landed at the very start of the response: no whole word was
        // heard, so there is nothing to quote and the readback drops the "after"
        // clause rather than speaking an empty quote.
        let u = barge_utterance("stop", vec![segment(Some("the weather today"), 0, 1_000)]);
        assert_eq!(
            only_reply(&brain, u),
            "I think you interrupted me, and then you said \"stop\". stop"
        );
    }

    #[test]
    fn barge_reads_back_only_the_last_chain_segment() {
        let (events, _seen) = event_collector();
        let brain = EchoBrain::new(events, Arc::new(BrainStats::default()));

        // A multi-cycle chain: the readback names the most recent cut, not the first.
        let u = barge_utterance(
            "no the other one",
            vec![
                segment(Some("first response text"), 900, 1_000),
                segment(Some("second response here"), 400, 1_000),
            ],
        );
        assert_eq!(
            only_reply(&brain, u),
            "I think you interrupted me after \"second\", and then you said \
             \"no the other one\". no the other one"
        );
    }

    #[test]
    fn interrupt_is_a_no_op() {
        let (events, seen) = event_collector();
        let stats = Arc::new(BrainStats::default());
        let brain = EchoBrain::new(events, Arc::clone(&stats));

        brain.interrupt(
            UtteranceId(42),
            InterruptProgress {
                heard_ms: 300,
                total_ms: 1_000,
            },
        );

        assert!(seen.lock().unwrap().is_empty());
        assert_eq!(stats.snapshot().no_transcript, 0);
        assert_eq!(stats.snapshot().speak_send_failures, 0);
    }
}
