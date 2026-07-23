//! `WavBrain` — the first `Brain` implementation. It answers every wake-gated
//! utterance with one fixed, pre-loaded PCM clip queued as `SpeakBody::Pcm`. No
//! transcriber, no synthesizer: it exercises the brain→playback plumbing alone.
//!
//! The clip is shared by `Arc::clone` — zero copies per utterance. A full or
//! disconnected response sink drops the reply (the utterance's audio still lives
//! in the record store, so a lost demo reply costs responsiveness, not data);
//! the drop is made loud via a typed event plus a counter.

use std::sync::Arc;

use futures::FutureExt;
use futures::future::BoxFuture;

use crate::brain::{BrainEventFn, BrainStats, send_or_report};
use crate::traits::{Brain, ResponseSink};
use crate::types::{InterruptProgress, SpeakBody, SpeakCmd, Utterance, UtteranceId};

/// The trivial `Brain`: every utterance is answered by queueing one fixed clip.
pub struct WavBrain {
    clip: Arc<[i16]>,
    events: BrainEventFn,
    stats: Arc<BrainStats>,
}

impl WavBrain {
    /// Build a `WavBrain` around a validated, pre-loaded clip. The clip is shared
    /// by `Arc::clone` into every response — no per-utterance copy.
    pub fn new(clip: Arc<[i16]>, events: BrainEventFn, stats: Arc<BrainStats>) -> Self {
        Self {
            clip,
            events,
            stats,
        }
    }
}

impl Brain for WavBrain {
    fn handle(&self, u: Utterance, mut out: ResponseSink) -> BoxFuture<'static, ()> {
        let utterance = u.id;
        let cmd = SpeakCmd {
            target: u.pod,
            in_reply_to: Some(u.id),
            body: SpeakBody::Pcm(Arc::clone(&self.clip)),
            interruptible: true,
            timings: u.timings.clone(),
        };
        send_or_report(&mut out, cmd, utterance, &self.events, &self.stats);
        // The reply is queued synchronously above; nothing remains to await.
        futures::future::ready(()).boxed()
    }

    fn interrupt(&self, _id: UtteranceId, _progress: InterruptProgress) {
        // No-op: the clip is fixed, so there is no partial-delivery state worth
        // recording — the flush itself is what stops the audio.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use futures::channel::mpsc;
    use pod_ingest::SegmentRef;

    use crate::brain::BrainEvent;
    use crate::types::{AudioSpan, DoaTrack, PodId, RoomId, StageTimings};

    /// A `BrainEventFn` that collects emitted events for assertion.
    fn event_collector() -> (BrainEventFn, Arc<Mutex<Vec<BrainEvent>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let f: BrainEventFn = Arc::new(move |e| sink.lock().unwrap().push(e));
        (f, seen)
    }

    fn test_utterance() -> Utterance {
        Utterance {
            id: UtteranceId(42),
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
            transcript: None,
            timings: StageTimings::default(),
            endpoint_cause: crate::types::EndpointCause::SoftEndpoint,
            wake: None,
            barge_in: None,
        }
    }

    #[test]
    fn emits_speak_cmd_sharing_the_clip() {
        let clip: Arc<[i16]> = Arc::from(vec![1i16, -2, 3].as_slice());
        let (events, seen) = event_collector();
        let stats = Arc::new(BrainStats::default());
        let brain = WavBrain::new(Arc::clone(&clip), events, Arc::clone(&stats));

        let (tx, mut rx) = mpsc::channel::<SpeakCmd>(1);
        let sink = ResponseSink::new(tx);
        drop(brain.handle(test_utterance(), sink));

        let cmd = rx.try_recv().expect("a SpeakCmd was queued");
        assert_eq!(cmd.target, PodId("pod-x".into()));
        assert_eq!(cmd.in_reply_to, Some(UtteranceId(42)));
        assert!(cmd.interruptible);
        match cmd.body {
            SpeakBody::Pcm(pcm) => assert!(
                Arc::ptr_eq(&pcm, &clip),
                "the clip is shared by Arc, not copied"
            ),
            SpeakBody::Text(_) => panic!("WavBrain queues Pcm, never Text"),
        }
        assert!(seen.lock().unwrap().is_empty(), "no failure on a live sink");
        assert_eq!(stats.snapshot().speak_send_failures, 0);
    }

    #[test]
    fn full_sink_emits_event_and_counts() {
        // A fresh per-clone sender always has its one guaranteed slot, so to see
        // a full sink both the buffer (0) and that slot must be exhausted first.
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
        let brain = WavBrain::new(Arc::from(vec![0i16].as_slice()), events, Arc::clone(&stats));

        drop(brain.handle(test_utterance(), ResponseSink::new(tx)));

        assert_eq!(
            *seen.lock().unwrap(),
            vec![BrainEvent::SinkFull {
                utterance: UtteranceId(42)
            }]
        );
        assert_eq!(stats.snapshot().speak_send_failures, 1);
    }

    #[test]
    fn dropped_receiver_takes_the_same_path() {
        let (tx, rx) = mpsc::channel::<SpeakCmd>(1);
        drop(rx);

        let (events, seen) = event_collector();
        let stats = Arc::new(BrainStats::default());
        let brain = WavBrain::new(Arc::from(vec![0i16].as_slice()), events, Arc::clone(&stats));

        drop(brain.handle(test_utterance(), ResponseSink::new(tx)));

        assert_eq!(
            *seen.lock().unwrap(),
            vec![BrainEvent::SinkFull {
                utterance: UtteranceId(42)
            }]
        );
        assert_eq!(stats.snapshot().speak_send_failures, 1);
    }

    #[test]
    fn interrupt_is_a_no_op() {
        let (events, seen) = event_collector();
        let stats = Arc::new(BrainStats::default());
        let brain = WavBrain::new(Arc::from(vec![0i16].as_slice()), events, Arc::clone(&stats));

        brain.interrupt(
            UtteranceId(42),
            InterruptProgress {
                heard_ms: 300,
                total_ms: 1_000,
            },
        );

        assert!(seen.lock().unwrap().is_empty());
        assert_eq!(stats.snapshot().speak_send_failures, 0);
    }
}
