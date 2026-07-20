//! The stage traits — `Transcriber`, `Synthesizer`, `Brain` — and the minimal
//! support types their signatures reference. Declared here; first implemented in
//! a later increment (the speech surface holds trait objects, so downstream
//! increments swap implementations without touching the plumbing).
//!
//! The support types (`SegmentAudio`, `TranscriptEvent`, `PcmChunk`,
//! `ResponseSink`) are deliberately minimal now; later increments extend them
//! rather than migrate. The `Transcriber` / `Synthesizer` item types are
//! `Result`s: a stage backed by a network call must report a failure in-band,
//! so the stream yields `Ok` events or a single terminal `Err`. The
//! `Utterance` surface envelope and the `Transcript` it carries are spine value
//! types and live in `types`.
//!
//! Stream contract for both stages: a stream ends either after an `is_final`
//! `TranscriptEvent` / at least one `PcmChunk`, or immediately after yielding
//! one `Err`. A stream that ends with neither is a caller-handled failure with
//! no detail (an implementation bug). Errors are carried in-band so the caller
//! — which knows the pod / segment / utterance ids — emits the correlated
//! failure line; implementations keep only counters.

use std::sync::Arc;

use futures::channel::mpsc;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use serde::Serialize;

use crate::types::{InterruptProgress, SpeakCmd, TranscriptConfidence, Utterance, UtteranceId};

/// The PCM handed to a `Transcriber`: the segment's samples plus their rate.
#[derive(Debug, Clone)]
pub struct SegmentAudio {
    pub pcm: Arc<[i16]>,
    pub sample_rate_hz: u32,
}

/// One incremental event from a streaming `Transcriber`: partial or final text.
#[derive(Debug, Clone, Serialize)]
pub struct TranscriptEvent {
    pub text: String,
    /// The last event of the stream, carrying the settled transcript text.
    pub is_final: bool,
    /// Aggregate STT quality signals, present on the final event when the
    /// backend returned `verbose_json`. `None` for a partial event or a
    /// plain-`json` backend without per-segment fields.
    pub confidence: Option<TranscriptConfidence>,
}

/// One chunk of synthesized PCM from a streaming `Synthesizer`.
#[derive(Debug, Clone)]
pub struct PcmChunk {
    pub pcm: Arc<[i16]>,
}

/// Why a `Transcriber` stream failed. Carried in-band as the stream's terminal
/// `Err`; the caller correlates it with pod / segment / utterance ids.
#[derive(Debug, thiserror::Error)]
pub enum TranscribeError {
    /// The backend could not be reached (connection refused, DNS, etc.).
    #[error("connect: {0}")]
    Connect(String),
    /// The request exceeded its time budget.
    #[error("timeout")]
    Timeout,
    /// The backend returned a non-success status. `body` is truncated by the
    /// constructor so it cannot bloat a JSONL line.
    #[error("status {code}: {body}")]
    Status { code: u16, body: String },
    /// The response body could not be parsed into the expected shape.
    #[error("decode: {0}")]
    Decode(String),
}

/// Why a `Synthesizer` stream failed. Like `TranscribeError`, plus a
/// spine-format mismatch, since the synthesizer produces PCM the spine admits.
#[derive(Debug, thiserror::Error)]
pub enum SynthesisError {
    /// The backend could not be reached (connection refused, DNS, etc.).
    #[error("connect: {0}")]
    Connect(String),
    /// The request exceeded its time budget.
    #[error("timeout")]
    Timeout,
    /// The backend returned a non-success status. `body` is truncated by the
    /// constructor so it cannot bloat a JSONL line.
    #[error("status {code}: {body}")]
    Status { code: u16, body: String },
    /// The response body could not be parsed into the expected shape.
    #[error("decode: {0}")]
    Decode(String),
    /// The decoded audio is not the spine format the playback path requires.
    #[error("format: {got}")]
    Format { got: String },
}

/// Why a `ResponseSink::try_send` was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkSendError {
    /// The bounded queue is full.
    Full,
    /// The pipeline's receiver has been dropped.
    Disconnected,
}

/// Observes every `SpeakCmd` a [`ResponseSink`] accepts. Synchronous and on the
/// brain's own thread, so an implementation must stay cheap — the barge-in ledger
/// takes a lock and returns.
pub type SinkTap = Arc<dyn Fn(&SpeakCmd) + Send + Sync>;

/// The channel a `Brain` writes its responses into. Wraps the sender half of the
/// response path; the pipeline holds the receiver. Bounded, so a slow playback
/// path back-pressures the brain rather than growing unboundedly.
pub struct ResponseSink {
    tx: mpsc::Sender<SpeakCmd>,
    tap: Option<SinkTap>,
}

impl ResponseSink {
    pub fn new(tx: mpsc::Sender<SpeakCmd>) -> Self {
        Self { tx, tap: None }
    }

    /// A sink that hands each accepted command to `tap` before returning. The tap
    /// is the surface's seam for counting a turn's responses and capturing their
    /// text; the brain is unaware of it.
    pub fn with_tap(tx: mpsc::Sender<SpeakCmd>, tap: SinkTap) -> Self {
        Self { tx, tap: Some(tap) }
    }

    /// Queue a response command. Errors if the queue is full or the pipeline's
    /// receiver has been dropped. Returns a small owned reason rather than the
    /// rejected command, since no caller retries it.
    ///
    /// The tap sees every command *offered* to the queue, including one the queue
    /// then refuses. It runs before the send because the send consumes `cmd`, and
    /// running first is also what keeps the tap ahead of the router: an observer
    /// can never see a command handled that it was not told about. A refused
    /// command is a dropped reply the user never hears, so an observer counting
    /// deliveries must not treat the turn as having completed.
    pub fn try_send(&mut self, cmd: SpeakCmd) -> Result<(), SinkSendError> {
        if let Some(tap) = self.tap.as_ref() {
            tap(&cmd);
        }
        self.tx.try_send(cmd).map_err(|e| {
            if e.is_full() {
                SinkSendError::Full
            } else {
                SinkSendError::Disconnected
            }
        })
    }
}

/// PCM in → incremental text out.
pub trait Transcriber: Send + Sync {
    fn transcribe(
        &self,
        audio: SegmentAudio,
    ) -> BoxStream<'static, Result<TranscriptEvent, TranscribeError>>;
}

/// Text in → chunked PCM out.
pub trait Synthesizer: Send + Sync {
    fn synthesize(&self, text: &str) -> BoxStream<'static, Result<PcmChunk, SynthesisError>>;
}

/// Utterance in → speak/act out; interruptible.
pub trait Brain: Send + Sync {
    fn handle(&self, u: Utterance, out: ResponseSink) -> BoxFuture<'static, ()>;
    /// The turn `id` was cut mid-playback; `progress` says how much of the
    /// response clip the user heard. Invoked only with a turn the playback writer
    /// confirmed was the one it was playing when the flush was accepted, so a stale
    /// interrupt for some other turn never reaches here. The cut itself is
    /// best-effort at the frame boundary: in a one-frame-wide race the job can play
    /// out just as the flush is accepted, so `progress` may report a `heard_ms` at
    /// or near `total_ms` for a response that effectively finished.
    fn interrupt(&self, id: UtteranceId, progress: InterruptProgress);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PodId, SpeakBody, StageTimings};
    use futures::{FutureExt, StreamExt};

    // Trivial implementations proving the traits are object-safe and their
    // signatures compile as declared.
    struct NullStages;

    impl Transcriber for NullStages {
        fn transcribe(
            &self,
            _audio: SegmentAudio,
        ) -> BoxStream<'static, Result<TranscriptEvent, TranscribeError>> {
            futures::stream::empty().boxed()
        }
    }

    impl Synthesizer for NullStages {
        fn synthesize(&self, _text: &str) -> BoxStream<'static, Result<PcmChunk, SynthesisError>> {
            futures::stream::empty().boxed()
        }
    }

    impl Brain for NullStages {
        fn handle(&self, _u: Utterance, _out: ResponseSink) -> BoxFuture<'static, ()> {
            futures::future::ready(()).boxed()
        }
        fn interrupt(&self, _id: UtteranceId, _progress: InterruptProgress) {}
    }

    #[test]
    fn traits_are_object_safe() {
        let _t: Box<dyn Transcriber> = Box::new(NullStages);
        let _s: Box<dyn Synthesizer> = Box::new(NullStages);
        let _b: Box<dyn Brain> = Box::new(NullStages);
    }

    fn cmd(text: &str) -> SpeakCmd {
        SpeakCmd {
            target: PodId("pod-x".into()),
            in_reply_to: Some(UtteranceId(1)),
            body: SpeakBody::Text(text.into()),
            interruptible: true,
            timings: StageTimings::default(),
        }
    }

    #[test]
    fn sink_forwards_to_channel() {
        let (tx, _rx) = mpsc::channel::<SpeakCmd>(1);
        let mut sink = ResponseSink::new(tx);
        sink.try_send(cmd("hi")).unwrap();
    }

    #[test]
    fn a_tapped_sink_shows_the_tap_every_command_it_is_offered() {
        use std::sync::Mutex;

        // Including one the queue refuses: an observer that counts deliveries must
        // hear about the refused command too, or it would wait forever for an
        // outcome that is never coming.
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let sink_seen = Arc::clone(&seen);
        // Zero buffer: `futures::mpsc` still guarantees one slot per sender, so
        // this is the smallest channel that refuses a second un-drained send.
        let (tx, _rx) = mpsc::channel::<SpeakCmd>(0);
        let mut sink = ResponseSink::with_tap(
            tx,
            Arc::new(move |cmd: &SpeakCmd| {
                if let SpeakBody::Text(text) = &cmd.body {
                    sink_seen.lock().unwrap().push(text.clone());
                }
            }),
        );

        sink.try_send(cmd("first")).unwrap();
        // Nothing has drained the first, so the second is refused.
        assert_eq!(sink.try_send(cmd("second")), Err(SinkSendError::Full));

        assert_eq!(*seen.lock().unwrap(), ["first", "second"]);
    }

    #[test]
    fn an_untapped_sink_needs_no_tap() {
        let (tx, mut rx) = mpsc::channel::<SpeakCmd>(1);
        let mut sink = ResponseSink::new(tx);
        sink.try_send(cmd("hi")).unwrap();
        assert!(rx.try_recv().is_ok());
    }
}
