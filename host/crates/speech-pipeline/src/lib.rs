//! The speech pipeline's embeddable core: the typed spine that carries VAD
//! segments, tracking events, and speak commands between stages. No I/O lives
//! here — `speech-surface` drives this under tokio.

pub mod assembler;
pub mod brain;
pub mod echo_brain;
pub mod http;
pub mod listener;
pub mod playback;
pub mod queue;
mod stats;
#[cfg(test)]
mod test_support;
pub mod tracking;
pub mod traits;
pub mod types;
pub mod wake;
pub mod wav;
pub mod wav_brain;

pub use assembler::{AssemblerLimits, AssemblerStats, SegmentAssembler};
pub use brain::{BrainEvent, BrainEventFn, BrainStats, BrainStatsSnapshot, WakeCommandReason};
pub use echo_brain::EchoBrain;
pub use http::{
    BuildError, HttpSynthesizer, HttpTranscriber, SttParams, SttStats, SttStatsSnapshot, TtsParams,
    TtsStats, TtsStatsSnapshot, Url,
};
pub use listener::{
    BargeInConfig, CarveTiming, CarvedUtterance, EndpointEvent, EndpointState, EndpointTransition,
    Endpointer, EndpointerConfig, Feed, FeedPermit, FeedSender, Listener, ListenerConfig,
    ListenerEvent, ListenerHandle, ListenerState, ListenerStats, ListenerStatsSnapshot,
    ListenerUtteranceId, MODEL_STATS_FLUSH_CHUNKS, OwwModels, OwwStream, PcmRing, ScoreStats,
    ScoreSummary, ScoredChunk, SileroConfig, SileroModel, SileroVad, StatsFlushCause, StatsModel,
    TransitionCause, WakeDetected, WakePolicy,
};
pub use playback::{
    AbortReason, FRAME_MS, FlushRejected, PacerConfig, PlayRejected, PlaybackEvent,
    PlaybackEventFn, PlaybackHandle, PlaybackJob, PlaybackStats, PlaybackStatsSnapshot,
    PlaybackWriter,
};
pub use queue::{DropOldestQueue, QueueStats, Receiver, Sender, StatsHandle};
pub use tracking::tracking_event;
pub use traits::{
    Brain, PcmChunk, ResponseSink, SegmentAudio, SynthesisError, Synthesizer, TranscribeError,
    Transcriber, TranscriptEvent,
};
pub use types::{
    AudioSpan, BargeInContext, Codec, ConfidenceGate, ContextSegment, DoaTrack, EndpointCause,
    GateReject, InterruptProgress, MAX_CONTEXT_SEGMENTS, MAX_RESOLVE_SAMPLES, PodId,
    ResolvedSpanAudio, RoomId, SPINE_FORMAT, Segment, SegmentEndCause, SegmentEndInfo,
    SegmentTelemetry, SpanResolveError, SpeakBody, SpeakCmd, SpeakerId, StageTimings,
    TrackingEvent, Transcript, TranscriptConfidence, Utterance, UtteranceId, WakeConfirmation,
    signed_offset_us, stage_delta_us,
};
pub use wake::{OwwConfig, OwwGate, WakeError, WakeOutcome};
pub use wav::{SpineFormatViolation, check_spine_format, write_spine_wav};
pub use wav_brain::WavBrain;
