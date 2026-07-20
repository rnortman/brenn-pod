//! The continuous listener: per-pod streaming wake + host endpointing, running
//! on live audio chunks rather than assembled segments.
//!
//! This is the substrate that demotes the device VAD segment to transport gating
//! and gives the host ownership of utterance semantics. The streaming
//! openWakeWord core lands first ([`oww_stream`]); the Silero endpointer, PCM
//! ring, per-pod state map, and listener thread build on top of it.

pub mod endpointer;
pub mod event;
mod ort_util;
pub mod oww_stream;
pub mod ring;
pub mod runtime;
pub mod silero;
pub mod stats;

pub use crate::types::EndpointCause;
pub use endpointer::{
    EndpointEvent, EndpointState, EndpointTransition, Endpointer, EndpointerConfig, TransitionCause,
};
pub use event::{
    CarveTiming, CarvedUtterance, Feed, ListenerEvent, ListenerUtteranceId, StatsFlushCause,
    StatsModel, WakePolicy,
};
pub use oww_stream::{OwwConfig, OwwModels, OwwStream, ScoredChunk, WakeDetected};
pub use ring::PcmRing;
pub use runtime::{
    BargeInConfig, FeedPermit, FeedSender, Listener, ListenerConfig, ListenerHandle, ListenerState,
    ListenerStats, ListenerStatsSnapshot,
};
pub use silero::{SileroConfig, SileroModel, SileroVad};
pub use stats::{ScoreStats, ScoreSummary, MODEL_STATS_FLUSH_CHUNKS};
