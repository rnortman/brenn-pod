//! Startup configuration: the TOML file (plus, later, clap overrides) that
//! parameterizes the daemon.
//!
//! The increment-1 subset: listen address (required — no default bind), the
//! connection cap, the JSONL sink, the record store, the pipeline bounds, and
//! the pod→room map. `deny_unknown_fields` on every table makes a typo fatal at
//! startup rather than a silent no-op; the required `listen_addr` forces an
//! explicit LAN address instead of guessing an interface (never `0.0.0.0`).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use audio_pipeline::playback::{
    INBOUND_PCM_RING_BYTES, INBOUND_PCM_WRITE_UNIT_BYTES, PLAYBACK_PREROLL_MAX_TARGET_BYTES,
};
use audio_pipeline::wire::MAX_AUDIO_PAYLOAD;
use serde::Deserialize;
use speech_pipeline::{
    ConfidenceGate, EndpointerConfig as ListenerEndpointerConfig, PacerConfig, Url, FRAME_MS,
};

use crate::psk::parse_psk_hex;

/// Silero VAD chunk duration: 512 samples at the 16 kHz spine rate. The listener's
/// endpointer knobs are chunk-counts; the `[endpointer]` table is ms-denominated,
/// so both sides single-source from [`ListenerEndpointerConfig::default`] through
/// this factor (defaults convert chunk→ms; [`EndpointerConfig::to_listener`] the
/// other way).
const SILERO_CHUNK_MS: u32 = 32;
/// Samples per millisecond at the 16 kHz spine rate — the preroll ms↔sample bridge.
const SAMPLES_PER_MS: u64 = 16;

/// Room name used for a pod absent from the `[pods]` map.
pub const UNMAPPED_ROOM: &str = "unmapped";

/// Parsed daemon configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// LAN address to bind the accept loop to. Required: no default bind, so an
    /// operator states the interface rather than falling back to `0.0.0.0`.
    pub listen_addr: SocketAddr,
    /// Path to the per-pod PSK secrets file ([`PskTable`]). Required: the ingest
    /// listener speaks only TLS-PSK, so a daemon without keys can serve no pod.
    pub pod_psk_file: PathBuf,
    /// Accept-gate semaphore size.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    #[serde(default)]
    pub jsonl: JsonlConfig,
    #[serde(default)]
    pub record: RecordConfig,
    #[serde(default)]
    pub pipeline: PipelineConfig,
    /// Playback pacing/queue bounds. Full defaults; applies whenever playback
    /// writers exist (every registered pod), brain or no brain.
    #[serde(default)]
    pub playback: PlaybackConfig,
    /// Wake-gate configuration. `None` when the `[wake]` table is absent — the
    /// server treats absence as bypass-with-warning, distinct from an explicit
    /// quiet `mode = "bypass"`.
    #[serde(default)]
    pub wake: Option<WakeConfig>,
    /// Host-endpointer configuration. `None` when the `[endpointer]` table is
    /// absent — the continuous listener's Silero endpointer is unwired and
    /// utterance boundaries fall back to device VAD-release alone.
    #[serde(default)]
    pub endpointer: Option<EndpointerConfig>,
    /// Brain configuration. `None` when the `[brain]` table is absent — no brain
    /// is wired and utterances go unanswered (increment-3 behavior).
    #[serde(default)]
    pub brain: Option<BrainConfig>,
    /// Speech-to-text configuration. `None` when the `[stt]` table is absent — no
    /// transcriber is wired and utterances mint with a null transcript. A present
    /// table enriches every utterance regardless of whether a brain consumes it.
    #[serde(default)]
    pub stt: Option<SttConfig>,
    /// Text-to-speech configuration. `None` when the `[tts]` table is absent — a
    /// `SpeakBody::Text` command then has no way to render and stays a
    /// `speak_unsupported` rejection.
    #[serde(default)]
    pub tts: Option<TtsConfig>,
    /// Pod-id → per-pod config (room mapping). Pods absent here resolve to
    /// [`UNMAPPED_ROOM`].
    #[serde(default)]
    pub pods: HashMap<String, PodConfig>,
}

impl Config {
    /// Read, parse, and validate the TOML config at `path`. Read, parse, and
    /// validation errors all carry the path and precise context.
    pub fn load(path: &Path) -> Result<Config, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let config = Config::parse(&text).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
        config.validate().map_err(|message| ConfigError::Invalid {
            path: path.to_path_buf(),
            message,
        })?;
        Ok(config)
    }

    /// Parse config from an in-memory TOML string (path-free; `load` wraps it).
    /// Semantic validation is separate — see [`Config::validate`].
    pub fn parse(text: &str) -> Result<Config, toml::de::Error> {
        toml::from_str(text)
    }

    /// Semantic checks the TOML grammar cannot express, so a misconfiguration is
    /// a precise startup error rather than a silent runtime hazard.
    pub fn validate(&self) -> Result<(), String> {
        if self.listen_addr.ip().is_unspecified() {
            return Err(format!(
                "listen_addr {} binds every interface; name a concrete LAN address",
                self.listen_addr
            ));
        }
        if self.pipeline.segment_queue_depth == 0 {
            return Err(
                "pipeline.segment_queue_depth must be at least 1 (0 drops every segment)"
                    .to_string(),
            );
        }
        if let Some(wake) = &self.wake {
            wake.validate()?;
        }
        if let Some(endpointer) = &self.endpointer {
            endpointer.validate()?;
        }
        if let Some(brain) = &self.brain {
            brain.validate()?;
        }
        if let Some(stt) = &self.stt {
            stt.validate()?;
        }
        if let Some(tts) = &self.tts {
            tts.validate()?;
        }
        // Cross-table: an echo brain reads back what it heard, so it is a
        // misconfiguration without both a transcriber (to hear) and a
        // synthesizer (to speak). Fatal at startup with the missing table named,
        // rather than a silently mute daemon.
        if let Some(brain) = &self.brain {
            if brain.mode == BrainMode::Echo {
                if self.stt.is_none() {
                    return Err(
                        "brain.mode = \"echo\" requires an [stt] table (nothing to transcribe with)"
                            .to_string(),
                    );
                }
                if self.tts.is_none() {
                    return Err(
                        "brain.mode = \"echo\" requires a [tts] table (nothing to speak with)"
                            .to_string(),
                    );
                }
            }
        }
        self.playback.validate()?;
        Ok(())
    }

    /// Resolve a pod's room. A pod absent from the map is [`RoomLookup::Unmapped`]
    /// — the caller warns on it but never rejects the pod.
    pub fn room_for(&self, pod_id: &str) -> RoomLookup {
        match self.pods.get(pod_id) {
            Some(pod) => RoomLookup::Mapped(pod.room.clone()),
            None => RoomLookup::Unmapped,
        }
    }
}

/// Outcome of a pod→room lookup. Distinguishes a configured room from the
/// unmapped fallback so the caller can warn on the latter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoomLookup {
    Mapped(String),
    Unmapped,
}

impl RoomLookup {
    /// The effective room name — the configured room, or [`UNMAPPED_ROOM`].
    pub fn room(&self) -> &str {
        match self {
            RoomLookup::Mapped(room) => room,
            RoomLookup::Unmapped => UNMAPPED_ROOM,
        }
    }

    pub fn is_unmapped(&self) -> bool {
        matches!(self, RoomLookup::Unmapped)
    }
}

/// Per-pod configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PodConfig {
    pub room: String,
}

/// JSONL observability sink configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonlConfig {
    #[serde(default)]
    pub sink: JsonlSink,
    /// Seconds between periodic `stage_health` lines. `0` disables the periodic
    /// line — the shutdown `stage_health` line still fires either way.
    #[serde(default = "default_stage_health_period_s")]
    pub stage_health_period_s: u64,
}

impl Default for JsonlConfig {
    fn default() -> Self {
        JsonlConfig {
            sink: JsonlSink::default(),
            stage_health_period_s: default_stage_health_period_s(),
        }
    }
}

/// Where the JSONL event stream goes: nowhere, standard output, or a file.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(from = "String")]
pub enum JsonlSink {
    /// No event-stream sink — the human console is the only observability
    /// surface. The default.
    #[default]
    None,
    Stdout,
    File(PathBuf),
}

impl JsonlSink {
    /// The configured destination as a stable label — `"none"`, `"stdout"`, or
    /// the file path — for the `daemon_start` event and its console header line.
    pub fn label(&self) -> String {
        match self {
            JsonlSink::None => "none".to_string(),
            JsonlSink::Stdout => "stdout".to_string(),
            JsonlSink::File(path) => path.display().to_string(),
        }
    }
}

impl From<String> for JsonlSink {
    fn from(value: String) -> Self {
        if value == "none" {
            JsonlSink::None
        } else if value == "stdout" || value == "-" {
            JsonlSink::Stdout
        } else {
            JsonlSink::File(PathBuf::from(value))
        }
    }
}

/// Record-store configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordConfig {
    /// Recording kill switch — on by default.
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_record_dir")]
    pub dir: PathBuf,
    #[serde(default = "default_cap_bytes")]
    pub cap_bytes: u64,
    /// Maximum store bytes attributable to a single pod id. Unset resolves to
    /// `cap_bytes / 2` via [`RecordConfig::resolved_pod_cap_bytes`], so one
    /// spoofed pod id can never claim more than half the store. A value
    /// `>= cap_bytes` makes per-pod enforcement inert (the global cap alone
    /// governs).
    #[serde(default)]
    pub pod_cap_bytes: Option<u64>,
    #[serde(default = "default_roll_max_bytes")]
    pub roll_max_bytes: u64,
    #[serde(default = "default_roll_max_age_s")]
    pub roll_max_age_s: u64,
}

impl RecordConfig {
    /// The per-pod byte quota the pruner enforces: the configured value, or
    /// `cap_bytes / 2` when unset. Both prune call paths resolve through here so
    /// the default lives in exactly one place.
    pub fn resolved_pod_cap_bytes(&self) -> u64 {
        self.pod_cap_bytes.unwrap_or(self.cap_bytes / 2)
    }
}

impl Default for RecordConfig {
    fn default() -> Self {
        RecordConfig {
            enabled: default_true(),
            dir: default_record_dir(),
            cap_bytes: default_cap_bytes(),
            pod_cap_bytes: None,
            roll_max_bytes: default_roll_max_bytes(),
            roll_max_age_s: default_roll_max_age_s(),
        }
    }
}

/// Pipeline bounds.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    #[serde(default = "default_segment_queue_depth")]
    pub segment_queue_depth: usize,
    #[serde(default = "default_max_segment_seconds")]
    pub max_segment_seconds: u64,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        PipelineConfig {
            segment_queue_depth: default_segment_queue_depth(),
            max_segment_seconds: default_max_segment_seconds(),
        }
    }
}

/// Playback pacing and queue bounds. Full defaults, so the table is optional
/// and applies to every playback writer.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlaybackConfig {
    /// Audio-ahead-of-real-time bound, in milliseconds. Must be at least one
    /// frame (20 ms) so the pacer has something to lead with.
    #[serde(default = "default_lead_ms")]
    pub lead_ms: u64,
    /// Per-frame-write budget, in milliseconds. Must be greater than zero.
    #[serde(default = "default_write_timeout_ms")]
    pub write_timeout_ms: u64,
    /// Bound on the shared `SpeakCmd` channel buffer.
    #[serde(default = "default_speak_queue_depth")]
    pub speak_queue_depth: usize,
    /// Bound on each per-pod writer's job queue.
    #[serde(default = "default_job_queue_depth")]
    pub job_queue_depth: usize,
}

impl Default for PlaybackConfig {
    fn default() -> Self {
        PlaybackConfig {
            lead_ms: default_lead_ms(),
            write_timeout_ms: default_write_timeout_ms(),
            speak_queue_depth: default_speak_queue_depth(),
            job_queue_depth: default_job_queue_depth(),
        }
    }
}

impl PlaybackConfig {
    /// Semantic checks: the lead must cover at least one frame and must not
    /// bank more audio than the device playout ring holds, the write timeout
    /// must be positive, and both queue depths must admit at least one.
    pub fn validate(&self) -> Result<(), String> {
        if self.lead_ms < FRAME_MS {
            return Err(format!(
                "playback.lead_ms {} must be at least {FRAME_MS} (one frame)",
                self.lead_ms
            ));
        }
        // The banked lead does not sit alone in the device playout ring: it must
        // co-reside with the escalated pre-roll target and one in-flight max frame.
        // The runtime bound therefore binds the full sum `lead + preroll_cap +
        // max_frame <= ring`, the same invariant the default's compile-time guard
        // enforces. A lead that fits the bare ring but breaks the combined budget
        // reproduces the wedge: the device cannot hold the sum, closes its receive
        // window, and the host write blocks into a `write_timeout_ms` abort. Reject
        // that config rather than let it wedge mid-playback.
        //
        // The subtraction is safe: a firmware compile-time guard already proves
        // `preroll_cap + max_frame <= ring`, so the remaining lead budget is
        // non-negative (it underflows at compile time otherwise, which is the
        // desired failure). The budget derives from the imported constants, so it
        // tracks any firmware retune of the ring, pre-roll cap, or frame size.
        const LEAD_BUDGET_BYTES: u64 =
            (INBOUND_PCM_RING_BYTES - PLAYBACK_PREROLL_MAX_TARGET_BYTES - MAX_AUDIO_PAYLOAD) as u64;
        let bytes_per_ms = (INBOUND_PCM_WRITE_UNIT_BYTES as u64) / FRAME_MS;
        let max_lead_ms = LEAD_BUDGET_BYTES / bytes_per_ms;
        // `lead_ms` is untrusted TOML (up to i64::MAX); a plain multiply wraps in
        // release and panics in debug for absurd values, which would defeat the very
        // guard below. `checked_mul` folds overflow into the over-budget rejection.
        match self.lead_ms.checked_mul(bytes_per_ms) {
            Some(lead_bytes) if lead_bytes <= LEAD_BUDGET_BYTES => {}
            Some(lead_bytes) => {
                return Err(format!(
                    "playback.lead_ms {} = {lead_bytes} B of audio plus the escalated pre-roll \
                     cap ({PLAYBACK_PREROLL_MAX_TARGET_BYTES} B) and one max frame \
                     ({MAX_AUDIO_PAYLOAD} B) exceeds the device playout ring \
                     ({INBOUND_PCM_RING_BYTES} B); the maximum acceptable lead is {max_lead_ms} ms",
                    self.lead_ms
                ));
            }
            None => {
                return Err(format!(
                    "playback.lead_ms {} is far too large; its byte-equivalent overflows and \
                     vastly exceeds the device playout ring ({INBOUND_PCM_RING_BYTES} B)",
                    self.lead_ms
                ));
            }
        }
        if self.write_timeout_ms == 0 {
            return Err("playback.write_timeout_ms must be greater than 0".to_string());
        }
        if self.speak_queue_depth == 0 {
            return Err("playback.speak_queue_depth must be at least 1".to_string());
        }
        if self.job_queue_depth == 0 {
            return Err("playback.job_queue_depth must be at least 1".to_string());
        }
        Ok(())
    }
}

/// Wake-gate configuration. A present `[wake]` table names an explicit `mode`;
/// `mode = "oww"` additionally requires all three model paths.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WakeConfig {
    /// Selects the gate implementation.
    pub mode: WakeMode,
    /// openWakeWord mel-spectrogram model. Required for `oww`; ignored for
    /// `bypass` (accepted so an operator can toggle mode without deleting paths).
    #[serde(default)]
    pub melspectrogram: Option<PathBuf>,
    /// openWakeWord embedding model. Required for `oww`; ignored for `bypass`.
    #[serde(default)]
    pub embedding: Option<PathBuf>,
    /// openWakeWord wake-phrase model. Required for `oww`; ignored for `bypass`.
    #[serde(default)]
    pub model: Option<PathBuf>,
    /// Sigmoid score above which a segment wakes. Must be in `(0.0, 1.0)`.
    #[serde(default = "default_wake_threshold")]
    pub threshold: f32,
}

impl WakeConfig {
    /// Semantic checks: `oww` needs all three model paths, and `threshold` must
    /// be a strict probability. Path presence/validity beyond "specified" is the
    /// gate's own load-time concern.
    pub fn validate(&self) -> Result<(), String> {
        if !(self.threshold > 0.0 && self.threshold < 1.0) {
            return Err(format!(
                "wake.threshold {} must be in the open interval (0.0, 1.0)",
                self.threshold
            ));
        }
        if self.mode == WakeMode::Oww {
            for (field, path) in [
                ("melspectrogram", &self.melspectrogram),
                ("embedding", &self.embedding),
                ("model", &self.model),
            ] {
                if path.is_none() {
                    return Err(format!("wake.{field} is required when wake.mode = \"oww\""));
                }
            }
        }
        Ok(())
    }
}

/// Host-endpointer configuration. A present `[endpointer]` table names the
/// Silero VAD model and optionally overrides the endpointer timing/threshold
/// knobs; every knob defaults to the design-tuned value when omitted.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointerConfig {
    /// Silero VAD ONNX model. Required — the whole point of the table. Path
    /// validity beyond "specified" is the endpointer's load-time concern.
    pub model: PathBuf,
    /// P(speech) at/above which a chunk counts toward onset. `(0.0, 1.0)`.
    #[serde(default = "default_onset_thresh")]
    pub onset_thresh: f32,
    /// P(speech) below which a chunk counts toward release. `(0.0, 1.0)`, and at
    /// most `onset_thresh` (hysteresis: release no higher than onset).
    #[serde(default = "default_release_thresh")]
    pub release_thresh: f32,
    /// Consecutive onset chunks (32 ms each) required to open an utterance.
    #[serde(default = "default_onset_chunks")]
    pub onset_chunks: u32,
    /// Silence held after speech before a soft endpoint, milliseconds.
    #[serde(default = "default_soft_hangover_ms")]
    pub soft_hangover_ms: u32,
    /// Window after a soft endpoint during which resumed speech continues the
    /// same utterance, milliseconds.
    #[serde(default = "default_continuation_window_ms")]
    pub continuation_window_ms: u32,
    /// Lead prepended to an utterance start so the first phoneme isn't clipped,
    /// milliseconds.
    #[serde(default = "default_preroll_pad_ms")]
    pub preroll_pad_ms: u32,
}

impl EndpointerConfig {
    /// Build the listener-crate endpointer config from this table: thresholds pass
    /// through, the ms windows quantize to Silero chunks, and the preroll converts
    /// to samples. `max_utterance_samples` is derived by the caller from the
    /// pipeline segment cap (it is not an `[endpointer]` knob), so the transport
    /// cap and the endpointer's forced-cap agree by construction.
    pub fn to_listener(&self, max_utterance_samples: u64) -> ListenerEndpointerConfig {
        ListenerEndpointerConfig {
            onset_thresh: self.onset_thresh,
            release_thresh: self.release_thresh,
            onset_chunks: self.onset_chunks,
            soft_hangover_chunks: self.soft_hangover_ms / SILERO_CHUNK_MS,
            continuation_chunks: self.continuation_window_ms / SILERO_CHUNK_MS,
            preroll_pad_samples: u64::from(self.preroll_pad_ms) * SAMPLES_PER_MS,
            max_utterance_samples,
        }
    }

    /// Semantic checks: both thresholds strict probabilities with the release no
    /// higher than the onset (hysteresis), and a non-zero onset run.
    pub fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("onset_thresh", self.onset_thresh),
            ("release_thresh", self.release_thresh),
        ] {
            if !(value > 0.0 && value < 1.0) {
                return Err(format!(
                    "endpointer.{field} {value} must be in the open interval (0.0, 1.0)"
                ));
            }
        }
        if self.release_thresh > self.onset_thresh {
            return Err(format!(
                "endpointer.release_thresh {} must not exceed onset_thresh {}",
                self.release_thresh, self.onset_thresh
            ));
        }
        if self.onset_chunks == 0 {
            return Err("endpointer.onset_chunks must be at least 1".to_string());
        }
        // The listener quantizes these ms windows to Silero chunks (32 ms). A value
        // under one chunk floors to zero chunks, which endpoints on the first
        // sub-release chunk (no hangover) or closes the instant it soft-endpoints
        // (no continuation) — machine-gun endpointing from a plausible `= 0` typo.
        for (field, ms) in [
            ("soft_hangover_ms", self.soft_hangover_ms),
            ("continuation_window_ms", self.continuation_window_ms),
        ] {
            if ms < 32 {
                return Err(format!(
                    "endpointer.{field} {ms} must be at least 32 ms (one Silero chunk)"
                ));
            }
        }
        Ok(())
    }
}

/// Which wake-gate implementation the daemon builds at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WakeMode {
    /// openWakeWord over `ort`.
    Oww,
    /// Pass-through gate: every segment bypasses scoring.
    Bypass,
}

/// Brain configuration. A present `[brain]` table names an explicit `mode`;
/// `mode = "wav"` additionally requires a `clip` path.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrainConfig {
    /// Selects the brain implementation.
    pub mode: BrainMode,
    /// Clip played back for every utterance. Required for `wav`. The clip is
    /// loaded and format-validated at startup — see [`crate::clip::load_clip`].
    #[serde(default)]
    pub clip: Option<PathBuf>,
}

impl BrainConfig {
    /// Semantic check: `wav` needs a `clip` path. Clip validity beyond
    /// "specified" is the loader's startup concern.
    pub fn validate(&self) -> Result<(), String> {
        if self.mode == BrainMode::Wav && self.clip.is_none() {
            return Err("brain.clip is required when brain.mode = \"wav\"".to_string());
        }
        Ok(())
    }
}

/// Which brain implementation the daemon builds at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BrainMode {
    /// Answer every utterance with a fixed configured clip.
    Wav,
    /// Read the transcript back as synthesized speech. Requires `[stt]` + `[tts]`.
    Echo,
}

/// Speech-to-text stage configuration. A present `[stt]` table names an explicit
/// `backend`; `backend = "http"` additionally requires `url` and `model`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SttConfig {
    /// Selects the transcriber implementation.
    pub backend: SttBackend,
    /// Base URL of the speaches container, e.g. `http://10.0.0.5:8000`. Required
    /// for `http`; must parse and use the plain-`http` scheme (no TLS stack).
    #[serde(default)]
    pub url: Option<String>,
    /// Whisper model name, as the operator's speaches install registers it.
    /// Required for `http`; no default — a baked-in model name would rot.
    #[serde(default)]
    pub model: Option<String>,
    /// Optional language hint; omitted from the request when absent.
    #[serde(default)]
    pub language: Option<String>,
    /// Total per-request budget, milliseconds. Must be greater than zero.
    #[serde(default = "default_http_timeout_ms")]
    pub timeout_ms: u64,
    /// Connect budget, milliseconds — a down container fails fast rather than at
    /// `timeout_ms`. Must be greater than zero.
    #[serde(default = "default_http_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// STT-confidence gate: reject a transcript as a likely hallucination when its
    /// worst-segment `no_speech_prob` exceeds this. Default `0.2` — the empty band
    /// of the live-hardware measurements (real commands ≤ 0.04; wake-in-noise
    /// hallucinations ≥ 0.35, a clean ~9× gap). A gated utterance routes to the
    /// same non-error no-command path as an empty transcript, never to the brain.
    #[serde(default = "default_no_speech_max")]
    pub no_speech_max: f32,
    /// Optional secondary gate: when set, also reject when the duration-weighted
    /// `avg_logprob` falls below this. Disabled by default — the measured logprob
    /// bands (real ≈ −0.28..−0.64; hallucination ≈ −0.97..−1.05) overlap less
    /// cleanly than `no_speech_prob`, so an operator opts in per install.
    #[serde(default)]
    pub avg_logprob_min: Option<f32>,
}

impl SttConfig {
    /// The confidence gate this config configures, for the pipeline to consult
    /// after transcription.
    pub fn confidence_gate(&self) -> ConfidenceGate {
        ConfidenceGate {
            no_speech_max: self.no_speech_max,
            avg_logprob_min: self.avg_logprob_min,
        }
    }

    /// Semantic checks: `http` needs `url` (parseable, `http` scheme) + `model`,
    /// both timeouts must be positive, `no_speech_max` must be a probability, and a
    /// present `avg_logprob_min` must be finite (a non-finite floor never fires).
    pub fn validate(&self) -> Result<(), String> {
        validate_timeouts("stt", self.timeout_ms, self.connect_timeout_ms)?;
        if !(0.0..=1.0).contains(&self.no_speech_max) {
            return Err(format!(
                "stt.no_speech_max {} must be in the closed interval [0.0, 1.0]",
                self.no_speech_max
            ));
        }
        if let Some(min) = self.avg_logprob_min {
            if !min.is_finite() {
                return Err(format!("stt.avg_logprob_min {min} must be a finite value"));
            }
        }
        match self.backend {
            SttBackend::Http => {
                let url = self
                    .url
                    .as_deref()
                    .ok_or("stt.url is required when stt.backend = \"http\"")?;
                validate_http_url("stt.url", url)?;
                if self.model.is_none() {
                    return Err("stt.model is required when stt.backend = \"http\"".to_string());
                }
            }
        }
        Ok(())
    }
}

/// Which transcriber implementation the daemon builds at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SttBackend {
    /// speaches (OpenAI-compatible) `/v1/audio/transcriptions` over plain HTTP.
    Http,
}

/// Text-to-speech stage configuration. A present `[tts]` table names an explicit
/// `backend`; `backend = "http"` additionally requires `url`, `model`, `voice`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TtsConfig {
    /// Selects the synthesizer implementation.
    pub backend: TtsBackend,
    /// Base URL of the speaches container, e.g. `http://10.0.0.5:8000`. Required
    /// for `http`; must parse and use the plain-`http` scheme (no TLS stack).
    #[serde(default)]
    pub url: Option<String>,
    /// TTS model name, as the operator's speaches install registers it. Required
    /// for `http`; no default — a baked-in model name would rot.
    #[serde(default)]
    pub model: Option<String>,
    /// Voice the model renders with. Required for `http`; no default.
    #[serde(default)]
    pub voice: Option<String>,
    /// Total per-request budget, milliseconds. Must be greater than zero.
    #[serde(default = "default_http_timeout_ms")]
    pub timeout_ms: u64,
    /// Connect budget, milliseconds — a down container fails fast rather than at
    /// `timeout_ms`. Must be greater than zero.
    #[serde(default = "default_http_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
}

impl TtsConfig {
    /// Semantic checks: `http` needs `url` (parseable, `http` scheme) + `model` +
    /// `voice`, and both timeouts must be positive.
    pub fn validate(&self) -> Result<(), String> {
        validate_timeouts("tts", self.timeout_ms, self.connect_timeout_ms)?;
        match self.backend {
            TtsBackend::Http => {
                let url = self
                    .url
                    .as_deref()
                    .ok_or("tts.url is required when tts.backend = \"http\"")?;
                validate_http_url("tts.url", url)?;
                if self.model.is_none() {
                    return Err("tts.model is required when tts.backend = \"http\"".to_string());
                }
                if self.voice.is_none() {
                    return Err("tts.voice is required when tts.backend = \"http\"".to_string());
                }
            }
        }
        Ok(())
    }
}

/// Which synthesizer implementation the daemon builds at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TtsBackend {
    /// speaches (OpenAI-compatible) `/v1/audio/speech` over plain HTTP.
    Http,
}

/// Parse-check a base URL and enforce the plain-`http` scheme. The HTTP speech
/// stages carry no TLS stack — they talk to a container on the operator's own
/// host — so an `https` endpoint is a loud startup error rather than a confusing
/// runtime "no TLS backend" failure.
fn validate_http_url(field: &str, url: &str) -> Result<(), String> {
    let parsed = Url::parse(url).map_err(|e| format!("{field} {url:?} is not a valid URL: {e}"))?;
    match parsed.scheme() {
        "http" => {}
        "https" => {
            return Err(format!(
                "{field} uses https, but the HTTP speech stages carry no TLS stack; \
                 use an http:// endpoint"
            ));
        }
        other => {
            return Err(format!(
                "{field} scheme {other:?} is unsupported; use an http:// endpoint"
            ));
        }
    }
    // Embedded credentials (`user:pass@host`) would ride verbatim onto the
    // `*_configured` startup line where any log reader could harvest them; reject
    // them rather than leak. The message never echoes the offending userinfo.
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(format!(
            "{field} carries embedded credentials (user:pass@host); credentials in \
             stage URLs are unsupported"
        ));
    }
    Ok(())
}

/// Both request budgets must be positive: a zero timeout is a request that can
/// never complete.
fn validate_timeouts(table: &str, timeout_ms: u64, connect_timeout_ms: u64) -> Result<(), String> {
    if timeout_ms == 0 {
        return Err(format!("{table}.timeout_ms must be greater than 0"));
    }
    if connect_timeout_ms == 0 {
        return Err(format!("{table}.connect_timeout_ms must be greater than 0"));
    }
    Ok(())
}

/// Per-pod pre-shared keys for the TLS-PSK ingest link: PSK identity (the pod id
/// the pod puts in the handshake, and must repeat in `Hello`) → 32-byte key.
///
/// The file is TOML, a flat table of `pod_id = "<64 hex chars>"`, written by
/// `podctl provision-audio-psk --host-psk-file`. Every parse problem is a hard
/// startup failure: a daemon that silently drops half its fleet's keys looks
/// exactly like a network fault. `Debug` prints identities and key *lengths*
/// only, so a key never reaches a log through an error or a `{:?}` on `Config`.
#[derive(Clone, Default)]
pub struct PskTable {
    keys: HashMap<String, [u8; 32]>,
}

impl PskTable {
    /// Read and validate the secrets file at `path`.
    pub fn load(path: &Path) -> Result<PskTable, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let mode_check = psk_file_mode_error(path);
        let table = PskTable::parse(&text)
            .and_then(|table| match mode_check {
                Some(message) => Err(message),
                None => Ok(table),
            })
            .map_err(|message| ConfigError::Invalid {
                path: path.to_path_buf(),
                message,
            })?;
        Ok(table)
    }

    /// Parse the secrets file's contents. Path-free; [`PskTable::load`] wraps it
    /// and adds the file-mode check (which needs the path).
    pub fn parse(text: &str) -> Result<PskTable, String> {
        let raw: toml::Value =
            toml::from_str(text).map_err(|e| format!("psk file is not valid TOML: {e}"))?;
        let table = raw
            .as_table()
            .ok_or_else(|| "psk file must be a table of pod_id = \"<hex>\"".to_string())?;
        let mut keys = HashMap::with_capacity(table.len());
        for (pod_id, value) in table {
            let hex = value.as_str().ok_or_else(|| {
                format!("psk entry for {pod_id:?} must be a 64-hex-character string")
            })?;
            // Duplicate ids are a TOML parse error, so reaching a second insert
            // for one id would mean the parser changed under us.
            keys.insert(pod_id.clone(), parse_psk_hex(pod_id, hex)?);
        }
        if keys.is_empty() {
            return Err("psk file defines no pods; no pod could connect".to_string());
        }
        Ok(PskTable { keys })
    }

    /// The key for `identity`, or `None` when the identity is unknown.
    pub fn key_for(&self, identity: &str) -> Option<&[u8; 32]> {
        self.keys.get(identity)
    }

    /// How many pods the table provisions (startup line; never the keys).
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

impl std::fmt::Debug for PskTable {
    /// Identities and key lengths only — the keys themselves never render.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ids: Vec<&str> = self.keys.keys().map(String::as_str).collect();
        ids.sort_unstable();
        write!(f, "PskTable {{ pods: {ids:?}, key_bytes: 32 }}")
    }
}

/// Reject a secrets file any other local account can read, matching ssh's posture
/// on private keys. Unix only — elsewhere there is no mode to check.
#[cfg(unix)]
fn psk_file_mode_error(path: &Path) -> Option<String> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(path).ok()?.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Some(format!(
            "psk file mode {mode:04o} is group/world-accessible; chmod 600 it"
        ));
    }
    None
}

#[cfg(not(unix))]
fn psk_file_mode_error(_path: &Path) -> Option<String> {
    None
}

/// A failure loading configuration or the PSK secrets file it names, carrying
/// the offending path.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse config file {path}: {source}")]
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("invalid {path}: {message}")]
    Invalid { path: PathBuf, message: String },
}

fn default_max_connections() -> usize {
    8
}
fn default_stage_health_period_s() -> u64 {
    30
}
fn default_true() -> bool {
    true
}
fn default_record_dir() -> PathBuf {
    PathBuf::from("./framelogs")
}
fn default_cap_bytes() -> u64 {
    8_000_000_000
}
fn default_roll_max_bytes() -> u64 {
    67_108_864
}
fn default_roll_max_age_s() -> u64 {
    900
}
fn default_segment_queue_depth() -> usize {
    8
}
fn default_max_segment_seconds() -> u64 {
    60
}
fn default_wake_threshold() -> f32 {
    0.5
}
// The `[endpointer]` ms/threshold defaults single-source from the listener's
// canonical `EndpointerConfig::default()` (chunk/sample-denominated), converting
// through `SILERO_CHUNK_MS`/`SAMPLES_PER_MS` so the two same-named types cannot
// drift. `to_listener` converts the other direction.
fn default_onset_thresh() -> f32 {
    ListenerEndpointerConfig::default().onset_thresh
}
fn default_release_thresh() -> f32 {
    ListenerEndpointerConfig::default().release_thresh
}
fn default_onset_chunks() -> u32 {
    ListenerEndpointerConfig::default().onset_chunks
}
fn default_soft_hangover_ms() -> u32 {
    ListenerEndpointerConfig::default().soft_hangover_chunks * SILERO_CHUNK_MS
}
fn default_continuation_window_ms() -> u32 {
    ListenerEndpointerConfig::default().continuation_chunks * SILERO_CHUNK_MS
}
fn default_preroll_pad_ms() -> u32 {
    (ListenerEndpointerConfig::default().preroll_pad_samples / SAMPLES_PER_MS) as u32
}
// The three pacing defaults delegate to `PacerConfig::default()` so the values
// live in one place — the pacer that runs at them — rather than as literals
// duplicated across the crate boundary. `speak_queue_depth` bounds the surface's
// `SpeakCmd` channel, which the pacer knows nothing about, so it stays local.
fn default_lead_ms() -> u64 {
    PacerConfig::default().lead_ms
}
fn default_write_timeout_ms() -> u64 {
    PacerConfig::default().write_timeout_ms
}
fn default_speak_queue_depth() -> usize {
    8
}
fn default_job_queue_depth() -> usize {
    PacerConfig::default().job_queue_depth
}
// STT/TTS request budgets are config-local: no lower-level owner defines them
// (unlike the pacing values, which delegate to `PacerConfig`). The two HTTP
// stages share the same defaults.
fn default_http_timeout_ms() -> u64 {
    15_000
}
fn default_http_connect_timeout_ms() -> u64 {
    2_000
}
// The primary STT-confidence gate. 0.2 sits in the empty band between the two
// measured populations (real commands ≤ 0.04, hallucinations ≥ 0.35).
fn default_no_speech_max() -> f32 {
    0.2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_config() {
        let text = r#"
listen_addr = "192.168.1.10:7380"
pod_psk_file = "/psk.toml"
max_connections = 16

[jsonl]
sink = "/var/log/speech.jsonl"
stage_health_period_s = 10

[record]
enabled = false
dir = "/data/framelogs"
cap_bytes = 1000
roll_max_bytes = 500
roll_max_age_s = 60

[pipeline]
segment_queue_depth = 4
max_segment_seconds = 30

[pods.pod-a1b2c3]
room = "kitchen"
"#;
        let config = Config::parse(text).expect("parse");
        assert_eq!(config.listen_addr, "192.168.1.10:7380".parse().unwrap());
        assert_eq!(config.max_connections, 16);
        assert_eq!(
            config.jsonl.sink,
            JsonlSink::File(PathBuf::from("/var/log/speech.jsonl"))
        );
        assert_eq!(config.jsonl.stage_health_period_s, 10);
        assert!(!config.record.enabled);
        assert_eq!(config.record.dir, PathBuf::from("/data/framelogs"));
        assert_eq!(config.record.cap_bytes, 1000);
        assert_eq!(config.record.pod_cap_bytes, None);
        assert_eq!(config.record.roll_max_bytes, 500);
        assert_eq!(config.record.roll_max_age_s, 60);
        assert_eq!(config.pipeline.segment_queue_depth, 4);
        assert_eq!(config.pipeline.max_segment_seconds, 30);
        assert_eq!(
            config.room_for("pod-a1b2c3"),
            RoomLookup::Mapped("kitchen".to_string())
        );
    }

    #[test]
    fn applies_defaults_for_optional_fields() {
        let config =
            Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n")
                .expect("parse");
        assert_eq!(config.max_connections, 8);
        assert_eq!(config.jsonl.sink, JsonlSink::None);
        assert_eq!(config.jsonl.stage_health_period_s, 30);
        assert!(config.record.enabled);
        assert_eq!(config.record.dir, PathBuf::from("./framelogs"));
        assert_eq!(config.record.cap_bytes, 8_000_000_000);
        assert_eq!(config.record.roll_max_bytes, 67_108_864);
        assert_eq!(config.record.roll_max_age_s, 900);
        assert_eq!(config.pipeline.segment_queue_depth, 8);
        assert_eq!(config.pipeline.max_segment_seconds, 60);
        assert!(config.pods.is_empty());
        assert!(config.wake.is_none());
    }

    #[test]
    fn pod_cap_bytes_parses_and_resolves() {
        // Explicit value parses and is returned as-is.
        let cfg = Config::parse(
            "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[record]\ncap_bytes = 1000\npod_cap_bytes = 250\n",
        )
        .expect("parse");
        assert_eq!(cfg.record.pod_cap_bytes, Some(250));
        assert_eq!(cfg.record.resolved_pod_cap_bytes(), 250);

        // Absent → half the global cap.
        let cfg = Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[record]\ncap_bytes = 1000\n")
            .expect("parse");
        assert_eq!(cfg.record.pod_cap_bytes, None);
        assert_eq!(cfg.record.resolved_pod_cap_bytes(), 500);
    }

    #[test]
    fn wake_absent_table_is_none() {
        let config =
            Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n")
                .expect("parse");
        assert!(config.wake.is_none());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn wake_bypass_ignores_model_paths() {
        let config = Config::parse(
            "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[wake]\nmode = \"bypass\"\nmodel = \"/m/wake.onnx\"",
        )
        .expect("parse");
        let wake = config.wake.as_ref().expect("wake table");
        assert_eq!(wake.mode, WakeMode::Bypass);
        assert_eq!(wake.threshold, 0.5);
        assert_eq!(wake.model, Some(PathBuf::from("/m/wake.onnx")));
        // Bypass accepts (and ignores) model paths — no missing-path rejection.
        assert!(config.validate().is_ok());
    }

    #[test]
    fn wake_oww_full() {
        let text = r#"
listen_addr = "10.0.0.5:7380"
pod_psk_file = "/psk.toml"
[wake]
mode = "oww"
melspectrogram = "/m/mel.onnx"
embedding = "/m/emb.onnx"
model = "/m/wake.onnx"
threshold = 0.7
"#;
        let config = Config::parse(text).expect("parse");
        let wake = config.wake.as_ref().expect("wake table");
        assert_eq!(wake.mode, WakeMode::Oww);
        assert_eq!(wake.melspectrogram, Some(PathBuf::from("/m/mel.onnx")));
        assert_eq!(wake.embedding, Some(PathBuf::from("/m/emb.onnx")));
        assert_eq!(wake.model, Some(PathBuf::from("/m/wake.onnx")));
        assert_eq!(wake.threshold, 0.7);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn wake_oww_missing_paths_rejected() {
        for (name, table) in [
            (
                "melspectrogram",
                "[wake]\nmode = \"oww\"\nembedding = \"/m/emb.onnx\"\nmodel = \"/m/wake.onnx\"",
            ),
            (
                "embedding",
                "[wake]\nmode = \"oww\"\nmelspectrogram = \"/m/mel.onnx\"\nmodel = \"/m/wake.onnx\"",
            ),
            (
                "model",
                "[wake]\nmode = \"oww\"\nmelspectrogram = \"/m/mel.onnx\"\nembedding = \"/m/emb.onnx\"",
            ),
        ] {
            let config = Config::parse(&format!(
                "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n{table}"
            ))
            .expect("parse");
            let err = config.validate().unwrap_err();
            assert!(err.contains(name), "expected {name} in message: {err}");
        }
    }

    #[test]
    fn wake_threshold_bounds_rejected() {
        for bad in ["0.0", "1.0", "-0.1", "1.5"] {
            let config = Config::parse(&format!(
                "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[wake]\nmode = \"bypass\"\nthreshold = {bad}"
            ))
            .expect("parse");
            let err = config.validate().unwrap_err();
            assert!(err.contains("threshold"), "threshold {bad}: {err}");
        }
    }

    #[test]
    fn wake_default_threshold_in_bounds() {
        let config =
            Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[wake]\nmode = \"oww\"\nmelspectrogram = \"/m/mel.onnx\"\nembedding = \"/m/emb.onnx\"\nmodel = \"/m/wake.onnx\"")
                .expect("parse");
        assert_eq!(config.wake.as_ref().unwrap().threshold, 0.5);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn wake_rejects_unknown_key() {
        let err =
            Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[wake]\nmode = \"bypass\"\nbogus = 1")
                .unwrap_err();
        assert!(err.to_string().contains("bogus"), "message: {err}");
    }

    #[test]
    fn wake_rejects_missing_mode() {
        let err =
            Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[wake]\nthreshold = 0.6").unwrap_err();
        assert!(err.to_string().contains("mode"), "message: {err}");
    }

    #[test]
    fn wake_rejects_unknown_mode() {
        assert!(
            Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[wake]\nmode = \"magic\"").is_err()
        );
    }

    #[test]
    fn endpointer_absent_table_is_none() {
        let config =
            Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n")
                .expect("parse");
        assert!(config.endpointer.is_none());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn endpointer_defaults_when_only_model_given() {
        let config = Config::parse(
            "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[endpointer]\nmodel = \"/m/silero.onnx\"",
        )
        .expect("parse");
        let ep = config.endpointer.as_ref().expect("endpointer table");
        assert_eq!(ep.model, PathBuf::from("/m/silero.onnx"));
        // Single-sourced from the listener's canonical `EndpointerConfig::default()`
        // (8 chunks = 256 ms hangover, 31 chunks = 992 ms continuation, 8000 samples
        // = 500 ms preroll), so the ms defaults track the chunk-count truth exactly.
        assert_eq!(ep.onset_thresh, 0.5);
        assert_eq!(ep.release_thresh, 0.35);
        assert_eq!(ep.onset_chunks, 3);
        assert_eq!(ep.soft_hangover_ms, 256);
        assert_eq!(ep.continuation_window_ms, 992);
        assert_eq!(ep.preroll_pad_ms, 500);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn endpointer_to_listener_quantizes_and_derives_cap() {
        // The ms/threshold table converts to the listener's chunk/sample knobs, and
        // `max_utterance_samples` is supplied by the caller (the pipeline cap), not
        // the table. Round-trips the defaults back to the canonical listener config.
        let config = Config::parse(
            "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[endpointer]\nmodel = \"/m/silero.onnx\"",
        )
        .expect("parse");
        let ep = config.endpointer.as_ref().expect("endpointer table");
        let listener = ep.to_listener(30 * 16_000);
        let canonical = ListenerEndpointerConfig::default();
        assert_eq!(listener.onset_thresh, canonical.onset_thresh);
        assert_eq!(listener.release_thresh, canonical.release_thresh);
        assert_eq!(listener.onset_chunks, canonical.onset_chunks);
        assert_eq!(
            listener.soft_hangover_chunks,
            canonical.soft_hangover_chunks
        );
        assert_eq!(listener.continuation_chunks, canonical.continuation_chunks);
        assert_eq!(listener.preroll_pad_samples, canonical.preroll_pad_samples);
        // The cap is the pipeline's, not the endpointer default's.
        assert_eq!(listener.max_utterance_samples, 30 * 16_000);
    }

    #[test]
    fn endpointer_requires_model() {
        let err =
            Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[endpointer]\nonset_thresh = 0.6")
                .unwrap_err();
        assert!(err.to_string().contains("model"), "message: {err}");
    }

    #[test]
    fn endpointer_rejects_out_of_range_threshold() {
        for bad in ["0.0", "1.0", "-0.1"] {
            let config = Config::parse(&format!(
                "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[endpointer]\nmodel = \"/m/s.onnx\"\nonset_thresh = {bad}"
            ))
            .expect("parse");
            let err = config.validate().unwrap_err();
            assert!(err.contains("onset_thresh"), "onset {bad}: {err}");
        }
    }

    #[test]
    fn endpointer_rejects_release_above_onset() {
        let config = Config::parse(
            "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[endpointer]\nmodel = \"/m/s.onnx\"\nonset_thresh = 0.4\nrelease_thresh = 0.6",
        )
        .expect("parse");
        let err = config.validate().unwrap_err();
        assert!(err.contains("release_thresh"), "message: {err}");
    }

    #[test]
    fn endpointer_rejects_zero_onset_chunks() {
        let config = Config::parse(
            "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[endpointer]\nmodel = \"/m/s.onnx\"\nonset_chunks = 0",
        )
        .expect("parse");
        let err = config.validate().unwrap_err();
        assert!(err.contains("onset_chunks"), "message: {err}");
    }

    #[test]
    fn endpointer_rejects_sub_chunk_hangover_and_continuation() {
        for field in ["soft_hangover_ms", "continuation_window_ms"] {
            for bad in ["0", "31"] {
                let config = Config::parse(&format!(
                    "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[endpointer]\nmodel = \"/m/s.onnx\"\n{field} = {bad}"
                ))
                .expect("parse");
                let err = config.validate().unwrap_err();
                assert!(err.contains(field), "{field} = {bad}: {err}");
            }
        }
    }

    #[test]
    fn endpointer_rejects_unknown_key() {
        let err = Config::parse(
            "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[endpointer]\nmodel = \"/m/s.onnx\"\nbogus = 1",
        )
        .unwrap_err();
        assert!(err.to_string().contains("bogus"), "message: {err}");
    }

    #[test]
    fn brain_absent_table_is_none() {
        let config =
            Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n")
                .expect("parse");
        assert!(config.brain.is_none());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn brain_wav_full() {
        let config = Config::parse(
            "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[brain]\nmode = \"wav\"\nclip = \"/clips/ack.wav\"",
        )
        .expect("parse");
        let brain = config.brain.as_ref().expect("brain table");
        assert_eq!(brain.mode, BrainMode::Wav);
        assert_eq!(brain.clip, Some(PathBuf::from("/clips/ack.wav")));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn brain_wav_missing_clip_rejected() {
        let config = Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[brain]\nmode = \"wav\"")
            .expect("parse");
        let err = config.validate().unwrap_err();
        assert!(err.contains("clip"), "expected clip in message: {err}");
    }

    #[test]
    fn brain_rejects_missing_mode() {
        let err = Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[brain]\nclip = \"/c/a.wav\"")
            .unwrap_err();
        assert!(err.to_string().contains("mode"), "message: {err}");
    }

    #[test]
    fn brain_rejects_unknown_mode() {
        assert!(Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[brain]\nmode = \"llm\"").is_err());
    }

    #[test]
    fn brain_rejects_unknown_key() {
        let err =
            Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[brain]\nmode = \"wav\"\nbogus = 1")
                .unwrap_err();
        assert!(err.to_string().contains("bogus"), "message: {err}");
    }

    #[test]
    fn playback_defaults_when_absent() {
        let config =
            Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n")
                .expect("parse");
        assert_eq!(
            config.playback.lead_ms,
            audio_pipeline::playback::PLAYBACK_BURST_LEAD_MS
        );
        assert_eq!(config.playback.write_timeout_ms, 1000);
        assert_eq!(config.playback.speak_queue_depth, 8);
        assert_eq!(config.playback.job_queue_depth, 2);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn playback_full_table_parses() {
        let text = r#"
listen_addr = "10.0.0.5:7380"
pod_psk_file = "/psk.toml"
[playback]
lead_ms = 300
write_timeout_ms = 500
speak_queue_depth = 16
job_queue_depth = 4
"#;
        let config = Config::parse(text).expect("parse");
        assert_eq!(config.playback.lead_ms, 300);
        assert_eq!(config.playback.write_timeout_ms, 500);
        assert_eq!(config.playback.speak_queue_depth, 16);
        assert_eq!(config.playback.job_queue_depth, 4);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn playback_rejects_lead_below_one_frame() {
        for bad in ["0", "19"] {
            let config = Config::parse(&format!(
                "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[playback]\nlead_ms = {bad}"
            ))
            .expect("parse");
            let err = config.validate().unwrap_err();
            assert!(err.contains("lead_ms"), "lead_ms {bad}: {err}");
        }
        // Exactly one frame is accepted.
        let config = Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[playback]\nlead_ms = 20")
            .expect("parse");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn playback_rejects_lead_exceeding_combined_ring_budget() {
        // The lead co-resides in the ring with the escalated pre-roll cap and one
        // max frame; the lead budget is the ring minus those two terms. One ms is
        // 32 B (640 B/frame ÷ 20 ms), so the max acceptable lead is 1 048 ms.
        let bytes_per_ms = (INBOUND_PCM_WRITE_UNIT_BYTES as u64) / FRAME_MS;
        let lead_budget_bytes =
            (INBOUND_PCM_RING_BYTES - PLAYBACK_PREROLL_MAX_TARGET_BYTES - MAX_AUDIO_PAYLOAD) as u64;
        let max_lead_ms = lead_budget_bytes / bytes_per_ms;
        // Independent value pin: the derivation above shares its formula with the
        // production `LEAD_BUDGET_BYTES`, so a matching mistake in both would pass
        // unnoticed. Pinning the concrete cap trips if either copy's arithmetic
        // silently changes.
        assert_eq!(
            max_lead_ms, 1_048,
            "combined-budget cap drifted from 1048 ms"
        );

        // The exact combined-budget cap is accepted.
        let config = Config::parse(&format!(
            "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[playback]\nlead_ms = {max_lead_ms}"
        ))
        .expect("parse");
        assert!(
            config.validate().is_ok(),
            "cap {max_lead_ms} ms should pass"
        );

        // One ms past the combined budget over-banks the device and is rejected.
        let over = max_lead_ms + 1;
        let config = Config::parse(&format!(
            "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[playback]\nlead_ms = {over}"
        ))
        .expect("parse");
        let err = config.validate().unwrap_err();
        assert!(err.contains("lead_ms"), "over-budget lead: {err}");
        assert!(err.contains("playout"), "over-budget lead: {err}");
        // The message's actionable payload is the computed max-lead hint; a wrong
        // divisor or a dropped hint must trip the test, not slip through.
        assert!(
            err.contains(&format!("{max_lead_ms} ms")),
            "over-budget lead must advertise the max acceptable lead: {err}"
        );

        // The old bare-ring cap (2 048 ms) now falls inside the hazard band and is
        // rejected — this pins the fix itself, not just the new boundary.
        let old_cap = (INBOUND_PCM_RING_BYTES as u64) / bytes_per_ms;
        assert!(old_cap > max_lead_ms, "old cap must exceed the new one");
        let config = Config::parse(&format!(
            "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[playback]\nlead_ms = {old_cap}"
        ))
        .expect("parse");
        assert!(
            config.validate().is_err(),
            "old bare-ring cap {old_cap} ms must now reject"
        );

        // An absurd lead whose byte-equivalent overflows u64 is rejected, not wrapped
        // into a spurious pass (or a debug-build multiply panic). TOML tops out at
        // i64::MAX, whose ×32 byte-equivalent overflows u64.
        let huge = i64::MAX as u64;
        let config = Config::parse(&format!(
            "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[playback]\nlead_ms = {huge}"
        ))
        .expect("parse");
        let err = config.validate().unwrap_err();
        assert!(err.contains("lead_ms"), "overflow lead: {err}");
    }

    #[test]
    fn playback_rejects_zero_write_timeout() {
        let config =
            Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[playback]\nwrite_timeout_ms = 0")
                .expect("parse");
        let err = config.validate().unwrap_err();
        assert!(err.contains("write_timeout_ms"), "message: {err}");
    }

    #[test]
    fn playback_rejects_zero_queue_depths() {
        for (field, table) in [
            ("speak_queue_depth", "[playback]\nspeak_queue_depth = 0"),
            ("job_queue_depth", "[playback]\njob_queue_depth = 0"),
        ] {
            let config = Config::parse(&format!(
                "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n{table}"
            ))
            .expect("parse");
            let err = config.validate().unwrap_err();
            assert!(err.contains(field), "expected {field} in message: {err}");
        }
    }

    #[test]
    fn playback_rejects_unknown_key() {
        let err = Config::parse(
            "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[playback]\nbogus = 1",
        )
        .unwrap_err();
        assert!(err.to_string().contains("bogus"), "message: {err}");
    }

    #[test]
    fn sink_stdout_keyword() {
        let config = Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[jsonl]\nsink = \"stdout\"")
            .expect("parse");
        assert_eq!(config.jsonl.sink, JsonlSink::Stdout);
    }

    #[test]
    fn rejects_unknown_key() {
        let err = Config::parse(
            "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\nbogus = 1",
        )
        .unwrap_err();
        assert!(err.to_string().contains("bogus"), "message: {err}");
    }

    #[test]
    fn rejects_unknown_key_in_nested_table() {
        // `deny_unknown_fields` applies at every nesting level, not just the top.
        let err =
            Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[record]\nenabled = true\ntypo = 1")
                .unwrap_err();
        assert!(err.to_string().contains("typo"), "message: {err}");
    }

    #[test]
    fn sink_dash_alias_is_stdout() {
        let config = Config::parse(
            "listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[jsonl]\nsink = \"-\"",
        )
        .expect("parse");
        assert_eq!(config.jsonl.sink, JsonlSink::Stdout);
    }

    #[test]
    fn sink_none_keyword() {
        let config = Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[jsonl]\nsink = \"none\"")
            .expect("parse");
        assert_eq!(config.jsonl.sink, JsonlSink::None);
    }

    #[test]
    fn validate_rejects_unspecified_listen_addr() {
        let config =
            Config::parse("listen_addr = \"0.0.0.0:7380\"\npod_psk_file = \"/psk.toml\"\n")
                .expect("parse");
        let err = config.validate().unwrap_err();
        assert!(err.contains("listen_addr"), "message: {err}");
    }

    #[test]
    fn validate_rejects_zero_segment_queue_depth() {
        let config =
            Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n[pipeline]\nsegment_queue_depth = 0")
                .expect("parse");
        let err = config.validate().unwrap_err();
        assert!(err.contains("segment_queue_depth"), "message: {err}");
    }

    #[test]
    fn validate_accepts_concrete_addr_and_defaults() {
        let config =
            Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n")
                .expect("parse");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn rejects_missing_listen_addr() {
        let err = Config::parse("max_connections = 4").unwrap_err();
        assert!(err.to_string().contains("listen_addr"), "message: {err}");
    }

    #[test]
    fn rejects_unparseable_listen_addr() {
        assert!(
            Config::parse("listen_addr = \"not-an-address\"\npod_psk_file = \"/psk.toml\"\n")
                .is_err()
        );
    }

    #[test]
    fn room_lookup_unmapped() {
        let config =
            Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n")
                .expect("parse");
        let lookup = config.room_for("pod-unknown");
        assert_eq!(lookup, RoomLookup::Unmapped);
        assert!(lookup.is_unmapped());
        assert_eq!(lookup.room(), UNMAPPED_ROOM);
    }

    #[test]
    fn load_reports_missing_file_with_path() {
        let err = Config::load(Path::new("/nonexistent/speech-surface.toml")).unwrap_err();
        assert!(matches!(err, ConfigError::Read { .. }));
        assert!(err.to_string().contains("/nonexistent/speech-surface.toml"));
    }

    /// Base config plus the `[stt]` and `[tts]` HTTP tables — the parrot's stage
    /// config, reused by several cross-table tests.
    const STT_TTS_TABLES: &str = r#"
[stt]
backend = "http"
url = "http://10.0.0.5:8000"
model = "Systran/faster-whisper-small"
language = "en"

[tts]
backend = "http"
url = "http://10.0.0.5:8000"
model = "speaches-ai/Kokoro-82M-v1.0-ONNX"
voice = "af_heart"
"#;

    fn with_addr(body: &str) -> String {
        format!("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n{body}")
    }

    #[test]
    fn stt_tts_absent_tables_are_none() {
        let config =
            Config::parse("listen_addr = \"10.0.0.5:7380\"\npod_psk_file = \"/psk.toml\"\n")
                .expect("parse");
        assert!(config.stt.is_none());
        assert!(config.tts.is_none());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn stt_tts_http_full_parses_with_defaults() {
        let config = Config::parse(&with_addr(STT_TTS_TABLES)).expect("parse");
        let stt = config.stt.as_ref().expect("stt table");
        assert_eq!(stt.backend, SttBackend::Http);
        assert_eq!(stt.url.as_deref(), Some("http://10.0.0.5:8000"));
        assert_eq!(stt.model.as_deref(), Some("Systran/faster-whisper-small"));
        assert_eq!(stt.language.as_deref(), Some("en"));
        assert_eq!(stt.timeout_ms, 15_000);
        assert_eq!(stt.connect_timeout_ms, 2_000);
        let tts = config.tts.as_ref().expect("tts table");
        assert_eq!(tts.backend, TtsBackend::Http);
        assert_eq!(tts.voice.as_deref(), Some("af_heart"));
        assert_eq!(tts.timeout_ms, 15_000);
        assert_eq!(tts.connect_timeout_ms, 2_000);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn stt_language_optional() {
        let config = Config::parse(&with_addr(
            "[stt]\nbackend = \"http\"\nurl = \"http://h:8000\"\nmodel = \"m\"",
        ))
        .expect("parse");
        assert!(config.stt.as_ref().unwrap().language.is_none());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn stt_alone_without_brain_is_valid() {
        // A transcriber with no brain enriches the utterance line — legal and useful.
        let config = Config::parse(&with_addr(
            "[stt]\nbackend = \"http\"\nurl = \"http://h:8000\"\nmodel = \"m\"",
        ))
        .expect("parse");
        assert!(config.tts.is_none());
        assert!(config.brain.is_none());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn stt_http_missing_url_rejected() {
        let config =
            Config::parse(&with_addr("[stt]\nbackend = \"http\"\nmodel = \"m\"")).expect("parse");
        let err = config.validate().unwrap_err();
        assert!(err.contains("stt.url"), "message: {err}");
    }

    #[test]
    fn stt_http_missing_model_rejected() {
        let config = Config::parse(&with_addr(
            "[stt]\nbackend = \"http\"\nurl = \"http://h:8000\"",
        ))
        .expect("parse");
        let err = config.validate().unwrap_err();
        assert!(err.contains("stt.model"), "message: {err}");
    }

    #[test]
    fn tts_http_missing_voice_rejected() {
        let config = Config::parse(&with_addr(
            "[tts]\nbackend = \"http\"\nurl = \"http://h:8000\"\nmodel = \"m\"",
        ))
        .expect("parse");
        let err = config.validate().unwrap_err();
        assert!(err.contains("tts.voice"), "message: {err}");
    }

    #[test]
    fn https_scheme_rejected_naming_deferral() {
        for (table, field) in [
            (
                "[stt]\nbackend = \"http\"\nurl = \"https://h:8000\"\nmodel = \"m\"",
                "stt.url",
            ),
            (
                "[tts]\nbackend = \"http\"\nurl = \"https://h:8000\"\nmodel = \"m\"\nvoice = \"v\"",
                "tts.url",
            ),
        ] {
            let config = Config::parse(&with_addr(table)).expect("parse");
            let err = config.validate().unwrap_err();
            assert!(err.contains(field), "{field}: {err}");
            assert!(err.contains("TLS"), "names the TLS deferral: {err}");
        }
    }

    #[test]
    fn non_http_scheme_rejected() {
        // A scheme that parses cleanly but is neither http nor https takes the
        // third validate_http_url branch — a distinct message, not the TLS one.
        let config = Config::parse(&with_addr(
            "[stt]\nbackend = \"http\"\nurl = \"ftp://h:8000\"\nmodel = \"m\"",
        ))
        .expect("parse");
        let err = config.validate().unwrap_err();
        assert!(err.contains("stt.url"), "message: {err}");
        assert!(err.contains("ftp"), "names the offending scheme: {err}");
        assert!(!err.contains("TLS"), "not the https/TLS message: {err}");
    }

    #[test]
    fn url_with_embedded_credentials_rejected() {
        let config = Config::parse(&with_addr(
            "[stt]\nbackend = \"http\"\nurl = \"http://user:secret@h:8000\"\nmodel = \"m\"",
        ))
        .expect("parse");
        let err = config.validate().unwrap_err();
        assert!(err.contains("stt.url"), "message: {err}");
        assert!(
            err.contains("credential"),
            "names the credential problem: {err}"
        );
        assert!(!err.contains("secret"), "never echoes the secret: {err}");
    }

    #[test]
    fn unparseable_url_rejected() {
        let config = Config::parse(&with_addr(
            "[stt]\nbackend = \"http\"\nurl = \"http://[not a url\"\nmodel = \"m\"",
        ))
        .expect("parse");
        let err = config.validate().unwrap_err();
        assert!(err.contains("stt.url"), "message: {err}");
    }

    #[test]
    fn zero_timeouts_rejected() {
        for (table, field) in [
            (
                "[stt]\nbackend = \"http\"\nurl = \"http://h:8000\"\nmodel = \"m\"\ntimeout_ms = 0",
                "stt.timeout_ms",
            ),
            (
                "[stt]\nbackend = \"http\"\nurl = \"http://h:8000\"\nmodel = \"m\"\nconnect_timeout_ms = 0",
                "stt.connect_timeout_ms",
            ),
        ] {
            let config = Config::parse(&with_addr(table)).expect("parse");
            let err = config.validate().unwrap_err();
            assert!(err.contains(field), "{field}: {err}");
        }
    }

    #[test]
    fn stt_rejects_unknown_key() {
        let err = Config::parse(&with_addr("[stt]\nbackend = \"http\"\nbogus = 1")).unwrap_err();
        assert!(err.to_string().contains("bogus"), "message: {err}");
    }

    #[test]
    fn stt_rejects_unknown_backend() {
        assert!(Config::parse(&with_addr("[stt]\nbackend = \"embedded\"")).is_err());
    }

    #[test]
    fn stt_confidence_gate_defaults() {
        let config = Config::parse(&with_addr(
            "[stt]\nbackend = \"http\"\nurl = \"http://h:8000\"\nmodel = \"m\"",
        ))
        .expect("parse");
        let stt = config.stt.as_ref().unwrap();
        assert_eq!(stt.no_speech_max, 0.2);
        assert!(stt.avg_logprob_min.is_none());
        let gate = stt.confidence_gate();
        assert_eq!(gate.no_speech_max, 0.2);
        assert!(gate.avg_logprob_min.is_none());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn stt_confidence_gate_custom_values_parse() {
        let config = Config::parse(&with_addr(
            "[stt]\nbackend = \"http\"\nurl = \"http://h:8000\"\nmodel = \"m\"\nno_speech_max = 0.35\navg_logprob_min = -0.9",
        ))
        .expect("parse");
        let gate = config.stt.as_ref().unwrap().confidence_gate();
        assert_eq!(gate.no_speech_max, 0.35);
        assert_eq!(gate.avg_logprob_min, Some(-0.9));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn stt_rejects_out_of_range_no_speech_max() {
        for bad in ["-0.1", "1.5"] {
            let config = Config::parse(&with_addr(&format!(
                "[stt]\nbackend = \"http\"\nurl = \"http://h:8000\"\nmodel = \"m\"\nno_speech_max = {bad}"
            )))
            .expect("parse");
            let err = config.validate().unwrap_err();
            assert!(err.contains("no_speech_max"), "no_speech_max {bad}: {err}");
        }
        // The bounds are inclusive: 0.0 (gate everything) and 1.0 (gate nothing)
        // are both legal.
        for ok in ["0.0", "1.0"] {
            let config = Config::parse(&with_addr(&format!(
                "[stt]\nbackend = \"http\"\nurl = \"http://h:8000\"\nmodel = \"m\"\nno_speech_max = {ok}"
            )))
            .expect("parse");
            assert!(config.validate().is_ok(), "no_speech_max {ok} should pass");
        }
    }

    #[test]
    fn stt_rejects_non_finite_avg_logprob_min() {
        // TOML admits `nan`/`inf` float literals. A non-finite floor is degenerate:
        // NaN never fires (every comparison against NaN is false) and ±inf gates
        // all-or-nothing, so an opted-in secondary gate would be silently inert —
        // reject it at startup rather than let it pass validation.
        for bad in ["nan", "inf", "-inf"] {
            let config = Config::parse(&with_addr(&format!(
                "[stt]\nbackend = \"http\"\nurl = \"http://h:8000\"\nmodel = \"m\"\navg_logprob_min = {bad}"
            )))
            .expect("parse");
            let err = config.validate().unwrap_err();
            assert!(
                err.contains("avg_logprob_min"),
                "avg_logprob_min {bad}: {err}"
            );
        }
        // A finite floor is accepted.
        let config = Config::parse(&with_addr(
            "[stt]\nbackend = \"http\"\nurl = \"http://h:8000\"\nmodel = \"m\"\navg_logprob_min = -0.9",
        ))
        .expect("parse");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn brain_echo_parses() {
        let config = Config::parse(&with_addr(&format!(
            "[brain]\nmode = \"echo\"\n{STT_TTS_TABLES}"
        )))
        .expect("parse");
        assert_eq!(config.brain.as_ref().unwrap().mode, BrainMode::Echo);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn brain_echo_without_stt_rejected() {
        let tts_only = r#"
[brain]
mode = "echo"
[tts]
backend = "http"
url = "http://h:8000"
model = "m"
voice = "v"
"#;
        let config = Config::parse(&with_addr(tts_only)).expect("parse");
        let err = config.validate().unwrap_err();
        assert!(err.contains("[stt]"), "message: {err}");
    }

    #[test]
    fn brain_echo_without_tts_rejected() {
        let stt_only = r#"
[brain]
mode = "echo"
[stt]
backend = "http"
url = "http://h:8000"
model = "m"
"#;
        let config = Config::parse(&with_addr(stt_only)).expect("parse");
        let err = config.validate().unwrap_err();
        assert!(err.contains("[tts]"), "message: {err}");
    }

    #[test]
    fn config_without_pod_psk_file_is_rejected() {
        let err = Config::parse("listen_addr = \"10.0.0.5:7380\"").unwrap_err();
        assert!(err.to_string().contains("pod_psk_file"), "message: {err}",);
    }

    const KEY_A: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

    #[test]
    fn psk_table_parses_pod_keys() {
        let table = PskTable::parse(&format!(
            "pod-aabbcc = \"{KEY_A}\"\npod-ddeeff = \"{}\"\n",
            "ff".repeat(32)
        ))
        .expect("parses");
        assert_eq!(table.len(), 2);
        assert_eq!(table.key_for("pod-aabbcc").expect("key")[0], 0x00);
        assert_eq!(table.key_for("pod-aabbcc").expect("key")[31], 0xff);
        assert_eq!(table.key_for("pod-ddeeff").expect("key"), &[0xff; 32]);
        assert!(table.key_for("pod-unknown").is_none());
    }

    #[test]
    fn psk_table_rejects_non_hex() {
        let err = PskTable::parse(&format!("pod-a = \"{}zz\"\n", &KEY_A[..62])).unwrap_err();
        assert!(err.contains("hexadecimal"), "message: {err}");
    }

    #[test]
    fn psk_table_rejects_wrong_length() {
        let err = PskTable::parse(&format!("pod-a = \"{}\"\n", &KEY_A[..62])).unwrap_err();
        assert!(err.contains("62 characters"), "message: {err}");
    }

    #[test]
    fn psk_table_rejects_non_string_entry() {
        let err = PskTable::parse("pod-a = 12345\n").unwrap_err();
        assert!(err.contains("64-hex"), "message: {err}");
    }

    #[test]
    fn psk_table_rejects_duplicate_pod_id() {
        let err =
            PskTable::parse(&format!("pod-a = \"{KEY_A}\"\npod-a = \"{KEY_A}\"\n")).unwrap_err();
        assert!(err.contains("valid TOML"), "message: {err}");
    }

    #[test]
    fn psk_table_rejects_empty_file() {
        let err = PskTable::parse("# no pods\n").unwrap_err();
        assert!(err.contains("no pods"), "message: {err}");
    }

    #[test]
    fn psk_table_debug_redacts_keys() {
        let table = PskTable::parse(&format!("pod-a = \"{KEY_A}\"\n")).expect("parses");
        let rendered = format!("{table:?}");
        assert_eq!(rendered, "PskTable { pods: [\"pod-a\"], key_bytes: 32 }");
        // Every 4-hex-character window of the key, in both cases: a leak of any
        // slice of it — head, tail, or middle — fails, not just the first bytes.
        let upper = KEY_A.to_ascii_uppercase();
        for hex in [KEY_A, upper.as_str()] {
            for window in hex.as_bytes().windows(4) {
                let window = std::str::from_utf8(window).expect("ascii hex");
                assert!(
                    !rendered.contains(window),
                    "rendered {rendered:?} leaks key material at {window:?}",
                );
            }
        }
    }

    /// A secrets file any other local account can read is a startup failure, not
    /// a warning — the whole fleet's keys sit in it.
    #[cfg(unix)]
    #[test]
    fn psk_table_rejects_group_readable_file() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("psk.toml");
        std::fs::write(&path, format!("pod-a = \"{KEY_A}\"\n")).expect("write");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).expect("chmod");
        let err = PskTable::load(&path).unwrap_err();
        assert!(
            err.to_string().contains("group/world-accessible"),
            "message: {err}",
        );
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
        let table = PskTable::load(&path).expect("loads at 0600");
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn psk_table_load_reports_a_missing_file() {
        let err = PskTable::load(Path::new("/nonexistent/psk.toml")).unwrap_err();
        assert!(err.to_string().contains("/nonexistent/psk.toml"), "{err}");
    }
}
