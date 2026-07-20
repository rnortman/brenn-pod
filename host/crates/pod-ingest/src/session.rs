//! Pod session state: the sans-I/O `SessionFsm` and its cross-connection
//! `ResumeLedger`.
//!
//! The FSM is fed decoded `StreamFrame`s plus a host-receive timestamp and
//! returns typed `SessionEvent`s in order. It reads no clock, owns no sockets,
//! threads, or files — the caller supplies frames and timestamps.
//!
//! A per-connection FSM cannot own cross-connection resume state and stay
//! sans-I/O, so the ledger is an `Arc<Mutex<ResumeLedger>>` handle the embedder
//! injects at construction; the FSM locks it only inside the two boundary
//! helpers that touch it (`open_segment`, `truncate_open_segment`), never across
//! the O(samples) audio decode. Holding that mutex is not I/O — it guards
//! in-memory state only — so the sans-I/O boundary stands. It is bounded per pod
//! (cap `PER_POD_CAP`) and in the number
//! of distinct pods (cap `MAX_PODS`, LRU) — pod ids arrive untrusted from the
//! wire `Hello`, so neither dimension can accumulate without limit.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use audio_pipeline::wire::{StreamFrame, AUDIO_PROTOCOL_VERSION};

use serde::Serialize;

use crate::clock::{DeviceMicros, HostMicros};

// Re-export the wire types that appear in `SessionFsm`'s public API so
// downstream crates consume them via `pod-ingest` and need no `audio-pipeline`
// edge of their own.
pub use audio_pipeline::wire::{ChannelSource, Codec, EndReason, TelemetryKind};

/// Per-pod cap on retained truncated-segment ids. Segment ids are per-boot
/// monotonic, so an un-resumed entry would otherwise accumulate forever across
/// pod reboots; past the cap the oldest id for that pod is evicted.
const PER_POD_CAP: usize = 1024;

/// Cap on distinct pods tracked at once. Pod ids come from the wire and unknown
/// pods are accepted, so a churning or hostile device could otherwise grow the
/// map without limit; past the cap the least-recently-noted pod's bucket is
/// evicted. Generous for a home fleet.
const MAX_PODS: usize = 256;

/// Tracks `(pod_id, segment_id)` pairs whose segment was truncated, so a later
/// connection carrying a `SegmentStart` for the same pair can be recognized as
/// a resume. Bounded per pod (cap `PER_POD_CAP`, evict-oldest id) and across
/// pods (cap `MAX_PODS`, evict least-recently-noted pod); both eviction kinds
/// are counted and queryable.
#[derive(Debug, Default)]
pub struct ResumeLedger {
    per_pod: HashMap<String, PodEntries>,
    /// Pod ids in least-to-most-recently-noted order, for `MAX_PODS` eviction.
    pod_order: VecDeque<String>,
    evictions: u64,
    pod_evictions: u64,
}

/// One pod's truncated ids: a membership set plus an insertion-order queue so
/// the oldest can be evicted at the cap.
#[derive(Debug, Default)]
struct PodEntries {
    members: HashSet<u32>,
    order: VecDeque<u32>,
}

impl ResumeLedger {
    pub fn new() -> Self {
        Self::default()
    }

    /// A fresh ledger wrapped in the `Arc<Mutex<_>>` handle that `SessionFsm`
    /// and the daemon share.
    pub fn shared() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self::new()))
    }

    /// Record that `segment_id` for `pod` was truncated. A duplicate id is a
    /// no-op (it is already resumable) but still refreshes the pod's recency. At
    /// the per-pod cap the oldest retained id for that pod is evicted; past
    /// `MAX_PODS` distinct pods the least-recently-noted pod is evicted whole.
    /// Both eviction kinds increment their counters.
    pub fn note_truncated(&mut self, pod: &str, segment_id: u32) {
        let entry = self.per_pod.entry(pod.to_string()).or_default();
        let mut id_evicted = false;
        if entry.members.insert(segment_id) {
            entry.order.push_back(segment_id);
            if entry.order.len() > PER_POD_CAP {
                if let Some(oldest) = entry.order.pop_front() {
                    entry.members.remove(&oldest);
                    id_evicted = true;
                }
            }
        }
        if id_evicted {
            self.evictions += 1;
        }
        self.touch_pod(pod);
        self.evict_pods_over_cap();
    }

    /// Move `pod` to the most-recently-noted end of the recency queue.
    fn touch_pod(&mut self, pod: &str) {
        if let Some(pos) = self.pod_order.iter().position(|p| p == pod) {
            self.pod_order.remove(pos);
        }
        self.pod_order.push_back(pod.to_string());
    }

    /// Drop least-recently-noted pods whole while over `MAX_PODS`.
    fn evict_pods_over_cap(&mut self) {
        while self.pod_order.len() > MAX_PODS {
            if let Some(pod) = self.pod_order.pop_front() {
                self.per_pod.remove(&pod);
                self.pod_evictions += 1;
            }
        }
    }

    /// If `(pod, segment_id)` was noted truncated, remove it and return `true`
    /// — a resumed segment cannot be resumed again. Otherwise return `false`.
    pub fn take_resume(&mut self, pod: &str, segment_id: u32) -> bool {
        let Some(entry) = self.per_pod.get_mut(pod) else {
            return false;
        };
        if !entry.members.remove(&segment_id) {
            return false;
        }
        if let Some(pos) = entry.order.iter().position(|&id| id == segment_id) {
            entry.order.remove(pos);
        }
        if entry.members.is_empty() {
            self.per_pod.remove(pod);
            if let Some(pos) = self.pod_order.iter().position(|p| p == pod) {
                self.pod_order.remove(pos);
            }
        }
        true
    }

    /// Total number of ids evicted at the per-pod cap over this ledger's life
    /// — surfaced in `stage_health` so silent loss is observable.
    pub fn evictions(&self) -> u64 {
        self.evictions
    }

    /// Total number of whole-pod buckets evicted at the `MAX_PODS` cap — also
    /// surfaced in `stage_health`, so pod-id churn is observable.
    pub fn pod_evictions(&self) -> u64 {
        self.pod_evictions
    }
}

// ── Format constraint ──────────────────────────────────────────────────────

/// The PCM/handshake format an embedder accepts. Supplied to [`SessionFsm::new`]
/// so `pod-ingest` stays policy-free: the accept-set lives in the constraint,
/// not the FSM. The spine's canonical constant (`SPINE_FORMAT`) lives in
/// `speech-pipeline`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FormatConstraint {
    /// Required `Hello.sample_rate_hz`.
    pub sample_rate_hz: u32,
    /// Required `Hello.bits_per_sample`.
    pub bits_per_sample: u8,
    /// Required `Hello.channels`.
    pub channels: u8,
    /// Required `Hello.codec`.
    pub codec: Codec,
    /// When `true`, `ChannelSource::Stereo` is rejected (any mono beam variant
    /// is accepted). `channels = 1` with `Stereo` is wire-legal, so the
    /// channel-count check does not subsume this.
    pub mono_beam_only: bool,
}

// ── Session events ─────────────────────────────────────────────────────────

/// A gap (or overlap) between an audio frame's index and the expected next
/// index. A negative delta (`got_index < expected_index`) is an overlap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gap {
    /// The sample index the FSM expected this frame to start at.
    pub expected_index: u64,
    /// The `first_sample_index` this frame actually carried.
    pub got_index: u64,
}

/// Outcome of the `samples_sent` vs `samples_received` cross-check at segment
/// close.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CrossCheck {
    /// Device-reported and receiver-counted sample totals agree.
    Match,
    /// The totals disagree.
    Mismatch {
        /// `SegmentEnd.samples_sent` reported by the device.
        sent: u64,
        /// Samples the FSM actually accepted this segment.
        received: u64,
    },
    /// Skipped: a resumed segment's device total covers the whole original
    /// segment, but the receiver only saw the resumed tail — a mismatch here
    /// is expected, not a bug.
    SkippedResume,
}

/// Why a segment or connection closed. Serializes to a snake-case label for
/// JSONL (`conn_closed.cause`), matching the spine's other serialized enums.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CloseCause {
    /// Clean EOF before/without a further frame.
    Eof,
    /// Socket read error (includes mid-frame TCP tear).
    ReadError,
    /// A frame failed to decode.
    DecodeError,
    /// A newer connection for the same pod superseded this one.
    Superseded,
    /// Process shutdown (SIGINT/SIGTERM).
    Shutdown,
    /// A duplicate `SegmentStart` force-truncated the open segment.
    ForcedByNewStart,
}

/// How a segment ended.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SegmentClose {
    /// A `SegmentEnd` closed the segment normally.
    Completed {
        /// The device's stated end reason.
        end_reason: EndReason,
        /// `SegmentEnd.frames_sent`.
        frames_sent: u32,
        /// `SegmentEnd.samples_sent`.
        samples_sent: u64,
        /// The sample-count cross-check result.
        cross_check: CrossCheck,
    },
    /// The segment was truncated (recorded into the ledger for resume).
    Truncated {
        /// What caused the truncation.
        cause: CloseCause,
    },
}

/// A protocol violation. `fatal` violations park the FSM in `Dead`; non-fatal
/// ones are counted and the offending frame is skipped. Serializes to a
/// snake-case label for JSONL (`protocol_error.kind`); a data-bearing variant
/// serializes as an object (`{"version_mismatch": {"got": …}}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolErrorKind {
    /// First frame on the connection was not `Hello` (fatal).
    NotHelloFirst,
    /// `Hello.version` did not match `AUDIO_PROTOCOL_VERSION` (fatal).
    VersionMismatch {
        /// The version the peer declared.
        got: u8,
    },
    /// `Hello` fields failed the [`FormatConstraint`] gate (fatal).
    FormatMismatch,
    /// A second `Hello` arrived after the handshake (non-fatal).
    HelloAfterHandshake,
    /// A `SegmentStart` arrived while a segment was already open (non-fatal;
    /// the open segment is force-truncated first).
    DuplicateSegmentStart,
    /// An `Audio` frame arrived outside any segment (non-fatal; skipped).
    AudioOutsideSegment,
    /// A `SegmentEnd` arrived without a matching open segment (non-fatal).
    SegmentEndWithoutStart,
    /// A server→device control frame (`EndOfAudio`/`FlushPlayback`) arrived on
    /// the device→host uplink (non-fatal).
    ControlFrameOnUplink,
    /// An `Audio` frame carried an odd-length PCM payload (non-fatal; skipped).
    OddPcmLength,
}

impl ProtocolErrorKind {
    /// Whether this error parks the FSM. Fatal errors are those that occur at
    /// or before the handshake gate; everything after is recoverable.
    fn is_fatal(self) -> bool {
        matches!(
            self,
            ProtocolErrorKind::NotHelloFirst
                | ProtocolErrorKind::VersionMismatch { .. }
                | ProtocolErrorKind::FormatMismatch
        )
    }
}

/// One thing that happened as a result of feeding a frame (or closing). The FSM
/// emits these in order; there are at most a few per frame.
#[derive(Debug, PartialEq)]
pub enum SessionEvent {
    /// The handshake completed and the pod's format was accepted.
    HelloAccepted {
        /// The pod's declared id.
        pod_id: String,
        /// Host receive time of the `Hello` frame.
        host_rx: HostMicros,
    },
    /// A segment opened.
    SegmentOpened {
        /// The device's segment counter.
        segment_id: u32,
        /// Absolute sample index of the segment's first sample.
        base_sample_index: u64,
        /// Leading pre-VAD-onset samples included in the segment.
        preroll_samples: u32,
        /// Device-clock anchor for telemetry sample-offset math.
        base_device_ts: DeviceMicros,
        /// `true` when this reopens a previously-truncated segment.
        is_resume: bool,
        /// Host receive time of the `SegmentStart` frame.
        host_rx: HostMicros,
    },
    /// A decoded chunk of PCM audio within the open segment.
    Audio {
        /// The enclosing segment's id.
        segment_id: u32,
        /// Absolute sample index of `pcm[0]`.
        first_sample_index: u64,
        /// Decoded mono S16 samples.
        pcm: Vec<i16>,
        /// Device-clock capture time of this frame's first sample.
        device_ts: DeviceMicros,
        /// Set when this frame's index broke continuity with the previous one.
        gap: Option<Gap>,
        /// Host receive time of this `Audio` frame.
        host_rx: HostMicros,
    },
    /// A telemetry reading within the open segment.
    Telemetry {
        /// The enclosing segment's id.
        segment_id: u32,
        /// Sample offset from the segment base, from the device timestamp.
        sample_offset: i64,
        /// The telemetry payload.
        kind: TelemetryKind,
        /// Device-clock time the reading was taken.
        device_ts: DeviceMicros,
        /// Host receive time of this telemetry frame.
        host_rx: HostMicros,
    },
    /// A segment closed (completed or truncated).
    SegmentClosed {
        /// The closed segment's id.
        segment_id: u32,
        /// How it closed.
        close: SegmentClose,
        /// Host time of the close.
        host_rx: HostMicros,
    },
    /// A protocol violation.
    ProtocolError {
        /// What was violated.
        kind: ProtocolErrorKind,
        /// Whether the FSM parked (caller must drop the connection).
        fatal: bool,
        /// Host time of the offending frame (or close).
        host_rx: HostMicros,
    },
}

// ── The FSM ─────────────────────────────────────────────────────────────────

/// Per-segment progress state.
#[derive(Debug)]
struct SegState {
    segment_id: u32,
    /// Device-clock anchor (`SegmentStart.base_device_ts_us`) for telemetry
    /// sample-offset math.
    base_device_ts_us: u64,
    /// Set when the segment reopened a truncated one — cross-check is skipped.
    is_resume: bool,
    /// Samples accepted this segment (per channel), for the cross-check.
    samples_received: u64,
    /// Last accepted frame's `first_sample_index`, for gap detection. Seeded
    /// with the segment base so the first frame is checked against it.
    last_index: u64,
    /// Sample count of the last accepted frame (0 before the first frame).
    last_samples: u64,
}

/// Which phase the session is in.
#[derive(Debug)]
enum State {
    /// Awaiting the mandatory first `Hello`.
    AwaitHello,
    /// Handshake done, no segment open.
    Idle,
    /// A segment is open.
    InSegment(SegState),
    /// A fatal error parked the FSM; further frames are contract violations.
    Dead,
}

/// Observability counters that are not carried on individual events.
#[derive(Debug, Default, Clone, Copy)]
pub struct FsmStats {
    /// Telemetry frames discarded because no segment was open — preserved
    /// behavior, but counted so the loss is visible in `stage_health`.
    pub telemetry_outside_segment: u64,
    /// Post-handshake non-fatal protocol violations. Counted so a peer spamming
    /// violations from one accepted connection is visible in `stage_health`
    /// rather than silently churning the observability stream.
    pub nonfatal_violations: u64,
}

/// Sans-I/O re-expression of the `audio-receiver` connection state machine.
/// The caller decodes frames and supplies host-receive timestamps; the FSM
/// owns the handshake, segment lifecycle, truncation/resume, cross-check, and
/// gap-detection rules and returns typed events.
#[derive(Debug)]
pub struct SessionFsm {
    constraint: FormatConstraint,
    state: State,
    pod_id: Option<String>,
    stats: FsmStats,
    /// Cross-connection resume state, shared across every pod connection. Locked
    /// only inside `open_segment` and `truncate_open_segment` — the sole two
    /// lock sites in the FSM — and never across a call back out of them, nor
    /// while any other lock is held. A single leaf-scoped lock makes deadlock
    /// impossible by construction, and the audio decode path never touches it.
    ledger: Arc<Mutex<ResumeLedger>>,
}

impl SessionFsm {
    /// Create an FSM awaiting the first `Hello`, accepting only formats the
    /// `constraint` allows. The `ledger` handle is shared across all connections.
    pub fn new(constraint: FormatConstraint, ledger: Arc<Mutex<ResumeLedger>>) -> Self {
        Self {
            constraint,
            state: State::AwaitHello,
            pod_id: None,
            stats: FsmStats::default(),
            ledger,
        }
    }

    /// The pod's declared id, once the handshake has completed.
    pub fn pod_id(&self) -> Option<&str> {
        self.pod_id.as_deref()
    }

    /// Whether a segment is currently open (the recorder's roll gate).
    pub fn segment_open(&self) -> bool {
        matches!(self.state, State::InSegment(_))
    }

    /// Observability counters not carried on events.
    pub fn stats(&self) -> FsmStats {
        self.stats
    }

    /// Feed one decoded frame. Events come back in order (at most a few).
    pub fn feed(&mut self, frame: StreamFrame, host_rx: HostMicros) -> Vec<SessionEvent> {
        let mut out = Vec::new();
        match &mut self.state {
            State::Dead => {
                debug_assert!(
                    false,
                    "SessionFsm::feed called after a fatal error; caller must drop the connection"
                );
            }
            State::AwaitHello => self.feed_await_hello(frame, host_rx, &mut out),
            State::Idle => self.feed_idle(frame, host_rx, &mut out),
            State::InSegment(_) => self.feed_in_segment(frame, host_rx, &mut out),
        }
        out
    }

    /// The connection ended (EOF, read/decode error, supersede, shutdown). If a
    /// segment was open it is truncated and noted in the ledger (via
    /// `truncate_open_segment`, which locks the ledger internally). The FSM parks
    /// afterward.
    pub fn close(&mut self, cause: CloseCause, at: HostMicros) -> Vec<SessionEvent> {
        let mut out = Vec::new();
        if self.segment_open() {
            self.truncate_open_segment(cause, at, &mut out);
        }
        self.state = State::Dead;
        out
    }

    // ── Per-state handlers ─────────────────────────────────────────────────

    fn feed_await_hello(
        &mut self,
        frame: StreamFrame,
        host_rx: HostMicros,
        out: &mut Vec<SessionEvent>,
    ) {
        let StreamFrame::Hello(hello) = frame else {
            self.fatal(ProtocolErrorKind::NotHelloFirst, host_rx, out);
            return;
        };
        if hello.version != AUDIO_PROTOCOL_VERSION {
            self.fatal(
                ProtocolErrorKind::VersionMismatch { got: hello.version },
                host_rx,
                out,
            );
            return;
        }
        if !self.format_ok(&hello) {
            self.fatal(ProtocolErrorKind::FormatMismatch, host_rx, out);
            return;
        }
        // TODO(pod-auth-threat-model): `pod_id` is an unauthenticated wire value
        // that keys the shared cross-connection `ResumeLedger` and drives
        // record-store attribution. Any LAN peer can impersonate a pod. The
        // trust model (LAN-trusted, device auth deferred) and any defense
        // (peer-IP binding, pre-shared key) need an explicit decision.
        let pod_id = hello.pod_id.as_str().to_owned();
        self.pod_id = Some(pod_id.clone());
        self.state = State::Idle;
        out.push(SessionEvent::HelloAccepted { pod_id, host_rx });
    }

    fn feed_idle(&mut self, frame: StreamFrame, host_rx: HostMicros, out: &mut Vec<SessionEvent>) {
        match frame {
            StreamFrame::Hello(_) => {
                self.nonfatal(ProtocolErrorKind::HelloAfterHandshake, host_rx, out);
            }
            StreamFrame::SegmentStart(ss) => {
                self.open_segment(ss, host_rx, out);
            }
            StreamFrame::Audio(_) => {
                self.nonfatal(ProtocolErrorKind::AudioOutsideSegment, host_rx, out);
            }
            StreamFrame::Telemetry(_) => {
                // Silently discarded (preserved behavior), but counted so the
                // loss is observable.
                self.stats.telemetry_outside_segment += 1;
            }
            StreamFrame::SegmentEnd(_) => {
                self.nonfatal(ProtocolErrorKind::SegmentEndWithoutStart, host_rx, out);
            }
            StreamFrame::EndOfAudio(_) | StreamFrame::FlushPlayback(_) => {
                self.nonfatal(ProtocolErrorKind::ControlFrameOnUplink, host_rx, out);
            }
        }
    }

    fn feed_in_segment(
        &mut self,
        frame: StreamFrame,
        host_rx: HostMicros,
        out: &mut Vec<SessionEvent>,
    ) {
        match frame {
            StreamFrame::Hello(_) => {
                self.nonfatal(ProtocolErrorKind::HelloAfterHandshake, host_rx, out);
            }
            StreamFrame::SegmentStart(ss) => {
                // Duplicate SegmentStart while open: force-truncate the current
                // segment, count the error, then open the new one.
                self.nonfatal(ProtocolErrorKind::DuplicateSegmentStart, host_rx, out);
                self.truncate_open_segment(CloseCause::ForcedByNewStart, host_rx, out);
                self.state = State::Idle;
                self.open_segment(ss, host_rx, out);
            }
            StreamFrame::Audio(af) => {
                let bytes = af.pcm.as_slice();
                if bytes.len() % 2 != 0 {
                    self.nonfatal(ProtocolErrorKind::OddPcmLength, host_rx, out);
                    return;
                }
                let pcm = decode_pcm(bytes);
                // Per-channel sample count: `first_sample_index` and
                // `SegmentEnd.samples_sent` are per-channel by wire contract, so
                // the accounting must divide out the channel count to stay
                // correct on multi-channel streams.
                let frame_samples = pcm.len() as u64 / u64::from(self.constraint.channels.max(1));
                let got = af.first_sample_index;
                let device_ts = DeviceMicros(af.device_ts_us);
                let seg = self.seg_mut();
                // `first_sample_index` is wire-controlled; a value near u64::MAX
                // would overflow the continuity add (panic in debug, wrap in
                // release). Compute the expected next index with a checked add and
                // treat an overflow as a discontinuity — a valid `got` can never
                // equal an overflowing sum.
                let expected_index = seg.last_index.checked_add(seg.last_samples);
                let gap = if expected_index == Some(got) {
                    None
                } else {
                    Some(Gap {
                        expected_index: expected_index.unwrap_or(u64::MAX),
                        got_index: got,
                    })
                };
                seg.last_index = got;
                seg.last_samples = frame_samples;
                seg.samples_received += frame_samples;
                let segment_id = seg.segment_id;
                out.push(SessionEvent::Audio {
                    segment_id,
                    first_sample_index: got,
                    pcm,
                    device_ts,
                    gap,
                    host_rx,
                });
            }
            StreamFrame::Telemetry(t) => {
                let sample_rate_hz = self.constraint.sample_rate_hz;
                let seg = self.seg_mut();
                let sample_offset =
                    ts_to_sample_offset(t.device_ts_us, seg.base_device_ts_us, sample_rate_hz);
                out.push(SessionEvent::Telemetry {
                    segment_id: seg.segment_id,
                    sample_offset,
                    kind: t.kind,
                    device_ts: DeviceMicros(t.device_ts_us),
                    host_rx,
                });
            }
            StreamFrame::SegmentEnd(se) => {
                let seg = self.seg_mut();
                let cross_check = if seg.is_resume {
                    CrossCheck::SkippedResume
                } else if se.samples_sent == seg.samples_received {
                    CrossCheck::Match
                } else {
                    CrossCheck::Mismatch {
                        sent: se.samples_sent,
                        received: seg.samples_received,
                    }
                };
                let segment_id = seg.segment_id;
                self.state = State::Idle;
                out.push(SessionEvent::SegmentClosed {
                    segment_id,
                    close: SegmentClose::Completed {
                        end_reason: se.reason,
                        frames_sent: se.frames_sent,
                        samples_sent: se.samples_sent,
                        cross_check,
                    },
                    host_rx,
                });
            }
            StreamFrame::EndOfAudio(_) | StreamFrame::FlushPlayback(_) => {
                self.nonfatal(ProtocolErrorKind::ControlFrameOnUplink, host_rx, out);
            }
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    /// Borrow the open segment's state. `feed` only routes into `feed_in_segment`
    /// from `State::InSegment`, so the other variants are unreachable here.
    fn seg_mut(&mut self) -> &mut SegState {
        let State::InSegment(seg) = &mut self.state else {
            unreachable!("seg_mut called without an open segment");
        };
        seg
    }

    /// Truncate the currently-open segment: note it in the ledger (so a later
    /// connection can resume it) and emit its `SegmentClosed{Truncated}`. The
    /// caller sets the successor state. The ledger note is the load-bearing
    /// invariant of the resume path, so both truncation sites share this.
    fn truncate_open_segment(
        &mut self,
        cause: CloseCause,
        at: HostMicros,
        out: &mut Vec<SessionEvent>,
    ) {
        let segment_id = self.seg_mut().segment_id;
        // `Idle`/`InSegment` are reachable only through the handshake, which sets
        // `pod_id`, so `None` here would mean a broken state invariant, not a
        // benign case — skipping the ledger note would silently misclassify the
        // eventual resume as a fresh segment.
        debug_assert!(self.pod_id.is_some(), "open segment without a pod id");
        if let Some(pod) = &self.pod_id {
            let mut ledger = self.ledger.lock().expect("resume ledger mutex poisoned");
            ledger.note_truncated(pod, segment_id);
        }
        out.push(SessionEvent::SegmentClosed {
            segment_id,
            close: SegmentClose::Truncated { cause },
            host_rx: at,
        });
    }

    /// Open a segment from a `SegmentStart`, resolving resume against the
    /// ledger. Caller must be in a segment-free state.
    fn open_segment(
        &mut self,
        ss: audio_pipeline::wire::SegmentStart,
        host_rx: HostMicros,
        out: &mut Vec<SessionEvent>,
    ) {
        debug_assert!(self.pod_id.is_some(), "open_segment without a pod id");
        let is_resume = match &self.pod_id {
            Some(pod) => {
                let mut ledger = self.ledger.lock().expect("resume ledger mutex poisoned");
                ledger.take_resume(pod, ss.segment_id)
            }
            None => false,
        };
        self.state = State::InSegment(SegState {
            segment_id: ss.segment_id,
            base_device_ts_us: ss.base_device_ts_us,
            is_resume,
            samples_received: 0,
            last_index: ss.base_sample_index,
            last_samples: 0,
        });
        out.push(SessionEvent::SegmentOpened {
            segment_id: ss.segment_id,
            base_sample_index: ss.base_sample_index,
            preroll_samples: ss.preroll_samples,
            base_device_ts: DeviceMicros(ss.base_device_ts_us),
            is_resume,
            host_rx,
        });
    }

    /// Validate a `Hello`'s format fields against the constraint.
    fn format_ok(&self, hello: &audio_pipeline::wire::Hello) -> bool {
        let c = &self.constraint;
        hello.sample_rate_hz == c.sample_rate_hz
            && hello.bits_per_sample == c.bits_per_sample
            && hello.channels == c.channels
            && hello.codec == c.codec
            && !(c.mono_beam_only && hello.channel_source == ChannelSource::Stereo)
    }

    /// Emit a fatal protocol error and park the FSM.
    fn fatal(&mut self, kind: ProtocolErrorKind, host_rx: HostMicros, out: &mut Vec<SessionEvent>) {
        debug_assert!(kind.is_fatal());
        self.state = State::Dead;
        out.push(SessionEvent::ProtocolError {
            kind,
            fatal: true,
            host_rx,
        });
    }

    /// Emit a non-fatal protocol error; state is unchanged.
    fn nonfatal(
        &mut self,
        kind: ProtocolErrorKind,
        host_rx: HostMicros,
        out: &mut Vec<SessionEvent>,
    ) {
        debug_assert!(!kind.is_fatal());
        self.stats.nonfatal_violations += 1;
        out.push(SessionEvent::ProtocolError {
            kind,
            fatal: false,
            host_rx,
        });
    }
}

/// Decode an even-length little-endian S16 PCM byte run into samples. Callers
/// reject odd-length payloads before reaching here.
fn decode_pcm(bytes: &[u8]) -> Vec<i16> {
    bytes
        .chunks_exact(2)
        .map(|c| i16::from_le_bytes([c[0], c[1]]))
        .collect()
}

/// Compute a telemetry timestamp's sample offset from the segment's device-clock
/// anchor. May be negative if the reading predates the segment base.
fn ts_to_sample_offset(device_ts_us: u64, base_device_ts_us: u64, sample_rate_hz: u32) -> i64 {
    let delta_us = device_ts_us as i64 - base_device_ts_us as i64;
    (delta_us as i128 * sample_rate_hz as i128 / 1_000_000) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_removes_on_hit() {
        let mut ledger = ResumeLedger::new();
        ledger.note_truncated("pod-a", 7);

        assert!(ledger.take_resume("pod-a", 7));
        // A resume cannot recur: the entry is gone after the first take.
        assert!(!ledger.take_resume("pod-a", 7));
    }

    #[test]
    fn take_miss_returns_false() {
        let mut ledger = ResumeLedger::new();
        assert!(!ledger.take_resume("pod-a", 1));

        ledger.note_truncated("pod-a", 1);
        // Different id, and different pod, both miss.
        assert!(!ledger.take_resume("pod-a", 2));
        assert!(!ledger.take_resume("pod-b", 1));
    }

    #[test]
    fn duplicate_note_is_noop() {
        let mut ledger = ResumeLedger::new();
        ledger.note_truncated("pod-a", 5);
        ledger.note_truncated("pod-a", 5);

        assert!(ledger.take_resume("pod-a", 5));
        assert!(!ledger.take_resume("pod-a", 5));
        assert_eq!(ledger.evictions(), 0);
    }

    #[test]
    fn pods_are_independent() {
        let mut ledger = ResumeLedger::new();
        ledger.note_truncated("pod-a", 3);
        ledger.note_truncated("pod-b", 3);

        assert!(ledger.take_resume("pod-a", 3));
        // pod-b's identical id is untouched.
        assert!(ledger.take_resume("pod-b", 3));
    }

    #[test]
    fn pod_cap_evicts_least_recently_noted() {
        let mut ledger = ResumeLedger::new();
        // Fill exactly to the pod cap: pods "0".."MAX_PODS", one id each.
        for p in 0..MAX_PODS {
            ledger.note_truncated(&p.to_string(), 1);
        }
        assert_eq!(ledger.pod_evictions(), 0);

        // Re-note pod "0" so it is now the most recently noted, not the oldest.
        ledger.note_truncated("0", 2);

        // A new distinct pod overflows the cap, evicting the LRU pod ("1").
        ledger.note_truncated(&MAX_PODS.to_string(), 1);
        assert_eq!(ledger.pod_evictions(), 1);
        assert!(!ledger.take_resume("1", 1), "LRU pod's entry was evicted");
        // The refreshed pod and the newcomer survive.
        assert!(ledger.take_resume("0", 1));
        assert!(ledger.take_resume(&MAX_PODS.to_string(), 1));
    }

    #[test]
    fn cap_evicts_oldest() {
        let mut ledger = ResumeLedger::new();
        // Fill exactly to the cap: ids 0..PER_POD_CAP, all retained.
        for id in 0..PER_POD_CAP as u32 {
            ledger.note_truncated("pod-a", id);
        }
        assert_eq!(ledger.evictions(), 0);
        assert!(ledger.take_resume("pod-a", 0));
        // Put id 0 back so the set is full again, then overflow by one.
        ledger.note_truncated("pod-a", 0);

        // One more distinct id overflows the cap, evicting the oldest (id 1).
        ledger.note_truncated("pod-a", PER_POD_CAP as u32);
        assert_eq!(ledger.evictions(), 1);
        assert!(!ledger.take_resume("pod-a", 1));
        // The newest id survives.
        assert!(ledger.take_resume("pod-a", PER_POD_CAP as u32));
    }

    // ── SessionFsm ─────────────────────────────────────────────────────────

    use audio_pipeline::wire::{
        AudioFrame, Hello, SegmentEnd, SegmentStart, StreamFrame, Telemetry,
    };
    use heapless::Vec as HVec;

    const SPINE: FormatConstraint = FormatConstraint {
        sample_rate_hz: 16_000,
        bits_per_sample: 16,
        channels: 1,
        codec: Codec::S16Le,
        mono_beam_only: true,
    };

    fn hello(source: ChannelSource) -> StreamFrame {
        StreamFrame::Hello(Hello {
            version: AUDIO_PROTOCOL_VERSION,
            pod_id: heapless::String::try_from("pod-test").unwrap(),
            sample_rate_hz: 16_000,
            bits_per_sample: 16,
            channels: 1,
            codec: Codec::S16Le,
            channel_source: source,
        })
    }

    /// An audio frame of `n` samples starting at `first`. Sample values are
    /// `first_sample_index`-derived so they are checkable if needed.
    fn audio(segment_id: u32, first: u64, n: usize, device_ts_us: u64) -> StreamFrame {
        let mut pcm: HVec<u8, { audio_pipeline::wire::MAX_AUDIO_PAYLOAD }> = HVec::new();
        for i in 0..n * 2 {
            pcm.push((i & 0xFF) as u8).ok();
        }
        StreamFrame::Audio(AudioFrame {
            segment_id,
            first_sample_index: first,
            device_ts_us,
            pcm,
        })
    }

    fn seg_start(segment_id: u32, base: u64, base_ts: u64) -> StreamFrame {
        StreamFrame::SegmentStart(SegmentStart {
            segment_id,
            base_sample_index: base,
            base_device_ts_us: base_ts,
            preroll_samples: 0,
        })
    }

    fn seg_end(segment_id: u32, frames: u32, samples: u64, reason: EndReason) -> StreamFrame {
        StreamFrame::SegmentEnd(SegmentEnd {
            segment_id,
            device_ts_us: 0,
            frames_sent: frames,
            samples_sent: samples,
            reason,
        })
    }

    fn hx(us: u64) -> HostMicros {
        HostMicros(us)
    }

    /// A fresh shared ledger handle for a test.
    fn new_ledger() -> Arc<Mutex<ResumeLedger>> {
        ResumeLedger::shared()
    }

    /// Feed a Hello and assert it was accepted, returning the FSM in Idle. The
    /// FSM is wired to the supplied shared ledger.
    fn accepted_fsm(ledger: Arc<Mutex<ResumeLedger>>) -> SessionFsm {
        let mut fsm = SessionFsm::new(SPINE, ledger);
        let ev = fsm.feed(hello(ChannelSource::AsrBeam), hx(1));
        assert!(matches!(
            ev.as_slice(),
            [SessionEvent::HelloAccepted { .. }]
        ));
        assert_eq!(fsm.pod_id(), Some("pod-test"));
        fsm
    }

    #[test]
    fn first_frame_not_hello_is_fatal() {
        let mut fsm = SessionFsm::new(SPINE, new_ledger());
        let ev = fsm.feed(seg_start(1, 0, 0), hx(1));
        assert_eq!(
            ev,
            vec![SessionEvent::ProtocolError {
                kind: ProtocolErrorKind::NotHelloFirst,
                fatal: true,
                host_rx: hx(1),
            }]
        );
        assert_eq!(fsm.pod_id(), None);
    }

    #[test]
    fn version_mismatch_is_fatal() {
        let mut fsm = SessionFsm::new(SPINE, new_ledger());
        let bad = StreamFrame::Hello(Hello {
            version: 99,
            pod_id: heapless::String::try_from("pod-test").unwrap(),
            sample_rate_hz: 16_000,
            bits_per_sample: 16,
            channels: 1,
            codec: Codec::S16Le,
            channel_source: ChannelSource::AsrBeam,
        });
        let ev = fsm.feed(bad, hx(2));
        assert_eq!(
            ev,
            vec![SessionEvent::ProtocolError {
                kind: ProtocolErrorKind::VersionMismatch { got: 99 },
                fatal: true,
                host_rx: hx(2),
            }]
        );
    }

    #[test]
    fn format_gate_rejects_stereo_and_wrong_rate_and_channels() {
        // `codec` is checked in `format_ok` too, but `Codec` has a single
        // variant today, so no mismatched-codec `Hello` can be constructed;
        // add a codec branch here when a second `Codec` variant lands.

        // Stereo channel source with mono_beam_only → fatal.
        let mut fsm = SessionFsm::new(SPINE, new_ledger());
        let ev = fsm.feed(hello(ChannelSource::Stereo), hx(1));
        assert!(matches!(
            ev.as_slice(),
            [SessionEvent::ProtocolError {
                kind: ProtocolErrorKind::FormatMismatch,
                fatal: true,
                ..
            }]
        ));

        // Wrong sample rate → fatal.
        let mut fsm = SessionFsm::new(SPINE, new_ledger());
        let wrong_rate = StreamFrame::Hello(Hello {
            version: AUDIO_PROTOCOL_VERSION,
            pod_id: heapless::String::try_from("pod-test").unwrap(),
            sample_rate_hz: 48_000,
            bits_per_sample: 16,
            channels: 1,
            codec: Codec::S16Le,
            channel_source: ChannelSource::AsrBeam,
        });
        let ev = fsm.feed(wrong_rate, hx(1));
        assert!(matches!(
            ev.as_slice(),
            [SessionEvent::ProtocolError {
                kind: ProtocolErrorKind::FormatMismatch,
                ..
            }]
        ));

        // Wrong channel count → fatal.
        let mut fsm = SessionFsm::new(SPINE, new_ledger());
        let wrong_ch = StreamFrame::Hello(Hello {
            version: AUDIO_PROTOCOL_VERSION,
            pod_id: heapless::String::try_from("pod-test").unwrap(),
            sample_rate_hz: 16_000,
            bits_per_sample: 16,
            channels: 2,
            codec: Codec::S16Le,
            channel_source: ChannelSource::AsrBeam,
        });
        let ev = fsm.feed(wrong_ch, hx(1));
        assert!(matches!(
            ev.as_slice(),
            [SessionEvent::ProtocolError {
                kind: ProtocolErrorKind::FormatMismatch,
                ..
            }]
        ));
    }

    #[test]
    fn mono_communication_beam_accepted() {
        // A mono beam variant other than AsrBeam must pass the gate.
        let mut fsm = SessionFsm::new(SPINE, new_ledger());
        let ev = fsm.feed(hello(ChannelSource::CommunicationBeam), hx(1));
        assert!(matches!(
            ev.as_slice(),
            [SessionEvent::HelloAccepted { .. }]
        ));
    }

    #[test]
    fn full_segment_sample_and_frame_counts() {
        let mut fsm = accepted_fsm(new_ledger());
        assert!(!fsm.segment_open());

        let ev = fsm.feed(seg_start(1, 0, 0), hx(10));
        assert!(matches!(
            ev.as_slice(),
            [SessionEvent::SegmentOpened {
                segment_id: 1,
                is_resume: false,
                ..
            }]
        ));
        assert!(fsm.segment_open());

        // 5 frames × 320 samples = 1600, contiguous → no gaps.
        for i in 0u64..5 {
            let ev = fsm.feed(audio(1, i * 320, 320, i * 20_000), hx(20 + i));
            match ev.as_slice() {
                [SessionEvent::Audio {
                    first_sample_index,
                    pcm,
                    gap,
                    ..
                }] => {
                    assert_eq!(*first_sample_index, i * 320);
                    assert_eq!(pcm.len(), 320);
                    assert_eq!(*gap, None);
                }
                other => panic!("expected one Audio event, got {other:?}"),
            }
        }

        let ev = fsm.feed(seg_end(1, 5, 1600, EndReason::VadRelease), hx(99));
        assert_eq!(
            ev,
            vec![SessionEvent::SegmentClosed {
                segment_id: 1,
                close: SegmentClose::Completed {
                    end_reason: EndReason::VadRelease,
                    frames_sent: 5,
                    samples_sent: 1600,
                    cross_check: CrossCheck::Match,
                },
                host_rx: hx(99),
            }]
        );
        assert!(!fsm.segment_open());
    }

    #[test]
    fn cross_check_mismatch_surfaces() {
        let mut fsm = accepted_fsm(new_ledger());
        fsm.feed(seg_start(1, 0, 0), hx(10));
        fsm.feed(audio(1, 0, 320, 0), hx(20));
        // Device claims 999 samples; we counted 320.
        let ev = fsm.feed(seg_end(1, 1, 999, EndReason::VadRelease), hx(30));
        assert_eq!(
            ev,
            vec![SessionEvent::SegmentClosed {
                segment_id: 1,
                close: SegmentClose::Completed {
                    end_reason: EndReason::VadRelease,
                    frames_sent: 1,
                    samples_sent: 999,
                    cross_check: CrossCheck::Mismatch {
                        sent: 999,
                        received: 320,
                    },
                },
                host_rx: hx(30),
            }]
        );
    }

    #[test]
    fn gap_detection_continuous_gap_overlap_and_first_frame() {
        let mut fsm = accepted_fsm(new_ledger());
        // Segment base is 100: the first frame must start at 100 to be continuous.
        fsm.feed(seg_start(1, 100, 0), hx(10));

        // First frame at 100 → continuous (checked against base).
        let ev = fsm.feed(audio(1, 100, 320, 0), hx(11));
        assert!(matches!(
            ev.as_slice(),
            [SessionEvent::Audio { gap: None, .. }]
        ));

        // Next expected at 420; a frame at 500 → gap.
        let ev = fsm.feed(audio(1, 500, 320, 0), hx(12));
        assert!(matches!(
            ev.as_slice(),
            [SessionEvent::Audio {
                gap: Some(Gap {
                    expected_index: 420,
                    got_index: 500
                }),
                ..
            }]
        ));

        // After the 500-frame (320 samples) expected is 820; a frame at 700 → overlap.
        let ev = fsm.feed(audio(1, 700, 320, 0), hx(13));
        assert!(matches!(
            ev.as_slice(),
            [SessionEvent::Audio {
                gap: Some(Gap {
                    expected_index: 820,
                    got_index: 700
                }),
                ..
            }]
        ));
    }

    #[test]
    fn first_frame_gap_against_base() {
        let mut fsm = accepted_fsm(new_ledger());
        fsm.feed(seg_start(1, 100, 0), hx(10));
        // First frame does not start at the base → gap against the base index.
        let ev = fsm.feed(audio(1, 132, 320, 0), hx(11));
        assert!(matches!(
            ev.as_slice(),
            [SessionEvent::Audio {
                gap: Some(Gap {
                    expected_index: 100,
                    got_index: 132
                }),
                ..
            }]
        ));
    }

    #[test]
    fn odd_pcm_payload_skipped() {
        let mut fsm = accepted_fsm(new_ledger());
        fsm.feed(seg_start(1, 0, 0), hx(10));
        let mut pcm: HVec<u8, { audio_pipeline::wire::MAX_AUDIO_PAYLOAD }> = HVec::new();
        for i in 0..5u8 {
            pcm.push(i).ok(); // odd byte count
        }
        let odd = StreamFrame::Audio(AudioFrame {
            segment_id: 1,
            first_sample_index: 0,
            device_ts_us: 0,
            pcm,
        });
        let ev = fsm.feed(odd, hx(11));
        assert_eq!(
            ev,
            vec![SessionEvent::ProtocolError {
                kind: ProtocolErrorKind::OddPcmLength,
                fatal: false,
                host_rx: hx(11),
            }]
        );
        // The skipped frame did not advance sample accounting: a clean 320-sample
        // frame at base is still continuous.
        let ev = fsm.feed(audio(1, 0, 320, 0), hx(12));
        assert!(matches!(
            ev.as_slice(),
            [SessionEvent::Audio { gap: None, .. }]
        ));
    }

    #[test]
    fn telemetry_offset_in_segment_and_outside_counted() {
        let mut fsm = accepted_fsm(new_ledger());

        // Telemetry outside a segment: discarded, counted, no event.
        let t_out = StreamFrame::Telemetry(Telemetry {
            device_ts_us: 0,
            kind: TelemetryKind::SpEnergy {
                values: [1.0, 2.0, 3.0, 4.0],
            },
        });
        let ev = fsm.feed(t_out, hx(5));
        assert!(ev.is_empty());
        assert_eq!(fsm.stats().telemetry_outside_segment, 1);

        // Anchor at 1_000_000 device µs; telemetry at +20 ms → offset 320.
        fsm.feed(seg_start(1, 0, 1_000_000), hx(10));
        let t = StreamFrame::Telemetry(Telemetry {
            device_ts_us: 1_020_000,
            kind: TelemetryKind::Azimuths {
                values: [0.1, 0.2, 0.3, 0.4],
            },
        });
        let ev = fsm.feed(t, hx(11));
        match ev.as_slice() {
            [SessionEvent::Telemetry {
                segment_id: 1,
                sample_offset,
                ..
            }] => assert_eq!(*sample_offset, 320),
            other => panic!("expected one Telemetry event, got {other:?}"),
        }
    }

    #[test]
    fn audio_outside_segment_is_nonfatal() {
        let mut fsm = accepted_fsm(new_ledger());
        let ev = fsm.feed(audio(1, 0, 320, 0), hx(5));
        assert_eq!(
            ev,
            vec![SessionEvent::ProtocolError {
                kind: ProtocolErrorKind::AudioOutsideSegment,
                fatal: false,
                host_rx: hx(5),
            }]
        );
        assert!(!fsm.segment_open());
    }

    #[test]
    fn segment_end_without_start_is_nonfatal() {
        let mut fsm = accepted_fsm(new_ledger());
        let ev = fsm.feed(seg_end(1, 0, 0, EndReason::VadRelease), hx(5));
        assert!(matches!(
            ev.as_slice(),
            [SessionEvent::ProtocolError {
                kind: ProtocolErrorKind::SegmentEndWithoutStart,
                fatal: false,
                ..
            }]
        ));
    }

    #[test]
    fn control_frame_on_uplink_is_nonfatal() {
        let mut fsm = accepted_fsm(new_ledger());
        for frame in [
            StreamFrame::EndOfAudio(audio_pipeline::wire::EndOfAudio {}),
            StreamFrame::FlushPlayback(audio_pipeline::wire::FlushPlayback {}),
        ] {
            let ev = fsm.feed(frame, hx(5));
            assert!(matches!(
                ev.as_slice(),
                [SessionEvent::ProtocolError {
                    kind: ProtocolErrorKind::ControlFrameOnUplink,
                    fatal: false,
                    ..
                }]
            ));
        }
    }

    #[test]
    fn hello_after_handshake_is_nonfatal() {
        let mut fsm = accepted_fsm(new_ledger());
        let ev = fsm.feed(hello(ChannelSource::AsrBeam), hx(5));
        assert!(matches!(
            ev.as_slice(),
            [SessionEvent::ProtocolError {
                kind: ProtocolErrorKind::HelloAfterHandshake,
                fatal: false,
                ..
            }]
        ));
        // Still usable: the handshake is not undone.
        assert_eq!(fsm.pod_id(), Some("pod-test"));
    }

    #[test]
    fn duplicate_segment_start_force_truncates() {
        let ledger = new_ledger();
        let mut fsm = accepted_fsm(ledger.clone());
        fsm.feed(seg_start(1, 0, 0), hx(10));
        fsm.feed(audio(1, 0, 320, 0), hx(11));

        // A new SegmentStart while segment 1 is open.
        let ev = fsm.feed(seg_start(2, 0, 0), hx(12));
        assert_eq!(
            ev,
            vec![
                SessionEvent::ProtocolError {
                    kind: ProtocolErrorKind::DuplicateSegmentStart,
                    fatal: false,
                    host_rx: hx(12),
                },
                SessionEvent::SegmentClosed {
                    segment_id: 1,
                    close: SegmentClose::Truncated {
                        cause: CloseCause::ForcedByNewStart,
                    },
                    host_rx: hx(12),
                },
                SessionEvent::SegmentOpened {
                    segment_id: 2,
                    base_sample_index: 0,
                    preroll_samples: 0,
                    base_device_ts: DeviceMicros(0),
                    is_resume: false,
                    host_rx: hx(12),
                },
            ]
        );
        // Segment 1 was noted truncated in the ledger.
        assert!(ledger.lock().unwrap().take_resume("pod-test", 1));
    }

    #[test]
    fn two_independent_segments() {
        let mut fsm = accepted_fsm(new_ledger());
        fsm.feed(seg_start(1, 0, 0), hx(10));
        fsm.feed(audio(1, 0, 320, 0), hx(11));
        let ev = fsm.feed(seg_end(1, 1, 320, EndReason::VadRelease), hx(12));
        assert!(matches!(
            ev.as_slice(),
            [SessionEvent::SegmentClosed {
                close: SegmentClose::Completed {
                    cross_check: CrossCheck::Match,
                    ..
                },
                ..
            }]
        ));
        // Second, independent segment starts clean.
        let ev = fsm.feed(seg_start(2, 400, 0), hx(13));
        assert!(matches!(
            ev.as_slice(),
            [SessionEvent::SegmentOpened {
                segment_id: 2,
                is_resume: false,
                ..
            }]
        ));
    }

    #[test]
    fn close_with_open_segment_truncates_and_notes_ledger() {
        let ledger = new_ledger();
        let mut fsm = accepted_fsm(ledger.clone());
        fsm.feed(seg_start(7, 0, 0), hx(10));
        let ev = fsm.close(CloseCause::ReadError, hx(50));
        assert_eq!(
            ev,
            vec![SessionEvent::SegmentClosed {
                segment_id: 7,
                close: SegmentClose::Truncated {
                    cause: CloseCause::ReadError,
                },
                host_rx: hx(50),
            }]
        );
        assert!(ledger.lock().unwrap().take_resume("pod-test", 7));
    }

    #[test]
    fn close_without_open_segment_is_empty() {
        let mut fsm = accepted_fsm(new_ledger());
        let ev = fsm.close(CloseCause::Eof, hx(50));
        assert!(ev.is_empty());
    }

    #[test]
    fn resume_after_truncation_skips_cross_check() {
        // Connection 1: open segment 3, take some audio, then truncate on close.
        let ledger = new_ledger();
        let mut fsm1 = accepted_fsm(ledger.clone());
        fsm1.feed(seg_start(3, 0, 0), hx(10));
        fsm1.feed(audio(3, 0, 320, 0), hx(11));
        fsm1.close(CloseCause::ReadError, hx(12));

        // Connection 2 (new FSM, same ledger): resume segment 3.
        let mut fsm2 = accepted_fsm(ledger.clone());
        let ev = fsm2.feed(seg_start(3, 320, 0), hx(20));
        assert!(matches!(
            ev.as_slice(),
            [SessionEvent::SegmentOpened {
                segment_id: 3,
                is_resume: true,
                ..
            }]
        ));
        fsm2.feed(audio(3, 320, 320, 0), hx(21));
        // Device's samples_sent covers the whole segment; cross-check is skipped.
        let ev = fsm2.feed(seg_end(3, 5, 1600, EndReason::VadRelease), hx(22));
        assert!(matches!(
            ev.as_slice(),
            [SessionEvent::SegmentClosed {
                close: SegmentClose::Completed {
                    cross_check: CrossCheck::SkippedResume,
                    ..
                },
                ..
            }]
        ));
    }

    #[test]
    fn ts_offset_zero_one_second_and_negative() {
        // At the base anchor → offset 0.
        assert_eq!(ts_to_sample_offset(1_000_000, 1_000_000, 16_000), 0);
        // One second later at 16 kHz → 16_000 samples.
        assert_eq!(ts_to_sample_offset(2_000_000, 1_000_000, 16_000), 16_000);
        // A reading predating the base (e.g. preroll) → negative offset.
        assert_eq!(ts_to_sample_offset(980_000, 1_000_000, 16_000), -320);
    }

    #[test]
    fn format_gate_rejects_wrong_bits_per_sample() {
        let mut fsm = SessionFsm::new(SPINE, new_ledger());
        let wrong_bits = StreamFrame::Hello(Hello {
            version: AUDIO_PROTOCOL_VERSION,
            pod_id: heapless::String::try_from("pod-test").unwrap(),
            sample_rate_hz: 16_000,
            bits_per_sample: 8,
            channels: 1,
            codec: Codec::S16Le,
            channel_source: ChannelSource::AsrBeam,
        });
        let ev = fsm.feed(wrong_bits, hx(1));
        assert!(matches!(
            ev.as_slice(),
            [SessionEvent::ProtocolError {
                kind: ProtocolErrorKind::FormatMismatch,
                fatal: true,
                ..
            }]
        ));
    }

    #[test]
    fn hostile_near_max_index_reports_gap_without_panic() {
        let mut fsm = accepted_fsm(new_ledger());
        // Segment base near u64::MAX: the first frame lands there (continuous),
        // the next frame's continuity add would overflow u64 — it must surface as
        // a gap, never panic.
        let base = u64::MAX - 100;
        fsm.feed(seg_start(1, base, 0), hx(10));
        // First frame lands exactly at the base → continuous.
        let ev = fsm.feed(audio(1, base, 320, 0), hx(11));
        assert!(matches!(
            ev.as_slice(),
            [SessionEvent::Audio { gap: None, .. }]
        ));
        // Now last_index is near u64::MAX; the next frame's expected index
        // (last_index + 320) overflows → gap with a saturated expectation, no panic.
        let ev = fsm.feed(audio(1, 0, 320, 0), hx(12));
        assert!(matches!(
            ev.as_slice(),
            [SessionEvent::Audio {
                gap: Some(Gap {
                    expected_index: u64::MAX,
                    got_index: 0
                }),
                ..
            }]
        ));
    }

    #[test]
    fn nonfatal_violations_counted() {
        let mut fsm = accepted_fsm(new_ledger());
        assert_eq!(fsm.stats().nonfatal_violations, 0);
        // Two post-handshake violations: audio outside a segment, then a second Hello.
        fsm.feed(audio(1, 0, 320, 0), hx(5));
        fsm.feed(hello(ChannelSource::AsrBeam), hx(6));
        assert_eq!(fsm.stats().nonfatal_violations, 2);
    }

    #[test]
    #[should_panic(expected = "after a fatal error")]
    fn feed_after_fatal_debug_asserts() {
        let mut fsm = SessionFsm::new(SPINE, new_ledger());
        // First frame not Hello → fatal, FSM parks in Dead.
        fsm.feed(seg_start(1, 0, 0), hx(1));
        assert!(!fsm.segment_open());
        // A further feed on a Dead FSM is a caller-contract violation.
        fsm.feed(hello(ChannelSource::AsrBeam), hx(2));
    }

    #[test]
    fn supersede_note_precedes_resume_lookup() {
        // Model the cross-connection ordering: connection 1 truncates segment 5
        // and its note lands in the shared ledger; connection 2, opened after,
        // observes that note as a resume. The embedder's await enforces the
        // temporal ordering; here the notes simply share one ledger handle.
        let ledger = new_ledger();

        let mut fsm1 = accepted_fsm(ledger.clone());
        fsm1.feed(seg_start(5, 0, 0), hx(10));
        fsm1.feed(audio(5, 0, 320, 0), hx(11));
        // Connection 1 closes with the segment still open → truncation note.
        fsm1.close(CloseCause::Superseded, hx(12));

        // Connection 2 (new FSM, same ledger) resumes segment 5.
        let mut fsm2 = accepted_fsm(ledger.clone());
        let ev = fsm2.feed(seg_start(5, 320, 0), hx(20));
        assert!(
            matches!(
                ev.as_slice(),
                [SessionEvent::SegmentOpened {
                    segment_id: 5,
                    is_resume: true,
                    ..
                }]
            ),
            "connection 2's resume lookup must observe connection 1's truncation note"
        );
    }

    #[test]
    fn audio_frame_takes_no_ledger_lock() {
        use std::sync::mpsc;
        use std::time::Duration;

        // An InSegment FSM: feeding an Audio frame must not touch the ledger.
        let ledger = new_ledger();
        let mut fsm = accepted_fsm(ledger.clone());
        fsm.feed(seg_start(1, 0, 0), hx(10));

        // Hold the ledger lock on THIS thread, then feed an Audio frame from a
        // SECOND thread. If the audio path locked the ledger it would block until
        // this thread released the guard; the bounded-timeout join proves it did
        // not. (Re-locking a std Mutex on the same thread would deadlock or
        // panic, so the feed must run off the lock-holding thread.)
        let guard = ledger.lock().expect("resume ledger mutex poisoned");

        let (tx, rx) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            let ev = fsm.feed(audio(1, 0, 320, 0), hx(11));
            assert!(
                matches!(ev.as_slice(), [SessionEvent::Audio { .. }]),
                "audio feed must produce an Audio event"
            );
            tx.send(()).expect("send completion");
        });

        rx.recv_timeout(Duration::from_secs(5))
            .expect("audio feed blocked on the ledger lock — the audio path must be lock-free");
        drop(guard);
        handle.join().expect("audio feed thread panicked");
    }
}
