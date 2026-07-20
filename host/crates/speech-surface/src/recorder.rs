//! The record store's per-connection **sidecar**: a small JSON file beside each
//! frame log carrying per-segment labels (wake class, end cause, counts) plus a
//! log-level `pinned` flag. The pruner (a later increment) reads these to decide
//! retention tiers; the `pin` subcommand and the daemon write them.
//!
//! Three rules are load-bearing here:
//!
//! - **Atomic writes.** The daemon rewrites the whole sidecar at each segment
//!   close and at connection end (segments accumulate in memory). Writing to a
//!   temp file and renaming into place means a crash never leaves a half-written
//!   sidecar — a reader sees either the old file or the new one.
//! - **Cross-process serialization.** The daemon and the out-of-process `pin`
//!   subcommand both write sidecars, and the pruner deletes them. Every
//!   read-modify-write-rename and every prune re-check runs while holding an
//!   exclusive advisory lock over the store *directory* ([`lock_store`]), so no
//!   two writers ever interleave: a pin can never be lost to a
//!   read→write→rename race, and no reader observes torn bytes. The lock object
//!   is the directory itself — stable across the sidecar's rename-based
//!   replacement, and it leaves no stray lock file behind. Because a single
//!   writer is ever in the critical section, the shared `.tmp` staging path is
//!   collision-free and self-cleaning (a crash-leftover temp is overwritten,
//!   never renamed into place, by the next write).
//! - **`pinned` is owned by the on-disk file, not daemon memory.** A pin applied
//!   to a currently-open log must survive the daemon's next rewrite. Under the
//!   store lock the daemon re-reads the existing sidecar's `pinned` flag and
//!   carries it forward (`pinned = on_disk || in_memory`); a pin is never un-set
//!   by the daemon.
//!
//! `pod_id` normally records the pod that produced the log, set from the hello
//! handshake when the connection opens. A sidecar minted without a live pod
//! connection — pinning a log that never got a sidecar — cannot know the origin
//! pod, so it carries the sentinel [`UNKNOWN_POD`]. Consumers must treat that
//! value as "origin unknown", never as a pod name. The sentinel is ALL-CAPS to
//! stay visually distinct from real pod ids; a real pod naming itself
//! `UNKNOWN_POD` is an accepted theoretical collision (the wire `pod_id` is ours
//! to govern), not guarded at runtime.

use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use pod_ingest::{FrameLogWriter, HostMicros, LogMeta};
use serde::{Deserialize, Serialize};
use serde_json::json;
use speech_pipeline::{Segment, SegmentEndCause};
use thiserror::Error;

use crate::jsonl::JsonlHandle;

/// Build identity stamped into each frame log's header metadata.
pub(crate) const BUILD_ID: &str = concat!("speech-surface/", env!("CARGO_PKG_VERSION"));

/// The set of frame logs currently open for writing (full store-relative-to-
/// absolute paths). The recorder side owns it: a connection inserts its log on
/// open, swaps it on rename/roll, and removes it on close or recorder failure.
/// The pruner reads it so a log being appended to is never a deletion candidate.
#[derive(Clone, Default)]
pub struct OpenLogs(Arc<Mutex<HashSet<PathBuf>>>);

impl OpenLogs {
    pub fn insert(&self, path: PathBuf) {
        self.0.lock().expect("open-logs set poisoned").insert(path);
    }

    pub fn remove(&self, path: &Path) {
        self.0.lock().expect("open-logs set poisoned").remove(path);
    }

    /// Live set handle for the pruner's re-check-before-delete.
    pub fn as_set(&self) -> &Mutex<HashSet<PathBuf>> {
        &self.0
    }
}

/// The shared handles a [`Recorder`] clones from its connection: the open-log
/// set, the process-wide recording latch, and the JSONL sink. They travel
/// together into every recorder, so they enter the constructor as one group.
pub struct RecorderShared {
    pub open_logs: OpenLogs,
    pub recording_failed: Arc<AtomicBool>,
    pub jsonl: JsonlHandle,
}

/// One connection's frame-log recorder: the writer and its sidecar, the current
/// log name and roll counter, plus clones of the shared handles the recorder
/// touches (the open-log set, the process-wide recording latch, the JSONL sink).
/// Recording never gates the data plane — any write failure latches recording
/// off process-wide via [`Recorder::disable`] and the pipeline continues.
pub struct Recorder {
    record_dir: PathBuf,
    log_name: String,
    writer: Option<FrameLogWriter>,
    sidecar: Option<Sidecar>,
    roll_seq: u64,
    open_logs: OpenLogs,
    recording_failed: Arc<AtomicBool>,
    jsonl: JsonlHandle,
}

impl Recorder {
    /// Open a connection's frame log under a connection-scoped name (capture from
    /// byte 0, no dependence on decoding `Hello`). When `recording_on`, creates
    /// the store dir and the writer, tracking the log as open before it lands on
    /// disk; a create failure latches recording off. Always returns a `Recorder`
    /// (with `writer: None` when off/failed) so the caller holds exactly one
    /// recorder value unconditionally.
    pub fn start(
        record_dir: PathBuf,
        conn_seq: u64,
        accept_iso: &str,
        recording_on: bool,
        shared: RecorderShared,
    ) -> Self {
        let RecorderShared {
            open_logs,
            recording_failed,
            jsonl,
        } = shared;
        let log_name = format!("{accept_iso}_{conn_seq}.framelog");
        let mut rec = Self {
            record_dir,
            log_name,
            writer: None,
            sidecar: None,
            roll_seq: 0,
            open_logs,
            recording_failed,
            jsonl,
        };
        if !recording_on {
            return rec;
        }
        match std::fs::create_dir_all(&rec.record_dir) {
            Ok(()) => {
                let path = rec.record_dir.join(&rec.log_name);
                let meta = LogMeta {
                    build_id: BUILD_ID.to_string(),
                    created_epoch_us: HostMicros::now(),
                    conn_seq,
                    rolled_from: None,
                };
                // Track the path as open *before* it exists on disk, so a
                // concurrent prune pass can never see the new file untracked
                // (classed `ungated`, age 0) and unlink a live capture. A
                // creation failure removes it again via `disable`.
                rec.open_logs.insert(path.clone());
                match FrameLogWriter::create(&path, meta) {
                    Ok(w) => rec.writer = Some(w),
                    Err(e) => rec.disable("create", &path, &path, &e.to_string()),
                }
            }
            Err(e) => {
                let dir = rec.record_dir.clone();
                let open_path = rec.record_dir.join(&rec.log_name);
                rec.disable("create_dir", &dir, &open_path, &e.to_string());
            }
        }
        rec
    }

    /// Honor the process-wide recording latch: once any connection's write
    /// fails, recording is off for the whole process, so a connection still
    /// holding a writer stops here rather than continuing to record.
    pub fn honor_latch(&mut self) {
        if self.writer.is_some() && self.recording_failed.load(Ordering::Relaxed) {
            if let Some(w) = self.writer.take() {
                let _ = w.finish();
                self.open_logs.remove(&self.record_dir.join(&self.log_name));
            }
            self.sidecar = None;
        }
    }

    /// Recorder tap, pre-decode: the bytes are captured before anything can
    /// reject them. A write error latches recording off.
    pub fn tap(&mut self, host_rx: HostMicros, framed: &[u8]) {
        let err = match self.writer.as_mut() {
            Some(w) => w.append(host_rx, framed).err(),
            None => None,
        };
        if let Some(e) = err {
            let path = self.record_dir.join(&self.log_name);
            self.disable("append", &path, &path, &e.to_string());
        }
    }

    /// At `Hello`, rename the connection-scoped log to a pod-named form and note
    /// the retained `Hello` frame, then start a fresh sidecar for the pod. The
    /// rename + note stay gated on holding a writer; the sidecar is created
    /// unconditionally. A rename failure keeps the connection-scoped name and
    /// emits `record_error` — capture continuity beats naming.
    pub fn on_hello(
        &mut self,
        pod_id: &str,
        accept_iso: &str,
        conn_seq: u64,
        host_rx: HostMicros,
        framed: &[u8],
    ) {
        if let Some(w) = &mut self.writer {
            let new_name = format!(
                "{}_{accept_iso}_{conn_seq}.framelog",
                sanitize_filename(pod_id)
            );
            let new_path = self.record_dir.join(&new_name);
            // Track the new name before it lands on disk (both names are
            // momentarily in the open set, both skipped by the pruner); swap to
            // just the new name on success, or drop it again on failure so a
            // never-created name is not left marked open.
            self.open_logs.insert(new_path.clone());
            match w.rename_to(&new_path) {
                Ok(()) => {
                    self.open_logs.remove(&self.record_dir.join(&self.log_name));
                    self.log_name = new_name;
                }
                Err(e) => {
                    self.open_logs.remove(&new_path);
                    // Keep writing under the connection-scoped name; capture
                    // continuity beats naming.
                    self.jsonl.emit(
                        "record_error",
                        &json!({
                            "cause": "rename",
                            "path": new_path.display().to_string(),
                            "detail": e.to_string(),
                        }),
                    );
                }
            }
            w.note_hello(host_rx, framed);
        }
        self.sidecar = Some(Sidecar::new(pod_id.to_string()));
    }

    /// Record one completed segment's sidecar entry (the recorder half of segment
    /// finalization). Written only while recording is live; a failure latches
    /// recording off process-wide. The atomic rewrite re-reads and re-serializes
    /// the whole (growing) segment list at each close — O(S²) over a log's S
    /// segments — but is bounded by `roll_max_bytes`.
    pub fn record_segment(&mut self, seg: &Segment) {
        if self.writer.is_none() {
            return;
        }
        let sc_path = sidecar_path(&self.record_dir.join(&self.log_name));
        let write_err = match self.sidecar.as_mut() {
            Some(sc) => {
                sc.push(SidecarSegment::from_segment(seg));
                sc.write_atomic(&sc_path).err()
            }
            None => None,
        };
        if let Some(e) = write_err {
            let open_path = self.record_dir.join(&self.log_name);
            self.disable("sidecar_write", &sc_path, &open_path, &e.to_string());
        }
    }

    /// Flush the frame log at a segment boundary and, if it is past the given
    /// size or age threshold, roll it: close the current file and open a fresh
    /// connection-suffixed one that replays standalone (new header, `rolled_from`
    /// set, retained `Hello` re-emitted). A fresh sidecar starts for the new log;
    /// the just-closed log's sidecar is already complete on disk. Any writer
    /// error latches recording off. Returns `true` iff a roll happened, so the
    /// caller can fire a background prune pass.
    pub fn maybe_roll(
        &mut self,
        pod_id: &str,
        conn_seq: u64,
        roll_max_bytes: u64,
        roll_max_age_s: u64,
    ) -> bool {
        // Flush at the segment boundary so a crash loses at most the in-progress
        // segment (there is none right now — we are between segments).
        let flush_err = match self.writer.as_mut() {
            Some(w) => w.flush().err(),
            None => return false,
        };
        if let Some(e) = flush_err {
            let path = self.record_dir.join(&self.log_name);
            self.disable("flush", &path, &path, &e.to_string());
            return false;
        }

        let now = HostMicros::now();
        let (over_bytes, over_age, rolled_bytes) = {
            let w = self.writer.as_mut().expect("writer present past flush");
            let bytes = w.bytes_written();
            (
                bytes > roll_max_bytes,
                w.age(now) > roll_max_age_s.saturating_mul(1_000_000),
                bytes,
            )
        };
        if !(over_bytes || over_age) {
            return false;
        }

        self.roll_seq += 1;
        let roll_iso = iso8601_ms(now.0);
        let new_name = format!(
            "{}_{roll_iso}_{conn_seq}_r{}.framelog",
            sanitize_filename(pod_id),
            self.roll_seq
        );
        let new_path = self.record_dir.join(&new_name);
        let old_path = self.record_dir.join(&self.log_name);
        let meta = LogMeta {
            build_id: BUILD_ID.to_string(),
            created_epoch_us: now,
            conn_seq,
            rolled_from: None, // `roll_to` sets this from the prior path.
        };
        // Track the new rolled log as open *before* `roll_to` creates it on disk,
        // mirroring the create/rename ordering: a new file is never on disk
        // untracked, so a concurrent prune pass cannot class it `ungated` (age 0)
        // and unlink a live capture. Both names sit in the open set across the roll.
        self.open_logs.insert(new_path.clone());
        let roll_result = self
            .writer
            .as_mut()
            .expect("writer present")
            .roll_to(&new_path, meta);
        match roll_result {
            Ok(()) => {
                self.jsonl.emit(
                    "record_rolled",
                    &json!({
                        "from": old_path.display().to_string(),
                        "to": new_path.display().to_string(),
                        "bytes": rolled_bytes,
                        "cause": if over_bytes { "bytes" } else { "age" },
                    }),
                );
                // The new name is already tracked; release the old one, now
                // closed and reclaimable by the pruner.
                self.open_logs.remove(&old_path);
                self.log_name = new_name;
                // The new log needs its own sidecar; the old one is complete on disk.
                self.sidecar = Some(Sidecar::new(pod_id));
                true
            }
            Err(e) => {
                // Drop the never-completed new name from the open set; the old
                // log closes via `disable`.
                self.open_logs.remove(&new_path);
                self.disable("roll", &new_path, &old_path, &e.to_string());
                false
            }
        }
    }

    /// Latch recording off for the whole process and emit a loud `record_error`.
    /// The pipeline is unaffected — audio flow never depends on the store. The
    /// now-closed log (`open_path`) drops from the open set so the pruner can
    /// reclaim it.
    fn disable(&mut self, cause: &str, path: &Path, open_path: &Path, detail: &str) {
        self.recording_failed.store(true, Ordering::Relaxed);
        self.writer = None;
        self.open_logs.remove(open_path);
        self.jsonl.emit(
            "record_error",
            &json!({
                "cause": cause,
                "path": path.display().to_string(),
                "detail": detail,
            }),
        );
    }

    /// Close the frame log at end of connection: flush the mid-segment tail
    /// (surfacing a failure like every other recorder write, since a silent
    /// truncation would otherwise be lost) and drop the log from the open set so
    /// the pruner can reclaim it.
    pub fn finish(&mut self) {
        if let Some(w) = self.writer.take() {
            if let Err(e) = w.finish() {
                let path = self.record_dir.join(&self.log_name);
                self.jsonl.emit(
                    "record_error",
                    &json!({
                        "cause": "finish",
                        "path": path.display().to_string(),
                        "detail": e.to_string(),
                    }),
                );
            }
            self.open_logs.remove(&self.record_dir.join(&self.log_name));
        }
    }

    /// The current log name, stamped into each assembled segment's `SegmentRef`.
    pub fn log_name(&self) -> &str {
        &self.log_name
    }
}

/// Sidecar schema version. The `wake` field and `pinned` flag exist from
/// increment 1 (always `ungated` / `false` until the wake gate lands) so later
/// increments activate retention tiering without a format change.
pub const SIDECAR_FORMAT_VERSION: u16 = 1;

/// Sentinel `pod_id` for a sidecar minted without a live pod connection
/// (e.g. pinning a log that never got a sidecar). See module docs.
pub const UNKNOWN_POD: &str = "UNKNOWN_POD";

/// A segment's wake class. Serializes to the snake-case label stored in the
/// sidecar `wake` field. The wake gate writes `Positive`/`Negative`; `Ungated`
/// is written when the gate is bypassed or made no decision. `Unknown`
/// catches any label a newer binary might write, so one unrecognized value never
/// makes a whole sidecar unparseable (which would demote a pinned log to
/// prunable via the unreadable-sidecar path).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WakeClass {
    /// No wake decision was made (gate bypassed, or a log the gate never saw).
    /// Retention tier-1, freely prunable — degrades tiering to oldest-first.
    Ungated,
    /// The gate accepted the segment. Retention tier-2 — pruned
    /// only after all tier-1 logs are gone.
    Positive,
    /// The gate rejected the segment. Retention tier-1, alongside
    /// `Ungated`.
    Negative,
    /// An unrecognized label from a newer binary. Classed tier-1, so forward
    /// compatibility never elevates an unknown class into the protected tier.
    #[serde(other)]
    Unknown,
}

/// Errors reading or writing a sidecar. A missing file is distinguished from a
/// real failure so the carry-forward path can treat "no sidecar yet" as
/// unpinned without masking corruption.
#[derive(Debug, Error)]
pub enum SidecarError {
    #[error("sidecar not found: {0}")]
    NotFound(PathBuf),
    #[error("sidecar I/O error at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("sidecar parse error at {path}: {source}")]
    Parse {
        path: PathBuf,
        source: serde_json::Error,
    },
}

/// One segment's labels within a sidecar.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SidecarSegment {
    pub segment_id: u32,
    /// Cap-rollover part index. Parts of one long span share a `segment_id` and
    /// differ only here (`0` is the sole part of an uncapped segment), so a
    /// multi-part span carries one entry per part. `#[serde(default)]` so
    /// pre-part sidecars deserialize as `part: 0`.
    #[serde(default)]
    pub part: u16,
    /// Wake class: `WakeClass::Ungated` unless the wake gate labeled this segment.
    pub wake: WakeClass,
    /// Host-clock (epoch µs) segment start — first-frame arrival.
    pub start_epoch_us: u64,
    /// Host-clock (epoch µs) segment end — the `SegmentEnd` receive stamp, or the
    /// start stamp when the segment was host-capped (no real end stamp exists).
    pub end_epoch_us: u64,
    pub end_cause: SegmentEndCause,
    pub truncated: bool,
    pub resumed: bool,
    pub gap_count: u32,
    /// Decoded S16 mono sample count for the segment.
    pub samples: u64,
}

impl SidecarSegment {
    /// Label an assembled `Segment` for the sidecar. `wake` is `ungated` — no
    /// wake decision exists in this increment.
    pub fn from_segment(seg: &Segment) -> Self {
        let start = seg.host_rx.0;
        // A host-capped segment has no `SegmentEnd` receive stamp; fall back to
        // the start so the interval is never negative.
        let end = seg.timings.segment_end_rx.map(|h| h.0).unwrap_or(start);
        Self {
            segment_id: seg.segment_id,
            part: seg.audio_ref.part,
            wake: WakeClass::Ungated,
            start_epoch_us: start,
            end_epoch_us: end,
            end_cause: seg.end.cause,
            truncated: seg.end.truncated,
            resumed: seg.end.resumed,
            gap_count: seg.end.gap_count,
            samples: seg.pcm.len() as u64,
        }
    }
}

/// A frame log's sidecar: pod identity, the pin flag, and one entry per segment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sidecar {
    pub format_version: u16,
    pub pod_id: String,
    pub pinned: bool,
    pub segments: Vec<SidecarSegment>,
}

impl Sidecar {
    /// A fresh, unpinned sidecar with no segments.
    pub fn new(pod_id: impl Into<String>) -> Self {
        Self {
            format_version: SIDECAR_FORMAT_VERSION,
            pod_id: pod_id.into(),
            pinned: false,
            segments: Vec::new(),
        }
    }

    /// Append a segment label.
    pub fn push(&mut self, segment: SidecarSegment) {
        self.segments.push(segment);
    }

    /// Read a sidecar from disk. A missing file yields `NotFound` (not an I/O
    /// error) so callers can branch on absence.
    pub fn read(path: &Path) -> Result<Self, SidecarError> {
        let bytes = match fs::read(path) {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(SidecarError::NotFound(path.to_path_buf()))
            }
            Err(source) => {
                return Err(SidecarError::Io {
                    path: path.to_path_buf(),
                    source,
                })
            }
        };
        serde_json::from_slice(&bytes).map_err(|source| SidecarError::Parse {
            path: path.to_path_buf(),
            source,
        })
    }

    /// Rewrite the sidecar atomically (temp file + rename) under the store lock,
    /// first carrying forward any pin and any out-of-band wake label already on
    /// disk so neither is cleared by an in-memory rewrite. Mutates `self` to the
    /// carried values so subsequent rewrites keep them. Holding the store lock
    /// across the carry-forward read and the rename is what makes both safe
    /// against a concurrent `pin` subcommand, `set_wake_class`, or daemon rewrite.
    pub fn write_atomic(&mut self, path: &Path) -> Result<(), SidecarError> {
        let _guard = store_lock_for(path)?;
        self.write_within_lock(path)
    }

    /// The read-modify-write body, assuming the store lock is already held by
    /// the caller. Used directly by [`set_pinned`] and [`set_wake_class`], which
    /// must hold the lock across their own read *and* this write so a concurrent
    /// daemon rewrite neither drops the new pin/label nor loses a segment
    /// appended in between.
    fn write_within_lock(&mut self, path: &Path) -> Result<(), SidecarError> {
        let disk = read_merge_view(path)?;
        self.pinned |= disk.pinned;
        // Carry forward any on-disk non-`Ungated` wake label over an in-memory
        // `Ungated` for the same segment, so a daemon rewrite from in-memory
        // state never clobbers a label written out-of-band by `set_wake_class`.
        // A label is never downgraded to `ungated`; an on-disk `Unknown` (a
        // newer-binary label) is preserved by the same rule.
        for disk_seg in &disk.segments {
            if disk_seg.wake == WakeClass::Ungated {
                continue;
            }
            if let Some(mem) = self
                .segments
                .iter_mut()
                .find(|s| s.segment_id == disk_seg.segment_id && s.part == disk_seg.part)
            {
                if mem.wake == WakeClass::Ungated {
                    mem.wake = disk_seg.wake;
                }
            }
        }
        let json = serde_json::to_vec_pretty(self).map_err(|source| SidecarError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
        let tmp = tmp_path(path);
        let io_err = |source: io::Error| SidecarError::Io {
            path: path.to_path_buf(),
            source,
        };
        fs::write(&tmp, &json).map_err(io_err)?;
        fs::rename(&tmp, path).map_err(io_err)?;
        Ok(())
    }
}

/// Set the `pinned` flag on the sidecar at `path` as a single locked
/// read-modify-write, creating a minimal sidecar if none exists. The store lock
/// is held across the read, the flag flip, and the rename, so a concurrent
/// daemon rewrite can neither drop the new pin (lost-pin TOCTOU) nor be clobbered
/// with a stale snapshot that erases a segment it appended. A present-but-
/// unreadable sidecar errors rather than being overwritten — unreadable pin
/// state fails safe toward retention, never toward silently dropping a pin.
pub fn set_pinned(path: &Path) -> Result<(), SidecarError> {
    let _guard = store_lock_for(path)?;
    let mut sidecar = match Sidecar::read(path) {
        Ok(existing) => existing,
        // No sidecar yet: mint one with the origin-unknown sentinel. The daemon
        // rewrites sidecars only for logs it currently has open, so a closed log
        // keeps this value on disk permanently.
        Err(SidecarError::NotFound(_)) => Sidecar::new(UNKNOWN_POD),
        Err(e) => return Err(e),
    };
    sidecar.pinned = true;
    sidecar.write_within_lock(path)
}

/// The outcome of a targeted wake-class update. The two non-`Updated` variants
/// are soft: recording is off or was degraded mid-run, so there is nothing to
/// label. Callers surface them as counted JSONL warnings, never as errors — a
/// sanctioned or degraded deployment must not train operators to ignore the
/// loud channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeClassUpdate {
    /// The matching segment's `wake` field was set and the sidecar rewritten.
    Updated,
    /// Nothing to label: either the store directory was absent at lock
    /// acquisition or the sidecar file for this log was never written.
    NoSidecar,
    /// The sidecar exists but carries no entry for `segment_id`. Defensive —
    /// `finalize_segment` writes the entry before the segment can be judged.
    NoSuchSegment,
}

/// Set the `wake` class of one segment part in the sidecar at `sidecar_path` as
/// a single locked read-modify-write, keyed on `(segment_id, part)` so a
/// cap-rolled span's parts (which share a `segment_id`) label independently. The
/// store lock is held across the read, the
/// field update, and the rename, so the write serializes against a concurrent
/// daemon rewrite and the out-of-process `pin` subcommand.
///
/// Two absences are soft, not errors: a missing store directory at lock
/// acquisition, and a missing sidecar file after the lock, both mean recording
/// is off or was degraded — there is nothing to label — and map to `NoSidecar`.
/// A present sidecar with no matching segment entry is `NoSuchSegment`. Real I/O
/// or parse failures other than these NotFounds stay errors, logged loudly —
/// the same posture as pin handling.
pub fn set_wake_class(
    sidecar_path: &Path,
    segment_id: u32,
    part: u16,
    wake: WakeClass,
) -> Result<WakeClassUpdate, SidecarError> {
    let _guard = match store_lock_for(sidecar_path) {
        Ok(guard) => guard,
        // A missing store directory means recording is off or was degraded —
        // nothing to label. Every other lock failure is a real I/O error.
        Err(SidecarError::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound => {
            return Ok(WakeClassUpdate::NoSidecar);
        }
        Err(e) => return Err(e),
    };
    let mut sidecar = match Sidecar::read(sidecar_path) {
        Ok(existing) => existing,
        Err(SidecarError::NotFound(_)) => return Ok(WakeClassUpdate::NoSidecar),
        Err(e) => return Err(e),
    };
    let Some(seg) = sidecar
        .segments
        .iter_mut()
        .find(|s| s.segment_id == segment_id && s.part == part)
    else {
        return Ok(WakeClassUpdate::NoSuchSegment);
    };
    seg.wake = wake;
    sidecar.write_within_lock(sidecar_path)?;
    Ok(WakeClassUpdate::Updated)
}

/// The sidecar path for a frame log: its `.framelog` extension replaced with
/// `.sidecar.json` (`{stem}.sidecar.json`).
pub fn sidecar_path(framelog: &Path) -> PathBuf {
    framelog.with_extension("sidecar.json")
}

/// The minimal read-back view for the carry-forward merge in
/// [`Sidecar::write_within_lock`]: the pin flag plus each segment's id and wake
/// class. Deserializing this rather than the whole [`Sidecar`] skips the
/// per-segment timing and string fields, keeping only what the merge reads.
#[derive(Deserialize)]
struct MergeView {
    #[serde(default)]
    pinned: bool,
    #[serde(default)]
    segments: Vec<MergeSegment>,
}

/// One segment part's id and wake class within a [`MergeView`].
#[derive(Deserialize)]
struct MergeSegment {
    segment_id: u32,
    #[serde(default)]
    part: u16,
    wake: WakeClass,
}

/// The on-disk pin and per-segment wake labels for the carry-forward merge,
/// treating a missing sidecar as empty and unpinned. A corrupt or unreadable
/// existing sidecar surfaces as an error rather than silently dropping a pin or
/// a label.
fn read_merge_view(path: &Path) -> Result<MergeView, SidecarError> {
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Ok(MergeView {
                pinned: false,
                segments: Vec::new(),
            })
        }
        Err(source) => {
            return Err(SidecarError::Io {
                path: path.to_path_buf(),
                source,
            })
        }
    };
    serde_json::from_slice(&bytes).map_err(|source| SidecarError::Parse {
        path: path.to_path_buf(),
        source,
    })
}

/// An exclusive advisory lock over all sidecar read-modify-writes in one store
/// directory, released when the guard drops (the directory file closes). Every
/// sidecar writer — the daemon's per-segment rewrite, the `pin` subcommand — and
/// the pruner's re-check-before-delete take this lock, so their read→write→rename
/// sequences never interleave. The lock object is the store directory itself:
/// stable across the sidecar's rename-based replacement (a per-sidecar lock file
/// would be replaced out from under a waiter), and it leaves no stray file
/// behind.
pub struct StoreLock {
    #[cfg(unix)]
    _dir: fs::File,
}

/// Acquire the exclusive store lock over `store_dir` (blocking until it is
/// available). The directory must already exist.
pub fn lock_store(store_dir: &Path) -> io::Result<StoreLock> {
    #[cfg(unix)]
    {
        let dir = fs::File::open(store_dir)?;
        rustix::fs::flock(&dir, rustix::fs::FlockOperation::LockExclusive)?;
        Ok(StoreLock { _dir: dir })
    }
    #[cfg(not(unix))]
    {
        // No advisory-lock primitive on this platform; the host targets Linux,
        // so this degrades to no cross-process serialization rather than failing
        // to build. Single-process safety still holds.
        let _ = store_dir;
        Ok(StoreLock {})
    }
}

/// Lock the store directory containing `sidecar_path`, mapping a lock failure to
/// `SidecarError::Io`. A sidecar path with no parent locks the current directory.
fn store_lock_for(sidecar_path: &Path) -> Result<StoreLock, SidecarError> {
    let dir = sidecar_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    lock_store(dir).map_err(|source| SidecarError::Io {
        path: dir.to_path_buf(),
        source,
    })
}

/// Sibling temp path for atomic replace: the target file name plus `.tmp`, in
/// the same directory so the rename stays on one filesystem. Safe as a shared
/// name because the store lock guarantees a single writer in the critical
/// section (see the module doc).
fn tmp_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(".tmp");
    path.with_file_name(name)
}

// ── File naming ─────────────────────────────────────────────────────────────
//
// The store names frame logs and exported segments after the pod id and a
// wall-clock instant; these helpers keep those components filesystem-safe.

/// Replace filesystem-unsafe characters in an identifier (typically a pod id,
/// which arrives untrusted from the wire) with underscores, so it is safe as a
/// file-name component on any platform. A leading `-` is rewritten to `_` as
/// well: this component is the head of the export/framelog file name, and a
/// name beginning with `-` reads as an option to unquoted-glob shell tooling
/// (`rm *.wav`, `scp *`), so an attacker-chosen pod id like `-rf` cannot smuggle
/// a flag.
pub fn sanitize_filename(s: &str) -> String {
    let mut out: String = s
        .chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => c,
            _ => '_',
        })
        .collect();
    if out.starts_with('-') {
        out.replace_range(0..1, "_");
    }
    out
}

/// Format an epoch-microseconds instant as a filesystem-safe UTC timestamp,
/// `YYYYMMDDTHHMMSS_mmmZ` (underscores for the colons and period so the string
/// is safe on every platform).
pub fn iso8601_ms(epoch_us: u64) -> String {
    let total_secs = epoch_us / 1_000_000;
    let ms = (epoch_us % 1_000_000) / 1_000;
    let ((yr, mth, day), (h, m, s)) = epoch_to_datetime(total_secs);
    format!("{yr:04}{mth:02}{day:02}T{h:02}{m:02}{s:02}_{ms:03}Z")
}

/// Decompose Unix epoch seconds into `((year, month, day), (hour, min, sec))`
/// via the standard Gregorian civil-from-days algorithm (no calendar-crate dep).
fn epoch_to_datetime(secs: u64) -> ((u32, u32, u32), (u32, u32, u32)) {
    let s_per_day = 86_400u64;
    let days_from_epoch = secs / s_per_day;
    let time_of_day = secs % s_per_day;
    let h = (time_of_day / 3_600) as u32;
    let m = ((time_of_day % 3_600) / 60) as u32;
    let s = (time_of_day % 60) as u32;

    let z = days_from_epoch as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mth = if mp < 10 { mp + 3 } else { mp - 9 };
    let yr = if mth <= 2 { y + 1 } else { y };
    ((yr as u32, mth as u32, d as u32), (h, m, s))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::segment;
    use speech_pipeline::SegmentEndInfo;

    #[test]
    fn from_segment_maps_fields() {
        let seg = segment(
            7,
            48_000,
            vec![],
            SegmentEndInfo::new(SegmentEndCause::VadRelease, false, 2, None),
        );
        let s = SidecarSegment::from_segment(&seg);
        assert_eq!(s.segment_id, 7);
        assert_eq!(s.part, 0);
        assert_eq!(s.wake, WakeClass::Ungated);
        assert_eq!(s.start_epoch_us, 1_000);
        assert_eq!(s.end_epoch_us, 5_000);
        assert_eq!(s.end_cause, SegmentEndCause::VadRelease);
        assert!(!s.truncated);
        assert!(!s.resumed);
        assert_eq!(s.gap_count, 2);
        assert_eq!(s.samples, 48_000);
    }

    #[test]
    fn host_capped_segment_end_falls_back_to_start() {
        let mut seg = segment(
            3,
            10,
            vec![],
            SegmentEndInfo::new(SegmentEndCause::HostCapped, false, 0, None),
        );
        seg.timings.segment_end_rx = None;
        let s = SidecarSegment::from_segment(&seg);
        assert_eq!(s.start_epoch_us, 1_000);
        assert_eq!(s.end_epoch_us, 1_000);
        assert!(s.truncated);
    }

    #[test]
    fn write_then_read_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pod-x_0.sidecar.json");
        let mut sc = Sidecar::new("pod-x");
        sc.push(SidecarSegment::from_segment(&segment(
            0,
            16_000,
            vec![],
            SegmentEndInfo::new(SegmentEndCause::VadRelease, false, 0, None),
        )));
        sc.write_atomic(&path).unwrap();

        let read = Sidecar::read(&path).unwrap();
        assert_eq!(read, sc);
        assert_eq!(read.format_version, SIDECAR_FORMAT_VERSION);
        assert_eq!(read.pod_id, "pod-x");
        assert!(!read.pinned);
        assert_eq!(read.segments.len(), 1);
    }

    #[test]
    fn rewrite_overwrites_atomically_leaving_no_temp() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pod-x_0.sidecar.json");
        let mut sc = Sidecar::new("pod-x");
        sc.write_atomic(&path).unwrap();
        sc.push(SidecarSegment::from_segment(&segment(
            1,
            10,
            vec![],
            SegmentEndInfo::new(SegmentEndCause::VadRelease, false, 0, None),
        )));
        sc.write_atomic(&path).unwrap();

        let read = Sidecar::read(&path).unwrap();
        assert_eq!(read.segments.len(), 1);
        // The temp file was renamed away, not left behind.
        assert!(!tmp_path(&path).exists());
    }

    #[test]
    fn write_carries_forward_an_on_disk_pin() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pod-x_0.sidecar.json");

        // Operator pins the log out-of-band.
        let mut pinned = Sidecar::new("pod-x");
        pinned.pinned = true;
        pinned.write_atomic(&path).unwrap();

        // The daemon rewrites with an in-memory sidecar that thinks it is unpinned.
        let mut daemon = Sidecar::new("pod-x");
        assert!(!daemon.pinned);
        daemon.write_atomic(&path).unwrap();

        // The pin survives, and the in-memory copy now reflects it.
        assert!(daemon.pinned);
        assert!(Sidecar::read(&path).unwrap().pinned);
    }

    #[test]
    fn read_missing_sidecar_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("absent.sidecar.json");
        assert!(matches!(
            Sidecar::read(&path),
            Err(SidecarError::NotFound(_))
        ));
    }

    #[test]
    fn read_corrupt_sidecar_is_parse_error_not_masked() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pod-x_0.sidecar.json");
        std::fs::write(&path, b"{ this is not json").unwrap();
        // A present-but-unparseable sidecar is a distinct Parse error, never
        // collapsed into "no sidecar" (which would drop a pin).
        assert!(matches!(
            Sidecar::read(&path),
            Err(SidecarError::Parse { .. })
        ));
    }

    #[test]
    fn write_atomic_on_corrupt_sidecar_errors_rather_than_clobbering() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pod-x_0.sidecar.json");
        std::fs::write(&path, b"garbage not json").unwrap();
        // The carry-forward read-back fails to parse, so the rewrite surfaces an
        // error instead of silently treating the corrupt file as unpinned and
        // overwriting it — the pin (if any) is never dropped by masking.
        let mut sc = Sidecar::new("pod-x");
        assert!(matches!(
            sc.write_atomic(&path),
            Err(SidecarError::Parse { .. })
        ));
    }

    #[test]
    fn set_pinned_without_sidecar_mints_unknown_pod() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pod-x_0.sidecar.json");
        // No sidecar exists yet — pinning mints one whose origin is unknown.
        set_pinned(&path).unwrap();
        let read = Sidecar::read(&path).unwrap();
        assert!(read.pinned);
        assert_eq!(read.pod_id, UNKNOWN_POD);
    }

    #[test]
    fn minted_unknown_pod_serializes_as_literal_string() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pod-x_0.sidecar.json");
        set_pinned(&path).unwrap();
        // Pin the on-disk wire form independently of the constant.
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(
            raw.contains(r#""pod_id": "UNKNOWN_POD""#),
            "sidecar JSON missing sentinel pod_id: {raw}"
        );
    }

    /// Write a sidecar carrying one `Ungated` entry for `segment_id` at `path`.
    fn seed_sidecar(path: &Path, segment_id: u32) {
        let mut sc = Sidecar::new("pod-x");
        sc.push(SidecarSegment::from_segment(&segment(
            segment_id,
            16_000,
            vec![],
            SegmentEndInfo::new(SegmentEndCause::VadRelease, false, 0, None),
        )));
        sc.write_atomic(path).unwrap();
    }

    #[test]
    fn set_wake_class_updates_the_matching_segment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pod-x_0.sidecar.json");
        seed_sidecar(&path, 3);

        assert_eq!(
            set_wake_class(&path, 3, 0, WakeClass::Positive).unwrap(),
            WakeClassUpdate::Updated
        );
        let read = Sidecar::read(&path).unwrap();
        assert_eq!(read.segments[0].wake, WakeClass::Positive);
    }

    #[test]
    fn set_wake_class_missing_store_dir_is_no_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        // Parent directory does not exist, so the store lock's open fails
        // NotFound — a soft outcome, never an I/O error.
        let path = dir.path().join("nostore").join("pod-x_0.sidecar.json");
        assert_eq!(
            set_wake_class(&path, 0, 0, WakeClass::Positive).unwrap(),
            WakeClassUpdate::NoSidecar
        );
    }

    #[test]
    fn set_wake_class_missing_sidecar_file_is_no_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        // Store directory exists (the lock acquires) but the sidecar was never
        // written.
        let path = dir.path().join("pod-x_0.sidecar.json");
        assert_eq!(
            set_wake_class(&path, 0, 0, WakeClass::Positive).unwrap(),
            WakeClassUpdate::NoSidecar
        );
    }

    #[test]
    fn set_wake_class_unknown_segment_is_no_such_segment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pod-x_0.sidecar.json");
        seed_sidecar(&path, 0);
        assert_eq!(
            set_wake_class(&path, 5, 0, WakeClass::Positive).unwrap(),
            WakeClassUpdate::NoSuchSegment
        );
        // The existing entry is untouched.
        assert_eq!(
            Sidecar::read(&path).unwrap().segments[0].wake,
            WakeClass::Ungated
        );
    }

    #[test]
    fn set_wake_class_disambiguates_parts_of_one_span() {
        // Two cap-rolled parts share `segment_id` 7 and differ only in `part`.
        // Labeling `(7, 1)` must touch the second entry, never the first.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pod-x_0.sidecar.json");
        let entry = |part: u16| SidecarSegment {
            segment_id: 7,
            part,
            wake: WakeClass::Ungated,
            start_epoch_us: 1,
            end_epoch_us: 2,
            end_cause: SegmentEndCause::HostCapped,
            truncated: true,
            resumed: part > 0,
            gap_count: 0,
            samples: 16,
        };
        let mut sc = Sidecar::new("pod-x");
        sc.push(entry(0));
        sc.push(entry(1));
        sc.write_atomic(&path).unwrap();

        assert_eq!(
            set_wake_class(&path, 7, 1, WakeClass::Positive).unwrap(),
            WakeClassUpdate::Updated
        );
        let read = Sidecar::read(&path).unwrap();
        assert_eq!(
            read.segments[0].wake,
            WakeClass::Ungated,
            "part 0 untouched"
        );
        assert_eq!(read.segments[1].wake, WakeClass::Positive, "part 1 labeled");
        // A part with no entry is `NoSuchSegment`, not a wrong-part label.
        assert_eq!(
            set_wake_class(&path, 7, 2, WakeClass::Positive).unwrap(),
            WakeClassUpdate::NoSuchSegment
        );
    }

    #[test]
    fn legacy_sidecar_without_part_deserializes_as_part_zero() {
        // A pre-part sidecar (no `part` field) round-trips with `part: 0`, so old
        // record stores stay labelable.
        let json = r#"{"format_version":1,"pod_id":"pod-x","pinned":false,
            "segments":[{"segment_id":4,"wake":"ungated","start_epoch_us":1,
            "end_epoch_us":2,"end_cause":"vad_release","truncated":false,
            "resumed":false,"gap_count":0,"samples":16}]}"#;
        let sc: Sidecar = serde_json::from_str(json).unwrap();
        assert_eq!(sc.segments[0].part, 0);
    }

    /// Write a sidecar carrying one entry for `segment_id` with the given wake
    /// class at `path`, out-of-band (as `set_wake_class` would leave it).
    fn seed_sidecar_with_wake(path: &Path, segment_id: u32, wake: WakeClass) {
        seed_sidecar(path, segment_id);
        set_wake_class(path, segment_id, 0, wake).unwrap();
    }

    #[test]
    fn daemon_rewrite_carries_forward_an_out_of_band_positive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pod-x_0.sidecar.json");

        // A `positive` label is written out-of-band (mirror of the pin case).
        seed_sidecar_with_wake(&path, 3, WakeClass::Positive);

        // The daemon rewrites from in-memory state that still thinks the segment
        // is `Ungated` (it never saw the wake update).
        let mut daemon = Sidecar::new("pod-x");
        daemon.push(SidecarSegment::from_segment(&segment(
            3,
            16_000,
            vec![],
            SegmentEndInfo::new(SegmentEndCause::VadRelease, false, 0, None),
        )));
        assert_eq!(daemon.segments[0].wake, WakeClass::Ungated);
        daemon.write_atomic(&path).unwrap();

        // The label survives on disk, and the in-memory copy now reflects it so
        // the next rewrite keeps it.
        assert_eq!(daemon.segments[0].wake, WakeClass::Positive);
        assert_eq!(
            Sidecar::read(&path).unwrap().segments[0].wake,
            WakeClass::Positive
        );
    }

    #[test]
    fn daemon_rewrite_preserves_an_on_disk_unknown() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pod-x_0.sidecar.json");
        seed_sidecar_with_wake(&path, 1, WakeClass::Unknown);

        let mut daemon = Sidecar::new("pod-x");
        daemon.push(SidecarSegment::from_segment(&segment(
            1,
            16_000,
            vec![],
            SegmentEndInfo::new(SegmentEndCause::VadRelease, false, 0, None),
        )));
        daemon.write_atomic(&path).unwrap();

        // A newer-binary `Unknown` label is a non-`Ungated` class and is carried
        // forward, not overwritten by the in-memory `Ungated`.
        assert_eq!(
            Sidecar::read(&path).unwrap().segments[0].wake,
            WakeClass::Unknown
        );
    }

    #[test]
    fn daemon_rewrite_never_downgrades_a_label_to_ungated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pod-x_0.sidecar.json");

        // On disk the segment is `Ungated`; in memory the daemon holds a fresh
        // `Negative` for the same segment (the merge must not let the on-disk
        // `Ungated` overwrite it).
        seed_sidecar(&path, 2);
        let mut daemon = Sidecar::new("pod-x");
        let mut seg = SidecarSegment::from_segment(&segment(
            2,
            16_000,
            vec![],
            SegmentEndInfo::new(SegmentEndCause::VadRelease, false, 0, None),
        ));
        seg.wake = WakeClass::Negative;
        daemon.push(seg);
        daemon.write_atomic(&path).unwrap();

        assert_eq!(daemon.segments[0].wake, WakeClass::Negative);
        assert_eq!(
            Sidecar::read(&path).unwrap().segments[0].wake,
            WakeClass::Negative
        );
    }

    #[test]
    fn sidecar_path_replaces_framelog_extension() {
        assert_eq!(
            sidecar_path(Path::new("/store/pod-x_2026_0.framelog")),
            PathBuf::from("/store/pod-x_2026_0.sidecar.json")
        );
    }

    #[test]
    fn sanitize_filename_replaces_unsafe_chars() {
        assert_eq!(sanitize_filename("pod-a_1B2"), "pod-a_1B2");
        assert_eq!(sanitize_filename("pod/../x y"), "pod____x_y");
    }

    #[test]
    fn sanitize_filename_never_starts_with_dash() {
        // A dash-leading pod id would read as a CLI option to unquoted-glob
        // shell tooling; the leading dash is rewritten so it cannot.
        assert_eq!(sanitize_filename("-rf"), "_rf");
        assert_eq!(sanitize_filename("--force"), "_-force");
        // Interior dashes are preserved (they are shell-safe).
        assert_eq!(sanitize_filename("pod-a1b2c3"), "pod-a1b2c3");
    }

    #[test]
    fn iso8601_ms_formats_a_known_epoch() {
        // 2023-11-14T22:13:20.000Z, plus 456 ms.
        assert_eq!(iso8601_ms(1_700_000_000_456_000), "20231114T221320_456Z");
        assert_eq!(iso8601_ms(0), "19700101T000000_000Z");
    }
}
