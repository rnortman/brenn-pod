//! Tiered retention pruning over the record store.
//!
//! When the store exceeds `cap_bytes` the pruner deletes whole frame logs
//! (framelog + sidecar together) until it is back under the cap. Deletion order
//! is class-tiered so the store degrades gracefully rather than by pure age:
//!
//! 1. **Tier 1** — logs whose best segment is `negative`/`ungated` (no
//!    wake-positive segment), oldest first. A log with no sidecar at all is
//!    `ungated`, so crash leftovers stay freely prunable.
//! 2. **Tier 2** — logs containing a `positive` segment, oldest first, reached
//!    only once tier 1 is exhausted.
//! 3. **Pinned logs are never candidates.** If only pinned (and currently-open)
//!    logs remain and the store is still over cap, the pruner halts and reports
//!    it rather than deleting a pin — the cap is advisory against pins by design.
//! 4. **A present-but-unreadable sidecar is protective, never prunable.** If a
//!    sidecar exists but fails to parse (corrupt, truncated), its pin state is
//!    unknown, so the log is kept — unreadable pin state fails safe toward
//!    retention — and the pruner returns it in `PruneOutcome::kept_corrupt` so
//!    the caller complains loudly rather than deleting a possibly-pinned
//!    recording.
//!
//! With every segment `ungated` (increments 1–3), all logs land in tier 1 and
//! this degrades to plain oldest-first.
//!
//! The pruner is synchronous filesystem work (the daemon runs it on
//! `spawn_blocking`) and emits no JSONL itself: it returns a [`PruneOutcome`]
//! the caller renders as `record_pruned` / `prune_sidecar_corrupt` /
//! `prune_halted` events.

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::Serialize;

use crate::recorder::{lock_store, sidecar_path, Sidecar, SidecarError, WakeClass, UNKNOWN_POD};

/// Retention tier of a frame log, ordered so tier 1 (freely prunable) sorts
/// before tier 2 (holds a wake-positive segment). Serializes to a snake-case
/// label for the `record_pruned` JSONL line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PruneTier {
    /// Best segment is `negative`/`ungated`, or no sidecar.
    Ungated,
    /// Contains at least one `positive` segment.
    Positive,
}

/// Why a log was deleted: to bring an over-quota pod bucket back under its
/// per-pod quota (phase 1), or to bring the whole store back under the global
/// cap (phase 2). Serializes to a snake-case label for the `record_pruned` line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PruneReason {
    /// Deleted by the per-pod quota phase (its owning bucket was over quota).
    PodQuota,
    /// Deleted by the global-cap phase.
    GlobalCap,
}

/// Which pod a store log's bytes are attributed to for quota accounting. Logs
/// with no readable attribution — no sidecar, the `UNKNOWN_POD` sentinel, or a
/// corrupt sidecar — all fold into one shared `Unattributed` bucket subject to
/// the same quota.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PodBucket {
    Pod(String),
    Unattributed,
}

impl PodBucket {
    /// The pod id for reporting, or `None` for the unattributed bucket.
    fn pod_id(&self) -> Option<String> {
        match self {
            PodBucket::Pod(id) => Some(id.clone()),
            PodBucket::Unattributed => None,
        }
    }
}

/// Inputs to one prune pass.
pub struct PruneRequest<'a> {
    /// Record-store directory to scan for `*.framelog` files.
    pub store_dir: &'a Path,
    /// Byte budget; the pass deletes candidates until the store is at or under it.
    pub cap_bytes: u64,
    /// Per-pod byte quota: phase 1 drains any pod bucket whose total exceeds this
    /// (deleting only that pod's own logs) before the global-cap phase runs. A
    /// value `>= cap_bytes` makes phase 1 inert.
    pub pod_cap_bytes: u64,
    /// Frame logs currently open for writing — never inventoried for deletion,
    /// but their bytes count toward the total. The recorder owns this set; it is
    /// the *live* set, re-checked immediately before each deletion (not a
    /// pass-start snapshot), so a log opened mid-pass is never unlinked.
    pub open_logs: &'a Mutex<HashSet<PathBuf>>,
}

/// One frame log deleted by a prune pass. The caller emits a `record_pruned`
/// line per entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrunedLog {
    pub framelog: PathBuf,
    /// The sidecar deleted alongside the framelog, if one was present.
    pub sidecar: Option<PathBuf>,
    /// Framelog + sidecar bytes reclaimed.
    pub bytes: u64,
    pub tier: PruneTier,
    /// The pod this log's bytes were attributed to, or `None` for the shared
    /// unattributed bucket (no sidecar, `UNKNOWN_POD`, or corrupt sidecar).
    pub pod_id: Option<String>,
    /// Which phase deleted this log.
    pub reason: PruneReason,
}

/// A pod bucket still over its quota after phase 1 — its residue is pinned,
/// open, or corrupt and cannot be drained. The caller emits a
/// `prune_pod_over_quota` warn line. This is the per-pod analog of
/// [`PruneHalt`] and deliberately does not imply the store is over its global
/// cap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodOverQuota {
    /// The pod id, or `None` for the shared unattributed bucket.
    pub pod_id: Option<String>,
    /// Bytes still attributed to the bucket after phase 1.
    pub remaining_bytes: u64,
    pub pod_cap_bytes: u64,
}

/// The store is still over cap after every deletable log was removed — the
/// caller emits a `prune_halted` error line. Recording continues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruneHalt {
    /// Bytes remaining in the store (pinned, open, or undeletable logs).
    pub remaining_bytes: u64,
    pub cap_bytes: u64,
}

/// A candidate whose deletion failed (permissions, a wedged file). The pass
/// records it and moves on rather than aborting, so one bad file cannot wedge
/// the whole store; the caller emits a `prune_delete_error` line per entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruneFailure {
    pub framelog: PathBuf,
    pub error: String,
}

/// A log kept because its sidecar is present but unreadable — its pin state is
/// unknown, so retention fails safe and the caller emits a loud error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorruptSidecar {
    pub framelog: PathBuf,
    /// The sidecar parse/read error, for the loud report.
    pub error: String,
}

/// Result of one prune pass.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PruneOutcome {
    pub pruned: Vec<PrunedLog>,
    /// Deletions that failed; the pass skipped them and continued.
    pub failed: Vec<PruneFailure>,
    /// Logs kept because their sidecar was present but unreadable; the caller
    /// emits a loud error per entry (retention failed safe toward keeping them).
    pub kept_corrupt: Vec<CorruptSidecar>,
    /// Set when the store stays over cap because only pinned/open/undeletable
    /// logs are left.
    pub halted: Option<PruneHalt>,
    /// Pod buckets still over their per-pod quota after phase 1 (their residue
    /// is pinned/open/corrupt). Reported, never force-drained.
    pub over_quota: Vec<PodOverQuota>,
}

/// A store log seen by the inventory, before any deletion decision.
struct LogEntry {
    framelog: PathBuf,
    sidecar: Option<PathBuf>,
    bytes: u64,
    tier: PruneTier,
    /// Earliest segment start (epoch µs), or 0 for a log with no/empty sidecar —
    /// so unlabeled crash leftovers sort oldest and prune first.
    age_key: u64,
    pinned: bool,
    open: bool,
    /// Pod bucket this log's bytes are attributed to for quota accounting.
    pod: PodBucket,
    /// Set to the parse/read error when a sidecar file is present but failed to
    /// parse. Its pin state is unknown, so the log is never deleted (fails safe
    /// toward retention) and the caller is told loudly.
    corrupt_error: Option<String>,
}

/// Run one prune pass over `req.store_dir` in two phases. Both delete
/// framelog+sidecar pairs oldest-first within tier, skip pinned and
/// currently-open logs, and record a failed delete in `PruneOutcome::failed`
/// (skipping it) so one wedged file cannot abort the pass and discard the
/// record of everything already deleted.
///
/// **Phase 1 — per-pod quota.** Each pod bucket whose total exceeds
/// `pod_cap_bytes` is drained of *its own* logs (same tier/age order and guards)
/// until it is at or under quota. This runs even when the store is under the
/// global cap, so a flooder is bounded the moment it crosses its quota — a
/// single spoofed pod id (default quota `cap_bytes / 2`) can never evict every
/// other pod's recordings. **Phase 2 — global cap.** The classic pass, over the
/// post-phase-1 totals.
///
/// Pod identities are authenticated at the TLS-PSK handshake, so the buckets
/// this quota bounds belong to provisioned pods — a flooder cannot mint fresh
/// identities to win a quota each.
pub fn prune(req: &PruneRequest) -> io::Result<PruneOutcome> {
    let mut logs = inventory(req)?;
    let mut outcome = PruneOutcome::default();
    // Report every corrupt sidecar encountered, regardless of cap: an unreadable
    // sidecar is a fault the operator must fix, and it kept its log from
    // deletion. Collected before the under-cap early return so it is never lost.
    for log in &logs {
        if let Some(error) = &log.corrupt_error {
            outcome.kept_corrupt.push(CorruptSidecar {
                framelog: log.framelog.clone(),
                error: error.clone(),
            });
        }
    }

    let mut total: u64 = logs.iter().map(|l| l.bytes).sum();

    // Common case: under the global cap and every bucket under quota — nothing to
    // delete, skip the sort entirely. The pruner runs after every roll, and in
    // steady state both conditions hold on the vast majority of those passes.
    // The gate's per-bucket tally borrows the pod keys (no per-log allocation on
    // this hot path); the owned, decrementable map below is built only when there
    // is real work to do.
    let any_over_quota = {
        let mut totals: HashMap<&PodBucket, u64> = HashMap::new();
        for log in &logs {
            *totals.entry(&log.pod).or_insert(0) += log.bytes;
        }
        totals.values().any(|&b| b > req.pod_cap_bytes)
    };
    if total <= req.cap_bytes && !any_over_quota {
        return Ok(outcome);
    }

    // Per-bucket totals, decremented as phase 1 drains each over-quota bucket.
    let mut bucket_totals: HashMap<PodBucket, u64> = HashMap::new();
    for log in &logs {
        *bucket_totals.entry(log.pod.clone()).or_insert(0) += log.bytes;
    }

    // Deletion order: tier 1 before tier 2, oldest first within a tier, then by
    // path for a deterministic tiebreak. This global order also fixes the order
    // within each bucket (relative order is preserved), so phase 1 drains a
    // bucket in the same tier/age sequence phase 2 would.
    logs.sort_by(|a, b| {
        a.tier
            .cmp(&b.tier)
            .then(a.age_key.cmp(&b.age_key))
            .then_with(|| a.framelog.cmp(&b.framelog))
    });

    // Phase 1: drain each over-quota bucket of its own logs. Walking the globally
    // sorted list and gating on the log's owning bucket handles every bucket in
    // one pass; a bucket stops being drained the moment its running total drops
    // to quota.
    // Every log phase 1 acts on — deleted, skipped, or failed — is recorded here
    // so phase 2 never revisits it in the same pass. Retrying a phase-1 *failure*
    // in phase 2 would double-record it (a spurious second `prune_delete_error`)
    // and, in the partial-delete case (sidecar unlinked, framelog unlink failed),
    // hit the already-gone sidecar with a NotFound and return before retrying the
    // now-deletable framelog. A failed candidate is retried on the next pass,
    // which re-inventories from disk.
    let mut attempted: HashSet<PathBuf> = HashSet::new();
    for log in &logs {
        let bucket_total = bucket_totals.get(&log.pod).copied().unwrap_or(0);
        if bucket_total <= req.pod_cap_bytes {
            continue;
        }
        attempted.insert(log.framelog.clone());
        if let Some(bytes) = try_delete(log, req, &mut outcome, PruneReason::PodQuota) {
            *bucket_totals
                .get_mut(&log.pod)
                .expect("bucket total present") -= bytes;
            total -= bytes;
        }
    }

    // Buckets still over quota after phase 1: pinned/open/corrupt residue that
    // cannot be drained. Reported, never force-drained; does not set `halted`.
    for (bucket, &remaining) in &bucket_totals {
        if remaining > req.pod_cap_bytes {
            outcome.over_quota.push(PodOverQuota {
                pod_id: bucket.pod_id(),
                remaining_bytes: remaining,
                pod_cap_bytes: req.pod_cap_bytes,
            });
        }
    }
    // Deterministic ordering for stable JSONL output and tests.
    outcome.over_quota.sort_by(|a, b| a.pod_id.cmp(&b.pod_id));

    // Phase 2: the classic global-cap pass over the post-phase-1 totals, skipping
    // logs phase 1 already removed.
    for log in &logs {
        if total <= req.cap_bytes {
            break;
        }
        if attempted.contains(&log.framelog) {
            continue;
        }
        if let Some(bytes) = try_delete(log, req, &mut outcome, PruneReason::GlobalCap) {
            total -= bytes;
        }
    }

    if total > req.cap_bytes {
        outcome.halted = Some(PruneHalt {
            remaining_bytes: total,
            cap_bytes: req.cap_bytes,
        });
    }
    Ok(outcome)
}

/// Attempt to delete one candidate log, applying every retention guard. Returns
/// the reclaimed bytes on success, or `None` when the log was skipped (corrupt,
/// pinned, open, upgraded on disk) or its deletion failed (recorded in
/// `outcome.failed`). Shared by both prune phases so the guards and the
/// sidecar-first unlink live in one place.
fn try_delete(
    log: &LogEntry,
    req: &PruneRequest,
    outcome: &mut PruneOutcome,
    reason: PruneReason,
) -> Option<u64> {
    if log.corrupt_error.is_some() {
        // A present-but-unreadable sidecar is protective: its pin state is
        // unknown, so retention fails safe and the log is never deleted (it was
        // already recorded in `kept_corrupt` for the loud report). Bytes still
        // count toward the totals, so a bucket/store of only corrupt logs halts.
        return None;
    }
    // Re-check the live open set right before deleting: a log opened for writing
    // after this pass's inventory (a new connection, a roll) must never be
    // unlinked out from under its writer.
    let opened_since_inventory = req
        .open_logs
        .lock()
        .expect("open-logs set poisoned")
        .contains(&log.framelog);
    if opened_since_inventory || log.open || log.pinned {
        return None;
    }
    // Re-read the on-disk sidecar under the store lock, held across the re-check
    // and the unlink below, so an out-of-process `pin` or an out-of-band wake
    // verdict (`set_wake_class`) that lands after this pass's inventory is still
    // observed and its log is spared. Unreadable state now (a sidecar gone
    // corrupt since inventory) or a lock failure fails safe toward retention —
    // never delete a possibly-pinned or higher-tier recording. The guard drops
    // when this function returns, releasing the lock between candidates.
    let _guard = match lock_store(req.store_dir) {
        Ok(g) => g,
        Err(e) => {
            outcome.failed.push(PruneFailure {
                framelog: log.framelog.clone(),
                error: format!("store lock: {e}"),
            });
            return None;
        }
    };
    if !on_disk_prunable(&log.framelog, log.tier) {
        return None;
    }
    // Delete the sidecar first: if the pair is interrupted after this, the
    // framelog is left sidecar-less (classed ungated, prunable next pass) rather
    // than leaving an orphan sidecar the framelog-keyed inventory would never
    // revisit. A failed delete is recorded and skipped, not propagated — one
    // wedged file must not abort the whole pass and lose the record of
    // everything already deleted.
    if let Some(sidecar) = &log.sidecar {
        if let Err(e) = std::fs::remove_file(sidecar) {
            outcome.failed.push(PruneFailure {
                framelog: log.framelog.clone(),
                error: e.to_string(),
            });
            return None;
        }
    }
    if let Err(e) = std::fs::remove_file(&log.framelog) {
        outcome.failed.push(PruneFailure {
            framelog: log.framelog.clone(),
            error: e.to_string(),
        });
        return None;
    }
    outcome.pruned.push(PrunedLog {
        framelog: log.framelog.clone(),
        sidecar: log.sidecar.clone(),
        bytes: log.bytes,
        tier: log.tier,
        pod_id: log.pod.pod_id(),
        reason,
    });
    Some(log.bytes)
}

/// Scan the store for `*.framelog` files and label each with its bytes, tier,
/// age, pin state, and whether it is currently open. A missing directory is an
/// empty store, not an error (a fresh daemon prunes before creating it).
fn inventory(req: &PruneRequest) -> io::Result<Vec<LogEntry>> {
    let entries = match std::fs::read_dir(req.store_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };

    let mut logs = Vec::new();
    for entry in entries {
        let framelog = entry?.path();
        if framelog.extension().and_then(|e| e.to_str()) != Some("framelog") {
            continue;
        }
        // A framelog can vanish between the directory read and this stat (a
        // connection renaming its pre-`Hello` log, a concurrent prune): treat
        // the gone file as "skip this entry", matching the sidecar branch
        // below, rather than aborting the whole retention pass on a benign race.
        let framelog_bytes = match std::fs::metadata(&framelog) {
            Ok(meta) => meta.len(),
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };

        // A sidecar file may or may not exist beside the log. Its bytes always
        // count and it is always deleted with the log, even when its JSON is
        // unreadable — parseability only governs tier/pin classification.
        let sidecar_candidate = sidecar_path(&framelog);
        let (sidecar, sidecar_bytes) = match std::fs::metadata(&sidecar_candidate) {
            Ok(meta) => (Some(sidecar_candidate.clone()), meta.len()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => (None, 0),
            Err(e) => return Err(e),
        };

        // Classify from the sidecar. Three outcomes:
        //   * parsed OK        → tier/pin/age from its contents.
        //   * absent           → ungated + unpinned; a crash leftover, prunable.
        //   * present, unreadable → protective: keep the log (its pin state is
        //     unknown, so retention fails safe) and complain loudly. A file that
        //     raced away between the metadata stat and this read is treated as
        //     absent, not corrupt.
        // Attribution folds three "no readable pod" cases into one shared
        // `Unattributed` bucket: absent sidecar, corrupt sidecar, and the
        // `UNKNOWN_POD` sentinel (a sidecar minted with no live connection).
        let mut corrupt_error = None;
        let (tier, pinned, age_key, pod) = match sidecar.as_ref().map(|path| Sidecar::read(path)) {
            Some(Ok(s)) => {
                let age = s
                    .segments
                    .iter()
                    .map(|seg| seg.start_epoch_us)
                    .min()
                    .unwrap_or(0);
                let pod = if s.pod_id == UNKNOWN_POD {
                    PodBucket::Unattributed
                } else {
                    PodBucket::Pod(s.pod_id.clone())
                };
                (tier_of(&s), s.pinned, age, pod)
            }
            None | Some(Err(SidecarError::NotFound(_))) => {
                (PruneTier::Ungated, false, 0, PodBucket::Unattributed)
            }
            Some(Err(e)) => {
                // Protective: keep the log (its pin state is unknown, so
                // retention fails safe) and hand the error to the caller for a
                // loud report rather than deleting a possibly-pinned recording.
                corrupt_error = Some(e.to_string());
                (PruneTier::Ungated, false, 0, PodBucket::Unattributed)
            }
        };

        let open = req
            .open_logs
            .lock()
            .expect("open-logs set poisoned")
            .contains(&framelog);
        logs.push(LogEntry {
            open,
            framelog,
            sidecar,
            bytes: framelog_bytes + sidecar_bytes,
            tier,
            age_key,
            pinned,
            pod,
            corrupt_error,
        });
    }
    Ok(logs)
}

/// Whether `framelog` may be deleted while draining `draining_tier`, judged from
/// its *live* on-disk sidecar rather than the inventory-time snapshot. Re-deriving
/// both pin state and tier is load-bearing: a `pin` can *create* a sidecar on a
/// previously sidecar-less log after this pass's inventory (trusting the snapshot
/// would delete the freshly-pinned recording), and a wake verdict written
/// out-of-band (`set_wake_class`) can lift an inventoried-`ungated` log into a
/// higher tier after the tier was captured — deleting it on the stale tier-1
/// classification would drop a just-detected wake while lower-tier logs survive.
/// A missing sidecar reads as unpinned tier-1 and prunable; a present-but-
/// unreadable one fails safe toward retention. The pruner calls this under the
/// store lock, so a concurrent `pin` or wake update cannot land between this read
/// and the unlink.
fn on_disk_prunable(framelog: &Path, draining_tier: PruneTier) -> bool {
    match Sidecar::read(&sidecar_path(framelog)) {
        Ok(sidecar) => !sidecar.pinned && tier_of(&sidecar) <= draining_tier,
        Err(SidecarError::NotFound(_)) => true,
        Err(_) => false,
    }
}

/// A log is tier 2 iff any of its segments is wake-positive; otherwise tier 1.
fn tier_of(sidecar: &Sidecar) -> PruneTier {
    if sidecar
        .segments
        .iter()
        .any(|s| s.wake == WakeClass::Positive)
    {
        PruneTier::Positive
    } else {
        PruneTier::Ungated
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recorder::SidecarSegment;
    use speech_pipeline::SegmentEndCause;

    /// Write a framelog of `size` bytes plus a sidecar labelling one segment with
    /// `wake` class starting at `start_epoch_us`, optionally pinned. Returns the
    /// framelog path.
    fn write_log(
        dir: &Path,
        name: &str,
        size: usize,
        wake: WakeClass,
        start_epoch_us: u64,
        pinned: bool,
    ) -> PathBuf {
        write_log_pod(dir, name, size, wake, start_epoch_us, pinned, "pod-x")
    }

    /// As [`write_log`], but the sidecar carries `pod_id` so per-pod quota tests
    /// can attribute bytes to a chosen bucket.
    #[allow(clippy::too_many_arguments)]
    fn write_log_pod(
        dir: &Path,
        name: &str,
        size: usize,
        wake: WakeClass,
        start_epoch_us: u64,
        pinned: bool,
        pod_id: &str,
    ) -> PathBuf {
        let framelog = dir.join(format!("{name}.framelog"));
        std::fs::write(&framelog, vec![0u8; size]).unwrap();
        let mut sidecar = Sidecar::new(pod_id);
        sidecar.pinned = pinned;
        sidecar.push(SidecarSegment {
            segment_id: 0,
            part: 0,
            wake,
            start_epoch_us,
            end_epoch_us: start_epoch_us + 1,
            end_cause: SegmentEndCause::VadRelease,
            truncated: false,
            resumed: false,
            gap_count: 0,
            samples: 16_000,
        });
        sidecar.write_atomic(&sidecar_path(&framelog)).unwrap();
        framelog
    }

    /// A prune request with per-pod enforcement effectively disabled
    /// (`pod_cap_bytes = u64::MAX`), so the classic global-cap tests exercise
    /// phase 2 alone.
    fn request<'a>(
        dir: &'a Path,
        cap_bytes: u64,
        open_logs: &'a Mutex<HashSet<PathBuf>>,
    ) -> PruneRequest<'a> {
        request_q(dir, cap_bytes, u64::MAX, open_logs)
    }

    /// A prune request with an explicit per-pod quota.
    fn request_q<'a>(
        dir: &'a Path,
        cap_bytes: u64,
        pod_cap_bytes: u64,
        open_logs: &'a Mutex<HashSet<PathBuf>>,
    ) -> PruneRequest<'a> {
        PruneRequest {
            store_dir: dir,
            cap_bytes,
            pod_cap_bytes,
            open_logs,
        }
    }

    fn pruned_names(outcome: &PruneOutcome) -> Vec<String> {
        outcome
            .pruned
            .iter()
            .map(|p| {
                p.framelog
                    .file_stem()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect()
    }

    #[test]
    fn all_ungated_prunes_oldest_first() {
        let dir = tempfile::tempdir().unwrap();
        // Three 1000-byte logs (plus sidecars); cap forces two deletions.
        write_log(dir.path(), "c", 1000, WakeClass::Ungated, 300, false);
        write_log(dir.path(), "a", 1000, WakeClass::Ungated, 100, false);
        write_log(dir.path(), "b", 1000, WakeClass::Ungated, 200, false);
        let open = Mutex::new(HashSet::new());

        // Under a cap leaving room for ~one log, the two oldest go first.
        let outcome = prune(&request(dir.path(), 1500, &open)).unwrap();
        assert_eq!(pruned_names(&outcome), vec!["a", "b"]);
        assert!(outcome.halted.is_none());
    }

    #[test]
    fn tier_one_pruned_before_tier_two() {
        let dir = tempfile::tempdir().unwrap();
        // A newer ungated log and an older positive log: age says the positive
        // is older, but tiering deletes the ungated first.
        write_log(
            dir.path(),
            "positive_old",
            1000,
            WakeClass::Positive,
            100,
            false,
        );
        write_log(
            dir.path(),
            "ungated_new",
            1000,
            WakeClass::Ungated,
            200,
            false,
        );
        let open = Mutex::new(HashSet::new());

        let outcome = prune(&request(dir.path(), 1500, &open)).unwrap();
        assert_eq!(pruned_names(&outcome), vec!["ungated_new"]);
        assert_eq!(outcome.pruned[0].tier, PruneTier::Ungated);
    }

    #[test]
    fn negative_is_tier_one_alongside_ungated() {
        let dir = tempfile::tempdir().unwrap();
        write_log(dir.path(), "neg", 1000, WakeClass::Negative, 100, false);
        write_log(dir.path(), "pos", 1000, WakeClass::Positive, 50, false);
        let open = Mutex::new(HashSet::new());

        // Positive is older but negative (tier 1) is deleted first.
        let outcome = prune(&request(dir.path(), 1500, &open)).unwrap();
        assert_eq!(pruned_names(&outcome), vec!["neg"]);
    }

    #[test]
    fn pinned_logs_are_never_pruned() {
        let dir = tempfile::tempdir().unwrap();
        let pinned = write_log(
            dir.path(),
            "pinned_old",
            1000,
            WakeClass::Ungated,
            100,
            true,
        );
        write_log(
            dir.path(),
            "loose_new",
            1000,
            WakeClass::Ungated,
            200,
            false,
        );
        let open = Mutex::new(HashSet::new());

        // Cap forces one deletion; the older log is pinned, so the newer loose
        // log is taken instead and the pin survives.
        let outcome = prune(&request(dir.path(), 1500, &open)).unwrap();
        assert_eq!(pruned_names(&outcome), vec!["loose_new"]);
        assert!(pinned.exists());
        assert!(sidecar_path(&pinned).exists());
        assert!(outcome.halted.is_none());
    }

    #[test]
    fn only_pinned_over_cap_halts() {
        let dir = tempfile::tempdir().unwrap();
        write_log(dir.path(), "p1", 1000, WakeClass::Ungated, 100, true);
        write_log(dir.path(), "p2", 1000, WakeClass::Ungated, 200, true);
        let open = Mutex::new(HashSet::new());

        let outcome = prune(&request(dir.path(), 500, &open)).unwrap();
        assert!(outcome.pruned.is_empty());
        let halt = outcome.halted.expect("halted over cap with only pins");
        assert_eq!(halt.cap_bytes, 500);
        assert!(halt.remaining_bytes > 500);
    }

    #[test]
    fn missing_sidecar_classed_ungated_and_prunable() {
        let dir = tempfile::tempdir().unwrap();
        // A framelog with no sidecar — a crash leftover.
        let bare = dir.path().join("crash.framelog");
        std::fs::write(&bare, vec![0u8; 2000]).unwrap();
        let open = Mutex::new(HashSet::new());

        let outcome = prune(&request(dir.path(), 500, &open)).unwrap();
        assert_eq!(pruned_names(&outcome), vec!["crash"]);
        assert_eq!(outcome.pruned[0].tier, PruneTier::Ungated);
        assert_eq!(outcome.pruned[0].sidecar, None);
        assert!(!bare.exists());
    }

    #[test]
    fn open_logs_are_skipped_but_counted() {
        let dir = tempfile::tempdir().unwrap();
        let open_log = write_log(dir.path(), "open_old", 1000, WakeClass::Ungated, 100, false);
        write_log(
            dir.path(),
            "closed_new",
            1000,
            WakeClass::Ungated,
            200,
            false,
        );
        let open = Mutex::new(HashSet::new());
        open.lock().unwrap().insert(open_log.clone());

        // The open log is oldest but excluded; the closed one is pruned. Its
        // bytes still counted toward the total (the cap forced a deletion).
        let outcome = prune(&request(dir.path(), 1500, &open)).unwrap();
        assert_eq!(pruned_names(&outcome), vec!["closed_new"]);
        assert!(open_log.exists());
    }

    #[test]
    fn under_cap_prunes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        write_log(dir.path(), "a", 1000, WakeClass::Ungated, 100, false);
        let open = Mutex::new(HashSet::new());

        let outcome = prune(&request(dir.path(), 1_000_000, &open)).unwrap();
        assert!(outcome.pruned.is_empty());
        assert!(outcome.halted.is_none());
    }

    #[test]
    fn missing_store_dir_is_empty_not_error() {
        let dir = tempfile::tempdir().unwrap();
        let absent = dir.path().join("does-not-exist");
        let open = Mutex::new(HashSet::new());
        let outcome = prune(&request(&absent, 0, &open)).unwrap();
        assert_eq!(outcome, PruneOutcome::default());
    }

    #[test]
    fn only_open_over_cap_halts() {
        let dir = tempfile::tempdir().unwrap();
        // A single in-progress (open, unpinned) log alone exceeds the cap, with
        // nothing closed and nothing pinned: the pass prunes nothing and halts.
        let open_log = write_log(
            dir.path(),
            "open_only",
            2000,
            WakeClass::Ungated,
            100,
            false,
        );
        let open = Mutex::new(HashSet::new());
        open.lock().unwrap().insert(open_log.clone());

        let outcome = prune(&request(dir.path(), 500, &open)).unwrap();
        assert!(outcome.pruned.is_empty());
        assert!(open_log.exists());
        let halt = outcome
            .halted
            .expect("halted over cap with only an open log");
        assert_eq!(halt.cap_bytes, 500);
        assert!(halt.remaining_bytes > 500);
    }

    #[test]
    fn corrupt_sidecar_log_is_kept_not_pruned() {
        let dir = tempfile::tempdir().unwrap();
        // A clean prunable log, plus a log whose sidecar is present but does not
        // parse. The corrupt one's age_key is 0, so it sorts first and would be
        // the first deletion candidate if it were prunable.
        let clean = write_log(dir.path(), "clean", 1000, WakeClass::Ungated, 100, false);
        let corrupt = dir.path().join("corrupt.framelog");
        std::fs::write(&corrupt, vec![0u8; 1000]).unwrap();
        std::fs::write(sidecar_path(&corrupt), b"{ not valid json").unwrap();
        let open = Mutex::new(HashSet::new());

        // Cap 0 forces deleting every candidate; the corrupt-sidecar log is kept
        // (protective) while the clean log is still pruned. The store stays over
        // cap because the kept log's bytes remain, so the pass halts.
        let outcome = prune(&request(dir.path(), 0, &open)).unwrap();
        assert_eq!(pruned_names(&outcome), vec!["clean"]);
        assert!(!clean.exists());
        assert!(corrupt.exists());
        assert!(sidecar_path(&corrupt).exists());
        assert!(outcome.halted.is_some());
        // The kept corrupt log is reported so the caller can complain loudly.
        assert_eq!(outcome.kept_corrupt.len(), 1);
        assert!(outcome.kept_corrupt[0]
            .framelog
            .to_string_lossy()
            .contains("corrupt"));
    }

    #[test]
    fn on_disk_prunable_reflects_a_pin_created_after_inventory() {
        use crate::recorder::set_pinned;
        let dir = tempfile::tempdir().unwrap();
        // A sidecar-less framelog — inventory would record `sidecar: None`, the
        // most-prunable class.
        let framelog = dir.path().join("crash.framelog");
        std::fs::write(&framelog, vec![0u8; 1000]).unwrap();
        // With no sidecar it is prunable...
        assert!(on_disk_prunable(&framelog, PruneTier::Ungated));
        // ...but once an out-of-band `pin` mints a sidecar (the post-inventory
        // race), the live re-check must see the pin and spare the log — even
        // though the inventory snapshot had no sidecar to consult.
        set_pinned(&sidecar_path(&framelog)).unwrap();
        assert!(!on_disk_prunable(&framelog, PruneTier::Ungated));
    }

    #[test]
    fn on_disk_prunable_spares_a_log_upgraded_to_positive_after_inventory() {
        use crate::recorder::set_wake_class;
        let dir = tempfile::tempdir().unwrap();
        // A log inventoried as tier-1: a sidecar with one ungated segment (id 0).
        let framelog = write_log(dir.path(), "wake", 1000, WakeClass::Ungated, 100, false);
        // While draining tier-1, it is a legitimate candidate...
        assert!(on_disk_prunable(&framelog, PruneTier::Ungated));
        // ...but once an out-of-band wake verdict labels it positive (the
        // post-inventory race), the live re-check lifts it to tier-2 and it must
        // be spared while tier-1 is being drained — never deleted on the stale
        // tier-1 classification, which would drop a just-detected wake.
        set_wake_class(&sidecar_path(&framelog), 0, 0, WakeClass::Positive).unwrap();
        assert!(!on_disk_prunable(&framelog, PruneTier::Ungated));
        // Draining tier-2, the now-positive log is a candidate again.
        assert!(on_disk_prunable(&framelog, PruneTier::Positive));
    }

    #[test]
    fn on_disk_prunable_keeps_a_corrupt_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let framelog = dir.path().join("x.framelog");
        std::fs::write(&framelog, vec![0u8; 10]).unwrap();
        std::fs::write(sidecar_path(&framelog), b"{ not json").unwrap();
        // A sidecar that fails to parse fails safe toward retention.
        assert!(!on_disk_prunable(&framelog, PruneTier::Ungated));
    }

    #[test]
    fn failed_delete_is_recorded_and_pass_continues() {
        let dir = tempfile::tempdir().unwrap();
        // One deletable log and one undeletable candidate — a directory named
        // like a framelog, so `remove_file` fails on it. The undeletable one has
        // no sidecar (age 0), so it sorts first and is attempted first.
        let good = write_log(dir.path(), "good", 1000, WakeClass::Ungated, 100, false);
        let wedged = dir.path().join("wedged.framelog");
        std::fs::create_dir(&wedged).unwrap();
        let open = Mutex::new(HashSet::new());

        // Cap 0 forces deleting every candidate; the wedged entry fails but the
        // good log is still pruned and recorded — one bad file does not abort.
        let outcome = prune(&request(dir.path(), 0, &open)).unwrap();
        assert_eq!(pruned_names(&outcome), vec!["good"]);
        assert!(!good.exists());
        assert_eq!(outcome.failed.len(), 1);
        assert_eq!(outcome.failed[0].framelog, wedged);
        // The failed candidate is still on disk, not silently gone.
        assert!(wedged.exists());
    }

    // Log payload dwarfs the sidecar JSON, so quota arithmetic in these tests is
    // dominated by this size and stays robust to sidecar byte jitter.
    const BIG: usize = 10_000;

    fn pruned_pods(outcome: &PruneOutcome) -> Vec<Option<String>> {
        outcome.pruned.iter().map(|p| p.pod_id.clone()).collect()
    }

    #[test]
    fn pod_quota_drains_over_quota_bucket_under_global_cap() {
        let dir = tempfile::tempdir().unwrap();
        // Flooder holds three logs; victim holds one. Global cap is huge, so only
        // the per-pod quota can trigger a deletion.
        write_log_pod(
            dir.path(),
            "f1",
            BIG,
            WakeClass::Ungated,
            100,
            false,
            "flood",
        );
        write_log_pod(
            dir.path(),
            "f2",
            BIG,
            WakeClass::Ungated,
            200,
            false,
            "flood",
        );
        write_log_pod(
            dir.path(),
            "f3",
            BIG,
            WakeClass::Ungated,
            300,
            false,
            "flood",
        );
        let victim = write_log_pod(
            dir.path(),
            "v1",
            BIG,
            WakeClass::Ungated,
            50,
            false,
            "victim",
        );
        let open = Mutex::new(HashSet::new());

        // Quota ~1.5 logs: two oldest flood logs go; f3 keeps the bucket at quota.
        let outcome = prune(&request_q(dir.path(), 1_000_000, 15_000, &open)).unwrap();
        assert_eq!(pruned_names(&outcome), vec!["f1", "f2"]);
        assert!(outcome
            .pruned
            .iter()
            .all(|p| p.reason == PruneReason::PodQuota));
        assert!(outcome
            .pruned
            .iter()
            .all(|p| p.pod_id.as_deref() == Some("flood")));
        assert!(victim.exists());
        assert!(outcome.halted.is_none());
        assert!(outcome.over_quota.is_empty());
    }

    #[test]
    fn pod_quota_spares_older_victim_a_global_pass_would_evict() {
        let dir = tempfile::tempdir().unwrap();
        // The victim's log is OLDER than the flooder's, so a global oldest-first
        // pass would evict the victim first. Phase 1 must drain the flooder and
        // leave the victim untouched.
        let victim = write_log_pod(
            dir.path(),
            "v",
            BIG,
            WakeClass::Ungated,
            10,
            false,
            "victim",
        );
        write_log_pod(
            dir.path(),
            "f1",
            BIG,
            WakeClass::Ungated,
            100,
            false,
            "flood",
        );
        write_log_pod(
            dir.path(),
            "f2",
            BIG,
            WakeClass::Ungated,
            200,
            false,
            "flood",
        );
        write_log_pod(
            dir.path(),
            "f3",
            BIG,
            WakeClass::Ungated,
            300,
            false,
            "flood",
        );
        let open = Mutex::new(HashSet::new());

        // Global cap would force ~one deletion; without quotas that would be the
        // victim. With a quota it is the flooder's oldest logs.
        let outcome = prune(&request_q(dir.path(), 35_000, 15_000, &open)).unwrap();
        assert!(victim.exists());
        assert!(outcome
            .pruned
            .iter()
            .all(|p| p.pod_id.as_deref() == Some("flood")));
        assert!(outcome.halted.is_none());
    }

    #[test]
    fn quota_eviction_follows_tier_order() {
        let dir = tempfile::tempdir().unwrap();
        // One pod, over quota by ~one log: its ungated log dies before its
        // positive log even though the positive is older.
        write_log_pod(
            dir.path(),
            "pos_old",
            BIG,
            WakeClass::Positive,
            100,
            false,
            "p",
        );
        write_log_pod(
            dir.path(),
            "ung_new",
            BIG,
            WakeClass::Ungated,
            200,
            false,
            "p",
        );
        let open = Mutex::new(HashSet::new());

        let outcome = prune(&request_q(dir.path(), 1_000_000, 15_000, &open)).unwrap();
        assert_eq!(pruned_names(&outcome), vec!["ung_new"]);
        assert_eq!(outcome.pruned[0].reason, PruneReason::PodQuota);
    }

    #[test]
    fn unattributed_bucket_shares_quota_and_reports_null_pod() {
        let dir = tempfile::tempdir().unwrap();
        // A sidecar-less crash leftover and an UNKNOWN_POD sidecar log share the
        // one unattributed bucket. Jointly over quota, the oldest is evicted.
        let bare = dir.path().join("crash.framelog");
        std::fs::write(&bare, vec![0u8; BIG]).unwrap();
        write_log_pod(
            dir.path(),
            "unk",
            BIG,
            WakeClass::Ungated,
            100,
            false,
            UNKNOWN_POD,
        );
        let open = Mutex::new(HashSet::new());

        let outcome = prune(&request_q(dir.path(), 1_000_000, 15_000, &open)).unwrap();
        // Sidecar-less log has age 0, sorts first, and is the one evicted.
        assert_eq!(pruned_names(&outcome), vec!["crash"]);
        assert_eq!(pruned_pods(&outcome), vec![None]);
        assert_eq!(outcome.pruned[0].reason, PruneReason::PodQuota);
    }

    #[test]
    fn corrupt_log_holds_bucket_over_quota_and_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        // Both fall in the unattributed bucket (corrupt sidecar -> unattributed).
        // The clean sidecar-less log is drained; the corrupt one is kept and its
        // bytes keep the bucket over quota, which is reported not force-drained.
        let clean = dir.path().join("clean.framelog");
        std::fs::write(&clean, vec![0u8; BIG]).unwrap();
        let corrupt = dir.path().join("corrupt.framelog");
        std::fs::write(&corrupt, vec![0u8; 2 * BIG]).unwrap();
        std::fs::write(sidecar_path(&corrupt), b"{ not valid json").unwrap();
        let open = Mutex::new(HashSet::new());

        let outcome = prune(&request_q(dir.path(), 1_000_000, 15_000, &open)).unwrap();
        assert_eq!(pruned_names(&outcome), vec!["clean"]);
        assert!(corrupt.exists());
        assert_eq!(outcome.kept_corrupt.len(), 1);
        assert_eq!(outcome.over_quota.len(), 1);
        assert_eq!(outcome.over_quota[0].pod_id, None);
        assert!(outcome.over_quota[0].remaining_bytes > 15_000);
        // A per-pod overflow is not a global halt.
        assert!(outcome.halted.is_none());
    }

    #[test]
    fn pinned_residue_reports_over_quota_without_halting() {
        let dir = tempfile::tempdir().unwrap();
        // Pinned log alone exceeds quota; the loose log is drained but the bucket
        // stays over quota on the pin. Reported via over_quota, no global halt.
        let pinned = write_log_pod(
            dir.path(),
            "pin",
            2 * BIG,
            WakeClass::Ungated,
            100,
            true,
            "p",
        );
        write_log_pod(
            dir.path(),
            "loose",
            BIG,
            WakeClass::Ungated,
            200,
            false,
            "p",
        );
        let open = Mutex::new(HashSet::new());

        let outcome = prune(&request_q(dir.path(), 1_000_000, 15_000, &open)).unwrap();
        assert_eq!(pruned_names(&outcome), vec!["loose"]);
        assert!(pinned.exists());
        assert_eq!(outcome.over_quota.len(), 1);
        assert_eq!(outcome.over_quota[0].pod_id.as_deref(), Some("p"));
        assert!(outcome.halted.is_none());
    }

    #[test]
    fn under_quota_over_global_cap_uses_global_reason() {
        let dir = tempfile::tempdir().unwrap();
        // Two pods, each under quota, but the store is over the global cap: pure
        // phase-2 behavior, oldest-first, tagged GlobalCap.
        write_log_pod(
            dir.path(),
            "a",
            BIG,
            WakeClass::Ungated,
            100,
            false,
            "pod-a",
        );
        write_log_pod(
            dir.path(),
            "b",
            BIG,
            WakeClass::Ungated,
            200,
            false,
            "pod-b",
        );
        let open = Mutex::new(HashSet::new());

        let outcome = prune(&request_q(dir.path(), 15_000, 100_000, &open)).unwrap();
        assert_eq!(pruned_names(&outcome), vec!["a"]);
        assert_eq!(outcome.pruned[0].reason, PruneReason::GlobalCap);
        assert!(outcome.over_quota.is_empty());
    }

    #[test]
    fn under_both_limits_prunes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        write_log_pod(
            dir.path(),
            "a",
            BIG,
            WakeClass::Ungated,
            100,
            false,
            "pod-a",
        );
        let open = Mutex::new(HashSet::new());

        let outcome = prune(&request_q(dir.path(), 1_000_000, 1_000_000, &open)).unwrap();
        assert!(outcome.pruned.is_empty());
        assert!(outcome.over_quota.is_empty());
        assert!(outcome.halted.is_none());
    }

    #[test]
    fn both_phases_delete_in_one_pass_with_mixed_reasons() {
        let dir = tempfile::tempdir().unwrap();
        // Flooder over quota, plus two real pods that together keep the store over
        // the global cap even after the flooder is drained to quota. One pass must
        // run BOTH phases: phase 1 drains the flooder (PodQuota), phase 2 then
        // evicts a real pod's log to reach the global cap (GlobalCap). Guards the
        // `total -= bytes` handoff — phase 2 must start from the post-phase-1 total.
        write_log_pod(
            dir.path(),
            "f1",
            BIG,
            WakeClass::Ungated,
            100,
            false,
            "flood",
        );
        write_log_pod(
            dir.path(),
            "f2",
            BIG,
            WakeClass::Ungated,
            200,
            false,
            "flood",
        );
        write_log_pod(
            dir.path(),
            "f3",
            BIG,
            WakeClass::Ungated,
            300,
            false,
            "flood",
        );
        // pod-a's log is the oldest overall, so once the flooder is at quota the
        // global pass takes it first.
        write_log_pod(dir.path(), "a", BIG, WakeClass::Ungated, 50, false, "pod-a");
        write_log_pod(
            dir.path(),
            "b",
            BIG,
            WakeClass::Ungated,
            400,
            false,
            "pod-b",
        );
        let open = Mutex::new(HashSet::new());

        // Quota ~1.5 logs drains the flooder to one log (f3). Global cap ~2.5 logs
        // is still exceeded by {f3, a, b}, so phase 2 evicts the oldest survivor.
        let outcome = prune(&request_q(dir.path(), 25_000, 15_000, &open)).unwrap();
        assert_eq!(pruned_names(&outcome), vec!["f1", "f2", "a"]);
        assert_eq!(
            outcome.pruned.iter().map(|p| p.reason).collect::<Vec<_>>(),
            vec![
                PruneReason::PodQuota,
                PruneReason::PodQuota,
                PruneReason::GlobalCap
            ]
        );
        // Flooder drained (via quota) before any global-cap eviction.
        assert_eq!(pruned_pods(&outcome)[0].as_deref(), Some("flood"));
        assert_eq!(pruned_pods(&outcome)[1].as_deref(), Some("flood"));
        assert_eq!(pruned_pods(&outcome)[2].as_deref(), Some("pod-a"));
        // Store is back under the global cap: no halt.
        assert!(outcome.halted.is_none());
    }

    #[test]
    fn phase1_failed_delete_is_not_retried_by_phase2() {
        let dir = tempfile::tempdir().unwrap();
        // An over-quota bucket whose oldest candidate is undeletable (a directory
        // named like a framelog). Phase 1 attempts it, records the failure, and
        // adds it to `attempted`; phase 2 must skip it, not retry — a retry would
        // double-record the failure. The loose log is a legitimate phase-1 victim.
        let wedged = dir.path().join("wedged.framelog");
        std::fs::create_dir(&wedged).unwrap();
        let mut sidecar = Sidecar::new("p");
        sidecar.push(SidecarSegment {
            segment_id: 0,
            part: 0,
            wake: WakeClass::Ungated,
            start_epoch_us: 50,
            end_epoch_us: 51,
            end_cause: SegmentEndCause::VadRelease,
            truncated: false,
            resumed: false,
            gap_count: 0,
            samples: 16_000,
        });
        sidecar.write_atomic(&sidecar_path(&wedged)).unwrap();
        let loose = write_log_pod(
            dir.path(),
            "loose",
            BIG,
            WakeClass::Ungated,
            100,
            false,
            "p",
        );
        let open = Mutex::new(HashSet::new());

        // Quota forces draining the bucket; global cap 0 makes phase 2 want to
        // delete everything left, but the only survivor is the wedged entry, which
        // is in `attempted` and must be skipped.
        let outcome = prune(&request_q(dir.path(), 0, 5_000, &open)).unwrap();
        // The loose log was pruned; the wedged entry failed exactly once.
        assert_eq!(pruned_names(&outcome), vec!["loose"]);
        assert!(!loose.exists());
        assert_eq!(outcome.failed.len(), 1);
        assert_eq!(outcome.failed[0].framelog, wedged);
        // Still on disk (a directory `remove_file` cannot unlink), not retried.
        assert!(wedged.exists());
    }
}
