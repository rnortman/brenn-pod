//! A reference to one segment inside a frame log, plus opening it directly
//! against a record store. Refs are store-root-relative so they survive store
//! relocation and serialize compactly; an absent frame log resolves to
//! `Pruned` (explicit, never a bare I/O error), because the pruner may
//! reclaim a log after a ref is minted.
//!
//! `SegmentRef` deserializes from sidecars/JSONL and (later) the Brenn bus, so
//! `log` is untrusted: `resolve_open` accepts only a single normal path
//! component and rejects an absolute or traversing `log` before any join,
//! distinctly from `Pruned` — a systematic bad-ref bug should be visible, not
//! read as routine pruning.
//!
//! Every part of a cap-rolled span shares one per-connection append log (the
//! recorder never writes per-part files), so `part` does not affect which
//! file `resolve_open` opens — it is provenance only, addressed within the
//! log by absolute sample index at the readback layer.

use std::io;
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};

use crate::framelog::{FrameLogError, FrameLogReader};

/// A segment inside a frame log, addressed by the log's store-root-relative
/// file name and the per-connection segment id.
///
/// A host-capped span rolls over into successive parts that share one
/// `segment_id`; `part` disambiguates them (`part: 0` is the first part / a wire
/// segment). Old refs that predate parts deserialize as `part: 0`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentRef {
    /// Frame-log file name, relative to the record store root.
    pub log: String,
    pub segment_id: u32,
    /// Cap-rollover part index within `segment_id`; `0` for the first part.
    #[serde(default)]
    pub part: u16,
}

/// The outcome of opening a ref's frame log. `Found` carries an open,
/// header-validated reader — there is no stat-then-open window for the
/// pruner to race.
pub enum Resolved {
    Found(FrameLogReader),
    /// The log is absent (open returned `NotFound`): the pruner reclaimed it,
    /// or it never existed. Routine, not a fault.
    Pruned,
}

/// Errors from `resolve_open`.
#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    /// `ref.log` is not a single normal path component. Untrusted input is
    /// rejected before any join — distinct from `Pruned` so a systematic
    /// bad-ref bug is visible, and distinct from `Fault` so it never reads as
    /// a store outage.
    #[error("invalid segment ref log name: {log:?}")]
    InvalidRef { log: String },
    /// The store is faulting or the log is unreadable: any open error other
    /// than `NotFound` (`EACCES`, `EIO`, a dropped mount) or a header failure
    /// (`BadMagic` / `UnsupportedVersion` / `BadMeta`). Never folded into
    /// `Pruned`.
    #[error("frame log open faulted: {0}")]
    Fault(#[source] FrameLogError),
}

/// Open a ref's frame log directly against a store root. `log` must be a
/// single normal path component: an absolute or traversing value escapes the
/// store, so it is rejected as `InvalidRef` and never joined. Otherwise the
/// file name is joined onto `store_root` and opened; `NotFound` yields
/// `Pruned` (the pruner may have reclaimed the log since the ref was minted);
/// any other open or header error yields `Fault`, so a genuine store outage
/// is never misreported as routine pruning.
///
/// `r.part` does not affect which file opens: every part of a cap-rolled span
/// shares one log.
pub fn resolve_open(store_root: &Path, r: &SegmentRef) -> Result<Resolved, ResolveError> {
    if !is_single_normal_component(&r.log) {
        return Err(ResolveError::InvalidRef { log: r.log.clone() });
    }
    let path = store_root.join(&r.log);
    match FrameLogReader::open(&path) {
        Ok(reader) => Ok(Resolved::Found(reader)),
        Err(FrameLogError::Io(e)) if e.kind() == io::ErrorKind::NotFound => Ok(Resolved::Pruned),
        Err(e) => Err(ResolveError::Fault(e)),
    }
}

/// True iff `log` is exactly one `Component::Normal` — no root, prefix, `.`,
/// `..`, empty, or multi-segment path.
fn is_single_normal_component(log: &str) -> bool {
    let mut components = Path::new(log).components();
    matches!(
        (components.next(), components.next()),
        (Some(Component::Normal(_)), None)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::HostMicros;
    use crate::framelog::{FrameLogWriter, LogItem, LogMeta};
    use std::fs;

    fn meta() -> LogMeta {
        LogMeta {
            build_id: "test".to_string(),
            created_epoch_us: HostMicros(1_700_000_000_000_000),
            conn_seq: 1,
            rolled_from: None,
        }
    }

    #[test]
    fn resolve_open_found_yields_planted_records() {
        let dir = tempfile::tempdir().unwrap();
        let log_name = "pod-a1b2c3_2026-07-03T00-00-00-000_1.framelog";
        let path = dir.path().join(log_name);
        let mut w = FrameLogWriter::create(&path, meta()).unwrap();
        w.append(HostMicros(10), &[1, 2, 3]).unwrap();
        w.finish().unwrap();

        let r = SegmentRef {
            log: log_name.to_string(),
            segment_id: 7,
            part: 0,
        };
        match resolve_open(dir.path(), &r).unwrap() {
            Resolved::Found(reader) => {
                let items: Vec<_> = reader.map(|i| i.unwrap()).collect();
                assert_eq!(
                    items,
                    vec![LogItem::Record {
                        host_rx: HostMicros(10),
                        payload: vec![1, 2, 3],
                    }]
                );
            }
            Resolved::Pruned => panic!("expected Found"),
        }
    }

    #[test]
    fn resolve_open_pruned_when_log_absent() {
        let dir = tempfile::tempdir().unwrap();
        let r = SegmentRef {
            log: "gone.framelog".to_string(),
            segment_id: 3,
            part: 0,
        };
        assert!(matches!(resolve_open(dir.path(), &r), Ok(Resolved::Pruned)));
    }

    #[test]
    fn resolve_open_rejects_traversing_and_absolute_logs() {
        let dir = tempfile::tempdir().unwrap();
        // Plant a file the traversal payload would reach, to prove the reject
        // happens before any join — not merely because the target is absent.
        fs::write(dir.path().join("secret"), b"RSFL").unwrap();

        for log in [
            "../secret",
            "sub/child.framelog",
            "/etc/passwd",
            "..",
            ".",
            "",
        ] {
            let r = SegmentRef {
                log: log.to_string(),
                segment_id: 1,
                part: 0,
            };
            match resolve_open(dir.path(), &r) {
                Err(ResolveError::InvalidRef { log: got }) => assert_eq!(got, log),
                Ok(_) => panic!("log {log:?} must be rejected as InvalidRef, resolved instead"),
                Err(e) => panic!("log {log:?} must be InvalidRef, got {e:?}"),
            }
        }
    }

    #[test]
    fn resolve_open_bad_magic_is_fault() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.framelog");
        fs::write(&path, b"XXXX\x01\x00\x00\x00").unwrap();
        let r = SegmentRef {
            log: "bad.framelog".to_string(),
            segment_id: 1,
            part: 0,
        };
        assert!(matches!(
            resolve_open(dir.path(), &r),
            Err(ResolveError::Fault(FrameLogError::BadMagic))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn resolve_open_unreadable_file_is_fault_not_pruned() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("locked.framelog");
        fs::write(&path, b"RSFL").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();

        let r = SegmentRef {
            log: "locked.framelog".to_string(),
            segment_id: 1,
            part: 0,
        };
        let result = resolve_open(dir.path(), &r);
        // Restore permissions so the tempdir can clean itself up.
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();

        match result {
            Err(ResolveError::Fault(FrameLogError::Io(e))) => {
                assert_eq!(e.kind(), io::ErrorKind::PermissionDenied);
            }
            Ok(_) => panic!("expected Fault, got Ok (test running as root?)"),
            Err(e) => panic!("expected Fault(Io(PermissionDenied)), got {e:?}"),
        }
    }

    #[test]
    fn deserializes_legacy_ref_without_part_as_zero() {
        // Sidecars/refs minted before parts existed carry no `part` field.
        let json = r#"{"log":"pod-a_1.framelog","segment_id":7}"#;
        let r: SegmentRef = serde_json::from_str(json).unwrap();
        assert_eq!(
            r,
            SegmentRef {
                log: "pod-a_1.framelog".to_string(),
                segment_id: 7,
                part: 0,
            }
        );
    }

    #[test]
    fn part_roundtrips_through_serde() {
        let r = SegmentRef {
            log: "pod-a_1.framelog".to_string(),
            segment_id: 7,
            part: 3,
        };
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(serde_json::from_str::<SegmentRef>(&json).unwrap(), r);
    }
}
