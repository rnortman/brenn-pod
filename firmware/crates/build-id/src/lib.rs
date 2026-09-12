//! Build identity capture — git commit hash + dirty flag, stamped at build time.
//!
//! Both the device firmware and the host harness depend on this crate and call
//! `build_id()` to obtain their compiled-in identity. Because both build from one
//! tree in one `make hil-test` invocation, the two stamps match on a clean rebuild.
//!
//! The `build.rs` script emits `cargo:rerun-if-changed` for the git ref files (HEAD,
//! the current branch's ref, and `packed-refs`) so the stamp is re-evaluated whenever
//! HEAD advances and can never go stale.

use device_protocol::BuildId;

/// Returns the build identity stamped into this binary at build time.
///
/// `commit` is the git SHA-1 hash of HEAD at the time this binary was built.
/// `dirty` is true if the working tree had uncommitted changes to tracked files.
pub fn build_id() -> BuildId {
    BuildId::truncating(env!("HIL_BUILD_COMMIT"), env!("HIL_BUILD_DIRTY") == "true")
}

/// A build identity as a startup line spells it: the first twelve hex digits of
/// the commit, and `+dirty` when that tree carried uncommitted changes to tracked
/// files.
///
/// Twelve because that is how a revision is quoted by hand and in a pin. Here
/// rather than in each binary that prints it, so a line from one device can be
/// compared with a line from another by eye — the whole point of putting the
/// stamp on the line. A commit shorter than twelve (a shallow clone) is printed
/// whole: the truncation is a maximum, not a width.
pub fn stamp(id: &BuildId) -> String {
    let commit: String = id.commit.chars().take(12).collect();
    let dirty = if id.dirty { "+dirty" } else { "" };
    format!("{commit}{dirty}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_id_returns_non_empty_commit() {
        let id = build_id();
        assert!(
            !id.commit.is_empty(),
            "build_id commit must not be empty; got: {:?}",
            id.commit
        );
    }

    #[test]
    fn build_id_is_deterministic() {
        // Two calls in the same process must return the same value (env! is compile-time).
        let a = build_id();
        let b = build_id();
        assert_eq!(a.commit, b.commit, "build_id commit must be deterministic");
        assert_eq!(a.dirty, b.dirty, "build_id dirty must be deterministic");
    }

    #[test]
    fn the_stamp_carries_twelve_hex_of_the_commit() {
        let id = BuildId::truncating("13a513d6b38a14194ecb2e1853a7a55d9e7f7dab", false);
        assert_eq!(stamp(&id), "13a513d6b38a");
    }

    #[test]
    fn a_dirty_tree_says_so_in_the_stamp() {
        let id = BuildId::truncating("13a513d6b38a14194ecb2e1853a7a55d9e7f7dab", true);
        assert_eq!(stamp(&id), "13a513d6b38a+dirty");
    }

    #[test]
    fn a_commit_shorter_than_twelve_is_stamped_whole() {
        let id = BuildId::truncating("13a513d", false);
        assert_eq!(stamp(&id), "13a513d");
    }

    #[test]
    fn build_id_commit_max_40_chars() {
        let id = build_id();
        assert!(
            id.commit.len() <= 40,
            "commit must be at most 40 chars; got len={}",
            id.commit.len()
        );
    }
}
