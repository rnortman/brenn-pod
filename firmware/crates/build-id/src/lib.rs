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
    let commit_str = env!("HIL_BUILD_COMMIT");
    let dirty_str = env!("HIL_BUILD_DIRTY");

    let dirty = dirty_str == "true";

    // Truncate to heapless::String<40> capacity (40 hex chars = full SHA-1).
    // A shallow clone or "unknown" will be shorter; truncation is safe.
    let mut commit = heapless::String::<40>::new();
    for ch in commit_str.chars().take(40) {
        // Infallible: we take at most 40 chars into a capacity-40 string.
        // Git SHA-1 hashes are hex (ASCII); this cannot overflow in practice,
        // but assert the invariant rather than silently discarding a push error.
        commit
            .push(ch)
            .expect("commit push failed — non-ASCII or too-long commit hash?");
    }

    BuildId { commit, dirty }
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
    fn build_id_commit_max_40_chars() {
        let id = build_id();
        assert!(
            id.commit.len() <= 40,
            "commit must be at most 40 chars; got len={}",
            id.commit.len()
        );
    }
}
