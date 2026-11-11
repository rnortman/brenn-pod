//! Build identity capture — git commit hash + dirty flag, stamped at build time.
//!
//! Both the device firmware and the host harness depend on this crate and call
//! `build_id()` to obtain their compiled-in identity. Because both build from one
//! tree in one `make hil-test` invocation, the two stamps match on a clean rebuild.
//!
//! A build with no checkout of this repository around it (a fetched revision)
//! takes its commit from `BRENN_POD_BUILD_COMMIT` instead; [`source`] says which.
//! Inside a checkout of this repository git always vouches, and a declared commit
//! that disagrees with it fails the build.
//!
//! The `build.rs` script emits `cargo:rerun-if-changed` for the git ref files (HEAD,
//! the current branch's ref, and `packed-refs`) so the stamp is re-evaluated whenever
//! HEAD advances and can never go stale.

use device_protocol::BuildId;

#[cfg(test)]
#[path = "../decide.rs"]
mod decide;

/// Returns the build identity stamped into this binary at build time.
///
/// `commit` is the git SHA-1 hash of HEAD at the time this binary was built.
/// `dirty` is true if the working tree had uncommitted changes to tracked files.
pub fn build_id() -> BuildId {
    BuildId::truncating(env!("HIL_BUILD_COMMIT"), env!("HIL_BUILD_DIRTY") == "true")
}

/// Who vouches for the commit in [`build_id`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    /// Git, from the checkout of this repository the build ran in. The dirty flag
    /// is git's.
    Git,
    /// The builder, through `BRENN_POD_BUILD_COMMIT`, for a build with no checkout
    /// of this repository around it. The dirty flag is not asserted and reads
    /// `false`.
    Declared,
}

/// Who vouches for the commit stamped into this binary.
pub fn source() -> Source {
    match env!("HIL_BUILD_SOURCE") {
        "git" => Source::Git,
        "declared" => Source::Declared,
        other => panic!("build-id: unknown build source {other:?} stamped by build.rs"),
    }
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

    /// The stamp against the commit the build was given in
    /// `BRENN_POD_BUILD_COMMIT`, read at compile time like the stamp itself.
    #[test]
    fn the_stamp_agrees_with_a_declared_commit() {
        let declared = option_env!("BRENN_POD_BUILD_COMMIT").map(str::to_ascii_lowercase);
        let id = build_id();
        match (source(), declared) {
            (Source::Declared, Some(declared)) => {
                assert_eq!(
                    id.commit.as_str(),
                    declared,
                    "a declared build carries the declared commit"
                );
                assert!(!id.dirty, "a declared build asserts no dirty flag");
            }
            (Source::Declared, None) => panic!("source is declared but nothing was declared"),
            (Source::Git, Some(declared)) => {
                assert_eq!(
                    id.commit.as_str(),
                    declared,
                    "git and the declaration agree, or the build fails"
                )
            }
            (Source::Git, None) => {}
        }
    }

    /// `make check-host` builds this crate with git's view hidden and not, and
    /// says which source each build must have taken in
    /// `BUILD_ID_EXPECT_SOURCE`. Unset, the test has nothing to compare.
    #[test]
    fn the_source_is_the_one_the_build_expected() {
        if let Some(expected) = option_env!("BUILD_ID_EXPECT_SOURCE") {
            // `source()` panics on a stamp it does not know.
            source();
            assert_eq!(env!("HIL_BUILD_SOURCE"), expected);
        }
    }

    mod decision {
        use std::ffi::OsStr;

        use crate::decide::{OWN_PREFIX, Stamp, decide, parse_declared};

        const HEAD: &str = "13a513d6b38a14194ecb2e1853a7a55d9e7f7dab";
        const OTHER: &str = "0000000000000000000000000000000000000000";

        fn own() -> Result<String, String> {
            Ok(OWN_PREFIX.to_owned())
        }

        fn git_at_head() -> (String, bool) {
            (HEAD.to_owned(), true)
        }

        fn git_not_asked() -> (String, bool) {
            panic!("git's commit was asked for where git does not vouch")
        }

        fn stamp(commit: &str, dirty: bool, source: &'static str) -> Stamp {
            Stamp {
                commit: commit.to_owned(),
                dirty,
                source,
            }
        }

        #[test]
        fn a_declaration_is_forty_hex_digits_lowercased() {
            assert_eq!(parse_declared(None), Ok(None));
            assert_eq!(
                parse_declared(Some(OsStr::new(&HEAD.to_ascii_uppercase()))),
                Ok(Some(HEAD.to_owned()))
            );
            for bad in [
                "abc",
                &HEAD[..39],
                format!("{HEAD}0").as_str(),
                HEAD.replace('a', "g").as_str(),
            ] {
                let err = parse_declared(Some(OsStr::new(bad))).unwrap_err();
                assert!(
                    err.contains("must be a commit as 40 hex digits"),
                    "{bad}: {err}"
                );
            }
        }

        #[test]
        fn at_its_own_place_git_vouches_with_its_dirty_flag() {
            assert_eq!(
                decide(own(), None, git_at_head),
                Ok(stamp(HEAD, true, "git"))
            );
            assert_eq!(
                decide(own(), Some(HEAD.to_owned()), git_at_head),
                Ok(stamp(HEAD, true, "git"))
            );
        }

        #[test]
        fn a_declaration_that_disagrees_with_the_checkout_fails() {
            let err = decide(own(), Some(OTHER.to_owned()), git_at_head).unwrap_err();
            assert!(
                err.contains("A declared commit cannot override a checkout"),
                "{err}"
            );
            assert!(err.contains(HEAD) && err.contains(OTHER), "{err}");
        }

        #[test]
        fn inside_another_repository_git_does_not_vouch() {
            let vendored = || Ok("vendor/build-id/".to_owned());
            let err = decide(vendored(), None, git_not_asked).unwrap_err();
            assert!(err.contains("not at `firmware/crates/build-id/`"), "{err}");
            assert!(err.contains("`vendor/build-id/`"), "{err}");
            assert_eq!(
                decide(vendored(), Some(OTHER.to_owned()), git_not_asked),
                Ok(stamp(OTHER, false, "declared"))
            );
            // The top of a work tree is another place too.
            assert!(decide(Ok(String::new()), None, git_not_asked).is_err());
        }

        #[test]
        fn where_git_cannot_answer_only_a_declaration_stamps() {
            let none = || Err("no repository".to_owned());
            let err = decide(none(), None, git_not_asked).unwrap_err();
            assert!(
                err.contains("no repository") && err.contains("declare the commit"),
                "{err}"
            );
            assert_eq!(
                decide(none(), Some(HEAD.to_owned()), git_not_asked),
                Ok(stamp(HEAD, false, "declared"))
            );
        }
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
