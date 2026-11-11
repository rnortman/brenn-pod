// Which commit build.rs stamps, and who vouches for it, as plain logic: build.rs
// feeds it what git and the environment said, and the crate's tests feed it every
// case. Included by both through a module path, so the two cannot drift apart.

use std::ffi::OsStr;

/// The variable a builder sets to declare the commit of a build that has no
/// checkout of this repository around it.
pub const DECLARED_VAR: &str = "BRENN_POD_BUILD_COMMIT";

/// Where this crate sits in a checkout of its own repository, as
/// `git rev-parse --show-prefix` prints it from the crate's directory.
pub const OWN_PREFIX: &str = "firmware/crates/build-id/";

/// What build.rs stamps into the crate.
#[derive(Debug, PartialEq, Eq)]
pub struct Stamp {
    pub commit: String,
    pub dirty: bool,
    /// `git` or `declared`.
    pub source: &'static str,
}

/// The declared commit, from the variable's value (`None` when unset), lowercased.
/// Anything but forty hex digits is refused outright, whether or not git would
/// have vouched.
pub fn parse_declared(value: Option<&OsStr>) -> Result<Option<String>, String> {
    let Some(raw) = value else {
        return Ok(None);
    };
    let Some(value) = raw.to_str() else {
        return Err(format!("build-id: {DECLARED_VAR} is not UTF-8: {raw:?}"));
    };
    if value.len() != 40 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(format!(
            "build-id: {DECLARED_VAR} must be a commit as 40 hex digits; got {value:?}"
        ));
    }
    Ok(Some(value.to_ascii_lowercase()))
}

/// The stamp, from git's answer to `--show-prefix` (`Err` says why git could not
/// answer), the declared commit, and `git_commit_and_dirty`, which is called only
/// when git vouches. `Err` is the message the build fails with.
///
/// Git vouches only when this crate sits at its own place in a checkout of this
/// repository. A crate vendored inside some other repository would otherwise be
/// stamped with that repository's commit.
pub fn decide(
    prefix: Result<String, String>,
    declared: Option<String>,
    git_commit_and_dirty: impl FnOnce() -> (String, bool),
) -> Result<Stamp, String> {
    let refusal = match prefix {
        Ok(prefix) if prefix == OWN_PREFIX => {
            let (commit, dirty) = git_commit_and_dirty();
            if let Some(declared) = &declared
                && *declared != commit
            {
                return Err(format!(
                    "build-id: {DECLARED_VAR} is {declared}, but this checkout is at \
                     {commit}. A declared commit cannot override a checkout; unset \
                     {DECLARED_VAR}."
                ));
            }
            return Ok(Stamp {
                commit,
                dirty,
                source: "git",
            });
        }
        refusal => refusal,
    };
    match (declared, refusal) {
        // The dirty flag is not asserted for a declared commit: nothing here can
        // see the tree it was built from.
        (Some(declared), _) => Ok(Stamp {
            commit: declared,
            dirty: false,
            source: "declared",
        }),
        // Fail rather than stamp a sentinel ("unknown"): a silent sentinel would let
        // the build-ID gate pass falsely when git is unavailable (e.g. Docker, CI
        // without git installed), defeating the core HIL invariant.
        (None, Ok(prefix)) => Err(format!(
            "build-id: git places this crate at `{prefix}` in its repository, not at \
             `{OWN_PREFIX}`, so that repository's commit is not this crate's. Declare \
             the commit in {DECLARED_VAR}."
        )),
        (None, Err(why)) => Err(format!(
            "build-id: {why}. Build from a checkout of this repository with git on \
             PATH, or declare the commit in {DECLARED_VAR}."
        )),
    }
}
