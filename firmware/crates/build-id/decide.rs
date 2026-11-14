// Which commit build.rs stamps, and who vouches for it, as plain logic: build.rs
// feeds it what git and the environment said, and the crate's tests feed it every
// case. Included by both through a module path, so the two cannot drift apart.

use std::ffi::OsStr;
use std::path::{Component, Path};

/// The variable a builder sets to declare the commit of a build that has no
/// checkout of this repository around it.
pub const DECLARED_VAR: &str = "BRENN_POD_BUILD_COMMIT";

/// Where this crate sits in a checkout of its own repository, as
/// `git rev-parse --show-prefix` prints it from the crate's directory.
pub const OWN_PREFIX: &str = "firmware/crates/build-id/";

/// The top of the checkout of this repository the crate would sit in, from the
/// crate's own directory: the directory above `firmware/crates/build-id`, or
/// `None` when the crate's directory does not end in those three components and
/// so no checkout of this repository can be around it. Reads nothing.
pub fn checkout_top(manifest_dir: &Path) -> Option<&Path> {
    let mut dir = manifest_dir;
    for expected in OWN_PREFIX.trim_end_matches('/').rsplit('/') {
        match dir.components().next_back() {
            Some(Component::Normal(name)) if name == expected => {}
            _ => return None,
        }
        dir = dir.parent()?;
    }
    Some(dir)
}

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

/// The stamp, from whether a checkout of this repository can be around the crate
/// (`checkout_top` found a `.git` entry where the checkout's would be), git's
/// answer to `--show-prefix` (`Err` says why git could not answer), the declared
/// commit, and `git_commit_and_dirty`. `Err` is the message the build fails with.
///
/// With no checkout possible, git is not asked anything: neither closure is
/// called. Otherwise git vouches only when this crate sits at its own place in a
/// checkout of this repository; a crate vendored inside some other repository
/// would otherwise be stamped with that repository's commit.
pub fn decide(
    in_checkout: bool,
    prefix: impl FnOnce() -> Result<String, String>,
    declared: Option<String>,
    git_commit_and_dirty: impl FnOnce() -> (String, bool),
) -> Result<Stamp, String> {
    if !in_checkout {
        return match declared {
            Some(declared) => Ok(declared_stamp(declared)),
            None => Err(format!(
                "build-id: no checkout of this repository is around this crate, so \
                 nothing can vouch for its commit. Declare the commit in {DECLARED_VAR}."
            )),
        };
    }
    let refusal = match prefix() {
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
        (Some(declared), _) => Ok(declared_stamp(declared)),
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

/// A declared commit's stamp. The dirty flag is not asserted: nothing here can
/// see the tree it was built from.
fn declared_stamp(commit: String) -> Stamp {
    Stamp {
        commit,
        dirty: false,
        source: "declared",
    }
}
