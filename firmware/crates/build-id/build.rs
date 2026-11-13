// build.rs for build-id crate.
//
// Stamps the current git commit + dirty flag into the binary, or, where git cannot
// place this crate in a checkout of its own repository, the commit the builder
// declares in BRENN_POD_BUILD_COMMIT. To keep the stamp from
// going stale, we emit `cargo:rerun-if-changed` for the git ref files that move when
// HEAD advances (see `emit_git_ref_rerun_triggers`). Without these, cargo would re-run
// the script only when files in this crate's own directory change — so HEAD could
// advance, downstream sources recompile, yet the cached commit stamp be silently reused,
// flashing a fresh binary tagged with a stale commit.

use std::fs;
use std::process::Command;

mod decide;

use decide::DECLARED_VAR;

fn main() {
    println!("cargo:rerun-if-env-changed={DECLARED_VAR}");

    // TODO(build-id-declared-build-runs-git): both git calls below run even when the
    // commit is declared, so a builder that lets no build action run a host program
    // cannot use the declared commit.

    // Re-run whenever HEAD or the current branch's ref moves, so the stamp tracks HEAD.
    // TODO(build-id-dirty-flag-stale): these triggers cover a moving ref only, so the
    // dirty flag below is not re-evaluated when tracked files change under an unmoved
    // ref; a build then carries the flag of this script's last run in its target dir.
    // In a linked worktree they watch the main checkout's `HEAD` and branch, not the
    // worktree's, so the commit below can be stale there too.
    emit_git_ref_rerun_triggers();

    let stamp = decide::parse_declared(std::env::var_os(DECLARED_VAR).as_deref())
        .and_then(|declared| decide::decide(git_prefix(), declared, git_commit_and_dirty))
        .unwrap_or_else(|why| panic!("{why}"));

    println!("cargo:rustc-env=HIL_BUILD_COMMIT={}", stamp.commit);
    println!("cargo:rustc-env=HIL_BUILD_DIRTY={}", stamp.dirty);
    println!("cargo:rustc-env=HIL_BUILD_SOURCE={}", stamp.source);
}

/// `git <args>` run in the crate's directory, with repository discovery starting
/// there. Git exports `GIT_DIR` to the hooks it runs (from a linked worktree, and
/// without `GIT_WORK_TREE`), and a build run from a hook would otherwise take the
/// current directory as the top of the work tree and misplace this crate in it.
fn git(args: &[&str]) -> Command {
    let mut cmd = Command::new("git");
    cmd.args(args)
        .current_dir(manifest_dir())
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE");
    cmd
}

/// `git rev-parse --show-prefix` from the crate's directory: this crate's path
/// within the repository git finds around it. `Err` says why git could not answer
/// (not on PATH, no repository).
fn git_prefix() -> Result<String, String> {
    let output = git(&["rev-parse", "--show-prefix"])
        .output()
        .map_err(|e| format!("failed to run `git rev-parse --show-prefix`: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "`git rev-parse --show-prefix` exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn manifest_dir() -> String {
    std::env::var("CARGO_MANIFEST_DIR").expect("cargo sets CARGO_MANIFEST_DIR for build scripts")
}

/// The checkout's HEAD and whether its tracked files carry uncommitted changes.
/// Called only once git has placed this crate in its own repository, so any git
/// failure here is a hard failure.
fn git_commit_and_dirty() -> (String, bool) {
    let commit_output = git(&["rev-parse", "HEAD"]).output().unwrap_or_else(|e| {
        panic!("build-id: failed to run `git rev-parse HEAD`: {e}. git must be on PATH.")
    });

    if !commit_output.status.success() {
        panic!(
            "build-id: `git rev-parse HEAD` exited with {}: {}",
            commit_output.status,
            String::from_utf8_lossy(&commit_output.stderr).trim()
        );
    }

    let commit = String::from_utf8_lossy(&commit_output.stdout)
        .trim()
        .to_string();
    if commit.is_empty() {
        panic!("build-id: `git rev-parse HEAD` returned empty output; not a git repository?");
    }

    // Dirty flag: non-empty `git status --porcelain --untracked-files=no` output means
    // dirty. Untracked files are excluded so always-present scratch (HIL captures, ADR
    // drafts, handoff notes) does not stamp a byte-clean committed HEAD as dirty; only
    // uncommitted changes to tracked files count.
    let dirty_output = git(&["status", "--porcelain", "--untracked-files=no"])
        .output()
        .unwrap_or_else(|e| {
            panic!("build-id: failed to run `git status --porcelain --untracked-files=no`: {e}. git must be on PATH.")
        });

    if !dirty_output.status.success() {
        panic!(
            "build-id: `git status --porcelain --untracked-files=no` exited with {}: {}",
            dirty_output.status,
            String::from_utf8_lossy(&dirty_output.stderr).trim()
        );
    }

    (commit, !dirty_output.stdout.is_empty())
}

/// Emit `cargo:rerun-if-changed` for the git ref files that move when HEAD advances,
/// so the commit/dirty stamp is re-evaluated on every commit or branch switch.
///
/// Covers three cases, degrading gracefully if any is absent or HEAD is detached:
/// - `HEAD` itself — changes on every checkout and on commits while detached.
/// - the current branch's loose ref — moves on every commit while on a branch. We read
///   `HEAD` to discover the symbolic-ref target rather than hardcoding `main`, so a
///   branch switch is also covered.
/// - `packed-refs` — where the branch tip lives after `git gc` / `git pack-refs`. The
///   loose↔packed transition itself re-triggers because cargo tracks the (non-)existence
///   of every declared path.
///
/// The git directory is resolved via `git rev-parse --git-common-dir`, so in a linked
/// worktree (where `.git` is a file pointing elsewhere) the paths watched, including
/// `HEAD` and the branch read from it, are the main checkout's, not the worktree's own
/// (`<common>/worktrees/<name>/HEAD`). Declaring a path that does not currently exist
/// is safe.
fn emit_git_ref_rerun_triggers() {
    let git_dir = git(&["rev-parse", "--git-common-dir"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| ".git".to_string());

    // HEAD (symbolic-ref pointer) — changes on every checkout / detached-HEAD commit.
    println!("cargo:rerun-if-changed={git_dir}/HEAD");
    // Packed refs — where the branch tip lives after `git gc` / `git pack-refs`.
    println!("cargo:rerun-if-changed={git_dir}/packed-refs");

    // Current branch's loose ref. Read HEAD to find the symbolic-ref target rather than
    // hardcoding `main`; if HEAD is detached (no `ref:` prefix) or unreadable, there is
    // no branch ref to track and HEAD itself already covers detached commits.
    if let Ok(head_contents) = fs::read_to_string(format!("{git_dir}/HEAD"))
        && let Some(ref_path) = head_contents.trim().strip_prefix("ref:")
    {
        let ref_path = ref_path.trim();
        if !ref_path.is_empty() {
            println!("cargo:rerun-if-changed={git_dir}/{ref_path}");
        }
    }
}
