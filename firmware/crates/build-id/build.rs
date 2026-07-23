// build.rs for build-id crate.
//
// Stamps the current git commit + dirty flag into the binary. To keep the stamp from
// going stale, we emit `cargo:rerun-if-changed` for the git ref files that move when
// HEAD advances (see `emit_git_ref_rerun_triggers`). Without these, cargo would re-run
// the script only when files in this crate's own directory change — so HEAD could
// advance, downstream sources recompile, yet the cached commit stamp be silently reused,
// flashing a fresh binary tagged with a stale commit.

use std::fs;
use std::process::Command;

fn main() {
    // Re-run whenever HEAD or the current branch's ref moves, so the stamp tracks HEAD.
    emit_git_ref_rerun_triggers();

    // Derive git commit hash.
    //
    // Hard-fail on any git error: a silent sentinel ("unknown") would allow the
    // build-ID gate to pass falsely when git is unavailable (e.g. Docker, CI
    // without git installed), defeating the core HIL invariant.
    let commit_output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap_or_else(|e| {
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
    //
    // Hard-fail on git error for the same reason as above.
    let dirty_output = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=no"])
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

    let dirty = !dirty_output.stdout.is_empty();

    println!("cargo:rustc-env=HIL_BUILD_COMMIT={commit}");
    println!("cargo:rustc-env=HIL_BUILD_DIRTY={dirty}");
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
/// The git directory is resolved via `git rev-parse --git-common-dir` so this works
/// under worktrees (where `.git` is a file pointing elsewhere and refs live in the
/// common dir). Declaring a path that does not currently exist is safe.
fn emit_git_ref_rerun_triggers() {
    let git_dir = Command::new("git")
        .args(["rev-parse", "--git-common-dir"])
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
