//! What a secrets file on a pod host is allowed to look like.
//!
//! One definition for every credential the host holds — the per-pod PSK table,
//! the bus bearer token, whatever comes next — so the fleet's secrets cannot
//! drift into different protections, and so the next hardening of the posture
//! (an ownership check, a symlink refusal, a stricter answer to an unreadable
//! stat) lands in one place and covers all of them.
//!
//! No dependencies: this is std and a policy.

use std::path::Path;

/// Reject a secrets file any other local account can read, matching ssh's
/// posture on private keys. `what` names the file in the message an operator
/// reads — "psk file", "token file" — and `None` means the posture is satisfied.
///
/// A path that cannot be stat'd answers `None` rather than a refusal: the caller
/// is about to read the file and will report the real failure with the real
/// error, which is a better message than a mode check guessing at one.
///
/// Unix only — elsewhere there is no mode to check.
#[cfg(unix)]
pub fn mode_error(path: &Path, what: &str) -> Option<String> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(path).ok()?.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Some(format!(
            "{what} mode {mode:04o} is group/world-accessible; chmod 600 it"
        ));
    }
    None
}

#[cfg(not(unix))]
pub fn mode_error(_path: &Path, _what: &str) -> Option<String> {
    None
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// A file at `mode` in a temporary directory.
    fn file_at(mode: u32) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = dir.path().join("secret");
        std::fs::write(&path, "s3cret").expect("the fixture writes");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))
            .expect("the fixture chmods");
        (dir, path)
    }

    #[test]
    fn owner_only_is_accepted() {
        let (_dir, path) = file_at(0o600);
        assert_eq!(mode_error(&path, "token file"), None);
    }

    #[test]
    fn read_only_to_the_owner_is_accepted_too() {
        // The posture is about *other* accounts; a 0400 key is stricter, not
        // wrong.
        let (_dir, path) = file_at(0o400);
        assert_eq!(mode_error(&path, "psk file"), None);
    }

    #[test]
    fn a_group_readable_file_is_refused_by_name_and_mode() {
        let (_dir, path) = file_at(0o640);
        let message = mode_error(&path, "psk file").expect("a group-readable secret is refused");
        assert!(message.starts_with("psk file mode 0640"), "{message}");
        assert!(message.contains("chmod 600"), "{message}");
    }

    #[test]
    fn a_world_readable_file_is_refused() {
        let (_dir, path) = file_at(0o644);
        let message = mode_error(&path, "token file").expect("a world-readable secret is refused");
        assert!(message.starts_with("token file mode 0644"), "{message}");
    }

    #[test]
    fn a_missing_file_is_the_readers_problem_not_this_checks() {
        let dir = tempfile::tempdir().expect("a temp dir");
        assert_eq!(mode_error(&dir.path().join("absent"), "token file"), None);
    }
}
