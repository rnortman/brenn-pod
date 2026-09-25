//! What a secrets file on a pod host is allowed to look like.
//!
//! One definition for every credential the host holds — the per-pod PSK table,
//! the bus bearer token, whatever comes next — so the fleet's secrets cannot
//! drift into different protections, and so the next hardening of the posture
//! (an ownership check, a symlink refusal, a stricter answer to an unreadable
//! stat) lands in one place and covers all of them. The posture is that one
//! policy's one parameter, declared by the deployment.
//!
//! No dependencies: this is std and a policy.

use std::path::Path;

/// Who protects a secrets file — the deployment's declaration, not a guess
/// from ownership or mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Posture {
    /// The file is the host's own: refused if any other local account can read it.
    OwnerOnly,
    /// The file is a member of a read-only tree the OS delivers to this
    /// single-purpose device world-readable by contract; the tree's
    /// protection (an authenticated fetch, a root-only ssh) is the file's.
    /// Refused only if any other account could *write* it.
    Payload,
}

/// Reject a secrets file the declared `posture` does not allow. Under
/// [`Posture::OwnerOnly`] that is a file any other local account can read,
/// matching ssh's posture on private keys; under [`Posture::Payload`] it is a
/// file any other account can write. `what` names the file in the message an
/// operator reads — "psk file", "token file" — and `None` means the posture is
/// satisfied.
///
/// A path that cannot be stat'd answers `None` rather than a refusal: the caller
/// is about to read the file and will report the real failure with the real
/// error, which is a better message than a mode check guessing at one.
///
/// Unix only — elsewhere there is no mode to check.
#[cfg(unix)]
pub fn mode_error(path: &Path, what: &str, posture: Posture) -> Option<String> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(path).ok()?.permissions().mode() & 0o777;
    match posture {
        Posture::OwnerOnly if mode & 0o077 != 0 => Some(format!(
            "{what} mode {mode:04o} is group/world-accessible; chmod 600 it"
        )),
        Posture::Payload if mode & 0o022 != 0 => Some(format!(
            "{what} mode {mode:04o} is group/world-writable; a payload member is read-only"
        )),
        _ => None,
    }
}

#[cfg(not(unix))]
pub fn mode_error(_path: &Path, _what: &str, _posture: Posture) -> Option<String> {
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
    fn owner_only_table() {
        // The posture is about *other* accounts; a 0400 key is stricter, not
        // wrong.
        for (mode, accepted) in [
            (0o600, true),
            (0o400, true),
            (0o640, false),
            (0o644, false),
            (0o622, false),
            (0o666, false),
        ] {
            let (_dir, path) = file_at(mode);
            let answer = mode_error(&path, "psk file", Posture::OwnerOnly);
            if accepted {
                assert_eq!(answer, None, "mode {mode:04o}");
            } else {
                let message = answer.unwrap_or_else(|| panic!("mode {mode:04o} is refused"));
                assert!(
                    message.starts_with(&format!("psk file mode {mode:04o}")),
                    "{message}"
                );
                assert!(message.contains("group/world-accessible"), "{message}");
                assert!(message.contains("chmod 600"), "{message}");
            }
        }
    }

    #[test]
    fn payload_table() {
        for (mode, accepted) in [
            (0o600, true),
            (0o400, true),
            (0o640, true),
            (0o644, true),
            (0o622, false),
            (0o666, false),
        ] {
            let (_dir, path) = file_at(mode);
            let answer = mode_error(&path, "token file", Posture::Payload);
            if accepted {
                assert_eq!(answer, None, "mode {mode:04o}");
            } else {
                let message = answer.unwrap_or_else(|| panic!("mode {mode:04o} is refused"));
                assert!(
                    message.starts_with(&format!("token file mode {mode:04o}")),
                    "{message}"
                );
                assert!(message.contains("group/world-writable"), "{message}");
                assert!(message.contains("read-only"), "{message}");
            }
        }
    }

    #[test]
    fn a_missing_file_is_the_readers_problem_not_this_checks() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let absent = dir.path().join("absent");
        for posture in [Posture::OwnerOnly, Posture::Payload] {
            assert_eq!(mode_error(&absent, "token file", posture), None);
        }
    }
}
