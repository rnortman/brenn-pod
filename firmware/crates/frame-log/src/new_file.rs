//! An output file that is either written whole or not left behind.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter};
use std::path::Path;

/// An error type that can say an output failed, and that a failed output could
/// not be removed.
pub(crate) trait NewFileError: Sized {
    /// Creating, writing, flushing or syncing the output failed.
    fn output(e: io::Error) -> Self;
    /// `error` happened after the output was created, and removing it failed
    /// with `remove`.
    fn incomplete(error: Self, remove: io::Error) -> Self;
}

/// Create `out`, which must not exist, fill it with `fill`, flush and sync it.
/// When anything after the create fails, `out` is removed, so no failed write
/// leaves a file behind; when the removal fails too, the error is
/// [`NewFileError::incomplete`].
pub(crate) fn write_new<T, E: NewFileError>(
    out: &Path,
    fill: impl FnOnce(&mut BufWriter<File>) -> Result<T, E>,
) -> Result<T, E> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(out)
        .map_err(E::output)?;
    let mut w = BufWriter::new(file);
    let result = fill(&mut w).and_then(|t| {
        let file = w.into_inner().map_err(|e| E::output(e.into_error()))?;
        file.sync_all().map_err(E::output)?;
        Ok(t)
    });
    result.map_err(|error| match std::fs::remove_file(out) {
        Ok(()) => error,
        Err(remove) => E::incomplete(error, remove),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExportError, WriteCaptureError};
    use std::io::Write;

    #[test]
    fn a_fill_that_fails_after_writing_leaves_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.wav");
        let result: Result<(), _> = write_new(&out, |w| {
            w.write_all(b"RIFF and a header promising more").unwrap();
            Err(ExportError::Changed)
        });
        assert!(matches!(result, Err(ExportError::Changed)), "{result:?}");
        assert!(!out.exists());
    }

    #[test]
    fn a_failed_removal_is_incomplete_and_names_both_errors() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.wav");
        let result: Result<(), _> = write_new(&out, |_| {
            std::fs::remove_file(&out).unwrap();
            Err(ExportError::Changed)
        });
        let err = result.unwrap_err();
        let message = err.to_string();
        let ExportError::Incomplete { error, remove } = &err else {
            panic!("expected Incomplete, got {err:?}");
        };
        assert!(matches!(**error, ExportError::Changed), "{error:?}");
        assert_eq!(remove.kind(), io::ErrorKind::NotFound);
        assert!(
            message.contains(&ExportError::Changed.to_string()),
            "{message}"
        );
        assert!(message.contains(&remove.to_string()), "{message}");
    }

    #[test]
    fn a_failed_removal_of_a_capture_log_is_incomplete() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.framelog");
        let result: Result<(), _> = write_new(&out, |_| {
            std::fs::remove_file(&out).unwrap();
            Err(WriteCaptureError::Spec("stand-in".to_owned()))
        });
        let err = result.unwrap_err();
        let WriteCaptureError::Incomplete { error, remove } = &err else {
            panic!("expected Incomplete, got {err:?}");
        };
        assert!(matches!(**error, WriteCaptureError::Spec(_)), "{error:?}");
        assert_eq!(remove.kind(), io::ErrorKind::NotFound);
        assert!(err.to_string().contains("must not be used"), "{err}");
    }

    #[test]
    fn a_fill_that_succeeds_keeps_what_it_wrote() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.wav");
        write_new(&out, |w| w.write_all(b"whole").map_err(ExportError::Output)).unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), b"whole");
    }

    #[test]
    fn an_existing_output_is_refused_and_kept() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.wav");
        std::fs::write(&out, b"theirs").unwrap();
        let result: Result<(), _> = write_new(&out, |_| -> Result<(), ExportError> {
            panic!("fill must not run")
        });
        let Err(ExportError::Output(e)) = &result else {
            panic!("expected Output, got {result:?}");
        };
        assert_eq!(e.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&out).unwrap(), b"theirs");
    }
}
