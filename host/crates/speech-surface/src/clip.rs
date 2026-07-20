//! Startup loader for the `WavBrain` clip: read a `.wav` into the one PCM
//! format the outbound chain assumes (`SPINE_FORMAT`: 16 kHz mono S16) or fail
//! fatally, naming the path and the offending property.
//!
//! No resampling: a silent resample would let a mis-produced asset "work" while
//! hiding a format bug in the one place the whole playback chain assumes one
//! format. The operator converts the asset once; a wrong format is a fatal
//! startup error, never a runtime surprise.

use std::path::{Path, PathBuf};
use std::sync::Arc;

// The spine-format check lives in `speech-pipeline` beside `SPINE_FORMAT`; re-export
// it here so this crate's loaders and `wav-import` reach it under one path.
pub use speech_pipeline::{check_spine_format, SpineFormatViolation};

/// A failure loading the brain clip, carrying the offending path and property.
#[derive(Debug, thiserror::Error)]
pub enum ClipError {
    #[error("failed to open clip {path}: {source}")]
    Open { path: PathBuf, source: hound::Error },
    #[error("failed to read clip {path}: {source}")]
    Read { path: PathBuf, source: hound::Error },
    #[error("clip {path}: {violation}")]
    Format {
        path: PathBuf,
        violation: SpineFormatViolation,
    },
    #[error("clip {path}: empty, no samples")]
    Empty { path: PathBuf },
    #[error("brain.clip is required when brain.mode = \"wav\"")]
    MissingPath,
}

/// Read a `.wav` clip at `path` into a shared PCM buffer, accepting only
/// `SPINE_FORMAT` (16 kHz, mono, 16-bit integer PCM) and rejecting an empty
/// clip. Any other format is a fatal error naming the offending property rather
/// than a reinterpretation of the bytes.
pub fn load_clip(path: &Path) -> Result<Arc<[i16]>, ClipError> {
    let reader = hound::WavReader::open(path).map_err(|source| ClipError::Open {
        path: path.to_path_buf(),
        source,
    })?;
    let spec = reader.spec();
    check_spine_format(&spec).map_err(|violation| ClipError::Format {
        path: path.to_path_buf(),
        violation,
    })?;
    let pcm = reader
        .into_samples::<i16>()
        .collect::<Result<Vec<i16>, _>>()
        .map_err(|source| ClipError::Read {
            path: path.to_path_buf(),
            source,
        })?;
    if pcm.is_empty() {
        return Err(ClipError::Empty {
            path: path.to_path_buf(),
        });
    }
    Ok(Arc::from(pcm))
}

#[cfg(test)]
mod tests {
    use super::*;
    use speech_pipeline::SPINE_FORMAT;

    fn write_wav(path: &Path, spec: hound::WavSpec, samples: &[i32]) {
        let mut w = hound::WavWriter::create(path, spec).unwrap();
        for &s in samples {
            w.write_sample(s).unwrap();
        }
        w.finalize().unwrap();
    }

    fn spine_spec() -> hound::WavSpec {
        hound::WavSpec {
            channels: 1,
            sample_rate: SPINE_FORMAT.sample_rate_hz,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        }
    }

    #[test]
    fn conforming_clip_loads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ok.wav");
        crate::test_support::write_spine_wav(&path, &[0i16, 1, 2, 3, -4]);
        let clip = load_clip(&path).unwrap();
        assert_eq!(&*clip, &[0i16, 1, 2, 3, -4]);
    }

    #[test]
    fn missing_file_is_open_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("absent.wav");
        let err = load_clip(&path).unwrap_err();
        assert!(matches!(err, ClipError::Open { .. }));
    }

    #[test]
    fn stereo_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stereo.wav");
        let mut spec = spine_spec();
        spec.channels = 2;
        write_wav(&path, spec, &[0, 0, 1, 1]);
        let err = load_clip(&path).unwrap_err();
        assert!(matches!(
            err,
            ClipError::Format {
                violation: SpineFormatViolation::Channels { channels: 2 },
                ..
            }
        ));
    }

    #[test]
    fn empty_clip_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.wav");
        crate::test_support::write_spine_wav(&path, &[]);
        let err = load_clip(&path).unwrap_err();
        assert!(matches!(err, ClipError::Empty { .. }));
    }
}
