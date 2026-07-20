//! The shared spine-format check: does a `.wav` spec match `SPINE_FORMAT`
//! (16 kHz mono S16 integer PCM)?
//!
//! This lives beside the `SPINE_FORMAT` spine type it compares against so the
//! one audio-format invariant has exactly one home. Every consumer that reads a
//! spine-format WAV — the brain-clip loader in `speech-surface` and the HTTP
//! synthesizer's decoded output — delegates here rather than re-deriving the
//! comparison, so the accept/reject decision cannot drift between them.
//!
//! No resampling: a silent resample would let a mis-produced asset "work" while
//! hiding a format bug in the one place the whole playback chain assumes one
//! format. A wrong format is a reported violation, never a reinterpretation of
//! the bytes.

use std::path::Path;

use crate::SPINE_FORMAT;

/// A `.wav` spec's deviation from `SPINE_FORMAT` (16 kHz mono S16 integer PCM),
/// carrying the offending value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SpineFormatViolation {
    #[error("expected mono, got {channels} channels")]
    Channels { channels: u16 },
    #[error("expected {expected} Hz, got {actual} Hz")]
    SampleRate { expected: u32, actual: u32 },
    #[error("expected {expected}-bit integer PCM, got {bits}-bit {format:?}")]
    BitDepth {
        expected: u16,
        bits: u16,
        format: hound::SampleFormat,
    },
}

/// Check a `.wav` spec against `SPINE_FORMAT` (16 kHz, mono, 16-bit integer PCM),
/// returning the first deviation. Callers add their own context (a path, an
/// endpoint); the comparison itself lives here so it cannot drift.
pub fn check_spine_format(spec: &hound::WavSpec) -> Result<(), SpineFormatViolation> {
    if spec.channels != u16::from(SPINE_FORMAT.channels) {
        return Err(SpineFormatViolation::Channels {
            channels: spec.channels,
        });
    }
    if spec.sample_rate != SPINE_FORMAT.sample_rate_hz {
        return Err(SpineFormatViolation::SampleRate {
            expected: SPINE_FORMAT.sample_rate_hz,
            actual: spec.sample_rate,
        });
    }
    // SPINE_FORMAT.codec is S16Le, so integer PCM at the spine bit depth.
    if spec.bits_per_sample != u16::from(SPINE_FORMAT.bits_per_sample)
        || spec.sample_format != hound::SampleFormat::Int
    {
        return Err(SpineFormatViolation::BitDepth {
            expected: u16::from(SPINE_FORMAT.bits_per_sample),
            bits: spec.bits_per_sample,
            format: spec.sample_format,
        });
    }
    Ok(())
}

/// Write `samples` as a `SPINE_FORMAT` (16 kHz mono S16) `.wav` at `path` — the
/// one writer for this on-disk format, so every exporter shares its error
/// handling and spec instead of re-deriving it.
pub fn write_spine_wav(path: &Path, samples: &[i16]) -> Result<(), hound::Error> {
    let spec = hound::WavSpec {
        channels: u16::from(SPINE_FORMAT.channels),
        sample_rate: SPINE_FORMAT.sample_rate_hz,
        bits_per_sample: u16::from(SPINE_FORMAT.bits_per_sample),
        sample_format: hound::SampleFormat::Int,
    };
    let mut writer = hound::WavWriter::create(path, spec)?;
    for &sample in samples {
        writer.write_sample(sample)?;
    }
    writer.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spine_spec() -> hound::WavSpec {
        hound::WavSpec {
            channels: 1,
            sample_rate: SPINE_FORMAT.sample_rate_hz,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        }
    }

    #[test]
    fn spine_spec_accepted() {
        assert_eq!(check_spine_format(&spine_spec()), Ok(()));
    }

    #[test]
    fn wrong_sample_rate_rejected() {
        let mut spec = spine_spec();
        spec.sample_rate = 44_100;
        assert_eq!(
            check_spine_format(&spec),
            Err(SpineFormatViolation::SampleRate {
                expected: 16_000,
                actual: 44_100,
            })
        );
    }

    #[test]
    fn stereo_rejected() {
        let mut spec = spine_spec();
        spec.channels = 2;
        assert_eq!(
            check_spine_format(&spec),
            Err(SpineFormatViolation::Channels { channels: 2 })
        );
    }

    #[test]
    fn wrong_bit_depth_rejected() {
        let mut spec = spine_spec();
        spec.bits_per_sample = 24;
        assert!(matches!(
            check_spine_format(&spec),
            Err(SpineFormatViolation::BitDepth { bits: 24, .. })
        ));
    }

    #[test]
    fn float_format_rejected() {
        let mut spec = spine_spec();
        spec.bits_per_sample = 32;
        spec.sample_format = hound::SampleFormat::Float;
        assert!(matches!(
            check_spine_format(&spec),
            Err(SpineFormatViolation::BitDepth {
                format: hound::SampleFormat::Float,
                ..
            })
        ));
    }
}
