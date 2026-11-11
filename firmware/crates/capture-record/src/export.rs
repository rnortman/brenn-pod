//! A capture record as a WAV file: every block's PCM in order, each gap as
//! silence, and a map that says which WAV frames are audio and which are
//! filled, and on what basis.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;

use serde::Serialize;

use crate::summary::{self, ReadError, TailSummary};
use crate::{Granted, Item};

/// Bytes of the WAV header this writes: `RIFF`, a 40-byte
/// `WAVE_FORMAT_EXTENSIBLE` `fmt ` chunk, and the `data` chunk's header.
pub const WAV_HEADER_BYTES: u64 = 68;

/// The PCM sub-format GUID, `00000001-0000-0010-8000-00aa00389b71`.
const KSDATAFORMAT_SUBTYPE_PCM: [u8; 16] = [
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x80, 0x00, 0x00, 0xaa, 0x00, 0x38, 0x9b, 0x71,
];

/// Where each stretch of the WAV file came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExportMap {
    /// Frames in the WAV file.
    pub wav_frames: u64,
    /// Every run of contiguous recorded audio.
    pub runs: Vec<Run>,
    /// Every gap, in order, with the silence that stands for it.
    pub holes: Vec<Hole>,
    /// The record's tail, as the summary states it.
    pub tail: TailSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Run {
    pub wav_frame: u64,
    /// The run's position in the record's delivered-frame count.
    pub first_frame: u64,
    pub frames: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Hole {
    pub wav_frame: u64,
    /// The gap's position in the record.
    pub at_frame: u64,
    /// Frames of silence written for it.
    pub filled_frames: u64,
    pub basis: Basis,
}

/// What a hole's silence is as long as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Basis {
    /// The gap's measured length.
    Measured,
    /// The summary's clock estimate, 0 where that estimate is negative.
    ClockEstimate,
    /// No estimate exists (no block follows the gap): no silence is written.
    Unknown,
}

/// Why an export produced no WAV file. Only [`ExportError::Incomplete`] leaves
/// one behind.
#[derive(Debug)]
pub enum ExportError {
    Read(ReadError),
    /// The audio and silence would pass the RIFF size limit.
    TooLarge {
        data_bytes: u128,
    },
    /// The record's bytes per second do not fit the WAV header's 32-bit field.
    ByteRate {
        byte_rate: u64,
    },
    /// The record ended before the items the first pass read: it changed
    /// while it was exported.
    Changed,
    /// Creating or writing the output failed.
    Output(io::Error),
    /// The export failed after the output was created, and the incomplete
    /// file, whose header states more data than it holds, could not be removed.
    Incomplete {
        error: Box<ExportError>,
        remove: io::Error,
    },
}

impl fmt::Display for ExportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExportError::Read(e) => e.fmt(f),
            ExportError::TooLarge { data_bytes } => write!(
                f,
                "the WAV data would be {data_bytes} bytes, past the 4 GiB RIFF limit"
            ),
            ExportError::ByteRate { byte_rate } => write!(
                f,
                "the record's {byte_rate} bytes per second do not fit a WAV header"
            ),
            ExportError::Changed => f.write_str("the capture record changed while it was exported"),
            ExportError::Output(e) => write!(f, "writing the WAV file: {e}"),
            ExportError::Incomplete { error, remove } => write!(
                f,
                "{error}; the incomplete WAV file could not be removed and must not be used: \
                 {remove}"
            ),
        }
    }
}

impl std::error::Error for ExportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ExportError::Read(e) => Some(e),
            ExportError::Output(e) => Some(e),
            ExportError::Incomplete { error, .. } => Some(error.as_ref()),
            _ => None,
        }
    }
}

fn wav_header(g: &Granted, byte_rate: u32, data_bytes: u32) -> Vec<u8> {
    let bytes_per_sample = g.sample_format.bytes_per_sample() as u16;
    let block_align = g.frame_bytes() as u16;
    let mut h = Vec::with_capacity(WAV_HEADER_BYTES as usize);
    h.extend_from_slice(b"RIFF");
    h.extend_from_slice(&(data_bytes + (WAV_HEADER_BYTES as u32 - 8)).to_le_bytes());
    h.extend_from_slice(b"WAVE");
    h.extend_from_slice(b"fmt ");
    h.extend_from_slice(&40u32.to_le_bytes());
    h.extend_from_slice(&0xfffeu16.to_le_bytes());
    h.extend_from_slice(&(g.channels as u16).to_le_bytes());
    h.extend_from_slice(&g.rate_hz.to_le_bytes());
    h.extend_from_slice(&byte_rate.to_le_bytes());
    h.extend_from_slice(&block_align.to_le_bytes());
    h.extend_from_slice(&(bytes_per_sample * 8).to_le_bytes());
    h.extend_from_slice(&22u16.to_le_bytes());
    h.extend_from_slice(&(bytes_per_sample * 8).to_le_bytes());
    // Channel mask 0: no speaker positions are claimed.
    h.extend_from_slice(&0u32.to_le_bytes());
    h.extend_from_slice(&KSDATAFORMAT_SUBTYPE_PCM);
    h.extend_from_slice(b"data");
    h.extend_from_slice(&data_bytes.to_le_bytes());
    debug_assert_eq!(h.len() as u64, WAV_HEADER_BYTES);
    h
}

/// Create `out`, which must not exist, fill it with `fill`, flush and sync it.
/// When anything after the create fails, `out` is removed, so no failed export
/// leaves a file whose header states more data than it holds.
fn write_new<T>(
    out: &Path,
    fill: impl FnOnce(&mut BufWriter<File>) -> Result<T, ExportError>,
) -> Result<T, ExportError> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(out)
        .map_err(ExportError::Output)?;
    let mut w = BufWriter::new(file);
    let result = fill(&mut w).and_then(|t| {
        let file = w
            .into_inner()
            .map_err(|e| ExportError::Output(e.into_error()))?;
        file.sync_all().map_err(ExportError::Output)?;
        Ok(t)
    });
    result.map_err(|error| match std::fs::remove_file(out) {
        Ok(()) => error,
        Err(remove) => ExportError::Incomplete {
            error: Box::new(error),
            remove,
        },
    })
}

/// Export the capture record at `path` to a new WAV file at `out`, which must
/// not exist. On any error nothing is left at `out`, unless the error is
/// [`ExportError::Incomplete`].
pub fn export_wav(path: &Path, out: &Path) -> Result<ExportMap, ExportError> {
    let scan = summary::scan(path).map_err(ExportError::Read)?;
    let g = &scan.header.granted;
    let byte_rate = u64::from(g.rate_hz) * g.frame_bytes() as u64;
    let byte_rate = u32::try_from(byte_rate).map_err(|_| ExportError::ByteRate { byte_rate })?;
    let fills: Vec<(u64, Basis)> = scan
        .summary
        .gaps
        .iter()
        .map(|gap| match (gap.frames, gap.clock_estimate_frames) {
            (Some(n), _) => (n, Basis::Measured),
            (None, Some(e)) => (e.max(0) as u64, Basis::ClockEstimate),
            (None, None) => (0, Basis::Unknown),
        })
        .collect();
    let wav_frames = u128::from(scan.summary.frames_recorded)
        + fills.iter().map(|&(n, _)| u128::from(n)).sum::<u128>();
    let data_bytes = wav_frames * g.frame_bytes() as u128;
    let limit = u128::from(u32::MAX) - u128::from(WAV_HEADER_BYTES - 8);
    if data_bytes > limit {
        return Err(ExportError::TooLarge { data_bytes });
    }

    let (wav_frames, runs, holes) = write_new(out, |w| {
        w.write_all(&wav_header(g, byte_rate, data_bytes as u32))
            .map_err(ExportError::Output)?;
        fill_wav(w, path, g.frame_bytes(), scan.items, fills)
    })?;
    Ok(ExportMap {
        wav_frames,
        runs,
        holes,
        tail: scan.summary.tail,
    })
}

/// Pass two: write the first `items` items of the record at `path` after the
/// header, each gap as the silence `fills` gives it, and return the WAV's frame
/// count, its runs and its holes.
fn fill_wav(
    w: &mut impl Write,
    path: &Path,
    frame_bytes: usize,
    items: u64,
    fills: Vec<(u64, Basis)>,
) -> Result<(u64, Vec<Run>, Vec<Hole>), ExportError> {
    let mut reader = summary::open(path).map_err(ExportError::Read)?;
    let mut runs: Vec<Run> = Vec::new();
    let mut holes = Vec::new();
    let mut wav_frame = 0u64;
    let mut fills = fills.into_iter();
    // Whether the next block may extend the last run: no hole since it.
    let mut extend = false;
    for _ in 0..items {
        let item = reader
            .next()
            .ok_or(ExportError::Changed)?
            .map_err(|e| ExportError::Read(ReadError::Io(e)))?;
        match item {
            Item::Block {
                first_frame,
                frames,
                pcm,
                ..
            } => {
                match runs.last_mut() {
                    Some(run) if extend && run.first_frame + run.frames == first_frame => {
                        run.frames += frames;
                    }
                    _ => runs.push(Run {
                        wav_frame,
                        first_frame,
                        frames,
                    }),
                }
                extend = true;
                w.write_all(&pcm).map_err(ExportError::Output)?;
                wav_frame += frames;
            }
            Item::Gap { gap, .. } => {
                let (filled_frames, basis) = fills.next().ok_or(ExportError::Changed)?;
                holes.push(Hole {
                    wav_frame,
                    at_frame: gap.at_frame,
                    filled_frames,
                    basis,
                });
                write_silence(w, filled_frames * frame_bytes as u64)
                    .map_err(ExportError::Output)?;
                wav_frame += filled_frames;
                extend = false;
            }
            Item::End { .. } => {}
        }
    }
    Ok((wav_frame, runs, holes))
}

fn write_silence(w: &mut impl Write, mut bytes: u64) -> io::Result<()> {
    const ZEROS: [u8; 8192] = [0; 8192];
    while bytes > 0 {
        let n = bytes.min(ZEROS.len() as u64) as usize;
        w.write_all(&ZEROS[..n])?;
        bytes -= n as u64;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn a_second_pass_that_cannot_read_the_record_leaves_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.wav");
        let gone = dir.path().join("gone.bpcr");
        let result = write_new(&out, |w| {
            w.write_all(b"RIFF").map_err(ExportError::Output)?;
            fill_wav(w, &gone, 4, 1, Vec::new())
        });
        assert!(matches!(result, Err(ExportError::Read(_))), "{result:?}");
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
        let source = std::error::Error::source(&err).expect("a source");
        assert!(matches!(
            source.downcast_ref::<ExportError>(),
            Some(ExportError::Changed)
        ));
        assert!(
            message.contains(&ExportError::Changed.to_string()),
            "{message}"
        );
        assert!(message.contains(&remove.to_string()), "{message}");
    }

    #[test]
    fn a_fill_that_succeeds_keeps_what_it_wrote() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.wav");
        write_new(&out, |w| w.write_all(b"whole").map_err(ExportError::Output)).unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), b"whole");
    }
}
