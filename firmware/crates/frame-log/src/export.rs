//! A frame log as a WAV file: every sample index once, in order, each hole in
//! the index as silence, and a map that says which WAV frames are audio, which
//! are filled and on what basis, and which audio a segment sent again.
//!
//! Two passes over the log. The first plans the file (its length, runs, holes
//! and resends) and refuses what has no defined reading before anything is
//! created; the second writes it.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;

use audio_pipeline::wire::Hello;
use serde::Serialize;

use crate::summary::{SummaryError, TailSummary, first_hello};
use crate::unmeasured::Unmeasured;
use crate::walk::{AudioStep, JumpKind, Step, walk};

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
    /// Every run of contiguous audio from the log.
    pub runs: Vec<Run>,
    /// Every hole in the index, in order, with the silence that stands for it.
    pub holes: Vec<Hole>,
    /// Audio a segment sent again, which the WAV holds once.
    pub resent: Vec<Resent>,
    /// The log's tail, as the summary states it.
    pub tail: TailSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Run {
    pub wav_frame: u64,
    /// The run's first sample index.
    pub first_frame: u64,
    pub frames: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Hole {
    pub wav_frame: u64,
    /// The sample index the hole is at.
    pub at_frame: u64,
    /// Frames of silence written for it.
    pub filled_frames: u64,
    pub basis: Basis,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Resent {
    pub segment_id: u32,
    /// The segment's base, where the resent audio begins.
    pub at_frame: u64,
    pub frames: u64,
}

/// What a hole's silence is as long as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Basis {
    /// A `Loss` frame's length, or an index jump inside a segment.
    Measured,
    /// A segment's `not_sent_frames`: audio the stream did not send.
    NotSent,
    /// The clock estimate of an unmeasured loss, 0 where that estimate is
    /// negative or another loss before the same audio carries it.
    ClockEstimate,
    /// An unmeasured loss with no estimate (no audio follows it, or no clock
    /// precedes it): no silence is written.
    Unknown,
}

/// Why an export produced no WAV file. Only [`ExportError::Incomplete`] leaves
/// one behind.
#[derive(Debug)]
pub enum ExportError {
    /// The log could not be read, or holds no hello.
    Read(SummaryError),
    /// A backward jump inside a segment, which has no defined reading.
    Overlap {
        segment_id: u32,
        at_frame: u64,
        frames: u64,
    },
    /// The audio and silence would pass the RIFF size limit.
    TooLarge { data_bytes: u128 },
    /// The log's bytes per second do not fit the WAV header's 32-bit field.
    ByteRate { byte_rate: u64 },
    /// The log held less on the second pass than on the first: it changed
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
            ExportError::Overlap {
                segment_id,
                at_frame,
                frames,
            } => write!(
                f,
                "segment {segment_id} goes back {frames} frames at index {at_frame}; \
                 an overlap inside a segment has no WAV reading"
            ),
            ExportError::TooLarge { data_bytes } => write!(
                f,
                "the WAV data would be {data_bytes} bytes, past the 4 GiB RIFF limit"
            ),
            ExportError::ByteRate { byte_rate } => write!(
                f,
                "the log's {byte_rate} bytes per second do not fit a WAV header"
            ),
            ExportError::Changed => f.write_str("the frame log changed while it was exported"),
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

/// A hole not yet placed: its silence goes before the next audio written.
#[derive(Debug, Clone, Copy)]
enum Pending {
    /// A known length: measured or not sent.
    Known {
        at_frame: u64,
        frames: u64,
        basis: Basis,
    },
    Unmeasured {
        at_frame: u64,
    },
}

/// A hole and the audio frame its silence goes before; `None` after the last.
struct PlannedHole {
    hole: Hole,
    before_audio: Option<u64>,
}

/// What the first pass found.
struct Plan {
    hello: Hello,
    sample_bytes: usize,
    /// Audio frames the second pass writes.
    audio_frames: u64,
    wav_frames: u64,
    runs: Vec<Run>,
    holes: Vec<PlannedHole>,
    resent: Vec<Resent>,
    /// The audio frames, by ordinal, whose first frames are already in the
    /// WAV, and how many.
    skips: Vec<(u64, u64)>,
    tail: TailSummary,
}

impl Plan {
    fn frame_bytes(&self) -> usize {
        self.sample_bytes * usize::from(self.hello.channels)
    }
}

/// The first pass's running state.
struct Planner {
    unmeasured: Unmeasured,
    /// The index after the last frame the WAV accounts for, audio or hole.
    covered: Option<u64>,
    wav_frame: u64,
    audio_frames: u64,
    pending: Vec<Pending>,
    /// Whether the next audio may extend the last run.
    extend: bool,
    runs: Vec<Run>,
    holes: Vec<PlannedHole>,
    resent: Vec<Resent>,
    skips: Vec<(u64, u64)>,
}

impl Planner {
    fn cover(&mut self, to: u64) {
        self.covered = Some(self.covered.map_or(to, |c| c.max(to)));
    }

    /// A hole of known length at `at_frame`. The part the WAV already accounts
    /// for (a gap or loss inside a resent stretch) is not filled again; a hole
    /// wholly inside it is no hole.
    fn known(&mut self, at_frame: u64, frames: u64, basis: Basis) {
        let end = at_frame + frames;
        let at_frame = self.covered.map_or(at_frame, |c| c.max(at_frame));
        if at_frame >= end {
            return;
        }
        self.pending.push(Pending::Known {
            at_frame,
            frames: end - at_frame,
            basis,
        });
        self.cover(end);
    }

    /// Place the pending holes before audio frame `a`, or after the last
    /// audio when `a` is `None`.
    fn place(&mut self, a: Option<&AudioStep>) {
        let mut estimates = self.unmeasured.resolve(a);
        for p in std::mem::take(&mut self.pending) {
            let (at_frame, filled_frames, basis) = match p {
                Pending::Known {
                    at_frame,
                    frames,
                    basis,
                } => (at_frame, frames, basis),
                // A WAV holds no negative silence: an estimate below 0 fills
                // nothing.
                Pending::Unmeasured { at_frame } => match estimates.next().flatten() {
                    Some(e) => (at_frame, e.max(0) as u64, Basis::ClockEstimate),
                    None => (at_frame, 0, Basis::Unknown),
                },
            };
            self.holes.push(PlannedHole {
                hole: Hole {
                    wav_frame: self.wav_frame,
                    at_frame,
                    filled_frames,
                    basis,
                },
                before_audio: a.map(|_| self.audio_frames),
            });
            self.wav_frame += filled_frames;
            self.extend = false;
        }
    }

    fn audio(&mut self, a: &AudioStep) -> Result<(), ExportError> {
        if let Some(j) = a.jump {
            match j.kind {
                JumpKind::Overlap => {
                    return Err(ExportError::Overlap {
                        segment_id: a.segment_id,
                        at_frame: j.expected,
                        frames: j.frames,
                    });
                }
                JumpKind::Gap => self.known(j.expected, j.frames, Basis::Measured),
            }
        }
        self.place(Some(a));
        let skip = self
            .covered
            .map_or(0, |c| c.saturating_sub(a.first_frame))
            .min(a.frames);
        if skip > 0 {
            self.skips.push((self.audio_frames, skip));
        }
        let first_frame = a.first_frame + skip;
        let frames = a.frames - skip;
        if frames > 0 {
            match self.runs.last_mut() {
                Some(run) if self.extend && run.first_frame + run.frames == first_frame => {
                    run.frames += frames;
                }
                _ => self.runs.push(Run {
                    wav_frame: self.wav_frame,
                    first_frame,
                    frames,
                }),
            }
            self.extend = true;
            self.wav_frame += frames;
        }
        self.cover(a.first_frame + a.frames);
        self.audio_frames += 1;
        Ok(())
    }

    fn take(&mut self, step: Step) -> Result<(), ExportError> {
        if let Some(h) = self.unmeasured.step(&step) {
            self.pending.push(Pending::Unmeasured {
                at_frame: h.at_frame,
            });
        }
        match step {
            Step::SegmentOpened {
                start,
                not_sent_frames,
                resent_frames,
                ..
            } => {
                let base = start.base_sample_index;
                if not_sent_frames > 0 {
                    self.known(base - not_sent_frames, not_sent_frames, Basis::NotSent);
                }
                if resent_frames > 0 {
                    self.resent.push(Resent {
                        segment_id: start.segment_id,
                        at_frame: base,
                        frames: resent_frames,
                    });
                }
            }
            Step::Audio(a) => self.audio(&a)?,
            Step::Loss {
                at_frame, samples, ..
            } => match samples {
                Some(n) => self.known(at_frame, n, Basis::Measured),
                None => self.pending.push(Pending::Unmeasured { at_frame }),
            },
            Step::Hello { .. }
            | Step::Describe { .. }
            | Step::SegmentClosed { .. }
            | Step::SegmentReplaced { .. }
            | Step::Read { .. }
            | Step::End { .. }
            | Step::Telemetry { .. }
            | Step::Status { .. } => {}
        }
        Ok(())
    }
}

/// Pass one: plan the WAV file for the log at `path`.
fn plan(path: &Path) -> Result<Plan, ExportError> {
    let read = ExportError::Read;
    let mut walk = walk(path).map_err(|e| read(SummaryError::Open(e)))?;
    let hello = first_hello(&mut walk).map_err(read)?;
    let mut p = Planner {
        unmeasured: Unmeasured::new(f64::from(hello.sample_rate_hz)),
        covered: None,
        wav_frame: 0,
        audio_frames: 0,
        pending: Vec::new(),
        extend: false,
        runs: Vec::new(),
        holes: Vec::new(),
        resent: Vec::new(),
        skips: Vec::new(),
    };
    for step in walk.by_ref() {
        p.take(step.map_err(|e| read(SummaryError::Io(e)))?)?;
    }
    if let Some(h) = p.unmeasured.finish() {
        p.pending.push(Pending::Unmeasured {
            at_frame: h.at_frame,
        });
    }
    p.place(None);
    let tail = TailSummary::from(walk.tail().expect("iteration ended without an I/O error"));
    Ok(Plan {
        sample_bytes: usize::from(hello.bits_per_sample / 8),
        hello,
        audio_frames: p.audio_frames,
        wav_frames: p.wav_frame,
        runs: p.runs,
        holes: p.holes,
        resent: p.resent,
        skips: p.skips,
        tail,
    })
}

fn wav_header(plan: &Plan, byte_rate: u32, data_bytes: u32) -> Vec<u8> {
    let bits = plan.hello.bits_per_sample as u16;
    let mut h = Vec::with_capacity(WAV_HEADER_BYTES as usize);
    h.extend_from_slice(b"RIFF");
    h.extend_from_slice(&(data_bytes + (WAV_HEADER_BYTES as u32 - 8)).to_le_bytes());
    h.extend_from_slice(b"WAVE");
    h.extend_from_slice(b"fmt ");
    h.extend_from_slice(&40u32.to_le_bytes());
    h.extend_from_slice(&0xfffeu16.to_le_bytes());
    h.extend_from_slice(&u16::from(plan.hello.channels).to_le_bytes());
    h.extend_from_slice(&plan.hello.sample_rate_hz.to_le_bytes());
    h.extend_from_slice(&byte_rate.to_le_bytes());
    h.extend_from_slice(&(plan.frame_bytes() as u16).to_le_bytes());
    h.extend_from_slice(&bits.to_le_bytes());
    h.extend_from_slice(&22u16.to_le_bytes());
    h.extend_from_slice(&bits.to_le_bytes());
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

/// Export the frame log at `path` to a new WAV file at `out`, which must not
/// exist. On any error nothing is left at `out`, unless the error is
/// [`ExportError::Incomplete`].
pub fn export_wav(path: &Path, out: &Path) -> Result<ExportMap, ExportError> {
    let plan = plan(path)?;
    let frame_bytes = plan.frame_bytes();
    let byte_rate = u64::from(plan.hello.sample_rate_hz) * frame_bytes as u64;
    let byte_rate = u32::try_from(byte_rate).map_err(|_| ExportError::ByteRate { byte_rate })?;
    let data_bytes = u128::from(plan.wav_frames) * frame_bytes as u128;
    let limit = u128::from(u32::MAX) - u128::from(WAV_HEADER_BYTES - 8);
    if data_bytes > limit {
        return Err(ExportError::TooLarge { data_bytes });
    }
    write_new(out, |w| {
        w.write_all(&wav_header(&plan, byte_rate, data_bytes as u32))
            .map_err(ExportError::Output)?;
        fill_wav(w, path, &plan)
    })?;
    Ok(ExportMap {
        wav_frames: plan.wav_frames,
        runs: plan.runs,
        holes: plan.holes.into_iter().map(|h| h.hole).collect(),
        resent: plan.resent,
        tail: plan.tail,
    })
}

/// Pass two: write the planned audio and silence of the log at `path`.
fn fill_wav(w: &mut impl Write, path: &Path, plan: &Plan) -> Result<(), ExportError> {
    let frame_bytes = plan.frame_bytes() as u64;
    let mut walk = walk(path).map_err(|e| ExportError::Read(SummaryError::Open(e)))?;
    let mut holes = plan.holes.iter().peekable();
    let mut skips = plan.skips.iter().peekable();
    let mut written = 0u64;
    let silence = |w: &mut dyn Write, frames: u64, written: &mut u64| {
        *written += frames;
        write_silence(w, frames * frame_bytes).map_err(ExportError::Output)
    };
    let mut ordinal = 0u64;
    while ordinal < plan.audio_frames {
        let step = walk
            .next()
            .ok_or(ExportError::Changed)?
            .map_err(|e| ExportError::Read(SummaryError::Io(e)))?;
        let Step::Audio(a) = step else { continue };
        while let Some(h) = holes.next_if(|h| h.before_audio == Some(ordinal)) {
            silence(w, h.hole.filled_frames, &mut written)?;
        }
        let skip = skips
            .next_if(|&&(n, _)| n == ordinal)
            .map_or(0, |&(_, s)| s);
        w.write_all(&a.pcm[(skip * frame_bytes) as usize..])
            .map_err(ExportError::Output)?;
        written += a.frames - skip;
        ordinal += 1;
    }
    for h in holes {
        silence(w, h.hole.filled_frames, &mut written)?;
    }
    if written != plan.wav_frames {
        return Err(ExportError::Changed);
    }
    Ok(())
}

fn write_silence(w: &mut dyn Write, mut bytes: u64) -> io::Result<()> {
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
    fn a_fill_that_succeeds_keeps_what_it_wrote() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.wav");
        write_new(&out, |w| w.write_all(b"whole").map_err(ExportError::Output)).unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), b"whole");
    }
}
