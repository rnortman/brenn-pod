//! A capture log written from given samples: the log a recorder would have
//! written had its device delivered a WAV file's samples at exactly the
//! nominal rate, with the losses and the end a spec states. WAV frame `i` is
//! the capture's sample index `i`.

use std::fmt;
use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;

use audio_pipeline::wide::{LossCause, StreamEndReason};
use serde::{Deserialize, Serialize};

use crate::capture_stream::{Block, End, Event, FrameWriter, Framer, Gap, Granted, prelude};
use crate::new_file::{NewFileError, write_new};
use crate::wav::{WavError, WavReader};
use crate::{
    BuildSource, CaptureGranted, CaptureMeta, Clocks, Device, DeviceControl, DeviceState,
    HostMicros, LogMeta, WriterBuild, WriterInfo,
};

/// What `write-capture` is told to write besides the samples. Unknown keys are
/// refused.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    /// The hello's `pod_id`, 1 to 32 bytes.
    pub pod_id: String,
    /// One name per channel, in interleave order.
    pub layout: Vec<String>,
    pub period_frames: u32,
    pub buffer_frames: u32,
    pub start: StartPair,
    pub record_id: String,
    /// The commit of the `frame-log` that writes the log, as its caller
    /// states it.
    pub commit: String,
    pub device: Device,
    #[serde(default)]
    pub device_state: Option<DeviceState>,
    #[serde(default)]
    pub losses: Vec<LossSpec>,
    #[serde(default)]
    pub end: Option<EndSpec>,
}

/// The capture's start pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartPair {
    pub mono_us: u64,
    pub wall_us: u64,
}

/// A loss at `at_frame`: measured (`writer_backlog` with `frames`) or
/// unmeasured (any other cause, with `elapsed_frames`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LossSpec {
    pub at_frame: u64,
    pub cause: LossCause,
    /// A measured loss's length: WAV frames not written.
    #[serde(default)]
    pub frames: Option<u64>,
    /// An unmeasured loss's duration in frames: no WAV frame is skipped, and
    /// every later stamp is this much later.
    #[serde(default)]
    pub elapsed_frames: Option<u64>,
    #[serde(default)]
    pub detail: Option<String>,
}

/// How the capture ends.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndSpec {
    pub reason: StreamEndReason,
    #[serde(default)]
    pub detail: Option<String>,
}

/// What was written.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Written {
    pub record_id: String,
    /// The index after the last frame the log accounts for.
    pub frames: u64,
}

/// Why `write-capture` wrote no log. Only [`WriteCaptureError::Incomplete`]
/// leaves one behind.
#[derive(Debug)]
pub enum WriteCaptureError {
    /// The spec could not be read or was refused.
    Spec(String),
    /// The WAV file could not be read or was refused.
    Wav(WavError),
    /// Creating or writing the log failed.
    Output(io::Error),
    /// The write failed after the log was created, and the log could not be
    /// removed.
    Incomplete {
        error: Box<WriteCaptureError>,
        remove: io::Error,
    },
}

impl fmt::Display for WriteCaptureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteCaptureError::Spec(why) => write!(f, "spec refused: {why}"),
            WriteCaptureError::Wav(e) => e.fmt(f),
            WriteCaptureError::Output(e) => write!(f, "writing the log: {e}"),
            WriteCaptureError::Incomplete { error, remove } => write!(
                f,
                "{error}; the incomplete log could not be removed and must not be used: {remove}"
            ),
        }
    }
}

impl std::error::Error for WriteCaptureError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WriteCaptureError::Wav(e) => Some(e),
            WriteCaptureError::Output(e) => Some(e),
            WriteCaptureError::Incomplete { error, .. } => Some(error.as_ref()),
            WriteCaptureError::Spec(_) => None,
        }
    }
}

impl NewFileError for WriteCaptureError {
    fn output(e: io::Error) -> Self {
        WriteCaptureError::Output(e)
    }

    fn incomplete(error: Self, remove: io::Error) -> Self {
        WriteCaptureError::Incomplete {
            error: Box::new(error),
            remove,
        }
    }
}

fn spec_err<T>(why: String) -> Result<T, WriteCaptureError> {
    Err(WriteCaptureError::Spec(why))
}

/// A loss, checked against the WAV.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    Measured { frames: u64 },
    Unmeasured { elapsed_frames: u64 },
}

/// The stamps of a capture delivered at exactly the nominal rate.
#[derive(Debug, Clone, Copy)]
struct Timeline {
    rate_hz: u32,
    start: StartPair,
}

impl Timeline {
    /// `T(k)`: the instant after `k` frames, with `elapsed` frames of
    /// unmeasured holes before them, as `(mono_us, wall_us)`. `None` past
    /// either clock's range.
    fn at(&self, k: u64, elapsed: u64) -> Option<(u64, u64)> {
        let frames = u128::from(k) + u128::from(elapsed);
        let since = frames * 1_000_000 / u128::from(self.rate_hz);
        let mono = u64::try_from(u128::from(self.start.mono_us) + since).ok()?;
        let wall = u64::try_from(u128::from(self.start.wall_us) + since).ok()?;
        Some((mono, wall))
    }
}

/// Check every loss against the WAV's `frames` and each other, in order.
fn shapes(losses: &[LossSpec], frames: u64) -> Result<Vec<Shape>, WriteCaptureError> {
    let mut end = 0u64;
    let mut out = Vec::with_capacity(losses.len());
    for (i, l) in losses.iter().enumerate() {
        let shape = match (l.cause, l.frames, l.elapsed_frames) {
            (LossCause::WriterBacklog, Some(n), None) => Shape::Measured { frames: n },
            (LossCause::WriterBacklog, _, _) => {
                return spec_err(format!(
                    "losses[{i}]: writer_backlog is measured and takes frames, not elapsed_frames"
                ));
            }
            (_, None, Some(m)) => Shape::Unmeasured { elapsed_frames: m },
            _ => {
                return spec_err(format!(
                    "losses[{i}]: its cause is unmeasured and takes elapsed_frames, not frames"
                ));
            }
        };
        if l.at_frame < end {
            return spec_err(format!(
                "losses[{i}] at frame {} is before the previous loss's end at {end}",
                l.at_frame
            ));
        }
        if l.at_frame > frames {
            return spec_err(format!(
                "losses[{i}] at frame {} is past the WAV's {frames} frames",
                l.at_frame
            ));
        }
        end = match shape {
            Shape::Measured { frames: 0 } => {
                return spec_err(format!("losses[{i}] measures 0 frames"));
            }
            Shape::Measured { frames: n } => match l.at_frame.checked_add(n) {
                Some(e) if e <= frames => e,
                _ => {
                    return spec_err(format!(
                        "losses[{i}]: {n} frames at {} reach past the WAV's {frames} frames",
                        l.at_frame
                    ));
                }
            },
            Shape::Unmeasured { .. } => l.at_frame,
        };
        out.push(shape);
    }
    Ok(out)
}

/// Read the spec at `path`.
fn read_spec(path: &Path) -> Result<Spec, WriteCaptureError> {
    let file = File::open(path)
        .map_err(|e| WriteCaptureError::Spec(format!("opening {}: {e}", path.display())))?;
    serde_json::from_reader(BufReader::new(file))
        .map_err(|e| WriteCaptureError::Spec(e.to_string()))
}

/// Write the capture log of the WAV file at `wav` and the spec at `spec` to
/// `out`, which must not exist. Everything the spec and the WAV's header can
/// be refused for is refused before `out` is created; on a later failure `out`
/// is removed, unless the error is [`WriteCaptureError::Incomplete`].
pub fn write_capture(spec: &Path, wav: &Path, out: &Path) -> Result<Written, WriteCaptureError> {
    let spec = read_spec(spec)?;
    let mut reader = WavReader::open(wav).map_err(WriteCaptureError::Wav)?;
    let format = reader.format();
    let frames = reader.frames();

    if spec.pod_id.is_empty() {
        return spec_err("pod_id is empty".to_owned());
    }
    if spec.layout.len() != usize::from(format.channels) {
        return spec_err(format!(
            "layout names {} channels and the WAV has {}",
            spec.layout.len(),
            format.channels
        ));
    }
    let granted = Granted {
        rate_hz: format.rate_hz,
        channels: u32::from(format.channels),
        sample_format: format.sample_format,
        period_frames: spec.period_frames,
        buffer_frames: spec.buffer_frames,
    };
    if spec.period_frames as usize > granted.max_block_frames() {
        return spec_err(format!(
            "period_frames {} is more than the {} frames one audio frame holds",
            spec.period_frames,
            granted.max_block_frames()
        ));
    }
    let shapes = shapes(&spec.losses, frames)?;
    let elapsed: u64 = shapes
        .iter()
        .map(|s| match s {
            Shape::Measured { .. } => 0,
            Shape::Unmeasured { elapsed_frames } => *elapsed_frames,
        })
        .try_fold(0u64, |a, m| a.checked_add(m))
        .ok_or_else(|| WriteCaptureError::Spec("elapsed_frames sum past 2^64".to_owned()))?;
    let timeline = Timeline {
        rate_hz: format.rate_hz,
        start: spec.start,
    };
    if timeline.at(frames, elapsed).is_none() {
        return spec_err("the capture's last stamp is past the clocks' range".to_owned());
    }
    let capture = CaptureMeta {
        record_id: spec.record_id.clone(),
        writer: WriterInfo {
            program: "frame-log".to_owned(),
            build: WriterBuild {
                commit: spec.commit.clone(),
                dirty: None,
                source: BuildSource::Caller,
            },
        },
        device: spec.device.clone(),
        granted: CaptureGranted {
            period_frames: spec.period_frames,
            buffer_frames: spec.buffer_frames,
        },
        clocks: Clocks::v1(),
        device_control: DeviceControl { performed: false },
        device_state: spec.device_state.clone(),
    };
    capture
        .validate()
        .map_err(|e| WriteCaptureError::Spec(e.to_string()))?;
    let meta = LogMeta {
        build_id: spec.commit.clone(),
        created_epoch_us: HostMicros(spec.start.wall_us),
        conn_seq: 0,
        rolled_from: None,
        capture: Some(capture),
    };
    let prelude = prelude(
        &spec.pod_id,
        &granted,
        &spec.layout,
        spec.start.mono_us,
        spec.start.wall_us,
    )
    .map_err(WriteCaptureError::Spec)?;

    let written_to = write_new(out, |file| {
        let mut w = FrameWriter::new(file, &meta, &prelude, Framer::new(&granted))
            .map_err(WriteCaptureError::Output)?;
        let mut stream = Stream {
            w: &mut w,
            reader: &mut reader,
            timeline,
            period: u64::from(spec.period_frames),
            next: 0,
            elapsed: 0,
        };
        for (loss, shape) in spec.losses.iter().zip(&shapes) {
            stream.loss(loss, *shape)?;
        }
        stream.blocks_to(frames)?;
        if let Some(end) = &spec.end {
            stream.end(end)?;
        }
        Ok(w.position())
    })?;
    Ok(Written {
        record_id: spec.record_id,
        frames: written_to,
    })
}

/// The write in progress.
struct Stream<'a, W: io::Write, R> {
    w: &'a mut FrameWriter<W>,
    reader: &'a mut WavReader<R>,
    timeline: Timeline,
    period: u64,
    /// The next WAV frame to read.
    next: u64,
    /// Frames of unmeasured holes so far.
    elapsed: u64,
}

impl<W: io::Write, R: io::Read + io::Seek> Stream<'_, W, R> {
    fn stamp(&self, k: u64) -> (u64, u64) {
        self.timeline
            .at(k, self.elapsed)
            .expect("every stamp was checked against the clocks' range")
    }

    fn read(&mut self, frames: u64) -> Result<Vec<u8>, WriteCaptureError> {
        self.reader
            .read_frames(frames)
            .map_err(|e| WriteCaptureError::Wav(WavError::Io(e)))
    }

    /// Write blocks of WAV frames from the next one up to `to`.
    fn blocks_to(&mut self, to: u64) -> Result<(), WriteCaptureError> {
        while self.next < to {
            let n = self.period.min(to - self.next);
            let pcm = self.read(n)?;
            let (mono_us, wall_us) = self.stamp(self.next + n);
            self.w
                .block(Block {
                    mono_us,
                    wall_us,
                    first_frame: self.next,
                    backlog_frames: 0,
                    pcm,
                })
                .map_err(WriteCaptureError::Output)?;
            self.next += n;
        }
        Ok(())
    }

    fn loss(&mut self, loss: &LossSpec, shape: Shape) -> Result<(), WriteCaptureError> {
        let at = loss.at_frame;
        self.blocks_to(at)?;
        let (frames, stamp_at) = match shape {
            Shape::Measured { frames } => {
                self.reader
                    .skip_frames(frames)
                    .map_err(|e| WriteCaptureError::Wav(WavError::Io(e)))?;
                self.next = at + frames;
                (Some(frames), at + frames)
            }
            Shape::Unmeasured { elapsed_frames } => {
                self.elapsed += elapsed_frames;
                (None, at)
            }
        };
        let (mono_us, wall_us) = self.stamp(stamp_at);
        self.w
            .event(
                mono_us,
                Event::Gap(Gap {
                    cause: loss.cause,
                    at_frame: at,
                    frames,
                    wall_us,
                    detail: loss.detail.clone().unwrap_or_default(),
                }),
            )
            .map_err(WriteCaptureError::Output)
    }

    fn end(&mut self, end: &EndSpec) -> Result<(), WriteCaptureError> {
        let at_frame = self.w.position();
        let (mono_us, wall_us) = self.stamp(at_frame);
        self.w
            .event(
                mono_us,
                Event::End(End {
                    reason: end.reason,
                    at_frame,
                    wall_us,
                    detail: end.detail.clone().unwrap_or_default(),
                }),
            )
            .map_err(WriteCaptureError::Output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wav::tests::pcm_wav;
    use serde_json::{Value, json};

    fn spec() -> Value {
        json!({
            "pod_id": "bench",
            "layout": ["l", "r"],
            "period_frames": 4,
            "buffer_frames": 64,
            "start": {"mono_us": 1_000, "wall_us": 5_000_000},
            "record_id": "9f0c1d2e3f405162738495a6b7c8d9e0",
            "commit": "0123456789abcdef0123456789abcdef01234567",
            "device": {"selector": "synthetic", "card_index": 0, "card_name": "none",
                       "card_long_name": "none", "pcm": "none", "usb_id": null}
        })
    }

    struct Run {
        dir: tempfile::TempDir,
    }

    impl Run {
        fn out(&self) -> std::path::PathBuf {
            self.dir.path().join("out.framelog")
        }
    }

    /// Write `spec` and a two-channel 16-bit WAV of `frames` frames, then run.
    fn run(spec: &Value, frames: usize) -> (Run, Result<Written, WriteCaptureError>) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("spec.json"), spec.to_string()).unwrap();
        let data: Vec<u8> = (0..frames * 4).map(|i| i as u8).collect();
        std::fs::write(dir.path().join("in.wav"), pcm_wav(2, 1000, 16, &data)).unwrap();
        let r = Run { dir };
        let result = write_capture(
            &r.dir.path().join("spec.json"),
            &r.dir.path().join("in.wav"),
            &r.out(),
        );
        (r, result)
    }

    fn refused(spec: &Value, frames: usize, needle: &str) {
        let (r, result) = run(spec, frames);
        match result {
            Err(WriteCaptureError::Spec(why)) => {
                assert!(why.contains(needle), "{why} lacks {needle:?}")
            }
            other => panic!("expected a spec refusal naming {needle:?}, got {other:?}"),
        }
        assert!(!r.out().exists());
    }

    fn with(key: &str, value: Value) -> Value {
        let mut s = spec();
        s[key] = value;
        s
    }

    #[test]
    fn a_plain_spec_writes_and_reports_its_frames() {
        let (r, result) = run(&spec(), 10);
        let written = result.unwrap();
        assert_eq!(written.frames, 10);
        assert_eq!(written.record_id, "9f0c1d2e3f405162738495a6b7c8d9e0");
        assert!(r.out().exists());
    }

    #[test]
    fn the_spec_is_refused_for_each_bad_shape() {
        refused(&with("extra", json!(1)), 10, "unknown field `extra`");
        refused(
            &with("layout", json!(["only"])),
            10,
            "layout names 1 channels",
        );
        refused(
            &with("layout", json!(["l", ""])),
            10,
            "empty name at channel 1",
        );
        refused(&with("layout", json!(["l", "l"])), 10, "names \"l\" twice");
        refused(&with("pod_id", json!("")), 10, "pod_id is empty");
        refused(
            &with("pod_id", json!("p".repeat(33))),
            10,
            "longer than 32 bytes",
        );
        refused(&with("record_id", json!("ABC")), 10, "record_id");
        refused(&with("commit", json!("abc")), 10, "commit");
        refused(&with("period_frames", json!(0)), 10, "period_frames is 0");
        refused(&with("period_frames", json!(65)), 10, "under period_frames");
        let mut big = with("period_frames", json!(20_000));
        big["buffer_frames"] = json!(20_000);
        refused(&big, 10, "frames one audio frame holds");
        refused(&with("end", json!({"reason": "exploded"})), 10, "exploded");
    }

    #[test]
    fn each_bad_loss_is_refused() {
        let losses = |l: Value| with("losses", l);
        refused(
            &losses(json!([{"at_frame": 1, "cause": "writer_backlog", "elapsed_frames": 2}])),
            10,
            "writer_backlog is measured",
        );
        refused(
            &losses(json!([{"at_frame": 1, "cause": "overrun", "frames": 2}])),
            10,
            "unmeasured",
        );
        refused(
            &losses(json!([{"at_frame": 1, "cause": "writer_backlog", "frames": 0}])),
            10,
            "measures 0 frames",
        );
        refused(
            &losses(
                json!([{"at_frame": 1, "cause": "writer_backlog", "frames": 3},
                           {"at_frame": 3, "cause": "suspend", "elapsed_frames": 1}]),
            ),
            10,
            "before the previous loss's end at 4",
        );
        refused(
            &losses(json!([{"at_frame": 8, "cause": "writer_backlog", "frames": 3}])),
            10,
            "reach past the WAV's 10 frames",
        );
        refused(
            &losses(json!([{"at_frame": 11, "cause": "overrun", "elapsed_frames": 1}])),
            10,
            "past the WAV's 10 frames",
        );
        refused(
            &losses(json!([{"at_frame": 1, "cause": "overrun", "elapsed_frames": 1, "x": 0}])),
            10,
            "unknown field `x`",
        );
    }

    #[test]
    fn an_unmeasured_loss_at_the_wav_s_length_is_allowed() {
        let s = with(
            "losses",
            json!([{"at_frame": 10, "cause": "transfer_error", "elapsed_frames": 3}]),
        );
        let (_r, result) = run(&s, 10);
        assert_eq!(result.unwrap().frames, 10);
    }

    #[test]
    fn a_stamp_past_the_clock_s_range_is_refused() {
        let s = with("start", json!({"mono_us": u64::MAX - 5, "wall_us": 0}));
        refused(&s, 10, "past the clocks' range");
    }

    #[test]
    fn an_existing_output_is_not_touched() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("spec.json"), spec().to_string()).unwrap();
        std::fs::write(dir.path().join("in.wav"), pcm_wav(2, 1000, 16, &[0; 8])).unwrap();
        let out = dir.path().join("out");
        std::fs::write(&out, b"mine").unwrap();
        let err = write_capture(
            &dir.path().join("spec.json"),
            &dir.path().join("in.wav"),
            &out,
        )
        .unwrap_err();
        assert!(matches!(err, WriteCaptureError::Output(_)), "{err:?}");
        assert_eq!(std::fs::read(&out).unwrap(), b"mine");
    }

    #[test]
    fn the_timeline_counts_unmeasured_holes() {
        let t = Timeline {
            rate_hz: 3,
            start: StartPair {
                mono_us: 10,
                wall_us: 100,
            },
        };
        assert_eq!(t.at(0, 0), Some((10, 100)));
        assert_eq!(t.at(1, 0), Some((10 + 333_333, 100 + 333_333)));
        assert_eq!(t.at(1, 2), Some((1_000_010, 1_000_100)));
    }
}
