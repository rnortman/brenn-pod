//! The event lines on stdout: one `pod-jsonl` line per event. Prose goes to
//! stderr. A reader may act on each line alone; what it may conclude is written
//! on each struct.

use std::io::Write;

use capture_record::{Device, Gap, GapCause, Granted, WriterBuild};
use serde::Serialize;

/// Writes event lines. A stdout the caller has closed does not stop the capture:
/// the first failed write is noted on stderr, and later lines are not attempted.
pub struct Lines<W: Write> {
    w: W,
    broken: bool,
}

impl<W: Write> Lines<W> {
    pub fn new(w: W) -> Self {
        Lines { w, broken: false }
    }

    /// Write one line, stamped with `pod_jsonl::now_ms`, and flush it.
    pub fn emit<T: Serialize>(&mut self, event: &str, fields: &T) {
        if self.broken {
            return;
        }
        let line = pod_jsonl::format_line_at(pod_jsonl::now_ms(), event, fields);
        let written = writeln!(self.w, "{line}").and_then(|()| self.w.flush());
        if let Err(e) = written {
            self.broken = true;
            note(format_args!(
                "writing event lines failed ({e}); no further lines are written, and the \
                 capture goes on"
            ));
        }
    }

    pub fn into_inner(self) -> W {
        self.w
    }
}

/// One line of prose on stderr. A stderr that cannot be written (its reader gone,
/// as when the caller that held stdout too has gone) is ignored: prose never
/// stops the capture or costs it its terminal line.
pub fn note(args: std::fmt::Arguments<'_>) {
    let _ = writeln!(std::io::stderr(), "pod-capture: {args}");
}

/// `capture_started`: the destination exists with its header and the stream is
/// running. The pair is the run's start anchor for this device.
#[derive(Debug, Serialize)]
pub struct CaptureStarted<'a> {
    pub record_id: &'a str,
    pub path: &'a str,
    pub device: &'a Device,
    pub granted: &'a Granted,
    pub layout: &'a [String],
    pub build: &'a WriterBuild,
    /// Always 0: the position the pair anchors.
    pub frame: u64,
    pub mono_us: u64,
    pub wall_us: u64,
}

/// `capture_gap`: audio is missing at `at_frame`, and the same fact is in the
/// record. An unmeasured gap is announced when it happens; a `writer_backlog`
/// gap when it is queued, which is when its length is final.
#[derive(Debug, Serialize)]
pub struct CaptureGap<'a> {
    pub record_id: &'a str,
    pub cause: GapCause,
    pub at_frame: u64,
    /// `null` when the hole was not measured.
    pub frames: Option<u64>,
    pub mono_us: u64,
    pub wall_us: u64,
    pub detail: &'a str,
}

impl<'a> CaptureGap<'a> {
    /// The line for a gap the record holds, stamped `mono_us` as its event is.
    pub fn of(record_id: &'a str, mono_us: u64, gap: &'a Gap) -> Self {
        CaptureGap {
            record_id,
            cause: gap.cause,
            at_frame: gap.at_frame,
            frames: gap.frames,
            mono_us,
            wall_us: gap.wall_us,
            detail: &gap.detail,
        }
    }
}

/// `capture_late`: the reader fell a period or more behind here. Nothing was
/// lost. One line at the start of each run of late blocks the record holds,
/// emitted once the run's first block is queued for the record; a gap ends a run.
#[derive(Debug, Serialize)]
pub struct CaptureLate<'a> {
    pub record_id: &'a str,
    pub at_frame: u64,
    pub backlog_frames: u32,
    pub mono_us: u64,
    pub wall_us: u64,
}

/// `capture_stopped`: the record ends with `end`/`stopped`, is synced and
/// closed, and holds every delivered frame outside its gaps. Exit status 0
/// follows.
#[derive(Debug, Serialize)]
pub struct CaptureStopped<'a> {
    pub record_id: &'a str,
    /// The final delivered-frame position.
    pub frames: u64,
    /// Gap records in the record.
    pub gaps: u64,
    pub mono_us: u64,
    pub wall_us: u64,
}

/// `capture_failed`: the capture is over and did not stop cleanly. Exit status
/// 1 follows.
#[derive(Debug, Serialize)]
pub struct CaptureFailed<'a> {
    /// `null` before a record exists.
    pub record_id: Option<&'a str>,
    pub reason: FailReason,
    pub detail: &'a str,
    /// The delivered-frame position the capture had reached; `null` before a
    /// record exists.
    pub at_frame: Option<u64>,
    pub mono_us: u64,
    pub wall_us: u64,
    /// Every card the kernel presents, for `no_card` and `ambiguous_card` only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cards: Option<Vec<CardLine>>,
}

/// Why a capture failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailReason {
    /// `--out` already exists; nothing was opened.
    DestinationExists,
    /// No card matched `--card`.
    NoCard,
    /// More than one card matched `--card`.
    AmbiguousCard,
    /// The capture PCM would not open; a busy device says so in `detail`.
    Open,
    /// The device refused the parameters; `detail` carries what it advertises.
    Params,
    /// The destination could not be created or its header written.
    Destination,
    /// The stream failed past recovery, would not start, or delivered nothing
    /// for too long. The record ends with `end`/`stream_died` where the writer
    /// could still write it.
    StreamDied,
    /// Free space fell under the floor; the record ends with
    /// `end`/`destination_full`.
    DestinationFull,
    /// A write to the destination failed; `detail` says whether the record
    /// ends with `end`/`write_failed`.
    WriteFailed,
    /// The writer had not reported within the bound after the stream stopped.
    /// It may have been held writing a record or syncing after its end record,
    /// so whether the record ends is for its reader to say.
    WriteStalled,
}

/// One card as `capture_failed` lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CardLine {
    /// The kernel's card index, as ALSA reports it.
    pub index: i32,
    pub name: String,
    pub long_name: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stdout whose reader has gone, counting the writes attempted on it.
    #[derive(Default)]
    struct Closed {
        writes: u32,
    }

    impl Write for Closed {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            self.writes += 1;
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_line_is_one_envelope_per_event() {
        let mut lines = Lines::new(Vec::new());
        lines.emit(
            "capture_late",
            &CaptureLate {
                record_id: "ab",
                at_frame: 640,
                backlog_frames: 320,
                mono_us: 1,
                wall_us: 2,
            },
        );
        let out = String::from_utf8(lines.into_inner()).unwrap();
        let v: serde_json::Value = serde_json::from_str(out.trim_end()).unwrap();
        assert!(out.ends_with('\n'));
        assert_eq!(v["event"], "capture_late");
        assert_eq!(v["at_frame"], 640);
        assert!(v["ts_ms"].is_u64());
    }

    #[test]
    fn a_closed_stdout_is_written_once_and_not_retried() {
        let mut lines = Lines::new(Closed::default());
        let fields = CaptureStopped {
            record_id: "ab",
            frames: 0,
            gaps: 0,
            mono_us: 0,
            wall_us: 0,
        };
        lines.emit("capture_stopped", &fields);
        lines.emit("capture_stopped", &fields);
        assert_eq!(lines.into_inner().writes, 1);
    }

    #[test]
    fn cards_are_omitted_unless_given_and_reasons_are_snake_case() {
        let failed = |cards| {
            serde_json::to_value(CaptureFailed {
                record_id: None,
                reason: FailReason::AmbiguousCard,
                detail: "two",
                at_frame: None,
                mono_us: 0,
                wall_us: 0,
                cards,
            })
            .unwrap()
        };
        let none = failed(None);
        assert!(none.get("cards").is_none());
        assert_eq!(none["reason"], "ambiguous_card");
        assert!(none["record_id"].is_null());
        let some = failed(Some(vec![CardLine {
            index: 2,
            name: "n".to_owned(),
            long_name: "l".to_owned(),
        }]));
        assert_eq!(some["cards"][0]["index"], 2);
    }
}
