//! Splice one frame log's audio into an output buffer by absolute sample
//! index. Replays the log through the same decode + FSM path live ingest
//! uses (`FrameLogReader` → `decode_frame` → `SessionFsm`), consuming
//! `SessionEvent::Audio` and copying each event's samples into the caller's
//! buffer at `first_sample_index - start_sample`. Placement is per-audio-event
//! rather than per-accumulated-segment, so intra-segment wire gaps splice as
//! silence at the correct offsets automatically (the caller pre-fills
//! silence; untouched slots stay whatever the caller gave).

use audio_pipeline::wire::decode_frame;
use serde::Serialize;

use crate::framelog::{FrameLogError, FrameLogReader, LogItem};
use crate::session::{FormatConstraint, ResumeLedger, SessionEvent, SessionFsm};

/// Why a log's replay stopped before clean EOF. The samples spliced so far
/// are kept; the remainder of the requested range stays whatever the caller
/// pre-filled (silence). A data-bearing variant serializes as an object
/// (`{"corrupt": "..."}`); a unit variant serializes as a snake_case string.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SpliceStop {
    /// The final record was cut short (writer crashed mid-write).
    TornTail,
    /// A corrupt record length, a mid-log read error, or an undecodable
    /// frame.
    Corrupt(String),
    /// `SessionFsm` parked on a fatal protocol error.
    ProtocolFatal,
}

/// Outcome of splicing one open frame log into an output buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpliceOutcome {
    /// Samples written into `out` from this log (real audio, not silence).
    pub samples_written: u64,
    /// `Some` when replay ended early; `None` on clean EOF or on cleanly
    /// reaching the end of the requested range.
    pub stopped: Option<SpliceStop>,
    /// Non-fatal `SessionEvent::ProtocolError` events the FSM emitted while
    /// replaying (a frame it rejected without parking, e.g. audio for an
    /// unknown segment). Their samples never reached `out`, so that range
    /// stays silence; without this count that hole is indistinguishable from
    /// a genuine wire gap.
    pub protocol_errors: u64,
}

/// Replay `reader` through the live decode path and copy every audio sample
/// whose absolute index falls in `[start_sample, start_sample + out.len())`
/// into `out` at `index - start_sample`. Untouched slots are left as given —
/// the caller pre-fills silence.
///
/// Frames before the range are still fully decoded and fed to the FSM
/// (session state must stay valid, and there is no index to seek by); only
/// the copy into `out` is skipped. Replay stops as soon as an `Audio` event's
/// `first_sample_index` reaches the range's end: a log holds one connection
/// whose sample counter only jumps forward on a gap in healthy operation, so
/// scanning the rest of a long log for a short span would decode a large,
/// useless tail. A malformed log with a backward jump could carry in-range
/// audio after the stop point; that audio is forfeited (a protocol anomaly,
/// not a case worth scanning every log to EOF for).
///
/// Returns `Err` on a genuine I/O fault mid-read (`FrameLogError::Io` — an
/// `EIO`, a mount dropping between records): this is the same fault class
/// `resolve_open` rejects at open time, so it must never be folded into a
/// diagnostic-only `SpliceStop` and read as routine truncation. Structural
/// corruption (a bad record length, an undecodable frame, a torn tail)
/// legitimately stays partial-beats-nothing and reports via `SpliceOutcome`.
pub fn splice_log_into(
    reader: FrameLogReader,
    constraint: FormatConstraint,
    start_sample: u64,
    out: &mut [i16],
) -> Result<SpliceOutcome, FrameLogError> {
    let end_sample = start_sample.saturating_add(out.len() as u64);
    let mut fsm = SessionFsm::new(constraint, ResumeLedger::shared());
    let mut samples_written = 0u64;
    let mut protocol_errors = 0u64;

    for item in reader {
        let (host_rx, payload) = match item {
            Ok(LogItem::Record { host_rx, payload }) => (host_rx, payload),
            Ok(LogItem::TornTail) => {
                return Ok(SpliceOutcome {
                    samples_written,
                    stopped: Some(SpliceStop::TornTail),
                    protocol_errors,
                });
            }
            Err(e @ FrameLogError::Io(_)) => return Err(e),
            Err(e) => {
                return Ok(SpliceOutcome {
                    samples_written,
                    stopped: Some(SpliceStop::Corrupt(e.to_string())),
                    protocol_errors,
                });
            }
        };

        let frame = match decode_frame(&payload) {
            Ok(f) => f,
            Err(e) => {
                return Ok(SpliceOutcome {
                    samples_written,
                    stopped: Some(SpliceStop::Corrupt(format!("{e:?}"))),
                    protocol_errors,
                });
            }
        };

        for ev in fsm.feed(frame, host_rx) {
            match ev {
                SessionEvent::Audio {
                    first_sample_index,
                    pcm,
                    ..
                } => {
                    if first_sample_index >= end_sample {
                        return Ok(SpliceOutcome {
                            samples_written,
                            stopped: None,
                            protocol_errors,
                        });
                    }
                    splice_event(
                        first_sample_index,
                        &pcm,
                        start_sample,
                        end_sample,
                        out,
                        &mut samples_written,
                    );
                }
                SessionEvent::ProtocolError { fatal: true, .. } => {
                    return Ok(SpliceOutcome {
                        samples_written,
                        stopped: Some(SpliceStop::ProtocolFatal),
                        protocol_errors,
                    });
                }
                SessionEvent::ProtocolError { fatal: false, .. } => {
                    protocol_errors += 1;
                }
                _ => {}
            }
        }
    }

    Ok(SpliceOutcome {
        samples_written,
        stopped: None,
        protocol_errors,
    })
}

/// Copy the overlap between an audio event's `[first_sample_index,
/// first_sample_index + pcm.len())` and the requested `[start_sample,
/// end_sample)` into `out`, accumulating the copied count.
fn splice_event(
    first_sample_index: u64,
    pcm: &[i16],
    start_sample: u64,
    end_sample: u64,
    out: &mut [i16],
    samples_written: &mut u64,
) {
    let event_end = first_sample_index.saturating_add(pcm.len() as u64);
    if event_end <= start_sample || first_sample_index >= end_sample {
        return;
    }
    let overlap_start = first_sample_index.max(start_sample);
    let overlap_end = event_end.min(end_sample);
    let src_offset = (overlap_start - first_sample_index) as usize;
    let dst_offset = (overlap_start - start_sample) as usize;
    let len = (overlap_end - overlap_start) as usize;
    out[dst_offset..dst_offset + len].copy_from_slice(&pcm[src_offset..src_offset + len]);
    *samples_written += len as u64;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::HostMicros;
    use crate::framelog::FrameLogWriter;
    use crate::session::Codec;
    use crate::test_fixtures::{audio, framed, meta, seg_end, seg_start, write_log};
    use audio_pipeline::wire::{
        ChannelSource, Hello, StreamFrame, AUDIO_PROTOCOL_VERSION, MAX_FRAME_BYTES,
    };
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::path::Path;

    const FORMAT: FormatConstraint = FormatConstraint {
        sample_rate_hz: 16_000,
        bits_per_sample: 16,
        channels: 1,
        codec: Codec::S16Le,
        mono_beam_only: true,
    };

    fn hello() -> StreamFrame {
        crate::test_fixtures::hello("pod-splice")
    }

    fn open(path: &Path) -> FrameLogReader {
        FrameLogReader::open(path).unwrap()
    }

    #[test]
    fn whole_segment_exact_samples_at_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.framelog");
        write_log(
            &path,
            &[hello(), seg_start(1, 0), audio(1, 0, 320), seg_end(1, 320)],
        );

        let mut out = vec![0i16; 320];
        let outcome = splice_log_into(open(&path), FORMAT, 0, &mut out).unwrap();
        assert_eq!(
            outcome,
            SpliceOutcome {
                samples_written: 320,
                stopped: None,
                protocol_errors: 0,
            }
        );
        assert_eq!(out[0], 1);
        assert_eq!(out[319], 320);
    }

    #[test]
    fn mid_segment_slice_interior_range() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.framelog");
        write_log(
            &path,
            &[hello(), seg_start(1, 0), audio(1, 0, 320), seg_end(1, 320)],
        );

        let mut out = vec![0i16; 100];
        let outcome = splice_log_into(open(&path), FORMAT, 50, &mut out).unwrap();
        assert_eq!(outcome.samples_written, 100);
        assert_eq!(outcome.stopped, None);
        assert_eq!(out[0], 51);
        assert_eq!(out[99], 150);
    }

    #[test]
    fn intra_segment_gap_is_silence_at_correct_offset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.framelog");
        write_log(
            &path,
            &[
                hello(),
                seg_start(1, 0),
                audio(1, 0, 100),
                // Gap: next frame starts at 200 instead of the expected 100.
                audio(1, 200, 100),
            ],
        );

        let mut out = vec![0i16; 300];
        let outcome = splice_log_into(open(&path), FORMAT, 0, &mut out).unwrap();
        assert_eq!(outcome.samples_written, 200);
        assert_eq!(outcome.stopped, None);
        assert_eq!(out[0], 1);
        assert_eq!(out[99], 100);
        assert_eq!(&out[100..200], &[0i16; 100][..]);
        assert_eq!(out[200], 1);
        assert_eq!(out[299], 100);
    }

    #[test]
    fn head_and_tail_beyond_recorded_audio_are_silence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.framelog");
        write_log(
            &path,
            &[
                hello(),
                seg_start(1, 100),
                audio(1, 100, 50),
                seg_end(1, 50),
            ],
        );

        // Span [0, 200): head [0,100) and tail [150,200) are unrecorded.
        let mut out = vec![0i16; 200];
        let outcome = splice_log_into(open(&path), FORMAT, 0, &mut out).unwrap();
        assert_eq!(outcome.samples_written, 50);
        assert_eq!(outcome.stopped, None);
        assert_eq!(&out[0..100], &[0i16; 100][..]);
        assert_eq!(out[100], 1);
        assert_eq!(out[149], 50);
        assert_eq!(&out[150..200], &[0i16; 50][..]);
    }

    #[test]
    fn torn_tail_keeps_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.framelog");
        write_log(&path, &[hello(), seg_start(1, 0), audio(1, 0, 320)]);

        // Append a partial record header to simulate a writer crash mid-write.
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[0xAA, 0xBB, 0xCC])
            .unwrap();

        let mut out = vec![0i16; 320];
        let outcome = splice_log_into(open(&path), FORMAT, 0, &mut out).unwrap();
        assert_eq!(outcome.samples_written, 320);
        assert_eq!(outcome.stopped, Some(SpliceStop::TornTail));
        assert_eq!(out[0], 1);
    }

    #[test]
    fn corrupt_record_length_stops_and_keeps_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.framelog");
        write_log(&path, &[hello(), seg_start(1, 0), audio(1, 0, 320)]);

        // Append a raw record header claiming an out-of-range length.
        let mut extra = Vec::new();
        extra.extend_from_slice(&99u64.to_le_bytes());
        extra.extend_from_slice(&((MAX_FRAME_BYTES + 1) as u16).to_le_bytes());
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&extra)
            .unwrap();

        let mut out = vec![0i16; 320];
        let outcome = splice_log_into(open(&path), FORMAT, 0, &mut out).unwrap();
        assert_eq!(outcome.samples_written, 320);
        assert!(matches!(outcome.stopped, Some(SpliceStop::Corrupt(_))));
    }

    #[test]
    fn undecodable_frame_stops_and_keeps_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.framelog");
        let mut w = FrameLogWriter::create(&path, meta(1_700_000_000_000_000)).unwrap();
        w.append(HostMicros(0), &framed(&hello())).unwrap();
        w.append(HostMicros(1), &framed(&seg_start(1, 0))).unwrap();
        w.append(HostMicros(2), &framed(&audio(1, 0, 320))).unwrap();
        // A well-formed-length record whose payload does not decode.
        w.append(HostMicros(3), &[0xFF; 8]).unwrap();
        w.finish().unwrap();

        let mut out = vec![0i16; 320];
        let outcome = splice_log_into(open(&path), FORMAT, 0, &mut out).unwrap();
        assert_eq!(outcome.samples_written, 320);
        assert!(matches!(outcome.stopped, Some(SpliceStop::Corrupt(_))));
    }

    #[test]
    fn early_termination_before_post_span_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.framelog");
        let mut w = FrameLogWriter::create(&path, meta(1_700_000_000_000_000)).unwrap();
        w.append(HostMicros(0), &framed(&hello())).unwrap();
        w.append(HostMicros(1), &framed(&seg_start(1, 0))).unwrap();
        // First frame covers the requested [0, 100) range and beyond.
        w.append(HostMicros(2), &framed(&audio(1, 0, 150))).unwrap();
        // Second frame's own start index (150) is past the range end (100) —
        // its arrival must stop replay before the corrupt record after it.
        w.append(HostMicros(3), &framed(&audio(1, 150, 170)))
            .unwrap();
        // Corrupt record planted after the span end — must never be reached.
        w.append(HostMicros(4), &[0xFF; 8]).unwrap();
        w.finish().unwrap();

        let mut out = vec![0i16; 100];
        let outcome = splice_log_into(open(&path), FORMAT, 0, &mut out).unwrap();
        assert_eq!(outcome.samples_written, 100);
        assert_eq!(
            outcome.stopped, None,
            "must stop cleanly before reaching post-span corruption"
        );
    }

    #[test]
    fn protocol_fatal_stops_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.framelog");
        // A fatal `ProtocolError` (`NotHelloFirst`/`VersionMismatch`/
        // `FormatMismatch`) can only fire on the very first frame a fresh
        // `SessionFsm` sees, in `feed_await_hello` — every later mismatch
        // (a second Hello, a format change mid-connection) is a *nonfatal*
        // event instead. So a log driving this branch necessarily has zero
        // audio before the stop: the Hello itself is the offending frame.
        let mismatched_hello = StreamFrame::Hello(Hello {
            version: AUDIO_PROTOCOL_VERSION,
            pod_id: heapless::String::try_from("pod-splice").unwrap(),
            sample_rate_hz: 16_000,
            bits_per_sample: 16,
            channels: 1,
            codec: Codec::S16Le,
            // `FORMAT.mono_beam_only` rejects a stereo channel source.
            channel_source: ChannelSource::Stereo,
        });
        write_log(&path, &[mismatched_hello]);

        let mut out = vec![0i16; 100];
        let outcome = splice_log_into(open(&path), FORMAT, 0, &mut out).unwrap();
        assert_eq!(outcome.samples_written, 0);
        assert_eq!(outcome.stopped, Some(SpliceStop::ProtocolFatal));
    }

    #[test]
    fn nonfatal_protocol_errors_are_counted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.framelog");
        write_log(
            &path,
            &[
                hello(),
                // Audio with no open segment: rejected as a nonfatal
                // `AudioOutsideSegment`, replay continues.
                audio(1, 0, 10),
                seg_start(1, 0),
                audio(1, 0, 320),
                seg_end(1, 320),
            ],
        );

        let mut out = vec![0i16; 320];
        let outcome = splice_log_into(open(&path), FORMAT, 0, &mut out).unwrap();
        assert_eq!(outcome.samples_written, 320);
        assert_eq!(outcome.stopped, None);
        assert_eq!(outcome.protocol_errors, 1);
    }
}
