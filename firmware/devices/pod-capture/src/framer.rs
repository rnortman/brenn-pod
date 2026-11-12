//! The framer: the items the writer thread receives, turned into the wire
//! frames of a capture's stream, each with its record stamp. No I/O: what it
//! returns is a complete stream that any sink could take.
//!
//! ```text
//! Hello, Capture(Describe)                         the prelude
//! block            SegmentStart if none is open; Audio; Capture(Read)
//! gap, measured    Capture(Loss{writer_backlog, Some(n)})
//! gap, unmeasured  SegmentEnd{SourceLoss} if a segment is open; Capture(Loss{cause, None})
//! end              SegmentEnd{StreamEnd} if a segment is open; Capture(End)
//! ```

use audio_pipeline::wide::{
    CAPTURE_SCHEMA_VERSION, CaptureFrame, LossCause, StreamEndReason, WideAudio, WideFrame,
};
use audio_pipeline::wire::{
    AUDIO_PROTOCOL_VERSION, ChannelSource, EndReason, Hello, SegmentEnd, SegmentStart,
};
use frame_log::HostMicros;

use crate::granted::Granted;

/// The most bytes of a `detail` a `Loss` or `End` frame carries; a longer one
/// is cut at a character boundary.
pub const MAX_DETAIL_BYTES: usize = 4096;

/// Audio is missing at `at_frame`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Gap {
    pub cause: LossCause,
    pub at_frame: u64,
    /// The exact length when the device delivered the frames and they were
    /// dropped (`writer_backlog`); `None` when the device lost them.
    pub frames: Option<u64>,
    pub wall_us: u64,
    pub detail: String,
}

/// The capture ended itself at `at_frame`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct End {
    pub reason: StreamEndReason,
    pub at_frame: u64,
    pub wall_us: u64,
    pub detail: String,
}

/// An event the writer receives, stamped `mono_us`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Gap(Gap),
    End(End),
}

/// One ALSA read as the writer receives it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    /// When the read returned.
    pub mono_us: u64,
    pub wall_us: u64,
    pub first_frame: u64,
    /// Frames already waiting in the device's buffer when the read returned.
    pub backlog_frames: u32,
    /// Whole interleaved frames.
    pub pcm: Vec<u8>,
}

/// A frame and the stamp its record carries.
pub type Stamped = (HostMicros, WideFrame);

/// The stream's opening frames: the hello and the `Describe`, both stamped
/// with the start pair's wall reading.
pub fn prelude(
    pod_id: &str,
    granted: &Granted,
    layout: &[String],
    start_mono_us: u64,
    start_wall_us: u64,
) -> Result<[Stamped; 2], String> {
    let pod_id = pod_id
        .try_into()
        .map_err(|_| format!("pod id {pod_id:?} is longer than 32 bytes"))?;
    let channels = u8::try_from(granted.channels)
        .map_err(|_| format!("{} channels do not fit a hello", granted.channels))?;
    let (bits_per_sample, codec) = granted.sample_format.wire();
    let stamp = HostMicros(start_wall_us);
    Ok([
        (
            stamp,
            WideFrame::Hello(Hello {
                version: AUDIO_PROTOCOL_VERSION,
                pod_id,
                sample_rate_hz: granted.rate_hz,
                bits_per_sample,
                channels,
                codec,
                channel_source: ChannelSource::Described,
            }),
        ),
        (
            stamp,
            WideFrame::Capture(CaptureFrame::Describe {
                schema: CAPTURE_SCHEMA_VERSION,
                channel_names: layout.to_vec(),
                period_frames: granted.period_frames,
                mono_us: start_mono_us,
            }),
        ),
    ])
}

/// The segment open now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Open {
    id: u32,
    frames_sent: u32,
    samples_sent: u64,
}

/// The segment state of one stream. `Copy`, so the writer can keep the state
/// of the last item that reached the destination and drop the state of one
/// that did not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Framer {
    rate_hz: u32,
    frame_bytes: usize,
    next_id: u32,
    open: Option<Open>,
    /// The index after the last frame accounted for: carried or in a
    /// measured loss.
    position: u64,
}

impl Framer {
    pub fn new(granted: &Granted) -> Self {
        Framer {
            rate_hz: granted.rate_hz,
            frame_bytes: granted.frame_bytes(),
            next_id: 0,
            open: None,
            position: 0,
        }
    }

    /// The index after the last frame accounted for.
    pub fn position(&self) -> u64 {
        self.position
    }

    /// The frames for one block. `device_ts_us` of its audio is its first
    /// frame's capture time, extrapolated at the nominal rate back from the
    /// read: `mono_us − (backlog_frames + frames) × 1 000 000 / rate`.
    pub fn block(&mut self, b: Block) -> Vec<Stamped> {
        let stamp = HostMicros(b.wall_us);
        let frames = (b.pcm.len() / self.frame_bytes) as u64;
        let behind = (u128::from(b.backlog_frames) + u128::from(frames)) * 1_000_000
            / u128::from(self.rate_hz);
        let device_ts_us = b
            .mono_us
            .saturating_sub(u64::try_from(behind).unwrap_or(u64::MAX));
        let mut out = Vec::with_capacity(3);
        let mut open = match self.open {
            Some(open) => open,
            None => {
                let id = self.next_id;
                self.next_id = self.next_id.wrapping_add(1);
                out.push((
                    stamp,
                    WideFrame::SegmentStart(SegmentStart {
                        segment_id: id,
                        base_sample_index: b.first_frame,
                        base_device_ts_us: device_ts_us,
                        preroll_samples: 0,
                    }),
                ));
                Open {
                    id,
                    frames_sent: 0,
                    samples_sent: 0,
                }
            }
        };
        // A segment of more than u32::MAX audio frames (2.7 years of 20 ms
        // reads) states the most the field holds; `samples_sent` stays exact.
        open.frames_sent = open.frames_sent.saturating_add(1);
        open.samples_sent += frames;
        self.open = Some(open);
        let segment_id = open.id;
        self.position = b.first_frame + frames;
        out.push((
            stamp,
            WideFrame::Audio(WideAudio {
                segment_id,
                first_sample_index: b.first_frame,
                device_ts_us,
                pcm: b.pcm,
            }),
        ));
        out.push((
            stamp,
            WideFrame::Capture(CaptureFrame::Read {
                first_sample_index: b.first_frame,
                mono_us: b.mono_us,
                backlog_frames: b.backlog_frames,
            }),
        ));
        out
    }

    /// The frames for one event, stamped `mono_us`.
    pub fn event(&mut self, mono_us: u64, event: Event) -> Vec<Stamped> {
        match event {
            Event::Gap(g) => {
                let stamp = HostMicros(g.wall_us);
                let mut out = Vec::with_capacity(2);
                match g.frames {
                    Some(n) => self.position = g.at_frame + n,
                    None => out.extend(self.close(stamp, mono_us, EndReason::SourceLoss)),
                }
                out.push((
                    stamp,
                    WideFrame::Capture(CaptureFrame::Loss {
                        cause: g.cause,
                        at_index: g.at_frame,
                        samples: g.frames,
                        mono_us,
                        detail: cut(g.detail),
                    }),
                ));
                out
            }
            Event::End(e) => {
                let stamp = HostMicros(e.wall_us);
                let mut out = Vec::with_capacity(2);
                out.extend(self.close(stamp, mono_us, EndReason::StreamEnd));
                out.push((
                    stamp,
                    WideFrame::Capture(CaptureFrame::End {
                        reason: e.reason,
                        at_index: e.at_frame,
                        mono_us,
                        detail: cut(e.detail),
                    }),
                ));
                out
            }
        }
    }

    /// Close the open segment, if one is.
    fn close(&mut self, stamp: HostMicros, mono_us: u64, reason: EndReason) -> Option<Stamped> {
        let open = self.open.take()?;
        Some((
            stamp,
            WideFrame::SegmentEnd(SegmentEnd {
                segment_id: open.id,
                device_ts_us: mono_us,
                frames_sent: open.frames_sent,
                samples_sent: open.samples_sent,
                reason,
            }),
        ))
    }
}

/// `detail` cut to at most [`MAX_DETAIL_BYTES`], at a character boundary.
fn cut(mut detail: String) -> String {
    if detail.len() > MAX_DETAIL_BYTES {
        let mut at = MAX_DETAIL_BYTES;
        while !detail.is_char_boundary(at) {
            at -= 1;
        }
        detail.truncate(at);
    }
    detail
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::granted::SampleFormat;

    /// 1 kHz, two channels of s16_le: 4 bytes a frame.
    fn granted() -> Granted {
        Granted {
            rate_hz: 1000,
            channels: 2,
            sample_format: SampleFormat::S16Le,
            period_frames: 10,
            buffer_frames: 160,
        }
    }

    fn block(first_frame: u64, frames: usize, backlog_frames: u32) -> Block {
        Block {
            mono_us: 1_000_000,
            wall_us: 7_000_000,
            first_frame,
            backlog_frames,
            pcm: vec![0; frames * 4],
        }
    }

    fn gap(cause: LossCause, at_frame: u64, frames: Option<u64>) -> Event {
        Event::Gap(Gap {
            cause,
            at_frame,
            frames,
            wall_us: 8_000_000,
            detail: "d".to_owned(),
        })
    }

    fn end(at_frame: u64) -> Event {
        Event::End(End {
            reason: StreamEndReason::Stopped,
            at_frame,
            wall_us: 9_000_000,
            detail: "told to stop".to_owned(),
        })
    }

    fn kinds(frames: &[Stamped]) -> Vec<&'static str> {
        frames
            .iter()
            .map(|(_, f)| match f {
                WideFrame::Hello(_) => "hello",
                WideFrame::SegmentStart(_) => "start",
                WideFrame::Audio(_) => "audio",
                WideFrame::SegmentEnd(s) if s.reason == EndReason::SourceLoss => "end/source_loss",
                WideFrame::SegmentEnd(s) if s.reason == EndReason::StreamEnd => "end/stream_end",
                WideFrame::Capture(CaptureFrame::Describe { .. }) => "describe",
                WideFrame::Capture(CaptureFrame::Read { .. }) => "read",
                WideFrame::Capture(CaptureFrame::Loss { .. }) => "loss",
                WideFrame::Capture(CaptureFrame::End { .. }) => "capture_end",
                other => panic!("the framer made {other:?}"),
            })
            .collect()
    }

    #[test]
    fn the_prelude_is_a_described_hello_and_the_description() {
        let layout = ["l".to_owned(), "r".to_owned()];
        let [(s0, hello), (s1, describe)] = prelude("bench", &granted(), &layout, 11, 22).unwrap();
        assert_eq!((s0, s1), (HostMicros(22), HostMicros(22)));
        let WideFrame::Hello(h) = hello else {
            panic!("{hello:?}")
        };
        assert_eq!(h.version, AUDIO_PROTOCOL_VERSION);
        assert_eq!(h.pod_id.as_str(), "bench");
        assert_eq!((h.sample_rate_hz, h.channels), (1000, 2));
        assert_eq!((h.bits_per_sample, h.codec), SampleFormat::S16Le.wire());
        assert_eq!(h.channel_source, ChannelSource::Described);
        assert_eq!(
            describe,
            WideFrame::Capture(CaptureFrame::Describe {
                schema: CAPTURE_SCHEMA_VERSION,
                channel_names: layout.to_vec(),
                period_frames: 10,
                mono_us: 11,
            })
        );
    }

    #[test]
    fn the_prelude_refuses_what_a_hello_cannot_say() {
        let mut g = granted();
        assert!(prelude(&"p".repeat(33), &g, &[], 0, 0).is_err());
        g.channels = 256;
        assert!(prelude("p", &g, &[], 0, 0).is_err());
    }

    #[test]
    fn the_first_block_opens_segment_0_and_the_next_does_not() {
        let mut f = Framer::new(&granted());
        let first = f.block(block(0, 10, 0));
        assert_eq!(kinds(&first), ["start", "audio", "read"]);
        assert!(first.iter().all(|(s, _)| *s == HostMicros(7_000_000)));
        let WideFrame::SegmentStart(start) = &first[0].1 else {
            panic!()
        };
        assert_eq!(start.segment_id, 0);
        assert_eq!(start.base_sample_index, 0);
        assert_eq!(start.preroll_samples, 0);
        // 10 frames at 1 kHz end at the read: the first was captured 10 ms
        // before it.
        assert_eq!(start.base_device_ts_us, 990_000);
        assert_eq!(kinds(&f.block(block(10, 10, 0))), ["audio", "read"]);
        assert_eq!(f.position(), 20);
    }

    #[test]
    fn an_audio_frame_s_time_counts_the_backlog_and_the_read_names_it() {
        let mut f = Framer::new(&granted());
        let frames = f.block(block(40, 10, 5));
        let WideFrame::Audio(a) = &frames[1].1 else {
            panic!()
        };
        assert_eq!(a.device_ts_us, 1_000_000 - 15_000);
        assert_eq!(a.first_sample_index, 40);
        assert_eq!(a.pcm.len(), 40);
        assert_eq!(
            frames[2].1,
            WideFrame::Capture(CaptureFrame::Read {
                first_sample_index: 40,
                mono_us: 1_000_000,
                backlog_frames: 5,
            })
        );
    }

    #[test]
    fn a_measured_gap_is_a_loss_inside_the_segment_and_advances_the_position() {
        let mut f = Framer::new(&granted());
        f.block(block(0, 10, 0));
        let frames = f.event(3, gap(LossCause::WriterBacklog, 10, Some(30)));
        assert_eq!(kinds(&frames), ["loss"]);
        assert_eq!(f.position(), 40);
        assert_eq!(kinds(&f.block(block(40, 10, 0))), ["audio", "read"]);
    }

    #[test]
    fn an_unmeasured_gap_closes_the_segment_and_the_next_block_opens_another() {
        let mut f = Framer::new(&granted());
        f.block(block(0, 10, 0));
        f.block(block(10, 7, 0));
        let frames = f.event(3, gap(LossCause::Overrun, 17, None));
        assert_eq!(kinds(&frames), ["end/source_loss", "loss"]);
        assert!(frames.iter().all(|(s, _)| *s == HostMicros(8_000_000)));
        assert_eq!(
            frames[0].1,
            WideFrame::SegmentEnd(SegmentEnd {
                segment_id: 0,
                device_ts_us: 3,
                frames_sent: 2,
                samples_sent: 17,
                reason: EndReason::SourceLoss,
            })
        );
        assert_eq!(
            frames[1].1,
            WideFrame::Capture(CaptureFrame::Loss {
                cause: LossCause::Overrun,
                at_index: 17,
                samples: None,
                mono_us: 3,
                detail: "d".to_owned(),
            })
        );
        assert_eq!(f.position(), 17);
        let next = f.block(block(17, 10, 0));
        let WideFrame::SegmentStart(start) = &next[0].1 else {
            panic!("{next:?}")
        };
        assert_eq!((start.segment_id, start.base_sample_index), (1, 17));
    }

    #[test]
    fn a_loss_with_no_segment_open_is_its_loss_alone() {
        let mut f = Framer::new(&granted());
        assert_eq!(
            kinds(&f.event(1, gap(LossCause::Suspend, 0, None))),
            ["loss"]
        );
        assert_eq!(
            kinds(&f.event(2, gap(LossCause::TransferError, 0, None))),
            ["loss"]
        );
        assert_eq!(
            kinds(&f.event(3, gap(LossCause::WriterBacklog, 0, Some(20)))),
            ["loss"]
        );
        let next = f.block(block(20, 10, 0));
        let WideFrame::SegmentStart(start) = &next[0].1 else {
            panic!("{next:?}")
        };
        assert_eq!((start.segment_id, start.base_sample_index), (0, 20));
    }

    #[test]
    fn an_end_closes_an_open_segment_with_stream_end() {
        let mut f = Framer::new(&granted());
        f.block(block(0, 10, 0));
        let frames = f.event(4, end(10));
        assert_eq!(kinds(&frames), ["end/stream_end", "capture_end"]);
        assert_eq!(
            frames[1].1,
            WideFrame::Capture(CaptureFrame::End {
                reason: StreamEndReason::Stopped,
                at_index: 10,
                mono_us: 4,
                detail: "told to stop".to_owned(),
            })
        );
        let mut idle = Framer::new(&granted());
        assert_eq!(kinds(&idle.event(4, end(0))), ["capture_end"]);
    }

    #[test]
    fn a_long_detail_is_cut_at_a_character_boundary() {
        let long = format!("{}é", "x".repeat(MAX_DETAIL_BYTES - 1));
        let cut = cut(long);
        assert_eq!(cut.len(), MAX_DETAIL_BYTES - 1);
        assert_eq!(super::cut("short".to_owned()), "short");
    }
}
