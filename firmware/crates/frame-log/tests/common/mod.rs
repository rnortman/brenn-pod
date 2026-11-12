//! Frame logs built frame by frame, for the walker's, summary's, export's and
//! binary's tests. Each test crate uses part of it.
#![allow(dead_code)]

use std::path::PathBuf;

use audio_pipeline::wide::{
    CAPTURE_SCHEMA_VERSION, CaptureFrame, LossCause, StreamEndReason, WideAudio, WideFrame,
    encode_wide,
};
use audio_pipeline::wire::{
    AUDIO_PROTOCOL_VERSION, ChannelSource, Codec, EndReason, Hello, SegmentEnd, SegmentStart,
};
use frame_log::{HostMicros, LogMeta, Summary, summarize, write_header, write_record};

pub const RATE: u32 = 16_000;
pub const PERIOD: u32 = 320;
pub const PERIOD_US: u64 = 20_000;
pub const START_MONO: u64 = 5_000_000;
pub const WALL_OFFSET: u64 = 1_700_000_000_000_000;

pub fn hello(channels: u8, bits: u8, codec: Codec, source: ChannelSource) -> WideFrame {
    WideFrame::Hello(Hello {
        version: AUDIO_PROTOCOL_VERSION,
        pod_id: heapless::String::try_from("bench").unwrap(),
        sample_rate_hz: RATE,
        bits_per_sample: bits,
        channels,
        codec,
        channel_source: source,
    })
}

pub fn start(segment_id: u32, base: u64, ts: u64) -> WideFrame {
    WideFrame::SegmentStart(SegmentStart {
        segment_id,
        base_sample_index: base,
        base_device_ts_us: ts,
        preroll_samples: 0,
    })
}

pub fn seg_end(segment_id: u32, samples_sent: u64, reason: EndReason) -> WideFrame {
    WideFrame::SegmentEnd(SegmentEnd {
        segment_id,
        device_ts_us: 0,
        frames_sent: 0,
        samples_sent,
        reason,
    })
}

pub fn audio(segment_id: u32, first: u64, ts: u64, pcm: Vec<u8>) -> WideFrame {
    WideFrame::Audio(WideAudio {
        segment_id,
        first_sample_index: first,
        device_ts_us: ts,
        pcm,
    })
}

pub fn describe(schema: u8, names: usize) -> WideFrame {
    WideFrame::Capture(CaptureFrame::Describe {
        schema,
        channel_names: vec!["m".to_owned(); names],
        period_frames: PERIOD,
        mono_us: 0,
    })
}

/// A log's bytes, built frame by frame.
pub struct Log {
    pub bytes: Vec<u8>,
    pub dir: tempfile::TempDir,
}

impl Log {
    pub fn new() -> Self {
        let mut bytes = Vec::new();
        let meta = LogMeta {
            build_id: "test".to_owned(),
            created_epoch_us: HostMicros(WALL_OFFSET),
            conn_seq: 0,
            rolled_from: None,
            capture: None,
        };
        write_header(&mut bytes, &meta).unwrap();
        Log {
            bytes,
            dir: tempfile::tempdir().unwrap(),
        }
    }

    pub fn frame(&mut self, stamp: u64, frame: WideFrame) {
        let mut buf = vec![0u8; 65_537];
        let n = encode_wide(&frame, &mut buf).unwrap();
        write_record(&mut self.bytes, HostMicros(stamp), &buf[..n]).unwrap();
    }

    pub fn path(&self) -> PathBuf {
        let path = self.dir.path().join("a.framelog");
        std::fs::write(&path, &self.bytes).unwrap();
        path
    }

    pub fn summary(&self) -> Summary {
        summarize(&self.path()).unwrap()
    }
}

/// A voice-host log: mono S16, segments opened and closed by a gate. Each
/// sample's value is its index, truncated to 16 bits.
pub fn voice_log(log: &mut Log, segments: &[(u32, u64, u64, EndReason)]) {
    log.frame(1, hello(1, 16, Codec::S16Le, ChannelSource::AsrBeam));
    for &(id, base, frames, reason) in segments {
        let ts = base * 1_000_000 / u64::from(RATE);
        log.frame(2, start(id, base, ts));
        for k in 0..frames / u64::from(PERIOD) {
            let first = base + k * u64::from(PERIOD);
            log.frame(
                3,
                audio(id, first, ts + k * PERIOD_US, mono_pcm(first, PERIOD)),
            );
        }
        log.frame(4, seg_end(id, frames, reason));
    }
}

/// Mono S16 PCM whose samples are their indices, truncated to 16 bits.
pub fn mono_pcm(first: u64, frames: u32) -> Vec<u8> {
    (first..first + u64::from(frames))
        .flat_map(|i| (i as u16).to_le_bytes())
        .collect()
}

/// Writes a capture's frames on a clock that advances a period per period.
pub struct Cap {
    pub log: Log,
    pub frame_bytes: usize,
    pub pos: u64,
    pub mono: u64,
    pub wall_offset: u64,
    pub segment: Option<(u32, u64)>,
    pub next_id: u32,
    pub pcm: Vec<u8>,
}

impl Cap {
    pub fn new(channels: u8, bits: u8) -> Self {
        let codec = if bits == 16 {
            Codec::S16Le
        } else {
            Codec::S32Le
        };
        let mut log = Log::new();
        log.frame(
            START_MONO + WALL_OFFSET,
            hello(channels, bits, codec, ChannelSource::Described),
        );
        log.frame(
            START_MONO + WALL_OFFSET,
            WideFrame::Capture(CaptureFrame::Describe {
                schema: CAPTURE_SCHEMA_VERSION,
                channel_names: (0..channels).map(|c| format!("ch{c}")).collect(),
                period_frames: PERIOD,
                mono_us: START_MONO,
            }),
        );
        Cap {
            log,
            frame_bytes: usize::from(channels) * usize::from(bits / 8),
            pos: 0,
            mono: START_MONO,
            wall_offset: WALL_OFFSET,
            segment: None,
            next_id: 0,
            pcm: Vec::new(),
        }
    }

    pub fn stamp(&self) -> u64 {
        self.mono + self.wall_offset
    }

    /// A block of `frames`, read `backlog` frames behind the source.
    pub fn block(&mut self, frames: u32, backlog: u32) {
        self.mono += u64::from(frames) * 1_000_000 / u64::from(RATE);
        let mono = self.mono + u64::from(backlog) * 1_000_000 / u64::from(RATE);
        let ts = mono - u64::from(backlog + frames) * 1_000_000 / u64::from(RATE);
        let id = match self.segment {
            Some((id, _)) => id,
            None => {
                let id = self.next_id;
                self.next_id += 1;
                self.log.frame(self.stamp(), start(id, self.pos, ts));
                self.segment = Some((id, 0));
                id
            }
        };
        let pcm: Vec<u8> = (0..frames as usize * self.frame_bytes)
            .map(|i| (self.pos as usize + i) as u8)
            .collect();
        self.pcm.extend_from_slice(&pcm);
        let first = self.pos;
        let stamp = mono + self.wall_offset;
        self.log.frame(stamp, audio(id, first, ts, pcm));
        self.log.frame(
            stamp,
            WideFrame::Capture(CaptureFrame::Read {
                first_sample_index: first,
                mono_us: mono,
                backlog_frames: backlog,
            }),
        );
        self.pos += u64::from(frames);
        self.segment = Some((id, self.segment.unwrap().1 + u64::from(frames)));
    }

    pub fn loss(&mut self, cause: LossCause, samples: Option<u64>) {
        if samples.is_none()
            && let Some((id, sent)) = self.segment.take()
        {
            self.log
                .frame(self.stamp(), seg_end(id, sent, EndReason::SourceLoss));
        }
        let frame = WideFrame::Capture(CaptureFrame::Loss {
            cause,
            at_index: self.pos,
            samples,
            mono_us: self.mono,
            detail: "lost".to_owned(),
        });
        self.log.frame(self.stamp(), frame);
        if let Some(n) = samples {
            self.pos += n;
            self.mono += n * 1_000_000 / u64::from(RATE);
        }
    }

    /// Close the open segment with `reason`, as a gate would.
    pub fn close(&mut self, reason: EndReason) {
        let (id, sent) = self.segment.take().unwrap();
        let stamp = self.stamp();
        self.log.frame(stamp, seg_end(id, sent, reason));
    }

    pub fn end(mut self, reason: StreamEndReason) -> Log {
        if let Some((id, sent)) = self.segment.take() {
            self.log
                .frame(self.stamp(), seg_end(id, sent, EndReason::StreamEnd));
        }
        let frame = WideFrame::Capture(CaptureFrame::End {
            reason,
            at_index: self.pos,
            mono_us: self.mono,
            detail: String::new(),
        });
        self.log.frame(self.stamp(), frame);
        self.log
    }
}
