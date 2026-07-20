//! Shared wire-frame and frame-log fixture builders for tests that need a
//! synthesized session on disk: `Hello`/`SegmentStart`/`Audio`/`SegmentEnd`
//! frames, encoded and written through the real `FrameLogWriter`. Every
//! frame-log-consuming test module used to hand-roll its own copy of this
//! plumbing; that drifts (a new wire field means a mechanical edit in every
//! copy), so it lives once here behind the `test-util` feature.
//!
//! `synth_session` (`crate::synth`) is the PCM-driven twin of this module —
//! it cannot express gaps or hand-picked sample indices, which is exactly
//! what frame-log-splice tests need, hence this separate, lower-level
//! builder set.

use audio_pipeline::wire::{
    encode_frame, AudioFrame, ChannelSource, Codec, EndReason, Hello, SegmentEnd, SegmentStart,
    StreamFrame, AUDIO_PROTOCOL_VERSION, MAX_AUDIO_PAYLOAD,
};
use heapless::Vec as HVec;

use crate::clock::HostMicros;
use crate::framelog::{FrameLogWriter, LogMeta};

/// Encode `frame` exactly as it would appear on the wire and in a frame log
/// (`[u16 len][postcard]`), so replay through `decode_frame` is byte-faithful.
pub fn framed(frame: &StreamFrame) -> Vec<u8> {
    let mut buf = [0u8; MAX_AUDIO_PAYLOAD + 64];
    let n = encode_frame(frame, &mut buf).expect("frame fits");
    buf[..n].to_vec()
}

/// A `Hello` for `pod_id`: 16 kHz mono S16, the ASR beam.
pub fn hello(pod_id: &str) -> StreamFrame {
    StreamFrame::Hello(Hello {
        version: AUDIO_PROTOCOL_VERSION,
        pod_id: heapless::String::try_from(pod_id).unwrap(),
        sample_rate_hz: 16_000,
        bits_per_sample: 16,
        channels: 1,
        codec: Codec::S16Le,
        channel_source: ChannelSource::AsrBeam,
    })
}

/// A `SegmentStart` at `base`, no preroll.
pub fn seg_start(segment_id: u32, base: u64) -> StreamFrame {
    StreamFrame::SegmentStart(SegmentStart {
        segment_id,
        base_sample_index: base,
        base_device_ts_us: 0,
        preroll_samples: 0,
    })
}

/// A normal (`VadRelease`) `SegmentEnd`.
pub fn seg_end(segment_id: u32, samples: u64) -> StreamFrame {
    StreamFrame::SegmentEnd(SegmentEnd {
        segment_id,
        device_ts_us: 0,
        frames_sent: 1,
        samples_sent: samples,
        reason: EndReason::VadRelease,
    })
}

/// An audio frame of `n_samples` samples starting at `first`, values `1..=n`
/// so silence (zero) is distinguishable from real audio.
pub fn audio(segment_id: u32, first: u64, n_samples: usize) -> StreamFrame {
    let mut pcm: HVec<u8, MAX_AUDIO_PAYLOAD> = HVec::new();
    for i in 0..n_samples {
        let v = (i as i16 + 1).to_le_bytes();
        pcm.push(v[0]).unwrap();
        pcm.push(v[1]).unwrap();
    }
    StreamFrame::Audio(AudioFrame {
        segment_id,
        first_sample_index: first,
        device_ts_us: 0,
        pcm,
    })
}

/// A `LogMeta` stamped at `base_epoch_us`.
pub fn meta(base_epoch_us: u64) -> LogMeta {
    LogMeta {
        build_id: "test".to_string(),
        created_epoch_us: HostMicros(base_epoch_us),
        conn_seq: 1,
        rolled_from: None,
    }
}

/// Write `frames` to a fresh frame log at `path`, each stamped
/// `base_epoch_us + i * 1ms` so a segment's derived timestamp is
/// deterministic and log-specific.
pub fn write_log_at(path: &std::path::Path, base_epoch_us: u64, frames: &[StreamFrame]) {
    let mut w = FrameLogWriter::create(path, meta(base_epoch_us)).unwrap();
    for (i, f) in frames.iter().enumerate() {
        w.append(HostMicros(base_epoch_us + i as u64 * 1_000), &framed(f))
            .unwrap();
    }
    w.finish().unwrap();
}

/// [`write_log_at`] with a fixed default epoch, for tests that don't care.
pub fn write_log(path: &std::path::Path, frames: &[StreamFrame]) {
    write_log_at(path, 1_700_000_000_000_000, frames);
}
