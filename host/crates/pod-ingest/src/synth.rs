//! Pure PCM → session-frame synthesis: the inverse of segment export. Given a
//! 16 kHz mono S16 PCM buffer and segment metadata, produce the ordered wire
//! frames a real pod would send for one VAD segment — `Hello`, `SegmentStart`
//! (with a caller-chosen preroll split), a run of 20 ms `Audio` frames, and a
//! `SegmentEnd` — each paired with a real-time-paced host-receive offset.
//!
//! Sans-I/O: this synthesizes frames only. A caller (the `wav-import` bin, or a
//! fixture test) supplies a base epoch and writes the frames through the real
//! `FrameLogWriter`, so any laptop-captured `.wav` becomes a replayable frame
//! log. Deterministic for fixed inputs and metadata.

use audio_pipeline::wire::{
    AudioFrame, ChannelSource, Codec, EndReason, Hello, SegmentEnd, SegmentStart, StreamFrame,
    AUDIO_PROTOCOL_VERSION, AUDIO_SAMPLES_PER_FRAME,
};
use heapless::String as HString;
use heapless::Vec as HVec;

use crate::clock::samples_to_micros;

/// Metadata for a synthesized single-segment session.
#[derive(Debug, Clone)]
pub struct SynthParams {
    /// Pod identity carried in `Hello` (≤ 32 bytes).
    pub pod_id: String,
    /// I2S sample rate in Hz; also paces the synthesized frames in real time.
    pub sample_rate_hz: u32,
    /// The segment's counter, shared by every frame of the segment.
    pub segment_id: u32,
    /// Absolute sample index (since capture start) of the first sample.
    pub base_sample_index: u64,
    /// Device-clock µs at `base_sample_index` — the segment's timing anchor.
    pub base_device_ts_us: u64,
    /// Leading samples that predate VAD onset. Must not exceed the PCM length.
    pub preroll_samples: u32,
    /// Which XVF3800 beam the `Hello` advertises.
    pub channel_source: ChannelSource,
}

/// One synthesized wire frame with its host-receive offset (µs from session
/// start). A caller adds its chosen base epoch to get the `host_rx` timestamp
/// each `FrameLogWriter::append` wants; the offsets pace `Audio` frames at the
/// real-time rate implied by `sample_rate_hz`.
#[derive(Debug, PartialEq)]
pub struct SynthFrame {
    /// Microseconds from the start of the synthesized session.
    pub host_rx_offset_us: u64,
    /// The wire frame at that offset.
    pub frame: StreamFrame,
}

/// Why synthesis could not proceed.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SynthError {
    /// `pod_id` did not fit the wire's 32-byte `Hello.pod_id`.
    #[error("pod_id too long ({0} bytes; max 32)")]
    PodIdTooLong(usize),
    /// `sample_rate_hz` was zero — no real-time pacing is definable.
    #[error("sample_rate_hz must be non-zero")]
    ZeroSampleRate,
    /// `preroll_samples` exceeded the PCM length — a preroll larger than the
    /// whole clip is meaningless.
    #[error("preroll_samples {preroll} exceeds pcm length {pcm_len}")]
    PrerollExceedsAudio { preroll: u32, pcm_len: usize },
}

/// Synthesize one VAD segment as ordered, real-time-paced wire frames.
///
/// The audio is split into 20 ms (`AUDIO_SAMPLES_PER_FRAME`) `Audio` frames; a
/// final short frame carries any remainder. `Hello` and `SegmentStart` sit at
/// offset 0; each `Audio` frame is paced by its first sample; `SegmentEnd`
/// lands just past the last sample and reports the true frame and sample
/// counts. The segment ends `VadRelease` (a normal, complete utterance).
pub fn synth_session(pcm: &[i16], params: &SynthParams) -> Result<Vec<SynthFrame>, SynthError> {
    if params.sample_rate_hz == 0 {
        return Err(SynthError::ZeroSampleRate);
    }
    if params.preroll_samples as usize > pcm.len() {
        return Err(SynthError::PrerollExceedsAudio {
            preroll: params.preroll_samples,
            pcm_len: pcm.len(),
        });
    }
    let pod_id = HString::<32>::try_from(params.pod_id.as_str())
        .map_err(|_| SynthError::PodIdTooLong(params.pod_id.len()))?;

    // Hello + SegmentStart + one Audio frame per chunk + SegmentEnd.
    let mut frames = Vec::with_capacity(3 + pcm.len().div_ceil(AUDIO_SAMPLES_PER_FRAME));

    frames.push(SynthFrame {
        host_rx_offset_us: 0,
        frame: StreamFrame::Hello(Hello {
            version: AUDIO_PROTOCOL_VERSION,
            pod_id,
            sample_rate_hz: params.sample_rate_hz,
            bits_per_sample: 16,
            channels: 1,
            codec: Codec::S16Le,
            channel_source: params.channel_source,
        }),
    });

    frames.push(SynthFrame {
        host_rx_offset_us: 0,
        frame: StreamFrame::SegmentStart(SegmentStart {
            segment_id: params.segment_id,
            base_sample_index: params.base_sample_index,
            base_device_ts_us: params.base_device_ts_us,
            preroll_samples: params.preroll_samples,
        }),
    });

    let mut frames_sent: u32 = 0;
    for (chunk_idx, chunk) in pcm.chunks(AUDIO_SAMPLES_PER_FRAME).enumerate() {
        let sample_offset = (chunk_idx * AUDIO_SAMPLES_PER_FRAME) as u64;
        let offset_us = samples_to_micros(sample_offset, params.sample_rate_hz);
        let bytes: HVec<u8, { audio_pipeline::wire::MAX_AUDIO_PAYLOAD }> =
            audio_pipeline::wire::pack_pcm_s16le(chunk);
        frames.push(SynthFrame {
            host_rx_offset_us: offset_us,
            frame: StreamFrame::Audio(AudioFrame {
                segment_id: params.segment_id,
                first_sample_index: params.base_sample_index + sample_offset,
                device_ts_us: params.base_device_ts_us + offset_us,
                pcm: bytes,
            }),
        });
        frames_sent += 1;
    }

    let total = pcm.len() as u64;
    let total_us = samples_to_micros(total, params.sample_rate_hz);
    frames.push(SynthFrame {
        host_rx_offset_us: total_us,
        frame: StreamFrame::SegmentEnd(SegmentEnd {
            segment_id: params.segment_id,
            device_ts_us: params.base_device_ts_us + total_us,
            frames_sent,
            samples_sent: total,
            reason: EndReason::VadRelease,
        }),
    });

    Ok(frames)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> SynthParams {
        SynthParams {
            pod_id: "pod-synth".into(),
            sample_rate_hz: 16_000,
            segment_id: 7,
            base_sample_index: 1_000,
            base_device_ts_us: 5_000_000,
            preroll_samples: 160,
            channel_source: ChannelSource::AsrBeam,
        }
    }

    fn ramp(n: usize) -> Vec<i16> {
        (0..n).map(|i| i as i16).collect()
    }

    #[test]
    fn short_final_frame_keeps_exact_chunk_length() {
        let pcm = ramp(AUDIO_SAMPLES_PER_FRAME + 5);
        let out = synth_session(&pcm, &params()).unwrap();
        let audio: Vec<_> = out
            .iter()
            .filter_map(|f| match &f.frame {
                StreamFrame::Audio(a) => Some(a),
                _ => None,
            })
            .collect();
        assert_eq!(audio.len(), 2);
        assert_eq!(audio[0].pcm.len(), AUDIO_SAMPLES_PER_FRAME * 2);
        assert_eq!(audio[1].pcm.len(), 10);
    }

    #[test]
    fn ordered_hello_start_audio_end() {
        // 2.5 audio frames worth of samples → 3 audio frames, last one short.
        let pcm = ramp(AUDIO_SAMPLES_PER_FRAME * 2 + AUDIO_SAMPLES_PER_FRAME / 2);
        let out = synth_session(&pcm, &params()).unwrap();

        assert!(matches!(out[0].frame, StreamFrame::Hello(_)));
        assert!(matches!(out[1].frame, StreamFrame::SegmentStart(_)));
        let audio: Vec<_> = out
            .iter()
            .filter(|f| matches!(f.frame, StreamFrame::Audio(_)))
            .collect();
        assert_eq!(audio.len(), 3, "two full frames plus a short remainder");
        assert!(matches!(
            out.last().unwrap().frame,
            StreamFrame::SegmentEnd(_)
        ));
    }

    #[test]
    fn hello_and_segment_start_fields_match_params() {
        let pcm = ramp(AUDIO_SAMPLES_PER_FRAME + 10);
        let p = params();
        let out = synth_session(&pcm, &p).unwrap();

        match &out[0].frame {
            StreamFrame::Hello(h) => {
                assert_eq!(h.version, AUDIO_PROTOCOL_VERSION);
                assert_eq!(h.pod_id.as_str(), p.pod_id.as_str());
                assert_eq!(h.sample_rate_hz, p.sample_rate_hz);
                assert_eq!(h.bits_per_sample, 16);
                assert_eq!(h.channels, 1);
                assert_eq!(h.codec, Codec::S16Le);
                assert_eq!(h.channel_source, p.channel_source);
            }
            other => panic!("expected Hello, got {other:?}"),
        }
        match &out[1].frame {
            StreamFrame::SegmentStart(s) => {
                assert_eq!(s.segment_id, p.segment_id);
                assert_eq!(s.base_sample_index, p.base_sample_index);
                assert_eq!(s.base_device_ts_us, p.base_device_ts_us);
                assert_eq!(s.preroll_samples, p.preroll_samples);
            }
            other => panic!("expected SegmentStart, got {other:?}"),
        }
    }

    #[test]
    fn frame_and_sample_counts_and_indices() {
        let pcm = ramp(AUDIO_SAMPLES_PER_FRAME * 2 + 5);
        let p = params();
        let out = synth_session(&pcm, &p).unwrap();

        // Audio frame sample indices step by the frame size off the base.
        let audio: Vec<&AudioFrame> = out
            .iter()
            .filter_map(|f| match &f.frame {
                StreamFrame::Audio(a) => Some(a),
                _ => None,
            })
            .collect();
        assert_eq!(audio.len(), 3);
        assert_eq!(audio[0].first_sample_index, p.base_sample_index);
        assert_eq!(
            audio[1].first_sample_index,
            p.base_sample_index + AUDIO_SAMPLES_PER_FRAME as u64
        );
        assert_eq!(audio[2].pcm.len(), 5 * 2, "short final frame, mono S16");

        match &out.last().unwrap().frame {
            StreamFrame::SegmentEnd(end) => {
                assert_eq!(end.frames_sent, 3);
                assert_eq!(end.samples_sent, pcm.len() as u64);
                assert_eq!(end.reason, EndReason::VadRelease);
            }
            other => panic!("expected SegmentEnd, got {other:?}"),
        }
    }

    #[test]
    fn realtime_pacing_offsets() {
        let pcm = ramp(AUDIO_SAMPLES_PER_FRAME * 2);
        let out = synth_session(&pcm, &params()).unwrap();

        // Hello + SegmentStart at 0; each 320-sample frame is 20 ms @ 16 kHz.
        assert_eq!(out[0].host_rx_offset_us, 0);
        assert_eq!(out[1].host_rx_offset_us, 0);
        let audio: Vec<u64> = out
            .iter()
            .filter(|f| matches!(f.frame, StreamFrame::Audio(_)))
            .map(|f| f.host_rx_offset_us)
            .collect();
        assert_eq!(audio, vec![0, 20_000]);
        assert_eq!(out.last().unwrap().host_rx_offset_us, 40_000);
    }

    #[test]
    fn deterministic_for_fixed_input() {
        let pcm = ramp(AUDIO_SAMPLES_PER_FRAME + 17);
        let a = synth_session(&pcm, &params()).unwrap();
        let b = synth_session(&pcm, &params()).unwrap();
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(&b) {
            assert_eq!(x.host_rx_offset_us, y.host_rx_offset_us);
            assert_eq!(x.frame, y.frame);
        }
    }

    #[test]
    fn preroll_larger_than_audio_rejected() {
        let pcm = ramp(100);
        let mut p = params();
        p.preroll_samples = 200;
        assert_eq!(
            synth_session(&pcm, &p),
            Err(SynthError::PrerollExceedsAudio {
                preroll: 200,
                pcm_len: 100,
            })
        );
    }

    #[test]
    fn zero_sample_rate_rejected() {
        let mut p = params();
        p.sample_rate_hz = 0;
        assert_eq!(
            synth_session(&ramp(320), &p),
            Err(SynthError::ZeroSampleRate)
        );
    }

    #[test]
    fn pod_id_too_long_rejected() {
        let mut p = params();
        p.pod_id = "x".repeat(33);
        assert_eq!(
            synth_session(&ramp(320), &p),
            Err(SynthError::PodIdTooLong(33))
        );
    }
}
