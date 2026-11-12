//! What the device granted, as `capture_started` states it, and the sample
//! formats the recorder takes.

use audio_pipeline::wide::MAX_WIDE_PCM_BYTES;
use audio_pipeline::wire::Codec;
use serde::{Deserialize, Serialize};

/// The stream parameters the device granted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Granted {
    pub rate_hz: u32,
    pub channels: u32,
    pub sample_format: SampleFormat,
    pub period_frames: u32,
    pub buffer_frames: u32,
}

impl Granted {
    /// Bytes of one frame: one sample per channel.
    pub fn frame_bytes(&self) -> usize {
        self.channels as usize * self.sample_format.bytes_per_sample()
    }

    /// The most whole frames one audio frame can carry.
    pub fn max_block_frames(&self) -> usize {
        MAX_WIDE_PCM_BYTES / self.frame_bytes()
    }
}

/// The sample formats this recorder can be asked for and can open an ALSA
/// stream in. It is a closed set of its own, not `Codec`: a codec the wire
/// gains is not a format the recorder takes until it has an arm here and in
/// the ALSA mapping. [`SampleFormat::wire`] names each on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SampleFormat {
    S16Le,
    S32Le,
}

impl SampleFormat {
    /// The width the hello states, in bytes.
    pub fn bytes_per_sample(self) -> usize {
        usize::from(self.wire().0 / 8)
    }

    /// The name used in event lines and on the command line.
    pub fn name(self) -> &'static str {
        match self {
            SampleFormat::S16Le => "s16_le",
            SampleFormat::S32Le => "s32_le",
        }
    }

    /// The hello's `bits_per_sample` and `codec` for this format.
    pub fn wire(self) -> (u8, Codec) {
        match self {
            SampleFormat::S16Le => (16, Codec::S16Le),
            SampleFormat::S32Le => (32, Codec::S32Le),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_format_names_are_the_serde_names() {
        for f in [SampleFormat::S16Le, SampleFormat::S32Le] {
            assert_eq!(serde_json::to_value(f).unwrap(), f.name());
        }
    }

    #[test]
    fn each_format_is_its_width_and_codec_on_the_wire() {
        assert_eq!(SampleFormat::S16Le.wire(), (16, Codec::S16Le));
        assert_eq!(SampleFormat::S32Le.wire(), (32, Codec::S32Le));
        assert_eq!(SampleFormat::S16Le.bytes_per_sample(), 2);
        assert_eq!(SampleFormat::S32Le.bytes_per_sample(), 4);
    }

    #[test]
    fn a_block_holds_the_most_whole_frames_that_fit_a_wide_audio_frame() {
        let g = Granted {
            rate_hz: 16_000,
            channels: 6,
            sample_format: SampleFormat::S16Le,
            period_frames: 320,
            buffer_frames: 5120,
        };
        assert_eq!(g.frame_bytes(), 12);
        assert_eq!(g.max_block_frames(), MAX_WIDE_PCM_BYTES / 12);
    }
}
