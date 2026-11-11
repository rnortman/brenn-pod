//! The header metadata: who wrote the record, from which device, with what the
//! device granted, and the start anchor on both clocks. Unknown keys are
//! ignored on read, so a key added later needs no version bump; a known key
//! with a value this version does not define is refused.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::MAX_BLOCK_PCM_BYTES;

/// The header metadata of a capture record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Header {
    /// 128 random bits as 32 lowercase hex digits: the record's identity,
    /// which another stream cites together with a frame range. The file name
    /// is not part of it.
    pub record_id: String,
    pub writer: WriterInfo,
    pub device: Device,
    /// What the open stream reports it was granted, not what was asked for.
    pub granted: Granted,
    /// One name per channel, in interleave order.
    pub layout: Vec<String>,
    pub clocks: Clocks,
    /// One clock pair read directly after the stream was triggered. Frame 0
    /// was captured within the device's start latency after it.
    pub started: StartPair,
    pub device_control: DeviceControl,
}

/// The program that wrote the record and the build it came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterInfo {
    pub program: String,
    pub build: WriterBuild,
}

/// The writer's build id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterBuild {
    /// 40 hex digits.
    pub commit: String,
    /// Whether the tree had uncommitted changes; `None` when the commit was
    /// declared by the builder rather than read from git.
    pub dirty: Option<bool>,
    pub source: BuildSource,
}

/// Who vouches for the build's commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildSource {
    Git,
    Declared,
}

/// The capture device as the kernel presents it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Device {
    /// The caller's card selector, verbatim.
    pub selector: String,
    pub card_index: u32,
    pub card_name: String,
    pub card_long_name: String,
    /// The PCM opened, e.g. `hw:2,0`.
    pub pcm: String,
    /// `vvvv:pppp`, or `None` for a card the kernel gives no USB id for.
    pub usb_id: Option<String>,
}

/// The stream parameters the device granted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    /// The most whole frames one block can carry.
    pub fn max_block_frames(&self) -> usize {
        MAX_BLOCK_PCM_BYTES / self.frame_bytes()
    }
}

/// Sample formats of format version 1. A reader refuses any other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SampleFormat {
    S16Le,
    S32Le,
}

impl SampleFormat {
    pub fn bytes_per_sample(self) -> usize {
        match self {
            SampleFormat::S16Le => 2,
            SampleFormat::S32Le => 4,
        }
    }

    /// The name used in the header and on command lines.
    pub fn name(self) -> &'static str {
        match self {
            SampleFormat::S16Le => "s16_le",
            SampleFormat::S32Le => "s32_le",
        }
    }
}

/// Which clock each stamp is read from. Version 1 defines one set of values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Clocks {
    pub mono: String,
    pub wall: String,
    pub unit: String,
}

impl Clocks {
    /// The clocks of format version 1: record stamps and every `mono_us` on
    /// `CLOCK_MONOTONIC`, every `wall_us` on `CLOCK_REALTIME`, in microseconds.
    pub fn v1() -> Self {
        Clocks {
            mono: "CLOCK_MONOTONIC".to_owned(),
            wall: "CLOCK_REALTIME".to_owned(),
            unit: "us".to_owned(),
        }
    }
}

/// One paired reading of the two clocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartPair {
    pub mono_us: u64,
    pub wall_us: u64,
}

/// What the writer did to the device beyond reading it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceControl {
    /// `false`: the writer sent the device no control command (no reboot, no
    /// register write). Says nothing about where a device's configuration may
    /// be recorded.
    pub performed: bool,
}

/// A header whose values break the format's rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderInvalid(pub String);

impl fmt::Display for HeaderInvalid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid capture record header: {}", self.0)
    }
}

impl std::error::Error for HeaderInvalid {}

fn is_lower_hex(s: &str, len: usize) -> bool {
    s.len() == len
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl Header {
    /// Check the rules serde cannot: identity and commit shapes, a positive
    /// rate and period, one layout name per channel, a frame that fits in a
    /// block, a dirty flag only where git vouches, and version 1's clocks.
    pub fn validate(&self) -> Result<(), HeaderInvalid> {
        let bad = |msg: String| Err(HeaderInvalid(msg));
        if !is_lower_hex(&self.record_id, 32) {
            return bad(format!(
                "record_id {:?} is not 32 lowercase hex digits",
                self.record_id
            ));
        }
        let build = &self.writer.build;
        if build.commit.len() != 40 || !build.commit.bytes().all(|b| b.is_ascii_hexdigit()) {
            return bad(format!(
                "writer.build.commit {:?} is not 40 hex digits",
                build.commit
            ));
        }
        match (build.source, build.dirty) {
            (BuildSource::Git, None) => {
                return bad("writer.build.dirty is null for a git build".to_owned());
            }
            (BuildSource::Declared, Some(_)) => {
                return bad("writer.build.dirty is set for a declared build".to_owned());
            }
            _ => {}
        }
        let g = &self.granted;
        if g.rate_hz == 0 {
            return bad("granted.rate_hz is 0".to_owned());
        }
        if g.channels == 0 {
            return bad("granted.channels is 0".to_owned());
        }
        if g.period_frames == 0 {
            return bad("granted.period_frames is 0".to_owned());
        }
        if g.buffer_frames < g.period_frames {
            return bad(format!(
                "granted.buffer_frames {} is under period_frames {}",
                g.buffer_frames, g.period_frames
            ));
        }
        if (g.channels as usize)
            .checked_mul(g.sample_format.bytes_per_sample())
            .is_none_or(|b| b > MAX_BLOCK_PCM_BYTES)
        {
            return bad(format!(
                "one frame of {} channels of {} does not fit in a block",
                g.channels,
                g.sample_format.name()
            ));
        }
        if self.layout.len() != g.channels as usize {
            return bad(format!(
                "layout names {} channels; granted.channels is {}",
                self.layout.len(),
                g.channels
            ));
        }
        if self.clocks != Clocks::v1() {
            return bad(format!(
                "clocks {:?} are not version 1's {:?}",
                self.clocks,
                Clocks::v1()
            ));
        }
        Ok(())
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
}
