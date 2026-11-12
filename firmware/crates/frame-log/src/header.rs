//! The header metadata of a frame log: who wrote it and, for a capture, from
//! which device and with what it was granted. Unknown keys are ignored on read,
//! so a key added later needs no version bump; a known key with a value this
//! crate does not define is refused.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::HostMicros;

/// Self-describing header metadata, serialized as JSON. Unknown fields are
/// ignored on read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogMeta {
    /// Build identity of the writing process.
    pub build_id: String,
    /// Wall clock of the writing machine at log creation. Serializes as a bare
    /// integer named `created_epoch_us`.
    pub created_epoch_us: HostMicros,
    /// Per-process connection sequence number; 0 for a capture, the one stream
    /// of its process.
    pub conn_seq: u64,
    /// Name of the prior log this one rolled from, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rolled_from: Option<String>,
    /// What describes a capture's writer and device; absent from a voice-host
    /// log.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture: Option<CaptureMeta>,
}

/// The header's description of a capture: the recording's identity, the
/// program and build that wrote it, the device, what the device granted that
/// the frames do not carry, the clocks, and what the writer did to the device.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureMeta {
    /// 128 random bits as 32 lowercase hex digits: the recording's identity,
    /// which another stream cites together with a frame range. The file name
    /// is not part of it.
    pub record_id: String,
    pub writer: WriterInfo,
    pub device: Device,
    pub granted: CaptureGranted,
    pub clocks: Clocks,
    pub device_control: DeviceControl,
}

/// The program that wrote the log and the build it came from.
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

/// What the device granted beyond what the hello carries. The period is also
/// in the stream's `Describe` frame, where a reader of the stream finds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureGranted {
    pub period_frames: u32,
    pub buffer_frames: u32,
}

/// Which clock each reading is taken from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Clocks {
    pub mono: String,
    pub wall: String,
    pub unit: String,
}

impl Clocks {
    /// The one set of clocks a capture is defined on: every `mono_us` on
    /// `CLOCK_MONOTONIC`, every record stamp on `CLOCK_REALTIME`, in
    /// microseconds.
    pub fn v1() -> Self {
        Clocks {
            mono: "CLOCK_MONOTONIC".to_owned(),
            wall: "CLOCK_REALTIME".to_owned(),
            unit: "us".to_owned(),
        }
    }
}

/// What the writer did to the device beyond reading it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceControl {
    /// `false`: the writer sent the device no control command (no reboot, no
    /// register write). Says nothing about where a device's configuration may
    /// be recorded.
    pub performed: bool,
}

/// A capture description whose values break the format's rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureMetaInvalid(pub String);

impl fmt::Display for CaptureMetaInvalid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid capture header: {}", self.0)
    }
}

impl std::error::Error for CaptureMetaInvalid {}

fn is_lower_hex(s: &str, len: usize) -> bool {
    s.len() == len
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl CaptureMeta {
    /// Check the rules serde cannot: identity and commit shapes, a dirty flag
    /// only where git vouches, a positive period within the buffer, and the
    /// clocks a capture is defined on.
    pub fn validate(&self) -> Result<(), CaptureMetaInvalid> {
        let bad = |msg: String| Err(CaptureMetaInvalid(msg));
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
        if g.period_frames == 0 {
            return bad("granted.period_frames is 0".to_owned());
        }
        if g.buffer_frames < g.period_frames {
            return bad(format!(
                "granted.buffer_frames {} is under period_frames {}",
                g.buffer_frames, g.period_frames
            ));
        }
        if self.clocks != Clocks::v1() {
            return bad(format!(
                "clocks {:?} are not {:?}",
                self.clocks,
                Clocks::v1()
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn capture() -> CaptureMeta {
        CaptureMeta {
            record_id: "9f0c1d2e3f405162738495a6b7c8d9e0".to_owned(),
            writer: WriterInfo {
                program: "pod-capture".to_owned(),
                build: WriterBuild {
                    commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                    dirty: Some(false),
                    source: BuildSource::Git,
                },
            },
            device: Device {
                selector: "Flex".to_owned(),
                card_index: 2,
                card_name: "Flex".to_owned(),
                card_long_name: "Flex long".to_owned(),
                pcm: "hw:2,0".to_owned(),
                usb_id: Some("2886:0022".to_owned()),
            },
            granted: CaptureGranted {
                period_frames: 320,
                buffer_frames: 5120,
            },
            clocks: Clocks::v1(),
            device_control: DeviceControl { performed: false },
        }
    }

    fn refused(c: &CaptureMeta, needle: &str) {
        let err = c.validate().unwrap_err();
        assert!(err.0.contains(needle), "{err} lacks {needle:?}");
    }

    #[test]
    fn a_well_formed_capture_validates() {
        capture().validate().unwrap();
        let mut c = capture();
        c.writer.build.source = BuildSource::Declared;
        c.writer.build.dirty = None;
        c.device.usb_id = None;
        c.validate().unwrap();
    }

    #[test]
    fn each_rule_refuses() {
        let mut c = capture();
        c.record_id = "9F0C1D2E3F405162738495A6B7C8D9E0".to_owned();
        refused(&c, "record_id");

        let mut c = capture();
        c.writer.build.commit = "abc".to_owned();
        refused(&c, "commit");

        let mut c = capture();
        c.writer.build.dirty = None;
        refused(&c, "null for a git build");

        let mut c = capture();
        c.writer.build.source = BuildSource::Declared;
        refused(&c, "set for a declared build");

        let mut c = capture();
        c.granted.period_frames = 0;
        refused(&c, "period_frames is 0");

        let mut c = capture();
        c.granted.buffer_frames = 319;
        refused(&c, "under period_frames");

        let mut c = capture();
        c.clocks.wall = "CLOCK_TAI".to_owned();
        refused(&c, "clocks");
    }

    #[test]
    fn the_json_keys_are_the_documented_ones() {
        let v = serde_json::to_value(capture()).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "record_id": "9f0c1d2e3f405162738495a6b7c8d9e0",
                "writer": {"program": "pod-capture",
                           "build": {"commit": "0123456789abcdef0123456789abcdef01234567",
                                     "dirty": false, "source": "git"}},
                "device": {"selector": "Flex", "card_index": 2, "card_name": "Flex",
                           "card_long_name": "Flex long", "pcm": "hw:2,0",
                           "usb_id": "2886:0022"},
                "granted": {"period_frames": 320, "buffer_frames": 5120},
                "clocks": {"mono": "CLOCK_MONOTONIC", "wall": "CLOCK_REALTIME", "unit": "us"},
                "device_control": {"performed": false}
            })
        );
    }
}
