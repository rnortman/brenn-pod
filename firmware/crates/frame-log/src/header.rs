//! The header metadata of a frame log: who wrote it and, for a capture, from
//! which device and with what it was granted. Unknown keys are ignored on read,
//! so a key added later needs no version bump; a known key with a value this
//! crate does not define is refused.

use std::collections::BTreeMap;
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
    /// What the writer read of the device's own state. Absent: the writer
    /// reads no device state, as every log written before the key existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_state: Option<DeviceState>,
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
    /// Where the card's USB device sits; `None` for a card not on USB, for a
    /// USB device sysfs could not describe, and in a log written before the
    /// key existed.
    #[serde(default)]
    pub usb: Option<UsbLocation>,
}

/// The USB device behind a card, as sysfs described it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsbLocation {
    /// The port path, e.g. `3-2.2`: the same while the device stays in one
    /// socket.
    pub port: String,
    pub bus: u16,
    /// The address on the bus, reassigned at every enumeration.
    pub address: u16,
    /// `None` for a device that reports no serial number.
    pub serial: Option<String>,
    /// The device's release number, four hex digits.
    pub bcd_device: String,
}

/// What the writer read of a device's own state before the stream started.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum DeviceState {
    /// The device is not one the writer knows how to read; nothing was sent
    /// to it.
    NotAttempted {
        why: NotAttemptedWhy,
        detail: String,
    },
    /// The writer tried and could not reach the device. No register value is
    /// recorded.
    Unavailable {
        why: UnavailableWhy,
        /// The system's own words, and the register a failed transfer was for.
        detail: String,
        /// The reader that tried; `None` until the device is known to be one
        /// the writer reads.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        kind: Option<String>,
        /// `CLOCK_MONOTONIC` directly before the first transfer; `None` when
        /// none was made.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        begun_mono_us: Option<u64>,
        /// The wall reading paired with `begun_mono_us`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        wall_us: Option<u64>,
    },
    /// Every register the reader knows was asked; each was read or refused.
    Read {
        /// The reader, e.g. `xvf3800`.
        kind: String,
        /// `CLOCK_MONOTONIC` directly before the first transfer.
        begun_mono_us: u64,
        /// `CLOCK_MONOTONIC` directly after the last transfer.
        ended_mono_us: u64,
        /// The wall reading paired with `begun_mono_us`.
        wall_us: u64,
        /// By register name; the names are the reader's.
        registers: BTreeMap<String, RegisterEntry>,
    },
}

/// Why a device's state was not read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotAttemptedWhy {
    /// The card's device is not on USB.
    NotUsb,
    /// The card's USB id is not one the writer reads.
    UnknownUsbId,
}

/// Why a device's state could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableWhy {
    /// sysfs could not describe the card's USB device.
    Sysfs,
    /// The device's node would not open.
    Open,
    /// A transfer did not complete.
    Transfer,
    /// The device behind the card changed or went away during the read.
    DeviceChanged,
}

/// One register of a device's state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterEntry {
    pub resid: u8,
    pub cmd: u8,
    #[serde(flatten)]
    pub reading: RegisterReading,
}

/// A register's payload, or the device's refusal of it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RegisterReading {
    Read {
        /// The payload as read, lowercase hex: the reading.
        bytes: String,
        /// The same payload decoded by the reader; `null` for a float that is
        /// not finite.
        value: serde_json::Value,
    },
    Refused {
        refused: RegisterRefusal,
    },
}

/// How a device refused a register.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "how", rename_all = "snake_case")]
pub enum RegisterRefusal {
    /// The device answered a status other than done, after the retry budget.
    Status { status: u8, attempts: u32 },
    /// The device declined the request (on USB, a stalled transfer).
    Declined,
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
    /// `false`: the writer wrote nothing to the device (no reboot, no register
    /// write). Reads of the device's state are in `device_state`.
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
        if let Some(DeviceState::Read {
            begun_mono_us,
            ended_mono_us,
            registers,
            ..
        }) = &self.device_state
        {
            if registers.is_empty() {
                return bad("device_state is read with no registers".to_owned());
            }
            if ended_mono_us < begun_mono_us {
                return bad(format!(
                    "device_state.ended_mono_us {ended_mono_us} is under begun_mono_us \
                     {begun_mono_us}"
                ));
            }
            for (name, entry) in registers {
                if let RegisterReading::Read { bytes, .. } = &entry.reading
                    && !is_even_lower_hex(bytes)
                {
                    return bad(format!(
                        "device_state.registers.{name}.bytes {bytes:?} is not lowercase hex \
                         of even length"
                    ));
                }
            }
        }
        Ok(())
    }
}

fn is_even_lower_hex(s: &str) -> bool {
    s.len().is_multiple_of(2) && is_lower_hex(s, s.len())
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
                usb: Some(UsbLocation {
                    port: "3-2.2".to_owned(),
                    bus: 3,
                    address: 28,
                    serial: Some("EXAMPLE-SERIAL".to_owned()),
                    bcd_device: "0100".to_owned(),
                }),
            },
            granted: CaptureGranted {
                period_frames: 320,
                buffer_frames: 5120,
            },
            clocks: Clocks::v1(),
            device_control: DeviceControl { performed: false },
            device_state: None,
        }
    }

    /// A `read` state with one register read and one refused of each kind.
    pub(crate) fn read_state() -> DeviceState {
        let entry = |resid, cmd, reading| RegisterEntry {
            resid,
            cmd,
            reading,
        };
        DeviceState::Read {
            kind: "xvf3800".to_owned(),
            begun_mono_us: 1_000,
            ended_mono_us: 1_041,
            wall_us: 1_794_000_000_000_000,
            registers: BTreeMap::from([
                (
                    "version".to_owned(),
                    entry(
                        48,
                        0,
                        RegisterReading::Read {
                            bytes: "020102".to_owned(),
                            value: serde_json::json!([2, 1, 2]),
                        },
                    ),
                ),
                (
                    "bld_repo_hash".to_owned(),
                    entry(
                        48,
                        3,
                        RegisterReading::Refused {
                            refused: RegisterRefusal::Status {
                                status: 3,
                                attempts: 1,
                            },
                        },
                    ),
                ),
                (
                    "aec_asroutgain".to_owned(),
                    entry(
                        33,
                        36,
                        RegisterReading::Refused {
                            refused: RegisterRefusal::Declined,
                        },
                    ),
                ),
            ]),
        }
    }

    fn round_trips(c: &CaptureMeta) -> serde_json::Value {
        let v = serde_json::to_value(c).unwrap();
        let back: CaptureMeta = serde_json::from_value(v.clone()).unwrap();
        assert_eq!(&back, c);
        back.validate().unwrap();
        v
    }

    #[test]
    fn a_read_state_round_trips_in_its_documented_keys() {
        let mut c = capture();
        c.device_state = Some(read_state());
        let v = round_trips(&c);
        assert_eq!(
            v["device_state"],
            serde_json::json!({
                "outcome": "read", "kind": "xvf3800",
                "begun_mono_us": 1_000, "ended_mono_us": 1_041,
                "wall_us": 1_794_000_000_000_000_u64,
                "registers": {
                    "version": {"resid": 48, "cmd": 0, "bytes": "020102", "value": [2, 1, 2]},
                    "bld_repo_hash": {"resid": 48, "cmd": 3,
                                      "refused": {"how": "status", "status": 3, "attempts": 1}},
                    "aec_asroutgain": {"resid": 33, "cmd": 36, "refused": {"how": "declined"}}
                }
            })
        );
    }

    #[test]
    fn a_non_finite_reading_round_trips_as_null_beside_its_bytes() {
        let mut c = capture();
        c.device_state = Some(DeviceState::Read {
            kind: "xvf3800".to_owned(),
            begun_mono_us: 5,
            ended_mono_us: 5,
            wall_us: 9,
            registers: BTreeMap::from([(
                "audio_mgr_ref_gain".to_owned(),
                RegisterEntry {
                    resid: 35,
                    cmd: 1,
                    reading: RegisterReading::Read {
                        bytes: "0000c07f".to_owned(),
                        value: serde_json::Value::Null,
                    },
                },
            )]),
        });
        let v = round_trips(&c);
        assert!(v["device_state"]["registers"]["audio_mgr_ref_gain"]["value"].is_null());
    }

    #[test]
    fn the_other_outcomes_round_trip_in_their_documented_keys() {
        let mut c = capture();
        c.device_state = Some(DeviceState::NotAttempted {
            why: NotAttemptedWhy::UnknownUsbId,
            detail: "046d:0825 is not an XVF3800".to_owned(),
        });
        assert_eq!(
            round_trips(&c)["device_state"],
            serde_json::json!({"outcome": "not_attempted", "why": "unknown_usb_id",
                               "detail": "046d:0825 is not an XVF3800"})
        );

        c.device.usb = None;
        c.device_state = Some(DeviceState::NotAttempted {
            why: NotAttemptedWhy::NotUsb,
            detail: "card 0 is not on USB".to_owned(),
        });
        assert_eq!(round_trips(&c)["device_state"]["why"], "not_usb");

        c.device_state = Some(DeviceState::Unavailable {
            why: UnavailableWhy::Sysfs,
            detail: "busnum: No such file or directory".to_owned(),
            kind: None,
            begun_mono_us: None,
            wall_us: None,
        });
        assert_eq!(
            round_trips(&c)["device_state"],
            serde_json::json!({"outcome": "unavailable", "why": "sysfs",
                               "detail": "busnum: No such file or directory"})
        );

        c.device_state = Some(DeviceState::Unavailable {
            why: UnavailableWhy::Transfer,
            detail: "at version: timed out".to_owned(),
            kind: Some("xvf3800".to_owned()),
            begun_mono_us: Some(7),
            wall_us: Some(8),
        });
        assert_eq!(
            round_trips(&c)["device_state"],
            serde_json::json!({"outcome": "unavailable", "why": "transfer",
                               "detail": "at version: timed out", "kind": "xvf3800",
                               "begun_mono_us": 7, "wall_us": 8})
        );
        for (why, text) in [
            (UnavailableWhy::Open, "open"),
            (UnavailableWhy::DeviceChanged, "device_changed"),
        ] {
            assert_eq!(serde_json::to_value(why).unwrap(), text);
        }
    }

    #[test]
    fn a_header_written_before_the_device_keys_reads_without_them() {
        let mut v = serde_json::to_value(capture()).unwrap();
        v["device"].as_object_mut().unwrap().remove("usb");
        assert!(v.get("device_state").is_none());
        let c: CaptureMeta = serde_json::from_value(v).unwrap();
        assert_eq!(c.device.usb, None);
        assert_eq!(c.device_state, None);
        c.validate().unwrap();
    }

    #[test]
    fn each_device_state_rule_refuses() {
        let with = |state| {
            let mut c = capture();
            c.device_state = Some(state);
            c
        };
        let set_bytes = |bytes: &str| {
            let mut s = read_state();
            if let DeviceState::Read { registers, .. } = &mut s {
                registers.get_mut("version").unwrap().reading = RegisterReading::Read {
                    bytes: bytes.to_owned(),
                    value: serde_json::Value::Null,
                };
            }
            with(s)
        };
        refused(&set_bytes("02010"), "registers.version.bytes");
        refused(&set_bytes("02A102"), "registers.version.bytes");
        refused(&set_bytes("0x0102"), "registers.version.bytes");
        set_bytes("").validate().unwrap();

        let mut s = read_state();
        if let DeviceState::Read { registers, .. } = &mut s {
            registers.clear();
        }
        refused(&with(s), "read with no registers");

        let mut s = read_state();
        if let DeviceState::Read { ended_mono_us, .. } = &mut s {
            *ended_mono_us = 999;
        }
        refused(&with(s), "is under begun_mono_us");
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
        c.device.usb = None;
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
                           "usb_id": "2886:0022",
                           "usb": {"port": "3-2.2", "bus": 3, "address": 28,
                                   "serial": "EXAMPLE-SERIAL", "bcd_device": "0100"}},
                "granted": {"period_frames": 320, "buffer_frames": 5120},
                "clocks": {"mono": "CLOCK_MONOTONIC", "wall": "CLOCK_REALTIME", "unit": "us"},
                "device_control": {"performed": false}
            })
        );
    }
}
