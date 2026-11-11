//! Shared integration-test fixtures: the header every test record is written
//! under, and the values it is built from. Included via `mod common;` by the
//! integration-test binaries in `tests/`; not every binary uses every item.
#![allow(dead_code)]

use capture_record::{
    BuildSource, Clocks, Device, DeviceControl, Granted, Header, SampleFormat, StartPair,
    WriterBuild, WriterInfo,
};

pub const RATE: u32 = 16_000;
pub const PERIOD: u32 = 320;
pub const START_MONO: u64 = 1_000_000;
pub const START_WALL: u64 = 1_794_000_000_000_000;

/// A valid header for a Flex-like device at `channels` and `sample_format`.
pub fn header(channels: u32, sample_format: SampleFormat) -> Header {
    Header {
        record_id: "0123456789abcdef0123456789abcdef".to_owned(),
        writer: WriterInfo {
            program: "test".to_owned(),
            build: WriterBuild {
                commit: "a".repeat(40),
                dirty: Some(false),
                source: BuildSource::Git,
            },
        },
        device: Device {
            selector: "Flex".to_owned(),
            card_index: 2,
            card_name: "reSpeaker Flex".to_owned(),
            card_long_name: "Seeed Studio reSpeaker Flex at usb-1".to_owned(),
            pcm: "hw:2,0".to_owned(),
            usb_id: Some("2886:0022".to_owned()),
        },
        granted: Granted {
            rate_hz: RATE,
            channels,
            sample_format,
            period_frames: PERIOD,
            buffer_frames: PERIOD * 16,
        },
        layout: (0..channels).map(|i| format!("ch{i}")).collect(),
        clocks: Clocks::v1(),
        started: StartPair {
            mono_us: START_MONO,
            wall_us: START_WALL,
        },
        device_control: DeviceControl { performed: false },
    }
}
