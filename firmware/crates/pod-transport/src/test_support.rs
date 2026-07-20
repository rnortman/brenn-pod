//! In-memory test doubles shared across crates.
//!
//! Compiled only under `cfg(test)` or the `test-helpers` feature, so these helpers
//! never enter a normal build. They exist so dependents (`hil-host`) can reuse the
//! same `FakePort` the crate's own tests use — items in a `#[cfg(test)]` module are
//! not exported to dependents.

use crate::{FrameReader, Harness};
use device_protocol::DeviceFrame;

/// An in-memory serial port: reads from a queued byte buffer, records writes.
pub struct FakePort {
    /// Bytes the harness will "read" from the device.
    pub rx: std::collections::VecDeque<u8>,
    /// Bytes the harness has written (inspectable by tests).
    pub tx: Vec<u8>,
}

impl FakePort {
    /// A port with nothing queued to read and nothing written.
    pub fn new() -> Self {
        Self {
            rx: std::collections::VecDeque::new(),
            tx: Vec::new(),
        }
    }

    /// Queue a `DeviceFrame` as COBS-encoded bytes for the harness to read.
    pub fn queue_frame(&mut self, frame: &DeviceFrame) {
        let mut buf = [0u8; 512];
        let len =
            device_protocol::framing::encode_device_frame(frame, &mut buf).unwrap_or_else(|e| {
                panic!(
                    "queue_frame: failed to encode frame ({e}); \
                     encoded size may exceed the 512-byte stack buffer"
                )
            });
        self.rx.extend(buf[..len].iter().copied());
    }
}

impl Default for FakePort {
    fn default() -> Self {
        Self::new()
    }
}

impl std::io::Read for FakePort {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.rx.is_empty() {
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "no data"));
        }
        let n = buf.len().min(self.rx.len());
        for (dst, src) in buf[..n].iter_mut().zip(self.rx.drain(..n)) {
            *dst = src;
        }
        Ok(n)
    }
}

impl std::io::Write for FakePort {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.tx.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Build a `Harness` driven by the given fake port.
pub fn make_harness(port: FakePort) -> Harness {
    Harness::new(Box::new(port))
}

/// Build a `FrameReader` driven by the given fake port.
pub fn make_frame_reader(port: FakePort) -> FrameReader {
    FrameReader::new(Box::new(port))
}
