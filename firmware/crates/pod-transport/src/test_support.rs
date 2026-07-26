//! In-memory test doubles shared across crates.
//!
//! Compiled only under `cfg(test)` or the `test-helpers` feature, so these helpers
//! never enter a normal build. They exist so dependents (`hil-host`) can reuse the
//! same `FakePort` the crate's own tests use — items in a `#[cfg(test)]` module are
//! not exported to dependents.

use crate::{FrameReader, Harness};
use device_protocol::DeviceFrame;

/// Per-write hook: receives the written bytes and the rx queue.
///
/// `Send` because `Harness` stores its port as `Box<dyn Transport>`, and
/// `Transport: Send`.
pub type OnWrite = Box<dyn FnMut(&[u8], &mut std::collections::VecDeque<u8>) + Send>;

/// An in-memory serial port: reads from a queued byte buffer, records writes.
pub struct FakePort {
    /// Bytes the harness will "read" from the device.
    pub rx: std::collections::VecDeque<u8>,
    /// Bytes the harness has written (inspectable by tests).
    pub tx: Vec<u8>,
    /// Called on each write with the written bytes and the rx queue, after the
    /// bytes are recorded into `tx`. Lets tests script "device answers only the
    /// Nth request" and count writes. `None` = inert port.
    pub on_write: Option<OnWrite>,
    /// Once this many writes have been attempted, every further write reports
    /// `BrokenPipe` — a transiently unusable endpoint. The `on_write` hook still
    /// runs for a failing write, because bytes can reach the device even when
    /// the write syscall reports an error. `None` = every write succeeds.
    pub fail_writes_after: Option<usize>,
    writes: usize,
}

impl FakePort {
    /// A port with nothing queued to read, nothing written, and no write hook.
    pub fn new() -> Self {
        Self {
            rx: std::collections::VecDeque::new(),
            tx: Vec::new(),
            on_write: None,
            fail_writes_after: None,
            writes: 0,
        }
    }

    /// Queue a `DeviceFrame` as COBS-encoded bytes for the harness to read.
    pub fn queue_frame(&mut self, frame: &DeviceFrame) {
        queue_frame_into(&mut self.rx, frame);
    }
}

/// Append a `DeviceFrame` as COBS-encoded bytes to an rx queue.
///
/// Free function so an `on_write` hook — which only gets the queue, not the
/// port — can answer a request mid-flight.
pub fn queue_frame_into(rx: &mut std::collections::VecDeque<u8>, frame: &DeviceFrame) {
    let mut buf = [0u8; 512];
    let len = device_protocol::framing::encode_device_frame(frame, &mut buf).unwrap_or_else(|e| {
        panic!(
            "queue_frame: failed to encode frame ({e}); \
             encoded size may exceed the 512-byte stack buffer"
        )
    });
    rx.extend(buf[..len].iter().copied());
}

impl Default for FakePort {
    fn default() -> Self {
        Self::new()
    }
}

/// Simulated read timeout: an empty port sleeps this long before reporting
/// `TimedOut`, the way a real port opened with a non-zero read timeout does, so
/// callers' wait loops sleep between polls instead of spinning a core.
const FAKE_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(1);

impl std::io::Read for FakePort {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.rx.is_empty() {
            std::thread::sleep(FAKE_READ_TIMEOUT);
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
        self.writes += 1;
        self.tx.extend_from_slice(buf);
        if let Some(hook) = self.on_write.as_mut() {
            hook(buf, &mut self.rx);
        }
        if self.fail_writes_after.is_some_and(|n| self.writes > n) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "fake port write failure",
            ));
        }
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
