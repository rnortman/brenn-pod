//! The three seams the capture loop is generic over, each with its one
//! production implementation.

use std::fs::File;
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::time::Duration;

use alsa_pcm::{ByteCapture, ClockPair, PcmError, Read, Wait};
use capture_record::Truncate;

/// Where captured frames come from.
///
/// Not reachy-pod's `run::PeriodSource`, which returns `i16` samples and retries
/// past a recovery: this one returns bytes and hands every recovery to its
/// caller, which is what puts the loss in the record.
pub trait CaptureSource {
    /// Trigger the stream.
    fn start(&mut self) -> Result<(), PcmError>;
    /// Wait up to `timeout` for a period. A stream recovery restarts the stream
    /// before waiting.
    fn wait(&mut self, timeout: Duration) -> Result<Wait, PcmError>;
    /// Read what is ready. Called only after [`wait`](Self::wait) said `Ready`.
    fn read(&mut self) -> Result<Read<'_>, PcmError>;
    /// Stop the stream now; frames still in the device's buffer are discarded.
    fn stop(&mut self) -> Result<(), PcmError>;
}

impl CaptureSource for ByteCapture<'_> {
    fn start(&mut self) -> Result<(), PcmError> {
        ByteCapture::start(self)
    }

    fn wait(&mut self, timeout: Duration) -> Result<Wait, PcmError> {
        ByteCapture::wait(self, timeout)
    }

    fn read(&mut self) -> Result<Read<'_>, PcmError> {
        ByteCapture::read(self)
    }

    fn stop(&mut self) -> Result<(), PcmError> {
        ByteCapture::stop(self)
    }
}

/// The capturing machine's clocks, read as a pair. Cloned into the writer
/// thread, which stamps the end records it writes itself.
pub trait Clock: Clone + Send + 'static {
    fn pair(&self) -> ClockPair;
}

/// `CLOCK_MONOTONIC` and `CLOCK_REALTIME` through [`alsa_pcm::clock_pair`].
#[derive(Debug, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn pair(&self) -> ClockPair {
        alsa_pcm::clock_pair()
    }
}

/// Where the record goes: written, cut back after a failed write, asked for its
/// free space, and synced at the end.
pub trait Destination: Write + Truncate + Send + 'static {
    /// Bytes still free to an unprivileged writer on the destination's file
    /// system.
    fn free_bytes(&self) -> io::Result<u64>;
    /// Push everything written to the storage device.
    fn sync(&self) -> io::Result<()>;
}

impl Destination for File {
    fn free_bytes(&self) -> io::Result<u64> {
        let mut st = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        // SAFETY: the descriptor is this open file's, and `st` is a valid place
        // for fstatvfs to write a statvfs into.
        let rc = unsafe { libc::fstatvfs(self.as_raw_fd(), st.as_mut_ptr()) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fstatvfs returned 0, so it filled `st`.
        let st = unsafe { st.assume_init() };
        // Both are u64 on 64-bit Linux and narrower on some 32-bit targets.
        #[allow(clippy::useless_conversion)]
        let (blocks, block_bytes) = (u64::from(st.f_bavail), u64::from(st.f_frsize));
        Ok(blocks.saturating_mul(block_bytes))
    }

    fn sync(&self) -> io::Result<()> {
        self.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_file_reports_free_space_and_syncs() {
        let dir = tempfile::tempdir().unwrap();
        let file = File::create_new(dir.path().join("r")).unwrap();
        assert!(file.free_bytes().unwrap() > 0);
        file.sync().unwrap();
    }

    #[test]
    fn the_system_clock_reads_a_pair() {
        let a = SystemClock.pair();
        let b = SystemClock.pair();
        assert!(b.mono_us >= a.mono_us);
        assert!(a.wall_us > 0);
    }
}
