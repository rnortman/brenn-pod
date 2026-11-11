//! A capture stream read as bytes, handing every recovery to its caller.

use std::time::Duration;

use alsa::PCM;
use alsa::pcm::{IO, State};

use crate::{PcmError, RecoveryBudget, Retry, granted};

/// A recovery the stream went through: the transfer or wait that failed, and the
/// stream restarted. Audio the device had buffered is gone and ALSA does not say how
/// much.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recovery {
    /// The errno the failure carried: `EPIPE` for an overrun, `ESTRPIPE` for a
    /// suspend, anything else for a transfer error recovery cleared.
    pub errno: i32,
    /// ALSA's text for the failure.
    pub detail: String,
}

/// What [`ByteCapture::wait`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Wait {
    /// At least a period is waiting: [`ByteCapture::read`] will not block.
    Ready,
    /// Nothing became ready within the timeout, or a signal woke the wait first.
    /// Nothing about the stream changed.
    TimedOut,
    /// The stream failed while being waited on and was recovered.
    Recovered(Recovery),
}

/// The frames one read delivered.
#[derive(Debug, PartialEq, Eq)]
pub struct Period<'a> {
    /// Interleaved frames exactly as the device delivered them.
    pub bytes: &'a [u8],
    /// Whole frames in `bytes`: a period, or fewer when the device delivered fewer.
    pub frames: usize,
    /// Frames already waiting in the device's buffer when the read returned. When
    /// that cannot be read, the stream has failed after this read and the next
    /// [`ByteCapture::wait`] reports it; a capture stream in that state has a full
    /// buffer, so the buffer size is reported.
    pub backlog_frames: u32,
}

/// What [`ByteCapture::read`] returned.
#[derive(Debug, PartialEq, Eq)]
pub enum Read<'a> {
    Period(Period<'a>),
    /// The read failed and the stream was recovered; nothing was read.
    Recovered(Recovery),
}

/// A capture stream read as bytes, so no sample type is compiled in.
///
/// Every recovery is handed to the caller, which is what lets the caller put the
/// loss where it happened. It charges the same [`RecoveryBudget`]: past the ceiling, or when
/// recovery itself fails, the stream is over.
pub struct ByteCapture<'a> {
    pcm: &'a PCM,
    io: IO<'a, u8>,
    /// One period of interleaved frames.
    buf: Vec<u8>,
    frame_bytes: usize,
    buffer_frames: u32,
    budget: RecoveryBudget,
}

impl<'a> ByteCapture<'a> {
    /// Wrap an open capture PCM, sized by what it was granted.
    pub fn new(pcm: &'a PCM) -> Result<Self, PcmError> {
        let granted = granted(pcm).map_err(|e| PcmError::Stream {
            reason: format!("cannot read back the granted parameters: {e}"),
        })?;
        let frame_bytes = usize::try_from(pcm.frames_to_bytes(1))
            .ok()
            .filter(|n| *n > 0)
            .ok_or_else(|| PcmError::Stream {
                reason: "the device states no byte width for a frame".to_string(),
            })?;
        Ok(Self {
            pcm,
            io: pcm.io_bytes(),
            buf: vec![0u8; granted.period_frames as usize * frame_bytes],
            frame_bytes,
            buffer_frames: granted.buffer_frames,
            budget: RecoveryBudget::new(),
        })
    }

    /// Trigger the stream.
    pub fn start(&mut self) -> Result<(), PcmError> {
        self.pcm.start().map_err(|e| PcmError::Stream {
            reason: format!("the stream would not start: {e}"),
        })
    }

    /// Stop the stream now. Frames still in the device's buffer are discarded.
    pub fn stop(&mut self) -> Result<(), PcmError> {
        self.pcm.drop().map_err(|e| PcmError::Stream {
            reason: format!("the stream would not stop: {e}"),
        })
    }

    /// Recoveries since the stream opened.
    pub fn recoveries(&self) -> u64 {
        self.budget.recoveries()
    }

    /// Wait up to `timeout` for a period to be ready.
    ///
    /// Recovery re-prepares the stream, and a prepared capture stream that is waited
    /// on never fills, so a stream found prepared is triggered first.
    pub fn wait(&mut self, timeout: Duration) -> Result<Wait, PcmError> {
        if self.pcm.state() == State::Prepared {
            self.start()?;
        }
        let ms = u32::try_from(timeout.as_millis()).unwrap_or(u32::MAX);
        match self.pcm.wait(Some(ms)) {
            Ok(true) => Ok(Wait::Ready),
            Ok(false) => Ok(Wait::TimedOut),
            Err(e) => Ok(match self.fail(e)? {
                None => Wait::TimedOut,
                Some(recovery) => Wait::Recovered(recovery),
            }),
        }
    }

    /// Read up to one period. Called after [`wait`](Self::wait) returned `Ready`, so
    /// it does not block. A signal-interrupted transfer is reissued.
    pub fn read(&mut self) -> Result<Read<'_>, PcmError> {
        loop {
            match self.io.readi(&mut self.buf) {
                Ok(frames) => {
                    self.budget.note_success();
                    let backlog_frames = self.backlog();
                    return Ok(Read::Period(Period {
                        bytes: &self.buf[..frames * self.frame_bytes],
                        frames,
                        backlog_frames,
                    }));
                }
                Err(e) => {
                    if let Some(recovery) = self.fail(e)? {
                        return Ok(Read::Recovered(recovery));
                    }
                }
            }
        }
    }

    /// Frames waiting in the device's buffer now.
    fn backlog(&self) -> u32 {
        match self.pcm.avail_update() {
            Ok(n) => u32::try_from(n).unwrap_or(self.buffer_frames),
            Err(_) => self.buffer_frames,
        }
    }

    /// Charge a failed transfer and recover. `None` is a signal interruption, to be
    /// reissued; `Err` is a stream that is over.
    fn fail(&mut self, e: alsa::Error) -> Result<Option<Recovery>, PcmError> {
        let errno = e.errno();
        let detail = e.to_string();
        match self.budget.note_failure("capture", self.pcm, e) {
            Ok(Retry::Interrupted) => Ok(None),
            Ok(Retry::Recovered) => Ok(Some(Recovery { errno, detail })),
            Err(reason) => Err(PcmError::Stream { reason }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Granted, PcmParams, open_pcm};
    use alsa::Direction;
    use alsa::pcm::Format;

    fn params(channels: u32, format: Format) -> PcmParams {
        PcmParams {
            rate_hz: 16_000,
            channels,
            format,
            period_frames: 320,
            periods: 16,
        }
    }

    fn null(params: &PcmParams) -> PCM {
        open_pcm("null", Direction::Capture, params)
            .expect("ALSA's built-in null device is part of libasound2-data")
    }

    fn reads_a_whole_period(channels: u32, format: Format, sample_bytes: usize) {
        let p = params(channels, format);
        let pcm = null(&p);
        assert_eq!(
            granted(&pcm).expect("granted"),
            Granted {
                rate_hz: 16_000,
                channels,
                format,
                period_frames: 320,
                buffer_frames: 320 * 16,
            }
        );
        let mut capture = ByteCapture::new(&pcm).expect("a byte reader");
        capture.start().expect("start");
        assert_eq!(
            capture.wait(Duration::from_millis(100)).expect("wait"),
            Wait::Ready
        );
        match capture.read().expect("read") {
            Read::Period(period) => {
                assert_eq!(period.frames, 320);
                assert_eq!(period.bytes.len(), 320 * channels as usize * sample_bytes);
                assert!(
                    period.backlog_frames <= 320 * 16,
                    "{}",
                    period.backlog_frames
                );
            }
            other => panic!("expected a period, got {other:?}"),
        }
    }

    #[test]
    fn six_channels_of_s16_le_read_as_a_whole_period_of_bytes() {
        reads_a_whole_period(6, Format::S16LE, 2);
    }

    #[test]
    fn two_channels_of_s32_le_read_as_a_whole_period_of_bytes() {
        reads_a_whole_period(2, Format::S32LE, 4);
    }

    #[test]
    fn a_stream_left_prepared_by_recovery_is_running_after_the_next_wait() {
        // Recovery re-prepares a stream. A prepared capture stream that is waited on
        // never fills, so every wait would time out and a healthy device would read as
        // one that delivers nothing. Against the null device: no hardware, but the
        // same state machine.
        let pcm = null(&params(6, Format::S16LE));
        let mut capture = ByteCapture::new(&pcm).expect("a byte reader");
        capture.start().expect("start");
        capture.stop().expect("stop");
        pcm.prepare().expect("prepare");
        assert_eq!(pcm.state(), State::Prepared);
        assert_eq!(
            capture.wait(Duration::from_millis(100)).expect("wait"),
            Wait::Ready
        );
        assert_eq!(
            pcm.state(),
            State::Running,
            "the wait must trigger the stream, or it is waiting on one that never fills"
        );
    }

    #[test]
    fn stopping_leaves_the_stream_stopped() {
        let pcm = null(&params(2, Format::S16LE));
        let mut capture = ByteCapture::new(&pcm).expect("a byte reader");
        capture.start().expect("start");
        assert_eq!(pcm.state(), State::Running);
        capture.stop().expect("stop");
        assert_eq!(pcm.state(), State::Setup);
    }
}
