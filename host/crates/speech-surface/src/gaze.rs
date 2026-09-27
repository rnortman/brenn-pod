//! The gaze seam: how a composing process that knows the robot's geometry picks
//! where the head looks when the wake word is heard.
//!
//! The server carries the microphone array's readings to the seam and asks it
//! once per wake; the motion knowledge — where the array sits, where the head
//! stands, which pose faces a bearing — stays with the composer.

pub use speech_pipeline::DoaSample;
use speech_pipeline::PodId;

/// A pose the head should raise to on a wake, named in the daemon's library,
/// with the pace of the move (`None`: the library's own).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GazePose {
    pub name: String,
    pub move_ms: Option<u64>,
}

/// Chooses where the head looks when the wake word is heard.
///
/// `doa` is every azimuth reading the array reported during the last half second
/// of the phrase, plus one chunk of slack past `wake_end_sample`, oldest first, in
/// the chip's convention (`TelemetryKind::Azimuths`: angle from the array axis,
/// radians, NaN on a beam tracking nothing); empty when no reading arrived in that
/// span. They say where the array heard the phrase from, relative to the array.
/// `wake_end_sample` is the listener's absolute index one past the phrase.
///
/// `Some` is the raise this wake takes, and then the pose the head answers from
/// at dispatch in place of the configured turn pose. `None` means respond as
/// configured. The name must be a library pose the daemon resolves; `keep`, and
/// any name or pace no motion script may carry, is refused by the server and
/// answered as `None` with a `gaze_refused` line.
///
/// Called on the pipeline task once per wake, after the epoch check. It must not
/// block or await.
pub trait WakeGaze: Send + Sync + 'static {
    fn choose(&self, pod: &PodId, doa: &[DoaSample], wake_end_sample: u64) -> Option<GazePose>;
}
