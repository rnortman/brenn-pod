//! The gaze seam: how a composing process that knows the robot's geometry picks
//! where the head looks when the wake word is heard.
//!
//! The server carries the microphone array's readings to the seam and asks it
//! once per wake; the motion knowledge — where the array sits, where the head
//! stands, how the head and the body share a bearing — stays with the composer.

pub use speech_pipeline::DoaSample;
use speech_pipeline::PodId;

/// A direction the head should face on a wake, in the motion wire's look
/// units: `bearing_mrad` from the base's forward, positive to the robot's
/// left; `elevation_mrad` above level. How the head and the body share the
/// bearing, and the pace, are the motion daemon's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GazeLook {
    pub bearing_mrad: i32,
    pub elevation_mrad: i32,
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
/// `Some` is the look this wake raises to, and then the look the head answers
/// from at dispatch in place of the configured turn pose. `None` means respond
/// as configured. A direction no motion script may carry is refused by the
/// server and answered as `None` with a `gaze_refused` line.
///
/// Called on the pipeline task once per wake, after the epoch check. It must not
/// block or await.
pub trait WakeGaze: Send + Sync + 'static {
    fn choose(&self, pod: &PodId, doa: &[DoaSample], wake_end_sample: u64) -> Option<GazeLook>;
}
