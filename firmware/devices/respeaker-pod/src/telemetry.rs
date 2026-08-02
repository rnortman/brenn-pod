//! Telemetry / VAD thread: this pod's seams for the shared poll core.
//!
//! The poll cadence, the VAD FSM driving, the segment-open bookkeeping and the
//! drop accounting live in `pod_streamer::telemetry`, shared with the Linux pod.
//! What is ESP-specific and stays here: the I2C bus behind its process-wide
//! mutex, the FreeRTOS sleep, the capture ring's write head, and the
//! VAD-threshold / hangover NVS loaders.

use audio_pipeline::vad::{VAD_HANGOVER_MS, decode_vad_hangover_ms, decode_vad_threshold};
use esp_idf_svc::hal::delay::FreeRtos;
use pod_streamer::telemetry::{
    Reading, TelemetryBus, TelemetryCore, TelemetryCtx, VAD_THRESHOLD_DEFAULT, read_f32x4_reading,
    run_telemetry_loop,
};

use crate::i2c::I2C_BUS;
use crate::nvs::open_audio_nvs;
use crate::streamer::esp_monotonic_us;
use crate::xvf3800::I2cControl;
use crate::{CAPTURE_RING, StreamerMsg, VAD_CLOSED_FLAG};
use xvf3800_ctrl::I2C_RETRY;

/// Poll rates, re-exported so boot logging names them from one place.
pub(crate) use pod_streamer::telemetry::{DOA_POLL_HZ, VAD_POLL_HZ};

/// The XVF3800 control plane as this pod reaches it: the shared I2C driver,
/// locked per reading rather than for the life of the loop, so HIL self-tests and
/// the GPO servicer can interleave their own transactions between ticks.
struct I2cTelemetryBus;

impl TelemetryBus for I2cTelemetryBus {
    fn read(&mut self, reading: Reading) -> Option<[f32; 4]> {
        let mut guard = I2C_BUS
            .lock()
            .unwrap_or_else(|_| panic!("I2C_BUS mutex poisoned in telemetry thread"));
        match guard.as_mut() {
            None => {
                // Logged on the SPENERGY reading only: the bus is either up or it
                // is not, and one line per tick is already the boot-bug signal.
                if reading == Reading::SpEnergy {
                    log::warn!("telemetry: I2C_BUS is None — boot init bug");
                }
                None
            }
            Some(drv) => read_f32x4_reading(&mut I2cControl::new(drv), I2C_RETRY, reading),
        }
    }
}

/// The capture ring's current write head — read at VAD onset so the streamer can
/// place the pre-roll cursor.
fn ring_write_head() -> u64 {
    let guard = CAPTURE_RING
        .lock()
        .unwrap_or_else(|_| panic!("CAPTURE_RING mutex poisoned in telemetry thread"));
    guard
        .as_ref()
        .expect("CAPTURE_RING is None in telemetry thread — boot init bug")
        .write_head
}

/// Whether a HIL test has quiesced capture. The onset arm is this thread's only
/// ring toucher, so while capture is parked the gate is fed silence.
fn capture_quiesced() -> bool {
    crate::capture::CAPTURE_QUIESCED.load(std::sync::atomic::Ordering::Acquire)
}

/// Spawn the telemetry/VAD thread.
///
/// Polls SPENERGY at `VAD_POLL_HZ` (20 Hz) and DoA at `DOA_POLL_HZ` (10 Hz) via I2C.
/// Feeds the VAD FSM; on onset sends `VadOpened` with the ring write-head, on release
/// sends `VadClosed`. Telemetry frames are sent only while a segment is open.
/// Channel-full drops are counted but tolerated (audio has priority).
/// I2C errors skip the poll without updating the FSM.
pub(crate) fn spawn_telemetry_vad_thread(tx: std::sync::mpsc::SyncSender<StreamerMsg>) {
    std::thread::Builder::new()
        .name("telemetry".into())
        .stack_size(12288)
        .spawn(move || {
            let core = TelemetryCore::new(load_vad_threshold(), load_vad_hangover_ms());
            let ctx = TelemetryCtx {
                tx: &tx,
                vad_closed_flag: &VAD_CLOSED_FLAG,
                write_head: &ring_write_head,
                now_us: &esp_monotonic_us,
                capture_quiesced: &capture_quiesced,
            };
            run_telemetry_loop(core, &mut I2cTelemetryBus, &ctx, &FreeRtos::delay_ms);
        })
        .expect("telemetry: thread spawn failed — heap exhausted?");
}

/// Read the VAD threshold from NVS (`"audio"` namespace, `"vad_threshold"` key,
/// 4-byte LE f32 blob), or return `VAD_THRESHOLD_DEFAULT` on any error.
fn load_vad_threshold() -> f32 {
    let nvs = match open_audio_nvs(false) {
        Ok(n) => n,
        Err(msg) => {
            log::warn!(
                "vad: cannot open audio NVS — {}; using default threshold {}",
                msg.as_str(),
                VAD_THRESHOLD_DEFAULT
            );
            return VAD_THRESHOLD_DEFAULT;
        }
    };
    let mut buf = [0u8; 4];
    match nvs.get_blob("vad_threshold", &mut buf) {
        Ok(Some(b)) => match decode_vad_threshold(b) {
            Some(t) => {
                log::info!("vad: loaded threshold {} from NVS", t);
                t
            }
            None => {
                log::warn!(
                    "vad: NVS vad_threshold blob invalid (wrong length or non-finite/negative); \
                     using default {}",
                    VAD_THRESHOLD_DEFAULT
                );
                VAD_THRESHOLD_DEFAULT
            }
        },
        Ok(None) => {
            // Key absent — fresh device or unprovisioned.
            log::info!(
                "vad: no vad_threshold in NVS; using default {}",
                VAD_THRESHOLD_DEFAULT
            );
            VAD_THRESHOLD_DEFAULT
        }
        Err(e) => {
            log::warn!(
                "vad: NVS get_blob vad_threshold failed: {:?}; using default {}",
                e,
                VAD_THRESHOLD_DEFAULT
            );
            VAD_THRESHOLD_DEFAULT
        }
    }
}

/// Read the device VAD hangover (milliseconds) from NVS (`"audio"` namespace,
/// `"vad_hangover_ms"` key, 4-byte LE `u32` blob), or return the compile-time
/// `VAD_HANGOVER_MS` default on any error.
fn load_vad_hangover_ms() -> u32 {
    let nvs = match open_audio_nvs(false) {
        Ok(n) => n,
        Err(msg) => {
            log::warn!(
                "vad: cannot open audio NVS — {}; using default hangover {} ms",
                msg.as_str(),
                VAD_HANGOVER_MS
            );
            return VAD_HANGOVER_MS;
        }
    };
    let mut buf = [0u8; 4];
    match nvs.get_blob("vad_hangover_ms", &mut buf) {
        Ok(Some(b)) => match decode_vad_hangover_ms(b) {
            Some(ms) => {
                log::info!("vad: loaded hangover {} ms from NVS", ms);
                ms
            }
            None => {
                log::warn!(
                    "vad: NVS vad_hangover_ms blob ({} bytes) invalid or out of range; \
                     using default {} ms",
                    b.len(),
                    VAD_HANGOVER_MS
                );
                VAD_HANGOVER_MS
            }
        },
        Ok(None) => {
            log::info!(
                "vad: no vad_hangover_ms in NVS; using default {} ms",
                VAD_HANGOVER_MS
            );
            VAD_HANGOVER_MS
        }
        Err(e) => {
            log::warn!(
                "vad: NVS get_blob vad_hangover_ms failed: {:?}; using default {} ms",
                e,
                VAD_HANGOVER_MS
            );
            VAD_HANGOVER_MS
        }
    }
}
