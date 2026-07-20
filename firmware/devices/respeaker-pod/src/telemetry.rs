//! Telemetry / VAD thread.
//!
//! Polls SPENERGY / DoA over I2C, drives the VAD FSM, and forwards telemetry to
//! the streamer. Also holds the VAD-threshold NVS loader.

use audio_pipeline::vad::{
    decode_vad_hangover_ms, decode_vad_threshold, vad_hangover_ticks_ms, VadStateMachine,
    VadTransition, VAD_HANGOVER_MS,
};
use audio_pipeline::wire::{Telemetry as WireTelemetry, TelemetryKind};
use esp_idf_svc::hal::delay::FreeRtos;

use crate::i2c::I2C_BUS;
use crate::nvs::open_audio_nvs;
use crate::xvf3800::{
    decode_f32x4, xvf3800_control_read, XVF3800_AEC_AZIMUTH_READ_LEN,
    XVF3800_AEC_AZIMUTH_VALUES_CMD, XVF3800_AEC_RESID, XVF3800_AEC_SPENERGY_READ_LEN,
    XVF3800_AEC_SPENERGY_VALUES_CMD,
};
use crate::{StreamerMsg, CAPTURE_RING, VAD_CLOSED_FLAG};

/// SPENERGY polling rate (Hz). 20 Hz → 50 ms poll interval for the VAD FSM.
pub(crate) const VAD_POLL_HZ: u32 = 20;

/// Direction-of-arrival polling rate (Hz). One extra I2C transaction per 100 ms.
pub(crate) const DOA_POLL_HZ: u32 = 10;

/// Default VAD gate threshold (dimensionless SPENERGY unit, max over four beams).
/// Used when NVS holds no valid `vad_threshold`.
const VAD_THRESHOLD_DEFAULT: f32 = 1.0;

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
            let hangover_ms = load_vad_hangover_ms();
            let hangover_ticks = vad_hangover_ticks_ms(hangover_ms, VAD_POLL_HZ);
            let threshold = load_vad_threshold();
            let mut vad = VadStateMachine::new(threshold, hangover_ticks);
            let mut segment_open = false;
            let mut telemetry_drops: u32 = 0;
            // DoA polls every Nth SPENERGY tick (20/10 = every 2nd tick).
            let doa_every_n = VAD_POLL_HZ / DOA_POLL_HZ;
            let mut tick: u32 = 0;

            loop {
                // ── Poll SPENERGY ────────────────────────────────────────────
                let sp_values_opt: Option<[f32; 4]> = {
                    let mut guard = I2C_BUS
                        .lock()
                        .unwrap_or_else(|_| panic!("I2C_BUS mutex poisoned in telemetry thread"));
                    match guard.as_mut() {
                        None => {
                            log::warn!("telemetry: I2C_BUS is None — boot init bug");
                            None
                        }
                        Some(drv) => {
                            let mut payload = [0u8; XVF3800_AEC_SPENERGY_READ_LEN];
                            match xvf3800_control_read(
                                drv,
                                XVF3800_AEC_RESID,
                                XVF3800_AEC_SPENERGY_VALUES_CMD,
                                XVF3800_AEC_SPENERGY_READ_LEN,
                                &mut payload,
                            ) {
                                Ok((0x00, _)) => {
                                    let [v0, v1, v2, v3] = decode_f32x4(&payload);
                                    Some([v0, v1, v2, v3])
                                }
                                Ok((status, _)) => {
                                    log::warn!("telemetry: SPENERGY status=0x{:02x}", status);
                                    None
                                }
                                Err(e) => {
                                    log::warn!("telemetry: SPENERGY I2C error: {:?}", e);
                                    None
                                }
                            }
                        }
                    }
                };

                let now_us = unsafe { esp_idf_svc::sys::esp_timer_get_time() } as u64;

                // ── VAD update ───────────────────────────────────────────────
                if let Some(sp) = sp_values_opt {
                    struct SpEnergySource(f32);
                    impl audio_pipeline::vad::VadSource for SpEnergySource {
                        fn energy(&self) -> f32 {
                            self.0
                        }
                    }
                    // While a HIL test has quiesced capture, feed silence so no new
                    // onset can fire (the Opened arm is this thread's only ring
                    // toucher); an already-open FSM still releases through the normal
                    // hangover path. NEG_INFINITY sits below every representable
                    // threshold under either FSM comparison.
                    let max_energy = if crate::capture::CAPTURE_QUIESCED
                        .load(std::sync::atomic::Ordering::Acquire)
                    {
                        f32::NEG_INFINITY
                    } else {
                        sp.iter().cloned().fold(f32::NEG_INFINITY, f32::max)
                    };
                    let transition = vad.update(&SpEnergySource(max_energy));

                    match transition {
                        VadTransition::Opened => {
                            let write_head = {
                                let guard = CAPTURE_RING.lock().unwrap_or_else(|_| {
                                    panic!("CAPTURE_RING mutex poisoned in telemetry thread")
                                });
                                guard
                                    .as_ref()
                                    .expect(
                                        "CAPTURE_RING is None in telemetry thread — boot init bug",
                                    )
                                    .write_head
                            };
                            log::info!(
                                "telemetry: VAD opened (write_head={} energy={:.3})",
                                write_head,
                                max_energy
                            );
                            // Clear before sending so the streamer sees a fresh flag.
                            VAD_CLOSED_FLAG.store(false, std::sync::atomic::Ordering::Release);
                            segment_open = true;
                            // On Full, reset segment_open to avoid pushing telemetry for a
                            // phantom segment (which could cascade to dropping VadClosed).
                            if tx.try_send(StreamerMsg::VadOpened { write_head }).is_err() {
                                telemetry_drops = telemetry_drops.saturating_add(1);
                                log::warn!(
                                    "telemetry: VadOpened dropped — streamer channel full; \
                                     utterance will be lost (drops so far this boot: {})",
                                    telemetry_drops
                                );
                                segment_open = false;
                            }
                        }
                        VadTransition::Closed => {
                            log::info!("telemetry: VAD closed (drops={})", telemetry_drops);
                            segment_open = false;
                            // Set unconditionally — the streamer polls this flag as a
                            // backup when the channel message is dropped.
                            VAD_CLOSED_FLAG.store(true, std::sync::atomic::Ordering::Release);
                            if tx.try_send(StreamerMsg::VadClosed).is_err() {
                                telemetry_drops = telemetry_drops.saturating_add(1);
                                log::warn!(
                                    "telemetry: VadClosed channel message dropped — \
                                     streamer will detect close via VAD_CLOSED_FLAG"
                                );
                            }
                        }
                        VadTransition::Unchanged => {}
                    }

                    // Forward SPENERGY telemetry while a segment is open.
                    if segment_open {
                        let tel = WireTelemetry {
                            device_ts_us: now_us,
                            kind: TelemetryKind::SpEnergy { values: sp },
                        };
                        match tx.try_send(StreamerMsg::Telemetry(tel)) {
                            Ok(()) => {}
                            Err(std::sync::mpsc::TrySendError::Full(_)) => {
                                telemetry_drops = telemetry_drops.saturating_add(1);
                            }
                            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                                log::warn!("telemetry: streamer channel disconnected");
                            }
                        }
                    }
                }

                // ── Poll DoA (every doa_every_n ticks) ──────────────────────
                if tick.is_multiple_of(doa_every_n) {
                    let az_values_opt: Option<[f32; 4]> = {
                        let mut guard = I2C_BUS.lock().unwrap_or_else(|_| {
                            panic!("I2C_BUS mutex poisoned in telemetry thread (DoA)")
                        });
                        match guard.as_mut() {
                            None => None,
                            Some(drv) => {
                                let mut payload = [0u8; XVF3800_AEC_AZIMUTH_READ_LEN];
                                match xvf3800_control_read(
                                    drv,
                                    XVF3800_AEC_RESID,
                                    XVF3800_AEC_AZIMUTH_VALUES_CMD,
                                    XVF3800_AEC_AZIMUTH_READ_LEN,
                                    &mut payload,
                                ) {
                                    Ok((0x00, _)) => {
                                        let [v0, v1, v2, v3] = decode_f32x4(&payload);
                                        Some([v0, v1, v2, v3])
                                    }
                                    Ok((status, _)) => {
                                        log::warn!("telemetry: DoA status=0x{:02x}", status);
                                        None
                                    }
                                    Err(e) => {
                                        log::warn!("telemetry: DoA I2C error: {:?}", e);
                                        None
                                    }
                                }
                            }
                        }
                    };

                    // Forward DoA telemetry while a segment is open.
                    if segment_open {
                        if let Some(az) = az_values_opt {
                            let doa_now_us =
                                unsafe { esp_idf_svc::sys::esp_timer_get_time() } as u64;
                            let tel = WireTelemetry {
                                device_ts_us: doa_now_us,
                                kind: TelemetryKind::Azimuths { values: az },
                            };
                            match tx.try_send(StreamerMsg::Telemetry(tel)) {
                                Ok(()) => {}
                                Err(std::sync::mpsc::TrySendError::Full(_)) => {
                                    telemetry_drops = telemetry_drops.saturating_add(1);
                                }
                                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                                    log::warn!("telemetry: streamer channel disconnected (DoA)");
                                }
                            }
                        }
                    }
                }

                tick = tick.wrapping_add(1);

                // 50 ms at 20 Hz.
                FreeRtos::delay_ms(1000 / VAD_POLL_HZ);
            }
        })
        .expect("telemetry: thread spawn failed — heap exhausted?");
}

/// Read the VAD threshold from NVS (`"audio"` namespace, `"vad_threshold"` key,
/// 4-byte LE f32 blob), or return `VAD_THRESHOLD_DEFAULT` on any error.
///
/// The blob decode + finite/non-negative guard live in
/// `audio_pipeline::vad::decode_vad_threshold` (host-tested). The NVS-plumbing arms
/// here (open error, key absent, get_blob error) are xtensa-only.
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
/// `VAD_HANGOVER_MS` default on any error. Mirrors `load_vad_threshold`.
///
/// The blob decode + range guard live in `audio_pipeline::vad::decode_vad_hangover_ms`
/// (host-tested). The NVS-plumbing arms here (open error, key absent, get_blob error)
/// are xtensa-only.
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
