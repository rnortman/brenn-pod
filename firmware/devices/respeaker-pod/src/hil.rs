//! HIL self-test result helpers and registry dispatch.
//!
//! The `test_report_*` builders in `device_protocol` produce the
//! `(Status, Payload::TestReport)` pairs every self-test handler returns;
//! [`DebugF32`] formats f32 values with canonical special-value tokens for failure
//! detail. [`run_handler`] is the single dispatch point that maps each `TestName`
//! to its handler.

// Host view: these items exist for the tests and for the device-gated call sites.
#![cfg_attr(not(target_os = "espidf"), allow(dead_code))]

use device_protocol::{Payload, Status};

#[cfg(target_os = "espidf")]
use build_id::build_id;
#[cfg(target_os = "espidf")]
use device_protocol::{TestName, MAX_TESTS, REGISTERED_TESTS};

#[cfg(target_os = "espidf")]
use crate::{
    run_amp_always_on_gpo_inert, run_capture_periodic_line, run_device_health_check,
    run_full_duplex_rx_integrity, run_gateway_probe_gate, run_gpio_self_test, run_i2c_bus_scan,
    run_i2s_waveform_sanity, run_playback_drain_rate, run_poll_readiness_bidir, run_psram_identity,
    run_speaker_output, run_stream_realtime_duplex, run_tls_inbound_backpressure,
    run_tls_inbound_frames, run_tls_reachability, run_tls_send_backpressure, run_udp_roundtrip,
    run_wifi_associate, run_wifi_power_save_check, run_wifi_reassociation, run_wifi_scan,
    run_xvf3800_doa_plausibility, run_xvf3800_reg_read, run_xvf3800_sp_energy,
};

// ── Self-test result helpers ──────────────────────────────────────────────────

/// Build a `(Status::Fail, Payload::TestReport)` pair from an already-built message.
///
/// The NVS helpers (`open_wifi_nvs`, `nvs_get_blob4`) return a ready `TestResultMsg` on
/// error; this wraps one as failure detail with no typed data.
pub(crate) fn test_report_fail_msg(msg: device_protocol::TestResultMsg) -> (Status, Payload) {
    (
        Status::Fail,
        Payload::TestReport(device_protocol::TestReport {
            detail: msg,
            data: device_protocol::TestData::None,
        }),
    )
}

// Typed-report builders (`test_report_ok`, `test_report_ok_detail`,
// `test_report_fail_fmt`, `test_report_fail_detail`) also live in `device_protocol`.
//
// Response-frame capacity: the worst-case `TestReport` is bounded against
// `device_protocol::RESPONSE_FRAME_BUF` (the encode buffer used in `main.rs`) by the
// `response_frame_fits_encode_buf` test in `device_protocol`.

// ── Self-test registry ────────────────────────────────────────────────────────

/// Execute a self-test by name. Shared handler for all `Command::RunTest(…)` dispatch.
#[cfg(target_os = "espidf")]
pub(crate) fn run_handler(name: TestName) -> (Status, Payload) {
    match name {
        TestName::Ping => (
            Status::Ok,
            Payload::Pong({
                let mut s = heapless::String::<64>::new();
                let _ = s.push_str("pong");
                s
            }),
        ),
        TestName::Identify => {
            let build = build_id();
            let mut tests: heapless::Vec<TestName, MAX_TESTS> = heapless::Vec::new();
            for &t in REGISTERED_TESTS {
                tests
                    .push(t)
                    .unwrap_or_else(|_| panic!("test registry overflow — increase MAX_TESTS"));
            }
            (Status::Ok, Payload::Identify { build, tests })
        }
        TestName::GpioSelfTest => run_gpio_self_test(),
        TestName::DeviceHealthCheck => run_device_health_check(),
        TestName::I2cBusScan => run_i2c_bus_scan(),
        TestName::Xvf3800RegRead => run_xvf3800_reg_read(),
        TestName::Xvf3800DoAPlausibility => run_xvf3800_doa_plausibility(),
        TestName::I2sWaveformSanity => run_i2s_waveform_sanity(),
        TestName::WifiAssociate => run_wifi_associate(),
        TestName::UdpRoundtrip => run_udp_roundtrip(),
        TestName::TlsReachability => run_tls_reachability(),
        TestName::WifiScan => run_wifi_scan(),
        TestName::Xvf3800SpEnergy => run_xvf3800_sp_energy(),
        TestName::WifiReassociation => run_wifi_reassociation(),
        TestName::GatewayProbeGate => run_gateway_probe_gate(),
        TestName::TlsInboundFrames => run_tls_inbound_frames(),
        TestName::TlsSendBackpressure => run_tls_send_backpressure(),
        TestName::SpeakerOutput => run_speaker_output(),
        TestName::AmpAlwaysOnGpoInert => run_amp_always_on_gpo_inert(),
        TestName::CapturePeriodicLine => run_capture_periodic_line(),
        TestName::PlaybackDrainRate => run_playback_drain_rate(),
        TestName::PollReadinessBidir => run_poll_readiness_bidir(),
        TestName::FullDuplexRxIntegrity => run_full_duplex_rx_integrity(),
        TestName::StreamRealtimeDuplex => run_stream_realtime_duplex(),
        TestName::PsramIdentity => run_psram_identity(),
        TestName::WifiPowerSaveCheck => run_wifi_power_save_check(),
        TestName::TlsInboundBackpressure => run_tls_inbound_backpressure(),
        TestName::TlsPskHandshake => crate::net_tests::run_tls_psk_handshake(),
        TestName::TlsPskWrongKeyRejected => crate::net_tests::run_tls_psk_wrong_key_rejected(),
    }
}

// ── f32 formatting for HIL payloads ───────────────────────────────────────────

/// Formatting wrapper for f32 with canonical tokens for special values.
///
/// Prints `"nan"` for NaN, `"inf"` / `"-inf"` for ±Inf, and `{:.6e}` (scientific
/// notation) for all finite values. Scientific notation keeps large-magnitude values
/// compact, so a failure detail carrying several of them stays inside
/// `TEST_RESULT_MSG_CAP` instead of being truncated.
pub(crate) struct DebugF32(pub(crate) f32);

impl core::fmt::Display for DebugF32 {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        if self.0.is_nan() {
            f.write_str("nan")
        } else if self.0.is_infinite() {
            if self.0.is_sign_positive() {
                f.write_str("inf")
            } else {
                f.write_str("-inf")
            }
        } else {
            write!(f, "{:.6e}", self.0)
        }
    }
}
