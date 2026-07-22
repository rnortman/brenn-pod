//! NVS-backed provisioning: open namespace helpers, typed blob/string reads, and
//! the `Provision*` / `SetVadThreshold` command handlers.

//! Every prod item here is esp-idf FFI (`EspNvs`) and is gated per-item
//! `#[cfg(target_os = "espidf")]`. The module itself stays host-visible so a
//! `#[cfg(test)]` module added here compiles and runs on the host.

#[cfg(target_os = "espidf")]
use esp_idf_svc::nvs::{EspNvs, NvsDefault};

#[cfg(target_os = "espidf")]
use audio_pipeline::vad::vad_threshold_ok;
#[cfg(target_os = "espidf")]
use device_protocol::{test_report_fail_detail, test_report_fail_fmt, Payload, Status};
#[cfg(target_os = "espidf")]
use wifi_diag::fmt_ipv4;

#[cfg(target_os = "espidf")]
use crate::hil::test_report_fail_msg;
#[cfg(target_os = "espidf")]
use crate::{ring_wifi_wake, WIFI_STACK};

/// Open an NVS namespace. Pass `read_write = true` to enable writes.
#[allow(clippy::result_large_err)] // device_protocol::TestResultMsg is the no-alloc error type on no_std
#[cfg(target_os = "espidf")]
fn open_nvs_namespace(
    name: &str,
    read_write: bool,
) -> Result<EspNvs<NvsDefault>, device_protocol::TestResultMsg> {
    let guard = WIFI_STACK
        .lock()
        .unwrap_or_else(|_| panic!("WIFI_STACK mutex poisoned"));
    let nvs_part = guard
        .as_ref()
        .ok_or_else(|| {
            let mut s = device_protocol::TestResultMsg::new();
            let _ = s.push_str("nvs partition not initialized");
            s
        })?
        .nvs
        .clone();
    drop(guard);
    EspNvs::new(nvs_part, name, read_write).map_err(|e| {
        let mut s = device_protocol::TestResultMsg::new();
        let _ = core::fmt::write(&mut s, format_args!("nvs open {} ns failed: {:?}", name, e));
        s
    })
}

/// Open the `"wifi"` NVS namespace.
#[allow(clippy::result_large_err)] // device_protocol::TestResultMsg is the no-alloc error type on no_std
#[cfg(target_os = "espidf")]
pub(crate) fn open_wifi_nvs(
    read_write: bool,
) -> Result<EspNvs<NvsDefault>, device_protocol::TestResultMsg> {
    open_nvs_namespace("wifi", read_write)
}

/// Open the `"audio"` NVS namespace.
#[allow(clippy::result_large_err)] // device_protocol::TestResultMsg is the no-alloc error type on no_std
#[cfg(target_os = "espidf")]
pub(crate) fn open_audio_nvs(
    read_write: bool,
) -> Result<EspNvs<NvsDefault>, device_protocol::TestResultMsg> {
    open_nvs_namespace("audio", read_write)
}

/// Read a 4-byte blob from the `"wifi"` NVS namespace.
#[allow(clippy::result_large_err)] // device_protocol::TestResultMsg is the no-alloc error type on no_std
#[cfg(target_os = "espidf")]
pub(crate) fn nvs_get_blob4(
    nvs: &EspNvs<NvsDefault>,
    key: &str,
) -> Result<[u8; 4], device_protocol::TestResultMsg> {
    let mut buf = [0u8; 4];
    match nvs.get_blob(key, &mut buf) {
        Ok(Some(b)) if b.len() == 4 => Ok([b[0], b[1], b[2], b[3]]),
        Ok(_) => {
            let mut s = device_protocol::TestResultMsg::new();
            let _ = core::fmt::write(
                &mut s,
                format_args!("no {} in NVS — run ProvisionPeer first", key),
            );
            Err(s)
        }
        Err(e) => {
            let mut s = device_protocol::TestResultMsg::new();
            let _ = core::fmt::write(&mut s, format_args!("nvs get_blob {} failed: {:?}", key, e));
            Err(s)
        }
    }
}

/// Read the 32-byte audio-link PSK blob from the `"wifi"` NVS namespace.
///
/// A short or absent blob is reported as unprovisioned rather than padded — a
/// truncated key would fail the TLS handshake with a far less obvious message.
#[allow(clippy::result_large_err)] // device_protocol::TestResultMsg is the no-alloc error type on no_std
#[cfg(target_os = "espidf")]
pub(crate) fn nvs_get_blob32(
    nvs: &EspNvs<NvsDefault>,
    key: &str,
) -> Result<[u8; 32], device_protocol::TestResultMsg> {
    let mut buf = [0u8; 32];
    match nvs.get_blob(key, &mut buf) {
        Ok(Some(b)) if b.len() == 32 => {
            let mut out = [0u8; 32];
            out.copy_from_slice(b);
            Ok(out)
        }
        Ok(_) => {
            let mut s = device_protocol::TestResultMsg::new();
            let _ = core::fmt::write(
                &mut s,
                format_args!("no {} in NVS — run ProvisionAudioPsk first", key),
            );
            Err(s)
        }
        Err(e) => {
            let mut s = device_protocol::TestResultMsg::new();
            let _ = core::fmt::write(&mut s, format_args!("nvs get_blob {} failed: {:?}", key, e));
            Err(s)
        }
    }
}

/// Read a string NVS key from the `"wifi"` namespace into a fixed buffer.
/// Returns `None` if the key is absent, `Some(str)` if present.
/// Returns `Err` with message on NVS error.
#[allow(clippy::result_large_err)] // device_protocol::TestResultMsg is the no-alloc error type on no_std
#[cfg(target_os = "espidf")]
pub(crate) fn nvs_get_str<'a>(
    nvs: &EspNvs<NvsDefault>,
    key: &str,
    buf: &'a mut [u8],
) -> Result<Option<&'a str>, device_protocol::TestResultMsg> {
    nvs.get_str(key, buf).map_err(|e| {
        let mut s = device_protocol::TestResultMsg::new();
        let _ = core::fmt::write(&mut s, format_args!("nvs get_str {} failed: {:?}", key, e));
        s
    })
}

/// Write SSID + passphrase to NVS and wake the WiFi supervisor to associate.
#[cfg(target_os = "espidf")]
pub(crate) fn handle_provision_wifi(
    ssid: heapless::String<32>,
    passphrase: heapless::String<64>,
) -> (Status, Payload) {
    let nvs = match open_wifi_nvs(true) {
        Ok(n) => n,
        Err(msg) => return test_report_fail_msg(msg),
    };
    if let Err(e) = nvs.set_str("ssid", ssid.as_str()) {
        return test_report_fail_detail("nvs set_str ssid failed", &e);
    }
    if let Err(e) = nvs.set_str("pass", passphrase.as_str()) {
        return test_report_fail_detail("nvs set_str pass failed", &e);
    }
    log::info!("provisioned wifi ssid={}", ssid.as_str());
    ring_wifi_wake();
    (Status::Ok, Payload::Empty)
}

/// Erase WiFi credentials ("ssid"/"pass") from NVS, clear any active temporary
/// override, drop the link, and wake the supervisor so it parks on the
/// credential-less state.
///
/// The temporary override is cleared too: `wifi_credentials_present()` consults it,
/// so leaving it set would keep the radio associating past this clear, breaking the
/// "make the device forget WiFi and park" guarantee this command exists for.
///
/// Idempotent: `remove` returning `Ok(false)` (key absent) is success. Other
/// `"wifi"`-namespace keys (peer/audio provisioning) are untouched.
///
/// The NVS handle is opened *before* the temporary override is cleared: `open_wifi_nvs`
/// is fallible, and this command's contract on `Status::Fail` is "nothing changed" (or,
/// for the partial-clear branch below, an explicit "link dropped" note). Clearing the
/// override first and then failing to open NVS would silently mutate state on a Fail
/// response — the override gone but nothing else touched, with no disconnect/wake — so
/// the fallible step runs first.
#[cfg(target_os = "espidf")]
pub(crate) fn handle_clear_wifi_credentials() -> (Status, Payload) {
    let nvs = match open_wifi_nvs(true) {
        Ok(n) => n,
        Err(msg) => return test_report_fail_msg(msg),
    };
    if crate::wifi::clear_temp_config_no_wake() {
        log::info!("cleared temporary wifi config override (ClearWifiCredentials)");
    }
    // NVS state and radio state must not disagree, including on partial failure: any
    // outcome that removed at least one key converges the radio to parked before
    // returning, so the device never keeps running on credentials it can no longer
    // re-associate from.
    let ssid_err = nvs.remove("ssid").err();
    let pass_err = nvs.remove("pass").err();
    if ssid_err.is_some() || pass_err.is_some() {
        // Drop the link regardless: `associate_from_active_config` treats a missing-or-empty ssid
        // as "no credentials", so a half-cleared device is already unprovisionable.
        let _ = crate::wifi::force_disconnect_wifi();
        ring_wifi_wake();
        if let Some(e) = ssid_err {
            return test_report_fail_detail(
                "nvs remove ssid failed (credentials partially removed; link dropped)",
                &e,
            );
        }
        let e = pass_err.expect("pass_err is Some when ssid_err is None in this branch");
        return test_report_fail_detail("nvs remove pass failed (ssid removed; link dropped)", &e);
    }
    log::info!("cleared wifi credentials");
    let _ = crate::wifi::force_disconnect_wifi();
    ring_wifi_wake();
    (Status::Ok, Payload::Empty)
}

/// The HIL peer endpoints one `ProvisionPeer` writes, bundled.
///
/// A bundle rather than a parameter list: the handler would otherwise take eleven
/// argument words, past the six the Xtensa realign-miscompile guard tolerates
/// (`TODO(xtensa-realign-stack-args)`, `firmware/tools/check-realign-args.sh`).
#[cfg(target_os = "espidf")]
pub(crate) struct PeerEndpoints {
    /// UDP echo port.
    pub(crate) udp_port: u16,
    /// TCP echo port.
    pub(crate) tcp_port: u16,
    /// Public TLS endpoint address, reached by literal IP.
    pub(crate) tls_host: [u8; 4],
    /// Public TLS endpoint port.
    pub(crate) tls_port: u16,
    /// Inbound audio-frame source port.
    pub(crate) inbound_frames_port: u16,
    /// Backpressure source port.
    pub(crate) backpressure_port: u16,
    /// Poll-readiness adversary port.
    pub(crate) poll_readiness_port: u16,
    /// `StreamRealtimeDuplex` listener port.
    pub(crate) rtd_port: u16,
    /// TLS-PSK listener holding this pod's real audio-link key.
    pub(crate) tls_psk_port: u16,
    /// TLS-PSK listener holding a different key for this pod's identity.
    pub(crate) tls_psk_bad_port: u16,
}

/// Write peer host/port configuration to NVS.
#[cfg(target_os = "espidf")]
pub(crate) fn handle_provision_peer(host: [u8; 4], ports: &PeerEndpoints) -> (Status, Payload) {
    let PeerEndpoints {
        udp_port,
        tcp_port,
        tls_host,
        tls_port,
        inbound_frames_port,
        backpressure_port,
        poll_readiness_port,
        rtd_port,
        tls_psk_port,
        tls_psk_bad_port,
    } = *ports;
    let nvs = match open_wifi_nvs(true) {
        Ok(n) => n,
        Err(msg) => return test_report_fail_msg(msg),
    };
    if let Err(e) = nvs.set_blob("peer_ip", &host) {
        return test_report_fail_detail("nvs set_blob peer_ip failed", &e);
    }
    if let Err(e) = nvs.set_blob("tls_host", &tls_host) {
        return test_report_fail_detail("nvs set_blob tls_host failed", &e);
    }
    if let Err(e) = nvs.set_u16("peer_udp", udp_port) {
        return test_report_fail_detail("nvs set_u16 peer_udp failed", &e);
    }
    if let Err(e) = nvs.set_u16("peer_tcp", tcp_port) {
        return test_report_fail_detail("nvs set_u16 peer_tcp failed", &e);
    }
    if let Err(e) = nvs.set_u16("peer_tls", tls_port) {
        return test_report_fail_detail("nvs set_u16 peer_tls failed", &e);
    }
    if let Err(e) = nvs.set_u16("peer_inb_tcp", inbound_frames_port) {
        return test_report_fail_detail("nvs set_u16 peer_inb_tcp failed", &e);
    }
    if let Err(e) = nvs.set_u16("peer_bp_tcp", backpressure_port) {
        return test_report_fail_detail("nvs set_u16 peer_bp_tcp failed", &e);
    }
    if let Err(e) = nvs.set_u16("peer_poll_tcp", poll_readiness_port) {
        return test_report_fail_detail("nvs set_u16 peer_poll_tcp failed", &e);
    }
    if let Err(e) = nvs.set_u16("peer_rtd_tcp", rtd_port) {
        return test_report_fail_detail("nvs set_u16 peer_rtd_tcp failed", &e);
    }
    if let Err(e) = nvs.set_u16("peer_psk_tcp", tls_psk_port) {
        return test_report_fail_detail("nvs set_u16 peer_psk_tcp failed", &e);
    }
    if let Err(e) = nvs.set_u16("peer_psk_bad", tls_psk_bad_port) {
        return test_report_fail_detail("nvs set_u16 peer_psk_bad failed", &e);
    }
    log::info!(
        "provisioned peer host={} udp={} tcp={} tls_host={} tls={} inbound_tcp={} bp_tcp={} \
         poll_tcp={} rtd_tcp={} psk_tcp={} pskbad_tcp={}",
        fmt_ipv4(host),
        udp_port,
        tcp_port,
        fmt_ipv4(tls_host),
        tls_port,
        inbound_frames_port,
        backpressure_port,
        poll_readiness_port,
        rtd_port,
        tls_psk_port,
        tls_psk_bad_port,
    );
    (Status::Ok, Payload::Empty)
}

/// Write audio receiver address + port to NVS.
#[cfg(target_os = "espidf")]
pub(crate) fn handle_provision_audio(host: [u8; 4], port: u16) -> (Status, Payload) {
    let nvs = match open_wifi_nvs(true) {
        Ok(n) => n,
        Err(msg) => return test_report_fail_msg(msg),
    };
    if let Err(e) = nvs.set_blob("audio_ip", &host) {
        return test_report_fail_detail("nvs set_blob audio_ip failed", &e);
    }
    if let Err(e) = nvs.set_u16("audio_port", port) {
        return test_report_fail_detail("nvs set_u16 audio_port failed", &e);
    }
    log::info!("provisioned audio host={} port={}", fmt_ipv4(host), port);
    (Status::Ok, Payload::Empty)
}

/// Write the 32-byte audio-link PSK to NVS and answer with this pod's identity.
///
/// The response carries the MAC-derived pod id, never the key: the provisioning host
/// needs the identity to key its own `pod_id → key` table, and that identity is
/// authoritative only on the device. Neither the key bytes nor any digest of them are
/// logged.
///
/// `POD_ID` is filled during WiFi stack initialization. If it is still empty the
/// command fails *before* touching NVS: a caller that recorded an empty identity would
/// build a host table entry no handshake can ever match, and a half-applied provision
/// (key stored, identity unknown) is worse than none.
#[cfg(target_os = "espidf")]
pub(crate) fn handle_provision_audio_psk(key: [u8; 32]) -> (Status, Payload) {
    let Some(pod_id) = crate::streamer::pod_id_snapshot() else {
        return test_report_fail_fmt(format_args!(
            "pod identity not yet initialized; retry after the wifi stack starts"
        ));
    };
    let nvs = match open_wifi_nvs(true) {
        Ok(n) => n,
        Err(msg) => return test_report_fail_msg(msg),
    };
    if let Err(e) = nvs.set_blob("audio_psk", &key) {
        return test_report_fail_detail("nvs set_blob audio_psk failed", &e);
    }
    log::info!(
        "provisioned audio_psk (32 bytes) pod_id={} (applies on next streamer connect)",
        pod_id.as_str()
    );
    (Status::Ok, Payload::PodId(pod_id))
}

/// Validate and write a VAD threshold to NVS. Applied on next boot.
/// Rejects NaN, ±Inf, and negative values via the shared `vad_threshold_ok` guard.
#[cfg(target_os = "espidf")]
pub(crate) fn handle_set_vad_threshold(threshold: f32) -> (Status, Payload) {
    if !vad_threshold_ok(threshold) {
        return test_report_fail_fmt(format_args!(
            "invalid threshold {}: must be finite and >= 0.0",
            threshold
        ));
    }
    let nvs = match open_audio_nvs(true) {
        Ok(n) => n,
        Err(msg) => return test_report_fail_msg(msg),
    };
    if let Err(e) = nvs.set_blob("vad_threshold", &threshold.to_le_bytes()) {
        return test_report_fail_detail("nvs set_blob vad_threshold failed", &e);
    }
    log::info!(
        "provisioned vad_threshold={} (applies on next boot)",
        threshold
    );
    (Status::Ok, Payload::Empty)
}

/// Write the device VAD hangover (milliseconds) to NVS. Applied on next boot.
///
/// Stored as a 4-byte little-endian `u32` blob under `"vad_hangover_ms"` in the
/// `"audio"` namespace, mirroring `handle_set_vad_threshold`.
#[cfg(target_os = "espidf")]
pub(crate) fn handle_set_vad_hangover(hangover_ms: u32) -> (Status, Payload) {
    let nvs = match open_audio_nvs(true) {
        Ok(n) => n,
        Err(msg) => return test_report_fail_msg(msg),
    };
    if let Err(e) = nvs.set_blob("vad_hangover_ms", &hangover_ms.to_le_bytes()) {
        return test_report_fail_detail("nvs set_blob vad_hangover_ms failed", &e);
    }
    log::info!(
        "provisioned vad_hangover_ms={} (applies on next boot)",
        hangover_ms
    );
    (Status::Ok, Payload::Empty)
}
