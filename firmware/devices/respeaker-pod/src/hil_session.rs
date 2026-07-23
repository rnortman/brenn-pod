//! RAM-only HIL session store: volatile peer config + audio-PSK override.
//!
//! Everything a HIL run generates or pushes for a test session — the peer
//! endpoints and the session audio PSK — lives here in RAM only. A power cycle
//! clears it; the persisted NVS state is never touched by a HIL run.
//!
//! Module invariant: this module never imports or receives an NVS handle for
//! writing. The only NVS touch is the read inside [`effective_audio_psk`]. The
//! volatile handlers are typed such that they cannot persist — that structural
//! fact is the enforcement of the no-NVS-writes rule.
//!
//! The key-bearing types carry no `Debug` impl (the `PodPsk` precedent), so a
//! stray `{:?}` cannot leak key bytes into a log line.

// Host view: the slot logic carries host unit tests; the NVS-touching accessor
// and the command handlers are device-gated.
#![cfg_attr(not(target_os = "espidf"), allow(dead_code))]

use std::sync::Mutex;

#[cfg(target_os = "espidf")]
use esp_idf_svc::nvs::{EspNvs, NvsDefault};

/// Peer endpoints pushed by `SetTemporaryPeerConfig`. RAM-only; a reboot
/// discards it; overwriting is allowed (last write wins).
#[derive(Clone)]
pub(crate) struct PeerConfig {
    /// UDP echo / reachable-probe target host.
    pub(crate) host: [u8; 4],
    /// UDP echo port.
    pub(crate) udp_port: u16,
    /// Public TLS endpoint host (literal IP) for `TlsReachability`.
    pub(crate) tls_host: [u8; 4],
    /// Public TLS endpoint port.
    pub(crate) tls_port: u16,
    /// Inbound audio-frame source port.
    pub(crate) inbound_frames_port: u16,
    /// Send-backpressure source port.
    pub(crate) backpressure_port: u16,
    /// Poll-readiness adversary port.
    pub(crate) poll_readiness_port: u16,
    /// `StreamRealtimeDuplex` listener port.
    pub(crate) rtd_port: u16,
    /// TLS-PSK listener holding this pod's session audio key.
    pub(crate) tls_psk_port: u16,
    /// TLS-PSK listener holding a different key for this pod's identity.
    pub(crate) tls_psk_bad_port: u16,
}

/// The whole volatile session. No `Debug`: it holds the audio PSK.
struct HilSession {
    peer: Option<PeerConfig>,
    audio_psk: Option<[u8; 32]>,
}

static SESSION: Mutex<HilSession> = Mutex::new(HilSession {
    peer: None,
    audio_psk: None,
});

/// Overwrite `key` with zero bytes, resisting dead-store elimination. Used
/// before a key leaves the slot (clear) or is replaced (overwrite).
fn zeroize_key(key: &mut [u8; 32]) {
    for b in key.iter_mut() {
        // SAFETY: `b` is a valid, aligned `u8` in a live array; the volatile
        // write keeps the store from being optimized away.
        unsafe { core::ptr::write_volatile(b, 0u8) };
    }
}

/// Store the peer config, replacing any previous one (last write wins).
pub(crate) fn set_peer(cfg: PeerConfig) {
    let mut guard = SESSION
        .lock()
        .unwrap_or_else(|_| panic!("HIL SESSION mutex poisoned"));
    guard.peer = Some(cfg);
}

/// A clone of the current peer config, or `None` if none has been pushed.
pub(crate) fn peer_config() -> Option<PeerConfig> {
    let guard = SESSION
        .lock()
        .unwrap_or_else(|_| panic!("HIL SESSION mutex poisoned"));
    guard.peer.clone()
}

/// Store the audio-PSK override, zeroizing any prior key first (last write wins).
pub(crate) fn set_audio_psk(key: [u8; 32]) {
    let mut guard = SESSION
        .lock()
        .unwrap_or_else(|_| panic!("HIL SESSION mutex poisoned"));
    if let Some(old) = guard.audio_psk.as_mut() {
        zeroize_key(old);
    }
    guard.audio_psk = Some(key);
}

/// Clear the audio-PSK override, zeroizing the stored key. Returns whether an
/// override was present.
pub(crate) fn clear_audio_psk() -> bool {
    let mut guard = SESSION
        .lock()
        .unwrap_or_else(|_| panic!("HIL SESSION mutex poisoned"));
    match guard.audio_psk.as_mut() {
        Some(old) => {
            zeroize_key(old);
            guard.audio_psk = None;
            true
        }
        None => false,
    }
}

/// A copy of the current audio-PSK override, or `None` if none is set.
pub(crate) fn audio_psk_override() -> Option<[u8; 32]> {
    let guard = SESSION
        .lock()
        .unwrap_or_else(|_| panic!("HIL SESSION mutex poisoned"));
    guard.audio_psk
}

/// Precedence core of [`effective_audio_psk`], with the NVS read injected so the
/// three branches (override wins, NVS fallback, neither) are testable off-device.
#[allow(clippy::result_large_err)] // device_protocol::TestResultMsg is the no-alloc error type on no_std
pub(crate) fn effective_audio_psk_from<E>(
    override_key: Option<[u8; 32]>,
    nvs_read: impl FnOnce() -> Result<[u8; 32], E>,
) -> Result<[u8; 32], device_protocol::TestResultMsg> {
    if let Some(key) = override_key {
        return Ok(key);
    }
    nvs_read().map_err(|_| {
        let mut s = device_protocol::TestResultMsg::new();
        let _ = s.push_str(NO_AUDIO_PSK_MSG);
        s
    })
}

/// Failure text when neither an override nor a stored key exists; names both
/// provisioning paths.
pub(crate) const NO_AUDIO_PSK_MSG: &str = "no audio_psk override or NVS key — run ProvisionAudioPsk (podctl) or \
     SetTemporaryAudioPsk (HIL) first";

/// The audio PSK every consumer should use: the RAM override if set, else the
/// persisted NVS key. This is the single accessor through which the override
/// shadows NVS.
#[cfg(target_os = "espidf")]
#[allow(clippy::result_large_err)] // device_protocol::TestResultMsg is the no-alloc error type on no_std
pub(crate) fn effective_audio_psk(
    nvs: &EspNvs<NvsDefault>,
) -> Result<[u8; 32], device_protocol::TestResultMsg> {
    effective_audio_psk_from(audio_psk_override(), || {
        crate::nvs::nvs_get_blob32(nvs, "audio_psk")
    })
}

/// Apply a RAM-only peer config override (`SetTemporaryPeerConfig`). Never
/// written to flash; a reboot discards it.
#[cfg(target_os = "espidf")]
pub(crate) fn handle_set_temporary_peer_config(
    cfg: PeerConfig,
) -> (device_protocol::Status, device_protocol::Payload) {
    set_peer(cfg);
    log::info!("hil session: peer config set (RAM-only; reboot clears)");
    (device_protocol::Status::Ok, device_protocol::Payload::Empty)
}

/// Apply a RAM-only audio-PSK override (`SetTemporaryAudioPsk`).
///
/// Fails without storing anything if the pod identity is not yet available: the
/// response carries the identity (authoritative only on-device), so there is
/// nothing useful to do without it. Never echoes the key.
///
/// The override takes effect for every audio-PSK read made after this call.
// TODO(hil-streamer-psk-quiesce): make a running streamer observe the override.
#[cfg(target_os = "espidf")]
pub(crate) fn handle_set_temporary_audio_psk(
    key: [u8; 32],
) -> (device_protocol::Status, device_protocol::Payload) {
    let (store, response) = set_temporary_audio_psk_outcome(crate::streamer::pod_id_snapshot());
    if store {
        set_audio_psk(key);
        log::info!(
            "hil session: audio_psk override set (32 bytes, RAM-only); applies to key reads from \
             now on (HIL tests) — a streamer provisioned at boot keeps its boot-time key until \
             reboot"
        );
    }
    response
}

/// Decision core of [`handle_set_temporary_audio_psk`]: given the pod identity (or
/// its absence), whether the key may be stored and what to answer. Pure, so both
/// arms — including "rejected stores nothing" — are testable off-device.
pub(crate) fn set_temporary_audio_psk_outcome(
    pod_id: Option<heapless::String<32>>,
) -> (bool, (device_protocol::Status, device_protocol::Payload)) {
    match pod_id {
        Some(pod_id) => (
            true,
            (
                device_protocol::Status::Ok,
                device_protocol::Payload::PodId(pod_id),
            ),
        ),
        None => (
            false,
            device_protocol::test_report_fail_fmt(format_args!(
                "pod identity not yet initialized; retry after the wifi stack starts"
            )),
        ),
    }
}

/// Clear the RAM-only audio-PSK override, zeroizing the slot. A clear with no
/// override active is a pure no-op returning `Ok`.
#[cfg(target_os = "espidf")]
pub(crate) fn handle_clear_temporary_audio_psk()
-> (device_protocol::Status, device_protocol::Payload) {
    if clear_audio_psk() {
        log::info!("hil session: audio_psk override cleared and zeroized");
    }
    (device_protocol::Status::Ok, device_protocol::Payload::Empty)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_peer() -> PeerConfig {
        PeerConfig {
            host: [10, 0, 0, 5],
            udp_port: 7001,
            tls_host: [1, 1, 1, 1],
            tls_port: 8443,
            inbound_frames_port: 7002,
            backpressure_port: 7003,
            poll_readiness_port: 7004,
            rtd_port: 7005,
            tls_psk_port: 7006,
            tls_psk_bad_port: 7007,
        }
    }

    // The static SESSION is process-global and cargo runs tests in parallel, so
    // every test acquires TEST_LOCK before touching it: order-independence is not
    // concurrency-safety. The guard is held for the whole test body so no two
    // tests race on the shared slots. Poison (a prior test panicking mid-body) is
    // recovered — a failed assertion must not cascade into unrelated tests.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn lock_session() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    // Caller must hold TEST_LOCK.
    fn reset() {
        let mut guard = SESSION.lock().unwrap();
        guard.peer = None;
        if let Some(old) = guard.audio_psk.as_mut() {
            zeroize_key(old);
        }
        guard.audio_psk = None;
    }

    #[test]
    fn peer_set_and_read_back() {
        let _lock = lock_session();
        reset();
        assert!(peer_config().is_none());
        set_peer(sample_peer());
        let got = peer_config().expect("peer set");
        assert_eq!(got.host, [10, 0, 0, 5]);
        assert_eq!(got.rtd_port, 7005);
        assert_eq!(got.tls_psk_bad_port, 7007);
    }

    #[test]
    fn peer_last_write_wins() {
        let _lock = lock_session();
        reset();
        set_peer(sample_peer());
        let mut second = sample_peer();
        second.host = [192, 168, 1, 9];
        set_peer(second);
        assert_eq!(peer_config().unwrap().host, [192, 168, 1, 9]);
    }

    #[test]
    fn audio_psk_override_precedence_and_clear() {
        let _lock = lock_session();
        reset();
        assert!(audio_psk_override().is_none());
        let key = [7u8; 32];
        set_audio_psk(key);
        assert_eq!(audio_psk_override(), Some(key));
        // Clear reports it was present, then reads back none.
        assert!(clear_audio_psk());
        assert!(audio_psk_override().is_none());
        // Clearing again is a no-op.
        assert!(!clear_audio_psk());
    }

    #[test]
    fn audio_psk_overwrite_last_write_wins() {
        let _lock = lock_session();
        reset();
        set_audio_psk([1u8; 32]);
        set_audio_psk([2u8; 32]);
        assert_eq!(audio_psk_override(), Some([2u8; 32]));
        reset();
    }

    /// The volatile overwrite itself, asserted directly: the slot-level tests above
    /// pass identically if `zeroize_key` were a no-op, so the secret-hygiene
    /// requirement needs its own check.
    #[test]
    fn zeroize_key_overwrites_every_byte() {
        let mut key = [0xABu8; 32];
        zeroize_key(&mut key);
        assert_eq!(key, [0u8; 32], "zeroize must overwrite every key byte");
    }

    #[test]
    fn effective_audio_psk_override_beats_nvs() {
        let got = effective_audio_psk_from(Some([9u8; 32]), || Ok::<_, ()>([1u8; 32]))
            .expect("override present");
        assert_eq!(got, [9u8; 32], "a live override must shadow the NVS key");
    }

    #[test]
    fn effective_audio_psk_falls_back_to_nvs() {
        let got =
            effective_audio_psk_from(None, || Ok::<_, ()>([1u8; 32])).expect("NVS key present");
        assert_eq!(got, [1u8; 32], "with no override the NVS key is used");
    }

    #[test]
    fn effective_audio_psk_names_both_paths_when_neither_exists() {
        let err = effective_audio_psk_from(None, || Err::<[u8; 32], ()>(()))
            .expect_err("neither source available");
        assert_eq!(
            err.as_str(),
            NO_AUDIO_PSK_MSG,
            "the failure must name both provisioning paths verbatim"
        );
    }

    #[test]
    fn set_temporary_audio_psk_rejected_without_pod_id() {
        let _lock = lock_session();
        reset();
        let (store, (status, payload)) = set_temporary_audio_psk_outcome(None);
        assert!(!store, "a rejected set must not store the key");
        assert_eq!(status, device_protocol::Status::Fail);
        assert!(
            matches!(payload, device_protocol::Payload::TestReport(_)),
            "the rejection carries a typed failure detail"
        );
        // A rejected outcome must leave the slot untouched.
        assert!(audio_psk_override().is_none());
    }

    #[test]
    fn set_temporary_audio_psk_answers_pod_id_on_success() {
        let mut pod_id: heapless::String<32> = heapless::String::new();
        pod_id.push_str("pod-abc").unwrap();
        let (store, (status, payload)) = set_temporary_audio_psk_outcome(Some(pod_id.clone()));
        assert!(store, "an accepted set stores the key");
        assert_eq!(status, device_protocol::Status::Ok);
        match payload {
            device_protocol::Payload::PodId(id) => assert_eq!(id, pod_id),
            other => panic!("expected a PodId payload, got {other:?}"),
        }
    }
}
