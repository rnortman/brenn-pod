//! Device↔host protocol types — shared between device firmware and host harness.
//!
//! Encoding/framing (postcard + COBS) is an opt-in concern: enable the `framing`
//! Cargo feature to compile `device_protocol::framing`. Schema-only consumers
//! carry zero postcard/encoding dependency by default.
//!
//! All structs carry `#[serde(deny_unknown_fields)]`; all enums are exhaustive
//! (no catch-all variant). A wire shape the harness was not compiled against
//! → hard postcard/serde error, never silent ignore.

#![cfg_attr(not(test), no_std)]

// `alloc` is only needed in tests (two_frames_decode_as_two, corrupt_middle_frame_skipped_next_decodes
// use alloc::vec!). Production (no_std) code paths do not allocate; the device relies on the
// esp-idf-svc global allocator only if its own code calls alloc, which it does not.
// Gating here avoids the crate implicitly requiring a global allocator on any no_std target.
#[cfg(test)]
extern crate alloc;

use heapless::Vec as HVec;
use serde::{Deserialize, Serialize};

// ── Capacity constants (schema-level; part of the wire contract) ──────────────

/// Maximum number of registered self-tests reported in `Payload::Identify`.
///
/// Wire-contract capacity: `Payload::Identify::tests` is typed as
/// `HVec<TestName, MAX_TESTS>`.  This constant must never be decreased.
/// It may be larger than `REGISTERED_TESTS.len()` (slack capacity is fine).
pub const MAX_TESTS: usize = 30;

/// The canonical, ordered registry of self-tests a conforming device advertises
/// in `Payload::Identify` and the host harness expects (HIL check 3).
///
/// **SINGLE SOURCE OF TRUTH.** The device builds its `Identify` vector from this
/// slice; the host builds its expected set from this slice. They cannot drift because
/// there is only one list.
///
/// Order matters for the wire vector. Membership — not order — is what check 3
/// compares, so reordering is wire-compatible. Keep it aligned with enum declaration
/// order for readability.
///
/// Adding a `TestName` variant without adding it here is caught at `cargo test` time
/// by `registered_tests_covers_all_variants` in `device-protocol`'s test suite.
///
/// TODO(post-feed-heap-durable-guard): `DeviceHealthCheck` runs once, at position 4 —
/// before `FullDuplexRxIntegrity` (position 24) — so no routine suite run samples heap
/// after the saturated-playback feed. The post-feed trough that discharged
/// `heap-gate-measure` (`docs/adr/2026/07/19-heap-gate-measure/implementation-log.md`) is
/// a one-time human-asserted number with no regression guard. `heap-gate-measure`'s design
/// deliberately scoped out a new durable test (design.md §5: "No new automated tests");
/// adding a second post-feed health sample to close this gap is a design decision for a
/// future item, not a call to make here.
pub const REGISTERED_TESTS: &[TestName] = &[
    TestName::Ping,
    TestName::Identify,
    TestName::GpioSelfTest,
    TestName::DeviceHealthCheck,
    TestName::I2cBusScan,
    TestName::Xvf3800RegRead,
    TestName::Xvf3800DoAPlausibility,
    TestName::I2sWaveformSanity,
    TestName::WifiAssociate,
    TestName::UdpRoundtrip,
    TestName::TcpRoundtrip,
    TestName::TlsReachability,
    TestName::WifiScan,
    TestName::Xvf3800SpEnergy,
    TestName::WifiReassociation,
    TestName::GatewayProbeGate,
    TestName::TcpInboundFrames,
    TestName::TcpSendBackpressure,
    TestName::SpeakerOutput,
    TestName::AmpAlwaysOnGpoInert,
    TestName::CapturePeriodicLine,
    TestName::PlaybackDrainRate,
    TestName::PollReadinessBidir,
    TestName::FullDuplexRxIntegrity,
    TestName::StreamRealtimeDuplex,
    TestName::PsramIdentity,
    TestName::WifiPowerSaveCheck,
    TestName::TcpInboundBackpressure,
    TestName::TlsPskHandshake,
    TestName::TlsPskWrongKeyRejected,
];

// ── `TcpInboundFrames` drain-budget tuning constants ──────────────────────────
//
// **Tuning constants, NOT wire/serde schema.** Unlike `MAX_TESTS` / `REGISTERED_TESTS`
// above (which are part of the wire contract and must never decrease), these three
// values carry no serialization role: they bound how long the device's
// `run_tcp_inbound_frames` handler waits before producing a `TestReport`. They live
// here so the device firmware *and* the host-side budget-invariant test reference a
// single source (mirroring the `REGISTERED_TESTS` single-source-of-truth pattern).
//
// They are plain integer seconds / counts, **not** `core::time::Duration` — this crate
// is `#![cfg_attr(not(test), no_std)]` and references no `Duration`/`std::time`. The
// device wraps them in `Duration::from_secs(...)` at the call sites; the host invariant
// test does plain integer arithmetic against `test_timeout(TcpInboundFrames).as_secs()`.
//
// **Invariant the host test asserts** (a conservative upper bound on the true device
// worst case, which is `max(connect, retries × read)` since connect and drain are on
// mutually exclusive paths):
//
//   MAX_IDLE_RETRIES × INBOUND_READ_TIMEOUT_SECS + INBOUND_CONNECT_TIMEOUT_SECS
//       < host test_timeout(TcpInboundFrames) − serial-round-trip margin
//
// The device's post-connect worst case (idle fail-fast) must land comfortably below the
// host's RunTest budget so the host always sees a typed `TestReport` rather than a silent
// "device not responding" hang (see design-hil-tcpinbound-fix.md §2.1).

/// Per-read blocking timeout (seconds) for the `TcpInboundFrames` drain loop.
/// Tuning constant, no wire/serde role. See the section comment above.
pub const INBOUND_READ_TIMEOUT_SECS: u64 = 2;

/// Connect timeout (seconds) for the `TcpInboundFrames` dedicated TCP connection.
/// Pulled under the host RunTest budget so even a reachable-but-slow connect surfaces as
/// a typed Fail rather than a host-side "not responding."
/// Tuning constant, no wire/serde role. See the section comment above.
pub const INBOUND_CONNECT_TIMEOUT_SECS: u64 = 5;

/// Maximum consecutive idle (read-timeout) returns before `run_tcp_inbound_frames`
/// fails fast with "server stalled." `MAX_IDLE_RETRIES × INBOUND_READ_TIMEOUT_SECS` is
/// the device's post-connect idle-fail-fast budget (3 × 2 s = 6 s).
/// Tuning constant, no wire/serde role. See the section comment above.
pub const MAX_IDLE_RETRIES: u32 = 3;

/// Device-side wall-clock cap (seconds, connect excluded) for the whole
/// `run_tcp_inbound_backpressure` flood drain. Sized ≥ 2× the expected drain-bound
/// duration (inbound-backpressure-hil design §2.4): the flood is drain-bound at ~6 s,
/// so 20 s leaves ample margin while still failing typed well inside the host's
/// `TcpInboundBackpressure` RunTest budget. Tuning constant, no wire/serde role. See
/// the section comment above.
pub const INBOUND_BP_DEADLINE_SECS: u64 = 20;

// ── Top-level frame types ─────────────────────────────────────────────────────

/// Frame the device emits to the host.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum DeviceFrame {
    /// Correlated to a request id.
    Response(Response),
    /// Unsolicited log record; no request id.
    Log(LogFrame),
    /// Optional unsolicited liveness frame. Harness tolerates but does not require it.
    Heartbeat,
}

/// Frame the host sends to the device.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub id: u32,
    pub command: Command,
}

// ── Commands ──────────────────────────────────────────────────────────────────

/// Commands the host may send.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum Command {
    /// Run a registered self-test by name (discriminant 0 — unchanged).
    RunTest(TestName),
    /// Provision WiFi credentials into device NVS (discriminant 1).
    ///
    /// The device persists SSID and passphrase to the `"wifi"` NVS namespace.
    /// Idempotent: re-provisioning overwrites prior values. Response is
    /// `Status::Ok + Payload::Empty` on success or `Status::Fail + Payload::TestReport`
    /// with a distinct error detail on NVS write failure.
    ProvisionWifi {
        ssid: heapless::String<32>,
        passphrase: heapless::String<64>,
    },
    /// Provision the HIL-host echo-server peer address and ports into device NVS
    /// (discriminant 2). The device reads these at test time to locate the UDP/TCP
    /// echo servers and the TLS target.
    ///
    /// - `host`: HIL-host LAN IP for UDP/TCP echo (4-byte big-endian octets).
    /// - `udp_port`: UDP echo port.
    /// - `tcp_port`: TCP echo port.
    /// - `tls_host`: Public-IP TLS endpoint (4-byte big-endian octets, NOT the HIL host).
    /// - `tls_port`: TLS endpoint port.
    /// - `inbound_frames_port`: TCP port of the audio-frame source server for the
    ///   `TcpInboundFrames` self-test. The host IP is the same as `host`. Stored in
    ///   NVS as `peer_inb_tcp` (u16).
    /// - `backpressure_port`: TCP port of the backpressure source server for the
    ///   `TcpSendBackpressure` self-test (the server withholds reads so the device
    ///   send buffer fills). The host IP is the same as `host`. Stored in NVS as
    ///   `peer_bp_tcp` (u16).
    /// - `poll_readiness_port`: TCP port of the poll-readiness adversary server for the
    ///   `PollReadinessBidir` self-test (the server queues inbound bytes so the device can
    ///   prove `poll(POLLIN)` readiness on real lwIP). The host IP is the same as `host`.
    ///   Stored in NVS as `peer_poll_tcp` (u16).
    /// - `rtd_port`: TCP port of the `StreamRealtimeDuplex` listener server for the
    ///   `StreamRealtimeDuplex` self-test (the device connects and streams a synthetic
    ///   segment so the host can time the drain). The host IP is the same as `host`.
    ///   Stored in NVS as `peer_rtd_tcp` (u16).
    /// - `tls_psk_port`: TCP port of the TLS-PSK listener holding this pod's real
    ///   audio-link key, for the `TlsPskHandshake` self-test. The host IP is the same as
    ///   `host`. Stored in NVS as `peer_psk_tcp` (u16).
    /// - `tls_psk_bad_port`: TCP port of the TLS-PSK listener holding a *different* key
    ///   for the same identity, for the `TlsPskWrongKeyRejected` self-test. Stored in NVS
    ///   as `peer_pskbad_tcp` (u16).
    ProvisionPeer {
        host: [u8; 4],
        udp_port: u16,
        tcp_port: u16,
        tls_host: [u8; 4],
        tls_port: u16,
        inbound_frames_port: u16,
        backpressure_port: u16,
        poll_readiness_port: u16,
        rtd_port: u16,
        tls_psk_port: u16,
        tls_psk_bad_port: u16,
    },
    /// Provision the audio receiver address and port into device NVS (discriminant 3).
    ///
    /// Written to the `"wifi"` NVS namespace as `audio_ip` (4 bytes) and `audio_port`
    /// (u16). Separate from `ProvisionPeer` so the echo self-tests (`TcpRoundtrip`,
    /// `UdpRoundtrip`) are not disturbed. If unprovisioned, the audio streamer thread
    /// logs once and parks; all other device behavior is unaffected.
    ///
    /// - `host`: Audio receiver LAN IP (4-byte big-endian octets).
    /// - `port`: Audio receiver TCP port (default 7380).
    ProvisionAudio { host: [u8; 4], port: u16 },
    /// Provision the VAD gate threshold into device NVS (discriminant 4).
    ///
    /// Written to the `"audio"` NVS namespace as a 4-byte little-endian `f32` blob
    /// under the key `"vad_threshold"`. Applied on next boot (no live update).
    /// Mirrors `ProvisionAudio` in structure. The device validates the stored value at
    /// boot (finite, >= 0.0) and falls back to the compile-time default if invalid.
    SetVadThreshold { threshold: f32 },
    /// Provision the device VAD hangover into device NVS (discriminant 5).
    ///
    /// Written to the `"audio"` NVS namespace as a 4-byte little-endian `u32` blob
    /// under the key `"vad_hangover_ms"`. Applied on next boot (no live update),
    /// mirroring `SetVadThreshold`. The device converts the stored milliseconds to
    /// poll ticks at boot and falls back to the compile-time default if the blob is
    /// absent or malformed.
    ///
    /// - `hangover_ms`: how long the device VAD gate stays open after the signal
    ///   drops below threshold, in milliseconds.
    SetVadHangover { hangover_ms: u32 },
    /// Erase WiFi credentials from device NVS (discriminant 6).
    ///
    /// Removes exactly the `"ssid"` and `"pass"` keys from the `"wifi"` NVS namespace,
    /// force-disconnects the current association, and wakes the WiFi supervisor so it
    /// observes the credential-less state and parks. Idempotent: absent keys are not an
    /// error. Other `"wifi"`-namespace keys (peer/audio provisioning) are untouched.
    /// Response is `Status::Ok` + `Payload::Empty` on success, or `Status::Fail` +
    /// `Payload::TestReport` with a distinct error detail on NVS failure.
    ///
    /// Also clears any active `SetTemporaryWifiConfig` override: the credential-less
    /// park guarantee must hold regardless of an override, so a temporary config
    /// cannot survive a credentials clear.
    ClearWifiCredentials,
    /// Apply a RAM-only WiFi config override, bypassing NVS (discriminant 7).
    ///
    /// Stores `ssid`/`passphrase` in the device's temporary-config slot — never written
    /// to flash — then force-disconnects and wakes the WiFi supervisor so the override
    /// takes effect immediately without a reboot. A power cycle always reverts to the
    /// persisted (NVS) config: the override cannot outlive RAM. Overwriting an existing
    /// override is allowed (last write wins). Empty `ssid` is rejected with
    /// `Status::Fail` (an empty ssid is the "no credentials" sentinel elsewhere;
    /// accepting it here would create an unreachable half-state).
    ///
    /// Precedence: while an override is active it wins over NVS credentials
    /// unconditionally, including over a `ProvisionWifi` that lands while it is set —
    /// "temporary" is an explicit operator action whose whole point is to shadow the
    /// persisted config until cleared.
    ///
    /// Response is `Status::Ok + Payload::Empty` on success, or `Status::Fail +
    /// Payload::TestReport` on empty ssid or NVS-unrelated failure. On the
    /// disconnect-unconfirmed `Status::Fail` (the old link's teardown could not be
    /// verified), the override is stored regardless and will be applied by a later
    /// probe or retry — a client that gets this Fail and wants no override active must
    /// still send `ClearTemporaryWifiConfig` to back it out.
    SetTemporaryWifiConfig {
        ssid: heapless::String<32>,
        passphrase: heapless::String<64>,
    },
    /// Clear the RAM-only WiFi config override, if any (discriminant 8).
    ///
    /// If an override was present: force-disconnects and wakes the supervisor so it
    /// reverts to NVS credentials (or parks if NVS holds none). If no override was
    /// present: a pure no-op returning `Status::Ok` — a healthy link on persisted
    /// credentials must not be bounced by a redundant clear.
    ///
    /// If an override was present but the previous link's disconnect/stop cannot be
    /// confirmed, returns `Status::Fail`: the override is cleared regardless (a later
    /// probe or retry will re-evaluate against NVS credentials), but the caller must
    /// not assume the trial link has actually been torn down yet.
    ClearTemporaryWifiConfig,
    /// Provision the audio-link pre-shared key into device NVS (discriminant 9).
    ///
    /// Takes effect on the streamer's next connect — no reboot needed. Idempotent:
    /// re-provisioning overwrites the prior key.
    ///
    /// The success response carries [`Payload::PodId`] — the device's MAC-derived pod
    /// identity — and deliberately never echoes the key. The identity is what the
    /// device presents as the TLS PSK identity, so the provisioning host needs it to
    /// write its own `pod_id → key` table entry, and it is authoritative only here.
    ProvisionAudioPsk { key: [u8; 32] },
}

/// Registered self-test names. Using an enum (not a free string) makes registry
/// set-equality and behavioral dispatch compiler-checked across both sides.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
pub enum TestName {
    Ping,
    Identify,
    /// GPIO controllability self-test: drives GPIO21 to both levels and reads back the output
    /// latch register, asserting readback matches the command. Proves the controllable+observable
    /// GPIO path; does NOT prove actual photon emission (no photosensor present).
    GpioSelfTest,
    /// Device health / resource-sanity self-test: reads ESP-IDF runtime health metrics
    /// (free heap, minimum-ever free heap, current-task stack high-water mark) and asserts
    /// each is above a conservative floor. Catches wrong-image flashes, memory regressions,
    /// and stack exhaustion early. Returns actual values in the message for human visibility.
    DeviceHealthCheck,
    /// I2C bus scan: initialises an I2C master and probes the 7-bit address space (0x08–0x77)
    /// for ACKs via zero-length writes. Reports the list of responding addresses in the result
    /// message. PASS criterion, pin assignment, and target addresses are device-defined.
    /// This is an assertion-as-probe: a failure is a hardware discovery, not a firmware bug.
    I2cBusScan,
    /// XVF3800 control register read: performs a write-then-read control transaction over I2C
    /// to read the DFU VERSION register (resid=240, cmd=88). Reports raw status byte and version
    /// payload bytes in the result message. PASS criterion: status=0x00 (done) and a plausible
    /// payload (not all-0x00, not all-0xFF, no NACK). This is an assertion-as-probe test:
    /// the exact framing is confirmed for formatBCE's firmware; for stock l16k2ch the transaction
    /// may differ — a FAIL is a discovery.
    Xvf3800RegRead,
    /// XVF3800 DoA plausibility self-test: reads AEC_AZIMUTH_VALUES (resid=33, cmd=75,
    /// 4×f32 LE, total 17 bytes = 1 status + 16 payload). Reports all four raw float values
    /// in the result message.
    ///
    /// PASS criterion:
    /// - Transaction succeeds (status=0x00, full 17 bytes received).
    /// - Every NON-NaN value is finite (not Inf) and within |x| ≤ π radians.
    /// - Index 2 (free-running scanner) MUST be finite-and-in-range (not NaN).
    ///
    /// NaN on indices 0 (focused tracker A), 1 (focused tracker B), or 3 (auto-select winner)
    /// is ACCEPTABLE — these trackers may legitimately read NaN when no beam is focused/selected
    /// (e.g. quiet room). NaN on index 2 is a FAIL.
    ///
    /// Azimuth convention: [-π, π] radians. For a linear array the practical range is [0, π]
    /// (broadside half-plane) but the sane bound is |x| ≤ π to accommodate any chip orientation.
    /// Source: usb-spike-verdict.md §7, xvf3800-control-protocol.md §5.
    Xvf3800DoAPlausibility,
    /// I2S audio-path waveform sanity test (assertion-as-probe — Cycle 6).
    ///
    /// Configures ESP32-S3 I2S0 as master RX from XVF3800:
    ///   BCLK=GPIO8, WS=GPIO7, DIN=GPIO43, 16 kHz, 16-bit, stereo (Philips).
    ///
    /// Captures ~250 ms / 4000 frames (16000 bytes). Computes per-channel stats:
    ///   min, max, RMS, saturation %.
    ///
    /// PASS criterion ("looks like a live audio waveform"):
    /// - Not all-zero: at least one channel has max-abs > ZERO_ABS_THRESHOLD (100).
    /// - Not stuck constant: at least one channel has spread (max−min) > STUCK_SPREAD_MULTIPLIER × ZERO_ABS_THRESHOLD (4 × 100 = 400).
    /// - Not railed/saturated: neither channel is at ±32700 for >95% of frames.
    /// - Real variance: at least one channel RMS > RMS_FLOOR (50).
    ///
    /// At least one of the two channels must pass all four criteria.
    /// A FAIL result (all-zero, stuck, saturated, or init error) is an expected possible
    /// outcome (master/slave mismatch, MCLK required) and recorded as a DISCOVERY.
    ///
    /// Reports: "PASS role=R ch0 min=N max=N rms=N sat=N% ch1 min=N max=N rms=N sat=N% frames=N"
    /// or "FAIL role=R reason=R ch0 min=N max=N rms=N sat=N% ch1 ... frames=N".
    I2sWaveformSanity,
    /// WiFi association + DHCP lease self-test (discriminant 8).
    ///
    /// Reads SSID/passphrase from NVS (`"wifi"` namespace), associates to a WPA2
    /// network, waits for netif up and DHCP lease. Returns `TestData::WifiAssociate` with
    /// IP, gateway, and RSSI on success.
    ///
    /// PASS criterion: IP non-zero/non-loopback, gateway non-zero, RSSI > −80 dBm
    /// and RSSI != 0. Missing NVS credentials produce a distinct Fail message.
    WifiAssociate,
    /// UDP round-trip self-test (discriminant 9).
    ///
    /// Reads peer IP and UDP port from NVS (`"wifi"` namespace), sends a fixed nonce,
    /// and asserts the echoed reply byte-matches (requires `ProvisionPeer` first).
    UdpRoundtrip,
    /// TCP round-trip self-test (discriminant 10).
    ///
    /// Reads peer IP and TCP port from NVS, connects, writes a nonce, reads it back,
    /// asserts byte-match (primary). Fallback: reads SSH banner prefix from pve00:22.
    TcpRoundtrip,
    /// TLS reachability proof self-test (discriminant 11).
    ///
    /// Reads TLS host IP and port from NVS, connects via `EspTls` with
    /// `use_crt_bundle_attach: true` (verifies against ESP-IDF bundled public CA store;
    /// `skip_common_name: true` since target is reached by literal IP).
    /// Asserts handshake completes (AC-B5.1/B5.2).
    TlsReachability,
    /// WiFi credential-less radio + AP scan self-test (discriminant 12).
    ///
    /// Starts the WiFi radio (`wifi.start()`) without any credentials or association,
    /// performs a passive AP scan, and asserts at least one AP is found.
    ///
    /// PASS criterion: scan returns ≥1 AP. Result string carries `aps=<n>` (authoritative),
    /// `best_rssi=<dBm>`, and up to a few truncated SSIDs for diagnostics.
    /// A zero-AP result (radio up, scan empty) is a distinct `Status::Fail` — a hardware/RF
    /// discovery, not an auto-pass. Requires no NVS credentials; runs on a factory-fresh
    /// device.
    WifiScan,
    /// XVF3800 SPENERGY plausibility self-test — assertion-as-probe (discriminant 13).
    ///
    /// Reads `AEC_SPENERGY_VALUES` (resid=33, cmd=80, 4×f32 LE, 17 bytes) via
    /// `xvf3800_control_read`. Mirrors `Xvf3800DoAPlausibility` in structure.
    ///
    /// PASS criterion:
    /// - Transaction succeeds (status=0x00, full 17 bytes received).
    /// - Every value is finite and ≥ 0.0 (NaN, Inf, or negative → FAIL).
    /// - Not all four values are exactly 0.0 in a non-silent room (all-zero → suspicious).
    ///
    /// An unexpected reading (NaN, negative, implausible magnitudes) is a discovery
    /// requiring human review before pinning as accepted truth (bring-up guardrail).
    /// Source: design §2.5.
    Xvf3800SpEnergy,
    /// WiFi re-association after forced drop (discriminant 14).
    ///
    /// Asserts the WiFi supervisor autonomously re-associates after a controlled
    /// disconnect. The test:
    ///   1. Verifies WiFi is already associated (fails immediately if not).
    ///   2. Forces a `disconnect()` on the driver to trigger a `StaDisconnected`
    ///      event and wake the supervisor.
    ///   3. Waits up to 90 s for `is_up()` to return `Some(true)` again (polling
    ///      every 500 ms).
    ///   4. On success, reads IP/gw/RSSI and returns `TestData::WifiReassociation`
    ///      with `reconnected = true` plus the IP/gateway/RSSI.
    ///   5. On timeout, returns `Status::Fail` with `"WifiReassociation: FAIL
    ///      wifi-up=false (timeout 90s)"`.
    ///
    /// The [`log_tokens::WIFI_DISCONNECTED`], [`log_tokens::WIFI_CONNECTED`],
    /// [`log_tokens::WIFI_DHCP_LEASE`], and [`log_tokens::WIFI_REASSOCIATED`] log lines are emitted as `DeviceFrame::Log`
    /// frames during the test, observable on the host. The host-side eval asserts
    /// both the typed report and those log lines.
    WifiReassociation,
    /// Gateway-reachability probe gate self-test (discriminant 15).
    ///
    /// Validates the gateway-probe decision logic in both directions by calling
    /// `ping_reachable` (the same inner probe used by the production supervisor)
    /// against a provisioned target IP (`peer_ip` NVS blob written by `ProvisionPeer`),
    /// rather than the live default gateway:
    ///
    ///   - **Reachable half:** device calls `ping_reachable(peer_ip, index)` where
    ///     `peer_ip` is the host's own (reachable) IP. Returns
    ///     `"PASS probe=reachable reassociated=false"` and asserts the supervisor does
    ///     **not** re-associate. This is the core peer-down-must-not-bounce assertion.
    ///   - **Unreachable half:** device calls `ping_reachable(blackhole_ip, index)` where
    ///     `blackhole_ip` is an unused-but-routable subnet IP (no host answers, so all
    ///     echoes time out). Returns `"PASS probe=unreachable reassociated=true ip=… gw=…"`.
    ///
    /// The host provisions the blackhole address as a second `ProvisionPeer` call before
    /// invoking the unreachable half. The two halves run as sequential sub-steps within
    /// a single `TestName::GatewayProbeGate` dispatch.
    ///
    /// Design: docs/adr/2026/06/16-wifi-gateway-reachability-gate/design.md §4.
    GatewayProbeGate,
    /// TCP inbound-frames self-test (discriminant 16).
    ///
    /// Opens a dedicated TCP connection to the HIL-host audio-frame source server
    /// (provisioned via `ProvisionPeer` → `peer_ip` + `peer_inbound_tcp` NVS keys).
    /// The server sends a fixed number of `StreamFrame::Audio` frames using the shared
    /// `encode_frame` codec. The device reads them via `drain_inbound` (the same
    /// reassembly/decode path used by the streamer's inbound drain), asserts it received
    /// exactly the expected count, and returns `TestData::TcpInboundFrames`.
    ///
    /// This test exercises the inbound framing/decode path on a dedicated connection,
    /// independent of the VAD-driven streamer. The concurrent capture-write + inbound-drain
    /// interleaving on the streamer's live socket is not covered here.
    ///
    /// Design: docs/adr/2026/06/17-audio-output-fullduplex/design.md §6.
    TcpInboundFrames,
    /// TCP send-backpressure self-test (discriminant 17).
    ///
    /// Opens a dedicated TCP connection to the HIL-host *backpressure* source server
    /// (provisioned via `ProvisionPeer` → `peer_ip` + `peer_bp_tcp` NVS keys), puts
    /// the socket into non-blocking mode (mirroring the streamer), and sends
    /// `StreamFrame::Audio` frames through the production `send_frame_bp` path. The
    /// server deliberately withholds reads after accepting, so the device send buffer
    /// fills and `send_frame_bp` is driven into its `poll(POLLOUT)` writability wait.
    ///
    /// This is the failing-assert-first bring-up proof of the `poll(POLLOUT)` path
    /// (design §3.1/§6) — the one primitive unproven on this lwIP/VFS firmware. The
    /// device asserts: a buffer-full send returns a bounded, clean outcome
    /// (`Sent`/`BackpressureAligned`, **never** a fatal `Err`); the bounded wait
    /// actually *waited* (elapsed on the order of the `WRITE_TIMEOUT_MS` budget when
    /// reads are withheld, not a near-instant spin); and the connection is still
    /// usable once the server resumes reading. Returns `TestData::TcpSendBackpressure`;
    /// the host-side eval enforces the same.
    ///
    /// Per CLAUDE.md bring-up doctrine the first observed timing/outcome gets human
    /// review before any assert is pinned green; a confirmed value is then baked in as
    /// a regression guard. Concurrent backpressure *during* live VAD-driven capture on
    /// the streamer's own socket is not covered here.
    ///
    /// Design: docs/adr/2026/06/17-audio-output-fullduplex/design-streamer-backpressure.md §6.
    TcpSendBackpressure,
    /// Speaker output (playback) bring-up self-test (discriminant 18).
    ///
    /// Synthesizes a 440 Hz sine tone on-device and pushes it through the full TX chain:
    /// I2S0 TX (GPIO44 DOUT) → XVF3800 → AIC3104 codec → TPA3139D2 amplifier → speaker.
    /// The device initializes the AIC3104 (the host owns codec config; the XVF3800 does not),
    /// pre-rolls I2S silence, soft-unmutes the DAC against that silence, plays the tone,
    /// soft-mutes the DAC, post-rolls silence, and stops TX. (There is no amp toggle: the amp
    /// is always-on hardware and the cmd-0 GPO write is read-only — AmpAlwaysOnGpoInert
    /// self-test; the DAC soft-mute is the click-safe lever.)
    ///
    /// PASS asserts the **programmatic** contract only: the AIC3104 init sequence wrote without
    /// I2C fault and every persistent config register read back at its written value
    /// (`codec=ok`), and the I2S TX emitted the tone without
    /// error. The *acoustic* result (audible 440 Hz at correct pitch) has no programmatic
    /// observable — there is no loopback — and is confirmed by the human running the test.
    ///
    /// Result string: `"PASS src=speaker freq=440 amp=50 dur_ms=1500 codec=ok"` or
    /// `"FAIL src=speaker reason=<codec-init|amp-enable|i2s-write> …"`.
    ///
    /// Design: docs/adr/2026/06/20-audio-output-speaker-bringup/design.md §2.7.
    SpeakerOutput,
    /// Amp always-on / GPO-inert regression guard (discriminant 19).
    ///
    /// Does **not** toggle the amp (impossible on this board — the TPA3139D2 is always-on
    /// hardware and not software-gateable). It asserts the *observable* inert reality so the
    /// always-on fact is a durable regression guard: reads the GPO vector (resid 20, cmd 0),
    /// writes it back with X0D31 (vector index 2) flipped via the read-only cmd 0, then
    /// re-reads and asserts the device accepted-and-reported-DONE **while X0D31 did not move**
    /// — proof the write is inert. PASS asserts `write_status == DONE` and the X0D31 readback
    /// is unchanged.
    ///
    /// Per CLAUDE.md bring-up doctrine this encodes the proven (expected) inert behavior, so
    /// it goes green immediately and stays as a guard. If a future firmware/hardware change
    /// ever makes the write actually move X0D31, this test FAILs — the desired alarm that the
    /// always-on premise (and the clean-shutdown design that rests on it) no longer holds.
    ///
    /// Result string: `"PASS src=amp gpo_write=inert x0d31=0x.. write_status=0x00"`.
    ///
    /// Design: docs/adr/2026/06/21-audio-output-clean-shutdown/design-realfix.md §2.4.
    AmpAlwaysOnGpoInert,
    /// Capture-thread periodic-summary-line observability regression guard (discriminant 20).
    ///
    /// Drives inbound audio through the **production** playback path (the device builds an
    /// `I2sStreamSink` wired to the same inbound PCM ring the live streamer uses, and
    /// feeds it valid S16_LE-mono PCM chunks for a few seconds) so the production capture
    /// thread drains them and emits its periodic [`log_tokens::CAPTURE_TX_LINE`] summary
    /// `log::info!` heartbeat (audio-pipeline-observability design §2.2 / §5). The handler
    /// holds the request open across at least two ~1 s emit windows, then returns
    /// `"PASS src=capture chunks_fed=<n>"`.
    ///
    /// The periodic line travels to the host as `DeviceFrame::Log` frames; the host-side eval
    /// collects them over the test window and asserts at least two [`log_tokens::CAPTURE_TX_LINE`]
    /// lines appeared (the cadence), following the `WifiReassociation` log-line-assertion
    /// pattern. Only the periodic-line *presence at cadence* is asserted here — the
    /// induced-anomaly warns (drop-burst / underrun-proxy) are out of scope
    /// (audio-pipeline-observability design §5 / §6 resolved-decision 1).
    ///
    /// Design: docs/adr/2026/06/22-audio-pipeline-observability/design.md §5.
    CapturePeriodicLine,
    /// Playback drain-rate discrimination self-test (discriminant 21).
    ///
    /// A numeric-bound timing/identity guard (kept SEPARATE from the `CapturePeriodicLine`
    /// presence/cadence guard, per CLAUDE.md "do not fold identity into presence"). It drives a
    /// **steady, at-least-real-time** inbound playback feed through the production path (the
    /// device builds an `I2sStreamSink` wired to the same inbound PCM ring the live
    /// streamer uses, and `accept`s valid S16_LE-mono PCM chunks in a tight retry loop, yielding
    /// only when the ring reports `Full`) so the production capture thread is held
    /// drain-bound and emits its periodic [`log_tokens::CAPTURE_TX_LINE`] summary lines. The handler
    /// holds the request open across several ~1 s emit windows, counts its own `Accepted::Full`
    /// returns, and returns `"PASS src=playback-drain chunks_fed=<n> feed_full=<n> feed_ms=<n>"`.
    ///
    /// The handler does NOT gate PASS/FAIL on the hardware-timing values — it reports them and
    /// the production periodic lines travel to the host as `DeviceFrame::Log` frames. The
    /// host-side eval owns the numeric bounds: it filters to SATURATED windows (`max_backlog`
    /// reached the channel depth, `feed_full` climbing), takes the per-chunk drain as the
    /// `write_us max` over those windows (corroborated by the within-window mean converging to
    /// it), and asserts the healthy 16 kHz behavior — `write_us ∈ [18_000, 22_000] µs` (320
    /// frames at 16 kHz = 20 ms) and `chunks ≥ 48`/window. These bounds discriminate the
    /// hardware I2S/DAC clock/slot mismatch signature (a) from per-chunk software/scheduling
    /// overhead (b).
    ///
    /// Per CLAUDE.md HIL doctrine this asserts the expected-correct behavior and is **expected
    /// to FAIL on current hardware** (observed `write_us max ≈ 30 ms` ⇒ effective ~10.6 kHz;
    /// `chunks ≈ 26–33`). That failure output — the true drain rate / `write_us` distribution /
    /// chunks-per-sec / backlog — is the discovery. The bad reading is NOT laundered into a
    /// regression floor; the healthy value is pinned and a fix must land before any floor.
    ///
    /// Design: docs/adr/2026/06/23-audio-consumer-throughput-stutter/design.md §2.
    PlaybackDrainRate,
    /// Bidirectional `poll()` readiness bring-up self-test (discriminant 22).
    ///
    /// The gating failing-assert-first proof that `poll(fd, POLLIN|POLLOUT, timeout)`
    /// reports per-direction readiness correctly on *this* lwIP/VFS firmware — the single
    /// platform fact the entire audio I/O event-loop architecture rests on
    /// (event-loop design §4 test #1, §5 risk #1). `set_nonblocking`/`poll(POLLOUT)` are
    /// already exercised by `TcpSendBackpressure`; `poll(POLLIN)` has **never** been
    /// exercised in any production path and is unproven. This test exercises it.
    ///
    /// Opens a dedicated TCP connection to the HIL-host *poll-readiness* adversary server
    /// (provisioned via `ProvisionPeer` → `peer_ip` + `peer_poll_tcp` NVS keys), flips the
    /// socket non-blocking (mirroring the streamer), and asserts on real lwIP:
    ///   - With the host having queued inbound bytes, `poll` reports **POLLIN** (and the
    ///     bytes then `read()` non-blocking) — the never-before-exercised readiness path.
    ///   - With the TX buffer holding room, `poll` reports **POLLOUT**.
    ///   - A `poll` over `POLLIN|POLLOUT` returns both direction bits together when both
    ///     conditions hold, so the event loop can multiplex one fd in one syscall.
    ///
    /// Written per CLAUDE.md §"Hardware Bring-Up" to ASSERT the expected behavior and be
    /// allowed to FAIL first: the failure output is the proof that `POLLIN` readiness works
    /// on this firmware (or the discovery that it does not, which kills the event-loop
    /// design before any production cutover). Returns `TestData::PollReadiness`; the
    /// host-side eval enforces the same. Requires prior `WifiAssociate` and `ProvisionPeer`.
    ///
    /// Design: docs/adr/2026/06/24-audio-io-architecture/design-event-loop.md §4 test #1.
    PollReadinessBidir,
    /// Full-duplex mic-RX integrity under playback self-test (discriminant 23).
    ///
    /// The direct regression guard that splitting TX from RX servicing (NON_BLOCK TX +
    /// core-1 pinning) eliminated the mic-capture starvation the root-cause analysis found:
    /// the pre-fix strict TX-then-RX pass with blocking TX writes capped mic capture at
    /// ~8 320 of 16 000 frames/s during playback (~48 % of samples silently dropped). It
    /// drives the SAME steady, at-least-real-time inbound playback feed as `PlaybackDrainRate`
    /// through the production path (an `I2sStreamSink` cloned onto the live inbound PCM ring,
    /// `accept`ing valid S16_LE-mono PCM in a tight retry loop) so the capture thread is held
    /// TX-drain-bound and must service mic RX concurrently under that load. The handler holds
    /// the request open across several ~1 s emit windows and returns
    /// `"PASS src=full-duplex-rx chunks_fed=<n> feed_full=<n> feed_ms=<n>"`.
    ///
    /// The handler does NOT gate PASS/FAIL on the RX figures — it reports the feed and the
    /// production capture thread emits its per-window `rx_deficit=` telemetry on the periodic
    /// [`log_tokens::CAPTURE_OBS_LINE`] line (`DeviceFrame::Log` frames). The host-side eval owns the
    /// assertion: over the saturated (`feed_full>0`) windows the device-computed, dead-banded
    /// `rx_deficit` must be 0 — mic RX kept its 16 kHz cadence under full playback load. Per
    /// CLAUDE.md HIL doctrine this ASSERTS the expected behavior (deficit zero), does not merely
    /// count the loss, and a nonzero reading is NOT laundered into a pass.
    ///
    /// Design: docs/adr/2026/07/01-host-to-device-dropout/design.md §5 (`FullDuplexRxIntegrity`).
    FullDuplexRxIntegrity,
    /// Streamer real-time duplex drain self-test (discriminant 24).
    ///
    /// The throughput/keep-up regression guard for the outbound streamer path (and, once
    /// Scenario B lands, the socket→sink playback path). The device drives the extracted
    /// `run_segment` drain loop against a test-owned capture ring and a synthetic producer
    /// thread, connecting to the HIL-host `StreamRealtimeDuplex` listener (provisioned via
    /// `ProvisionPeer` → `peer_ip` + `peer_rtd_tcp` NVS keys). The host times how fast the
    /// pre-roll burst drains and asserts the streamer keeps up with real time.
    ///
    /// Written per CLAUDE.md §"Hardware Bring-Up" to ASSERT the expected behavior and be
    /// allowed to FAIL first against the current one-action-per-wake loop: the recorded
    /// burst wall time is the discovery. Returns `TestData::Rtd`;
    /// the host-side eval owns the burst-drain and catch-up bounds. Requires prior
    /// `WifiAssociate` and `ProvisionPeer`.
    StreamRealtimeDuplex,
    /// PSRAM presence + identity self-test (discriminant 25).
    ///
    /// Asserts the on-module octal PSRAM initialized and reads its size, per the
    /// CLAUDE.md bring-up doctrine: presence (`esp_psram_is_initialized()`) and identity
    /// (`esp_psram_get_size() == 8 MiB`, the vendor-documented XIAO ESP32-S3 R8 size) are
    /// baked into a permanent registry test rather than probed and discarded. The free
    /// SPIRAM byte count is reported for observability but not asserted. Presence and
    /// identity are asserted in the same handler but reported distinctly so a failure
    /// classifies itself.
    PsramIdentity,
    /// WiFi modem power-save identity self-test (discriminant 26).
    ///
    /// Reads `esp_wifi_get_ps` back after `ensure_wifi_started` (the start path that
    /// forces `WIFI_PS_NONE`) and asserts the radio is in `WIFI_PS_NONE`. Modem power
    /// save was the root-cause mechanism of a host→device playback-dropout regime;
    /// this is the durable regression guard that PS stays off. Reports the raw
    /// `wifi_ps_type_t` value (0 = `WIFI_PS_NONE`, 1 = `MIN_MODEM`, 2 = `MAX_MODEM`),
    /// not a bool, so an unexpected reading is visible verbatim per bring-up doctrine.
    /// Requires no NVS credentials — a started radio suffices, like `WifiScan`.
    WifiPowerSaveCheck,
    /// TCP inbound-backpressure self-test (discriminant 27).
    ///
    /// Connects to the HIL-host inbound-frames source (provisioned via `ProvisionPeer` →
    /// `peer_ip` + `peer_inb_tcp` NVS keys), selects the flood profile with an in-band
    /// selector byte (`b'F'`), and drains an unpaced over-capacity Audio flood through the
    /// **production** socket → `drain_inbound` → ring path (a `StallCountingSink` wrapping
    /// `build_inbound_stream_sink()`'s `I2sStreamSink`) while the real capture thread drains
    /// at real time. Asserts `full_stalls > 0` (the socket-level accumulator-full read-skip
    /// and the ring-`Full`/held-frame-retry livelock guard actually engaged on real lwIP),
    /// an exact frame count (nothing dropped for fullness), and a clean EOF (the connection
    /// stayed up under sustained backpressure). This is the one integration property no
    /// existing test reaches on hardware: every in-repo producer that drives the real
    /// socket path paces at real time and never fills the ring off-socket.
    ///
    /// Returns `TestData::TcpInboundBackpressure`; the host-side eval enforces the same.
    /// Requires prior `WifiAssociate` and `ProvisionPeer`.
    TcpInboundBackpressure,
    /// TLS-PSK handshake proof over the production audio-link client (discriminant 28).
    ///
    /// Reads the audio-link key (`audio_psk`, written by [`Command::ProvisionAudioPsk`])
    /// and the host's TLS-PSK listener port (`peer_psk_tcp`) from NVS, then connects with
    /// the same `tls_connect_psk` path the streamer uses. Asserts the handshake completes,
    /// reports the negotiated protocol version and ciphersuite for host-side assertion,
    /// and round-trips one echo payload through the tunnel.
    ///
    /// Returns `TestData::TlsPskHandshake`. An unexpected negotiated version or suite is a
    /// discovery requiring human review before the host-side expectation is adjusted
    /// (bring-up guardrail). Requires prior `WifiAssociate`, `ProvisionPeer` and
    /// `ProvisionAudioPsk`.
    TlsPskHandshake,
    /// TLS-PSK identity-negative self-test (discriminant 29).
    ///
    /// Connects to the listener that holds a *different* key for this pod's identity
    /// (`peer_pskbad_tcp`) and asserts the handshake fails — promptly, by a TLS alert
    /// rather than by the deadline expiring. No application byte can cross, because a
    /// failed handshake yields no stream to write to. Kept separate from
    /// [`TestName::TlsPskHandshake`] per the presence-vs-identity doctrine: one
    /// proves the link works, this one proves the key is what makes it work.
    ///
    /// Returns `TestData::TlsPskRejected`. Requires the same provisioning as
    /// [`TestName::TlsPskHandshake`].
    TlsPskWrongKeyRejected,
}

// ── Response ──────────────────────────────────────────────────────────────────

/// Response frame from the device.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Response {
    pub id: u32,
    pub status: Status,
    pub payload: Payload,
}

/// Outcome of a request.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
pub enum Status {
    Ok,
    Fail,
    /// Defensive: device does not implement the requested command. Unreachable on
    /// the happy path when check 3 (registry set-equality) passes (requirements §4.4).
    Unsupported,
}

/// Capacity in bytes of [`TestResultMsg`]. Sized to hold the widest failure detail
/// (worst-case field-by-field bound: 154 bytes) plus headroom. This const is the single
/// lever for future field additions: the alias derives from it, and the device-side
/// `format_truncating_marked` call sites take it as their const-generic width, so a bump
/// here propagates everywhere with no `192` literals to chase.
pub const TEST_RESULT_MSG_CAP: usize = 192;

/// Message string carried by [`TestReport`]'s `detail`. Every carrier that flows into
/// a report detail uses it so no widen-at-the-boundary truncation point exists.
pub type TestResultMsg = heapless::String<TEST_RESULT_MSG_CAP>;

/// Response payload variants.
///
/// `TestReport` is much larger than the other variants (a 192-byte detail string plus
/// typed data). The lint's remedy — boxing — is unavailable: this crate is `no_std` with
/// no allocator on the device, and every payload is built on the stack immediately before
/// being serialized, so the enum's in-memory size costs one short-lived frame and never a
/// heap round-trip.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum Payload {
    Pong(heapless::String<64>),
    Identify {
        build: BuildId,
        tests: HVec<TestName, MAX_TESTS>,
    },
    Empty,
    /// Typed self-test outcome (discriminant 3).
    TestReport(TestReport),
    /// The device's MAC-derived pod identity, e.g. `"pod-aabbcc"` (discriminant 4).
    ///
    /// Returned by [`Command::ProvisionAudioPsk`] so the provisioning host can key its
    /// own PSK table by the identity the device will present in the TLS handshake. The
    /// key itself is never echoed. Capacity must match the streamer's `Hello::pod_id`.
    PodId(heapless::String<32>),
}

/// Typed self-test outcome: machine-checkable data plus human-readable detail.
///
/// On [`Status::Fail`] `detail` carries the full failure narrative and `data` is
/// [`TestData::None`]. On [`Status::Ok`] `data` is authoritative and `detail` is
/// usually empty. `detail` is free text — the host never parses it.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct TestReport {
    pub detail: TestResultMsg,
    pub data: TestData,
}

/// Upper bound on addresses reported by an I2C scan: the 7-bit scan space
/// 0x08..=0x77 is 112 addresses, the worst case where every address ACKs.
pub const I2C_SCAN_MAX_ADDRS: usize = 112;

/// Device-side encode buffer size for an outbound response frame. Bounded by
/// `response_frame_fits_encode_buf`, which encodes the worst-case `TestReport`.
pub const RESPONSE_FRAME_BUF: usize = 512;

/// Byte cap applied to each SSID reported by `WifiScan`. The truncation rule is part
/// of the contract: the device truncates scanned SSIDs to this many bytes and the host
/// truncates its configured-SSID comparison prefix identically (via
/// [`truncate_utf8_prefix`]), so the two agree byte-for-byte.
pub const SSID_TRUNC_BYTES: usize = 16;

/// Largest prefix of `s` that fits in `max_bytes` while ending on a UTF-8 char
/// boundary. Shared by both sides of the `WifiScan` SSID contract so device-truncated
/// SSIDs and the host's configured-SSID comparison prefix are computed by one rule.
pub fn truncate_utf8_prefix(s: &str, max_bytes: usize) -> &str {
    let end = s
        .char_indices()
        .take_while(|&(idx, ch)| idx + ch.len_utf8() <= max_bytes)
        .last()
        .map(|(idx, ch)| idx + ch.len_utf8())
        .unwrap_or(0);
    &s[..end]
}

/// Where a PSRAM malloc probe allocation landed.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
pub enum MallocProbe {
    Null,
    Internal,
    External,
}

/// Per-test typed result data.
///
/// Field lists are the wire contract: the device constructs these, the host
/// destructures them with exhaustive patterns (no `..` rest pattern), so any field
/// add/remove/rename/retype is a compile error on both sides.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum TestData {
    /// Fail paths and tests with no machine-checked data. The diagnostic lives in
    /// [`TestReport::detail`].
    None,
    DeviceHealth {
        heap_free: u32,
        min_heap: u32,
        stack_hwm: u32,
        supervisor_hwm: u32,
        streamer_hwm: u32,
        writer_anomalies: u32,
        encode_failures: u32,
        tx_write_failures: u32,
    },
    I2cScan {
        found: HVec<u8, I2C_SCAN_MAX_ADDRS>,
        bus_errors: u32,
    },
    Xvf3800RegRead {
        status: u8,
        version: [u8; 3],
    },
    Xvf3800Doa {
        status: u8,
        az: [f32; 4],
    },
    Xvf3800SpEnergy {
        status: u8,
        sp: [f32; 4],
    },
    AmpGpoInert {
        x0d31: u8,
        write_status: u8,
    },
    I2sWaveform {
        min: i32,
        max: i32,
        rms: i32,
        sat_pct: u32,
        samples: u32,
        ac1: i32,
    },
    WifiScan {
        aps: u32,
        best_rssi: i32,
        ssids: HVec<heapless::String<SSID_TRUNC_BYTES>, 3>,
    },
    WifiAssociate {
        ip: [u8; 4],
        gateway: [u8; 4],
        rssi: i32,
    },
    UdpEcho {
        bytes: u32,
        peer_ip: [u8; 4],
        peer_port: u16,
    },
    TcpEcho {
        bytes: u32,
        peer_ip: [u8; 4],
        peer_port: u16,
    },
    TlsHandshake {
        peer_ip: [u8; 4],
        peer_port: u16,
    },
    TcpInboundFrames {
        inbound_frames: u32,
        peer_ip: [u8; 4],
        peer_port: u16,
    },
    TcpSendBackpressure {
        a_resumed: bool,
        a_rc: u32,
        a_ru: bool,
    },
    PollReadiness {
        pollin: bool,
        pollout: bool,
        both: bool,
        read_bytes: u32,
    },
    Rtd {
        underruns: u64,
        gap_ms: u64,
        consumed: u64,
    },
    WifiReassociation {
        reconnected: bool,
        ip: [u8; 4],
        gateway: [u8; 4],
        rssi: i32,
    },
    GatewayProbeGate {
        blackhole_reachable: bool,
        reassociated: bool,
        ip: [u8; 4],
        gateway: [u8; 4],
        rssi: i32,
    },
    SpeakerOutput {
        freq: u32,
        amp: u32,
        dur_ms: u32,
        codec_ok: bool,
    },
    CapturePeriodicLine {
        chunks_fed: u32,
    },
    PlaybackDrainRate {
        chunks_fed: u32,
        feed_full: u32,
        feed_ms: u32,
        tx_wf: u32,
    },
    FullDuplexRxIntegrity {
        chunks_fed: u32,
        feed_full: u32,
        feed_ms: u32,
    },
    PsramIdentity {
        init: bool,
        size: u32,
        spiram_free: u32,
        malloc_probe: MallocProbe,
    },
    WifiPowerSaveCheck {
        /// Raw `wifi_ps_type_t` read back via `esp_wifi_get_ps`. [`WIFI_PS_NONE_RAW`]
        /// (expected), 1 = `MIN_MODEM`, 2 = `MAX_MODEM`. Raw so a suspect value is
        /// itself diagnostic.
        ps_mode: u32,
    },
    TcpInboundBackpressure {
        /// Total Audio frames the device decoded and routed to the sink on this
        /// connection. Must equal the host's flood size exactly.
        inbound_frames: u32,
        /// Socket-path `Accepted::Full` count — the per-connection view of
        /// `I2sStreamSink::full_stalls`, counted at the `StallCountingSink` call site.
        sink_full_events: u32,
        peer_ip: [u8; 4],
        peer_port: u16,
    },
    TlsPskHandshake {
        peer_ip: [u8; 4],
        peer_port: u16,
        /// Wall-clock milliseconds from TCP connect to completed handshake (the
        /// cold-connect latency the streamer pays once per connect).
        handshake_ms: u32,
        /// Negotiated protocol version as mbedTLS spells it, e.g. `"TLSv1.2"`.
        version: TlsVersionStr,
        /// Negotiated ciphersuite as mbedTLS spells it, e.g.
        /// `"TLS-ECDHE-PSK-WITH-CHACHA20-POLY1305-SHA256"`.
        ciphersuite: TlsSuiteStr,
        /// Bytes echoed back through the tunnel and byte-matched by the device.
        echo_bytes: u32,
    },
    TlsPskRejected {
        peer_ip: [u8; 4],
        peer_port: u16,
        /// Milliseconds from the start of the handshake to the peer's refusal,
        /// measured. A refusal that arrives promptly is a real TLS alert; one
        /// that consumes the handshake deadline means nothing was refused —
        /// the device fails the test in that case rather than reporting it.
        reject_ms: u32,
    },
}

/// Negotiated TLS protocol version string, as reported by `mbedtls_ssl_get_version`.
/// Sized for `"TLSv1.2"` and any sibling spelling with room to spare.
pub type TlsVersionStr = heapless::String<16>;

/// Negotiated TLS ciphersuite name, as reported by `mbedtls_ssl_get_ciphersuite`.
/// Sized for the longest name this link can negotiate,
/// `"TLS-ECDHE-PSK-WITH-CHACHA20-POLY1305-SHA256"` (43 bytes).
pub type TlsSuiteStr = heapless::String<48>;

/// Raw `wifi_ps_type_t` value for `WIFI_PS_NONE` (power save off) — the expected
/// [`TestData::WifiPowerSaveCheck::ps_mode`]. Owned here because the wire meaning is
/// protocol-shared: the host cannot reference the device-only `esp_idf_svc::sys`
/// constant, so both sides compare against this single definition instead of a bare
/// literal re-asserted in prose.
pub const WIFI_PS_NONE_RAW: u32 = 0;

// ── Build identity ────────────────────────────────────────────────────────────

/// Build identity stamped into both firmware and harness at build time.
///
/// `commit` is a 40-hex-char git SHA-1 hash (or shorter abbreviated hash if the
/// tree was built from a shallow clone). `dirty` is true if the working tree had
/// uncommitted changes at build time.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub struct BuildId {
    pub commit: heapless::String<40>,
    pub dirty: bool,
}

// ── Log frame ─────────────────────────────────────────────────────────────────

/// Unsolicited log record emitted by the device's custom `log::Log` backend.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LogFrame {
    pub level: LogLevel,
    /// `log::Record::target()` — module path or similar. Truncated to 64 chars.
    pub target: heapless::String<64>,
    /// Log message. Truncated to 200 bytes at a UTF-8 char boundary
    /// (see [`format_truncating`]).
    pub message: heapless::String<200>,
}

// Graceful char-boundary truncation helpers live in the `truncfmt` crate (a
// heapless-only crate with no serde dependency). Re-exported here for source
// compatibility with `device_protocol::` call sites.
pub use truncfmt::{format_truncating, format_truncating_marked, TRUNCATION_SENTINEL};

/// Log level mirroring `log::Level`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

// ── Device console TX-sink pure logic ─────────────────────────────────────────

pub mod console;

// ── XVF3800 plausibility predicates ───────────────────────────────────────────
//
// The accept predicates guarding SPENERGY / DoA-azimuth values crossing the
// device↔host self-test protocol. Only the predicates are shared; each side keeps
// its own FAIL-string formatting.

/// Sane azimuth bound in radians: |x| ≤ π.
///
/// Convention: [-π, π]. For a linear array the practical range is [0, π] (broadside
/// half-plane) but the chip may output any value in [-π, π] depending on array
/// orientation and mounting. Values outside (or ±Inf) are structurally implausible.
pub const AZIMUTH_MAX_ABS: f32 = core::f32::consts::PI;

/// Accept predicate for a SPENERGY value: finite and non-negative (energy is never
/// negative). NaN and ±Inf are rejected.
pub fn sp_energy_ok(v: f32) -> bool {
    v.is_finite() && v >= 0.0
}

/// Accept predicate for a DoA azimuth value. NaN is acceptable (no speech / no beam
/// focused); any non-NaN value must be finite and within |x| ≤ [`AZIMUTH_MAX_ABS`].
pub fn doa_azimuth_ok(v: f32) -> bool {
    v.is_nan() || (v.is_finite() && v.abs() <= AZIMUTH_MAX_ABS)
}

// ── Typed self-test report builders ───────────────────────────────────────────

/// Build a `(Status::Ok, Payload::TestReport)` pair with empty detail.
pub fn test_report_ok(data: TestData) -> (Status, Payload) {
    (
        Status::Ok,
        Payload::TestReport(TestReport {
            detail: TestResultMsg::new(),
            data,
        }),
    )
}

/// Build a `(Status::Ok, Payload::TestReport)` pair carrying supplementary human text.
///
/// Detail is never machine-read, so overflow truncates at a UTF-8 char boundary with
/// the `…` sentinel rather than panicking.
pub fn test_report_ok_detail(data: TestData, args: core::fmt::Arguments) -> (Status, Payload) {
    let detail = format_truncating_marked::<{ TEST_RESULT_MSG_CAP }>(args, TRUNCATION_SENTINEL);
    (Status::Ok, Payload::TestReport(TestReport { detail, data }))
}

/// Build a `(Status::Fail, Payload::TestReport)` pair from a format string.
///
/// Data is [`TestData::None`]; the whole diagnostic is the truncating-with-sentinel
/// detail string.
pub fn test_report_fail_fmt(args: core::fmt::Arguments) -> (Status, Payload) {
    let detail = format_truncating_marked::<{ TEST_RESULT_MSG_CAP }>(args, TRUNCATION_SENTINEL);
    (
        Status::Fail,
        Payload::TestReport(TestReport {
            detail,
            data: TestData::None,
        }),
    )
}

/// Build a `(Status::Fail, Payload::TestReport)` pair from a static diagnostic string.
///
/// The no-formatting counterpart to [`test_report_fail_fmt`] for constant-message fail
/// paths; data is [`TestData::None`] and the message is truncated with the same sentinel.
pub fn test_report_fail(msg: &str) -> (Status, Payload) {
    test_report_fail_fmt(format_args!("{msg}"))
}

/// Build a failing report from a prefix plus a `Debug`-rendered value.
pub fn test_report_fail_detail(prefix: &str, detail: &dyn core::fmt::Debug) -> (Status, Payload) {
    test_report_fail_fmt(format_args!("{prefix}: {detail:?}"))
}

/// Build a `(Status::Fail, Payload::TestReport)` pair carrying typed `data`.
///
/// The failing counterpart to [`test_report_ok_detail`]: `data` rides along for
/// machine eval while the diagnostic is the truncating-with-sentinel detail string,
/// so a fail path with a typed payload keeps the same sentinel guarantee as the
/// other builders instead of hand-rolling `core::fmt::write`.
pub fn test_report_fail_data(data: TestData, args: core::fmt::Arguments) -> (Status, Payload) {
    let detail = format_truncating_marked::<{ TEST_RESULT_MSG_CAP }>(args, TRUNCATION_SENTINEL);
    (
        Status::Fail,
        Payload::TestReport(TestReport { detail, data }),
    )
}

// ── Device health threshold logic ─────────────────────────────────────────────
//
// Conservative floors that catch gross regressions without false-failing normal
// operation, plus the pure evaluation over them. `run_device_health_check` (device
// side, ESP-IDF FFI) reads the runtime metrics and calls `evaluate_health`; the
// pure part lives here so it is host-testable without FFI.

/// Minimum acceptable free heap (bytes). This is the primary leak guard.
///
/// The ~43.3 KB steady-state figure this floor was originally sized against predates the
/// PSRAM migration (design-delta-13/14) and is now stale: `heap-gate-measure`
/// (`docs/adr/2026/07/19-heap-gate-measure/implementation-log.md`) recorded post-PSRAM
/// `free_heap` of 124_164-125_824 B (~121-123 KiB) with WiFi/lwIP streaming and the inbound
/// PCM ring active, consistent with the post-PSRAM `min_heap` population independently
/// baked in `rtd-heap-floor-rebake`. The 36 KB floor is unchanged and still sits well below
/// every observed steady state.
///
/// Since the 2026-07-19 heap-floor rebake, `HEAP_MIN_EVER_FLOOR` (53_248) sits
/// *above* this floor. `min_heap` is a since-boot low watermark of `free_heap`,
/// so `min_heap ≤ free_heap` always; `free_heap < HEAP_FREE_FLOOR` therefore
/// implies `min_heap < HEAP_MIN_EVER_FLOOR` too. This floor's independent
/// failure mode is dominated in practice — `evaluate_health` checks `min_heap`
/// first so the more-informative (more-breached) failure is reported; this
/// floor is retained for message specificity and pure-predicate coverage, not
/// for independent detection.
pub const HEAP_FREE_FLOOR: u32 = 36_864;

/// Minimum acceptable lifetime-minimum free heap (bytes) — the since-boot
/// low-watermark, which only ratchets down.
///
/// Baked 2026-07-19 from five power-cycled cold-boot full-suite HIL runs at
/// normal signal strength (`docs/adr/2026/07/19-rtd-heap-floor-rebake/run-record.md`),
/// post-PSRAM-redesign: observed `mh_post` (the since-boot internal-RAM low
/// watermark, which also gates the DeviceHealthCheck self-test that samples
/// this same metric earlier in every suite run) ranged 76_008–78_564. Floor is
/// the largest multiple of 4 KiB ≤ 0.75 × the observed minimum (76_008), with
/// 25% headroom for run-to-run spread. Clean-link samples only (RSSI −60 to
/// −67 dBm at RTD start) — the weak-signal/stressed-link condition was not
/// measured; margin was not widened to compensate.
///
/// TODO(heap-floor-post-flash-boot-path-offset): baked exclusively on `POWERON`
/// samples, but this floor also gates `DeviceHealthCheck` on every boot path
/// including post-flash resets; a single post-flash sample measured 8 KB below the
/// bake minimum. See `TODO.md` for the reconciliation plan.
///
/// Triage rule for the unmeasured stressed-link condition (`design-delta-1.md` §1
/// skipped the weak-signal sample; margin was not widened to compensate): a floor
/// failure observed at RSSI ≤ −70 dBm at RTD/health-check start is presumed to be
/// this unmeasured regime, not a regression, and requires a paired clean-link run
/// before the floor itself is touched.
pub const HEAP_MIN_EVER_FLOOR: u32 = 53_248;

/// Minimum acceptable stack high-water mark for the protocol loop task (bytes).
/// `CONFIG_ESP_MAIN_TASK_STACK_SIZE=16384` (sdkconfig.defaults). A floor of 1 KB
/// catches near-exhausted stacks while leaving room for normal nesting depth.
/// If WiFi activity ever pushes the HWM below this floor, raise
/// `CONFIG_ESP_MAIN_TASK_STACK_SIZE` rather than lowering the floor.
pub const STACK_HWM_FLOOR: u32 = 1_024;

/// Pure health evaluation: checks three metrics against their floors.
///
/// Returns `Some(fail_report)` for the first metric below its floor, `None` when all
/// three are healthy. The healthy case carries no payload: the caller owns the typed
/// [`TestData::DeviceHealth`] report, which holds these metrics plus the per-thread
/// ones this function cannot see.
///
/// Extracted from `run_device_health_check` so threshold logic is unit-testable
/// on the host without ESP-IDF FFI. Field names are consistent across fail details
/// (heap_free / min_heap / stack_hwm / tx_write_failures everywhere).
///
/// `tx_write_failures` counts whole-frame TX drops (ring full, all-or-nothing write
/// returned 0). A non-zero value is environmental — ring fills while host port is
/// closed — not a device fault. The field is surfaced for observability only; it
/// does not affect pass/fail.
pub fn evaluate_health(
    free_heap: u32,
    min_heap: u32,
    stack_hwm: u32,
    tx_write_failures: u32,
) -> Option<(Status, Payload)> {
    // min_heap is checked before free_heap: since min_heap <= free_heap always
    // (min_heap is a since-boot low watermark of free_heap), a free_heap breach
    // never occurs without a min_heap breach at least as severe. Checking
    // min_heap first surfaces the binding constraint's message.
    if min_heap < HEAP_MIN_EVER_FLOOR {
        return Some(test_report_fail_fmt(format_args!(
            "FAIL min_heap={min_heap}<{HEAP_MIN_EVER_FLOOR} heap_free={free_heap} stack_hwm={stack_hwm} tx_write_failures={tx_write_failures}"
        )));
    }
    if free_heap < HEAP_FREE_FLOOR {
        return Some(test_report_fail_fmt(format_args!(
            "FAIL heap_free={free_heap}<{HEAP_FREE_FLOOR} min_heap={min_heap} stack_hwm={stack_hwm} tx_write_failures={tx_write_failures}"
        )));
    }
    if stack_hwm < STACK_HWM_FLOOR {
        return Some(test_report_fail_fmt(format_args!(
            "FAIL stack_hwm={stack_hwm}<{STACK_HWM_FLOOR} heap_free={free_heap} min_heap={min_heap} tx_write_failures={tx_write_failures}"
        )));
    }
    None
}

// ── COBS framing helpers ──────────────────────────────────────────────────────

pub mod log_tokens;

/// Encoding/framing helpers (postcard + COBS). Enabled by the `framing` Cargo feature.
/// Schema-only consumers (e.g. `build-id`) do not enable this feature and thus carry
/// zero postcard/encoding dependency.
/// Also compiled in `#[cfg(test)]` so the crate's own round-trip tests can use the
/// encode helpers without callers needing to enable `framing`.
#[cfg(any(feature = "framing", test))]
pub mod framing {
    use super::{DeviceFrame, Request};

    /// Encode a `DeviceFrame` into a COBS-framed postcard packet.
    ///
    /// Returns the number of bytes written into `buf`. The last byte is always `0`
    /// (the COBS frame delimiter).
    pub fn encode_device_frame(
        frame: &DeviceFrame,
        buf: &mut [u8],
    ) -> Result<usize, postcard::Error> {
        postcard::to_slice_cobs(frame, buf).map(|s| s.len())
    }

    /// Encode a `Request` into a COBS-framed postcard packet.
    ///
    /// Returns the number of bytes written into `buf`.
    pub fn encode_request(req: &Request, buf: &mut [u8]) -> Result<usize, postcard::Error> {
        postcard::to_slice_cobs(req, buf).map(|s| s.len())
    }
}

// ── Postcard-derived discriminant helper ─────────────────────────────────────

/// The 1-byte postcard wire discriminant for a fieldless `TestName` variant.
///
/// Derived from postcard rather than a hand-rolled table: serializing a unit enum
/// variant with `postcard::to_slice` yields its varint index as a single byte.
/// With 17 variants (indices 0–16) the index always fits in one byte; `to_slice`
/// would return an error if a future expansion required a second varint byte, making
/// the overflow detectable at test time.
///
/// **Feature gate is mandatory.** `postcard` is an optional dep pulled in only by
/// `framing = ["dep:postcard"]`. Schema-only consumers such as `build-id` do not
/// enable `framing` and must not reference this function.
///
/// Use `postcard::to_slice`, **not** `to_slice_cobs`: COBS framing prepends overhead
/// bytes; the bare varint index is `buf[0]` after plain `to_slice`.
#[cfg(any(feature = "framing", test))]
pub fn test_name_discriminant(t: &TestName) -> u8 {
    let mut buf = [0u8; 1];
    postcard::to_slice(t, &mut buf).expect("TestName index must fit one byte");
    buf[0]
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use heapless::String as HString;
    use postcard::accumulator::{CobsAccumulator, FeedResult};

    /// Decode a single COBS-framed postcard packet from `encoded` and return the value.
    ///
    /// Panics with a distinct message on `OverFull` vs `DeserError` so roundtrip test
    /// failures are immediately diagnosable without reconstructing which arm fired.
    fn decode_one_cobs<T>(encoded: &[u8]) -> T
    where
        T: serde::de::DeserializeOwned,
    {
        let mut acc: CobsAccumulator<512> = CobsAccumulator::new();
        match acc.feed::<T>(encoded) {
            FeedResult::Success {
                data,
                remaining: _r,
            } => data,
            FeedResult::OverFull(r) => {
                panic!(
                    "decode_one_cobs: OverFull — encoded frame ({} bytes) exceeds \
                     accumulator capacity; remaining {} bytes unprocessed",
                    encoded.len(),
                    r.len()
                );
            }
            FeedResult::DeserError(r) => {
                panic!(
                    "decode_one_cobs: DeserError — postcard/serde rejected the frame \
                     ({} encoded bytes, {} remaining after error)",
                    encoded.len(),
                    r.len()
                );
            }
            FeedResult::Consumed => {
                panic!(
                    "decode_one_cobs: Consumed — accumulator consumed all {} bytes \
                     without producing a frame; frame may be incomplete",
                    encoded.len()
                );
            }
        }
    }

    fn roundtrip_device_frame(frame: &DeviceFrame) -> DeviceFrame {
        let mut buf = [0u8; 512];
        let len = framing::encode_device_frame(frame, &mut buf).expect("encode failed");
        decode_one_cobs(&buf[..len])
    }

    #[test]
    fn roundtrip_response_ok_pong() {
        let frame = DeviceFrame::Response(Response {
            id: 1,
            status: Status::Ok,
            payload: Payload::Pong({
                let mut s = HString::new();
                s.push_str("pong").unwrap();
                s
            }),
        });
        assert_eq!(roundtrip_device_frame(&frame), frame);
    }

    #[test]
    fn roundtrip_response_identify() {
        // populated from REGISTERED_TESTS
        let mut tests: HVec<TestName, MAX_TESTS> = HVec::new();
        for &t in REGISTERED_TESTS {
            tests.push(t).unwrap();
        }
        // Pin the vector contents against REGISTERED_TESTS before the codec test,
        // so accidental truncation or reordering of the registry fails here with a
        // clear message rather than only in registered_tests_covers_all_variants.
        assert_eq!(
            tests.as_slice(),
            REGISTERED_TESTS,
            "Identify tests vector must equal REGISTERED_TESTS exactly"
        );
        let mut commit = HString::new();
        commit
            .push_str("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2")
            .unwrap();
        let frame = DeviceFrame::Response(Response {
            id: 2,
            status: Status::Ok,
            payload: Payload::Identify {
                build: BuildId {
                    commit,
                    dirty: false,
                },
                tests,
            },
        });
        assert_eq!(roundtrip_device_frame(&frame), frame);
    }

    #[test]
    fn roundtrip_log_frame() {
        let mut target = HString::new();
        target.push_str("respeaker_pod").unwrap();
        let mut message = HString::new();
        message.push_str("firmware starting").unwrap();
        let frame = DeviceFrame::Log(LogFrame {
            level: LogLevel::Info,
            target,
            message,
        });
        assert_eq!(roundtrip_device_frame(&frame), frame);
    }

    #[test]
    fn roundtrip_heartbeat() {
        let frame = DeviceFrame::Heartbeat;
        assert_eq!(roundtrip_device_frame(&frame), frame);
    }

    /// Golden-byte assertions for every `TestName` variant encoded as a `Command::RunTest`
    /// request.  These pin the exact COBS-framed postcard bytes, catching discriminant
    /// collisions or serde regressions for each variant independently.
    ///
    /// Golden bytes were generated with `id=0`; each row is 5 bytes followed by a 0x00
    /// COBS delimiter.  COBS replaces every 0x00 in the postcard payload with a chain
    /// pointer, so the raw postcard layout `[id-varint, cmd-discriminant,
    /// testname-discriminant]` is not directly readable from the wire bytes.
    ///
    /// Postcard discriminants (id=0, Command::RunTest=discriminant 0):
    ///   TestName: Ping=0, Identify=1, GpioSelfTest=2, DeviceHealthCheck=3, I2cBusScan=4,
    ///   Xvf3800RegRead=5, Xvf3800DoAPlausibility=6, I2sWaveformSanity=7,
    ///   WifiAssociate=8, UdpRoundtrip=9, TcpRoundtrip=10, TlsReachability=11, WifiScan=12,
    ///   Xvf3800SpEnergy=13, WifiReassociation=14, GatewayProbeGate=15, TcpInboundFrames=16,
    ///   TcpSendBackpressure=17, SpeakerOutput=18, AmpAlwaysOnGpoInert=19,
    ///   CapturePeriodicLine=20, PlaybackDrainRate=21, PollReadinessBidir=22,
    ///   FullDuplexRxIntegrity=23, StreamRealtimeDuplex=24, PsramIdentity=25
    ///
    /// Ping encodes to all-0x01 COBS bytes (before the 0x00 delimiter) because all three
    /// postcard bytes are 0x00 (id=0, cmd=0, testname=0), and COBS replaces each zero with
    /// a chain pointer of 0x01 (next zero immediately).  Other variants have a non-zero
    /// testname discriminant, which COBS leaves in place and changes only the preceding
    /// chain pointer.
    ///
    /// For discriminants 8–13, the testname varint encodes as a single byte (varint for
    /// 8–127 is just the byte itself, high bit clear). The COBS encoding for these follows
    /// the same pattern as 1–7.
    #[test]
    fn request_run_test_golden_bytes_all_variants() {
        let cases: &[(TestName, &[u8])] = &[
            (TestName::Ping, &[0x01, 0x01, 0x01, 0x01, 0x00]),
            (TestName::Identify, &[0x01, 0x01, 0x02, 0x01, 0x00]),
            (TestName::GpioSelfTest, &[0x01, 0x01, 0x02, 0x02, 0x00]),
            (TestName::DeviceHealthCheck, &[0x01, 0x01, 0x02, 0x03, 0x00]),
            (TestName::I2cBusScan, &[0x01, 0x01, 0x02, 0x04, 0x00]),
            (TestName::Xvf3800RegRead, &[0x01, 0x01, 0x02, 0x05, 0x00]),
            (
                TestName::Xvf3800DoAPlausibility,
                &[0x01, 0x01, 0x02, 0x06, 0x00],
            ),
            (TestName::I2sWaveformSanity, &[0x01, 0x01, 0x02, 0x07, 0x00]),
            (TestName::WifiAssociate, &[0x01, 0x01, 0x02, 0x08, 0x00]),
            (TestName::UdpRoundtrip, &[0x01, 0x01, 0x02, 0x09, 0x00]),
            (TestName::TcpRoundtrip, &[0x01, 0x01, 0x02, 0x0a, 0x00]),
            (TestName::TlsReachability, &[0x01, 0x01, 0x02, 0x0b, 0x00]),
            (TestName::WifiScan, &[0x01, 0x01, 0x02, 0x0c, 0x00]),
            (TestName::Xvf3800SpEnergy, &[0x01, 0x01, 0x02, 0x0d, 0x00]),
            (TestName::WifiReassociation, &[0x01, 0x01, 0x02, 0x0e, 0x00]),
            (TestName::GatewayProbeGate, &[0x01, 0x01, 0x02, 0x0f, 0x00]),
            (TestName::TcpInboundFrames, &[0x01, 0x01, 0x02, 0x10, 0x00]),
            (
                TestName::TcpSendBackpressure,
                &[0x01, 0x01, 0x02, 0x11, 0x00],
            ),
            (TestName::SpeakerOutput, &[0x01, 0x01, 0x02, 0x12, 0x00]),
            (
                TestName::AmpAlwaysOnGpoInert,
                &[0x01, 0x01, 0x02, 0x13, 0x00],
            ),
            (
                TestName::CapturePeriodicLine,
                &[0x01, 0x01, 0x02, 0x14, 0x00],
            ),
            (TestName::PlaybackDrainRate, &[0x01, 0x01, 0x02, 0x15, 0x00]),
            (
                TestName::PollReadinessBidir,
                &[0x01, 0x01, 0x02, 0x16, 0x00],
            ),
            (
                TestName::FullDuplexRxIntegrity,
                &[0x01, 0x01, 0x02, 0x17, 0x00],
            ),
            (
                TestName::StreamRealtimeDuplex,
                &[0x01, 0x01, 0x02, 0x18, 0x00],
            ),
            (TestName::PsramIdentity, &[0x01, 0x01, 0x02, 0x19, 0x00]),
        ];
        for (variant, expected) in cases {
            let req = Request {
                id: 0,
                command: Command::RunTest(*variant),
            };
            let mut buf = [0u8; 64];
            let len = framing::encode_request(&req, &mut buf).expect("encode failed");
            assert_eq!(
                &buf[..len],
                *expected,
                "golden bytes mismatch for TestName::{:?} — discriminant collision or serde regression",
                variant
            );
            // Also verify the bytes decode back to the original request.
            assert_eq!(decode_one_cobs::<Request>(&buf[..len]), req);
        }
    }

    #[test]
    fn sp_energy_ok_accepts_finite_nonnegative() {
        assert!(sp_energy_ok(0.0));
        assert!(sp_energy_ok(1.5));
        assert!(sp_energy_ok(f32::MAX));
    }

    #[test]
    fn sp_energy_ok_rejects_nan_inf_negative() {
        assert!(!sp_energy_ok(f32::NAN));
        assert!(!sp_energy_ok(f32::INFINITY));
        assert!(!sp_energy_ok(f32::NEG_INFINITY));
        assert!(!sp_energy_ok(-0.001));
        assert!(!sp_energy_ok(-1.0));
    }

    #[test]
    fn doa_azimuth_ok_accepts_nan() {
        // NaN is acceptable (no speech / no beam focused) — must NOT be flipped by
        // a naive `is_finite() && abs <= MAX`.
        assert!(doa_azimuth_ok(f32::NAN));
    }

    #[test]
    fn doa_azimuth_ok_boundary_and_range() {
        assert!(doa_azimuth_ok(0.0));
        assert!(doa_azimuth_ok(AZIMUTH_MAX_ABS));
        assert!(doa_azimuth_ok(-AZIMUTH_MAX_ABS));
        // Just outside the bound → rejected.
        assert!(!doa_azimuth_ok(AZIMUTH_MAX_ABS + 0.001));
        assert!(!doa_azimuth_ok(-AZIMUTH_MAX_ABS - 0.001));
    }

    #[test]
    fn doa_azimuth_ok_rejects_inf() {
        assert!(!doa_azimuth_ok(f32::INFINITY));
        assert!(!doa_azimuth_ok(f32::NEG_INFINITY));
    }

    // ── evaluate_health ────────────────────────────────────────────────────────
    //
    // Ported from the device's on-device unit tests (previously xtensa-only, which is
    // why the host mirror's stale floors went unnoticed). Literals are pinned against
    // the real device floors and guarded by evaluate_health_floor_constants_match_literals.
    //
    // `evaluate_health` is a pure predicate that does not cross-validate its two heap
    // arguments, so some boundary tests below (e.g. `free_heap` below floor with
    // `min_heap` at floor, where `min_heap > free_heap`) construct combinations that
    // cannot occur on real hardware — there `min_heap <= free_heap` always holds,
    // since `min_heap` is a since-boot low watermark of `free_heap`. Those tests still
    // exercise the branch logic itself in isolation; they are not evidence the
    // `heap_free` branch fires independently in production (see `HEAP_FREE_FLOOR`'s
    // doc-comment).

    /// Unwrap a fail report from `evaluate_health` into (status, detail).
    fn health_fail(result: Option<(Status, Payload)>) -> (Status, String) {
        let (status, payload) = result.expect("expected a fail report");
        match payload {
            Payload::TestReport(r) => {
                assert_eq!(r.data, TestData::None, "fail reports carry TestData::None");
                (status, r.detail.as_str().to_string())
            }
            other => panic!("unexpected payload variant: {other:?}"),
        }
    }

    #[test]
    fn evaluate_health_all_at_floor_pass() {
        assert!(
            evaluate_health(HEAP_FREE_FLOOR, HEAP_MIN_EVER_FLOOR, STACK_HWM_FLOOR, 0).is_none(),
            "all metrics at floor must pass"
        );
    }

    #[test]
    fn evaluate_health_heap_free_below_floor_fails() {
        let (status, msg) = health_fail(evaluate_health(
            HEAP_FREE_FLOOR - 1,
            HEAP_MIN_EVER_FLOOR,
            STACK_HWM_FLOOR,
            0,
        ));
        assert_eq!(status, Status::Fail);
        assert!(
            msg.starts_with(&format!(
                "FAIL heap_free={}<{HEAP_FREE_FLOOR}",
                HEAP_FREE_FLOOR - 1
            )),
            "expected FAIL heap_free prefix; got: {msg}"
        );
    }

    #[test]
    fn evaluate_health_min_heap_below_floor_fails() {
        let (status, msg) = health_fail(evaluate_health(
            HEAP_FREE_FLOOR,
            HEAP_MIN_EVER_FLOOR - 1,
            STACK_HWM_FLOOR,
            0,
        ));
        assert_eq!(status, Status::Fail);
        // Hardcoded literal (not built from the constants `evaluate_health` itself
        // interpolates): witnesses both the baked number and the message format
        // independently of `evaluate_health_floor_constants_match_literals`.
        assert!(
            msg.starts_with("FAIL min_heap=53247<53248"),
            "expected FAIL min_heap prefix; got: {msg}"
        );
    }

    #[test]
    fn evaluate_health_stack_hwm_below_floor_fails() {
        let (status, msg) = health_fail(evaluate_health(
            HEAP_FREE_FLOOR,
            HEAP_MIN_EVER_FLOOR,
            STACK_HWM_FLOOR - 1,
            0,
        ));
        assert_eq!(status, Status::Fail);
        assert!(
            msg.starts_with(&format!(
                "FAIL stack_hwm={}<{STACK_HWM_FLOOR}",
                STACK_HWM_FLOOR - 1
            )),
            "expected FAIL stack_hwm prefix; got: {msg}"
        );
    }

    /// min_heap check fires before heap_free (priority ordering) — min_heap is the
    /// binding constraint whenever both are breached, since min_heap <= free_heap.
    #[test]
    fn evaluate_health_min_heap_fails_first() {
        let (status, msg) = health_fail(evaluate_health(0, 0, 0, 0));
        assert_eq!(status, Status::Fail);
        assert!(
            msg.starts_with("FAIL min_heap="),
            "min_heap check must fire before heap_free; got: {msg}"
        );
    }

    /// Non-zero tx_write_failures is environmental, never a fault. It reaches the host
    /// as a `TestData::DeviceHealth` field built by the caller, not through this gate.
    #[test]
    fn evaluate_health_tx_failures_do_not_fail() {
        assert!(
            evaluate_health(HEAP_FREE_FLOOR, HEAP_MIN_EVER_FLOOR, STACK_HWM_FLOOR, 7).is_none(),
            "non-zero tx_write_failures must not fail"
        );
    }

    /// Guard: these floors are hardware-baked, not free to edit in passing. A move
    /// forces a deliberate re-bake with fresh provenance, not a literal-test fixup.
    #[test]
    fn evaluate_health_floor_constants_match_literals() {
        assert_eq!(
            HEAP_FREE_FLOOR, 36_864,
            "HEAP_FREE_FLOOR changed; this floor is hardware-baked (see its doc-comment) — \
             update the run record with fresh provenance before changing it"
        );
        assert_eq!(
            HEAP_MIN_EVER_FLOOR, 53_248,
            "HEAP_MIN_EVER_FLOOR changed; this floor is hardware-baked — update \
             docs/adr/2026/07/19-rtd-heap-floor-rebake/run-record.md with fresh provenance \
             before changing it"
        );
        assert_eq!(
            STACK_HWM_FLOOR, 1_024,
            "STACK_HWM_FLOOR constant changed; update literal tests"
        );
    }

    #[test]
    fn two_frames_decode_as_two() {
        // Concatenate two COBS-framed packets; both must decode independently.
        let frame1 = DeviceFrame::Heartbeat;
        let frame2 = DeviceFrame::Response(Response {
            id: 1,
            status: Status::Ok,
            payload: Payload::Empty,
        });

        let mut buf1 = [0u8; 64];
        let mut buf2 = [0u8; 64];
        let len1 = framing::encode_device_frame(&frame1, &mut buf1).unwrap();
        let len2 = framing::encode_device_frame(&frame2, &mut buf2).unwrap();

        let mut combined = alloc::vec![0u8; len1 + len2];
        combined[..len1].copy_from_slice(&buf1[..len1]);
        combined[len1..].copy_from_slice(&buf2[..len2]);

        let mut acc: CobsAccumulator<512> = CobsAccumulator::new();
        let mut decoded = alloc::vec![];
        let mut remaining: &[u8] = &combined;
        loop {
            match acc.feed::<DeviceFrame>(remaining) {
                FeedResult::Success { data, remaining: r } => {
                    decoded.push(data);
                    remaining = r;
                    if remaining.is_empty() {
                        break;
                    }
                }
                FeedResult::Consumed => break,
                FeedResult::OverFull(_) | FeedResult::DeserError(_) => {
                    // Neither should occur for two well-formed frames.
                    // Use unreachable! rather than break so a future edit that
                    // accidentally corrupts the test data fails loudly here
                    // rather than silently propagating to the outer assert_eq.
                    unreachable!("unexpected OverFull/DeserError for well-formed frames");
                }
            }
        }
        assert_eq!(decoded, alloc::vec![frame1, frame2]);
    }

    /// A COBS-layer framing error (overhead byte pointing past the available data)
    /// must be skipped; the following clean frame still decodes correctly.
    ///
    /// Both this test and `corrupt_middle_frame_skipped_next_decodes` produce
    /// `FeedResult::DeserError` — postcard's `CobsAccumulator` has no distinct
    /// FeedResult arm for "bad COBS overhead" vs. "bad serde payload"; both become
    /// `DeserError`.  The behavioral difference is in what fails inside
    /// `from_bytes_cobs`: here the COBS overhead byte lies about frame length
    /// (overhead 0x05, delimiter 1 byte later); in the companion test a valid COBS
    /// empty frame fails postcard deserialization.  Both paths exercise the same
    /// accumulator resync code.
    ///
    /// Byte sequence: `[0x05, 0x00, <valid Heartbeat frame>]`
    ///   - `0x05` = COBS overhead byte claiming the next zero is 4 bytes ahead.
    ///   - `0x00` = frame delimiter arriving only 1 byte later → overhead lies → COBS error.
    ///   - Valid Heartbeat frame follows, which must decode despite the prior corruption.
    #[test]
    fn cobs_layer_corrupt_overhead_resyncs_next_frame() {
        let good = DeviceFrame::Heartbeat;
        let mut good_buf = [0u8; 64];
        let good_len = framing::encode_device_frame(&good, &mut good_buf).unwrap();

        // Malformed COBS: overhead byte 0x05 claims the next zero is 4 bytes away,
        // but the actual zero delimiter arrives at the very next byte (offset 1).
        // The COBS accumulator sees the overhead as a lie and produces a framing error.
        let cobs_corrupt: &[u8] = &[0x05, 0x00];

        let mut combined = alloc::vec![];
        combined.extend_from_slice(cobs_corrupt);
        combined.extend_from_slice(&good_buf[..good_len]);

        let mut acc: CobsAccumulator<512> = CobsAccumulator::new();
        let mut decoded = alloc::vec![];
        let mut remaining: &[u8] = &combined;
        loop {
            match acc.feed::<DeviceFrame>(remaining) {
                FeedResult::Success { data, remaining: r } => {
                    decoded.push(data);
                    remaining = r;
                }
                FeedResult::DeserError(r) | FeedResult::OverFull(r) => {
                    // Expected for the corrupt frame — skip and resync.
                    remaining = r;
                }
                FeedResult::Consumed => break,
            }
            if remaining.is_empty() {
                break;
            }
        }
        assert!(
            decoded.contains(&good),
            "Heartbeat must decode after COBS-layer corrupt overhead byte; got: {:?}",
            decoded
        );
    }

    #[test]
    fn corrupt_middle_frame_skipped_next_decodes() {
        // A corrupt frame (bad COBS payload) must be skipped; the following clean
        // frame still decodes (COBS resync on the next zero delimiter).
        let good = DeviceFrame::Heartbeat;
        let mut good_buf = [0u8; 64];
        let good_len = framing::encode_device_frame(&good, &mut good_buf).unwrap();

        // A single zero byte is a valid COBS "empty" frame which will produce a
        // deserialization error (empty payload is not a valid DeviceFrame), simulating
        // a corrupt frame.
        let corrupt: &[u8] = &[0x01, 0x00]; // COBS-encoded empty data, will fail serde

        let mut combined = alloc::vec![];
        combined.extend_from_slice(corrupt);
        combined.extend_from_slice(&good_buf[..good_len]);

        let mut acc: CobsAccumulator<512> = CobsAccumulator::new();
        let mut decoded = alloc::vec![];
        let mut remaining: &[u8] = &combined;
        loop {
            match acc.feed::<DeviceFrame>(remaining) {
                FeedResult::Success { data, remaining: r } => {
                    decoded.push(data);
                    remaining = r;
                }
                FeedResult::DeserError(r) => {
                    // Expected: corrupt frame → skip, continue
                    remaining = r;
                }
                FeedResult::Consumed => break,
                FeedResult::OverFull(r) => {
                    remaining = r;
                }
            }
            if remaining.is_empty() {
                break;
            }
        }
        assert!(
            decoded.contains(&good),
            "next clean frame must decode after corrupt frame; got: {:?}",
            decoded
        );
    }

    #[test]
    fn encode_at_capacity_succeeds_pong() {
        // Exactly 64-char pong string must encode without error.
        let mut s = HString::<64>::new();
        for _ in 0..64 {
            s.push('x').unwrap();
        }
        let frame = DeviceFrame::Response(Response {
            id: 0,
            status: Status::Ok,
            payload: Payload::Pong(s),
        });
        let mut buf = [0u8; 512];
        framing::encode_device_frame(&frame, &mut buf).expect("at-capacity encode must succeed");
    }

    /// `LogFrame.target` at full 64-char capacity must encode without error.
    /// Guards against copy-paste bound errors (e.g. String<6> instead of String<64>).
    #[test]
    fn encode_at_capacity_succeeds_log_target() {
        let mut target = HString::<64>::new();
        for _ in 0..64 {
            target.push('t').unwrap();
        }
        let mut message = HString::<200>::new();
        message.push_str("msg").unwrap();
        let frame = DeviceFrame::Log(LogFrame {
            level: LogLevel::Info,
            target,
            message,
        });
        let mut buf = [0u8; 512];
        framing::encode_device_frame(&frame, &mut buf)
            .expect("at-capacity log target encode must succeed");
    }

    /// `LogFrame.message` at full 200-char capacity must encode without error.
    /// Guards against copy-paste bound errors (e.g. String<20> instead of String<200>).
    #[test]
    fn encode_at_capacity_succeeds_log_message() {
        let mut target = HString::<64>::new();
        target.push_str("tgt").unwrap();
        let mut message = HString::<200>::new();
        for _ in 0..200 {
            message.push('m').unwrap();
        }
        let frame = DeviceFrame::Log(LogFrame {
            level: LogLevel::Warn,
            target,
            message,
        });
        let mut buf = [0u8; 512];
        framing::encode_device_frame(&frame, &mut buf)
            .expect("at-capacity log message encode must succeed");
    }

    // ── Strictness rejection tests (design §4, §2.2) ──────────────────────────
    //
    // These tests verify that a wire frame whose shape diverges from the compiled
    // schema produces a hard postcard/serde error, never silently decodes.

    /// An enum discriminant not present in `DeviceFrame` (value 5) must hard-error.
    /// Postcard encodes enum discriminants as varint; DeviceFrame has variants 0–2.
    #[test]
    fn unknown_device_frame_discriminant_errors() {
        // Postcard varint encoding of discriminant 5 with no payload bytes.
        let raw: &[u8] = &[5u8];
        let result = postcard::from_bytes::<DeviceFrame>(raw);
        assert!(
            result.is_err(),
            "unknown discriminant 5 must produce Err, not silent decode; got: {result:?}"
        );
    }

    /// An unknown `Command` discriminant must hard-error — no silent decode.
    #[test]
    fn unknown_command_discriminant_errors() {
        // Discriminant 5 (SetVadHangover) must decode successfully.
        // Encode SetVadHangover { hangover_ms: 3000 }: discriminant 5 (varint 0x05),
        // followed by the varint for 3000 (0xb8, 0x17).
        let raw_valid: &[u8] = &[0x05u8, 0xb8, 0x17];
        let result = postcard::from_bytes::<Command>(raw_valid);
        assert!(
            matches!(result, Ok(Command::SetVadHangover { hangover_ms }) if hangover_ms == 3000),
            "discriminant 5 (SetVadHangover) must decode successfully; got: {result:?}"
        );

        // Discriminant 6 (ClearWifiCredentials) must decode successfully.
        let raw_clear: &[u8] = &[6u8];
        let result = postcard::from_bytes::<Command>(raw_clear);
        assert!(
            matches!(result, Ok(Command::ClearWifiCredentials)),
            "discriminant 6 (ClearWifiCredentials) must decode successfully; got: {result:?}"
        );

        // Discriminant 8 (ClearTemporaryWifiConfig) must decode successfully.
        let raw_clear_temp: &[u8] = &[8u8];
        let result = postcard::from_bytes::<Command>(raw_clear_temp);
        assert!(
            matches!(result, Ok(Command::ClearTemporaryWifiConfig)),
            "discriminant 8 (ClearTemporaryWifiConfig) must decode successfully; got: {result:?}"
        );

        // Discriminant 9 (ProvisionAudioPsk) must decode successfully: the varint
        // discriminant is followed by the 32 raw key bytes (postcard encodes a fixed-size
        // array with no length prefix).
        let mut raw_psk = [0u8; 33];
        raw_psk[0] = 9;
        for (i, b) in raw_psk[1..].iter_mut().enumerate() {
            *b = (i + 1) as u8;
        }
        let result = postcard::from_bytes::<Command>(&raw_psk);
        assert!(
            matches!(result, Ok(Command::ProvisionAudioPsk { key }) if key[0] == 1 && key[31] == 32),
            "discriminant 9 (ProvisionAudioPsk) must decode successfully; got: {result:?}"
        );

        let raw: &[u8] = &[10u8];
        let result = postcard::from_bytes::<Command>(raw);
        assert!(
            result.is_err(),
            "unknown Command discriminant 10 must produce Err, not silent decode; got: {result:?}"
        );
    }

    /// `Command::ProvisionAudioPsk` golden bytes and roundtrip.
    ///
    /// Postcard encodes `[u8; 32]` as 32 raw bytes with no length prefix, so the raw
    /// payload for `Request { id: 0, .. }` is `[id=0x00, disc=0x09, key[0..32]]`. The key
    /// used here is `1..=32` (no zero bytes), so COBS emits one escape byte for the zero
    /// id and then a single 33-byte run.
    #[test]
    fn provision_audio_psk_golden_roundtrip() {
        let mut key = [0u8; 32];
        for (i, b) in key.iter_mut().enumerate() {
            *b = (i + 1) as u8;
        }
        let req = Request {
            id: 0,
            command: Command::ProvisionAudioPsk { key },
        };
        let mut buf = [0u8; 64];
        let len = framing::encode_request(&req, &mut buf).expect("encode failed");
        assert_eq!(decode_one_cobs::<Request>(&buf[..len]), req);

        let mut expected = alloc::vec![0x01u8, 0x22, 0x09];
        expected.extend_from_slice(&key);
        expected.push(0x00);
        assert_eq!(
            &buf[..len],
            expected.as_slice(),
            "ProvisionAudioPsk golden bytes mismatch — discriminant or array layout regression"
        );
    }

    /// A `Payload::PodId` response round-trips, and its golden bytes pin discriminant 4.
    ///
    /// The provisioning host reads the pod identity out of this payload to key its PSK
    /// table, so a silent discriminant shift would misattribute keys.
    #[test]
    fn payload_pod_id_golden_roundtrip() {
        let mut pod_id = heapless::String::<32>::new();
        pod_id.push_str("pod-aabbcc").unwrap();
        let frame = DeviceFrame::Response(Response {
            id: 1,
            status: Status::Ok,
            payload: Payload::PodId(pod_id),
        });
        let mut buf = [0u8; 64];
        let len = framing::encode_device_frame(&frame, &mut buf).expect("encode failed");
        assert_eq!(decode_one_cobs::<DeviceFrame>(&buf[..len]), frame);

        // Raw postcard: [DeviceFrame::Response=0x00, id=0x01, Status::Ok=0x00,
        // Payload::PodId=0x04, str len=0x0a, 10 ASCII bytes]. COBS escapes both zero
        // bytes, so the frame is 0x01, 0x02, 0x01, then a 12-byte run (code 0x0d).
        let mut expected = alloc::vec![0x01u8, 0x02, 0x01, 0x0d, 0x04, 0x0a];
        expected.extend_from_slice(b"pod-aabbcc");
        expected.push(0x00);
        assert_eq!(
            &buf[..len],
            expected.as_slice(),
            "Payload::PodId golden bytes mismatch — discriminant or layout regression"
        );
    }

    /// `Command::ClearWifiCredentials` golden bytes and roundtrip.
    ///
    /// Unit variant, so the postcard payload for `Request { id: 0, .. }` is
    /// `[id-varint=0x00, cmd-discriminant=0x06]`; COBS escapes the zero id byte.
    #[test]
    fn clear_wifi_credentials_golden_roundtrip() {
        let req = Request {
            id: 0,
            command: Command::ClearWifiCredentials,
        };
        let mut buf = [0u8; 32];
        let len = framing::encode_request(&req, &mut buf).expect("encode failed");
        assert_eq!(decode_one_cobs::<Request>(&buf[..len]), req);
        let expected: &[u8] = &[0x01, 0x02, 0x06, 0x00];
        assert_eq!(
            &buf[..len],
            expected,
            "ClearWifiCredentials golden bytes mismatch — discriminant or layout regression"
        );
    }

    /// `Command::SetTemporaryWifiConfig` golden bytes and roundtrip.
    ///
    /// Payload: `Request { id: 0, command: SetTemporaryWifiConfig { ssid: "n", passphrase: "p" } }`.
    /// Postcard encodes a `heapless::String` as a varint length prefix + UTF-8 bytes.
    #[test]
    fn set_temporary_wifi_config_golden_roundtrip() {
        let mut ssid = heapless::String::<32>::new();
        ssid.push_str("n").unwrap();
        let mut passphrase = heapless::String::<64>::new();
        passphrase.push_str("p").unwrap();
        let req = Request {
            id: 0,
            command: Command::SetTemporaryWifiConfig { ssid, passphrase },
        };
        let mut buf = [0u8; 32];
        let len = framing::encode_request(&req, &mut buf).expect("encode failed");
        assert_eq!(decode_one_cobs::<Request>(&buf[..len]), req);
        // id=0x00, discriminant=0x07, ssid len=0x01 'n'=0x6e, pass len=0x01 'p'=0x70.
        let expected: &[u8] = &[0x01, 0x06, 0x07, 0x01, 0x6e, 0x01, 0x70, 0x00];
        assert_eq!(
            &buf[..len],
            expected,
            "SetTemporaryWifiConfig golden bytes mismatch — discriminant or layout regression"
        );
    }

    /// `Command::ClearTemporaryWifiConfig` golden bytes and roundtrip.
    #[test]
    fn clear_temporary_wifi_config_golden_roundtrip() {
        let req = Request {
            id: 0,
            command: Command::ClearTemporaryWifiConfig,
        };
        let mut buf = [0u8; 32];
        let len = framing::encode_request(&req, &mut buf).expect("encode failed");
        assert_eq!(decode_one_cobs::<Request>(&buf[..len]), req);
        let expected: &[u8] = &[0x01, 0x02, 0x08, 0x00];
        assert_eq!(
            &buf[..len],
            expected,
            "ClearTemporaryWifiConfig golden bytes mismatch — discriminant or layout regression"
        );
    }

    /// `SetVadThreshold` round-trip: postcard encode → decode equality.
    #[test]
    fn set_vad_threshold_roundtrip() {
        let req = Request {
            id: 42,
            command: Command::SetVadThreshold { threshold: 1.5 },
        };
        let mut buf = [0u8; 64];
        let len = framing::encode_request(&req, &mut buf).expect("encode failed");
        let decoded = decode_one_cobs::<Request>(&buf[..len]);
        assert_eq!(decoded.id, 42);
        match decoded.command {
            Command::SetVadThreshold { threshold } => {
                assert!(
                    (threshold - 1.5f32).abs() < 1e-6,
                    "threshold round-trip mismatch: {threshold}"
                );
            }
            other => panic!("expected SetVadThreshold; got {other:?}"),
        }
    }

    /// Golden-byte test for `SetVadThreshold { threshold: 1.0 }` (discriminant 4).
    ///
    /// postcard serializes `f32` as `v.to_bits().to_le_bytes()` — 4 fixed little-endian
    /// bytes. 1.0f32.to_bits() = 0x3f800000, LE = [0x00, 0x00, 0x80, 0x3f].
    /// With id=0 (varint 0x00), Command discriminant 4 (varint 0x04), and the 4 payload
    /// bytes, the raw postcard is [0x00, 0x04, 0x00, 0x00, 0x80, 0x3f].
    ///
    /// The pinned bytes below were generated empirically and verified to round-trip.
    #[test]
    fn set_vad_threshold_golden_bytes() {
        let req = Request {
            id: 0,
            command: Command::SetVadThreshold { threshold: 1.0 },
        };
        let mut buf = [0u8; 64];
        let len = framing::encode_request(&req, &mut buf).expect("encode failed");
        let encoded = &buf[..len];

        // Verify round-trip first (always meaningful).
        let decoded = decode_one_cobs::<Request>(encoded);
        assert_eq!(decoded.id, 0);
        match decoded.command {
            Command::SetVadThreshold { threshold } => {
                assert!(
                    (threshold - 1.0f32).abs() < 1e-6,
                    "golden round-trip mismatch: {threshold}"
                );
            }
            other => panic!("golden: expected SetVadThreshold; got {other:?}"),
        }

        // Pin the exact wire bytes: id=0, discriminant 4, f32 1.0 LE = 0x3f800000.
        // Postcard raw (6 bytes): [0x00, 0x04, 0x00, 0x00, 0x80, 0x3f]
        //   - 0x00: id=0 (postcard varint)
        //   - 0x04: command discriminant 4 (postcard varint)
        //   - 0x00, 0x00, 0x80, 0x3f: f32 1.0 in little-endian (0x3f800000)
        // COBS encoding (zeros at positions 0, 2, 3 of the 6-byte payload):
        //   Group 1: 0 non-zero bytes before pos-0 zero → overhead 0x01
        //   Group 2: 1 non-zero byte (0x04) before pos-2 zero → overhead 0x02, data [0x04]
        //   Group 3: 0 non-zero bytes before pos-3 zero → overhead 0x01
        //   Group 4: 2 non-zero bytes (0x80, 0x3f) to end → overhead 0x03, data [0x80, 0x3f]
        //   Frame delimiter: 0x00
        //   Result: [0x01, 0x02, 0x04, 0x01, 0x03, 0x80, 0x3f, 0x00]
        let expected: &[u8] = &[0x01, 0x02, 0x04, 0x01, 0x03, 0x80, 0x3f, 0x00];
        assert_eq!(
            encoded, expected,
            "SetVadThreshold golden bytes mismatch — discriminant or f32 encoding changed"
        );
    }

    /// `SetVadHangover` round-trip: postcard encode → decode equality.
    #[test]
    fn set_vad_hangover_roundtrip() {
        let req = Request {
            id: 42,
            command: Command::SetVadHangover { hangover_ms: 3000 },
        };
        let mut buf = [0u8; 64];
        let len = framing::encode_request(&req, &mut buf).expect("encode failed");
        let decoded = decode_one_cobs::<Request>(&buf[..len]);
        assert_eq!(decoded.id, 42);
        match decoded.command {
            Command::SetVadHangover { hangover_ms } => assert_eq!(hangover_ms, 3000),
            other => panic!("expected SetVadHangover; got {other:?}"),
        }
    }

    /// Golden-byte test for `SetVadHangover { hangover_ms: 3000 }` (discriminant 5).
    ///
    /// postcard serializes `u32` as an LEB128 varint. 3000 = 0b101110111000, so the
    /// low 7 bits (0x38) get a continuation bit → 0xb8, and the next 7 bits (0x17)
    /// terminate → varint [0xb8, 0x17]. With id=0 (varint 0x00) and Command
    /// discriminant 5 (varint 0x05), the raw postcard is [0x00, 0x05, 0xb8, 0x17].
    ///
    /// COBS encoding (one zero at position 0 of the 4-byte payload):
    ///   Group 1: 0 non-zero bytes before pos-0 zero → overhead 0x01
    ///   Group 2: 3 non-zero bytes (0x05, 0xb8, 0x17) to end → overhead 0x04, data
    ///   Frame delimiter: 0x00
    ///   Result: [0x01, 0x04, 0x05, 0xb8, 0x17, 0x00]
    #[test]
    fn set_vad_hangover_golden_bytes() {
        let req = Request {
            id: 0,
            command: Command::SetVadHangover { hangover_ms: 3000 },
        };
        let mut buf = [0u8; 64];
        let len = framing::encode_request(&req, &mut buf).expect("encode failed");
        let encoded = &buf[..len];

        // Verify round-trip first (always meaningful).
        let decoded = decode_one_cobs::<Request>(encoded);
        assert_eq!(decoded.id, 0);
        match decoded.command {
            Command::SetVadHangover { hangover_ms } => assert_eq!(hangover_ms, 3000),
            other => panic!("golden: expected SetVadHangover; got {other:?}"),
        }

        let expected: &[u8] = &[0x01, 0x04, 0x05, 0xb8, 0x17, 0x00];
        assert_eq!(
            encoded, expected,
            "SetVadHangover golden bytes mismatch — discriminant or u32 varint encoding changed"
        );
    }

    /// Exhaustiveness + containment guard: every `TestName` variant must appear in
    /// `REGISTERED_TESTS`, and `REGISTERED_TESTS` must fit within `MAX_TESTS`.
    ///
    /// This test is the **single tripwire** for the single-source-of-truth invariant:
    ///
    /// 1. The `all_test_name_variants!` macro call below is the *single* list of
    ///    variant names. It expands to both an exhaustive `match` (the compile-time
    ///    tripwire when a variant is added or removed) and a `Vec` built from the same
    ///    token list. There is no second hand-maintained list to drift.
    ///
    /// 2. The `contains` assertion then verifies every variant in that Vec is also in
    ///    `REGISTERED_TESTS`. A newly added variant forces a compile error (the match
    ///    is non-exhaustive) until the developer adds it to the macro call; then a
    ///    runtime failure until they also add it to `REGISTERED_TESTS`.
    ///
    /// Fires under `cargo test` (i.e. `make check-host` / CI), not on a plain `cargo build`.
    /// The device-side exhaustive `run_handler` dispatch match catches enum-without-handler
    /// at firmware-build time.
    #[test]
    fn registered_tests_covers_all_variants() {
        // Macro that takes a comma-separated list of TestName variant *names* (no path prefix)
        // and expands to:
        //   (a) an exhaustive `match` over a dummy value — fails to compile if a variant is
        //       added/removed from TestName without updating this call, and
        //   (b) a Vec<TestName> built from the same token list.
        //
        // Because both the match arms and the Vec pushes derive from the same $variant tokens,
        // there is only one list to maintain. Update by adding/removing a name here.
        macro_rules! all_test_name_variants {
            ($($variant:ident),* $(,)?) => {{
                // (a) exhaustive match — compiler tripwire
                let _check = |dummy: &TestName| match dummy {
                    $(TestName::$variant => {},)*
                };
                // (b) build Vec from the same token list
                vec![$(TestName::$variant,)*]
            }};
        }
        // Single source of all variants — update this list when TestName gains a variant.
        let all_variants: Vec<TestName> = all_test_name_variants!(
            Ping,
            Identify,
            GpioSelfTest,
            DeviceHealthCheck,
            I2cBusScan,
            Xvf3800RegRead,
            Xvf3800DoAPlausibility,
            I2sWaveformSanity,
            WifiAssociate,
            UdpRoundtrip,
            TcpRoundtrip,
            TlsReachability,
            WifiScan,
            Xvf3800SpEnergy,
            WifiReassociation,
            GatewayProbeGate,
            TcpInboundFrames,
            TcpSendBackpressure,
            SpeakerOutput,
            AmpAlwaysOnGpoInert,
            CapturePeriodicLine,
            PlaybackDrainRate,
            PollReadinessBidir,
            FullDuplexRxIntegrity,
            StreamRealtimeDuplex,
            PsramIdentity,
            WifiPowerSaveCheck,
            TcpInboundBackpressure,
            TlsPskHandshake,
            TlsPskWrongKeyRejected,
        );

        // Step 2: every variant must be in REGISTERED_TESTS.
        for v in &all_variants {
            assert!(
                REGISTERED_TESTS.contains(v),
                "TestName variant {v:?} is missing from REGISTERED_TESTS — add it to the \
                 REGISTERED_TESTS const in device-protocol/src/lib.rs"
            );
        }
        // The variant count must also equal REGISTERED_TESTS.len() — catches the case where
        // a variant is removed from the enum without removing it from REGISTERED_TESTS.
        assert_eq!(
            all_variants.len(),
            REGISTERED_TESTS.len(),
            "variant count ({}) != REGISTERED_TESTS.len() ({}); \
             update both the exhaustive match and REGISTERED_TESTS",
            all_variants.len(),
            REGISTERED_TESTS.len()
        );

        // Step 3: no duplicates in REGISTERED_TESTS.
        for (i, a) in REGISTERED_TESTS.iter().enumerate() {
            for b in &REGISTERED_TESTS[i + 1..] {
                assert_ne!(a, b, "REGISTERED_TESTS contains duplicate entry {a:?}");
            }
        }

        // Step 4: REGISTERED_TESTS fits in MAX_TESTS (the wire-vector capacity).
        assert!(
            REGISTERED_TESTS.len() <= MAX_TESTS,
            "REGISTERED_TESTS.len() ({}) > MAX_TESTS ({MAX_TESTS}); \
             increase MAX_TESTS",
            REGISTERED_TESTS.len()
        );
    }

    /// Wire-discriminant stability guard: pins the postcard byte for every `TestName`
    /// variant. Renumbering a variant silently breaks wire compatibility with any device
    /// running old firmware.
    ///
    /// This test uses `test_name_discriminant` from `device-protocol` (derived via
    /// `postcard::to_slice`, not a hand table), so it also exercises the derived helper.
    /// Extends the former `hil-host` guard to include discriminant 16 (`TcpInboundFrames`),
    /// which was missing from the previous full guard.
    #[test]
    fn discriminant_values_are_stable() {
        use super::test_name_discriminant;
        assert_eq!(
            test_name_discriminant(&TestName::Ping),
            0,
            "Ping discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::Identify),
            1,
            "Identify discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::GpioSelfTest),
            2,
            "GpioSelfTest discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::DeviceHealthCheck),
            3,
            "DeviceHealthCheck discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::I2cBusScan),
            4,
            "I2cBusScan discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::Xvf3800RegRead),
            5,
            "Xvf3800RegRead discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::Xvf3800DoAPlausibility),
            6,
            "Xvf3800DoAPlausibility discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::I2sWaveformSanity),
            7,
            "I2sWaveformSanity discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::WifiAssociate),
            8,
            "WifiAssociate discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::UdpRoundtrip),
            9,
            "UdpRoundtrip discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::TcpRoundtrip),
            10,
            "TcpRoundtrip discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::TlsReachability),
            11,
            "TlsReachability discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::WifiScan),
            12,
            "WifiScan discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::Xvf3800SpEnergy),
            13,
            "Xvf3800SpEnergy discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::WifiReassociation),
            14,
            "WifiReassociation discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::GatewayProbeGate),
            15,
            "GatewayProbeGate discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::TcpInboundFrames),
            16,
            "TcpInboundFrames discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::TcpSendBackpressure),
            17,
            "TcpSendBackpressure discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::SpeakerOutput),
            18,
            "SpeakerOutput discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::AmpAlwaysOnGpoInert),
            19,
            "AmpAlwaysOnGpoInert discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::CapturePeriodicLine),
            20,
            "CapturePeriodicLine discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::PlaybackDrainRate),
            21,
            "PlaybackDrainRate discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::PollReadinessBidir),
            22,
            "PollReadinessBidir discriminant changed"
        );
        assert_eq!(
            test_name_discriminant(&TestName::FullDuplexRxIntegrity),
            23,
            "FullDuplexRxIntegrity discriminant changed"
        );
    }

    /// `Command::ProvisionWifi` golden bytes and roundtrip.
    ///
    /// Postcard layout (id=0): `[id-varint=0x00, cmd-discriminant=0x01,
    /// ssid-len-varint, ssid-bytes, pass-len-varint, pass-bytes]`.
    /// COBS encodes any embedded zeros. This test pins the exact bytes for a
    /// representative SSID/passphrase pair to catch discriminant or layout regressions.
    #[test]
    fn provision_wifi_golden_roundtrip() {
        let mut ssid = heapless::String::<32>::new();
        ssid.push_str("TestNet").unwrap();
        let mut passphrase = heapless::String::<64>::new();
        passphrase.push_str("s3cr3t!").unwrap();
        let req = Request {
            id: 0,
            command: Command::ProvisionWifi { ssid, passphrase },
        };
        let mut buf = [0u8; 128];
        let len = framing::encode_request(&req, &mut buf).expect("encode failed");
        // Verify round-trip
        assert_eq!(decode_one_cobs::<Request>(&buf[..len]), req);
        // Pin the golden bytes (derived from actual encoder output).
        // COBS layout: overhead byte 0x01 points to next zero at position 1 (the 0x00 id),
        // then 0x12 (18) points 18 positions ahead to the frame delimiter.
        let expected: &[u8] = &[
            0x01, 0x12, 0x01, 0x07, b'T', b'e', b's', b't', b'N', b'e', b't', 0x07, b's', b'3',
            b'c', b'r', b'3', b't', b'!', 0x00,
        ];
        assert_eq!(
            &buf[..len],
            expected,
            "ProvisionWifi golden bytes mismatch — discriminant or layout regression"
        );
    }

    /// `Command::ProvisionPeer` golden bytes and roundtrip.
    ///
    /// Verifies discriminant 2 and that all fields (host, udp_port, tcp_port,
    /// tls_host, tls_port, inbound_frames_port, backpressure_port, poll_readiness_port,
    /// rtd_port, tls_psk_port, tls_psk_bad_port) survive a COBS/postcard round-trip
    /// unchanged.
    #[test]
    fn provision_peer_golden_roundtrip() {
        let req = Request {
            id: 0,
            command: Command::ProvisionPeer {
                host: [192, 168, 1, 50],
                udp_port: 9,
                tcp_port: 7,
                tls_host: [1, 1, 1, 1],
                tls_port: 443,
                inbound_frames_port: 17382,
                backpressure_port: 17383,
                poll_readiness_port: 17384,
                rtd_port: 17385,
                tls_psk_port: 17386,
                tls_psk_bad_port: 17387,
            },
        };
        let mut buf = [0u8; 128];
        let len = framing::encode_request(&req, &mut buf).expect("encode failed");
        // Verify round-trip
        assert_eq!(decode_one_cobs::<Request>(&buf[..len]), req);
        // The frame must be well under the 512-byte buffer limit.
        assert!(
            len <= 512,
            "ProvisionPeer frame ({len} bytes) exceeds 512-byte buffer limit"
        );
        // Pin the golden bytes to catch discriminant or field-order regressions.
        // Layout: COBS overhead byte (points past the next embedded 0x00), id=0x00
        // (embedded zero → COBS-encoded), discriminant=0x02, host bytes, udp_port varint,
        // tcp_port varint, tls_host bytes, tls_port varint, inbound_frames_port varint,
        // backpressure_port varint, poll_readiness_port varint, rtd_port varint,
        // tls_psk_port varint, tls_psk_bad_port varint.
        // postcard encodes u16 as a variable-length integer (not raw LE bytes).
        // Generated from a test run: id=0, ProvisionPeer discriminant=2 in postcard,
        // host=[192,168,1,50], udp=9, tcp=7, tls_host=[1,1,1,1], tls_port=443,
        // inbound_frames_port=17382, backpressure_port=17383, poll_readiness_port=17384,
        // rtd_port=17385, tls_psk_port=17386, tls_psk_bad_port=17387.
        // Postcard encodes u16 as a varint; 17382=0x43E6 → 0xE6,0x87,0x01,
        // 17383=0x43E7 → 0xE7,0x87,0x01, 17384=0x43E8 → 0xE8,0x87,0x01,
        // 17385=0x43E9 → 0xE9,0x87,0x01, 17386=0x43EA → 0xEA,0x87,0x01,
        // 17387=0x43EB → 0xEB,0x87,0x01 (COBS leaves non-zero bytes in place).
        // COBS: overhead byte 0x01 points to the embedded zero (id=0x00) at index 1;
        // 0x20 (32) then points 32 bytes ahead to the trailing 0x00 frame delimiter
        // (the rest of the payload has no embedded zeros).
        let expected: &[u8] = &[
            0x01, 0x20, 0x02, 0xc0, 0xa8, 0x01, 0x32, 0x09, 0x07, 0x01, 0x01, 0x01, 0x01, 0xbb,
            0x03, 0xe6, 0x87, 0x01, 0xe7, 0x87, 0x01, 0xe8, 0x87, 0x01, 0xe9, 0x87, 0x01, 0xea,
            0x87, 0x01, 0xeb, 0x87, 0x01, 0x00,
        ];
        assert_eq!(
            &buf[..len],
            expected,
            "ProvisionPeer golden bytes mismatch — discriminant or layout regression\n\
             actual bytes: {buf:02x?}",
        );
    }

    /// `Command::ProvisionAudio` golden bytes and roundtrip.
    ///
    /// Verifies discriminant 3 and that both fields (host, port) survive a
    /// COBS/postcard round-trip unchanged. Pins exact bytes so a discriminant collision
    /// with `ProvisionPeer` (discriminant 2) is detected immediately.
    #[test]
    fn provision_audio_golden_roundtrip() {
        let req = Request {
            id: 0,
            command: Command::ProvisionAudio {
                host: [192, 168, 1, 100],
                port: 7380,
            },
        };
        let mut buf = [0u8; 128];
        let len = framing::encode_request(&req, &mut buf).expect("encode failed");
        // Verify round-trip
        assert_eq!(decode_one_cobs::<Request>(&buf[..len]), req);
        // Frame must fit in the 512-byte buffer limit.
        assert!(
            len <= 512,
            "ProvisionAudio frame ({len} bytes) exceeds 512-byte buffer limit"
        );
        // Pin golden bytes: discriminant 3, host [192,168,1,100], port 7380 (LE: 0xD4, 0x39).
        // Postcard layout (id=0): [0x00, 0x03, 0xc0, 0xa8, 0x01, 0x64, 0xd4, 0x39]
        // COBS: id=0x00 → chain (overhead 0x01 at byte 0, next chain at offset 2 = 0x08).
        let expected: &[u8] = &[0x01, 0x08, 0x03, 0xc0, 0xa8, 0x01, 0x64, 0xd4, 0x39, 0x00];
        assert_eq!(
            &buf[..len],
            expected,
            "ProvisionAudio golden bytes mismatch — discriminant or layout regression"
        );
    }

    /// A truncated struct byte sequence (too few bytes) must hard-error.
    ///
    /// Postcard is a flat binary format; `from_bytes` consumes exactly the bytes
    /// each field needs. Sending fewer bytes than a struct expects (e.g. a `BuildId`
    /// missing its `dirty` byte) produces a `DeserializeUnexpectedEnd` error.
    ///
    /// Note on "extra trailing field" strictness: postcard's `from_bytes` does NOT
    /// error on trailing bytes after a fully-decoded struct — it only consumes as
    /// many bytes as the type requires. Strictness against extra bytes is enforced
    /// at the COBS frame boundary (the accumulator splits on zero delimiters) rather
    /// than by the serde decoder. This test confirms the truncation (too-few-bytes)
    /// direction, which always hard-errors.
    #[test]
    fn struct_truncated_payload_errors() {
        // Serialize a valid BuildId, then truncate the last byte.
        let mut commit = heapless::String::<40>::new();
        commit.push_str("aabbccdd").unwrap();
        let id = BuildId {
            commit,
            dirty: false,
        };
        let mut buf = [0u8; 64];
        let encoded = postcard::to_slice(&id, &mut buf).expect("encode failed");
        let encoded_len = encoded.len();
        assert!(encoded_len > 1, "encoded BuildId must be at least 2 bytes");

        // Drop the last byte → truncated payload.
        let truncated = &encoded[..encoded_len - 1];
        let result = postcard::from_bytes::<BuildId>(truncated);
        assert!(
            result.is_err(),
            "truncated struct payload must produce Err; got: {result:?}"
        );
    }
}

#[cfg(test)]
mod typed_report_tests {
    use super::*;
    use postcard::accumulator::{CobsAccumulator, FeedResult};

    fn roundtrip(frame: &DeviceFrame) -> DeviceFrame {
        let mut buf = [0u8; 1024];
        let len = framing::encode_device_frame(frame, &mut buf).expect("encode failed");
        let mut acc: CobsAccumulator<1024> = CobsAccumulator::new();
        match acc.feed_ref::<DeviceFrame>(&buf[..len]) {
            FeedResult::Success { data, .. } => data,
            _ => panic!("decode failed"),
        }
    }

    fn report(status: Status, detail: &str, data: TestData) -> DeviceFrame {
        let mut d = TestResultMsg::new();
        d.push_str(detail).unwrap();
        DeviceFrame::Response(Response {
            id: 7,
            status,
            payload: Payload::TestReport(TestReport { detail: d, data }),
        })
    }

    /// The wire discriminant each `TestData` variant must serialize with.
    ///
    /// The match is exhaustive with no `_` arm, so adding a variant fails to compile
    /// until its discriminant is written here — and the returned literals are an
    /// independent record that a reorder must contradict. A new arm also requires a
    /// new entry in [`all_test_data_cases`]; `test_data_cases_cover_every_discriminant`
    /// checks that.
    fn canonical_discriminant(d: &TestData) -> u8 {
        match d {
            TestData::None => 0,
            TestData::DeviceHealth { .. } => 1,
            TestData::I2cScan { .. } => 2,
            TestData::Xvf3800RegRead { .. } => 3,
            TestData::Xvf3800Doa { .. } => 4,
            TestData::Xvf3800SpEnergy { .. } => 5,
            TestData::AmpGpoInert { .. } => 6,
            TestData::I2sWaveform { .. } => 7,
            TestData::WifiScan { .. } => 8,
            TestData::WifiAssociate { .. } => 9,
            TestData::UdpEcho { .. } => 10,
            TestData::TcpEcho { .. } => 11,
            TestData::TlsHandshake { .. } => 12,
            TestData::TcpInboundFrames { .. } => 13,
            TestData::TcpSendBackpressure { .. } => 14,
            TestData::PollReadiness { .. } => 15,
            TestData::Rtd { .. } => 16,
            TestData::WifiReassociation { .. } => 17,
            TestData::GatewayProbeGate { .. } => 18,
            TestData::SpeakerOutput { .. } => 19,
            TestData::CapturePeriodicLine { .. } => 20,
            TestData::PlaybackDrainRate { .. } => 21,
            TestData::FullDuplexRxIntegrity { .. } => 22,
            TestData::PsramIdentity { .. } => 23,
            TestData::WifiPowerSaveCheck { .. } => 24,
            TestData::TcpInboundBackpressure { .. } => 25,
            TestData::TlsPskHandshake { .. } => 26,
            TestData::TlsPskRejected { .. } => 27,
        }
    }

    /// Every `TestData` variant must survive a postcard roundtrip with its fields
    /// intact. Adding a variant requires a case here; the pairing with
    /// [`canonical_discriminant`] is enforced by
    /// `test_data_cases_cover_every_discriminant`.
    fn all_test_data_cases() -> [TestData; 28] {
        let mut found = HVec::<u8, I2C_SCAN_MAX_ADDRS>::new();
        for a in 0x08u8..=0x77 {
            found.push(a).unwrap();
        }
        let mut ssids = HVec::<heapless::String<16>, 3>::new();
        for s in ["home", "guest", "iot"] {
            let mut h = heapless::String::<16>::new();
            h.push_str(s).unwrap();
            ssids.push(h).unwrap();
        }
        [
            TestData::None,
            TestData::DeviceHealth {
                heap_free: 123_456,
                min_heap: 100_000,
                stack_hwm: 4096,
                supervisor_hwm: 2048,
                streamer_hwm: 3072,
                writer_anomalies: 0,
                encode_failures: 0,
                tx_write_failures: 0,
            },
            TestData::I2cScan {
                found,
                bus_errors: 0,
            },
            TestData::Xvf3800RegRead {
                status: 0,
                version: [1, 0, 0],
            },
            TestData::Xvf3800Doa {
                status: 0,
                az: [0.0, -1.5, 2.75, 1.0],
            },
            TestData::Xvf3800SpEnergy {
                status: 0,
                sp: [0.0, 1.0, 2.5, 10.0],
            },
            TestData::AmpGpoInert {
                x0d31: 0x00,
                write_status: 0,
            },
            TestData::I2sWaveform {
                min: -32768,
                max: 32767,
                rms: 1234,
                sat_pct: 0,
                samples: 16000,
                ac1: -5,
            },
            TestData::WifiScan {
                aps: 12,
                best_rssi: -42,
                ssids,
            },
            TestData::WifiAssociate {
                ip: [192, 168, 1, 10],
                gateway: [192, 168, 1, 1],
                rssi: -55,
            },
            TestData::UdpEcho {
                bytes: 64,
                peer_ip: [10, 0, 0, 1],
                peer_port: 9000,
            },
            TestData::TcpEcho {
                bytes: 64,
                peer_ip: [10, 0, 0, 1],
                peer_port: 9001,
            },
            TestData::TlsHandshake {
                peer_ip: [1, 1, 1, 1],
                peer_port: 443,
            },
            TestData::TcpInboundFrames {
                inbound_frames: 3,
                peer_ip: [10, 0, 0, 2],
                peer_port: 9002,
            },
            TestData::TcpSendBackpressure {
                a_resumed: true,
                a_rc: 2,
                a_ru: true,
            },
            TestData::PollReadiness {
                pollin: true,
                pollout: true,
                both: true,
                read_bytes: 32,
            },
            TestData::Rtd {
                underruns: 0,
                gap_ms: 0,
                consumed: 500,
            },
            TestData::WifiReassociation {
                reconnected: true,
                ip: [192, 168, 1, 10],
                gateway: [192, 168, 1, 1],
                rssi: -55,
            },
            TestData::GatewayProbeGate {
                blackhole_reachable: false,
                reassociated: true,
                ip: [192, 168, 1, 10],
                gateway: [192, 168, 1, 1],
                rssi: -55,
            },
            TestData::SpeakerOutput {
                freq: 440,
                amp: 8000,
                dur_ms: 500,
                codec_ok: true,
            },
            TestData::CapturePeriodicLine { chunks_fed: 50 },
            TestData::PlaybackDrainRate {
                chunks_fed: 50,
                feed_full: 0,
                feed_ms: 1000,
                tx_wf: 0,
            },
            TestData::FullDuplexRxIntegrity {
                chunks_fed: 50,
                feed_full: 0,
                feed_ms: 1000,
            },
            TestData::PsramIdentity {
                init: true,
                size: 8 * 1024 * 1024,
                spiram_free: 7_000_000,
                malloc_probe: MallocProbe::External,
            },
            TestData::WifiPowerSaveCheck { ps_mode: 0 },
            TestData::TcpInboundBackpressure {
                inbound_frames: 300,
                sink_full_events: 12,
                peer_ip: [10, 0, 0, 3],
                peer_port: 9003,
            },
            TestData::TlsPskHandshake {
                peer_ip: [10, 0, 0, 4],
                peer_port: 17386,
                handshake_ms: 217,
                version: {
                    let mut v = TlsVersionStr::new();
                    v.push_str("TLSv1.2").unwrap();
                    v
                },
                ciphersuite: {
                    let mut c = TlsSuiteStr::new();
                    c.push_str("TLS-ECDHE-PSK-WITH-CHACHA20-POLY1305-SHA256")
                        .unwrap();
                    c
                },
                echo_bytes: 16,
            },
            TestData::TlsPskRejected {
                peer_ip: [10, 0, 0, 4],
                peer_port: 17387,
                reject_ms: 43,
            },
        ]
    }

    #[test]
    fn roundtrip_all_test_data_variants() {
        for data in all_test_data_cases() {
            let frame = report(Status::Ok, "", data);
            assert_eq!(roundtrip(&frame), frame);
        }
    }

    /// Every `TestData` variant's discriminant is pinned against the independent
    /// literals in [`canonical_discriminant`], not against its position in the cases
    /// list. A reorder or mid-enum insertion changes the encoded byte while the
    /// literal stays put, so it fails here.
    #[test]
    fn test_data_discriminants_pinned() {
        let mut buf = [0u8; 256];
        for data in all_test_data_cases() {
            let expected = canonical_discriminant(&data);
            assert!(expected < 128, "single-byte varint assumption broken");
            let bytes = postcard::to_slice(&data, &mut buf).expect("serialize failed");
            assert_eq!(
                bytes[0], expected,
                "TestData discriminant changed for {data:?} — wire regression"
            );
        }
    }

    /// The cases list must exercise every pinned discriminant exactly once. Combined
    /// with the exhaustive match in [`canonical_discriminant`] (which fails to compile
    /// on a new variant), this keeps the roundtrip, discriminant-pin, and
    /// encode-buffer-bound guards covering the whole enum.
    #[test]
    fn test_data_cases_cover_every_discriminant() {
        let cases = all_test_data_cases();
        let mut seen: Vec<u8> = cases.iter().map(canonical_discriminant).collect();
        seen.sort_unstable();
        let expected: Vec<u8> = (0..cases.len() as u8).collect();
        assert_eq!(
            seen, expected,
            "all_test_data_cases must contain each TestData variant exactly once"
        );
    }

    /// NaN azimuth crosses the wire as a real `f32` NaN (no "nan" string parsing).
    #[test]
    fn roundtrip_doa_nan_is_nan() {
        let frame = report(
            Status::Ok,
            "",
            TestData::Xvf3800Doa {
                status: 0,
                az: [f32::NAN, 0.0, f32::INFINITY, -1.0],
            },
        );
        let DeviceFrame::Response(resp) = roundtrip(&frame) else {
            panic!("expected Response");
        };
        let Payload::TestReport(TestReport {
            data: TestData::Xvf3800Doa { az, .. },
            ..
        }) = resp.payload
        else {
            panic!("expected Xvf3800Doa report");
        };
        assert!(az[0].is_nan());
        assert!(az[2].is_infinite() && az[2].is_sign_positive());
    }

    /// Golden bytes for a representative typed report. Pins `Payload::TestReport`'s
    /// discriminant (3) and the full frame layout of one variant. Per-variant
    /// `TestData` numbering is pinned by `test_data_discriminants_pinned`.
    #[test]
    fn test_report_golden_bytes() {
        let frame = report(
            Status::Ok,
            "",
            TestData::Xvf3800RegRead {
                status: 0x00,
                version: [0x01, 0x00, 0x00],
            },
        );
        let mut buf = [0u8; 128];
        let len = framing::encode_device_frame(&frame, &mut buf).expect("encode failed");
        assert_eq!(
            &buf[..len],
            // COBS-framed. Payload bytes before framing: Response(0x00) id=7
            // status=Ok(0x00) payload=TestReport(0x03) detail-len=0
            // TestData::Xvf3800RegRead(0x03) status=0x00 version=01 00 00.
            &[1, 2, 7, 2, 3, 2, 3, 2, 1, 1, 1, 0][..],
            "TestReport golden bytes changed — discriminant or field-order regression"
        );
    }

    /// The worst-case response frame — max-length `detail` paired with the widest
    /// `TestData` — must COBS-encode within the device's `RESPONSE_FRAME_BUF`.
    #[test]
    fn response_frame_fits_encode_buf() {
        let detail = "x".repeat(TEST_RESULT_MSG_CAP);
        let mut buf = [0u8; RESPONSE_FRAME_BUF];
        for data in all_test_data_cases() {
            let frame = report(Status::Fail, &detail, data);
            framing::encode_device_frame(&frame, &mut buf)
                .expect("worst-case response frame exceeds RESPONSE_FRAME_BUF");
        }
    }

    /// Fail reports carry the diagnostic in `detail` with `TestData::None`; overflow
    /// truncates with the sentinel rather than panicking.
    #[test]
    fn fail_report_truncates_with_sentinel() {
        let long = "x".repeat(TEST_RESULT_MSG_CAP * 2);
        let (status, payload) = test_report_fail_fmt(format_args!("{long}"));
        assert_eq!(status, Status::Fail);
        let Payload::TestReport(r) = payload else {
            panic!("expected TestReport");
        };
        assert_eq!(r.data, TestData::None);
        assert!(r.detail.ends_with(TRUNCATION_SENTINEL));
        assert!(r.detail.len() <= TEST_RESULT_MSG_CAP);
    }

    #[test]
    fn fail_data_builder_carries_payload_and_truncates_with_sentinel() {
        let long = "x".repeat(TEST_RESULT_MSG_CAP * 2);
        let (status, payload) = test_report_fail_data(
            TestData::WifiPowerSaveCheck { ps_mode: 2 },
            format_args!("{long}"),
        );
        assert_eq!(status, Status::Fail);
        let Payload::TestReport(r) = payload else {
            panic!("expected TestReport");
        };
        assert_eq!(r.data, TestData::WifiPowerSaveCheck { ps_mode: 2 });
        assert!(r.detail.ends_with(TRUNCATION_SENTINEL));
        assert!(r.detail.len() <= TEST_RESULT_MSG_CAP);
    }

    #[test]
    fn ok_builders_set_status_and_detail() {
        let (status, payload) = test_report_ok(TestData::CapturePeriodicLine { chunks_fed: 3 });
        assert_eq!(status, Status::Ok);
        let Payload::TestReport(r) = payload else {
            panic!("expected TestReport");
        };
        assert!(r.detail.is_empty());

        let (status, payload) =
            test_report_ok_detail(TestData::None, format_args!("gpio {} lines ok", 4));
        assert_eq!(status, Status::Ok);
        let Payload::TestReport(r) = payload else {
            panic!("expected TestReport");
        };
        assert_eq!(r.detail.as_str(), "gpio 4 lines ok");
    }

    #[test]
    fn fail_detail_builder_formats_prefix_and_debug() {
        let (status, payload) = test_report_fail_detail("i2c write", &(3u8, "EIO"));
        assert_eq!(status, Status::Fail);
        let Payload::TestReport(r) = payload else {
            panic!("expected TestReport");
        };
        assert_eq!(r.detail.as_str(), "i2c write: (3, \"EIO\")");
    }

    #[test]
    fn truncate_utf8_prefix_ascii_within_and_at_boundary() {
        assert_eq!(truncate_utf8_prefix("abc", 16), "abc");
        assert_eq!(
            truncate_utf8_prefix("0123456789abcdef", 16),
            "0123456789abcdef"
        );
        assert_eq!(
            truncate_utf8_prefix("0123456789abcdefX", 16),
            "0123456789abcdef"
        );
    }

    #[test]
    fn truncate_utf8_prefix_never_overshoots_on_multibyte_straddle() {
        // 15 ASCII bytes + '€' (3 bytes, starts at idx 15, ends at 18). A char that
        // starts before the boundary but ends past it must be excluded, so the result
        // never exceeds max_bytes.
        let s = "0123456789abcde€";
        let out = truncate_utf8_prefix(s, 16);
        assert!(out.len() <= 16, "overshot: {} bytes", out.len());
        assert_eq!(out, "0123456789abcde");
        // The straddling char fits once the cap admits its full width.
        assert_eq!(truncate_utf8_prefix(s, 18), s);
    }

    #[test]
    fn truncate_utf8_prefix_zero_and_empty() {
        assert_eq!(truncate_utf8_prefix("abc", 0), "");
        assert_eq!(truncate_utf8_prefix("", 16), "");
    }
}
