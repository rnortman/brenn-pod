//! WiFi driver stack, reconnect supervisor, and network-association self-tests.
//!
//! Holds the process-lifetime WiFi stack (`WIFI_STACK`) + event subscriptions,
//! the reconnect supervisor thread, and the HIL WiFi handlers
//! (`run_wifi_scan`/`run_wifi_associate`/`run_wifi_reassociation`/
//! `run_gateway_probe_gate`). Extracted from `main.rs` per design.md §2.1.

use crate::hil::test_report_fail_msg;
use crate::nvs::{nvs_get_blob4, nvs_get_str, open_wifi_nvs};
use device_protocol::{
    log_tokens, test_report_fail, test_report_fail_data, test_report_fail_detail,
    test_report_fail_fmt, test_report_ok, truncate_utf8_prefix, Payload, Status, TestData,
    SSID_TRUNC_BYTES, WIFI_PS_NONE_RAW,
};
use esp_idf_svc::eventloop::{EspSubscription, EspSystemEventLoop, System};
use esp_idf_svc::hal::delay::FreeRtos;
use esp_idf_svc::ipv4::{IpInfo, Ipv4Addr};
use esp_idf_svc::nvs::EspDefaultNvsPartition;
use esp_idf_svc::ping::{Configuration as PingConfiguration, EspPing};
use esp_idf_svc::wifi::{AuthMethod, BlockingWifi, ClientConfiguration, Configuration, EspWifi};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use wifi_diag::{fmt_ipv4, WifiSnapshot};
use wifi_reconnect::{compute_wait_secs, Backoff, GW_UNREACHABLE_THRESHOLD, TICK_INTERVAL_SECS};

/// Process-lifetime sender half of the WiFi supervisor doorbell channel.
///
/// Capacity-1 `sync_channel`: multiple simultaneous wake signals coalesce to one
/// pending ring (the receiver checks actual state on each wake, so coalescing is
/// always correct).  Callers use `try_send(())`, ignoring `Full` — one pending ring
/// is sufficient.
///
/// `None` until `spawn_wifi_supervisor_thread()` is called in `main()`.  Any caller
/// that fires before the supervisor is spawned simply has its ring dropped, which is
/// safe (the supervisor's first action is to try association anyway).
static WIFI_WAKE_TX: Mutex<Option<std::sync::mpsc::SyncSender<()>>> = Mutex::new(None);

/// Process-lifetime WiFi stack. Holds `BlockingWifi<EspWifi<'static>>` and the
/// NVS partition handle so both survive for the entire firmware run. Initialized
/// at boot (before the protocol loop); the WiFi driver does NOT connect at boot —
/// the `WifiAssociate` test handler calls `set_configuration`/`start`/`connect`/
/// `wait_netif_up` so the device boots and serves the protocol even when no
/// credentials are provisioned yet. The NVS partition is kept here (not just in
/// boot init) because provisioning handlers need it at any time.
pub(crate) struct WifiStack {
    pub(crate) wifi: BlockingWifi<EspWifi<'static>>,
    pub(crate) nvs: EspDefaultNvsPartition,
}

pub(crate) static WIFI_STACK: Mutex<Option<WifiStack>> = Mutex::new(None);

/// RAM-only temporary WiFi credentials. When `Some`, the supervisor associates from
/// these instead of NVS. Never written to flash; a reboot always reverts to NVS —
/// that structural fact is the entire safety argument for "trial credentials".
///
/// Locked (and released) before `WIFI_STACK` is taken, exactly where the NVS
/// credential read happens today. Never nests with `WIFI_STACK` or `WRITER`.
pub(crate) struct TempWifiConfig {
    pub(crate) ssid: heapless::String<32>,
    pub(crate) pass: heapless::String<64>,
}

pub(crate) static WIFI_TEMP_CONFIG: Mutex<Option<TempWifiConfig>> = Mutex::new(None);

// ── WiFi event subscriptions (process-lifetime, boot-initialized) ─────────────

/// Holds the two event subscriptions and the retained `EspSystemEventLoop` clone
/// that keeps the underlying singleton loop reachable after `sysloop` is moved into
/// `BlockingWifi`. All three must outlive the program; dropping an
/// `EspSubscription` silently unregisters its callback.
///
/// Populated once at boot (after `WIFI_STACK`), then left alone. No callback ever
/// locks this struct, so there is no lock-ordering concern.
pub(crate) struct WifiEventSubs {
    pub(crate) _wifi_sub: EspSubscription<'static, System>,
    pub(crate) _ip_sub: EspSubscription<'static, System>,
    pub(crate) _sysloop: EspSystemEventLoop,
}

/// Process-lifetime WiFi/IP event subscription handles.
///
/// `None` until the WiFi stack is initialized at boot. Populated exactly once;
/// the `WifiEventSubs` is never accessed after the initial store.
pub(crate) static WIFI_EVENT_SUBS: Mutex<Option<WifiEventSubs>> = Mutex::new(None);

/// True while the supervisor thread is blocked inside `associate_from_active_config()`
/// for an attempt it initiated itself; false otherwise. Written only by the supervisor
/// thread (around its own attempt); read by `ring_wifi_wake_on_disconnect`, which runs
/// on the event-loop callback task.
///
/// Exists because a failed association attempt against an unreachable AP/bogus SSID can
/// itself drive the driver through a `StaDisconnected` transition — the *attempt's own*
/// teardown, not news about a previously-up link. Ringing the doorbell for that event
/// would let the pending ring bypass the backoff wait the supervisor is about to compute
/// on its next loop iteration (`recv_timeout` returns immediately on any pending ring),
/// collapsing "wait ~30s+" to "back-to-back with the attempt that just failed". The flag
/// scopes the ring-suppression to exactly the attempt's own blocking window, so a
/// `StaDisconnected` for a link that was genuinely up (the doorbell-while-up path) is
/// unaffected and still rings.
///
/// TODO(wifi-assoc-inflight-flag-generation-race): this suppression window is timing-
/// based, not state-based, and has a residual race the store/clear pair does not close.
/// `store(true)`/`store(false)` bracket the *call* to `associate_from_active_config()`,
/// but the failing attempt's `StaDisconnected` event is delivered asynchronously on the
/// event-loop task; nothing here guarantees the callback (`main.rs`'s
/// `WifiEvent::StaDisconnected` handler) has actually run by the time `store(false)`
/// executes — `BlockingWifi::connect()`/`stop()` wait on driver *state*, not on every
/// other event subscriber having been dispatched. If the callback runs after
/// `store(false)`, it sees the flag false and rings anyway, bypassing the backoff wait
/// that was just computed for the next attempt (reintroducing the ~17.4s flake this flag
/// exists to prevent, at low probability). A state-based fix (e.g. an attempt-generation
/// counter stamped by the supervisor and checked by the callback, so a ring is only
/// suppressed for the exact attempt that produced it) would close this properly; not
/// done here — see TODO.md.
static WIFI_ASSOC_IN_FLIGHT: AtomicBool = AtomicBool::new(false);

/// Ring the doorbell for a `StaDisconnected` event, unless the disconnect is an artifact
/// of an association attempt the supervisor itself currently has in flight (see
/// [`WIFI_ASSOC_IN_FLIGHT`]) — in which case the ring is suppressed. The supervisor is
/// already awake inside the attempt and will re-evaluate state on its own the moment the
/// attempt returns; honoring a self-inflicted ring here would only let that attempt
/// cancel the backoff wait it is about to compute for the *next* attempt.
pub(crate) fn ring_wifi_wake_on_disconnect() {
    if WIFI_ASSOC_IN_FLIGHT.load(Ordering::Acquire) {
        log::debug!(
            "wifi-supervisor: StaDisconnected during in-flight attempt — doorbell ring suppressed"
        );
        return;
    }
    ring_wifi_wake();
}

// ── WiFi supervisor thread ────────────────────────────────────────────────────

/// Monotonic seconds since boot (`esp_timer_get_time` µs → s).
///
/// Shared by the WiFi supervisor and streamer so their clocks cannot drift.
pub(crate) fn monotonic_secs() -> u64 {
    (unsafe { esp_idf_svc::sys::esp_timer_get_time() } as u64) / 1_000_000
}

/// Per-thread jitter seed for reconnect backoff de-sync (XOR'd with an attempt
/// counter at each draw). Hardware RNG; no cryptographic quality required or implied.
pub(crate) fn jitter_seed() -> u32 {
    // SAFETY: esp_random takes no arguments, has no failure mode, and is callable
    // from any task; before RF is enabled it degrades to pseudo-random, which is
    // fine for de-sync jitter.
    unsafe { esp_idf_svc::sys::esp_random() }
}

/// Spawn the WiFi supervisor thread (8 KiB stack).
///
/// Sole owner of the reconnect loop: the only runtime caller of
/// `associate_from_active_config()` (besides the HIL `WifiAssociate` handler,
/// serialized via `WIFI_STACK`).
///
/// Wake sources (capacity-1 doorbell channel):
/// 1. `StaDisconnected` event callback.
/// 2. `handle_provision_wifi` after a successful NVS write.
///
/// A 30 s health tick (`recv_timeout`) is the backstop for losses that produce no
/// disconnect event. On each tick and each doorbell-while-up wake, the gateway is
/// probed via ICMP; sustained unreachability triggers force-reassociation.
///
/// Precondition: `WIFI_STACK` and `WIFI_EVENT_SUBS` are initialized.
pub(crate) fn spawn_wifi_supervisor_thread() {
    let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);

    // Populate the process-lifetime sender so callbacks and the streamer can ring it.
    {
        let mut guard = WIFI_WAKE_TX
            .lock()
            .unwrap_or_else(|_| panic!("WIFI_WAKE_TX mutex poisoned during init"));
        *guard = Some(tx);
    }

    // Kick the supervisor so it performs the first association immediately.
    ring_wifi_wake();

    // ESP-IDF's std::thread::Builder::name() does NOT propagate to the FreeRTOS
    // task name (the espidf target's set_name is a no-op). Without the workaround
    // below, xTaskGetHandle(c"wifi-supervisor") returns NULL and the health-check
    // HWM gate reports supervisor_hwm=0.
    //
    // Workaround: set esp_pthread_set_cfg(thread_name) before spawn, then restore
    // to NULL afterward. The cfg is in the *calling* task's TLS, so the restore
    // prevents later spawns from inheriting "wifi-supervisor".
    //
    // SAFETY: esp_pthread_set_cfg deep-copies the cfg; the 'static C string is
    // valid for the spawn duration. A failed spawn panics (unrecoverable).
    // Note: the TLS restore runs after the spawn's `.expect()`. Under the abort
    // panic policy a spawn failure aborts, so the restore being skipped has no
    // reachable consequence. If panic="unwind" is ever adopted, use a scopeguard
    // to restore the TLS unconditionally.
    {
        let mut cfg = unsafe { esp_idf_svc::sys::esp_pthread_get_default_config() };
        // 15 chars = CONFIG_FREERTOS_MAX_TASK_NAME_LEN - 1 (NUL). Do not lengthen.
        cfg.thread_name = c"wifi-supervisor".as_ptr();
        let set_rc = unsafe { esp_idf_svc::sys::esp_pthread_set_cfg(&cfg) };
        if set_rc != esp_idf_svc::sys::ESP_OK {
            log::warn!(
                "wifi-supervisor: esp_pthread_set_cfg failed (rc={set_rc:#x}) — task name will be 'pthread', DeviceHealthCheck will report supervisor_hwm=0"
            );
        }

        let _handle = std::thread::Builder::new()
            .name("wifi-supervisor".into())
            .stack_size(8192)
            .spawn(move || {
            log::info!("{}", log_tokens::WIFI_SUPERVISOR_STARTED);

            let mut backoff = Backoff::new();
            // Consecutive gateway-unreachable probes; force-reassociate at threshold.
            let mut consecutive_gw_unreachable: u32 = 0;
            let now_secs = monotonic_secs;
            let mut last_attempt_secs: u64 = now_secs();
            let jitter_seed_base: u32 = jitter_seed();
            let mut attempt_counter: u32 = 0;

            loop {
                // ── Compute wait and block ───────────────────────────────────────
                let jittered_backoff =
                    backoff.next_wait_secs(jitter_seed_base ^ attempt_counter);
                let wait_secs =
                    compute_wait_secs(now_secs(), last_attempt_secs, jittered_backoff, TICK_INTERVAL_SECS);
                let wait = std::time::Duration::from_secs(wait_secs);

                let doorbell_rang = match rx.recv_timeout(wait) {
                    Ok(()) => true,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => false,
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                        // All senders dropped — impossible since WIFI_WAKE_TX holds
                        // one for the process lifetime.  Log and continue rather than
                        // exit (exiting would re-introduce the original bug).
                        log::error!("wifi-supervisor: doorbell channel disconnected — impossible; continuing");
                        false
                    }
                };

                // ── State check ─────────────────────────────────────────────────
                let is_up = wifi_is_up_nonblocking();

                match (is_up, doorbell_rang) {
                    (Some(true), false) => {
                        // Healthy tick — probe gateway to detect stuck-but-associated links.
                        let probe = probe_gateway_reachable();
                        match probe {
                            GatewayProbe::Reachable | GatewayProbe::Indeterminate => {
                                backoff.record_success();
                                consecutive_gw_unreachable = 0;
                                // Advance tick clock so compute_wait_secs derives a fresh deadline.
                                last_attempt_secs = now_secs();
                                continue;
                            }
                            GatewayProbe::Unreachable => {
                                consecutive_gw_unreachable =
                                    consecutive_gw_unreachable.saturating_add(1);
                                if consecutive_gw_unreachable < GW_UNREACHABLE_THRESHOLD {
                                    log::warn!(
                                        "wifi-supervisor: gateway probe failed on tick ({}/{}) — deferring force-reassociate",
                                        consecutive_gw_unreachable,
                                        GW_UNREACHABLE_THRESHOLD
                                    );
                                    // Advance the tick clock so we don't busy-spin.
                                    last_attempt_secs = now_secs();
                                    continue;
                                }
                                // Threshold reached — fall through to force-reassociate.
                                log::warn!(
                                    "wifi-supervisor: gateway unreachable for {} consecutive tick probes — forcing re-associate",
                                    GW_UNREACHABLE_THRESHOLD
                                );
                                consecutive_gw_unreachable = 0;
                                // Best-effort disconnect/stop before re-associate.
                                // Return value ignored: this arm falls through to
                                // associate regardless, and the None case is logged
                                // inside the helper.
                                let _ = force_disconnect_wifi();
                                // Fall through to associate below.
                            }
                        }
                    }
                    (Some(true), true) => {
                        // Doorbell while up: either a self-recovered StaDisconnected, a
                        // provisioning signal, or a credential clear. A clear can race an
                        // in-flight association (credentials are read before the
                        // WIFI_STACK lock is taken), leaving the link up on credentials
                        // NVS no longer holds. Re-check NVS first so that state cannot
                        // survive the doorbell.
                        if !wifi_credentials_present() {
                            log::info!(
                                "{} (provision to connect)",
                                log_tokens::WIFI_PARKED_NO_CREDS
                            );
                            let _ = force_disconnect_wifi();
                            continue;
                        }
                        // Probe gateway to corroborate link health.
                        let probe = probe_gateway_reachable();
                        match probe {
                            GatewayProbe::Reachable | GatewayProbe::Indeterminate => {
                                consecutive_gw_unreachable = 0;
                                // No record_success() here — only the steady-state tick arm
                                // is the authoritative success signal for backoff reset.
                                // No last_attempt_secs advance — the channel is drained after
                                // a doorbell, so recv_timeout naturally picks up the tick cadence.
                                log::info!(
                                    "wifi-supervisor: doorbell while up — gateway reachable (probe={:?}); ignoring app-level wake",
                                    probe
                                );
                                continue;
                            }
                            GatewayProbe::Unreachable => {
                                consecutive_gw_unreachable =
                                    consecutive_gw_unreachable.saturating_add(1);
                                if consecutive_gw_unreachable < GW_UNREACHABLE_THRESHOLD {
                                    log::warn!(
                                        "wifi-supervisor: gateway probe failed on doorbell-while-up ({}/{}) — deferring force-reassociate",
                                        consecutive_gw_unreachable,
                                        GW_UNREACHABLE_THRESHOLD
                                    );
                                    // Advance tick clock to prevent busy-spin.
                                    last_attempt_secs = now_secs();
                                    continue;
                                }
                                // Threshold reached — fall through to force-reassociate.
                                log::warn!(
                                    "wifi-supervisor: gateway unreachable for {} consecutive probes (doorbell-while-up) — forcing re-associate",
                                    GW_UNREACHABLE_THRESHOLD
                                );
                                consecutive_gw_unreachable = 0;
                                // Best-effort disconnect/stop before re-associate.
                                // Return value ignored: this arm falls through to
                                // associate regardless, and the None case is logged
                                // inside the helper.
                                let _ = force_disconnect_wifi();
                                // Fall through to associate below.
                            }
                        }
                    }
                    (Some(false), _) | (None, _) => {
                        // Link is down (or lock was busy — treat conservatively as down).
                        // Fall through to associate.
                    }
                }

                // ── Associate ───────────────────────────────────────────────────
                last_attempt_secs = now_secs();
                attempt_counter = attempt_counter.wrapping_add(1);
                log::info!(
                    "{} ({})",
                    log_tokens::WIFI_REASSOC_ATTEMPT_START,
                    attempt_counter
                );

                WIFI_ASSOC_IN_FLIGHT.store(true, Ordering::Release);
                let attempt_result = associate_from_active_config();
                WIFI_ASSOC_IN_FLIGHT.store(false, Ordering::Release);

                match attempt_result {
                    Ok((ip, gw, rssi)) => {
                        log::info!(
                            "{} ip={} gw={} rssi={}",
                            log_tokens::WIFI_REASSOCIATED,
                            fmt_ipv4(ip),
                            fmt_ipv4(gw),
                            rssi
                        );
                        backoff.record_success();
                        consecutive_gw_unreachable = 0;
                    }
                    Err(msg) if msg.as_str().contains(log_tokens::NO_NVS_CREDENTIALS) => {
                        // Unprovisioned — park until credentials arrive via doorbell.
                        log::info!("{} (provision to connect)", log_tokens::WIFI_PARKED_NO_CREDS);
                        // Not an RF failure; don't charge backoff.
                    }
                    Err(msg) => {
                        log::warn!(
                            "{} ({}): {}",
                            log_tokens::WIFI_REASSOC_ATTEMPT_FAILED,
                            attempt_counter,
                            msg.as_str()
                        );
                        backoff.record_failure();
                        if backoff.is_slow_lane() {
                            log::warn!(
                                "{} (n={}) — check credentials/AP",
                                log_tokens::WIFI_CONSECUTIVE_FAILURES,
                                backoff.consecutive_failures()
                            );
                        }
                    }
                }
            }
        })
        .expect("wifi-supervisor: thread spawn failed — heap exhausted?");

        // Restore main's TLS thread_name to NULL (see workaround comment above).
        cfg.thread_name = core::ptr::null();
        let restore_rc = unsafe { esp_idf_svc::sys::esp_pthread_set_cfg(&cfg) };
        if restore_rc != esp_idf_svc::sys::ESP_OK {
            log::warn!(
                "wifi-supervisor: esp_pthread_set_cfg restore failed (rc={restore_rc:#x}) — subsequent thread spawns from main may inherit task name 'wifi-supervisor'"
            );
        }
    }
}

/// Start the WiFi radio if not already started. Idempotent.
///
/// Also forces `WIFI_PS_NONE` after every start: the ESP-IDF default
/// (`WIFI_PS_MIN_MODEM`) lets the radio doze between DTIMs whenever uplink
/// goes quiet, which loses/delays downlink playback packets by 300–900 ms
/// and latches the streaming path into an RTO-paced burst/gap regime — see
/// docs/adr/2026/07/01-host-to-device-dropout/root-cause-analysis.md for the
/// full analysis. The pod is mains-powered; PS_NONE is the intended
/// production setting.
fn ensure_wifi_started(
    wifi: &mut BlockingWifi<EspWifi<'static>>,
) -> Result<(), esp_idf_svc::sys::EspError> {
    if !wifi.is_started()? {
        wifi.start()?;
    }
    // Re-applied on every start path (incl. supervisor stop/start cycles) so the
    // setting can never silently revert. Failure is non-fatal: warn and continue.
    let rc =
        unsafe { esp_idf_svc::sys::esp_wifi_set_ps(esp_idf_svc::sys::wifi_ps_type_t_WIFI_PS_NONE) };
    if rc != esp_idf_svc::sys::ESP_OK {
        log::warn!(
            "wifi: esp_wifi_set_ps(WIFI_PS_NONE) failed (rc={rc:#x}) — radio stays in default modem power save; expect downlink playback dropouts"
        );
    } else {
        log::info!("wifi: modem power save disabled (PS_NONE)");
    }
    Ok(())
}

/// WiFi radio + AP scan self-test (no credentials required).
///
/// Starts the radio, scans for APs, and asserts at least one is found. Reports
/// AP count, best RSSI, and up to 3 SSIDs.
pub(crate) fn run_wifi_scan() -> (Status, Payload) {
    let mut guard = WIFI_STACK
        .lock()
        .unwrap_or_else(|_| panic!("WIFI_STACK mutex poisoned"));
    let stack = guard
        .as_mut()
        .expect("WIFI_STACK is None — not initialized at boot");

    if let Err(e) = ensure_wifi_started(&mut stack.wifi) {
        return test_report_fail_detail("wifi start failed", &e);
    }

    let aps = match stack.wifi.scan() {
        Ok(list) => list,
        Err(e) => return test_report_fail_detail("wifi scan failed", &e),
    };

    let ap_count = aps.len();
    if ap_count == 0 {
        return test_report_fail("scan found 0 APs — radio up but nothing heard");
    }

    let best_rssi = aps.iter().map(|ap| ap.signal_strength).max().unwrap_or(0);

    // Up to 3 SSIDs, each truncated to SSID_TRUNC_BYTES on a char boundary.
    let mut ssids: heapless::Vec<heapless::String<SSID_TRUNC_BYTES>, 3> = heapless::Vec::new();
    for ap in aps.iter().take(3) {
        let mut trunc = heapless::String::<SSID_TRUNC_BYTES>::new();
        let _ = trunc.push_str(truncate_utf8_prefix(ap.ssid.as_str(), SSID_TRUNC_BYTES));
        let _ = ssids.push(trunc);
    }

    log::info!("wifi_scan: aps={ap_count} best_rssi={best_rssi} ssids={ssids:?}");
    test_report_ok(TestData::WifiScan {
        aps: ap_count as u32,
        best_rssi: best_rssi.into(),
        ssids,
    })
}

/// Read the current WiFi modem power-save mode via `esp_wifi_get_ps`.
///
/// Returns `Some(raw wifi_ps_type_t)` on `ESP_OK`, else `None`. The out-param is
/// initialized to a non-`PS_NONE` sentinel (`MAX_MODEM`) so an FFI that returns OK
/// without writing can never masquerade as `WIFI_PS_NONE`. The call does not log, so
/// it is safe to invoke under the `WIFI_STACK` guard without the `WIFI_STACK` ↔
/// `WRITER` lock-inversion concern.
fn read_ps_mode() -> Option<u32> {
    let mut mode: esp_idf_svc::sys::wifi_ps_type_t =
        esp_idf_svc::sys::wifi_ps_type_t_WIFI_PS_MAX_MODEM;
    let rc = unsafe { esp_idf_svc::sys::esp_wifi_get_ps(&mut mode) };
    if rc == esp_idf_svc::sys::ESP_OK {
        Some(mode as u32)
    } else {
        None
    }
}

// The wire-shared `WIFI_PS_NONE_RAW` must equal the device-only ESP-IDF sys constant it
// stands in for; catch any future renumber at compile time (a `debug_assert` would be
// compiled out of release firmware, letting a renumbered PS-on-to-0 mode pass silently).
const _: () = assert!(
    esp_idf_svc::sys::wifi_ps_type_t_WIFI_PS_NONE == WIFI_PS_NONE_RAW,
    "WIFI_PS_NONE_RAW drifted from esp_idf_svc WIFI_PS_NONE"
);

/// WiFi modem power-save identity self-test (no credentials required).
///
/// Starts the radio (the path that forces `WIFI_PS_NONE`), reads the power-save mode
/// back, and asserts it is `WIFI_PS_NONE`. The raw mode is reported on both pass and
/// mismatch. Written assert-first: a non-`NONE` reading FAILs loudly with the raw
/// value for human review, per bring-up doctrine.
pub(crate) fn run_wifi_power_save_check() -> (Status, Payload) {
    let mut guard = WIFI_STACK
        .lock()
        .unwrap_or_else(|_| panic!("WIFI_STACK mutex poisoned"));
    let stack = guard
        .as_mut()
        .expect("WIFI_STACK is None — not initialized at boot");

    if let Err(e) = ensure_wifi_started(&mut stack.wifi) {
        return test_report_fail_detail("wifi start failed", &e);
    }

    let mode = match read_ps_mode() {
        Some(m) => m,
        None => return test_report_fail("esp_wifi_get_ps failed — cannot read power-save mode"),
    };

    if mode == WIFI_PS_NONE_RAW {
        log::info!("wifi_power_save_check: ps_mode={mode} (WIFI_PS_NONE)");
        test_report_ok(TestData::WifiPowerSaveCheck { ps_mode: mode })
    } else {
        test_report_fail_data(
            TestData::WifiPowerSaveCheck { ps_mode: mode },
            format_args!(
                "FAIL power save on: ps_mode={mode} (expected 0=WIFI_PS_NONE; 1=MIN_MODEM 2=MAX_MODEM)"
            ),
        )
    }
}

/// Non-blocking snapshot of WiFi state via `try_lock`.
///
/// Returns all-`None` fields if `WIFI_STACK` is busy. Does not log while
/// holding the guard to avoid `WIFI_STACK` ↔ `WRITER` lock inversion.
pub(crate) fn snapshot_wifi_state() -> WifiSnapshot {
    let guard = match WIFI_STACK.try_lock() {
        Ok(g) => g,
        Err(_) => {
            return WifiSnapshot {
                up: None,
                ip: None,
                gw: None,
                rssi: None,
                ps_mode: None,
            }
        }
    };

    let Some(stack) = guard.as_ref() else {
        return WifiSnapshot {
            up: None,
            ip: None,
            gw: None,
            rssi: None,
            ps_mode: None,
        };
    };

    let up = stack.wifi.is_up().ok();
    let (ip, gw) = match stack.wifi.wifi().sta_netif().get_ip_info() {
        Ok(info) => (Some(info.ip.octets()), Some(info.subnet.gateway.octets())),
        Err(_) => (None, None),
    };
    let rssi = stack.wifi.wifi().get_rssi().ok();
    let ps_mode = read_ps_mode();

    WifiSnapshot {
        up,
        ip,
        gw,
        rssi,
        ps_mode,
    }
}

/// Non-blocking WiFi up check via `try_lock`. Returns `None` if the lock is busy.
pub(crate) fn wifi_is_up_nonblocking() -> Option<bool> {
    let guard = WIFI_STACK.try_lock().ok()?;
    let stack = guard.as_ref()?;
    stack.wifi.is_up().ok()
}

/// Result of a gateway ICMP-probe attempt.
///
/// - `Reachable` — at least one echo reply received.
/// - `Unreachable` — zero replies; the gateway did not answer.
/// - `Indeterminate` — probe could not run (lock busy, no lease, ping-stack error).
///   Never treated as `Unreachable`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GatewayProbe {
    Reachable,
    Unreachable,
    Indeterminate,
}

/// Send 3 ICMP pings to `target` on the given netif and classify the result.
///
/// Caller must NOT hold the `WIFI_STACK` guard — `EspPing` blocks the thread
/// for the full session, which would deadlock concurrent `WIFI_STACK` users.
///
/// `received >= 1 of 3` → `Reachable` (tolerates one lost reply). The 1 s
/// per-ping timeout absorbs modem-sleep DTIM jitter (~100–300 ms).
fn ping_reachable(target: Ipv4Addr, netif_index: u32) -> GatewayProbe {
    let conf = PingConfiguration {
        count: 3,
        interval: std::time::Duration::from_millis(500),
        timeout: std::time::Duration::from_secs(1),
        data_size: 32,
        tos: 0,
    };
    let mut ping = EspPing::new(netif_index);
    match ping.ping(target, &conf) {
        Ok(summary) if summary.received >= 1 => GatewayProbe::Reachable,
        Ok(_) => GatewayProbe::Unreachable,
        Err(e) => {
            log::warn!(
                "gateway-probe: EspPing error (treating as Indeterminate): {:?}",
                e
            );
            GatewayProbe::Indeterminate
        }
    }
}

/// Probe the default gateway's reachability via ICMP ping.
///
/// Resolves gateway IP and netif index under `WIFI_STACK.try_lock()`, drops the
/// guard, then delegates to `ping_reachable`. Returns `Indeterminate` (without
/// pinging) if the lock is busy, the stack is uninitialized, or no DHCP lease
/// exists (gateway is `0.0.0.0`).
fn probe_gateway_reachable() -> GatewayProbe {
    // Resolve gateway and netif index under try_lock; drop guard before blocking ping.
    let (gateway, netif_index) = {
        let guard = match WIFI_STACK.try_lock() {
            Ok(g) => g,
            Err(_) => {
                log::debug!("gateway-probe: WIFI_STACK busy — Indeterminate");
                return GatewayProbe::Indeterminate;
            }
        };
        let Some(stack) = guard.as_ref() else {
            log::error!("gateway-probe: WIFI_STACK uninitialized — invariant violated; returning Indeterminate");
            return GatewayProbe::Indeterminate;
        };
        let ip_info = match stack.wifi.wifi().sta_netif().get_ip_info() {
            Ok(info) => info,
            Err(e) => {
                log::warn!("gateway-probe: get_ip_info failed — Indeterminate: {:?}", e);
                return GatewayProbe::Indeterminate;
            }
        };
        let gw = ip_info.subnet.gateway;
        if gw == Ipv4Addr::UNSPECIFIED {
            log::debug!("gateway-probe: gateway is 0.0.0.0 (no DHCP lease) — Indeterminate");
            return GatewayProbe::Indeterminate;
        }
        let index = stack.wifi.wifi().sta_netif().get_index();
        (gw, index)
    };
    ping_reachable(gateway, netif_index)
}

/// Ring the WiFi supervisor doorbell (fire-and-forget).
///
/// Coalescing is safe: the supervisor re-reads actual state on each wake.
pub(crate) fn ring_wifi_wake() {
    let guard = WIFI_WAKE_TX.lock().unwrap_or_else(|_| {
        panic!("WIFI_WAKE_TX mutex poisoned — another thread panicked holding it")
    });
    if let Some(ref tx) = *guard {
        let _ = tx.try_send(()); // Full → already pending; None → before spawn; both OK.
    }
}

/// Whether an active WiFi credential source exists: a RAM-only temporary override,
/// or (absent an override) a usable (non-empty) NVS SSID.
///
/// Mirrors the credential precondition in [`associate_from_active_config`]. Takes no
/// `WIFI_STACK` lock. On an NVS read error, reports `true` so a transient failure
/// never drops a healthy link.
fn wifi_credentials_present() -> bool {
    let has_override = WIFI_TEMP_CONFIG
        .lock()
        .unwrap_or_else(|_| panic!("WIFI_TEMP_CONFIG mutex poisoned"))
        .is_some();
    if has_override {
        return true;
    }
    let nvs = match open_wifi_nvs(false) {
        Ok(n) => n,
        Err(_) => return true,
    };
    let mut ssid_buf = [0u8; 33];
    match nvs_get_str(&nvs, "ssid", &mut ssid_buf) {
        Ok(Some(s)) => !s.is_empty(),
        Ok(None) => false,
        Err(_) => true,
    }
}

/// Associate WiFi from the active config — a RAM-only temporary override if one is
/// set, else NVS credentials (WPA2-Personal).
///
/// Reads the override (if any) or NVS SSID/passphrase, configures the driver,
/// associates, and waits for netif up. Returns `Ok((ip, gateway, rssi))`. On
/// failure, stops and resets the driver for re-entrancy.
#[allow(clippy::result_large_err)] // device_protocol::TestResultMsg is the no-alloc error type on no_std
fn associate_from_active_config() -> Result<([u8; 4], [u8; 4], i32), device_protocol::TestResultMsg>
{
    // ── Read credentials: override first, else NVS ────────────────────────────
    let override_creds: Option<(heapless::String<32>, heapless::String<64>)> = {
        let guard = WIFI_TEMP_CONFIG
            .lock()
            .unwrap_or_else(|_| panic!("WIFI_TEMP_CONFIG mutex poisoned"));
        guard.as_ref().map(|c| (c.ssid.clone(), c.pass.clone()))
    };

    let (ssid_cfg, pass_cfg): (heapless::String<32>, heapless::String<64>) =
        if let Some((ssid, pass)) = override_creds {
            (ssid, pass)
        } else {
            let nvs = open_wifi_nvs(false)?;

            let mut ssid_buf = [0u8; 33];
            let ssid = match nvs_get_str(&nvs, "ssid", &mut ssid_buf)? {
                Some(s) if !s.is_empty() => s.to_owned(),
                _ => {
                    let mut e = device_protocol::TestResultMsg::new();
                    let _ = e.push_str(log_tokens::NO_NVS_CREDENTIALS);
                    let _ = e.push_str(" — provision first");
                    return Err(e);
                }
            };

            let mut pass_buf = [0u8; 65];
            let pass = match nvs_get_str(&nvs, "pass", &mut pass_buf)? {
                Some(s) => s.to_owned(),
                None => {
                    let mut e = device_protocol::TestResultMsg::new();
                    let _ = e.push_str(log_tokens::NO_NVS_CREDENTIALS);
                    let _ = e.push_str(" — provision first");
                    return Err(e);
                }
            };
            drop(nvs);

            let ssid_cfg: heapless::String<32> = ssid.as_str().try_into().map_err(|_| {
                let mut e = device_protocol::TestResultMsg::new();
                let _ = e.push_str(
                    "ssid from NVS exceeds ClientConfiguration capacity — NVS write path bug",
                );
                e
            })?;
            let pass_cfg: heapless::String<64> = pass.as_str().try_into().map_err(|_| {
                let mut e = device_protocol::TestResultMsg::new();
                let _ = e.push_str(
                    "passphrase from NVS exceeds ClientConfiguration capacity — NVS write path bug",
                );
                e
            })?;
            (ssid_cfg, pass_cfg)
        };

    // ── Configure + connect ───────────────────────────────────────────────────
    let mut guard = WIFI_STACK
        .lock()
        .unwrap_or_else(|_| panic!("WIFI_STACK mutex poisoned"));
    let stack = guard
        .as_mut()
        .expect("WIFI_STACK is None — not initialized at boot");

    let wifi_config = Configuration::Client(ClientConfiguration {
        ssid: ssid_cfg,
        auth_method: AuthMethod::WPA2Personal,
        password: pass_cfg,
        bssid: None,
        channel: None,
        ..Default::default()
    });

    if let Err(e) = stack.wifi.set_configuration(&wifi_config) {
        let mut msg = device_protocol::TestResultMsg::new();
        let _ = core::fmt::write(&mut msg, format_args!("set_configuration failed: {:?}", e));
        return Err(msg);
    }

    if let Err(e) = ensure_wifi_started(&mut stack.wifi) {
        let mut msg = device_protocol::TestResultMsg::new();
        let _ = core::fmt::write(&mut msg, format_args!("wifi start failed: {:?}", e));
        return Err(msg);
    }

    if let Err(e) = stack.wifi.connect() {
        if let Err(stop_err) = stack.wifi.stop() {
            log::warn!(
                "wifi stop after connect failure also failed: {:?}",
                stop_err
            );
        }
        let mut msg = device_protocol::TestResultMsg::new();
        let _ = core::fmt::write(&mut msg, format_args!("assoc failed: {:?}", e));
        return Err(msg);
    }

    if let Err(e) = stack.wifi.wait_netif_up() {
        if let Err(disc_err) = stack.wifi.disconnect() {
            log::warn!(
                "wifi disconnect after netif-up failure also failed: {:?}",
                disc_err
            );
        }
        if let Err(stop_err) = stack.wifi.stop() {
            log::warn!(
                "wifi stop after netif-up failure also failed: {:?}",
                stop_err
            );
        }
        let mut msg = device_protocol::TestResultMsg::new();
        let _ = core::fmt::write(&mut msg, format_args!("dhcp/netif-up timeout: {:?}", e));
        return Err(msg);
    }

    // ── Read IP info + RSSI ───────────────────────────────────────────────────
    let ip_info: IpInfo = stack.wifi.wifi().sta_netif().get_ip_info().map_err(|e| {
        let mut msg = device_protocol::TestResultMsg::new();
        let _ = core::fmt::write(&mut msg, format_args!("get_ip_info failed: {:?}", e));
        msg
    })?;

    let rssi: i32 = stack.wifi.wifi().get_rssi().map_err(|e| {
        let mut msg = device_protocol::TestResultMsg::new();
        let _ = core::fmt::write(&mut msg, format_args!("get_rssi failed: {:?}", e));
        msg
    })?;

    let ip_octets = ip_info.ip.octets();
    let gw_octets = ip_info.subnet.gateway.octets();

    // ── Assert valid IP ───────────────────────────────────────────────────────
    if ip_octets == [0, 0, 0, 0] || ip_octets[0] == 127 {
        let mut msg = device_protocol::TestResultMsg::new();
        let _ = core::fmt::write(
            &mut msg,
            format_args!("ip invalid ip={}", fmt_ipv4(ip_octets)),
        );
        return Err(msg);
    }
    if gw_octets == [0, 0, 0, 0] {
        let mut msg = device_protocol::TestResultMsg::new();
        let _ = msg.push_str("gateway is zero");
        return Err(msg);
    }
    if rssi == 0 || rssi <= -80 {
        let mut msg = device_protocol::TestResultMsg::new();
        let _ = core::fmt::write(&mut msg, format_args!("rssi={} not in (-80,0)", rssi));
        return Err(msg);
    }

    Ok((ip_octets, gw_octets, rssi))
}

/// WiFi association + DHCP self-test.
///
/// Associates from NVS credentials and returns IP/gateway/RSSI. Idempotent:
/// returns current info if already connected. Leaves the stack up for
/// subsequent network tests.
pub(crate) fn run_wifi_associate() -> (Status, Payload) {
    // ── Idempotent check: already up? ─────────────────────────────────────────
    {
        let guard = WIFI_STACK
            .lock()
            .unwrap_or_else(|_| panic!("WIFI_STACK mutex poisoned"));
        let stack = guard
            .as_ref()
            .expect("WIFI_STACK is None — not initialized at boot");
        match stack.wifi.is_up() {
            Ok(true) => {
                let ip_info = match stack.wifi.wifi().sta_netif().get_ip_info() {
                    Ok(i) => i,
                    Err(e) => {
                        return test_report_fail_detail("get_ip_info (already up) failed", &e)
                    }
                };
                let rssi = match stack.wifi.wifi().get_rssi() {
                    Ok(r) => r,
                    Err(e) => return test_report_fail_detail("get_rssi (already up) failed", &e),
                };
                let ip_octets = ip_info.ip.octets();
                let gw_octets = ip_info.subnet.gateway.octets();
                log::info!(
                    "wifi_associate: already up ip={} gw={} rssi={}",
                    fmt_ipv4(ip_octets),
                    fmt_ipv4(gw_octets),
                    rssi
                );
                return test_report_ok(TestData::WifiAssociate {
                    ip: ip_octets,
                    gateway: gw_octets,
                    rssi,
                });
            }
            Ok(false) => { /* not up — fall through to connect */ }
            Err(e) => return test_report_fail_detail("wifi is_up query failed", &e),
        }
    }

    // ── Associate via the active config (override, else NVS) ──────────────────
    match associate_from_active_config() {
        Ok((ip_octets, gw_octets, rssi)) => {
            log::info!(
                "wifi_associate ok ip={} gw={} rssi={}",
                fmt_ipv4(ip_octets),
                fmt_ipv4(gw_octets),
                rssi
            );
            test_report_ok(TestData::WifiAssociate {
                ip: ip_octets,
                gateway: gw_octets,
                rssi,
            })
        }
        Err(msg) => test_report_fail_msg(msg),
    }
}

/// Locks `WIFI_STACK` and issues a best-effort `disconnect()` + `stop()` before a
/// forced re-associate. Returns `true` only if `WIFI_STACK` was present *and* both
/// `disconnect()` and `stop()` succeeded — i.e. the caller can trust the old link is
/// actually down. Returns `false` on `WIFI_STACK` being `None` (an invariant
/// violation, logged here) or on either driver call failing (also logged, at `warn`).
///
/// Most callers (the supervisor's own tick arms, `run_gateway_probe_gate`) treat this
/// as best-effort and ignore the result: a failure there self-heals on the
/// supervisor's next tick or association attempt regardless. Callers that need to
/// know whether the *old* link was actually torn down before reporting success to an
/// operator (e.g. `handle_set_temporary_wifi_config`) must check the return value.
/// Panics if the mutex is poisoned.
pub(crate) fn force_disconnect_wifi() -> bool {
    let mut guard = WIFI_STACK
        .lock()
        .unwrap_or_else(|_| panic!("WIFI_STACK mutex poisoned in force_disconnect_wifi"));
    if let Some(ref mut stack) = *guard {
        let disconnect_err = stack.wifi.disconnect().err().filter(|e| {
            // ESP_ERR_WIFI_NOT_STARTED means the driver was already stopped (e.g. as
            // part of a just-completed failed association attempt's own cleanup) —
            // there was nothing to disconnect from, which is the goal state, not a
            // teardown failure. Any other error is a genuine unconfirmed teardown.
            e.code() != esp_idf_svc::sys::ESP_ERR_WIFI_NOT_STARTED
        });
        if let Some(ref e) = disconnect_err {
            log::warn!(
                "force-disconnect: pre-reassociate disconnect failed (ignored): {:?}",
                e
            );
        }
        let stop_err = stack
            .wifi
            .stop()
            .err()
            .filter(|e| e.code() != esp_idf_svc::sys::ESP_ERR_WIFI_NOT_STARTED);
        if let Some(ref e) = stop_err {
            log::warn!(
                "force-disconnect: pre-reassociate stop failed (ignored): {:?}",
                e
            );
        }
        disconnect_err.is_none() && stop_err.is_none()
    } else {
        log::error!(
            "force-disconnect: WIFI_STACK is None in force-reassociate arm — invariant violated"
        );
        false
    }
}

/// Polls until wifi is up with a DHCP-complete (non-zero-gateway) lease, or times
/// out after 90 s. `label` prefixes all failure messages. On success returns
/// `(ip, gw, rssi)` for the caller to format its own PASS string.
#[allow(clippy::result_large_err)] // TestReport carries a 192-byte heapless detail string
#[allow(clippy::type_complexity)] // (ip, gw, rssi) tuple mirrors the call sites' bindings
fn poll_for_wifi_up(label: &str) -> Result<([u8; 4], [u8; 4], i32), (Status, Payload)> {
    const POLL_INTERVAL_MS: u32 = 500;
    const TIMEOUT_SECS: u32 = 90;
    const MAX_POLLS: u32 = TIMEOUT_SECS * 1000 / POLL_INTERVAL_MS;

    for _ in 0..MAX_POLLS {
        FreeRtos::delay_ms(POLL_INTERVAL_MS);
        if wifi_is_up_nonblocking() == Some(true) {
            let guard = WIFI_STACK
                .lock()
                .unwrap_or_else(|_| panic!("WIFI_STACK mutex poisoned in poll_for_wifi_up"));
            let stack = match guard.as_ref() {
                Some(s) => s,
                None => {
                    return Err(test_report_fail_fmt(format_args!(
                        "{label}: WIFI_STACK vanished after is_up reported true"
                    )))
                }
            };
            let ip_info = match stack.wifi.wifi().sta_netif().get_ip_info() {
                Ok(i) => i,
                Err(e) => {
                    return Err(test_report_fail_fmt(format_args!(
                        "{label}: get_ip_info failed: {e:?}"
                    )))
                }
            };
            let rssi = match stack.wifi.wifi().get_rssi() {
                Ok(r) => r,
                Err(e) => {
                    return Err(test_report_fail_fmt(format_args!(
                        "{label}: get_rssi failed: {e:?}"
                    )))
                }
            };
            let ip = ip_info.ip.octets();
            let gw = ip_info.subnet.gateway.octets();
            // Wait for DHCP to finish (is_up can be true before the lease arrives).
            if gw == [0, 0, 0, 0] {
                continue;
            }
            return Ok((ip, gw, rssi));
        }
    }

    Err(test_report_fail_fmt(format_args!(
        "{label}: FAIL wifi-up=false (timeout {TIMEOUT_SECS}s)"
    )))
}

/// WiFi re-association self-test.
///
/// Forces a disconnect and verifies the supervisor autonomously re-associates
/// within 90 s. Requires WiFi to be already associated.
pub(crate) fn run_wifi_reassociation() -> (Status, Payload) {
    {
        let mut guard = WIFI_STACK
            .lock()
            .unwrap_or_else(|_| panic!("WIFI_STACK mutex poisoned"));
        let stack = guard
            .as_mut()
            .expect("WIFI_STACK is None — not initialized at boot");
        match stack.wifi.is_up() {
            Ok(true) => {} // good — proceed to disconnect
            Ok(false) => {
                return test_report_fail_fmt(format_args!(
                "WifiReassociation: WiFi not associated at test start — run WifiAssociate first",
            ))
            }
            Err(e) => return test_report_fail_detail("WifiReassociation: is_up query failed", &e),
        }
        // Force disconnect — continue even on error since the supervisor will
        // still attempt reconnect on its next tick.
        if let Err(e) = stack.wifi.disconnect() {
            log::warn!(
                "wifi_reassociation: forced disconnect returned error (continuing): {:?}",
                e
            );
        } else {
            log::info!("wifi_reassociation: forced disconnect issued — waiting for supervisor to re-associate");
        }
    }
    ring_wifi_wake();

    // Poll for re-association.
    let (ip, gw, rssi) = match poll_for_wifi_up("WifiReassociation") {
        Ok(t) => t,
        Err(f) => return f,
    };
    test_report_ok(TestData::WifiReassociation {
        reconnected: true,
        ip,
        gateway: gw,
        rssi,
    })
}

/// Gateway probe gate self-test.
///
/// Validates both halves of the gateway-reachability probe:
/// - Half 1: pings the HIL host (provisioned peer_ip), asserts `Reachable`.
/// - Half 2: pings a computed blackhole IP on the LAN, asserts `Unreachable`,
///   then forces disconnect + re-associate and waits for link recovery.
///
/// Requires prior `WifiAssociate` and `ProvisionPeer`.
pub(crate) fn run_gateway_probe_gate() -> (Status, Payload) {
    // Read peer_ip from NVS before acquiring WIFI_STACK.
    let peer_ip_arr: [u8; 4] = {
        let nvs = match open_wifi_nvs(false) {
            Ok(n) => n,
            Err(msg) => return test_report_fail_msg(msg),
        };
        match nvs_get_blob4(&nvs, "peer_ip") {
            Ok(a) => a,
            Err(msg) => return test_report_fail_msg(msg),
        }
    };

    let (device_ip_arr, gw_arr, prefix_len, netif_index) = {
        let guard = WIFI_STACK
            .lock()
            .unwrap_or_else(|_| panic!("WIFI_STACK mutex poisoned"));
        let stack = guard
            .as_ref()
            .expect("WIFI_STACK is None — not initialized at boot");
        match stack.wifi.is_up() {
            Ok(true) => {}
            Ok(false) => {
                return test_report_fail_fmt(format_args!(
                    "GatewayProbeGate: WiFi not associated at test start — run WifiAssociate first",
                ))
            }
            Err(e) => return test_report_fail_detail("GatewayProbeGate: is_up query failed", &e),
        }

        let ip_info = match stack.wifi.wifi().sta_netif().get_ip_info() {
            Ok(i) => i,
            Err(e) => return test_report_fail_detail("GatewayProbeGate: get_ip_info failed", &e),
        };
        let device_ip_arr = ip_info.ip.octets();
        let gw_arr = ip_info.subnet.gateway.octets();
        let prefix_len: u8 = ip_info.subnet.mask.0;
        let netif_index = stack.wifi.wifi().sta_netif().get_index();
        (device_ip_arr, gw_arr, prefix_len, netif_index)
    };

    let peer_ip = Ipv4Addr::from(peer_ip_arr);

    // Half 1: reachable target.
    log::info!(
        "gateway-probe-gate: half-1 reachable probe → peer_ip={}",
        fmt_ipv4(peer_ip_arr)
    );
    match ping_reachable(peer_ip, netif_index) {
        GatewayProbe::Reachable => {
            log::info!("gateway-probe-gate: half-1 PASS probe=reachable reassociated=false");
        }
        GatewayProbe::Unreachable => {
            return test_report_fail_fmt(format_args!(
                "GatewayProbeGate: half-1 FAIL probe=unreachable for peer_ip — network problem or wrong peer_ip provisioned",
            ));
        }
        GatewayProbe::Indeterminate => {
            return test_report_fail_fmt(format_args!(
                "GatewayProbeGate: half-1 FAIL probe=indeterminate for peer_ip — ping stack error",
            ));
        }
    }

    // Pick the highest host address on the subnet that is neither the gateway
    // nor this device. No ARP responder → ping returns Unreachable.
    let blackhole: Ipv4Addr = {
        if prefix_len == 0 || prefix_len >= 32 {
            return test_report_fail_fmt(format_args!(
                "GatewayProbeGate: cannot compute blackhole IP — subnet prefix_len out of range (expected /1–/31)",
            ));
        }
        let host_bits = 32 - prefix_len as u32;
        let mask_u32: u32 = !0u32 << host_bits;
        let ip_u32 = u32::from_be_bytes(device_ip_arr);
        let network_u32 = ip_u32 & mask_u32;
        let broadcast_u32 = network_u32 | !mask_u32;
        // Try broadcast-1, broadcast-2, broadcast-3 to avoid gateway and device.
        let gw_u32 = u32::from_be_bytes(gw_arr);
        let device_u32 = ip_u32;
        let mut candidate = None;
        for offset in 1u32..=3 {
            let addr_u32 = broadcast_u32.saturating_sub(offset);
            if addr_u32 != gw_u32
                && addr_u32 != device_u32
                && addr_u32 > network_u32
                && addr_u32 < broadcast_u32
            {
                candidate = Some(addr_u32);
                break;
            }
        }
        match candidate {
            Some(a) => Ipv4Addr::from(a.to_be_bytes()),
            None => {
                return test_report_fail_fmt(format_args!(
                    "GatewayProbeGate: cannot find a blackhole candidate on this subnet (subnet too small?)",
                ));
            }
        }
    };

    // Half 2: blackhole target.
    log::info!(
        "gateway-probe-gate: half-2 blackhole probe → blackhole={}",
        fmt_ipv4(blackhole.octets())
    );
    match ping_reachable(blackhole, netif_index) {
        GatewayProbe::Unreachable => {
            log::info!("gateway-probe-gate: half-2 blackhole probe=unreachable — proceeding to force-reassociate");
        }
        GatewayProbe::Reachable => {
            return test_report_fail_fmt(format_args!(
                "GatewayProbeGate: half-2 FAIL probe=reachable for blackhole address — something answered ICMP on the blackhole IP",
            ));
        }
        GatewayProbe::Indeterminate => {
            return test_report_fail_fmt(format_args!(
                "GatewayProbeGate: half-2 FAIL probe=indeterminate for blackhole — ping stack error",
            ));
        }
    }

    // Force-reassociate: disconnect + stop, then let the supervisor reconnect.
    if !force_disconnect_wifi() {
        return test_report_fail_fmt(format_args!(
            "GatewayProbeGate: WIFI_STACK is None before force-reassociate — invariant violated",
        ));
    }
    log::info!(
        "gateway-probe-gate: force-disconnect issued — waiting for supervisor to re-associate"
    );
    ring_wifi_wake();

    let (ip, gw, rssi) = match poll_for_wifi_up("GatewayProbeGate") {
        Ok(t) => t,
        Err(f) => return f,
    };
    test_report_ok(TestData::GatewayProbeGate {
        blackhole_reachable: false,
        reassociated: true,
        ip,
        gateway: gw,
        rssi,
    })
}

/// Apply a RAM-only temporary WiFi config override, bypassing NVS.
///
/// Validates `ssid` is non-empty (empty ssid is the "no credentials" sentinel
/// elsewhere; accepting it here would create an unreachable half-state), stores the
/// override, logs [`log_tokens::WIFI_TEMP_CONFIG_APPLIED`] with the ssid only (never
/// the passphrase), force-disconnects, and wakes the supervisor so the override takes
/// effect on its next loop iteration without a reboot.
///
/// If the old link's `disconnect()`/`stop()` cannot be confirmed to have succeeded,
/// returns `Status::Fail`: the override is stored either way (so a later probe/retry
/// can still pick it up), but the caller must not assume the override has taken
/// effect — the old link may still be up on the previous config.
pub(crate) fn handle_set_temporary_wifi_config(
    ssid: heapless::String<32>,
    pass: heapless::String<64>,
) -> (Status, Payload) {
    if ssid.is_empty() {
        return test_report_fail("empty ssid rejected — would alias the no-credentials sentinel");
    }
    log::info!("{}{}", log_tokens::WIFI_TEMP_CONFIG_APPLIED, ssid.as_str());
    {
        let mut guard = WIFI_TEMP_CONFIG
            .lock()
            .unwrap_or_else(|_| panic!("WIFI_TEMP_CONFIG mutex poisoned"));
        *guard = Some(TempWifiConfig { ssid, pass });
    }
    let disconnected = force_disconnect_wifi();
    ring_wifi_wake();
    if !disconnected {
        return test_report_fail(
            "override stored, but the previous link's disconnect/stop could not be confirmed \
             — it may still be up on the old config until a later probe or retry applies the override",
        );
    }
    (Status::Ok, Payload::Empty)
}

/// Clear the RAM-only temporary WiFi config override, if any.
///
/// If an override was present: logs [`log_tokens::WIFI_TEMP_CONFIG_CLEARED`],
/// force-disconnects, and wakes the supervisor so it reverts to NVS credentials (or
/// parks if NVS holds none). If no override was present: a pure no-op — a healthy
/// persisted-credential link must not be bounced by a redundant clear.
///
/// If an override was present but the old link's `disconnect()`/`stop()` cannot be
/// confirmed to have succeeded, returns `Status::Fail`: the override is cleared either
/// way (so the supervisor's next probe/retry no longer sees it), but the caller must
/// not assume the trial link has actually been torn down — it may still be up on the
/// just-cleared config until a later probe or retry re-evaluates.
pub(crate) fn handle_clear_temporary_wifi_config() -> (Status, Payload) {
    let was_present = clear_temp_config_no_wake();
    if was_present {
        log::info!("{}", log_tokens::WIFI_TEMP_CONFIG_CLEARED);
        let disconnected = force_disconnect_wifi();
        ring_wifi_wake();
        if !disconnected {
            return test_report_fail(
                "override cleared, but the previous link's disconnect/stop could not be \
                 confirmed — it may still be up on the temporary config until a later probe \
                 or retry applies the revert",
            );
        }
    }
    (Status::Ok, Payload::Empty)
}

/// Clear the RAM-only temporary WiFi config override without disturbing the link.
///
/// Used by `handle_clear_wifi_credentials` (`nvs.rs`), which already performs its own
/// force-disconnect + ring after clearing NVS; a second doorbell here would be
/// redundant. Returns whether an override was actually present (for logging by the
/// caller, which owns the wifi-credentials-clear narrative).
pub(crate) fn clear_temp_config_no_wake() -> bool {
    let mut guard = WIFI_TEMP_CONFIG
        .lock()
        .unwrap_or_else(|_| panic!("WIFI_TEMP_CONFIG mutex poisoned"));
    guard.take().is_some()
}
